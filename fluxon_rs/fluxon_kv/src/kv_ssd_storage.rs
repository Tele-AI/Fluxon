use crate::master_kv_router::put::PutIDForAKey;
use crate::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult};
use ::tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    HybridCacheEntry, HybridCachePolicy, PsyncIoEngineConfig, Source,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Instant;

const MEMORY_CAPACITY_BYTES: usize = 1;
const MEMORY_SHARDS: usize = 1;
const BLOCK_SIZE_BYTES: usize = 64 * 1024 * 1024;
const FLUSH_BUFFER_BYTES: usize = BLOCK_SIZE_BYTES;
const SUBMIT_QUEUE_BYTES: usize = 2 * FLUSH_BUFFER_BYTES;
// Thirteen workload-sized values (13 * 4.5 MiB) fit below one 64 MiB block.
// This is an I/O aggregation bound only; every entry keeps an independent
// single-KV generation and result.
pub(crate) const MAX_PERSIST_BATCH_ITEMS: usize = 13;
pub const MIN_CAPACITY_BYTES: u64 = BLOCK_SIZE_BYTES as u64;

pub fn safe_path_component(raw: &str) -> String {
    format!("v1-{}", hex::encode(Sha256::digest(raw.as_bytes())))
}

#[derive(Clone, Debug)]
pub struct KvSsdStorageRootLimit {
    pub root_dir: PathBuf,
    pub limit_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct KvSsdStorageInit {
    pub roots: Vec<KvSsdStorageRootLimit>,
    pub write_rate_limit_bytes_per_sec: Option<u64>,
    pub write_burst_bytes: Option<u64>,
    pub capacity_writeback_enabled: bool,
}

#[derive(Clone, Debug, Default)]
pub struct KvSsdStorageDeviceUsage {
    pub device: String,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub persist_requests: u64,
    pub persist_successes: u64,
    pub persist_failures: u64,
    pub persist_bytes: u64,
    pub persist_duration_us: u64,
    pub persist_batch_requests: u64,
    pub persist_batch_items: u64,
    pub persist_flush_batches: u64,
    pub persist_busy_batches: u64,
    pub persist_admission_skips: u64,
    pub persist_batch_duration_us: u64,
    pub write_candidate_items: u64,
    pub write_candidate_bytes: u64,
    pub write_admitted_items: u64,
    pub write_admitted_bytes: u64,
    pub write_dropped_items: u64,
    pub write_dropped_bytes: u64,
    pub write_refunded_items: u64,
    pub write_refunded_bytes: u64,
    pub load_requests: u64,
    pub load_successes: u64,
    pub load_misses: u64,
    pub load_failures: u64,
    pub load_bytes: u64,
    pub load_duration_us: u64,
    pub memory_hits: u64,
    pub disk_hits: u64,
    pub outer_hits: u64,
    pub removals: u64,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
struct KvSsdKey {
    key: String,
    put_id: PutIDForAKey,
}

#[derive(Debug)]
struct WriteRateLimiter {
    rate_bytes_per_sec: u64,
    burst_bytes: u64,
    available_bytes: u64,
    refill_remainder: u128,
    last_refill: Instant,
}

impl WriteRateLimiter {
    fn new(rate_bytes_per_sec: u64, burst_bytes: u64) -> Self {
        Self {
            rate_bytes_per_sec,
            burst_bytes,
            available_bytes: burst_bytes,
            refill_remainder: 0,
            last_refill: Instant::now(),
        }
    }

    fn refill(&mut self, now: Instant) {
        let elapsed_ns = now.duration_since(self.last_refill).as_nanos();
        self.last_refill = now;
        if self.available_bytes == self.burst_bytes {
            self.refill_remainder = 0;
            return;
        }
        let scaled = elapsed_ns
            .saturating_mul(u128::from(self.rate_bytes_per_sec))
            .saturating_add(self.refill_remainder);
        let refill = scaled / 1_000_000_000u128;
        self.refill_remainder = scaled % 1_000_000_000u128;
        let refill = u64::try_from(refill).unwrap_or(u64::MAX);
        self.available_bytes = self
            .available_bytes
            .saturating_add(refill)
            .min(self.burst_bytes);
        if self.available_bytes == self.burst_bytes {
            self.refill_remainder = 0;
        }
    }

    fn try_consume(&mut self, bytes: u64, now: Instant) -> bool {
        self.refill(now);
        if bytes > self.available_bytes {
            return false;
        }
        self.available_bytes -= bytes;
        true
    }
}

type SsdEntry = HybridCacheEntry<KvSsdKey, Vec<u8>>;

/// Pins a just-persisted entry until the master has published its SSD backing.
pub(crate) struct KvSsdPersistGuard {
    _entry: Option<SsdEntry>,
}

#[derive(Clone, Debug)]
pub(crate) struct KvSsdPersistSource {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub addr: u64,
    pub len: u64,
}

pub(crate) struct KvSsdPersistCopy {
    key: String,
    put_id: PutIDForAKey,
    data: Vec<u8>,
}

impl KvSsdPersistCopy {
    fn len(&self) -> u64 {
        u64::try_from(self.data.len()).unwrap_or(u64::MAX)
    }
}

pub(crate) struct KvSsdPersistBatchPermit {
    _guard: OwnedMutexGuard<()>,
}

/// The only owner-local SSD store used by Fluxon KV.
///
/// DRAM ownership, distributed routing, and Put/Get terminal state remain in
/// their existing modules. This type only persists and materializes bytes for
/// one `(key, put_id)` generation.
#[derive(Debug)]
pub struct KvSsdStorage {
    cache: HybridCache<KvSsdKey, Vec<u8>>,
    root_dir: PathBuf,
    capacity_bytes: u64,
    capacity_writeback_enabled: bool,
    write_rate_limiter: Option<Mutex<WriteRateLimiter>>,
    // Exactly one admitted write batch may own Foyer's global durability
    // barrier. New unrelated pressure batches use try_lock and fail open to
    // ordinary DRAM reclaim instead of becoming SSD backlog.
    persist_batch_gate: Arc<AsyncMutex<()>>,
    entry_lengths: Mutex<HashMap<KvSsdKey, u64>>,
    logical_used_bytes: AtomicU64,
    persist_requests: AtomicU64,
    persist_successes: AtomicU64,
    persist_failures: AtomicU64,
    persist_bytes: AtomicU64,
    persist_duration_us: AtomicU64,
    persist_batch_requests: AtomicU64,
    persist_batch_items: AtomicU64,
    persist_flush_batches: AtomicU64,
    persist_busy_batches: AtomicU64,
    persist_admission_skips: AtomicU64,
    persist_batch_duration_us: AtomicU64,
    write_candidate_items: AtomicU64,
    write_candidate_bytes: AtomicU64,
    write_admitted_items: AtomicU64,
    write_admitted_bytes: AtomicU64,
    write_dropped_items: AtomicU64,
    write_dropped_bytes: AtomicU64,
    write_refunded_items: AtomicU64,
    write_refunded_bytes: AtomicU64,
    load_requests: AtomicU64,
    load_successes: AtomicU64,
    load_misses: AtomicU64,
    load_failures: AtomicU64,
    load_bytes: AtomicU64,
    load_duration_us: AtomicU64,
    memory_hits: AtomicU64,
    disk_hits: AtomicU64,
    outer_hits: AtomicU64,
    removals: AtomicU64,
}

impl KvSsdStorage {
    pub async fn new(init: KvSsdStorageInit) -> KvResult<Self> {
        let write_rate_limiter = match (init.write_rate_limit_bytes_per_sec, init.write_burst_bytes)
        {
            (None, None) => None,
            (Some(rate), Some(burst)) if rate > 0 && burst > 0 => {
                Some(Mutex::new(WriteRateLimiter::new(rate, burst)))
            }
            _ => {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: "SSD write rate and burst must be configured together and be positive"
                        .to_string(),
                }));
            }
        };
        let [root] = init.roots.as_slice() else {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "kv ssd storage currently requires exactly one local root, got {}",
                    init.roots.len()
                ),
            }));
        };
        if root.limit_bytes < MIN_CAPACITY_BYTES {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "kv ssd capacity must be at least {} bytes, got {}",
                    BLOCK_SIZE_BYTES, root.limit_bytes
                ),
            }));
        }
        let capacity_bytes = usize::try_from(root.limit_bytes).map_err(|_| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!("kv ssd capacity does not fit usize: {}", root.limit_bytes),
            })
        })?;

        fs::create_dir_all(&root.root_dir)
            .map_err(|err| file_error(&root.root_dir, "create root", err))?;
        let storage_root = root.root_dir.join("foyer");
        match fs::remove_dir_all(&storage_root) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(file_error(&storage_root, "clear old store", err)),
        }
        fs::create_dir_all(&storage_root)
            .map_err(|err| file_error(&storage_root, "create store", err))?;

        let device = FsDeviceBuilder::new(&storage_root)
            .with_capacity(capacity_bytes)
            .with_direct(true)
            .build()
            .map_err(|err| storage_error("build filesystem device", err))?;
        let engine = BlockEngineConfig::new(device)
            .with_block_size(BLOCK_SIZE_BYTES)
            .with_buffer_pool_size(FLUSH_BUFFER_BYTES)
            .with_submit_queue_size_threshold(SUBMIT_QUEUE_BYTES);
        let cache = HybridCacheBuilder::new()
            .with_name("fluxon_kv_ssd")
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            .with_flush_on_close(false)
            .memory(MEMORY_CAPACITY_BYTES)
            .with_shards(MEMORY_SHARDS)
            .with_weighter(|_key: &KvSsdKey, value: &Vec<u8>| value.len())
            .with_filter(|_key: &KvSsdKey, _value: &Vec<u8>| false)
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(engine)
            .build()
            .await
            .map_err(|err| storage_error("build cache", err))?;

        tracing::info!(
            root = %root.root_dir.display(),
            capacity_bytes = root.limit_bytes,
            write_rate_limit_bytes_per_sec = init.write_rate_limit_bytes_per_sec,
            write_burst_bytes = init.write_burst_bytes,
            capacity_writeback_enabled = init.capacity_writeback_enabled,
            direct_io = true,
            "Initialized owner-local KV SSD backing"
        );

        Ok(Self {
            cache,
            root_dir: root.root_dir.clone(),
            capacity_bytes: root.limit_bytes,
            capacity_writeback_enabled: init.capacity_writeback_enabled,
            write_rate_limiter,
            persist_batch_gate: Arc::new(AsyncMutex::new(())),
            entry_lengths: Mutex::new(HashMap::new()),
            logical_used_bytes: AtomicU64::new(0),
            persist_requests: AtomicU64::new(0),
            persist_successes: AtomicU64::new(0),
            persist_failures: AtomicU64::new(0),
            persist_bytes: AtomicU64::new(0),
            persist_duration_us: AtomicU64::new(0),
            persist_batch_requests: AtomicU64::new(0),
            persist_batch_items: AtomicU64::new(0),
            persist_flush_batches: AtomicU64::new(0),
            persist_busy_batches: AtomicU64::new(0),
            persist_admission_skips: AtomicU64::new(0),
            persist_batch_duration_us: AtomicU64::new(0),
            write_candidate_items: AtomicU64::new(0),
            write_candidate_bytes: AtomicU64::new(0),
            write_admitted_items: AtomicU64::new(0),
            write_admitted_bytes: AtomicU64::new(0),
            write_dropped_items: AtomicU64::new(0),
            write_dropped_bytes: AtomicU64::new(0),
            write_refunded_items: AtomicU64::new(0),
            write_refunded_bytes: AtomicU64::new(0),
            load_requests: AtomicU64::new(0),
            load_successes: AtomicU64::new(0),
            load_misses: AtomicU64::new(0),
            load_failures: AtomicU64::new(0),
            load_bytes: AtomicU64::new(0),
            load_duration_us: AtomicU64::new(0),
            memory_hits: AtomicU64::new(0),
            disk_hits: AtomicU64::new(0),
            outer_hits: AtomicU64::new(0),
            removals: AtomicU64::new(0),
        })
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    pub(crate) fn capacity_writeback_enabled(&self) -> bool {
        self.capacity_writeback_enabled
    }

    pub fn usage_snapshot(&self) -> KvSsdStorageDeviceUsage {
        KvSsdStorageDeviceUsage {
            device: self.root_dir.display().to_string(),
            capacity_bytes: self.capacity_bytes,
            used_bytes: self
                .logical_used_bytes
                .load(Ordering::Relaxed)
                .min(self.capacity_bytes),
            persist_requests: self.persist_requests.load(Ordering::Relaxed),
            persist_successes: self.persist_successes.load(Ordering::Relaxed),
            persist_failures: self.persist_failures.load(Ordering::Relaxed),
            persist_bytes: self.persist_bytes.load(Ordering::Relaxed),
            persist_duration_us: self.persist_duration_us.load(Ordering::Relaxed),
            persist_batch_requests: self.persist_batch_requests.load(Ordering::Relaxed),
            persist_batch_items: self.persist_batch_items.load(Ordering::Relaxed),
            persist_flush_batches: self.persist_flush_batches.load(Ordering::Relaxed),
            persist_busy_batches: self.persist_busy_batches.load(Ordering::Relaxed),
            persist_admission_skips: self.persist_admission_skips.load(Ordering::Relaxed),
            persist_batch_duration_us: self.persist_batch_duration_us.load(Ordering::Relaxed),
            write_candidate_items: self.write_candidate_items.load(Ordering::Relaxed),
            write_candidate_bytes: self.write_candidate_bytes.load(Ordering::Relaxed),
            write_admitted_items: self.write_admitted_items.load(Ordering::Relaxed),
            write_admitted_bytes: self.write_admitted_bytes.load(Ordering::Relaxed),
            write_dropped_items: self.write_dropped_items.load(Ordering::Relaxed),
            write_dropped_bytes: self.write_dropped_bytes.load(Ordering::Relaxed),
            write_refunded_items: self.write_refunded_items.load(Ordering::Relaxed),
            write_refunded_bytes: self.write_refunded_bytes.load(Ordering::Relaxed),
            load_requests: self.load_requests.load(Ordering::Relaxed),
            load_successes: self.load_successes.load(Ordering::Relaxed),
            load_misses: self.load_misses.load(Ordering::Relaxed),
            load_failures: self.load_failures.load(Ordering::Relaxed),
            load_bytes: self.load_bytes.load(Ordering::Relaxed),
            load_duration_us: self.load_duration_us.load(Ordering::Relaxed),
            memory_hits: self.memory_hits.load(Ordering::Relaxed),
            disk_hits: self.disk_hits.load(Ordering::Relaxed),
            outer_hits: self.outer_hits.load(Ordering::Relaxed),
            removals: self.removals.load(Ordering::Relaxed),
        }
    }

    pub async fn close(&self) -> KvResult<()> {
        self.cache
            .close()
            .await
            .map_err(|err| storage_error("close", err))
    }

    /// Select at most one bounded durability batch from master-confirmed
    /// last-backing candidates. Smaller values win ties because they preserve
    /// more independently reusable keys per admitted byte. The decision is
    /// immediate: candidates outside the burst/rate budget are returned as
    /// false and must be reclaimed without waiting for SSD bandwidth.
    pub(crate) fn admit_owner_write_candidates(&self, lengths: &[u64]) -> Vec<bool> {
        let mut order = (0..lengths.len()).collect::<Vec<_>>();
        order.sort_by_key(|index| (lengths[*index], *index));
        let mut admitted = vec![false; lengths.len()];
        let mut admitted_items = 0usize;
        let mut admitted_bytes = 0u64;
        let now = Instant::now();
        let mut limiter = self
            .write_rate_limiter
            .as_ref()
            .map(|limiter| limiter.lock());
        for index in order {
            if admitted_items == MAX_PERSIST_BATCH_ITEMS {
                break;
            }
            let len = lengths[index];
            let allowed = limiter
                .as_mut()
                .is_none_or(|limiter| limiter.try_consume(len, now));
            if allowed {
                admitted[index] = true;
                admitted_items += 1;
                admitted_bytes = admitted_bytes.saturating_add(len);
            }
        }
        let candidate_bytes = lengths.iter().copied().fold(0u64, u64::saturating_add);
        let candidate_items = u64::try_from(lengths.len()).unwrap_or(u64::MAX);
        let admitted_items = u64::try_from(admitted_items).unwrap_or(u64::MAX);
        let dropped_items = candidate_items.saturating_sub(admitted_items);
        let dropped_bytes = candidate_bytes.saturating_sub(admitted_bytes);
        self.write_candidate_items
            .fetch_add(candidate_items, Ordering::Relaxed);
        self.write_candidate_bytes
            .fetch_add(candidate_bytes, Ordering::Relaxed);
        self.write_admitted_items
            .fetch_add(admitted_items, Ordering::Relaxed);
        self.write_admitted_bytes
            .fetch_add(admitted_bytes, Ordering::Relaxed);
        self.write_dropped_items
            .fetch_add(dropped_items, Ordering::Relaxed);
        self.write_dropped_bytes
            .fetch_add(dropped_bytes, Ordering::Relaxed);
        admitted
    }

    /// Return rate-budget bytes when a provisionally admitted no-queue batch
    /// loses the persist gate before any copy or I/O starts. This keeps gate
    /// contention from silently burning future bandwidth while preserving the
    /// immediate-Drop contract.
    pub(crate) fn refund_owner_write_admission(&self, lengths: &[u64]) {
        if lengths.is_empty() {
            return;
        }
        let Some(limiter) = self.write_rate_limiter.as_ref() else {
            return;
        };
        let refunded_bytes = lengths.iter().copied().fold(0u64, u64::saturating_add);
        let mut limiter = limiter.lock();
        limiter.refill(Instant::now());
        limiter.available_bytes = limiter
            .available_bytes
            .saturating_add(refunded_bytes)
            .min(limiter.burst_bytes);
        drop(limiter);
        self.write_refunded_items.fetch_add(
            u64::try_from(lengths.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.write_refunded_bytes
            .fetch_add(refunded_bytes, Ordering::Relaxed);
    }

    /// Copy exact source bytes into owner-owned buffers. Callers may release
    /// source holders after this returns; durability uses only these copies.
    pub(crate) fn copy_batch_from_addrs(
        sources: &[KvSsdPersistSource],
    ) -> Vec<KvResult<KvSsdPersistCopy>> {
        sources
            .iter()
            .map(|source| {
                let len = checked_len(source.len, "persist copy")?;
                let data =
                    unsafe { std::slice::from_raw_parts(source.addr as *const u8, len).to_vec() };
                Ok(KvSsdPersistCopy {
                    key: source.key.clone(),
                    put_id: source.put_id,
                    data,
                })
            })
            .collect()
    }

    pub(crate) async fn persist_batch_from_addrs(
        &self,
        sources: &[KvSsdPersistSource],
    ) -> Vec<KvResult<Option<KvSsdPersistGuard>>> {
        if sources.is_empty() {
            return Vec::new();
        }
        match self.try_acquire_persist_batch(sources.len()) {
            Ok(Some(permit)) => {
                self.persist_batch_from_copies_with_permit(
                    permit,
                    Self::copy_batch_from_addrs(sources),
                )
                .await
            }
            Ok(None) => sources.iter().map(|_| Ok(None)).collect(),
            Err(_) => sources
                .iter()
                .map(|_| Err(persist_batch_size_error(sources.len())))
                .collect(),
        }
    }

    pub(crate) fn try_acquire_persist_batch(
        &self,
        item_count: usize,
    ) -> KvResult<Option<KvSsdPersistBatchPermit>> {
        self.persist_requests
            .fetch_add(item_count as u64, Ordering::Relaxed);
        self.persist_batch_requests.fetch_add(1, Ordering::Relaxed);
        self.persist_batch_items
            .fetch_add(item_count as u64, Ordering::Relaxed);
        if item_count == 0 || item_count > MAX_PERSIST_BATCH_ITEMS {
            self.persist_failures
                .fetch_add(item_count as u64, Ordering::Relaxed);
            return Err(persist_batch_size_error(item_count));
        }
        let Ok(guard) = self.persist_batch_gate.clone().try_lock_owned() else {
            self.persist_busy_batches.fetch_add(1, Ordering::Relaxed);
            self.persist_admission_skips
                .fetch_add(item_count as u64, Ordering::Relaxed);
            return Ok(None);
        };
        Ok(Some(KvSsdPersistBatchPermit { _guard: guard }))
    }

    /// Persist owner-owned copies through the canonical durability path.
    #[cfg(test)]
    pub(crate) async fn persist_batch_from_copies(
        &self,
        copies: Vec<KvResult<KvSsdPersistCopy>>,
    ) -> Vec<KvResult<Option<KvSsdPersistGuard>>> {
        if copies.is_empty() {
            return Vec::new();
        }
        let item_count = copies.len();
        match self.try_acquire_persist_batch(item_count) {
            Ok(Some(permit)) => {
                self.persist_batch_from_copies_with_permit(permit, copies)
                    .await
            }
            Ok(None) => copies.into_iter().map(|copy| copy.map(|_| None)).collect(),
            Err(_) => copies
                .into_iter()
                .map(|copy| copy.and_then(|_| Err(persist_batch_size_error(item_count))))
                .collect(),
        }
    }

    pub(crate) async fn persist_batch_from_copies_with_permit(
        &self,
        _permit: KvSsdPersistBatchPermit,
        copies: Vec<KvResult<KvSsdPersistCopy>>,
    ) -> Vec<KvResult<Option<KvSsdPersistGuard>>> {
        let item_count = copies.len();
        let lengths = copies
            .iter()
            .map(|copy| copy.as_ref().map_or(0, KvSsdPersistCopy::len))
            .collect::<Vec<_>>();
        let started_at = Instant::now();
        let mut results = std::iter::repeat_with(|| None)
            .take(item_count)
            .collect::<Vec<Option<KvResult<Option<KvSsdPersistGuard>>>>>();
        let mut batch_keys = std::collections::HashSet::with_capacity(item_count);
        let mut pending = Vec::<(usize, KvSsdKey, u64, SsdEntry)>::new();

        for (index, copy) in copies.into_iter().enumerate() {
            let copy = match copy {
                Ok(copy) => copy,
                Err(err) => {
                    results[index] = Some(Err(err));
                    continue;
                }
            };
            let len = lengths[index];
            let cache_key = KvSsdKey {
                key: copy.key.clone(),
                put_id: copy.put_id,
            };
            if !batch_keys.insert(cache_key.clone()) {
                results[index] = Some(Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "kv ssd persist batch contains duplicate generation: key={} put_id=({},{})",
                        copy.key, copy.put_id.0, copy.put_id.1
                    ),
                })));
                continue;
            }
            let existing_len = self.entry_lengths.lock().get(&cache_key).copied();
            if let Some(existing_len) = existing_len {
                if existing_len != len {
                    results[index] = Some(Err(KvError::Api(ApiError::InvalidArgument {
                        detail: format!(
                            "kv ssd duplicate persist length mismatch: key={} put_id=({},{}) existing={} requested={}",
                            copy.key, copy.put_id.0, copy.put_id.1, existing_len, len
                        ),
                    })));
                    continue;
                }
                // Lengths are published only after the batch durability
                // barrier, so a tracked generation can be replayed directly.
                if self.cache.storage().may_contains(&cache_key) {
                    results[index] = Some(Ok(Some(KvSsdPersistGuard { _entry: None })));
                    continue;
                }
                self.forget_entry(&cache_key);
            }

            match self
                .cache
                .storage_writer(cache_key.clone())
                .force()
                .insert(copy.data)
            {
                Some(entry) => pending.push((index, cache_key, len, entry)),
                None => {
                    results[index] = Some(Err(KvError::Api(ApiError::FileWriteError {
                        path: self.root_dir.display().to_string(),
                        offset: 0,
                        detail: format!(
                            "SSD admission rejected: key={} put_id=({},{})",
                            copy.key, copy.put_id.0, copy.put_id.1
                        ),
                    })));
                }
            }
        }

        if !pending.is_empty() {
            self.persist_flush_batches.fetch_add(1, Ordering::Relaxed);
            // One global barrier covers every independently keyed insertion in
            // this admitted batch. No per-item Wait is interleaved into the
            // Foyer submit queue.
            self.cache.storage().wait().await;
        }
        for (index, cache_key, len, entry) in pending {
            if !self.cache.storage().may_contains(&cache_key) {
                results[index] = Some(Err(KvError::Api(ApiError::FileWriteError {
                    path: self.root_dir.display().to_string(),
                    offset: 0,
                    detail: format!(
                        "SSD commit missing: key={} put_id=({},{})",
                        cache_key.key, cache_key.put_id.0, cache_key.put_id.1
                    ),
                })));
                continue;
            }
            let mut lengths = self.entry_lengths.lock();
            if lengths.insert(cache_key, len).is_none() {
                self.logical_used_bytes.fetch_add(len, Ordering::Relaxed);
            }
            drop(lengths);
            results[index] = Some(Ok(Some(KvSsdPersistGuard {
                _entry: Some(entry),
            })));
        }

        let elapsed = elapsed_us(started_at);
        self.persist_batch_duration_us
            .fetch_add(elapsed, Ordering::Relaxed);
        self.persist_duration_us
            .fetch_add(elapsed.saturating_mul(item_count as u64), Ordering::Relaxed);
        let mut successes = 0u64;
        let mut failures = 0u64;
        let mut bytes = 0u64;
        let results = results
            .into_iter()
            .enumerate()
            .map(|(index, result)| {
                let result = result.unwrap_or_else(|| {
                    Err(KvError::Api(ApiError::Unknown {
                        detail: "kv ssd batch result was not populated".to_string(),
                    }))
                });
                match &result {
                    Ok(Some(_)) => {
                        successes = successes.saturating_add(1);
                        bytes = bytes.saturating_add(lengths[index]);
                    }
                    Ok(None) => {}
                    Err(_) => failures = failures.saturating_add(1),
                }
                result
            })
            .collect::<Vec<_>>();
        self.persist_successes
            .fetch_add(successes, Ordering::Relaxed);
        self.persist_failures.fetch_add(failures, Ordering::Relaxed);
        self.persist_bytes.fetch_add(bytes, Ordering::Relaxed);
        results
    }

    #[cfg(test)]
    async fn persist(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        data: &[u8],
    ) -> KvResult<KvSsdPersistGuard> {
        let source = KvSsdPersistSource {
            key: key.to_string(),
            put_id,
            addr: data.as_ptr() as u64,
            len: data.len() as u64,
        };
        match self
            .persist_batch_from_addrs(std::slice::from_ref(&source))
            .await
            .pop()
            .expect("one persist source must produce one result")?
        {
            Some(guard) => Ok(guard),
            None => Err(KvError::Api(ApiError::Unknown {
                detail: "test persist was skipped by an unexpected busy batch".to_string(),
            })),
        }
    }

    pub(crate) async fn load_into_addr(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        target_addr: u64,
        len: u64,
        target_capacity: u64,
    ) -> KvResult<()> {
        self.load_requests.fetch_add(1, Ordering::Relaxed);
        let started_at = Instant::now();
        let result = self
            .load_into_addr_inner(key, put_id, target_addr, len, target_capacity)
            .await;
        self.load_duration_us
            .fetch_add(elapsed_us(started_at), Ordering::Relaxed);
        match &result {
            Ok(()) => {
                self.load_successes.fetch_add(1, Ordering::Relaxed);
                self.load_bytes.fetch_add(len, Ordering::Relaxed);
            }
            Err(KvError::Api(ApiError::KeyNotFound { .. })) => {
                self.load_misses.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.load_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
        result
    }

    async fn load_into_addr_inner(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        target_addr: u64,
        len: u64,
        target_capacity: u64,
    ) -> KvResult<()> {
        if target_capacity < len {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "kv ssd target too small: key={} put_id=({},{}) len={} capacity={}",
                    key, put_id.0, put_id.1, len, target_capacity
                ),
            }));
        }
        let len_usize = checked_len(len, "load")?;
        let cache_key = KvSsdKey {
            key: key.to_string(),
            put_id,
        };
        let entry = self
            .cache
            .get(&cache_key)
            .await
            .map_err(|err| storage_error("load", err))?
            .ok_or_else(|| {
                self.forget_entry(&cache_key);
                KvError::Api(ApiError::KeyNotFound {
                    key: key.to_string(),
                })
            })?;
        match entry.source() {
            Source::Memory => self.memory_hits.fetch_add(1, Ordering::Relaxed),
            Source::Disk => self.disk_hits.fetch_add(1, Ordering::Relaxed),
            Source::Outer => self.outer_hits.fetch_add(1, Ordering::Relaxed),
        };
        if entry.value().len() != len_usize {
            tracing::warn!(
                key,
                put_time_ms = put_id.0,
                put_version = put_id.1,
                expected_len = len,
                actual_len = entry.value().len(),
                "Dropping corrupt/stale KV SSD entry with a length mismatch"
            );
            self.forget_entry(&cache_key);
            self.cache.remove(&cache_key);
            return Err(KvError::Api(ApiError::KeyNotFound {
                key: key.to_string(),
            }));
        }
        unsafe {
            std::ptr::copy_nonoverlapping(
                entry.value().as_ptr(),
                target_addr as *mut u8,
                len_usize,
            );
        }
        Ok(())
    }

    pub(crate) async fn remove_exact(&self, key: &str, put_id: PutIDForAKey) -> bool {
        let _batch_guard = self.persist_batch_gate.lock().await;
        let cache_key = KvSsdKey {
            key: key.to_string(),
            put_id,
        };
        let removed = self.entry_lengths.lock().remove(&cache_key);
        self.cache.remove(&cache_key);
        if let Some(len) = removed {
            self.logical_used_bytes.fetch_sub(len, Ordering::Relaxed);
            self.removals.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    fn forget_entry(&self, key: &KvSsdKey) {
        if let Some(len) = self.entry_lengths.lock().remove(key) {
            self.logical_used_bytes.fetch_sub(len, Ordering::Relaxed);
        }
    }
}

fn elapsed_us(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn checked_len(len: u64, operation: &str) -> KvResult<usize> {
    let len = usize::try_from(len).map_err(|_| {
        KvError::Api(ApiError::InvalidArgument {
            detail: format!("kv ssd {operation} len does not fit usize: {len}"),
        })
    })?;
    if len == 0 {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: format!("kv ssd {operation} len must be positive"),
        }));
    }
    Ok(len)
}

fn persist_batch_size_error(item_count: usize) -> KvError {
    KvError::Api(ApiError::InvalidArgument {
        detail: format!(
            "kv ssd persist batch item count is outside 1..={}: items={}",
            MAX_PERSIST_BATCH_ITEMS, item_count
        ),
    })
}

fn storage_error(operation: &str, err: impl std::fmt::Display) -> KvError {
    KvError::Api(ApiError::Unknown {
        detail: format!("kv ssd {operation} failed: {err}"),
    })
}

fn file_error(path: &Path, operation: &str, err: std::io::Error) -> KvError {
    KvError::Api(ApiError::FileWriteError {
        path: path.display().to_string(),
        offset: 0,
        detail: format!("{operation}: {err}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_manager::NodeID;
    use crate::master_kv_router::{
        CommittedSlotReplica, KvMemoryReplica, KvNodeReplicas, KvReplicaBacking, OneKvNodesRoutes,
        SsdReplicaCommitStatus,
    };
    use crate::master_seg_manager::NodeTombTag;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;
    use std::time::Duration;

    fn test_root(name: &str) -> PathBuf {
        let target = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/mnt/nvme0/mjq_build/push_sglang_fluxon_target"));
        target.join("kv_ssd_tests").join(format!(
            "{}-{}-{}",
            name,
            std::process::id(),
            uuid::Uuid::new_v4()
        ))
    }

    #[test]
    fn write_rate_limiter_refills_without_queueing() {
        let start = Instant::now();
        let mut limiter = WriteRateLimiter {
            rate_bytes_per_sec: 100,
            burst_bytes: 100,
            available_bytes: 100,
            refill_remainder: 0,
            last_refill: start,
        };
        assert!(limiter.try_consume(60, start));
        assert!(!limiter.try_consume(50, start));
        assert!(limiter.try_consume(50, start + Duration::from_millis(500)));
        assert_eq!(limiter.available_bytes, 40);
    }

    #[tokio::test]
    async fn owner_candidate_admission_prefers_small_values_and_one_batch() {
        let root = test_root("candidate-admission");
        let store = KvSsdStorage::new(KvSsdStorageInit {
            roots: vec![KvSsdStorageRootLimit {
                root_dir: root.clone(),
                limit_bytes: MIN_CAPACITY_BYTES,
            }],
            write_rate_limit_bytes_per_sec: Some(1),
            write_burst_bytes: Some(10),
            capacity_writeback_enabled: true,
        })
        .await
        .unwrap();
        let admitted = store.admit_owner_write_candidates(&[8, 3, 3]);
        assert_eq!(admitted, vec![false, true, true]);
        let usage = store.usage_snapshot();
        assert_eq!(usage.write_candidate_items, 3);
        assert_eq!(usage.write_candidate_bytes, 14);
        assert_eq!(usage.write_admitted_items, 2);
        assert_eq!(usage.write_admitted_bytes, 6);
        assert_eq!(usage.write_dropped_items, 1);
        assert_eq!(usage.write_dropped_bytes, 8);

        store.close().await.unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn busy_gate_refund_restores_owner_write_budget() {
        let root = test_root("candidate-refund");
        let store = KvSsdStorage::new(KvSsdStorageInit {
            roots: vec![KvSsdStorageRootLimit {
                root_dir: root.clone(),
                limit_bytes: MIN_CAPACITY_BYTES,
            }],
            write_rate_limit_bytes_per_sec: Some(1),
            write_burst_bytes: Some(10),
            capacity_writeback_enabled: true,
        })
        .await
        .unwrap();

        assert_eq!(store.admit_owner_write_candidates(&[8]), vec![true]);
        store.refund_owner_write_admission(&[8]);
        assert_eq!(store.admit_owner_write_candidates(&[8]), vec![true]);
        let usage = store.usage_snapshot();
        assert_eq!(usage.write_refunded_items, 1);
        assert_eq!(usage.write_refunded_bytes, 8);

        store.close().await.unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn persist_and_load_one_generation() {
        let root = test_root("roundtrip");
        let store = KvSsdStorage::new(KvSsdStorageInit {
            roots: vec![KvSsdStorageRootLimit {
                root_dir: root.clone(),
                limit_bytes: BLOCK_SIZE_BYTES as u64,
            }],
            write_rate_limit_bytes_per_sec: None,
            write_burst_bytes: None,
            capacity_writeback_enabled: true,
        })
        .await
        .unwrap();
        let input = vec![0x5au8; 4096];
        let guard = store.persist("key", (7, 3), &input).await.unwrap();
        let mut output = vec![0u8; input.len()];
        store
            .load_into_addr(
                "key",
                (7, 3),
                output.as_mut_ptr() as u64,
                input.len() as u64,
                output.len() as u64,
            )
            .await
            .unwrap();
        assert_eq!(output, input);
        let usage = store.usage_snapshot();
        assert_eq!(usage.used_bytes, input.len() as u64);
        assert_eq!(usage.persist_requests, 1);
        assert_eq!(usage.persist_successes, 1);
        assert_eq!(usage.persist_failures, 0);
        assert_eq!(usage.persist_bytes, input.len() as u64);
        assert_eq!(usage.load_requests, 1);
        assert_eq!(usage.load_successes, 1);
        assert_eq!(usage.load_misses, 0);
        assert_eq!(usage.load_bytes, input.len() as u64);
        assert_eq!(usage.memory_hits + usage.disk_hits + usage.outer_hits, 1);

        assert!(store.remove_exact("key", (7, 3)).await);
        assert!(!store.remove_exact("key", (7, 3)).await);
        assert_eq!(store.usage_snapshot().used_bytes, 0);
        assert!(
            store
                .load_into_addr(
                    "key",
                    (7, 3),
                    output.as_mut_ptr() as u64,
                    input.len() as u64,
                    output.len() as u64,
                )
                .await
                .is_err()
        );
        let usage = store.usage_snapshot();
        assert_eq!(usage.load_requests, 2);
        assert_eq!(usage.load_misses, 1);
        assert_eq!(usage.removals, 1);
        drop(guard);
        store.close().await.unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn copied_persist_no_longer_reads_the_source_buffer() {
        let root = test_root("copied-persist");
        let store = KvSsdStorage::new(KvSsdStorageInit {
            roots: vec![KvSsdStorageRootLimit {
                root_dir: root.clone(),
                limit_bytes: BLOCK_SIZE_BYTES as u64,
            }],
            write_rate_limit_bytes_per_sec: None,
            write_burst_bytes: None,
            capacity_writeback_enabled: true,
        })
        .await
        .unwrap();
        let mut input = vec![0x3cu8; 4096];
        let source = KvSsdPersistSource {
            key: "copied-key".to_string(),
            put_id: (8, 4),
            addr: input.as_ptr() as u64,
            len: input.len() as u64,
        };
        let copies = KvSsdStorage::copy_batch_from_addrs(std::slice::from_ref(&source));
        input.fill(0xe7);
        let guard = store
            .persist_batch_from_copies(copies)
            .await
            .pop()
            .unwrap()
            .unwrap()
            .expect("copied persist must acquire the durability gate");
        let mut output = vec![0u8; input.len()];
        store
            .load_into_addr(
                "copied-key",
                (8, 4),
                output.as_mut_ptr() as u64,
                output.len() as u64,
                output.len() as u64,
            )
            .await
            .unwrap();
        assert!(output.iter().all(|byte| *byte == 0x3c));

        drop(guard);
        store.close().await.unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn one_batch_uses_one_flush_barrier_and_keeps_per_key_results() {
        let root = test_root("one-batch-one-barrier");
        let store = KvSsdStorage::new(KvSsdStorageInit {
            roots: vec![KvSsdStorageRootLimit {
                root_dir: root.clone(),
                limit_bytes: MIN_CAPACITY_BYTES,
            }],
            write_rate_limit_bytes_per_sec: None,
            write_burst_bytes: None,
            capacity_writeback_enabled: true,
        })
        .await
        .unwrap();
        let payloads = (0..MAX_PERSIST_BATCH_ITEMS)
            .map(|index| vec![u8::try_from(index + 1).unwrap(); 1024 * 1024])
            .collect::<Vec<_>>();
        let sources = payloads
            .iter()
            .enumerate()
            .map(|(index, payload)| KvSsdPersistSource {
                key: format!("batch-victim-{index}"),
                put_id: (31, u32::try_from(index).unwrap()),
                addr: payload.as_ptr() as u64,
                len: payload.len() as u64,
            })
            .collect::<Vec<_>>();
        let guards = store
            .persist_batch_from_addrs(&sources)
            .await
            .into_iter()
            .map(|result| result.unwrap().expect("the only batch must be admitted"))
            .collect::<Vec<_>>();
        assert_eq!(guards.len(), MAX_PERSIST_BATCH_ITEMS);

        for (index, payload) in payloads.iter().enumerate() {
            let mut output = vec![0; payload.len()];
            store
                .load_into_addr(
                    &format!("batch-victim-{index}"),
                    (31, u32::try_from(index).unwrap()),
                    output.as_mut_ptr() as u64,
                    output.len() as u64,
                    output.len() as u64,
                )
                .await
                .unwrap();
            assert_eq!(&output, payload);
        }
        let usage = store.usage_snapshot();
        assert_eq!(usage.persist_successes, MAX_PERSIST_BATCH_ITEMS as u64);
        assert_eq!(usage.persist_failures, 0);
        assert_eq!(usage.persist_batch_requests, 1);
        assert_eq!(usage.persist_flush_batches, 1);
        assert_eq!(usage.persist_busy_batches, 0);
        assert_eq!(usage.persist_admission_skips, 0);
        assert_eq!(usage.load_successes, MAX_PERSIST_BATCH_ITEMS as u64);

        drop(guards);
        store.close().await.unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn retrying_a_multi_key_batch_reuses_durable_generations() {
        let root = test_root("multi-key-batch-retry");
        let store = KvSsdStorage::new(KvSsdStorageInit {
            roots: vec![KvSsdStorageRootLimit {
                root_dir: root.clone(),
                limit_bytes: MIN_CAPACITY_BYTES,
            }],
            write_rate_limit_bytes_per_sec: None,
            write_burst_bytes: None,
            capacity_writeback_enabled: true,
        })
        .await
        .unwrap();
        let original = (0..3)
            .map(|index| vec![u8::try_from(0x41 + index).unwrap(); 4096])
            .collect::<Vec<_>>();
        let original_sources = original
            .iter()
            .enumerate()
            .map(|(index, payload)| KvSsdPersistSource {
                key: format!("retry-victim-{index}"),
                put_id: (35, u32::try_from(index).unwrap()),
                addr: payload.as_ptr() as u64,
                len: payload.len() as u64,
            })
            .collect::<Vec<_>>();
        let first_guards = store
            .persist_batch_from_addrs(&original_sources)
            .await
            .into_iter()
            .map(|result| result.unwrap().expect("the first batch must be admitted"))
            .collect::<Vec<_>>();

        // A retry may arrive after the caller has reused its source buffer. The
        // generation identity, not the retried bytes, owns the durable result.
        let retry_payloads = original
            .iter()
            .map(|payload| vec![0xee; payload.len()])
            .collect::<Vec<_>>();
        let retry_sources = retry_payloads
            .iter()
            .enumerate()
            .map(|(index, payload)| KvSsdPersistSource {
                key: format!("retry-victim-{index}"),
                put_id: (35, u32::try_from(index).unwrap()),
                addr: payload.as_ptr() as u64,
                len: payload.len() as u64,
            })
            .collect::<Vec<_>>();
        let retry_guards = store
            .persist_batch_from_addrs(&retry_sources)
            .await
            .into_iter()
            .map(|result| result.unwrap().expect("a durable retry must be replayable"))
            .collect::<Vec<_>>();

        for (index, expected) in original.iter().enumerate() {
            let mut output = vec![0; expected.len()];
            store
                .load_into_addr(
                    &format!("retry-victim-{index}"),
                    (35, u32::try_from(index).unwrap()),
                    output.as_mut_ptr() as u64,
                    output.len() as u64,
                    output.len() as u64,
                )
                .await
                .unwrap();
            assert_eq!(&output, expected);
        }
        let expected_bytes = original.iter().map(Vec::len).sum::<usize>() as u64;
        let usage = store.usage_snapshot();
        assert_eq!(usage.used_bytes, expected_bytes);
        assert_eq!(usage.persist_requests, 6);
        assert_eq!(usage.persist_successes, 6);
        assert_eq!(usage.persist_failures, 0);
        assert_eq!(usage.persist_batch_requests, 2);
        assert_eq!(usage.persist_flush_batches, 1);
        assert_eq!(usage.persist_busy_batches, 0);

        drop(retry_guards);
        drop(first_guards);
        store.close().await.unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn busy_write_batch_skips_without_copying_or_queueing() {
        let root = test_root("busy-batch-skip");
        let store = KvSsdStorage::new(KvSsdStorageInit {
            roots: vec![KvSsdStorageRootLimit {
                root_dir: root.clone(),
                limit_bytes: MIN_CAPACITY_BYTES,
            }],
            write_rate_limit_bytes_per_sec: None,
            write_burst_bytes: None,
            capacity_writeback_enabled: true,
        })
        .await
        .unwrap();
        let input = vec![0xabu8; 4096];
        let source = KvSsdPersistSource {
            key: "busy-skip".to_string(),
            put_id: (37, 1),
            addr: input.as_ptr() as u64,
            len: input.len() as u64,
        };
        let held = store.persist_batch_gate.lock().await;
        let result = store
            .persist_batch_from_addrs(std::slice::from_ref(&source))
            .await
            .pop()
            .unwrap()
            .unwrap();
        assert!(result.is_none());
        drop(held);

        let usage = store.usage_snapshot();
        assert_eq!(usage.persist_requests, 1);
        assert_eq!(usage.persist_successes, 0);
        assert_eq!(usage.persist_failures, 0);
        assert_eq!(usage.persist_busy_batches, 1);
        assert_eq!(usage.persist_admission_skips, 1);
        assert_eq!(usage.persist_flush_batches, 0);

        store.close().await.unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn persist_commit_memory_evict_and_ssd_load_roundtrip() {
        let root = test_root("route-roundtrip");
        let store = KvSsdStorage::new(KvSsdStorageInit {
            roots: vec![KvSsdStorageRootLimit {
                root_dir: root.clone(),
                limit_bytes: MIN_CAPACITY_BYTES,
            }],
            write_rate_limit_bytes_per_sec: None,
            write_burst_bytes: None,
            capacity_writeback_enabled: true,
        })
        .await
        .unwrap();
        let owner: NodeID = "owner".to_string().into();
        let put_id = (19, 4);
        let input = vec![0xa5u8; 4096];
        let persist_guard = store.persist("route-key", put_id, &input).await.unwrap();

        let route = Arc::new(OneKvNodesRoutes {
            put_id,
            radix: None,
            lease_id: None,
            atomic_group: None,
            node_replicas: RwLock::new(HashMap::from([(
                owner.clone(),
                KvNodeReplicas::memory(
                    NodeTombTag::new(),
                    KvMemoryReplica {
                        backing: KvReplicaBacking::CommittedSlot(CommittedSlotReplica {
                            owner: crate::owner_segment::OwnerGeneration::new(
                                owner.as_ref().to_string(),
                                1,
                            ),
                            allocation_id: 7,
                            segment_offset: 0,
                            capacity_bytes: input.len() as u64,
                            addr: input.as_ptr() as u64,
                            len: input.len() as u64,
                            base_addr: input.as_ptr() as u64,
                            segment_registration_epoch: 1,
                        }),
                        owner_local_indexed: true,
                        get_durable_reservation: None,
                        capacity_reservation: None,
                    },
                ),
            )])),
            get_durable_slots_used: AtomicU32::new(0),
        });
        assert_eq!(
            route.commit_ssd_replica(&owner, input.len() as u64),
            SsdReplicaCommitStatus::Committed
        );

        // This is the terminal state produced by the already-covered exact
        // owner-memory reclaim transaction: the node and SSD route remain,
        // while DRAM is no longer readable.
        route.node_replicas.write().get_mut(&owner).unwrap().memory = None;
        {
            let replicas = route.node_replicas.read();
            let owner_backings = replicas.get(&owner).unwrap();
            assert!(owner_backings.memory.is_none());
            assert_eq!(
                owner_backings.ssd.as_ref().map(|ssd| ssd.len),
                Some(input.len() as u64)
            );
        }

        let mut output = vec![0u8; input.len()];
        store
            .load_into_addr(
                "route-key",
                put_id,
                output.as_mut_ptr() as u64,
                output.len() as u64,
                output.len() as u64,
            )
            .await
            .unwrap();
        assert_eq!(output, input);

        drop(persist_guard);
        store.close().await.unwrap();
        fs::remove_dir_all(root).unwrap();
    }
}
