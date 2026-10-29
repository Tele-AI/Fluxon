use crate::config::{KvSsdStorageBackend, KvSsdUringMode, KvSsdWritebackConfig};
use crate::kv_ssd_storage_foyer::{FoyerKvSsdPersistGuard, FoyerKvSsdStorage};
use crate::master_kv_router::msg_pack::{SparseGetRange, SsdReplicaEviction};
use crate::master_kv_router::put::PutIDForAKey;
use crate::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult};
use ::tokio::{
    sync::{
        Mutex as TokioMutex, Notify, OwnedSemaphorePermit, Semaphore, mpsc as tokio_mpsc, oneshot,
        watch,
    },
    task,
};
use fluxon_framework_compiled::shutdown::{ShutdownGate, ShutdownGuard};
use futures::stream::{FuturesUnordered, StreamExt};
use io_uring::{IoUring, opcode, types::Fd};
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::{self, OpenOptions};
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub(crate) const SSD_ALIGNMENT: usize = 512;
const DEFAULT_SHARDS_PER_OWNER: usize = 4;
const DEFAULT_URING_THREADS: usize = 16;
const DEFAULT_URING_IO_DEPTH: usize = 128;
const DEFAULT_URING_READ_WEIGHT: usize = 2;
const DEFAULT_WRITE_QUEUE_DEPTH: usize = 256;
const DEFAULT_READ_QUEUE_DEPTH: usize = 16;
const DEFAULT_READ_INFLIGHT: usize = 16;
const DEFAULT_READ_MAX_INFLIGHT_BYTES: usize = 512 * 1024 * 1024;
const DEFAULT_READ_MAX_INFLIGHT_OPS: usize = 256;
const DEFAULT_EVICTION_QUEUE_DEPTH: usize = 1024;
pub(crate) const DEFAULT_READ_TRANSFER_PIPELINE_CHUNK_BYTES: u64 = 4 * 1024 * 1024;
pub(crate) const DEFAULT_READ_TRANSFER_PIPELINE_INFLIGHT: usize = 4;
pub(crate) const MAX_PERSIST_BATCH_ITEMS: usize = 256;
/// Retained for configuration compatibility.  Native storage itself only
/// needs sector alignment, while existing owner configurations use this value
/// as their conservative SSD capacity floor.
pub const MIN_CAPACITY_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug)]
struct LegacyWriteRateLimiter {
    rate_bytes_per_sec: u64,
    burst_bytes: u64,
    available_bytes: u64,
    refill_remainder: u128,
    last_refill: Instant,
}

impl LegacyWriteRateLimiter {
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
        let refill = u64::try_from(scaled / 1_000_000_000u128).unwrap_or(u64::MAX);
        self.refill_remainder = scaled % 1_000_000_000u128;
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

    fn refund(&mut self, bytes: u64, now: Instant) {
        self.refill(now);
        self.available_bytes = self
            .available_bytes
            .saturating_add(bytes)
            .min(self.burst_bytes);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum KvSsdPersistClass {
    RequiredLastBacking,
    BestEffortEarly,
}

impl KvSsdPersistClass {
    fn as_u8(self) -> u8 {
        match self {
            Self::BestEffortEarly => 0,
            Self::RequiredLastBacking => 1,
        }
    }
}

/// Shared priority token for one exact generation. A Required follower can
/// promote an already queued Early write without starting a duplicate I/O.
#[derive(Clone, Debug)]
pub(crate) struct KvSsdPersistPriority(Arc<AtomicU8>);

impl KvSsdPersistPriority {
    pub(crate) fn new(class: KvSsdPersistClass) -> Self {
        Self(Arc::new(AtomicU8::new(class.as_u8())))
    }

    pub(crate) fn promote_required(&self) {
        self.0.store(
            KvSsdPersistClass::RequiredLastBacking.as_u8(),
            Ordering::Release,
        );
    }

    pub(crate) fn class(&self) -> KvSsdPersistClass {
        if self.0.load(Ordering::Acquire) == KvSsdPersistClass::RequiredLastBacking.as_u8() {
            KvSsdPersistClass::RequiredLastBacking
        } else {
            KvSsdPersistClass::BestEffortEarly
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct KvSsdPersistSource {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub addr: u64,
    pub len: u64,
    pub priority: KvSsdPersistPriority,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct KvSsdRangeLoadStats {
    /// Wall time spent pinning the generation, issuing native SSD reads, and
    /// materializing scratch-buffer payloads into the compact destination.
    pub service_us: u64,
    /// Bytes submitted to O_DIRECT after widening unaligned ranges.
    pub physical_bytes: u64,
}

pub(crate) struct KvSsdPersistCopy {
    key: String,
    put_id: PutIDForAKey,
    len: u64,
    data: AlignedBuffer,
    priority: KvSsdPersistPriority,
}

/// Keeps caller-owned read destination memory alive until every submitted SSD
/// command that can access it has reached a terminal completion.
///
/// The concrete guard remains owned by the caller's allocation layer.  This
/// storage-level token only extends that guard across the io_uring boundary,
/// where dropping the async caller does not cancel an already submitted read.
#[derive(Clone)]
pub(crate) struct KvSsdReadTargetLifetime {
    _guard: Arc<dyn Send + Sync>,
}

impl KvSsdReadTargetLifetime {
    pub(crate) fn new<T>(guard: Arc<T>) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self { _guard: guard }
    }
}

pub(crate) struct KvSsdPersistBatchPermit {
    _total_batch: OwnedSemaphorePermit,
    _early_batch: Option<OwnedSemaphorePermit>,
    _total_bytes: OwnedSemaphorePermit,
    _early_bytes: Option<OwnedSemaphorePermit>,
    item_count: usize,
    charged_bytes: u32,
    class: KvSsdPersistClass,
}

impl KvSsdPersistBatchPermit {
    /// Keep the byte reservation while discarding stale or joined candidates.
    pub(crate) fn retain_item_count(&mut self, item_count: usize) {
        assert!(item_count > 0 && item_count <= self.item_count);
        self.item_count = item_count;
    }
}

#[derive(Clone, Debug)]
pub struct KvSsdStorageInit {
    pub root_limits: Vec<KvSsdStorageRootLimit>,
    pub uring_mode: KvSsdUringMode,
    pub backend: KvSsdStorageBackend,
    pub writeback: KvSsdWritebackConfig,
}

#[derive(Clone, Debug)]
pub struct KvSsdStorageRootLimit {
    pub root_dir: PathBuf,
    pub limit_bytes: u64,
}

#[derive(Debug)]
pub struct KvSsdStorage {
    root_dirs: Vec<PathBuf>,
    devices: Vec<SsdDeviceWorker>,
    shard_to_device: Vec<usize>,
    next_write_device: AtomicUsize,
    write_config: KvSsdWritebackConfig,
    legacy_write_rate_limiter: Option<Mutex<LegacyWriteRateLimiter>>,
    write_admission: Arc<SsdWriteAdmission>,
    stats: Arc<KvSsdStorageStats>,
    read_admission: Arc<SsdReadAdmission>,
    inner: Arc<Mutex<KvSsdStorageInner>>,
    space_notify: Arc<Notify>,
    shutdown_gate: ShutdownGate,
    shutdown_tx: watch::Sender<bool>,
    worker_close_state: TokioMutex<SsdWorkerCloseState>,
    eviction_rx: Mutex<Option<tokio_mpsc::Receiver<Vec<SsdReplicaEviction>>>>,
    eviction_tx_guard: Mutex<Option<tokio_mpsc::Sender<Vec<SsdReplicaEviction>>>>,
    foyer: Option<FoyerKvSsdStorage>,
}

#[derive(Debug)]
struct SsdDeviceWorker {
    device_id: u64,
    root_dir: PathBuf,
    shard_ids: Vec<usize>,
    max_shard_capacity: u64,
    write_admission: Arc<SsdDeviceWriteAdmission>,
    runtime: Mutex<Option<SsdDeviceRuntime>>,
}

#[derive(Debug)]
struct SsdDeviceRuntime {
    io: Arc<UringIoEngine>,
    required_write_tx: tokio_mpsc::Sender<WriteCommand>,
    early_write_tx: tokio_mpsc::Sender<WriteCommand>,
    read_tx: tokio_mpsc::Sender<ReadCommand>,
    writer_handle: task::JoinHandle<()>,
    reader_handle: task::JoinHandle<()>,
}

#[derive(Debug)]
struct SsdWriteAdmission {
    total_batches: Arc<Semaphore>,
    early_batches: Arc<Semaphore>,
    total_bytes: Arc<Semaphore>,
    early_bytes: Arc<Semaphore>,
    max_total_bytes: u32,
    max_early_bytes: u32,
    max_batches: u32,
}

impl SsdWriteAdmission {
    fn new(config: &KvSsdWritebackConfig) -> Self {
        let max_total_bytes = u32::try_from(config.required_queue_bytes)
            .expect("validated SSD required queue bytes must fit u32");
        let max_early_bytes = u32::try_from(config.early_queue_bytes)
            .expect("validated SSD early queue bytes must fit u32");
        Self {
            total_batches: Arc::new(Semaphore::new(config.max_inflight_batches)),
            early_batches: Arc::new(Semaphore::new(
                config.max_inflight_batches.saturating_sub(1),
            )),
            total_bytes: Arc::new(Semaphore::new(max_total_bytes as usize)),
            early_bytes: Arc::new(Semaphore::new(max_early_bytes as usize)),
            max_total_bytes,
            max_early_bytes,
            max_batches: config.max_inflight_batches as u32,
        }
    }

    fn close(&self) {
        self.total_batches.close();
        self.early_batches.close();
        self.total_bytes.close();
        self.early_bytes.close();
    }
}

#[derive(Debug)]
struct SsdDeviceWriteAdmission {
    total_bytes: Arc<Semaphore>,
    early_bytes: Arc<Semaphore>,
    max_total_bytes: u32,
    max_early_bytes: u32,
    active_bytes: Arc<AtomicU64>,
}

impl SsdDeviceWriteAdmission {
    fn new(config: &KvSsdWritebackConfig) -> Self {
        let max_total_bytes = config.required_queue_bytes as u32;
        let max_early_bytes = config.early_queue_bytes as u32;
        Self {
            total_bytes: Arc::new(Semaphore::new(max_total_bytes as usize)),
            early_bytes: Arc::new(Semaphore::new(max_early_bytes as usize)),
            max_total_bytes,
            max_early_bytes,
            active_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    async fn acquire(
        &self,
        class: KvSsdPersistClass,
        aligned_len: u64,
    ) -> KvResult<SsdDeviceWritePermit> {
        let total_charge = aligned_len.min(u64::from(self.max_total_bytes)) as u32;
        let total = Arc::clone(&self.total_bytes)
            .acquire_many_owned(total_charge)
            .await
            .map_err(|_| {
                KvError::Api(ApiError::SystemShutdown {
                    detail: "SSD device Required byte queue is closed".to_string(),
                })
            })?;
        let early = if class == KvSsdPersistClass::BestEffortEarly {
            let early_charge = total_charge.min(self.max_early_bytes);
            Some(
                Arc::clone(&self.early_bytes)
                    .acquire_many_owned(early_charge)
                    .await
                    .map_err(|_| {
                        KvError::Api(ApiError::SystemShutdown {
                            detail: "SSD device Early byte queue is closed".to_string(),
                        })
                    })?,
            )
        } else {
            None
        };
        self.active_bytes
            .fetch_add(u64::from(total_charge), Ordering::AcqRel);
        Ok(SsdDeviceWritePermit {
            _total: total,
            _early: early,
            active_bytes: Arc::clone(&self.active_bytes),
            charged_bytes: u64::from(total_charge),
        })
    }

    fn close(&self) {
        self.total_bytes.close();
        self.early_bytes.close();
    }
}

struct SsdDeviceWritePermit {
    _total: OwnedSemaphorePermit,
    _early: Option<OwnedSemaphorePermit>,
    active_bytes: Arc<AtomicU64>,
    charged_bytes: u64,
}

impl Drop for SsdDeviceWritePermit {
    fn drop(&mut self) {
        self.active_bytes
            .fetch_sub(self.charged_bytes, Ordering::AcqRel);
    }
}

#[derive(Debug)]
enum SsdWorkerCloseState {
    Open,
    Closing(task::JoinHandle<Result<(), String>>),
    Closed(Result<(), String>),
}

#[derive(Clone, Debug)]
struct SsdDeviceRoot {
    device_id: u64,
    root_dir: PathBuf,
    limit_bytes: u64,
}

struct OpenedSsdShard {
    shard_id: usize,
    device_idx: usize,
    capacity: u64,
    file: std::fs::File,
}

#[derive(Clone, Debug)]
struct SsdShardSpec {
    shard_id: usize,
    device_idx: usize,
    capacity: u64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SsdLoadedChunk {
    pub offset: u64,
    pub stage_addr: u64,
    pub len: u64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct KvSsdStorageDeviceUsage {
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

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct KvSsdStorageStatsSnapshot {
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
}

#[derive(Debug, Default)]
struct KvSsdStorageStats {
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
}

impl KvSsdStorageStats {
    fn snapshot(&self) -> KvSsdStorageStatsSnapshot {
        KvSsdStorageStatsSnapshot {
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
        }
    }
}

#[derive(Debug)]
struct KvSsdStorageInner {
    ring: SsdRingBuffer,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct KvSsdKey {
    key: String,
    put_id: PutIDForAKey,
}

#[derive(Clone, Debug)]
struct SsdIndexEntry {
    shard_id: usize,
    begin: u64,
    len: u64,
    aligned_len: u64,
    file_offset: u64,
}

#[derive(Clone, Debug)]
struct SsdReadPinInfo {
    entry: SsdIndexEntry,
    count: usize,
}

#[derive(Clone, Debug)]
enum SsdEntryState {
    Writing(SsdIndexEntry),
    Committed(SsdIndexEntry),
}

impl SsdEntryState {
    fn entry(&self) -> &SsdIndexEntry {
        match self {
            Self::Writing(entry) | Self::Committed(entry) => entry,
        }
    }
}

#[derive(Debug)]
struct SsdShardRing {
    capacity: u64,
    head: u64,
    tail: u64,
    used_bytes: u64,
    busy_begins: BTreeMap<u64, usize>,
    order: VecDeque<SsdOrderEntry>,
}

#[derive(Debug)]
struct SsdOrderEntry {
    key: KvSsdKey,
    begin: u64,
}

#[derive(Debug)]
struct SsdRingBuffer {
    shards: Vec<SsdShardRing>,
    next_shard: usize,
    entries: HashMap<KvSsdKey, SsdEntryState>,
    read_pins: HashMap<KvSsdKey, SsdReadPinInfo>,
}

#[derive(Debug)]
enum SsdPreparedWrite {
    Ready {
        entry: SsdIndexEntry,
        evicted: Vec<KvSsdKey>,
    },
    Existing,
    BlockedByBusyIo,
}

#[derive(Debug)]
enum SsdAllocation {
    Ready {
        begin: u64,
        file_offset: u64,
        evicted: Vec<KvSsdKey>,
    },
    BlockedByBusyIo,
    TooLarge,
}

impl SsdRingBuffer {
    fn new(shard_capacities: Vec<u64>) -> Self {
        assert!(!shard_capacities.is_empty());
        Self {
            shards: shard_capacities
                .into_iter()
                .map(|capacity| SsdShardRing {
                    capacity,
                    head: 0,
                    tail: 0,
                    used_bytes: 0,
                    busy_begins: BTreeMap::new(),
                    order: VecDeque::new(),
                })
                .collect(),
            next_shard: 0,
            entries: HashMap::new(),
            read_pins: HashMap::new(),
        }
    }

    #[cfg(test)]
    fn get(&self, key: &KvSsdKey) -> Option<SsdIndexEntry> {
        match self.entries.get(key) {
            Some(SsdEntryState::Committed(entry)) if self.is_offset_valid(entry) => {
                Some(entry.clone())
            }
            _ => None,
        }
    }

    fn pin_read(&mut self, key: &KvSsdKey) -> Option<SsdIndexEntry> {
        let entry = match self.entries.get(key) {
            Some(SsdEntryState::Committed(entry)) if self.is_offset_valid(entry) => entry.clone(),
            _ => return None,
        };
        if let Some(pin) = self.read_pins.get_mut(key) {
            pin.count += 1;
        } else {
            self.read_pins.insert(
                key.clone(),
                SsdReadPinInfo {
                    entry: entry.clone(),
                    count: 1,
                },
            );
            self.mark_busy(&entry);
        }
        Some(entry)
    }

    fn unpin_read(&mut self, key: &KvSsdKey) {
        match self.read_pins.get_mut(key) {
            Some(pin) if pin.count > 1 => pin.count -= 1,
            Some(_) => {
                let pin = self
                    .read_pins
                    .remove(key)
                    .expect("kv ssd read pin disappeared while unpinning");
                self.unmark_busy(&pin.entry);
            }
            None => debug_assert!(false, "missing kv ssd read pin for key={key:?}"),
        }
    }

    fn mark_busy(&mut self, entry: &SsdIndexEntry) {
        let count = self.shards[entry.shard_id]
            .busy_begins
            .entry(entry.begin)
            .or_default();
        *count += 1;
    }

    fn unmark_busy(&mut self, entry: &SsdIndexEntry) {
        let shard = &mut self.shards[entry.shard_id];
        let remove_begin = match shard.busy_begins.get_mut(&entry.begin) {
            Some(count) if *count > 1 => {
                *count -= 1;
                false
            }
            Some(_) => true,
            None => {
                debug_assert!(false, "missing kv ssd busy entry for {entry:?}");
                false
            }
        };
        if remove_begin {
            shard.busy_begins.remove(&entry.begin);
        }
    }

    fn account_entry_inserted(&mut self, entry: &SsdIndexEntry) {
        let shard = &mut self.shards[entry.shard_id];
        shard.used_bytes = shard.used_bytes.saturating_add(entry.aligned_len);
        debug_assert!(shard.used_bytes <= shard.capacity);
    }

    fn account_entry_removed(&mut self, entry: &SsdIndexEntry) {
        let shard = &mut self.shards[entry.shard_id];
        debug_assert!(shard.used_bytes >= entry.aligned_len);
        shard.used_bytes = shard.used_bytes.saturating_sub(entry.aligned_len);
    }

    fn remove_entry(&mut self, key: &KvSsdKey) -> Option<SsdEntryState> {
        let state = self.entries.remove(key)?;
        let entry = state.entry().clone();
        self.account_entry_removed(&entry);
        if matches!(state, SsdEntryState::Writing(_)) {
            self.unmark_busy(&entry);
        }
        Some(state)
    }

    #[cfg(test)]
    fn prepare_write(&mut self, key: KvSsdKey, len: u64) -> KvResult<SsdPreparedWrite> {
        let allowed_shards = (0..self.shards.len()).collect::<Vec<_>>();
        self.prepare_write_on_shards(key, len, &allowed_shards)
    }

    fn prepare_write_on_shards(
        &mut self,
        key: KvSsdKey,
        len: u64,
        allowed_shards: &[usize],
    ) -> KvResult<SsdPreparedWrite> {
        if allowed_shards.is_empty() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "kv ssd device has no shards".to_string(),
            }));
        }
        if let Some(state) = self.entries.get(&key) {
            return Ok(match state {
                SsdEntryState::Writing(_) => SsdPreparedWrite::BlockedByBusyIo,
                SsdEntryState::Committed(entry) if entry.len == len => SsdPreparedWrite::Existing,
                SsdEntryState::Committed(entry) => {
                    return Err(KvError::Api(ApiError::InvalidArgument {
                        detail: format!(
                            "kv ssd duplicate persist length mismatch: key={} put_id=({},{}) existing_len={} requested_len={}",
                            key.key, key.put_id.0, key.put_id.1, entry.len, len
                        ),
                    }));
                }
            });
        }
        if self.read_pins.contains_key(&key) {
            return Ok(SsdPreparedWrite::BlockedByBusyIo);
        }
        let aligned_len = align_up_u64(len, SSD_ALIGNMENT as u64)?;
        let max_capacity = self
            .shards
            .iter()
            .enumerate()
            .filter(|(idx, _)| allowed_shards.contains(idx))
            .map(|(_, shard)| shard.capacity)
            .max()
            .ok_or_else(|| {
                KvError::Api(ApiError::InvalidArgument {
                    detail: format!("kv ssd device has invalid shard set: {allowed_shards:?}"),
                })
            })?;
        if aligned_len > max_capacity {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "kv ssd value len={} aligned_len={} exceeds shard capacity={}",
                    len, aligned_len, max_capacity
                ),
            }));
        }

        let shard_count = self.shards.len();
        for offset in 0..shard_count {
            let shard_id = (self.next_shard + offset) % shard_count;
            if !allowed_shards.contains(&shard_id) {
                continue;
            }
            let (begin, file_offset, evicted) = match self
                .allocate_contiguous(shard_id, aligned_len)
            {
                SsdAllocation::Ready {
                    begin,
                    file_offset,
                    evicted,
                } => (begin, file_offset, evicted),
                SsdAllocation::BlockedByBusyIo => continue,
                SsdAllocation::TooLarge => unreachable!("aligned_len was checked against capacity"),
            };
            self.next_shard = (shard_id + 1) % shard_count;

            let entry = SsdIndexEntry {
                shard_id,
                begin,
                len,
                aligned_len,
                file_offset,
            };
            self.entries
                .insert(key.clone(), SsdEntryState::Writing(entry.clone()));
            self.account_entry_inserted(&entry);
            self.mark_busy(&entry);
            self.shards[shard_id]
                .order
                .push_back(SsdOrderEntry { key, begin });
            return Ok(SsdPreparedWrite::Ready { entry, evicted });
        }

        Ok(SsdPreparedWrite::BlockedByBusyIo)
    }

    fn allocate_contiguous(&mut self, shard_id: usize, size: u64) -> SsdAllocation {
        let shard = &self.shards[shard_id];
        if size > shard.capacity {
            return SsdAllocation::TooLarge;
        }
        let capacity = shard.capacity;
        let mut head = shard.head;
        let phys = head % capacity;
        let space_until_end = capacity - phys;
        if size > space_until_end {
            head += space_until_end;
        }
        let begin = head;
        let new_head = head + size;
        let new_tail = new_head.saturating_sub(capacity);
        if self.has_busy_entries_before(shard_id, new_tail) {
            return SsdAllocation::BlockedByBusyIo;
        }

        self.shards[shard_id].head = new_head;
        let evicted = self.advance_tail(shard_id, new_tail);
        SsdAllocation::Ready {
            begin,
            file_offset: begin % capacity,
            evicted,
        }
    }

    fn advance_tail(&mut self, shard_id: usize, new_tail: u64) -> Vec<KvSsdKey> {
        if new_tail <= self.shards[shard_id].tail {
            return Vec::new();
        }
        debug_assert!(!self.has_busy_entries_before(shard_id, new_tail));
        self.shards[shard_id].tail = new_tail;
        let mut evicted = Vec::new();

        while self.shards[shard_id]
            .order
            .front()
            .is_some_and(|ordered| ordered.begin < new_tail)
        {
            let ordered = self.shards[shard_id]
                .order
                .pop_front()
                .expect("front order entry exists");
            let is_current_generation = self.entries.get(&ordered.key).is_some_and(|state| {
                let entry = state.entry();
                entry.shard_id == shard_id && entry.begin == ordered.begin
            });
            if is_current_generation
                && matches!(
                    self.remove_entry(&ordered.key),
                    Some(SsdEntryState::Committed(_))
                )
            {
                evicted.push(ordered.key);
            }
        }
        evicted
    }

    fn commit(&mut self, key: &KvSsdKey, success: bool) -> bool {
        let Some(state) = self.entries.get(key) else {
            return false;
        };
        let entry = match state {
            SsdEntryState::Writing(entry) => entry.clone(),
            SsdEntryState::Committed(_) => return true,
        };
        if !self.is_offset_valid(&entry) || !success {
            self.remove_entry(key);
            return false;
        }
        self.entries
            .insert(key.clone(), SsdEntryState::Committed(entry.clone()));
        self.unmark_busy(&entry);
        true
    }

    fn remove(&mut self, key: &KvSsdKey) -> bool {
        self.remove_entry(key).is_some()
    }

    fn is_offset_valid(&self, entry: &SsdIndexEntry) -> bool {
        self.shards
            .get(entry.shard_id)
            .is_some_and(|shard| entry.begin >= shard.tail)
    }

    fn used_bytes_by_shard(&self) -> Vec<u64> {
        self.shards.iter().map(|shard| shard.used_bytes).collect()
    }

    fn has_busy_entries_before(&self, shard_id: usize, new_tail: u64) -> bool {
        if new_tail <= self.shards[shard_id].tail {
            return false;
        }
        self.shards[shard_id]
            .busy_begins
            .first_key_value()
            .is_some_and(|(begin, _)| *begin < new_tail)
    }
}

struct SsdReadPin {
    inner: Arc<Mutex<KvSsdStorageInner>>,
    space_notify: Arc<Notify>,
    key: KvSsdKey,
}

pub(crate) struct KvSsdPersistGuard {
    _native_pin: Option<SsdReadPin>,
    _foyer_guard: Option<FoyerKvSsdPersistGuard>,
}

impl Drop for SsdReadPin {
    fn drop(&mut self) {
        self.inner.lock().ring.unpin_read(&self.key);
        self.space_notify.notify_one();
    }
}

struct WriteCommand {
    key: KvSsdKey,
    entry_len: u64,
    data: AlignedBuffer,
    priority: KvSsdPersistPriority,
    queued_at: Instant,
    device_permit: SsdDeviceWritePermit,
    done_tx: oneshot::Sender<KvResult<KvSsdPersistGuard>>,
}

struct ReadCommand {
    key: KvSsdKey,
    entry: SsdIndexEntry,
    file_offset: u64,
    target: ReadTarget,
    _admission: SsdReadAdmissionPermit,
    _read_pin: Option<Arc<SsdReadPin>>,
    _target_lifetime: Option<KvSsdReadTargetLifetime>,
    done_tx: oneshot::Sender<KvResult<ReadOutput>>,
}

struct WriteTask {
    key: KvSsdKey,
    entry: SsdIndexEntry,
    data: AlignedBuffer,
    _device_permit: SsdDeviceWritePermit,
    done_tx: oneshot::Sender<KvResult<KvSsdPersistGuard>>,
}

struct SsdWriteRoute {
    tx: tokio_mpsc::Sender<WriteCommand>,
    permit: SsdDeviceWritePermit,
}

struct ReadTask {
    key: KvSsdKey,
    entry: SsdIndexEntry,
    file_offset: u64,
    target: ReadTarget,
    _admission: SsdReadAdmissionPermit,
    _read_pin: Option<Arc<SsdReadPin>>,
    _target_lifetime: Option<KvSsdReadTargetLifetime>,
    done_tx: oneshot::Sender<KvResult<ReadOutput>>,
}

impl From<ReadCommand> for ReadTask {
    fn from(command: ReadCommand) -> Self {
        Self {
            key: command.key,
            entry: command.entry,
            file_offset: command.file_offset,
            target: command.target,
            _admission: command._admission,
            _read_pin: command._read_pin,
            _target_lifetime: command._target_lifetime,
            done_tx: command.done_tx,
        }
    }
}

struct WriteCompletion {
    key: KvSsdKey,
    success: bool,
    result: KvResult<()>,
    done_tx: oneshot::Sender<KvResult<KvSsdPersistGuard>>,
}

struct ReadCompletion {
    key: KvSsdKey,
    entry: SsdIndexEntry,
    result: KvResult<ReadOutput>,
    _admission: SsdReadAdmissionPermit,
    _read_pin: Option<Arc<SsdReadPin>>,
    _target_lifetime: Option<KvSsdReadTargetLifetime>,
    done_tx: oneshot::Sender<KvResult<ReadOutput>>,
}

enum ReadTarget {
    Scratch { len: usize },
    Direct { target_addr: u64, len: usize },
}

struct SparseReadRequest {
    range: SparseGetRange,
    file_offset: u64,
    payload_offset: u64,
    target: ReadTarget,
}

impl ReadTarget {
    fn io_len(&self) -> usize {
        match self {
            Self::Scratch { len } | Self::Direct { len, .. } => *len,
        }
    }
}

#[derive(Debug)]
struct SsdReadAdmission {
    bytes: Arc<Semaphore>,
    ops: Arc<Semaphore>,
    max_bytes: usize,
}

impl SsdReadAdmission {
    fn new(max_bytes: usize, max_ops: usize) -> Self {
        assert!(max_bytes > 0 && max_bytes <= u32::MAX as usize);
        assert!(max_ops > 0);
        Self {
            bytes: Arc::new(Semaphore::new(max_bytes)),
            ops: Arc::new(Semaphore::new(max_ops)),
            max_bytes,
        }
    }

    async fn acquire(&self, io_len: usize) -> SsdReadAdmissionPermit {
        let charged_bytes = io_len.min(self.max_bytes);
        // Take the operation slot first so requests waiting on the ops limit do
        // not consume byte permits and silently grow beyond max_ops.
        let op_permit = Arc::clone(&self.ops)
            .acquire_owned()
            .await
            .expect("kv ssd read op admission semaphore remains open");
        let byte_permit = Arc::clone(&self.bytes)
            .acquire_many_owned(charged_bytes as u32)
            .await
            .expect("kv ssd read byte admission semaphore remains open");
        SsdReadAdmissionPermit {
            _bytes: byte_permit,
            _op: op_permit,
        }
    }
}

struct SsdReadAdmissionPermit {
    _bytes: OwnedSemaphorePermit,
    _op: OwnedSemaphorePermit,
}

enum ReadOutput {
    Scratch(AlignedBuffer),
    Direct,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SsdReadPath {
    Scratch,
    Direct,
}

pub fn safe_path_component(raw: &str) -> String {
    format!("v1-{}", hex::encode(Sha256::digest(raw.as_bytes())))
}

impl KvSsdStorage {
    pub async fn new(init: KvSsdStorageInit) -> KvResult<Self> {
        init.writeback.validate()?;
        if init.root_limits.is_empty() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "kv ssd storage root_limits must contain at least one path".to_string(),
            }));
        }
        for (idx, root_limit) in init.root_limits.iter().enumerate() {
            if root_limit.limit_bytes < SSD_ALIGNMENT as u64 {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "kv ssd storage root_limits[{idx}].limit_bytes must be >= {}",
                        SSD_ALIGNMENT
                    ),
                }));
            }
        }
        let write_config = init.writeback.clone();
        let legacy_write_rate_limiter = match (
            write_config.legacy_rate_limit_bytes_per_sec,
            write_config.legacy_burst_bytes,
        ) {
            (Some(rate), Some(burst)) if rate > 0 && burst > 0 => {
                Some(Mutex::new(LegacyWriteRateLimiter::new(rate, burst)))
            }
            _ => None,
        };
        let write_admission = Arc::new(SsdWriteAdmission::new(&write_config));
        let stats = Arc::new(KvSsdStorageStats::default());

        if init.backend == KvSsdStorageBackend::Foyer {
            if init.root_limits.len() != 1 {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "foyer kv ssd backend requires exactly one root, got {}",
                        init.root_limits.len()
                    ),
                }));
            }
            tracing::warn!(
                "Using test-only Foyer KV SSD backend; proactive SSD eviction notifications are unavailable"
            );
            let root_limit = init
                .root_limits
                .into_iter()
                .next()
                .expect("foyer root count checked above");
            let dummy_capacity = root_limit.limit_bytes;
            let foyer = FoyerKvSsdStorage::new(root_limit).await?;
            let root_dirs = vec![foyer.root_dir().to_path_buf()];
            let (eviction_tx, eviction_rx) = tokio_mpsc::channel(DEFAULT_EVICTION_QUEUE_DEPTH);
            let (shutdown_tx, _) = watch::channel(false);
            return Ok(Self {
                root_dirs,
                devices: Vec::new(),
                shard_to_device: Vec::new(),
                next_write_device: AtomicUsize::new(0),
                write_config,
                legacy_write_rate_limiter,
                write_admission,
                stats,
                read_admission: Arc::new(SsdReadAdmission::new(
                    DEFAULT_READ_MAX_INFLIGHT_BYTES,
                    DEFAULT_READ_MAX_INFLIGHT_OPS,
                )),
                inner: Arc::new(Mutex::new(KvSsdStorageInner {
                    ring: SsdRingBuffer::new(vec![dummy_capacity]),
                })),
                space_notify: Arc::new(Notify::new()),
                shutdown_gate: ShutdownGate::new(),
                shutdown_tx,
                worker_close_state: TokioMutex::new(SsdWorkerCloseState::Open),
                eviction_rx: Mutex::new(Some(eviction_rx)),
                eviction_tx_guard: Mutex::new(Some(eviction_tx)),
                foyer: Some(foyer),
            });
        }

        let device_roots = deduplicate_device_roots(&init.root_limits)?;
        let effective_root_dirs = device_roots
            .iter()
            .map(|root| root.root_dir.clone())
            .collect::<Vec<_>>();
        let shard_specs = build_shard_specs(&device_roots)?;
        let opened_shards = open_cache_files(&device_roots, &shard_specs)?;
        let inner = Arc::new(Mutex::new(KvSsdStorageInner {
            ring: SsdRingBuffer::new(
                shard_specs
                    .iter()
                    .map(|spec| spec.capacity)
                    .collect::<Vec<_>>(),
            ),
        }));
        let space_notify = Arc::new(Notify::new());
        let (eviction_tx, eviction_rx) = tokio_mpsc::channel(DEFAULT_EVICTION_QUEUE_DEPTH);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let shard_count = shard_specs.len();
        let mut shard_to_device = vec![0usize; shard_count];
        let mut device_shards = device_roots
            .iter()
            .map(|root| (root.clone(), Vec::<(usize, std::fs::File)>::new()))
            .collect::<Vec<_>>();
        for opened in opened_shards {
            debug_assert_eq!(
                inner.lock().ring.shards[opened.shard_id].capacity,
                opened.capacity
            );
            shard_to_device[opened.shard_id] = opened.device_idx;
            device_shards[opened.device_idx]
                .1
                .push((opened.shard_id, opened.file));
        }

        let mut devices = Vec::with_capacity(device_shards.len());
        for (device_root, shard_files) in device_shards {
            let shard_ids = shard_files
                .iter()
                .map(|(shard_id, _)| *shard_id)
                .collect::<Vec<_>>();
            let max_shard_capacity = shard_ids
                .iter()
                .filter_map(|shard_id| shard_specs.get(*shard_id))
                .map(|spec| spec.capacity)
                .max()
                .ok_or_else(|| {
                    KvError::Api(ApiError::InvalidArgument {
                        detail: format!(
                            "kv ssd device has no shards: device_id={} root_dir={}",
                            device_root.device_id,
                            device_root.root_dir.display()
                        ),
                    })
                })?;
            let io = Arc::new(UringIoEngine::new_multi(
                shard_files,
                UringConfig {
                    threads: DEFAULT_URING_THREADS,
                    io_depth: DEFAULT_URING_IO_DEPTH,
                    mode: init.uring_mode,
                },
            )?);
            let (required_write_tx, required_write_rx) =
                tokio_mpsc::channel(DEFAULT_WRITE_QUEUE_DEPTH);
            let (early_write_tx, early_write_rx) = tokio_mpsc::channel(DEFAULT_WRITE_QUEUE_DEPTH);
            let (read_tx, read_rx) = tokio_mpsc::channel(DEFAULT_READ_QUEUE_DEPTH);

            let writer_handle = task::spawn(ssd_writer_loop(
                Arc::clone(&inner),
                required_write_rx,
                early_write_rx,
                Arc::clone(&io),
                Arc::clone(&space_notify),
                init.writeback.clone(),
                Arc::clone(&stats),
                shard_ids.clone(),
                eviction_tx.clone(),
                shutdown_rx.clone(),
            ));
            let reader_handle = task::spawn(ssd_reader_loop(
                Arc::clone(&inner),
                read_rx,
                Arc::clone(&io),
                DEFAULT_READ_INFLIGHT,
            ));

            devices.push(SsdDeviceWorker {
                device_id: device_root.device_id,
                root_dir: device_root.root_dir,
                shard_ids,
                max_shard_capacity,
                write_admission: Arc::new(SsdDeviceWriteAdmission::new(&init.writeback)),
                runtime: Mutex::new(Some(SsdDeviceRuntime {
                    io,
                    required_write_tx,
                    early_write_tx,
                    read_tx,
                    writer_handle,
                    reader_handle,
                })),
            });
        }

        Ok(Self {
            root_dirs: effective_root_dirs,
            devices,
            shard_to_device,
            next_write_device: AtomicUsize::new(0),
            write_config,
            legacy_write_rate_limiter,
            write_admission,
            stats,
            read_admission: Arc::new(SsdReadAdmission::new(
                DEFAULT_READ_MAX_INFLIGHT_BYTES,
                DEFAULT_READ_MAX_INFLIGHT_OPS,
            )),
            inner,
            space_notify,
            shutdown_gate: ShutdownGate::new(),
            shutdown_tx,
            worker_close_state: TokioMutex::new(SsdWorkerCloseState::Open),
            eviction_rx: Mutex::new(Some(eviction_rx)),
            eviction_tx_guard: Mutex::new(None),
            foyer: None,
        })
    }

    pub fn root_dirs(&self) -> &[PathBuf] {
        &self.root_dirs
    }

    pub(crate) fn device_usage_snapshot(&self) -> Vec<KvSsdStorageDeviceUsage> {
        if let Some(foyer) = self.foyer.as_ref() {
            return vec![KvSsdStorageDeviceUsage {
                device: format!("foyer:{}", foyer.root_dir().display()),
                capacity_bytes: foyer.capacity_bytes(),
                used_bytes: foyer.logical_used_bytes(),
                ..Default::default()
            }];
        }
        let inner = self.inner.lock();
        let used_by_shard = inner.ring.used_bytes_by_shard();
        self.devices
            .iter()
            .enumerate()
            .map(|(device_idx, device)| {
                let mut capacity_bytes = 0u64;
                let mut used_bytes = 0u64;
                for shard_id in &device.shard_ids {
                    if self
                        .shard_to_device
                        .get(*shard_id)
                        .is_some_and(|mapped| *mapped == device_idx)
                    {
                        let Some(shard) = inner.ring.shards.get(*shard_id) else {
                            continue;
                        };
                        capacity_bytes = capacity_bytes.saturating_add(shard.capacity);
                        used_bytes = used_bytes
                            .saturating_add(used_by_shard.get(*shard_id).copied().unwrap_or(0));
                    }
                }
                KvSsdStorageDeviceUsage {
                    device: format!("dev:{}:{}", device.device_id, device.root_dir.display()),
                    capacity_bytes,
                    used_bytes,
                    ..Default::default()
                }
            })
            .collect()
    }

    pub(crate) fn stats_snapshot(&self) -> KvSsdStorageStatsSnapshot {
        self.stats.snapshot()
    }

    /// Compatibility aggregate used by the existing owner observability and
    /// reclaim paths.  Native storage keeps per-device geometry separate, but
    /// the old API exposed one owner-level record.
    pub(crate) fn usage_snapshot(&self) -> KvSsdStorageDeviceUsage {
        let devices = self.device_usage_snapshot();
        let mut usage = devices
            .first()
            .cloned()
            .unwrap_or_else(|| KvSsdStorageDeviceUsage {
                device: self
                    .root_dirs
                    .first()
                    .map(|root| root.display().to_string())
                    .unwrap_or_default(),
                ..Default::default()
            });
        usage.capacity_bytes = devices.iter().map(|device| device.capacity_bytes).sum();
        usage.used_bytes = devices.iter().map(|device| device.used_bytes).sum();
        let snapshot = self.stats.snapshot();
        usage.persist_requests = snapshot.persist_requests;
        usage.persist_successes = snapshot.persist_successes;
        usage.persist_failures = snapshot.persist_failures;
        usage.persist_bytes = snapshot.persist_bytes;
        usage.persist_duration_us = snapshot.persist_duration_us;
        usage.persist_batch_requests = snapshot.persist_batch_requests;
        usage.persist_batch_items = snapshot.persist_batch_items;
        usage.persist_flush_batches = snapshot.persist_flush_batches;
        usage.persist_busy_batches = snapshot.persist_busy_batches;
        usage.persist_admission_skips = snapshot.persist_admission_skips;
        usage.persist_batch_duration_us = snapshot.persist_batch_duration_us;
        usage.write_candidate_items = snapshot.write_candidate_items;
        usage.write_candidate_bytes = snapshot.write_candidate_bytes;
        usage.write_admitted_items = snapshot.write_admitted_items;
        usage.write_admitted_bytes = snapshot.write_admitted_bytes;
        usage.write_dropped_items = snapshot.write_dropped_items;
        usage.write_dropped_bytes = snapshot.write_dropped_bytes;
        usage
    }

    /// Legacy capacity policy hook.  The native writer always keeps the
    /// last-backing path enabled; the compatibility config can still disable
    /// it for the older owner eviction decision.
    pub(crate) fn capacity_writeback_enabled(&self) -> bool {
        self.write_config.capacity_writeback_enabled
    }

    /// Immediate, no-queue admission for the older owner-local candidate
    /// path.  New callers use `try_acquire_persist_batch`; this shim preserves
    /// the pre-flight byte/rate contract while forwarding to the same stats.
    pub(crate) fn admit_owner_write_candidates(&self, lengths: &[u64]) -> Vec<bool> {
        let now = Instant::now();
        let mut limiter = self
            .legacy_write_rate_limiter
            .as_ref()
            .map(|value| value.lock());
        let mut admitted = vec![false; lengths.len()];
        for (index, len) in lengths.iter().copied().enumerate() {
            if len == 0 || index >= MAX_PERSIST_BATCH_ITEMS {
                continue;
            }
            let allowed = limiter
                .as_mut()
                .is_none_or(|value| value.try_consume(len, now));
            if allowed {
                admitted[index] = true;
            }
        }
        self.stats
            .write_candidate_items
            .fetch_add(lengths.len() as u64, Ordering::Relaxed);
        self.stats.write_candidate_bytes.fetch_add(
            lengths.iter().copied().fold(0u64, u64::saturating_add),
            Ordering::Relaxed,
        );
        let admitted_bytes = lengths
            .iter()
            .zip(admitted.iter())
            .filter_map(|(len, ok)| ok.then_some(*len))
            .fold(0u64, u64::saturating_add);
        let admitted_items = admitted.iter().filter(|ok| **ok).count() as u64;
        self.stats
            .write_admitted_items
            .fetch_add(admitted_items, Ordering::Relaxed);
        self.stats
            .write_admitted_bytes
            .fetch_add(admitted_bytes, Ordering::Relaxed);
        self.stats
            .write_dropped_items
            .fetch_add(lengths.len() as u64 - admitted_items, Ordering::Relaxed);
        self.stats.write_dropped_bytes.fetch_add(
            lengths
                .iter()
                .copied()
                .fold(0u64, u64::saturating_add)
                .saturating_sub(admitted_bytes),
            Ordering::Relaxed,
        );
        admitted
    }

    pub(crate) fn refund_owner_write_admission(&self, lengths: &[u64]) {
        let bytes = lengths.iter().copied().fold(0u64, u64::saturating_add);
        if let Some(limiter) = self.legacy_write_rate_limiter.as_ref() {
            limiter.lock().refund(bytes, Instant::now());
        }
    }

    pub(crate) fn take_eviction_rx(&self) -> Option<tokio_mpsc::Receiver<Vec<SsdReplicaEviction>>> {
        self.eviction_rx.lock().take()
    }

    fn shutdown_guard(&self, operation: &'static str) -> KvResult<ShutdownGuard> {
        self.shutdown_gate.try_guard().ok_or_else(|| {
            KvError::Api(ApiError::SystemShutdown {
                detail: format!("KvSsdStorage is closed; rejecting {operation}"),
            })
        })
    }

    pub(crate) fn stop_admission(&self) {
        self.shutdown_gate.stop_admission();
        self.write_admission.close();
        for device in &self.devices {
            device.write_admission.close();
        }
        self.shutdown_tx.send_replace(true);
    }

    pub async fn close(&self) -> KvResult<()> {
        self.stop_admission();
        self.shutdown_gate.wait_for_quiescence().await;
        let worker_result = {
            let mut state = self.worker_close_state.lock().await;
            if matches!(*state, SsdWorkerCloseState::Open) {
                let runtimes = self
                    .devices
                    .iter()
                    .filter_map(|device| device.runtime.lock().take())
                    .collect::<Vec<_>>();
                self.space_notify.notify_waiters();
                *state =
                    SsdWorkerCloseState::Closing(task::spawn(close_ssd_device_runtimes(runtimes)));
            }

            let result = match &mut *state {
                SsdWorkerCloseState::Closing(handle) => match handle.await {
                    Ok(result) => result,
                    Err(err) => Err(format!("kv ssd worker shutdown task failed: {err}")),
                },
                SsdWorkerCloseState::Closed(result) => result.clone(),
                SsdWorkerCloseState::Open => unreachable!("SSD close task must be installed"),
            };
            *state = SsdWorkerCloseState::Closed(result.clone());
            result
        };

        if let Some(foyer) = self.foyer.as_ref() {
            foyer.close().await?;
        }
        self.eviction_tx_guard.lock().take();

        worker_result.map_err(|detail| KvError::Api(ApiError::Unknown { detail }))
    }

    async fn next_write_route(
        &self,
        entry_len: u64,
        class: KvSsdPersistClass,
    ) -> KvResult<SsdWriteRoute> {
        if self.devices.is_empty() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "kv ssd storage has no active device".to_string(),
            }));
        }
        let aligned_len = align_ssd_io_len(entry_len)?;
        let capacities = self
            .devices
            .iter()
            .map(|device| device.max_shard_capacity)
            .collect::<Vec<_>>();
        let start_idx = self.next_write_device.fetch_add(1, Ordering::Relaxed) % self.devices.len();
        let mut candidates = self
            .devices
            .iter()
            .enumerate()
            .filter_map(|(index, device)| {
                (device.max_shard_capacity >= aligned_len).then_some(index)
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|index| {
            let device = &self.devices[*index];
            (
                device.write_admission.active_bytes.load(Ordering::Acquire),
                (*index + self.devices.len() - start_idx) % self.devices.len(),
            )
        });
        let Some(device_idx) = candidates.into_iter().next() else {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "kv ssd value len={} aligned_len={} exceeds every device shard capacity: {:?}",
                    entry_len, aligned_len, capacities
                ),
            }));
        };
        let device = &self.devices[device_idx];
        let permit = device.write_admission.acquire(class, aligned_len).await?;
        let tx = device
            .runtime
            .lock()
            .as_ref()
            .map(|runtime| match class {
                KvSsdPersistClass::RequiredLastBacking => runtime.required_write_tx.clone(),
                KvSsdPersistClass::BestEffortEarly => runtime.early_write_tx.clone(),
            })
            .ok_or_else(|| {
                KvError::Api(ApiError::SystemShutdown {
                    detail: "kv ssd write worker is closed".to_string(),
                })
            })?;
        Ok(SsdWriteRoute { tx, permit })
    }

    fn read_tx_for_shard(&self, shard_id: usize) -> KvResult<tokio_mpsc::Sender<ReadCommand>> {
        let Some(device_idx) = self.shard_to_device.get(shard_id).copied() else {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!("kv ssd invalid shard id for read: {}", shard_id),
            }));
        };
        let Some(device) = self.devices.get(device_idx) else {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "kv ssd invalid device index for read: shard_id={} device_idx={}",
                    shard_id, device_idx
                ),
            }));
        };
        if !device.shard_ids.contains(&shard_id) {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "kv ssd shard/device route mismatch: shard_id={} device_idx={} device_id={} root_dir={}",
                    shard_id,
                    device_idx,
                    device.device_id,
                    device.root_dir.display()
                ),
            }));
        }
        device
            .runtime
            .lock()
            .as_ref()
            .map(|runtime| runtime.read_tx.clone())
            .ok_or_else(|| {
                KvError::Api(ApiError::SystemShutdown {
                    detail: "kv ssd read worker is closed".to_string(),
                })
            })
    }

    pub(crate) fn write_batch_limits(&self) -> (u64, usize) {
        (
            self.write_config.batch_max_bytes,
            self.write_config
                .batch_max_items
                .min(MAX_PERSIST_BATCH_ITEMS),
        )
    }

    pub(crate) fn max_early_persist_batches(&self) -> usize {
        self.write_config.max_inflight_batches - 1
    }

    fn persist_batch_charge(&self, lengths: &[u64]) -> KvResult<u32> {
        if lengths.is_empty() || lengths.len() > MAX_PERSIST_BATCH_ITEMS {
            return Err(persist_batch_size_error(lengths.len()));
        }
        let mut total = 0u64;
        for len in lengths {
            if *len == 0 {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: "kv ssd persist batch value len must be positive".to_string(),
                }));
            }
            total = total.checked_add(align_ssd_io_len(*len)?).ok_or_else(|| {
                KvError::Api(ApiError::InvalidArgument {
                    detail: "kv ssd persist batch aligned byte count overflowed".to_string(),
                })
            })?;
        }
        Ok(total.min(u64::from(self.write_admission.max_total_bytes)) as u32)
    }

    fn persist_batch_permit(
        &self,
        class: KvSsdPersistClass,
        item_count: usize,
        charged_bytes: u32,
        total_batch: OwnedSemaphorePermit,
        early_batch: Option<OwnedSemaphorePermit>,
        total_bytes: OwnedSemaphorePermit,
        early_bytes: Option<OwnedSemaphorePermit>,
    ) -> KvSsdPersistBatchPermit {
        KvSsdPersistBatchPermit {
            _total_batch: total_batch,
            _early_batch: early_batch,
            _total_bytes: total_bytes,
            _early_bytes: early_bytes,
            item_count,
            charged_bytes,
            class,
        }
    }

    fn record_write_candidates(&self, lengths: &[u64]) {
        self.stats
            .write_candidate_items
            .fetch_add(lengths.len() as u64, Ordering::Relaxed);
        self.stats.write_candidate_bytes.fetch_add(
            lengths.iter().copied().fold(0u64, u64::saturating_add),
            Ordering::Relaxed,
        );
    }

    fn record_write_admission(&self, lengths: &[u64], admitted: bool) {
        let bytes = lengths.iter().copied().fold(0u64, u64::saturating_add);
        if admitted {
            self.stats
                .write_admitted_items
                .fetch_add(lengths.len() as u64, Ordering::Relaxed);
            self.stats
                .write_admitted_bytes
                .fetch_add(bytes, Ordering::Relaxed);
        } else {
            self.stats
                .persist_admission_skips
                .fetch_add(1, Ordering::Relaxed);
            self.stats
                .write_dropped_items
                .fetch_add(lengths.len() as u64, Ordering::Relaxed);
            self.stats
                .write_dropped_bytes
                .fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Copy exact source bytes into aligned, owner-owned buffers. Callers hold
    /// the local segment range guard while this method reads the raw address.
    pub(crate) fn copy_batch_from_addrs(
        sources: &[KvSsdPersistSource],
    ) -> Vec<KvResult<KvSsdPersistCopy>> {
        sources
            .iter()
            .map(|source| {
                validate_key(&source.key)?;
                let len = usize::try_from(source.len).map_err(|_| {
                    KvError::Api(ApiError::InvalidArgument {
                        detail: format!(
                            "kv ssd persist copy len does not fit usize: {}",
                            source.len
                        ),
                    })
                })?;
                if len == 0 {
                    return Err(KvError::Api(ApiError::InvalidArgument {
                        detail: "kv ssd persist copy len must be positive".to_string(),
                    }));
                }
                let aligned_len = align_up_usize(len, SSD_ALIGNMENT)?;
                let data = unsafe { AlignedBuffer::copy_from_addr(source.addr, len, aligned_len)? };
                Ok(KvSsdPersistCopy {
                    key: source.key.clone(),
                    put_id: source.put_id,
                    len: source.len,
                    data,
                    priority: source.priority.clone(),
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
        let lengths = sources.iter().map(|source| source.len).collect::<Vec<_>>();
        let class = if sources
            .iter()
            .any(|source| source.priority.class() == KvSsdPersistClass::RequiredLastBacking)
        {
            KvSsdPersistClass::RequiredLastBacking
        } else {
            KvSsdPersistClass::BestEffortEarly
        };
        let permit = match class {
            KvSsdPersistClass::RequiredLastBacking => {
                match self.acquire_persist_batch(class, &lengths).await {
                    Ok(permit) => permit,
                    Err(err) => {
                        let detail = err.to_string();
                        return sources
                            .iter()
                            .map(|_| {
                                Err(KvError::Api(ApiError::Unknown {
                                    detail: detail.clone(),
                                }))
                            })
                            .collect::<Vec<_>>();
                    }
                }
            }
            KvSsdPersistClass::BestEffortEarly => {
                match self.try_acquire_persist_batch(class, &lengths) {
                    Ok(Some(permit)) => permit,
                    Ok(None) => return sources.iter().map(|_| Ok(None)).collect(),
                    Err(err) => {
                        let detail = err.to_string();
                        return sources
                            .iter()
                            .map(|_| {
                                Err(KvError::Api(ApiError::Unknown {
                                    detail: detail.clone(),
                                }))
                            })
                            .collect();
                    }
                }
            }
        };
        self.persist_batch_from_copies_with_permit(permit, Self::copy_batch_from_addrs(sources))
            .await
    }

    pub(crate) fn try_acquire_persist_batch(
        &self,
        class: KvSsdPersistClass,
        lengths: &[u64],
    ) -> KvResult<Option<KvSsdPersistBatchPermit>> {
        let charged_bytes = self.persist_batch_charge(lengths)?;
        self.record_write_candidates(lengths);
        if !self.shutdown_gate.is_accepting() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "KvSsdStorage is closed; rejecting persist batch".to_string(),
            }));
        }
        let Ok(total_batch) = Arc::clone(&self.write_admission.total_batches).try_acquire_owned()
        else {
            self.record_write_admission(lengths, false);
            return Ok(None);
        };
        let early_batch = if class == KvSsdPersistClass::BestEffortEarly {
            let Ok(permit) = Arc::clone(&self.write_admission.early_batches).try_acquire_owned()
            else {
                self.record_write_admission(lengths, false);
                return Ok(None);
            };
            Some(permit)
        } else {
            None
        };
        let Ok(total_bytes) =
            Arc::clone(&self.write_admission.total_bytes).try_acquire_many_owned(charged_bytes)
        else {
            self.record_write_admission(lengths, false);
            return Ok(None);
        };
        let early_bytes = if class == KvSsdPersistClass::BestEffortEarly {
            let early_charge = charged_bytes.min(self.write_admission.max_early_bytes);
            let Ok(permit) =
                Arc::clone(&self.write_admission.early_bytes).try_acquire_many_owned(early_charge)
            else {
                self.record_write_admission(lengths, false);
                return Ok(None);
            };
            Some(permit)
        } else {
            None
        };
        self.record_write_admission(lengths, true);
        Ok(Some(self.persist_batch_permit(
            class,
            lengths.len(),
            charged_bytes,
            total_batch,
            early_batch,
            total_bytes,
            early_bytes,
        )))
    }

    pub(crate) async fn acquire_persist_batch(
        &self,
        class: KvSsdPersistClass,
        lengths: &[u64],
    ) -> KvResult<KvSsdPersistBatchPermit> {
        let charged_bytes = self.persist_batch_charge(lengths)?;
        self.record_write_candidates(lengths);
        if !self.shutdown_gate.is_accepting() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "KvSsdStorage is closed; rejecting persist batch".to_string(),
            }));
        }
        // Early waiters reserve their own quota first, leaving the required
        // batch and byte reserve available while early traffic is saturated.
        let (early_batch, early_bytes) = match class {
            KvSsdPersistClass::BestEffortEarly => {
                let batch = Arc::clone(&self.write_admission.early_batches)
                    .acquire_owned()
                    .await
                    .map_err(|_| {
                        KvError::Api(ApiError::SystemShutdown {
                            detail: "KvSsdStorage early batch admission is closed".to_string(),
                        })
                    })?;
                let bytes = Arc::clone(&self.write_admission.early_bytes)
                    .acquire_many_owned(charged_bytes.min(self.write_admission.max_early_bytes))
                    .await
                    .map_err(|_| {
                        KvError::Api(ApiError::SystemShutdown {
                            detail: "KvSsdStorage early byte admission is closed".to_string(),
                        })
                    })?;
                (Some(batch), Some(bytes))
            }
            KvSsdPersistClass::RequiredLastBacking => (None, None),
        };
        let total_batch = Arc::clone(&self.write_admission.total_batches)
            .acquire_owned()
            .await
            .map_err(|_| {
                KvError::Api(ApiError::SystemShutdown {
                    detail: "KvSsdStorage batch admission is closed".to_string(),
                })
            })?;
        let total_bytes = Arc::clone(&self.write_admission.total_bytes)
            .acquire_many_owned(charged_bytes)
            .await
            .map_err(|_| {
                KvError::Api(ApiError::SystemShutdown {
                    detail: "KvSsdStorage byte admission is closed".to_string(),
                })
            })?;
        self.record_write_admission(lengths, true);
        Ok(self.persist_batch_permit(
            class,
            lengths.len(),
            charged_bytes,
            total_batch,
            early_batch,
            total_bytes,
            early_bytes,
        ))
    }

    pub(crate) async fn persist_batch_from_copies_with_permit(
        &self,
        permit: KvSsdPersistBatchPermit,
        copies: Vec<KvResult<KvSsdPersistCopy>>,
    ) -> Vec<KvResult<Option<KvSsdPersistGuard>>> {
        let item_count = copies.len();
        if item_count != permit.item_count
            || item_count == 0
            || item_count > MAX_PERSIST_BATCH_ITEMS
        {
            return copies
                .into_iter()
                .map(|copy| {
                    copy.and_then(|_| {
                        Err(KvError::Api(ApiError::InvalidArgument {
                            detail: format!(
                                "kv ssd persist batch permit mismatch: permitted={} actual={} max={}",
                                permit.item_count, item_count, MAX_PERSIST_BATCH_ITEMS
                            ),
                        }))
                    })
                })
                .collect();
        }

        debug_assert!(permit.charged_bytes > 0);
        let batch_started = Instant::now();
        let item_lengths = copies
            .iter()
            .map(|copy| copy.as_ref().ok().map(|copy| copy.len).unwrap_or(0))
            .collect::<Vec<_>>();
        self.stats
            .persist_batch_requests
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .persist_batch_items
            .fetch_add(item_count as u64, Ordering::Relaxed);
        self.stats
            .persist_requests
            .fetch_add(item_count as u64, Ordering::Relaxed);

        let _shutdown_guard = match self.shutdown_guard("persist batch") {
            Ok(guard) => guard,
            Err(_) => {
                return copies
                    .into_iter()
                    .map(|copy| {
                        copy.and_then(|_| {
                            Err(KvError::Api(ApiError::SystemShutdown {
                                detail: "KvSsdStorage is closed; rejecting persist batch"
                                    .to_string(),
                            }))
                        })
                    })
                    .collect();
            }
        };

        let mut results = std::iter::repeat_with(|| None)
            .take(item_count)
            .collect::<Vec<Option<KvResult<Option<KvSsdPersistGuard>>>>>();
        let mut generations = HashSet::with_capacity(item_count);
        let mut inflight = FuturesUnordered::new();
        for (index, copy) in copies.into_iter().enumerate() {
            let KvSsdPersistCopy {
                key,
                put_id,
                len,
                data,
                priority,
            } = match copy {
                Ok(copy) => copy,
                Err(err) => {
                    results[index] = Some(Err(err));
                    continue;
                }
            };
            let generation = KvSsdKey {
                key: key.clone(),
                put_id,
            };
            if !generations.insert(generation) {
                results[index] = Some(Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "kv ssd persist batch contains duplicate generation: key={} put_id=({},{})",
                        key, put_id.0, put_id.1
                    ),
                })));
                continue;
            }
            if permit.class == KvSsdPersistClass::RequiredLastBacking {
                priority.promote_required();
            }

            inflight.push(async move {
                let result = if let Some(foyer) = self.foyer.as_ref() {
                    let len_usize = usize::try_from(len).map_err(|_| {
                        KvError::Api(ApiError::InvalidArgument {
                            detail: format!("foyer kv ssd persist len does not fit usize: {len}"),
                        })
                    })?;
                    let bytes = unsafe { std::slice::from_raw_parts(data.as_ptr(), len_usize) };
                    foyer
                        .persist(&key, put_id, bytes)
                        .await
                        .map(|guard| KvSsdPersistGuard {
                            _native_pin: None,
                            _foyer_guard: Some(guard),
                        })
                } else {
                    self.persist_buffer(&key, put_id, len, data, priority).await
                };
                Ok::<_, KvError>((index, result.map(Some)))
            });
        }

        while let Some(completion) = inflight.next().await {
            match completion {
                Ok((index, result)) => results[index] = Some(result),
                Err(err) => {
                    debug_assert!(false, "unexpected persist batch wrapper error: {err}");
                }
            }
        }
        let completed = results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(KvError::Api(ApiError::Unknown {
                        detail: "kv ssd persist batch result was not populated".to_string(),
                    }))
                })
            })
            .collect::<Vec<_>>();
        let mut successes = 0u64;
        let mut success_bytes = 0u64;
        for (result, len) in completed.iter().zip(item_lengths) {
            if matches!(result, Ok(Some(_))) {
                successes += 1;
                success_bytes = success_bytes.saturating_add(len);
            }
        }
        self.stats
            .persist_successes
            .fetch_add(successes, Ordering::Relaxed);
        self.stats
            .persist_failures
            .fetch_add(item_count as u64 - successes, Ordering::Relaxed);
        self.stats
            .persist_bytes
            .fetch_add(success_bytes, Ordering::Relaxed);
        let elapsed_us = batch_started.elapsed().as_micros() as u64;
        self.stats
            .persist_duration_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
        self.stats
            .persist_batch_duration_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
        drop(permit);
        completed
    }

    pub(crate) async fn remove_exact(&self, key: &str, put_id: PutIDForAKey) -> bool {
        let Ok(_shutdown_guard) = self.shutdown_guard("remove_exact") else {
            return false;
        };
        let Ok(_batch_guard) = Arc::clone(&self.write_admission.total_batches)
            .acquire_many_owned(self.write_admission.max_batches)
            .await
        else {
            return false;
        };
        if let Some(foyer) = self.foyer.as_ref() {
            return foyer.remove_exact(key, put_id).await;
        }
        self.inner.lock().ring.remove(&KvSsdKey {
            key: key.to_string(),
            put_id,
        })
    }

    pub(crate) async fn persist_from_addr(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        addr: u64,
        len: u64,
    ) -> KvResult<KvSsdPersistGuard> {
        let _shutdown_guard = self.shutdown_guard("persist_from_addr")?;
        validate_key(key)?;
        if let Some(foyer) = self.foyer.as_ref() {
            return foyer
                .persist_from_addr(key, put_id, addr, len)
                .await
                .map(|guard| KvSsdPersistGuard {
                    _native_pin: None,
                    _foyer_guard: Some(guard),
                });
        }
        let len_usize = usize::try_from(len).map_err(|_| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!("kv ssd persist len does not fit usize: {}", len),
            })
        })?;
        let aligned_len = align_up_usize(len_usize, SSD_ALIGNMENT)?;
        let data = unsafe { AlignedBuffer::copy_from_addr(addr, len_usize, aligned_len)? };
        self.persist_buffer(
            key,
            put_id,
            len,
            data,
            KvSsdPersistPriority::new(KvSsdPersistClass::RequiredLastBacking),
        )
        .await
    }

    pub async fn persist(&self, key: &str, put_id: PutIDForAKey, data: &[u8]) -> KvResult<()> {
        let _shutdown_guard = self.shutdown_guard("persist")?;
        validate_key(key)?;
        if let Some(foyer) = self.foyer.as_ref() {
            let guard = foyer.persist(key, put_id, data).await?;
            drop(guard);
            return Ok(());
        }
        let aligned_len = align_up_usize(data.len(), SSD_ALIGNMENT)?;
        let mut buffer = AlignedBuffer::zeroed(aligned_len)?;
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), buffer.as_mut_ptr(), data.len());
        }
        let guard = self
            .persist_buffer(
                key,
                put_id,
                data.len() as u64,
                buffer,
                KvSsdPersistPriority::new(KvSsdPersistClass::RequiredLastBacking),
            )
            .await?;
        drop(guard);
        Ok(())
    }

    async fn persist_buffer(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        entry_len: u64,
        data: AlignedBuffer,
        priority: KvSsdPersistPriority,
    ) -> KvResult<KvSsdPersistGuard> {
        let (done_tx, done_rx) = oneshot::channel();
        let class = priority.class();
        let route = self.next_write_route(entry_len, class).await?;
        route
            .tx
            .send(WriteCommand {
                key: KvSsdKey {
                    key: key.to_string(),
                    put_id,
                },
                entry_len,
                data,
                priority,
                queued_at: Instant::now(),
                device_permit: route.permit,
                done_tx,
            })
            .await
            .map_err(|err| {
                KvError::Api(ApiError::InvalidArgument {
                    detail: format!("kv ssd write queue closed: {}", err),
                })
            })?;
        done_rx.await.map_err(|err| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!("kv ssd write completion closed: {}", err),
            })
        })?
    }

    pub async fn load_into_addr(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        target_addr: u64,
        len: u64,
        target_len: u64,
    ) -> KvResult<()> {
        let _shutdown_guard = self.shutdown_guard("load_into_addr")?;
        validate_key(key)?;
        if let Some(foyer) = self.foyer.as_ref() {
            return foyer
                .load_into_addr(key, put_id, target_addr, len, target_len)
                .await;
        }
        if target_len < len {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "kv ssd target capacity too small for key={} put_id=({},{}) len={} target_len={}",
                    key, put_id.0, put_id.1, len, target_len
                ),
            }));
        }
        let key = KvSsdKey {
            key: key.to_string(),
            put_id,
        };
        let (entry, read_pin) = {
            let mut inner = self.inner.lock();
            let Some(entry) = inner.ring.pin_read(&key) else {
                return Err(KvError::Api(ApiError::KeyNotFound {
                    key: key.key.clone(),
                }));
            };
            (
                entry,
                SsdReadPin {
                    inner: Arc::clone(&self.inner),
                    space_notify: Arc::clone(&self.space_notify),
                    key: key.clone(),
                },
            )
        };
        if entry.len != len {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "kv ssd length mismatch for key={} put_id=({},{}) expected={} actual={}",
                    key.key, put_id.0, put_id.1, len, entry.len
                ),
            }));
        }

        let len_usize = usize::try_from(len).map_err(|_| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!("kv ssd load len does not fit usize: {}", len),
            })
        })?;
        let aligned_len_usize = usize::try_from(entry.aligned_len).map_err(|_| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "kv ssd aligned load len does not fit usize: {}",
                    entry.aligned_len
                ),
            })
        })?;
        let target = match choose_read_path(&entry, target_addr, len, target_len) {
            SsdReadPath::Direct => ReadTarget::Direct {
                target_addr,
                len: aligned_len_usize,
            },
            SsdReadPath::Scratch => ReadTarget::Scratch {
                len: aligned_len_usize,
            },
        };
        let output = self
            .submit_read_command(
                key,
                entry.clone(),
                entry.file_offset,
                target,
                Some(Arc::new(read_pin)),
                None,
            )
            .await?;
        if let ReadOutput::Scratch(buffer) = output {
            unsafe {
                std::ptr::copy_nonoverlapping(buffer.as_ptr(), target_addr as *mut u8, len_usize);
            }
        }
        Ok(())
    }

    /// Materialize selected object byte ranges into one compact caller-owned
    /// staging allocation.  Native O_DIRECT reads are widened to 512-byte
    /// windows and copied from aligned scratch buffers, while one read pin is
    /// retained across the complete range batch.
    pub(crate) async fn load_ranges_into_addr(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        object_len: u64,
        target_addr: u64,
        target_len: u64,
        ranges: &[SparseGetRange],
        max_inflight: usize,
        target_lifetime: KvSsdReadTargetLifetime,
    ) -> KvResult<KvSsdRangeLoadStats> {
        let _shutdown_guard = self.shutdown_guard("load_ranges_into_addr")?;
        validate_key(key)?;
        if self.foyer.is_some() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "sparse physical range Get is unsupported by the Foyer SSD backend"
                    .to_string(),
            }));
        }
        if ranges.is_empty() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "sparse SSD range Get requires at least one range".to_string(),
            }));
        }
        if max_inflight == 0 {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "sparse SSD range Get max_inflight must be > 0".to_string(),
            }));
        }
        let mut destination_windows = Vec::with_capacity(ranges.len());
        for (index, range) in ranges.iter().enumerate() {
            if range.len == 0 {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!("sparse SSD range has zero len: index={index}"),
                }));
            }
            let src_end = checked_add_u64(range.src_offset, range.len, "sparse source end")?;
            let dst_end = checked_add_u64(range.dst_offset, range.len, "sparse target end")?;
            if src_end > object_len || dst_end > target_len {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "sparse SSD range is out of bounds: index={} src_end={} object_len={} dst_end={} target_len={}",
                        index, src_end, object_len, dst_end, target_len
                    ),
                }));
            }
            destination_windows.push((range.dst_offset, dst_end));
        }
        destination_windows.sort_unstable();
        if destination_windows
            .windows(2)
            .any(|pair| pair[0].1 > pair[1].0)
        {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "sparse SSD destination ranges must not overlap".to_string(),
            }));
        }

        let service_started_at = Instant::now();
        let key = KvSsdKey {
            key: key.to_string(),
            put_id,
        };
        let (entry, batch_read_pin) = {
            let mut inner = self.inner.lock();
            let Some(entry) = inner.ring.pin_read(&key) else {
                return Err(KvError::Api(ApiError::KeyNotFound {
                    key: key.key.clone(),
                }));
            };
            (
                entry,
                Arc::new(SsdReadPin {
                    inner: Arc::clone(&self.inner),
                    space_notify: Arc::clone(&self.space_notify),
                    key: key.clone(),
                }),
            )
        };
        if entry.len != object_len {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "kv ssd sparse length mismatch for key={} put_id=({},{}) expected={} actual={}",
                    key.key, put_id.0, put_id.1, object_len, entry.len
                ),
            }));
        }

        // Finish all geometry validation before submitting the first read. This
        // prevents a late malformed range from turning validation failure into
        // a partially executed sparse Get.
        let mut requests = Vec::with_capacity(ranges.len());
        let mut physical_bytes = 0u64;
        for range in ranges.iter().copied() {
            let destination =
                checked_add_u64(target_addr, range.dst_offset, "sparse direct target addr")?;
            let direct_file_offset = checked_add_u64(
                entry.file_offset,
                range.src_offset,
                "sparse direct file offset",
            )?;
            let target_capacity = target_len
                .checked_sub(range.dst_offset)
                .expect("sparse target bounds were validated above");
            let (file_offset, payload_offset, target) = if choose_chunk_read_path(
                destination,
                range.len,
                target_capacity,
                direct_file_offset,
            ) == SsdReadPath::Direct
            {
                (
                    direct_file_offset,
                    0,
                    ReadTarget::Direct {
                        target_addr: destination,
                        len: usize::try_from(range.len).map_err(|_| {
                            KvError::Api(ApiError::InvalidArgument {
                                detail: "sparse SSD direct read length does not fit usize"
                                    .to_string(),
                            })
                        })?,
                    },
                )
            } else {
                let (aligned_start, read_len, payload_offset) =
                    sparse_read_window(range.src_offset, range.len, entry.aligned_len)?;
                let file_offset = checked_add_u64(
                    entry.file_offset,
                    aligned_start,
                    "sparse scratch file offset",
                )?;
                let target = ReadTarget::Scratch {
                    len: usize::try_from(read_len).map_err(|_| {
                        KvError::Api(ApiError::InvalidArgument {
                            detail: "sparse SSD aligned read length does not fit usize".to_string(),
                        })
                    })?,
                };
                (file_offset, payload_offset, target)
            };
            physical_bytes = physical_bytes
                .checked_add(u64::try_from(target.io_len()).map_err(|_| {
                    KvError::Api(ApiError::InvalidArgument {
                        detail: "sparse SSD physical read length does not fit u64".to_string(),
                    })
                })?)
                .ok_or_else(|| {
                    KvError::Api(ApiError::InvalidArgument {
                        detail: "sparse SSD physical byte count overflow".to_string(),
                    })
                })?;
            requests.push(SparseReadRequest {
                range,
                file_offset,
                payload_offset,
                target,
            });
        }

        let submit_request = |request: SparseReadRequest| {
            let key = key.clone();
            let entry = entry.clone();
            let read_pin = Arc::clone(&batch_read_pin);
            let target_lifetime = target_lifetime.clone();
            async move {
                let output = self
                    .submit_read_command(
                        key,
                        entry,
                        request.file_offset,
                        request.target,
                        Some(read_pin),
                        Some(target_lifetime),
                    )
                    .await?;
                Ok::<_, KvError>((request.range, request.payload_offset, output))
            }
        };
        let result =
            run_bounded_sparse_reads(requests, max_inflight, target_addr, submit_request).await;
        // Every submitted read must reach terminal before the shared generation
        // pin can be released and the caller may reclaim its staging allocation.
        drop(batch_read_pin);
        result?;
        Ok(KvSsdRangeLoadStats {
            service_us: service_started_at
                .elapsed()
                .as_micros()
                .min(u64::MAX as u128) as u64,
            physical_bytes,
        })
    }

    pub(crate) async fn load_into_addr_chunks(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        target_addr: u64,
        len: u64,
        target_len: u64,
        chunk_bytes: u64,
        max_read_inflight: usize,
        ready_tx: tokio_mpsc::Sender<SsdLoadedChunk>,
    ) -> KvResult<()> {
        let _shutdown_guard = self.shutdown_guard("load_into_addr_chunks")?;
        validate_key(key)?;
        if let Some(foyer) = self.foyer.as_ref() {
            return foyer
                .load_into_addr_chunks(
                    key,
                    put_id,
                    target_addr,
                    len,
                    target_len,
                    chunk_bytes,
                    max_read_inflight,
                    ready_tx,
                )
                .await;
        }
        if target_len < len {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "kv ssd target capacity too small for chunked load: key={} put_id=({},{}) len={} target_len={}",
                    key, put_id.0, put_id.1, len, target_len
                ),
            }));
        }
        let chunk_bytes = align_up_u64(chunk_bytes.max(1), SSD_ALIGNMENT as u64)?;
        let key = KvSsdKey {
            key: key.to_string(),
            put_id,
        };
        let (entry, batch_read_pin) = {
            let mut inner = self.inner.lock();
            let Some(entry) = inner.ring.pin_read(&key) else {
                return Err(KvError::Api(ApiError::KeyNotFound {
                    key: key.key.clone(),
                }));
            };
            (
                entry,
                Arc::new(SsdReadPin {
                    inner: Arc::clone(&self.inner),
                    space_notify: Arc::clone(&self.space_notify),
                    key: key.clone(),
                }),
            )
        };
        if entry.len != len {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "kv ssd length mismatch for chunked load: key={} put_id=({},{}) expected={} actual={}",
                    key.key, put_id.0, put_id.1, len, entry.len
                ),
            }));
        }

        let mut next_offset = 0u64;
        let mut inflight = FuturesUnordered::new();
        let max_read_inflight = max_read_inflight.max(1);
        let mut first_error = None;

        loop {
            while first_error.is_none() && next_offset < len && inflight.len() < max_read_inflight {
                let payload_len = chunk_bytes.min(len - next_offset);
                let stage_addr = match checked_add_u64(target_addr, next_offset, "chunk stage addr")
                {
                    Ok(stage_addr) => stage_addr,
                    Err(err) => {
                        first_error = Some(err);
                        break;
                    }
                };
                let remaining_target_len = target_len - next_offset;
                inflight.push(self.load_entry_range_into_addr(
                    key.clone(),
                    entry.clone(),
                    next_offset,
                    payload_len,
                    stage_addr,
                    remaining_target_len,
                    Arc::clone(&batch_read_pin),
                ));
                next_offset += payload_len;
            }

            let Some(chunk) = inflight.next().await else {
                break;
            };
            match chunk {
                Ok(chunk) if first_error.is_none() => {
                    if let Err(err) = ready_tx.send(chunk).await {
                        first_error = Some(KvError::Api(ApiError::InvalidArgument {
                            detail: format!("kv ssd chunk ready queue closed: {err}"),
                        }));
                    }
                }
                Ok(_) => {}
                Err(err) => {
                    first_error.get_or_insert(err);
                }
            }
        }
        if let Some(err) = first_error {
            Err(err)
        } else {
            Ok(())
        }
    }

    async fn load_entry_range_into_addr(
        &self,
        key: KvSsdKey,
        entry: SsdIndexEntry,
        offset: u64,
        payload_len: u64,
        target_addr: u64,
        target_len: u64,
        read_pin: Arc<SsdReadPin>,
    ) -> KvResult<SsdLoadedChunk> {
        if payload_len == 0 {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "kv ssd chunk payload len must be positive".to_string(),
            }));
        }
        let payload_end = checked_add_u64(offset, payload_len, "chunk payload end")?;
        if payload_end > entry.len {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "kv ssd chunk exceeds entry len: offset={} len={} entry_len={}",
                    offset, payload_len, entry.len
                ),
            }));
        }
        let read_len = align_up_u64(payload_len, SSD_ALIGNMENT as u64)?;
        let read_end = checked_add_u64(offset, read_len, "chunk read end")?;
        if read_end > entry.aligned_len {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "kv ssd aligned chunk exceeds entry aligned len: offset={} read_len={} aligned_len={}",
                    offset, read_len, entry.aligned_len
                ),
            }));
        }
        if target_len < read_len {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "kv ssd chunk target capacity too small: offset={} read_len={} target_len={}",
                    offset, read_len, target_len
                ),
            }));
        }
        let file_offset = checked_add_u64(entry.file_offset, offset, "chunk file offset")?;
        let read_len_usize = usize::try_from(read_len).map_err(|_| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!("kv ssd chunk read len does not fit usize: {}", read_len),
            })
        })?;
        let payload_len_usize = usize::try_from(payload_len).map_err(|_| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "kv ssd chunk payload len does not fit usize: {}",
                    payload_len
                ),
            })
        })?;
        let target = match choose_chunk_read_path(target_addr, read_len, target_len, file_offset) {
            SsdReadPath::Direct => ReadTarget::Direct {
                target_addr,
                len: read_len_usize,
            },
            SsdReadPath::Scratch => ReadTarget::Scratch {
                len: read_len_usize,
            },
        };
        let output = self
            .submit_read_command(key, entry, file_offset, target, Some(read_pin), None)
            .await?;
        if let ReadOutput::Scratch(buffer) = output {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    buffer.as_ptr(),
                    target_addr as *mut u8,
                    payload_len_usize,
                );
            }
        }
        Ok(SsdLoadedChunk {
            offset,
            stage_addr: target_addr,
            len: payload_len,
        })
    }

    async fn submit_read_command(
        &self,
        key: KvSsdKey,
        entry: SsdIndexEntry,
        file_offset: u64,
        target: ReadTarget,
        read_pin: Option<Arc<SsdReadPin>>,
        target_lifetime: Option<KvSsdReadTargetLifetime>,
    ) -> KvResult<ReadOutput> {
        let admission = self.read_admission.acquire(target.io_len()).await;
        let (done_tx, done_rx) = oneshot::channel();
        let read_tx = self.read_tx_for_shard(entry.shard_id)?;
        read_tx
            .send(ReadCommand {
                key,
                entry,
                file_offset,
                target,
                _admission: admission,
                _read_pin: read_pin,
                _target_lifetime: target_lifetime,
                done_tx,
            })
            .await
            .map_err(|err| {
                KvError::Api(ApiError::InvalidArgument {
                    detail: format!("kv ssd read queue closed: {}", err),
                })
            })?;
        done_rx.await.map_err(|err| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!("kv ssd read completion closed: {}", err),
            })
        })?
    }

    #[cfg(test)]
    async fn has_entry(&self, key: &str, put_id: PutIDForAKey) -> bool {
        if let Some(foyer) = self.foyer.as_ref() {
            return foyer.contains(key, put_id);
        }
        let key = KvSsdKey {
            key: key.to_string(),
            put_id,
        };
        self.inner.lock().ring.get(&key).is_some()
    }
}

async fn close_ssd_device_runtimes(runtimes: Vec<SsdDeviceRuntime>) -> Result<(), String> {
    let mut joins = FuturesUnordered::new();
    let mut io_engines = Vec::with_capacity(runtimes.len());
    for runtime in runtimes {
        let SsdDeviceRuntime {
            io,
            required_write_tx,
            early_write_tx,
            read_tx,
            writer_handle,
            reader_handle,
        } = runtime;
        drop(required_write_tx);
        drop(early_write_tx);
        drop(read_tx);
        joins.push(writer_handle);
        joins.push(reader_handle);
        io_engines.push(io);
    }

    let mut worker_error = None;
    while let Some(result) = joins.next().await {
        if let Err(err) = result {
            worker_error.get_or_insert_with(|| err.to_string());
        }
    }

    task::spawn_blocking(move || drop(io_engines))
        .await
        .map_err(|err| format!("kv ssd io_uring shutdown task failed: {err}"))?;
    worker_error.map_or(Ok(()), |detail| {
        Err(format!("kv ssd worker failed during shutdown: {detail}"))
    })
}

fn prepare_ssd_write(
    inner: &Arc<Mutex<KvSsdStorageInner>>,
    space_notify: &Arc<Notify>,
    key: &KvSsdKey,
    entry_len: u64,
    shard_ids: &[usize],
) -> (KvResult<SsdPreparedWrite>, Option<KvSsdPersistGuard>) {
    let (prepared, existing_pinned) = {
        let mut inner = inner.lock();
        let prepared = inner
            .ring
            .prepare_write_on_shards(key.clone(), entry_len, shard_ids);
        let existing_pinned = matches!(&prepared, Ok(SsdPreparedWrite::Existing))
            && inner.ring.pin_read(key).is_some();
        (prepared, existing_pinned)
    };
    let guard = existing_pinned.then(|| KvSsdPersistGuard {
        _native_pin: Some(SsdReadPin {
            inner: Arc::clone(inner),
            space_notify: Arc::clone(space_notify),
            key: key.clone(),
        }),
        _foyer_guard: None,
    });
    (prepared, guard)
}

async fn ssd_writer_loop(
    inner: Arc<Mutex<KvSsdStorageInner>>,
    mut required_rx: tokio_mpsc::Receiver<WriteCommand>,
    mut early_rx: tokio_mpsc::Receiver<WriteCommand>,
    io: Arc<UringIoEngine>,
    space_notify: Arc<Notify>,
    config: KvSsdWritebackConfig,
    stats: Arc<KvSsdStorageStats>,
    shard_ids: Vec<usize>,
    eviction_tx: tokio_mpsc::Sender<Vec<SsdReplicaEviction>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut required_pending = VecDeque::new();
    let mut early_pending = VecDeque::new();
    let mut inflight = FuturesUnordered::new();
    let max_inflight = config.max_inflight_ops.max(1);
    let max_batch_items = config.batch_max_items.min(MAX_PERSIST_BATCH_ITEMS).max(1);
    let batch_wait = Duration::from_micros(config.batch_max_wait_us);
    let mut required_open = true;
    let mut early_open = true;
    let mut blocked_on_space = false;

    loop {
        promote_required_writes(&mut required_pending, &mut early_pending);

        let pending_ready = !required_pending.is_empty() || !early_pending.is_empty();
        let oldest = oldest_write_enqueue(&required_pending, &early_pending);
        let limits_reached = pending_write_limits_reached(
            &required_pending,
            &early_pending,
            max_batch_items,
            config.batch_max_bytes,
        );
        let wait_expired = oldest.is_some_and(|queued_at| queued_at.elapsed() >= batch_wait);
        let inputs_closed = !required_open && !early_open;
        if pending_ready
            && inflight.len() < max_inflight
            && !blocked_on_space
            && (limits_reached || wait_expired || inputs_closed)
        {
            let available_ops = max_inflight - inflight.len();
            let commands = take_write_batch(
                &mut required_pending,
                &mut early_pending,
                available_ops.min(max_batch_items),
                config.batch_max_bytes,
            );
            let scheduled_items = commands.len();
            let scheduled_bytes = commands.iter().fold(0u64, |total, cmd| {
                total.saturating_add(cmd.data.len() as u64)
            });
            let scheduled_wait_us = commands
                .iter()
                .map(|cmd| cmd.queued_at.elapsed().as_micros() as u64)
                .max()
                .unwrap_or(0);
            let scheduled_required = commands
                .iter()
                .filter(|cmd| cmd.priority.class() == KvSsdPersistClass::RequiredLastBacking)
                .count();
            let mut tasks = Vec::new();
            let mut evicted = Vec::new();
            let mut blocked = Vec::new();
            for cmd in commands {
                let (prepared, existing_guard) =
                    prepare_ssd_write(&inner, &space_notify, &cmd.key, cmd.entry_len, &shard_ids);
                match prepared {
                    Ok(SsdPreparedWrite::Ready {
                        entry,
                        evicted: item_evicted,
                    }) => {
                        evicted.extend(item_evicted);
                        tasks.push(WriteTask {
                            key: cmd.key,
                            entry,
                            data: cmd.data,
                            _device_permit: cmd.device_permit,
                            done_tx: cmd.done_tx,
                        });
                    }
                    Ok(SsdPreparedWrite::Existing) => {
                        let result = existing_guard.ok_or_else(|| {
                            KvError::Api(ApiError::KeyNotFound {
                                key: cmd.key.key.clone(),
                            })
                        });
                        let _ = cmd.done_tx.send(result);
                    }
                    Ok(SsdPreparedWrite::BlockedByBusyIo) => blocked.push(cmd),
                    Err(err) => {
                        let _ = cmd.done_tx.send(Err(err));
                    }
                }
            }
            let had_blocked = !blocked.is_empty();
            if had_blocked {
                stats.persist_busy_batches.fetch_add(1, Ordering::Relaxed);
            }
            for cmd in blocked {
                enqueue_write_command(cmd, &mut required_pending, &mut early_pending);
            }
            blocked_on_space = tasks.is_empty() && had_blocked;
            if !tasks.is_empty() {
                stats.persist_flush_batches.fetch_add(1, Ordering::Relaxed);
                tracing::trace!(
                    scheduled_items,
                    scheduled_bytes,
                    scheduled_wait_us,
                    scheduled_required,
                    submitted_items = tasks.len(),
                    "native SSD writer submitting byte-aware io_uring batch"
                );
                publish_ssd_evictions(&eviction_tx, &mut shutdown_rx, evicted).await;
                let descriptors = tasks
                    .iter()
                    .map(|task| SsdWriteDescriptor {
                        shard_id: task.entry.shard_id,
                        ptr: task.data.as_ptr(),
                        len: task.data.len(),
                        offset: task.entry.file_offset,
                    })
                    .collect::<Vec<_>>();
                let submissions = io.write_batch_at_async(&descriptors);
                debug_assert_eq!(submissions.len(), tasks.len());
                for (task, submission) in tasks.into_iter().zip(submissions) {
                    inflight.push(execute_submitted_write(task, submission));
                }
            }
            continue;
        }

        if inputs_closed
            && required_pending.is_empty()
            && early_pending.is_empty()
            && inflight.is_empty()
        {
            break;
        }

        let timer_wait = oldest
            .map(|queued_at| batch_wait.saturating_sub(queued_at.elapsed()))
            .unwrap_or(Duration::from_secs(3600));
        let local_pending = required_pending.len() + early_pending.len();
        let required_queue_has_room = local_pending < DEFAULT_WRITE_QUEUE_DEPTH;
        let early_queue_has_room = local_pending < DEFAULT_WRITE_QUEUE_DEPTH.saturating_sub(1);
        tokio::select! {
            Some(completion) = inflight.next(), if !inflight.is_empty() => {
                finish_write_completion(&inner, &space_notify, completion);
                blocked_on_space = false;
            }
            maybe_cmd = required_rx.recv(), if required_open && required_queue_has_room => {
                match maybe_cmd {
                    Some(cmd) => {
                        enqueue_write_command(cmd, &mut required_pending, &mut early_pending);
                        blocked_on_space = false;
                    }
                    None => required_open = false,
                }
            }
            maybe_cmd = early_rx.recv(), if early_open && early_queue_has_room && !blocked_on_space => {
                match maybe_cmd {
                    Some(cmd) => {
                        enqueue_write_command(cmd, &mut required_pending, &mut early_pending);
                        blocked_on_space = false;
                    }
                    None => early_open = false,
                }
            }
            _ = space_notify.notified(), if pending_ready => {
                blocked_on_space = false;
            }
            _ = ::tokio::time::sleep(timer_wait), if pending_ready && !blocked_on_space => {
                // The next loop dispatches the byte-aware batch.
            }
        }
    }
}

fn enqueue_write_command(
    cmd: WriteCommand,
    required: &mut VecDeque<WriteCommand>,
    early: &mut VecDeque<WriteCommand>,
) {
    match cmd.priority.class() {
        KvSsdPersistClass::RequiredLastBacking => required.push_back(cmd),
        KvSsdPersistClass::BestEffortEarly => early.push_back(cmd),
    }
}

fn promote_required_writes(
    required: &mut VecDeque<WriteCommand>,
    early: &mut VecDeque<WriteCommand>,
) {
    let mut retained = VecDeque::with_capacity(early.len());
    while let Some(cmd) = early.pop_front() {
        if cmd.priority.class() == KvSsdPersistClass::RequiredLastBacking {
            required.push_back(cmd);
        } else {
            retained.push_back(cmd);
        }
    }
    *early = retained;
}

fn oldest_write_enqueue(
    required: &VecDeque<WriteCommand>,
    early: &VecDeque<WriteCommand>,
) -> Option<Instant> {
    required
        .iter()
        .map(|cmd| cmd.queued_at)
        .chain(early.iter().map(|cmd| cmd.queued_at))
        .min()
}

fn pending_write_limits_reached(
    required: &VecDeque<WriteCommand>,
    early: &VecDeque<WriteCommand>,
    max_items: usize,
    max_bytes: u64,
) -> bool {
    let mut items = 0usize;
    let mut bytes = 0u64;
    for cmd in required.iter().chain(early.iter()) {
        items += 1;
        bytes = bytes.saturating_add(cmd.data.len() as u64);
        if items >= max_items || bytes >= max_bytes || cmd.data.len() as u64 > max_bytes {
            return true;
        }
    }
    false
}

fn take_write_batch(
    required: &mut VecDeque<WriteCommand>,
    early: &mut VecDeque<WriteCommand>,
    max_items: usize,
    max_bytes: u64,
) -> Vec<WriteCommand> {
    let mut batch = Vec::new();
    let mut bytes = 0u64;
    take_write_batch_from_queue(required, &mut batch, &mut bytes, max_items, max_bytes);
    if required.is_empty() && batch.len() < max_items {
        take_write_batch_from_queue(early, &mut batch, &mut bytes, max_items, max_bytes);
    }
    batch
}

fn take_write_batch_from_queue(
    queue: &mut VecDeque<WriteCommand>,
    batch: &mut Vec<WriteCommand>,
    bytes: &mut u64,
    max_items: usize,
    max_bytes: u64,
) {
    let scan = queue.len();
    for _ in 0..scan {
        if batch.len() >= max_items {
            break;
        }
        let cmd = queue
            .pop_front()
            .expect("write batch scan length must match queue length");
        let item_bytes = cmd.data.len() as u64;
        let fits = batch.is_empty() || bytes.saturating_add(item_bytes) <= max_bytes;
        if fits {
            *bytes = bytes.saturating_add(item_bytes);
            batch.push(cmd);
            if item_bytes > max_bytes {
                break;
            }
        } else {
            queue.push_back(cmd);
        }
    }
}

async fn publish_ssd_evictions(
    eviction_tx: &tokio_mpsc::Sender<Vec<SsdReplicaEviction>>,
    shutdown_rx: &mut watch::Receiver<bool>,
    evicted: Vec<KvSsdKey>,
) {
    if evicted.is_empty() {
        return;
    }
    let replicas = evicted
        .into_iter()
        .map(|key| SsdReplicaEviction {
            key: key.key,
            put_id: key.put_id,
        })
        .collect();
    if *shutdown_rx.borrow() {
        tracing::debug!("Skipping SSD eviction publication during storage shutdown");
        return;
    }
    tokio::select! {
        result = eviction_tx.send(replicas) => {
            if let Err(err) = result {
                tracing::warn!("SSD eviction receiver is closed; notification cannot be delivered: {err}");
            }
        }
        _ = shutdown_rx.changed() => {
            if !*shutdown_rx.borrow() {
                tracing::warn!("SSD eviction shutdown signal closed unexpectedly");
            } else {
                tracing::debug!("Cancelled SSD eviction publication during storage shutdown");
            }
        }
    }
}

fn finish_write_completion(
    inner: &Arc<Mutex<KvSsdStorageInner>>,
    space_notify: &Arc<Notify>,
    completion: WriteCompletion,
) {
    let (committed, commit_pinned) = {
        let mut inner = inner.lock();
        let committed = inner.ring.commit(&completion.key, completion.success);
        let commit_pinned = committed && inner.ring.pin_read(&completion.key).is_some();
        (committed, commit_pinned)
    };
    space_notify.notify_one();
    let result = match completion.result {
        Err(err) => Err(err),
        Ok(()) if committed && commit_pinned => Ok(KvSsdPersistGuard {
            _native_pin: Some(SsdReadPin {
                inner: Arc::clone(inner),
                space_notify: Arc::clone(space_notify),
                key: completion.key.clone(),
            }),
            _foyer_guard: None,
        }),
        Ok(()) => Err(KvError::Api(ApiError::KeyNotFound {
            key: completion.key.key.clone(),
        })),
    };
    let _ = completion.done_tx.send(result);
}

#[derive(Clone, Copy)]
struct SsdWriteDescriptor {
    shard_id: usize,
    ptr: *const u8,
    len: usize,
    offset: u64,
}

async fn execute_submitted_write(
    task: WriteTask,
    submission: io::Result<oneshot::Receiver<io::Result<usize>>>,
) -> WriteCompletion {
    let WriteTask {
        key,
        entry,
        data,
        _device_permit,
        done_tx,
    } = task;
    let data_len = data.len();
    let file_offset = entry.file_offset;
    let result = async move {
        let rx = submission?;
        let written = rx
            .await
            .map_err(|_| io::Error::other("kv ssd write completion dropped"))??;
        if written != data_len {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                format!("short kv ssd write: {} != {}", written, data_len),
            )
            .into());
        }
        Ok(())
    }
    .await;
    // The io_uring SQE references this allocation directly; retain it until
    // the per-item completion has been observed.
    drop(data);
    drop(_device_permit);
    let result = result.map_err(|err| file_error_for_entry(&key, file_offset, err));
    WriteCompletion {
        key,
        success: result.is_ok(),
        result,
        done_tx,
    }
}

async fn ssd_reader_loop(
    inner: Arc<Mutex<KvSsdStorageInner>>,
    mut rx: tokio_mpsc::Receiver<ReadCommand>,
    io: Arc<UringIoEngine>,
    read_inflight: usize,
) {
    let mut inflight = FuturesUnordered::new();
    let max_inflight = read_inflight.max(1);
    let mut rx_open = true;

    loop {
        if inflight.is_empty() {
            if !rx_open {
                break;
            }
            match rx.recv().await {
                Some(cmd) => inflight.push(execute_read(cmd.into(), Arc::clone(&io))),
                None => rx_open = false,
            }
            continue;
        }

        tokio::select! {
            Some(completion) = inflight.next(), if !inflight.is_empty() => {
                let valid = inner.lock().ring.is_offset_valid(&completion.entry);
                let result = if valid {
                    completion.result
                } else {
                    inner.lock().ring.remove(&completion.key);
                    Err(KvError::Api(ApiError::KeyNotFound {
                        key: completion.key.key.clone(),
                    }))
                };
                let _ = completion.done_tx.send(result);
            }
            maybe_cmd = rx.recv(), if rx_open && inflight.len() < max_inflight => {
                match maybe_cmd {
                    Some(cmd) => inflight.push(execute_read(cmd.into(), Arc::clone(&io))),
                    None => rx_open = false,
                }
            }
        }
    }
}

async fn execute_read(task: ReadTask, io: Arc<UringIoEngine>) -> ReadCompletion {
    let ReadTask {
        key,
        entry,
        file_offset,
        target,
        _admission,
        _read_pin,
        _target_lifetime,
        done_tx,
    } = task;
    let shard_id = entry.shard_id;
    let result = match target {
        ReadTarget::Scratch { len } => match AlignedBuffer::zeroed(len) {
            Ok(mut buffer) => {
                let buffer_len = buffer.len();
                let io_result: io::Result<ReadOutput> = async {
                    let rx = {
                        let buffer_ptr = buffer.as_mut_ptr();
                        io.read_at_async(shard_id, buffer_ptr, buffer_len, file_offset)?
                    };
                    let read = rx
                        .await
                        .map_err(|_| io::Error::other("kv ssd read completion dropped"))??;
                    if read != buffer_len {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!("short kv ssd read: {} != {}", read, buffer_len),
                        ));
                    }
                    Ok(ReadOutput::Scratch(buffer))
                }
                .await;
                io_result.map_err(|err| file_error_for_entry(&key, file_offset, err))
            }
            Err(err) => Err(err),
        },
        ReadTarget::Direct { target_addr, len } => {
            let io_result: io::Result<ReadOutput> = async {
                let rx = io.read_at_async(shard_id, target_addr as *mut u8, len, file_offset)?;
                let read = rx
                    .await
                    .map_err(|_| io::Error::other("kv ssd read completion dropped"))??;
                if read != len {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("short kv ssd direct read: {} != {}", read, len),
                    ));
                }
                Ok(ReadOutput::Direct)
            }
            .await;
            io_result.map_err(|err| file_error_for_entry(&key, file_offset, err))
        }
    };
    ReadCompletion {
        key,
        entry,
        result,
        _admission,
        _read_pin,
        _target_lifetime,
        done_tx,
    }
}

#[derive(Clone, Copy)]
struct UringConfig {
    threads: usize,
    io_depth: usize,
    mode: KvSsdUringMode,
}

#[derive(Clone, Copy)]
enum IoType {
    Read,
    Write,
    Readv,
    Writev,
}

struct IoCtx {
    io_type: IoType,
    fd: RawFd,
    len: usize,
    offset: u64,
    complete: oneshot::Sender<io::Result<usize>>,
    buffer: Option<(*mut u8, usize)>,
    iovecs: Option<Box<[libc::iovec]>>,
}

unsafe impl Send for IoCtx {}

enum IoSubmission {
    Single(IoCtx),
    Batch(Vec<IoCtx>),
}

struct UringShard {
    read_rx: crossbeam::channel::Receiver<IoCtx>,
    write_rx: crossbeam::channel::Receiver<IoSubmission>,
    uring: Option<IoUring>,
    io_depth: usize,
    read_weight: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SubmitWaitAction {
    Retry,
    DrainCompletions,
    FailWorker,
}

fn submit_wait_error_action(err: &io::Error) -> SubmitWaitAction {
    if err.kind() == io::ErrorKind::Interrupted {
        SubmitWaitAction::Retry
    } else if err.raw_os_error() == Some(libc::EBUSY) {
        SubmitWaitAction::DrainCompletions
    } else {
        SubmitWaitAction::FailWorker
    }
}

impl UringShard {
    fn run(mut self) {
        let mut read_inflight = 0usize;
        let mut write_inflight = 0usize;
        let mut read_closed = false;
        let mut write_closed = false;
        let mut submitted = HashMap::new();
        let mut next_token = 1u64;
        let mut pending_writes = VecDeque::new();

        loop {
            let mut inflight = read_inflight + write_inflight;
            while inflight < self.io_depth && !(read_closed && write_closed) {
                let next = self.try_recv_weighted(
                    &mut read_closed,
                    &mut write_closed,
                    read_inflight,
                    write_inflight,
                    &mut pending_writes,
                );
                let Some(ctx) = next else {
                    break;
                };
                self.submit_ctx(
                    ctx,
                    &mut submitted,
                    &mut next_token,
                    &mut read_inflight,
                    &mut write_inflight,
                );
                inflight = read_inflight + write_inflight;
            }

            if read_closed && write_closed && inflight == 0 {
                debug_assert!(submitted.is_empty());
                return;
            }
            if inflight == 0 {
                let Some(ctx) =
                    self.recv_blocking(&mut read_closed, &mut write_closed, &mut pending_writes)
                else {
                    continue;
                };
                self.submit_ctx(
                    ctx,
                    &mut submitted,
                    &mut next_token,
                    &mut read_inflight,
                    &mut write_inflight,
                );
                continue;
            }
            let submit_result = self
                .uring
                .as_mut()
                .expect("io_uring must exist while its worker is running")
                .submit_and_wait(1);
            if let Err(err) = submit_result {
                match submit_wait_error_action(&err) {
                    SubmitWaitAction::Retry | SubmitWaitAction::DrainCompletions => {
                        self.drain_completions(
                            &mut submitted,
                            &mut read_inflight,
                            &mut write_inflight,
                        );
                        continue;
                    }
                    SubmitWaitAction::FailWorker => {
                        let error_kind = err.kind();
                        let detail = format!("io_uring submit failed: {err}");
                        drop(self.uring.take());
                        for (_, ctx) in submitted.drain() {
                            let _ = ctx
                                .complete
                                .send(Err(io::Error::new(error_kind, detail.clone())));
                        }
                        return;
                    }
                }
            }

            self.drain_completions(&mut submitted, &mut read_inflight, &mut write_inflight);
        }
    }

    fn drain_completions(
        &mut self,
        submitted: &mut HashMap<u64, IoCtx>,
        read_inflight: &mut usize,
        write_inflight: &mut usize,
    ) {
        let uring = self
            .uring
            .as_mut()
            .expect("io_uring must exist while draining completions");
        for cqe in uring.completion() {
            let token = cqe.user_data();
            if token == 0 {
                continue;
            }
            let Some(ctx) = submitted.remove(&token) else {
                tracing::warn!("Ignoring io_uring completion with unknown token {token}");
                continue;
            };
            match ctx.io_type {
                IoType::Read | IoType::Readv => *read_inflight = read_inflight.saturating_sub(1),
                IoType::Write | IoType::Writev => {
                    *write_inflight = write_inflight.saturating_sub(1)
                }
            }
            let result = cqe.result();
            let send_result = if result < 0 {
                Err(io::Error::from_raw_os_error(-result))
            } else {
                Ok(result as usize)
            };
            let _ = ctx.complete.send(send_result);
        }
    }

    fn try_recv_weighted(
        &self,
        read_closed: &mut bool,
        write_closed: &mut bool,
        read_inflight: usize,
        write_inflight: usize,
        pending_writes: &mut VecDeque<IoCtx>,
    ) -> Option<IoCtx> {
        let prefer_read = read_inflight <= write_inflight.saturating_mul(self.read_weight);
        if prefer_read {
            self.try_recv_read(read_closed)
                .or_else(|| self.try_recv_write(write_closed, pending_writes))
        } else {
            self.try_recv_write(write_closed, pending_writes)
                .or_else(|| self.try_recv_read(read_closed))
        }
    }

    fn try_recv_read(&self, read_closed: &mut bool) -> Option<IoCtx> {
        if *read_closed {
            return None;
        }
        match self.read_rx.try_recv() {
            Ok(ctx) => Some(ctx),
            Err(crossbeam::channel::TryRecvError::Empty) => None,
            Err(crossbeam::channel::TryRecvError::Disconnected) => {
                *read_closed = true;
                None
            }
        }
    }

    fn try_recv_write(
        &self,
        write_closed: &mut bool,
        pending_writes: &mut VecDeque<IoCtx>,
    ) -> Option<IoCtx> {
        if let Some(ctx) = pending_writes.pop_front() {
            return Some(ctx);
        }
        if *write_closed {
            return None;
        }
        match self.write_rx.try_recv() {
            Ok(submission) => {
                extend_io_submission(pending_writes, submission);
                pending_writes.pop_front()
            }
            Err(crossbeam::channel::TryRecvError::Empty) => None,
            Err(crossbeam::channel::TryRecvError::Disconnected) => {
                *write_closed = true;
                None
            }
        }
    }

    fn recv_blocking(
        &self,
        read_closed: &mut bool,
        write_closed: &mut bool,
        pending_writes: &mut VecDeque<IoCtx>,
    ) -> Option<IoCtx> {
        loop {
            if let Some(ctx) = pending_writes.pop_front() {
                return Some(ctx);
            }
            match (!*read_closed, !*write_closed) {
                (true, true) => {
                    crossbeam::channel::select! {
                        recv(self.read_rx) -> msg => match msg {
                            Ok(ctx) => return Some(ctx),
                            Err(_) => *read_closed = true,
                        },
                        recv(self.write_rx) -> msg => match msg {
                            Ok(submission) => {
                                extend_io_submission(pending_writes, submission);
                            }
                            Err(_) => *write_closed = true,
                        },
                    }
                }
                (true, false) => match self.read_rx.recv() {
                    Ok(ctx) => return Some(ctx),
                    Err(_) => *read_closed = true,
                },
                (false, true) => match self.write_rx.recv() {
                    Ok(submission) => {
                        extend_io_submission(pending_writes, submission);
                    }
                    Err(_) => *write_closed = true,
                },
                (false, false) => return None,
            }
        }
    }

    fn submit_ctx(
        &mut self,
        ctx: IoCtx,
        submitted: &mut HashMap<u64, IoCtx>,
        next_token: &mut u64,
        read_inflight: &mut usize,
        write_inflight: &mut usize,
    ) {
        let fd = Fd(ctx.fd);
        let buffer = ctx.buffer;
        let iovecs_ptr = ctx
            .iovecs
            .as_ref()
            .map(|iovecs| iovecs.as_ptr())
            .unwrap_or(std::ptr::null());
        let sqe = match ctx.io_type {
            IoType::Read => {
                let Some((ptr, len)) = buffer else {
                    let _ = ctx.complete.send(Err(io::Error::other(
                        "single-buffer read missing buffer pointer",
                    )));
                    return;
                };
                opcode::Read::new(fd, ptr, len as _)
                    .offset(ctx.offset)
                    .build()
            }
            IoType::Write => {
                let Some((ptr, len)) = buffer else {
                    let _ = ctx.complete.send(Err(io::Error::other(
                        "single-buffer write missing buffer pointer",
                    )));
                    return;
                };
                opcode::Write::new(fd, ptr as *const u8, len as _)
                    .offset(ctx.offset)
                    .build()
            }
            IoType::Readv => opcode::Readv::new(fd, iovecs_ptr, ctx.len as _)
                .offset(ctx.offset)
                .build(),
            IoType::Writev => opcode::Writev::new(fd, iovecs_ptr, ctx.len as _)
                .offset(ctx.offset)
                .build(),
        };
        let io_type = ctx.io_type;
        let token = next_submission_token(next_token, submitted);
        let sqe = sqe.user_data(token);
        let Some(uring) = self.uring.as_mut() else {
            let _ = ctx
                .complete
                .send(Err(io::Error::other("io_uring worker is closed")));
            return;
        };
        let push_result = unsafe { uring.submission().push(&sqe) };
        if push_result.is_err() {
            let _ = ctx
                .complete
                .send(Err(io::Error::other("submission queue full")));
            return;
        }
        let replaced = submitted.insert(token, ctx);
        debug_assert!(
            replaced.is_none(),
            "io_uring submission token must be unique"
        );
        match io_type {
            IoType::Read | IoType::Readv => *read_inflight += 1,
            IoType::Write | IoType::Writev => *write_inflight += 1,
        }
    }
}

fn extend_io_submission(pending: &mut VecDeque<IoCtx>, submission: IoSubmission) {
    match submission {
        IoSubmission::Single(ctx) => pending.push_back(ctx),
        IoSubmission::Batch(items) => pending.extend(items),
    }
}

fn next_submission_token(next_token: &mut u64, submitted: &HashMap<u64, IoCtx>) -> u64 {
    loop {
        if *next_token == 0 {
            *next_token = 1;
        }
        let token = *next_token;
        *next_token = token.wrapping_add(1);
        if !submitted.contains_key(&token) {
            return token;
        }
    }
}

#[derive(Debug)]
struct UringIoEngine {
    _files: Vec<std::fs::File>,
    fds: HashMap<usize, RawFd>,
    read_txs: Vec<crossbeam::channel::Sender<IoCtx>>,
    write_txs: Vec<crossbeam::channel::Sender<IoSubmission>>,
    handles: Vec<JoinHandle<()>>,
    mode: KvSsdUringMode,
}

impl UringIoEngine {
    fn new_multi(shard_files: Vec<(usize, std::fs::File)>, cfg: UringConfig) -> io::Result<Self> {
        if cfg.threads == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "threads must be > 0",
            ));
        }
        if shard_files.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "at least one fd is required",
            ));
        }
        let fds = shard_files
            .iter()
            .map(|(shard_id, file)| (*shard_id, file.as_raw_fd()))
            .collect::<HashMap<_, _>>();
        let files = shard_files
            .into_iter()
            .map(|(_, file)| file)
            .collect::<Vec<_>>();
        let mut read_txs = Vec::with_capacity(cfg.threads);
        let mut write_txs = Vec::with_capacity(cfg.threads);
        let mut handles = Vec::with_capacity(cfg.threads);
        for idx in 0..cfg.threads {
            let (read_tx, read_rx) = crossbeam::channel::bounded(cfg.io_depth * 2);
            let (write_tx, write_rx) = crossbeam::channel::bounded(cfg.io_depth * 2);
            let uring = IoUring::builder().build(cfg.io_depth as u32)?;
            let handle = std::thread::Builder::new()
                .name(format!("fluxon-kv-ssd-uring-{idx}"))
                .spawn(move || {
                    UringShard {
                        read_rx,
                        write_rx,
                        uring: Some(uring),
                        io_depth: cfg.io_depth,
                        read_weight: DEFAULT_URING_READ_WEIGHT,
                    }
                    .run()
                })?;
            read_txs.push(read_tx);
            write_txs.push(write_tx);
            handles.push(handle);
        }
        Ok(Self {
            _files: files,
            fds,
            read_txs,
            write_txs,
            handles,
            mode: cfg.mode,
        })
    }

    fn read_at_async(
        &self,
        shard_id: usize,
        ptr: *mut u8,
        len: usize,
        offset: u64,
    ) -> io::Result<oneshot::Receiver<io::Result<usize>>> {
        match self.mode {
            KvSsdUringMode::SingleBuffer => {
                self.submit_buffer(IoType::Read, shard_id, ptr, len, offset)
            }
            KvSsdUringMode::Iovec => self.readv_at_async(shard_id, vec![(ptr, len)], offset),
        }
    }

    fn write_at_async(
        &self,
        shard_id: usize,
        ptr: *const u8,
        len: usize,
        offset: u64,
    ) -> io::Result<oneshot::Receiver<io::Result<usize>>> {
        match self.mode {
            KvSsdUringMode::SingleBuffer => {
                self.submit_buffer(IoType::Write, shard_id, ptr as *mut u8, len, offset)
            }
            KvSsdUringMode::Iovec => self.writev_at_async(shard_id, vec![(ptr, len)], offset),
        }
    }

    /// Submit one scheduler batch without concatenating value buffers. Items
    /// are grouped by io_uring worker, pushed as one channel message per
    /// worker, and retain independent completion receivers.
    fn write_batch_at_async(
        &self,
        descriptors: &[SsdWriteDescriptor],
    ) -> Vec<io::Result<oneshot::Receiver<io::Result<usize>>>> {
        let mut results = std::iter::repeat_with(|| None)
            .take(descriptors.len())
            .collect::<Vec<Option<io::Result<oneshot::Receiver<io::Result<usize>>>>>>();
        let mut groups = (0..self.write_txs.len())
            .map(|_| Vec::new())
            .collect::<Vec<Vec<IoCtx>>>();
        for (index, descriptor) in descriptors.iter().copied().enumerate() {
            match self.build_write_ctx(descriptor) {
                Ok((ctx, rx)) => {
                    let worker = descriptor.shard_id % groups.len();
                    groups[worker].push(ctx);
                    results[index] = Some(Ok(rx));
                }
                Err(err) => results[index] = Some(Err(err)),
            }
        }
        for (worker, contexts) in groups.into_iter().enumerate() {
            if contexts.is_empty() {
                continue;
            }
            if let Err(err) = self.write_txs[worker].send(IoSubmission::Batch(contexts)) {
                let detail = format!("io_uring write batch send failed: {err}");
                let submission = err.0;
                let mut failed = VecDeque::new();
                extend_io_submission(&mut failed, submission);
                for ctx in failed {
                    let _ = ctx.complete.send(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        detail.clone(),
                    )));
                }
            }
        }
        results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(io::Error::other(
                        "io_uring write batch result was not populated",
                    ))
                })
            })
            .collect()
    }

    fn build_write_ctx(
        &self,
        descriptor: SsdWriteDescriptor,
    ) -> io::Result<(IoCtx, oneshot::Receiver<io::Result<usize>>)> {
        validate_direct_io(
            [(descriptor.ptr as usize, descriptor.len)],
            descriptor.offset,
        )?;
        let (complete, rx) = oneshot::channel();
        let (io_type, buffer, iovecs, len) = match self.mode {
            KvSsdUringMode::SingleBuffer => (
                IoType::Write,
                Some((descriptor.ptr as *mut u8, descriptor.len)),
                None,
                descriptor.len,
            ),
            KvSsdUringMode::Iovec => {
                let iovecs = vec![libc::iovec {
                    iov_base: descriptor.ptr as *mut libc::c_void,
                    iov_len: descriptor.len,
                }]
                .into_boxed_slice();
                (IoType::Writev, None, Some(iovecs), 1)
            }
        };
        Ok((
            IoCtx {
                io_type,
                fd: self.fd(descriptor.shard_id)?,
                len,
                offset: descriptor.offset,
                complete,
                buffer,
                iovecs,
            },
            rx,
        ))
    }

    fn readv_at_async(
        &self,
        shard_id: usize,
        iovecs: Vec<(*mut u8, usize)>,
        offset: u64,
    ) -> io::Result<oneshot::Receiver<io::Result<usize>>> {
        self.submit_iovecs(IoType::Readv, shard_id, iovecs, offset)
    }

    fn writev_at_async(
        &self,
        shard_id: usize,
        iovecs: Vec<(*const u8, usize)>,
        offset: u64,
    ) -> io::Result<oneshot::Receiver<io::Result<usize>>> {
        let iovecs = iovecs
            .into_iter()
            .map(|(ptr, len)| (ptr as *mut u8, len))
            .collect();
        self.submit_iovecs(IoType::Writev, shard_id, iovecs, offset)
    }

    fn submit_buffer(
        &self,
        io_type: IoType,
        shard_id: usize,
        ptr: *mut u8,
        len: usize,
        offset: u64,
    ) -> io::Result<oneshot::Receiver<io::Result<usize>>> {
        if !matches!(io_type, IoType::Read | IoType::Write) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "single-buffer submit requires read/write io type",
            ));
        }
        validate_direct_io([(ptr as usize, len)], offset)?;
        let (tx, rx) = oneshot::channel();
        let ctx = IoCtx {
            io_type,
            fd: self.fd(shard_id)?,
            len,
            offset,
            complete: tx,
            buffer: Some((ptr, len)),
            iovecs: None,
        };
        self.send_ctx(io_type, shard_id, ctx)?;
        Ok(rx)
    }

    fn submit_iovecs(
        &self,
        io_type: IoType,
        shard_id: usize,
        iovecs: Vec<(*mut u8, usize)>,
        offset: u64,
    ) -> io::Result<oneshot::Receiver<io::Result<usize>>> {
        if iovecs.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "readv/writev requires at least one iovec",
            ));
        }
        validate_direct_io(
            iovecs.iter().map(|(ptr, len)| (*ptr as usize, *len)),
            offset,
        )?;
        let iovecs_libc = iovecs
            .iter()
            .map(|(ptr, len)| libc::iovec {
                iov_base: *ptr as *mut libc::c_void,
                iov_len: *len,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let (tx, rx) = oneshot::channel();
        let ctx = IoCtx {
            io_type,
            fd: self.fd(shard_id)?,
            len: iovecs_libc.len(),
            offset,
            complete: tx,
            buffer: None,
            iovecs: Some(iovecs_libc),
        };
        self.send_ctx(io_type, shard_id, ctx)?;
        Ok(rx)
    }

    fn fd(&self, shard_id: usize) -> io::Result<RawFd> {
        self.fds.get(&shard_id).copied().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid SSD shard id {shard_id}"),
            )
        })
    }

    fn send_ctx(&self, io_type: IoType, shard_id: usize, ctx: IoCtx) -> io::Result<()> {
        match io_type {
            IoType::Read | IoType::Readv => self.read_txs[shard_id % self.read_txs.len()]
                .send(ctx)
                .map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        format!("io_uring read send failed: {err}"),
                    )
                }),
            IoType::Write | IoType::Writev => self.write_txs[shard_id % self.write_txs.len()]
                .send(IoSubmission::Single(ctx))
                .map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        format!("io_uring write send failed: {err}"),
                    )
                }),
        }
    }
}

impl Drop for UringIoEngine {
    fn drop(&mut self) {
        self.read_txs.clear();
        self.write_txs.clear();
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

struct AlignedBuffer {
    ptr: NonNull<u8>,
    len: usize,
    mapped_len: usize,
}

unsafe impl Send for AlignedBuffer {}

impl AlignedBuffer {
    fn zeroed(len: usize) -> KvResult<Self> {
        if len == 0 || !len.is_multiple_of(SSD_ALIGNMENT) {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "aligned buffer len must be positive and {}-byte aligned: {}",
                    SSD_ALIGNMENT, len
                ),
            }));
        }
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        assert!(page_size > 0 && (page_size as usize).is_multiple_of(SSD_ALIGNMENT));
        let mapped_len = align_up_usize(len, page_size as usize)?;
        // Anonymous pages are zeroed and return to the OS on drop, bypassing heap retention.
        let raw = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                mapped_len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if raw == libc::MAP_FAILED {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!("SSD buffer mmap failed: {}", io::Error::last_os_error()),
            }));
        }
        let Some(ptr) = NonNull::new(raw.cast::<u8>()) else {
            unsafe { libc::munmap(raw, mapped_len) };
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "SSD buffer mmap returned a null address".to_string(),
            }));
        };
        Ok(Self {
            ptr,
            len,
            mapped_len,
        })
    }

    unsafe fn copy_from_addr(addr: u64, actual_len: usize, aligned_len: usize) -> KvResult<Self> {
        let mut buffer = Self::zeroed(aligned_len)?;
        unsafe {
            std::ptr::copy_nonoverlapping(addr as *const u8, buffer.as_mut_ptr(), actual_len);
        }
        Ok(buffer)
    }

    fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    fn len(&self) -> usize {
        self.len
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        let rc = unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.mapped_len) };
        assert_eq!(
            rc,
            0,
            "SSD buffer munmap failed: {}",
            io::Error::last_os_error()
        );
    }
}

fn validate_key(key: &str) -> KvResult<()> {
    if key.is_empty() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "kv ssd storage key must be non-empty".to_string(),
        }));
    }
    Ok(())
}

fn persist_batch_size_error(item_count: usize) -> KvError {
    KvError::Api(ApiError::InvalidArgument {
        detail: format!(
            "kv ssd persist batch item count is outside 1..={}: items={}",
            MAX_PERSIST_BATCH_ITEMS, item_count
        ),
    })
}

fn choose_read_path(
    entry: &SsdIndexEntry,
    target_addr: u64,
    len: u64,
    target_len: u64,
) -> SsdReadPath {
    if len == 0 || entry.len != len {
        return SsdReadPath::Scratch;
    }
    if target_addr.is_multiple_of(SSD_ALIGNMENT as u64)
        && target_len >= entry.aligned_len
        && entry.file_offset.is_multiple_of(SSD_ALIGNMENT as u64)
    {
        SsdReadPath::Direct
    } else {
        SsdReadPath::Scratch
    }
}

fn choose_chunk_read_path(
    target_addr: u64,
    read_len: u64,
    target_len: u64,
    file_offset: u64,
) -> SsdReadPath {
    if read_len != 0
        && target_addr.is_multiple_of(SSD_ALIGNMENT as u64)
        && read_len.is_multiple_of(SSD_ALIGNMENT as u64)
        && target_len >= read_len
        && file_offset.is_multiple_of(SSD_ALIGNMENT as u64)
    {
        SsdReadPath::Direct
    } else {
        SsdReadPath::Scratch
    }
}

fn sparse_read_window(
    src_offset: u64,
    payload_len: u64,
    aligned_entry_len: u64,
) -> KvResult<(u64, u64, u64)> {
    if payload_len == 0 {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "sparse SSD payload length must be positive".to_string(),
        }));
    }
    let alignment = SSD_ALIGNMENT as u64;
    let payload_end = checked_add_u64(src_offset, payload_len, "sparse payload end")?;
    let aligned_start = src_offset / alignment * alignment;
    let aligned_end = align_up_u64(payload_end, alignment)?;
    if aligned_end > aligned_entry_len {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "sparse aligned range exceeds entry: start={} end={} entry_aligned_len={}",
                aligned_start, aligned_end, aligned_entry_len
            ),
        }));
    }
    Ok((
        aligned_start,
        aligned_end - aligned_start,
        src_offset - aligned_start,
    ))
}

async fn run_bounded_sparse_reads<I, F, Fut>(
    requests: I,
    max_inflight: usize,
    target_addr: u64,
    mut submit: F,
) -> KvResult<()>
where
    I: IntoIterator<Item = SparseReadRequest>,
    F: FnMut(SparseReadRequest) -> Fut,
    Fut: std::future::Future<Output = KvResult<(SparseGetRange, u64, ReadOutput)>>,
{
    debug_assert!(max_inflight > 0);
    let mut pending = requests.into_iter();
    let mut reads = FuturesUnordered::new();
    while reads.len() < max_inflight {
        let Some(request) = pending.next() else {
            break;
        };
        reads.push(submit(request));
    }

    let mut first_error = None;
    while let Some(result) = reads.next().await {
        if let Err(err) = materialize_sparse_read_result(result, target_addr) {
            first_error.get_or_insert(err);
        }
        // Do not start pending ranges after the first error. Futures already
        // admitted into this bounded set are still drained to terminal.
        if first_error.is_none() {
            if let Some(request) = pending.next() {
                reads.push(submit(request));
            }
        }
    }
    match first_error {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

async fn drain_sparse_read_results<S>(mut reads: S, target_addr: u64) -> KvResult<()>
where
    S: futures::Stream<Item = KvResult<(SparseGetRange, u64, ReadOutput)>> + Unpin,
{
    let mut first_error = None;
    while let Some(result) = reads.next().await {
        if let Err(err) = materialize_sparse_read_result(result, target_addr) {
            first_error.get_or_insert(err);
        }
    }
    match first_error {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

fn materialize_sparse_read_result(
    result: KvResult<(SparseGetRange, u64, ReadOutput)>,
    target_addr: u64,
) -> KvResult<()> {
    result.and_then(|(range, payload_offset, output)| {
        let ReadOutput::Scratch(buffer) = output else {
            return Ok(());
        };
        let payload_offset = usize::try_from(payload_offset).map_err(|_| {
            KvError::Api(ApiError::InvalidArgument {
                detail: "sparse SSD scratch offset does not fit usize".to_string(),
            })
        })?;
        let payload_len = usize::try_from(range.len).map_err(|_| {
            KvError::Api(ApiError::InvalidArgument {
                detail: "sparse SSD payload length does not fit usize".to_string(),
            })
        })?;
        let destination = checked_add_u64(target_addr, range.dst_offset, "sparse target addr")?;
        unsafe {
            std::ptr::copy_nonoverlapping(
                buffer.as_ptr().add(payload_offset),
                destination as *mut u8,
                payload_len,
            );
        }
        Ok(())
    })
}

fn choose_device_shard_count(limit_bytes: u64) -> usize {
    let max_aligned_shards = (limit_bytes / SSD_ALIGNMENT as u64).max(1) as usize;
    DEFAULT_SHARDS_PER_OWNER.min(max_aligned_shards).max(1)
}

fn select_write_device(
    max_shard_capacities: &[u64],
    aligned_len: u64,
    start_idx: usize,
) -> Option<usize> {
    (0..max_shard_capacities.len())
        .map(|offset| start_idx.wrapping_add(offset) % max_shard_capacities.len())
        .find(|device_idx| max_shard_capacities[*device_idx] >= aligned_len)
}

fn aligned_shard_capacity(capacity_bytes: u64, shard_count: usize) -> KvResult<u64> {
    let raw = capacity_bytes / shard_count as u64;
    let capacity = raw / SSD_ALIGNMENT as u64 * SSD_ALIGNMENT as u64;
    if capacity == 0 {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "kv ssd storage capacity is too small for shard count".to_string(),
        }));
    }
    Ok(capacity)
}

fn build_shard_specs(device_roots: &[SsdDeviceRoot]) -> KvResult<Vec<SsdShardSpec>> {
    if device_roots.is_empty() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "kv ssd storage root_limits must contain at least one path".to_string(),
        }));
    }
    let mut shard_specs = Vec::new();
    for (device_idx, root) in device_roots.iter().enumerate() {
        let shard_count = choose_device_shard_count(root.limit_bytes);
        let shard_capacity = aligned_shard_capacity(root.limit_bytes, shard_count)?;
        for _ in 0..shard_count {
            let shard_id = shard_specs.len();
            shard_specs.push(SsdShardSpec {
                shard_id,
                device_idx,
                capacity: shard_capacity,
            });
        }
    }
    Ok(shard_specs)
}

fn deduplicate_device_roots(root_limits: &[KvSsdStorageRootLimit]) -> KvResult<Vec<SsdDeviceRoot>> {
    if root_limits.is_empty() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "kv ssd storage root_limits must contain at least one path".to_string(),
        }));
    }
    let mut seen_devices = HashSet::new();
    let mut device_roots = Vec::new();
    for root_limit in root_limits {
        fs::create_dir_all(&root_limit.root_dir)
            .map_err(|err| file_error(&root_limit.root_dir, 0, err))?;
        let metadata = fs::metadata(&root_limit.root_dir)
            .map_err(|err| file_error(&root_limit.root_dir, 0, err))?;
        let device_id = metadata.dev();
        if seen_devices.insert(device_id) {
            device_roots.push(SsdDeviceRoot {
                device_id,
                root_dir: root_limit.root_dir.clone(),
                limit_bytes: root_limit.limit_bytes,
            });
        }
    }
    if device_roots.is_empty() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "kv ssd storage root_limits contains no usable device".to_string(),
        }));
    }
    Ok(device_roots)
}

fn open_cache_files(
    device_roots: &[SsdDeviceRoot],
    shard_specs: &[SsdShardSpec],
) -> KvResult<Vec<OpenedSsdShard>> {
    if device_roots.is_empty() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "kv ssd storage root_limits must contain at least one path".to_string(),
        }));
    }
    let mut files = Vec::with_capacity(shard_specs.len());
    for spec in shard_specs {
        let Some(device_root) = device_roots.get(spec.device_idx) else {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "kv ssd shard spec references invalid device index: shard_id={} device_idx={}",
                    spec.shard_id, spec.device_idx
                ),
            }));
        };
        let root_dir = &device_root.root_dir;
        let shards_dir = root_dir.join("shards");
        fs::create_dir_all(&shards_dir).map_err(|err| file_error(&shards_dir, 0, err))?;
        let path = shards_dir.join(format!("shard-{:06}.dat", spec.shard_id));
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(&path)
            .map_err(|err| file_error(&path, 0, err))?;
        file.set_len(spec.capacity)
            .map_err(|err| file_error(&path, 0, err))?;
        files.push(OpenedSsdShard {
            shard_id: spec.shard_id,
            device_idx: spec.device_idx,
            capacity: spec.capacity,
            file,
        });
    }
    Ok(files)
}

fn align_up_usize(value: usize, alignment: usize) -> KvResult<usize> {
    value
        .checked_add(alignment - 1)
        .map(|v| v / alignment * alignment)
        .ok_or_else(|| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!("alignment overflow for value={}", value),
            })
        })
}

fn align_up_u64(value: u64, alignment: u64) -> KvResult<u64> {
    value
        .checked_add(alignment - 1)
        .map(|v| v / alignment * alignment)
        .ok_or_else(|| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!("alignment overflow for value={}", value),
            })
        })
}

pub(crate) fn align_ssd_io_len(len: u64) -> KvResult<u64> {
    align_up_u64(len, SSD_ALIGNMENT as u64)
}

fn checked_add_u64(lhs: u64, rhs: u64, label: &str) -> KvResult<u64> {
    lhs.checked_add(rhs).ok_or_else(|| {
        KvError::Api(ApiError::InvalidArgument {
            detail: format!("kv ssd {label} overflow: {lhs} + {rhs}"),
        })
    })
}

fn validate_direct_io(
    iovecs: impl IntoIterator<Item = (usize, usize)>,
    offset: u64,
) -> io::Result<()> {
    ensure_aligned("offset", offset as usize)?;
    for (addr, len) in iovecs {
        ensure_aligned("buffer address", addr)?;
        ensure_aligned("iovec length", len)?;
    }
    Ok(())
}

fn ensure_aligned(name: &str, value: usize) -> io::Result<()> {
    if value.is_multiple_of(SSD_ALIGNMENT) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("O_DIRECT {name} {value:#x} is not {SSD_ALIGNMENT}-byte aligned"),
        ))
    }
}

fn file_error_for_entry(key: &KvSsdKey, offset: u64, err: io::Error) -> KvError {
    KvError::Api(ApiError::FileWriteError {
        path: format!("kv-ssd://{}@({},{})", key.key, key.put_id.0, key.put_id.1),
        offset,
        detail: err.to_string(),
    })
}

fn file_error(path: &Path, offset: u64, err: io::Error) -> KvError {
    KvError::Api(ApiError::FileWriteError {
        path: path.to_string_lossy().to_string(),
        offset,
        detail: err.to_string(),
    })
}

impl From<io::Error> for KvError {
    fn from(err: io::Error) -> Self {
        KvError::Api(ApiError::FileWriteError {
            path: "kv-ssd://io".to_string(),
            offset: 0,
            detail: err.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use uuid::Uuid;

    #[test]
    fn aligned_buffer_preserves_io_length_and_zero_padding() {
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        for len in [SSD_ALIGNMENT, page_size, page_size + SSD_ALIGNMENT] {
            let payload = vec![0xa5; len - 17];
            let buffer = unsafe {
                AlignedBuffer::copy_from_addr(payload.as_ptr() as u64, payload.len(), len)
            }
            .unwrap();
            assert_eq!(buffer.len(), len);
            assert!((buffer.as_ptr() as usize).is_multiple_of(SSD_ALIGNMENT));
            let bytes = unsafe { std::slice::from_raw_parts(buffer.as_ptr(), len) };
            assert_eq!(&bytes[..payload.len()], payload.as_slice());
            assert!(bytes[payload.len()..].iter().all(|&byte| byte == 0));
        }
    }

    #[test]
    fn aligned_buffer_rejects_invalid_or_overflowing_lengths() {
        assert!(AlignedBuffer::zeroed(0).is_err());
        assert!(AlignedBuffer::zeroed(SSD_ALIGNMENT - 1).is_err());
        assert!(AlignedBuffer::zeroed(usize::MAX - (SSD_ALIGNMENT - 1)).is_err());
    }

    #[test]
    fn sparse_read_window_aligns_both_edges() {
        assert_eq!(sparse_read_window(513, 700, 2048).unwrap(), (512, 1024, 1));
        assert_eq!(sparse_read_window(0, 512, 512).unwrap(), (0, 512, 0));
        assert!(sparse_read_window(1, 512, 512).is_err());
    }

    #[::tokio::test]
    async fn sparse_read_first_error_still_drains_later_results() {
        let observed = Arc::new(AtomicUsize::new(0));
        let observed_for_stream = Arc::clone(&observed);
        let first_error = KvError::Api(ApiError::InvalidArgument {
            detail: "injected sparse read failure".to_string(),
        });
        let reads = futures::stream::iter(vec![
            Err(first_error),
            Ok((
                SparseGetRange {
                    src_offset: 0,
                    dst_offset: 0,
                    len: SSD_ALIGNMENT as u64,
                },
                0,
                ReadOutput::Direct,
            )),
        ])
        .inspect(move |_| {
            observed_for_stream.fetch_add(1, Ordering::Relaxed);
        });

        assert!(drain_sparse_read_results(reads, 0).await.is_err());
        assert_eq!(observed.load(Ordering::Relaxed), 2);
    }

    #[::tokio::test]
    async fn sparse_read_scheduler_honors_max_inflight() {
        let requests = (0..5)
            .map(|index| SparseReadRequest {
                range: SparseGetRange {
                    src_offset: index,
                    dst_offset: index,
                    len: 1,
                },
                file_offset: index,
                payload_offset: 0,
                target: ReadTarget::Scratch { len: SSD_ALIGNMENT },
            })
            .collect::<Vec<_>>();
        let gate = Arc::new(Semaphore::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let scheduler = {
            let gate = Arc::clone(&gate);
            let started = Arc::clone(&started);
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            ::tokio::spawn(run_bounded_sparse_reads(requests, 2, 0, move |request| {
                let gate = Arc::clone(&gate);
                let started = Arc::clone(&started);
                let active = Arc::clone(&active);
                let peak = Arc::clone(&peak);
                async move {
                    started.fetch_add(1, Ordering::AcqRel);
                    let current = active.fetch_add(1, Ordering::AcqRel) + 1;
                    peak.fetch_max(current, Ordering::AcqRel);
                    let permit = gate.acquire_owned().await.unwrap();
                    permit.forget();
                    active.fetch_sub(1, Ordering::AcqRel);
                    Ok((request.range, request.payload_offset, ReadOutput::Direct))
                }
            }))
        };

        ::tokio::time::timeout(Duration::from_secs(1), async {
            while started.load(Ordering::Acquire) < 2 {
                ::tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for _ in 0..10 {
            ::tokio::task::yield_now().await;
        }
        assert_eq!(started.load(Ordering::Acquire), 2);
        assert_eq!(peak.load(Ordering::Acquire), 2);

        gate.add_permits(1);
        ::tokio::time::timeout(Duration::from_secs(1), async {
            while started.load(Ordering::Acquire) < 3 {
                ::tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(peak.load(Ordering::Acquire), 2);

        gate.add_permits(4);
        scheduler.await.unwrap().unwrap();
        assert_eq!(started.load(Ordering::Acquire), 5);
        assert_eq!(active.load(Ordering::Acquire), 0);
        assert_eq!(peak.load(Ordering::Acquire), 2);
    }

    #[::tokio::test]
    async fn sparse_read_scheduler_stops_pending_after_error_and_drains_started() {
        let requests = (0..5)
            .map(|index| SparseReadRequest {
                range: SparseGetRange {
                    src_offset: index,
                    dst_offset: index,
                    len: 1,
                },
                file_offset: index,
                payload_offset: 0,
                target: ReadTarget::Scratch { len: SSD_ALIGNMENT },
            })
            .collect::<Vec<_>>();
        let gate = Arc::new(Semaphore::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let drained = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let scheduler = {
            let gate = Arc::clone(&gate);
            let started = Arc::clone(&started);
            let drained = Arc::clone(&drained);
            ::tokio::spawn(run_bounded_sparse_reads(requests, 2, 0, move |request| {
                let gate = Arc::clone(&gate);
                let started = Arc::clone(&started);
                let drained = Arc::clone(&drained);
                async move {
                    started.fetch_add(1, Ordering::AcqRel);
                    if request.range.src_offset == 0 {
                        return Err(KvError::Api(ApiError::InvalidArgument {
                            detail: "injected sparse read failure".to_string(),
                        }));
                    }
                    let permit = gate.acquire_owned().await.unwrap();
                    permit.forget();
                    drained.store(true, Ordering::Release);
                    Ok((request.range, request.payload_offset, ReadOutput::Direct))
                }
            }))
        };

        ::tokio::time::timeout(Duration::from_secs(1), async {
            while started.load(Ordering::Acquire) < 2 {
                ::tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for _ in 0..10 {
            ::tokio::task::yield_now().await;
        }
        assert_eq!(started.load(Ordering::Acquire), 2);
        assert!(!scheduler.is_finished());
        assert!(!drained.load(Ordering::Acquire));

        gate.add_permits(1);
        assert!(scheduler.await.unwrap().is_err());
        assert_eq!(started.load(Ordering::Acquire), 2);
        assert!(drained.load(Ordering::Acquire));
    }

    #[::tokio::test]
    async fn cancelled_sparse_caller_keeps_pin_and_target_until_delayed_io_terminal() {
        struct TargetDropProbe(Arc<std::sync::atomic::AtomicBool>);

        impl Drop for TargetDropProbe {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let store = new_store(8 * SSD_ALIGNMENT as u64).await;
        let data = vec![0x5au8; SSD_ALIGNMENT];
        store.persist("sparse-cancel", (9, 4), &data).await.unwrap();
        let key = KvSsdKey {
            key: "sparse-cancel".to_string(),
            put_id: (9, 4),
        };
        let batch_pin = {
            let entry = store
                .inner
                .lock()
                .ring
                .pin_read(&key)
                .expect("persisted sparse entry must be pinnable");
            assert_eq!(entry.len, data.len() as u64);
            Arc::new(SsdReadPin {
                inner: Arc::clone(&store.inner),
                space_notify: Arc::clone(&store.space_notify),
                key: key.clone(),
            })
        };
        let delayed_command_pin = Arc::clone(&batch_pin);
        let target_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let target_lifetime =
            KvSsdReadTargetLifetime::new(Arc::new(TargetDropProbe(Arc::clone(&target_dropped))));
        let delayed_command_target = target_lifetime.clone();
        let delayed_terminal = Arc::new(Notify::new());
        let delayed_terminal_task = Arc::clone(&delayed_terminal);
        let (started_tx, started_rx) = oneshot::channel();
        let delayed_io = ::tokio::spawn(async move {
            let _ = started_tx.send(());
            delayed_terminal_task.notified().await;
            drop(delayed_command_pin);
            drop(delayed_command_target);
        });
        started_rx.await.unwrap();

        // Simulate cancellation dropping load_ranges_into_addr while a command
        // already submitted to the SSD worker is still pending.
        drop(batch_pin);
        drop(target_lifetime);
        assert!(store.inner.lock().ring.read_pins.contains_key(&key));
        assert!(!target_dropped.load(Ordering::Acquire));

        delayed_terminal.notify_one();
        delayed_io.await.unwrap();
        assert!(!store.inner.lock().ring.read_pins.contains_key(&key));
        assert!(target_dropped.load(Ordering::Acquire));
    }

    #[::tokio::test]
    async fn native_sparse_ranges_copy_only_requested_payloads() {
        let store = new_store(16 * SSD_ALIGNMENT as u64).await;
        let data = (0..1537)
            .map(|value| (value % 251) as u8)
            .collect::<Vec<_>>();
        store.persist("sparse", (7, 2), &data).await.unwrap();
        let mut target = vec![0xa5u8; 19];
        let ranges = vec![
            SparseGetRange {
                src_offset: 509,
                dst_offset: 0,
                len: 7,
            },
            SparseGetRange {
                src_offset: 1023,
                dst_offset: 11,
                len: 8,
            },
        ];
        let stats = store
            .load_ranges_into_addr(
                "sparse",
                (7, 2),
                data.len() as u64,
                target.as_mut_ptr() as u64,
                target.len() as u64,
                &ranges,
                2,
                KvSsdReadTargetLifetime::new(Arc::new(())),
            )
            .await
            .unwrap();
        assert_eq!(&target[..7], &data[509..516]);
        assert_eq!(&target[7..11], &[0xa5; 4]);
        assert_eq!(&target[11..19], &data[1023..1031]);
        // Both payloads straddle a 512-byte boundary, so each native
        // O_DIRECT read is widened to a 1024-byte scratch window.
        assert_eq!(stats.physical_bytes, 4 * SSD_ALIGNMENT as u64);
    }

    #[::tokio::test]
    async fn native_sparse_aligned_range_reads_into_direct_target() {
        let store = new_store(16 * SSD_ALIGNMENT as u64).await;
        let data = (0..3 * SSD_ALIGNMENT)
            .map(|value| (value % 251) as u8)
            .collect::<Vec<_>>();
        store.persist("sparse-direct", (8, 3), &data).await.unwrap();
        let mut target = AlignedBuffer::zeroed(SSD_ALIGNMENT).unwrap();
        let ranges = [SparseGetRange {
            src_offset: SSD_ALIGNMENT as u64,
            dst_offset: 0,
            len: SSD_ALIGNMENT as u64,
        }];

        let stats = store
            .load_ranges_into_addr(
                "sparse-direct",
                (8, 3),
                data.len() as u64,
                target.as_mut_ptr() as u64,
                target.len() as u64,
                &ranges,
                1,
                KvSsdReadTargetLifetime::new(Arc::new(())),
            )
            .await
            .unwrap();

        let actual = unsafe { std::slice::from_raw_parts(target.as_ptr(), target.len()) };
        assert_eq!(actual, &data[SSD_ALIGNMENT..2 * SSD_ALIGNMENT]);
        assert_eq!(stats.physical_bytes, SSD_ALIGNMENT as u64);
    }

    fn new_root() -> PathBuf {
        std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/mnt/nvme0/mjq_build/push_sglang_fluxon_target"))
            .join("fluxon_kv_ssd_tests")
            .join(Uuid::new_v4().to_string())
    }

    async fn new_store_with_mode(max_bytes: u64, uring_mode: KvSsdUringMode) -> KvSsdStorage {
        KvSsdStorage::new(KvSsdStorageInit {
            root_limits: vec![KvSsdStorageRootLimit {
                root_dir: new_root(),
                limit_bytes: max_bytes,
            }],
            uring_mode,
            backend: KvSsdStorageBackend::Native,
            writeback: Default::default(),
        })
        .await
        .unwrap()
    }

    async fn new_store(max_bytes: u64) -> KvSsdStorage {
        new_store_with_mode(max_bytes, KvSsdUringMode::SingleBuffer).await
    }

    async fn new_foyer_store(max_bytes: u64) -> KvSsdStorage {
        KvSsdStorage::new(KvSsdStorageInit {
            root_limits: vec![KvSsdStorageRootLimit {
                root_dir: new_root(),
                limit_bytes: max_bytes,
            }],
            uring_mode: KvSsdUringMode::SingleBuffer,
            backend: KvSsdStorageBackend::Foyer,
            writeback: Default::default(),
        })
        .await
        .unwrap()
    }

    #[::tokio::test]
    async fn close_is_repeatable_joins_workers_and_rejects_new_operations() {
        let store = new_store(4 * SSD_ALIGNMENT as u64).await;
        store
            .persist("before-close", (1, 0), &[7u8; 500])
            .await
            .unwrap();

        ::tokio::time::timeout(Duration::from_secs(5), store.close())
            .await
            .expect("SSD close must complete")
            .unwrap();
        assert!(
            store
                .devices
                .iter()
                .all(|device| device.runtime.lock().is_none())
        );
        assert!(matches!(
            &*store.worker_close_state.lock().await,
            SsdWorkerCloseState::Closed(Ok(()))
        ));

        store.close().await.unwrap();
        let err = store
            .persist("after-close", (2, 0), &[8u8; 500])
            .await
            .unwrap_err();
        assert!(matches!(err, KvError::Api(ApiError::SystemShutdown { .. })));
    }

    #[::tokio::test]
    async fn close_resumes_worker_join_after_waiting_caller_is_cancelled() {
        let store = Arc::new(new_store(4 * SSD_ALIGNMENT as u64).await);
        let write_tx = store.devices[0]
            .runtime
            .lock()
            .as_ref()
            .expect("test store must have an open writer")
            .required_write_tx
            .clone();

        let store_for_close = Arc::clone(&store);
        let close_task = task::spawn(async move { store_for_close.close().await });
        ::tokio::time::timeout(Duration::from_secs(1), async {
            while store.devices[0].runtime.lock().is_some() {
                task::yield_now().await;
            }
        })
        .await
        .expect("close must install its background worker join");
        assert!(!close_task.is_finished());

        close_task.abort();
        assert!(close_task.await.unwrap_err().is_cancelled());
        drop(write_tx);

        ::tokio::time::timeout(Duration::from_secs(5), store.close())
            .await
            .expect("a later close must resume the installed worker join")
            .unwrap();
        assert!(matches!(
            &*store.worker_close_state.lock().await,
            SsdWorkerCloseState::Closed(Ok(()))
        ));
    }

    #[::tokio::test]
    async fn eviction_publication_backpressures_instead_of_dropping_when_full() {
        let (tx, mut rx) = tokio_mpsc::channel(1);
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        tx.send(vec![SsdReplicaEviction {
            key: "first".to_string(),
            put_id: (1, 0),
        }])
        .await
        .unwrap();

        let publish = publish_ssd_evictions(&tx, &mut shutdown_rx, vec![test_key("second", 2)]);
        tokio::pin!(publish);
        assert!(
            ::tokio::time::timeout(Duration::from_millis(10), publish.as_mut())
                .await
                .is_err()
        );
        assert_eq!(rx.recv().await.unwrap()[0].key, "first");
        ::tokio::time::timeout(Duration::from_secs(1), publish)
            .await
            .expect("eviction publish must resume when capacity is available");
        assert_eq!(rx.recv().await.unwrap()[0].key, "second");
    }

    #[::tokio::test]
    async fn eviction_publication_unblocks_when_storage_shutdown_starts() {
        let (tx, _rx) = tokio_mpsc::channel(1);
        tx.send(vec![SsdReplicaEviction {
            key: "first".to_string(),
            put_id: (1, 0),
        }])
        .await
        .unwrap();
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let publish = publish_ssd_evictions(&tx, &mut shutdown_rx, vec![test_key("second", 2)]);
        tokio::pin!(publish);
        assert!(
            ::tokio::time::timeout(Duration::from_millis(10), publish.as_mut())
                .await
                .is_err()
        );
        shutdown_tx.send_replace(true);
        ::tokio::time::timeout(Duration::from_secs(1), publish)
            .await
            .expect("shutdown must unblock eviction publication");
    }

    #[::tokio::test]
    async fn read_admission_enforces_byte_and_op_budgets() {
        let admission = SsdReadAdmission::new(1024, 2);
        let first = admission.acquire(512).await;
        let second = admission.acquire(512).await;
        assert_eq!(admission.bytes.available_permits(), 0);
        assert_eq!(admission.ops.available_permits(), 0);

        let blocked = admission.acquire(1);
        tokio::pin!(blocked);
        assert!(
            ::tokio::time::timeout(Duration::from_millis(10), blocked.as_mut())
                .await
                .is_err()
        );

        drop(first);
        let third = ::tokio::time::timeout(Duration::from_secs(1), blocked)
            .await
            .expect("read admission must resume when both budgets are available");
        assert_eq!(admission.bytes.available_permits(), 511);
        assert_eq!(admission.ops.available_permits(), 0);

        drop(second);
        drop(third);
        assert_eq!(admission.bytes.available_permits(), 1024);
        assert_eq!(admission.ops.available_permits(), 2);
    }

    #[::tokio::test]
    async fn oversized_read_uses_the_full_byte_budget_exclusively() {
        let admission = SsdReadAdmission::new(1024, 2);
        let existing = admission.acquire(512).await;

        let oversized = admission.acquire(2048);
        tokio::pin!(oversized);
        assert!(
            ::tokio::time::timeout(Duration::from_millis(10), oversized.as_mut())
                .await
                .is_err()
        );
        drop(existing);

        let oversized = ::tokio::time::timeout(Duration::from_secs(1), oversized)
            .await
            .expect("oversized read must proceed after earlier reads finish");
        assert_eq!(admission.bytes.available_permits(), 0);

        let small = admission.acquire(1);
        tokio::pin!(small);
        assert!(
            ::tokio::time::timeout(Duration::from_millis(10), small.as_mut())
                .await
                .is_err()
        );
        drop(oversized);
        let small = ::tokio::time::timeout(Duration::from_secs(1), small)
            .await
            .expect("small read must resume after the oversized read finishes");
        drop(small);
        assert_eq!(admission.bytes.available_permits(), 1024);
    }

    #[::tokio::test]
    async fn op_limited_waiter_does_not_consume_byte_budget() {
        let admission = SsdReadAdmission::new(1024, 1);
        let first = admission.acquire(1).await;

        let blocked = admission.acquire(1);
        tokio::pin!(blocked);
        assert!(
            ::tokio::time::timeout(Duration::from_millis(10), blocked.as_mut())
                .await
                .is_err()
        );
        assert_eq!(admission.ops.available_permits(), 0);
        assert_eq!(admission.bytes.available_permits(), 1023);

        drop(first);
        let second = ::tokio::time::timeout(Duration::from_secs(1), blocked)
            .await
            .expect("read admission must resume when an operation slot is released");
        drop(second);
        assert_eq!(admission.ops.available_permits(), 1);
        assert_eq!(admission.bytes.available_permits(), 1024);
    }

    #[test]
    fn staged_read_byte_budget_covers_128_full_chunks() {
        assert_eq!(
            DEFAULT_READ_MAX_INFLIGHT_BYTES
                / usize::try_from(DEFAULT_READ_TRANSFER_PIPELINE_CHUNK_BYTES).unwrap(),
            128
        );
    }

    #[::tokio::test]
    async fn foyer_backend_persists_and_loads_from_disk_without_memory_admission() {
        let store = new_foyer_store(128 * 1024 * 1024).await;
        let data = (0..8193).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let put_id = (91, 2);

        store.persist("foyer", put_id, &data).await.unwrap();
        let foyer = store.foyer.as_ref().unwrap();
        assert_eq!(foyer.memory_usage(), 0);

        let mut loaded = vec![0u8; data.len()];
        store
            .load_into_addr(
                "foyer",
                put_id,
                loaded.as_mut_ptr() as u64,
                data.len() as u64,
                loaded.len() as u64,
            )
            .await
            .unwrap();
        assert_eq!(loaded, data);
        assert_eq!(foyer.source_counts(), (0, 1, 0));
        assert_eq!(foyer.memory_usage(), 0);

        let mut chunked = vec![0u8; data.len()];
        let (ready_tx, mut ready_rx) = tokio_mpsc::channel(8);
        store
            .load_into_addr_chunks(
                "foyer",
                put_id,
                chunked.as_mut_ptr() as u64,
                data.len() as u64,
                chunked.len() as u64,
                4096,
                4,
                ready_tx,
            )
            .await
            .unwrap();
        let mut chunks = Vec::new();
        while let Some(chunk) = ready_rx.recv().await {
            chunks.push((chunk.offset, chunk.len));
        }
        assert_eq!(chunked, data);
        assert_eq!(chunks, vec![(0, 4096), (4096, 4096), (8192, 1)]);
        assert_eq!(foyer.source_counts(), (0, 2, 0));
        assert_eq!(foyer.memory_usage(), 0);
    }

    #[::tokio::test]
    async fn foyer_backend_backpressures_concurrent_persists_until_durable() {
        const ENTRY_COUNT: usize = 24;
        const ENTRY_BYTES: usize = 1024 * 1024;

        let store = new_foyer_store(128 * 1024 * 1024).await;
        let keys = (0..ENTRY_COUNT)
            .map(|idx| format!("foyer-concurrent-{idx}"))
            .collect::<Vec<_>>();
        let values = (0..ENTRY_COUNT)
            .map(|idx| vec![(idx % 251) as u8; ENTRY_BYTES])
            .collect::<Vec<_>>();
        let persists = keys
            .iter()
            .zip(values.iter())
            .enumerate()
            .map(|(idx, (key, value))| store.persist(key, (100 + idx as u64, 0), value));
        for result in futures::future::join_all(persists).await {
            result.unwrap();
        }

        let foyer = store.foyer.as_ref().unwrap();
        assert_eq!(foyer.memory_usage(), 0);
        for (idx, (key, expected)) in keys.iter().zip(values.iter()).enumerate() {
            let mut loaded = vec![0u8; expected.len()];
            store
                .load_into_addr(
                    key,
                    (100 + idx as u64, 0),
                    loaded.as_mut_ptr() as u64,
                    loaded.len() as u64,
                    loaded.len() as u64,
                )
                .await
                .unwrap();
            assert_eq!(&loaded, expected);
        }
        assert_eq!(foyer.memory_usage(), 0);
    }

    #[::tokio::test]
    async fn foyer_backend_rejects_multiple_roots() {
        let err = KvSsdStorage::new(KvSsdStorageInit {
            root_limits: vec![
                KvSsdStorageRootLimit {
                    root_dir: new_root(),
                    limit_bytes: 128 * 1024 * 1024,
                },
                KvSsdStorageRootLimit {
                    root_dir: new_root(),
                    limit_bytes: 128 * 1024 * 1024,
                },
            ],
            uring_mode: KvSsdUringMode::SingleBuffer,
            backend: KvSsdStorageBackend::Foyer,
            writeback: Default::default(),
        })
        .await
        .unwrap_err();
        assert!(format!("{err}").contains("requires exactly one root"));
    }

    fn test_key(key: &str, version: u64) -> KvSsdKey {
        KvSsdKey {
            key: key.to_string(),
            put_id: (version, 0),
        }
    }

    #[::tokio::test]
    async fn write_batch_coalesces_commands_until_byte_limit() {
        let config = KvSsdWritebackConfig::default();
        let admission = SsdDeviceWriteAdmission::new(&config);
        let mut required = VecDeque::new();
        let mut early = VecDeque::new();

        for version in 0..3 {
            let len = SSD_ALIGNMENT;
            let (done_tx, _done_rx) = oneshot::channel();
            early.push_back(WriteCommand {
                key: test_key("batch", version),
                entry_len: len as u64,
                data: AlignedBuffer::zeroed(len).unwrap(),
                priority: KvSsdPersistPriority::new(KvSsdPersistClass::BestEffortEarly),
                queued_at: Instant::now(),
                device_permit: admission
                    .acquire(KvSsdPersistClass::BestEffortEarly, len as u64)
                    .await
                    .unwrap(),
                done_tx,
            });
        }

        let max_bytes = 2 * SSD_ALIGNMENT as u64;
        assert!(pending_write_limits_reached(
            &required, &early, 32, max_bytes
        ));
        let batch = take_write_batch(&mut required, &mut early, 32, max_bytes);
        assert_eq!(batch.len(), 2);
        assert!(required.is_empty());
        assert_eq!(early.len(), 1);
    }

    fn prepare_ready(ring: &mut SsdRingBuffer, key: &KvSsdKey) -> SsdIndexEntry {
        match ring.prepare_write(key.clone(), 500).unwrap() {
            SsdPreparedWrite::Ready { entry, .. } => entry,
            other => panic!("expected ready SSD write, got {other:?}"),
        }
    }

    #[test]
    fn shard_specs_preserve_per_device_limits() {
        let device_roots = vec![
            SsdDeviceRoot {
                device_id: 1,
                root_dir: PathBuf::from("/tmp/fluxon-test-ssd-a"),
                limit_bytes: 4 * SSD_ALIGNMENT as u64,
            },
            SsdDeviceRoot {
                device_id: 2,
                root_dir: PathBuf::from("/tmp/fluxon-test-ssd-b"),
                limit_bytes: 8 * SSD_ALIGNMENT as u64,
            },
        ];

        let shard_specs = build_shard_specs(&device_roots).unwrap();
        let mut capacity_by_device = [0u64; 2];
        for spec in shard_specs {
            capacity_by_device[spec.device_idx] =
                capacity_by_device[spec.device_idx].saturating_add(spec.capacity);
        }

        assert_eq!(capacity_by_device[0], 4 * SSD_ALIGNMENT as u64);
        assert_eq!(capacity_by_device[1], 8 * SSD_ALIGNMENT as u64);
    }

    #[test]
    fn write_device_selection_skips_devices_with_undersized_shards() {
        let capacities = [1024, 4096];

        assert_eq!(select_write_device(&capacities, 2048, 0), Some(1));
        assert_eq!(select_write_device(&capacities, 2048, 1), Some(1));
        assert_eq!(select_write_device(&capacities, 8192, 0), None);
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum ReferenceSqeShape {
        Read,
        Write,
        Readv,
        Writev,
    }

    fn fluxon_current_sqe_shape(io_type: IoType, iovec_count: usize) -> ReferenceSqeShape {
        assert!(iovec_count > 0);
        match io_type {
            IoType::Read => ReferenceSqeShape::Read,
            IoType::Write => ReferenceSqeShape::Write,
            IoType::Readv => ReferenceSqeShape::Readv,
            IoType::Writev => ReferenceSqeShape::Writev,
        }
    }

    fn ringline_reference_direct_io_sqe_shape(
        io_type: IoType,
        iovec_count: usize,
    ) -> Option<ReferenceSqeShape> {
        assert!(iovec_count > 0);
        match (io_type, iovec_count) {
            (IoType::Readv, 1) => Some(ReferenceSqeShape::Read),
            (IoType::Writev, 1) => Some(ReferenceSqeShape::Write),
            (_, _) => None,
        }
    }

    fn ringline_reference_direct_io_allowed(buf_addr: u64, len: u64, offset: u64) -> bool {
        len != 0
            && buf_addr.is_multiple_of(SSD_ALIGNMENT as u64)
            && len.is_multiple_of(SSD_ALIGNMENT as u64)
            && offset.is_multiple_of(SSD_ALIGNMENT as u64)
    }

    #[::tokio::test]
    async fn persist_and_load_roundtrip() {
        let store = new_store(1024 * 1024).await;
        let data = b"hello from ssd";
        let put_id = (10, 1);
        store.persist("k", put_id, data).await.unwrap();

        let mut out = vec![0u8; data.len()];
        store
            .load_into_addr(
                "k",
                put_id,
                out.as_mut_ptr() as u64,
                out.len() as u64,
                out.len() as u64,
            )
            .await
            .unwrap();
        assert_eq!(out, data);
    }

    #[::tokio::test]
    async fn required_admission_retains_credit_when_early_batches_are_saturated() {
        let store = new_store(1024 * 1024).await;
        let lengths = [SSD_ALIGNMENT as u64];
        let mut early = Vec::new();
        for _ in 0..3 {
            early.push(
                store
                    .try_acquire_persist_batch(KvSsdPersistClass::BestEffortEarly, &lengths)
                    .unwrap()
                    .expect("the three Early credits must be available"),
            );
        }
        assert!(
            store
                .try_acquire_persist_batch(KvSsdPersistClass::BestEffortEarly, &lengths)
                .unwrap()
                .is_none(),
            "Early writes must not consume the reserved Required batch credit"
        );
        let mut waiting =
            Box::pin(store.acquire_persist_batch(KvSsdPersistClass::BestEffortEarly, &lengths));
        assert!(futures::poll!(&mut waiting).is_pending());
        let required = store
            .try_acquire_persist_batch(KvSsdPersistClass::RequiredLastBacking, &lengths)
            .unwrap()
            .expect("Required write must retain one admission credit");
        drop(required);
        drop(waiting);
        drop(early);

        // A waiter that already owns an early batch must release it on cancel,
        // without borrowing total credit while the early byte quota is full.
        let held_bytes = store
            .write_admission
            .early_bytes
            .clone()
            .acquire_many_owned(store.write_admission.max_early_bytes)
            .await
            .unwrap();
        let mut waiting =
            Box::pin(store.acquire_persist_batch(KvSsdPersistClass::BestEffortEarly, &lengths));
        assert!(futures::poll!(&mut waiting).is_pending());
        assert_eq!(store.write_admission.early_batches.available_permits(), 2);
        assert_eq!(store.write_admission.total_batches.available_permits(), 4);
        let required = store
            .try_acquire_persist_batch(KvSsdPersistClass::RequiredLastBacking, &lengths)
            .unwrap()
            .expect("an early byte waiter must leave Required credit available");
        drop(required);
        drop(waiting);
        assert_eq!(store.write_admission.early_batches.available_permits(), 3);
        assert_eq!(store.write_admission.total_batches.available_permits(), 4);
        drop(held_bytes);
        store.close().await.unwrap();
    }

    #[::tokio::test]
    async fn native_multi_sqe_batch_preserves_variable_length_values() {
        let store = new_store(16 * 1024 * 1024).await;
        let values = [700usize, 4097, 65_537, 257_003]
            .into_iter()
            .enumerate()
            .map(|(index, len)| {
                (
                    format!("batch-{index}"),
                    (100 + index as u64, 0),
                    vec![(index as u8).wrapping_add(17); len],
                )
            })
            .collect::<Vec<_>>();
        let persists = values
            .iter()
            .map(|(key, put_id, value)| store.persist(key, *put_id, value));
        for result in futures::future::join_all(persists).await {
            result.unwrap();
        }
        for (key, put_id, expected) in values {
            let mut loaded = vec![0u8; expected.len()];
            store
                .load_into_addr(
                    &key,
                    put_id,
                    loaded.as_mut_ptr() as u64,
                    loaded.len() as u64,
                    loaded.len() as u64,
                )
                .await
                .unwrap();
            assert_eq!(loaded, expected);
        }
    }

    #[::tokio::test]
    async fn persist_and_load_roundtrip_supports_iovec_ablation_mode() {
        let store = new_store_with_mode(1024 * 1024, KvSsdUringMode::Iovec).await;
        let data = b"hello from ssd through iovec";
        let put_id = (10, 2);
        store.persist("k-iovec", put_id, data).await.unwrap();

        let mut out = vec![0u8; data.len()];
        store
            .load_into_addr(
                "k-iovec",
                put_id,
                out.as_mut_ptr() as u64,
                out.len() as u64,
                out.len() as u64,
            )
            .await
            .unwrap();
        assert_eq!(out, data);
    }

    #[::tokio::test]
    async fn aligned_load_roundtrip_uses_direct_target() {
        let store = new_store(1024 * 1024).await;
        let data = (0..4096).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let put_id = (11, 1);
        store.persist("aligned", put_id, &data).await.unwrap();

        let mut out = AlignedBuffer::zeroed(data.len()).unwrap();
        let target_addr = out.as_mut_ptr() as u64;
        let entry = {
            let key = KvSsdKey {
                key: "aligned".to_string(),
                put_id,
            };
            store.inner.lock().ring.get(&key).unwrap()
        };
        assert_eq!(
            choose_read_path(&entry, target_addr, data.len() as u64, data.len() as u64),
            SsdReadPath::Direct
        );

        store
            .load_into_addr(
                "aligned",
                put_id,
                target_addr,
                data.len() as u64,
                data.len() as u64,
            )
            .await
            .unwrap();

        let out_slice = unsafe { std::slice::from_raw_parts(out.as_ptr(), data.len()) };
        assert_eq!(out_slice, data.as_slice());
    }

    #[::tokio::test]
    async fn chunked_load_roundtrip_streams_ready_chunks() {
        let store = new_store(1024 * 1024).await;
        let data = (0..2500).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let put_id = (13, 1);
        store.persist("chunked", put_id, &data).await.unwrap();

        let mut out =
            AlignedBuffer::zeroed(align_ssd_io_len(data.len() as u64).unwrap() as usize).unwrap();
        let target_addr = out.as_mut_ptr() as u64;
        let (tx, mut rx) = ::tokio::sync::mpsc::channel(2);
        let producer = store.load_into_addr_chunks(
            "chunked",
            put_id,
            target_addr,
            data.len() as u64,
            out.len() as u64,
            1024,
            2,
            tx,
        );
        let consumer = async {
            let mut chunks = Vec::new();
            while let Some(chunk) = rx.recv().await {
                chunks.push((chunk.offset, chunk.len));
            }
            chunks
        };
        let (producer_res, mut chunks) = ::tokio::join!(producer, consumer);
        producer_res.unwrap();
        chunks.sort_unstable();
        assert_eq!(chunks, vec![(0, 1024), (1024, 1024), (2048, 452)]);

        let out_slice = unsafe { std::slice::from_raw_parts(out.as_ptr(), data.len()) };
        assert_eq!(out_slice, data.as_slice());
    }

    #[::tokio::test]
    async fn chunked_load_drains_issued_reads_after_a_chunk_error() {
        let store = new_store(1024 * 1024).await;
        let data = (0..2500).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let put_id = (13, 2);
        store.persist("chunk-error", put_id, &data).await.unwrap();

        let mut out = AlignedBuffer::zeroed(2560).unwrap();
        let (ready_tx, _ready_rx) = ::tokio::sync::mpsc::channel(3);
        let err = store
            .load_into_addr_chunks(
                "chunk-error",
                put_id,
                out.as_mut_ptr() as u64,
                data.len() as u64,
                data.len() as u64,
                1024,
                3,
                ready_tx,
            )
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("chunk target capacity too small"));

        let out_slice = unsafe { std::slice::from_raw_parts(out.as_ptr(), 2048) };
        assert_eq!(out_slice, &data[..2048]);
    }

    #[::tokio::test]
    async fn chunked_load_drains_issued_reads_when_ready_receiver_closes() {
        let store = new_store(1024 * 1024).await;
        let data = (0..2500).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let put_id = (13, 3);
        store.persist("chunk-closed", put_id, &data).await.unwrap();

        let mut out = AlignedBuffer::zeroed(2560).unwrap();
        let (ready_tx, ready_rx) = ::tokio::sync::mpsc::channel(1);
        drop(ready_rx);
        let err = store
            .load_into_addr_chunks(
                "chunk-closed",
                put_id,
                out.as_mut_ptr() as u64,
                data.len() as u64,
                out.len() as u64,
                1024,
                3,
                ready_tx,
            )
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("chunk ready queue closed"));

        let out_slice = unsafe { std::slice::from_raw_parts(out.as_ptr(), data.len()) };
        assert_eq!(out_slice, data.as_slice());
    }

    #[test]
    fn read_path_uses_direct_for_aligned_target_with_enough_capacity() {
        let aligned = SsdIndexEntry {
            shard_id: 0,
            begin: 0,
            len: 4096,
            aligned_len: 4096,
            file_offset: 0,
        };
        assert_eq!(
            choose_read_path(&aligned, 4096, 4096, 4096),
            SsdReadPath::Direct
        );
        assert_eq!(
            choose_read_path(&aligned, 4097, 4096, 4096),
            SsdReadPath::Scratch
        );

        let unaligned_len = SsdIndexEntry {
            len: 500,
            aligned_len: 512,
            ..aligned
        };
        assert_eq!(
            choose_read_path(&unaligned_len, 4096, 500, 512),
            SsdReadPath::Direct
        );
        assert_eq!(
            choose_read_path(&unaligned_len, 4096, 500, 500),
            SsdReadPath::Scratch
        );
    }

    #[test]
    fn chunk_read_path_matches_ringline_alignment_plus_stage_capacity() {
        let cases = [
            (4096, 512, 512, 0, SsdReadPath::Direct),
            (4097, 512, 512, 0, SsdReadPath::Scratch),
            (4096, 500, 512, 0, SsdReadPath::Scratch),
            (4096, 512, 511, 0, SsdReadPath::Scratch),
            (4096, 512, 512, 1, SsdReadPath::Scratch),
            (4096, 0, 512, 0, SsdReadPath::Scratch),
        ];

        for (target_addr, read_len, target_len, file_offset, expected) in cases {
            let ringline_direct =
                ringline_reference_direct_io_allowed(target_addr, read_len, file_offset);
            assert_eq!(
                choose_chunk_read_path(target_addr, read_len, target_len, file_offset),
                expected
            );
            assert_eq!(
                expected == SsdReadPath::Direct,
                ringline_direct && target_len >= read_len
            );
        }
    }

    #[test]
    fn full_read_path_adds_payload_contract_to_ringline_alignment() {
        let entry = SsdIndexEntry {
            shard_id: 0,
            begin: 0,
            len: 500,
            aligned_len: 512,
            file_offset: 0,
        };
        let target_addr = 4096;

        assert!(ringline_reference_direct_io_allowed(
            target_addr,
            entry.aligned_len,
            entry.file_offset
        ));
        assert_eq!(
            choose_read_path(&entry, target_addr, entry.len, entry.aligned_len),
            SsdReadPath::Direct
        );
        assert_eq!(
            choose_read_path(&entry, target_addr, entry.len - 1, entry.aligned_len),
            SsdReadPath::Scratch
        );
        assert_eq!(
            choose_read_path(&entry, target_addr, entry.len, entry.len),
            SsdReadPath::Scratch
        );
    }

    #[test]
    fn ringline_reference_uses_single_buffer_direct_io_opcodes() {
        assert_eq!(
            fluxon_current_sqe_shape(IoType::Readv, 1),
            ReferenceSqeShape::Readv
        );
        assert_eq!(
            ringline_reference_direct_io_sqe_shape(IoType::Readv, 1),
            Some(ReferenceSqeShape::Read)
        );
        assert_eq!(
            fluxon_current_sqe_shape(IoType::Writev, 1),
            ReferenceSqeShape::Writev
        );
        assert_eq!(
            ringline_reference_direct_io_sqe_shape(IoType::Writev, 1),
            Some(ReferenceSqeShape::Write)
        );
        assert_eq!(
            ringline_reference_direct_io_sqe_shape(IoType::Readv, 2),
            None
        );
    }

    #[test]
    fn submit_wait_error_action_classifies_transient_errors() {
        let interrupted = io::Error::from(io::ErrorKind::Interrupted);
        let busy = io::Error::from_raw_os_error(libc::EBUSY);
        let invalid = io::Error::from_raw_os_error(libc::EINVAL);

        assert_eq!(
            submit_wait_error_action(&interrupted),
            SubmitWaitAction::Retry
        );
        assert_eq!(
            submit_wait_error_action(&busy),
            SubmitWaitAction::DrainCompletions
        );
        assert_eq!(
            submit_wait_error_action(&invalid),
            SubmitWaitAction::FailWorker
        );
    }

    #[derive(Clone, Copy, Debug)]
    enum PerfOpcode {
        FluxonReadv,
        FluxonWritev,
        RinglineRead,
        RinglineWrite,
    }

    #[derive(Clone, Copy, Debug)]
    struct PerfResult {
        opcode: PerfOpcode,
        ops: usize,
        bytes_per_op: usize,
        elapsed: std::time::Duration,
    }

    impl PerfResult {
        fn ns_per_op(&self) -> f64 {
            self.elapsed.as_nanos() as f64 / self.ops as f64
        }

        fn mib_per_sec(&self) -> f64 {
            let bytes = self.ops as f64 * self.bytes_per_op as f64;
            bytes / self.elapsed.as_secs_f64() / 1024.0 / 1024.0
        }
    }

    fn best_perf_result(results: &[PerfResult]) -> PerfResult {
        results
            .iter()
            .min_by_key(|result| result.elapsed)
            .copied()
            .unwrap()
    }

    fn median_perf_result(results: &[PerfResult]) -> PerfResult {
        let mut sorted = results.to_vec();
        sorted.sort_by_key(|result| result.elapsed);
        sorted[sorted.len() / 2]
    }

    fn print_perf_pair_result(
        stat: &str,
        label: &str,
        fluxon: PerfResult,
        ringline: PerfResult,
        rounds: usize,
    ) {
        println!(
            "uring opcode perf {stat}-of-{rounds} {label}: {:?}: ops={} bytes/op={} elapsed={:?} ns/op={:.1} MiB/s={:.1}",
            fluxon.opcode,
            fluxon.ops,
            fluxon.bytes_per_op,
            fluxon.elapsed,
            fluxon.ns_per_op(),
            fluxon.mib_per_sec()
        );
        println!(
            "uring opcode perf {stat}-of-{rounds} {label}: {:?}: ops={} bytes/op={} elapsed={:?} ns/op={:.1} MiB/s={:.1}",
            ringline.opcode,
            ringline.ops,
            ringline.bytes_per_op,
            ringline.elapsed,
            ringline.ns_per_op(),
            ringline.mib_per_sec()
        );
        println!(
            "uring opcode perf delta {stat}-of-{rounds} {label}: ringline-style relative to Fluxon = {:.2}%",
            (fluxon.ns_per_op() - ringline.ns_per_op()) / fluxon.ns_per_op() * 100.0
        );
    }

    fn print_perf_pair(
        label: &str,
        fluxon_results: &[PerfResult],
        ringline_results: &[PerfResult],
    ) {
        assert_eq!(fluxon_results.len(), ringline_results.len());
        let rounds = fluxon_results.len();
        print_perf_pair_result(
            "best",
            label,
            best_perf_result(fluxon_results),
            best_perf_result(ringline_results),
            rounds,
        );
        print_perf_pair_result(
            "median",
            label,
            median_perf_result(fluxon_results),
            median_perf_result(ringline_results),
            rounds,
        );
    }

    fn run_uring_io_perf(
        opcode: PerfOpcode,
        fd: RawFd,
        buffer: &mut AlignedBuffer,
        ops: usize,
        bytes_per_op: usize,
        offset_slots: usize,
    ) -> io::Result<PerfResult> {
        assert!(offset_slots > 0);
        let mut ring = IoUring::builder().build(64)?;
        let start = std::time::Instant::now();
        for idx in 0..ops {
            let offset = u64::try_from((idx % offset_slots) * bytes_per_op).unwrap();
            let iovec = libc::iovec {
                iov_base: buffer.as_mut_ptr() as *mut libc::c_void,
                iov_len: bytes_per_op,
            };
            let sqe = match opcode {
                PerfOpcode::FluxonReadv => {
                    opcode::Readv::new(Fd(fd), &iovec, 1).offset(offset).build()
                }
                PerfOpcode::FluxonWritev => opcode::Writev::new(Fd(fd), &iovec, 1)
                    .offset(offset)
                    .build(),
                PerfOpcode::RinglineRead => {
                    opcode::Read::new(Fd(fd), buffer.as_mut_ptr(), bytes_per_op as _)
                        .offset(offset)
                        .build()
                }
                PerfOpcode::RinglineWrite => {
                    opcode::Write::new(Fd(fd), buffer.as_ptr(), bytes_per_op as _)
                        .offset(offset)
                        .build()
                }
            }
            .user_data((idx + 1) as u64);
            unsafe {
                ring.submission()
                    .push(&sqe)
                    .map_err(|_| io::Error::other("submission queue full"))?;
            }
            ring.submit_and_wait(1)?;
            let mut cq = ring.completion();
            let cqe: io_uring::cqueue::Entry = cq
                .next()
                .ok_or_else(|| io::Error::other("missing completion"))?;
            if cqe.result() != bytes_per_op as i32 {
                return Err(io::Error::other(format!(
                    "short uring perf completion: {} != {}",
                    cqe.result(),
                    bytes_per_op
                )));
            }
        }
        Ok(PerfResult {
            opcode,
            ops,
            bytes_per_op,
            elapsed: start.elapsed(),
        })
    }

    #[test]
    #[ignore = "manual perf comparison for Fluxon Readv/Writev vs ringline-style Read/Write"]
    fn perf_compare_fluxon_iovecs_with_ringline_single_buffer_ops() {
        let dir = std::env::temp_dir().join(format!("fluxon-kv-ssd-perf-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        for (bytes_per_op, ops, offset_slots, rounds) in [
            (4096usize, 256usize, 128usize, 3usize),
            (1024 * 1024usize, 12usize, 12usize, 3usize),
            (10 * 1024 * 1024usize, 3usize, 3usize, 3usize),
        ] {
            assert!(bytes_per_op.is_multiple_of(SSD_ALIGNMENT));
            let file_len = bytes_per_op.checked_mul(offset_slots).unwrap();
            let path = dir.join(format!("direct-{bytes_per_op}.dat"));
            let file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .custom_flags(libc::O_DIRECT)
                .open(&path)
                .unwrap();
            file.set_len(file_len as u64).unwrap();

            let mut seed = AlignedBuffer::zeroed(bytes_per_op).unwrap();
            unsafe {
                std::ptr::write_bytes(seed.as_mut_ptr(), 0x5a, bytes_per_op);
            }
            for slot in 0..offset_slots {
                let offset = u64::try_from(slot * bytes_per_op).unwrap();
                let written = unsafe {
                    libc::pwrite(
                        file.as_raw_fd(),
                        seed.as_ptr() as *const _,
                        bytes_per_op,
                        offset as libc::off_t,
                    )
                };
                assert_eq!(written, bytes_per_op as isize);
            }

            let mut readv_buffer = AlignedBuffer::zeroed(bytes_per_op).unwrap();
            let mut read_buffer = AlignedBuffer::zeroed(bytes_per_op).unwrap();
            let mut writev_buffer = AlignedBuffer::zeroed(bytes_per_op).unwrap();
            let mut write_buffer = AlignedBuffer::zeroed(bytes_per_op).unwrap();
            unsafe {
                std::ptr::write_bytes(writev_buffer.as_mut_ptr(), 0xa5, bytes_per_op);
                std::ptr::write_bytes(write_buffer.as_mut_ptr(), 0x3c, bytes_per_op);
            }

            let warmup_ops = ops.min(16);
            let _ = run_uring_io_perf(
                PerfOpcode::FluxonReadv,
                file.as_raw_fd(),
                &mut readv_buffer,
                warmup_ops,
                bytes_per_op,
                offset_slots,
            )
            .unwrap();
            let _ = run_uring_io_perf(
                PerfOpcode::RinglineRead,
                file.as_raw_fd(),
                &mut read_buffer,
                warmup_ops,
                bytes_per_op,
                offset_slots,
            )
            .unwrap();
            let _ = run_uring_io_perf(
                PerfOpcode::FluxonWritev,
                file.as_raw_fd(),
                &mut writev_buffer,
                warmup_ops,
                bytes_per_op,
                offset_slots,
            )
            .unwrap();
            let _ = run_uring_io_perf(
                PerfOpcode::RinglineWrite,
                file.as_raw_fd(),
                &mut write_buffer,
                warmup_ops,
                bytes_per_op,
                offset_slots,
            )
            .unwrap();

            let mut readv_results = Vec::with_capacity(rounds);
            let mut read_results = Vec::with_capacity(rounds);
            let mut writev_results = Vec::with_capacity(rounds);
            let mut write_results = Vec::with_capacity(rounds);
            for round_idx in 0..rounds {
                if round_idx % 2 == 0 {
                    readv_results.push(
                        run_uring_io_perf(
                            PerfOpcode::FluxonReadv,
                            file.as_raw_fd(),
                            &mut readv_buffer,
                            ops,
                            bytes_per_op,
                            offset_slots,
                        )
                        .unwrap(),
                    );
                    read_results.push(
                        run_uring_io_perf(
                            PerfOpcode::RinglineRead,
                            file.as_raw_fd(),
                            &mut read_buffer,
                            ops,
                            bytes_per_op,
                            offset_slots,
                        )
                        .unwrap(),
                    );
                } else {
                    read_results.push(
                        run_uring_io_perf(
                            PerfOpcode::RinglineRead,
                            file.as_raw_fd(),
                            &mut read_buffer,
                            ops,
                            bytes_per_op,
                            offset_slots,
                        )
                        .unwrap(),
                    );
                    readv_results.push(
                        run_uring_io_perf(
                            PerfOpcode::FluxonReadv,
                            file.as_raw_fd(),
                            &mut readv_buffer,
                            ops,
                            bytes_per_op,
                            offset_slots,
                        )
                        .unwrap(),
                    );
                }

                if round_idx % 2 == 0 {
                    writev_results.push(
                        run_uring_io_perf(
                            PerfOpcode::FluxonWritev,
                            file.as_raw_fd(),
                            &mut writev_buffer,
                            ops,
                            bytes_per_op,
                            offset_slots,
                        )
                        .unwrap(),
                    );
                    write_results.push(
                        run_uring_io_perf(
                            PerfOpcode::RinglineWrite,
                            file.as_raw_fd(),
                            &mut write_buffer,
                            ops,
                            bytes_per_op,
                            offset_slots,
                        )
                        .unwrap(),
                    );
                } else {
                    write_results.push(
                        run_uring_io_perf(
                            PerfOpcode::RinglineWrite,
                            file.as_raw_fd(),
                            &mut write_buffer,
                            ops,
                            bytes_per_op,
                            offset_slots,
                        )
                        .unwrap(),
                    );
                    writev_results.push(
                        run_uring_io_perf(
                            PerfOpcode::FluxonWritev,
                            file.as_raw_fd(),
                            &mut writev_buffer,
                            ops,
                            bytes_per_op,
                            offset_slots,
                        )
                        .unwrap(),
                    );
                }
            }

            print_perf_pair("read", &readv_results, &read_results);
            print_perf_pair("write", &writev_results, &write_results);

            fs::remove_file(&path).ok();
        }
        fs::remove_dir(&dir).ok();
    }

    #[derive(Clone, Copy, Debug)]
    enum StoragePerfOp {
        Persist,
        Load,
    }

    #[derive(Clone, Copy, Debug)]
    struct StoragePerfResult {
        mode: KvSsdUringMode,
        op: StoragePerfOp,
        ops: usize,
        bytes_per_op: usize,
        elapsed: std::time::Duration,
    }

    impl StoragePerfResult {
        fn ns_per_op(&self) -> f64 {
            self.elapsed.as_nanos() as f64 / self.ops as f64
        }

        fn mib_per_sec(&self) -> f64 {
            let bytes = self.ops as f64 * self.bytes_per_op as f64;
            bytes / self.elapsed.as_secs_f64() / 1024.0 / 1024.0
        }
    }

    fn storage_perf_capacity(bytes_per_op: usize, ops: usize) -> u64 {
        let bytes = bytes_per_op
            .checked_mul(ops)
            .and_then(|value| value.checked_mul(2))
            .unwrap();
        align_up_u64(bytes as u64, SSD_ALIGNMENT as u64).unwrap()
    }

    fn storage_perf_data(bytes_per_op: usize) -> Vec<u8> {
        (0..bytes_per_op)
            .map(|idx| ((idx * 31 + idx / 251) % 251) as u8)
            .collect()
    }

    async fn run_storage_persist_perf(
        mode: KvSsdUringMode,
        bytes_per_op: usize,
        ops: usize,
    ) -> StoragePerfResult {
        let store = new_store_with_mode(storage_perf_capacity(bytes_per_op, ops), mode).await;
        let data = storage_perf_data(bytes_per_op);
        let start = std::time::Instant::now();
        for idx in 0..ops {
            store
                .persist(
                    &format!("persist-{mode:?}-{idx}"),
                    (idx as u64, 0),
                    data.as_slice(),
                )
                .await
                .unwrap();
        }
        StoragePerfResult {
            mode,
            op: StoragePerfOp::Persist,
            ops,
            bytes_per_op,
            elapsed: start.elapsed(),
        }
    }

    async fn storage_with_seeded_values(
        mode: KvSsdUringMode,
        bytes_per_op: usize,
        ops: usize,
        data: &[u8],
    ) -> KvSsdStorage {
        let store = new_store_with_mode(storage_perf_capacity(bytes_per_op, ops), mode).await;
        for idx in 0..ops {
            store
                .persist(&format!("load-{mode:?}-{idx}"), (idx as u64, 0), data)
                .await
                .unwrap();
        }
        store
    }

    async fn run_storage_load_perf(
        mode: KvSsdUringMode,
        bytes_per_op: usize,
        ops: usize,
    ) -> StoragePerfResult {
        let data = storage_perf_data(bytes_per_op);
        let store = storage_with_seeded_values(mode, bytes_per_op, ops, data.as_slice()).await;
        let mut out = AlignedBuffer::zeroed(bytes_per_op).unwrap();
        let start = std::time::Instant::now();
        for idx in 0..ops {
            store
                .load_into_addr(
                    &format!("load-{mode:?}-{idx}"),
                    (idx as u64, 0),
                    out.as_mut_ptr() as u64,
                    bytes_per_op as u64,
                    out.len() as u64,
                )
                .await
                .unwrap();
        }
        let elapsed = start.elapsed();
        let out_slice = unsafe { std::slice::from_raw_parts(out.as_ptr(), data.len()) };
        assert_eq!(out_slice, data.as_slice());
        StoragePerfResult {
            mode,
            op: StoragePerfOp::Load,
            ops,
            bytes_per_op,
            elapsed,
        }
    }

    fn best_storage_perf_result(results: &[StoragePerfResult]) -> StoragePerfResult {
        results
            .iter()
            .min_by_key(|result| result.elapsed)
            .copied()
            .unwrap()
    }

    fn median_storage_perf_result(results: &[StoragePerfResult]) -> StoragePerfResult {
        let mut sorted = results.to_vec();
        sorted.sort_by_key(|result| result.elapsed);
        sorted[sorted.len() / 2]
    }

    fn print_storage_perf_pair_result(
        stat: &str,
        label: &str,
        iovec: StoragePerfResult,
        single_buffer: StoragePerfResult,
        rounds: usize,
    ) {
        println!(
            "kv ssd storage perf {stat}-of-{rounds} {label}: {:?} {:?}: ops={} bytes/op={} elapsed={:?} ns/op={:.1} MiB/s={:.1}",
            iovec.mode,
            iovec.op,
            iovec.ops,
            iovec.bytes_per_op,
            iovec.elapsed,
            iovec.ns_per_op(),
            iovec.mib_per_sec()
        );
        println!(
            "kv ssd storage perf {stat}-of-{rounds} {label}: {:?} {:?}: ops={} bytes/op={} elapsed={:?} ns/op={:.1} MiB/s={:.1}",
            single_buffer.mode,
            single_buffer.op,
            single_buffer.ops,
            single_buffer.bytes_per_op,
            single_buffer.elapsed,
            single_buffer.ns_per_op(),
            single_buffer.mib_per_sec()
        );
        println!(
            "kv ssd storage perf delta {stat}-of-{rounds} {label}: single_buffer relative to iovec = {:.2}%",
            (iovec.ns_per_op() - single_buffer.ns_per_op()) / iovec.ns_per_op() * 100.0
        );
    }

    fn print_storage_perf_pair(
        label: &str,
        iovec_results: &[StoragePerfResult],
        single_buffer_results: &[StoragePerfResult],
    ) {
        assert_eq!(iovec_results.len(), single_buffer_results.len());
        let rounds = iovec_results.len();
        print_storage_perf_pair_result(
            "best",
            label,
            best_storage_perf_result(iovec_results),
            best_storage_perf_result(single_buffer_results),
            rounds,
        );
        print_storage_perf_pair_result(
            "median",
            label,
            median_storage_perf_result(iovec_results),
            median_storage_perf_result(single_buffer_results),
            rounds,
        );
    }

    #[::tokio::test]
    #[ignore = "manual KvSsdStorage-level perf comparison for iovec vs single-buffer uring mode"]
    async fn perf_compare_kv_ssd_storage_iovec_with_single_buffer_mode() {
        for (bytes_per_op, ops, rounds) in [
            (1024 * 1024usize, 4usize, 3usize),
            (10 * 1024 * 1024usize, 2usize, 3usize),
        ] {
            assert!(bytes_per_op.is_multiple_of(SSD_ALIGNMENT));

            let mut iovec_persist_results = Vec::with_capacity(rounds);
            let mut single_persist_results = Vec::with_capacity(rounds);
            let mut iovec_load_results = Vec::with_capacity(rounds);
            let mut single_load_results = Vec::with_capacity(rounds);

            for round_idx in 0..rounds {
                if round_idx % 2 == 0 {
                    iovec_persist_results.push(
                        run_storage_persist_perf(KvSsdUringMode::Iovec, bytes_per_op, ops).await,
                    );
                    single_persist_results.push(
                        run_storage_persist_perf(KvSsdUringMode::SingleBuffer, bytes_per_op, ops)
                            .await,
                    );
                    iovec_load_results.push(
                        run_storage_load_perf(KvSsdUringMode::Iovec, bytes_per_op, ops).await,
                    );
                    single_load_results.push(
                        run_storage_load_perf(KvSsdUringMode::SingleBuffer, bytes_per_op, ops)
                            .await,
                    );
                } else {
                    single_persist_results.push(
                        run_storage_persist_perf(KvSsdUringMode::SingleBuffer, bytes_per_op, ops)
                            .await,
                    );
                    iovec_persist_results.push(
                        run_storage_persist_perf(KvSsdUringMode::Iovec, bytes_per_op, ops).await,
                    );
                    single_load_results.push(
                        run_storage_load_perf(KvSsdUringMode::SingleBuffer, bytes_per_op, ops)
                            .await,
                    );
                    iovec_load_results.push(
                        run_storage_load_perf(KvSsdUringMode::Iovec, bytes_per_op, ops).await,
                    );
                }
            }

            let label = format!("{} bytes/op persist", bytes_per_op);
            print_storage_perf_pair(&label, &iovec_persist_results, &single_persist_results);
            let label = format!("{} bytes/op load", bytes_per_op);
            print_storage_perf_pair(&label, &iovec_load_results, &single_load_results);
        }
    }

    #[::tokio::test]
    async fn unaligned_payload_loads_direct_when_stage_capacity_is_aligned() {
        let store = new_store(1024 * 1024).await;
        let data = (0..500).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let put_id = (12, 1);
        store.persist("unaligned", put_id, &data).await.unwrap();

        let mut out = AlignedBuffer::zeroed(SSD_ALIGNMENT).unwrap();
        let target_addr = out.as_mut_ptr() as u64;
        let entry = {
            let key = KvSsdKey {
                key: "unaligned".to_string(),
                put_id,
            };
            store.inner.lock().ring.get(&key).unwrap()
        };
        assert_eq!(entry.len, data.len() as u64);
        assert_eq!(entry.aligned_len, SSD_ALIGNMENT as u64);
        assert_eq!(
            choose_read_path(&entry, target_addr, data.len() as u64, SSD_ALIGNMENT as u64),
            SsdReadPath::Direct
        );

        store
            .load_into_addr(
                "unaligned",
                put_id,
                target_addr,
                data.len() as u64,
                SSD_ALIGNMENT as u64,
            )
            .await
            .unwrap();

        let out_slice = unsafe { std::slice::from_raw_parts(out.as_ptr(), data.len()) };
        assert_eq!(out_slice, data.as_slice());
    }

    #[::tokio::test]
    async fn storage_deduplicates_root_dirs_on_same_device() {
        let root_a = new_root();
        let root_b = new_root();
        let store = KvSsdStorage::new(KvSsdStorageInit {
            root_limits: vec![
                KvSsdStorageRootLimit {
                    root_dir: root_a.clone(),
                    limit_bytes: 4 * SSD_ALIGNMENT as u64,
                },
                KvSsdStorageRootLimit {
                    root_dir: root_b.clone(),
                    limit_bytes: 8 * SSD_ALIGNMENT as u64,
                },
            ],
            uring_mode: KvSsdUringMode::SingleBuffer,
            backend: KvSsdStorageBackend::Native,
            writeback: Default::default(),
        })
        .await
        .unwrap();

        assert_eq!(
            fs::metadata(&root_a).unwrap().dev(),
            fs::metadata(&root_b).unwrap().dev()
        );
        assert_eq!(store.root_dirs(), &[root_a.clone()]);
        assert_eq!(store.devices.len(), 1);
        assert_eq!(store.shard_to_device, vec![0, 0, 0, 0]);
        assert!(root_a.join("shards/shard-000000.dat").exists());
        assert!(root_a.join("shards/shard-000001.dat").exists());
        assert!(root_a.join("shards/shard-000002.dat").exists());
        assert!(root_a.join("shards/shard-000003.dat").exists());
        assert!(!root_b.join("shards").exists());
    }

    #[test]
    fn ring_prepare_write_on_shards_uses_only_allowed_shards() {
        let mut ring = SsdRingBuffer::new(vec![1024, 1024, 1024, 1024]);
        let mut allocated_shards = Vec::new();

        for version in 0..4 {
            let key = test_key("per-device", version);
            let entry = match ring
                .prepare_write_on_shards(key.clone(), 500, &[1, 3])
                .unwrap()
            {
                SsdPreparedWrite::Ready { entry, .. } => entry,
                other => panic!("expected ready SSD write, got {other:?}"),
            };
            allocated_shards.push(entry.shard_id);
            assert!(ring.commit(&key, true));
        }

        assert_eq!(allocated_shards, vec![1, 3, 1, 3]);
    }

    #[::tokio::test]
    async fn ring_keeps_new_entry_and_expires_old() {
        let store = new_store(1024).await;
        let mut eviction_rx = store
            .take_eviction_rx()
            .expect("SSD eviction receiver must be available once");
        store.persist("old", (1, 0), &[1u8; 500]).await.unwrap();
        store.persist("filler", (2, 0), &[2u8; 500]).await.unwrap();
        store.persist("new", (3, 0), &[3u8; 500]).await.unwrap();

        assert!(!store.has_entry("old", (1, 0)).await);
        assert!(store.has_entry("filler", (2, 0)).await);
        assert!(store.has_entry("new", (3, 0)).await);
        let evicted = ::tokio::time::timeout(Duration::from_secs(1), eviction_rx.recv())
            .await
            .expect("SSD eviction notification must arrive")
            .expect("SSD eviction channel must stay open");
        assert_eq!(
            evicted,
            vec![SsdReplicaEviction {
                key: "old".to_string(),
                put_id: (1, 0),
            }]
        );
    }

    #[::tokio::test]
    async fn persist_guard_blocks_eviction_until_route_commit_finishes() {
        let store = Arc::new(new_store(512).await);
        let mut eviction_rx = store
            .take_eviction_rx()
            .expect("SSD eviction receiver must be available once");
        let old_data = vec![1u8; 500];
        let guard = store
            .persist_from_addr(
                "old",
                (1, 0),
                old_data.as_ptr() as u64,
                old_data.len() as u64,
            )
            .await
            .unwrap();

        let store_for_write = Arc::clone(&store);
        let mut new_write =
            ::tokio::spawn(
                async move { store_for_write.persist("new", (2, 0), &[2u8; 500]).await },
            );
        assert!(
            ::tokio::time::timeout(Duration::from_millis(50), &mut new_write)
                .await
                .is_err(),
            "new write must wait while the old route commit holds an entry pin"
        );

        drop(guard);
        ::tokio::time::timeout(Duration::from_secs(1), &mut new_write)
            .await
            .expect("new write must resume after the route commit pin is dropped")
            .expect("new write task must complete")
            .expect("new write must succeed");
        let evicted = ::tokio::time::timeout(Duration::from_secs(1), eviction_rx.recv())
            .await
            .expect("SSD eviction notification must arrive")
            .expect("SSD eviction channel must stay open");
        assert_eq!(evicted[0].key, "old");
        assert_eq!(evicted[0].put_id, (1, 0));
    }

    #[::tokio::test]
    async fn blocked_writer_keeps_backpressure_in_the_bounded_channel() {
        let store = Arc::new(new_store(512).await);
        let old_data = vec![1u8; 500];
        let guard = store
            .persist_from_addr(
                "old",
                (1, 0),
                old_data.as_ptr() as u64,
                old_data.len() as u64,
            )
            .await
            .unwrap();

        let mut writes = Vec::new();
        // Fill both the bounded local pending queue and the input channel.
        for idx in 0..(2 * DEFAULT_WRITE_QUEUE_DEPTH + 2) {
            let store = Arc::clone(&store);
            writes.push(::tokio::spawn(async move {
                store
                    .persist(&format!("queued-{idx}"), (idx as u64 + 2, 0), &[2u8; 500])
                    .await
            }));
        }

        ::tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let capacity = store.devices[0]
                    .runtime
                    .lock()
                    .as_ref()
                    .expect("SSD runtime must remain open")
                    .required_write_tx
                    .capacity();
                if capacity == 0 {
                    break;
                }
                ::tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocked writer must leave excess work in the bounded channel");
        ::tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            store.devices[0]
                .runtime
                .lock()
                .as_ref()
                .expect("SSD runtime must remain open")
                .required_write_tx
                .capacity(),
            0,
            "the writer must not drain blocked commands into an unbounded local queue"
        );

        drop(guard);
        for write in writes {
            ::tokio::time::timeout(Duration::from_secs(2), write)
                .await
                .expect("queued write must resume")
                .expect("queued write task must not panic")
                .expect("queued write must succeed");
        }
    }

    #[::tokio::test]
    async fn required_write_is_received_while_early_write_waits_for_space() {
        let store = new_store(512).await;
        let old_data = vec![1u8; 500];
        let old_guard = store
            .persist_from_addr(
                "old",
                (1, 0),
                old_data.as_ptr() as u64,
                old_data.len() as u64,
            )
            .await
            .unwrap();

        let early_route = store
            .next_write_route(500, KvSsdPersistClass::BestEffortEarly)
            .await
            .unwrap();
        let (early_done_tx, early_done_rx) = oneshot::channel();
        early_route
            .tx
            .send(WriteCommand {
                key: test_key("early", 2),
                entry_len: 500,
                data: AlignedBuffer::zeroed(SSD_ALIGNMENT).unwrap(),
                priority: KvSsdPersistPriority::new(KvSsdPersistClass::BestEffortEarly),
                queued_at: Instant::now(),
                device_permit: early_route.permit,
                done_tx: early_done_tx,
            })
            .await
            .unwrap();
        ::tokio::time::timeout(Duration::from_secs(1), async {
            while store.stats_snapshot().persist_busy_batches == 0 {
                ::tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the Early write must reach the SSD-space blocked state");

        let required_route = store
            .next_write_route(500, KvSsdPersistClass::RequiredLastBacking)
            .await
            .unwrap();
        let (required_done_tx, required_done_rx) = oneshot::channel();
        required_route
            .tx
            .send(WriteCommand {
                key: test_key("required", 3),
                entry_len: 500,
                data: AlignedBuffer::zeroed(SSD_ALIGNMENT).unwrap(),
                priority: KvSsdPersistPriority::new(KvSsdPersistClass::RequiredLastBacking),
                queued_at: Instant::now(),
                device_permit: required_route.permit,
                done_tx: required_done_tx,
            })
            .await
            .unwrap();
        ::tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let capacity = store.devices[0]
                    .runtime
                    .lock()
                    .as_ref()
                    .expect("SSD runtime must remain open")
                    .required_write_tx
                    .capacity();
                if capacity == DEFAULT_WRITE_QUEUE_DEPTH {
                    break;
                }
                ::tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Required work must leave the bounded channel while Early is blocked");

        drop(old_guard);
        let required_guard = ::tokio::time::timeout(Duration::from_secs(2), required_done_rx)
            .await
            .expect("Required write must resume")
            .expect("Required completion channel must stay open")
            .expect("Required write must succeed");
        drop(required_guard);
        let early_guard = ::tokio::time::timeout(Duration::from_secs(2), early_done_rx)
            .await
            .expect("Early write must resume")
            .expect("Early completion channel must stay open")
            .expect("Early write must succeed");
        drop(early_guard);
    }

    #[test]
    fn ring_read_pin_blocks_overwrite_until_unpinned() {
        let mut ring = SsdRingBuffer::new(vec![1024]);
        let old = test_key("old", 1);
        let filler = test_key("filler", 2);
        let new = test_key("new", 3);

        let old_entry = prepare_ready(&mut ring, &old);
        assert_eq!(old_entry.begin, 0);
        assert!(ring.commit(&old, true));
        prepare_ready(&mut ring, &filler);
        assert!(ring.commit(&filler, true));

        let pinned = ring.pin_read(&old).unwrap();
        assert_eq!(pinned.begin, old_entry.begin);
        assert!(matches!(
            ring.prepare_write(new.clone(), 500).unwrap(),
            SsdPreparedWrite::BlockedByBusyIo
        ));
        assert!(ring.get(&old).is_some());

        ring.unpin_read(&old);
        let new_entry = prepare_ready(&mut ring, &new);
        assert_eq!(new_entry.file_offset, 0);
        assert!(ring.commit(&new, true));
        assert!(ring.get(&old).is_none());
    }

    #[test]
    fn ring_writing_entry_blocks_overwrite_until_write_finishes() {
        let mut ring = SsdRingBuffer::new(vec![1024]);
        let old = test_key("old", 1);
        let filler = test_key("filler", 2);
        let new = test_key("new", 3);

        let old_entry = prepare_ready(&mut ring, &old);
        assert_eq!(old_entry.begin, 0);
        prepare_ready(&mut ring, &filler);

        assert!(matches!(
            ring.prepare_write(new.clone(), 500).unwrap(),
            SsdPreparedWrite::BlockedByBusyIo
        ));

        assert!(ring.commit(&old, true));
        let new_entry = prepare_ready(&mut ring, &new);
        assert_eq!(new_entry.file_offset, 0);
    }

    #[test]
    fn ring_tracks_used_bytes_and_busy_offsets_incrementally() {
        let mut ring = SsdRingBuffer::new(vec![1024]);
        let old = test_key("old", 1);
        let filler = test_key("filler", 2);
        let new = test_key("new", 3);

        assert_eq!(ring.used_bytes_by_shard(), vec![0]);
        let old_entry = prepare_ready(&mut ring, &old);
        assert_eq!(ring.used_bytes_by_shard(), vec![512]);
        assert_eq!(ring.shards[0].busy_begins.get(&old_entry.begin), Some(&1));

        assert!(ring.commit(&old, true));
        assert!(ring.shards[0].busy_begins.is_empty());
        ring.pin_read(&old).unwrap();
        ring.pin_read(&old).unwrap();
        assert_eq!(ring.shards[0].busy_begins.get(&old_entry.begin), Some(&1));
        ring.unpin_read(&old);
        assert_eq!(ring.shards[0].busy_begins.get(&old_entry.begin), Some(&1));
        ring.unpin_read(&old);
        assert!(ring.shards[0].busy_begins.is_empty());

        prepare_ready(&mut ring, &filler);
        assert!(ring.commit(&filler, true));
        assert_eq!(ring.used_bytes_by_shard(), vec![1024]);
        prepare_ready(&mut ring, &new);
        assert_eq!(ring.used_bytes_by_shard(), vec![1024]);
        assert!(ring.get(&old).is_none());
    }

    #[test]
    fn ring_failed_write_releases_incremental_accounting() {
        let mut ring = SsdRingBuffer::new(vec![1024]);
        let key = test_key("failed", 1);
        let entry = prepare_ready(&mut ring, &key);

        assert_eq!(ring.used_bytes_by_shard(), vec![512]);
        assert_eq!(ring.shards[0].busy_begins.get(&entry.begin), Some(&1));
        assert!(!ring.commit(&key, false));
        assert_eq!(ring.used_bytes_by_shard(), vec![0]);
        assert!(ring.shards[0].busy_begins.is_empty());
    }

    #[test]
    fn ring_stale_failed_reservation_cannot_remove_same_key_retry() {
        let mut ring = SsdRingBuffer::new(vec![512, 512]);
        let retried = test_key("retried", 1);
        let wrap = test_key("wrap", 2);

        let failed = prepare_ready(&mut ring, &retried);
        assert_eq!((failed.shard_id, failed.begin), (0, 0));
        assert!(!ring.commit(&retried, false));

        let current = prepare_ready(&mut ring, &retried);
        assert_eq!((current.shard_id, current.begin), (1, 0));
        assert!(ring.commit(&retried, true));

        let wrapped = match ring.prepare_write(wrap.clone(), 500).unwrap() {
            SsdPreparedWrite::Ready { entry, evicted } => {
                assert!(evicted.is_empty());
                entry
            }
            other => panic!("expected ready SSD write, got {other:?}"),
        };
        assert_eq!((wrapped.shard_id, wrapped.begin), (0, 512));
        assert!(ring.commit(&wrap, true));

        assert_eq!(ring.get(&retried).unwrap().shard_id, 1);
        assert_eq!(ring.used_bytes_by_shard(), vec![512, 512]);
    }

    #[test]
    fn ring_does_not_reuse_a_key_while_its_removed_entry_is_pinned() {
        let mut ring = SsdRingBuffer::new(vec![1024]);
        let key = test_key("same-key", 1);
        prepare_ready(&mut ring, &key);
        assert!(ring.commit(&key, true));
        ring.pin_read(&key).unwrap();
        ring.remove(&key);

        assert!(matches!(
            ring.prepare_write(key.clone(), 500).unwrap(),
            SsdPreparedWrite::BlockedByBusyIo
        ));
        ring.unpin_read(&key);
        assert!(matches!(
            ring.prepare_write(key, 500).unwrap(),
            SsdPreparedWrite::Ready { .. }
        ));
    }

    #[test]
    fn safe_component_is_stable_collision_resistant_and_has_no_dot_segments() {
        assert_eq!(
            safe_path_component("owner/a:b"),
            "v1-a2c1effab8d74aa90b8f7b43f9afa10f4c9f5899dd880fc176d33cc06cf7200a"
        );
        assert_ne!(
            safe_path_component("owner/a:b"),
            safe_path_component("owner_a_b")
        );
        for raw in ["", ".", "..", "owner/name"] {
            let component = safe_path_component(raw);
            assert_eq!(component.len(), 67);
            assert!(!matches!(component.as_str(), "." | ".."));
            assert!(!component.contains('/'));
        }
    }
}
