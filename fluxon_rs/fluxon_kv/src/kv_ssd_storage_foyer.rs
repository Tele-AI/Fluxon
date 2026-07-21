use crate::kv_ssd_storage::{KvSsdStorageRootLimit, SsdLoadedChunk, align_ssd_io_len};
use crate::master_kv_router::put::PutIDForAKey;
use crate::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult};
use ::tokio::sync::{Mutex as TokioMutex, mpsc as tokio_mpsc};
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    HybridCacheEntry, HybridCachePolicy, PsyncIoEngineConfig, Source,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const FOYER_MEMORY_CAPACITY_BYTES: usize = 1;
const FOYER_MEMORY_SHARDS: usize = 1;
const FOYER_BLOCK_SIZE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
struct FoyerKvSsdKey {
    key: String,
    put_id: PutIDForAKey,
}

type FoyerEntry = HybridCacheEntry<FoyerKvSsdKey, Vec<u8>>;

pub(crate) struct FoyerKvSsdPersistGuard {
    _entry: FoyerEntry,
}

#[derive(Debug)]
pub(crate) struct FoyerKvSsdStorage {
    cache: HybridCache<FoyerKvSsdKey, Vec<u8>>,
    root_dir: PathBuf,
    capacity_bytes: u64,
    // Serialize forced storage writes because Foyer drops submissions when its byte queue is full.
    persist_gate: TokioMutex<()>,
    entry_lengths: Mutex<HashMap<FoyerKvSsdKey, u64>>,
    logical_used_bytes: AtomicU64,
    memory_hits: AtomicU64,
    disk_hits: AtomicU64,
    outer_hits: AtomicU64,
}

impl FoyerKvSsdStorage {
    pub(crate) async fn new(root_limit: KvSsdStorageRootLimit) -> KvResult<Self> {
        if root_limit.limit_bytes < FOYER_BLOCK_SIZE_BYTES as u64 {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "foyer kv ssd capacity must be at least {} bytes, got {}",
                    FOYER_BLOCK_SIZE_BYTES, root_limit.limit_bytes
                ),
            }));
        }
        let capacity_bytes = usize::try_from(root_limit.limit_bytes).map_err(|_| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "foyer kv ssd capacity does not fit usize: {}",
                    root_limit.limit_bytes
                ),
            })
        })?;

        fs::create_dir_all(&root_limit.root_dir)
            .map_err(|err| foyer_file_error(&root_limit.root_dir, err))?;
        let foyer_root = root_limit.root_dir.join("foyer");
        match fs::remove_dir_all(&foyer_root) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(foyer_file_error(&foyer_root, err)),
        }
        fs::create_dir_all(&foyer_root).map_err(|err| foyer_file_error(&foyer_root, err))?;

        let device = FsDeviceBuilder::new(&foyer_root)
            .with_capacity(capacity_bytes)
            // Keep the ablation backend outside the kernel page cache, matching native SSD I/O.
            .with_direct(true)
            .build()
            .map_err(|err| foyer_error("build filesystem device", err))?;
        let engine = BlockEngineConfig::new(device).with_block_size(FOYER_BLOCK_SIZE_BYTES);
        let cache = HybridCacheBuilder::new()
            .with_name("fluxon_kv_ssd_foyer")
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            .with_flush_on_close(false)
            .memory(FOYER_MEMORY_CAPACITY_BYTES)
            .with_shards(FOYER_MEMORY_SHARDS)
            .with_weighter(|_key: &FoyerKvSsdKey, value: &Vec<u8>| value.len())
            .with_filter(|_key: &FoyerKvSsdKey, _value: &Vec<u8>| false)
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(engine)
            .build()
            .await
            .map_err(|err| foyer_error("build hybrid cache", err))?;

        tracing::warn!(
            root_dir = %root_limit.root_dir.display(),
            capacity_bytes = root_limit.limit_bytes,
            memory_capacity_bytes = FOYER_MEMORY_CAPACITY_BYTES,
            memory_shards = FOYER_MEMORY_SHARDS,
            block_size_bytes = FOYER_BLOCK_SIZE_BYTES,
            storage_admission = "forced",
            persist_concurrency = 1,
            direct_io = true,
            "Initialized test-only Foyer KV SSD backend with memory admission disabled"
        );

        Ok(Self {
            cache,
            root_dir: root_limit.root_dir,
            capacity_bytes: root_limit.limit_bytes,
            persist_gate: TokioMutex::new(()),
            entry_lengths: Mutex::new(HashMap::new()),
            logical_used_bytes: AtomicU64::new(0),
            memory_hits: AtomicU64::new(0),
            disk_hits: AtomicU64::new(0),
            outer_hits: AtomicU64::new(0),
        })
    }

    pub(crate) fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    pub(crate) fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    pub(crate) fn logical_used_bytes(&self) -> u64 {
        self.logical_used_bytes
            .load(Ordering::Relaxed)
            .min(self.capacity_bytes)
    }

    pub(crate) async fn close(&self) -> KvResult<()> {
        self.cache
            .close()
            .await
            .map_err(|err| foyer_error("close", err))
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, key: &str, put_id: PutIDForAKey) -> bool {
        self.cache.contains(&FoyerKvSsdKey {
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
    ) -> KvResult<FoyerKvSsdPersistGuard> {
        let len_usize = usize::try_from(len).map_err(|_| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!("foyer kv ssd persist len does not fit usize: {len}"),
            })
        })?;
        if len_usize == 0 {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "foyer kv ssd persist len must be positive".to_string(),
            }));
        }
        let _persist_permit = self.persist_gate.lock().await;
        let data = unsafe { std::slice::from_raw_parts(addr as *const u8, len_usize) }.to_vec();
        self.persist_vec_locked(key, put_id, data).await
    }

    pub(crate) async fn persist(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        data: &[u8],
    ) -> KvResult<FoyerKvSsdPersistGuard> {
        if data.is_empty() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "foyer kv ssd persist len must be positive".to_string(),
            }));
        }
        let _persist_permit = self.persist_gate.lock().await;
        self.persist_vec_locked(key, put_id, data.to_vec()).await
    }

    async fn persist_vec_locked(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        data: Vec<u8>,
    ) -> KvResult<FoyerKvSsdPersistGuard> {
        let cache_key = FoyerKvSsdKey {
            key: key.to_string(),
            put_id,
        };
        let len = data.len() as u64;
        if let Some(existing_len) = self.entry_lengths.lock().get(&cache_key).copied() {
            if existing_len != len {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "foyer kv ssd duplicate persist length mismatch: key={} put_id=({},{}) existing_len={} requested_len={}",
                        key, put_id.0, put_id.1, existing_len, len
                    ),
                }));
            }
        }

        let entry = self
            .cache
            .storage_writer(cache_key.clone())
            .force()
            .insert(data)
            .ok_or_else(|| {
                KvError::Api(ApiError::FileWriteError {
                    path: self.root_dir.display().to_string(),
                    offset: 0,
                    detail: format!(
                        "foyer rejected forced storage admission for key={} put_id=({},{})",
                        key, put_id.0, put_id.1
                    ),
                })
            })?;
        self.cache.storage().wait().await;
        if !self.cache.storage().may_contains(&cache_key) {
            return Err(KvError::Api(ApiError::FileWriteError {
                path: self.root_dir.display().to_string(),
                offset: 0,
                detail: format!(
                    "foyer did not commit key={} put_id=({},{}) to storage",
                    key, put_id.0, put_id.1
                ),
            }));
        }

        let mut entry_lengths = self.entry_lengths.lock();
        if entry_lengths.insert(cache_key, len).is_none() {
            self.logical_used_bytes.fetch_add(len, Ordering::Relaxed);
        }
        drop(entry_lengths);
        Ok(FoyerKvSsdPersistGuard { _entry: entry })
    }

    pub(crate) async fn load_into_addr(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        target_addr: u64,
        len: u64,
        target_len: u64,
    ) -> KvResult<()> {
        if target_len < len {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "foyer kv ssd target capacity too small for key={} put_id=({},{}) len={} target_len={}",
                    key, put_id.0, put_id.1, len, target_len
                ),
            }));
        }
        let entry = self.load_entry(key, put_id, len).await?;
        let len_usize = usize::try_from(len).map_err(|_| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!("foyer kv ssd load len does not fit usize: {len}"),
            })
        })?;
        unsafe {
            std::ptr::copy_nonoverlapping(
                entry.value().as_ptr(),
                target_addr as *mut u8,
                len_usize,
            );
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn load_into_addr_chunks(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        target_addr: u64,
        len: u64,
        target_len: u64,
        chunk_bytes: u64,
        _max_read_inflight: usize,
        ready_tx: tokio_mpsc::Sender<SsdLoadedChunk>,
    ) -> KvResult<()> {
        if target_len < len {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "foyer kv ssd target capacity too small for chunked load: key={} put_id=({},{}) len={} target_len={}",
                    key, put_id.0, put_id.1, len, target_len
                ),
            }));
        }
        let chunk_bytes = align_ssd_io_len(chunk_bytes.max(1))?;
        let entry = self.load_entry(key, put_id, len).await?;
        let mut offset = 0u64;
        while offset < len {
            let payload_len = chunk_bytes.min(len - offset);
            let end = offset.checked_add(payload_len).ok_or_else(|| {
                KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "foyer kv ssd chunk range overflow: offset={offset} len={payload_len}"
                    ),
                })
            })?;
            let stage_addr = target_addr.checked_add(offset).ok_or_else(|| {
                KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "foyer kv ssd chunk address overflow: target_addr={target_addr} offset={offset}"
                    ),
                })
            })?;
            let offset_usize = usize::try_from(offset).map_err(|_| {
                KvError::Api(ApiError::InvalidArgument {
                    detail: format!("foyer kv ssd chunk offset does not fit usize: {offset}"),
                })
            })?;
            let end_usize = usize::try_from(end).map_err(|_| {
                KvError::Api(ApiError::InvalidArgument {
                    detail: format!("foyer kv ssd chunk end does not fit usize: {end}"),
                })
            })?;
            unsafe {
                std::ptr::copy_nonoverlapping(
                    entry.value()[offset_usize..end_usize].as_ptr(),
                    stage_addr as *mut u8,
                    end_usize - offset_usize,
                );
            }
            ready_tx
                .send(SsdLoadedChunk {
                    offset,
                    stage_addr,
                    len: payload_len,
                })
                .await
                .map_err(|err| {
                    KvError::Api(ApiError::InvalidArgument {
                        detail: format!("foyer kv ssd chunk ready queue closed: {err}"),
                    })
                })?;
            offset = end;
        }
        Ok(())
    }

    async fn load_entry(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        expected_len: u64,
    ) -> KvResult<FoyerEntry> {
        let cache_key = FoyerKvSsdKey {
            key: key.to_string(),
            put_id,
        };
        let entry = self
            .cache
            .get(&cache_key)
            .await
            .map_err(|err| foyer_error("load entry", err))?
            .ok_or_else(|| {
                self.forget_entry(&cache_key);
                KvError::Api(ApiError::KeyNotFound {
                    key: key.to_string(),
                })
            })?;
        self.record_source(entry.source());
        if entry.value().len() as u64 != expected_len {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "foyer kv ssd length mismatch for key={} put_id=({},{}) expected={} actual={}",
                    key,
                    put_id.0,
                    put_id.1,
                    expected_len,
                    entry.value().len()
                ),
            }));
        }
        Ok(entry)
    }

    fn forget_entry(&self, key: &FoyerKvSsdKey) {
        if let Some(len) = self.entry_lengths.lock().remove(key) {
            self.logical_used_bytes.fetch_sub(len, Ordering::Relaxed);
        }
    }

    fn record_source(&self, source: Source) {
        match source {
            Source::Memory => self.memory_hits.fetch_add(1, Ordering::Relaxed),
            Source::Disk => self.disk_hits.fetch_add(1, Ordering::Relaxed),
            Source::Outer => self.outer_hits.fetch_add(1, Ordering::Relaxed),
        };
    }

    #[cfg(test)]
    pub(crate) fn memory_usage(&self) -> usize {
        self.cache.memory().usage()
    }

    #[cfg(test)]
    pub(crate) fn source_counts(&self) -> (u64, u64, u64) {
        (
            self.memory_hits.load(Ordering::Relaxed),
            self.disk_hits.load(Ordering::Relaxed),
            self.outer_hits.load(Ordering::Relaxed),
        )
    }
}

fn foyer_error(operation: &str, err: impl std::fmt::Display) -> KvError {
    KvError::Api(ApiError::Unknown {
        detail: format!("foyer kv ssd {operation} failed: {err}"),
    })
}

fn foyer_file_error(path: &Path, err: std::io::Error) -> KvError {
    KvError::Api(ApiError::FileWriteError {
        path: path.display().to_string(),
        offset: 0,
        detail: err.to_string(),
    })
}
