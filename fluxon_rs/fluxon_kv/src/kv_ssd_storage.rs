use crate::config::{KvSsdStorageBackend, KvSsdUringMode};
use crate::kv_ssd_storage_foyer::{FoyerKvSsdPersistGuard, FoyerKvSsdStorage};
use crate::master_kv_router::msg_pack::SsdReplicaEviction;
use crate::master_kv_router::put::PutIDForAKey;
use crate::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult};
use ::tokio::{
    sync::{Mutex as TokioMutex, Notify, mpsc as tokio_mpsc, oneshot, watch},
    task,
};
use fluxon_framework_compiled::shutdown::{ShutdownGate, ShutdownGuard};
use futures::stream::{FuturesUnordered, StreamExt};
use io_uring::{IoUring, opcode, types::Fd};
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, OpenOptions};
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::JoinHandle;

pub(crate) const SSD_ALIGNMENT: usize = 512;
const DEFAULT_SHARDS_PER_OWNER: usize = 4;
const DEFAULT_URING_THREADS: usize = 16;
const DEFAULT_URING_IO_DEPTH: usize = 128;
const DEFAULT_URING_READ_WEIGHT: usize = 2;
const DEFAULT_WRITE_QUEUE_DEPTH: usize = 8;
const DEFAULT_READ_QUEUE_DEPTH: usize = 16;
const DEFAULT_WRITE_INFLIGHT: usize = 2;
const DEFAULT_READ_INFLIGHT: usize = 16;
const DEFAULT_EVICTION_QUEUE_DEPTH: usize = 1024;
pub(crate) const DEFAULT_READ_TRANSFER_PIPELINE_CHUNK_BYTES: u64 = 4 * 1024 * 1024;
pub(crate) const DEFAULT_READ_TRANSFER_PIPELINE_INFLIGHT: usize = 4;

#[derive(Clone, Debug)]
pub struct KvSsdStorageInit {
    pub root_limits: Vec<KvSsdStorageRootLimit>,
    pub uring_mode: KvSsdUringMode,
    pub backend: KvSsdStorageBackend,
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
    runtime: Mutex<Option<SsdDeviceRuntime>>,
}

#[derive(Debug)]
struct SsdDeviceRuntime {
    io: Arc<UringIoEngine>,
    write_tx: tokio_mpsc::Sender<WriteCommand>,
    read_tx: tokio_mpsc::Sender<ReadCommand>,
    writer_handle: task::JoinHandle<()>,
    reader_handle: task::JoinHandle<()>,
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

#[derive(Clone, Debug)]
pub(crate) struct KvSsdStorageDeviceUsage {
    pub device: String,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
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
    order: VecDeque<KvSsdKey>,
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
        let pin = self
            .read_pins
            .entry(key.clone())
            .or_insert_with(|| SsdReadPinInfo {
                entry: entry.clone(),
                count: 0,
            });
        pin.count += 1;
        Some(entry)
    }

    fn unpin_read(&mut self, key: &KvSsdKey) {
        match self.read_pins.get_mut(key) {
            Some(pin) if pin.count > 1 => pin.count -= 1,
            Some(_) => {
                self.read_pins.remove(key);
            }
            None => debug_assert!(false, "missing kv ssd read pin for key={key:?}"),
        }
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
            self.shards[shard_id].order.push_back(key);
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

        while let Some(key) = self.shards[shard_id].order.front() {
            match self.entries.get(key) {
                Some(state) if state.entry().begin >= new_tail => break,
                _ => {
                    let key = self.shards[shard_id]
                        .order
                        .pop_front()
                        .expect("front key exists");
                    if matches!(self.entries.remove(&key), Some(SsdEntryState::Committed(_))) {
                        evicted.push(key);
                    }
                }
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
            self.entries.remove(key);
            return false;
        }
        self.entries
            .insert(key.clone(), SsdEntryState::Committed(entry));
        true
    }

    fn remove(&mut self, key: &KvSsdKey) {
        self.entries.remove(key);
    }

    fn is_offset_valid(&self, entry: &SsdIndexEntry) -> bool {
        self.shards
            .get(entry.shard_id)
            .is_some_and(|shard| entry.begin >= shard.tail)
    }

    fn used_bytes_by_shard(&self) -> Vec<u64> {
        let mut out = vec![0u64; self.shards.len()];
        for state in self.entries.values() {
            let entry = state.entry();
            if self.is_offset_valid(entry) {
                out[entry.shard_id] = out[entry.shard_id].saturating_add(entry.aligned_len);
            }
        }
        out
    }

    fn has_busy_entries_before(&self, shard_id: usize, new_tail: u64) -> bool {
        if new_tail <= self.shards[shard_id].tail {
            return false;
        }
        let writing_busy = self.entries.values().any(|state| match state {
            SsdEntryState::Writing(entry) => entry.shard_id == shard_id && entry.begin < new_tail,
            SsdEntryState::Committed(_) => false,
        });
        if writing_busy {
            return true;
        }
        self.read_pins
            .values()
            .any(|pin| pin.entry.shard_id == shard_id && pin.entry.begin < new_tail)
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
    done_tx: oneshot::Sender<KvResult<KvSsdPersistGuard>>,
}

struct ReadCommand {
    key: KvSsdKey,
    entry: SsdIndexEntry,
    file_offset: u64,
    target: ReadTarget,
    _read_pin: Option<SsdReadPin>,
    done_tx: oneshot::Sender<KvResult<ReadOutput>>,
}

struct WriteTask {
    key: KvSsdKey,
    entry: SsdIndexEntry,
    data: AlignedBuffer,
    done_tx: oneshot::Sender<KvResult<KvSsdPersistGuard>>,
}

struct ReadTask {
    key: KvSsdKey,
    entry: SsdIndexEntry,
    file_offset: u64,
    target: ReadTarget,
    _read_pin: Option<SsdReadPin>,
    done_tx: oneshot::Sender<KvResult<ReadOutput>>,
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
    _read_pin: Option<SsdReadPin>,
    done_tx: oneshot::Sender<KvResult<ReadOutput>>,
}

enum ReadTarget {
    Scratch(AlignedBuffer),
    Direct { target_addr: u64, len: usize },
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
            let (write_tx, write_rx) = tokio_mpsc::channel(DEFAULT_WRITE_QUEUE_DEPTH);
            let (read_tx, read_rx) = tokio_mpsc::channel(DEFAULT_READ_QUEUE_DEPTH);

            let writer_handle = task::spawn(ssd_writer_loop(
                Arc::clone(&inner),
                write_rx,
                Arc::clone(&io),
                Arc::clone(&space_notify),
                DEFAULT_WRITE_INFLIGHT,
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
                runtime: Mutex::new(Some(SsdDeviceRuntime {
                    io,
                    write_tx,
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
                }
            })
            .collect()
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

    fn next_write_tx(&self, entry_len: u64) -> KvResult<tokio_mpsc::Sender<WriteCommand>> {
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
        loop {
            let start_idx = self.next_write_device.load(Ordering::Relaxed);
            let Some(device_idx) = select_write_device(&capacities, aligned_len, start_idx) else {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "kv ssd value len={} aligned_len={} exceeds every device shard capacity: {:?}",
                        entry_len, aligned_len, capacities
                    ),
                }));
            };
            let next_idx = (device_idx + 1) % self.devices.len();
            if self
                .next_write_device
                .compare_exchange_weak(start_idx, next_idx, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return self.devices[device_idx]
                    .runtime
                    .lock()
                    .as_ref()
                    .map(|runtime| runtime.write_tx.clone())
                    .ok_or_else(|| {
                        KvError::Api(ApiError::SystemShutdown {
                            detail: "kv ssd write worker is closed".to_string(),
                        })
                    });
            }
        }
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
        self.persist_buffer(key, put_id, len, data).await
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
            .persist_buffer(key, put_id, data.len() as u64, buffer)
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
    ) -> KvResult<KvSsdPersistGuard> {
        let (done_tx, done_rx) = oneshot::channel();
        let write_tx = self.next_write_tx(entry_len)?;
        write_tx
            .send(WriteCommand {
                key: KvSsdKey {
                    key: key.to_string(),
                    put_id,
                },
                entry_len,
                data,
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
            SsdReadPath::Scratch => ReadTarget::Scratch(AlignedBuffer::zeroed(aligned_len_usize)?),
        };
        let output = self
            .submit_read_command(
                key,
                entry.clone(),
                entry.file_offset,
                target,
                Some(read_pin),
            )
            .await?;
        if let ReadOutput::Scratch(buffer) = output {
            unsafe {
                std::ptr::copy_nonoverlapping(buffer.as_ptr(), target_addr as *mut u8, len_usize);
            }
        }
        Ok(())
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
        let (entry, _read_pin) = {
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
                    "kv ssd length mismatch for chunked load: key={} put_id=({},{}) expected={} actual={}",
                    key.key, put_id.0, put_id.1, len, entry.len
                ),
            }));
        }

        let mut next_offset = 0u64;
        let mut inflight = FuturesUnordered::new();
        let max_read_inflight = max_read_inflight.max(1);

        loop {
            while next_offset < len && inflight.len() < max_read_inflight {
                let payload_len = chunk_bytes.min(len - next_offset);
                let stage_addr = checked_add_u64(target_addr, next_offset, "chunk stage addr")?;
                let remaining_target_len = target_len - next_offset;
                inflight.push(self.load_entry_range_into_addr(
                    key.clone(),
                    entry.clone(),
                    next_offset,
                    payload_len,
                    stage_addr,
                    remaining_target_len,
                ));
                next_offset += payload_len;
            }

            let Some(chunk) = inflight.next().await else {
                break;
            };
            let chunk = chunk?;
            ready_tx.send(chunk).await.map_err(|err| {
                KvError::Api(ApiError::InvalidArgument {
                    detail: format!("kv ssd chunk ready queue closed: {}", err),
                })
            })?;
        }
        Ok(())
    }

    async fn load_entry_range_into_addr(
        &self,
        key: KvSsdKey,
        entry: SsdIndexEntry,
        offset: u64,
        payload_len: u64,
        target_addr: u64,
        target_len: u64,
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
            SsdReadPath::Scratch => ReadTarget::Scratch(AlignedBuffer::zeroed(read_len_usize)?),
        };
        let output = self
            .submit_read_command(key, entry, file_offset, target, None)
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
        read_pin: Option<SsdReadPin>,
    ) -> KvResult<ReadOutput> {
        let (done_tx, done_rx) = oneshot::channel();
        let read_tx = self.read_tx_for_shard(entry.shard_id)?;
        read_tx
            .send(ReadCommand {
                key,
                entry,
                file_offset,
                target,
                _read_pin: read_pin,
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
            write_tx,
            read_tx,
            writer_handle,
            reader_handle,
        } = runtime;
        drop(write_tx);
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
    mut rx: tokio_mpsc::Receiver<WriteCommand>,
    io: Arc<UringIoEngine>,
    space_notify: Arc<Notify>,
    write_inflight: usize,
    shard_ids: Vec<usize>,
    eviction_tx: tokio_mpsc::Sender<Vec<SsdReplicaEviction>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut pending: VecDeque<WriteCommand> = VecDeque::new();
    let mut inflight = FuturesUnordered::new();
    let max_inflight = write_inflight.max(1);

    loop {
        while inflight.len() < max_inflight {
            let Some(cmd) = pending.pop_front() else {
                break;
            };
            let (prepared, existing_guard) =
                prepare_ssd_write(&inner, &space_notify, &cmd.key, cmd.entry_len, &shard_ids);
            match prepared {
                Ok(SsdPreparedWrite::Ready { entry, evicted }) => {
                    publish_ssd_evictions(&eviction_tx, &mut shutdown_rx, evicted).await;
                    inflight.push(execute_write(
                        WriteTask {
                            key: cmd.key,
                            entry,
                            data: cmd.data,
                            done_tx: cmd.done_tx,
                        },
                        Arc::clone(&io),
                    ));
                }
                Ok(SsdPreparedWrite::Existing) => {
                    let result = existing_guard.ok_or_else(|| {
                        KvError::Api(ApiError::KeyNotFound {
                            key: cmd.key.key.clone(),
                        })
                    });
                    let _ = cmd.done_tx.send(result);
                }
                Ok(SsdPreparedWrite::BlockedByBusyIo) => {
                    pending.push_front(cmd);
                    break;
                }
                Err(err) => {
                    let _ = cmd.done_tx.send(Err(err));
                }
            }
        }

        tokio::select! {
            Some(completion) = inflight.next(), if !inflight.is_empty() => {
                finish_write_completion(&inner, &space_notify, completion);
            }
            Some(cmd) = rx.recv() => {
                pending.push_back(cmd);
            }
            _ = space_notify.notified(), if !pending.is_empty() => {
                // Retry pending commands after an active read/write releases a ring position.
            }
            else => {
                if pending.is_empty() && inflight.is_empty() {
                    break;
                }
            },
        }
    }

    while !pending.is_empty() || !inflight.is_empty() {
        while inflight.len() < max_inflight {
            let Some(cmd) = pending.pop_front() else {
                break;
            };
            let (prepared, existing_guard) =
                prepare_ssd_write(&inner, &space_notify, &cmd.key, cmd.entry_len, &shard_ids);
            match prepared {
                Ok(SsdPreparedWrite::Ready { entry, evicted }) => {
                    publish_ssd_evictions(&eviction_tx, &mut shutdown_rx, evicted).await;
                    inflight.push(execute_write(
                        WriteTask {
                            key: cmd.key,
                            entry,
                            data: cmd.data,
                            done_tx: cmd.done_tx,
                        },
                        Arc::clone(&io),
                    ));
                }
                Ok(SsdPreparedWrite::Existing) => {
                    let result = existing_guard.ok_or_else(|| {
                        KvError::Api(ApiError::KeyNotFound {
                            key: cmd.key.key.clone(),
                        })
                    });
                    let _ = cmd.done_tx.send(result);
                }
                Ok(SsdPreparedWrite::BlockedByBusyIo) => {
                    pending.push_front(cmd);
                    break;
                }
                Err(err) => {
                    let _ = cmd.done_tx.send(Err(err));
                }
            }
        }

        if let Some(completion) = inflight.next().await {
            finish_write_completion(&inner, &space_notify, completion);
        } else if !pending.is_empty() {
            space_notify.notified().await;
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
        changed = shutdown_rx.changed() => {
            if changed.is_err() || !*shutdown_rx.borrow() {
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

async fn execute_write(task: WriteTask, io: Arc<UringIoEngine>) -> WriteCompletion {
    let WriteTask {
        key,
        entry,
        data,
        done_tx,
    } = task;
    let data_len = data.len();
    let shard_id = entry.shard_id;
    let file_offset = entry.file_offset;
    let result = async move {
        let rx = {
            let data_ptr = data.as_ptr();
            io.write_at_async(shard_id, data_ptr, data_len, file_offset)?
        };
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
    let mut pending = VecDeque::new();
    let mut inflight = FuturesUnordered::new();
    let max_inflight = read_inflight.max(1);

    loop {
        while inflight.len() < max_inflight {
            let Some(task) = pending.pop_front() else {
                break;
            };
            inflight.push(execute_read(task, Arc::clone(&io)));
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
            Some(cmd) = rx.recv() => {
                pending.push_back(ReadTask {
                    key: cmd.key,
                    entry: cmd.entry,
                    file_offset: cmd.file_offset,
                    target: cmd.target,
                    _read_pin: cmd._read_pin,
                    done_tx: cmd.done_tx,
                });
            }
            else => break,
        }
    }

    while let Some(completion) = inflight.next().await {
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
}

async fn execute_read(task: ReadTask, io: Arc<UringIoEngine>) -> ReadCompletion {
    let ReadTask {
        key,
        entry,
        file_offset,
        target,
        _read_pin,
        done_tx,
    } = task;
    let shard_id = entry.shard_id;
    let result = async move {
        match target {
            ReadTarget::Scratch(mut buffer) => {
                let buffer_len = buffer.len();
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
            ReadTarget::Direct { target_addr, len } => {
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
        }
    }
    .await
    .map_err(|err| file_error_for_entry(&key, file_offset, err));
    ReadCompletion {
        key,
        entry,
        result,
        _read_pin,
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

struct UringShard {
    read_rx: crossbeam::channel::Receiver<IoCtx>,
    write_rx: crossbeam::channel::Receiver<IoCtx>,
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

        loop {
            let mut inflight = read_inflight + write_inflight;
            while inflight < self.io_depth && !(read_closed && write_closed) {
                let next = self.try_recv_weighted(
                    &mut read_closed,
                    &mut write_closed,
                    read_inflight,
                    write_inflight,
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
                let Some(ctx) = self.recv_blocking(&mut read_closed, &mut write_closed) else {
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
    ) -> Option<IoCtx> {
        let prefer_read = read_inflight <= write_inflight.saturating_mul(self.read_weight);
        if prefer_read {
            self.try_recv_read(read_closed)
                .or_else(|| self.try_recv_write(write_closed))
        } else {
            self.try_recv_write(write_closed)
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

    fn try_recv_write(&self, write_closed: &mut bool) -> Option<IoCtx> {
        if *write_closed {
            return None;
        }
        match self.write_rx.try_recv() {
            Ok(ctx) => Some(ctx),
            Err(crossbeam::channel::TryRecvError::Empty) => None,
            Err(crossbeam::channel::TryRecvError::Disconnected) => {
                *write_closed = true;
                None
            }
        }
    }

    fn recv_blocking(&self, read_closed: &mut bool, write_closed: &mut bool) -> Option<IoCtx> {
        loop {
            match (!*read_closed, !*write_closed) {
                (true, true) => {
                    crossbeam::channel::select! {
                        recv(self.read_rx) -> msg => match msg {
                            Ok(ctx) => return Some(ctx),
                            Err(_) => *read_closed = true,
                        },
                        recv(self.write_rx) -> msg => match msg {
                            Ok(ctx) => return Some(ctx),
                            Err(_) => *write_closed = true,
                        },
                    }
                }
                (true, false) => match self.read_rx.recv() {
                    Ok(ctx) => return Some(ctx),
                    Err(_) => *read_closed = true,
                },
                (false, true) => match self.write_rx.recv() {
                    Ok(ctx) => return Some(ctx),
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
    write_txs: Vec<crossbeam::channel::Sender<IoCtx>>,
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
        self.pick_tx(io_type, shard_id).send(ctx).map_err(|err| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                format!("io_uring send failed: {}", err),
            )
        })?;
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
        self.pick_tx(io_type, shard_id).send(ctx).map_err(|err| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                format!("io_uring send failed: {}", err),
            )
        })?;
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

    fn pick_tx(&self, io_type: IoType, shard_id: usize) -> &crossbeam::channel::Sender<IoCtx> {
        match io_type {
            IoType::Read => &self.read_txs[shard_id % self.read_txs.len()],
            IoType::Write => &self.write_txs[shard_id % self.write_txs.len()],
            IoType::Readv => &self.read_txs[shard_id % self.read_txs.len()],
            IoType::Writev => &self.write_txs[shard_id % self.write_txs.len()],
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
        let mut raw = std::ptr::null_mut();
        let rc = unsafe { libc::posix_memalign(&mut raw, SSD_ALIGNMENT, len) };
        if rc != 0 || raw.is_null() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!("posix_memalign failed with rc={}", rc),
            }));
        }
        unsafe {
            std::ptr::write_bytes(raw as *mut u8, 0, len);
        }
        Ok(Self {
            ptr: NonNull::new(raw as *mut u8).expect("posix_memalign returned non-null"),
            len,
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
        unsafe {
            libc::free(self.ptr.as_ptr() as *mut libc::c_void);
        }
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

    fn new_root() -> PathBuf {
        std::env::current_dir()
            .unwrap()
            .join("target")
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
            .write_tx
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
