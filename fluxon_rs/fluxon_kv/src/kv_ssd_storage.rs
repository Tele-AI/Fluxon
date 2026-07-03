use crate::master_kv_router::put::PutIDForAKey;
use crate::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult};
use ::tokio::{
    sync::{Notify, mpsc as tokio_mpsc, oneshot},
    task,
};
use futures::stream::{FuturesUnordered, StreamExt};
use io_uring::{IoUring, opcode, types::Fd};
use parking_lot::Mutex;
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
pub(crate) const DEFAULT_READ_TRANSFER_PIPELINE_CHUNK_BYTES: u64 = 4 * 1024 * 1024;
pub(crate) const DEFAULT_READ_TRANSFER_PIPELINE_INFLIGHT: usize = 4;

#[derive(Clone, Debug)]
pub struct KvSsdStorageInit {
    pub root_dirs: Vec<PathBuf>,
    pub max_bytes: u64,
}

#[derive(Debug)]
pub struct KvSsdStorage {
    root_dirs: Vec<PathBuf>,
    devices: Vec<SsdDeviceWorker>,
    shard_to_device: Vec<usize>,
    next_write_device: AtomicUsize,
    inner: Arc<Mutex<KvSsdStorageInner>>,
    space_notify: Arc<Notify>,
}

#[derive(Debug)]
struct SsdDeviceWorker {
    device_id: u64,
    root_dir: PathBuf,
    shard_ids: Vec<usize>,
    _files: Vec<std::fs::File>,
    _io: Arc<UringIoEngine>,
    write_tx: tokio_mpsc::Sender<WriteCommand>,
    read_tx: tokio_mpsc::Sender<ReadCommand>,
}

#[derive(Clone, Debug)]
struct SsdDeviceRoot {
    device_id: u64,
    root_dir: PathBuf,
}

struct OpenedSsdShard {
    shard_id: usize,
    device_idx: usize,
    file: std::fs::File,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SsdLoadedChunk {
    pub offset: u64,
    pub stage_addr: u64,
    pub len: u64,
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
    Ready(SsdIndexEntry),
    Existing,
    BlockedByBusyIo,
}

#[derive(Debug)]
enum SsdAllocation {
    Ready { begin: u64, file_offset: u64 },
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
        if self.entries.contains_key(&key) {
            return Ok(SsdPreparedWrite::Existing);
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
            let (begin, file_offset) = match self.allocate_contiguous(shard_id, aligned_len) {
                SsdAllocation::Ready { begin, file_offset } => (begin, file_offset),
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
            return Ok(SsdPreparedWrite::Ready(entry));
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
        self.advance_tail(shard_id, new_tail);
        SsdAllocation::Ready {
            begin,
            file_offset: begin % capacity,
        }
    }

    fn advance_tail(&mut self, shard_id: usize, new_tail: u64) {
        if new_tail <= self.shards[shard_id].tail {
            return;
        }
        debug_assert!(!self.has_busy_entries_before(shard_id, new_tail));
        self.shards[shard_id].tail = new_tail;

        while let Some(key) = self.shards[shard_id].order.front() {
            match self.entries.get(key) {
                Some(state) if state.entry().begin >= new_tail => break,
                _ => {
                    let key = self.shards[shard_id]
                        .order
                        .pop_front()
                        .expect("front key exists");
                    self.entries.remove(&key);
                }
            }
        }
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
    done_tx: oneshot::Sender<KvResult<()>>,
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
    done_tx: oneshot::Sender<KvResult<()>>,
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
    done_tx: oneshot::Sender<KvResult<()>>,
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
    let mut out = String::with_capacity(raw.len().max(1));
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "unnamed".to_string()
    } else {
        out
    }
}

impl KvSsdStorage {
    pub fn new(init: KvSsdStorageInit) -> KvResult<Self> {
        if init.max_bytes < SSD_ALIGNMENT as u64 {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!("kv ssd storage max_bytes must be >= {}", SSD_ALIGNMENT),
            }));
        }
        if init.root_dirs.is_empty() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "kv ssd storage root_dirs must contain at least one path".to_string(),
            }));
        }

        let device_roots = deduplicate_device_roots(&init.root_dirs)?;
        let effective_root_dirs = device_roots
            .iter()
            .map(|root| root.root_dir.clone())
            .collect::<Vec<_>>();
        let shard_count = choose_shard_count(init.max_bytes, device_roots.len());
        let shard_capacity = aligned_shard_capacity(init.max_bytes, shard_count)?;
        let opened_shards = open_cache_files(&device_roots, shard_count, shard_capacity)?;
        let inner = Arc::new(Mutex::new(KvSsdStorageInner {
            ring: SsdRingBuffer::new(vec![shard_capacity; shard_count]),
        }));
        let space_notify = Arc::new(Notify::new());
        let mut shard_to_device = vec![0usize; shard_count];
        let mut device_shards = device_roots
            .iter()
            .map(|root| (root.clone(), Vec::<(usize, std::fs::File)>::new()))
            .collect::<Vec<_>>();
        for opened in opened_shards {
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
            let fds = shard_files
                .iter()
                .map(|(shard_id, file)| (*shard_id, file.as_raw_fd()))
                .collect::<Vec<_>>();
            let io = Arc::new(UringIoEngine::new_multi(
                fds,
                UringConfig {
                    threads: DEFAULT_URING_THREADS,
                    io_depth: DEFAULT_URING_IO_DEPTH,
                },
            )?);
            let (write_tx, write_rx) = tokio_mpsc::channel(DEFAULT_WRITE_QUEUE_DEPTH);
            let (read_tx, read_rx) = tokio_mpsc::channel(DEFAULT_READ_QUEUE_DEPTH);

            task::spawn(ssd_writer_loop(
                Arc::clone(&inner),
                write_rx,
                Arc::clone(&io),
                Arc::clone(&space_notify),
                DEFAULT_WRITE_INFLIGHT,
                shard_ids.clone(),
            ));
            task::spawn(ssd_reader_loop(
                Arc::clone(&inner),
                read_rx,
                Arc::clone(&io),
                DEFAULT_READ_INFLIGHT,
            ));

            devices.push(SsdDeviceWorker {
                device_id: device_root.device_id,
                root_dir: device_root.root_dir,
                shard_ids,
                _files: shard_files
                    .into_iter()
                    .map(|(_, file)| file)
                    .collect::<Vec<_>>(),
                _io: io,
                write_tx,
                read_tx,
            });
        }

        Ok(Self {
            root_dirs: effective_root_dirs,
            devices,
            shard_to_device,
            next_write_device: AtomicUsize::new(0),
            inner,
            space_notify,
        })
    }

    pub fn root_dirs(&self) -> &[PathBuf] {
        &self.root_dirs
    }

    fn next_write_tx(&self) -> KvResult<tokio_mpsc::Sender<WriteCommand>> {
        if self.devices.is_empty() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "kv ssd storage has no active device".to_string(),
            }));
        }
        let idx = self.next_write_device.fetch_add(1, Ordering::Relaxed) % self.devices.len();
        Ok(self.devices[idx].write_tx.clone())
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
        Ok(device.read_tx.clone())
    }

    pub async fn persist_from_addr(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        addr: u64,
        len: u64,
    ) -> KvResult<()> {
        validate_key(key)?;
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
        validate_key(key)?;
        let aligned_len = align_up_usize(data.len(), SSD_ALIGNMENT)?;
        let mut buffer = AlignedBuffer::zeroed(aligned_len)?;
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), buffer.as_mut_ptr(), data.len());
        }
        self.persist_buffer(key, put_id, data.len() as u64, buffer)
            .await
    }

    async fn persist_buffer(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        entry_len: u64,
        data: AlignedBuffer,
    ) -> KvResult<()> {
        let (done_tx, done_rx) = oneshot::channel();
        let write_tx = self.next_write_tx()?;
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
        validate_key(key)?;
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
        validate_key(key)?;
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
        let key = KvSsdKey {
            key: key.to_string(),
            put_id,
        };
        self.inner.lock().ring.get(&key).is_some()
    }
}

async fn ssd_writer_loop(
    inner: Arc<Mutex<KvSsdStorageInner>>,
    mut rx: tokio_mpsc::Receiver<WriteCommand>,
    io: Arc<UringIoEngine>,
    space_notify: Arc<Notify>,
    write_inflight: usize,
    shard_ids: Vec<usize>,
) {
    let mut pending: VecDeque<WriteCommand> = VecDeque::new();
    let mut inflight = FuturesUnordered::new();
    let max_inflight = write_inflight.max(1);

    loop {
        while inflight.len() < max_inflight {
            let Some(cmd) = pending.pop_front() else {
                break;
            };
            let prepared = {
                let mut inner = inner.lock();
                inner
                    .ring
                    .prepare_write_on_shards(cmd.key.clone(), cmd.entry_len, &shard_ids)
            };
            match prepared {
                Ok(SsdPreparedWrite::Ready(entry)) => {
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
                    let _ = cmd.done_tx.send(Ok(()));
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
            let prepared = {
                let mut inner = inner.lock();
                inner
                    .ring
                    .prepare_write_on_shards(cmd.key.clone(), cmd.entry_len, &shard_ids)
            };
            match prepared {
                Ok(SsdPreparedWrite::Ready(entry)) => {
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
                    let _ = cmd.done_tx.send(Ok(()));
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

fn finish_write_completion(
    inner: &Arc<Mutex<KvSsdStorageInner>>,
    space_notify: &Notify,
    completion: WriteCompletion,
) {
    let committed = inner
        .lock()
        .ring
        .commit(&completion.key, completion.success);
    space_notify.notify_one();
    let result = if completion.success && !committed {
        Err(KvError::Api(ApiError::KeyNotFound {
            key: completion.key.key.clone(),
        }))
    } else {
        completion.result
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
            io.writev_at_async(shard_id, vec![(data_ptr, data_len)], file_offset)?
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
                    io.readv_at_async(shard_id, vec![(buffer_ptr, buffer_len)], file_offset)?
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
                let rx =
                    io.readv_at_async(shard_id, vec![(target_addr as *mut u8, len)], file_offset)?;
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
}

#[derive(Clone, Copy)]
enum IoType {
    Readv,
    Writev,
}

struct IoCtx {
    io_type: IoType,
    fd: RawFd,
    len: usize,
    offset: u64,
    complete: oneshot::Sender<io::Result<usize>>,
    iovecs: Box<[libc::iovec]>,
}

unsafe impl Send for IoCtx {}

struct UringShard {
    read_rx: crossbeam::channel::Receiver<IoCtx>,
    write_rx: crossbeam::channel::Receiver<IoCtx>,
    uring: IoUring,
    io_depth: usize,
    read_weight: usize,
}

impl UringShard {
    fn run(mut self) {
        let mut read_inflight = 0usize;
        let mut write_inflight = 0usize;
        let mut read_closed = false;
        let mut write_closed = false;

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
                self.submit_ctx(ctx, &mut read_inflight, &mut write_inflight);
                inflight = read_inflight + write_inflight;
            }

            if read_closed && write_closed && inflight == 0 {
                return;
            }
            if inflight == 0 {
                let Some(ctx) = self.recv_blocking(&mut read_closed, &mut write_closed) else {
                    continue;
                };
                self.submit_ctx(ctx, &mut read_inflight, &mut write_inflight);
                continue;
            }
            if let Err(err) = self.uring.submit_and_wait(1) {
                while let Some(cqe) = self.uring.completion().next() {
                    let data = cqe.user_data();
                    if data != 0 {
                        let ctx = unsafe { Box::from_raw(data as *mut IoCtx) };
                        let _ = ctx.complete.send(Err(io::Error::other(format!(
                            "io_uring submit failed: {err}"
                        ))));
                    }
                }
                return;
            }

            for cqe in self.uring.completion() {
                let data = cqe.user_data();
                if data == 0 {
                    continue;
                }
                let ctx = unsafe { Box::from_raw(data as *mut IoCtx) };
                match ctx.io_type {
                    IoType::Readv => read_inflight = read_inflight.saturating_sub(1),
                    IoType::Writev => write_inflight = write_inflight.saturating_sub(1),
                }
                let res = cqe.result();
                let send_res = if res < 0 {
                    Err(io::Error::from_raw_os_error(-res))
                } else {
                    Ok(res as usize)
                };
                let _ = ctx.complete.send(send_res);
            }
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

    fn submit_ctx(&mut self, ctx: IoCtx, read_inflight: &mut usize, write_inflight: &mut usize) {
        let fd = Fd(ctx.fd);
        let iovecs_ptr = ctx.iovecs.as_ptr();
        let sqe = match ctx.io_type {
            IoType::Readv => opcode::Readv::new(fd, iovecs_ptr, ctx.len as _)
                .offset(ctx.offset)
                .build(),
            IoType::Writev => opcode::Writev::new(fd, iovecs_ptr, ctx.len as _)
                .offset(ctx.offset)
                .build(),
        };
        let io_type = ctx.io_type;
        let data = Box::into_raw(Box::new(ctx)) as u64;
        let sqe = sqe.user_data(data);
        let push_result = unsafe { self.uring.submission().push(&sqe) };
        if push_result.is_err() {
            let ctx = unsafe { Box::from_raw(data as *mut IoCtx) };
            let _ = ctx
                .complete
                .send(Err(io::Error::other("submission queue full")));
            return;
        }
        match io_type {
            IoType::Readv => *read_inflight += 1,
            IoType::Writev => *write_inflight += 1,
        }
    }
}

#[derive(Debug)]
struct UringIoEngine {
    fds: HashMap<usize, RawFd>,
    read_txs: Vec<crossbeam::channel::Sender<IoCtx>>,
    write_txs: Vec<crossbeam::channel::Sender<IoCtx>>,
    handles: Vec<JoinHandle<()>>,
}

impl UringIoEngine {
    fn new_multi(shard_fds: Vec<(usize, RawFd)>, cfg: UringConfig) -> io::Result<Self> {
        if cfg.threads == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "threads must be > 0",
            ));
        }
        if shard_fds.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "at least one fd is required",
            ));
        }
        let fds = shard_fds.into_iter().collect::<HashMap<_, _>>();
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
                        uring,
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
            fds,
            read_txs,
            write_txs,
            handles,
        })
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
            iovecs: iovecs_libc,
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

fn choose_shard_count(max_bytes: u64, root_count: usize) -> usize {
    let max_aligned_shards = (max_bytes / SSD_ALIGNMENT as u64).max(1) as usize;
    DEFAULT_SHARDS_PER_OWNER
        .max(root_count)
        .min(max_aligned_shards)
        .max(1)
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

fn deduplicate_device_roots(root_dirs: &[PathBuf]) -> KvResult<Vec<SsdDeviceRoot>> {
    if root_dirs.is_empty() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "kv ssd storage root_dirs must contain at least one path".to_string(),
        }));
    }
    let mut seen_devices = HashSet::new();
    let mut device_roots = Vec::new();
    for root_dir in root_dirs {
        fs::create_dir_all(root_dir).map_err(|err| file_error(root_dir, 0, err))?;
        let metadata = fs::metadata(root_dir).map_err(|err| file_error(root_dir, 0, err))?;
        let device_id = metadata.dev();
        if seen_devices.insert(device_id) {
            device_roots.push(SsdDeviceRoot {
                device_id,
                root_dir: root_dir.clone(),
            });
        }
    }
    if device_roots.is_empty() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "kv ssd storage root_dirs contains no usable device".to_string(),
        }));
    }
    Ok(device_roots)
}

fn open_cache_files(
    device_roots: &[SsdDeviceRoot],
    shard_count: usize,
    shard_capacity: u64,
) -> KvResult<Vec<OpenedSsdShard>> {
    if device_roots.is_empty() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "kv ssd storage root_dirs must contain at least one path".to_string(),
        }));
    }
    let mut files = Vec::with_capacity(shard_count);
    for shard_id in 0..shard_count {
        let device_idx = shard_id % device_roots.len();
        let root_dir = &device_roots[device_idx].root_dir;
        let shards_dir = root_dir.join("shards");
        fs::create_dir_all(&shards_dir).map_err(|err| file_error(&shards_dir, 0, err))?;
        let path = shards_dir.join(format!("shard-{shard_id:06}.dat"));
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(&path)
            .map_err(|err| file_error(&path, 0, err))?;
        file.set_len(shard_capacity)
            .map_err(|err| file_error(&path, 0, err))?;
        files.push(OpenedSsdShard {
            shard_id,
            device_idx,
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
    use uuid::Uuid;

    fn new_root() -> PathBuf {
        std::env::current_dir()
            .unwrap()
            .join("target")
            .join("fluxon_kv_ssd_tests")
            .join(Uuid::new_v4().to_string())
    }

    async fn new_store(max_bytes: u64) -> KvSsdStorage {
        KvSsdStorage::new(KvSsdStorageInit {
            root_dirs: vec![new_root()],
            max_bytes,
        })
        .unwrap()
    }

    fn test_key(key: &str, version: u64) -> KvSsdKey {
        KvSsdKey {
            key: key.to_string(),
            put_id: (version, 0),
        }
    }

    fn prepare_ready(ring: &mut SsdRingBuffer, key: &KvSsdKey) -> SsdIndexEntry {
        match ring.prepare_write(key.clone(), 500).unwrap() {
            SsdPreparedWrite::Ready(entry) => entry,
            other => panic!("expected ready SSD write, got {other:?}"),
        }
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
            root_dirs: vec![root_a.clone(), root_b.clone()],
            max_bytes: 4 * SSD_ALIGNMENT as u64,
        })
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
                SsdPreparedWrite::Ready(entry) => entry,
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
        store.persist("old", (1, 0), &[1u8; 500]).await.unwrap();
        store.persist("filler", (2, 0), &[2u8; 500]).await.unwrap();
        store.persist("new", (3, 0), &[3u8; 500]).await.unwrap();

        assert!(!store.has_entry("old", (1, 0)).await);
        assert!(store.has_entry("filler", (2, 0)).await);
        assert!(store.has_entry("new", (3, 0)).await);
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
    fn safe_component_replaces_path_separators() {
        assert_eq!(safe_path_component("owner/a:b"), "owner_a_b");
    }
}
