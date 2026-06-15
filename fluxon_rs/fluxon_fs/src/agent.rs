use std::borrow::Cow;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use equivalent::Equivalent;
use fluxon_commu::p2p::RpcTransportPolicy;
use hashbrown::HashMap;
use parking_lot::{Condvar, Mutex, RwLock};
use prost::bytes::{Bytes, BytesMut};
use sha2::{Digest, Sha256};

use fluxon_framework_compiled::shutdown::ViewShutdownExt;
use fluxon_fs_core::config::{
    FLUXON_FS_METADATA_INVALIDATION_STATE_JSON_KEY, FLUXON_FS_MOUNT_EXPORTS_JSON_KEY,
    FLUXON_FS_RPC_TOKEN_PAYLOAD_KEY, FluxonFsRequestIdentity, FsMetadataInvalidationEventWire,
    FsMetadataInvalidationScopeWire, FsMetadataInvalidationStateWire, build_rpc_token,
};
use fluxon_fs_core::s3_gateway::{
    FS_S3_INTERNAL_MULTIPART_PAYLOAD_KEY, is_internal_multipart_relpath,
};
use fluxon_kv::Framework as KvFramework;
use fluxon_kv::KvClientTrait;
use fluxon_kv::cluster_manager::NodeID;
use fluxon_kv::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult};
use fluxon_kv::user_api::FluxonUserApi;
use fluxon_kv::user_api::flat_dict::{FlatDict, FlatValue};
use fluxon_kv::user_api::{
    USER_RPC_DEFAULT_TIMEOUT_MS, decode_flat_dict_bytes, encode_flat_dict_bytes,
};
use fluxon_kv::user_rpc::user_rpc_call;
use tokio::sync::{Mutex as TokioMutex, mpsc as tokio_mpsc};

use crate::cache_controller::{CacheController, PieceKey, StagePieceFn, StagePieceRangeFn};
use crate::config::{
    CacheMode, FLUXON_FS_CONTROL_SCHEMA_VERSION, FS_MASTER_BOOTSTRAP_PULL_INTERVAL_MS,
    FS_MASTER_CONFIG_RPC_PATH, FS_MASTER_EXPORT_REGISTRY_RPC_PATH,
    FS_MASTER_METADATA_INVALIDATION_PUBLISH_RPC_PATH, FluxonFsExport, FluxonFsExportRoutingMode,
    FluxonFsGlobalConfig, FluxonFsMasterConfig, FluxonFsRule, FluxonFsS3KvMissPolicy,
    OnRefreshError, WriteMode, parse_master_config_from_file,
};
use crate::remote_disk_cache::{
    REMOTE_DISK_CACHE_METRICS_SOURCE, REMOTE_DISK_CACHE_MIN_FILE_BYTES,
    REMOTE_DISK_CACHE_READ_CHUNK_BYTES, RemoteDiskCacheManager, disk_cache_max_bytes_from_env,
    resolve_disk_cache_root,
};
use crate::retry::{
    BackoffConfig, DEFAULT_WARN_INTERVAL_SECS, WarnConfig, next_backoff, should_warn,
};
use crate::write_session_rpc::{
    self, FsAbortWriteSessionReq, FsCloseWriteSessionReq, FsOpenWriteSessionReq,
    FsWriteSessionChunkReq, FsWriteSessionDataAck, FsWriteSessionDataFrame,
};
use fluxon_util::run_async_from_sync::{SyncAsyncBridge, spawn_blocking_allow_sync_async_bridge};

// Keep the chunk size consistent with the shared FS contract (S3 gateway piece size).
pub const REMOTE_CHUNK_BYTES: usize = fluxon_fs_core::s3_gateway::FS_S3_OBJECT_PIECE_BYTES;
pub const REMOTE_READ_CHUNK_BYTES: usize = 8 * 1024 * 1024;
pub const REMOTE_WRITE_SESSION_CHUNK_BYTES: usize = 8 * 1024 * 1024;
const REMOTE_WRITE_SESSION_SEND_BATCH_MAX_FRAMES: usize = 4;
fn remote_write_session_peer_sender_workers() -> usize {
    4
}
const REMOTE_WRITE_SESSION_RPC_TIMEOUT_MAX_MS: u64 = 240_000;
const REMOTE_WRITE_SESSION_RPC_TIMEOUT_PER_MIB_MS: u64 = 1_000;
const REMOTE_WRITE_SESSION_CONTROL_RPC_TIMEOUT_MS: u64 = 240_000;
const REMOTE_INLINE_FD_CACHE_MAX_RESIDENT_BYTES: usize = 64 * 1024 * 1024;
const REMOTE_INLINE_FD_CACHE_MAX_ENTRIES: usize = 256;

// English note: many retry loops in this module use seconds-level intervals.
// We intentionally cap shutdown latency by splitting sleeps into small chunks.
const SHUTDOWN_POLL_SLEEP_STEP_MS: u64 = 200;

#[derive(Debug)]
pub enum OpenPlan {
    /// Caller should use real filesystem directly.
    Bypass { local_write_through: bool },

    /// Return a memfd-backed file (Python side will create memfd and open it).
    Bytes(Vec<u8>),

    /// Return a read-only memfd-backed fd. Python should take ownership of the fd and open it.
    Fd {
        fd: OwnedFd,
        size: i64,
        mtime_ns: i64,
        export_name: Option<String>,
        relpath: Option<String>,
        upload_on_close: bool,
    },

    /// Return a Python-side file-like object that reads/writes via remote RPC on demand.
    ///
    /// English note:
    /// - This plan intentionally avoids content IO at open-time.
    /// - Content may still be served from KV during read operations (piece cache), but that happens
    ///   at read-time, not open-time.
    /// - It is not an OS-level fd; Python must implement a file-like wrapper and must not promise
    ///   `fileno()`-based behaviors (mmap, fcntl/flock, passing fd to C extensions, etc.).
    RemoteHandle {
        export_name: String,
        relpath: String,
        size: i64,
        mtime_ns: i64,
    },
}

#[derive(Debug, Clone)]
pub struct RemoteStat {
    pub exists: bool,
    pub is_file: bool,
    pub is_dir: bool,
    pub size: i64,
    pub mtime_ns: i64,
    pub mode: i64,
}

#[derive(Debug, Clone)]
pub struct RemoteDirEntry {
    pub name: String,
    pub is_file: bool,
    pub is_dir: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i64)]
pub(crate) enum FsAgentRpcErrorKind {
    InvalidArgument = 1,
    Os = 2,
    AccessDenied = 3,
    Internal = 4,
}

pub(crate) const FS_AGENT_RPC_ERR_KIND_KEY: &str = "err_kind";

impl FsAgentRpcErrorKind {
    pub(crate) fn as_i64(self) -> i64 {
        self as i64
    }

    pub(crate) fn from_i64(value: i64) -> Option<Self> {
        match value {
            1 => Some(Self::InvalidArgument),
            2 => Some(Self::Os),
            3 => Some(Self::AccessDenied),
            4 => Some(Self::Internal),
            _ => None,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum FsAgentError {
    #[error("invalid argument: {detail}")]
    InvalidArgument { detail: String },

    #[error("shutdown: {detail}")]
    Shutdown { detail: String },

    #[error("access denied: path={path} detail={detail}")]
    AccessDenied { path: String, detail: String },

    #[error("os error: errno={errno} path={path} detail={detail}")]
    Os {
        errno: i32,
        path: String,
        detail: String,
    },

    #[error("kv error: {0}")]
    Kv(#[from] KvError),

    #[error("io error: path={path} detail={detail}")]
    Io { path: String, detail: String },
}

impl FsAgentError {
    pub fn os(errno: i32, path: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::Os {
            errno,
            path: path.into(),
            detail: detail.into(),
        }
    }
}

#[derive(Debug, Clone)]
struct Mount {
    mount_dir_abs: String,
    export_name: String,
}

#[derive(Debug, Clone)]
struct MountRemoteDirPrepared {
    mdir: String,
    remote_root_dir_abs: String,
    master_cfg: Option<FluxonFsMasterConfig>,
}

pub struct FluxonFsAgent {
    lifecycle: crate::Framework,
    kv_framework: Arc<KvFramework>,
    api: Arc<FluxonUserApi>,
    rt_handle: tokio::runtime::Handle,

    cfg: Arc<RwLock<Option<FluxonFsGlobalConfig>>>,
    master_cfg: Arc<RwLock<Option<FluxonFsMasterConfig>>>,
    master_pull_interval_ms: Arc<RwLock<Option<u64>>>,
    mounts: Arc<RwLock<Vec<Mount>>>,

    export_rr: Arc<Mutex<BTreeMap<String, usize>>>,
    export_nodes_cache: Arc<Mutex<ExportNodesCache>>,
    request_identity: RwLock<Option<CurrentRequestIdentity>>,
    access_model_fingerprint: RwLock<Option<String>>,
    remote_metadata_cache: RwLock<HashMap<RemoteMetadataCacheKey, RemoteMetadataCacheEntry>>,
    remote_write_sessions: Arc<RwLock<HashMap<String, Arc<RemoteWriteSessionClientEntry>>>>,
    remote_write_sessions_by_remote:
        Arc<RwLock<HashMap<String, Arc<RemoteWriteSessionClientEntry>>>>,
    remote_write_peer_senders: RwLock<HashMap<String, Arc<RemoteWriteSessionPeerSender>>>,
    remote_write_session_next_id: AtomicU64,
    remote_open_cache_resident_bytes: AtomicUsize,
    remote_open_cache_access_seq: AtomicU64,
    metadata_invalidation_state: MetadataInvalidationState,
    metadata_invalidation_publish: MetadataInvalidationPublishQueue,
    cache_controller: Arc<CacheController>,
    remote_disk_cache: RwLock<Option<Arc<RemoteDiskCacheManager>>>,

    // file_abs -> first seen unix_ms of a cache refresh failure
    refresh_error_first_seen_ms: Mutex<BTreeMap<String, u64>>,
}

#[derive(Debug, Clone)]
struct ExportNodesCache {
    // export_name -> (last_refresh_instant, nodes)
    nodes: BTreeMap<String, (Instant, Vec<String>)>,
}

#[derive(Debug, Clone)]
struct CurrentRequestIdentity {
    identity: FluxonFsRequestIdentity,
    fingerprint: String,
}

#[derive(Debug, Clone)]
struct RemoteWriteSessionCallCtx {
    export: FluxonFsExport,
    relpath_rpc: String,
    fs_rpc_token: Option<String>,
    allow_s3_internal_multipart: bool,
}

#[derive(Debug)]
struct RemoteWriteSessionClientSubmit {
    base_seq_no: u64,
    offset: i64,
    data: Bytes,
    enqueued_at: Instant,
}

#[derive(Debug)]
struct RemoteWriteSessionClientFrame {
    seq_no: u64,
    offset: i64,
    data: Bytes,
    enqueued_at: Instant,
}

#[derive(Debug, Clone)]
struct RemoteWriteSessionClientPendingBatch {
    seq_no: u64,
    frame_count: u64,
    total_bytes: usize,
}

#[derive(Debug, Clone)]
struct RemoteWriteSessionClientSendBatch {
    seq_no: u64,
    offset: i64,
    data_parts: Vec<Bytes>,
    frame_count: u64,
    total_bytes: usize,
    enqueued_at: Instant,
}

#[derive(Debug, Clone, Copy)]
struct RemoteWriteSessionClientPendingConfirm {
    expected_frames: u64,
    timeout_bytes: usize,
}

#[derive(Debug)]
struct RemoteWriteSessionPeerSender {
    node_id: String,
    ready_tx: tokio_mpsc::UnboundedSender<Arc<RemoteWriteSessionClientEntry>>,
}

#[derive(Debug)]
struct RemoteWriteSessionClientEntry {
    node_id: String,
    remote_session_id: String,
    export_name: String,
    relpath_rpc: String,
    fs_rpc_token: Option<String>,
    allow_s3_internal_multipart: bool,
    peer_sender: Arc<RemoteWriteSessionPeerSender>,
    state: Arc<RemoteWriteSessionClientState>,
}

#[derive(Debug, Default)]
struct RemoteWriteSessionClientState {
    progress: Mutex<RemoteWriteSessionClientProgress>,
    cv: Condvar,
}

#[derive(Debug, Default)]
struct RemoteWriteSessionClientProgress {
    fatal_error: Option<String>,
    next_seq_no: u64,
    submitted_frames: u64,
    sent_frames: u64,
    acked_frames: u64,
    sending_frames: u64,
    buffered_off: Option<i64>,
    buffer: BytesMut,
    max_inflight_chunks: usize,
    queued_frames: VecDeque<RemoteWriteSessionClientFrame>,
    queued_frame_bytes: usize,
    pending_batches: BTreeMap<u64, RemoteWriteSessionClientPendingBatch>,
    early_acked_batches: BTreeMap<u64, u64>,
    confirm_inflight: bool,
    scheduled_sends: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RemoteWriteSessionClientFollowup {
    schedule_count: usize,
    confirm_seq_no: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetadataInvalidationScope {
    Exact,
    Prefix,
}

#[derive(Debug, Clone)]
struct MetadataInvalidationEvent {
    export_name: String,
    relpath: String,
    scope: MetadataInvalidationScope,
    seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct RemoteMetadataCacheKey {
    identity_fingerprint: String,
    export_name: String,
    relpath: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct RemoteMetadataCacheLookupKey<'a> {
    identity_fingerprint: &'a str,
    export_name: &'a str,
    relpath: &'a str,
}

impl<'a> Equivalent<RemoteMetadataCacheKey> for RemoteMetadataCacheLookupKey<'a> {
    fn equivalent(&self, key: &RemoteMetadataCacheKey) -> bool {
        self.identity_fingerprint == key.identity_fingerprint
            && self.export_name == key.export_name
            && self.relpath == key.relpath
    }
}

#[derive(Debug, Clone)]
struct RemoteMetadataCacheEntry {
    stat: RemoteStat,
    sig: Option<String>,
    authorized_at_ms: u64,
    invalidation_seq: u64,
    inline_fd: Option<Arc<RemoteInlineFdCacheEntry>>,
}

#[derive(Debug)]
struct RemoteInlineFdCacheEntry {
    fd: OwnedFd,
    size_bytes: usize,
    sig: String,
    last_access_seq: AtomicU64,
}

#[derive(Debug, Default)]
struct MetadataInvalidationState {
    latest_seq: AtomicU64,
}

#[derive(Debug, Clone)]
struct MetadataInvalidationPublishQueue {
    latest_seq: Arc<AtomicU64>,
    flush_inflight: Arc<AtomicBool>,
    pending: Arc<Mutex<VecDeque<MetadataInvalidationEvent>>>,
}

impl Default for MetadataInvalidationPublishQueue {
    fn default() -> Self {
        Self {
            latest_seq: Arc::new(AtomicU64::new(0)),
            flush_inflight: Arc::new(AtomicBool::new(false)),
            pending: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

fn metadata_cache_matches_exact(
    key: &RemoteMetadataCacheKey,
    export_name: &str,
    relpath: &str,
) -> bool {
    key.export_name == export_name && key.relpath == relpath
}

fn metadata_cache_matches_prefix(
    key: &RemoteMetadataCacheKey,
    export_name: &str,
    relpath_prefix: &str,
) -> bool {
    if key.export_name != export_name {
        return false;
    }
    if relpath_prefix.is_empty() {
        return true;
    }
    let prefix_slash = format!("{}/", relpath_prefix);
    key.relpath == relpath_prefix || key.relpath.starts_with(&prefix_slash)
}

fn metadata_cache_lookup_key<'a>(
    identity_fingerprint: &'a str,
    export_name: &'a str,
    relpath: &'a str,
) -> RemoteMetadataCacheLookupKey<'a> {
    RemoteMetadataCacheLookupKey {
        identity_fingerprint,
        export_name,
        relpath,
    }
}

fn open_cache_fd_entry_count(
    cache: &HashMap<RemoteMetadataCacheKey, RemoteMetadataCacheEntry>,
) -> usize {
    cache
        .values()
        .filter(|entry| entry.inline_fd.is_some())
        .count()
}

fn open_cache_take_inline_fd(entry: &mut RemoteMetadataCacheEntry) -> usize {
    entry.inline_fd.take().map(|fd| fd.size_bytes).unwrap_or(0)
}

fn remote_write_session_rpc_timeout_ms(payload_len_bytes: usize) -> u64 {
    let mib = (payload_len_bytes.saturating_add((1 << 20) - 1) / (1 << 20)) as u64;
    let timeout_ms = USER_RPC_DEFAULT_TIMEOUT_MS
        .saturating_add(mib.saturating_mul(REMOTE_WRITE_SESSION_RPC_TIMEOUT_PER_MIB_MS));
    timeout_ms
        .max(USER_RPC_DEFAULT_TIMEOUT_MS)
        .min(REMOTE_WRITE_SESSION_RPC_TIMEOUT_MAX_MS)
}

fn remote_write_session_frame_count(payload_len_bytes: usize) -> u64 {
    payload_len_bytes.div_ceil(REMOTE_WRITE_SESSION_CHUNK_BYTES) as u64
}

#[cfg(test)]
fn remote_write_session_frame_seq(
    base_seq_no: u64,
    window_base_bytes: usize,
    chunk_idx: usize,
) -> u64 {
    base_seq_no
        .saturating_add((window_base_bytes / REMOTE_WRITE_SESSION_CHUNK_BYTES) as u64)
        .saturating_add(chunk_idx as u64)
}

fn remote_write_session_remote_key(node_id: &str, remote_session_id: &str) -> String {
    format!("{}\0{}", node_id, remote_session_id)
}

fn remote_write_session_schedule_entry(session: &Arc<RemoteWriteSessionClientEntry>) {
    if session.peer_sender.ready_tx.send(session.clone()).is_err() {
        session.state.fail(format!(
            "remote write-session peer sender closed unexpectedly: session_id={} node_id={}",
            session.remote_session_id, session.node_id
        ));
    }
}

fn remote_write_session_should_schedule_locked(
    progress: &RemoteWriteSessionClientProgress,
) -> usize {
    if progress.queued_frames.is_empty() {
        return 0;
    }
    let max_inflight = progress.max_inflight_chunks.max(1) as u64;
    let inflight_frames = progress
        .sent_frames
        .saturating_add(progress.sending_frames)
        .saturating_sub(progress.acked_frames);
    if inflight_frames >= max_inflight {
        return 0;
    }
    let available_frames = max_inflight.saturating_sub(inflight_frames) as usize;
    let schedulable_frames = progress.queued_frames.len().min(available_frames);
    if schedulable_frames == 0 {
        return 0;
    }
    schedulable_frames.div_ceil(REMOTE_WRITE_SESSION_SEND_BATCH_MAX_FRAMES)
}

fn remote_write_session_fill_schedule_locked(
    progress: &mut RemoteWriteSessionClientProgress,
) -> usize {
    let desired = remote_write_session_should_schedule_locked(progress);
    let additional = desired.saturating_sub(progress.scheduled_sends);
    progress.scheduled_sends = progress.scheduled_sends.saturating_add(additional);
    additional
}

fn remote_write_session_stall_confirm_seq_locked(
    progress: &RemoteWriteSessionClientProgress,
) -> Option<u64> {
    let inflight_frames = progress.sent_frames.saturating_sub(progress.acked_frames);
    if inflight_frames < progress.max_inflight_chunks.max(1) as u64 {
        return None;
    }
    progress.pending_batches.keys().next_back().copied()
}

fn remote_write_session_refresh_acked_frames_locked(
    progress: &mut RemoteWriteSessionClientProgress,
) {
    progress.acked_frames = progress
        .pending_batches
        .keys()
        .next()
        .copied()
        .unwrap_or(progress.sent_frames);
}

impl RemoteWriteSessionClientState {
    fn submitted_frames(&self) -> Result<u64, String> {
        let progress = self.progress.lock();
        if let Some(detail) = progress.fatal_error.clone() {
            return Err(detail);
        }
        Ok(progress.submitted_frames)
    }

    fn wait_for_sent_frames(&self, expected_frames: u64) -> Result<(), String> {
        let mut progress = self.progress.lock();
        loop {
            if let Some(detail) = progress.fatal_error.clone() {
                return Err(detail);
            }
            if progress.sent_frames >= expected_frames {
                return Ok(());
            }
            self.cv.wait(&mut progress);
        }
    }

    fn acked_frames_ready(&self, expected_frames: u64) -> Result<bool, String> {
        let progress = self.progress.lock();
        if let Some(detail) = progress.fatal_error.clone() {
            return Err(detail);
        }
        Ok(progress.acked_frames >= expected_frames)
    }

    #[cfg(test)]
    fn wait_for_acked_frames(&self, expected_frames: u64) -> Result<(), String> {
        let mut progress = self.progress.lock();
        loop {
            if let Some(detail) = progress.fatal_error.clone() {
                return Err(detail);
            }
            if progress.acked_frames >= expected_frames {
                return Ok(());
            }
            self.cv.wait(&mut progress);
        }
    }

    fn buffer_append(
        &self,
        start_off: i64,
        data: Bytes,
        submit_bytes: usize,
        max_inflight_chunks: usize,
    ) -> Result<Vec<RemoteWriteSessionClientSubmit>, String> {
        if data.is_empty() {
            return Ok(Vec::new());
        }
        let submit_bytes = std::cmp::max(1, submit_bytes);
        let mut progress = self.progress.lock();
        if let Some(detail) = progress.fatal_error.clone() {
            return Err(detail);
        }
        let mut submits = Vec::new();
        progress.max_inflight_chunks = std::cmp::max(1, max_inflight_chunks);
        let live_len = progress.buffer.len();
        if let Some(buffered_off) = progress.buffered_off {
            let expected_off = buffered_off.saturating_add(live_len as i64);
            if expected_off != start_off {
                let flush_len = progress.buffer.len();
                if let Some(submit) = take_buffered_submit_locked(&mut progress, flush_len) {
                    submits.push(submit);
                }
                progress.buffered_off = Some(start_off);
            }
        } else {
            progress.buffered_off = Some(start_off);
        }
        progress.buffer.extend_from_slice(data.as_ref());
        while progress.buffer.len() >= submit_bytes {
            if let Some(submit) = take_buffered_submit_locked(&mut progress, submit_bytes) {
                submits.push(submit);
            }
        }
        Ok(submits)
    }

    fn flush_buffered(&self) -> Result<Vec<RemoteWriteSessionClientSubmit>, String> {
        let mut progress = self.progress.lock();
        if let Some(detail) = progress.fatal_error.clone() {
            return Err(detail);
        }
        if progress.buffer.is_empty() {
            return Ok(Vec::new());
        }
        let flush_len = progress.buffer.len();
        Ok(take_buffered_submit_locked(&mut progress, flush_len)
            .into_iter()
            .collect())
    }

    fn enqueue_submit(
        &self,
        submit: RemoteWriteSessionClientSubmit,
    ) -> Result<RemoteWriteSessionClientFollowup, String> {
        if submit.data.is_empty() {
            return Ok(RemoteWriteSessionClientFollowup::default());
        }
        let mut progress = self.progress.lock();
        if let Some(detail) = progress.fatal_error.clone() {
            return Err(detail);
        }
        let mut frame_start = 0usize;
        let mut next_seq_no = submit.base_seq_no;
        let mut next_offset = submit.offset;
        while frame_start < submit.data.len() {
            let frame_end = std::cmp::min(
                frame_start.saturating_add(REMOTE_WRITE_SESSION_CHUNK_BYTES),
                submit.data.len(),
            );
            let frame_data = submit.data.slice(frame_start..frame_end);
            progress.queued_frame_bytes =
                progress.queued_frame_bytes.saturating_add(frame_data.len());
            progress
                .queued_frames
                .push_back(RemoteWriteSessionClientFrame {
                    seq_no: next_seq_no,
                    offset: next_offset,
                    data: frame_data,
                    enqueued_at: submit.enqueued_at,
                });
            next_seq_no = next_seq_no.saturating_add(1);
            next_offset = next_offset.saturating_add((frame_end - frame_start) as i64);
            frame_start = frame_end;
        }
        let mut followup = RemoteWriteSessionClientFollowup::default();
        followup.schedule_count = remote_write_session_fill_schedule_locked(&mut progress);
        followup.confirm_seq_no = remote_write_session_stall_confirm_seq_locked(&progress);
        Ok(followup)
    }

    fn take_next_batch_for_send(
        &self,
    ) -> Result<Option<RemoteWriteSessionClientSendBatch>, String> {
        let mut progress = self.progress.lock();
        progress.scheduled_sends = progress.scheduled_sends.saturating_sub(1);
        if let Some(detail) = progress.fatal_error.clone() {
            return Err(detail);
        }
        let inflight_frames = progress
            .sent_frames
            .saturating_add(progress.sending_frames)
            .saturating_sub(progress.acked_frames);
        if inflight_frames >= progress.max_inflight_chunks.max(1) as u64 {
            return Ok(None);
        }
        let Some(frame) = progress.queued_frames.pop_front() else {
            return Ok(None);
        };
        progress.queued_frame_bytes = progress.queued_frame_bytes.saturating_sub(frame.data.len());
        let max_frames = usize::try_from(
            (progress.max_inflight_chunks.max(1) as u64).saturating_sub(inflight_frames),
        )
        .unwrap_or(REMOTE_WRITE_SESSION_SEND_BATCH_MAX_FRAMES)
        .max(1)
        .min(REMOTE_WRITE_SESSION_SEND_BATCH_MAX_FRAMES);
        let seq_no = frame.seq_no;
        let offset = frame.offset;
        let enqueued_at = frame.enqueued_at;
        let mut total_bytes = frame.data.len();
        let mut frame_count = 1u64;
        let mut next_seq_no = frame.seq_no.saturating_add(1);
        let mut next_offset = frame.offset.saturating_add(frame.data.len() as i64);
        let mut data_parts = vec![frame.data];
        while data_parts.len() < max_frames {
            let Some(next_frame) = progress.queued_frames.front() else {
                break;
            };
            if next_frame.seq_no != next_seq_no || next_frame.offset != next_offset {
                break;
            }
            let next_frame = progress
                .queued_frames
                .pop_front()
                .expect("front frame must exist");
            progress.queued_frame_bytes = progress
                .queued_frame_bytes
                .saturating_sub(next_frame.data.len());
            total_bytes = total_bytes.saturating_add(next_frame.data.len());
            next_seq_no = next_seq_no.saturating_add(1);
            next_offset = next_offset.saturating_add(next_frame.data.len() as i64);
            frame_count = frame_count.saturating_add(1);
            data_parts.push(next_frame.data);
        }
        progress.sending_frames = progress.sending_frames.saturating_add(frame_count);
        Ok(Some(RemoteWriteSessionClientSendBatch {
            seq_no,
            offset,
            data_parts,
            frame_count,
            total_bytes,
            enqueued_at,
        }))
    }

    fn finish_batch_send(
        &self,
        batch: &RemoteWriteSessionClientSendBatch,
        result: Result<(), String>,
    ) -> RemoteWriteSessionClientFollowup {
        let mut progress = self.progress.lock();
        progress.sending_frames = progress.sending_frames.saturating_sub(batch.frame_count);
        match result {
            Ok(()) => {
                progress.sent_frames = progress.sent_frames.saturating_add(batch.frame_count);
                let early_acked = progress.early_acked_batches.remove(&batch.seq_no);
                if let Some(acked_frames) = early_acked {
                    if acked_frames != batch.frame_count {
                        let detail = format!(
                            "remote write-session early ack frame_count mismatch: seq_no={} expected={} got={}",
                            batch.seq_no, batch.frame_count, acked_frames
                        );
                        if progress.fatal_error.is_none() {
                            progress.fatal_error = Some(detail);
                        }
                        progress.queued_frames.clear();
                        progress.queued_frame_bytes = 0;
                        progress.pending_batches.clear();
                        progress.early_acked_batches.clear();
                        progress.confirm_inflight = false;
                        progress.scheduled_sends = 0;
                        self.cv.notify_all();
                        return RemoteWriteSessionClientFollowup::default();
                    }
                } else {
                    progress.pending_batches.insert(
                        batch.seq_no,
                        RemoteWriteSessionClientPendingBatch {
                            seq_no: batch.seq_no,
                            frame_count: batch.frame_count,
                            total_bytes: batch.total_bytes,
                        },
                    );
                }
                remote_write_session_refresh_acked_frames_locked(&mut progress);
                let mut followup = RemoteWriteSessionClientFollowup {
                    schedule_count: remote_write_session_fill_schedule_locked(&mut progress),
                    confirm_seq_no: None,
                };
                followup.confirm_seq_no = remote_write_session_stall_confirm_seq_locked(&progress);
                self.cv.notify_all();
                followup
            }
            Err(detail) => {
                if progress.fatal_error.is_none() {
                    progress.fatal_error = Some(detail);
                }
                progress.queued_frames.clear();
                progress.queued_frame_bytes = 0;
                progress.pending_batches.clear();
                progress.early_acked_batches.clear();
                progress.confirm_inflight = false;
                progress.scheduled_sends = 0;
                self.cv.notify_all();
                RemoteWriteSessionClientFollowup::default()
            }
        }
    }

    fn handle_data_ack(&self, ack: &FsWriteSessionDataAck) -> Result<bool, String> {
        let mut progress = self.progress.lock();
        if !ack.ok {
            let detail = if ack.err_detail.trim().is_empty() {
                format!(
                    "remote write-session data ack failed: seq_no={}",
                    ack.seq_no
                )
            } else {
                ack.err_detail.clone()
            };
            if progress.fatal_error.is_none() {
                progress.fatal_error = Some(detail.clone());
            }
            progress.queued_frames.clear();
            progress.queued_frame_bytes = 0;
            progress.pending_batches.clear();
            progress.early_acked_batches.clear();
            progress.scheduled_sends = 0;
            self.cv.notify_all();
            return Err(detail);
        }
        if let Some(pending) = progress.pending_batches.get(&ack.seq_no) {
            if pending.frame_count != ack.frame_count {
                let detail = format!(
                    "remote write-session data ack frame_count mismatch: seq_no={} expected={} got={}",
                    ack.seq_no, pending.frame_count, ack.frame_count
                );
                if progress.fatal_error.is_none() {
                    progress.fatal_error = Some(detail.clone());
                }
                progress.scheduled_sends = 0;
                self.cv.notify_all();
                return Err(detail);
            }
        } else if ack.seq_no >= progress.sent_frames {
            progress
                .early_acked_batches
                .insert(ack.seq_no, ack.frame_count);
            self.cv.notify_all();
            return Ok(false);
        }
        progress.pending_batches.remove(&ack.seq_no);
        remote_write_session_refresh_acked_frames_locked(&mut progress);
        let should_schedule = remote_write_session_fill_schedule_locked(&mut progress) > 0;
        self.cv.notify_all();
        Ok(should_schedule)
    }

    fn begin_confirm_pending_batch(
        &self,
        seq_no: u64,
    ) -> Result<Option<RemoteWriteSessionClientPendingConfirm>, String> {
        let mut progress = self.progress.lock();
        if let Some(detail) = progress.fatal_error.clone() {
            return Err(detail);
        }
        let Some((expected_frames, timeout_bytes)) =
            progress.pending_batches.get(&seq_no).map(|pending| {
                (
                    pending.seq_no.saturating_add(pending.frame_count),
                    pending.total_bytes,
                )
            })
        else {
            return Ok(None);
        };
        if progress.confirm_inflight {
            return Ok(None);
        }
        progress.confirm_inflight = true;
        Ok(Some(RemoteWriteSessionClientPendingConfirm {
            expected_frames,
            timeout_bytes,
        }))
    }

    fn confirm_delivered_upto(&self, expected_frames: u64) -> Result<bool, String> {
        let mut progress = self.progress.lock();
        if let Some(detail) = progress.fatal_error.clone() {
            return Err(detail);
        }
        progress.confirm_inflight = false;
        let delivered: Vec<u64> = progress
            .pending_batches
            .iter()
            .filter_map(|(seq_no, batch)| {
                let batch_end = batch.seq_no.saturating_add(batch.frame_count);
                (batch_end <= expected_frames).then_some(*seq_no)
            })
            .collect();
        for seq_no in delivered {
            progress.pending_batches.remove(&seq_no);
        }
        remote_write_session_refresh_acked_frames_locked(&mut progress);
        let should_schedule = remote_write_session_fill_schedule_locked(&mut progress) > 0;
        self.cv.notify_all();
        Ok(should_schedule)
    }

    fn fail(&self, detail: String) {
        let mut progress = self.progress.lock();
        if progress.fatal_error.is_none() {
            progress.fatal_error = Some(detail);
        }
        progress.queued_frames.clear();
        progress.queued_frame_bytes = 0;
        progress.pending_batches.clear();
        progress.early_acked_batches.clear();
        progress.confirm_inflight = false;
        progress.scheduled_sends = 0;
        self.cv.notify_all();
    }
}

fn take_buffered_submit_locked(
    progress: &mut RemoteWriteSessionClientProgress,
    take_len: usize,
) -> Option<RemoteWriteSessionClientSubmit> {
    if progress.buffer.is_empty() {
        progress.buffered_off = None;
        return None;
    }
    let take_len = std::cmp::min(take_len, progress.buffer.len());
    let offset = progress.buffered_off?;
    let payload = progress.buffer.split_to(take_len).freeze();
    let frame_count = remote_write_session_frame_count(payload.len());
    let submit = RemoteWriteSessionClientSubmit {
        base_seq_no: progress.next_seq_no,
        offset,
        data: payload,
        enqueued_at: Instant::now(),
    };
    progress.next_seq_no = progress.next_seq_no.saturating_add(frame_count);
    progress.submitted_frames = progress.submitted_frames.saturating_add(frame_count);
    if progress.buffer.is_empty() {
        progress.buffered_off = None;
    } else {
        progress.buffered_off = Some(offset.saturating_add(take_len as i64));
    }
    Some(submit)
}

async fn remote_write_session_send_batch_task(
    kv_framework: Arc<KvFramework>,
    session: Arc<RemoteWriteSessionClientEntry>,
    batch: RemoteWriteSessionClientSendBatch,
) -> Result<FsWriteSessionDataAck, String> {
    if batch.data_parts.is_empty() {
        return Ok(FsWriteSessionDataAck {
            session_id: session.remote_session_id.clone(),
            seq_no: batch.seq_no,
            frame_count: 0,
            ok: true,
            err_detail: String::new(),
        });
    }
    let rpc_frame = FsWriteSessionDataFrame {
        export: session.export_name.clone(),
        relpath: session.relpath_rpc.clone(),
        session_id: session.remote_session_id.clone(),
        seq_no: batch.seq_no,
        offset: batch.offset,
        fs_rpc_token: session.fs_rpc_token.clone(),
        allow_s3_internal_multipart: session.allow_s3_internal_multipart,
    };
    write_session_rpc::send_write_session_data_bytes(
        kv_framework.as_ref(),
        session.node_id.clone().into(),
        rpc_frame,
        batch.data_parts,
        RpcTransportPolicy::AllowTransferRpcFastPath,
    )
    .await
    .map_err(|e| e.to_string())
}

async fn remote_write_session_confirm_frames_task(
    kv_framework: Arc<KvFramework>,
    session: Arc<RemoteWriteSessionClientEntry>,
    expected_frames: u64,
    timeout: Duration,
) -> Result<(), String> {
    let req = write_session_rpc::FsWaitWriteSessionPayloadsReq {
        export: session.export_name.clone(),
        relpath: session.relpath_rpc.clone(),
        session_id: session.remote_session_id.clone(),
        expected_data_frames: expected_frames,
        fs_rpc_token: session.fs_rpc_token.clone(),
        allow_s3_internal_multipart: session.allow_s3_internal_multipart,
    };
    let resp = write_session_rpc::call_wait_write_session_payloads(
        kv_framework.as_ref(),
        session.node_id.clone().into(),
        req,
        Some(timeout),
    )
    .await
    .map_err(|e| e.to_string())?;
    if resp.ok {
        return Ok(());
    }
    Err(if resp.err_detail.trim().is_empty() {
        format!(
            "remote write-session confirm failed: session_id={} expected_frames={}",
            session.remote_session_id, expected_frames
        )
    } else {
        resp.err_detail
    })
}

fn remote_write_session_schedule_ack_timeout(
    rt_handle: &tokio::runtime::Handle,
    kv_framework: Arc<KvFramework>,
    session: Arc<RemoteWriteSessionClientEntry>,
    seq_no: u64,
) {
    let Some(confirm) = (match session.state.begin_confirm_pending_batch(seq_no) {
        Ok(v) => v,
        Err(_) => None,
    }) else {
        return;
    };
    rt_handle.spawn(async move {
        let timeout =
            Duration::from_millis(remote_write_session_rpc_timeout_ms(confirm.timeout_bytes));
        let result = remote_write_session_confirm_frames_task(
            kv_framework,
            session.clone(),
            confirm.expected_frames,
            timeout,
        )
        .await;
        match result {
            Ok(()) => match session
                .state
                .confirm_delivered_upto(confirm.expected_frames)
            {
                Ok(true) => remote_write_session_schedule_entry(&session),
                Ok(false) | Err(_) => {}
            },
            Err(detail) => session.state.fail(detail),
        }
    });
}

fn fs_agent_io_err(path_for_err: &str, detail: impl Into<String>) -> FsAgentError {
    FsAgentError::Io {
        path: path_for_err.to_string(),
        detail: detail.into(),
    }
}

fn flush_pending_metadata_invalidations_blocking(
    api: Arc<FluxonUserApi>,
    master_cfg: Arc<RwLock<Option<FluxonFsMasterConfig>>>,
    pending: Arc<Mutex<VecDeque<MetadataInvalidationEvent>>>,
) {
    let Some(master_cfg) = master_cfg.read().clone() else {
        return;
    };
    let batch: Vec<MetadataInvalidationEvent> = {
        let mut pending = pending.lock();
        if pending.is_empty() {
            return;
        }
        pending.drain(..).collect()
    };
    let events: Vec<FsMetadataInvalidationEventWire> = batch
        .iter()
        .map(|event| FsMetadataInvalidationEventWire {
            export_name: event.export_name.clone(),
            relpath: event.relpath.clone(),
            scope: match event.scope {
                MetadataInvalidationScope::Exact => FsMetadataInvalidationScopeWire::Exact,
                MetadataInvalidationScope::Prefix => FsMetadataInvalidationScopeWire::Prefix,
            },
            seq: event.seq,
        })
        .collect();
    let payload = FlatDict::from([
        (
            "schema_version".to_string(),
            FlatValue::Int64(FLUXON_FS_CONTROL_SCHEMA_VERSION),
        ),
        (
            "events_json".to_string(),
            FlatValue::String(serde_json::to_string(&events).unwrap_or_else(|_| "[]".to_string())),
        ),
    ]);
    let call_res = api.rpc_client().call(
        &master_cfg.instance_key,
        FS_MASTER_METADATA_INVALIDATION_PUBLISH_RPC_PATH,
        payload,
        None,
    );
    if call_res.is_err() {
        let mut pending = pending.lock();
        for event in batch.into_iter().rev() {
            pending.push_front(event);
        }
    }
}

#[derive(Clone)]
struct AgentRemoteRuntime {
    lifecycle: crate::Framework,
    api: Arc<FluxonUserApi>,
    cfg: Arc<RwLock<Option<FluxonFsGlobalConfig>>>,
    master_cfg: Arc<RwLock<Option<FluxonFsMasterConfig>>>,
    master_pull_interval_ms: Arc<RwLock<Option<u64>>>,
    export_rr: Arc<Mutex<BTreeMap<String, usize>>>,
    export_nodes_cache: Arc<Mutex<ExportNodesCache>>,
}

impl AgentRemoteRuntime {
    fn shutdown_poller_is_running(&self) -> bool {
        ViewShutdownExt::register_shutdown_poller(&self.lifecycle).is_running()
    }

    fn effective_master_pull_interval_ms(&self) -> u64 {
        self.master_pull_interval_ms
            .read()
            .as_ref()
            .copied()
            .unwrap_or(FS_MASTER_BOOTSTRAP_PULL_INTERVAL_MS)
    }

    fn get_export_cfg(&self, export_name: &str) -> Result<FluxonFsExport, String> {
        let cfg_guard = self.cfg.read();
        let Some(cfg) = cfg_guard.as_ref() else {
            return Err("fluxon_fs cache config is not loaded yet".to_string());
        };
        cfg.exports
            .get(export_name)
            .cloned()
            .ok_or_else(|| format!("unknown export_name: {}", export_name))
    }

    fn export_registry_snapshot(
        &self,
        export_name: &str,
        master_cfg: &FluxonFsMasterConfig,
    ) -> Result<Vec<String>, String> {
        let payload: FlatDict = FlatDict::from([
            (
                "schema_version".to_string(),
                FlatValue::Int64(FLUXON_FS_CONTROL_SCHEMA_VERSION),
            ),
            ("op".to_string(), FlatValue::String("snapshot".to_string())),
            (
                "export_name".to_string(),
                FlatValue::String(export_name.to_string()),
            ),
        ]);
        let resp = self
            .api
            .rpc_client()
            .call(
                &master_cfg.instance_key,
                FS_MASTER_EXPORT_REGISTRY_RPC_PATH,
                payload,
                None,
            )
            .map_err(|e| e.to_string())?;
        let ok = match resp.get("ok") {
            Some(FlatValue::Bool(v)) => *v,
            _ => return Err("export registry rpc response missing ok".to_string()),
        };
        if !ok {
            let err = match resp.get("err") {
                Some(FlatValue::String(s)) if !s.trim().is_empty() => s.clone(),
                _ => "export registry rpc returned ok=false".to_string(),
            };
            return Err(err);
        }
        let nodes_json = match resp.get("nodes_json") {
            Some(FlatValue::String(s)) if !s.trim().is_empty() => s.clone(),
            _ => return Err("export registry rpc response missing nodes_json".to_string()),
        };
        serde_json::from_str(&nodes_json)
            .map_err(|e| format!("parse export registry nodes_json failed: {}", e))
    }

    fn export_nodes_get_or_refresh(&self, export_name: &str) -> Result<Vec<String>, String> {
        let master_cfg_guard = self.master_cfg.read();
        let Some(master_cfg) = master_cfg_guard.as_ref() else {
            return Err(
                "fluxon_fs master config is required for export routing_mode=agent_registry"
                    .to_string(),
            );
        };
        let refresh_every = Duration::from_millis(self.effective_master_pull_interval_ms());
        let now = Instant::now();
        {
            let cache = self.export_nodes_cache.lock();
            if let Some((ts, nodes)) = cache.nodes.get(export_name) {
                if now.duration_since(*ts) < refresh_every {
                    return Ok(nodes.clone());
                }
            }
        }
        let nodes = self.export_registry_snapshot(export_name, master_cfg)?;
        if nodes.is_empty() {
            return Err(format!(
                "no online fluxon_fs_agent registered for export={}",
                export_name
            ));
        }
        {
            let mut cache = self.export_nodes_cache.lock();
            cache
                .nodes
                .insert(export_name.to_string(), (now, nodes.clone()));
        }
        Ok(nodes)
    }

    fn remote_next_node(&self, export_name: &str, nodes: &[String]) -> String {
        let mut rr = self.export_rr.lock();
        let idx = rr.get(export_name).copied().unwrap_or(0);
        let node = nodes[idx % nodes.len()].clone();
        rr.insert(export_name.to_string(), idx + 1);
        node
    }
}

impl FluxonFsAgent {
    pub fn new(
        lifecycle: crate::Framework,
        kv_framework: Arc<KvFramework>,
        api: FluxonUserApi,
        rt_handle: tokio::runtime::Handle,
    ) -> Self {
        // Install typed write-session response handlers on every caller-side framework.
        // Without this, external clients can send 7101/7103/7105 but will never consume
        // 7102/7104/7106 replies, which surfaces as request timeouts.
        {
            let _guard = rt_handle.enter();
            crate::write_session_rpc::register_callers(kv_framework.as_ref());
        }
        let api = Arc::new(api);
        let mounts = Arc::new(RwLock::new(Vec::new()));
        let cfg = Arc::new(RwLock::new(None));
        let master_cfg = Arc::new(RwLock::new(None));
        let master_pull_interval_ms = Arc::new(RwLock::new(None));
        let export_rr = Arc::new(Mutex::new(BTreeMap::new()));
        let export_nodes_cache = Arc::new(Mutex::new(ExportNodesCache {
            nodes: BTreeMap::new(),
        }));
        let remote_write_sessions = Arc::new(RwLock::new(HashMap::<
            String,
            Arc<RemoteWriteSessionClientEntry>,
        >::new()));
        let remote_write_sessions_by_remote = Arc::new(RwLock::new(HashMap::<
            String,
            Arc<RemoteWriteSessionClientEntry>,
        >::new()));
        let runtime = AgentRemoteRuntime {
            lifecycle: lifecycle.clone(),
            api: api.clone(),
            cfg: cfg.clone(),
            master_cfg: master_cfg.clone(),
            master_pull_interval_ms: master_pull_interval_ms.clone(),
            export_rr: export_rr.clone(),
            export_nodes_cache: export_nodes_cache.clone(),
        };
        let stage_runtime = runtime.clone();
        let stage_piece_fn: StagePieceFn = Arc::new(move |piece_key, identity| {
            stage_piece_to_kv_via_runtime(&stage_runtime, piece_key, identity)
        });
        let stage_runtime_range = runtime.clone();
        let stage_piece_range_fn: StagePieceRangeFn =
            Arc::new(move |piece_key, piece_count, identity| {
                stage_piece_range_to_kv_via_runtime(
                    &stage_runtime_range,
                    piece_key,
                    piece_count,
                    identity,
                )
            });
        let cache_controller = CacheController::start(
            fluxon_fs_core::config::FluxonFsCacheControllerConfig::default(),
            stage_piece_fn,
            stage_piece_range_fn,
            rt_handle.clone(),
        );
        Self {
            lifecycle,
            kv_framework,
            api,
            rt_handle,
            cfg,
            master_cfg,
            master_pull_interval_ms,
            mounts,
            export_rr,
            export_nodes_cache,
            request_identity: RwLock::new(None),
            access_model_fingerprint: RwLock::new(None),
            remote_metadata_cache: RwLock::new(HashMap::new()),
            remote_write_sessions,
            remote_write_sessions_by_remote,
            remote_write_peer_senders: RwLock::new(HashMap::new()),
            remote_write_session_next_id: AtomicU64::new(1),
            remote_open_cache_resident_bytes: AtomicUsize::new(0),
            remote_open_cache_access_seq: AtomicU64::new(0),
            metadata_invalidation_state: MetadataInvalidationState::default(),
            metadata_invalidation_publish: MetadataInvalidationPublishQueue::default(),
            cache_controller,
            remote_disk_cache: RwLock::new(None),
            refresh_error_first_seen_ms: Mutex::new(BTreeMap::new()),
        }
    }

    fn shutdown_poller_is_running(&self) -> bool {
        ViewShutdownExt::register_shutdown_poller(&self.lifecycle).is_running()
    }

    fn shutdown_aware_sleep(&self, dur: Duration, op: &str) -> Result<(), FsAgentError> {
        if dur.is_zero() {
            return Ok(());
        }
        let step = Duration::from_millis(SHUTDOWN_POLL_SLEEP_STEP_MS);
        let mut left = dur;
        while left > Duration::from_millis(0) {
            if !self.shutdown_poller_is_running() {
                return Err(FsAgentError::Shutdown {
                    detail: format!("stopped by framework shutdown during sleep: op={}", op),
                });
            }
            let chunk = if left > step { step } else { left };
            thread::sleep(chunk);
            left = left.saturating_sub(chunk);
        }
        Ok(())
    }

    fn apply_cache_config_with_exports(
        &self,
        mut cfg: FluxonFsGlobalConfig,
        exports: BTreeMap<String, FluxonFsExport>,
    ) {
        // Prefer more specific rules first.
        cfg.rules
            .sort_by_key(|r| std::cmp::Reverse(r.dir_abs.len()));
        cfg.exports = exports;
        *self.cfg.write() = Some(cfg);
        self.export_nodes_cache.lock().nodes.clear();
        self.export_rr.lock().clear();
    }

    pub fn set_cache_config_yaml(&self, cache_yaml: &str) -> Result<(), FsAgentError> {
        let cfg = crate::config::parse_cache_config_yaml(cache_yaml).map_err(|e| {
            FsAgentError::InvalidArgument {
                detail: e.to_string(),
            }
        })?;
        let exports = cfg.exports.clone();
        self.apply_cache_config_with_exports(cfg, exports);
        Ok(())
    }

    pub fn remote_write_session_target_inflight_bytes(&self) -> u64 {
        self.cfg
            .read()
            .as_ref()
            .map(|cfg| cfg.write_session_target_inflight_bytes)
            .unwrap_or(
                fluxon_fs_core::config::FS_CACHE_DEFAULT_WRITE_SESSION_TARGET_INFLIGHT_BYTES_V1,
            )
    }

    fn ensure_remote_disk_cache(&self) -> Result<Arc<RemoteDiskCacheManager>, String> {
        if let Some(cache) = self.remote_disk_cache.read().as_ref().cloned() {
            return Ok(cache);
        }
        let instance_key = self
            .kv_framework
            .cluster_manager_view()
            .cluster_manager()
            .get_self_info()
            .id
            .to_string();
        let shared_file_path = if self.kv_framework.is_external_mode() {
            self.kv_framework
                .external_client_api_view()
                .external_client_api()
                .inner()
                .shared_file_path()
        } else {
            self.kv_framework
                .client_seg_pool_view()
                .client_seg_pool()
                .shared_file_path()
                .to_string()
        };
        let cache_root = resolve_disk_cache_root(Path::new(&shared_file_path), &instance_key);
        let cache =
            RemoteDiskCacheManager::new(cache_root.clone(), disk_cache_max_bytes_from_env())
                .map_err(|err| {
                    format!(
                        "init remote disk cache failed: root={} err={}",
                        cache_root.display(),
                        err
                    )
                })?;
        let cache = Arc::new(cache);
        let mut guard = self.remote_disk_cache.write();
        Ok(guard.get_or_insert_with(|| cache.clone()).clone())
    }

    fn build_remote_write_session_call_ctx(
        &self,
        export_name: &str,
        relpath: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<RemoteWriteSessionCallCtx, FsAgentError> {
        let (_cfg, export) = self.get_export_cfg(export_name)?;
        let relpath_rpc = normalize_relpath_rpc(relpath);
        let payload = self.request_payload_with_identity(FlatDict::new(), request_identity)?;
        Ok(RemoteWriteSessionCallCtx {
            export,
            relpath_rpc: relpath_rpc.as_ref().to_string(),
            fs_rpc_token: payload
                .get(FLUXON_FS_RPC_TOKEN_PAYLOAD_KEY)
                .and_then(|v| match v {
                    FlatValue::String(s) => Some(s.clone()),
                    _ => None,
                }),
            allow_s3_internal_multipart: is_internal_multipart_relpath(relpath_rpc.as_ref()),
        })
    }

    fn alloc_remote_write_session_client_token(&self) -> String {
        format!(
            "wscli-{:016x}",
            self.remote_write_session_next_id
                .fetch_add(1, Ordering::Relaxed)
        )
    }

    fn remote_write_session_client_lookup(
        &self,
        session_id: &str,
    ) -> Option<Arc<RemoteWriteSessionClientEntry>> {
        self.remote_write_sessions.read().get(session_id).cloned()
    }

    fn remote_write_session_client_remove(
        &self,
        session_id: &str,
    ) -> Option<Arc<RemoteWriteSessionClientEntry>> {
        let removed = self.remote_write_sessions.write().remove(session_id);
        if let Some(entry) = removed.as_ref() {
            let key = remote_write_session_remote_key(&entry.node_id, &entry.remote_session_id);
            self.remote_write_sessions_by_remote.write().remove(&key);
        }
        removed
    }

    fn remote_write_session_client_insert(
        &self,
        session_id: String,
        entry: Arc<RemoteWriteSessionClientEntry>,
    ) {
        self.remote_write_sessions
            .write()
            .insert(session_id, entry.clone());
        let key = remote_write_session_remote_key(&entry.node_id, &entry.remote_session_id);
        self.remote_write_sessions_by_remote
            .write()
            .insert(key, entry);
    }

    fn remote_write_peer_sender_get_or_insert(
        &self,
        node_id: &str,
    ) -> Arc<RemoteWriteSessionPeerSender> {
        if let Some(sender) = self.remote_write_peer_senders.read().get(node_id).cloned() {
            return sender;
        }
        let mut senders = self.remote_write_peer_senders.write();
        if let Some(sender) = senders.get(node_id).cloned() {
            return sender;
        }
        let (ready_tx, ready_rx) =
            tokio_mpsc::unbounded_channel::<Arc<RemoteWriteSessionClientEntry>>();
        let sender = Arc::new(RemoteWriteSessionPeerSender {
            node_id: node_id.to_string(),
            ready_tx,
        });
        let ready_rx = Arc::new(TokioMutex::new(ready_rx));
        let kv_framework = self.kv_framework.clone();
        for _ in 0..remote_write_session_peer_sender_workers() {
            let sender_for_task = sender.clone();
            let ready_rx_for_task = ready_rx.clone();
            let kv_framework_for_task = kv_framework.clone();
            self.rt_handle.spawn(async move {
                loop {
                    let Some(session) = ({
                        let mut rx = ready_rx_for_task.lock().await;
                        rx.recv().await
                    }) else {
                        break;
                    };
                    let batch = match session.state.take_next_batch_for_send() {
                        Ok(Some(batch)) => batch,
                        Ok(None) => continue,
                        Err(_) => continue,
                    };
                    let batch_for_state = batch.clone();
                    let result = remote_write_session_send_batch_task(
                        kv_framework_for_task.clone(),
                        session.clone(),
                        batch,
                    )
                    .await;
                    let (followup, ack_should_schedule) = match result {
                        Ok(ack) => {
                            if ack.session_id != session.remote_session_id
                                || ack.seq_no != batch_for_state.seq_no
                                || ack.frame_count != batch_for_state.frame_count
                            {
                                let detail = format!(
                                    "remote write-session data ack mismatch: session_id={} got_session_id={} seq_no={} got_seq_no={} expected_frames={} got_frames={}",
                                    session.remote_session_id,
                                    ack.session_id,
                                    batch_for_state.seq_no,
                                    ack.seq_no,
                                    batch_for_state.frame_count,
                                    ack.frame_count,
                                );
                                (
                                    session
                                        .state
                                        .finish_batch_send(&batch_for_state, Err(detail)),
                                    false,
                                )
                            } else {
                                let followup =
                                    session.state.finish_batch_send(&batch_for_state, Ok(()));
                                let ack_should_schedule =
                                    session.state.handle_data_ack(&ack).unwrap_or(false);
                                (followup, ack_should_schedule)
                            }
                        }
                        Err(detail) => (
                            session
                                .state
                                .finish_batch_send(&batch_for_state, Err(detail)),
                            false,
                        ),
                    };
                    if ack_should_schedule {
                        let _ = sender_for_task.ready_tx.send(session.clone());
                    }
                    for _ in 0..followup.schedule_count {
                        let _ = sender_for_task.ready_tx.send(session.clone());
                    }
                }
            });
        }
        senders.insert(node_id.to_string(), sender.clone());
        sender
    }

    fn remote_write_session_schedule(
        &self,
        session: &Arc<RemoteWriteSessionClientEntry>,
        path_for_err: &str,
    ) -> Result<(), FsAgentError> {
        if session.peer_sender.ready_tx.send(session.clone()).is_ok() {
            return Ok(());
        }
        let detail = format!(
            "remote write-session peer sender closed unexpectedly: session_id={} node_id={}",
            session.remote_session_id, session.node_id
        );
        session.state.fail(detail.clone());
        Err(FsAgentError::Io {
            path: path_for_err.to_string(),
            detail,
        })
    }

    pub fn upsert_export_cfg(
        &self,
        export_name: String,
        export: FluxonFsExport,
    ) -> Result<(), FsAgentError> {
        if export_name.trim().is_empty() {
            return Err(FsAgentError::InvalidArgument {
                detail: "export_name must be non-empty".to_string(),
            });
        }
        let mut cfg_guard = self.cfg.write();
        let Some(cfg) = cfg_guard.as_mut() else {
            return Err(FsAgentError::InvalidArgument {
                detail: "fluxon_fs cache config is not loaded yet".to_string(),
            });
        };
        cfg.exports.insert(export_name, export);
        self.export_nodes_cache.lock().nodes.clear();
        self.export_rr.lock().clear();
        Ok(())
    }

    pub fn set_master_config_from_file(&self, config_path: &str) -> Result<(), FsAgentError> {
        if config_path.trim().is_empty() {
            return Err(FsAgentError::InvalidArgument {
                detail: "config_path must be non-empty".to_string(),
            });
        }
        let master_cfg = parse_master_config_from_file(config_path).map_err(|e| {
            FsAgentError::InvalidArgument {
                detail: format!("{}", e),
            }
        })?;
        *self.master_cfg.write() = Some(master_cfg);
        *self.master_pull_interval_ms.write() = None;
        Ok(())
    }

    pub fn set_master_config(&self, master_cfg: FluxonFsMasterConfig) {
        *self.master_cfg.write() = Some(master_cfg);
        *self.master_pull_interval_ms.write() = None;
    }

    fn update_master_pull_interval_ms(&self, pull_interval_ms: u64) {
        *self.master_pull_interval_ms.write() = Some(pull_interval_ms);
    }

    fn clear_remote_open_cache_all(&self) {
        let mut cache = self.remote_metadata_cache.write();
        let removed_bytes: usize = cache
            .values()
            .filter_map(|entry| entry.inline_fd.as_ref().map(|fd| fd.size_bytes))
            .sum();
        cache.clear();
        if removed_bytes > 0 {
            self.remote_open_cache_resident_bytes
                .fetch_sub(removed_bytes, Ordering::Relaxed);
        }
        self.remote_open_cache_resident_bytes
            .store(0, Ordering::Relaxed);
    }

    fn update_access_model_fingerprint(&self, access_model_json: Option<&str>) {
        let next = access_model_json.map(|text| {
            let mut hasher = Sha256::new();
            hasher.update(text.as_bytes());
            hex::encode(hasher.finalize())
        });
        let mut guard = self.access_model_fingerprint.write();
        if guard.as_ref() != next.as_ref() {
            *guard = next;
            self.clear_remote_open_cache_all();
        }
    }

    fn effective_master_pull_interval_ms(&self) -> u64 {
        self.master_pull_interval_ms
            .read()
            .as_ref()
            .copied()
            .unwrap_or(FS_MASTER_BOOTSTRAP_PULL_INTERVAL_MS)
    }

    pub fn set_request_identity(&self, username: &str, password: &str) -> Result<(), FsAgentError> {
        if username.trim().is_empty() {
            return Err(FsAgentError::InvalidArgument {
                detail: "request_identity.username must be non-empty".to_string(),
            });
        }
        if password.is_empty() {
            return Err(FsAgentError::InvalidArgument {
                detail: "request_identity.password must be non-empty".to_string(),
            });
        }
        let identity = FluxonFsRequestIdentity {
            username: username.to_string(),
            password: password.to_string(),
        };
        let fingerprint = Self::identity_fingerprint(&identity);
        *self.request_identity.write() = Some(CurrentRequestIdentity {
            identity,
            fingerprint,
        });
        Ok(())
    }

    pub fn clear_request_identity(&self) {
        *self.request_identity.write() = None;
    }

    pub fn is_cache_config_loaded(&self) -> bool {
        self.cfg.read().is_some()
    }

    pub fn wait_cache_config_loaded(&self) {
        while !self.is_cache_config_loaded() {
            if !self.shutdown_poller_is_running() {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn parse_mount_exports_json_text(
        &self,
        mount_exports_json: &str,
    ) -> Result<BTreeMap<String, FluxonFsExport>, FsAgentError> {
        let mount_exports: BTreeMap<String, FluxonFsExport> =
            serde_json::from_str(mount_exports_json).map_err(|e| {
                FsAgentError::InvalidArgument {
                    detail: format!("parse mount_exports_json failed: {}", e),
                }
            })?;
        for (export_name, export) in &mount_exports {
            if export_name.trim().is_empty() || export_name.contains('/') {
                return Err(FsAgentError::InvalidArgument {
                    detail: format!("invalid export_name in mount export view: {}", export_name),
                });
            }
            if export.remote_root_dir_abs.trim().is_empty()
                || !Path::new(&export.remote_root_dir_abs).is_absolute()
            {
                return Err(FsAgentError::InvalidArgument {
                    detail: format!(
                        "invalid remote_root_dir_abs in mount export view: export={} remote_root_dir_abs={}",
                        export_name, export.remote_root_dir_abs
                    ),
                });
            }
        }
        Ok(mount_exports)
    }

    fn fetch_master_config_snapshot_once(
        &self,
        master_cfg: &FluxonFsMasterConfig,
    ) -> Result<
        (
            String,
            String,
            u64,
            Option<String>,
            Option<FsMetadataInvalidationStateWire>,
        ),
        FsAgentError,
    > {
        let payload: FlatDict = FlatDict::from([(
            "schema_version".to_string(),
            FlatValue::Int64(FLUXON_FS_CONTROL_SCHEMA_VERSION),
        )]);
        let resp = self
            .api
            .rpc_client()
            .call(
                &master_cfg.instance_key,
                FS_MASTER_CONFIG_RPC_PATH,
                payload,
                None,
            )
            .map_err(FsAgentError::Kv)?;

        let got_schema = match resp.get("schema_version") {
            Some(FlatValue::Int64(s)) => *s,
            _ => {
                return Err(FsAgentError::InvalidArgument {
                    detail: "master config snapshot missing schema_version".to_string(),
                });
            }
        };
        if got_schema != FLUXON_FS_CONTROL_SCHEMA_VERSION {
            return Err(FsAgentError::InvalidArgument {
                detail: format!(
                    "master config snapshot schema_version mismatch: expected={} got={}",
                    FLUXON_FS_CONTROL_SCHEMA_VERSION, got_schema
                ),
            });
        }
        let cfg_text = match resp.get("config_yaml") {
            Some(FlatValue::String(t)) if !t.trim().is_empty() => t.clone(),
            _ => {
                return Err(FsAgentError::InvalidArgument {
                    detail: "master config snapshot missing config_yaml".to_string(),
                });
            }
        };
        let mount_exports_json = match resp.get(FLUXON_FS_MOUNT_EXPORTS_JSON_KEY) {
            Some(FlatValue::String(t)) => t.clone(),
            _ => {
                return Err(FsAgentError::InvalidArgument {
                    detail: format!(
                        "master config snapshot missing {}",
                        FLUXON_FS_MOUNT_EXPORTS_JSON_KEY
                    ),
                });
            }
        };
        let pull_interval_ms = match resp.get("pull_interval_ms") {
            Some(FlatValue::Int64(v)) if *v > 0 => *v as u64,
            _ => {
                return Err(FsAgentError::InvalidArgument {
                    detail: "master config snapshot missing pull_interval_ms".to_string(),
                });
            }
        };
        let invalidation_state = match resp.get(FLUXON_FS_METADATA_INVALIDATION_STATE_JSON_KEY) {
            Some(FlatValue::String(text)) if !text.trim().is_empty() => Some(
                serde_json::from_str::<FsMetadataInvalidationStateWire>(text).map_err(|e| {
                    FsAgentError::InvalidArgument {
                        detail: format!("invalid metadata invalidation state: {}", e),
                    }
                })?,
            ),
            Some(FlatValue::String(_)) | None => None,
            _ => {
                return Err(FsAgentError::InvalidArgument {
                    detail: format!(
                        "master config snapshot invalid {}",
                        FLUXON_FS_METADATA_INVALIDATION_STATE_JSON_KEY
                    ),
                });
            }
        };
        let access_model_json =
            match resp.get(fluxon_fs_core::config::FLUXON_FS_CONFIG_ACCESS_MODEL_JSON_KEY) {
                Some(FlatValue::String(text)) if !text.trim().is_empty() => Some(text.clone()),
                Some(FlatValue::String(_)) | None => None,
                _ => {
                    return Err(FsAgentError::InvalidArgument {
                        detail: format!(
                            "master config snapshot invalid {}",
                            fluxon_fs_core::config::FLUXON_FS_CONFIG_ACCESS_MODEL_JSON_KEY
                        ),
                    });
                }
            };
        Ok((
            cfg_text,
            mount_exports_json,
            pull_interval_ms,
            access_model_json,
            invalidation_state,
        ))
    }

    fn apply_master_config_snapshot(
        &self,
        cfg_text: &str,
        mount_exports_json: &str,
        access_model_json: Option<&str>,
        invalidation_state: Option<&FsMetadataInvalidationStateWire>,
    ) -> Result<(), FsAgentError> {
        let cfg = crate::config::parse_cache_config_yaml(cfg_text).map_err(|e| {
            FsAgentError::InvalidArgument {
                detail: e.to_string(),
            }
        })?;
        let mount_exports = self.parse_mount_exports_json_text(mount_exports_json)?;
        self.apply_cache_config_with_exports(cfg, mount_exports);
        self.update_access_model_fingerprint(access_model_json);
        if let Some(state) = invalidation_state {
            self.apply_metadata_invalidation_state(state);
        }
        Ok(())
    }

    pub fn load_cache_config_from_master_config_file(
        &self,
        config_path: &str,
    ) -> Result<(), FsAgentError> {
        let master_cfg = parse_master_config_from_file(config_path).map_err(|e| {
            FsAgentError::InvalidArgument {
                detail: format!("{}", e),
            }
        })?;
        *self.master_cfg.write() = Some(master_cfg.clone());
        *self.master_pull_interval_ms.write() = None;

        loop {
            if !self.shutdown_poller_is_running() {
                return Err(FsAgentError::Shutdown {
                    detail: format!(
                        "stopped by framework shutdown: master={}",
                        master_cfg.instance_key
                    ),
                });
            }

            let (
                cfg_text,
                mount_exports_json,
                pull_interval_ms,
                access_model_json,
                invalidation_state,
            ) = match self.fetch_master_config_snapshot_once(&master_cfg) {
                Ok(v) => v,
                Err(e) => {
                    let retry_interval_ms = self.effective_master_pull_interval_ms();
                    tracing::warn!(
                        "fluxon_fs config fetch failed: master={} interval_ms={} err={}",
                        master_cfg.instance_key,
                        retry_interval_ms,
                        e
                    );
                    self.shutdown_aware_sleep(
                        Duration::from_millis(retry_interval_ms),
                        "load_cache_config_from_master_config_file.fetch_snapshot_failed",
                    )?;
                    continue;
                }
            };
            self.update_master_pull_interval_ms(pull_interval_ms);

            match self.apply_master_config_snapshot(
                &cfg_text,
                &mount_exports_json,
                access_model_json.as_deref(),
                invalidation_state.as_ref(),
            ) {
                Ok(()) => {
                    tracing::info!(
                        "fluxon_fs config loaded: master={} schema_version={} pull_interval_ms={}",
                        master_cfg.instance_key,
                        FLUXON_FS_CONTROL_SCHEMA_VERSION,
                        pull_interval_ms
                    );
                    return Ok(());
                }
                Err(e) => {
                    let retry_interval_ms = self.effective_master_pull_interval_ms();
                    tracing::warn!(
                        "fluxon_fs apply config failed; blocking retry: master={} interval_ms={} err={}",
                        master_cfg.instance_key,
                        retry_interval_ms,
                        e
                    );
                    self.shutdown_aware_sleep(
                        Duration::from_millis(retry_interval_ms),
                        "load_cache_config_from_master_config_file.apply_config_failed",
                    )?;
                    continue;
                }
            }
        }
    }

    pub fn run_cache_config_sync_from_master_config_file_forever(
        &self,
        config_path: &str,
    ) -> Result<(), FsAgentError> {
        let master_cfg = parse_master_config_from_file(config_path).map_err(|e| {
            FsAgentError::InvalidArgument {
                detail: format!("{}", e),
            }
        })?;
        *self.master_cfg.write() = Some(master_cfg.clone());
        *self.master_pull_interval_ms.write() = None;

        let mut loaded_once = false;
        loop {
            if !self.shutdown_poller_is_running() {
                return Ok(());
            }
            match self.fetch_master_config_snapshot_once(&master_cfg) {
                Ok((
                    cfg_text,
                    mount_exports_json,
                    pull_interval_ms,
                    access_model_json,
                    invalidation_state,
                )) => {
                    self.update_master_pull_interval_ms(pull_interval_ms);
                    match self.apply_master_config_snapshot(
                        &cfg_text,
                        &mount_exports_json,
                        access_model_json.as_deref(),
                        invalidation_state.as_ref(),
                    ) {
                        Ok(()) => {
                            if !loaded_once {
                                tracing::info!(
                                    "fluxon_fs config sync started: master={} schema_version={} pull_interval_ms={}",
                                    master_cfg.instance_key,
                                    FLUXON_FS_CONTROL_SCHEMA_VERSION,
                                    pull_interval_ms
                                );
                                loaded_once = true;
                            }
                        }
                        Err(e) => {
                            let retry_interval_ms = self.effective_master_pull_interval_ms();
                            tracing::warn!(
                                "fluxon_fs apply synced config failed: master={} interval_ms={} err={}",
                                master_cfg.instance_key,
                                retry_interval_ms,
                                e
                            );
                        }
                    }
                }
                Err(e) => {
                    let retry_interval_ms = self.effective_master_pull_interval_ms();
                    tracing::warn!(
                        "fluxon_fs synced config fetch failed: master={} interval_ms={} err={}",
                        master_cfg.instance_key,
                        retry_interval_ms,
                        e
                    );
                }
            }
            self.shutdown_aware_sleep(
                Duration::from_millis(self.effective_master_pull_interval_ms()),
                "run_cache_config_sync_from_master_config_file_forever.sleep",
            )?;
        }
    }

    pub fn mount_remote_dir(
        &self,
        local_mount_dir_abs: &str,
        export_name: &str,
    ) -> Result<(), FsAgentError> {
        let prepared = self.mount_remote_dir_prepare(local_mount_dir_abs, export_name)?;

        // English note: mount registry is a control-plane feature. If the agent was bootstrapped
        // from an fs master config file, we have the master_cfg and will report this mount to fs master.
        // This keeps the fs master panel accurate without forcing unit tests (that only call
        // set_cache_config_yaml) to spin up a master.
        if let Some(master_cfg) = prepared.master_cfg.clone() {
            let payload: FlatDict = FlatDict::from([
                (
                    "schema_version".to_string(),
                    FlatValue::Int64(FLUXON_FS_CONTROL_SCHEMA_VERSION),
                ),
                (
                    "local_mount_dir_abs".to_string(),
                    FlatValue::String(prepared.mdir.clone()),
                ),
                (
                    "remote_root_dir_abs".to_string(),
                    FlatValue::String(prepared.remote_root_dir_abs.clone()),
                ),
            ]);
            let resp_res = self.api.rpc_client().call(
                &master_cfg.instance_key,
                fluxon_fs_core::config::FS_MASTER_MOUNT_REGISTRY_RPC_PATH,
                payload,
                None,
            );

            let err_detail: Option<FsAgentError> = Self::mount_registry_resp_to_err(resp_res);

            if let Some(e) = err_detail {
                self.mount_remote_dir_rollback(&prepared.mdir, export_name);
                return Err(e);
            }
        }
        Ok(())
    }

    pub async fn mount_remote_dir_async(
        &self,
        local_mount_dir_abs: &str,
        export_name: &str,
    ) -> Result<(), FsAgentError> {
        let prepared = self.mount_remote_dir_prepare(local_mount_dir_abs, export_name)?;

        if let Some(master_cfg) = prepared.master_cfg.clone() {
            let payload: FlatDict = FlatDict::from([
                (
                    "schema_version".to_string(),
                    FlatValue::Int64(FLUXON_FS_CONTROL_SCHEMA_VERSION),
                ),
                (
                    "local_mount_dir_abs".to_string(),
                    FlatValue::String(prepared.mdir.clone()),
                ),
                (
                    "remote_root_dir_abs".to_string(),
                    FlatValue::String(prepared.remote_root_dir_abs.clone()),
                ),
            ]);

            let payload_bytes = encode_flat_dict_bytes(&payload).map_err(FsAgentError::Kv)?;
            let node: NodeID = master_cfg.instance_key.clone().into();
            let resp_bytes = user_rpc_call(
                self.kv_framework.as_ref(),
                node,
                fluxon_fs_core::config::FS_MASTER_MOUNT_REGISTRY_RPC_PATH.to_string(),
                payload_bytes,
                USER_RPC_DEFAULT_TIMEOUT_MS,
            )
            .await
            .map_err(FsAgentError::Kv)?;
            let resp = decode_flat_dict_bytes(&resp_bytes).map_err(FsAgentError::Kv)?;

            let err_detail: Option<FsAgentError> = Self::mount_registry_resp_to_err(Ok(resp));
            if let Some(e) = err_detail {
                self.mount_remote_dir_rollback(&prepared.mdir, export_name);
                return Err(e);
            }
        }
        Ok(())
    }

    fn mount_remote_dir_prepare(
        &self,
        local_mount_dir_abs: &str,
        export_name: &str,
    ) -> Result<MountRemoteDirPrepared, FsAgentError> {
        if local_mount_dir_abs.trim().is_empty() {
            return Err(FsAgentError::InvalidArgument {
                detail: "local_mount_dir_abs must be non-empty".to_string(),
            });
        }
        if export_name.trim().is_empty() {
            return Err(FsAgentError::InvalidArgument {
                detail: "export_name must be non-empty".to_string(),
            });
        }
        if !Path::new(local_mount_dir_abs).is_absolute() {
            return Err(FsAgentError::InvalidArgument {
                detail: "local_mount_dir_abs must be absolute".to_string(),
            });
        }

        let cfg_guard = self.cfg.read();
        let Some(cfg) = cfg_guard.as_ref() else {
            return Err(FsAgentError::InvalidArgument {
                detail: "fluxon_fs cache config is not loaded yet".to_string(),
            });
        };
        if !cfg.exports.contains_key(export_name) {
            return Err(FsAgentError::InvalidArgument {
                detail: format!("unknown export_name: {}", export_name),
            });
        }
        let remote_root_dir_abs = cfg
            .exports
            .get(export_name)
            .unwrap()
            .remote_root_dir_abs
            .clone();

        let mdir = strip_trailing_slash(local_mount_dir_abs);
        if mdir == "/" {
            return Err(FsAgentError::InvalidArgument {
                detail: "local_mount_dir_abs cannot be '/'".to_string(),
            });
        }

        // English note: do not require mountpoint absence.
        //
        // Causal chain:
        // - Real deployments often pre-create directories (image layer, config management, humans).
        // - Rejecting an existing empty directory forces environment mutation (e.g. manual rm/mkdir),
        //   which is brittle and violates the "software adapts to environment" expectation.
        // - Therefore we accept an existing empty directory, and only fail if it is non-empty (to
        //   avoid silently shadowing local files).
        let mdir_p = Path::new(&mdir);
        match fs::metadata(mdir_p) {
            Ok(meta) => {
                if !meta.is_dir() {
                    return Err(FsAgentError::InvalidArgument {
                        detail: format!("local_mount_dir_abs is not a directory: {}", mdir),
                    });
                }
                let mut it = fs::read_dir(mdir_p).map_err(|e| {
                    FsAgentError::os(
                        e.raw_os_error().unwrap_or(libc::EIO),
                        mdir.clone(),
                        e.to_string(),
                    )
                })?;
                if it.next().is_some() {
                    return Err(FsAgentError::InvalidArgument {
                        detail: format!(
                            "local_mount_dir_abs must be an empty directory: {} (refuse to shadow existing files)",
                            mdir
                        ),
                    });
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir_all(mdir_p).map_err(|e| {
                    FsAgentError::os(
                        e.raw_os_error().unwrap_or(libc::EIO),
                        mdir.clone(),
                        e.to_string(),
                    )
                })?;
            }
            Err(e) => {
                return Err(FsAgentError::os(
                    e.raw_os_error().unwrap_or(libc::EIO),
                    mdir.clone(),
                    e.to_string(),
                ));
            }
        }

        {
            let mut mounts = self.mounts.write();
            for m in mounts.iter() {
                if is_within_dir(&mdir, &m.mount_dir_abs) || is_within_dir(&m.mount_dir_abs, &mdir)
                {
                    return Err(FsAgentError::InvalidArgument {
                        detail: format!("mount overlap: new={} existing={}", mdir, m.mount_dir_abs),
                    });
                }
            }
            mounts.push(Mount {
                mount_dir_abs: mdir.clone(),
                export_name: export_name.to_string(),
            });
            mounts.sort_by_key(|m| std::cmp::Reverse(m.mount_dir_abs.len()));
        }

        Ok(MountRemoteDirPrepared {
            mdir,
            remote_root_dir_abs,
            master_cfg: self.master_cfg.read().clone(),
        })
    }

    fn mount_remote_dir_rollback(&self, mdir: &str, export_name: &str) {
        // English note: keep mount state consistent with the control-plane registry.
        let mut mounts = self.mounts.write();
        if let Some(pos) = mounts
            .iter()
            .position(|m| m.mount_dir_abs == mdir && m.export_name == export_name)
        {
            mounts.remove(pos);
        }
    }

    fn mount_registry_resp_to_err(resp_res: KvResult<FlatDict>) -> Option<FsAgentError> {
        match resp_res {
            Ok(resp) => match resp.get("ok") {
                Some(FlatValue::Bool(true)) => None,
                Some(FlatValue::Bool(false)) => {
                    let err = match resp.get("err") {
                        Some(FlatValue::String(s)) if !s.trim().is_empty() => s.clone(),
                        _ => "mount registry rpc returned ok=false".to_string(),
                    };
                    Some(FsAgentError::InvalidArgument { detail: err })
                }
                _ => Some(FsAgentError::InvalidArgument {
                    detail: "mount registry rpc response missing 'ok'".to_string(),
                }),
            },
            Err(e) => Some(FsAgentError::Kv(e)),
        }
    }

    fn export_registry_snapshot(
        &self,
        export_name: &str,
        master_cfg: &FluxonFsMasterConfig,
    ) -> KvResult<Vec<String>> {
        if export_name.trim().is_empty() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "export_name must be non-empty".to_string(),
            }));
        }

        let payload: FlatDict = FlatDict::from([
            (
                "schema_version".to_string(),
                FlatValue::Int64(FLUXON_FS_CONTROL_SCHEMA_VERSION),
            ),
            ("op".to_string(), FlatValue::String("snapshot".to_string())),
            (
                "export_name".to_string(),
                FlatValue::String(export_name.to_string()),
            ),
        ]);
        let resp = self.api.rpc_client().call(
            &master_cfg.instance_key,
            fluxon_fs_core::config::FS_MASTER_EXPORT_REGISTRY_RPC_PATH,
            payload,
            None,
        )?;

        let ok = match resp.get("ok") {
            Some(FlatValue::Bool(v)) => *v,
            _ => {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: "export registry rpc response missing ok".to_string(),
                }));
            }
        };
        if !ok {
            let err = match resp.get("err") {
                Some(FlatValue::String(s)) if !s.trim().is_empty() => s.clone(),
                _ => "export registry rpc returned ok=false".to_string(),
            };
            return Err(KvError::Api(ApiError::InvalidArgument { detail: err }));
        }

        let nodes_json = match resp.get("nodes_json") {
            Some(FlatValue::String(s)) if !s.trim().is_empty() => s.clone(),
            _ => {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: "export registry rpc response missing nodes_json".to_string(),
                }));
            }
        };
        let nodes: Vec<String> = serde_json::from_str(&nodes_json).map_err(|e| {
            KvError::Api(ApiError::Unknown {
                detail: format!("parse export registry nodes_json failed: {}", e),
            })
        })?;
        Ok(nodes)
    }

    fn export_nodes_get_or_refresh(&self, export_name: &str) -> KvResult<Vec<String>> {
        let master_cfg_guard = self.master_cfg.read();
        let Some(master_cfg) = master_cfg_guard.as_ref() else {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail:
                    "fluxon_fs master config is required for export routing_mode=agent_registry"
                        .to_string(),
            }));
        };

        // English note: registry refresh follows the current master-delivered control-plane interval.
        let refresh_every = Duration::from_millis(self.effective_master_pull_interval_ms());
        let now = Instant::now();

        {
            let cache = self.export_nodes_cache.lock();
            if let Some((ts, nodes)) = cache.nodes.get(export_name) {
                if now.duration_since(*ts) < refresh_every {
                    return Ok(nodes.clone());
                }
            }
        }

        let nodes = self.export_registry_snapshot(export_name, master_cfg)?;
        if nodes.is_empty() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "no online fluxon_fs_agent registered for export={}",
                    export_name
                ),
            }));
        }
        {
            let mut cache = self.export_nodes_cache.lock();
            cache
                .nodes
                .insert(export_name.to_string(), (now, nodes.clone()));
        }
        Ok(nodes)
    }

    pub fn open_plan(&self, file_abs: &str, mode: &str) -> Result<OpenPlan, FsAgentError> {
        if !Path::new(file_abs).is_absolute() {
            return Err(FsAgentError::InvalidArgument {
                detail: format!("file_abs must be absolute: {}", file_abs),
            });
        }
        if mode.trim().is_empty() {
            return Err(FsAgentError::InvalidArgument {
                detail: "mode must be non-empty".to_string(),
            });
        }

        if let Some((mount_dir_abs, export_name, relpath)) = self.match_mount(file_abs) {
            return self.open_plan_remote(file_abs, mode, &mount_dir_abs, &export_name, &relpath);
        }

        self.open_plan_local(file_abs, mode)
    }

    pub fn local_write_through_on_close(
        &self,
        file_abs: &str,
        mode: &str,
    ) -> Result<(), FsAgentError> {
        if !is_write_mode(mode) {
            return Ok(());
        }

        let cfg_guard = self.cfg.read();
        let Some(cfg) = cfg_guard.as_ref() else {
            return Ok(());
        };
        let Some(rule) = match_rule(file_abs, &cfg.rules) else {
            return Ok(());
        };
        if rule.cache_mode == CacheMode::Disabled {
            return Ok(());
        }
        if rule.write_mode != WriteMode::WriteThrough {
            return Ok(());
        }
        if !Path::new(file_abs).exists() {
            return Ok(());
        }

        let md = fs::metadata(file_abs).map_err(|e| FsAgentError::Io {
            path: file_abs.to_string(),
            detail: e.to_string(),
        })?;
        let size = md.len();
        if size > rule.max_cache_bytes {
            return Ok(());
        }
        let sig = fs_sig_from_metadata(&md);
        let Some(kv_key) = kv_key_for_file(file_abs, rule) else {
            return Ok(());
        };
        let bytes = read_file_all(file_abs, size as usize)?;
        let ok = self.cache_try_put(&kv_key, &rule.bytes_field_key, &bytes, sig);
        if ok {
            self.refresh_error_first_seen_ms.lock().remove(file_abs);
        }
        Ok(())
    }

    pub fn remote_stat_by_handle(
        &self,
        export_name: &str,
        relpath: &str,
        path_for_err: &str,
    ) -> Result<RemoteStat, FsAgentError> {
        self.remote_stat_by_handle_with_identity(export_name, relpath, path_for_err, None)
    }

    pub fn remote_stat_by_handle_with_identity(
        &self,
        export_name: &str,
        relpath: &str,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<RemoteStat, FsAgentError> {
        let (_cfg, export) = self.get_export_cfg(export_name)?;
        let relpath_rpc = normalize_relpath_rpc(relpath);
        self.remote_stat_with_internal_multipart(
            export_name,
            &export,
            relpath_rpc.as_ref(),
            path_for_err,
            request_identity,
            false,
        )
    }

    pub(crate) fn remote_stat_by_handle_s3_gateway_with_identity(
        &self,
        export_name: &str,
        relpath: &str,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<RemoteStat, FsAgentError> {
        let (_cfg, export) = self.get_export_cfg(export_name)?;
        let relpath_rpc = normalize_relpath_rpc(relpath);
        self.remote_stat_with_internal_multipart(
            export_name,
            &export,
            relpath_rpc.as_ref(),
            path_for_err,
            request_identity,
            true,
        )
    }

    pub(crate) fn remote_stat_via_exporter_s3_gateway_with_identity(
        &self,
        exporter_id: &str,
        export_name: &str,
        relpath: &str,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<RemoteStat, FsAgentError> {
        let (_cfg, export) = self.get_export_cfg(export_name)?;
        let relpath_rpc = normalize_relpath_rpc(relpath);
        let payload: FlatDict = FlatDict::from([
            (
                "export".to_string(),
                FlatValue::String(export_name.to_string()),
            ),
            (
                "relpath".to_string(),
                FlatValue::String(relpath_rpc.as_ref().to_string()),
            ),
        ]);
        let payload = self.request_payload_with_identity_and_internal_multipart(
            payload,
            request_identity,
            is_internal_multipart_relpath(relpath_rpc.as_ref()),
        )?;
        let resp = self.remote_rpc_call_forever_on_node_with_timeout(
            exporter_id,
            &export.rpc_paths.stat,
            &payload,
            "stat",
            None,
        )?;
        let ok = match resp.get("ok") {
            Some(FlatValue::Bool(v)) => *v,
            _ => {
                return Err(FsAgentError::InvalidArgument {
                    detail: "remote stat response missing ok".to_string(),
                });
            }
        };
        if !ok {
            return Err(err_from_resp(&resp, path_for_err));
        }
        Ok(RemoteStat {
            exists: matches!(resp.get("exists"), Some(FlatValue::Bool(true))),
            is_file: matches!(resp.get("is_file"), Some(FlatValue::Bool(true))),
            is_dir: matches!(resp.get("is_dir"), Some(FlatValue::Bool(true))),
            size: get_i64(&resp, "size").unwrap_or(0),
            mtime_ns: get_i64(&resp, "mtime_ns").unwrap_or(0),
            mode: get_i64(&resp, "mode").unwrap_or(0),
        })
    }

    pub fn remote_read_chunk_by_handle(
        &self,
        export_name: &str,
        relpath: &str,
        offset: i64,
        n: i64,
        file_size: i64,
        mtime_ns: i64,
        allow_kv_cache: bool,
        path_for_err: &str,
    ) -> Result<Vec<u8>, FsAgentError> {
        self.remote_read_chunk_by_handle_with_identity(
            export_name,
            relpath,
            offset,
            n,
            file_size,
            mtime_ns,
            allow_kv_cache,
            path_for_err,
            None,
        )
    }

    pub fn remote_read_chunk_by_handle_with_identity(
        &self,
        export_name: &str,
        relpath: &str,
        offset: i64,
        n: i64,
        file_size: i64,
        mtime_ns: i64,
        allow_kv_cache: bool,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<Vec<u8>, FsAgentError> {
        // English note: keep legacy behavior for non-S3 callers.
        self.remote_read_chunk_by_handle_with_s3_policy(
            export_name,
            relpath,
            offset,
            n,
            file_size,
            mtime_ns,
            allow_kv_cache,
            FluxonFsS3KvMissPolicy::StageToKvThenRead,
            path_for_err,
            request_identity,
            false,
        )
    }

    pub fn remote_read_chunk_by_handle_s3(
        &self,
        export_name: &str,
        relpath: &str,
        offset: i64,
        n: i64,
        file_size: i64,
        mtime_ns: i64,
        allow_kv_cache: bool,
        kv_miss_policy: FluxonFsS3KvMissPolicy,
        path_for_err: &str,
    ) -> Result<Vec<u8>, FsAgentError> {
        self.remote_read_chunk_by_handle_s3_with_identity(
            export_name,
            relpath,
            offset,
            n,
            file_size,
            mtime_ns,
            allow_kv_cache,
            kv_miss_policy,
            path_for_err,
            None,
        )
    }

    pub fn remote_read_chunk_by_handle_s3_with_identity(
        &self,
        export_name: &str,
        relpath: &str,
        offset: i64,
        n: i64,
        file_size: i64,
        mtime_ns: i64,
        allow_kv_cache: bool,
        kv_miss_policy: FluxonFsS3KvMissPolicy,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<Vec<u8>, FsAgentError> {
        self.remote_read_chunk_by_handle_with_s3_policy(
            export_name,
            relpath,
            offset,
            n,
            file_size,
            mtime_ns,
            allow_kv_cache,
            kv_miss_policy,
            path_for_err,
            request_identity,
            false,
        )
    }

    pub(crate) fn remote_read_chunk_by_handle_s3_gateway_with_identity(
        &self,
        export_name: &str,
        relpath: &str,
        offset: i64,
        n: i64,
        file_size: i64,
        mtime_ns: i64,
        allow_kv_cache: bool,
        kv_miss_policy: FluxonFsS3KvMissPolicy,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<Vec<u8>, FsAgentError> {
        self.remote_read_chunk_by_handle_with_s3_policy(
            export_name,
            relpath,
            offset,
            n,
            file_size,
            mtime_ns,
            allow_kv_cache,
            kv_miss_policy,
            path_for_err,
            request_identity,
            true,
        )
    }

    fn remote_read_chunk_by_handle_with_s3_policy(
        &self,
        export_name: &str,
        relpath: &str,
        offset: i64,
        n: i64,
        file_size: i64,
        mtime_ns: i64,
        allow_kv_cache: bool,
        kv_miss_policy: FluxonFsS3KvMissPolicy,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
        allow_s3_internal_multipart: bool,
    ) -> Result<Vec<u8>, FsAgentError> {
        let (_cfg, export) = self.get_export_cfg(export_name)?;
        let relpath_rpc = normalize_relpath_rpc(relpath);
        let read_remote = |offset2: i64, length2: i64| -> Result<Vec<u8>, FsAgentError> {
            if allow_s3_internal_multipart {
                return self.remote_read_chunk_with_internal_multipart(
                    export_name,
                    &export,
                    relpath_rpc.as_ref(),
                    offset2,
                    length2,
                    path_for_err,
                    request_identity,
                    true,
                );
            }
            self.remote_read_chunk(
                export_name,
                &export,
                relpath_rpc.as_ref(),
                offset2,
                length2,
                path_for_err,
                request_identity,
            )
        };
        if offset < 0 {
            return Err(FsAgentError::InvalidArgument {
                detail: format!("offset must be >= 0 (got {})", offset),
            });
        }
        if n < 0 {
            return Err(FsAgentError::InvalidArgument {
                detail: format!("n must be >= 0 (got {})", n),
            });
        }
        if n == 0 {
            return Ok(Vec::new());
        }
        if n > (REMOTE_READ_CHUNK_BYTES as i64) {
            return Err(FsAgentError::InvalidArgument {
                detail: format!(
                    "n too large for remote_read_chunk_by_handle: n={} (max={})",
                    n, REMOTE_READ_CHUNK_BYTES
                ),
            });
        }

        // Fast path: KV cache not enabled for this call.
        //
        // English note: `stage_to_kv_then_read` has a strict contract: it requires KV to be enabled.
        // No fallback behavior here because it would hide config mistakes.
        if !allow_kv_cache {
            if kv_miss_policy == FluxonFsS3KvMissPolicy::StageToKvThenRead {
                return Err(FsAgentError::InvalidArgument {
                    detail: "s3 kv_miss_policy=stage_to_kv_then_read requires allow_kv_cache=true"
                        .to_string(),
                });
            }
            return read_remote(offset, n);
        }
        if export.cache_kv_key_prefix.trim().is_empty()
            || export.cache_bytes_field_key.trim().is_empty()
        {
            if kv_miss_policy == FluxonFsS3KvMissPolicy::StageToKvThenRead {
                return Err(FsAgentError::InvalidArgument {
                    detail: format!(
                        "s3 kv_miss_policy=stage_to_kv_then_read requires export.cache_kv_key_prefix and export.cache_bytes_field_key: export={} relpath={}",
                        export_name,
                        relpath_rpc.as_ref()
                    ),
                });
            }
            return read_remote(offset, n);
        }
        if file_size < 0 {
            return Err(FsAgentError::InvalidArgument {
                detail: format!("file_size must be >= 0 (got {})", file_size),
            });
        }
        if mtime_ns < 0 {
            return Err(FsAgentError::InvalidArgument {
                detail: format!("mtime_ns must be >= 0 (got {})", mtime_ns),
            });
        }
        if offset >= file_size {
            // Match agent read_chunk behavior: offset beyond EOF returns empty.
            return Ok(Vec::new());
        }

        // English note (causal chain):
        // - export.cache_max_bytes is the explicit "this file size is cacheable" contract.
        // - For non-cacheable files, KV lookups are pure overhead and can dominate S3 throughput.
        // - Therefore:
        //   - kv_miss_policy=remote_read => bypass KV entirely for this object.
        //   - kv_miss_policy=stage_to_kv_then_read => hard error (no fallback), because staging would
        //     violate the explicit size contract and introduce write amplification surprises.
        let cache_allowed_by_size = (file_size as u64) <= export.cache_max_bytes;
        if !cache_allowed_by_size {
            if kv_miss_policy == FluxonFsS3KvMissPolicy::StageToKvThenRead {
                return Err(FsAgentError::InvalidArgument {
                    detail: format!(
                        "s3 kv_miss_policy=stage_to_kv_then_read requires file_size <= export.cache_max_bytes: export={} relpath={} file_size={} cache_max_bytes={}",
                        export_name,
                        relpath_rpc.as_ref(),
                        file_size,
                        export.cache_max_bytes
                    ),
                });
            }
            return read_remote(offset, n);
        }

        // Match agent read_chunk behavior: cap n at EOF.
        let avail = file_size - offset;
        let n_eff = std::cmp::min(n, avail);
        if n_eff <= 0 {
            return Ok(Vec::new());
        }

        // English note (causal chain):
        // - The user expects: "read checks KV first; KV hit => no remote read RPC".
        // - We reuse the stable fs_s3/v2 piece cache KV layout to avoid schema drift.
        // - We keep two explicit modes for KV miss handling:
        //   - remote_read: do NOT write to KV in the hot path (avoid write amplification and KV RTT).
        //   - stage_to_kv_then_read: explicitly stage the missing piece to KV via agent RPC.
        // - We intentionally DO NOT re-stat the remote file in the hot path:
        //   - The KV key is already versioned by (size, mtime_ns) via `sig`.
        //   - Any extra remote_stat in the per-piece path makes S3 GET RTT-bound again.
        let sig = fluxon_fs_core::s3_gateway::object_sig_string(file_size, mtime_ns);
        let piece_bytes = REMOTE_CHUNK_BYTES as i64;
        let end_inclusive = match offset.checked_add(n_eff.saturating_sub(1)) {
            Some(v) => v,
            None => {
                return Err(FsAgentError::InvalidArgument {
                    detail: format!("offset+n overflow: offset={} n={}", offset, n_eff),
                });
            }
        };
        let start_piece = offset / piece_bytes;
        let end_piece = end_inclusive / piece_bytes;
        if end_piece < start_piece || end_piece - start_piece > 1 {
            return Err(FsAgentError::InvalidArgument {
                detail: format!(
                    "read range spans too many pieces: start_piece={} end_piece={} n_eff={} piece_bytes={}",
                    start_piece, end_piece, n_eff, piece_bytes
                ),
            });
        }

        let start_in_piece = (offset - (start_piece * piece_bytes)) as usize;
        let want_total = n_eff as usize;

        let piece_kv_key = |piece_idx: i64| -> String {
            fluxon_fs_core::s3_gateway::kv_piece_key(
                &export.cache_kv_key_prefix,
                export_name,
                relpath_rpc.as_ref(),
                &sig,
                piece_idx,
            )
        };

        let try_read_piece_kv = |piece_idx: i64| -> Option<Vec<u8>> {
            let key = piece_kv_key(piece_idx);
            self.kv_try_get_bytes_field(&key, &export.cache_bytes_field_key)
        };

        // English note (causal chain):
        // - RemoteRead is the "KV read-only" policy: KV hit => serve from KV; otherwise read the
        //   requested range directly from remote, without staging/backfilling.
        // - This keeps the cold-cache path to "1 KV GET + 1 remote read" (instead of forcing a
        //   full-piece remote read + slice), and keeps small Range GETs efficient.
        if kv_miss_policy == FluxonFsS3KvMissPolicy::RemoteRead {
            if start_piece == end_piece {
                if let Some(p0) = try_read_piece_kv(start_piece) {
                    if start_in_piece >= p0.len() {
                        return Ok(Vec::new());
                    }
                    let end0 = std::cmp::min(p0.len(), start_in_piece + want_total);
                    return Ok(p0[start_in_piece..end0].to_vec());
                }
                return read_remote(offset, n_eff);
            }

            let kv0 = try_read_piece_kv(start_piece);
            let kv1 = try_read_piece_kv(end_piece);
            if let (Some(p0), Some(p1)) = (kv0, kv1) {
                if start_in_piece >= p0.len() {
                    return Ok(Vec::new());
                }
                let take0 = std::cmp::min(want_total, p0.len() - start_in_piece);
                let mut out: Vec<u8> =
                    Vec::with_capacity(std::cmp::min(want_total, take0 + p1.len()));
                out.extend_from_slice(&p0[start_in_piece..start_in_piece + take0]);
                let left = want_total.saturating_sub(take0);
                if left > 0 {
                    let take1 = std::cmp::min(left, p1.len());
                    out.extend_from_slice(&p1[0..take1]);
                }
                return Ok(out);
            }
            // English note: keep 2-piece reads consistent: do not mix KV hit + remote read.
            return read_remote(offset, n_eff);
        }

        if start_piece == end_piece {
            if let Some(p0) = try_read_piece_kv(start_piece) {
                if start_in_piece >= p0.len() {
                    return Ok(Vec::new());
                }
                let end0 = std::cmp::min(p0.len(), start_in_piece + want_total);
                let out = p0[start_in_piece..end0].to_vec();
                return Ok(out);
            }

            let out = read_remote(offset, n_eff)?;
            if export.async_backfill_enabled {
                let _ = self.cache_controller.handle_suggest(
                    PieceKey {
                        export: export_name.to_string(),
                        relpath: relpath_rpc.as_ref().to_string(),
                        sig: sig.clone(),
                        piece_idx: start_piece,
                    },
                    request_identity.cloned(),
                );
            }
            return Ok(out);
        }

        let kv0 = try_read_piece_kv(start_piece);
        let kv1 = try_read_piece_kv(end_piece);

        let (p0, p1) = if let (Some(p0), Some(p1)) = (kv0.clone(), kv1.clone()) {
            (p0, p1)
        } else {
            let out = read_remote(offset, n_eff)?;
            if export.async_backfill_enabled {
                if kv0.is_none() {
                    let _ = self.cache_controller.handle_suggest(
                        PieceKey {
                            export: export_name.to_string(),
                            relpath: relpath_rpc.as_ref().to_string(),
                            sig: sig.clone(),
                            piece_idx: start_piece,
                        },
                        request_identity.cloned(),
                    );
                }
                if kv1.is_none() {
                    let _ = self.cache_controller.handle_suggest(
                        PieceKey {
                            export: export_name.to_string(),
                            relpath: relpath_rpc.as_ref().to_string(),
                            sig: sig.clone(),
                            piece_idx: end_piece,
                        },
                        request_identity.cloned(),
                    );
                }
            }
            return Ok(out);
        };

        if start_in_piece >= p0.len() {
            return Ok(Vec::new());
        }
        let take0 = std::cmp::min(want_total, p0.len() - start_in_piece);
        let mut out: Vec<u8> = Vec::with_capacity(std::cmp::min(want_total, take0 + p1.len()));
        out.extend_from_slice(&p0[start_in_piece..start_in_piece + take0]);
        let left = want_total.saturating_sub(take0);
        if left > 0 {
            let take1 = std::cmp::min(left, p1.len());
            out.extend_from_slice(&p1[0..take1]);
        }
        Ok(out)
    }

    pub fn remote_write_chunk_by_handle(
        &self,
        export_name: &str,
        relpath: &str,
        offset: i64,
        data: Vec<u8>,
        path_for_err: &str,
    ) -> Result<(), FsAgentError> {
        self.remote_write_chunk_by_handle_with_identity(
            export_name,
            relpath,
            offset,
            data,
            path_for_err,
            None,
        )
    }

    pub fn remote_write_chunk_by_handle_with_identity(
        &self,
        export_name: &str,
        relpath: &str,
        offset: i64,
        data: Vec<u8>,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<(), FsAgentError> {
        let (_cfg, export) = self.get_export_cfg(export_name)?;
        let relpath_rpc = normalize_relpath_rpc(relpath);
        self.remote_write_chunk(
            export_name,
            &export,
            relpath_rpc.as_ref(),
            offset,
            data,
            path_for_err,
            request_identity,
        )
    }

    pub(crate) fn remote_write_chunk_by_handle_s3_gateway_with_identity(
        &self,
        export_name: &str,
        relpath: &str,
        offset: i64,
        data: Vec<u8>,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<(), FsAgentError> {
        let (_cfg, export) = self.get_export_cfg(export_name)?;
        let relpath_rpc = normalize_relpath_rpc(relpath);
        self.remote_write_chunk_with_internal_multipart(
            export_name,
            &export,
            relpath_rpc.as_ref(),
            offset,
            data,
            path_for_err,
            request_identity,
            true,
        )
    }

    pub fn remote_open_write_session_by_handle(
        &self,
        export_name: &str,
        relpath: &str,
        path_for_err: &str,
    ) -> Result<(String, i64, i64), FsAgentError> {
        self.remote_open_write_session_by_handle_with_identity(
            export_name,
            relpath,
            path_for_err,
            None,
        )
    }

    pub fn remote_open_write_session_by_handle_with_identity(
        &self,
        export_name: &str,
        relpath: &str,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<(String, i64, i64), FsAgentError> {
        let ctx =
            self.build_remote_write_session_call_ctx(export_name, relpath, request_identity)?;
        let (node_id, remote_session_id, size, mtime_ns) = self.remote_open_write_session(
            export_name,
            &ctx.export,
            ctx.relpath_rpc.as_str(),
            path_for_err,
            ctx.fs_rpc_token.clone(),
            ctx.allow_s3_internal_multipart,
        )?;
        let client_session_id = self.alloc_remote_write_session_client_token();
        let peer_sender = self.remote_write_peer_sender_get_or_insert(&node_id);
        let state = Arc::new(RemoteWriteSessionClientState::default());
        let entry = Arc::new(RemoteWriteSessionClientEntry {
            node_id,
            remote_session_id,
            export_name: export_name.to_string(),
            relpath_rpc: ctx.relpath_rpc,
            fs_rpc_token: ctx.fs_rpc_token,
            allow_s3_internal_multipart: ctx.allow_s3_internal_multipart,
            peer_sender,
            state: state.clone(),
        });
        self.remote_write_session_client_insert(client_session_id.clone(), entry);
        Ok((client_session_id, size, mtime_ns))
    }

    pub fn remote_write_session_chunk_by_handle_with_identity(
        &self,
        export_name: &str,
        relpath: &str,
        session_id: &str,
        offset: i64,
        data: Vec<u8>,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<(), FsAgentError> {
        let (node_id, req) = if let Some(session) =
            self.remote_write_session_client_lookup(session_id)
        {
            (
                session.node_id.clone(),
                FsWriteSessionChunkReq {
                    export: session.export_name.clone(),
                    relpath: session.relpath_rpc.clone(),
                    session_id: session.remote_session_id.clone(),
                    offset,
                    fs_rpc_token: session.fs_rpc_token.clone(),
                    allow_s3_internal_multipart: session.allow_s3_internal_multipart,
                },
            )
        } else {
            let ctx =
                self.build_remote_write_session_call_ctx(export_name, relpath, request_identity)?;
            (
                self.remote_pick_node_forever(export_name, &ctx.export, "write_session_chunk")?,
                FsWriteSessionChunkReq {
                    export: export_name.to_string(),
                    relpath: ctx.relpath_rpc,
                    session_id: session_id.to_string(),
                    offset,
                    fs_rpc_token: ctx.fs_rpc_token,
                    allow_s3_internal_multipart: ctx.allow_s3_internal_multipart,
                },
            )
        };
        let timeout_ms = remote_write_session_rpc_timeout_ms(data.len());
        let timeout = std::time::Duration::from_millis(timeout_ms);
        let kv_framework = self.kv_framework.clone();
        let resp = self
            .rt_handle
            .run_async_from_sync(async move {
                write_session_rpc::call_write_session_chunk(
                    kv_framework.as_ref(),
                    node_id.into(),
                    req,
                    data,
                    Some(timeout),
                )
                .await
            })
            .map_err(|e| FsAgentError::InvalidArgument {
                detail: format!("write_session_chunk async bridge failed: {}", e),
            })??;
        if resp.ok {
            return Ok(());
        }
        Err(FsAgentError::Io {
            path: path_for_err.to_string(),
            detail: if resp.err_detail.trim().is_empty() {
                "remote write_session_chunk failed".to_string()
            } else {
                resp.err_detail
            },
        })
    }

    fn remote_enqueue_write_session_submits(
        &self,
        session: &Arc<RemoteWriteSessionClientEntry>,
        submits: Vec<RemoteWriteSessionClientSubmit>,
        path_for_err: &str,
    ) -> Result<(), FsAgentError> {
        for submit in submits {
            let followup = session
                .state
                .enqueue_submit(submit)
                .map_err(|detail| fs_agent_io_err(path_for_err, detail))?;
            if let Some(confirm_seq_no) = followup.confirm_seq_no {
                remote_write_session_schedule_ack_timeout(
                    &self.rt_handle,
                    self.kv_framework.clone(),
                    session.clone(),
                    confirm_seq_no,
                );
            }
            for _ in 0..followup.schedule_count {
                self.remote_write_session_schedule(session, path_for_err)?;
            }
        }
        Ok(())
    }

    fn remote_wait_write_session_client_barrier(
        &self,
        session: &Arc<RemoteWriteSessionClientEntry>,
        path_for_err: &str,
    ) -> Result<u64, FsAgentError> {
        let expected_data_frames = session
            .state
            .submitted_frames()
            .map_err(|detail| fs_agent_io_err(path_for_err, detail))?;
        if expected_data_frames == 0 {
            return Ok(0);
        }
        session
            .state
            .wait_for_sent_frames(expected_data_frames)
            .map_err(|detail| fs_agent_io_err(path_for_err, detail))?;
        let node_id: NodeID = session.node_id.clone().into();
        let req = write_session_rpc::FsWaitWriteSessionPayloadsReq {
            export: session.export_name.clone(),
            relpath: session.relpath_rpc.clone(),
            session_id: session.remote_session_id.clone(),
            expected_data_frames,
            fs_rpc_token: session.fs_rpc_token.clone(),
            allow_s3_internal_multipart: session.allow_s3_internal_multipart,
        };
        let kv_framework = self.kv_framework.clone();
        let timeout = std::time::Duration::from_millis(REMOTE_WRITE_SESSION_CONTROL_RPC_TIMEOUT_MS);
        let resp = self
            .rt_handle
            .run_async_from_sync(async move {
                write_session_rpc::call_wait_write_session_payloads(
                    kv_framework.as_ref(),
                    node_id,
                    req,
                    Some(timeout),
                )
                .await
            })
            .map_err(|e| FsAgentError::InvalidArgument {
                detail: format!("wait_write_session_payloads async bridge failed: {}", e),
            })??;
        if !resp.ok {
            return Err(FsAgentError::Io {
                path: path_for_err.to_string(),
                detail: if resp.err_detail.trim().is_empty() {
                    "remote wait_write_session_payloads failed".to_string()
                } else {
                    resp.err_detail
                },
            });
        }
        session
            .state
            .confirm_delivered_upto(expected_data_frames)
            .map_err(|detail| fs_agent_io_err(path_for_err, detail))?;
        Ok(expected_data_frames)
    }

    pub fn remote_buffer_write_session_payload_by_handle_with_identity(
        &self,
        _export_name: &str,
        _relpath: &str,
        session_id: &str,
        offset: i64,
        data: Bytes,
        submit_bytes: usize,
        max_inflight_chunks: usize,
        path_for_err: &str,
        _request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<(), FsAgentError> {
        if data.is_empty() {
            return Ok(());
        }
        if let Some(session) = self.remote_write_session_client_lookup(session_id) {
            let submits = session
                .state
                .buffer_append(offset, data, submit_bytes, max_inflight_chunks)
                .map_err(|detail| fs_agent_io_err(path_for_err, detail))?;
            return self.remote_enqueue_write_session_submits(&session, submits, path_for_err);
        }
        Err(FsAgentError::InvalidArgument {
            detail: format!("unknown write session: {}", session_id),
        })
    }

    pub fn remote_flush_write_session_buffer_by_handle_with_identity(
        &self,
        _export_name: &str,
        _relpath: &str,
        session_id: &str,
        path_for_err: &str,
        _request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<(), FsAgentError> {
        let Some(session) = self.remote_write_session_client_lookup(session_id) else {
            return Ok(());
        };
        let submits = session
            .state
            .flush_buffered()
            .map_err(|detail| fs_agent_io_err(path_for_err, detail))?;
        self.remote_enqueue_write_session_submits(&session, submits, path_for_err)
    }

    pub fn remote_wait_write_session_payloads_by_handle_with_identity(
        &self,
        export_name: &str,
        relpath: &str,
        session_id: &str,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<(), FsAgentError> {
        let Some(session) = self.remote_write_session_client_lookup(session_id) else {
            return Ok(());
        };
        self.remote_flush_write_session_buffer_by_handle_with_identity(
            export_name,
            relpath,
            session_id,
            path_for_err,
            request_identity,
        )?;
        self.remote_wait_write_session_client_barrier(&session, path_for_err)?;
        Ok(())
    }

    pub fn remote_truncate_write_session_by_handle_with_identity(
        &self,
        export_name: &str,
        relpath: &str,
        session_id: &str,
        size: i64,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<(), FsAgentError> {
        if let Some(session) = self.remote_write_session_client_lookup(session_id) {
            self.remote_flush_write_session_buffer_by_handle_with_identity(
                export_name,
                relpath,
                session_id,
                path_for_err,
                request_identity,
            )?;
            self.remote_wait_write_session_client_barrier(&session, path_for_err)?;
            let rpc_path = self
                .get_export_cfg(session.export_name.as_str())?
                .1
                .rpc_paths
                .truncate_write_session;
            let payload: FlatDict = FlatDict::from([
                (
                    "export".to_string(),
                    FlatValue::String(session.export_name.clone()),
                ),
                (
                    "relpath".to_string(),
                    FlatValue::String(session.relpath_rpc.clone()),
                ),
                (
                    "session_id".to_string(),
                    FlatValue::String(session.remote_session_id.clone()),
                ),
                ("size".to_string(), FlatValue::Int64(size)),
            ]);
            return self.remote_call_ok_on_node_with_timeout(
                &session.node_id,
                rpc_path.as_str(),
                payload,
                "truncate_write_session",
                path_for_err,
                session.fs_rpc_token.clone(),
                session.allow_s3_internal_multipart,
                None,
            );
        }
        let ctx =
            self.build_remote_write_session_call_ctx(export_name, relpath, request_identity)?;
        let payload: FlatDict = FlatDict::from([
            (
                "export".to_string(),
                FlatValue::String(export_name.to_string()),
            ),
            (
                "relpath".to_string(),
                FlatValue::String(ctx.relpath_rpc.clone()),
            ),
            (
                "session_id".to_string(),
                FlatValue::String(session_id.to_string()),
            ),
            ("size".to_string(), FlatValue::Int64(size)),
        ]);
        self.remote_call_ok_with_internal_multipart(
            export_name,
            &ctx.export,
            &ctx.export.rpc_paths.truncate_write_session,
            payload,
            "truncate_write_session",
            path_for_err,
            request_identity,
            ctx.allow_s3_internal_multipart,
        )
    }

    pub fn remote_close_write_session_by_handle_with_identity(
        &self,
        export_name: &str,
        relpath: &str,
        session_id: &str,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<(i64, i64), FsAgentError> {
        let session = self.remote_write_session_client_lookup(session_id);
        let (node_id, req, relpath_rpc_for_invalidate) = if let Some(session) = session.as_ref() {
            self.remote_flush_write_session_buffer_by_handle_with_identity(
                export_name,
                relpath,
                session_id,
                path_for_err,
                request_identity,
            )?;
            // Close must not advertise frames that are only locally submitted but
            // not yet delivered to the remote agent. Otherwise close can arrive
            // first and time out waiting for tail frames that are still queued
            // in the client-side sender or transport.
            let expected_data_frames =
                self.remote_wait_write_session_client_barrier(session, path_for_err)?;
            (
                session.node_id.clone(),
                FsCloseWriteSessionReq {
                    export: session.export_name.clone(),
                    relpath: session.relpath_rpc.clone(),
                    session_id: session.remote_session_id.clone(),
                    expected_data_frames,
                    fs_rpc_token: session.fs_rpc_token.clone(),
                    allow_s3_internal_multipart: session.allow_s3_internal_multipart,
                },
                session.relpath_rpc.clone(),
            )
        } else {
            let ctx =
                self.build_remote_write_session_call_ctx(export_name, relpath, request_identity)?;
            (
                self.remote_pick_node_forever(export_name, &ctx.export, "close_write_session")?,
                FsCloseWriteSessionReq {
                    export: export_name.to_string(),
                    relpath: ctx.relpath_rpc.clone(),
                    session_id: session_id.to_string(),
                    expected_data_frames: 0,
                    fs_rpc_token: ctx.fs_rpc_token,
                    allow_s3_internal_multipart: ctx.allow_s3_internal_multipart,
                },
                ctx.relpath_rpc,
            )
        };
        let kv_framework = self.kv_framework.clone();
        let timeout = std::time::Duration::from_millis(REMOTE_WRITE_SESSION_CONTROL_RPC_TIMEOUT_MS);
        let resp = self
            .rt_handle
            .run_async_from_sync(async move {
                write_session_rpc::call_close_write_session(
                    kv_framework.as_ref(),
                    node_id.into(),
                    req,
                    Some(timeout),
                )
                .await
            })
            .map_err(|e| FsAgentError::InvalidArgument {
                detail: format!("close_write_session async bridge failed: {}", e),
            })??;
        if !resp.ok {
            return Err(FsAgentError::Io {
                path: path_for_err.to_string(),
                detail: if resp.err_detail.trim().is_empty() {
                    "remote close_write_session failed".to_string()
                } else {
                    resp.err_detail
                },
            });
        }
        if session.is_some() {
            let _ = self.remote_write_session_client_remove(session_id);
        }
        self.metadata_cache_invalidate_exact_and_publish(
            export_name,
            relpath_rpc_for_invalidate.as_str(),
        );
        Ok((resp.size, resp.mtime_ns))
    }

    pub fn remote_abort_write_session_by_handle_with_identity(
        &self,
        export_name: &str,
        relpath: &str,
        session_id: &str,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<(), FsAgentError> {
        let session = self.remote_write_session_client_lookup(session_id);
        if let Some(session) = session.as_ref() {
            session
                .state
                .fail("write session aborted locally".to_string());
        }
        let (node_id, req) = if let Some(session) = session.as_ref() {
            (
                session.node_id.clone(),
                FsAbortWriteSessionReq {
                    export: session.export_name.clone(),
                    relpath: session.relpath_rpc.clone(),
                    session_id: session.remote_session_id.clone(),
                    fs_rpc_token: session.fs_rpc_token.clone(),
                    allow_s3_internal_multipart: session.allow_s3_internal_multipart,
                },
            )
        } else {
            let ctx =
                self.build_remote_write_session_call_ctx(export_name, relpath, request_identity)?;
            (
                self.remote_pick_node_forever(export_name, &ctx.export, "abort_write_session")?,
                FsAbortWriteSessionReq {
                    export: export_name.to_string(),
                    relpath: ctx.relpath_rpc,
                    session_id: session_id.to_string(),
                    fs_rpc_token: ctx.fs_rpc_token,
                    allow_s3_internal_multipart: ctx.allow_s3_internal_multipart,
                },
            )
        };
        let kv_framework = self.kv_framework.clone();
        let timeout = std::time::Duration::from_millis(REMOTE_WRITE_SESSION_CONTROL_RPC_TIMEOUT_MS);
        let resp = self
            .rt_handle
            .run_async_from_sync(async move {
                write_session_rpc::call_abort_write_session(
                    kv_framework.as_ref(),
                    node_id.into(),
                    req,
                    Some(timeout),
                )
                .await
            })
            .map_err(|e| FsAgentError::InvalidArgument {
                detail: format!("abort_write_session async bridge failed: {}", e),
            })??;
        if resp.ok {
            if session.is_some() {
                let _ = self.remote_write_session_client_remove(session_id);
            }
            return Ok(());
        }
        Err(FsAgentError::Io {
            path: path_for_err.to_string(),
            detail: if resp.err_detail.trim().is_empty() {
                "remote abort_write_session failed".to_string()
            } else {
                resp.err_detail
            },
        })
    }

    pub fn remote_truncate_by_handle(
        &self,
        export_name: &str,
        relpath: &str,
        size: i64,
        path_for_err: &str,
    ) -> Result<(), FsAgentError> {
        self.remote_truncate_by_handle_with_identity(export_name, relpath, size, path_for_err, None)
    }

    pub fn remote_truncate_by_handle_with_identity(
        &self,
        export_name: &str,
        relpath: &str,
        size: i64,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<(), FsAgentError> {
        let (_cfg, export) = self.get_export_cfg(export_name)?;
        let relpath_rpc = normalize_relpath_rpc(relpath);
        self.remote_truncate_with_internal_multipart(
            export_name,
            &export,
            relpath_rpc.as_ref(),
            size,
            path_for_err,
            request_identity,
            false,
        )
    }

    pub(crate) fn remote_truncate_by_handle_s3_gateway_with_identity(
        &self,
        export_name: &str,
        relpath: &str,
        size: i64,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<(), FsAgentError> {
        let (_cfg, export) = self.get_export_cfg(export_name)?;
        let relpath_rpc = normalize_relpath_rpc(relpath);
        self.remote_truncate_with_internal_multipart(
            export_name,
            &export,
            relpath_rpc.as_ref(),
            size,
            path_for_err,
            request_identity,
            true,
        )
    }

    pub fn remote_list_dir_json_by_handle(
        &self,
        export_name: &str,
        relpath: &str,
        path_for_err: &str,
    ) -> Result<String, FsAgentError> {
        self.remote_list_dir_json_by_handle_with_identity(export_name, relpath, path_for_err, None)
    }

    pub fn remote_list_dir_json_by_handle_with_identity(
        &self,
        export_name: &str,
        relpath: &str,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<String, FsAgentError> {
        let (_cfg, export) = self.get_export_cfg(export_name)?;
        let relpath_rpc = normalize_relpath_rpc(relpath);
        let payload: FlatDict = FlatDict::from([
            (
                "export".to_string(),
                FlatValue::String(export_name.to_string()),
            ),
            (
                "relpath".to_string(),
                FlatValue::String(relpath_rpc.as_ref().to_string()),
            ),
        ]);
        let payload = self.request_payload_with_identity(payload, request_identity)?;
        let resp = self.remote_rpc_call_forever(
            export_name,
            &export,
            &export.rpc_paths.list_dir,
            &payload,
            "list_dir",
        )?;
        let ok = match resp.get("ok") {
            Some(FlatValue::Bool(v)) => *v,
            _ => false,
        };
        if !ok {
            return Err(err_from_resp(&resp, path_for_err));
        }
        match resp.get("entries_json") {
            Some(FlatValue::String(s)) => Ok(s.clone()),
            _ => Err(FsAgentError::InvalidArgument {
                detail: "list_dir response missing entries_json".to_string(),
            }),
        }
    }

    pub(crate) fn remote_list_dir_json_by_handle_s3_gateway_with_identity(
        &self,
        export_name: &str,
        relpath: &str,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<String, FsAgentError> {
        let (_cfg, export) = self.get_export_cfg(export_name)?;
        let relpath_rpc = normalize_relpath_rpc(relpath);
        let payload: FlatDict = FlatDict::from([
            (
                "export".to_string(),
                FlatValue::String(export_name.to_string()),
            ),
            (
                "relpath".to_string(),
                FlatValue::String(relpath_rpc.as_ref().to_string()),
            ),
        ]);
        let payload = self.request_payload_with_identity_and_internal_multipart(
            payload,
            request_identity,
            is_internal_multipart_relpath(relpath_rpc.as_ref()),
        )?;
        let resp = self.remote_rpc_call_forever(
            export_name,
            &export,
            &export.rpc_paths.list_dir,
            &payload,
            "list_dir",
        )?;
        let ok = match resp.get("ok") {
            Some(FlatValue::Bool(v)) => *v,
            _ => false,
        };
        if !ok {
            return Err(err_from_resp(&resp, path_for_err));
        }
        match resp.get("entries_json") {
            Some(FlatValue::String(s)) => Ok(s.clone()),
            _ => Err(FsAgentError::InvalidArgument {
                detail: "list_dir response missing entries_json".to_string(),
            }),
        }
    }

    pub fn remote_mkdir_by_handle(
        &self,
        export_name: &str,
        relpath: &str,
        mode: i64,
        path_for_err: &str,
    ) -> Result<(), FsAgentError> {
        self.remote_mkdir_by_handle_with_identity(export_name, relpath, mode, path_for_err, None)
    }

    pub fn remote_mkdir_by_handle_with_identity(
        &self,
        export_name: &str,
        relpath: &str,
        mode: i64,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<(), FsAgentError> {
        let (_cfg, export) = self.get_export_cfg(export_name)?;
        let relpath_rpc = normalize_relpath_rpc(relpath);
        self.remote_call_ok_with_internal_multipart(
            export_name,
            &export,
            &export.rpc_paths.mkdir,
            FlatDict::from([
                (
                    "export".to_string(),
                    FlatValue::String(export_name.to_string()),
                ),
                (
                    "relpath".to_string(),
                    FlatValue::String(relpath_rpc.as_ref().to_string()),
                ),
                ("mode".to_string(), FlatValue::Int64(mode)),
            ]),
            "mkdir",
            path_for_err,
            request_identity,
            false,
        )?;
        match request_identity {
            Some(identity) => self.metadata_cache_invalidate_prefix_with_identity(
                identity,
                export_name,
                relpath_rpc.as_ref(),
            ),
            None => {
                self.metadata_cache_invalidate_prefix_and_publish(export_name, relpath_rpc.as_ref())
            }
        }
        Ok(())
    }

    pub(crate) fn remote_mkdir_by_handle_s3_gateway_with_identity(
        &self,
        export_name: &str,
        relpath: &str,
        mode: i64,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<(), FsAgentError> {
        let (_cfg, export) = self.get_export_cfg(export_name)?;
        let relpath_rpc = normalize_relpath_rpc(relpath);
        self.remote_call_ok_with_internal_multipart(
            export_name,
            &export,
            &export.rpc_paths.mkdir,
            FlatDict::from([
                (
                    "export".to_string(),
                    FlatValue::String(export_name.to_string()),
                ),
                (
                    "relpath".to_string(),
                    FlatValue::String(relpath_rpc.as_ref().to_string()),
                ),
                ("mode".to_string(), FlatValue::Int64(mode)),
            ]),
            "mkdir",
            path_for_err,
            request_identity,
            is_internal_multipart_relpath(relpath_rpc.as_ref()),
        )?;
        match request_identity {
            Some(identity) => self.metadata_cache_invalidate_prefix_with_identity(
                identity,
                export_name,
                relpath_rpc.as_ref(),
            ),
            None => {
                self.metadata_cache_invalidate_prefix_and_publish(export_name, relpath_rpc.as_ref())
            }
        }
        Ok(())
    }

    pub fn direct_write_fd_on_close(
        &self,
        export_name: &str,
        relpath: &str,
    ) -> Result<(), FsAgentError> {
        let (_cfg, _export) = self.get_export_cfg(export_name)?;
        let relpath_rpc = normalize_relpath_rpc(relpath);
        self.metadata_cache_invalidate_exact_and_publish(export_name, relpath_rpc.as_ref());
        Ok(())
    }

    pub fn remote_unlink_by_handle(
        &self,
        export_name: &str,
        relpath: &str,
        path_for_err: &str,
    ) -> Result<(), FsAgentError> {
        self.remote_unlink_by_handle_with_identity(export_name, relpath, path_for_err, None)
    }

    pub fn remote_unlink_by_handle_with_identity(
        &self,
        export_name: &str,
        relpath: &str,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<(), FsAgentError> {
        let (_cfg, export) = self.get_export_cfg(export_name)?;
        let relpath_rpc = normalize_relpath_rpc(relpath);
        self.remote_call_ok_with_internal_multipart(
            export_name,
            &export,
            &export.rpc_paths.unlink,
            FlatDict::from([
                (
                    "export".to_string(),
                    FlatValue::String(export_name.to_string()),
                ),
                (
                    "relpath".to_string(),
                    FlatValue::String(relpath_rpc.as_ref().to_string()),
                ),
            ]),
            "unlink",
            path_for_err,
            request_identity,
            false,
        )?;
        match request_identity {
            Some(identity) => self.metadata_cache_invalidate_exact_with_identity(
                identity,
                export_name,
                relpath_rpc.as_ref(),
            ),
            None => {
                self.metadata_cache_invalidate_exact_and_publish(export_name, relpath_rpc.as_ref())
            }
        }
        Ok(())
    }

    pub(crate) fn remote_unlink_by_handle_s3_gateway_with_identity(
        &self,
        export_name: &str,
        relpath: &str,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<(), FsAgentError> {
        let (_cfg, export) = self.get_export_cfg(export_name)?;
        let relpath_rpc = normalize_relpath_rpc(relpath);
        self.remote_call_ok_with_internal_multipart(
            export_name,
            &export,
            &export.rpc_paths.unlink,
            FlatDict::from([
                (
                    "export".to_string(),
                    FlatValue::String(export_name.to_string()),
                ),
                (
                    "relpath".to_string(),
                    FlatValue::String(relpath_rpc.as_ref().to_string()),
                ),
            ]),
            "unlink",
            path_for_err,
            request_identity,
            is_internal_multipart_relpath(relpath_rpc.as_ref()),
        )?;
        match request_identity {
            Some(identity) => self.metadata_cache_invalidate_exact_with_identity(
                identity,
                export_name,
                relpath_rpc.as_ref(),
            ),
            None => {
                self.metadata_cache_invalidate_exact_and_publish(export_name, relpath_rpc.as_ref())
            }
        }
        Ok(())
    }

    pub fn remote_rmdir_by_handle(
        &self,
        export_name: &str,
        relpath: &str,
        path_for_err: &str,
    ) -> Result<(), FsAgentError> {
        self.remote_rmdir_by_handle_with_identity(export_name, relpath, path_for_err, None)
    }

    pub fn remote_rmdir_by_handle_with_identity(
        &self,
        export_name: &str,
        relpath: &str,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<(), FsAgentError> {
        let (_cfg, export) = self.get_export_cfg(export_name)?;
        let relpath_rpc = normalize_relpath_rpc(relpath);
        self.remote_call_ok_with_internal_multipart(
            export_name,
            &export,
            &export.rpc_paths.rmdir,
            FlatDict::from([
                (
                    "export".to_string(),
                    FlatValue::String(export_name.to_string()),
                ),
                (
                    "relpath".to_string(),
                    FlatValue::String(relpath_rpc.as_ref().to_string()),
                ),
            ]),
            "rmdir",
            path_for_err,
            request_identity,
            false,
        )?;
        match request_identity {
            Some(identity) => self.metadata_cache_invalidate_prefix_with_identity(
                identity,
                export_name,
                relpath_rpc.as_ref(),
            ),
            None => {
                self.metadata_cache_invalidate_prefix_and_publish(export_name, relpath_rpc.as_ref())
            }
        }
        Ok(())
    }

    pub(crate) fn remote_rmdir_by_handle_s3_gateway_with_identity(
        &self,
        export_name: &str,
        relpath: &str,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<(), FsAgentError> {
        let (_cfg, export) = self.get_export_cfg(export_name)?;
        let relpath_rpc = normalize_relpath_rpc(relpath);
        self.remote_call_ok_with_internal_multipart(
            export_name,
            &export,
            &export.rpc_paths.rmdir,
            FlatDict::from([
                (
                    "export".to_string(),
                    FlatValue::String(export_name.to_string()),
                ),
                (
                    "relpath".to_string(),
                    FlatValue::String(relpath_rpc.as_ref().to_string()),
                ),
            ]),
            "rmdir",
            path_for_err,
            request_identity,
            is_internal_multipart_relpath(relpath_rpc.as_ref()),
        )?;
        match request_identity {
            Some(identity) => self.metadata_cache_invalidate_prefix_with_identity(
                identity,
                export_name,
                relpath_rpc.as_ref(),
            ),
            None => {
                self.metadata_cache_invalidate_prefix_and_publish(export_name, relpath_rpc.as_ref())
            }
        }
        Ok(())
    }

    pub fn remote_rename_by_handle(
        &self,
        export_name: &str,
        src_relpath: &str,
        dst_relpath: &str,
        path_for_err: &str,
    ) -> Result<(), FsAgentError> {
        self.remote_rename_by_handle_with_identity(
            export_name,
            src_relpath,
            dst_relpath,
            path_for_err,
            None,
        )
    }

    pub fn remote_rename_by_handle_with_identity(
        &self,
        export_name: &str,
        src_relpath: &str,
        dst_relpath: &str,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<(), FsAgentError> {
        let (_cfg, export) = self.get_export_cfg(export_name)?;
        let src_relpath_rpc = normalize_relpath_rpc(src_relpath);
        let dst_relpath_rpc = normalize_relpath_rpc(dst_relpath);
        self.remote_call_ok(
            export_name,
            &export,
            &export.rpc_paths.rename,
            FlatDict::from([
                (
                    "export".to_string(),
                    FlatValue::String(export_name.to_string()),
                ),
                (
                    "src_relpath".to_string(),
                    FlatValue::String(src_relpath_rpc.as_ref().to_string()),
                ),
                (
                    "dst_relpath".to_string(),
                    FlatValue::String(dst_relpath_rpc.as_ref().to_string()),
                ),
            ]),
            "rename",
            path_for_err,
            request_identity,
        )?;
        match request_identity {
            Some(identity) => {
                self.metadata_cache_invalidate_prefix_with_identity(
                    identity,
                    export_name,
                    src_relpath_rpc.as_ref(),
                );
                self.metadata_cache_invalidate_prefix_with_identity(
                    identity,
                    export_name,
                    dst_relpath_rpc.as_ref(),
                );
            }
            None => {
                self.metadata_cache_invalidate_prefix_and_publish(
                    export_name,
                    src_relpath_rpc.as_ref(),
                );
                self.metadata_cache_invalidate_prefix_and_publish(
                    export_name,
                    dst_relpath_rpc.as_ref(),
                );
            }
        }
        Ok(())
    }

    pub fn is_remote_path(&self, file_abs: &str) -> Result<bool, FsAgentError> {
        ensure_abs_path(file_abs)?;
        Ok(self.match_mount(file_abs).is_some())
    }

    pub fn path_stat(&self, file_abs: &str) -> Result<RemoteStat, FsAgentError> {
        ensure_abs_path(file_abs)?;

        if let Some((_, export_name, relpath)) = self.match_mount(file_abs) {
            let (_cfg, export) = self.get_export_cfg(&export_name)?;
            let relpath_rpc = normalize_relpath_rpc(&relpath);
            return self.remote_stat(&export_name, &export, relpath_rpc.as_ref(), file_abs, None);
        }

        local_stat_follow(file_abs)
    }

    pub fn path_lstat(&self, file_abs: &str) -> Result<RemoteStat, FsAgentError> {
        ensure_abs_path(file_abs)?;

        if let Some((_, export_name, relpath)) = self.match_mount(file_abs) {
            let (_cfg, export) = self.get_export_cfg(&export_name)?;
            let relpath_rpc = normalize_relpath_rpc(&relpath);
            return self.remote_stat(&export_name, &export, relpath_rpc.as_ref(), file_abs, None);
        }

        local_stat_nofollow(file_abs)
    }

    pub fn path_list_dir(&self, file_abs: &str) -> Result<Vec<RemoteDirEntry>, FsAgentError> {
        ensure_abs_path(file_abs)?;

        if let Some((_mount_dir_abs, export_name, relpath)) = self.match_mount(file_abs) {
            let (_cfg, export) = self.get_export_cfg(&export_name)?;
            let relpath_rpc = normalize_relpath_rpc(&relpath);
            let entries =
                self.remote_list_dir(&export_name, &export, relpath_rpc.as_ref(), file_abs, None)?;
            let mut out: Vec<RemoteDirEntry> = Vec::new();
            for e in entries {
                let name = match e.get("name") {
                    Some(FlatValue::String(s)) => s.to_string(),
                    _ => continue,
                };
                let is_file = matches!(e.get("is_file"), Some(FlatValue::Bool(true)));
                let is_dir = matches!(e.get("is_dir"), Some(FlatValue::Bool(true)));
                out.push(RemoteDirEntry {
                    name,
                    is_file,
                    is_dir,
                });
            }

            return Ok(out);
        }

        local_list_dir(file_abs)
    }

    pub fn path_mkdir(&self, file_abs: &str, mode: i64) -> Result<(), FsAgentError> {
        ensure_abs_path(file_abs)?;

        if let Some((_, export_name, relpath)) = self.match_mount(file_abs) {
            let (_cfg, export) = self.get_export_cfg(&export_name)?;
            self.remote_call_ok(
                &export_name,
                &export,
                &export.rpc_paths.mkdir,
                FlatDict::from([
                    (
                        "export".to_string(),
                        FlatValue::String(export_name.to_string()),
                    ),
                    ("relpath".to_string(), FlatValue::String(relpath.clone())),
                    ("mode".to_string(), FlatValue::Int64(mode)),
                ]),
                "mkdir",
                file_abs,
                None,
            )?;
            self.metadata_cache_invalidate_prefix_and_publish(&export_name, &relpath);
            return Ok(());
        }

        local_mkdir(file_abs, mode)
    }

    pub fn path_rmdir(&self, file_abs: &str) -> Result<(), FsAgentError> {
        ensure_abs_path(file_abs)?;

        if let Some((_, export_name, relpath)) = self.match_mount(file_abs) {
            let (_cfg, export) = self.get_export_cfg(&export_name)?;
            self.remote_call_ok(
                &export_name,
                &export,
                &export.rpc_paths.rmdir,
                FlatDict::from([
                    (
                        "export".to_string(),
                        FlatValue::String(export_name.to_string()),
                    ),
                    ("relpath".to_string(), FlatValue::String(relpath.clone())),
                ]),
                "rmdir",
                file_abs,
                None,
            )?;
            self.metadata_cache_invalidate_prefix_and_publish(&export_name, &relpath);
            return Ok(());
        }

        local_rmdir(file_abs)
    }

    pub fn path_unlink(&self, file_abs: &str) -> Result<(), FsAgentError> {
        ensure_abs_path(file_abs)?;

        if let Some((_, export_name, relpath)) = self.match_mount(file_abs) {
            let (_cfg, export) = self.get_export_cfg(&export_name)?;
            self.remote_call_ok(
                &export_name,
                &export,
                &export.rpc_paths.unlink,
                FlatDict::from([
                    (
                        "export".to_string(),
                        FlatValue::String(export_name.to_string()),
                    ),
                    ("relpath".to_string(), FlatValue::String(relpath.clone())),
                ]),
                "unlink",
                file_abs,
                None,
            )?;
            self.metadata_cache_invalidate_exact_and_publish(&export_name, &relpath);
            return Ok(());
        }

        local_unlink(file_abs)
    }

    pub fn path_chmod(&self, file_abs: &str, mode: i64) -> Result<(), FsAgentError> {
        ensure_abs_path(file_abs)?;

        if let Some((_, export_name, relpath)) = self.match_mount(file_abs) {
            let (_cfg, export) = self.get_export_cfg(&export_name)?;
            self.remote_call_ok(
                &export_name,
                &export,
                &export.rpc_paths.chmod,
                FlatDict::from([
                    (
                        "export".to_string(),
                        FlatValue::String(export_name.to_string()),
                    ),
                    ("relpath".to_string(), FlatValue::String(relpath.clone())),
                    ("mode".to_string(), FlatValue::Int64(mode)),
                ]),
                "chmod",
                file_abs,
                None,
            )?;
            self.metadata_cache_invalidate_exact_and_publish(&export_name, &relpath);
            return Ok(());
        }

        local_chmod(file_abs, mode)
    }

    pub fn path_utime(
        &self,
        file_abs: &str,
        atime_ns: Option<i64>,
        mtime_ns: Option<i64>,
    ) -> Result<(), FsAgentError> {
        ensure_abs_path(file_abs)?;

        if let Some((_, export_name, relpath)) = self.match_mount(file_abs) {
            let (_cfg, export) = self.get_export_cfg(&export_name)?;

            let mut payload: FlatDict = FlatDict::new();
            payload.insert(
                "export".to_string(),
                FlatValue::String(export_name.to_string()),
            );
            payload.insert("relpath".to_string(), FlatValue::String(relpath.clone()));
            if let (Some(a), Some(m)) = (atime_ns, mtime_ns) {
                payload.insert("atime_ns".to_string(), FlatValue::Int64(a));
                payload.insert("mtime_ns".to_string(), FlatValue::Int64(m));
            }
            self.remote_call_ok(
                &export_name,
                &export,
                &export.rpc_paths.utime,
                payload,
                "utime",
                file_abs,
                None,
            )?;
            self.metadata_cache_invalidate_exact_and_publish(&export_name, &relpath);
            return Ok(());
        }

        local_utime(file_abs, atime_ns, mtime_ns)
    }

    pub fn path_rename(&self, src_abs: &str, dst_abs: &str) -> Result<(), FsAgentError> {
        ensure_abs_path(src_abs)?;
        ensure_abs_path(dst_abs)?;

        let src = self.match_mount(src_abs);
        let dst = self.match_mount(dst_abs);

        if src.is_none() && dst.is_none() {
            return local_rename(src_abs, dst_abs);
        }
        if src.is_none() || dst.is_none() {
            return Err(FsAgentError::os(
                libc::EXDEV,
                src_abs,
                "cross-device rename not supported",
            ));
        }
        let (src_mdir, src_export, src_rel) = src.unwrap();
        let (dst_mdir, dst_export, dst_rel) = dst.unwrap();
        if src_export != dst_export || src_mdir != dst_mdir {
            return Err(FsAgentError::os(
                libc::EXDEV,
                src_abs,
                "cross-mount rename not supported",
            ));
        }
        let (_cfg, export) = self.get_export_cfg(&src_export)?;

        self.remote_call_ok(
            &src_export,
            &export,
            &export.rpc_paths.rename,
            FlatDict::from([
                (
                    "export".to_string(),
                    FlatValue::String(src_export.to_string()),
                ),
                (
                    "src_relpath".to_string(),
                    FlatValue::String(src_rel.clone()),
                ),
                (
                    "dst_relpath".to_string(),
                    FlatValue::String(dst_rel.clone()),
                ),
            ]),
            "rename",
            src_abs,
            None,
        )?;
        self.metadata_cache_invalidate_prefix_and_publish(&src_export, &src_rel);
        self.metadata_cache_invalidate_prefix_and_publish(&src_export, &dst_rel);
        Ok(())
    }

    fn open_plan_local(&self, file_abs: &str, mode: &str) -> Result<OpenPlan, FsAgentError> {
        let cfg_guard = self.cfg.read();
        let Some(cfg) = cfg_guard.as_ref() else {
            return Ok(OpenPlan::Bypass {
                local_write_through: false,
            });
        };

        let Some(rule) = match_rule(file_abs, &cfg.rules) else {
            return Ok(OpenPlan::Bypass {
                local_write_through: false,
            });
        };

        if rule.cache_mode == CacheMode::Disabled {
            return Ok(OpenPlan::Bypass {
                local_write_through: false,
            });
        }

        if is_write_mode(mode) {
            let local_write_through = rule.write_mode == WriteMode::WriteThrough;
            return Ok(OpenPlan::Bypass {
                local_write_through,
            });
        }

        if !Path::new(file_abs).exists() {
            return Ok(OpenPlan::Bypass {
                local_write_through: false,
            });
        }

        let md = fs::metadata(file_abs).map_err(|e| FsAgentError::Io {
            path: file_abs.to_string(),
            detail: e.to_string(),
        })?;
        let size = md.len();
        if size > rule.max_cache_bytes {
            return Ok(OpenPlan::Bypass {
                local_write_through: false,
            });
        }

        let sig = fs_sig_from_metadata(&md);
        let (_, mtime_ns, _) = sig;
        let Some(kv_key) = kv_key_for_file(file_abs, rule) else {
            return Ok(OpenPlan::Bypass {
                local_write_through: false,
            });
        };

        let got = self.cache_try_get(&kv_key, &rule.bytes_field_key);
        if let Some((cached_bytes, cached_sig)) = got.as_ref() {
            if *cached_sig == sig {
                if let Some(plan) = self.inline_bytes_to_fd_plan(
                    None,
                    None,
                    None,
                    size as i64,
                    mtime_ns,
                    cached_bytes,
                ) {
                    return Ok(plan);
                }
                return Ok(OpenPlan::Bytes(cached_bytes.clone()));
            }

            if let Some(start_ms) = self
                .refresh_error_first_seen_ms
                .lock()
                .get(file_abs)
                .copied()
            {
                if within_stale_window(cfg.stale_window_ms, start_ms) {
                    if let Some(plan) = self.inline_bytes_to_fd_plan(
                        None,
                        None,
                        None,
                        size as i64,
                        mtime_ns,
                        &cached_bytes,
                    ) {
                        return Ok(plan);
                    }
                    return Ok(OpenPlan::Bytes(cached_bytes.clone()));
                }
            }
        }

        let fresh = read_file_all(file_abs, size as usize)?;
        let ok = self.cache_try_put(&kv_key, &rule.bytes_field_key, &fresh, sig);
        if ok {
            self.refresh_error_first_seen_ms.lock().remove(file_abs);
            if let Some(plan) =
                self.inline_bytes_to_fd_plan(None, None, None, size as i64, mtime_ns, &fresh)
            {
                return Ok(plan);
            }
            return Ok(OpenPlan::Bytes(fresh));
        }

        let now_ms = now_unix_ms();
        self.refresh_error_first_seen_ms
            .lock()
            .entry(file_abs.to_string())
            .or_insert(now_ms);

        if let Some((cached_bytes, _cached_sig)) = got {
            if rule.on_refresh_error == OnRefreshError::ApplyStaleWindow {
                let start_ms = self
                    .refresh_error_first_seen_ms
                    .lock()
                    .get(file_abs)
                    .copied()
                    .ok_or_else(|| FsAgentError::InvalidArgument {
                        detail: format!(
                            "local open_plan stale fallback missing refresh_error_first_seen_ms entry: {}",
                            file_abs
                        ),
                    })?;
                if within_stale_window(cfg.stale_window_ms, start_ms) {
                    if let Some(plan) = self.inline_bytes_to_fd_plan(
                        None,
                        None,
                        None,
                        size as i64,
                        mtime_ns,
                        &cached_bytes,
                    ) {
                        return Ok(plan);
                    }
                    return Ok(OpenPlan::Bytes(cached_bytes));
                }
            }
        }

        if let Some(plan) =
            self.inline_bytes_to_fd_plan(None, None, None, size as i64, mtime_ns, &fresh)
        {
            return Ok(plan);
        }
        Ok(OpenPlan::Bytes(fresh))
    }

    fn open_plan_remote(
        &self,
        file_abs: &str,
        mode: &str,
        _mount_dir_abs: &str,
        export_name: &str,
        relpath: &str,
    ) -> Result<OpenPlan, FsAgentError> {
        let (_cfg, export) = self.get_export_cfg(export_name)?;
        let relpath_rpc = normalize_relpath_rpc(relpath);
        let is_read_only = !mode.contains('w')
            && !mode.contains('a')
            && !mode.contains('x')
            && !mode.contains('+');
        let mut current_identity: Option<FluxonFsRequestIdentity> = None;
        let is_truncate_write_mode =
            mode.contains('w') && !mode.contains('a') && !mode.contains('x');
        let cached = if is_read_only {
            let request_identity = self.request_identity.read();
            request_identity.as_ref().and_then(|v| {
                self.metadata_cache_lookup(
                    &v.fingerprint,
                    export_name,
                    relpath_rpc.as_ref(),
                    export.metadata_cache_ttl_ms,
                )
            })
        } else {
            None
        };
        let st = match cached.as_ref() {
            Some(entry) => entry.stat.clone(),
            None => {
                if !is_read_only {
                    if is_truncate_write_mode {
                        RemoteStat {
                            exists: false,
                            is_file: true,
                            is_dir: false,
                            size: 0,
                            mtime_ns: 0,
                            mode: 0,
                        }
                    } else {
                        if current_identity.is_none() {
                            current_identity = self.current_request_identity();
                        }
                        let st = self.remote_stat(
                            export_name,
                            &export,
                            relpath_rpc.as_ref(),
                            file_abs,
                            current_identity.as_ref(),
                        )?;
                        if st.exists && st.is_file && st.size >= 0 && st.mtime_ns >= 0 && !st.is_dir
                        {
                            if let Some(identity_fingerprint) =
                                self.current_request_identity_fingerprint()
                            {
                                self.metadata_cache_store(
                                    &identity_fingerprint,
                                    export_name,
                                    relpath_rpc.as_ref(),
                                    &st,
                                );
                            }
                        }
                        st
                    }
                } else {
                    if current_identity.is_none() {
                        current_identity = self.current_request_identity();
                    }
                    let (st, inline_bytes) = self.remote_open_read(
                        export_name,
                        &export,
                        relpath_rpc.as_ref(),
                        file_abs,
                        current_identity.as_ref(),
                    )?;
                    let current_identity_fingerprint = self.current_request_identity_fingerprint();
                    if let Some(identity_fingerprint) = current_identity_fingerprint.as_deref() {
                        self.metadata_cache_store(
                            identity_fingerprint,
                            export_name,
                            relpath_rpc.as_ref(),
                            &st,
                        );
                    }
                    if let Some(bytes) = inline_bytes.as_ref() {
                        if !is_write_mode(mode) && st.exists && st.is_file {
                            self.maybe_stage_inline_open_read_piece(
                                export_name,
                                &export,
                                relpath_rpc.as_ref(),
                                &st,
                                bytes,
                            );
                            if let Some(plan) = self.inline_bytes_to_fd_plan(
                                current_identity_fingerprint.as_deref(),
                                Some(export_name),
                                Some(relpath_rpc.as_ref()),
                                st.size,
                                st.mtime_ns,
                                bytes,
                            ) {
                                return Ok(plan);
                            }
                            return Ok(OpenPlan::Bytes(bytes.clone()));
                        }
                    }
                    st
                }
            }
        };
        let exists = st.exists;
        if st.is_dir {
            return Err(FsAgentError::os(libc::EISDIR, file_abs, "is a directory"));
        }

        if mode.contains('x') && exists {
            return Err(FsAgentError::os(libc::EEXIST, file_abs, "file exists"));
        }

        // English note: keep Python open() semantics explicit; creation is only allowed for x/w/a.
        if !exists && !mode.contains('x') && !mode.contains('w') && !mode.contains('a') {
            return Err(FsAgentError::os(libc::ENOENT, file_abs, "no such file"));
        }

        if st.size < 0 {
            return Err(FsAgentError::InvalidArgument {
                detail: format!("remote stat returned negative size: {}", st.size),
            });
        }
        if st.mtime_ns < 0 {
            return Err(FsAgentError::InvalidArgument {
                detail: format!("remote stat returned negative mtime_ns: {}", st.mtime_ns),
            });
        }

        // Apply remote open-time create/truncate semantics explicitly so later
        // write-session open does not need to mutate file shape.
        if mode.contains('w') {
            if current_identity.is_none() {
                current_identity = self.current_request_identity();
            }
            self.remote_truncate(
                export_name,
                &export,
                relpath_rpc.as_ref(),
                0,
                file_abs,
                current_identity.as_ref(),
            )?;
        }

        // Apply remote create semantics early for append/exclusive create.
        if mode.contains('a') && !exists {
            if current_identity.is_none() {
                current_identity = self.current_request_identity();
            }
            self.remote_truncate(
                export_name,
                &export,
                relpath_rpc.as_ref(),
                0,
                file_abs,
                current_identity.as_ref(),
            )?;
        }
        if mode.contains('x') && !exists {
            // Exclusive create: create an empty file at open-time.
            if current_identity.is_none() {
                current_identity = self.current_request_identity();
            }
            self.remote_truncate(
                export_name,
                &export,
                relpath_rpc.as_ref(),
                0,
                file_abs,
                current_identity.as_ref(),
            )?;
        }

        // English note:
        // - For remote mounted paths:
        //   - open() returns a Python-side file-like object (RemoteHandle).
        //   - file content IO happens at read/write time; reads may consult KV (piece cache) first.
        //
        // Keep "open sets initial position for append" in Python; we only return size metadata.
        let size = if mode.contains('w') || mode.contains('x') {
            0
        } else if mode.contains('a') && !st.exists {
            0
        } else {
            st.size
        };

        if let Some(plan) =
            self.same_host_direct_write_fd_plan(export_name, &export, relpath_rpc.as_ref(), mode)?
        {
            return Ok(plan);
        }

        if is_read_only {
            if let Some(entry) = cached.as_ref() {
                if let Some(sig) = entry.sig.as_ref() {
                    if st.exists
                        && st.is_file
                        && st.size >= 0
                        && (st.size as u64) <= export.inline_bytes_max_bytes
                        && (st.size as usize) <= REMOTE_CHUNK_BYTES
                    {
                        let current_identity_fingerprint =
                            self.current_request_identity_fingerprint();
                        let Some(identity_fingerprint) = current_identity_fingerprint.as_deref()
                        else {
                            return Ok(OpenPlan::RemoteHandle {
                                export_name: export_name.to_string(),
                                relpath: relpath_rpc.as_ref().to_string(),
                                size,
                                mtime_ns: st.mtime_ns,
                            });
                        };
                        if let Some(fd) = self.inline_fd_cache_lookup(
                            identity_fingerprint,
                            export_name,
                            relpath_rpc.as_ref(),
                            sig,
                        ) {
                            return Ok(OpenPlan::Fd {
                                fd,
                                size: st.size,
                                mtime_ns: st.mtime_ns,
                                export_name: None,
                                relpath: None,
                                upload_on_close: false,
                            });
                        }
                        let key = fluxon_fs_core::s3_gateway::kv_piece_key(
                            &export.cache_kv_key_prefix,
                            export_name,
                            relpath_rpc.as_ref(),
                            sig,
                            0,
                        );
                        if let Some(bytes) =
                            self.kv_try_get_bytes_field(&key, &export.cache_bytes_field_key)
                        {
                            if bytes.len() as i64 == st.size {
                                self.metadata_cache_store(
                                    identity_fingerprint,
                                    export_name,
                                    relpath_rpc.as_ref(),
                                    &st,
                                );
                                if let Some(plan) = self.inline_bytes_to_fd_plan(
                                    Some(identity_fingerprint),
                                    Some(export_name),
                                    Some(relpath_rpc.as_ref()),
                                    st.size,
                                    st.mtime_ns,
                                    &bytes,
                                ) {
                                    return Ok(plan);
                                }
                                return Ok(OpenPlan::Bytes(bytes));
                            }
                        }
                        if current_identity.is_none() {
                            current_identity = self.current_request_identity();
                        }
                        let (fresh_st, fresh_inline_bytes) = self.remote_open_read(
                            export_name,
                            &export,
                            relpath_rpc.as_ref(),
                            file_abs,
                            current_identity.as_ref(),
                        )?;
                        let current_identity_fingerprint =
                            self.current_request_identity_fingerprint();
                        if let Some(identity_fingerprint) = current_identity_fingerprint.as_deref()
                        {
                            self.metadata_cache_store(
                                identity_fingerprint,
                                export_name,
                                relpath_rpc.as_ref(),
                                &fresh_st,
                            );
                        }
                        if let Some(bytes) = fresh_inline_bytes.as_ref() {
                            if fresh_st.exists && fresh_st.is_file {
                                self.maybe_stage_inline_open_read_piece(
                                    export_name,
                                    &export,
                                    relpath_rpc.as_ref(),
                                    &fresh_st,
                                    bytes,
                                );
                                if let Some(plan) = self.inline_bytes_to_fd_plan(
                                    current_identity_fingerprint.as_deref(),
                                    Some(export_name),
                                    Some(relpath_rpc.as_ref()),
                                    fresh_st.size,
                                    fresh_st.mtime_ns,
                                    bytes,
                                ) {
                                    return Ok(plan);
                                }
                                return Ok(OpenPlan::Bytes(bytes.clone()));
                            }
                        }
                        if fresh_st.is_dir {
                            return Err(FsAgentError::os(libc::EISDIR, file_abs, "is a directory"));
                        }
                        if !fresh_st.exists {
                            return Err(FsAgentError::os(libc::ENOENT, file_abs, "no such file"));
                        }
                        return Ok(OpenPlan::RemoteHandle {
                            export_name: export_name.to_string(),
                            relpath: relpath_rpc.as_ref().to_string(),
                            size: fresh_st.size,
                            mtime_ns: fresh_st.mtime_ns,
                        });
                    }
                }
            }
            if st.exists && st.is_file && st.size >= REMOTE_DISK_CACHE_MIN_FILE_BYTES as i64 {
                if current_identity.is_none() {
                    current_identity = self.current_request_identity();
                }
                if let Some(plan) = self.large_file_disk_cache_fd_plan(
                    export_name,
                    relpath_rpc.as_ref(),
                    &st,
                    file_abs,
                    current_identity.as_ref(),
                )? {
                    return Ok(plan);
                }
            }
        }
        Ok(OpenPlan::RemoteHandle {
            export_name: export_name.to_string(),
            relpath: relpath_rpc.as_ref().to_string(),
            size,
            mtime_ns: st.mtime_ns,
        })
    }

    fn get_export_cfg(
        &self,
        export_name: &str,
    ) -> Result<(FluxonFsGlobalConfig, FluxonFsExport), FsAgentError> {
        let cfg_guard = self.cfg.read();
        let Some(cfg) = cfg_guard.as_ref() else {
            return Err(FsAgentError::InvalidArgument {
                detail: "fluxon_fs cache config is not loaded yet".to_string(),
            });
        };
        let exp = cfg
            .exports
            .get(export_name)
            .ok_or_else(|| FsAgentError::InvalidArgument {
                detail: format!("unknown export_name: {}", export_name),
            })?;
        Ok((cfg.clone(), exp.clone()))
    }

    fn match_mount(&self, file_abs: &str) -> Option<(String, String, String)> {
        let mounts = self.mounts.read();
        for m in mounts.iter() {
            if is_within_dir(file_abs, &m.mount_dir_abs) {
                let rel = safe_relpath_under_dir(file_abs, &m.mount_dir_abs)
                    .unwrap_or_else(|| "".to_string());
                return Some((m.mount_dir_abs.clone(), m.export_name.clone(), rel));
            }
        }
        None
    }

    fn current_request_identity(&self) -> Option<FluxonFsRequestIdentity> {
        self.request_identity
            .read()
            .as_ref()
            .map(|v| v.identity.clone())
    }

    fn current_request_identity_fingerprint(&self) -> Option<String> {
        self.request_identity
            .read()
            .as_ref()
            .map(|v| v.fingerprint.clone())
    }

    fn same_host_direct_write_fd_plan(
        &self,
        export_name: &str,
        export: &FluxonFsExport,
        relpath: &str,
        mode: &str,
    ) -> Result<Option<OpenPlan>, FsAgentError> {
        let is_write_only = is_write_mode(mode)
            && !mode.contains('r')
            && !mode.contains('+')
            && !mode.contains('t');
        if !is_write_only {
            return Ok(None);
        }

        let nodes: Vec<String> = match export.routing_mode {
            FluxonFsExportRoutingMode::StaticNodes => export.nodes.clone(),
            FluxonFsExportRoutingMode::AgentRegistry => self
                .export_nodes_get_or_refresh(export_name)
                .map_err(FsAgentError::Kv)?,
        };
        if nodes.len() != 1 {
            return Ok(None);
        }
        if !self.is_same_host_member(&nodes[0]) {
            return Ok(None);
        }

        let local_path = safe_join_export_root(export.remote_root_dir_abs.as_str(), relpath)?;
        let mut opts = OpenOptions::new();
        opts.write(true);
        if mode.contains('a') {
            opts.append(true).create(true);
        } else {
            opts.create(true);
        }
        let file = opts.open(&local_path).map_err(|e| FsAgentError::Io {
            path: local_path.display().to_string(),
            detail: e.to_string(),
        })?;
        let md = file.metadata().map_err(|e| FsAgentError::Io {
            path: local_path.display().to_string(),
            detail: e.to_string(),
        })?;
        let (size, mtime_sec, mtime_nsec) = fs_sig_from_metadata(&md);
        let mtime_ns = mtime_sec
            .saturating_mul(1_000_000_000)
            .saturating_add(mtime_nsec);
        Ok(Some(OpenPlan::Fd {
            fd: file.into(),
            size,
            mtime_ns,
            export_name: Some(export_name.to_string()),
            relpath: Some(relpath.to_string()),
            upload_on_close: true,
        }))
    }

    fn is_same_host_member(&self, node_id: &str) -> bool {
        let current_hostname = read_hostname_best_effort();
        let current_product_uuid = read_product_uuid_best_effort();
        if current_hostname.is_none() && current_product_uuid.is_none() {
            return false;
        }
        let members = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.api.membership_snapshot()
        })) {
            Ok(v) => v,
            Err(_) => return false,
        };
        let Some(member) = members.into_iter().find(|m| m.id.as_str() == node_id) else {
            return false;
        };
        let same_hostname = current_hostname.as_ref().is_some_and(|hostname| {
            member
                .metadata
                .get("hostname")
                .map(|v| v == hostname)
                .unwrap_or(false)
        });
        let same_product_uuid = current_product_uuid.as_ref().is_some_and(|uuid| {
            member
                .metadata
                .get("product_uuid")
                .map(|v| v == uuid)
                .unwrap_or(false)
        });
        same_hostname || same_product_uuid
    }

    fn identity_fingerprint(identity: &FluxonFsRequestIdentity) -> String {
        let mut hasher = Sha256::new();
        hasher.update(identity.username.as_bytes());
        hasher.update(b"\0");
        hasher.update(identity.password.as_bytes());
        hex::encode(hasher.finalize())
    }

    fn metadata_cache_key(
        identity_fingerprint: &str,
        export_name: &str,
        relpath: &str,
    ) -> RemoteMetadataCacheKey {
        RemoteMetadataCacheKey {
            identity_fingerprint: identity_fingerprint.to_string(),
            export_name: export_name.to_string(),
            relpath: relpath.to_string(),
        }
    }

    fn enqueue_metadata_invalidation(
        &self,
        export_name: &str,
        relpath: &str,
        scope: MetadataInvalidationScope,
    ) {
        let seq = self
            .metadata_invalidation_publish
            .latest_seq
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        let event = MetadataInvalidationEvent {
            export_name: export_name.to_string(),
            relpath: relpath.to_string(),
            scope,
            seq,
        };
        self.metadata_invalidation_publish
            .pending
            .lock()
            .push_back(event);
    }

    fn apply_metadata_invalidation_event(
        &self,
        export_name: &str,
        relpath: &str,
        scope: MetadataInvalidationScope,
        seq: u64,
    ) {
        let current = self
            .metadata_invalidation_state
            .latest_seq
            .load(Ordering::Acquire);
        if seq <= current {
            return;
        }
        match scope {
            MetadataInvalidationScope::Exact => {
                self.metadata_cache_invalidate_exact(export_name, relpath)
            }
            MetadataInvalidationScope::Prefix => {
                self.metadata_cache_invalidate_prefix(export_name, relpath)
            }
        }
        self.metadata_invalidation_state
            .latest_seq
            .store(seq, Ordering::Release);
    }

    fn apply_metadata_invalidation_state(&self, state: &FsMetadataInvalidationStateWire) {
        let current = self
            .metadata_invalidation_state
            .latest_seq
            .load(Ordering::Acquire);
        if state.latest_seq <= current {
            return;
        }
        let mut next_seq = current;
        for event in state.events.iter() {
            if event.seq <= current {
                continue;
            }
            let scope = match event.scope {
                FsMetadataInvalidationScopeWire::Exact => MetadataInvalidationScope::Exact,
                FsMetadataInvalidationScopeWire::Prefix => MetadataInvalidationScope::Prefix,
            };
            self.apply_metadata_invalidation_event(
                &event.export_name,
                &event.relpath,
                scope,
                event.seq,
            );
            next_seq = next_seq.max(event.seq);
        }
        if next_seq < state.latest_seq {
            let mut cache = self.remote_metadata_cache.write();
            let removed_bytes: usize = cache
                .values()
                .filter_map(|entry| entry.inline_fd.as_ref().map(|fd| fd.size_bytes))
                .sum();
            cache.clear();
            if removed_bytes > 0 {
                self.remote_open_cache_resident_bytes
                    .fetch_sub(removed_bytes, Ordering::Relaxed);
            }
            self.remote_open_cache_resident_bytes
                .store(0, Ordering::Relaxed);
            next_seq = state.latest_seq;
        }
        self.metadata_invalidation_state
            .latest_seq
            .store(next_seq, Ordering::Release);
    }

    fn flush_pending_metadata_invalidations(&self) {
        if self
            .metadata_invalidation_publish
            .flush_inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let api = self.api.clone();
        let master_cfg = self.master_cfg.clone();
        let pending = self.metadata_invalidation_publish.pending.clone();
        let inflight = self.metadata_invalidation_publish.flush_inflight.clone();
        std::mem::drop(self.rt_handle.spawn_blocking(move || {
            flush_pending_metadata_invalidations_blocking(api, master_cfg, pending);
            inflight.store(false, Ordering::Release);
        }));
    }

    fn metadata_cache_lookup(
        &self,
        identity_fingerprint: &str,
        export_name: &str,
        relpath: &str,
        ttl_ms: u64,
    ) -> Option<RemoteMetadataCacheEntry> {
        if ttl_ms == 0 {
            return None;
        }
        let now = now_unix_ms();
        let key = metadata_cache_lookup_key(identity_fingerprint, export_name, relpath);
        let cache = self.remote_metadata_cache.read();
        let entry = cache.get(&key)?;
        let latest_seq = self
            .metadata_invalidation_state
            .latest_seq
            .load(Ordering::Acquire);
        if entry.invalidation_seq < latest_seq {
            drop(cache);
            let mut cache = self.remote_metadata_cache.write();
            if let Some(existing) = cache.get(&key) {
                if existing.invalidation_seq < latest_seq {
                    cache.remove(&key);
                }
            }
            return None;
        }
        if now.saturating_sub(entry.authorized_at_ms) > ttl_ms {
            drop(cache);
            let mut cache = self.remote_metadata_cache.write();
            if let Some(existing) = cache.get(&key) {
                if now.saturating_sub(existing.authorized_at_ms) > ttl_ms {
                    cache.remove(&key);
                }
            }
            return None;
        }
        Some(entry.clone())
    }

    fn metadata_cache_store(
        &self,
        identity_fingerprint: &str,
        export_name: &str,
        relpath: &str,
        stat: &RemoteStat,
    ) {
        if !(stat.exists && stat.is_file && !stat.is_dir && stat.size >= 0 && stat.mtime_ns >= 0) {
            return;
        }
        let sig = Some(fluxon_fs_core::s3_gateway::object_sig_string(
            stat.size,
            stat.mtime_ns,
        ));
        let lookup_key = metadata_cache_lookup_key(identity_fingerprint, export_name, relpath);
        let authorized_at_ms = now_unix_ms();
        let invalidation_seq = self
            .metadata_invalidation_state
            .latest_seq
            .load(Ordering::Acquire);
        let mut cache = self.remote_metadata_cache.write();
        let prev_inline_fd = cache.get(&lookup_key).and_then(|entry| {
            if entry.sig == sig {
                entry.inline_fd.clone()
            } else {
                None
            }
        });
        if let Some(prev) = cache.get_mut(&lookup_key) {
            if prev.sig != sig {
                let removed = open_cache_take_inline_fd(prev);
                if removed > 0 {
                    self.remote_open_cache_resident_bytes
                        .fetch_sub(removed, Ordering::Relaxed);
                }
            }
            prev.stat = stat.clone();
            prev.sig = sig;
            prev.authorized_at_ms = authorized_at_ms;
            prev.invalidation_seq = invalidation_seq;
            prev.inline_fd = prev_inline_fd;
        } else {
            let key = Self::metadata_cache_key(identity_fingerprint, export_name, relpath);
            cache.insert(
                key,
                RemoteMetadataCacheEntry {
                    stat: stat.clone(),
                    sig,
                    authorized_at_ms,
                    invalidation_seq,
                    inline_fd: None,
                },
            );
        }
    }

    fn metadata_cache_invalidate_exact(&self, export_name: &str, relpath: &str) {
        let mut cache = self.remote_metadata_cache.write();
        let keys: Vec<RemoteMetadataCacheKey> = cache
            .keys()
            .filter(|key| metadata_cache_matches_exact(key, export_name, relpath))
            .cloned()
            .collect();
        for key in keys {
            if let Some(entry) = cache.remove(&key) {
                if let Some(fd) = entry.inline_fd {
                    self.remote_open_cache_resident_bytes
                        .fetch_sub(fd.size_bytes, Ordering::Relaxed);
                }
            }
        }
    }

    fn metadata_cache_invalidate_prefix(&self, export_name: &str, relpath_prefix: &str) {
        let mut cache = self.remote_metadata_cache.write();
        let keys: Vec<RemoteMetadataCacheKey> = cache
            .keys()
            .filter(|key| metadata_cache_matches_prefix(key, export_name, relpath_prefix))
            .cloned()
            .collect();
        for key in keys {
            if let Some(entry) = cache.remove(&key) {
                if let Some(fd) = entry.inline_fd {
                    self.remote_open_cache_resident_bytes
                        .fetch_sub(fd.size_bytes, Ordering::Relaxed);
                }
            }
        }
    }

    fn metadata_cache_invalidate_exact_and_publish(&self, export_name: &str, relpath: &str) {
        self.metadata_cache_invalidate_exact(export_name, relpath);
        self.enqueue_metadata_invalidation(export_name, relpath, MetadataInvalidationScope::Exact);
        self.flush_pending_metadata_invalidations();
    }

    fn metadata_cache_invalidate_prefix_and_publish(
        &self,
        export_name: &str,
        relpath_prefix: &str,
    ) {
        self.metadata_cache_invalidate_prefix(export_name, relpath_prefix);
        self.enqueue_metadata_invalidation(
            export_name,
            relpath_prefix,
            MetadataInvalidationScope::Prefix,
        );
        self.flush_pending_metadata_invalidations();
    }

    fn metadata_cache_invalidate_exact_with_identity(
        &self,
        identity: &FluxonFsRequestIdentity,
        export_name: &str,
        relpath: &str,
    ) {
        let _ = identity;
        self.metadata_cache_invalidate_exact_and_publish(export_name, relpath);
    }

    fn metadata_cache_invalidate_prefix_with_identity(
        &self,
        identity: &FluxonFsRequestIdentity,
        export_name: &str,
        relpath_prefix: &str,
    ) {
        let _ = identity;
        self.metadata_cache_invalidate_prefix_and_publish(export_name, relpath_prefix);
    }

    fn inline_bytes_to_fd_plan(
        &self,
        identity_fingerprint: Option<&str>,
        export_name: Option<&str>,
        relpath: Option<&str>,
        size: i64,
        mtime_ns: i64,
        bytes: &[u8],
    ) -> Option<OpenPlan> {
        if size < 0
            || mtime_ns < 0
            || bytes.len() as i64 != size
            || bytes.len() > REMOTE_CHUNK_BYTES
        {
            return None;
        }

        let fd = build_inline_memfd(bytes, "fluxon_fs_agent")?;
        if let (Some(identity_fingerprint), Some(export_name), Some(relpath)) =
            (identity_fingerprint, export_name, relpath)
        {
            let sig = fluxon_fs_core::s3_gateway::object_sig_string(size, mtime_ns);
            if let Ok(dup_fd) = reopen_owned_fd(&fd) {
                self.inline_fd_cache_store(
                    identity_fingerprint,
                    export_name,
                    relpath,
                    &sig,
                    fd,
                    bytes.len(),
                );
                return Some(OpenPlan::Fd {
                    fd: dup_fd,
                    size,
                    mtime_ns,
                    export_name: None,
                    relpath: None,
                    upload_on_close: false,
                });
            }
        }

        Some(OpenPlan::Fd {
            fd,
            size,
            mtime_ns,
            export_name: None,
            relpath: None,
            upload_on_close: false,
        })
    }

    fn large_file_disk_cache_fd_plan(
        &self,
        export_name: &str,
        relpath: &str,
        st: &RemoteStat,
        file_abs: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<Option<OpenPlan>, FsAgentError> {
        if !st.exists
            || !st.is_file
            || st.is_dir
            || st.size < REMOTE_DISK_CACHE_MIN_FILE_BYTES as i64
            || st.mtime_ns < 0
        {
            return Ok(None);
        }

        let cache = match self.ensure_remote_disk_cache() {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(
                    "skip remote disk cache: source={} export={} relpath={} err={}",
                    REMOTE_DISK_CACHE_METRICS_SOURCE,
                    export_name,
                    relpath,
                    err
                );
                return Ok(None);
            }
        };
        if !cache.should_cache(st.size as u64) {
            return Ok(None);
        }

        let cache_path = match cache.lookup(
            export_name,
            relpath,
            st.size as u64,
            st.mtime_ns as u64,
        ) {
            Ok(Some(path)) => path,
            Ok(None) => {
                let mut remote_err: Option<FsAgentError> = None;
                let materialized = cache.materialize(
                    export_name,
                    relpath,
                    st.size as u64,
                    st.mtime_ns as u64,
                    |tmp_path| {
                        let fill_res = self.fill_remote_disk_cache_file(
                            export_name,
                            relpath,
                            st,
                            file_abs,
                            request_identity,
                            tmp_path,
                        );
                        if let Err(err) = fill_res {
                            remote_err = Some(err);
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "remote disk cache fill failed",
                            ));
                        }
                        Ok(())
                    },
                );
                if let Some(err) = remote_err {
                    return Err(err);
                }
                match materialized {
                    Ok(path) => path,
                    Err(err) => {
                        tracing::warn!(
                            "skip remote disk cache materialize: source={} export={} relpath={} err={}",
                            REMOTE_DISK_CACHE_METRICS_SOURCE,
                            export_name,
                            relpath,
                            err
                        );
                        return Ok(None);
                    }
                }
            }
            Err(err) => {
                tracing::warn!(
                    "skip remote disk cache lookup: source={} export={} relpath={} err={}",
                    REMOTE_DISK_CACHE_METRICS_SOURCE,
                    export_name,
                    relpath,
                    err
                );
                return Ok(None);
            }
        };
        let fd = match cache.open_read_fd(&cache_path) {
            Ok(fd) => fd,
            Err(err) => {
                tracing::warn!(
                    "skip remote disk cache open: source={} export={} relpath={} path={} err={}",
                    REMOTE_DISK_CACHE_METRICS_SOURCE,
                    export_name,
                    relpath,
                    cache_path.display(),
                    err
                );
                return Ok(None);
            }
        };
        Ok(Some(OpenPlan::Fd {
            fd,
            size: st.size,
            mtime_ns: st.mtime_ns,
            export_name: None,
            relpath: None,
            upload_on_close: false,
        }))
    }

    fn fill_remote_disk_cache_file(
        &self,
        export_name: &str,
        relpath: &str,
        st: &RemoteStat,
        file_abs: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
        tmp_path: &Path,
    ) -> Result<(), FsAgentError> {
        let mut file = fs::File::create(tmp_path).map_err(|err| {
            FsAgentError::os(
                err.raw_os_error().unwrap_or(libc::EIO),
                tmp_path.display().to_string(),
                format!("create remote disk cache temp file failed: {}", err),
            )
        })?;
        let mut pos = 0i64;
        while pos < st.size {
            let want = std::cmp::min(REMOTE_DISK_CACHE_READ_CHUNK_BYTES as i64, st.size - pos);
            // Large-file disk cache fill must bypass KV piece cache. If it falls back to the
            // default stage_to_kv_then_read policy with allow_kv_cache=false, large reads fail
            // before the disk-cache materialization can even begin.
            let data = self.remote_read_chunk_by_handle_s3_with_identity(
                export_name,
                relpath,
                pos,
                want,
                st.size,
                st.mtime_ns,
                false,
                FluxonFsS3KvMissPolicy::RemoteRead,
                file_abs,
                request_identity,
            )?;
            if data.is_empty() {
                break;
            }
            file.write_all(&data).map_err(|err| {
                FsAgentError::os(
                    err.raw_os_error().unwrap_or(libc::EIO),
                    tmp_path.display().to_string(),
                    format!("write remote disk cache temp file failed: {}", err),
                )
            })?;
            pos += data.len() as i64;
        }
        if pos != st.size {
            return Err(FsAgentError::InvalidArgument {
                detail: format!(
                    "remote disk cache fill short read: export={} relpath={} expected={} got={}",
                    export_name, relpath, st.size, pos
                ),
            });
        }
        file.sync_all().map_err(|err| {
            FsAgentError::os(
                err.raw_os_error().unwrap_or(libc::EIO),
                tmp_path.display().to_string(),
                format!("sync remote disk cache temp file failed: {}", err),
            )
        })?;
        Ok(())
    }

    fn inline_fd_cache_lookup(
        &self,
        identity_fingerprint: &str,
        export_name: &str,
        relpath: &str,
        sig: &str,
    ) -> Option<OwnedFd> {
        let lookup_key = metadata_cache_lookup_key(identity_fingerprint, export_name, relpath);
        let cache = self.remote_metadata_cache.read();
        let entry = cache.get(&lookup_key)?;
        let inline_fd = entry.inline_fd.as_ref()?;
        if inline_fd.sig != sig {
            return None;
        }
        let next_seq = self
            .remote_open_cache_access_seq
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        inline_fd.last_access_seq.store(next_seq, Ordering::Relaxed);
        reopen_owned_fd(&inline_fd.fd).ok()
    }

    fn inline_fd_cache_store(
        &self,
        identity_fingerprint: &str,
        export_name: &str,
        relpath: &str,
        sig: &str,
        fd: OwnedFd,
        size_bytes: usize,
    ) {
        let lookup_key = metadata_cache_lookup_key(identity_fingerprint, export_name, relpath);
        let mut cache = self.remote_metadata_cache.write();
        let Some(entry) = cache.get_mut(&lookup_key) else {
            return;
        };
        if entry.sig.as_deref() != Some(sig) {
            return;
        }
        if let Some(prev) = entry.inline_fd.replace(Arc::new(RemoteInlineFdCacheEntry {
            fd,
            size_bytes,
            sig: sig.to_string(),
            last_access_seq: AtomicU64::new(
                self.remote_open_cache_access_seq
                    .fetch_add(1, Ordering::Relaxed)
                    + 1,
            ),
        })) {
            self.remote_open_cache_resident_bytes
                .fetch_sub(prev.size_bytes, Ordering::Relaxed);
        }
        self.remote_open_cache_resident_bytes
            .fetch_add(size_bytes, Ordering::Relaxed);
        while self
            .remote_open_cache_resident_bytes
            .load(Ordering::Relaxed)
            > REMOTE_INLINE_FD_CACHE_MAX_RESIDENT_BYTES
            || open_cache_fd_entry_count(&cache) > REMOTE_INLINE_FD_CACHE_MAX_ENTRIES
        {
            let Some(evict_key) = cache
                .iter()
                .filter_map(|(k, entry)| {
                    let fd = entry.inline_fd.as_ref()?;
                    Some((k.clone(), fd.last_access_seq.load(Ordering::Relaxed)))
                })
                .min_by_key(|(_, seq)| *seq)
                .map(|(k, _)| k)
            else {
                break;
            };
            if let Some(entry) = cache.get_mut(&evict_key) {
                if let Some(fd) = entry.inline_fd.take() {
                    self.remote_open_cache_resident_bytes
                        .fetch_sub(fd.size_bytes, Ordering::Relaxed);
                }
            }
        }
    }

    fn request_payload_with_identity(
        &self,
        payload: FlatDict,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<FlatDict, FsAgentError> {
        self.request_payload_with_identity_and_internal_multipart(payload, request_identity, false)
    }

    fn request_payload_with_identity_and_internal_multipart(
        &self,
        mut payload: FlatDict,
        request_identity: Option<&FluxonFsRequestIdentity>,
        allow_s3_internal_multipart: bool,
    ) -> Result<FlatDict, FsAgentError> {
        let identity = match request_identity {
            Some(v) => Some(v.clone()),
            None => self.current_request_identity(),
        };
        if let Some(identity) = identity {
            let now_unix_ms_i64 = now_unix_ms().min(i64::MAX as u64) as i64;
            let token = build_rpc_token(&identity, now_unix_ms_i64).map_err(|e| {
                FsAgentError::InvalidArgument {
                    detail: format!("build fs rpc token failed: {}", e),
                }
            })?;
            payload.insert(
                FLUXON_FS_RPC_TOKEN_PAYLOAD_KEY.to_string(),
                FlatValue::String(token),
            );
        }
        if allow_s3_internal_multipart {
            payload.insert(
                FS_S3_INTERNAL_MULTIPART_PAYLOAD_KEY.to_string(),
                FlatValue::Bool(true),
            );
        }
        Ok(payload)
    }

    fn remote_next_node(&self, export_name: &str, nodes: &[String]) -> String {
        let mut rr = self.export_rr.lock();
        let idx = rr.get(export_name).copied().unwrap_or(0);
        let node = nodes[idx % nodes.len()].clone();
        rr.insert(export_name.to_string(), idx + 1);
        node
    }

    fn remote_rpc_call_forever(
        &self,
        export_name: &str,
        export: &FluxonFsExport,
        rpc_path: &str,
        payload: &FlatDict,
        op_desc: &str,
    ) -> KvResult<FlatDict> {
        self.remote_rpc_call_forever_with_timeout(
            export_name,
            export,
            rpc_path,
            payload,
            op_desc,
            None,
        )
    }

    fn remote_rpc_call_forever_with_timeout(
        &self,
        export_name: &str,
        export: &FluxonFsExport,
        rpc_path: &str,
        payload: &FlatDict,
        op_desc: &str,
        timeout_ms: Option<u64>,
    ) -> KvResult<FlatDict> {
        let backoff = BackoffConfig {
            initial_secs: 5,
            max_secs: 30,
        };
        let warn_cfg = WarnConfig {
            warn_interval_secs: DEFAULT_WARN_INTERVAL_SECS,
        };
        let mut last_warn: Option<Instant> = None;
        let mut attempt: u32 = 0;

        loop {
            if !self.shutdown_poller_is_running() {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!("fluxon_fs stopped by framework shutdown: op={}", op_desc),
                }));
            }

            let nodes_v: Vec<String>;
            let nodes: &[String] = match export.routing_mode {
                FluxonFsExportRoutingMode::StaticNodes => export.nodes.as_slice(),
                FluxonFsExportRoutingMode::AgentRegistry => {
                    nodes_v = self.export_nodes_get_or_refresh(export_name)?;
                    nodes_v.as_slice()
                }
            };
            if nodes.is_empty() {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "export has no available nodes: export={} op={}",
                        export_name, op_desc
                    ),
                }));
            }

            let node_id = self.remote_next_node(export_name, nodes);
            let res = self
                .api
                .rpc_client()
                .call(&node_id, rpc_path, payload.clone(), timeout_ms);
            match res {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if is_node_not_found(&e) {
                        if export.routing_mode == FluxonFsExportRoutingMode::AgentRegistry {
                            // English note:
                            // - AgentRegistry is dynamic (fs agents register online exports to master).
                            // - NodeNotFound usually means the registry is stale (agent crashed / left).
                            // - Drop the cached snapshot so the next attempt refreshes from master immediately.
                            let mut cache = self.export_nodes_cache.lock();
                            cache.nodes.remove(export_name);
                        }
                        let now = Instant::now();
                        if should_warn(now, &mut last_warn, warn_cfg) {
                            tracing::warn!(
                                "fluxon_fs remote rpc node not found; retry next node: op={} export={} node={} path={} err={}",
                                op_desc,
                                export_name,
                                node_id,
                                rpc_path,
                                e
                            );
                        }
                        if nodes.len() > 1 {
                            continue;
                        }
                        return Err(e);
                    }
                    if is_network_err(&e) {
                        let now = Instant::now();
                        if should_warn(now, &mut last_warn, warn_cfg) {
                            tracing::warn!(
                                "fluxon_fs remote rpc failed; blocking retry: op={} export={} node={} path={} err={}",
                                op_desc,
                                export_name,
                                node_id,
                                rpc_path,
                                e
                            );
                        }
                        let delay = next_backoff(backoff, attempt);
                        attempt = attempt.saturating_add(1);
                        if let Err(se) = self
                            .shutdown_aware_sleep(delay, "remote_rpc_call_forever.backoff_sleep")
                        {
                            return Err(KvError::Api(ApiError::Unknown {
                                detail: se.to_string(),
                            }));
                        }
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    fn remote_rpc_call_forever_on_node_with_timeout(
        &self,
        node_id: &str,
        rpc_path: &str,
        payload: &FlatDict,
        op_desc: &str,
        timeout_ms: Option<u64>,
    ) -> KvResult<FlatDict> {
        let backoff = BackoffConfig {
            initial_secs: 5,
            max_secs: 30,
        };
        let warn_cfg = WarnConfig {
            warn_interval_secs: DEFAULT_WARN_INTERVAL_SECS,
        };
        let mut last_warn: Option<Instant> = None;
        let mut attempt: u32 = 0;
        loop {
            if !self.shutdown_poller_is_running() {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!("fluxon_fs stopped by framework shutdown: op={}", op_desc),
                }));
            }
            let res = self
                .api
                .rpc_client()
                .call(node_id, rpc_path, payload.clone(), timeout_ms);
            match res {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if is_network_err(&e) || is_node_not_found(&e) {
                        let now = Instant::now();
                        if should_warn(now, &mut last_warn, warn_cfg) {
                            tracing::warn!(
                                "fluxon_fs remote rpc to fixed node failed; blocking retry: op={} node={} path={} err={}",
                                op_desc,
                                node_id,
                                rpc_path,
                                e
                            );
                        }
                        let delay = next_backoff(backoff, attempt);
                        attempt = attempt.saturating_add(1);
                        if let Err(se) = self.shutdown_aware_sleep(
                            delay,
                            "remote_rpc_call_forever_on_node_with_timeout.backoff_sleep",
                        ) {
                            return Err(KvError::Api(ApiError::Unknown {
                                detail: se.to_string(),
                            }));
                        }
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    fn remote_pick_node_forever(
        &self,
        export_name: &str,
        export: &FluxonFsExport,
        op_desc: &str,
    ) -> KvResult<String> {
        let nodes_v: Vec<String>;
        let nodes: &[String] = match export.routing_mode {
            FluxonFsExportRoutingMode::StaticNodes => export.nodes.as_slice(),
            FluxonFsExportRoutingMode::AgentRegistry => {
                nodes_v = self.export_nodes_get_or_refresh(export_name)?;
                nodes_v.as_slice()
            }
        };
        if nodes.is_empty() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "export has no available nodes: export={} op={}",
                    export_name, op_desc
                ),
            }));
        }
        Ok(self.remote_next_node(export_name, nodes))
    }

    fn remote_stat(
        &self,
        export_name: &str,
        export: &FluxonFsExport,
        relpath: &str,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<RemoteStat, FsAgentError> {
        self.remote_stat_with_internal_multipart(
            export_name,
            export,
            relpath,
            path_for_err,
            request_identity,
            false,
        )
    }

    fn remote_stat_with_internal_multipart(
        &self,
        export_name: &str,
        export: &FluxonFsExport,
        relpath: &str,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
        allow_s3_internal_multipart: bool,
    ) -> Result<RemoteStat, FsAgentError> {
        let payload: FlatDict = FlatDict::from([
            (
                "export".to_string(),
                FlatValue::String(export_name.to_string()),
            ),
            (
                "relpath".to_string(),
                FlatValue::String(relpath.to_string()),
            ),
        ]);
        let payload = self.request_payload_with_identity_and_internal_multipart(
            payload,
            request_identity,
            allow_s3_internal_multipart && is_internal_multipart_relpath(relpath),
        )?;
        let resp = self.remote_rpc_call_forever(
            export_name,
            export,
            &export.rpc_paths.stat,
            &payload,
            "stat",
        )?;

        let ok = match resp.get("ok") {
            Some(FlatValue::Bool(v)) => *v,
            _ => {
                return Err(FsAgentError::InvalidArgument {
                    detail: "remote stat response missing ok".to_string(),
                });
            }
        };
        if !ok {
            return Err(err_from_resp(&resp, path_for_err));
        }
        Ok(RemoteStat {
            exists: matches!(resp.get("exists"), Some(FlatValue::Bool(true))),
            is_file: matches!(resp.get("is_file"), Some(FlatValue::Bool(true))),
            is_dir: matches!(resp.get("is_dir"), Some(FlatValue::Bool(true))),
            size: get_i64(&resp, "size").unwrap_or(0),
            mtime_ns: get_i64(&resp, "mtime_ns").unwrap_or(0),
            mode: get_i64(&resp, "mode").unwrap_or(0),
        })
    }

    fn remote_open_read(
        &self,
        export_name: &str,
        export: &FluxonFsExport,
        relpath: &str,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<(RemoteStat, Option<Vec<u8>>), FsAgentError> {
        let payload: FlatDict = FlatDict::from([
            (
                "export".to_string(),
                FlatValue::String(export_name.to_string()),
            ),
            (
                "relpath".to_string(),
                FlatValue::String(relpath.to_string()),
            ),
        ]);
        let payload = self.request_payload_with_identity(payload, request_identity)?;
        let resp = self.remote_rpc_call_forever(
            export_name,
            export,
            &export.rpc_paths.open_read,
            &payload,
            "open_read",
        )?;

        let ok = match resp.get("ok") {
            Some(FlatValue::Bool(v)) => *v,
            _ => {
                return Err(FsAgentError::InvalidArgument {
                    detail: "remote open_read response missing ok".to_string(),
                });
            }
        };
        if !ok {
            return Err(err_from_resp(&resp, path_for_err));
        }
        let stat = RemoteStat {
            exists: matches!(resp.get("exists"), Some(FlatValue::Bool(true))),
            is_file: matches!(resp.get("is_file"), Some(FlatValue::Bool(true))),
            is_dir: matches!(resp.get("is_dir"), Some(FlatValue::Bool(true))),
            size: get_i64(&resp, "size").unwrap_or(0),
            mtime_ns: get_i64(&resp, "mtime_ns").unwrap_or(0),
            mode: get_i64(&resp, "mode").unwrap_or(0),
        };
        let inline_bytes = match resp.get("data") {
            Some(FlatValue::Bytes(b)) => Some(b.clone()),
            _ => None,
        };
        Ok((stat, inline_bytes))
    }

    fn remote_open_write_session(
        &self,
        export_name: &str,
        export: &FluxonFsExport,
        relpath: &str,
        path_for_err: &str,
        fs_rpc_token: Option<String>,
        allow_s3_internal_multipart: bool,
    ) -> Result<(String, String, i64, i64), FsAgentError> {
        let req = FsOpenWriteSessionReq {
            export: export_name.to_string(),
            relpath: relpath.to_string(),
            fs_rpc_token,
            allow_s3_internal_multipart,
        };
        let node_id = self.remote_pick_node_forever(export_name, export, "open_write_session")?;
        let node_id_for_resp = node_id.clone();
        let kv_framework = self.kv_framework.clone();
        let timeout = std::time::Duration::from_millis(REMOTE_WRITE_SESSION_CONTROL_RPC_TIMEOUT_MS);
        let resp = self
            .rt_handle
            .run_async_from_sync(async move {
                write_session_rpc::call_open_write_session(
                    kv_framework.as_ref(),
                    node_id.into(),
                    req,
                    Some(timeout),
                )
                .await
            })
            .map_err(|e| FsAgentError::InvalidArgument {
                detail: format!("open_write_session async bridge failed: {}", e),
            })??;

        if !resp.ok {
            return Err(FsAgentError::Io {
                path: path_for_err.to_string(),
                detail: if resp.err_detail.trim().is_empty() {
                    "remote open_write_session failed".to_string()
                } else {
                    resp.err_detail
                },
            });
        }
        if resp.session_id.trim().is_empty() {
            return Err(FsAgentError::InvalidArgument {
                detail: "remote open_write_session response missing session_id".to_string(),
            });
        }
        Ok((node_id_for_resp, resp.session_id, resp.size, resp.mtime_ns))
    }

    fn maybe_stage_inline_open_read_piece(
        &self,
        export_name: &str,
        export: &FluxonFsExport,
        relpath: &str,
        stat: &RemoteStat,
        bytes: &[u8],
    ) {
        if export.cache_kv_key_prefix.trim().is_empty()
            || export.cache_bytes_field_key.trim().is_empty()
            || stat.size < 0
            || stat.mtime_ns < 0
            || bytes.len() as i64 != stat.size
            || bytes.len() > REMOTE_CHUNK_BYTES
        {
            return;
        }
        let sig = fluxon_fs_core::s3_gateway::object_sig_string(stat.size, stat.mtime_ns);
        let key = fluxon_fs_core::s3_gateway::kv_piece_key(
            &export.cache_kv_key_prefix,
            export_name,
            relpath,
            &sig,
            0,
        );
        let mut v = FlatDict::new();
        v.insert(
            export.cache_bytes_field_key.clone(),
            FlatValue::Bytes(bytes.to_vec()),
        );
        if export.async_backfill_enabled {
            let api = self.api.clone();
            std::mem::drop(self.rt_handle.spawn(async move {
                let _ = spawn_blocking_allow_sync_async_bridge(move || {
                    if let Err(e) = api.kv().put(&key, v) {
                        tracing::debug!(
                            key = %key,
                            err = %e,
                            "fluxon_fs async inline open_read backfill put failed (best-effort)"
                        );
                    }
                })
                .await;
            }));
            return;
        }
        let _ = self.api.kv().put(&key, v);
    }

    fn remote_list_dir(
        &self,
        export_name: &str,
        export: &FluxonFsExport,
        relpath: &str,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<Vec<FlatDict>, FsAgentError> {
        let payload: FlatDict = FlatDict::from([
            (
                "export".to_string(),
                FlatValue::String(export_name.to_string()),
            ),
            (
                "relpath".to_string(),
                FlatValue::String(relpath.to_string()),
            ),
        ]);
        let payload = self.request_payload_with_identity(payload, request_identity)?;
        let resp = self.remote_rpc_call_forever(
            export_name,
            export,
            &export.rpc_paths.list_dir,
            &payload,
            "list_dir",
        )?;
        let ok = match resp.get("ok") {
            Some(FlatValue::Bool(v)) => *v,
            _ => false,
        };
        if !ok {
            return Err(err_from_resp(&resp, path_for_err));
        }
        let entries_json = match resp.get("entries_json") {
            Some(FlatValue::String(s)) => s,
            _ => {
                return Err(FsAgentError::InvalidArgument {
                    detail: "list_dir response missing entries_json".to_string(),
                });
            }
        };
        let v: serde_json::Value =
            serde_json::from_str(entries_json).map_err(|e| FsAgentError::InvalidArgument {
                detail: format!("entries_json parse failed: {}", e),
            })?;
        let Some(items) = v.as_array() else {
            return Err(FsAgentError::InvalidArgument {
                detail: "entries_json must decode to list".to_string(),
            });
        };

        let mut out: Vec<FlatDict> = Vec::new();
        for it in items {
            let Some(obj) = it.as_object() else {
                continue;
            };
            let mut d: FlatDict = FlatDict::new();
            if let Some(name) = obj.get("name").and_then(|x| x.as_str()) {
                d.insert("name".to_string(), FlatValue::String(name.to_string()));
            }
            if let Some(b) = obj.get("is_file").and_then(|x| x.as_bool()) {
                d.insert("is_file".to_string(), FlatValue::Bool(b));
            }
            if let Some(b) = obj.get("is_dir").and_then(|x| x.as_bool()) {
                d.insert("is_dir".to_string(), FlatValue::Bool(b));
            }
            out.push(d);
        }
        Ok(out)
    }

    fn remote_read_chunk(
        &self,
        export_name: &str,
        export: &FluxonFsExport,
        relpath: &str,
        offset: i64,
        length: i64,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<Vec<u8>, FsAgentError> {
        self.remote_read_chunk_with_internal_multipart(
            export_name,
            export,
            relpath,
            offset,
            length,
            path_for_err,
            request_identity,
            false,
        )
    }

    fn remote_read_chunk_with_internal_multipart(
        &self,
        export_name: &str,
        export: &FluxonFsExport,
        relpath: &str,
        offset: i64,
        length: i64,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
        allow_s3_internal_multipart: bool,
    ) -> Result<Vec<u8>, FsAgentError> {
        let payload: FlatDict = FlatDict::from([
            (
                "export".to_string(),
                FlatValue::String(export_name.to_string()),
            ),
            (
                "relpath".to_string(),
                FlatValue::String(relpath.to_string()),
            ),
            ("offset".to_string(), FlatValue::Int64(offset)),
            ("length".to_string(), FlatValue::Int64(length)),
        ]);
        let payload = self.request_payload_with_identity_and_internal_multipart(
            payload,
            request_identity,
            allow_s3_internal_multipart && is_internal_multipart_relpath(relpath),
        )?;
        let resp = self.remote_rpc_call_forever(
            export_name,
            export,
            &export.rpc_paths.read_chunk,
            &payload,
            "read_chunk",
        )?;
        let ok = match resp.get("ok") {
            Some(FlatValue::Bool(v)) => *v,
            _ => false,
        };
        if !ok {
            return Err(err_from_resp(&resp, path_for_err));
        }
        let data = match resp.get("data") {
            Some(FlatValue::Bytes(b)) => b.clone(),
            _ => {
                return Err(FsAgentError::InvalidArgument {
                    detail: "read_chunk response missing data".to_string(),
                });
            }
        };
        Ok(data)
    }

    fn remote_write_chunk(
        &self,
        export_name: &str,
        export: &FluxonFsExport,
        relpath: &str,
        offset: i64,
        data: Vec<u8>,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<(), FsAgentError> {
        self.remote_write_chunk_with_internal_multipart(
            export_name,
            export,
            relpath,
            offset,
            data,
            path_for_err,
            request_identity,
            false,
        )
    }

    fn remote_write_chunk_with_internal_multipart(
        &self,
        export_name: &str,
        export: &FluxonFsExport,
        relpath: &str,
        offset: i64,
        data: Vec<u8>,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
        allow_s3_internal_multipart: bool,
    ) -> Result<(), FsAgentError> {
        let payload: FlatDict = FlatDict::from([
            (
                "export".to_string(),
                FlatValue::String(export_name.to_string()),
            ),
            (
                "relpath".to_string(),
                FlatValue::String(relpath.to_string()),
            ),
            ("offset".to_string(), FlatValue::Int64(offset)),
            ("data".to_string(), FlatValue::Bytes(data)),
        ]);
        let payload = self.request_payload_with_identity_and_internal_multipart(
            payload,
            request_identity,
            allow_s3_internal_multipart && is_internal_multipart_relpath(relpath),
        )?;
        let resp = self.remote_rpc_call_forever(
            export_name,
            export,
            &export.rpc_paths.write_chunk,
            &payload,
            "write_chunk",
        )?;
        let ok = match resp.get("ok") {
            Some(FlatValue::Bool(v)) => *v,
            _ => false,
        };
        if !ok {
            return Err(err_from_resp(&resp, path_for_err));
        }
        self.metadata_cache_invalidate_exact_and_publish(export_name, relpath);
        Ok(())
    }

    fn remote_truncate(
        &self,
        export_name: &str,
        export: &FluxonFsExport,
        relpath: &str,
        size: i64,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<(), FsAgentError> {
        self.remote_truncate_with_internal_multipart(
            export_name,
            export,
            relpath,
            size,
            path_for_err,
            request_identity,
            false,
        )
    }

    fn remote_truncate_with_internal_multipart(
        &self,
        export_name: &str,
        export: &FluxonFsExport,
        relpath: &str,
        size: i64,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
        allow_s3_internal_multipart: bool,
    ) -> Result<(), FsAgentError> {
        let payload: FlatDict = FlatDict::from([
            (
                "export".to_string(),
                FlatValue::String(export_name.to_string()),
            ),
            (
                "relpath".to_string(),
                FlatValue::String(relpath.to_string()),
            ),
            ("size".to_string(), FlatValue::Int64(size)),
        ]);
        let payload = self.request_payload_with_identity_and_internal_multipart(
            payload,
            request_identity,
            allow_s3_internal_multipart && is_internal_multipart_relpath(relpath),
        )?;
        let resp = self.remote_rpc_call_forever(
            export_name,
            export,
            &export.rpc_paths.truncate,
            &payload,
            "truncate",
        )?;
        let ok = match resp.get("ok") {
            Some(FlatValue::Bool(v)) => *v,
            _ => false,
        };
        if !ok {
            return Err(err_from_resp(&resp, path_for_err));
        }
        self.metadata_cache_invalidate_exact_and_publish(export_name, relpath);
        Ok(())
    }

    fn remote_call_ok(
        &self,
        export_name: &str,
        export: &FluxonFsExport,
        rpc_path: &str,
        payload: FlatDict,
        op: &str,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
    ) -> Result<(), FsAgentError> {
        self.remote_call_ok_with_internal_multipart(
            export_name,
            export,
            rpc_path,
            payload,
            op,
            path_for_err,
            request_identity,
            false,
        )
    }

    fn remote_call_ok_with_internal_multipart(
        &self,
        export_name: &str,
        export: &FluxonFsExport,
        rpc_path: &str,
        payload: FlatDict,
        op: &str,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
        allow_s3_internal_multipart: bool,
    ) -> Result<(), FsAgentError> {
        self.remote_call_ok_with_internal_multipart_and_timeout(
            export_name,
            export,
            rpc_path,
            payload,
            op,
            path_for_err,
            request_identity,
            allow_s3_internal_multipart,
            None,
        )
    }

    fn remote_call_ok_with_internal_multipart_and_timeout(
        &self,
        export_name: &str,
        export: &FluxonFsExport,
        rpc_path: &str,
        payload: FlatDict,
        op: &str,
        path_for_err: &str,
        request_identity: Option<&FluxonFsRequestIdentity>,
        allow_s3_internal_multipart: bool,
        timeout_ms: Option<u64>,
    ) -> Result<(), FsAgentError> {
        let payload = self.request_payload_with_identity_and_internal_multipart(
            payload,
            request_identity,
            allow_s3_internal_multipart,
        )?;
        let resp = self.remote_rpc_call_forever_with_timeout(
            export_name,
            export,
            rpc_path,
            &payload,
            op,
            timeout_ms,
        )?;
        let ok = match resp.get("ok") {
            Some(FlatValue::Bool(v)) => *v,
            _ => false,
        };
        if ok {
            return Ok(());
        }
        Err(err_from_resp(&resp, path_for_err))
    }

    fn remote_call_ok_on_node_with_timeout(
        &self,
        node_id: &str,
        rpc_path: &str,
        payload: FlatDict,
        op: &str,
        path_for_err: &str,
        fs_rpc_token: Option<String>,
        allow_s3_internal_multipart: bool,
        timeout_ms: Option<u64>,
    ) -> Result<(), FsAgentError> {
        let mut payload = payload;
        if let Some(token) = fs_rpc_token {
            payload.insert(
                FLUXON_FS_RPC_TOKEN_PAYLOAD_KEY.to_string(),
                FlatValue::String(token),
            );
        }
        if allow_s3_internal_multipart {
            payload.insert(
                FS_S3_INTERNAL_MULTIPART_PAYLOAD_KEY.to_string(),
                FlatValue::Bool(true),
            );
        }
        let resp = self.remote_rpc_call_forever_on_node_with_timeout(
            node_id, rpc_path, &payload, op, timeout_ms,
        )?;
        let ok = match resp.get("ok") {
            Some(FlatValue::Bool(v)) => *v,
            _ => false,
        };
        if ok {
            return Ok(());
        }
        Err(err_from_resp(&resp, path_for_err))
    }

    fn cache_try_get(
        &self,
        kv_key: &str,
        bytes_field_key: &str,
    ) -> Option<(Vec<u8>, (i64, i64, i64))> {
        let got = match self.api.kv().get(kv_key) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    "fluxon_fs cache get failed (best-effort): key={} err={}",
                    kv_key,
                    e
                );
                return None;
            }
        };
        let Some(d) = got else {
            return None;
        };
        let size = get_i64(&d, "fs.size")?;
        let msec = get_i64(&d, "fs.mtime_sec")?;
        let mnsec = get_i64(&d, "fs.mtime_nsec")?;
        let payload = match d.get(bytes_field_key) {
            Some(FlatValue::Bytes(b)) => b.clone(),
            _ => return None,
        };
        Some((payload, (size, msec, mnsec)))
    }

    fn kv_try_get_bytes_field(&self, kv_key: &str, bytes_field_key: &str) -> Option<Vec<u8>> {
        let got = match self.api.kv().get(kv_key) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    "fluxon_fs kv get failed (best-effort): key={} err={}",
                    kv_key,
                    e
                );
                return None;
            }
        };
        let Some(d) = got else {
            return None;
        };
        match d.get(bytes_field_key) {
            Some(FlatValue::Bytes(b)) => Some(b.clone()),
            _ => None,
        }
    }

    fn cache_try_put(
        &self,
        kv_key: &str,
        bytes_field_key: &str,
        data: &[u8],
        sig: (i64, i64, i64),
    ) -> bool {
        let mut value: FlatDict = FlatDict::new();
        value.insert("fs.size".to_string(), FlatValue::Int64(sig.0));
        value.insert("fs.mtime_sec".to_string(), FlatValue::Int64(sig.1));
        value.insert("fs.mtime_nsec".to_string(), FlatValue::Int64(sig.2));
        value.insert(
            "cache.write_unix_ms".to_string(),
            FlatValue::Int64(now_unix_ms() as i64),
        );
        value.insert("cache.schema_version".to_string(), FlatValue::Int64(1));
        value.insert(bytes_field_key.to_string(), FlatValue::Bytes(data.to_vec()));

        match self.api.kv().put(kv_key, value) {
            Ok(()) => true,
            Err(e) => {
                tracing::warn!(
                    "fluxon_fs cache put failed (best-effort): key={} err={}",
                    kv_key,
                    e
                );
                false
            }
        }
    }
}

fn stage_piece_to_kv_via_runtime(
    runtime: &AgentRemoteRuntime,
    piece_key: &PieceKey,
    identity: Option<&FluxonFsRequestIdentity>,
) -> Result<(), String> {
    if !runtime.shutdown_poller_is_running() {
        return Err("shutdown in progress".to_string());
    }
    let export = runtime.get_export_cfg(&piece_key.export)?;
    let nodes: Vec<String> = match export.routing_mode {
        FluxonFsExportRoutingMode::StaticNodes => export.nodes.clone(),
        FluxonFsExportRoutingMode::AgentRegistry => {
            runtime.export_nodes_get_or_refresh(&piece_key.export)?
        }
    };
    if nodes.is_empty() {
        return Err(format!(
            "export has no available nodes: export={}",
            piece_key.export
        ));
    }
    let node_id = runtime.remote_next_node(&piece_key.export, &nodes);
    let mut payload: FlatDict = FlatDict::from([
        (
            "export".to_string(),
            FlatValue::String(piece_key.export.clone()),
        ),
        (
            "relpath".to_string(),
            FlatValue::String(piece_key.relpath.clone()),
        ),
        ("sig".to_string(), FlatValue::String(piece_key.sig.clone())),
        (
            "piece_idx".to_string(),
            FlatValue::Int64(piece_key.piece_idx),
        ),
    ]);
    if let Some(identity) = identity {
        let now_unix_ms_i64 = now_unix_ms().min(i64::MAX as u64) as i64;
        let token = build_rpc_token(identity, now_unix_ms_i64)
            .map_err(|e| format!("build fs rpc token failed: {}", e))?;
        payload.insert(
            FLUXON_FS_RPC_TOKEN_PAYLOAD_KEY.to_string(),
            FlatValue::String(token),
        );
    }
    let resp = runtime
        .api
        .rpc_client()
        .call(
            &node_id,
            fluxon_fs_core::s3_gateway::FS_S3_LOAD_PART_FILE_TO_KV_RPC_PATH,
            payload,
            None,
        )
        .map_err(|e| e.to_string())?;
    match resp.get("ok") {
        Some(FlatValue::Bool(true)) => Ok(()),
        _ => Err(format!("s3_load_part_file_to_kv failed: {:?}", resp)),
    }
}

fn stage_piece_range_to_kv_via_runtime(
    runtime: &AgentRemoteRuntime,
    piece_key: &PieceKey,
    piece_count: usize,
    identity: Option<&FluxonFsRequestIdentity>,
) -> Result<(), String> {
    if piece_count <= 1 {
        return stage_piece_to_kv_via_runtime(runtime, piece_key, identity);
    }
    if !runtime.shutdown_poller_is_running() {
        return Err("shutdown in progress".to_string());
    }
    let export = runtime.get_export_cfg(&piece_key.export)?;
    let nodes: Vec<String> = match export.routing_mode {
        FluxonFsExportRoutingMode::StaticNodes => export.nodes.clone(),
        FluxonFsExportRoutingMode::AgentRegistry => {
            runtime.export_nodes_get_or_refresh(&piece_key.export)?
        }
    };
    if nodes.is_empty() {
        return Err(format!(
            "export has no available nodes: export={}",
            piece_key.export
        ));
    }
    let node_id = runtime.remote_next_node(&piece_key.export, &nodes);
    let mut payload: FlatDict = FlatDict::from([
        (
            "export".to_string(),
            FlatValue::String(piece_key.export.clone()),
        ),
        (
            "relpath".to_string(),
            FlatValue::String(piece_key.relpath.clone()),
        ),
        ("sig".to_string(), FlatValue::String(piece_key.sig.clone())),
        (
            "start_piece_idx".to_string(),
            FlatValue::Int64(piece_key.piece_idx),
        ),
        (
            "piece_count".to_string(),
            FlatValue::Int64(piece_count as i64),
        ),
    ]);
    if let Some(identity) = identity {
        let now_unix_ms_i64 = now_unix_ms().min(i64::MAX as u64) as i64;
        let token = build_rpc_token(identity, now_unix_ms_i64)
            .map_err(|e| format!("build fs rpc token failed: {}", e))?;
        payload.insert(
            FLUXON_FS_RPC_TOKEN_PAYLOAD_KEY.to_string(),
            FlatValue::String(token),
        );
    }
    let resp = runtime
        .api
        .rpc_client()
        .call(
            &node_id,
            fluxon_fs_core::s3_gateway::FS_S3_LOAD_PART_FILE_RANGE_TO_KV_RPC_PATH,
            payload,
            None,
        )
        .map_err(|e| e.to_string())?;
    match resp.get("ok") {
        Some(FlatValue::Bool(true)) => Ok(()),
        _ => Err(format!("s3_load_part_file_range_to_kv failed: {:?}", resp)),
    }
}

fn is_network_err(e: &KvError) -> bool {
    matches!(e, KvError::P2p(_))
}

fn is_node_not_found(e: &KvError) -> bool {
    use fluxon_kv::rpcresp_kvresult_convert::msg_and_error::P2pError;
    match e {
        KvError::P2p(P2pError::NodeNotFound { .. }) => true,
        KvError::P2p(P2pError::RPCCallFailed { err, .. }) => {
            // English note:
            // - In practice NodeNotFound may be wrapped by `RPCCallFailed` (stringified inner error),
            //   e.g. err="NodeNotFound { node: \"...\" }".
            // - Treat it as node-not-found so routing can retry other nodes or refresh agent_registry.
            err.contains("NodeNotFound")
        }
        _ => false,
    }
}

fn get_i64(d: &FlatDict, key: &str) -> Option<i64> {
    match d.get(key) {
        Some(FlatValue::Int64(v)) => Some(*v),
        _ => None,
    }
}

fn err_from_resp(resp: &FlatDict, path_for_err: &str) -> FsAgentError {
    let err_s = match resp.get("err") {
        Some(FlatValue::String(s)) => s.to_string(),
        _ => "remote error".to_string(),
    };
    let Some(err_kind_i64) = get_i64(resp, FS_AGENT_RPC_ERR_KIND_KEY) else {
        return FsAgentError::InvalidArgument {
            detail: format!(
                "remote error response missing err_kind: path={} detail={}",
                path_for_err, err_s
            ),
        };
    };
    let Some(err_kind) = FsAgentRpcErrorKind::from_i64(err_kind_i64) else {
        return FsAgentError::InvalidArgument {
            detail: format!(
                "remote error response has unknown err_kind={} path={} detail={}",
                err_kind_i64, path_for_err, err_s
            ),
        };
    };
    match err_kind {
        FsAgentRpcErrorKind::InvalidArgument => FsAgentError::InvalidArgument { detail: err_s },
        FsAgentRpcErrorKind::Os => {
            let Some(errno_i64) = get_i64(resp, "errno") else {
                return FsAgentError::InvalidArgument {
                    detail: format!(
                        "remote os error missing errno: path={} detail={}",
                        path_for_err, err_s
                    ),
                };
            };
            FsAgentError::os(errno_i64 as i32, path_for_err, err_s)
        }
        FsAgentRpcErrorKind::AccessDenied => FsAgentError::AccessDenied {
            path: path_for_err.to_string(),
            detail: err_s,
        },
        FsAgentRpcErrorKind::Internal => FsAgentError::Kv(KvError::Api(ApiError::Unknown {
            detail: format!(
                "remote agent internal error: path={} detail={}",
                path_for_err, err_s
            ),
        })),
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_millis() as u64
}

fn reopen_owned_fd(fd: &OwnedFd) -> Result<OwnedFd, std::io::Error> {
    let path = format!("/proc/self/fd/{}", fd.as_raw_fd());
    let cpath = std::ffi::CString::new(path).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "fd path contains nul byte",
        )
    })?;
    let reopened = unsafe { libc::open(cpath.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC) };
    if reopened < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(reopened) })
}

fn build_inline_memfd(bytes: &[u8], name: &str) -> Option<OwnedFd> {
    let cname = std::ffi::CString::new(name).ok()?;
    let raw_fd =
        unsafe { libc::memfd_create(cname.as_ptr(), libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING) };
    if raw_fd < 0 {
        return None;
    }
    let mut file = unsafe { fs::File::from_raw_fd(raw_fd) };
    if file.write_all(bytes).is_err() {
        return None;
    }
    if file.flush().is_err() {
        return None;
    }
    let fd_ref = file.as_raw_fd();
    if unsafe { libc::lseek(fd_ref, 0, libc::SEEK_SET) } < 0 {
        return None;
    }
    let seals = libc::F_SEAL_SEAL | libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_WRITE;
    if unsafe { libc::fcntl(fd_ref, libc::F_ADD_SEALS, seals) } < 0 {
        return None;
    }
    Some(file.into())
}

fn within_stale_window(stale_window_ms: u64, start_ms: u64) -> bool {
    now_unix_ms().saturating_sub(start_ms) <= stale_window_ms
}

fn is_write_mode(mode: &str) -> bool {
    mode.contains('w') || mode.contains('a') || mode.contains('+') || mode.contains('x')
}

fn normalize_relpath_rpc(relpath: &str) -> Cow<'_, str> {
    if relpath.is_empty() {
        // Fluxon FS RPC requires non-empty relpath; "." maps to export root.
        Cow::Borrowed(".")
    } else {
        Cow::Borrowed(relpath)
    }
}

fn safe_join_export_root(
    remote_root_dir_abs: &str,
    relpath: &str,
) -> Result<PathBuf, FsAgentError> {
    if remote_root_dir_abs.trim().is_empty() {
        return Err(FsAgentError::InvalidArgument {
            detail: "remote_root_dir_abs must be non-empty".to_string(),
        });
    }
    let root = PathBuf::from(remote_root_dir_abs);
    if !root.is_absolute() {
        return Err(FsAgentError::InvalidArgument {
            detail: "remote_root_dir_abs must be an absolute path".to_string(),
        });
    }
    let root_r = root
        .canonicalize()
        .map_err(|e| FsAgentError::InvalidArgument {
            detail: format!("canonicalize export root failed: {}", e),
        })?;
    let mut rel = relpath.replace('\\', "/");
    while rel.starts_with('/') {
        rel = rel[1..].to_string();
    }
    let parts: Vec<&str> = rel
        .split('/')
        .filter(|x| !x.is_empty() && *x != ".")
        .collect();
    if parts.iter().any(|x| *x == "..") {
        return Err(FsAgentError::InvalidArgument {
            detail: "relpath contains '..'".to_string(),
        });
    }
    let p = if parts.is_empty() {
        root_r.clone()
    } else {
        root_r.join(parts.join("/"))
    };
    if p != root_r && !p.starts_with(&root_r) {
        return Err(FsAgentError::InvalidArgument {
            detail: "resolved path escapes export root".to_string(),
        });
    }
    Ok(p)
}

fn read_hostname_best_effort() -> Option<String> {
    let raw = fs::read_to_string("/etc/hostname").ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn read_product_uuid_best_effort() -> Option<String> {
    let raw = fs::read_to_string("/sys/class/dmi/id/product_uuid").ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn fs_sig_from_metadata(md: &fs::Metadata) -> (i64, i64, i64) {
    let size = md.len() as i64;
    let mtime_ns = md
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| (d.as_nanos() as i128).min(i64::MAX as i128) as i64)
        .unwrap_or(0);
    (size, mtime_ns / 1_000_000_000, mtime_ns % 1_000_000_000)
}

fn read_file_all(path: &str, limit: usize) -> Result<Vec<u8>, FsAgentError> {
    let mut f = fs::File::open(path).map_err(|e| FsAgentError::Io {
        path: path.to_string(),
        detail: e.to_string(),
    })?;
    let mut out: Vec<u8> = Vec::new();
    f.read_to_end(&mut out).map_err(|e| FsAgentError::Io {
        path: path.to_string(),
        detail: e.to_string(),
    })?;
    if out.len() > limit {
        return Err(FsAgentError::os(
            libc::EFBIG,
            path,
            "file content exceeds limit",
        ));
    }
    Ok(out)
}

fn ensure_abs_path(path: &str) -> Result<(), FsAgentError> {
    if !Path::new(path).is_absolute() {
        return Err(FsAgentError::InvalidArgument {
            detail: format!("path must be absolute: {}", path),
        });
    }
    Ok(())
}

fn io_error_to_os(path: &str, e: std::io::Error) -> FsAgentError {
    let errno = e.raw_os_error().unwrap_or(libc::EIO);
    FsAgentError::os(errno, path, e.to_string())
}

fn local_stat_from_metadata(_path: &str, md: fs::Metadata) -> RemoteStat {
    let ft = md.file_type();
    let size = md.len() as i64;
    let (sig_size, mtime_sec, mtime_nsec) = fs_sig_from_metadata(&md);
    let mtime_ns = mtime_sec
        .saturating_mul(1_000_000_000)
        .saturating_add(mtime_nsec);
    debug_assert_eq!(sig_size, size);

    #[cfg(unix)]
    let mode: i64 = {
        use std::os::unix::fs::MetadataExt;
        md.mode() as i64
    };
    #[cfg(not(unix))]
    let mode: i64 = 0;

    RemoteStat {
        exists: true,
        is_file: ft.is_file(),
        is_dir: ft.is_dir(),
        size,
        mtime_ns,
        mode,
    }
}

fn local_stat_follow(path: &str) -> Result<RemoteStat, FsAgentError> {
    match fs::metadata(path) {
        Ok(md) => Ok(local_stat_from_metadata(path, md)),
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                return Ok(RemoteStat {
                    exists: false,
                    is_file: false,
                    is_dir: false,
                    size: 0,
                    mtime_ns: 0,
                    mode: 0,
                });
            }
            Err(io_error_to_os(path, e))
        }
    }
}

fn local_stat_nofollow(path: &str) -> Result<RemoteStat, FsAgentError> {
    match fs::symlink_metadata(path) {
        Ok(md) => Ok(local_stat_from_metadata(path, md)),
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                return Ok(RemoteStat {
                    exists: false,
                    is_file: false,
                    is_dir: false,
                    size: 0,
                    mtime_ns: 0,
                    mode: 0,
                });
            }
            Err(io_error_to_os(path, e))
        }
    }
}

fn local_list_dir(path: &str) -> Result<Vec<RemoteDirEntry>, FsAgentError> {
    let rd = fs::read_dir(path).map_err(|e| io_error_to_os(path, e))?;
    let mut out: Vec<RemoteDirEntry> = Vec::new();
    for ent in rd {
        let ent = match ent {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("read_dir entry failed: path={} err={}", path, e);
                continue;
            }
        };
        let name = ent.file_name().to_string_lossy().into_owned();
        let md = match ent.metadata() {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    "read_dir entry metadata failed: path={} name={} err={}",
                    path,
                    name,
                    e
                );
                continue;
            }
        };
        let ft = md.file_type();
        out.push(RemoteDirEntry {
            name,
            is_file: ft.is_file(),
            is_dir: ft.is_dir(),
        });
    }
    Ok(out)
}

#[cfg(unix)]
fn cstring_path(path: &str) -> Result<std::ffi::CString, FsAgentError> {
    std::ffi::CString::new(path.as_bytes()).map_err(|_| FsAgentError::InvalidArgument {
        detail: "path contains NUL".to_string(),
    })
}

fn local_mkdir(path: &str, mode: i64) -> Result<(), FsAgentError> {
    #[cfg(unix)]
    {
        if mode < 0 {
            return Err(FsAgentError::InvalidArgument {
                detail: "mkdir mode must be non-negative".to_string(),
            });
        }
        let c_path = cstring_path(path)?;
        let rc = unsafe { libc::mkdir(c_path.as_ptr(), mode as libc::mode_t) };
        if rc != 0 {
            let e = std::io::Error::last_os_error();
            // Handle EEXIST: if the path is already a directory, treat as success.
            // This matches Python os.makedirs(exist_ok=True) behavior.
            if e.raw_os_error() == Some(libc::EEXIST) {
                if let Ok(m) = std::fs::metadata(path) {
                    if m.is_dir() {
                        return Ok(());
                    }
                }
            }
            return Err(io_error_to_os(path, e));
        }
        return Ok(());
    }

    #[cfg(not(unix))]
    {
        let _ = (path, mode);
        Err(FsAgentError::InvalidArgument {
            detail: "mkdir not supported on non-unix".to_string(),
        })
    }
}

fn local_rmdir(path: &str) -> Result<(), FsAgentError> {
    #[cfg(unix)]
    {
        let c_path = cstring_path(path)?;
        let rc = unsafe { libc::rmdir(c_path.as_ptr()) };
        if rc != 0 {
            let e = std::io::Error::last_os_error();
            return Err(io_error_to_os(path, e));
        }
        return Ok(());
    }

    #[cfg(not(unix))]
    {
        let _ = path;
        Err(FsAgentError::InvalidArgument {
            detail: "rmdir not supported on non-unix".to_string(),
        })
    }
}

fn local_unlink(path: &str) -> Result<(), FsAgentError> {
    #[cfg(unix)]
    {
        let c_path = cstring_path(path)?;
        let rc = unsafe { libc::unlink(c_path.as_ptr()) };
        if rc != 0 {
            let e = std::io::Error::last_os_error();
            return Err(io_error_to_os(path, e));
        }
        return Ok(());
    }

    #[cfg(not(unix))]
    {
        let _ = path;
        Err(FsAgentError::InvalidArgument {
            detail: "unlink not supported on non-unix".to_string(),
        })
    }
}

fn local_rename(src: &str, dst: &str) -> Result<(), FsAgentError> {
    #[cfg(unix)]
    {
        let c_src = cstring_path(src)?;
        let c_dst = cstring_path(dst)?;
        let rc = unsafe { libc::rename(c_src.as_ptr(), c_dst.as_ptr()) };
        if rc != 0 {
            let e = std::io::Error::last_os_error();
            return Err(io_error_to_os(src, e));
        }
        return Ok(());
    }

    #[cfg(not(unix))]
    {
        let _ = (src, dst);
        Err(FsAgentError::InvalidArgument {
            detail: "rename not supported on non-unix".to_string(),
        })
    }
}

fn local_chmod(path: &str, mode: i64) -> Result<(), FsAgentError> {
    #[cfg(unix)]
    {
        if mode < 0 {
            return Err(FsAgentError::InvalidArgument {
                detail: "chmod mode must be non-negative".to_string(),
            });
        }
        let c_path = cstring_path(path)?;
        let rc = unsafe { libc::chmod(c_path.as_ptr(), mode as libc::mode_t) };
        if rc != 0 {
            let e = std::io::Error::last_os_error();
            return Err(io_error_to_os(path, e));
        }
        return Ok(());
    }

    #[cfg(not(unix))]
    {
        let _ = (path, mode);
        Err(FsAgentError::InvalidArgument {
            detail: "chmod not supported on non-unix".to_string(),
        })
    }
}

fn local_utime(
    path: &str,
    atime_ns: Option<i64>,
    mtime_ns: Option<i64>,
) -> Result<(), FsAgentError> {
    #[cfg(unix)]
    {
        if atime_ns.is_some() != mtime_ns.is_some() {
            return Err(FsAgentError::InvalidArgument {
                detail: "atime_ns and mtime_ns must be both set or both None".to_string(),
            });
        }
        let c_path = cstring_path(path)?;

        // Python os.utime(path, None) means "set to now". We implement that by passing NULL timespec.
        let rc = if atime_ns.is_none() {
            unsafe { libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), std::ptr::null(), 0) }
        } else {
            let at = atime_ns.unwrap();
            let mt = mtime_ns.unwrap();
            if at < 0 || mt < 0 {
                return Err(FsAgentError::InvalidArgument {
                    detail: "atime_ns/mtime_ns must be non-negative".to_string(),
                });
            }
            let times = [
                libc::timespec {
                    tv_sec: (at / 1_000_000_000) as libc::time_t,
                    tv_nsec: (at % 1_000_000_000) as libc::c_long,
                },
                libc::timespec {
                    tv_sec: (mt / 1_000_000_000) as libc::time_t,
                    tv_nsec: (mt % 1_000_000_000) as libc::c_long,
                },
            ];
            unsafe { libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), 0) }
        };
        if rc != 0 {
            let e = std::io::Error::last_os_error();
            return Err(io_error_to_os(path, e));
        }
        return Ok(());
    }

    #[cfg(not(unix))]
    {
        let _ = (path, atime_ns, mtime_ns);
        Err(FsAgentError::InvalidArgument {
            detail: "utime not supported on non-unix".to_string(),
        })
    }
}

fn strip_trailing_slash(p: &str) -> String {
    let mut s = p.to_string();
    while s.len() > 1 && s.ends_with('/') {
        s.pop();
    }
    s
}

fn is_within_dir(file_abs: &str, dir_abs: &str) -> bool {
    if file_abs == dir_abs {
        return true;
    }
    if !dir_abs.ends_with('/') {
        return file_abs.starts_with(&format!("{}/", dir_abs));
    }
    file_abs.starts_with(dir_abs)
}

fn safe_relpath_under_dir(file_abs: &str, dir_abs: &str) -> Option<String> {
    if file_abs == dir_abs {
        return Some("".to_string());
    }
    let file_p = Path::new(file_abs);
    let dir_p = Path::new(dir_abs);
    let rel = file_p.strip_prefix(dir_p).ok()?;
    let mut parts: Vec<String> = Vec::new();
    for c in rel.components() {
        match c {
            std::path::Component::Normal(s) => parts.push(s.to_string_lossy().into_owned()),
            std::path::Component::CurDir => {}
            _ => return None,
        }
    }
    Some(parts.join("/"))
}

fn match_rule<'a>(file_abs: &str, rules: &'a [FluxonFsRule]) -> Option<&'a FluxonFsRule> {
    for r in rules.iter() {
        if is_within_dir(file_abs, &strip_trailing_slash(&r.dir_abs)) {
            return Some(r);
        }
    }
    None
}

fn kv_key_for_file(file_abs: &str, rule: &FluxonFsRule) -> Option<String> {
    let rel = safe_relpath_under_dir(file_abs, &rule.dir_abs)?;
    if rel.is_empty() {
        return None;
    }
    Some(format!("{}{}", rule.kv_key_prefix, rel))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsRawFd;
    use std::thread;
    use std::time::Duration;

    fn sample_entry() -> RemoteMetadataCacheEntry {
        RemoteMetadataCacheEntry {
            stat: RemoteStat {
                exists: true,
                is_file: true,
                is_dir: false,
                size: 4,
                mtime_ns: 7,
                mode: 0o644,
            },
            sig: Some("sig".to_string()),
            authorized_at_ms: 1,
            invalidation_seq: 0,
            inline_fd: None,
        }
    }

    fn sample_key(identity_fp: &str, export: &str, relpath: &str) -> RemoteMetadataCacheKey {
        RemoteMetadataCacheKey {
            identity_fingerprint: identity_fp.to_string(),
            export_name: export.to_string(),
            relpath: relpath.to_string(),
        }
    }

    #[test]
    fn invalidate_exact_removes_all_identity_variants() {
        let mut cache = HashMap::new();
        cache.insert(sample_key("alice", "exp", "dir/file.bin"), sample_entry());
        cache.insert(sample_key("bob", "exp", "dir/file.bin"), sample_entry());
        cache.insert(sample_key("alice", "exp", "dir/other.bin"), sample_entry());

        cache.retain(|key, _| !metadata_cache_matches_exact(key, "exp", "dir/file.bin"));

        assert!(!cache.contains_key(&sample_key("alice", "exp", "dir/file.bin")));
        assert!(!cache.contains_key(&sample_key("bob", "exp", "dir/file.bin")));
        assert!(cache.contains_key(&sample_key("alice", "exp", "dir/other.bin")));
    }

    #[test]
    fn invalidate_prefix_removes_all_identity_variants_under_prefix() {
        let mut cache = HashMap::new();
        cache.insert(sample_key("alice", "exp", "dir"), sample_entry());
        cache.insert(sample_key("bob", "exp", "dir/file.bin"), sample_entry());
        cache.insert(
            sample_key("alice", "exp", "dir/sub/leaf.bin"),
            sample_entry(),
        );
        cache.insert(sample_key("alice", "exp", "other/file.bin"), sample_entry());

        cache.retain(|key, _| !metadata_cache_matches_prefix(key, "exp", "dir"));

        assert!(!cache.contains_key(&sample_key("alice", "exp", "dir")));
        assert!(!cache.contains_key(&sample_key("bob", "exp", "dir/file.bin")));
        assert!(!cache.contains_key(&sample_key("alice", "exp", "dir/sub/leaf.bin")));
        assert!(cache.contains_key(&sample_key("alice", "exp", "other/file.bin")));
    }

    #[test]
    fn inline_fd_cache_entry_cap_evicts_oldest_entries() {
        let mut cache: HashMap<RemoteMetadataCacheKey, RemoteMetadataCacheEntry> = HashMap::new();
        let resident_bytes = AtomicUsize::new(0);
        let access_seq = AtomicU64::new(0);

        for idx in 0..(REMOTE_INLINE_FD_CACHE_MAX_ENTRIES + 1) {
            let fd = build_inline_memfd(format!("file-{idx}").as_bytes(), "fluxon_fs_agent_test")
                .expect("memfd");
            let mut entry = sample_entry();
            entry.sig = Some(format!("sig-{idx}"));
            entry.inline_fd = Some(Arc::new(RemoteInlineFdCacheEntry {
                fd,
                size_bytes: 4,
                sig: format!("sig-{idx}"),
                last_access_seq: AtomicU64::new(access_seq.fetch_add(1, Ordering::Relaxed) + 1),
            }));
            resident_bytes.fetch_add(4, Ordering::Relaxed);
            cache.insert(
                sample_key("alice", "exp", &format!("dir/file-{idx}.bin")),
                entry,
            );
        }

        while resident_bytes.load(Ordering::Relaxed) > REMOTE_INLINE_FD_CACHE_MAX_RESIDENT_BYTES
            || open_cache_fd_entry_count(&cache) > REMOTE_INLINE_FD_CACHE_MAX_ENTRIES
        {
            let evict_key = cache
                .iter()
                .filter_map(|(k, entry)| {
                    let fd = entry.inline_fd.as_ref()?;
                    Some((k.clone(), fd.last_access_seq.load(Ordering::Relaxed)))
                })
                .min_by_key(|(_, seq)| *seq)
                .map(|(k, _)| k)
                .expect("evict key");
            let removed = open_cache_take_inline_fd(cache.get_mut(&evict_key).expect("entry"));
            resident_bytes.fetch_sub(removed, Ordering::Relaxed);
        }

        assert_eq!(
            open_cache_fd_entry_count(&cache),
            REMOTE_INLINE_FD_CACHE_MAX_ENTRIES
        );
        let first_key = sample_key("alice", "exp", "dir/file-0.bin");
        assert!(cache.contains_key(&first_key));
        assert!(
            cache
                .get(&first_key)
                .and_then(|entry| entry.inline_fd.as_ref())
                .is_none()
        );
        let newest_key = sample_key(
            "alice",
            "exp",
            &format!("dir/file-{}.bin", REMOTE_INLINE_FD_CACHE_MAX_ENTRIES),
        );
        assert!(
            cache
                .get(&newest_key)
                .and_then(|entry| entry.inline_fd.as_ref())
                .is_some()
        );
        for entry in cache.values() {
            if let Some(fd) = entry.inline_fd.as_ref() {
                assert!(fd.fd.as_raw_fd() >= 0);
            }
        }
    }

    #[test]
    fn metadata_invalidation_state_applies_exact_and_advances_seq() {
        let mut cache = HashMap::new();
        cache.insert(sample_key("alice", "exp", "dir/file.bin"), sample_entry());
        cache.insert(sample_key("bob", "exp", "dir/file.bin"), sample_entry());
        cache.insert(sample_key("alice", "exp", "dir/other.bin"), sample_entry());

        let state = FsMetadataInvalidationStateWire {
            latest_seq: 3,
            events: vec![FsMetadataInvalidationEventWire {
                export_name: "exp".to_string(),
                relpath: "dir/file.bin".to_string(),
                scope: FsMetadataInvalidationScopeWire::Exact,
                seq: 3,
            }],
        };

        for event in &state.events {
            if matches!(event.scope, FsMetadataInvalidationScopeWire::Exact) {
                cache.retain(|key, _| {
                    !metadata_cache_matches_exact(key, &event.export_name, &event.relpath)
                });
            }
        }

        assert!(!cache.contains_key(&sample_key("alice", "exp", "dir/file.bin")));
        assert!(!cache.contains_key(&sample_key("bob", "exp", "dir/file.bin")));
        assert!(cache.contains_key(&sample_key("alice", "exp", "dir/other.bin")));
        assert_eq!(state.latest_seq, 3);
    }

    #[test]
    fn metadata_lookup_seq_rejects_stale_entry() {
        let mut entry = sample_entry();
        entry.invalidation_seq = 2;
        let latest_seq = 3_u64;
        assert!(entry.invalidation_seq < latest_seq);
    }

    #[test]
    fn access_model_fingerprint_change_requires_cache_flush() {
        let mut hasher = Sha256::new();
        hasher.update(br#"{"users":[{"username":"a"}]}"#);
        let fp1 = hex::encode(hasher.finalize());

        let mut hasher = Sha256::new();
        hasher.update(br#"{"users":[{"username":"b"}]}"#);
        let fp2 = hex::encode(hasher.finalize());

        assert_ne!(fp1, fp2);
    }

    #[test]
    fn remote_write_session_frame_count_matches_chunk_boundaries() {
        assert_eq!(remote_write_session_frame_count(0), 0);
        assert_eq!(remote_write_session_frame_count(1), 1);
        assert_eq!(
            remote_write_session_frame_count(REMOTE_WRITE_SESSION_CHUNK_BYTES),
            1
        );
        assert_eq!(
            remote_write_session_frame_count(REMOTE_WRITE_SESSION_CHUNK_BYTES + 1),
            2
        );
        assert_eq!(
            remote_write_session_frame_count(REMOTE_WRITE_SESSION_CHUNK_BYTES * 4),
            4
        );
    }

    #[test]
    fn remote_write_session_frame_seq_advances_across_payload_submissions() {
        let chunk = REMOTE_WRITE_SESSION_CHUNK_BYTES;
        let first: Vec<u64> = (0..4)
            .map(|idx| remote_write_session_frame_seq(0, 0, idx))
            .collect();
        let second: Vec<u64> = (0..4)
            .map(|idx| remote_write_session_frame_seq(4, 0, idx))
            .collect();
        let later_window: Vec<u64> = (0..2)
            .map(|idx| remote_write_session_frame_seq(8, chunk, idx))
            .collect();

        assert_eq!(first, vec![0, 1, 2, 3]);
        assert_eq!(second, vec![4, 5, 6, 7]);
        assert_eq!(later_window, vec![9, 10]);
    }

    #[test]
    fn write_session_client_state_waits_until_frames_are_acked() {
        let state = Arc::new(RemoteWriteSessionClientState::default());
        assert!(
            state
                .enqueue_submit(RemoteWriteSessionClientSubmit {
                    base_seq_no: 0,
                    offset: 0,
                    data: Bytes::from_static(b"payload"),
                    enqueued_at: Instant::now(),
                })
                .expect("enqueue submit")
                .schedule_count
                > 0
        );

        let sent_waiter_state = state.clone();
        let sent_waiter = thread::spawn(move || sent_waiter_state.wait_for_sent_frames(1));
        let ack_waiter_state = state.clone();
        let ack_waiter = thread::spawn(move || ack_waiter_state.wait_for_acked_frames(1));

        thread::sleep(Duration::from_millis(20));
        let batch = state
            .take_next_batch_for_send()
            .expect("take batch")
            .expect("batch must exist");
        assert_eq!(batch.seq_no, 0);
        assert_eq!(batch.offset, 0);
        assert_eq!(batch.frame_count, 1);
        thread::sleep(Duration::from_millis(20));
        let followup = state.finish_batch_send(&batch, Ok(()));
        assert_eq!(followup.schedule_count, 0);
        assert_eq!(followup.confirm_seq_no, Some(0));
        assert_eq!(sent_waiter.join().expect("join waiter"), Ok(()));
        assert_eq!(
            state.handle_data_ack(&FsWriteSessionDataAck {
                session_id: "remote-sess".to_string(),
                seq_no: 0,
                frame_count: 1,
                ok: true,
                err_detail: String::new(),
            }),
            Ok(false)
        );

        assert_eq!(ack_waiter.join().expect("join waiter"), Ok(()));
    }

    #[test]
    fn write_session_client_state_surfaces_fatal_error() {
        let state = RemoteWriteSessionClientState::default();
        assert!(
            state
                .enqueue_submit(RemoteWriteSessionClientSubmit {
                    base_seq_no: 0,
                    offset: 0,
                    data: Bytes::from_static(b"payload"),
                    enqueued_at: Instant::now(),
                })
                .expect("enqueue submit")
                .schedule_count
                > 0
        );
        let _batch = state
            .take_next_batch_for_send()
            .expect("take batch")
            .expect("batch must exist");
        let followup = state.finish_batch_send(&_batch, Err("boom".to_string()));
        assert_eq!(followup.schedule_count, 0);
        assert_eq!(followup.confirm_seq_no, None);

        assert_eq!(state.wait_for_sent_frames(1), Err("boom".to_string()));
        assert_eq!(
            state.enqueue_submit(RemoteWriteSessionClientSubmit {
                base_seq_no: 1,
                offset: 7,
                data: Bytes::from_static(b"x"),
                enqueued_at: Instant::now(),
            }),
            Err("boom".to_string())
        );
    }

    #[test]
    fn write_session_client_state_accepts_early_ack_before_send_finishes() {
        let state = RemoteWriteSessionClientState::default();
        assert!(
            state
                .enqueue_submit(RemoteWriteSessionClientSubmit {
                    base_seq_no: 0,
                    offset: 0,
                    data: Bytes::from_static(b"payload"),
                    enqueued_at: Instant::now(),
                })
                .expect("enqueue submit")
                .schedule_count
                > 0
        );
        let batch = state
            .take_next_batch_for_send()
            .expect("take batch")
            .expect("batch must exist");
        assert_eq!(
            state.handle_data_ack(&FsWriteSessionDataAck {
                session_id: "remote-sess".to_string(),
                seq_no: 0,
                frame_count: 1,
                ok: true,
                err_detail: String::new(),
            }),
            Ok(false)
        );
        let followup = state.finish_batch_send(&batch, Ok(()));
        assert_eq!(followup.schedule_count, 0);
        assert_eq!(followup.confirm_seq_no, None);
        assert_eq!(state.wait_for_acked_frames(1), Ok(()));
    }

    #[test]
    fn write_session_client_state_reports_acked_frames_ready() {
        let state = RemoteWriteSessionClientState::default();
        assert!(
            state
                .enqueue_submit(RemoteWriteSessionClientSubmit {
                    base_seq_no: 0,
                    offset: 0,
                    data: Bytes::from_static(b"payload"),
                    enqueued_at: Instant::now(),
                })
                .expect("enqueue submit")
                .schedule_count
                > 0
        );
        let batch = state
            .take_next_batch_for_send()
            .expect("take batch")
            .expect("batch must exist");
        assert_eq!(state.acked_frames_ready(1), Ok(false));
        let followup = state.finish_batch_send(&batch, Ok(()));
        assert_eq!(followup.schedule_count, 0);
        assert_eq!(followup.confirm_seq_no, Some(0));
        assert_eq!(state.acked_frames_ready(1), Ok(false));
        assert_eq!(
            state.handle_data_ack(&FsWriteSessionDataAck {
                session_id: "remote-sess".to_string(),
                seq_no: 0,
                frame_count: 1,
                ok: true,
                err_detail: String::new(),
            }),
            Ok(false)
        );
        assert_eq!(state.acked_frames_ready(1), Ok(true));
    }

    #[test]
    fn write_session_client_state_confirm_releases_pending_without_ack() {
        let state = RemoteWriteSessionClientState::default();
        assert!(
            state
                .enqueue_submit(RemoteWriteSessionClientSubmit {
                    base_seq_no: 0,
                    offset: 0,
                    data: Bytes::from_static(b"payload"),
                    enqueued_at: Instant::now(),
                })
                .expect("enqueue submit")
                .schedule_count
                > 0
        );
        let batch = state
            .take_next_batch_for_send()
            .expect("take batch")
            .expect("batch must exist");
        let followup = state.finish_batch_send(&batch, Ok(()));
        assert_eq!(followup.schedule_count, 0);
        assert_eq!(followup.confirm_seq_no, Some(0));
        let confirm = state
            .begin_confirm_pending_batch(0)
            .expect("begin confirm")
            .expect("confirm must exist");
        assert_eq!(confirm.expected_frames, 1);
        assert_eq!(
            state.confirm_delivered_upto(confirm.expected_frames),
            Ok(false)
        );
        assert_eq!(state.wait_for_acked_frames(1), Ok(()));
    }

    #[test]
    fn write_session_client_state_can_schedule_multiple_batches_concurrently() {
        let state = RemoteWriteSessionClientState::default();
        let payload = vec![0u8; REMOTE_WRITE_SESSION_CHUNK_BYTES * 4];
        let followup = state
            .buffer_append(
                0,
                Bytes::from(payload),
                REMOTE_WRITE_SESSION_CHUNK_BYTES * 4,
                16,
            )
            .expect("buffer append");
        assert_eq!(followup.len(), 1);
        let enqueue = state
            .enqueue_submit(followup.into_iter().next().expect("submit"))
            .expect("enqueue submit");
        assert_eq!(enqueue.schedule_count, 1);

        let first = state
            .take_next_batch_for_send()
            .expect("take first")
            .expect("first batch");
        assert_eq!(first.frame_count, 4);

        let payload2 = vec![1u8; REMOTE_WRITE_SESSION_CHUNK_BYTES * 4];
        let followup2 = state
            .buffer_append(
                (REMOTE_WRITE_SESSION_CHUNK_BYTES * 4) as i64,
                Bytes::from(payload2),
                REMOTE_WRITE_SESSION_CHUNK_BYTES * 4,
                16,
            )
            .expect("buffer append 2");
        let enqueue2 = state
            .enqueue_submit(followup2.into_iter().next().expect("submit 2"))
            .expect("enqueue submit 2");
        assert_eq!(enqueue2.schedule_count, 1);

        let second = state
            .take_next_batch_for_send()
            .expect("take second")
            .expect("second batch");
        assert_eq!(second.seq_no, 4);
        assert_eq!(second.frame_count, 4);

        let done1 = state.finish_batch_send(&first, Ok(()));
        let done2 = state.finish_batch_send(&second, Ok(()));
        assert_eq!(done1.schedule_count, 0);
        assert_eq!(done2.schedule_count, 0);
    }
}
