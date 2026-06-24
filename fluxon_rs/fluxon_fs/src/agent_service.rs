use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Context;
use prost::bytes::Bytes;
use serde_json::json;

use fluxon_framework_compiled::shutdown::ViewShutdownExt;
use fluxon_fs_core::config::{
    FLUXON_FS_COMPONENT_METADATA_KEY, FLUXON_FS_CONFIG_ACCESS_MODEL_JSON_KEY,
    FLUXON_FS_CONTROL_SCHEMA_VERSION, FLUXON_FS_EXPORT_OVERLAY_JSON_KEY,
    FLUXON_FS_RPC_TOKEN_PAYLOAD_KEY, FS_AGENT_DECLARED_EXPORT_JSON_KEY,
    FS_AGENT_EXPORT_PUBLISH_RPC_PATH, FS_AGENT_EXPORT_UNPUBLISH_RPC_PATH,
    FS_AGENT_EXPORTS_SNAPSHOT_RPC_PATH, FS_MASTER_AGENT_EXPORTS_PUSH_RPC_PATH,
    FS_MASTER_BOOTSTRAP_PULL_INTERVAL_MS, FS_MASTER_CONFIG_RPC_PATH, FluxonFsComponent,
    FluxonFsExport, FluxonFsExportRpcPaths, FluxonFsMasterConfig, FluxonFsOp,
    FluxonFsRuntimeAccessModel, FluxonFsScopeAccessMode, FsAgentDeclaredExportWire,
    FsAgentExportOverlayWire, FsAgentExportSnapshotItemWire, access_model_required_mode_for_op,
    admin_browse_export_for_agent_instance_key_v1, extract_cache_config_yaml_from_yaml_text,
    is_admin_browse_export_name_v1, parse_cache_config_yaml, parse_master_config_from_yaml_text,
    parse_runtime_access_model_json_text, runtime_access_model_allows_path,
    runtime_access_model_can_browse_dir, runtime_access_model_has_bucket_write_access,
    runtime_access_model_visible_dir_entry, verify_rpc_token,
};
use fluxon_kv::FsMountKind;
use fluxon_kv::cluster_manager::NodeID;
use fluxon_kv::config::ClientConfigYaml;
use fluxon_kv::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult};
use fluxon_kv::user_api::FluxonUserApi;
use fluxon_kv::user_api::flat_dict::{FlatDict, FlatValue};
use fluxon_kv::user_api::{
    USER_RPC_DEFAULT_TIMEOUT_MS, decode_flat_dict_bytes, encode_flat_dict_bytes,
};
use fluxon_kv::user_rpc::user_rpc_call;
use fluxon_kv::{ConfigArg, run_client_with_startup_member_metadata};
use fluxon_util::fs_statvfs::{
    mount_point_for_abs_dir, normalize_abs_dir_label, statvfs_used_total,
};
use parking_lot::{Condvar, Mutex, RwLock};
use tokio::runtime::Runtime;
use tokio::sync::Notify;

use crate::agent::{FS_AGENT_RPC_ERR_KIND_KEY, FsAgentRpcErrorKind};
use crate::config::FluxonFsExportRoutingMode;
use crate::config::FluxonFsGlobalConfig;
use crate::write_session_rpc::{
    self, FsAbortWriteSessionReq, FsAbortWriteSessionResp, FsCloseWriteSessionReq,
    FsCloseWriteSessionResp, FsOpenWriteSessionReq, FsOpenWriteSessionResp,
    FsWaitWriteSessionPayloadsReq, FsWaitWriteSessionPayloadsResp, FsWriteSessionChunkReq,
    FsWriteSessionChunkResp, FsWriteSessionDataFrame,
};

pub(crate) mod transfer_agent;

pub const CHUNK_BYTES: usize = 1024 * 1024;
pub const READ_CHUNK_BYTES: usize = 8 * 1024 * 1024;
pub const WRITE_SESSION_CHUNK_BYTES: usize = crate::agent::REMOTE_WRITE_SESSION_CHUNK_BYTES;
const WRITE_SESSION_MAX_INFLIGHT_CHUNKS: usize = 4;
const WRITE_SESSION_MAX_QUEUED_BYTES: usize =
    WRITE_SESSION_CHUNK_BYTES * WRITE_SESSION_MAX_INFLIGHT_CHUNKS;
const WRITE_SESSION_IDLE_TIMEOUT_SECS: u64 = 180;
const WRITE_SESSION_REAP_INTERVAL_SECS: u64 = 30;
const WRITE_SESSION_CLOSE_WAIT_TIMEOUT_SECS: u64 = 30;
pub(crate) const TRANSFER_HEARTBEAT_INTERVAL_MS: i64 = 5_000;
pub(crate) const TRANSFER_STREAM_RPC_TIMEOUT_MS: u64 = 60_000;
pub(crate) const TRANSFER_WORKER_COORDINATION_RPC_TIMEOUT_MS: u64 = 30_000;
const AGENT_EXPORTS_SNAPSHOT_SCHEMA_VERSION_KEY: &str = "schema_version";
const AGENT_EXPORTS_SNAPSHOT_EXPORTS_JSON_KEY: &str = "exports_json";
const AGENT_EXPORT_NAME_KEY: &str = "export_name";
const S3_INTERNAL_MULTIPART_PAYLOAD_KEY: &str =
    fluxon_fs_core::s3_gateway::FS_S3_INTERNAL_MULTIPART_PAYLOAD_KEY;
static WRITE_PATH_TRANSIENT_OWNER_SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
struct AgentMount {
    export_name: String,
    local_mount_dir_abs: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeclaredExportOrigin {
    StaticConfig,
    RuntimePublish,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeclaredExportRecord {
    export: FluxonFsExport,
    origin: DeclaredExportOrigin,
}

#[derive(Debug)]
struct AgentExportsState {
    declared_exports: BTreeMap<String, DeclaredExportRecord>,
    overlay_disabled: BTreeSet<String>,
    overlay_upserts: BTreeMap<String, FluxonFsExport>,
    effective_exports: BTreeMap<String, FluxonFsExport>,
    revision: u64,
    pushed_revision: u64,
}

#[derive(Clone)]
struct AgentExportsHandle {
    state: Arc<Mutex<AgentExportsState>>,
    changed: Arc<Notify>,
    internal_exports: Arc<BTreeMap<String, FluxonFsExport>>,
}

impl AgentExportsHandle {
    fn new_from_static_cfg(
        cfg: &FluxonFsGlobalConfig,
        internal_exports: BTreeMap<String, FluxonFsExport>,
    ) -> Self {
        let mut declared_exports: BTreeMap<String, DeclaredExportRecord> = BTreeMap::new();
        for (export_name, export) in cfg.exports.iter() {
            upsert_declared_export_record(
                &mut declared_exports,
                export_name.to_string(),
                export.clone(),
                DeclaredExportOrigin::StaticConfig,
            );
        }
        let effective_exports =
            build_effective_exports(&declared_exports, &BTreeSet::new(), &BTreeMap::new());
        Self {
            state: Arc::new(Mutex::new(AgentExportsState {
                declared_exports,
                overlay_disabled: BTreeSet::new(),
                overlay_upserts: BTreeMap::new(),
                effective_exports,
                revision: 1,
                pushed_revision: 0,
            })),
            changed: Arc::new(Notify::new()),
            internal_exports: Arc::new(internal_exports),
        }
    }

    fn publish_export(&self, export_name: String, export: FluxonFsExport) {
        let mut st = self.state.lock();
        upsert_declared_export_record(
            &mut st.declared_exports,
            export_name,
            export,
            DeclaredExportOrigin::RuntimePublish,
        );
        apply_export_state_change_locked(&mut st, &self.changed);
    }

    fn remove_runtime_export(&self, export_name: &str) -> bool {
        let mut st = self.state.lock();
        let Some(existing) = st.declared_exports.get(export_name) else {
            return false;
        };
        if existing.origin == DeclaredExportOrigin::StaticConfig {
            return false;
        }
        st.declared_exports.remove(export_name);
        apply_export_state_change_locked(&mut st, &self.changed);
        true
    }

    fn is_static_declared_export(&self, export_name: &str) -> bool {
        let st = self.state.lock();
        matches!(
            st.declared_exports.get(export_name),
            Some(record) if record.origin == DeclaredExportOrigin::StaticConfig
        )
    }

    fn declared_export_record(&self, export_name: &str) -> Option<DeclaredExportRecord> {
        let st = self.state.lock();
        st.declared_exports.get(export_name).cloned()
    }

    fn apply_master_overlay(&self, mut overlay: FsAgentExportOverlayWire) {
        overlay.disabled_exports.sort();
        overlay.disabled_exports.dedup();
        let mut st = self.state.lock();
        let next_disabled: BTreeSet<String> = overlay.disabled_exports.into_iter().collect();
        if st.overlay_disabled == next_disabled && st.overlay_upserts == overlay.upsert_exports {
            return;
        }
        st.overlay_disabled = next_disabled;
        st.overlay_upserts = overlay.upsert_exports;
        apply_export_state_change_locked(&mut st, &self.changed);
    }

    fn effective_export(&self, export_name: &str) -> KvResult<FluxonFsExport> {
        let st = self.state.lock();
        let export = st.effective_exports.get(export_name).ok_or_else(|| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!("unknown export: {}", export_name),
            })
        })?;
        Ok(export.clone())
    }

    fn rpc_export(&self, export_name: &str) -> KvResult<FluxonFsExport> {
        if let Some(export) = self.internal_exports.get(export_name) {
            return Ok(export.clone());
        }
        self.effective_export(export_name)
    }

    fn export_root_dir_abs(&self, export_name: &str) -> KvResult<String> {
        Ok(self.rpc_export(export_name)?.remote_root_dir_abs)
    }

    fn snapshot_items_for_master(&self) -> (u64, Vec<FsAgentExportSnapshotItemWire>) {
        let st = self.state.lock();
        let mut out: Vec<FsAgentExportSnapshotItemWire> = Vec::new();
        for (export_name, export) in st.effective_exports.iter() {
            out.push(FsAgentExportSnapshotItemWire {
                export_name: export_name.to_string(),
                export: export.clone(),
            });
        }
        (st.revision, out)
    }

    fn mark_pushed_revision(&self, rev: u64) {
        let mut st = self.state.lock();
        if st.pushed_revision < rev {
            st.pushed_revision = rev;
        }
    }

    fn pushed_revision(&self) -> u64 {
        let st = self.state.lock();
        st.pushed_revision
    }

    fn current_revision(&self) -> u64 {
        let st = self.state.lock();
        st.revision
    }

    fn export_root_dirs_abs_snapshot(&self) -> Vec<String> {
        let st = self.state.lock();
        let mut dirs: Vec<String> = st
            .effective_exports
            .values()
            .map(|export| normalize_abs_dir_label(export.remote_root_dir_abs.as_str()))
            .filter(|p| !p.is_empty())
            .collect();
        dirs.sort();
        dirs.dedup();
        dirs
    }
}

fn upsert_declared_export_record(
    declared_exports: &mut BTreeMap<String, DeclaredExportRecord>,
    export_name: String,
    export: FluxonFsExport,
    new_origin: DeclaredExportOrigin,
) {
    let origin = match declared_exports.get(&export_name) {
        Some(existing) => existing.origin,
        None => new_origin,
    };
    declared_exports.insert(export_name, DeclaredExportRecord { export, origin });
}

fn build_effective_exports(
    declared_exports: &BTreeMap<String, DeclaredExportRecord>,
    overlay_disabled: &BTreeSet<String>,
    overlay_upserts: &BTreeMap<String, FluxonFsExport>,
) -> BTreeMap<String, FluxonFsExport> {
    let mut effective_exports: BTreeMap<String, FluxonFsExport> = BTreeMap::new();
    for (export_name, record) in declared_exports {
        effective_exports.insert(export_name.clone(), record.export.clone());
    }
    for export_name in overlay_disabled {
        effective_exports.remove(export_name);
    }
    for (export_name, export) in overlay_upserts {
        effective_exports.insert(export_name.clone(), export.clone());
    }
    effective_exports
}

fn apply_export_state_change_locked(st: &mut AgentExportsState, changed: &Notify) {
    let next_effective = build_effective_exports(
        &st.declared_exports,
        &st.overlay_disabled,
        &st.overlay_upserts,
    );
    if st.effective_exports == next_effective {
        return;
    }
    st.effective_exports = next_effective;
    st.revision = st.revision.saturating_add(1);
    changed.notify_one();
}

#[derive(Clone)]
struct AgentAccessModelHandle {
    model: Arc<RwLock<Option<FluxonFsRuntimeAccessModel>>>,
}

impl AgentAccessModelHandle {
    fn new(model: Option<FluxonFsRuntimeAccessModel>) -> Self {
        Self {
            model: Arc::new(RwLock::new(model)),
        }
    }

    fn get(&self) -> Option<FluxonFsRuntimeAccessModel> {
        self.model.read().clone()
    }

    fn set(&self, model: Option<FluxonFsRuntimeAccessModel>) {
        *self.model.write() = model;
    }
}

#[derive(Clone)]
struct AgentDataPlaneHandlers {
    stat: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync>,
    open_read: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync>,
    list_dir: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync>,
    read_chunk: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync>,
    open_write_session: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync>,
    write_session_chunk: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync>,
    truncate_write_session: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync>,
    close_write_session: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync>,
    abort_write_session: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync>,
    write_chunk: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync>,
    truncate: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync>,
    mkdir: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync>,
    rmdir: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync>,
    unlink: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync>,
    rename: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync>,
    chmod: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync>,
    utime: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync>,
}

pub struct FluxonFsAgentHandle {
    exports: AgentExportsHandle,
    data_plane: AgentDataPlaneHandlers,
    registered_export_rpc_paths: Arc<Mutex<BTreeMap<String, FluxonFsExportRpcPaths>>>,
}

#[derive(Debug)]
struct QueuedWriteChunk {
    seq_no: u64,
    offset: u64,
    data: Bytes,
    is_data_frame: bool,
}

#[derive(Debug, Clone)]
struct WriteSessionStoredIoError {
    errno: i32,
    detail: String,
}

#[derive(Debug, Clone, Default)]
struct WriteSessionTimingStats {
    enqueue_state_lock_wait_ns: u64,
    enqueue_backpressure_wait_ns: u64,
    writer_state_lock_wait_ns: u64,
    writer_file_lock_wait_ns: u64,
    writer_seek_ns: u64,
    writer_write_ns: u64,
    close_wait_ns: u64,
    bytes_enqueued: u64,
    bytes_written: u64,
    chunks_enqueued: u64,
    chunks_written: u64,
}

fn write_path_key(export: &str, path: &std::path::Path) -> String {
    format!("{}\0{}", export, path.display())
}

fn active_write_owner_detail(owner: &str) -> String {
    format!("write path busy: active_owner={}", owner)
}

fn resp_err_busy(detail: impl Into<String>) -> FlatDict {
    resp_err(FsAgentRpcErrorKind::Os, detail.into(), Some(libc::EBUSY))
}

fn next_transient_write_path_owner(op: &str) -> String {
    format!(
        "{}:{}",
        op,
        WRITE_PATH_TRANSIENT_OWNER_SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

fn acquire_transient_write_path_guard(
    active_write_paths: &ActiveWritePathsHandle,
    key: &str,
    op: &str,
) -> Result<ActiveWritePathGuard, FlatDict> {
    let owner = next_transient_write_path_owner(op);
    acquire_transient_write_path_guard_with_owner(active_write_paths, key, &owner)
}

fn acquire_transient_write_path_guard_with_owner(
    active_write_paths: &ActiveWritePathsHandle,
    key: &str,
    owner: &str,
) -> Result<ActiveWritePathGuard, FlatDict> {
    active_write_paths
        .try_acquire_transient(key, owner)
        .map_err(|existing| resp_err_busy(active_write_owner_detail(&existing)))
}

struct WriteSessionEntry {
    export_name: String,
    relpath: String,
    write_path_key: String,
    write_path_owner: String,
    current_pos: u64,
    last_touched: Instant,
    queued_chunks: VecDeque<QueuedWriteChunk>,
    queued_bytes: usize,
    scheduled: bool,
    writing: bool,
    closing: bool,
    aborted: bool,
    fatal_error: Option<WriteSessionStoredIoError>,
    expected_data_frames: Option<u64>,
    highest_received_seq: Option<u64>,
    highest_written_seq: Option<u64>,
    received_data_frame_seqs: BTreeSet<u64>,
    written_data_frame_seqs: BTreeSet<u64>,
    timing: WriteSessionTimingStats,
    created_at: Instant,
}

struct WriteSessionEntryHandleInner {
    state: Mutex<WriteSessionEntry>,
    file: Mutex<fs::File>,
    cv: Condvar,
}

type WriteSessionEntryHandle = Arc<WriteSessionEntryHandleInner>;

#[derive(Clone)]
struct WriteExecutorHandle {
    ready: Arc<Mutex<VecDeque<WriteSessionEntryHandle>>>,
    cv: Arc<Condvar>,
    drain_all: bool,
}

#[derive(Clone)]
struct ActiveWritePathsHandle {
    entries: Arc<Mutex<BTreeMap<String, String>>>,
}

impl ActiveWritePathsHandle {
    fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn try_acquire_owned(&self, key: &str, owner: &str) -> Result<(), String> {
        let mut entries = self.entries.lock();
        match entries.get(key) {
            Some(existing) if existing != owner => Err(existing.clone()),
            Some(_) => Ok(()),
            None => {
                entries.insert(key.to_string(), owner.to_string());
                Ok(())
            }
        }
    }

    fn release_owned(&self, key: &str, owner: &str) {
        let mut entries = self.entries.lock();
        if matches!(entries.get(key), Some(existing) if existing == owner) {
            entries.remove(key);
        }
    }

    fn try_acquire_transient(
        &self,
        key: &str,
        owner: &str,
    ) -> Result<ActiveWritePathGuard, String> {
        self.try_acquire_owned(key, owner)?;
        Ok(ActiveWritePathGuard {
            handle: self.clone(),
            key: key.to_string(),
            owner: owner.to_string(),
            released: false,
        })
    }
}

struct ActiveWritePathGuard {
    handle: ActiveWritePathsHandle,
    key: String,
    owner: String,
    released: bool,
}

impl Drop for ActiveWritePathGuard {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        self.handle.release_owned(&self.key, &self.owner);
        self.released = true;
    }
}

impl WriteExecutorHandle {
    fn new(worker_count: usize) -> Self {
        let this = Self {
            ready: Arc::new(Mutex::new(VecDeque::new())),
            cv: Arc::new(Condvar::new()),
            drain_all: configured_write_executor_drain_all(),
        };
        for idx in 0..worker_count {
            let ready = this.ready.clone();
            let cv = this.cv.clone();
            let executor = this.clone();
            let _ = thread::Builder::new()
                .name(format!("fluxon_fs_write_executor_{idx}"))
                .spawn(move || write_executor_worker_loop(ready, cv, executor));
        }
        this
    }

    fn schedule(&self, entry: WriteSessionEntryHandle) {
        let mut ready = self.ready.lock();
        ready.push_back(entry);
        self.cv.notify_one();
    }
}

fn configured_write_executor_worker_count() -> usize {
    if let Ok(raw) = std::env::var("FLUXON_FS_WRITE_EXECUTOR_WORKERS") {
        if let Ok(parsed) = raw.trim().parse::<usize>() {
            return parsed.max(1);
        }
    }
    std::thread::available_parallelism()
        .map(|v| v.get().clamp(4, 32))
        .unwrap_or(4)
}

fn configured_write_executor_drain_all() -> bool {
    if let Ok(raw) = std::env::var("FLUXON_FS_WRITE_EXECUTOR_DRAIN_ALL") {
        let raw = raw.trim().to_ascii_lowercase();
        return matches!(raw.as_str(), "1" | "true" | "yes" | "on");
    }
    false
}

#[derive(Clone)]
struct AgentWriteSessionsHandle {
    next_id: Arc<AtomicU64>,
    entries: Arc<Mutex<BTreeMap<String, WriteSessionEntryHandle>>>,
    active_write_paths: ActiveWritePathsHandle,
}

impl AgentWriteSessionsHandle {
    fn new() -> Self {
        Self {
            next_id: Arc::new(AtomicU64::new(1)),
            entries: Arc::new(Mutex::new(BTreeMap::new())),
            active_write_paths: ActiveWritePathsHandle::new(),
        }
    }

    fn alloc_id(&self) -> String {
        format!("{:016x}", self.next_id.fetch_add(1, Ordering::Relaxed))
    }

    fn insert(&self, session_id: String, entry: WriteSessionEntry, file: fs::File) {
        self.entries.lock().insert(
            session_id,
            Arc::new(WriteSessionEntryHandleInner {
                state: Mutex::new(entry),
                file: Mutex::new(file),
                cv: Condvar::new(),
            }),
        );
    }

    fn get(&self, session_id: &str) -> Option<WriteSessionEntryHandle> {
        self.entries.lock().get(session_id).cloned()
    }

    fn take(&self, session_id: &str) -> Option<WriteSessionEntryHandle> {
        let entry = self.entries.lock().remove(session_id);
        if let Some(handle) = entry.as_ref() {
            self.release_entry_path_lease(handle);
        }
        entry
    }

    fn reap_idle(&self, idle_for: Duration) -> usize {
        let now = Instant::now();
        let mut removed = 0usize;
        let mut release_handles: Vec<WriteSessionEntryHandle> = Vec::new();
        let mut entries = self.entries.lock();
        entries.retain(|_, entry| {
            let keep = {
                let mut state = entry.state.lock();
                let idle = now.saturating_duration_since(state.last_touched) >= idle_for;
                let busy = state.writing || state.scheduled || !state.queued_chunks.is_empty();
                if idle && !busy && !state.closing {
                    state.aborted = true;
                    entry.cv.notify_all();
                    false
                } else {
                    true
                }
            };
            if !keep {
                removed = removed.saturating_add(1);
                release_handles.push(entry.clone());
            }
            keep
        });
        drop(entries);
        for handle in release_handles {
            self.release_entry_path_lease(&handle);
        }
        removed
    }

    fn release_entry_path_lease(&self, entry: &WriteSessionEntryHandle) {
        let state = entry.state.lock();
        self.active_write_paths
            .release_owned(&state.write_path_key, &state.write_path_owner);
    }
}

fn write_session_stored_io_error(e: &std::io::Error) -> WriteSessionStoredIoError {
    WriteSessionStoredIoError {
        errno: e.raw_os_error().unwrap_or(libc::EIO),
        detail: e.to_string(),
    }
}

fn write_session_accepts_chunk_while_closing(
    state: &WriteSessionEntry,
    seq_no: u64,
    is_data_frame: bool,
) -> bool {
    if !state.closing {
        return true;
    }
    is_data_frame && matches!(state.expected_data_frames, Some(expected) if seq_no < expected)
}

fn write_session_pending_expected_frames(state: &WriteSessionEntry) -> bool {
    match state.expected_data_frames {
        Some(expected) if expected > 0 => state.written_data_frame_seqs.len() < expected as usize,
        _ => false,
    }
}

fn write_session_pending_received_frames(state: &WriteSessionEntry, expected_frames: u64) -> bool {
    expected_frames > 0 && state.received_data_frame_seqs.len() < expected_frames as usize
}

fn write_session_merge_expected_frames(current: Option<u64>, incoming: Option<u64>) -> Option<u64> {
    match (current, incoming) {
        (Some(current), Some(incoming)) => Some(current.max(incoming)),
        (Some(current), None) => Some(current),
        (None, Some(incoming)) => Some(incoming),
        (None, None) => None,
    }
}

fn write_session_close_timeout_detail(session_id: &str, state: &WriteSessionEntry) -> String {
    let expected = state.expected_data_frames.unwrap_or(0);
    format!(
        "write session close timed out: session_id={} expected_frames={} received_frames={} written_frames={} queued_chunks={} queued_bytes={} highest_received_seq={} highest_written_seq={}",
        session_id,
        expected,
        state.received_data_frame_seqs.len(),
        state.written_data_frame_seqs.len(),
        state.queued_chunks.len(),
        state.queued_bytes,
        state
            .highest_received_seq
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string()),
        state
            .highest_written_seq
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string()),
    )
}

fn write_executor_worker_loop(
    ready: Arc<Mutex<VecDeque<WriteSessionEntryHandle>>>,
    cv: Arc<Condvar>,
    write_executor: WriteExecutorHandle,
) {
    loop {
        let entry = {
            let mut guard = ready.lock();
            while guard.is_empty() {
                cv.wait(&mut guard);
            }
            guard.pop_front()
        };
        let Some(entry) = entry else {
            continue;
        };
        if write_executor.drain_all {
            while drain_write_session_entry_once(&entry) {}
        } else if drain_write_session_entry_once(&entry) {
            write_executor.schedule(entry);
        }
    }
}

fn drain_write_session_entry_once(entry_handle: &WriteSessionEntryHandle) -> bool {
    let (chunk, need_seek) = {
        let state_lock_started = Instant::now();
        let mut state = entry_handle.state.lock();
        state.timing.writer_state_lock_wait_ns = state
            .timing
            .writer_state_lock_wait_ns
            .saturating_add(state_lock_started.elapsed().as_nanos() as u64);
        if state.aborted {
            state.queued_chunks.clear();
            state.queued_bytes = 0;
            state.writing = false;
            state.scheduled = false;
            entry_handle.cv.notify_all();
            return false;
        }
        if state.fatal_error.is_some() {
            state.queued_chunks.clear();
            state.queued_bytes = 0;
            state.writing = false;
            state.scheduled = false;
            entry_handle.cv.notify_all();
            return false;
        }
        let Some(chunk) = state.queued_chunks.pop_front() else {
            state.writing = false;
            state.scheduled = false;
            entry_handle.cv.notify_all();
            return false;
        };
        state.queued_bytes = state.queued_bytes.saturating_sub(chunk.data.len());
        let need_seek = state.current_pos != chunk.offset;
        state.writing = true;
        state.last_touched = Instant::now();
        entry_handle.cv.notify_all();
        (chunk, need_seek)
    };

    let write_res = {
        let file_lock_started = Instant::now();
        let mut file = entry_handle.file.lock();
        let file_lock_wait_ns = file_lock_started.elapsed().as_nanos() as u64;
        let mut seek_ns = 0u64;
        let mut write_ns = 0u64;
        if need_seek {
            let seek_started = Instant::now();
            if let Err(e) = file.seek(SeekFrom::Start(chunk.offset)) {
                seek_ns = seek_started.elapsed().as_nanos() as u64;
                (Err(e), file_lock_wait_ns, seek_ns, write_ns)
            } else {
                seek_ns = seek_started.elapsed().as_nanos() as u64;
                let write_started = Instant::now();
                let res = file.write_all(&chunk.data);
                write_ns = write_started.elapsed().as_nanos() as u64;
                (res, file_lock_wait_ns, seek_ns, write_ns)
            }
        } else {
            let write_started = Instant::now();
            let res = file.write_all(&chunk.data);
            write_ns = write_started.elapsed().as_nanos() as u64;
            (res, file_lock_wait_ns, seek_ns, write_ns)
        }
    };

    let mut requeue = false;
    let state_lock_started = Instant::now();
    let mut state = entry_handle.state.lock();
    state.timing.writer_state_lock_wait_ns = state
        .timing
        .writer_state_lock_wait_ns
        .saturating_add(state_lock_started.elapsed().as_nanos() as u64);
    let (write_res, file_lock_wait_ns, seek_ns, write_ns) = write_res;
    state.timing.writer_file_lock_wait_ns = state
        .timing
        .writer_file_lock_wait_ns
        .saturating_add(file_lock_wait_ns);
    state.timing.writer_seek_ns = state.timing.writer_seek_ns.saturating_add(seek_ns);
    state.timing.writer_write_ns = state.timing.writer_write_ns.saturating_add(write_ns);
    state.last_touched = Instant::now();
    state.writing = false;
    match write_res {
        Ok(()) => {
            state.current_pos = chunk.offset.saturating_add(chunk.data.len() as u64);
            state.timing.bytes_written = state
                .timing
                .bytes_written
                .saturating_add(chunk.data.len() as u64);
            state.timing.chunks_written = state.timing.chunks_written.saturating_add(1);
            state.highest_written_seq = Some(
                state
                    .highest_written_seq
                    .map(|prev| prev.max(chunk.seq_no))
                    .unwrap_or(chunk.seq_no),
            );
            if chunk.is_data_frame {
                state.written_data_frame_seqs.insert(chunk.seq_no);
            }
            if state.aborted {
                state.queued_chunks.clear();
                state.queued_bytes = 0;
                state.scheduled = false;
                entry_handle.cv.notify_all();
                return false;
            }
            if state.fatal_error.is_some() {
                state.queued_chunks.clear();
                state.queued_bytes = 0;
                state.scheduled = false;
                entry_handle.cv.notify_all();
                return false;
            }
            if state.queued_chunks.is_empty() {
                state.scheduled = false;
                entry_handle.cv.notify_all();
            } else {
                requeue = true;
                entry_handle.cv.notify_all();
            }
        }
        Err(e) => {
            state.fatal_error = Some(write_session_stored_io_error(&e));
            state.queued_chunks.clear();
            state.queued_bytes = 0;
            state.scheduled = false;
            entry_handle.cv.notify_all();
            return false;
        }
    }
    requeue
}

pub fn run_agent_blocking(config_path: &str, workdir: &str) -> anyhow::Result<()> {
    if config_path.trim().is_empty() {
        anyhow::bail!("config_path must be non-empty");
    }
    if workdir.trim().is_empty() {
        anyhow::bail!("workdir must be non-empty");
    }
    let config_path = PathBuf::from(config_path);
    let workdir = PathBuf::from(workdir);
    if !config_path.exists() {
        anyhow::bail!("config not found: {}", config_path.display());
    }
    if !workdir.exists() {
        anyhow::bail!("workdir not found: {}", workdir.display());
    }
    std::env::set_current_dir(&workdir)
        .with_context(|| format!("set workdir failed: {}", workdir.display()))?;

    let raw = std::fs::read_to_string(&config_path)
        .with_context(|| format!("read config: {}", config_path.display()))?;

    let master_cfg =
        parse_master_config_from_yaml_text(&raw).map_err(|e| anyhow::anyhow!("{}", e))?;
    let cache_yaml =
        extract_cache_config_yaml_from_yaml_text(&raw).map_err(|e| anyhow::anyhow!("{}", e))?;
    let fs_cache = parse_cache_config_yaml(&cache_yaml).map_err(|e| anyhow::anyhow!("{}", e))?;
    let initial_access_model = extract_initial_access_model_from_fluxon_config(&raw)?;

    let kv_yaml = extract_kvclient_config_yaml_from_fluxon_config(&raw)?;
    let mut kv_cfg = kv_yaml.verify().map_err(|e| anyhow::anyhow!("{}", e))?;

    // Ensure external client mode; FS agent is a filesystem RPC server, not a contributing data node.
    let dram = kv_cfg.contribute_to_cluster_pool_size.dram;
    let vram_is_zero = kv_cfg
        .contribute_to_cluster_pool_size
        .vram
        .values()
        .all(|v| *v == 0);
    if !(dram == 0 && vram_is_zero) {
        anyhow::bail!(
            "kvclient must be zero-contribution (external client) mode for fluxon_fs agent"
        );
    }
    let agent_mounts = extract_agent_mounts_from_yaml_text(&raw)?;

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .with_context(|| "build tokio runtime")?,
    );
    let rt2 = rt.clone();
    let res = rt.as_ref().block_on(async move {
        async_main(
            rt2,
            kv_cfg,
            master_cfg,
            cache_yaml,
            fs_cache,
            initial_access_model,
            agent_mounts,
        )
        .await
    });

    // Causal chain:
    // - When initialization fails early, dropping a Tokio runtime may block indefinitely while
    //   waiting for blocking tasks to stop.
    // - For service-style entrypoints, failing fast is preferable to hanging on runtime drop.
    if let Ok(rt0) = Arc::try_unwrap(rt) {
        rt0.shutdown_background();
    }

    res
}

async fn async_main(
    rt: Arc<Runtime>,
    kv_cfg: fluxon_kv::config::ClientConfig,
    master_cfg: FluxonFsMasterConfig,
    cache_yaml: String,
    fs_cache: FluxonFsGlobalConfig,
    initial_access_model: Option<FluxonFsRuntimeAccessModel>,
    agent_mounts: Vec<AgentMount>,
) -> anyhow::Result<()> {
    let instance_key = kv_cfg.instance_key.to_string();
    let startup_member_metadata = HashMap::from([(
        FLUXON_FS_COMPONENT_METADATA_KEY.to_string(),
        FluxonFsComponent::Agent.as_metadata_value().to_string(),
    )]);
    let (admin_browse_export_name, admin_browse_export) =
        admin_browse_export_for_agent_instance_key_v1(instance_key.as_str());
    let mut internal_exports: BTreeMap<String, FluxonFsExport> = BTreeMap::new();
    internal_exports.insert(admin_browse_export_name, admin_browse_export);
    let (framework, _client_cfg2) =
        run_client_with_startup_member_metadata(ConfigArg::Config(kv_cfg), startup_member_metadata)
            .await
            .with_context(|| "start kvclient (external)")?;

    let fs_framework = crate::new_fs_framework(format!("fluxon_fs.agent:{}", instance_key));

    let api_for_reg = Arc::new(
        FluxonUserApi::new(framework.clone(), rt.handle().clone())
            .map_err(|e| anyhow::anyhow!("{}", e))?,
    );
    let master_pull_interval_ms = Arc::new(RwLock::new(FS_MASTER_BOOTSTRAP_PULL_INTERVAL_MS));
    let access_model = AgentAccessModelHandle::new(initial_access_model);
    let agent_handle = register_agent_with_access_model(
        api_for_reg.clone(),
        &fs_cache,
        internal_exports,
        FLUXON_FS_CONTROL_SCHEMA_VERSION,
        access_model.clone(),
    )
    .map_err(|e| anyhow::anyhow!("{}", e))?;
    start_export_mount_stat_sampler(
        fs_framework.clone(),
        framework.clone(),
        agent_handle.exports.clone(),
    );
    start_agent_exports_push_actor(
        fs_framework.clone(),
        framework.clone(),
        agent_handle.exports.clone(),
        master_cfg.clone(),
        master_pull_interval_ms.clone(),
    );
    start_access_model_sync_actor(
        fs_framework.clone(),
        framework.clone(),
        api_for_reg.clone(),
        access_model,
        agent_handle.exports.clone(),
        agent_handle.data_plane.clone(),
        agent_handle.registered_export_rpc_paths.clone(),
        master_cfg.clone(),
        master_pull_interval_ms.clone(),
    );

    if !agent_mounts.is_empty() {
        let api_for_agent = FluxonUserApi::new(framework.clone(), rt.handle().clone())
            .map_err(|e| anyhow::anyhow!("{}", e))?;
        let agent = crate::agent::FluxonFsAgent::new(
            fs_framework.clone(),
            framework.clone(),
            api_for_agent,
            rt.handle().clone(),
        );
        agent
            .set_cache_config_yaml(&cache_yaml)
            .map_err(|e| anyhow::anyhow!("{}", e))?;
        agent.set_master_config(master_cfg);

        for m in agent_mounts {
            agent
                .mount_remote_dir_async(&m.local_mount_dir_abs, &m.export_name)
                .await
                .map_err(|e| anyhow::anyhow!("{}", e))?;
            tracing::info!(
                "fluxon_fs agent mounted: export={} local_mount_dir_abs={}",
                m.export_name,
                m.local_mount_dir_abs
            );
        }
    }

    tracing::info!("fluxon_fs agent ready");
    fs_framework.wait_shutdown_signal().await;
    fs_framework
        .shutdown()
        .await
        .map_err(|e| anyhow::anyhow!("framework shutdown failed: {}", e))?;
    framework
        .shutdown()
        .await
        .map_err(|e| anyhow::anyhow!("kv framework shutdown failed: {}", e))?;
    Ok(())
}

fn current_master_pull_interval_ms(master_pull_interval_ms: &Arc<RwLock<u64>>) -> u64 {
    *master_pull_interval_ms.read()
}

fn extract_initial_access_model_from_fluxon_config(
    raw: &str,
) -> anyhow::Result<Option<FluxonFsRuntimeAccessModel>> {
    let _ = raw;
    Ok(None)
}

fn start_export_mount_stat_sampler(
    fs_framework: crate::Framework,
    kv_framework: Arc<fluxon_kv::Framework>,
    exports: AgentExportsHandle,
) {
    // English note:
    // - Export roots are real directories on fluxon_fs agents and are user-visible storage.
    // - Topology should show used/total for these mount points directly (no "spool" indirection).
    //
    // This sampler is intentionally lightweight (statvfs) and must not affect data-plane RPCs.
    const SAMPLE_INTERVAL: Duration = Duration::from_secs(30);

    let poller = ViewShutdownExt::register_shutdown_poller(&fs_framework);
    let metrics = kv_framework
        .metric_reporter_view()
        .metric_reporter()
        .metrics();

    tokio::spawn(async move {
        let mut last_rev: u64 = 0;
        let mut export_root_dirs_abs: Vec<String> = Vec::new();
        loop {
            if !poller.is_running() {
                return;
            }

            let rev = exports.current_revision();
            if rev != last_rev {
                export_root_dirs_abs = exports.export_root_dirs_abs_snapshot();
                last_rev = rev;
            }

            for dir_abs in &export_root_dirs_abs {
                match statvfs_used_total(dir_abs.as_str()) {
                    Ok((used, total)) => match mount_point_for_abs_dir(dir_abs.as_str()) {
                        Ok(mp) => {
                            metrics.set_fs_mount_fs_bytes(
                                FsMountKind::Export,
                                dir_abs.as_str(),
                                mp.as_str(),
                                used,
                                total,
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                "fs mount mountinfo lookup failed: kind=export dir={} err={}",
                                dir_abs,
                                e
                            );
                        }
                    },
                    Err(e) => {
                        tracing::warn!(
                            "fs mount statvfs failed: kind=export dir={} err={}",
                            dir_abs,
                            e
                        );
                    }
                }
            }

            tokio::time::sleep(SAMPLE_INTERVAL).await;
        }
    });
}

fn start_agent_exports_push_actor(
    fs_framework: crate::Framework,
    kv_framework: Arc<fluxon_kv::Framework>,
    exports: AgentExportsHandle,
    master_cfg: FluxonFsMasterConfig,
    master_pull_interval_ms: Arc<RwLock<u64>>,
) {
    const RETRY_LOG_TICKS: u64 = 25;

    let poller = ViewShutdownExt::register_shutdown_poller(&fs_framework);
    let master_id = master_cfg.instance_key.to_string();
    let master_node: NodeID = master_id.clone().into();
    let schema_version = FLUXON_FS_CONTROL_SCHEMA_VERSION;

    tokio::spawn(async move {
        let mut waited_ticks = 0u64;
        let mut waited_ms = 0u64;

        loop {
            if !poller.is_running() {
                return;
            }

            let rev = exports.current_revision();
            let pushed = exports.pushed_revision();
            if rev <= pushed {
                tokio::select! {
                    _ = exports.changed.notified() => {}
                    _ = tokio::time::sleep(Duration::from_millis(200)) => {}
                }
                continue;
            }

            let (snapshot_rev, items) = exports.snapshot_items_for_master();
            let payload: FlatDict = FlatDict::from([
                (
                    AGENT_EXPORTS_SNAPSHOT_SCHEMA_VERSION_KEY.to_string(),
                    FlatValue::Int64(schema_version),
                ),
                (
                    AGENT_EXPORTS_SNAPSHOT_EXPORTS_JSON_KEY.to_string(),
                    FlatValue::String(serde_json::to_string(&items).unwrap()),
                ),
            ]);
            let payload_bytes = match encode_flat_dict_bytes(&payload) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        "fluxon_fs agent export push encode failed: master={} err={}",
                        master_id,
                        e
                    );
                    let interval_ms = current_master_pull_interval_ms(&master_pull_interval_ms);
                    tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                    waited_ticks += 1;
                    waited_ms = waited_ms.saturating_add(interval_ms);
                    continue;
                }
            };

            let call_res = user_rpc_call(
                kv_framework.as_ref(),
                master_node.clone(),
                FS_MASTER_AGENT_EXPORTS_PUSH_RPC_PATH.to_string(),
                payload_bytes,
                USER_RPC_DEFAULT_TIMEOUT_MS,
            )
            .await;

            match call_res {
                Ok(resp_bytes) => {
                    let resp = match decode_flat_dict_bytes(&resp_bytes) {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!(
                                "fluxon_fs agent export push decode failed: master={} err={}",
                                master_id,
                                e
                            );
                            let interval_ms =
                                current_master_pull_interval_ms(&master_pull_interval_ms);
                            tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                            waited_ticks += 1;
                            waited_ms = waited_ms.saturating_add(interval_ms);
                            continue;
                        }
                    };
                    match resp.get("ok") {
                        Some(FlatValue::Bool(true)) => {
                            exports.mark_pushed_revision(snapshot_rev);
                            waited_ticks = 0;
                            waited_ms = 0;
                            continue;
                        }
                        _ => {
                            tracing::warn!(
                                "fluxon_fs agent export push returned ok=false: master={} resp={:?}",
                                master_id,
                                resp
                            );
                        }
                    }
                }
                Err(e) => {
                    waited_ticks += 1;
                    if waited_ticks % RETRY_LOG_TICKS == 0 {
                        tracing::warn!(
                            "fluxon_fs agent export push rpc retrying: master={} waited_s={} err={:?}",
                            master_id,
                            waited_ms / 1000,
                            e
                        );
                    }
                }
            }

            let interval_ms = current_master_pull_interval_ms(&master_pull_interval_ms);
            tokio::time::sleep(Duration::from_millis(interval_ms)).await;
            waited_ms = waited_ms.saturating_add(interval_ms);
        }
    });
}

fn start_access_model_sync_actor(
    fs_framework: crate::Framework,
    kv_framework: Arc<fluxon_kv::Framework>,
    api: Arc<FluxonUserApi>,
    access_model: AgentAccessModelHandle,
    exports: AgentExportsHandle,
    data_plane: AgentDataPlaneHandlers,
    registered_export_rpc_paths: Arc<Mutex<BTreeMap<String, FluxonFsExportRpcPaths>>>,
    master_cfg: FluxonFsMasterConfig,
    master_pull_interval_ms: Arc<RwLock<u64>>,
) {
    const RETRY_LOG_TICKS: u64 = 25;

    let poller = ViewShutdownExt::register_shutdown_poller(&fs_framework);
    let master_id = master_cfg.instance_key.to_string();
    let master_node: NodeID = master_id.clone().into();
    let schema_version = FLUXON_FS_CONTROL_SCHEMA_VERSION;

    tokio::spawn(async move {
        let mut waited_ticks = 0u64;
        let mut waited_ms = 0u64;

        loop {
            if !poller.is_running() {
                return;
            }

            let payload: FlatDict = FlatDict::from([(
                AGENT_EXPORTS_SNAPSHOT_SCHEMA_VERSION_KEY.to_string(),
                FlatValue::Int64(schema_version),
            )]);
            let payload_bytes = match encode_flat_dict_bytes(&payload) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        "fluxon_fs access model sync encode failed: master={} err={}",
                        master_id,
                        e
                    );
                    let interval_ms = current_master_pull_interval_ms(&master_pull_interval_ms);
                    tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                    waited_ticks += 1;
                    waited_ms = waited_ms.saturating_add(interval_ms);
                    continue;
                }
            };

            let call_res = user_rpc_call(
                kv_framework.as_ref(),
                master_node.clone(),
                FS_MASTER_CONFIG_RPC_PATH.to_string(),
                payload_bytes,
                USER_RPC_DEFAULT_TIMEOUT_MS,
            )
            .await;

            match call_res {
                Ok(resp_bytes) => {
                    let resp = match decode_flat_dict_bytes(&resp_bytes) {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!(
                                "fluxon_fs access model sync decode failed: master={} err={}",
                                master_id,
                                e
                            );
                            let interval_ms =
                                current_master_pull_interval_ms(&master_pull_interval_ms);
                            tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                            waited_ticks += 1;
                            waited_ms = waited_ms.saturating_add(interval_ms);
                            continue;
                        }
                    };
                    let next_pull_interval_ms = match resp.get("pull_interval_ms") {
                        Some(FlatValue::Int64(v)) if *v > 0 => *v as u64,
                        _ => {
                            tracing::warn!(
                                "fluxon_fs access model sync invalid field type: master={} key=pull_interval_ms",
                                master_id
                            );
                            let interval_ms =
                                current_master_pull_interval_ms(&master_pull_interval_ms);
                            tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                            waited_ticks += 1;
                            waited_ms = waited_ms.saturating_add(interval_ms);
                            continue;
                        }
                    };
                    let model = match resp.get(FLUXON_FS_CONFIG_ACCESS_MODEL_JSON_KEY) {
                        Some(FlatValue::String(text)) if !text.trim().is_empty() => {
                            match parse_runtime_access_model_json_text(text) {
                                Ok(v) => Some(v),
                                Err(e) => {
                                    tracing::warn!(
                                        "fluxon_fs access model sync parse failed: master={} err={}",
                                        master_id,
                                        e
                                    );
                                    let interval_ms =
                                        current_master_pull_interval_ms(&master_pull_interval_ms);
                                    tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                                    waited_ticks += 1;
                                    waited_ms = waited_ms.saturating_add(interval_ms);
                                    continue;
                                }
                            }
                        }
                        Some(FlatValue::String(_)) | None => None,
                        _ => {
                            tracing::warn!(
                                "fluxon_fs access model sync invalid field type: master={} key={}",
                                master_id,
                                FLUXON_FS_CONFIG_ACCESS_MODEL_JSON_KEY
                            );
                            let interval_ms =
                                current_master_pull_interval_ms(&master_pull_interval_ms);
                            tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                            waited_ticks += 1;
                            waited_ms = waited_ms.saturating_add(interval_ms);
                            continue;
                        }
                    };
                    let overlay = match resp.get(FLUXON_FS_EXPORT_OVERLAY_JSON_KEY) {
                        Some(FlatValue::String(text)) => {
                            match serde_json::from_str::<FsAgentExportOverlayWire>(text) {
                                Ok(v) => v,
                                Err(e) => {
                                    tracing::warn!(
                                        "fluxon_fs export overlay sync parse failed: master={} err={}",
                                        master_id,
                                        e
                                    );
                                    let interval_ms =
                                        current_master_pull_interval_ms(&master_pull_interval_ms);
                                    tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                                    waited_ticks += 1;
                                    waited_ms = waited_ms.saturating_add(interval_ms);
                                    continue;
                                }
                            }
                        }
                        _ => {
                            tracing::warn!(
                                "fluxon_fs export overlay sync invalid field type: master={} key={}",
                                master_id,
                                FLUXON_FS_EXPORT_OVERLAY_JSON_KEY
                            );
                            let interval_ms =
                                current_master_pull_interval_ms(&master_pull_interval_ms);
                            tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                            waited_ticks += 1;
                            waited_ms = waited_ms.saturating_add(interval_ms);
                            continue;
                        }
                    };
                    let mut register_failed = false;
                    for (export_name, export) in &overlay.upsert_exports {
                        if let Err(e) = register_export_rpc_paths_if_needed(
                            &api,
                            &data_plane,
                            &registered_export_rpc_paths,
                            export_name.as_str(),
                            export,
                        ) {
                            tracing::warn!(
                                "fluxon_fs export overlay register rpc paths failed: master={} export={} err={}",
                                master_id,
                                export_name,
                                e
                            );
                            register_failed = true;
                            break;
                        }
                    }
                    if register_failed {
                        let interval_ms = current_master_pull_interval_ms(&master_pull_interval_ms);
                        tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                        waited_ticks += 1;
                        waited_ms = waited_ms.saturating_add(interval_ms);
                        continue;
                    }
                    *master_pull_interval_ms.write() = next_pull_interval_ms;
                    access_model.set(model);
                    exports.apply_master_overlay(overlay);
                    waited_ticks = 0;
                    waited_ms = 0;
                    tokio::time::sleep(Duration::from_millis(next_pull_interval_ms)).await;
                }
                Err(e) => {
                    waited_ticks += 1;
                    if waited_ticks % RETRY_LOG_TICKS == 0 {
                        tracing::warn!(
                            "fluxon_fs access model sync rpc failed: master={} waited_s={} err={}",
                            master_id,
                            waited_ms / 1000,
                            e
                        );
                    }
                    let interval_ms = current_master_pull_interval_ms(&master_pull_interval_ms);
                    tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                    waited_ms = waited_ms.saturating_add(interval_ms);
                }
            }
        }
    });
}

fn extract_kvclient_config_yaml_from_fluxon_config(raw: &str) -> anyhow::Result<ClientConfigYaml> {
    let v: serde_yaml::Value = serde_yaml::from_str(raw).with_context(|| "parse config yaml")?;
    let top = v.as_mapping().context("config file must be a mapping")?;
    let kv = top
        .get(&serde_yaml::Value::String("kvclient".to_string()))
        .context("config must include kvclient mapping")?;
    serde_yaml::from_value(kv.clone()).with_context(|| "parse kvclient yaml")
}

fn extract_agent_mounts_from_yaml_text(text: &str) -> anyhow::Result<Vec<AgentMount>> {
    let root: serde_yaml::Value =
        serde_yaml::from_str(text).with_context(|| "parse config yaml")?;
    let top = root.as_mapping().context("config file must be a mapping")?;
    let fs_v = top
        .get(&serde_yaml::Value::String("fluxon_fs".to_string()))
        .context("fluxon_fs is required")?;
    let fs = fs_v.as_mapping().context("fluxon_fs must be a mapping")?;

    let mounts_v = match fs.get(&serde_yaml::Value::String("agent_mounts".to_string())) {
        Some(v) => v,
        None => return Ok(Vec::new()),
    };
    let mounts = mounts_v
        .as_sequence()
        .context("fluxon_fs.agent_mounts must be a list")?;

    fn require_non_empty_string(
        m: &serde_yaml::Mapping,
        key: &str,
        ctx: &str,
    ) -> anyhow::Result<String> {
        let v = m
            .get(&serde_yaml::Value::String(key.to_string()))
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .with_context(|| format!("{}.{key} must be non-empty string", ctx))?;
        Ok(v)
    }

    let mut out: Vec<AgentMount> = Vec::new();
    for (i, item) in mounts.iter().enumerate() {
        let ctx = format!("fluxon_fs.agent_mounts[{i}]");
        let m = item
            .as_mapping()
            .with_context(|| format!("{ctx} must be a mapping"))?;
        out.push(AgentMount {
            export_name: require_non_empty_string(m, "export_name", &ctx)?,
            local_mount_dir_abs: require_non_empty_string(m, "local_mount_dir_abs", &ctx)?,
        });
    }
    Ok(out)
}

fn validate_runtime_declared_export(export_name: &str, export: &FluxonFsExport) -> KvResult<()> {
    if export.remote_root_dir_abs.trim().is_empty() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "declared export remote_root_dir_abs must be non-empty: export={}",
                export_name
            ),
        }));
    }
    if !PathBuf::from(export.remote_root_dir_abs.as_str()).is_absolute() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "declared export remote_root_dir_abs must be absolute: export={}",
                export_name
            ),
        }));
    }
    match fs::metadata(export.remote_root_dir_abs.as_str()) {
        Ok(md) => {
            if !md.is_dir() {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "declared export remote_root_dir_abs must be a directory: export={}",
                        export_name
                    ),
                }));
            }
        }
        Err(e) => {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "declared export remote_root_dir_abs metadata failed: export={} remote_root_dir_abs={} err={}",
                    export_name, export.remote_root_dir_abs, e
                ),
            }));
        }
    }
    match export.routing_mode {
        FluxonFsExportRoutingMode::StaticNodes => {
            if export.nodes.is_empty() {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "declared export nodes must be non-empty when routing_mode=static_nodes: export={}",
                        export_name
                    ),
                }));
            }
            for node in &export.nodes {
                if node.trim().is_empty() {
                    return Err(KvError::Api(ApiError::InvalidArgument {
                        detail: format!(
                            "declared export nodes contains empty string: export={}",
                            export_name
                        ),
                    }));
                }
            }
        }
        FluxonFsExportRoutingMode::AgentRegistry => {
            if !export.nodes.is_empty() {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "declared export nodes must be empty when routing_mode=agent_registry: export={}",
                        export_name
                    ),
                }));
            }
        }
    }
    if !export.cache_kv_key_prefix.starts_with('/') || !export.cache_kv_key_prefix.ends_with('/') {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "declared export cache_kv_key_prefix must start and end with '/': export={}",
                export_name
            ),
        }));
    }
    if export.cache_bytes_field_key.trim().is_empty() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "declared export cache_bytes_field_key must be non-empty: export={}",
                export_name
            ),
        }));
    }
    if export.cache_max_bytes == 0 {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "declared export cache_max_bytes must be > 0: export={}",
                export_name
            ),
        }));
    }
    for (field, value) in [
        ("stat", export.rpc_paths.stat.as_str()),
        ("open_read", export.rpc_paths.open_read.as_str()),
        ("list_dir", export.rpc_paths.list_dir.as_str()),
        ("read_chunk", export.rpc_paths.read_chunk.as_str()),
        (
            "open_write_session",
            export.rpc_paths.open_write_session.as_str(),
        ),
        (
            "write_session_chunk",
            export.rpc_paths.write_session_chunk.as_str(),
        ),
        (
            "truncate_write_session",
            export.rpc_paths.truncate_write_session.as_str(),
        ),
        (
            "close_write_session",
            export.rpc_paths.close_write_session.as_str(),
        ),
        (
            "abort_write_session",
            export.rpc_paths.abort_write_session.as_str(),
        ),
        ("write_chunk", export.rpc_paths.write_chunk.as_str()),
        ("truncate", export.rpc_paths.truncate.as_str()),
        ("mkdir", export.rpc_paths.mkdir.as_str()),
        ("rmdir", export.rpc_paths.rmdir.as_str()),
        ("unlink", export.rpc_paths.unlink.as_str()),
        ("rename", export.rpc_paths.rename.as_str()),
        ("chmod", export.rpc_paths.chmod.as_str()),
        ("utime", export.rpc_paths.utime.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "declared export rpc_paths.{} must be non-empty: export={}",
                    field, export_name
                ),
            }));
        }
    }
    Ok(())
}

fn register_export_rpc_paths_if_needed(
    api: &Arc<FluxonUserApi>,
    handlers: &AgentDataPlaneHandlers,
    registered_export_rpc_paths: &Arc<Mutex<BTreeMap<String, FluxonFsExportRpcPaths>>>,
    export_name: &str,
    export: &FluxonFsExport,
) -> KvResult<()> {
    let mut registered = registered_export_rpc_paths.lock();
    if let Some(existing_rpc_paths) = registered.get(export_name) {
        if existing_rpc_paths != &export.rpc_paths {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "registered export rpc_paths cannot change without agent restart: export={}",
                    export_name
                ),
            }));
        }
        return Ok(());
    }
    // English note:
    // - Path registration and `registered_export_rpc_paths` must move together.
    // - If we mark the export as registered before the RPC server accepts all paths, retries
    //   would be skipped forever and the export would stay half-registered.
    api.rpc_server()
        .register(export.rpc_paths.stat.as_str(), handlers.stat.clone())?;
    api.rpc_server().register(
        export.rpc_paths.open_read.as_str(),
        handlers.open_read.clone(),
    )?;
    api.rpc_server().register(
        export.rpc_paths.list_dir.as_str(),
        handlers.list_dir.clone(),
    )?;
    api.rpc_server().register(
        export.rpc_paths.read_chunk.as_str(),
        handlers.read_chunk.clone(),
    )?;
    api.rpc_server().register(
        export.rpc_paths.open_write_session.as_str(),
        handlers.open_write_session.clone(),
    )?;
    api.rpc_server().register(
        export.rpc_paths.write_session_chunk.as_str(),
        handlers.write_session_chunk.clone(),
    )?;
    api.rpc_server().register(
        export.rpc_paths.truncate_write_session.as_str(),
        handlers.truncate_write_session.clone(),
    )?;
    api.rpc_server().register(
        export.rpc_paths.close_write_session.as_str(),
        handlers.close_write_session.clone(),
    )?;
    api.rpc_server().register(
        export.rpc_paths.abort_write_session.as_str(),
        handlers.abort_write_session.clone(),
    )?;
    api.rpc_server().register(
        export.rpc_paths.write_chunk.as_str(),
        handlers.write_chunk.clone(),
    )?;
    api.rpc_server().register(
        export.rpc_paths.truncate.as_str(),
        handlers.truncate.clone(),
    )?;
    api.rpc_server()
        .register(export.rpc_paths.mkdir.as_str(), handlers.mkdir.clone())?;
    api.rpc_server()
        .register(export.rpc_paths.rmdir.as_str(), handlers.rmdir.clone())?;
    api.rpc_server()
        .register(export.rpc_paths.unlink.as_str(), handlers.unlink.clone())?;
    api.rpc_server()
        .register(export.rpc_paths.rename.as_str(), handlers.rename.clone())?;
    api.rpc_server()
        .register(export.rpc_paths.chmod.as_str(), handlers.chmod.clone())?;
    api.rpc_server()
        .register(export.rpc_paths.utime.as_str(), handlers.utime.clone())?;
    registered.insert(export_name.to_string(), export.rpc_paths.clone());
    Ok(())
}

pub fn register_agent(
    api: Arc<FluxonUserApi>,
    cfg: &FluxonFsGlobalConfig,
    schema_version: i64,
) -> KvResult<FluxonFsAgentHandle> {
    register_agent_with_access_model(
        api,
        cfg,
        BTreeMap::new(),
        schema_version,
        AgentAccessModelHandle::new(None),
    )
}

fn register_agent_with_access_model(
    api: Arc<FluxonUserApi>,
    cfg: &FluxonFsGlobalConfig,
    internal_exports: BTreeMap<String, FluxonFsExport>,
    schema_version: i64,
    access_model: AgentAccessModelHandle,
) -> KvResult<FluxonFsAgentHandle> {
    fn invalid(detail: impl Into<String>) -> KvError {
        KvError::Api(ApiError::InvalidArgument {
            detail: detail.into(),
        })
    }

    fn validate_export_name(export_name: &str) -> KvResult<()> {
        let e = export_name.trim();
        if e.is_empty() {
            return Err(invalid("export_name must be non-empty"));
        }
        if is_admin_browse_export_name_v1(e) {
            return Err(invalid(format!("export_name prefix is reserved: {}", e)));
        }
        if e.contains('/') {
            return Err(invalid("export_name must not contain '/'"));
        }
        if e.chars().any(|c| c.is_whitespace()) {
            return Err(invalid("export_name must not contain whitespace"));
        }
        Ok(())
    }

    let exports = AgentExportsHandle::new_from_static_cfg(cfg, internal_exports.clone());
    let registered_export_rpc_paths: Arc<Mutex<BTreeMap<String, FluxonFsExportRpcPaths>>> =
        Arc::new(Mutex::new(
            cfg.exports
                .iter()
                .map(|(export_name, export)| (export_name.clone(), export.rpc_paths.clone()))
                .chain(
                    internal_exports.iter().map(|(export_name, export)| {
                        (export_name.clone(), export.rpc_paths.clone())
                    }),
                )
                .collect(),
        ));
    let write_sessions = AgentWriteSessionsHandle::new();
    let write_executor = WriteExecutorHandle::new(configured_write_executor_worker_count());
    let rt_handle = api.runtime_handle();
    write_session_rpc::register_callers(api.framework().as_ref());

    let data_plane = AgentDataPlaneHandlers {
        stat: {
            let ex = exports.clone();
            let access_model = access_model.clone();
            Arc::new(move |_from, payload| Ok(handle_stat(&ex, &access_model, payload)))
        },
        open_read: {
            let ex = exports.clone();
            let access_model = access_model.clone();
            Arc::new(move |_from, payload| Ok(handle_open_read(&ex, &access_model, payload)))
        },
        list_dir: {
            let ex = exports.clone();
            let access_model = access_model.clone();
            Arc::new(move |_from, payload| Ok(handle_list_dir(&ex, &access_model, payload)))
        },
        read_chunk: {
            let ex = exports.clone();
            let access_model = access_model.clone();
            Arc::new(move |_from, payload| Ok(handle_read_chunk(&ex, &access_model, payload)))
        },
        open_write_session: {
            let ex = exports.clone();
            let access_model = access_model.clone();
            let write_sessions = write_sessions.clone();
            Arc::new(move |_from, payload| {
                Ok(handle_open_write_session(
                    &ex,
                    &access_model,
                    &write_sessions,
                    payload,
                ))
            })
        },
        write_session_chunk: {
            let access_model = access_model.clone();
            let write_sessions = write_sessions.clone();
            let write_executor = write_executor.clone();
            Arc::new(move |_from, payload| {
                Ok(handle_write_session_chunk(
                    &access_model,
                    &write_sessions,
                    &write_executor,
                    payload,
                ))
            })
        },
        truncate_write_session: {
            let access_model = access_model.clone();
            let write_sessions = write_sessions.clone();
            Arc::new(move |_from, payload| {
                Ok(handle_truncate_write_session(
                    &access_model,
                    &write_sessions,
                    payload,
                ))
            })
        },
        close_write_session: {
            let access_model = access_model.clone();
            let write_sessions = write_sessions.clone();
            Arc::new(move |_from, payload| {
                Ok(handle_close_write_session(
                    &access_model,
                    &write_sessions,
                    payload,
                ))
            })
        },
        abort_write_session: {
            let access_model = access_model.clone();
            let write_sessions = write_sessions.clone();
            Arc::new(move |_from, payload| {
                Ok(handle_abort_write_session(
                    &access_model,
                    &write_sessions,
                    payload,
                ))
            })
        },
        write_chunk: {
            let ex = exports.clone();
            let access_model = access_model.clone();
            let active_write_paths = write_sessions.active_write_paths.clone();
            Arc::new(move |_from, payload| {
                Ok(handle_write_chunk(
                    &ex,
                    &access_model,
                    &active_write_paths,
                    payload,
                ))
            })
        },
        truncate: {
            let ex = exports.clone();
            let access_model = access_model.clone();
            let active_write_paths = write_sessions.active_write_paths.clone();
            Arc::new(move |_from, payload| {
                Ok(handle_truncate(
                    &ex,
                    &access_model,
                    &active_write_paths,
                    payload,
                ))
            })
        },
        mkdir: {
            let ex = exports.clone();
            let access_model = access_model.clone();
            Arc::new(move |_from, payload| Ok(handle_mkdir(&ex, &access_model, payload)))
        },
        rmdir: {
            let ex = exports.clone();
            let access_model = access_model.clone();
            Arc::new(move |_from, payload| Ok(handle_rmdir(&ex, &access_model, payload)))
        },
        unlink: {
            let ex = exports.clone();
            let access_model = access_model.clone();
            let active_write_paths = write_sessions.active_write_paths.clone();
            Arc::new(move |_from, payload| {
                Ok(handle_unlink(
                    &ex,
                    &access_model,
                    &active_write_paths,
                    payload,
                ))
            })
        },
        rename: {
            let ex = exports.clone();
            let access_model = access_model.clone();
            let active_write_paths = write_sessions.active_write_paths.clone();
            Arc::new(move |_from, payload| {
                Ok(handle_rename(
                    &ex,
                    &access_model,
                    &active_write_paths,
                    payload,
                ))
            })
        },
        chmod: {
            let ex = exports.clone();
            let access_model = access_model.clone();
            let active_write_paths = write_sessions.active_write_paths.clone();
            Arc::new(move |_from, payload| {
                Ok(handle_chmod(
                    &ex,
                    &access_model,
                    &active_write_paths,
                    payload,
                ))
            })
        },
        utime: {
            let ex = exports.clone();
            let access_model = access_model.clone();
            let active_write_paths = write_sessions.active_write_paths.clone();
            Arc::new(move |_from, payload| {
                Ok(handle_utime(
                    &ex,
                    &access_model,
                    &active_write_paths,
                    payload,
                ))
            })
        },
    };
    {
        let chunk_access_model = access_model.clone();
        let chunk_write_sessions = write_sessions.clone();
        let chunk_write_executor = write_executor.clone();
        write_session_rpc::register_chunk_handler(
            api.framework().as_ref(),
            rt_handle.clone(),
            move |_from, req, data| {
                handle_write_session_chunk_typed(
                    &chunk_access_model,
                    &chunk_write_sessions,
                    &chunk_write_executor,
                    req,
                    data,
                )
            },
        );
        let chunk_access_model = access_model.clone();
        let chunk_write_sessions = write_sessions.clone();
        let chunk_write_executor = write_executor.clone();
        write_session_rpc::register_data_handler(
            api.framework().as_ref(),
            rt_handle.clone(),
            move |_from, req, data| {
                handle_write_session_data_typed(
                    &chunk_access_model,
                    &chunk_write_sessions,
                    &chunk_write_executor,
                    req,
                    data,
                )
            },
        );
        let open_exports = exports.clone();
        let open_access_model = access_model.clone();
        let open_write_sessions = write_sessions.clone();
        write_session_rpc::register_open_handler(
            api.framework().as_ref(),
            rt_handle.clone(),
            move |_from, req| {
                handle_open_write_session_typed(
                    &open_exports,
                    &open_access_model,
                    &open_write_sessions,
                    req,
                )
            },
        );
        let close_access_model = access_model.clone();
        let close_write_sessions = write_sessions.clone();
        write_session_rpc::register_close_handler(
            api.framework().as_ref(),
            rt_handle.clone(),
            move |_from, req| {
                handle_close_write_session_typed(&close_access_model, &close_write_sessions, req)
            },
        );
        let wait_access_model = access_model.clone();
        let wait_write_sessions = write_sessions.clone();
        write_session_rpc::register_wait_payloads_handler(
            api.framework().as_ref(),
            rt_handle.clone(),
            move |_from, req| {
                handle_wait_write_session_payloads_typed(
                    &wait_access_model,
                    &wait_write_sessions,
                    req,
                )
            },
        );
        let abort_access_model = access_model.clone();
        let abort_write_sessions = write_sessions.clone();
        write_session_rpc::register_abort_handler(
            api.framework().as_ref(),
            rt_handle,
            move |_from, req| {
                handle_abort_write_session_typed(&abort_access_model, &abort_write_sessions, req)
            },
        );
    }

    let mut path_to_handler: BTreeMap<
        String,
        Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync>,
    > = BTreeMap::new();
    for exp in cfg.exports.values() {
        path_to_handler
            .entry(exp.rpc_paths.stat.clone())
            .or_insert_with(|| data_plane.stat.clone());
        path_to_handler
            .entry(exp.rpc_paths.open_read.clone())
            .or_insert_with(|| data_plane.open_read.clone());
        path_to_handler
            .entry(exp.rpc_paths.list_dir.clone())
            .or_insert_with(|| data_plane.list_dir.clone());
        path_to_handler
            .entry(exp.rpc_paths.read_chunk.clone())
            .or_insert_with(|| data_plane.read_chunk.clone());
        path_to_handler
            .entry(exp.rpc_paths.open_write_session.clone())
            .or_insert_with(|| data_plane.open_write_session.clone());
        path_to_handler
            .entry(exp.rpc_paths.write_session_chunk.clone())
            .or_insert_with(|| data_plane.write_session_chunk.clone());
        path_to_handler
            .entry(exp.rpc_paths.truncate_write_session.clone())
            .or_insert_with(|| data_plane.truncate_write_session.clone());
        path_to_handler
            .entry(exp.rpc_paths.close_write_session.clone())
            .or_insert_with(|| data_plane.close_write_session.clone());
        path_to_handler
            .entry(exp.rpc_paths.abort_write_session.clone())
            .or_insert_with(|| data_plane.abort_write_session.clone());
        path_to_handler
            .entry(exp.rpc_paths.write_chunk.clone())
            .or_insert_with(|| data_plane.write_chunk.clone());
        path_to_handler
            .entry(exp.rpc_paths.truncate.clone())
            .or_insert_with(|| data_plane.truncate.clone());
        path_to_handler
            .entry(exp.rpc_paths.mkdir.clone())
            .or_insert_with(|| data_plane.mkdir.clone());
        path_to_handler
            .entry(exp.rpc_paths.rmdir.clone())
            .or_insert_with(|| data_plane.rmdir.clone());
        path_to_handler
            .entry(exp.rpc_paths.unlink.clone())
            .or_insert_with(|| data_plane.unlink.clone());
        path_to_handler
            .entry(exp.rpc_paths.rename.clone())
            .or_insert_with(|| data_plane.rename.clone());
        path_to_handler
            .entry(exp.rpc_paths.chmod.clone())
            .or_insert_with(|| data_plane.chmod.clone());
        path_to_handler
            .entry(exp.rpc_paths.utime.clone())
            .or_insert_with(|| data_plane.utime.clone());
    }
    for exp in internal_exports.values() {
        path_to_handler
            .entry(exp.rpc_paths.stat.clone())
            .or_insert_with(|| data_plane.stat.clone());
        path_to_handler
            .entry(exp.rpc_paths.open_read.clone())
            .or_insert_with(|| data_plane.open_read.clone());
        path_to_handler
            .entry(exp.rpc_paths.list_dir.clone())
            .or_insert_with(|| data_plane.list_dir.clone());
        path_to_handler
            .entry(exp.rpc_paths.read_chunk.clone())
            .or_insert_with(|| data_plane.read_chunk.clone());
        path_to_handler
            .entry(exp.rpc_paths.open_write_session.clone())
            .or_insert_with(|| data_plane.open_write_session.clone());
        path_to_handler
            .entry(exp.rpc_paths.write_session_chunk.clone())
            .or_insert_with(|| data_plane.write_session_chunk.clone());
        path_to_handler
            .entry(exp.rpc_paths.truncate_write_session.clone())
            .or_insert_with(|| data_plane.truncate_write_session.clone());
        path_to_handler
            .entry(exp.rpc_paths.close_write_session.clone())
            .or_insert_with(|| data_plane.close_write_session.clone());
        path_to_handler
            .entry(exp.rpc_paths.abort_write_session.clone())
            .or_insert_with(|| data_plane.abort_write_session.clone());
        path_to_handler
            .entry(exp.rpc_paths.write_chunk.clone())
            .or_insert_with(|| data_plane.write_chunk.clone());
        path_to_handler
            .entry(exp.rpc_paths.truncate.clone())
            .or_insert_with(|| data_plane.truncate.clone());
        path_to_handler
            .entry(exp.rpc_paths.mkdir.clone())
            .or_insert_with(|| data_plane.mkdir.clone());
        path_to_handler
            .entry(exp.rpc_paths.rmdir.clone())
            .or_insert_with(|| data_plane.rmdir.clone());
        path_to_handler
            .entry(exp.rpc_paths.unlink.clone())
            .or_insert_with(|| data_plane.unlink.clone());
        path_to_handler
            .entry(exp.rpc_paths.rename.clone())
            .or_insert_with(|| data_plane.rename.clone());
        path_to_handler
            .entry(exp.rpc_paths.chmod.clone())
            .or_insert_with(|| data_plane.chmod.clone());
        path_to_handler
            .entry(exp.rpc_paths.utime.clone())
            .or_insert_with(|| data_plane.utime.clone());
    }

    for (path, handler) in path_to_handler.into_iter() {
        api.rpc_server().register(&path, handler)?;
    }

    // S3 gateway helper RPCs (stable global paths; not part of per-export rpc_paths).
    {
        let ex = exports.clone();
        let api2 = api.clone();
        let access_model = access_model.clone();
        let handler: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync> =
            Arc::new(move |_from, payload| {
                Ok(handle_s3_stage_object_to_kv(
                    api2.clone(),
                    &ex,
                    &access_model,
                    payload,
                ))
            });
        api.rpc_server().register(
            fluxon_fs_core::s3_gateway::FS_S3_STAGE_OBJECT_TO_KV_RPC_PATH,
            handler,
        )?;
    }
    {
        let ex = exports.clone();
        let api2 = api.clone();
        let access_model = access_model.clone();
        let handler: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync> =
            Arc::new(move |_from, payload| {
                Ok(handle_s3_load_part_file_to_kv(
                    api2.clone(),
                    &ex,
                    &access_model,
                    payload,
                ))
            });
        api.rpc_server().register(
            fluxon_fs_core::s3_gateway::FS_S3_LOAD_PART_FILE_TO_KV_RPC_PATH,
            handler,
        )?;
    }
    {
        let ex = exports.clone();
        let api2 = api.clone();
        let access_model = access_model.clone();
        let handler: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync> =
            Arc::new(move |_from, payload| {
                Ok(handle_s3_load_part_file_range_to_kv(
                    api2.clone(),
                    &ex,
                    &access_model,
                    payload,
                ))
            });
        api.rpc_server().register(
            fluxon_fs_core::s3_gateway::FS_S3_LOAD_PART_FILE_RANGE_TO_KV_RPC_PATH,
            handler,
        )?;
    }

    // Master pull model:
    // - FS master rebuilds export registry by pulling snapshots from online FS agents.
    // - This RPC returns the full exports snapshot for this agent process.
    {
        let ex = exports.clone();
        let handler: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync> =
            Arc::new(move |_from, payload| {
                let got = payload.get(AGENT_EXPORTS_SNAPSHOT_SCHEMA_VERSION_KEY);
                let got_i64 = match got {
                    Some(FlatValue::Int64(v)) => *v,
                    _ => {
                        return Err(KvError::Api(ApiError::InvalidArgument {
                            detail: format!(
                                "{AGENT_EXPORTS_SNAPSHOT_SCHEMA_VERSION_KEY} must be int64"
                            ),
                        }));
                    }
                };
                if got_i64 != schema_version {
                    return Err(KvError::Api(ApiError::InvalidArgument {
                        detail: format!(
                            "schema_version mismatch: expected={} got={}",
                            schema_version, got_i64
                        ),
                    }));
                }

                let (_rev, items) = ex.snapshot_items_for_master();
                let mut out: FlatDict = FlatDict::new();
                out.insert("ok".to_string(), FlatValue::Bool(true));
                out.insert(
                    AGENT_EXPORTS_SNAPSHOT_EXPORTS_JSON_KEY.to_string(),
                    FlatValue::String(serde_json::to_string(&items).unwrap()),
                );
                Ok(out)
            });
        api.rpc_server()
            .register(FS_AGENT_EXPORTS_SNAPSHOT_RPC_PATH, handler)?;
    }

    // Dynamic export publish/unpublish.
    {
        let ex = exports.clone();
        let api2 = api.clone();
        let handlers = data_plane.clone();
        let registered_export_rpc_paths2 = registered_export_rpc_paths.clone();
        let handler: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync> = Arc::new(
            move |_from, payload| {
                let got = payload.get(AGENT_EXPORTS_SNAPSHOT_SCHEMA_VERSION_KEY);
                let got_i64 = match got {
                    Some(FlatValue::Int64(v)) => *v,
                    _ => {
                        return Err(KvError::Api(ApiError::InvalidArgument {
                            detail: format!(
                                "{AGENT_EXPORTS_SNAPSHOT_SCHEMA_VERSION_KEY} must be int64"
                            ),
                        }));
                    }
                };
                if got_i64 != schema_version {
                    return Err(KvError::Api(ApiError::InvalidArgument {
                        detail: format!(
                            "schema_version mismatch: expected={} got={}",
                            schema_version, got_i64
                        ),
                    }));
                }

                let declared_export_json = match payload.get(FS_AGENT_DECLARED_EXPORT_JSON_KEY) {
                    Some(FlatValue::String(s)) if !s.trim().is_empty() => s.clone(),
                    _ => {
                        return Err(invalid(format!(
                            "{FS_AGENT_DECLARED_EXPORT_JSON_KEY} must be non-empty string"
                        )));
                    }
                };
                let declared_export: FsAgentDeclaredExportWire =
                    serde_json::from_str(&declared_export_json).map_err(|e| {
                        invalid(format!(
                            "parse {FS_AGENT_DECLARED_EXPORT_JSON_KEY} failed: {}",
                            e
                        ))
                    })?;
                let export_name = declared_export.export_name.trim().to_string();
                validate_export_name(&export_name)?;
                if ex.is_static_declared_export(&export_name) {
                    return Err(invalid(format!(
                        "cannot publish export already declared in static config: export={}",
                        export_name
                    )));
                }
                if let Some(existing) = ex.declared_export_record(&export_name) {
                    if existing.export.rpc_paths != declared_export.export.rpc_paths {
                        return Err(invalid(format!(
                            "runtime publish cannot change rpc_paths for existing export: export={}",
                            export_name
                        )));
                    }
                }
                validate_runtime_declared_export(&export_name, &declared_export.export)?;
                register_export_rpc_paths_if_needed(
                    &api2,
                    &handlers,
                    &registered_export_rpc_paths2,
                    export_name.as_str(),
                    &declared_export.export,
                )?;
                ex.publish_export(export_name.clone(), declared_export.export);

                let mut out: FlatDict = FlatDict::new();
                out.insert("ok".to_string(), FlatValue::Bool(true));
                Ok(out)
            },
        );
        api.rpc_server()
            .register(FS_AGENT_EXPORT_PUBLISH_RPC_PATH, handler)?;
    }
    {
        let ex = exports.clone();
        let handler: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync> =
            Arc::new(move |_from, payload| {
                let got = payload.get(AGENT_EXPORTS_SNAPSHOT_SCHEMA_VERSION_KEY);
                let got_i64 = match got {
                    Some(FlatValue::Int64(v)) => *v,
                    _ => {
                        return Err(KvError::Api(ApiError::InvalidArgument {
                            detail: format!(
                                "{AGENT_EXPORTS_SNAPSHOT_SCHEMA_VERSION_KEY} must be int64"
                            ),
                        }));
                    }
                };
                if got_i64 != schema_version {
                    return Err(KvError::Api(ApiError::InvalidArgument {
                        detail: format!(
                            "schema_version mismatch: expected={} got={}",
                            schema_version, got_i64
                        ),
                    }));
                }

                let export_name = match payload.get(AGENT_EXPORT_NAME_KEY) {
                    Some(FlatValue::String(s)) => s.trim().to_string(),
                    _ => return Err(invalid(format!("{AGENT_EXPORT_NAME_KEY} must be string"))),
                };
                validate_export_name(&export_name)?;

                if ex.is_static_declared_export(&export_name) {
                    return Err(invalid(format!(
                        "cannot unpublish static config export: export={}",
                        export_name
                    )));
                }

                let existed = ex.remove_runtime_export(&export_name);
                let mut out: FlatDict = FlatDict::new();
                out.insert("ok".to_string(), FlatValue::Bool(true));
                out.insert("existed".to_string(), FlatValue::Bool(existed));
                Ok(out)
            });
        api.rpc_server()
            .register(FS_AGENT_EXPORT_UNPUBLISH_RPC_PATH, handler)?;
    }

    {
        let write_sessions = write_sessions.clone();
        tokio::spawn(async move {
            let idle_for = Duration::from_secs(WRITE_SESSION_IDLE_TIMEOUT_SECS);
            let interval = Duration::from_secs(WRITE_SESSION_REAP_INTERVAL_SECS);
            loop {
                tokio::time::sleep(interval).await;
                let removed = write_sessions.reap_idle(idle_for);
                if removed > 0 {
                    tracing::info!(
                        "fluxon_fs write session reaper removed {} idle sessions",
                        removed
                    );
                }
            }
        });
    }

    Ok(FluxonFsAgentHandle {
        exports,
        data_plane,
        registered_export_rpc_paths,
    })
}

fn resp_err(kind: FsAgentRpcErrorKind, detail: impl Into<String>, errno: Option<i32>) -> FlatDict {
    let mut d = FlatDict::new();
    d.insert("ok".to_string(), FlatValue::Bool(false));
    d.insert(
        FS_AGENT_RPC_ERR_KIND_KEY.to_string(),
        FlatValue::Int64(kind.as_i64()),
    );
    if let Some(errno) = errno {
        d.insert("errno".to_string(), FlatValue::Int64(errno as i64));
    }
    d.insert("err".to_string(), FlatValue::String(detail.into()));
    d
}

fn resp_err_access(detail: impl Into<String>) -> FlatDict {
    resp_err(FsAgentRpcErrorKind::AccessDenied, detail, None)
}

fn now_unix_ms_i64() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|d| (d.as_millis() as i128).min(i64::MAX as i128) as i64)
        .unwrap_or(0)
}

fn request_username_from_payload(
    access_model: &AgentAccessModelHandle,
    payload: &FlatDict,
) -> Result<Option<String>, FlatDict> {
    let Some(model) = access_model.get() else {
        return Err(resp_err_access("fs access model unavailable"));
    };
    let token = match payload.get(FLUXON_FS_RPC_TOKEN_PAYLOAD_KEY) {
        Some(FlatValue::String(s)) if !s.trim().is_empty() => s.as_str(),
        _ => return Err(resp_err_access("missing fs_rpc_token")),
    };
    let claims = match verify_rpc_token(&model, token, now_unix_ms_i64()) {
        Ok(v) => v,
        Err(e) => return Err(resp_err_access(format!("fs rpc auth failed: {}", e))),
    };
    Ok(Some(claims.username))
}

fn authorize_relpath_mode(
    access_model: &AgentAccessModelHandle,
    payload: &FlatDict,
    export_name: &str,
    relpath: &str,
    mode: FluxonFsScopeAccessMode,
) -> Result<Option<String>, FlatDict> {
    if is_admin_browse_export_name_v1(export_name) {
        return Ok(None);
    }
    let username = request_username_from_payload(access_model, payload)?;
    let Some(username) = username.clone() else {
        return Ok(None);
    };
    let model = access_model.get().unwrap();
    if fluxon_fs_core::s3_gateway::is_internal_multipart_relpath(relpath) {
        if payload_allows_s3_internal_multipart(payload)
            && runtime_access_model_has_bucket_write_access(&model, &username, export_name)
        {
            return Ok(Some(username));
        }
        return Err(resp_err_access(format!(
            "fs internal multipart access denied: username={} export_name={} relpath={}",
            username, export_name, relpath
        )));
    }
    if runtime_access_model_allows_path(&model, &username, export_name, relpath, mode) {
        return Ok(Some(username));
    }
    Err(resp_err_access(format!(
        "fs access denied: username={} export_name={} relpath={} mode={}",
        username,
        export_name,
        relpath,
        mode.form_value()
    )))
}

fn authorize_stat_path(
    access_model: &AgentAccessModelHandle,
    payload: &FlatDict,
    export_name: &str,
    relpath: &str,
) -> Result<Option<String>, FlatDict> {
    if is_admin_browse_export_name_v1(export_name) {
        return Ok(None);
    }
    let username = request_username_from_payload(access_model, payload)?;
    let Some(username) = username.clone() else {
        return Ok(None);
    };
    let model = access_model.get().unwrap();
    if fluxon_fs_core::s3_gateway::is_internal_multipart_relpath(relpath) {
        if payload_allows_s3_internal_multipart(payload)
            && runtime_access_model_has_bucket_write_access(&model, &username, export_name)
        {
            return Ok(Some(username));
        }
        return Err(resp_err_access(format!(
            "fs internal multipart stat denied: username={} export_name={} relpath={}",
            username, export_name, relpath
        )));
    }
    if runtime_access_model_allows_path(
        &model,
        &username,
        export_name,
        relpath,
        FluxonFsScopeAccessMode::Read,
    ) || runtime_access_model_can_browse_dir(&model, &username, export_name, relpath)
    {
        return Ok(Some(username));
    }
    Err(resp_err_access(format!(
        "fs stat denied: username={} export_name={} relpath={}",
        username, export_name, relpath
    )))
}

fn authorize_read_path(
    access_model: &AgentAccessModelHandle,
    payload: &FlatDict,
    export_name: &str,
    relpath: &str,
) -> Result<Option<String>, FlatDict> {
    if is_admin_browse_export_name_v1(export_name) {
        return Ok(None);
    }
    let username = request_username_from_payload(access_model, payload)?;
    let Some(username) = username.clone() else {
        return Ok(None);
    };
    let model = access_model.get().unwrap();
    if fluxon_fs_core::s3_gateway::is_internal_multipart_relpath(relpath) {
        if payload_allows_s3_internal_multipart(payload)
            && runtime_access_model_has_bucket_write_access(&model, &username, export_name)
        {
            return Ok(Some(username));
        }
        return Err(resp_err_access(format!(
            "fs internal multipart read denied: username={} export_name={} relpath={}",
            username, export_name, relpath
        )));
    }
    if runtime_access_model_allows_path(
        &model,
        &username,
        export_name,
        relpath,
        FluxonFsScopeAccessMode::Read,
    ) {
        return Ok(Some(username));
    }
    Err(resp_err_access(format!(
        "fs read denied: username={} export_name={} relpath={}",
        username, export_name, relpath
    )))
}

fn stat_fields_from_metadata(md: &fs::Metadata) -> (bool, bool, i64, i64, i64) {
    let ft = md.file_type();
    let size = md.len() as i64;
    let mtime_ns = md
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| (d.as_nanos() as i128).min(i64::MAX as i128) as i64)
        .unwrap_or(0);
    #[cfg(unix)]
    let mode: i64 = {
        use std::os::unix::fs::MetadataExt;
        md.mode() as i64
    };
    #[cfg(not(unix))]
    let mode: i64 = 0;
    (ft.is_file(), ft.is_dir(), size, mtime_ns, mode)
}

fn stat_response_from_metadata(md: &fs::Metadata) -> BTreeMap<String, FlatValue> {
    let (is_file, is_dir, size, mtime_ns, mode) = stat_fields_from_metadata(md);
    BTreeMap::from([
        ("exists".to_string(), FlatValue::Bool(true)),
        ("is_file".to_string(), FlatValue::Bool(is_file)),
        ("is_dir".to_string(), FlatValue::Bool(is_dir)),
        ("size".to_string(), FlatValue::Int64(size)),
        ("mtime_ns".to_string(), FlatValue::Int64(mtime_ns)),
        ("mode".to_string(), FlatValue::Int64(mode)),
    ])
}

fn authorize_list_dir_path(
    access_model: &AgentAccessModelHandle,
    payload: &FlatDict,
    export_name: &str,
    relpath: &str,
) -> Result<Option<String>, FlatDict> {
    if is_admin_browse_export_name_v1(export_name) {
        return Ok(None);
    }
    let username = request_username_from_payload(access_model, payload)?;
    let Some(username) = username.clone() else {
        return Ok(None);
    };
    let model = access_model.get().unwrap();
    if fluxon_fs_core::s3_gateway::is_internal_multipart_relpath(relpath) {
        if payload_allows_s3_internal_multipart(payload)
            && runtime_access_model_has_bucket_write_access(&model, &username, export_name)
        {
            return Ok(Some(username));
        }
        return Err(resp_err_access(format!(
            "fs internal multipart list_dir denied: username={} export_name={} relpath={}",
            username, export_name, relpath
        )));
    }
    if runtime_access_model_can_browse_dir(&model, &username, export_name, relpath) {
        return Ok(Some(username));
    }
    Err(resp_err_access(format!(
        "fs list_dir denied: username={} export_name={} relpath={}",
        username, export_name, relpath
    )))
}

fn payload_allows_s3_internal_multipart(payload: &FlatDict) -> bool {
    matches!(
        payload.get(S3_INTERNAL_MULTIPART_PAYLOAD_KEY),
        Some(FlatValue::Bool(true))
    )
}

fn authorize_rename_paths(
    access_model: &AgentAccessModelHandle,
    payload: &FlatDict,
    export_name: &str,
    src_relpath: &str,
    dst_relpath: &str,
) -> Result<Option<String>, FlatDict> {
    let username = request_username_from_payload(access_model, payload)?;
    let Some(username) = username.clone() else {
        return Ok(None);
    };
    let model = access_model.get().unwrap();
    let allowed = runtime_access_model_allows_path(
        &model,
        &username,
        export_name,
        src_relpath,
        FluxonFsScopeAccessMode::ReadWrite,
    ) && runtime_access_model_allows_path(
        &model,
        &username,
        export_name,
        dst_relpath,
        FluxonFsScopeAccessMode::ReadWrite,
    );
    if allowed {
        return Ok(Some(username));
    }
    Err(resp_err_access(format!(
        "fs rename denied: username={} export_name={} src_relpath={} dst_relpath={}",
        username, export_name, src_relpath, dst_relpath
    )))
}

fn handle_stat(
    exports: &AgentExportsHandle,
    access_model: &AgentAccessModelHandle,
    payload: FlatDict,
) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let relpath = match require_str(&payload, "relpath") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if let Err(resp) = authorize_stat_path(access_model, &payload, &export, &relpath) {
        return resp;
    }
    let root_dir_abs = match exports.export_root_dir_abs(&export) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let p = match safe_join_root(&root_dir_abs, &relpath) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let md = match fs::metadata(&p) {
        Ok(v) => v,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                return resp_ok(BTreeMap::from([
                    ("exists".to_string(), FlatValue::Bool(false)),
                    ("is_file".to_string(), FlatValue::Bool(false)),
                    ("is_dir".to_string(), FlatValue::Bool(false)),
                    ("size".to_string(), FlatValue::Int64(0)),
                    ("mtime_ns".to_string(), FlatValue::Int64(0)),
                    ("mode".to_string(), FlatValue::Int64(0)),
                ]));
            }
            return resp_err_io(e);
        }
    };
    let ft = md.file_type();
    let size = md.len() as i64;
    let mtime_ns = md
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| (d.as_nanos() as i128).min(i64::MAX as i128) as i64)
        .unwrap_or(0);
    #[cfg(unix)]
    let mode: i64 = {
        use std::os::unix::fs::MetadataExt;
        md.mode() as i64
    };
    #[cfg(not(unix))]
    let mode: i64 = 0;

    resp_ok(BTreeMap::from([
        ("exists".to_string(), FlatValue::Bool(true)),
        ("is_file".to_string(), FlatValue::Bool(ft.is_file())),
        ("is_dir".to_string(), FlatValue::Bool(ft.is_dir())),
        ("size".to_string(), FlatValue::Int64(size)),
        ("mtime_ns".to_string(), FlatValue::Int64(mtime_ns)),
        ("mode".to_string(), FlatValue::Int64(mode)),
    ]))
}

fn handle_open_read(
    exports: &AgentExportsHandle,
    access_model: &AgentAccessModelHandle,
    payload: FlatDict,
) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let relpath = match require_str(&payload, "relpath") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if let Err(resp) = authorize_read_path(access_model, &payload, &export, &relpath) {
        return resp;
    }
    let export_cfg = match exports.rpc_export(&export) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let root_dir_abs = match exports.export_root_dir_abs(&export) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let p = match safe_join_root(&root_dir_abs, &relpath) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let mut file = match fs::File::open(&p) {
        Ok(v) => Some(v),
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                return resp_ok(BTreeMap::from([
                    ("exists".to_string(), FlatValue::Bool(false)),
                    ("is_file".to_string(), FlatValue::Bool(false)),
                    ("is_dir".to_string(), FlatValue::Bool(false)),
                    ("size".to_string(), FlatValue::Int64(0)),
                    ("mtime_ns".to_string(), FlatValue::Int64(0)),
                    ("mode".to_string(), FlatValue::Int64(0)),
                ]));
            }
            None
        }
    };

    let md = match file.as_ref() {
        Some(f) => match f.metadata() {
            Ok(v) => v,
            Err(e) => return resp_err_io(e),
        },
        None => match fs::metadata(&p) {
            Ok(v) => v,
            Err(e) => return resp_err_io(e),
        },
    };
    let (is_file, _is_dir, size, mtime_ns, _mode) = stat_fields_from_metadata(&md);
    let mut resp = stat_response_from_metadata(&md);

    if let Some(f) = file.as_mut() {
        if is_file
            && size >= 0
            && (size as u64) <= export_cfg.inline_bytes_max_bytes
            && (size as usize) <= READ_CHUNK_BYTES
        {
            let mut buf = vec![0_u8; size as usize];
            if let Err(e) = f.read_exact(&mut buf) {
                return resp_err_io(e);
            }
            let md_after = match f.metadata() {
                Ok(v) => v,
                Err(e) => return resp_err_io(e),
            };
            let (_, _, size_after, mtime_ns_after, _) = stat_fields_from_metadata(&md_after);
            if size_after == size && mtime_ns_after == mtime_ns {
                resp.insert("data".to_string(), FlatValue::Bytes(buf));
            } else {
                resp = stat_response_from_metadata(&md_after);
            }
        }
    }
    resp_ok(resp)
}

fn handle_list_dir(
    exports: &AgentExportsHandle,
    access_model: &AgentAccessModelHandle,
    payload: FlatDict,
) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let relpath = match require_str(&payload, "relpath") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let username = match authorize_list_dir_path(access_model, &payload, &export, &relpath) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let root_dir_abs = match exports.export_root_dir_abs(&export) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let p = match safe_join_root(&root_dir_abs, &relpath) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let rd = match fs::read_dir(&p) {
        Ok(v) => v,
        Err(e) => return resp_err_io(e),
    };
    let mut items: Vec<serde_json::Value> = Vec::new();
    let filtered_model = if username.is_some() {
        access_model.get()
    } else {
        None
    };
    for ent in rd {
        let ent = match ent {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("list_dir entry failed: err={}", e);
                continue;
            }
        };
        let name = ent.file_name().to_string_lossy().to_string();
        let md = match ent.metadata() {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("list_dir entry metadata failed: name={} err={}", name, e);
                continue;
            }
        };
        let ft = md.file_type();
        if let Some(username) = username.as_ref() {
            let model = filtered_model.as_ref().unwrap();
            let child_relpath = if relpath == "." {
                name.clone()
            } else {
                format!("{}/{}", relpath.trim_end_matches('/'), name)
            };
            if !runtime_access_model_visible_dir_entry(
                &model,
                username,
                &export,
                &child_relpath,
                ft.is_dir(),
            ) {
                continue;
            }
        }
        let size = md.len() as i64;
        let mtime_ns = md
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| (d.as_nanos() as i128).min(i64::MAX as i128) as i64)
            .unwrap_or(0);
        #[cfg(unix)]
        let mode: i64 = {
            use std::os::unix::fs::MetadataExt;
            md.mode() as i64
        };
        #[cfg(not(unix))]
        let mode: i64 = 0;
        items.push(json!({
            "name": name,
            "is_file": ft.is_file(),
            "is_dir": ft.is_dir(),
            "size": size,
            "mtime_ns": mtime_ns,
            "mode": mode,
        }));
    }
    let entries_json = match serde_json::to_string(&items) {
        Ok(v) => v,
        Err(e) => {
            return resp_err_kverr(KvError::Api(ApiError::Unknown {
                detail: format!("json encode failed: {}", e),
            }));
        }
    };
    resp_ok(BTreeMap::from([(
        "entries_json".to_string(),
        FlatValue::String(entries_json),
    )]))
}

fn handle_s3_stage_object_to_kv(
    api: Arc<FluxonUserApi>,
    exports: &AgentExportsHandle,
    access_model: &AgentAccessModelHandle,
    payload: FlatDict,
) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let relpath = match require_str(&payload, "relpath") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if let Err(resp) = authorize_relpath_mode(
        access_model,
        &payload,
        &export,
        &relpath,
        access_model_required_mode_for_op(FluxonFsOp::ReadChunk),
    ) {
        return resp;
    }

    let exp = match exports.rpc_export(&export) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if exp.cache_kv_key_prefix.trim().is_empty() {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "export.cache_kv_key_prefix must be non-empty: export={}",
                export
            ),
        }));
    }
    if exp.cache_bytes_field_key.trim().is_empty() {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "export.cache_bytes_field_key must be non-empty: export={}",
                export
            ),
        }));
    }

    let p = match safe_join_root(&exp.remote_root_dir_abs, &relpath) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let md = match fs::metadata(&p) {
        Ok(v) => v,
        Err(e) => return resp_err_io(e),
    };
    if !md.is_file() {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: "relpath must refer to a file".to_string(),
        }));
    }

    let size = md.len() as i64;
    if size < 0 {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: "invalid file size".to_string(),
        }));
    }

    let mtime_ns = md
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| (d.as_nanos() as i128).min(i64::MAX as i128) as i64)
        .unwrap_or(0);
    let sig = fluxon_fs_core::s3_gateway::object_sig_string(size, mtime_ns);
    let chunk_bytes = fluxon_fs_core::s3_gateway::FS_S3_OBJECT_CHUNK_BYTES as i64;
    let manifest_key = fluxon_fs_core::s3_gateway::kv_manifest_key(
        &exp.cache_kv_key_prefix,
        &export,
        &relpath,
        &sig,
    );

    match api.kv().is_exist(&manifest_key) {
        Ok(true) => {
            return resp_ok(BTreeMap::from([
                ("sig".to_string(), FlatValue::String(sig)),
                ("size".to_string(), FlatValue::Int64(size)),
                ("mtime_ns".to_string(), FlatValue::Int64(mtime_ns)),
                ("chunk_bytes".to_string(), FlatValue::Int64(chunk_bytes)),
            ]));
        }
        Ok(false) => {}
        Err(e) => return resp_err_kverr(e),
    }

    // English note:
    // - In v2 the S3 gateway uses KV as a cross-request cache for fixed-size pieces (1MiB).
    // - `stage_object_to_kv` is intentionally meta-only: it does NOT prefetch the whole object into KV.
    // - The gateway fills missing pieces on-demand via export RPC `read_chunk` and writes them to KV.
    let pieces: i64 = if size == 0 {
        0
    } else {
        (size + chunk_bytes - 1) / chunk_bytes
    };

    let mut man = FlatDict::new();
    man.insert("sig".to_string(), FlatValue::String(sig.clone()));
    man.insert("size".to_string(), FlatValue::Int64(size));
    man.insert("mtime_ns".to_string(), FlatValue::Int64(mtime_ns));
    man.insert("chunk_bytes".to_string(), FlatValue::Int64(chunk_bytes));
    man.insert("chunks".to_string(), FlatValue::Int64(pieces));
    if let Err(e) = api.kv().put(&manifest_key, man) {
        return resp_err_kverr(e);
    }

    resp_ok(BTreeMap::from([
        ("sig".to_string(), FlatValue::String(sig)),
        ("size".to_string(), FlatValue::Int64(size)),
        ("mtime_ns".to_string(), FlatValue::Int64(mtime_ns)),
        ("chunk_bytes".to_string(), FlatValue::Int64(chunk_bytes)),
        ("chunks".to_string(), FlatValue::Int64(pieces)),
    ]))
}

fn handle_s3_load_part_file_to_kv(
    api: Arc<FluxonUserApi>,
    exports: &AgentExportsHandle,
    access_model: &AgentAccessModelHandle,
    payload: FlatDict,
) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let relpath = match require_str(&payload, "relpath") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if let Err(resp) = authorize_relpath_mode(
        access_model,
        &payload,
        &export,
        &relpath,
        access_model_required_mode_for_op(FluxonFsOp::ReadChunk),
    ) {
        return resp;
    }
    let sig = match require_str(&payload, "sig") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let piece_idx = match require_i64(&payload, "piece_idx") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if piece_idx < 0 {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!("piece_idx must be >= 0 (got {})", piece_idx),
        }));
    }

    let exp = match exports.rpc_export(&export) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if exp.cache_kv_key_prefix.trim().is_empty() {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "export.cache_kv_key_prefix must be non-empty: export={}",
                export
            ),
        }));
    }
    if exp.cache_bytes_field_key.trim().is_empty() {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "export.cache_bytes_field_key must be non-empty: export={}",
                export
            ),
        }));
    }

    let p = match safe_join_root(&exp.remote_root_dir_abs, &relpath) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let md = match fs::metadata(&p) {
        Ok(v) => v,
        Err(e) => return resp_err_io(e),
    };
    if !md.is_file() {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: "relpath must refer to a file".to_string(),
        }));
    }

    let size = md.len() as i64;
    if size < 0 {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: "invalid file size".to_string(),
        }));
    }
    if size == 0 {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: "cannot load part for empty file".to_string(),
        }));
    }

    let mtime_ns = md
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| (d.as_nanos() as i128).min(i64::MAX as i128) as i64)
        .unwrap_or(0);
    let actual_sig = fluxon_fs_core::s3_gateway::object_sig_string(size, mtime_ns);
    if actual_sig != sig {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "sig mismatch: expected_sig={} actual_sig={} export={} relpath={}",
                sig, actual_sig, export, relpath
            ),
        }));
    }

    let piece_bytes = fluxon_fs_core::s3_gateway::FS_S3_OBJECT_CHUNK_BYTES as i64;
    let off = match piece_idx.checked_mul(piece_bytes) {
        Some(v) => v,
        None => {
            return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "piece_idx overflow: piece_idx={} piece_bytes={}",
                    piece_idx, piece_bytes
                ),
            }));
        }
    };
    if off < 0 || off >= size {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "piece out of range: piece_idx={} offset={} size={}",
                piece_idx, off, size
            ),
        }));
    }
    let want_i64 = std::cmp::min(piece_bytes, size - off);
    if want_i64 <= 0 || want_i64 > (CHUNK_BYTES as i64) {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "invalid piece length: piece_idx={} offset={} want={}",
                piece_idx, off, want_i64
            ),
        }));
    }
    let want = want_i64 as usize;

    let mut f = match fs::File::open(&p) {
        Ok(v) => v,
        Err(e) => return resp_err_io(e),
    };
    if let Err(e) = f.seek(SeekFrom::Start(off as u64)) {
        return resp_err_io(e);
    }
    let mut buf = vec![0u8; want];
    if let Err(e) = f.read_exact(&mut buf) {
        return resp_err_io(e);
    }

    let key = fluxon_fs_core::s3_gateway::kv_piece_key(
        &exp.cache_kv_key_prefix,
        &export,
        &relpath,
        &sig,
        piece_idx,
    );
    let resp_bytes = buf.clone();
    let mut v = FlatDict::new();
    v.insert(exp.cache_bytes_field_key.clone(), FlatValue::Bytes(buf));
    if let Err(e) = api.kv().put(&key, v) {
        return resp_err_kverr(e);
    }
    // English note (causal chain):
    // - `stage_to_kv_then_read` should not require an extra KV GET roundtrip on the caller side.
    // - The agent already has the piece bytes in memory; we return them as part of the RPC response
    //   so the caller can serve the bytes immediately while still populating KV.
    //
    // Contract: callers must treat missing `data` as a hard error (no fallback).
    resp_ok(BTreeMap::from([(
        "data".to_string(),
        FlatValue::Bytes(resp_bytes),
    )]))
}

fn handle_s3_load_part_file_range_to_kv(
    api: Arc<FluxonUserApi>,
    exports: &AgentExportsHandle,
    access_model: &AgentAccessModelHandle,
    payload: FlatDict,
) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let relpath = match require_str(&payload, "relpath") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if let Err(resp) = authorize_relpath_mode(
        access_model,
        &payload,
        &export,
        &relpath,
        access_model_required_mode_for_op(FluxonFsOp::ReadChunk),
    ) {
        return resp;
    }
    let sig = match require_str(&payload, "sig") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let start_piece_idx = match require_i64(&payload, "start_piece_idx") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let piece_count = match require_i64(&payload, "piece_count") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if start_piece_idx < 0 || piece_count <= 0 {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "invalid piece range: start_piece_idx={} piece_count={}",
                start_piece_idx, piece_count
            ),
        }));
    }
    let exp = match exports.rpc_export(&export) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if exp.cache_kv_key_prefix.trim().is_empty() {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "export.cache_kv_key_prefix must be non-empty: export={}",
                export
            ),
        }));
    }
    if exp.cache_bytes_field_key.trim().is_empty() {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "export.cache_bytes_field_key must be non-empty: export={}",
                export
            ),
        }));
    }

    let p = match safe_join_root(&exp.remote_root_dir_abs, &relpath) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let md = match fs::metadata(&p) {
        Ok(v) => v,
        Err(e) => return resp_err_io(e),
    };
    if !md.is_file() {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: "relpath must refer to a file".to_string(),
        }));
    }

    let size = md.len() as i64;
    let mtime_ns = md
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| (d.as_nanos() as i128).min(i64::MAX as i128) as i64)
        .unwrap_or(0);
    let actual_sig = fluxon_fs_core::s3_gateway::object_sig_string(size, mtime_ns);
    if actual_sig != sig {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "sig mismatch: expected_sig={} actual_sig={} export={} relpath={}",
                sig, actual_sig, export, relpath
            ),
        }));
    }

    let piece_bytes = fluxon_fs_core::s3_gateway::FS_S3_OBJECT_CHUNK_BYTES as i64;
    let start_off = match start_piece_idx.checked_mul(piece_bytes) {
        Some(v) => v,
        None => {
            return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "start_piece_idx overflow: start_piece_idx={} piece_bytes={}",
                    start_piece_idx, piece_bytes
                ),
            }));
        }
    };
    if start_off < 0 || start_off >= size {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "piece range out of file: start_piece_idx={} offset={} size={}",
                start_piece_idx, start_off, size
            ),
        }));
    }
    let max_bytes = match piece_count.checked_mul(piece_bytes) {
        Some(v) => v,
        None => {
            return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "piece_count overflow: piece_count={} piece_bytes={}",
                    piece_count, piece_bytes
                ),
            }));
        }
    };
    if max_bytes <= 0 {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "invalid piece_count bytes: start_piece_idx={} piece_count={}",
                start_piece_idx, piece_count
            ),
        }));
    }
    let end_off = std::cmp::min(size, start_off.saturating_add(max_bytes));
    if end_off <= start_off {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: "empty piece range".to_string(),
        }));
    }
    let want = (end_off - start_off) as usize;
    let mut f = match fs::File::open(&p) {
        Ok(v) => v,
        Err(e) => return resp_err_io(e),
    };
    if let Err(e) = f.seek(SeekFrom::Start(start_off as u64)) {
        return resp_err_io(e);
    }
    let mut buf = vec![0u8; want];
    if let Err(e) = f.read_exact(&mut buf) {
        return resp_err_io(e);
    }

    let mut staged_count: i64 = 0;
    for i in 0..piece_count {
        let piece_idx = start_piece_idx + i;
        let piece_off = match piece_idx.checked_mul(piece_bytes) {
            Some(v) => v,
            None => break,
        };
        if piece_off >= size {
            break;
        }
        let piece_end = std::cmp::min(size, piece_off.saturating_add(piece_bytes));
        if piece_end <= piece_off {
            break;
        }
        let rel_start = (piece_off - start_off) as usize;
        let rel_end = (piece_end - start_off) as usize;
        if rel_end > buf.len() || rel_start >= rel_end {
            break;
        }
        let key = fluxon_fs_core::s3_gateway::kv_piece_key(
            &exp.cache_kv_key_prefix,
            &export,
            &relpath,
            &sig,
            piece_idx,
        );
        let mut v = FlatDict::new();
        v.insert(
            exp.cache_bytes_field_key.clone(),
            FlatValue::Bytes(buf[rel_start..rel_end].to_vec()),
        );
        if let Err(e) = api.kv().put(&key, v) {
            return resp_err_kverr(e);
        }
        staged_count += 1;
    }
    resp_ok(BTreeMap::from([
        ("data".to_string(), FlatValue::Bytes(buf)),
        ("staged_count".to_string(), FlatValue::Int64(staged_count)),
    ]))
}

fn handle_read_chunk(
    exports: &AgentExportsHandle,
    access_model: &AgentAccessModelHandle,
    payload: FlatDict,
) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let relpath = match require_str(&payload, "relpath") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if let Err(resp) = authorize_relpath_mode(
        access_model,
        &payload,
        &export,
        &relpath,
        access_model_required_mode_for_op(FluxonFsOp::ReadChunk),
    ) {
        return resp;
    }
    let offset = match require_i64(&payload, "offset") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let length = match require_i64(&payload, "length") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if offset < 0 || length < 0 || (length as usize) > READ_CHUNK_BYTES {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: "offset/length out of range".to_string(),
        }));
    }
    let root_dir_abs = match exports.export_root_dir_abs(&export) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let p = match safe_join_root(&root_dir_abs, &relpath) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let mut f = match fs::File::open(&p) {
        Ok(v) => v,
        Err(e) => return resp_err_io(e),
    };
    let md = match f.metadata() {
        Ok(v) => v,
        Err(e) => return resp_err_io(e),
    };
    let size = md.len() as i64;
    if offset > size {
        return resp_ok(BTreeMap::from([(
            "data".to_string(),
            FlatValue::Bytes(Vec::new()),
        )]));
    }
    let to_read = std::cmp::min(length, size - offset) as usize;
    if let Err(e) = f.seek(SeekFrom::Start(offset as u64)) {
        return resp_err_io(e);
    }
    let mut buf = vec![0u8; to_read];
    if let Err(e) = f.read_exact(&mut buf) {
        return resp_err_io(e);
    }
    resp_ok(BTreeMap::from([(
        "data".to_string(),
        FlatValue::Bytes(buf),
    )]))
}

fn handle_open_write_session(
    exports: &AgentExportsHandle,
    access_model: &AgentAccessModelHandle,
    write_sessions: &AgentWriteSessionsHandle,
    payload: FlatDict,
) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let relpath = match require_str(&payload, "relpath") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if let Err(resp) = authorize_relpath_mode(
        access_model,
        &payload,
        &export,
        &relpath,
        access_model_required_mode_for_op(FluxonFsOp::WriteChunk),
    ) {
        return resp;
    }
    let root_dir_abs = match exports.export_root_dir_abs(&export) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let p = match safe_join_root(&root_dir_abs, &relpath) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let session_id = write_sessions.alloc_id();
    let write_path_key = write_path_key(&export, &p);
    let write_path_owner = format!("session:{}", session_id);
    if let Err(existing) = write_sessions
        .active_write_paths
        .try_acquire_owned(&write_path_key, &write_path_owner)
    {
        return resp_err_busy(active_write_owner_detail(&existing));
    }
    let file = match fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&p)
    {
        Ok(v) => v,
        Err(e) => {
            write_sessions
                .active_write_paths
                .release_owned(&write_path_key, &write_path_owner);
            return resp_err_io(e);
        }
    };
    let md = match file.metadata() {
        Ok(v) => v,
        Err(e) => {
            write_sessions
                .active_write_paths
                .release_owned(&write_path_key, &write_path_owner);
            return resp_err_io(e);
        }
    };
    let (_is_file, _is_dir, size, mtime_ns, _mode) = stat_fields_from_metadata(&md);
    write_sessions.insert(
        session_id.clone(),
        WriteSessionEntry {
            export_name: export,
            relpath,
            write_path_key,
            write_path_owner,
            current_pos: size.max(0) as u64,
            last_touched: Instant::now(),
            queued_chunks: VecDeque::new(),
            queued_bytes: 0,
            scheduled: false,
            writing: false,
            closing: false,
            aborted: false,
            fatal_error: None,
            expected_data_frames: None,
            highest_received_seq: None,
            highest_written_seq: None,
            received_data_frame_seqs: BTreeSet::new(),
            written_data_frame_seqs: BTreeSet::new(),
            timing: WriteSessionTimingStats::default(),
            created_at: Instant::now(),
        },
        file,
    );
    resp_ok(BTreeMap::from([
        ("session_id".to_string(), FlatValue::String(session_id)),
        ("size".to_string(), FlatValue::Int64(size)),
        ("mtime_ns".to_string(), FlatValue::Int64(mtime_ns)),
        (
            "chunk_bytes".to_string(),
            FlatValue::Int64(WRITE_SESSION_CHUNK_BYTES as i64),
        ),
    ]))
}

fn insert_typed_request_auth_payload(payload: &mut FlatDict, fs_rpc_token: Option<String>) {
    if let Some(token) = fs_rpc_token {
        payload.insert(
            FLUXON_FS_RPC_TOKEN_PAYLOAD_KEY.to_string(),
            FlatValue::String(token),
        );
    }
}

fn handle_open_write_session_typed(
    exports: &AgentExportsHandle,
    access_model: &AgentAccessModelHandle,
    write_sessions: &AgentWriteSessionsHandle,
    req: FsOpenWriteSessionReq,
) -> FsOpenWriteSessionResp {
    let mut payload = FlatDict::from([
        ("export".to_string(), FlatValue::String(req.export)),
        ("relpath".to_string(), FlatValue::String(req.relpath)),
    ]);
    insert_typed_request_auth_payload(&mut payload, req.fs_rpc_token);
    if req.allow_s3_internal_multipart {
        payload.insert(
            S3_INTERNAL_MULTIPART_PAYLOAD_KEY.to_string(),
            FlatValue::Bool(true),
        );
    }
    let resp = handle_open_write_session(exports, access_model, write_sessions, payload);
    FsOpenWriteSessionResp {
        ok: matches!(resp.get("ok"), Some(FlatValue::Bool(true))),
        err_kind: match resp.get(FS_AGENT_RPC_ERR_KIND_KEY) {
            Some(FlatValue::Int64(v)) => *v,
            _ => 0,
        },
        err_detail: match resp.get("err") {
            Some(FlatValue::String(v)) => v.clone(),
            _ => String::new(),
        },
        err_errno: match resp.get("errno") {
            Some(FlatValue::Int64(v)) => i32::try_from(*v).unwrap_or(libc::EIO),
            _ => 0,
        },
        session_id: match resp.get("session_id") {
            Some(FlatValue::String(v)) => v.clone(),
            _ => String::new(),
        },
        size: match resp.get("size") {
            Some(FlatValue::Int64(v)) => *v,
            _ => 0,
        },
        mtime_ns: match resp.get("mtime_ns") {
            Some(FlatValue::Int64(v)) => *v,
            _ => 0,
        },
        chunk_bytes: match resp.get("chunk_bytes") {
            Some(FlatValue::Int64(v)) => *v,
            _ => 0,
        },
    }
}

fn handle_write_session_chunk_typed(
    access_model: &AgentAccessModelHandle,
    write_sessions: &AgentWriteSessionsHandle,
    write_executor: &WriteExecutorHandle,
    req: FsWriteSessionChunkReq,
    data: Vec<u8>,
) -> FsWriteSessionChunkResp {
    let mut payload = FlatDict::from([
        ("export".to_string(), FlatValue::String(req.export)),
        ("relpath".to_string(), FlatValue::String(req.relpath)),
        ("session_id".to_string(), FlatValue::String(req.session_id)),
        ("offset".to_string(), FlatValue::Int64(req.offset)),
        ("data".to_string(), FlatValue::Bytes(data)),
    ]);
    insert_typed_request_auth_payload(&mut payload, req.fs_rpc_token);
    if req.allow_s3_internal_multipart {
        payload.insert(
            S3_INTERNAL_MULTIPART_PAYLOAD_KEY.to_string(),
            FlatValue::Bool(true),
        );
    }
    let resp = handle_write_session_chunk(access_model, write_sessions, write_executor, payload);
    FsWriteSessionChunkResp {
        ok: matches!(resp.get("ok"), Some(FlatValue::Bool(true))),
        err_kind: match resp.get(FS_AGENT_RPC_ERR_KIND_KEY) {
            Some(FlatValue::Int64(v)) => *v,
            _ => 0,
        },
        err_detail: match resp.get("err") {
            Some(FlatValue::String(v)) => v.clone(),
            _ => String::new(),
        },
        err_errno: match resp.get("errno") {
            Some(FlatValue::Int64(v)) => i32::try_from(*v).unwrap_or(libc::EIO),
            _ => 0,
        },
    }
}

fn enqueue_write_session_chunk(
    write_sessions: &AgentWriteSessionsHandle,
    write_executor: &WriteExecutorHandle,
    export: &str,
    relpath: &str,
    session_id: &str,
    seq_no: u64,
    offset: i64,
    data: Bytes,
    is_data_frame: bool,
) -> FlatDict {
    let state_lock_started = Instant::now();
    if offset < 0 {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: "offset must be non-negative".to_string(),
        }));
    }
    if data.len() > WRITE_SESSION_CHUNK_BYTES {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: "session chunk too large".to_string(),
        }));
    }
    let Some(entry_handle) = write_sessions.get(session_id) else {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!("unknown write session: {}", session_id),
        }));
    };
    let mut state = entry_handle.state.lock();
    state.timing.enqueue_state_lock_wait_ns = state
        .timing
        .enqueue_state_lock_wait_ns
        .saturating_add(state_lock_started.elapsed().as_nanos() as u64);
    if state.export_name != export || state.relpath != relpath {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: "write session export/relpath mismatch".to_string(),
        }));
    }
    let queued_bytes_before = state.queued_bytes;
    while state.queued_bytes.saturating_add(data.len()) > WRITE_SESSION_MAX_QUEUED_BYTES
        && !state.aborted
        && !state.closing
        && state.fatal_error.is_none()
    {
        let wait_started = Instant::now();
        entry_handle.cv.wait(&mut state);
        state.timing.enqueue_backpressure_wait_ns = state
            .timing
            .enqueue_backpressure_wait_ns
            .saturating_add(wait_started.elapsed().as_nanos() as u64);
    }
    if state.aborted {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!("write session aborted: {}", session_id),
        }));
    }
    if !write_session_accepts_chunk_while_closing(&state, seq_no, is_data_frame) {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!("write session is closing: {}", session_id),
        }));
    }
    if let Some(err) = state.fatal_error.as_ref() {
        return resp_err(FsAgentRpcErrorKind::Os, err.detail.clone(), Some(err.errno));
    }
    if is_data_frame {
        if let Some(expected) = state.expected_data_frames {
            if seq_no >= expected {
                return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "write session data frame seq out of range: session={} seq={} expected={}",
                        session_id, seq_no, expected
                    ),
                }));
            }
        }
        if state.written_data_frame_seqs.contains(&seq_no)
            || !state.received_data_frame_seqs.insert(seq_no)
        {
            state.last_touched = Instant::now();
            entry_handle.cv.notify_all();
            return resp_ok(BTreeMap::new());
        }
    }
    state.queued_bytes = state.queued_bytes.saturating_add(data.len());
    state.timing.bytes_enqueued = state
        .timing
        .bytes_enqueued
        .saturating_add(data.len() as u64);
    state.timing.chunks_enqueued = state.timing.chunks_enqueued.saturating_add(1);
    state.highest_received_seq = Some(
        state
            .highest_received_seq
            .map(|prev| prev.max(seq_no))
            .unwrap_or(seq_no),
    );
    state.queued_chunks.push_back(QueuedWriteChunk {
        seq_no,
        offset: offset as u64,
        data,
        is_data_frame,
    });
    state.last_touched = Instant::now();
    let should_schedule = !state.scheduled;
    if should_schedule {
        state.scheduled = true;
    }
    drop(state);
    if should_schedule {
        write_executor.schedule(entry_handle.clone());
    }
    resp_ok(BTreeMap::new())
}

fn handle_write_session_chunk(
    access_model: &AgentAccessModelHandle,
    write_sessions: &AgentWriteSessionsHandle,
    write_executor: &WriteExecutorHandle,
    payload: FlatDict,
) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let relpath = match require_str(&payload, "relpath") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if let Err(resp) = authorize_relpath_mode(
        access_model,
        &payload,
        &export,
        &relpath,
        access_model_required_mode_for_op(FluxonFsOp::WriteChunk),
    ) {
        return resp;
    }
    let session_id = match require_str(&payload, "session_id") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let offset = match require_i64(&payload, "offset") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let data = match payload.get("data") {
        Some(FlatValue::Bytes(b)) => Bytes::copy_from_slice(b.as_slice()),
        _ => {
            return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
                detail: "data must be bytes".to_string(),
            }));
        }
    };
    enqueue_write_session_chunk(
        write_sessions,
        write_executor,
        &export,
        &relpath,
        &session_id,
        0,
        offset,
        data,
        false,
    )
}

fn handle_write_session_data_oneway(
    access_model: &AgentAccessModelHandle,
    write_sessions: &AgentWriteSessionsHandle,
    write_executor: &WriteExecutorHandle,
    req: FsWriteSessionDataFrame,
    payloads: Vec<Bytes>,
) -> KvResult<()> {
    let mut payload = FlatDict::from([
        ("export".to_string(), FlatValue::String(req.export.clone())),
        (
            "relpath".to_string(),
            FlatValue::String(req.relpath.clone()),
        ),
    ]);
    insert_typed_request_auth_payload(&mut payload, req.fs_rpc_token.clone());
    if req.allow_s3_internal_multipart {
        payload.insert(
            S3_INTERNAL_MULTIPART_PAYLOAD_KEY.to_string(),
            FlatValue::Bool(true),
        );
    }
    if let Err(resp) = authorize_relpath_mode(
        access_model,
        &payload,
        &req.export,
        &req.relpath,
        access_model_required_mode_for_op(FluxonFsOp::WriteChunk),
    ) {
        let detail = match resp.get("err") {
            Some(FlatValue::String(v)) => v.clone(),
            _ => "write_session data authorize failed".to_string(),
        };
        return Err(KvError::Api(ApiError::InvalidArgument { detail }));
    }
    let mut next_offset = req.offset;
    for (idx, data) in payloads.into_iter().enumerate() {
        let data_len = data.len() as i64;
        let resp = enqueue_write_session_chunk(
            write_sessions,
            write_executor,
            &req.export,
            &req.relpath,
            &req.session_id,
            req.seq_no.saturating_add(idx as u64),
            next_offset,
            data,
            true,
        );
        if !matches!(resp.get("ok"), Some(FlatValue::Bool(true))) {
            let detail = match resp.get("err") {
                Some(FlatValue::String(v)) => v.clone(),
                _ => "write_session data enqueue failed".to_string(),
            };
            return Err(KvError::Api(ApiError::InvalidArgument { detail }));
        }
        next_offset = next_offset.saturating_add(data_len);
    }
    Ok(())
}

fn handle_write_session_data_typed(
    access_model: &AgentAccessModelHandle,
    write_sessions: &AgentWriteSessionsHandle,
    write_executor: &WriteExecutorHandle,
    req: FsWriteSessionDataFrame,
    payloads: Vec<Bytes>,
) -> crate::write_session_rpc::FsWriteSessionDataAck {
    let payload_frame_count = payloads.len() as u64;
    let session_id = req.session_id.clone();
    let seq_no = req.seq_no;
    match handle_write_session_data_oneway(
        access_model,
        write_sessions,
        write_executor,
        req,
        payloads,
    ) {
        Ok(()) => crate::write_session_rpc::FsWriteSessionDataAck {
            session_id,
            seq_no,
            frame_count: payload_frame_count,
            ok: true,
            err_detail: String::new(),
        },
        Err(err) => crate::write_session_rpc::FsWriteSessionDataAck {
            session_id,
            seq_no,
            frame_count: payload_frame_count,
            ok: false,
            err_detail: err.to_string(),
        },
    }
}

fn handle_truncate_write_session(
    access_model: &AgentAccessModelHandle,
    write_sessions: &AgentWriteSessionsHandle,
    payload: FlatDict,
) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let relpath = match require_str(&payload, "relpath") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if let Err(resp) = authorize_relpath_mode(
        access_model,
        &payload,
        &export,
        &relpath,
        access_model_required_mode_for_op(FluxonFsOp::Truncate),
    ) {
        return resp;
    }
    let session_id = match require_str(&payload, "session_id") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let size = match require_i64(&payload, "size") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if size < 0 {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: "size must be non-negative".to_string(),
        }));
    }
    let Some(entry_handle) = write_sessions.get(&session_id) else {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!("unknown write session: {}", session_id),
        }));
    };
    let mut state = entry_handle.state.lock();
    if state.export_name != export || state.relpath != relpath {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: "write session export/relpath mismatch".to_string(),
        }));
    }
    while state.writing || state.scheduled || !state.queued_chunks.is_empty() {
        entry_handle.cv.wait(&mut state);
    }
    if state.aborted {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!("write session aborted: {}", session_id),
        }));
    }
    if let Some(err) = state.fatal_error.as_ref() {
        return resp_err(FsAgentRpcErrorKind::Os, err.detail.clone(), Some(err.errno));
    }
    drop(state);
    let file = entry_handle.file.lock();
    if let Err(e) = file.set_len(size as u64) {
        return resp_err_io(e);
    }
    drop(file);
    let mut state = entry_handle.state.lock();
    if state.current_pos > size as u64 {
        state.current_pos = size as u64;
    }
    state.last_touched = Instant::now();
    entry_handle.cv.notify_all();
    resp_ok(BTreeMap::new())
}

fn handle_close_write_session(
    access_model: &AgentAccessModelHandle,
    write_sessions: &AgentWriteSessionsHandle,
    payload: FlatDict,
) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let relpath = match require_str(&payload, "relpath") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if let Err(resp) = authorize_relpath_mode(
        access_model,
        &payload,
        &export,
        &relpath,
        access_model_required_mode_for_op(FluxonFsOp::WriteChunk),
    ) {
        return resp;
    }
    let session_id = match require_str(&payload, "session_id") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let expected_data_frames = match payload.get("expected_data_frames") {
        Some(FlatValue::Int64(v)) if *v >= 0 => Some(*v as u64),
        Some(_) => {
            return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
                detail: "expected_data_frames must be non-negative int64".to_string(),
            }));
        }
        None => None,
    };
    let Some(entry_handle) = write_sessions.get(&session_id) else {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!("unknown write session: {}", session_id),
        }));
    };
    let mut state = entry_handle.state.lock();
    if state.export_name != export || state.relpath != relpath {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: "write session export/relpath mismatch".to_string(),
        }));
    }
    state.expected_data_frames =
        write_session_merge_expected_frames(state.expected_data_frames, expected_data_frames);
    state.closing = true;
    state.last_touched = Instant::now();
    let close_deadline =
        Instant::now() + Duration::from_secs(WRITE_SESSION_CLOSE_WAIT_TIMEOUT_SECS);
    while state.writing
        || state.scheduled
        || !state.queued_chunks.is_empty()
        || write_session_pending_expected_frames(&state)
    {
        let now = Instant::now();
        if now >= close_deadline {
            let detail = write_session_close_timeout_detail(&session_id, &state);
            state.aborted = true;
            state.queued_chunks.clear();
            state.queued_bytes = 0;
            state.writing = false;
            state.scheduled = false;
            entry_handle.cv.notify_all();
            drop(state);
            let _ = write_sessions.take(&session_id);
            return resp_err_kverr(KvError::Api(ApiError::Unknown { detail }));
        }
        let wait_started = Instant::now();
        entry_handle
            .cv
            .wait_for(&mut state, close_deadline.saturating_duration_since(now));
        state.timing.close_wait_ns = state
            .timing
            .close_wait_ns
            .saturating_add(wait_started.elapsed().as_nanos() as u64);
    }
    if state.aborted {
        drop(state);
        let _ = write_sessions.take(&session_id);
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!("write session aborted: {}", session_id),
        }));
    }
    if let Some(err) = state.fatal_error.as_ref() {
        let resp = resp_err(FsAgentRpcErrorKind::Os, err.detail.clone(), Some(err.errno));
        drop(state);
        let _ = write_sessions.take(&session_id);
        return resp;
    }
    drop(state);
    let file = entry_handle.file.lock();
    let md = match file.metadata() {
        Ok(v) => v,
        Err(e) => {
            drop(file);
            let _ = write_sessions.take(&session_id);
            return resp_err_io(e);
        }
    };
    let (_is_file, _is_dir, size, mtime_ns, _mode) = stat_fields_from_metadata(&md);
    drop(file);
    let _ = write_sessions.take(&session_id);
    resp_ok(BTreeMap::from([
        ("size".to_string(), FlatValue::Int64(size)),
        ("mtime_ns".to_string(), FlatValue::Int64(mtime_ns)),
    ]))
}

fn handle_close_write_session_typed(
    access_model: &AgentAccessModelHandle,
    write_sessions: &AgentWriteSessionsHandle,
    req: FsCloseWriteSessionReq,
) -> FsCloseWriteSessionResp {
    let mut payload = FlatDict::from([
        ("export".to_string(), FlatValue::String(req.export)),
        ("relpath".to_string(), FlatValue::String(req.relpath)),
        ("session_id".to_string(), FlatValue::String(req.session_id)),
        (
            "expected_data_frames".to_string(),
            FlatValue::Int64(i64::try_from(req.expected_data_frames).unwrap_or(i64::MAX)),
        ),
    ]);
    insert_typed_request_auth_payload(&mut payload, req.fs_rpc_token);
    if req.allow_s3_internal_multipart {
        payload.insert(
            S3_INTERNAL_MULTIPART_PAYLOAD_KEY.to_string(),
            FlatValue::Bool(true),
        );
    }
    let resp = handle_close_write_session(access_model, write_sessions, payload);
    FsCloseWriteSessionResp {
        ok: matches!(resp.get("ok"), Some(FlatValue::Bool(true))),
        err_kind: match resp.get(FS_AGENT_RPC_ERR_KIND_KEY) {
            Some(FlatValue::Int64(v)) => *v,
            _ => 0,
        },
        err_detail: match resp.get("err") {
            Some(FlatValue::String(v)) => v.clone(),
            _ => String::new(),
        },
        err_errno: match resp.get("errno") {
            Some(FlatValue::Int64(v)) => i32::try_from(*v).unwrap_or(libc::EIO),
            _ => 0,
        },
        size: match resp.get("size") {
            Some(FlatValue::Int64(v)) => *v,
            _ => 0,
        },
        mtime_ns: match resp.get("mtime_ns") {
            Some(FlatValue::Int64(v)) => *v,
            _ => 0,
        },
    }
}

fn handle_wait_write_session_payloads(
    access_model: &AgentAccessModelHandle,
    write_sessions: &AgentWriteSessionsHandle,
    payload: FlatDict,
) -> FlatDict {
    let wait_started = Instant::now();
    let export = match require_str(&payload, "export") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let relpath = match require_str(&payload, "relpath") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if let Err(resp) = authorize_relpath_mode(
        access_model,
        &payload,
        &export,
        &relpath,
        access_model_required_mode_for_op(FluxonFsOp::WriteChunk),
    ) {
        return resp;
    }
    let session_id = match require_str(&payload, "session_id") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let expected_data_frames = match payload.get("expected_data_frames") {
        Some(FlatValue::Int64(v)) if *v >= 0 => *v as u64,
        Some(_) => {
            return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
                detail: "expected_data_frames must be non-negative int64".to_string(),
            }));
        }
        None => 0,
    };
    if expected_data_frames == 0 {
        return resp_ok(BTreeMap::new());
    }
    let Some(entry_handle) = write_sessions.get(&session_id) else {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!("unknown write session: {}", session_id),
        }));
    };
    let mut state = entry_handle.state.lock();
    if state.export_name != export || state.relpath != relpath {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: "write session export/relpath mismatch".to_string(),
        }));
    }
    state.expected_data_frames =
        write_session_merge_expected_frames(state.expected_data_frames, Some(expected_data_frames));
    state.last_touched = Instant::now();
    let deadline = Instant::now() + Duration::from_secs(WRITE_SESSION_CLOSE_WAIT_TIMEOUT_SECS);
    while write_session_pending_received_frames(&state, expected_data_frames) {
        if state.aborted {
            return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
                detail: format!("write session aborted: {}", session_id),
            }));
        }
        if let Some(err) = state.fatal_error.as_ref() {
            return resp_err(FsAgentRpcErrorKind::Os, err.detail.clone(), Some(err.errno));
        }
        let now = Instant::now();
        if now >= deadline {
            return resp_err_kverr(KvError::Api(ApiError::Unknown {
                detail: write_session_close_timeout_detail(&session_id, &state),
            }));
        }
        entry_handle
            .cv
            .wait_for(&mut state, deadline.saturating_duration_since(now));
    }
    resp_ok(BTreeMap::new())
}

fn handle_wait_write_session_payloads_typed(
    access_model: &AgentAccessModelHandle,
    write_sessions: &AgentWriteSessionsHandle,
    req: FsWaitWriteSessionPayloadsReq,
) -> FsWaitWriteSessionPayloadsResp {
    let mut payload = FlatDict::from([
        ("export".to_string(), FlatValue::String(req.export)),
        ("relpath".to_string(), FlatValue::String(req.relpath)),
        ("session_id".to_string(), FlatValue::String(req.session_id)),
        (
            "expected_data_frames".to_string(),
            FlatValue::Int64(i64::try_from(req.expected_data_frames).unwrap_or(i64::MAX)),
        ),
    ]);
    insert_typed_request_auth_payload(&mut payload, req.fs_rpc_token);
    if req.allow_s3_internal_multipart {
        payload.insert(
            S3_INTERNAL_MULTIPART_PAYLOAD_KEY.to_string(),
            FlatValue::Bool(true),
        );
    }
    let resp = handle_wait_write_session_payloads(access_model, write_sessions, payload);
    FsWaitWriteSessionPayloadsResp {
        ok: matches!(resp.get("ok"), Some(FlatValue::Bool(true))),
        err_kind: match resp.get(FS_AGENT_RPC_ERR_KIND_KEY) {
            Some(FlatValue::Int64(v)) => *v,
            _ => 0,
        },
        err_detail: match resp.get("err") {
            Some(FlatValue::String(v)) => v.clone(),
            _ => String::new(),
        },
        err_errno: match resp.get("errno") {
            Some(FlatValue::Int64(v)) => i32::try_from(*v).unwrap_or(libc::EIO),
            _ => 0,
        },
    }
}

fn handle_abort_write_session(
    access_model: &AgentAccessModelHandle,
    write_sessions: &AgentWriteSessionsHandle,
    payload: FlatDict,
) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let relpath = match require_str(&payload, "relpath") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if let Err(resp) = authorize_relpath_mode(
        access_model,
        &payload,
        &export,
        &relpath,
        access_model_required_mode_for_op(FluxonFsOp::WriteChunk),
    ) {
        return resp;
    }
    let session_id = match require_str(&payload, "session_id") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    {
        let entries = write_sessions.entries.lock();
        if let Some(existing) = entries.get(&session_id) {
            let mut state = existing.state.lock();
            if state.export_name != export || state.relpath != relpath {
                return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
                    detail: "write session export/relpath mismatch".to_string(),
                }));
            }
            state.aborted = true;
            state.queued_chunks.clear();
            state.queued_bytes = 0;
            existing.cv.notify_all();
        }
    }
    let _ = write_sessions.take(&session_id);
    resp_ok(BTreeMap::new())
}

fn handle_abort_write_session_typed(
    access_model: &AgentAccessModelHandle,
    write_sessions: &AgentWriteSessionsHandle,
    req: FsAbortWriteSessionReq,
) -> FsAbortWriteSessionResp {
    let mut payload = FlatDict::from([
        ("export".to_string(), FlatValue::String(req.export)),
        ("relpath".to_string(), FlatValue::String(req.relpath)),
        ("session_id".to_string(), FlatValue::String(req.session_id)),
    ]);
    insert_typed_request_auth_payload(&mut payload, req.fs_rpc_token);
    if req.allow_s3_internal_multipart {
        payload.insert(
            S3_INTERNAL_MULTIPART_PAYLOAD_KEY.to_string(),
            FlatValue::Bool(true),
        );
    }
    let resp = handle_abort_write_session(access_model, write_sessions, payload);
    FsAbortWriteSessionResp {
        ok: matches!(resp.get("ok"), Some(FlatValue::Bool(true))),
        err_kind: match resp.get(FS_AGENT_RPC_ERR_KIND_KEY) {
            Some(FlatValue::Int64(v)) => *v,
            _ => 0,
        },
        err_detail: match resp.get("err") {
            Some(FlatValue::String(v)) => v.clone(),
            _ => String::new(),
        },
        err_errno: match resp.get("errno") {
            Some(FlatValue::Int64(v)) => i32::try_from(*v).unwrap_or(libc::EIO),
            _ => 0,
        },
    }
}

fn handle_write_chunk(
    exports: &AgentExportsHandle,
    access_model: &AgentAccessModelHandle,
    active_write_paths: &ActiveWritePathsHandle,
    payload: FlatDict,
) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let relpath = match require_str(&payload, "relpath") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if let Err(resp) = authorize_relpath_mode(
        access_model,
        &payload,
        &export,
        &relpath,
        access_model_required_mode_for_op(FluxonFsOp::WriteChunk),
    ) {
        return resp;
    }
    let offset = match require_i64(&payload, "offset") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let data = match payload.get("data") {
        Some(FlatValue::Bytes(b)) => b.as_slice(),
        _ => {
            return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
                detail: "data must be bytes".to_string(),
            }));
        }
    };
    if offset < 0 {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: "offset must be non-negative".to_string(),
        }));
    }
    if data.len() > CHUNK_BYTES {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: "chunk too large".to_string(),
        }));
    }
    let root_dir_abs = match exports.export_root_dir_abs(&export) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let p = match safe_join_root(&root_dir_abs, &relpath) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let write_path_key = write_path_key(&export, &p);
    let _write_path_guard = match acquire_transient_write_path_guard(
        active_write_paths,
        &write_path_key,
        "write_chunk",
    ) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let mut f = match fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&p)
    {
        Ok(v) => v,
        Err(e) => return resp_err_io(e),
    };
    if let Err(e) = f.seek(SeekFrom::Start(offset as u64)) {
        return resp_err_io(e);
    }
    if let Err(e) = f.write_all(data) {
        return resp_err_io(e);
    }
    resp_ok(BTreeMap::new())
}

fn handle_truncate(
    exports: &AgentExportsHandle,
    access_model: &AgentAccessModelHandle,
    active_write_paths: &ActiveWritePathsHandle,
    payload: FlatDict,
) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let relpath = match require_str(&payload, "relpath") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if let Err(resp) = authorize_relpath_mode(
        access_model,
        &payload,
        &export,
        &relpath,
        access_model_required_mode_for_op(FluxonFsOp::Truncate),
    ) {
        return resp;
    }
    let size = match require_i64(&payload, "size") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if size < 0 {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: "size must be non-negative".to_string(),
        }));
    }
    let root_dir_abs = match exports.export_root_dir_abs(&export) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let p = match safe_join_root(&root_dir_abs, &relpath) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let write_path_key = write_path_key(&export, &p);
    let _write_path_guard =
        match acquire_transient_write_path_guard(active_write_paths, &write_path_key, "truncate") {
            Ok(v) => v,
            Err(resp) => return resp,
        };
    let f = match fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&p)
    {
        Ok(v) => v,
        Err(e) => return resp_err_io(e),
    };
    if let Err(e) = f.set_len(size as u64) {
        return resp_err_io(e);
    }
    resp_ok(BTreeMap::new())
}

fn handle_mkdir(
    exports: &AgentExportsHandle,
    access_model: &AgentAccessModelHandle,
    payload: FlatDict,
) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let relpath = match require_str(&payload, "relpath") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if let Err(resp) = authorize_relpath_mode(
        access_model,
        &payload,
        &export,
        &relpath,
        access_model_required_mode_for_op(FluxonFsOp::Mkdir),
    ) {
        return resp;
    }
    let mode = match require_i64(&payload, "mode") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let root_dir_abs = match exports.export_root_dir_abs(&export) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let p = match safe_join_root(&root_dir_abs, &relpath) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        let mut b = fs::DirBuilder::new();
        b.mode(mode as u32);
        if let Err(e) = b.create(&p) {
            return resp_err_io(e);
        }
        return resp_ok(BTreeMap::new());
    }
    #[cfg(not(unix))]
    {
        let _ = mode;
        if let Err(e) = fs::create_dir(&p) {
            return resp_err_io(e);
        }
        resp_ok(BTreeMap::new())
    }
}

fn handle_rmdir(
    exports: &AgentExportsHandle,
    access_model: &AgentAccessModelHandle,
    payload: FlatDict,
) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let relpath = match require_str(&payload, "relpath") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if let Err(resp) = authorize_relpath_mode(
        access_model,
        &payload,
        &export,
        &relpath,
        access_model_required_mode_for_op(FluxonFsOp::Rmdir),
    ) {
        return resp;
    }
    let root_dir_abs = match exports.export_root_dir_abs(&export) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let p = match safe_join_root(&root_dir_abs, &relpath) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if let Err(e) = fs::remove_dir(&p) {
        return resp_err_io(e);
    }
    resp_ok(BTreeMap::new())
}

fn handle_unlink(
    exports: &AgentExportsHandle,
    access_model: &AgentAccessModelHandle,
    active_write_paths: &ActiveWritePathsHandle,
    payload: FlatDict,
) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let relpath = match require_str(&payload, "relpath") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if let Err(resp) = authorize_relpath_mode(
        access_model,
        &payload,
        &export,
        &relpath,
        access_model_required_mode_for_op(FluxonFsOp::Unlink),
    ) {
        return resp;
    }
    let root_dir_abs = match exports.export_root_dir_abs(&export) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let p = match safe_join_root(&root_dir_abs, &relpath) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let write_path_key = write_path_key(&export, &p);
    let _write_path_guard =
        match acquire_transient_write_path_guard(active_write_paths, &write_path_key, "unlink") {
            Ok(v) => v,
            Err(resp) => return resp,
        };
    if let Err(e) = fs::remove_file(&p) {
        return resp_err_io(e);
    }
    resp_ok(BTreeMap::new())
}

fn handle_rename(
    exports: &AgentExportsHandle,
    access_model: &AgentAccessModelHandle,
    active_write_paths: &ActiveWritePathsHandle,
    payload: FlatDict,
) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let src_relpath = match require_str(&payload, "src_relpath") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let dst_relpath = match require_str(&payload, "dst_relpath") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if let Err(resp) =
        authorize_rename_paths(access_model, &payload, &export, &src_relpath, &dst_relpath)
    {
        return resp;
    }
    let root_dir_abs = match exports.export_root_dir_abs(&export) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let src = match safe_join_root(&root_dir_abs, &src_relpath) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let dst = match safe_join_root(&root_dir_abs, &dst_relpath) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let src_write_path_key = write_path_key(&export, &src);
    let dst_write_path_key = write_path_key(&export, &dst);
    let transient_owner = next_transient_write_path_owner("rename");
    let mut guards = Vec::with_capacity(2);
    let mut guard_keys = vec![src_write_path_key.as_str(), dst_write_path_key.as_str()];
    guard_keys.sort_unstable();
    guard_keys.dedup();
    for key in guard_keys {
        let guard = match acquire_transient_write_path_guard_with_owner(
            active_write_paths,
            key,
            &transient_owner,
        ) {
            Ok(v) => v,
            Err(resp) => return resp,
        };
        guards.push(guard);
    }
    if let Err(e) = fs::rename(&src, &dst) {
        return resp_err_io(e);
    }
    resp_ok(BTreeMap::new())
}

fn handle_chmod(
    exports: &AgentExportsHandle,
    access_model: &AgentAccessModelHandle,
    active_write_paths: &ActiveWritePathsHandle,
    payload: FlatDict,
) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let relpath = match require_str(&payload, "relpath") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if let Err(resp) = authorize_relpath_mode(
        access_model,
        &payload,
        &export,
        &relpath,
        access_model_required_mode_for_op(FluxonFsOp::Chmod),
    ) {
        return resp;
    }
    let mode = match require_i64(&payload, "mode") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let root_dir_abs = match exports.export_root_dir_abs(&export) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let p = match safe_join_root(&root_dir_abs, &relpath) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let write_path_key = write_path_key(&export, &p);
    let _write_path_guard =
        match acquire_transient_write_path_guard(active_write_paths, &write_path_key, "chmod") {
            Ok(v) => v,
            Err(resp) => return resp,
        };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perm = fs::Permissions::from_mode(mode as u32);
        if let Err(e) = fs::set_permissions(&p, perm) {
            return resp_err_io(e);
        }
        return resp_ok(BTreeMap::new());
    }
    #[cfg(not(unix))]
    {
        let _ = mode;
        let _ = p;
        resp_err_kverr(KvError::Api(ApiError::NotImplemented {}))
    }
}

fn handle_utime(
    exports: &AgentExportsHandle,
    access_model: &AgentAccessModelHandle,
    active_write_paths: &ActiveWritePathsHandle,
    payload: FlatDict,
) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let relpath = match require_str(&payload, "relpath") {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    if let Err(resp) = authorize_relpath_mode(
        access_model,
        &payload,
        &export,
        &relpath,
        access_model_required_mode_for_op(FluxonFsOp::Utime),
    ) {
        return resp;
    }
    let root_dir_abs = match exports.export_root_dir_abs(&export) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let p = match safe_join_root(&root_dir_abs, &relpath) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let write_path_key = write_path_key(&export, &p);
    let _write_path_guard =
        match acquire_transient_write_path_guard(active_write_paths, &write_path_key, "utime") {
            Ok(v) => v,
            Err(resp) => return resp,
        };

    let atime_ns = payload.get("atime_ns");
    let mtime_ns = payload.get("mtime_ns");

    #[cfg(unix)]
    {
        use std::ffi::CString;
        let c_path = match CString::new(p.to_string_lossy().as_bytes()) {
            Ok(v) => v,
            Err(_) => {
                return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
                    detail: "path contains NUL".to_string(),
                }));
            }
        };

        let rc = if atime_ns.is_none() && mtime_ns.is_none() {
            unsafe { libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), std::ptr::null(), 0) }
        } else {
            let at = match atime_ns {
                Some(FlatValue::Int64(v)) => *v,
                _ => {
                    return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
                        detail: "atime_ns must be int64".to_string(),
                    }));
                }
            };
            let mt = match mtime_ns {
                Some(FlatValue::Int64(v)) => *v,
                _ => {
                    return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
                        detail: "mtime_ns must be int64".to_string(),
                    }));
                }
            };
            if at < 0 || mt < 0 {
                return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
                    detail: "atime_ns/mtime_ns must be non-negative".to_string(),
                }));
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
            let err = std::io::Error::last_os_error();
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }

    #[cfg(not(unix))]
    {
        let _ = (p, atime_ns, mtime_ns);
        resp_err_kverr(KvError::Api(ApiError::NotImplemented {}))
    }
}

fn safe_join_root(remote_root_dir_abs: &str, relpath: &str) -> KvResult<PathBuf> {
    if remote_root_dir_abs.trim().is_empty() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "remote_root_dir_abs must be non-empty".to_string(),
        }));
    }
    let root = PathBuf::from(remote_root_dir_abs);
    if !root.is_absolute() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "remote_root_dir_abs must be an absolute path".to_string(),
        }));
    }
    let root_r = root.canonicalize().map_err(|e| {
        KvError::Api(ApiError::InvalidArgument {
            detail: format!("canonicalize export root failed: {}", e),
        })
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
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "relpath contains '..'".to_string(),
        }));
    }
    let p = if parts.is_empty() {
        root_r.clone()
    } else {
        root_r.join(parts.join("/"))
    };

    // Ensure no escape.
    if p != root_r && !p.starts_with(&root_r) {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "resolved path escapes export root".to_string(),
        }));
    }
    Ok(p)
}

fn resp_ok(mut extra: FlatDict) -> FlatDict {
    extra.insert("ok".to_string(), FlatValue::Bool(true));
    extra
}

fn resp_err_io(e: std::io::Error) -> FlatDict {
    let errno = e.raw_os_error().unwrap_or(libc::EIO);
    resp_err(FsAgentRpcErrorKind::Os, e.to_string(), Some(errno))
}

fn resp_err_kverr(e: KvError) -> FlatDict {
    match e {
        KvError::Api(ApiError::InvalidArgument { detail }) => {
            resp_err(FsAgentRpcErrorKind::InvalidArgument, detail, None)
        }
        other => resp_err(FsAgentRpcErrorKind::Internal, other.to_string(), None),
    }
}

fn require_str(payload: &FlatDict, key: &str) -> KvResult<String> {
    match payload.get(key) {
        Some(FlatValue::String(s)) => {
            if s.is_empty() {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!("{} must be non-empty", key),
                }));
            }
            Ok(s.clone())
        }
        _ => Err(KvError::Api(ApiError::InvalidArgument {
            detail: format!("{} must be string", key),
        })),
    }
}

fn require_i64(payload: &FlatDict, key: &str) -> KvResult<i64> {
    match payload.get(key) {
        Some(FlatValue::Int64(v)) => Ok(*v),
        _ => Err(KvError::Api(ApiError::InvalidArgument {
            detail: format!("{} must be int64", key),
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluxon_fs_core::config::{
        FLUXON_FS_RPC_TOKEN_PAYLOAD_KEY, FluxonFsGlobalConfig, FluxonFsRequestIdentity,
        FluxonFsRuntimeAccessModel, FluxonFsRuntimeAccessUser, FluxonFsScopeAccess,
        FluxonFsScopeAccessMode, agent_registry_export_for_name_and_root_v1, build_rpc_token,
    };
    use sha2::Digest;

    fn browse_only_access_model() -> FluxonFsRuntimeAccessModel {
        FluxonFsRuntimeAccessModel {
            users: vec![FluxonFsRuntimeAccessUser {
                username: "alice".to_string(),
                can_manage_users: false,
                rpc_token_secret_sha256_hex: hex::encode(sha2::Sha256::digest(b"pw")),
            }],
            scope_access: vec![FluxonFsScopeAccess {
                export_name: "exp".to_string(),
                prefix: "dir/".to_string(),
                mode: FluxonFsScopeAccessMode::Read,
                usernames: vec!["alice".to_string()],
            }],
        }
    }

    fn read_write_access_model() -> FluxonFsRuntimeAccessModel {
        FluxonFsRuntimeAccessModel {
            users: vec![FluxonFsRuntimeAccessUser {
                username: "alice".to_string(),
                can_manage_users: false,
                rpc_token_secret_sha256_hex: hex::encode(sha2::Sha256::digest(b"pw")),
            }],
            scope_access: vec![FluxonFsScopeAccess {
                export_name: "exp".to_string(),
                prefix: String::new(),
                mode: FluxonFsScopeAccessMode::ReadWrite,
                usernames: vec!["alice".to_string()],
            }],
        }
    }

    fn payload_for(identity: &FluxonFsRequestIdentity) -> FlatDict {
        let token = build_rpc_token(identity, 1_000).unwrap();
        FlatDict::from([(
            FLUXON_FS_RPC_TOKEN_PAYLOAD_KEY.to_string(),
            FlatValue::String(token),
        )])
    }

    fn rpc_token_for(identity: &FluxonFsRequestIdentity) -> String {
        build_rpc_token(identity, 1_000).unwrap()
    }

    fn test_exports_handle(root_dir_abs: &str) -> AgentExportsHandle {
        let export_name = "exp".to_string();
        let export = agent_registry_export_for_name_and_root_v1(&export_name, root_dir_abs);
        let cfg = FluxonFsGlobalConfig {
            stale_window_ms: 0,
            write_session_target_inflight_bytes: 64 * 1024 * 1024,
            rules: Vec::new(),
            exports: BTreeMap::from([(export_name, export)]),
        };
        AgentExportsHandle::new_from_static_cfg(&cfg, BTreeMap::new())
    }

    fn test_temp_dir(prefix: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "{}_{}_{}",
            prefix,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn test_write_session_entry() -> WriteSessionEntry {
        WriteSessionEntry {
            export_name: "exp".to_string(),
            relpath: "file.bin".to_string(),
            write_path_key: "exp\0/tmp/file.bin".to_string(),
            write_path_owner: "session:test".to_string(),
            current_pos: 0,
            last_touched: Instant::now(),
            queued_chunks: VecDeque::new(),
            queued_bytes: 0,
            scheduled: false,
            writing: false,
            closing: false,
            aborted: false,
            fatal_error: None,
            expected_data_frames: None,
            highest_received_seq: None,
            highest_written_seq: None,
            received_data_frame_seqs: BTreeSet::new(),
            written_data_frame_seqs: BTreeSet::new(),
            timing: WriteSessionTimingStats::default(),
            created_at: Instant::now(),
        }
    }

    fn test_write_session_handle(chunks: VecDeque<QueuedWriteChunk>) -> WriteSessionEntryHandle {
        let mut state = test_write_session_entry();
        state.queued_chunks = chunks;
        state.queued_bytes = state.queued_chunks.iter().map(|c| c.data.len()).sum();
        state.scheduled = true;
        let dir = std::env::temp_dir();
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = dir.join(format!(
            "fluxon_write_session_test_{}_{}",
            std::process::id(),
            ts
        ));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        let _ = std::fs::remove_file(&path);
        Arc::new(WriteSessionEntryHandleInner {
            state: Mutex::new(state),
            file: Mutex::new(file),
            cv: Condvar::new(),
        })
    }

    #[test]
    fn authorize_stat_allows_browse_visible_dir() {
        let access_model = AgentAccessModelHandle::new(Some(browse_only_access_model()));
        let identity = FluxonFsRequestIdentity {
            username: "alice".to_string(),
            password: "pw".to_string(),
        };
        let payload = payload_for(&identity);
        let got = authorize_stat_path(&access_model, &payload, "exp", "dir");
        assert!(got.is_ok());
    }

    #[test]
    fn authorize_read_rejects_browse_only_dir() {
        let access_model = AgentAccessModelHandle::new(Some(browse_only_access_model()));
        let identity = FluxonFsRequestIdentity {
            username: "alice".to_string(),
            password: "pw".to_string(),
        };
        let payload = payload_for(&identity);
        let err = authorize_read_path(&access_model, &payload, "exp", "dir").unwrap_err();
        match err.get("err") {
            Some(FlatValue::String(s)) => assert!(s.contains("fs read denied")),
            other => panic!("unexpected error payload: {:?}", other),
        }
    }

    #[test]
    fn typed_open_write_session_accepts_fs_rpc_token() {
        let identity = FluxonFsRequestIdentity {
            username: "alice".to_string(),
            password: "pw".to_string(),
        };
        let root = test_temp_dir("fluxon_typed_open_write_session_token");
        let exports = test_exports_handle(root.to_str().unwrap());
        let access_model = AgentAccessModelHandle::new(Some(read_write_access_model()));
        let write_sessions = AgentWriteSessionsHandle::new();
        let resp = handle_open_write_session_typed(
            &exports,
            &access_model,
            &write_sessions,
            FsOpenWriteSessionReq {
                export: "exp".to_string(),
                relpath: "dir/file.bin".to_string(),
                fs_rpc_token: Some(rpc_token_for(&identity)),
                allow_s3_internal_multipart: false,
            },
        );
        assert!(resp.ok, "typed open_write_session failed: {:?}", resp);
        assert!(!resp.session_id.is_empty());
        assert!(root.join("dir/file.bin").exists());
        let _ = write_sessions.take(&resp.session_id);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn typed_open_write_session_preserves_existing_file_for_append_flow() {
        let identity = FluxonFsRequestIdentity {
            username: "alice".to_string(),
            password: "pw".to_string(),
        };
        let root = test_temp_dir("fluxon_typed_open_write_session_append");
        let file_path = root.join("dir/file.bin");
        std::fs::create_dir_all(file_path.parent().unwrap()).unwrap();
        std::fs::write(&file_path, b"abc").unwrap();
        let exports = test_exports_handle(root.to_str().unwrap());
        let access_model = AgentAccessModelHandle::new(Some(read_write_access_model()));
        let write_sessions = AgentWriteSessionsHandle::new();
        let resp = handle_open_write_session_typed(
            &exports,
            &access_model,
            &write_sessions,
            FsOpenWriteSessionReq {
                export: "exp".to_string(),
                relpath: "dir/file.bin".to_string(),
                fs_rpc_token: Some(rpc_token_for(&identity)),
                allow_s3_internal_multipart: false,
            },
        );
        assert!(resp.ok, "typed open_write_session failed: {:?}", resp);
        assert_eq!(resp.size, 3);
        assert_eq!(std::fs::read(&file_path).unwrap(), b"abc");
        let _ = write_sessions.take(&resp.session_id);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn closing_session_accepts_expected_late_data_frame() {
        let mut state = test_write_session_entry();
        state.closing = true;
        state.expected_data_frames = Some(4);
        assert!(write_session_accepts_chunk_while_closing(&state, 3, true));
        assert!(!write_session_accepts_chunk_while_closing(&state, 4, true));
    }

    #[test]
    fn closing_session_rejects_legacy_chunk_write() {
        let mut state = test_write_session_entry();
        state.closing = true;
        state.expected_data_frames = Some(4);
        assert!(!write_session_accepts_chunk_while_closing(&state, 0, false));
    }

    #[test]
    fn pending_expected_frames_uses_written_set_not_highest_seq() {
        let mut state = test_write_session_entry();
        state.expected_data_frames = Some(4);
        state.highest_written_seq = Some(3);
        state.written_data_frame_seqs.extend([0, 2, 3]);
        assert!(write_session_pending_expected_frames(&state));
        state.written_data_frame_seqs.insert(1);
        assert!(!write_session_pending_expected_frames(&state));
    }

    #[test]
    fn merge_expected_frames_preserves_existing_nonzero_on_zero_update() {
        assert_eq!(
            write_session_merge_expected_frames(Some(16), Some(0)),
            Some(16)
        );
        assert_eq!(
            write_session_merge_expected_frames(Some(8), Some(4)),
            Some(8)
        );
        assert_eq!(
            write_session_merge_expected_frames(Some(4), Some(12)),
            Some(12)
        );
        assert_eq!(write_session_merge_expected_frames(None, Some(6)), Some(6));
    }

    #[test]
    fn write_executor_processes_one_chunk_and_requeues() {
        let handle = test_write_session_handle(VecDeque::from(vec![
            QueuedWriteChunk {
                seq_no: 0,
                offset: 0,
                data: Bytes::from_static(b"abc"),
                is_data_frame: true,
            },
            QueuedWriteChunk {
                seq_no: 1,
                offset: 3,
                data: Bytes::from_static(b"def"),
                is_data_frame: true,
            },
        ]));
        assert!(drain_write_session_entry_once(&handle));
        {
            let state = handle.state.lock();
            assert_eq!(state.current_pos, 3);
            assert_eq!(state.queued_chunks.len(), 1);
            assert!(state.scheduled);
            assert!(!state.writing);
            assert_eq!(state.timing.chunks_written, 1);
        }
        assert!(!drain_write_session_entry_once(&handle));
        let state = handle.state.lock();
        assert_eq!(state.current_pos, 6);
        assert_eq!(state.queued_chunks.len(), 0);
        assert!(!state.scheduled);
        assert!(!state.writing);
        assert_eq!(state.timing.chunks_written, 2);
    }

    #[test]
    fn write_executor_drain_all_mode_empties_session_queue() {
        let handle = test_write_session_handle(VecDeque::from(vec![
            QueuedWriteChunk {
                seq_no: 0,
                offset: 0,
                data: Bytes::from_static(b"abc"),
                is_data_frame: true,
            },
            QueuedWriteChunk {
                seq_no: 1,
                offset: 3,
                data: Bytes::from_static(b"def"),
                is_data_frame: true,
            },
            QueuedWriteChunk {
                seq_no: 2,
                offset: 6,
                data: Bytes::from_static(b"ghi"),
                is_data_frame: true,
            },
        ]));
        while drain_write_session_entry_once(&handle) {}
        let state = handle.state.lock();
        assert_eq!(state.current_pos, 9);
        assert_eq!(state.queued_chunks.len(), 0);
        assert!(!state.scheduled);
        assert!(!state.writing);
        assert_eq!(state.timing.chunks_written, 3);
    }

    #[test]
    fn active_write_paths_blocks_transient_op_while_session_is_active() {
        let active = ActiveWritePathsHandle::new();
        let key = "exp\0/tmp/file.bin";
        active.try_acquire_owned(key, "session:1").unwrap();
        let err = match acquire_transient_write_path_guard(&active, key, "write_chunk") {
            Ok(_) => panic!("expected EBUSY while session path lease is active"),
            Err(v) => v,
        };
        match err.get("errno") {
            Some(FlatValue::Int64(v)) => assert_eq!(*v, libc::EBUSY as i64),
            other => panic!("unexpected errno payload: {:?}", other),
        }
        active.release_owned(key, "session:1");
        assert!(acquire_transient_write_path_guard(&active, key, "write_chunk").is_ok());
    }

    #[test]
    fn taking_session_releases_owned_write_path_lease() {
        let sessions = AgentWriteSessionsHandle::new();
        let session_id = sessions.alloc_id();
        let state = test_write_session_entry();
        let path = std::env::temp_dir().join(format!(
            "fluxon_write_session_take_release_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        let _ = std::fs::remove_file(&path);
        sessions
            .active_write_paths
            .try_acquire_owned(&state.write_path_key, &state.write_path_owner)
            .unwrap();
        sessions.insert(session_id.clone(), state, file);
        assert!(sessions.take(&session_id).is_some());
        assert!(
            sessions
                .active_write_paths
                .try_acquire_owned("exp\0/tmp/file.bin", "session:2")
                .is_ok()
        );
    }
}
