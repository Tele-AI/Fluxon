use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use fluxon_fs_core::config::{
    FS_AGENT_TRANSFER_STREAM_CLOSE_RPC_PATH, FS_AGENT_TRANSFER_STREAM_NEXT_RPC_PATH,
    FS_AGENT_TRANSFER_STREAM_OPEN_RPC_PATH,
    FS_MASTER_TRANSFER_SCHEDULER_HEARTBEAT_RPC_PATH, FS_MASTER_TRANSFER_SCHEDULER_RESULT_RPC_PATH,
    FluxonFsTransferBatchCollectInfoWire, FluxonFsTransferBatchKind,
    FluxonFsTransferCollectInfoKind, FluxonFsTransferDispositionWire,
    FluxonFsTransferFailedFileReasonKindWire,
    FluxonFsTransferReadStreamCloseWire, FluxonFsTransferReadStreamNextResultWire,
    FluxonFsTransferReadStreamNextWire, FluxonFsTransferReadStreamOpenResultWire,
    FluxonFsTransferReadStreamOpenWire,
    FluxonFsTransferSkipEntryKind, FluxonFsTransferSkipEntryWire,
    FluxonFsTransferManifestEntryWire, FluxonFsTransferManifestWire,
    FluxonFsTransferScanMode,
    FluxonFsTransferScanEventAckWire, FluxonFsTransferScanEventKindWire,
    FluxonFsTransferScanEventWire, FluxonFsTransferScanLaunchResultWire,
    FluxonFsTransferScanAssignmentWire, FluxonFsTransferScanBatchWire,
    FluxonFsTransferScanChildUnitWire, FluxonFsTransferScanFrontier,
    FluxonFsTransferScanFrontierDirEntry, FluxonFsTransferScanFrontierEntry,
    FluxonFsTransferScanResultWire,
    FluxonFsTransferSymlinkNoticeEntryWire, FluxonFsTransferWorkerCollectInfoResultWire,
    FluxonFsTransferWorkerAssignmentWire, FluxonFsTransferWorkerFileResultWire,
    FluxonFsTransferWorkerFailedFileResultWire,
    FluxonFsTransferWorkerHeartbeatResultWire, FluxonFsTransferWorkerHeartbeatTelemetryWire,
    FluxonFsTransferWorkerHeartbeatWire,
    FluxonFsTransferWorkerLaunchResultWire, FluxonFsTransferWorkerResultAckWire,
    FluxonFsTransferWorkerResultWire, FluxonFsTransferWorkerStopReasonWire,
    transfer_collect_info_output_relpath,
};
use fluxon_fs_core::retry::{
    BackoffConfig, DEFAULT_WARN_INTERVAL_SECS, WarnConfig, next_backoff, should_warn,
};
use fluxon_kv::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError};
use fluxon_kv::user_api::flat_dict::{FlatDict, FlatValue};
use fluxon_kv::user_api::FluxonUserApi;
use parking_lot::{Condvar, Mutex};

use super::{
    AgentExportsHandle, CHUNK_BYTES, TRANSFER_HEARTBEAT_INTERVAL_MS,
    TRANSFER_STREAM_RPC_TIMEOUT_MS, TRANSFER_WORKER_COORDINATION_RPC_TIMEOUT_MS, require_i64,
    require_str, resp_err_io, resp_err_kverr, resp_ok, safe_join_root,
};

fn empty_transfer_scan_frontier() -> FluxonFsTransferScanFrontier {
    FluxonFsTransferScanFrontier {
        direct_files: Vec::new(),
        direct_dirs: Vec::new(),
        empty_dirs: Vec::new(),
    }
}

// Recursive view collected while evaluating one candidate subtree. It is used
// only during scan splitting and never becomes durable by itself.
#[derive(Debug, Default)]
struct TransferTreeCollection {
    files: Vec<FluxonFsTransferScanFrontierEntry>,
    symlink_notices: Vec<FluxonFsTransferSymlinkNoticeEntryWire>,
    empty_dirs: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
struct TransferScanDeadline {
    expire_unix_ms: i64,
}

impl TransferScanDeadline {
    fn from_assignment(assignment: &FluxonFsTransferScanAssignmentWire) -> Option<Self> {
        if assignment.lease_expire_unix_ms <= 0 {
            return None;
        }
        Some(Self {
            expire_unix_ms: assignment.lease_expire_unix_ms,
        })
    }

    fn reached(&self) -> bool {
        chrono::Utc::now().timestamp_millis() >= self.expire_unix_ms
    }
}

const TRANSFER_SCAN_ROOT_LISTING_SLICE_ENTRY_LIMIT: usize = 4096;
const TRANSFER_DIRECT_BATCH_READY_FILE_COUNT: usize = 4096;
const TRANSFER_DIRECT_BATCH_READY_EMPTY_DIR_COUNT: usize = 4096;
const TRANSFER_MERGEABLE_EMPTY_DIR_BUDGET: usize = 4096;
// Keep one mergeable empty-dir payload small enough for the scan control RPC.
const TRANSFER_MERGEABLE_EMPTY_DIR_ESTIMATED_BYTES_BUDGET: usize = 128 * 1024;
const TRANSFER_EMPTY_DIR_MANIFEST_ENTRY_ESTIMATED_OVERHEAD_BYTES: usize = 32;

fn estimate_empty_dir_manifest_entry_bytes(relpath: &str) -> usize {
    relpath
        .len()
        .saturating_add(TRANSFER_EMPTY_DIR_MANIFEST_ENTRY_ESTIMATED_OVERHEAD_BYTES)
}

fn estimate_empty_dir_manifest_bytes(relpaths: &[String]) -> usize {
    relpaths.iter().fold(0_usize, |acc, relpath| {
        acc.saturating_add(estimate_empty_dir_manifest_entry_bytes(relpath.as_str()))
    })
}

fn transfer_manifest_is_empty_dirs_only_batch(
    manifest: &FluxonFsTransferManifestWire,
    collect_infos: &[FluxonFsTransferBatchCollectInfoWire],
) -> bool {
    manifest.entries.is_empty()
        && collect_infos.is_empty()
        && !manifest.empty_dir_relpaths.is_empty()
}

struct TransferRootDirListingSession {
    job_id: String,
    scan_epoch: i64,
    root_relpath: String,
    generation: i64,
    lease_expire_unix_ms: i64,
    read_dir: fs::ReadDir,
    pending_direct_files: Vec<FluxonFsTransferScanFrontierEntry>,
    pending_direct_symlink_notices: Vec<FluxonFsTransferSymlinkNoticeEntryWire>,
    pending_direct_bytes: i64,
    pending_direct_empty_dirs: Vec<String>,
    next_direct_files_batch_index: i64,
    emitted_direct_files_batch_count: i64,
    emitted_child_scan_unit_count: usize,
    direct_dirs: Vec<FluxonFsTransferScanFrontierDirEntry>,
    root_total_bytes: i64,
    root_visible_entries: bool,
}

struct TransferSubtreeStreamingDirFrame {
    dir_abs: PathBuf,
    dir_relpath: String,
    read_dir: fs::ReadDir,
    saw_visible_child: bool,
}

struct TransferSubtreeStreamingSession {
    job_id: String,
    scan_epoch: i64,
    root_relpath: String,
    generation: i64,
    lease_expire_unix_ms: i64,
    dir_stack: Vec<TransferSubtreeStreamingDirFrame>,
    pending_files: Vec<FluxonFsTransferScanFrontierEntry>,
    pending_symlink_notices: Vec<FluxonFsTransferSymlinkNoticeEntryWire>,
    pending_bytes: i64,
    pending_empty_dirs: Vec<String>,
    next_batch_index: i64,
}

#[derive(Default)]
struct TransferScanSessionState {
    root_dir_listing_sessions: BTreeMap<String, TransferRootDirListingSession>,
    subtree_streaming_sessions: BTreeMap<String, TransferSubtreeStreamingSession>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransferScanRegistryTaskState {
    Running,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TransferScanRegistryEntry {
    state: TransferScanRegistryTaskState,
    dedup_expire_unix_ms: i64,
}

#[derive(Debug, Default)]
struct TransferScanRegistryState {
    tasks: BTreeMap<String, TransferScanRegistryEntry>,
}

struct CompletedTransferRootDirListing {
    direct_files: Vec<FluxonFsTransferScanFrontierEntry>,
    direct_symlink_notices: Vec<FluxonFsTransferSymlinkNoticeEntryWire>,
    direct_empty_dirs: Vec<String>,
    direct_dirs: Vec<FluxonFsTransferScanFrontierDirEntry>,
    emitted_child_scan_unit_count: usize,
    root_total_bytes: i64,
    root_visible_entries: bool,
    emitted_direct_files_batch_count: i64,
    direct_files_only_batches: Vec<FluxonFsTransferScanBatchWire>,
}

enum TransferRootDirListingOutcome {
    Complete(CompletedTransferRootDirListing),
    Finished(FluxonFsTransferScanResultWire),
    Partial(FluxonFsTransferScanResultWire),
}

static TRANSFER_SCAN_SESSION_STATE: OnceLock<Mutex<TransferScanSessionState>> = OnceLock::new();

fn transfer_scan_session_state() -> &'static Mutex<TransferScanSessionState> {
    TRANSFER_SCAN_SESSION_STATE.get_or_init(|| Mutex::new(TransferScanSessionState::default()))
}

fn cleanup_expired_transfer_scan_sessions(
    state: &mut TransferScanSessionState,
    now_unix_ms: i64,
) {
    state
        .root_dir_listing_sessions
        .retain(|_, session| session.lease_expire_unix_ms <= 0 || session.lease_expire_unix_ms > now_unix_ms);
    state
        .subtree_streaming_sessions
        .retain(|_, session| session.lease_expire_unix_ms <= 0 || session.lease_expire_unix_ms > now_unix_ms);
}

fn same_root_continuation_scan_unit(
    assignment: &FluxonFsTransferScanAssignmentWire,
) -> FluxonFsTransferScanChildUnitWire {
    // Reusing scan_unit_id keeps the root listing cursor bound to one logical
    // continuation chain instead of restarting from the beginning.
    FluxonFsTransferScanChildUnitWire {
        scan_unit_id: assignment.scan_unit_id.clone(),
        root_relpath: assignment.root_relpath.clone(),
        generation: assignment.generation,
        scan_mode: assignment.scan_mode,
    }
}

fn delegated_child_scan_mode() -> FluxonFsTransferScanMode {
    FluxonFsTransferScanMode::FullTree
}

fn subtree_streaming_scan_mode() -> FluxonFsTransferScanMode {
    FluxonFsTransferScanMode::SubtreeStreaming
}

fn direct_files_only_batch_id_for_partition(
    assignment: &FluxonFsTransferScanAssignmentWire,
    partition_index: i64,
) -> String {
    format!(
        "{}__direct_files_only__{}",
        assignment.scan_unit_id, partition_index
    )
}

fn subtree_slice_batch_id_for_partition(
    assignment: &FluxonFsTransferScanAssignmentWire,
    partition_index: i64,
) -> String {
    format!(
        "{}__subtree_slice__{}",
        assignment.scan_unit_id, partition_index
    )
}

fn build_direct_files_only_batch_from_entries_with_batch_id(
    batch_id: String,
    assignment: &FluxonFsTransferScanAssignmentWire,
    root_relpath: String,
    direct_files: Vec<FluxonFsTransferScanFrontierEntry>,
    direct_symlink_notices: Vec<FluxonFsTransferSymlinkNoticeEntryWire>,
    empty_dir_relpaths: Vec<String>,
) -> Result<FluxonFsTransferScanBatchWire, FlatDict> {
    Ok(FluxonFsTransferScanBatchWire {
        batch_id,
        root_relpath,
        batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
        manifest_blob: build_transfer_manifest_blob(direct_files, empty_dir_relpaths)?,
        collect_infos: build_symlink_collect_infos(direct_symlink_notices)?,
        generation: assignment.generation,
    })
}

fn build_subtree_slice_batch_from_entries_with_batch_id(
    batch_id: String,
    assignment: &FluxonFsTransferScanAssignmentWire,
    files: Vec<FluxonFsTransferScanFrontierEntry>,
    symlink_notices: Vec<FluxonFsTransferSymlinkNoticeEntryWire>,
    empty_dir_relpaths: Vec<String>,
) -> Result<FluxonFsTransferScanBatchWire, FlatDict> {
    Ok(FluxonFsTransferScanBatchWire {
        batch_id,
        root_relpath: assignment.root_relpath.clone(),
        batch_kind: FluxonFsTransferBatchKind::SubtreeSlice,
        manifest_blob: build_transfer_manifest_blob(files, empty_dir_relpaths)?,
        collect_infos: build_symlink_collect_infos(symlink_notices)?,
        generation: assignment.generation,
    })
}

fn flush_pending_root_direct_files_batch(
    assignment: &FluxonFsTransferScanAssignmentWire,
    session: &mut TransferRootDirListingSession,
) -> Result<Option<FluxonFsTransferScanBatchWire>, FlatDict> {
    if session.pending_direct_files.is_empty()
        && session.pending_direct_symlink_notices.is_empty()
        && session.pending_direct_empty_dirs.is_empty()
    {
        return Ok(None);
    }
    let batch = build_direct_files_only_batch_from_entries_with_batch_id(
        direct_files_only_batch_id_for_partition(
            assignment,
            session.next_direct_files_batch_index,
        ),
        assignment,
        assignment.root_relpath.clone(),
        std::mem::take(&mut session.pending_direct_files),
        std::mem::take(&mut session.pending_direct_symlink_notices),
        std::mem::take(&mut session.pending_direct_empty_dirs),
    )?;
    session.pending_direct_bytes = 0;
    session.next_direct_files_batch_index = session.next_direct_files_batch_index.saturating_add(1);
    session.emitted_direct_files_batch_count = session.emitted_direct_files_batch_count.saturating_add(1);
    Ok(Some(batch))
}

fn should_flush_subtree_stream_batch(
    batch_ready_bytes: i64,
    pending_bytes: i64,
    pending_file_like_count: usize,
    pending_empty_dir_count: usize,
) -> bool {
    (batch_ready_bytes > 0 && pending_bytes >= batch_ready_bytes)
        || pending_file_like_count >= TRANSFER_DIRECT_BATCH_READY_FILE_COUNT
        || pending_empty_dir_count >= TRANSFER_DIRECT_BATCH_READY_EMPTY_DIR_COUNT
}

fn flush_pending_subtree_stream_batch(
    assignment: &FluxonFsTransferScanAssignmentWire,
    session: &mut TransferSubtreeStreamingSession,
) -> Result<Option<FluxonFsTransferScanBatchWire>, FlatDict> {
    if session.pending_files.is_empty()
        && session.pending_symlink_notices.is_empty()
        && session.pending_empty_dirs.is_empty()
    {
        return Ok(None);
    }
    let batch = build_subtree_slice_batch_from_entries_with_batch_id(
        subtree_slice_batch_id_for_partition(assignment, session.next_batch_index),
        assignment,
        std::mem::take(&mut session.pending_files),
        std::mem::take(&mut session.pending_symlink_notices),
        std::mem::take(&mut session.pending_empty_dirs),
    )?;
    session.pending_bytes = 0;
    session.next_batch_index = session.next_batch_index.saturating_add(1);
    Ok(Some(batch))
}

fn build_partial_root_dir_listing_result(
    assignment: &FluxonFsTransferScanAssignmentWire,
    direct_files: &[FluxonFsTransferScanFrontierEntry],
    direct_dirs: &[FluxonFsTransferScanFrontierDirEntry],
    empty_dirs: &[String],
    child_scan_units: Vec<FluxonFsTransferScanChildUnitWire>,
    direct_files_only_batches: Vec<FluxonFsTransferScanBatchWire>,
) -> FluxonFsTransferScanResultWire {
    let mut frontier_direct_files = direct_files.to_vec();
    let mut frontier_direct_dirs = direct_dirs.to_vec();
    let mut frontier_empty_dirs = empty_dirs
        .iter()
        .map(|relpath| FluxonFsTransferScanFrontierDirEntry {
            relpath: relpath.clone(),
        })
        .collect::<Vec<_>>();
    frontier_direct_files.sort_by(|a, b| a.relpath.cmp(&b.relpath));
    frontier_direct_dirs.sort_by(|a, b| a.relpath.cmp(&b.relpath));
    frontier_empty_dirs.sort_by(|a, b| a.relpath.cmp(&b.relpath));
    FluxonFsTransferScanResultWire {
        job_id: assignment.job_id.clone(),
        scan_epoch: assignment.scan_epoch,
        scan_unit_id: assignment.scan_unit_id.clone(),
        scan_task_id: assignment.scan_task_id.clone(),
        root_relpath: assignment.root_relpath.clone(),
        generation: assignment.generation,
        frontier: FluxonFsTransferScanFrontier {
            direct_files: frontier_direct_files,
            direct_dirs: frontier_direct_dirs,
            empty_dirs: frontier_empty_dirs,
        },
        direct_files_only_batches,
        child_scan_units: {
            let mut child_scan_units = child_scan_units;
            child_scan_units.push(same_root_continuation_scan_unit(assignment));
            child_scan_units
        },
        full_dir_batches: Vec::new(),
        finished: false,
    }
}

fn build_finished_empty_transfer_scan_result(
    assignment: &FluxonFsTransferScanAssignmentWire,
) -> FluxonFsTransferScanResultWire {
    FluxonFsTransferScanResultWire {
        job_id: assignment.job_id.clone(),
        scan_epoch: assignment.scan_epoch,
        scan_unit_id: assignment.scan_unit_id.clone(),
        scan_task_id: assignment.scan_task_id.clone(),
        root_relpath: assignment.root_relpath.clone(),
        generation: assignment.generation,
        frontier: empty_transfer_scan_frontier(),
        direct_files_only_batches: Vec::new(),
        child_scan_units: Vec::new(),
        full_dir_batches: Vec::new(),
        finished: true,
    }
}

fn open_transfer_root_dir_listing_session(
    root_dir_abs: &str,
    assignment: &FluxonFsTransferScanAssignmentWire,
) -> Result<Option<TransferRootDirListingSession>, FlatDict> {
    let dir_abs = safe_join_root(root_dir_abs, assignment.root_relpath.as_str()).map_err(resp_err_kverr)?;
    let read_dir = match retry_after_target_path_chmod(
        dir_abs.as_path(),
        "root_read_dir",
        dir_abs.as_path(),
        || fs::read_dir(&dir_abs),
    ) {
        Ok(v) => v,
        Err(resp) => {
            tracing::warn!(
                "transfer best-effort read repair failed: op=root_read_dir relpath={} resp={:?}",
                assignment.root_relpath,
                resp
            );
            return Ok(None);
        }
    };
    Ok(Some(TransferRootDirListingSession {
        job_id: assignment.job_id.clone(),
        scan_epoch: assignment.scan_epoch,
        root_relpath: assignment.root_relpath.clone(),
        generation: assignment.generation,
        lease_expire_unix_ms: assignment.lease_expire_unix_ms,
        read_dir,
        pending_direct_files: Vec::new(),
        pending_direct_symlink_notices: Vec::new(),
        pending_direct_bytes: 0,
        pending_direct_empty_dirs: Vec::new(),
        next_direct_files_batch_index: 0,
        emitted_direct_files_batch_count: 0,
        emitted_child_scan_unit_count: 0,
        direct_dirs: Vec::new(),
        root_total_bytes: 0,
        root_visible_entries: false,
    }))
}

fn take_transfer_root_dir_listing_session(
    root_dir_abs: &str,
    assignment: &FluxonFsTransferScanAssignmentWire,
) -> Result<Option<TransferRootDirListingSession>, FlatDict> {
    let now_unix_ms = chrono::Utc::now().timestamp_millis();
    let mut state = transfer_scan_session_state().lock();
    cleanup_expired_transfer_scan_sessions(&mut state, now_unix_ms);
    if let Some(mut session) = state.root_dir_listing_sessions.remove(assignment.scan_unit_id.as_str()) {
        if session.job_id == assignment.job_id
            && session.scan_epoch == assignment.scan_epoch
            && session.root_relpath == assignment.root_relpath
            && session.generation == assignment.generation
        {
            session.lease_expire_unix_ms = assignment.lease_expire_unix_ms;
            return Ok(Some(session));
        }
    }
    drop(state);
    open_transfer_root_dir_listing_session(root_dir_abs, assignment)
}

fn store_transfer_root_dir_listing_session(
    scan_unit_id: &str,
    session: TransferRootDirListingSession,
) {
    let mut state = transfer_scan_session_state().lock();
    state
        .root_dir_listing_sessions
        .insert(scan_unit_id.to_string(), session);
}

fn open_transfer_subtree_streaming_dir_frame(
    dir_abs: PathBuf,
    dir_relpath: String,
) -> Result<TransferSubtreeStreamingDirFrame, FlatDict> {
    let read_dir = retry_after_target_path_chmod(
        dir_abs.as_path(),
        "subtree_stream_read_dir",
        dir_abs.as_path(),
        || fs::read_dir(&dir_abs),
    )?;
    Ok(TransferSubtreeStreamingDirFrame {
        dir_abs,
        dir_relpath,
        read_dir,
        saw_visible_child: false,
    })
}

fn open_transfer_subtree_streaming_session(
    root_dir_abs: &str,
    assignment: &FluxonFsTransferScanAssignmentWire,
) -> Result<Option<TransferSubtreeStreamingSession>, FlatDict> {
    if is_relpath_skipped(&assignment.skip_entries, assignment.root_relpath.as_str()) {
        return Ok(None);
    }
    let dir_abs = safe_join_root(root_dir_abs, assignment.root_relpath.as_str()).map_err(resp_err_kverr)?;
    let root_md = retry_after_target_path_chmod(
        Path::new(root_dir_abs),
        "subtree_stream_root_symlink_metadata",
        Path::new(&dir_abs),
        || fs::symlink_metadata(Path::new(&dir_abs)),
    )?;
    if !root_md.is_dir() {
        return Err(resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "subtree streaming scan requires directory root: root_relpath={}",
                assignment.root_relpath
            ),
        })));
    }
    Ok(Some(TransferSubtreeStreamingSession {
        job_id: assignment.job_id.clone(),
        scan_epoch: assignment.scan_epoch,
        root_relpath: assignment.root_relpath.clone(),
        generation: assignment.generation,
        lease_expire_unix_ms: assignment.lease_expire_unix_ms,
        dir_stack: vec![open_transfer_subtree_streaming_dir_frame(
            PathBuf::from(dir_abs),
            assignment.root_relpath.clone(),
        )?],
        pending_files: Vec::new(),
        pending_symlink_notices: Vec::new(),
        pending_bytes: 0,
        pending_empty_dirs: Vec::new(),
        next_batch_index: 0,
    }))
}

fn take_transfer_subtree_streaming_session(
    root_dir_abs: &str,
    assignment: &FluxonFsTransferScanAssignmentWire,
) -> Result<Option<TransferSubtreeStreamingSession>, FlatDict> {
    let now_unix_ms = chrono::Utc::now().timestamp_millis();
    let mut state = transfer_scan_session_state().lock();
    cleanup_expired_transfer_scan_sessions(&mut state, now_unix_ms);
    if let Some(mut session) = state
        .subtree_streaming_sessions
        .remove(assignment.scan_unit_id.as_str())
    {
        if session.job_id == assignment.job_id
            && session.scan_epoch == assignment.scan_epoch
            && session.root_relpath == assignment.root_relpath
            && session.generation == assignment.generation
        {
            session.lease_expire_unix_ms = assignment.lease_expire_unix_ms;
            return Ok(Some(session));
        }
    }
    drop(state);
    open_transfer_subtree_streaming_session(root_dir_abs, assignment)
}

fn store_transfer_subtree_streaming_session(
    scan_unit_id: &str,
    session: TransferSubtreeStreamingSession,
) {
    let mut state = transfer_scan_session_state().lock();
    state
        .subtree_streaming_sessions
        .insert(scan_unit_id.to_string(), session);
}

fn take_new_child_scan_units_from_session(
    session: &mut TransferRootDirListingSession,
    generation: i64,
) -> Vec<FluxonFsTransferScanChildUnitWire> {
    let mut child_scan_units = session.direct_dirs[session.emitted_child_scan_unit_count..]
        .iter()
        .map(|entry| {
            new_child_scan_unit(
                entry.relpath.clone(),
                generation.saturating_add(1),
                delegated_child_scan_mode(),
            )
        })
        .collect::<Vec<_>>();
    session.emitted_child_scan_unit_count = session.direct_dirs.len();
    child_scan_units.sort_by(|a, b| a.root_relpath.cmp(&b.root_relpath));
    child_scan_units
}

fn should_flush_direct_batch(
    batch_ready_bytes: i64,
    pending_direct_bytes: i64,
    pending_direct_files: usize,
    pending_direct_empty_dirs: usize,
) -> bool {
    (batch_ready_bytes > 0 && pending_direct_bytes >= batch_ready_bytes)
        || pending_direct_files >= TRANSFER_DIRECT_BATCH_READY_FILE_COUNT
        || pending_direct_empty_dirs >= TRANSFER_DIRECT_BATCH_READY_EMPTY_DIR_COUNT
}

fn probe_root_child_dir_is_empty(
    root_dir_abs: &str,
    child_relpath: &str,
    skip_entries: &[FluxonFsTransferSkipEntryWire],
    deadline: Option<TransferScanDeadline>,
) -> Result<bool, FlatDict> {
    let dir_abs = safe_join_root(root_dir_abs, child_relpath).map_err(resp_err_kverr)?;
    let rd = match retry_after_target_path_chmod(
        dir_abs.parent().unwrap_or(dir_abs.as_path()),
        "root_child_probe_read_dir",
        dir_abs.as_path(),
        || fs::read_dir(&dir_abs),
    ) {
        Ok(v) => v,
        Err(resp) => {
            tracing::warn!(
                "transfer best-effort read repair failed: op=root_child_probe_read_dir relpath={} resp={:?}",
                child_relpath,
                resp
            );
            return Ok(false);
        }
    };
    for ent in rd {
        if deadline.is_some_and(|deadline| deadline.reached()) {
            return Ok(false);
        }
        let ent = match ent {
            Ok(v) => v,
            Err(err) if io_error_is_permission_denied(&err) => {
                log_transfer_permission_denied_skip(
                    "root_child_probe_read_dir_entry",
                    child_relpath,
                    &err,
                );
                continue;
            }
            Err(err) => {
                tracing::warn!(
                    "transfer root child empty-dir probe read_dir entry failed: relpath={} err={}",
                    child_relpath,
                    err
                );
                return Ok(false);
            }
        };
        let name = ent.file_name().to_string_lossy().to_string();
        let nested_relpath = normalize_child_relpath(child_relpath, name.as_str());
        if is_relpath_skipped(skip_entries, nested_relpath.as_str()) {
            continue;
        }
        let nested_path = ent.path();
        let md = match retry_after_target_path_chmod(
            nested_path.parent().unwrap_or(nested_path.as_path()),
            "root_child_probe_symlink_metadata",
            nested_path.as_path(),
            || fs::symlink_metadata(&nested_path),
        ) {
            Ok(v) => v,
            Err(resp) => {
                tracing::warn!(
                    "transfer best-effort read repair failed: op=root_child_probe_symlink_metadata relpath={} resp={:?}",
                    nested_relpath,
                    resp
                );
                return Ok(false);
            }
        };
        if md.file_type().is_symlink() || md.is_file() || md.is_dir() {
            return Ok(false);
        }
    }
    Ok(true)
}

#[derive(Default)]
struct PartitionedDirectBatchContent {
    direct_files: Vec<FluxonFsTransferScanFrontierEntry>,
    direct_symlink_notices: Vec<FluxonFsTransferSymlinkNoticeEntryWire>,
    empty_dir_relpaths: Vec<String>,
    total_bytes: i64,
}

impl PartitionedDirectBatchContent {
    fn is_empty(&self) -> bool {
        self.direct_files.is_empty()
            && self.direct_symlink_notices.is_empty()
            && self.empty_dir_relpaths.is_empty()
    }

    fn should_flush(&self, batch_ready_bytes: i64) -> bool {
        should_flush_direct_batch(
            batch_ready_bytes,
            self.total_bytes,
            self.direct_files.len(),
            self.empty_dir_relpaths.len(),
        )
    }
}

fn partition_direct_batch_content(
    batch_ready_bytes: i64,
    direct_files: Vec<FluxonFsTransferScanFrontierEntry>,
    direct_symlink_notices: Vec<FluxonFsTransferSymlinkNoticeEntryWire>,
    empty_dir_relpaths: Vec<String>,
) -> Vec<PartitionedDirectBatchContent> {
    let mut out: Vec<PartitionedDirectBatchContent> = Vec::new();
    let mut current = PartitionedDirectBatchContent::default();
    for file in direct_files {
        current.total_bytes = current.total_bytes.saturating_add(file.size.max(0));
        current.direct_files.push(file);
        if current.should_flush(batch_ready_bytes) {
            out.push(std::mem::take(&mut current));
        }
    }
    for notice in direct_symlink_notices {
        current.direct_symlink_notices.push(notice);
    }
    for empty_dir_relpath in empty_dir_relpaths {
        current.empty_dir_relpaths.push(empty_dir_relpath);
        if current.should_flush(batch_ready_bytes) {
            out.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

fn build_partitioned_direct_files_only_batches(
    assignment: &FluxonFsTransferScanAssignmentWire,
    root_relpath: String,
    direct_files: Vec<FluxonFsTransferScanFrontierEntry>,
    direct_symlink_notices: Vec<FluxonFsTransferSymlinkNoticeEntryWire>,
    empty_dir_relpaths: Vec<String>,
) -> Result<Vec<FluxonFsTransferScanBatchWire>, FlatDict> {
    let mut batches = Vec::new();
    for partition in partition_direct_batch_content(
        assignment.batch_ready_bytes,
        direct_files,
        direct_symlink_notices,
        empty_dir_relpaths,
    ) {
        batches.push(build_direct_files_only_batch_from_entries(
            assignment,
            root_relpath.clone(),
            partition.direct_files,
            partition.direct_symlink_notices,
            partition.empty_dir_relpaths,
        )?);
    }
    Ok(batches)
}

fn build_partitioned_root_direct_files_only_batches(
    assignment: &FluxonFsTransferScanAssignmentWire,
    next_partition_index: &mut i64,
    direct_files: Vec<FluxonFsTransferScanFrontierEntry>,
    direct_symlink_notices: Vec<FluxonFsTransferSymlinkNoticeEntryWire>,
    empty_dir_relpaths: Vec<String>,
) -> Result<Vec<FluxonFsTransferScanBatchWire>, FlatDict> {
    let mut batches = Vec::new();
    for partition in partition_direct_batch_content(
        assignment.batch_ready_bytes,
        direct_files,
        direct_symlink_notices,
        empty_dir_relpaths,
    ) {
        batches.push(build_root_direct_files_only_batch_from_entries(
            assignment,
            *next_partition_index,
            partition.direct_files,
            partition.direct_symlink_notices,
            partition.empty_dir_relpaths,
        )?);
        *next_partition_index = next_partition_index.saturating_add(1);
    }
    Ok(batches)
}

fn collect_transfer_root_dir_listing_slice(
    root_dir_abs: &str,
    assignment: &FluxonFsTransferScanAssignmentWire,
    deadline: Option<TransferScanDeadline>,
) -> Result<TransferRootDirListingOutcome, FlatDict> {
    let Some(mut session) = take_transfer_root_dir_listing_session(root_dir_abs, assignment)? else {
        return Ok(TransferRootDirListingOutcome::Finished(
            build_finished_empty_transfer_scan_result(assignment),
        ));
    };
    let mut scanned_entries: usize = 0;
    let mut direct_files_only_batches: Vec<FluxonFsTransferScanBatchWire> = Vec::new();
    loop {
        if scanned_entries >= TRANSFER_SCAN_ROOT_LISTING_SLICE_ENTRY_LIMIT
            || deadline.is_some_and(|deadline| deadline.reached())
        {
            let child_scan_units =
                take_new_child_scan_units_from_session(&mut session, assignment.generation);
            let partial = build_partial_root_dir_listing_result(
                assignment,
                session.pending_direct_files.as_slice(),
                session.direct_dirs.as_slice(),
                session.pending_direct_empty_dirs.as_slice(),
                child_scan_units,
                direct_files_only_batches,
            );
            store_transfer_root_dir_listing_session(assignment.scan_unit_id.as_str(), session);
            return Ok(TransferRootDirListingOutcome::Partial(partial));
        }
        let ent = match session.read_dir.next() {
            Some(Ok(v)) => v,
            Some(Err(err)) if io_error_is_permission_denied(&err) => {
                scanned_entries = scanned_entries.saturating_add(1);
                continue;
            }
            Some(Err(err)) => return Err(resp_err_io(err)),
            None => {
                if session.emitted_direct_files_batch_count > 0 {
                    if let Some(batch) =
                        flush_pending_root_direct_files_batch(assignment, &mut session)?
                    {
                        direct_files_only_batches.push(batch);
                    }
                }
                return Ok(TransferRootDirListingOutcome::Complete(
                    CompletedTransferRootDirListing {
                        direct_files: std::mem::take(&mut session.pending_direct_files),
                        direct_symlink_notices: std::mem::take(
                            &mut session.pending_direct_symlink_notices,
                        ),
                        direct_empty_dirs: std::mem::take(&mut session.pending_direct_empty_dirs),
                        direct_dirs: session.direct_dirs,
                        emitted_child_scan_unit_count: session.emitted_child_scan_unit_count,
                        root_total_bytes: session.root_total_bytes,
                        root_visible_entries: session.root_visible_entries,
                        emitted_direct_files_batch_count: session.emitted_direct_files_batch_count,
                        direct_files_only_batches,
                    },
                ));
            }
        };
        scanned_entries = scanned_entries.saturating_add(1);
        let name = ent.file_name().to_string_lossy().to_string();
        let child_relpath = normalize_child_relpath(assignment.root_relpath.as_str(), name.as_str());
        if is_relpath_skipped(&assignment.skip_entries, child_relpath.as_str()) {
            continue;
        }
        let child_path = ent.path();
        let md = match retry_after_target_path_chmod(
            child_path.parent().unwrap_or(child_path.as_path()),
            "root_symlink_metadata",
            child_path.as_path(),
            || fs::symlink_metadata(&child_path),
        ) {
            Ok(v) => v,
            Err(resp) => {
                tracing::warn!(
                    "transfer best-effort read repair failed: op=root_symlink_metadata relpath={} resp={:?}",
                    child_relpath,
                    resp
                );
                continue;
            }
        };
        if md.file_type().is_symlink() {
            session.root_visible_entries = true;
            let link_target = match retry_after_parent_dir_chmod(
                child_path.parent().unwrap_or(child_path.as_path()),
                "root_read_link",
                child_path.as_path(),
                || fs::read_link(&child_path),
            ) {
                Ok(v) => v,
                Err(resp) => {
                    tracing::warn!(
                        "transfer best-effort read repair failed: op=root_read_link relpath={} resp={:?}",
                        child_relpath,
                        resp
                    );
                    continue;
                }
            };
            session
                .pending_direct_symlink_notices
                .push(FluxonFsTransferSymlinkNoticeEntryWire {
                    relpath: child_relpath,
                    link_target: link_target.to_string_lossy().to_string(),
                });
            continue;
        }
        if md.is_file() {
            let size = md.len().min(i64::MAX as u64) as i64;
            session.root_visible_entries = true;
            session.root_total_bytes = session.root_total_bytes.saturating_add(size);
            session.pending_direct_files.push(FluxonFsTransferScanFrontierEntry {
                relpath: child_relpath,
                size,
            });
            session.pending_direct_bytes = session.pending_direct_bytes.saturating_add(size);
            if should_flush_direct_batch(
                assignment.batch_ready_bytes,
                session.pending_direct_bytes,
                session.pending_direct_files.len(),
                session.pending_direct_empty_dirs.len(),
            ) {
                if let Some(batch) = flush_pending_root_direct_files_batch(assignment, &mut session)? {
                    direct_files_only_batches.push(batch);
                }
            }
            continue;
        }
        if !md.is_dir() {
            continue;
        }
        session.root_visible_entries = true;
        if probe_root_child_dir_is_empty(
            root_dir_abs,
            child_relpath.as_str(),
            &assignment.skip_entries,
            deadline,
        )? {
            session.pending_direct_empty_dirs.push(child_relpath);
            if should_flush_direct_batch(
                assignment.batch_ready_bytes,
                session.pending_direct_bytes,
                session.pending_direct_files.len(),
                session.pending_direct_empty_dirs.len(),
            ) {
                if let Some(batch) = flush_pending_root_direct_files_batch(assignment, &mut session)? {
                    direct_files_only_batches.push(batch);
                }
            }
        } else {
            session.direct_dirs.push(FluxonFsTransferScanFrontierDirEntry {
                relpath: child_relpath,
            });
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransferWorkerRegistryTaskState {
    Running,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TransferWorkerRegistryEntry {
    state: TransferWorkerRegistryTaskState,
    dedup_expire_unix_ms: i64,
}

#[derive(Debug, Default)]
struct TransferWorkerRegistryState {
    tasks: BTreeMap<String, TransferWorkerRegistryEntry>,
}

#[derive(Debug, Default)]
struct TransferReadStreamRegistryState {
    streams: BTreeMap<String, TransferReadStreamActorHandle>,
    dedup_by_worker_file: BTreeMap<(String, String, String), String>,
}

// Process-local dedupe for worker_task_id. Launch RPCs may be retried forever,
// so the destination agent must avoid spawning the same concrete attempt twice.
#[derive(Clone, Default)]
pub(super) struct TransferWorkerRegistryHandle {
    state: Arc<Mutex<TransferWorkerRegistryState>>,
}

// Source-side scan launch is also retryable, so one scan_task_id must map to
// at most one local background actor until the task reaches completion.
#[derive(Clone, Default)]
pub(super) struct TransferScanRegistryHandle {
    state: Arc<Mutex<TransferScanRegistryState>>,
}

// Source-side transfer streams are process-local file-handle reuse objects.
// They are intentionally not durable; worker retries can reopen from offset.
#[derive(Clone, Default)]
pub(super) struct TransferReadStreamRegistryHandle {
    state: Arc<Mutex<TransferReadStreamRegistryState>>,
}

const TRANSFER_READ_STREAM_PREFETCH_BYTES: usize = 2 * CHUNK_BYTES;
const TRANSFER_WORKER_INITIAL_FILE_LANES: usize = 8;
const TRANSFER_WORKER_MAX_FILE_LANES: usize = 64;
const TRANSFER_WORKER_TARGET_GOODPUT_BYTES_PER_SEC: i64 = 1_000_000_000;
const TRANSFER_WORKER_LANE_RAMP_INTERVAL: Duration = Duration::from_secs(5);
const TRANSFER_WORKER_LANE_POLL_INTERVAL: Duration = Duration::from_secs(1);
const TRANSFER_WORKER_MIN_IMPROVEMENT_PERCENT: i64 = 5;
const TRANSFER_WORKER_HEARTBEAT_EMPTY_DIR_PROGRESS_COUNT: i64 = 512;
const TRANSFER_WORKER_TELEMETRY_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
const TRANSFER_WORKER_THROUGHPUT_LOG_INTERVAL: Duration =
    TRANSFER_WORKER_TELEMETRY_HEARTBEAT_INTERVAL;

enum TransferReadStreamActorCommand {
    Next {
        next_offset: i64,
        length: i64,
        resp_tx: mpsc::Sender<Result<FluxonFsTransferReadStreamNextResultWire, FlatDict>>,
    },
    Close,
}

// ActorOwned keeps one opened source file and a bounded sequential prefetch
// buffer. The worker still consumes the stream strictly in order; the actor
// only decouples source-side file reads from per-chunk RPC timing.
struct TransferReadStreamActorOwned {
    file: fs::File,
    file_size: i64,
    next_offset: i64,
    producer_read_offset: i64,
    replay_valid: bool,
    replay_offset: i64,
    replay_data: Vec<u8>,
    prefetched_segments: VecDeque<Vec<u8>>,
    prefetched_head_offset: usize,
    prefetched_total_bytes: usize,
    terminal_error: Option<FlatDict>,
    command_rx: mpsc::Receiver<TransferReadStreamActorCommand>,
}

#[derive(Debug, Clone)]
struct TransferReadStreamActorHandle {
    file_size: i64,
    command_tx: mpsc::Sender<TransferReadStreamActorCommand>,
}

#[derive(Debug)]
pub(crate) enum TransferWorkerExecutionError {
    Fatal(FlatDict),
    Stop(FluxonFsTransferWorkerStopReasonWire),
}

// Bridges a detached local worker thread with master-issued lease decisions.
// Every long-running action must go through this controller before more visible
// progress is made on the destination filesystem.
struct TransferWorkerRemoteControl {
    api: Arc<FluxonUserApi>,
    master_id: String,
    assignment: FluxonFsTransferWorkerAssignmentWire,
    heartbeat: TransferWorkerHeartbeatGate,
    open_streams: Mutex<BTreeMap<String, TransferReadStreamHandle>>,
    progress: Arc<TransferWorkerProgressWindow>,
}

#[derive(Debug, Clone)]
enum TransferWorkerRemoteControlTerminalState {
    Stop(FluxonFsTransferWorkerStopReasonWire),
    Fatal(FlatDict),
}

struct TransferWorkerHeartbeatGate {
    state: Mutex<TransferWorkerHeartbeatGateState>,
    heartbeat_cv: Condvar,
}

struct TransferWorkerHeartbeatGateState {
    last_heartbeat_completed_unix_ms: i64,
    last_heartbeat_materialized_empty_dirs: i64,
    granted_lease_expire_unix_ms: i64,
    heartbeat_inflight: bool,
    terminal_state: Option<TransferWorkerRemoteControlTerminalState>,
}

#[derive(Debug)]
enum TransferWorkerHeartbeatGateError {
    Retryable {
        heartbeat_detail: &'static str,
        detail: String,
    },
    Terminal(TransferWorkerExecutionError),
}

#[derive(Debug)]
struct PreparedTransferFile {
    staging_relpath: String,
    final_relpath: String,
    visible_size: i64,
}

#[derive(Debug)]
struct PreparedTransferCollectInfo {
    collect_kind: FluxonFsTransferCollectInfoKind,
    staging_relpath: String,
    output_relpath: String,
    materialized_bytes: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TransferReadStreamHandle {
    stream_id: String,
    file_size: i64,
}

// Worker-local file lanes are an execution detail of one accepted worker
// attempt. They must not change batch ownership or durable lease semantics.
#[derive(Debug, Clone)]
struct TransferWorkerLanePolicy {
    initial_file_lanes: usize,
    max_file_lanes: usize,
    target_goodput_bytes_per_sec: i64,
    lane_ramp_interval: Duration,
    lane_poll_interval: Duration,
    min_improvement_percent: i64,
}

impl TransferWorkerLanePolicy {
    fn production_default() -> Self {
        Self {
            initial_file_lanes: TRANSFER_WORKER_INITIAL_FILE_LANES,
            max_file_lanes: TRANSFER_WORKER_MAX_FILE_LANES,
            target_goodput_bytes_per_sec: TRANSFER_WORKER_TARGET_GOODPUT_BYTES_PER_SEC,
            lane_ramp_interval: TRANSFER_WORKER_LANE_RAMP_INTERVAL,
            lane_poll_interval: TRANSFER_WORKER_LANE_POLL_INTERVAL,
            min_improvement_percent: TRANSFER_WORKER_MIN_IMPROVEMENT_PERCENT,
        }
    }

    fn normalized(&self) -> Self {
        let max_file_lanes = self.max_file_lanes.max(1);
        let initial_file_lanes = self.initial_file_lanes.clamp(1, max_file_lanes);
        let target_goodput_bytes_per_sec = self.target_goodput_bytes_per_sec.max(1);
        let lane_poll_interval = if self.lane_poll_interval.is_zero() {
            Duration::from_millis(1)
        } else {
            self.lane_poll_interval
        };
        Self {
            initial_file_lanes,
            max_file_lanes,
            target_goodput_bytes_per_sec,
            lane_ramp_interval: self.lane_ramp_interval,
            lane_poll_interval,
            min_improvement_percent: self.min_improvement_percent.max(0),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TransferWorkerLaneFileResult {
    result: FluxonFsTransferWorkerFileResultWire,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TransferWorkerLaneFailedFileResult {
    result: FluxonFsTransferWorkerFailedFileResultWire,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TransferWorkerLaneTask {
    File(FluxonFsTransferManifestEntryWire),
    EmptyDir(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TransferWorkerLaneOutcome {
    EmptyDirCreated,
    Visible(TransferWorkerLaneFileResult),
    Failed(TransferWorkerLaneFailedFileResult),
}

fn decrement_pending_lane_result_count(counter: &AtomicUsize) {
    counter
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
            current.checked_sub(1)
        })
        .expect("pending lane result count underflow");
}

fn transfer_worker_lane_execution_exhausted(
    active_lane_count: usize,
    queued_result_count: usize,
    finished_tasks: usize,
    total_tasks: usize,
) -> bool {
    active_lane_count == 0 && queued_result_count == 0 && finished_tasks < total_tasks
}

#[derive(Debug, Clone)]
struct TransferWorkerLogContext {
    job_id: String,
    batch_id: String,
    worker_id: String,
    worker_task_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TransferWorkerThroughputSample {
    window_started_unix_ms: i64,
    window_elapsed_ms: i64,
    window_bytes: i64,
    window_goodput_bytes_per_sec: i64,
    desired_file_lanes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TransferWorkerProgressSnapshot {
    total_written_bytes: i64,
    peak_sample_goodput_bytes_per_sec: i64,
    last_sample: TransferWorkerThroughputSample,
}

#[derive(Debug)]
struct TransferWorkerProgressWindow {
    policy: Arc<TransferWorkerLanePolicy>,
    window_bytes: AtomicI64,
    window_started_unix_ms: AtomicI64,
    last_window_goodput_bytes_per_sec: AtomicI64,
    total_written_bytes: AtomicI64,
    total_materialized_empty_dirs: AtomicI64,
    peak_sample_goodput_bytes_per_sec: AtomicI64,
    last_logged_unix_ms: AtomicI64,
    desired_file_lanes: AtomicUsize,
}

impl TransferWorkerProgressWindow {
    fn new(policy: Arc<TransferWorkerLanePolicy>, now_unix_ms: i64) -> Self {
        Self {
            desired_file_lanes: AtomicUsize::new(policy.initial_file_lanes),
            policy,
            window_bytes: AtomicI64::new(0),
            window_started_unix_ms: AtomicI64::new(now_unix_ms),
            last_window_goodput_bytes_per_sec: AtomicI64::new(0),
            total_written_bytes: AtomicI64::new(0),
            total_materialized_empty_dirs: AtomicI64::new(0),
            peak_sample_goodput_bytes_per_sec: AtomicI64::new(0),
            last_logged_unix_ms: AtomicI64::new(now_unix_ms),
        }
    }

    fn desired_file_lanes(&self) -> usize {
        self.desired_file_lanes.load(Ordering::SeqCst)
    }

    fn record_written_bytes_and_maybe_ramp(&self, bytes: i64, now_unix_ms: i64) {
        let normalized = bytes.max(0);
        self.window_bytes.fetch_add(normalized, Ordering::SeqCst);
        self.total_written_bytes.fetch_add(normalized, Ordering::SeqCst);
        self.maybe_ramp(now_unix_ms);
    }

    fn record_materialized_empty_dir(&self) {
        self.total_materialized_empty_dirs.fetch_add(1, Ordering::SeqCst);
    }

    fn total_materialized_empty_dirs(&self) -> i64 {
        self.total_materialized_empty_dirs.load(Ordering::SeqCst)
    }

    fn maybe_ramp(&self, now_unix_ms: i64) {
        let window_started_unix_ms = self.window_started_unix_ms.load(Ordering::SeqCst);
        let elapsed_ms = now_unix_ms.saturating_sub(window_started_unix_ms);
        if elapsed_ms < self.policy.lane_ramp_interval.as_millis() as i64 {
            return;
        }
        if self
            .window_started_unix_ms
            .compare_exchange(
                window_started_unix_ms,
                now_unix_ms,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return;
        }
        let window_bytes = self.window_bytes.swap(0, Ordering::SeqCst);
        let current_goodput = if elapsed_ms <= 0 {
            0
        } else {
            window_bytes
                .saturating_mul(1000)
                .saturating_div(elapsed_ms.max(1))
        };
        if window_bytes <= 0 {
            self.last_window_goodput_bytes_per_sec
                .store(current_goodput, Ordering::SeqCst);
            return;
        }
        let previous_goodput = self
            .last_window_goodput_bytes_per_sec
            .swap(current_goodput, Ordering::SeqCst);
        if self.desired_file_lanes.load(Ordering::SeqCst) >= self.policy.max_file_lanes {
            return;
        }
        if current_goodput >= self.policy.target_goodput_bytes_per_sec {
            return;
        }
        if previous_goodput > 0 {
            let delta = current_goodput.saturating_sub(previous_goodput);
            let improvement_percent =
                delta.saturating_mul(100).saturating_div(previous_goodput.max(1));
            if improvement_percent < self.policy.min_improvement_percent {
                return;
            }
        }
        self.desired_file_lanes.fetch_add(1, Ordering::SeqCst);
    }

    fn maybe_take_log_sample(&self, now_unix_ms: i64) -> Option<TransferWorkerThroughputSample> {
        let last_logged_unix_ms = self.last_logged_unix_ms.load(Ordering::SeqCst);
        let elapsed_ms = now_unix_ms.saturating_sub(last_logged_unix_ms);
        if elapsed_ms < TRANSFER_WORKER_THROUGHPUT_LOG_INTERVAL.as_millis() as i64 {
            return None;
        }
        if self
            .last_logged_unix_ms
            .compare_exchange(
                last_logged_unix_ms,
                now_unix_ms,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return None;
        }
        let window_started_unix_ms = self.window_started_unix_ms.load(Ordering::SeqCst);
        let window_elapsed_ms = now_unix_ms.saturating_sub(window_started_unix_ms);
        let window_bytes = self.window_bytes.load(Ordering::SeqCst);
        let window_goodput_bytes_per_sec = if window_elapsed_ms <= 0 {
            0
        } else {
            window_bytes
                .saturating_mul(1000)
                .saturating_div(window_elapsed_ms.max(1))
        };
        self.peak_sample_goodput_bytes_per_sec.fetch_max(
            window_goodput_bytes_per_sec.max(0),
            Ordering::SeqCst,
        );
        Some(TransferWorkerThroughputSample {
            window_started_unix_ms,
            window_elapsed_ms,
            window_bytes,
            window_goodput_bytes_per_sec,
            desired_file_lanes: self.desired_file_lanes(),
        })
    }

    fn snapshot(&self, now_unix_ms: i64) -> TransferWorkerProgressSnapshot {
        let window_started_unix_ms = self.window_started_unix_ms.load(Ordering::SeqCst);
        let window_elapsed_ms = now_unix_ms.saturating_sub(window_started_unix_ms);
        let window_bytes = self.window_bytes.load(Ordering::SeqCst);
        let window_goodput_bytes_per_sec = if window_elapsed_ms <= 0 {
            0
        } else {
            window_bytes
                .saturating_mul(1000)
                .saturating_div(window_elapsed_ms.max(1))
        };
        let peak_sample_goodput_bytes_per_sec = self
            .peak_sample_goodput_bytes_per_sec
            .fetch_max(window_goodput_bytes_per_sec.max(0), Ordering::SeqCst)
            .max(window_goodput_bytes_per_sec.max(0));
        TransferWorkerProgressSnapshot {
            total_written_bytes: self.total_written_bytes.load(Ordering::SeqCst),
            peak_sample_goodput_bytes_per_sec,
            last_sample: TransferWorkerThroughputSample {
                window_started_unix_ms,
                window_elapsed_ms,
                window_bytes,
                window_goodput_bytes_per_sec,
                desired_file_lanes: self.desired_file_lanes(),
            },
        }
    }
}

fn transfer_worker_telemetry_from_progress_snapshot(
    progress_snapshot: &TransferWorkerProgressSnapshot,
) -> FluxonFsTransferWorkerHeartbeatTelemetryWire {
    let sample = &progress_snapshot.last_sample;
    FluxonFsTransferWorkerHeartbeatTelemetryWire {
        total_written_bytes: progress_snapshot.total_written_bytes,
        window_started_unix_ms: sample.window_started_unix_ms,
        window_elapsed_ms: sample.window_elapsed_ms,
        window_bytes: sample.window_bytes,
        window_goodput_bytes_per_sec: sample.window_goodput_bytes_per_sec,
        desired_file_lanes: sample.desired_file_lanes as i64,
    }
}

impl TransferReadStreamActorOwned {
    fn run(mut self) {
        while let Ok(cmd) = self.command_rx.recv() {
            match cmd {
                TransferReadStreamActorCommand::Next {
                    next_offset,
                    length,
                    resp_tx,
                } => {
                    let _ = resp_tx.send(self.handle_next(next_offset, length));
                }
                TransferReadStreamActorCommand::Close => {
                    return;
                }
            }
        }
    }

    fn handle_next(
        &mut self,
        next_offset: i64,
        length: i64,
    ) -> Result<FluxonFsTransferReadStreamNextResultWire, FlatDict> {
        if next_offset < 0 || length < 0 || (length as usize) > CHUNK_BYTES {
            return Err(resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "transfer stream next offset/length out of range: next_offset={} length={}",
                    next_offset, length
                ),
            })));
        }
        if let Some(err) = self.terminal_error.clone() {
            return Err(err);
        }
        if self.replay_valid && next_offset == self.replay_offset {
            return Ok(FluxonFsTransferReadStreamNextResultWire {
                stream_missing: false,
                data: self.replay_data.clone(),
            });
        }
        if next_offset != self.next_offset {
            return Err(resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "transfer stream next offset mismatch: expected={} replay_offset={} got={}",
                    self.next_offset, self.replay_offset, next_offset
                ),
            })));
        }
        if next_offset >= self.file_size {
            self.replay_valid = true;
            self.replay_offset = next_offset;
            self.replay_data.clear();
            return Ok(FluxonFsTransferReadStreamNextResultWire {
                stream_missing: false,
                data: Vec::new(),
            });
        }
        self.fill_prefetch_queue().map_err(|err| self.cache_terminal_error(err))?;
        let to_take = std::cmp::min(length as usize, (self.file_size - next_offset) as usize);
        let buf = self
            .take_prefetched_bytes(to_take)
            .map_err(|err| self.cache_terminal_error(err))?;
        self.replay_valid = true;
        self.replay_offset = next_offset;
        self.replay_data = buf.clone();
        self.next_offset = next_offset.saturating_add(buf.len() as i64);
        self.fill_prefetch_queue().map_err(|err| self.cache_terminal_error(err))?;
        Ok(FluxonFsTransferReadStreamNextResultWire {
            stream_missing: false,
            data: buf,
        })
    }

    fn cache_terminal_error(&mut self, err: FlatDict) -> FlatDict {
        self.terminal_error = Some(err.clone());
        err
    }

    fn fill_prefetch_queue(&mut self) -> Result<(), FlatDict> {
        while self.prefetched_total_bytes < TRANSFER_READ_STREAM_PREFETCH_BYTES
            && self.producer_read_offset < self.file_size
        {
            let remaining = (self.file_size - self.producer_read_offset) as usize;
            let read_len = std::cmp::min(CHUNK_BYTES, remaining);
            let mut buf = vec![0u8; read_len];
            self.file.read_exact(&mut buf).map_err(resp_err_io)?;
            self.producer_read_offset = self.producer_read_offset.saturating_add(read_len as i64);
            self.prefetched_total_bytes = self.prefetched_total_bytes.saturating_add(read_len);
            self.prefetched_segments.push_back(buf);
        }
        Ok(())
    }

    fn take_prefetched_bytes(&mut self, needed: usize) -> Result<Vec<u8>, FlatDict> {
        let mut out = Vec::with_capacity(needed);
        while out.len() < needed {
            let Some(front) = self.prefetched_segments.front() else {
                return Err(resp_err_kverr(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "transfer stream prefetch underflow: needed={} produced={}",
                        needed,
                        out.len()
                    ),
                })));
            };
            let available = front.len().saturating_sub(self.prefetched_head_offset);
            let take = std::cmp::min(available, needed - out.len());
            out.extend_from_slice(
                &front[self.prefetched_head_offset..self.prefetched_head_offset + take],
            );
            self.prefetched_head_offset += take;
            self.prefetched_total_bytes = self.prefetched_total_bytes.saturating_sub(take);
            if self.prefetched_head_offset == front.len() {
                self.prefetched_segments.pop_front();
                self.prefetched_head_offset = 0;
            }
        }
        Ok(out)
    }
}

impl TransferReadStreamActorHandle {
    fn new_unstarted(
        mut file: fs::File,
        file_size: i64,
        initial_offset: i64,
    ) -> Result<(Self, TransferReadStreamActorOwned), FlatDict> {
        if initial_offset < 0 || initial_offset > file_size {
            return Err(resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "transfer stream initial offset out of range: initial_offset={} file_size={}",
                    initial_offset, file_size
                ),
            })));
        }
        file.seek(SeekFrom::Start(initial_offset as u64))
            .map_err(resp_err_io)?;
        let (command_tx, command_rx) = mpsc::channel();
        let actor = TransferReadStreamActorOwned {
            file,
            file_size,
            next_offset: initial_offset,
            producer_read_offset: initial_offset,
            replay_valid: false,
            replay_offset: initial_offset,
            replay_data: Vec::new(),
            prefetched_segments: VecDeque::new(),
            prefetched_head_offset: 0,
            prefetched_total_bytes: 0,
            terminal_error: None,
            command_rx,
        };
        Ok((
            Self {
                file_size,
                command_tx,
            },
            actor,
        ))
    }

    fn start(stream_id: &str, actor: TransferReadStreamActorOwned) -> Result<(), FlatDict> {
        thread::Builder::new()
            .name(format!("fluxon_fs_transfer_read_stream_{}", stream_id))
            .spawn(move || actor.run())
            .map_err(|err| {
                resp_err_kverr(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "spawn transfer read stream actor failed: stream_id={} err={}",
                        stream_id, err
                    ),
                }))
            })?;
        Ok(())
    }

    fn next_chunk(
        &self,
        next_offset: i64,
        length: i64,
    ) -> Result<FluxonFsTransferReadStreamNextResultWire, FlatDict> {
        let (resp_tx, resp_rx) = mpsc::channel();
        self.command_tx
            .send(TransferReadStreamActorCommand::Next {
                next_offset,
                length,
                resp_tx,
            })
            .map_err(|_| {
                resp_err_kverr(KvError::Api(ApiError::Unknown {
                    detail: "transfer stream actor command channel closed".to_string(),
                }))
            })?;
        resp_rx.recv().map_err(|_| {
            resp_err_kverr(KvError::Api(ApiError::Unknown {
                detail: "transfer stream actor response channel closed".to_string(),
            }))
        })?
    }

    fn close(&self) {
        let _ = self.command_tx.send(TransferReadStreamActorCommand::Close);
    }
}

struct TransferWorkerCoordinator<ReadChunkFn, CheckpointFn>
where
    ReadChunkFn: Fn(&FluxonFsTransferManifestEntryWire, i64, i64) -> Result<Vec<u8>, TransferWorkerExecutionError>,
    CheckpointFn: Fn() -> Result<(), TransferWorkerExecutionError>,
{
    log_context: TransferWorkerLogContext,
    policy: Arc<TransferWorkerLanePolicy>,
    checkpoint_continue: CheckpointFn,
    read_chunk: ReadChunkFn,
    progress: Arc<TransferWorkerProgressWindow>,
    stopped: AtomicBool,
}

impl<ReadChunkFn, CheckpointFn> TransferWorkerCoordinator<ReadChunkFn, CheckpointFn>
where
    ReadChunkFn: Fn(&FluxonFsTransferManifestEntryWire, i64, i64) -> Result<Vec<u8>, TransferWorkerExecutionError>,
    CheckpointFn: Fn() -> Result<(), TransferWorkerExecutionError>,
{
    fn new(
        log_context: TransferWorkerLogContext,
        policy: Arc<TransferWorkerLanePolicy>,
        progress: Arc<TransferWorkerProgressWindow>,
        checkpoint_continue: CheckpointFn,
        read_chunk: ReadChunkFn,
    ) -> Self {
        let policy = Arc::new(policy.normalized());
        Self {
            log_context,
            policy: policy.clone(),
            checkpoint_continue,
            read_chunk,
            progress,
            stopped: AtomicBool::new(false),
        }
    }

    fn desired_file_lanes(&self) -> usize {
        self.progress.desired_file_lanes()
    }

    fn lane_poll_interval(&self) -> Duration {
        self.policy.lane_poll_interval
    }

    fn checkpoint_continue(&self) -> Result<(), TransferWorkerExecutionError> {
        if self.stopped.load(Ordering::SeqCst) {
            return Err(TransferWorkerExecutionError::Stop(
                FluxonFsTransferWorkerStopReasonWire::Superseded,
            ));
        }
        (self.checkpoint_continue)()
    }

    fn read_chunk(
        &self,
        file: &FluxonFsTransferManifestEntryWire,
        read_offset: i64,
        length: i64,
    ) -> Result<Vec<u8>, TransferWorkerExecutionError> {
        if self.stopped.load(Ordering::SeqCst) {
            return Err(TransferWorkerExecutionError::Stop(
                FluxonFsTransferWorkerStopReasonWire::Superseded,
            ));
        }
        (self.read_chunk)(file, read_offset, length)
    }

    fn record_written_bytes(&self, bytes: i64) {
        self.progress
            .record_written_bytes_and_maybe_ramp(bytes, chrono::Utc::now().timestamp_millis());
    }

    fn record_materialized_empty_dir(&self) {
        self.progress.record_materialized_empty_dir();
    }

    fn tick_progress_window(&self) {
        let now_unix_ms = chrono::Utc::now().timestamp_millis();
        self.progress.maybe_ramp(now_unix_ms);
        if let Some(sample) = self.progress.maybe_take_log_sample(now_unix_ms) {
            tracing::info!(
                "transfer worker throughput sample: job_id={} batch_id={} worker_id={} worker_task_id={} sample_unix_ms={} window_started_unix_ms={} window_elapsed_ms={} window_bytes={} window_goodput_bytes_per_sec={} desired_file_lanes={}",
                self.log_context.job_id,
                self.log_context.batch_id,
                self.log_context.worker_id,
                self.log_context.worker_task_id,
                now_unix_ms,
                sample.window_started_unix_ms,
                sample.window_elapsed_ms,
                sample.window_bytes,
                sample.window_goodput_bytes_per_sec,
                sample.desired_file_lanes,
            );
        }
    }

    fn progress_snapshot(&self) -> TransferWorkerProgressSnapshot {
        self.progress.snapshot(chrono::Utc::now().timestamp_millis())
    }

    fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
    }
}

impl TransferReadStreamRegistryHandle {
    pub(super) fn new() -> Self {
        Self::default()
    }

    fn open_stream(
        &self,
        root_dir_abs: &str,
        open: FluxonFsTransferReadStreamOpenWire,
    ) -> Result<FluxonFsTransferReadStreamOpenResultWire, FlatDict> {
        let dedup_key = (
            open.worker_task_id.clone(),
            open.export.clone(),
            open.relpath.clone(),
        );
        {
            let state = self.state.lock();
            if let Some(stream_id) = state.dedup_by_worker_file.get(&dedup_key) {
                let entry = state
                    .streams
                    .get(stream_id)
                    .ok_or_else(|| {
                        resp_err_kverr(KvError::Api(ApiError::Unknown {
                            detail: format!(
                                "transfer read stream dedup state missing stream entry: stream_id={}",
                                stream_id
                            ),
                        }))
                    })?
                    .clone();
                return Ok(FluxonFsTransferReadStreamOpenResultWire {
                    stream_id: stream_id.clone(),
                    size: entry.file_size,
                });
            }
        }
        let full_path = safe_join_root(root_dir_abs, open.relpath.as_str()).map_err(resp_err_kverr)?;
        let file = open_file_with_target_path_chmod_retry(&full_path, "open_stream")?;
        let md = file.metadata().map_err(resp_err_io)?;
        let file_size = md.len().min(i64::MAX as u64) as i64;
        let stream_id = uuid::Uuid::new_v4().to_string();
        let (entry, actor) =
            TransferReadStreamActorHandle::new_unstarted(file, file_size, open.initial_offset)?;
        let mut state = self.state.lock();
        if let Some(existing_stream_id) = state.dedup_by_worker_file.get(&dedup_key) {
            let existing_entry = state
                .streams
                .get(existing_stream_id)
                .ok_or_else(|| {
                    resp_err_kverr(KvError::Api(ApiError::Unknown {
                        detail: format!(
                            "transfer read stream dedup state missing existing entry: stream_id={}",
                            existing_stream_id
                        ),
                    }))
                })?
                .clone();
            return Ok(FluxonFsTransferReadStreamOpenResultWire {
                stream_id: existing_stream_id.clone(),
                size: existing_entry.file_size,
            });
        }
        state.streams.insert(stream_id.clone(), entry);
        state.dedup_by_worker_file.insert(dedup_key, stream_id.clone());
        drop(state);
        if let Err(resp) = TransferReadStreamActorHandle::start(stream_id.as_str(), actor) {
            let mut state = self.state.lock();
            state.streams.remove(stream_id.as_str());
            state.dedup_by_worker_file
                .retain(|_, existing_stream_id| existing_stream_id != &stream_id);
            return Err(resp);
        }
        Ok(FluxonFsTransferReadStreamOpenResultWire {
            stream_id,
            size: file_size,
        })
    }

    fn next_chunk(
        &self,
        req: FluxonFsTransferReadStreamNextWire,
    ) -> Result<FluxonFsTransferReadStreamNextResultWire, FlatDict> {
        if req.next_offset < 0 || req.length < 0 || (req.length as usize) > CHUNK_BYTES {
            return Err(resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "transfer stream next offset/length out of range: next_offset={} length={}",
                    req.next_offset, req.length
                ),
            })));
        }
        let entry = {
            let state = self.state.lock();
            match state.streams.get(req.stream_id.as_str()).cloned() {
                Some(v) => v,
                None => {
                    return Ok(FluxonFsTransferReadStreamNextResultWire {
                        stream_missing: true,
                        data: Vec::new(),
                    });
                }
            }
        };
        entry.next_chunk(req.next_offset, req.length)
    }

    fn close_stream(&self, stream_id: &str) {
        let mut state = self.state.lock();
        let Some(entry) = state.streams.remove(stream_id) else {
            return;
        };
        state.dedup_by_worker_file.retain(|_, existing_stream_id| existing_stream_id != stream_id);
        entry.close();
    }
}

fn require_transfer_payload_str(payload: &FlatDict, key: &str) -> Result<String, FlatDict> {
    require_str(payload, key).map_err(resp_err_kverr)
}

fn require_transfer_payload_i64(payload: &FlatDict, key: &str) -> Result<i64, FlatDict> {
    require_i64(payload, key).map_err(resp_err_kverr)
}

fn parse_transfer_scan_assignment_payload(
    payload: &FlatDict,
) -> Result<FluxonFsTransferScanAssignmentWire, FlatDict> {
    let raw = require_transfer_payload_str(payload, "assignment_json")?;
    serde_json::from_str(&raw).map_err(|e| {
        resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!("parse assignment_json failed: {}", e),
        }))
    })
}

fn parse_transfer_worker_assignment_payload(
    payload: &FlatDict,
) -> Result<FluxonFsTransferWorkerAssignmentWire, FlatDict> {
    let raw = require_transfer_payload_str(payload, "assignment_json")?;
    serde_json::from_str(&raw).map_err(|e| {
        resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!("parse assignment_json failed: {}", e),
        }))
    })
}

fn parse_transfer_stream_open_payload(
    payload: &FlatDict,
) -> Result<FluxonFsTransferReadStreamOpenWire, FlatDict> {
    Ok(FluxonFsTransferReadStreamOpenWire {
        worker_task_id: require_transfer_payload_str(payload, "worker_task_id")?,
        export: require_transfer_payload_str(payload, "export")?,
        relpath: require_transfer_payload_str(payload, "relpath")?,
        initial_offset: require_transfer_payload_i64(payload, "initial_offset")?,
    })
}

fn parse_transfer_stream_next_payload(
    payload: &FlatDict,
) -> Result<FluxonFsTransferReadStreamNextWire, FlatDict> {
    Ok(FluxonFsTransferReadStreamNextWire {
        stream_id: require_transfer_payload_str(payload, "stream_id")?,
        next_offset: require_transfer_payload_i64(payload, "next_offset")?,
        length: require_transfer_payload_i64(payload, "length")?,
    })
}

fn parse_transfer_stream_close_payload(
    payload: &FlatDict,
) -> Result<FluxonFsTransferReadStreamCloseWire, FlatDict> {
    Ok(FluxonFsTransferReadStreamCloseWire {
        stream_id: require_transfer_payload_str(payload, "stream_id")?,
    })
}

fn encode_transfer_stream_open_result_payload(
    result: &FluxonFsTransferReadStreamOpenResultWire,
) -> FlatDict {
    resp_ok(BTreeMap::from([
        (
            "stream_id".to_string(),
            FlatValue::String(result.stream_id.clone()),
        ),
        ("size".to_string(), FlatValue::Int64(result.size)),
    ]))
}

fn decode_transfer_stream_open_result_payload(
    resp: &FlatDict,
) -> Result<FluxonFsTransferReadStreamOpenResultWire, TransferWorkerRpcFailure> {
    if !transfer_rpc_response_ok(resp) {
        return Err(TransferWorkerRpcFailure::Fatal(resp.clone()));
    }
    Ok(FluxonFsTransferReadStreamOpenResultWire {
        stream_id: require_str(resp, "stream_id").map_err(resp_err_kverr).map_err(
            |err| {
                invalid_transfer_rpc_response(format!(
                    "transfer read stream open response missing stream_id: err={}",
                    transfer_rpc_response_err_text(&err)
                ))
            },
        )?,
        size: require_i64(resp, "size").map_err(resp_err_kverr).map_err(|err| {
            invalid_transfer_rpc_response(format!(
                "transfer read stream open response missing size: err={}",
                transfer_rpc_response_err_text(&err)
            ))
        })?,
    })
}

fn encode_transfer_stream_next_result_payload(
    result: &FluxonFsTransferReadStreamNextResultWire,
) -> FlatDict {
    resp_ok(BTreeMap::from([
        (
            "stream_missing".to_string(),
            FlatValue::Bool(result.stream_missing),
        ),
        ("data".to_string(), FlatValue::Bytes(result.data.clone())),
    ]))
}

fn decode_transfer_stream_next_result_payload(
    resp: &FlatDict,
) -> Result<FluxonFsTransferReadStreamNextResultWire, TransferWorkerRpcFailure> {
    if !transfer_rpc_response_ok(resp) {
        return Err(TransferWorkerRpcFailure::Fatal(resp.clone()));
    }
    let stream_missing = match resp.get("stream_missing") {
        Some(FlatValue::Bool(v)) => *v,
        _ => {
            return Err(invalid_transfer_rpc_response(format!(
                "transfer read stream next response missing stream_missing: err={}",
                transfer_rpc_response_err_text(resp)
            )));
        }
    };
    let data = match resp.get("data") {
        Some(FlatValue::Bytes(v)) => v.clone(),
        _ => {
            return Err(invalid_transfer_rpc_response(format!(
                "transfer read stream next response missing data: err={}",
                transfer_rpc_response_err_text(resp)
            )));
        }
    };
    Ok(FluxonFsTransferReadStreamNextResultWire {
        stream_missing,
        data,
    })
}

fn normalize_child_relpath(parent: &str, child_name: &str) -> String {
    if parent == "." {
        child_name.to_string()
    } else {
        format!("{}/{}", parent.trim_end_matches('/'), child_name)
    }
}

fn skip_entry_matches_relpath(skip: &FluxonFsTransferSkipEntryWire, relpath: &str) -> bool {
    match skip.kind {
        FluxonFsTransferSkipEntryKind::Dir => {
            relpath == skip.relpath
                || relpath
                    .strip_prefix(skip.relpath.as_str())
                    .is_some_and(|suffix| suffix.starts_with('/'))
        }
        FluxonFsTransferSkipEntryKind::File => relpath == skip.relpath,
    }
}

fn is_relpath_skipped(skip_entries: &[FluxonFsTransferSkipEntryWire], relpath: &str) -> bool {
    skip_entries
        .iter()
        .any(|skip| skip_entry_matches_relpath(skip, relpath))
}

fn file_name_from_relpath(relpath: &str) -> Result<&str, FlatDict> {
    relpath.rsplit('/').next().filter(|v| !v.is_empty()).ok_or_else(|| {
        resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!("relpath must contain file name: {}", relpath),
        }))
    })
}

fn transfer_staging_dir_for_file(staging_prefix: &str, relpath: &str) -> String {
    format!(
        "{}/{}",
        staging_prefix.trim_end_matches('/'),
        relpath.trim_end_matches('/')
    )
}

fn transfer_staging_file_relpath(staging_prefix: &str, relpath: &str) -> Result<String, FlatDict> {
    let file_name = file_name_from_relpath(relpath)?;
    Ok(format!(
        "{}/{}.fluxon.part",
        transfer_staging_dir_for_file(staging_prefix, relpath),
        file_name
    ))
}

fn nearest_existing_dir(path: &Path) -> Result<PathBuf, std::io::Error> {
    for ancestor in path.ancestors() {
        match fs::metadata(ancestor) {
            Ok(metadata) => {
                if metadata.is_dir() {
                    return Ok(ancestor.to_path_buf());
                }
                return Err(std::io::Error::other(format!(
                    "nearest existing ancestor is not a directory: path={}",
                    ancestor.display()
                )));
            }
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) if err.kind() == ErrorKind::PermissionDenied => continue,
            Err(err) => {
                return Err(std::io::Error::new(
                    err.kind(),
                    format!(
                        "inspect nearest existing ancestor failed: path={} err={}",
                        ancestor.display(),
                        err
                    ),
                ));
            }
        }
    }
    Err(std::io::Error::new(
        ErrorKind::NotFound,
        format!("no existing ancestor found for path={}", path.display()),
    ))
}

fn repair_permission_denied_dir_for_retry(
    repair_anchor: &Path,
    op: &str,
    target_path: &Path,
    initial_err: &std::io::Error,
) -> Result<PathBuf, FlatDict> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let repair_dir = nearest_existing_dir(repair_anchor).map_err(|locate_err| {
            resp_err_kverr(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "locate existing directory for chmod retry failed: op={} target_path={} repair_anchor={} initial_err={} locate_err={}",
                    op,
                    target_path.display(),
                    repair_anchor.display(),
                    initial_err,
                    locate_err
                ),
            }))
        })?;
        fs::set_permissions(&repair_dir, fs::Permissions::from_mode(0o777)).map_err(|chmod_err| {
            resp_err_kverr(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "chmod 777 parent directory for retry failed: op={} target_path={} repair_dir={} initial_err={} chmod_err={}",
                    op,
                    target_path.display(),
                    repair_dir.display(),
                    initial_err,
                    chmod_err
                ),
            }))
        })?;
        tracing::warn!(
            "transfer permission retry repair applied: op={} target_path={} repair_dir={} initial_err={}",
            op,
            target_path.display(),
            repair_dir.display(),
            initial_err
        );
        Ok(repair_dir)
    }
    #[cfg(not(unix))]
    {
        let _ = repair_anchor;
        let _ = op;
        Err(resp_err_io(std::io::Error::new(
            initial_err.kind(),
            format!(
                "permission denied and chmod retry unsupported on non-unix: target_path={} err={}",
                target_path.display(),
                initial_err
            ),
        )))
    }
}

fn retry_after_parent_dir_chmod<T, F>(
    repair_anchor: &Path,
    op: &str,
    target_path: &Path,
    mut attempt: F,
) -> Result<T, FlatDict>
where
    F: FnMut() -> Result<T, std::io::Error>,
{
    match attempt() {
        Ok(value) => Ok(value),
        Err(initial_err) if initial_err.kind() == ErrorKind::PermissionDenied => {
            let repair_dir =
                repair_permission_denied_dir_for_retry(repair_anchor, op, target_path, &initial_err)?;
            attempt().map_err(|retry_err| {
                resp_err_kverr(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "retry after chmod 777 failed: op={} target_path={} repair_dir={} initial_err={} retry_err={}",
                        op,
                        target_path.display(),
                        repair_dir.display(),
                        initial_err,
                        retry_err
                    ),
                }))
            })
        }
        Err(err) => Err(resp_err_io(err)),
    }
}

fn create_dir_all_with_parent_dir_chmod_retry(path: &Path) -> Result<(), FlatDict> {
    retry_after_parent_dir_chmod(path, "create_dir_all", path, || fs::create_dir_all(path))
}

fn open_create_file_with_parent_dir_chmod_retry(path: &Path) -> Result<fs::File, FlatDict> {
    let repair_anchor = path.parent().unwrap_or(path);
    retry_after_parent_dir_chmod(repair_anchor, "open_create_file", path, || {
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
    })
}

fn open_file_with_target_path_chmod_retry(path: &Path, op: &str) -> Result<fs::File, FlatDict> {
    let repair_anchor = path.parent().unwrap_or(path);
    retry_after_target_path_chmod(repair_anchor, op, path, || fs::File::open(path))
}

fn rename_with_dst_parent_dir_chmod_retry(src: &Path, dst: &Path) -> Result<(), FlatDict> {
    let repair_anchor = dst.parent().unwrap_or(dst);
    retry_after_parent_dir_chmod(repair_anchor, "rename", dst, || fs::rename(src, dst))
}

fn ensure_transfer_parent_dirs(root: &PathBuf, relpath: &str) -> Result<(), FlatDict> {
    let full = safe_join_root(root.to_string_lossy().as_ref(), relpath).map_err(resp_err_kverr)?;
    let Some(parent) = full.parent() else {
        return Ok(());
    };
    create_dir_all_with_parent_dir_chmod_retry(parent)
}

fn materialize_transfer_empty_dir(root: &PathBuf, relpath: &str) -> Result<(), FlatDict> {
    if relpath == "." {
        return create_dir_all_with_parent_dir_chmod_retry(root);
    }
    let full = safe_join_root(root.to_string_lossy().as_ref(), relpath).map_err(resp_err_kverr)?;
    create_dir_all_with_parent_dir_chmod_retry(&full)
}

fn read_symlink_target_text(path: PathBuf) -> Result<String, FlatDict> {
    let target = fs::read_link(path).map_err(resp_err_io)?;
    Ok(target.to_string_lossy().to_string())
}

fn build_symlink_notice_collect_blob(
    entries: Vec<FluxonFsTransferSymlinkNoticeEntryWire>,
) -> Result<Vec<u8>, FlatDict> {
    let mut out = Vec::new();
    for entry in entries {
        let line = serde_json::to_string(&entry).map_err(|e| {
            resp_err_kverr(KvError::Api(ApiError::Unknown {
                detail: format!("encode symlink notice json line failed: {}", e),
            }))
        })?;
        out.extend_from_slice(line.as_bytes());
        out.push(b'\n');
    }
    Ok(out)
}

fn build_symlink_collect_infos(
    entries: Vec<FluxonFsTransferSymlinkNoticeEntryWire>,
) -> Result<Vec<FluxonFsTransferBatchCollectInfoWire>, FlatDict> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    Ok(vec![FluxonFsTransferBatchCollectInfoWire {
        collect_kind: FluxonFsTransferCollectInfoKind::SymlinkNotice,
        collect_blob: build_symlink_notice_collect_blob(entries)?,
    }])
}

// Collect the full recursive subtree for one scan decision. Symlinks are not
// followed; they are converted into collect-info records instead.
fn collect_transfer_tree(
    root_dir_abs: &str,
    root_relpath: &str,
    skip_entries: &[FluxonFsTransferSkipEntryWire],
) -> Result<TransferTreeCollection, FlatDict> {
    collect_transfer_tree_with_deadline(root_dir_abs, root_relpath, skip_entries, None)
}

fn collect_transfer_tree_with_deadline(
    root_dir_abs: &str,
    root_relpath: &str,
    skip_entries: &[FluxonFsTransferSkipEntryWire],
    deadline: Option<TransferScanDeadline>,
) -> Result<TransferTreeCollection, FlatDict> {
    if is_relpath_skipped(skip_entries, root_relpath) {
        return Ok(TransferTreeCollection::default());
    }
    let mut out = TransferTreeCollection::default();
    let root_path = safe_join_root(root_dir_abs, root_relpath).map_err(resp_err_kverr)?;
    let md = match retry_after_target_path_chmod(
        root_path.as_path(),
        "symlink_metadata_root",
        root_path.as_path(),
        || fs::symlink_metadata(&root_path),
    ) {
        Ok(v) => v,
        Err(resp) => {
            tracing::warn!(
                "transfer best-effort read repair failed: op=symlink_metadata_root relpath={} resp={:?}",
                root_relpath,
                resp
            );
            return Ok(out);
        }
    };
    if md.file_type().is_symlink() {
        return Ok(out);
    }
    if md.is_file() {
        out.files.push(FluxonFsTransferScanFrontierEntry {
            relpath: root_relpath.to_string(),
            size: md.len().min(i64::MAX as u64) as i64,
        });
        return Ok(out);
    }
    if !md.is_dir() {
        return Ok(out);
    }
    let mut stack: Vec<(PathBuf, String)> = vec![(root_path, root_relpath.to_string())];
    while let Some((dir_abs, rel)) = stack.pop() {
        if deadline.is_some_and(|deadline| deadline.reached()) {
            return Err(resp_err_kverr(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "transfer scan subtree collection exceeded deadline: root_relpath={}",
                    root_relpath
                ),
            })));
        }
        let mut child_dirs: Vec<(PathBuf, String)> = Vec::new();
        let mut has_child_coverage = false;
        let rd = match retry_after_target_path_chmod(
            dir_abs.as_path(),
            "read_dir",
            dir_abs.as_path(),
            || fs::read_dir(&dir_abs),
        ) {
            Ok(v) => v,
            Err(resp) => {
                tracing::warn!(
                    "transfer best-effort read repair failed: op=read_dir relpath={} resp={:?}",
                    rel,
                    resp
                );
                continue;
            }
        };
        for ent in rd {
            if deadline.is_some_and(|deadline| deadline.reached()) {
                return Err(resp_err_kverr(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "transfer scan subtree collection exceeded deadline while reading children: root_relpath={}",
                        root_relpath
                    ),
                })));
            }
            let ent = match ent {
                Ok(v) => v,
                Err(err) if io_error_is_permission_denied(&err) => {
                    log_transfer_permission_denied_skip("read_dir_entry", rel.as_str(), &err);
                    continue;
                }
                Err(err) => return Err(resp_err_io(err)),
            };
            let name = ent.file_name().to_string_lossy().to_string();
            let child_rel = normalize_child_relpath(rel.as_str(), name.as_str());
            if is_relpath_skipped(skip_entries, child_rel.as_str()) {
                continue;
            }
            let child_path = ent.path();
            let md = match retry_after_target_path_chmod(
                child_path.as_path(),
                "symlink_metadata",
                child_path.as_path(),
                || fs::symlink_metadata(&child_path),
            ) {
                Ok(v) => v,
                Err(resp) => {
                    tracing::warn!(
                        "transfer best-effort read repair failed: op=symlink_metadata relpath={} resp={:?}",
                        child_rel,
                        resp
                    );
                    continue;
                }
            };
            if md.file_type().is_symlink() {
                has_child_coverage = true;
                let link_target = match retry_after_parent_dir_chmod(
                    child_path.as_path(),
                    "read_link",
                    child_path.as_path(),
                    || fs::read_link(&child_path),
                ) {
                    Ok(v) => v,
                    Err(resp) => {
                        tracing::warn!(
                            "transfer best-effort read repair failed: op=read_link relpath={} resp={:?}",
                            child_rel,
                            resp
                        );
                        continue;
                    }
                };
                out.symlink_notices.push(FluxonFsTransferSymlinkNoticeEntryWire {
                    relpath: child_rel,
                    link_target: link_target.to_string_lossy().to_string(),
                });
                continue;
            }
            if md.is_dir() {
                has_child_coverage = true;
                child_dirs.push((ent.path(), child_rel));
                continue;
            }
            if md.is_file() {
                has_child_coverage = true;
                out.files.push(FluxonFsTransferScanFrontierEntry {
                    relpath: child_rel,
                    size: md.len().min(i64::MAX as u64) as i64,
                });
            }
        }
        if !has_child_coverage {
            out.empty_dirs.push(rel);
            continue;
        }
        stack.extend(child_dirs.into_iter());
    }
    out.files.sort_by(|a, b| a.relpath.cmp(&b.relpath));
    out.empty_dirs.sort();
    out.symlink_notices.sort_by(|a, b| a.relpath.cmp(&b.relpath));
    Ok(out)
}

fn build_transfer_manifest_blob(
    entries: Vec<FluxonFsTransferScanFrontierEntry>,
    empty_dir_relpaths: Vec<String>,
) -> Result<Vec<u8>, FlatDict> {
    let manifest = FluxonFsTransferManifestWire::new(
        entries
            .into_iter()
            .map(|entry| FluxonFsTransferManifestEntryWire {
                relpath: entry.relpath,
                size: entry.size,
            })
            .collect(),
        empty_dir_relpaths,
    );
    manifest.encode_to_blob().map_err(|e| {
        resp_err_kverr(KvError::Api(ApiError::Unknown {
            detail: format!("encode transfer manifest failed: {}", e),
        }))
    })
}

fn new_child_scan_unit(
    root_relpath: String,
    generation: i64,
    scan_mode: FluxonFsTransferScanMode,
) -> FluxonFsTransferScanChildUnitWire {
    FluxonFsTransferScanChildUnitWire {
        scan_unit_id: uuid::Uuid::new_v4().to_string(),
        root_relpath,
        generation,
        scan_mode,
    }
}

fn new_streaming_child_scan_unit(
    root_relpath: String,
    generation: i64,
) -> FluxonFsTransferScanChildUnitWire {
    new_child_scan_unit(root_relpath, generation, subtree_streaming_scan_mode())
}

const TRANSFER_WORKER_RPC_RETRY_BACKOFF: BackoffConfig = BackoffConfig {
    initial_secs: 1,
    max_secs: 10,
};

const TRANSFER_WORKER_RPC_RETRY_WARN: WarnConfig = WarnConfig {
    warn_interval_secs: DEFAULT_WARN_INTERVAL_SECS,
};

enum TransferWorkerRpcFailure {
    Retryable { detail: String },
    Fatal(FlatDict),
}

fn io_error_is_permission_denied(err: &std::io::Error) -> bool {
    matches!(err.kind(), ErrorKind::PermissionDenied)
        || matches!(err.raw_os_error(), Some(errno) if errno == libc::EACCES || errno == libc::EPERM)
}

fn log_transfer_permission_denied_skip(op: &str, relpath: &str, err: &std::io::Error) {
    tracing::warn!(
        "transfer best-effort skip unreadable source path: op={} relpath={} err={}",
        op,
        relpath,
        err
    );
}

fn transfer_rpc_response_ok(resp: &FlatDict) -> bool {
    matches!(resp.get("ok"), Some(FlatValue::Bool(true)))
}

fn transfer_rpc_response_err_text(resp: &FlatDict) -> String {
    match resp.get("err") {
        Some(FlatValue::String(v)) if !v.trim().is_empty() => v.clone(),
        _ => format!("{:?}", resp),
    }
}

fn invalid_transfer_rpc_response(detail: impl Into<String>) -> TransferWorkerRpcFailure {
    TransferWorkerRpcFailure::Fatal(resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
        detail: detail.into(),
    })))
}

fn transfer_rpc_error_detail(resp: &FlatDict) -> Option<&str> {
    match resp.get("err") {
        Some(FlatValue::String(v)) if !v.trim().is_empty() => Some(v.as_str()),
        _ => None,
    }
}

fn classify_transfer_worker_fatal(resp: &FlatDict) -> Option<(&'static str, String)> {
    let detail = transfer_rpc_error_detail(resp)?;
    if detail.contains("transfer source file size changed during worker execution") {
        return Some(("source_content_changed", detail.to_string()));
    }
    if detail.contains("transfer worker source ended before expected size") {
        return Some(("source_content_changed", detail.to_string()));
    }
    if let Some(FlatValue::Int64(err_kind)) = resp.get("err_kind") {
        if *err_kind == 3 {
            return Some(("source_permission_denied", detail.to_string()));
        }
    }
    if let Some(FlatValue::Int64(err_kind)) = resp.get("err_kind") {
        if *err_kind == 2 {
            if let Some(FlatValue::Int64(errno)) = resp.get("errno") {
                if *errno == libc::ENOENT as i64 {
                    return Some(("source_content_changed", detail.to_string()));
                }
                if *errno == libc::EACCES as i64 || *errno == libc::EPERM as i64 {
                    return Some(("source_permission_denied", detail.to_string()));
                }
            }
        }
    }
    None
}

fn classify_transfer_failed_file(
    file: &FluxonFsTransferManifestEntryWire,
    resp: &FlatDict,
) -> Option<FluxonFsTransferWorkerFailedFileResultWire> {
    let (kind, detail) = classify_transfer_worker_fatal(resp)?;
    let reason_kind = match kind {
        "source_content_changed" => FluxonFsTransferFailedFileReasonKindWire::SourceContentChanged,
        "source_permission_denied" => {
            FluxonFsTransferFailedFileReasonKindWire::SourcePermissionDenied
        }
        _ => return None,
    };
    Some(FluxonFsTransferWorkerFailedFileResultWire {
        relpath: file.relpath.clone(),
        reason_kind,
        reason_detail: detail,
    })
}

impl TransferWorkerExecutionError {
    fn fatal(resp: FlatDict) -> Self {
        Self::Fatal(resp)
    }
}

fn stop_reason_or_superseded(
    stop_reason: Option<FluxonFsTransferWorkerStopReasonWire>,
) -> FluxonFsTransferWorkerStopReasonWire {
    stop_reason.unwrap_or(FluxonFsTransferWorkerStopReasonWire::Superseded)
}

fn transfer_worker_task_dedup_expire_unix_ms(lease_expire_unix_ms: i64) -> i64 {
    lease_expire_unix_ms.saturating_add(TRANSFER_HEARTBEAT_INTERVAL_MS)
}

fn transfer_scan_task_dedup_expire_unix_ms(lease_expire_unix_ms: i64) -> i64 {
    lease_expire_unix_ms.saturating_add(TRANSFER_HEARTBEAT_INTERVAL_MS)
}

fn build_transfer_scan_event(
    assignment: &FluxonFsTransferScanAssignmentWire,
    event_seq_no: i64,
    event_kind: FluxonFsTransferScanEventKindWire,
    direct_files_only_batches: Vec<FluxonFsTransferScanBatchWire>,
    child_scan_units: Vec<FluxonFsTransferScanChildUnitWire>,
    full_dir_batches: Vec<FluxonFsTransferScanBatchWire>,
    error_detail: String,
) -> FluxonFsTransferScanEventWire {
    FluxonFsTransferScanEventWire {
        job_id: assignment.job_id.clone(),
        scan_epoch: assignment.scan_epoch,
        scan_unit_id: assignment.scan_unit_id.clone(),
        scan_task_id: assignment.scan_task_id.clone(),
        root_relpath: assignment.root_relpath.clone(),
        generation: assignment.generation,
        event_seq_no,
        event_kind,
        direct_files_only_batches,
        child_scan_units,
        full_dir_batches,
        error_detail,
    }
}

fn split_same_root_continuation_from_child_scan_units(
    assignment: &FluxonFsTransferScanAssignmentWire,
    child_scan_units: Vec<FluxonFsTransferScanChildUnitWire>,
) -> (Vec<FluxonFsTransferScanChildUnitWire>, bool) {
    let mut continue_locally = false;
    let mut retained = Vec::with_capacity(child_scan_units.len());
    for child in child_scan_units {
        if child.scan_unit_id == assignment.scan_unit_id
            && child.root_relpath == assignment.root_relpath
            && child.generation == assignment.generation
        {
            continue_locally = true;
            continue;
        }
        retained.push(child);
    }
    (retained, continue_locally)
}

fn build_transfer_scan_events_for_result(
    assignment: &FluxonFsTransferScanAssignmentWire,
    event_seq_no_start: i64,
    result: FluxonFsTransferScanResultWire,
) -> (Vec<FluxonFsTransferScanEventWire>, bool, i64) {
    let (child_scan_units, continue_locally) = split_same_root_continuation_from_child_scan_units(
        assignment,
        result.child_scan_units,
    );
    if continue_locally {
        let event = build_transfer_scan_event(
            assignment,
            event_seq_no_start,
            FluxonFsTransferScanEventKindWire::Append,
            result.direct_files_only_batches,
            child_scan_units,
            result.full_dir_batches,
            String::new(),
        );
        return (
            vec![event],
            true,
            event_seq_no_start.saturating_add(1),
        );
    }
    let mut next_event_seq_no = event_seq_no_start;
    let mut events = Vec::new();
    let has_payload = !result.direct_files_only_batches.is_empty()
        || !child_scan_units.is_empty()
        || !result.full_dir_batches.is_empty();
    if has_payload {
        events.push(build_transfer_scan_event(
            assignment,
            next_event_seq_no,
            FluxonFsTransferScanEventKindWire::Append,
            result.direct_files_only_batches,
            child_scan_units,
            result.full_dir_batches,
            String::new(),
        ));
        next_event_seq_no = next_event_seq_no.saturating_add(1);
    }
    events.push(build_transfer_scan_event(
        assignment,
        next_event_seq_no,
        FluxonFsTransferScanEventKindWire::Finished,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        String::new(),
    ));
    (
        events,
        false,
        next_event_seq_no.saturating_add(1),
    )
}

fn send_transfer_scan_event_once(
    api: &FluxonUserApi,
    master_id: &str,
    event: &FluxonFsTransferScanEventWire,
) -> Result<FluxonFsTransferScanEventAckWire, String> {
    if master_id.trim().is_empty() {
        return Err("transfer scan event submit requires non-empty master_id".to_string());
    }
    let event_json = serde_json::to_string(event)
        .map_err(|e| format!("serialize transfer scan event failed: {}", e))?;
    let payload = FlatDict::from([(
        "scan_event_json".to_string(),
        FlatValue::String(event_json),
    )]);
    let resp = api
        .rpc_client()
        .call(
            master_id,
            FS_MASTER_TRANSFER_SCHEDULER_RESULT_RPC_PATH,
            payload,
            Some(TRANSFER_WORKER_COORDINATION_RPC_TIMEOUT_MS),
        )
        .map_err(|e| e.to_string())?;
    let ack_json = match resp.get("scan_event_ack_json") {
        Some(FlatValue::String(v)) if !v.trim().is_empty() => v.clone(),
        _ => {
            return Err(format!(
                "transfer scan event response missing scan_event_ack_json: scan_task_id={} err={}",
                event.scan_task_id,
                transfer_rpc_response_err_text(&resp),
            ));
        }
    };
    serde_json::from_str(&ack_json).map_err(|e| {
        format!(
            "parse transfer scan event ack failed: scan_task_id={} err={}",
            event.scan_task_id, e
        )
    })
}

fn send_transfer_scan_event_with_retry(
    api: &FluxonUserApi,
    master_id: &str,
    assignment: &mut FluxonFsTransferScanAssignmentWire,
    event: &FluxonFsTransferScanEventWire,
) -> Result<FluxonFsTransferScanEventAckWire, String> {
    let mut last_warn: Option<Instant> = None;
    let mut attempt: u32 = 0;
    loop {
        if assignment.lease_expire_unix_ms > 0
            && chrono::Utc::now().timestamp_millis() >= assignment.lease_expire_unix_ms
        {
            return Ok(FluxonFsTransferScanEventAckWire::stop(false));
        }
        match send_transfer_scan_event_once(api, master_id, event) {
            Ok(ack) => {
                if ack.continue_running && ack.lease_expire_unix_ms > 0 {
                    assignment.lease_expire_unix_ms = ack.lease_expire_unix_ms;
                }
                return Ok(ack);
            }
            Err(err) => {
                let now = Instant::now();
                if should_warn(now, &mut last_warn, TRANSFER_WORKER_RPC_RETRY_WARN) {
                    tracing::warn!(
                        "transfer scan event retry: job_id={} scan_unit_id={} scan_task_id={} event_kind={:?} event_seq_no={} attempt={} err={}",
                        assignment.job_id,
                        assignment.scan_unit_id,
                        assignment.scan_task_id,
                        event.event_kind,
                        event.event_seq_no,
                        attempt.saturating_add(1),
                        err,
                    );
                }
                let delay = next_backoff(TRANSFER_WORKER_RPC_RETRY_BACKOFF, attempt);
                attempt = attempt.saturating_add(1);
                std::thread::sleep(delay);
            }
        }
    }
}

fn run_transfer_scan_background_task(
    registry: TransferScanRegistryHandle,
    api: Arc<FluxonUserApi>,
    master_id: String,
    exports: AgentExportsHandle,
    mut assignment: FluxonFsTransferScanAssignmentWire,
) {
    let root_dir_abs = match exports.export_root_dir_abs(assignment.src_export.as_str()) {
        Ok(v) => v,
        Err(err) => {
            let failed = build_transfer_scan_event(
                &assignment,
                0,
                FluxonFsTransferScanEventKindWire::Failed,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                format!("{}", err),
            );
            let _ = send_transfer_scan_event_with_retry(
                api.as_ref(),
                master_id.as_str(),
                &mut assignment,
                &failed,
            );
            registry.finish_task(
                assignment.scan_task_id.as_str(),
                transfer_scan_task_dedup_expire_unix_ms(assignment.lease_expire_unix_ms),
            );
            return;
        }
    };
    tracing::info!(
        "transfer scan task start: job_id={} scan_epoch={} scan_unit_id={} scan_task_id={} root_relpath={} generation={} scan_mode={:?}",
        assignment.job_id,
        assignment.scan_epoch,
        assignment.scan_unit_id,
        assignment.scan_task_id,
        assignment.root_relpath,
        assignment.generation,
        assignment.scan_mode,
    );
    let started = build_transfer_scan_event(
        &assignment,
        0,
        FluxonFsTransferScanEventKindWire::Started,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        String::new(),
    );
    let started_ack = match send_transfer_scan_event_with_retry(
        api.as_ref(),
        master_id.as_str(),
        &mut assignment,
        &started,
    ) {
        Ok(v) => v,
        Err(err) => {
            tracing::warn!(
                "transfer scan task failed to submit started event: job_id={} scan_unit_id={} scan_task_id={} err={}",
                assignment.job_id,
                assignment.scan_unit_id,
                assignment.scan_task_id,
                err,
            );
            registry.finish_task(
                assignment.scan_task_id.as_str(),
                transfer_scan_task_dedup_expire_unix_ms(assignment.lease_expire_unix_ms),
            );
            return;
        }
    };
    if !started_ack.continue_running {
        registry.finish_task(
            assignment.scan_task_id.as_str(),
            transfer_scan_task_dedup_expire_unix_ms(assignment.lease_expire_unix_ms),
        );
        return;
    }
    let mut next_event_seq_no = 1_i64;
    loop {
        let result = match build_transfer_scan_result_for_root_dir_abs(
            root_dir_abs.as_str(),
            &assignment,
        ) {
            Ok(v) => v,
            Err(resp) => {
                let failed = build_transfer_scan_event(
                    &assignment,
                    next_event_seq_no,
                    FluxonFsTransferScanEventKindWire::Failed,
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    transfer_rpc_response_err_text(&resp),
                );
                let _ = send_transfer_scan_event_with_retry(
                    api.as_ref(),
                    master_id.as_str(),
                    &mut assignment,
                    &failed,
                );
                break;
            }
        };
        let (events, continue_locally, next_seq_no_after_events) =
            build_transfer_scan_events_for_result(&assignment, next_event_seq_no, result);
        next_event_seq_no = next_seq_no_after_events;
        let mut should_continue_scan = continue_locally;
        for event in events {
            let ack = match send_transfer_scan_event_with_retry(
                api.as_ref(),
                master_id.as_str(),
                &mut assignment,
                &event,
            ) {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(
                        "transfer scan task failed to submit event: job_id={} scan_unit_id={} scan_task_id={} event_kind={:?} event_seq_no={} err={}",
                        assignment.job_id,
                        assignment.scan_unit_id,
                        assignment.scan_task_id,
                        event.event_kind,
                        event.event_seq_no,
                        err,
                    );
                    should_continue_scan = false;
                    break;
                }
            };
            if !ack.continue_running {
                should_continue_scan = false;
                break;
            }
        }
        if !should_continue_scan {
            break;
        }
    }
    registry.finish_task(
        assignment.scan_task_id.as_str(),
        transfer_scan_task_dedup_expire_unix_ms(assignment.lease_expire_unix_ms),
    );
}

impl TransferScanRegistryHandle {
    pub(super) fn new() -> Self {
        Self::default()
    }

    fn prune_expired_completed_tasks_locked(
        state: &mut TransferScanRegistryState,
        now_unix_ms: i64,
    ) {
        state.tasks.retain(|_, entry| {
            entry.state == TransferScanRegistryTaskState::Running
                || entry.dedup_expire_unix_ms > now_unix_ms
        });
    }

    fn finish_task(&self, scan_task_id: &str, dedup_expire_unix_ms: i64) {
        let mut state = self.state.lock();
        state.tasks.insert(
            scan_task_id.to_string(),
            TransferScanRegistryEntry {
                state: TransferScanRegistryTaskState::Completed,
                dedup_expire_unix_ms,
            },
        );
    }

    fn launch_task(
        &self,
        api: Arc<FluxonUserApi>,
        master_id: &str,
        exports: &AgentExportsHandle,
        assignment: FluxonFsTransferScanAssignmentWire,
    ) -> Result<FluxonFsTransferScanLaunchResultWire, FlatDict> {
        let now_unix_ms = chrono::Utc::now().timestamp_millis();
        {
            let mut state = self.state.lock();
            Self::prune_expired_completed_tasks_locked(&mut state, now_unix_ms);
            if let Some(existing) = state.tasks.get(assignment.scan_task_id.as_str()) {
                return Ok(match existing.state {
                    TransferScanRegistryTaskState::Running => {
                        FluxonFsTransferScanLaunchResultWire::already_running()
                    }
                    TransferScanRegistryTaskState::Completed => {
                        FluxonFsTransferScanLaunchResultWire::already_completed()
                    }
                });
            }
            state.tasks.insert(
                assignment.scan_task_id.clone(),
                TransferScanRegistryEntry {
                    state: TransferScanRegistryTaskState::Running,
                    dedup_expire_unix_ms: transfer_scan_task_dedup_expire_unix_ms(
                        assignment.lease_expire_unix_ms,
                    ),
                },
            );
        }
        let registry = self.clone();
        let api2 = api.clone();
        let master_id2 = master_id.to_string();
        let exports2 = exports.clone();
        let assignment2 = assignment.clone();
        let thread_name = format!("fluxon_fs_transfer_scan_{}", assignment.scan_task_id);
        match thread::Builder::new().name(thread_name).spawn(move || {
            run_transfer_scan_background_task(
                registry,
                api2,
                master_id2,
                exports2,
                assignment2,
            );
        }) {
            Ok(_) => Ok(FluxonFsTransferScanLaunchResultWire::started()),
            Err(err) => {
                self.state.lock().tasks.remove(assignment.scan_task_id.as_str());
                Err(resp_err_kverr(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "spawn transfer scan thread failed: scan_task_id={} err={}",
                        assignment.scan_task_id, err
                    ),
                })))
            }
        }
    }
}

impl TransferWorkerRegistryHandle {
    pub(super) fn new() -> Self {
        Self::default()
    }

    fn prune_expired_completed_tasks_locked(
        state: &mut TransferWorkerRegistryState,
        now_unix_ms: i64,
    ) {
        state.tasks.retain(|_, entry| {
            entry.state == TransferWorkerRegistryTaskState::Running
                || entry.dedup_expire_unix_ms > now_unix_ms
        });
    }

    fn finish_task(&self, worker_task_id: &str, dedup_expire_unix_ms: i64) {
        let mut state = self.state.lock();
        state.tasks.insert(
            worker_task_id.to_string(),
            TransferWorkerRegistryEntry {
                state: TransferWorkerRegistryTaskState::Completed,
                dedup_expire_unix_ms,
            },
        );
    }

    fn launch_task(
        &self,
        api: Arc<FluxonUserApi>,
        master_id: &str,
        exports: &AgentExportsHandle,
        assignment: FluxonFsTransferWorkerAssignmentWire,
    ) -> Result<FluxonFsTransferWorkerLaunchResultWire, FlatDict> {
        let now_unix_ms = chrono::Utc::now().timestamp_millis();
        {
            let mut state = self.state.lock();
            Self::prune_expired_completed_tasks_locked(&mut state, now_unix_ms);
            if let Some(existing) = state.tasks.get(assignment.worker_task_id.as_str()) {
                return Ok(match existing.state {
                    TransferWorkerRegistryTaskState::Running => {
                        FluxonFsTransferWorkerLaunchResultWire::already_running()
                    }
                    TransferWorkerRegistryTaskState::Completed => {
                        FluxonFsTransferWorkerLaunchResultWire::already_completed()
                    }
                });
            }
            state.tasks.insert(
                assignment.worker_task_id.clone(),
                TransferWorkerRegistryEntry {
                    state: TransferWorkerRegistryTaskState::Running,
                    dedup_expire_unix_ms: transfer_worker_task_dedup_expire_unix_ms(
                        assignment.lease_expire_unix_ms,
                    ),
                },
            );
        }
        let registry = self.clone();
        let api2 = api.clone();
        let master_id2 = master_id.to_string();
        let exports2 = exports.clone();
        let assignment2 = assignment.clone();
        let thread_name = format!(
            "fluxon_fs_transfer_worker_{}",
            assignment.worker_task_id
        );
        match thread::Builder::new().name(thread_name).spawn(move || {
            run_transfer_worker_background_task(
                registry,
                api2,
                master_id2,
                exports2,
                assignment2,
            );
        }) {
            Ok(_) => Ok(FluxonFsTransferWorkerLaunchResultWire::started()),
            Err(err) => {
                self.state.lock().tasks.remove(assignment.worker_task_id.as_str());
                Err(resp_err_kverr(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "spawn transfer worker thread failed: worker_task_id={} err={}",
                        assignment.worker_task_id, err
                    ),
                })))
            }
        }
    }
}

fn retry_after_target_path_chmod<T, F>(
    repair_anchor: &Path,
    op: &str,
    target_path: &Path,
    mut attempt: F,
) -> Result<T, FlatDict>
where
    F: FnMut() -> Result<T, std::io::Error>,
{
    match attempt() {
        Ok(value) => Ok(value),
        Err(initial_err) if initial_err.kind() == ErrorKind::PermissionDenied => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;

                let repair_path = match fs::symlink_metadata(target_path) {
                    Ok(md) if md.file_type().is_symlink() => nearest_existing_dir(repair_anchor)
                        .map_err(|locate_err| {
                            resp_err_kverr(KvError::Api(ApiError::Unknown {
                                detail: format!(
                                    "locate existing directory for chmod retry failed: op={} target_path={} repair_anchor={} initial_err={} locate_err={}",
                                    op,
                                    target_path.display(),
                                    repair_anchor.display(),
                                    initial_err,
                                    locate_err
                                ),
                            }))
                        })?,
                    Ok(_) => target_path.to_path_buf(),
                    Err(err) if err.kind() == ErrorKind::PermissionDenied => {
                        nearest_existing_dir(repair_anchor).map_err(|locate_err| {
                            resp_err_kverr(KvError::Api(ApiError::Unknown {
                                detail: format!(
                                    "locate existing directory for chmod retry failed: op={} target_path={} repair_anchor={} initial_err={} locate_err={}",
                                    op,
                                    target_path.display(),
                                    repair_anchor.display(),
                                    initial_err,
                                    locate_err
                                ),
                            }))
                        })?
                    }
                    Err(err) => {
                        return Err(resp_err_kverr(KvError::Api(ApiError::Unknown {
                            detail: format!(
                                "inspect target path for chmod retry failed: op={} target_path={} initial_err={} inspect_err={}",
                                op,
                                target_path.display(),
                                initial_err,
                                err
                            ),
                        })));
                    }
                };
                fs::set_permissions(&repair_path, fs::Permissions::from_mode(0o777)).map_err(|chmod_err| {
                    resp_err_kverr(KvError::Api(ApiError::Unknown {
                        detail: format!(
                            "chmod 777 target path for retry failed: op={} target_path={} repair_path={} initial_err={} chmod_err={}",
                            op,
                            target_path.display(),
                            repair_path.display(),
                            initial_err,
                            chmod_err
                        ),
                    }))
                })?;
                tracing::warn!(
                    "transfer permission retry repair applied: op={} target_path={} repair_path={} initial_err={}",
                    op,
                    target_path.display(),
                    repair_path.display(),
                    initial_err
                );
                attempt().map_err(|retry_err| {
                    resp_err_kverr(KvError::Api(ApiError::Unknown {
                        detail: format!(
                            "retry after chmod 777 failed: op={} target_path={} repair_path={} initial_err={} retry_err={}",
                            op,
                            target_path.display(),
                            repair_path.display(),
                            initial_err,
                            retry_err
                        ),
                    }))
                })
            }
            #[cfg(not(unix))]
            {
                let _ = repair_anchor;
                let _ = op;
                Err(resp_err_io(std::io::Error::new(
                    initial_err.kind(),
                    format!(
                        "permission denied and chmod retry unsupported on non-unix: target_path={} err={}",
                        target_path.display(),
                        initial_err
                    ),
                )))
            }
        }
        Err(err) => Err(resp_err_io(err)),
    }
}

fn retry_transfer_worker_rpc_with_backoff<T, F>(
    assignment: &FluxonFsTransferWorkerAssignmentWire,
    op_name: &str,
    op_detail: &str,
    backoff: BackoffConfig,
    warn_cfg: WarnConfig,
    mut op: F,
) -> Result<T, FlatDict>
where
    F: FnMut() -> Result<T, TransferWorkerRpcFailure>,
{
    let mut last_warn: Option<Instant> = None;
    let mut attempt: u32 = 0;
    loop {
        match op() {
            Ok(v) => return Ok(v),
            Err(TransferWorkerRpcFailure::Fatal(resp)) => return Err(resp),
            Err(TransferWorkerRpcFailure::Retryable { detail }) => {
                let now = Instant::now();
                if should_warn(now, &mut last_warn, warn_cfg) {
                    tracing::warn!(
                        "transfer worker rpc retry: op={} job_id={} batch_id={} worker_id={} worker_task_id={} detail={} err={}",
                        op_name,
                        assignment.job_id,
                        assignment.batch_id,
                        assignment.worker_id,
                        assignment.worker_task_id,
                        op_detail,
                        detail
                    );
                }
                let delay = next_backoff(backoff, attempt);
                attempt = attempt.saturating_add(1);
                std::thread::sleep(delay);
            }
        }
    }
}

fn retry_transfer_worker_rpc_forever_with_control<T, BeforeAttemptFn, OpFn>(
    assignment: &FluxonFsTransferWorkerAssignmentWire,
    op_name: &str,
    op_detail: &str,
    mut before_attempt: BeforeAttemptFn,
    mut op: OpFn,
) -> Result<T, TransferWorkerExecutionError>
where
    BeforeAttemptFn: FnMut() -> Result<(), TransferWorkerExecutionError>,
    OpFn: FnMut() -> Result<T, TransferWorkerRpcFailure>,
{
    // Default worker semantics are retry-forever for transport failures, but
    // every retry gate still checks whether the master has superseded the
    // worker attempt and asked it to stop.
    let mut last_warn: Option<Instant> = None;
    let mut attempt: u32 = 0;
    loop {
        before_attempt()?;
        match op() {
            Ok(v) => return Ok(v),
            Err(TransferWorkerRpcFailure::Fatal(resp)) => {
                return Err(TransferWorkerExecutionError::fatal(resp));
            }
            Err(TransferWorkerRpcFailure::Retryable { detail }) => {
                let now = Instant::now();
                if should_warn(now, &mut last_warn, TRANSFER_WORKER_RPC_RETRY_WARN) {
                    tracing::warn!(
                        "transfer worker rpc retry: op={} job_id={} batch_id={} worker_id={} worker_task_id={} detail={} err={}",
                        op_name,
                        assignment.job_id,
                        assignment.batch_id,
                        assignment.worker_id,
                        assignment.worker_task_id,
                        op_detail,
                        detail
                    );
                }
                let delay = next_backoff(TRANSFER_WORKER_RPC_RETRY_BACKOFF, attempt);
                attempt = attempt.saturating_add(1);
                std::thread::sleep(delay);
            }
        }
    }
}

fn send_transfer_worker_heartbeat_once(
    api: &FluxonUserApi,
    master_id: &str,
    assignment: &FluxonFsTransferWorkerAssignmentWire,
    heartbeat_unix_ms: i64,
    telemetry: Option<FluxonFsTransferWorkerHeartbeatTelemetryWire>,
) -> Result<FluxonFsTransferWorkerHeartbeatResultWire, TransferWorkerRpcFailure> {
    if master_id.trim().is_empty() {
        return Err(invalid_transfer_rpc_response(
            "transfer worker heartbeat requires non-empty master_id",
        ));
    }
    let heartbeat = FluxonFsTransferWorkerHeartbeatWire {
        job_id: assignment.job_id.clone(),
        worker_id: assignment.worker_id.clone(),
        assigned_batch_id: assignment.batch_id.clone(),
        worker_task_id: assignment.worker_task_id.clone(),
        heartbeat_unix_ms,
        telemetry,
    };
    let heartbeat_json = serde_json::to_string(&heartbeat).map_err(|e| {
        TransferWorkerRpcFailure::Fatal(resp_err_kverr(KvError::Api(ApiError::Unknown {
            detail: format!("serialize transfer worker heartbeat failed: {}", e),
        })))
    })?;
    let payload = FlatDict::from([(
        "heartbeat_json".to_string(),
        FlatValue::String(heartbeat_json),
    )]);
    let resp = api
        .rpc_client()
        .call(
            master_id,
            FS_MASTER_TRANSFER_SCHEDULER_HEARTBEAT_RPC_PATH,
            payload,
            Some(TRANSFER_WORKER_COORDINATION_RPC_TIMEOUT_MS),
        )
        .map_err(|e| TransferWorkerRpcFailure::Retryable {
            detail: e.to_string(),
        })?;
    let heartbeat_result_json = match resp.get("heartbeat_result_json") {
        Some(FlatValue::String(v)) if !v.trim().is_empty() => v.clone(),
        _ => {
            return Err(invalid_transfer_rpc_response(format!(
                "transfer worker heartbeat response missing heartbeat_result_json: worker_task_id={} err={}",
                assignment.worker_task_id,
                transfer_rpc_response_err_text(&resp)
            )));
        }
    };
    serde_json::from_str(&heartbeat_result_json).map_err(|e| {
        invalid_transfer_rpc_response(format!(
            "parse transfer worker heartbeat result failed: worker_task_id={} err={}",
            assignment.worker_task_id, e
        ))
    })
}

fn send_transfer_worker_result_once(
    api: &FluxonUserApi,
    master_id: &str,
    assignment: &FluxonFsTransferWorkerAssignmentWire,
    result: &FluxonFsTransferWorkerResultWire,
) -> Result<FluxonFsTransferWorkerResultAckWire, TransferWorkerRpcFailure> {
    if master_id.trim().is_empty() {
        return Err(invalid_transfer_rpc_response(
            "transfer worker result submit requires non-empty master_id",
        ));
    }
    let result_json = serde_json::to_string(result).map_err(|e| {
        TransferWorkerRpcFailure::Fatal(resp_err_kverr(KvError::Api(ApiError::Unknown {
            detail: format!("serialize transfer worker result failed: {}", e),
        })))
    })?;
    let payload = FlatDict::from([(
        "result_json".to_string(),
        FlatValue::String(result_json),
    )]);
    let resp = api
        .rpc_client()
        .call(
            master_id,
            FS_MASTER_TRANSFER_SCHEDULER_RESULT_RPC_PATH,
            payload,
            Some(TRANSFER_WORKER_COORDINATION_RPC_TIMEOUT_MS),
        )
        .map_err(|e| TransferWorkerRpcFailure::Retryable {
            detail: e.to_string(),
        })?;
    let result_ack_json = match resp.get("result_ack_json") {
        Some(FlatValue::String(v)) if !v.trim().is_empty() => v.clone(),
        _ => {
            return Err(invalid_transfer_rpc_response(format!(
                "transfer worker result response missing result_ack_json: worker_task_id={} err={}",
                assignment.worker_task_id,
                transfer_rpc_response_err_text(&resp)
            )));
        }
    };
    serde_json::from_str(&result_ack_json).map_err(|e| {
        invalid_transfer_rpc_response(format!(
            "parse transfer worker result ack failed: worker_task_id={} err={}",
            assignment.worker_task_id, e
        ))
    })
}

fn report_transfer_worker_fatal_once(
    api: &FluxonUserApi,
    master_id: &str,
    assignment: &FluxonFsTransferWorkerAssignmentWire,
    fatal_kind: &str,
    fatal_message: &str,
) -> Result<(), TransferWorkerRpcFailure> {
    if master_id.trim().is_empty() {
        return Err(invalid_transfer_rpc_response(
            "transfer worker fatal report requires non-empty master_id",
        ));
    }
    let payload_json = serde_json::json!({
        "job_id": assignment.job_id,
        "batch_id": assignment.batch_id,
        "worker_id": assignment.worker_id,
        "worker_task_id": assignment.worker_task_id,
        "fatal_kind": fatal_kind,
        "fatal_message": fatal_message,
    })
    .to_string();
    let payload = FlatDict::from([(
        "worker_fatal_json".to_string(),
        FlatValue::String(payload_json),
    )]);
    let resp = api
        .rpc_client()
        .call(
            master_id,
            FS_MASTER_TRANSFER_SCHEDULER_RESULT_RPC_PATH,
            payload,
            Some(TRANSFER_WORKER_COORDINATION_RPC_TIMEOUT_MS),
        )
        .map_err(|e| TransferWorkerRpcFailure::Retryable {
            detail: e.to_string(),
        })?;
    if transfer_rpc_response_ok(&resp) {
        return Ok(());
    }
    Err(invalid_transfer_rpc_response(format!(
        "transfer worker fatal report response not ok: worker_task_id={} err={}",
        assignment.worker_task_id,
        transfer_rpc_response_err_text(&resp)
    )))
}

fn open_transfer_read_stream_via_rpc_once(
    api: &FluxonUserApi,
    assignment: &FluxonFsTransferWorkerAssignmentWire,
    file: &FluxonFsTransferManifestEntryWire,
    initial_offset: i64,
) -> Result<TransferReadStreamHandle, TransferWorkerRpcFailure> {
    let payload = FlatDict::from([
        (
            "worker_task_id".to_string(),
            FlatValue::String(assignment.worker_task_id.clone()),
        ),
        (
            "export".to_string(),
            FlatValue::String(assignment.src_export.clone()),
        ),
        (
            "relpath".to_string(),
            FlatValue::String(file.relpath.clone()),
        ),
        ("initial_offset".to_string(), FlatValue::Int64(initial_offset)),
    ]);
    let resp = api
        .rpc_client()
        .call(
            assignment.src_exporter_id.as_str(),
            FS_AGENT_TRANSFER_STREAM_OPEN_RPC_PATH,
            payload,
            Some(TRANSFER_STREAM_RPC_TIMEOUT_MS),
        )
        .map_err(|e| TransferWorkerRpcFailure::Retryable {
            detail: e.to_string(),
        })?;
    let result = decode_transfer_stream_open_result_payload(&resp).map_err(|err| match err {
        TransferWorkerRpcFailure::Retryable { detail } => {
            invalid_transfer_rpc_response(format!(
                "transfer read stream open retryable decode failure unexpectedly escaped: relpath={} err={}",
                file.relpath, detail
            ))
        }
        fatal => fatal,
    })?;
    Ok(TransferReadStreamHandle {
        stream_id: result.stream_id,
        file_size: result.size,
    })
}

fn next_transfer_read_stream_via_rpc_once(
    api: &FluxonUserApi,
    assignment: &FluxonFsTransferWorkerAssignmentWire,
    file: &FluxonFsTransferManifestEntryWire,
    stream_id: &str,
    next_offset: i64,
    length: i64,
) -> Result<FluxonFsTransferReadStreamNextResultWire, TransferWorkerRpcFailure> {
    let payload = FlatDict::from([
        (
            "stream_id".to_string(),
            FlatValue::String(stream_id.to_string()),
        ),
        ("next_offset".to_string(), FlatValue::Int64(next_offset)),
        ("length".to_string(), FlatValue::Int64(length)),
    ]);
    let resp = api
        .rpc_client()
        .call(
            assignment.src_exporter_id.as_str(),
            FS_AGENT_TRANSFER_STREAM_NEXT_RPC_PATH,
            payload,
            Some(TRANSFER_STREAM_RPC_TIMEOUT_MS),
        )
        .map_err(|e| TransferWorkerRpcFailure::Retryable {
            detail: e.to_string(),
        })?;
    decode_transfer_stream_next_result_payload(&resp).map_err(|err| match err {
        TransferWorkerRpcFailure::Retryable { detail } => invalid_transfer_rpc_response(format!(
            "transfer read stream next retryable decode failure unexpectedly escaped: relpath={} offset={} err={}",
            file.relpath, next_offset, detail
        )),
        fatal => fatal,
    })
}

fn close_transfer_read_stream_via_rpc_once(
    api: &FluxonUserApi,
    assignment: &FluxonFsTransferWorkerAssignmentWire,
    stream_id: &str,
) -> Result<(), TransferWorkerRpcFailure> {
    let payload = FlatDict::from([(
        "stream_id".to_string(),
        FlatValue::String(stream_id.to_string()),
    )]);
    let resp = api
        .rpc_client()
        .call(
            assignment.src_exporter_id.as_str(),
            FS_AGENT_TRANSFER_STREAM_CLOSE_RPC_PATH,
            payload,
            Some(TRANSFER_STREAM_RPC_TIMEOUT_MS),
        )
        .map_err(|e| TransferWorkerRpcFailure::Retryable {
            detail: e.to_string(),
        })?;
    if !transfer_rpc_response_ok(&resp) {
        return Err(TransferWorkerRpcFailure::Fatal(resp));
    }
    Ok(())
}

impl TransferWorkerRemoteControl {
    fn new(
        api: Arc<FluxonUserApi>,
        master_id: String,
        assignment: FluxonFsTransferWorkerAssignmentWire,
        progress: Arc<TransferWorkerProgressWindow>,
    ) -> Self {
        let granted_lease_expire_unix_ms = assignment.lease_expire_unix_ms;
        Self {
            api,
            master_id,
            assignment,
            heartbeat: TransferWorkerHeartbeatGate::new(granted_lease_expire_unix_ms),
            open_streams: Mutex::new(BTreeMap::new()),
            progress,
        }
    }

    fn dedup_expire_unix_ms(&self) -> i64 {
        self.heartbeat.dedup_expire_unix_ms()
    }

    fn before_heartbeat_retry_attempt(&self) -> Result<(), TransferWorkerExecutionError> {
        self.heartbeat.before_heartbeat_retry_attempt()
    }

    fn ensure_continue(&self, force: bool) -> Result<(), TransferWorkerExecutionError> {
        let mut last_warn: Option<Instant> = None;
        let mut attempt: u32 = 0;
        loop {
            self.before_heartbeat_retry_attempt()?;
            let current_materialized_empty_dirs = self.progress.total_materialized_empty_dirs();
            match self
                .heartbeat
                .ensure_continue(
                    force,
                    current_materialized_empty_dirs,
                    |heartbeat_unix_ms, _heartbeat_detail| {
                        let progress_snapshot =
                            self.progress.snapshot(chrono::Utc::now().timestamp_millis());
                        let telemetry =
                            Some(transfer_worker_telemetry_from_progress_snapshot(&progress_snapshot));
                        send_transfer_worker_heartbeat_once(
                            self.api.as_ref(),
                            self.master_id.as_str(),
                            &self.assignment,
                            heartbeat_unix_ms,
                            telemetry,
                        )
                    },
                ) {
                Ok(()) => return Ok(()),
                Err(TransferWorkerHeartbeatGateError::Terminal(err)) => return Err(err),
                Err(TransferWorkerHeartbeatGateError::Retryable {
                    heartbeat_detail,
                    detail,
                }) => {
                    let now = Instant::now();
                    if should_warn(now, &mut last_warn, TRANSFER_WORKER_RPC_RETRY_WARN) {
                        tracing::warn!(
                            "transfer worker rpc retry: op={} job_id={} batch_id={} worker_id={} worker_task_id={} detail={} err={}",
                            "heartbeat",
                            self.assignment.job_id,
                            self.assignment.batch_id,
                            self.assignment.worker_id,
                            self.assignment.worker_task_id,
                            heartbeat_detail,
                            detail
                        );
                    }
                    let delay = next_backoff(TRANSFER_WORKER_RPC_RETRY_BACKOFF, attempt);
                    attempt = attempt.saturating_add(1);
                    std::thread::sleep(delay);
                }
            }
        }
    }

    fn read_chunk_with_retry(
        &self,
        file: &FluxonFsTransferManifestEntryWire,
        read_offset: i64,
        length: i64,
    ) -> Result<Vec<u8>, TransferWorkerExecutionError> {
        self.read_chunk_via_stream_with_retry(file, read_offset, length)
    }

    fn open_stream_with_retry(
        &self,
        file: &FluxonFsTransferManifestEntryWire,
        initial_offset: i64,
    ) -> Result<TransferReadStreamHandle, TransferWorkerExecutionError> {
        let api = self.api.clone();
        let assignment = self.assignment.clone();
        let op_detail = format!(
            "src_exporter_id={} relpath={} initial_offset={}",
            assignment.src_exporter_id, file.relpath, initial_offset
        );
        retry_transfer_worker_rpc_forever_with_control(
            &assignment,
            "open_stream",
            op_detail.as_str(),
            || self.ensure_continue(false),
            || {
                open_transfer_read_stream_via_rpc_once(
                    api.as_ref(),
                    &assignment,
                    file,
                    initial_offset,
                )
            },
        )
    }

    fn next_stream_chunk_with_retry(
        &self,
        file: &FluxonFsTransferManifestEntryWire,
        stream_id: &str,
        next_offset: i64,
        length: i64,
    ) -> Result<FluxonFsTransferReadStreamNextResultWire, TransferWorkerExecutionError> {
        let api = self.api.clone();
        let assignment = self.assignment.clone();
        let op_detail = format!(
            "src_exporter_id={} relpath={} stream_id={} offset={} length={}",
            assignment.src_exporter_id, file.relpath, stream_id, next_offset, length
        );
        retry_transfer_worker_rpc_forever_with_control(
            &assignment,
            "next_stream_chunk",
            op_detail.as_str(),
            || self.ensure_continue(false),
            || {
                next_transfer_read_stream_via_rpc_once(
                    api.as_ref(),
                    &assignment,
                    file,
                    stream_id,
                    next_offset,
                    length,
                )
            },
        )
    }

    fn close_stream_with_retry(
        &self,
        stream_id: &str,
    ) -> Result<(), TransferWorkerExecutionError> {
        let api = self.api.clone();
        let assignment = self.assignment.clone();
        let op_detail = format!(
            "src_exporter_id={} stream_id={}",
            assignment.src_exporter_id, stream_id
        );
        retry_transfer_worker_rpc_forever_with_control(
            &assignment,
            "close_stream",
            op_detail.as_str(),
            || self.ensure_continue(false),
            || close_transfer_read_stream_via_rpc_once(api.as_ref(), &assignment, stream_id),
        )
    }

    fn read_chunk_via_stream_with_retry(
        &self,
        file: &FluxonFsTransferManifestEntryWire,
        read_offset: i64,
        length: i64,
    ) -> Result<Vec<u8>, TransferWorkerExecutionError> {
        loop {
            let existing = self.open_streams.lock().get(file.relpath.as_str()).cloned();
            let stream = match existing {
                Some(existing) => existing,
                None => {
                    let opened = self.open_stream_with_retry(file, read_offset)?;
                    if opened.file_size != file.size {
                        return Err(TransferWorkerExecutionError::fatal(resp_err_kverr(
                            KvError::Api(ApiError::InvalidArgument {
                                detail: format!(
                                    "transfer source file size changed during worker execution: relpath={} expected={} actual={}",
                                    file.relpath, file.size, opened.file_size
                                ),
                            }),
                        )));
                    }
                    let mut open_streams = self.open_streams.lock();
                    if let Some(existing) = open_streams.get(file.relpath.as_str()) {
                        existing.clone()
                    } else {
                        open_streams.insert(file.relpath.clone(), opened.clone());
                        opened
                    }
                }
            };
            let result = self.next_stream_chunk_with_retry(
                file,
                stream.stream_id.as_str(),
                read_offset,
                length,
            )?;
            if result.stream_missing {
                self.open_streams.lock().remove(file.relpath.as_str());
                continue;
            }
            if read_offset.saturating_add(result.data.len() as i64) >= file.size {
                self.close_file_stream(file.relpath.as_str())?;
            }
            return Ok(result.data);
        }
    }

    fn close_file_stream(&self, relpath: &str) -> Result<(), TransferWorkerExecutionError> {
        let stream = self.open_streams.lock().get(relpath).cloned();
        let Some(stream) = stream else {
            return Ok(());
        };
        self.close_stream_with_retry(stream.stream_id.as_str())?;
        self.open_streams.lock().remove(relpath);
        Ok(())
    }

    fn close_all_streams(&self) {
        let streams: Vec<TransferReadStreamHandle> = {
            let mut open_streams = self.open_streams.lock();
            let drained = open_streams.values().cloned().collect();
            open_streams.clear();
            drained
        };
        for stream in streams {
            let _ = self.close_stream_with_retry(stream.stream_id.as_str());
        }
    }

    fn submit_result_with_retry(
        &self,
        result: &FluxonFsTransferWorkerResultWire,
    ) -> Result<(), TransferWorkerExecutionError> {
        let api = self.api.clone();
        let master_id = self.master_id.clone();
        let assignment = self.assignment.clone();
        let ack = retry_transfer_worker_rpc_forever_with_control(
            &assignment,
            "submit_result",
            "final",
            || self.ensure_continue(false),
            || {
                send_transfer_worker_result_once(
                    api.as_ref(),
                    master_id.as_str(),
                    &assignment,
                    result,
                )
            },
        )?;
        if ack.accepted {
            return Ok(());
        }
        Err(TransferWorkerExecutionError::Stop(stop_reason_or_superseded(
            ack.stop_reason,
        )))
    }
}

impl TransferWorkerRemoteControlTerminalState {
    fn to_execution_error(&self) -> TransferWorkerExecutionError {
        match self {
            Self::Stop(reason) => TransferWorkerExecutionError::Stop(*reason),
            Self::Fatal(resp) => TransferWorkerExecutionError::Fatal(resp.clone()),
        }
    }
}

impl TransferWorkerHeartbeatGate {
    fn new(granted_lease_expire_unix_ms: i64) -> Self {
        Self {
            state: Mutex::new(TransferWorkerHeartbeatGateState {
                last_heartbeat_completed_unix_ms: 0,
                last_heartbeat_materialized_empty_dirs: 0,
                granted_lease_expire_unix_ms,
                heartbeat_inflight: false,
                terminal_state: None,
            }),
            heartbeat_cv: Condvar::new(),
        }
    }

    fn dedup_expire_unix_ms(&self) -> i64 {
        let state = self.state.lock();
        transfer_worker_task_dedup_expire_unix_ms(
            state
                .granted_lease_expire_unix_ms
                .max(chrono::Utc::now().timestamp_millis()),
        )
    }

    fn terminal_error_locked(
        state: &TransferWorkerHeartbeatGateState,
    ) -> Option<TransferWorkerExecutionError> {
        state
            .terminal_state
            .as_ref()
            .map(TransferWorkerRemoteControlTerminalState::to_execution_error)
    }

    fn cache_terminal_error_locked(
        state: &mut TransferWorkerHeartbeatGateState,
        err: &TransferWorkerExecutionError,
    ) {
        state.terminal_state = Some(match err {
            TransferWorkerExecutionError::Stop(reason) => {
                TransferWorkerRemoteControlTerminalState::Stop(*reason)
            }
            TransferWorkerExecutionError::Fatal(resp) => {
                TransferWorkerRemoteControlTerminalState::Fatal(resp.clone())
            }
        });
    }

    fn before_heartbeat_retry_attempt(&self) -> Result<(), TransferWorkerExecutionError> {
        let state = self.state.lock();
        if let Some(err) = Self::terminal_error_locked(&state) {
            return Err(err);
        }
        Ok(())
    }

    // A worker may have many local file lanes, but the master lease still has
    // only one authority stream. This gate serializes one heartbeat RPC
    // attempt per worker_task_id. Retry/backoff must happen outside the gate so
    // a transient timeout cannot hold sibling lanes behind a stale inflight
    // flag for the entire retry lifetime.
    fn ensure_continue<HeartbeatOp>(
        &self,
        force: bool,
        current_materialized_empty_dirs: i64,
        mut heartbeat_op: HeartbeatOp,
    ) -> Result<(), TransferWorkerHeartbeatGateError>
    where
        HeartbeatOp: FnMut(
            i64,
            &'static str,
        ) -> Result<FluxonFsTransferWorkerHeartbeatResultWire, TransferWorkerRpcFailure>,
    {
        loop {
            let (heartbeat_unix_ms, heartbeat_detail) = {
                let now_unix_ms = chrono::Utc::now().timestamp_millis();
                let mut state = self.state.lock();
                if let Some(err) = Self::terminal_error_locked(&state) {
                    return Err(TransferWorkerHeartbeatGateError::Terminal(err));
                }
                let first_heartbeat_missing = state.last_heartbeat_completed_unix_ms == 0;
                let heartbeat_due = first_heartbeat_missing
                    || now_unix_ms - state.last_heartbeat_completed_unix_ms
                        >= TRANSFER_WORKER_TELEMETRY_HEARTBEAT_INTERVAL.as_millis() as i64;
                let empty_dir_progress_due = current_materialized_empty_dirs
                    .saturating_sub(state.last_heartbeat_materialized_empty_dirs)
                    >= TRANSFER_WORKER_HEARTBEAT_EMPTY_DIR_PROGRESS_COUNT;
                let lease_expired = now_unix_ms >= state.granted_lease_expire_unix_ms;
                let should_send = if force {
                    first_heartbeat_missing || lease_expired
                } else {
                    heartbeat_due || empty_dir_progress_due || lease_expired
                };
                if !should_send {
                    return Ok(());
                }
                if state.heartbeat_inflight {
                    self.heartbeat_cv.wait(&mut state);
                    continue;
                }
                let heartbeat_detail = if state.last_heartbeat_completed_unix_ms == 0 {
                    "initial"
                } else if lease_expired {
                    "lease_refresh"
                } else if empty_dir_progress_due {
                    "empty_dir_progress"
                } else {
                    "periodic"
                };
                state.heartbeat_inflight = true;
                (now_unix_ms, heartbeat_detail)
            };

            let heartbeat_result = heartbeat_op(heartbeat_unix_ms, heartbeat_detail);
            let mut state = self.state.lock();
            state.heartbeat_inflight = false;
            let result = match heartbeat_result {
                Ok(heartbeat_result) if heartbeat_result.continue_running => {
                    state.last_heartbeat_completed_unix_ms =
                        chrono::Utc::now().timestamp_millis();
                    state.last_heartbeat_materialized_empty_dirs = current_materialized_empty_dirs;
                    state.granted_lease_expire_unix_ms = heartbeat_result.lease_expire_unix_ms;
                    Ok(())
                }
                Ok(heartbeat_result) => {
                    state.last_heartbeat_completed_unix_ms =
                        chrono::Utc::now().timestamp_millis();
                    state.last_heartbeat_materialized_empty_dirs = current_materialized_empty_dirs;
                    let reason = stop_reason_or_superseded(heartbeat_result.stop_reason);
                    state.terminal_state =
                        Some(TransferWorkerRemoteControlTerminalState::Stop(reason));
                    Err(TransferWorkerHeartbeatGateError::Terminal(
                        TransferWorkerExecutionError::Stop(reason),
                    ))
                }
                Err(TransferWorkerRpcFailure::Retryable { detail }) => {
                    Err(TransferWorkerHeartbeatGateError::Retryable {
                        heartbeat_detail,
                        detail,
                    })
                }
                Err(TransferWorkerRpcFailure::Fatal(resp)) => {
                    let err = TransferWorkerExecutionError::fatal(resp);
                    Self::cache_terminal_error_locked(&mut state, &err);
                    Err(TransferWorkerHeartbeatGateError::Terminal(err))
                }
            };
            self.heartbeat_cv.notify_all();
            return result;
        }
    }
}

// Worker execution is detached from the launch RPC. The registry keeps a short
// dedupe window after completion so repeated launch RPCs for the same attempt
// can respond with AlreadyCompleted instead of spawning again.
fn run_transfer_worker_background_task(
    registry: TransferWorkerRegistryHandle,
    api: Arc<FluxonUserApi>,
    master_id: String,
    exports: AgentExportsHandle,
    assignment: FluxonFsTransferWorkerAssignmentWire,
) {
    let worker_task_id = assignment.worker_task_id.clone();
    let progress = Arc::new(TransferWorkerProgressWindow::new(
        Arc::new(TransferWorkerLanePolicy::production_default().normalized()),
        chrono::Utc::now().timestamp_millis(),
    ));
    let control = Arc::new(TransferWorkerRemoteControl::new(
        api,
        master_id,
        assignment.clone(),
        progress.clone(),
    ));
    let dedup_expire_unix_ms = match control.ensure_continue(true) {
        Ok(()) => {
            let dst_export_root = match exports.export_root_dir_abs(assignment.dst_export.as_str()) {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(
                        "transfer worker destination export lookup failed: job_id={} batch_id={} worker_id={} worker_task_id={} dst_export={} dst_root_relpath={} err={}",
                        assignment.job_id,
                        assignment.batch_id,
                        assignment.worker_id,
                        assignment.worker_task_id,
                        assignment.dst_export,
                        assignment.dst_root_relpath,
                        err
                    );
                    registry.finish_task(worker_task_id.as_str(), control.dedup_expire_unix_ms());
                    return;
                }
            };
            let dst_root = match safe_join_root(
                dst_export_root.as_str(),
                assignment.dst_root_relpath.as_str(),
            ) {
                Ok(v) => PathBuf::from(v),
                Err(err) => {
                    tracing::warn!(
                        "transfer worker destination join failed: job_id={} batch_id={} worker_id={} worker_task_id={} dst_export={} dst_root_relpath={} err={}",
                        assignment.job_id,
                        assignment.batch_id,
                        assignment.worker_id,
                        assignment.worker_task_id,
                        assignment.dst_export,
                        assignment.dst_root_relpath,
                        err
                    );
                    registry.finish_task(worker_task_id.as_str(), control.dedup_expire_unix_ms());
                    return;
                }
            };
            if let Err(resp) = create_dir_all_with_parent_dir_chmod_retry(&dst_root) {
                tracing::warn!(
                    "transfer worker destination root prepare failed: job_id={} batch_id={} worker_id={} worker_task_id={} dst_export={} dst_root_relpath={} dst_root_abs={} resp={:?}",
                    assignment.job_id,
                    assignment.batch_id,
                    assignment.worker_id,
                    assignment.worker_task_id,
                    assignment.dst_export,
                    assignment.dst_root_relpath,
                    dst_root.display(),
                    resp
                );
                registry.finish_task(worker_task_id.as_str(), control.dedup_expire_unix_ms());
                return;
            }
            match execute_transfer_worker_assignment_with_policy_and_progress(
                &assignment,
                &dst_root,
                TransferWorkerLanePolicy::production_default(),
                progress,
                {
                    let control = control.clone();
                    move || control.ensure_continue(false)
                },
                {
                    let control = control.clone();
                    move |file, read_offset, length| control.read_chunk_with_retry(file, read_offset, length)
                },
            ) {
                Ok(result) => {
                    control.close_all_streams();
                    if let Err(resp) =
                        cleanup_transfer_worker_attempt_artifacts(&dst_root, &assignment)
                    {
                        log_transfer_worker_cleanup_failure("before_result_submit", &assignment, &resp);
                    }
                    match control.submit_result_with_retry(&result) {
                    Ok(()) => control.dedup_expire_unix_ms(),
                    Err(TransferWorkerExecutionError::Stop(reason)) => {
                        tracing::info!(
                            "transfer worker result submission stopped: job_id={} batch_id={} worker_id={} worker_task_id={} reason={:?}",
                            assignment.job_id,
                            assignment.batch_id,
                            assignment.worker_id,
                            assignment.worker_task_id,
                            reason
                        );
                        control.dedup_expire_unix_ms()
                    }
                    Err(TransferWorkerExecutionError::Fatal(resp)) => {
                        tracing::warn!(
                            "transfer worker result submission failed: job_id={} batch_id={} worker_id={} worker_task_id={} resp={:?}",
                            assignment.job_id,
                            assignment.batch_id,
                            assignment.worker_id,
                            assignment.worker_task_id,
                            resp
                        );
                        control.dedup_expire_unix_ms()
                    }
                }
                }
                Err(TransferWorkerExecutionError::Stop(reason)) => {
                    control.close_all_streams();
                    if let Err(resp) =
                        cleanup_transfer_worker_attempt_artifacts(&dst_root, &assignment)
                    {
                        log_transfer_worker_cleanup_failure("after_stop", &assignment, &resp);
                    }
                    tracing::info!(
                        "transfer worker stopped: job_id={} batch_id={} worker_id={} worker_task_id={} reason={:?}",
                        assignment.job_id,
                        assignment.batch_id,
                        assignment.worker_id,
                        assignment.worker_task_id,
                        reason
                    );
                    control.dedup_expire_unix_ms()
                }
                Err(TransferWorkerExecutionError::Fatal(resp)) => {
                    control.close_all_streams();
                    if let Err(cleanup_resp) =
                        cleanup_transfer_worker_attempt_artifacts(&dst_root, &assignment)
                    {
                        log_transfer_worker_cleanup_failure("after_fatal", &assignment, &cleanup_resp);
                    }
                    if let Some((fatal_kind, fatal_message)) =
                        classify_transfer_worker_fatal(&resp)
                    {
                        match report_transfer_worker_fatal_once(
                            control.api.as_ref(),
                            control.master_id.as_str(),
                            &assignment,
                            fatal_kind,
                            fatal_message.as_str(),
                        ) {
                            Ok(()) => {}
                            Err(TransferWorkerRpcFailure::Retryable { detail }) => {
                                tracing::warn!(
                                    "transfer worker fatal report retryable failure: job_id={} batch_id={} worker_id={} worker_task_id={} kind={} detail={}",
                                    assignment.job_id,
                                    assignment.batch_id,
                                    assignment.worker_id,
                                    assignment.worker_task_id,
                                    fatal_kind,
                                    detail
                                );
                            }
                            Err(TransferWorkerRpcFailure::Fatal(report_resp)) => {
                                tracing::warn!(
                                    "transfer worker fatal report fatal failure: job_id={} batch_id={} worker_id={} worker_task_id={} kind={} resp={:?}",
                                    assignment.job_id,
                                    assignment.batch_id,
                                    assignment.worker_id,
                                    assignment.worker_task_id,
                                    fatal_kind,
                                    report_resp
                                );
                            }
                        }
                    }
                    tracing::warn!(
                        "transfer worker failed: job_id={} batch_id={} worker_id={} worker_task_id={} dst_export={} dst_root_relpath={} dst_root_abs={} resp={:?}",
                        assignment.job_id,
                        assignment.batch_id,
                        assignment.worker_id,
                        assignment.worker_task_id,
                        assignment.dst_export,
                        assignment.dst_root_relpath,
                        dst_root.display(),
                        resp
                    );
                    control.dedup_expire_unix_ms()
                }
            }
        }
        Err(TransferWorkerExecutionError::Stop(reason)) => {
            let dst_export_root = exports.export_root_dir_abs(assignment.dst_export.as_str()).ok();
            let dst_root = dst_export_root.and_then(|dst_export_root| {
                safe_join_root(dst_export_root.as_str(), assignment.dst_root_relpath.as_str())
                    .ok()
                    .map(PathBuf::from)
            });
            if let Some(dst_root) = dst_root {
                if let Err(resp) = cleanup_transfer_worker_attempt_artifacts(&dst_root, &assignment) {
                    log_transfer_worker_cleanup_failure("before_execution_stop", &assignment, &resp);
                }
            }
            tracing::info!(
                "transfer worker launch stopped before execution: job_id={} batch_id={} worker_id={} worker_task_id={} reason={:?}",
                assignment.job_id,
                assignment.batch_id,
                assignment.worker_id,
                assignment.worker_task_id,
                reason
            );
            control.dedup_expire_unix_ms()
        }
        Err(TransferWorkerExecutionError::Fatal(resp)) => {
            let dst_export_root = exports.export_root_dir_abs(assignment.dst_export.as_str()).ok();
            let dst_root = dst_export_root.and_then(|dst_export_root| {
                safe_join_root(dst_export_root.as_str(), assignment.dst_root_relpath.as_str())
                    .ok()
                    .map(PathBuf::from)
            });
            if let Some(dst_root) = dst_root {
                if let Err(cleanup_resp) =
                    cleanup_transfer_worker_attempt_artifacts(&dst_root, &assignment)
                {
                    log_transfer_worker_cleanup_failure(
                        "before_execution_fatal",
                        &assignment,
                        &cleanup_resp,
                    );
                }
            }
            tracing::warn!(
                "transfer worker launch heartbeat failed: job_id={} batch_id={} worker_id={} worker_task_id={} resp={:?}",
                assignment.job_id,
                assignment.batch_id,
                assignment.worker_id,
                assignment.worker_task_id,
                resp
            );
            control.dedup_expire_unix_ms()
        }
    };
    registry.finish_task(worker_task_id.as_str(), dedup_expire_unix_ms);
}

#[cfg(test)]
fn encode_transfer_scan_result(
    result: &FluxonFsTransferScanResultWire,
    err_context: &str,
) -> FlatDict {
    let result_json = match serde_json::to_string(result) {
        Ok(v) => v,
        Err(e) => {
            return resp_err_kverr(KvError::Api(ApiError::Unknown {
                detail: format!("serialize {} failed: {}", err_context, e),
            }));
        }
    };
    resp_ok(BTreeMap::from([(
        "result_json".to_string(),
        FlatValue::String(result_json),
    )]))
}

fn encode_transfer_scan_launch_result(
    result: &FluxonFsTransferScanLaunchResultWire,
    err_context: &str,
) -> FlatDict {
    let result_json = match serde_json::to_string(result) {
        Ok(v) => v,
        Err(e) => {
            return resp_err_kverr(KvError::Api(ApiError::Unknown {
                detail: format!("serialize {} failed: {}", err_context, e),
            }));
        }
    };
    resp_ok(BTreeMap::from([(
        "launch_result_json".to_string(),
        FlatValue::String(result_json),
    )]))
}

fn encode_transfer_worker_launch_result(
    result: &FluxonFsTransferWorkerLaunchResultWire,
    err_context: &str,
) -> FlatDict {
    let result_json = match serde_json::to_string(result) {
        Ok(v) => v,
        Err(e) => {
            return resp_err_kverr(KvError::Api(ApiError::Unknown {
                detail: format!("serialize {} failed: {}", err_context, e),
            }));
        }
    };
    resp_ok(BTreeMap::from([(
        "launch_result_json".to_string(),
        FlatValue::String(result_json),
    )]))
}

#[cfg(test)]
fn build_disposition_blocked_scan_result(
    assignment: FluxonFsTransferScanAssignmentWire,
    _disposition: FluxonFsTransferDispositionWire,
) -> FlatDict {
    let result = FluxonFsTransferScanResultWire {
        job_id: assignment.job_id,
        scan_epoch: assignment.scan_epoch,
        scan_unit_id: assignment.scan_unit_id,
        scan_task_id: assignment.scan_task_id,
        root_relpath: assignment.root_relpath,
        generation: assignment.generation,
        frontier: empty_transfer_scan_frontier(),
        direct_files_only_batches: Vec::new(),
        child_scan_units: Vec::new(),
        full_dir_batches: Vec::new(),
        finished: true,
    };
    encode_transfer_scan_result(&result, "disposition-blocked scan result")
}

fn full_dir_disposition_for_assignment(
    assignment: &FluxonFsTransferScanAssignmentWire,
) -> Option<FluxonFsTransferDispositionWire> {
    // Only FullDir blocks further scan of the subtree because it already covers
    // everything below root_relpath. DirectFilesOnly never blocks child scans.
    assignment
        .known_dispositions
        .iter()
        .find(|disposition| {
            disposition.root_relpath == assignment.root_relpath
                && disposition.batch_kind == FluxonFsTransferBatchKind::FullDir
        })
        .cloned()
}

fn full_dir_disposition_covers_root(
    assignment: &FluxonFsTransferScanAssignmentWire,
    root_relpath: &str,
) -> bool {
    assignment.known_dispositions.iter().any(|disposition| {
        disposition.batch_kind == FluxonFsTransferBatchKind::FullDir
            && disposition.root_relpath == root_relpath
    })
}

fn direct_files_only_disposition_covers_root(
    assignment: &FluxonFsTransferScanAssignmentWire,
    root_relpath: &str,
) -> bool {
    if root_relpath != assignment.root_relpath {
        return false;
    }
    assignment.known_dispositions.iter().any(|disposition| {
        disposition.generation == assignment.generation
            && disposition.batch_kind == FluxonFsTransferBatchKind::DirectFilesOnly
            && disposition.root_relpath == root_relpath
    })
}

fn live_child_scan_root_blocks_root(
    assignment: &FluxonFsTransferScanAssignmentWire,
    root_relpath: &str,
) -> bool {
    assignment
        .live_child_scan_roots
        .iter()
        .any(|live_root| live_root == root_relpath)
}

fn subtree_has_any_known_partition(
    assignment: &FluxonFsTransferScanAssignmentWire,
    root_relpath: &str,
) -> bool {
    assignment.known_dispositions.iter().any(|disposition| {
        if disposition.root_relpath == root_relpath {
            return true;
        }
        if root_relpath == "." {
            return true;
        }
        disposition
            .root_relpath
            .starts_with(format!("{}/", root_relpath).as_str())
    }) || assignment.live_child_scan_roots.iter().any(|live_root| {
        if live_root == root_relpath {
            return true;
        }
        if root_relpath == "." {
            return true;
        }
        live_root.starts_with(format!("{}/", root_relpath).as_str())
    })
}

#[derive(Debug)]
struct TransferScanMaterializedSubtree {
    files: Vec<FluxonFsTransferScanFrontierEntry>,
    symlink_notices: Vec<FluxonFsTransferSymlinkNoticeEntryWire>,
    empty_dirs: Vec<String>,
    total_bytes: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransferSubtreeClosureState {
    Mergeable,
    Closed,
    Incomplete,
}

#[derive(Debug)]
struct TransferSubtreeBatchPlan {
    closure: TransferSubtreeClosureState,
    total_bytes: i64,
    root_is_empty: bool,
    mergeable_empty_dir_count: usize,
    mergeable_empty_dir_estimated_bytes: usize,
    direct_files_only_batches: Vec<FluxonFsTransferScanBatchWire>,
    full_dir_batches: Vec<FluxonFsTransferScanBatchWire>,
    child_scan_units: Vec<FluxonFsTransferScanChildUnitWire>,
}

fn materialize_transfer_subtree(
    root_dir_abs: &str,
    assignment: &FluxonFsTransferScanAssignmentWire,
    root_relpath: &str,
    deadline: Option<TransferScanDeadline>,
) -> Result<TransferScanMaterializedSubtree, FlatDict> {
    let collection = collect_transfer_tree_with_deadline(
        root_dir_abs,
        root_relpath,
        &assignment.skip_entries,
        deadline,
    )?;
    let total_bytes = collection
        .files
        .iter()
        .fold(0_i64, |acc, entry| acc.saturating_add(entry.size));
    Ok(TransferScanMaterializedSubtree {
        files: collection.files,
        symlink_notices: collection.symlink_notices,
        empty_dirs: collection.empty_dirs,
        total_bytes,
    })
}

fn plan_transfer_subtree_batches(
    root_dir_abs: &str,
    assignment: &FluxonFsTransferScanAssignmentWire,
    root_relpath: &str,
    deadline: Option<TransferScanDeadline>,
) -> Result<TransferSubtreeBatchPlan, FlatDict> {
    if full_dir_disposition_covers_root(assignment, root_relpath)
        || live_child_scan_root_blocks_root(assignment, root_relpath)
    {
        return Ok(TransferSubtreeBatchPlan {
            closure: TransferSubtreeClosureState::Closed,
            total_bytes: 0,
            root_is_empty: false,
            mergeable_empty_dir_count: 0,
            mergeable_empty_dir_estimated_bytes: 0,
            direct_files_only_batches: Vec::new(),
            full_dir_batches: Vec::new(),
            child_scan_units: Vec::new(),
        });
    }
    if deadline.is_some_and(|deadline| deadline.reached()) {
        return Ok(TransferSubtreeBatchPlan {
            closure: TransferSubtreeClosureState::Incomplete,
            total_bytes: 0,
            root_is_empty: false,
            mergeable_empty_dir_count: 0,
            mergeable_empty_dir_estimated_bytes: 0,
            direct_files_only_batches: Vec::new(),
            full_dir_batches: Vec::new(),
            child_scan_units: Vec::new(),
        });
    }
    let dir_abs = safe_join_root(root_dir_abs, root_relpath).map_err(resp_err_kverr)?;
    let rd = match retry_after_target_path_chmod(
        dir_abs.parent().unwrap_or(dir_abs.as_path()),
        "plan_read_dir",
        dir_abs.as_path(),
        || fs::read_dir(&dir_abs),
    ) {
        Ok(v) => v,
        Err(resp) => {
            tracing::warn!(
                "transfer best-effort read repair failed: op=plan_read_dir root_relpath={} resp={:?}",
                root_relpath,
                resp
            );
            return Ok(TransferSubtreeBatchPlan {
                closure: TransferSubtreeClosureState::Incomplete,
                total_bytes: 0,
                root_is_empty: false,
                mergeable_empty_dir_count: 0,
                mergeable_empty_dir_estimated_bytes: 0,
                direct_files_only_batches: Vec::new(),
                full_dir_batches: Vec::new(),
                child_scan_units: Vec::new(),
            });
        }
    };
    let root_direct_files_only_closed =
        direct_files_only_disposition_covers_root(assignment, root_relpath);
    let mut total_bytes = 0_i64;
    let mut has_visible_entries = false;
    let mut direct_listing_complete = true;
    let mut direct_files: Vec<FluxonFsTransferScanFrontierEntry> = Vec::new();
    let mut direct_symlink_notices: Vec<FluxonFsTransferSymlinkNoticeEntryWire> = Vec::new();
    let mut child_dirs: Vec<String> = Vec::new();
    for ent in rd {
        if deadline.is_some_and(|deadline| deadline.reached()) {
            direct_listing_complete = false;
            break;
        }
        let ent = match ent {
            Ok(v) => v,
            Err(err) if io_error_is_permission_denied(&err) => {
                continue;
            }
            Err(err) => return Err(resp_err_io(err)),
        };
        let name = ent.file_name().to_string_lossy().to_string();
        let child_relpath = normalize_child_relpath(root_relpath, name.as_str());
        if is_relpath_skipped(&assignment.skip_entries, child_relpath.as_str()) {
            continue;
        }
        let child_path = ent.path();
        let md = match retry_after_target_path_chmod(
            child_path.as_path(),
            "plan_symlink_metadata",
            child_path.as_path(),
            || fs::symlink_metadata(&child_path),
        ) {
            Ok(v) => v,
            Err(resp) => {
                tracing::warn!(
                    "transfer best-effort read repair failed: op=plan_symlink_metadata relpath={} resp={:?}",
                    child_relpath,
                    resp
                );
                direct_listing_complete = false;
                continue;
            }
        };
        if md.file_type().is_symlink() {
            has_visible_entries = true;
            let link_target = match retry_after_parent_dir_chmod(
                child_path.as_path(),
                "plan_read_link",
                child_path.as_path(),
                || fs::read_link(&child_path),
            ) {
                Ok(v) => v,
                Err(resp) => {
                    tracing::warn!(
                        "transfer best-effort read repair failed: op=plan_read_link relpath={} resp={:?}",
                        child_relpath,
                        resp
                    );
                    direct_listing_complete = false;
                    continue;
                }
            };
            direct_symlink_notices.push(FluxonFsTransferSymlinkNoticeEntryWire {
                relpath: child_relpath,
                link_target: link_target.to_string_lossy().to_string(),
            });
            continue;
        }
        if md.is_file() {
            has_visible_entries = true;
            let size = md.len().min(i64::MAX as u64) as i64;
            total_bytes = total_bytes.saturating_add(size);
            direct_files.push(FluxonFsTransferScanFrontierEntry {
                relpath: child_relpath,
                size,
            });
            continue;
        }
        if !md.is_dir() {
            continue;
        }
        has_visible_entries = true;
        child_dirs.push(child_relpath);
    }
    if direct_listing_complete && !has_visible_entries {
        return Ok(TransferSubtreeBatchPlan {
            closure: TransferSubtreeClosureState::Mergeable,
            total_bytes: 0,
            root_is_empty: true,
            mergeable_empty_dir_count: 1,
            mergeable_empty_dir_estimated_bytes: estimate_empty_dir_manifest_entry_bytes(root_relpath),
            direct_files_only_batches: Vec::new(),
            full_dir_batches: Vec::new(),
            child_scan_units: Vec::new(),
        });
    }
    child_dirs.sort();
    let mut direct_files_only_batches: Vec<FluxonFsTransferScanBatchWire> = Vec::new();
    let mut full_dir_batches: Vec<FluxonFsTransferScanBatchWire> = Vec::new();
    let mut child_scan_units: Vec<FluxonFsTransferScanChildUnitWire> = Vec::new();
    let mut mergeable_child_relpaths: Vec<String> = Vec::new();
    let mut mergeable_empty_child_relpaths: Vec<String> = Vec::new();
    let mut child_partitioned = subtree_has_any_known_partition(assignment, root_relpath);
    let mut child_incomplete = false;
    let mut mergeable_empty_dir_count: usize = 0;
    let mut mergeable_empty_dir_estimated_bytes: usize = 0;
    for child_relpath in child_dirs {
        let child_plan = plan_transfer_subtree_batches(
            root_dir_abs,
            assignment,
            child_relpath.as_str(),
            deadline,
        )?;
        direct_files_only_batches.extend(child_plan.direct_files_only_batches);
        full_dir_batches.extend(child_plan.full_dir_batches);
        child_scan_units.extend(child_plan.child_scan_units);
        match child_plan.closure {
            TransferSubtreeClosureState::Mergeable => {
                let child_empty_dir_count = child_plan.mergeable_empty_dir_count;
                let child_empty_dir_estimated_bytes =
                    child_plan.mergeable_empty_dir_estimated_bytes;
                if mergeable_empty_dir_count
                    .saturating_add(child_empty_dir_count)
                    > TRANSFER_MERGEABLE_EMPTY_DIR_BUDGET
                    || mergeable_empty_dir_estimated_bytes
                        .saturating_add(child_empty_dir_estimated_bytes)
                        > TRANSFER_MERGEABLE_EMPTY_DIR_ESTIMATED_BYTES_BUDGET
                {
                    child_incomplete = true;
                    continue;
                }
                total_bytes = total_bytes.saturating_add(child_plan.total_bytes);
                mergeable_empty_dir_count =
                    mergeable_empty_dir_count.saturating_add(child_empty_dir_count);
                mergeable_empty_dir_estimated_bytes = mergeable_empty_dir_estimated_bytes
                    .saturating_add(child_empty_dir_estimated_bytes);
                if child_plan.root_is_empty {
                    mergeable_empty_child_relpaths.push(child_relpath);
                } else {
                    mergeable_child_relpaths.push(child_relpath);
                }
            }
            TransferSubtreeClosureState::Closed => {
                child_partitioned = true;
            }
            TransferSubtreeClosureState::Incomplete => {
                child_incomplete = true;
            }
        }
    }
    if !direct_listing_complete || child_incomplete {
        for child_relpath in mergeable_child_relpaths {
            child_scan_units.push(new_streaming_child_scan_unit(
                child_relpath,
                assignment.generation.saturating_add(1),
            ));
        }
        if direct_listing_complete
            && (!direct_files.is_empty()
                || !direct_symlink_notices.is_empty()
                || !mergeable_empty_child_relpaths.is_empty())
            && !root_direct_files_only_closed
        {
            direct_files_only_batches.extend(build_partitioned_direct_files_only_batches(
                assignment,
                root_relpath.to_string(),
                direct_files,
                direct_symlink_notices,
                mergeable_empty_child_relpaths,
            )?);
        }
        sort_transfer_scan_batches(&mut direct_files_only_batches);
        full_dir_batches.sort_by(|a, b| a.root_relpath.cmp(&b.root_relpath));
        return Ok(TransferSubtreeBatchPlan {
            closure: TransferSubtreeClosureState::Incomplete,
            total_bytes: 0,
            root_is_empty: false,
            mergeable_empty_dir_count: 0,
            mergeable_empty_dir_estimated_bytes: 0,
            direct_files_only_batches,
            full_dir_batches,
            child_scan_units,
        });
    }
    if !root_direct_files_only_closed
        && !child_partitioned
        && total_bytes >= assignment.batch_ready_bytes
    {
        return Ok(TransferSubtreeBatchPlan {
            closure: TransferSubtreeClosureState::Closed,
            total_bytes: 0,
            root_is_empty: false,
            mergeable_empty_dir_count: 0,
            mergeable_empty_dir_estimated_bytes: 0,
            direct_files_only_batches: Vec::new(),
            full_dir_batches: Vec::new(),
            child_scan_units: vec![new_streaming_child_scan_unit(
                root_relpath.to_string(),
                assignment.generation.saturating_add(1),
            )],
        });
    }
    if !root_direct_files_only_closed && !child_partitioned {
        return Ok(TransferSubtreeBatchPlan {
            closure: TransferSubtreeClosureState::Mergeable,
            total_bytes,
            root_is_empty: false,
            mergeable_empty_dir_count,
            mergeable_empty_dir_estimated_bytes,
            direct_files_only_batches: Vec::new(),
            full_dir_batches: Vec::new(),
            child_scan_units: Vec::new(),
        });
    }
    for child_relpath in mergeable_child_relpaths {
        child_scan_units.push(new_streaming_child_scan_unit(
            child_relpath,
            assignment.generation.saturating_add(1),
        ));
    }
    if (!direct_files.is_empty()
        || !direct_symlink_notices.is_empty()
        || !mergeable_empty_child_relpaths.is_empty())
        && !root_direct_files_only_closed
    {
        direct_files_only_batches.extend(build_partitioned_direct_files_only_batches(
            assignment,
            root_relpath.to_string(),
            direct_files,
            direct_symlink_notices,
            mergeable_empty_child_relpaths,
        )?);
    }
    sort_transfer_scan_batches(&mut direct_files_only_batches);
    full_dir_batches.sort_by(|a, b| a.root_relpath.cmp(&b.root_relpath));
    Ok(TransferSubtreeBatchPlan {
        closure: TransferSubtreeClosureState::Closed,
        total_bytes: 0,
        root_is_empty: false,
        mergeable_empty_dir_count: 0,
        mergeable_empty_dir_estimated_bytes: 0,
        direct_files_only_batches,
        full_dir_batches,
        child_scan_units,
    })
}

fn build_full_dir_batch_from_materialized_subtree(
    assignment: &FluxonFsTransferScanAssignmentWire,
    root_relpath: String,
    subtree: TransferScanMaterializedSubtree,
) -> Result<FluxonFsTransferScanBatchWire, FlatDict> {
    Ok(FluxonFsTransferScanBatchWire {
        batch_id: uuid::Uuid::new_v4().to_string(),
        root_relpath,
        batch_kind: FluxonFsTransferBatchKind::FullDir,
        manifest_blob: build_transfer_manifest_blob(subtree.files, subtree.empty_dirs)?,
        collect_infos: build_symlink_collect_infos(subtree.symlink_notices)?,
        generation: assignment.generation,
    })
}

fn build_direct_files_only_batch_from_entries(
    assignment: &FluxonFsTransferScanAssignmentWire,
    root_relpath: String,
    direct_files: Vec<FluxonFsTransferScanFrontierEntry>,
    direct_symlink_notices: Vec<FluxonFsTransferSymlinkNoticeEntryWire>,
    empty_dir_relpaths: Vec<String>,
) -> Result<FluxonFsTransferScanBatchWire, FlatDict> {
    build_direct_files_only_batch_from_entries_with_batch_id(
        uuid::Uuid::new_v4().to_string(),
        assignment,
        root_relpath,
        direct_files,
        direct_symlink_notices,
        empty_dir_relpaths,
    )
}

fn build_root_direct_files_only_batch_from_entries(
    assignment: &FluxonFsTransferScanAssignmentWire,
    partition_index: i64,
    direct_files: Vec<FluxonFsTransferScanFrontierEntry>,
    direct_symlink_notices: Vec<FluxonFsTransferSymlinkNoticeEntryWire>,
    empty_dir_relpaths: Vec<String>,
) -> Result<FluxonFsTransferScanBatchWire, FlatDict> {
    build_direct_files_only_batch_from_entries_with_batch_id(
        direct_files_only_batch_id_for_partition(assignment, partition_index),
        assignment,
        assignment.root_relpath.clone(),
        direct_files,
        direct_symlink_notices,
        empty_dir_relpaths,
    )
}

fn sort_transfer_scan_batches(batches: &mut [FluxonFsTransferScanBatchWire]) {
    batches.sort_by(|a, b| a.root_relpath.cmp(&b.root_relpath).then(a.batch_id.cmp(&b.batch_id)));
}

fn build_full_dir_batch_for_mergeable_subtree(
    root_dir_abs: &str,
    assignment: &FluxonFsTransferScanAssignmentWire,
    root_relpath: &str,
) -> Result<FluxonFsTransferScanBatchWire, FlatDict> {
    let subtree = materialize_transfer_subtree(root_dir_abs, assignment, root_relpath, None)?;
    build_full_dir_batch_from_materialized_subtree(assignment, root_relpath.to_string(), subtree)
}

fn build_finished_empty_subtree_stream_result(
    assignment: &FluxonFsTransferScanAssignmentWire,
) -> FluxonFsTransferScanResultWire {
    FluxonFsTransferScanResultWire {
        job_id: assignment.job_id.clone(),
        scan_epoch: assignment.scan_epoch,
        scan_unit_id: assignment.scan_unit_id.clone(),
        scan_task_id: assignment.scan_task_id.clone(),
        root_relpath: assignment.root_relpath.clone(),
        generation: assignment.generation,
        frontier: empty_transfer_scan_frontier(),
        direct_files_only_batches: Vec::new(),
        child_scan_units: Vec::new(),
        full_dir_batches: Vec::new(),
        finished: true,
    }
}

fn build_transfer_scan_result_for_subtree_streaming_root_dir_abs(
    root_dir_abs: &str,
    assignment: &FluxonFsTransferScanAssignmentWire,
) -> Result<FluxonFsTransferScanResultWire, FlatDict> {
    let Some(mut session) = take_transfer_subtree_streaming_session(root_dir_abs, assignment)? else {
        return Ok(build_finished_empty_subtree_stream_result(assignment));
    };
    loop {
        if session
            .dir_stack
            .is_empty()
        {
            let mut full_dir_batches = Vec::new();
            if let Some(batch) = flush_pending_subtree_stream_batch(assignment, &mut session)? {
                full_dir_batches.push(batch);
            }
            return Ok(FluxonFsTransferScanResultWire {
                job_id: assignment.job_id.clone(),
                scan_epoch: assignment.scan_epoch,
                scan_unit_id: assignment.scan_unit_id.clone(),
                scan_task_id: assignment.scan_task_id.clone(),
                root_relpath: assignment.root_relpath.clone(),
                generation: assignment.generation,
                frontier: empty_transfer_scan_frontier(),
                direct_files_only_batches: Vec::new(),
                child_scan_units: Vec::new(),
                full_dir_batches,
                finished: true,
            });
        }
        if TransferScanDeadline::from_assignment(assignment).is_some_and(|deadline| deadline.reached()) {
            let mut full_dir_batches = Vec::new();
            if let Some(batch) = flush_pending_subtree_stream_batch(assignment, &mut session)? {
                full_dir_batches.push(batch);
            }
            store_transfer_subtree_streaming_session(assignment.scan_unit_id.as_str(), session);
            return Ok(FluxonFsTransferScanResultWire {
                job_id: assignment.job_id.clone(),
                scan_epoch: assignment.scan_epoch,
                scan_unit_id: assignment.scan_unit_id.clone(),
                scan_task_id: assignment.scan_task_id.clone(),
                root_relpath: assignment.root_relpath.clone(),
                generation: assignment.generation,
                frontier: empty_transfer_scan_frontier(),
                direct_files_only_batches: Vec::new(),
                child_scan_units: vec![same_root_continuation_scan_unit(assignment)],
                full_dir_batches,
                finished: false,
            });
        }
        let next_entry = {
            let frame = session.dir_stack.last_mut().unwrap();
            frame.read_dir.next()
        };
        let Some(next_entry) = next_entry else {
            let frame = session.dir_stack.pop().unwrap();
            if !frame.saw_visible_child {
                session.pending_empty_dirs.push(frame.dir_relpath);
            }
            if should_flush_subtree_stream_batch(
                assignment.batch_ready_bytes,
                session.pending_bytes,
                session.pending_files.len().saturating_add(session.pending_symlink_notices.len()),
                session.pending_empty_dirs.len(),
            ) {
                let batch = flush_pending_subtree_stream_batch(assignment, &mut session)?.unwrap();
                store_transfer_subtree_streaming_session(assignment.scan_unit_id.as_str(), session);
                return Ok(FluxonFsTransferScanResultWire {
                    job_id: assignment.job_id.clone(),
                    scan_epoch: assignment.scan_epoch,
                    scan_unit_id: assignment.scan_unit_id.clone(),
                    scan_task_id: assignment.scan_task_id.clone(),
                    root_relpath: assignment.root_relpath.clone(),
                    generation: assignment.generation,
                    frontier: empty_transfer_scan_frontier(),
                    direct_files_only_batches: Vec::new(),
                    child_scan_units: vec![same_root_continuation_scan_unit(assignment)],
                    full_dir_batches: vec![batch],
                    finished: false,
                });
            }
            continue;
        };
        let ent = match next_entry {
            Ok(v) => v,
            Err(err) if io_error_is_permission_denied(&err) => {
                continue;
            }
            Err(err) => return Err(resp_err_io(err)),
        };
        let name = ent.file_name().to_string_lossy().to_string();
        let frame = session.dir_stack.last_mut().unwrap();
        let child_relpath = normalize_child_relpath(frame.dir_relpath.as_str(), name.as_str());
        if is_relpath_skipped(&assignment.skip_entries, child_relpath.as_str()) {
            continue;
        }
        let child_path = ent.path();
        let md = retry_after_target_path_chmod(
            child_path.as_path(),
            "subtree_stream_symlink_metadata",
            child_path.as_path(),
            || fs::symlink_metadata(&child_path),
        )?;
        if md.file_type().is_symlink() {
            frame.saw_visible_child = true;
            let link_target = retry_after_parent_dir_chmod(
                child_path.as_path(),
                "subtree_stream_read_link",
                child_path.as_path(),
                || fs::read_link(&child_path),
            )?;
            session
                .pending_symlink_notices
                .push(FluxonFsTransferSymlinkNoticeEntryWire {
                    relpath: child_relpath,
                    link_target: link_target.to_string_lossy().to_string(),
                });
        } else if md.is_dir() {
            frame.saw_visible_child = true;
            session.dir_stack.push(open_transfer_subtree_streaming_dir_frame(
                child_path,
                child_relpath,
            )?);
        } else if md.is_file() {
            frame.saw_visible_child = true;
            let size = md.len().min(i64::MAX as u64) as i64;
            session.pending_bytes = session.pending_bytes.saturating_add(size);
            session
                .pending_files
                .push(FluxonFsTransferScanFrontierEntry { relpath: child_relpath, size });
        }
        if should_flush_subtree_stream_batch(
            assignment.batch_ready_bytes,
            session.pending_bytes,
            session.pending_files.len().saturating_add(session.pending_symlink_notices.len()),
            session.pending_empty_dirs.len(),
        ) {
            let batch = flush_pending_subtree_stream_batch(assignment, &mut session)?.unwrap();
            store_transfer_subtree_streaming_session(assignment.scan_unit_id.as_str(), session);
            return Ok(FluxonFsTransferScanResultWire {
                job_id: assignment.job_id.clone(),
                scan_epoch: assignment.scan_epoch,
                scan_unit_id: assignment.scan_unit_id.clone(),
                scan_task_id: assignment.scan_task_id.clone(),
                root_relpath: assignment.root_relpath.clone(),
                generation: assignment.generation,
                frontier: empty_transfer_scan_frontier(),
                direct_files_only_batches: Vec::new(),
                child_scan_units: vec![same_root_continuation_scan_unit(assignment)],
                full_dir_batches: vec![batch],
                finished: false,
            });
        }
    }
}

pub(crate) fn build_transfer_scan_result_for_root_dir_abs(
    root_dir_abs: &str,
    assignment: &FluxonFsTransferScanAssignmentWire,
) -> Result<FluxonFsTransferScanResultWire, FlatDict> {
    if full_dir_disposition_for_assignment(assignment).is_some() {
        return Ok(FluxonFsTransferScanResultWire {
            job_id: assignment.job_id.clone(),
            scan_epoch: assignment.scan_epoch,
            scan_unit_id: assignment.scan_unit_id.clone(),
            scan_task_id: assignment.scan_task_id.clone(),
            root_relpath: assignment.root_relpath.clone(),
            generation: assignment.generation,
            frontier: empty_transfer_scan_frontier(),
            direct_files_only_batches: Vec::new(),
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        });
    }
    if assignment.scan_mode == FluxonFsTransferScanMode::SubtreeStreaming {
        return build_transfer_scan_result_for_subtree_streaming_root_dir_abs(
            root_dir_abs,
            assignment,
        );
    }
    let deadline = TransferScanDeadline::from_assignment(assignment);
    let root_listing = match collect_transfer_root_dir_listing_slice(root_dir_abs, assignment, deadline)? {
        TransferRootDirListingOutcome::Complete(v) => v,
        TransferRootDirListingOutcome::Finished(result) => return Ok(result),
        TransferRootDirListingOutcome::Partial(result) => return Ok(result),
    };
    let mut direct_files = root_listing.direct_files;
    let mut direct_symlink_notices = root_listing.direct_symlink_notices;
    let mut direct_empty_dirs = root_listing.direct_empty_dirs;
    let mut direct_dirs = root_listing.direct_dirs;
    let mut direct_files_only_batches = root_listing.direct_files_only_batches;
    let mut full_dir_batches: Vec<FluxonFsTransferScanBatchWire> = Vec::new();
    let mut child_scan_units: Vec<FluxonFsTransferScanChildUnitWire> = Vec::new();
    let mut root_total_bytes = root_listing.root_total_bytes;
    let root_visible_entries = root_listing.root_visible_entries;
    let delegated_child_scan_unit_count = root_listing.emitted_child_scan_unit_count;
    let root_is_empty = !root_visible_entries;
    if root_is_empty {
        return Ok(FluxonFsTransferScanResultWire {
            job_id: assignment.job_id.clone(),
            scan_epoch: assignment.scan_epoch,
            scan_unit_id: assignment.scan_unit_id.clone(),
            scan_task_id: assignment.scan_task_id.clone(),
            root_relpath: assignment.root_relpath.clone(),
            generation: assignment.generation,
            frontier: empty_transfer_scan_frontier(),
            direct_files_only_batches: Vec::new(),
            child_scan_units: Vec::new(),
            full_dir_batches: vec![build_subtree_slice_batch_from_entries_with_batch_id(
                subtree_slice_batch_id_for_partition(assignment, 0),
                assignment,
                Vec::new(),
                Vec::new(),
                vec![assignment.root_relpath.clone()],
            )?],
            finished: true,
        });
    }
    direct_files.sort_by(|a, b| a.relpath.cmp(&b.relpath));
    direct_empty_dirs.sort();
    direct_dirs.sort_by(|a, b| a.relpath.cmp(&b.relpath));
    direct_symlink_notices.sort_by(|a, b| a.relpath.cmp(&b.relpath));
    if assignment.scan_mode == FluxonFsTransferScanMode::RootDirectFanoutOnly
        || assignment.scan_mode == FluxonFsTransferScanMode::DirectoryDirectFanoutOnly
        || delegated_child_scan_unit_count > 0
    {
        if (!direct_files.is_empty()
            || !direct_symlink_notices.is_empty()
            || !direct_empty_dirs.is_empty())
            && !direct_files_only_disposition_covers_root(assignment, assignment.root_relpath.as_str())
        {
            let mut next_partition_index = root_listing.emitted_direct_files_batch_count;
            direct_files_only_batches.extend(build_partitioned_root_direct_files_only_batches(
                assignment,
                &mut next_partition_index,
                direct_files.clone(),
                direct_symlink_notices.clone(),
                direct_empty_dirs.clone(),
            )?);
        }
        child_scan_units.extend(
            direct_dirs[delegated_child_scan_unit_count..]
                .iter()
                .map(|entry| {
                    new_child_scan_unit(
                        entry.relpath.clone(),
                        assignment.generation + 1,
                        delegated_child_scan_mode(),
                    )
                }),
        );
        child_scan_units.sort_by(|a, b| a.root_relpath.cmp(&b.root_relpath));
        sort_transfer_scan_batches(&mut direct_files_only_batches);
        return Ok(FluxonFsTransferScanResultWire {
            job_id: assignment.job_id.clone(),
            scan_epoch: assignment.scan_epoch,
            scan_unit_id: assignment.scan_unit_id.clone(),
            scan_task_id: assignment.scan_task_id.clone(),
            root_relpath: assignment.root_relpath.clone(),
            generation: assignment.generation,
            frontier: FluxonFsTransferScanFrontier {
                direct_files,
                direct_dirs,
                empty_dirs: direct_empty_dirs
                    .into_iter()
                    .map(|relpath| FluxonFsTransferScanFrontierDirEntry { relpath })
                    .collect(),
            },
            direct_files_only_batches,
            child_scan_units,
            full_dir_batches,
            finished: true,
        });
    }
    let mut mergeable_child_relpaths: Vec<String> = Vec::new();
    let mut mergeable_empty_child_relpaths: Vec<String> = Vec::new();
    let mut incomplete_child_relpaths: Vec<String> = Vec::new();
    let mut root_partitioned = root_listing.emitted_direct_files_batch_count > 0
        || direct_files_only_disposition_covers_root(assignment, assignment.root_relpath.as_str());
    let mut mergeable_empty_dir_count = direct_empty_dirs.len();
    let mut mergeable_empty_dir_estimated_bytes = estimate_empty_dir_manifest_bytes(&direct_empty_dirs);
    for child_relpath in direct_dirs.iter().map(|entry| entry.relpath.clone()) {
        let child_plan = plan_transfer_subtree_batches(
            root_dir_abs,
            assignment,
            child_relpath.as_str(),
            deadline,
        )?;
        direct_files_only_batches.extend(child_plan.direct_files_only_batches);
        full_dir_batches.extend(child_plan.full_dir_batches);
        child_scan_units.extend(child_plan.child_scan_units);
        match child_plan.closure {
            TransferSubtreeClosureState::Mergeable => {
                let child_empty_dir_count = child_plan.mergeable_empty_dir_count;
                let child_empty_dir_estimated_bytes =
                    child_plan.mergeable_empty_dir_estimated_bytes;
                if mergeable_empty_dir_count
                    .saturating_add(child_empty_dir_count)
                    > TRANSFER_MERGEABLE_EMPTY_DIR_BUDGET
                    || mergeable_empty_dir_estimated_bytes
                        .saturating_add(child_empty_dir_estimated_bytes)
                        > TRANSFER_MERGEABLE_EMPTY_DIR_ESTIMATED_BYTES_BUDGET
                {
                    incomplete_child_relpaths.push(child_relpath);
                    continue;
                }
                root_total_bytes = root_total_bytes.saturating_add(child_plan.total_bytes);
                mergeable_empty_dir_count =
                    mergeable_empty_dir_count.saturating_add(child_empty_dir_count);
                mergeable_empty_dir_estimated_bytes = mergeable_empty_dir_estimated_bytes
                    .saturating_add(child_empty_dir_estimated_bytes);
                if child_plan.root_is_empty {
                    mergeable_empty_child_relpaths.push(child_relpath);
                } else {
                    mergeable_child_relpaths.push(child_relpath);
                }
            }
            TransferSubtreeClosureState::Closed => {
                root_partitioned = true;
            }
            TransferSubtreeClosureState::Incomplete => {
                incomplete_child_relpaths.push(child_relpath);
            }
        }
    }
    for child_relpath in mergeable_child_relpaths {
        root_partitioned = true;
        child_scan_units.push(new_streaming_child_scan_unit(
            child_relpath,
            assignment.generation.saturating_add(1),
        ));
    }
    if !incomplete_child_relpaths.is_empty() {
        if (!direct_files.is_empty()
            || !direct_symlink_notices.is_empty()
            || !mergeable_empty_child_relpaths.is_empty())
            && !direct_files_only_disposition_covers_root(assignment, assignment.root_relpath.as_str())
        {
            let mut next_partition_index = root_listing.emitted_direct_files_batch_count;
            direct_empty_dirs.extend(mergeable_empty_child_relpaths);
            direct_empty_dirs.sort();
            direct_files_only_batches.extend(build_partitioned_root_direct_files_only_batches(
                assignment,
                &mut next_partition_index,
                direct_files.clone(),
                direct_symlink_notices.clone(),
                direct_empty_dirs.clone(),
            )?);
        }
        for child_relpath in incomplete_child_relpaths {
            child_scan_units.push(new_child_scan_unit(
                child_relpath,
                assignment.generation + 1,
                delegated_child_scan_mode(),
            ));
        }
        child_scan_units.sort_by(|a, b| a.root_relpath.cmp(&b.root_relpath));
        sort_transfer_scan_batches(&mut direct_files_only_batches);
        full_dir_batches.sort_by(|a, b| a.root_relpath.cmp(&b.root_relpath));
        return Ok(FluxonFsTransferScanResultWire {
            job_id: assignment.job_id.clone(),
            scan_epoch: assignment.scan_epoch,
            scan_unit_id: assignment.scan_unit_id.clone(),
            scan_task_id: assignment.scan_task_id.clone(),
            root_relpath: assignment.root_relpath.clone(),
            generation: assignment.generation,
            frontier: FluxonFsTransferScanFrontier {
                direct_files,
                direct_dirs,
                empty_dirs: direct_empty_dirs
                    .into_iter()
                    .map(|relpath| FluxonFsTransferScanFrontierDirEntry { relpath })
                    .collect(),
            },
            direct_files_only_batches,
            child_scan_units,
            full_dir_batches,
            finished: false,
        });
    }
    if !root_partitioned {
        return Ok(FluxonFsTransferScanResultWire {
            job_id: assignment.job_id.clone(),
            scan_epoch: assignment.scan_epoch,
            scan_unit_id: assignment.scan_unit_id.clone(),
            scan_task_id: assignment.scan_task_id.clone(),
            root_relpath: assignment.root_relpath.clone(),
            generation: assignment.generation,
            frontier: empty_transfer_scan_frontier(),
            direct_files_only_batches: Vec::new(),
            child_scan_units: vec![new_streaming_child_scan_unit(
                assignment.root_relpath.clone(),
                assignment.generation.saturating_add(1),
            )],
            full_dir_batches: Vec::new(),
            finished: true,
        });
    }
    if (!direct_files.is_empty()
        || !direct_symlink_notices.is_empty()
        || !direct_empty_dirs.is_empty()
        || !mergeable_empty_child_relpaths.is_empty())
        && !direct_files_only_disposition_covers_root(assignment, assignment.root_relpath.as_str())
    {
        let mut next_partition_index = root_listing.emitted_direct_files_batch_count;
        direct_empty_dirs.extend(mergeable_empty_child_relpaths);
        direct_empty_dirs.sort();
        direct_files_only_batches.extend(build_partitioned_root_direct_files_only_batches(
            assignment,
            &mut next_partition_index,
            direct_files.clone(),
            direct_symlink_notices,
            direct_empty_dirs.clone(),
        )?);
    }
    child_scan_units.sort_by(|a, b| a.root_relpath.cmp(&b.root_relpath));
    sort_transfer_scan_batches(&mut direct_files_only_batches);
    full_dir_batches.sort_by(|a, b| a.root_relpath.cmp(&b.root_relpath));
    Ok(FluxonFsTransferScanResultWire {
        job_id: assignment.job_id.clone(),
        scan_epoch: assignment.scan_epoch,
        scan_unit_id: assignment.scan_unit_id.clone(),
        scan_task_id: assignment.scan_task_id.clone(),
        root_relpath: assignment.root_relpath.clone(),
        generation: assignment.generation,
        frontier: FluxonFsTransferScanFrontier {
            direct_files,
            direct_dirs,
            empty_dirs: direct_empty_dirs
                .into_iter()
                .map(|relpath| FluxonFsTransferScanFrontierDirEntry { relpath })
                .collect(),
        },
        direct_files_only_batches,
        child_scan_units,
        full_dir_batches,
        finished: true,
    })
}

#[cfg(test)]
fn handle_transfer_scan_assignment(
    exports: &AgentExportsHandle,
    assignment: FluxonFsTransferScanAssignmentWire,
) -> FlatDict {
    let current_disposition = full_dir_disposition_for_assignment(&assignment);
    if let Some(disposition) = current_disposition {
        return build_disposition_blocked_scan_result(assignment, disposition);
    }
    let root_dir_abs = match exports.export_root_dir_abs(assignment.src_export.as_str()) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    tracing::info!(
        "transfer scan assignment start: job_id={} scan_epoch={} scan_unit_id={} scan_task_id={} src_export={} src_exporter_id={} root_relpath={} generation={} known_disposition_count={}",
        assignment.job_id,
        assignment.scan_epoch,
        assignment.scan_unit_id,
        assignment.scan_task_id,
        assignment.src_export,
        assignment.src_exporter_id,
        assignment.root_relpath,
        assignment.generation,
        assignment.known_dispositions.len(),
    );
    let result = match build_transfer_scan_result_for_root_dir_abs(root_dir_abs.as_str(), &assignment) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    encode_transfer_scan_result(&result, "transfer scan result")
}

// Write a file into its staging suffix path first. The file is still invisible
// to transfer correctness until a later rename promotes it into the final path.
fn prepare_transfer_file_streaming<ReadChunkFn, CheckpointFn>(
    dst_root: &PathBuf,
    staging_prefix: &str,
    file: &FluxonFsTransferManifestEntryWire,
    coordinator: &TransferWorkerCoordinator<ReadChunkFn, CheckpointFn>,
) -> Result<PreparedTransferFile, TransferWorkerExecutionError>
where
    ReadChunkFn:
        Fn(&FluxonFsTransferManifestEntryWire, i64, i64) -> Result<Vec<u8>, TransferWorkerExecutionError>,
    CheckpointFn: Fn() -> Result<(), TransferWorkerExecutionError>,
{
    let staging_relpath = transfer_staging_file_relpath(staging_prefix, file.relpath.as_str())
        .map_err(TransferWorkerExecutionError::fatal)?;
    let final_relpath = file.relpath.clone();
    ensure_transfer_parent_dirs(dst_root, staging_relpath.as_str())
        .map_err(TransferWorkerExecutionError::fatal)?;
    ensure_transfer_parent_dirs(dst_root, final_relpath.as_str())
        .map_err(TransferWorkerExecutionError::fatal)?;
    let staging_abs = safe_join_root(dst_root.to_string_lossy().as_ref(), staging_relpath.as_str())
        .map_err(resp_err_kverr)
        .map_err(TransferWorkerExecutionError::fatal)?;
    let mut dst_file = open_create_file_with_parent_dir_chmod_retry(&staging_abs)
        .map_err(TransferWorkerExecutionError::fatal)?;
    dst_file
        .set_len(0)
        .map_err(resp_err_io)
        .map_err(TransferWorkerExecutionError::fatal)?;
    let mut copied: i64 = 0;
    while copied < file.size {
        coordinator.checkpoint_continue()?;
        let remaining = file.size.saturating_sub(copied);
        let chunk = coordinator.read_chunk(file, copied, remaining.min(CHUNK_BYTES as i64))?;
        if chunk.is_empty() {
            return Err(TransferWorkerExecutionError::fatal(resp_err_kverr(KvError::Api(
                ApiError::InvalidArgument {
                    detail: format!(
                        "transfer worker source ended before expected size: relpath={} expected={} copied={}",
                        file.relpath, file.size, copied
                    ),
                },
            ))));
        }
        dst_file
            .write_all(&chunk)
            .map_err(resp_err_io)
            .map_err(TransferWorkerExecutionError::fatal)?;
        copied = copied.saturating_add(chunk.len() as i64);
        coordinator.record_written_bytes(chunk.len() as i64);
    }
    if copied != file.size {
        return Err(TransferWorkerExecutionError::fatal(resp_err_kverr(KvError::Api(
            ApiError::InvalidArgument {
            detail: format!(
                "transfer worker size mismatch before staging completion: relpath={} expected={} actual={}",
                file.relpath, file.size, copied
            ),
        }))));
    }
    // The staged file is still invisible at this point, so one more checkpoint
    // keeps supersession able to stop the worker before any later visible
    // promotion step.
    coordinator.checkpoint_continue()?;
    dst_file
        .sync_data()
        .map_err(resp_err_io)
        .map_err(TransferWorkerExecutionError::fatal)?;
    drop(dst_file);
    Ok(PreparedTransferFile {
        staging_relpath,
        final_relpath,
        visible_size: copied,
    })
}

fn execute_transfer_single_file<ReadChunkFn, CheckpointFn>(
    dst_root: &PathBuf,
    staging_prefix: &str,
    file: &FluxonFsTransferManifestEntryWire,
    coordinator: &TransferWorkerCoordinator<ReadChunkFn, CheckpointFn>,
) -> Result<TransferWorkerLaneOutcome, TransferWorkerExecutionError>
where
    ReadChunkFn:
        Fn(&FluxonFsTransferManifestEntryWire, i64, i64) -> Result<Vec<u8>, TransferWorkerExecutionError>,
    CheckpointFn: Fn() -> Result<(), TransferWorkerExecutionError>,
{
    coordinator.checkpoint_continue()?;
    let prepared = match prepare_transfer_file_streaming(dst_root, staging_prefix, file, coordinator) {
        Ok(v) => v,
        Err(TransferWorkerExecutionError::Fatal(resp)) => {
            if let Some(failed) = classify_transfer_failed_file(file, &resp) {
                let staging_relpath =
                    transfer_staging_file_relpath(staging_prefix, file.relpath.as_str())
                        .map_err(TransferWorkerExecutionError::fatal)?;
                let staging_abs = safe_join_root(
                    dst_root.to_string_lossy().as_ref(),
                    staging_relpath.as_str(),
                )
                .map_err(resp_err_kverr)
                .map_err(TransferWorkerExecutionError::fatal)?;
                match fs::remove_file(&staging_abs) {
                    Ok(()) => {}
                    Err(err) if err.kind() == ErrorKind::NotFound => {}
                    Err(err) => return Err(TransferWorkerExecutionError::fatal(resp_err_io(err))),
                }
                return Ok(TransferWorkerLaneOutcome::Failed(
                    TransferWorkerLaneFailedFileResult { result: failed },
                ));
            }
            return Err(TransferWorkerExecutionError::Fatal(resp));
        }
        Err(err) => return Err(err),
    };
    coordinator.checkpoint_continue()?;
    let result = promote_prepared_transfer_file(dst_root, PreparedTransferFile {
        staging_relpath: prepared.staging_relpath.clone(),
        final_relpath: prepared.final_relpath.clone(),
        visible_size: prepared.visible_size,
    })
    .map_err(TransferWorkerExecutionError::fatal);
    match result {
        Ok(result) => Ok(TransferWorkerLaneOutcome::Visible(TransferWorkerLaneFileResult {
            result,
        })),
        Err(TransferWorkerExecutionError::Fatal(resp)) => {
            if let Some(failed) = classify_transfer_failed_file(file, &resp) {
                let staging_abs = safe_join_root(
                    dst_root.to_string_lossy().as_ref(),
                    prepared.staging_relpath.as_str(),
                )
                .map_err(resp_err_kverr)
                .map_err(TransferWorkerExecutionError::fatal)?;
                match fs::remove_file(&staging_abs) {
                    Ok(()) => {}
                    Err(err) if err.kind() == ErrorKind::NotFound => {}
                    Err(err) => return Err(TransferWorkerExecutionError::fatal(resp_err_io(err))),
                }
                Ok(TransferWorkerLaneOutcome::Failed(
                    TransferWorkerLaneFailedFileResult { result: failed },
                ))
            } else {
                Err(TransferWorkerExecutionError::Fatal(resp))
            }
        }
        Err(err) => Err(err),
    }
}

fn execute_transfer_empty_dir<ReadChunkFn, CheckpointFn>(
    dst_root: &PathBuf,
    empty_dir_relpath: &str,
    coordinator: &TransferWorkerCoordinator<ReadChunkFn, CheckpointFn>,
) -> Result<TransferWorkerLaneOutcome, TransferWorkerExecutionError>
where
    ReadChunkFn:
        Fn(&FluxonFsTransferManifestEntryWire, i64, i64) -> Result<Vec<u8>, TransferWorkerExecutionError>,
    CheckpointFn: Fn() -> Result<(), TransferWorkerExecutionError>,
{
    coordinator.checkpoint_continue()?;
    materialize_transfer_empty_dir(dst_root, empty_dir_relpath)
        .map_err(TransferWorkerExecutionError::fatal)?;
    coordinator.record_materialized_empty_dir();
    Ok(TransferWorkerLaneOutcome::EmptyDirCreated)
}

fn execute_transfer_worker_assignment_with_policy<ReadChunkFn, CheckpointFn>(
    assignment: &FluxonFsTransferWorkerAssignmentWire,
    dst_root: &PathBuf,
    policy: TransferWorkerLanePolicy,
    checkpoint_continue: CheckpointFn,
    read_chunk: ReadChunkFn,
) -> Result<FluxonFsTransferWorkerResultWire, TransferWorkerExecutionError>
where
    ReadChunkFn:
        Fn(&FluxonFsTransferManifestEntryWire, i64, i64) -> Result<Vec<u8>, TransferWorkerExecutionError>
            + Send
            + Sync
            + 'static,
    CheckpointFn: Fn() -> Result<(), TransferWorkerExecutionError> + Send + Sync + 'static,
{
    let policy = policy.normalized();
    let progress = Arc::new(TransferWorkerProgressWindow::new(
        Arc::new(policy.clone()),
        chrono::Utc::now().timestamp_millis(),
    ));
    execute_transfer_worker_assignment_with_policy_and_progress(
        assignment,
        dst_root,
        policy,
        progress,
        checkpoint_continue,
        read_chunk,
    )
}

fn execute_transfer_worker_assignment_with_policy_and_progress<ReadChunkFn, CheckpointFn>(
    assignment: &FluxonFsTransferWorkerAssignmentWire,
    dst_root: &PathBuf,
    policy: TransferWorkerLanePolicy,
    progress: Arc<TransferWorkerProgressWindow>,
    checkpoint_continue: CheckpointFn,
    read_chunk: ReadChunkFn,
) -> Result<FluxonFsTransferWorkerResultWire, TransferWorkerExecutionError>
where
    ReadChunkFn:
        Fn(&FluxonFsTransferManifestEntryWire, i64, i64) -> Result<Vec<u8>, TransferWorkerExecutionError>
            + Send
            + Sync
            + 'static,
    CheckpointFn: Fn() -> Result<(), TransferWorkerExecutionError> + Send + Sync + 'static,
{
    create_dir_all_with_parent_dir_chmod_retry(dst_root)
        .map_err(TransferWorkerExecutionError::fatal)?;
    let manifest =
        FluxonFsTransferManifestWire::decode_from_blob(assignment.manifest_blob.as_slice())
            .map_err(|e| {
                TransferWorkerExecutionError::fatal(resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
                    detail: format!("decode transfer worker manifest failed: {}", e),
                })))
            })?;
    if transfer_manifest_is_empty_dirs_only_batch(&manifest, assignment.collect_infos.as_slice()) {
        // Empty-dir-only batches never generate byte-based ramp-up signals, so
        // they must start at full local lane width to avoid serial tail work.
        let empty_dir_lane_count = manifest
            .empty_dir_relpaths
            .len()
            .clamp(1, policy.max_file_lanes.max(1));
        progress
            .desired_file_lanes
            .store(empty_dir_lane_count, Ordering::SeqCst);
    }
    let mut collect_info_results: Vec<FluxonFsTransferWorkerCollectInfoResultWire> = Vec::new();
    let mut file_results: Vec<FluxonFsTransferWorkerFileResultWire> = Vec::new();
    let mut failed_file_results: Vec<FluxonFsTransferWorkerFailedFileResultWire> = Vec::new();
    let coordinator = Arc::new(TransferWorkerCoordinator::new(
        TransferWorkerLogContext {
            job_id: assignment.job_id.clone(),
            batch_id: assignment.batch_id.clone(),
            worker_id: assignment.worker_id.clone(),
            worker_task_id: assignment.worker_task_id.clone(),
        },
        Arc::new(policy),
        progress,
        checkpoint_continue,
        read_chunk,
    ));
    let mut lane_tasks: Vec<TransferWorkerLaneTask> =
        Vec::with_capacity(manifest.entries.len() + manifest.empty_dir_relpaths.len());
    // Keep file tasks at the front so many empty directories do not delay
    // byte-producing work behind a serial directory-only setup phase.
    for entry in manifest.entries {
        lane_tasks.push(TransferWorkerLaneTask::File(entry));
    }
    for empty_dir_relpath in manifest.empty_dir_relpaths {
        lane_tasks.push(TransferWorkerLaneTask::EmptyDir(empty_dir_relpath));
    }
    let lane_tasks = Arc::new(lane_tasks);
    if !lane_tasks.is_empty() {
        let next_index = Arc::new(AtomicUsize::new(0));
        let active_lane_count = Arc::new(AtomicUsize::new(0));
        let queued_result_count = Arc::new(AtomicUsize::new(0));
        let (result_tx, result_rx) =
            mpsc::channel::<Result<TransferWorkerLaneOutcome, TransferWorkerExecutionError>>();
        let mut started_lanes: usize = 0;
        let mut finished_tasks: usize = 0;
        let mut lane_handles: Vec<thread::JoinHandle<()>> = Vec::new();
        while finished_tasks < lane_tasks.len() {
            coordinator.tick_progress_window();
            while started_lanes < coordinator.desired_file_lanes()
                && started_lanes < coordinator.policy.max_file_lanes
                && started_lanes < lane_tasks.len()
            {
                started_lanes += 1;
                active_lane_count.fetch_add(1, Ordering::SeqCst);
                let dst_root2 = dst_root.clone();
                let staging_prefix = assignment.staging_prefix.clone();
                let lane_tasks2 = lane_tasks.clone();
                let next_index2 = next_index.clone();
                let active_lane_count2 = active_lane_count.clone();
                let queued_result_count2 = queued_result_count.clone();
                let result_tx2 = result_tx.clone();
                let coordinator2 = coordinator.clone();
                let handle = thread::spawn(move || {
                    loop {
                        let task_index = next_index2.fetch_add(1, Ordering::SeqCst);
                        if task_index >= lane_tasks2.len() {
                            active_lane_count2.fetch_sub(1, Ordering::SeqCst);
                            return;
                        }
                        let result = match &lane_tasks2[task_index] {
                            TransferWorkerLaneTask::File(file) => execute_transfer_single_file(
                                &dst_root2,
                                staging_prefix.as_str(),
                                file,
                                coordinator2.as_ref(),
                            ),
                            TransferWorkerLaneTask::EmptyDir(empty_dir_relpath) => {
                                execute_transfer_empty_dir(
                                    &dst_root2,
                                    empty_dir_relpath.as_str(),
                                    coordinator2.as_ref(),
                                )
                            }
                        };
                        let terminal = result.is_err();
                        queued_result_count2.fetch_add(1, Ordering::SeqCst);
                        if result_tx2.send(result).is_err() {
                            decrement_pending_lane_result_count(queued_result_count2.as_ref());
                            active_lane_count2.fetch_sub(1, Ordering::SeqCst);
                            return;
                        }
                        if terminal {
                            active_lane_count2.fetch_sub(1, Ordering::SeqCst);
                            return;
                        }
                    }
                });
                lane_handles.push(handle);
            }
            match result_rx.recv_timeout(coordinator.lane_poll_interval()) {
                Ok(result) => {
                    decrement_pending_lane_result_count(queued_result_count.as_ref());
                    match result {
                        Ok(TransferWorkerLaneOutcome::EmptyDirCreated) => {
                            finished_tasks += 1;
                        }
                        Ok(TransferWorkerLaneOutcome::Visible(file_result)) => {
                            finished_tasks += 1;
                            file_results.push(file_result.result);
                        }
                        Ok(TransferWorkerLaneOutcome::Failed(file_result)) => {
                            finished_tasks += 1;
                            failed_file_results.push(file_result.result);
                        }
                        Err(err) => {
                            coordinator.stop();
                            for handle in lane_handles {
                                let _ = handle.join();
                            }
                            return Err(err);
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    continue;
                }
                Err(_) => {
                    coordinator.stop();
                    for handle in lane_handles {
                        let _ = handle.join();
                    }
                    return Err(TransferWorkerExecutionError::fatal(resp_err_kverr(
                        KvError::Api(ApiError::Unknown {
                            detail: "transfer worker lane tasks stopped before batch completion"
                                .to_string(),
                        }),
                    )));
                }
            }
            if transfer_worker_lane_execution_exhausted(
                active_lane_count.load(Ordering::SeqCst),
                queued_result_count.load(Ordering::SeqCst),
                finished_tasks,
                lane_tasks.len(),
            ) {
                coordinator.stop();
                for handle in lane_handles {
                    let _ = handle.join();
                }
                return Err(TransferWorkerExecutionError::fatal(resp_err_kverr(
                    KvError::Api(ApiError::Unknown {
                        detail: format!(
                            "transfer worker exhausted lane tasks before completion: finished={} total={}",
                            finished_tasks,
                            lane_tasks.len()
                        ),
                    }),
                )));
            }
        }
        // Lane tasks must be joined before collect-info materialization, but
        // the worker attempt itself is still live. Stopping the coordinator
        // here would make every later checkpoint report Superseded even though
        // the batch is still executing its collect-info phase.
        for handle in lane_handles {
            let _ = handle.join();
        }
        file_results.sort_by(|a, b| a.relpath.cmp(&b.relpath));
        failed_file_results.sort_by(|a, b| a.relpath.cmp(&b.relpath));
    }
    for collect_info in &assignment.collect_infos {
        coordinator.checkpoint_continue()?;
        let prepared = prepare_transfer_collect_info_materialization(
            dst_root,
            assignment.batch_id.as_str(),
            assignment.worker_task_id.as_str(),
            collect_info,
        )
        .map_err(TransferWorkerExecutionError::fatal)?;
        coordinator.checkpoint_continue()?;
        collect_info_results.push(
            promote_prepared_transfer_collect_info(dst_root, prepared)
                .map_err(TransferWorkerExecutionError::fatal)?,
        );
    }
    let progress_snapshot = coordinator.progress_snapshot();
    let sample = &progress_snapshot.last_sample;
    let final_telemetry = Some(transfer_worker_telemetry_from_progress_snapshot(
        &progress_snapshot,
    ));
    tracing::info!(
        "transfer worker throughput summary: job_id={} batch_id={} worker_id={} worker_task_id={} total_written_bytes={} peak_sample_goodput_bytes_per_sec={} last_window_started_unix_ms={} last_window_elapsed_ms={} last_window_bytes={} last_window_goodput_bytes_per_sec={} desired_file_lanes={} completed_file_count={} failed_file_count={} collect_info_count={}",
        assignment.job_id,
        assignment.batch_id,
        assignment.worker_id,
        assignment.worker_task_id,
        progress_snapshot.total_written_bytes,
        progress_snapshot.peak_sample_goodput_bytes_per_sec,
        sample.window_started_unix_ms,
        sample.window_elapsed_ms,
        sample.window_bytes,
        sample.window_goodput_bytes_per_sec,
        sample.desired_file_lanes,
        file_results.len(),
        failed_file_results.len(),
        collect_info_results.len(),
    );
    Ok(FluxonFsTransferWorkerResultWire {
        job_id: assignment.job_id.clone(),
        batch_id: assignment.batch_id.clone(),
        worker_task_id: assignment.worker_task_id.clone(),
        worker_id: assignment.worker_id.clone(),
        file_results,
        failed_file_results,
        collect_info_results,
        final_telemetry,
    })
}

// Rename is the visibility boundary for one data file.
fn promote_prepared_transfer_file(
    dst_root: &PathBuf,
    file: PreparedTransferFile,
) -> Result<FluxonFsTransferWorkerFileResultWire, FlatDict> {
    let staging_abs = safe_join_root(dst_root.to_string_lossy().as_ref(), file.staging_relpath.as_str())
        .map_err(resp_err_kverr)?;
    let final_abs = safe_join_root(dst_root.to_string_lossy().as_ref(), file.final_relpath.as_str())
        .map_err(resp_err_kverr)?;
    rename_with_dst_parent_dir_chmod_retry(&staging_abs, &final_abs)?;
    Ok(FluxonFsTransferWorkerFileResultWire {
        relpath: file.final_relpath.clone(),
        staging_relpath: file.staging_relpath,
        final_relpath: file.final_relpath,
        visible_size: file.visible_size,
    })
}

// Collect-info materialization uses the same staging/promotion contract as data
// files so metadata-like outputs do not appear partially written either.
fn prepare_transfer_collect_info_materialization(
    dst_root: &PathBuf,
    batch_id: &str,
    worker_task_id: &str,
    collect_info: &FluxonFsTransferBatchCollectInfoWire,
) -> Result<PreparedTransferCollectInfo, FlatDict> {
    let output_relpath = transfer_collect_info_output_relpath(batch_id, collect_info.collect_kind)
        .map_err(|detail| {
            resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "build transfer collect info output relpath failed: batch_id={} err={}",
                    batch_id, detail
                ),
            }))
        })?;
    let staging_relpath = transfer_collect_info_staging_relpath(
        batch_id,
        worker_task_id,
        collect_info.collect_kind,
    )?;
    ensure_transfer_parent_dirs(dst_root, staging_relpath.as_str())?;
    ensure_transfer_parent_dirs(dst_root, output_relpath.as_str())?;
    let staging_abs = safe_join_root(dst_root.to_string_lossy().as_ref(), staging_relpath.as_str())
        .map_err(resp_err_kverr)?;
    let mut dst_file = open_create_file_with_parent_dir_chmod_retry(&staging_abs)?;
    dst_file.set_len(0).map_err(resp_err_io)?;
    dst_file
        .write_all(collect_info.collect_blob.as_slice())
        .map_err(resp_err_io)?;
    dst_file.sync_data().map_err(resp_err_io)?;
    drop(dst_file);
    Ok(PreparedTransferCollectInfo {
        collect_kind: collect_info.collect_kind,
        staging_relpath,
        output_relpath,
        materialized_bytes: collect_info.collect_blob.len() as i64,
    })
}

fn transfer_collect_info_staging_relpath(
    batch_id: &str,
    worker_task_id: &str,
    collect_kind: FluxonFsTransferCollectInfoKind,
) -> Result<String, FlatDict> {
    let output_relpath = transfer_collect_info_output_relpath(batch_id, collect_kind).map_err(|detail| {
        resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "build transfer collect info output relpath failed: batch_id={} err={}",
                batch_id, detail
            ),
        }))
    })?;
    Ok(format!("{}.{}.fluxon.part", output_relpath, worker_task_id))
}

fn prune_empty_parent_dirs(mut current: PathBuf, root: &PathBuf) -> Result<(), FlatDict> {
    while current != *root {
        match fs::remove_dir(&current) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) if err.kind() == ErrorKind::DirectoryNotEmpty => break,
            Err(err) => return Err(resp_err_io(err)),
        }
        let Some(parent) = current.parent() else {
            break;
        };
        current = parent.to_path_buf();
    }
    Ok(())
}

fn cleanup_attempt_staging_prefix(dst_root: &PathBuf, staging_prefix: &str) -> Result<(), FlatDict> {
    let staging_abs =
        safe_join_root(dst_root.to_string_lossy().as_ref(), staging_prefix).map_err(resp_err_kverr)?;
    match fs::remove_dir_all(&staging_abs) {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(resp_err_io(err)),
    }
    let Some(parent) = staging_abs.parent() else {
        return Ok(());
    };
    prune_empty_parent_dirs(parent.to_path_buf(), dst_root)
}

fn cleanup_attempt_collect_info_staging_files(
    dst_root: &PathBuf,
    assignment: &FluxonFsTransferWorkerAssignmentWire,
) -> Result<(), FlatDict> {
    for collect_info in &assignment.collect_infos {
        let staging_relpath = transfer_collect_info_staging_relpath(
            assignment.batch_id.as_str(),
            assignment.worker_task_id.as_str(),
            collect_info.collect_kind,
        )?;
        let staging_abs = safe_join_root(
            dst_root.to_string_lossy().as_ref(),
            staging_relpath.as_str(),
        )
        .map_err(resp_err_kverr)?;
        match fs::remove_file(&staging_abs) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => return Err(resp_err_io(err)),
        }
        let Some(parent) = staging_abs.parent() else {
            continue;
        };
        prune_empty_parent_dirs(parent.to_path_buf(), dst_root)?;
    }
    Ok(())
}

// Every worker attempt owns exactly one task-scoped staging subtree. Cleaning
// it on terminal exit prevents stale `.fluxon.stage/...` artifacts from
// polluting the visible destination tree after retries or supersession.
pub(crate) fn cleanup_transfer_worker_attempt_artifacts(
    dst_root: &PathBuf,
    assignment: &FluxonFsTransferWorkerAssignmentWire,
) -> Result<(), FlatDict> {
    cleanup_attempt_staging_prefix(dst_root, assignment.staging_prefix.as_str())?;
    cleanup_attempt_collect_info_staging_files(dst_root, assignment)?;
    Ok(())
}

fn log_transfer_worker_cleanup_failure(
    phase: &str,
    assignment: &FluxonFsTransferWorkerAssignmentWire,
    resp: &FlatDict,
) {
    tracing::warn!(
        "transfer worker cleanup failed: phase={} job_id={} batch_id={} worker_id={} worker_task_id={} resp={:?}",
        phase,
        assignment.job_id,
        assignment.batch_id,
        assignment.worker_id,
        assignment.worker_task_id,
        resp
    );
}

// Rename is the visibility boundary for one collect-info object.
fn promote_prepared_transfer_collect_info(
    dst_root: &PathBuf,
    collect_info: PreparedTransferCollectInfo,
) -> Result<FluxonFsTransferWorkerCollectInfoResultWire, FlatDict> {
    let staging_abs = safe_join_root(
        dst_root.to_string_lossy().as_ref(),
        collect_info.staging_relpath.as_str(),
    )
    .map_err(resp_err_kverr)?;
    let output_abs = safe_join_root(
        dst_root.to_string_lossy().as_ref(),
        collect_info.output_relpath.as_str(),
    )
    .map_err(resp_err_kverr)?;
    rename_with_dst_parent_dir_chmod_retry(&staging_abs, &output_abs)?;
    Ok(FluxonFsTransferWorkerCollectInfoResultWire {
        collect_kind: collect_info.collect_kind,
        output_relpath: collect_info.output_relpath,
        materialized_bytes: collect_info.materialized_bytes,
    })
}

pub(crate) fn read_transfer_chunk_from_root_dir_abs(
    root_dir_abs: &str,
    relpath: &str,
    offset: i64,
    length: i64,
) -> Result<Vec<u8>, FlatDict> {
    if offset < 0 || length < 0 || (length as usize) > CHUNK_BYTES {
        return Err(resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "transfer read offset/length out of range: offset={} length={}",
                offset, length
            ),
        })));
    }
    let p = safe_join_root(root_dir_abs, relpath).map_err(resp_err_kverr)?;
    let mut f = open_file_with_target_path_chmod_retry(&p, "read_transfer_chunk")?;
    let md = f.metadata().map_err(resp_err_io)?;
    let size = md.len().min(i64::MAX as u64) as i64;
    if offset >= size {
        return Ok(Vec::new());
    }
    let to_read = std::cmp::min(length, size - offset) as usize;
    f.seek(SeekFrom::Start(offset as u64)).map_err(resp_err_io)?;
    let mut buf = vec![0u8; to_read];
    f.read_exact(&mut buf).map_err(resp_err_io)?;
    Ok(buf)
}

// Worker execution trusts the persisted manifest instead of rescanning the
// batch root. This keeps retries deterministic and aligned with what the
// scheduler/store already committed as the batch definition.
pub(crate) fn execute_transfer_worker_assignment<ReadChunkFn, CheckpointFn>(
    assignment: &FluxonFsTransferWorkerAssignmentWire,
    dst_root: &PathBuf,
    checkpoint_continue: CheckpointFn,
    read_chunk: ReadChunkFn,
) -> Result<FluxonFsTransferWorkerResultWire, TransferWorkerExecutionError>
where
    ReadChunkFn:
        Fn(&FluxonFsTransferManifestEntryWire, i64, i64) -> Result<Vec<u8>, TransferWorkerExecutionError>
            + Send
            + Sync
            + 'static,
    CheckpointFn: Fn() -> Result<(), TransferWorkerExecutionError> + Send + Sync + 'static,
{
    execute_transfer_worker_assignment_with_policy(
        assignment,
        dst_root,
        TransferWorkerLanePolicy::production_default(),
        checkpoint_continue,
        read_chunk,
    )
}

pub(super) fn handle_transfer_scan(
    api: Arc<FluxonUserApi>,
    master_id: &str,
    exports: &AgentExportsHandle,
    registry: &TransferScanRegistryHandle,
    payload: FlatDict,
) -> FlatDict {
    let assignment = match parse_transfer_scan_assignment_payload(&payload) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    match registry.launch_task(api, master_id, exports, assignment) {
        Ok(result) => encode_transfer_scan_launch_result(&result, "transfer scan launch result"),
        Err(resp) => resp,
    }
}

pub(super) fn handle_transfer_read(exports: &AgentExportsHandle, payload: FlatDict) -> FlatDict {
    let export = match require_transfer_payload_str(&payload, "export") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let relpath = match require_transfer_payload_str(&payload, "relpath") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let offset = match require_transfer_payload_i64(&payload, "offset") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let length = match require_transfer_payload_i64(&payload, "length") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if offset < 0 || length < 0 || (length as usize) > CHUNK_BYTES {
        return resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "transfer read offset/length out of range: offset={} length={}",
                offset, length
            ),
        }));
    }
    let root_dir_abs = match exports.export_root_dir_abs(export.as_str()) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let buf =
        match read_transfer_chunk_from_root_dir_abs(root_dir_abs.as_str(), relpath.as_str(), offset, length) {
            Ok(v) => v,
            Err(resp) => return resp,
        };
    resp_ok(BTreeMap::from([("data".to_string(), FlatValue::Bytes(buf))]))
}

pub(super) fn handle_transfer_stream_open(
    exports: &AgentExportsHandle,
    registry: &TransferReadStreamRegistryHandle,
    payload: FlatDict,
) -> FlatDict {
    let open = match parse_transfer_stream_open_payload(&payload) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let root_dir_abs = match exports.export_root_dir_abs(open.export.as_str()) {
        Ok(v) => v,
        Err(e) => return resp_err_kverr(e),
    };
    let result = match registry.open_stream(root_dir_abs.as_str(), open) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    encode_transfer_stream_open_result_payload(&result)
}

pub(super) fn handle_transfer_stream_next(
    registry: &TransferReadStreamRegistryHandle,
    payload: FlatDict,
) -> FlatDict {
    let req = match parse_transfer_stream_next_payload(&payload) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let result = match registry.next_chunk(req) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    encode_transfer_stream_next_result_payload(&result)
}

pub(super) fn handle_transfer_stream_close(
    registry: &TransferReadStreamRegistryHandle,
    payload: FlatDict,
) -> FlatDict {
    let req = match parse_transfer_stream_close_payload(&payload) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    registry.close_stream(req.stream_id.as_str());
    resp_ok(BTreeMap::new())
}

pub(super) fn handle_transfer_worker(
    api: Arc<FluxonUserApi>,
    master_id: &str,
    exports: &AgentExportsHandle,
    registry: &TransferWorkerRegistryHandle,
    payload: FlatDict,
) -> FlatDict {
    let assignment = match parse_transfer_worker_assignment_payload(&payload) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    match registry.launch_task(api, master_id, exports, assignment) {
        Ok(result) => encode_transfer_worker_launch_result(&result, "transfer worker launch result"),
        Err(resp) => resp,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use fluxon_fs_core::config::{
        FluxonFsExport, FluxonFsGlobalConfig, FluxonFsTransferDispositionWire,
        agent_registry_export_for_name_and_root_v1,
    };
    use tempfile::TempDir;

    fn test_export(root_dir_abs: &str) -> FluxonFsExport {
        agent_registry_export_for_name_and_root_v1("src", root_dir_abs)
    }

    fn test_exports_handle(root_dir_abs: &str) -> AgentExportsHandle {
        let mut exports = BTreeMap::new();
        exports.insert("src".to_string(), test_export(root_dir_abs));
        AgentExportsHandle::new_from_static_cfg(
            &FluxonFsGlobalConfig {
                stale_window_ms: 1,
                write_session_target_inflight_bytes: 64 * 1024 * 1024,
                rules: Vec::new(),
                exports,
            },
            BTreeMap::new(),
        )
    }

    fn write_file(root: &TempDir, relpath: &str, data: &[u8]) {
        let abs = root.path().join(relpath);
        let parent = abs.parent().unwrap();
        fs::create_dir_all(parent).unwrap();
        let mut f = fs::File::create(abs).unwrap();
        f.write_all(data).unwrap();
        f.sync_all().unwrap();
    }

    #[cfg(unix)]
    fn write_symlink(root: &TempDir, relpath: &str, target: &str) {
        let abs = root.path().join(relpath);
        let parent = abs.parent().unwrap();
        fs::create_dir_all(parent).unwrap();
        std::os::unix::fs::symlink(target, abs).unwrap();
    }

    #[cfg(unix)]
    fn chmod_mode(path: &Path, mode: u32) {
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(mode);
        fs::set_permissions(path, permissions).unwrap();
    }

    fn decode_result_json(resp: &FlatDict) -> FluxonFsTransferScanResultWire {
        let FlatValue::String(result_json) = resp.get("result_json").unwrap() else {
            panic!("missing result_json");
        };
        serde_json::from_str(result_json).unwrap()
    }

    fn child_scan_unit_roots(result: &FluxonFsTransferScanResultWire) -> Vec<String> {
        result
            .child_scan_units
            .iter()
            .map(|child| child.root_relpath.clone())
            .collect()
    }

    fn assert_all_child_scan_units_are_subtree_streaming(
        result: &FluxonFsTransferScanResultWire,
    ) {
        assert!(result
            .child_scan_units
            .iter()
            .all(|child| child.scan_mode == FluxonFsTransferScanMode::SubtreeStreaming));
    }

    fn ok_bool(resp: &FlatDict) -> bool {
        matches!(resp.get("ok"), Some(FlatValue::Bool(true)))
    }

    fn decode_open_result_payload(resp: &FlatDict) -> FluxonFsTransferReadStreamOpenResultWire {
        match decode_transfer_stream_open_result_payload(resp) {
            Ok(v) => v,
            Err(TransferWorkerRpcFailure::Fatal(other)) => {
                panic!("unexpected open result fatal decode error: {:?}", other)
            }
            Err(TransferWorkerRpcFailure::Retryable { detail }) => {
                panic!(
                    "unexpected open result retryable decode error: {}",
                    detail
                )
            }
        }
    }

    fn decode_next_result_payload(resp: &FlatDict) -> FluxonFsTransferReadStreamNextResultWire {
        match decode_transfer_stream_next_result_payload(resp) {
            Ok(v) => v,
            Err(TransferWorkerRpcFailure::Fatal(other)) => {
                panic!("unexpected next result fatal decode error: {:?}", other)
            }
            Err(TransferWorkerRpcFailure::Retryable { detail }) => {
                panic!(
                    "unexpected next result retryable decode error: {}",
                    detail
                )
            }
        }
    }

    fn decode_symlink_notice_collect_blob(
        blob: &[u8],
    ) -> Vec<FluxonFsTransferSymlinkNoticeEntryWire> {
        std::str::from_utf8(blob)
            .unwrap()
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn test_worker_assignment(
        relpath: &str,
        size: i64,
    ) -> FluxonFsTransferWorkerAssignmentWire {
        FluxonFsTransferWorkerAssignmentWire {
            job_id: "job".to_string(),
            batch_id: "batch".to_string(),
            worker_task_id: "task".to_string(),
            worker_id: "worker-0".to_string(),
            batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
            src_export: "src".to_string(),
            dst_export: "dst".to_string(),
            src_exporter_id: "src-exporter".to_string(),
            dst_exporter_id: "dst-exporter".to_string(),
            dst_root_relpath: ".".to_string(),
            root_relpath: ".".to_string(),
            staging_prefix: ".fluxon.stage/job/batch".to_string(),
            lease_expire_unix_ms: 0,
            manifest_blob: build_transfer_manifest_blob(vec![
                FluxonFsTransferScanFrontierEntry {
                    relpath: relpath.to_string(),
                    size,
                },
            ], Vec::new())
            .unwrap(),
            collect_infos: Vec::new(),
        }
    }

    fn test_worker_log_context() -> TransferWorkerLogContext {
        TransferWorkerLogContext {
            job_id: "job".to_string(),
            batch_id: "batch".to_string(),
            worker_id: "worker-0".to_string(),
            worker_task_id: "task".to_string(),
        }
    }

    fn test_transfer_coordinator<ReadChunkFn, CheckpointFn>(
        checkpoint_continue: CheckpointFn,
        read_chunk: ReadChunkFn,
    ) -> TransferWorkerCoordinator<ReadChunkFn, CheckpointFn>
    where
        ReadChunkFn: Fn(&FluxonFsTransferManifestEntryWire, i64, i64) -> Result<Vec<u8>, TransferWorkerExecutionError>,
        CheckpointFn: Fn() -> Result<(), TransferWorkerExecutionError>,
    {
        let policy = Arc::new(TransferWorkerLanePolicy::production_default());
        let progress = Arc::new(TransferWorkerProgressWindow::new(
            Arc::new(policy.as_ref().normalized()),
            0,
        ));
        TransferWorkerCoordinator::new(
            test_worker_log_context(),
            policy,
            progress,
            checkpoint_continue,
            read_chunk,
        )
    }

    #[test]
    fn normalize_child_relpath_uses_dot_as_root_only() {
        assert_eq!(normalize_child_relpath(".", "a"), "a");
        assert_eq!(normalize_child_relpath("x", "a"), "x/a");
        assert_eq!(normalize_child_relpath("x/", "a"), "x/a");
    }

    #[test]
    fn transfer_staging_file_relpath_appends_fluxon_part_suffix() {
        assert_eq!(
            transfer_staging_file_relpath(".fluxon.stage/job/batch", "a/b.bin").unwrap(),
            ".fluxon.stage/job/batch/a/b.bin/b.bin.fluxon.part"
        );
    }

    #[test]
    fn build_transfer_manifest_blob_round_trips_entries() {
        let blob = build_transfer_manifest_blob(vec![
            FluxonFsTransferScanFrontierEntry {
                relpath: "a".to_string(),
                size: 1,
            },
            FluxonFsTransferScanFrontierEntry {
                relpath: "b/c".to_string(),
                size: 2,
            },
        ], vec!["empty".to_string()])
        .unwrap();
        let manifest = FluxonFsTransferManifestWire::decode_from_blob(&blob).unwrap();
        assert_eq!(manifest.entry_count, 2);
        assert_eq!(manifest.total_bytes, 3);
        assert_eq!(
            manifest.entries,
            vec![
                FluxonFsTransferManifestEntryWire {
                    relpath: "a".to_string(),
                    size: 1,
                },
                FluxonFsTransferManifestEntryWire {
                    relpath: "b/c".to_string(),
                    size: 2,
                },
            ]
        );
        assert_eq!(manifest.empty_dir_relpaths, vec!["empty".to_string()]);
    }

    #[test]
    fn materialize_transfer_collect_info_writes_task_scoped_staging_then_output_file() {
        let root = TempDir::new().unwrap();
        let collect_infos = build_symlink_collect_infos(vec![FluxonFsTransferSymlinkNoticeEntryWire {
            relpath: "root/link-file.bin".to_string(),
            link_target: "target/file.bin".to_string(),
        }])
        .unwrap();
        let prepared = prepare_transfer_collect_info_materialization(
            &root.path().to_path_buf(),
            "batch-1",
            "task-1",
            &collect_infos[0],
        )
        .unwrap();
        assert_eq!(
            prepared.staging_relpath,
            "fluxon_collect_info/batches/batch-1/symlinks.jsonl.task-1.fluxon.part"
        );
        let result =
            promote_prepared_transfer_collect_info(&root.path().to_path_buf(), prepared).unwrap();
        assert_eq!(
            result.output_relpath,
            "fluxon_collect_info/batches/batch-1/symlinks.jsonl"
        );
        let written = fs::read(root.path().join(result.output_relpath)).unwrap();
        assert_eq!(written, collect_infos[0].collect_blob);
    }

    #[test]
    fn collect_transfer_tree_returns_sorted_recursive_files() {
        let root = TempDir::new().unwrap();
        write_file(&root, "root/z.bin", b"z");
        write_file(&root, "root/a/x.bin", b"xx");
        fs::create_dir_all(root.path().join("root/a/empty")).unwrap();

        let tree =
            collect_transfer_tree(root.path().to_str().unwrap(), "root", &Vec::new()).unwrap();
        assert_eq!(
            tree.files,
            vec![
                FluxonFsTransferScanFrontierEntry {
                    relpath: "root/a/x.bin".to_string(),
                    size: 2,
                },
                FluxonFsTransferScanFrontierEntry {
                    relpath: "root/z.bin".to_string(),
                    size: 1,
                },
            ]
        );
        assert_eq!(tree.empty_dirs, vec!["root/a/empty".to_string()]);
        assert!(tree.symlink_notices.is_empty());
    }

    #[test]
    fn collect_transfer_tree_respects_skip_entries() {
        let root = TempDir::new().unwrap();
        write_file(&root, "root/keep/z.bin", b"z");
        write_file(&root, "root/skipdir/a.bin", b"a");
        write_file(&root, "root/keep/skip.bin", b"s");

        let tree = collect_transfer_tree(
            root.path().to_str().unwrap(),
            "root",
            &vec![
                FluxonFsTransferSkipEntryWire {
                    kind: FluxonFsTransferSkipEntryKind::Dir,
                    relpath: "root/skipdir".to_string(),
                },
                FluxonFsTransferSkipEntryWire {
                    kind: FluxonFsTransferSkipEntryKind::File,
                    relpath: "root/keep/skip.bin".to_string(),
                },
            ],
        )
        .unwrap();
        assert_eq!(
            tree.files,
            vec![FluxonFsTransferScanFrontierEntry {
                relpath: "root/keep/z.bin".to_string(),
                size: 1,
            }]
        );
        assert!(tree.symlink_notices.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn collect_transfer_tree_skips_file_and_directory_symlinks() {
        let root = TempDir::new().unwrap();
        write_file(&root, "root/real/file.bin", b"abc");
        write_file(&root, "root/real/sub/leaf.bin", b"xy");
        write_symlink(&root, "root/link-file.bin", "real/file.bin");
        write_symlink(&root, "root/link-dir", "real/sub");

        let tree =
            collect_transfer_tree(root.path().to_str().unwrap(), "root", &Vec::new()).unwrap();
        assert_eq!(
            tree.files,
            vec![
                FluxonFsTransferScanFrontierEntry {
                    relpath: "root/real/file.bin".to_string(),
                    size: 3,
                },
                FluxonFsTransferScanFrontierEntry {
                    relpath: "root/real/sub/leaf.bin".to_string(),
                    size: 2,
                },
            ]
        );
        assert_eq!(
            tree.symlink_notices,
            vec![
                FluxonFsTransferSymlinkNoticeEntryWire {
                    relpath: "root/link-dir".to_string(),
                    link_target: "real/sub".to_string(),
                },
                FluxonFsTransferSymlinkNoticeEntryWire {
                    relpath: "root/link-file.bin".to_string(),
                    link_target: "real/file.bin".to_string(),
                },
            ]
        );
    }

    #[test]
    fn handle_transfer_read_reads_slice_and_eof_as_empty() {
        let root = TempDir::new().unwrap();
        write_file(&root, "f.bin", b"abcdef");
        let exports = test_exports_handle(root.path().to_str().unwrap());

        let read_resp = handle_transfer_read(
            &exports,
            FlatDict::from([
                ("export".to_string(), FlatValue::String("src".to_string())),
                ("relpath".to_string(), FlatValue::String("f.bin".to_string())),
                ("offset".to_string(), FlatValue::Int64(2)),
                ("length".to_string(), FlatValue::Int64(3)),
            ]),
        );
        assert!(ok_bool(&read_resp));
        match read_resp.get("data").unwrap() {
            FlatValue::Bytes(v) => assert_eq!(v.as_slice(), b"cde"),
            other => panic!("unexpected data response: {:?}", other),
        }

        let eof_resp = handle_transfer_read(
            &exports,
            FlatDict::from([
                ("export".to_string(), FlatValue::String("src".to_string())),
                ("relpath".to_string(), FlatValue::String("f.bin".to_string())),
                ("offset".to_string(), FlatValue::Int64(6)),
                ("length".to_string(), FlatValue::Int64(1)),
            ]),
        );
        assert!(ok_bool(&eof_resp));
        match eof_resp.get("data").unwrap() {
            FlatValue::Bytes(v) => assert!(v.is_empty()),
            other => panic!("unexpected eof response: {:?}", other),
        }
    }

    #[cfg(unix)]
    #[test]
    fn handle_transfer_read_best_effort_recovers_permission_denied_file() {
        let root = TempDir::new().unwrap();
        write_file(&root, "f.bin", b"abcdef");
        let file = root.path().join("f.bin");
        chmod_mode(&file, 0o000);
        let exports = test_exports_handle(root.path().to_str().unwrap());

        let read_resp = handle_transfer_read(
            &exports,
            FlatDict::from([
                ("export".to_string(), FlatValue::String("src".to_string())),
                ("relpath".to_string(), FlatValue::String("f.bin".to_string())),
                ("offset".to_string(), FlatValue::Int64(1)),
                ("length".to_string(), FlatValue::Int64(3)),
            ]),
        );
        chmod_mode(&file, 0o755);
        assert!(ok_bool(&read_resp));
        match read_resp.get("data").unwrap() {
            FlatValue::Bytes(v) => assert_eq!(v.as_slice(), b"bcd"),
            other => panic!("unexpected data response: {:?}", other),
        }
    }

    #[test]
    fn handle_transfer_read_rejects_length_larger_than_chunk_bytes() {
        let root = TempDir::new().unwrap();
        write_file(&root, "f.bin", b"abc");
        let exports = test_exports_handle(root.path().to_str().unwrap());

        let resp = handle_transfer_read(
            &exports,
            FlatDict::from([
                ("export".to_string(), FlatValue::String("src".to_string())),
                ("relpath".to_string(), FlatValue::String("f.bin".to_string())),
                ("offset".to_string(), FlatValue::Int64(0)),
                ("length".to_string(), FlatValue::Int64(CHUNK_BYTES as i64 + 1)),
            ]),
        );
        assert!(matches!(resp.get("ok"), Some(FlatValue::Bool(false))));
    }

    #[test]
    fn handle_transfer_stream_next_replays_same_offset_and_close_marks_missing() {
        let root = TempDir::new().unwrap();
        write_file(&root, "f.bin", b"abcdef");
        let exports = test_exports_handle(root.path().to_str().unwrap());
        let stream_registry = TransferReadStreamRegistryHandle::new();

        let open_resp = handle_transfer_stream_open(
            &exports,
            &stream_registry,
            FlatDict::from([
                (
                    "worker_task_id".to_string(),
                    FlatValue::String("task-0".to_string()),
                ),
                ("export".to_string(), FlatValue::String("src".to_string())),
                ("relpath".to_string(), FlatValue::String("f.bin".to_string())),
                ("initial_offset".to_string(), FlatValue::Int64(0)),
            ]),
        );
        assert!(ok_bool(&open_resp));
        let open_result = decode_open_result_payload(&open_resp);
        assert_eq!(open_result.size, 6);

        let next_payload = |next_offset: i64, length: i64| {
            FlatDict::from([
                (
                    "stream_id".to_string(),
                    FlatValue::String(open_result.stream_id.clone()),
                ),
                ("next_offset".to_string(), FlatValue::Int64(next_offset)),
                ("length".to_string(), FlatValue::Int64(length)),
            ])
        };

        let first_resp = handle_transfer_stream_next(&stream_registry, next_payload(0, 3));
        assert!(ok_bool(&first_resp));
        let first = decode_next_result_payload(&first_resp);
        assert!(!first.stream_missing);
        assert_eq!(first.data, b"abc".to_vec());

        let replay_resp = handle_transfer_stream_next(&stream_registry, next_payload(0, 3));
        assert!(ok_bool(&replay_resp));
        let replay = decode_next_result_payload(&replay_resp);
        assert!(!replay.stream_missing);
        assert_eq!(replay.data, b"abc".to_vec());

        let second_resp = handle_transfer_stream_next(&stream_registry, next_payload(3, 3));
        assert!(ok_bool(&second_resp));
        let second = decode_next_result_payload(&second_resp);
        assert!(!second.stream_missing);
        assert_eq!(second.data, b"def".to_vec());

        let eof_resp = handle_transfer_stream_next(&stream_registry, next_payload(6, 1));
        assert!(ok_bool(&eof_resp));
        let eof = decode_next_result_payload(&eof_resp);
        assert!(!eof.stream_missing);
        assert!(eof.data.is_empty());

        let eof_replay_resp = handle_transfer_stream_next(&stream_registry, next_payload(6, 1));
        assert!(ok_bool(&eof_replay_resp));
        let eof_replay = decode_next_result_payload(&eof_replay_resp);
        assert!(!eof_replay.stream_missing);
        assert!(eof_replay.data.is_empty());

        let close_resp = handle_transfer_stream_close(
            &stream_registry,
            FlatDict::from([(
                "stream_id".to_string(),
                FlatValue::String(open_result.stream_id.clone()),
            )]),
        );
        assert!(ok_bool(&close_resp));

        let missing_resp = handle_transfer_stream_next(&stream_registry, next_payload(6, 1));
        assert!(ok_bool(&missing_resp));
        let missing = decode_next_result_payload(&missing_resp);
        assert!(missing.stream_missing);
        assert!(missing.data.is_empty());
    }

    #[test]
    fn handle_transfer_stream_next_rejects_non_sequential_offset_after_prefetch() {
        let root = TempDir::new().unwrap();
        write_file(&root, "f.bin", b"abcdefgh");
        let exports = test_exports_handle(root.path().to_str().unwrap());
        let stream_registry = TransferReadStreamRegistryHandle::new();

        let open_resp = handle_transfer_stream_open(
            &exports,
            &stream_registry,
            FlatDict::from([
                (
                    "worker_task_id".to_string(),
                    FlatValue::String("task-1".to_string()),
                ),
                ("export".to_string(), FlatValue::String("src".to_string())),
                ("relpath".to_string(), FlatValue::String("f.bin".to_string())),
                ("initial_offset".to_string(), FlatValue::Int64(0)),
            ]),
        );
        assert!(ok_bool(&open_resp));
        let open_result = decode_open_result_payload(&open_resp);

        let first_resp = handle_transfer_stream_next(
            &stream_registry,
            FlatDict::from([
                (
                    "stream_id".to_string(),
                    FlatValue::String(open_result.stream_id.clone()),
                ),
                ("next_offset".to_string(), FlatValue::Int64(0)),
                ("length".to_string(), FlatValue::Int64(3)),
            ]),
        );
        assert!(ok_bool(&first_resp));
        let first = decode_next_result_payload(&first_resp);
        assert_eq!(first.data, b"abc".to_vec());

        let invalid_resp = handle_transfer_stream_next(
            &stream_registry,
            FlatDict::from([
                (
                    "stream_id".to_string(),
                    FlatValue::String(open_result.stream_id),
                ),
                ("next_offset".to_string(), FlatValue::Int64(4)),
                ("length".to_string(), FlatValue::Int64(2)),
            ]),
        );
        assert!(matches!(invalid_resp.get("ok"), Some(FlatValue::Bool(false))));
    }

    #[test]
    fn handle_transfer_stream_open_resumes_from_initial_offset() {
        let root = TempDir::new().unwrap();
        write_file(&root, "f.bin", b"abcdefgh");
        let exports = test_exports_handle(root.path().to_str().unwrap());
        let stream_registry = TransferReadStreamRegistryHandle::new();

        let open_resp = handle_transfer_stream_open(
            &exports,
            &stream_registry,
            FlatDict::from([
                (
                    "worker_task_id".to_string(),
                    FlatValue::String("task-2".to_string()),
                ),
                ("export".to_string(), FlatValue::String("src".to_string())),
                ("relpath".to_string(), FlatValue::String("f.bin".to_string())),
                ("initial_offset".to_string(), FlatValue::Int64(3)),
            ]),
        );
        assert!(ok_bool(&open_resp));
        let open_result = decode_open_result_payload(&open_resp);
        assert_eq!(open_result.size, 8);

        let next_resp = handle_transfer_stream_next(
            &stream_registry,
            FlatDict::from([
                (
                    "stream_id".to_string(),
                    FlatValue::String(open_result.stream_id),
                ),
                ("next_offset".to_string(), FlatValue::Int64(3)),
                ("length".to_string(), FlatValue::Int64(3)),
            ]),
        );
        assert!(ok_bool(&next_resp));
        let next = decode_next_result_payload(&next_resp);
        assert!(!next.stream_missing);
        assert_eq!(next.data, b"def".to_vec());
    }

    #[cfg(unix)]
    #[test]
    fn handle_transfer_stream_open_best_effort_recovers_permission_denied_file() {
        let root = TempDir::new().unwrap();
        write_file(&root, "f.bin", b"abcdefgh");
        let file = root.path().join("f.bin");
        chmod_mode(&file, 0o000);
        let exports = test_exports_handle(root.path().to_str().unwrap());
        let stream_registry = TransferReadStreamRegistryHandle::new();

        let open_resp = handle_transfer_stream_open(
            &exports,
            &stream_registry,
            FlatDict::from([
                (
                    "worker_task_id".to_string(),
                    FlatValue::String("task-3".to_string()),
                ),
                ("export".to_string(), FlatValue::String("src".to_string())),
                ("relpath".to_string(), FlatValue::String("f.bin".to_string())),
                ("initial_offset".to_string(), FlatValue::Int64(3)),
            ]),
        );
        chmod_mode(&file, 0o755);
        assert!(ok_bool(&open_resp));
        let open_result = decode_open_result_payload(&open_resp);
        assert_eq!(open_result.size, 8);
    }

    #[test]
    fn handle_transfer_scan_assignment_short_circuits_full_dir_disposition() {
        let exports = test_exports_handle(TempDir::new().unwrap().path().to_str().unwrap());
        let assignment = FluxonFsTransferScanAssignmentWire {
            job_id: "job".to_string(),
            scan_epoch: 1,
            scan_unit_id: "scan".to_string(),
            scan_task_id: "task".to_string(),
            root_relpath: "root".to_string(),
            generation: 7,
            scan_mode: FluxonFsTransferScanMode::FullTree,
            src_export: "src".to_string(),
            src_exporter_id: "src-exporter".to_string(),
            batch_ready_bytes: 8,
            lease_expire_unix_ms: 0,
            known_dispositions: vec![FluxonFsTransferDispositionWire {
                root_relpath: "root".to_string(),
                generation: 7,
                batch_kind: FluxonFsTransferBatchKind::FullDir,
            }],
            live_child_scan_roots: Vec::new(),
            skip_entries: Vec::new(),
        };
        let resp = handle_transfer_scan_assignment(&exports, assignment);
        let result = decode_result_json(&resp);
        assert!(result.finished);
        assert!(result.direct_files_only_batches.is_empty());
        assert!(result.child_scan_units.is_empty());
        assert!(result.full_dir_batches.is_empty());
    }

    #[test]
    fn handle_transfer_scan_assignment_recomputes_direct_files_only_disposition() {
        let root = TempDir::new().unwrap();
        write_file(&root, "root/direct.bin", b"abc");
        write_file(&root, "root/child/file.bin", b"x");
        let exports = test_exports_handle(root.path().to_str().unwrap());
        let assignment = FluxonFsTransferScanAssignmentWire {
            job_id: "job".to_string(),
            scan_epoch: 1,
            scan_unit_id: "scan".to_string(),
            scan_task_id: "task".to_string(),
            root_relpath: "root".to_string(),
            generation: 7,
            scan_mode: FluxonFsTransferScanMode::FullTree,
            src_export: "src".to_string(),
            src_exporter_id: "src-exporter".to_string(),
            batch_ready_bytes: 8,
            lease_expire_unix_ms: 0,
            known_dispositions: vec![FluxonFsTransferDispositionWire {
                root_relpath: "root".to_string(),
                generation: 7,
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
            }],
            live_child_scan_roots: Vec::new(),
            skip_entries: Vec::new(),
        };
        let resp = handle_transfer_scan_assignment(&exports, assignment);
        let result = decode_result_json(&resp);
        assert!(result.finished);
        assert!(result.direct_files_only_batches.is_empty());
        assert_eq!(child_scan_unit_roots(&result), vec!["root/child".to_string()]);
        assert_all_child_scan_units_are_subtree_streaming(&result);
        assert!(result.full_dir_batches.is_empty());
    }

    #[test]
    fn handle_transfer_scan_assignment_classifies_direct_files_and_child_dirs() {
        let root = TempDir::new().unwrap();
        write_file(&root, "root/direct.bin", b"abc");
        write_file(&root, "root/big/data.bin", b"12345");
        write_file(&root, "root/small/child.bin", b"12");
        fs::create_dir_all(root.path().join("root/big/empty-nested")).unwrap();
        fs::create_dir_all(root.path().join("root/empty")).unwrap();
        let exports = test_exports_handle(root.path().to_str().unwrap());

        let resp = handle_transfer_scan_assignment(
            &exports,
            FluxonFsTransferScanAssignmentWire {
                job_id: "job".to_string(),
                scan_epoch: 1,
                scan_unit_id: "scan".to_string(),
                scan_task_id: "task".to_string(),
                root_relpath: "root".to_string(),
                generation: 3,
                scan_mode: FluxonFsTransferScanMode::FullTree,
                src_export: "src".to_string(),
                src_exporter_id: "src-exporter".to_string(),
                batch_ready_bytes: 4,
                lease_expire_unix_ms: 0,
                known_dispositions: Vec::new(),
                live_child_scan_roots: Vec::new(),
                skip_entries: Vec::new(),
            },
        );
        let result = decode_result_json(&resp);

        assert!(result.finished);
        assert_eq!(result.direct_files_only_batches.len(), 1);
        let direct_files_only_batch = &result.direct_files_only_batches[0];
        assert_eq!(direct_files_only_batch.root_relpath, "root".to_string());
        let direct_manifest =
            FluxonFsTransferManifestWire::decode_from_blob(&direct_files_only_batch.manifest_blob)
                .unwrap();
        assert_eq!(
            direct_manifest.entries,
            vec![FluxonFsTransferManifestEntryWire {
                relpath: "root/direct.bin".to_string(),
                size: 3,
            }]
        );
        assert_eq!(
            direct_manifest.empty_dir_relpaths,
            vec!["root/empty".to_string()]
        );
        assert_eq!(
            child_scan_unit_roots(&result),
            vec!["root/big".to_string(), "root/small".to_string()]
        );
        assert_all_child_scan_units_are_subtree_streaming(&result);
        assert!(result.full_dir_batches.is_empty());
    }

    #[test]
    fn handle_transfer_scan_assignment_groups_empty_children_into_direct_batch_without_direct_files() {
        let root = TempDir::new().unwrap();
        write_file(&root, "root/big/data.bin", b"12345");
        fs::create_dir_all(root.path().join("root/empty-a")).unwrap();
        fs::create_dir_all(root.path().join("root/empty-b")).unwrap();
        let exports = test_exports_handle(root.path().to_str().unwrap());

        let resp = handle_transfer_scan_assignment(
            &exports,
            FluxonFsTransferScanAssignmentWire {
                job_id: "job".to_string(),
                scan_epoch: 1,
                scan_unit_id: "scan".to_string(),
                scan_task_id: "task".to_string(),
                root_relpath: "root".to_string(),
                generation: 3,
                scan_mode: FluxonFsTransferScanMode::FullTree,
                src_export: "src".to_string(),
                src_exporter_id: "src-exporter".to_string(),
                batch_ready_bytes: 4,
                lease_expire_unix_ms: 0,
                known_dispositions: Vec::new(),
                live_child_scan_roots: Vec::new(),
                skip_entries: Vec::new(),
            },
        );
        let result = decode_result_json(&resp);

        assert!(result.finished);
        assert_eq!(result.direct_files_only_batches.len(), 1);
        assert_eq!(child_scan_unit_roots(&result), vec!["root/big".to_string()]);
        assert_all_child_scan_units_are_subtree_streaming(&result);
        assert!(result.full_dir_batches.is_empty());
        let direct_manifest =
            FluxonFsTransferManifestWire::decode_from_blob(&result.direct_files_only_batches[0].manifest_blob)
                .unwrap();
        assert!(direct_manifest.entries.is_empty());
        assert_eq!(
            direct_manifest.empty_dir_relpaths,
            vec!["root/empty-a".to_string(), "root/empty-b".to_string()]
        );
    }

    #[test]
    fn build_transfer_scan_result_large_empty_heavy_subtree_is_delegated_instead_of_merged() {
        let root = TempDir::new().unwrap();
        for idx in 0..(TRANSFER_MERGEABLE_EMPTY_DIR_BUDGET + 1) {
            fs::create_dir_all(root.path().join(format!("root/huge/empty-{idx:05}"))).unwrap();
        }
        let result = build_transfer_scan_result_for_root_dir_abs(
            root.path().to_str().unwrap(),
            &FluxonFsTransferScanAssignmentWire {
                job_id: "job".to_string(),
                scan_epoch: 1,
                scan_unit_id: "scan-empty-heavy".to_string(),
                scan_task_id: "task-empty-heavy".to_string(),
                root_relpath: "root".to_string(),
                generation: 1,
                scan_mode: FluxonFsTransferScanMode::FullTree,
                src_export: "src".to_string(),
                src_exporter_id: "src-exporter".to_string(),
                batch_ready_bytes: 8,
                lease_expire_unix_ms: 0,
                known_dispositions: Vec::new(),
                live_child_scan_roots: Vec::new(),
                skip_entries: Vec::new(),
            },
        )
        .unwrap();
        assert!(!result.finished);
        assert_eq!(result.direct_files_only_batches.len(), 1);
        assert_eq!(result.full_dir_batches.len(), 0);
        assert_eq!(result.child_scan_units.len(), 1);
        assert_eq!(result.child_scan_units[0].root_relpath, "root/huge".to_string());
        let manifest = FluxonFsTransferManifestWire::decode_from_blob(
            &result.direct_files_only_batches[0].manifest_blob,
        )
        .unwrap();
        assert!(manifest.entries.is_empty());
        assert!(!manifest.empty_dir_relpaths.is_empty());
        assert!(
            manifest.empty_dir_relpaths.len() <= TRANSFER_MERGEABLE_EMPTY_DIR_BUDGET
        );
        assert!(
            estimate_empty_dir_manifest_bytes(&manifest.empty_dir_relpaths)
                <= TRANSFER_MERGEABLE_EMPTY_DIR_ESTIMATED_BYTES_BUDGET
        );
    }

    #[test]
    fn build_transfer_scan_result_empty_heavy_root_uses_byte_budget_splitting() {
        let root = TempDir::new().unwrap();
        let child_count = (TRANSFER_MERGEABLE_EMPTY_DIR_ESTIMATED_BYTES_BUDGET
            / estimate_empty_dir_manifest_entry_bytes(
                format!("root/branch-00000/{}", "x".repeat(200)).as_str(),
            ))
            + 1;
        for idx in 0..child_count {
            fs::create_dir_all(root.path().join(format!(
                "root/branch-{idx:05}/{}",
                "x".repeat(200)
            )))
            .unwrap();
        }
        let result = build_transfer_scan_result_for_root_dir_abs(
            root.path().to_str().unwrap(),
            &FluxonFsTransferScanAssignmentWire {
                job_id: "job".to_string(),
                scan_epoch: 1,
                scan_unit_id: "scan-empty-byte-budget".to_string(),
                scan_task_id: "task-empty-byte-budget".to_string(),
                root_relpath: "root".to_string(),
                generation: 1,
                scan_mode: FluxonFsTransferScanMode::FullTree,
                src_export: "src".to_string(),
                src_exporter_id: "src-exporter".to_string(),
                batch_ready_bytes: 8,
                lease_expire_unix_ms: 0,
                known_dispositions: Vec::new(),
                live_child_scan_roots: Vec::new(),
                skip_entries: Vec::new(),
            },
        )
        .unwrap();
        assert!(!result.finished);
        assert!(result.direct_files_only_batches.is_empty());
        assert!(!result.child_scan_units.is_empty());
        assert!(result
            .child_scan_units
            .iter()
            .any(|child| child.scan_mode == FluxonFsTransferScanMode::FullTree));
        assert!(result.child_scan_units.iter().all(|child| {
            child.scan_mode == FluxonFsTransferScanMode::FullTree
                || child.scan_mode == FluxonFsTransferScanMode::SubtreeStreaming
        }));
        assert!(result.full_dir_batches.is_empty());
    }

    #[test]
    fn build_transfer_scan_events_for_result_flushes_payload_before_empty_finished() {
        let assignment = FluxonFsTransferScanAssignmentWire {
            job_id: "job".to_string(),
            scan_epoch: 1,
            scan_unit_id: "scan".to_string(),
            scan_task_id: "task".to_string(),
            root_relpath: "root".to_string(),
            generation: 1,
            scan_mode: FluxonFsTransferScanMode::FullTree,
            src_export: "src".to_string(),
            src_exporter_id: "src-exporter".to_string(),
            batch_ready_bytes: 8,
            lease_expire_unix_ms: 0,
            known_dispositions: Vec::new(),
            live_child_scan_roots: Vec::new(),
            skip_entries: Vec::new(),
        };
        let batch = FluxonFsTransferScanBatchWire {
            batch_id: "batch".to_string(),
            root_relpath: "root".to_string(),
            batch_kind: FluxonFsTransferBatchKind::FullDir,
            manifest_blob: vec![1, 2, 3],
            collect_infos: Vec::new(),
            generation: 1,
        };
        let result = FluxonFsTransferScanResultWire {
            job_id: assignment.job_id.clone(),
            scan_epoch: assignment.scan_epoch,
            scan_unit_id: assignment.scan_unit_id.clone(),
            scan_task_id: assignment.scan_task_id.clone(),
            root_relpath: assignment.root_relpath.clone(),
            generation: assignment.generation,
            frontier: empty_transfer_scan_frontier(),
            direct_files_only_batches: Vec::new(),
            child_scan_units: Vec::new(),
            full_dir_batches: vec![batch],
            finished: true,
        };
        let (events, continue_locally, next_event_seq_no) =
            build_transfer_scan_events_for_result(&assignment, 7, result);
        assert!(!continue_locally);
        assert_eq!(next_event_seq_no, 9);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_kind, FluxonFsTransferScanEventKindWire::Append);
        assert_eq!(events[0].event_seq_no, 7);
        assert_eq!(events[0].full_dir_batches.len(), 1);
        assert_eq!(events[1].event_kind, FluxonFsTransferScanEventKindWire::Finished);
        assert_eq!(events[1].event_seq_no, 8);
        assert!(events[1].direct_files_only_batches.is_empty());
        assert!(events[1].child_scan_units.is_empty());
        assert!(events[1].full_dir_batches.is_empty());
    }

    #[test]
    fn build_transfer_scan_result_resumes_root_listing_from_same_scan_unit_id() {
        let root = TempDir::new().unwrap();
        for idx in 0..(TRANSFER_SCAN_ROOT_LISTING_SLICE_ENTRY_LIMIT + 1) {
            let relpath = format!("root/file-{idx:05}.bin");
            write_file(&root, relpath.as_str(), b"x");
        }
        let assignment = FluxonFsTransferScanAssignmentWire {
            job_id: "job".to_string(),
            scan_epoch: 1,
            scan_unit_id: "scan-root-cont".to_string(),
            scan_task_id: "task-1".to_string(),
            root_relpath: "root".to_string(),
            generation: 1,
            scan_mode: FluxonFsTransferScanMode::FullTree,
            src_export: "src".to_string(),
            src_exporter_id: "src-exporter".to_string(),
            batch_ready_bytes: 8,
            lease_expire_unix_ms: 0,
            known_dispositions: Vec::new(),
            live_child_scan_roots: Vec::new(),
            skip_entries: Vec::new(),
        };

        let first = build_transfer_scan_result_for_root_dir_abs(
            root.path().to_str().unwrap(),
            &assignment,
        )
        .unwrap();
        assert!(!first.finished);
        assert!(!first.direct_files_only_batches.is_empty());
        assert!(first.full_dir_batches.is_empty());
        assert_eq!(first.child_scan_units.len(), 1);
        assert_eq!(first.child_scan_units[0].scan_unit_id, assignment.scan_unit_id);
        assert_eq!(first.child_scan_units[0].root_relpath, assignment.root_relpath);
        assert_eq!(first.child_scan_units[0].generation, assignment.generation);
        let first_entry_count = first
            .direct_files_only_batches
            .iter()
            .map(|batch| {
                FluxonFsTransferManifestWire::decode_from_blob(batch.manifest_blob.as_slice())
                    .unwrap()
                    .entries
                    .len()
            })
            .sum::<usize>();
        assert_eq!(first_entry_count, TRANSFER_SCAN_ROOT_LISTING_SLICE_ENTRY_LIMIT);

        let second_assignment = FluxonFsTransferScanAssignmentWire {
            scan_task_id: "task-2".to_string(),
            ..assignment.clone()
        };
        let second = build_transfer_scan_result_for_root_dir_abs(
            root.path().to_str().unwrap(),
            &second_assignment,
        )
        .unwrap();
        assert!(second.finished);
        assert_eq!(second.direct_files_only_batches.len(), 1);
        assert!(second.child_scan_units.is_empty());
        assert!(second.full_dir_batches.is_empty());
        let manifest = FluxonFsTransferManifestWire::decode_from_blob(
            &second.direct_files_only_batches[0].manifest_blob,
        )
        .unwrap();
        assert_eq!(manifest.entries.len(), 1);
    }

    #[test]
    fn build_transfer_scan_result_emits_multiple_root_direct_batches_in_one_result() {
        let root = TempDir::new().unwrap();
        for idx in 0..10 {
            let relpath = format!("root/file-{idx:02}.bin");
            write_file(&root, relpath.as_str(), b"x");
        }
        let result = build_transfer_scan_result_for_root_dir_abs(
            root.path().to_str().unwrap(),
            &FluxonFsTransferScanAssignmentWire {
                job_id: "job".to_string(),
                scan_epoch: 1,
                scan_unit_id: "scan-root-direct".to_string(),
                scan_task_id: "task-1".to_string(),
                root_relpath: "root".to_string(),
                generation: 1,
                scan_mode: FluxonFsTransferScanMode::FullTree,
                src_export: "src".to_string(),
                src_exporter_id: "src-exporter".to_string(),
                batch_ready_bytes: 3,
                lease_expire_unix_ms: 0,
                known_dispositions: Vec::new(),
                live_child_scan_roots: Vec::new(),
                skip_entries: Vec::new(),
            },
        )
        .unwrap();
        assert!(result.finished);
        assert!(result.child_scan_units.is_empty());
        assert!(result.full_dir_batches.is_empty());
        assert!(result.direct_files_only_batches.len() > 1);
        let entry_count = result
            .direct_files_only_batches
            .iter()
            .map(|batch| {
                FluxonFsTransferManifestWire::decode_from_blob(batch.manifest_blob.as_slice())
                    .unwrap()
                    .entries
                    .len()
            })
            .sum::<usize>();
        assert_eq!(entry_count, 10);
    }

    #[test]
    fn build_transfer_scan_result_root_direct_fanout_only_emits_child_scan_units_without_recursing() {
        let root = TempDir::new().unwrap();
        write_file(&root, "root/direct.bin", b"abc");
        write_file(&root, "root/child/payload.bin", b"xyz");
        let result = build_transfer_scan_result_for_root_dir_abs(
            root.path().to_str().unwrap(),
            &FluxonFsTransferScanAssignmentWire {
                job_id: "job".to_string(),
                scan_epoch: 1,
                scan_unit_id: "scan-root-fanout".to_string(),
                scan_task_id: "task-root-fanout".to_string(),
                root_relpath: "root".to_string(),
                generation: 1,
                scan_mode: FluxonFsTransferScanMode::RootDirectFanoutOnly,
                src_export: "src".to_string(),
                src_exporter_id: "src-exporter".to_string(),
                batch_ready_bytes: 8,
                lease_expire_unix_ms: 0,
                known_dispositions: Vec::new(),
                live_child_scan_roots: Vec::new(),
                skip_entries: Vec::new(),
            },
        )
        .unwrap();
        assert!(result.finished);
        assert_eq!(result.direct_files_only_batches.len(), 1);
        assert_eq!(result.child_scan_units.len(), 1);
        assert!(result.full_dir_batches.is_empty());
        assert_eq!(result.child_scan_units[0].root_relpath, "root/child".to_string());
        assert_eq!(
            result.child_scan_units[0].scan_mode,
            FluxonFsTransferScanMode::FullTree
        );
        let manifest =
            FluxonFsTransferManifestWire::decode_from_blob(&result.direct_files_only_batches[0].manifest_blob)
                .unwrap();
        assert_eq!(
            manifest.entries,
            vec![FluxonFsTransferManifestEntryWire {
                relpath: "root/direct.bin".to_string(),
                size: 3,
            }]
        );
        assert!(manifest.empty_dir_relpaths.is_empty());
    }

    #[test]
    fn build_transfer_scan_result_directory_direct_fanout_only_emits_child_scan_units_without_recursing() {
        let root = TempDir::new().unwrap();
        write_file(&root, "root/child/direct.bin", b"abc");
        write_file(&root, "root/child/grand/payload.bin", b"xyz");
        let result = build_transfer_scan_result_for_root_dir_abs(
            root.path().to_str().unwrap(),
            &FluxonFsTransferScanAssignmentWire {
                job_id: "job".to_string(),
                scan_epoch: 1,
                scan_unit_id: "scan-child-fanout".to_string(),
                scan_task_id: "task-child-fanout".to_string(),
                root_relpath: "root/child".to_string(),
                generation: 2,
                scan_mode: FluxonFsTransferScanMode::DirectoryDirectFanoutOnly,
                src_export: "src".to_string(),
                src_exporter_id: "src-exporter".to_string(),
                batch_ready_bytes: 8,
                lease_expire_unix_ms: 0,
                known_dispositions: Vec::new(),
                live_child_scan_roots: Vec::new(),
                skip_entries: Vec::new(),
            },
        )
        .unwrap();
        assert!(result.finished);
        assert_eq!(result.direct_files_only_batches.len(), 1);
        assert_eq!(result.child_scan_units.len(), 1);
        assert!(result.full_dir_batches.is_empty());
        assert_eq!(result.child_scan_units[0].root_relpath, "root/child/grand".to_string());
        assert_eq!(
            result.child_scan_units[0].scan_mode,
            FluxonFsTransferScanMode::FullTree
        );
        let manifest =
            FluxonFsTransferManifestWire::decode_from_blob(&result.direct_files_only_batches[0].manifest_blob)
                .unwrap();
        assert_eq!(
            manifest.entries,
            vec![FluxonFsTransferManifestEntryWire {
                relpath: "root/child/direct.bin".to_string(),
                size: 3,
            }]
        );
        assert!(manifest.empty_dir_relpaths.is_empty());
    }

    #[test]
    fn build_transfer_scan_result_full_tree_child_assignment_streams_grandchild() {
        let root = TempDir::new().unwrap();
        write_file(&root, "root/child/direct.bin", b"abc");
        write_file(&root, "root/child/grand/payload.bin", b"xyz");
        let result = build_transfer_scan_result_for_root_dir_abs(
            root.path().to_str().unwrap(),
            &FluxonFsTransferScanAssignmentWire {
                job_id: "job".to_string(),
                scan_epoch: 1,
                scan_unit_id: "scan-child-full-tree".to_string(),
                scan_task_id: "task-child-full-tree".to_string(),
                root_relpath: "root/child".to_string(),
                generation: 2,
                scan_mode: FluxonFsTransferScanMode::FullTree,
                src_export: "src".to_string(),
                src_exporter_id: "src-exporter".to_string(),
                batch_ready_bytes: 8,
                lease_expire_unix_ms: 0,
                known_dispositions: Vec::new(),
                live_child_scan_roots: Vec::new(),
                skip_entries: Vec::new(),
            },
        )
        .unwrap();
        assert!(result.finished);
        assert_eq!(result.direct_files_only_batches.len(), 1);
        assert_eq!(result.direct_files_only_batches[0].root_relpath, "root/child");
        let manifest =
            FluxonFsTransferManifestWire::decode_from_blob(&result.direct_files_only_batches[0].manifest_blob)
                .unwrap();
        assert_eq!(
            manifest.entries,
            vec![FluxonFsTransferManifestEntryWire {
                relpath: "root/child/direct.bin".to_string(),
                size: 3,
            }]
        );
        assert!(manifest.empty_dir_relpaths.is_empty());
        assert_eq!(child_scan_unit_roots(&result), vec!["root/child/grand".to_string()]);
        assert_all_child_scan_units_are_subtree_streaming(&result);
        assert!(result.full_dir_batches.is_empty());
    }

    #[test]
    fn build_transfer_scan_result_root_direct_fanout_only_keeps_empty_child_dirs_in_parent_batch() {
        let root = TempDir::new().unwrap();
        write_file(&root, "root/direct.bin", b"abc");
        write_file(&root, "root/child/payload.bin", b"xyz");
        fs::create_dir_all(root.path().join("root/empty-a")).unwrap();
        fs::create_dir_all(root.path().join("root/empty-b")).unwrap();
        let result = build_transfer_scan_result_for_root_dir_abs(
            root.path().to_str().unwrap(),
            &FluxonFsTransferScanAssignmentWire {
                job_id: "job".to_string(),
                scan_epoch: 1,
                scan_unit_id: "scan-root-fanout-empty".to_string(),
                scan_task_id: "task-root-fanout-empty".to_string(),
                root_relpath: "root".to_string(),
                generation: 1,
                scan_mode: FluxonFsTransferScanMode::RootDirectFanoutOnly,
                src_export: "src".to_string(),
                src_exporter_id: "src-exporter".to_string(),
                batch_ready_bytes: 8,
                lease_expire_unix_ms: 0,
                known_dispositions: Vec::new(),
                live_child_scan_roots: Vec::new(),
                skip_entries: Vec::new(),
            },
        )
        .unwrap();
        assert!(result.finished);
        assert_eq!(result.direct_files_only_batches.len(), 1);
        assert_eq!(result.child_scan_units.len(), 1);
        assert_eq!(result.child_scan_units[0].root_relpath, "root/child".to_string());
        let manifest =
            FluxonFsTransferManifestWire::decode_from_blob(&result.direct_files_only_batches[0].manifest_blob)
                .unwrap();
        assert_eq!(
            manifest.entries,
            vec![FluxonFsTransferManifestEntryWire {
                relpath: "root/direct.bin".to_string(),
                size: 3,
            }]
        );
        assert_eq!(
            manifest.empty_dir_relpaths,
            vec!["root/empty-a".to_string(), "root/empty-b".to_string()]
        );
    }

    #[test]
    fn build_transfer_scan_result_finishes_split_root_without_reaggregating_delegated_child_dirs() {
        let root = TempDir::new().unwrap();
        write_file(&root, "root/direct.bin", b"abc");
        write_file(&root, "root/child/payload.bin", b"xyz");
        let root_dir = root.path().join("root");
        let mut read_dir = fs::read_dir(&root_dir).unwrap();
        while let Some(entry) = read_dir.next() {
            entry.unwrap();
        }
        store_transfer_root_dir_listing_session(
            "scan-split",
            TransferRootDirListingSession {
                job_id: "job".to_string(),
                scan_epoch: 1,
                root_relpath: "root".to_string(),
                generation: 1,
                lease_expire_unix_ms: 0,
                read_dir,
                pending_direct_files: vec![FluxonFsTransferScanFrontierEntry {
                    relpath: "root/direct.bin".to_string(),
                    size: 3,
                }],
                pending_direct_symlink_notices: Vec::new(),
                pending_direct_bytes: 3,
                pending_direct_empty_dirs: Vec::new(),
                next_direct_files_batch_index: 0,
                emitted_direct_files_batch_count: 0,
                emitted_child_scan_unit_count: 1,
                direct_dirs: vec![FluxonFsTransferScanFrontierDirEntry {
                    relpath: "root/child".to_string(),
                }],
                root_total_bytes: 3,
                root_visible_entries: true,
            },
        );
        let result = build_transfer_scan_result_for_root_dir_abs(
            root.path().to_str().unwrap(),
            &FluxonFsTransferScanAssignmentWire {
                job_id: "job".to_string(),
                scan_epoch: 1,
                scan_unit_id: "scan-split".to_string(),
                scan_task_id: "task-final".to_string(),
                root_relpath: "root".to_string(),
                generation: 1,
                scan_mode: FluxonFsTransferScanMode::FullTree,
                src_export: "src".to_string(),
                src_exporter_id: "src-exporter".to_string(),
                batch_ready_bytes: 8,
                lease_expire_unix_ms: 0,
                known_dispositions: Vec::new(),
                live_child_scan_roots: Vec::new(),
                skip_entries: Vec::new(),
            },
        )
        .unwrap();
        assert!(result.finished);
        assert_eq!(result.direct_files_only_batches.len(), 1);
        assert!(result.child_scan_units.is_empty());
        assert!(result.full_dir_batches.is_empty());
        let manifest =
            FluxonFsTransferManifestWire::decode_from_blob(&result.direct_files_only_batches[0].manifest_blob)
                .unwrap();
        assert_eq!(
            manifest.entries,
            vec![FluxonFsTransferManifestEntryWire {
                relpath: "root/direct.bin".to_string(),
                size: 3,
            }]
        );
    }

    #[test]
    fn build_transfer_scan_result_split_root_finishes_by_delegating_remaining_child_dirs() {
        let root = TempDir::new().unwrap();
        write_file(&root, "root/direct.bin", b"abc");
        write_file(&root, "root/child-a/payload.bin", b"xyz");
        write_file(&root, "root/child-b/payload.bin", b"uvw");
        let root_dir = root.path().join("root");
        let mut read_dir = fs::read_dir(&root_dir).unwrap();
        while let Some(entry) = read_dir.next() {
            entry.unwrap();
        }
        store_transfer_root_dir_listing_session(
            "scan-split-tail",
            TransferRootDirListingSession {
                job_id: "job".to_string(),
                scan_epoch: 1,
                root_relpath: "root".to_string(),
                generation: 1,
                lease_expire_unix_ms: 0,
                read_dir,
                pending_direct_files: vec![FluxonFsTransferScanFrontierEntry {
                    relpath: "root/direct.bin".to_string(),
                    size: 3,
                }],
                pending_direct_symlink_notices: Vec::new(),
                pending_direct_bytes: 3,
                pending_direct_empty_dirs: Vec::new(),
                next_direct_files_batch_index: 0,
                emitted_direct_files_batch_count: 0,
                emitted_child_scan_unit_count: 1,
                direct_dirs: vec![
                    FluxonFsTransferScanFrontierDirEntry {
                        relpath: "root/child-a".to_string(),
                    },
                    FluxonFsTransferScanFrontierDirEntry {
                        relpath: "root/child-b".to_string(),
                    },
                ],
                root_total_bytes: 3,
                root_visible_entries: true,
            },
        );
        let result = build_transfer_scan_result_for_root_dir_abs(
            root.path().to_str().unwrap(),
            &FluxonFsTransferScanAssignmentWire {
                job_id: "job".to_string(),
                scan_epoch: 1,
                scan_unit_id: "scan-split-tail".to_string(),
                scan_task_id: "task-final".to_string(),
                root_relpath: "root".to_string(),
                generation: 1,
                scan_mode: FluxonFsTransferScanMode::FullTree,
                src_export: "src".to_string(),
                src_exporter_id: "src-exporter".to_string(),
                batch_ready_bytes: 8,
                lease_expire_unix_ms: 0,
                known_dispositions: Vec::new(),
                live_child_scan_roots: Vec::new(),
                skip_entries: Vec::new(),
            },
        )
        .unwrap();
        assert!(result.finished);
        assert_eq!(result.direct_files_only_batches.len(), 1);
        assert_eq!(result.child_scan_units.len(), 1);
        assert_eq!(result.child_scan_units[0].root_relpath, "root/child-b".to_string());
        assert_eq!(
            result.child_scan_units[0].scan_mode,
            FluxonFsTransferScanMode::FullTree
        );
        assert!(result.full_dir_batches.is_empty());
    }

    #[test]
    fn handle_transfer_scan_assignment_does_not_reaggregate_exact_live_child_scan_root() {
        let root = TempDir::new().unwrap();
        write_file(&root, "root/direct.bin", b"abc");
        write_file(&root, "root/child/payload.bin", b"xyz");
        let exports = test_exports_handle(root.path().to_str().unwrap());

        let resp = handle_transfer_scan_assignment(
            &exports,
            FluxonFsTransferScanAssignmentWire {
                job_id: "job".to_string(),
                scan_epoch: 1,
                scan_unit_id: "scan".to_string(),
                scan_task_id: "task".to_string(),
                root_relpath: "root".to_string(),
                generation: 1,
                scan_mode: FluxonFsTransferScanMode::FullTree,
                src_export: "src".to_string(),
                src_exporter_id: "src-exporter".to_string(),
                batch_ready_bytes: 8,
                lease_expire_unix_ms: 0,
                known_dispositions: Vec::new(),
                live_child_scan_roots: vec!["root/child".to_string()],
                skip_entries: Vec::new(),
            },
        );
        let result = decode_result_json(&resp);

        assert!(result.finished);
        assert_eq!(result.direct_files_only_batches.len(), 1);
        assert!(result.child_scan_units.is_empty());
        assert!(result.full_dir_batches.is_empty());
        let manifest =
            FluxonFsTransferManifestWire::decode_from_blob(&result.direct_files_only_batches[0].manifest_blob)
                .unwrap();
        assert_eq!(
            manifest.entries,
            vec![FluxonFsTransferManifestEntryWire {
                relpath: "root/direct.bin".to_string(),
                size: 3,
            }]
        );
    }

    #[test]
    fn handle_transfer_scan_assignment_emits_full_dir_batch_for_empty_root() {
        let root = TempDir::new().unwrap();
        fs::create_dir_all(root.path().join("root")).unwrap();
        let exports = test_exports_handle(root.path().to_str().unwrap());

        let resp = handle_transfer_scan_assignment(
            &exports,
            FluxonFsTransferScanAssignmentWire {
                job_id: "job".to_string(),
                scan_epoch: 1,
                scan_unit_id: "scan".to_string(),
                scan_task_id: "task".to_string(),
                root_relpath: "root".to_string(),
                generation: 1,
                scan_mode: FluxonFsTransferScanMode::FullTree,
                src_export: "src".to_string(),
                src_exporter_id: "src-exporter".to_string(),
                batch_ready_bytes: 4,
                lease_expire_unix_ms: 0,
                known_dispositions: Vec::new(),
                live_child_scan_roots: Vec::new(),
                skip_entries: Vec::new(),
            },
        );
        let result = decode_result_json(&resp);

        assert!(result.finished);
        assert!(result.direct_files_only_batches.is_empty());
        assert!(result.child_scan_units.is_empty());
        assert_eq!(result.full_dir_batches.len(), 1);
        let manifest =
            FluxonFsTransferManifestWire::decode_from_blob(&result.full_dir_batches[0].manifest_blob)
                .unwrap();
        assert!(manifest.entries.is_empty());
        assert_eq!(manifest.empty_dir_relpaths, vec!["root".to_string()]);
    }

    #[test]
    fn handle_transfer_scan_assignment_emits_parent_full_dir_when_entire_root_is_ready() {
        let root = TempDir::new().unwrap();
        write_file(&root, "root/direct.bin", b"abc");
        write_file(&root, "root/keep.bin", b"12345");
        let exports = test_exports_handle(root.path().to_str().unwrap());

        let resp = handle_transfer_scan_assignment(
            &exports,
            FluxonFsTransferScanAssignmentWire {
                job_id: "job".to_string(),
                scan_epoch: 1,
                scan_unit_id: "scan".to_string(),
                scan_task_id: "task".to_string(),
                root_relpath: ".".to_string(),
                generation: 1,
                scan_mode: FluxonFsTransferScanMode::FullTree,
                src_export: "src".to_string(),
                src_exporter_id: "src-exporter".to_string(),
                batch_ready_bytes: 4,
                lease_expire_unix_ms: 0,
                known_dispositions: Vec::new(),
                live_child_scan_roots: Vec::new(),
                skip_entries: Vec::new(),
            },
        );
        let result = decode_result_json(&resp);

        assert!(result.finished);
        assert!(result.direct_files_only_batches.is_empty());
        assert_eq!(child_scan_unit_roots(&result), vec!["root".to_string()]);
        assert_all_child_scan_units_are_subtree_streaming(&result);
        assert!(result.full_dir_batches.is_empty());
    }

    #[test]
    fn handle_transfer_scan_assignment_does_not_reaggregate_root_when_descendant_batch_is_durable() {
        let root = TempDir::new().unwrap();
        write_file(&root, "root/direct.bin", b"abc");
        write_file(&root, "root/big/data.bin", b"12345");
        write_file(&root, "root/small/child.bin", b"12");
        let exports = test_exports_handle(root.path().to_str().unwrap());

        let resp = handle_transfer_scan_assignment(
            &exports,
            FluxonFsTransferScanAssignmentWire {
                job_id: "job".to_string(),
                scan_epoch: 1,
                scan_unit_id: "scan".to_string(),
                scan_task_id: "task".to_string(),
                root_relpath: "root".to_string(),
                generation: 3,
                scan_mode: FluxonFsTransferScanMode::FullTree,
                src_export: "src".to_string(),
                src_exporter_id: "src-exporter".to_string(),
                batch_ready_bytes: 4,
                lease_expire_unix_ms: 0,
                known_dispositions: vec![FluxonFsTransferDispositionWire {
                    root_relpath: "root/big".to_string(),
                    generation: 3,
                    batch_kind: FluxonFsTransferBatchKind::FullDir,
                }],
                live_child_scan_roots: Vec::new(),
                skip_entries: Vec::new(),
            },
        );
        let result = decode_result_json(&resp);

        assert!(result.finished);
        assert_eq!(result.direct_files_only_batches.len(), 1);
        let direct_files_only_batch = &result.direct_files_only_batches[0];
        assert_eq!(direct_files_only_batch.root_relpath, "root".to_string());
        let direct_manifest =
            FluxonFsTransferManifestWire::decode_from_blob(&direct_files_only_batch.manifest_blob)
                .unwrap();
        assert_eq!(
            direct_manifest.entries,
            vec![FluxonFsTransferManifestEntryWire {
                relpath: "root/direct.bin".to_string(),
                size: 3,
            }]
        );
        assert!(result
            .full_dir_batches
            .iter()
            .all(|batch| batch.root_relpath != "root"));
        assert!(result
            .full_dir_batches
            .iter()
            .all(|batch| batch.root_relpath != "root/big"));
        assert_eq!(child_scan_unit_roots(&result), vec!["root/small".to_string()]);
        assert_all_child_scan_units_are_subtree_streaming(&result);
        assert!(result.full_dir_batches.is_empty());
    }

    #[test]
    fn handle_transfer_scan_assignment_honors_cross_generation_descendant_full_dir_during_restart() {
        let root = TempDir::new().unwrap();
        write_file(&root, "root/direct.bin", b"abc");
        write_file(&root, "root/big/data.bin", b"12345");
        write_file(&root, "root/small/child.bin", b"12");
        let exports = test_exports_handle(root.path().to_str().unwrap());

        let resp = handle_transfer_scan_assignment(
            &exports,
            FluxonFsTransferScanAssignmentWire {
                job_id: "job".to_string(),
                scan_epoch: 1,
                scan_unit_id: "scan".to_string(),
                scan_task_id: "task".to_string(),
                root_relpath: "root".to_string(),
                generation: 1,
                scan_mode: FluxonFsTransferScanMode::FullTree,
                src_export: "src".to_string(),
                src_exporter_id: "src-exporter".to_string(),
                batch_ready_bytes: 4,
                lease_expire_unix_ms: 0,
                known_dispositions: vec![FluxonFsTransferDispositionWire {
                    root_relpath: "root/big".to_string(),
                    generation: 2,
                    batch_kind: FluxonFsTransferBatchKind::FullDir,
                }],
                live_child_scan_roots: Vec::new(),
                skip_entries: Vec::new(),
            },
        );
        let result = decode_result_json(&resp);

        assert!(result.finished);
        assert_eq!(result.direct_files_only_batches.len(), 1);
        let direct_files_only_batch = &result.direct_files_only_batches[0];
        assert_eq!(direct_files_only_batch.root_relpath, "root".to_string());
        let direct_manifest =
            FluxonFsTransferManifestWire::decode_from_blob(&direct_files_only_batch.manifest_blob)
                .unwrap();
        assert_eq!(
            direct_manifest.entries,
            vec![FluxonFsTransferManifestEntryWire {
                relpath: "root/direct.bin".to_string(),
                size: 3,
            }]
        );
        assert!(result
            .full_dir_batches
            .iter()
            .all(|batch| batch.root_relpath != "root"));
        assert!(result
            .full_dir_batches
            .iter()
            .all(|batch| batch.root_relpath != "root/big"));
        assert_eq!(child_scan_unit_roots(&result), vec!["root/small".to_string()]);
        assert_all_child_scan_units_are_subtree_streaming(&result);
        assert!(result.full_dir_batches.is_empty());
    }

    #[test]
    fn handle_transfer_scan_assignment_replays_descendant_current_layer_when_only_partial_descendant_direct_files_batch_is_durable() {
        let root = TempDir::new().unwrap();
        write_file(&root, "root/child/a.bin", b"ab");
        write_file(&root, "root/child/b.bin", b"cd");
        let exports = test_exports_handle(root.path().to_str().unwrap());

        let resp = handle_transfer_scan_assignment(
            &exports,
            FluxonFsTransferScanAssignmentWire {
                job_id: "job".to_string(),
                scan_epoch: 1,
                scan_unit_id: "scan".to_string(),
                scan_task_id: "task".to_string(),
                root_relpath: "root".to_string(),
                generation: 1,
                scan_mode: FluxonFsTransferScanMode::FullTree,
                src_export: "src".to_string(),
                src_exporter_id: "src-exporter".to_string(),
                batch_ready_bytes: 64,
                lease_expire_unix_ms: 0,
                known_dispositions: vec![FluxonFsTransferDispositionWire {
                    root_relpath: "root/child".to_string(),
                    generation: 1,
                    batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                }],
                live_child_scan_roots: Vec::new(),
                skip_entries: Vec::new(),
            },
        );
        let result = decode_result_json(&resp);

        assert!(result.finished);
        assert!(result.child_scan_units.is_empty());
        assert!(result.full_dir_batches.is_empty());
        assert_eq!(result.direct_files_only_batches.len(), 1);
        assert_eq!(result.direct_files_only_batches[0].root_relpath, "root/child");
        let manifest =
            FluxonFsTransferManifestWire::decode_from_blob(&result.direct_files_only_batches[0].manifest_blob)
                .unwrap();
        assert_eq!(
            manifest.entries,
            vec![
                FluxonFsTransferManifestEntryWire {
                    relpath: "root/child/a.bin".to_string(),
                    size: 2,
                },
                FluxonFsTransferManifestEntryWire {
                    relpath: "root/child/b.bin".to_string(),
                    size: 2,
                },
            ]
        );
    }

    #[test]
    fn handle_transfer_scan_assignment_closes_first_over_threshold_ancestor_subtree() {
        let root = TempDir::new().unwrap();
        write_file(&root, "root/parent/direct.bin", b"abcdefghij");
        write_file(&root, "root/parent/child/grand.bin", b"klmnopqrst");
        let exports = test_exports_handle(root.path().to_str().unwrap());

        let resp = handle_transfer_scan_assignment(
            &exports,
            FluxonFsTransferScanAssignmentWire {
                job_id: "job".to_string(),
                scan_epoch: 1,
                scan_unit_id: "scan".to_string(),
                scan_task_id: "task".to_string(),
                root_relpath: "root".to_string(),
                generation: 1,
                scan_mode: FluxonFsTransferScanMode::FullTree,
                src_export: "src".to_string(),
                src_exporter_id: "src-exporter".to_string(),
                batch_ready_bytes: 15,
                lease_expire_unix_ms: 0,
                known_dispositions: Vec::new(),
                live_child_scan_roots: Vec::new(),
                skip_entries: Vec::new(),
            },
        );
        let result = decode_result_json(&resp);

        assert!(result.finished);
        assert!(result.direct_files_only_batches.is_empty());
        assert_eq!(child_scan_unit_roots(&result), vec!["root/parent".to_string()]);
        assert_all_child_scan_units_are_subtree_streaming(&result);
        assert!(result.full_dir_batches.is_empty());
    }

    #[test]
    fn handle_transfer_scan_assignment_closes_root_when_child_only_completes_threshold() {
        let root = TempDir::new().unwrap();
        write_file(&root, "root/direct.bin", b"abcdefghij");
        write_file(&root, "root/child/grand.bin", b"klmnopqrst");
        let exports = test_exports_handle(root.path().to_str().unwrap());

        let resp = handle_transfer_scan_assignment(
            &exports,
            FluxonFsTransferScanAssignmentWire {
                job_id: "job".to_string(),
                scan_epoch: 1,
                scan_unit_id: "scan".to_string(),
                scan_task_id: "task".to_string(),
                root_relpath: "root".to_string(),
                generation: 1,
                scan_mode: FluxonFsTransferScanMode::FullTree,
                src_export: "src".to_string(),
                src_exporter_id: "src-exporter".to_string(),
                batch_ready_bytes: 15,
                lease_expire_unix_ms: 0,
                known_dispositions: Vec::new(),
                live_child_scan_roots: Vec::new(),
                skip_entries: Vec::new(),
            },
        );
        let result = decode_result_json(&resp);

        assert!(result.finished);
        assert_eq!(result.direct_files_only_batches.len(), 1);
        let manifest =
            FluxonFsTransferManifestWire::decode_from_blob(&result.direct_files_only_batches[0].manifest_blob)
                .unwrap();
        assert_eq!(
            manifest.entries,
            vec![FluxonFsTransferManifestEntryWire {
                relpath: "root/direct.bin".to_string(),
                size: 10,
            }]
        );
        assert_eq!(child_scan_unit_roots(&result), vec!["root/child".to_string()]);
        assert_all_child_scan_units_are_subtree_streaming(&result);
        assert!(result.full_dir_batches.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn handle_transfer_scan_assignment_best_effort_recovers_permission_denied_root() {
        let root = TempDir::new().unwrap();
        write_file(&root, "root/blocked/file.bin", b"abc");
        let blocked = root.path().join("root").join("blocked");
        chmod_mode(&blocked, 0o000);
        let exports = test_exports_handle(root.path().to_str().unwrap());

        let resp = handle_transfer_scan_assignment(
            &exports,
            FluxonFsTransferScanAssignmentWire {
                job_id: "job".to_string(),
                scan_epoch: 1,
                scan_unit_id: "scan".to_string(),
                scan_task_id: "task".to_string(),
                root_relpath: "root/blocked".to_string(),
                generation: 1,
                scan_mode: FluxonFsTransferScanMode::FullTree,
                src_export: "src".to_string(),
                src_exporter_id: "src-exporter".to_string(),
                batch_ready_bytes: 4,
                lease_expire_unix_ms: 0,
                known_dispositions: Vec::new(),
                live_child_scan_roots: Vec::new(),
                skip_entries: Vec::new(),
            },
        );
        chmod_mode(&blocked, 0o755);
        let result = decode_result_json(&resp);

        assert!(ok_bool(&resp));
        assert!(result.finished);
        assert_eq!(child_scan_unit_roots(&result), vec!["root/blocked".to_string()]);
        assert_all_child_scan_units_are_subtree_streaming(&result);
        assert!(result.full_dir_batches.is_empty());
    }

    #[test]
    fn handle_transfer_scan_assignment_emits_multiple_ready_full_dir_children() {
        let root = TempDir::new().unwrap();
        write_file(&root, "root/direct.bin", b"abc");
        write_file(&root, "root/a/payload.bin", b"12345");
        write_file(&root, "root/b/payload.bin", b"67890");
        write_file(&root, "root/c/payload.bin", b"12");
        let exports = test_exports_handle(root.path().to_str().unwrap());

        let resp = handle_transfer_scan_assignment(
            &exports,
            FluxonFsTransferScanAssignmentWire {
                job_id: "job".to_string(),
                scan_epoch: 1,
                scan_unit_id: "scan".to_string(),
                scan_task_id: "task".to_string(),
                root_relpath: "root".to_string(),
                generation: 1,
                scan_mode: FluxonFsTransferScanMode::FullTree,
                src_export: "src".to_string(),
                src_exporter_id: "src-exporter".to_string(),
                batch_ready_bytes: 4,
                lease_expire_unix_ms: 0,
                known_dispositions: Vec::new(),
                live_child_scan_roots: Vec::new(),
                skip_entries: Vec::new(),
            },
        );
        let result = decode_result_json(&resp);

        assert!(result.finished);
        assert_eq!(result.direct_files_only_batches.len(), 1);
        let direct_files_only_batch = &result.direct_files_only_batches[0];
        let direct_manifest =
            FluxonFsTransferManifestWire::decode_from_blob(&direct_files_only_batch.manifest_blob)
                .unwrap();
        assert_eq!(
            direct_manifest.entries,
            vec![FluxonFsTransferManifestEntryWire {
                relpath: "root/direct.bin".to_string(),
                size: 3,
            }]
        );
        assert_eq!(
            child_scan_unit_roots(&result),
            vec!["root/a".to_string(), "root/b".to_string(), "root/c".to_string()]
        );
        assert_all_child_scan_units_are_subtree_streaming(&result);
        assert!(result.full_dir_batches.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn handle_transfer_scan_assignment_full_dir_batch_carries_symlink_collect_info() {
        let root = TempDir::new().unwrap();
        write_file(&root, "root/big/data.bin", b"12345");
        write_symlink(&root, "root/big/link.bin", "data.bin");
        let exports = test_exports_handle(root.path().to_str().unwrap());

        let resp = handle_transfer_scan_assignment(
            &exports,
            FluxonFsTransferScanAssignmentWire {
                job_id: "job".to_string(),
                scan_epoch: 1,
                scan_unit_id: "scan".to_string(),
                scan_task_id: "task".to_string(),
                root_relpath: "root/big".to_string(),
                generation: 3,
                scan_mode: FluxonFsTransferScanMode::SubtreeStreaming,
                src_export: "src".to_string(),
                src_exporter_id: "src-exporter".to_string(),
                batch_ready_bytes: 4,
                lease_expire_unix_ms: 0,
                known_dispositions: Vec::new(),
                live_child_scan_roots: Vec::new(),
                skip_entries: Vec::new(),
            },
        );
        let result = decode_result_json(&resp);
        assert_eq!(result.full_dir_batches.len(), 1);
        assert_eq!(result.full_dir_batches[0].batch_kind, FluxonFsTransferBatchKind::SubtreeSlice);
        assert_eq!(result.full_dir_batches[0].collect_infos.len(), 1);
        assert_eq!(
            decode_symlink_notice_collect_blob(
                result.full_dir_batches[0].collect_infos[0]
                    .collect_blob
                    .as_slice()
            ),
            vec![FluxonFsTransferSymlinkNoticeEntryWire {
                relpath: "root/big/link.bin".to_string(),
                link_target: "data.bin".to_string(),
            }]
        );
    }

    #[cfg(unix)]
    #[test]
    fn handle_transfer_scan_assignment_skips_symlinks_without_traversal() {
        let root = TempDir::new().unwrap();
        write_file(&root, "root/direct.bin", b"abc");
        write_file(&root, "root/real/sub/file.bin", b"1234");
        write_symlink(&root, "root/link-file.bin", "real/sub/file.bin");
        write_symlink(&root, "root/link-dir", "real/sub");
        let exports = test_exports_handle(root.path().to_str().unwrap());

        let resp = handle_transfer_scan_assignment(
            &exports,
            FluxonFsTransferScanAssignmentWire {
                job_id: "job".to_string(),
                scan_epoch: 1,
                scan_unit_id: "scan".to_string(),
                scan_task_id: "task".to_string(),
                root_relpath: "root".to_string(),
                generation: 1,
                scan_mode: FluxonFsTransferScanMode::SubtreeStreaming,
                src_export: "src".to_string(),
                src_exporter_id: "src-exporter".to_string(),
                batch_ready_bytes: 8,
                lease_expire_unix_ms: 0,
                known_dispositions: Vec::new(),
                live_child_scan_roots: Vec::new(),
                skip_entries: Vec::new(),
            },
        );
        let result = decode_result_json(&resp);

        assert!(result.finished);
        assert!(result.frontier.direct_files.is_empty());
        assert!(result.frontier.direct_dirs.is_empty());
        assert!(result.direct_files_only_batches.is_empty());
        assert!(result.child_scan_units.is_empty());
        assert_eq!(result.full_dir_batches.len(), 1);
        assert_eq!(result.full_dir_batches[0].batch_kind, FluxonFsTransferBatchKind::SubtreeSlice);
        assert_eq!(result.full_dir_batches[0].root_relpath, "root".to_string());
        let manifest =
            FluxonFsTransferManifestWire::decode_from_blob(&result.full_dir_batches[0].manifest_blob)
                .unwrap();
        assert_eq!(
            manifest.entries,
            vec![
                FluxonFsTransferManifestEntryWire {
                    relpath: "root/direct.bin".to_string(),
                    size: 3,
                },
                FluxonFsTransferManifestEntryWire {
                    relpath: "root/real/sub/file.bin".to_string(),
                    size: 4,
                },
            ]
        );
        let direct_files_only_batch = &result.full_dir_batches[0];
        assert_eq!(direct_files_only_batch.collect_infos.len(), 1);
        assert_eq!(
            direct_files_only_batch.collect_infos[0].collect_kind,
            FluxonFsTransferCollectInfoKind::SymlinkNotice
        );
        let mut notices = decode_symlink_notice_collect_blob(
            direct_files_only_batch.collect_infos[0].collect_blob.as_slice()
        );
        notices.sort_by(|a, b| a.relpath.cmp(&b.relpath));
        assert_eq!(
            notices,
            vec![
                FluxonFsTransferSymlinkNoticeEntryWire {
                    relpath: "root/link-dir".to_string(),
                    link_target: "real/sub".to_string(),
                },
                FluxonFsTransferSymlinkNoticeEntryWire {
                    relpath: "root/link-file.bin".to_string(),
                    link_target: "real/sub/file.bin".to_string(),
                },
            ]
        );
    }

    #[test]
    fn prepare_transfer_file_from_chunks_promotes_staged_file_to_final_path() {
        let root = TempDir::new().unwrap();
        let dst_root = root.path().to_path_buf();
        let coordinator = test_transfer_coordinator(
            || Ok(()),
            {
                let chunks = Arc::new(Mutex::new(vec![b"ab".to_vec(), b"cde".to_vec(), Vec::new()]));
                move |_file, _read_offset, _length| {
                    let mut chunks = chunks.lock();
                    Ok(chunks.remove(0))
                }
            },
        );
        let prepared = prepare_transfer_file_streaming(
            &dst_root,
            ".fluxon.stage/job/batch",
            &FluxonFsTransferManifestEntryWire {
                relpath: "dir/file.bin".to_string(),
                size: 5,
            },
            &coordinator,
        )
        .unwrap();
        assert!(!root.path().join("dir/file.bin").exists());
        let result = promote_prepared_transfer_file(&dst_root, prepared).unwrap();

        assert_eq!(result.relpath, "dir/file.bin");
        assert_eq!(result.final_relpath, "dir/file.bin");
        assert_eq!(
            result.staging_relpath,
            ".fluxon.stage/job/batch/dir/file.bin/file.bin.fluxon.part"
        );
        assert_eq!(result.visible_size, 5);
        assert_eq!(
            fs::read(root.path().join("dir/file.bin")).unwrap(),
            b"abcde".to_vec()
        );
        assert!(!root
            .path()
            .join(".fluxon.stage/job/batch/dir/file.bin/file.bin.fluxon.part")
            .exists());
    }

    #[test]
    fn prepare_transfer_file_from_chunks_truncates_existing_staging_file() {
        let root = TempDir::new().unwrap();
        let dst_root = root.path().to_path_buf();
        let stale_staging =
            root.path()
                .join(".fluxon.stage/job/batch/dir/file.bin/file.bin.fluxon.part");
        fs::create_dir_all(stale_staging.parent().unwrap()).unwrap();
        fs::write(&stale_staging, b"stale-data").unwrap();

        let coordinator = test_transfer_coordinator(
            || Ok(()),
            {
                let chunks = Arc::new(Mutex::new(vec![b"xy".to_vec(), Vec::new()]));
                move |_file, _read_offset, _length| {
                    let mut chunks = chunks.lock();
                    Ok(chunks.remove(0))
                }
            },
        );
        let prepared = prepare_transfer_file_streaming(
            &dst_root,
            ".fluxon.stage/job/batch",
            &FluxonFsTransferManifestEntryWire {
                relpath: "dir/file.bin".to_string(),
                size: 2,
            },
            &coordinator,
        )
        .unwrap();
        promote_prepared_transfer_file(&dst_root, prepared).unwrap();

        assert_eq!(fs::read(root.path().join("dir/file.bin")).unwrap(), b"xy".to_vec());
    }

    #[test]
    fn prepare_transfer_file_from_chunks_rejects_size_mismatch_and_keeps_staging_file() {
        let root = TempDir::new().unwrap();
        let dst_root = root.path().to_path_buf();
        let coordinator = test_transfer_coordinator(
            || Ok(()),
            {
                let chunks = Arc::new(Mutex::new(vec![b"xy".to_vec(), Vec::new()]));
                move |_file, _read_offset, _length| {
                    let mut chunks = chunks.lock();
                    Ok(chunks.remove(0))
                }
            },
        );
        let err = prepare_transfer_file_streaming(
            &dst_root,
            ".fluxon.stage/job/batch",
            &FluxonFsTransferManifestEntryWire {
                relpath: "dir/file.bin".to_string(),
                size: 4,
            },
            &coordinator,
        )
        .unwrap_err();

        let err = match err {
            TransferWorkerExecutionError::Fatal(resp) => resp,
            other => panic!("unexpected worker error: {:?}", other),
        };
        assert!(matches!(err.get("ok"), Some(FlatValue::Bool(false))));
        assert!(!root.path().join("dir/file.bin").exists());
        assert_eq!(
            fs::read(
                root.path()
                    .join(".fluxon.stage/job/batch/dir/file.bin/file.bin.fluxon.part")
            )
            .unwrap(),
            b"xy".to_vec()
        );
    }

    #[test]
    fn execute_transfer_worker_assignment_materializes_manifest_empty_dirs() {
        let root = TempDir::new().unwrap();
        let dst_root = root.path().to_path_buf();
        let assignment = FluxonFsTransferWorkerAssignmentWire {
            job_id: "job".to_string(),
            batch_id: "batch".to_string(),
            worker_task_id: "task".to_string(),
            worker_id: "worker-0".to_string(),
            batch_kind: FluxonFsTransferBatchKind::FullDir,
            src_export: "src".to_string(),
            dst_export: "dst".to_string(),
            src_exporter_id: "src-exporter".to_string(),
            dst_exporter_id: "dst-exporter".to_string(),
            dst_root_relpath: ".".to_string(),
            root_relpath: "root".to_string(),
            staging_prefix: ".fluxon.stage/job/batch".to_string(),
            lease_expire_unix_ms: 0,
            manifest_blob: build_transfer_manifest_blob(
                Vec::new(),
                vec![".".to_string(), "nested/empty".to_string()],
            )
            .unwrap(),
            collect_infos: Vec::new(),
        };

        let result = execute_transfer_worker_assignment(
            &assignment,
            &dst_root,
            || Ok(()),
            |_file, _read_offset, _length| Ok(Vec::new()),
        )
        .unwrap();

        assert!(result.file_results.is_empty());
        assert!(root.path().is_dir());
        assert!(root.path().join("nested/empty").is_dir());
    }

    #[test]
    fn execute_transfer_worker_assignment_records_materialized_empty_dir_progress() {
        let root = TempDir::new().unwrap();
        let dst_root = root.path().to_path_buf();
        let progress = Arc::new(TransferWorkerProgressWindow::new(
            Arc::new(TransferWorkerLanePolicy::production_default().normalized()),
            chrono::Utc::now().timestamp_millis(),
        ));
        let assignment = FluxonFsTransferWorkerAssignmentWire {
            job_id: "job".to_string(),
            batch_id: "batch".to_string(),
            worker_task_id: "task".to_string(),
            worker_id: "worker-0".to_string(),
            batch_kind: FluxonFsTransferBatchKind::FullDir,
            src_export: "src".to_string(),
            dst_export: "dst".to_string(),
            src_exporter_id: "src-exporter".to_string(),
            dst_exporter_id: "dst-exporter".to_string(),
            dst_root_relpath: ".".to_string(),
            root_relpath: "root".to_string(),
            staging_prefix: ".fluxon.stage/job/batch".to_string(),
            lease_expire_unix_ms: 0,
            manifest_blob: build_transfer_manifest_blob(
                Vec::new(),
                vec!["empty-a".to_string(), "nested/empty-b".to_string()],
            )
            .unwrap(),
            collect_infos: Vec::new(),
        };

        let result = execute_transfer_worker_assignment_with_policy_and_progress(
            &assignment,
            &dst_root,
            TransferWorkerLanePolicy::production_default(),
            progress.clone(),
            || Ok(()),
            |_file, _read_offset, _length| Ok(Vec::new()),
        )
        .unwrap();

        assert!(result.file_results.is_empty());
        assert_eq!(progress.total_materialized_empty_dirs(), 2);
    }

    #[test]
    fn execute_transfer_worker_assignment_empty_dirs_only_uses_full_lane_width() {
        let root = TempDir::new().unwrap();
        let dst_root = root.path().to_path_buf();
        let policy = TransferWorkerLanePolicy {
            initial_file_lanes: 1,
            max_file_lanes: 4,
            target_goodput_bytes_per_sec: i64::MAX,
            lane_ramp_interval: Duration::from_secs(60),
            lane_poll_interval: Duration::from_millis(10),
            min_improvement_percent: 0,
        };
        let progress = Arc::new(TransferWorkerProgressWindow::new(
            Arc::new(policy.normalized()),
            chrono::Utc::now().timestamp_millis(),
        ));
        let assignment = FluxonFsTransferWorkerAssignmentWire {
            job_id: "job".to_string(),
            batch_id: "batch".to_string(),
            worker_task_id: "task".to_string(),
            worker_id: "worker-0".to_string(),
            batch_kind: FluxonFsTransferBatchKind::FullDir,
            src_export: "src".to_string(),
            dst_export: "dst".to_string(),
            src_exporter_id: "src-exporter".to_string(),
            dst_exporter_id: "dst-exporter".to_string(),
            dst_root_relpath: ".".to_string(),
            root_relpath: "root".to_string(),
            staging_prefix: ".fluxon.stage/job/batch".to_string(),
            lease_expire_unix_ms: 0,
            manifest_blob: build_transfer_manifest_blob(
                Vec::new(),
                vec![
                    "empty-a".to_string(),
                    "empty-b".to_string(),
                    "empty-c".to_string(),
                    "empty-d".to_string(),
                ],
            )
            .unwrap(),
            collect_infos: Vec::new(),
        };

        let result = execute_transfer_worker_assignment_with_policy_and_progress(
            &assignment,
            &dst_root,
            policy,
            progress.clone(),
            || Ok(()),
            |_file, _read_offset, _length| Ok(Vec::new()),
        )
        .unwrap();

        assert!(result.file_results.is_empty());
        assert_eq!(progress.desired_file_lanes(), 4);
    }

    #[test]
    fn transfer_worker_lane_execution_exhausted_requires_empty_result_queue() {
        assert!(!transfer_worker_lane_execution_exhausted(0, 1, 3, 4));
        assert!(!transfer_worker_lane_execution_exhausted(1, 0, 3, 4));
        assert!(!transfer_worker_lane_execution_exhausted(0, 0, 4, 4));
        assert!(transfer_worker_lane_execution_exhausted(0, 0, 3, 4));
    }

    #[test]
    fn execute_transfer_worker_assignment_creates_missing_destination_root() {
        let root = TempDir::new().unwrap();
        let dst_root = root.path().join("missing").join("target");
        let file_bytes = b"hello".to_vec();
        let assignment = test_worker_assignment("dir/file.bin", file_bytes.len() as i64);

        let result = execute_transfer_worker_assignment(
            &assignment,
            &dst_root,
            || Ok(()),
            {
                let file_bytes = file_bytes.clone();
                move |_file, _read_offset, _length| Ok(file_bytes.clone())
            },
        )
        .unwrap();

        assert_eq!(result.file_results.len(), 1);
        assert!(dst_root.is_dir());
        assert_eq!(fs::read(dst_root.join("dir/file.bin")).unwrap(), file_bytes);
    }

    #[cfg(unix)]
    #[test]
    fn create_dir_all_with_parent_dir_chmod_retry_repairs_permission_denied_parent() {
        let root = TempDir::new().unwrap();
        let locked_parent = root.path().join("locked");
        fs::create_dir_all(&locked_parent).unwrap();
        chmod_mode(&locked_parent, 0o555);

        let target = locked_parent.join("nested").join("leaf");
        create_dir_all_with_parent_dir_chmod_retry(&target).unwrap();

        assert!(target.is_dir());
        assert_eq!(fs::metadata(&locked_parent).unwrap().permissions().mode() & 0o777, 0o777);
    }

    #[cfg(unix)]
    #[test]
    fn execute_transfer_worker_assignment_repairs_permission_denied_destination_parent() {
        let root = TempDir::new().unwrap();
        let locked_parent = root.path().join("locked");
        fs::create_dir_all(&locked_parent).unwrap();
        chmod_mode(&locked_parent, 0o555);

        let dst_root = locked_parent.join("missing").join("target");
        let file_bytes = b"hello".to_vec();
        let assignment = test_worker_assignment("dir/file.bin", file_bytes.len() as i64);

        let result = execute_transfer_worker_assignment(
            &assignment,
            &dst_root,
            || Ok(()),
            {
                let file_bytes = file_bytes.clone();
                move |_file, _read_offset, _length| Ok(file_bytes.clone())
            },
        )
        .unwrap();

        assert_eq!(result.file_results.len(), 1);
        assert!(dst_root.is_dir());
        assert_eq!(fs::read(dst_root.join("dir/file.bin")).unwrap(), file_bytes);
        assert_eq!(fs::metadata(&locked_parent).unwrap().permissions().mode() & 0o777, 0o777);
    }

    #[test]
    fn execute_transfer_worker_assignment_retries_checkpoint_until_success() {
        let root = TempDir::new().unwrap();
        let dst_root = root.path().to_path_buf();
        let file_bytes = b"hello".to_vec();
        let assignment = test_worker_assignment("dir/file.bin", file_bytes.len() as i64);
        let heartbeat_attempts = Arc::new(AtomicUsize::new(0));
        let result = execute_transfer_worker_assignment(
            &assignment,
            &dst_root,
            {
                let assignment = assignment.clone();
                let heartbeat_attempts = heartbeat_attempts.clone();
                move || {
                retry_transfer_worker_rpc_with_backoff(
                    &assignment,
                    "checkpoint",
                    "test-checkpoint",
                    BackoffConfig {
                        initial_secs: 0,
                        max_secs: 0,
                    },
                    WarnConfig {
                        warn_interval_secs: 0,
                    },
                    || {
                        let attempt =
                            heartbeat_attempts.fetch_add(1, Ordering::SeqCst) + 1;
                        if attempt < 3 {
                            return Err(TransferWorkerRpcFailure::Retryable {
                                detail: format!(
                                    "transient heartbeat failure attempt={}",
                                    attempt
                                ),
                            });
                        }
                        Ok(())
                    },
                )
                .map_err(TransferWorkerExecutionError::fatal)
            }
            },
            {
                let file_bytes = file_bytes.clone();
                move |file, read_offset, _length| {
                if file.relpath != "dir/file.bin" {
                    return Err(TransferWorkerExecutionError::fatal(resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
                        detail: format!("unexpected file relpath: {}", file.relpath),
                    }))));
                }
                if read_offset == 0 {
                    return Ok(file_bytes.clone());
                }
                Ok(Vec::new())
            }
            },
        )
        .unwrap();

        assert!(heartbeat_attempts.load(Ordering::SeqCst) >= 3);
        assert_eq!(result.file_results.len(), 1);
        assert_eq!(
            fs::read(root.path().join("dir/file.bin")).unwrap(),
            file_bytes
        );
    }

    #[test]
    fn execute_transfer_worker_assignment_retries_read_chunk_until_success() {
        let root = TempDir::new().unwrap();
        let dst_root = root.path().to_path_buf();
        let file_bytes = b"payload".to_vec();
        let assignment = test_worker_assignment("dir/file.bin", file_bytes.len() as i64);
        let read_attempts = Arc::new(AtomicUsize::new(0));
        let result = execute_transfer_worker_assignment(
            &assignment,
            &dst_root,
            || Ok(()),
            {
                let assignment = assignment.clone();
                let file_bytes = file_bytes.clone();
                let read_attempts = read_attempts.clone();
                move |file, read_offset, _length| {
                if file.relpath != "dir/file.bin" {
                    return Err(TransferWorkerExecutionError::fatal(resp_err_kverr(KvError::Api(ApiError::InvalidArgument {
                        detail: format!("unexpected file relpath: {}", file.relpath),
                    }))));
                }
                let op_detail = format!(
                    "test-read relpath={} offset={}",
                    file.relpath, read_offset
                );
                retry_transfer_worker_rpc_with_backoff(
                    &assignment,
                    "read_chunk",
                    op_detail.as_str(),
                    BackoffConfig {
                        initial_secs: 0,
                        max_secs: 0,
                    },
                    WarnConfig {
                        warn_interval_secs: 0,
                    },
                    || {
                        if read_offset == 0 {
                            let attempt = read_attempts.fetch_add(1, Ordering::SeqCst) + 1;
                            if attempt < 3 {
                                return Err(TransferWorkerRpcFailure::Retryable {
                                    detail: format!(
                                        "transient read failure attempt={}",
                                        attempt
                                    ),
                                });
                            }
                            return Ok(file_bytes.clone());
                        }
                        Ok(Vec::new())
                    },
                )
                .map_err(TransferWorkerExecutionError::fatal)
            }
            },
        )
        .unwrap();

        assert_eq!(read_attempts.load(Ordering::SeqCst), 3);
        assert_eq!(result.file_results.len(), 1);
        assert_eq!(
            fs::read(root.path().join("dir/file.bin")).unwrap(),
            file_bytes
        );
    }

    #[test]
    fn execute_transfer_worker_assignment_stops_before_visible_promotion() {
        let root = TempDir::new().unwrap();
        let dst_root = root.path().to_path_buf();
        let file_bytes = b"hello".to_vec();
        let assignment = test_worker_assignment("dir/file.bin", file_bytes.len() as i64);
        let checkpoint_calls = Arc::new(AtomicUsize::new(0));
        let result = execute_transfer_worker_assignment(
            &assignment,
            &dst_root,
            {
                let checkpoint_calls = checkpoint_calls.clone();
                move || {
                let calls = checkpoint_calls.fetch_add(1, Ordering::SeqCst) + 1;
                if calls >= 4 {
                    return Err(TransferWorkerExecutionError::Stop(
                        FluxonFsTransferWorkerStopReasonWire::Superseded,
                    ));
                }
                Ok(())
            }
            },
            {
                let file_bytes = file_bytes.clone();
                move |_file, read_offset, _length| {
                if read_offset == 0 {
                    return Ok(file_bytes.clone());
                }
                Ok(Vec::new())
            }
            },
        );
        assert!(matches!(
            result,
            Err(TransferWorkerExecutionError::Stop(
                FluxonFsTransferWorkerStopReasonWire::Superseded
            ))
        ));
        assert!(!root.path().join("dir/file.bin").exists());
        assert_eq!(
            fs::read(
                root.path()
                    .join(".fluxon.stage/job/batch/dir/file.bin/file.bin.fluxon.part")
            )
            .unwrap(),
            file_bytes
        );
    }

    #[test]
    fn execute_transfer_worker_assignment_can_run_multiple_file_lanes() {
        let root = TempDir::new().unwrap();
        let dst_root = root.path().to_path_buf();
        let assignment = FluxonFsTransferWorkerAssignmentWire {
            manifest_blob: build_transfer_manifest_blob(
                vec![
                    FluxonFsTransferScanFrontierEntry {
                        relpath: "dir/a.bin".to_string(),
                        size: 3,
                    },
                    FluxonFsTransferScanFrontierEntry {
                        relpath: "dir/b.bin".to_string(),
                        size: 3,
                    },
                ],
                Vec::new(),
            )
            .unwrap(),
            ..test_worker_assignment("dir/a.bin", 3)
        };
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let policy = TransferWorkerLanePolicy {
            initial_file_lanes: 1,
            max_file_lanes: 2,
            target_goodput_bytes_per_sec: i64::MAX,
            lane_ramp_interval: Duration::from_millis(50),
            lane_poll_interval: Duration::from_millis(10),
            min_improvement_percent: 0,
        };
        let result = execute_transfer_worker_assignment_with_policy(
            &assignment,
            &dst_root,
            policy,
            || Ok(()),
            {
                let in_flight = in_flight.clone();
                let max_in_flight = max_in_flight.clone();
                move |_file, read_offset, _length| {
                    if read_offset >= 3 {
                        return Ok(Vec::new());
                    }
                    let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    loop {
                        let observed = max_in_flight.load(Ordering::SeqCst);
                        if current <= observed {
                            break;
                        }
                        if max_in_flight
                            .compare_exchange(
                                observed,
                                current,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            )
                            .is_ok()
                        {
                            break;
                        }
                    }
                    std::thread::sleep(Duration::from_millis(70));
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    Ok(vec![b'x'])
                }
            },
        )
        .unwrap();

        assert_eq!(result.file_results.len(), 2);
        assert!(max_in_flight.load(Ordering::SeqCst) >= 2);
        assert_eq!(fs::read(root.path().join("dir/a.bin")).unwrap(), b"xxx".to_vec());
        assert_eq!(fs::read(root.path().join("dir/b.bin")).unwrap(), b"xxx".to_vec());
    }

    #[test]
    fn execute_transfer_worker_assignment_materializes_collect_info_after_files() {
        let root = TempDir::new().unwrap();
        let dst_root = root.path().to_path_buf();
        let file_bytes = b"hello".to_vec();
        let collect_infos = build_symlink_collect_infos(vec![FluxonFsTransferSymlinkNoticeEntryWire {
            relpath: "dir/link.bin".to_string(),
            link_target: "dir/file.bin".to_string(),
        }])
        .unwrap();
        let assignment = FluxonFsTransferWorkerAssignmentWire {
            collect_infos: collect_infos.clone(),
            ..test_worker_assignment("dir/file.bin", file_bytes.len() as i64)
        };
        let result = execute_transfer_worker_assignment(
            &assignment,
            &dst_root,
            || Ok(()),
            {
                let file_bytes = file_bytes.clone();
                move |_file, read_offset, _length| {
                    if read_offset == 0 {
                        return Ok(file_bytes.clone());
                    }
                    Ok(Vec::new())
                }
            },
        )
        .unwrap();

        assert_eq!(result.file_results.len(), 1);
        assert_eq!(result.collect_info_results.len(), 1);
        assert_eq!(
            fs::read(root.path().join("dir/file.bin")).unwrap(),
            file_bytes
        );
        assert_eq!(
            result.collect_info_results[0].output_relpath,
            "fluxon_collect_info/batches/batch/symlinks.jsonl"
        );
        assert_eq!(
            fs::read(root.path().join("fluxon_collect_info/batches/batch/symlinks.jsonl")).unwrap(),
            collect_infos[0].collect_blob
        );
    }

    #[test]
    fn execute_transfer_worker_assignment_records_failed_file_and_continues() {
        let root = TempDir::new().unwrap();
        let dst_root = root.path().to_path_buf();
        let assignment = FluxonFsTransferWorkerAssignmentWire {
            manifest_blob: build_transfer_manifest_blob(vec![
                FluxonFsTransferScanFrontierEntry {
                    relpath: "dir/good.bin".to_string(),
                    size: 5,
                },
                FluxonFsTransferScanFrontierEntry {
                    relpath: "dir/bad.bin".to_string(),
                    size: 5,
                },
            ], Vec::new())
            .unwrap(),
            ..test_worker_assignment("dir/good.bin", 5)
        };
        let result = execute_transfer_worker_assignment(
            &assignment,
            &dst_root,
            || Ok(()),
            move |file, read_offset, _length| {
                if file.relpath == "dir/good.bin" {
                    if read_offset == 0 {
                        return Ok(b"hello".to_vec());
                    }
                    return Ok(Vec::new());
                }
                if read_offset == 0 {
                    return Ok(b"he".to_vec());
                }
                Ok(Vec::new())
            },
        )
        .unwrap();

        assert_eq!(result.file_results.len(), 1);
        assert_eq!(result.file_results[0].relpath, "dir/good.bin");
        assert_eq!(result.failed_file_results.len(), 1);
        assert_eq!(result.failed_file_results[0].relpath, "dir/bad.bin");
        assert_eq!(
            result.failed_file_results[0].reason_kind,
            FluxonFsTransferFailedFileReasonKindWire::SourceContentChanged
        );
        assert!(root.path().join("dir/good.bin").exists());
        assert!(!root.path().join("dir/bad.bin").exists());
    }

    #[test]
    fn execute_transfer_worker_assignment_records_permission_denied_file_and_continues() {
        let root = TempDir::new().unwrap();
        let dst_root = root.path().to_path_buf();
        let assignment = FluxonFsTransferWorkerAssignmentWire {
            manifest_blob: build_transfer_manifest_blob(
                vec![
                    FluxonFsTransferScanFrontierEntry {
                        relpath: "dir/good.bin".to_string(),
                        size: 5,
                    },
                    FluxonFsTransferScanFrontierEntry {
                        relpath: "dir/denied.bin".to_string(),
                        size: 5,
                    },
                ],
                Vec::new(),
            )
            .unwrap(),
            ..test_worker_assignment("dir/good.bin", 5)
        };
        let result = execute_transfer_worker_assignment(
            &assignment,
            &dst_root,
            || Ok(()),
            move |file, read_offset, _length| {
                if file.relpath == "dir/good.bin" {
                    if read_offset == 0 {
                        return Ok(b"hello".to_vec());
                    }
                    return Ok(Vec::new());
                }
                Err(TransferWorkerExecutionError::fatal(resp_err_io(
                    std::io::Error::from_raw_os_error(libc::EACCES),
                )))
            },
        )
        .unwrap();

        assert_eq!(result.file_results.len(), 1);
        assert_eq!(result.file_results[0].relpath, "dir/good.bin");
        assert_eq!(result.failed_file_results.len(), 1);
        assert_eq!(result.failed_file_results[0].relpath, "dir/denied.bin");
        assert_eq!(
            result.failed_file_results[0].reason_kind,
            FluxonFsTransferFailedFileReasonKindWire::SourcePermissionDenied
        );
        assert!(!result.failed_file_results[0].reason_detail.is_empty());
        assert!(root.path().join("dir/good.bin").exists());
        assert!(!root.path().join("dir/denied.bin").exists());
    }

    #[test]
    fn transfer_worker_heartbeat_gate_serializes_concurrent_heartbeat_attempts() {
        let gate = Arc::new(TransferWorkerHeartbeatGate::new(
            chrono::Utc::now().timestamp_millis() + 60_000,
        ));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let attempt_count = Arc::new(AtomicUsize::new(0));
        let start_barrier = Arc::new(std::sync::Barrier::new(8));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let gate2 = gate.clone();
            let in_flight2 = in_flight.clone();
            let max_in_flight2 = max_in_flight.clone();
            let attempt_count2 = attempt_count.clone();
            let start_barrier2 = start_barrier.clone();
            handles.push(thread::spawn(move || {
                start_barrier2.wait();
                gate2
                    .ensure_continue(true, 0, |_heartbeat_unix_ms, heartbeat_detail| {
                        assert!(matches!(heartbeat_detail, "initial" | "lease_refresh"));
                        let current = in_flight2.fetch_add(1, Ordering::SeqCst) + 1;
                        loop {
                            let observed = max_in_flight2.load(Ordering::SeqCst);
                            if current <= observed {
                                break;
                            }
                            if max_in_flight2
                                .compare_exchange(
                                    observed,
                                    current,
                                    Ordering::SeqCst,
                                    Ordering::SeqCst,
                                )
                                .is_ok()
                            {
                                break;
                            }
                        }
                        attempt_count2.fetch_add(1, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(50));
                        in_flight2.fetch_sub(1, Ordering::SeqCst);
                        Ok(FluxonFsTransferWorkerHeartbeatResultWire::continue_running(
                            chrono::Utc::now().timestamp_millis() + 60_000,
                        ))
                    })
                    .unwrap();
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(attempt_count.load(Ordering::SeqCst), 1);
        assert_eq!(max_in_flight.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn transfer_worker_heartbeat_gate_triggers_on_empty_dir_progress() {
        let gate = TransferWorkerHeartbeatGate::new(chrono::Utc::now().timestamp_millis() + 60_000);
        gate.ensure_continue(true, 0, |_heartbeat_unix_ms, heartbeat_detail| {
            assert_eq!(heartbeat_detail, "initial");
            Ok(FluxonFsTransferWorkerHeartbeatResultWire::continue_running(
                chrono::Utc::now().timestamp_millis() + 60_000,
            ))
        })
        .unwrap();

        gate.ensure_continue(
            false,
            TRANSFER_WORKER_HEARTBEAT_EMPTY_DIR_PROGRESS_COUNT.saturating_sub(1),
            |_heartbeat_unix_ms, _heartbeat_detail| {
                panic!("unexpected heartbeat before empty-dir progress threshold")
            },
        )
        .unwrap();

        let progress_heartbeat_count = Arc::new(AtomicUsize::new(0));
        gate.ensure_continue(
            false,
            TRANSFER_WORKER_HEARTBEAT_EMPTY_DIR_PROGRESS_COUNT,
            {
                let progress_heartbeat_count = progress_heartbeat_count.clone();
                move |_heartbeat_unix_ms, heartbeat_detail| {
                    assert_eq!(heartbeat_detail, "empty_dir_progress");
                    progress_heartbeat_count.fetch_add(1, Ordering::SeqCst);
                    Ok(FluxonFsTransferWorkerHeartbeatResultWire::continue_running(
                        chrono::Utc::now().timestamp_millis() + 60_000,
                    ))
                }
            },
        )
        .unwrap();

        gate.ensure_continue(
            false,
            TRANSFER_WORKER_HEARTBEAT_EMPTY_DIR_PROGRESS_COUNT,
            |_heartbeat_unix_ms, _heartbeat_detail| {
                panic!("unexpected repeated heartbeat without new empty-dir progress")
            },
        )
        .unwrap();

        assert_eq!(progress_heartbeat_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn transfer_worker_heartbeat_gate_releases_inflight_after_retryable_failure() {
        let gate = Arc::new(TransferWorkerHeartbeatGate::new(
            chrono::Utc::now().timestamp_millis() + 60_000,
        ));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let attempt_count = Arc::new(AtomicUsize::new(0));
        let retry_observed = Arc::new(AtomicBool::new(false));
        let success_count = Arc::new(AtomicUsize::new(0));
        let start_barrier = Arc::new(std::sync::Barrier::new(2));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let gate2 = gate.clone();
            let in_flight2 = in_flight.clone();
            let max_in_flight2 = max_in_flight.clone();
            let attempt_count2 = attempt_count.clone();
            let retry_observed2 = retry_observed.clone();
            let success_count2 = success_count.clone();
            let start_barrier2 = start_barrier.clone();
            handles.push(thread::spawn(move || {
                start_barrier2.wait();
                loop {
                    match gate2.ensure_continue(true, 0, |_heartbeat_unix_ms, heartbeat_detail| {
                        assert!(matches!(heartbeat_detail, "initial" | "lease_refresh"));
                        let current = in_flight2.fetch_add(1, Ordering::SeqCst) + 1;
                        loop {
                            let observed = max_in_flight2.load(Ordering::SeqCst);
                            if current <= observed {
                                break;
                            }
                            if max_in_flight2
                                .compare_exchange(
                                    observed,
                                    current,
                                    Ordering::SeqCst,
                                    Ordering::SeqCst,
                                )
                                .is_ok()
                            {
                                break;
                            }
                        }
                        let attempt = attempt_count2.fetch_add(1, Ordering::SeqCst) + 1;
                        std::thread::sleep(Duration::from_millis(20));
                        in_flight2.fetch_sub(1, Ordering::SeqCst);
                        if attempt == 1 {
                            retry_observed2.store(true, Ordering::SeqCst);
                            return Err(TransferWorkerRpcFailure::Retryable {
                                detail: "transient heartbeat timeout".to_string(),
                            });
                        }
                        Ok(FluxonFsTransferWorkerHeartbeatResultWire::continue_running(
                            chrono::Utc::now().timestamp_millis() + 60_000,
                        ))
                    }) {
                        Ok(()) => {
                            success_count2.fetch_add(1, Ordering::SeqCst);
                            return;
                        }
                        Err(TransferWorkerHeartbeatGateError::Retryable { .. }) => continue,
                        Err(TransferWorkerHeartbeatGateError::Terminal(err)) => {
                            panic!("unexpected terminal heartbeat error: {:?}", err);
                        }
                    }
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        assert!(retry_observed.load(Ordering::SeqCst));
        assert_eq!(success_count.load(Ordering::SeqCst), 2);
        assert!(attempt_count.load(Ordering::SeqCst) >= 2);
        assert_eq!(max_in_flight.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn cleanup_transfer_worker_attempt_artifacts_removes_empty_stage_tree_after_success() {
        let root = TempDir::new().unwrap();
        let dst_root = root.path().to_path_buf();
        let file_bytes = b"hello".to_vec();
        let assignment = test_worker_assignment("dir/file.bin", file_bytes.len() as i64);
        let result = execute_transfer_worker_assignment(
            &assignment,
            &dst_root,
            || Ok(()),
            {
                let file_bytes = file_bytes.clone();
                move |_file, read_offset, _length| {
                    if read_offset == 0 {
                        return Ok(file_bytes.clone());
                    }
                    Ok(Vec::new())
                }
            },
        )
        .unwrap();

        assert_eq!(result.file_results.len(), 1);
        assert!(root.path().join(".fluxon.stage/job/batch").exists());

        cleanup_transfer_worker_attempt_artifacts(&dst_root, &assignment).unwrap();

        assert_eq!(fs::read(root.path().join("dir/file.bin")).unwrap(), file_bytes);
        assert!(!root.path().join(".fluxon.stage").exists());
    }

    #[test]
    fn cleanup_transfer_worker_attempt_artifacts_removes_stop_leftovers_and_collect_info_parts() {
        let root = TempDir::new().unwrap();
        let dst_root = root.path().to_path_buf();
        let file_bytes = b"hello".to_vec();
        let collect_infos = build_symlink_collect_infos(vec![FluxonFsTransferSymlinkNoticeEntryWire {
            relpath: "root/link-file.bin".to_string(),
            link_target: "target/file.bin".to_string(),
        }])
        .unwrap();
        let assignment = FluxonFsTransferWorkerAssignmentWire {
            collect_infos: collect_infos.clone(),
            ..test_worker_assignment("dir/file.bin", file_bytes.len() as i64)
        };
        let prepared_collect = prepare_transfer_collect_info_materialization(
            &dst_root,
            assignment.batch_id.as_str(),
            assignment.worker_task_id.as_str(),
            &collect_infos[0],
        )
        .unwrap();
        let result = execute_transfer_worker_assignment(
            &assignment,
            &dst_root,
            || Err(TransferWorkerExecutionError::Stop(
                FluxonFsTransferWorkerStopReasonWire::Superseded,
            )),
            {
                let file_bytes = file_bytes.clone();
                move |_file, read_offset, _length| {
                    if read_offset == 0 {
                        return Ok(file_bytes.clone());
                    }
                    Ok(Vec::new())
                }
            },
        );

        assert!(matches!(
            result,
            Err(TransferWorkerExecutionError::Stop(
                FluxonFsTransferWorkerStopReasonWire::Superseded
            ))
        ));
        assert!(root.path().join(prepared_collect.staging_relpath.as_str()).exists());

        cleanup_transfer_worker_attempt_artifacts(&dst_root, &assignment).unwrap();

        assert!(!root.path().join(".fluxon.stage").exists());
        assert!(!root.path().join(prepared_collect.staging_relpath.as_str()).exists());
    }
}
