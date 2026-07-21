use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Context;
use fluxon_fs_core::config::{
    FluxonFsAccessModel, FluxonFsAccessUser, FluxonFsGlobalConfig, FluxonFsLocalTransferCheckJobSpecWire, FluxonFsS3GatewayConfig,
    FluxonFsS3KvMissPolicy, FluxonFsTransferBatchKind, FluxonFsTransferBatchState,
    FluxonFsTransferBatchCollectInfoWire, FluxonFsTransferManifestWire,
    FluxonFsTransferScanMode,
    FluxonFsTransferDispositionWire, FluxonFsTransferScanAssignmentWire,
    normalize_transfer_skip_entries,
    FluxonFsTransferSkipEntryKind, FluxonFsTransferSkipEntryWire,
    FluxonFsTransferStateStoreConfig, FluxonFsTransferWorkerAssignmentWire,
    FluxonFsTransferWorkerStopReasonWire,
    FluxonFsTransferWorkerHeartbeatWire,
    FLUXON_FS_LOCAL_TRANSFER_CHECK_DST_EXPORT, FLUXON_FS_LOCAL_TRANSFER_CHECK_SRC_EXPORT,
};
#[cfg(test)]
use fluxon_fs_core::config::{
    FluxonFsTransferStateStoreKind, FluxonFsTransferStateStoreTiKvConfig,
};
use fluxon_fs_s3_gateway::{
    DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY, FsMasterAdminBackend, FsTransferBatchRecord,
    FsTransferCreateJobArg, FsTransferJobRecord, GatewayAccessConfig, GatewayState, S3Error,
};
use fluxon_kv::user_api::flat_dict::{FlatDict, FlatValue};
use futures::future::BoxFuture;
use parking_lot::Mutex;
use uuid::Uuid;

use crate::agent_service::transfer_agent::{
    TransferWorkerExecutionError, build_transfer_scan_result_for_root_dir_abs,
    cleanup_transfer_worker_attempt_artifacts,
    execute_transfer_worker_assignment,
    read_transfer_chunk_from_root_dir_abs,
};
use crate::master_http::{TRANSFER_HEARTBEAT_EXTENSION_MS, TRANSFER_WORKER_LEASE_MS};

const LOCAL_CHECK_SRC_EXPORT: &str = FLUXON_FS_LOCAL_TRANSFER_CHECK_SRC_EXPORT;
const LOCAL_CHECK_DST_EXPORT: &str = FLUXON_FS_LOCAL_TRANSFER_CHECK_DST_EXPORT;
const LOCAL_TRANSFER_WORKER_COUNT: i64 = 1;

#[cfg(test)]
const LOCAL_TRANSFER_TEST_SLEEP_AFTER_APPLY_MS_ENV: &str =
    "FLUXON_FS_LOCAL_TRANSFER_TEST_SLEEP_AFTER_APPLY_MS";

#[derive(Debug, Clone)]
pub struct LocalTransferCheckArg {
    pub src_root_dir: String,
    pub transfer_state_store: FluxonFsTransferStateStoreConfig,
    pub batch_ready_bytes: i64,
    pub skip_entries: Vec<LocalTransferSkipEntry>,
    pub checker_concurrency_limit: Option<usize>,
    pub enable_cli_progress: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LocalTransferSkipEntryKind {
    Dir,
    File,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LocalTransferSkipEntry {
    pub kind: LocalTransferSkipEntryKind,
    pub relpath: String,
}

#[derive(Debug, Clone)]
struct LocalScanUnit {
    scan_unit_id: String,
    root_relpath: String,
    generation: i64,
    scan_mode: FluxonFsTransferScanMode,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LocalTransferCheckSummary {
    pub job_id: String,
    pub scan_epoch: i64,
    pub batch_count: usize,
    pub full_dir_batch_count: usize,
    pub direct_files_only_batch_count: usize,
}

#[derive(Debug, Clone)]
pub struct LocalTransferRunArg {
    pub src_root_dir: String,
    pub dst_root_dir: String,
    pub transfer_state_store: FluxonFsTransferStateStoreConfig,
    pub batch_ready_bytes: i64,
    pub skip_entries: Vec<LocalTransferSkipEntry>,
    pub checker_concurrency_limit: Option<usize>,
    pub enable_cli_progress: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LocalTransferManifestEntrySummary {
    pub relpath: String,
    pub size: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LocalTransferBatchInspectSummary {
    pub batch_id: String,
    pub root_relpath: String,
    pub batch_kind: String,
    pub state: String,
    pub generation: i64,
    pub file_count: i64,
    pub total_bytes: i64,
    pub empty_dir_count: i64,
    pub entries: Vec<LocalTransferManifestEntrySummary>,
    pub empty_dir_relpaths: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LocalTransferCollectInfoInspectSummary {
    pub batch_id: String,
    pub collect_kind: String,
    pub collect_blob_bytes: i64,
    pub materialized: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LocalTransferWorkerAttemptInspectSummary {
    pub batch_id: String,
    pub worker_id: String,
    pub worker_task_id: String,
    pub dst_exporter_id: String,
    pub state: String,
    pub launch_attempt_count: i64,
    pub visible_file_count: i64,
    pub visible_bytes: i64,
    pub last_error: String,
    pub stop_reason: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LocalTransferJobInspectSummary {
    pub job_id: String,
    pub scan_epoch: i64,
    pub scan_finished: bool,
    pub job_state: String,
    pub open_batches: i64,
    pub running_batch_count: usize,
    pub batches: Vec<LocalTransferBatchInspectSummary>,
    pub worker_attempts: Vec<LocalTransferWorkerAttemptInspectSummary>,
    pub collect_infos: Vec<LocalTransferCollectInfoInspectSummary>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LocalTransferBatchStatusSummary {
    pub batch_id: String,
    pub root_relpath: String,
    pub batch_kind: String,
    pub state: String,
    pub generation: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LocalTransferJobStatusSummary {
    pub job_id: String,
    pub scan_epoch: i64,
    pub scan_finished: bool,
    pub job_state: String,
    pub open_batches: i64,
    pub running_batch_count: usize,
    pub batches: Vec<LocalTransferBatchStatusSummary>,
    pub worker_attempts: Vec<LocalTransferWorkerAttemptInspectSummary>,
    pub collect_infos: Vec<LocalTransferCollectInfoInspectSummary>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LocalTransferRunSummary {
    pub job_id: String,
    pub scan_epoch: i64,
    pub batch_count: usize,
    pub full_dir_batch_count: usize,
    pub direct_files_only_batch_count: usize,
    pub finished_batch_count: usize,
    pub materialized_collect_info_count: usize,
    pub completed: bool,
    pub open_batches: i64,
}

#[derive(Default)]
struct LocalTransferProgressState {
    active_checkers: AtomicUsize,
    pending_scan_units: AtomicUsize,
    completed_scan_units: AtomicUsize,
    discovered_symlink_notices: AtomicUsize,
    stop: AtomicBool,
}

#[derive(Clone, Default)]
struct LocalNullBackend;

impl fluxon_fs_s3_gateway::FsS3Backend for LocalNullBackend {
    fn stat(
        &self,
        _request_identity: fluxon_fs_core::config::FluxonFsRequestIdentity,
        _export_name: Arc<str>,
        _relpath: Arc<str>,
    ) -> BoxFuture<'static, Result<fluxon_fs_s3_gateway::RemoteStat, S3Error>> {
        Box::pin(async move {
            Err(S3Error::Internal {
                detail: "local transfer checker backend does not support stat".to_string(),
            })
        })
    }

    fn list_dir(
        &self,
        _request_identity: fluxon_fs_core::config::FluxonFsRequestIdentity,
        _export_name: Arc<str>,
        _relpath: Arc<str>,
    ) -> BoxFuture<'static, Result<Vec<fluxon_fs_s3_gateway::RemoteDirEntry>, S3Error>> {
        Box::pin(async move {
            Err(S3Error::Internal {
                detail: "local transfer checker backend does not support list_dir".to_string(),
            })
        })
    }

    fn read_chunk_cached(
        &self,
        _request_identity: fluxon_fs_core::config::FluxonFsRequestIdentity,
        _export_name: Arc<str>,
        _relpath: Arc<str>,
        _offset: i64,
        _length: i64,
        _file_size: i64,
        _mtime_ns: i64,
    ) -> BoxFuture<'static, Result<Vec<u8>, S3Error>> {
        Box::pin(async move {
            Err(S3Error::Internal {
                detail: "local transfer checker backend does not support read_chunk_cached".to_string(),
            })
        })
    }

    fn write_chunk(
        &self,
        _request_identity: fluxon_fs_core::config::FluxonFsRequestIdentity,
        _export_name: Arc<str>,
        _relpath: Arc<str>,
        _offset: i64,
        _data: Vec<u8>,
    ) -> BoxFuture<'static, Result<(), S3Error>> {
        Box::pin(async move {
            Err(S3Error::Internal {
                detail: "local transfer checker backend does not support write_chunk".to_string(),
            })
        })
    }

    fn truncate(
        &self,
        _request_identity: fluxon_fs_core::config::FluxonFsRequestIdentity,
        _export_name: Arc<str>,
        _relpath: Arc<str>,
        _size: i64,
    ) -> BoxFuture<'static, Result<(), S3Error>> {
        Box::pin(async move {
            Err(S3Error::Internal {
                detail: "local transfer checker backend does not support truncate".to_string(),
            })
        })
    }

    fn mkdir(
        &self,
        _request_identity: fluxon_fs_core::config::FluxonFsRequestIdentity,
        _export_name: Arc<str>,
        _relpath: Arc<str>,
        _mode: i64,
    ) -> BoxFuture<'static, Result<(), S3Error>> {
        Box::pin(async move {
            Err(S3Error::Internal {
                detail: "local transfer checker backend does not support mkdir".to_string(),
            })
        })
    }

    fn rename(
        &self,
        _request_identity: fluxon_fs_core::config::FluxonFsRequestIdentity,
        _export_name: Arc<str>,
        _src_relpath: Arc<str>,
        _dst_relpath: Arc<str>,
    ) -> BoxFuture<'static, Result<(), S3Error>> {
        Box::pin(async move {
            Err(S3Error::Internal {
                detail: "local transfer checker backend does not support rename".to_string(),
            })
        })
    }

    fn unlink(
        &self,
        _request_identity: fluxon_fs_core::config::FluxonFsRequestIdentity,
        _export_name: Arc<str>,
        _relpath: Arc<str>,
    ) -> BoxFuture<'static, Result<(), S3Error>> {
        Box::pin(async move {
            Err(S3Error::Internal {
                detail: "local transfer checker backend does not support unlink".to_string(),
            })
        })
    }

    fn rmdir(
        &self,
        _request_identity: fluxon_fs_core::config::FluxonFsRequestIdentity,
        _export_name: Arc<str>,
        _relpath: Arc<str>,
    ) -> BoxFuture<'static, Result<(), S3Error>> {
        Box::pin(async move {
            Err(S3Error::Internal {
                detail: "local transfer checker backend does not support rmdir".to_string(),
            })
        })
    }
}

#[derive(Clone, Default)]
struct LocalNullMasterAdminBackend;

impl FsMasterAdminBackend for LocalNullMasterAdminBackend {
    fn list_fs_master_members(
        &self,
    ) -> BoxFuture<'static, Result<Vec<fluxon_fs_s3_gateway::FsMasterMemberRecord>, String>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn list_fs_master_online_member_ids(
        &self,
    ) -> BoxFuture<'static, Result<std::collections::BTreeSet<String>, String>> {
        Box::pin(async { Ok(std::collections::BTreeSet::new()) })
    }

    fn list_fs_master_agent_dir(
        &self,
        _agent_instance_key: String,
        _dir_abs: String,
    ) -> BoxFuture<'static, Result<Vec<fluxon_fs_s3_gateway::FsMasterAdminBrowseDirEntry>, String>>
    {
        Box::pin(async { Ok(Vec::new()) })
    }
}

#[derive(Clone)]
struct LocalFsStatBackend {
    dst_root: PathBuf,
}

impl fluxon_fs_s3_gateway::FsS3Backend for LocalFsStatBackend {
    fn stat(
        &self,
        _request_identity: fluxon_fs_core::config::FluxonFsRequestIdentity,
        _export_name: Arc<str>,
        relpath: Arc<str>,
    ) -> BoxFuture<'static, Result<fluxon_fs_s3_gateway::RemoteStat, S3Error>> {
        let dst_root = self.dst_root.clone();
        let relpath = relpath.to_string();
        Box::pin(async move {
            let abs = dst_root.join(relpath.as_str());
            let md = match fs::metadata(&abs) {
                Ok(v) => v,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(fluxon_fs_s3_gateway::RemoteStat {
                        exists: false,
                        is_file: false,
                        is_dir: false,
                        size: 0,
                        mtime_ns: 0,
                    });
                }
                Err(e) => {
                    return Err(S3Error::Internal {
                        detail: format!("local transfer stat failed: path={} err={}", abs.display(), e),
                    });
                }
            };
            Ok(fluxon_fs_s3_gateway::RemoteStat {
                exists: true,
                is_file: md.is_file(),
                is_dir: md.is_dir(),
                size: md.len().min(i64::MAX as u64) as i64,
                mtime_ns: 0,
            })
        })
    }

    fn list_dir(
        &self,
        _request_identity: fluxon_fs_core::config::FluxonFsRequestIdentity,
        _export_name: Arc<str>,
        _relpath: Arc<str>,
    ) -> BoxFuture<'static, Result<Vec<fluxon_fs_s3_gateway::RemoteDirEntry>, S3Error>> {
        Box::pin(async move {
            Err(S3Error::Internal {
                detail: "local transfer stat backend does not support list_dir".to_string(),
            })
        })
    }

    fn read_chunk_cached(
        &self,
        _request_identity: fluxon_fs_core::config::FluxonFsRequestIdentity,
        _export_name: Arc<str>,
        _relpath: Arc<str>,
        _offset: i64,
        _length: i64,
        _file_size: i64,
        _mtime_ns: i64,
    ) -> BoxFuture<'static, Result<Vec<u8>, S3Error>> {
        Box::pin(async move {
            Err(S3Error::Internal {
                detail: "local transfer stat backend does not support read_chunk_cached".to_string(),
            })
        })
    }

    fn write_chunk(
        &self,
        _request_identity: fluxon_fs_core::config::FluxonFsRequestIdentity,
        _export_name: Arc<str>,
        _relpath: Arc<str>,
        _offset: i64,
        _data: Vec<u8>,
    ) -> BoxFuture<'static, Result<(), S3Error>> {
        Box::pin(async move {
            Err(S3Error::Internal {
                detail: "local transfer stat backend does not support write_chunk".to_string(),
            })
        })
    }

    fn truncate(
        &self,
        _request_identity: fluxon_fs_core::config::FluxonFsRequestIdentity,
        _export_name: Arc<str>,
        _relpath: Arc<str>,
        _size: i64,
    ) -> BoxFuture<'static, Result<(), S3Error>> {
        Box::pin(async move {
            Err(S3Error::Internal {
                detail: "local transfer stat backend does not support truncate".to_string(),
            })
        })
    }

    fn mkdir(
        &self,
        _request_identity: fluxon_fs_core::config::FluxonFsRequestIdentity,
        _export_name: Arc<str>,
        _relpath: Arc<str>,
        _mode: i64,
    ) -> BoxFuture<'static, Result<(), S3Error>> {
        Box::pin(async move {
            Err(S3Error::Internal {
                detail: "local transfer stat backend does not support mkdir".to_string(),
            })
        })
    }

    fn rename(
        &self,
        _request_identity: fluxon_fs_core::config::FluxonFsRequestIdentity,
        _export_name: Arc<str>,
        _src_relpath: Arc<str>,
        _dst_relpath: Arc<str>,
    ) -> BoxFuture<'static, Result<(), S3Error>> {
        Box::pin(async move {
            Err(S3Error::Internal {
                detail: "local transfer stat backend does not support rename".to_string(),
            })
        })
    }

    fn unlink(
        &self,
        _request_identity: fluxon_fs_core::config::FluxonFsRequestIdentity,
        _export_name: Arc<str>,
        _relpath: Arc<str>,
    ) -> BoxFuture<'static, Result<(), S3Error>> {
        Box::pin(async move {
            Err(S3Error::Internal {
                detail: "local transfer stat backend does not support unlink".to_string(),
            })
        })
    }

    fn rmdir(
        &self,
        _request_identity: fluxon_fs_core::config::FluxonFsRequestIdentity,
        _export_name: Arc<str>,
        _relpath: Arc<str>,
    ) -> BoxFuture<'static, Result<(), S3Error>> {
        Box::pin(async move {
            Err(S3Error::Internal {
                detail: "local transfer stat backend does not support rmdir".to_string(),
            })
        })
    }
}

fn build_known_dispositions_for_local_scan(
    job_id: &str,
    root_relpath: &str,
    generation: i64,
    batches: &[FsTransferBatchRecord],
    direct_files_complete_records: &[fluxon_fs_s3_gateway::FsTransferDirectFilesCompleteRecord],
) -> Vec<FluxonFsTransferDispositionWire> {
    let mut known: Vec<FluxonFsTransferDispositionWire> = batches
        .iter()
        .filter(|batch| {
            if batch.job_id != job_id {
                return false;
            }
            if batch.root_relpath == root_relpath
                && batch.batch_kind == FluxonFsTransferBatchKind::DirectFilesOnly
            {
                // Exact-root current-layer closure is represented only by the
                // durable direct-files-complete marker. A partial exact-root
                // DirectFilesOnly batch must not suppress replay of the
                // remaining direct files on the same root.
                return false;
            }
            if batch.root_relpath == root_relpath {
                return true;
            }
            if root_relpath == "." {
                return true;
            }
            let prefix = format!("{}/", root_relpath);
            batch.root_relpath.starts_with(prefix.as_str())
        })
        .map(|batch| FluxonFsTransferDispositionWire {
            root_relpath: batch.root_relpath.clone(),
            generation: batch.generation,
            batch_kind: batch.batch_kind,
        })
        .collect();
    known.extend(
        direct_files_complete_records
            .iter()
            .filter(|row| {
                if row.job_id != job_id {
                    return false;
                }
                if row.root_relpath == root_relpath {
                    return true;
                }
                if root_relpath == "." {
                    return true;
                }
                let prefix = format!("{}/", root_relpath);
                row.root_relpath.starts_with(prefix.as_str())
            })
            .map(|row| FluxonFsTransferDispositionWire {
                root_relpath: row.root_relpath.clone(),
                generation,
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
            }),
    );
    known.sort_by(|a, b| {
        a.root_relpath
            .cmp(&b.root_relpath)
            .then(a.batch_kind.as_db_str().cmp(b.batch_kind.as_db_str()))
            .then(a.generation.cmp(&b.generation))
    });
    known.dedup_by(|a, b| {
        a.root_relpath == b.root_relpath
            && a.batch_kind == b.batch_kind
            && a.generation == b.generation
    });
    known
}

fn resolve_checker_concurrency(limit: Option<usize>) -> anyhow::Result<usize> {
    if let Some(v) = limit {
        if v == 0 {
            anyhow::bail!("checker_concurrency_limit must be > 0 when specified");
        }
        return Ok(v);
    }
    let auto = thread::available_parallelism()
        .map(|v| v.get())
        .unwrap_or(1);
    Ok(std::cmp::max(1, auto))
}

fn count_symlink_notices_in_collect_blob(blob: &[u8]) -> usize {
    blob.iter().filter(|v| **v == b'\n').count()
}

fn count_symlink_notices_in_scan_result(
    result: &fluxon_fs_core::config::FluxonFsTransferScanResultWire,
) -> usize {
    let mut count = 0_usize;
    for batch in &result.direct_files_only_batches {
        for collect_info in &batch.collect_infos {
            count = count.saturating_add(count_symlink_notices_in_collect_blob(
                collect_info.collect_blob.as_slice(),
            ));
        }
    }
    for batch in &result.full_dir_batches {
        for collect_info in &batch.collect_infos {
            count = count.saturating_add(count_symlink_notices_in_collect_blob(
                collect_info.collect_blob.as_slice(),
            ));
        }
    }
    count
}

fn log_local_transfer_progress_snapshot(
    state: &GatewayState,
    job_id: &str,
    progress: &LocalTransferProgressState,
    checker_concurrency: usize,
    started_at: Instant,
    reused_existing_job: bool,
    phase: &str,
) {
    let snapshot = match state.transfer_job_snapshot(job_id) {
        Ok(Some(v)) => v,
        Ok(None) => {
            tracing::info!(
                target: "fluxon_fs::local_transfer_checker",
                "phase={} job_id={} checker_limit={} active_checkers={} pending_scan_units={} completed_scan_units={} ready_batches=unknown full_dir_batches=unknown direct_files_only_batches=unknown discovered_symlink_notices={} reused_existing_job={} elapsed_secs={} detail=job_snapshot_absent",
                phase,
                job_id,
                checker_concurrency,
                progress.active_checkers.load(Ordering::SeqCst),
                progress.pending_scan_units.load(Ordering::SeqCst),
                progress.completed_scan_units.load(Ordering::SeqCst),
                progress.discovered_symlink_notices.load(Ordering::SeqCst),
                reused_existing_job,
                started_at.elapsed().as_secs(),
            );
            return;
        }
        Err(err) => {
            tracing::info!(
                target: "fluxon_fs::local_transfer_checker",
                "phase={} job_id={} checker_limit={} active_checkers={} pending_scan_units={} completed_scan_units={} ready_batches=unknown full_dir_batches=unknown direct_files_only_batches=unknown discovered_symlink_notices={} reused_existing_job={} elapsed_secs={} detail=job_snapshot_error err={}",
                phase,
                job_id,
                checker_concurrency,
                progress.active_checkers.load(Ordering::SeqCst),
                progress.pending_scan_units.load(Ordering::SeqCst),
                progress.completed_scan_units.load(Ordering::SeqCst),
                progress.discovered_symlink_notices.load(Ordering::SeqCst),
                reused_existing_job,
                started_at.elapsed().as_secs(),
                err,
            );
            return;
        }
    };
    let ready_batches = match state.list_transfer_batches() {
        Ok(v) => v
            .into_iter()
            .filter(|batch| {
                batch.job_id == job_id && batch.state == FluxonFsTransferBatchState::Ready
            })
            .collect::<Vec<_>>(),
        Err(err) => {
            tracing::info!(
                target: "fluxon_fs::local_transfer_checker",
                "phase={} job_id={} checker_limit={} active_checkers={} pending_scan_units={} completed_scan_units={} ready_batches=unknown full_dir_batches=unknown direct_files_only_batches=unknown discovered_symlink_notices={} reused_existing_job={} elapsed_secs={} detail=list_batches_error err={}",
                phase,
                job_id,
                checker_concurrency,
                progress.active_checkers.load(Ordering::SeqCst),
                progress.pending_scan_units.load(Ordering::SeqCst),
                progress.completed_scan_units.load(Ordering::SeqCst),
                progress.discovered_symlink_notices.load(Ordering::SeqCst),
                reused_existing_job,
                started_at.elapsed().as_secs(),
                err,
            );
            return;
        }
    };
    let full_dir_batch_count = ready_batches
        .iter()
        .filter(|batch| batch.batch_kind == FluxonFsTransferBatchKind::FullDir)
        .count();
    let direct_files_only_batch_count = ready_batches
        .iter()
        .filter(|batch| batch.batch_kind == FluxonFsTransferBatchKind::DirectFilesOnly)
        .count();
    tracing::info!(
        target: "fluxon_fs::local_transfer_checker",
        "phase={} job_id={} scan_epoch={} scan_finished={} checker_limit={} active_checkers={} pending_scan_units={} completed_scan_units={} ready_batches={} full_dir_batches={} direct_files_only_batches={} discovered_symlink_notices={} reused_existing_job={} elapsed_secs={}",
        phase,
        job_id,
        snapshot.scan_epoch,
        snapshot.scan_finished,
        checker_concurrency,
        progress.active_checkers.load(Ordering::SeqCst),
        progress.pending_scan_units.load(Ordering::SeqCst),
        progress.completed_scan_units.load(Ordering::SeqCst),
        ready_batches.len(),
        full_dir_batch_count,
        direct_files_only_batch_count,
        progress.discovered_symlink_notices.load(Ordering::SeqCst),
        reused_existing_job,
        started_at.elapsed().as_secs(),
    );
}

fn spawn_local_transfer_progress_reporter(
    state: GatewayState,
    job_id: String,
    progress: Arc<LocalTransferProgressState>,
    checker_concurrency: usize,
    started_at: Instant,
    reused_existing_job: bool,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while !progress.stop.load(Ordering::SeqCst) {
            log_local_transfer_progress_snapshot(
                &state,
                job_id.as_str(),
                progress.as_ref(),
                checker_concurrency,
                started_at,
                reused_existing_job,
                "running",
            );
            for _ in 0..10 {
                if progress.stop.load(Ordering::SeqCst) {
                    return;
                }
                thread::sleep(Duration::from_millis(100));
            }
        }
    })
}

#[cfg(test)]
fn build_local_tikv_transfer_state_store_config(
    pd_endpoints: Vec<String>,
    key_prefix: String,
) -> FluxonFsTransferStateStoreConfig {
    FluxonFsTransferStateStoreConfig {
        kind: FluxonFsTransferStateStoreKind::TiKv(FluxonFsTransferStateStoreTiKvConfig {
            pd_endpoints,
            key_prefix,
        }),
    }
}

fn new_local_gateway_state(
    transfer_state_store: FluxonFsTransferStateStoreConfig,
) -> anyhow::Result<GatewayState> {
    new_local_gateway_state_with_backend(transfer_state_store, Arc::new(LocalNullBackend))
}

fn new_local_gateway_state_with_backend(
    transfer_state_store: FluxonFsTransferStateStoreConfig,
    backend: Arc<dyn fluxon_fs_s3_gateway::FsS3Backend>,
) -> anyhow::Result<GatewayState> {
    GatewayState::new(
        "local_transfer_checker".to_string(),
        String::new(),
        GatewayAccessConfig {
            // English note: local transfer check uses transfer state only.
            // Access-control tables are out of scope here, so a process-local in-memory access DB is sufficient.
            access_db_path: ":memory:".to_string(),
            bootstrap_access_model: FluxonFsAccessModel {
                users: vec![FluxonFsAccessUser {
                    username: "local_transfer_checker".to_string(),
                    password: "local_transfer_checker_pw".to_string(),
                    can_manage_users: true,
                }],
                scope_access: Vec::new(),
            },
            transfer_state_store: Some(transfer_state_store),
        },
        Arc::new(FluxonFsGlobalConfig {
            stale_window_ms: 0,
            rules: Vec::new(),
            exports: std::collections::BTreeMap::new(),
        }),
        FluxonFsS3GatewayConfig {
            get_object_inflight_pieces: 1,
            kv_miss_policy: FluxonFsS3KvMissPolicy::RemoteRead,
        },
        backend,
        Arc::new(LocalNullMasterAdminBackend),
        None,
        None,
    )
    .map_err(|e| anyhow::anyhow!("{}", e))
}

// A local check/run still uses the same batch-scoped worker identity contract
// as the distributed scheduler. Each reassignment keeps the stable worker_id
// and fences attempts through a fresh worker_task_id.
fn local_transfer_worker_id(job_id: &str, batch_id: &str) -> String {
    format!("{}__local_batch_{}", job_id, batch_id)
}

fn local_transfer_worker_resp_err(detail: impl Into<String>) -> FlatDict {
    FlatDict::from([
        ("ok".to_string(), FlatValue::Bool(false)),
        ("err".to_string(), FlatValue::String(detail.into())),
    ])
}

fn execute_local_transfer_worker_for_batch(
    state: &GatewayState,
    job_id: &str,
    batch_id: &str,
    src_root: &Path,
    dst_root: &Path,
) -> anyhow::Result<()> {
    let snapshot = state
        .transfer_job_snapshot(job_id)
        .map_err(|e| anyhow::anyhow!("{}", e))?
        .ok_or_else(|| anyhow::anyhow!("transfer job snapshot missing: job_id={}", job_id))?;
    let job = snapshot.job;
    let batch = state
        .list_transfer_batches()
        .map_err(|e| anyhow::anyhow!("{}", e))?
        .into_iter()
        .find(|batch| batch.job_id == job_id && batch.batch_id == batch_id)
        .ok_or_else(|| anyhow::anyhow!("transfer batch missing: job_id={} batch_id={}", job_id, batch_id))?;
    let worker_id = local_transfer_worker_id(job.job_id.as_str(), batch.batch_id.as_str());
    let worker_task_id = format!("{}__{}", worker_id, Uuid::new_v4());
    let now_unix_ms = chrono::Utc::now().timestamp_millis();
    let lease_expire_unix_ms = now_unix_ms + TRANSFER_WORKER_LEASE_MS;
    let collect_infos = state
        .list_transfer_batch_collect_infos()
        .map_err(|e| anyhow::anyhow!("{}", e))?
        .into_iter()
        .filter(|row| row.job_id == job_id && row.batch_id == batch_id)
        .map(|row| FluxonFsTransferBatchCollectInfoWire {
            collect_kind: row.collect_kind,
            collect_blob: row.collect_blob,
        })
        .collect::<Vec<_>>();
    state
        .assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            batch.batch_id.as_str(),
            LOCAL_CHECK_SRC_EXPORT,
            worker_id.as_str(),
            worker_task_id.as_str(),
            LOCAL_CHECK_DST_EXPORT,
            lease_expire_unix_ms,
        )
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    let assignment = FluxonFsTransferWorkerAssignmentWire {
        job_id: job.job_id.clone(),
        batch_id: batch.batch_id.clone(),
        worker_task_id: worker_task_id.clone(),
        batch_kind: batch.batch_kind,
        worker_id: worker_id.clone(),
        src_export: job.src_export.clone(),
        dst_export: job.dst_export.clone(),
        src_exporter_id: "local".to_string(),
        dst_exporter_id: "local".to_string(),
        dst_root_relpath: job.dst_root_relpath.clone(),
        root_relpath: batch.root_relpath.clone(),
        staging_prefix: format!(
            ".fluxon.stage/{}/{}/{}",
            job.job_id, batch.batch_id, worker_task_id
        ),
        lease_expire_unix_ms,
        manifest_blob: batch.manifest_blob.clone(),
        collect_infos,
    };
    let first_heartbeat = FluxonFsTransferWorkerHeartbeatWire {
        job_id: job.job_id.clone(),
        worker_id: worker_id.clone(),
        assigned_batch_id: batch.batch_id.clone(),
        worker_task_id: worker_task_id.clone(),
        heartbeat_unix_ms: now_unix_ms,
        telemetry: None,
    };
    let first_heartbeat = state
        .apply_transfer_worker_heartbeat(
            &first_heartbeat,
            now_unix_ms,
            TRANSFER_HEARTBEAT_EXTENSION_MS,
        )
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    if !first_heartbeat.continue_running {
        anyhow::bail!("local transfer worker heartbeat stopped before execution");
    }
    let src_root_dir_abs = src_root
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("src_root must be valid utf-8"))?
        .to_string();
    let dst_root = dst_root.to_path_buf();
    let state = Arc::new(state.clone());
    let assignment_arc = Arc::new(assignment.clone());
    let result = execute_transfer_worker_assignment(
        &assignment,
        &dst_root,
        {
            let state = state.clone();
            let assignment = assignment_arc.clone();
            move || {
            let heartbeat_unix_ms = chrono::Utc::now().timestamp_millis();
            let heartbeat = FluxonFsTransferWorkerHeartbeatWire {
                job_id: assignment.job_id.clone(),
                worker_id: assignment.worker_id.clone(),
                assigned_batch_id: assignment.batch_id.clone(),
                worker_task_id: assignment.worker_task_id.clone(),
                heartbeat_unix_ms,
                telemetry: None,
            };
            state
                .apply_transfer_worker_heartbeat(
                    &heartbeat,
                    heartbeat_unix_ms,
                    TRANSFER_HEARTBEAT_EXTENSION_MS,
                )
                .map_err(|detail| {
                    TransferWorkerExecutionError::Fatal(local_transfer_worker_resp_err(format!(
                        "apply local transfer worker heartbeat failed: {}",
                        detail
                    )))
                })?
                .continue_running
                .then_some(())
                .ok_or(TransferWorkerExecutionError::Stop(
                    fluxon_fs_core::config::FluxonFsTransferWorkerStopReasonWire::Superseded,
                ))?;
            Ok(())
        }
        },
        {
            let src_root_dir_abs = src_root_dir_abs.clone();
            move |file, read_offset, length| {
            read_transfer_chunk_from_root_dir_abs(
                src_root_dir_abs.as_str(),
                file.relpath.as_str(),
                read_offset,
                length,
            )
            .map_err(TransferWorkerExecutionError::Fatal)
        }
        },
    )
    .map_err(|resp| anyhow::anyhow!("execute local transfer worker failed: {:?}", resp))?;
    cleanup_transfer_worker_attempt_artifacts(&dst_root, &assignment)
        .map_err(|resp| anyhow::anyhow!("cleanup local transfer worker artifacts failed: {:?}", resp))?;
    state
        .apply_transfer_worker_result(&result)
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    Ok(())
}

pub fn inspect_local_transfer_job_blocking(
    transfer_state_store: FluxonFsTransferStateStoreConfig,
    job_id: &str,
) -> anyhow::Result<LocalTransferJobInspectSummary> {
    if job_id.trim().is_empty() {
        anyhow::bail!("job_id must be non-empty");
    }
    let state = new_local_gateway_state(transfer_state_store)?;
    let snapshot = state
        .transfer_job_snapshot(job_id)
        .map_err(|e| anyhow::anyhow!("{}", e))?
        .ok_or_else(|| anyhow::anyhow!("transfer job snapshot missing: job_id={}", job_id))?;
    let mut batches = state
        .list_transfer_batches()
        .map_err(|e| anyhow::anyhow!("{}", e))?
        .into_iter()
        .filter(|batch| batch.job_id == job_id)
        .map(|batch| {
            let manifest = FluxonFsTransferManifestWire::decode_from_blob(batch.manifest_blob.as_slice())
                .map_err(|e| anyhow::anyhow!("decode transfer manifest failed: {}", e))?;
            let file_count = manifest.entries.len() as i64;
            let total_bytes = manifest
                .entries
                .iter()
                .fold(0_i64, |acc, entry| acc.saturating_add(entry.size));
            let empty_dir_count = manifest.empty_dir_relpaths.len() as i64;
            Ok(LocalTransferBatchInspectSummary {
                batch_id: batch.batch_id,
                root_relpath: batch.root_relpath,
                batch_kind: batch.batch_kind.as_db_str().to_string(),
                state: batch.state.as_db_str().to_string(),
                generation: batch.generation,
                file_count,
                total_bytes,
                empty_dir_count,
                entries: manifest
                    .entries
                    .into_iter()
                    .map(|entry| LocalTransferManifestEntrySummary {
                        relpath: entry.relpath,
                        size: entry.size,
                    })
                    .collect(),
                empty_dir_relpaths: manifest.empty_dir_relpaths,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    batches.sort_by(|a, b| {
        a.root_relpath
            .cmp(&b.root_relpath)
            .then(a.generation.cmp(&b.generation))
            .then(a.batch_id.cmp(&b.batch_id))
    });
    let mut collect_infos = state
        .list_transfer_batch_collect_infos()
        .map_err(|e| anyhow::anyhow!("{}", e))?
        .into_iter()
        .filter(|row| row.job_id == job_id)
        .map(|row| LocalTransferCollectInfoInspectSummary {
            batch_id: row.batch_id,
            collect_kind: row.collect_kind.as_db_str().to_string(),
            collect_blob_bytes: row.collect_blob.len() as i64,
            materialized: row.materialized,
        })
        .collect::<Vec<_>>();
    collect_infos.sort_by(|a, b| {
        a.batch_id
            .cmp(&b.batch_id)
            .then(a.collect_kind.cmp(&b.collect_kind))
    });
    let mut worker_attempts = state
        .list_transfer_worker_attempt_records()
        .map_err(|e| anyhow::anyhow!("{}", e))?
        .into_iter()
        .filter(|row| row.job_id == job_id)
        .map(|row| LocalTransferWorkerAttemptInspectSummary {
            batch_id: row.batch_id,
            worker_id: row.worker_id,
            worker_task_id: row.worker_task_id,
            dst_exporter_id: row.dst_exporter_id,
            state: row.state.as_db_str().to_string(),
            launch_attempt_count: row.launch_attempt_count,
            visible_file_count: row.visible_file_count,
            visible_bytes: row.visible_bytes,
            last_error: row.last_error,
            stop_reason: match row.stop_reason {
                Some(FluxonFsTransferWorkerStopReasonWire::Superseded) => "superseded".to_string(),
                Some(FluxonFsTransferWorkerStopReasonWire::Cancelled) => "cancelled".to_string(),
                None => String::new(),
            },
        })
        .collect::<Vec<_>>();
    worker_attempts.sort_by(|a, b| {
        a.batch_id
            .cmp(&b.batch_id)
            .then(a.worker_id.cmp(&b.worker_id))
            .then(a.worker_task_id.cmp(&b.worker_task_id))
    });
    Ok(LocalTransferJobInspectSummary {
        job_id: snapshot.job.job_id,
        scan_epoch: snapshot.scan_epoch,
        scan_finished: snapshot.scan_finished,
        job_state: snapshot.job.state.as_db_str().to_string(),
        open_batches: snapshot.open_batches,
        running_batch_count: snapshot.running_batches.len(),
        batches,
        worker_attempts,
        collect_infos,
    })
}

pub fn inspect_local_transfer_job_status_blocking(
    transfer_state_store: FluxonFsTransferStateStoreConfig,
    job_id: &str,
) -> anyhow::Result<LocalTransferJobStatusSummary> {
    if job_id.trim().is_empty() {
        anyhow::bail!("job_id must be non-empty");
    }
    let state = new_local_gateway_state(transfer_state_store)?;
    let snapshot = state
        .transfer_job_snapshot(job_id)
        .map_err(|e| anyhow::anyhow!("{}", e))?
        .ok_or_else(|| anyhow::anyhow!("transfer job snapshot missing: job_id={}", job_id))?;
    let mut batches = state
        .list_transfer_batches()
        .map_err(|e| anyhow::anyhow!("{}", e))?
        .into_iter()
        .filter(|batch| batch.job_id == job_id)
        .map(|batch| LocalTransferBatchStatusSummary {
            batch_id: batch.batch_id,
            root_relpath: batch.root_relpath,
            batch_kind: batch.batch_kind.as_db_str().to_string(),
            state: batch.state.as_db_str().to_string(),
            generation: batch.generation,
        })
        .collect::<Vec<_>>();
    batches.sort_by(|a, b| {
        a.root_relpath
            .cmp(&b.root_relpath)
            .then(a.generation.cmp(&b.generation))
            .then(a.batch_id.cmp(&b.batch_id))
    });
    let mut collect_infos = state
        .list_transfer_batch_collect_infos()
        .map_err(|e| anyhow::anyhow!("{}", e))?
        .into_iter()
        .filter(|row| row.job_id == job_id)
        .map(|row| LocalTransferCollectInfoInspectSummary {
            batch_id: row.batch_id,
            collect_kind: row.collect_kind.as_db_str().to_string(),
            collect_blob_bytes: row.collect_blob.len() as i64,
            materialized: row.materialized,
        })
        .collect::<Vec<_>>();
    collect_infos.sort_by(|a, b| {
        a.batch_id
            .cmp(&b.batch_id)
            .then(a.collect_kind.cmp(&b.collect_kind))
    });
    let mut worker_attempts = state
        .list_transfer_worker_attempt_records()
        .map_err(|e| anyhow::anyhow!("{}", e))?
        .into_iter()
        .filter(|row| row.job_id == job_id)
        .map(|row| LocalTransferWorkerAttemptInspectSummary {
            batch_id: row.batch_id,
            worker_id: row.worker_id,
            worker_task_id: row.worker_task_id,
            dst_exporter_id: row.dst_exporter_id,
            state: row.state.as_db_str().to_string(),
            launch_attempt_count: row.launch_attempt_count,
            visible_file_count: row.visible_file_count,
            visible_bytes: row.visible_bytes,
            last_error: row.last_error,
            stop_reason: match row.stop_reason {
                Some(FluxonFsTransferWorkerStopReasonWire::Superseded) => "superseded".to_string(),
                Some(FluxonFsTransferWorkerStopReasonWire::Cancelled) => "cancelled".to_string(),
                None => String::new(),
            },
        })
        .collect::<Vec<_>>();
    worker_attempts.sort_by(|a, b| {
        a.batch_id
            .cmp(&b.batch_id)
            .then(a.worker_id.cmp(&b.worker_id))
            .then(a.worker_task_id.cmp(&b.worker_task_id))
    });
    Ok(LocalTransferJobStatusSummary {
        job_id: snapshot.job.job_id,
        scan_epoch: snapshot.scan_epoch,
        scan_finished: snapshot.scan_finished,
        job_state: snapshot.job.state.as_db_str().to_string(),
        open_batches: snapshot.open_batches,
        running_batch_count: snapshot.running_batches.len(),
        batches,
        worker_attempts,
        collect_infos,
    })
}

pub fn run_local_transfer_blocking(
    arg: LocalTransferRunArg,
) -> anyhow::Result<LocalTransferRunSummary> {
    if arg.src_root_dir.trim().is_empty() {
        anyhow::bail!("src_root_dir must be non-empty");
    }
    if arg.dst_root_dir.trim().is_empty() {
        anyhow::bail!("dst_root_dir must be non-empty");
    }
    let src_root = PathBuf::from(arg.src_root_dir.as_str());
    if !src_root.exists() {
        anyhow::bail!("src_root_dir not found: {}", src_root.display());
    }
    if !src_root.is_dir() {
        anyhow::bail!("src_root_dir must be a directory: {}", src_root.display());
    }
    let src_root = src_root
        .canonicalize()
        .with_context(|| format!("canonicalize src_root_dir failed: {}", src_root.display()))?;
    let src_root_str = src_root
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("src_root_dir must be valid utf-8"))?
        .to_string();
    let dst_root = PathBuf::from(arg.dst_root_dir.as_str());
    fs::create_dir_all(&dst_root)
        .with_context(|| format!("create dst_root_dir failed: {}", dst_root.display()))?;
    if !dst_root.is_dir() {
        anyhow::bail!("dst_root_dir must be a directory: {}", dst_root.display());
    }
    let dst_root = dst_root
        .canonicalize()
        .with_context(|| format!("canonicalize dst_root_dir failed: {}", dst_root.display()))?;

    let scan_summary = run_local_transfer_check_blocking(LocalTransferCheckArg {
        src_root_dir: src_root_str,
        transfer_state_store: arg.transfer_state_store.clone(),
        batch_ready_bytes: arg.batch_ready_bytes,
        skip_entries: arg.skip_entries,
        checker_concurrency_limit: arg.checker_concurrency_limit,
        enable_cli_progress: arg.enable_cli_progress,
    })?;

    let state = new_local_gateway_state_with_backend(
        arg.transfer_state_store.clone(),
        Arc::new(LocalFsStatBackend {
            dst_root: dst_root.clone(),
        }),
    )?;
    state
        .update_transfer_job_desired_worker_count(
            scan_summary.job_id.as_str(),
            LOCAL_TRANSFER_WORKER_COUNT,
        )
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    for _ in 0..64 {
        let snapshot = state
            .transfer_job_snapshot(scan_summary.job_id.as_str())
            .map_err(|e| anyhow::anyhow!("{}", e))?
            .ok_or_else(|| anyhow::anyhow!("transfer job snapshot missing: job_id={}", scan_summary.job_id))?;
        if snapshot.job.state == fluxon_fs_core::config::FluxonFsTransferJobState::Completed {
            let inspect =
                inspect_local_transfer_job_blocking(arg.transfer_state_store, scan_summary.job_id.as_str())?;
            let finished_batch_count = inspect
                .batches
                .iter()
                .filter(|batch| batch.state == FluxonFsTransferBatchState::Finished.as_db_str())
                .count();
            let materialized_collect_info_count = inspect
                .collect_infos
                .iter()
                .filter(|row| row.materialized)
                .count();
            return Ok(LocalTransferRunSummary {
                job_id: inspect.job_id,
                scan_epoch: inspect.scan_epoch,
                batch_count: inspect.batches.len(),
                full_dir_batch_count: inspect
                    .batches
                    .iter()
                    .filter(|batch| batch.batch_kind == FluxonFsTransferBatchKind::FullDir.as_db_str())
                    .count(),
                direct_files_only_batch_count: inspect
                    .batches
                    .iter()
                    .filter(|batch| batch.batch_kind == FluxonFsTransferBatchKind::DirectFilesOnly.as_db_str())
                    .count(),
                finished_batch_count,
                materialized_collect_info_count,
                completed: inspect.job_state
                    == fluxon_fs_core::config::FluxonFsTransferJobState::Completed.as_db_str(),
                open_batches: inspect.open_batches,
            });
        }
        let ready_batches = state
            .list_transfer_batches()
            .map_err(|e| anyhow::anyhow!("{}", e))?
            .into_iter()
            .filter(|batch| {
                batch.job_id == scan_summary.job_id
                    && batch.state == FluxonFsTransferBatchState::Ready
            })
            .collect::<Vec<_>>();
        if ready_batches.is_empty() {
            state
                .reconcile_transfer_scheduler_state(chrono::Utc::now().timestamp_millis())
                .map_err(|e| anyhow::anyhow!("{}", e))?;
            continue;
        }
        execute_local_transfer_worker_for_batch(
            &state,
            scan_summary.job_id.as_str(),
            ready_batches[0].batch_id.as_str(),
            src_root.as_path(),
            dst_root.as_path(),
        )?;
        state
            .reconcile_transfer_scheduler_state(chrono::Utc::now().timestamp_millis())
            .map_err(|e| anyhow::anyhow!("{}", e))?;
    }
    anyhow::bail!(
        "local transfer run did not converge: job_id={}",
        scan_summary.job_id
    );
}

fn normalize_skip_entries(
    entries: Vec<LocalTransferSkipEntry>,
) -> anyhow::Result<Vec<FluxonFsTransferSkipEntryWire>> {
    let mut raw_entries = Vec::with_capacity(entries.len());
    for entry in entries {
        raw_entries.push(FluxonFsTransferSkipEntryWire {
            kind: match entry.kind {
                LocalTransferSkipEntryKind::Dir => FluxonFsTransferSkipEntryKind::Dir,
                LocalTransferSkipEntryKind::File => FluxonFsTransferSkipEntryKind::File,
            },
            relpath: entry.relpath,
        });
    }
    normalize_transfer_skip_entries(raw_entries).map_err(|e| anyhow::anyhow!("{}", e))
}

fn encode_job_spec(spec: &FluxonFsLocalTransferCheckJobSpecWire) -> anyhow::Result<Vec<u8>> {
    serde_json::to_vec(spec).map_err(|e| anyhow::anyhow!("encode local job spec failed: {}", e))
}

fn decode_job_spec(blob: &[u8]) -> anyhow::Result<FluxonFsLocalTransferCheckJobSpecWire> {
    serde_json::from_slice(blob)
        .map_err(|e| anyhow::anyhow!("decode local job spec failed: {}", e))
}

fn find_reusable_local_job(
    state: &GatewayState,
    spec: &FluxonFsLocalTransferCheckJobSpecWire,
    spec_blob: &[u8],
) -> anyhow::Result<Option<FsTransferJobRecord>> {
    let jobs = state
        .list_running_transfer_jobs()
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    let mut matched: Option<FsTransferJobRecord> = None;
    for job in jobs {
        if job.src_export != LOCAL_CHECK_SRC_EXPORT || job.dst_export != LOCAL_CHECK_DST_EXPORT {
            continue;
        }
        let job_spec = match decode_job_spec(job.job_spec_blob.as_slice()) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if job_spec.src_root_dir_abs != spec.src_root_dir_abs {
            continue;
        }
        if job.job_spec_blob.as_slice() == spec_blob {
            if matched.is_some() {
                anyhow::bail!(
                    "multiple reusable local transfer jobs found for src_root_dir_abs={}",
                    spec.src_root_dir_abs
                );
            }
            matched = Some(job);
            continue;
        }
        anyhow::bail!(
            "existing local transfer job has different durable contract: src_root_dir_abs={}",
            spec.src_root_dir_abs
        );
    }
    Ok(matched)
}

#[cfg(test)]
fn maybe_sleep_after_apply_transfer_scan_result_for_test() {
    let Ok(raw) = std::env::var(LOCAL_TRANSFER_TEST_SLEEP_AFTER_APPLY_MS_ENV) else {
        return;
    };
    let sleep_ms: u64 = raw.parse().expect(
        "FLUXON_FS_LOCAL_TRANSFER_TEST_SLEEP_AFTER_APPLY_MS must be a valid u64 millisecond value",
    );
    if sleep_ms > 0 {
        thread::sleep(Duration::from_millis(sleep_ms));
    }
}

#[cfg(not(test))]
fn maybe_sleep_after_apply_transfer_scan_result_for_test() {}

fn run_local_scan_epoch(
    state: &GatewayState,
    src_root_dir: &str,
    checker_concurrency: usize,
    job_id: &str,
    batch_ready_bytes: i64,
    skip_entries: &[FluxonFsTransferSkipEntryWire],
    progress: Option<Arc<LocalTransferProgressState>>,
) -> anyhow::Result<i64> {
    let scan_epoch = state
        .begin_transfer_scan_epoch(job_id)
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    let queue = Arc::new(Mutex::new(vec![LocalScanUnit {
        scan_unit_id: format!("{}__scan_epoch_{}__{}", job_id, scan_epoch, Uuid::new_v4()),
        root_relpath: ".".to_string(),
        generation: 1,
        scan_mode: FluxonFsTransferScanMode::RootDirectFanoutOnly,
    }]));
    let pending_units = Arc::new(AtomicUsize::new(1));
    if let Some(progress) = progress.as_ref() {
        progress.pending_scan_units.store(1, Ordering::SeqCst);
    }
    let stop = Arc::new(AtomicBool::new(false));
    let first_error = Arc::new(Mutex::new(None::<String>));
    let worker_count = std::cmp::max(1, checker_concurrency);
    let mut handles = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let queue2 = queue.clone();
        let pending_units2 = pending_units.clone();
        let stop2 = stop.clone();
        let first_error2 = first_error.clone();
        let state2 = state.clone();
        let job_id2 = job_id.to_string();
        let src_root_dir2 = src_root_dir.to_string();
        let skip_entries2 = skip_entries.to_vec();
        let progress2 = progress.clone();
        let handle = thread::spawn(move || loop {
            if stop2.load(Ordering::SeqCst) {
                return;
            }
            let scan_unit = {
                let mut queue = queue2.lock();
                queue.pop()
            };
            let Some(scan_unit) = scan_unit else {
                if pending_units2.load(Ordering::SeqCst) == 0 {
                    return;
                }
                thread::sleep(Duration::from_millis(1));
                continue;
            };
            if stop2.load(Ordering::SeqCst) {
                pending_units2.fetch_sub(1, Ordering::SeqCst);
                if let Some(progress) = progress2.as_ref() {
                    progress
                        .pending_scan_units
                        .store(pending_units2.load(Ordering::SeqCst), Ordering::SeqCst);
                }
                return;
            }
            if let Some(progress) = progress2.as_ref() {
                progress.active_checkers.fetch_add(1, Ordering::SeqCst);
                progress
                    .pending_scan_units
                    .store(pending_units2.load(Ordering::SeqCst), Ordering::SeqCst);
            }
            let known_batches = match state2.list_transfer_batches() {
                Ok(v) => v,
                Err(e) => {
                    *first_error2.lock() = Some(e);
                    stop2.store(true, Ordering::SeqCst);
                    if let Some(progress) = progress2.as_ref() {
                        progress.active_checkers.fetch_sub(1, Ordering::SeqCst);
                    }
                    return;
                }
            };
            let known_direct_files_complete_records =
                match state2.list_transfer_direct_files_complete_records() {
                    Ok(v) => v,
                    Err(e) => {
                        *first_error2.lock() = Some(e);
                        stop2.store(true, Ordering::SeqCst);
                        if let Some(progress) = progress2.as_ref() {
                            progress.active_checkers.fetch_sub(1, Ordering::SeqCst);
                        }
                        return;
                    }
                };
            let assignment = FluxonFsTransferScanAssignmentWire {
                job_id: job_id2.clone(),
                scan_epoch,
                scan_unit_id: scan_unit.scan_unit_id.clone(),
                scan_task_id: format!("{}__local_scan_task__{}", job_id2, Uuid::new_v4()),
                root_relpath: scan_unit.root_relpath.clone(),
                generation: scan_unit.generation,
                scan_mode: scan_unit.scan_mode,
                src_export: LOCAL_CHECK_SRC_EXPORT.to_string(),
                src_exporter_id: "local".to_string(),
                batch_ready_bytes,
                lease_expire_unix_ms: 0,
                known_dispositions: build_known_dispositions_for_local_scan(
                    job_id2.as_str(),
                    scan_unit.root_relpath.as_str(),
                    scan_unit.generation,
                    &known_batches,
                    &known_direct_files_complete_records,
                ),
                live_child_scan_roots: Vec::new(),
                skip_entries: skip_entries2.clone(),
            };
            let result = match build_transfer_scan_result_for_root_dir_abs(
                src_root_dir2.as_str(),
                &assignment,
            ) {
                Ok(v) => v,
                Err(resp) => {
                    *first_error2.lock() =
                        Some(format!("local transfer scan failed: {:?}", resp));
                    stop2.store(true, Ordering::SeqCst);
                    if let Some(progress) = progress2.as_ref() {
                        progress.active_checkers.fetch_sub(1, Ordering::SeqCst);
                    }
                    return;
                }
            };
            if let Err(e) = state2.apply_transfer_scan_result(&result) {
                *first_error2.lock() = Some(e);
                stop2.store(true, Ordering::SeqCst);
                if let Some(progress) = progress2.as_ref() {
                    progress.active_checkers.fetch_sub(1, Ordering::SeqCst);
                }
                return;
            }
            if let Some(progress) = progress2.as_ref() {
                progress.completed_scan_units.fetch_add(1, Ordering::SeqCst);
                progress.discovered_symlink_notices.fetch_add(
                    count_symlink_notices_in_scan_result(&result),
                    Ordering::SeqCst,
                );
            }
            // The test hook widens the process-kill window after durable batch commit.
            maybe_sleep_after_apply_transfer_scan_result_for_test();
            if !result.child_scan_units.is_empty() {
                let mut queue = queue2.lock();
                pending_units2.fetch_add(result.child_scan_units.len(), Ordering::SeqCst);
                for child in result.child_scan_units {
                    queue.push(LocalScanUnit {
                        scan_unit_id: child.scan_unit_id,
                        root_relpath: child.root_relpath,
                        generation: child.generation,
                        scan_mode: child.scan_mode,
                    });
                }
            }
            pending_units2.fetch_sub(1, Ordering::SeqCst);
            if let Some(progress) = progress2.as_ref() {
                progress.active_checkers.fetch_sub(1, Ordering::SeqCst);
                progress
                    .pending_scan_units
                    .store(pending_units2.load(Ordering::SeqCst), Ordering::SeqCst);
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle
            .join()
            .map_err(|_| anyhow::anyhow!("local transfer checker worker thread panicked"))?;
    }
    if let Some(err) = first_error.lock().clone() {
        anyhow::bail!(err);
    }
    state
        .finish_transfer_scan_epoch(job_id, scan_epoch)
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    Ok(scan_epoch)
}

pub fn run_local_transfer_check_blocking(
    arg: LocalTransferCheckArg,
) -> anyhow::Result<LocalTransferCheckSummary> {
    if arg.src_root_dir.trim().is_empty() {
        anyhow::bail!("src_root_dir must be non-empty");
    }
    if arg.batch_ready_bytes <= 0 {
        anyhow::bail!("batch_ready_bytes must be > 0");
    }
    let src_root_dir = PathBuf::from(arg.src_root_dir.as_str());
    if !src_root_dir.exists() {
        anyhow::bail!("src_root_dir not found: {}", src_root_dir.display());
    }
    if !src_root_dir.is_dir() {
        anyhow::bail!("src_root_dir must be a directory: {}", src_root_dir.display());
    }
    let src_root_dir = src_root_dir
        .canonicalize()
        .with_context(|| format!("canonicalize src_root_dir failed: {}", src_root_dir.display()))?;
    let src_root_dir_str = src_root_dir
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("src_root_dir must be valid utf-8"))?
        .to_string();
    let normalized_skip_entries = normalize_skip_entries(arg.skip_entries)?;
    let checker_concurrency = resolve_checker_concurrency(arg.checker_concurrency_limit)?;
    let spec = FluxonFsLocalTransferCheckJobSpecWire {
        src_root_dir_abs: src_root_dir_str.clone(),
        batch_ready_bytes: arg.batch_ready_bytes,
        skip_entries: normalized_skip_entries.clone(),
    };
    let spec_blob = encode_job_spec(&spec)?;
    let state = new_local_gateway_state(arg.transfer_state_store.clone())?;
    let (job, reused_existing_job) = match find_reusable_local_job(&state, &spec, spec_blob.as_slice())? {
        Some(job) => (job, true),
        None => state
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: LOCAL_CHECK_SRC_EXPORT.to_string(),
                src_root_relpath: ".".to_string(),
                dst_export: LOCAL_CHECK_DST_EXPORT.to_string(),
                dst_root_relpath: ".".to_string(),
                desired_scan_concurrency: DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 0,
                batch_ready_bytes: arg.batch_ready_bytes,
                job_spec_blob: spec_blob.clone(),
            })
            .map(|job| (job, false))
            .map_err(|e| anyhow::anyhow!("{}", e))?,
    };
    let started_at = Instant::now();
    let progress = if arg.enable_cli_progress {
        Some(Arc::new(LocalTransferProgressState::default()))
    } else {
        None
    };
    let progress_reporter = if let Some(progress) = progress.as_ref() {
        log_local_transfer_progress_snapshot(
            &state,
            job.job_id.as_str(),
            progress.as_ref(),
            checker_concurrency,
            started_at,
            reused_existing_job,
            "start",
        );
        Some(spawn_local_transfer_progress_reporter(
            state.clone(),
            job.job_id.clone(),
            progress.clone(),
            checker_concurrency,
            started_at,
            reused_existing_job,
        ))
    } else {
        None
    };
    let scan_epoch = match run_local_scan_epoch(
        &state,
        src_root_dir_str.as_str(),
        checker_concurrency,
        job.job_id.as_str(),
        arg.batch_ready_bytes,
        &normalized_skip_entries,
        progress.clone(),
    ) {
        Ok(v) => v,
        Err(err) => {
            if let Some(progress) = progress.as_ref() {
                progress.stop.store(true, Ordering::SeqCst);
            }
            if let Some(handle) = progress_reporter {
                handle.join().map_err(|_| {
                    anyhow::anyhow!("local transfer checker progress reporter panicked")
                })?;
            }
            return Err(err);
        }
    };
    let batches = state
        .list_transfer_batches()
        .map_err(|e| anyhow::anyhow!("{}", e))?
        .into_iter()
        .filter(|batch| {
            batch.job_id == job.job_id && batch.state == FluxonFsTransferBatchState::Ready
        })
        .collect::<Vec<_>>();
    let full_dir_batch_count = batches
        .iter()
        .filter(|batch| batch.batch_kind == FluxonFsTransferBatchKind::FullDir)
        .count();
    let direct_files_only_batch_count = batches
        .iter()
        .filter(|batch| batch.batch_kind == FluxonFsTransferBatchKind::DirectFilesOnly)
        .count();
    if let Some(progress) = progress.as_ref() {
        progress.stop.store(true, Ordering::SeqCst);
    }
    if let Some(handle) = progress_reporter {
        handle
            .join()
            .map_err(|_| anyhow::anyhow!("local transfer checker progress reporter panicked"))?;
    }
    if let Some(progress) = progress.as_ref() {
        log_local_transfer_progress_snapshot(
            &state,
            job.job_id.as_str(),
            progress.as_ref(),
            checker_concurrency,
            started_at,
            reused_existing_job,
            "finish",
        );
    }
    Ok(LocalTransferCheckSummary {
        job_id: job.job_id,
        scan_epoch,
        batch_count: batches.len(),
        full_dir_batch_count,
        direct_files_only_batch_count,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::fs::{self, File};
    use std::net::TcpListener;
    use std::path::{Path, PathBuf};
    use std::process::{self, Child, Command, Stdio};
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use fluxon_fs_core::config::{
        FluxonFsTransferBatchKind, FluxonFsTransferBatchState, FluxonFsTransferDispositionWire,
        FluxonFsTransferJobState, FluxonFsTransferManifestWire, FluxonFsTransferStateStoreConfig,
    };
    use fluxon_fs_s3_gateway::{FsTransferBatchRecord, GatewayState};
    use tempfile::{Builder, TempDir};
    use uuid::Uuid;

    use super::{
        LOCAL_TRANSFER_TEST_SLEEP_AFTER_APPLY_MS_ENV, LocalTransferCheckArg,
        LocalTransferSkipEntry, LocalTransferSkipEntryKind,
        build_known_dispositions_for_local_scan,
        build_local_tikv_transfer_state_store_config, decode_job_spec,
        execute_local_transfer_worker_for_batch, new_local_gateway_state,
        new_local_gateway_state_with_backend, run_local_transfer_check_blocking, LocalFsStatBackend,
    };

    const LOCAL_TRANSFER_CHECKER_KILL_RESUME_TEST_NAME: &str =
        "local_transfer_checker::tests::local_transfer_checker_recovers_after_process_kill_and_matches_expected_batches";
    const LOCAL_TRANSFER_TEST_CHILD_TRANSFER_STATE_STORE_JSON_ENV: &str =
        "FLUXON_FS_TEST_CHILD_TRANSFER_STATE_STORE_JSON";
    const LOCAL_TRANSFER_TEST_IO_ROOT: &str =
        "/tmp/fluxon-example/fluxon_fs_transfer_tikv/rust_local_checker_io";
    const LOCAL_TRANSFER_TEST_TIKV_WORK_ROOT: &str =
        "/tmp/fluxon-example/fluxon_fs_transfer_tikv/rust_local_checker";
    const LOCAL_TRANSFER_TEST_TIKV_READY_TIMEOUT_SECS: u64 = 180;
    const LOCAL_TRANSFER_TEST_TIKV_PD_LEASE_SECS: u64 = 60;
    const LOCAL_TRANSFER_TEST_TIKV_UNIFIED_READPOOL_MAX_THREADS: u64 = 4;
    const LOCAL_TRANSFER_TEST_TIKV_STORAGE_READPOOL_CONCURRENCY: u64 = 2;
    const LOCAL_TRANSFER_TEST_TIKV_COPROCESSOR_READPOOL_CONCURRENCY: u64 = 2;
    const LOCAL_TRANSFER_TEST_TIKV_ENDPOINT_MAX_CONCURRENCY: u64 = 8;
    const LOCAL_TRANSFER_TEST_TIKV_BACKGROUND_THREAD_COUNT: u64 = 2;
    const LOCAL_TRANSFER_TEST_TIKV_SCHEDULER_CONCURRENCY: u64 = 2048;
    const LOCAL_TRANSFER_TEST_TIKV_SCHEDULER_WORKER_POOL_SIZE: u64 = 2;
    const LOCAL_TRANSFER_TEST_TIKV_APPLY_POOL_SIZE: u64 = 1;
    const LOCAL_TRANSFER_TEST_TIKV_STORE_POOL_SIZE: u64 = 1;
    const LOCAL_TRANSFER_TEST_TIKV_ROCKSDB_MAX_BACKGROUND_JOBS: u64 = 2;
    const LOCAL_TRANSFER_TEST_TIKV_RAFTDB_MAX_BACKGROUND_JOBS: u64 = 2;

    fn process_is_alive(pid: u32) -> bool {
        Path::new("/proc").join(pid.to_string()).exists()
    }

    fn extract_test_dir_pid(dir_name: &str, prefix: &str) -> Option<u32> {
        let suffix = dir_name.strip_prefix(prefix)?.strip_prefix("_pid")?;
        let (pid_text, _) = suffix.split_once('_')?;
        pid_text.parse::<u32>().ok()
    }

    fn cleanup_stale_test_temp_dirs(root: &Path, prefix: &str) {
        fs::create_dir_all(root).unwrap();
        let rd = fs::read_dir(root).unwrap();
        for ent in rd {
            let ent = ent.unwrap();
            if !ent.file_type().unwrap().is_dir() {
                continue;
            }
            let dir_name = ent.file_name().to_string_lossy().to_string();
            let Some(pid) = extract_test_dir_pid(dir_name.as_str(), prefix) else {
                continue;
            };
            if process_is_alive(pid) {
                continue;
            }
            match fs::remove_dir_all(ent.path()) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => panic!(
                    "failed to remove stale test temp dir path={} err={}",
                    ent.path().display(),
                    err
                ),
            }
        }
    }

    fn new_test_temp_dir(root: &str, prefix: &str) -> TempDir {
        let root_path = Path::new(root);
        cleanup_stale_test_temp_dirs(root_path, prefix);
        let prefix_with_pid = format!("{}_pid{}_", prefix, process::id());
        Builder::new()
            .prefix(prefix_with_pid.as_str())
            .tempdir_in(root_path)
            .unwrap()
    }

    fn new_fixture_temp_dir() -> TempDir {
        new_test_temp_dir(LOCAL_TRANSFER_TEST_IO_ROOT, "fluxon_fs_local_transfer")
    }

    fn new_tikv_runtime_dir() -> TempDir {
        new_test_temp_dir(
            LOCAL_TRANSFER_TEST_TIKV_WORK_ROOT,
            "fluxon_fs_local_transfer_tikv",
        )
    }

    fn write_file(root: &TempDir, relpath: &str, data: &[u8]) {
        let path = root.path().join(relpath);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, data).unwrap();
    }

    fn write_repeat_file(root: &TempDir, relpath: &str, byte: u8, size: usize) {
        write_file(root, relpath, &vec![byte; size]);
    }

    #[cfg(unix)]
    fn write_symlink(root: &TempDir, relpath: &str, target: &str) {
        let path = root.path().join(relpath);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        std::os::unix::fs::symlink(target, path).unwrap();
    }

    fn build_kill_resume_fixture(root: &TempDir) {
        write_repeat_file(root, "alpha/root-a.bin", b'a', 5);
        write_repeat_file(root, "alpha/root-b.bin", b'b', 4);
        write_repeat_file(root, "alpha/deep/child-a.bin", b'c', 3);
        write_repeat_file(root, "alpha/deep/child-b.bin", b'd', 2);
        write_repeat_file(root, "beta/direct.bin", b'e', 6);
        write_repeat_file(root, "beta/sub/leaf.bin", b'f', 7);
        write_repeat_file(root, "gamma/branch1/file.bin", b'g', 8);
        write_repeat_file(root, "gamma/branch2/nested/leaf.bin", b'h', 9);
        write_repeat_file(root, "gamma/branch2/nested/other.bin", b'i', 1);
        write_repeat_file(root, "skip-dir/hidden.bin", b'j', 10);
        write_repeat_file(root, "keep/skip-file.bin", b'k', 11);
        write_repeat_file(root, "keep/keep-file.bin", b'l', 12);
        fs::create_dir_all(root.path().join("empty-dir")).unwrap();
    }

    fn local_check_arg_with_store(
        src_root_dir: &TempDir,
        transfer_state_store: FluxonFsTransferStateStoreConfig,
        batch_ready_bytes: i64,
        skip_entries: Vec<LocalTransferSkipEntry>,
    ) -> LocalTransferCheckArg {
        LocalTransferCheckArg {
            src_root_dir: src_root_dir.path().to_str().unwrap().to_string(),
            transfer_state_store,
            batch_ready_bytes,
            skip_entries,
            checker_concurrency_limit: Some(1),
            enable_cli_progress: false,
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct ExpectedBatch {
        root_relpath: String,
        batch_kind: FluxonFsTransferBatchKind,
        entries: Vec<(String, i64)>,
        empty_dir_relpaths: Vec<String>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ExpectedSubtreeClosureState {
        Mergeable,
        Closed,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct ExpectedSubtreePlan {
        closure: ExpectedSubtreeClosureState,
        total_bytes: i64,
        root_is_empty: bool,
        batches: Vec<ExpectedBatch>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct ActualBatch {
        root_relpath: String,
        batch_kind: FluxonFsTransferBatchKind,
        entries: Vec<(String, i64)>,
        empty_dir_relpaths: Vec<String>,
    }

    fn normalize_test_skip_entries(
        entries: &[LocalTransferSkipEntry],
    ) -> (BTreeSet<String>, BTreeSet<String>) {
        let mut skipped_dirs = BTreeSet::new();
        let mut skipped_files = BTreeSet::new();
        for entry in entries {
            match entry.kind {
                LocalTransferSkipEntryKind::Dir => {
                    skipped_dirs.insert(entry.relpath.clone());
                }
                LocalTransferSkipEntryKind::File => {
                    skipped_files.insert(entry.relpath.clone());
                }
            }
        }
        (skipped_dirs, skipped_files)
    }

    fn test_relpath_is_skipped(
        relpath: &str,
        skipped_dirs: &BTreeSet<String>,
        skipped_files: &BTreeSet<String>,
    ) -> bool {
        if skipped_files.contains(relpath) {
            return true;
        }
        skipped_dirs.iter().any(|dir| {
            relpath == dir
                || relpath
                    .strip_prefix(dir.as_str())
                    .is_some_and(|suffix| suffix.starts_with('/'))
        })
    }

    fn normalize_child_relpath_for_test(parent: &str, child_name: &str) -> String {
        if parent == "." {
            child_name.to_string()
        } else {
            format!("{}/{}", parent.trim_end_matches('/'), child_name)
        }
    }

    fn collect_entries_for_subtree_oracle(
        src_root_dir: &Path,
        root_relpath: &str,
        skipped_dirs: &BTreeSet<String>,
        skipped_files: &BTreeSet<String>,
    ) -> Vec<(String, i64)> {
        if test_relpath_is_skipped(root_relpath, skipped_dirs, skipped_files) {
            return Vec::new();
        }
        let mut out = Vec::new();
        let mut stack: Vec<(PathBuf, String)> =
            vec![(src_root_dir.join(root_relpath), root_relpath.to_string())];
        while let Some((dir_abs, rel)) = stack.pop() {
            let mut child_dirs = Vec::new();
            let rd = fs::read_dir(dir_abs).unwrap();
            for ent in rd {
                let ent = ent.unwrap();
                let name = ent.file_name().to_string_lossy().to_string();
                let child_rel = normalize_child_relpath_for_test(rel.as_str(), name.as_str());
                if test_relpath_is_skipped(child_rel.as_str(), skipped_dirs, skipped_files) {
                    continue;
                }
                let md = fs::symlink_metadata(ent.path()).unwrap();
                if md.file_type().is_symlink() {
                    continue;
                }
                if md.is_dir() {
                    child_dirs.push((ent.path(), child_rel));
                    continue;
                }
                if md.is_file() {
                    out.push((child_rel, md.len().min(i64::MAX as u64) as i64));
                }
            }
            child_dirs.sort_by(|a, b| a.1.cmp(&b.1));
            for child in child_dirs.into_iter().rev() {
                stack.push(child);
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    fn collect_empty_dirs_for_subtree_oracle(
        src_root_dir: &Path,
        root_relpath: &str,
        skipped_dirs: &BTreeSet<String>,
        skipped_files: &BTreeSet<String>,
    ) -> Vec<String> {
        if test_relpath_is_skipped(root_relpath, skipped_dirs, skipped_files) {
            return Vec::new();
        }
        let mut out = Vec::new();
        let mut stack: Vec<(PathBuf, String)> =
            vec![(src_root_dir.join(root_relpath), root_relpath.to_string())];
        while let Some((dir_abs, rel)) = stack.pop() {
            let mut has_child_coverage = false;
            let mut child_dirs = Vec::new();
            let rd = fs::read_dir(dir_abs).unwrap();
            for ent in rd {
                let ent = ent.unwrap();
                let name = ent.file_name().to_string_lossy().to_string();
                let child_rel = normalize_child_relpath_for_test(rel.as_str(), name.as_str());
                if test_relpath_is_skipped(child_rel.as_str(), skipped_dirs, skipped_files) {
                    continue;
                }
                let md = fs::symlink_metadata(ent.path()).unwrap();
                if md.file_type().is_symlink() {
                    continue;
                }
                if md.is_dir() {
                    has_child_coverage = true;
                    child_dirs.push((ent.path(), child_rel));
                    continue;
                }
                if md.is_file() {
                    has_child_coverage = true;
                }
            }
            if !has_child_coverage {
                out.push(rel);
                continue;
            }
            child_dirs.sort_by(|a, b| a.1.cmp(&b.1));
            for child in child_dirs.into_iter().rev() {
                stack.push(child);
            }
        }
        out.sort();
        out
    }

    fn collect_direct_listing_for_test(
        src_root_dir: &Path,
        root_relpath: &str,
        skipped_dirs: &BTreeSet<String>,
        skipped_files: &BTreeSet<String>,
    ) -> (bool, Vec<(String, i64)>, Vec<String>) {
        let dir_abs = src_root_dir.join(root_relpath);
        let rd = fs::read_dir(dir_abs).unwrap();
        let mut has_visible_entries = false;
        let mut direct_files = Vec::new();
        let mut child_dirs = Vec::new();
        for ent in rd {
            let ent = ent.unwrap();
            let name = ent.file_name().to_string_lossy().to_string();
            let child_rel = normalize_child_relpath_for_test(root_relpath, name.as_str());
            if test_relpath_is_skipped(child_rel.as_str(), skipped_dirs, skipped_files) {
                continue;
            }
            let md = fs::symlink_metadata(ent.path()).unwrap();
            if md.file_type().is_symlink() {
                continue;
            }
            if md.is_file() {
                has_visible_entries = true;
                let size = md.len().min(i64::MAX as u64) as i64;
                direct_files.push((child_rel, size));
                continue;
            }
            if md.is_dir() {
                has_visible_entries = true;
                child_dirs.push(child_rel);
            }
        }
        direct_files.sort_by(|a, b| a.0.cmp(&b.0));
        child_dirs.sort();
        (has_visible_entries, direct_files, child_dirs)
    }

    fn expected_full_dir_batch_for_test(
        src_root_dir: &Path,
        root_relpath: &str,
        skipped_dirs: &BTreeSet<String>,
        skipped_files: &BTreeSet<String>,
    ) -> ExpectedBatch {
        ExpectedBatch {
            root_relpath: root_relpath.to_string(),
            batch_kind: FluxonFsTransferBatchKind::FullDir,
            entries: collect_entries_for_subtree_oracle(
                src_root_dir,
                root_relpath,
                skipped_dirs,
                skipped_files,
            ),
            empty_dir_relpaths: collect_empty_dirs_for_subtree_oracle(
                src_root_dir,
                root_relpath,
                skipped_dirs,
                skipped_files,
            ),
        }
    }

    fn plan_expected_subtree_batches_for_test(
        src_root_dir: &Path,
        root_relpath: &str,
        batch_ready_bytes: i64,
        skipped_dirs: &BTreeSet<String>,
        skipped_files: &BTreeSet<String>,
    ) -> ExpectedSubtreePlan {
        let (has_visible_entries, direct_files, child_dirs) = collect_direct_listing_for_test(
            src_root_dir,
            root_relpath,
            skipped_dirs,
            skipped_files,
        );
        if !has_visible_entries {
            return ExpectedSubtreePlan {
                closure: ExpectedSubtreeClosureState::Mergeable,
                total_bytes: 0,
                root_is_empty: true,
                batches: Vec::new(),
            };
        }

        let mut total_bytes = direct_files
            .iter()
            .fold(0_i64, |acc, (_, size)| acc.saturating_add(*size));
        let mut child_partitioned = false;
        let mut mergeable_child_relpaths = Vec::new();
        let mut mergeable_empty_child_relpaths = Vec::new();
        let mut batches = Vec::new();
        for child_rel in child_dirs {
            let child_plan = plan_expected_subtree_batches_for_test(
                src_root_dir,
                child_rel.as_str(),
                batch_ready_bytes,
                skipped_dirs,
                skipped_files,
            );
            match child_plan.closure {
                ExpectedSubtreeClosureState::Mergeable => {
                    total_bytes = total_bytes.saturating_add(child_plan.total_bytes);
                    if child_plan.root_is_empty {
                        mergeable_empty_child_relpaths.push(child_rel);
                    } else {
                        mergeable_child_relpaths.push(child_rel);
                    }
                }
                ExpectedSubtreeClosureState::Closed => {
                    child_partitioned = true;
                    batches.extend(child_plan.batches);
                }
            }
        }

        if !child_partitioned && total_bytes >= batch_ready_bytes {
            return ExpectedSubtreePlan {
                closure: ExpectedSubtreeClosureState::Closed,
                total_bytes: 0,
                root_is_empty: false,
                batches: vec![expected_full_dir_batch_for_test(
                    src_root_dir,
                    root_relpath,
                    skipped_dirs,
                    skipped_files,
                )],
            };
        }
        if !child_partitioned {
            return ExpectedSubtreePlan {
                closure: ExpectedSubtreeClosureState::Mergeable,
                total_bytes,
                root_is_empty: false,
                batches: Vec::new(),
            };
        }

        for child_rel in mergeable_child_relpaths {
            batches.push(expected_full_dir_batch_for_test(
                src_root_dir,
                child_rel.as_str(),
                skipped_dirs,
                skipped_files,
            ));
        }
        if !direct_files.is_empty() || !mergeable_empty_child_relpaths.is_empty() {
            batches.push(ExpectedBatch {
                root_relpath: root_relpath.to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                entries: direct_files,
                empty_dir_relpaths: mergeable_empty_child_relpaths,
            });
        }
        ExpectedSubtreePlan {
            closure: ExpectedSubtreeClosureState::Closed,
            total_bytes: 0,
            root_is_empty: false,
            batches,
        }
    }

    fn collect_expected_batches(
        src_root_dir: &TempDir,
        batch_ready_bytes: i64,
        skip_entries: &[LocalTransferSkipEntry],
    ) -> Vec<ExpectedBatch> {
        let (skipped_dirs, skipped_files) = normalize_test_skip_entries(skip_entries);
        let (root_has_visible_entries, root_direct_files, root_child_dirs) = collect_direct_listing_for_test(
            src_root_dir.path(),
            ".",
            &skipped_dirs,
            &skipped_files,
        );
        let mut out = Vec::new();
        if !root_has_visible_entries {
            out.push(expected_full_dir_batch_for_test(
                src_root_dir.path(),
                ".",
                &skipped_dirs,
                &skipped_files,
            ));
            return out;
        }
        for child_rel in root_child_dirs {
            let child_plan = plan_expected_subtree_batches_for_test(
                src_root_dir.path(),
                child_rel.as_str(),
                batch_ready_bytes,
                &skipped_dirs,
                &skipped_files,
            );
            match child_plan.closure {
                ExpectedSubtreeClosureState::Mergeable => {
                    out.push(expected_full_dir_batch_for_test(
                        src_root_dir.path(),
                        child_rel.as_str(),
                        &skipped_dirs,
                        &skipped_files,
                    ));
                }
                ExpectedSubtreeClosureState::Closed => {
                    out.extend(child_plan.batches);
                }
            }
        }
        if !root_direct_files.is_empty() {
            out.push(ExpectedBatch {
                root_relpath: ".".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                entries: root_direct_files,
                empty_dir_relpaths: Vec::new(),
            });
        }
        out.sort_by(|a, b| {
            a.root_relpath
                .cmp(&b.root_relpath)
                .then_with(|| a.batch_kind.as_db_str().cmp(b.batch_kind.as_db_str()))
        });
        out
    }

    fn collect_actual_ready_batches(
        transfer_state_store: FluxonFsTransferStateStoreConfig,
        job_id: &str,
    ) -> Vec<ActualBatch> {
        let state = new_local_gateway_state(transfer_state_store).unwrap();
        let mut out = Vec::new();
        for batch in state.list_transfer_batches().unwrap() {
            if batch.job_id != job_id {
                continue;
            }
            let manifest = FluxonFsTransferManifestWire::decode_from_blob(batch.manifest_blob.as_slice())
                .unwrap();
            let mut entries = manifest
                .entries
                .into_iter()
                .map(|entry| (entry.relpath, entry.size))
                .collect::<Vec<_>>();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            let mut empty_dir_relpaths = manifest.empty_dir_relpaths;
            empty_dir_relpaths.sort();
            out.push(ActualBatch {
                root_relpath: batch.root_relpath,
                batch_kind: batch.batch_kind,
                entries,
                empty_dir_relpaths,
            });
        }
        out.sort_by(|a, b| {
            a.root_relpath
                .cmp(&b.root_relpath)
                .then_with(|| a.batch_kind.as_db_str().cmp(b.batch_kind.as_db_str()))
        });
        out
    }

    fn repo_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf()
    }

    fn tikv_ext_dir() -> PathBuf {
        repo_root().join("fluxon_release").join("ext_images").join("tikv")
    }

    fn require_tikv_runtime_path(relname: &str) -> PathBuf {
        let path = tikv_ext_dir().join(relname);
        assert!(
            path.is_file(),
            "missing TiKV runtime file: {}. Run `python3 setup_and_pack/pack_release_ext.py` first.",
            path.display()
        );
        path
    }

    fn pick_free_port() -> u16 {
        TcpListener::bind(("127.0.0.1", 0))
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    fn read_log_file(path: &Path) -> String {
        match fs::read_to_string(path) {
            Ok(v) => v,
            Err(e) => format!("read log failed: path={} err={}", path.display(), e),
        }
    }

    fn terminate_child(child: &mut Child) {
        let _ = child.kill();
        let _ = child.wait();
    }

    fn wait_for_tcp_ready(label: &str, port: u16, child: &mut Child, log_path: &Path) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                return;
            }
            if let Some(status) = child.try_wait().unwrap() {
                panic!(
                    "{} exited early: status={} log={}",
                    label,
                    status,
                    read_log_file(log_path)
                );
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {} on port {} log={}",
                label,
                port,
                read_log_file(log_path)
            );
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn wait_for_tikv_transfer_state_store_ready(
        transfer_state_store: FluxonFsTransferStateStoreConfig,
        pd_log_path: &Path,
        tikv_log_path: &Path,
    ) {
        let deadline =
            Instant::now() + Duration::from_secs(LOCAL_TRANSFER_TEST_TIKV_READY_TIMEOUT_SECS);
        loop {
            let last_err = match new_local_gateway_state(transfer_state_store.clone()) {
                Ok(state) => match state.list_running_transfer_jobs() {
                    Ok(_) => return,
                    Err(e) => e,
                },
                Err(e) => e.to_string(),
            };
            assert!(
                Instant::now() < deadline,
                "timed out waiting for TiKV transfer state store readiness: err={} pd_log={} tikv_log={}",
                last_err,
                read_log_file(pd_log_path),
                read_log_file(tikv_log_path)
            );
            thread::sleep(Duration::from_millis(200));
        }
    }

    struct LocalTiKvHarness {
        _runtime_dir: TempDir,
        pd_proc: Child,
        tikv_proc: Child,
        pd_log_path: PathBuf,
        tikv_log_path: PathBuf,
        transfer_state_store: FluxonFsTransferStateStoreConfig,
    }

    impl LocalTiKvHarness {
        fn new() -> Self {
            let runtime_dir = new_tikv_runtime_dir();
            let pd_port = pick_free_port();
            let pd_peer_port = pick_free_port();
            let tikv_port = pick_free_port();
            let tikv_status_port = pick_free_port();

            let pd_start_script = require_tikv_runtime_path("start_pd.sh");
            let tikv_start_script = require_tikv_runtime_path("start_tikv.sh");

            let pd_config_path = runtime_dir.path().join("pd_config.sh");
            let pd_runtime_config_path = runtime_dir.path().join("pd.toml");
            let tikv_config_path = runtime_dir.path().join("tikv_config.sh");
            let tikv_runtime_config_path = runtime_dir.path().join("tikv.toml");
            let pd_log_path = runtime_dir.path().join("pd.log");
            let tikv_log_path = runtime_dir.path().join("tikv.log");
            let pd_endpoint = format!("127.0.0.1:{}", pd_port);

            fs::write(
                &pd_runtime_config_path,
                format!("lease = {}\n", LOCAL_TRANSFER_TEST_TIKV_PD_LEASE_SECS),
            )
            .unwrap();
            fs::write(
                &pd_config_path,
                format!(
                    "declare -a PD_ARGS=(\n\
  --config \"$WORKDIR/pd.toml\"\n\
  --name pd0\n\
  --data-dir \"$WORKDIR/pd-data\"\n\
  --client-urls \"http://127.0.0.1:{pd_port}\"\n\
  --advertise-client-urls \"http://127.0.0.1:{pd_port}\"\n\
  --peer-urls \"http://127.0.0.1:{pd_peer_port}\"\n\
  --advertise-peer-urls \"http://127.0.0.1:{pd_peer_port}\"\n\
  --initial-cluster \"pd0=http://127.0.0.1:{pd_peer_port}\"\n\
  --log-file \"$WORKDIR/pd.log\"\n\
)\n"
                ),
            )
            .unwrap();
            fs::write(
                &tikv_runtime_config_path,
                format!(
                    "[readpool.unified]\n\
max-thread-count = {LOCAL_TRANSFER_TEST_TIKV_UNIFIED_READPOOL_MAX_THREADS}\n\
\n\
[readpool.storage]\n\
high-concurrency = {LOCAL_TRANSFER_TEST_TIKV_STORAGE_READPOOL_CONCURRENCY}\n\
normal-concurrency = {LOCAL_TRANSFER_TEST_TIKV_STORAGE_READPOOL_CONCURRENCY}\n\
low-concurrency = {LOCAL_TRANSFER_TEST_TIKV_STORAGE_READPOOL_CONCURRENCY}\n\
\n\
[readpool.coprocessor]\n\
high-concurrency = {LOCAL_TRANSFER_TEST_TIKV_COPROCESSOR_READPOOL_CONCURRENCY}\n\
normal-concurrency = {LOCAL_TRANSFER_TEST_TIKV_COPROCESSOR_READPOOL_CONCURRENCY}\n\
low-concurrency = {LOCAL_TRANSFER_TEST_TIKV_COPROCESSOR_READPOOL_CONCURRENCY}\n\
\n\
[server]\n\
end-point-max-concurrency = {LOCAL_TRANSFER_TEST_TIKV_ENDPOINT_MAX_CONCURRENCY}\n\
background-thread-count = {LOCAL_TRANSFER_TEST_TIKV_BACKGROUND_THREAD_COUNT}\n\
\n\
[storage]\n\
scheduler-concurrency = {LOCAL_TRANSFER_TEST_TIKV_SCHEDULER_CONCURRENCY}\n\
scheduler-worker-pool-size = {LOCAL_TRANSFER_TEST_TIKV_SCHEDULER_WORKER_POOL_SIZE}\n\
\n\
[raftstore]\n\
apply-pool-size = {LOCAL_TRANSFER_TEST_TIKV_APPLY_POOL_SIZE}\n\
store-pool-size = {LOCAL_TRANSFER_TEST_TIKV_STORE_POOL_SIZE}\n\
\n\
[rocksdb]\n\
max-background-jobs = {LOCAL_TRANSFER_TEST_TIKV_ROCKSDB_MAX_BACKGROUND_JOBS}\n\
\n\
[raftdb]\n\
max-background-jobs = {LOCAL_TRANSFER_TEST_TIKV_RAFTDB_MAX_BACKGROUND_JOBS}\n"
                ),
            )
            .unwrap();
            fs::write(
                &tikv_config_path,
                format!(
                    "declare -a TIKV_ARGS=(\n\
  --config \"$WORKDIR/tikv.toml\"\n\
  --pd-endpoints \"{pd_endpoint}\"\n\
  --addr \"127.0.0.1:{tikv_port}\"\n\
  --advertise-addr \"127.0.0.1:{tikv_port}\"\n\
  --status-addr \"127.0.0.1:{tikv_status_port}\"\n\
  --data-dir \"$WORKDIR/tikv-data\"\n\
  --log-file \"$WORKDIR/tikv.log\"\n\
)\n"
                ),
            )
            .unwrap();

            let pd_stdout = File::create(&pd_log_path).unwrap();
            let pd_stderr = pd_stdout.try_clone().unwrap();
            let mut pd_proc = Command::new(&pd_start_script)
                .arg("--config")
                .arg(&pd_config_path)
                .arg("--workdir")
                .arg(runtime_dir.path())
                .stdin(Stdio::null())
                .stdout(Stdio::from(pd_stdout))
                .stderr(Stdio::from(pd_stderr))
                .spawn()
                .unwrap();
            wait_for_tcp_ready("pd-server", pd_port, &mut pd_proc, &pd_log_path);

            let tikv_stdout = File::create(&tikv_log_path).unwrap();
            let tikv_stderr = tikv_stdout.try_clone().unwrap();
            let mut tikv_proc = Command::new(&tikv_start_script)
                .arg("--config")
                .arg(&tikv_config_path)
                .arg("--workdir")
                .arg(runtime_dir.path())
                .stdin(Stdio::null())
                .stdout(Stdio::from(tikv_stdout))
                .stderr(Stdio::from(tikv_stderr))
                .spawn()
                .unwrap();
            wait_for_tcp_ready("tikv-server", tikv_port, &mut tikv_proc, &tikv_log_path);

            let transfer_state_store = build_local_tikv_transfer_state_store_config(
                vec![pd_endpoint],
                format!("/fluxon_fs_transfer_test/{}/", Uuid::new_v4()),
            );
            wait_for_tikv_transfer_state_store_ready(
                transfer_state_store.clone(),
                &pd_log_path,
                &tikv_log_path,
            );

            Self {
                _runtime_dir: runtime_dir,
                pd_proc,
                tikv_proc,
                pd_log_path,
                tikv_log_path,
                transfer_state_store,
            }
        }
    }

    impl Drop for LocalTiKvHarness {
        fn drop(&mut self) {
            terminate_child(&mut self.tikv_proc);
            terminate_child(&mut self.pd_proc);
        }
    }

    fn materialize_batch_outputs_for_local_e2e(
        state: &GatewayState,
        job_id: &str,
        batch_id: &str,
        src_root: &Path,
        dst_root: &Path,
    ) {
        execute_local_transfer_worker_for_batch(state, job_id, batch_id, src_root, dst_root).unwrap();
    }

    fn assert_actual_batches_match_expected(
        actual_batches: &[ActualBatch],
        expected_batches: &[ExpectedBatch],
    ) {
        assert_eq!(actual_batches.len(), expected_batches.len());
        let mut seen_relpaths = BTreeSet::new();
        for actual in actual_batches {
            for (relpath, _) in actual.entries.iter() {
                assert!(
                    seen_relpaths.insert(relpath.clone()),
                    "duplicate relpath across actual batches: {}",
                    relpath
                );
            }
            for relpath in actual.empty_dir_relpaths.iter() {
                assert!(
                    seen_relpaths.insert(relpath.clone()),
                    "duplicate relpath across actual batches: {}",
                    relpath
                );
            }
        }
        for (actual, expected) in actual_batches.iter().zip(expected_batches.iter()) {
            assert_eq!(actual.root_relpath, expected.root_relpath);
            assert_eq!(actual.batch_kind, expected.batch_kind);
            assert_eq!(actual.entries, expected.entries);
            assert_eq!(actual.empty_dir_relpaths, expected.empty_dir_relpaths);
        }
    }

    fn wait_until_job_has_durable_batch(
        transfer_state_store: FluxonFsTransferStateStoreConfig,
    ) -> String {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let state = new_local_gateway_state(transfer_state_store.clone()).unwrap();
            let batches = state.list_transfer_batches().unwrap();
            let jobs = state.list_running_transfer_jobs().unwrap();
            for job in jobs {
                if job.state != FluxonFsTransferJobState::Running {
                    continue;
                }
                if batches.iter().any(|batch| batch.job_id == job.job_id) {
                    return job.job_id;
                }
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for local checker to persist at least one durable batch"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn run_child_check_from_env() {
        let src_root_dir = std::env::var("FLUXON_FS_TEST_CHILD_SRC_ROOT_DIR").unwrap();
        let transfer_state_store_json =
            std::env::var(LOCAL_TRANSFER_TEST_CHILD_TRANSFER_STATE_STORE_JSON_ENV).unwrap();
        let transfer_state_store: FluxonFsTransferStateStoreConfig =
            serde_json::from_str(transfer_state_store_json.as_str()).unwrap();
        let batch_ready_bytes: i64 = std::env::var("FLUXON_FS_TEST_CHILD_BATCH_READY_BYTES")
            .unwrap()
            .parse()
            .unwrap();
        let skip_entries_json = std::env::var("FLUXON_FS_TEST_CHILD_SKIP_ENTRIES_JSON").unwrap();
        let skip_entries: Vec<LocalTransferSkipEntry> =
            serde_json::from_str(skip_entries_json.as_str()).unwrap();
        run_local_transfer_check_blocking(LocalTransferCheckArg {
            src_root_dir,
            transfer_state_store,
            batch_ready_bytes,
            skip_entries,
            checker_concurrency_limit: Some(1),
            enable_cli_progress: false,
        })
        .unwrap();
    }

    fn maybe_run_child_check_from_env() -> bool {
        if std::env::var("FLUXON_FS_TEST_CHILD_MODE").ok().as_deref() != Some("run_local_check") {
            return false;
        }
        run_child_check_from_env();
        true
    }

    #[test]
    #[ignore = "requires fluxon_release/ext_images/tikv prepared by setup_and_pack/pack_release_ext.py"]
    fn local_transfer_checker_reuses_existing_job_for_same_spec() {
        if maybe_run_child_check_from_env() {
            return;
        }
        let src = new_fixture_temp_dir();
        let tikv = LocalTiKvHarness::new();
        write_file(&src, "root/a.bin", b"abc");

        let first = run_local_transfer_check_blocking(LocalTransferCheckArg {
            src_root_dir: src.path().to_str().unwrap().to_string(),
            transfer_state_store: tikv.transfer_state_store.clone(),
            batch_ready_bytes: 4,
            skip_entries: Vec::new(),
            checker_concurrency_limit: Some(1),
            enable_cli_progress: false,
        })
        .unwrap();
        let second = run_local_transfer_check_blocking(LocalTransferCheckArg {
            src_root_dir: src.path().to_str().unwrap().to_string(),
            transfer_state_store: tikv.transfer_state_store.clone(),
            batch_ready_bytes: 4,
            skip_entries: Vec::new(),
            checker_concurrency_limit: Some(1),
            enable_cli_progress: false,
        })
        .unwrap();

        assert_eq!(first.job_id, second.job_id);
    }

    #[test]
    fn local_transfer_checker_oracle_prefers_deeper_qualifying_subtree() {
        let src = new_fixture_temp_dir();
        write_repeat_file(&src, "root/parent/direct.bin", b'a', 10);
        write_repeat_file(&src, "root/parent/child/grand.bin", b'b', 10);

        let expected_batches = collect_expected_batches(&src, 15, &[]);
        assert_eq!(
            expected_batches,
            vec![ExpectedBatch {
                root_relpath: "root/parent".to_string(),
                batch_kind: FluxonFsTransferBatchKind::FullDir,
                entries: vec![
                    ("root/parent/child/grand.bin".to_string(), 10),
                    ("root/parent/direct.bin".to_string(), 10),
                ],
                empty_dir_relpaths: Vec::new(),
            }]
        );
    }

    #[test]
    fn local_transfer_checker_oracle_closes_mergeable_sibling_subtrees_after_partition_boundary() {
        let src = new_fixture_temp_dir();
        write_repeat_file(&src, "root/big/a.bin", b'a', 8);
        write_repeat_file(&src, "root/big/b.bin", b'b', 9);
        write_repeat_file(&src, "root/small/direct.bin", b'c', 5);
        write_repeat_file(&src, "root/small/child/leaf.bin", b'd', 4);

        let expected_batches = collect_expected_batches(&src, 15, &[]);
        assert_eq!(
            expected_batches,
            vec![
                ExpectedBatch {
                    root_relpath: "root/big".to_string(),
                    batch_kind: FluxonFsTransferBatchKind::FullDir,
                    entries: vec![
                        ("root/big/a.bin".to_string(), 8),
                        ("root/big/b.bin".to_string(), 9),
                    ],
                    empty_dir_relpaths: Vec::new(),
                },
                ExpectedBatch {
                    root_relpath: "root/small".to_string(),
                    batch_kind: FluxonFsTransferBatchKind::FullDir,
                    entries: vec![
                        ("root/small/child/leaf.bin".to_string(), 4),
                        ("root/small/direct.bin".to_string(), 5),
                    ],
                    empty_dir_relpaths: Vec::new(),
                },
            ]
        );
    }

    #[test]
    fn local_transfer_checker_oracle_groups_empty_children_into_direct_batch_after_partition_boundary() {
        let src = new_fixture_temp_dir();
        write_repeat_file(&src, "root/big/a.bin", b'a', 8);
        write_repeat_file(&src, "root/big/b.bin", b'b', 9);
        fs::create_dir_all(src.path().join("root/empty-a")).unwrap();
        fs::create_dir_all(src.path().join("root/empty-b")).unwrap();

        let expected_batches = collect_expected_batches(&src, 15, &[]);
        assert_eq!(
            expected_batches,
            vec![
                ExpectedBatch {
                    root_relpath: "root".to_string(),
                    batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                    entries: Vec::new(),
                    empty_dir_relpaths: vec![
                        "root/empty-a".to_string(),
                        "root/empty-b".to_string(),
                    ],
                },
                ExpectedBatch {
                    root_relpath: "root/big".to_string(),
                    batch_kind: FluxonFsTransferBatchKind::FullDir,
                    entries: vec![
                        ("root/big/a.bin".to_string(), 8),
                        ("root/big/b.bin".to_string(), 9),
                    ],
                    empty_dir_relpaths: Vec::new(),
                },
            ]
        );
    }

    #[test]
    fn local_known_dispositions_include_descendants_for_root_assignment() {
        let batches = vec![
            FsTransferBatchRecord {
                job_id: "job".to_string(),
                batch_id: "batch-root".to_string(),
                root_relpath: ".".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                state: FluxonFsTransferBatchState::Ready,
                assigned_src_exporter_id: String::new(),
                assigned_dst_exporter_id: String::new(),
                owner_worker_id: String::new(),
                owner_worker_task_id: String::new(),
                lease_expire_unix_ms: 0,
                manifest_blob: Vec::new(),
                generation: 1,
                last_counted_scan_epoch: 0,
            },
            FsTransferBatchRecord {
                job_id: "job".to_string(),
                batch_id: "batch-child".to_string(),
                root_relpath: "root/big".to_string(),
                batch_kind: FluxonFsTransferBatchKind::FullDir,
                state: FluxonFsTransferBatchState::Ready,
                assigned_src_exporter_id: String::new(),
                assigned_dst_exporter_id: String::new(),
                owner_worker_id: String::new(),
                owner_worker_task_id: String::new(),
                lease_expire_unix_ms: 0,
                manifest_blob: Vec::new(),
                generation: 1,
                last_counted_scan_epoch: 0,
            },
            FsTransferBatchRecord {
                job_id: "job".to_string(),
                batch_id: "batch-other-generation".to_string(),
                root_relpath: "root/other".to_string(),
                batch_kind: FluxonFsTransferBatchKind::FullDir,
                state: FluxonFsTransferBatchState::Ready,
                assigned_src_exporter_id: String::new(),
                assigned_dst_exporter_id: String::new(),
                owner_worker_id: String::new(),
                owner_worker_task_id: String::new(),
                lease_expire_unix_ms: 0,
                manifest_blob: Vec::new(),
                generation: 2,
                last_counted_scan_epoch: 0,
            },
            FsTransferBatchRecord {
                job_id: "other-job".to_string(),
                batch_id: "batch-other-job".to_string(),
                root_relpath: "root/foreign".to_string(),
                batch_kind: FluxonFsTransferBatchKind::FullDir,
                state: FluxonFsTransferBatchState::Ready,
                assigned_src_exporter_id: String::new(),
                assigned_dst_exporter_id: String::new(),
                owner_worker_id: String::new(),
                owner_worker_task_id: String::new(),
                lease_expire_unix_ms: 0,
                manifest_blob: Vec::new(),
                generation: 1,
                last_counted_scan_epoch: 0,
            },
        ];

        let known = build_known_dispositions_for_local_scan("job", ".", 1, &batches, &[]);
        assert_eq!(
            known,
            vec![
                FluxonFsTransferDispositionWire {
                    root_relpath: "root/big".to_string(),
                    generation: 1,
                    batch_kind: FluxonFsTransferBatchKind::FullDir,
                },
                FluxonFsTransferDispositionWire {
                    root_relpath: "root/other".to_string(),
                    generation: 2,
                    batch_kind: FluxonFsTransferBatchKind::FullDir,
                },
            ]
        );
    }

    #[test]
    fn local_known_dispositions_exclude_exact_root_direct_files_only_batch_without_complete_marker() {
        let known = build_known_dispositions_for_local_scan(
            "job",
            "root",
            1,
            &[FsTransferBatchRecord {
                job_id: "job".to_string(),
                batch_id: "batch-root".to_string(),
                root_relpath: "root".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                state: FluxonFsTransferBatchState::Ready,
                assigned_src_exporter_id: String::new(),
                assigned_dst_exporter_id: String::new(),
                owner_worker_id: String::new(),
                owner_worker_task_id: String::new(),
                lease_expire_unix_ms: 0,
                manifest_blob: Vec::new(),
                generation: 1,
                last_counted_scan_epoch: 0,
            }],
            &[],
        );
        assert!(known.is_empty());
    }

    #[test]
    fn local_known_dispositions_include_direct_files_complete_marker() {
        let known = build_known_dispositions_for_local_scan(
            "job",
            "root",
            11,
            &[],
            &[fluxon_fs_s3_gateway::FsTransferDirectFilesCompleteRecord {
                job_id: "job".to_string(),
                root_relpath: "root".to_string(),
                completed_at_unix_ms: 456,
            }],
        );
        assert_eq!(
            known,
            vec![FluxonFsTransferDispositionWire {
                root_relpath: "root".to_string(),
                generation: 11,
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
            }]
        );
    }

    #[test]
    #[ignore = "requires fluxon_release/ext_images/tikv prepared by setup_and_pack/pack_release_ext.py"]
    fn local_transfer_checker_skip_entries_exclude_subtree_and_file() {
        if maybe_run_child_check_from_env() {
            return;
        }
        let src = new_fixture_temp_dir();
        let tikv = LocalTiKvHarness::new();
        write_file(&src, "keep/direct.bin", b"123");
        write_file(&src, "skipdir/child.bin", b"456");
        write_file(&src, "keep/skip.bin", b"789");

        let summary = run_local_transfer_check_blocking(LocalTransferCheckArg {
            src_root_dir: src.path().to_str().unwrap().to_string(),
            transfer_state_store: tikv.transfer_state_store.clone(),
            batch_ready_bytes: 1024,
            skip_entries: vec![
                LocalTransferSkipEntry {
                    kind: LocalTransferSkipEntryKind::Dir,
                    relpath: "skipdir".to_string(),
                },
                LocalTransferSkipEntry {
                    kind: LocalTransferSkipEntryKind::File,
                    relpath: "keep/skip.bin".to_string(),
                },
            ],
            checker_concurrency_limit: Some(1),
            enable_cli_progress: false,
        })
        .unwrap();

        assert_eq!(summary.full_dir_batch_count, 1);
        assert_eq!(summary.direct_files_only_batch_count, 0);
        let state = new_local_gateway_state(tikv.transfer_state_store.clone()).unwrap();
        let batches = state.list_transfer_batches().unwrap();
        let mut visible_relpaths = Vec::new();
        for batch in batches {
            if batch.job_id != summary.job_id {
                continue;
            }
            let manifest = fluxon_fs_core::config::FluxonFsTransferManifestWire::decode_from_blob(
                batch.manifest_blob.as_slice(),
            )
            .unwrap();
            for entry in manifest.entries {
                visible_relpaths.push(entry.relpath);
            }
        }
        visible_relpaths.sort();
        assert_eq!(visible_relpaths, vec!["keep/direct.bin".to_string()]);
    }

    #[test]
    #[ignore = "requires fluxon_release/ext_images/tikv prepared by setup_and_pack/pack_release_ext.py"]
    fn local_transfer_checker_rejects_nested_skip_entries() {
        if maybe_run_child_check_from_env() {
            return;
        }
        let src = new_fixture_temp_dir();
        let tikv = LocalTiKvHarness::new();
        write_file(&src, "a/b/file.bin", b"x");

        let err = run_local_transfer_check_blocking(LocalTransferCheckArg {
            src_root_dir: src.path().to_str().unwrap().to_string(),
            transfer_state_store: tikv.transfer_state_store.clone(),
            batch_ready_bytes: 1024,
            skip_entries: vec![
                LocalTransferSkipEntry {
                    kind: LocalTransferSkipEntryKind::Dir,
                    relpath: "a".to_string(),
                },
                LocalTransferSkipEntry {
                    kind: LocalTransferSkipEntryKind::File,
                    relpath: "a/b/file.bin".to_string(),
                },
            ],
            checker_concurrency_limit: Some(1),
            enable_cli_progress: false,
        })
        .unwrap_err();

        assert!(format!("{}", err).contains("nested skip entries are not allowed"));
    }

    #[test]
    #[ignore = "requires fluxon_release/ext_images/tikv prepared by setup_and_pack/pack_release_ext.py"]
    fn local_transfer_checker_recovers_after_process_kill_and_matches_expected_batches() {
        if maybe_run_child_check_from_env() {
            return;
        }

        let src = new_fixture_temp_dir();
        build_kill_resume_fixture(&src);
        let tikv = LocalTiKvHarness::new();
        let batch_ready_bytes = 15;
        let skip_entries = vec![
            LocalTransferSkipEntry {
                kind: LocalTransferSkipEntryKind::Dir,
                relpath: "skip-dir".to_string(),
            },
            LocalTransferSkipEntry {
                kind: LocalTransferSkipEntryKind::File,
                relpath: "keep/skip-file.bin".to_string(),
            },
        ];
        let skip_entries_json = serde_json::to_string(&skip_entries).unwrap();
        let transfer_state_store_json =
            serde_json::to_string(&tikv.transfer_state_store).unwrap();

        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(LOCAL_TRANSFER_CHECKER_KILL_RESUME_TEST_NAME)
            .env("FLUXON_FS_TEST_CHILD_MODE", "run_local_check")
            .env(
                "FLUXON_FS_TEST_CHILD_SRC_ROOT_DIR",
                src.path().to_str().unwrap(),
            )
            .env(
                LOCAL_TRANSFER_TEST_CHILD_TRANSFER_STATE_STORE_JSON_ENV,
                transfer_state_store_json,
            )
            .env(
                "FLUXON_FS_TEST_CHILD_BATCH_READY_BYTES",
                batch_ready_bytes.to_string(),
            )
            .env(
                "FLUXON_FS_TEST_CHILD_SKIP_ENTRIES_JSON",
                skip_entries_json,
            )
            .env(LOCAL_TRANSFER_TEST_SLEEP_AFTER_APPLY_MS_ENV, "250")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        let partial_job_id = wait_until_job_has_durable_batch(tikv.transfer_state_store.clone());
        child.kill().unwrap();
        let _ = child.wait().unwrap();

        let state_before_resume = new_local_gateway_state(tikv.transfer_state_store.clone()).unwrap();
        let job_before_resume = state_before_resume
            .list_running_transfer_jobs()
            .unwrap()
            .into_iter()
            .find(|job| job.job_id == partial_job_id)
            .unwrap();
        assert!(!job_before_resume.scan_finished);
        let partial_batch_count = state_before_resume
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .filter(|batch| batch.job_id == partial_job_id)
            .count();
        assert!(partial_batch_count > 0);

        let resumed = run_local_transfer_check_blocking(local_check_arg_with_store(
            &src,
            tikv.transfer_state_store.clone(),
            batch_ready_bytes,
            skip_entries.clone(),
        ))
        .unwrap();
        assert_eq!(resumed.job_id, partial_job_id);

        let final_state = new_local_gateway_state(tikv.transfer_state_store.clone()).unwrap();
        let final_job = final_state
            .list_running_transfer_jobs()
            .unwrap()
            .into_iter()
            .find(|job| job.job_id == resumed.job_id)
            .unwrap();
        let final_spec = decode_job_spec(final_job.job_spec_blob.as_slice()).unwrap();
        assert_eq!(
            final_spec.src_root_dir_abs,
            src.path().canonicalize().unwrap().to_str().unwrap()
        );
        assert_eq!(final_spec.batch_ready_bytes, batch_ready_bytes);
        assert!(!final_job.scan_finished || resumed.batch_count >= partial_batch_count);

        let expected_batches =
            collect_expected_batches(&src, batch_ready_bytes, skip_entries.as_slice());
        let actual_batches =
            collect_actual_ready_batches(tikv.transfer_state_store.clone(), resumed.job_id.as_str());
        assert_actual_batches_match_expected(actual_batches.as_slice(), expected_batches.as_slice());

        let actual_file_set = actual_batches
            .iter()
            .flat_map(|batch| batch.entries.iter().cloned())
            .collect::<BTreeMap<_, _>>();
        let expected_file_set = expected_batches
            .iter()
            .flat_map(|batch| batch.entries.iter().cloned())
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual_file_set, expected_file_set);
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "requires fluxon_release/ext_images/tikv prepared by setup_and_pack/pack_release_ext.py"]
    fn local_transfer_checker_best_effort_recovers_permission_denied_subtree_with_tikv_state_store() {
        if maybe_run_child_check_from_env() {
            return;
        }

        let src = new_fixture_temp_dir();
        let tikv = LocalTiKvHarness::new();
        build_kill_resume_fixture(&src);
        let blocked = src.path().join("alpha/deep");
        let mut permissions = fs::metadata(&blocked).unwrap().permissions();
        permissions.set_mode(0o000);
        fs::set_permissions(&blocked, permissions).unwrap();

        let summary = run_local_transfer_check_blocking(local_check_arg_with_store(
            &src,
            tikv.transfer_state_store.clone(),
            15,
            Vec::new(),
        ))
        .unwrap();

        let mut permissions = fs::metadata(&blocked).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&blocked, permissions).unwrap();

        let final_state = new_local_gateway_state(tikv.transfer_state_store.clone()).unwrap();
        let final_job = final_state
            .list_running_transfer_jobs()
            .unwrap()
            .into_iter()
            .find(|job| job.job_id == summary.job_id)
            .unwrap();
        assert!(final_job.scan_finished);

        let expected_batches = collect_expected_batches(&src, 15, &[]);
        let actual_batches =
            collect_actual_ready_batches(tikv.transfer_state_store.clone(), summary.job_id.as_str());
        let mut seen_actual_relpaths = BTreeSet::new();
        for actual in &actual_batches {
            for (relpath, _) in actual.entries.iter() {
                assert!(
                    seen_actual_relpaths.insert(relpath.clone()),
                    "duplicate relpath across actual batches: {}",
                    relpath
                );
            }
            for relpath in actual.empty_dir_relpaths.iter() {
                assert!(
                    seen_actual_relpaths.insert(relpath.clone()),
                    "duplicate relpath across actual batches: {}",
                    relpath
                );
            }
        }

        let actual_file_set = actual_batches
            .iter()
            .flat_map(|batch| batch.entries.iter().cloned())
            .collect::<BTreeMap<_, _>>();
        let expected_file_set = expected_batches
            .iter()
            .flat_map(|batch| batch.entries.iter().cloned())
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual_file_set, expected_file_set);

        let actual_empty_dir_set = actual_batches
            .iter()
            .flat_map(|batch| batch.empty_dir_relpaths.iter().cloned())
            .collect::<BTreeSet<_>>();
        let expected_empty_dir_set = expected_batches
            .iter()
            .flat_map(|batch| batch.empty_dir_relpaths.iter().cloned())
            .collect::<BTreeSet<_>>();
        assert_eq!(actual_empty_dir_set, expected_empty_dir_set);
    }

    #[test]
    #[ignore = "requires fluxon_release/ext_images/tikv prepared by setup_and_pack/pack_release_ext.py"]
    fn local_transfer_checker_collects_expected_batches_with_tikv_state_store() {
        if maybe_run_child_check_from_env() {
            return;
        }

        let src = new_fixture_temp_dir();
        build_kill_resume_fixture(&src);
        let tikv = LocalTiKvHarness::new();
        let batch_ready_bytes = 15;
        let skip_entries = vec![
            LocalTransferSkipEntry {
                kind: LocalTransferSkipEntryKind::Dir,
                relpath: "skip-dir".to_string(),
            },
            LocalTransferSkipEntry {
                kind: LocalTransferSkipEntryKind::File,
                relpath: "keep/skip-file.bin".to_string(),
            },
        ];

        let summary = run_local_transfer_check_blocking(local_check_arg_with_store(
            &src,
            tikv.transfer_state_store.clone(),
            batch_ready_bytes,
            skip_entries.clone(),
        ))
        .unwrap();

        let final_state = new_local_gateway_state(tikv.transfer_state_store.clone()).unwrap();
        let final_job = final_state
            .list_running_transfer_jobs()
            .unwrap()
            .into_iter()
            .find(|job| job.job_id == summary.job_id)
            .unwrap();
        assert!(
            final_job.scan_finished,
            "scan did not finish: pd_log={} tikv_log={}",
            read_log_file(&tikv.pd_log_path),
            read_log_file(&tikv.tikv_log_path)
        );

        let expected_batches =
            collect_expected_batches(&src, batch_ready_bytes, skip_entries.as_slice());
        let actual_batches =
            collect_actual_ready_batches(tikv.transfer_state_store.clone(), summary.job_id.as_str());
        assert_actual_batches_match_expected(actual_batches.as_slice(), expected_batches.as_slice());

        let actual_file_set = actual_batches
            .iter()
            .flat_map(|batch| batch.entries.iter().cloned())
            .collect::<BTreeMap<_, _>>();
        let expected_file_set = expected_batches
            .iter()
            .flat_map(|batch| batch.entries.iter().cloned())
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual_file_set, expected_file_set);
    }

    #[test]
    #[ignore = "requires fluxon_release/ext_images/tikv prepared by setup_and_pack/pack_release_ext.py"]
    fn local_transfer_checker_groups_many_empty_children_into_one_direct_batch_with_tikv_state_store() {
        if maybe_run_child_check_from_env() {
            return;
        }

        let src = new_fixture_temp_dir();
        write_repeat_file(&src, "root/big/a.bin", b'a', 8);
        write_repeat_file(&src, "root/big/b.bin", b'b', 9);
        let expected_empty_dirs = (0..64)
            .map(|idx| {
                let relpath = format!("root/empty-{idx:03}");
                fs::create_dir_all(src.path().join(relpath.as_str())).unwrap();
                relpath
            })
            .collect::<Vec<_>>();

        let tikv = LocalTiKvHarness::new();
        let summary = run_local_transfer_check_blocking(local_check_arg_with_store(
            &src,
            tikv.transfer_state_store.clone(),
            15,
            Vec::new(),
        ))
        .unwrap();

        let actual_batches =
            collect_actual_ready_batches(tikv.transfer_state_store.clone(), summary.job_id.as_str());
        assert_actual_batches_match_expected(
            actual_batches.as_slice(),
            &[
                ExpectedBatch {
                    root_relpath: "root".to_string(),
                    batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                    entries: Vec::new(),
                    empty_dir_relpaths: expected_empty_dirs,
                },
                ExpectedBatch {
                    root_relpath: "root/big".to_string(),
                    batch_kind: FluxonFsTransferBatchKind::FullDir,
                    entries: vec![
                        ("root/big/a.bin".to_string(), 8),
                        ("root/big/b.bin".to_string(), 9),
                    ],
                    empty_dir_relpaths: Vec::new(),
                },
            ],
        );
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "requires fluxon_release/ext_images/tikv prepared by setup_and_pack/pack_release_ext.py"]
    fn local_transfer_checker_end_to_end_completes_job_with_tikv_state_store() {
        if maybe_run_child_check_from_env() {
            return;
        }

        let src = new_fixture_temp_dir();
        build_kill_resume_fixture(&src);
        write_symlink(&src, "keep/link-to-keep", "keep-file.bin");

        let tikv = LocalTiKvHarness::new();
        let batch_ready_bytes = 15;
        let skip_entries = vec![
            LocalTransferSkipEntry {
                kind: LocalTransferSkipEntryKind::Dir,
                relpath: "skip-dir".to_string(),
            },
            LocalTransferSkipEntry {
                kind: LocalTransferSkipEntryKind::File,
                relpath: "keep/skip-file.bin".to_string(),
            },
        ];

        let summary = run_local_transfer_check_blocking(local_check_arg_with_store(
            &src,
            tikv.transfer_state_store.clone(),
            batch_ready_bytes,
            skip_entries.clone(),
        ))
        .unwrap();

        let dst = new_fixture_temp_dir();
        let backend = Arc::new(LocalFsStatBackend {
            dst_root: dst.path().to_path_buf(),
        });
        let state =
            new_local_gateway_state_with_backend(tikv.transfer_state_store.clone(), backend).unwrap();

        let mut iterations = 0;
        loop {
            iterations += 1;
            assert!(
                iterations <= 32,
                "local e2e worker loop did not converge: job_id={}",
                summary.job_id
            );
            let snapshot = state
                .transfer_job_snapshot(summary.job_id.as_str())
                .unwrap()
                .unwrap();
            if snapshot.job.state == FluxonFsTransferJobState::Completed {
                assert_eq!(snapshot.open_batches, 0);
                break;
            }
            let ready_batches = state
                .list_transfer_batches()
                .unwrap()
                .into_iter()
                .filter(|batch| {
                    batch.job_id == summary.job_id
                        && batch.state == FluxonFsTransferBatchState::Ready
                })
                .collect::<Vec<_>>();
            assert!(
                !ready_batches.is_empty(),
                "expected ready batch while job not completed: job_id={}",
                summary.job_id
            );
            for batch in ready_batches {
                materialize_batch_outputs_for_local_e2e(
                    &state,
                    summary.job_id.as_str(),
                    batch.batch_id.as_str(),
                    src.path(),
                    dst.path(),
                );
                state
                    .reconcile_transfer_scheduler_state(chrono::Utc::now().timestamp_millis())
                    .unwrap();
            }
        }

        let final_state = new_local_gateway_state_with_backend(
            tikv.transfer_state_store.clone(),
            Arc::new(LocalFsStatBackend {
                dst_root: dst.path().to_path_buf(),
            }),
        )
        .unwrap();
        let final_job = final_state
            .list_running_transfer_jobs()
            .unwrap()
            .into_iter()
            .find(|job| job.job_id == summary.job_id);
        assert!(final_job.is_none());
        let final_snapshot = final_state
            .transfer_job_snapshot(summary.job_id.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(final_snapshot.job.state, FluxonFsTransferJobState::Completed);
        assert_eq!(final_snapshot.open_batches, 0);
        let collect_infos = final_state
            .list_transfer_batch_collect_infos()
            .unwrap()
            .into_iter()
            .filter(|row| row.job_id == summary.job_id)
            .collect::<Vec<_>>();
        assert!(
            collect_infos.iter().all(|row| row.materialized),
            "all batch collect infos must be materialized once job completes"
        );
        let (skipped_dirs, skipped_files) = normalize_test_skip_entries(skip_entries.as_slice());
        let expected_entries = collect_entries_for_subtree_oracle(
            src.path(),
            ".",
            &skipped_dirs,
            &skipped_files,
        );
        for (relpath, expected_size) in expected_entries {
            let src_bytes = fs::read(src.path().join(relpath.as_str())).unwrap();
            let dst_bytes = fs::read(dst.path().join(relpath.as_str())).unwrap();
            assert_eq!(dst_bytes.len() as i64, expected_size);
            assert_eq!(dst_bytes, src_bytes, "mismatched file bytes for {}", relpath);
        }
    }
}
