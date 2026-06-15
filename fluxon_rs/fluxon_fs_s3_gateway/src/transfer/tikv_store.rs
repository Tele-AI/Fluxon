use std::ops::Bound;
use std::sync::Arc;
use std::thread;

use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use fluxon_fs_core::config::{
    FLUXON_FS_LOCAL_TRANSFER_CHECK_DST_EXPORT, FLUXON_FS_LOCAL_TRANSFER_CHECK_SRC_EXPORT,
    FluxonFsGlobalConfig, FluxonFsTransferBatchCollectInfoWire, FluxonFsTransferBatchKind,
    FluxonFsTransferBatchState, FluxonFsTransferCollectInfoKind, FluxonFsTransferJobState,
    FluxonFsTransferManifestWire, FluxonFsTransferScanBatchWire, FluxonFsTransferScanResultWire,
    FluxonFsTransferStateStoreTiKvConfig, FluxonFsTransferSymlinkNoticeEntryWire,
    FluxonFsTransferWorkerHeartbeatResultWire, FluxonFsTransferWorkerHeartbeatWire,
    FluxonFsTransferWorkerResultAckWire, FluxonFsTransferWorkerResultWire,
    FluxonFsTransferWorkerStopReasonWire, transfer_collect_info_output_relpath,
};
use fluxon_util::prefix_scan::{
    PrefixScanAction, prefix_scan_key_after, prefix_scan_range_end_exclusive,
};
use postcard::{from_bytes as postcard_from_bytes, to_stdvec as postcard_to_stdvec};
use serde::de::DeserializeOwned;
use tikv_client::{BoundRange, Key, Transaction, TransactionClient};
use tracing::info;

use crate::FsS3Backend;

use super::db::{decode_transfer_manifest_blob, normalize_transfer_root_relpath};
use super::types::{
    FsTransferBatchCollectInfoRecord, FsTransferBatchFileIssueRecord, FsTransferBatchRecord,
    FsTransferDirectFilesCompleteRecord, FsTransferJobRecord, FsTransferJobSnapshot,
    FsTransferJobSummarySnapshot, FsTransferReadyBatchClass, FsTransferReadyBatchDispatch,
    FsTransferRunningBatchOwnerSnapshot, FsTransferSchedulerJobSnapshot,
    FsTransferWorkerAttemptRecord, FsTransferWorkerAttemptState, FsTransferWorkerLeaseRecord,
    TransferStateStore,
};

const TIKV_SCAN_PAGE_LIMIT: u32 = 1024;
const TRANSFER_STOP_BATCH_CLEANUP_LIMIT: usize = 256;
const TRANSFER_STOP_LEASE_CLEANUP_LIMIT: usize = 256;
const TRANSFER_CANCELLED_BY_USER_ERR: &str = "transfer job cancelled by user";
const KEY_NS_JOB: &[u8] = b"job";
const KEY_NS_BATCH: &[u8] = b"batch";
const KEY_NS_BATCH_EQ: &[u8] = b"batch_eq";
const KEY_NS_BATCH_STATE: &[u8] = b"batch_state";
const KEY_NS_COLLECT_INFO: &[u8] = b"collect_info";
const KEY_NS_FILE_ISSUE: &[u8] = b"file_issue";
const KEY_NS_WORKER_LEASE: &[u8] = b"worker_lease";
const KEY_NS_WORKER_ATTEMPT: &[u8] = b"worker_attempt";
const KEY_NS_COVERAGE_CLAIM: &[u8] = b"coverage_claim";
const KEY_NS_DIRECT_FILES_COMPLETE: &[u8] = b"direct_files_complete";

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum TransferCoveragePathKind {
    File,
    SymlinkNotice,
    EmptyDir,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct TransferCoverageClaimRecord {
    job_id: String,
    relpath: String,
    path_kind: TransferCoveragePathKind,
    batch_id: String,
    batch_root_relpath: String,
    batch_kind: FluxonFsTransferBatchKind,
}

struct FilteredClaimedBatchMaterialization {
    manifest: FluxonFsTransferManifestWire,
    collect_infos: Vec<FluxonFsTransferBatchCollectInfoWire>,
    claims: Vec<TransferCoverageClaimRecord>,
}

#[derive(Debug, Clone)]
struct ReconcileDoneBatchSnapshot {
    batch: FsTransferBatchRecord,
    manifest: FluxonFsTransferManifestWire,
    file_issue_relpaths: std::collections::BTreeSet<String>,
    collect_infos: Vec<FsTransferBatchCollectInfoRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconcileDoneBatchTargetState {
    Ready,
    Finished,
}

fn join_transfer_root_relpath(root_relpath: &str, relpath: &str) -> String {
    if root_relpath == "." {
        return relpath.to_string();
    }
    if relpath == "." {
        return root_relpath.to_string();
    }
    format!("{}/{}", root_relpath.trim_end_matches('/'), relpath)
}

fn transfer_worker_stop_reason_for_job_state(
    job_state: FluxonFsTransferJobState,
) -> FluxonFsTransferWorkerStopReasonWire {
    match job_state {
        FluxonFsTransferJobState::Stopping | FluxonFsTransferJobState::Cancelled => {
            FluxonFsTransferWorkerStopReasonWire::Cancelled
        }
        FluxonFsTransferJobState::Running
        | FluxonFsTransferJobState::Completed
        | FluxonFsTransferJobState::Failed => FluxonFsTransferWorkerStopReasonWire::Superseded,
    }
}

fn transfer_manifest_is_empty_dirs_only(
    manifest: &FluxonFsTransferManifestWire,
    has_collect_infos: bool,
) -> bool {
    manifest.entries.is_empty() && !has_collect_infos && !manifest.empty_dir_relpaths.is_empty()
}

fn transfer_batch_manifest_dispatch_class(
    manifest: &FluxonFsTransferManifestWire,
    has_collect_infos: bool,
) -> FsTransferReadyBatchClass {
    if transfer_manifest_is_empty_dirs_only(manifest, has_collect_infos) {
        FsTransferReadyBatchClass::EmptyDirsOnly
    } else {
        FsTransferReadyBatchClass::Payload
    }
}

fn decode_transfer_batch_manifest_dispatch_class(
    batch: &FsTransferBatchRecord,
    has_collect_infos: bool,
) -> Result<FsTransferReadyBatchClass, String> {
    let manifest = decode_transfer_manifest_blob(batch.manifest_blob.as_slice())?;
    Ok(transfer_batch_manifest_dispatch_class(
        &manifest,
        has_collect_infos,
    ))
}

// Thin handle to the durable transfer store actors. Worker control-plane RPCs
// stay on a dedicated fast lane so heavy scan/materialization commands do not
// queue ahead of launch acks and heartbeats.
pub(crate) struct TiKvTransferStateStore {
    slow_command_tx: Sender<StoreCommand>,
    fast_command_tx: Sender<StoreCommand>,
}

#[derive(Clone)]
pub(crate) struct TiKvTransferReconcileHandle {
    command_tx: Sender<ReconcileCommand>,
}

impl TiKvTransferStateStore {
    pub(crate) fn new(cfg: &FluxonFsTransferStateStoreTiKvConfig) -> Result<Self, String> {
        let pd_endpoints = cfg.pd_endpoints.clone();
        let key_prefix = cfg.key_prefix.clone().into_bytes();
        let (slow_command_tx, slow_command_rx) = unbounded();
        let (slow_init_tx, slow_init_rx) = bounded(1);
        thread::Builder::new()
            .name("fluxon_fs_transfer_tikv_store_slow".to_string())
            .spawn(move || {
                run_tikv_store_thread(slow_command_rx, slow_init_tx, pd_endpoints, key_prefix);
            })
            .map_err(|e| format!("spawn transfer tikv store thread failed: {}", e))?;
        slow_init_rx
            .recv()
            .map_err(|_| "transfer tikv store init channel closed".to_string())??;

        let pd_endpoints = cfg.pd_endpoints.clone();
        let key_prefix = cfg.key_prefix.clone().into_bytes();
        let (fast_command_tx, fast_command_rx) = unbounded();
        let (fast_init_tx, fast_init_rx) = bounded(1);
        thread::Builder::new()
            .name("fluxon_fs_transfer_tikv_store_fast".to_string())
            .spawn(move || {
                run_tikv_store_thread(fast_command_rx, fast_init_tx, pd_endpoints, key_prefix);
            })
            .map_err(|e| format!("spawn transfer tikv fast store thread failed: {}", e))?;
        fast_init_rx
            .recv()
            .map_err(|_| "transfer tikv fast store init channel closed".to_string())??;
        Ok(Self {
            slow_command_tx,
            fast_command_tx,
        })
    }

    fn send_slow_command<T>(
        &self,
        build: impl FnOnce(ResponseSender<T>) -> StoreCommand,
    ) -> Result<T, String> {
        let (resp_tx, resp_rx) = bounded(1);
        self.slow_command_tx
            .send(build(resp_tx))
            .map_err(|_| "transfer tikv store thread stopped".to_string())?;
        resp_rx
            .recv()
            .map_err(|_| "transfer tikv store response channel closed".to_string())?
    }

    fn send_fast_command<T>(
        &self,
        build: impl FnOnce(ResponseSender<T>) -> StoreCommand,
    ) -> Result<T, String> {
        let (resp_tx, resp_rx) = bounded(1);
        self.fast_command_tx
            .send(build(resp_tx))
            .map_err(|_| "transfer tikv fast store thread stopped".to_string())?;
        resp_rx
            .recv()
            .map_err(|_| "transfer tikv fast store response channel closed".to_string())?
    }
}

impl TiKvTransferReconcileHandle {
    pub(crate) fn new(cfg: &FluxonFsTransferStateStoreTiKvConfig) -> Result<Self, String> {
        let (command_tx, command_rx) = unbounded();
        let (init_tx, init_rx) = bounded(1);
        let pd_endpoints = cfg.pd_endpoints.clone();
        let key_prefix = cfg.key_prefix.clone().into_bytes();
        thread::Builder::new()
            .name("fluxon_fs_transfer_tikv_reconcile".to_string())
            .spawn(move || {
                run_tikv_reconcile_thread(command_rx, init_tx, pd_endpoints, key_prefix);
            })
            .map_err(|e| format!("spawn transfer tikv reconcile thread failed: {}", e))?;
        init_rx
            .recv()
            .map_err(|_| "transfer tikv reconcile init channel closed".to_string())??;
        Ok(Self { command_tx })
    }

    pub(crate) fn reconcile_transfer_scheduler_state_blocking(
        &self,
        backend: Arc<dyn FsS3Backend>,
        fs_cache: Arc<FluxonFsGlobalConfig>,
        now_unix_ms: i64,
    ) -> Result<(), String> {
        let (resp_tx, resp_rx) = bounded(1);
        self.command_tx
            .send(ReconcileCommand::Run {
                backend,
                fs_cache,
                now_unix_ms,
                resp: resp_tx,
            })
            .map_err(|_| "transfer tikv reconcile thread stopped".to_string())?;
        resp_rx
            .recv()
            .map_err(|_| "transfer tikv reconcile response channel closed".to_string())?
    }
}

impl TransferStateStore for TiKvTransferStateStore {
    fn insert_transfer_job(&self, job: &FsTransferJobRecord) -> Result<(), String> {
        self.send_slow_command(|resp| StoreCommand::InsertTransferJob {
            job: job.clone(),
            resp,
        })
    }

    fn cancel_transfer_job(&self, job_id: &str, now_unix_ms: i64) -> Result<(), String> {
        self.send_slow_command(|resp| StoreCommand::CancelTransferJob {
            job_id: job_id.to_string(),
            now_unix_ms,
            resp,
        })
    }

    fn update_transfer_job_desired_concurrency(
        &self,
        job_id: &str,
        desired_scan_concurrency: i64,
        desired_worker_count: i64,
    ) -> Result<(), String> {
        self.send_slow_command(|resp| StoreCommand::UpdateTransferJobDesiredConcurrency {
            job_id: job_id.to_string(),
            desired_scan_concurrency,
            desired_worker_count,
            resp,
        })
    }

    fn update_transfer_job_desired_worker_count(
        &self,
        job_id: &str,
        desired_worker_count: i64,
    ) -> Result<(), String> {
        self.send_slow_command(|resp| StoreCommand::UpdateTransferJobDesiredWorkerCount {
            job_id: job_id.to_string(),
            desired_worker_count,
            resp,
        })
    }

    fn import_transfer_prescan_job(
        &self,
        job_id: &str,
        src_export: &str,
        src_root_relpath: &str,
        dst_export: &str,
        dst_root_relpath: &str,
        desired_scan_concurrency: i64,
        desired_worker_count: i64,
    ) -> Result<FsTransferJobRecord, String> {
        self.send_slow_command(|resp| StoreCommand::ImportTransferPrescanJob {
            job_id: job_id.to_string(),
            src_export: src_export.to_string(),
            src_root_relpath: src_root_relpath.to_string(),
            dst_export: dst_export.to_string(),
            dst_root_relpath: dst_root_relpath.to_string(),
            desired_scan_concurrency,
            desired_worker_count,
            resp,
        })
    }

    fn load_transfer_job_record(
        &self,
        job_id: &str,
    ) -> Result<Option<FsTransferJobRecord>, String> {
        self.send_fast_command(|resp| StoreCommand::LoadTransferJobRecord {
            job_id: job_id.to_string(),
            resp,
        })
    }

    fn load_transfer_job_records(&self) -> Result<Vec<FsTransferJobRecord>, String> {
        self.send_fast_command(StoreCommand::LoadTransferJobRecords)
    }

    fn load_transfer_job_summary_snapshots(
        &self,
    ) -> Result<Vec<FsTransferJobSummarySnapshot>, String> {
        self.send_slow_command(StoreCommand::LoadTransferJobSummarySnapshots)
    }

    fn load_transfer_scheduler_job_snapshot(
        &self,
        job_id: &str,
    ) -> Result<Option<FsTransferSchedulerJobSnapshot>, String> {
        self.send_fast_command(|resp| StoreCommand::LoadTransferSchedulerJobSnapshot {
            job_id: job_id.to_string(),
            resp,
        })
    }

    fn load_transfer_job_snapshot(
        &self,
        job_id: &str,
    ) -> Result<Option<FsTransferJobSnapshot>, String> {
        self.send_slow_command(|resp| StoreCommand::LoadTransferJobSnapshot {
            job_id: job_id.to_string(),
            resp,
        })
    }

    fn load_transfer_batch_record(
        &self,
        job_id: &str,
        batch_id: &str,
    ) -> Result<Option<FsTransferBatchRecord>, String> {
        self.send_fast_command(|resp| StoreCommand::LoadTransferBatchRecord {
            job_id: job_id.to_string(),
            batch_id: batch_id.to_string(),
            resp,
        })
    }

    fn load_transfer_job_snapshots(&self) -> Result<Vec<FsTransferJobSnapshot>, String> {
        self.send_slow_command(StoreCommand::LoadTransferJobSnapshots)
    }

    fn load_transfer_batches(&self) -> Result<Vec<FsTransferBatchRecord>, String> {
        self.send_slow_command(StoreCommand::LoadTransferBatches)
    }

    fn load_transfer_batches_for_job(
        &self,
        job_id: &str,
    ) -> Result<Vec<FsTransferBatchRecord>, String> {
        self.send_slow_command(|resp| StoreCommand::LoadTransferBatchesForJob {
            job_id: job_id.to_string(),
            resp,
        })
    }

    fn load_transfer_direct_files_complete_records(
        &self,
    ) -> Result<Vec<FsTransferDirectFilesCompleteRecord>, String> {
        self.send_slow_command(StoreCommand::LoadTransferDirectFilesCompleteRecords)
    }

    fn load_transfer_direct_files_complete_records_for_job(
        &self,
        job_id: &str,
    ) -> Result<Vec<FsTransferDirectFilesCompleteRecord>, String> {
        self.send_slow_command(
            |resp| StoreCommand::LoadTransferDirectFilesCompleteRecordsForJob {
                job_id: job_id.to_string(),
                resp,
            },
        )
    }

    fn load_transfer_worker_attempt_records(
        &self,
    ) -> Result<Vec<FsTransferWorkerAttemptRecord>, String> {
        self.send_slow_command(StoreCommand::LoadTransferWorkerAttemptRecords)
    }

    fn load_transfer_batch_collect_info_records(
        &self,
    ) -> Result<Vec<FsTransferBatchCollectInfoRecord>, String> {
        self.send_slow_command(StoreCommand::LoadTransferBatchCollectInfoRecords)
    }

    fn load_transfer_batch_file_issue_records(
        &self,
    ) -> Result<Vec<FsTransferBatchFileIssueRecord>, String> {
        self.send_slow_command(StoreCommand::LoadTransferBatchFileIssueRecords)
    }

    fn load_next_ready_transfer_batch_for_job(
        &self,
        job_id: &str,
        batch_class: FsTransferReadyBatchClass,
    ) -> Result<Option<FsTransferReadyBatchDispatch>, String> {
        self.send_fast_command(|resp| StoreCommand::LoadNextReadyTransferBatchForJob {
            job_id: job_id.to_string(),
            batch_class,
            resp,
        })
    }

    fn load_ready_transfer_batches_for_job(
        &self,
        job_id: &str,
    ) -> Result<Vec<FsTransferBatchRecord>, String> {
        self.send_fast_command(|resp| StoreCommand::LoadReadyTransferBatchesForJob {
            job_id: job_id.to_string(),
            resp,
        })
    }

    fn begin_transfer_scan_epoch(&self, job_id: &str) -> Result<i64, String> {
        self.send_slow_command(|resp| StoreCommand::BeginTransferScanEpoch {
            job_id: job_id.to_string(),
            resp,
        })
    }

    fn finish_transfer_scan_epoch(&self, job_id: &str, scan_epoch: i64) -> Result<(), String> {
        self.send_slow_command(|resp| StoreCommand::FinishTransferScanEpoch {
            job_id: job_id.to_string(),
            scan_epoch,
            resp,
        })
    }

    fn apply_transfer_scan_append(
        &self,
        result: &FluxonFsTransferScanResultWire,
    ) -> Result<(), String> {
        self.send_slow_command(|resp| StoreCommand::ApplyTransferScanAppend {
            result: result.clone(),
            resp,
        })
    }

    fn finish_transfer_scan_unit(
        &self,
        result: &FluxonFsTransferScanResultWire,
    ) -> Result<(), String> {
        self.send_slow_command(|resp| StoreCommand::FinishTransferScanUnit {
            result: result.clone(),
            resp,
        })
    }

    fn apply_transfer_scan_result(
        &self,
        result: &FluxonFsTransferScanResultWire,
    ) -> Result<(), String> {
        self.send_slow_command(|resp| StoreCommand::ApplyTransferScanResult {
            result: result.clone(),
            resp,
        })
    }

    fn assign_transfer_batch_to_worker(
        &self,
        job_id: &str,
        batch_id: &str,
        src_exporter_id: &str,
        worker_id: &str,
        worker_task_id: &str,
        dst_exporter_id: &str,
        lease_expire_unix_ms: i64,
    ) -> Result<(), String> {
        self.send_fast_command(|resp| StoreCommand::AssignTransferBatchToWorker {
            job_id: job_id.to_string(),
            batch_id: batch_id.to_string(),
            src_exporter_id: src_exporter_id.to_string(),
            worker_id: worker_id.to_string(),
            worker_task_id: worker_task_id.to_string(),
            dst_exporter_id: dst_exporter_id.to_string(),
            lease_expire_unix_ms,
            resp,
        })
    }

    fn record_transfer_worker_launch_retry(
        &self,
        job_id: &str,
        batch_id: &str,
        worker_id: &str,
        worker_task_id: &str,
        err_text: &str,
        now_unix_ms: i64,
    ) -> Result<(), String> {
        self.send_fast_command(|resp| StoreCommand::RecordTransferWorkerLaunchRetry {
            job_id: job_id.to_string(),
            batch_id: batch_id.to_string(),
            worker_id: worker_id.to_string(),
            worker_task_id: worker_task_id.to_string(),
            err_text: err_text.to_string(),
            now_unix_ms,
            resp,
        })
    }

    fn mark_transfer_worker_launch_acknowledged(
        &self,
        job_id: &str,
        batch_id: &str,
        worker_id: &str,
        worker_task_id: &str,
        now_unix_ms: i64,
    ) -> Result<(), String> {
        self.send_fast_command(|resp| StoreCommand::MarkTransferWorkerLaunchAcknowledged {
            job_id: job_id.to_string(),
            batch_id: batch_id.to_string(),
            worker_id: worker_id.to_string(),
            worker_task_id: worker_task_id.to_string(),
            now_unix_ms,
            resp,
        })
    }

    fn mark_transfer_worker_attempt_stopped(
        &self,
        job_id: &str,
        batch_id: &str,
        worker_id: &str,
        worker_task_id: &str,
        stop_reason: Option<FluxonFsTransferWorkerStopReasonWire>,
        err_text: &str,
        now_unix_ms: i64,
    ) -> Result<(), String> {
        self.send_fast_command(|resp| StoreCommand::MarkTransferWorkerAttemptStopped {
            job_id: job_id.to_string(),
            batch_id: batch_id.to_string(),
            worker_id: worker_id.to_string(),
            worker_task_id: worker_task_id.to_string(),
            stop_reason,
            err_text: err_text.to_string(),
            now_unix_ms,
            resp,
        })
    }

    fn apply_transfer_worker_result(
        &self,
        result: &FluxonFsTransferWorkerResultWire,
    ) -> Result<FluxonFsTransferWorkerResultAckWire, String> {
        self.send_fast_command(|resp| StoreCommand::ApplyTransferWorkerResult {
            result: result.clone(),
            resp,
        })
    }

    fn apply_transfer_worker_heartbeat(
        &self,
        heartbeat: &FluxonFsTransferWorkerHeartbeatWire,
        heartbeat_received_unix_ms: i64,
        heartbeat_lease_duration_ms: i64,
    ) -> Result<FluxonFsTransferWorkerHeartbeatResultWire, String> {
        self.send_fast_command(|resp| StoreCommand::ApplyTransferWorkerHeartbeat {
            heartbeat: heartbeat.clone(),
            heartbeat_received_unix_ms,
            heartbeat_lease_duration_ms,
            resp,
        })
    }
}

type ResponseSender<T> = Sender<Result<T, String>>;
type ReconcileResponseSender<T> = Sender<Result<T, String>>;

enum ReconcileCommand {
    Run {
        backend: Arc<dyn FsS3Backend>,
        fs_cache: Arc<FluxonFsGlobalConfig>,
        now_unix_ms: i64,
        resp: ReconcileResponseSender<()>,
    },
}

// Commands mirror TransferStateStore methods. Fast and slow lanes both funnel
// into TiKV transactions; row locks, not queue ordering, remain the durable
// correctness boundary across the lanes.
enum StoreCommand {
    InsertTransferJob {
        job: FsTransferJobRecord,
        resp: ResponseSender<()>,
    },
    CancelTransferJob {
        job_id: String,
        now_unix_ms: i64,
        resp: ResponseSender<()>,
    },
    UpdateTransferJobDesiredConcurrency {
        job_id: String,
        desired_scan_concurrency: i64,
        desired_worker_count: i64,
        resp: ResponseSender<()>,
    },
    UpdateTransferJobDesiredWorkerCount {
        job_id: String,
        desired_worker_count: i64,
        resp: ResponseSender<()>,
    },
    ImportTransferPrescanJob {
        job_id: String,
        src_export: String,
        src_root_relpath: String,
        dst_export: String,
        dst_root_relpath: String,
        desired_scan_concurrency: i64,
        desired_worker_count: i64,
        resp: ResponseSender<FsTransferJobRecord>,
    },
    LoadTransferJobRecord {
        job_id: String,
        resp: ResponseSender<Option<FsTransferJobRecord>>,
    },
    LoadTransferJobRecords(ResponseSender<Vec<FsTransferJobRecord>>),
    LoadTransferJobSummarySnapshots(ResponseSender<Vec<FsTransferJobSummarySnapshot>>),
    LoadTransferSchedulerJobSnapshot {
        job_id: String,
        resp: ResponseSender<Option<FsTransferSchedulerJobSnapshot>>,
    },
    LoadTransferJobSnapshot {
        job_id: String,
        resp: ResponseSender<Option<FsTransferJobSnapshot>>,
    },
    LoadTransferBatchRecord {
        job_id: String,
        batch_id: String,
        resp: ResponseSender<Option<FsTransferBatchRecord>>,
    },
    LoadTransferJobSnapshots(ResponseSender<Vec<FsTransferJobSnapshot>>),
    LoadTransferBatches(ResponseSender<Vec<FsTransferBatchRecord>>),
    LoadTransferBatchesForJob {
        job_id: String,
        resp: ResponseSender<Vec<FsTransferBatchRecord>>,
    },
    LoadTransferDirectFilesCompleteRecords(
        ResponseSender<Vec<FsTransferDirectFilesCompleteRecord>>,
    ),
    LoadTransferDirectFilesCompleteRecordsForJob {
        job_id: String,
        resp: ResponseSender<Vec<FsTransferDirectFilesCompleteRecord>>,
    },
    LoadTransferWorkerAttemptRecords(ResponseSender<Vec<FsTransferWorkerAttemptRecord>>),
    LoadTransferBatchCollectInfoRecords(ResponseSender<Vec<FsTransferBatchCollectInfoRecord>>),
    LoadTransferBatchFileIssueRecords(ResponseSender<Vec<FsTransferBatchFileIssueRecord>>),
    LoadNextReadyTransferBatchForJob {
        job_id: String,
        batch_class: FsTransferReadyBatchClass,
        resp: ResponseSender<Option<FsTransferReadyBatchDispatch>>,
    },
    LoadReadyTransferBatchesForJob {
        job_id: String,
        resp: ResponseSender<Vec<FsTransferBatchRecord>>,
    },
    BeginTransferScanEpoch {
        job_id: String,
        resp: ResponseSender<i64>,
    },
    FinishTransferScanEpoch {
        job_id: String,
        scan_epoch: i64,
        resp: ResponseSender<()>,
    },
    ApplyTransferScanAppend {
        result: FluxonFsTransferScanResultWire,
        resp: ResponseSender<()>,
    },
    FinishTransferScanUnit {
        result: FluxonFsTransferScanResultWire,
        resp: ResponseSender<()>,
    },
    ApplyTransferScanResult {
        result: FluxonFsTransferScanResultWire,
        resp: ResponseSender<()>,
    },
    AssignTransferBatchToWorker {
        job_id: String,
        batch_id: String,
        src_exporter_id: String,
        worker_id: String,
        worker_task_id: String,
        dst_exporter_id: String,
        lease_expire_unix_ms: i64,
        resp: ResponseSender<()>,
    },
    RecordTransferWorkerLaunchRetry {
        job_id: String,
        batch_id: String,
        worker_id: String,
        worker_task_id: String,
        err_text: String,
        now_unix_ms: i64,
        resp: ResponseSender<()>,
    },
    MarkTransferWorkerLaunchAcknowledged {
        job_id: String,
        batch_id: String,
        worker_id: String,
        worker_task_id: String,
        now_unix_ms: i64,
        resp: ResponseSender<()>,
    },
    MarkTransferWorkerAttemptStopped {
        job_id: String,
        batch_id: String,
        worker_id: String,
        worker_task_id: String,
        stop_reason: Option<FluxonFsTransferWorkerStopReasonWire>,
        err_text: String,
        now_unix_ms: i64,
        resp: ResponseSender<()>,
    },
    ApplyTransferWorkerResult {
        result: FluxonFsTransferWorkerResultWire,
        resp: ResponseSender<FluxonFsTransferWorkerResultAckWire>,
    },
    ApplyTransferWorkerHeartbeat {
        heartbeat: FluxonFsTransferWorkerHeartbeatWire,
        heartbeat_received_unix_ms: i64,
        heartbeat_lease_duration_ms: i64,
        resp: ResponseSender<FluxonFsTransferWorkerHeartbeatResultWire>,
    },
}

fn execute_store_command(
    runtime: &tokio::runtime::Runtime,
    core: &TiKvTransferStateStoreCore,
    command: StoreCommand,
) {
    match command {
        StoreCommand::InsertTransferJob { job, resp } => {
            let _ = resp.send(runtime.block_on(core.insert_transfer_job(job)));
        }
        StoreCommand::CancelTransferJob {
            job_id,
            now_unix_ms,
            resp,
        } => {
            let _ =
                resp.send(runtime.block_on(core.cancel_transfer_job(job_id.as_str(), now_unix_ms)));
        }
        StoreCommand::UpdateTransferJobDesiredConcurrency {
            job_id,
            desired_scan_concurrency,
            desired_worker_count,
            resp,
        } => {
            let _ = resp.send(
                runtime.block_on(core.update_transfer_job_desired_concurrency(
                    job_id.as_str(),
                    desired_scan_concurrency,
                    desired_worker_count,
                )),
            );
        }
        StoreCommand::UpdateTransferJobDesiredWorkerCount {
            job_id,
            desired_worker_count,
            resp,
        } => {
            let _ = resp.send(runtime.block_on(
                core.update_transfer_job_desired_worker_count(
                    job_id.as_str(),
                    desired_worker_count,
                ),
            ));
        }
        StoreCommand::ImportTransferPrescanJob {
            job_id,
            src_export,
            src_root_relpath,
            dst_export,
            dst_root_relpath,
            desired_scan_concurrency,
            desired_worker_count,
            resp,
        } => {
            let _ = resp.send(runtime.block_on(core.import_transfer_prescan_job(
                job_id.as_str(),
                src_export.as_str(),
                src_root_relpath.as_str(),
                dst_export.as_str(),
                dst_root_relpath.as_str(),
                desired_scan_concurrency,
                desired_worker_count,
            )));
        }
        StoreCommand::LoadTransferJobRecord { job_id, resp } => {
            let _ = resp.send(runtime.block_on(core.load_transfer_job_record(job_id.as_str())));
        }
        StoreCommand::LoadTransferJobRecords(resp) => {
            let _ = resp.send(runtime.block_on(core.load_transfer_job_records()));
        }
        StoreCommand::LoadTransferJobSummarySnapshots(resp) => {
            let _ = resp.send(runtime.block_on(core.load_transfer_job_summary_snapshots()));
        }
        StoreCommand::LoadTransferSchedulerJobSnapshot { job_id, resp } => {
            let _ = resp
                .send(runtime.block_on(core.load_transfer_scheduler_job_snapshot(job_id.as_str())));
        }
        StoreCommand::LoadTransferJobSnapshot { job_id, resp } => {
            let _ = resp.send(runtime.block_on(core.load_transfer_job_snapshot(job_id.as_str())));
        }
        StoreCommand::LoadTransferBatchRecord {
            job_id,
            batch_id,
            resp,
        } => {
            let _ = resp.send(
                runtime
                    .block_on(core.load_transfer_batch_record(job_id.as_str(), batch_id.as_str())),
            );
        }
        StoreCommand::LoadTransferJobSnapshots(resp) => {
            let _ = resp.send(runtime.block_on(core.load_transfer_job_snapshots()));
        }
        StoreCommand::LoadTransferBatches(resp) => {
            let _ = resp.send(runtime.block_on(core.load_transfer_batches()));
        }
        StoreCommand::LoadTransferBatchesForJob { job_id, resp } => {
            let _ =
                resp.send(runtime.block_on(core.load_transfer_batches_for_job(job_id.as_str())));
        }
        StoreCommand::LoadTransferDirectFilesCompleteRecords(resp) => {
            let _ = resp.send(runtime.block_on(core.load_transfer_direct_files_complete_records()));
        }
        StoreCommand::LoadTransferDirectFilesCompleteRecordsForJob { job_id, resp } => {
            let _ = resp.send(runtime.block_on(
                core.load_transfer_direct_files_complete_records_for_job(job_id.as_str()),
            ));
        }
        StoreCommand::LoadTransferWorkerAttemptRecords(resp) => {
            let _ = resp.send(runtime.block_on(core.load_transfer_worker_attempt_records()));
        }
        StoreCommand::LoadTransferBatchCollectInfoRecords(resp) => {
            let _ = resp.send(runtime.block_on(core.load_transfer_batch_collect_info_records()));
        }
        StoreCommand::LoadTransferBatchFileIssueRecords(resp) => {
            let _ = resp.send(runtime.block_on(core.load_transfer_batch_file_issue_records()));
        }
        StoreCommand::LoadNextReadyTransferBatchForJob {
            job_id,
            batch_class,
            resp,
        } => {
            let _ = resp.send(runtime.block_on(
                core.load_next_ready_transfer_batch_for_job(job_id.as_str(), batch_class),
            ));
        }
        StoreCommand::LoadReadyTransferBatchesForJob { job_id, resp } => {
            let _ = resp
                .send(runtime.block_on(core.load_ready_transfer_batches_for_job(job_id.as_str())));
        }
        StoreCommand::BeginTransferScanEpoch { job_id, resp } => {
            let _ = resp.send(runtime.block_on(core.begin_transfer_scan_epoch(job_id.as_str())));
        }
        StoreCommand::FinishTransferScanEpoch {
            job_id,
            scan_epoch,
            resp,
        } => {
            let _ = resp.send(
                runtime.block_on(core.finish_transfer_scan_epoch(job_id.as_str(), scan_epoch)),
            );
        }
        StoreCommand::ApplyTransferScanAppend { result, resp } => {
            let _ = resp.send(runtime.block_on(core.apply_transfer_scan_append(&result)));
        }
        StoreCommand::FinishTransferScanUnit { result, resp } => {
            let _ = resp.send(runtime.block_on(core.finish_transfer_scan_unit(&result)));
        }
        StoreCommand::ApplyTransferScanResult { result, resp } => {
            let _ = resp.send(runtime.block_on(core.apply_transfer_scan_result(&result)));
        }
        StoreCommand::AssignTransferBatchToWorker {
            job_id,
            batch_id,
            src_exporter_id,
            worker_id,
            worker_task_id,
            dst_exporter_id,
            lease_expire_unix_ms,
            resp,
        } => {
            let _ = resp.send(runtime.block_on(core.assign_transfer_batch_to_worker(
                job_id.as_str(),
                batch_id.as_str(),
                src_exporter_id.as_str(),
                worker_id.as_str(),
                worker_task_id.as_str(),
                dst_exporter_id.as_str(),
                lease_expire_unix_ms,
            )));
        }
        StoreCommand::RecordTransferWorkerLaunchRetry {
            job_id,
            batch_id,
            worker_id,
            worker_task_id,
            err_text,
            now_unix_ms,
            resp,
        } => {
            let _ = resp.send(runtime.block_on(core.record_transfer_worker_launch_retry(
                job_id.as_str(),
                batch_id.as_str(),
                worker_id.as_str(),
                worker_task_id.as_str(),
                err_text.as_str(),
                now_unix_ms,
            )));
        }
        StoreCommand::MarkTransferWorkerLaunchAcknowledged {
            job_id,
            batch_id,
            worker_id,
            worker_task_id,
            now_unix_ms,
            resp,
        } => {
            let _ = resp.send(
                runtime.block_on(core.mark_transfer_worker_launch_acknowledged(
                    job_id.as_str(),
                    batch_id.as_str(),
                    worker_id.as_str(),
                    worker_task_id.as_str(),
                    now_unix_ms,
                )),
            );
        }
        StoreCommand::MarkTransferWorkerAttemptStopped {
            job_id,
            batch_id,
            worker_id,
            worker_task_id,
            stop_reason,
            err_text,
            now_unix_ms,
            resp,
        } => {
            let _ = resp.send(runtime.block_on(core.mark_transfer_worker_attempt_stopped(
                job_id.as_str(),
                batch_id.as_str(),
                worker_id.as_str(),
                worker_task_id.as_str(),
                stop_reason,
                err_text.as_str(),
                now_unix_ms,
            )));
        }
        StoreCommand::ApplyTransferWorkerResult { result, resp } => {
            let _ = resp.send(runtime.block_on(core.apply_transfer_worker_result(&result)));
        }
        StoreCommand::ApplyTransferWorkerHeartbeat {
            heartbeat,
            heartbeat_received_unix_ms,
            heartbeat_lease_duration_ms,
            resp,
        } => {
            let _ = resp.send(runtime.block_on(core.apply_transfer_worker_heartbeat(
                &heartbeat,
                heartbeat_received_unix_ms,
                heartbeat_lease_duration_ms,
            )));
        }
    }
}

fn run_tikv_store_thread(
    command_rx: Receiver<StoreCommand>,
    init_tx: Sender<Result<(), String>>,
    pd_endpoints: Vec<String>,
    key_prefix: Vec<u8>,
) {
    // This thread owns one store runtime and executes one command lane
    // sequentially. Multiple lanes use independent TiKV clients and rely on
    // durable row locks for cross-lane correctness.
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(v) => v,
        Err(e) => {
            let _ = init_tx.send(Err(format!("create transfer tikv runtime failed: {}", e)));
            return;
        }
    };
    let core = match runtime.block_on(TiKvTransferStateStoreCore::new(pd_endpoints, key_prefix)) {
        Ok(v) => v,
        Err(e) => {
            let _ = init_tx.send(Err(e));
            return;
        }
    };
    if init_tx.send(Ok(())).is_err() {
        return;
    }
    while let Ok(command) = command_rx.recv() {
        execute_store_command(&runtime, &core, command);
    }
}

fn run_tikv_reconcile_thread(
    command_rx: Receiver<ReconcileCommand>,
    init_tx: Sender<Result<(), String>>,
    pd_endpoints: Vec<String>,
    key_prefix: Vec<u8>,
) {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(v) => v,
        Err(e) => {
            let _ = init_tx.send(Err(format!(
                "create transfer tikv reconcile runtime failed: {}",
                e
            )));
            return;
        }
    };
    let core = match runtime.block_on(TiKvTransferStateStoreCore::new(pd_endpoints, key_prefix)) {
        Ok(v) => v,
        Err(e) => {
            let _ = init_tx.send(Err(e));
            return;
        }
    };
    if init_tx.send(Ok(())).is_err() {
        return;
    }
    while let Ok(command) = command_rx.recv() {
        match command {
            ReconcileCommand::Run {
                backend,
                fs_cache,
                now_unix_ms,
                resp,
            } => {
                let _ = resp.send(runtime.block_on(core.reconcile_transfer_scheduler_state(
                    backend,
                    fs_cache,
                    now_unix_ms,
                )));
            }
        }
    }
}

// Core implementation of the durable transfer state machine on top of TiKV
// transactions.
struct TiKvTransferStateStoreCore {
    client: TransactionClient,
    key_prefix: Vec<u8>,
}

// Ownership is represented by a pair. Both ids must be empty together or filled
// together; any mixed state is considered corrupt and will be requeued.
fn transfer_batch_owner_is_empty(batch: &FsTransferBatchRecord) -> bool {
    batch.owner_worker_id.trim().is_empty() && batch.owner_worker_task_id.trim().is_empty()
}

fn transfer_batch_owner_is_consistent(batch: &FsTransferBatchRecord) -> bool {
    batch.owner_worker_id.trim().is_empty() == batch.owner_worker_task_id.trim().is_empty()
}

macro_rules! tx_try {
    ($tx:ident, $expr:expr) => {
        match $expr {
            Ok(value) => value,
            Err(err) => return Err(rollback_with_error(&mut $tx, err).await),
        }
    };
}

impl TiKvTransferStateStoreCore {
    fn transfer_job_open_batch_count(job: &FsTransferJobRecord) -> i64 {
        job.ready_batch_count
            .saturating_add(job.running_batch_count)
            .saturating_add(job.done_batch_count)
            .saturating_add(job.expired_batch_count)
    }

    fn transfer_job_pending_batch_count(job: &FsTransferJobRecord) -> i64 {
        job.ready_batch_count
            .saturating_add(job.expired_batch_count)
    }

    fn transfer_job_done_batch_count(job: &FsTransferJobRecord) -> i64 {
        job.done_batch_count
            .saturating_add(job.finished_batch_count)
    }

    fn adjust_job_batch_state_count(
        job: &mut FsTransferJobRecord,
        state: FluxonFsTransferBatchState,
        delta: i64,
    ) {
        match state {
            FluxonFsTransferBatchState::Ready => {
                job.ready_batch_count = job.ready_batch_count.saturating_add(delta);
            }
            FluxonFsTransferBatchState::Running => {
                job.running_batch_count = job.running_batch_count.saturating_add(delta);
            }
            FluxonFsTransferBatchState::Done => {
                job.done_batch_count = job.done_batch_count.saturating_add(delta);
            }
            FluxonFsTransferBatchState::Finished => {
                job.finished_batch_count = job.finished_batch_count.saturating_add(delta);
            }
            FluxonFsTransferBatchState::Expired => {
                job.expired_batch_count = job.expired_batch_count.saturating_add(delta);
            }
            FluxonFsTransferBatchState::Cancelled => {}
        }
    }

    async fn new(pd_endpoints: Vec<String>, key_prefix: Vec<u8>) -> Result<Self, String> {
        if pd_endpoints.is_empty() {
            return Err("transfer tikv pd_endpoints must be non-empty".to_string());
        }
        if key_prefix.is_empty() {
            return Err("transfer tikv key_prefix must be non-empty".to_string());
        }
        let client = TransactionClient::new(pd_endpoints)
            .await
            .map_err(|e| format!("connect transfer tikv client failed: {}", e))?;
        Ok(Self { client, key_prefix })
    }

    async fn begin_mutation_tx(&self, context: &str) -> Result<Transaction, String> {
        self.client
            .begin_pessimistic()
            .await
            .map_err(|e| format!("begin {} transaction failed: {}", context, e))
    }

    async fn insert_transfer_job(&self, job: FsTransferJobRecord) -> Result<(), String> {
        let mut tx = self
            .client
            .begin_pessimistic()
            .await
            .map_err(|e| format!("begin insert transfer job transaction failed: {}", e))?;
        let job_key = self.job_key(job.job_id.as_str());
        let existing = tx_try!(
            tx,
            tx.get(job_key.clone())
                .await
                .map_err(|e| format!("read transfer job before insert failed: {}", e))
        );
        if existing.is_some() {
            let err = format!("transfer job already exists: job_id={}", job.job_id);
            return Err(rollback_with_error(&mut tx, err).await);
        }
        tx_try!(
            tx,
            put_record(&mut tx, job_key, &job, "transfer job insert").await
        );
        tx.commit()
            .await
            .map_err(|e| format!("commit insert transfer job failed: {}", e))?;
        Ok(())
    }

    async fn cancel_transfer_job(&self, job_id: &str, now_unix_ms: i64) -> Result<(), String> {
        let (mut tx, mut job) = self.begin_locked_job_tx(job_id).await?;
        if job.state == FluxonFsTransferJobState::Stopping
            || job.state == FluxonFsTransferJobState::Cancelled
        {
            let _ = tx.rollback().await;
            return Ok(());
        }
        if job.state != FluxonFsTransferJobState::Running {
            let err = format!("no running transfer job found: job_id={}", job_id);
            return Err(rollback_with_error(&mut tx, err).await);
        }
        job.state = FluxonFsTransferJobState::Stopping;
        job.scan_finished = true;
        job.desired_worker_count = 0;
        job.last_error = TRANSFER_CANCELLED_BY_USER_ERR.to_string();
        job.updated_at_unix_ms = now_unix_ms;
        tx_try!(
            tx,
            put_record(
                &mut tx,
                self.job_key(job_id),
                &job,
                "mark transfer job stopping",
            )
            .await
        );
        tx.commit()
            .await
            .map_err(|e| format!("commit cancel transfer job job_id={} failed: {}", job_id, e))?;
        Ok(())
    }

    async fn update_transfer_job_desired_worker_count(
        &self,
        job_id: &str,
        desired_worker_count: i64,
    ) -> Result<(), String> {
        let (mut tx, mut job) = self.begin_locked_job_tx(job_id).await?;
        if job.state != FluxonFsTransferJobState::Running {
            let err = format!("no running transfer job found: job_id={}", job_id);
            return Err(rollback_with_error(&mut tx, err).await);
        }
        job.desired_worker_count = desired_worker_count;
        job.last_error.clear();
        job.updated_at_unix_ms = chrono::Utc::now().timestamp_millis();
        tx_try!(
            tx,
            put_record(
                &mut tx,
                self.job_key(job_id),
                &job,
                "update transfer job desired_worker_count",
            )
            .await
        );
        tx.commit().await.map_err(|e| {
            format!(
                "commit update transfer job desired_worker_count job_id={} failed: {}",
                job_id, e
            )
        })?;
        Ok(())
    }

    async fn update_transfer_job_desired_concurrency(
        &self,
        job_id: &str,
        desired_scan_concurrency: i64,
        desired_worker_count: i64,
    ) -> Result<(), String> {
        let (mut tx, mut job) = self.begin_locked_job_tx(job_id).await?;
        if job.state != FluxonFsTransferJobState::Running {
            let err = format!("no running transfer job found: job_id={}", job_id);
            return Err(rollback_with_error(&mut tx, err).await);
        }
        job.desired_scan_concurrency = desired_scan_concurrency;
        job.desired_worker_count = desired_worker_count;
        job.last_error.clear();
        job.updated_at_unix_ms = chrono::Utc::now().timestamp_millis();
        tx_try!(
            tx,
            put_record(
                &mut tx,
                self.job_key(job_id),
                &job,
                "update transfer job desired concurrency",
            )
            .await
        );
        tx.commit().await.map_err(|e| {
            format!(
                "commit update transfer job desired concurrency job_id={} failed: {}",
                job_id, e
            )
        })?;
        Ok(())
    }

    async fn import_transfer_prescan_job(
        &self,
        job_id: &str,
        src_export: &str,
        src_root_relpath: &str,
        dst_export: &str,
        dst_root_relpath: &str,
        desired_scan_concurrency: i64,
        desired_worker_count: i64,
    ) -> Result<FsTransferJobRecord, String> {
        let (mut tx, mut job) = self.begin_locked_job_tx(job_id).await?;
        if job.state != FluxonFsTransferJobState::Running {
            let err = format!("no running transfer job found: job_id={}", job_id);
            return Err(rollback_with_error(&mut tx, err).await);
        }
        if job.src_export != FLUXON_FS_LOCAL_TRANSFER_CHECK_SRC_EXPORT
            || job.dst_export != FLUXON_FS_LOCAL_TRANSFER_CHECK_DST_EXPORT
        {
            let err = format!("transfer job is not a local prescan job: job_id={}", job_id);
            return Err(rollback_with_error(&mut tx, err).await);
        }
        job.src_export = src_export.trim().to_string();
        job.src_root_relpath = normalize_transfer_root_relpath(src_root_relpath)?;
        job.dst_export = dst_export.trim().to_string();
        job.dst_root_relpath = normalize_transfer_root_relpath(dst_root_relpath)?;
        job.desired_scan_concurrency = desired_scan_concurrency;
        job.desired_worker_count = desired_worker_count;
        job.last_error.clear();
        job.updated_at_unix_ms = chrono::Utc::now().timestamp_millis();
        tx_try!(
            tx,
            put_record(
                &mut tx,
                self.job_key(job_id),
                &job,
                "import transfer prescan job",
            )
            .await
        );
        tx.commit().await.map_err(|e| {
            format!(
                "commit import transfer prescan job job_id={} failed: {}",
                job_id, e
            )
        })?;
        Ok(job)
    }

    async fn load_transfer_job_records(&self) -> Result<Vec<FsTransferJobRecord>, String> {
        let mut tx = self
            .client
            .begin_optimistic()
            .await
            .map_err(|e| format!("begin load transfer job transaction failed: {}", e))?;
        let mut jobs = match self.load_jobs_from_tx(&mut tx).await {
            Ok(v) => v,
            Err(err) => return Err(rollback_with_error(&mut tx, err).await),
        };
        jobs.sort_by(|a, b| {
            a.created_at_unix_ms
                .cmp(&b.created_at_unix_ms)
                .then(a.job_id.cmp(&b.job_id))
        });
        let _ = tx.rollback().await;
        Ok(jobs)
    }

    async fn load_transfer_job_record(
        &self,
        job_id: &str,
    ) -> Result<Option<FsTransferJobRecord>, String> {
        let mut tx = self
            .client
            .begin_optimistic()
            .await
            .map_err(|e| format!("begin load transfer job record transaction failed: {}", e))?;
        let job = match get_record::<FsTransferJobRecord>(
            &mut tx,
            self.job_key(job_id),
            "transfer job record lookup",
        )
        .await
        {
            Ok(v) => v,
            Err(err) => return Err(rollback_with_error(&mut tx, err).await),
        };
        let _ = tx.rollback().await;
        Ok(job)
    }

    async fn load_transfer_batch_record(
        &self,
        job_id: &str,
        batch_id: &str,
    ) -> Result<Option<FsTransferBatchRecord>, String> {
        let mut tx =
            self.client.begin_optimistic().await.map_err(|e| {
                format!("begin load transfer batch record transaction failed: {}", e)
            })?;
        let batch = match get_record::<FsTransferBatchRecord>(
            &mut tx,
            self.batch_key(job_id, batch_id),
            "transfer batch record lookup",
        )
        .await
        {
            Ok(v) => v,
            Err(err) => return Err(rollback_with_error(&mut tx, err).await),
        };
        let _ = tx.rollback().await;
        Ok(batch)
    }

    async fn load_transfer_job_summary_snapshots(
        &self,
    ) -> Result<Vec<FsTransferJobSummarySnapshot>, String> {
        let mut tx =
            self.client.begin_optimistic().await.map_err(|e| {
                format!("begin load transfer job summary transaction failed: {}", e)
            })?;
        let mut jobs = match self.load_jobs_from_tx(&mut tx).await {
            Ok(v) => v,
            Err(err) => return Err(rollback_with_error(&mut tx, err).await),
        };
        jobs.sort_by(|a, b| {
            a.created_at_unix_ms
                .cmp(&b.created_at_unix_ms)
                .then(a.job_id.cmp(&b.job_id))
        });
        let mut out = Vec::with_capacity(jobs.len());
        for job in jobs {
            let running_batch_owners = match self
                .load_running_batch_owner_snapshots_for_job_from_tx(&mut tx, job.job_id.as_str())
                .await
            {
                Ok(v) => v,
                Err(err) => return Err(rollback_with_error(&mut tx, err).await),
            };
            let open_batches = Self::transfer_job_open_batch_count(&job);
            let pending_batches = Self::transfer_job_pending_batch_count(&job);
            out.push(FsTransferJobSummarySnapshot {
                scan_epoch: job.scan_epoch,
                scan_finished: job.scan_finished,
                open_batches,
                pending_batches,
                done_batches: Self::transfer_job_done_batch_count(&job),
                failed_file_count: job.failed_file_count,
                job,
                running_batch_owners,
                live_detail: None,
            });
        }
        let _ = tx.rollback().await;
        Ok(out)
    }

    async fn load_transfer_scheduler_job_snapshot(
        &self,
        job_id: &str,
    ) -> Result<Option<FsTransferSchedulerJobSnapshot>, String> {
        let mut tx = self.client.begin_optimistic().await.map_err(|e| {
            format!(
                "begin load transfer scheduler job snapshot transaction failed: {}",
                e
            )
        })?;
        let job = match get_record::<FsTransferJobRecord>(
            &mut tx,
            self.job_key(job_id),
            "transfer scheduler job snapshot job lookup",
        )
        .await
        {
            Ok(v) => v,
            Err(err) => return Err(rollback_with_error(&mut tx, err).await),
        };
        let mut payload_running_batch_count = 0_i64;
        let mut empty_dir_only_running_batch_count = 0_i64;
        if let Some(job_ref) = &job {
            if job_ref.running_batch_count > 0 {
                let running_batches = match self
                    .load_batches_in_state_for_job_from_tx(
                        &mut tx,
                        job_id,
                        FluxonFsTransferBatchState::Running,
                    )
                    .await
                {
                    Ok(v) => v,
                    Err(err) => return Err(rollback_with_error(&mut tx, err).await),
                };
                for batch in running_batches {
                    match decode_transfer_batch_manifest_dispatch_class(&batch, false) {
                        Ok(FsTransferReadyBatchClass::Payload) => {
                            payload_running_batch_count += 1;
                        }
                        Ok(FsTransferReadyBatchClass::EmptyDirsOnly) => {
                            empty_dir_only_running_batch_count += 1;
                        }
                        Err(err) => return Err(rollback_with_error(&mut tx, err).await),
                    }
                }
            }
        }
        let _ = tx.rollback().await;
        Ok(job.map(|job| FsTransferSchedulerJobSnapshot {
            scan_epoch: job.scan_epoch,
            scan_finished: job.scan_finished,
            running_batch_count: job.running_batch_count,
            payload_running_batch_count,
            empty_dir_only_running_batch_count,
            job,
        }))
    }

    async fn load_transfer_job_snapshot(
        &self,
        job_id: &str,
    ) -> Result<Option<FsTransferJobSnapshot>, String> {
        let mut tx =
            self.client.begin_optimistic().await.map_err(|e| {
                format!("begin load transfer job snapshot transaction failed: {}", e)
            })?;
        let Some(job) = (match get_record::<FsTransferJobRecord>(
            &mut tx,
            self.job_key(job_id),
            "transfer job snapshot job lookup",
        )
        .await
        {
            Ok(v) => v,
            Err(err) => return Err(rollback_with_error(&mut tx, err).await),
        }) else {
            let _ = tx.rollback().await;
            return Ok(None);
        };
        let running_batches = match self
            .load_batches_in_state_for_job_from_tx(
                &mut tx,
                job_id,
                FluxonFsTransferBatchState::Running,
            )
            .await
        {
            Ok(v) => v,
            Err(err) => return Err(rollback_with_error(&mut tx, err).await),
        };
        let worker_attempts = match self
            .load_worker_attempts_for_job_from_tx(&mut tx, job_id)
            .await
        {
            Ok(v) => v,
            Err(err) => return Err(rollback_with_error(&mut tx, err).await),
        };
        let failed_files = match self.load_file_issues_for_job_from_tx(&mut tx, job_id).await {
            Ok(v) => v,
            Err(err) => return Err(rollback_with_error(&mut tx, err).await),
        };
        let failed_file_count = job.failed_file_count.max(failed_files.len() as i64);
        let open_batches = Self::transfer_job_open_batch_count(&job);
        let done_batches = Self::transfer_job_done_batch_count(&job);
        let scan_epoch = job.scan_epoch;
        let scan_finished = job.scan_finished;
        let _ = tx.rollback().await;
        Ok(Some(FsTransferJobSnapshot {
            scan_epoch,
            scan_finished,
            job,
            open_batches,
            done_batches,
            failed_file_count,
            running_batches,
            worker_attempts,
            failed_files,
            live_detail: None,
        }))
    }

    async fn load_transfer_job_snapshots(&self) -> Result<Vec<FsTransferJobSnapshot>, String> {
        let jobs = self.load_transfer_job_records().await?;
        let batches = self.load_transfer_batches().await?;
        let worker_attempts = self.load_transfer_worker_attempt_records().await?;
        let failed_files = self.load_transfer_batch_file_issue_records().await?;
        let mut open_batch_count_by_job = std::collections::BTreeMap::<String, i64>::new();
        let mut done_batch_count_by_job = std::collections::BTreeMap::<String, i64>::new();
        let mut failed_file_count_by_job = std::collections::BTreeMap::<String, i64>::new();
        let mut running_batches_by_job =
            std::collections::BTreeMap::<String, Vec<FsTransferBatchRecord>>::new();
        let mut worker_attempts_by_job =
            std::collections::BTreeMap::<String, Vec<FsTransferWorkerAttemptRecord>>::new();
        let mut failed_files_by_job =
            std::collections::BTreeMap::<String, Vec<FsTransferBatchFileIssueRecord>>::new();
        for batch in batches {
            if matches!(
                batch.state,
                FluxonFsTransferBatchState::Ready
                    | FluxonFsTransferBatchState::Running
                    | FluxonFsTransferBatchState::Expired
                    | FluxonFsTransferBatchState::Done
            ) {
                *open_batch_count_by_job
                    .entry(batch.job_id.clone())
                    .or_insert(0) += 1;
            }
            if batch.state == FluxonFsTransferBatchState::Running {
                running_batches_by_job
                    .entry(batch.job_id.clone())
                    .or_default()
                    .push(batch);
            } else if matches!(
                batch.state,
                FluxonFsTransferBatchState::Done | FluxonFsTransferBatchState::Finished
            ) {
                *done_batch_count_by_job
                    .entry(batch.job_id.clone())
                    .or_insert(0) += 1;
            }
        }
        for attempt in worker_attempts {
            worker_attempts_by_job
                .entry(attempt.job_id.clone())
                .or_default()
                .push(attempt);
        }
        for issue in failed_files {
            *failed_file_count_by_job
                .entry(issue.job_id.clone())
                .or_insert(0) += 1;
            failed_files_by_job
                .entry(issue.job_id.clone())
                .or_default()
                .push(issue);
        }
        let mut out = Vec::with_capacity(jobs.len());
        for job in jobs {
            let running_batches = running_batches_by_job
                .remove(&job.job_id)
                .unwrap_or_default();
            let worker_attempts = worker_attempts_by_job
                .remove(&job.job_id)
                .unwrap_or_default();
            let failed_files = failed_files_by_job.remove(&job.job_id).unwrap_or_default();
            out.push(FsTransferJobSnapshot {
                scan_epoch: job.scan_epoch,
                scan_finished: job.scan_finished,
                open_batches: Self::transfer_job_open_batch_count(&job),
                done_batches: Self::transfer_job_done_batch_count(&job),
                failed_file_count: job
                    .failed_file_count
                    .max(failed_file_count_by_job.remove(&job.job_id).unwrap_or(0)),
                running_batches,
                worker_attempts,
                failed_files,
                job,
                live_detail: None,
            });
        }
        Ok(out)
    }

    async fn load_transfer_batches(&self) -> Result<Vec<FsTransferBatchRecord>, String> {
        let mut tx = self
            .client
            .begin_optimistic()
            .await
            .map_err(|e| format!("begin load transfer batch transaction failed: {}", e))?;
        let mut batches = match self.load_batches_from_tx(&mut tx).await {
            Ok(v) => v,
            Err(err) => return Err(rollback_with_error(&mut tx, err).await),
        };
        batches.sort_by(|a, b| {
            a.job_id
                .cmp(&b.job_id)
                .then(a.root_relpath.cmp(&b.root_relpath))
                .then(a.generation.cmp(&b.generation))
                .then(a.batch_id.cmp(&b.batch_id))
        });
        let _ = tx.rollback().await;
        Ok(batches)
    }

    async fn load_transfer_batches_for_job(
        &self,
        job_id: &str,
    ) -> Result<Vec<FsTransferBatchRecord>, String> {
        let mut tx =
            self.client.begin_optimistic().await.map_err(|e| {
                format!("begin load transfer batch by job transaction failed: {}", e)
            })?;
        let batches = match self.load_batches_for_job_from_tx(&mut tx, job_id).await {
            Ok(v) => v,
            Err(err) => return Err(rollback_with_error(&mut tx, err).await),
        };
        let _ = tx.rollback().await;
        Ok(batches)
    }

    async fn load_transfer_direct_files_complete_records(
        &self,
    ) -> Result<Vec<FsTransferDirectFilesCompleteRecord>, String> {
        let mut tx = self.client.begin_optimistic().await.map_err(|e| {
            format!(
                "begin load transfer direct files complete transaction failed: {}",
                e
            )
        })?;
        let mut rows = match self
            .load_direct_files_complete_records_from_tx(&mut tx)
            .await
        {
            Ok(v) => v,
            Err(err) => return Err(rollback_with_error(&mut tx, err).await),
        };
        rows.sort_by(|a, b| {
            a.job_id
                .cmp(&b.job_id)
                .then(a.root_relpath.cmp(&b.root_relpath))
        });
        let _ = tx.rollback().await;
        Ok(rows)
    }

    async fn load_transfer_direct_files_complete_records_for_job(
        &self,
        job_id: &str,
    ) -> Result<Vec<FsTransferDirectFilesCompleteRecord>, String> {
        let mut tx = self.client.begin_optimistic().await.map_err(|e| {
            format!(
                "begin load transfer direct files complete by job transaction failed: {}",
                e
            )
        })?;
        let rows = match self
            .load_direct_files_complete_records_for_job_from_tx(&mut tx, job_id)
            .await
        {
            Ok(v) => v,
            Err(err) => return Err(rollback_with_error(&mut tx, err).await),
        };
        let _ = tx.rollback().await;
        Ok(rows)
    }

    async fn load_transfer_worker_attempt_records(
        &self,
    ) -> Result<Vec<FsTransferWorkerAttemptRecord>, String> {
        let mut tx = self.client.begin_optimistic().await.map_err(|e| {
            format!(
                "begin load transfer worker attempt transaction failed: {}",
                e
            )
        })?;
        let mut rows = match self.load_worker_attempts_from_tx(&mut tx).await {
            Ok(v) => v,
            Err(err) => return Err(rollback_with_error(&mut tx, err).await),
        };
        rows.sort_by(|a, b| {
            a.job_id
                .cmp(&b.job_id)
                .then(a.batch_id.cmp(&b.batch_id))
                .then(a.created_at_unix_ms.cmp(&b.created_at_unix_ms))
                .then(a.worker_task_id.cmp(&b.worker_task_id))
        });
        let _ = tx.rollback().await;
        Ok(rows)
    }

    async fn load_transfer_batch_collect_info_records(
        &self,
    ) -> Result<Vec<FsTransferBatchCollectInfoRecord>, String> {
        let mut tx =
            self.client.begin_optimistic().await.map_err(|e| {
                format!("begin load transfer collect info transaction failed: {}", e)
            })?;
        let mut rows = match self.load_collect_infos_from_tx(&mut tx).await {
            Ok(v) => v,
            Err(err) => return Err(rollback_with_error(&mut tx, err).await),
        };
        rows.sort_by(|a, b| {
            a.job_id
                .cmp(&b.job_id)
                .then(a.batch_id.cmp(&b.batch_id))
                .then(a.collect_kind.as_db_str().cmp(b.collect_kind.as_db_str()))
        });
        let _ = tx.rollback().await;
        Ok(rows)
    }

    async fn load_transfer_batch_file_issue_records(
        &self,
    ) -> Result<Vec<FsTransferBatchFileIssueRecord>, String> {
        let mut tx = self
            .client
            .begin_optimistic()
            .await
            .map_err(|e| format!("begin load transfer file issue transaction failed: {}", e))?;
        let mut rows = match self.load_file_issues_from_tx(&mut tx).await {
            Ok(v) => v,
            Err(err) => return Err(rollback_with_error(&mut tx, err).await),
        };
        rows.sort_by(|a, b| {
            a.job_id
                .cmp(&b.job_id)
                .then(a.batch_id.cmp(&b.batch_id))
                .then(a.relpath.cmp(&b.relpath))
        });
        let _ = tx.rollback().await;
        Ok(rows)
    }

    async fn load_next_ready_transfer_batch_for_job(
        &self,
        job_id: &str,
        batch_class: FsTransferReadyBatchClass,
    ) -> Result<Option<FsTransferReadyBatchDispatch>, String> {
        let mut tx = self.client.begin_optimistic().await.map_err(|e| {
            format!(
                "begin load next ready transfer batch transaction failed: {}",
                e
            )
        })?;
        let state_prefix = self.batch_state_prefix(job_id, FluxonFsTransferBatchState::Ready);
        let state_keys = match scan_all_keys(&mut tx, state_prefix.clone()).await {
            Ok(v) => v,
            Err(err) => return Err(rollback_with_error(&mut tx, err).await),
        };
        for key_bytes in state_keys {
            let batch_id = match decode_batch_id_from_state_key(&key_bytes, state_prefix.as_slice())
            {
                Ok(v) => v,
                Err(err) => return Err(rollback_with_error(&mut tx, err).await),
            };
            let batch = match get_required_record::<FsTransferBatchRecord>(
                &mut tx,
                self.batch_key(job_id, batch_id.as_str()),
                "next ready transfer batch lookup",
            )
            .await
            {
                Ok(v) => v,
                Err(err) => return Err(rollback_with_error(&mut tx, err).await),
            };
            let manifest_dispatch_class =
                match decode_transfer_batch_manifest_dispatch_class(&batch, false) {
                    Ok(v) => v,
                    Err(err) => return Err(rollback_with_error(&mut tx, err).await),
                };
            if manifest_dispatch_class != batch_class {
                continue;
            }
            let collect_infos = match self
                .load_collect_infos_for_batch_from_tx(&mut tx, job_id, batch_id.as_str())
                .await
            {
                Ok(v) => v,
                Err(err) => return Err(rollback_with_error(&mut tx, err).await),
            };
            let effective_dispatch_class = match decode_transfer_batch_manifest_dispatch_class(
                &batch,
                !collect_infos.is_empty(),
            ) {
                Ok(v) => v,
                Err(err) => return Err(rollback_with_error(&mut tx, err).await),
            };
            if effective_dispatch_class != batch_class {
                continue;
            }
            let _ = tx.rollback().await;
            return Ok(Some(FsTransferReadyBatchDispatch {
                batch,
                collect_infos,
            }));
        }
        let _ = tx.rollback().await;
        Ok(None)
    }

    async fn load_ready_transfer_batches_for_job(
        &self,
        job_id: &str,
    ) -> Result<Vec<FsTransferBatchRecord>, String> {
        let mut tx =
            self.client.begin_optimistic().await.map_err(|e| {
                format!("begin load ready transfer batch transaction failed: {}", e)
            })?;
        let state_prefix = self.batch_state_prefix(job_id, FluxonFsTransferBatchState::Ready);
        let state_keys = match scan_all_keys(&mut tx, state_prefix.clone()).await {
            Ok(v) => v,
            Err(err) => return Err(rollback_with_error(&mut tx, err).await),
        };
        let mut batches = Vec::with_capacity(state_keys.len());
        for key_bytes in state_keys {
            let batch_id = match decode_batch_id_from_state_key(&key_bytes, state_prefix.as_slice())
            {
                Ok(v) => v,
                Err(err) => return Err(rollback_with_error(&mut tx, err).await),
            };
            let batch = match get_required_record::<FsTransferBatchRecord>(
                &mut tx,
                self.batch_key(job_id, batch_id.as_str()),
                "ready transfer batch lookup",
            )
            .await
            {
                Ok(v) => v,
                Err(err) => return Err(rollback_with_error(&mut tx, err).await),
            };
            batches.push(batch);
        }
        batches.sort_by(|a, b| {
            a.root_relpath
                .cmp(&b.root_relpath)
                .then(a.generation.cmp(&b.generation))
                .then(a.batch_id.cmp(&b.batch_id))
        });
        let _ = tx.rollback().await;
        Ok(batches)
    }

    async fn begin_transfer_scan_epoch(&self, job_id: &str) -> Result<i64, String> {
        let (mut tx, mut job) = self.begin_locked_job_tx(job_id).await?;
        if job.state != FluxonFsTransferJobState::Running {
            let err = format!("no running transfer job found: job_id={}", job_id);
            return Err(rollback_with_error(&mut tx, err).await);
        }
        let next_epoch = job.scan_epoch + 1;
        job.scan_epoch = next_epoch;
        job.scan_finished = false;
        job.scan_discovered_batch_count = 0;
        job.scan_discovered_file_count = 0;
        job.scan_discovered_bytes = 0;
        job.last_error.clear();
        job.updated_at_unix_ms = chrono::Utc::now().timestamp_millis();
        tx_try!(
            tx,
            put_record(
                &mut tx,
                self.job_key(job_id),
                &job,
                "begin transfer scan epoch"
            )
            .await
        );
        tx.commit().await.map_err(|e| {
            format!(
                "commit begin transfer scan epoch job_id={} failed: {}",
                job_id, e
            )
        })?;
        Ok(next_epoch)
    }

    async fn finish_transfer_scan_epoch(
        &self,
        job_id: &str,
        scan_epoch: i64,
    ) -> Result<(), String> {
        let (mut tx, mut job) = self.begin_locked_job_tx(job_id).await?;
        if job.state != FluxonFsTransferJobState::Running || job.scan_epoch != scan_epoch {
            let err = format!(
                "transfer scan finish rejected for stale or missing scan epoch: job_id={} scan_epoch={}",
                job_id, scan_epoch
            );
            return Err(rollback_with_error(&mut tx, err).await);
        }
        job.scan_finished = true;
        job.updated_at_unix_ms = chrono::Utc::now().timestamp_millis();
        tx_try!(
            tx,
            put_record(
                &mut tx,
                self.job_key(job_id),
                &job,
                "finish transfer scan epoch"
            )
            .await
        );
        tx.commit().await.map_err(|e| {
            format!(
                "commit finish transfer scan epoch job_id={} scan_epoch={} failed: {}",
                job_id, scan_epoch, e
            )
        })?;
        Ok(())
    }

    async fn apply_transfer_scan_batches_locked_tx(
        &self,
        tx: &mut Transaction,
        job: &mut FsTransferJobRecord,
        result: &FluxonFsTransferScanResultWire,
    ) -> Result<(), String> {
        job.last_error.clear();
        for batch in &result.direct_files_only_batches {
            let (discovered_batch_count, discovered_file_count, discovered_bytes) =
                self.insert_or_reuse_batch_locked_tx(tx, job, batch).await?;
            job.scan_discovered_batch_count = job
                .scan_discovered_batch_count
                .saturating_add(discovered_batch_count.max(0));
            job.scan_discovered_file_count = job
                .scan_discovered_file_count
                .saturating_add(discovered_file_count.max(0));
            job.scan_discovered_bytes = job
                .scan_discovered_bytes
                .saturating_add(discovered_bytes.max(0));
        }
        for batch in &result.full_dir_batches {
            let (discovered_batch_count, discovered_file_count, discovered_bytes) =
                self.insert_or_reuse_batch_locked_tx(tx, job, batch).await?;
            job.scan_discovered_batch_count = job
                .scan_discovered_batch_count
                .saturating_add(discovered_batch_count.max(0));
            job.scan_discovered_file_count = job
                .scan_discovered_file_count
                .saturating_add(discovered_file_count.max(0));
            job.scan_discovered_bytes = job
                .scan_discovered_bytes
                .saturating_add(discovered_bytes.max(0));
        }
        Ok(())
    }

    async fn apply_transfer_scan_append(
        &self,
        result: &FluxonFsTransferScanResultWire,
    ) -> Result<(), String> {
        let (mut tx, mut job) = self.begin_locked_job_tx(result.job_id.as_str()).await?;
        if job.state == FluxonFsTransferJobState::Stopping
            || job.state == FluxonFsTransferJobState::Cancelled
        {
            let _ = tx.rollback().await;
            return Ok(());
        }
        if job.state != FluxonFsTransferJobState::Running {
            let err = format!(
                "scan append rejected for missing running job: job_id={}",
                result.job_id
            );
            return Err(rollback_with_error(&mut tx, err).await);
        }
        if job.scan_epoch != result.scan_epoch {
            let err = format!(
                "scan append rejected for stale scan epoch: job_id={} result_epoch={} current_epoch={}",
                result.job_id, result.scan_epoch, job.scan_epoch
            );
            return Err(rollback_with_error(&mut tx, err).await);
        }
        if let Err(err) = self
            .apply_transfer_scan_batches_locked_tx(&mut tx, &mut job, result)
            .await
        {
            return Err(rollback_with_error(&mut tx, err).await);
        }
        job.updated_at_unix_ms = chrono::Utc::now().timestamp_millis();
        tx_try!(
            tx,
            put_record(
                &mut tx,
                self.job_key(result.job_id.as_str()),
                &job,
                "apply transfer scan append job update",
            )
            .await
        );
        tx.commit().await.map_err(|e| {
            format!(
                "commit apply transfer scan append job_id={} failed: {}",
                result.job_id, e
            )
        })?;
        Ok(())
    }

    async fn finish_transfer_scan_unit(
        &self,
        _result: &FluxonFsTransferScanResultWire,
    ) -> Result<(), String> {
        // Finished payloads already committed through apply_transfer_scan_result
        // before the master parks this unit in pending_finalize. Keeping the
        // finalize path as a no-op avoids re-locking the hot job row during
        // the final scan-epoch drain.
        Ok(())
    }

    async fn apply_transfer_scan_result(
        &self,
        result: &FluxonFsTransferScanResultWire,
    ) -> Result<(), String> {
        let (mut tx, mut job) = self.begin_locked_job_tx(result.job_id.as_str()).await?;
        // Scan-unit runtime still stays process-local, but durable apply now
        // owns direct-file coverage claims and the direct-listing completion
        // marker for each scanned root.
        if job.state == FluxonFsTransferJobState::Stopping
            || job.state == FluxonFsTransferJobState::Cancelled
        {
            let _ = tx.rollback().await;
            return Ok(());
        }
        if job.state != FluxonFsTransferJobState::Running {
            let err = format!(
                "scan result rejected for missing running job: job_id={}",
                result.job_id
            );
            return Err(rollback_with_error(&mut tx, err).await);
        }
        if job.scan_epoch != result.scan_epoch {
            let err = format!(
                "scan result rejected for stale scan epoch: job_id={} result_epoch={} current_epoch={}",
                result.job_id, result.scan_epoch, job.scan_epoch
            );
            return Err(rollback_with_error(&mut tx, err).await);
        }
        let normalized_result_root = tx_try!(
            tx,
            normalize_transfer_root_relpath(result.root_relpath.as_str())
        );
        let root_listing_incomplete = result.child_scan_units.iter().any(|child| {
            child.scan_unit_id == result.scan_unit_id
                && child.root_relpath == normalized_result_root
                && child.generation == result.generation
        });
        if let Err(err) = self
            .apply_transfer_scan_batches_locked_tx(&mut tx, &mut job, result)
            .await
        {
            return Err(rollback_with_error(&mut tx, err).await);
        }
        if !root_listing_incomplete {
            tx_try!(
                tx,
                self.upsert_direct_files_complete_marker_locked_tx(
                    &mut tx,
                    result.job_id.as_str(),
                    normalized_result_root.as_str(),
                    chrono::Utc::now().timestamp_millis(),
                )
                .await
            );
        }
        job.updated_at_unix_ms = chrono::Utc::now().timestamp_millis();
        tx_try!(
            tx,
            put_record(
                &mut tx,
                self.job_key(result.job_id.as_str()),
                &job,
                "apply transfer scan result job update",
            )
            .await
        );
        tx.commit().await.map_err(|e| {
            format!(
                "commit apply transfer scan result job_id={} failed: {}",
                result.job_id, e
            )
        })?;
        Ok(())
    }

    async fn assign_transfer_batch_to_worker(
        &self,
        job_id: &str,
        batch_id: &str,
        src_exporter_id: &str,
        worker_id: &str,
        worker_task_id: &str,
        dst_exporter_id: &str,
        lease_expire_unix_ms: i64,
    ) -> Result<(), String> {
        let (mut tx, mut job) = self.begin_locked_job_tx(job_id).await?;
        // A running owner is valid only when the batch row and the worker-lease
        // row are written together in the same transaction.
        if job.state != FluxonFsTransferJobState::Running {
            let err = format!("no running transfer job found: job_id={}", job_id);
            return Err(rollback_with_error(&mut tx, err).await);
        }
        let mut batch = tx_try!(
            tx,
            get_required_record::<FsTransferBatchRecord>(
                &mut tx,
                self.batch_key(job_id, batch_id),
                "assign transfer batch lookup",
            )
            .await
        );
        if batch.state != FluxonFsTransferBatchState::Ready {
            let err = format!(
                "transfer batch is not assignable: job_id={} batch_id={}",
                job_id, batch_id
            );
            return Err(rollback_with_error(&mut tx, err).await);
        }
        if batch.assigned_src_exporter_id.is_empty() {
            if src_exporter_id.trim().is_empty() {
                let err = format!(
                    "transfer batch source exporter binding must be non-empty: job_id={} batch_id={}",
                    job_id, batch_id
                );
                return Err(rollback_with_error(&mut tx, err).await);
            }
            batch.assigned_src_exporter_id = src_exporter_id.to_string();
        } else if batch.assigned_src_exporter_id != src_exporter_id {
            let err = format!(
                "transfer batch source exporter binding mismatch: job_id={} batch_id={} assigned_src_exporter_id={} requested_src_exporter_id={}",
                job_id, batch_id, batch.assigned_src_exporter_id, src_exporter_id
            );
            return Err(rollback_with_error(&mut tx, err).await);
        }
        if batch.assigned_dst_exporter_id.is_empty() {
            if dst_exporter_id.trim().is_empty() {
                let err = format!(
                    "transfer batch target exporter binding must be non-empty: job_id={} batch_id={}",
                    job_id, batch_id
                );
                return Err(rollback_with_error(&mut tx, err).await);
            }
            batch.assigned_dst_exporter_id = dst_exporter_id.to_string();
        } else if batch.assigned_dst_exporter_id != dst_exporter_id {
            let err = format!(
                "transfer batch target exporter binding mismatch: job_id={} batch_id={} assigned_dst_exporter_id={} requested_dst_exporter_id={}",
                job_id, batch_id, batch.assigned_dst_exporter_id, dst_exporter_id
            );
            return Err(rollback_with_error(&mut tx, err).await);
        }
        tx_try!(
            tx,
            delete_key(
                &mut tx,
                self.batch_state_key(job_id, FluxonFsTransferBatchState::Ready, batch_id),
                "delete ready batch state index",
            )
            .await
        );
        Self::adjust_job_batch_state_count(&mut job, FluxonFsTransferBatchState::Ready, -1);
        batch.state = FluxonFsTransferBatchState::Running;
        batch.owner_worker_id = worker_id.to_string();
        batch.owner_worker_task_id = worker_task_id.to_string();
        batch.lease_expire_unix_ms = lease_expire_unix_ms;
        tx_try!(
            tx,
            put_record(
                &mut tx,
                self.batch_key(job_id, batch_id),
                &batch,
                "assign transfer batch"
            )
            .await
        );
        tx_try!(
            tx,
            put_marker(
                &mut tx,
                self.batch_state_key(job_id, FluxonFsTransferBatchState::Running, batch_id),
                "insert running batch state index",
            )
            .await
        );
        Self::adjust_job_batch_state_count(&mut job, FluxonFsTransferBatchState::Running, 1);
        job.updated_at_unix_ms = chrono::Utc::now().timestamp_millis();
        tx_try!(
            tx,
            put_record(
                &mut tx,
                self.job_key(job_id),
                &job,
                "assign transfer batch job update",
            )
            .await
        );
        let worker_lease = FsTransferWorkerLeaseRecord {
            job_id: job_id.to_string(),
            worker_id: worker_id.to_string(),
            worker_task_id: worker_task_id.to_string(),
            assigned_batch_id: batch_id.to_string(),
            lease_expire_unix_ms,
        };
        tx_try!(
            tx,
            put_record(
                &mut tx,
                self.worker_lease_key(job_id, worker_task_id),
                &worker_lease,
                "assign transfer worker lease",
            )
            .await
        );
        let now_unix_ms = chrono::Utc::now().timestamp_millis();
        let attempt = FsTransferWorkerAttemptRecord {
            job_id: job_id.to_string(),
            batch_id: batch_id.to_string(),
            worker_id: worker_id.to_string(),
            worker_task_id: worker_task_id.to_string(),
            dst_exporter_id: dst_exporter_id.to_string(),
            state: FsTransferWorkerAttemptState::Launching,
            launch_attempt_count: 0,
            visible_file_count: 0,
            visible_bytes: 0,
            last_error: String::new(),
            stop_reason: None,
            created_at_unix_ms: now_unix_ms,
            updated_at_unix_ms: now_unix_ms,
        };
        tx_try!(
            tx,
            put_record(
                &mut tx,
                self.worker_attempt_key(job_id, worker_task_id),
                &attempt,
                "insert transfer worker attempt",
            )
            .await
        );
        tx.commit().await.map_err(|e| {
            format!(
                "commit assign transfer batch job_id={} batch_id={} failed: {}",
                job_id, batch_id, e
            )
        })?;
        Ok(())
    }

    async fn record_transfer_worker_launch_retry(
        &self,
        job_id: &str,
        batch_id: &str,
        worker_id: &str,
        worker_task_id: &str,
        err_text: &str,
        now_unix_ms: i64,
    ) -> Result<(), String> {
        let mut tx = self
            .begin_mutation_tx("record transfer worker launch retry")
            .await?;
        let Some(mut attempt) = tx_try!(
            tx,
            get_record_for_update::<FsTransferWorkerAttemptRecord>(
                &mut tx,
                self.worker_attempt_key(job_id, worker_task_id),
                "record transfer worker launch retry lookup",
            )
            .await
        ) else {
            let err = format!(
                "transfer worker attempt missing during launch retry: job_id={} batch_id={} worker_task_id={}",
                job_id, batch_id, worker_task_id
            );
            return Err(rollback_with_error(&mut tx, err).await);
        };
        if attempt.batch_id != batch_id || attempt.worker_id != worker_id {
            let err = format!(
                "transfer worker attempt mismatch during launch retry: job_id={} batch_id={} worker_task_id={}",
                job_id, batch_id, worker_task_id
            );
            return Err(rollback_with_error(&mut tx, err).await);
        }
        attempt.launch_attempt_count = attempt.launch_attempt_count.saturating_add(1);
        attempt.last_error = err_text.to_string();
        attempt.updated_at_unix_ms = now_unix_ms;
        tx_try!(
            tx,
            put_record(
                &mut tx,
                self.worker_attempt_key(job_id, worker_task_id),
                &attempt,
                "record transfer worker launch retry",
            )
            .await
        );
        tx.commit().await.map_err(|e| {
            format!(
                "commit record transfer worker launch retry job_id={} batch_id={} worker_task_id={} failed: {}",
                job_id, batch_id, worker_task_id, e
            )
        })?;
        Ok(())
    }

    async fn mark_transfer_worker_launch_acknowledged(
        &self,
        job_id: &str,
        batch_id: &str,
        worker_id: &str,
        worker_task_id: &str,
        now_unix_ms: i64,
    ) -> Result<(), String> {
        let mut tx = self
            .begin_mutation_tx("mark transfer worker launch acknowledged")
            .await?;
        let Some(mut attempt) = tx_try!(
            tx,
            get_record_for_update::<FsTransferWorkerAttemptRecord>(
                &mut tx,
                self.worker_attempt_key(job_id, worker_task_id),
                "mark transfer worker launch acknowledged lookup",
            )
            .await
        ) else {
            let err = format!(
                "transfer worker attempt missing during launch ack: job_id={} batch_id={} worker_task_id={}",
                job_id, batch_id, worker_task_id
            );
            return Err(rollback_with_error(&mut tx, err).await);
        };
        if attempt.batch_id != batch_id || attempt.worker_id != worker_id {
            let err = format!(
                "transfer worker attempt mismatch during launch ack: job_id={} batch_id={} worker_task_id={}",
                job_id, batch_id, worker_task_id
            );
            return Err(rollback_with_error(&mut tx, err).await);
        }
        attempt.state = FsTransferWorkerAttemptState::Running;
        attempt.last_error.clear();
        attempt.updated_at_unix_ms = now_unix_ms;
        tx_try!(
            tx,
            put_record(
                &mut tx,
                self.worker_attempt_key(job_id, worker_task_id),
                &attempt,
                "mark transfer worker launch acknowledged",
            )
            .await
        );
        tx.commit().await.map_err(|e| {
            format!(
                "commit mark transfer worker launch acknowledged job_id={} batch_id={} worker_task_id={} failed: {}",
                job_id, batch_id, worker_task_id, e
            )
        })?;
        Ok(())
    }

    async fn mark_transfer_worker_attempt_stopped(
        &self,
        job_id: &str,
        batch_id: &str,
        worker_id: &str,
        worker_task_id: &str,
        stop_reason: Option<FluxonFsTransferWorkerStopReasonWire>,
        err_text: &str,
        now_unix_ms: i64,
    ) -> Result<(), String> {
        let mut tx = self
            .begin_mutation_tx("mark transfer worker attempt stopped")
            .await?;
        let Some(mut attempt) = tx_try!(
            tx,
            get_record_for_update::<FsTransferWorkerAttemptRecord>(
                &mut tx,
                self.worker_attempt_key(job_id, worker_task_id),
                "mark transfer worker attempt stopped lookup",
            )
            .await
        ) else {
            let err = format!(
                "transfer worker attempt missing during stop: job_id={} batch_id={} worker_task_id={}",
                job_id, batch_id, worker_task_id
            );
            return Err(rollback_with_error(&mut tx, err).await);
        };
        if attempt.batch_id != batch_id || attempt.worker_id != worker_id {
            let err = format!(
                "transfer worker attempt mismatch during stop: job_id={} batch_id={} worker_task_id={}",
                job_id, batch_id, worker_task_id
            );
            return Err(rollback_with_error(&mut tx, err).await);
        }
        attempt.state = FsTransferWorkerAttemptState::Stopped;
        attempt.stop_reason = stop_reason;
        attempt.last_error = err_text.to_string();
        attempt.updated_at_unix_ms = now_unix_ms;
        tx_try!(
            tx,
            put_record(
                &mut tx,
                self.worker_attempt_key(job_id, worker_task_id),
                &attempt,
                "mark transfer worker attempt stopped",
            )
            .await
        );
        tx.commit().await.map_err(|e| {
            format!(
                "commit mark transfer worker attempt stopped job_id={} batch_id={} worker_task_id={} failed: {}",
                job_id, batch_id, worker_task_id, e
            )
        })?;
        Ok(())
    }

    async fn apply_transfer_worker_result(
        &self,
        result: &FluxonFsTransferWorkerResultWire,
    ) -> Result<FluxonFsTransferWorkerResultAckWire, String> {
        let total_start = std::time::Instant::now();
        let (mut tx, mut job) = self.begin_locked_job_tx(result.job_id.as_str()).await?;
        let stop_reason = transfer_worker_stop_reason_for_job_state(job.state);
        let now_unix_ms = chrono::Utc::now().timestamp_millis();
        if job.state == FluxonFsTransferJobState::Stopping
            || job.state == FluxonFsTransferJobState::Cancelled
        {
            tx_try!(
                tx,
                self.stop_worker_for_stopping_job_locked_tx(
                    &mut tx,
                    &mut job,
                    result.job_id.as_str(),
                    result.batch_id.as_str(),
                    result.worker_id.as_str(),
                    result.worker_task_id.as_str(),
                    now_unix_ms,
                )
                .await
            );
            job.updated_at_unix_ms = now_unix_ms;
            tx_try!(
                tx,
                put_record(
                    &mut tx,
                    self.job_key(result.job_id.as_str()),
                    &job,
                    "apply transfer worker result stopping job update",
                )
                .await
            );
            tx.commit().await.map_err(|e| {
                format!(
                    "commit apply transfer worker result stop cleanup job_id={} batch_id={} worker_id={} failed: {}",
                    result.job_id, result.batch_id, result.worker_id, e
                )
            })?;
            return Ok(FluxonFsTransferWorkerResultAckWire::stop(
                FluxonFsTransferWorkerStopReasonWire::Cancelled,
            ));
        }
        if job.state != FluxonFsTransferJobState::Running {
            tx_try!(
                tx,
                delete_key(
                    &mut tx,
                    self.worker_lease_key(result.job_id.as_str(), result.worker_task_id.as_str()),
                    "delete transfer worker lease for non-running job result",
                )
                .await
            );
            tx_try!(
                tx,
                self.stop_worker_attempt_if_active_locked_tx(
                    &mut tx,
                    result.job_id.as_str(),
                    result.batch_id.as_str(),
                    result.worker_id.as_str(),
                    result.worker_task_id.as_str(),
                    stop_reason,
                    "worker result rejected because the job is no longer running",
                    now_unix_ms,
                    "worker result non-running attempt lookup",
                )
                .await
            );
            tx.commit().await.map_err(|e| {
                format!(
                    "commit apply transfer worker result non-running stop job_id={} batch_id={} worker_id={} failed: {}",
                    result.job_id, result.batch_id, result.worker_id, e
                )
            })?;
            return Ok(FluxonFsTransferWorkerResultAckWire::stop(stop_reason));
        }
        // Result acceptance only proves that the current owner attempt finished
        // local work. The batch stays Running-without-owner until reconcile
        // verifies final target visibility and moves it to Finished or Ready.
        let Some(mut batch) = tx_try!(
            tx,
            get_record::<FsTransferBatchRecord>(
                &mut tx,
                self.batch_key(result.job_id.as_str(), result.batch_id.as_str()),
                "worker result batch lookup",
            )
            .await
        ) else {
            let worker_attempt_key =
                self.worker_attempt_key(result.job_id.as_str(), result.worker_task_id.as_str());
            if let Some(mut attempt) = tx_try!(
                tx,
                get_record::<FsTransferWorkerAttemptRecord>(
                    &mut tx,
                    worker_attempt_key.clone(),
                    "worker result attempt lookup before batch-missing stop",
                )
                .await
            ) {
                if attempt.worker_id == result.worker_id
                    && attempt.batch_id == result.batch_id
                    && attempt.state != FsTransferWorkerAttemptState::Finished
                    && attempt.state != FsTransferWorkerAttemptState::Stopped
                {
                    attempt.state = FsTransferWorkerAttemptState::Stopped;
                    attempt.stop_reason = Some(stop_reason);
                    attempt.last_error =
                        "worker result batch missing or no longer owned by this attempt"
                            .to_string();
                    attempt.updated_at_unix_ms = chrono::Utc::now().timestamp_millis();
                    tx_try!(
                        tx,
                        put_record(
                            &mut tx,
                            worker_attempt_key,
                            &attempt,
                            "apply transfer worker result batch-missing stop attempt update",
                        )
                        .await
                    );
                    tx.commit().await.map_err(|e| {
                        format!(
                            "commit apply transfer worker result batch-missing stop job_id={} batch_id={} worker_id={} failed: {}",
                            result.job_id, result.batch_id, result.worker_id, e
                        )
                    })?;
                } else {
                    let _ = tx.rollback().await;
                }
            } else {
                let _ = tx.rollback().await;
            }
            return Ok(FluxonFsTransferWorkerResultAckWire::stop(stop_reason));
        };
        if batch.state != FluxonFsTransferBatchState::Running
            || batch.owner_worker_id != result.worker_id
            || batch.owner_worker_task_id != result.worker_task_id
        {
            let worker_attempt_key =
                self.worker_attempt_key(result.job_id.as_str(), result.worker_task_id.as_str());
            if let Some(mut attempt) = tx_try!(
                tx,
                get_record::<FsTransferWorkerAttemptRecord>(
                    &mut tx,
                    worker_attempt_key.clone(),
                    "worker result attempt lookup before ownership-mismatch stop",
                )
                .await
            ) {
                if attempt.worker_id == result.worker_id
                    && attempt.batch_id == result.batch_id
                    && attempt.state != FsTransferWorkerAttemptState::Finished
                    && attempt.state != FsTransferWorkerAttemptState::Stopped
                {
                    attempt.state = FsTransferWorkerAttemptState::Stopped;
                    attempt.stop_reason = Some(stop_reason);
                    attempt.last_error =
                        "worker result ownership mismatch; attempt superseded by newer owner"
                            .to_string();
                    attempt.updated_at_unix_ms = chrono::Utc::now().timestamp_millis();
                    tx_try!(
                        tx,
                        put_record(
                            &mut tx,
                            worker_attempt_key,
                            &attempt,
                            "apply transfer worker result ownership-mismatch stop attempt update",
                        )
                        .await
                    );
                    tx.commit().await.map_err(|e| {
                        format!(
                            "commit apply transfer worker result ownership-mismatch stop job_id={} batch_id={} worker_id={} failed: {}",
                            result.job_id, result.batch_id, result.worker_id, e
                        )
                    })?;
                } else {
                    let _ = tx.rollback().await;
                }
            } else {
                let _ = tx.rollback().await;
            }
            return Ok(FluxonFsTransferWorkerResultAckWire::stop(stop_reason));
        }
        let Some(worker_lease) = tx_try!(
            tx,
            get_record::<FsTransferWorkerLeaseRecord>(
                &mut tx,
                self.worker_lease_key(result.job_id.as_str(), result.worker_task_id.as_str()),
                "worker result lease lookup",
            )
            .await
        ) else {
            let worker_attempt_key =
                self.worker_attempt_key(result.job_id.as_str(), result.worker_task_id.as_str());
            if let Some(mut attempt) = tx_try!(
                tx,
                get_record::<FsTransferWorkerAttemptRecord>(
                    &mut tx,
                    worker_attempt_key.clone(),
                    "worker result attempt lookup before lease-missing stop",
                )
                .await
            ) {
                if attempt.worker_id == result.worker_id
                    && attempt.batch_id == result.batch_id
                    && attempt.state != FsTransferWorkerAttemptState::Finished
                    && attempt.state != FsTransferWorkerAttemptState::Stopped
                {
                    attempt.state = FsTransferWorkerAttemptState::Stopped;
                    attempt.stop_reason = Some(stop_reason);
                    attempt.last_error =
                        "worker result lease missing; attempt superseded by newer owner"
                            .to_string();
                    attempt.updated_at_unix_ms = chrono::Utc::now().timestamp_millis();
                    tx_try!(
                        tx,
                        put_record(
                            &mut tx,
                            worker_attempt_key,
                            &attempt,
                            "apply transfer worker result lease-missing stop attempt update",
                        )
                        .await
                    );
                    tx.commit().await.map_err(|e| {
                        format!(
                            "commit apply transfer worker result lease-missing stop job_id={} batch_id={} worker_id={} failed: {}",
                            result.job_id, result.batch_id, result.worker_id, e
                        )
                    })?;
                } else {
                    let _ = tx.rollback().await;
                }
            } else {
                let _ = tx.rollback().await;
            }
            return Ok(FluxonFsTransferWorkerResultAckWire::stop(stop_reason));
        };
        if worker_lease.worker_id != result.worker_id
            || worker_lease.assigned_batch_id != result.batch_id
        {
            let worker_attempt_key =
                self.worker_attempt_key(result.job_id.as_str(), result.worker_task_id.as_str());
            if let Some(mut attempt) = tx_try!(
                tx,
                get_record::<FsTransferWorkerAttemptRecord>(
                    &mut tx,
                    worker_attempt_key.clone(),
                    "worker result attempt lookup before lease-mismatch stop",
                )
                .await
            ) {
                if attempt.worker_id == result.worker_id
                    && attempt.batch_id == result.batch_id
                    && attempt.state != FsTransferWorkerAttemptState::Finished
                    && attempt.state != FsTransferWorkerAttemptState::Stopped
                {
                    attempt.state = FsTransferWorkerAttemptState::Stopped;
                    attempt.stop_reason = Some(stop_reason);
                    attempt.last_error =
                        "worker result lease mismatch; attempt superseded by newer owner"
                            .to_string();
                    attempt.updated_at_unix_ms = chrono::Utc::now().timestamp_millis();
                    tx_try!(
                        tx,
                        put_record(
                            &mut tx,
                            worker_attempt_key,
                            &attempt,
                            "apply transfer worker result lease-mismatch stop attempt update",
                        )
                        .await
                    );
                    tx.commit().await.map_err(|e| {
                        format!(
                            "commit apply transfer worker result lease-mismatch stop job_id={} batch_id={} worker_id={} failed: {}",
                            result.job_id, result.batch_id, result.worker_id, e
                        )
                    })?;
                } else {
                    let _ = tx.rollback().await;
                }
            } else {
                let _ = tx.rollback().await;
            }
            return Ok(FluxonFsTransferWorkerResultAckWire::stop(stop_reason));
        }
        tx_try!(
            tx,
            delete_key(
                &mut tx,
                self.batch_state_key(
                    result.job_id.as_str(),
                    FluxonFsTransferBatchState::Running,
                    result.batch_id.as_str(),
                ),
                "delete running batch state index after worker result",
            )
            .await
        );
        Self::adjust_job_batch_state_count(&mut job, FluxonFsTransferBatchState::Running, -1);
        tx_try!(
            tx,
            delete_key(
                &mut tx,
                self.worker_lease_key(result.job_id.as_str(), result.worker_task_id.as_str()),
                "delete transfer worker lease after worker result",
            )
            .await
        );
        batch.state = FluxonFsTransferBatchState::Done;
        batch.owner_worker_id.clear();
        batch.owner_worker_task_id.clear();
        batch.lease_expire_unix_ms = 0;
        tx_try!(
            tx,
            put_record(
                &mut tx,
                self.batch_key(result.job_id.as_str(), result.batch_id.as_str()),
                &batch,
                "apply transfer worker result batch update",
            )
            .await
        );
        tx_try!(
            tx,
            put_marker(
                &mut tx,
                self.batch_state_key(
                    result.job_id.as_str(),
                    FluxonFsTransferBatchState::Done,
                    result.batch_id.as_str(),
                ),
                "insert done batch state index after worker result",
            )
            .await
        );
        Self::adjust_job_batch_state_count(&mut job, FluxonFsTransferBatchState::Done, 1);
        let visible_file_count = result.file_results.len() as i64;
        let visible_bytes = result
            .file_results
            .iter()
            .fold(0_i64, |acc, row| acc.saturating_add(row.visible_size));
        let now_unix_ms = chrono::Utc::now().timestamp_millis();
        for failed_file in &result.failed_file_results {
            let issue_key = self.file_issue_key(
                result.job_id.as_str(),
                result.batch_id.as_str(),
                failed_file.relpath.as_str(),
            );
            let existing = tx_try!(
                tx,
                get_record::<FsTransferBatchFileIssueRecord>(
                    &mut tx,
                    issue_key.clone(),
                    "worker result file issue lookup",
                )
                .await
            );
            let issue_was_new = existing.is_none();
            let record = match existing {
                Some(mut existing) => {
                    existing.reason_kind = failed_file.reason_kind;
                    existing.reason_detail = failed_file.reason_detail.clone();
                    existing.updated_at_unix_ms = now_unix_ms;
                    existing
                }
                None => FsTransferBatchFileIssueRecord {
                    job_id: result.job_id.clone(),
                    batch_id: result.batch_id.clone(),
                    relpath: failed_file.relpath.clone(),
                    reason_kind: failed_file.reason_kind,
                    reason_detail: failed_file.reason_detail.clone(),
                    created_at_unix_ms: now_unix_ms,
                    updated_at_unix_ms: now_unix_ms,
                },
            };
            if issue_was_new {
                job.failed_file_count = job.failed_file_count.saturating_add(1);
            }
            tx_try!(
                tx,
                put_record(
                    &mut tx,
                    issue_key,
                    &record,
                    "apply transfer worker result file issue update",
                )
                .await
            );
        }
        job.updated_at_unix_ms = now_unix_ms;
        tx_try!(
            tx,
            put_record(
                &mut tx,
                self.job_key(result.job_id.as_str()),
                &job,
                "apply transfer worker result job update",
            )
            .await
        );
        if let Some(mut attempt) = tx_try!(
            tx,
            get_record::<FsTransferWorkerAttemptRecord>(
                &mut tx,
                self.worker_attempt_key(result.job_id.as_str(), result.worker_task_id.as_str()),
                "worker result attempt lookup",
            )
            .await
        ) {
            attempt.state = FsTransferWorkerAttemptState::Finished;
            attempt.visible_file_count = visible_file_count;
            attempt.visible_bytes = visible_bytes;
            attempt.last_error.clear();
            attempt.stop_reason = None;
            attempt.updated_at_unix_ms = now_unix_ms;
            tx_try!(
                tx,
                put_record(
                    &mut tx,
                    self.worker_attempt_key(result.job_id.as_str(), result.worker_task_id.as_str()),
                    &attempt,
                    "apply transfer worker result attempt update",
                )
                .await
            );
        }
        for collect_info_result in &result.collect_info_results {
            let mut collect_info = tx_try!(
                tx,
                get_required_record::<FsTransferBatchCollectInfoRecord>(
                    &mut tx,
                    self.collect_info_key(
                        result.job_id.as_str(),
                        result.batch_id.as_str(),
                        collect_info_result.collect_kind,
                    ),
                    "worker result collect info lookup",
                )
                .await
            );
            collect_info.materialized = true;
            collect_info.materialized_at_unix_ms = chrono::Utc::now().timestamp_millis();
            tx_try!(
                tx,
                put_record(
                    &mut tx,
                    self.collect_info_key(
                        result.job_id.as_str(),
                        result.batch_id.as_str(),
                        collect_info_result.collect_kind,
                    ),
                    &collect_info,
                    "apply transfer worker result collect info update",
                )
                .await
            );
        }
        let commit_start = std::time::Instant::now();
        tx.commit().await.map_err(|e| {
            format!(
                "commit apply transfer worker result job_id={} batch_id={} failed: {}",
                result.job_id, result.batch_id, e
            )
        })?;
        info!(
            "transfer tikv apply worker result committed: job_id={} batch_id={} worker_id={} worker_task_id={} file_result_count={} failed_file_result_count={} collect_info_result_count={} commit_elapsed_ms={} total_elapsed_ms={}",
            result.job_id,
            result.batch_id,
            result.worker_id,
            result.worker_task_id,
            result.file_results.len(),
            result.failed_file_results.len(),
            result.collect_info_results.len(),
            commit_start.elapsed().as_millis(),
            total_start.elapsed().as_millis(),
        );
        Ok(FluxonFsTransferWorkerResultAckWire::accepted())
    }

    async fn apply_transfer_worker_heartbeat(
        &self,
        heartbeat: &FluxonFsTransferWorkerHeartbeatWire,
        heartbeat_received_unix_ms: i64,
        heartbeat_lease_duration_ms: i64,
    ) -> Result<FluxonFsTransferWorkerHeartbeatResultWire, String> {
        let total_start = std::time::Instant::now();
        if heartbeat_lease_duration_ms < 0 {
            return Err(format!(
                "heartbeat lease duration must be non-negative: job_id={} batch_id={} worker_id={} duration_ms={}",
                heartbeat.job_id,
                heartbeat.assigned_batch_id,
                heartbeat.worker_id,
                heartbeat_lease_duration_ms
            ));
        }
        // Durable worker liveness must be fenced by the master/store local
        // receipt time, never by the remote worker-reported wall clock.
        let lease_expire_unix_ms =
            heartbeat_received_unix_ms.saturating_add(heartbeat_lease_duration_ms);
        let mut tx = self
            .begin_mutation_tx("apply transfer worker heartbeat")
            .await?;
        let Some(job) = tx_try!(
            tx,
            get_record::<FsTransferJobRecord>(
                &mut tx,
                self.job_key(heartbeat.job_id.as_str()),
                "worker heartbeat job lookup",
            )
            .await
        ) else {
            let err = format!("no transfer job found: job_id={}", heartbeat.job_id);
            return Err(rollback_with_error(&mut tx, err).await);
        };
        let stop_reason = transfer_worker_stop_reason_for_job_state(job.state);
        if job.state == FluxonFsTransferJobState::Stopping
            || job.state == FluxonFsTransferJobState::Cancelled
        {
            let _ = tx.rollback().await;
            let (mut tx, mut job) = self.begin_locked_job_tx(heartbeat.job_id.as_str()).await?;
            tx_try!(
                tx,
                self.stop_worker_for_stopping_job_locked_tx(
                    &mut tx,
                    &mut job,
                    heartbeat.job_id.as_str(),
                    heartbeat.assigned_batch_id.as_str(),
                    heartbeat.worker_id.as_str(),
                    heartbeat.worker_task_id.as_str(),
                    heartbeat_received_unix_ms,
                )
                .await
            );
            job.updated_at_unix_ms = heartbeat_received_unix_ms;
            tx_try!(
                tx,
                put_record(
                    &mut tx,
                    self.job_key(heartbeat.job_id.as_str()),
                    &job,
                    "apply transfer worker heartbeat stopping job update",
                )
                .await
            );
            tx.commit().await.map_err(|e| {
                format!(
                    "commit apply transfer worker heartbeat stop cleanup job_id={} batch_id={} worker_id={} failed: {}",
                    heartbeat.job_id, heartbeat.assigned_batch_id, heartbeat.worker_id, e
                )
            })?;
            return Ok(FluxonFsTransferWorkerHeartbeatResultWire::stop(
                FluxonFsTransferWorkerStopReasonWire::Cancelled,
            ));
        }
        if job.state != FluxonFsTransferJobState::Running {
            tx_try!(
                tx,
                delete_key(
                    &mut tx,
                    self.worker_lease_key(
                        heartbeat.job_id.as_str(),
                        heartbeat.worker_task_id.as_str()
                    ),
                    "delete transfer worker lease for non-running job heartbeat",
                )
                .await
            );
            tx_try!(
                tx,
                self.stop_worker_attempt_if_active_locked_tx(
                    &mut tx,
                    heartbeat.job_id.as_str(),
                    heartbeat.assigned_batch_id.as_str(),
                    heartbeat.worker_id.as_str(),
                    heartbeat.worker_task_id.as_str(),
                    stop_reason,
                    "worker heartbeat rejected because the job is no longer running",
                    heartbeat_received_unix_ms,
                    "worker heartbeat non-running attempt lookup",
                )
                .await
            );
            tx.commit().await.map_err(|e| {
                format!(
                    "commit apply transfer worker heartbeat non-running stop job_id={} batch_id={} worker_id={} failed: {}",
                    heartbeat.job_id, heartbeat.assigned_batch_id, heartbeat.worker_id, e
                )
            })?;
            return Ok(FluxonFsTransferWorkerHeartbeatResultWire::stop(stop_reason));
        }
        // Heartbeat only extends the durable lease for the current owner
        // attempt. Any ownership mismatch is treated as a stop immediately.
        let Some(mut batch) = tx_try!(
            tx,
            get_record_for_update::<FsTransferBatchRecord>(
                &mut tx,
                self.batch_key(
                    heartbeat.job_id.as_str(),
                    heartbeat.assigned_batch_id.as_str()
                ),
                "worker heartbeat batch lookup",
            )
            .await
        ) else {
            let worker_attempt_key = self
                .worker_attempt_key(heartbeat.job_id.as_str(), heartbeat.worker_task_id.as_str());
            if let Some(mut attempt) = tx_try!(
                tx,
                get_record_for_update::<FsTransferWorkerAttemptRecord>(
                    &mut tx,
                    worker_attempt_key.clone(),
                    "worker heartbeat attempt lookup before batch-missing stop",
                )
                .await
            ) {
                if attempt.worker_id == heartbeat.worker_id
                    && attempt.batch_id == heartbeat.assigned_batch_id
                    && attempt.state != FsTransferWorkerAttemptState::Finished
                    && attempt.state != FsTransferWorkerAttemptState::Stopped
                {
                    attempt.state = FsTransferWorkerAttemptState::Stopped;
                    attempt.stop_reason = Some(stop_reason);
                    attempt.last_error =
                        "worker heartbeat batch missing or no longer owned by this attempt"
                            .to_string();
                    attempt.updated_at_unix_ms = heartbeat_received_unix_ms;
                    tx_try!(
                        tx,
                        put_record(
                            &mut tx,
                            worker_attempt_key,
                            &attempt,
                            "apply transfer worker heartbeat batch-missing stop attempt update",
                        )
                        .await
                    );
                    tx.commit().await.map_err(|e| {
                        format!(
                            "commit apply transfer worker heartbeat batch-missing stop job_id={} batch_id={} worker_id={} failed: {}",
                            heartbeat.job_id, heartbeat.assigned_batch_id, heartbeat.worker_id, e
                        )
                    })?;
                } else {
                    let _ = tx.rollback().await;
                }
            } else {
                let _ = tx.rollback().await;
            }
            return Ok(FluxonFsTransferWorkerHeartbeatResultWire::stop(stop_reason));
        };
        if batch.state != FluxonFsTransferBatchState::Running
            || batch.owner_worker_id != heartbeat.worker_id
            || batch.owner_worker_task_id != heartbeat.worker_task_id
        {
            let worker_attempt_key = self
                .worker_attempt_key(heartbeat.job_id.as_str(), heartbeat.worker_task_id.as_str());
            if let Some(mut attempt) = tx_try!(
                tx,
                get_record_for_update::<FsTransferWorkerAttemptRecord>(
                    &mut tx,
                    worker_attempt_key.clone(),
                    "worker heartbeat attempt lookup before ownership-mismatch stop",
                )
                .await
            ) {
                if attempt.worker_id == heartbeat.worker_id
                    && attempt.batch_id == heartbeat.assigned_batch_id
                    && attempt.state != FsTransferWorkerAttemptState::Finished
                    && attempt.state != FsTransferWorkerAttemptState::Stopped
                {
                    attempt.state = FsTransferWorkerAttemptState::Stopped;
                    attempt.stop_reason = Some(stop_reason);
                    attempt.last_error =
                        "worker heartbeat ownership mismatch; attempt superseded by newer owner"
                            .to_string();
                    attempt.updated_at_unix_ms = heartbeat_received_unix_ms;
                    tx_try!(
                        tx,
                        put_record(
                            &mut tx,
                            worker_attempt_key,
                            &attempt,
                            "apply transfer worker heartbeat ownership-mismatch stop attempt update",
                        )
                        .await
                    );
                    tx.commit().await.map_err(|e| {
                        format!(
                            "commit apply transfer worker heartbeat ownership-mismatch stop job_id={} batch_id={} worker_id={} failed: {}",
                            heartbeat.job_id, heartbeat.assigned_batch_id, heartbeat.worker_id, e
                        )
                    })?;
                } else {
                    let _ = tx.rollback().await;
                }
            } else {
                let _ = tx.rollback().await;
            }
            return Ok(FluxonFsTransferWorkerHeartbeatResultWire::stop(stop_reason));
        }
        let Some(mut worker_lease) = tx_try!(
            tx,
            get_record_for_update::<FsTransferWorkerLeaseRecord>(
                &mut tx,
                self.worker_lease_key(heartbeat.job_id.as_str(), heartbeat.worker_task_id.as_str()),
                "worker heartbeat lease lookup",
            )
            .await
        ) else {
            let worker_attempt_key = self
                .worker_attempt_key(heartbeat.job_id.as_str(), heartbeat.worker_task_id.as_str());
            if let Some(mut attempt) = tx_try!(
                tx,
                get_record_for_update::<FsTransferWorkerAttemptRecord>(
                    &mut tx,
                    worker_attempt_key.clone(),
                    "worker heartbeat attempt lookup before lease-missing stop",
                )
                .await
            ) {
                if attempt.worker_id == heartbeat.worker_id
                    && attempt.batch_id == heartbeat.assigned_batch_id
                    && attempt.state != FsTransferWorkerAttemptState::Finished
                    && attempt.state != FsTransferWorkerAttemptState::Stopped
                {
                    attempt.state = FsTransferWorkerAttemptState::Stopped;
                    attempt.stop_reason = Some(stop_reason);
                    attempt.last_error =
                        "worker heartbeat lease missing; attempt superseded by newer owner"
                            .to_string();
                    attempt.updated_at_unix_ms = heartbeat_received_unix_ms;
                    tx_try!(
                        tx,
                        put_record(
                            &mut tx,
                            worker_attempt_key,
                            &attempt,
                            "apply transfer worker heartbeat lease-missing stop attempt update",
                        )
                        .await
                    );
                    tx.commit().await.map_err(|e| {
                        format!(
                            "commit apply transfer worker heartbeat lease-missing stop job_id={} batch_id={} worker_id={} failed: {}",
                            heartbeat.job_id, heartbeat.assigned_batch_id, heartbeat.worker_id, e
                        )
                    })?;
                } else {
                    let _ = tx.rollback().await;
                }
            } else {
                let _ = tx.rollback().await;
            }
            return Ok(FluxonFsTransferWorkerHeartbeatResultWire::stop(stop_reason));
        };
        if worker_lease.worker_id != heartbeat.worker_id
            || worker_lease.assigned_batch_id != heartbeat.assigned_batch_id
        {
            let worker_attempt_key = self
                .worker_attempt_key(heartbeat.job_id.as_str(), heartbeat.worker_task_id.as_str());
            if let Some(mut attempt) = tx_try!(
                tx,
                get_record_for_update::<FsTransferWorkerAttemptRecord>(
                    &mut tx,
                    worker_attempt_key.clone(),
                    "worker heartbeat attempt lookup before lease-mismatch stop",
                )
                .await
            ) {
                if attempt.worker_id == heartbeat.worker_id
                    && attempt.batch_id == heartbeat.assigned_batch_id
                    && attempt.state != FsTransferWorkerAttemptState::Finished
                    && attempt.state != FsTransferWorkerAttemptState::Stopped
                {
                    attempt.state = FsTransferWorkerAttemptState::Stopped;
                    attempt.stop_reason = Some(stop_reason);
                    attempt.last_error =
                        "worker heartbeat lease mismatch; attempt superseded by newer owner"
                            .to_string();
                    attempt.updated_at_unix_ms = heartbeat_received_unix_ms;
                    tx_try!(
                        tx,
                        put_record(
                            &mut tx,
                            worker_attempt_key,
                            &attempt,
                            "apply transfer worker heartbeat lease-mismatch stop attempt update",
                        )
                        .await
                    );
                    tx.commit().await.map_err(|e| {
                        format!(
                            "commit apply transfer worker heartbeat lease-mismatch stop job_id={} batch_id={} worker_id={} failed: {}",
                            heartbeat.job_id, heartbeat.assigned_batch_id, heartbeat.worker_id, e
                        )
                    })?;
                } else {
                    let _ = tx.rollback().await;
                }
            } else {
                let _ = tx.rollback().await;
            }
            return Ok(FluxonFsTransferWorkerHeartbeatResultWire::stop(stop_reason));
        }
        worker_lease.lease_expire_unix_ms = lease_expire_unix_ms;
        batch.lease_expire_unix_ms = lease_expire_unix_ms;
        tx_try!(
            tx,
            put_record(
                &mut tx,
                self.batch_key(
                    heartbeat.job_id.as_str(),
                    heartbeat.assigned_batch_id.as_str()
                ),
                &batch,
                "apply transfer worker heartbeat batch lease update",
            )
            .await
        );
        tx_try!(
            tx,
            put_record(
                &mut tx,
                self.worker_lease_key(heartbeat.job_id.as_str(), heartbeat.worker_task_id.as_str()),
                &worker_lease,
                "apply transfer worker heartbeat lease update",
            )
            .await
        );
        let commit_start = std::time::Instant::now();
        tx.commit().await.map_err(|e| {
            format!(
                "commit apply transfer worker heartbeat job_id={} batch_id={} worker_id={} failed: {}",
                heartbeat.job_id, heartbeat.assigned_batch_id, heartbeat.worker_id, e
            )
        })?;
        info!(
            "transfer tikv apply heartbeat committed: job_id={} batch_id={} worker_id={} worker_task_id={} lease_expire_unix_ms={} commit_elapsed_ms={} total_elapsed_ms={}",
            heartbeat.job_id,
            heartbeat.assigned_batch_id,
            heartbeat.worker_id,
            heartbeat.worker_task_id,
            lease_expire_unix_ms,
            commit_start.elapsed().as_millis(),
            total_start.elapsed().as_millis(),
        );
        Ok(FluxonFsTransferWorkerHeartbeatResultWire::continue_running(
            lease_expire_unix_ms,
        ))
    }

    async fn reconcile_transfer_scheduler_state(
        &self,
        backend: Arc<dyn FsS3Backend>,
        _fs_cache: Arc<FluxonFsGlobalConfig>,
        now_unix_ms: i64,
    ) -> Result<(), String> {
        // Reconcile is the convergence pass after crashes, missed RPCs, or late
        // worker exits. It is the only place that can finish batches/jobs by
        // observing the destination side instead of trusting transient memory.
        let jobs = self.load_transfer_job_records().await?;
        for job in jobs {
            if job.state != FluxonFsTransferJobState::Running
                && job.state != FluxonFsTransferJobState::Stopping
            {
                continue;
            }
            self.reconcile_single_job(job.job_id.as_str(), backend.clone(), now_unix_ms)
                .await?;
        }
        Ok(())
    }

    async fn load_done_batch_snapshot(
        &self,
        job_id: &str,
        batch_id: &str,
    ) -> Result<Option<ReconcileDoneBatchSnapshot>, String> {
        let mut tx = self
            .client
            .begin_optimistic()
            .await
            .map_err(|e| format!("begin load done batch snapshot transaction failed: {}", e))?;
        let Some(batch) = (match get_record::<FsTransferBatchRecord>(
            &mut tx,
            self.batch_key(job_id, batch_id),
            "load done batch snapshot batch lookup",
        )
        .await
        {
            Ok(v) => v,
            Err(err) => return Err(rollback_with_error(&mut tx, err).await),
        }) else {
            let _ = tx.rollback().await;
            return Ok(None);
        };
        if batch.state != FluxonFsTransferBatchState::Done {
            let _ = tx.rollback().await;
            return Ok(None);
        }
        let manifest = match decode_transfer_manifest_blob(batch.manifest_blob.as_slice()) {
            Ok(v) => v,
            Err(err) => return Err(rollback_with_error(&mut tx, err).await),
        };
        let file_issue_relpaths = match self
            .load_file_issues_for_batch_from_tx(&mut tx, job_id, batch.batch_id.as_str())
            .await
        {
            Ok(v) => v
                .into_iter()
                .map(|issue| issue.relpath)
                .collect::<std::collections::BTreeSet<_>>(),
            Err(err) => return Err(rollback_with_error(&mut tx, err).await),
        };
        let collect_infos = match self
            .load_collect_infos_for_batch_from_tx(&mut tx, job_id, batch.batch_id.as_str())
            .await
        {
            Ok(v) => v,
            Err(err) => return Err(rollback_with_error(&mut tx, err).await),
        };
        let _ = tx.rollback().await;
        Ok(Some(ReconcileDoneBatchSnapshot {
            batch,
            manifest,
            file_issue_relpaths,
            collect_infos,
        }))
    }

    async fn evaluate_done_batch_target_state(
        &self,
        job: &FsTransferJobRecord,
        snapshot: &ReconcileDoneBatchSnapshot,
        backend: Arc<dyn FsS3Backend>,
    ) -> Result<
        (
            ReconcileDoneBatchTargetState,
            Vec<FsTransferBatchCollectInfoRecord>,
        ),
        String,
    > {
        if snapshot.batch.assigned_dst_exporter_id.trim().is_empty() {
            return Err(format!(
                "done batch target reconcile requires assigned_dst_exporter_id: job_id={} batch_id={}",
                job.job_id, snapshot.batch.batch_id
            ));
        }
        let mut target_complete = true;
        for entry in &snapshot.manifest.entries {
            if snapshot
                .file_issue_relpaths
                .contains(entry.relpath.as_str())
            {
                continue;
            }
            let final_relpath =
                join_transfer_root_relpath(job.dst_root_relpath.as_str(), entry.relpath.as_str());
            let stat = backend
                .stat_on_exporter(
                    fluxon_fs_core::config::FluxonFsRequestIdentity {
                        username: String::new(),
                        password: String::new(),
                    },
                    Arc::from(snapshot.batch.assigned_dst_exporter_id.as_str()),
                    Arc::from(job.dst_export.as_str()),
                    Arc::from(final_relpath.as_str()),
                )
                .await
                .map_err(|e| {
                    format!(
                        "target reconcile stat failed: job_id={} batch_id={} relpath={} err={:?}",
                        job.job_id, snapshot.batch.batch_id, final_relpath, e
                    )
                })?;
            if !stat.exists || !stat.is_file || stat.size != entry.size {
                target_complete = false;
                break;
            }
        }
        if target_complete {
            for empty_dir_relpath in &snapshot.manifest.empty_dir_relpaths {
                let final_relpath = join_transfer_root_relpath(
                    job.dst_root_relpath.as_str(),
                    empty_dir_relpath.as_str(),
                );
                let stat = backend
                    .stat_on_exporter(
                        fluxon_fs_core::config::FluxonFsRequestIdentity {
                            username: String::new(),
                            password: String::new(),
                        },
                        Arc::from(snapshot.batch.assigned_dst_exporter_id.as_str()),
                        Arc::from(job.dst_export.as_str()),
                        Arc::from(final_relpath.as_str()),
                    )
                    .await
                    .map_err(|e| {
                        format!(
                            "target reconcile empty dir stat failed: job_id={} batch_id={} relpath={} err={:?}",
                            job.job_id, snapshot.batch.batch_id, final_relpath, e
                        )
                    })?;
                if !stat.exists || !stat.is_dir {
                    target_complete = false;
                    break;
                }
            }
        }
        let mut next_collect_infos = snapshot.collect_infos.clone();
        let mut collect_info_complete = true;
        for collect_info in &mut next_collect_infos {
            let collect_relpath = transfer_collect_info_output_relpath(
                snapshot.batch.batch_id.as_str(),
                collect_info.collect_kind,
            )?;
            let final_relpath =
                join_transfer_root_relpath(job.dst_root_relpath.as_str(), collect_relpath.as_str());
            let stat = backend
                .stat_on_exporter(
                    fluxon_fs_core::config::FluxonFsRequestIdentity {
                        username: String::new(),
                        password: String::new(),
                    },
                    Arc::from(snapshot.batch.assigned_dst_exporter_id.as_str()),
                    Arc::from(job.dst_export.as_str()),
                    Arc::from(final_relpath.as_str()),
                )
                .await
                .map_err(|e| {
                    format!(
                        "target reconcile collect info stat failed: job_id={} batch_id={} collect_kind={} relpath={} err={:?}",
                        job.job_id,
                        snapshot.batch.batch_id,
                        collect_info.collect_kind.as_db_str(),
                        final_relpath,
                        e
                    )
                })?;
            let exists =
                stat.exists && stat.is_file && stat.size == collect_info.collect_blob.len() as i64;
            collect_info.materialized = exists;
            if !exists {
                collect_info_complete = false;
            }
        }
        target_complete = target_complete && collect_info_complete;
        let next_state = if target_complete {
            ReconcileDoneBatchTargetState::Finished
        } else {
            ReconcileDoneBatchTargetState::Ready
        };
        Ok((next_state, next_collect_infos))
    }

    async fn reconcile_single_job(
        &self,
        job_id: &str,
        backend: Arc<dyn FsS3Backend>,
        now_unix_ms: i64,
    ) -> Result<(), String> {
        let (mut tx, mut job) = self.begin_locked_job_tx(job_id).await?;
        if job.state == FluxonFsTransferJobState::Stopping {
            tx_try!(
                tx,
                self.reconcile_stopping_job_locked_tx(&mut tx, &mut job, now_unix_ms)
                    .await
            );
            tx.commit().await.map_err(|e| {
                format!(
                    "commit reconcile stopping transfer job job_id={} failed: {}",
                    job_id, e
                )
            })?;
            return Ok(());
        }
        if job.state != FluxonFsTransferJobState::Running {
            let _ = tx.rollback().await;
            return Ok(());
        }
        // Job row is the coarse authority lock for all durable transitions
        // within one transfer job.
        let mut tx_dirty = false;
        let mut job_dirty = false;
        let worker_leases_by_task = tx_try!(
            tx,
            self.load_worker_leases_for_job_from_tx(&mut tx, job_id)
                .await
        )
        .into_iter()
        .map(|row| (row.worker_task_id.clone(), row))
        .collect::<std::collections::BTreeMap<String, FsTransferWorkerLeaseRecord>>();
        let batches = tx_try!(tx, self.load_batches_for_job_from_tx(&mut tx, job_id).await);
        let mut deferred_done_batches: Vec<FsTransferBatchRecord> = Vec::new();
        for mut batch in batches {
            if batch.state == FluxonFsTransferBatchState::Expired {
                tx_try!(
                    tx,
                    delete_key(
                        &mut tx,
                        self.batch_state_key(
                            job_id,
                            FluxonFsTransferBatchState::Expired,
                            batch.batch_id.as_str()
                        ),
                        "delete running batch state index during reconcile requeue",
                    )
                    .await
                );
                Self::adjust_job_batch_state_count(
                    &mut job,
                    FluxonFsTransferBatchState::Expired,
                    -1,
                );
                batch.state = FluxonFsTransferBatchState::Ready;
                batch.owner_worker_id.clear();
                batch.owner_worker_task_id.clear();
                batch.lease_expire_unix_ms = 0;
                tx_try!(
                    tx,
                    put_record(
                        &mut tx,
                        self.batch_key(job_id, batch.batch_id.as_str()),
                        &batch,
                        "requeue expired transfer batch",
                    )
                    .await
                );
                tx_try!(
                    tx,
                    put_marker(
                        &mut tx,
                        self.batch_state_key(
                            job_id,
                            FluxonFsTransferBatchState::Ready,
                            batch.batch_id.as_str()
                        ),
                        "insert ready batch state index during reconcile requeue",
                    )
                    .await
                );
                Self::adjust_job_batch_state_count(&mut job, FluxonFsTransferBatchState::Ready, 1);
                tx_dirty = true;
                job_dirty = true;
                continue;
            }
            if batch.state != FluxonFsTransferBatchState::Running {
                if batch.state != FluxonFsTransferBatchState::Done {
                    continue;
                }
                deferred_done_batches.push(batch);
                continue;
            }
            if batch.state == FluxonFsTransferBatchState::Running {
                if !transfer_batch_owner_is_consistent(&batch) {
                    tx_try!(
                        tx,
                        delete_key(
                            &mut tx,
                            self.batch_state_key(
                                job_id,
                                FluxonFsTransferBatchState::Running,
                                batch.batch_id.as_str()
                            ),
                            "delete running batch state index during inconsistent-owner requeue",
                        )
                        .await
                    );
                    Self::adjust_job_batch_state_count(
                        &mut job,
                        FluxonFsTransferBatchState::Running,
                        -1,
                    );
                    batch.state = FluxonFsTransferBatchState::Ready;
                    batch.owner_worker_id.clear();
                    batch.owner_worker_task_id.clear();
                    batch.lease_expire_unix_ms = 0;
                    tx_try!(
                        tx,
                        put_record(
                            &mut tx,
                            self.batch_key(job_id, batch.batch_id.as_str()),
                            &batch,
                            "requeue inconsistent-owner transfer batch",
                        )
                        .await
                    );
                    tx_try!(
                        tx,
                        put_marker(
                            &mut tx,
                            self.batch_state_key(
                                job_id,
                                FluxonFsTransferBatchState::Ready,
                                batch.batch_id.as_str()
                            ),
                            "insert ready batch state index during inconsistent-owner reconcile",
                        )
                        .await
                    );
                    Self::adjust_job_batch_state_count(
                        &mut job,
                        FluxonFsTransferBatchState::Ready,
                        1,
                    );
                    tx_dirty = true;
                    job_dirty = true;
                    continue;
                }
                if !transfer_batch_owner_is_empty(&batch) {
                    // Running batch with a missing or stale lease is returned to
                    // Ready so a fresh worker attempt can be launched.
                    let lease = worker_leases_by_task.get(batch.owner_worker_task_id.as_str());
                    if lease.is_none() && batch.lease_expire_unix_ms > now_unix_ms {
                        continue;
                    }
                    let lease_is_live = lease
                        .map(|row| {
                            row.worker_id == batch.owner_worker_id
                                && row.assigned_batch_id == batch.batch_id
                                && row.lease_expire_unix_ms > now_unix_ms
                        })
                        .unwrap_or(false);
                    if lease_is_live {
                        continue;
                    }
                    let old_owner_worker_id = batch.owner_worker_id.clone();
                    let old_owner_worker_task_id = batch.owner_worker_task_id.clone();
                    tx_try!(
                        tx,
                        delete_key(
                            &mut tx,
                            self.batch_state_key(
                                job_id,
                                FluxonFsTransferBatchState::Running,
                                batch.batch_id.as_str()
                            ),
                            "delete running batch state index during stale-worker requeue",
                        )
                        .await
                    );
                    Self::adjust_job_batch_state_count(
                        &mut job,
                        FluxonFsTransferBatchState::Running,
                        -1,
                    );
                    if lease.is_some() {
                        tx_try!(
                            tx,
                            delete_key(
                                &mut tx,
                                self.worker_lease_key(job_id, batch.owner_worker_task_id.as_str()),
                                "delete stale transfer worker lease during reconcile requeue",
                            )
                            .await
                        );
                    }
                    if let Some(mut attempt) = tx_try!(
                        tx,
                        get_record::<FsTransferWorkerAttemptRecord>(
                            &mut tx,
                            self.worker_attempt_key(job_id, old_owner_worker_task_id.as_str()),
                            "reconcile stale-worker worker attempt lookup",
                        )
                        .await
                    ) {
                        if attempt.batch_id == batch.batch_id
                            && attempt.worker_id == old_owner_worker_id
                            && attempt.state != FsTransferWorkerAttemptState::Finished
                            && attempt.state != FsTransferWorkerAttemptState::Stopped
                        {
                            attempt.state = FsTransferWorkerAttemptState::Stopped;
                            attempt.stop_reason =
                                Some(FluxonFsTransferWorkerStopReasonWire::Superseded);
                            attempt.last_error =
                                "reconcile revoked stale worker ownership and requeued batch"
                                    .to_string();
                            attempt.updated_at_unix_ms = now_unix_ms;
                            tx_try!(
                                tx,
                                put_record(
                                    &mut tx,
                                    self.worker_attempt_key(
                                        job_id,
                                        old_owner_worker_task_id.as_str()
                                    ),
                                    &attempt,
                                    "reconcile stale-worker worker attempt stop update",
                                )
                                .await
                            );
                        }
                    }
                    batch.state = FluxonFsTransferBatchState::Ready;
                    batch.owner_worker_id.clear();
                    batch.owner_worker_task_id.clear();
                    batch.lease_expire_unix_ms = 0;
                    tx_try!(
                        tx,
                        put_record(
                            &mut tx,
                            self.batch_key(job_id, batch.batch_id.as_str()),
                            &batch,
                            "requeue stale-worker transfer batch",
                        )
                        .await
                    );
                    tx_try!(
                        tx,
                        put_marker(
                            &mut tx,
                            self.batch_state_key(
                                job_id,
                                FluxonFsTransferBatchState::Ready,
                                batch.batch_id.as_str()
                            ),
                            "insert ready batch state index during stale-worker reconcile",
                        )
                        .await
                    );
                    Self::adjust_job_batch_state_count(
                        &mut job,
                        FluxonFsTransferBatchState::Ready,
                        1,
                    );
                    tx_dirty = true;
                    job_dirty = true;
                    continue;
                }
            }
        }

        let open_batches = Self::transfer_job_open_batch_count(&job);
        if job.scan_finished && open_batches == 0 {
            job.state = FluxonFsTransferJobState::Completed;
            job.last_error.clear();
            job_dirty = true;
        }
        if job_dirty {
            job.updated_at_unix_ms = now_unix_ms;
            tx_dirty = true;
        }
        // English note: the no-op fast path must not bypass deferred Done ->
        // Finished reconciliation. That second phase intentionally runs after
        // the coarse job transaction releases the job row, so the presence of
        // deferred_done_batches is itself meaningful work for this reconcile pass.
        if !tx_dirty {
            let _ = tx.rollback().await;
            if deferred_done_batches.is_empty() {
                return Ok(());
            }
        } else {
            if job_dirty {
                tx_try!(
                    tx,
                    put_record(
                        &mut tx,
                        self.job_key(job_id),
                        &job,
                        "reconcile transfer job update",
                    )
                    .await
                );
            }
            tx.commit().await.map_err(|e| {
                format!(
                    "commit reconcile transfer scheduler state job_id={} failed: {}",
                    job_id, e
                )
            })?;
        }
        for batch in deferred_done_batches {
            let current_job = self
                .load_transfer_job_record(job_id)
                .await?
                .ok_or_else(|| {
                    format!(
                        "no transfer job found during reconcile done phase: job_id={}",
                        job_id
                    )
                })?;
            if current_job.state != FluxonFsTransferJobState::Running {
                continue;
            }
            let Some(done_snapshot) = self
                .load_done_batch_snapshot(job_id, batch.batch_id.as_str())
                .await?
            else {
                continue;
            };
            let (next_target_state, next_collect_infos) = self
                .evaluate_done_batch_target_state(&current_job, &done_snapshot, backend.clone())
                .await?;
            let (mut tx, mut job) = self.begin_locked_job_tx(job_id).await?;
            if job.state != FluxonFsTransferJobState::Running {
                let _ = tx.rollback().await;
                continue;
            }
            let Some(mut current_batch) = get_record::<FsTransferBatchRecord>(
                &mut tx,
                self.batch_key(job_id, batch.batch_id.as_str()),
                "reconcile done batch current batch lookup",
            )
            .await?
            else {
                let _ = tx.rollback().await;
                continue;
            };
            if current_batch.state != FluxonFsTransferBatchState::Done {
                let _ = tx.rollback().await;
                continue;
            }
            for collect_info in next_collect_infos {
                let mut next_collect_info = collect_info;
                next_collect_info.materialized_at_unix_ms = if next_collect_info.materialized {
                    if next_collect_info.materialized_at_unix_ms > 0 {
                        next_collect_info.materialized_at_unix_ms
                    } else {
                        now_unix_ms
                    }
                } else {
                    0
                };
                tx_try!(
                    tx,
                    put_record(
                        &mut tx,
                        self.collect_info_key(
                            job_id,
                            current_batch.batch_id.as_str(),
                            next_collect_info.collect_kind,
                        ),
                        &next_collect_info,
                        "reconcile collect info materialized state",
                    )
                    .await
                );
            }
            tx_try!(
                tx,
                delete_key(
                    &mut tx,
                    self.batch_state_key(
                        job_id,
                        FluxonFsTransferBatchState::Done,
                        current_batch.batch_id.as_str()
                    ),
                    "delete done batch state index during reconcile finish",
                )
                .await
            );
            Self::adjust_job_batch_state_count(&mut job, FluxonFsTransferBatchState::Done, -1);
            current_batch.state = match next_target_state {
                ReconcileDoneBatchTargetState::Ready => FluxonFsTransferBatchState::Ready,
                ReconcileDoneBatchTargetState::Finished => FluxonFsTransferBatchState::Finished,
            };
            current_batch.owner_worker_id.clear();
            current_batch.owner_worker_task_id.clear();
            current_batch.lease_expire_unix_ms = 0;
            tx_try!(
                tx,
                put_record(
                    &mut tx,
                    self.batch_key(job_id, current_batch.batch_id.as_str()),
                    &current_batch,
                    "reconcile transfer batch state",
                )
                .await
            );
            tx_try!(
                tx,
                put_marker(
                    &mut tx,
                    self.batch_state_key(
                        job_id,
                        current_batch.state,
                        current_batch.batch_id.as_str()
                    ),
                    "insert reconciled batch state index",
                )
                .await
            );
            Self::adjust_job_batch_state_count(&mut job, current_batch.state, 1);
            job.updated_at_unix_ms = now_unix_ms;
            let open_batches = Self::transfer_job_open_batch_count(&job);
            if job.scan_finished && open_batches == 0 {
                job.state = FluxonFsTransferJobState::Completed;
                job.last_error.clear();
                job.updated_at_unix_ms = now_unix_ms;
            }
            tx_try!(
                tx,
                put_record(
                    &mut tx,
                    self.job_key(job_id),
                    &job,
                    "reconcile transfer job update after done batch phase",
                )
                .await
            );
            tx.commit().await.map_err(|e| {
                format!(
                    "commit reconcile done batch phase job_id={} batch_id={} failed: {}",
                    job_id, current_batch.batch_id, e
                )
            })?;
        }
        Ok(())
    }

    async fn begin_locked_job_tx(
        &self,
        job_id: &str,
    ) -> Result<(Transaction, FsTransferJobRecord), String> {
        // The job record is the authority root for transfer state. Locking it
        // first serializes all same-job transitions.
        let mut tx = self
            .client
            .begin_pessimistic()
            .await
            .map_err(|e| format!("begin transfer state transaction failed: {}", e))?;
        let Some(job) = tx_try!(
            tx,
            get_record_for_update::<FsTransferJobRecord>(
                &mut tx,
                self.job_key(job_id),
                "lock transfer job",
            )
            .await
        ) else {
            let err = format!("no transfer job found: job_id={}", job_id);
            return Err(rollback_with_error(&mut tx, err).await);
        };
        Ok((tx, job))
    }

    async fn insert_or_reuse_batch_locked_tx(
        &self,
        tx: &mut Transaction,
        job: &mut FsTransferJobRecord,
        batch: &FluxonFsTransferScanBatchWire,
    ) -> Result<(i64, i64, i64), String> {
        // Equivalent batch key is the durable substitute for persisting scan
        // units. The same committed coverage must map to exactly one manifest
        // and collect-info payload even after timeout generation bumps.
        let incoming_manifest = decode_transfer_manifest_blob(batch.manifest_blob.as_slice())?;
        let normalized_root = normalize_transfer_root_relpath(batch.root_relpath.as_str())?;
        let eq_key = self.batch_eq_key(job.job_id.as_str(), normalized_root.as_str(), batch);
        let existing_batch_id = tx
            .get(eq_key.clone())
            .await
            .map_err(|e| {
                format!(
                    "query equivalent transfer batch job_id={} root_relpath={} batch_kind={} failed: {}",
                    job.job_id,
                    normalized_root,
                    batch.batch_kind.as_db_str(),
                    e
                )
            })?
            .map(|v| String::from_utf8(v).map_err(|e| format!("decode batch_eq batch_id failed: {}", e)))
            .transpose()?;
        if let Some(existing_batch_id) = existing_batch_id.as_ref() {
            let existing_batch = get_required_record::<FsTransferBatchRecord>(
                tx,
                self.batch_key(job.job_id.as_str(), existing_batch_id.as_str()),
                "equivalent transfer batch lookup",
            )
            .await?;
            let existing_manifest =
                decode_transfer_manifest_blob(existing_batch.manifest_blob.as_slice())?;
            validate_equivalent_manifests(
                job.job_id.as_str(),
                existing_batch_id.as_str(),
                normalized_root.as_str(),
                batch.batch_kind,
                &existing_manifest,
                &incoming_manifest,
            )?;
            self.verify_equivalent_batch_collect_infos(
                tx,
                job.job_id.as_str(),
                existing_batch_id.as_str(),
                &batch.collect_infos,
            )
            .await?;
            if existing_batch.last_counted_scan_epoch == job.scan_epoch {
                return Ok((0, 0, 0));
            }
            let mut updated_batch = existing_batch;
            updated_batch.last_counted_scan_epoch = job.scan_epoch;
            put_record(
                tx,
                self.batch_key(job.job_id.as_str(), existing_batch_id.as_str()),
                &updated_batch,
                "update transfer batch counted scan epoch",
            )
            .await?;
            return Ok(transfer_manifest_stats(&incoming_manifest));
        }
        let batch_id = batch.batch_id.clone();
        let (manifest_to_store, collect_infos_to_store, claims) = match batch.batch_kind {
            FluxonFsTransferBatchKind::DirectFilesOnly => {
                let Some(filtered) = self
                    .filter_path_claimed_batch_locked_tx(
                        tx,
                        job,
                        batch_id.as_str(),
                        normalized_root.as_str(),
                        &incoming_manifest,
                        &batch.collect_infos,
                        FluxonFsTransferBatchKind::DirectFilesOnly,
                        "direct_files_only",
                    )
                    .await?
                else {
                    return Ok((0, 0, 0));
                };
                (filtered.manifest, filtered.collect_infos, filtered.claims)
            }
            FluxonFsTransferBatchKind::SubtreeSlice => {
                let Some(filtered) = self
                    .filter_path_claimed_batch_locked_tx(
                        tx,
                        job,
                        batch_id.as_str(),
                        normalized_root.as_str(),
                        &incoming_manifest,
                        &batch.collect_infos,
                        FluxonFsTransferBatchKind::SubtreeSlice,
                        "subtree_slice",
                    )
                    .await?
                else {
                    return Ok((0, 0, 0));
                };
                (filtered.manifest, filtered.collect_infos, filtered.claims)
            }
            FluxonFsTransferBatchKind::FullDir => {
                let claims = self
                    .validate_no_coverage_overlap_for_full_dir_batch_locked_tx(
                        tx,
                        job,
                        normalized_root.as_str(),
                        &incoming_manifest,
                        &batch.collect_infos,
                    )
                    .await?;
                (
                    incoming_manifest.clone(),
                    batch.collect_infos.clone(),
                    claims,
                )
            }
        };
        let batch_stats = transfer_manifest_stats(&manifest_to_store);
        let manifest_blob = manifest_to_store.encode_to_blob()?;
        let record = FsTransferBatchRecord {
            job_id: job.job_id.clone(),
            batch_id: batch_id.clone(),
            root_relpath: normalized_root,
            batch_kind: batch.batch_kind,
            state: FluxonFsTransferBatchState::Ready,
            assigned_src_exporter_id: String::new(),
            assigned_dst_exporter_id: String::new(),
            owner_worker_id: String::new(),
            owner_worker_task_id: String::new(),
            lease_expire_unix_ms: 0,
            manifest_blob,
            generation: batch.generation,
            last_counted_scan_epoch: job.scan_epoch,
        };
        put_record(
            tx,
            self.batch_key(job.job_id.as_str(), batch_id.as_str()),
            &record,
            "insert transfer batch",
        )
        .await?;
        tx.put(eq_key, batch_id.as_bytes().to_vec())
            .await
            .map_err(|e| {
                format!(
                    "insert transfer batch eq index job_id={} batch_id={} failed: {}",
                    job.job_id, batch_id, e
                )
            })?;
        put_marker(
            tx,
            self.batch_state_key(
                job.job_id.as_str(),
                FluxonFsTransferBatchState::Ready,
                batch_id.as_str(),
            ),
            "insert transfer batch ready state index",
        )
        .await?;
        Self::adjust_job_batch_state_count(job, FluxonFsTransferBatchState::Ready, 1);
        for collect_info in &collect_infos_to_store {
            let collect_record = FsTransferBatchCollectInfoRecord {
                job_id: job.job_id.clone(),
                batch_id: batch_id.clone(),
                collect_kind: collect_info.collect_kind,
                collect_blob: collect_info.collect_blob.clone(),
                materialized: false,
                materialized_at_unix_ms: 0,
            };
            put_record(
                tx,
                self.collect_info_key(
                    job.job_id.as_str(),
                    batch_id.as_str(),
                    collect_info.collect_kind,
                ),
                &collect_record,
                "insert transfer batch collect info",
            )
            .await?;
        }
        self.insert_coverage_claims_locked_tx(tx, claims.as_slice(), batch_id.as_str())
            .await?;
        Ok(batch_stats)
    }

    async fn load_coverage_claims_for_relpaths_from_tx(
        &self,
        tx: &mut Transaction,
        job_id: &str,
        relpaths: &[String],
    ) -> Result<std::collections::BTreeMap<String, TransferCoverageClaimRecord>, String> {
        if relpaths.is_empty() {
            return Ok(std::collections::BTreeMap::new());
        }
        let pairs = tx
            .batch_get(
                relpaths
                    .iter()
                    .map(|relpath| self.coverage_claim_key(job_id, relpath.as_str())),
            )
            .await
            .map_err(|e| format!("batch get transfer coverage claims failed: {}", e))?;
        let mut out = std::collections::BTreeMap::new();
        for pair in pairs {
            let claim = decode_record::<TransferCoverageClaimRecord>(
                pair.1.as_slice(),
                "transfer coverage claim record",
            )?;
            out.insert(claim.relpath.clone(), claim);
        }
        Ok(out)
    }

    async fn filter_path_claimed_batch_locked_tx(
        &self,
        tx: &mut Transaction,
        job: &FsTransferJobRecord,
        batch_id: &str,
        normalized_root: &str,
        incoming_manifest: &FluxonFsTransferManifestWire,
        incoming_collect_infos: &[FluxonFsTransferBatchCollectInfoWire],
        expected_batch_kind: FluxonFsTransferBatchKind,
        overlap_label: &str,
    ) -> Result<Option<FilteredClaimedBatchMaterialization>, String> {
        let incoming_notices = decode_symlink_notices_from_collect_infos(incoming_collect_infos)?;
        let mut relpaths = incoming_manifest
            .entries
            .iter()
            .map(|entry| entry.relpath.clone())
            .collect::<Vec<_>>();
        relpaths.extend(incoming_notices.iter().map(|entry| entry.relpath.clone()));
        relpaths.extend(incoming_manifest.empty_dir_relpaths.iter().cloned());
        relpaths.sort();
        relpaths.dedup();
        let existing_claims = self
            .load_coverage_claims_for_relpaths_from_tx(tx, job.job_id.as_str(), relpaths.as_slice())
            .await?;
        let mut filtered_entries = Vec::new();
        let mut filtered_notices = Vec::new();
        let mut filtered_empty_dir_relpaths = Vec::new();
        let mut claims = Vec::new();
        for entry in &incoming_manifest.entries {
            let Some(existing_claim) = existing_claims.get(entry.relpath.as_str()) else {
                filtered_entries.push(entry.clone());
                claims.push(TransferCoverageClaimRecord {
                    job_id: job.job_id.clone(),
                    relpath: entry.relpath.clone(),
                    path_kind: TransferCoveragePathKind::File,
                    batch_id: batch_id.to_string(),
                    batch_root_relpath: normalized_root.to_string(),
                    batch_kind: expected_batch_kind,
                });
                continue;
            };
            if existing_claim.path_kind != TransferCoveragePathKind::File {
                return Err(format!(
                    "transfer coverage claim kind mismatch for direct file: job_id={} relpath={} existing_kind={:?}",
                    job.job_id, entry.relpath, existing_claim.path_kind
                ));
            }
            if existing_claim.batch_kind != expected_batch_kind
                || existing_claim.batch_root_relpath != normalized_root
            {
                return Err(format!(
                    "transfer {} file overlap rejected: job_id={} root_relpath={} relpath={} existing_batch_id={} existing_root_relpath={} existing_batch_kind={}",
                    overlap_label,
                    job.job_id,
                    normalized_root,
                    entry.relpath,
                    existing_claim.batch_id,
                    existing_claim.batch_root_relpath,
                    existing_claim.batch_kind.as_db_str()
                ));
            }
        }
        for notice in incoming_notices {
            let Some(existing_claim) = existing_claims.get(notice.relpath.as_str()) else {
                filtered_notices.push(notice.clone());
                claims.push(TransferCoverageClaimRecord {
                    job_id: job.job_id.clone(),
                    relpath: notice.relpath.clone(),
                    path_kind: TransferCoveragePathKind::SymlinkNotice,
                    batch_id: batch_id.to_string(),
                    batch_root_relpath: normalized_root.to_string(),
                    batch_kind: expected_batch_kind,
                });
                continue;
            };
            if existing_claim.path_kind != TransferCoveragePathKind::SymlinkNotice {
                return Err(format!(
                    "transfer coverage claim kind mismatch for symlink notice: job_id={} relpath={} existing_kind={:?}",
                    job.job_id, notice.relpath, existing_claim.path_kind
                ));
            }
            if existing_claim.batch_kind != expected_batch_kind
                || existing_claim.batch_root_relpath != normalized_root
            {
                return Err(format!(
                    "transfer {} symlink notice overlap rejected: job_id={} root_relpath={} relpath={} existing_batch_id={} existing_root_relpath={} existing_batch_kind={}",
                    overlap_label,
                    job.job_id,
                    normalized_root,
                    notice.relpath,
                    existing_claim.batch_id,
                    existing_claim.batch_root_relpath,
                    existing_claim.batch_kind.as_db_str()
                ));
            }
        }
        for empty_dir_relpath in &incoming_manifest.empty_dir_relpaths {
            let Some(existing_claim) = existing_claims.get(empty_dir_relpath.as_str()) else {
                filtered_empty_dir_relpaths.push(empty_dir_relpath.clone());
                claims.push(TransferCoverageClaimRecord {
                    job_id: job.job_id.clone(),
                    relpath: empty_dir_relpath.clone(),
                    path_kind: TransferCoveragePathKind::EmptyDir,
                    batch_id: batch_id.to_string(),
                    batch_root_relpath: normalized_root.to_string(),
                    batch_kind: expected_batch_kind,
                });
                continue;
            };
            if existing_claim.path_kind != TransferCoveragePathKind::EmptyDir {
                return Err(format!(
                    "transfer coverage claim kind mismatch for empty dir: job_id={} relpath={} existing_kind={:?}",
                    job.job_id, empty_dir_relpath, existing_claim.path_kind
                ));
            }
            if existing_claim.batch_kind != expected_batch_kind
                || existing_claim.batch_root_relpath != normalized_root
            {
                return Err(format!(
                    "transfer {} empty dir overlap rejected: job_id={} root_relpath={} relpath={} existing_batch_id={} existing_root_relpath={} existing_batch_kind={}",
                    overlap_label,
                    job.job_id,
                    normalized_root,
                    empty_dir_relpath,
                    existing_claim.batch_id,
                    existing_claim.batch_root_relpath,
                    existing_claim.batch_kind.as_db_str()
                ));
            }
        }
        if filtered_entries.is_empty()
            && filtered_notices.is_empty()
            && filtered_empty_dir_relpaths.is_empty()
        {
            return Ok(None);
        }
        Ok(Some(FilteredClaimedBatchMaterialization {
            manifest: FluxonFsTransferManifestWire::new(
                filtered_entries,
                filtered_empty_dir_relpaths,
            ),
            collect_infos: build_symlink_collect_infos(filtered_notices)?,
            claims,
        }))
    }

    async fn validate_no_coverage_overlap_for_full_dir_batch_locked_tx(
        &self,
        tx: &mut Transaction,
        job: &FsTransferJobRecord,
        normalized_root: &str,
        incoming_manifest: &FluxonFsTransferManifestWire,
        incoming_collect_infos: &[FluxonFsTransferBatchCollectInfoWire],
    ) -> Result<Vec<TransferCoverageClaimRecord>, String> {
        let incoming_notices = decode_symlink_notices_from_collect_infos(incoming_collect_infos)?;
        let mut relpaths = incoming_manifest
            .entries
            .iter()
            .map(|entry| entry.relpath.clone())
            .collect::<Vec<_>>();
        relpaths.extend(incoming_notices.iter().map(|entry| entry.relpath.clone()));
        relpaths.extend(incoming_manifest.empty_dir_relpaths.iter().cloned());
        relpaths.sort();
        relpaths.dedup();
        let existing_claims = self
            .load_coverage_claims_for_relpaths_from_tx(tx, job.job_id.as_str(), relpaths.as_slice())
            .await?;
        let mut claims = Vec::new();
        for entry in &incoming_manifest.entries {
            if let Some(existing_claim) = existing_claims.get(entry.relpath.as_str()) {
                return Err(format!(
                    "transfer full dir overlap rejected: job_id={} root_relpath={} relpath={} existing_batch_id={} existing_root_relpath={} existing_batch_kind={}",
                    job.job_id,
                    normalized_root,
                    entry.relpath,
                    existing_claim.batch_id,
                    existing_claim.batch_root_relpath,
                    existing_claim.batch_kind.as_db_str()
                ));
            }
            claims.push(TransferCoverageClaimRecord {
                job_id: job.job_id.clone(),
                relpath: entry.relpath.clone(),
                path_kind: TransferCoveragePathKind::File,
                batch_id: String::new(),
                batch_root_relpath: normalized_root.to_string(),
                batch_kind: FluxonFsTransferBatchKind::FullDir,
            });
        }
        for notice in incoming_notices {
            if let Some(existing_claim) = existing_claims.get(notice.relpath.as_str()) {
                return Err(format!(
                    "transfer full dir symlink notice overlap rejected: job_id={} root_relpath={} relpath={} existing_batch_id={} existing_root_relpath={} existing_batch_kind={}",
                    job.job_id,
                    normalized_root,
                    notice.relpath,
                    existing_claim.batch_id,
                    existing_claim.batch_root_relpath,
                    existing_claim.batch_kind.as_db_str()
                ));
            }
            claims.push(TransferCoverageClaimRecord {
                job_id: job.job_id.clone(),
                relpath: notice.relpath.clone(),
                path_kind: TransferCoveragePathKind::SymlinkNotice,
                batch_id: String::new(),
                batch_root_relpath: normalized_root.to_string(),
                batch_kind: FluxonFsTransferBatchKind::FullDir,
            });
        }
        for empty_dir_relpath in &incoming_manifest.empty_dir_relpaths {
            if let Some(existing_claim) = existing_claims.get(empty_dir_relpath.as_str()) {
                return Err(format!(
                    "transfer full dir empty dir overlap rejected: job_id={} root_relpath={} relpath={} existing_batch_id={} existing_root_relpath={} existing_batch_kind={}",
                    job.job_id,
                    normalized_root,
                    empty_dir_relpath,
                    existing_claim.batch_id,
                    existing_claim.batch_root_relpath,
                    existing_claim.batch_kind.as_db_str()
                ));
            }
            claims.push(TransferCoverageClaimRecord {
                job_id: job.job_id.clone(),
                relpath: empty_dir_relpath.clone(),
                path_kind: TransferCoveragePathKind::EmptyDir,
                batch_id: String::new(),
                batch_root_relpath: normalized_root.to_string(),
                batch_kind: FluxonFsTransferBatchKind::FullDir,
            });
        }
        Ok(claims)
    }

    async fn insert_coverage_claims_locked_tx(
        &self,
        tx: &mut Transaction,
        claims: &[TransferCoverageClaimRecord],
        batch_id: &str,
    ) -> Result<(), String> {
        for claim in claims {
            let record = TransferCoverageClaimRecord {
                batch_id: batch_id.to_string(),
                ..claim.clone()
            };
            put_record(
                tx,
                self.coverage_claim_key(record.job_id.as_str(), record.relpath.as_str()),
                &record,
                "insert transfer coverage claim",
            )
            .await?;
        }
        Ok(())
    }

    async fn upsert_direct_files_complete_marker_locked_tx(
        &self,
        tx: &mut Transaction,
        job_id: &str,
        root_relpath: &str,
        now_unix_ms: i64,
    ) -> Result<(), String> {
        let record = FsTransferDirectFilesCompleteRecord {
            job_id: job_id.to_string(),
            root_relpath: root_relpath.to_string(),
            completed_at_unix_ms: now_unix_ms,
        };
        put_record(
            tx,
            self.direct_files_complete_key(job_id, root_relpath),
            &record,
            "upsert transfer direct files complete marker",
        )
        .await
    }

    async fn verify_equivalent_batch_collect_infos(
        &self,
        tx: &mut Transaction,
        job_id: &str,
        batch_id: &str,
        incoming_collect_infos: &[fluxon_fs_core::config::FluxonFsTransferBatchCollectInfoWire],
    ) -> Result<(), String> {
        let existing_collect_infos = self
            .load_collect_infos_for_batch_from_tx(tx, job_id, batch_id)
            .await?;
        if existing_collect_infos.len() != incoming_collect_infos.len() {
            return Err(format!(
                "equivalent transfer batch collect info count mismatch: job_id={} batch_id={} existing={} incoming={}",
                job_id,
                batch_id,
                existing_collect_infos.len(),
                incoming_collect_infos.len()
            ));
        }
        for (existing, incoming) in existing_collect_infos
            .iter()
            .zip(incoming_collect_infos.iter())
        {
            if existing.collect_kind != incoming.collect_kind
                || existing.collect_blob.as_slice() != incoming.collect_blob.as_slice()
            {
                return Err(format!(
                    "equivalent transfer batch collect info mismatch: job_id={} batch_id={} collect_kind={}",
                    job_id,
                    batch_id,
                    incoming.collect_kind.as_db_str()
                ));
            }
        }
        Ok(())
    }

    async fn stop_worker_attempt_if_active_locked_tx(
        &self,
        tx: &mut Transaction,
        job_id: &str,
        batch_id: &str,
        worker_id: &str,
        worker_task_id: &str,
        stop_reason: FluxonFsTransferWorkerStopReasonWire,
        err_text: &str,
        now_unix_ms: i64,
        context: &str,
    ) -> Result<(), String> {
        let attempt_key = self.worker_attempt_key(job_id, worker_task_id);
        let Some(mut attempt) = get_record_for_update::<FsTransferWorkerAttemptRecord>(
            tx,
            attempt_key.clone(),
            context,
        )
        .await?
        else {
            return Ok(());
        };
        if attempt.batch_id != batch_id
            || attempt.worker_id != worker_id
            || attempt.state == FsTransferWorkerAttemptState::Finished
            || attempt.state == FsTransferWorkerAttemptState::Stopped
        {
            return Ok(());
        }
        attempt.state = FsTransferWorkerAttemptState::Stopped;
        attempt.stop_reason = Some(stop_reason);
        attempt.last_error = err_text.to_string();
        attempt.updated_at_unix_ms = now_unix_ms;
        put_record(
            tx,
            attempt_key,
            &attempt,
            "update transfer worker attempt stop state",
        )
        .await
    }

    async fn cancel_open_batch_locked_tx(
        &self,
        tx: &mut Transaction,
        job: &mut FsTransferJobRecord,
        job_id: &str,
        batch: &mut FsTransferBatchRecord,
        err_text: &str,
        now_unix_ms: i64,
    ) -> Result<(), String> {
        let previous_state = batch.state;
        let previous_worker_id = batch.owner_worker_id.clone();
        let previous_worker_task_id = batch.owner_worker_task_id.clone();
        if matches!(
            previous_state,
            FluxonFsTransferBatchState::Ready
                | FluxonFsTransferBatchState::Running
                | FluxonFsTransferBatchState::Done
                | FluxonFsTransferBatchState::Expired
        ) {
            delete_key(
                tx,
                self.batch_state_key(job_id, previous_state, batch.batch_id.as_str()),
                "delete open batch state index during transfer stop cleanup",
            )
            .await?;
            Self::adjust_job_batch_state_count(job, previous_state, -1);
        }
        if !previous_worker_task_id.is_empty() {
            delete_key(
                tx,
                self.worker_lease_key(job_id, previous_worker_task_id.as_str()),
                "delete worker lease during transfer stop cleanup",
            )
            .await?;
            self.stop_worker_attempt_if_active_locked_tx(
                tx,
                job_id,
                batch.batch_id.as_str(),
                previous_worker_id.as_str(),
                previous_worker_task_id.as_str(),
                FluxonFsTransferWorkerStopReasonWire::Cancelled,
                err_text,
                now_unix_ms,
                "transfer stop cleanup attempt lookup",
            )
            .await?;
        }
        batch.state = FluxonFsTransferBatchState::Cancelled;
        batch.owner_worker_id.clear();
        batch.owner_worker_task_id.clear();
        batch.lease_expire_unix_ms = 0;
        put_record(
            tx,
            self.batch_key(job_id, batch.batch_id.as_str()),
            batch,
            "mark transfer batch cancelled during stop cleanup",
        )
        .await?;
        put_marker(
            tx,
            self.batch_state_key(
                job_id,
                FluxonFsTransferBatchState::Cancelled,
                batch.batch_id.as_str(),
            ),
            "insert cancelled batch state index during stop cleanup",
        )
        .await
    }

    async fn stop_worker_for_stopping_job_locked_tx(
        &self,
        tx: &mut Transaction,
        job: &mut FsTransferJobRecord,
        job_id: &str,
        batch_id: &str,
        worker_id: &str,
        worker_task_id: &str,
        now_unix_ms: i64,
    ) -> Result<(), String> {
        if let Some(mut batch) = get_record::<FsTransferBatchRecord>(
            tx,
            self.batch_key(job_id, batch_id),
            "transfer stop worker batch lookup",
        )
        .await?
        {
            if batch.state == FluxonFsTransferBatchState::Running
                && batch.owner_worker_id == worker_id
                && batch.owner_worker_task_id == worker_task_id
            {
                self.cancel_open_batch_locked_tx(
                    tx,
                    job,
                    job_id,
                    &mut batch,
                    TRANSFER_CANCELLED_BY_USER_ERR,
                    now_unix_ms,
                )
                .await?;
                return Ok(());
            }
        }
        delete_key(
            tx,
            self.worker_lease_key(job_id, worker_task_id),
            "delete worker lease during stopping-job worker stop",
        )
        .await?;
        self.stop_worker_attempt_if_active_locked_tx(
            tx,
            job_id,
            batch_id,
            worker_id,
            worker_task_id,
            FluxonFsTransferWorkerStopReasonWire::Cancelled,
            TRANSFER_CANCELLED_BY_USER_ERR,
            now_unix_ms,
            "transfer stop worker attempt lookup",
        )
        .await
    }

    async fn load_some_batch_ids_in_state_from_tx(
        &self,
        tx: &mut Transaction,
        job_id: &str,
        state: FluxonFsTransferBatchState,
        max_count: usize,
    ) -> Result<Vec<String>, String> {
        let state_prefix = self.batch_state_prefix(job_id, state);
        let state_keys = scan_some_keys(tx, state_prefix.clone(), max_count).await?;
        let mut batch_ids = Vec::with_capacity(state_keys.len());
        for key_bytes in state_keys {
            batch_ids.push(decode_batch_id_from_state_key(
                &key_bytes,
                state_prefix.as_slice(),
            )?);
        }
        Ok(batch_ids)
    }

    async fn load_some_worker_leases_for_job_from_tx(
        &self,
        tx: &mut Transaction,
        job_id: &str,
        max_count: usize,
    ) -> Result<Vec<FsTransferWorkerLeaseRecord>, String> {
        let pairs = scan_some_pairs(tx, self.worker_lease_prefix(job_id), max_count).await?;
        let mut leases = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            leases.push(decode_record::<FsTransferWorkerLeaseRecord>(
                value.as_slice(),
                "transfer worker lease record by job",
            )?);
        }
        leases.sort_by(|a, b| a.worker_task_id.cmp(&b.worker_task_id));
        Ok(leases)
    }

    async fn has_worker_lease_from_tx(
        &self,
        tx: &mut Transaction,
        job_id: &str,
    ) -> Result<bool, String> {
        Ok(!scan_some_keys(tx, self.worker_lease_prefix(job_id), 1)
            .await?
            .is_empty())
    }

    async fn reconcile_stopping_job_locked_tx(
        &self,
        tx: &mut Transaction,
        job: &mut FsTransferJobRecord,
        now_unix_ms: i64,
    ) -> Result<(), String> {
        let job_id = job.job_id.clone();
        let mut remaining_batch_budget = TRANSFER_STOP_BATCH_CLEANUP_LIMIT;
        for state in [
            FluxonFsTransferBatchState::Running,
            FluxonFsTransferBatchState::Done,
            FluxonFsTransferBatchState::Ready,
            FluxonFsTransferBatchState::Expired,
        ] {
            if remaining_batch_budget == 0 {
                break;
            }
            let batch_ids = self
                .load_some_batch_ids_in_state_from_tx(
                    tx,
                    job_id.as_str(),
                    state,
                    remaining_batch_budget,
                )
                .await?;
            for batch_id in batch_ids {
                let Some(mut batch) = get_record::<FsTransferBatchRecord>(
                    tx,
                    self.batch_key(job_id.as_str(), batch_id.as_str()),
                    "transfer stop cleanup batch lookup",
                )
                .await?
                else {
                    delete_key(
                        tx,
                        self.batch_state_key(job_id.as_str(), state, batch_id.as_str()),
                        "delete stale batch state index during transfer stop cleanup",
                    )
                    .await?;
                    continue;
                };
                if batch.state != state {
                    delete_key(
                        tx,
                        self.batch_state_key(job_id.as_str(), state, batch_id.as_str()),
                        "delete mismatched batch state index during transfer stop cleanup",
                    )
                    .await?;
                    continue;
                }
                self.cancel_open_batch_locked_tx(
                    tx,
                    job,
                    job_id.as_str(),
                    &mut batch,
                    TRANSFER_CANCELLED_BY_USER_ERR,
                    now_unix_ms,
                )
                .await?;
                remaining_batch_budget = remaining_batch_budget.saturating_sub(1);
                if remaining_batch_budget == 0 {
                    break;
                }
            }
        }

        for lease in self
            .load_some_worker_leases_for_job_from_tx(
                tx,
                job_id.as_str(),
                TRANSFER_STOP_LEASE_CLEANUP_LIMIT,
            )
            .await?
        {
            delete_key(
                tx,
                self.worker_lease_key(job_id.as_str(), lease.worker_task_id.as_str()),
                "delete orphan worker lease during transfer stop cleanup",
            )
            .await?;
            self.stop_worker_attempt_if_active_locked_tx(
                tx,
                job_id.as_str(),
                lease.assigned_batch_id.as_str(),
                lease.worker_id.as_str(),
                lease.worker_task_id.as_str(),
                FluxonFsTransferWorkerStopReasonWire::Cancelled,
                TRANSFER_CANCELLED_BY_USER_ERR,
                now_unix_ms,
                "transfer stop cleanup orphan lease attempt lookup",
            )
            .await?;
        }

        let has_open_batches = Self::transfer_job_open_batch_count(job) > 0;
        if has_open_batches || self.has_worker_lease_from_tx(tx, job_id.as_str()).await? {
            job.updated_at_unix_ms = now_unix_ms;
            put_record(
                tx,
                self.job_key(job_id.as_str()),
                job,
                "update stopping transfer job progress",
            )
            .await?;
            return Ok(());
        }
        job.state = FluxonFsTransferJobState::Cancelled;
        job.scan_finished = true;
        job.desired_worker_count = 0;
        job.last_error = TRANSFER_CANCELLED_BY_USER_ERR.to_string();
        job.updated_at_unix_ms = now_unix_ms;
        put_record(
            tx,
            self.job_key(job_id.as_str()),
            job,
            "mark transfer job cancelled after stop cleanup",
        )
        .await
    }

    async fn load_jobs_from_tx(
        &self,
        tx: &mut Transaction,
    ) -> Result<Vec<FsTransferJobRecord>, String> {
        let pairs = scan_all_pairs(tx, self.namespace_prefix(KEY_NS_JOB)).await?;
        let mut jobs = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            jobs.push(decode_record::<FsTransferJobRecord>(
                value.as_slice(),
                "transfer job record",
            )?);
        }
        Ok(jobs)
    }

    async fn load_batches_from_tx(
        &self,
        tx: &mut Transaction,
    ) -> Result<Vec<FsTransferBatchRecord>, String> {
        let pairs = scan_all_pairs(tx, self.namespace_prefix(KEY_NS_BATCH)).await?;
        let mut batches = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            batches.push(decode_record::<FsTransferBatchRecord>(
                value.as_slice(),
                "transfer batch record",
            )?);
        }
        Ok(batches)
    }

    async fn load_batches_for_job_from_tx(
        &self,
        tx: &mut Transaction,
        job_id: &str,
    ) -> Result<Vec<FsTransferBatchRecord>, String> {
        let pairs = scan_all_pairs(tx, self.batch_prefix(job_id)).await?;
        let mut batches = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            batches.push(decode_record::<FsTransferBatchRecord>(
                value.as_slice(),
                "transfer batch record by job",
            )?);
        }
        batches.sort_by(|a, b| {
            a.root_relpath
                .cmp(&b.root_relpath)
                .then(a.generation.cmp(&b.generation))
                .then(a.batch_id.cmp(&b.batch_id))
        });
        Ok(batches)
    }

    async fn load_direct_files_complete_records_from_tx(
        &self,
        tx: &mut Transaction,
    ) -> Result<Vec<FsTransferDirectFilesCompleteRecord>, String> {
        let pairs = scan_all_pairs(tx, self.namespace_prefix(KEY_NS_DIRECT_FILES_COMPLETE)).await?;
        let mut rows = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            rows.push(decode_record::<FsTransferDirectFilesCompleteRecord>(
                value.as_slice(),
                "transfer direct files complete record",
            )?);
        }
        Ok(rows)
    }

    async fn load_direct_files_complete_records_for_job_from_tx(
        &self,
        tx: &mut Transaction,
        job_id: &str,
    ) -> Result<Vec<FsTransferDirectFilesCompleteRecord>, String> {
        let pairs = scan_all_pairs(tx, self.direct_files_complete_prefix(job_id)).await?;
        let mut rows = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            rows.push(decode_record::<FsTransferDirectFilesCompleteRecord>(
                value.as_slice(),
                "transfer direct files complete record by job",
            )?);
        }
        rows.sort_by(|a, b| a.root_relpath.cmp(&b.root_relpath));
        Ok(rows)
    }

    async fn load_worker_leases_for_job_from_tx(
        &self,
        tx: &mut Transaction,
        job_id: &str,
    ) -> Result<Vec<FsTransferWorkerLeaseRecord>, String> {
        let pairs = scan_all_pairs(tx, self.worker_lease_prefix(job_id)).await?;
        let mut leases = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            leases.push(decode_record::<FsTransferWorkerLeaseRecord>(
                value.as_slice(),
                "transfer worker lease record by job",
            )?);
        }
        leases.sort_by(|a, b| a.worker_task_id.cmp(&b.worker_task_id));
        Ok(leases)
    }

    async fn load_worker_attempts_from_tx(
        &self,
        tx: &mut Transaction,
    ) -> Result<Vec<FsTransferWorkerAttemptRecord>, String> {
        let pairs = scan_all_pairs(tx, self.namespace_prefix(KEY_NS_WORKER_ATTEMPT)).await?;
        let mut rows = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            rows.push(decode_record::<FsTransferWorkerAttemptRecord>(
                value.as_slice(),
                "transfer worker attempt record",
            )?);
        }
        Ok(rows)
    }

    async fn load_worker_attempts_for_job_from_tx(
        &self,
        tx: &mut Transaction,
        job_id: &str,
    ) -> Result<Vec<FsTransferWorkerAttemptRecord>, String> {
        let pairs = scan_all_pairs(tx, self.worker_attempt_prefix(job_id)).await?;
        let mut rows = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            rows.push(decode_record::<FsTransferWorkerAttemptRecord>(
                value.as_slice(),
                "transfer worker attempt record by job",
            )?);
        }
        rows.sort_by(|a, b| {
            a.batch_id
                .cmp(&b.batch_id)
                .then(a.created_at_unix_ms.cmp(&b.created_at_unix_ms))
                .then(a.worker_task_id.cmp(&b.worker_task_id))
        });
        Ok(rows)
    }

    async fn load_collect_infos_from_tx(
        &self,
        tx: &mut Transaction,
    ) -> Result<Vec<FsTransferBatchCollectInfoRecord>, String> {
        let pairs = scan_all_pairs(tx, self.namespace_prefix(KEY_NS_COLLECT_INFO)).await?;
        let mut rows = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            rows.push(decode_record::<FsTransferBatchCollectInfoRecord>(
                value.as_slice(),
                "transfer batch collect info record",
            )?);
        }
        Ok(rows)
    }

    async fn load_file_issues_from_tx(
        &self,
        tx: &mut Transaction,
    ) -> Result<Vec<FsTransferBatchFileIssueRecord>, String> {
        let pairs = scan_all_pairs(tx, self.namespace_prefix(KEY_NS_FILE_ISSUE)).await?;
        let mut rows = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            rows.push(decode_record::<FsTransferBatchFileIssueRecord>(
                value.as_slice(),
                "transfer batch file issue record",
            )?);
        }
        Ok(rows)
    }

    async fn load_file_issues_for_job_from_tx(
        &self,
        tx: &mut Transaction,
        job_id: &str,
    ) -> Result<Vec<FsTransferBatchFileIssueRecord>, String> {
        let pairs = scan_all_pairs(tx, self.file_issue_job_prefix(job_id)).await?;
        let mut rows = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            rows.push(decode_record::<FsTransferBatchFileIssueRecord>(
                value.as_slice(),
                "transfer batch file issue record by job",
            )?);
        }
        rows.sort_by(|a, b| {
            a.batch_id
                .cmp(&b.batch_id)
                .then(a.relpath.cmp(&b.relpath))
                .then(a.created_at_unix_ms.cmp(&b.created_at_unix_ms))
        });
        Ok(rows)
    }

    async fn load_file_issues_for_batch_from_tx(
        &self,
        tx: &mut Transaction,
        job_id: &str,
        batch_id: &str,
    ) -> Result<Vec<FsTransferBatchFileIssueRecord>, String> {
        let pairs = scan_all_pairs(tx, self.file_issue_prefix(job_id, batch_id)).await?;
        let mut rows = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            rows.push(decode_record::<FsTransferBatchFileIssueRecord>(
                value.as_slice(),
                "transfer batch file issue record by batch",
            )?);
        }
        rows.sort_by(|a, b| a.relpath.cmp(&b.relpath));
        Ok(rows)
    }

    async fn load_collect_infos_for_batch_from_tx(
        &self,
        tx: &mut Transaction,
        job_id: &str,
        batch_id: &str,
    ) -> Result<Vec<FsTransferBatchCollectInfoRecord>, String> {
        let pairs = scan_all_pairs(tx, self.collect_info_prefix(job_id, batch_id)).await?;
        let mut rows = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            rows.push(decode_record::<FsTransferBatchCollectInfoRecord>(
                value.as_slice(),
                "transfer batch collect info record by batch",
            )?);
        }
        rows.sort_by(|a, b| a.collect_kind.as_db_str().cmp(b.collect_kind.as_db_str()));
        Ok(rows)
    }

    async fn load_batches_in_state_for_job_from_tx(
        &self,
        tx: &mut Transaction,
        job_id: &str,
        state: FluxonFsTransferBatchState,
    ) -> Result<Vec<FsTransferBatchRecord>, String> {
        let state_prefix = self.batch_state_prefix(job_id, state);
        let state_keys = scan_all_keys(tx, state_prefix.clone()).await?;
        let mut batches = Vec::with_capacity(state_keys.len());
        for key_bytes in state_keys {
            let batch_id = decode_batch_id_from_state_key(&key_bytes, state_prefix.as_slice())?;
            let batch = get_required_record::<FsTransferBatchRecord>(
                tx,
                self.batch_key(job_id, batch_id.as_str()),
                "transfer batch lookup by state",
            )
            .await?;
            batches.push(batch);
        }
        batches.sort_by(|a, b| {
            a.root_relpath
                .cmp(&b.root_relpath)
                .then(a.generation.cmp(&b.generation))
                .then(a.batch_id.cmp(&b.batch_id))
        });
        Ok(batches)
    }

    async fn load_running_batch_owner_snapshots_for_job_from_tx(
        &self,
        tx: &mut Transaction,
        job_id: &str,
    ) -> Result<Vec<FsTransferRunningBatchOwnerSnapshot>, String> {
        let running_batches = self
            .load_batches_in_state_for_job_from_tx(tx, job_id, FluxonFsTransferBatchState::Running)
            .await?;
        Ok(running_batches
            .into_iter()
            .map(|batch| FsTransferRunningBatchOwnerSnapshot {
                batch_id: batch.batch_id,
                owner_worker_task_id: batch.owner_worker_task_id,
            })
            .collect())
    }

    fn namespace_prefix(&self, namespace: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.key_prefix.len() + namespace.len() + 1);
        out.extend_from_slice(self.key_prefix.as_slice());
        out.extend_from_slice(namespace);
        out.push(0);
        out
    }

    fn job_key(&self, job_id: &str) -> Vec<u8> {
        compose_key(self.key_prefix.as_slice(), KEY_NS_JOB, &[job_id.as_bytes()])
    }

    fn batch_prefix(&self, job_id: &str) -> Vec<u8> {
        compose_key(
            self.key_prefix.as_slice(),
            KEY_NS_BATCH,
            &[job_id.as_bytes()],
        )
    }

    fn batch_key(&self, job_id: &str, batch_id: &str) -> Vec<u8> {
        compose_key(
            self.key_prefix.as_slice(),
            KEY_NS_BATCH,
            &[job_id.as_bytes(), batch_id.as_bytes()],
        )
    }

    fn batch_eq_key(
        &self,
        job_id: &str,
        root_relpath: &str,
        batch: &FluxonFsTransferScanBatchWire,
    ) -> Vec<u8> {
        match batch.batch_kind {
            FluxonFsTransferBatchKind::DirectFilesOnly => compose_key(
                self.key_prefix.as_slice(),
                KEY_NS_BATCH_EQ,
                &[
                    job_id.as_bytes(),
                    root_relpath.as_bytes(),
                    batch.batch_kind.as_db_str().as_bytes(),
                    batch.batch_id.as_bytes(),
                ],
            ),
            FluxonFsTransferBatchKind::SubtreeSlice => compose_key(
                self.key_prefix.as_slice(),
                KEY_NS_BATCH_EQ,
                &[
                    job_id.as_bytes(),
                    root_relpath.as_bytes(),
                    batch.batch_kind.as_db_str().as_bytes(),
                    batch.batch_id.as_bytes(),
                ],
            ),
            FluxonFsTransferBatchKind::FullDir => compose_key(
                self.key_prefix.as_slice(),
                KEY_NS_BATCH_EQ,
                &[
                    job_id.as_bytes(),
                    root_relpath.as_bytes(),
                    batch.batch_kind.as_db_str().as_bytes(),
                ],
            ),
        }
    }

    fn batch_state_prefix(&self, job_id: &str, state: FluxonFsTransferBatchState) -> Vec<u8> {
        compose_key(
            self.key_prefix.as_slice(),
            KEY_NS_BATCH_STATE,
            &[job_id.as_bytes(), state.as_db_str().as_bytes()],
        )
    }

    fn batch_state_key(
        &self,
        job_id: &str,
        state: FluxonFsTransferBatchState,
        batch_id: &str,
    ) -> Vec<u8> {
        compose_key(
            self.key_prefix.as_slice(),
            KEY_NS_BATCH_STATE,
            &[
                job_id.as_bytes(),
                state.as_db_str().as_bytes(),
                batch_id.as_bytes(),
            ],
        )
    }

    fn collect_info_prefix(&self, job_id: &str, batch_id: &str) -> Vec<u8> {
        compose_key(
            self.key_prefix.as_slice(),
            KEY_NS_COLLECT_INFO,
            &[job_id.as_bytes(), batch_id.as_bytes()],
        )
    }

    fn collect_info_key(
        &self,
        job_id: &str,
        batch_id: &str,
        collect_kind: FluxonFsTransferCollectInfoKind,
    ) -> Vec<u8> {
        compose_key(
            self.key_prefix.as_slice(),
            KEY_NS_COLLECT_INFO,
            &[
                job_id.as_bytes(),
                batch_id.as_bytes(),
                collect_kind.as_db_str().as_bytes(),
            ],
        )
    }

    fn file_issue_prefix(&self, job_id: &str, batch_id: &str) -> Vec<u8> {
        compose_key(
            self.key_prefix.as_slice(),
            KEY_NS_FILE_ISSUE,
            &[job_id.as_bytes(), batch_id.as_bytes()],
        )
    }

    fn file_issue_job_prefix(&self, job_id: &str) -> Vec<u8> {
        compose_key(
            self.key_prefix.as_slice(),
            KEY_NS_FILE_ISSUE,
            &[job_id.as_bytes()],
        )
    }

    fn file_issue_key(&self, job_id: &str, batch_id: &str, relpath: &str) -> Vec<u8> {
        compose_key(
            self.key_prefix.as_slice(),
            KEY_NS_FILE_ISSUE,
            &[job_id.as_bytes(), batch_id.as_bytes(), relpath.as_bytes()],
        )
    }

    fn worker_lease_prefix(&self, job_id: &str) -> Vec<u8> {
        compose_key(
            self.key_prefix.as_slice(),
            KEY_NS_WORKER_LEASE,
            &[job_id.as_bytes()],
        )
    }

    fn worker_lease_key(&self, job_id: &str, worker_task_id: &str) -> Vec<u8> {
        compose_key(
            self.key_prefix.as_slice(),
            KEY_NS_WORKER_LEASE,
            &[job_id.as_bytes(), worker_task_id.as_bytes()],
        )
    }

    fn worker_attempt_key(&self, job_id: &str, worker_task_id: &str) -> Vec<u8> {
        compose_key(
            self.key_prefix.as_slice(),
            KEY_NS_WORKER_ATTEMPT,
            &[job_id.as_bytes(), worker_task_id.as_bytes()],
        )
    }

    fn worker_attempt_prefix(&self, job_id: &str) -> Vec<u8> {
        compose_key(
            self.key_prefix.as_slice(),
            KEY_NS_WORKER_ATTEMPT,
            &[job_id.as_bytes()],
        )
    }

    fn coverage_claim_key(&self, job_id: &str, relpath: &str) -> Vec<u8> {
        compose_key(
            self.key_prefix.as_slice(),
            KEY_NS_COVERAGE_CLAIM,
            &[job_id.as_bytes(), relpath.as_bytes()],
        )
    }

    fn direct_files_complete_key(&self, job_id: &str, root_relpath: &str) -> Vec<u8> {
        compose_key(
            self.key_prefix.as_slice(),
            KEY_NS_DIRECT_FILES_COMPLETE,
            &[job_id.as_bytes(), root_relpath.as_bytes()],
        )
    }

    fn direct_files_complete_prefix(&self, job_id: &str) -> Vec<u8> {
        compose_key(
            self.key_prefix.as_slice(),
            KEY_NS_DIRECT_FILES_COMPLETE,
            &[job_id.as_bytes()],
        )
    }
}

fn compose_key(prefix: &[u8], namespace: &[u8], parts: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        prefix.len() + namespace.len() + parts.iter().map(|part| part.len() + 1).sum::<usize>(),
    );
    out.extend_from_slice(prefix);
    out.extend_from_slice(namespace);
    for part in parts {
        out.push(0);
        out.extend_from_slice(part);
    }
    out
}

async fn put_record<T: serde::Serialize>(
    tx: &mut Transaction,
    key: Vec<u8>,
    value: &T,
    context: &str,
) -> Result<(), String> {
    let encoded =
        postcard_to_stdvec(value).map_err(|e| format!("encode {} failed: {}", context, e))?;
    tx.put(key, encoded)
        .await
        .map_err(|e| format!("write {} failed: {}", context, e))
}

async fn put_marker(tx: &mut Transaction, key: Vec<u8>, context: &str) -> Result<(), String> {
    tx.put(key, Vec::<u8>::new())
        .await
        .map_err(|e| format!("write {} failed: {}", context, e))
}

async fn delete_key(tx: &mut Transaction, key: Vec<u8>, context: &str) -> Result<(), String> {
    tx.delete(key)
        .await
        .map_err(|e| format!("delete {} failed: {}", context, e))
}

fn decode_record<T: DeserializeOwned>(blob: &[u8], context: &str) -> Result<T, String> {
    postcard_from_bytes(blob).map_err(|e| format!("decode {} failed: {}", context, e))
}

async fn get_record<T: DeserializeOwned>(
    tx: &mut Transaction,
    key: Vec<u8>,
    context: &str,
) -> Result<Option<T>, String> {
    let value = tx
        .get(key)
        .await
        .map_err(|e| format!("read {} failed: {}", context, e))?;
    value
        .map(|blob| decode_record(blob.as_slice(), context))
        .transpose()
}

async fn get_record_for_update<T: DeserializeOwned>(
    tx: &mut Transaction,
    key: Vec<u8>,
    context: &str,
) -> Result<Option<T>, String> {
    let value = tx
        .get_for_update(key)
        .await
        .map_err(|e| format!("read {} failed: {}", context, e))?;
    value
        .map(|blob| decode_record(blob.as_slice(), context))
        .transpose()
}

async fn get_required_record<T: DeserializeOwned>(
    tx: &mut Transaction,
    key: Vec<u8>,
    context: &str,
) -> Result<T, String> {
    get_record(tx, key, context)
        .await?
        .ok_or_else(|| format!("{} missing", context))
}

async fn rollback_with_error(tx: &mut Transaction, err: String) -> String {
    match tx.rollback().await {
        Ok(_) => err,
        Err(rollback_err) => format!("{}; rollback failed: {}", err, rollback_err),
    }
}

async fn scan_some_pairs(
    tx: &mut Transaction,
    prefix: Vec<u8>,
    max_count: usize,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
    if max_count == 0 {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    scan_tikv_pairs_paginated(tx, prefix, |key, value| {
        out.push((key.to_vec(), value.to_vec()));
        if out.len() >= max_count {
            return Ok(PrefixScanAction::Break);
        }
        Ok(PrefixScanAction::Continue)
    })
    .await?;
    Ok(out)
}

async fn scan_some_keys(
    tx: &mut Transaction,
    prefix: Vec<u8>,
    max_count: usize,
) -> Result<Vec<Vec<u8>>, String> {
    if max_count == 0 {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    scan_tikv_keys_paginated(tx, prefix, |key| {
        out.push(key.to_vec());
        if out.len() >= max_count {
            return Ok(PrefixScanAction::Break);
        }
        Ok(PrefixScanAction::Continue)
    })
    .await?;
    Ok(out)
}

async fn scan_all_pairs(
    tx: &mut Transaction,
    prefix: Vec<u8>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
    let mut out = Vec::new();
    scan_tikv_pairs_paginated(tx, prefix, |key, value| {
        out.push((key.to_vec(), value.to_vec()));
        Ok(PrefixScanAction::Continue)
    })
    .await?;
    Ok(out)
}

async fn scan_all_keys(tx: &mut Transaction, prefix: Vec<u8>) -> Result<Vec<Vec<u8>>, String> {
    let mut out = Vec::new();
    scan_tikv_keys_paginated(tx, prefix, |key| {
        out.push(key.to_vec());
        Ok(PrefixScanAction::Continue)
    })
    .await?;
    Ok(out)
}

async fn scan_tikv_pairs_paginated<F>(
    tx: &mut Transaction,
    prefix: Vec<u8>,
    mut on_pair: F,
) -> Result<(), String>
where
    F: FnMut(&[u8], &[u8]) -> Result<PrefixScanAction, String>,
{
    let mut start = prefix.clone();
    let end = prefix_scan_range_end_exclusive(prefix.as_slice());
    let mut page_limit = TIKV_SCAN_PAGE_LIMIT;
    loop {
        if let Some(end_key) = end.as_ref() {
            if start >= *end_key {
                break;
            }
        }
        let range = range_from_start(start.clone(), end.clone());
        let page = match tx.scan(range, page_limit).await {
            Ok(page) => page,
            Err(err) if is_tikv_scan_response_too_large(&err) && page_limit > 1 => {
                page_limit = std::cmp::max(1, page_limit / 2);
                continue;
            }
            Err(err) if is_tikv_scan_response_too_large(&err) => {
                return Err(format!(
                    "scan transfer tikv range failed even at single-entry page size for prefix {:?}: {}",
                    prefix, err
                ));
            }
            Err(err) => return Err(format!("scan transfer tikv range failed: {}", err)),
        };
        let mut saw_any = false;
        let mut last_key = None::<Vec<u8>>;
        for pair in page {
            let key_bytes = Vec::<u8>::from(pair.0);
            let value_bytes = pair.1;
            let action = on_pair(key_bytes.as_slice(), value_bytes.as_slice())?;
            last_key = Some(key_bytes);
            saw_any = true;
            if action == PrefixScanAction::Break {
                return Ok(());
            }
        }
        if !saw_any {
            break;
        }
        start = prefix_scan_key_after(last_key.as_ref().unwrap().as_slice());
    }
    Ok(())
}

async fn scan_tikv_keys_paginated<F>(
    tx: &mut Transaction,
    prefix: Vec<u8>,
    mut on_key: F,
) -> Result<(), String>
where
    F: FnMut(&[u8]) -> Result<PrefixScanAction, String>,
{
    let mut start = prefix.clone();
    let end = prefix_scan_range_end_exclusive(prefix.as_slice());
    let mut page_limit = TIKV_SCAN_PAGE_LIMIT;
    loop {
        if let Some(end_key) = end.as_ref() {
            if start >= *end_key {
                break;
            }
        }
        let range = range_from_start(start.clone(), end.clone());
        let page = match tx.scan_keys(range, page_limit).await {
            Ok(page) => page,
            Err(err) if is_tikv_scan_response_too_large(&err) && page_limit > 1 => {
                page_limit = std::cmp::max(1, page_limit / 2);
                continue;
            }
            Err(err) if is_tikv_scan_response_too_large(&err) => {
                return Err(format!(
                    "scan transfer tikv keys failed even at single-entry page size for prefix {:?}: {}",
                    prefix, err
                ));
            }
            Err(err) => return Err(format!("scan transfer tikv keys failed: {}", err)),
        };
        let mut saw_any = false;
        let mut last_key = None::<Vec<u8>>;
        for key in page {
            let key_bytes = Vec::<u8>::from(key);
            let action = on_key(key_bytes.as_slice())?;
            last_key = Some(key_bytes);
            saw_any = true;
            if action == PrefixScanAction::Break {
                return Ok(());
            }
        }
        if !saw_any {
            break;
        }
        start = prefix_scan_key_after(last_key.as_ref().unwrap().as_slice());
    }
    Ok(())
}

fn is_tikv_scan_response_too_large(err: &tikv_client::Error) -> bool {
    match err {
        tikv_client::Error::GrpcAPI(status) => {
            let code = format!("{:?}", status.code());
            code == "OutOfRange" && status.message().contains("message length too large")
        }
        _ => false,
    }
}

fn range_from_start(start: Vec<u8>, end: Option<Vec<u8>>) -> BoundRange {
    match end {
        Some(end_key) => BoundRange::new(
            Bound::Included(Key::from(start)),
            Bound::Excluded(Key::from(end_key)),
        ),
        None => BoundRange::range_from(Key::from(start)),
    }
}

fn decode_batch_id_from_state_key(key: &[u8], state_prefix: &[u8]) -> Result<String, String> {
    if !key.starts_with(state_prefix) {
        return Err("transfer batch state key prefix mismatch".to_string());
    }
    let suffix = &key[state_prefix.len()..];
    if suffix.is_empty() || suffix[0] != 0 {
        return Err("transfer batch state key missing separator".to_string());
    }
    String::from_utf8(suffix[1..].to_vec())
        .map_err(|e| format!("decode transfer batch state batch_id failed: {}", e))
}

fn build_symlink_notice_collect_blob(
    entries: &[FluxonFsTransferSymlinkNoticeEntryWire],
) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    for entry in entries {
        let line = serde_json::to_string(entry)
            .map_err(|e| format!("encode transfer symlink notice json line failed: {}", e))?;
        out.extend_from_slice(line.as_bytes());
        out.push(b'\n');
    }
    Ok(out)
}

fn decode_symlink_notice_collect_blob(
    blob: &[u8],
) -> Result<Vec<FluxonFsTransferSymlinkNoticeEntryWire>, String> {
    std::str::from_utf8(blob)
        .map_err(|e| format!("decode transfer symlink notice blob utf8 failed: {}", e))?
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            serde_json::from_str::<FluxonFsTransferSymlinkNoticeEntryWire>(line)
                .map_err(|e| format!("decode transfer symlink notice json line failed: {}", e))
        })
        .collect()
}

fn decode_symlink_notices_from_collect_infos(
    collect_infos: &[FluxonFsTransferBatchCollectInfoWire],
) -> Result<Vec<FluxonFsTransferSymlinkNoticeEntryWire>, String> {
    let mut out = Vec::new();
    for collect_info in collect_infos {
        match collect_info.collect_kind {
            FluxonFsTransferCollectInfoKind::SymlinkNotice => {
                out.extend(decode_symlink_notice_collect_blob(
                    collect_info.collect_blob.as_slice(),
                )?);
            }
        }
    }
    Ok(out)
}

fn build_symlink_collect_infos(
    entries: Vec<FluxonFsTransferSymlinkNoticeEntryWire>,
) -> Result<Vec<FluxonFsTransferBatchCollectInfoWire>, String> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    Ok(vec![FluxonFsTransferBatchCollectInfoWire {
        collect_kind: FluxonFsTransferCollectInfoKind::SymlinkNotice,
        collect_blob: build_symlink_notice_collect_blob(entries.as_slice())?,
    }])
}

fn transfer_manifest_stats(manifest: &FluxonFsTransferManifestWire) -> (i64, i64, i64) {
    (1, manifest.entry_count.max(0), manifest.total_bytes.max(0))
}

fn validate_equivalent_manifests(
    job_id: &str,
    batch_id: &str,
    normalized_root: &str,
    batch_kind: FluxonFsTransferBatchKind,
    existing_manifest: &FluxonFsTransferManifestWire,
    incoming_manifest: &FluxonFsTransferManifestWire,
) -> Result<(), String> {
    if existing_manifest.entry_count != incoming_manifest.entry_count {
        return Err(format!(
            "equivalent transfer batch entry_count mismatch: job_id={} batch_id={} root_relpath={} batch_kind={}",
            job_id,
            batch_id,
            normalized_root,
            batch_kind.as_db_str()
        ));
    }
    if existing_manifest.total_bytes != incoming_manifest.total_bytes {
        return Err(format!(
            "equivalent transfer batch total_bytes mismatch: job_id={} batch_id={} root_relpath={} batch_kind={}",
            job_id,
            batch_id,
            normalized_root,
            batch_kind.as_db_str()
        ));
    }
    if existing_manifest.entries.len() != incoming_manifest.entries.len() {
        return Err(format!(
            "equivalent transfer batch entry vector length mismatch: job_id={} batch_id={} root_relpath={} batch_kind={}",
            job_id,
            batch_id,
            normalized_root,
            batch_kind.as_db_str()
        ));
    }
    for (existing, incoming) in existing_manifest
        .entries
        .iter()
        .zip(incoming_manifest.entries.iter())
    {
        if existing.relpath != incoming.relpath || existing.size != incoming.size {
            return Err(format!(
                "equivalent transfer batch entry mismatch: job_id={} batch_id={} existing_relpath={} incoming_relpath={} existing_size={} incoming_size={}",
                job_id, batch_id, existing.relpath, incoming.relpath, existing.size, incoming.size
            ));
        }
    }
    if existing_manifest.empty_dir_relpaths.len() != incoming_manifest.empty_dir_relpaths.len() {
        return Err(format!(
            "equivalent transfer batch empty_dir_relpaths length mismatch: job_id={} batch_id={} root_relpath={} batch_kind={}",
            job_id,
            batch_id,
            normalized_root,
            batch_kind.as_db_str()
        ));
    }
    for (existing, incoming) in existing_manifest
        .empty_dir_relpaths
        .iter()
        .zip(incoming_manifest.empty_dir_relpaths.iter())
    {
        if existing != incoming {
            return Err(format!(
                "equivalent transfer batch empty_dir_relpath mismatch: job_id={} batch_id={} existing_relpath={} incoming_relpath={}",
                job_id, batch_id, existing, incoming
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{transfer_batch_manifest_dispatch_class, transfer_manifest_is_empty_dirs_only};
    use crate::FsTransferReadyBatchClass;
    use fluxon_fs_core::config::{
        FluxonFsTransferBatchCollectInfoWire, FluxonFsTransferCollectInfoKind,
        FluxonFsTransferManifestEntryWire, FluxonFsTransferManifestWire,
    };

    #[test]
    fn transfer_manifest_is_empty_dirs_only_requires_empty_collect_infos() {
        let manifest =
            FluxonFsTransferManifestWire::new(Vec::new(), vec!["root/empty".to_string()]);
        let collect_infos = [FluxonFsTransferBatchCollectInfoWire {
            collect_kind: FluxonFsTransferCollectInfoKind::SymlinkNotice,
            collect_blob: vec![1],
        }];
        assert!(transfer_manifest_is_empty_dirs_only(&manifest, false));
        assert!(!transfer_manifest_is_empty_dirs_only(
            &manifest,
            !collect_infos.is_empty(),
        ));
    }

    #[test]
    fn transfer_batch_manifest_dispatch_class_distinguishes_payload_from_empty_dirs_only() {
        let empty_only_manifest =
            FluxonFsTransferManifestWire::new(Vec::new(), vec!["root/empty".to_string()]);
        assert_eq!(
            transfer_batch_manifest_dispatch_class(&empty_only_manifest, false),
            FsTransferReadyBatchClass::EmptyDirsOnly,
        );

        let payload_manifest = FluxonFsTransferManifestWire::new(
            vec![FluxonFsTransferManifestEntryWire {
                relpath: "root/file.bin".to_string(),
                size: 1,
            }],
            vec!["root/empty".to_string()],
        );
        assert_eq!(
            transfer_batch_manifest_dispatch_class(&payload_manifest, false),
            FsTransferReadyBatchClass::Payload,
        );
    }
}
