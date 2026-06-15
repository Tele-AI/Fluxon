use fluxon_fs_core::config::{
    FluxonFsTransferBatchKind, FluxonFsTransferBatchState, FluxonFsTransferCollectInfoKind,
    FluxonFsTransferFailedFileReasonKindWire, FluxonFsTransferJobState,
    FluxonFsTransferScanResultWire, FluxonFsTransferWorkerHeartbeatResultWire,
    FluxonFsTransferWorkerHeartbeatTelemetryWire, FluxonFsTransferWorkerHeartbeatWire,
    FluxonFsTransferWorkerResultAckWire, FluxonFsTransferWorkerResultWire,
    FluxonFsTransferWorkerStopReasonWire,
};
use parking_lot::Mutex;
use std::collections::BTreeSet;
use std::sync::Arc;

pub const DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY: i64 = 10;

fn default_transfer_job_desired_scan_concurrency() -> i64 {
    DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY
}

// Authoritative durable record for one transfer job. scan_epoch is the
// invalidation counter for source-side scans.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FsTransferJobRecord {
    pub job_id: String,
    pub src_export: String,
    pub src_root_relpath: String,
    pub dst_export: String,
    pub dst_root_relpath: String,
    #[serde(default = "default_transfer_job_desired_scan_concurrency")]
    pub desired_scan_concurrency: i64,
    pub desired_worker_count: i64,
    pub batch_ready_bytes: i64,
    pub job_spec_blob: Vec<u8>,
    pub scan_epoch: i64,
    pub scan_finished: bool,
    #[serde(default)]
    pub scan_discovered_batch_count: i64,
    #[serde(default)]
    pub scan_discovered_file_count: i64,
    #[serde(default)]
    pub scan_discovered_bytes: i64,
    #[serde(default)]
    pub ready_batch_count: i64,
    #[serde(default)]
    pub running_batch_count: i64,
    #[serde(default)]
    pub done_batch_count: i64,
    #[serde(default)]
    pub finished_batch_count: i64,
    #[serde(default)]
    pub expired_batch_count: i64,
    #[serde(default)]
    pub failed_file_count: i64,
    pub state: FluxonFsTransferJobState,
    #[serde(default)]
    pub last_error: String,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

// Authoritative durable batch row. Ownership is valid only when state is
// Running and both owner fields describe the same live worker attempt.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FsTransferBatchRecord {
    pub job_id: String,
    pub batch_id: String,
    pub root_relpath: String,
    pub batch_kind: FluxonFsTransferBatchKind,
    pub state: FluxonFsTransferBatchState,
    #[serde(default)]
    pub assigned_src_exporter_id: String,
    #[serde(default)]
    pub assigned_dst_exporter_id: String,
    pub owner_worker_id: String,
    pub owner_worker_task_id: String,
    pub lease_expire_unix_ms: i64,
    pub manifest_blob: Vec<u8>,
    pub generation: i64,
    #[serde(default)]
    pub last_counted_scan_epoch: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FsTransferDirectFilesCompleteRecord {
    pub job_id: String,
    pub root_relpath: String,
    pub completed_at_unix_ms: i64,
}

// Durable keepalive for one worker attempt. Reconcile uses this row to decide
// whether a running batch still has a live owner after master restarts.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FsTransferWorkerLeaseRecord {
    pub job_id: String,
    pub worker_id: String,
    pub worker_task_id: String,
    pub assigned_batch_id: String,
    pub lease_expire_unix_ms: i64,
}

// Durable business-layer attempt record for one concrete worker_task_id.
// This record exists for observability and retry explanation. Batch ownership
// still remains authoritative for correctness.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FsTransferWorkerAttemptRecord {
    pub job_id: String,
    pub batch_id: String,
    pub worker_id: String,
    pub worker_task_id: String,
    pub dst_exporter_id: String,
    pub state: FsTransferWorkerAttemptState,
    pub launch_attempt_count: i64,
    pub visible_file_count: i64,
    pub visible_bytes: i64,
    pub last_error: String,
    pub stop_reason: Option<FluxonFsTransferWorkerStopReasonWire>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FsTransferWorkerAttemptState {
    Launching,
    Running,
    Stopped,
    Finished,
}

impl FsTransferWorkerAttemptState {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::Launching => "launching",
            Self::Running => "running",
            Self::Stopped => "stopped",
            Self::Finished => "finished",
        }
    }
}

// Durable auxiliary output for a batch, such as symlink notices. materialized
// is updated from worker result acceptance and later target-side reconcile.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FsTransferBatchCollectInfoRecord {
    pub job_id: String,
    pub batch_id: String,
    pub collect_kind: FluxonFsTransferCollectInfoKind,
    pub collect_blob: Vec<u8>,
    pub materialized: bool,
    pub materialized_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FsTransferBatchFileIssueRecord {
    pub job_id: String,
    pub batch_id: String,
    pub relpath: String,
    pub reason_kind: FluxonFsTransferFailedFileReasonKindWire,
    pub reason_detail: String,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsTransferReadyBatchDispatch {
    pub batch: FsTransferBatchRecord,
    pub collect_infos: Vec<FsTransferBatchCollectInfoRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsTransferReadyBatchClass {
    Payload,
    EmptyDirsOnly,
}

#[derive(Debug, Clone)]
pub struct FsTransferCreateJobArg {
    pub src_export: String,
    pub src_root_relpath: String,
    pub dst_export: String,
    pub dst_root_relpath: String,
    pub desired_scan_concurrency: i64,
    pub desired_worker_count: i64,
    pub batch_ready_bytes: i64,
    pub job_spec_blob: Vec<u8>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FsTransferJobSnapshot {
    pub scan_epoch: i64,
    pub scan_finished: bool,
    pub job: FsTransferJobRecord,
    pub open_batches: i64,
    pub done_batches: i64,
    pub failed_file_count: i64,
    pub running_batches: Vec<FsTransferBatchRecord>,
    pub worker_attempts: Vec<FsTransferWorkerAttemptRecord>,
    pub failed_files: Vec<FsTransferBatchFileIssueRecord>,
    pub live_detail: Option<FsTransferJobLiveDetailSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FsTransferSchedulerJobSnapshot {
    pub scan_epoch: i64,
    pub scan_finished: bool,
    pub job: FsTransferJobRecord,
    pub running_batch_count: i64,
    pub payload_running_batch_count: i64,
    pub empty_dir_only_running_batch_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FsTransferRecentFailureSnapshot {
    pub failure_index: i64,
    pub unix_ms: i64,
    pub scope: FsTransferFailureScope,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FsTransferFailureScope {
    Scan,
    WorkerLaunch,
    WorkerHeartbeat,
    WorkerStop,
    WorkerResult,
}

impl FsTransferFailureScope {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::WorkerLaunch => "worker_launch",
            Self::WorkerHeartbeat => "worker_heartbeat",
            Self::WorkerStop => "worker_stop",
            Self::WorkerResult => "worker_result",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FsTransferScanLiveDetailSnapshot {
    pub queued_scan_unit_count: i64,
    pub inflight_scan_unit_count: i64,
    pub completed_scan_unit_count: i64,
    pub discovered_batch_count: i64,
    pub discovered_file_count: i64,
    pub discovered_bytes: i64,
    pub scan_rate_files_per_sec: i64,
    pub scan_rate_bytes_per_sec: i64,
    pub last_scan_result_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FsTransferWorkerLiveSnapshot {
    pub worker_id: String,
    pub worker_task_id: String,
    pub batch_id: String,
    pub state: FsTransferWorkerAttemptState,
    pub launch_attempt_count: i64,
    pub visible_file_count: i64,
    pub visible_bytes: i64,
    pub lease_expire_unix_ms: i64,
    pub last_heartbeat_unix_ms: i64,
    pub current_bandwidth_bytes_per_sec: i64,
    pub total_written_bytes: i64,
    pub desired_file_lanes: i64,
    pub last_error: String,
    pub stop_reason: Option<FluxonFsTransferWorkerStopReasonWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FsTransferWorkerAggregateLiveDetailSnapshot {
    pub launching_worker_count: i64,
    pub running_worker_count: i64,
    pub stopped_worker_count: i64,
    pub finished_worker_count: i64,
    pub writing_batch_count: i64,
    pub aggregate_visible_file_count: i64,
    pub aggregate_visible_bytes: i64,
    pub aggregate_live_bandwidth_bytes_per_sec: i64,
    pub aggregate_total_written_bytes: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FsTransferJobLiveDetailSnapshot {
    pub scan: FsTransferScanLiveDetailSnapshot,
    pub workers: FsTransferWorkerAggregateLiveDetailSnapshot,
    pub recent_failures: Vec<FsTransferRecentFailureSnapshot>,
    pub active_workers: Vec<FsTransferWorkerLiveSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FsTransferJobSummarySnapshot {
    pub scan_epoch: i64,
    pub scan_finished: bool,
    pub open_batches: i64,
    pub pending_batches: i64,
    pub done_batches: i64,
    pub failed_file_count: i64,
    pub job: FsTransferJobRecord,
    pub running_batch_owners: Vec<FsTransferRunningBatchOwnerSnapshot>,
    pub live_detail: Option<FsTransferJobLiveDetailSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FsTransferRunningBatchOwnerSnapshot {
    pub batch_id: String,
    pub owner_worker_task_id: String,
}

#[derive(Debug, Clone)]
pub(crate) struct FsTransferWorkerHeartbeatLiveTelemetry {
    pub total_written_bytes: i64,
    pub window_started_unix_ms: i64,
    pub window_elapsed_ms: i64,
    pub window_bytes: i64,
    pub window_goodput_bytes_per_sec: i64,
    pub desired_file_lanes: i64,
}

impl FsTransferWorkerHeartbeatLiveTelemetry {
    pub(crate) fn from_wire(wire: &FluxonFsTransferWorkerHeartbeatTelemetryWire) -> Self {
        Self {
            total_written_bytes: wire.total_written_bytes,
            window_started_unix_ms: wire.window_started_unix_ms,
            window_elapsed_ms: wire.window_elapsed_ms,
            window_bytes: wire.window_bytes,
            window_goodput_bytes_per_sec: wire.window_goodput_bytes_per_sec,
            desired_file_lanes: wire.desired_file_lanes,
        }
    }
}

// The store is the durable authority for every state transition in transfer.
// Master and agents may speculatively act, but scan results, batch ownership,
// heartbeats, and completion become true only after one of these methods
// accepts the transition.
pub trait TransferStateStore: Send + Sync {
    fn insert_transfer_job(&self, job: &FsTransferJobRecord) -> Result<(), String>;
    fn cancel_transfer_job(&self, job_id: &str, now_unix_ms: i64) -> Result<(), String>;
    fn update_transfer_job_desired_concurrency(
        &self,
        job_id: &str,
        desired_scan_concurrency: i64,
        desired_worker_count: i64,
    ) -> Result<(), String>;
    fn update_transfer_job_desired_worker_count(
        &self,
        job_id: &str,
        desired_worker_count: i64,
    ) -> Result<(), String>;
    fn import_transfer_prescan_job(
        &self,
        job_id: &str,
        src_export: &str,
        src_root_relpath: &str,
        dst_export: &str,
        dst_root_relpath: &str,
        desired_scan_concurrency: i64,
        desired_worker_count: i64,
    ) -> Result<FsTransferJobRecord, String>;
    fn load_transfer_job_record(&self, job_id: &str)
    -> Result<Option<FsTransferJobRecord>, String>;
    fn load_transfer_job_records(&self) -> Result<Vec<FsTransferJobRecord>, String>;
    fn load_transfer_job_summary_snapshots(
        &self,
    ) -> Result<Vec<FsTransferJobSummarySnapshot>, String>;
    fn load_transfer_scheduler_job_snapshot(
        &self,
        job_id: &str,
    ) -> Result<Option<FsTransferSchedulerJobSnapshot>, String>;
    fn load_transfer_job_snapshot(
        &self,
        job_id: &str,
    ) -> Result<Option<FsTransferJobSnapshot>, String>;
    fn load_transfer_batch_record(
        &self,
        job_id: &str,
        batch_id: &str,
    ) -> Result<Option<FsTransferBatchRecord>, String>;
    fn load_transfer_job_snapshots(&self) -> Result<Vec<FsTransferJobSnapshot>, String>;
    fn load_transfer_batches(&self) -> Result<Vec<FsTransferBatchRecord>, String>;
    fn load_transfer_batches_for_job(
        &self,
        job_id: &str,
    ) -> Result<Vec<FsTransferBatchRecord>, String>;
    fn load_transfer_direct_files_complete_records(
        &self,
    ) -> Result<Vec<FsTransferDirectFilesCompleteRecord>, String>;
    fn load_transfer_direct_files_complete_records_for_job(
        &self,
        job_id: &str,
    ) -> Result<Vec<FsTransferDirectFilesCompleteRecord>, String>;
    fn load_transfer_worker_attempt_records(
        &self,
    ) -> Result<Vec<FsTransferWorkerAttemptRecord>, String>;
    fn load_transfer_batch_collect_info_records(
        &self,
    ) -> Result<Vec<FsTransferBatchCollectInfoRecord>, String>;
    fn load_transfer_batch_file_issue_records(
        &self,
    ) -> Result<Vec<FsTransferBatchFileIssueRecord>, String>;
    fn load_next_ready_transfer_batch_for_job(
        &self,
        job_id: &str,
        batch_class: FsTransferReadyBatchClass,
    ) -> Result<Option<FsTransferReadyBatchDispatch>, String>;
    fn load_ready_transfer_batches_for_job(
        &self,
        job_id: &str,
    ) -> Result<Vec<FsTransferBatchRecord>, String>;
    fn begin_transfer_scan_epoch(&self, job_id: &str) -> Result<i64, String>;
    fn finish_transfer_scan_epoch(&self, job_id: &str, scan_epoch: i64) -> Result<(), String>;
    fn apply_transfer_scan_append(
        &self,
        result: &FluxonFsTransferScanResultWire,
    ) -> Result<(), String>;
    fn finish_transfer_scan_unit(
        &self,
        result: &FluxonFsTransferScanResultWire,
    ) -> Result<(), String>;
    fn apply_transfer_scan_result(
        &self,
        result: &FluxonFsTransferScanResultWire,
    ) -> Result<(), String>;
    fn assign_transfer_batch_to_worker(
        &self,
        job_id: &str,
        batch_id: &str,
        src_exporter_id: &str,
        worker_id: &str,
        worker_task_id: &str,
        dst_exporter_id: &str,
        lease_expire_unix_ms: i64,
    ) -> Result<(), String>;
    fn record_transfer_worker_launch_retry(
        &self,
        job_id: &str,
        batch_id: &str,
        worker_id: &str,
        worker_task_id: &str,
        err_text: &str,
        now_unix_ms: i64,
    ) -> Result<(), String>;
    fn mark_transfer_worker_launch_acknowledged(
        &self,
        job_id: &str,
        batch_id: &str,
        worker_id: &str,
        worker_task_id: &str,
        now_unix_ms: i64,
    ) -> Result<(), String>;
    fn mark_transfer_worker_attempt_stopped(
        &self,
        job_id: &str,
        batch_id: &str,
        worker_id: &str,
        worker_task_id: &str,
        stop_reason: Option<FluxonFsTransferWorkerStopReasonWire>,
        err_text: &str,
        now_unix_ms: i64,
    ) -> Result<(), String>;
    fn apply_transfer_worker_result(
        &self,
        result: &FluxonFsTransferWorkerResultWire,
    ) -> Result<FluxonFsTransferWorkerResultAckWire, String>;
    fn apply_transfer_worker_heartbeat(
        &self,
        heartbeat: &FluxonFsTransferWorkerHeartbeatWire,
        heartbeat_received_unix_ms: i64,
        heartbeat_lease_duration_ms: i64,
    ) -> Result<FluxonFsTransferWorkerHeartbeatResultWire, String>;
}

#[derive(Clone)]
pub(crate) struct TransferWorkerSchedulerHandle {
    pub(crate) wake: Arc<tokio::sync::Notify>,
}

impl TransferWorkerSchedulerHandle {
    pub(crate) fn new() -> Self {
        Self {
            wake: Arc::new(tokio::sync::Notify::new()),
        }
    }

    pub(crate) fn notify(&self) {
        self.wake.notify_waiters();
    }
}

// This handle is process-local dedupe for scan execution only. Correctness does
// not depend on it because scan_epoch and durable batch equivalence already
// reject stale or duplicate scan outputs.
#[derive(Clone)]
pub(crate) struct TransferScanSchedulerHandle {
    pub(crate) wake: Arc<tokio::sync::Notify>,
    active_scan_jobs: Arc<Mutex<BTreeSet<String>>>,
}

impl TransferScanSchedulerHandle {
    pub(crate) fn new() -> Self {
        Self {
            wake: Arc::new(tokio::sync::Notify::new()),
            active_scan_jobs: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }

    pub(crate) fn notify(&self) {
        self.wake.notify_waiters();
    }

    pub(crate) fn try_begin_scan_job(&self, job_id: &str) -> bool {
        let mut active_scan_jobs = self.active_scan_jobs.lock();
        if active_scan_jobs.contains(job_id) {
            return false;
        }
        active_scan_jobs.insert(job_id.to_string());
        true
    }

    pub(crate) fn finish_scan_job(&self, job_id: &str) {
        self.active_scan_jobs.lock().remove(job_id);
        self.notify();
    }
}
