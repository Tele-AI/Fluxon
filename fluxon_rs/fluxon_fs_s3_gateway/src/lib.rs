use axum::Router;
use axum::body::HttpBody as _;
use axum::body::{Body, boxed};
use axum::extract::{Form, Multipart, Path, State};
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode, Uri, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{any, delete, get, post, put};
use base64::Engine as _;
use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, TimeZone as _, Utc};
use fluxon_fs_core::config::{
    FluxonFsAccessModel, FluxonFsAccessUser, FluxonFsExport, FluxonFsExportRoutingMode,
    FluxonFsGlobalConfig, FluxonFsRequestIdentity, FluxonFsS3GatewayConfig,
    FluxonFsS3PermissionAccount, FluxonFsS3PermissionAction, FluxonFsScopeAccess,
    FluxonFsScopeAccessMode, FluxonFsTransferStateStoreConfig, FluxonFsTransferStateStoreKind,
    FsAgentExportOverlayWire, access_model_from_s3_permission_list,
    agent_registry_export_for_name_and_root_v1, is_admin_browse_export_name_v1,
    runtime_access_model_from_s3_permission_list, runtime_access_model_to_json_text,
    s3_permission_list_from_access_model,
};
use fluxon_fs_core::path::{safe_abs_dirpath, safe_relpath};
use fluxon_fs_core::s3_gateway as fs_s3;
use fluxon_observability::keys::{
    PROM_LABEL_TRANSFER_DST_EXPORT, PROM_LABEL_TRANSFER_JOB_ID, PROM_LABEL_TRANSFER_SRC_EXPORT,
    PROM_METRIC_TRANSFER_JOB_BANDWIDTH_BYTES_PER_SEC,
    PROM_METRIC_TRANSFER_JOB_RUNNING_WORKER_COUNT, PROM_METRIC_TRANSFER_JOB_TOTAL_WRITTEN_BYTES,
    PROM_METRIC_TRANSFER_JOB_WRITING_BATCH_COUNT,
};
use fluxon_observability::metrics_actor::MetricsHandle as ObserveMetricsHandle;
use fluxon_util::prom_remote_write::{LABEL_NAME, Label, Sample, TimeSeries};
use futures::StreamExt;
use futures::future::BoxFuture;
use hmac::{Hmac, Mac as _};
use parking_lot::{Mutex, RwLock};
use rusqlite::{Connection, params};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use tower::ServiceExt as _;
use uuid::Uuid;

mod transfer;

#[cfg(test)]
use transfer::encode_transfer_manifest_blob;
pub use transfer::{
    DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY, FsTransferBatchCollectInfoRecord,
    FsTransferBatchFileIssueRecord, FsTransferBatchRecord, FsTransferCreateJobArg,
    FsTransferDirectFilesCompleteRecord, FsTransferFailureScope, FsTransferJobLiveDetailSnapshot,
    FsTransferJobRecord, FsTransferJobSnapshot, FsTransferReadyBatchClass,
    FsTransferRecentFailureSnapshot, FsTransferSchedulerJobSnapshot,
};
use transfer::{
    FsTransferScanLiveDetailSnapshot, FsTransferWorkerAggregateLiveDetailSnapshot,
    FsTransferWorkerAttemptState, FsTransferWorkerHeartbeatLiveTelemetry,
    FsTransferWorkerLiveSnapshot, TiKvTransferReconcileHandle, TiKvTransferStateStore,
    TransferScanSchedulerHandle, TransferStateStore, TransferWorkerSchedulerHandle,
};

type HmacSha256 = Hmac<Sha256>;

pub const SERVICE_NAME: &str = "fs_s3";

// Proxy-injected headers (fluxon_cli -> gateway).
pub const HDR_ORIGINAL_URI: &str = "x-fluxon-cli-proxy-original-uri";
pub const HDR_ORIGINAL_HOST: &str = "x-fluxon-cli-proxy-original-host";

// S3 / SigV4 constants.
const SIGV4_ALG: &str = "AWS4-HMAC-SHA256";
const SIGV4_SERVICE: &str = "s3";
const SIGV4_TERMINATOR: &str = "aws4_request";
const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

const FS_RPC_CHUNK_BYTES: usize = fs_s3::FS_S3_OBJECT_PIECE_BYTES;

// Multipart staging is stored inside the export directory. We intentionally keep it
// in-band (remote FS) so the gateway stays stateless.
const MULTIPART_DIR_PREFIX: &str = fs_s3::FS_S3_MULTIPART_DIR_PREFIX;
const MULTIPART_MAX_PARTS: i64 = 10_000;
const TRANSFER_HISTORY_EMIT_INTERVAL_MS: i64 = 1000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransferHistoryStep {
    OneSecond,
    FiveSeconds,
    ThirtySeconds,
    OneMinute,
    FiveMinutes,
    FifteenMinutes,
    OneHour,
}

impl TransferHistoryStep {
    const fn as_str(self) -> &'static str {
        match self {
            Self::OneSecond => "1s",
            Self::FiveSeconds => "5s",
            Self::ThirtySeconds => "30s",
            Self::OneMinute => "1m",
            Self::FiveMinutes => "5m",
            Self::FifteenMinutes => "15m",
            Self::OneHour => "1h",
        }
    }
}

fn choose_transfer_history_step(start_unix_ms: i64, end_unix_ms: i64) -> TransferHistoryStep {
    let span_ms = end_unix_ms.saturating_sub(start_unix_ms).max(0);
    if span_ms <= 15 * 60 * 1000 {
        TransferHistoryStep::OneSecond
    } else if span_ms <= 60 * 60 * 1000 {
        TransferHistoryStep::FiveSeconds
    } else if span_ms <= 6 * 60 * 60 * 1000 {
        TransferHistoryStep::ThirtySeconds
    } else if span_ms <= 24 * 60 * 60 * 1000 {
        TransferHistoryStep::OneMinute
    } else if span_ms <= 7 * 24 * 60 * 60 * 1000 {
        TransferHistoryStep::FiveMinutes
    } else if span_ms <= 30 * 24 * 60 * 60 * 1000 {
        TransferHistoryStep::FifteenMinutes
    } else {
        TransferHistoryStep::OneHour
    }
}

fn escape_prom_label_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '"' => out.push_str("\\\""),
            _ => out.push(ch),
        }
    }
    out
}

fn build_transfer_job_timeseries(
    metric_name: &str,
    labels: &[(String, String)],
    value: f64,
    unix_ms: i64,
) -> TimeSeries {
    let mut all_labels = Vec::with_capacity(labels.len() + 1);
    all_labels.push(Label {
        name: LABEL_NAME.to_string(),
        value: metric_name.to_string(),
    });
    for (name, value) in labels {
        all_labels.push(Label {
            name: name.clone(),
            value: value.clone(),
        });
    }
    TimeSeries {
        labels: all_labels,
        samples: vec![Sample {
            value,
            timestamp: unix_ms,
        }],
    }
}

#[derive(Debug, Clone)]
pub struct TransferHistoryQueryConfig {
    pub prometheus_base_url: String,
}

#[derive(Debug, Clone)]
pub struct GatewayAccessConfig {
    pub access_db_path: String,
    pub bootstrap_access_model: FluxonFsAccessModel,
    pub transfer_state_store: Option<FluxonFsTransferStateStoreConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsMountRegistryRecord {
    pub external_instance_key: String,
    pub local_mount_dir_abs: String,
    pub remote_root_dir_abs: String,
    pub updated_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsExportRegistryRecord {
    pub export_name: String,
    pub agent_instance_key: String,
    pub remote_root_dir_abs: String,
    pub export: FluxonFsExport,
    pub updated_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsExportOverlayDisabledRecord {
    pub agent_instance_key: String,
    pub export_name: String,
    pub updated_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsExportOverlayUpsertRecord {
    pub agent_instance_key: String,
    pub export_name: String,
    pub export: FluxonFsExport,
    pub updated_unix_ms: i64,
}

pub trait FsS3Backend: Send + Sync {
    fn ensure_export_config(
        &self,
        _export_name: &str,
        _export: &FluxonFsExport,
    ) -> Result<(), String> {
        Ok(())
    }

    // English note: use Arc<str> to avoid per-piece String cloning on hot paths (GET Object).
    fn stat(
        &self,
        request_identity: FluxonFsRequestIdentity,
        export_name: Arc<str>,
        relpath: Arc<str>,
    ) -> BoxFuture<'static, Result<RemoteStat, S3Error>>;
    fn stat_on_exporter(
        &self,
        request_identity: FluxonFsRequestIdentity,
        exporter_id: Arc<str>,
        export_name: Arc<str>,
        relpath: Arc<str>,
    ) -> BoxFuture<'static, Result<RemoteStat, S3Error>> {
        let _ = exporter_id;
        self.stat(request_identity, export_name, relpath)
    }
    fn list_dir(
        &self,
        request_identity: FluxonFsRequestIdentity,
        export_name: Arc<str>,
        relpath: Arc<str>,
    ) -> BoxFuture<'static, Result<Vec<RemoteDirEntry>, S3Error>>;
    fn read_chunk_cached(
        &self,
        request_identity: FluxonFsRequestIdentity,
        export_name: Arc<str>,
        relpath: Arc<str>,
        offset: i64,
        length: i64,
        file_size: i64,
        mtime_ns: i64,
    ) -> BoxFuture<'static, Result<Vec<u8>, S3Error>>;
    fn write_chunk(
        &self,
        request_identity: FluxonFsRequestIdentity,
        export_name: Arc<str>,
        relpath: Arc<str>,
        offset: i64,
        data: Vec<u8>,
    ) -> BoxFuture<'static, Result<(), S3Error>>;
    fn truncate(
        &self,
        request_identity: FluxonFsRequestIdentity,
        export_name: Arc<str>,
        relpath: Arc<str>,
        size: i64,
    ) -> BoxFuture<'static, Result<(), S3Error>>;
    fn mkdir(
        &self,
        request_identity: FluxonFsRequestIdentity,
        export_name: Arc<str>,
        relpath: Arc<str>,
        mode: i64,
    ) -> BoxFuture<'static, Result<(), S3Error>>;
    fn rename(
        &self,
        request_identity: FluxonFsRequestIdentity,
        export_name: Arc<str>,
        src_relpath: Arc<str>,
        dst_relpath: Arc<str>,
    ) -> BoxFuture<'static, Result<(), S3Error>>;
    fn unlink(
        &self,
        request_identity: FluxonFsRequestIdentity,
        export_name: Arc<str>,
        relpath: Arc<str>,
    ) -> BoxFuture<'static, Result<(), S3Error>>;
    fn rmdir(
        &self,
        request_identity: FluxonFsRequestIdentity,
        export_name: Arc<str>,
        relpath: Arc<str>,
    ) -> BoxFuture<'static, Result<(), S3Error>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FsMasterMemberKind {
    Agent,
    Controller,
}

impl FsMasterMemberKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Controller => "controller",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsMasterMemberRecord {
    pub kind: FsMasterMemberKind,
    pub member_id: String,
    pub owner_id: String,
    pub hostname: String,
    pub addresses: Vec<String>,
    pub port: Option<i64>,
    pub pid: String,
    pub cmd: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsMasterMountRecord {
    pub external_instance_key: String,
    pub local_mount_dir_abs: String,
    pub remote_root_dir_abs: String,
    pub updated_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsMasterAdminRuntimeExportRecord {
    pub export_name: String,
    pub remote_root_dir_abs: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsMasterAdminRuntimeAgentExports {
    pub agent_instance_key: String,
    pub runtime_exports: Vec<FsMasterAdminRuntimeExportRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsMasterAdminManagedExportRecord {
    pub export_name: String,
    pub remote_root_dir_abs: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsMasterAdminManagedAgentExports {
    pub agent_instance_key: String,
    pub managed_exports: Vec<FsMasterAdminManagedExportRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsMasterAdminSnapshot {
    pub members: Vec<FsMasterMemberRecord>,
    pub mounts: Vec<FsMasterMountRecord>,
    pub runtime_agent_exports: Vec<FsMasterAdminRuntimeAgentExports>,
    pub managed_agent_exports: Vec<FsMasterAdminManagedAgentExports>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsMasterAdminBrowseDirEntry {
    pub name: String,
    pub is_file: bool,
    pub is_dir: bool,
}

pub trait FsMasterAdminBackend: Send + Sync {
    fn list_fs_master_members(
        &self,
    ) -> BoxFuture<'static, Result<Vec<FsMasterMemberRecord>, String>>;
    fn list_fs_master_online_member_ids(
        &self,
    ) -> BoxFuture<'static, Result<BTreeSet<String>, String>>;
    fn list_fs_master_agent_dir(
        &self,
        agent_instance_key: String,
        dir_abs: String,
    ) -> BoxFuture<'static, Result<Vec<FsMasterAdminBrowseDirEntry>, String>>;
}

#[derive(Clone)]
pub struct GatewayState {
    cluster_name: String,
    access_db_path: String,
    external_base_path: String,
    access_db: Arc<Mutex<Connection>>,
    transfer_state_store: Option<Arc<dyn TransferStateStore>>,
    transfer_reconcile_handle: Option<Arc<TiKvTransferReconcileHandle>>,
    permission_list: Arc<RwLock<Vec<FluxonFsS3PermissionAccount>>>,
    fs_cache: Arc<FluxonFsGlobalConfig>,
    s3_cfg: FluxonFsS3GatewayConfig,
    backend: Arc<dyn FsS3Backend>,
    fs_master_admin_backend: Arc<dyn FsMasterAdminBackend>,
    ui_transfer_tasks: Arc<RwLock<BTreeMap<String, UiTransferTaskHandle>>>,
    transfer_worker_scheduler: TransferWorkerSchedulerHandle,
    transfer_scan_scheduler: TransferScanSchedulerHandle,
    transfer_live_detail: Arc<Mutex<BTreeMap<String, TransferJobLiveDetailState>>>,
    transfer_history: Option<TransferHistoryRuntime>,
}

const TRANSFER_RECENT_FAILURE_LIMIT: usize = 8;

#[derive(Debug, Clone, Default)]
struct TransferJobLiveDetailState {
    scan: TransferScanLiveDetailState,
    workers: BTreeMap<String, TransferWorkerLiveDetailState>,
    recent_failures: VecDeque<FsTransferRecentFailureSnapshot>,
    next_failure_index: i64,
}

#[derive(Debug, Clone, Default)]
struct TransferScanLiveDetailState {
    queued_scan_unit_count: i64,
    inflight_scan_unit_count: i64,
    completed_scan_unit_count: i64,
    discovered_batch_count: i64,
    discovered_file_count: i64,
    discovered_bytes: i64,
    scan_rate_files_per_sec: i64,
    scan_rate_bytes_per_sec: i64,
    last_scan_result_unix_ms: i64,
}

#[derive(Debug, Clone)]
struct TransferWorkerLiveDetailState {
    worker_id: String,
    worker_task_id: String,
    batch_id: String,
    state: FsTransferWorkerAttemptState,
    launch_attempt_count: i64,
    visible_file_count: i64,
    visible_bytes: i64,
    lease_expire_unix_ms: i64,
    last_heartbeat_unix_ms: i64,
    current_bandwidth_bytes_per_sec: i64,
    total_written_bytes: i64,
    desired_file_lanes: i64,
    last_error: String,
    stop_reason: Option<fluxon_fs_core::config::FluxonFsTransferWorkerStopReasonWire>,
}

#[derive(Debug, Clone)]
struct TransferJobHistoryMeta {
    src_export: String,
    dst_export: String,
    last_emit_unix_ms: i64,
}

#[derive(Clone)]
struct TransferHistoryRuntime {
    metrics_handle: ObserveMetricsHandle,
    query: TransferHistoryQueryConfig,
    meta_by_job_id: Arc<Mutex<BTreeMap<String, TransferJobHistoryMeta>>>,
    http: reqwest::Client,
}

#[derive(Debug, serde::Deserialize)]
struct TransferPromResp {
    status: String,
    data: TransferPromData,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransferPromData {
    result: Vec<TransferPromResultSample>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
enum TransferPromResultSample {
    Range { values: Vec<(f64, String)> },
}

impl TransferWorkerLiveDetailState {
    fn new(
        job_batch_id: &str,
        worker_id: &str,
        worker_task_id: &str,
        lease_expire_unix_ms: i64,
    ) -> Self {
        Self {
            worker_id: worker_id.to_string(),
            worker_task_id: worker_task_id.to_string(),
            batch_id: job_batch_id.to_string(),
            state: FsTransferWorkerAttemptState::Launching,
            launch_attempt_count: 0,
            visible_file_count: 0,
            visible_bytes: 0,
            lease_expire_unix_ms,
            last_heartbeat_unix_ms: 0,
            current_bandwidth_bytes_per_sec: 0,
            total_written_bytes: 0,
            desired_file_lanes: 0,
            last_error: String::new(),
            stop_reason: None,
        }
    }

    fn snapshot(&self) -> FsTransferWorkerLiveSnapshot {
        FsTransferWorkerLiveSnapshot {
            worker_id: self.worker_id.clone(),
            worker_task_id: self.worker_task_id.clone(),
            batch_id: self.batch_id.clone(),
            state: self.state,
            launch_attempt_count: self.launch_attempt_count,
            visible_file_count: self.visible_file_count,
            visible_bytes: self.visible_bytes,
            lease_expire_unix_ms: self.lease_expire_unix_ms,
            last_heartbeat_unix_ms: self.last_heartbeat_unix_ms,
            current_bandwidth_bytes_per_sec: self.current_bandwidth_bytes_per_sec,
            total_written_bytes: self.total_written_bytes,
            desired_file_lanes: self.desired_file_lanes,
            last_error: self.last_error.clone(),
            stop_reason: self.stop_reason,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TransferJobHistoryPoint {
    pub unix_ms: i64,
    pub bandwidth_bytes_per_sec: f64,
    pub running_worker_count: f64,
    pub writing_batch_count: f64,
    pub total_written_bytes: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TransferJobHistorySnapshot {
    pub start_unix_ms: i64,
    pub end_unix_ms: i64,
    pub points: Vec<TransferJobHistoryPoint>,
}

impl TransferJobLiveDetailState {
    fn reset_scan_for_new_epoch(&mut self) {
        self.scan = TransferScanLiveDetailState::default();
    }

    fn push_failure(&mut self, unix_ms: i64, scope: FsTransferFailureScope, message: String) {
        let failure_index = self.next_failure_index;
        self.next_failure_index = self.next_failure_index.saturating_add(1);
        self.recent_failures
            .push_front(FsTransferRecentFailureSnapshot {
                failure_index,
                unix_ms,
                scope,
                message,
            });
        while self.recent_failures.len() > TRANSFER_RECENT_FAILURE_LIMIT {
            self.recent_failures.pop_back();
        }
    }

    fn snapshot(
        &self,
        current_running_batch_owner_by_batch_id: &BTreeMap<String, String>,
    ) -> FsTransferJobLiveDetailSnapshot {
        let mut active_workers: Vec<FsTransferWorkerLiveSnapshot> = self
            .workers
            .values()
            .filter(|worker| {
                current_running_batch_owner_by_batch_id
                    .get(worker.batch_id.as_str())
                    .map(|owner_worker_task_id| owner_worker_task_id == &worker.worker_task_id)
                    .unwrap_or(false)
            })
            .map(TransferWorkerLiveDetailState::snapshot)
            .collect();
        active_workers.sort_by(|a, b| {
            a.batch_id
                .cmp(&b.batch_id)
                .then(a.worker_task_id.cmp(&b.worker_task_id))
        });
        let mut launching_worker_count = 0_i64;
        let mut running_worker_count = 0_i64;
        let mut stopped_worker_count = 0_i64;
        let mut finished_worker_count = 0_i64;
        let mut writing_batch_count = 0_i64;
        let mut aggregate_visible_file_count = 0_i64;
        let mut aggregate_visible_bytes = 0_i64;
        let mut aggregate_live_bandwidth_bytes_per_sec = 0_i64;
        let mut aggregate_total_written_bytes = 0_i64;
        for worker in &active_workers {
            match worker.state {
                FsTransferWorkerAttemptState::Launching => launching_worker_count += 1,
                FsTransferWorkerAttemptState::Running => running_worker_count += 1,
                FsTransferWorkerAttemptState::Stopped => stopped_worker_count += 1,
                FsTransferWorkerAttemptState::Finished => finished_worker_count += 1,
            }
            if worker.state == FsTransferWorkerAttemptState::Running {
                writing_batch_count += 1;
            }
            aggregate_visible_file_count =
                aggregate_visible_file_count.saturating_add(worker.visible_file_count);
            aggregate_visible_bytes = aggregate_visible_bytes.saturating_add(worker.visible_bytes);
            aggregate_live_bandwidth_bytes_per_sec = aggregate_live_bandwidth_bytes_per_sec
                .saturating_add(worker.current_bandwidth_bytes_per_sec.max(0));
            aggregate_total_written_bytes =
                aggregate_total_written_bytes.saturating_add(worker.total_written_bytes.max(0));
        }
        FsTransferJobLiveDetailSnapshot {
            scan: FsTransferScanLiveDetailSnapshot {
                queued_scan_unit_count: self.scan.queued_scan_unit_count,
                inflight_scan_unit_count: self.scan.inflight_scan_unit_count,
                completed_scan_unit_count: self.scan.completed_scan_unit_count,
                discovered_batch_count: self.scan.discovered_batch_count,
                discovered_file_count: self.scan.discovered_file_count,
                discovered_bytes: self.scan.discovered_bytes,
                scan_rate_files_per_sec: self.scan.scan_rate_files_per_sec,
                scan_rate_bytes_per_sec: self.scan.scan_rate_bytes_per_sec,
                last_scan_result_unix_ms: self.scan.last_scan_result_unix_ms,
            },
            workers: FsTransferWorkerAggregateLiveDetailSnapshot {
                launching_worker_count,
                running_worker_count,
                stopped_worker_count,
                finished_worker_count,
                writing_batch_count,
                aggregate_visible_file_count,
                aggregate_visible_bytes,
                aggregate_live_bandwidth_bytes_per_sec,
                aggregate_total_written_bytes,
            },
            recent_failures: self.recent_failures.iter().cloned().collect(),
            active_workers,
        }
    }
}

impl GatewayState {
    pub fn new(
        cluster_name: String,
        external_base_path: String,
        access: GatewayAccessConfig,
        fs_cache: Arc<FluxonFsGlobalConfig>,
        s3_cfg: FluxonFsS3GatewayConfig,
        backend: Arc<dyn FsS3Backend>,
        fs_master_admin_backend: Arc<dyn FsMasterAdminBackend>,
        transfer_history_metrics_handle: Option<ObserveMetricsHandle>,
        transfer_history_query: Option<TransferHistoryQueryConfig>,
    ) -> Result<Self, String> {
        let access_db = Arc::new(Mutex::new(open_access_db(&access.access_db_path)?));
        let permission_list = {
            let mut conn = access_db.lock();
            load_or_bootstrap_permission_list_from_db(
                &mut conn,
                &access.bootstrap_access_model,
                fs_cache.as_ref(),
            )?
        };
        let external_base_path = normalize_external_base_path(&external_base_path);
        let transfer_worker_scheduler = TransferWorkerSchedulerHandle::new();
        let transfer_scan_scheduler = TransferScanSchedulerHandle::new();
        let (transfer_state_store, transfer_reconcile_handle) =
            match access.transfer_state_store.clone() {
                Some(cfg) => (
                    Some(build_transfer_state_store(cfg.clone())?),
                    Some(Arc::new(TiKvTransferReconcileHandle::new(
                        &match cfg.kind {
                            FluxonFsTransferStateStoreKind::TiKv(ref tikv) => tikv.clone(),
                        },
                    )?)),
                ),
                None => (None, None),
            };
        let transfer_history = match (transfer_history_metrics_handle, transfer_history_query) {
            (Some(metrics_handle), Some(query)) => Some(TransferHistoryRuntime {
                metrics_handle,
                query,
                meta_by_job_id: Arc::new(Mutex::new(BTreeMap::new())),
                http: reqwest::Client::new(),
            }),
            _ => None,
        };
        Ok(Self {
            cluster_name,
            access_db_path: access.access_db_path,
            external_base_path,
            access_db,
            transfer_state_store,
            transfer_reconcile_handle,
            permission_list: Arc::new(RwLock::new(permission_list)),
            fs_cache,
            s3_cfg,
            backend,
            fs_master_admin_backend,
            ui_transfer_tasks: Arc::new(RwLock::new(BTreeMap::new())),
            transfer_worker_scheduler,
            transfer_scan_scheduler,
            transfer_live_detail: Arc::new(Mutex::new(BTreeMap::new())),
            transfer_history,
        })
    }

    fn transfer_history_record_job_meta(&self, job_id: &str, src_export: &str, dst_export: &str) {
        let Some(history) = self.transfer_history.as_ref() else {
            return;
        };
        let mut guard = history.meta_by_job_id.lock();
        guard
            .entry(job_id.to_string())
            .and_modify(|meta| {
                meta.src_export = src_export.to_string();
                meta.dst_export = dst_export.to_string();
            })
            .or_insert_with(|| TransferJobHistoryMeta {
                src_export: src_export.to_string(),
                dst_export: dst_export.to_string(),
                last_emit_unix_ms: 0,
            });
    }

    fn transfer_history_emit_job_point_if_due(
        &self,
        job_id: &str,
        now_unix_ms: i64,
        running_worker_count: i64,
        writing_batch_count: i64,
        aggregate_bandwidth_bytes_per_sec: i64,
        total_written_bytes: i64,
    ) {
        let Some(history) = self.transfer_history.as_ref() else {
            return;
        };
        let mut guard = history.meta_by_job_id.lock();
        let Some(meta) = guard.get_mut(job_id) else {
            return;
        };
        if meta.last_emit_unix_ms > 0
            && now_unix_ms.saturating_sub(meta.last_emit_unix_ms)
                < TRANSFER_HISTORY_EMIT_INTERVAL_MS
        {
            return;
        }
        meta.last_emit_unix_ms = now_unix_ms;
        let base_labels = vec![
            (PROM_LABEL_TRANSFER_JOB_ID.to_string(), job_id.to_string()),
            (
                PROM_LABEL_TRANSFER_SRC_EXPORT.to_string(),
                meta.src_export.clone(),
            ),
            (
                PROM_LABEL_TRANSFER_DST_EXPORT.to_string(),
                meta.dst_export.clone(),
            ),
        ];
        let series = vec![
            build_transfer_job_timeseries(
                PROM_METRIC_TRANSFER_JOB_BANDWIDTH_BYTES_PER_SEC,
                &base_labels,
                aggregate_bandwidth_bytes_per_sec.max(0) as f64,
                now_unix_ms,
            ),
            build_transfer_job_timeseries(
                PROM_METRIC_TRANSFER_JOB_RUNNING_WORKER_COUNT,
                &base_labels,
                running_worker_count.max(0) as f64,
                now_unix_ms,
            ),
            build_transfer_job_timeseries(
                PROM_METRIC_TRANSFER_JOB_WRITING_BATCH_COUNT,
                &base_labels,
                writing_batch_count.max(0) as f64,
                now_unix_ms,
            ),
            build_transfer_job_timeseries(
                PROM_METRIC_TRANSFER_JOB_TOTAL_WRITTEN_BYTES,
                &base_labels,
                total_written_bytes.max(0) as f64,
                now_unix_ms,
            ),
        ];
        history.metrics_handle.try_submit_timeseries(series);
    }

    fn transfer_job_live_totals_for_history(
        entry: &TransferJobLiveDetailState,
    ) -> (i64, i64, i64, i64) {
        let mut running_worker_count = 0_i64;
        let mut writing_batch_count = 0_i64;
        let mut aggregate_bandwidth_bytes_per_sec = 0_i64;
        let mut total_written_bytes = 0_i64;
        for worker in entry.workers.values() {
            if worker.state == FsTransferWorkerAttemptState::Running {
                running_worker_count += 1;
                writing_batch_count += 1;
            }
            aggregate_bandwidth_bytes_per_sec = aggregate_bandwidth_bytes_per_sec
                .saturating_add(worker.current_bandwidth_bytes_per_sec.max(0));
            total_written_bytes =
                total_written_bytes.saturating_add(worker.total_written_bytes.max(0));
        }
        (
            running_worker_count,
            writing_batch_count,
            aggregate_bandwidth_bytes_per_sec,
            total_written_bytes,
        )
    }

    async fn transfer_job_history_snapshot(
        &self,
        job_id: &str,
        start_unix_ms: i64,
        end_unix_ms: i64,
    ) -> Result<TransferJobHistorySnapshot, String> {
        let history = self.transfer_history.as_ref().ok_or_else(|| {
            "transfer history is unavailable because observability is not configured".to_string()
        })?;
        let start_unix_ms = start_unix_ms.max(0);
        let end_unix_ms = end_unix_ms.max(start_unix_ms);
        let start_s = (start_unix_ms as f64) / 1000.0;
        let end_s = (end_unix_ms as f64) / 1000.0;
        let step = choose_transfer_history_step(start_unix_ms, end_unix_ms);
        let selector = format!(
            "{}=\"{}\"",
            PROM_LABEL_TRANSFER_JOB_ID,
            escape_prom_label_value(job_id)
        );
        let bandwidth = self
            .transfer_history_query_range(
                history,
                &format!(
                    "{}{{{}}}",
                    PROM_METRIC_TRANSFER_JOB_BANDWIDTH_BYTES_PER_SEC, selector
                ),
                start_s,
                end_s,
                step.as_str(),
            )
            .await?;
        let running_workers = self
            .transfer_history_query_range(
                history,
                &format!(
                    "{}{{{}}}",
                    PROM_METRIC_TRANSFER_JOB_RUNNING_WORKER_COUNT, selector
                ),
                start_s,
                end_s,
                step.as_str(),
            )
            .await?;
        let writing_batches = self
            .transfer_history_query_range(
                history,
                &format!(
                    "{}{{{}}}",
                    PROM_METRIC_TRANSFER_JOB_WRITING_BATCH_COUNT, selector
                ),
                start_s,
                end_s,
                step.as_str(),
            )
            .await?;
        let total_written_bytes = self
            .transfer_history_query_range(
                history,
                &format!(
                    "{}{{{}}}",
                    PROM_METRIC_TRANSFER_JOB_TOTAL_WRITTEN_BYTES, selector
                ),
                start_s,
                end_s,
                step.as_str(),
            )
            .await?;
        let max_len = bandwidth
            .len()
            .max(running_workers.len())
            .max(writing_batches.len())
            .max(total_written_bytes.len());
        let mut points = Vec::with_capacity(max_len);
        for idx in 0..max_len {
            let unix_ms = bandwidth
                .get(idx)
                .or_else(|| running_workers.get(idx))
                .or_else(|| writing_batches.get(idx))
                .or_else(|| total_written_bytes.get(idx))
                .map(|(ts, _)| *ts)
                .unwrap_or(start_unix_ms);
            points.push(TransferJobHistoryPoint {
                unix_ms,
                bandwidth_bytes_per_sec: bandwidth.get(idx).map(|(_, v)| *v).unwrap_or(0.0),
                running_worker_count: running_workers.get(idx).map(|(_, v)| *v).unwrap_or(0.0),
                writing_batch_count: writing_batches.get(idx).map(|(_, v)| *v).unwrap_or(0.0),
                total_written_bytes: total_written_bytes.get(idx).map(|(_, v)| *v).unwrap_or(0.0),
            });
        }
        Ok(TransferJobHistorySnapshot {
            start_unix_ms,
            end_unix_ms,
            points,
        })
    }

    async fn transfer_history_query_range(
        &self,
        history: &TransferHistoryRuntime,
        promql: &str,
        start_s: f64,
        end_s: f64,
        step: &str,
    ) -> Result<Vec<(i64, f64)>, String> {
        let url = format!(
            "{}/api/v1/query_range",
            history.query.prometheus_base_url.trim_end_matches('/')
        );
        let resp = history
            .http
            .get(url)
            .query(&[
                ("query", promql),
                ("start", &format!("{start_s:.3}")),
                ("end", &format!("{end_s:.3}")),
                ("step", step),
            ])
            .send()
            .await
            .map_err(|e| format!("prometheus query_range failed: {}", e))?;
        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| format!("read prometheus query_range body failed: {}", e))?;
        if !status.is_success() {
            return Err(format!("prometheus http {}: {}", status.as_u16(), body));
        }
        let parsed: TransferPromResp = serde_json::from_str(body.as_str())
            .map_err(|e| format!("parse prometheus query_range json failed: {}", e))?;
        if parsed.status != "success" {
            return Err(format!("prometheus status != success: {}", body));
        }
        let mut out = Vec::new();
        for series in parsed.data.result {
            let TransferPromResultSample::Range { values } = series;
            for (ts_s, value_s) in values {
                let Ok(value) = value_s.parse::<f64>() else {
                    continue;
                };
                if !value.is_finite() {
                    continue;
                }
                out.push((((ts_s * 1000.0).round()) as i64, value));
            }
        }
        out.sort_by_key(|(ts, _)| *ts);
        Ok(out)
    }

    pub fn transfer_feature_enabled(&self) -> bool {
        self.transfer_state_store.is_some()
    }

    pub fn access_db_path(&self) -> &str {
        self.access_db_path.as_str()
    }

    pub(crate) fn load_effective_fs_exports(
        &self,
    ) -> Result<BTreeMap<String, FluxonFsExport>, String> {
        let runtime_exports = self.list_fs_export_registry_records()?;
        let overlay_disabled = self.list_fs_export_overlay_disabled_records()?;
        let overlay_upserts = self.list_fs_export_overlay_upsert_records()?;
        Ok(build_effective_fs_exports(
            &self.fs_cache.exports,
            &runtime_exports,
            &overlay_disabled,
            &overlay_upserts,
        ))
    }

    pub(crate) fn ensure_effective_fs_export(
        &self,
        export_name: &str,
    ) -> Result<Option<FluxonFsExport>, String> {
        let export = self.load_effective_fs_exports()?.remove(export_name);
        let Some(export) = export else {
            return Ok(None);
        };
        self.backend.ensure_export_config(export_name, &export)?;
        Ok(Some(export))
    }

    pub fn try_begin_transfer_scan_job(&self, job_id: &str) -> bool {
        self.transfer_scan_scheduler.try_begin_scan_job(job_id)
    }

    pub fn finish_transfer_scan_job(&self, job_id: &str) {
        self.transfer_scan_scheduler.finish_scan_job(job_id);
    }

    pub fn transfer_job_live_detail_snapshot(
        &self,
        job_id: &str,
        current_running_batch_owner_by_batch_id: &BTreeMap<String, String>,
    ) -> Option<FsTransferJobLiveDetailSnapshot> {
        self.transfer_live_detail
            .lock()
            .get(job_id)
            .map(|state| state.snapshot(current_running_batch_owner_by_batch_id))
    }

    pub fn note_transfer_scan_runtime_counts(
        &self,
        job_id: &str,
        queued_scan_unit_count: i64,
        inflight_scan_unit_count: i64,
    ) {
        let mut guard = self.transfer_live_detail.lock();
        let entry = guard.entry(job_id.to_string()).or_default();
        entry.scan.queued_scan_unit_count = queued_scan_unit_count.max(0);
        entry.scan.inflight_scan_unit_count = inflight_scan_unit_count.max(0);
    }

    pub fn note_transfer_scan_epoch_started(&self, job_id: &str) {
        let mut guard = self.transfer_live_detail.lock();
        let entry = guard.entry(job_id.to_string()).or_default();
        entry.reset_scan_for_new_epoch();
    }

    pub fn note_transfer_scan_result_accepted(
        &self,
        job_id: &str,
        result_unix_ms: i64,
        discovered_batch_count: i64,
        discovered_file_count: i64,
        discovered_bytes: i64,
    ) {
        let mut guard = self.transfer_live_detail.lock();
        let entry = guard.entry(job_id.to_string()).or_default();
        let prev_unix_ms = entry.scan.last_scan_result_unix_ms;
        entry.scan.completed_scan_unit_count =
            entry.scan.completed_scan_unit_count.saturating_add(1);
        entry.scan.discovered_batch_count = entry
            .scan
            .discovered_batch_count
            .saturating_add(discovered_batch_count.max(0));
        entry.scan.discovered_file_count = entry
            .scan
            .discovered_file_count
            .saturating_add(discovered_file_count.max(0));
        entry.scan.discovered_bytes = entry
            .scan
            .discovered_bytes
            .saturating_add(discovered_bytes.max(0));
        if prev_unix_ms > 0 && result_unix_ms > prev_unix_ms {
            let elapsed_ms = result_unix_ms.saturating_sub(prev_unix_ms).max(1);
            entry.scan.scan_rate_files_per_sec = discovered_file_count
                .max(0)
                .saturating_mul(1000)
                .saturating_div(elapsed_ms);
            entry.scan.scan_rate_bytes_per_sec = discovered_bytes
                .max(0)
                .saturating_mul(1000)
                .saturating_div(elapsed_ms);
        }
        entry.scan.last_scan_result_unix_ms = result_unix_ms;
    }

    pub fn note_transfer_failure(
        &self,
        job_id: &str,
        unix_ms: i64,
        scope: FsTransferFailureScope,
        message: &str,
    ) {
        let mut guard = self.transfer_live_detail.lock();
        let entry = guard.entry(job_id.to_string()).or_default();
        entry.push_failure(unix_ms, scope, message.to_string());
    }

    pub fn note_transfer_batch_assigned(
        &self,
        job_id: &str,
        batch_id: &str,
        worker_id: &str,
        worker_task_id: &str,
        lease_expire_unix_ms: i64,
    ) {
        let mut guard = self.transfer_live_detail.lock();
        let entry = guard.entry(job_id.to_string()).or_default();
        entry.workers.insert(
            worker_task_id.to_string(),
            TransferWorkerLiveDetailState::new(
                batch_id,
                worker_id,
                worker_task_id,
                lease_expire_unix_ms,
            ),
        );
    }

    pub fn note_transfer_worker_launch_retry(
        &self,
        job_id: &str,
        worker_task_id: &str,
        now_unix_ms: i64,
        err_text: &str,
    ) {
        let mut guard = self.transfer_live_detail.lock();
        let entry = guard.entry(job_id.to_string()).or_default();
        if let Some(worker) = entry.workers.get_mut(worker_task_id) {
            worker.launch_attempt_count = worker.launch_attempt_count.saturating_add(1);
            worker.last_error = err_text.to_string();
        }
        entry.push_failure(
            now_unix_ms,
            FsTransferFailureScope::WorkerLaunch,
            err_text.to_string(),
        );
    }

    pub fn note_transfer_worker_launch_acknowledged(&self, job_id: &str, worker_task_id: &str) {
        let mut guard = self.transfer_live_detail.lock();
        let entry = guard.entry(job_id.to_string()).or_default();
        if let Some(worker) = entry.workers.get_mut(worker_task_id) {
            worker.state = FsTransferWorkerAttemptState::Running;
            worker.last_error.clear();
        }
    }

    pub(crate) fn note_transfer_worker_heartbeat(
        &self,
        job_id: &str,
        worker_task_id: &str,
        heartbeat_received_unix_ms: i64,
        lease_expire_unix_ms: i64,
        telemetry: Option<&FsTransferWorkerHeartbeatLiveTelemetry>,
    ) {
        let mut guard = self.transfer_live_detail.lock();
        let entry = guard.entry(job_id.to_string()).or_default();
        if let Some(worker) = entry.workers.get_mut(worker_task_id) {
            worker.state = FsTransferWorkerAttemptState::Running;
            worker.last_heartbeat_unix_ms = heartbeat_received_unix_ms;
            worker.lease_expire_unix_ms = lease_expire_unix_ms;
            if let Some(telemetry) = telemetry {
                worker.current_bandwidth_bytes_per_sec =
                    telemetry.window_goodput_bytes_per_sec.max(0);
                worker.total_written_bytes = telemetry.total_written_bytes.max(0);
                worker.desired_file_lanes = telemetry.desired_file_lanes.max(0);
            }
        }
        let totals = Self::transfer_job_live_totals_for_history(entry);
        drop(guard);
        self.transfer_history_emit_job_point_if_due(
            job_id,
            heartbeat_received_unix_ms,
            totals.0,
            totals.1,
            totals.2,
            totals.3,
        );
    }

    pub fn note_transfer_worker_stopped(
        &self,
        job_id: &str,
        worker_task_id: &str,
        now_unix_ms: i64,
        stop_reason: Option<fluxon_fs_core::config::FluxonFsTransferWorkerStopReasonWire>,
        err_text: &str,
    ) {
        let mut guard = self.transfer_live_detail.lock();
        let entry = guard.entry(job_id.to_string()).or_default();
        if let Some(worker) = entry.workers.get_mut(worker_task_id) {
            worker.state = FsTransferWorkerAttemptState::Stopped;
            worker.stop_reason = stop_reason;
            worker.last_error = err_text.to_string();
            worker.current_bandwidth_bytes_per_sec = 0;
        }
        if !err_text.trim().is_empty() {
            entry.push_failure(
                now_unix_ms,
                FsTransferFailureScope::WorkerStop,
                err_text.to_string(),
            );
        }
        let totals = Self::transfer_job_live_totals_for_history(entry);
        drop(guard);
        self.transfer_history_emit_job_point_if_due(
            job_id,
            now_unix_ms,
            totals.0,
            totals.1,
            totals.2,
            totals.3,
        );
    }

    pub(crate) fn note_transfer_worker_result_applied(
        &self,
        job_id: &str,
        worker_task_id: &str,
        visible_file_count: i64,
        visible_bytes: i64,
        final_telemetry: Option<&FsTransferWorkerHeartbeatLiveTelemetry>,
    ) {
        let mut guard = self.transfer_live_detail.lock();
        let entry = guard.entry(job_id.to_string()).or_default();
        let mut emit_final_bandwidth_bytes_per_sec = 0_i64;
        if let Some(worker) = entry.workers.get_mut(worker_task_id) {
            worker.state = FsTransferWorkerAttemptState::Finished;
            worker.visible_file_count = visible_file_count.max(0);
            worker.visible_bytes = visible_bytes.max(0);
            if let Some(telemetry) = final_telemetry {
                worker.total_written_bytes = worker
                    .total_written_bytes
                    .max(telemetry.total_written_bytes.max(0));
                worker.desired_file_lanes = telemetry.desired_file_lanes.max(0);
                emit_final_bandwidth_bytes_per_sec = telemetry.window_goodput_bytes_per_sec.max(0);
            }
            // Emit the last goodput sample once, but clear live bandwidth in
            // state so finished workers do not inflate later history points.
            worker.current_bandwidth_bytes_per_sec = 0;
            worker.last_error.clear();
            worker.stop_reason = None;
        }
        let now_unix_ms = Utc::now().timestamp_millis();
        let totals = Self::transfer_job_live_totals_for_history(entry);
        let emit_bandwidth_bytes_per_sec =
            totals.2.saturating_add(emit_final_bandwidth_bytes_per_sec);
        drop(guard);
        self.transfer_history_emit_job_point_if_due(
            job_id,
            now_unix_ms,
            totals.0,
            totals.1,
            emit_bandwidth_bytes_per_sec,
            totals.3,
        );
    }

    pub fn note_transfer_job_cancelled(&self, job_id: &str, now_unix_ms: i64) {
        let mut guard = self.transfer_live_detail.lock();
        let entry = guard.entry(job_id.to_string()).or_default();
        for worker in entry.workers.values_mut() {
            if worker.state == FsTransferWorkerAttemptState::Finished {
                continue;
            }
            worker.state = FsTransferWorkerAttemptState::Stopped;
            worker.stop_reason =
                Some(fluxon_fs_core::config::FluxonFsTransferWorkerStopReasonWire::Cancelled);
            worker.last_error = "transfer job cancelled by user".to_string();
            worker.current_bandwidth_bytes_per_sec = 0;
            worker.lease_expire_unix_ms = 0;
        }
        entry.scan.queued_scan_unit_count = 0;
        entry.scan.inflight_scan_unit_count = 0;
        entry.scan.scan_rate_files_per_sec = 0;
        entry.scan.scan_rate_bytes_per_sec = 0;
        let totals = Self::transfer_job_live_totals_for_history(entry);
        drop(guard);
        self.transfer_history_emit_job_point_if_due(
            job_id,
            now_unix_ms,
            totals.0,
            totals.1,
            totals.2,
            totals.3,
        );
    }

    pub fn access_model_json_text(&self) -> Result<String, String> {
        let permission_list = self.permission_list.read();
        let model = runtime_access_model_from_s3_permission_list(&permission_list)?;
        runtime_access_model_to_json_text(&model)
    }

    pub fn permission_list_snapshot(&self) -> Vec<FluxonFsS3PermissionAccount> {
        self.permission_list.read().clone()
    }

    pub async fn snapshot_fs_master_admin(&self) -> Result<FsMasterAdminSnapshot, String> {
        let members = self
            .fs_master_admin_backend
            .list_fs_master_members()
            .await?;
        let online_member_ids = self
            .fs_master_admin_backend
            .list_fs_master_online_member_ids()
            .await?;
        let online_agent_ids: BTreeSet<String> = members
            .iter()
            .filter(|member| member.kind == FsMasterMemberKind::Agent)
            .map(|member| member.member_id.clone())
            .filter(|member_id| !member_id.trim().is_empty())
            .filter(|member_id| online_member_ids.contains(member_id))
            .collect();
        let mounts: Vec<FsMasterMountRecord> = self
            .list_fs_mount_registry_records()?
            .into_iter()
            .filter(|row| online_member_ids.contains(&row.external_instance_key))
            .map(|row| FsMasterMountRecord {
                external_instance_key: row.external_instance_key,
                local_mount_dir_abs: row.local_mount_dir_abs,
                remote_root_dir_abs: row.remote_root_dir_abs,
                updated_unix_ms: row.updated_unix_ms,
            })
            .collect();
        let mut runtime_exports = self.list_fs_export_registry_records()?;
        runtime_exports.retain(|row| online_member_ids.contains(&row.agent_instance_key));
        runtime_exports.sort_by(|a, b| {
            (
                a.agent_instance_key.clone(),
                a.export_name.clone(),
                a.remote_root_dir_abs.clone(),
            )
                .cmp(&(
                    b.agent_instance_key.clone(),
                    b.export_name.clone(),
                    b.remote_root_dir_abs.clone(),
                ))
        });
        let runtime_agent_exports =
            build_fs_master_admin_runtime_agent_exports(&online_agent_ids, &runtime_exports);
        let overlay_disabled = self.list_fs_export_overlay_disabled_records()?;
        let overlay_upserts = self.list_fs_export_overlay_upsert_records()?;
        let managed_agent_exports = build_fs_master_admin_managed_agent_exports(
            &online_agent_ids,
            &runtime_agent_exports,
            &overlay_disabled,
            &overlay_upserts,
        );
        Ok(FsMasterAdminSnapshot {
            members,
            mounts,
            runtime_agent_exports,
            managed_agent_exports,
        })
    }

    pub async fn list_fs_master_agent_dir(
        &self,
        agent_instance_key: &str,
        dir_abs: &str,
    ) -> Result<Vec<FsMasterAdminBrowseDirEntry>, String> {
        self.fs_master_admin_backend
            .list_fs_master_agent_dir(agent_instance_key.to_string(), dir_abs.to_string())
            .await
    }

    pub fn persist_fs_mount_registry_record(
        &self,
        record: &FsMountRegistryRecord,
    ) -> Result<(), String> {
        let conn = self.access_db.lock();
        persist_fs_mount_registry_record_to_db(&conn, record)
    }

    pub fn list_fs_mount_registry_records(&self) -> Result<Vec<FsMountRegistryRecord>, String> {
        let conn = self.access_db.lock();
        load_fs_mount_registry_records_from_db(&conn)
    }

    pub fn replace_fs_export_registry_for_agent(
        &self,
        agent_instance_key: &str,
        records: &[FsExportRegistryRecord],
    ) -> Result<(), String> {
        let mut conn = self.access_db.lock();
        replace_fs_export_registry_for_agent_in_db(&mut conn, agent_instance_key, records)
    }

    pub fn delete_fs_export_registry_for_agent(
        &self,
        agent_instance_key: &str,
    ) -> Result<(), String> {
        let conn = self.access_db.lock();
        delete_fs_export_registry_for_agent_in_db(&conn, agent_instance_key)
    }

    pub fn list_fs_export_registry_records(&self) -> Result<Vec<FsExportRegistryRecord>, String> {
        let conn = self.access_db.lock();
        load_fs_export_registry_records_from_db(&conn)
    }

    pub fn upsert_fs_export_overlay_disabled(
        &self,
        agent_instance_key: &str,
        export_name: &str,
    ) -> Result<(), String> {
        let conn = self.access_db.lock();
        upsert_fs_export_overlay_disabled_in_db(&conn, agent_instance_key, export_name)
    }

    pub fn delete_fs_export_overlay_disabled(
        &self,
        agent_instance_key: &str,
        export_name: &str,
    ) -> Result<(), String> {
        let conn = self.access_db.lock();
        delete_fs_export_overlay_disabled_in_db(&conn, agent_instance_key, export_name)
    }

    pub fn list_fs_export_overlay_disabled_records(
        &self,
    ) -> Result<Vec<FsExportOverlayDisabledRecord>, String> {
        let conn = self.access_db.lock();
        load_fs_export_overlay_disabled_records_from_db(&conn)
    }

    pub fn upsert_fs_export_overlay_upsert(
        &self,
        agent_instance_key: &str,
        export_name: &str,
        export: &FluxonFsExport,
    ) -> Result<(), String> {
        let conn = self.access_db.lock();
        upsert_fs_export_overlay_upsert_in_db(&conn, agent_instance_key, export_name, export)
    }

    pub fn delete_fs_export_overlay_upsert(
        &self,
        agent_instance_key: &str,
        export_name: &str,
    ) -> Result<(), String> {
        let conn = self.access_db.lock();
        delete_fs_export_overlay_upsert_in_db(&conn, agent_instance_key, export_name)
    }

    pub fn list_fs_export_overlay_upsert_records(
        &self,
    ) -> Result<Vec<FsExportOverlayUpsertRecord>, String> {
        let conn = self.access_db.lock();
        load_fs_export_overlay_upsert_records_from_db(&conn)
    }

    pub fn add_fs_master_export(
        &self,
        agent_instance_key: &str,
        export_name: &str,
        remote_root_dir_abs: &str,
    ) -> Result<(), String> {
        if is_admin_browse_export_name_v1(export_name) {
            return Err(format!("export_name prefix is reserved: {}", export_name));
        }
        if !is_valid_bucket_name(export_name) {
            return Err(format!(
                "invalid export name for S3 bucket: export_name={} (expected 3-63 chars, [a-z0-9-], no leading/trailing '-')",
                export_name
            ));
        }
        let remote_root_dir_abs = safe_abs_dirpath(remote_root_dir_abs).map_err(|e| {
            format!(
                "remote_root_dir_abs must be a normalized absolute path: input={} err={}",
                remote_root_dir_abs, e
            )
        })?;
        let conn = self.access_db.lock();
        delete_fs_export_overlay_disabled_in_db(&conn, agent_instance_key, export_name)?;
        let export =
            agent_registry_export_for_name_and_root_v1(export_name, remote_root_dir_abs.as_str());
        upsert_fs_export_overlay_upsert_in_db(&conn, agent_instance_key, export_name, &export)
    }

    pub fn remove_fs_master_export(
        &self,
        agent_instance_key: &str,
        export_name: &str,
    ) -> Result<(), String> {
        let conn = self.access_db.lock();
        let has_declared_agent_registry_export = matches!(
            self.fs_cache.exports.get(export_name),
            Some(export) if export.routing_mode == FluxonFsExportRoutingMode::AgentRegistry
        );
        let runtime_exports = load_fs_export_registry_records_from_db(&conn)?;
        let has_runtime_export = runtime_exports.iter().any(|record| {
            record.agent_instance_key == agent_instance_key && record.export_name == export_name
        });
        let overlay_upserts = load_fs_export_overlay_upsert_records_from_db(&conn)?;
        let has_overlay_upsert = overlay_upserts.iter().any(|record| {
            record.agent_instance_key == agent_instance_key && record.export_name == export_name
        });
        if has_overlay_upsert {
            delete_fs_export_overlay_upsert_in_db(&conn, agent_instance_key, export_name)?;
        }
        if has_declared_agent_registry_export || has_runtime_export || has_overlay_upsert {
            return upsert_fs_export_overlay_disabled_in_db(&conn, agent_instance_key, export_name);
        }
        delete_fs_export_overlay_disabled_in_db(&conn, agent_instance_key, export_name)
    }

    pub fn load_fs_export_overlay_for_agent(
        &self,
        agent_instance_key: &str,
    ) -> Result<FsAgentExportOverlayWire, String> {
        let conn = self.access_db.lock();
        load_fs_export_overlay_for_agent_from_db(&conn, agent_instance_key)
    }

    fn persist_permission_list_state(
        &self,
        permission_list: &[FluxonFsS3PermissionAccount],
    ) -> Result<(), String> {
        let mut conn = self.access_db.lock();
        let normalized_permission_list = persist_permission_list_to_db(&mut conn, permission_list)?;
        *self.permission_list.write() = normalized_permission_list;
        Ok(())
    }

    fn create_ui_transfer_task(
        &self,
        owner_username: String,
        kind: UiTransferTaskKind,
        name: String,
        total_bytes: i64,
        summary: String,
        detail: String,
        source: UiTransferTaskEndpoint,
        target: UiTransferTaskEndpoint,
    ) -> UiTransferTaskHandle {
        let task_id = Uuid::new_v4().to_string();
        let handle = UiTransferTaskHandle::new(
            owner_username,
            UiTransferTaskSnapshot {
                ok: true,
                task_id: task_id.clone(),
                kind,
                name,
                started_at_ms: Utc::now().timestamp_millis(),
                done_bytes: 0,
                total_bytes: total_bytes.max(0),
                stage: UiTransferTaskStage::Running,
                summary,
                detail,
                source,
                target,
                can_pause: true,
                can_resume: false,
                can_cancel: true,
            },
        );
        self.ui_transfer_tasks
            .write()
            .insert(task_id, handle.clone());
        handle
    }

    fn ui_transfer_task_for_owner(
        &self,
        task_id: &str,
        owner_username: &str,
    ) -> Option<UiTransferTaskHandle> {
        let handle = self.ui_transfer_tasks.read().get(task_id).cloned()?;
        if handle.owner_username() != owner_username {
            return None;
        }
        Some(handle)
    }

    fn list_ui_transfer_tasks_for_owner(
        &self,
        owner_username: &str,
    ) -> Vec<UiTransferTaskSnapshot> {
        let mut out: Vec<UiTransferTaskSnapshot> = self
            .ui_transfer_tasks
            .read()
            .values()
            .filter(|handle| handle.owner_username() == owner_username)
            .map(|handle| handle.snapshot())
            .collect();
        out.sort_by(|a, b| {
            b.started_at_ms
                .cmp(&a.started_at_ms)
                .then_with(|| b.task_id.cmp(&a.task_id))
        });
        out
    }
}

fn build_fs_master_admin_runtime_agent_exports(
    online_agent_ids: &BTreeSet<String>,
    runtime_exports: &[FsExportRegistryRecord],
) -> Vec<FsMasterAdminRuntimeAgentExports> {
    let mut runtime_by_agent: BTreeMap<String, Vec<FsMasterAdminRuntimeExportRecord>> =
        BTreeMap::new();

    for agent_instance_key in online_agent_ids {
        runtime_by_agent.insert(agent_instance_key.clone(), Vec::new());
    }

    for record in runtime_exports {
        if !online_agent_ids.contains(&record.agent_instance_key) {
            continue;
        }
        if is_admin_browse_export_name_v1(record.export_name.as_str()) {
            continue;
        }
        runtime_by_agent
            .get_mut(&record.agent_instance_key)
            .unwrap()
            .push(FsMasterAdminRuntimeExportRecord {
                export_name: record.export_name.clone(),
                remote_root_dir_abs: record.remote_root_dir_abs.clone(),
            });
    }

    let mut out: Vec<FsMasterAdminRuntimeAgentExports> = Vec::new();
    for agent_instance_key in online_agent_ids {
        let mut runtime_exports = runtime_by_agent.remove(agent_instance_key).unwrap();
        runtime_exports.sort_by(|a, b| {
            (a.export_name.clone(), a.remote_root_dir_abs.clone())
                .cmp(&(b.export_name.clone(), b.remote_root_dir_abs.clone()))
        });
        out.push(FsMasterAdminRuntimeAgentExports {
            agent_instance_key: agent_instance_key.clone(),
            runtime_exports,
        });
    }
    out
}

fn build_effective_fs_exports(
    static_exports: &BTreeMap<String, FluxonFsExport>,
    runtime_exports: &[FsExportRegistryRecord],
    overlay_disabled: &[FsExportOverlayDisabledRecord],
    overlay_upserts: &[FsExportOverlayUpsertRecord],
) -> BTreeMap<String, FluxonFsExport> {
    let mut exports_by_agent: BTreeMap<String, BTreeMap<String, FluxonFsExport>> = BTreeMap::new();
    for record in runtime_exports {
        if is_admin_browse_export_name_v1(record.export_name.as_str()) {
            continue;
        }
        exports_by_agent
            .entry(record.agent_instance_key.clone())
            .or_default()
            .insert(record.export_name.clone(), record.export.clone());
    }
    for record in overlay_upserts {
        if is_admin_browse_export_name_v1(record.export_name.as_str()) {
            continue;
        }
        exports_by_agent
            .entry(record.agent_instance_key.clone())
            .or_default()
            .insert(record.export_name.clone(), record.export.clone());
    }
    for record in overlay_disabled {
        if let Some(exports) = exports_by_agent.get_mut(record.agent_instance_key.as_str()) {
            exports.remove(record.export_name.as_str());
        }
    }

    let mut out: BTreeMap<String, FluxonFsExport> = static_exports
        .iter()
        .filter(|(export_name, _)| !is_admin_browse_export_name_v1(export_name.as_str()))
        .map(|(export_name, export)| (export_name.clone(), export.clone()))
        .collect();
    for exports in exports_by_agent.values() {
        for (export_name, export) in exports {
            out.entry(export_name.clone())
                .or_insert_with(|| export.clone());
        }
    }
    out
}

fn build_fs_master_admin_managed_agent_exports(
    online_agent_ids: &BTreeSet<String>,
    runtime_agent_exports: &[FsMasterAdminRuntimeAgentExports],
    overlay_disabled: &[FsExportOverlayDisabledRecord],
    overlay_upserts: &[FsExportOverlayUpsertRecord],
) -> Vec<FsMasterAdminManagedAgentExports> {
    let mut runtime_by_agent: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    for agent in runtime_agent_exports {
        let mut by_name: BTreeMap<String, String> = BTreeMap::new();
        for record in &agent.runtime_exports {
            by_name.insert(
                record.export_name.clone(),
                record.remote_root_dir_abs.clone(),
            );
        }
        runtime_by_agent.insert(agent.agent_instance_key.clone(), by_name);
    }

    let mut out: Vec<FsMasterAdminManagedAgentExports> = Vec::new();
    for agent_instance_key in online_agent_ids {
        let mut managed_by_name: BTreeMap<String, String> = BTreeMap::new();
        if let Some(runtime_exports) = runtime_by_agent.get(agent_instance_key) {
            for (export_name, remote_root_dir_abs) in runtime_exports {
                managed_by_name.insert(export_name.clone(), remote_root_dir_abs.clone());
            }
        }
        for record in overlay_upserts {
            if record.agent_instance_key != *agent_instance_key {
                continue;
            }
            if is_admin_browse_export_name_v1(record.export_name.as_str()) {
                continue;
            }
            managed_by_name.insert(
                record.export_name.clone(),
                record.export.remote_root_dir_abs.clone(),
            );
        }
        for record in overlay_disabled {
            if record.agent_instance_key != *agent_instance_key {
                continue;
            }
            managed_by_name.remove(record.export_name.as_str());
        }
        let managed_exports: Vec<FsMasterAdminManagedExportRecord> = managed_by_name
            .into_iter()
            .map(
                |(export_name, remote_root_dir_abs)| FsMasterAdminManagedExportRecord {
                    export_name,
                    remote_root_dir_abs,
                },
            )
            .collect();
        out.push(FsMasterAdminManagedAgentExports {
            agent_instance_key: agent_instance_key.clone(),
            managed_exports,
        });
    }
    out
}

fn open_access_db(path: &str) -> Result<Connection, String> {
    if path.trim().is_empty() {
        return Err("access_db_path must be non-empty".to_string());
    }
    let db_path = std::path::Path::new(path);
    if let Some(parent_dir) = db_path.parent() {
        if !parent_dir.as_os_str().is_empty() {
            std::fs::create_dir_all(parent_dir).map_err(|e| {
                format!(
                    "create access db parent dir failed: path={} parent={} err={}",
                    path,
                    parent_dir.display(),
                    e
                )
            })?;
        }
    }
    let conn = Connection::open(path).map_err(|e| format!("open access db failed: {}", e))?;
    conn.execute_batch(
        r#"
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 5000;
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS access_users (
    username TEXT PRIMARY KEY,
    password TEXT NOT NULL,
    can_manage_users INTEGER NOT NULL CHECK (can_manage_users IN (0, 1))
);
CREATE TABLE IF NOT EXISTS access_scope (
    scope_id INTEGER PRIMARY KEY AUTOINCREMENT,
    export_name TEXT NOT NULL,
    prefix TEXT NOT NULL,
    mode TEXT NOT NULL,
    UNIQUE (export_name, prefix, mode)
);
CREATE TABLE IF NOT EXISTS access_scope_binding (
    scope_id INTEGER NOT NULL,
    username TEXT NOT NULL,
    PRIMARY KEY (scope_id, username),
    FOREIGN KEY (scope_id) REFERENCES access_scope(scope_id) ON DELETE CASCADE,
    FOREIGN KEY (username) REFERENCES access_users(username) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS fs_mount_registry (
    external_instance_key TEXT NOT NULL,
    local_mount_dir_abs TEXT NOT NULL,
    remote_root_dir_abs TEXT NOT NULL,
    updated_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (external_instance_key, local_mount_dir_abs)
);
CREATE TABLE IF NOT EXISTS fs_export_registry (
    export_name TEXT NOT NULL,
    agent_instance_key TEXT NOT NULL,
    remote_root_dir_abs TEXT NOT NULL,
    export_json TEXT NOT NULL,
    updated_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (export_name, agent_instance_key)
);
CREATE TABLE IF NOT EXISTS fs_export_overlay_disabled (
    agent_instance_key TEXT NOT NULL,
    export_name TEXT NOT NULL,
    updated_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (agent_instance_key, export_name)
);
CREATE TABLE IF NOT EXISTS fs_export_overlay_upsert (
    agent_instance_key TEXT NOT NULL,
    export_name TEXT NOT NULL,
    export_json TEXT NOT NULL,
    updated_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (agent_instance_key, export_name)
);
"#,
    )
    .map_err(|e| format!("initialize access db schema failed: {}", e))?;
    ensure_fs_export_registry_export_json_column(&conn)?;
    Ok(conn)
}

fn build_transfer_state_store(
    cfg: FluxonFsTransferStateStoreConfig,
) -> Result<Arc<dyn TransferStateStore>, String> {
    let FluxonFsTransferStateStoreKind::TiKv(tikv) = cfg.kind;
    Ok(Arc::new(TiKvTransferStateStore::new(&tikv)?))
}

fn ensure_fs_export_registry_export_json_column(conn: &Connection) -> Result<(), String> {
    let mut has_export_json = false;
    let mut stmt = conn
        .prepare("PRAGMA table_info(fs_export_registry)")
        .map_err(|e| format!("prepare fs_export_registry table_info failed: {}", e))?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|e| format!("query fs_export_registry table_info failed: {}", e))?;
    for row in rows {
        let column_name =
            row.map_err(|e| format!("decode fs_export_registry table_info row failed: {}", e))?;
        if column_name == "export_json" {
            has_export_json = true;
            break;
        }
    }
    if has_export_json {
        return Ok(());
    }
    conn.execute(
        "ALTER TABLE fs_export_registry ADD COLUMN export_json TEXT NOT NULL DEFAULT ''",
        [],
    )
    .map_err(|e| format!("alter fs_export_registry add export_json failed: {}", e))?;
    conn.execute("DELETE FROM fs_export_registry", [])
        .map_err(|e| format!("clear legacy fs_export_registry rows failed: {}", e))?;
    Ok(())
}

fn access_db_user_count(conn: &Connection) -> Result<i64, String> {
    conn.query_row("SELECT COUNT(*) FROM access_users", [], |row| {
        row.get::<_, i64>(0)
    })
    .map_err(|e| format!("query access user count failed: {}", e))
}

fn load_access_model_from_db(conn: &Connection) -> Result<AccessModel, String> {
    let mut users: Vec<AccessUser> = Vec::new();
    let mut user_stmt = conn
        .prepare(
            "SELECT username, password, can_manage_users
             FROM access_users
             ORDER BY username",
        )
        .map_err(|e| format!("prepare access_users query failed: {}", e))?;
    let user_rows = user_stmt
        .query_map([], |row| {
            Ok(AccessUser {
                username: row.get(0)?,
                password: row.get(1)?,
                can_manage_users: row.get::<_, i64>(2)? != 0,
            })
        })
        .map_err(|e| format!("query access_users failed: {}", e))?;
    for row in user_rows {
        users.push(row.map_err(|e| format!("decode access_users row failed: {}", e))?);
    }

    let mut scope_map: BTreeMap<(String, String, ScopeAccessMode), Vec<String>> = BTreeMap::new();
    let mut scope_stmt = conn
        .prepare(
            "SELECT s.export_name, s.prefix, s.mode, b.username
             FROM access_scope s
             LEFT JOIN access_scope_binding b ON b.scope_id = s.scope_id
             ORDER BY s.export_name, s.prefix, s.mode, b.username",
        )
        .map_err(|e| format!("prepare access_scope query failed: {}", e))?;
    let scope_rows = scope_stmt
        .query_map([], |row| {
            let mode_raw: String = row.get(2)?;
            let mode = ScopeAccessMode::from_form_value(&mode_raw).ok_or_else(|| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("invalid scope_access mode: {}", mode_raw),
                    )),
                )
            })?;
            let username: Option<String> = row.get(3)?;
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                mode,
                username,
            ))
        })
        .map_err(|e| format!("query access_scope failed: {}", e))?;
    for row in scope_rows {
        let (export_name, prefix, mode, username) =
            row.map_err(|e| format!("decode access_scope row failed: {}", e))?;
        let usernames = scope_map.entry((export_name, prefix, mode)).or_default();
        let Some(username) = username else {
            return Err("scope_access row without any bound user".to_string());
        };
        usernames.push(username);
    }

    let mut scope_access: Vec<ScopeAccess> = Vec::new();
    for ((export_name, prefix, mode), usernames) in scope_map {
        scope_access.push(ScopeAccess {
            export_name,
            prefix,
            mode,
            usernames,
        });
    }
    Ok(AccessModel {
        users,
        scope_access,
    })
}

fn load_permission_list_from_db(
    conn: &Connection,
) -> Result<Vec<FluxonFsS3PermissionAccount>, String> {
    let model = load_access_model_from_db(conn)?;
    permission_list_from_access_model(&model)
}

fn load_or_bootstrap_permission_list_from_db(
    conn: &mut Connection,
    bootstrap_access_model: &AccessModel,
    _fs_cache: &FluxonFsGlobalConfig,
) -> Result<Vec<FluxonFsS3PermissionAccount>, String> {
    if access_db_user_count(conn)? != 0 {
        return load_permission_list_from_db(conn);
    }
    persist_access_model_to_db(conn, bootstrap_access_model)
}

fn persist_access_model_to_db(
    conn: &mut Connection,
    model: &AccessModel,
) -> Result<Vec<FluxonFsS3PermissionAccount>, String> {
    let permission_list = permission_list_from_access_model(model)?;
    let normalized_model = access_model_from_permission_list(&permission_list)?;
    let normalized_permission_list = permission_list_from_access_model(&normalized_model)?;
    // English note: control-plane mutations rewrite the normalized access snapshot inside one
    // transaction. Data-plane authorization reads stay on the in-memory cache, so this does not
    // add per-request DB cost.
    let tx = conn
        .transaction()
        .map_err(|e| format!("begin state db transaction failed: {}", e))?;
    tx.execute("DELETE FROM access_scope_binding", [])
        .map_err(|e| format!("clear access_scope_binding failed: {}", e))?;
    tx.execute("DELETE FROM access_scope", [])
        .map_err(|e| format!("clear access_scope failed: {}", e))?;
    tx.execute("DELETE FROM access_users", [])
        .map_err(|e| format!("clear access_users failed: {}", e))?;

    for user in &normalized_model.users {
        tx.execute(
            "INSERT INTO access_users(username, password, can_manage_users) VALUES (?1, ?2, ?3)",
            params![
                user.username,
                user.password,
                if user.can_manage_users { 1_i64 } else { 0_i64 }
            ],
        )
        .map_err(|e| format!("insert access_user {} failed: {}", user.username, e))?;
    }

    for scope in &normalized_model.scope_access {
        tx.execute(
            "INSERT INTO access_scope(export_name, prefix, mode) VALUES (?1, ?2, ?3)",
            params![scope.export_name, scope.prefix, scope.mode.form_value()],
        )
        .map_err(|e| {
            format!(
                "insert scope_access export_name={} prefix={} mode={} failed: {}",
                scope.export_name,
                scope.prefix,
                scope.mode.form_value(),
                e
            )
        })?;
        let scope_id = tx.last_insert_rowid();
        for username in &scope.usernames {
            tx.execute(
                "INSERT INTO access_scope_binding(scope_id, username) VALUES (?1, ?2)",
                params![scope_id, username],
            )
            .map_err(|e| {
                format!(
                    "insert scope_access binding export_name={} prefix={} mode={} username={} failed: {}",
                    scope.export_name,
                    scope.prefix,
                    scope.mode.form_value(),
                    username,
                    e
                )
            })?;
        }
    }

    tx.commit()
        .map_err(|e| format!("commit state db transaction failed: {}", e))?;
    Ok(normalized_permission_list)
}

fn persist_permission_list_to_db(
    conn: &mut Connection,
    permission_list: &[FluxonFsS3PermissionAccount],
) -> Result<Vec<FluxonFsS3PermissionAccount>, String> {
    let model = access_model_from_permission_list(permission_list)?;
    persist_access_model_to_db(conn, &model)
}

fn persist_fs_mount_registry_record_to_db(
    conn: &Connection,
    record: &FsMountRegistryRecord,
) -> Result<(), String> {
    conn.execute(
        "INSERT INTO fs_mount_registry(external_instance_key, local_mount_dir_abs, remote_root_dir_abs, updated_unix_ms)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(external_instance_key, local_mount_dir_abs)
         DO UPDATE SET remote_root_dir_abs=excluded.remote_root_dir_abs, updated_unix_ms=excluded.updated_unix_ms",
        params![
            record.external_instance_key,
            record.local_mount_dir_abs,
            record.remote_root_dir_abs,
            record.updated_unix_ms,
        ],
    )
    .map_err(|e| {
        format!(
            "upsert fs_mount_registry external_instance_key={} local_mount_dir_abs={} failed: {}",
            record.external_instance_key, record.local_mount_dir_abs, e
        )
    })?;
    Ok(())
}

fn load_fs_mount_registry_records_from_db(
    conn: &Connection,
) -> Result<Vec<FsMountRegistryRecord>, String> {
    let mut stmt = conn
        .prepare(
            "SELECT external_instance_key, local_mount_dir_abs, remote_root_dir_abs, updated_unix_ms
             FROM fs_mount_registry
             ORDER BY external_instance_key, local_mount_dir_abs",
        )
        .map_err(|e| format!("prepare fs_mount_registry query failed: {}", e))?;
    let rows = stmt
        .query_map([], |row| {
            Ok(FsMountRegistryRecord {
                external_instance_key: row.get(0)?,
                local_mount_dir_abs: row.get(1)?,
                remote_root_dir_abs: row.get(2)?,
                updated_unix_ms: row.get(3)?,
            })
        })
        .map_err(|e| format!("query fs_mount_registry failed: {}", e))?;
    let mut out: Vec<FsMountRegistryRecord> = Vec::new();
    for row in rows {
        out.push(row.map_err(|e| format!("decode fs_mount_registry row failed: {}", e))?);
    }
    Ok(out)
}

fn replace_fs_export_registry_for_agent_in_db(
    conn: &mut Connection,
    agent_instance_key: &str,
    records: &[FsExportRegistryRecord],
) -> Result<(), String> {
    let tx = conn
        .transaction()
        .map_err(|e| format!("begin fs_export_registry transaction failed: {}", e))?;
    tx.execute(
        "DELETE FROM fs_export_registry WHERE agent_instance_key = ?1",
        params![agent_instance_key],
    )
    .map_err(|e| {
        format!(
            "delete fs_export_registry rows for agent_instance_key={} failed: {}",
            agent_instance_key, e
        )
    })?;
    for record in records {
        let export_json = serde_json::to_string(&record.export).map_err(|e| {
            format!(
                "serialize fs_export_registry export_name={} agent_instance_key={} failed: {}",
                record.export_name, record.agent_instance_key, e
            )
        })?;
        tx.execute(
            "INSERT INTO fs_export_registry(export_name, agent_instance_key, remote_root_dir_abs, export_json, updated_unix_ms)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                record.export_name,
                record.agent_instance_key,
                record.remote_root_dir_abs,
                export_json,
                record.updated_unix_ms,
            ],
        )
        .map_err(|e| {
            format!(
                "insert fs_export_registry export_name={} agent_instance_key={} failed: {}",
                record.export_name, record.agent_instance_key, e
            )
        })?;
    }
    tx.commit()
        .map_err(|e| format!("commit fs_export_registry transaction failed: {}", e))?;
    Ok(())
}

fn delete_fs_export_registry_for_agent_in_db(
    conn: &Connection,
    agent_instance_key: &str,
) -> Result<(), String> {
    conn.execute(
        "DELETE FROM fs_export_registry WHERE agent_instance_key = ?1",
        params![agent_instance_key],
    )
    .map_err(|e| {
        format!(
            "delete fs_export_registry rows for agent_instance_key={} failed: {}",
            agent_instance_key, e
        )
    })?;
    Ok(())
}

fn load_fs_export_registry_records_from_db(
    conn: &Connection,
) -> Result<Vec<FsExportRegistryRecord>, String> {
    let mut stmt = conn
        .prepare(
            "SELECT export_name, agent_instance_key, remote_root_dir_abs, export_json, updated_unix_ms
             FROM fs_export_registry
             ORDER BY export_name, agent_instance_key",
        )
        .map_err(|e| format!("prepare fs_export_registry query failed: {}", e))?;
    let rows = stmt
        .query_map([], |row| {
            let export_json: String = row.get(3)?;
            let export = serde_json::from_str::<FluxonFsExport>(&export_json).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    3,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("parse fs_export_registry export_json failed: {}", e),
                    )),
                )
            })?;
            Ok(FsExportRegistryRecord {
                export_name: row.get(0)?,
                agent_instance_key: row.get(1)?,
                remote_root_dir_abs: row.get(2)?,
                export,
                updated_unix_ms: row.get(4)?,
            })
        })
        .map_err(|e| format!("query fs_export_registry failed: {}", e))?;
    let mut out: Vec<FsExportRegistryRecord> = Vec::new();
    for row in rows {
        out.push(row.map_err(|e| format!("decode fs_export_registry row failed: {}", e))?);
    }
    Ok(out)
}

fn upsert_fs_export_overlay_disabled_in_db(
    conn: &Connection,
    agent_instance_key: &str,
    export_name: &str,
) -> Result<(), String> {
    let updated_unix_ms = Utc::now().timestamp_millis();
    conn.execute(
        "INSERT INTO fs_export_overlay_disabled(agent_instance_key, export_name, updated_unix_ms)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(agent_instance_key, export_name)
         DO UPDATE SET updated_unix_ms=excluded.updated_unix_ms",
        params![agent_instance_key, export_name, updated_unix_ms],
    )
    .map_err(|e| {
        format!(
            "upsert fs_export_overlay_disabled agent_instance_key={} export_name={} failed: {}",
            agent_instance_key, export_name, e
        )
    })?;
    Ok(())
}

fn delete_fs_export_overlay_disabled_in_db(
    conn: &Connection,
    agent_instance_key: &str,
    export_name: &str,
) -> Result<(), String> {
    conn.execute(
        "DELETE FROM fs_export_overlay_disabled WHERE agent_instance_key = ?1 AND export_name = ?2",
        params![agent_instance_key, export_name],
    )
    .map_err(|e| {
        format!(
            "delete fs_export_overlay_disabled agent_instance_key={} export_name={} failed: {}",
            agent_instance_key, export_name, e
        )
    })?;
    Ok(())
}

fn load_fs_export_overlay_disabled_records_from_db(
    conn: &Connection,
) -> Result<Vec<FsExportOverlayDisabledRecord>, String> {
    let mut stmt = conn
        .prepare(
            "SELECT agent_instance_key, export_name, updated_unix_ms
             FROM fs_export_overlay_disabled
             ORDER BY agent_instance_key, export_name",
        )
        .map_err(|e| format!("prepare fs_export_overlay_disabled query failed: {}", e))?;
    let rows = stmt
        .query_map([], |row| {
            Ok(FsExportOverlayDisabledRecord {
                agent_instance_key: row.get(0)?,
                export_name: row.get(1)?,
                updated_unix_ms: row.get(2)?,
            })
        })
        .map_err(|e| format!("query fs_export_overlay_disabled failed: {}", e))?;
    let mut out: Vec<FsExportOverlayDisabledRecord> = Vec::new();
    for row in rows {
        out.push(row.map_err(|e| format!("decode fs_export_overlay_disabled row failed: {}", e))?);
    }
    Ok(out)
}

fn upsert_fs_export_overlay_upsert_in_db(
    conn: &Connection,
    agent_instance_key: &str,
    export_name: &str,
    export: &FluxonFsExport,
) -> Result<(), String> {
    let export_json = serde_json::to_string(export).map_err(|e| {
        format!(
            "serialize overlay export export_name={} failed: {}",
            export_name, e
        )
    })?;
    let updated_unix_ms = Utc::now().timestamp_millis();
    conn.execute(
        "INSERT INTO fs_export_overlay_upsert(agent_instance_key, export_name, export_json, updated_unix_ms)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(agent_instance_key, export_name)
         DO UPDATE SET export_json=excluded.export_json, updated_unix_ms=excluded.updated_unix_ms",
        params![agent_instance_key, export_name, export_json, updated_unix_ms],
    )
    .map_err(|e| {
        format!(
            "upsert fs_export_overlay_upsert agent_instance_key={} export_name={} failed: {}",
            agent_instance_key, export_name, e
        )
    })?;
    Ok(())
}

fn delete_fs_export_overlay_upsert_in_db(
    conn: &Connection,
    agent_instance_key: &str,
    export_name: &str,
) -> Result<(), String> {
    conn.execute(
        "DELETE FROM fs_export_overlay_upsert WHERE agent_instance_key = ?1 AND export_name = ?2",
        params![agent_instance_key, export_name],
    )
    .map_err(|e| {
        format!(
            "delete fs_export_overlay_upsert agent_instance_key={} export_name={} failed: {}",
            agent_instance_key, export_name, e
        )
    })?;
    Ok(())
}

fn load_fs_export_overlay_upsert_records_from_db(
    conn: &Connection,
) -> Result<Vec<FsExportOverlayUpsertRecord>, String> {
    let mut stmt = conn
        .prepare(
            "SELECT agent_instance_key, export_name, export_json, updated_unix_ms
             FROM fs_export_overlay_upsert
             ORDER BY agent_instance_key, export_name",
        )
        .map_err(|e| format!("prepare fs_export_overlay_upsert query failed: {}", e))?;
    let rows = stmt
        .query_map([], |row| {
            let export_json: String = row.get(2)?;
            let export = serde_json::from_str::<FluxonFsExport>(&export_json).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("invalid overlay export_json: {}", e),
                    )),
                )
            })?;
            Ok(FsExportOverlayUpsertRecord {
                agent_instance_key: row.get(0)?,
                export_name: row.get(1)?,
                export,
                updated_unix_ms: row.get(3)?,
            })
        })
        .map_err(|e| format!("query fs_export_overlay_upsert failed: {}", e))?;
    let mut out: Vec<FsExportOverlayUpsertRecord> = Vec::new();
    for row in rows {
        out.push(row.map_err(|e| format!("decode fs_export_overlay_upsert row failed: {}", e))?);
    }
    Ok(out)
}

fn load_fs_export_overlay_for_agent_from_db(
    conn: &Connection,
    agent_instance_key: &str,
) -> Result<FsAgentExportOverlayWire, String> {
    let disabled_records = load_fs_export_overlay_disabled_records_from_db(conn)?;
    let upsert_records = load_fs_export_overlay_upsert_records_from_db(conn)?;

    let mut disabled_exports: Vec<String> = disabled_records
        .into_iter()
        .filter(|record| record.agent_instance_key == agent_instance_key)
        .map(|record| record.export_name)
        .collect();
    disabled_exports.sort();
    disabled_exports.dedup();

    let mut upsert_exports: BTreeMap<String, FluxonFsExport> = BTreeMap::new();
    for record in upsert_records {
        if record.agent_instance_key != agent_instance_key {
            continue;
        }
        upsert_exports.insert(record.export_name, record.export);
    }

    Ok(FsAgentExportOverlayWire {
        disabled_exports,
        upsert_exports,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum UiTransferTaskKind {
    Copy,
    Move,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum UiTransferTaskStage {
    Running,
    Paused,
    Done,
    Error,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UiTransferTaskCommand {
    Run,
    Pause,
    Cancel,
}

#[derive(Debug, Clone, serde::Serialize)]
struct UiTransferTaskEndpoint {
    bucket: String,
    key: String,
    prefix: String,
}

#[derive(Debug, Clone, serde::Serialize)]
struct UiTransferTaskSnapshot {
    ok: bool,
    task_id: String,
    kind: UiTransferTaskKind,
    name: String,
    started_at_ms: i64,
    done_bytes: i64,
    total_bytes: i64,
    stage: UiTransferTaskStage,
    summary: String,
    detail: String,
    source: UiTransferTaskEndpoint,
    target: UiTransferTaskEndpoint,
    can_pause: bool,
    can_resume: bool,
    can_cancel: bool,
}

#[derive(Debug, Clone)]
struct UiTransferTaskRecord {
    owner_username: String,
    snapshot: UiTransferTaskSnapshot,
    command: UiTransferTaskCommand,
}

#[derive(Clone)]
struct UiTransferTaskHandle {
    inner: Arc<Mutex<UiTransferTaskRecord>>,
    notify: Arc<tokio::sync::Notify>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UiTransferTaskGate {
    Run,
    Wait,
    Cancel,
}

impl UiTransferTaskHandle {
    fn new(owner_username: String, snapshot: UiTransferTaskSnapshot) -> Self {
        Self {
            inner: Arc::new(Mutex::new(UiTransferTaskRecord {
                owner_username,
                snapshot,
                command: UiTransferTaskCommand::Run,
            })),
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn owner_username(&self) -> String {
        self.inner.lock().owner_username.clone()
    }

    fn snapshot(&self) -> UiTransferTaskSnapshot {
        let guard = self.inner.lock();
        ui_transfer_task_snapshot_with_controls(&guard.snapshot, guard.command)
    }

    fn set_running(&self, done_bytes: i64, summary: impl Into<String>, detail: impl Into<String>) {
        let mut guard = self.inner.lock();
        guard.snapshot.done_bytes = done_bytes.max(0);
        guard.snapshot.stage = UiTransferTaskStage::Running;
        guard.snapshot.summary = summary.into();
        guard.snapshot.detail = detail.into();
    }

    fn set_done(&self, done_bytes: i64, summary: impl Into<String>, detail: impl Into<String>) {
        let mut guard = self.inner.lock();
        guard.snapshot.done_bytes = done_bytes.max(0);
        guard.snapshot.stage = UiTransferTaskStage::Done;
        guard.snapshot.summary = summary.into();
        guard.snapshot.detail = detail.into();
    }

    fn set_error(&self, done_bytes: i64, summary: impl Into<String>, detail: impl Into<String>) {
        let mut guard = self.inner.lock();
        guard.snapshot.done_bytes = done_bytes.max(0);
        guard.snapshot.stage = UiTransferTaskStage::Error;
        guard.snapshot.summary = summary.into();
        guard.snapshot.detail = detail.into();
    }

    fn set_cancelled(
        &self,
        done_bytes: i64,
        summary: impl Into<String>,
        detail: impl Into<String>,
    ) {
        let mut guard = self.inner.lock();
        guard.snapshot.done_bytes = done_bytes.max(0);
        guard.snapshot.stage = UiTransferTaskStage::Cancelled;
        guard.snapshot.summary = summary.into();
        guard.snapshot.detail = detail.into();
    }

    fn request_pause(&self) -> UiTransferTaskSnapshot {
        let mut guard = self.inner.lock();
        if matches!(
            guard.snapshot.stage,
            UiTransferTaskStage::Done | UiTransferTaskStage::Error | UiTransferTaskStage::Cancelled
        ) {
            return ui_transfer_task_snapshot_with_controls(&guard.snapshot, guard.command);
        }
        guard.command = UiTransferTaskCommand::Pause;
        guard.snapshot.summary = format!(
            "Pause requested for {}",
            ui_transfer_kind_title(guard.snapshot.kind).to_lowercase()
        );
        ui_transfer_task_snapshot_with_controls(&guard.snapshot, guard.command)
    }

    fn request_resume(&self) -> UiTransferTaskSnapshot {
        let mut guard = self.inner.lock();
        if matches!(
            guard.snapshot.stage,
            UiTransferTaskStage::Done | UiTransferTaskStage::Error | UiTransferTaskStage::Cancelled
        ) {
            return ui_transfer_task_snapshot_with_controls(&guard.snapshot, guard.command);
        }
        guard.command = UiTransferTaskCommand::Run;
        if guard.snapshot.stage == UiTransferTaskStage::Paused {
            guard.snapshot.stage = UiTransferTaskStage::Running;
        }
        guard.snapshot.summary = format!(
            "Resuming {}",
            ui_transfer_kind_title(guard.snapshot.kind).to_lowercase()
        );
        let snapshot = ui_transfer_task_snapshot_with_controls(&guard.snapshot, guard.command);
        drop(guard);
        self.notify.notify_waiters();
        snapshot
    }

    fn request_cancel(&self) -> UiTransferTaskSnapshot {
        let mut guard = self.inner.lock();
        if matches!(
            guard.snapshot.stage,
            UiTransferTaskStage::Done | UiTransferTaskStage::Error | UiTransferTaskStage::Cancelled
        ) {
            return ui_transfer_task_snapshot_with_controls(&guard.snapshot, guard.command);
        }
        guard.command = UiTransferTaskCommand::Cancel;
        guard.snapshot.summary = format!(
            "Cancelling {}",
            ui_transfer_kind_title(guard.snapshot.kind).to_lowercase()
        );
        let snapshot = ui_transfer_task_snapshot_with_controls(&guard.snapshot, guard.command);
        drop(guard);
        self.notify.notify_waiters();
        snapshot
    }

    fn gate(&self, done_bytes: i64, total_bytes: i64) -> UiTransferTaskGate {
        let mut guard = self.inner.lock();
        match guard.command {
            UiTransferTaskCommand::Run => UiTransferTaskGate::Run,
            UiTransferTaskCommand::Pause => {
                guard.snapshot.done_bytes = done_bytes.max(0);
                guard.snapshot.stage = UiTransferTaskStage::Paused;
                guard.snapshot.summary =
                    format!("{} paused", ui_transfer_kind_title(guard.snapshot.kind));
                guard.snapshot.detail = ui_transfer_task_detail(done_bytes, total_bytes);
                UiTransferTaskGate::Wait
            }
            UiTransferTaskCommand::Cancel => UiTransferTaskGate::Cancel,
        }
    }

    fn wait_token(&self) -> impl std::future::Future<Output = ()> + '_ {
        self.notify.notified()
    }
}

fn ui_transfer_task_snapshot_with_controls(
    snapshot: &UiTransferTaskSnapshot,
    command: UiTransferTaskCommand,
) -> UiTransferTaskSnapshot {
    let mut out = snapshot.clone();
    let (can_pause, can_resume, can_cancel) = match out.stage {
        UiTransferTaskStage::Done | UiTransferTaskStage::Error | UiTransferTaskStage::Cancelled => {
            (false, false, false)
        }
        UiTransferTaskStage::Paused => match command {
            UiTransferTaskCommand::Cancel => (false, false, false),
            UiTransferTaskCommand::Run | UiTransferTaskCommand::Pause => (false, true, true),
        },
        UiTransferTaskStage::Running => match command {
            UiTransferTaskCommand::Run => (true, false, true),
            UiTransferTaskCommand::Pause => (false, true, true),
            UiTransferTaskCommand::Cancel => (false, false, false),
        },
    };
    out.can_pause = can_pause;
    out.can_resume = can_resume;
    out.can_cancel = can_cancel;
    out
}

fn normalize_external_base_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() || trimmed == "/" {
        return String::new();
    }
    let no_trailing = trimmed.trim_end_matches('/');
    if no_trailing.starts_with('/') {
        return no_trailing.to_string();
    }
    format!("/{}", no_trailing)
}

#[derive(Debug, Clone)]
struct AuthAccount {
    username: String,
    password: String,
    can_manage_users: bool,
    permissions: Vec<fluxon_fs_core::config::FluxonFsS3PermissionRule>,
}

impl AuthAccount {
    fn from_cfg(v: &FluxonFsS3PermissionAccount) -> Self {
        let mut can_manage_users = false;
        let permissions = v
            .permissions
            .iter()
            .filter_map(|rule| {
                if permission_rule_is_manage(rule) {
                    can_manage_users = true;
                    return None;
                }
                Some(rule.clone())
            })
            .collect();
        Self {
            username: v.username.clone(),
            password: v.password.clone(),
            can_manage_users,
            permissions,
        }
    }
}

fn request_identity_from_account(account: &AuthAccount) -> FluxonFsRequestIdentity {
    FluxonFsRequestIdentity {
        username: account.username.clone(),
        password: account.password.clone(),
    }
}

type ScopeAccessMode = FluxonFsScopeAccessMode;
type AccessUser = FluxonFsAccessUser;
type ScopeAccess = FluxonFsScopeAccess;
type AccessModel = FluxonFsAccessModel;

fn permission_bucket_matches(rule_bucket: &str, bucket: &str) -> bool {
    rule_bucket == "*" || rule_bucket == bucket
}

fn permission_action_matches(
    allowed: FluxonFsS3PermissionAction,
    required: FluxonFsS3PermissionAction,
) -> bool {
    allowed == FluxonFsS3PermissionAction::All || allowed == required
}

fn permission_rule_has_action(
    rule: &fluxon_fs_core::config::FluxonFsS3PermissionRule,
    action: FluxonFsS3PermissionAction,
) -> bool {
    rule.actions.iter().any(|v| *v == action)
}

fn permission_rule_is_manage(rule: &fluxon_fs_core::config::FluxonFsS3PermissionRule) -> bool {
    rule.bucket == "*"
        && rule.prefix.is_empty()
        && permission_rule_has_action(rule, FluxonFsS3PermissionAction::All)
}

fn scope_access_manage_rule() -> fluxon_fs_core::config::FluxonFsS3PermissionRule {
    fluxon_fs_core::config::FluxonFsS3PermissionRule {
        bucket: "*".to_string(),
        prefix: "".to_string(),
        actions: vec![FluxonFsS3PermissionAction::All],
    }
}

fn permission_rule_grants_read(rule: &fluxon_fs_core::config::FluxonFsS3PermissionRule) -> bool {
    permission_rule_has_action(rule, FluxonFsS3PermissionAction::All)
        || (permission_rule_has_action(rule, FluxonFsS3PermissionAction::ListBucket)
            && permission_rule_has_action(rule, FluxonFsS3PermissionAction::GetObject))
}

fn access_model_from_permission_list(
    permission_list: &[FluxonFsS3PermissionAccount],
) -> Result<AccessModel, String> {
    access_model_from_s3_permission_list(permission_list)
}

fn permission_list_from_access_model(
    model: &AccessModel,
) -> Result<Vec<FluxonFsS3PermissionAccount>, String> {
    s3_permission_list_from_access_model(model)
}

fn account_can_manage_permissions(account: &AuthAccount) -> bool {
    account.can_manage_users
}

fn account_has_bucket_access(account: &AuthAccount, bucket: &str) -> bool {
    if account.can_manage_users {
        return true;
    }
    account
        .permissions
        .iter()
        .any(|rule| permission_bucket_matches(&rule.bucket, bucket))
}

fn account_has_ui_bucket_browse_access(account: &AuthAccount, bucket: &str) -> bool {
    if account.can_manage_users {
        return true;
    }
    account.permissions.iter().any(|rule| {
        permission_bucket_matches(&rule.bucket, bucket) && permission_rule_grants_read(rule)
    })
}

fn account_can_browse_ui_prefix(account: &AuthAccount, bucket: &str, prefix: &str) -> bool {
    if account.can_manage_users {
        return true;
    }
    account.permissions.iter().any(|rule| {
        permission_bucket_matches(&rule.bucket, bucket)
            && permission_rule_grants_read(rule)
            && (prefix.starts_with(rule.prefix.as_str()) || rule.prefix.starts_with(prefix))
    })
}

fn account_can_browse_ui_file(account: &AuthAccount, bucket: &str, key: &str) -> bool {
    if account.can_manage_users {
        return true;
    }
    account.permissions.iter().any(|rule| {
        permission_bucket_matches(&rule.bucket, bucket)
            && permission_rule_grants_read(rule)
            && key.starts_with(rule.prefix.as_str())
    })
}

fn account_has_bucket_action(
    account: &AuthAccount,
    bucket: &str,
    prefix: &str,
    action: FluxonFsS3PermissionAction,
) -> bool {
    if account.can_manage_users {
        return true;
    }
    account.permissions.iter().any(|rule| {
        permission_bucket_matches(&rule.bucket, bucket)
            && prefix.starts_with(rule.prefix.as_str())
            && rule
                .actions
                .iter()
                .any(|allowed| permission_action_matches(*allowed, action))
    })
}

fn account_has_bucket_browse_action(
    account: &AuthAccount,
    bucket: &str,
    prefix: &str,
    action: FluxonFsS3PermissionAction,
) -> bool {
    if account.can_manage_users {
        return true;
    }
    account.permissions.iter().any(|rule| {
        permission_bucket_matches(&rule.bucket, bucket)
            && (prefix.starts_with(rule.prefix.as_str()) || rule.prefix.starts_with(prefix))
            && rule
                .actions
                .iter()
                .any(|allowed| permission_action_matches(*allowed, action))
    })
}

fn account_has_object_action(
    account: &AuthAccount,
    bucket: &str,
    key: &str,
    action: FluxonFsS3PermissionAction,
) -> bool {
    if account.can_manage_users {
        return true;
    }
    account.permissions.iter().any(|rule| {
        permission_bucket_matches(&rule.bucket, bucket)
            && key.starts_with(rule.prefix.as_str())
            && rule
                .actions
                .iter()
                .any(|allowed| permission_action_matches(*allowed, action))
    })
}

fn forbidden_access_denied(detail: impl Into<String>) -> S3Error {
    S3Error::AccessDenied {
        detail: detail.into(),
    }
}

fn ui_forbidden_response(detail: impl Into<String>) -> Response {
    text_response(StatusCode::FORBIDDEN, detail.into())
}

fn find_account_by_username(st: &GatewayState, username: &str) -> Option<AuthAccount> {
    st.permission_list
        .read()
        .iter()
        .find(|v| v.username == username)
        .map(AuthAccount::from_cfg)
}

fn list_permitted_buckets(st: &GatewayState, account: &AuthAccount) -> Result<Vec<String>, String> {
    Ok(st
        .load_effective_fs_exports()?
        .keys()
        .filter(|bucket| account_has_ui_bucket_browse_access(account, bucket))
        .cloned()
        .collect())
}

fn persist_permission_list(
    st: &GatewayState,
    permission_list: &[FluxonFsS3PermissionAccount],
) -> Result<(), String> {
    st.persist_permission_list_state(permission_list)
}

#[derive(thiserror::Error, Debug)]
pub enum S3Error {
    #[error("access denied: {detail}")]
    AccessDenied { detail: String },
    #[error("invalid request: {detail}")]
    InvalidRequest { detail: String },
    #[error("invalid range: {detail}")]
    InvalidRange { detail: String },
    #[error("no such bucket: {bucket}")]
    NoSuchBucket { bucket: String },
    #[error("no such key: {bucket}/{key}")]
    NoSuchKey { bucket: String, key: String },
    #[error("no such upload: {upload_id}")]
    NoSuchUpload { upload_id: String },
    #[error("internal error: {detail}")]
    Internal { detail: String },
}

pub fn validate_exports_bucket_names(cfg: &FluxonFsGlobalConfig) -> anyhow::Result<()> {
    for name in cfg.exports.keys() {
        if !is_valid_bucket_name(name) {
            anyhow::bail!(
                "invalid export name for S3 bucket (bucket==export): export={:?} (expected 3-63 chars, [a-z0-9-], no leading/trailing '-')",
                name
            );
        }
    }
    Ok(())
}

fn is_valid_bucket_name(s: &str) -> bool {
    let s = s.trim();
    if s.len() < 3 || s.len() > 63 {
        return false;
    }
    let bytes = s.as_bytes();
    if bytes.first().copied().unwrap() == b'-' || bytes.last().copied().unwrap() == b'-' {
        return false;
    }
    for &b in bytes {
        let ok = (b'a'..=b'z').contains(&b) || (b'0'..=b'9').contains(&b) || b == b'-';
        if !ok {
            return false;
        }
    }
    true
}

pub fn build_router(st: Arc<GatewayState>) -> Router {
    Router::new()
        // SSR UI (browser-oriented).
        .route("/", any(handle_any_root))
        .route("/ui", get(ui_redirect_to_ui_slash))
        .route("/ui/", get(ui_index))
        .route("/ui/api/transfers", get(ui_transfer_task_list))
        .route("/ui/api/transfer_prescans", get(ui_transfer_prescan_list))
        .route(
            "/ui/api/transfer_prescans/:job_id/import",
            post(ui_transfer_prescan_import),
        )
        .route(
            "/ui/api/transfer_jobs",
            get(ui_transfer_job_list).post(ui_transfer_job_create),
        )
        .route("/ui/api/transfer_job/:job_id", get(ui_transfer_job_detail))
        .route(
            "/ui/api/transfer_job/:job_id/history",
            get(ui_transfer_job_history),
        )
        .route(
            "/ui/api/transfer_job/:job_id/failure/:failure_index",
            get(ui_transfer_job_failure_detail),
        )
        .route(
            "/ui/api/transfer_job/:job_id/file_issue",
            get(ui_transfer_job_file_issue_detail),
        )
        .route(
            "/ui/api/transfer_job/:job_id/workers",
            post(ui_transfer_job_update_workers),
        )
        .route(
            "/ui/api/transfer_job/:job_id/cancel",
            post(ui_transfer_job_cancel),
        )
        .route("/ui/api/transfer/:task_id", get(ui_transfer_task_status))
        .route(
            "/ui/api/transfer/:task_id/:action",
            post(ui_transfer_task_control),
        )
        .route(
            "/ui/account/password",
            get(ui_account_password_redirect_to_slash),
        )
        .route(
            "/ui/account/password/",
            get(ui_account_password_page).post(ui_account_password_save),
        )
        .route("/ui/transfers", get(ui_transfers_redirect_to_slash))
        .route("/ui/transfers/", get(ui_transfers_page))
        .route("/ui/admin", get(ui_admin_redirect_to_slash))
        .route("/ui/admin/", get(ui_admin_home_page))
        .route("/ui/admin/users", get(ui_admin_users_redirect_to_slash))
        .route("/ui/admin/users/", get(ui_admin_users_page))
        .route("/ui/admin/users/create", post(ui_admin_users_create))
        .route("/ui/admin/users/access", post(ui_admin_users_update_access))
        .route(
            "/ui/admin/users/reset_password",
            post(ui_admin_users_reset_password),
        )
        .route("/ui/admin/users/delete", post(ui_admin_users_delete))
        .route(
            "/ui/admin/permissions",
            get(ui_admin_permissions_redirect_to_slash),
        )
        .route("/ui/admin/permissions/", get(ui_admin_permissions_page))
        .route("/ui/admin/permissions/create", post(ui_admin_scope_create))
        .route(
            "/ui/admin/permissions/update_users",
            post(ui_admin_scope_update_users),
        )
        .route("/ui/admin/permissions/delete", post(ui_admin_scope_delete))
        .route(
            "/ui/admin/fs_master",
            get(ui_admin_fs_master_redirect_to_slash),
        )
        .route("/ui/admin/fs_master/", get(ui_admin_fs_master_page))
        .route(
            "/ui/admin/fs_master/browse",
            get(ui_admin_fs_master_agent_browse),
        )
        .route(
            "/ui/admin/fs_master/exports/add",
            post(ui_admin_fs_master_export_add),
        )
        .route(
            "/ui/admin/fs_master/exports/remove",
            post(ui_admin_fs_master_export_remove),
        )
        .route("/ui/:bucket", get(ui_bucket_redirect_to_slash))
        .route("/ui/:bucket/", get(ui_bucket_browse))
        .route("/ui/:bucket/api/ls", get(ui_bucket_api_ls))
        .route(
            "/ui/:bucket/api/multipart/create",
            post(ui_bucket_api_multipart_create),
        )
        .route(
            "/ui/:bucket/api/multipart/:upload_id/part/:part_number",
            put(ui_bucket_api_multipart_part),
        )
        .route(
            "/ui/:bucket/api/multipart/:upload_id/complete",
            post(ui_bucket_api_multipart_complete),
        )
        .route(
            "/ui/:bucket/api/multipart/:upload_id",
            delete(ui_bucket_api_multipart_abort),
        )
        .route("/ui/:bucket/api/upload", post(ui_bucket_api_upload))
        .route("/ui/:bucket/api/mkdir", post(ui_bucket_api_mkdir))
        .route("/ui/:bucket/api/delete", post(ui_bucket_api_delete))
        .route(
            "/ui/:bucket/api/delete_folder",
            post(ui_bucket_api_delete_folder),
        )
        .route("/ui/:bucket/api/copy", post(ui_bucket_api_copy))
        .route("/ui/:bucket/api/move", post(ui_bucket_api_move))
        .route("/ui/:bucket/upload", post(ui_bucket_upload))
        .route("/ui/:bucket/mkdir", post(ui_bucket_mkdir))
        .route("/ui/:bucket/delete", post(ui_bucket_delete))
        .route("/ui/:bucket/obj/*key", get(ui_bucket_get_object))
        // S3 API (SigV4).
        .route("/*path", any(handle_any))
        .fallback(any(handle_any_root_or_not_found))
        .with_state::<()>(st)
}

async fn handle_any_root(State(st): State<Arc<GatewayState>>, req: Request<Body>) -> Response {
    let method = req.method().clone();
    let headers = req.headers().clone();
    let uri = req.uri().clone();
    let body = req.into_body();
    match handle_any_impl(st, method, String::new(), uri.clone(), headers, body).await {
        Ok(resp) => resp,
        Err(e) => s3_error_response(&uri, e),
    }
}

pub async fn handle_direct_root_request(st: Arc<GatewayState>, req: Request<Body>) -> Response {
    tracing::info!(
        path = %req.uri().path(),
        query = ?req.uri().query(),
        "fs_s3 direct root handler invoked"
    );
    handle_any_root(State(st), req).await
}

pub async fn handle_external_request(st: Arc<GatewayState>, mut req: Request<Body>) -> Response {
    let service_path = match external_request_service_path(&st.external_base_path, req.uri().path())
    {
        Some(v) => v,
        None => return text_response(StatusCode::NOT_FOUND, "not found".to_string()),
    };
    let path_and_query = match req.uri().query() {
        Some(q) => format!("{}?{}", service_path, q),
        None => service_path,
    };
    let new_uri = match path_and_query.parse::<Uri>() {
        Ok(v) => v,
        Err(e) => {
            return text_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("rewrite external request uri failed: {}", e),
            );
        }
    };
    *req.uri_mut() = new_uri;
    // English note:
    // - The outer master_http route uses `/fs_s3/*path`, so Axum has already inserted one set of
    //   URL params into request extensions before we delegate into the gateway router.
    // - The gateway router also matches `/*path`. If the outer params are kept, Axum appends the
    //   inner wildcard param instead of replacing it, and `Path<String>` in `handle_any` sees two
    //   path arguments for a one-parameter extractor.
    // - Clear request extensions before re-routing so the delegated router starts from a clean
    //   match context and bucket paths like `/fs_s3/fluxon-release` stay on the S3 API path.
    req.extensions_mut().clear();
    build_router(st).oneshot(req).await.unwrap()
}

fn external_request_service_path(base: &str, req_path: &str) -> Option<String> {
    let normalized_req_path = if req_path.is_empty() { "/" } else { req_path };
    if base.is_empty() {
        return Some(normalized_req_path.to_string());
    }
    if normalized_req_path == base || normalized_req_path == format!("{}/", base) {
        return Some("/".to_string());
    }
    let with_slash = format!("{}/", base);
    let rest = normalized_req_path.strip_prefix(&with_slash)?;
    if rest.is_empty() {
        Some("/".to_string())
    } else {
        Some(format!("/{}", rest))
    }
}

async fn handle_any_root_or_not_found(
    State(st): State<Arc<GatewayState>>,
    req: Request<Body>,
) -> Response {
    let path = req.uri().path().to_string();
    let base = st.external_base_path.as_str();
    if path.is_empty()
        || path == "/"
        || (!base.is_empty() && (path == base || path == format!("{}/", base)))
    {
        tracing::info!(path = %path, base = %base, "fs_s3 root fallback matched");
        return handle_any_root(State(st), req).await;
    }
    tracing::info!(path = %path, base = %base, "fs_s3 root fallback not matched");
    text_response(StatusCode::NOT_FOUND, "not found".to_string())
}

async fn handle_any(
    State(st): State<Arc<GatewayState>>,
    Path(path): Path<String>,
    req: Request<Body>,
) -> Response {
    let method = req.method().clone();
    let headers = req.headers().clone();
    let uri = req.uri().clone();
    let body = req.into_body();
    match handle_any_impl(st, method, path, uri.clone(), headers, body).await {
        Ok(resp) => resp,
        Err(e) => s3_error_response(&uri, e),
    }
}

async fn handle_any_impl(
    st: Arc<GatewayState>,
    method: Method,
    path: String,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, S3Error> {
    // Authenticate first (SigV4).
    let auth_ctx = auth_verify_sigv4(&st, &headers)?;
    let (orig_path, orig_query) = auth_original_path_and_query(
        &st.external_base_path,
        &st.cluster_name,
        &uri,
        &headers,
        &path,
    )?;
    verify_sigv4_authorization_header(
        &st.external_base_path,
        &auth_ctx,
        &headers,
        &method,
        &orig_path,
        orig_query.as_deref(),
    )?;

    handle_any_authed_impl(st, auth_ctx, method, path, uri, headers, body).await
}

async fn handle_any_authed_impl(
    st: Arc<GatewayState>,
    auth_ctx: AuthCtx,
    method: Method,
    path: String,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, S3Error> {
    let params = parse_query_params(uri.query().unwrap_or(""));
    let request_identity = auth_ctx.request_identity();

    let (bucket, key_opt) = split_bucket_and_key(&path);
    if bucket.is_empty() {
        if method == Method::GET {
            let buckets = list_permitted_buckets(&st, &auth_ctx.account).map_err(|detail| {
                S3Error::Internal {
                    detail: format!("load permitted buckets failed: {}", detail),
                }
            })?;
            return Ok(resp_xml(
                StatusCode::OK,
                list_buckets_xml(&auth_ctx.username, &buckets),
            ));
        }
        return Err(S3Error::InvalidRequest {
            detail: format!("unsupported method on service root: {}", method),
        });
    }

    if st
        .ensure_effective_fs_export(bucket)
        .map_err(|detail| S3Error::Internal {
            detail: format!("load effective export failed: {}", detail),
        })?
        .is_none()
    {
        return Err(S3Error::NoSuchBucket {
            bucket: bucket.to_string(),
        });
    }

    match (method.clone(), key_opt) {
        (Method::HEAD, None) => {
            if !account_has_bucket_browse_action(
                &auth_ctx.account,
                bucket,
                "",
                FluxonFsS3PermissionAction::ListBucket,
            ) {
                return Err(forbidden_access_denied(format!(
                    "account {} lacks s3:ListBucket on bucket {}",
                    auth_ctx.username, bucket
                )));
            }
            Ok(resp_empty(StatusCode::OK))
        }
        (Method::GET, None) => {
            if params.contains_key("uploads") {
                let prefix = params.get("prefix").map(|s| s.as_str()).unwrap_or("");
                if !account_has_bucket_action(
                    &auth_ctx.account,
                    bucket,
                    prefix,
                    FluxonFsS3PermissionAction::ListBucketMultipartUploads,
                ) {
                    return Err(forbidden_access_denied(format!(
                        "account {} lacks s3:ListBucketMultipartUploads on bucket {} prefix {}",
                        auth_ctx.username, bucket, prefix
                    )));
                }
                return Ok(resp_xml(
                    StatusCode::OK,
                    list_multipart_uploads_xml(
                        &st,
                        &auth_ctx.account,
                        request_identity.clone(),
                        Arc::from(bucket),
                        &params,
                    )
                    .await?,
                ));
            }

            // Causal chain:
            // - AWS SDKs commonly send list-type=2.
            // - Some clients omit list-type and use legacy GET /bucket; treat it as v2 to reduce client-side branching.
            if let Some(v) = params.get("list-type") {
                if v != "2" {
                    return Err(S3Error::InvalidRequest {
                        detail: format!("unsupported list-type (expected 2): {}", v),
                    });
                }
            }
            let prefix = params.get("prefix").map(|s| s.as_str()).unwrap_or("");
            if !account_has_bucket_browse_action(
                &auth_ctx.account,
                bucket,
                prefix,
                FluxonFsS3PermissionAction::ListBucket,
            ) {
                return Err(forbidden_access_denied(format!(
                    "account {} lacks s3:ListBucket on bucket {} prefix {}",
                    auth_ctx.username, bucket, prefix
                )));
            }
            return Ok(resp_xml(
                StatusCode::OK,
                list_objects_v2_xml(&st, request_identity.clone(), Arc::from(bucket), &params)
                    .await?,
            ));
        }
        (Method::PUT, None) => {
            // Causal chain:
            // - `rclone` issues `PUT /bucket` before object upload as an idempotent bucket-ensure step.
            // - Fluxon exports are statically configured buckets, so we must not try to create anything here.
            // - Treat bucket-root PUT as a no-op success for existing exports so S3 clients can proceed to PUT Object.
            if !account_has_bucket_access(&auth_ctx.account, bucket) {
                return Err(forbidden_access_denied(format!(
                    "account {} lacks access to bucket {}",
                    auth_ctx.username, bucket
                )));
            }
            return Ok(resp_empty(StatusCode::OK));
        }
        (Method::POST, Some(key)) if params.contains_key("uploads") => {
            let rel = safe_relpath(key).map_err(|e| S3Error::InvalidRequest {
                detail: format!("invalid object key: {}", e),
            })?;
            verify_user_object_key(&rel)?;
            if !account_has_object_action(
                &auth_ctx.account,
                bucket,
                &rel,
                FluxonFsS3PermissionAction::PutObject,
            ) {
                return Err(forbidden_access_denied(format!(
                    "account {} lacks s3:PutObject on s3://{}/{}",
                    auth_ctx.username, bucket, rel
                )));
            }
            return Ok(
                multipart_create(&st, request_identity.clone(), Arc::from(bucket), &rel).await?,
            );
        }
        (Method::GET, Some(key)) if params.contains_key("uploadId") => {
            let upload_id = params
                .get("uploadId")
                .ok_or_else(|| S3Error::InvalidRequest {
                    detail: "missing uploadId".to_string(),
                })?
                .to_string();
            let rel = safe_relpath(key).map_err(|e| S3Error::InvalidRequest {
                detail: format!("invalid object key: {}", e),
            })?;
            verify_user_object_key(&rel)?;
            if !account_has_object_action(
                &auth_ctx.account,
                bucket,
                &rel,
                FluxonFsS3PermissionAction::ListMultipartUploadParts,
            ) {
                return Err(forbidden_access_denied(format!(
                    "account {} lacks s3:ListMultipartUploadParts on s3://{}/{}",
                    auth_ctx.username, bucket, rel
                )));
            }
            return Ok(resp_xml(
                StatusCode::OK,
                list_parts_xml(
                    &st,
                    request_identity.clone(),
                    Arc::from(bucket),
                    &rel,
                    &upload_id,
                    &params,
                )
                .await?,
            ));
        }
        (Method::PUT, Some(key))
            if params.contains_key("uploadId") && params.contains_key("partNumber") =>
        {
            let upload_id = params
                .get("uploadId")
                .ok_or_else(|| S3Error::InvalidRequest {
                    detail: "missing uploadId".to_string(),
                })?
                .to_string();
            let pn = params
                .get("partNumber")
                .ok_or_else(|| S3Error::InvalidRequest {
                    detail: "missing partNumber".to_string(),
                })?
                .to_string();
            let part_number: i64 = pn.parse().map_err(|_| S3Error::InvalidRequest {
                detail: format!("invalid partNumber: {}", pn),
            })?;
            let rel = safe_relpath(key).map_err(|e| S3Error::InvalidRequest {
                detail: format!("invalid object key: {}", e),
            })?;
            verify_user_object_key(&rel)?;
            if !account_has_object_action(
                &auth_ctx.account,
                bucket,
                &rel,
                FluxonFsS3PermissionAction::PutObject,
            ) {
                return Err(forbidden_access_denied(format!(
                    "account {} lacks s3:PutObject on s3://{}/{}",
                    auth_ctx.username, bucket, rel
                )));
            }
            return Ok(multipart_upload_part(
                &st,
                request_identity.clone(),
                Arc::from(bucket),
                &rel,
                &upload_id,
                part_number,
                body,
            )
            .await?);
        }
        (Method::POST, Some(key)) if params.contains_key("uploadId") => {
            let upload_id = params
                .get("uploadId")
                .ok_or_else(|| S3Error::InvalidRequest {
                    detail: "missing uploadId".to_string(),
                })?
                .to_string();
            let rel = safe_relpath(key).map_err(|e| S3Error::InvalidRequest {
                detail: format!("invalid object key: {}", e),
            })?;
            verify_user_object_key(&rel)?;
            if !account_has_object_action(
                &auth_ctx.account,
                bucket,
                &rel,
                FluxonFsS3PermissionAction::PutObject,
            ) {
                return Err(forbidden_access_denied(format!(
                    "account {} lacks s3:PutObject on s3://{}/{}",
                    auth_ctx.username, bucket, rel
                )));
            }
            return Ok(multipart_complete(
                &st,
                request_identity.clone(),
                Arc::from(bucket),
                &rel,
                &upload_id,
                body,
            )
            .await?);
        }
        (Method::DELETE, Some(key)) if params.contains_key("uploadId") => {
            let upload_id = params
                .get("uploadId")
                .ok_or_else(|| S3Error::InvalidRequest {
                    detail: "missing uploadId".to_string(),
                })?
                .to_string();
            let rel = safe_relpath(key).map_err(|e| S3Error::InvalidRequest {
                detail: format!("invalid object key: {}", e),
            })?;
            verify_user_object_key(&rel)?;
            if !account_has_object_action(
                &auth_ctx.account,
                bucket,
                &rel,
                FluxonFsS3PermissionAction::AbortMultipartUpload,
            ) {
                return Err(forbidden_access_denied(format!(
                    "account {} lacks s3:AbortMultipartUpload on s3://{}/{}",
                    auth_ctx.username, bucket, rel
                )));
            }
            return Ok(multipart_abort(
                &st,
                request_identity.clone(),
                Arc::from(bucket),
                &rel,
                &upload_id,
            )
            .await?);
        }
        (Method::GET, Some(key)) | (Method::HEAD, Some(key)) => {
            let rel = safe_relpath(key).map_err(|e| S3Error::InvalidRequest {
                detail: format!("invalid object key: {}", e),
            })?;
            verify_user_object_key(&rel)?;
            if !account_has_object_action(
                &auth_ctx.account,
                bucket,
                &rel,
                FluxonFsS3PermissionAction::GetObject,
            ) {
                return Err(forbidden_access_denied(format!(
                    "account {} lacks s3:GetObject on s3://{}/{}",
                    auth_ctx.username, bucket, rel
                )));
            }
            let bucket_arc: Arc<str> = Arc::from(bucket);
            let rel_arc: Arc<str> = Arc::from(rel.as_str());
            let stat = st
                .backend
                .stat(
                    request_identity.clone(),
                    bucket_arc.clone(),
                    rel_arc.clone(),
                )
                .await?;
            if !stat.exists || !stat.is_file {
                return Err(S3Error::NoSuchKey {
                    bucket: bucket.to_string(),
                    key: rel.to_string(),
                });
            }

            if method == Method::HEAD {
                let (range_start, range_end_inclusive) = parse_range_header(&headers, stat.size)?;
                let mut resp = resp_empty(if range_start.is_some() {
                    StatusCode::PARTIAL_CONTENT
                } else {
                    StatusCode::OK
                });
                apply_object_headers(
                    resp.headers_mut(),
                    stat.size,
                    stat.mtime_ns,
                    range_start,
                    range_end_inclusive,
                );
                return Ok(resp);
            }

            let (range_start, range_end_inclusive) = parse_range_header(&headers, stat.size)?;
            let body2 = get_object_stream(
                st.clone(),
                request_identity.clone(),
                bucket_arc,
                rel_arc,
                stat.size,
                stat.mtime_ns,
                range_start,
                range_end_inclusive,
            )
            .await?;
            let status = if range_start.is_some() {
                StatusCode::PARTIAL_CONTENT
            } else {
                StatusCode::OK
            };
            let mut resp: Response = Response::new(boxed(body2));
            *resp.status_mut() = status;
            apply_object_headers(
                resp.headers_mut(),
                stat.size,
                stat.mtime_ns,
                range_start,
                range_end_inclusive,
            );
            return Ok(resp);
        }
        (Method::PUT, Some(key)) => {
            let rel = safe_relpath(key).map_err(|e| S3Error::InvalidRequest {
                detail: format!("invalid object key: {}", e),
            })?;
            verify_user_object_key(&rel)?;
            if !account_has_object_action(
                &auth_ctx.account,
                bucket,
                &rel,
                FluxonFsS3PermissionAction::PutObject,
            ) {
                return Err(forbidden_access_denied(format!(
                    "account {} lacks s3:PutObject on s3://{}/{}",
                    auth_ctx.username, bucket, rel
                )));
            }
            let bucket_arc: Arc<str> = Arc::from(bucket);
            let rel_arc: Arc<str> = Arc::from(rel.as_str());
            ensure_parent_dirs(&st, request_identity.clone(), bucket_arc.clone(), &rel).await?;
            let (etag, size) = put_object_stream_with_sha256_etag(
                &st,
                request_identity.clone(),
                bucket_arc.clone(),
                rel_arc.clone(),
                body,
            )
            .await?;
            st.backend
                .truncate(request_identity.clone(), bucket_arc, rel_arc, size)
                .await?;
            Ok(resp_empty_with_etag(StatusCode::OK, &etag))
        }
        (Method::DELETE, Some(key)) => {
            let rel = safe_relpath(key).map_err(|e| S3Error::InvalidRequest {
                detail: format!("invalid object key: {}", e),
            })?;
            verify_user_object_key(&rel)?;
            if !account_has_object_action(
                &auth_ctx.account,
                bucket,
                &rel,
                FluxonFsS3PermissionAction::DeleteObject,
            ) {
                return Err(forbidden_access_denied(format!(
                    "account {} lacks s3:DeleteObject on s3://{}/{}",
                    auth_ctx.username, bucket, rel
                )));
            }
            let bucket_arc: Arc<str> = Arc::from(bucket);
            let rel_arc: Arc<str> = Arc::from(rel.as_str());
            let stat = st
                .backend
                .stat(
                    request_identity.clone(),
                    bucket_arc.clone(),
                    rel_arc.clone(),
                )
                .await?;
            if stat.exists {
                st.backend
                    .unlink(request_identity.clone(), bucket_arc, rel_arc)
                    .await?;
            }
            Ok(resp_empty(StatusCode::NO_CONTENT))
        }
        _ => Err(S3Error::InvalidRequest {
            detail: format!("unsupported method/path: method={} path={}", method, path),
        }),
    }
}

fn split_bucket_and_key(path: &str) -> (&str, Option<&str>) {
    let p = path.trim_start_matches('/');
    if p.is_empty() {
        return ("", None);
    }
    let Some((bucket, rest)) = p.split_once('/') else {
        return (p, None);
    };
    let rest = rest.trim_start_matches('/');
    if rest.is_empty() {
        (bucket, None)
    } else {
        (bucket, Some(rest))
    }
}

fn parse_query_params(q: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::<String, String>::new();
    for (k, v) in url::form_urlencoded::parse(q.as_bytes()) {
        out.insert(k.to_string(), v.to_string());
    }
    out
}

fn verify_user_object_key(relpath: &str) -> Result<(), S3Error> {
    if fs_s3::is_internal_multipart_relpath(relpath) {
        return Err(S3Error::InvalidRequest {
            detail: format!("reserved object key prefix: {}", MULTIPART_DIR_PREFIX),
        });
    }
    Ok(())
}

fn parse_range_header(
    headers: &HeaderMap,
    size: i64,
) -> Result<(Option<i64>, Option<i64>), S3Error> {
    let Some(v) = headers.get(header::RANGE) else {
        return Ok((None, None));
    };
    let s = v.to_str().map_err(|_| S3Error::InvalidRange {
        detail: "invalid Range header".to_string(),
    })?;
    let s = s.trim();
    let Some(rest) = s.strip_prefix("bytes=") else {
        return Err(S3Error::InvalidRange {
            detail: "only bytes= ranges are supported".to_string(),
        });
    };
    let Some((a, b)) = rest.split_once('-') else {
        return Err(S3Error::InvalidRange {
            detail: "invalid Range header".to_string(),
        });
    };
    if a.is_empty() {
        // suffix range: "-N"
        let suffix: i64 = b.parse().map_err(|_| S3Error::InvalidRange {
            detail: "invalid suffix range".to_string(),
        })?;
        if suffix <= 0 {
            return Err(S3Error::InvalidRange {
                detail: "invalid suffix range".to_string(),
            });
        }
        let start = (size - suffix).max(0);
        let end = size.saturating_sub(1);
        return Ok((Some(start), Some(end)));
    }
    let start: i64 = a.parse().map_err(|_| S3Error::InvalidRange {
        detail: "invalid range start".to_string(),
    })?;
    if start < 0 || start >= size {
        return Err(S3Error::InvalidRange {
            detail: "range start out of bounds".to_string(),
        });
    }
    if b.is_empty() {
        return Ok((Some(start), Some(size - 1)));
    }
    let end: i64 = b.parse().map_err(|_| S3Error::InvalidRange {
        detail: "invalid range end".to_string(),
    })?;
    if end < start || end >= size {
        return Err(S3Error::InvalidRange {
            detail: "range end out of bounds".to_string(),
        });
    }
    Ok((Some(start), Some(end)))
}

fn object_version_etag(size: i64, mtime_ns: i64) -> String {
    format!("s{}_m{}", size.max(0), mtime_ns.max(0))
}

fn object_last_modified_http_date(mtime_ns: i64) -> String {
    ts_from_mtime_ns(mtime_ns)
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string()
}

fn normalize_http_etag_for_compare(value: &str) -> Option<&str> {
    let mut s = value.trim();
    if s.is_empty() {
        return None;
    }
    if s.starts_with("W/") || s.starts_with("w/") {
        s = s[2..].trim();
    }
    if s.len() >= 2 && s.as_bytes()[0] == b'"' && s.as_bytes()[s.len() - 1] == b'"' {
        s = &s[1..s.len() - 1];
    }
    if s.is_empty() { None } else { Some(s) }
}

fn parse_http_date_utc_seconds(value: &str) -> Option<i64> {
    let s = value.trim();
    if s.is_empty() {
        return None;
    }
    // English note:
    // - We only need a stable resume guard for browsers.
    // - Most clients use IMF-fixdate: "Sun, 06 Nov 1994 08:49:37 GMT".
    if let Ok(dt) = chrono::DateTime::parse_from_rfc2822(s) {
        return Some(dt.with_timezone(&Utc).timestamp());
    }
    let naive = NaiveDateTime::parse_from_str(s, "%a, %d %b %Y %H:%M:%S GMT").ok()?;
    Some(Utc.from_utc_datetime(&naive).timestamp())
}

fn if_range_allows_range(if_range: &str, size: i64, mtime_ns: i64) -> bool {
    let token = if_range.trim();
    if token.is_empty() {
        return false;
    }
    // RFC behavior: If-Range is either an entity-tag or an HTTP-date.
    if token.starts_with('"') || token.starts_with("W/") || token.starts_with("w/") {
        let want = match normalize_http_etag_for_compare(token) {
            Some(v) => v,
            None => return false,
        };
        let current = object_version_etag(size, mtime_ns);
        return want == current;
    }
    let want_ts = match parse_http_date_utc_seconds(token) {
        Some(v) => v,
        None => return false,
    };
    let current_ts = (mtime_ns / 1_000_000_000).max(0);
    want_ts == current_ts
}

fn apply_object_headers(
    headers: &mut axum::http::HeaderMap,
    size: i64,
    mtime_ns: i64,
    range_start: Option<i64>,
    range_end_inclusive: Option<i64>,
) {
    headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    headers.insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"{}\"", object_version_etag(size, mtime_ns))).unwrap(),
    );
    headers.insert(
        header::LAST_MODIFIED,
        HeaderValue::from_str(&object_last_modified_http_date(mtime_ns)).unwrap(),
    );
    if let Some(start) = range_start {
        let end = range_end_inclusive.unwrap_or(size - 1);
        let len = end - start + 1;
        headers.insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&len.to_string()).unwrap(),
        );
        headers.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes {}-{}/{}", start, end, size)).unwrap(),
        );
    } else {
        headers.insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&size.to_string()).unwrap(),
        );
    }
}

async fn get_object_stream(
    st: Arc<GatewayState>,
    request_identity: FluxonFsRequestIdentity,
    bucket: Arc<str>,
    rel: Arc<str>,
    file_size: i64,
    mtime_ns: i64,
    range_start: Option<i64>,
    range_end_inclusive: Option<i64>,
) -> Result<Body, S3Error> {
    if file_size < 0 {
        return Err(S3Error::Internal {
            detail: format!("invalid object size: {}", file_size),
        });
    }
    if file_size == 0 {
        return Ok(Body::empty());
    }

    let start = range_start.unwrap_or(0);
    let end = range_end_inclusive.unwrap_or(file_size - 1);
    let total = end - start + 1;
    if start < 0 || end < start || end >= file_size || total <= 0 {
        return Err(S3Error::InvalidRange {
            detail: "invalid range".to_string(),
        });
    }

    let inflight_u64 = st.s3_cfg.get_object_inflight_pieces;
    let inflight: usize = inflight_u64.try_into().map_err(|_| S3Error::Internal {
        detail: format!("get_object_inflight_pieces overflow: {}", inflight_u64),
    })?;
    if inflight == 0 {
        return Err(S3Error::Internal {
            detail: "invalid get_object_inflight_pieces=0".to_string(),
        });
    }
    let backend = st.backend.clone();
    let bucket_s = bucket.clone();
    let rel_s = rel.clone();

    let end_inclusive = end;
    let piece_bytes = FS_RPC_CHUNK_BYTES as i64;
    let mut off = start;

    // English note (causal chain):
    // - S3 GET should be bandwidth-bound; a strictly-serial per-piece loop makes it RTT-bound.
    // - We parallelize fixed-size piece reads with a bounded inflight window to provide backpressure.
    // - `buffered(inflight)` preserves order, so the HTTP body is still a sequential stream.
    let req_iter = std::iter::from_fn(move || {
        if off > end_inclusive {
            return None;
        }
        let left = end_inclusive - off + 1;
        let in_piece = piece_bytes - (off % piece_bytes);
        let n = std::cmp::min(left, in_piece);
        // English note: invariants (start/end validated above, off only moves forward) guarantee n > 0.
        let out = (off, n);
        off += n;
        Some(out)
    });
    let stream = futures::stream::iter(req_iter.map(move |(off, n)| {
        let backend = backend.clone();
        let bucket_s = bucket_s.clone();
        let rel_s = rel_s.clone();
        let bucket_dbg = bucket_s.clone();
        let rel_dbg = rel_s.clone();
        let request_identity = request_identity.clone();
        async move {
            if n <= 0 {
                return Err(S3Error::Internal {
                    detail: format!("invalid get_object chunk plan: off={} n={}", off, n),
                });
            }
            let data = backend
                .read_chunk_cached(request_identity, bucket_s, rel_s, off, n, file_size, mtime_ns)
                .await?;
            if data.len() as i64 != n {
                return Err(S3Error::Internal {
                    detail: format!(
                        "short read from backend: bucket={} rel={} off={} want={} got={} file_size={}",
                        bucket_dbg.as_ref(),
                        rel_dbg.as_ref(),
                        off,
                        n,
                        data.len(),
                        file_size
                    ),
                });
            }
            Ok::<Bytes, S3Error>(Bytes::from(data))
        }
    }))
    .buffered(inflight);

    Ok(Body::wrap_stream::<_, Bytes, S3Error>(stream))
}

async fn put_object_stream_with_sha256_etag(
    st: &GatewayState,
    request_identity: FluxonFsRequestIdentity,
    bucket: Arc<str>,
    rel: Arc<str>,
    mut body: Body,
) -> Result<(String, i64), S3Error> {
    let mut hasher = Sha256::new();
    let mut off: i64 = 0;
    while let Some(chunk) = body.data().await {
        let chunk = chunk.map_err(|e| S3Error::Internal {
            detail: format!("read request body failed: {}", e),
        })?;
        if chunk.is_empty() {
            continue;
        }
        hasher.update(&chunk);
        for part in chunk.chunks(FS_RPC_CHUNK_BYTES) {
            st.backend
                .write_chunk(
                    request_identity.clone(),
                    bucket.clone(),
                    rel.clone(),
                    off,
                    part.to_vec(),
                )
                .await?;
            off += part.len() as i64;
        }
    }
    let etag = hex::encode(hasher.finalize());
    Ok((etag, off))
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct RemoteDirEntry {
    pub name: String,
    pub is_file: bool,
    pub is_dir: bool,
    pub size: i64,
    pub mtime_ns: i64,
}

#[derive(Debug, Clone)]
pub struct RemoteStat {
    pub exists: bool,
    pub is_file: bool,
    pub is_dir: bool,
    pub size: i64,
    pub mtime_ns: i64,
}

async fn ensure_parent_dirs(
    st: &GatewayState,
    request_identity: FluxonFsRequestIdentity,
    export_name: Arc<str>,
    relpath: &str,
) -> Result<(), S3Error> {
    let p = relpath.trim_start_matches('/');
    let parts: Vec<&str> = p.split('/').filter(|s| !s.is_empty()).collect();
    if parts.len() <= 1 {
        return Ok(());
    }

    // Causal chain:
    // - S3 PUT Object has no explicit "mkdir -p" step.
    // - Fluxon FS exports are filesystem-backed, so we must create parent directories.
    // - The agent RPC `mkdir` does not ignore EEXIST, so we probe via `stat` first to keep error handling explicit.
    let mut cur = String::new();
    for d in &parts[..parts.len() - 1] {
        if cur.is_empty() {
            cur = d.to_string();
        } else {
            cur = format!("{}/{}", cur, d);
        }
        let stt = st
            .backend
            .stat(
                request_identity.clone(),
                export_name.clone(),
                Arc::from(cur.as_str()),
            )
            .await?;
        if stt.exists {
            if !stt.is_dir {
                return Err(S3Error::InvalidRequest {
                    detail: format!("parent path exists but is not a dir: {}", cur),
                });
            }
            continue;
        }
        st.backend
            .mkdir(
                request_identity.clone(),
                export_name.clone(),
                Arc::from(cur.as_str()),
                0o755,
            )
            .await?;
    }
    Ok(())
}

// ---------------- S3 XML ----------------

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('\"', "&quot;")
        .replace('\'', "&#39;")
}

fn list_buckets_xml(owner: &str, buckets: &[String]) -> String {
    let now: DateTime<Utc> = Utc::now();
    let mut s = String::new();
    s.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    s.push_str("<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");
    s.push_str(&format!(
        "  <Owner><ID>{}</ID><DisplayName>{}</DisplayName></Owner>\n",
        xml_escape(owner),
        xml_escape(owner)
    ));
    s.push_str("  <Buckets>\n");
    for name in buckets {
        s.push_str("    <Bucket>\n");
        s.push_str(&format!("      <Name>{}</Name>\n", xml_escape(name)));
        s.push_str(&format!(
            "      <CreationDate>{}</CreationDate>\n",
            now.to_rfc3339()
        ));
        s.push_str("    </Bucket>\n");
    }
    s.push_str("  </Buckets>\n");
    s.push_str("</ListAllMyBucketsResult>\n");
    s
}

fn normalize_dir_rel_from_prefix(prefix: &str) -> String {
    // Fluxon FS agent RPC requires `relpath` to be a non-empty string, but it can be "."
    // to represent the export root directory.
    let mut dir_rel = prefix.trim_start_matches('/').to_string();
    while dir_rel.ends_with('/') {
        dir_rel.pop();
    }
    if dir_rel.is_empty() {
        return ".".to_string();
    }
    dir_rel
}

async fn list_objects_v2_xml(
    st: &GatewayState,
    request_identity: FluxonFsRequestIdentity,
    bucket: Arc<str>,
    params: &BTreeMap<String, String>,
) -> Result<String, S3Error> {
    let prefix = params
        .get("prefix")
        .map(|s| s.to_string())
        .unwrap_or_else(|| "".to_string());
    let delimiter = params
        .get("delimiter")
        .map(|s| s.to_string())
        .unwrap_or_else(|| "/".to_string());
    let mut files: Vec<(String, RemoteDirEntry)> = Vec::new();
    let mut dirs: Vec<String> = Vec::new();
    match delimiter.as_str() {
        "/" => {
            let dir_rel = normalize_dir_rel_from_prefix(&prefix);
            let entries = st
                .backend
                .list_dir(
                    request_identity.clone(),
                    bucket.clone(),
                    Arc::from(dir_rel.as_str()),
                )
                .await?;
            for e in entries {
                if e.name == MULTIPART_DIR_PREFIX {
                    continue;
                }
                if e.is_dir {
                    dirs.push(format!("{}{}{}", prefix, e.name, "/"));
                } else if e.is_file {
                    let key = format!("{}{}", prefix, e.name);
                    files.push((key, e));
                }
            }
            dirs.sort();
            files.sort_by(|a, b| a.0.cmp(&b.0));
        }
        "" => {
            // Causal chain:
            // - Some S3 clients (for example `rclone size`) send `delimiter=` explicitly.
            // - In S3 ListObjectsV2, empty delimiter means "do not fold directories into CommonPrefixes".
            // - Therefore the gateway must recurse and emit a flat object list instead of rejecting the request.
            let mut pending = vec![(normalize_dir_rel_from_prefix(&prefix), prefix.clone())];
            while let Some((dir_rel, key_prefix)) = pending.pop() {
                let entries = st
                    .backend
                    .list_dir(
                        request_identity.clone(),
                        bucket.clone(),
                        Arc::from(dir_rel.as_str()),
                    )
                    .await?;
                let mut child_dirs: Vec<(String, String)> = Vec::new();
                for e in entries {
                    if e.name == MULTIPART_DIR_PREFIX {
                        continue;
                    }
                    if e.is_dir {
                        let child_key_prefix = format!("{}{}{}", key_prefix, e.name, "/");
                        child_dirs.push((
                            normalize_dir_rel_from_prefix(&child_key_prefix),
                            child_key_prefix,
                        ));
                    } else if e.is_file {
                        let key = format!("{}{}", key_prefix, e.name);
                        files.push((key, e));
                    }
                }
                child_dirs.sort_by(|a, b| a.1.cmp(&b.1));
                for child in child_dirs.into_iter().rev() {
                    pending.push(child);
                }
            }
            files.sort_by(|a, b| a.0.cmp(&b.0));
        }
        _ => {
            return Err(S3Error::InvalidRequest {
                detail: format!("unsupported delimiter: {}", delimiter),
            });
        }
    }

    let max_keys: usize = params
        .get("max-keys")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000);
    let mut emitted: usize = 0;

    let mut s = String::new();
    s.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    s.push_str("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");
    s.push_str(&format!("  <Name>{}</Name>\n", xml_escape(bucket.as_ref())));
    s.push_str(&format!("  <Prefix>{}</Prefix>\n", xml_escape(&prefix)));
    s.push_str(&format!(
        "  <Delimiter>{}</Delimiter>\n",
        xml_escape(&delimiter)
    ));
    s.push_str("  <IsTruncated>false</IsTruncated>\n");

    for d in dirs {
        if emitted >= max_keys {
            break;
        }
        s.push_str("  <CommonPrefixes>\n");
        s.push_str(&format!("    <Prefix>{}</Prefix>\n", xml_escape(&d)));
        s.push_str("  </CommonPrefixes>\n");
        emitted += 1;
    }

    for (key, f) in files {
        if emitted >= max_keys {
            break;
        }
        let lm = ts_from_mtime_ns(f.mtime_ns);
        s.push_str("  <Contents>\n");
        s.push_str(&format!("    <Key>{}</Key>\n", xml_escape(&key)));
        s.push_str(&format!(
            "    <LastModified>{}</LastModified>\n",
            lm.to_rfc3339()
        ));
        s.push_str(&format!("    <Size>{}</Size>\n", f.size));
        s.push_str("    <ETag>\"\"</ETag>\n");
        s.push_str("    <StorageClass>STANDARD</StorageClass>\n");
        s.push_str("  </Contents>\n");
        emitted += 1;
    }

    s.push_str("</ListBucketResult>\n");
    Ok(s)
}

fn ts_from_mtime_ns(mtime_ns: i64) -> DateTime<Utc> {
    let secs = (mtime_ns / 1_000_000_000).max(0) as i64;
    let nanos = (mtime_ns % 1_000_000_000).max(0) as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos).unwrap_or_else(|| Utc::now())
}

// ---------------- Multipart ----------------

fn new_upload_id() -> String {
    Uuid::new_v4().simple().to_string()
}

fn multipart_upload_dir(upload_id: &str) -> String {
    format!("{}/{}", MULTIPART_DIR_PREFIX, upload_id)
}

fn multipart_meta_path(upload_id: &str) -> String {
    format!("{}/meta.json", multipart_upload_dir(upload_id))
}

fn multipart_part_path(upload_id: &str, part_number: i64) -> Result<String, S3Error> {
    if !(1..=MULTIPART_MAX_PARTS).contains(&part_number) {
        return Err(S3Error::InvalidRequest {
            detail: format!(
                "invalid partNumber (expected 1..{}): {}",
                MULTIPART_MAX_PARTS, part_number
            ),
        });
    }
    Ok(format!(
        "{}/part-{:05}",
        multipart_upload_dir(upload_id),
        part_number
    ))
}

fn multipart_part_etag_path(upload_id: &str, part_number: i64) -> Result<String, S3Error> {
    Ok(format!(
        "{}.etag",
        multipart_part_path(upload_id, part_number)?
    ))
}

#[derive(Debug, Clone, serde::Deserialize)]
struct MultipartMetaRecord {
    key: String,
}

async fn multipart_load_meta(
    st: &GatewayState,
    request_identity: FluxonFsRequestIdentity,
    bucket: Arc<str>,
    upload_id: &str,
) -> Result<MultipartMetaRecord, S3Error> {
    let meta_path = multipart_meta_path(upload_id);
    let meta_path_arc: Arc<str> = Arc::from(meta_path.as_str());
    let meta_stat = st
        .backend
        .stat(
            request_identity.clone(),
            bucket.clone(),
            meta_path_arc.clone(),
        )
        .await?;
    if !meta_stat.exists || !meta_stat.is_file {
        return Err(S3Error::NoSuchUpload {
            upload_id: upload_id.to_string(),
        });
    }
    let n = std::cmp::min(meta_stat.size, FS_RPC_CHUNK_BYTES as i64);
    let data = st
        .backend
        .read_chunk_cached(
            request_identity,
            bucket,
            meta_path_arc,
            0,
            n,
            meta_stat.size,
            meta_stat.mtime_ns,
        )
        .await?;
    serde_json::from_slice(&data).map_err(|e| S3Error::Internal {
        detail: format!("decode multipart meta failed: {}", e),
    })
}

async fn multipart_create_upload_id(
    st: &GatewayState,
    request_identity: FluxonFsRequestIdentity,
    bucket: Arc<str>,
    key: &str,
) -> Result<String, S3Error> {
    // Ensure multipart root dir exists.
    let root_stat = st
        .backend
        .stat(
            request_identity.clone(),
            bucket.clone(),
            Arc::from(MULTIPART_DIR_PREFIX),
        )
        .await?;
    if !root_stat.exists {
        st.backend
            .mkdir(
                request_identity.clone(),
                bucket.clone(),
                Arc::from(MULTIPART_DIR_PREFIX),
                0o755,
            )
            .await?;
    }

    let upload_id = new_upload_id();
    let meta = serde_json::json!({
        "bucket": bucket.as_ref(),
        "key": key,
        "upload_id": upload_id,
        "created_unix_ms": Utc::now().timestamp_millis(),
    });
    let meta_bytes = serde_json::to_vec(&meta).map_err(|e| S3Error::Internal {
        detail: format!("json encode meta failed: {}", e),
    })?;
    let meta_path = multipart_meta_path(&upload_id);
    ensure_parent_dirs(st, request_identity.clone(), bucket.clone(), &meta_path).await?;
    let meta_path_arc: Arc<str> = Arc::from(meta_path.as_str());
    st.backend
        .write_chunk(
            request_identity.clone(),
            bucket.clone(),
            meta_path_arc.clone(),
            0,
            meta_bytes.clone(),
        )
        .await?;
    st.backend
        .truncate(
            request_identity,
            bucket.clone(),
            meta_path_arc,
            meta_bytes.len() as i64,
        )
        .await?;

    Ok(upload_id)
}

async fn multipart_create(
    st: &GatewayState,
    request_identity: FluxonFsRequestIdentity,
    bucket: Arc<str>,
    key: &str,
) -> Result<Response, S3Error> {
    let upload_id = multipart_create_upload_id(st, request_identity, bucket.clone(), key).await?;
    Ok(resp_xml(
        StatusCode::OK,
        xml_initiate_multipart(bucket.as_ref(), key, &upload_id),
    ))
}

fn xml_initiate_multipart(bucket: &str, key: &str, upload_id: &str) -> String {
    let mut s = String::new();
    s.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    s.push_str(
        "<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
    );
    s.push_str(&format!("  <Bucket>{}</Bucket>\n", xml_escape(bucket)));
    s.push_str(&format!("  <Key>{}</Key>\n", xml_escape(key)));
    s.push_str(&format!(
        "  <UploadId>{}</UploadId>\n",
        xml_escape(upload_id)
    ));
    s.push_str("</InitiateMultipartUploadResult>\n");
    s
}

async fn multipart_upload_part(
    st: &GatewayState,
    request_identity: FluxonFsRequestIdentity,
    bucket: Arc<str>,
    key: &str,
    upload_id: &str,
    part_number: i64,
    body: Body,
) -> Result<Response, S3Error> {
    let meta = multipart_load_meta(st, request_identity.clone(), bucket.clone(), upload_id).await?;
    if meta.key != key {
        return Err(S3Error::InvalidRequest {
            detail: format!(
                "multipart upload key mismatch: upload_id={} expected_key={} got_key={}",
                upload_id, meta.key, key
            ),
        });
    }

    let part_path = multipart_part_path(upload_id, part_number)?;
    ensure_parent_dirs(st, request_identity.clone(), bucket.clone(), &part_path).await?;
    let part_path_arc: Arc<str> = Arc::from(part_path.as_str());
    let (etag, size) = put_object_stream_with_sha256_etag(
        st,
        request_identity.clone(),
        bucket.clone(),
        part_path_arc.clone(),
        body,
    )
    .await?;
    st.backend
        .truncate(
            request_identity.clone(),
            bucket.clone(),
            part_path_arc,
            size,
        )
        .await?;

    let etag_path = multipart_part_etag_path(upload_id, part_number)?;
    let etag_path_arc: Arc<str> = Arc::from(etag_path.as_str());
    st.backend
        .write_chunk(
            request_identity.clone(),
            bucket.clone(),
            etag_path_arc.clone(),
            0,
            etag.as_bytes().to_vec(),
        )
        .await?;
    st.backend
        .truncate(request_identity, bucket, etag_path_arc, etag.len() as i64)
        .await?;

    Ok(resp_empty_with_etag(StatusCode::OK, &etag))
}

async fn list_multipart_uploads_xml(
    st: &GatewayState,
    account: &AuthAccount,
    request_identity: FluxonFsRequestIdentity,
    bucket: Arc<str>,
    params: &BTreeMap<String, String>,
) -> Result<String, S3Error> {
    let prefix = params.get("prefix").map(|s| s.as_str()).unwrap_or("");
    let root_stat = st
        .backend
        .stat(
            request_identity.clone(),
            bucket.clone(),
            Arc::from(MULTIPART_DIR_PREFIX),
        )
        .await?;
    if !root_stat.exists {
        return Ok(xml_list_multipart_uploads(bucket.as_ref(), Vec::new()));
    }
    let entries = st
        .backend
        .list_dir(
            request_identity.clone(),
            bucket.clone(),
            Arc::from(MULTIPART_DIR_PREFIX),
        )
        .await?;
    let mut uploads: Vec<(String, String)> = Vec::new(); // (key, upload_id)
    for e in entries {
        if !e.is_dir {
            continue;
        }
        let upload_id = e.name;
        let meta =
            match multipart_load_meta(st, request_identity.clone(), bucket.clone(), &upload_id)
                .await
            {
                Ok(v) => v,
                Err(S3Error::NoSuchUpload { .. }) => continue,
                Err(e) => return Err(e),
            };
        if meta.key.is_empty() {
            continue;
        }
        if !meta.key.starts_with(prefix) {
            continue;
        }
        if !account_has_bucket_action(
            account,
            bucket.as_ref(),
            &meta.key,
            FluxonFsS3PermissionAction::ListBucketMultipartUploads,
        ) {
            continue;
        }
        uploads.push((meta.key, upload_id));
    }
    uploads.sort();
    Ok(xml_list_multipart_uploads(bucket.as_ref(), uploads))
}

fn xml_list_multipart_uploads(bucket: &str, uploads: Vec<(String, String)>) -> String {
    let mut s = String::new();
    s.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    s.push_str("<ListMultipartUploadsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");
    s.push_str(&format!("  <Bucket>{}</Bucket>\n", xml_escape(bucket)));
    s.push_str("  <IsTruncated>false</IsTruncated>\n");
    for (key, upload_id) in uploads {
        s.push_str("  <Upload>\n");
        s.push_str(&format!("    <Key>{}</Key>\n", xml_escape(&key)));
        s.push_str(&format!(
            "    <UploadId>{}</UploadId>\n",
            xml_escape(&upload_id)
        ));
        s.push_str("    <Initiator><ID>admin</ID><DisplayName>admin</DisplayName></Initiator>\n");
        s.push_str("    <Owner><ID>admin</ID><DisplayName>admin</DisplayName></Owner>\n");
        s.push_str("    <StorageClass>STANDARD</StorageClass>\n");
        s.push_str("  </Upload>\n");
    }
    s.push_str("</ListMultipartUploadsResult>\n");
    s
}

async fn list_parts_xml(
    st: &GatewayState,
    request_identity: FluxonFsRequestIdentity,
    bucket: Arc<str>,
    key: &str,
    upload_id: &str,
    _params: &BTreeMap<String, String>,
) -> Result<String, S3Error> {
    let meta = multipart_load_meta(st, request_identity.clone(), bucket.clone(), upload_id).await?;
    if meta.key != key {
        return Err(S3Error::InvalidRequest {
            detail: format!(
                "multipart upload key mismatch: upload_id={} expected_key={} got_key={}",
                upload_id, meta.key, key
            ),
        });
    }
    let dir = multipart_upload_dir(upload_id);
    let entries = st
        .backend
        .list_dir(request_identity.clone(), bucket.clone(), dir.into())
        .await?;
    let mut parts: Vec<(i64, String, i64)> = Vec::new(); // (pn, etag, size)
    for e in entries {
        if !e.is_file {
            continue;
        }
        let Some(rest) = e.name.strip_prefix("part-") else {
            continue;
        };
        let pn: i64 = rest.parse::<i64>().unwrap_or(0);
        if pn <= 0 {
            continue;
        }
        let etag_path = multipart_part_etag_path(upload_id, pn)?;
        let etag_stat = st
            .backend
            .stat(
                request_identity.clone(),
                bucket.clone(),
                etag_path.clone().into(),
            )
            .await?;
        if !etag_stat.exists {
            continue;
        }
        let n = std::cmp::min(etag_stat.size, FS_RPC_CHUNK_BYTES as i64);
        let etag_bytes = st
            .backend
            .read_chunk_cached(
                request_identity.clone(),
                bucket.clone(),
                etag_path.clone().into(),
                0,
                n,
                etag_stat.size,
                etag_stat.mtime_ns,
            )
            .await?;
        let etag = String::from_utf8_lossy(&etag_bytes).trim().to_string();
        parts.push((pn, etag, e.size));
    }
    parts.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(xml_list_parts(bucket.as_ref(), key, upload_id, parts))
}

fn xml_list_parts(
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: Vec<(i64, String, i64)>,
) -> String {
    let mut s = String::new();
    s.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    s.push_str("<ListPartsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");
    s.push_str(&format!("  <Bucket>{}</Bucket>\n", xml_escape(bucket)));
    s.push_str(&format!("  <Key>{}</Key>\n", xml_escape(key)));
    s.push_str(&format!(
        "  <UploadId>{}</UploadId>\n",
        xml_escape(upload_id)
    ));
    s.push_str("  <IsTruncated>false</IsTruncated>\n");
    for (pn, etag, size) in parts {
        s.push_str("  <Part>\n");
        s.push_str(&format!("    <PartNumber>{}</PartNumber>\n", pn));
        s.push_str(&format!("    <ETag>\"{}\"</ETag>\n", xml_escape(&etag)));
        s.push_str(&format!("    <Size>{}</Size>\n", size));
        s.push_str("  </Part>\n");
    }
    s.push_str("</ListPartsResult>\n");
    s
}

#[derive(Debug, Clone)]
struct CompletePart {
    part_number: i64,
    etag: String,
}

fn parse_complete_multipart_body(body: &[u8]) -> Result<Vec<CompletePart>, S3Error> {
    let text = String::from_utf8_lossy(body);
    let mut parts: Vec<CompletePart> = Vec::new();
    let mut rest = text.as_ref();
    loop {
        let Some(p0) = rest.find("<Part>") else {
            break;
        };
        let rest2 = &rest[p0 + "<Part>".len()..];
        let Some(p1) = rest2.find("</Part>") else {
            break;
        };
        let part_xml = &rest2[..p1];
        rest = &rest2[p1 + "</Part>".len()..];

        let pn = extract_xml_tag_text(part_xml, "PartNumber")?;
        let et = extract_xml_tag_text(part_xml, "ETag")?;
        let part_number: i64 = pn.parse().map_err(|_| S3Error::InvalidRequest {
            detail: format!("invalid PartNumber: {}", pn),
        })?;
        let etag = et.trim().trim_matches('\"').to_string();
        parts.push(CompletePart { part_number, etag });
    }
    if parts.is_empty() {
        // English note:
        // - The browser UI uses the same multipart finalize path for every file size.
        // - A zero-byte upload therefore completes with an empty part list.
        // - We accept that shape here and let multipart_complete materialize a zero-byte object.
        if text.contains("<CompleteMultipartUpload") {
            return Ok(Vec::new());
        }
        return Err(S3Error::InvalidRequest {
            detail: "empty CompleteMultipartUpload body".to_string(),
        });
    }
    Ok(parts)
}

fn extract_xml_tag_text(xml: &str, tag: &str) -> Result<String, S3Error> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let Some(a) = xml.find(&open) else {
        return Err(S3Error::InvalidRequest {
            detail: format!("missing tag: {}", tag),
        });
    };
    let rest = &xml[a + open.len()..];
    let Some(b) = rest.find(&close) else {
        return Err(S3Error::InvalidRequest {
            detail: format!("missing closing tag: {}", tag),
        });
    };
    Ok(rest[..b].trim().to_string())
}

async fn multipart_complete(
    st: &GatewayState,
    request_identity: FluxonFsRequestIdentity,
    bucket: Arc<str>,
    key: &str,
    upload_id: &str,
    body: Body,
) -> Result<Response, S3Error> {
    let meta = multipart_load_meta(st, request_identity.clone(), bucket.clone(), upload_id).await?;
    if meta.key != key {
        return Err(S3Error::InvalidRequest {
            detail: format!(
                "multipart upload key mismatch: upload_id={} expected_key={} got_key={}",
                upload_id, meta.key, key
            ),
        });
    }
    let body_bytes = hyper::body::to_bytes(body)
        .await
        .map_err(|e| S3Error::InvalidRequest {
            detail: format!("read complete body failed: {}", e),
        })?;
    let parts = parse_complete_multipart_body(&body_bytes)?;
    let mut out_hasher = Sha256::new();
    let mut off: i64 = 0;
    ensure_parent_dirs(st, request_identity.clone(), bucket.clone(), key).await?;

    for p in parts.iter() {
        let part_path = multipart_part_path(upload_id, p.part_number)?;
        let etag_path = multipart_part_etag_path(upload_id, p.part_number)?;
        let etag_path_arc: Arc<str> = Arc::from(etag_path.as_str());
        let etag_stat = st
            .backend
            .stat(
                request_identity.clone(),
                bucket.clone(),
                etag_path_arc.clone(),
            )
            .await?;
        if !etag_stat.exists {
            return Err(S3Error::NoSuchUpload {
                upload_id: upload_id.to_string(),
            });
        }
        let n_et = std::cmp::min(etag_stat.size, FS_RPC_CHUNK_BYTES as i64);
        let etag_bytes = st
            .backend
            .read_chunk_cached(
                request_identity.clone(),
                bucket.clone(),
                etag_path_arc,
                0,
                n_et,
                etag_stat.size,
                etag_stat.mtime_ns,
            )
            .await?;
        let etag_got = String::from_utf8_lossy(&etag_bytes).trim().to_string();
        if etag_got != p.etag {
            return Err(S3Error::InvalidRequest {
                detail: format!(
                    "etag mismatch for part {}: expected={} got={}",
                    p.part_number, p.etag, etag_got
                ),
            });
        }
        let part_path_arc: Arc<str> = Arc::from(part_path.as_str());
        let st_part = st
            .backend
            .stat(
                request_identity.clone(),
                bucket.clone(),
                part_path_arc.clone(),
            )
            .await?;
        if !st_part.exists {
            return Err(S3Error::NoSuchUpload {
                upload_id: upload_id.to_string(),
            });
        }
        let mut left = st_part.size;
        let mut read_off: i64 = 0;
        while left > 0 {
            let n = std::cmp::min(left, FS_RPC_CHUNK_BYTES as i64);
            let chunk = st
                .backend
                .read_chunk_cached(
                    request_identity.clone(),
                    bucket.clone(),
                    part_path_arc.clone(),
                    read_off,
                    n,
                    st_part.size,
                    st_part.mtime_ns,
                )
                .await?;
            if chunk.is_empty() {
                break;
            }
            let nbytes = chunk.len() as i64;
            out_hasher.update(&chunk);
            // Chunk the write to satisfy the agent RPC size bound.
            for part in chunk.chunks(FS_RPC_CHUNK_BYTES) {
                st.backend
                    .write_chunk(
                        request_identity.clone(),
                        bucket.clone(),
                        Arc::from(key),
                        off,
                        part.to_vec(),
                    )
                    .await?;
                off += part.len() as i64;
            }
            read_off += nbytes;
            left -= nbytes;
        }
    }
    st.backend
        .truncate(
            request_identity.clone(),
            bucket.clone(),
            Arc::from(key),
            off,
        )
        .await?;
    let etag = hex::encode(out_hasher.finalize());
    cleanup_upload(st, request_identity, bucket.clone(), upload_id).await?;
    Ok(resp_xml(
        StatusCode::OK,
        xml_complete_multipart(bucket.as_ref(), key, &etag),
    ))
}

fn xml_complete_multipart(bucket: &str, key: &str, etag: &str) -> String {
    let mut s = String::new();
    s.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    s.push_str(
        "<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
    );
    s.push_str(&format!("  <Bucket>{}</Bucket>\n", xml_escape(bucket)));
    s.push_str(&format!("  <Key>{}</Key>\n", xml_escape(key)));
    s.push_str(&format!("  <ETag>\"{}\"</ETag>\n", xml_escape(etag)));
    s.push_str("</CompleteMultipartUploadResult>\n");
    s
}

async fn multipart_abort(
    st: &GatewayState,
    request_identity: FluxonFsRequestIdentity,
    bucket: Arc<str>,
    key: &str,
    upload_id: &str,
) -> Result<Response, S3Error> {
    let meta = multipart_load_meta(st, request_identity.clone(), bucket.clone(), upload_id).await?;
    if meta.key != key {
        return Err(S3Error::InvalidRequest {
            detail: format!(
                "multipart upload key mismatch: upload_id={} expected_key={} got_key={}",
                upload_id, meta.key, key
            ),
        });
    }
    cleanup_upload(st, request_identity, bucket, upload_id).await?;
    Ok(resp_empty(StatusCode::NO_CONTENT))
}

async fn cleanup_upload(
    st: &GatewayState,
    request_identity: FluxonFsRequestIdentity,
    bucket: Arc<str>,
    upload_id: &str,
) -> Result<(), S3Error> {
    let dir = multipart_upload_dir(upload_id);
    let dir_arc: Arc<str> = Arc::from(dir.as_str());
    let st_dir = st
        .backend
        .stat(request_identity.clone(), bucket.clone(), dir_arc.clone())
        .await?;
    if !st_dir.exists {
        return Ok(());
    }
    let entries = st
        .backend
        .list_dir(request_identity.clone(), bucket.clone(), dir_arc)
        .await?;
    for e in entries {
        if e.is_file {
            st.backend
                .unlink(
                    request_identity.clone(),
                    bucket.clone(),
                    Arc::from(format!("{}/{}", dir, e.name)),
                )
                .await?;
        }
    }
    st.backend
        .rmdir(request_identity, bucket, Arc::from(dir.as_str()))
        .await?;
    Ok(())
}

// ---------------- SigV4 ----------------

#[derive(Debug, Clone)]
struct AuthCtx {
    username: String,
    secret_key: String,
    amz_date: String,
    region: String,
    account: AuthAccount,
}

impl AuthCtx {
    fn request_identity(&self) -> FluxonFsRequestIdentity {
        FluxonFsRequestIdentity {
            username: self.username.clone(),
            password: self.secret_key.clone(),
        }
    }
}

fn auth_verify_sigv4(st: &GatewayState, headers: &HeaderMap) -> Result<AuthCtx, S3Error> {
    let auth = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| S3Error::AccessDenied {
            detail: "missing Authorization header".to_string(),
        })?;
    if !auth.starts_with(SIGV4_ALG) {
        return Err(S3Error::AccessDenied {
            detail: "unsupported authorization algorithm".to_string(),
        });
    }
    let amz_date = headers
        .get("x-amz-date")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| S3Error::AccessDenied {
            detail: "missing x-amz-date header".to_string(),
        })?
        .to_string();

    let (access_key, region) = parse_credential_from_authorization(auth)?;
    let Some(account) = find_account_by_username(st, &access_key) else {
        return Err(S3Error::AccessDenied {
            detail: format!("invalid access key: {}", access_key),
        });
    };
    Ok(AuthCtx {
        username: account.username.clone(),
        secret_key: account.password.clone(),
        amz_date,
        region,
        account,
    })
}

fn auth_original_path_and_query(
    external_base_path: &str,
    cluster_name: &str,
    fallback_uri: &Uri,
    headers: &HeaderMap,
    upstream_path_capture: &str,
) -> Result<(String, Option<String>), S3Error> {
    if let Some(v) = headers.get(HDR_ORIGINAL_URI).and_then(|x| x.to_str().ok()) {
        // Header holds a path-absolute URI string (no scheme), e.g. "/r/fs_s3/cluster/bucket/key?x=1".
        let (path, query) = split_path_and_query(v)?;

        // Causal chain:
        // - fluxon_cli forwards the original URI so upstream can validate SigV4 for the *client-visible* path.
        // - The upstream handler routes by the rewritten path (stripped /r/<service>/<cluster>/...).
        // - If the forwarded original path does not match the rewritten path, signature validation would be ambiguous.
        let expected_suffix = if upstream_path_capture.trim().is_empty() {
            "/".to_string()
        } else {
            format!("/{}", upstream_path_capture.trim_start_matches('/'))
        };
        let expected_prefix = format!("/r/{}/{}/", SERVICE_NAME, cluster_name);
        if !path.starts_with(&expected_prefix) {
            return Err(S3Error::InvalidRequest {
                detail: format!("invalid original uri header (unexpected prefix): {}", path),
            });
        }
        let rest = format!("/{}", path[expected_prefix.len()..].trim_start_matches('/'));
        if rest != expected_suffix {
            return Err(S3Error::InvalidRequest {
                detail: format!(
                    "invalid original uri header (path mismatch): orig_rest={} expected={}",
                    rest, expected_suffix
                ),
            });
        }
        return Ok((path, query));
    }
    let path = direct_public_path(external_base_path, upstream_path_capture);
    let query = fallback_uri.query().map(|q| q.to_string());
    Ok((path, query))
}

fn direct_public_path(external_base_path: &str, upstream_path_capture: &str) -> String {
    let mut out = if external_base_path.is_empty() {
        String::new()
    } else {
        external_base_path.to_string()
    };
    out.push('/');
    let trimmed = upstream_path_capture.trim_start_matches('/');
    if !trimmed.is_empty() {
        out.push_str(trimmed);
    }
    out
}

fn split_path_and_query(s: &str) -> Result<(String, Option<String>), S3Error> {
    let u = s.trim();
    if !u.starts_with('/') {
        return Err(S3Error::InvalidRequest {
            detail: format!(
                "invalid original uri header (expected absolute path): {}",
                s
            ),
        });
    }
    if let Some((p, q)) = u.split_once('?') {
        Ok((p.to_string(), Some(q.to_string())))
    } else {
        Ok((u.to_string(), None))
    }
}

fn parse_credential_from_authorization(auth: &str) -> Result<(String, String), S3Error> {
    let parts = auth.splitn(2, ' ').collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err(S3Error::AccessDenied {
            detail: "invalid Authorization header".to_string(),
        });
    }
    let kvs = parts[1]
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();
    for kv in kvs {
        if let Some(rest) = kv.strip_prefix("Credential=") {
            let items = rest.split('/').collect::<Vec<_>>();
            if items.len() < 5 {
                return Err(S3Error::AccessDenied {
                    detail: format!("invalid Credential: {}", rest),
                });
            }
            let access_key = items[0].to_string();
            let region = items[2].to_string();
            let service = items[3];
            let term = items[4];
            if service != SIGV4_SERVICE || term != SIGV4_TERMINATOR {
                return Err(S3Error::AccessDenied {
                    detail: format!("invalid credential scope: {}", rest),
                });
            }
            return Ok((access_key, region));
        }
    }
    Err(S3Error::AccessDenied {
        detail: "Authorization missing Credential".to_string(),
    })
}

fn verify_sigv4_authorization_header(
    external_base_path: &str,
    ctx: &AuthCtx,
    headers: &HeaderMap,
    method: &Method,
    canonical_uri: &str,
    canonical_query: Option<&str>,
) -> Result<(), S3Error> {
    let auth = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| S3Error::AccessDenied {
            detail: "missing Authorization header".to_string(),
        })?
        .to_string();

    let (signed_headers, signature_hex, scope_date) = parse_auth_signed_headers_and_sig(&auth)?;
    let scope = format!(
        "{}/{}/{}/{}",
        scope_date, ctx.region, SIGV4_SERVICE, SIGV4_TERMINATOR
    );

    let payload_hash = headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or(UNSIGNED_PAYLOAD)
        .to_string();

    let canonical_query2 = canonical_query_string(canonical_query.unwrap_or(""));
    let canonical_headers = canonical_headers_string(headers, &signed_headers)?;
    let signed_headers_joined = signed_headers.join(";");
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method.as_str(),
        canonical_uri,
        canonical_query2,
        canonical_headers,
        signed_headers_joined,
        payload_hash
    );
    let cr_hash = sha256_hex(canonical_request.as_bytes());
    let string_to_sign = format!("{}\n{}\n{}\n{}", SIGV4_ALG, ctx.amz_date, scope, cr_hash);
    let signing_key = derive_signing_key(&ctx.secret_key, &scope_date, &ctx.region, SIGV4_SERVICE);
    let expect = hmac_sha256_hex(&signing_key, string_to_sign.as_bytes());
    if eq_hex(&expect, &signature_hex) {
        return Ok(());
    }

    let alt_canonical_uri = canonical_uri_without_base_path(external_base_path, canonical_uri);
    if let Some(alt_uri) = alt_canonical_uri.as_deref() {
        let alt_canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method.as_str(),
            alt_uri,
            canonical_query2,
            canonical_headers,
            signed_headers_joined,
            payload_hash
        );
        let alt_cr_hash = sha256_hex(alt_canonical_request.as_bytes());
        let alt_string_to_sign = format!(
            "{}\n{}\n{}\n{}",
            SIGV4_ALG, ctx.amz_date, scope, alt_cr_hash
        );
        let alt_expect = hmac_sha256_hex(&signing_key, alt_string_to_sign.as_bytes());
        if eq_hex(&alt_expect, &signature_hex) {
            return Ok(());
        }
        tracing::warn!(
            username = %ctx.username,
            method = %method,
            canonical_uri = canonical_uri,
            alt_canonical_uri = alt_uri,
            canonical_query = canonical_query.unwrap_or(""),
            signed_headers = %signed_headers_joined,
            expected_signature = %expect,
            alt_expected_signature = %alt_expect,
            provided_signature = %signature_hex,
            "s3 sigv4 signature mismatch"
        );
    } else {
        tracing::warn!(
            username = %ctx.username,
            method = %method,
            canonical_uri = canonical_uri,
            canonical_query = canonical_query.unwrap_or(""),
            signed_headers = %signed_headers_joined,
            expected_signature = %expect,
            provided_signature = %signature_hex,
            "s3 sigv4 signature mismatch"
        );
    }
    Err(S3Error::AccessDenied {
        detail: "signature mismatch".to_string(),
    })
}

fn canonical_uri_without_base_path(
    external_base_path: &str,
    canonical_uri: &str,
) -> Option<String> {
    let base = external_base_path.trim_end_matches('/');
    if base.is_empty() {
        return None;
    }
    let rest = canonical_uri.strip_prefix(base)?;
    if rest.is_empty() {
        return Some("/".to_string());
    }
    if rest.starts_with('/') {
        return Some(rest.to_string());
    }
    Some(format!("/{}", rest))
}

fn parse_auth_signed_headers_and_sig(auth: &str) -> Result<(Vec<String>, String, String), S3Error> {
    // Example:
    //   AWS4-HMAC-SHA256 Credential=admin/20260213/us-east-1/s3/aws4_request, SignedHeaders=host;..., Signature=...
    let parts = auth.splitn(2, ' ').collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err(S3Error::AccessDenied {
            detail: "invalid Authorization header".to_string(),
        });
    }
    let mut scope_date: Option<String> = None;
    let mut signed_headers: Option<Vec<String>> = None;
    let mut signature: Option<String> = None;
    let kvs = parts[1]
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();
    for kv in kvs {
        if let Some(rest) = kv.strip_prefix("Credential=") {
            let items = rest.split('/').collect::<Vec<_>>();
            if items.len() < 5 {
                return Err(S3Error::AccessDenied {
                    detail: format!("invalid Credential: {}", rest),
                });
            }
            scope_date = Some(items[1].to_string());
        } else if let Some(rest) = kv.strip_prefix("SignedHeaders=") {
            let hs = rest
                .split(';')
                .map(|s| s.trim().to_ascii_lowercase())
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>();
            signed_headers = Some(hs);
        } else if let Some(rest) = kv.strip_prefix("Signature=") {
            signature = Some(rest.trim().to_string());
        }
    }
    let scope_date = scope_date.ok_or_else(|| S3Error::AccessDenied {
        detail: "Authorization missing Credential".to_string(),
    })?;
    let signed_headers = signed_headers.ok_or_else(|| S3Error::AccessDenied {
        detail: "Authorization missing SignedHeaders".to_string(),
    })?;
    if signed_headers.is_empty() {
        return Err(S3Error::AccessDenied {
            detail: "SignedHeaders is empty".to_string(),
        });
    }
    let signature_hex = signature.ok_or_else(|| S3Error::AccessDenied {
        detail: "Authorization missing Signature".to_string(),
    })?;
    Ok((signed_headers, signature_hex, scope_date))
}

fn canonical_headers_string(
    headers: &HeaderMap,
    signed_headers: &[String],
) -> Result<String, S3Error> {
    let mut out = String::new();
    for name in signed_headers.iter() {
        if name == "host" {
            if let Some(v) = headers.get(HDR_ORIGINAL_HOST).and_then(|v| v.to_str().ok()) {
                out.push_str("host:");
                out.push_str(v.trim());
                out.push('\n');
                continue;
            }
        }
        let hv = headers
            .get(name.as_str())
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| S3Error::AccessDenied {
                detail: format!("missing signed header: {}", name),
            })?;
        out.push_str(name);
        out.push(':');
        out.push_str(hv.trim());
        out.push('\n');
    }
    Ok(out)
}

fn canonical_query_string(q: &str) -> String {
    let mut pairs: Vec<(String, String)> = Vec::new();
    for (k, v) in url::form_urlencoded::parse(q.as_bytes()) {
        pairs.push((k.to_string(), v.to_string()));
    }
    pairs.sort();
    let mut ser = url::form_urlencoded::Serializer::new(String::new());
    for (k, v) in pairs {
        ser.append_pair(&k, &v);
    }
    ser.finish().replace('+', "%20")
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

fn derive_signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_secret = format!("AWS4{}", secret);
    let k_date = hmac_sha256(k_secret.as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, SIGV4_TERMINATOR.as_bytes())
}

fn hmac_sha256(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(msg);
    mac.finalize().into_bytes().to_vec()
}

fn hmac_sha256_hex(key: &[u8], msg: &[u8]) -> String {
    hex::encode(hmac_sha256(key, msg))
}

fn eq_hex(a: &str, b: &str) -> bool {
    // Avoid early-exit on length mismatch.
    if a.len() != b.len() {
        return false;
    }
    let mut ok: u8 = 0;
    for (x, y) in a.as_bytes().iter().zip(b.as_bytes().iter()) {
        ok |= x ^ y;
    }
    ok == 0
}

// ---------------- UI (SSR) ----------------

// English note:
// - UI remains in the crate root scope for now to keep router wiring and helper visibility stable.
// - The source moved into a separate Rust file so lib.rs no longer mixes the entire SSR UI domain
//   with the gateway data plane in one 6k+ line file.
include!("ui_ssr.rs");

// ---------------- Responses ----------------

fn resp_xml(status: StatusCode, xml: String) -> Response {
    let mut resp = xml.into_response();
    *resp.status_mut() = status;
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml; charset=utf-8"),
    );
    resp
}

fn resp_empty(status: StatusCode) -> Response {
    let mut resp = Response::new(boxed(Body::empty()));
    *resp.status_mut() = status;
    resp
}

fn resp_empty_with_etag(status: StatusCode, etag: &str) -> Response {
    let mut resp = resp_empty(status);
    resp.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"{}\"", etag)).unwrap(),
    );
    resp
}

fn json_response<T: serde::Serialize + ?Sized>(status: StatusCode, value: &T) -> Response {
    let mut resp = serde_json::to_string(value).unwrap().into_response();
    *resp.status_mut() = status;
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    resp
}

fn ui_json_for_script<T: serde::Serialize + ?Sized>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap()
        .replace('<', "\\u003c")
        .replace('&', "\\u0026")
}

fn text_response(status: StatusCode, body: String) -> Response {
    let mut resp = body.into_response();
    *resp.status_mut() = status;
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    resp
}

fn s3_error_response(_uri: &Uri, e: S3Error) -> Response {
    let (code, msg, status) = match &e {
        S3Error::AccessDenied { detail } => ("AccessDenied", detail.clone(), StatusCode::FORBIDDEN),
        S3Error::InvalidRequest { detail } => {
            ("InvalidRequest", detail.clone(), StatusCode::BAD_REQUEST)
        }
        S3Error::InvalidRange { detail } => (
            "InvalidRange",
            detail.clone(),
            StatusCode::RANGE_NOT_SATISFIABLE,
        ),
        S3Error::NoSuchBucket { bucket } => ("NoSuchBucket", bucket.clone(), StatusCode::NOT_FOUND),
        S3Error::NoSuchKey { key, .. } => ("NoSuchKey", key.clone(), StatusCode::NOT_FOUND),
        S3Error::NoSuchUpload { upload_id } => {
            ("NoSuchUpload", upload_id.clone(), StatusCode::NOT_FOUND)
        }
        S3Error::Internal { detail } => (
            "InternalError",
            detail.clone(),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    };
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{}</Code><Message>{}</Message></Error>\n",
        xml_escape(code),
        xml_escape(&msg)
    );
    resp_xml(status, body)
}

#[cfg(test)]
mod tests {
    use crate::open_access_db;
    use std::collections::{BTreeMap, BTreeSet};
    use std::fs::{self, File};
    use std::net::TcpListener;
    use std::path::{Path, PathBuf};
    use std::process::{self, Child, Command, Stdio};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;
    use std::time::Instant;

    use axum::Router;
    use axum::body::Body;
    use axum::body::HttpBody as _;
    use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode, Uri};
    use axum::routing::any;
    use chrono::Utc;
    use parking_lot::Mutex;
    use tower::ServiceExt as _;

    use super::{
        FS_RPC_CHUNK_BYTES, FsMasterAdminBackend, FsS3Backend, FsTransferCreateJobArg,
        GatewayAccessConfig, GatewayState, S3Error, encode_transfer_manifest_blob,
    };
    use crate::transfer::encode_transfer_manifest_blob_with_empty_dirs;
    use fluxon_fs_core::config::{
        FLUXON_FS_LOCAL_TRANSFER_CHECK_DST_EXPORT, FLUXON_FS_LOCAL_TRANSFER_CHECK_SRC_EXPORT,
        FluxonFsAccessModel, FluxonFsAccessUser, FluxonFsExport, FluxonFsExportRoutingMode,
        FluxonFsGlobalConfig, FluxonFsLocalTransferCheckJobSpecWire, FluxonFsRequestIdentity,
        FluxonFsS3GatewayConfig, FluxonFsS3KvMissPolicy, FluxonFsS3PermissionAccount,
        FluxonFsS3PermissionAction, FluxonFsTransferBatchCollectInfoWire,
        FluxonFsTransferBatchKind, FluxonFsTransferBatchState, FluxonFsTransferCollectInfoKind,
        FluxonFsTransferJobState, FluxonFsTransferManifestEntryWire, FluxonFsTransferManifestWire,
        FluxonFsTransferScanBatchWire, FluxonFsTransferScanFrontier,
        FluxonFsTransferScanResultWire, FluxonFsTransferStateStoreConfig,
        FluxonFsTransferStateStoreKind, FluxonFsTransferStateStoreTiKvConfig,
        FluxonFsTransferSymlinkNoticeEntryWire, FluxonFsTransferWorkerCollectInfoResultWire,
        FluxonFsTransferWorkerFileResultWire, FluxonFsTransferWorkerResultWire,
        export_rpc_paths_for_export_name_v1, transfer_collect_info_output_relpath,
    };
    use std::sync::OnceLock;
    use uuid::Uuid;

    const TEST_TIKV_WORK_ROOT: &str = "/mnt/nvme0/fluxon_fs_transfer_tikv/rust_gateway";
    const TEST_TIKV_READY_TIMEOUT_SECS: u64 = 180;
    const TEST_TIKV_PD_LEASE_SECS: u64 = 60;
    const TEST_TIKV_UNIFIED_READPOOL_MAX_THREADS: u64 = 4;
    const TEST_TIKV_STORAGE_READPOOL_CONCURRENCY: u64 = 2;
    const TEST_TIKV_COPROCESSOR_READPOOL_CONCURRENCY: u64 = 2;
    const TEST_TIKV_ENDPOINT_MAX_CONCURRENCY: u64 = 8;
    const TEST_TIKV_BACKGROUND_THREAD_COUNT: u64 = 2;
    const TEST_TIKV_SCHEDULER_CONCURRENCY: u64 = 2048;
    const TEST_TIKV_SCHEDULER_WORKER_POOL_SIZE: u64 = 2;
    const TEST_TIKV_APPLY_POOL_SIZE: u64 = 1;
    const TEST_TIKV_STORE_POOL_SIZE: u64 = 1;
    const TEST_TIKV_ROCKSDB_MAX_BACKGROUND_JOBS: u64 = 2;
    const TEST_TIKV_RAFTDB_MAX_BACKGROUND_JOBS: u64 = 2;

    #[derive(Clone)]
    struct MemBackend {
        data: Arc<Vec<u8>>,
        reqs: Arc<Mutex<Vec<(i64, i64)>>>,
        inflight: Arc<AtomicUsize>,
        max_inflight: Arc<AtomicUsize>,
    }

    impl FsS3Backend for MemBackend {
        fn stat(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            _export_name: Arc<str>,
            _relpath: Arc<str>,
        ) -> futures::future::BoxFuture<'static, Result<super::RemoteStat, S3Error>> {
            Box::pin(async move {
                Err(S3Error::Internal {
                    detail: "unused in test".to_string(),
                })
            })
        }

        fn list_dir(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            _export_name: Arc<str>,
            _relpath: Arc<str>,
        ) -> futures::future::BoxFuture<'static, Result<Vec<super::RemoteDirEntry>, S3Error>>
        {
            Box::pin(async move {
                Err(S3Error::Internal {
                    detail: "unused in test".to_string(),
                })
            })
        }

        fn read_chunk_cached(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            _export_name: Arc<str>,
            _relpath: Arc<str>,
            offset: i64,
            length: i64,
            _file_size: i64,
            _mtime_ns: i64,
        ) -> futures::future::BoxFuture<'static, Result<Vec<u8>, S3Error>> {
            let this = self.clone();
            Box::pin(async move {
                this.reqs.lock().push((offset, length));

                let cur = this.inflight.fetch_add(1, Ordering::SeqCst) + 1;
                loop {
                    let prev = this.max_inflight.load(Ordering::SeqCst);
                    if cur <= prev {
                        break;
                    }
                    if this
                        .max_inflight
                        .compare_exchange(prev, cur, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        break;
                    }
                }

                // Make concurrency visible to the test.
                tokio::time::sleep(Duration::from_millis(10)).await;

                let off_usize: usize = offset.try_into().map_err(|_| S3Error::Internal {
                    detail: format!("offset overflow: {}", offset),
                })?;
                let len_usize: usize = length.try_into().map_err(|_| S3Error::Internal {
                    detail: format!("length overflow: {}", length),
                })?;
                let end = off_usize
                    .checked_add(len_usize)
                    .ok_or_else(|| S3Error::Internal {
                        detail: "range overflow".to_string(),
                    })?;
                if end > this.data.len() {
                    return Err(S3Error::Internal {
                        detail: "out of range".to_string(),
                    });
                }
                let out = this.data[off_usize..end].to_vec();

                this.inflight.fetch_sub(1, Ordering::SeqCst);
                Ok(out)
            })
        }

        fn write_chunk(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            _export_name: Arc<str>,
            _relpath: Arc<str>,
            _offset: i64,
            _data: Vec<u8>,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            Box::pin(async move {
                Err(S3Error::Internal {
                    detail: "unused in test".to_string(),
                })
            })
        }

        fn truncate(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            _export_name: Arc<str>,
            _relpath: Arc<str>,
            _size: i64,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            Box::pin(async move {
                Err(S3Error::Internal {
                    detail: "unused in test".to_string(),
                })
            })
        }

        fn mkdir(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            _export_name: Arc<str>,
            _relpath: Arc<str>,
            _mode: i64,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            Box::pin(async move {
                Err(S3Error::Internal {
                    detail: "unused in test".to_string(),
                })
            })
        }

        fn unlink(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            _export_name: Arc<str>,
            _relpath: Arc<str>,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            Box::pin(async move {
                Err(S3Error::Internal {
                    detail: "unused in test".to_string(),
                })
            })
        }

        fn rename(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            _export_name: Arc<str>,
            _src_relpath: Arc<str>,
            _dst_relpath: Arc<str>,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            Box::pin(async move {
                Err(S3Error::Internal {
                    detail: "unused in test".to_string(),
                })
            })
        }

        fn rmdir(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            _export_name: Arc<str>,
            _relpath: Arc<str>,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            Box::pin(async move {
                Err(S3Error::Internal {
                    detail: "unused in test".to_string(),
                })
            })
        }
    }

    #[derive(Clone, Default)]
    struct TestFsMasterAdminBackend;

    impl FsMasterAdminBackend for TestFsMasterAdminBackend {
        fn list_fs_master_members(
            &self,
        ) -> futures::future::BoxFuture<'static, Result<Vec<super::FsMasterMemberRecord>, String>>
        {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn list_fs_master_online_member_ids(
            &self,
        ) -> futures::future::BoxFuture<'static, Result<BTreeSet<String>, String>> {
            Box::pin(async { Ok(BTreeSet::new()) })
        }

        fn list_fs_master_agent_dir(
            &self,
            _agent_instance_key: String,
            _dir_abs: String,
        ) -> futures::future::BoxFuture<
            'static,
            Result<Vec<super::FsMasterAdminBrowseDirEntry>, String>,
        > {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    #[derive(Clone)]
    struct StaticFsMasterAdminBackend {
        members: Vec<super::FsMasterMemberRecord>,
        online_member_ids: BTreeSet<String>,
    }

    impl FsMasterAdminBackend for StaticFsMasterAdminBackend {
        fn list_fs_master_members(
            &self,
        ) -> futures::future::BoxFuture<'static, Result<Vec<super::FsMasterMemberRecord>, String>>
        {
            let members = self.members.clone();
            Box::pin(async move { Ok(members) })
        }

        fn list_fs_master_online_member_ids(
            &self,
        ) -> futures::future::BoxFuture<'static, Result<BTreeSet<String>, String>> {
            let online_member_ids = self.online_member_ids.clone();
            Box::pin(async move { Ok(online_member_ids) })
        }

        fn list_fs_master_agent_dir(
            &self,
            _agent_instance_key: String,
            _dir_abs: String,
        ) -> futures::future::BoxFuture<
            'static,
            Result<Vec<super::FsMasterAdminBrowseDirEntry>, String>,
        > {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    #[derive(Clone, Default)]
    struct ObjectBackend {
        objects: Arc<Mutex<BTreeMap<(String, String), Vec<u8>>>>,
        directories: Arc<Mutex<BTreeSet<(String, String)>>>,
        rename_count: Arc<AtomicUsize>,
    }

    impl ObjectBackend {
        fn insert(&self, bucket: &str, key: &str, data: &[u8]) {
            self.objects
                .lock()
                .insert((bucket.to_string(), key.to_string()), data.to_vec());
        }

        fn insert_dir(&self, bucket: &str, key: &str) {
            self.directories
                .lock()
                .insert((bucket.to_string(), key.to_string()));
        }

        fn get(&self, bucket: &str, key: &str) -> Option<Vec<u8>> {
            self.objects
                .lock()
                .get(&(bucket.to_string(), key.to_string()))
                .cloned()
        }

        fn has_directory_coverage(&self, bucket: &str, key: &str) -> bool {
            if key == "." || key.is_empty() {
                return true;
            }
            if self
                .directories
                .lock()
                .contains(&(bucket.to_string(), key.to_string()))
            {
                return true;
            }
            let dir_prefix = format!("{}/", key);
            if self.directories.lock().iter().any(|(dir_bucket, dir_key)| {
                dir_bucket == bucket && dir_key.starts_with(dir_prefix.as_str())
            }) {
                return true;
            }
            self.objects.lock().keys().any(|(obj_bucket, obj_key)| {
                obj_bucket == bucket && obj_key.starts_with(dir_prefix.as_str())
            })
        }
    }

    #[derive(Clone)]
    struct SlowObjectBackend {
        inner: ObjectBackend,
        delay: Duration,
    }

    impl SlowObjectBackend {
        fn new(delay: Duration) -> Self {
            Self {
                inner: ObjectBackend::default(),
                delay,
            }
        }

        fn insert(&self, bucket: &str, key: &str, data: &[u8]) {
            self.inner.insert(bucket, key, data);
        }

        fn get(&self, bucket: &str, key: &str) -> Option<Vec<u8>> {
            self.inner.get(bucket, key)
        }
    }

    impl FsS3Backend for ObjectBackend {
        fn stat(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            export_name: Arc<str>,
            relpath: Arc<str>,
        ) -> futures::future::BoxFuture<'static, Result<super::RemoteStat, S3Error>> {
            let this = self.clone();
            let bucket = export_name.to_string();
            let key = relpath.to_string();
            Box::pin(async move {
                let objects = this.objects.lock();
                if let Some(data) = objects.get(&(bucket.clone(), key.clone())) {
                    return Ok(super::RemoteStat {
                        exists: true,
                        is_file: true,
                        is_dir: false,
                        size: data.len() as i64,
                        mtime_ns: 1,
                    });
                }
                drop(objects);
                if this.has_directory_coverage(bucket.as_str(), key.as_str()) {
                    return Ok(super::RemoteStat {
                        exists: true,
                        is_file: false,
                        is_dir: true,
                        size: 0,
                        mtime_ns: 0,
                    });
                }
                Ok(super::RemoteStat {
                    exists: false,
                    is_file: false,
                    is_dir: false,
                    size: 0,
                    mtime_ns: 0,
                })
            })
        }

        fn list_dir(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            export_name: Arc<str>,
            relpath: Arc<str>,
        ) -> futures::future::BoxFuture<'static, Result<Vec<super::RemoteDirEntry>, S3Error>>
        {
            let this = self.clone();
            let bucket = export_name.to_string();
            let dir_rel = relpath.to_string();
            Box::pin(async move {
                let objects = this.objects.lock();
                let directories = this.directories.lock();
                let dir_prefix = if dir_rel == "." || dir_rel.is_empty() {
                    String::new()
                } else {
                    format!("{}/", dir_rel)
                };
                let mut entries: BTreeMap<String, super::RemoteDirEntry> = BTreeMap::new();
                for ((obj_bucket, obj_key), data) in objects.iter() {
                    if obj_bucket != &bucket {
                        continue;
                    }
                    let Some(rest) = obj_key.strip_prefix(&dir_prefix) else {
                        continue;
                    };
                    if rest.is_empty() {
                        continue;
                    }
                    if let Some((child, _tail)) = rest.split_once('/') {
                        entries
                            .entry(child.to_string())
                            .or_insert_with(|| super::RemoteDirEntry {
                                name: child.to_string(),
                                is_file: false,
                                is_dir: true,
                                size: 0,
                                mtime_ns: 0,
                            });
                    } else {
                        entries.insert(
                            rest.to_string(),
                            super::RemoteDirEntry {
                                name: rest.to_string(),
                                is_file: true,
                                is_dir: false,
                                size: data.len() as i64,
                                mtime_ns: 1,
                            },
                        );
                    }
                }
                for (dir_bucket, dir_key) in directories.iter() {
                    if dir_bucket != &bucket {
                        continue;
                    }
                    let Some(rest) = dir_key.strip_prefix(&dir_prefix) else {
                        continue;
                    };
                    if rest.is_empty() {
                        continue;
                    }
                    if let Some((child, _tail)) = rest.split_once('/') {
                        entries
                            .entry(child.to_string())
                            .or_insert_with(|| super::RemoteDirEntry {
                                name: child.to_string(),
                                is_file: false,
                                is_dir: true,
                                size: 0,
                                mtime_ns: 0,
                            });
                    } else {
                        entries
                            .entry(rest.to_string())
                            .or_insert_with(|| super::RemoteDirEntry {
                                name: rest.to_string(),
                                is_file: false,
                                is_dir: true,
                                size: 0,
                                mtime_ns: 0,
                            });
                    }
                }
                Ok(entries.into_values().collect())
            })
        }

        fn read_chunk_cached(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            export_name: Arc<str>,
            relpath: Arc<str>,
            offset: i64,
            length: i64,
            _file_size: i64,
            _mtime_ns: i64,
        ) -> futures::future::BoxFuture<'static, Result<Vec<u8>, S3Error>> {
            let this = self.clone();
            let bucket = export_name.to_string();
            let key = relpath.to_string();
            Box::pin(async move {
                let objects = this.objects.lock();
                let data = objects
                    .get(&(bucket, key))
                    .ok_or_else(|| S3Error::Internal {
                        detail: "missing object in test backend".to_string(),
                    })?;
                let start: usize = offset.try_into().map_err(|_| S3Error::Internal {
                    detail: format!("offset overflow: {}", offset),
                })?;
                let len: usize = length.try_into().map_err(|_| S3Error::Internal {
                    detail: format!("length overflow: {}", length),
                })?;
                let end = start.checked_add(len).ok_or_else(|| S3Error::Internal {
                    detail: "range overflow".to_string(),
                })?;
                Ok(data[start..end].to_vec())
            })
        }

        fn write_chunk(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            export_name: Arc<str>,
            relpath: Arc<str>,
            offset: i64,
            data: Vec<u8>,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            let this = self.clone();
            let bucket = export_name.to_string();
            let key = relpath.to_string();
            Box::pin(async move {
                let offset_usize: usize = offset.try_into().map_err(|_| S3Error::Internal {
                    detail: format!("offset overflow: {}", offset),
                })?;
                let mut objects = this.objects.lock();
                let entry = objects.entry((bucket, key)).or_default();
                let needed =
                    offset_usize
                        .checked_add(data.len())
                        .ok_or_else(|| S3Error::Internal {
                            detail: "write overflow".to_string(),
                        })?;
                if entry.len() < needed {
                    entry.resize(needed, 0);
                }
                entry[offset_usize..needed].copy_from_slice(&data);
                Ok(())
            })
        }

        fn truncate(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            export_name: Arc<str>,
            relpath: Arc<str>,
            size: i64,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            let this = self.clone();
            let bucket = export_name.to_string();
            let key = relpath.to_string();
            Box::pin(async move {
                let size_usize: usize = size.try_into().map_err(|_| S3Error::Internal {
                    detail: format!("size overflow: {}", size),
                })?;
                let mut objects = this.objects.lock();
                let entry = objects.entry((bucket, key)).or_default();
                entry.resize(size_usize, 0);
                Ok(())
            })
        }

        fn mkdir(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            export_name: Arc<str>,
            relpath: Arc<str>,
            _mode: i64,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            let this = self.clone();
            let bucket = export_name.to_string();
            let key = relpath.to_string();
            Box::pin(async move {
                this.insert_dir(bucket.as_str(), key.as_str());
                Ok(())
            })
        }

        fn rename(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            export_name: Arc<str>,
            src_relpath: Arc<str>,
            dst_relpath: Arc<str>,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            let this = self.clone();
            let bucket = export_name.to_string();
            let src_key = src_relpath.to_string();
            let dst_key = dst_relpath.to_string();
            Box::pin(async move {
                this.rename_count.fetch_add(1, Ordering::SeqCst);
                let mut objects = this.objects.lock();
                let data = objects.remove(&(bucket.clone(), src_key)).ok_or_else(|| {
                    S3Error::Internal {
                        detail: "missing source object in test backend".to_string(),
                    }
                })?;
                objects.insert((bucket, dst_key), data);
                Ok(())
            })
        }

        fn unlink(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            export_name: Arc<str>,
            relpath: Arc<str>,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            let this = self.clone();
            let bucket = export_name.to_string();
            let key = relpath.to_string();
            Box::pin(async move {
                this.objects.lock().remove(&(bucket, key));
                Ok(())
            })
        }

        fn rmdir(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            export_name: Arc<str>,
            relpath: Arc<str>,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            let this = self.clone();
            let bucket = export_name.to_string();
            let key = relpath.to_string();
            Box::pin(async move {
                this.directories.lock().remove(&(bucket, key));
                Ok(())
            })
        }
    }

    impl FsS3Backend for SlowObjectBackend {
        fn stat(
            &self,
            request_identity: FluxonFsRequestIdentity,
            export_name: Arc<str>,
            relpath: Arc<str>,
        ) -> futures::future::BoxFuture<'static, Result<super::RemoteStat, S3Error>> {
            self.inner.stat(request_identity, export_name, relpath)
        }

        fn list_dir(
            &self,
            request_identity: FluxonFsRequestIdentity,
            export_name: Arc<str>,
            relpath: Arc<str>,
        ) -> futures::future::BoxFuture<'static, Result<Vec<super::RemoteDirEntry>, S3Error>>
        {
            self.inner.list_dir(request_identity, export_name, relpath)
        }

        fn read_chunk_cached(
            &self,
            request_identity: FluxonFsRequestIdentity,
            export_name: Arc<str>,
            relpath: Arc<str>,
            offset: i64,
            length: i64,
            file_size: i64,
            mtime_ns: i64,
        ) -> futures::future::BoxFuture<'static, Result<Vec<u8>, S3Error>> {
            let inner = self.inner.clone();
            let delay = self.delay;
            Box::pin(async move {
                tokio::time::sleep(delay).await;
                inner
                    .read_chunk_cached(
                        request_identity,
                        export_name,
                        relpath,
                        offset,
                        length,
                        file_size,
                        mtime_ns,
                    )
                    .await
            })
        }

        fn write_chunk(
            &self,
            request_identity: FluxonFsRequestIdentity,
            export_name: Arc<str>,
            relpath: Arc<str>,
            offset: i64,
            data: Vec<u8>,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            let inner = self.inner.clone();
            let delay = self.delay;
            Box::pin(async move {
                tokio::time::sleep(delay).await;
                inner
                    .write_chunk(request_identity, export_name, relpath, offset, data)
                    .await
            })
        }

        fn truncate(
            &self,
            request_identity: FluxonFsRequestIdentity,
            export_name: Arc<str>,
            relpath: Arc<str>,
            size: i64,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            self.inner
                .truncate(request_identity, export_name, relpath, size)
        }

        fn mkdir(
            &self,
            request_identity: FluxonFsRequestIdentity,
            export_name: Arc<str>,
            relpath: Arc<str>,
            mode: i64,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            self.inner
                .mkdir(request_identity, export_name, relpath, mode)
        }

        fn rename(
            &self,
            request_identity: FluxonFsRequestIdentity,
            export_name: Arc<str>,
            src_relpath: Arc<str>,
            dst_relpath: Arc<str>,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            self.inner
                .rename(request_identity, export_name, src_relpath, dst_relpath)
        }

        fn unlink(
            &self,
            request_identity: FluxonFsRequestIdentity,
            export_name: Arc<str>,
            relpath: Arc<str>,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            self.inner.unlink(request_identity, export_name, relpath)
        }

        fn rmdir(
            &self,
            request_identity: FluxonFsRequestIdentity,
            export_name: Arc<str>,
            relpath: Arc<str>,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            self.inner.rmdir(request_identity, export_name, relpath)
        }
    }

    #[derive(Clone, Default)]
    struct ListDirAccessDeniedBackend;

    impl FsS3Backend for ListDirAccessDeniedBackend {
        fn stat(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            _export_name: Arc<str>,
            _relpath: Arc<str>,
        ) -> futures::future::BoxFuture<'static, Result<super::RemoteStat, S3Error>> {
            Box::pin(async move {
                Ok(super::RemoteStat {
                    exists: true,
                    is_file: false,
                    is_dir: true,
                    size: 0,
                    mtime_ns: 0,
                })
            })
        }

        fn list_dir(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            _export_name: Arc<str>,
            _relpath: Arc<str>,
        ) -> futures::future::BoxFuture<'static, Result<Vec<super::RemoteDirEntry>, S3Error>>
        {
            Box::pin(async move {
                Err(S3Error::AccessDenied {
                    detail: "remote fs permission denied: export=demo relpath=. (scope_access does not allow this operation)"
                        .to_string(),
                })
            })
        }

        fn read_chunk_cached(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            _export_name: Arc<str>,
            _relpath: Arc<str>,
            _offset: i64,
            _length: i64,
            _file_size: i64,
            _mtime_ns: i64,
        ) -> futures::future::BoxFuture<'static, Result<Vec<u8>, S3Error>> {
            Box::pin(async move {
                Err(S3Error::Internal {
                    detail: "unused in test".to_string(),
                })
            })
        }

        fn write_chunk(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            _export_name: Arc<str>,
            _relpath: Arc<str>,
            _offset: i64,
            _data: Vec<u8>,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            Box::pin(async move {
                Err(S3Error::Internal {
                    detail: "unused in test".to_string(),
                })
            })
        }

        fn truncate(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            _export_name: Arc<str>,
            _relpath: Arc<str>,
            _size: i64,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            Box::pin(async move {
                Err(S3Error::Internal {
                    detail: "unused in test".to_string(),
                })
            })
        }

        fn mkdir(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            _export_name: Arc<str>,
            _relpath: Arc<str>,
            _mode: i64,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            Box::pin(async move {
                Err(S3Error::Internal {
                    detail: "unused in test".to_string(),
                })
            })
        }

        fn rename(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            _export_name: Arc<str>,
            _src_relpath: Arc<str>,
            _dst_relpath: Arc<str>,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            Box::pin(async move {
                Err(S3Error::Internal {
                    detail: "unused in test".to_string(),
                })
            })
        }

        fn unlink(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            _export_name: Arc<str>,
            _relpath: Arc<str>,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            Box::pin(async move {
                Err(S3Error::Internal {
                    detail: "unused in test".to_string(),
                })
            })
        }

        fn rmdir(
            &self,
            _request_identity: FluxonFsRequestIdentity,
            _export_name: Arc<str>,
            _relpath: Arc<str>,
        ) -> futures::future::BoxFuture<'static, Result<(), S3Error>> {
            Box::pin(async move {
                Err(S3Error::Internal {
                    detail: "unused in test".to_string(),
                })
            })
        }
    }

    fn test_request_identity() -> FluxonFsRequestIdentity {
        FluxonFsRequestIdentity {
            username: "test_user".to_string(),
            password: "test_password".to_string(),
        }
    }

    fn test_export(name: &str) -> FluxonFsExport {
        FluxonFsExport {
            remote_root_dir_abs: format!("/tmp/{}", name),
            routing_mode: FluxonFsExportRoutingMode::StaticNodes,
            nodes: vec!["n1".to_string()],
            cache_kv_key_prefix: format!("/{}/", name),
            cache_bytes_field_key: format!("{}_bytes", name),
            cache_max_bytes: 1024,
            rpc_paths: export_rpc_paths_for_export_name_v1(name),
        }
    }

    fn test_agent_member(member_id: &str) -> super::FsMasterMemberRecord {
        super::FsMasterMemberRecord {
            kind: super::FsMasterMemberKind::Agent,
            member_id: member_id.to_string(),
            owner_id: "owner".to_string(),
            hostname: "host".to_string(),
            addresses: vec!["127.0.0.1".to_string()],
            port: Some(3000),
            pid: "123".to_string(),
            cmd: "fluxon_fs_agent".to_string(),
        }
    }

    fn test_access_db_path() -> String {
        let root = test_tikv_work_root().join("access_db");
        fs::create_dir_all(&root).unwrap();
        cleanup_stale_test_access_db_files(root.as_path(), "fluxon_fs_s3_gateway_access_test");
        root.join(format!(
            "fluxon_fs_s3_gateway_access_test_pid{}_{}.db",
            process::id(),
            Uuid::new_v4()
        ))
        .to_string_lossy()
        .into_owned()
    }

    #[test]
    fn test_open_access_db_creates_parent_dir() {
        let root = test_tikv_work_root().join(format!(
            "fluxon_fs_s3_gateway_access_parent_pid{}_{}",
            process::id(),
            Uuid::new_v4()
        ));
        let db_path = root.join("nested").join("access.db");
        let db_path_str = db_path.to_string_lossy().into_owned();

        let conn =
            open_access_db(&db_path_str).expect("open_access_db should create missing parent dir");
        drop(conn);

        assert!(
            db_path.is_file(),
            "access db should exist after open: {}",
            db_path.display()
        );
        std::fs::remove_file(&db_path).unwrap();
        std::fs::remove_dir_all(&root).unwrap();
    }

    fn test_gateway_access_config() -> GatewayAccessConfig {
        shared_test_tikv_access_config()
    }

    fn build_test_tikv_gateway_access_config(
        access_db_path: String,
        pd_endpoints: Vec<String>,
        key_prefix: String,
    ) -> GatewayAccessConfig {
        GatewayAccessConfig {
            access_db_path,
            bootstrap_access_model: test_bootstrap_access_model(),
            transfer_state_store: Some(FluxonFsTransferStateStoreConfig {
                kind: FluxonFsTransferStateStoreKind::TiKv(FluxonFsTransferStateStoreTiKvConfig {
                    pd_endpoints,
                    key_prefix,
                }),
            }),
        }
    }

    fn build_test_gateway_access_config_without_transfer(
        access_db_path: String,
    ) -> GatewayAccessConfig {
        GatewayAccessConfig {
            access_db_path,
            bootstrap_access_model: test_bootstrap_access_model(),
            transfer_state_store: None,
        }
    }

    fn test_bootstrap_access_model() -> FluxonFsAccessModel {
        FluxonFsAccessModel {
            users: vec![FluxonFsAccessUser {
                username: "admin".to_string(),
                password: "admin_pw".to_string(),
                can_manage_users: true,
            }],
            scope_access: Vec::new(),
        }
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
        repo_root()
            .join("fluxon_release")
            .join("ext_images")
            .join("tikv")
    }

    fn require_tikv_runtime_path(relname: &str) -> PathBuf {
        let path = tikv_ext_dir().join(relname);
        assert!(
            path.is_file(),
            "missing TiKV runtime file: {}. Run `python3 setup_and_pack/pack_release_ext.py --release-dir fluxon_release` first.",
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

    fn test_tikv_work_root() -> PathBuf {
        let root = PathBuf::from(TEST_TIKV_WORK_ROOT);
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn extract_test_dir_pid(dir_name: &str, prefix: &str) -> Option<u32> {
        let suffix = dir_name.strip_prefix(prefix)?.strip_prefix("_pid")?;
        let (pid_text, _) = suffix.split_once('_')?;
        pid_text.parse::<u32>().ok()
    }

    fn process_is_alive(pid: u32) -> bool {
        Path::new("/proc").join(pid.to_string()).exists()
    }

    fn cleanup_stale_test_temp_dirs(root: &Path, prefix: &str) {
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

    fn cleanup_stale_test_access_db_files(root: &Path, prefix: &str) {
        let rd = fs::read_dir(root).unwrap();
        for ent in rd {
            let ent = ent.unwrap();
            if !ent.file_type().unwrap().is_file() {
                continue;
            }
            let file_name = ent.file_name().to_string_lossy().to_string();
            let Some(pid) = extract_test_dir_pid(file_name.trim_end_matches(".db"), prefix) else {
                continue;
            };
            if process_is_alive(pid) {
                continue;
            }
            match fs::remove_file(ent.path()) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => panic!(
                    "failed to remove stale access db path={} err={}",
                    ent.path().display(),
                    err
                ),
            }
        }
    }

    struct TestTempDir {
        path: PathBuf,
    }

    impl TestTempDir {
        fn new(prefix: &str) -> Self {
            let root = test_tikv_work_root();
            cleanup_stale_test_temp_dirs(root.as_path(), prefix);
            let path = root.join(format!(
                "{}_pid{}_{}",
                prefix,
                process::id(),
                Uuid::new_v4()
            ));
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn path(&self) -> &Path {
            self.path.as_path()
        }
    }

    impl Drop for TestTempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
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
        access_config: GatewayAccessConfig,
        pd_log_path: &Path,
        tikv_log_path: &Path,
    ) {
        let deadline = Instant::now() + Duration::from_secs(TEST_TIKV_READY_TIMEOUT_SECS);
        loop {
            let last_err = match GatewayState::new(
                "test_cluster".to_string(),
                "".to_string(),
                access_config.clone(),
                Arc::new(FluxonFsGlobalConfig {
                    stale_window_ms: 0,
                    rules: Vec::new(),
                    exports: BTreeMap::new(),
                }),
                FluxonFsS3GatewayConfig {
                    get_object_inflight_pieces: 4,
                    kv_miss_policy: FluxonFsS3KvMissPolicy::RemoteRead,
                },
                Arc::new(ObjectBackend::default()),
                Arc::new(TestFsMasterAdminBackend),
                None,
                None,
            ) {
                Ok(state) => match state.list_running_transfer_jobs() {
                    Ok(_) => return,
                    Err(e) => e,
                },
                Err(e) => e,
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
        _runtime_dir: TestTempDir,
        pd_proc: Child,
        tikv_proc: Child,
        _pd_log_path: PathBuf,
        _tikv_log_path: PathBuf,
        pd_endpoint: String,
        access_config: GatewayAccessConfig,
    }

    impl LocalTiKvHarness {
        fn new() -> Self {
            let runtime_dir = TestTempDir::new("fluxon_fs_gateway_tikv");
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
                format!("lease = {}\n", TEST_TIKV_PD_LEASE_SECS),
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
max-thread-count = {TEST_TIKV_UNIFIED_READPOOL_MAX_THREADS}\n\
\n\
[readpool.storage]\n\
high-concurrency = {TEST_TIKV_STORAGE_READPOOL_CONCURRENCY}\n\
normal-concurrency = {TEST_TIKV_STORAGE_READPOOL_CONCURRENCY}\n\
low-concurrency = {TEST_TIKV_STORAGE_READPOOL_CONCURRENCY}\n\
\n\
[readpool.coprocessor]\n\
high-concurrency = {TEST_TIKV_COPROCESSOR_READPOOL_CONCURRENCY}\n\
normal-concurrency = {TEST_TIKV_COPROCESSOR_READPOOL_CONCURRENCY}\n\
low-concurrency = {TEST_TIKV_COPROCESSOR_READPOOL_CONCURRENCY}\n\
\n\
[server]\n\
end-point-max-concurrency = {TEST_TIKV_ENDPOINT_MAX_CONCURRENCY}\n\
background-thread-count = {TEST_TIKV_BACKGROUND_THREAD_COUNT}\n\
\n\
[storage]\n\
scheduler-concurrency = {TEST_TIKV_SCHEDULER_CONCURRENCY}\n\
scheduler-worker-pool-size = {TEST_TIKV_SCHEDULER_WORKER_POOL_SIZE}\n\
\n\
[raftstore]\n\
apply-pool-size = {TEST_TIKV_APPLY_POOL_SIZE}\n\
store-pool-size = {TEST_TIKV_STORE_POOL_SIZE}\n\
\n\
[rocksdb]\n\
max-background-jobs = {TEST_TIKV_ROCKSDB_MAX_BACKGROUND_JOBS}\n\
\n\
[raftdb]\n\
max-background-jobs = {TEST_TIKV_RAFTDB_MAX_BACKGROUND_JOBS}\n"
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

            let access_config = build_test_tikv_gateway_access_config(
                runtime_dir
                    .path()
                    .join("access.db")
                    .to_str()
                    .unwrap()
                    .to_string(),
                vec![pd_endpoint.clone()],
                format!("/fluxon_fs_transfer_gateway_test/{}/", Uuid::new_v4()),
            );
            wait_for_tikv_transfer_state_store_ready(
                access_config.clone(),
                &pd_log_path,
                &tikv_log_path,
            );

            Self {
                _runtime_dir: runtime_dir,
                pd_proc,
                tikv_proc,
                _pd_log_path: pd_log_path,
                _tikv_log_path: tikv_log_path,
                pd_endpoint,
                access_config,
            }
        }
    }

    impl Drop for LocalTiKvHarness {
        fn drop(&mut self) {
            terminate_child(&mut self.tikv_proc);
            terminate_child(&mut self.pd_proc);
        }
    }

    fn shared_test_tikv_access_config() -> GatewayAccessConfig {
        static SHARED_TIKV: OnceLock<parking_lot::Mutex<LocalTiKvHarness>> = OnceLock::new();
        let pd_endpoint = {
            let harness = SHARED_TIKV
                .get_or_init(|| parking_lot::Mutex::new(LocalTiKvHarness::new()))
                .lock();
            harness.pd_endpoint.clone()
        };
        build_test_tikv_gateway_access_config(
            test_access_db_path(),
            vec![pd_endpoint],
            format!(
                "/fluxon_fs_transfer_gateway_test/shared/{}/",
                Uuid::new_v4()
            ),
        )
    }

    #[test]
    fn test_auth_original_path_and_query_rebuilds_direct_nested_service_root() {
        let headers = HeaderMap::new();
        let uri: Uri = "/".parse().unwrap();
        let (path, query) =
            super::auth_original_path_and_query("/fs_s3", "test_cluster", &uri, &headers, "")
                .unwrap();
        assert_eq!(path, "/fs_s3/");
        assert_eq!(query, None);
    }

    #[test]
    fn test_build_fs_master_admin_managed_agent_exports_uses_runtime_and_overlay_per_agent() {
        let online_agent_ids = BTreeSet::from([
            "fluxon_fs_agent_infra44-ThinkStation-PX".to_string(),
            "fluxon_fs_agent_infra46-ThinkStation-PX".to_string(),
        ]);
        let runtime_agent_exports = vec![
            super::FsMasterAdminRuntimeAgentExports {
                agent_instance_key: "fluxon_fs_agent_infra44-ThinkStation-PX".to_string(),
                runtime_exports: vec![
                    super::FsMasterAdminRuntimeExportRecord {
                        export_name: "deployer-runtime-infra44".to_string(),
                        remote_root_dir_abs: "/mnt/nvme0/store_team_dev/fluxon_deploy".to_string(),
                    },
                    super::FsMasterAdminRuntimeExportRecord {
                        export_name: "fluxon-release".to_string(),
                        remote_root_dir_abs:
                            "/mnt/nvme0/store_team_dev/fluxon_deploy/fluxon_release".to_string(),
                    },
                ],
            },
            super::FsMasterAdminRuntimeAgentExports {
                agent_instance_key: "fluxon_fs_agent_infra46-ThinkStation-PX".to_string(),
                runtime_exports: vec![super::FsMasterAdminRuntimeExportRecord {
                    export_name: "deployer-runtime-infra46".to_string(),
                    remote_root_dir_abs: "/mnt/nvme0/store_team_dev/fluxon_deploy".to_string(),
                }],
            },
        ];
        let managed_agent_exports = super::build_fs_master_admin_managed_agent_exports(
            &online_agent_ids,
            &runtime_agent_exports,
            &[],
            &[],
        );

        assert_eq!(managed_agent_exports.len(), 2);
        assert_eq!(
            managed_agent_exports[0]
                .managed_exports
                .iter()
                .map(|record| record.export_name.as_str())
                .collect::<Vec<_>>(),
            vec!["deployer-runtime-infra44", "fluxon-release"]
        );
        assert_eq!(
            managed_agent_exports[1]
                .managed_exports
                .iter()
                .map(|record| record.export_name.as_str())
                .collect::<Vec<_>>(),
            vec!["deployer-runtime-infra46"]
        );
    }

    #[tokio::test]
    async fn test_snapshot_fs_master_admin_filters_offline_agents() {
        let fs_master_admin_backend = StaticFsMasterAdminBackend {
            members: vec![
                test_agent_member("fluxon_fs_agent_infra44-ThinkStation-PX"),
                test_agent_member("fluxon_fs_agent_infra46-ThinkStation-PX"),
            ],
            online_member_ids: BTreeSet::from([
                "fluxon_fs_agent_infra44-ThinkStation-PX".to_string()
            ]),
        };
        let st = GatewayState::new(
            "test_cluster".to_string(),
            "".to_string(),
            test_gateway_access_config(),
            Arc::new(FluxonFsGlobalConfig {
                stale_window_ms: 0,
                rules: Vec::new(),
                exports: BTreeMap::new(),
            }),
            FluxonFsS3GatewayConfig {
                get_object_inflight_pieces: 4,
                kv_miss_policy: FluxonFsS3KvMissPolicy::RemoteRead,
            },
            Arc::new(ObjectBackend::default()),
            Arc::new(fs_master_admin_backend),
            None,
            None,
        )
        .unwrap();
        st.replace_fs_export_registry_for_agent(
            "fluxon_fs_agent_infra44-ThinkStation-PX",
            &[super::FsExportRegistryRecord {
                export_name: "deployer-runtime-infra44".to_string(),
                agent_instance_key: "fluxon_fs_agent_infra44-ThinkStation-PX".to_string(),
                remote_root_dir_abs: "/mnt/nvme0/store_team_dev/fluxon_deploy".to_string(),
                export: super::agent_registry_export_for_name_and_root_v1(
                    "deployer-runtime-infra44",
                    "/mnt/nvme0/store_team_dev/fluxon_deploy",
                ),
                updated_unix_ms: 1,
            }],
        )
        .unwrap();
        st.replace_fs_export_registry_for_agent(
            "fluxon_fs_agent_infra46-ThinkStation-PX",
            &[super::FsExportRegistryRecord {
                export_name: "deployer-runtime-infra46".to_string(),
                agent_instance_key: "fluxon_fs_agent_infra46-ThinkStation-PX".to_string(),
                remote_root_dir_abs: "/mnt/nvme0/store_team_dev/fluxon_deploy".to_string(),
                export: super::agent_registry_export_for_name_and_root_v1(
                    "deployer-runtime-infra46",
                    "/mnt/nvme0/store_team_dev/fluxon_deploy",
                ),
                updated_unix_ms: 1,
            }],
        )
        .unwrap();

        let snapshot = st.snapshot_fs_master_admin().await.unwrap();

        assert_eq!(snapshot.runtime_agent_exports.len(), 1);
        assert_eq!(
            snapshot.runtime_agent_exports[0].agent_instance_key,
            "fluxon_fs_agent_infra44-ThinkStation-PX"
        );
        assert_eq!(snapshot.managed_agent_exports.len(), 1);
        assert_eq!(
            snapshot.managed_agent_exports[0].agent_instance_key,
            "fluxon_fs_agent_infra44-ThinkStation-PX"
        );
    }

    #[test]
    fn test_auth_original_path_and_query_rebuilds_direct_nested_bucket_path() {
        let headers = HeaderMap::new();
        let uri: Uri = "/fluxon-release?delimiter=%2F&prefix=".parse().unwrap();
        let (path, query) = super::auth_original_path_and_query(
            "/fs_s3",
            "test_cluster",
            &uri,
            &headers,
            "fluxon-release",
        )
        .unwrap();
        assert_eq!(path, "/fs_s3/fluxon-release");
        assert_eq!(query.as_deref(), Some("delimiter=%2F&prefix="));
    }

    #[test]
    fn test_auth_original_path_and_query_prefers_proxy_original_uri_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            super::HDR_ORIGINAL_URI,
            HeaderValue::from_static("/r/fs_s3/test_cluster/fluxon-release?list-type=2"),
        );
        let uri: Uri = "/fluxon-release?list-type=2".parse().unwrap();
        let (path, query) = super::auth_original_path_and_query(
            "/fs_s3",
            "test_cluster",
            &uri,
            &headers,
            "fluxon-release",
        )
        .unwrap();
        assert_eq!(path, "/r/fs_s3/test_cluster/fluxon-release");
        assert_eq!(query.as_deref(), Some("list-type=2"));
    }

    fn test_state_with_buckets_and_access_config_and_base_path(
        backend: Arc<dyn FsS3Backend>,
        buckets: &[&str],
        access_config: GatewayAccessConfig,
        external_base_path: &str,
    ) -> Arc<GatewayState> {
        let mut exports = BTreeMap::new();
        for bucket in buckets {
            exports.insert((*bucket).to_string(), test_export(bucket));
        }
        let fs_cache = FluxonFsGlobalConfig {
            stale_window_ms: 0,
            rules: Vec::new(),
            exports,
        };
        let account = FluxonFsS3PermissionAccount {
            username: "a".to_string(),
            password: "b".to_string(),
            permissions: vec![fluxon_fs_core::config::FluxonFsS3PermissionRule {
                bucket: "*".to_string(),
                prefix: "".to_string(),
                actions: vec![FluxonFsS3PermissionAction::All],
            }],
        };
        let st = GatewayState::new(
            "test_cluster".to_string(),
            external_base_path.to_string(),
            access_config,
            Arc::new(fs_cache),
            FluxonFsS3GatewayConfig {
                get_object_inflight_pieces: 4,
                kv_miss_policy: FluxonFsS3KvMissPolicy::RemoteRead,
            },
            backend,
            Arc::new(TestFsMasterAdminBackend),
            None,
            None,
        )
        .unwrap();
        st.persist_permission_list_state(&[account]).unwrap();
        Arc::new(st)
    }

    fn test_state_with_buckets_and_base_path(
        backend: Arc<dyn FsS3Backend>,
        buckets: &[&str],
        external_base_path: &str,
    ) -> Arc<GatewayState> {
        test_state_with_buckets_and_access_config_and_base_path(
            backend,
            buckets,
            test_gateway_access_config(),
            external_base_path,
        )
    }

    fn test_state_with_buckets(
        backend: Arc<dyn FsS3Backend>,
        buckets: &[&str],
    ) -> Arc<GatewayState> {
        test_state_with_buckets_and_base_path(backend, buckets, "")
    }

    fn test_state_with_buckets_without_transfer(
        backend: Arc<dyn FsS3Backend>,
        buckets: &[&str],
    ) -> Arc<GatewayState> {
        test_state_with_buckets_and_access_config_and_base_path(
            backend,
            buckets,
            build_test_gateway_access_config_without_transfer(test_access_db_path()),
            "",
        )
    }

    fn test_state_with_permission_list(
        backend: Arc<dyn FsS3Backend>,
        buckets: &[&str],
        permission_list: Vec<FluxonFsS3PermissionAccount>,
    ) -> Arc<GatewayState> {
        let mut exports = BTreeMap::new();
        for bucket in buckets {
            exports.insert((*bucket).to_string(), test_export(bucket));
        }
        let fs_cache = FluxonFsGlobalConfig {
            stale_window_ms: 0,
            rules: Vec::new(),
            exports,
        };
        let st = GatewayState::new(
            "test_cluster".to_string(),
            "".to_string(),
            test_gateway_access_config(),
            Arc::new(fs_cache),
            FluxonFsS3GatewayConfig {
                get_object_inflight_pieces: 4,
                kv_miss_policy: FluxonFsS3KvMissPolicy::RemoteRead,
            },
            backend,
            Arc::new(TestFsMasterAdminBackend),
            None,
            None,
        )
        .unwrap();
        st.persist_permission_list_state(&permission_list).unwrap();
        Arc::new(st)
    }

    fn test_transfer_state() -> GatewayState {
        test_transfer_state_with_backend_and_access_config(
            Arc::new(ObjectBackend::default()),
            test_gateway_access_config(),
        )
    }

    fn test_transfer_state_with_access_config(access_config: GatewayAccessConfig) -> GatewayState {
        test_transfer_state_with_backend_and_access_config(
            Arc::new(ObjectBackend::default()),
            access_config,
        )
    }

    fn test_transfer_state_with_backend_and_access_config(
        backend: Arc<dyn FsS3Backend>,
        access_config: GatewayAccessConfig,
    ) -> GatewayState {
        GatewayState::new(
            "test_cluster".to_string(),
            "".to_string(),
            access_config,
            Arc::new(FluxonFsGlobalConfig {
                stale_window_ms: 0,
                rules: Vec::new(),
                exports: BTreeMap::new(),
            }),
            FluxonFsS3GatewayConfig {
                get_object_inflight_pieces: 4,
                kv_miss_policy: FluxonFsS3KvMissPolicy::RemoteRead,
            },
            backend,
            Arc::new(TestFsMasterAdminBackend),
            None,
            None,
        )
        .unwrap()
    }

    #[test]
    fn test_transfer_job_create_rejected_when_transfer_feature_disabled() {
        let st = test_transfer_state_with_access_config(
            build_test_gateway_access_config_without_transfer(test_access_db_path()),
        );
        let err = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 1,
                batch_ready_bytes: 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap_err();
        assert_eq!(
            err,
            "transfer feature is disabled because transfer_state_store is not configured"
        );
    }

    fn create_single_batch_transfer_job(
        st: &GatewayState,
        dst_export: &str,
        entries: &[(&str, i64)],
        collect_infos: Vec<FluxonFsTransferBatchCollectInfoWire>,
    ) -> super::FsTransferJobRecord {
        create_single_batch_transfer_job_with_manifest_blob(
            st,
            dst_export,
            "",
            FluxonFsTransferBatchKind::DirectFilesOnly,
            test_manifest_blob(entries),
            collect_infos,
        )
    }

    fn create_single_batch_transfer_job_with_manifest_blob(
        st: &GatewayState,
        dst_export: &str,
        root_relpath: &str,
        batch_kind: FluxonFsTransferBatchKind,
        manifest_blob: Vec<u8>,
        collect_infos: Vec<FluxonFsTransferBatchCollectInfoWire>,
    ) -> super::FsTransferJobRecord {
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: dst_export.to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 1,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let scan_epoch = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        let batch_wire = FluxonFsTransferScanBatchWire {
            batch_id: "batch-1".to_string(),
            root_relpath: root_relpath.to_string(),
            batch_kind,
            manifest_blob,
            collect_infos,
            generation: 1,
        };
        let (direct_files_only_batches, full_dir_batches) =
            if batch_kind == FluxonFsTransferBatchKind::DirectFilesOnly {
                (vec![batch_wire], Vec::new())
            } else {
                (Vec::new(), vec![batch_wire])
            };
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id: format!("{}__scan_epoch_{}", job.job_id, scan_epoch),
            scan_task_id: "scan-task-1".to_string(),
            root_relpath: "".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches,
            child_scan_units: Vec::new(),
            full_dir_batches,
            finished: true,
        })
        .unwrap();
        st.finish_transfer_scan_epoch(job.job_id.as_str(), scan_epoch)
            .unwrap();
        job
    }

    fn test_manifest_blob(entries: &[(&str, i64)]) -> Vec<u8> {
        encode_transfer_manifest_blob(
            entries
                .iter()
                .map(|(relpath, size)| FluxonFsTransferManifestEntryWire {
                    relpath: (*relpath).to_string(),
                    size: *size,
                })
                .collect(),
        )
        .unwrap()
    }

    fn test_local_prescan_job_spec_blob(src_root_dir_abs: &str, batch_ready_bytes: i64) -> Vec<u8> {
        serde_json::to_vec(&FluxonFsLocalTransferCheckJobSpecWire {
            src_root_dir_abs: src_root_dir_abs.to_string(),
            batch_ready_bytes,
            skip_entries: Vec::new(),
        })
        .unwrap()
    }

    fn test_manifest_blob_with_empty_dirs(
        entries: &[(&str, i64)],
        empty_dir_relpaths: &[&str],
    ) -> Vec<u8> {
        encode_transfer_manifest_blob_with_empty_dirs(
            entries
                .iter()
                .map(|(relpath, size)| FluxonFsTransferManifestEntryWire {
                    relpath: (*relpath).to_string(),
                    size: *size,
                })
                .collect(),
            empty_dir_relpaths
                .iter()
                .map(|relpath| (*relpath).to_string())
                .collect(),
        )
        .unwrap()
    }

    fn test_symlink_collect_infos(
        entries: &[(&str, &str)],
    ) -> Vec<FluxonFsTransferBatchCollectInfoWire> {
        let mut blob = Vec::new();
        for (relpath, link_target) in entries {
            let line = serde_json::to_string(&FluxonFsTransferSymlinkNoticeEntryWire {
                relpath: (*relpath).to_string(),
                link_target: (*link_target).to_string(),
            })
            .unwrap();
            blob.extend_from_slice(line.as_bytes());
            blob.push(b'\n');
        }
        vec![FluxonFsTransferBatchCollectInfoWire {
            collect_kind: FluxonFsTransferCollectInfoKind::SymlinkNotice,
            collect_blob: blob,
        }]
    }

    fn test_transfer_worker_id(job_id: &str, batch_id: &str) -> String {
        format!("{}__batch_{}", job_id, batch_id)
    }

    #[test]
    fn test_transfer_job_snapshot_counts_materialized_batches_even_when_workers_zero() {
        let st = test_transfer_state();
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 0,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let scan_epoch = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        let result = FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id: format!("{}__scan_epoch_{}", job.job_id, scan_epoch),
            scan_task_id: "scan-task-1".to_string(),
            root_relpath: "".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-1".to_string(),
                root_relpath: "".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                manifest_blob: test_manifest_blob(&[("a.bin", 123)]),
                collect_infos: Vec::new(),
                generation: 1,
            }],
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        };
        st.apply_transfer_scan_result(&result).unwrap();
        st.finish_transfer_scan_epoch(job.job_id.as_str(), scan_epoch)
            .unwrap();

        let snapshot = st
            .transfer_job_snapshot(job.job_id.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.job.desired_worker_count, 0);
        assert_eq!(snapshot.open_batches, 1);
        assert_eq!(snapshot.scan_epoch, scan_epoch);
        assert!(snapshot.scan_finished);
    }

    #[test]
    fn test_transfer_job_snapshot_live_scan_discovered_stats_match_durable_batches() {
        let st = test_transfer_state();
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 0,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let scan_epoch = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        st.note_transfer_scan_result_accepted(job.job_id.as_str(), 10, 99, 999, 9999);
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id: format!("{}__scan_epoch_{}__root", job.job_id, scan_epoch),
            scan_task_id: "scan-task-root".to_string(),
            root_relpath: "root".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-1".to_string(),
                root_relpath: "root".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                manifest_blob: test_manifest_blob(&[("root/a.bin", 3), ("root/b.bin", 5)]),
                collect_infos: Vec::new(),
                generation: 1,
            }],
            child_scan_units: Vec::new(),
            full_dir_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-2".to_string(),
                root_relpath: "root/subtree".to_string(),
                batch_kind: FluxonFsTransferBatchKind::FullDir,
                manifest_blob: test_manifest_blob(&[("root/subtree/c.bin", 7)]),
                collect_infos: Vec::new(),
                generation: 2,
            }],
            finished: true,
        })
        .unwrap();
        st.finish_transfer_scan_epoch(job.job_id.as_str(), scan_epoch)
            .unwrap();

        let snapshot = st
            .transfer_job_snapshot(job.job_id.as_str())
            .unwrap()
            .unwrap();
        let live_detail = snapshot.live_detail.unwrap();
        assert_eq!(live_detail.scan.discovered_batch_count, 2);
        assert_eq!(live_detail.scan.discovered_file_count, 3);
        assert_eq!(live_detail.scan.discovered_bytes, 15);
        assert_eq!(live_detail.scan.completed_scan_unit_count, 1);
    }

    #[test]
    fn test_transfer_scan_result_subtree_slice_batches_are_item_deduped() {
        let st = test_transfer_state();
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 0,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let scan_epoch = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        let make_result = |scan_unit_id: &str, batch_id: &str| FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id: scan_unit_id.to_string(),
            scan_task_id: format!("{}__task", scan_unit_id),
            root_relpath: "root".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: Vec::new(),
            child_scan_units: Vec::new(),
            full_dir_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: batch_id.to_string(),
                root_relpath: "root".to_string(),
                batch_kind: FluxonFsTransferBatchKind::SubtreeSlice,
                manifest_blob: test_manifest_blob_with_empty_dirs(
                    &[("root/a.bin", 3)],
                    &["root/empty"],
                ),
                collect_infos: Vec::new(),
                generation: 1,
            }],
            finished: true,
        };
        st.apply_transfer_scan_result(&make_result("scan-1", "batch-1"))
            .unwrap();
        st.apply_transfer_scan_result(&make_result("scan-2", "batch-2"))
            .unwrap();

        let batches = st
            .list_transfer_batches_for_job(job.job_id.as_str())
            .unwrap()
            .into_iter()
            .filter(|batch| batch.state == FluxonFsTransferBatchState::Ready)
            .collect::<Vec<_>>();
        assert_eq!(batches.len(), 1);
        assert_eq!(
            batches[0].batch_kind,
            FluxonFsTransferBatchKind::SubtreeSlice
        );
        let manifest =
            FluxonFsTransferManifestWire::decode_from_blob(batches[0].manifest_blob.as_slice())
                .unwrap();
        assert_eq!(
            manifest.entries,
            vec![FluxonFsTransferManifestEntryWire {
                relpath: "root/a.bin".to_string(),
                size: 3,
            }]
        );
        assert_eq!(manifest.empty_dir_relpaths, vec!["root/empty".to_string()]);
    }

    #[test]
    fn test_begin_transfer_scan_epoch_resets_live_scan_progress() {
        let st = test_transfer_state();
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 0,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let _scan_epoch_1 = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        st.note_transfer_scan_runtime_counts(job.job_id.as_str(), 3, 2);

        let scan_epoch_2 = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        assert!(scan_epoch_2 > 1);

        let snapshot = st
            .transfer_job_snapshot(job.job_id.as_str())
            .unwrap()
            .unwrap();
        let live_detail = snapshot.live_detail.unwrap();
        assert_eq!(live_detail.scan.queued_scan_unit_count, 0);
        assert_eq!(live_detail.scan.inflight_scan_unit_count, 0);
        assert_eq!(live_detail.scan.completed_scan_unit_count, 0);
        assert_eq!(live_detail.scan.discovered_batch_count, 0);
        assert_eq!(live_detail.scan.discovered_file_count, 0);
        assert_eq!(live_detail.scan.discovered_bytes, 0);
        assert_eq!(live_detail.scan.last_scan_result_unix_ms, 0);
    }

    #[test]
    fn test_transfer_scan_discovered_stats_count_each_batch_once_per_epoch() {
        let st = test_transfer_state();
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 0,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let scan_epoch_1 = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        let result = FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch: scan_epoch_1,
            scan_unit_id: format!("{}__scan_epoch_{}__root", job.job_id, scan_epoch_1),
            scan_task_id: "scan-task-root".to_string(),
            root_relpath: "root".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-1".to_string(),
                root_relpath: "root".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                manifest_blob: test_manifest_blob(&[("root/a.bin", 3), ("root/b.bin", 5)]),
                collect_infos: Vec::new(),
                generation: 1,
            }],
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        };
        st.apply_transfer_scan_result(&result).unwrap();
        st.apply_transfer_scan_result(&result).unwrap();

        let snapshot_epoch_1 = st
            .transfer_job_snapshot(job.job_id.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(snapshot_epoch_1.job.scan_discovered_batch_count, 1);
        assert_eq!(snapshot_epoch_1.job.scan_discovered_file_count, 2);
        assert_eq!(snapshot_epoch_1.job.scan_discovered_bytes, 8);
        let live_detail_epoch_1 = snapshot_epoch_1.live_detail.unwrap();
        assert_eq!(live_detail_epoch_1.scan.discovered_batch_count, 1);
        assert_eq!(live_detail_epoch_1.scan.discovered_file_count, 2);
        assert_eq!(live_detail_epoch_1.scan.discovered_bytes, 8);

        let scan_epoch_2 = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            scan_epoch: scan_epoch_2,
            scan_unit_id: format!("{}__scan_epoch_{}__root", job.job_id, scan_epoch_2),
            ..result.clone()
        })
        .unwrap();

        let snapshot_epoch_2 = st
            .transfer_job_snapshot(job.job_id.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(snapshot_epoch_2.job.scan_discovered_batch_count, 1);
        assert_eq!(snapshot_epoch_2.job.scan_discovered_file_count, 2);
        assert_eq!(snapshot_epoch_2.job.scan_discovered_bytes, 8);
        let live_detail_epoch_2 = snapshot_epoch_2.live_detail.unwrap();
        assert_eq!(live_detail_epoch_2.scan.discovered_batch_count, 1);
        assert_eq!(live_detail_epoch_2.scan.discovered_file_count, 2);
        assert_eq!(live_detail_epoch_2.scan.discovered_bytes, 8);
    }

    #[test]
    fn test_transfer_scan_result_accepts_multiple_direct_batches_for_same_root() {
        let st = test_transfer_state();
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 0,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let scan_epoch = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id: format!("{}__scan_epoch_{}__root", job.job_id, scan_epoch),
            scan_task_id: "scan-task-root".to_string(),
            root_relpath: "root".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![
                FluxonFsTransferScanBatchWire {
                    batch_id: "batch-1".to_string(),
                    root_relpath: "root".to_string(),
                    batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                    manifest_blob: test_manifest_blob(&[("root/a.bin", 3)]),
                    collect_infos: Vec::new(),
                    generation: 1,
                },
                FluxonFsTransferScanBatchWire {
                    batch_id: "batch-2".to_string(),
                    root_relpath: "root".to_string(),
                    batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                    manifest_blob: test_manifest_blob(&[("root/b.bin", 5)]),
                    collect_infos: Vec::new(),
                    generation: 1,
                },
            ],
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        })
        .unwrap();
        let batches = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .filter(|batch| batch.job_id == job.job_id)
            .collect::<Vec<_>>();
        assert_eq!(batches.len(), 2);
        assert!(batches.iter().all(|batch| batch.root_relpath == "root"));
        assert!(
            batches
                .iter()
                .all(|batch| batch.batch_kind == FluxonFsTransferBatchKind::DirectFilesOnly)
        );
    }

    #[test]
    fn test_transfer_scan_result_filters_duplicate_direct_file_overlap_on_rescan() {
        let st = test_transfer_state();
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 0,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let scan_epoch = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id: format!("{}__scan_epoch_{}__root", job.job_id, scan_epoch),
            scan_task_id: "scan-task-root-1".to_string(),
            root_relpath: "root".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-1".to_string(),
                root_relpath: "root".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                manifest_blob: test_manifest_blob(&[("root/a.bin", 3), ("root/b.bin", 5)]),
                collect_infos: Vec::new(),
                generation: 1,
            }],
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        })
        .unwrap();
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id: format!("{}__scan_epoch_{}__root_retry", job.job_id, scan_epoch),
            scan_task_id: "scan-task-root-2".to_string(),
            root_relpath: "root".to_string(),
            generation: 2,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-2".to_string(),
                root_relpath: "root".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                manifest_blob: test_manifest_blob(&[("root/b.bin", 5), ("root/c.bin", 7)]),
                collect_infos: Vec::new(),
                generation: 2,
            }],
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        })
        .unwrap();
        let batches = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .filter(|batch| batch.job_id == job.job_id)
            .collect::<Vec<_>>();
        assert_eq!(batches.len(), 2);
        let filtered_batch = batches
            .iter()
            .find(|batch| batch.batch_id == "batch-2")
            .unwrap();
        let manifest =
            FluxonFsTransferManifestWire::decode_from_blob(filtered_batch.manifest_blob.as_slice())
                .unwrap();
        assert_eq!(
            manifest.entries,
            vec![FluxonFsTransferManifestEntryWire {
                relpath: "root/c.bin".to_string(),
                size: 7,
            }]
        );
    }

    #[test]
    fn test_transfer_scan_result_filters_duplicate_direct_empty_dir_overlap_on_rescan() {
        let st = test_transfer_state();
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 0,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let scan_epoch = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id: format!("{}__scan_epoch_{}__root", job.job_id, scan_epoch),
            scan_task_id: "scan-task-root-1".to_string(),
            root_relpath: "root".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-1".to_string(),
                root_relpath: "root".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                manifest_blob: test_manifest_blob_with_empty_dirs(
                    &[],
                    &["root/empty-a", "root/empty-b"],
                ),
                collect_infos: Vec::new(),
                generation: 1,
            }],
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        })
        .unwrap();
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id: format!("{}__scan_epoch_{}__root_retry", job.job_id, scan_epoch),
            scan_task_id: "scan-task-root-2".to_string(),
            root_relpath: "root".to_string(),
            generation: 2,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-2".to_string(),
                root_relpath: "root".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                manifest_blob: test_manifest_blob_with_empty_dirs(
                    &[],
                    &["root/empty-b", "root/empty-c"],
                ),
                collect_infos: Vec::new(),
                generation: 2,
            }],
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        })
        .unwrap();
        let batches = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .filter(|batch| batch.job_id == job.job_id)
            .collect::<Vec<_>>();
        assert_eq!(batches.len(), 2);
        let filtered_batch = batches
            .iter()
            .find(|batch| batch.batch_id == "batch-2")
            .unwrap();
        let manifest =
            FluxonFsTransferManifestWire::decode_from_blob(filtered_batch.manifest_blob.as_slice())
                .unwrap();
        assert!(manifest.entries.is_empty());
        assert_eq!(
            manifest.empty_dir_relpaths,
            vec!["root/empty-c".to_string()]
        );
    }

    #[test]
    fn test_transfer_scan_result_rejects_full_dir_empty_dir_overlap_with_existing_direct_batch() {
        let st = test_transfer_state();
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 0,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let scan_epoch = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id: format!("{}__scan_epoch_{}__root", job.job_id, scan_epoch),
            scan_task_id: "scan-task-root-1".to_string(),
            root_relpath: "root".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-1".to_string(),
                root_relpath: "root".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                manifest_blob: test_manifest_blob_with_empty_dirs(&[], &["root/empty"]),
                collect_infos: Vec::new(),
                generation: 1,
            }],
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        })
        .unwrap();
        let err = st
            .apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
                job_id: job.job_id.clone(),
                scan_epoch,
                scan_unit_id: format!("{}__scan_epoch_{}__child", job.job_id, scan_epoch),
                scan_task_id: "scan-task-root-2".to_string(),
                root_relpath: "root/empty".to_string(),
                generation: 2,
                frontier: FluxonFsTransferScanFrontier {
                    direct_files: Vec::new(),
                    direct_dirs: Vec::new(),
                    empty_dirs: Vec::new(),
                },
                direct_files_only_batches: Vec::new(),
                child_scan_units: Vec::new(),
                full_dir_batches: vec![FluxonFsTransferScanBatchWire {
                    batch_id: "batch-2".to_string(),
                    root_relpath: "root/empty".to_string(),
                    batch_kind: FluxonFsTransferBatchKind::FullDir,
                    manifest_blob: test_manifest_blob_with_empty_dirs(&[], &["root/empty"]),
                    collect_infos: Vec::new(),
                    generation: 2,
                }],
                finished: true,
            })
            .unwrap_err();
        assert!(err.contains("empty dir overlap rejected"));
    }

    #[test]
    fn test_transfer_scan_result_writes_direct_files_complete_marker_after_root_completion() {
        let st = test_transfer_state();
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 0,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let scan_epoch = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        let scan_unit_id = format!("{}__scan_epoch_{}__root", job.job_id, scan_epoch);
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id: scan_unit_id.clone(),
            scan_task_id: "scan-task-root-1".to_string(),
            root_relpath: "root".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-1".to_string(),
                root_relpath: "root".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                manifest_blob: test_manifest_blob(&[("root/a.bin", 3)]),
                collect_infos: Vec::new(),
                generation: 1,
            }],
            child_scan_units: vec![fluxon_fs_core::config::FluxonFsTransferScanChildUnitWire {
                scan_unit_id: scan_unit_id.clone(),
                root_relpath: "root".to_string(),
                generation: 1,
                scan_mode: fluxon_fs_core::config::FluxonFsTransferScanMode::FullTree,
            }],
            full_dir_batches: Vec::new(),
            finished: false,
        })
        .unwrap();
        assert!(
            st.list_transfer_direct_files_complete_records()
                .unwrap()
                .into_iter()
                .filter(|row| row.job_id == job.job_id)
                .collect::<Vec<_>>()
                .is_empty()
        );
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id,
            scan_task_id: "scan-task-root-2".to_string(),
            root_relpath: "root".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-2".to_string(),
                root_relpath: "root".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                manifest_blob: test_manifest_blob(&[("root/b.bin", 5)]),
                collect_infos: Vec::new(),
                generation: 1,
            }],
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        })
        .unwrap();
        let complete_rows = st
            .list_transfer_direct_files_complete_records()
            .unwrap()
            .into_iter()
            .filter(|row| row.job_id == job.job_id)
            .collect::<Vec<_>>();
        assert_eq!(complete_rows.len(), 1);
        assert_eq!(complete_rows[0].root_relpath, "root".to_string());
    }

    #[test]
    fn test_transfer_scan_discovered_stats_follow_durable_job_totals() {
        let st = test_transfer_state();
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 0,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let scan_epoch = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        st.note_transfer_scan_result_accepted(job.job_id.as_str(), 10, 99, 999, 9999);
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id: format!("{}__scan_epoch_{}__root", job.job_id, scan_epoch),
            scan_task_id: "scan-task-root".to_string(),
            root_relpath: "root".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-1".to_string(),
                root_relpath: "root".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                manifest_blob: test_manifest_blob(&[("root/a.bin", 3), ("root/b.bin", 5)]),
                collect_infos: Vec::new(),
                generation: 1,
            }],
            child_scan_units: Vec::new(),
            full_dir_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-2".to_string(),
                root_relpath: "root/subtree".to_string(),
                batch_kind: FluxonFsTransferBatchKind::FullDir,
                manifest_blob: test_manifest_blob(&[("root/subtree/c.bin", 7)]),
                collect_infos: Vec::new(),
                generation: 2,
            }],
            finished: true,
        })
        .unwrap();

        let snapshot = st
            .transfer_job_snapshot(job.job_id.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.job.scan_discovered_batch_count, 2);
        assert_eq!(snapshot.job.scan_discovered_file_count, 3);
        assert_eq!(snapshot.job.scan_discovered_bytes, 15);
        let live_detail = snapshot.live_detail.unwrap();
        assert_eq!(live_detail.scan.discovered_batch_count, 2);
        assert_eq!(live_detail.scan.discovered_file_count, 3);
        assert_eq!(live_detail.scan.discovered_bytes, 15);
        assert_eq!(live_detail.scan.completed_scan_unit_count, 1);
    }

    #[test]
    fn test_import_transfer_prescan_job_rebinds_existing_scan_state() {
        let st = test_transfer_state();
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: FLUXON_FS_LOCAL_TRANSFER_CHECK_SRC_EXPORT.to_string(),
                src_root_relpath: ".".to_string(),
                dst_export: FLUXON_FS_LOCAL_TRANSFER_CHECK_DST_EXPORT.to_string(),
                dst_root_relpath: ".".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 0,
                batch_ready_bytes: 1024,
                job_spec_blob: test_local_prescan_job_spec_blob("/dev/shm/prescan-src", 1024),
            })
            .unwrap();
        let scan_epoch = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id: format!("{}__scan_epoch_{}", job.job_id, scan_epoch),
            scan_task_id: "scan-task-1".to_string(),
            root_relpath: "photos".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-1".to_string(),
                root_relpath: "photos".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                manifest_blob: test_manifest_blob(&[("photos/a.bin", 123)]),
                collect_infos: Vec::new(),
                generation: 1,
            }],
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        })
        .unwrap();
        st.finish_transfer_scan_epoch(job.job_id.as_str(), scan_epoch)
            .unwrap();

        let imported = st
            .import_transfer_prescan_job(
                job.job_id.as_str(),
                "src-export",
                "nested/source",
                "dst-export",
                "ingest/run-1",
                super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                4,
            )
            .unwrap();
        assert_eq!(imported.src_export, "src-export");
        assert_eq!(imported.src_root_relpath, "nested/source");
        assert_eq!(imported.dst_export, "dst-export");
        assert_eq!(imported.dst_root_relpath, "ingest/run-1");
        assert_eq!(imported.desired_worker_count, 4);

        let snapshot = st
            .transfer_job_snapshot(job.job_id.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.open_batches, 1);
        assert!(snapshot.scan_finished);
        assert_eq!(snapshot.job.src_export, "src-export");
        assert_eq!(snapshot.job.dst_export, "dst-export");
        assert_eq!(snapshot.job.desired_worker_count, 4);
    }

    #[test]
    fn test_import_transfer_prescan_job_rejects_non_prescan_job() {
        let st = test_transfer_state();
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: ".".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: ".".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 0,
                batch_ready_bytes: 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let err = st
            .import_transfer_prescan_job(
                job.job_id.as_str(),
                "src-export",
                ".",
                "dst-export",
                ".",
                super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                1,
            )
            .unwrap_err();
        assert!(err.contains("not a local prescan job"));
    }

    #[test]
    fn test_reconcile_transfer_scheduler_state_completes_quiescent_job() {
        let st = test_transfer_state();
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 1,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let scan_epoch = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id: format!("{}__scan_epoch_{}", job.job_id, scan_epoch),
            scan_task_id: "scan-task-1".to_string(),
            root_relpath: ".".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: Vec::new(),
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        })
        .unwrap();
        st.finish_transfer_scan_epoch(job.job_id.as_str(), scan_epoch)
            .unwrap();
        st.reconcile_transfer_scheduler_state(Utc::now().timestamp_millis())
            .unwrap();

        let snapshot = st
            .transfer_job_snapshot(job.job_id.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.job.state, FluxonFsTransferJobState::Completed);
        assert_eq!(snapshot.open_batches, 0);
        assert!(snapshot.scan_finished);
    }

    #[test]
    fn test_transfer_worker_result_records_failed_file_issue_without_failing_job() {
        let st = test_transfer_state();
        let job = create_single_batch_transfer_job(&st, "dst", &[("a.bin", 123)], Vec::new());
        let worker_id = test_transfer_worker_id(job.job_id.as_str(), "batch-1");
        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            worker_id.as_str(),
            "worker-task-1",
            "dst-exporter-1",
            Utc::now().timestamp_millis() + 60_000,
        )
        .unwrap();
        st.apply_transfer_worker_result(&FluxonFsTransferWorkerResultWire {
            job_id: job.job_id.clone(),
            batch_id: "batch-1".to_string(),
            worker_task_id: "worker-task-1".to_string(),
            worker_id,
            file_results: Vec::new(),
            failed_file_results: vec![
                fluxon_fs_core::config::FluxonFsTransferWorkerFailedFileResultWire {
                    relpath: "a.bin".to_string(),
                    reason_kind: fluxon_fs_core::config::FluxonFsTransferFailedFileReasonKindWire::SourceContentChanged,
                    reason_detail: "transfer source file size changed during worker execution: relpath=a.bin expected=123 actual=456".to_string(),
                },
            ],
            collect_info_results: Vec::new(),
            final_telemetry: None,
        })
        .unwrap();

        let snapshot = st
            .transfer_job_snapshot(job.job_id.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.job.state, FluxonFsTransferJobState::Running);
        assert_eq!(snapshot.open_batches, 1);
        assert_eq!(snapshot.failed_file_count, 1);
        assert_eq!(snapshot.failed_files.len(), 1);
        assert_eq!(snapshot.failed_files[0].relpath, "a.bin");

        let running = st.list_running_transfer_jobs().unwrap();
        assert!(
            running.into_iter().any(|row| row.job_id == job.job_id),
            "job with file issues must remain in running transfer job list"
        );
    }

    #[test]
    fn test_equivalent_batch_materialization_reuses_existing_record() {
        let st = test_transfer_state();
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 0,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let scan_epoch_1 = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch: scan_epoch_1,
            scan_unit_id: format!("{}__scan_epoch_{}", job.job_id, scan_epoch_1),
            scan_task_id: "scan-task-1".to_string(),
            root_relpath: "".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-1".to_string(),
                root_relpath: "".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                manifest_blob: test_manifest_blob(&[("a.bin", 123)]),
                collect_infos: Vec::new(),
                generation: 1,
            }],
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        })
        .unwrap();
        st.finish_transfer_scan_epoch(job.job_id.as_str(), scan_epoch_1)
            .unwrap();

        let scan_epoch_2 = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch: scan_epoch_2,
            scan_unit_id: format!("{}__scan_epoch_{}", job.job_id, scan_epoch_2),
            scan_task_id: "scan-task-2".to_string(),
            root_relpath: "".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-2".to_string(),
                root_relpath: "".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                manifest_blob: test_manifest_blob(&[("a.bin", 123)]),
                collect_infos: Vec::new(),
                generation: 1,
            }],
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        })
        .unwrap();
        st.finish_transfer_scan_epoch(job.job_id.as_str(), scan_epoch_2)
            .unwrap();

        let batches = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .filter(|batch| batch.job_id == job.job_id)
            .collect::<Vec<_>>();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].batch_id, "batch-1");
    }

    #[test]
    fn test_stale_scan_result_is_rejected_after_new_scan_epoch() {
        let st = test_transfer_state();
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 0,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let stale_scan_epoch = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        let live_scan_epoch = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        assert!(live_scan_epoch > stale_scan_epoch);

        let err = st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch: stale_scan_epoch,
            scan_unit_id: format!("{}__scan_epoch_{}", job.job_id, stale_scan_epoch),
            scan_task_id: "scan-task-1".to_string(),
            root_relpath: "".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: Vec::new(),
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        });
        assert!(err.is_err());
    }

    #[test]
    fn test_stale_worker_result_is_rejected_after_reassignment() {
        let st = test_transfer_state();
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 1,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let scan_epoch = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id: format!("{}__scan_epoch_{}", job.job_id, scan_epoch),
            scan_task_id: "scan-task-1".to_string(),
            root_relpath: "".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-1".to_string(),
                root_relpath: "".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                manifest_blob: test_manifest_blob(&[("a.bin", 123)]),
                collect_infos: Vec::new(),
                generation: 1,
            }],
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        })
        .unwrap();
        st.finish_transfer_scan_epoch(job.job_id.as_str(), scan_epoch)
            .unwrap();

        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            test_transfer_worker_id(job.job_id.as_str(), "batch-1").as_str(),
            "worker-task-1",
            "dst-exporter-1",
            1,
        )
        .unwrap();
        st.reconcile_transfer_scheduler_state(Utc::now().timestamp_millis())
            .unwrap();
        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            test_transfer_worker_id(job.job_id.as_str(), "batch-1").as_str(),
            "worker-task-2",
            "dst-exporter-1",
            Utc::now().timestamp_millis() + 60_000,
        )
        .unwrap();

        let ack = st.apply_transfer_worker_result(&FluxonFsTransferWorkerResultWire {
            job_id: job.job_id.clone(),
            batch_id: "batch-1".to_string(),
            worker_task_id: "worker-task-1".to_string(),
            worker_id: test_transfer_worker_id(job.job_id.as_str(), "batch-1"),
            file_results: vec![FluxonFsTransferWorkerFileResultWire {
                relpath: "a.bin".to_string(),
                staging_relpath: ".fluxon.stage/job/batch/a.bin.fluxon.part".to_string(),
                final_relpath: "a.bin".to_string(),
                visible_size: 123,
            }],
            failed_file_results: Vec::new(),
            collect_info_results: Vec::new(),
            final_telemetry: None,
        });
        assert!(!ack.unwrap().accepted);

        let batch = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .find(|batch| batch.job_id == job.job_id && batch.batch_id == "batch-1")
            .unwrap();
        assert_eq!(batch.state, FluxonFsTransferBatchState::Running);
        assert_eq!(batch.owner_worker_task_id, "worker-task-2");
    }

    #[test]
    fn test_transfer_worker_result_moves_batch_to_done_until_reconcile() {
        let st = test_transfer_state();
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 1,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let scan_epoch = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id: format!("{}__scan_epoch_{}", job.job_id, scan_epoch),
            scan_task_id: "scan-task-1".to_string(),
            root_relpath: "".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-1".to_string(),
                root_relpath: "".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                manifest_blob: test_manifest_blob(&[("a.bin", 123)]),
                collect_infos: Vec::new(),
                generation: 1,
            }],
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        })
        .unwrap();
        st.finish_transfer_scan_epoch(job.job_id.as_str(), scan_epoch)
            .unwrap();

        let worker_id = test_transfer_worker_id(job.job_id.as_str(), "batch-1");
        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            worker_id.as_str(),
            "worker-task-1",
            "dst-exporter-1",
            1_000,
        )
        .unwrap();
        st.apply_transfer_worker_result(&FluxonFsTransferWorkerResultWire {
            job_id: job.job_id.clone(),
            batch_id: "batch-1".to_string(),
            worker_task_id: "worker-task-1".to_string(),
            worker_id: worker_id.clone(),
            file_results: vec![FluxonFsTransferWorkerFileResultWire {
                relpath: "a.bin".to_string(),
                staging_relpath: ".fluxon.stage/job/batch/a.bin.fluxon.part".to_string(),
                final_relpath: "a.bin".to_string(),
                visible_size: 123,
            }],
            failed_file_results: Vec::new(),
            collect_info_results: Vec::new(),
            final_telemetry: None,
        })
        .unwrap();

        let batch = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .find(|batch| batch.job_id == job.job_id && batch.batch_id == "batch-1")
            .unwrap();
        assert_eq!(batch.state, FluxonFsTransferBatchState::Done);
        assert!(batch.owner_worker_id.is_empty());
        assert!(batch.owner_worker_task_id.is_empty());
        assert_eq!(batch.lease_expire_unix_ms, 0);

        let snapshot = st
            .transfer_job_snapshot(job.job_id.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.open_batches, 1);
        assert!(snapshot.running_batches.is_empty());
        assert_eq!(snapshot.done_batches, 1);

        let summary = st
            .list_transfer_job_summaries()
            .unwrap()
            .into_iter()
            .find(|summary| summary.job.job_id == job.job_id)
            .unwrap();
        assert_eq!(summary.pending_batches, 0);
        assert_eq!(summary.done_batches, 1);
    }

    #[test]
    fn test_transfer_reconcile_keeps_running_batch_with_live_lease() {
        let st = test_transfer_state();
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 1,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let scan_epoch = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id: format!("{}__scan_epoch_{}", job.job_id, scan_epoch),
            scan_task_id: "scan-task-1".to_string(),
            root_relpath: "".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-1".to_string(),
                root_relpath: "".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                manifest_blob: test_manifest_blob(&[("a.bin", 123)]),
                collect_infos: Vec::new(),
                generation: 1,
            }],
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        })
        .unwrap();
        st.finish_transfer_scan_epoch(job.job_id.as_str(), scan_epoch)
            .unwrap();

        let worker_id = test_transfer_worker_id(job.job_id.as_str(), "batch-1");
        let lease_expire_unix_ms = Utc::now().timestamp_millis() + 60_000;
        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            worker_id.as_str(),
            "worker-task-1",
            "dst-exporter-1",
            lease_expire_unix_ms,
        )
        .unwrap();
        st.reconcile_transfer_scheduler_state(Utc::now().timestamp_millis())
            .unwrap();

        let batch = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .find(|batch| batch.job_id == job.job_id && batch.batch_id == "batch-1")
            .unwrap();
        assert_eq!(batch.state, FluxonFsTransferBatchState::Running);
        assert_eq!(batch.owner_worker_id, worker_id);
        assert_eq!(batch.owner_worker_task_id, "worker-task-1");
        assert_eq!(batch.lease_expire_unix_ms, lease_expire_unix_ms);
    }

    #[test]
    fn test_transfer_reconcile_ready_after_worker_result_when_target_incomplete() {
        let st = test_transfer_state();
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 1,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let scan_epoch = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id: format!("{}__scan_epoch_{}", job.job_id, scan_epoch),
            scan_task_id: "scan-task-1".to_string(),
            root_relpath: "".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-1".to_string(),
                root_relpath: "".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                manifest_blob: test_manifest_blob(&[("a.bin", 123)]),
                collect_infos: Vec::new(),
                generation: 1,
            }],
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        })
        .unwrap();
        st.finish_transfer_scan_epoch(job.job_id.as_str(), scan_epoch)
            .unwrap();

        let worker_id = test_transfer_worker_id(job.job_id.as_str(), "batch-1");
        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            worker_id.as_str(),
            "worker-task-1",
            "dst-exporter-1",
            Utc::now().timestamp_millis() + 60_000,
        )
        .unwrap();
        st.apply_transfer_worker_result(&FluxonFsTransferWorkerResultWire {
            job_id: job.job_id.clone(),
            batch_id: "batch-1".to_string(),
            worker_task_id: "worker-task-1".to_string(),
            worker_id,
            file_results: vec![FluxonFsTransferWorkerFileResultWire {
                relpath: "a.bin".to_string(),
                staging_relpath: ".fluxon.stage/job/batch/a.bin.fluxon.part".to_string(),
                final_relpath: "a.bin".to_string(),
                visible_size: 123,
            }],
            failed_file_results: Vec::new(),
            collect_info_results: Vec::new(),
            final_telemetry: None,
        })
        .unwrap();
        st.reconcile_transfer_scheduler_state(Utc::now().timestamp_millis())
            .unwrap();

        let batch = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .find(|batch| batch.job_id == job.job_id && batch.batch_id == "batch-1")
            .unwrap();
        assert_eq!(batch.state, FluxonFsTransferBatchState::Ready);
        assert!(batch.owner_worker_id.is_empty());
        assert!(batch.owner_worker_task_id.is_empty());
        assert_eq!(batch.lease_expire_unix_ms, 0);
    }

    #[test]
    fn test_transfer_reconcile_requires_collect_info_materialization_before_finish() {
        let backend = ObjectBackend::default();
        backend.insert("dst", "a.bin", b"payload-1234");
        let st = test_state_with_buckets(Arc::new(backend.clone()), &["src", "dst"]);
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 1,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let scan_epoch = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        let collect_infos = test_symlink_collect_infos(&[("link.bin", "target.bin")]);
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id: format!("{}__scan_epoch_{}", job.job_id, scan_epoch),
            scan_task_id: "scan-task-1".to_string(),
            root_relpath: "".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-1".to_string(),
                root_relpath: "".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                manifest_blob: test_manifest_blob(&[("a.bin", 12)]),
                collect_infos: collect_infos.clone(),
                generation: 1,
            }],
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        })
        .unwrap();
        st.finish_transfer_scan_epoch(job.job_id.as_str(), scan_epoch)
            .unwrap();

        let worker_id = test_transfer_worker_id(job.job_id.as_str(), "batch-1");
        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            worker_id.as_str(),
            "worker-task-1",
            "dst-exporter-1",
            Utc::now().timestamp_millis() + 60_000,
        )
        .unwrap();
        st.apply_transfer_worker_result(&FluxonFsTransferWorkerResultWire {
            job_id: job.job_id.clone(),
            batch_id: "batch-1".to_string(),
            worker_task_id: "worker-task-1".to_string(),
            worker_id,
            file_results: vec![FluxonFsTransferWorkerFileResultWire {
                relpath: "a.bin".to_string(),
                staging_relpath: ".fluxon.stage/job/batch/a.bin.fluxon.part".to_string(),
                final_relpath: "a.bin".to_string(),
                visible_size: 12,
            }],
            failed_file_results: Vec::new(),
            collect_info_results: Vec::new(),
            final_telemetry: None,
        })
        .unwrap();
        st.reconcile_transfer_scheduler_state(Utc::now().timestamp_millis())
            .unwrap();

        let batch = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .find(|batch| batch.job_id == job.job_id && batch.batch_id == "batch-1")
            .unwrap();
        assert_eq!(batch.state, FluxonFsTransferBatchState::Ready);
        let collect_info = st
            .list_transfer_batch_collect_infos()
            .unwrap()
            .into_iter()
            .find(|row| row.job_id == job.job_id && row.batch_id == "batch-1")
            .unwrap();
        assert!(!collect_info.materialized);
    }

    #[test]
    fn test_transfer_reconcile_finishes_batch_and_completes_job_after_target_visible() {
        let backend = ObjectBackend::default();
        backend.insert("dst", "a.bin", b"payload-1234");
        let st = test_state_with_buckets(Arc::new(backend), &["src", "dst"]);
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 1,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let scan_epoch = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id: format!("{}__scan_epoch_{}", job.job_id, scan_epoch),
            scan_task_id: "scan-task-1".to_string(),
            root_relpath: "".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-1".to_string(),
                root_relpath: "".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                manifest_blob: test_manifest_blob(&[("a.bin", 12)]),
                collect_infos: Vec::new(),
                generation: 1,
            }],
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        })
        .unwrap();
        st.finish_transfer_scan_epoch(job.job_id.as_str(), scan_epoch)
            .unwrap();

        let worker_id = test_transfer_worker_id(job.job_id.as_str(), "batch-1");
        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            worker_id.as_str(),
            "worker-task-1",
            "dst-exporter-1",
            Utc::now().timestamp_millis() + 60_000,
        )
        .unwrap();
        st.apply_transfer_worker_result(&FluxonFsTransferWorkerResultWire {
            job_id: job.job_id.clone(),
            batch_id: "batch-1".to_string(),
            worker_task_id: "worker-task-1".to_string(),
            worker_id,
            file_results: vec![FluxonFsTransferWorkerFileResultWire {
                relpath: "a.bin".to_string(),
                staging_relpath: ".fluxon.stage/job/batch/a.bin.fluxon.part".to_string(),
                final_relpath: "a.bin".to_string(),
                visible_size: 12,
            }],
            failed_file_results: Vec::new(),
            collect_info_results: Vec::new(),
            final_telemetry: None,
        })
        .unwrap();
        st.reconcile_transfer_scheduler_state(Utc::now().timestamp_millis())
            .unwrap();

        let batch = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .find(|batch| batch.job_id == job.job_id && batch.batch_id == "batch-1")
            .unwrap();
        assert_eq!(batch.state, FluxonFsTransferBatchState::Finished);
        assert!(batch.owner_worker_id.is_empty());
        assert!(batch.owner_worker_task_id.is_empty());
        assert_eq!(batch.lease_expire_unix_ms, 0);

        let snapshot = st
            .transfer_job_snapshot(job.job_id.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.job.state, FluxonFsTransferJobState::Completed);
        assert_eq!(snapshot.open_batches, 0);
        assert!(snapshot.scan_finished);
    }

    #[test]
    fn test_transfer_reconcile_finishes_batch_when_missing_file_marked_as_issue() {
        let backend = ObjectBackend::default();
        backend.insert("dst", "a.bin", b"payload-1234");
        let st = test_state_with_buckets(Arc::new(backend), &["src", "dst"]);
        let job = create_single_batch_transfer_job(
            &st,
            "dst",
            &[("a.bin", 12), ("b.bin", 34)],
            Vec::new(),
        );

        let worker_id = test_transfer_worker_id(job.job_id.as_str(), "batch-1");
        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            worker_id.as_str(),
            "worker-task-1",
            "dst-exporter-1",
            Utc::now().timestamp_millis() + 60_000,
        )
        .unwrap();
        st.apply_transfer_worker_result(&FluxonFsTransferWorkerResultWire {
            job_id: job.job_id.clone(),
            batch_id: "batch-1".to_string(),
            worker_task_id: "worker-task-1".to_string(),
            worker_id,
            file_results: vec![FluxonFsTransferWorkerFileResultWire {
                relpath: "a.bin".to_string(),
                staging_relpath: ".fluxon.stage/job/batch/a.bin.fluxon.part".to_string(),
                final_relpath: "a.bin".to_string(),
                visible_size: 12,
            }],
            failed_file_results: vec![
                fluxon_fs_core::config::FluxonFsTransferWorkerFailedFileResultWire {
                    relpath: "b.bin".to_string(),
                    reason_kind: fluxon_fs_core::config::FluxonFsTransferFailedFileReasonKindWire::SourceContentChanged,
                    reason_detail: "transfer worker source ended before expected size: relpath=b.bin expected=34 copied=12".to_string(),
                },
            ],
            collect_info_results: Vec::new(),
            final_telemetry: None,
        })
        .unwrap();
        st.reconcile_transfer_scheduler_state(Utc::now().timestamp_millis())
            .unwrap();

        let batch = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .find(|batch| batch.job_id == job.job_id && batch.batch_id == "batch-1")
            .unwrap();
        assert_eq!(batch.state, FluxonFsTransferBatchState::Finished);

        let snapshot = st
            .transfer_job_snapshot(job.job_id.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.job.state, FluxonFsTransferJobState::Completed);
        assert_eq!(snapshot.failed_file_count, 1);
        assert_eq!(snapshot.open_batches, 0);
    }

    #[test]
    fn test_transfer_reconcile_finishes_batch_after_collect_info_visible() {
        let backend = ObjectBackend::default();
        backend.insert("dst", "a.bin", b"payload-1234");
        let collect_infos = test_symlink_collect_infos(&[("link.bin", "target.bin")]);
        let collect_output_relpath = transfer_collect_info_output_relpath(
            "batch-1",
            FluxonFsTransferCollectInfoKind::SymlinkNotice,
        )
        .unwrap();
        backend.insert(
            "dst",
            collect_output_relpath.as_str(),
            collect_infos[0].collect_blob.as_slice(),
        );
        let st = test_state_with_buckets(Arc::new(backend.clone()), &["src", "dst"]);
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 1,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let scan_epoch = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id: format!("{}__scan_epoch_{}", job.job_id, scan_epoch),
            scan_task_id: "scan-task-1".to_string(),
            root_relpath: "".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-1".to_string(),
                root_relpath: "".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                manifest_blob: test_manifest_blob(&[("a.bin", 12)]),
                collect_infos: collect_infos.clone(),
                generation: 1,
            }],
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        })
        .unwrap();
        st.finish_transfer_scan_epoch(job.job_id.as_str(), scan_epoch)
            .unwrap();

        let worker_id = test_transfer_worker_id(job.job_id.as_str(), "batch-1");
        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            worker_id.as_str(),
            "worker-task-1",
            "dst-exporter-1",
            Utc::now().timestamp_millis() + 60_000,
        )
        .unwrap();
        st.apply_transfer_worker_result(&FluxonFsTransferWorkerResultWire {
            job_id: job.job_id.clone(),
            batch_id: "batch-1".to_string(),
            worker_task_id: "worker-task-1".to_string(),
            worker_id,
            file_results: vec![FluxonFsTransferWorkerFileResultWire {
                relpath: "a.bin".to_string(),
                staging_relpath: ".fluxon.stage/job/batch/a.bin.fluxon.part".to_string(),
                final_relpath: "a.bin".to_string(),
                visible_size: 12,
            }],
            failed_file_results: Vec::new(),
            collect_info_results: vec![FluxonFsTransferWorkerCollectInfoResultWire {
                collect_kind: FluxonFsTransferCollectInfoKind::SymlinkNotice,
                output_relpath: collect_output_relpath.clone(),
                materialized_bytes: collect_infos[0].collect_blob.len() as i64,
            }],
            final_telemetry: None,
        })
        .unwrap();
        st.reconcile_transfer_scheduler_state(Utc::now().timestamp_millis())
            .unwrap();

        let batch = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .find(|batch| batch.job_id == job.job_id && batch.batch_id == "batch-1")
            .unwrap();
        assert_eq!(batch.state, FluxonFsTransferBatchState::Finished);
        let collect_info = st
            .list_transfer_batch_collect_infos()
            .unwrap()
            .into_iter()
            .find(|row| row.job_id == job.job_id && row.batch_id == "batch-1")
            .unwrap();
        assert!(collect_info.materialized);
    }

    #[test]
    fn test_transfer_reconcile_expired_batch_requeues_and_clears_ownership() {
        let st = test_transfer_state();
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 1,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let scan_epoch = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id: format!("{}__scan_epoch_{}", job.job_id, scan_epoch),
            scan_task_id: "scan-task-1".to_string(),
            root_relpath: "".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-1".to_string(),
                root_relpath: "".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                manifest_blob: test_manifest_blob(&[("a.bin", 123)]),
                collect_infos: Vec::new(),
                generation: 1,
            }],
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        })
        .unwrap();
        st.finish_transfer_scan_epoch(job.job_id.as_str(), scan_epoch)
            .unwrap();

        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            test_transfer_worker_id(job.job_id.as_str(), "batch-1").as_str(),
            "worker-task-1",
            "dst-exporter-1",
            1,
        )
        .unwrap();
        st.reconcile_transfer_scheduler_state(Utc::now().timestamp_millis())
            .unwrap();

        let batch = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .find(|batch| batch.job_id == job.job_id && batch.batch_id == "batch-1")
            .unwrap();
        assert_eq!(batch.state, FluxonFsTransferBatchState::Ready);
        assert!(batch.owner_worker_id.is_empty());
        assert!(batch.owner_worker_task_id.is_empty());
        assert_eq!(batch.lease_expire_unix_ms, 0);

        let snapshot = st
            .transfer_job_snapshot(job.job_id.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.job.state, FluxonFsTransferJobState::Running);
        assert_eq!(snapshot.open_batches, 1);
    }

    #[test]
    #[ignore = "requires fluxon_release/ext_images/tikv prepared by setup_and_pack/pack_release_ext.py"]
    fn test_stale_worker_result_is_rejected_after_reassignment_with_tikv_state_store() {
        let tikv = LocalTiKvHarness::new();
        let st = test_transfer_state_with_access_config(tikv.access_config.clone());
        let job = create_single_batch_transfer_job(&st, "dst", &[("a.bin", 123)], Vec::new());

        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            test_transfer_worker_id(job.job_id.as_str(), "batch-1").as_str(),
            "worker-task-1",
            "dst-exporter-1",
            1,
        )
        .unwrap();
        st.reconcile_transfer_scheduler_state(Utc::now().timestamp_millis())
            .unwrap();
        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            test_transfer_worker_id(job.job_id.as_str(), "batch-1").as_str(),
            "worker-task-2",
            "dst-exporter-1",
            Utc::now().timestamp_millis() + 60_000,
        )
        .unwrap();

        let ack = st.apply_transfer_worker_result(&FluxonFsTransferWorkerResultWire {
            job_id: job.job_id.clone(),
            batch_id: "batch-1".to_string(),
            worker_task_id: "worker-task-1".to_string(),
            worker_id: test_transfer_worker_id(job.job_id.as_str(), "batch-1"),
            file_results: vec![FluxonFsTransferWorkerFileResultWire {
                relpath: "a.bin".to_string(),
                staging_relpath: ".fluxon.stage/job/batch/a.bin.fluxon.part".to_string(),
                final_relpath: "a.bin".to_string(),
                visible_size: 123,
            }],
            failed_file_results: Vec::new(),
            collect_info_results: Vec::new(),
            final_telemetry: None,
        });
        assert!(!ack.unwrap().accepted);

        let batch = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .find(|batch| batch.job_id == job.job_id && batch.batch_id == "batch-1")
            .unwrap();
        assert_eq!(batch.state, FluxonFsTransferBatchState::Running);
        assert_eq!(batch.owner_worker_task_id, "worker-task-2");
    }

    #[test]
    #[ignore = "requires fluxon_release/ext_images/tikv prepared by setup_and_pack/pack_release_ext.py"]
    fn test_transfer_worker_result_moves_batch_to_done_until_reconcile_with_tikv_state_store() {
        let tikv = LocalTiKvHarness::new();
        let st = test_transfer_state_with_access_config(tikv.access_config.clone());
        let job = create_single_batch_transfer_job(&st, "dst", &[("a.bin", 123)], Vec::new());

        let worker_id = test_transfer_worker_id(job.job_id.as_str(), "batch-1");
        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            worker_id.as_str(),
            "worker-task-1",
            "dst-exporter-1",
            1_000,
        )
        .unwrap();
        st.apply_transfer_worker_result(&FluxonFsTransferWorkerResultWire {
            job_id: job.job_id.clone(),
            batch_id: "batch-1".to_string(),
            worker_task_id: "worker-task-1".to_string(),
            worker_id: worker_id.clone(),
            file_results: vec![FluxonFsTransferWorkerFileResultWire {
                relpath: "a.bin".to_string(),
                staging_relpath: ".fluxon.stage/job/batch/a.bin.fluxon.part".to_string(),
                final_relpath: "a.bin".to_string(),
                visible_size: 123,
            }],
            failed_file_results: Vec::new(),
            collect_info_results: Vec::new(),
            final_telemetry: None,
        })
        .unwrap();

        let batch = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .find(|batch| batch.job_id == job.job_id && batch.batch_id == "batch-1")
            .unwrap();
        assert_eq!(batch.state, FluxonFsTransferBatchState::Done);
        assert!(batch.owner_worker_id.is_empty());
        assert!(batch.owner_worker_task_id.is_empty());
        assert_eq!(batch.lease_expire_unix_ms, 0);

        let snapshot = st
            .transfer_job_snapshot(job.job_id.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.open_batches, 1);
        assert!(snapshot.running_batches.is_empty());
        assert_eq!(snapshot.done_batches, 1);
    }

    #[test]
    #[ignore = "requires fluxon_release/ext_images/tikv prepared by setup_and_pack/pack_release_ext.py"]
    fn test_transfer_reconcile_ready_after_worker_result_when_target_incomplete_with_tikv_state_store()
     {
        let tikv = LocalTiKvHarness::new();
        let st = test_transfer_state_with_access_config(tikv.access_config.clone());
        let job = create_single_batch_transfer_job(&st, "dst", &[("a.bin", 123)], Vec::new());

        let worker_id = test_transfer_worker_id(job.job_id.as_str(), "batch-1");
        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            worker_id.as_str(),
            "worker-task-1",
            "dst-exporter-1",
            Utc::now().timestamp_millis() + 60_000,
        )
        .unwrap();
        st.apply_transfer_worker_result(&FluxonFsTransferWorkerResultWire {
            job_id: job.job_id.clone(),
            batch_id: "batch-1".to_string(),
            worker_task_id: "worker-task-1".to_string(),
            worker_id,
            file_results: vec![FluxonFsTransferWorkerFileResultWire {
                relpath: "a.bin".to_string(),
                staging_relpath: ".fluxon.stage/job/batch/a.bin.fluxon.part".to_string(),
                final_relpath: "a.bin".to_string(),
                visible_size: 123,
            }],
            failed_file_results: Vec::new(),
            collect_info_results: Vec::new(),
            final_telemetry: None,
        })
        .unwrap();
        st.reconcile_transfer_scheduler_state(Utc::now().timestamp_millis())
            .unwrap();

        let batch = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .find(|batch| batch.job_id == job.job_id && batch.batch_id == "batch-1")
            .unwrap();
        assert_eq!(batch.state, FluxonFsTransferBatchState::Ready);
        assert!(batch.owner_worker_id.is_empty());
        assert!(batch.owner_worker_task_id.is_empty());
        assert_eq!(batch.lease_expire_unix_ms, 0);
    }

    #[test]
    #[ignore = "requires fluxon_release/ext_images/tikv prepared by setup_and_pack/pack_release_ext.py"]
    fn test_transfer_reconcile_finishes_batch_and_completes_job_after_target_visible_with_tikv_state_store()
     {
        let tikv = LocalTiKvHarness::new();
        let backend = Arc::new(ObjectBackend::default());
        backend.insert("dst", "a.bin", b"payload-1234");
        let st =
            test_transfer_state_with_backend_and_access_config(backend, tikv.access_config.clone());
        let job = create_single_batch_transfer_job(&st, "dst", &[("a.bin", 12)], Vec::new());

        let worker_id = test_transfer_worker_id(job.job_id.as_str(), "batch-1");
        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            worker_id.as_str(),
            "worker-task-1",
            "dst-exporter-1",
            Utc::now().timestamp_millis() + 60_000,
        )
        .unwrap();
        st.apply_transfer_worker_result(&FluxonFsTransferWorkerResultWire {
            job_id: job.job_id.clone(),
            batch_id: "batch-1".to_string(),
            worker_task_id: "worker-task-1".to_string(),
            worker_id,
            file_results: vec![FluxonFsTransferWorkerFileResultWire {
                relpath: "a.bin".to_string(),
                staging_relpath: ".fluxon.stage/job/batch/a.bin.fluxon.part".to_string(),
                final_relpath: "a.bin".to_string(),
                visible_size: 12,
            }],
            failed_file_results: Vec::new(),
            collect_info_results: Vec::new(),
            final_telemetry: None,
        })
        .unwrap();
        st.reconcile_transfer_scheduler_state(Utc::now().timestamp_millis())
            .unwrap();

        let batch = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .find(|batch| batch.job_id == job.job_id && batch.batch_id == "batch-1")
            .unwrap();
        assert_eq!(batch.state, FluxonFsTransferBatchState::Finished);
        assert!(batch.owner_worker_id.is_empty());
        assert!(batch.owner_worker_task_id.is_empty());
        assert_eq!(batch.lease_expire_unix_ms, 0);

        let snapshot = st
            .transfer_job_snapshot(job.job_id.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.job.state, FluxonFsTransferJobState::Completed);
        assert_eq!(snapshot.open_batches, 0);
        assert!(snapshot.scan_finished);
    }

    #[test]
    #[ignore = "requires fluxon_release/ext_images/tikv prepared by setup_and_pack/pack_release_ext.py"]
    fn test_transfer_reconcile_keeps_batch_ready_when_manifest_empty_dir_missing_with_tikv_state_store()
     {
        let tikv = LocalTiKvHarness::new();
        let backend = Arc::new(ObjectBackend::default());
        backend.insert("dst", "a.bin", b"payload-1234");
        let st =
            test_transfer_state_with_backend_and_access_config(backend, tikv.access_config.clone());
        let job = create_single_batch_transfer_job_with_manifest_blob(
            &st,
            "dst",
            "",
            FluxonFsTransferBatchKind::FullDir,
            test_manifest_blob_with_empty_dirs(&[("a.bin", 12)], &["emptydir"]),
            Vec::new(),
        );

        let worker_id = test_transfer_worker_id(job.job_id.as_str(), "batch-1");
        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            worker_id.as_str(),
            "worker-task-1",
            "dst-exporter-1",
            Utc::now().timestamp_millis() + 60_000,
        )
        .unwrap();
        st.apply_transfer_worker_result(&FluxonFsTransferWorkerResultWire {
            job_id: job.job_id.clone(),
            batch_id: "batch-1".to_string(),
            worker_task_id: "worker-task-1".to_string(),
            worker_id,
            file_results: vec![FluxonFsTransferWorkerFileResultWire {
                relpath: "a.bin".to_string(),
                staging_relpath: ".fluxon.stage/job/batch/a.bin.fluxon.part".to_string(),
                final_relpath: "a.bin".to_string(),
                visible_size: 12,
            }],
            failed_file_results: Vec::new(),
            collect_info_results: Vec::new(),
            final_telemetry: None,
        })
        .unwrap();
        st.reconcile_transfer_scheduler_state(Utc::now().timestamp_millis())
            .unwrap();

        let batch = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .find(|batch| batch.job_id == job.job_id && batch.batch_id == "batch-1")
            .unwrap();
        assert_eq!(batch.state, FluxonFsTransferBatchState::Ready);

        let snapshot = st
            .transfer_job_snapshot(job.job_id.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.job.state, FluxonFsTransferJobState::Running);
        assert_eq!(snapshot.open_batches, 1);
    }

    #[test]
    #[ignore = "requires fluxon_release/ext_images/tikv prepared by setup_and_pack/pack_release_ext.py"]
    fn test_transfer_reconcile_finishes_batch_when_manifest_empty_dir_visible_with_tikv_state_store()
     {
        let tikv = LocalTiKvHarness::new();
        let backend = Arc::new(ObjectBackend::default());
        backend.insert("dst", "a.bin", b"payload-1234");
        backend.insert_dir("dst", "emptydir");
        let st =
            test_transfer_state_with_backend_and_access_config(backend, tikv.access_config.clone());
        let job = create_single_batch_transfer_job_with_manifest_blob(
            &st,
            "dst",
            "",
            FluxonFsTransferBatchKind::FullDir,
            test_manifest_blob_with_empty_dirs(&[("a.bin", 12)], &["emptydir"]),
            Vec::new(),
        );

        let worker_id = test_transfer_worker_id(job.job_id.as_str(), "batch-1");
        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            worker_id.as_str(),
            "worker-task-1",
            "dst-exporter-1",
            Utc::now().timestamp_millis() + 60_000,
        )
        .unwrap();
        st.apply_transfer_worker_result(&FluxonFsTransferWorkerResultWire {
            job_id: job.job_id.clone(),
            batch_id: "batch-1".to_string(),
            worker_task_id: "worker-task-1".to_string(),
            worker_id,
            file_results: vec![FluxonFsTransferWorkerFileResultWire {
                relpath: "a.bin".to_string(),
                staging_relpath: ".fluxon.stage/job/batch/a.bin.fluxon.part".to_string(),
                final_relpath: "a.bin".to_string(),
                visible_size: 12,
            }],
            failed_file_results: Vec::new(),
            collect_info_results: Vec::new(),
            final_telemetry: None,
        })
        .unwrap();
        st.reconcile_transfer_scheduler_state(Utc::now().timestamp_millis())
            .unwrap();

        let batch = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .find(|batch| batch.job_id == job.job_id && batch.batch_id == "batch-1")
            .unwrap();
        assert_eq!(batch.state, FluxonFsTransferBatchState::Finished);

        let snapshot = st
            .transfer_job_snapshot(job.job_id.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.job.state, FluxonFsTransferJobState::Completed);
        assert_eq!(snapshot.open_batches, 0);
    }

    #[test]
    #[ignore = "requires fluxon_release/ext_images/tikv prepared by setup_and_pack/pack_release_ext.py"]
    fn test_transfer_reconcile_finishes_batch_after_collect_info_visible_with_tikv_state_store() {
        let tikv = LocalTiKvHarness::new();
        let backend = ObjectBackend::default();
        backend.insert("dst", "a.bin", b"payload-1234");
        let collect_infos = test_symlink_collect_infos(&[("link.bin", "target.bin")]);
        let collect_output_relpath = transfer_collect_info_output_relpath(
            "batch-1",
            FluxonFsTransferCollectInfoKind::SymlinkNotice,
        )
        .unwrap();
        backend.insert(
            "dst",
            collect_output_relpath.as_str(),
            collect_infos[0].collect_blob.as_slice(),
        );
        let st = test_transfer_state_with_backend_and_access_config(
            Arc::new(backend.clone()),
            tikv.access_config.clone(),
        );
        let job =
            create_single_batch_transfer_job(&st, "dst", &[("a.bin", 12)], collect_infos.clone());

        let worker_id = test_transfer_worker_id(job.job_id.as_str(), "batch-1");
        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            worker_id.as_str(),
            "worker-task-1",
            "dst-exporter-1",
            Utc::now().timestamp_millis() + 60_000,
        )
        .unwrap();
        st.apply_transfer_worker_result(&FluxonFsTransferWorkerResultWire {
            job_id: job.job_id.clone(),
            batch_id: "batch-1".to_string(),
            worker_task_id: "worker-task-1".to_string(),
            worker_id,
            file_results: vec![FluxonFsTransferWorkerFileResultWire {
                relpath: "a.bin".to_string(),
                staging_relpath: ".fluxon.stage/job/batch/a.bin.fluxon.part".to_string(),
                final_relpath: "a.bin".to_string(),
                visible_size: 12,
            }],
            failed_file_results: Vec::new(),
            collect_info_results: vec![FluxonFsTransferWorkerCollectInfoResultWire {
                collect_kind: FluxonFsTransferCollectInfoKind::SymlinkNotice,
                output_relpath: collect_output_relpath.clone(),
                materialized_bytes: collect_infos[0].collect_blob.len() as i64,
            }],
            final_telemetry: None,
        })
        .unwrap();
        st.reconcile_transfer_scheduler_state(Utc::now().timestamp_millis())
            .unwrap();

        let batch = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .find(|batch| batch.job_id == job.job_id && batch.batch_id == "batch-1")
            .unwrap();
        assert_eq!(batch.state, FluxonFsTransferBatchState::Finished);
        let collect_info = st
            .list_transfer_batch_collect_infos()
            .unwrap()
            .into_iter()
            .find(|row| row.job_id == job.job_id && row.batch_id == "batch-1")
            .unwrap();
        assert!(collect_info.materialized);
    }

    #[test]
    #[ignore = "requires fluxon_release/ext_images/tikv prepared by setup_and_pack/pack_release_ext.py"]
    fn test_transfer_worker_heartbeat_refreshes_worker_lease_with_tikv_state_store() {
        let tikv = LocalTiKvHarness::new();
        let st = test_transfer_state_with_access_config(tikv.access_config.clone());
        let job = create_single_batch_transfer_job(&st, "dst", &[("a.bin", 123)], Vec::new());

        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            test_transfer_worker_id(job.job_id.as_str(), "batch-1").as_str(),
            "worker-task-1",
            "dst-exporter-1",
            100,
        )
        .unwrap();

        let heartbeat_result = st
            .apply_transfer_worker_heartbeat(
                &fluxon_fs_core::config::FluxonFsTransferWorkerHeartbeatWire {
                    job_id: job.job_id.clone(),
                    worker_id: test_transfer_worker_id(job.job_id.as_str(), "batch-1"),
                    assigned_batch_id: "batch-1".to_string(),
                    worker_task_id: "worker-task-1".to_string(),
                    heartbeat_unix_ms: 90,
                    telemetry: None,
                },
                90,
                100,
            )
            .unwrap();
        assert_eq!(heartbeat_result.lease_expire_unix_ms, 190);

        let batch = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .find(|batch| batch.job_id == job.job_id && batch.batch_id == "batch-1")
            .unwrap();
        assert_eq!(batch.state, FluxonFsTransferBatchState::Running);
        assert_eq!(batch.owner_worker_task_id, "worker-task-1");
        assert_eq!(
            batch.lease_expire_unix_ms,
            heartbeat_result.lease_expire_unix_ms
        );
    }

    #[test]
    #[ignore = "requires fluxon_release/ext_images/tikv prepared by setup_and_pack/pack_release_ext.py"]
    fn test_transfer_reconcile_expired_batch_requeues_and_clears_ownership_with_tikv_state_store() {
        let tikv = LocalTiKvHarness::new();
        let st = test_transfer_state_with_access_config(tikv.access_config.clone());
        let job = create_single_batch_transfer_job(&st, "dst", &[("a.bin", 123)], Vec::new());

        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            test_transfer_worker_id(job.job_id.as_str(), "batch-1").as_str(),
            "worker-task-1",
            "dst-exporter-1",
            1,
        )
        .unwrap();
        st.reconcile_transfer_scheduler_state(Utc::now().timestamp_millis())
            .unwrap();

        let batch = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .find(|batch| batch.job_id == job.job_id && batch.batch_id == "batch-1")
            .unwrap();
        assert_eq!(batch.state, FluxonFsTransferBatchState::Ready);
        assert!(batch.owner_worker_id.is_empty());
        assert!(batch.owner_worker_task_id.is_empty());
        assert_eq!(batch.lease_expire_unix_ms, 0);

        let snapshot = st
            .transfer_job_snapshot(job.job_id.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.job.state, FluxonFsTransferJobState::Running);
        assert_eq!(snapshot.open_batches, 1);
    }

    #[test]
    fn test_transfer_worker_heartbeat_refreshes_worker_lease() {
        let st = test_transfer_state();
        let job = st
            .create_transfer_job(FsTransferCreateJobArg {
                src_export: "src".to_string(),
                src_root_relpath: "".to_string(),
                dst_export: "dst".to_string(),
                dst_root_relpath: "".to_string(),
                desired_scan_concurrency: super::DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
                desired_worker_count: 1,
                batch_ready_bytes: 8 * 1024 * 1024 * 1024,
                job_spec_blob: Vec::new(),
            })
            .unwrap();
        let scan_epoch = st.begin_transfer_scan_epoch(job.job_id.as_str()).unwrap();
        st.apply_transfer_scan_result(&FluxonFsTransferScanResultWire {
            job_id: job.job_id.clone(),
            scan_epoch,
            scan_unit_id: format!("{}__scan_epoch_{}", job.job_id, scan_epoch),
            scan_task_id: "scan-task-1".to_string(),
            root_relpath: "".to_string(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![FluxonFsTransferScanBatchWire {
                batch_id: "batch-1".to_string(),
                root_relpath: "".to_string(),
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                manifest_blob: test_manifest_blob(&[("a.bin", 123)]),
                collect_infos: Vec::new(),
                generation: 1,
            }],
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        })
        .unwrap();
        st.finish_transfer_scan_epoch(job.job_id.as_str(), scan_epoch)
            .unwrap();
        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            test_transfer_worker_id(job.job_id.as_str(), "batch-1").as_str(),
            "worker-task-1",
            "dst-exporter-1",
            100,
        )
        .unwrap();

        let heartbeat_result = st
            .apply_transfer_worker_heartbeat(
                &fluxon_fs_core::config::FluxonFsTransferWorkerHeartbeatWire {
                    job_id: job.job_id.clone(),
                    worker_id: test_transfer_worker_id(job.job_id.as_str(), "batch-1"),
                    assigned_batch_id: "batch-1".to_string(),
                    worker_task_id: "worker-task-1".to_string(),
                    heartbeat_unix_ms: 90,
                    telemetry: None,
                },
                90,
                100,
            )
            .unwrap();
        assert_eq!(heartbeat_result.lease_expire_unix_ms, 190);

        let batch = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .find(|batch| batch.job_id == job.job_id && batch.batch_id == "batch-1")
            .unwrap();
        assert_eq!(batch.state, FluxonFsTransferBatchState::Running);
        assert_eq!(batch.owner_worker_task_id, "worker-task-1");
        assert_eq!(
            batch.lease_expire_unix_ms,
            heartbeat_result.lease_expire_unix_ms
        );
    }

    #[test]
    fn test_transfer_worker_heartbeat_uses_master_receipt_time_for_lease_authority() {
        let st = test_transfer_state();
        let job = create_single_batch_transfer_job(&st, "dst", &[("a.bin", 123)], Vec::new());
        let worker_id = test_transfer_worker_id(job.job_id.as_str(), "batch-1");
        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            worker_id.as_str(),
            "worker-task-1",
            "dst-exporter-1",
            100,
        )
        .unwrap();

        let heartbeat_result = st
            .apply_transfer_worker_heartbeat(
                &fluxon_fs_core::config::FluxonFsTransferWorkerHeartbeatWire {
                    job_id: job.job_id.clone(),
                    worker_id: worker_id.clone(),
                    assigned_batch_id: "batch-1".to_string(),
                    worker_task_id: "worker-task-1".to_string(),
                    heartbeat_unix_ms: 10_000,
                    telemetry: None,
                },
                10,
                100,
            )
            .unwrap();
        assert_eq!(heartbeat_result.lease_expire_unix_ms, 110);

        st.reconcile_transfer_scheduler_state(111).unwrap();

        let batch = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .find(|batch| batch.job_id == job.job_id && batch.batch_id == "batch-1")
            .unwrap();
        assert_eq!(batch.state, FluxonFsTransferBatchState::Ready);
        assert!(batch.owner_worker_id.is_empty());
        assert!(batch.owner_worker_task_id.is_empty());
    }

    #[test]
    fn test_cancel_transfer_job_marks_job_stopping_then_reconcile_cancels_open_work() {
        let st = test_transfer_state();
        let job = create_single_batch_transfer_job(&st, "dst", &[("a.bin", 123)], Vec::new());
        let worker_id = test_transfer_worker_id(job.job_id.as_str(), "batch-1");
        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            worker_id.as_str(),
            "worker-task-1",
            "dst-exporter-1",
            100,
        )
        .unwrap();

        st.cancel_transfer_job(job.job_id.as_str()).unwrap();

        let snapshot = st
            .transfer_job_snapshot(job.job_id.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.job.state, FluxonFsTransferJobState::Stopping);
        assert!(snapshot.job.scan_finished);
        assert_eq!(snapshot.job.desired_worker_count, 0);
        assert_eq!(snapshot.open_batches, 1);
        assert_eq!(snapshot.running_batches.len(), 1);
        assert_eq!(snapshot.job.last_error, "transfer job cancelled by user");

        st.reconcile_transfer_scheduler_state(101).unwrap();

        let snapshot = st
            .transfer_job_snapshot(job.job_id.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.job.state, FluxonFsTransferJobState::Cancelled);
        assert_eq!(snapshot.open_batches, 0);
        assert!(snapshot.running_batches.is_empty());

        let batch = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .find(|batch| batch.job_id == job.job_id && batch.batch_id == "batch-1")
            .unwrap();
        assert_eq!(batch.state, FluxonFsTransferBatchState::Cancelled);
        assert!(batch.owner_worker_id.is_empty());
        assert!(batch.owner_worker_task_id.is_empty());
        assert_eq!(batch.lease_expire_unix_ms, 0);

        let attempts = st.list_transfer_worker_attempt_records().unwrap();
        let attempt = attempts
            .iter()
            .find(|attempt| {
                attempt.job_id == job.job_id && attempt.worker_task_id == "worker-task-1"
            })
            .unwrap();
        assert_eq!(format!("{:?}", attempt.state), "Stopped");
        assert_eq!(
            attempt.stop_reason,
            Some(fluxon_fs_core::config::FluxonFsTransferWorkerStopReasonWire::Cancelled)
        );
        assert_eq!(attempt.last_error, "transfer job cancelled by user");

        let heartbeat_result = st
            .apply_transfer_worker_heartbeat(
                &fluxon_fs_core::config::FluxonFsTransferWorkerHeartbeatWire {
                    job_id: job.job_id.clone(),
                    worker_id,
                    assigned_batch_id: "batch-1".to_string(),
                    worker_task_id: "worker-task-1".to_string(),
                    heartbeat_unix_ms: 90,
                    telemetry: None,
                },
                90,
                100,
            )
            .unwrap();
        assert!(!heartbeat_result.continue_running);
        assert_eq!(
            heartbeat_result.stop_reason,
            Some(fluxon_fs_core::config::FluxonFsTransferWorkerStopReasonWire::Cancelled)
        );
    }

    #[test]
    fn test_transfer_worker_heartbeat_stop_marks_superseded_attempt_stopped() {
        let st = test_transfer_state();
        let job = create_single_batch_transfer_job(&st, "dst", &[("a.bin", 123)], Vec::new());
        let worker_id = test_transfer_worker_id(job.job_id.as_str(), "batch-1");
        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            worker_id.as_str(),
            "worker-task-old",
            "dst-exporter-1",
            100,
        )
        .unwrap();

        st.reconcile_transfer_scheduler_state(101).unwrap();

        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            worker_id.as_str(),
            "worker-task-new",
            "dst-exporter-1",
            300,
        )
        .unwrap();

        let heartbeat_result = st
            .apply_transfer_worker_heartbeat(
                &fluxon_fs_core::config::FluxonFsTransferWorkerHeartbeatWire {
                    job_id: job.job_id.clone(),
                    worker_id: worker_id.clone(),
                    assigned_batch_id: "batch-1".to_string(),
                    worker_task_id: "worker-task-old".to_string(),
                    heartbeat_unix_ms: 150,
                    telemetry: None,
                },
                150,
                100,
            )
            .unwrap();
        assert!(!heartbeat_result.continue_running);
        assert_eq!(
            heartbeat_result.stop_reason,
            Some(fluxon_fs_core::config::FluxonFsTransferWorkerStopReasonWire::Superseded)
        );

        let attempts = st.list_transfer_worker_attempt_records().unwrap();
        let old_attempt = attempts
            .iter()
            .find(|attempt| {
                attempt.job_id == job.job_id && attempt.worker_task_id == "worker-task-old"
            })
            .unwrap();
        assert_eq!(format!("{:?}", old_attempt.state), "Stopped");
        assert_eq!(
            old_attempt.stop_reason,
            Some(fluxon_fs_core::config::FluxonFsTransferWorkerStopReasonWire::Superseded)
        );

        let new_attempt = attempts
            .iter()
            .find(|attempt| {
                attempt.job_id == job.job_id && attempt.worker_task_id == "worker-task-new"
            })
            .unwrap();
        assert_eq!(format!("{:?}", new_attempt.state), "Launching");
        assert_eq!(new_attempt.stop_reason, None);
    }

    #[test]
    fn test_transfer_worker_result_stop_marks_superseded_attempt_stopped() {
        let st = test_transfer_state();
        let job = create_single_batch_transfer_job(&st, "dst", &[("a.bin", 123)], Vec::new());
        let worker_id = test_transfer_worker_id(job.job_id.as_str(), "batch-1");
        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            worker_id.as_str(),
            "worker-task-old",
            "dst-exporter-1",
            100,
        )
        .unwrap();

        st.reconcile_transfer_scheduler_state(101).unwrap();

        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            worker_id.as_str(),
            "worker-task-new",
            "dst-exporter-1",
            300,
        )
        .unwrap();

        let result_ack = st
            .apply_transfer_worker_result(
                &fluxon_fs_core::config::FluxonFsTransferWorkerResultWire {
                    job_id: job.job_id.clone(),
                    batch_id: "batch-1".to_string(),
                    worker_task_id: "worker-task-old".to_string(),
                    worker_id: worker_id.clone(),
                    file_results: vec![
                        fluxon_fs_core::config::FluxonFsTransferWorkerFileResultWire {
                            relpath: "a.bin".to_string(),
                            staging_relpath: ".fluxon.stage/job/batch/a.bin.fluxon.part"
                                .to_string(),
                            final_relpath: "a.bin".to_string(),
                            visible_size: 123,
                        },
                    ],
                    failed_file_results: Vec::new(),
                    collect_info_results: Vec::new(),
                    final_telemetry: None,
                },
            )
            .unwrap();
        assert!(!result_ack.accepted);
        assert_eq!(
            result_ack.stop_reason,
            Some(fluxon_fs_core::config::FluxonFsTransferWorkerStopReasonWire::Superseded)
        );

        let attempts = st.list_transfer_worker_attempt_records().unwrap();
        let old_attempt = attempts
            .iter()
            .find(|attempt| {
                attempt.job_id == job.job_id && attempt.worker_task_id == "worker-task-old"
            })
            .unwrap();
        assert_eq!(format!("{:?}", old_attempt.state), "Stopped");
        assert_eq!(
            old_attempt.stop_reason,
            Some(fluxon_fs_core::config::FluxonFsTransferWorkerStopReasonWire::Superseded)
        );

        let new_attempt = attempts
            .iter()
            .find(|attempt| {
                attempt.job_id == job.job_id && attempt.worker_task_id == "worker-task-new"
            })
            .unwrap();
        assert_eq!(format!("{:?}", new_attempt.state), "Launching");
        assert_eq!(new_attempt.stop_reason, None);
    }

    #[test]
    fn test_transfer_reconcile_stale_worker_requeue_marks_silent_superseded_attempt_stopped() {
        let st = test_transfer_state();
        let job = create_single_batch_transfer_job(&st, "dst", &[("a.bin", 123)], Vec::new());
        let worker_id = test_transfer_worker_id(job.job_id.as_str(), "batch-1");
        st.assign_transfer_batch_to_worker(
            job.job_id.as_str(),
            "batch-1",
            "src-exporter",
            worker_id.as_str(),
            "worker-task-old",
            "dst-exporter-1",
            100,
        )
        .unwrap();

        st.reconcile_transfer_scheduler_state(101).unwrap();

        let batch = st
            .list_transfer_batches()
            .unwrap()
            .into_iter()
            .find(|batch| batch.job_id == job.job_id && batch.batch_id == "batch-1")
            .unwrap();
        assert_eq!(batch.state, FluxonFsTransferBatchState::Ready);
        assert!(batch.owner_worker_id.is_empty());
        assert!(batch.owner_worker_task_id.is_empty());

        let attempts = st.list_transfer_worker_attempt_records().unwrap();
        let old_attempt = attempts
            .iter()
            .find(|attempt| {
                attempt.job_id == job.job_id && attempt.worker_task_id == "worker-task-old"
            })
            .unwrap();
        assert_eq!(format!("{:?}", old_attempt.state), "Stopped");
        assert_eq!(
            old_attempt.stop_reason,
            Some(fluxon_fs_core::config::FluxonFsTransferWorkerStopReasonWire::Superseded)
        );
    }

    #[test]
    fn test_access_model_json_text_reflects_persisted_runtime_updates() {
        let backend = Arc::new(MemBackend {
            data: Arc::new(Vec::new()),
            reqs: Arc::new(Mutex::new(Vec::new())),
            inflight: Arc::new(AtomicUsize::new(0)),
            max_inflight: Arc::new(AtomicUsize::new(0)),
        });
        let st = test_state_with_permission_list(
            backend,
            &["bucket-a", "bucket-b"],
            vec![FluxonFsS3PermissionAccount {
                username: "alice".to_string(),
                password: "pw-1".to_string(),
                permissions: vec![fluxon_fs_core::config::FluxonFsS3PermissionRule {
                    bucket: "bucket-a".to_string(),
                    prefix: "reports/".to_string(),
                    actions: vec![
                        FluxonFsS3PermissionAction::ListBucket,
                        FluxonFsS3PermissionAction::GetObject,
                    ],
                }],
            }],
        );

        let initial_json = st.access_model_json_text().unwrap();
        assert!(initial_json.contains("\"username\":\"alice\""));
        assert!(initial_json.contains("\"export_name\":\"bucket-a\""));
        assert!(initial_json.contains("\"prefix\":\"reports/\""));
        assert!(initial_json.contains("\"mode\":\"read\""));
        assert!(!initial_json.contains("pw-1"));
        assert!(initial_json.contains("\"rpc_token_secret_sha256_hex\":"));

        st.persist_permission_list_state(&[
            FluxonFsS3PermissionAccount {
                username: "alice".to_string(),
                password: "pw-1".to_string(),
                permissions: vec![fluxon_fs_core::config::FluxonFsS3PermissionRule {
                    bucket: "bucket-a".to_string(),
                    prefix: "reports/".to_string(),
                    actions: vec![
                        FluxonFsS3PermissionAction::ListBucket,
                        FluxonFsS3PermissionAction::GetObject,
                    ],
                }],
            },
            FluxonFsS3PermissionAccount {
                username: "bob".to_string(),
                password: "pw-2".to_string(),
                permissions: vec![fluxon_fs_core::config::FluxonFsS3PermissionRule {
                    bucket: "bucket-b".to_string(),
                    prefix: "ingest/".to_string(),
                    actions: vec![FluxonFsS3PermissionAction::All],
                }],
            },
        ])
        .unwrap();

        let updated_json = st.access_model_json_text().unwrap();
        assert!(updated_json.contains("\"username\":\"bob\""));
        assert!(updated_json.contains("\"export_name\":\"bucket-b\""));
        assert!(updated_json.contains("\"prefix\":\"ingest/\""));
        assert!(updated_json.contains("\"mode\":\"read_write\""));
        assert!(!updated_json.contains("pw-2"));
    }

    fn basic_auth_headers(username: &str, password: &str) -> HeaderMap {
        use axum::http::header::AUTHORIZATION;
        use base64::Engine as _;
        let raw = format!("{}:{}", username, password);
        let encoded = base64::engine::general_purpose::STANDARD.encode(raw.as_bytes());
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Basic {}", encoded)).unwrap(),
        );
        headers
    }

    #[test]
    fn test_ui_require_identity_admin_can_view_as() {
        let backend = Arc::new(ObjectBackend::default());
        let admin = FluxonFsS3PermissionAccount {
            username: "admin".to_string(),
            password: "admin_pw".to_string(),
            permissions: vec![fluxon_fs_core::config::FluxonFsS3PermissionRule {
                bucket: "*".to_string(),
                prefix: "".to_string(),
                actions: vec![FluxonFsS3PermissionAction::All],
            }],
        };
        let user = FluxonFsS3PermissionAccount {
            username: "u1".to_string(),
            password: "u1_pw".to_string(),
            permissions: vec![fluxon_fs_core::config::FluxonFsS3PermissionRule {
                bucket: "demo".to_string(),
                prefix: "".to_string(),
                actions: vec![
                    FluxonFsS3PermissionAction::ListBucket,
                    FluxonFsS3PermissionAction::GetObject,
                ],
            }],
        };
        let st = test_state_with_permission_list(backend, &["demo"], vec![admin, user]);
        let headers = basic_auth_headers("admin", "admin_pw");
        let identity = super::ui_require_identity(&headers, &st, Some("u1".to_string())).unwrap();
        assert_eq!(identity.viewer_username(), "admin");
        assert_eq!(identity.actor_username(), "u1");
        assert!(identity.is_impersonating());
    }

    #[test]
    fn test_ui_require_identity_non_admin_view_as_is_forbidden() {
        let backend = Arc::new(ObjectBackend::default());
        let admin = FluxonFsS3PermissionAccount {
            username: "admin".to_string(),
            password: "admin_pw".to_string(),
            permissions: vec![fluxon_fs_core::config::FluxonFsS3PermissionRule {
                bucket: "*".to_string(),
                prefix: "".to_string(),
                actions: vec![FluxonFsS3PermissionAction::All],
            }],
        };
        let user = FluxonFsS3PermissionAccount {
            username: "u1".to_string(),
            password: "u1_pw".to_string(),
            permissions: vec![fluxon_fs_core::config::FluxonFsS3PermissionRule {
                bucket: "demo".to_string(),
                prefix: "".to_string(),
                actions: vec![
                    FluxonFsS3PermissionAction::ListBucket,
                    FluxonFsS3PermissionAction::GetObject,
                ],
            }],
        };
        let st = test_state_with_permission_list(backend, &["demo"], vec![admin, user]);
        let headers = basic_auth_headers("u1", "u1_pw");
        let resp =
            super::ui_require_identity(&headers, &st, Some("admin".to_string())).unwrap_err();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn test_ui_require_identity_unknown_view_as_is_bad_request() {
        let backend = Arc::new(ObjectBackend::default());
        let admin = FluxonFsS3PermissionAccount {
            username: "admin".to_string(),
            password: "admin_pw".to_string(),
            permissions: vec![fluxon_fs_core::config::FluxonFsS3PermissionRule {
                bucket: "*".to_string(),
                prefix: "".to_string(),
                actions: vec![FluxonFsS3PermissionAction::All],
            }],
        };
        let st = test_state_with_permission_list(backend, &["demo"], vec![admin]);
        let headers = basic_auth_headers("admin", "admin_pw");
        let resp = super::ui_require_identity(&headers, &st, Some("no_such_user".to_string()))
            .unwrap_err();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_access_model_roundtrip_with_scope_access_and_manager_user() {
        let permission_list = vec![
            FluxonFsS3PermissionAccount {
                username: "admin".to_string(),
                password: "admin_pw".to_string(),
                permissions: vec![fluxon_fs_core::config::FluxonFsS3PermissionRule {
                    bucket: "*".to_string(),
                    prefix: "".to_string(),
                    actions: vec![FluxonFsS3PermissionAction::All],
                }],
            },
            FluxonFsS3PermissionAccount {
                username: "alice".to_string(),
                password: "alice_pw".to_string(),
                permissions: vec![fluxon_fs_core::config::FluxonFsS3PermissionRule {
                    bucket: "demo".to_string(),
                    prefix: "docs/".to_string(),
                    actions: vec![
                        FluxonFsS3PermissionAction::ListBucket,
                        FluxonFsS3PermissionAction::GetObject,
                    ],
                }],
            },
            FluxonFsS3PermissionAccount {
                username: "bob".to_string(),
                password: "bob_pw".to_string(),
                permissions: vec![
                    fluxon_fs_core::config::FluxonFsS3PermissionRule {
                        bucket: "demo".to_string(),
                        prefix: "docs/".to_string(),
                        actions: vec![
                            FluxonFsS3PermissionAction::ListBucket,
                            FluxonFsS3PermissionAction::GetObject,
                        ],
                    },
                    fluxon_fs_core::config::FluxonFsS3PermissionRule {
                        bucket: "demo".to_string(),
                        prefix: "uploads/".to_string(),
                        actions: vec![
                            FluxonFsS3PermissionAction::ListBucket,
                            FluxonFsS3PermissionAction::ListBucketMultipartUploads,
                            FluxonFsS3PermissionAction::ListMultipartUploadParts,
                            FluxonFsS3PermissionAction::GetObject,
                            FluxonFsS3PermissionAction::PutObject,
                            FluxonFsS3PermissionAction::DeleteObject,
                            FluxonFsS3PermissionAction::AbortMultipartUpload,
                        ],
                    },
                ],
            },
        ];

        let model = super::access_model_from_permission_list(&permission_list).unwrap();
        assert_eq!(
            model.users,
            vec![
                super::AccessUser {
                    username: "admin".to_string(),
                    password: "admin_pw".to_string(),
                    can_manage_users: true,
                },
                super::AccessUser {
                    username: "alice".to_string(),
                    password: "alice_pw".to_string(),
                    can_manage_users: false,
                },
                super::AccessUser {
                    username: "bob".to_string(),
                    password: "bob_pw".to_string(),
                    can_manage_users: false,
                },
            ]
        );
        assert_eq!(
            model.scope_access,
            vec![
                super::ScopeAccess {
                    export_name: "demo".to_string(),
                    prefix: "docs/".to_string(),
                    mode: super::ScopeAccessMode::Read,
                    usernames: vec!["alice".to_string(), "bob".to_string()],
                },
                super::ScopeAccess {
                    export_name: "demo".to_string(),
                    prefix: "uploads/".to_string(),
                    mode: super::ScopeAccessMode::ReadWrite,
                    usernames: vec!["bob".to_string()],
                },
            ]
        );

        let roundtrip_permission_list = super::permission_list_from_access_model(&model).unwrap();
        let roundtrip_model =
            super::access_model_from_permission_list(&roundtrip_permission_list).unwrap();
        assert_eq!(roundtrip_model, model);
    }

    #[test]
    fn test_bootstrap_access_model_does_not_expand_manager_scope_access() {
        let model = super::AccessModel {
            users: vec![super::AccessUser {
                username: "admin".to_string(),
                password: "admin_pw".to_string(),
                can_manage_users: true,
            }],
            scope_access: Vec::new(),
        };
        let permission_list = super::permission_list_from_access_model(&model).unwrap();
        let roundtrip_model = super::access_model_from_permission_list(&permission_list).unwrap();
        assert_eq!(roundtrip_model.users, model.users);
        assert!(roundtrip_model.scope_access.is_empty());
    }

    #[test]
    fn test_ui_parse_scope_users_form_accepts_single_username_value() {
        let form = super::ui_parse_scope_users_form(
            b"bucket=demo&prefix=docs%2F&mode=read&usernames=admin",
        )
        .unwrap();
        assert_eq!(form.bucket, "demo");
        assert_eq!(form.prefix, "docs/");
        assert_eq!(form.mode, "read");
        assert_eq!(form.usernames, Some(vec!["admin".to_string()]));
    }

    #[test]
    fn test_ui_parse_scope_users_form_collects_repeated_username_fields() {
        let form = super::ui_parse_scope_users_form(
            b"bucket=demo&prefix=docs%2F&mode=read_write&usernames=alice&usernames=bob",
        )
        .unwrap();
        assert_eq!(form.bucket, "demo");
        assert_eq!(form.prefix, "docs/");
        assert_eq!(form.mode, "read_write");
        assert_eq!(
            form.usernames,
            Some(vec!["alice".to_string(), "bob".to_string()])
        );
    }

    #[test]
    fn test_ui_browse_scope_allows_ancestors_and_blocks_unrelated_paths() {
        let account = super::AuthAccount {
            username: "alice".to_string(),
            password: "alice_pw".to_string(),
            can_manage_users: false,
            permissions: vec![fluxon_fs_core::config::FluxonFsS3PermissionRule {
                bucket: "demo".to_string(),
                prefix: "docs/private/".to_string(),
                actions: vec![
                    FluxonFsS3PermissionAction::ListBucket,
                    FluxonFsS3PermissionAction::GetObject,
                ],
            }],
        };

        assert!(super::account_has_ui_bucket_browse_access(&account, "demo"));
        assert!(super::account_can_browse_ui_prefix(&account, "demo", ""));
        assert!(super::account_can_browse_ui_prefix(
            &account, "demo", "docs/"
        ));
        assert!(super::account_can_browse_ui_prefix(
            &account,
            "demo",
            "docs/private/"
        ));
        assert!(!super::account_can_browse_ui_prefix(
            &account, "demo", "logs/"
        ));
        assert!(super::account_can_browse_ui_file(
            &account,
            "demo",
            "docs/private/a.txt"
        ));
        assert!(!super::account_can_browse_ui_file(
            &account,
            "demo",
            "docs/public/a.txt"
        ));
        assert!(!super::account_can_browse_ui_file(
            &account,
            "other",
            "docs/private/a.txt"
        ));
    }

    #[test]
    fn test_manager_account_has_manage_permission_without_data_permissions() {
        let account = super::AuthAccount::from_cfg(&FluxonFsS3PermissionAccount {
            username: "admin".to_string(),
            password: "admin_pw".to_string(),
            permissions: vec![fluxon_fs_core::config::FluxonFsS3PermissionRule {
                bucket: "*".to_string(),
                prefix: "".to_string(),
                actions: vec![FluxonFsS3PermissionAction::All],
            }],
        });

        assert!(super::account_can_manage_permissions(&account));
        assert!(super::account_has_ui_bucket_browse_access(&account, "demo"));
        assert!(account.permissions.is_empty());
    }

    #[test]
    fn test_list_permitted_buckets_includes_all_buckets_for_manager_without_scope_access() {
        let backend = Arc::new(ObjectBackend::default());
        let admin = FluxonFsS3PermissionAccount {
            username: "admin".to_string(),
            password: "admin_pw".to_string(),
            permissions: vec![fluxon_fs_core::config::FluxonFsS3PermissionRule {
                bucket: "*".to_string(),
                prefix: "".to_string(),
                actions: vec![FluxonFsS3PermissionAction::All],
            }],
        };
        let st = test_state_with_permission_list(backend, &["demo", "logs"], vec![admin]);
        let account = super::find_account_by_username(&st, "admin").unwrap();

        assert_eq!(
            super::list_permitted_buckets(&st, &account).unwrap(),
            vec!["demo".to_string(), "logs".to_string()]
        );
    }

    #[tokio::test]
    async fn test_external_request_from_nested_router_clears_outer_path_params() {
        let backend = Arc::new(MemBackend {
            data: Arc::new(Vec::new()),
            reqs: Arc::new(Mutex::new(Vec::new())),
            inflight: Arc::new(AtomicUsize::new(0)),
            max_inflight: Arc::new(AtomicUsize::new(0)),
        });
        let st = test_state_with_buckets_and_base_path(backend, &["fluxon-release"], "/fs_s3");
        let outer = Router::new().route(
            "/fs_s3/*path",
            any({
                let st = st.clone();
                move |req: Request<Body>| {
                    let st = st.clone();
                    async move { super::handle_external_request(st, req).await }
                }
            }),
        );

        let resp = outer
            .oneshot(
                Request::builder()
                    .uri("/fs_s3/fluxon-release")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_ui_bucket_browse_remote_access_denied_is_forbidden() {
        let st = test_state_with_buckets(Arc::new(ListDirAccessDeniedBackend), &["demo"]);
        let auth = basic_auth_headers("a", "b")
            .get(axum::http::header::AUTHORIZATION)
            .unwrap()
            .clone();
        let resp = super::build_router(st)
            .oneshot(
                Request::builder()
                    .uri("/ui/demo/")
                    .header(axum::http::header::AUTHORIZATION, auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        let body = hyper::body::to_bytes(resp.into_body()).await.unwrap();
        let body_text = String::from_utf8_lossy(&body);
        assert!(body_text.contains("permission denied"));
        assert!(body_text.contains("scope_access"));
    }

    #[tokio::test]
    async fn test_ui_bucket_browse_marks_transfer_disabled_when_backend_missing() {
        let st =
            test_state_with_buckets_without_transfer(Arc::new(ObjectBackend::default()), &["demo"]);
        let auth = basic_auth_headers("a", "b")
            .get(axum::http::header::AUTHORIZATION)
            .unwrap()
            .clone();
        let resp = super::build_router(st)
            .oneshot(
                Request::builder()
                    .uri("/ui/demo/")
                    .header(axum::http::header::AUTHORIZATION, auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = hyper::body::to_bytes(resp.into_body()).await.unwrap();
        let body_text = String::from_utf8_lossy(&body);
        assert!(body_text.contains("\"transfer_enabled\":false"));
        assert!(body_text.contains("Directory Transfer Unavailable"));
        assert!(body_text.contains("TiKV-backed transfer state store"));
        assert!(body_text.contains("Failed to read dragged item. Start the drag from a FluxonFS page tab, folder, or object row and drop it again."));
    }

    fn test_auth_ctx(st: &Arc<GatewayState>) -> super::AuthCtx {
        let account = super::AuthAccount::from_cfg(&st.permission_list.read()[0]);
        super::AuthCtx {
            username: account.username.clone(),
            secret_key: account.password.clone(),
            amz_date: "20260316T000000Z".to_string(),
            region: "us-east-1".to_string(),
            account,
        }
    }

    fn test_auth_ctx_for_username(st: &Arc<GatewayState>, username: &str) -> super::AuthCtx {
        let account = super::find_account_by_username(st, username).unwrap();
        super::AuthCtx {
            username: account.username.clone(),
            secret_key: account.password.clone(),
            amz_date: "20260316T000000Z".to_string(),
            region: "us-east-1".to_string(),
            account,
        }
    }

    #[tokio::test]
    async fn test_s3_list_bucket_allows_scope_ancestor_prefixes_and_blocks_unrelated_prefixes() {
        let backend = Arc::new(ObjectBackend::default());
        let alice = FluxonFsS3PermissionAccount {
            username: "alice".to_string(),
            password: "alice_pw".to_string(),
            permissions: vec![fluxon_fs_core::config::FluxonFsS3PermissionRule {
                bucket: "demo".to_string(),
                prefix: "docs/private/".to_string(),
                actions: vec![
                    FluxonFsS3PermissionAction::ListBucket,
                    FluxonFsS3PermissionAction::GetObject,
                ],
            }],
        };
        let st = test_state_with_permission_list(backend, &["demo"], vec![alice]);
        let auth_ctx = test_auth_ctx_for_username(&st, "alice");

        let root_resp = super::handle_any_authed_impl(
            st.clone(),
            auth_ctx.clone(),
            Method::GET,
            "demo".to_string(),
            "/demo?list-type=2&prefix=&delimiter=%2F".parse().unwrap(),
            HeaderMap::new(),
            Body::empty(),
        )
        .await
        .unwrap();
        assert_eq!(root_resp.status(), StatusCode::OK);

        let ancestor_resp = super::handle_any_authed_impl(
            st.clone(),
            auth_ctx.clone(),
            Method::GET,
            "demo".to_string(),
            "/demo?list-type=2&prefix=docs%2F&delimiter=%2F"
                .parse()
                .unwrap(),
            HeaderMap::new(),
            Body::empty(),
        )
        .await
        .unwrap();
        assert_eq!(ancestor_resp.status(), StatusCode::OK);

        let exact_resp = super::handle_any_authed_impl(
            st.clone(),
            auth_ctx.clone(),
            Method::GET,
            "demo".to_string(),
            "/demo?list-type=2&prefix=docs%2Fprivate%2F&delimiter=%2F"
                .parse()
                .unwrap(),
            HeaderMap::new(),
            Body::empty(),
        )
        .await
        .unwrap();
        assert_eq!(exact_resp.status(), StatusCode::OK);

        let err = super::handle_any_authed_impl(
            st,
            auth_ctx,
            Method::GET,
            "demo".to_string(),
            "/demo?list-type=2&prefix=logs%2F&delimiter=%2F"
                .parse()
                .unwrap(),
            HeaderMap::new(),
            Body::empty(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, super::S3Error::AccessDenied { .. }));
    }

    #[tokio::test]
    async fn test_multipart_upload_id_is_bound_to_original_key() {
        let backend = ObjectBackend::default();
        let st = test_state_with_buckets(Arc::new(backend), &["demo"]);
        let request_identity = test_request_identity();
        let upload_id = super::multipart_create_upload_id(
            &st,
            request_identity.clone(),
            Arc::from("demo"),
            "docs/good.bin",
        )
        .await
        .unwrap();

        let err = super::multipart_upload_part(
            &st,
            request_identity.clone(),
            Arc::from("demo"),
            "docs/other.bin",
            &upload_id,
            1,
            Body::from("hello"),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, super::S3Error::InvalidRequest { detail } if detail.contains("multipart upload key mismatch"))
        );

        let err = super::list_parts_xml(
            &st,
            request_identity.clone(),
            Arc::from("demo"),
            "docs/other.bin",
            &upload_id,
            &BTreeMap::new(),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, super::S3Error::InvalidRequest { detail } if detail.contains("multipart upload key mismatch"))
        );

        let err = super::multipart_complete(
            &st,
            request_identity.clone(),
            Arc::from("demo"),
            "docs/other.bin",
            &upload_id,
            Body::from("<CompleteMultipartUpload></CompleteMultipartUpload>"),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, super::S3Error::InvalidRequest { detail } if detail.contains("multipart upload key mismatch"))
        );

        let err = super::multipart_abort(
            &st,
            request_identity,
            Arc::from("demo"),
            "docs/other.bin",
            &upload_id,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, super::S3Error::InvalidRequest { detail } if detail.contains("multipart upload key mismatch"))
        );
    }

    #[tokio::test]
    async fn test_bucket_root_put_is_noop_success_for_existing_export() {
        let backend = Arc::new(ObjectBackend::default());
        let st = test_state_with_buckets(backend, &["deployer-runtime-infra44"]);
        let resp = super::handle_any_authed_impl(
            st.clone(),
            test_auth_ctx(&st),
            Method::PUT,
            "deployer-runtime-infra44".to_string(),
            "/deployer-runtime-infra44".parse().unwrap(),
            HeaderMap::new(),
            Body::empty(),
        )
        .await
        .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_manager_can_access_bucket_without_explicit_scope_access() {
        let backend = Arc::new(ObjectBackend::default());
        backend.insert("demo", "hello.txt", b"hello");
        let admin = FluxonFsS3PermissionAccount {
            username: "admin".to_string(),
            password: "admin_pw".to_string(),
            permissions: vec![fluxon_fs_core::config::FluxonFsS3PermissionRule {
                bucket: "*".to_string(),
                prefix: "".to_string(),
                actions: vec![FluxonFsS3PermissionAction::All],
            }],
        };
        let st = test_state_with_permission_list(backend, &["demo", "logs"], vec![admin]);
        let auth_ctx = test_auth_ctx(&st);

        let list_resp = super::handle_any_authed_impl(
            st.clone(),
            auth_ctx.clone(),
            Method::GET,
            "demo".to_string(),
            "/demo?list-type=2&prefix=&delimiter=%2F".parse().unwrap(),
            HeaderMap::new(),
            Body::empty(),
        )
        .await
        .unwrap();
        assert_eq!(list_resp.status(), StatusCode::OK);

        let get_resp = super::handle_any_authed_impl(
            st,
            auth_ctx,
            Method::GET,
            "demo/hello.txt".to_string(),
            "/demo/hello.txt".parse().unwrap(),
            HeaderMap::new(),
            Body::empty(),
        )
        .await
        .unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_put_object_after_bucket_root_put_writes_object() {
        let backend = Arc::new(ObjectBackend::default());
        let st = test_state_with_buckets(backend.clone(), &["deployer-runtime-infra44"]);

        let bucket_resp = super::handle_any_authed_impl(
            st.clone(),
            test_auth_ctx(&st),
            Method::PUT,
            "deployer-runtime-infra44".to_string(),
            "/deployer-runtime-infra44".parse().unwrap(),
            HeaderMap::new(),
            Body::empty(),
        )
        .await
        .unwrap();
        assert_eq!(bucket_resp.status(), StatusCode::OK);

        let object_resp = super::handle_any_authed_impl(
            st.clone(),
            test_auth_ctx(&st),
            Method::PUT,
            "deployer-runtime-infra44/download/rclone-e2e.txt".to_string(),
            "/deployer-runtime-infra44/download/rclone-e2e.txt"
                .parse()
                .unwrap(),
            HeaderMap::new(),
            Body::from("fluxon-rclone-e2e\n"),
        )
        .await
        .unwrap();

        assert_eq!(object_resp.status(), StatusCode::OK);
        assert_eq!(
            backend
                .get("deployer-runtime-infra44", "download/rclone-e2e.txt")
                .unwrap(),
            b"fluxon-rclone-e2e\n".to_vec()
        );
    }

    #[tokio::test]
    async fn test_get_object_stream_parallel_and_ordered() {
        let size: usize = (FS_RPC_CHUNK_BYTES * 2) + 123;
        let mut data = vec![0u8; size];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }

        let backend = MemBackend {
            data: Arc::new(data.clone()),
            reqs: Arc::new(Mutex::new(Vec::new())),
            inflight: Arc::new(AtomicUsize::new(0)),
            max_inflight: Arc::new(AtomicUsize::new(0)),
        };

        let fs_cache = FluxonFsGlobalConfig {
            stale_window_ms: 0,
            rules: Vec::new(),
            exports: BTreeMap::new(),
        };
        let st = GatewayState::new(
            "test_cluster".to_string(),
            String::new(),
            test_gateway_access_config(),
            Arc::new(fs_cache),
            FluxonFsS3GatewayConfig {
                get_object_inflight_pieces: 4,
                kv_miss_policy: FluxonFsS3KvMissPolicy::RemoteRead,
            },
            Arc::new(backend.clone()),
            Arc::new(TestFsMasterAdminBackend),
            None,
            None,
        )
        .unwrap();
        st.persist_permission_list_state(&[FluxonFsS3PermissionAccount {
            username: "a".to_string(),
            password: "b".to_string(),
            permissions: vec![fluxon_fs_core::config::FluxonFsS3PermissionRule {
                bucket: "*".to_string(),
                prefix: "".to_string(),
                actions: vec![FluxonFsS3PermissionAction::All],
            }],
        }])
        .unwrap();
        let st = Arc::new(st);

        // Span multiple pieces with an unaligned start.
        let start: i64 = 123;
        let end_inclusive: i64 = (FS_RPC_CHUNK_BYTES as i64) + 42;
        let expected = data[start as usize..=(end_inclusive as usize)].to_vec();

        let mut body = super::get_object_stream(
            st,
            test_request_identity(),
            "bucket".into(),
            "rel".into(),
            size as i64,
            1,
            Some(start),
            Some(end_inclusive),
        )
        .await
        .unwrap();

        let mut out: Vec<u8> = Vec::new();
        while let Some(chunk) = body.data().await {
            let chunk = chunk.unwrap();
            out.extend_from_slice(&chunk);
        }

        assert_eq!(out, expected);

        let reqs = backend.reqs.lock().clone();
        assert!(reqs.len() >= 2);
        for (off, n) in reqs {
            assert!(n > 0);
            assert!(n as usize <= FS_RPC_CHUNK_BYTES);
            let off_in_piece = (off % (FS_RPC_CHUNK_BYTES as i64)) as i64;
            assert!(off_in_piece + n <= FS_RPC_CHUNK_BYTES as i64);
        }

        let max_inflight = backend.max_inflight.load(Ordering::SeqCst);
        assert!(max_inflight >= 2);
        assert!(max_inflight <= 4);
    }

    #[tokio::test]
    async fn test_ui_copy_or_move_object_cross_bucket_copy() {
        let backend = ObjectBackend::default();
        backend.insert("src", "alpha.txt", b"alpha-data");
        let st = test_state_with_buckets(Arc::new(backend.clone()), &["src", "dst"]);
        let account = super::AuthAccount::from_cfg(&st.permission_list.read()[0]);

        let result = super::ui_copy_or_move_object_impl(
            &st,
            &account,
            "src",
            "alpha.txt".to_string(),
            "dst".to_string(),
            "".to_string(),
            false,
        )
        .await
        .unwrap();

        assert_eq!(result.bucket, "dst");
        assert_eq!(result.key, "alpha.txt");
        assert_eq!(
            backend.get("src", "alpha.txt").unwrap(),
            b"alpha-data".to_vec()
        );
        assert_eq!(
            backend.get("dst", "alpha.txt").unwrap(),
            b"alpha-data".to_vec()
        );
    }

    #[tokio::test]
    async fn test_ui_copy_or_move_object_cross_bucket_move_removes_source() {
        let backend = ObjectBackend::default();
        backend.insert("src", "alpha.txt", b"alpha-data");
        let st = test_state_with_buckets(Arc::new(backend.clone()), &["src", "dst"]);
        let account = super::AuthAccount::from_cfg(&st.permission_list.read()[0]);

        let result = super::ui_copy_or_move_object_impl(
            &st,
            &account,
            "src",
            "alpha.txt".to_string(),
            "dst".to_string(),
            "".to_string(),
            true,
        )
        .await
        .unwrap();

        assert_eq!(result.bucket, "dst");
        assert_eq!(result.key, "alpha.txt");
        assert!(backend.get("src", "alpha.txt").is_none());
        assert_eq!(
            backend.get("dst", "alpha.txt").unwrap(),
            b"alpha-data".to_vec()
        );
        assert_eq!(backend.rename_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_ui_copy_or_move_object_same_bucket_move_uses_rename() {
        let backend = ObjectBackend::default();
        backend.insert("src", "folder/alpha.txt", b"alpha-data");
        let st = test_state_with_buckets(Arc::new(backend.clone()), &["src"]);
        let account = super::AuthAccount::from_cfg(&st.permission_list.read()[0]);

        let result = super::ui_copy_or_move_object_impl(
            &st,
            &account,
            "src",
            "folder/alpha.txt".to_string(),
            "src".to_string(),
            "next/".to_string(),
            true,
        )
        .await
        .unwrap();

        assert_eq!(result.bucket, "src");
        assert_eq!(result.key, "next/alpha.txt");
        assert!(backend.get("src", "folder/alpha.txt").is_none());
        assert_eq!(
            backend.get("src", "next/alpha.txt").unwrap(),
            b"alpha-data".to_vec()
        );
        assert_eq!(backend.rename_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_ui_start_copy_or_move_task_reports_progress_until_done() {
        let backend = SlowObjectBackend::new(Duration::from_millis(20));
        let data = vec![7u8; (FS_RPC_CHUNK_BYTES * 2) + 17];
        backend.insert("src", "alpha.bin", &data);
        let st = test_state_with_buckets(Arc::new(backend.clone()), &["src", "dst"]);
        let account = super::AuthAccount::from_cfg(&st.permission_list.read()[0]);

        let mut snapshot = super::ui_start_copy_or_move_task(
            &st,
            &account,
            "src",
            "alpha.bin".to_string(),
            "dst".to_string(),
            "".to_string(),
            false,
        )
        .await
        .unwrap();

        assert_eq!(snapshot.kind, super::UiTransferTaskKind::Copy);
        assert_eq!(snapshot.total_bytes, data.len() as i64);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let mut saw_inflight_progress =
            snapshot.done_bytes > 0 && snapshot.done_bytes < snapshot.total_bytes;
        while snapshot.stage == super::UiTransferTaskStage::Running
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(25)).await;
            snapshot = st
                .ui_transfer_task_for_owner(&snapshot.task_id, &account.username)
                .unwrap()
                .snapshot();
            if snapshot.done_bytes > 0 && snapshot.done_bytes < snapshot.total_bytes {
                saw_inflight_progress = true;
            }
        }

        assert_eq!(snapshot.stage, super::UiTransferTaskStage::Done);
        assert!(saw_inflight_progress);
        assert_eq!(snapshot.done_bytes, data.len() as i64);
        assert_eq!(snapshot.source.bucket, "src");
        assert_eq!(snapshot.source.key, "alpha.bin");
        assert_eq!(snapshot.target.bucket, "dst");
        assert_eq!(snapshot.target.key, "alpha.bin");
        assert_eq!(backend.get("dst", "alpha.bin").unwrap(), data);
    }

    #[tokio::test]
    async fn test_ui_start_copy_or_move_task_pause_and_resume() {
        let backend = SlowObjectBackend::new(Duration::from_millis(20));
        let data = vec![9u8; (FS_RPC_CHUNK_BYTES * 3) + 11];
        backend.insert("src", "alpha.bin", &data);
        let st = test_state_with_buckets(Arc::new(backend.clone()), &["src", "dst"]);
        let account = super::AuthAccount::from_cfg(&st.permission_list.read()[0]);

        let mut snapshot = super::ui_start_copy_or_move_task(
            &st,
            &account,
            "src",
            "alpha.bin".to_string(),
            "dst".to_string(),
            "".to_string(),
            false,
        )
        .await
        .unwrap();

        let progress_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while snapshot.done_bytes == 0 && tokio::time::Instant::now() < progress_deadline {
            tokio::time::sleep(Duration::from_millis(25)).await;
            snapshot = st
                .ui_transfer_task_for_owner(&snapshot.task_id, &account.username)
                .unwrap()
                .snapshot();
        }
        assert!(snapshot.done_bytes > 0);

        let task = st
            .ui_transfer_task_for_owner(&snapshot.task_id, &account.username)
            .unwrap();
        snapshot = task.request_pause();
        let pause_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while snapshot.stage != super::UiTransferTaskStage::Paused
            && tokio::time::Instant::now() < pause_deadline
        {
            tokio::time::sleep(Duration::from_millis(25)).await;
            snapshot = task.snapshot();
        }
        assert_eq!(snapshot.stage, super::UiTransferTaskStage::Paused);
        let paused_done_bytes = snapshot.done_bytes;

        tokio::time::sleep(Duration::from_millis(120)).await;
        snapshot = task.snapshot();
        assert_eq!(snapshot.stage, super::UiTransferTaskStage::Paused);
        assert_eq!(snapshot.done_bytes, paused_done_bytes);

        snapshot = task.request_resume();
        let done_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while snapshot.stage != super::UiTransferTaskStage::Done
            && tokio::time::Instant::now() < done_deadline
        {
            tokio::time::sleep(Duration::from_millis(25)).await;
            snapshot = task.snapshot();
        }
        assert_eq!(snapshot.stage, super::UiTransferTaskStage::Done);
        assert_eq!(backend.get("dst", "alpha.bin").unwrap(), data);
    }

    #[tokio::test]
    async fn test_ui_start_copy_or_move_task_cancel_keeps_source_and_removes_destination() {
        let backend = SlowObjectBackend::new(Duration::from_millis(20));
        let data = vec![5u8; (FS_RPC_CHUNK_BYTES * 3) + 23];
        backend.insert("src", "alpha.bin", &data);
        let st = test_state_with_buckets(Arc::new(backend.clone()), &["src", "dst"]);
        let account = super::AuthAccount::from_cfg(&st.permission_list.read()[0]);

        let mut snapshot = super::ui_start_copy_or_move_task(
            &st,
            &account,
            "src",
            "alpha.bin".to_string(),
            "dst".to_string(),
            "".to_string(),
            true,
        )
        .await
        .unwrap();

        let progress_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while snapshot.done_bytes == 0 && tokio::time::Instant::now() < progress_deadline {
            tokio::time::sleep(Duration::from_millis(25)).await;
            snapshot = st
                .ui_transfer_task_for_owner(&snapshot.task_id, &account.username)
                .unwrap()
                .snapshot();
        }
        assert!(snapshot.done_bytes > 0);

        let task = st
            .ui_transfer_task_for_owner(&snapshot.task_id, &account.username)
            .unwrap();
        snapshot = task.request_cancel();
        let cancel_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while snapshot.stage != super::UiTransferTaskStage::Cancelled
            && snapshot.stage != super::UiTransferTaskStage::Error
            && tokio::time::Instant::now() < cancel_deadline
        {
            tokio::time::sleep(Duration::from_millis(25)).await;
            snapshot = task.snapshot();
        }
        assert_eq!(snapshot.stage, super::UiTransferTaskStage::Cancelled);
        assert_eq!(backend.get("src", "alpha.bin").unwrap(), data);
        assert!(backend.get("dst", "alpha.bin").is_none());
    }

    #[tokio::test]
    async fn test_list_objects_v2_accepts_empty_delimiter_for_recursive_listing() {
        let backend = ObjectBackend::default();
        backend.insert("fluxon-release", "fluxon_release.sha256", b"sha-data");
        backend.insert("fluxon-release", "profiles/dev/config.yaml", b"cfg-data");
        let st = test_state_with_buckets(Arc::new(backend), &["fluxon-release"]);

        let mut params = BTreeMap::new();
        params.insert("delimiter".to_string(), "".to_string());
        params.insert("prefix".to_string(), "".to_string());
        params.insert("max-keys".to_string(), "1000".to_string());

        let xml = super::list_objects_v2_xml(
            &st,
            test_request_identity(),
            Arc::from("fluxon-release"),
            &params,
        )
        .await
        .unwrap();

        assert!(xml.contains("<Key>fluxon_release.sha256</Key>"));
        assert!(xml.contains("<Key>profiles/dev/config.yaml</Key>"));
        assert!(!xml.contains("<CommonPrefixes>"));
        assert!(xml.contains("<Delimiter></Delimiter>"));
    }
}
