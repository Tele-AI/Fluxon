use std::collections::BTreeSet;
use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use chrono::Utc;
use fluxon_framework_compiled::shutdown::ViewShutdownExt;
use fluxon_fs_core::config::{
    FS_AGENT_TRANSFER_SCAN_RPC_PATH, FS_AGENT_TRANSFER_WORKER_RPC_PATH, FluxonFsTransferBatchKind,
    FluxonFsTransferBatchState, FluxonFsTransferDispositionWire, FluxonFsTransferJobState,
    FluxonFsTransferManifestWire, FluxonFsTransferScanAssignmentWire,
    FluxonFsTransferScanChildUnitWire, FluxonFsTransferScanEventAckWire,
    FluxonFsTransferScanEventKindWire, FluxonFsTransferScanEventWire,
    FluxonFsTransferScanLaunchDispositionWire, FluxonFsTransferScanLaunchResultWire,
    FluxonFsTransferScanMode, FluxonFsTransferScanResultWire, FluxonFsTransferSkipEntryWire,
    FluxonFsTransferWorkerAssignmentWire, FluxonFsTransferWorkerHeartbeatWire,
    FluxonFsTransferWorkerLaunchDispositionWire, FluxonFsTransferWorkerLaunchResultWire,
    FluxonFsTransferWorkerResultWire, decode_transfer_job_spec,
    extract_cache_config_yaml_from_yaml_text, parse_cache_config_yaml,
    parse_master_config_from_yaml_text, parse_master_panel_config_from_yaml_text,
};
use fluxon_fs_s3_gateway::FsTransferReadyBatchClass;
use fluxon_kv::user_api::FluxonUserApi;
use fluxon_kv::user_api::flat_dict::{FlatDict, FlatValue};
use fluxon_util::run_async_from_sync::spawn_blocking_allow_sync_async_bridge;
use parking_lot::Mutex;
use tracing::{info, warn};
use uuid::Uuid;

use super::{
    TRANSFER_CONTROL_RPC_TIMEOUT_MS, TRANSFER_HEARTBEAT_EXTENSION_MS,
    TRANSFER_SCHEDULER_IDLE_SLEEP_MS, TRANSFER_WORKER_LEASE_MS,
    extract_kvclient_config_yaml_from_fluxon_config,
};

fn list_online_export_agents(
    s3_state: &fluxon_fs_s3_gateway::GatewayState,
    export_name: &str,
) -> Result<Vec<String>, String> {
    let rows = s3_state.list_fs_export_registry_records()?;
    let mut agent_ids: Vec<String> = rows
        .into_iter()
        .filter(|row| row.export_name == export_name)
        .map(|row| row.agent_instance_key)
        .collect();
    agent_ids.sort();
    agent_ids.dedup();
    Ok(agent_ids)
}

fn stable_transfer_agent_index(placement_key: &str, agent_count: usize) -> Option<usize> {
    if agent_count == 0 {
        return None;
    }
    let mut hash = 14695981039346656037_u64;
    for byte in placement_key.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(1099511628211_u64);
    }
    Some((hash % agent_count as u64) as usize)
}

// Transfer ownership stays durable-state authoritative. Agent selection only
// chooses which online endpoint handles one scan unit or one batch attempt.
fn choose_online_export_agent(agent_ids: &[String], placement_key: &str) -> Option<String> {
    stable_transfer_agent_index(placement_key, agent_ids.len())
        .and_then(|index| agent_ids.get(index).cloned())
}

fn transfer_scan_unit_placement_key(
    job_id: &str,
    scan_epoch: i64,
    scan_unit: &MasterScanUnit,
) -> String {
    format!(
        "scan:{}:{}:{}:{}",
        job_id, scan_epoch, scan_unit.root_relpath, scan_unit.generation
    )
}

fn transfer_batch_placement_key(
    direction: &str,
    export_name: &str,
    job_id: &str,
    batch_id: &str,
) -> String {
    format!("{}:{}:{}:{}", direction, export_name, job_id, batch_id)
}

async fn call_transfer_scan_launch_rpc(
    api: Arc<FluxonUserApi>,
    agent_instance_key: &str,
    assignment: &FluxonFsTransferScanAssignmentWire,
) -> Result<FluxonFsTransferScanLaunchResultWire, String> {
    let payload_json = serde_json::to_string(assignment)
        .map_err(|e| format!("serialize transfer scan assignment failed: {}", e))?;
    let agent_instance_key = agent_instance_key.to_string();
    let payload: FlatDict = FlatDict::from([(
        "assignment_json".to_string(),
        FlatValue::String(payload_json),
    )]);
    let api2 = api.clone();
    let agent_instance_key2 = agent_instance_key.clone();
    let resp = spawn_blocking_allow_sync_async_bridge(move || {
        api2.rpc_client().call(
            agent_instance_key2.as_str(),
            FS_AGENT_TRANSFER_SCAN_RPC_PATH,
            payload,
            Some(TRANSFER_CONTROL_RPC_TIMEOUT_MS),
        )
    })
    .await
    .map_err(|e| {
        format!(
            "transfer scan rpc join failed: agent={} err={}",
            agent_instance_key, e
        )
    })?
    .map_err(|e| {
        format!(
            "transfer scan rpc failed: agent={} err={}",
            agent_instance_key, e
        )
    })?;
    let result_json = match resp.get("launch_result_json") {
        Some(FlatValue::String(v)) if !v.trim().is_empty() => v.clone(),
        _ => {
            return Err(format!(
                "transfer scan rpc missing launch_result_json: agent={}",
                agent_instance_key
            ));
        }
    };
    serde_json::from_str(&result_json).map_err(|e| {
        format!(
            "parse transfer scan launch result failed: agent={} err={}",
            agent_instance_key, e
        )
    })
}

async fn call_transfer_worker_launch_rpc(
    api: Arc<FluxonUserApi>,
    agent_instance_key: &str,
    assignment: &FluxonFsTransferWorkerAssignmentWire,
) -> Result<FluxonFsTransferWorkerLaunchResultWire, String> {
    let payload_json = serde_json::to_string(assignment)
        .map_err(|e| format!("serialize transfer worker assignment failed: {}", e))?;
    let agent_instance_key = agent_instance_key.to_string();
    let payload: FlatDict = FlatDict::from([(
        "assignment_json".to_string(),
        FlatValue::String(payload_json),
    )]);
    let api2 = api.clone();
    let agent_instance_key2 = agent_instance_key.clone();
    let resp = spawn_blocking_allow_sync_async_bridge(move || {
        api2.rpc_client().call(
            agent_instance_key2.as_str(),
            FS_AGENT_TRANSFER_WORKER_RPC_PATH,
            payload,
            Some(TRANSFER_CONTROL_RPC_TIMEOUT_MS),
        )
    })
    .await
    .map_err(|e| {
        format!(
            "transfer worker rpc join failed: agent={} err={}",
            agent_instance_key, e
        )
    })?
    .map_err(|e| {
        format!(
            "transfer worker rpc failed: agent={} err={}",
            agent_instance_key, e
        )
    })?;
    let result_json = match resp.get("launch_result_json") {
        Some(FlatValue::String(v)) if !v.trim().is_empty() => v.clone(),
        _ => {
            return Err(format!(
                "transfer worker rpc missing launch_result_json: agent={}",
                agent_instance_key
            ));
        }
    };
    serde_json::from_str(&result_json).map_err(|e| {
        format!(
            "parse transfer worker launch result failed: agent={} err={}",
            agent_instance_key, e
        )
    })
}

// Worker ownership is batch-scoped. Concurrency is controlled by the number of
// running batches, not by hashing batches into synthetic worker slots.
fn transfer_batch_worker_id(job_id: &str, batch_id: &str) -> String {
    format!("{}__batch_{}", job_id, batch_id)
}

fn transfer_dispatch_capacity(running_batch_count: usize, desired_worker_count: i64) -> usize {
    desired_worker_count
        .saturating_sub(running_batch_count as i64)
        .max(0) as usize
}

// A scan assignment must not expire before the control RPC itself times out.
// Otherwise the master can mark a still-running scan stale and drop a valid
// result that returns before the RPC deadline.
const TRANSFER_SCAN_ASSIGNMENT_TIMEOUT_GRACE_MS: i64 = 30_000;
const TRANSFER_SCAN_ASSIGNMENT_TIMEOUT_MS: i64 =
    (TRANSFER_CONTROL_RPC_TIMEOUT_MS as i64) + TRANSFER_SCAN_ASSIGNMENT_TIMEOUT_GRACE_MS;
const TRANSFER_RECONCILE_INTERVAL_MS: u64 = 5_000;
const TRANSFER_SCAN_CONCURRENCY_SYSTEM_LIMIT: usize = 128;
const TRANSFER_SCAN_FINALIZE_RETRY_DELAY_MS: i64 = 1_000;

#[derive(Debug, Clone)]
struct MasterScanUnit {
    scan_unit_id: Option<String>,
    root_relpath: String,
    generation: i64,
    scan_mode: FluxonFsTransferScanMode,
}

#[derive(Debug, Clone)]
struct MasterInflightScan {
    assignment: FluxonFsTransferScanAssignmentWire,
    started_unix_ms: i64,
    last_accepted_event_seq_no: i64,
}

#[derive(Debug, Clone)]
struct MasterPendingScanFinalize {
    result: FluxonFsTransferScanResultWire,
    next_retry_unix_ms: i64,
}

#[derive(Default)]
pub(crate) struct TransferScanRuntimeState {
    jobs: BTreeMap<String, TransferJobScanRuntime>,
}

#[derive(Debug, Default)]
struct TransferJobScanRuntime {
    active_scan_epoch: i64,
    queue: VecDeque<MasterScanUnit>,
    inflight: BTreeMap<String, MasterInflightScan>,
    pending_finalize: BTreeMap<String, MasterPendingScanFinalize>,
    scan_finished_durable: bool,
}

impl TransferJobScanRuntime {
    fn reset_for_new_epoch(&mut self, scan_epoch: i64, root_relpath: String) {
        self.active_scan_epoch = scan_epoch;
        self.queue.clear();
        self.inflight.clear();
        self.pending_finalize.clear();
        self.scan_finished_durable = false;
        self.queue.push_back(MasterScanUnit {
            scan_unit_id: None,
            root_relpath,
            generation: 1,
            scan_mode: FluxonFsTransferScanMode::RootDirectFanoutOnly,
        });
    }

    fn enqueue_child_unit(&mut self, child: FluxonFsTransferScanChildUnitWire) {
        self.queue.push_front(MasterScanUnit {
            scan_unit_id: Some(child.scan_unit_id),
            root_relpath: child.root_relpath,
            generation: child.generation,
            scan_mode: child.scan_mode,
        });
    }

    fn enqueue_child_units(&mut self, child_scan_units: Vec<FluxonFsTransferScanChildUnitWire>) {
        for child in child_scan_units.into_iter().rev() {
            self.enqueue_child_unit(child);
        }
    }
}

// Scan restart does not persist scan units. Instead, the master ships already
// durable committed coverage for the subtree so the source agent can suppress
// duplicate batch emission even after timeout-driven generation bumps.
fn build_known_dispositions_for_scan(
    job_id: &str,
    root_relpath: &str,
    generation: i64,
    batches: &[fluxon_fs_s3_gateway::FsTransferBatchRecord],
    direct_files_complete_records: &[fluxon_fs_s3_gateway::FsTransferDirectFilesCompleteRecord],
) -> Vec<FluxonFsTransferDispositionWire> {
    let mut known: Vec<FluxonFsTransferDispositionWire> = batches
        .iter()
        .filter(|batch| {
            if batch.job_id != job_id {
                return false;
            }
            if batch.batch_kind == FluxonFsTransferBatchKind::SubtreeSlice {
                // Streaming execution slices are durable work units, not
                // durable full-subtree coverage markers for restart planning.
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

fn load_job_skip_entries(
    job: &fluxon_fs_s3_gateway::FsTransferJobRecord,
) -> Result<Vec<FluxonFsTransferSkipEntryWire>, String> {
    decode_transfer_job_spec(job.job_spec_blob.as_slice()).map(|spec| spec.skip_entries)
}

// Launch retries must stop once durable ownership moved away from this worker
// attempt. The store remains the authority even while the RPC is being retried.
fn transfer_batch_owner_matches_assignment(
    s3_state: &fluxon_fs_s3_gateway::GatewayState,
    assignment: &FluxonFsTransferWorkerAssignmentWire,
) -> Result<bool, String> {
    Ok(s3_state
        .transfer_batch_record(assignment.job_id.as_str(), assignment.batch_id.as_str())?
        .is_some_and(|batch| {
            batch.state == FluxonFsTransferBatchState::Running
                && batch.owner_worker_id == assignment.worker_id
                && batch.owner_worker_task_id == assignment.worker_task_id
        }))
}

// Launch RPC is intentionally short-lived: it only asks the destination agent
// to start the worker thread for this attempt. The real execution lifetime is
// governed later by heartbeat/result RPCs and durable lease decisions.
async fn launch_transfer_worker_until_ack_or_superseded(
    api: Arc<FluxonUserApi>,
    s3_state: Arc<fluxon_fs_s3_gateway::GatewayState>,
    dst_exporter_id: String,
    assignment: FluxonFsTransferWorkerAssignmentWire,
) -> Result<(), String> {
    let mut attempt: u32 = 0;
    loop {
        if !transfer_batch_owner_matches_assignment(s3_state.as_ref(), &assignment)? {
            return Ok(());
        }
        match call_transfer_worker_launch_rpc(api.clone(), dst_exporter_id.as_str(), &assignment)
            .await
        {
            Ok(launch_result) => {
                let now_unix_ms = Utc::now().timestamp_millis();
                match launch_result.disposition {
                    FluxonFsTransferWorkerLaunchDispositionWire::Started
                    | FluxonFsTransferWorkerLaunchDispositionWire::AlreadyRunning
                    | FluxonFsTransferWorkerLaunchDispositionWire::AlreadyCompleted => {
                        s3_state.mark_transfer_worker_launch_acknowledged(
                            assignment.job_id.as_str(),
                            assignment.batch_id.as_str(),
                            assignment.worker_id.as_str(),
                            assignment.worker_task_id.as_str(),
                            now_unix_ms,
                        )?;
                    }
                }
                return Ok(());
            }
            Err(err) => {
                attempt = attempt.saturating_add(1);
                let now_unix_ms = Utc::now().timestamp_millis();
                s3_state.record_transfer_worker_launch_retry(
                    assignment.job_id.as_str(),
                    assignment.batch_id.as_str(),
                    assignment.worker_id.as_str(),
                    assignment.worker_task_id.as_str(),
                    err.as_str(),
                    now_unix_ms,
                )?;
                tracing::warn!(
                    "transfer worker launch rpc retry: job_id={} batch_id={} worker_id={} worker_task_id={} dst_exporter_id={} attempt={} err={}",
                    assignment.job_id,
                    assignment.batch_id,
                    assignment.worker_id,
                    assignment.worker_task_id,
                    dst_exporter_id,
                    attempt,
                    err
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

fn scan_assignment_matches_live(
    runtime_state: &Arc<Mutex<TransferScanRuntimeState>>,
    assignment: &FluxonFsTransferScanAssignmentWire,
) -> bool {
    let runtime_guard = runtime_state.lock();
    let Some(runtime) = runtime_guard.jobs.get(assignment.job_id.as_str()) else {
        return false;
    };
    if runtime.active_scan_epoch != assignment.scan_epoch {
        return false;
    }
    runtime
        .inflight
        .get(assignment.scan_unit_id.as_str())
        .is_some_and(|inflight| {
            inflight.assignment.scan_task_id == assignment.scan_task_id
                && inflight.assignment.scan_epoch == assignment.scan_epoch
                && inflight.assignment.generation == assignment.generation
        })
}

async fn launch_transfer_scan_until_ack_or_superseded(
    api: Arc<FluxonUserApi>,
    runtime_state: Arc<Mutex<TransferScanRuntimeState>>,
    src_exporter_id: String,
    assignment: FluxonFsTransferScanAssignmentWire,
) -> Result<(), String> {
    let mut attempt: u32 = 0;
    loop {
        if !scan_assignment_matches_live(&runtime_state, &assignment) {
            return Ok(());
        }
        match call_transfer_scan_launch_rpc(api.clone(), src_exporter_id.as_str(), &assignment)
            .await
        {
            Ok(launch_result) => match launch_result.disposition {
                FluxonFsTransferScanLaunchDispositionWire::Started
                | FluxonFsTransferScanLaunchDispositionWire::AlreadyRunning
                | FluxonFsTransferScanLaunchDispositionWire::AlreadyCompleted => {
                    return Ok(());
                }
            },
            Err(err) => {
                attempt = attempt.saturating_add(1);
                tracing::warn!(
                    "transfer scan launch rpc retry: job_id={} scan_unit_id={} scan_task_id={} src_exporter_id={} attempt={} err={}",
                    assignment.job_id,
                    assignment.scan_unit_id,
                    assignment.scan_task_id,
                    src_exporter_id,
                    attempt,
                    err,
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

fn take_completed_scan_epoch_if_ready(
    runtime: &mut TransferJobScanRuntime,
    snapshot: &fluxon_fs_s3_gateway::FsTransferSchedulerJobSnapshot,
) -> bool {
    if snapshot.scan_finished {
        runtime.scan_finished_durable = true;
        runtime.queue.clear();
        runtime.inflight.clear();
        runtime.pending_finalize.clear();
        return false;
    }
    runtime.queue.is_empty() && runtime.inflight.is_empty() && runtime.pending_finalize.is_empty()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransferScanSchedulerAction {
    FinishEpoch,
    DispatchAssignments,
    WaitForSourceExporter,
    Idle,
}

fn choose_transfer_scan_scheduler_action(
    should_finish_scan_epoch: bool,
    scan_finished: bool,
    has_online_src_exporters: bool,
) -> TransferScanSchedulerAction {
    if should_finish_scan_epoch {
        return TransferScanSchedulerAction::FinishEpoch;
    }
    if scan_finished {
        return TransferScanSchedulerAction::Idle;
    }
    if has_online_src_exporters {
        return TransferScanSchedulerAction::DispatchAssignments;
    }
    TransferScanSchedulerAction::WaitForSourceExporter
}

fn expire_scan_timeouts(runtime: &mut TransferJobScanRuntime, now_unix_ms: i64) {
    let expired_scan_unit_ids: Vec<String> = runtime
        .inflight
        .iter()
        .filter(|(_, inflight)| {
            inflight.assignment.lease_expire_unix_ms <= now_unix_ms
                || now_unix_ms.saturating_sub(inflight.started_unix_ms)
                    >= TRANSFER_SCAN_ASSIGNMENT_TIMEOUT_MS
        })
        .map(|(scan_unit_id, _)| scan_unit_id.clone())
        .collect();
    for scan_unit_id in expired_scan_unit_ids {
        if let Some(inflight) = runtime.inflight.remove(scan_unit_id.as_str()) {
            runtime.queue.push_back(MasterScanUnit {
                scan_unit_id: None,
                root_relpath: inflight.assignment.root_relpath,
                generation: inflight.assignment.generation + 1,
                scan_mode: inflight.assignment.scan_mode,
            });
        }
    }
}

fn take_retry_ready_pending_scan_finalize(
    runtime: &TransferJobScanRuntime,
    now_unix_ms: i64,
) -> Option<FluxonFsTransferScanResultWire> {
    runtime
        .pending_finalize
        .values()
        .find(|pending| pending.next_retry_unix_ms <= now_unix_ms)
        .map(|pending| pending.result.clone())
}

fn complete_pending_scan_finalize_if_live(
    runtime_state: &Arc<Mutex<TransferScanRuntimeState>>,
    result: &FluxonFsTransferScanResultWire,
) -> bool {
    let mut runtime_guard = runtime_state.lock();
    let Some(runtime) = runtime_guard.jobs.get_mut(result.job_id.as_str()) else {
        return false;
    };
    if runtime.active_scan_epoch != result.scan_epoch {
        return false;
    }
    let Some(pending) = runtime.pending_finalize.get(result.scan_unit_id.as_str()) else {
        return false;
    };
    if pending.result != *result {
        return false;
    }
    runtime
        .pending_finalize
        .remove(result.scan_unit_id.as_str());
    runtime.enqueue_child_units(result.child_scan_units.clone());
    true
}

fn schedule_pending_scan_finalize_retry(
    runtime_state: &Arc<Mutex<TransferScanRuntimeState>>,
    result: &FluxonFsTransferScanResultWire,
    now_unix_ms: i64,
) -> bool {
    let mut runtime_guard = runtime_state.lock();
    let Some(runtime) = runtime_guard.jobs.get_mut(result.job_id.as_str()) else {
        return false;
    };
    if runtime.active_scan_epoch != result.scan_epoch {
        return false;
    }
    let Some(pending) = runtime
        .pending_finalize
        .get_mut(result.scan_unit_id.as_str())
    else {
        return false;
    };
    if pending.result != *result {
        return false;
    }
    pending.next_retry_unix_ms = now_unix_ms.saturating_add(TRANSFER_SCAN_FINALIZE_RETRY_DELAY_MS);
    true
}

fn build_scan_assignment(
    job: &fluxon_fs_s3_gateway::FsTransferJobRecord,
    src_exporter_id: String,
    scan_epoch: i64,
    scan_unit: MasterScanUnit,
    skip_entries: Vec<FluxonFsTransferSkipEntryWire>,
    batches: &[fluxon_fs_s3_gateway::FsTransferBatchRecord],
    direct_files_complete_records: &[fluxon_fs_s3_gateway::FsTransferDirectFilesCompleteRecord],
    now_unix_ms: i64,
) -> FluxonFsTransferScanAssignmentWire {
    let scan_unit_id = scan_unit.scan_unit_id.clone().unwrap_or_else(|| {
        format!(
            "{}__scan_epoch_{}__{}",
            job.job_id,
            scan_epoch,
            Uuid::new_v4()
        )
    });
    FluxonFsTransferScanAssignmentWire {
        job_id: job.job_id.clone(),
        scan_epoch,
        scan_unit_id: scan_unit_id.clone(),
        scan_task_id: format!("{}__task__{}", scan_unit_id, Uuid::new_v4()),
        root_relpath: scan_unit.root_relpath.clone(),
        generation: scan_unit.generation,
        scan_mode: scan_unit.scan_mode,
        src_export: job.src_export.clone(),
        src_exporter_id,
        batch_ready_bytes: job.batch_ready_bytes,
        lease_expire_unix_ms: now_unix_ms + TRANSFER_SCAN_ASSIGNMENT_TIMEOUT_MS,
        known_dispositions: build_known_dispositions_for_scan(
            job.job_id.as_str(),
            scan_unit.root_relpath.as_str(),
            scan_unit.generation,
            batches,
            direct_files_complete_records,
        ),
        live_child_scan_roots: Vec::new(),
        skip_entries,
    }
}

fn build_live_child_scan_roots_for_scan(
    runtime: &TransferJobScanRuntime,
    root_relpath: &str,
) -> Vec<String> {
    let mut roots = runtime
        .queue
        .iter()
        .map(|scan_unit| scan_unit.root_relpath.as_str())
        .chain(
            runtime
                .inflight
                .values()
                .map(|inflight| inflight.assignment.root_relpath.as_str()),
        )
        .filter(|candidate| {
            if *candidate == root_relpath {
                return false;
            }
            if root_relpath == "." {
                return true;
            }
            candidate.starts_with(format!("{}/", root_relpath).as_str())
        })
        .map(|relpath| relpath.to_string())
        .collect::<Vec<_>>();
    roots.sort();
    roots.dedup();
    roots
}

#[cfg(test)]
fn scan_result_matches_live_assignment(
    runtime_state: &TransferScanRuntimeState,
    result: &FluxonFsTransferScanResultWire,
) -> bool {
    let Some(runtime) = runtime_state.jobs.get(result.job_id.as_str()) else {
        return false;
    };
    if runtime.active_scan_epoch != result.scan_epoch {
        return false;
    }
    runtime
        .inflight
        .get(result.scan_unit_id.as_str())
        .is_some_and(|inflight| {
            inflight.assignment.scan_task_id == result.scan_task_id
                && inflight.assignment.scan_epoch == result.scan_epoch
                && inflight.assignment.generation == result.generation
        })
}

#[cfg(test)]
fn accept_scan_result_if_live_with_apply<ApplyFn>(
    runtime_state: &Arc<Mutex<TransferScanRuntimeState>>,
    result: FluxonFsTransferScanResultWire,
    apply_durable: ApplyFn,
) -> Result<bool, String>
where
    ApplyFn: FnOnce(&FluxonFsTransferScanResultWire) -> Result<(), String>,
{
    let mut runtime_guard = runtime_state.lock();
    if !scan_result_matches_live_assignment(&runtime_guard, &result) {
        tracing::warn!(
            "drop stale transfer scan result: job_id={} scan_epoch={} scan_unit_id={} scan_task_id={} generation={}",
            result.job_id,
            result.scan_epoch,
            result.scan_unit_id,
            result.scan_task_id,
            result.generation,
        );
        return Ok(false);
    }
    apply_durable(&result)?;
    let runtime = runtime_guard.jobs.get_mut(result.job_id.as_str()).unwrap();
    runtime.inflight.remove(result.scan_unit_id.as_str());
    runtime.enqueue_child_units(result.child_scan_units);
    Ok(true)
}

fn note_scan_runtime_counts(
    s3_state: &fluxon_fs_s3_gateway::GatewayState,
    job_id: &str,
    runtime_state: &Arc<Mutex<TransferScanRuntimeState>>,
) {
    let (queued_scan_unit_count, inflight_scan_unit_count) = {
        let runtime_guard = runtime_state.lock();
        let Some(runtime) = runtime_guard.jobs.get(job_id) else {
            return;
        };
        (runtime.queue.len() as i64, runtime.inflight.len() as i64)
    };
    s3_state.note_transfer_scan_runtime_counts(
        job_id,
        queued_scan_unit_count,
        inflight_scan_unit_count,
    );
}

fn accumulate_batch_manifest_stats(
    batch: &fluxon_fs_core::config::FluxonFsTransferScanBatchWire,
) -> Result<(i64, i64), String> {
    let manifest = FluxonFsTransferManifestWire::decode_from_blob(batch.manifest_blob.as_slice())
        .map_err(|e| format!("decode transfer scan batch manifest failed: {}", e))?;
    let discovered_file_count = manifest.entries.len() as i64;
    let discovered_bytes = manifest
        .entries
        .iter()
        .fold(0_i64, |acc, entry| acc.saturating_add(entry.size.max(0)));
    Ok((discovered_file_count, discovered_bytes))
}

fn scan_result_live_detail_delta(
    result: &FluxonFsTransferScanResultWire,
) -> Result<(i64, i64, i64), String> {
    let mut discovered_batch_count = 0_i64;
    let mut discovered_file_count = 0_i64;
    let mut discovered_bytes = 0_i64;
    for batch in &result.direct_files_only_batches {
        let (batch_file_count, batch_bytes) = accumulate_batch_manifest_stats(batch)?;
        discovered_batch_count = discovered_batch_count.saturating_add(1);
        discovered_file_count = discovered_file_count.saturating_add(batch_file_count);
        discovered_bytes = discovered_bytes.saturating_add(batch_bytes);
    }
    for batch in &result.full_dir_batches {
        let (batch_file_count, batch_bytes) = accumulate_batch_manifest_stats(batch)?;
        discovered_batch_count = discovered_batch_count.saturating_add(1);
        discovered_file_count = discovered_file_count.saturating_add(batch_file_count);
        discovered_bytes = discovered_bytes.saturating_add(batch_bytes);
    }
    Ok((
        discovered_batch_count,
        discovered_file_count,
        discovered_bytes,
    ))
}

fn transfer_scan_event_result_view(
    event: &FluxonFsTransferScanEventWire,
) -> FluxonFsTransferScanResultWire {
    FluxonFsTransferScanResultWire {
        job_id: event.job_id.clone(),
        scan_epoch: event.scan_epoch,
        scan_unit_id: event.scan_unit_id.clone(),
        scan_task_id: event.scan_task_id.clone(),
        root_relpath: event.root_relpath.clone(),
        generation: event.generation,
        frontier: fluxon_fs_core::config::FluxonFsTransferScanFrontier {
            direct_files: Vec::new(),
            direct_dirs: Vec::new(),
            empty_dirs: Vec::new(),
        },
        direct_files_only_batches: event.direct_files_only_batches.clone(),
        child_scan_units: event.child_scan_units.clone(),
        full_dir_batches: event.full_dir_batches.clone(),
        finished: event.event_kind == FluxonFsTransferScanEventKindWire::Finished,
    }
}

fn scan_event_live_detail_delta(
    event: &FluxonFsTransferScanEventWire,
) -> Result<(i64, i64, i64), String> {
    scan_result_live_detail_delta(&transfer_scan_event_result_view(event))
}

fn accept_scan_event_if_live(
    s3_state: &fluxon_fs_s3_gateway::GatewayState,
    runtime_state: &Arc<Mutex<TransferScanRuntimeState>>,
    event: FluxonFsTransferScanEventWire,
) -> Result<FluxonFsTransferScanEventAckWire, String> {
    let Some(job) = s3_state.transfer_job_record(event.job_id.as_str())? else {
        runtime_state.lock().jobs.remove(event.job_id.as_str());
        return Ok(FluxonFsTransferScanEventAckWire::stop(false));
    };
    if job.state != FluxonFsTransferJobState::Running {
        runtime_state.lock().jobs.remove(event.job_id.as_str());
        return Ok(FluxonFsTransferScanEventAckWire::stop(false));
    }
    let now_unix_ms = Utc::now().timestamp_millis();
    let lease_expire_unix_ms = now_unix_ms.saturating_add(TRANSFER_SCAN_ASSIGNMENT_TIMEOUT_MS);
    let mut runtime_guard = runtime_state.lock();
    let Some(runtime) = runtime_guard.jobs.get_mut(event.job_id.as_str()) else {
        return Ok(FluxonFsTransferScanEventAckWire::stop(false));
    };
    if runtime.active_scan_epoch != event.scan_epoch {
        return Ok(FluxonFsTransferScanEventAckWire::stop(false));
    }
    let accepted_finished = transfer_scan_event_result_view(&event);
    if let Some(pending) = runtime.pending_finalize.get(event.scan_unit_id.as_str()) {
        if pending.result == accepted_finished
            && event.event_kind == FluxonFsTransferScanEventKindWire::Finished
        {
            return Ok(FluxonFsTransferScanEventAckWire::stop(true));
        }
        return Ok(FluxonFsTransferScanEventAckWire::stop(false));
    }
    let Some(inflight) = runtime.inflight.get_mut(event.scan_unit_id.as_str()) else {
        return Ok(FluxonFsTransferScanEventAckWire::stop(false));
    };
    if inflight.assignment.scan_task_id != event.scan_task_id
        || inflight.assignment.scan_epoch != event.scan_epoch
        || inflight.assignment.generation != event.generation
    {
        return Ok(FluxonFsTransferScanEventAckWire::stop(false));
    }
    if event.event_seq_no < inflight.last_accepted_event_seq_no {
        return Ok(FluxonFsTransferScanEventAckWire::continue_running(
            false,
            inflight.assignment.lease_expire_unix_ms,
        ));
    }
    if event.event_seq_no == inflight.last_accepted_event_seq_no {
        return Ok(FluxonFsTransferScanEventAckWire::continue_running(
            false,
            inflight.assignment.lease_expire_unix_ms,
        ));
    }
    if event.event_seq_no != inflight.last_accepted_event_seq_no.saturating_add(1) {
        return Err(format!(
            "transfer scan event sequence gap: job_id={} scan_unit_id={} scan_task_id={} got_seq_no={} last_seq_no={}",
            event.job_id,
            event.scan_unit_id,
            event.scan_task_id,
            event.event_seq_no,
            inflight.last_accepted_event_seq_no,
        ));
    }
    match event.event_kind {
        FluxonFsTransferScanEventKindWire::Started => {
            inflight.last_accepted_event_seq_no = event.event_seq_no;
            inflight.assignment.lease_expire_unix_ms = lease_expire_unix_ms;
            inflight.started_unix_ms = now_unix_ms;
            Ok(FluxonFsTransferScanEventAckWire::continue_running(
                true,
                lease_expire_unix_ms,
            ))
        }
        FluxonFsTransferScanEventKindWire::Append => {
            let result = transfer_scan_event_result_view(&event);
            s3_state.apply_transfer_scan_append(&result)?;
            inflight.last_accepted_event_seq_no = event.event_seq_no;
            inflight.assignment.lease_expire_unix_ms = lease_expire_unix_ms;
            inflight.started_unix_ms = now_unix_ms;
            let _ = inflight;
            runtime.enqueue_child_units(result.child_scan_units);
            Ok(FluxonFsTransferScanEventAckWire::continue_running(
                true,
                lease_expire_unix_ms,
            ))
        }
        FluxonFsTransferScanEventKindWire::Finished => {
            let result = transfer_scan_event_result_view(&event);
            s3_state.apply_transfer_scan_result(&result)?;
            let scan_unit_id = event.scan_unit_id.clone();
            inflight.last_accepted_event_seq_no = event.event_seq_no;
            let _ = inflight;
            runtime.inflight.remove(scan_unit_id.as_str());
            runtime.pending_finalize.insert(
                scan_unit_id,
                MasterPendingScanFinalize {
                    result,
                    next_retry_unix_ms: now_unix_ms,
                },
            );
            Ok(FluxonFsTransferScanEventAckWire::stop(true))
        }
        FluxonFsTransferScanEventKindWire::Failed => {
            let requeue_root_relpath = inflight.assignment.root_relpath.clone();
            let requeue_generation = inflight.assignment.generation.saturating_add(1);
            let requeue_scan_mode = inflight.assignment.scan_mode;
            let _ = inflight;
            let requeue = MasterScanUnit {
                scan_unit_id: None,
                root_relpath: requeue_root_relpath,
                generation: requeue_generation,
                scan_mode: requeue_scan_mode,
            };
            runtime.inflight.remove(event.scan_unit_id.as_str());
            runtime.queue.push_back(requeue);
            s3_state.note_transfer_failure(
                event.job_id.as_str(),
                now_unix_ms,
                fluxon_fs_s3_gateway::FsTransferFailureScope::Scan,
                event.error_detail.as_str(),
            );
            Ok(FluxonFsTransferScanEventAckWire::stop(true))
        }
    }
}

#[cfg(test)]
fn accept_scan_event_if_live_with_apply<ApplyFn>(
    runtime_state: &Arc<Mutex<TransferScanRuntimeState>>,
    event: FluxonFsTransferScanEventWire,
    apply_finished: ApplyFn,
) -> Result<FluxonFsTransferScanEventAckWire, String>
where
    ApplyFn: FnOnce(&FluxonFsTransferScanResultWire) -> Result<(), String>,
{
    let now_unix_ms = Utc::now().timestamp_millis();
    let lease_expire_unix_ms = now_unix_ms.saturating_add(TRANSFER_SCAN_ASSIGNMENT_TIMEOUT_MS);
    let mut runtime_guard = runtime_state.lock();
    let Some(runtime) = runtime_guard.jobs.get_mut(event.job_id.as_str()) else {
        return Ok(FluxonFsTransferScanEventAckWire::stop(false));
    };
    if runtime.active_scan_epoch != event.scan_epoch {
        return Ok(FluxonFsTransferScanEventAckWire::stop(false));
    }
    let accepted_finished = transfer_scan_event_result_view(&event);
    if let Some(pending) = runtime.pending_finalize.get(event.scan_unit_id.as_str()) {
        if pending.result == accepted_finished
            && event.event_kind == FluxonFsTransferScanEventKindWire::Finished
        {
            return Ok(FluxonFsTransferScanEventAckWire::stop(true));
        }
        return Ok(FluxonFsTransferScanEventAckWire::stop(false));
    }
    let Some(inflight) = runtime.inflight.get_mut(event.scan_unit_id.as_str()) else {
        return Ok(FluxonFsTransferScanEventAckWire::stop(false));
    };
    if inflight.assignment.scan_task_id != event.scan_task_id
        || inflight.assignment.scan_epoch != event.scan_epoch
        || inflight.assignment.generation != event.generation
    {
        return Ok(FluxonFsTransferScanEventAckWire::stop(false));
    }
    if event.event_seq_no < inflight.last_accepted_event_seq_no {
        return Ok(FluxonFsTransferScanEventAckWire::continue_running(
            false,
            inflight.assignment.lease_expire_unix_ms,
        ));
    }
    if event.event_seq_no == inflight.last_accepted_event_seq_no {
        return Ok(FluxonFsTransferScanEventAckWire::continue_running(
            false,
            inflight.assignment.lease_expire_unix_ms,
        ));
    }
    if event.event_seq_no != inflight.last_accepted_event_seq_no.saturating_add(1) {
        return Err(format!(
            "transfer scan event sequence gap: job_id={} scan_unit_id={} scan_task_id={} got_seq_no={} last_seq_no={}",
            event.job_id,
            event.scan_unit_id,
            event.scan_task_id,
            event.event_seq_no,
            inflight.last_accepted_event_seq_no,
        ));
    }
    match event.event_kind {
        FluxonFsTransferScanEventKindWire::Started => {
            inflight.last_accepted_event_seq_no = event.event_seq_no;
            inflight.assignment.lease_expire_unix_ms = lease_expire_unix_ms;
            inflight.started_unix_ms = now_unix_ms;
            Ok(FluxonFsTransferScanEventAckWire::continue_running(
                true,
                lease_expire_unix_ms,
            ))
        }
        FluxonFsTransferScanEventKindWire::Append => {
            let result = transfer_scan_event_result_view(&event);
            inflight.last_accepted_event_seq_no = event.event_seq_no;
            inflight.assignment.lease_expire_unix_ms = lease_expire_unix_ms;
            inflight.started_unix_ms = now_unix_ms;
            let _ = inflight;
            runtime.enqueue_child_units(result.child_scan_units);
            Ok(FluxonFsTransferScanEventAckWire::continue_running(
                true,
                lease_expire_unix_ms,
            ))
        }
        FluxonFsTransferScanEventKindWire::Finished => {
            let result = transfer_scan_event_result_view(&event);
            apply_finished(&result)?;
            let scan_unit_id = event.scan_unit_id.clone();
            inflight.last_accepted_event_seq_no = event.event_seq_no;
            let _ = inflight;
            runtime.inflight.remove(scan_unit_id.as_str());
            runtime.pending_finalize.insert(
                scan_unit_id,
                MasterPendingScanFinalize {
                    result,
                    next_retry_unix_ms: now_unix_ms,
                },
            );
            Ok(FluxonFsTransferScanEventAckWire::stop(true))
        }
        FluxonFsTransferScanEventKindWire::Failed => {
            let requeue_root_relpath = inflight.assignment.root_relpath.clone();
            let requeue_generation = inflight.assignment.generation.saturating_add(1);
            let requeue_scan_mode = inflight.assignment.scan_mode;
            let _ = inflight;
            let requeue = MasterScanUnit {
                scan_unit_id: None,
                root_relpath: requeue_root_relpath,
                generation: requeue_generation,
                scan_mode: requeue_scan_mode,
            };
            runtime.inflight.remove(event.scan_unit_id.as_str());
            runtime.queue.push_back(requeue);
            Ok(FluxonFsTransferScanEventAckWire::stop(true))
        }
    }
}

async fn dispatch_scan_assignments_until_full(
    fs_framework: crate::Framework,
    api: Arc<FluxonUserApi>,
    s3_state: Arc<fluxon_fs_s3_gateway::GatewayState>,
    runtime_state: Arc<Mutex<TransferScanRuntimeState>>,
    job: fluxon_fs_s3_gateway::FsTransferJobRecord,
    src_exporter_ids: Vec<String>,
    skip_entries: Vec<FluxonFsTransferSkipEntryWire>,
    now_unix_ms: i64,
) -> Result<(), String> {
    let scan_concurrency_limit = std::cmp::min(
        TRANSFER_SCAN_CONCURRENCY_SYSTEM_LIMIT,
        std::cmp::max(1_i64, job.desired_scan_concurrency) as usize,
    );
    // One dispatch pass only needs the current durable coverage snapshot for
    // this job. Reusing one job-scoped snapshot avoids reloading the full
    // transfer namespace before every single scan-unit launch.
    let batches = s3_state.list_transfer_batches_for_job(job.job_id.as_str())?;
    let direct_files_complete_records =
        s3_state.list_transfer_direct_files_complete_records_for_job(job.job_id.as_str())?;
    loop {
        let assignment = {
            let mut runtime_guard = runtime_state.lock();
            let runtime = runtime_guard.jobs.entry(job.job_id.clone()).or_default();
            if runtime.active_scan_epoch != job.scan_epoch {
                runtime.reset_for_new_epoch(job.scan_epoch, job.src_root_relpath.clone());
            }
            expire_scan_timeouts(runtime, now_unix_ms);
            if runtime.inflight.len() >= scan_concurrency_limit {
                None
            } else {
                runtime.queue.pop_front().and_then(|scan_unit| {
                    let placement_key = transfer_scan_unit_placement_key(
                        job.job_id.as_str(),
                        runtime.active_scan_epoch,
                        &scan_unit,
                    );
                    let src_exporter_id = choose_online_export_agent(
                        src_exporter_ids.as_slice(),
                        placement_key.as_str(),
                    )?;
                    let mut assignment = build_scan_assignment(
                        &job,
                        src_exporter_id,
                        runtime.active_scan_epoch,
                        scan_unit.clone(),
                        skip_entries.clone(),
                        &batches,
                        &direct_files_complete_records,
                        now_unix_ms,
                    );
                    assignment.live_child_scan_roots = build_live_child_scan_roots_for_scan(
                        runtime,
                        scan_unit.root_relpath.as_str(),
                    );
                    Some(assignment)
                })
            }
        };
        let Some(assignment) = assignment else {
            return Ok(());
        };
        {
            let mut runtime_guard = runtime_state.lock();
            let runtime = runtime_guard.jobs.entry(job.job_id.clone()).or_default();
            runtime.inflight.insert(
                assignment.scan_unit_id.clone(),
                MasterInflightScan {
                    assignment: assignment.clone(),
                    started_unix_ms: now_unix_ms,
                    last_accepted_event_seq_no: -1,
                },
            );
        }
        note_scan_runtime_counts(s3_state.as_ref(), job.job_id.as_str(), &runtime_state);
        let api2 = api.clone();
        let s3_state2 = s3_state.clone();
        let runtime_state2 = runtime_state.clone();
        let scan_agent_id = assignment.src_exporter_id.clone();
        let mut shutdown_waiter = fs_framework.register_shutdown_waiter();
        crate::framework::spawn_fs_task(&fs_framework, "transfer_scan_launch", async move {
            let scan_unit_id = assignment.scan_unit_id.clone();
            let launch_result = tokio::select! {
                _ = shutdown_waiter.wait() => return,
                result = launch_transfer_scan_until_ack_or_superseded(
                    api2.clone(),
                    runtime_state2.clone(),
                    scan_agent_id,
                    assignment.clone(),
                ) => result,
            };
            if let Err(err) = launch_result {
                tracing::warn!(
                    "transfer scan launch loop exited with error: job_id={} scan_unit_id={} scan_task_id={} err={}",
                    assignment.job_id,
                    scan_unit_id,
                    assignment.scan_task_id,
                    err,
                );
            }
            note_scan_runtime_counts(
                s3_state2.as_ref(),
                assignment.job_id.as_str(),
                &runtime_state2,
            );
        });
    }
}

async fn run_transfer_scan_scheduler_once(
    fs_framework: crate::Framework,
    api: Arc<FluxonUserApi>,
    s3_state: Arc<fluxon_fs_s3_gateway::GatewayState>,
    runtime_state: Arc<Mutex<TransferScanRuntimeState>>,
) -> Result<(), String> {
    let now_unix_ms = Utc::now().timestamp_millis();
    let jobs = s3_state.list_running_transfer_jobs()?;
    let running_job_ids = jobs
        .iter()
        .map(|job| job.job_id.clone())
        .collect::<BTreeSet<_>>();
    runtime_state
        .lock()
        .jobs
        .retain(|job_id, _| running_job_ids.contains(job_id));
    for job in jobs {
        let skip_entries = load_job_skip_entries(&job)?;
        let snapshot = match s3_state.transfer_scheduler_job_snapshot(job.job_id.as_str())? {
            Some(v) => v,
            None => continue,
        };
        let pending_finished_result = {
            let runtime_guard = runtime_state.lock();
            runtime_guard
                .jobs
                .get(job.job_id.as_str())
                .and_then(|runtime| take_retry_ready_pending_scan_finalize(runtime, now_unix_ms))
        };
        if let Some(result) = pending_finished_result {
            if let Err(err) = s3_state.finish_transfer_scan_unit(&result) {
                let _ = schedule_pending_scan_finalize_retry(&runtime_state, &result, now_unix_ms);
                warn!(
                    "finish transfer scan unit retry scheduled: job_id={} scan_unit_id={} scan_task_id={} err={}",
                    result.job_id, result.scan_unit_id, result.scan_task_id, err
                );
            } else {
                let _ = complete_pending_scan_finalize_if_live(&runtime_state, &result);
            }
        }
        let should_finish_scan_epoch = {
            let mut runtime_guard = runtime_state.lock();
            let runtime = runtime_guard.jobs.entry(job.job_id.clone()).or_default();
            if runtime.active_scan_epoch != snapshot.scan_epoch {
                runtime.reset_for_new_epoch(snapshot.scan_epoch, job.src_root_relpath.clone());
            }
            expire_scan_timeouts(runtime, now_unix_ms);
            take_completed_scan_epoch_if_ready(runtime, &snapshot)
        };
        note_scan_runtime_counts(s3_state.as_ref(), job.job_id.as_str(), &runtime_state);
        let src_exporter_ids = if should_finish_scan_epoch || snapshot.scan_finished {
            Vec::new()
        } else {
            list_online_export_agents(s3_state.as_ref(), job.src_export.as_str())?
        };
        match choose_transfer_scan_scheduler_action(
            should_finish_scan_epoch,
            snapshot.scan_finished,
            !src_exporter_ids.is_empty(),
        ) {
            TransferScanSchedulerAction::FinishEpoch => {
                if let Err(err) =
                    s3_state.finish_transfer_scan_epoch(job.job_id.as_str(), snapshot.scan_epoch)
                {
                    tracing::warn!(
                        "finish transfer scan epoch failed: job_id={} scan_epoch={} err={}",
                        job.job_id,
                        snapshot.scan_epoch,
                        err
                    );
                    s3_state.note_transfer_failure(
                        job.job_id.as_str(),
                        Utc::now().timestamp_millis(),
                        fluxon_fs_s3_gateway::FsTransferFailureScope::Scan,
                        err.as_str(),
                    );
                }
            }
            TransferScanSchedulerAction::DispatchAssignments => {
                dispatch_scan_assignments_until_full(
                    fs_framework.clone(),
                    api.clone(),
                    s3_state.clone(),
                    runtime_state.clone(),
                    job.clone(),
                    src_exporter_ids.clone(),
                    skip_entries.clone(),
                    now_unix_ms,
                )
                .await?;
            }
            TransferScanSchedulerAction::WaitForSourceExporter
            | TransferScanSchedulerAction::Idle => {}
        }
    }
    Ok(())
}

async fn run_transfer_worker_scheduler_once(
    fs_framework: crate::Framework,
    api: Arc<FluxonUserApi>,
    s3_state: Arc<fluxon_fs_s3_gateway::GatewayState>,
) -> Result<(), String> {
    let now_unix_ms = Utc::now().timestamp_millis();
    let jobs = s3_state.list_running_transfer_jobs()?;
    for job in jobs {
        if job.desired_worker_count <= 0 {
            continue;
        }
        let snapshot = match s3_state.transfer_scheduler_job_snapshot(job.job_id.as_str())? {
            Some(v) => v,
            None => continue,
        };
        let src_exporter_ids =
            list_online_export_agents(s3_state.as_ref(), job.src_export.as_str())?;
        if src_exporter_ids.is_empty() {
            continue;
        }
        let dst_exporter_ids =
            list_online_export_agents(s3_state.as_ref(), job.dst_export.as_str())?;
        if dst_exporter_ids.is_empty() {
            continue;
        }
        for (batch_class, running_batch_count) in [
            (
                FsTransferReadyBatchClass::Payload,
                snapshot.payload_running_batch_count,
            ),
            (
                FsTransferReadyBatchClass::EmptyDirsOnly,
                snapshot.empty_dir_only_running_batch_count,
            ),
        ] {
            let mut dispatch_capacity = transfer_dispatch_capacity(
                running_batch_count.max(0_i64) as usize,
                job.desired_worker_count,
            );
            while dispatch_capacity > 0 {
                let Some(dispatch_batch) = s3_state
                    .load_next_ready_transfer_batch_for_job(job.job_id.as_str(), batch_class)?
                else {
                    break;
                };
                let batch = dispatch_batch.batch;
                let dst_exporter_id = if batch.assigned_dst_exporter_id.trim().is_empty() {
                    let dst_placement_key = transfer_batch_placement_key(
                        "dst",
                        job.dst_export.as_str(),
                        job.job_id.as_str(),
                        batch.batch_id.as_str(),
                    );
                    let Some(dst_exporter_id) = choose_online_export_agent(
                        dst_exporter_ids.as_slice(),
                        dst_placement_key.as_str(),
                    ) else {
                        break;
                    };
                    dst_exporter_id
                } else if dst_exporter_ids
                    .iter()
                    .any(|agent_id| agent_id == &batch.assigned_dst_exporter_id)
                {
                    batch.assigned_dst_exporter_id.clone()
                } else {
                    break;
                };
                let src_exporter_id = if batch.assigned_src_exporter_id.trim().is_empty() {
                    let src_placement_key = transfer_batch_placement_key(
                        "src",
                        job.src_export.as_str(),
                        job.job_id.as_str(),
                        batch.batch_id.as_str(),
                    );
                    let Some(src_exporter_id) = choose_online_export_agent(
                        src_exporter_ids.as_slice(),
                        src_placement_key.as_str(),
                    ) else {
                        break;
                    };
                    src_exporter_id
                } else if src_exporter_ids
                    .iter()
                    .any(|agent_id| agent_id == &batch.assigned_src_exporter_id)
                {
                    batch.assigned_src_exporter_id.clone()
                } else {
                    break;
                };
                let worker_id =
                    transfer_batch_worker_id(job.job_id.as_str(), batch.batch_id.as_str());
                let worker_task_id = format!("{}__{}", worker_id, Uuid::new_v4());
                let collect_infos = dispatch_batch
                    .collect_infos
                    .into_iter()
                    .map(
                        |row| fluxon_fs_core::config::FluxonFsTransferBatchCollectInfoWire {
                            collect_kind: row.collect_kind,
                            collect_blob: row.collect_blob,
                        },
                    )
                    .collect();
                s3_state.assign_transfer_batch_to_worker(
                    job.job_id.as_str(),
                    batch.batch_id.as_str(),
                    src_exporter_id.as_str(),
                    worker_id.as_str(),
                    worker_task_id.as_str(),
                    dst_exporter_id.as_str(),
                    now_unix_ms + TRANSFER_WORKER_LEASE_MS,
                )?;
                dispatch_capacity -= 1;
                let assignment = FluxonFsTransferWorkerAssignmentWire {
                    job_id: job.job_id.clone(),
                    batch_id: batch.batch_id.clone(),
                    worker_task_id: worker_task_id.clone(),
                    batch_kind: batch.batch_kind,
                    worker_id: worker_id.clone(),
                    src_export: job.src_export.clone(),
                    dst_export: job.dst_export.clone(),
                    src_exporter_id,
                    dst_exporter_id: dst_exporter_id.clone(),
                    dst_root_relpath: job.dst_root_relpath.clone(),
                    root_relpath: batch.root_relpath.clone(),
                    staging_prefix: format!(
                        ".fluxon.stage/{}/{}/{}",
                        job.job_id, batch.batch_id, worker_task_id
                    ),
                    lease_expire_unix_ms: now_unix_ms + TRANSFER_WORKER_LEASE_MS,
                    manifest_blob: batch.manifest_blob.clone(),
                    collect_infos,
                };
                let api2 = api.clone();
                let s3_state2 = s3_state.clone();
                let mut shutdown_waiter = fs_framework.register_shutdown_waiter();
                crate::framework::spawn_fs_task(
                    &fs_framework,
                    "transfer_worker_launch",
                    async move {
                        let launch_result = tokio::select! {
                            _ = shutdown_waiter.wait() => return,
                            result = launch_transfer_worker_until_ack_or_superseded(
                                api2.clone(),
                                s3_state2.clone(),
                                dst_exporter_id.clone(),
                                assignment.clone(),
                            ) => result,
                        };
                        if let Err(err) = launch_result {
                            let _ = s3_state2.mark_transfer_worker_attempt_stopped(
                                assignment.job_id.as_str(),
                                assignment.batch_id.as_str(),
                                assignment.worker_id.as_str(),
                                assignment.worker_task_id.as_str(),
                                None,
                                err.as_str(),
                                Utc::now().timestamp_millis(),
                            );
                            s3_state2.note_transfer_failure(
                                assignment.job_id.as_str(),
                                Utc::now().timestamp_millis(),
                                fluxon_fs_s3_gateway::FsTransferFailureScope::WorkerLaunch,
                                err.as_str(),
                            );
                            tracing::warn!(
                                "transfer worker launch failed: job_id={} batch_id={} worker_id={} worker_task_id={} err={}",
                                assignment.job_id,
                                assignment.batch_id,
                                assignment.worker_id,
                                assignment.worker_task_id,
                                err
                            );
                        }
                    },
                );
            }
        }
    }
    Ok(())
}

pub(crate) fn start_transfer_reconcile_actor(
    fs_framework: crate::Framework,
    s3_state: Arc<fluxon_fs_s3_gateway::GatewayState>,
) {
    let mut shutdown_waiter = fs_framework.register_shutdown_waiter();
    crate::framework::spawn_fs_task(&fs_framework, "transfer_reconcile", async move {
        loop {
            let now_unix_ms = Utc::now().timestamp_millis();
            if let Err(err) = s3_state.reconcile_transfer_scheduler_state(now_unix_ms) {
                tracing::warn!("transfer reconcile actor failed: {}", err);
            }
            tokio::select! {
                _ = shutdown_waiter.wait() => return,
                _ = tokio::time::sleep(Duration::from_millis(TRANSFER_RECONCILE_INTERVAL_MS)) => {}
            }
        }
    });
}

// The scan scheduler is level-triggered by explicit scan wakeups and also runs
// on a periodic idle tick so scan timeouts and newly visible source agents
// converge even after missed notifications.
pub(crate) fn start_transfer_scan_scheduler_actor(
    fs_framework: crate::Framework,
    api: Arc<FluxonUserApi>,
    s3_state: Arc<fluxon_fs_s3_gateway::GatewayState>,
    runtime_state: Arc<Mutex<TransferScanRuntimeState>>,
) {
    let wake = s3_state.transfer_scan_scheduler_wait_handle();
    let mut shutdown_waiter = fs_framework.register_shutdown_waiter();
    let actor_framework = fs_framework.clone();
    crate::framework::spawn_fs_task(&fs_framework, "transfer_scan_scheduler", async move {
        loop {
            let reconcile_result = tokio::select! {
                _ = shutdown_waiter.wait() => return,
                result = run_transfer_scan_scheduler_once(
                    actor_framework.clone(),
                    api.clone(),
                    s3_state.clone(),
                    runtime_state.clone(),
                ) => result,
            };
            if let Err(err) = reconcile_result {
                tracing::warn!("transfer scan scheduler reconcile failed: {}", err);
            }
            tokio::select! {
                _ = shutdown_waiter.wait() => return,
                _ = wake.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(TRANSFER_SCHEDULER_IDLE_SLEEP_MS)) => {}
            }
        }
    });
}

// The worker scheduler only dispatches ready batches. Durable reconcile runs
// on its own cadence so launch acks and heartbeats are not blocked behind a
// full convergence pass on every wake-up.
pub(crate) fn start_transfer_worker_scheduler_actor(
    fs_framework: crate::Framework,
    api: Arc<FluxonUserApi>,
    s3_state: Arc<fluxon_fs_s3_gateway::GatewayState>,
) {
    let wake = s3_state.transfer_worker_scheduler_wait_handle();
    let mut shutdown_waiter = fs_framework.register_shutdown_waiter();
    let actor_framework = fs_framework.clone();
    crate::framework::spawn_fs_task(&fs_framework, "transfer_worker_scheduler", async move {
        loop {
            let scheduler_result = tokio::select! {
                _ = shutdown_waiter.wait() => return,
                result = run_transfer_worker_scheduler_once(
                    actor_framework.clone(),
                    api.clone(),
                    s3_state.clone(),
                ) => result,
            };
            if let Err(err) = scheduler_result {
                tracing::warn!("transfer worker scheduler failed: {}", err);
            }
            tokio::select! {
                _ = shutdown_waiter.wait() => return,
                _ = wake.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(TRANSFER_SCHEDULER_IDLE_SLEEP_MS)) => {}
            }
        }
    });
}

// The result path accepts two durable mutations: scan results and worker final
// results. Heartbeats stay on a separate path because they only extend or stop
// one live worker attempt.
pub(crate) fn register_transfer_result_and_heartbeat_rpc(
    api: Arc<FluxonUserApi>,
    s3_state: Arc<fluxon_fs_s3_gateway::GatewayState>,
    runtime_state: Arc<Mutex<TransferScanRuntimeState>>,
) -> fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvResult<()> {
    {
        let s3_state2 = s3_state.clone();
        let runtime_state2 = runtime_state.clone();
        let handler: Arc<
            dyn Fn(
                    String,
                    FlatDict,
                )
                    -> fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvResult<FlatDict>
                + Send
                + Sync
                + 'static,
        > = Arc::new(move |_from_node_id, payload| {
            if let Some(FlatValue::String(v)) = payload.get("worker_fatal_json") {
                let fatal = serde_json::from_str::<serde_json::Value>(v).map_err(|e| {
                    fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                        fluxon_kv::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                            detail: format!("parse worker_fatal_json failed: {}", e),
                        },
                    )
                })?;
                let job_id = fatal
                    .get("job_id")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                            fluxon_kv::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                                detail: "worker_fatal_json.job_id must be non-empty string".to_string(),
                            },
                        )
                    })?;
                let batch_id = fatal
                    .get("batch_id")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                            fluxon_kv::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                                detail: "worker_fatal_json.batch_id must be non-empty string".to_string(),
                            },
                        )
                    })?;
                let worker_id = fatal
                    .get("worker_id")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                            fluxon_kv::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                                detail: "worker_fatal_json.worker_id must be non-empty string".to_string(),
                            },
                        )
                    })?;
                let worker_task_id = fatal
                    .get("worker_task_id")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                            fluxon_kv::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                                detail: "worker_fatal_json.worker_task_id must be non-empty string".to_string(),
                            },
                        )
                    })?;
                let fatal_kind = fatal
                    .get("fatal_kind")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let fatal_message = fatal
                    .get("fatal_message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let now_unix_ms = Utc::now().timestamp_millis();
                s3_state2
                    .mark_transfer_worker_attempt_stopped(
                        job_id,
                        batch_id,
                        worker_id,
                        worker_task_id,
                        None,
                        fatal_message,
                        now_unix_ms,
                    )
                    .map_err(|detail| {
                        fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                            fluxon_kv::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                                detail,
                            },
                        )
                    })?;
                s3_state2.note_transfer_failure(
                    job_id,
                    now_unix_ms,
                    fluxon_fs_s3_gateway::FsTransferFailureScope::WorkerStop,
                    fatal_message,
                );
                warn!(
                    "transfer worker fatal recorded without job failure: job_id={} batch_id={} worker_id={} worker_task_id={} kind={} message={}",
                    job_id, batch_id, worker_id, worker_task_id, fatal_kind, fatal_message,
                );
                return Ok(FlatDict::from([("ok".to_string(), FlatValue::Bool(true))]));
            }
            if let Some(FlatValue::String(v)) = payload.get("scan_event_json") {
                if v.trim().is_empty() {
                    return Err(
                        fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                            fluxon_kv::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                                detail: "scan_event_json must be non-empty string".to_string(),
                            },
                        ),
                    );
                }
                let event = serde_json::from_str::<FluxonFsTransferScanEventWire>(v).map_err(|e| {
                    fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                        fluxon_kv::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                            detail: format!("parse scan_event_json failed: {}", e),
                        },
                    )
                })?;
                let event_received_unix_ms = Utc::now().timestamp_millis();
                let ack = accept_scan_event_if_live(s3_state2.as_ref(), &runtime_state2, event.clone())
                    .map_err(|detail| {
                        s3_state2.note_transfer_failure(
                            event.job_id.as_str(),
                            event_received_unix_ms,
                            fluxon_fs_s3_gateway::FsTransferFailureScope::Scan,
                            detail.as_str(),
                        );
                        fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                            fluxon_kv::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                                detail,
                            },
                        )
                    })?;
                if ack.accepted {
                    match event.event_kind {
                        FluxonFsTransferScanEventKindWire::Append
                        | FluxonFsTransferScanEventKindWire::Finished => {
                            match scan_event_live_detail_delta(&event) {
                                Ok((
                                    discovered_batch_count,
                                    discovered_file_count,
                                    discovered_bytes,
                                )) => {
                                    s3_state2.note_transfer_scan_result_accepted(
                                        event.job_id.as_str(),
                                        event_received_unix_ms,
                                        discovered_batch_count,
                                        discovered_file_count,
                                        discovered_bytes,
                                    );
                                }
                                Err(err) => {
                                    s3_state2.note_transfer_failure(
                                        event.job_id.as_str(),
                                        event_received_unix_ms,
                                        fluxon_fs_s3_gateway::FsTransferFailureScope::Scan,
                                        err.as_str(),
                                    );
                                }
                            }
                        }
                        FluxonFsTransferScanEventKindWire::Started => {}
                        FluxonFsTransferScanEventKindWire::Failed => {}
                    }
                }
                let append_enqueued_child_frontier = ack.accepted
                    && event.event_kind == FluxonFsTransferScanEventKindWire::Append
                    && !event.child_scan_units.is_empty();
                let append_emitted_ready_batches = ack.accepted
                    && event.event_kind == FluxonFsTransferScanEventKindWire::Append
                    && (!event.direct_files_only_batches.is_empty()
                        || !event.full_dir_batches.is_empty());
                let finish_emitted_ready_batches = ack.accepted
                    && event.event_kind == FluxonFsTransferScanEventKindWire::Finished
                    && (!event.direct_files_only_batches.is_empty()
                        || !event.full_dir_batches.is_empty());
                let scan_frontier_changed = append_enqueued_child_frontier
                    || (ack.accepted
                        && matches!(
                            event.event_kind,
                            FluxonFsTransferScanEventKindWire::Finished
                                | FluxonFsTransferScanEventKindWire::Failed
                        ));
                if append_enqueued_child_frontier
                    || (ack.accepted
                        && matches!(
                            event.event_kind,
                            FluxonFsTransferScanEventKindWire::Finished
                                | FluxonFsTransferScanEventKindWire::Failed
                        ))
                {
                    note_scan_runtime_counts(
                        s3_state2.as_ref(),
                        event.job_id.as_str(),
                        &runtime_state2,
                    );
                }
                if scan_frontier_changed {
                    s3_state2.transfer_scan_scheduler_notify();
                }
                if append_emitted_ready_batches || finish_emitted_ready_batches {
                    s3_state2.transfer_worker_scheduler_notify();
                }
                let ack_json = serde_json::to_string(&ack).map_err(|e| {
                    fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                        fluxon_kv::rpcresp_kvresult_convert::msg_and_error::ApiError::Unknown {
                            detail: format!("serialize transfer scan event ack failed: {}", e),
                        },
                    )
                })?;
                return Ok(FlatDict::from([(
                    "scan_event_ack_json".to_string(),
                    FlatValue::String(ack_json),
                )]));
            }
            let result_json = match payload.get("result_json") {
                Some(FlatValue::String(v)) if !v.trim().is_empty() => v.clone(),
                _ => {
                    return Err(
                        fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                            fluxon_kv::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                                detail: "result_json must be non-empty string".to_string(),
                            },
                        ),
                    );
                }
            };
            let result = serde_json::from_str::<FluxonFsTransferWorkerResultWire>(&result_json)
                .map_err(|e| {
                    fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                        fluxon_kv::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                            detail: format!("parse transfer result_json failed: {}", e),
                        },
                    )
                })?;
            let start = std::time::Instant::now();
            info!(
                "transfer worker result rpc begin: job_id={} batch_id={} worker_id={} worker_task_id={} file_result_count={} collect_info_result_count={}",
                result.job_id,
                result.batch_id,
                result.worker_id,
                result.worker_task_id,
                result.file_results.len(),
                result.collect_info_results.len(),
            );
            let ack = s3_state2
                .apply_transfer_worker_result(&result)
                .map_err(|detail| {
                    s3_state2.note_transfer_failure(
                        result.job_id.as_str(),
                        Utc::now().timestamp_millis(),
                        fluxon_fs_s3_gateway::FsTransferFailureScope::WorkerResult,
                        detail.as_str(),
                    );
                    fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                    fluxon_kv::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                        detail,
                    },
                )
                })?;
            info!(
                "transfer worker result rpc done: job_id={} batch_id={} worker_id={} worker_task_id={} accepted={} elapsed_ms={}",
                result.job_id,
                result.batch_id,
                result.worker_id,
                result.worker_task_id,
                ack.accepted,
                start.elapsed().as_millis(),
            );
            let ack_json = serde_json::to_string(&ack).map_err(|e| {
                fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                    fluxon_kv::rpcresp_kvresult_convert::msg_and_error::ApiError::Unknown {
                        detail: format!("serialize transfer worker result ack failed: {}", e),
                    },
                )
            })?;
            Ok(FlatDict::from([(
                "result_ack_json".to_string(),
                FlatValue::String(ack_json),
            )]))
        });
        api.rpc_server().register(
            fluxon_fs_core::config::FS_MASTER_TRANSFER_SCHEDULER_RESULT_RPC_PATH,
            handler,
        )?;
    }
    {
        let s3_state2 = s3_state.clone();
        let handler: Arc<
            dyn Fn(
                    String,
                    FlatDict,
                )
                    -> fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvResult<FlatDict>
                + Send
                + Sync
                + 'static,
        > = Arc::new(move |_from_node_id, payload| {
            if let Some(FlatValue::String(v)) = payload.get("heartbeat_json") {
                let heartbeat = serde_json::from_str::<FluxonFsTransferWorkerHeartbeatWire>(v)
                    .map_err(|e| {
                        fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                            fluxon_kv::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                                detail: format!("parse heartbeat_json failed: {}", e),
                            },
                        )
                    })?;
                let heartbeat_received_unix_ms = Utc::now().timestamp_millis();
                let start = std::time::Instant::now();
                info!(
                    "transfer worker heartbeat rpc begin: job_id={} batch_id={} worker_id={} worker_task_id={} heartbeat_unix_ms={}",
                    heartbeat.job_id,
                    heartbeat.assigned_batch_id,
                    heartbeat.worker_id,
                    heartbeat.worker_task_id,
                    heartbeat.heartbeat_unix_ms,
                );
                let heartbeat_result = s3_state2
                    .apply_transfer_worker_heartbeat(
                        &heartbeat,
                        heartbeat_received_unix_ms,
                        TRANSFER_HEARTBEAT_EXTENSION_MS,
                    )
                    .map_err(|detail| {
                        s3_state2.note_transfer_failure(
                            heartbeat.job_id.as_str(),
                            heartbeat_received_unix_ms,
                            fluxon_fs_s3_gateway::FsTransferFailureScope::WorkerHeartbeat,
                            detail.as_str(),
                        );
                        fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                            fluxon_kv::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                                detail,
                            },
                        )
                    })?;
                if heartbeat_result.continue_running {
                    info!(
                        "transfer worker heartbeat rpc continue: job_id={} batch_id={} worker_id={} worker_task_id={} lease_expire_unix_ms={} elapsed_ms={}",
                        heartbeat.job_id,
                        heartbeat.assigned_batch_id,
                        heartbeat.worker_id,
                        heartbeat.worker_task_id,
                        heartbeat_result.lease_expire_unix_ms,
                        start.elapsed().as_millis(),
                    );
                } else {
                    warn!(
                        "transfer worker heartbeat rpc stop: job_id={} batch_id={} worker_id={} worker_task_id={} stop_reason={} elapsed_ms={}",
                        heartbeat.job_id,
                        heartbeat.assigned_batch_id,
                        heartbeat.worker_id,
                        heartbeat.worker_task_id,
                        heartbeat_result
                            .stop_reason
                            .map(|v| match v {
                                fluxon_fs_core::config::FluxonFsTransferWorkerStopReasonWire::Superseded => "superseded",
                                fluxon_fs_core::config::FluxonFsTransferWorkerStopReasonWire::Cancelled => "cancelled",
                            })
                            .unwrap_or(""),
                        start.elapsed().as_millis(),
                    );
                }
                let heartbeat_result_json =
                    serde_json::to_string(&heartbeat_result).map_err(|e| {
                        fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                            fluxon_kv::rpcresp_kvresult_convert::msg_and_error::ApiError::Unknown {
                                detail: format!(
                                    "serialize transfer worker heartbeat result failed: {}",
                                    e
                                ),
                            },
                        )
                    })?;
                return Ok(FlatDict::from([(
                    "heartbeat_result_json".to_string(),
                    FlatValue::String(heartbeat_result_json),
                )]));
            } else {
                return Err(
                    fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                        fluxon_kv::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                            detail: "heartbeat_json must be non-empty string".to_string(),
                        },
                    ),
                );
            }
        });
        api.rpc_server().register(
            fluxon_fs_core::config::FS_MASTER_TRANSFER_SCHEDULER_HEARTBEAT_RPC_PATH,
            handler,
        )?;
    }
    Ok(())
}

// Standalone check mode still runs the same transfer scheduler and state-store
// contract. The difference is operational: jobs can use desired_worker_count <=
// 0 so scanning/materialization planning happens without launching workers.
pub fn run_transfer_check_blocking(config_path: &str, workdir: &str) -> anyhow::Result<()> {
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
    run_transfer_check_blocking_from_yaml_text(&raw)
}

pub fn run_transfer_check_blocking_from_yaml_text(raw: &str) -> anyhow::Result<()> {
    if raw.trim().is_empty() {
        anyhow::bail!("config yaml must be non-empty");
    }
    let master_cfg =
        parse_master_config_from_yaml_text(raw).map_err(|e| anyhow::anyhow!("{}", e))?;
    let panel_cfg =
        parse_master_panel_config_from_yaml_text(raw).map_err(|e| anyhow::anyhow!("{}", e))?;
    let cache_yaml =
        extract_cache_config_yaml_from_yaml_text(raw).map_err(|e| anyhow::anyhow!("{}", e))?;
    let fs_cache = parse_cache_config_yaml(&cache_yaml).map_err(|e| anyhow::anyhow!("{}", e))?;
    fluxon_fs_s3_gateway::validate_exports_bucket_names(&fs_cache)?;
    let pull_interval_ms = master_cfg
        .pull_interval_ms
        .with_context(|| "fluxon_fs.master.pull_interval_ms is required for transfer check")?;

    let kv_yaml = extract_kvclient_config_yaml_from_fluxon_config(raw)?;
    let kv_cfg = kv_yaml.verify().map_err(|e| anyhow::anyhow!("{}", e))?;
    if kv_cfg.instance_key.to_string() != master_cfg.instance_key {
        anyhow::bail!(
            "kvclient.instance_key must match fluxon_fs.master.instance_key (got kvclient.instance_key={:?} fluxon_fs.master.instance_key={:?})",
            kv_cfg.instance_key,
            master_cfg.instance_key
        );
    }
    let dram = kv_cfg.contribute_to_cluster_pool_size.dram;
    let vram_is_zero = kv_cfg
        .contribute_to_cluster_pool_size
        .vram
        .values()
        .all(|v| *v == 0);
    if !(dram == 0 && vram_is_zero) {
        anyhow::bail!(
            "kvclient must be zero-contribution (external client) mode for transfer check"
        );
    }

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .with_context(|| "build tokio runtime")?,
    );
    let rt2 = rt.clone();
    let res = rt.as_ref().block_on(async move {
        super::async_main(
            rt2,
            kv_cfg,
            master_cfg,
            panel_cfg,
            cache_yaml,
            fs_cache,
            pull_interval_ms,
            true,
        )
        .await
    });
    if let Ok(rt0) = Arc::try_unwrap(rt) {
        rt0.shutdown_background();
    }
    res
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::{fs, io::Write};

    use fluxon_fs_core::config::{
        FluxonFsTransferBatchKind, FluxonFsTransferBatchState, FluxonFsTransferManifestWire,
        FluxonFsTransferScanAssignmentWire, FluxonFsTransferScanChildUnitWire,
        FluxonFsTransferScanEventKindWire, FluxonFsTransferScanEventWire,
        FluxonFsTransferScanFrontier, FluxonFsTransferScanMode, FluxonFsTransferScanResultWire,
    };
    use parking_lot::Mutex;
    use tempfile::TempDir;

    use super::{
        MasterInflightScan, MasterScanUnit, TRANSFER_SCAN_ASSIGNMENT_TIMEOUT_MS,
        TransferJobScanRuntime, TransferScanRuntimeState, TransferScanSchedulerAction,
        accept_scan_event_if_live_with_apply, accept_scan_result_if_live_with_apply,
        build_known_dispositions_for_scan, build_live_child_scan_roots_for_scan,
        build_scan_assignment, choose_online_export_agent, choose_transfer_scan_scheduler_action,
        complete_pending_scan_finalize_if_live, expire_scan_timeouts,
        scan_result_live_detail_delta, stable_transfer_agent_index,
        take_retry_ready_pending_scan_finalize, transfer_batch_worker_id,
        transfer_dispatch_capacity,
    };

    #[test]
    fn test_transfer_batch_worker_id_is_batch_scoped() {
        assert_eq!(
            transfer_batch_worker_id("job-1", "batch-a"),
            transfer_batch_worker_id("job-1", "batch-a")
        );
        assert_ne!(
            transfer_batch_worker_id("job-1", "batch-a"),
            transfer_batch_worker_id("job-1", "batch-b")
        );
    }

    #[test]
    fn test_transfer_dispatch_capacity_depends_on_running_batch_count_only() {
        assert_eq!(transfer_dispatch_capacity(0, 4), 4);
        assert_eq!(transfer_dispatch_capacity(2, 4), 2);
        assert_eq!(transfer_dispatch_capacity(4, 4), 0);
        assert_eq!(transfer_dispatch_capacity(5, 4), 0);
        assert_eq!(transfer_dispatch_capacity(0, 0), 0);
    }

    #[test]
    fn test_scan_assignment_timeout_covers_control_rpc_timeout() {
        assert!(
            TRANSFER_SCAN_ASSIGNMENT_TIMEOUT_MS > super::TRANSFER_CONTROL_RPC_TIMEOUT_MS as i64
        );
    }

    #[test]
    fn test_choose_transfer_scan_scheduler_action_prioritizes_epoch_finish() {
        assert_eq!(
            choose_transfer_scan_scheduler_action(true, false, false),
            TransferScanSchedulerAction::FinishEpoch
        );
        assert_eq!(
            choose_transfer_scan_scheduler_action(false, false, false),
            TransferScanSchedulerAction::WaitForSourceExporter
        );
        assert_eq!(
            choose_transfer_scan_scheduler_action(false, false, true),
            TransferScanSchedulerAction::DispatchAssignments
        );
        assert_eq!(
            choose_transfer_scan_scheduler_action(false, true, false),
            TransferScanSchedulerAction::Idle
        );
    }

    #[test]
    fn test_stable_transfer_agent_index_is_deterministic() {
        let first = stable_transfer_agent_index("scan:job:1:root:1", 4);
        let second = stable_transfer_agent_index("scan:job:1:root:1", 4);
        assert_eq!(first, second);
        assert_eq!(stable_transfer_agent_index("anything", 0), None);
    }

    #[test]
    fn test_choose_online_export_agent_distributes_across_agents() {
        let agent_ids = vec![
            "agent-a".to_string(),
            "agent-b".to_string(),
            "agent-c".to_string(),
        ];
        let mut chosen = std::collections::BTreeSet::new();
        for idx in 0..64 {
            let key = format!("batch:job-1:{}", idx);
            let agent = choose_online_export_agent(agent_ids.as_slice(), key.as_str()).unwrap();
            chosen.insert(agent);
        }
        assert!(chosen.len() > 1);
        assert!(
            chosen.is_subset(
                &agent_ids
                    .iter()
                    .cloned()
                    .collect::<std::collections::BTreeSet<_>>()
            )
        );
    }

    #[test]
    fn test_build_known_dispositions_for_scan_includes_descendants_for_root_assignment() {
        let batches = vec![
            fluxon_fs_s3_gateway::FsTransferBatchRecord {
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
            fluxon_fs_s3_gateway::FsTransferBatchRecord {
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
            fluxon_fs_s3_gateway::FsTransferBatchRecord {
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
            fluxon_fs_s3_gateway::FsTransferBatchRecord {
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

        let known = build_known_dispositions_for_scan("job", ".", 1, &batches, &[]);
        assert_eq!(
            known,
            vec![
                fluxon_fs_core::config::FluxonFsTransferDispositionWire {
                    root_relpath: "root/big".to_string(),
                    generation: 1,
                    batch_kind: FluxonFsTransferBatchKind::FullDir,
                },
                fluxon_fs_core::config::FluxonFsTransferDispositionWire {
                    root_relpath: "root/other".to_string(),
                    generation: 2,
                    batch_kind: FluxonFsTransferBatchKind::FullDir,
                },
            ]
        );
    }

    #[test]
    fn test_build_known_dispositions_for_scan_excludes_exact_root_direct_files_only_batch_without_complete_marker()
     {
        let known = build_known_dispositions_for_scan(
            "job",
            "root",
            1,
            &[fluxon_fs_s3_gateway::FsTransferBatchRecord {
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
    fn test_build_known_dispositions_for_scan_ignores_subtree_slice_batches() {
        let known = build_known_dispositions_for_scan(
            "job",
            "root",
            1,
            &[fluxon_fs_s3_gateway::FsTransferBatchRecord {
                job_id: "job".to_string(),
                batch_id: "batch-root".to_string(),
                root_relpath: "root".to_string(),
                batch_kind: FluxonFsTransferBatchKind::SubtreeSlice,
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
    fn test_build_known_dispositions_for_scan_includes_direct_files_complete_marker() {
        let known = build_known_dispositions_for_scan(
            "job",
            "root",
            9,
            &[],
            &[fluxon_fs_s3_gateway::FsTransferDirectFilesCompleteRecord {
                job_id: "job".to_string(),
                root_relpath: "root".to_string(),
                completed_at_unix_ms: 123,
            }],
        );
        assert_eq!(
            known,
            vec![fluxon_fs_core::config::FluxonFsTransferDispositionWire {
                root_relpath: "root".to_string(),
                generation: 9,
                batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
            }]
        );
    }

    #[test]
    fn test_build_scan_assignment_reuses_child_scan_unit_id() {
        let job = fluxon_fs_s3_gateway::FsTransferJobRecord {
            job_id: "job".to_string(),
            src_export: "src".to_string(),
            src_root_relpath: ".".to_string(),
            dst_export: "dst".to_string(),
            dst_root_relpath: ".".to_string(),
            desired_scan_concurrency: 1,
            desired_worker_count: 0,
            batch_ready_bytes: 8,
            job_spec_blob: Vec::new(),
            scan_epoch: 7,
            scan_finished: false,
            scan_discovered_batch_count: 0,
            scan_discovered_file_count: 0,
            scan_discovered_bytes: 0,
            ready_batch_count: 0,
            running_batch_count: 0,
            done_batch_count: 0,
            finished_batch_count: 0,
            expired_batch_count: 0,
            failed_file_count: 0,
            state: fluxon_fs_core::config::FluxonFsTransferJobState::Running,
            last_error: String::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        };
        let assignment = build_scan_assignment(
            &job,
            "src-exporter".to_string(),
            7,
            super::MasterScanUnit {
                scan_unit_id: Some("scan-child-cont".to_string()),
                root_relpath: "root".to_string(),
                generation: 3,
                scan_mode: FluxonFsTransferScanMode::FullTree,
            },
            Vec::new(),
            &[],
            &[],
            123,
        );
        assert_eq!(assignment.scan_unit_id, "scan-child-cont".to_string());
        assert_eq!(assignment.root_relpath, "root".to_string());
        assert_eq!(assignment.generation, 3);
    }

    #[test]
    fn test_reset_for_new_epoch_seeds_root_direct_fanout_only_scan_unit() {
        let mut runtime = TransferJobScanRuntime::default();
        runtime.reset_for_new_epoch(3, ".".to_string());
        assert_eq!(runtime.active_scan_epoch, 3);
        assert!(runtime.inflight.is_empty());
        assert_eq!(runtime.queue.len(), 1);
        let scan_unit = runtime.queue.front().unwrap();
        assert!(scan_unit.scan_unit_id.is_none());
        assert_eq!(scan_unit.root_relpath, ".".to_string());
        assert_eq!(scan_unit.generation, 1);
        assert_eq!(
            scan_unit.scan_mode,
            FluxonFsTransferScanMode::RootDirectFanoutOnly
        );
    }

    #[test]
    fn test_enqueue_child_units_prioritizes_newer_frontier_before_older_siblings() {
        let mut runtime = TransferJobScanRuntime::default();
        runtime.queue.push_back(MasterScanUnit {
            scan_unit_id: Some("older-sibling".to_string()),
            root_relpath: "root/older".to_string(),
            generation: 2,
            scan_mode: FluxonFsTransferScanMode::FullTree,
        });
        runtime.enqueue_child_units(vec![
            FluxonFsTransferScanChildUnitWire {
                scan_unit_id: "child-a".to_string(),
                root_relpath: "root/a".to_string(),
                generation: 3,
                scan_mode: FluxonFsTransferScanMode::FullTree,
            },
            FluxonFsTransferScanChildUnitWire {
                scan_unit_id: "child-b".to_string(),
                root_relpath: "root/b".to_string(),
                generation: 3,
                scan_mode: FluxonFsTransferScanMode::FullTree,
            },
        ]);
        let order = runtime
            .queue
            .iter()
            .map(|scan_unit| scan_unit.root_relpath.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            order,
            vec![
                "root/a".to_string(),
                "root/b".to_string(),
                "root/older".to_string(),
            ]
        );
    }

    #[test]
    fn test_build_live_child_scan_roots_for_scan_excludes_exact_root_and_collects_descendants() {
        let mut runtime = TransferJobScanRuntime::default();
        runtime.queue.push_back(MasterScanUnit {
            scan_unit_id: Some("scan-root".to_string()),
            root_relpath: "root".to_string(),
            generation: 1,
            scan_mode: FluxonFsTransferScanMode::RootDirectFanoutOnly,
        });
        runtime.queue.push_back(MasterScanUnit {
            scan_unit_id: Some("scan-child-queued".to_string()),
            root_relpath: "root/queued".to_string(),
            generation: 2,
            scan_mode: FluxonFsTransferScanMode::FullTree,
        });
        runtime.inflight.insert(
            "scan-child-inflight".to_string(),
            MasterInflightScan {
                assignment: FluxonFsTransferScanAssignmentWire {
                    job_id: "job".to_string(),
                    scan_epoch: 1,
                    scan_unit_id: "scan-child-inflight".to_string(),
                    scan_task_id: "task-child-inflight".to_string(),
                    root_relpath: "root/inflight".to_string(),
                    generation: 2,
                    scan_mode: FluxonFsTransferScanMode::FullTree,
                    src_export: "src".to_string(),
                    src_exporter_id: "src-exporter".to_string(),
                    batch_ready_bytes: 4,
                    lease_expire_unix_ms: 0,
                    known_dispositions: Vec::new(),
                    live_child_scan_roots: Vec::new(),
                    skip_entries: Vec::new(),
                },
                started_unix_ms: 0,
                last_accepted_event_seq_no: -1,
            },
        );

        let live_roots = build_live_child_scan_roots_for_scan(&runtime, "root");
        assert_eq!(
            live_roots,
            vec!["root/inflight".to_string(), "root/queued".to_string()]
        );
    }

    #[test]
    fn test_accept_scan_result_if_live_rejects_stale_scan_task_without_running_apply() {
        let runtime_state = Arc::new(Mutex::new(TransferScanRuntimeState::default()));
        let job_id = "job-1".to_string();
        let scan_epoch = 7;
        let live_assignment = FluxonFsTransferScanAssignmentWire {
            job_id: job_id.clone(),
            scan_epoch,
            scan_unit_id: "scan-1".to_string(),
            scan_task_id: "task-live".to_string(),
            root_relpath: "root".to_string(),
            generation: 1,
            scan_mode: FluxonFsTransferScanMode::FullTree,
            src_export: "src".to_string(),
            src_exporter_id: "src-exporter".to_string(),
            batch_ready_bytes: 4,
            lease_expire_unix_ms: i64::MAX,
            known_dispositions: Vec::new(),
            live_child_scan_roots: Vec::new(),
            skip_entries: Vec::new(),
        };
        {
            let mut runtime_guard = runtime_state.lock();
            let runtime = runtime_guard.jobs.entry(job_id.clone()).or_default();
            runtime.active_scan_epoch = scan_epoch;
            runtime.inflight.insert(
                live_assignment.scan_unit_id.clone(),
                MasterInflightScan {
                    assignment: live_assignment.clone(),
                    started_unix_ms: 0,
                    last_accepted_event_seq_no: -1,
                },
            );
        }
        let apply_count = Arc::new(AtomicUsize::new(0));
        let live_result = FluxonFsTransferScanResultWire {
            job_id: job_id.clone(),
            scan_epoch,
            scan_unit_id: live_assignment.scan_unit_id.clone(),
            scan_task_id: live_assignment.scan_task_id.clone(),
            root_relpath: live_assignment.root_relpath.clone(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![
                fluxon_fs_core::config::FluxonFsTransferScanBatchWire {
                    batch_id: "batch-live".to_string(),
                    root_relpath: "root".to_string(),
                    batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                    manifest_blob: fluxon_fs_core::config::FluxonFsTransferManifestWire::new(
                        vec![fluxon_fs_core::config::FluxonFsTransferManifestEntryWire {
                            relpath: "root/direct.bin".to_string(),
                            size: 3,
                        }],
                        Vec::new(),
                    )
                    .encode_to_blob()
                    .unwrap(),
                    collect_infos: Vec::new(),
                    generation: 1,
                },
            ],
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        };
        {
            let apply_count2 = apply_count.clone();
            assert!(
                accept_scan_result_if_live_with_apply(&runtime_state, live_result, move |_| {
                    apply_count2.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .unwrap()
            );
        }
        assert_eq!(apply_count.load(Ordering::SeqCst), 1);
        {
            let runtime_guard = runtime_state.lock();
            let runtime = runtime_guard.jobs.get(job_id.as_str()).unwrap();
            assert!(runtime.inflight.is_empty());
        }

        {
            let mut runtime_guard = runtime_state.lock();
            let runtime = runtime_guard.jobs.entry(job_id.clone()).or_insert_with(|| {
                let mut runtime = TransferJobScanRuntime::default();
                runtime.active_scan_epoch = scan_epoch;
                runtime
            });
            runtime.inflight.insert(
                "scan-2".to_string(),
                MasterInflightScan {
                    assignment: FluxonFsTransferScanAssignmentWire {
                        scan_unit_id: "scan-2".to_string(),
                        scan_task_id: "task-current".to_string(),
                        ..live_assignment.clone()
                    },
                    started_unix_ms: 0,
                    last_accepted_event_seq_no: -1,
                },
            );
        }
        let stale_result = FluxonFsTransferScanResultWire {
            scan_unit_id: "scan-2".to_string(),
            scan_task_id: "task-stale".to_string(),
            ..FluxonFsTransferScanResultWire {
                job_id: job_id.clone(),
                scan_epoch,
                scan_unit_id: String::new(),
                scan_task_id: String::new(),
                root_relpath: live_assignment.root_relpath.clone(),
                generation: 1,
                frontier: FluxonFsTransferScanFrontier {
                    direct_files: Vec::new(),
                    direct_dirs: Vec::new(),
                    empty_dirs: Vec::new(),
                },
                direct_files_only_batches: vec![
                    fluxon_fs_core::config::FluxonFsTransferScanBatchWire {
                        batch_id: "batch-stale".to_string(),
                        root_relpath: "root".to_string(),
                        batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                        manifest_blob: fluxon_fs_core::config::FluxonFsTransferManifestWire::new(
                            vec![fluxon_fs_core::config::FluxonFsTransferManifestEntryWire {
                                relpath: "root/stale.bin".to_string(),
                                size: 9,
                            }],
                            Vec::new(),
                        )
                        .encode_to_blob()
                        .unwrap(),
                        collect_infos: Vec::new(),
                        generation: 1,
                    },
                ],
                child_scan_units: Vec::new(),
                full_dir_batches: Vec::new(),
                finished: true,
            }
        };
        {
            let apply_count2 = apply_count.clone();
            assert!(
                !accept_scan_result_if_live_with_apply(&runtime_state, stale_result, move |_| {
                    apply_count2.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .unwrap()
            );
        }
        assert_eq!(apply_count.load(Ordering::SeqCst), 1);
        {
            let runtime_guard = runtime_state.lock();
            let runtime = runtime_guard.jobs.get(job_id.as_str()).unwrap();
            assert!(runtime.inflight.contains_key("scan-2"));
        }
    }

    #[test]
    fn test_exact_root_partial_direct_files_batch_does_not_block_same_root_continuation() {
        let root = TempDir::new().unwrap();
        let root_dir = root.path().join("root");
        fs::create_dir_all(&root_dir).unwrap();
        for idx in 0..4097 {
            let mut handle = fs::File::create(root_dir.join(format!("file-{idx:04}.bin"))).unwrap();
            handle.write_all(b"x").unwrap();
        }
        let first_assignment = FluxonFsTransferScanAssignmentWire {
            job_id: "job".to_string(),
            scan_epoch: 1,
            scan_unit_id: "scan-root".to_string(),
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
        let first_result =
            crate::agent_service::transfer_agent::build_transfer_scan_result_for_root_dir_abs(
                root.path().to_str().unwrap(),
                &first_assignment,
            )
            .unwrap();
        assert!(!first_result.finished);
        assert_eq!(first_result.child_scan_units.len(), 1);
        assert!(!first_result.direct_files_only_batches.is_empty());
        let durable_batches = first_result
            .direct_files_only_batches
            .iter()
            .map(|batch| fluxon_fs_s3_gateway::FsTransferBatchRecord {
                job_id: "job".to_string(),
                batch_id: batch.batch_id.clone(),
                root_relpath: batch.root_relpath.clone(),
                batch_kind: batch.batch_kind,
                state: FluxonFsTransferBatchState::Ready,
                assigned_src_exporter_id: String::new(),
                assigned_dst_exporter_id: String::new(),
                owner_worker_id: String::new(),
                owner_worker_task_id: String::new(),
                lease_expire_unix_ms: 0,
                manifest_blob: batch.manifest_blob.clone(),
                generation: batch.generation,
                last_counted_scan_epoch: 0,
            })
            .collect::<Vec<_>>();
        let continuation_assignment = FluxonFsTransferScanAssignmentWire {
            scan_task_id: "task-2".to_string(),
            known_dispositions: build_known_dispositions_for_scan(
                "job",
                "root",
                1,
                durable_batches.as_slice(),
                &[],
            ),
            ..first_assignment
        };
        let continuation_result =
            crate::agent_service::transfer_agent::build_transfer_scan_result_for_root_dir_abs(
                root.path().to_str().unwrap(),
                &continuation_assignment,
            )
            .unwrap();
        assert!(continuation_result.finished);
        assert!(continuation_result.child_scan_units.is_empty());
        let remaining_entry_count = continuation_result
            .direct_files_only_batches
            .iter()
            .map(|batch| {
                FluxonFsTransferManifestWire::decode_from_blob(batch.manifest_blob.as_slice())
                    .unwrap()
                    .entries
                    .len()
            })
            .sum::<usize>();
        assert_eq!(remaining_entry_count, 1);
    }

    #[test]
    fn test_stale_scan_result_does_not_produce_accepted_live_delta() {
        let runtime_state = Arc::new(Mutex::new(TransferScanRuntimeState::default()));
        let job_id = "job-1".to_string();
        let scan_epoch = 7;
        let live_assignment = FluxonFsTransferScanAssignmentWire {
            job_id: job_id.clone(),
            scan_epoch,
            scan_unit_id: "scan-2".to_string(),
            scan_task_id: "task-current".to_string(),
            root_relpath: "root".to_string(),
            generation: 1,
            scan_mode: FluxonFsTransferScanMode::FullTree,
            src_export: "src".to_string(),
            src_exporter_id: "src-exporter".to_string(),
            batch_ready_bytes: 4,
            lease_expire_unix_ms: i64::MAX,
            known_dispositions: Vec::new(),
            live_child_scan_roots: Vec::new(),
            skip_entries: Vec::new(),
        };
        {
            let mut runtime_guard = runtime_state.lock();
            let runtime = runtime_guard.jobs.entry(job_id.clone()).or_default();
            runtime.active_scan_epoch = scan_epoch;
            runtime.inflight.insert(
                live_assignment.scan_unit_id.clone(),
                MasterInflightScan {
                    assignment: live_assignment.clone(),
                    started_unix_ms: 0,
                    last_accepted_event_seq_no: -1,
                },
            );
        }
        let stale_result = FluxonFsTransferScanResultWire {
            job_id: job_id.clone(),
            scan_epoch,
            scan_unit_id: live_assignment.scan_unit_id.clone(),
            scan_task_id: "task-stale".to_string(),
            root_relpath: live_assignment.root_relpath.clone(),
            generation: 1,
            frontier: FluxonFsTransferScanFrontier {
                direct_files: Vec::new(),
                direct_dirs: Vec::new(),
                empty_dirs: Vec::new(),
            },
            direct_files_only_batches: vec![
                fluxon_fs_core::config::FluxonFsTransferScanBatchWire {
                    batch_id: "batch-stale".to_string(),
                    root_relpath: "root".to_string(),
                    batch_kind: FluxonFsTransferBatchKind::DirectFilesOnly,
                    manifest_blob: fluxon_fs_core::config::FluxonFsTransferManifestWire::new(
                        vec![fluxon_fs_core::config::FluxonFsTransferManifestEntryWire {
                            relpath: "root/stale.bin".to_string(),
                            size: 9,
                        }],
                        Vec::new(),
                    )
                    .encode_to_blob()
                    .unwrap(),
                    collect_infos: Vec::new(),
                    generation: 1,
                },
            ],
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            finished: true,
        };
        let accepted =
            accept_scan_result_if_live_with_apply(&runtime_state, stale_result.clone(), |_| Ok(()))
                .unwrap();
        assert!(!accepted);
        let live_detail_delta = scan_result_live_detail_delta(&stale_result).unwrap();
        assert_eq!(live_detail_delta, (1, 1, 9));
    }

    #[test]
    fn test_duplicate_scan_event_does_not_extend_inflight_lease() {
        let existing_lease_expire_unix_ms = 123_456;
        let existing_started_unix_ms = 98_765;
        let duplicate_event_seq_no = 2;
        let duplicate_ack =
            fluxon_fs_core::config::FluxonFsTransferScanEventAckWire::continue_running(
                false,
                existing_lease_expire_unix_ms,
            );
        assert!(!duplicate_ack.accepted);
        assert!(duplicate_ack.continue_running);
        assert_eq!(
            duplicate_ack.lease_expire_unix_ms,
            existing_lease_expire_unix_ms
        );

        let mut runtime = TransferJobScanRuntime::default();
        runtime.active_scan_epoch = 3;
        runtime.inflight.insert(
            "scan-dup".to_string(),
            MasterInflightScan {
                assignment: FluxonFsTransferScanAssignmentWire {
                    job_id: "job-dup".to_string(),
                    scan_epoch: 3,
                    scan_unit_id: "scan-dup".to_string(),
                    scan_task_id: "task-dup".to_string(),
                    root_relpath: "root".to_string(),
                    generation: 1,
                    scan_mode: FluxonFsTransferScanMode::FullTree,
                    src_export: "src".to_string(),
                    src_exporter_id: "src-exporter".to_string(),
                    batch_ready_bytes: 4,
                    lease_expire_unix_ms: existing_lease_expire_unix_ms,
                    known_dispositions: Vec::new(),
                    live_child_scan_roots: Vec::new(),
                    skip_entries: Vec::new(),
                },
                started_unix_ms: existing_started_unix_ms,
                last_accepted_event_seq_no: duplicate_event_seq_no,
            },
        );
        let inflight = runtime.inflight.get("scan-dup").unwrap();
        assert_eq!(
            inflight.assignment.lease_expire_unix_ms,
            existing_lease_expire_unix_ms
        );
        assert_eq!(inflight.started_unix_ms, existing_started_unix_ms);
        assert_eq!(inflight.last_accepted_event_seq_no, duplicate_event_seq_no);
    }

    #[test]
    fn test_expire_scan_timeouts_requeues_scan_after_duplicate_event() {
        let lease_expire_unix_ms = 10_000;
        let started_unix_ms = 9_000;
        let mut runtime = TransferJobScanRuntime::default();
        runtime.active_scan_epoch = 5;
        runtime.inflight.insert(
            "scan-timeout".to_string(),
            MasterInflightScan {
                assignment: FluxonFsTransferScanAssignmentWire {
                    job_id: "job-timeout".to_string(),
                    scan_epoch: 5,
                    scan_unit_id: "scan-timeout".to_string(),
                    scan_task_id: "task-timeout".to_string(),
                    root_relpath: "root/sub".to_string(),
                    generation: 4,
                    scan_mode: FluxonFsTransferScanMode::FullTree,
                    src_export: "src".to_string(),
                    src_exporter_id: "src-exporter".to_string(),
                    batch_ready_bytes: 4,
                    lease_expire_unix_ms,
                    known_dispositions: Vec::new(),
                    live_child_scan_roots: Vec::new(),
                    skip_entries: Vec::new(),
                },
                started_unix_ms,
                last_accepted_event_seq_no: 7,
            },
        );

        let duplicate_ack =
            fluxon_fs_core::config::FluxonFsTransferScanEventAckWire::continue_running(
                false,
                lease_expire_unix_ms,
            );
        assert!(!duplicate_ack.accepted);
        assert_eq!(duplicate_ack.lease_expire_unix_ms, lease_expire_unix_ms);

        expire_scan_timeouts(&mut runtime, lease_expire_unix_ms + 1);
        assert!(runtime.inflight.is_empty());
        assert_eq!(runtime.queue.len(), 1);
        let requeued = runtime.queue.front().unwrap();
        assert_eq!(requeued.root_relpath, "root/sub");
        assert_eq!(requeued.generation, 5);
        assert_eq!(requeued.scan_mode, FluxonFsTransferScanMode::FullTree);
    }

    #[test]
    fn test_finished_scan_event_is_acked_before_durable_finalize() {
        let runtime_state = Arc::new(Mutex::new(TransferScanRuntimeState::default()));
        let job_id = "job-finish".to_string();
        let scan_unit_id = "scan-finish".to_string();
        let scan_task_id = "task-finish".to_string();
        let scan_epoch = 11;
        {
            let mut runtime_guard = runtime_state.lock();
            let runtime = runtime_guard.jobs.entry(job_id.clone()).or_default();
            runtime.active_scan_epoch = scan_epoch;
            runtime.inflight.insert(
                scan_unit_id.clone(),
                MasterInflightScan {
                    assignment: FluxonFsTransferScanAssignmentWire {
                        job_id: job_id.clone(),
                        scan_epoch,
                        scan_unit_id: scan_unit_id.clone(),
                        scan_task_id: scan_task_id.clone(),
                        root_relpath: "root/empty".to_string(),
                        generation: 2,
                        scan_mode: FluxonFsTransferScanMode::FullTree,
                        src_export: "src".to_string(),
                        src_exporter_id: "src-exporter".to_string(),
                        batch_ready_bytes: 1024,
                        lease_expire_unix_ms: 123_456,
                        known_dispositions: Vec::new(),
                        live_child_scan_roots: Vec::new(),
                        skip_entries: Vec::new(),
                    },
                    started_unix_ms: 77,
                    last_accepted_event_seq_no: -1,
                },
            );
        }

        let event = FluxonFsTransferScanEventWire {
            job_id: job_id.clone(),
            scan_epoch,
            scan_unit_id: scan_unit_id.clone(),
            scan_task_id: scan_task_id.clone(),
            root_relpath: "root/empty".to_string(),
            generation: 2,
            event_seq_no: 0,
            event_kind: FluxonFsTransferScanEventKindWire::Finished,
            direct_files_only_batches: Vec::new(),
            child_scan_units: Vec::new(),
            full_dir_batches: Vec::new(),
            error_detail: String::new(),
        };

        let applied = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let applied2 = applied.clone();
        let ack = accept_scan_event_if_live_with_apply(&runtime_state, event.clone(), move |_| {
            applied2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
        assert!(ack.accepted);
        assert!(!ack.continue_running);
        assert_eq!(applied.load(std::sync::atomic::Ordering::SeqCst), 1);

        {
            let runtime_guard = runtime_state.lock();
            let runtime = runtime_guard.jobs.get(job_id.as_str()).unwrap();
            assert!(runtime.inflight.is_empty());
            assert_eq!(runtime.pending_finalize.len(), 1);
            assert_eq!(
                runtime
                    .pending_finalize
                    .get(scan_unit_id.as_str())
                    .unwrap()
                    .result
                    .root_relpath,
                "root/empty"
            );
        }

        let pending = {
            let runtime_guard = runtime_state.lock();
            take_retry_ready_pending_scan_finalize(
                runtime_guard.jobs.get(job_id.as_str()).unwrap(),
                i64::MAX,
            )
            .unwrap()
        };
        assert!(complete_pending_scan_finalize_if_live(
            &runtime_state,
            &pending,
        ));
        {
            let runtime_guard = runtime_state.lock();
            let runtime = runtime_guard.jobs.get(job_id.as_str()).unwrap();
            assert!(runtime.inflight.is_empty());
            assert!(runtime.queue.is_empty());
            assert!(runtime.pending_finalize.is_empty());
        }
    }
}
