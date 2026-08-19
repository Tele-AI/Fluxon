use super::{
    ClientKvApiInner, ClientKvApiView, OwnerKeyMaterializationGuard,
    OwnerKeyMaterializationOutcome, OwnerSlotLease,
};
use crate::cluster_manager::app_logic_ext::ClusterManagerAppLogicExt;
use crate::memholder::{MemoryInfo, UserMemHolder, UserMemHolderExposeKind};
// no StageScope; timestamps-based metrics only
use crate::observe_kvope::{obe_get_cache_hit, obe_get_cache_miss};
use crate::{
    cluster_manager::NodeID,
    master_kv_router::msg_pack::{
        BatchGetBindItemReq, BatchGetBindReq, BatchGetBindResp, BatchGetDoneItemReq,
        BatchGetDoneReq, BatchGetDoneResp, BatchGetRevokeReq, BatchGetRevokeResp,
        BatchGetStartItemResp, BatchGetStartReq, BatchGetStartResp, BatchIsExistReq,
        GetAllocationMode, GetBindTarget, GetDoneReq, GetDoneResp, GetMetaReq, GetMetaResp,
        GetPreparedLocalReserveTarget, GetRevokeReq, GetSourceKind, GetStartReq, GetStartResp,
    },
    owner_segment::{
        OwnerGeneration, OwnerGetDestinationCapability, OwnerGetSourceCapability,
        OwnerRouteCommitMode, OwnerSegmentTransferItem, OwnerSegmentTransferOutcome,
        OwnerTargetWriteCapability, OwnerTransferErrorCode, OwnerTransferOpId, OwnerTransferOpKind,
        OwnerTransferReceipt,
    },
    p2p::{control_plane_rpc::call_control_plane_rpc, msg_pack::MsgPack},
    rpcresp_kvresult_convert::msg_and_error::codes_api,
    rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult, OK},
};
use ::tokio::sync::Semaphore;
use futures::stream::{self, StreamExt};
use limit_thirdparty::tokio;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;

const BATCH_GET_DONE_MAX_INFLIGHT: usize = 4;

fn allocation_mode_installs_owner_index(mode: GetAllocationMode) -> bool {
    matches!(
        mode,
        GetAllocationMode::ReuseReplica
            | GetAllocationMode::DurableReplica
            | GetAllocationMode::LocalCommittedSlot
            | GetAllocationMode::RequesterLocalPromote
    )
}

fn batch_get_done_rpc_limiter() -> &'static Semaphore {
    static LIMITER: OnceLock<Semaphore> = OnceLock::new();
    LIMITER.get_or_init(|| Semaphore::new(BATCH_GET_DONE_MAX_INFLIGHT))
}

async fn release_prepared_get_target(
    inner: &ClientKvApiInner,
    target: &GetPreparedLocalReserveTarget,
) -> KvResult<()> {
    inner
        .owner_release_local_reserve_slot_lease(OwnerSlotLease {
            value_len: target.len,
            slot_size: target.capacity_bytes,
            slots: vec![target.clone()],
        })
        .await
}

fn batch_get_done_response_matches(get_ids: &[u64], response: &BatchGetDoneResp) -> bool {
    response.items.len() == get_ids.len()
        && response
            .items
            .iter()
            .zip(get_ids)
            .all(|(item, expected_get_id)| item.get_id == *expected_get_id)
}

fn late_target_matches_start(start: &BatchGetStartItemResp, target: &GetBindTarget) -> bool {
    match target {
        GetBindTarget::PreparedLocalReserve(expected) => {
            start.prepared_target.as_ref() == Some(expected)
                && start.reused_committed_slot.is_none()
                && start.target_addr == expected.addr
                && start.target_base_addr == expected.base_addr
        }
        GetBindTarget::RequesterLocalSource => {
            start.prepared_target.is_none()
                && start.source_kind == GetSourceKind::Memory
                && start.source_route_token.is_some()
                && start.src_addr == start.target_addr
                && start.src_base_addr == start.target_base_addr
                && start.reused_committed_slot.as_ref().is_some_and(|slot| {
                    slot.addr == start.src_addr
                        && slot.base_addr == start.src_base_addr
                        && slot.len == start.len
                })
        }
        GetBindTarget::ExternalSink(_) | GetBindTarget::Invalid => false,
    }
}

enum GetToTargetAttempt {
    Prepared {
        source_owner: OwnerGeneration,
        terminal_sequence: u64,
        capability: OwnerTargetWriteCapability,
        result: KvResult<OwnerTransferReceipt>,
        source_error_code: Option<OwnerTransferErrorCode>,
        transfer_us: i64,
        terminal_definitive: bool,
    },
    Rejected(KvError),
}

struct PendingGetToTarget {
    index: usize,
    capability: OwnerTargetWriteCapability,
    item: OwnerSegmentTransferItem,
}

fn owner_source_lease_error(context: &str, detail: impl Into<String>) -> KvError {
    KvError::Api(ApiError::Unknown {
        detail: format!("{context}: {}", detail.into()),
    })
}

fn prepare_claimed_get_target_error(
    key: &str,
    get_id: u64,
    error: crate::owner_segment::OwnerTransferItemError,
) -> KvError {
    if error.code == OwnerTransferErrorCode::StalePlan {
        KvError::Api(ApiError::StaleGetPlan {
            get_id,
            key: key.to_string(),
            detail: error.detail,
        })
    } else {
        owner_source_lease_error(
            "PrepareTarget",
            format!("{:?}: {}", error.code, error.detail),
        )
    }
}

fn planned_get_source_terminal_error(
    key: &str,
    get_id: u64,
    source_error_code: Option<OwnerTransferErrorCode>,
    error: KvError,
) -> KvError {
    if matches!(
        source_error_code,
        Some(
            OwnerTransferErrorCode::NotFound
                | OwnerTransferErrorCode::Reclaiming
                | OwnerTransferErrorCode::StaleGeneration
        )
    ) {
        KvError::Api(ApiError::StaleGetPlan {
            get_id,
            key: key.to_string(),
            detail: format!("GetToTarget source became stale after Plan: {error}"),
        })
    } else {
        error
    }
}

async fn execute_get_to_target_items(
    inner: &ClientKvApiInner,
    keys: &[String],
    start_items: &[BatchGetStartItemResp],
    late_targets: &[Option<GetBindTarget>],
) -> Vec<Option<GetToTargetAttempt>> {
    let mut results = std::iter::repeat_with(|| None)
        .take(start_items.len())
        .collect::<Vec<_>>();
    let self_info = inner.view.cluster_manager().get_self_info();
    let coordinator = OwnerGeneration::new(self_info.id.clone(), self_info.node_start_time);
    let mut by_source = HashMap::<OwnerGeneration, Vec<PendingGetToTarget>>::new();

    for (index, ((key, start), late_target)) in
        keys.iter().zip(start_items).zip(late_targets).enumerate()
    {
        if start.error_code != OK
            || start.prepared_target.is_none()
            || late_target
                .as_ref()
                .is_some_and(|target| !late_target_matches_start(start, target))
        {
            continue;
        }
        let source = match start.source_kind {
            GetSourceKind::Memory => match start.source_route_token.as_ref() {
                Some(token) if start.ssd_source_route_token.is_none() => {
                    OwnerGetSourceCapability::Memory(token.clone())
                }
                _ => continue,
            },
            GetSourceKind::Ssd => match start.ssd_source_route_token.as_ref() {
                Some(token) if start.source_route_token.is_none() => {
                    OwnerGetSourceCapability::Ssd(token.clone())
                }
                _ => continue,
            },
        };
        let source_owner = source
            .owner()
            .expect("validated Get source capability has an owner")
            .clone();
        let target = start
            .prepared_target
            .as_ref()
            .expect("prepared late target must materialize into the start item")
            .clone();
        let Some(sequence) = start.get_id.checked_add(1) else {
            results[index] = Some(GetToTargetAttempt::Rejected(owner_source_lease_error(
                "GetToTarget",
                "master Get id overflow",
            )));
            continue;
        };
        let op_id = OwnerTransferOpId::new(coordinator.clone(), sequence, OwnerTransferOpKind::Get);
        let prepared = inner
            .owner_segment_allocator
            .lock()
            .prepare_claimed_get_target(
                op_id.clone(),
                key.clone(),
                start.put_id,
                start.atomic_group.clone(),
                target,
            );
        let (lease_id, slot) = match prepared {
            OwnerSegmentTransferOutcome::TargetPrepared { lease_id, slot, .. }
            | OwnerSegmentTransferOutcome::TargetDataReady { lease_id, slot } => (lease_id, slot),
            OwnerSegmentTransferOutcome::Error(error) => {
                results[index] = Some(GetToTargetAttempt::Rejected(
                    prepare_claimed_get_target_error(key, start.get_id, error),
                ));
                continue;
            }
            other => {
                results[index] = Some(GetToTargetAttempt::Rejected(owner_source_lease_error(
                    "PrepareTarget",
                    format!("unexpected owner target terminal: {other:?}"),
                )));
                continue;
            }
        };
        let capability = OwnerTargetWriteCapability {
            operation: op_id.clone(),
            lease_id,
            slot,
        };
        by_source
            .entry(source_owner)
            .or_default()
            .push(PendingGetToTarget {
                index,
                capability: capability.clone(),
                item: OwnerSegmentTransferItem::GetToTarget {
                    op_id,
                    source,
                    destination: OwnerGetDestinationCapability::OwnerSlot(capability),
                },
            });
    }

    for (source, pending) in by_source {
        let items = pending
            .iter()
            .map(|pending| pending.item.clone())
            .collect::<Vec<_>>();
        let transfer_started_at = Instant::now();
        match super::put::owner_segment_transfer_batch_until_definitive(
            inner,
            &source,
            items,
            "get_to_target",
        )
        .await
        {
            Ok(responses) if responses.len() == pending.len() => {
                let transfer_us = transfer_started_at
                    .elapsed()
                    .as_micros()
                    .min(i64::MAX as u128) as i64;
                for (pending, response) in pending.into_iter().zip(responses) {
                    let terminal_sequence = response.terminal_sequence;
                    let (result, source_error_code) = match response.outcome {
                        OwnerSegmentTransferOutcome::GetToTargetCompleted { receipt }
                            if receipt.destination
                                == OwnerGetDestinationCapability::OwnerSlot(
                                    pending.capability.clone(),
                                ) =>
                        {
                            let owner_receipt = OwnerTransferReceipt {
                                completion_id: receipt.completion_id,
                                direction: receipt.direction,
                                bytes: receipt.bytes,
                                source: Some(receipt.source.clone()),
                                target: pending.capability.slot.clone(),
                                source_registration_epoch: receipt
                                    .source
                                    .segment_registration_epoch,
                                target_registration_epoch: pending
                                    .capability
                                    .slot
                                    .segment_registration_epoch,
                            };
                            match inner.owner_segment_allocator.lock().mark_target_data_ready(
                                &pending.capability.operation,
                                &pending.capability.lease_id,
                                owner_receipt.clone(),
                            ) {
                                OwnerSegmentTransferOutcome::TargetDataReady { .. }
                                | OwnerSegmentTransferOutcome::TargetCommitted { .. } => {
                                    (Ok(owner_receipt), None)
                                }
                                terminal => (
                                    Err(owner_source_lease_error(
                                        "GetToTarget",
                                        format!(
                                            "requester rejected source completion receipt: {terminal:?}"
                                        ),
                                    )),
                                    None,
                                ),
                            }
                        }
                        OwnerSegmentTransferOutcome::Error(error) => {
                            let code = error.code;
                            (
                                Err(owner_source_lease_error(
                                    "GetToTarget",
                                    format!("{:?}: {}", code, error.detail),
                                )),
                                Some(code),
                            )
                        }
                        other => (
                            Err(owner_source_lease_error(
                                "GetToTarget",
                                format!("unexpected source terminal: {other:?}"),
                            )),
                            None,
                        ),
                    };
                    results[pending.index] = Some(GetToTargetAttempt::Prepared {
                        source_owner: source.clone(),
                        terminal_sequence,
                        capability: pending.capability,
                        result,
                        source_error_code,
                        transfer_us,
                        terminal_definitive: true,
                    });
                }
            }
            Ok(responses) => {
                let detail = format!(
                    "owner response length mismatch: expected={} got={}",
                    pending.len(),
                    responses.len()
                );
                for pending in pending {
                    results[pending.index] = Some(GetToTargetAttempt::Prepared {
                        source_owner: source.clone(),
                        terminal_sequence: 0,
                        capability: pending.capability,
                        result: Err(owner_source_lease_error("GetToTarget", &detail)),
                        source_error_code: None,
                        transfer_us: transfer_started_at
                            .elapsed()
                            .as_micros()
                            .min(i64::MAX as u128) as i64,
                        terminal_definitive: false,
                    });
                }
            }
            Err(error) => {
                let detail = error.to_string();
                for pending in pending {
                    results[pending.index] = Some(GetToTargetAttempt::Prepared {
                        source_owner: source.clone(),
                        terminal_sequence: 0,
                        capability: pending.capability,
                        result: Err(owner_source_lease_error("GetToTarget", &detail)),
                        source_error_code: None,
                        transfer_us: transfer_started_at
                            .elapsed()
                            .as_micros()
                            .min(i64::MAX as u128) as i64,
                        terminal_definitive: false,
                    });
                }
            }
        }
    }
    results
}

fn abort_unpublished_get_target(
    inner: &ClientKvApiInner,
    capability: &OwnerTargetWriteCapability,
    reason: &str,
) -> KvResult<()> {
    match inner.owner_segment_allocator.lock().abort_target(
        &capability.operation,
        &capability.lease_id,
        reason.to_string(),
    ) {
        OwnerSegmentTransferOutcome::TargetAborted => Ok(()),
        terminal => Err(owner_source_lease_error(
            "AbortTarget",
            format!("unexpected target terminal: {terminal:?}"),
        )),
    }
}

#[derive(Clone)]
pub(crate) struct StartedGetRevokeCleanup {
    pub(crate) get_id: u64,
    pub(crate) prepared_target: Option<GetPreparedLocalReserveTarget>,
}

async fn run_started_get_revoke_cleanup(
    view: ClientKvApiView,
    pending: Vec<StartedGetRevokeCleanup>,
    context: &'static str,
) {
    if pending.is_empty() {
        return;
    }
    let mut attempt = 1u32;
    loop {
        let get_ids = pending.iter().map(|item| item.get_id).collect::<Vec<_>>();
        let response = view.client_kv_api().inner().batch_get_revoke(get_ids).await;
        let resp = match response {
            Ok(resp)
                if resp.items.len() == pending.len()
                    && resp
                        .items
                        .iter()
                        .zip(&pending)
                        .all(|(resp, expected)| resp.get_id == expected.get_id) =>
            {
                resp
            }
            Ok(resp) => {
                tracing::warn!(
                    "{} Revoke response shape/identity mismatch; retaining prepared slots for retry: expected={} got={} attempt={}",
                    context,
                    pending.len(),
                    resp.items.len(),
                    attempt
                );
                tokio::time::sleep(Duration::from_millis(
                    (50u64.saturating_mul(1u64 << attempt.min(6))).min(2_000),
                ))
                .await;
                attempt = attempt.saturating_add(1);
                continue;
            }
            Err(err) => {
                if matches!(&err, KvError::Api(ApiError::SystemShutdown { .. })) {
                    tracing::warn!(
                        "{} Revoke cleanup stopped during owner shutdown: items={}",
                        context,
                        pending.len()
                    );
                    return;
                }
                tracing::warn!(
                    "{} Revoke uncertain; retaining get ids and prepared slots for retry: items={} attempt={} err={}",
                    context,
                    pending.len(),
                    attempt,
                    err
                );
                tokio::time::sleep(Duration::from_millis(
                    (50u64.saturating_mul(1u64 << attempt.min(6))).min(2_000),
                ))
                .await;
                attempt = attempt.saturating_add(1);
                continue;
            }
        };

        for (expected, item_resp) in pending.iter().zip(resp.items) {
            if let Err(err) = crate::rpcresp_kvresult_convert::try_from_code(
                item_resp.error_code,
                item_resp.error_json,
            ) {
                // A terminal Done may have won.  Never release a possibly
                // committed slot from the losing Revoke path.
                tracing::warn!(
                    "{} Revoke reached non-releasable terminal: get_id={} err={}",
                    context,
                    expected.get_id,
                    err
                );
                continue;
            }
            let Some(target) = expected.prepared_target.as_ref() else {
                continue;
            };
            let mut release_attempt = 1u32;
            loop {
                match release_prepared_get_target(view.client_kv_api().inner(), target).await {
                    Ok(()) => break,
                    Err(err) => {
                        tracing::error!(
                            "{} Revoke confirmed but prepared slot release failed; retrying: get_id={} attempt={} err={}",
                            context,
                            expected.get_id,
                            release_attempt,
                            err
                        );
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        release_attempt = release_attempt.saturating_add(1);
                    }
                }
            }
        }
        return;
    }
}

/// Move cleanup ownership to a registered task before awaiting it.  If the
/// caller future is cancelled, the task still drives Revoke to a definite
/// terminal and releases only confirmed-uncommitted prepared slots.
pub(crate) async fn finish_started_get_revoke_cleanup(
    inner: &ClientKvApiInner,
    pending: Vec<StartedGetRevokeCleanup>,
    context: &'static str,
) {
    if pending.is_empty() {
        return;
    }
    let (done_tx, done_rx) = ::tokio::sync::oneshot::channel::<()>();
    let spawn_view = inner.view.clone_view();
    let worker_view = spawn_view.clone();
    spawn_view.spawn("started_get_revoke_cleanup", async move {
        run_started_get_revoke_cleanup(worker_view, pending, context).await;
        let _ = done_tx.send(());
    });
    let _ = done_rx.await;
}

#[derive(Debug, Clone)]
pub struct RemoteGetInfo {
    get_id: u64,
    data_len: usize,
    src_addr: u64,
    target_addr: u64,
    node_id: NodeID,
    peer_is_src_or_target: bool,
}

impl std::fmt::Display for RemoteGetInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GetInfo{{ get_id: {}, data_len: {} bytes, src_addr: {:#x}, target_addr: {:#x}, node_id: {:?}, remote_transfer: {} }}",
            self.get_id,
            self.data_len,
            self.src_addr,
            self.target_addr,
            self.node_id,
            self.peer_is_src_or_target
        )
    }
}

impl RemoteGetInfo {
    pub fn data_len(&self) -> usize {
        self.data_len
    }

    pub fn is_remote_transfer(&self) -> bool {
        self.peer_is_src_or_target
    }
}

struct OwnerGetDonePending {
    idx: usize,
    key: String,
    start_item: BatchGetStartItemResp,
    peer_is_remote: bool,
    transfer_us: i64,
    owner_slot_memory_info: Option<Arc<MemoryInfo>>,
    target_write_capability: Option<OwnerTargetWriteCapability>,
    get_to_target_terminal: Option<(OwnerGeneration, u64)>,
    late_target: Option<GetBindTarget>,
    materialization_guard: Option<OwnerKeyMaterializationGuard>,
}

async fn run_async_owner_get_route_commit(
    view: ClientKvApiView,
    mut pending: Vec<OwnerGetDonePending>,
) {
    let inner = view.client_kv_api().inner();
    let mut attempt = 1u32;
    while !pending.is_empty() && view.register_shutdown_poller().is_running() {
        let done_items = pending
            .iter()
            .map(|item| BatchGetDoneItemReq {
                get_id: item.start_item.get_id,
                late_target: item.late_target.clone(),
            })
            .collect::<Vec<_>>();
        let get_ids = done_items
            .iter()
            .map(|item| item.get_id)
            .collect::<Vec<_>>();
        let response = inner.batch_get_done_items(done_items).await;
        let mut retry_pending = Vec::new();
        match response {
            Ok(response) if batch_get_done_response_matches(&get_ids, &response) => {
                for (mut item, terminal) in pending.into_iter().zip(response.items) {
                    let expected_mode = if item.start_item.prepared_target.is_some() {
                        GetAllocationMode::LocalCommittedSlot
                    } else {
                        GetAllocationMode::RequesterLocalPromote
                    };
                    let terminal_result = crate::rpcresp_kvresult_convert::try_from_code(
                        terminal.error_code,
                        terminal.error_json.clone(),
                    );
                    if let Err(error) = terminal_result {
                        // The RPC itself completed and this item carries a
                        // master terminal.  In particular, a late target can
                        // reach this point after its 60-second Plan expired
                        // while owner allocation pressure was waiting for a
                        // slot.  Replaying the same get_id can never recreate
                        // that Plan.  Keeping the key materialization fence in
                        // RoutePending would instead block a later
                        // PutFromSource forever.
                        //
                        // The caller may already be consuming the returned
                        // bytes because this is the Async path.  Aborting the
                        // hidden cache target therefore removes only route
                        // eligibility and its hidden index reference; the
                        // resident MemoryInfo holder keeps the physical slot
                        // alive until the caller releases it.
                        let target_aborted =
                            inner.abort_hidden_pending_local_get(&item.key, item.start_item.get_id);
                        if target_aborted {
                            if let Some(mut guard) = item.materialization_guard.take() {
                                guard.finish(OwnerKeyMaterializationOutcome::Failed);
                            }
                            tracing::warn!(
                                key = item.key,
                                get_id = item.start_item.get_id,
                                attempt,
                                error = %error,
                                "Async Get CommitTarget reached a rejected master terminal; removed the unpublishable cache target"
                            );
                        } else {
                            tracing::error!(
                                key = item.key,
                                get_id = item.start_item.get_id,
                                attempt,
                                error = %error,
                                "Async Get CommitTarget was rejected and its hidden cache target could not be closed"
                            );
                            // Preserve the materialization fence until local
                            // state reaches a definite terminal.  This is an
                            // invariant-recovery path; the normal
                            // DataReady/RoutePending states close above.
                            retry_pending.push(item);
                        }
                        continue;
                    }
                    if terminal.allocation_mode != expected_mode {
                        tracing::error!(
                            key = item.key,
                            get_id = item.start_item.get_id,
                            expected_mode = ?expected_mode,
                            actual_mode = ?terminal.allocation_mode,
                            "Async Get CommitTarget changed allocation mode; retaining RoutePending payload"
                        );
                        retry_pending.push(item);
                        continue;
                    }
                    match inner.promote_hidden_owner_slot_get(
                        &item.key,
                        item.start_item.get_id,
                        item.start_item.put_id,
                    ) {
                        Ok(memory_info) => {
                            if let Some(prepared) = item.owner_slot_memory_info.as_ref() {
                                assert!(
                                    Arc::ptr_eq(prepared, &memory_info),
                                    "Async Get commit must retain the unique owner-slot MemoryInfo"
                                );
                            }
                            inner.owner_hot_track_committed(
                                &item.key,
                                item.start_item.put_id,
                                &memory_info,
                            );
                            if let Some(mut guard) = item.materialization_guard.take() {
                                guard.finish(OwnerKeyMaterializationOutcome::Committed);
                            }
                        }
                        Err(error) => {
                            tracing::warn!(
                                key = item.key,
                                get_id = item.start_item.get_id,
                                attempt,
                                error = %error,
                                "Async Get master route committed but local CommitTarget is not ready; replaying"
                            );
                            retry_pending.push(item);
                        }
                    }
                }
            }
            Ok(response) => {
                tracing::warn!(
                    expected_items = get_ids.len(),
                    actual_items = response.items.len(),
                    attempt,
                    "Async Get CommitTarget response identity is uncertain; replaying the same operations"
                );
                retry_pending = pending;
            }
            Err(error) => {
                tracing::warn!(
                    items = get_ids.len(),
                    attempt,
                    error = %error,
                    "Async Get CommitTarget RPC is uncertain; replaying the same operations"
                );
                retry_pending = pending;
            }
        }
        pending = retry_pending;
        if pending.is_empty() {
            break;
        }
        let retry_delay =
            Duration::from_millis((10u64.saturating_mul(1u64 << attempt.min(8))).min(2_000));
        attempt = attempt.saturating_add(1);
        tokio::time::sleep(retry_delay).await;
    }
    if !pending.is_empty() {
        tracing::warn!(
            items = pending.len(),
            "Async Get CommitTarget stopped during shutdown with RoutePending payloads"
        );
    }
}

impl ClientKvApiInner {
    pub async fn batch_get_finish_started(
        &self,
        keys: Vec<String>,
        start_items: Vec<BatchGetStartItemResp>,
        late_targets: Vec<Option<GetBindTarget>>,
        transfer_concurrency: usize,
    ) -> KvResult<Vec<KvResult<Option<(Arc<UserMemHolder>, Option<RemoteGetInfo>)>>>> {
        self.batch_get_finish_started_with_commit_mode(
            keys,
            start_items,
            late_targets,
            transfer_concurrency,
            OwnerRouteCommitMode::Async,
        )
        .await
    }

    pub(crate) async fn batch_get_finish_started_with_commit_mode(
        &self,
        keys: Vec<String>,
        start_items: Vec<BatchGetStartItemResp>,
        late_targets: Vec<Option<GetBindTarget>>,
        transfer_concurrency: usize,
        route_commit_mode: OwnerRouteCommitMode,
    ) -> KvResult<Vec<KvResult<Option<(Arc<UserMemHolder>, Option<RemoteGetInfo>)>>>> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_get_finish_started"
                    .to_string(),
            }));
        }
        if keys.len() != start_items.len() || keys.len() != late_targets.len() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "batch_get_finish_started length mismatch: keys={} start_items={} late_targets={}",
                    keys.len(),
                    start_items.len(),
                    late_targets.len(),
                ),
            }));
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let lifecycle_started_at = Instant::now();
        let lifecycle_requested_keys = keys.len();

        let transfer_concurrency = transfer_concurrency.max(1);
        let metrics = self.metrics_handle();
        let client_id = self.client_id_str();
        let node_role = self.node_role();
        let self_node_id = self.view.cluster_manager().get_self_info().id.clone();
        let mut get_to_target_results =
            execute_get_to_target_items(self, &keys, &start_items, &late_targets).await;

        let mut results: Vec<
            Option<KvResult<Option<(Arc<UserMemHolder>, Option<RemoteGetInfo>)>>>,
        > = (0..keys.len()).map(|_| None).collect();
        let mut done_pending = Vec::new();
        let mut transfer_error_cleanup = Vec::new();
        let mut transfer_futures = Vec::new();
        let mut lifecycle_zero_copy_items = 0usize;
        let mut lifecycle_transfer_items = 0usize;
        let mut lifecycle_remote_transfer_items = 0usize;
        let mut lifecycle_transfer_bytes = 0u64;
        let mut lifecycle_remote_transfer_bytes = 0u64;
        let mut lifecycle_transfer_sum_us = 0u64;
        let mut lifecycle_transfer_max_us = 0u64;
        let mut lifecycle_local_ssd_items = 0usize;
        let mut lifecycle_remote_ssd_items = 0usize;
        let mut lifecycle_transfer_source_nodes = HashSet::new();
        let mut lifecycle_ssd_source_nodes = HashSet::new();

        for (idx, ((key, start_item), late_target)) in keys
            .into_iter()
            .zip(start_items.into_iter())
            .zip(late_targets)
            .enumerate()
        {
            if let Some(target) = late_target.as_ref()
                && !late_target_matches_start(&start_item, target)
            {
                let mismatch = KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "late Get target does not match the transfer descriptor: key={} get_id={}",
                        key, start_item.get_id
                    ),
                });
                results[idx] = Some(match start_item.prepared_target.as_ref() {
                    Some(target) => release_prepared_get_target(self, target)
                        .await
                        .map(|_| ())
                        .and(Err(mismatch)),
                    None => Err(mismatch),
                });
                continue;
            }
            if start_item.error_code == codes_api::API_KEY_NOT_FOUND {
                results[idx] = Some(match start_item.prepared_target.as_ref() {
                    Some(target) => release_prepared_get_target(self, target)
                        .await
                        .map(|_| None),
                    None => Ok(None),
                });
                continue;
            }
            if let Err(err) = crate::rpcresp_kvresult_convert::try_from_code(
                start_item.error_code,
                start_item.error_json.clone(),
            ) {
                results[idx] = Some(match start_item.prepared_target.as_ref() {
                    Some(target) => release_prepared_get_target(self, target)
                        .await
                        .map(|_| ())
                        .and(Err(err)),
                    None => Err(err),
                });
                continue;
            }

            let get_to_target = get_to_target_results[idx].take();

            let direct_target = match get_to_target {
                Some(GetToTargetAttempt::Prepared {
                    source_owner,
                    terminal_sequence,
                    capability,
                    result: Ok(receipt),
                    source_error_code: _,
                    transfer_us,
                    terminal_definitive: _,
                }) => Some((
                    capability,
                    receipt,
                    transfer_us,
                    source_owner,
                    terminal_sequence,
                )),
                Some(GetToTargetAttempt::Prepared {
                    source_owner,
                    terminal_sequence,
                    capability,
                    result: Err(error),
                    source_error_code,
                    terminal_definitive,
                    ..
                }) => {
                    let abort = terminal_definitive.then(|| {
                        abort_unpublished_get_target(
                            self,
                            &capability,
                            "GetToTarget reached a failed terminal",
                        )
                    });
                    transfer_error_cleanup.push(StartedGetRevokeCleanup {
                        get_id: start_item.get_id,
                        prepared_target: None,
                    });
                    let terminal_can_ack =
                        terminal_definitive && abort.as_ref().is_some_and(|result| result.is_ok());
                    let error = planned_get_source_terminal_error(
                        &key,
                        start_item.get_id,
                        source_error_code,
                        error,
                    );
                    results[idx] = Some(match abort {
                        None | Some(Ok(())) => Err(error),
                        Some(Err(abort_error)) => Err(owner_source_lease_error(
                            "GetToTarget",
                            format!(
                                "transfer failed ({error}); target abort also failed ({abort_error})"
                            ),
                        )),
                    });
                    if terminal_can_ack {
                        self.owner_transfer_peer_tracker
                            .record_terminal(&source_owner, terminal_sequence);
                    }
                    continue;
                }
                Some(GetToTargetAttempt::Rejected(error)) => {
                    transfer_error_cleanup.push(StartedGetRevokeCleanup {
                        get_id: start_item.get_id,
                        prepared_target: start_item.prepared_target.clone(),
                    });
                    results[idx] = Some(Err(error));
                    continue;
                }
                None => None,
            };
            if start_item.source_kind == GetSourceKind::Ssd && direct_target.is_none() {
                transfer_error_cleanup.push(StartedGetRevokeCleanup {
                    get_id: start_item.get_id,
                    prepared_target: start_item.prepared_target.clone(),
                });
                results[idx] = Some(Err(owner_source_lease_error(
                    "SSD GetToTarget",
                    "master did not provide an owner SSD source token and requester target",
                )));
                continue;
            }
            let requester_local_same_slot = start_item.source_kind == GetSourceKind::Memory
                && start_item.node_id == self_node_id
                && start_item.reused_committed_slot.is_some()
                && start_item.src_addr == start_item.target_addr;
            if start_item.source_route_token.is_some()
                && direct_target.is_none()
                && !requester_local_same_slot
            {
                transfer_error_cleanup.push(StartedGetRevokeCleanup {
                    get_id: start_item.get_id,
                    prepared_target: start_item.prepared_target.clone(),
                });
                results[idx] = Some(Err(owner_source_lease_error(
                    "GetToTarget",
                    "owner memory source cannot fall back to requester RDMA READ",
                )));
                continue;
            }

            let peer_id = if start_item.node_id == self_node_id {
                None
            } else {
                Some(start_item.node_id.clone())
            };
            let peer_is_remote = peer_id.is_some();
            let get_id = start_item.get_id;
            let src_addr = start_item.src_addr;
            let target_addr = start_item.target_addr;
            let len = start_item.len;

            lifecycle_transfer_source_nodes.insert(start_item.node_id.to_string());
            if start_item.source_kind == GetSourceKind::Ssd {
                lifecycle_ssd_source_nodes.insert(start_item.node_id.to_string());
                if peer_is_remote {
                    lifecycle_remote_ssd_items = lifecycle_remote_ssd_items.saturating_add(1);
                } else {
                    lifecycle_local_ssd_items = lifecycle_local_ssd_items.saturating_add(1);
                }
            }

            if let Some((capability, receipt, transfer_us, source_owner, terminal_sequence)) =
                direct_target
            {
                debug_assert_eq!(receipt.target, capability.slot);
                lifecycle_transfer_items = lifecycle_transfer_items.saturating_add(1);
                lifecycle_transfer_bytes = lifecycle_transfer_bytes.saturating_add(len);
                if peer_is_remote {
                    lifecycle_remote_transfer_items =
                        lifecycle_remote_transfer_items.saturating_add(1);
                    lifecycle_remote_transfer_bytes =
                        lifecycle_remote_transfer_bytes.saturating_add(len);
                }
                let transfer_us_u64 = transfer_us.max(0) as u64;
                lifecycle_transfer_sum_us =
                    lifecycle_transfer_sum_us.saturating_add(transfer_us_u64);
                lifecycle_transfer_max_us = lifecycle_transfer_max_us.max(transfer_us_u64);
                done_pending.push(OwnerGetDonePending {
                    idx,
                    key,
                    start_item,
                    peer_is_remote,
                    transfer_us,
                    owner_slot_memory_info: None,
                    target_write_capability: Some(capability),
                    get_to_target_terminal: Some((source_owner, terminal_sequence)),
                    late_target,
                    materialization_guard: None,
                });
                continue;
            }

            if start_item.source_kind == GetSourceKind::Memory
                && peer_id.is_none()
                && src_addr == target_addr
            {
                lifecycle_zero_copy_items = lifecycle_zero_copy_items.saturating_add(1);
                done_pending.push(OwnerGetDonePending {
                    idx,
                    key,
                    start_item,
                    peer_is_remote,
                    transfer_us: 0,
                    owner_slot_memory_info: None,
                    target_write_capability: None,
                    get_to_target_terminal: None,
                    late_target,
                    materialization_guard: None,
                });
                continue;
            }

            lifecycle_transfer_items = lifecycle_transfer_items.saturating_add(1);
            lifecycle_transfer_bytes = lifecycle_transfer_bytes.saturating_add(len);
            if peer_is_remote {
                lifecycle_remote_transfer_items = lifecycle_remote_transfer_items.saturating_add(1);
                lifecycle_remote_transfer_bytes =
                    lifecycle_remote_transfer_bytes.saturating_add(len);
            }

            transfer_futures.push(async move {
                let transfer_started_at = Instant::now();
                let transfer_result = self
                    .view
                    .client_transfer_engine()
                    .transfer_data_no_copy(peer_id, true, src_addr, target_addr, len, None)
                    .await
                    .map(|_| ())
                    .map_err(|err| {
                        KvError::Api(ApiError::Transfer {
                            from_addr: src_addr,
                            to_addr: target_addr,
                            len,
                            error: err.to_string(),
                        })
                    });
                let transfer_us = transfer_started_at
                    .elapsed()
                    .as_micros()
                    .min(i64::MAX as u128) as i64;
                (
                    idx,
                    key,
                    start_item,
                    peer_is_remote,
                    get_id,
                    transfer_us,
                    transfer_result,
                    late_target,
                )
            });
        }

        let lifecycle_plan_us = lifecycle_started_at
            .elapsed()
            .as_micros()
            .min(i64::MAX as u128) as i64;
        let transfer_wall_started_at = Instant::now();
        let mut transfer_stream =
            stream::iter(transfer_futures).buffer_unordered(transfer_concurrency);
        while let Some(joined) = transfer_stream.next().await {
            match joined {
                (
                    idx,
                    key,
                    start_item,
                    peer_is_remote,
                    _get_id,
                    transfer_us,
                    Ok(_breakdown),
                    late_target,
                ) => {
                    let transfer_us_u64 = transfer_us.max(0) as u64;
                    lifecycle_transfer_sum_us =
                        lifecycle_transfer_sum_us.saturating_add(transfer_us_u64);
                    lifecycle_transfer_max_us = lifecycle_transfer_max_us.max(transfer_us_u64);
                    done_pending.push(OwnerGetDonePending {
                        idx,
                        key,
                        start_item,
                        peer_is_remote,
                        transfer_us,
                        owner_slot_memory_info: None,
                        target_write_capability: None,
                        get_to_target_terminal: None,
                        late_target,
                        materialization_guard: None,
                    });
                }
                (
                    idx,
                    _key,
                    start_item,
                    _peer_is_remote,
                    get_id,
                    _transfer_us,
                    Err(err),
                    _late_target,
                ) => {
                    results[idx] = Some(Err(err));
                    transfer_error_cleanup.push(StartedGetRevokeCleanup {
                        get_id,
                        prepared_target: start_item.prepared_target,
                    });
                }
            }
        }
        let lifecycle_transfer_wall_us = transfer_wall_started_at
            .elapsed()
            .as_micros()
            .min(i64::MAX as u128) as i64;

        let transfer_cleanup_started_at = Instant::now();
        finish_started_get_revoke_cleanup(
            self,
            transfer_error_cleanup,
            "batch_get transfer failure",
        )
        .await;
        let lifecycle_transfer_cleanup_us = transfer_cleanup_started_at
            .elapsed()
            .as_micros()
            .min(i64::MAX as u128) as i64;

        let install_started_at = Instant::now();
        let mut ready_done_pending = Vec::with_capacity(done_pending.len());
        let mut install_failed_cleanup = Vec::new();
        for mut pending in done_pending {
            let install_result = match (
                pending.start_item.prepared_target.as_ref(),
                pending.start_item.reused_committed_slot.as_ref(),
            ) {
                (Some(_), Some(_)) => Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "Get returned both a prepared destination and an existing owner slot: key={}",
                        pending.key
                    ),
                })),
                (prepared, reused) => {
                    let Some(target) = prepared.or(reused) else {
                        ready_done_pending.push(pending);
                        continue;
                    };
                    match u32::try_from(pending.start_item.len) {
                        Ok(_)
                            if prepared.is_some() && pending.target_write_capability.is_some() =>
                        {
                            self.install_hidden_leased_pending_local_get(
                                &pending.key,
                                pending.start_item.get_id,
                                pending.start_item.put_id,
                                pending
                                    .target_write_capability
                                    .as_ref()
                                    .expect("checked target capability"),
                            )
                            .map(Some)
                        }
                        Ok(len) if prepared.is_some() => self
                            .install_hidden_pending_local_get(
                                &pending.key,
                                pending.start_item.get_id,
                                pending.start_item.put_id,
                                target.addr,
                                target.base_addr,
                                len,
                                target.allocation_id,
                                target.segment_offset,
                                target.capacity_bytes,
                            )
                            .map(Some),
                        Ok(len) => self
                            .install_hidden_global_shared_get(
                                &pending.key,
                                pending.start_item.get_id,
                                pending.start_item.put_id,
                                target.addr,
                                target.base_addr,
                                len,
                                target.allocation_id,
                                target.segment_offset,
                                target.capacity_bytes,
                            )
                            .map(Some),
                        Err(_) => Err(KvError::Api(ApiError::InvalidArgument {
                            detail: format!(
                                "owner-slot Get value length exceeds u32: key={} len={}",
                                pending.key, pending.start_item.len
                            ),
                        })),
                    }
                }
            };
            match install_result {
                Ok(memory_info) => {
                    pending.owner_slot_memory_info = memory_info;
                    ready_done_pending.push(pending);
                }
                Err(err) => {
                    let mut target_terminal_installed = false;
                    let prepared_target =
                        if let Some(capability) = pending.target_write_capability.as_ref() {
                            match abort_unpublished_get_target(
                                self,
                                capability,
                                "hidden Get target installation failed",
                            ) {
                                Ok(()) => target_terminal_installed = true,
                                Err(abort_error) => {
                                    tracing::error!(
                                        key = pending.key,
                                        get_id = pending.start_item.get_id,
                                        error = %abort_error,
                                        "failed to abort an uninstalled GetToTarget destination"
                                    );
                                }
                            }
                            None
                        } else {
                            pending.start_item.prepared_target.clone()
                        };
                    if target_terminal_installed
                        && let Some((source_owner, terminal_sequence)) =
                            pending.get_to_target_terminal.take()
                    {
                        self.owner_transfer_peer_tracker
                            .record_terminal(&source_owner, terminal_sequence);
                    }
                    install_failed_cleanup.push(StartedGetRevokeCleanup {
                        get_id: pending.start_item.get_id,
                        prepared_target,
                    });
                    results[pending.idx] = Some(Err(err));
                }
            }
        }
        done_pending = ready_done_pending;

        finish_started_get_revoke_cleanup(
            self,
            install_failed_cleanup,
            "batch_get pending install failure",
        )
        .await;
        let lifecycle_install_us = install_started_at
            .elapsed()
            .as_micros()
            .min(i64::MAX as u128) as i64;

        let mut async_commit_pending = Vec::new();
        if route_commit_mode == OwnerRouteCommitMode::Async {
            let mut synchronous_pending = Vec::new();
            for mut pending in done_pending {
                let owner_target = pending.owner_slot_memory_info.is_some()
                    && (pending.target_write_capability.is_some()
                        || pending.start_item.reused_committed_slot.is_some());
                if !owner_target {
                    synchronous_pending.push(pending);
                    continue;
                }
                if let Some(capability) = pending.target_write_capability.as_ref() {
                    let started = self
                        .owner_segment_allocator
                        .lock()
                        .begin_data_ready_get_target_commit(capability);
                    if !matches!(
                        started,
                        OwnerSegmentTransferOutcome::TargetCommitPending { .. }
                            | OwnerSegmentTransferOutcome::TargetCommitted { .. }
                    ) {
                        tracing::warn!(
                            key = pending.key,
                            get_id = pending.start_item.get_id,
                            outcome = ?started,
                            "Get target could not install an Async CommitTarget guard; keeping the synchronous path"
                        );
                        synchronous_pending.push(pending);
                        continue;
                    }
                }
                let Some(materialization_guard) = self
                    .begin_async_get_key_materialization(&pending.key, pending.start_item.put_id)
                else {
                    tracing::warn!(
                        key = pending.key,
                        get_id = pending.start_item.get_id,
                        "Get target could not extend its key fence through Async CommitTarget; keeping the synchronous path"
                    );
                    synchronous_pending.push(pending);
                    continue;
                };
                pending.materialization_guard = Some(materialization_guard);

                let memory_info = pending
                    .owner_slot_memory_info
                    .as_ref()
                    .expect("Async owner Get requires its hidden MemoryInfo")
                    .clone();
                let data_len = pending.start_item.len as usize;
                metrics.record_l2_hit_locality(pending.peer_is_remote, data_len as u64);
                metrics.record_get_io_locality(
                    pending.peer_is_remote,
                    data_len as u64,
                    pending.transfer_us,
                );
                metrics.observe_cache_value_size(&client_id, node_role.as_str(), data_len as u64);
                let get_info = RemoteGetInfo {
                    get_id: pending.start_item.get_id,
                    data_len,
                    src_addr: pending.start_item.src_addr,
                    target_addr: pending.start_item.target_addr,
                    node_id: pending.start_item.node_id.clone().into(),
                    peer_is_src_or_target: pending.peer_is_remote,
                };
                results[pending.idx] = Some(Ok(Some((
                    Arc::new(UserMemHolder::new(
                        memory_info,
                        self.get_or_init_all_memholder_refcount(),
                        UserMemHolderExposeKind::SegPtr,
                    )),
                    Some(get_info),
                ))));
                async_commit_pending.push(pending);
            }
            done_pending = synchronous_pending;
        }

        let lifecycle_async_commit_items = async_commit_pending.len();
        if !async_commit_pending.is_empty() {
            let acknowledged_terminals = async_commit_pending
                .iter_mut()
                .filter_map(|pending| pending.get_to_target_terminal.take())
                .collect::<Vec<_>>();
            let spawn_view = self.view.clone_view();
            let worker_view = spawn_view.clone();
            spawn_view.spawn("async_owner_get_route_commit", async move {
                run_async_owner_get_route_commit(worker_view, async_commit_pending).await;
            });
            // The target is DataReady/RoutePending and the registered task now
            // owns its MemoryInfo. Source terminal replay is no longer needed.
            for (source_owner, terminal_sequence) in acknowledged_terminals {
                self.owner_transfer_peer_tracker
                    .record_terminal(&source_owner, terminal_sequence);
            }
        }

        let done_items = done_pending
            .iter()
            .map(|pending| BatchGetDoneItemReq {
                get_id: pending.start_item.get_id,
                late_target: pending.late_target.clone(),
            })
            .collect::<Vec<_>>();
        let done_get_ids = done_items
            .iter()
            .map(|item| item.get_id)
            .collect::<Vec<_>>();
        let lifecycle_done_items = done_get_ids.len();
        let done_first_get_id = done_get_ids.first().copied();
        let done_last_get_id = done_get_ids.last().copied();
        let mut done_attempt = 1u32;
        let done_started_at = Instant::now();
        let done_resp = loop {
            match self.batch_get_done_items(done_items.clone()).await {
                Ok(resp) if batch_get_done_response_matches(&done_get_ids, &resp) => break resp,
                Ok(resp) => {
                    tracing::warn!(
                        "batch_get_done response shape/identity mismatch; retaining pending-visible slots and retrying the same idempotent get_ids: items={} first_get_id={:?} last_get_id={:?} got_items={} got_first_get_id={:?} got_last_get_id={:?} attempt={}",
                        done_get_ids.len(),
                        done_first_get_id,
                        done_last_get_id,
                        resp.items.len(),
                        resp.items.first().map(|item| item.get_id),
                        resp.items.last().map(|item| item.get_id),
                        done_attempt
                    );
                }
                Err(err) => {
                    if matches!(&err, KvError::Api(ApiError::SystemShutdown { .. })) {
                        // Transfer has completed but the requester target
                        // terminal is unknown. Do not advance the piggyback ACK
                        // watermark; replay must remain available for
                        // generation reconciliation.
                        return Err(err);
                    }
                    tracing::warn!(
                        "batch_get_done transport uncertain; retaining pending-visible slots and retrying the same idempotent get_ids: items={} first_get_id={:?} last_get_id={:?} attempt={} err={}",
                        done_get_ids.len(),
                        done_first_get_id,
                        done_last_get_id,
                        done_attempt,
                        err
                    );
                }
            }
            tokio::time::sleep(Duration::from_millis(
                (10u64.saturating_mul(1u64 << done_attempt.min(8))).min(2_000),
            ))
            .await;
            done_attempt = done_attempt.saturating_add(1);
        };
        let lifecycle_done_us = done_started_at.elapsed().as_micros().min(i64::MAX as u128) as i64;
        let publish_started_at = Instant::now();
        let master_node_id: NodeID = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?
            .into();
        let mut local_hot_admissions = Vec::new();
        let mut late_bind_error_cleanup = Vec::new();

        for (mut pending, done_item) in done_pending.into_iter().zip(done_resp.items.into_iter()) {
            let done_result = crate::rpcresp_kvresult_convert::try_from_code(
                done_item.error_code,
                done_item.error_json.clone(),
            );
            if let Err(err) = done_result {
                if pending.late_target.is_some() {
                    // A definite late-Bind failure leaves the scalar Plan at
                    // master. Drive Revoke to a terminal. The local target has already moved into
                    // hidden owner state, so its physical cleanup stays below
                    // and must not be duplicated by the Prepared-slot path.
                    late_bind_error_cleanup.push(StartedGetRevokeCleanup {
                        get_id: pending.start_item.get_id,
                        prepared_target: None,
                    });
                }
                if pending.start_item.prepared_target.is_some()
                    || pending.start_item.reused_committed_slot.is_some()
                {
                    let target_aborted = self
                        .abort_hidden_pending_local_get(&pending.key, pending.start_item.get_id);
                    let canonical = self
                        .local_committed_mem_holder_for_put_id(
                            &pending.key,
                            pending.start_item.put_id,
                        )
                        .await;
                    if (target_aborted || canonical.is_some())
                        && let Some((source_owner, terminal_sequence)) =
                            pending.get_to_target_terminal.take()
                    {
                        self.owner_transfer_peer_tracker
                            .record_terminal(&source_owner, terminal_sequence);
                    }
                    // Installation moved this slot out of Prepared and into the
                    // resident MemoryInfo lifecycle. Removing the hidden index and
                    // dropping its final local Arc performs the one valid release.
                    // Calling release_prepared_get_target here would release the same
                    // slot through the obsolete Prepared-state path.
                    drop(pending.owner_slot_memory_info.take());
                    // A same-version PutDone/GetDone may have won while this
                    // transfer was in flight.  Converge on the owner's
                    // canonical local backing instead of turning an already
                    // available KV page into a prefix miss.
                    if let Some(memory_info) = canonical {
                        let user_mem_holder = Arc::new(UserMemHolder::new(
                            memory_info,
                            self.get_or_init_all_memholder_refcount(),
                            UserMemHolderExposeKind::SegPtr,
                        ));
                        results[pending.idx] = Some(Ok(Some((user_mem_holder, None))));
                        continue;
                    }
                }
                results[pending.idx] = Some(Err(err));
                continue;
            }
            let expose_kind = if done_item.allocation_mode == GetAllocationMode::Temporary {
                UserMemHolderExposeKind::OwnedCopy
            } else {
                UserMemHolderExposeKind::SegPtr
            };
            let data_len = pending.start_item.len as usize;
            metrics.record_l2_hit_locality(pending.peer_is_remote, data_len as u64);
            metrics.record_get_io_locality(
                pending.peer_is_remote,
                data_len as u64,
                pending.transfer_us,
            );
            let owner_slot_mode = if pending.start_item.prepared_target.is_some() {
                Some(GetAllocationMode::LocalCommittedSlot)
            } else if pending.start_item.reused_committed_slot.is_some() {
                Some(GetAllocationMode::RequesterLocalPromote)
            } else {
                None
            };
            let memory_info = if let Some(expected_mode) = owner_slot_mode {
                if done_item.allocation_mode != expected_mode {
                    let target_aborted = self
                        .abort_hidden_pending_local_get(&pending.key, pending.start_item.get_id);
                    if target_aborted
                        && let Some((source_owner, terminal_sequence)) =
                            pending.get_to_target_terminal.take()
                    {
                        self.owner_transfer_peer_tracker
                            .record_terminal(&source_owner, terminal_sequence);
                    }
                    results[pending.idx] = Some(Err(KvError::Api(ApiError::Unknown {
                        detail: format!(
                            "owner-slot Get completed with unexpected allocation mode: key={} expected={:?} got={:?}",
                            pending.key, expected_mode, done_item.allocation_mode
                        ),
                    })));
                    continue;
                }
                let memory_info = match self.promote_hidden_owner_slot_get(
                    &pending.key,
                    pending.start_item.get_id,
                    pending.start_item.put_id,
                ) {
                    Ok(memory_info) => memory_info,
                    Err(err) => {
                        results[pending.idx] = Some(Err(err));
                        continue;
                    }
                };
                if let Some(prepared) = pending.owner_slot_memory_info.as_ref() {
                    assert!(
                        Arc::ptr_eq(prepared, &memory_info),
                        "Get promotion must retain the unique owner-slot MemoryInfo"
                    );
                }
                local_hot_admissions.push((
                    pending.key.clone(),
                    pending.start_item.put_id,
                    memory_info.clone(),
                    pending.start_item.atomic_group.clone(),
                ));
                if let Some((source_owner, terminal_sequence)) =
                    pending.get_to_target_terminal.take()
                {
                    self.owner_transfer_peer_tracker
                        .record_terminal(&source_owner, terminal_sequence);
                }
                memory_info
            } else {
                if matches!(
                    done_item.allocation_mode,
                    GetAllocationMode::LocalCommittedSlot
                        | GetAllocationMode::RequesterLocalPromote
                ) {
                    results[pending.idx] = Some(Err(KvError::Api(ApiError::Unknown {
                        detail: format!(
                            "master returned local committed-slot mode without a prepared target: key={}",
                            pending.key
                        ),
                    })));
                    continue;
                }
                let offset = pending.start_item.target_addr - pending.start_item.target_base_addr;
                let memory_info = Arc::new(
                    MemoryInfo::new(
                        offset,
                        pending.start_item.len as u32,
                        done_item.holder_id,
                        pending.key.clone(),
                        master_node_id.clone(),
                        self.view.clone(),
                    )
                    .await,
                );
                if allocation_mode_installs_owner_index(done_item.allocation_mode)
                    && self.install_get_cached_info_if_unfenced(
                        &pending.key,
                        pending.start_item.put_id,
                        memory_info.clone(),
                    )
                {
                    metrics.observe_cache_value_size(
                        &client_id,
                        node_role.as_str(),
                        data_len as u64,
                    );
                }
                memory_info
            };
            let get_info = RemoteGetInfo {
                get_id: pending.start_item.get_id,
                data_len,
                src_addr: pending.start_item.src_addr,
                target_addr: pending.start_item.target_addr,
                node_id: pending.start_item.node_id.clone().into(),
                peer_is_src_or_target: pending.peer_is_remote,
            };
            if matches!(
                done_item.allocation_mode,
                GetAllocationMode::LocalCommittedSlot | GetAllocationMode::RequesterLocalPromote
            ) {
                metrics.observe_cache_value_size(&client_id, node_role.as_str(), data_len as u64);
            }
            let user_mem_holder = Arc::new(UserMemHolder::new(
                memory_info,
                self.get_or_init_all_memholder_refcount(),
                expose_kind,
            ));
            results[pending.idx] = Some(Ok(Some((user_mem_holder, Some(get_info)))));
        }

        finish_started_get_revoke_cleanup(
            self,
            late_bind_error_cleanup,
            "batch_get late Bind terminal failure",
        )
        .await;

        // Publish every local index before Moka admission starts selecting
        // individual capacity victims.
        let lifecycle_local_hot_admissions = local_hot_admissions.len();
        for (key, put_id, memory_info, _atomic_group) in local_hot_admissions {
            self.owner_hot_track_committed(&key, put_id, &memory_info);
        }

        let output = results
            .into_iter()
            .map(|item| {
                item.unwrap_or_else(|| {
                    Err(KvError::Api(ApiError::Unknown {
                        detail: "batch_get_finish_started result slot was not populated"
                            .to_string(),
                    }))
                })
            })
            .collect::<Vec<_>>();
        let lifecycle_hits = output
            .iter()
            .filter(|item| matches!(item, Ok(Some(_))))
            .count();
        let lifecycle_misses = output
            .iter()
            .filter(|item| matches!(item, Ok(None)))
            .count();
        let lifecycle_errors = output.iter().filter(|item| matches!(item, Err(_))).count();
        let lifecycle_publish_us = publish_started_at
            .elapsed()
            .as_micros()
            .min(i64::MAX as u128) as i64;
        let lifecycle_total_us = lifecycle_started_at
            .elapsed()
            .as_micros()
            .min(i64::MAX as u128) as i64;
        tracing::info!(
            "external Get finish lifecycle: requested={} transfer_concurrency={} zero_copy_items={} transfer_items={} remote_transfer_items={} local_ssd_items={} remote_ssd_items={} transfer_source_nodes={} ssd_source_nodes={} transfer_bytes={} remote_transfer_bytes={} plan_us={} transfer_wall_us={} transfer_sum_us={} transfer_max_us={} transfer_cleanup_us={} install_us={} async_commit_items={} sync_done_items={} done_attempts={} sync_done_us={} local_hot_admissions={} publish_us={} hits={} misses={} errors={} total_us={}",
            lifecycle_requested_keys,
            transfer_concurrency,
            lifecycle_zero_copy_items,
            lifecycle_transfer_items,
            lifecycle_remote_transfer_items,
            lifecycle_local_ssd_items,
            lifecycle_remote_ssd_items,
            lifecycle_transfer_source_nodes.len(),
            lifecycle_ssd_source_nodes.len(),
            lifecycle_transfer_bytes,
            lifecycle_remote_transfer_bytes,
            lifecycle_plan_us,
            lifecycle_transfer_wall_us,
            lifecycle_transfer_sum_us,
            lifecycle_transfer_max_us,
            lifecycle_transfer_cleanup_us,
            lifecycle_install_us,
            lifecycle_async_commit_items,
            lifecycle_done_items,
            done_attempt,
            lifecycle_done_us,
            lifecycle_local_hot_admissions,
            lifecycle_publish_us,
            lifecycle_hits,
            lifecycle_misses,
            lifecycle_errors,
            lifecycle_total_us,
        );
        Ok(output)
    }

    pub async fn batch_get(
        &self,
        keys: Vec<String>,
        transfer_concurrency: usize,
    ) -> KvResult<Vec<KvResult<Option<(Arc<UserMemHolder>, Option<RemoteGetInfo>)>>>> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_get".to_string(),
            }));
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let metrics = self.metrics_handle();
        let client_id = self.client_id_str();
        let node_role = self.node_role();
        let mut results: Vec<
            Option<KvResult<Option<(Arc<UserMemHolder>, Option<RemoteGetInfo>)>>>,
        > = (0..keys.len()).map(|_| None).collect();
        let mut missing_indices = Vec::new();
        let mut missing_keys = Vec::new();

        for (idx, key) in keys.iter().enumerate() {
            if let Some(memory_info) = self.local_visible_mem_holder_waiting(key).await {
                let user_mem_holder = Arc::new(UserMemHolder::new(
                    memory_info.clone(),
                    self.get_or_init_all_memholder_refcount(),
                    UserMemHolderExposeKind::SegPtr,
                ));
                obe_get_cache_hit(
                    &metrics,
                    &client_id,
                    &node_role,
                    key,
                    memory_info.len as u64,
                );
                metrics.record_get_io_locality(false, memory_info.len as u64, 0);
                results[idx] = Some(Ok(Some((user_mem_holder, None))));
            } else {
                obe_get_cache_miss(&metrics, &client_id, &node_role, key);
                missing_indices.push(idx);
                missing_keys.push(key.clone());
            }
        }

        if !missing_keys.is_empty() {
            let start_resp = super::external_api::batch_get_start_with_local_reserve_targets(
                self,
                &missing_keys,
            )
            .await?;
            if start_resp.items.len() != missing_keys.len() {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "batch_get_start response length mismatch: expected={} got={}",
                        missing_keys.len(),
                        start_resp.items.len(),
                    ),
                }));
            }
            let late_targets = vec![None; start_resp.items.len()];
            let finished = self
                .batch_get_finish_started(
                    missing_keys,
                    start_resp.items,
                    late_targets,
                    transfer_concurrency,
                )
                .await?;
            if finished.len() != missing_indices.len() {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "batch_get_finish_started result length mismatch: expected={} got={}",
                        missing_indices.len(),
                        finished.len(),
                    ),
                }));
            }
            for (idx, result) in missing_indices.into_iter().zip(finished) {
                results[idx] = Some(result);
            }
        }

        Ok(results
            .into_iter()
            .map(|item| {
                item.unwrap_or_else(|| {
                    Err(KvError::Api(ApiError::Unknown {
                        detail: "batch_get result slot was not populated".to_string(),
                    }))
                })
            })
            .collect())
    }
    pub async fn batch_is_exist(
        &self,
        keys: Vec<String>,
        allow_local_snapshot: bool,
    ) -> KvResult<Vec<bool>> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_is_exist".to_string(),
            }));
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = vec![false; keys.len()];
        let mut missing_indices = Vec::new();
        let mut missing_keys = Vec::new();
        for (idx, key) in keys.iter().enumerate() {
            if allow_local_snapshot && self.has_local_snapshot(key) {
                results[idx] = true;
                continue;
            }
            missing_indices.push(idx);
            missing_keys.push(key.clone());
        }
        if missing_keys.is_empty() {
            return Ok(results);
        }

        let req = MsgPack {
            serialize_part: BatchIsExistReq { keys: missing_keys },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = call_control_plane_rpc(
            &self.rpc_caller_batch_is_exist,
            self.view.p2p_module(),
            master_node_id.into(),
            req,
            None,
            0,
        )
        .await
        .map_err(KvError::from)?;
        let resp_part = resp.serialize_part;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp_part.error_code,
            resp_part.error_json.clone(),
        )?;
        if resp_part.exists_list.len() != missing_indices.len() {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "batch_is_exist response length mismatch: expected={} got={}",
                    missing_indices.len(),
                    resp_part.exists_list.len()
                ),
            }));
        }
        for (idx, exists) in missing_indices
            .into_iter()
            .zip(resp_part.exists_list.into_iter())
        {
            results[idx] = exists;
        }
        Ok(results)
    }

    pub async fn is_exist_with_local_snapshot(
        &self,
        key: &str,
        allow_local_snapshot: bool,
    ) -> KvResult<bool> {
        let mut results = self
            .batch_is_exist(vec![key.to_string()], allow_local_snapshot)
            .await?;
        Ok(results.pop().unwrap_or(false))
    }

    /// becaused we cached local kv metadata, so we make `MemHolder` with Arc here
    pub async fn get(
        &self,
        key: &str,
    ) -> KvResult<Option<(Arc<UserMemHolder>, Option<RemoteGetInfo>)>> {
        let mut results = self.batch_get(vec![key.to_string()], 1).await?;
        results.pop().unwrap_or_else(|| {
            Err(KvError::Api(ApiError::Unknown {
                detail: "single-key Get returned no result".to_string(),
            }))
        })
    }
    pub async fn is_exist(&self, key: &str) -> KvResult<bool> {
        self.is_exist_with_local_snapshot(key, false).await
    }

    /// Get metadata for a key without transferring data
    pub async fn get_meta(&self, key: &str) -> KvResult<GetMetaResp> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting get_meta".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: GetMetaReq {
                key: key.to_string(),
            },
            raw_bytes: Vec::new(),
        };

        // 获取 master 节点 ID
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;

        // 调用 RPC
        let resp = call_control_plane_rpc(
            &self.rpc_caller_get_meta,
            self.view.p2p_module(),
            master_node_id.into(),
            req,
            None,
            0,
        )
        .await
        .map_err(KvError::from)?;

        Ok(resp.serialize_part)
    }

    /// 开始 Get 操作，获取数据位置和信息
    pub async fn get_start(&self, key: &str) -> KvResult<GetStartResp> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting get_start".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: GetStartReq {
                key: key.to_string(),
                prepared_target: None,
                external_sink_target: None,
            },
            raw_bytes: Vec::new(),
        };

        // 获取 master 节点 ID
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;

        // 调用 RPC
        let resp = call_control_plane_rpc(
            &self.rpc_caller_get_start,
            self.view.p2p_module(),
            master_node_id.into(),
            req,
            None,
            0,
        )
        .await
        .map_err(KvError::from)?;

        Ok(resp.serialize_part)
    }

    pub async fn batch_get_start(&self, keys: Vec<String>) -> KvResult<BatchGetStartResp> {
        self.batch_get_start_with_prepared_targets(keys, Vec::new())
            .await
    }

    pub(crate) async fn batch_get_start_with_prepared_targets(
        &self,
        keys: Vec<String>,
        prepared_targets: Vec<Option<GetPreparedLocalReserveTarget>>,
    ) -> KvResult<BatchGetStartResp> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_get_start".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: BatchGetStartReq {
                keys,
                prepared_targets,
                external_sink_targets: Vec::new(),
            },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = call_control_plane_rpc(
            &self.rpc_caller_batch_get_start,
            self.view.p2p_module(),
            master_node_id.into(),
            req,
            None,
            0,
        )
        .await
        .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
        Ok(resp.serialize_part)
    }

    pub(crate) async fn batch_get_bind_targets(
        &self,
        items: Vec<BatchGetBindItemReq>,
    ) -> KvResult<BatchGetBindResp> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_get_bind".to_string(),
            }));
        }
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = call_control_plane_rpc(
            &self.rpc_caller_batch_get_bind,
            self.view.p2p_module(),
            master_node_id.into(),
            MsgPack {
                serialize_part: BatchGetBindReq { items },
                raw_bytes: Vec::new(),
            },
            None,
            0,
        )
        .await
        .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
        Ok(resp.serialize_part)
    }

    /// 撤销 Get 操作，释放已分配的资源
    pub async fn get_revoke(&self, get_id: u64) -> KvResult<()> {
        let req = MsgPack {
            serialize_part: GetRevokeReq { get_id },
            raw_bytes: Vec::new(),
        };

        // 获取 master 节点 ID
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;

        // 调用 RPC
        let _resp = call_control_plane_rpc(
            &self.rpc_caller_get_revoke,
            self.view.p2p_module(),
            master_node_id.into(),
            req,
            None,
            0,
        )
        .await
        .map_err(KvError::from)?;

        Ok(())
    }

    pub async fn batch_get_revoke(&self, get_ids: Vec<u64>) -> KvResult<BatchGetRevokeResp> {
        if get_ids.is_empty() {
            return Ok(BatchGetRevokeResp {
                items: Vec::new(),
                error_code: OK,
                error_json: String::new(),
            });
        }
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_get_revoke".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: BatchGetRevokeReq { get_ids },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = call_control_plane_rpc(
            &self.rpc_caller_batch_get_revoke,
            self.view.p2p_module(),
            master_node_id.into(),
            req,
            None,
            0,
        )
        .await
        .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
        Ok(resp.serialize_part)
    }

    /// 完成 Get 操作，清理资源
    pub async fn get_done(&self, get_id: u64) -> KvResult<GetDoneResp> {
        let req = MsgPack {
            serialize_part: GetDoneReq { get_id },
            raw_bytes: Vec::new(),
        };

        // 获取 master 节点 ID
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;

        // 调用 RPC
        let resp = call_control_plane_rpc(
            &self.rpc_caller_get_done,
            self.view.p2p_module(),
            master_node_id.into(),
            req,
            None,
            0,
        )
        .await
        .map_err(KvError::from)?;

        Ok(resp.serialize_part)
    }

    pub async fn batch_get_done(&self, get_ids: Vec<u64>) -> KvResult<BatchGetDoneResp> {
        self.batch_get_done_items(
            get_ids
                .into_iter()
                .map(|get_id| BatchGetDoneItemReq {
                    get_id,
                    late_target: None,
                })
                .collect(),
        )
        .await
    }

    pub async fn batch_get_done_items(
        &self,
        items: Vec<BatchGetDoneItemReq>,
    ) -> KvResult<BatchGetDoneResp> {
        if items.is_empty() {
            return Ok(BatchGetDoneResp {
                items: Vec::new(),
                error_code: OK,
                error_json: String::new(),
                server_process_us: 0,
            });
        }
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_get_done_items".to_string(),
            }));
        }
        // The master performs synchronous Moka policy work before acknowledging
        // committed-slot Done.  Bound the number of callers entering that path
        // so a capacity scan cannot park every master Tokio worker on Moka's
        // blocking housekeeper lock.  The permit covers one RPC attempt only;
        // idempotent retry backoff releases it and lets other atomic_batches converge.
        let _done_rpc_permit = batch_get_done_rpc_limiter()
            .acquire()
            .await
            .expect("the process-wide BatchGetDone limiter is never closed");
        let req = MsgPack {
            serialize_part: BatchGetDoneReq { items },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = call_control_plane_rpc(
            &self.rpc_caller_batch_get_done,
            self.view.p2p_module(),
            master_node_id.into(),
            req,
            None,
            2,
        )
        .await
        .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
        Ok(resp.serialize_part)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        allocation_mode_installs_owner_index, batch_get_done_response_matches,
        late_target_matches_start, planned_get_source_terminal_error,
        prepare_claimed_get_target_error,
    };
    use crate::master_kv_router::msg_pack::{
        BatchGetDoneItemResp, BatchGetDoneResp, BatchGetStartItemResp, GetAllocationMode,
        GetBindTarget, GetSourceKind,
    };
    use crate::owner_segment::{OwnerGeneration, OwnerSlotDesc, OwnerTransferErrorCode};
    use crate::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError};

    fn response(get_ids: &[u64]) -> BatchGetDoneResp {
        BatchGetDoneResp {
            items: get_ids
                .iter()
                .map(|get_id| BatchGetDoneItemResp {
                    get_id: *get_id,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn batch_get_done_requires_exact_response_identity() {
        assert!(batch_get_done_response_matches(
            &[11, 22],
            &response(&[11, 22])
        ));
        assert!(!batch_get_done_response_matches(
            &[11, 22],
            &response(&[22, 11])
        ));
        assert!(!batch_get_done_response_matches(
            &[11, 22],
            &response(&[11])
        ));
    }

    #[test]
    fn source_disappearing_after_plan_becomes_a_structured_stale_plan() {
        let stale = planned_get_source_terminal_error(
            "planned-key",
            41,
            Some(OwnerTransferErrorCode::NotFound),
            KvError::Api(ApiError::Unknown {
                detail: "source manifest disappeared".to_string(),
            }),
        );
        assert!(matches!(
            stale,
            KvError::Api(ApiError::StaleGetPlan {
                get_id: 41,
                ref key,
                ..
            }) if key == "planned-key"
        ));

        let protocol_conflict = planned_get_source_terminal_error(
            "planned-key",
            41,
            Some(OwnerTransferErrorCode::Conflict),
            KvError::Api(ApiError::Unknown {
                detail: "replay changed the source token".to_string(),
            }),
        );
        assert!(matches!(
            protocol_conflict,
            KvError::Api(ApiError::Unknown { .. })
        ));
    }

    #[test]
    fn target_materializing_after_plan_becomes_a_structured_stale_plan() {
        let stale = prepare_claimed_get_target_error(
            "planned-key",
            42,
            crate::owner_segment::OwnerTransferItemError::new(
                OwnerTransferErrorCode::StalePlan,
                "exact generation materialized locally",
            ),
        );
        assert!(matches!(
            stale,
            KvError::Api(ApiError::StaleGetPlan {
                get_id: 42,
                ref key,
                ..
            }) if key == "planned-key"
        ));

        let protocol_conflict = prepare_claimed_get_target_error(
            "planned-key",
            42,
            crate::owner_segment::OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "allocation identity is already bound",
            ),
        );
        assert!(matches!(
            protocol_conflict,
            KvError::Api(ApiError::Unknown { .. })
        ));
    }

    #[test]
    fn requester_local_borrow_never_enters_the_owner_index() {
        assert!(!allocation_mode_installs_owner_index(
            GetAllocationMode::RequesterLocalBorrow
        ));
        assert!(!allocation_mode_installs_owner_index(
            GetAllocationMode::Temporary
        ));
        assert!(allocation_mode_installs_owner_index(
            GetAllocationMode::ReuseReplica
        ));
        assert!(allocation_mode_installs_owner_index(
            GetAllocationMode::LocalCommittedSlot
        ));
        assert!(allocation_mode_installs_owner_index(
            GetAllocationMode::RequesterLocalPromote
        ));
    }

    #[test]
    fn late_owner_target_must_match_the_transferred_descriptor_exactly() {
        let prepared = OwnerSlotDesc {
            owner: OwnerGeneration::new("requester", 7),
            allocation_id: 11,
            segment_offset: 0x2000,
            capacity_bytes: 8192,
            addr: 0x5000,
            base_addr: 0x3000,
            len: 4096,
            segment_registration_epoch: 2,
        };
        let prepared_start = BatchGetStartItemResp {
            target_addr: prepared.addr,
            target_base_addr: prepared.base_addr,
            len: prepared.len,
            prepared_target: Some(prepared.clone()),
            ..Default::default()
        };
        assert!(late_target_matches_start(
            &prepared_start,
            &GetBindTarget::PreparedLocalReserve(prepared.clone())
        ));
        let mut wrong_prepared = prepared.clone();
        wrong_prepared.allocation_id += 1;
        assert!(!late_target_matches_start(
            &prepared_start,
            &GetBindTarget::PreparedLocalReserve(wrong_prepared)
        ));

        let source = OwnerSlotDesc {
            owner: OwnerGeneration::new("requester", 7),
            allocation_id: 13,
            segment_offset: 0x4000,
            capacity_bytes: 8192,
            addr: 0x7000,
            base_addr: 0x3000,
            len: 4096,
            segment_registration_epoch: 2,
        };
        let reused_start = BatchGetStartItemResp {
            src_addr: source.addr,
            src_base_addr: source.base_addr,
            target_addr: source.addr,
            target_base_addr: source.base_addr,
            len: source.len,
            source_kind: GetSourceKind::Memory,
            source_route_token: Some(Default::default()),
            reused_committed_slot: Some(source.clone()),
            ..Default::default()
        };
        assert!(late_target_matches_start(
            &reused_start,
            &GetBindTarget::RequesterLocalSource
        ));
        let mut wrong_reused = reused_start;
        wrong_reused.target_addr += 1;
        assert!(!late_target_matches_start(
            &wrong_reused,
            &GetBindTarget::RequesterLocalSource
        ));
    }
}
