use crate::client_kv_api::ClientKvApi;
use crate::client_kv_api::get::{StartedGetRevokeCleanup, finish_started_get_revoke_cleanup};
use crate::client_kv_api::msg_pack::{
    ExternalBatchGetCancelPlan, ExternalBatchGetCancelReq, ExternalBatchGetCancelResp,
    ExternalBatchGetItemResp, ExternalBatchGetLocalProbeReq, ExternalBatchGetLocalProbeResp,
    ExternalBatchGetReq, ExternalBatchGetResp, ExternalBatchGetStartReq, ExternalBatchGetStartResp,
    ExternalBatchGetStartTransferPlan, ExternalBatchGetTransferReq, ExternalBatchGetTransferResp,
    ExternalBatchIsExistReq, ExternalBatchIsExistResp, ExternalBatchPutCommitItemResp,
    ExternalBatchPutCommitReq, ExternalBatchPutCommitResp, ExternalBatchPutStartItemResp,
    ExternalBatchPutStartReq, ExternalBatchPutStartResp, ExternalBatchPutTransferEndItemResp,
    ExternalBatchPutTransferEndReq, ExternalBatchPutTransferEndResp, ExternalDeleteReq,
    ExternalDeleteResp, ExternalExecutePlannedGetReq, ExternalExecutePlannedGetResp,
    ExternalGetReq, ExternalGetResp, ExternalIsExistReq, ExternalIsExistResp,
    ExternalObservabilitySnapshotReq, ExternalObservabilitySnapshotResp, ExternalPutCommitReq,
    ExternalPutCommitResp, ExternalPutRevokeReq, ExternalPutRevokeResp, ExternalPutStartReq,
    ExternalPutStartResp, ExternalPutTransferEndReq, ExternalPutTransferEndResp, TestPutPhaseTrace,
};
use crate::client_kv_api::{
    self, ExternalGetKeyInterest, ExternalGetKeySharedOp, ExternalGetKeySharedPhase,
    ExternalGetStartEntry, ExternalGetStartOwnerItem, ExternalGetStartPrefixResult,
    ExternalGetStartSharedItemResult, ExternalGetStartTransferOutput,
    ExternalLocalFirstPutKeyReservation, ExternalPendingPutCtx, ExternalPutKeyOutcome,
    OwnerLocalPublishItem, OwnerLocalPublishJob, OwnerLocalReserveSlotLease,
    OwnerLocalReserveSlotRef,
};
use crate::client_seg_pool::{ResolveSideTransferLaneReq, parse_side_transfer_worker_lane_idx};
use crate::cluster_manager::NodeIDString;
use crate::cluster_manager::{
    META_KEY_SHARED_STORAGE_NODE_ID, META_KEY_SHARED_STORAGE_NODE_START_TIME,
};
use crate::master_kv_router::msg_pack::{
    BatchGetStartItemResp, BatchGetStartResp, BatchPutDoneItemReq, BatchPutRevokeItemReq,
    GetPreparedLocalReserveTarget, PutAtomicGroup, PutDoneCommittedSlot,
    build_put_atomic_group_assignments,
};
use crate::memholder::MemholderManagerTrait;
use crate::memholder::NodeHolderKey;
use crate::memholder::{UserMemHolder, UserMemHolderExposeKind};
use crate::p2p::msg_pack::MsgPack;
use crate::rpcresp_kvresult_convert::FromError;
use crate::rpcresp_kvresult_convert::ToResult;
use crate::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult, OK, codes_api};
use async_trait::async_trait;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use tracing;

fn duration_to_i64_us(duration: std::time::Duration) -> i64 {
    duration.as_micros().min(i64::MAX as u128) as i64
}

const SIDE_TRANSFER_OWNER_RPC_TIMEOUT_SECS: u64 = 30;
const SIDE_TRANSFER_TARGET_RESOLVE_TIMEOUT_SECS: u64 = 10;
// A handle can legitimately wait behind one full SGLang request-timeout
// window before layerwise restore consumes it.  Keep a bounded crash cleanup
// lease, but do not expire a live plan earlier than the supported 300-second
// request window used by the aligned agent workload.
const EXTERNAL_GET_START_HANDLE_TTL: Duration = Duration::from_secs(360);
const EXTERNAL_GET_START_HANDLE_SWEEP_INTERVAL: Duration = Duration::from_secs(5);
// A BatchGetStart transport error has an unknown commit point: the master may
// already hold the prepared target even though the owner never received its
// get_id.  Keep such slots out of reuse until the master's 60-second inflight
// Get TTL has elapsed, with a small scheduling margin.
const PREPARED_GET_START_UNCERTAIN_QUARANTINE: Duration = Duration::from_secs(65);
// PutStart has the same unknown-commit-point problem as GetStart.  Keep the
// owner reclaim fence alive beyond the master's 60-second inflight Put TTL if
// the caller future is cancelled or the RPC returns a transport error.
const EXTERNAL_PUT_START_UNCERTAIN_QUARANTINE: Duration = Duration::from_secs(65);
// Keep control-plane batching, but bound the failure domain of one uncertain
// BatchGetDone. Atomic groups are never split merely to satisfy this soft cap.
const EXTERNAL_GET_FINISH_TARGET_KEYS_PER_BATCH: usize = 128;
const EXTERNAL_GET_FINISH_BATCH_CONCURRENCY: usize = 4;

/// Resolve the generation carried by a MemberLeft event without guessing.
/// ClusterEvent::MemberLeft contains only a node id, so a currently live
/// generation makes the event ambiguous (normally a delayed leave after a
/// reconnect).  Missing previous-generation metadata is ambiguous as well.
pub(crate) fn external_member_left_departed_epoch(
    previous_epoch: Option<i64>,
    current_epoch: Option<i64>,
) -> Option<i64> {
    current_epoch.is_none().then_some(previous_epoch).flatten()
}

/// MemberLeft is a cold path.  It may scan the handle registry, but removal is
/// conditional on both requester identity and membership generation so a
/// collected handle id can never delete a newer generation's entry.
pub(crate) fn cleanup_external_get_start_handles_for_generation(
    registry: &dashmap::DashMap<u64, ExternalGetStartEntry>,
    req_node_id: &str,
    requester_node_start_time: i64,
) -> usize {
    let handles = registry
        .iter()
        .filter_map(|entry| {
            let value = entry.value();
            (value.req_node_id == req_node_id
                && value.requester_node_start_time == Some(requester_node_start_time))
            .then_some(*entry.key())
        })
        .collect::<Vec<_>>();

    handles
        .into_iter()
        .filter(|handle| {
            registry
                .remove_if(handle, |_, value| {
                    value.req_node_id == req_node_id
                        && value.requester_node_start_time == Some(requester_node_start_time)
                })
                .is_some()
        })
        .count()
}

struct ExternalPutStartFenceClaim {
    view: Option<crate::client_kv_api::ClientKvApiView>,
    fence: Option<Arc<client_kv_api::ExternalPendingPutFenceGuard>>,
    uncertain_delay: Duration,
    #[cfg(test)]
    uncertain_test_sink:
        Option<Arc<parking_lot::Mutex<Option<Arc<client_kv_api::ExternalPendingPutFenceGuard>>>>>,
}

impl ExternalPutStartFenceClaim {
    fn new(
        view: crate::client_kv_api::ClientKvApiView,
        fence: Arc<client_kv_api::ExternalPendingPutFenceGuard>,
    ) -> Self {
        Self {
            view: Some(view),
            fence: Some(fence),
            uncertain_delay: EXTERNAL_PUT_START_UNCERTAIN_QUARANTINE,
            #[cfg(test)]
            uncertain_test_sink: None,
        }
    }

    #[cfg(test)]
    fn new_for_cancellation_test(
        fence: Arc<client_kv_api::ExternalPendingPutFenceGuard>,
        sink: Arc<parking_lot::Mutex<Option<Arc<client_kv_api::ExternalPendingPutFenceGuard>>>>,
    ) -> Self {
        Self {
            view: None,
            fence: Some(fence),
            uncertain_delay: Duration::ZERO,
            uncertain_test_sink: Some(sink),
        }
    }

    fn take_for_pending_context(&mut self) -> Arc<client_kv_api::ExternalPendingPutFenceGuard> {
        self.fence
            .take()
            .expect("external PutStart fence claim is armed")
    }

    fn release_after_definite_response(&mut self) {
        drop(self.fence.take());
    }
}

impl Drop for ExternalPutStartFenceClaim {
    fn drop(&mut self) {
        let Some(fence) = self.fence.take() else {
            return;
        };
        #[cfg(test)]
        if let Some(sink) = self.uncertain_test_sink.take() {
            let replaced = sink.lock().replace(fence);
            assert!(replaced.is_none(), "cancellation test sink must be empty");
            return;
        }
        let delay = self.uncertain_delay;
        let view = self
            .view
            .take()
            .expect("production uncertain PutStart fence requires owner view");
        let spawn_view = view.clone();
        let worker_view = view;
        spawn_view.spawn("external_put_start_uncertain_fence", async move {
            let mut shutdown_waiter = worker_view.register_shutdown_waiter();
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = shutdown_waiter.wait() => return,
            }
            drop(fence);
        });
    }
}

pub(crate) fn spawn_external_get_start_handle_sweeper(view: crate::client_kv_api::ClientKvApiView) {
    let spawn_view = view.clone();
    let worker_view = view;
    spawn_view.spawn("external_get_start_handle_sweeper", async move {
        let mut shutdown_waiter = worker_view.register_shutdown_waiter();
        let mut interval = tokio::time::interval(EXTERNAL_GET_START_HANDLE_SWEEP_INTERVAL);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let expired = worker_view
                        .client_kv_api()
                        .inner()
                        .external_get_start_registry
                        .iter()
                        .filter_map(|entry| {
                            (entry.value().created_at.elapsed() >= EXTERNAL_GET_START_HANDLE_TTL)
                                .then_some(*entry.key())
                        })
                        .collect::<Vec<_>>();
                    if !expired.is_empty() {
                        let inner = worker_view.client_kv_api().inner();
                        let mut removed = 0usize;
                        for handle in expired {
                            removed += usize::from(inner.external_get_start_registry.remove(&handle).is_some());
                        }
                        tracing::warn!(
                            "expired abandoned external Get handles: removed={} ttl_secs={}",
                            removed,
                            EXTERNAL_GET_START_HANDLE_TTL.as_secs()
                        );
                    }
                }
                _ = shutdown_waiter.wait() => break,
            }
        }
    });
}

#[derive(Clone)]
struct LocalCommittedCachePublish {
    src_offset: u64,
    len: u32,
}

fn local_committed_cache_publish(
    op: &'static str,
    key: &str,
    put_id: crate::master_kv_router::put::PutIDForAKey,
    pending_ctx: &ExternalPendingPutCtx,
    req_src_offset: u64,
    req_len: u64,
) -> KvResult<LocalCommittedCachePublish> {
    if pending_ctx.src_offset != req_src_offset || pending_ctx.len != req_len {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "{op} local cache publish request mismatches pending ctx: key={} put_id=({},{}) req_src_offset={} ctx_src_offset={} req_len={} ctx_len={}",
                key,
                put_id.0,
                put_id.1,
                req_src_offset,
                pending_ctx.src_offset,
                req_len,
                pending_ctx.len
            ),
        }));
    }
    let len = u32::try_from(req_len).map_err(|_| {
        KvError::Api(ApiError::Unknown {
            detail: format!(
                "{op} local cache len does not fit u32: key={} len={}",
                key, req_len
            ),
        })
    })?;
    Ok(LocalCommittedCachePublish {
        src_offset: req_src_offset,
        len,
    })
}

fn external_pending_put_ctx_missing(
    op: &'static str,
    key: &str,
    put_id: crate::master_kv_router::put::PutIDForAKey,
) -> KvError {
    KvError::Api(ApiError::InvalidArgument {
        detail: format!(
            "{op} requires a live owner pending Put context: key={} put_id=({},{}) (missing, expired, or already terminal)",
            key, put_id.0, put_id.1
        ),
    })
}

fn require_external_pending_put_ctx(
    inner: &client_kv_api::ClientKvApiInner,
    op: &'static str,
    key: &str,
    put_id: crate::master_kv_router::put::PutIDForAKey,
) -> KvResult<ExternalPendingPutCtx> {
    require_external_pending_put_ctx_value(
        inner
            .external_pending_puts
            .get(&(key.to_string(), put_id.0, put_id.1)),
        op,
        key,
        put_id,
    )
}

fn require_external_pending_put_ctx_value(
    pending_ctx: Option<ExternalPendingPutCtx>,
    op: &'static str,
    key: &str,
    put_id: crate::master_kv_router::put::PutIDForAKey,
) -> KvResult<ExternalPendingPutCtx> {
    pending_ctx.ok_or_else(|| external_pending_put_ctx_missing(op, key, put_id))
}

async fn best_effort_revoke_missing_external_pending_ctx(
    inner: &client_kv_api::ClientKvApiInner,
    op: &'static str,
    key: &str,
    put_id: crate::master_kv_router::put::PutIDForAKey,
) -> bool {
    if let Err(err) = inner.put_revoke(key, put_id).await {
        tracing::warn!(
            "{} could not clean missing pending Put context at master: key={} put_id=({},{}) err={}",
            op,
            key,
            put_id.0,
            put_id.1,
            err
        );
        false
    } else {
        true
    }
}

fn external_local_first_error_item(err: &KvError) -> ExternalBatchPutStartItemResp {
    ExternalBatchPutStartItemResp {
        put_id: None,
        ..ExternalBatchPutStartItemResp::from_error(err)
    }
}

async fn commit_external_local_first_pending(
    inner: &client_kv_api::ClientKvApiInner,
    key: &str,
    put_id: crate::master_kv_router::put::PutIDForAKey,
    ctx: &ExternalPendingPutCtx,
    req_src_offset: u64,
    req_len: u64,
    op: &'static str,
) -> KvResult<PutDoneCommittedSlot> {
    if ctx.peer_id.is_some() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "{op} local-first pending ctx must not carry peer_id: key={} put_id=({},{})",
                key, put_id.0, put_id.1
            ),
        }));
    }
    if ctx.src_offset != req_src_offset || ctx.len != req_len {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "{op} local-first request mismatches pending ctx: key={} put_id=({},{}) req_src_offset={} ctx_src_offset={} req_len={} ctx_len={}",
                key, put_id.0, put_id.1, req_src_offset, ctx.src_offset, req_len, ctx.len
            ),
        }));
    }
    let slot_ref = ctx.local_reserve_slot.as_ref().ok_or_else(|| {
        KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "{op} pending ctx is not local-first: key={} put_id=({},{})",
                key, put_id.0, put_id.1
            ),
        })
    })?;
    let slot_size = ctx.local_reserve_slot_size.ok_or_else(|| {
        KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "{op} local-first pending ctx missing slot_size: key={} put_id=({},{})",
                key, put_id.0, put_id.1
            ),
        })
    })?;
    let len = u32::try_from(req_len).map_err(|_| {
        KvError::Api(ApiError::Unknown {
            detail: format!(
                "{op} local-first len does not fit u32: key={} len={}",
                key, req_len
            ),
        })
    })?;
    let memory_info = inner
        .build_local_reserve_resident_memory_info(
            key,
            slot_ref.ptr,
            len,
            slot_size,
            slot_ref.grant_id,
            slot_ref.slot_index,
        )
        .await;
    inner.install_precommit_local_visible_memory_info(key, memory_info.clone());
    ctx._pending_fence.disarm_local_slot_lease();
    Ok(PutDoneCommittedSlot {
        grant_id: slot_ref.grant_id,
        slot_index: slot_ref.slot_index,
        slot_size,
        addr: slot_ref.ptr,
        base_addr: slot_ref.base_addr,
        len: req_len,
    })
}

async fn release_external_local_first_pending_slot(
    inner: &client_kv_api::ClientKvApiInner,
    ctx: &ExternalPendingPutCtx,
    op: &'static str,
    key: &str,
    put_id: crate::master_kv_router::put::PutIDForAKey,
) {
    if let Err(err) = ctx._pending_fence.release_local_slot_lease_now(inner).await {
        tracing::error!(
            "{} could not release local-first pending slot: key={} put_id=({},{}) err={}",
            op,
            key,
            put_id.0,
            put_id.1,
            err
        );
    }
}

fn spawn_external_local_first_publish(
    inner: &client_kv_api::ClientKvApiInner,
    items: Vec<OwnerLocalPublishItem>,
    pending_contexts: Vec<ExternalPendingPutCtx>,
) {
    assert_eq!(
        items.len(),
        pending_contexts.len(),
        "external local-first publish job must retain one context per item"
    );
    let view = inner.view.clone_view();
    let spawn_view = view.clone();
    spawn_view.spawn("external_local_first_route_publish", async move {
        client_kv_api::put::publish_owner_local_job(
            view,
            OwnerLocalPublishJob {
                items,
                key_reservation_ids: Vec::new(),
                external_pending_contexts: pending_contexts,
            },
        )
        .await;
    });
}

pub(crate) fn normalize_external_get_start_group_lens(
    keys_len: usize,
    atomic_group_lens: Option<Vec<usize>>,
) -> KvResult<Vec<usize>> {
    match atomic_group_lens {
        Some(group_lens) => {
            if group_lens.is_empty() {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: "external_batch_get_start atomic_group_lens must be non-empty"
                        .to_string(),
                }));
            }
            if group_lens.iter().any(|len| *len == 0) {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: "external_batch_get_start atomic_group_lens entries must be > 0"
                        .to_string(),
                }));
            }
            let group_sum = group_lens.iter().sum::<usize>();
            if group_sum != keys_len {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "external_batch_get_start atomic_group_lens must sum to keys length: sum={} keys={}",
                        group_sum, keys_len
                    ),
                }));
            }
            Ok(group_lens)
        }
        None => Ok(vec![keys_len]),
    }
}

fn normalize_external_put_start_group_lens(
    items_len: usize,
    atomic_group_lens: Option<Vec<usize>>,
) -> KvResult<Vec<usize>> {
    let group_lens = atomic_group_lens.unwrap_or_else(|| vec![1; items_len]);
    if items_len != 0 && group_lens.is_empty() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "external_batch_put_start atomic_group_lens must be non-empty".to_string(),
        }));
    }
    if group_lens.iter().any(|group_len| *group_len == 0) {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "external_batch_put_start atomic_group_lens entries must be > 0".to_string(),
        }));
    }
    let sum = group_lens
        .iter()
        .try_fold(0usize, |sum, group_len| sum.checked_add(*group_len))
        .ok_or_else(|| {
            KvError::Api(ApiError::InvalidArgument {
                detail: "external_batch_put_start atomic_group_lens sum overflowed usize"
                    .to_string(),
            })
        })?;
    if sum != items_len {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "external_batch_put_start atomic_group_lens must sum to items length: sum={} items={}",
                sum, items_len
            ),
        }));
    }
    Ok(group_lens)
}

fn external_get_start_error_kind_from_code(error_code: u32, error_json: &str) -> Option<String> {
    if error_code == OK || error_code == codes_api::API_KEY_NOT_FOUND {
        None
    } else if error_json.is_empty() {
        Some(format!("error_code:{}", error_code))
    } else {
        Some(format!("error_code:{} {}", error_code, error_json))
    }
}

fn compute_external_get_start_raw_prefix(
    item_codes: &[(u32, String)],
) -> (usize, Option<usize>, Option<String>) {
    let mut prefix_hit_len = 0usize;
    for (idx, (error_code, error_json)) in item_codes.iter().enumerate() {
        if *error_code == OK {
            prefix_hit_len += 1;
            continue;
        }
        return (
            prefix_hit_len,
            Some(idx),
            external_get_start_error_kind_from_code(*error_code, error_json),
        );
    }
    (prefix_hit_len, None, None)
}

pub(crate) fn compute_external_get_start_transfer_prefix(
    raw_prefix_hit_len: usize,
    group_lens: &[usize],
    prefix_best_effort: bool,
) -> usize {
    let mut transferable_len = 0usize;
    for group_len in group_lens.iter().copied() {
        let next = transferable_len + group_len;
        if next > raw_prefix_hit_len {
            break;
        }
        transferable_len = next;
    }
    let requested_len = group_lens.iter().sum::<usize>();
    if !prefix_best_effort && transferable_len != requested_len {
        0
    } else {
        transferable_len
    }
}

pub(crate) fn validate_external_get_consume_prefix(
    consume_prefix_len: usize,
    transferable_len: usize,
    group_lens: &[usize],
) -> KvResult<()> {
    if consume_prefix_len == 0 || consume_prefix_len > transferable_len {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "external get_transfer consume_prefix_len must be within the live prefix: consume={} transferable={}",
                consume_prefix_len, transferable_len
            ),
        }));
    }

    let mut group_end = 0usize;
    for group_len in group_lens.iter().copied() {
        group_end = group_end.checked_add(group_len).ok_or_else(|| {
            KvError::Api(ApiError::InvalidArgument {
                detail: "external get_transfer atomic-group boundary overflowed usize".to_string(),
            })
        })?;
        if group_end >= consume_prefix_len {
            break;
        }
    }
    if group_end != consume_prefix_len {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "external get_transfer consume_prefix_len splits an atomic group: consume={} group_lens={:?}",
                consume_prefix_len, group_lens
            ),
        }));
    }
    Ok(())
}

fn collect_external_get_start_missing<T>(
    keys: &[String],
    item_slots: &[Option<T>],
) -> (Vec<usize>, Vec<String>) {
    assert_eq!(
        keys.len(),
        item_slots.len(),
        "external get_start local snapshot length must match keys"
    );
    keys.iter()
        .zip(item_slots.iter())
        .enumerate()
        .filter_map(|(idx, (key, item))| item.is_none().then(|| (idx, key.clone())))
        .unzip()
}

fn collect_all_local_external_get_start_infos(
    items: &[ExternalGetStartOwnerItem],
) -> Option<Vec<Arc<crate::memholder::MemoryInfo>>> {
    items
        .iter()
        .map(|item| match item {
            ExternalGetStartOwnerItem::Local { memory_info } => Some(memory_info.clone()),
            ExternalGetStartOwnerItem::Shared { .. } => None,
        })
        .collect()
}

fn prepared_target_from_slot(
    slot_size: u64,
    slot: &OwnerLocalReserveSlotRef,
) -> GetPreparedLocalReserveTarget {
    GetPreparedLocalReserveTarget {
        grant_id: slot.grant_id,
        slot_index: slot.slot_index,
        slot_size,
        addr: slot.ptr,
        base_addr: slot.base_addr,
    }
}

async fn release_prepared_get_target(
    inner: &client_kv_api::ClientKvApiInner,
    target: &GetPreparedLocalReserveTarget,
) -> KvResult<()> {
    inner
        .owner_release_local_reserve_slot_lease(OwnerLocalReserveSlotLease {
            value_len: target.slot_size,
            slot_size: target.slot_size,
            slots: vec![OwnerLocalReserveSlotRef {
                grant_id: target.grant_id,
                slot_index: target.slot_index,
                ptr: target.addr,
                base_addr: target.base_addr,
            }],
        })
        .await
}

/// Cancellation-safe ownership for slots claimed before BatchGetStart.
/// Accepted slots are disarmed one by one and become owned by their per-key
/// flight.  Any slots still present when the future is dropped are returned by
/// a registered owner task, so an RPC cancellation cannot strand Prepared
/// state in the local-reserve pool.
struct PreparedGetSlotClaimGuard {
    view: crate::client_kv_api::ClientKvApiView,
    lease: Option<OwnerLocalReserveSlotLease>,
    drop_delay: Duration,
}

impl PreparedGetSlotClaimGuard {
    fn new(view: crate::client_kv_api::ClientKvApiView, lease: OwnerLocalReserveSlotLease) -> Self {
        Self {
            view,
            lease: Some(lease),
            drop_delay: PREPARED_GET_START_UNCERTAIN_QUARANTINE,
        }
    }

    fn lease(&self) -> &OwnerLocalReserveSlotLease {
        self.lease.as_ref().expect("prepared slot claim is armed")
    }

    fn disarm_accepted(&mut self, target: &GetPreparedLocalReserveTarget) {
        let lease = self.lease.as_mut().expect("prepared slot claim is armed");
        lease.slots.retain(|slot| {
            !(slot.grant_id == target.grant_id
                && slot.slot_index == target.slot_index
                && slot.ptr == target.addr
                && slot.base_addr == target.base_addr)
        });
    }

    fn mark_start_response_received(&mut self) {
        self.drop_delay = Duration::ZERO;
    }

    fn handoff_started(
        &mut self,
        items: &[crate::master_kv_router::msg_pack::BatchGetStartItemResp],
    ) -> Vec<StartedGetRevokeCleanup> {
        items
            .iter()
            .filter(|item| item.error_code == OK)
            .map(|item| {
                if let Some(target) = item.prepared_target.as_ref() {
                    self.disarm_accepted(target);
                }
                StartedGetRevokeCleanup {
                    get_id: item.get_id,
                    prepared_target: item.prepared_target.clone(),
                }
            })
            .collect()
    }
}

impl Drop for PreparedGetSlotClaimGuard {
    fn drop(&mut self) {
        let Some(lease) = self.lease.take() else {
            return;
        };
        if lease.slots.is_empty() {
            return;
        }
        let drop_delay = self.drop_delay;
        let spawn_view = self.view.clone();
        let worker_view = spawn_view.clone();
        spawn_view.spawn("prepared_get_slot_drop_cleanup", async move {
            if !drop_delay.is_zero() {
                tracing::warn!(
                    "quarantining prepared Get slots after uncertain BatchGetStart: slots={} delay_secs={}",
                    lease.slots.len(),
                    drop_delay.as_secs()
                );
                let mut shutdown_waiter = worker_view.register_shutdown_waiter();
                tokio::select! {
                    _ = tokio::time::sleep(drop_delay) => {}
                    _ = shutdown_waiter.wait() => return,
                }
            }
            if let Err(err) = worker_view
                .client_kv_api()
                .inner()
                .owner_release_local_reserve_slot_lease(lease)
                .await
            {
                tracing::error!("prepared Get slot Drop cleanup failed: {}", err);
            }
        });
    }
}

async fn batch_get_start_with_local_reserve_targets(
    inner: &client_kv_api::ClientKvApiInner,
    keys: &[String],
) -> KvResult<BatchGetStartResp> {
    let Some(value_len) = inner
        .test_spec_config
        .owner_local_reserve_expected_capacity
        .as_ref()
        .map(|expected| expected.value_len)
    else {
        return inner.batch_get_start(keys.to_vec()).await;
    };
    let lease = inner
        .owner_claim_local_reserve_slot_lease(value_len, keys.len())
        .await?;
    let mut claim_guard = PreparedGetSlotClaimGuard::new(inner.view.clone_view(), lease);
    let prepared_targets = claim_guard
        .lease()
        .slots
        .iter()
        .map(|slot| {
            Some(prepared_target_from_slot(
                claim_guard.lease().slot_size,
                slot,
            ))
        })
        .collect::<Vec<_>>();
    let response_result = inner
        .batch_get_start_with_prepared_targets(keys.to_vec(), prepared_targets.clone())
        .await;
    let response = match response_result {
        Ok(response) => {
            claim_guard.mark_start_response_received();
            response
        }
        Err(err) => return Err(err),
    };
    if response.items.len() != keys.len() {
        let started = claim_guard.handoff_started(&response.items);
        if !started.is_empty() {
            finish_started_get_revoke_cleanup(inner, started, "BatchGetStart length mismatch")
                .await;
        }
        return Err(KvError::Api(ApiError::Unknown {
            detail: format!(
                "external_batch_get_start response length mismatch: expected={} got={}",
                keys.len(),
                response.items.len()
            ),
        }));
    }

    let accepted_exactly =
        response
            .items
            .iter()
            .zip(prepared_targets.iter())
            .all(|(item, requested_target)| {
                item.error_code != OK || item.prepared_target.as_ref() == requested_target.as_ref()
            });
    if !accepted_exactly {
        let started = claim_guard.handoff_started(&response.items);
        finish_started_get_revoke_cleanup(inner, started, "BatchGetStart target mismatch").await;
        return Err(KvError::Api(ApiError::Unknown {
            detail: "master did not accept the exact prepared local-reserve Get target".to_string(),
        }));
    }
    for (item, requested_target) in response.items.iter().zip(prepared_targets.iter()) {
        if item.error_code == OK {
            claim_guard.disarm_accepted(
                requested_target
                    .as_ref()
                    .expect("external prepared target vector must be dense"),
            );
        }
    }
    Ok(response)
}

async fn batch_get_bind_with_local_reserve_targets(
    inner: &client_kv_api::ClientKvApiInner,
    get_ids: &[u64],
) -> KvResult<Vec<BatchGetStartItemResp>> {
    if get_ids.is_empty() {
        return Ok(Vec::new());
    }
    let Some(value_len) = inner
        .test_spec_config
        .owner_local_reserve_expected_capacity
        .as_ref()
        .map(|expected| expected.value_len)
    else {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "planned external Get requires owner-local reserve capacity".to_string(),
        }));
    };
    let lease = inner
        .owner_claim_local_reserve_slot_lease(value_len, get_ids.len())
        .await?;
    let mut claim_guard = PreparedGetSlotClaimGuard::new(inner.view.clone_view(), lease);
    let prepared_targets = claim_guard
        .lease()
        .slots
        .iter()
        .map(|slot| prepared_target_from_slot(claim_guard.lease().slot_size, slot))
        .collect::<Vec<_>>();
    let response = match inner
        .batch_get_bind_prepared_targets(get_ids.to_vec(), prepared_targets.clone())
        .await
    {
        Ok(response) => {
            claim_guard.mark_start_response_received();
            response
        }
        Err(err) => return Err(err),
    };
    if response.items.len() != get_ids.len() {
        let started = claim_guard.handoff_started(&response.items);
        finish_started_get_revoke_cleanup(inner, started, "BatchGetBind length mismatch").await;
        return Err(KvError::Api(ApiError::Unknown {
            detail: format!(
                "planned BatchGetBind response length mismatch: expected={} got={}",
                get_ids.len(),
                response.items.len()
            ),
        }));
    }
    let identities_match = response
        .items
        .iter()
        .zip(get_ids)
        .all(|(item, get_id)| item.get_id == *get_id);
    let targets_match =
        response
            .items
            .iter()
            .zip(prepared_targets.iter())
            .all(|(item, expected)| {
                item.error_code != OK || item.prepared_target.as_ref() == Some(expected)
            });
    if !identities_match || !targets_match {
        let started = claim_guard.handoff_started(&response.items);
        finish_started_get_revoke_cleanup(inner, started, "BatchGetBind identity/target mismatch")
            .await;
        return Err(KvError::Api(ApiError::Unknown {
            detail: "master did not replay the exact planned Get identity and prepared target"
                .to_string(),
        }));
    }
    for (item, target) in response.items.iter().zip(prepared_targets.iter()) {
        if item.error_code == OK {
            claim_guard.disarm_accepted(target);
        }
    }
    Ok(response.items)
}

#[cfg(test)]
mod external_put_pending_tests {
    use super::{ExternalPutStartFenceClaim, require_external_pending_put_ctx_value};
    use crate::client_kv_api::{
        ExternalPendingPutFenceGuard, OwnerKeyControlTable,
        acquire_external_pending_put_fence_for_key,
    };
    use crate::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError};
    use parking_lot::Mutex;
    use std::sync::Arc;

    #[test]
    fn missing_or_expired_pending_context_is_not_a_commit_capability() {
        let result = require_external_pending_put_ctx_value(
            None,
            "external_put_commit",
            "missing-key",
            (17, 3),
        );
        let Err(KvError::Api(ApiError::InvalidArgument { detail })) = result else {
            panic!("missing pending context must be rejected")
        };
        assert!(detail.contains("requires a live owner pending Put context"));
        assert!(detail.contains("put_id=(17,3)"));
    }

    #[test]
    fn cancelled_put_start_hands_fence_to_uncertainty_quarantine() {
        let controls = Arc::new(OwnerKeyControlTable::default());
        let fence = acquire_external_pending_put_fence_for_key(&controls, "cancelled-start")
            .expect("pending fence acquisition must succeed");
        let sink: Arc<Mutex<Option<Arc<ExternalPendingPutFenceGuard>>>> =
            Arc::new(Mutex::new(None));

        let claim = ExternalPutStartFenceClaim::new_for_cancellation_test(fence, sink.clone());
        drop(claim);
        assert_eq!(
            controls.lock_key("cancelled-start")["cancelled-start"].external_pending_puts,
            1,
            "cancellation must transfer, not drop, the reclaim fence"
        );

        drop(sink.lock().take());
        assert!(
            controls
                .lock_key("cancelled-start")
                .get("cancelled-start")
                .is_none()
        );
    }
}

#[cfg(test)]
mod external_get_start_batch_tests {
    use super::{
        abandon_unstarted_external_get_key_locked,
        cleanup_external_get_start_handles_for_generation, clear_external_get_key_marker_locked,
        collect_external_get_start_missing, compute_external_get_start_raw_prefix,
        compute_external_get_start_transfer_prefix, decide_external_get_key_item,
        external_member_left_departed_epoch, normalize_external_put_start_group_lens,
        observe_external_get_consume_phases, partition_external_get_finish_leaders,
        register_external_get_key_under_fence, validate_external_get_consume_prefix,
    };
    use crate::client_kv_api::{
        ExternalGetKeyInterest, ExternalGetKeySharedOp, ExternalGetKeySharedPhase,
        ExternalGetStartEntry, ExternalGetStartOwnerItem, ExternalGetStartSharedItemResult,
        OwnerKeyControlState,
    };
    use crate::master_kv_router::msg_pack::{
        BatchGetStartItemResp, PutAtomicGroup, PutAtomicGroupMember,
    };
    use crate::rpcresp_kvresult_convert::msg_and_error::{OK, codes_api};
    use dashmap::DashMap;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    fn abandoned_handle_entry(
        req_node_id: &str,
        requester_node_start_time: Option<i64>,
    ) -> ExternalGetStartEntry {
        ExternalGetStartEntry {
            req_node_id: req_node_id.to_string(),
            requester_node_start_time,
            keys: Vec::new(),
            items: Vec::new(),
            atomic_group_lens: Vec::new(),
            created_at: Instant::now(),
        }
    }

    #[test]
    fn member_left_requires_previous_epoch_and_skips_delayed_leave_after_reconnect() {
        assert_eq!(
            external_member_left_departed_epoch(Some(11), None),
            Some(11)
        );
        assert_eq!(external_member_left_departed_epoch(None, None), None);
        assert_eq!(
            external_member_left_departed_epoch(Some(11), Some(12)),
            None
        );
        assert_eq!(external_member_left_departed_epoch(None, Some(12)), None);
    }

    #[test]
    fn member_left_removes_only_matching_requester_handle_generation() {
        let registry = DashMap::new();
        registry.insert(1, abandoned_handle_entry("external-a", Some(11)));
        registry.insert(2, abandoned_handle_entry("external-a", Some(12)));
        registry.insert(3, abandoned_handle_entry("external-b", Some(11)));
        registry.insert(4, abandoned_handle_entry("external-a", None));

        assert_eq!(
            cleanup_external_get_start_handles_for_generation(&registry, "external-a", 11),
            1
        );
        assert!(!registry.contains_key(&1));
        assert!(registry.contains_key(&2));
        assert!(registry.contains_key(&3));
        assert!(registry.contains_key(&4));
    }

    #[test]
    fn all_local_batch_has_no_master_fallback_and_keeps_full_atomic_prefix() {
        let keys = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let local_slots = vec![Some(1_u8), Some(2), Some(3)];

        let (missing_indices, missing_keys) =
            collect_external_get_start_missing(&keys, &local_slots);
        assert!(missing_indices.is_empty());
        assert!(missing_keys.is_empty());

        let item_codes = vec![(OK, String::new()); keys.len()];
        let (raw_prefix, first_miss, first_error) =
            compute_external_get_start_raw_prefix(&item_codes);
        assert_eq!(raw_prefix, keys.len());
        assert_eq!(first_miss, None);
        assert_eq!(first_error, None);
        assert_eq!(
            compute_external_get_start_transfer_prefix(raw_prefix, &[1, 2], true),
            keys.len()
        );
    }

    #[test]
    fn mixed_batch_falls_back_only_for_missing_keys_and_preserves_group_boundary() {
        let keys = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];
        let local_slots = vec![Some(1_u8), Some(2), None, Some(4)];

        let (missing_indices, missing_keys) =
            collect_external_get_start_missing(&keys, &local_slots);
        assert_eq!(missing_indices, vec![2]);
        assert_eq!(missing_keys, vec!["c"]);

        let item_codes = vec![
            (OK, String::new()),
            (OK, String::new()),
            (codes_api::API_KEY_NOT_FOUND, String::new()),
            (OK, String::new()),
        ];
        let (raw_prefix, first_miss, first_error) =
            compute_external_get_start_raw_prefix(&item_codes);
        assert_eq!(raw_prefix, 2);
        assert_eq!(first_miss, Some(2));
        assert_eq!(first_error, None);
        assert_eq!(
            compute_external_get_start_transfer_prefix(raw_prefix, &[2, 2], true),
            2
        );
        assert_eq!(
            compute_external_get_start_transfer_prefix(raw_prefix, &[2, 2], false),
            0
        );
    }

    #[test]
    fn get_transfer_consume_prefix_must_be_live_and_end_at_group_boundary() {
        let group_lens = [2, 2, 3];
        assert!(validate_external_get_consume_prefix(2, 4, &group_lens).is_ok());
        assert!(validate_external_get_consume_prefix(4, 4, &group_lens).is_ok());
        assert!(validate_external_get_consume_prefix(0, 4, &group_lens).is_err());
        assert!(validate_external_get_consume_prefix(1, 4, &group_lens).is_err());
        assert!(validate_external_get_consume_prefix(3, 4, &group_lens).is_err());
        assert!(validate_external_get_consume_prefix(5, 4, &group_lens).is_err());
    }

    #[test]
    fn consume_phase_snapshot_distinguishes_ready_data_from_real_wait() {
        let finishing = Arc::new(ExternalGetKeySharedOp::new("finishing".to_string()));
        finishing.state.lock().phase = ExternalGetKeySharedPhase::Finishing {
            item: BatchGetStartItemResp::default(),
        };
        let ready = Arc::new(ExternalGetKeySharedOp::new("ready".to_string()));
        let observed_at = Instant::now();
        {
            let mut state = ready.state.lock();
            state.phase = ExternalGetKeySharedPhase::Ready {
                result: ExternalGetStartSharedItemResult::Miss,
            };
            state.terminal_at = Some(observed_at - Duration::from_millis(5));
        }
        let items = vec![
            ExternalGetStartOwnerItem::Shared {
                interest: ExternalGetKeyInterest::new(finishing, false),
            },
            ExternalGetStartOwnerItem::Shared {
                interest: ExternalGetKeyInterest::new(ready, false),
            },
        ];

        let snapshot = observe_external_get_consume_phases(&items, observed_at);
        assert_eq!(snapshot.finishing, 1);
        assert_eq!(snapshot.ready, 1);
        assert_eq!(snapshot.pending_before_consume(), 1);
        assert_eq!(snapshot.terminal_before_consume(), 1);
        assert_eq!(snapshot.terminal_age_count, 1);
        assert_eq!(snapshot.terminal_age_mean_us(), 5_000);
        assert_eq!(snapshot.terminal_age_max_us, 5_000);
    }

    #[test]
    fn put_groups_default_per_item_and_reject_malformed_partitions() {
        assert_eq!(
            normalize_external_put_start_group_lens(3, None).unwrap(),
            vec![1, 1, 1]
        );
        assert_eq!(
            normalize_external_put_start_group_lens(3, Some(vec![2, 1])).unwrap(),
            vec![2, 1]
        );
        assert!(normalize_external_put_start_group_lens(3, Some(vec![1, 1])).is_err());
        assert!(normalize_external_put_start_group_lens(3, Some(vec![1, 0, 2])).is_err());
    }

    #[test]
    fn overlapping_nonidentical_batches_share_each_key_but_keep_leaders_batched() {
        fn register_batch(
            controls: &mut HashMap<String, OwnerKeyControlState>,
            keys: &[&str],
        ) -> (
            Vec<ExternalGetStartOwnerItem>,
            Vec<Arc<ExternalGetKeySharedOp>>,
        ) {
            let mut leaders = Vec::new();
            let items = keys
                .iter()
                .map(|key| {
                    let control = controls.entry((*key).to_string()).or_default();
                    let interest =
                        register_external_get_key_under_fence(control, key, &mut leaders);
                    ExternalGetStartOwnerItem::Shared { interest }
                })
                .collect();
            (items, leaders)
        }

        let mut controls = HashMap::new();
        let (mut batch_a, leaders_a) =
            register_batch(&mut controls, &["a", "shared-b", "shared-c"]);
        let (mut batch_b, leaders_b) =
            register_batch(&mut controls, &["shared-b", "shared-c", "d"]);

        assert_eq!(
            leaders_a.len(),
            3,
            "first required batch has one leader subset"
        );
        assert_eq!(
            leaders_b.len(),
            1,
            "second batch starts only its new leader d"
        );
        let shared_b_a = match &batch_a[1] {
            ExternalGetStartOwnerItem::Shared { interest } => interest.op().clone(),
            ExternalGetStartOwnerItem::Local { .. } => unreachable!(),
        };
        let shared_b_b = match &batch_b[0] {
            ExternalGetStartOwnerItem::Shared { interest } => interest.op().clone(),
            ExternalGetStartOwnerItem::Local { .. } => unreachable!(),
        };
        assert!(Arc::ptr_eq(&shared_b_a, &shared_b_b));

        // Batch A keeps only [a,b], while batch B keeps [b,c,d].  Prefix
        // decisions are independent, yet each physical key operation is still
        // represented by one shared marker.
        for (idx, item) in batch_a.iter_mut().enumerate() {
            decide_external_get_key_item(item, idx < 2);
        }
        for item in &mut batch_b {
            decide_external_get_key_item(item, true);
        }
        let state_b = shared_b_a.state.lock();
        assert_eq!(state_b.undecided, 0);
        assert_eq!(state_b.retained, 2);
        drop(state_b);
        let shared_c = controls["shared-c"]
            .external_get
            .as_ref()
            .expect("shared-c marker")
            .state
            .lock();
        assert_eq!(shared_c.undecided, 0);
        assert_eq!(shared_c.retained, 1);
    }

    #[test]
    fn dropped_pending_interest_retires_decision_without_losing_batch_sharing() {
        let key = "cancel-safe-key".to_string();
        let mut controls = HashMap::new();
        let mut first_leaders = Vec::new();
        let mut second_leaders = Vec::new();
        let first = register_external_get_key_under_fence(
            controls.entry(key.clone()).or_default(),
            &key,
            &mut first_leaders,
        );
        let mut second = register_external_get_key_under_fence(
            controls.entry(key.clone()).or_default(),
            &key,
            &mut second_leaders,
        );
        assert_eq!(first_leaders.len(), 1);
        assert!(second_leaders.is_empty());
        assert!(Arc::ptr_eq(first.op(), second.op()));
        assert_eq!(first.op().state.lock().undecided, 2);

        drop(first);
        assert_eq!(second.op().state.lock().undecided, 1);
        second.decide(true);
        let state = second.op().state.lock();
        assert_eq!(state.undecided, 0);
        assert_eq!(state.retained, 1);
    }

    #[limit_thirdparty::tokio::test]
    async fn aborting_waiter_future_retires_pending_interest_and_wakes_flight() {
        let op = Arc::new(ExternalGetKeySharedOp::new("abort-safe-key".to_string()));
        let waiter_op = op.clone();
        let (registered_tx, registered_rx) = ::tokio::sync::oneshot::channel();
        let waiter = ::tokio::spawn(async move {
            let _interest = crate::client_kv_api::ExternalGetKeyInterest::new(waiter_op, true);
            let _ = registered_tx.send(());
            futures::future::pending::<()>().await;
        });

        registered_rx.await.expect("waiter registered interest");
        assert_eq!(op.state.lock().undecided, 1);
        let notified = op.notify.notified();
        futures::pin_mut!(notified);
        notified.as_mut().enable();

        waiter.abort();
        assert!(
            waiter
                .await
                .expect_err("waiter must be aborted")
                .is_cancelled()
        );
        assert_eq!(op.state.lock().undecided, 0);
        ::tokio::time::timeout(std::time::Duration::from_secs(1), notified)
            .await
            .expect("interest Drop must wake the flight");
    }

    #[test]
    fn old_singleflight_cleanup_cannot_remove_new_generation() {
        let key = "aba-key".to_string();
        let old = Arc::new(ExternalGetKeySharedOp::new(key.clone()));
        let mut controls = HashMap::from([(
            key.clone(),
            OwnerKeyControlState {
                local_puts: 0,
                external_pending_puts: 0,
                external_put: None,
                remote_put: None,
                source_eviction_selection: None,
                reclaim: None,
                external_get: Some(old.clone()),
                local_access_fence: None,
            },
        )]);
        clear_external_get_key_marker_locked(&mut controls, &old);
        assert!(!controls.contains_key(&key));

        let new = Arc::new(ExternalGetKeySharedOp::new(key.clone()));
        controls.entry(key.clone()).or_default().external_get = Some(new.clone());
        clear_external_get_key_marker_locked(&mut controls, &old);
        assert!(
            controls[&key]
                .external_get
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &new))
        );
    }

    #[test]
    fn cancelled_planning_leader_becomes_miss_and_clears_only_its_generation() {
        let key = "planning-cancel-key".to_string();
        let op = Arc::new(ExternalGetKeySharedOp::new(key.clone()));
        let mut controls = HashMap::from([(
            key.clone(),
            OwnerKeyControlState {
                local_puts: 0,
                external_pending_puts: 0,
                external_put: None,
                remote_put: None,
                source_eviction_selection: None,
                reclaim: None,
                external_get: Some(op.clone()),
                local_access_fence: None,
            },
        )]);

        assert!(abandon_unstarted_external_get_key_locked(
            &mut controls,
            &op
        ));
        assert!(!controls.contains_key(&key));
        assert!(matches!(
            &op.state.lock().phase,
            crate::client_kv_api::ExternalGetKeySharedPhase::Ready {
                result: crate::client_kv_api::ExternalGetStartSharedItemResult::Miss
            }
        ));
        assert!(!abandon_unstarted_external_get_key_locked(
            &mut controls,
            &op
        ));
    }

    #[test]
    fn done_partition_keeps_atomic_groups_intact_and_bounds_other_batches() {
        let group = PutAtomicGroup {
            members: (0..3)
                .map(|idx| PutAtomicGroupMember {
                    key: format!("group-{idx}"),
                    put_id: (7, idx),
                })
                .collect(),
        };
        let leader = |key: &str, atomic_group: Option<PutAtomicGroup>| {
            (
                Arc::new(ExternalGetKeySharedOp::new(key.to_string())),
                BatchGetStartItemResp {
                    atomic_group,
                    ..Default::default()
                },
            )
        };
        let batches = partition_external_get_finish_leaders(
            vec![
                leader("single-a", None),
                leader("group-0", Some(group.clone())),
                leader("single-b", None),
                leader("group-1", Some(group.clone())),
                leader("group-2", Some(group.clone())),
                leader("single-c", None),
            ],
            2,
        );

        let group_batch_count = batches
            .iter()
            .filter(|batch| batch.iter().any(|(op, _)| op.key.starts_with("group-")))
            .count();
        assert_eq!(group_batch_count, 1, "one atomic group must not be split");
        let group_batch = batches
            .iter()
            .find(|batch| batch.iter().any(|(op, _)| op.key == "group-0"))
            .unwrap();
        assert_eq!(
            group_batch
                .iter()
                .filter(|(op, _)| op.key.starts_with("group-"))
                .count(),
            3
        );
        assert!(batches.iter().all(|batch| {
            batch.len() <= 2
                || batch
                    .iter()
                    .all(|(_, item)| item.atomic_group.as_ref() == Some(&group))
        }));
    }
}

#[derive(Debug, Default)]
struct ExternalGetConsumePhaseSnapshot {
    local: usize,
    starting: usize,
    started: usize,
    finishing: usize,
    revoking: usize,
    ready: usize,
    failed: usize,
    terminal_age_count: usize,
    terminal_age_sum_us: u64,
    terminal_age_max_us: u64,
}

impl ExternalGetConsumePhaseSnapshot {
    fn terminal_before_consume(&self) -> usize {
        self.local
            .saturating_add(self.ready)
            .saturating_add(self.failed)
    }

    fn pending_before_consume(&self) -> usize {
        self.starting
            .saturating_add(self.started)
            .saturating_add(self.finishing)
            .saturating_add(self.revoking)
    }

    fn terminal_age_mean_us(&self) -> u64 {
        if self.terminal_age_count == 0 {
            0
        } else {
            self.terminal_age_sum_us / self.terminal_age_count as u64
        }
    }
}

fn observe_external_get_consume_phases(
    items: &[ExternalGetStartOwnerItem],
    observed_at: Instant,
) -> ExternalGetConsumePhaseSnapshot {
    let mut snapshot = ExternalGetConsumePhaseSnapshot::default();
    for item in items {
        let ExternalGetStartOwnerItem::Shared { interest } = item else {
            snapshot.local = snapshot.local.saturating_add(1);
            continue;
        };
        let state = interest.op().state.lock();
        match &state.phase {
            ExternalGetKeySharedPhase::Starting => {
                snapshot.starting = snapshot.starting.saturating_add(1)
            }
            ExternalGetKeySharedPhase::Started { .. } => {
                snapshot.started = snapshot.started.saturating_add(1)
            }
            ExternalGetKeySharedPhase::Finishing { .. } => {
                snapshot.finishing = snapshot.finishing.saturating_add(1)
            }
            ExternalGetKeySharedPhase::Revoking { .. } => {
                snapshot.revoking = snapshot.revoking.saturating_add(1)
            }
            ExternalGetKeySharedPhase::Ready { .. } => {
                snapshot.ready = snapshot.ready.saturating_add(1)
            }
            ExternalGetKeySharedPhase::Failed { .. } => {
                snapshot.failed = snapshot.failed.saturating_add(1)
            }
        }
        if matches!(
            &state.phase,
            ExternalGetKeySharedPhase::Ready { .. } | ExternalGetKeySharedPhase::Failed { .. }
        ) {
            if let Some(age) = state
                .terminal_at
                .and_then(|terminal_at| observed_at.checked_duration_since(terminal_at))
            {
                let age_us = age.as_micros().min(u64::MAX as u128) as u64;
                snapshot.terminal_age_count = snapshot.terminal_age_count.saturating_add(1);
                snapshot.terminal_age_sum_us = snapshot.terminal_age_sum_us.saturating_add(age_us);
                snapshot.terminal_age_max_us = snapshot.terminal_age_max_us.max(age_us);
            }
        }
    }
    snapshot
}

async fn finish_external_get_start_transfer(
    view: crate::client_kv_api::ClientKvApiView,
    transfer_items: Vec<ExternalGetStartOwnerItem>,
    _transfer_concurrency: usize,
) -> KvResult<ExternalGetStartTransferOutput> {
    let client_api = view.client_kv_api();
    let inner = client_api.inner();
    let refcount = inner.get_or_init_all_memholder_refcount();
    let waits = transfer_items.into_iter().map(|item| {
        let refcount = refcount.clone();
        async move {
            match item {
                ExternalGetStartOwnerItem::Local { memory_info } => Ok(Some((
                    Arc::new(UserMemHolder::new(
                        memory_info,
                        refcount,
                        UserMemHolderExposeKind::SegPtr,
                    )),
                    None,
                ))),
                ExternalGetStartOwnerItem::Shared { interest } => {
                    match wait_external_get_key_result(interest.op().clone()).await? {
                        ExternalGetStartSharedItemResult::Hit { memholder } => {
                            Ok(Some((memholder, None)))
                        }
                        ExternalGetStartSharedItemResult::Miss => Ok(None),
                        ExternalGetStartSharedItemResult::Error {
                            error_code,
                            error_json,
                        } => Err(KvError::from_json(error_code, &error_json)),
                    }
                }
            }
        }
    });
    Ok(futures::future::join_all(waits).await)
}

fn external_get_start_error_parts(err: &KvError) -> (u32, String) {
    (err.code(), err.to_json())
}

async fn wait_external_get_key_not_revoking(op: Arc<ExternalGetKeySharedOp>) {
    loop {
        let notified = op.notify.notified();
        futures::pin_mut!(notified);
        let should_wait = {
            let state = op.state.lock();
            if matches!(state.phase, ExternalGetKeySharedPhase::Revoking { .. }) {
                notified.as_mut().enable();
                true
            } else {
                false
            }
        };
        if !should_wait {
            return;
        }
        notified.await;
    }
}

fn register_external_get_key_under_fence(
    control: &mut client_kv_api::OwnerKeyControlState,
    key: &str,
    leaders: &mut Vec<Arc<ExternalGetKeySharedOp>>,
) -> ExternalGetKeyInterest {
    if let Some(op) = control.external_get.clone() {
        let mut state = op.state.lock();
        let decision_registered = match state.phase {
            ExternalGetKeySharedPhase::Starting | ExternalGetKeySharedPhase::Started { .. } => {
                state.undecided = state
                    .undecided
                    .checked_add(1)
                    .expect("external Get singleflight undecided overflow");
                true
            }
            ExternalGetKeySharedPhase::Finishing { .. }
            | ExternalGetKeySharedPhase::Ready { .. }
            | ExternalGetKeySharedPhase::Failed { .. } => false,
            ExternalGetKeySharedPhase::Revoking { .. } => {
                unreachable!("revoking markers were checked under the same fence")
            }
        };
        drop(state);
        ExternalGetKeyInterest::new(op, decision_registered)
    } else {
        let op = Arc::new(ExternalGetKeySharedOp::new(key.to_string()));
        control.external_get = Some(op.clone());
        leaders.push(op.clone());
        ExternalGetKeyInterest::new(op, true)
    }
}

/// Owns newly installed `Starting` markers until the complete request batch is
/// ready to hand them to one BatchGetStart worker.  If planning is cancelled
/// while waiting for an older Revoke on another key, Drop converts only these
/// never-started operations to a safe miss and removes their markers.  No
/// prepared target or master identity exists yet, so there is nothing to
/// revoke and no storage state to guess.
struct ExternalGetPlanningLeadersGuard<'a> {
    inner: &'a client_kv_api::ClientKvApiInner,
    leaders: Vec<Arc<ExternalGetKeySharedOp>>,
    handed_off: bool,
}

impl<'a> ExternalGetPlanningLeadersGuard<'a> {
    fn new(inner: &'a client_kv_api::ClientKvApiInner) -> Self {
        Self {
            inner,
            leaders: Vec::new(),
            handed_off: false,
        }
    }

    fn leaders_mut(&mut self) -> &mut Vec<Arc<ExternalGetKeySharedOp>> {
        &mut self.leaders
    }

    fn handoff(mut self) -> Vec<Arc<ExternalGetKeySharedOp>> {
        self.handed_off = true;
        std::mem::take(&mut self.leaders)
    }
}

impl Drop for ExternalGetPlanningLeadersGuard<'_> {
    fn drop(&mut self) {
        if self.handed_off {
            return;
        }
        for op in &self.leaders {
            let notify = {
                let mut controls = self.inner.owner_key_control.lock_key(&op.key);
                abandon_unstarted_external_get_key_locked(&mut controls, op)
            };
            if notify {
                self.inner.untrack_external_get_flight(op);
                op.notify.notify_waiters();
            }
        }
    }
}

async fn plan_external_get_key_items(
    inner: &client_kv_api::ClientKvApiInner,
    keys: &[String],
) -> (
    Vec<ExternalGetStartOwnerItem>,
    Vec<Arc<ExternalGetKeySharedOp>>,
) {
    enum KeyAttempt {
        Wait(Arc<ExternalGetKeySharedOp>),
        Ready {
            item: ExternalGetStartOwnerItem,
            hot_touch: Option<(
                crate::master_kv_router::put::PutIDForAKey,
                Arc<crate::memholder::MemoryInfo>,
            )>,
        },
    }

    let mut items = Vec::with_capacity(keys.len());
    let mut planning_leaders = ExternalGetPlanningLeadersGuard::new(inner);
    let mut hot_touches = Vec::new();
    for key in keys {
        loop {
            // One short per-key shard section atomically chooses local,
            // joiner, or leader.  No synchronous lock spans the request batch
            // or the wait for an older Revoke to finish.
            let attempt = {
                let mut controls = inner.owner_key_control.lock_key(key);
                let revoking = controls
                    .get(key)
                    .and_then(|state| state.external_get.clone())
                    .filter(|op| {
                        matches!(
                            op.state.lock().phase,
                            ExternalGetKeySharedPhase::Revoking { .. }
                        )
                    });
                if let Some(op) = revoking {
                    KeyAttempt::Wait(op)
                } else {
                    let fenced = controls
                        .get(key)
                        .is_some_and(|state| state.local_access_fenced());
                    if !fenced {
                        if let Some(memory_info) = inner.local_visible_mem_holder_unfenced(key) {
                            let hot_touch = inner.get_cached_info.get(key).and_then(|cached| {
                                Arc::ptr_eq(&cached.mem_holder, &memory_info).then_some((
                                    (cached.put_time_ms, cached.put_version),
                                    memory_info.clone(),
                                ))
                            });
                            KeyAttempt::Ready {
                                item: ExternalGetStartOwnerItem::Local { memory_info },
                                hot_touch,
                            }
                        } else {
                            let control = controls.entry(key.clone()).or_default();
                            let leader_count = planning_leaders.leaders.len();
                            let interest = register_external_get_key_under_fence(
                                control,
                                key,
                                planning_leaders.leaders_mut(),
                            );
                            if planning_leaders.leaders.len() != leader_count {
                                inner.track_external_get_flight(interest.op());
                            }
                            KeyAttempt::Ready {
                                item: ExternalGetStartOwnerItem::Shared { interest },
                                hot_touch: None,
                            }
                        }
                    } else {
                        let control = controls.entry(key.clone()).or_default();
                        let leader_count = planning_leaders.leaders.len();
                        let interest = register_external_get_key_under_fence(
                            control,
                            key,
                            planning_leaders.leaders_mut(),
                        );
                        if planning_leaders.leaders.len() != leader_count {
                            inner.track_external_get_flight(interest.op());
                        }
                        KeyAttempt::Ready {
                            item: ExternalGetStartOwnerItem::Shared { interest },
                            hot_touch: None,
                        }
                    }
                }
            };

            match attempt {
                KeyAttempt::Wait(op) => wait_external_get_key_not_revoking(op).await,
                KeyAttempt::Ready { item, hot_touch } => {
                    if let Some((put_id, memory_info)) = hot_touch {
                        hot_touches.push((key.clone(), put_id, memory_info));
                    }
                    items.push(item);
                    break;
                }
            }
        }
    }

    for (key, put_id, memory_info) in hot_touches {
        inner.owner_hot_touch_or_promote(&key, put_id, &memory_info);
    }
    (items, planning_leaders.handoff())
}

fn clear_external_get_key_marker_locked(
    controls: &mut std::collections::HashMap<String, client_kv_api::OwnerKeyControlState>,
    op: &Arc<ExternalGetKeySharedOp>,
) {
    let remove_control = if let Some(control) = controls.get_mut(&op.key) {
        if control
            .external_get
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, op))
        {
            control.external_get = None;
        }
        control.is_idle()
    } else {
        false
    };
    if remove_control {
        controls.remove(&op.key);
    }
}

fn abandon_unstarted_external_get_key_locked(
    controls: &mut std::collections::HashMap<String, client_kv_api::OwnerKeyControlState>,
    op: &Arc<ExternalGetKeySharedOp>,
) -> bool {
    let mut state = op.state.lock();
    if !matches!(state.phase, ExternalGetKeySharedPhase::Starting) {
        return false;
    }
    state.phase = ExternalGetKeySharedPhase::Ready {
        result: ExternalGetStartSharedItemResult::Miss,
    };
    state.terminal_at = Some(Instant::now());
    clear_external_get_key_marker_locked(controls, op);
    true
}

fn publish_external_get_key_terminal(
    inner: &client_kv_api::ClientKvApiInner,
    op: &Arc<ExternalGetKeySharedOp>,
    phase: ExternalGetKeySharedPhase,
) {
    {
        let mut controls = inner.owner_key_control.lock_key(&op.key);
        let mut state = op.state.lock();
        state.phase = phase;
        state.terminal_at = Some(Instant::now());
        clear_external_get_key_marker_locked(&mut controls, op);
    }
    inner.untrack_external_get_flight(op);
    op.notify.notify_waiters();
}

fn publish_external_get_key_failed(
    inner: &client_kv_api::ClientKvApiInner,
    op: &Arc<ExternalGetKeySharedOp>,
    err: &KvError,
) {
    let (error_code, error_json) = external_get_start_error_parts(err);
    publish_external_get_key_terminal(
        inner,
        op,
        ExternalGetKeySharedPhase::Failed {
            error_code,
            error_json,
        },
    );
}

fn publish_external_get_key_started(
    op: &Arc<ExternalGetKeySharedOp>,
    item: crate::master_kv_router::msg_pack::BatchGetStartItemResp,
) {
    {
        let mut state = op.state.lock();
        assert!(matches!(state.phase, ExternalGetKeySharedPhase::Starting));
        state.phase = ExternalGetKeySharedPhase::Started { item };
    }
    op.notify.notify_waiters();
}

fn external_get_key_ready_from_code(
    error_code: u32,
    error_json: String,
) -> ExternalGetStartSharedItemResult {
    if error_code == codes_api::API_KEY_NOT_FOUND {
        ExternalGetStartSharedItemResult::Miss
    } else {
        ExternalGetStartSharedItemResult::Error {
            error_code,
            error_json,
        }
    }
}

async fn wait_external_get_key_start_code(
    op: Arc<ExternalGetKeySharedOp>,
) -> KvResult<(u32, String)> {
    loop {
        let notified = op.notify.notified();
        futures::pin_mut!(notified);
        let should_wait = {
            let state = op.state.lock();
            match &state.phase {
                ExternalGetKeySharedPhase::Starting => {
                    notified.as_mut().enable();
                    true
                }
                ExternalGetKeySharedPhase::Started { item }
                | ExternalGetKeySharedPhase::Finishing { item } => {
                    return Ok((item.error_code, item.error_json.clone()));
                }
                ExternalGetKeySharedPhase::Ready { result } => {
                    return Ok(match result {
                        ExternalGetStartSharedItemResult::Hit { .. } => (OK, String::new()),
                        ExternalGetStartSharedItemResult::Miss => {
                            (codes_api::API_KEY_NOT_FOUND, String::new())
                        }
                        ExternalGetStartSharedItemResult::Error {
                            error_code,
                            error_json,
                        } => (*error_code, error_json.clone()),
                    });
                }
                ExternalGetKeySharedPhase::Failed {
                    error_code,
                    error_json,
                } => return Err(KvError::from_json(*error_code, error_json)),
                ExternalGetKeySharedPhase::Revoking { .. } => {
                    return Err(KvError::Api(ApiError::Unknown {
                        detail: format!(
                            "external Get key entered revoke before prefix decision: key={}",
                            op.key
                        ),
                    }));
                }
            }
        };
        if should_wait {
            notified.await;
        }
    }
}

async fn wait_external_get_key_result(
    op: Arc<ExternalGetKeySharedOp>,
) -> KvResult<ExternalGetStartSharedItemResult> {
    loop {
        let notified = op.notify.notified();
        futures::pin_mut!(notified);
        let should_wait = {
            let state = op.state.lock();
            match &state.phase {
                ExternalGetKeySharedPhase::Starting
                | ExternalGetKeySharedPhase::Started { .. }
                | ExternalGetKeySharedPhase::Finishing { .. } => {
                    notified.as_mut().enable();
                    true
                }
                ExternalGetKeySharedPhase::Ready { result } => return Ok(result.clone()),
                ExternalGetKeySharedPhase::Failed {
                    error_code,
                    error_json,
                } => return Err(KvError::from_json(*error_code, error_json)),
                ExternalGetKeySharedPhase::Revoking { .. } => {
                    return Err(KvError::Api(ApiError::Unknown {
                        detail: format!(
                            "retained external Get key was unexpectedly revoked: key={}",
                            op.key
                        ),
                    }));
                }
            }
        };
        if should_wait {
            notified.await;
        }
    }
}

fn decide_external_get_key_item(item: &mut ExternalGetStartOwnerItem, retain: bool) {
    if let ExternalGetStartOwnerItem::Shared { interest } = item {
        interest.decide(retain);
    }
}

enum ExternalGetKeyLeaderAction {
    Finish {
        op: Arc<ExternalGetKeySharedOp>,
        item: crate::master_kv_router::msg_pack::BatchGetStartItemResp,
    },
    Revoke {
        op: Arc<ExternalGetKeySharedOp>,
        item: crate::master_kv_router::msg_pack::BatchGetStartItemResp,
    },
    Terminal,
}

type ExternalGetFinishLeader = (Arc<ExternalGetKeySharedOp>, BatchGetStartItemResp);

struct ExternalGetFinishUnit {
    group: Option<PutAtomicGroup>,
    leaders: Vec<ExternalGetFinishLeader>,
}

/// Partition leaders into independent Done failure domains while preserving
/// caller-declared atomic groups. The target is deliberately soft: one large
/// atomic group stays intact instead of being split across terminal RPCs.
fn partition_external_get_finish_leaders(
    leaders: Vec<ExternalGetFinishLeader>,
    target_keys_per_batch: usize,
) -> Vec<Vec<ExternalGetFinishLeader>> {
    if leaders.is_empty() {
        return Vec::new();
    }
    let target_keys_per_batch = target_keys_per_batch.max(1);
    let mut units: Vec<ExternalGetFinishUnit> = Vec::new();
    let mut group_unit_by_anchor = HashMap::<(String, u64, u32), usize>::new();

    for leader in leaders {
        let group = leader.1.atomic_group.as_ref();
        let Some(group) = group else {
            units.push(ExternalGetFinishUnit {
                group: None,
                leaders: vec![leader],
            });
            continue;
        };
        let Some(anchor) = group.members.first() else {
            // Master validation rejects empty groups. Treat malformed legacy
            // metadata as a singleton here so partitioning cannot lose work.
            units.push(ExternalGetFinishUnit {
                group: None,
                leaders: vec![leader],
            });
            continue;
        };
        let signature = (anchor.key.clone(), anchor.put_id.0, anchor.put_id.1);
        if let Some(unit_idx) = group_unit_by_anchor.get(&signature).copied()
            && units[unit_idx].group.as_ref() == Some(group)
        {
            units[unit_idx].leaders.push(leader);
            continue;
        }
        let unit_idx = units.len();
        units.push(ExternalGetFinishUnit {
            group: Some(group.clone()),
            leaders: vec![leader],
        });
        group_unit_by_anchor.insert(signature, unit_idx);
    }

    let mut batches = Vec::new();
    let mut current = Vec::new();
    for mut unit in units {
        if !current.is_empty()
            && current.len().saturating_add(unit.leaders.len()) > target_keys_per_batch
        {
            batches.push(std::mem::take(&mut current));
        }
        current.append(&mut unit.leaders);
        if current.len() >= target_keys_per_batch {
            batches.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        batches.push(current);
    }
    batches
}

async fn classify_external_get_key_leader(
    inner: &client_kv_api::ClientKvApiInner,
    op: Arc<ExternalGetKeySharedOp>,
) -> ExternalGetKeyLeaderAction {
    loop {
        let notified = op.notify.notified();
        futures::pin_mut!(notified);
        let wait_for_decisions = {
            let state = op.state.lock();
            if matches!(
                state.phase,
                ExternalGetKeySharedPhase::Starting | ExternalGetKeySharedPhase::Started { .. }
            ) && state.undecided != 0
            {
                notified.as_mut().enable();
                true
            } else {
                false
            }
        };
        if wait_for_decisions {
            notified.await;
            continue;
        }

        let mut notify_terminal = false;
        let action = {
            // Planner lock order is owner fence -> per-key op.  Use the same
            // order here so no request can join after undecided reaches zero
            // but before the leader closes admission.
            let mut controls = inner.owner_key_control.lock_key(&op.key);
            let mut state = op.state.lock();
            match &state.phase {
                ExternalGetKeySharedPhase::Started { item } if state.undecided == 0 => {
                    let item = item.clone();
                    if item.error_code != OK {
                        state.phase = ExternalGetKeySharedPhase::Ready {
                            result: external_get_key_ready_from_code(
                                item.error_code,
                                item.error_json,
                            ),
                        };
                        state.terminal_at = Some(Instant::now());
                        clear_external_get_key_marker_locked(&mut controls, &op);
                        notify_terminal = true;
                        Some(ExternalGetKeyLeaderAction::Terminal)
                    } else if state.retained == 0 {
                        state.phase = ExternalGetKeySharedPhase::Revoking { item: item.clone() };
                        Some(ExternalGetKeyLeaderAction::Revoke {
                            op: op.clone(),
                            item,
                        })
                    } else {
                        state.phase = ExternalGetKeySharedPhase::Finishing { item: item.clone() };
                        Some(ExternalGetKeyLeaderAction::Finish {
                            op: op.clone(),
                            item,
                        })
                    }
                }
                ExternalGetKeySharedPhase::Starting | ExternalGetKeySharedPhase::Started { .. } => {
                    None
                }
                ExternalGetKeySharedPhase::Finishing { .. }
                | ExternalGetKeySharedPhase::Revoking { .. }
                | ExternalGetKeySharedPhase::Ready { .. }
                | ExternalGetKeySharedPhase::Failed { .. } => {
                    Some(ExternalGetKeyLeaderAction::Terminal)
                }
            }
        };
        if notify_terminal {
            inner.untrack_external_get_flight(&op);
            op.notify.notify_waiters();
        }
        if let Some(action) = action {
            return action;
        }
        tokio::task::yield_now().await;
    }
}

async fn finish_external_get_key_leaders(
    view: crate::client_kv_api::ClientKvApiView,
    leaders: Vec<Arc<ExternalGetKeySharedOp>>,
    transfer_concurrency: usize,
) {
    let client_api = view.client_kv_api();
    let inner = client_api.inner();
    let mut finish = Vec::new();
    let mut revoke = Vec::new();
    for op in leaders {
        match classify_external_get_key_leader(inner, op).await {
            ExternalGetKeyLeaderAction::Finish { op, item } => finish.push((op, item)),
            ExternalGetKeyLeaderAction::Revoke { op, item } => revoke.push((op, item)),
            ExternalGetKeyLeaderAction::Terminal => {}
        }
    }

    if !revoke.is_empty() {
        let mut attempt = 1u32;
        loop {
            let get_ids = revoke
                .iter()
                .map(|(_, item)| item.get_id)
                .collect::<Vec<_>>();
            let response = inner.batch_get_revoke(get_ids).await;
            let resp = match response {
                Ok(resp)
                    if resp.items.len() == revoke.len()
                        && resp
                            .items
                            .iter()
                            .zip(&revoke)
                            .all(|(resp, (_, expected))| resp.get_id == expected.get_id) =>
                {
                    resp
                }
                Ok(resp) => {
                    tracing::warn!(
                        "BatchGetRevoke response length mismatch; retaining prepared slots and retrying: expected={} got={} attempt={}",
                        revoke.len(),
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
                            "BatchGetRevoke cleanup stopped during owner shutdown: items={}",
                            revoke.len()
                        );
                        return;
                    }
                    tracing::warn!(
                        "BatchGetRevoke uncertain; retaining operation identity and prepared slots for retry: items={} attempt={} err={}",
                        revoke.len(),
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

            for ((op, item), revoke_item) in revoke.drain(..).zip(resp.items) {
                if let Err(err) = crate::rpcresp_kvresult_convert::try_from_code(
                    revoke_item.error_code,
                    revoke_item.error_json,
                ) {
                    // Done won the per-get terminal lock (or requester identity
                    // is invalid).  The candidate may be committed, so never
                    // return its slot from the losing Revoke path.
                    publish_external_get_key_failed(inner, &op, &err);
                    continue;
                }
                if let Some(target) = item.prepared_target.as_ref() {
                    let mut release_attempt = 1u32;
                    loop {
                        match release_prepared_get_target(inner, target).await {
                            Ok(()) => break,
                            Err(err) => {
                                if !inner.view.register_shutdown_poller().is_running() {
                                    return;
                                }
                                tracing::error!(
                                    "Revoke confirmed but prepared slot release failed; keeping key fenced for retry: key={} get_id={} attempt={} err={}",
                                    op.key,
                                    item.get_id,
                                    release_attempt,
                                    err
                                );
                                tokio::time::sleep(Duration::from_millis(1000)).await;
                                release_attempt = release_attempt.saturating_add(1);
                            }
                        }
                    }
                }
                publish_external_get_key_terminal(
                    inner,
                    &op,
                    ExternalGetKeySharedPhase::Ready {
                        result: ExternalGetStartSharedItemResult::Miss,
                    },
                );
            }
            break;
        }
    }

    if finish.is_empty() {
        return;
    }
    let finish_batches =
        partition_external_get_finish_leaders(finish, EXTERNAL_GET_FINISH_TARGET_KEYS_PER_BATCH);
    let batch_concurrency = EXTERNAL_GET_FINISH_BATCH_CONCURRENCY
        .min(finish_batches.len())
        .max(1);
    let per_batch_transfer_concurrency = transfer_concurrency
        .max(1)
        .div_ceil(batch_concurrency)
        .max(1);
    tracing::debug!(
        "external Get singleflight finish partitioned: batches={} batch_concurrency={} transfer_concurrency_per_batch={}",
        finish_batches.len(),
        batch_concurrency,
        per_batch_transfer_concurrency
    );

    let finish_futures = finish_batches.into_iter().map(|batch| {
        let batch_view = view.clone();
        async move {
            let keys = batch
                .iter()
                .map(|(op, _)| op.key.clone())
                .collect::<Vec<_>>();
            let start_items = batch
                .iter()
                .map(|(_, item)| item.clone())
                .collect::<Vec<_>>();
            let result = batch_view
                .client_kv_api()
                .inner()
                .batch_get_finish_started(keys, start_items, per_batch_transfer_concurrency)
                .await;
            (batch, result)
        }
    });
    let mut finish_stream =
        futures::stream::iter(finish_futures).buffer_unordered(batch_concurrency);
    while let Some((batch, finish_result)) = finish_stream.next().await {
        match finish_result {
            Ok(results) if results.len() == batch.len() => {
                // Publish each confirmed sub-batch immediately. A different
                // uncertain Done atomic_batch therefore cannot retain these flights
                // or their pending-visible slots.
                for ((op, _), result) in batch.into_iter().zip(results) {
                    let phase = match result {
                        Ok(Some((memholder, _))) => ExternalGetKeySharedPhase::Ready {
                            result: ExternalGetStartSharedItemResult::Hit { memholder },
                        },
                        Ok(None) => ExternalGetKeySharedPhase::Ready {
                            result: ExternalGetStartSharedItemResult::Miss,
                        },
                        Err(err) => {
                            let (error_code, error_json) = external_get_start_error_parts(&err);
                            ExternalGetKeySharedPhase::Ready {
                                result: ExternalGetStartSharedItemResult::Error {
                                    error_code,
                                    error_json,
                                },
                            }
                        }
                    };
                    publish_external_get_key_terminal(inner, &op, phase);
                }
            }
            Ok(results) => {
                let err = KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "singleflight leader finish length mismatch: expected={} got={}",
                        batch.len(),
                        results.len()
                    ),
                });
                for (op, _) in batch {
                    publish_external_get_key_failed(inner, &op, &err);
                }
            }
            Err(err) => {
                for (op, _) in batch {
                    publish_external_get_key_failed(inner, &op, &err);
                }
            }
        }
    }
}

async fn prepare_external_get_batch_plan(
    inner: &client_kv_api::ClientKvApiInner,
    req: &ExternalBatchGetStartReq,
    group_lens: &[usize],
) -> KvResult<(
    ExternalGetStartPrefixResult,
    Vec<String>,
    Vec<ExternalGetStartOwnerItem>,
)> {
    let (mut items, leaders) = plan_external_get_key_items(inner, &req.keys).await;
    if !leaders.is_empty() {
        let leader_keys = leaders.iter().map(|op| op.key.clone()).collect::<Vec<_>>();
        let leader_count = leaders.len();
        let spawn_view = inner.view.clone_view();
        let worker_view = spawn_view.clone();
        let transfer_concurrency = req.transfer_concurrency;
        spawn_view.spawn("external_get_key_singleflight", async move {
            let batch_start_started_at = Instant::now();
            let start_result = {
                let worker_inner = worker_view.client_kv_api().inner();
                batch_get_start_with_local_reserve_targets(worker_inner, &leader_keys).await
            };
            let batch_start_us = duration_to_i64_us(batch_start_started_at.elapsed());
            match start_result {
                Ok(start_resp) => {
                    assert_eq!(
                        start_resp.items.len(),
                        leaders.len(),
                        "validated BatchGetStart response must match leader count"
                    );
                    let start_hits = start_resp
                        .items
                        .iter()
                        .filter(|item| item.error_code == OK)
                        .count();
                    let start_misses = start_resp
                        .items
                        .iter()
                        .filter(|item| item.error_code == codes_api::API_KEY_NOT_FOUND)
                        .count();
                    let start_errors = start_resp
                        .items
                        .len()
                        .saturating_sub(start_hits)
                        .saturating_sub(start_misses);
                    tracing::info!(
                        "external Get leader start lifecycle: leaders={} hits={} misses={} errors={} batch_get_start_us={} outcome=ok",
                        leader_count,
                        start_hits,
                        start_misses,
                        start_errors,
                        batch_start_us,
                    );
                    for (op, item) in leaders.iter().zip(start_resp.items) {
                        publish_external_get_key_started(op, item);
                    }
                    finish_external_get_key_leaders(worker_view, leaders, transfer_concurrency)
                        .await;
                }
                Err(err) => {
                    tracing::info!(
                        "external Get leader start lifecycle: leaders={} hits=0 misses=0 errors={} batch_get_start_us={} outcome=error",
                        leader_count,
                        leader_count,
                        batch_start_us,
                    );
                    let worker_inner = worker_view.client_kv_api().inner();
                    for op in &leaders {
                        publish_external_get_key_failed(worker_inner, op, &err);
                    }
                }
            }
        });
    }

    let mut item_codes = Vec::with_capacity(items.len());
    for item in &items {
        match item {
            ExternalGetStartOwnerItem::Local { .. } => item_codes.push((OK, String::new())),
            ExternalGetStartOwnerItem::Shared { interest } => {
                match wait_external_get_key_start_code(interest.op().clone()).await {
                    Ok(code) => item_codes.push(code),
                    Err(err) => return Err(err),
                }
            }
        }
    }

    let (raw_prefix_hit_len, first_miss_index, first_error_kind) =
        compute_external_get_start_raw_prefix(&item_codes);
    let transferable_len = compute_external_get_start_transfer_prefix(
        raw_prefix_hit_len,
        group_lens,
        req.prefix_best_effort,
    );
    if let Some(error_kind) = first_error_kind.as_ref() {
        tracing::warn!(
            "external_batch_get_start prefix stopped on non-key-miss item: req_node_id={} raw_prefix_hit_len={} transferable_len={} first_miss_index={:?} error_kind={}",
            req.req_node_id,
            raw_prefix_hit_len,
            transferable_len,
            first_miss_index,
            error_kind
        );
    }
    for (idx, item) in items.iter_mut().enumerate() {
        decide_external_get_key_item(item, idx < transferable_len);
    }

    let transfer_keys = req.keys[..transferable_len].to_vec();
    let transfer_items = items.into_iter().take(transferable_len).collect::<Vec<_>>();
    Ok((
        ExternalGetStartPrefixResult {
            raw_prefix_hit_len,
            transferable_len,
            first_miss_index,
            first_error_kind,
        },
        transfer_keys,
        transfer_items,
    ))
}

impl ClientKvApi {
    fn is_side_transfer_worker(&self) -> bool {
        self.inner()
            .view
            .cluster_manager()
            .get_self_info()
            .metadata
            .get("side_transfer_worker")
            .is_some_and(|v| v == "true")
    }

    fn expected_owner_start_time_for_external_path(&self) -> i64 {
        let self_info = self.inner().view.cluster_manager().get_self_info();
        if !self.is_side_transfer_worker() {
            return self_info.node_start_time;
        }
        self_info
            .metadata
            .get(META_KEY_SHARED_STORAGE_NODE_START_TIME)
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(self_info.node_start_time)
    }

    fn owner_node_id_for_side_transfer(&self) -> KvResult<String> {
        self.inner()
            .view
            .cluster_manager()
            .get_self_info()
            .metadata
            .get(META_KEY_SHARED_STORAGE_NODE_ID)
            .cloned()
            .ok_or_else(|| {
                KvError::Api(ApiError::Unknown {
                    detail: "side-transfer worker missing shared-storage owner id".to_string(),
                })
            })
    }

    fn side_transfer_worker_lane_idx(&self) -> KvResult<u16> {
        let self_id = self.inner().view.cluster_manager().get_self_info().id;
        parse_side_transfer_worker_lane_idx(&self_id).ok_or_else(|| {
            KvError::Api(ApiError::Unknown {
                detail: format!(
                    "side-transfer worker missing '__side_<idx>' suffix in id: {}",
                    self_id
                ),
            })
        })
    }

    async fn resolve_remote_side_transfer_target(
        &self,
        owner_peer_id: &str,
        lane_idx: u16,
    ) -> Option<(NodeIDString, u64)> {
        tracing::info!(
            "resolving remote side-transfer target: owner={} lane_idx={}",
            owner_peer_id,
            lane_idx
        );
        let resp = match self
            .inner()
            .rpc_caller_resolve_side_transfer_lane
            .call(
                self.inner().view.p2p_module(),
                owner_peer_id.to_string().into(),
                MsgPack {
                    serialize_part: ResolveSideTransferLaneReq { lane_idx },
                    raw_bytes: Vec::new(),
                },
                Some(Duration::from_secs(
                    SIDE_TRANSFER_TARGET_RESOLVE_TIMEOUT_SECS,
                )),
                0,
            )
            .await
        {
            Ok(resp) => resp,
            Err(err) => {
                tracing::warn!(
                    "resolve_remote_side_transfer_target rpc failed: owner={} lane_idx={} err={:?}",
                    owner_peer_id,
                    lane_idx,
                    err
                );
                return None;
            }
        };
        if resp.serialize_part.error_code != OK {
            tracing::warn!(
                "resolve_remote_side_transfer_target returned error: owner={} lane_idx={} error_code={} error_json={}",
                owner_peer_id,
                lane_idx,
                resp.serialize_part.error_code,
                resp.serialize_part.error_json
            );
            return None;
        }
        let side_id = match resp.serialize_part.side_id {
            Some(side_id) => side_id,
            None => {
                tracing::info!(
                    "resolve_remote_side_transfer_target returned no side_id: owner={} lane_idx={} target_base_addr={:?}",
                    owner_peer_id,
                    lane_idx,
                    resp.serialize_part.target_base_addr
                );
                return None;
            }
        };
        let target_base_addr = match resp.serialize_part.target_base_addr {
            Some(target_base_addr) => target_base_addr,
            None => {
                tracing::info!(
                    "resolve_remote_side_transfer_target returned no target_base_addr: owner={} lane_idx={} side_id={}",
                    owner_peer_id,
                    lane_idx,
                    side_id
                );
                return None;
            }
        };
        tracing::info!(
            "resolved remote side-transfer target: owner={} lane_idx={} side_id={} target_base_addr={:#x}",
            owner_peer_id,
            lane_idx,
            side_id,
            target_base_addr
        );
        Some((side_id.into(), target_base_addr))
    }

    fn side_transfer_unsupported(op: &'static str) -> KvError {
        KvError::Api(ApiError::Unknown {
            detail: format!("{op} is unsupported on side-transfer worker"),
        })
    }

    async fn external_put_transfer_end_side_worker(
        &self,
        req: ExternalPutTransferEndReq,
    ) -> KvResult<ExternalPutTransferEndResp> {
        let inner = self.inner();
        let total_started_at = Instant::now();

        let Some(put_id) = req.put_id else {
            let err = KvError::Unreachable(
                crate::rpcresp_kvresult_convert::msg_and_error::UnreachableError::RpcDecodeError {
                    rpc_input_json: format!("missing put_id; key={}", req.key),
                },
            );
            return Ok(ExternalPutTransferEndResp::from_error(&err));
        };

        let owner_id = self.owner_node_id_for_side_transfer()?;
        let lane_idx = self.side_transfer_worker_lane_idx()?;
        let mut target_peer_id = req.peer_id.clone().map(Into::into);
        let mut target_base_addr = req.target_base_addr;
        if let Some(owner_peer_id) = req.peer_id.as_deref() {
            if let Some((side_peer_id, side_target_base_addr)) = self
                .resolve_remote_side_transfer_target(owner_peer_id, lane_idx)
                .await
            {
                tracing::info!(
                    "side-transfer lane resolved: source_lane={} owner_peer={} target_side={} target_base_addr={:#x}",
                    lane_idx,
                    owner_peer_id,
                    side_peer_id,
                    side_target_base_addr
                );
                target_peer_id = Some(side_peer_id);
                target_base_addr = Some(side_target_base_addr);
            }
        }
        let transfer_peer_id_for_trace = target_peer_id.as_ref().map(|peer| peer.to_string());

        let transfer_started_at = Instant::now();
        if let Err(e) = inner
            .put_transfer(
                &req.key,
                put_id,
                req.src_offset,
                req.target_offset,
                req.len,
                target_peer_id,
                target_base_addr,
            )
            .await
        {
            let revoke_req = MsgPack {
                serialize_part: ExternalPutRevokeReq {
                    key: req.key.clone(),
                    put_id: Some(put_id),
                    started_time: req.started_time,
                },
                raw_bytes: Vec::new(),
            };
            if let Err(revoke_err) = inner
                .rpc_caller_external_put_revoke
                .call(
                    inner.view.p2p_module(),
                    owner_id.clone().into(),
                    revoke_req,
                    Some(Duration::from_secs(SIDE_TRANSFER_OWNER_RPC_TIMEOUT_SECS)),
                    0,
                )
                .await
            {
                tracing::warn!(
                    "side-transfer revoke RPC failed after transfer error: owner={} key={} put_id=({},{}) err={:?}",
                    owner_id,
                    req.key,
                    put_id.0,
                    put_id.1,
                    revoke_err
                );
            }
            return Ok(ExternalPutTransferEndResp::from_error(&e));
        }
        let put_transfer_total_us = duration_to_i64_us(transfer_started_at.elapsed());

        let commit_req = MsgPack {
            serialize_part: ExternalPutCommitReq {
                key: req.key.clone(),
                len: req.len,
                src_offset: req.src_offset,
                remote_target: req
                    .peer_id
                    .as_deref()
                    .is_some_and(|peer| peer != owner_id.as_str()),
                put_id: Some(put_id),
                lease_id: req.lease_id,
                started_time: req.started_time,
                test_observe_put_phases: req.test_observe_put_phases,
            },
            raw_bytes: Vec::new(),
        };
        let commit_resp = inner
            .rpc_caller_external_put_commit
            .call(
                inner.view.p2p_module(),
                owner_id.into(),
                commit_req,
                Some(Duration::from_secs(SIDE_TRANSFER_OWNER_RPC_TIMEOUT_SECS)),
                0,
            )
            .await
            .map_err(KvError::from)?;
        commit_resp.serialize_part.clone().to_result()?;

        let mut trace = req.test_observe_put_phases.then(TestPutPhaseTrace::default);
        if let Some(trace_ref) = trace.as_mut() {
            trace_ref.owner_external_put_transfer_end_total_us =
                duration_to_i64_us(total_started_at.elapsed());
            trace_ref.owner_put_transfer_total_us = put_transfer_total_us;
            trace_ref.owner_put_transfer_peer_id = transfer_peer_id_for_trace;
            if let Some(commit_trace) = commit_resp.serialize_part.test_put_phase_trace.as_ref() {
                trace_ref.merge_from(commit_trace);
            }
        }

        Ok(ExternalPutTransferEndResp {
            error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
            error_json: String::new(),
            test_put_phase_trace: trace,
        })
    }
}

/// External API trait for managing external client requests
#[async_trait]
pub trait HandlerForExternalClient {
    /// Validate external's observed owner start_time (0 means skip validation for legacy callers)
    fn validate_requester_owner_status_updated(&self, started_time: i64) -> KvResult<()>;
    async fn external_get(&self, req: ExternalGetReq) -> KvResult<ExternalGetResp>;
    async fn external_batch_get(&self, req: ExternalBatchGetReq) -> KvResult<ExternalBatchGetResp>;
    async fn external_batch_get_local_probe(
        &self,
        req: ExternalBatchGetLocalProbeReq,
    ) -> KvResult<ExternalBatchGetLocalProbeResp>;
    async fn external_batch_get_start(
        &self,
        req: ExternalBatchGetStartReq,
    ) -> KvResult<ExternalBatchGetStartResp>;
    async fn external_batch_get_transfer(
        &self,
        req: ExternalBatchGetTransferReq,
    ) -> KvResult<ExternalBatchGetTransferResp>;
    async fn external_batch_get_cancel(
        &self,
        req: ExternalBatchGetCancelReq,
    ) -> KvResult<ExternalBatchGetCancelResp>;
    async fn external_execute_planned_get(
        &self,
        req: ExternalExecutePlannedGetReq,
    ) -> KvResult<ExternalExecutePlannedGetResp>;
    async fn external_put_start(&self, req: ExternalPutStartReq) -> KvResult<ExternalPutStartResp>;
    async fn external_batch_put_start(
        &self,
        req: ExternalBatchPutStartReq,
    ) -> KvResult<ExternalBatchPutStartResp>;
    // deprecated: transfer merged into external_put_transfer_end
    async fn external_put_transfer_end(
        &self,
        req: ExternalPutTransferEndReq,
    ) -> KvResult<ExternalPutTransferEndResp>;
    async fn external_batch_put_transfer_end(
        &self,
        req: ExternalBatchPutTransferEndReq,
    ) -> KvResult<ExternalBatchPutTransferEndResp>;
    async fn external_put_commit(
        &self,
        req: ExternalPutCommitReq,
    ) -> KvResult<ExternalPutCommitResp>;
    async fn external_batch_put_commit(
        &self,
        req: ExternalBatchPutCommitReq,
    ) -> KvResult<ExternalBatchPutCommitResp>;
    async fn external_put_revoke(
        &self,
        req: ExternalPutRevokeReq,
    ) -> KvResult<ExternalPutRevokeResp>;
    async fn external_delete(&self, req: ExternalDeleteReq) -> KvResult<ExternalDeleteResp>;
    async fn external_is_exist(&self, req: ExternalIsExistReq) -> KvResult<ExternalIsExistResp>;
    async fn external_batch_is_exist(
        &self,
        req: ExternalBatchIsExistReq,
    ) -> KvResult<ExternalBatchIsExistResp>;
    async fn external_observability_snapshot(
        &self,
        req: ExternalObservabilitySnapshotReq,
    ) -> KvResult<ExternalObservabilitySnapshotResp>;
}

/// Handle external get request
#[async_trait]
impl HandlerForExternalClient for ClientKvApi {
    fn validate_requester_owner_status_updated(&self, started_time: i64) -> KvResult<()> {
        // Validate owner start time if provided (non-zero)
        // only when the requestor has the right owner start time, address computation will be right
        let expected = self.expected_owner_start_time_for_external_path();
        if started_time != 0 && started_time != expected {
            return Err(KvError::Api(ApiError::OwnerStartTimeMismatch {
                expected,
                got: started_time,
            }));
        }
        Ok(())
    }
    async fn external_get(&self, req: ExternalGetReq) -> KvResult<ExternalGetResp> {
        if self.is_side_transfer_worker() {
            return Err(Self::side_transfer_unsupported("external_get"));
        }
        let inner = self.inner();

        self.validate_requester_owner_status_updated(req.started_time)?;

        // dummy implementation, tmp owner user memholder for temporary holding to make self memholder
        let (memholder, _) = match inner.get(&req.key).await? {
            Some(holder) => holder,
            None => {
                return Ok(ExternalGetResp {
                    external_memholder_info: None,
                    error_code:
                        crate::rpcresp_kvresult_convert::msg_and_error::codes_api::API_KEY_NOT_FOUND,
                    error_json: String::from("Key not found"),
                });
            }
        };
        let external_memholder_info =
            inner.install_external_get_holding(&req.req_node_id, memholder.memory_info());
        Ok(ExternalGetResp {
            external_memholder_info: Some(external_memholder_info),
            error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
            error_json: String::new(),
        })
    }

    async fn external_batch_get(&self, req: ExternalBatchGetReq) -> KvResult<ExternalBatchGetResp> {
        if self.is_side_transfer_worker() {
            return Err(Self::side_transfer_unsupported("external_batch_get"));
        }
        let inner = self.inner();

        self.validate_requester_owner_status_updated(req.started_time)?;

        let batch_results = inner.batch_get(req.keys, req.transfer_concurrency).await?;
        let mut items = Vec::with_capacity(batch_results.len());
        for item_result in batch_results {
            match item_result {
                Ok(Some((memholder, _))) => {
                    let external_memholder_info = inner.install_external_get_holding(
                        &req.req_node_id,
                        memholder.memory_info(),
                    );
                    items.push(ExternalBatchGetItemResp {
                        error_code: OK,
                        error_json: String::new(),
                        external_memholder_info: Some(external_memholder_info),
                    });
                }
                Ok(None) => items.push(ExternalBatchGetItemResp {
                    error_code:
                        crate::rpcresp_kvresult_convert::msg_and_error::codes_api::API_KEY_NOT_FOUND,
                    error_json: "Key not found".to_string(),
                    external_memholder_info: None,
                }),
                Err(err) => items.push(ExternalBatchGetItemResp {
                    external_memholder_info: None,
                    ..ExternalBatchGetItemResp::from_error(&err)
                }),
            }
        }

        Ok(ExternalBatchGetResp {
            items,
            error_code: OK,
            error_json: String::new(),
        })
    }

    async fn external_batch_get_local_probe(
        &self,
        req: ExternalBatchGetLocalProbeReq,
    ) -> KvResult<ExternalBatchGetLocalProbeResp> {
        if self.is_side_transfer_worker() {
            return Err(Self::side_transfer_unsupported(
                "external_batch_get_local_probe",
            ));
        }
        if req.keys.is_empty() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "external_batch_get_local_probe requires at least one key".to_string(),
            }));
        }
        if req.plan_handle == 0 {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "external_batch_get_local_probe requires a non-zero plan_handle"
                    .to_string(),
            }));
        }
        self.validate_requester_owner_status_updated(req.started_time)?;
        let inner = self.inner();
        let identity = (req.req_node_id.clone(), req.started_time, req.plan_handle);
        let operation_lock = inner
            .external_get_local_probe_locks
            .get_lock(identity.clone());
        let _operation_guard = operation_lock.lock().await;
        if let Some((completed_keys, completed)) = inner
            .completed_external_get_local_probes
            .get(&identity)
            .await
        {
            if completed_keys != req.keys {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "external_batch_get_local_probe operation identity was reused with different keys: plan_handle={}",
                        req.plan_handle
                    ),
                }));
            }
            return Ok(completed);
        }
        let mut items = Vec::with_capacity(req.keys.len());
        let mut hot_touches = Vec::new();
        for key in &req.keys {
            let (item, hot_touch) = {
                let controls = inner.owner_key_control.lock_key(key);
                if controls
                    .get(key)
                    .is_some_and(|state| state.local_access_fenced())
                {
                    (None, None)
                } else if let Some(memory_info) = inner.local_visible_mem_holder_unfenced(key) {
                    let hot_touch = inner.get_cached_info.get(key).and_then(|cached| {
                        Arc::ptr_eq(&cached.mem_holder, &memory_info).then_some((
                            (cached.put_time_ms, cached.put_version),
                            memory_info.clone(),
                        ))
                    });
                    // Install the holder under the same per-key fence used for
                    // local visibility. Reclaim cannot slip between the probe
                    // decision and the pin that owns this exact backing.
                    let info = inner.install_external_get_holding(&req.req_node_id, memory_info);
                    (Some(info), hot_touch)
                } else {
                    (None, None)
                }
            };
            if let Some((put_id, memory_info)) = hot_touch {
                hot_touches.push((key.clone(), put_id, memory_info));
            }
            items.push(item);
        }
        for (key, put_id, memory_info) in hot_touches {
            inner.owner_hot_touch_or_promote(&key, put_id, &memory_info);
        }
        let local_items = items.iter().filter(|item| item.is_some()).count();
        let remote_items = items.len().saturating_sub(local_items);
        inner
            .planned_get_counters
            .local_probe_batches
            .fetch_add(1, Ordering::Relaxed);
        inner
            .planned_get_counters
            .local_probe_items
            .fetch_add(items.len() as u64, Ordering::Relaxed);
        inner
            .planned_get_counters
            .local_probe_local_items
            .fetch_add(local_items as u64, Ordering::Relaxed);
        inner
            .planned_get_counters
            .local_probe_remote_items
            .fetch_add(remote_items as u64, Ordering::Relaxed);
        tracing::debug!(
            requested = req.keys.len(),
            local = local_items,
            remote = remote_items,
            "external Get owner-local probe"
        );
        let response = ExternalBatchGetLocalProbeResp {
            items,
            error_code: OK,
            error_json: String::new(),
        };
        inner
            .completed_external_get_local_probes
            .insert(identity, (req.keys, response.clone()))
            .await;
        Ok(response)
    }

    async fn external_batch_get_start(
        &self,
        req: ExternalBatchGetStartReq,
    ) -> KvResult<ExternalBatchGetStartResp> {
        if self.is_side_transfer_worker() {
            return Err(Self::side_transfer_unsupported("external_batch_get_start"));
        }
        if req.keys.is_empty() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "external_batch_get_start requires at least one key".to_string(),
            }));
        }
        let inner = self.inner();
        self.validate_requester_owner_status_updated(req.started_time)?;
        let observe_started_at = Instant::now();
        let requested_len = req.keys.len();

        let group_lens =
            normalize_external_get_start_group_lens(req.keys.len(), req.atomic_group_lens.clone())?;
        let (prefix, transfer_keys, transfer_items) =
            prepare_external_get_batch_plan(inner, &req, &group_lens).await?;
        let phase_snapshot = observe_external_get_consume_phases(&transfer_items, Instant::now());
        let handle = inner
            .next_external_get_start_handle
            .fetch_add(1, Ordering::Relaxed);
        let inline_memory_infos = (transfer_items.len() == req.keys.len())
            .then(|| collect_all_local_external_get_start_infos(&transfer_items))
            .flatten();
        let transfer_plan = if let Some(memory_infos) = inline_memory_infos {
            assert_eq!(
                memory_infos.len(),
                req.keys.len(),
                "inline local get_start plan must cover the full request"
            );
            let items = memory_infos
                .into_iter()
                .map(|memory_info| ExternalBatchGetItemResp {
                    error_code: OK,
                    error_json: String::new(),
                    external_memholder_info: Some(
                        inner.install_external_get_holding(&req.req_node_id, memory_info),
                    ),
                })
                .collect();
            ExternalBatchGetStartTransferPlan::InlineLocal { items }
        } else {
            inner.external_get_start_registry.insert(
                handle,
                ExternalGetStartEntry {
                    req_node_id: req.req_node_id.clone(),
                    requester_node_start_time: inner
                        .view
                        .cluster_manager()
                        .get_member_info_cached(&req.req_node_id)
                        .map(|member| member.node_start_time),
                    keys: transfer_keys,
                    items: transfer_items,
                    atomic_group_lens: group_lens,
                    created_at: Instant::now(),
                },
            );
            ExternalBatchGetStartTransferPlan::OwnerRpc
        };
        let inline_local = matches!(
            &transfer_plan,
            ExternalBatchGetStartTransferPlan::InlineLocal { .. }
        );
        tracing::info!(
            "external Get start lifecycle: handle={} requested={} raw_prefix={} transferable={} inline_local={} local={} starting={} started={} finishing={} revoking={} ready={} failed={} terminal_before_return={} pending_before_return={} terminal_age_mean_us={} terminal_age_max_us={} total_us={}",
            handle,
            requested_len,
            prefix.raw_prefix_hit_len,
            prefix.transferable_len,
            inline_local,
            phase_snapshot.local,
            phase_snapshot.starting,
            phase_snapshot.started,
            phase_snapshot.finishing,
            phase_snapshot.revoking,
            phase_snapshot.ready,
            phase_snapshot.failed,
            phase_snapshot.terminal_before_consume(),
            phase_snapshot.pending_before_consume(),
            phase_snapshot.terminal_age_mean_us(),
            phase_snapshot.terminal_age_max_us,
            duration_to_i64_us(observe_started_at.elapsed()),
        );

        Ok(ExternalBatchGetStartResp {
            error_code: OK,
            error_json: String::new(),
            handle,
            raw_prefix_hit_len: prefix.raw_prefix_hit_len,
            transfer_plan,
        })
    }

    async fn external_batch_get_transfer(
        &self,
        req: ExternalBatchGetTransferReq,
    ) -> KvResult<ExternalBatchGetTransferResp> {
        if self.is_side_transfer_worker() {
            return Err(Self::side_transfer_unsupported(
                "external_batch_get_transfer",
            ));
        }
        let inner = self.inner();
        self.validate_requester_owner_status_updated(req.started_time)?;
        let consume_started_at = Instant::now();

        let Some((_handle, mut entry)) = inner.external_get_start_registry.remove(&req.handle)
        else {
            return Err(KvError::Api(ApiError::KeyNotFound {
                key: format!("external_get_start_handle:{}", req.handle),
            }));
        };
        if entry.req_node_id != req.req_node_id {
            tracing::warn!(
                "external_batch_get_transfer req_node_id mismatch: handle={} expected={} got={}",
                req.handle,
                entry.req_node_id,
                req.req_node_id
            );
        }
        if let Err(err) = validate_external_get_consume_prefix(
            req.consume_prefix_len,
            entry.keys.len(),
            &entry.atomic_group_lens,
        ) {
            inner.external_get_start_registry.insert(req.handle, entry);
            return Err(err);
        }
        assert_eq!(
            entry.keys.len(),
            entry.items.len(),
            "external get-start registry keys and items must stay aligned"
        );
        let available_prefix_len = entry.keys.len();
        let handle_age_us = duration_to_i64_us(entry.created_at.elapsed());
        let phase_snapshot = observe_external_get_consume_phases(
            &entry.items[..req.consume_prefix_len],
            Instant::now(),
        );
        let tail_items = entry.items.split_off(req.consume_prefix_len);
        entry.keys.truncate(req.consume_prefix_len);
        if !tail_items.is_empty() {
            tracing::info!(
                "external_batch_get_transfer consumed prefix and released owner tail: handle={} available={} consumed={} released_tail={}",
                req.handle,
                available_prefix_len,
                req.consume_prefix_len,
                tail_items.len()
            );
        }
        drop(tail_items);
        let keys = entry.keys;
        let finish_wait_started_at = Instant::now();
        let transfer_results_result =
            finish_external_get_start_transfer(inner.view.clone_view(), entry.items, 0).await;
        let finish_wait_us = duration_to_i64_us(finish_wait_started_at.elapsed());
        let transfer_results = match transfer_results_result {
            Ok(results) => results,
            Err(err) => {
                tracing::info!(
                    "external Get consume lifecycle: handle={} available={} consumed={} released_tail={} handle_age_us={} local_before={} starting_before={} started_before={} finishing_before={} revoking_before={} ready_before={} failed_before={} terminal_before={} pending_before={} terminal_age_mean_us={} terminal_age_max_us={} finish_wait_us={} hits=0 misses=0 errors={} install_us=0 total_us={} outcome=error",
                    req.handle,
                    available_prefix_len,
                    req.consume_prefix_len,
                    available_prefix_len.saturating_sub(req.consume_prefix_len),
                    handle_age_us,
                    phase_snapshot.local,
                    phase_snapshot.starting,
                    phase_snapshot.started,
                    phase_snapshot.finishing,
                    phase_snapshot.revoking,
                    phase_snapshot.ready,
                    phase_snapshot.failed,
                    phase_snapshot.terminal_before_consume(),
                    phase_snapshot.pending_before_consume(),
                    phase_snapshot.terminal_age_mean_us(),
                    phase_snapshot.terminal_age_max_us,
                    finish_wait_us,
                    req.consume_prefix_len,
                    duration_to_i64_us(consume_started_at.elapsed()),
                );
                return Err(err);
            }
        };
        if transfer_results.len() != keys.len() {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "external_batch_get_transfer result length mismatch: expected={} got={}",
                    keys.len(),
                    transfer_results.len()
                ),
            }));
        }
        let install_started_at = Instant::now();
        let mut items = Vec::with_capacity(transfer_results.len());
        let mut hits = 0usize;
        let mut misses = 0usize;
        let mut errors = 0usize;
        for (key, item_result) in keys.iter().zip(transfer_results) {
            match item_result {
                Ok(Some((memholder, _))) => {
                    hits = hits.saturating_add(1);
                    let external_memholder_info = inner
                        .install_external_get_holding(&req.req_node_id, memholder.memory_info());
                    items.push(ExternalBatchGetItemResp {
                        error_code: OK,
                        error_json: String::new(),
                        external_memholder_info: Some(external_memholder_info),
                    });
                }
                Ok(None) => {
                    misses = misses.saturating_add(1);
                    items.push(ExternalBatchGetItemResp {
                        error_code: codes_api::API_KEY_NOT_FOUND,
                        error_json: format!("Key not found: {}", key),
                        external_memholder_info: None,
                    });
                }
                Err(err) => {
                    errors = errors.saturating_add(1);
                    items.push(ExternalBatchGetItemResp {
                        external_memholder_info: None,
                        ..ExternalBatchGetItemResp::from_error(&err)
                    });
                }
            }
        }
        let install_us = duration_to_i64_us(install_started_at.elapsed());
        tracing::info!(
            "external Get consume lifecycle: handle={} available={} consumed={} released_tail={} handle_age_us={} local_before={} starting_before={} started_before={} finishing_before={} revoking_before={} ready_before={} failed_before={} terminal_before={} pending_before={} terminal_age_mean_us={} terminal_age_max_us={} finish_wait_us={} hits={} misses={} errors={} install_us={} total_us={} outcome=ok",
            req.handle,
            available_prefix_len,
            req.consume_prefix_len,
            available_prefix_len.saturating_sub(req.consume_prefix_len),
            handle_age_us,
            phase_snapshot.local,
            phase_snapshot.starting,
            phase_snapshot.started,
            phase_snapshot.finishing,
            phase_snapshot.revoking,
            phase_snapshot.ready,
            phase_snapshot.failed,
            phase_snapshot.terminal_before_consume(),
            phase_snapshot.pending_before_consume(),
            phase_snapshot.terminal_age_mean_us(),
            phase_snapshot.terminal_age_max_us,
            finish_wait_us,
            hits,
            misses,
            errors,
            install_us,
            duration_to_i64_us(consume_started_at.elapsed()),
        );
        Ok(ExternalBatchGetTransferResp {
            items,
            error_code: OK,
            error_json: String::new(),
        })
    }

    async fn external_execute_planned_get(
        &self,
        req: ExternalExecutePlannedGetReq,
    ) -> KvResult<ExternalExecutePlannedGetResp> {
        if self.is_side_transfer_worker() {
            return Err(Self::side_transfer_unsupported(
                "external_execute_planned_get",
            ));
        }
        if req.items.is_empty() || req.transfer_concurrency == 0 {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "external_execute_planned_get requires non-empty items and positive concurrency: items={} concurrency={}",
                    req.items.len(),
                    req.transfer_concurrency
                ),
            }));
        }
        let inner = self.inner();
        self.validate_requester_owner_status_updated(req.started_time)?;
        let identity = (req.req_node_id.clone(), req.started_time, req.plan_handle);
        let operation_lock = inner
            .planned_external_get_execute_locks
            .get_lock(identity.clone());
        let _operation_guard = operation_lock.lock().await;
        if let Some(completed) = inner
            .completed_planned_external_get_executes
            .get(&identity)
            .await
        {
            return Ok(completed);
        }

        let mut requested_by_key = HashMap::with_capacity(req.items.len());
        for item in &req.items {
            if item.key.is_empty()
                || requested_by_key
                    .insert(item.key.clone(), item.get_id)
                    .is_some()
            {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "external_execute_planned_get requires unique non-empty keys: plan_handle={}",
                        req.plan_handle
                    ),
                }));
            }
        }
        let keys = req
            .items
            .iter()
            .map(|item| item.key.clone())
            .collect::<Vec<_>>();
        let (mut owner_items, leaders) = plan_external_get_key_items(inner, &keys).await;
        let local_items = owner_items
            .iter()
            .filter(|item| matches!(item, ExternalGetStartOwnerItem::Local { .. }))
            .count();
        let leader_items = leaders.len();
        let follower_items = owner_items
            .len()
            .saturating_sub(local_items)
            .saturating_sub(leader_items);
        inner
            .planned_get_counters
            .batches
            .fetch_add(1, Ordering::Relaxed);
        inner
            .planned_get_counters
            .local_items
            .fetch_add(local_items as u64, Ordering::Relaxed);
        inner
            .planned_get_counters
            .leader_items
            .fetch_add(leader_items as u64, Ordering::Relaxed);
        inner
            .planned_get_counters
            .follower_items
            .fetch_add(follower_items as u64, Ordering::Relaxed);
        tracing::info!(
            plan_handle = req.plan_handle,
            items = owner_items.len(),
            local_items,
            leader_items,
            follower_items,
            "planned CPU Get owner classification"
        );
        let leader_get_ids = leaders
            .iter()
            .map(|leader| {
                requested_by_key
                    .get(&leader.key)
                    .copied()
                    .expect("planned leader key must come from the request")
            })
            .collect::<Vec<_>>();
        let leader_id_set = leader_get_ids
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        let mut cleanup = req
            .items
            .iter()
            .filter(|item| !leader_id_set.contains(&item.get_id))
            .map(|item| StartedGetRevokeCleanup {
                get_id: item.get_id,
                prepared_target: None,
            })
            .collect::<Vec<_>>();

        let bound_items =
            match batch_get_bind_with_local_reserve_targets(inner, &leader_get_ids).await {
                Ok(items) => items,
                Err(err) => {
                    for leader in &leaders {
                        publish_external_get_key_failed(inner, leader, &err);
                    }
                    cleanup.extend(leader_get_ids.into_iter().map(|get_id| {
                        StartedGetRevokeCleanup {
                            get_id,
                            prepared_target: None,
                        }
                    }));
                    finish_started_get_revoke_cleanup(
                        inner,
                        cleanup,
                        "planned external Get bind failure",
                    )
                    .await;
                    let response = ExternalExecutePlannedGetResp {
                        items: Vec::new(),
                        error_code: err.code(),
                        error_json: err.to_json(),
                    };
                    inner
                        .completed_planned_external_get_executes
                        .insert(identity, response.clone())
                        .await;
                    return Ok(response);
                }
            };
        assert_eq!(leaders.len(), bound_items.len());
        for (leader, item) in leaders.iter().zip(bound_items) {
            if item.error_code != OK {
                cleanup.push(StartedGetRevokeCleanup {
                    get_id: item.get_id,
                    prepared_target: None,
                });
            }
            publish_external_get_key_started(leader, item);
        }
        for item in &mut owner_items {
            decide_external_get_key_item(item, true);
        }

        // From this point on the owner has published Started for every leader,
        // so it must retain lifecycle ownership independently of the inbound
        // RPC future.  P2P timeout/cancellation drops the handler future; if
        // finish lived on that future, the per-key marker would remain in
        // Finishing forever and every replay would only become a follower.
        // The registered owner task is process-scoped and therefore continues
        // both transfer/Done and unused-operation cleanup after caller loss.
        let spawn_view = inner.view.clone_view();
        let worker_view = spawn_view.clone();
        let finish_leaders = leaders;
        let transfer_concurrency = req.transfer_concurrency;
        let plan_handle = req.plan_handle;
        spawn_view.spawn(
            format!("planned_external_get_finish_{plan_handle}"),
            async move {
                let finish_view = worker_view.clone();
                let cleanup_view = worker_view;
                let finish = finish_external_get_key_leaders(
                    finish_view,
                    finish_leaders,
                    transfer_concurrency,
                );
                let cleanup_future = async move {
                    finish_started_get_revoke_cleanup(
                        cleanup_view.client_kv_api().inner(),
                        cleanup,
                        "planned external Get unused operation",
                    )
                    .await;
                };
                tokio::join!(finish, cleanup_future);
            },
        );

        let transfer_results =
            finish_external_get_start_transfer(inner.view.clone_view(), owner_items, 0).await?;
        if transfer_results.len() != keys.len() {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "planned external Get result length mismatch: expected={} got={}",
                    keys.len(),
                    transfer_results.len()
                ),
            }));
        }
        let mut items = Vec::with_capacity(keys.len());
        for (key, result) in keys.iter().zip(transfer_results) {
            match result {
                Ok(Some((holder, _))) => items.push(ExternalBatchGetItemResp {
                    error_code: OK,
                    error_json: String::new(),
                    external_memholder_info: Some(
                        inner.install_external_get_holding(&req.req_node_id, holder.memory_info()),
                    ),
                }),
                Ok(None) => items.push(ExternalBatchGetItemResp {
                    error_code: codes_api::API_KEY_NOT_FOUND,
                    error_json: format!("Key not found: {key}"),
                    external_memholder_info: None,
                }),
                Err(err) => items.push(ExternalBatchGetItemResp {
                    external_memholder_info: None,
                    ..ExternalBatchGetItemResp::from_error(&err)
                }),
            }
        }
        let response = ExternalExecutePlannedGetResp {
            items,
            error_code: OK,
            error_json: String::new(),
        };
        inner
            .completed_planned_external_get_executes
            .insert(identity, response.clone())
            .await;
        Ok(response)
    }

    async fn external_batch_get_cancel(
        &self,
        req: ExternalBatchGetCancelReq,
    ) -> KvResult<ExternalBatchGetCancelResp> {
        if self.is_side_transfer_worker() {
            return Err(Self::side_transfer_unsupported("external_batch_get_cancel"));
        }
        let inner = self.inner();
        self.validate_requester_owner_status_updated(req.started_time)?;

        match req.transfer_plan {
            ExternalBatchGetCancelPlan::OwnerRpc => {
                if let Some((_handle, entry)) =
                    inner.external_get_start_registry.remove(&req.handle)
                {
                    if entry.req_node_id != req.req_node_id {
                        tracing::warn!(
                            "external_batch_get_cancel req_node_id mismatch: handle={} expected={} got={}",
                            req.handle,
                            entry.req_node_id,
                            req.req_node_id
                        );
                    }
                    drop(entry);
                }
            }
            ExternalBatchGetCancelPlan::InlineLocal { holder_ids } => {
                for holder_id in holder_ids {
                    let _ = inner
                        .external_get_holding
                        .remove(&NodeHolderKey::new(req.req_node_id.clone(), holder_id));
                }
            }
        }
        Ok(ExternalBatchGetCancelResp {
            error_code: OK,
            error_json: String::new(),
        })
    }

    /// Handle external put start request: allocate with native KV op and return offsets.
    /// For remote targets, return a local staging offset (no `peer_id` exposed); owner records
    /// remote context internally and completes transfer during `external_put_transfer_end`.
    async fn external_put_start(&self, req: ExternalPutStartReq) -> KvResult<ExternalPutStartResp> {
        let inner = self.inner();
        let started_at = Instant::now();

        self.validate_requester_owner_status_updated(req.started_time)?;
        // Register before the master PutStart RPC. Reclaim Prepare checks this
        // counter under the same per-key key-control shard, closing the old gap
        // between master admission and insertion into external_pending_puts.
        let pending_fence = inner.acquire_external_pending_put_fence(&req.key)?;
        let mut pending_fence_claim =
            ExternalPutStartFenceClaim::new(inner.view.clone_view(), pending_fence);

        let put_start_started_at = Instant::now();
        let source_node_id = if self.is_side_transfer_worker() {
            Some(self.owner_node_id_for_side_transfer()?.into())
        } else {
            None
        };
        let (put_start_resp, master_put_start_rpc_us) = inner
            .put_start_with_source_node(
                &req.key,
                req.len as u32,
                req.reject_if_inflight_same_key,
                req.reject_if_exist_same_key,
                req.make_replica_task,
                req.preferred_sub_cluster.as_deref(),
                source_node_id,
            )
            .await
            .map_err(|e| {
                tracing::error!("Failed to start put operation: {}", e);
                e
            })?;
        // Ensure master responded OK before using returned addresses
        if let Err(err) = crate::rpcresp_kvresult_convert::try_from_code(
            put_start_resp.error_code,
            put_start_resp.error_json.clone(),
        ) {
            // An application response proves the master did not admit this Put;
            // no uncertainty quarantine is required.
            pending_fence_claim.release_after_definite_response();
            return Err(err);
        }
        tracing::debug!(
            "handle external put start for key: {}, len: {}",
            req.key,
            req.len
        );

        // Master-owner returns absolute addresses due to Mooncake; use base-addrs from RPC to compute offsets
        let src_offset = put_start_resp.src_addr - put_start_resp.src_base_addr;
        let is_local_target =
            &*put_start_resp.node_id == &*inner.view.cluster_manager().get_self_info().id;
        // Compute the offset that the external should write to:
        // - If target is local, return the local target offset
        // - If target is remote, return a local staging offset (src_offset) and record remote ctx internally
        let target_offset = if is_local_target {
            put_start_resp.target_addr - put_start_resp.target_base_addr
        } else {
            // Remote target: external still writes into owner's shared memory (src_offset).
            src_offset
        };
        let remote_offset = put_start_resp.target_addr - put_start_resp.target_base_addr;
        let replica_admitted = put_start_resp.replica_target.is_some();
        inner.external_pending_puts.insert(
            (
                req.key.clone(),
                put_start_resp.put_id.0,
                put_start_resp.put_id.1,
            ),
            ExternalPendingPutCtx {
                peer_id: if is_local_target {
                    None
                } else {
                    Some(put_start_resp.node_id.clone())
                },
                src_offset,
                target_base_addr: put_start_resp.target_base_addr,
                target_offset: if is_local_target {
                    target_offset
                } else {
                    remote_offset
                },
                len: req.len,
                make_replica_task: req.make_replica_task && replica_admitted,
                preferred_sub_cluster: req.preferred_sub_cluster.clone(),
                local_reserve_slot: None,
                local_reserve_slot_size: None,
                atomic_group: None,
                _pending_fence: pending_fence_claim.take_for_pending_context(),
            },
        );
        if !is_local_target {
            tracing::debug!(
                "external_put_start stash remote ctx: key={}, put_id=({},{}) peer_id={}, target_base={:#x}, target_off={:#x}, src_off(staging)={:#x}",
                req.key,
                put_start_resp.put_id.0,
                put_start_resp.put_id.1,
                put_start_resp.node_id,
                put_start_resp.target_base_addr,
                remote_offset,
                src_offset
            );
        }
        Ok(ExternalPutStartResp {
            error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
            src_offset,
            target_offset,
            transfer_target_offset: if is_local_target {
                None
            } else {
                Some(put_start_resp.target_addr - put_start_resp.target_base_addr)
            },
            // Expose peer_id only for owner to reconstruct abs target at transfer_end; external can ignore.
            peer_id: if is_local_target {
                None
            } else {
                Some(put_start_resp.node_id)
            },
            src_base_addr: put_start_resp.src_base_addr,
            target_base_addr: put_start_resp.target_base_addr,
            error_json: String::new(),
            put_id: Some(put_start_resp.put_id),
            test_put_phase_trace: if req.test_observe_put_phases {
                let owner_external_put_start_total_us = duration_to_i64_us(started_at.elapsed());
                let owner_put_start_total_us = duration_to_i64_us(put_start_started_at.elapsed());
                Some(TestPutPhaseTrace {
                    owner_external_put_start_total_us,
                    owner_put_start_total_us,
                    owner_master_put_start_rpc_us: master_put_start_rpc_us,
                    owner_master_put_start_server_us: put_start_resp.server_process_us,
                    ..Default::default()
                })
            } else {
                None
            },
        })
    }

    async fn external_batch_put_start(
        &self,
        req: ExternalBatchPutStartReq,
    ) -> KvResult<ExternalBatchPutStartResp> {
        if self.is_side_transfer_worker() {
            return Err(Self::side_transfer_unsupported("external_batch_put_start"));
        }
        let inner = self.inner();

        self.validate_requester_owner_status_updated(req.started_time)?;

        let atomic_group_lens = normalize_external_put_start_group_lens(
            req.items.len(),
            req.atomic_group_lens.clone(),
        )?;

        let Some(first_item) = req.items.first() else {
            return Ok(ExternalBatchPutStartResp {
                items: Vec::new(),
                error_code: OK,
                error_json: String::new(),
            });
        };
        let value_len = first_item.len;
        if req.items.iter().any(|item| item.len != value_len) {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "external_batch_put_start local-first requires uniform item len"
                    .to_string(),
            }));
        }

        let pending_fences = loop {
            let mut pending_fences = Vec::with_capacity(req.items.len());
            let mut wait_for = None;
            let mut wait_for_local_access = None;
            for item in &req.items {
                match inner.reserve_external_local_first_put_key(
                    &item.key,
                    item.reject_if_inflight_same_key,
                    item.reject_if_exist_same_key,
                ) {
                    Ok(ExternalLocalFirstPutKeyReservation::Leader(pending_fence)) => {
                        pending_fences.push(pending_fence);
                    }
                    Ok(ExternalLocalFirstPutKeyReservation::Wait(op)) => {
                        if atomic_group_lens.len() != 1 {
                            let err = KvError::Api(ApiError::KeyBeingWritten {
                                key: item.key.clone(),
                            });
                            return Ok(ExternalBatchPutStartResp {
                                items: req
                                    .items
                                    .iter()
                                    .map(|_| external_local_first_error_item(&err))
                                    .collect(),
                                error_code: OK,
                                error_json: String::new(),
                            });
                        }
                        wait_for = Some((item.key.clone(), op));
                        break;
                    }
                    Ok(ExternalLocalFirstPutKeyReservation::WaitForLocalAccess(completion)) => {
                        wait_for_local_access = Some((item.key.clone(), completion));
                        break;
                    }
                    Err(err) => {
                        let items = req
                            .items
                            .iter()
                            .map(|_| external_local_first_error_item(&err))
                            .collect();
                        return Ok(ExternalBatchPutStartResp {
                            items,
                            error_code: OK,
                            error_json: String::new(),
                        });
                    }
                }
            }
            if let Some((fenced_key, mut completion)) = wait_for_local_access {
                // A partial atomic_batch must not hold local-put fences while
                // the precise source-eviction/reclaim generation drains.  The
                // watch receiver was subscribed under the per-key lock, so a
                // rollback/finalize between this drop and changed().await is
                // observed without polling or a lost wakeup.
                drop(pending_fences);
                let wait_started_at = Instant::now();
                tracing::info!(
                    fenced_key,
                    items = req.items.len(),
                    "external local-first Put waiting for owner source/reclaim fence"
                );
                loop {
                    if *completion.borrow_and_update() {
                        break;
                    }
                    if completion.changed().await.is_err() {
                        // The state owner disappeared; re-evaluate under the
                        // key lock instead of treating channel close as success.
                        break;
                    }
                }
                tracing::info!(
                    fenced_key,
                    items = req.items.len(),
                    wait_us = duration_to_i64_us(wait_started_at.elapsed()),
                    "external local-first Put resumed after owner source/reclaim fence"
                );
                continue;
            }
            let Some((joined_key, op)) = wait_for else {
                break pending_fences;
            };

            // Never retain a partial atomic_batch while waiting for another
            // key. Two overlapping requests with different key order must not
            // each hold one leader fence and wait forever on the other.
            drop(pending_fences);
            match op.wait().await {
                ExternalPutKeyOutcome::Succeeded => {
                    tracing::info!(
                        joined_key,
                        items = req.items.len(),
                        "external local-first Put reused an inflight leader result"
                    );
                    let err = KvError::Api(ApiError::KeyAlreadyExists { key: joined_key });
                    return Ok(ExternalBatchPutStartResp {
                        items: req
                            .items
                            .iter()
                            .map(|_| external_local_first_error_item(&err))
                            .collect(),
                        error_code: OK,
                        error_json: String::new(),
                    });
                }
                ExternalPutKeyOutcome::Failed => continue,
                ExternalPutKeyOutcome::InFlight => {
                    unreachable!("shared Put wait must return a terminal outcome")
                }
            }
        };

        // Finish every fallible, purely logical derivation before claiming
        // physical slots.  The per-key fences above are RAII-owned, so any
        // error or cancellation before cache insertion releases both counters.
        let put_ids = req
            .items
            .iter()
            .map(|_| inner.next_external_local_first_put_id())
            .collect::<Vec<_>>();
        let keys_and_put_ids = req
            .items
            .iter()
            .map(|item| item.key.clone())
            .zip(put_ids.iter().copied())
            .collect::<Vec<_>>();
        let atomic_groups =
            build_put_atomic_group_assignments(&keys_and_put_ids, &atomic_group_lens)
                .map_err(|detail| KvError::Api(ApiError::InvalidArgument { detail }))?;

        let slot_lease = match inner
            .owner_claim_local_reserve_slot_lease(value_len, req.items.len())
            .await
        {
            Ok(slot_lease) => slot_lease,
            Err(err) => return Err(err),
        };
        let self_node_id = inner.view.cluster_manager().get_self_info().id.clone();
        let slot_size = slot_lease.slot_size;
        for (slot_ref, pending_fence) in slot_lease.slots.iter().zip(&pending_fences) {
            pending_fence.attach_local_slot_lease(OwnerLocalReserveSlotLease {
                value_len,
                slot_size,
                slots: vec![slot_ref.clone()],
            });
        }
        let mut items = Vec::with_capacity(req.items.len());
        for (idx, ((req_item, slot_ref), pending_fence)) in req
            .items
            .into_iter()
            .zip(slot_lease.slots.into_iter())
            .zip(pending_fences.into_iter())
            .enumerate()
        {
            let put_id = put_ids[idx];
            let src_offset = slot_ref.ptr.saturating_sub(slot_ref.base_addr);
            inner.external_pending_puts.insert(
                (req_item.key.clone(), put_id.0, put_id.1),
                ExternalPendingPutCtx {
                    peer_id: None,
                    src_offset,
                    target_base_addr: slot_ref.base_addr,
                    target_offset: src_offset,
                    len: req_item.len,
                    make_replica_task: req_item.make_replica_task,
                    preferred_sub_cluster: req_item.preferred_sub_cluster.clone(),
                    local_reserve_slot: Some(slot_ref.clone()),
                    local_reserve_slot_size: Some(slot_size),
                    atomic_group: atomic_groups[idx].clone(),
                    _pending_fence: pending_fence,
                },
            );
            tracing::debug!(
                "external_batch_put_start local-first: key={} put_id=({},{}) node_id={} base={:#x} offset={:#x} len={}",
                req_item.key,
                put_id.0,
                put_id.1,
                self_node_id,
                slot_ref.base_addr,
                src_offset,
                req_item.len
            );
            items.push(ExternalBatchPutStartItemResp {
                error_code: OK,
                src_offset,
                target_offset: src_offset,
                transfer_target_offset: None,
                peer_id: None,
                src_base_addr: slot_ref.base_addr,
                target_base_addr: slot_ref.base_addr,
                error_json: String::new(),
                put_id: Some(put_id),
            });
        }

        Ok(ExternalBatchPutStartResp {
            items,
            error_code: OK,
            error_json: String::new(),
        })
    }

    /// Handle external transfer+end request - transfer data then commit
    async fn external_put_transfer_end(
        &self,
        req: ExternalPutTransferEndReq,
    ) -> KvResult<ExternalPutTransferEndResp> {
        if self.is_side_transfer_worker() {
            return self.external_put_transfer_end_side_worker(req).await;
        }
        let inner = self.inner();
        let total_started_at = Instant::now();

        self.validate_requester_owner_status_updated(req.started_time)?;

        // Extract put_id early so we can revoke on transfer failure
        let Some(put_id) = req.put_id else {
            let err = crate::rpcresp_kvresult_convert::msg_and_error::KvError::Unreachable(
                crate::rpcresp_kvresult_convert::msg_and_error::UnreachableError::RpcDecodeError {
                    rpc_input_json: format!("missing put_id; key={}", req.key),
                },
            );
            return Ok(ExternalPutTransferEndResp::from_error(&err));
        };

        let pending_ctx = match require_external_pending_put_ctx(
            inner,
            "external_put_transfer_end",
            &req.key,
            put_id,
        ) {
            Ok(ctx) => ctx,
            Err(err) => {
                best_effort_revoke_missing_external_pending_ctx(
                    inner,
                    "external_put_transfer_end",
                    &req.key,
                    put_id,
                )
                .await;
                return Ok(ExternalPutTransferEndResp::from_error(&err));
            }
        };
        let admitted_replica_task = pending_ctx.make_replica_task;
        let self_node_id = inner.view.cluster_manager().get_self_info().id.clone();
        let req_remote_target = req
            .peer_id
            .as_deref()
            .is_some_and(|peer| peer != self_node_id.as_str());
        let has_remote_target = pending_ctx.peer_id.is_some();
        if req_remote_target != has_remote_target {
            let err = KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "external_put_transfer_end peer_id mismatches pending ctx: key={} put_id=({},{}) req_remote_target={} ctx_remote_target={}",
                    req.key, put_id.0, put_id.1, req_remote_target, has_remote_target
                ),
            });
            match inner.put_revoke(&req.key, put_id).await {
                Ok(_) => {
                    inner
                        .external_pending_puts
                        .invalidate(&(req.key.clone(), put_id.0, put_id.1))
                }
                Err(revoke_err) => tracing::warn!(
                    "external_put_transfer_end put_revoke failed after peer_id mismatch; pending fence retained: key={} put_id=({},{}) err={}",
                    req.key,
                    put_id.0,
                    put_id.1,
                    revoke_err
                ),
            }
            return Ok(ExternalPutTransferEndResp::from_error(&err));
        }
        let put_transfer_total_us = 0;
        let transfer_peer_id_for_trace = None;

        if inner.skip_put_end_commit_enabled() {
            inner
                .external_pending_puts
                .invalidate(&(req.key.clone(), put_id.0, put_id.1));
            tracing::warn!(
                "skip_put_end_commit test-only fast-path: returning success without external put_end; key={} put_id=({},{}) payload_len={}",
                req.key,
                put_id.0,
                put_id.1,
                req.len
            );
            return Ok(ExternalPutTransferEndResp {
                error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
                error_json: String::new(),
                test_put_phase_trace: if req.test_observe_put_phases {
                    Some(TestPutPhaseTrace {
                        owner_external_put_transfer_end_total_us: duration_to_i64_us(
                            total_started_at.elapsed(),
                        ),
                        owner_put_transfer_total_us: put_transfer_total_us,
                        owner_put_transfer_peer_id: transfer_peer_id_for_trace,
                        ..Default::default()
                    })
                } else {
                    None
                },
            });
        }

        let end_started_at = Instant::now();
        let publish_local_cache = true;
        let local_cache_publish = match local_committed_cache_publish(
            "external_put_transfer_end",
            &req.key,
            put_id,
            &pending_ctx,
            req.src_offset,
            req.len,
        ) {
            Ok(publish) => publish,
            Err(err) => {
                match inner.put_revoke(&req.key, put_id).await {
                    Ok(_) => inner.external_pending_puts.invalidate(&(
                        req.key.clone(),
                        put_id.0,
                        put_id.1,
                    )),
                    Err(revoke_err) => tracing::warn!(
                        "external_put_transfer_end put_revoke failed after local cache publish precheck error; pending fence retained: key={} put_id=({},{}) err={}",
                        req.key,
                        put_id.0,
                        put_id.1,
                        revoke_err
                    ),
                }
                return Ok(crate::rpcresp_kvresult_convert::FromError::from_error(&err));
            }
        };
        let put_end = match inner
            .put_end_with_local_cache_publish(&req.key, put_id, req.lease_id, publish_local_cache)
            .await
        {
            Ok(end) => end,
            Err(e) => {
                tracing::error!("Failed to end put operation: {}", e);
                tracing::warn!(
                    "external_put_transfer_end PutDone was not terminal; pending fence retained for retry: key={} put_id=({},{})",
                    req.key,
                    put_id.0,
                    put_id.1
                );
                return Ok(crate::rpcresp_kvresult_convert::FromError::from_error(&e));
            }
        };
        let put_end_stats = put_end.stats;
        let put_end_total_us = duration_to_i64_us(end_started_at.elapsed());

        let Some(holder_id) = put_end.local_cache_holder_id else {
            let err = KvError::Api(ApiError::Unknown {
                detail: format!(
                    "external_put_transfer_end missing local cache holder after local commit: key={} put_id=({},{})",
                    req.key, put_id.0, put_id.1
                ),
            });
            tracing::warn!(
                "external_put_transfer_end retained pending fence after terminal response omitted local holder: key={} put_id=({},{})",
                req.key,
                put_id.0,
                put_id.1
            );
            return Ok(crate::rpcresp_kvresult_convert::FromError::from_error(&err));
        };
        inner
            .install_local_committed_memory_info(
                &req.key,
                put_id,
                local_cache_publish.src_offset,
                local_cache_publish.len,
                holder_id,
            )
            .await?;
        inner
            .external_pending_puts
            .invalidate(&(req.key.clone(), put_id.0, put_id.1));
        if admitted_replica_task {
            if let Err(err) = inner
                .ensure_remote_put(
                    &req.key,
                    put_id,
                    pending_ctx.preferred_sub_cluster.clone(),
                    true,
                )
                .await
            {
                tracing::warn!(
                    "external_put_transfer_end make replica task failed after local commit: key={} put_id=({},{}) err={}",
                    req.key,
                    put_id.0,
                    put_id.1,
                    err
                );
            }
        }

        Ok(ExternalPutTransferEndResp {
            error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
            error_json: String::new(),
            test_put_phase_trace: if req.test_observe_put_phases {
                Some(TestPutPhaseTrace {
                    owner_external_put_transfer_end_total_us: duration_to_i64_us(
                        total_started_at.elapsed(),
                    ),
                    owner_put_transfer_total_us: put_transfer_total_us,
                    owner_put_transfer_peer_id: transfer_peer_id_for_trace,
                    owner_put_end_total_us: put_end_total_us,
                    owner_master_put_end_rpc_us: put_end_stats.master_put_end_rpc_us,
                    owner_master_put_end_server_us: put_end_stats.master_put_end_server_us,
                    ..Default::default()
                })
            } else {
                None
            },
        })
    }

    async fn external_batch_put_transfer_end(
        &self,
        req: ExternalBatchPutTransferEndReq,
    ) -> KvResult<ExternalBatchPutTransferEndResp> {
        if self.is_side_transfer_worker() {
            return Err(Self::side_transfer_unsupported(
                "external_batch_put_transfer_end",
            ));
        }
        let inner = self.inner();

        self.validate_requester_owner_status_updated(req.started_time)?;

        if req.items.is_empty() {
            return Ok(ExternalBatchPutTransferEndResp {
                items: Vec::new(),
                error_code: OK,
                error_json: String::new(),
            });
        }

        #[derive(Clone)]
        struct DonePending {
            idx: usize,
            key: String,
            put_id: crate::master_kv_router::put::PutIDForAKey,
            lease_id: Option<u64>,
            len: u64,
            put_locality: Option<(bool, i64)>,
            local_cache_publish: LocalCommittedCachePublish,
            make_replica_task: bool,
            // Keep the per-key reclaim fence alive until the master's terminal
            // response has been applied to the owner-local index.
            _pending_ctx: ExternalPendingPutCtx,
        }

        let mut results: Vec<Option<ExternalBatchPutTransferEndItemResp>> =
            (0..req.items.len()).map(|_| None).collect();
        let mut done_pending = Vec::new();
        let mut revoke_pending = Vec::new();
        let mut local_publish_items = Vec::new();
        let mut local_publish_contexts = Vec::new();

        for (idx, item) in req.items.into_iter().enumerate() {
            let Some(put_id) = item.put_id else {
                let err = KvError::Unreachable(
                    crate::rpcresp_kvresult_convert::msg_and_error::UnreachableError::RpcDecodeError {
                        rpc_input_json: format!(
                            "missing put_id in external_batch_put_transfer_end; key={}",
                            item.key
                        ),
                    },
                );
                results[idx] = Some(ExternalBatchPutTransferEndItemResp::from_error(&err));
                continue;
            };

            let pending_ctx = match require_external_pending_put_ctx(
                inner,
                "external_batch_put_transfer_end",
                &item.key,
                put_id,
            ) {
                Ok(ctx) => ctx,
                Err(err) => {
                    results[idx] = Some(ExternalBatchPutTransferEndItemResp::from_error(&err));
                    revoke_pending.push(BatchPutRevokeItemReq {
                        key: item.key.clone(),
                        put_id,
                    });
                    continue;
                }
            };
            if pending_ctx.local_reserve_slot.is_some() {
                if item.peer_id.is_some() {
                    let err = KvError::Api(ApiError::InvalidArgument {
                        detail: format!(
                            "external_batch_put_transfer_end local-first item must be local target: key={} put_id=({},{})",
                            item.key, put_id.0, put_id.1
                        ),
                    });
                    results[idx] = Some(ExternalBatchPutTransferEndItemResp::from_error(&err));
                    release_external_local_first_pending_slot(
                        inner,
                        &pending_ctx,
                        "external_batch_put_transfer_end",
                        &item.key,
                        put_id,
                    )
                    .await;
                    inner
                        .external_pending_puts
                        .invalidate(&(item.key.clone(), put_id.0, put_id.1));
                    continue;
                }
                match commit_external_local_first_pending(
                    inner,
                    &item.key,
                    put_id,
                    &pending_ctx,
                    item.src_offset,
                    item.len,
                    "external_batch_put_transfer_end",
                )
                .await
                {
                    Ok(committed_slot) => {
                        inner.record_put_locality(false, item.len, 0);
                        inner.external_pending_puts.invalidate(&(
                            item.key.clone(),
                            put_id.0,
                            put_id.1,
                        ));
                        local_publish_items.push(OwnerLocalPublishItem {
                            key: item.key.clone(),
                            put_id,
                            value_len: item.len,
                            lease_id: item.lease_id,
                            committed_slot,
                            make_replica_task: pending_ctx.make_replica_task,
                            preferred_sub_cluster: pending_ctx.preferred_sub_cluster.clone(),
                            atomic_group: pending_ctx.atomic_group.clone(),
                        });
                        local_publish_contexts.push(pending_ctx);
                        results[idx] = Some(ExternalBatchPutTransferEndItemResp {
                            error_code: OK,
                            error_json: String::new(),
                        });
                    }
                    Err(err) => {
                        release_external_local_first_pending_slot(
                            inner,
                            &pending_ctx,
                            "external_batch_put_transfer_end",
                            &item.key,
                            put_id,
                        )
                        .await;
                        inner.external_pending_puts.invalidate(&(
                            item.key.clone(),
                            put_id.0,
                            put_id.1,
                        ));
                        results[idx] = Some(ExternalBatchPutTransferEndItemResp::from_error(&err));
                    }
                }
                continue;
            }
            let has_remote_append = pending_ctx.peer_id.is_some();
            if item.peer_id.is_some() || has_remote_append {
                let err = KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "external_batch_put_transfer_end remote primary target is no longer supported: key={} put_id=({},{}) req_remote_target={} ctx_remote_target={}",
                        item.key,
                        put_id.0,
                        put_id.1,
                        item.peer_id.is_some(),
                        has_remote_append
                    ),
                });
                results[idx] = Some(ExternalBatchPutTransferEndItemResp::from_error(&err));
                revoke_pending.push(BatchPutRevokeItemReq {
                    key: item.key.clone(),
                    put_id,
                });
                continue;
            }
            let local_cache_publish = match local_committed_cache_publish(
                "external_batch_put_transfer_end",
                &item.key,
                put_id,
                &pending_ctx,
                item.src_offset,
                item.len,
            ) {
                Ok(publish) => publish,
                Err(err) => {
                    results[idx] = Some(ExternalBatchPutTransferEndItemResp::from_error(&err));
                    revoke_pending.push(BatchPutRevokeItemReq {
                        key: item.key.clone(),
                        put_id,
                    });
                    continue;
                }
            };
            done_pending.push(DonePending {
                idx,
                key: item.key,
                put_id,
                lease_id: item.lease_id,
                len: item.len,
                put_locality: Some((false, 0)),
                local_cache_publish,
                make_replica_task: pending_ctx.make_replica_task,
                _pending_ctx: pending_ctx,
            });
        }

        if !local_publish_items.is_empty() {
            spawn_external_local_first_publish(inner, local_publish_items, local_publish_contexts);
        }

        if !revoke_pending.is_empty() {
            let requested = revoke_pending.clone();
            match inner.batch_put_revoke(revoke_pending).await {
                Ok(response) if response.items.len() == requested.len() => {
                    for (request, response) in requested.into_iter().zip(response.items) {
                        if response.key == request.key
                            && response.put_id == request.put_id
                            && response.error_code == OK
                        {
                            inner.external_pending_puts.invalidate(&(
                                request.key,
                                request.put_id.0,
                                request.put_id.1,
                            ));
                        }
                    }
                }
                Ok(response) => tracing::warn!(
                    "external_batch_put_transfer_end batch_put_revoke response length mismatch: expected={} got={}",
                    requested.len(),
                    response.items.len()
                ),
                Err(err) => tracing::warn!(
                    "external_batch_put_transfer_end batch_put_revoke failed after precheck errors; pending fences retained for retry: {}",
                    err
                ),
            }
        }

        if inner.skip_put_end_commit_enabled() {
            for pending in done_pending {
                inner.external_pending_puts.invalidate(&(
                    pending.key.clone(),
                    pending.put_id.0,
                    pending.put_id.1,
                ));
                if let Some((remote, transfer_us)) = pending.put_locality {
                    inner.record_put_locality(remote, pending.len, transfer_us);
                }
                results[pending.idx] = Some(ExternalBatchPutTransferEndItemResp {
                    error_code: OK,
                    error_json: String::new(),
                });
            }
            return Ok(ExternalBatchPutTransferEndResp {
                items: results
                    .into_iter()
                    .map(|item| {
                        item.unwrap_or_else(|| ExternalBatchPutTransferEndItemResp {
                            error_code: OK,
                            error_json: String::new(),
                        })
                    })
                    .collect(),
                error_code: OK,
                error_json: String::new(),
            });
        }

        let done_resp = inner
            .batch_put_done(
                done_pending
                    .iter()
                    .map(|pending| BatchPutDoneItemReq {
                        key: pending.key.clone(),
                        put_id: pending.put_id,
                        lease_id: pending.lease_id,
                        committed_slot: None,
                        publish_local_cache: true,
                        atomic_group: None,
                    })
                    .collect(),
            )
            .await?;
        if done_resp.items.len() != done_pending.len() {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "external_batch_put_transfer_end done response length mismatch: expected={} got={}",
                    done_pending.len(),
                    done_resp.items.len()
                ),
            }));
        }

        for (pending, done_item) in done_pending.into_iter().zip(done_resp.items.into_iter()) {
            if let Err(err) = crate::rpcresp_kvresult_convert::try_from_code(
                done_item.error_code,
                done_item.error_json.clone(),
            ) {
                results[pending.idx] = Some(ExternalBatchPutTransferEndItemResp::from_error(&err));
                continue;
            }
            if let Some((remote, transfer_us)) = pending.put_locality {
                inner.record_put_locality(remote, pending.len, transfer_us);
            }
            let Some(holder_id) = done_item.local_cache_holder_id else {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "external_batch_put_transfer_end missing local cache holder after local commit: key={} put_id=({},{})",
                        pending.key, pending.put_id.0, pending.put_id.1
                    ),
                }));
            };
            inner
                .install_local_committed_memory_info(
                    &pending.key,
                    pending.put_id,
                    pending.local_cache_publish.src_offset,
                    pending.local_cache_publish.len,
                    holder_id,
                )
                .await?;
            inner.external_pending_puts.invalidate(&(
                pending.key.clone(),
                pending.put_id.0,
                pending.put_id.1,
            ));
            if pending.make_replica_task {
                if let Err(err) = inner
                    .ensure_remote_put(
                        &pending.key,
                        pending.put_id,
                        pending._pending_ctx.preferred_sub_cluster.clone(),
                        true,
                    )
                    .await
                {
                    tracing::warn!(
                        "external_batch_put_transfer_end make replica task failed after local commit: key={} put_id=({},{}) err={}",
                        pending.key,
                        pending.put_id.0,
                        pending.put_id.1,
                        err
                    );
                }
            }
            results[pending.idx] = Some(ExternalBatchPutTransferEndItemResp {
                error_code: OK,
                error_json: String::new(),
            });
        }

        Ok(ExternalBatchPutTransferEndResp {
            items: results
                .into_iter()
                .map(|item| {
                    item.unwrap_or_else(|| ExternalBatchPutTransferEndItemResp {
                        error_code: OK,
                        error_json: String::new(),
                    })
                })
                .collect(),
            error_code: OK,
            error_json: String::new(),
        })
    }

    async fn external_put_commit(
        &self,
        req: ExternalPutCommitReq,
    ) -> KvResult<ExternalPutCommitResp> {
        if self.is_side_transfer_worker() {
            return Err(Self::side_transfer_unsupported("external_put_commit"));
        }
        let inner = self.inner();
        self.validate_requester_owner_status_updated(req.started_time)?;
        let Some(put_id) = req.put_id else {
            let err = KvError::Unreachable(
                crate::rpcresp_kvresult_convert::msg_and_error::UnreachableError::RpcDecodeError {
                    rpc_input_json: format!("missing put_id; key={}", req.key),
                },
            );
            return Ok(ExternalPutCommitResp::from_error(&err));
        };
        let pending_ctx = match require_external_pending_put_ctx(
            inner,
            "external_put_commit",
            &req.key,
            put_id,
        ) {
            Ok(ctx) => ctx,
            Err(err) => {
                best_effort_revoke_missing_external_pending_ctx(
                    inner,
                    "external_put_commit",
                    &req.key,
                    put_id,
                )
                .await;
                return Ok(ExternalPutCommitResp::from_error(&err));
            }
        };
        let has_remote_append = pending_ctx.peer_id.is_some();
        if req.remote_target || has_remote_append {
            let err = KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "external_put_commit remote primary target is no longer supported: key={} put_id=({},{}) req_remote_target={} ctx_remote_target={}",
                    req.key, put_id.0, put_id.1, req.remote_target, has_remote_append
                ),
            });
            match inner.put_revoke(&req.key, put_id).await {
                Ok(_) => {
                    inner
                        .external_pending_puts
                        .invalidate(&(req.key.clone(), put_id.0, put_id.1))
                }
                Err(revoke_err) => tracing::warn!(
                    "external_put_commit put_revoke failed after remote primary target; pending fence retained: key={} put_id=({},{}) err={}",
                    req.key,
                    put_id.0,
                    put_id.1,
                    revoke_err
                ),
            }
            return Ok(ExternalPutCommitResp::from_error(&err));
        }
        let admitted_replica_task = pending_ctx.make_replica_task;

        let end_started_at = Instant::now();
        let local_cache_publish = match local_committed_cache_publish(
            "external_put_commit",
            &req.key,
            put_id,
            &pending_ctx,
            req.src_offset,
            req.len,
        ) {
            Ok(publish) => publish,
            Err(err) => {
                match inner.put_revoke(&req.key, put_id).await {
                    Ok(_) => inner.external_pending_puts.invalidate(&(
                        req.key.clone(),
                        put_id.0,
                        put_id.1,
                    )),
                    Err(revoke_err) => tracing::warn!(
                        "external_put_commit put_revoke failed after local cache publish precheck error; pending fence retained: key={} put_id=({},{}) err={}",
                        req.key,
                        put_id.0,
                        put_id.1,
                        revoke_err
                    ),
                }
                return Ok(ExternalPutCommitResp::from_error(&err));
            }
        };
        let put_end = match inner
            .put_end_with_local_cache_publish(&req.key, put_id, req.lease_id, true)
            .await
        {
            Ok(end) => end,
            Err(e) => {
                tracing::warn!(
                    "external_put_commit PutDone was not terminal; pending fence retained for retry: key={} put_id=({},{}) err={}",
                    req.key,
                    put_id.0,
                    put_id.1,
                    e
                );
                return Ok(ExternalPutCommitResp::from_error(&e));
            }
        };
        let put_end_stats = put_end.stats;
        let put_end_total_us = duration_to_i64_us(end_started_at.elapsed());
        let Some(holder_id) = put_end.local_cache_holder_id else {
            let err = KvError::Api(ApiError::Unknown {
                detail: format!(
                    "external_put_commit missing local cache holder after local commit: key={} put_id=({},{})",
                    req.key, put_id.0, put_id.1
                ),
            });
            tracing::warn!(
                "external_put_commit retained pending fence after terminal response omitted local holder: key={} put_id=({},{})",
                req.key,
                put_id.0,
                put_id.1
            );
            return Ok(ExternalPutCommitResp::from_error(&err));
        };
        inner
            .install_local_committed_memory_info(
                &req.key,
                put_id,
                local_cache_publish.src_offset,
                local_cache_publish.len,
                holder_id,
            )
            .await?;
        inner
            .external_pending_puts
            .invalidate(&(req.key.clone(), put_id.0, put_id.1));
        if admitted_replica_task {
            if let Err(err) = inner
                .ensure_remote_put(
                    &req.key,
                    put_id,
                    pending_ctx.preferred_sub_cluster.clone(),
                    true,
                )
                .await
            {
                tracing::warn!(
                    "external_put_commit make replica task failed after local commit: key={} put_id=({},{}) err={}",
                    req.key,
                    put_id.0,
                    put_id.1,
                    err
                );
            }
        }

        Ok(ExternalPutCommitResp {
            error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
            error_json: String::new(),
            test_put_phase_trace: if req.test_observe_put_phases {
                Some(TestPutPhaseTrace {
                    owner_put_end_total_us: put_end_total_us,
                    owner_master_put_end_rpc_us: put_end_stats.master_put_end_rpc_us,
                    owner_master_put_end_server_us: put_end_stats.master_put_end_server_us,
                    ..Default::default()
                })
            } else {
                None
            },
        })
    }

    async fn external_batch_put_commit(
        &self,
        req: ExternalBatchPutCommitReq,
    ) -> KvResult<ExternalBatchPutCommitResp> {
        if self.is_side_transfer_worker() {
            return Err(Self::side_transfer_unsupported("external_batch_put_commit"));
        }
        let inner = self.inner();

        self.validate_requester_owner_status_updated(req.started_time)?;

        if req.items.is_empty() {
            return Ok(ExternalBatchPutCommitResp {
                items: Vec::new(),
                error_code: OK,
                error_json: String::new(),
            });
        }

        #[derive(Clone)]
        struct DonePending {
            idx: usize,
            key: String,
            put_id: crate::master_kv_router::put::PutIDForAKey,
            lease_id: Option<u64>,
            local_cache_publish: LocalCommittedCachePublish,
            make_replica_task: bool,
            _pending_ctx: ExternalPendingPutCtx,
        }

        let mut results: Vec<Option<ExternalBatchPutCommitItemResp>> =
            (0..req.items.len()).map(|_| None).collect();
        let mut done_pending = Vec::new();
        let mut revoke_pending = Vec::new();
        let mut local_publish_items = Vec::new();
        let mut local_publish_contexts = Vec::new();

        for (idx, item) in req.items.into_iter().enumerate() {
            let Some(put_id) = item.put_id else {
                let err = KvError::Unreachable(
                    crate::rpcresp_kvresult_convert::msg_and_error::UnreachableError::RpcDecodeError {
                        rpc_input_json: format!(
                            "missing put_id in external_batch_put_commit; key={}",
                            item.key
                        ),
                    },
                );
                results[idx] = Some(ExternalBatchPutCommitItemResp::from_error(&err));
                continue;
            };
            let pending_ctx = match require_external_pending_put_ctx(
                inner,
                "external_batch_put_commit",
                &item.key,
                put_id,
            ) {
                Ok(ctx) => ctx,
                Err(err) => {
                    results[idx] = Some(ExternalBatchPutCommitItemResp::from_error(&err));
                    revoke_pending.push(BatchPutRevokeItemReq {
                        key: item.key.clone(),
                        put_id,
                    });
                    continue;
                }
            };
            if pending_ctx.local_reserve_slot.is_some() {
                if item.remote_target {
                    let err = KvError::Api(ApiError::InvalidArgument {
                        detail: format!(
                            "external_batch_put_commit local-first item must be local target: key={} put_id=({},{})",
                            item.key, put_id.0, put_id.1
                        ),
                    });
                    results[idx] = Some(ExternalBatchPutCommitItemResp::from_error(&err));
                    release_external_local_first_pending_slot(
                        inner,
                        &pending_ctx,
                        "external_batch_put_commit",
                        &item.key,
                        put_id,
                    )
                    .await;
                    inner
                        .external_pending_puts
                        .invalidate(&(item.key.clone(), put_id.0, put_id.1));
                    continue;
                }
                match commit_external_local_first_pending(
                    inner,
                    &item.key,
                    put_id,
                    &pending_ctx,
                    item.src_offset,
                    item.len,
                    "external_batch_put_commit",
                )
                .await
                {
                    Ok(committed_slot) => {
                        inner.record_put_locality(false, item.len, 0);
                        inner.external_pending_puts.invalidate(&(
                            item.key.clone(),
                            put_id.0,
                            put_id.1,
                        ));
                        local_publish_items.push(OwnerLocalPublishItem {
                            key: item.key.clone(),
                            put_id,
                            value_len: item.len,
                            lease_id: item.lease_id,
                            committed_slot,
                            make_replica_task: pending_ctx.make_replica_task,
                            preferred_sub_cluster: pending_ctx.preferred_sub_cluster.clone(),
                            atomic_group: pending_ctx.atomic_group.clone(),
                        });
                        local_publish_contexts.push(pending_ctx);
                        results[idx] = Some(ExternalBatchPutCommitItemResp {
                            error_code: OK,
                            error_json: String::new(),
                        });
                    }
                    Err(err) => {
                        release_external_local_first_pending_slot(
                            inner,
                            &pending_ctx,
                            "external_batch_put_commit",
                            &item.key,
                            put_id,
                        )
                        .await;
                        inner.external_pending_puts.invalidate(&(
                            item.key.clone(),
                            put_id.0,
                            put_id.1,
                        ));
                        results[idx] = Some(ExternalBatchPutCommitItemResp::from_error(&err));
                    }
                }
                continue;
            }
            let has_remote_append = pending_ctx.peer_id.is_some();
            if item.remote_target || has_remote_append {
                let err = KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "external_batch_put_commit remote primary target is no longer supported: key={} put_id=({},{}) req_remote_target={} ctx_remote_target={}",
                        item.key, put_id.0, put_id.1, item.remote_target, has_remote_append
                    ),
                });
                results[idx] = Some(ExternalBatchPutCommitItemResp::from_error(&err));
                revoke_pending.push(BatchPutRevokeItemReq {
                    key: item.key.clone(),
                    put_id,
                });
                continue;
            };
            let local_cache_publish = match local_committed_cache_publish(
                "external_batch_put_commit",
                &item.key,
                put_id,
                &pending_ctx,
                item.src_offset,
                item.len,
            ) {
                Ok(publish) => publish,
                Err(err) => {
                    results[idx] = Some(ExternalBatchPutCommitItemResp::from_error(&err));
                    revoke_pending.push(BatchPutRevokeItemReq {
                        key: item.key.clone(),
                        put_id,
                    });
                    continue;
                }
            };
            done_pending.push(DonePending {
                idx,
                key: item.key.clone(),
                put_id,
                lease_id: item.lease_id,
                local_cache_publish,
                make_replica_task: pending_ctx.make_replica_task,
                _pending_ctx: pending_ctx,
            });
        }

        if !local_publish_items.is_empty() {
            spawn_external_local_first_publish(inner, local_publish_items, local_publish_contexts);
        }

        if !revoke_pending.is_empty() {
            let requested = revoke_pending.clone();
            match inner.batch_put_revoke(revoke_pending).await {
                Ok(response) if response.items.len() == requested.len() => {
                    for (request, response) in requested.into_iter().zip(response.items) {
                        if response.key == request.key
                            && response.put_id == request.put_id
                            && response.error_code == OK
                        {
                            inner.external_pending_puts.invalidate(&(
                                request.key,
                                request.put_id.0,
                                request.put_id.1,
                            ));
                        }
                    }
                }
                Ok(response) => tracing::warn!(
                    "external_batch_put_commit batch_put_revoke response length mismatch: expected={} got={}",
                    requested.len(),
                    response.items.len()
                ),
                Err(err) => tracing::warn!(
                    "external_batch_put_commit batch_put_revoke failed after precheck errors; pending fences retained for retry: {}",
                    err
                ),
            }
        }

        if inner.skip_put_end_commit_enabled() {
            for pending in done_pending {
                inner.external_pending_puts.invalidate(&(
                    pending.key.clone(),
                    pending.put_id.0,
                    pending.put_id.1,
                ));
                results[pending.idx] = Some(ExternalBatchPutCommitItemResp {
                    error_code: OK,
                    error_json: String::new(),
                });
            }
            return Ok(ExternalBatchPutCommitResp {
                items: results
                    .into_iter()
                    .map(|item| {
                        item.unwrap_or_else(|| ExternalBatchPutCommitItemResp {
                            error_code: OK,
                            error_json: String::new(),
                        })
                    })
                    .collect(),
                error_code: OK,
                error_json: String::new(),
            });
        }

        let done_resp = inner
            .batch_put_done(
                done_pending
                    .iter()
                    .map(|pending| BatchPutDoneItemReq {
                        key: pending.key.clone(),
                        put_id: pending.put_id,
                        lease_id: pending.lease_id,
                        committed_slot: None,
                        publish_local_cache: true,
                        atomic_group: None,
                    })
                    .collect(),
            )
            .await?;
        if done_resp.items.len() != done_pending.len() {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "external_batch_put_commit done response length mismatch: expected={} got={}",
                    done_pending.len(),
                    done_resp.items.len()
                ),
            }));
        }

        for (pending, done_item) in done_pending.into_iter().zip(done_resp.items.into_iter()) {
            if let Err(err) = crate::rpcresp_kvresult_convert::try_from_code(
                done_item.error_code,
                done_item.error_json.clone(),
            ) {
                results[pending.idx] = Some(ExternalBatchPutCommitItemResp::from_error(&err));
                continue;
            }
            let Some(holder_id) = done_item.local_cache_holder_id else {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "external_batch_put_commit missing local cache holder after local commit: key={} put_id=({},{})",
                        pending.key, pending.put_id.0, pending.put_id.1
                    ),
                }));
            };
            inner
                .install_local_committed_memory_info(
                    &pending.key,
                    pending.put_id,
                    pending.local_cache_publish.src_offset,
                    pending.local_cache_publish.len,
                    holder_id,
                )
                .await?;
            inner.external_pending_puts.invalidate(&(
                pending.key.clone(),
                pending.put_id.0,
                pending.put_id.1,
            ));
            if pending.make_replica_task {
                if let Err(err) = inner
                    .ensure_remote_put(
                        &pending.key,
                        pending.put_id,
                        pending._pending_ctx.preferred_sub_cluster.clone(),
                        true,
                    )
                    .await
                {
                    tracing::warn!(
                        "external_batch_put_commit make replica task failed after local commit: key={} put_id=({},{}) err={}",
                        pending.key,
                        pending.put_id.0,
                        pending.put_id.1,
                        err
                    );
                }
            }
            results[pending.idx] = Some(ExternalBatchPutCommitItemResp {
                error_code: OK,
                error_json: String::new(),
            });
        }

        Ok(ExternalBatchPutCommitResp {
            items: results
                .into_iter()
                .map(|item| {
                    item.unwrap_or_else(|| ExternalBatchPutCommitItemResp {
                        error_code: OK,
                        error_json: String::new(),
                    })
                })
                .collect(),
            error_code: OK,
            error_json: String::new(),
        })
    }

    async fn external_put_revoke(
        &self,
        req: ExternalPutRevokeReq,
    ) -> KvResult<ExternalPutRevokeResp> {
        if self.is_side_transfer_worker() {
            return Err(Self::side_transfer_unsupported("external_put_revoke"));
        }
        let inner = self.inner();
        self.validate_requester_owner_status_updated(req.started_time)?;
        let Some(put_id) = req.put_id else {
            let err = KvError::Unreachable(
                crate::rpcresp_kvresult_convert::msg_and_error::UnreachableError::RpcDecodeError {
                    rpc_input_json: format!("missing put_id; key={}", req.key),
                },
            );
            return Ok(ExternalPutRevokeResp::from_error(&err));
        };
        let pending_ctx = match require_external_pending_put_ctx(
            inner,
            "external_put_revoke",
            &req.key,
            put_id,
        ) {
            Ok(ctx) => ctx,
            Err(err) => {
                best_effort_revoke_missing_external_pending_ctx(
                    inner,
                    "external_put_revoke",
                    &req.key,
                    put_id,
                )
                .await;
                return Ok(ExternalPutRevokeResp::from_error(&err));
            }
        };
        if pending_ctx.local_reserve_slot.is_some() {
            return match pending_ctx
                ._pending_fence
                .release_local_slot_lease_now(inner)
                .await
            {
                Ok(_) => {
                    inner
                        .external_pending_puts
                        .invalidate(&(req.key.clone(), put_id.0, put_id.1));
                    Ok(ExternalPutRevokeResp {
                        error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
                        error_json: String::new(),
                    })
                }
                Err(e) => Ok(ExternalPutRevokeResp::from_error(&e)),
            };
        }
        match inner.put_revoke(&req.key, put_id).await {
            Ok(_) => {
                inner
                    .external_pending_puts
                    .invalidate(&(req.key.clone(), put_id.0, put_id.1));
                Ok(ExternalPutRevokeResp {
                    error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
                    error_json: String::new(),
                })
            }
            Err(e) => Ok(ExternalPutRevokeResp::from_error(&e)),
        }
    }

    /// Handle external delete request
    async fn external_delete(&self, req: ExternalDeleteReq) -> KvResult<ExternalDeleteResp> {
        if self.is_side_transfer_worker() {
            return Err(Self::side_transfer_unsupported("external_delete"));
        }
        let inner = self.inner();

        self.validate_requester_owner_status_updated(req.started_time)?;

        match inner.delete(&req.key).await {
            Ok(_) => Ok(ExternalDeleteResp {
                error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
                error_json: String::new(),
            }),
            Err(e) => Ok(ExternalDeleteResp::from_error(&e)),
        }
    }

    /// Handle external is_exist request
    async fn external_is_exist(&self, req: ExternalIsExistReq) -> KvResult<ExternalIsExistResp> {
        if self.is_side_transfer_worker() {
            return Err(Self::side_transfer_unsupported("external_is_exist"));
        }
        let inner = self.inner();

        self.validate_requester_owner_status_updated(req.started_time)?;

        match inner.is_exist(&req.key).await {
            Ok(exists) => Ok(ExternalIsExistResp {
                error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
                exists,
                error_json: String::new(),
            }),
            Err(e) => Ok(ExternalIsExistResp {
                exists: false,
                ..ExternalIsExistResp::from_error(&e)
            }),
        }
    }

    async fn external_batch_is_exist(
        &self,
        req: ExternalBatchIsExistReq,
    ) -> KvResult<ExternalBatchIsExistResp> {
        if self.is_side_transfer_worker() {
            return Err(Self::side_transfer_unsupported("external_batch_is_exist"));
        }
        let inner = self.inner();

        self.validate_requester_owner_status_updated(req.started_time)?;

        match inner
            .batch_is_exist(req.keys, req.allow_local_snapshot)
            .await
        {
            Ok(exists_list) => Ok(ExternalBatchIsExistResp {
                error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
                exists_list,
                error_json: String::new(),
            }),
            Err(e) => Ok(ExternalBatchIsExistResp {
                exists_list: Vec::new(),
                ..ExternalBatchIsExistResp::from_error(&e)
            }),
        }
    }

    async fn external_observability_snapshot(
        &self,
        req: ExternalObservabilitySnapshotReq,
    ) -> KvResult<ExternalObservabilitySnapshotResp> {
        if self.is_side_transfer_worker() {
            return Err(Self::side_transfer_unsupported(
                "external_observability_snapshot",
            ));
        }
        self.validate_requester_owner_status_updated(req.started_time)?;
        Ok(ExternalObservabilitySnapshotResp::success(
            self.inner().locality_snapshot(),
        ))
    }
}
