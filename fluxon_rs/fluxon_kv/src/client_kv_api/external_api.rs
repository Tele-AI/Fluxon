use crate::client_kv_api::ClientKvApi;
use crate::client_kv_api::msg_pack::{
    ExternalBatchGetCancelPlan, ExternalBatchGetCancelReq, ExternalBatchGetCancelResp,
    ExternalBatchGetItemResp, ExternalBatchGetReq, ExternalBatchGetResp, ExternalBatchGetStartReq,
    ExternalBatchGetStartResp, ExternalBatchGetStartTransferPlan, ExternalBatchGetTransferReq,
    ExternalBatchGetTransferResp, ExternalBatchIsExistReq, ExternalBatchIsExistResp,
    ExternalBatchPutCommitItemResp, ExternalBatchPutCommitReq, ExternalBatchPutCommitResp,
    ExternalBatchPutStartItemResp, ExternalBatchPutStartReq, ExternalBatchPutStartResp,
    ExternalBatchPutTransferEndItemResp, ExternalBatchPutTransferEndReq,
    ExternalBatchPutTransferEndResp, ExternalDeleteReq, ExternalDeleteResp, ExternalGetReq,
    ExternalGetResp, ExternalIsExistReq, ExternalIsExistResp, ExternalObservabilitySnapshotReq,
    ExternalObservabilitySnapshotResp, ExternalPutCommitReq, ExternalPutCommitResp,
    ExternalPutRevokeReq, ExternalPutRevokeResp, ExternalPutStartReq, ExternalPutStartResp,
    ExternalPutTransferEndReq, ExternalPutTransferEndResp, TestPutPhaseTrace,
};
use crate::client_kv_api::{
    self, ExternalGetStartDedupKey, ExternalGetStartEntry, ExternalGetStartOwnerItem,
    ExternalGetStartPrefixResult, ExternalGetStartSharedItemResult, ExternalGetStartSharedOp,
    ExternalGetStartSharedPhase, ExternalGetStartTransferOutput, ExternalPendingPutCtx,
    ReplicaTaskTarget,
};
use crate::client_seg_pool::{ResolveSideTransferLaneReq, parse_side_transfer_worker_lane_idx};
use crate::cluster_manager::NodeIDString;
use crate::cluster_manager::{
    META_KEY_SHARED_STORAGE_NODE_ID, META_KEY_SHARED_STORAGE_NODE_START_TIME,
};
use crate::master_kv_router::msg_pack::{
    BatchGetStartItemResp, BatchGetStartResp, BatchPutDoneItemReq, BatchPutRevokeItemReq,
    BatchPutStartItemReq, PutDoneCommittedSlot, build_put_atomic_group_assignments,
};
use crate::memholder::MemholderManagerTrait;
use crate::memholder::NodeHolderKey;
use crate::memholder::{UserMemHolder, UserMemHolderExposeKind};
use crate::p2p::msg_pack::MsgPack;
use crate::rpcresp_kvresult_convert::FromError;
use crate::rpcresp_kvresult_convert::ToResult;
use crate::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult, OK, codes_api};
use async_trait::async_trait;
use dashmap::mapref::entry::Entry;
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
const GET_TARGET_NO_SPACE_RESERVE_SHRINK_RETRY_LIMIT: usize = 8;

#[derive(Clone)]
struct LocalCommittedCachePublish {
    src_offset: u64,
    len: u32,
}

fn local_committed_cache_publish(
    op: &'static str,
    key: &str,
    put_id: crate::master_kv_router::put::PutIDForAKey,
    pending_ctx: Option<&ExternalPendingPutCtx>,
    req_src_offset: u64,
    req_len: u64,
) -> KvResult<LocalCommittedCachePublish> {
    if let Some(ctx) = pending_ctx {
        if ctx.src_offset != req_src_offset || ctx.len != req_len {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "{op} local cache publish request mismatches pending ctx: key={} put_id=({},{}) req_src_offset={} ctx_src_offset={} req_len={} ctx_len={}",
                    key, put_id.0, put_id.1, req_src_offset, ctx.src_offset, req_len, ctx.len
                ),
            }));
        }
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

fn external_local_first_error_item(err: &KvError) -> ExternalBatchPutStartItemResp {
    ExternalBatchPutStartItemResp {
        put_id: None,
        ..ExternalBatchPutStartItemResp::from_error(err)
    }
}

fn external_local_first_release_keys(inner: &client_kv_api::ClientKvApiInner, keys: &[String]) {
    for key in keys {
        inner.release_external_local_first_put_key(key);
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
    inner.promote_precommit_local_reserve_resident_slot_if_same(
        key,
        put_id,
        memory_info,
        ctx.atomic_group.as_ref(),
    )?;
    Ok(PutDoneCommittedSlot {
        grant_id: slot_ref.grant_id,
        slot_index: slot_ref.slot_index,
        slot_size,
        addr: slot_ref.ptr,
        base_addr: slot_ref.base_addr,
        len: req_len,
    })
}

fn spawn_external_local_first_publish(
    inner: &client_kv_api::ClientKvApiInner,
    key: String,
    put_id: crate::master_kv_router::put::PutIDForAKey,
    lease_id: Option<u64>,
    committed_slot: PutDoneCommittedSlot,
    make_replica_task: bool,
    preferred_sub_cluster: Option<String>,
    atomic_group: Option<crate::master_kv_router::msg_pack::PutAtomicGroup>,
) {
    let view = inner.view.clone_view();
    let spawn_view = view.clone();
    spawn_view.spawn("external_local_first_route_publish", async move {
        let inner = view.client_kv_api().inner();
        let publish_ok = match inner
            .batch_put_done(vec![BatchPutDoneItemReq {
                key: key.clone(),
                put_id,
                lease_id,
                committed_slot: Some(committed_slot),
                publish_local_cache: false,
                atomic_group,
            }])
            .await
        {
            Ok(resp) => {
                if let Some(item) = resp.items.into_iter().next() {
                    if let Err(err) =
                        crate::rpcresp_kvresult_convert::try_from_code(item.error_code, item.error_json)
                    {
                        tracing::warn!(
                            "external local-first route publish failed: key={} put_id=({},{}) err={}",
                            key,
                            put_id.0,
                            put_id.1,
                            err
                        );
                        false
                    } else {
                        true
                    }
                } else {
                    tracing::warn!(
                        "external local-first route publish returned empty response: key={} put_id=({},{})",
                        key,
                        put_id.0,
                        put_id.1
                    );
                    false
                }
            }
            Err(err) => {
                tracing::warn!(
                    "external local-first route publish rpc failed: key={} put_id=({},{}) err={}",
                    key,
                    put_id.0,
                    put_id.1,
                    err
                );
                false
            }
        };
        if publish_ok && make_replica_task {
            if let Err(err) = inner
                .make_replica_append_task(&key, put_id, preferred_sub_cluster, true)
                .await
            {
                tracing::warn!(
                    "external local-first enqueue replica append failed: key={} put_id=({},{}) err={}",
                    key,
                    put_id.0,
                    put_id.1,
                    err
                );
            }
        }
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
            ExternalGetStartOwnerItem::Started { .. } => None,
        })
        .collect()
}

fn collect_get_target_no_space_indices(items: &[BatchGetStartItemResp]) -> Vec<usize> {
    items
        .iter()
        .enumerate()
        .filter_map(|(idx, item)| (item.error_code == codes_api::API_NO_SPACE).then_some(idx))
        .collect()
}

async fn batch_get_start_with_reserve_pressure_retry(
    inner: &client_kv_api::ClientKvApiInner,
    keys: &[String],
) -> KvResult<BatchGetStartResp> {
    let mut response = inner.batch_get_start(keys.to_vec()).await?;
    if response.items.len() != keys.len() {
        return Err(KvError::Api(ApiError::Unknown {
            detail: format!(
                "external_batch_get_start response length mismatch: expected={} got={}",
                keys.len(),
                response.items.len()
            ),
        }));
    }

    for attempt in 1..=GET_TARGET_NO_SPACE_RESERVE_SHRINK_RETRY_LIMIT {
        let retry_indices = collect_get_target_no_space_indices(&response.items);
        if retry_indices.is_empty() {
            break;
        }
        let no_space_before = retry_indices.len();
        if !crate::client_kv_api::local_reserve_rebalance::release_one_excess_reserve_grant_for_get_target_pressure(inner).await
        {
            break;
        }
        let retry_keys = retry_indices
            .iter()
            .map(|idx| keys[*idx].clone())
            .collect::<Vec<_>>();
        let retry_response = inner.batch_get_start(retry_keys).await?;
        if retry_response.items.len() != retry_indices.len() {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "external_batch_get_start NoSpace retry response length mismatch: expected={} got={} attempt={}",
                    retry_indices.len(),
                    retry_response.items.len(),
                    attempt
                ),
            }));
        }
        response.server_process_us = response
            .server_process_us
            .saturating_add(retry_response.server_process_us);
        for (idx, item) in retry_indices
            .into_iter()
            .zip(retry_response.items.into_iter())
        {
            response.items[idx] = item;
        }
        let no_space_after = collect_get_target_no_space_indices(&response.items).len();
        tracing::info!(
            "external_batch_get_start retried local Get-target NoSpace after reserve shrink: attempt={} no_space_before={} no_space_after={}",
            attempt,
            no_space_before,
            no_space_after
        );
    }
    Ok(response)
}

#[cfg(test)]
mod external_get_start_batch_tests {
    use super::{
        collect_external_get_start_missing, collect_get_target_no_space_indices,
        compute_external_get_start_raw_prefix, compute_external_get_start_transfer_prefix,
        normalize_external_put_start_group_lens,
    };
    use crate::master_kv_router::msg_pack::BatchGetStartItemResp;
    use crate::rpcresp_kvresult_convert::msg_and_error::{OK, codes_api};

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
    fn get_target_pressure_retry_selects_only_no_space_items() {
        let mut items = vec![BatchGetStartItemResp::default(); 4];
        items[0].error_code = OK;
        items[1].error_code = codes_api::API_NO_SPACE;
        items[2].error_code = codes_api::API_KEY_NOT_FOUND;
        items[3].error_code = codes_api::API_NO_SPACE;
        assert_eq!(collect_get_target_no_space_indices(&items), vec![1, 3]);
    }
}

async fn finish_external_get_start_transfer(
    view: crate::client_kv_api::ClientKvApiView,
    transfer_items: Vec<ExternalGetStartOwnerItem>,
    transfer_concurrency: usize,
) -> KvResult<ExternalGetStartTransferOutput> {
    let client_api = view.client_kv_api();
    let inner = client_api.inner();
    let refcount = inner.get_or_init_all_memholder_refcount();
    let mut results: Vec<
        Option<KvResult<Option<(Arc<UserMemHolder>, Option<client_kv_api::RemoteGetInfo>)>>>,
    > = (0..transfer_items.len()).map(|_| None).collect();
    let mut started_positions = Vec::new();
    let mut started_keys = Vec::new();
    let mut started_items = Vec::new();

    for (idx, item) in transfer_items.into_iter().enumerate() {
        match item {
            ExternalGetStartOwnerItem::Local { memory_info } => {
                let holder = Arc::new(UserMemHolder::new(
                    memory_info,
                    refcount.clone(),
                    UserMemHolderExposeKind::SegPtr,
                ));
                results[idx] = Some(Ok(Some((holder, None))));
            }
            ExternalGetStartOwnerItem::Started { key, item } => {
                started_positions.push(idx);
                started_keys.push(key);
                started_items.push(item);
            }
        }
    }

    if !started_items.is_empty() {
        let started_results = inner
            .batch_get_finish_started(started_keys, started_items, transfer_concurrency)
            .await?;
        if started_results.len() != started_positions.len() {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "external get_start transfer result length mismatch: expected={} got={}",
                    started_positions.len(),
                    started_results.len()
                ),
            }));
        }
        for (idx, result) in started_positions
            .into_iter()
            .zip(started_results.into_iter())
        {
            results[idx] = Some(result);
        }
    }

    Ok(results
        .into_iter()
        .map(|item| {
            item.unwrap_or_else(|| {
                Err(KvError::Api(ApiError::Unknown {
                    detail: "external get_start transfer result slot was not populated".to_string(),
                }))
            })
        })
        .collect())
}

fn external_get_start_error_parts(err: &KvError) -> (u32, String) {
    (err.code(), err.to_json())
}

fn register_external_get_start_waiter(shared_op: &Arc<ExternalGetStartSharedOp>) {
    let mut state = shared_op.state.lock();
    state.waiter_count = state
        .waiter_count
        .checked_add(1)
        .expect("external get_start shared waiter count overflow");
}

fn release_external_get_start_waiter(
    inner: &client_kv_api::ClientKvApiInner,
    shared_op: &Arc<ExternalGetStartSharedOp>,
) {
    let should_remove = {
        let mut state = shared_op.state.lock();
        assert!(
            state.waiter_count > 0,
            "external get_start shared waiter count underflow"
        );
        state.waiter_count -= 1;
        state.waiter_count == 0
    };
    if should_remove {
        inner
            .external_get_start_by_key
            .remove_if(&shared_op.dedup_key, |_, op| {
                if !Arc::ptr_eq(op, shared_op) {
                    return false;
                }
                op.state.lock().waiter_count == 0
            });
    }
}

fn publish_external_get_start_failed(shared_op: &Arc<ExternalGetStartSharedOp>, err: &KvError) {
    let (error_code, error_json) = external_get_start_error_parts(err);
    {
        let mut state = shared_op.state.lock();
        state.phase = ExternalGetStartSharedPhase::Failed {
            error_code,
            error_json,
        };
    }
    shared_op.notify.notify_waiters();
}

fn publish_external_get_start_ready(
    shared_op: &Arc<ExternalGetStartSharedOp>,
    keys: Vec<String>,
    transfer_result: KvResult<ExternalGetStartTransferOutput>,
) {
    let phase = match transfer_result {
        Ok(transfer_results) => {
            if transfer_results.len() != keys.len() {
                let err = KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "external shared get_start transfer result length mismatch: expected={} got={}",
                        keys.len(),
                        transfer_results.len()
                    ),
                });
                let (error_code, error_json) = external_get_start_error_parts(&err);
                ExternalGetStartSharedPhase::Failed {
                    error_code,
                    error_json,
                }
            } else {
                let items = keys
                    .iter()
                    .zip(transfer_results.into_iter())
                    .map(|(_key, item_result)| match item_result {
                        Ok(Some((memholder, _))) => {
                            ExternalGetStartSharedItemResult::Hit { memholder }
                        }
                        Ok(None) => ExternalGetStartSharedItemResult::Miss,
                        Err(err) => {
                            let (error_code, error_json) = external_get_start_error_parts(&err);
                            ExternalGetStartSharedItemResult::Error {
                                error_code,
                                error_json,
                            }
                        }
                    })
                    .collect::<Vec<_>>();
                let prefix = match {
                    let state = shared_op.state.lock();
                    match &state.phase {
                        ExternalGetStartSharedPhase::Running { prefix, .. }
                        | ExternalGetStartSharedPhase::Ready { prefix, .. } => Ok(prefix.clone()),
                        ExternalGetStartSharedPhase::Starting
                        | ExternalGetStartSharedPhase::Failed { .. } => Err(KvError::Api(
                            ApiError::Unknown {
                                detail:
                                    "external shared get_start transfer completed before prefix was published"
                                        .to_string(),
                            },
                        )),
                    }
                } {
                    Ok(prefix) => prefix,
                    Err(err) => {
                        let (error_code, error_json) = external_get_start_error_parts(&err);
                        let mut state = shared_op.state.lock();
                        state.phase = ExternalGetStartSharedPhase::Failed {
                            error_code,
                            error_json,
                        };
                        shared_op.notify.notify_waiters();
                        return;
                    }
                };
                ExternalGetStartSharedPhase::Ready {
                    prefix,
                    keys,
                    items,
                }
            }
        }
        Err(err) => {
            let (error_code, error_json) = external_get_start_error_parts(&err);
            ExternalGetStartSharedPhase::Failed {
                error_code,
                error_json,
            }
        }
    };
    {
        let mut state = shared_op.state.lock();
        state.phase = phase;
    }
    shared_op.notify.notify_waiters();
}

async fn wait_external_get_start_prefix(
    shared_op: Arc<ExternalGetStartSharedOp>,
) -> KvResult<ExternalGetStartPrefixResult> {
    loop {
        let notified = shared_op.notify.notified();
        futures::pin_mut!(notified);
        let should_wait = {
            let state = shared_op.state.lock();
            match &state.phase {
                ExternalGetStartSharedPhase::Starting => {
                    notified.as_mut().enable();
                    true
                }
                ExternalGetStartSharedPhase::Running { prefix, .. }
                | ExternalGetStartSharedPhase::Ready { prefix, .. } => {
                    return Ok(prefix.clone());
                }
                ExternalGetStartSharedPhase::Failed {
                    error_code,
                    error_json,
                } => return Err(KvError::from_json(*error_code, error_json)),
            }
        };
        if should_wait {
            notified.await;
        }
    }
}

async fn wait_external_get_start_transfer(
    shared_op: Arc<ExternalGetStartSharedOp>,
) -> KvResult<(Vec<String>, Vec<ExternalGetStartSharedItemResult>)> {
    loop {
        let notified = shared_op.notify.notified();
        futures::pin_mut!(notified);
        let should_wait = {
            let state = shared_op.state.lock();
            match &state.phase {
                ExternalGetStartSharedPhase::Starting
                | ExternalGetStartSharedPhase::Running { .. } => {
                    notified.as_mut().enable();
                    true
                }
                ExternalGetStartSharedPhase::Ready { keys, items, .. } => {
                    return Ok((keys.clone(), items.clone()));
                }
                ExternalGetStartSharedPhase::Failed {
                    error_code,
                    error_json,
                } => return Err(KvError::from_json(*error_code, error_json)),
            }
        };
        if should_wait {
            notified.await;
        }
    }
}

async fn prepare_external_get_start_shared_op(
    inner: &client_kv_api::ClientKvApiInner,
    req: &ExternalBatchGetStartReq,
    group_lens: &[usize],
) -> KvResult<(
    ExternalGetStartPrefixResult,
    Vec<String>,
    Vec<ExternalGetStartOwnerItem>,
)> {
    let local_memory_infos = inner.local_visible_mem_holders(&req.keys);
    if local_memory_infos.iter().all(Option::is_some) {
        let key_count = req.keys.len();
        let transfer_items = local_memory_infos
            .into_iter()
            .map(|memory_info| ExternalGetStartOwnerItem::Local {
                memory_info: memory_info.expect("all local-visible entries were checked"),
            })
            .collect();
        tracing::debug!(
            key_count,
            req_node_id = %req.req_node_id,
            "external_batch_get_start used all-local-visible batch fast path"
        );
        return Ok((
            ExternalGetStartPrefixResult {
                raw_prefix_hit_len: key_count,
                transferable_len: key_count,
                first_miss_index: None,
                first_error_kind: None,
            },
            req.keys.clone(),
            transfer_items,
        ));
    }

    let mut item_slots: Vec<Option<ExternalGetStartOwnerItem>> = local_memory_infos
        .into_iter()
        .map(|memory_info| {
            memory_info.map(|memory_info| ExternalGetStartOwnerItem::Local { memory_info })
        })
        .collect();
    let (missing_indices, missing_keys) =
        collect_external_get_start_missing(&req.keys, &item_slots);

    if !missing_keys.is_empty() {
        let start_resp = batch_get_start_with_reserve_pressure_retry(inner, &missing_keys).await?;
        for ((idx, key), item) in missing_indices
            .into_iter()
            .zip(missing_keys.into_iter())
            .zip(start_resp.items.into_iter())
        {
            item_slots[idx] = Some(ExternalGetStartOwnerItem::Started { key, item });
        }
    }

    let items = item_slots
        .into_iter()
        .map(|item| {
            item.ok_or_else(|| {
                KvError::Api(ApiError::Unknown {
                    detail: "external_batch_get_start item slot was not populated".to_string(),
                })
            })
        })
        .collect::<KvResult<Vec<_>>>()?;
    let item_codes = items
        .iter()
        .map(|item| match item {
            ExternalGetStartOwnerItem::Local { .. } => (OK, String::new()),
            ExternalGetStartOwnerItem::Started { item, .. } => {
                (item.error_code, item.error_json.clone())
            }
        })
        .collect::<Vec<_>>();
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

    let revoke_get_ids = items
        .iter()
        .skip(transferable_len)
        .filter_map(|item| match item {
            ExternalGetStartOwnerItem::Started { item, .. } if item.error_code == OK => {
                Some(item.get_id)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    if !revoke_get_ids.is_empty() {
        inner.batch_get_revoke(revoke_get_ids).await?;
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

        let group_lens =
            normalize_external_get_start_group_lens(req.keys.len(), req.atomic_group_lens.clone())?;
        let dedup_key = ExternalGetStartDedupKey {
            keys: req.keys.clone(),
            atomic_group_lens: group_lens.clone(),
            prefix_best_effort: req.prefix_best_effort,
        };
        let (shared_op, is_creator) = match inner.external_get_start_by_key.entry(dedup_key.clone())
        {
            Entry::Occupied(entry) => {
                let shared_op = entry.get().clone();
                register_external_get_start_waiter(&shared_op);
                (shared_op, false)
            }
            Entry::Vacant(entry) => {
                let shared_op = Arc::new(ExternalGetStartSharedOp::new(
                    dedup_key,
                    req.transfer_concurrency,
                ));
                entry.insert(shared_op.clone());
                (shared_op, true)
            }
        };

        if is_creator {
            match prepare_external_get_start_shared_op(inner, &req, &group_lens).await {
                Ok((prefix, transfer_keys, transfer_items)) => {
                    let inline_memory_infos = (transfer_items.len() == req.keys.len())
                        .then(|| collect_all_local_external_get_start_infos(&transfer_items))
                        .flatten();
                    let inline_local = inline_memory_infos.is_some();
                    if let Some(memory_infos) = inline_memory_infos {
                        shared_op
                            .inline_local_memory_infos
                            .set(memory_infos)
                            .unwrap_or_else(|_| {
                                panic!("external get_start inline plan initialized twice")
                            });
                    }
                    {
                        let mut state = shared_op.state.lock();
                        state.phase = ExternalGetStartSharedPhase::Running {
                            prefix: prefix.clone(),
                            keys: transfer_keys.clone(),
                        };
                    }
                    shared_op.notify.notify_waiters();

                    if !inline_local {
                        let transfer_shared_op = shared_op.clone();
                        let transfer_concurrency = shared_op.transfer_concurrency;
                        let spawn_view = inner.view.clone_view();
                        let transfer_view = spawn_view.clone();
                        spawn_view.spawn("external_get_start_transfer", async move {
                            let transfer_result = finish_external_get_start_transfer(
                                transfer_view,
                                transfer_items,
                                transfer_concurrency,
                            )
                            .await;
                            publish_external_get_start_ready(
                                &transfer_shared_op,
                                transfer_keys,
                                transfer_result,
                            );
                        });
                    }
                }
                Err(err) => {
                    publish_external_get_start_failed(&shared_op, &err);
                    release_external_get_start_waiter(inner, &shared_op);
                    return Err(err);
                }
            }
        }

        let prefix = match wait_external_get_start_prefix(shared_op.clone()).await {
            Ok(prefix) => prefix,
            Err(err) => {
                release_external_get_start_waiter(inner, &shared_op);
                return Err(err);
            }
        };
        let handle = inner
            .next_external_get_start_handle
            .fetch_add(1, Ordering::Relaxed);
        let transfer_plan = if let Some(memory_infos) = shared_op.inline_local_memory_infos.get() {
            assert_eq!(
                memory_infos.len(),
                req.keys.len(),
                "inline local get_start plan must cover the full request"
            );
            let items = memory_infos
                .iter()
                .map(|memory_info| ExternalBatchGetItemResp {
                    error_code: OK,
                    error_json: String::new(),
                    external_memholder_info: Some(
                        inner.install_external_get_holding(&req.req_node_id, memory_info.clone()),
                    ),
                })
                .collect();
            release_external_get_start_waiter(inner, &shared_op);
            ExternalBatchGetStartTransferPlan::InlineLocal { items }
        } else {
            inner.external_get_start_registry.insert(
                handle,
                Arc::new(ExternalGetStartEntry {
                    req_node_id: req.req_node_id.clone(),
                    shared_op,
                }),
            );
            ExternalBatchGetStartTransferPlan::OwnerRpc
        };

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

        let Some((_handle, entry)) = inner.external_get_start_registry.remove(&req.handle) else {
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
        let shared_op = entry.shared_op.clone();
        let result = match wait_external_get_start_transfer(shared_op.clone()).await {
            Ok((keys, shared_items)) => {
                if shared_items.len() != keys.len() {
                    Err(KvError::Api(ApiError::Unknown {
                        detail: format!(
                            "external_batch_get_transfer shared result length mismatch: expected={} got={}",
                            keys.len(),
                            shared_items.len()
                        ),
                    }))
                } else {
                    let mut items = Vec::with_capacity(shared_items.len());
                    for (key, item_result) in keys.iter().zip(shared_items.into_iter()) {
                        match item_result {
                            ExternalGetStartSharedItemResult::Hit { memholder } => {
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
                            ExternalGetStartSharedItemResult::Miss => {
                                items.push(ExternalBatchGetItemResp {
                                    error_code: codes_api::API_KEY_NOT_FOUND,
                                    error_json: format!("Key not found: {}", key),
                                    external_memholder_info: None,
                                });
                            }
                            ExternalGetStartSharedItemResult::Error {
                                error_code,
                                error_json,
                            } => {
                                items.push(ExternalBatchGetItemResp {
                                    error_code,
                                    error_json,
                                    external_memholder_info: None,
                                });
                            }
                        }
                    }
                    Ok(ExternalBatchGetTransferResp {
                        items,
                        error_code: OK,
                        error_json: String::new(),
                    })
                }
            }
            Err(err) => Err(err),
        };
        release_external_get_start_waiter(inner, &shared_op);
        result
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
                    release_external_get_start_waiter(inner, &entry.shared_op);
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
        crate::rpcresp_kvresult_convert::try_from_code(
            put_start_resp.error_code,
            put_start_resp.error_json.clone(),
        )?;
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
        let replica_target =
            put_start_resp
                .replica_target
                .as_ref()
                .map(|target| ReplicaTaskTarget {
                    node_id: target.node_id.clone(),
                    target_offset: target.target_addr - target.target_base_addr,
                    target_base_addr: target.target_base_addr,
                    len: target.len,
                });
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
                make_replica_task: req.make_replica_task && replica_target.is_some(),
                preferred_sub_cluster: req.preferred_sub_cluster.clone(),
                replica_target,
                local_reserve_slot: None,
                local_reserve_slot_size: None,
                atomic_group: None,
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

        let mut reserved_keys = Vec::with_capacity(req.items.len());
        for item in &req.items {
            if let Err(err) = inner.reserve_external_local_first_put_key(
                &item.key,
                item.reject_if_inflight_same_key,
                item.reject_if_exist_same_key,
            ) {
                external_local_first_release_keys(inner, &reserved_keys);
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
            reserved_keys.push(item.key.clone());
        }

        let slot_lease = match inner
            .owner_claim_local_reserve_slot_lease(value_len, req.items.len())
            .await
        {
            Ok(slot_lease) => slot_lease,
            Err(err) => {
                external_local_first_release_keys(inner, &reserved_keys);
                return Err(err);
            }
        };
        let self_node_id = inner.view.cluster_manager().get_self_info().id.clone();
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
        let mut items = Vec::with_capacity(req.items.len());
        for (idx, (req_item, slot_ref)) in req
            .items
            .into_iter()
            .zip(slot_lease.slots.into_iter())
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
                    replica_target: None,
                    local_reserve_slot: Some(slot_ref.clone()),
                    local_reserve_slot_size: Some(slot_lease.slot_size),
                    atomic_group: atomic_groups[idx].clone(),
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

        let pending_ctx = inner
            .external_pending_puts
            .get(&(req.key.clone(), put_id.0, put_id.1))
            .map(|ctx| ctx.clone());
        let replica_target = pending_ctx
            .as_ref()
            .and_then(|ctx| ctx.replica_target.clone());
        let admitted_replica_task = pending_ctx
            .as_ref()
            .map(|ctx| ctx.make_replica_task)
            .unwrap_or(false);
        let self_node_id = inner.view.cluster_manager().get_self_info().id.clone();
        let req_remote_target = req
            .peer_id
            .as_deref()
            .is_some_and(|peer| peer != self_node_id.as_str());
        let has_remote_target = pending_ctx
            .as_ref()
            .and_then(|ctx| ctx.peer_id.as_ref())
            .is_some();
        if pending_ctx.is_some() && req_remote_target != has_remote_target {
            let err = KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "external_put_transfer_end peer_id mismatches pending ctx: key={} put_id=({},{}) req_remote_target={} ctx_remote_target={}",
                    req.key, put_id.0, put_id.1, req_remote_target, has_remote_target
                ),
            });
            if let Err(revoke_err) = inner.put_revoke(&req.key, put_id).await {
                tracing::warn!(
                    "external_put_transfer_end put_revoke failed after peer_id mismatch: key={} put_id=({},{}) err={}",
                    req.key,
                    put_id.0,
                    put_id.1,
                    revoke_err
                );
            }
            inner
                .external_pending_puts
                .invalidate(&(req.key.clone(), put_id.0, put_id.1));
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
            pending_ctx.as_ref(),
            req.src_offset,
            req.len,
        ) {
            Ok(publish) => publish,
            Err(err) => {
                if let Err(revoke_err) = inner.put_revoke(&req.key, put_id).await {
                    tracing::warn!(
                        "external_put_transfer_end put_revoke failed after local cache publish precheck error: key={} put_id=({},{}) err={}",
                        req.key,
                        put_id.0,
                        put_id.1,
                        revoke_err
                    );
                }
                inner
                    .external_pending_puts
                    .invalidate(&(req.key.clone(), put_id.0, put_id.1));
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
                inner
                    .external_pending_puts
                    .invalidate(&(req.key.clone(), put_id.0, put_id.1));
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
            inner
                .external_pending_puts
                .invalidate(&(req.key.clone(), put_id.0, put_id.1));
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
        if admitted_replica_task && replica_target.is_some() {
            if let Err(err) = inner
                .make_replica_task(
                    &req.key,
                    put_id,
                    replica_target
                        .clone()
                        .expect("make_replica_task requires pre-reserved replica target"),
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
            replica_target: Option<ReplicaTaskTarget>,
        }

        let mut results: Vec<Option<ExternalBatchPutTransferEndItemResp>> =
            (0..req.items.len()).map(|_| None).collect();
        let mut done_pending = Vec::new();
        let mut revoke_pending = Vec::new();

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

            let pending_ctx = inner
                .external_pending_puts
                .get(&(item.key.clone(), put_id.0, put_id.1))
                .map(|ctx| ctx.clone());
            if let Some(ctx) = pending_ctx.as_ref() {
                if ctx.local_reserve_slot.is_some() {
                    if item.peer_id.is_some() {
                        let err = KvError::Api(ApiError::InvalidArgument {
                            detail: format!(
                                "external_batch_put_transfer_end local-first item must be local target: key={} put_id=({},{})",
                                item.key, put_id.0, put_id.1
                            ),
                        });
                        results[idx] = Some(ExternalBatchPutTransferEndItemResp::from_error(&err));
                        inner.external_pending_puts.invalidate(&(
                            item.key.clone(),
                            put_id.0,
                            put_id.1,
                        ));
                        inner.release_external_local_first_put_key(&item.key);
                        continue;
                    }
                    match commit_external_local_first_pending(
                        inner,
                        &item.key,
                        put_id,
                        ctx,
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
                            inner.release_external_local_first_put_key(&item.key);
                            spawn_external_local_first_publish(
                                inner,
                                item.key.clone(),
                                put_id,
                                item.lease_id,
                                committed_slot,
                                ctx.make_replica_task,
                                ctx.preferred_sub_cluster.clone(),
                                ctx.atomic_group.clone(),
                            );
                            results[idx] = Some(ExternalBatchPutTransferEndItemResp {
                                error_code: OK,
                                error_json: String::new(),
                            });
                        }
                        Err(err) => {
                            inner.external_pending_puts.invalidate(&(
                                item.key.clone(),
                                put_id.0,
                                put_id.1,
                            ));
                            inner.release_external_local_first_put_key(&item.key);
                            results[idx] =
                                Some(ExternalBatchPutTransferEndItemResp::from_error(&err));
                        }
                    }
                    continue;
                }
            }
            let has_remote_append = pending_ctx
                .as_ref()
                .and_then(|ctx| ctx.peer_id.as_ref())
                .is_some();
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
                inner
                    .external_pending_puts
                    .invalidate(&(item.key.clone(), put_id.0, put_id.1));
                continue;
            }
            let local_cache_publish = match local_committed_cache_publish(
                "external_batch_put_transfer_end",
                &item.key,
                put_id,
                pending_ctx.as_ref(),
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
                    inner
                        .external_pending_puts
                        .invalidate(&(item.key.clone(), put_id.0, put_id.1));
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
                make_replica_task: pending_ctx
                    .as_ref()
                    .map(|ctx| ctx.make_replica_task)
                    .unwrap_or(false),
                replica_target: pending_ctx
                    .as_ref()
                    .and_then(|ctx| ctx.replica_target.clone()),
            });
        }

        if !revoke_pending.is_empty() {
            if let Err(err) = inner.batch_put_revoke(revoke_pending).await {
                tracing::warn!(
                    "external_batch_put_transfer_end batch_put_revoke failed after precheck errors: {}",
                    err
                );
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
            inner.external_pending_puts.invalidate(&(
                pending.key.clone(),
                pending.put_id.0,
                pending.put_id.1,
            ));
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
            if pending.make_replica_task {
                let target = pending
                    .replica_target
                    .clone()
                    .expect("make_replica_task requires pre-reserved replica target");
                if let Err(err) = inner
                    .make_replica_task(&pending.key, pending.put_id, target)
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
        let pending_ctx = inner
            .external_pending_puts
            .get(&(req.key.clone(), put_id.0, put_id.1))
            .map(|ctx| ctx.clone());
        let has_remote_append = pending_ctx
            .as_ref()
            .and_then(|ctx| ctx.peer_id.as_ref())
            .is_some();
        if req.remote_target || has_remote_append {
            let err = KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "external_put_commit remote primary target is no longer supported: key={} put_id=({},{}) req_remote_target={} ctx_remote_target={}",
                    req.key, put_id.0, put_id.1, req.remote_target, has_remote_append
                ),
            });
            if let Err(revoke_err) = inner.put_revoke(&req.key, put_id).await {
                tracing::warn!(
                    "external_put_commit put_revoke failed after remote primary target: key={} put_id=({},{}) err={}",
                    req.key,
                    put_id.0,
                    put_id.1,
                    revoke_err
                );
            }
            inner
                .external_pending_puts
                .invalidate(&(req.key.clone(), put_id.0, put_id.1));
            return Ok(ExternalPutCommitResp::from_error(&err));
        }
        let replica_target = pending_ctx
            .as_ref()
            .and_then(|ctx| ctx.replica_target.clone());
        let admitted_replica_task = pending_ctx
            .as_ref()
            .map(|ctx| ctx.make_replica_task)
            .unwrap_or(false);

        let end_started_at = Instant::now();
        let local_cache_publish = match local_committed_cache_publish(
            "external_put_commit",
            &req.key,
            put_id,
            pending_ctx.as_ref(),
            req.src_offset,
            req.len,
        ) {
            Ok(publish) => publish,
            Err(err) => {
                if let Err(revoke_err) = inner.put_revoke(&req.key, put_id).await {
                    tracing::warn!(
                        "external_put_commit put_revoke failed after local cache publish precheck error: key={} put_id=({},{}) err={}",
                        req.key,
                        put_id.0,
                        put_id.1,
                        revoke_err
                    );
                }
                inner
                    .external_pending_puts
                    .invalidate(&(req.key.clone(), put_id.0, put_id.1));
                return Ok(ExternalPutCommitResp::from_error(&err));
            }
        };
        let put_end = match inner
            .put_end_with_local_cache_publish(&req.key, put_id, req.lease_id, true)
            .await
        {
            Ok(end) => end,
            Err(e) => {
                inner
                    .external_pending_puts
                    .invalidate(&(req.key.clone(), put_id.0, put_id.1));
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
            inner
                .external_pending_puts
                .invalidate(&(req.key.clone(), put_id.0, put_id.1));
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
        if admitted_replica_task && replica_target.is_some() {
            if let Err(err) = inner
                .make_replica_task(
                    &req.key,
                    put_id,
                    replica_target
                        .clone()
                        .expect("make_replica_task requires pre-reserved replica target"),
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
            replica_target: Option<ReplicaTaskTarget>,
        }

        let mut results: Vec<Option<ExternalBatchPutCommitItemResp>> =
            (0..req.items.len()).map(|_| None).collect();
        let mut done_pending = Vec::new();
        let mut revoke_pending = Vec::new();

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
            let pending_ctx = inner
                .external_pending_puts
                .get(&(item.key.clone(), put_id.0, put_id.1))
                .map(|ctx| ctx.clone());
            if let Some(ctx) = pending_ctx.as_ref() {
                if ctx.local_reserve_slot.is_some() {
                    if item.remote_target {
                        let err = KvError::Api(ApiError::InvalidArgument {
                            detail: format!(
                                "external_batch_put_commit local-first item must be local target: key={} put_id=({},{})",
                                item.key, put_id.0, put_id.1
                            ),
                        });
                        results[idx] = Some(ExternalBatchPutCommitItemResp::from_error(&err));
                        inner.external_pending_puts.invalidate(&(
                            item.key.clone(),
                            put_id.0,
                            put_id.1,
                        ));
                        inner.release_external_local_first_put_key(&item.key);
                        continue;
                    }
                    match commit_external_local_first_pending(
                        inner,
                        &item.key,
                        put_id,
                        ctx,
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
                            inner.release_external_local_first_put_key(&item.key);
                            spawn_external_local_first_publish(
                                inner,
                                item.key.clone(),
                                put_id,
                                item.lease_id,
                                committed_slot,
                                ctx.make_replica_task,
                                ctx.preferred_sub_cluster.clone(),
                                ctx.atomic_group.clone(),
                            );
                            results[idx] = Some(ExternalBatchPutCommitItemResp {
                                error_code: OK,
                                error_json: String::new(),
                            });
                        }
                        Err(err) => {
                            inner.external_pending_puts.invalidate(&(
                                item.key.clone(),
                                put_id.0,
                                put_id.1,
                            ));
                            inner.release_external_local_first_put_key(&item.key);
                            results[idx] = Some(ExternalBatchPutCommitItemResp::from_error(&err));
                        }
                    }
                    continue;
                }
            }
            let has_remote_append = pending_ctx
                .as_ref()
                .and_then(|ctx| ctx.peer_id.as_ref())
                .is_some();
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
                inner
                    .external_pending_puts
                    .invalidate(&(item.key.clone(), put_id.0, put_id.1));
                continue;
            };
            let local_cache_publish = match local_committed_cache_publish(
                "external_batch_put_commit",
                &item.key,
                put_id,
                pending_ctx.as_ref(),
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
                    inner
                        .external_pending_puts
                        .invalidate(&(item.key.clone(), put_id.0, put_id.1));
                    continue;
                }
            };
            done_pending.push(DonePending {
                idx,
                key: item.key.clone(),
                put_id,
                lease_id: item.lease_id,
                local_cache_publish,
                make_replica_task: pending_ctx
                    .as_ref()
                    .map(|ctx| ctx.make_replica_task)
                    .unwrap_or(false),
                replica_target: pending_ctx
                    .as_ref()
                    .and_then(|ctx| ctx.replica_target.clone()),
            });
        }

        if !revoke_pending.is_empty() {
            if let Err(err) = inner.batch_put_revoke(revoke_pending).await {
                tracing::warn!(
                    "external_batch_put_commit batch_put_revoke failed after local cache publish precheck errors: {}",
                    err
                );
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
            inner.external_pending_puts.invalidate(&(
                pending.key.clone(),
                pending.put_id.0,
                pending.put_id.1,
            ));
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
            if pending.make_replica_task {
                let target = pending
                    .replica_target
                    .clone()
                    .expect("make_replica_task requires pre-reserved replica target");
                if let Err(err) = inner
                    .make_replica_task(&pending.key, pending.put_id, target)
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
        let pending_ctx = inner
            .external_pending_puts
            .get(&(req.key.clone(), put_id.0, put_id.1))
            .map(|ctx| ctx.clone());
        inner
            .external_pending_puts
            .invalidate(&(req.key.clone(), put_id.0, put_id.1));
        if let Some(ctx) = pending_ctx {
            if let (Some(slot_ref), Some(slot_size)) =
                (ctx.local_reserve_slot, ctx.local_reserve_slot_size)
            {
                inner.release_external_local_first_put_key(&req.key);
                let lease = client_kv_api::OwnerLocalReserveSlotLease {
                    value_len: ctx.len,
                    slot_size,
                    slots: vec![slot_ref],
                };
                return match inner.owner_release_local_reserve_slot_lease(lease).await {
                    Ok(_) => Ok(ExternalPutRevokeResp {
                        error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
                        error_json: String::new(),
                    }),
                    Err(e) => Ok(ExternalPutRevokeResp::from_error(&e)),
                };
            }
        }
        match inner.put_revoke(&req.key, put_id).await {
            Ok(_) => Ok(ExternalPutRevokeResp {
                error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
                error_json: String::new(),
            }),
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
