use super::{KvReplicaBacking, MasterKvRouterView, NodeValueReplicaDesc};
use crate::cluster_manager::{NodeID, NodeIDString};
use crate::master_kv_router::msg_pack::{
    BatchOwnerReclaimReq, OwnerReclaimBacking, OwnerReclaimItem, OwnerReclaimItemResp,
    OwnerReclaimItemState, OwnerReclaimPhase, OwnerReclaimReason,
};
use crate::memholder::MemholderManagerTrait;
use crate::p2p::msg_pack::{MIN_EXPLICIT_RPC_TIMEOUT_SECS, MsgPack, RPCCaller};
use crate::rpcresp_kvresult_convert::msg_and_error::OK;
use limit_thirdparty::tokio;
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

const OWNER_RECLAIM_RPC_TIMEOUT: Duration = Duration::from_secs(MIN_EXPLICIT_RPC_TIMEOUT_SECS);
const OWNER_RECLAIM_MAX_BATCH: usize = 256;
const OWNER_RECLAIM_MERGE_WINDOW: Duration = Duration::from_millis(5);
const EVICTION_RECLAIM_RETRY_INITIAL: Duration = Duration::from_millis(100);
const EVICTION_RECLAIM_RETRY_MAX: Duration = Duration::from_secs(1);
const EVICTION_RECLAIM_MAX_RETRY_COUNT: u32 = 5;

fn should_restore_after_retry(retry_count: u32) -> bool {
    retry_count >= EVICTION_RECLAIM_MAX_RETRY_COUNT
}

fn eviction_reclaim_retry_delay(retry_count: u32) -> Duration {
    let multiplier = 1u32 << retry_count.saturating_sub(1).min(16);
    EVICTION_RECLAIM_RETRY_INITIAL
        .saturating_mul(multiplier)
        .min(EVICTION_RECLAIM_RETRY_MAX)
}

#[cfg(test)]
mod timeout_contract_tests {
    use super::*;
    use crate::p2p::msg_pack::validate_explicit_rpc_timeout;

    #[test]
    fn owner_reclaim_timeout_satisfies_rpc_contract() {
        validate_explicit_rpc_timeout(Some(OWNER_RECLAIM_RPC_TIMEOUT)).unwrap();
    }

    #[test]
    fn eviction_reclaim_retry_restoration_is_bounded() {
        assert!(!should_restore_after_retry(
            EVICTION_RECLAIM_MAX_RETRY_COUNT - 1
        ));
        assert!(should_restore_after_retry(EVICTION_RECLAIM_MAX_RETRY_COUNT));
        assert!(should_restore_after_retry(u32::MAX));
    }

    #[test]
    fn eviction_reclaim_retry_paces_restore_view_holders() {
        assert_eq!(eviction_reclaim_retry_delay(1), Duration::from_millis(100));
        assert_eq!(eviction_reclaim_retry_delay(2), Duration::from_millis(200));
        assert_eq!(eviction_reclaim_retry_delay(3), Duration::from_millis(400));
        assert_eq!(eviction_reclaim_retry_delay(4), Duration::from_millis(800));
        assert_eq!(
            eviction_reclaim_retry_delay(u32::MAX),
            Duration::from_secs(1)
        );
    }
}

#[derive(Clone, Debug)]
pub(crate) struct EvictionReclaimRequest {
    pub owner_node_id: NodeIDString,
    pub key: String,
    pub desc: NodeValueReplicaDesc,
    pub retry_count: u32,
}

fn route_item(
    view: &MasterKvRouterView,
    owner_node_id: &NodeID,
    key: &str,
    expected_put_id: Option<(u64, u32)>,
    required_slot_size: Option<u64>,
    reason: OwnerReclaimReason,
    epoch: u64,
) -> Option<OwnerReclaimItem> {
    let route = view.master_kv_router().inner().kv_routes.get(key)?.clone();
    if expected_put_id.is_some_and(|put_id| put_id != route.put_id) || route.lease_id.is_some() {
        return None;
    }
    let replicas = route.nodes_replicas.read();
    let target = replicas.get(owner_node_id)?;
    if target.tomb_tag.is_tomb() {
        return None;
    }
    let backing = match &target.backing {
        KvReplicaBacking::Allocation(_) if required_slot_size.is_none() => {
            if target.owner_local_indexed {
                OwnerReclaimBacking::Allocation
            } else {
                OwnerReclaimBacking::UnindexedAllocation
            }
        }
        KvReplicaBacking::Allocation(_) => return None,
        KvReplicaBacking::CommittedSlot(slot)
            if slot.owner_node_id == *owner_node_id
                && required_slot_size.map_or(true, |slot_size| slot.slot_size == slot_size) =>
        {
            OwnerReclaimBacking::CommittedSlot {
                grant_id: slot.grant_id,
                slot_index: slot.slot_index,
                slot_size: slot.slot_size,
            }
        }
        KvReplicaBacking::CommittedSlot(_) => return None,
    };
    let has_other_live_replica = replicas
        .iter()
        .any(|(node_id, replica)| node_id != owner_node_id && !replica.tomb_tag.is_tomb());
    drop(replicas);
    if reason == OwnerReclaimReason::Reserve {
        let normally_recoverable = has_other_live_replica
            && (view
                .master_kv_router()
                .owner_cache_allows_unrecoverable_eviction(owner_node_id.as_ref())
                || {
                    let desc = NodeValueReplicaDesc {
                        weight_bytes: 0,
                        put_id: route.put_id,
                    };
                    view.master_kv_router()
                        .eviction_cache_entry_recoverable_status(owner_node_id.as_ref(), key, &desc)
                        == super::RecoverableReplicaStatus::Recoverable
                });
        if !normally_recoverable
            && !view
                .master_kv_router()
                .owner_cache_allows_unrecoverable_reserve_pressure_eviction(owner_node_id.as_ref())
        {
            return None;
        }
    }
    Some(OwnerReclaimItem {
        key: key.to_string(),
        put_id: route.put_id,
        epoch,
        backing,
        reason,
    })
}

fn master_has_holder(view: &MasterKvRouterView, key: &str) -> bool {
    view.master_kv_router()
        .inner()
        .get_holding
        .inner()
        .iter()
        .any(|entry| entry.value().key == key)
}

fn item_still_valid(view: &MasterKvRouterView, owner: &NodeID, item: &OwnerReclaimItem) -> bool {
    if !view
        .master_kv_router()
        .inner()
        .key_activity
        .reclaim_matches(item)
    {
        return false;
    }
    route_item(
        view,
        owner,
        &item.key,
        Some(item.put_id),
        match &item.backing {
            OwnerReclaimBacking::Allocation | OwnerReclaimBacking::UnindexedAllocation => None,
            OwnerReclaimBacking::CommittedSlot { slot_size, .. } => Some(*slot_size),
        },
        item.reason,
        item.epoch,
    )
    .is_some_and(|current| current.backing == item.backing && current.reason == item.reason)
}

async fn call_owner_phase(
    view: &MasterKvRouterView,
    owner: &NodeID,
    phase: OwnerReclaimPhase,
    items: Vec<OwnerReclaimItem>,
) -> Result<Vec<OwnerReclaimItemResp>, String> {
    if items.is_empty() {
        return Ok(Vec::new());
    }
    debug_assert!(
        items
            .iter()
            .all(|item| item.backing != OwnerReclaimBacking::UnindexedAllocation)
    );
    let caller = RPCCaller::<BatchOwnerReclaimReq>::new();
    caller.regist(view.p2p_module());
    let resp = caller
        .call(
            view.p2p_module(),
            owner.clone(),
            MsgPack {
                serialize_part: BatchOwnerReclaimReq {
                    phase,
                    items: items.clone(),
                },
                raw_bytes: Vec::new(),
            },
            Some(OWNER_RECLAIM_RPC_TIMEOUT),
            1,
        )
        .await
        .map_err(|err| format!("{err:?}"))?;
    if resp.serialize_part.error_code != OK {
        return Err(format!(
            "code={} error={}",
            resp.serialize_part.error_code, resp.serialize_part.error_json
        ));
    }
    if resp.serialize_part.items.len() != items.len() {
        return Err(format!(
            "owner reclaim response length mismatch: phase={phase:?} expected={} got={}",
            items.len(),
            resp.serialize_part.items.len()
        ));
    }
    for (request, response) in items.iter().zip(resp.serialize_part.items.iter()) {
        if request.key != response.key || request.epoch != response.epoch {
            return Err(format!(
                "owner reclaim response identity mismatch: phase={phase:?} request=({}, {}) response=({}, {})",
                request.key, request.epoch, response.key, response.epoch
            ));
        }
    }
    Ok(resp.serialize_part.items)
}

fn clear_master_fence(view: &MasterKvRouterView, item: &OwnerReclaimItem) {
    let cleared = view
        .master_kv_router()
        .inner()
        .key_activity
        .clear_reclaim(item);
    if !cleared {
        tracing::warn!(
            "owner reclaim master fence did not match during cleanup: key={} epoch={}",
            item.key,
            item.epoch
        );
    }
}

async fn abort_prepared(
    view: &MasterKvRouterView,
    owner: &NodeID,
    items: Vec<OwnerReclaimItem>,
) -> Vec<OwnerReclaimItem> {
    if items.is_empty() {
        return Vec::new();
    }
    match call_owner_phase(view, owner, OwnerReclaimPhase::Abort, items.clone()).await {
        Ok(responses) => {
            let mut already_committed = Vec::new();
            for (item, response) in items.into_iter().zip(responses.into_iter()) {
                match response.state {
                    OwnerReclaimItemState::Committed => already_committed.push(item),
                    OwnerReclaimItemState::Aborted
                    | OwnerReclaimItemState::Stale
                    | OwnerReclaimItemState::Finalized => clear_master_fence(view, &item),
                    state => tracing::warn!(
                        "owner reclaim abort returned unresolved state: key={} epoch={} state={:?} detail={}",
                        item.key,
                        item.epoch,
                        state,
                        response.detail
                    ),
                }
            }
            already_committed
        }
        Err(err) => {
            tracing::warn!(
                "owner reclaim abort RPC failed; retaining master fences: owner={} keys={} err={}",
                owner,
                items.len(),
                err
            );
            spawn_abort_retry(view.clone(), owner.clone(), items);
            Vec::new()
        }
    }
}

fn spawn_abort_retry(view: MasterKvRouterView, owner: NodeID, items: Vec<OwnerReclaimItem>) {
    if items.is_empty() {
        return;
    }
    let spawn_view = view.clone();
    let _ = spawn_view.spawn("owner_reclaim_abort_retry", async move {
        let mut pending = items;
        let mut committed = Vec::new();
        let mut delay = Duration::from_millis(25);
        for _attempt in 1..=8 {
            tokio::time::sleep(delay).await;
            match call_owner_phase(&view, &owner, OwnerReclaimPhase::Abort, pending.clone()).await {
                Ok(responses) => {
                    let mut next = Vec::new();
                    for (item, response) in pending.into_iter().zip(responses.into_iter()) {
                        match response.state {
                            OwnerReclaimItemState::Committed => committed.push(item),
                            OwnerReclaimItemState::Aborted
                            | OwnerReclaimItemState::Stale
                            | OwnerReclaimItemState::Finalized => clear_master_fence(&view, &item),
                            _ => next.push(item),
                        }
                    }
                    pending = next;
                    if pending.is_empty() {
                        break;
                    }
                }
                Err(err) => tracing::warn!(
                    "owner reclaim abort retry failed: owner={} keys={} err={}",
                    owner,
                    pending.len(),
                    err
                ),
            }
            delay = (delay * 2).min(Duration::from_secs(1));
        }
        if !committed.is_empty() {
            let _ = finish_committed(&view, &owner, committed).await;
        }
        if !pending.is_empty() {
            tracing::error!(
                "owner reclaim abort retry exhausted; fences retained: owner={} keys={}",
                owner,
                pending.len()
            );
        }
    });
}

fn reclaim_backing_matches(replica: &super::KvRouteInfo, expected: &OwnerReclaimBacking) -> bool {
    match (&replica.backing, expected) {
        (KvReplicaBacking::Allocation(_), OwnerReclaimBacking::Allocation) => {
            replica.owner_local_indexed
        }
        (KvReplicaBacking::Allocation(_), OwnerReclaimBacking::UnindexedAllocation) => {
            !replica.owner_local_indexed
        }
        (
            KvReplicaBacking::CommittedSlot(slot),
            OwnerReclaimBacking::CommittedSlot {
                grant_id,
                slot_index,
                slot_size,
            },
        ) => {
            slot.grant_id == *grant_id
                && slot.slot_index == *slot_index
                && slot.slot_size == *slot_size
        }
        _ => false,
    }
}

fn remove_reclaimed_replica(
    view: &MasterKvRouterView,
    owner: &NodeID,
    item: &OwnerReclaimItem,
) -> bool {
    if !view
        .master_kv_router()
        .inner()
        .key_activity
        .reclaim_matches(item)
    {
        return false;
    }
    let _route_guard = view.master_kv_router().inner().route_lifetime_lock.lock();
    let Some(route) = view
        .master_kv_router()
        .inner()
        .kv_routes
        .get(&item.key)
        .map(|route| route.clone())
    else {
        return true;
    };
    if route.put_id != item.put_id {
        return true;
    }
    let removed = {
        let mut replicas = route.nodes_replicas.write();
        let Some(replica) = replicas.get(owner) else {
            return true;
        };
        if !reclaim_backing_matches(replica, &item.backing) {
            // This backing is no longer advertised by the fenced route version.
            return true;
        }
        replicas.remove(owner).is_some()
    };
    if !removed {
        return true;
    }

    if route.nodes_replicas.read().is_empty() {
        let route_removed = view
            .master_kv_router()
            .inner()
            .kv_routes
            .remove_if(&item.key, |_, current| current.put_id == item.put_id)
            .is_some();
        if route_removed && view.master_kv_router().prefix_index_enabled() {
            let view_task = view.clone();
            let key = item.key.clone();
            let put_id = item.put_id;
            let spawn_view = view.clone();
            let _ = spawn_view.spawn("owner_reclaim_remove_prefix", async move {
                let mut tree = view_task
                    .master_kv_router()
                    .inner()
                    .prefix_index
                    .write()
                    .await;
                tree.remove(&key, put_id);
            });
        }
    }
    if let Some(cache) = view
        .master_kv_router()
        .get_node_cache_controller(owner.as_ref())
    {
        let _ = cache.remove(&item.key);
    }
    view.master_kv_router()
        .remove_node_writeback_tier1_entry(owner.as_ref(), &item.key);
    true
}

fn finish_unindexed_allocations(
    view: &MasterKvRouterView,
    owner: &NodeID,
    items: Vec<OwnerReclaimItem>,
) -> u32 {
    let mut reclaimed = 0u32;
    for item in items {
        debug_assert_eq!(item.backing, OwnerReclaimBacking::UnindexedAllocation);
        if remove_reclaimed_replica(view, owner, &item) {
            clear_master_fence(view, &item);
            reclaimed = reclaimed.saturating_add(1);
        } else {
            tracing::error!(
                "unindexed allocation reclaim backing could not be removed from master route: owner={} key={} epoch={}",
                owner,
                item.key,
                item.epoch
            );
        }
    }
    reclaimed
}

fn partition_reclaim_coordination(
    items: Vec<OwnerReclaimItem>,
) -> (Vec<OwnerReclaimItem>, Vec<OwnerReclaimItem>) {
    items
        .into_iter()
        .partition(|item| item.backing == OwnerReclaimBacking::UnindexedAllocation)
}

async fn finish_committed(
    view: &MasterKvRouterView,
    owner: &NodeID,
    items: Vec<OwnerReclaimItem>,
) -> u32 {
    let mut removed = Vec::new();
    for item in items {
        if remove_reclaimed_replica(view, owner, &item) {
            removed.push(item);
        } else {
            tracing::error!(
                "owner reclaim backing could not be removed from master route: owner={} key={} epoch={}",
                owner,
                item.key,
                item.epoch
            );
        }
    }
    if removed.is_empty() {
        return 0;
    }
    match call_owner_phase(view, owner, OwnerReclaimPhase::Finalize, removed.clone()).await {
        Ok(responses) => {
            let mut finalized = 0u32;
            let mut retry = Vec::new();
            for (item, response) in removed.into_iter().zip(responses.into_iter()) {
                if response.state == OwnerReclaimItemState::Finalized {
                    clear_master_fence(view, &item);
                    finalized = finalized.saturating_add(1);
                } else {
                    tracing::warn!(
                        "owner reclaim finalize returned unresolved state: owner={} key={} epoch={} state={:?} detail={}",
                        owner,
                        item.key,
                        item.epoch,
                        response.state,
                        response.detail
                    );
                    retry.push(item);
                }
            }
            spawn_finalize_retry(view.clone(), owner.clone(), retry);
            finalized
        }
        Err(err) => {
            tracing::warn!(
                "owner reclaim finalize RPC failed; retaining both fences: owner={} keys={} err={}",
                owner,
                removed.len(),
                err
            );
            spawn_finalize_retry(view.clone(), owner.clone(), removed);
            0
        }
    }
}

fn spawn_finalize_retry(view: MasterKvRouterView, owner: NodeID, items: Vec<OwnerReclaimItem>) {
    if items.is_empty() {
        return;
    }
    let spawn_view = view.clone();
    let _ = spawn_view.spawn("owner_reclaim_finalize_retry", async move {
        let mut pending = items;
        let mut delay = Duration::from_millis(25);
        for _attempt in 1..=8 {
            tokio::time::sleep(delay).await;
            match call_owner_phase(&view, &owner, OwnerReclaimPhase::Finalize, pending.clone())
                .await
            {
                Ok(responses) => {
                    let mut next = Vec::new();
                    for (item, response) in pending.into_iter().zip(responses.into_iter()) {
                        if response.state == OwnerReclaimItemState::Finalized {
                            clear_master_fence(&view, &item);
                        } else {
                            next.push(item);
                        }
                    }
                    pending = next;
                    if pending.is_empty() {
                        return;
                    }
                }
                Err(err) => tracing::warn!(
                    "owner reclaim finalize retry failed: owner={} keys={} err={}",
                    owner,
                    pending.len(),
                    err
                ),
            }
            delay = (delay * 2).min(Duration::from_secs(1));
        }
        tracing::error!(
            "owner reclaim finalize retry exhausted; fences retained: owner={} keys={}",
            owner,
            pending.len()
        );
    });
}

async fn reclaim_items(
    view: &MasterKvRouterView,
    owner: &NodeID,
    candidates: Vec<OwnerReclaimItem>,
) -> u32 {
    let counters = view
        .master_kv_router()
        .eviction_reclaim_counters(owner.as_ref());
    let mut fenced = Vec::new();
    for item in candidates {
        if master_has_holder(view, &item.key) {
            // A completed get holder keeps an Allocation alive after its route disappears. It is
            // not a reclaim exclusion: owner Prepare is the authority that fences the local index
            // and rejects reclaim only while a real MemoryInfo reader is still active.
            counters
                .master_get_holder_observed
                .fetch_add(1, Ordering::Relaxed);
        }
        match view
            .master_kv_router()
            .inner()
            .key_activity
            .try_install_reclaim(&item)
        {
            Ok(()) => {
                if item_still_valid(view, owner, &item) {
                    fenced.push(item);
                } else {
                    counters.route_changed.fetch_add(1, Ordering::Relaxed);
                    clear_master_fence(view, &item);
                }
            }
            Err(activity) => {
                counters
                    .master_activity_deferred
                    .fetch_add(1, Ordering::Relaxed);
                tracing::trace!(
                    "owner reclaim deferred by master activity: owner={} key={} puts={} gets={} replicas={} reclaim_installed={}",
                    owner,
                    item.key,
                    activity.puts,
                    activity.gets,
                    activity.replicas,
                    activity.reclaim_installed
                );
            }
        }
    }
    if fenced.is_empty() {
        return 0;
    }

    let (master_only, owner_coordinated) = partition_reclaim_coordination(fenced);
    let master_reclaimed = finish_unindexed_allocations(view, owner, master_only);
    if owner_coordinated.is_empty() {
        counters
            .completed
            .fetch_add(u64::from(master_reclaimed), Ordering::Relaxed);
        return master_reclaimed;
    }

    let prepare_responses = match call_owner_phase(
        view,
        owner,
        OwnerReclaimPhase::Prepare,
        owner_coordinated.clone(),
    )
    .await
    {
        Ok(responses) => responses,
        Err(err) => {
            tracing::warn!(
                "owner reclaim prepare RPC failed; aborting batch: owner={} keys={} err={}",
                owner,
                owner_coordinated.len(),
                err
            );
            let _ = abort_prepared(view, owner, owner_coordinated).await;
            counters
                .completed
                .fetch_add(u64::from(master_reclaimed), Ordering::Relaxed);
            return master_reclaimed;
        }
    };
    let mut prepared = Vec::new();
    let mut committed = Vec::new();
    for (item, response) in owner_coordinated
        .into_iter()
        .zip(prepare_responses.into_iter())
    {
        match response.state {
            OwnerReclaimItemState::Prepared => prepared.push(item),
            OwnerReclaimItemState::Committed => committed.push(item),
            OwnerReclaimItemState::Busy => {
                if response.detail == "owner local memory still has active holders" {
                    counters
                        .owner_holder_deferred
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    counters
                        .owner_other_deferred
                        .fetch_add(1, Ordering::Relaxed);
                }
                clear_master_fence(view, &item);
            }
            _ => {
                counters
                    .owner_other_deferred
                    .fetch_add(1, Ordering::Relaxed);
                clear_master_fence(view, &item);
            }
        }
    }

    let mut invalid_prepared = Vec::new();
    prepared.retain(|item| {
        if master_has_holder(view, &item.key) {
            counters
                .master_get_holder_observed
                .fetch_add(1, Ordering::Relaxed);
        }
        let valid = item_still_valid(view, owner, item);
        if !valid {
            counters.route_changed.fetch_add(1, Ordering::Relaxed);
            invalid_prepared.push(item.clone());
        }
        valid
    });
    committed.extend(abort_prepared(view, owner, invalid_prepared).await);

    if !prepared.is_empty() {
        match call_owner_phase(view, owner, OwnerReclaimPhase::Commit, prepared.clone()).await {
            Ok(responses) => {
                let mut unresolved = Vec::new();
                for (item, response) in prepared.into_iter().zip(responses.into_iter()) {
                    if response.state == OwnerReclaimItemState::Committed {
                        committed.push(item);
                    } else {
                        unresolved.push(item);
                    }
                }
                committed.extend(abort_prepared(view, owner, unresolved).await);
            }
            Err(err) => {
                tracing::warn!(
                    "owner reclaim commit RPC failed; resolving with abort: owner={} keys={} err={}",
                    owner,
                    prepared.len(),
                    err
                );
                committed.extend(abort_prepared(view, owner, prepared).await);
            }
        }
    }
    let reclaimed = master_reclaimed.saturating_add(finish_committed(view, owner, committed).await);
    counters
        .completed
        .fetch_add(u64::from(reclaimed), Ordering::Relaxed);
    reclaimed
}

fn collect_reserve_candidates(
    view: &MasterKvRouterView,
    owner: &NodeID,
    slot_size: u64,
    limit: usize,
) -> Vec<OwnerReclaimItem> {
    let mut candidates = Vec::new();
    // Moka iteration may synchronously drain cache maintenance and eviction listeners. Under
    // pressure that made a 256-item candidate lookup take seconds. Route metadata is the
    // reclaim authority, so snapshot its keys and validate candidates directly instead.
    let route_keys = view
        .master_kv_router()
        .inner()
        .kv_routes
        .iter()
        .map(|route| route.key().clone())
        .collect::<Vec<_>>();
    for key in route_keys {
        if candidates.len() >= limit {
            break;
        }
        if !view
            .master_kv_router()
            .inner()
            .key_activity
            .is_quiescent(&key)
        {
            continue;
        }
        if let Some(item) = route_item(
            view,
            owner,
            &key,
            None,
            Some(slot_size),
            OwnerReclaimReason::Reserve,
            view.master_kv_router().next_owner_reclaim_epoch(),
        ) {
            candidates.push(item);
        }
    }
    candidates
}

async fn reclaim_reserve_candidate_batches<F, Fut>(
    candidates: Vec<OwnerReclaimItem>,
    required_free_slots: u32,
    mut reclaim_batch: F,
) -> (u32, usize)
where
    F: FnMut(Vec<OwnerReclaimItem>) -> Fut,
    Fut: Future<Output = u32>,
{
    let mut reclaimed = 0u32;
    let mut scanned_candidates = 0usize;
    while reclaimed < required_free_slots && scanned_candidates < candidates.len() {
        let remaining_slots = required_free_slots.saturating_sub(reclaimed);
        let batch_len = usize::try_from(remaining_slots)
            .unwrap_or(usize::MAX)
            .min(candidates.len() - scanned_candidates);
        let batch_end = scanned_candidates + batch_len;
        let batch = candidates[scanned_candidates..batch_end].to_vec();
        scanned_candidates = batch_end;

        let batch_size = u32::try_from(batch.len()).unwrap_or(u32::MAX);
        let batch_reclaimed = reclaim_batch(batch).await;
        debug_assert!(batch_reclaimed <= batch_size);
        reclaimed = reclaimed.saturating_add(batch_reclaimed.min(batch_size));
    }
    (reclaimed, scanned_candidates)
}

#[cfg(test)]
mod reserve_scan_tests {
    use super::{
        OwnerReclaimBacking, OwnerReclaimItem, OwnerReclaimReason, partition_reclaim_coordination,
        reclaim_reserve_candidate_batches,
    };
    use std::future::ready;

    fn candidate(index: u32) -> OwnerReclaimItem {
        OwnerReclaimItem {
            key: format!("candidate-{index}"),
            put_id: (u64::from(index), 0),
            epoch: u64::from(index),
            backing: OwnerReclaimBacking::CommittedSlot {
                grant_id: u64::from(index),
                slot_index: index,
                slot_size: 8 * 1024 * 1024,
            },
            reason: OwnerReclaimReason::Reserve,
        }
    }

    #[test]
    fn only_unindexed_allocations_skip_owner_coordination() {
        let mut indexed_allocation = candidate(1);
        indexed_allocation.backing = OwnerReclaimBacking::Allocation;
        indexed_allocation.reason = OwnerReclaimReason::CapacityEviction;
        let mut unindexed_allocation = candidate(2);
        unindexed_allocation.backing = OwnerReclaimBacking::UnindexedAllocation;
        unindexed_allocation.reason = OwnerReclaimReason::CapacityEviction;
        let committed_slot = candidate(3);

        let (master_only, owner_coordinated) = partition_reclaim_coordination(vec![
            indexed_allocation,
            unindexed_allocation.clone(),
            committed_slot,
        ]);

        assert_eq!(master_only, vec![unindexed_allocation]);
        assert_eq!(owner_coordinated.len(), 2);
        assert!(
            owner_coordinated
                .iter()
                .all(|item| item.backing != OwnerReclaimBacking::UnindexedAllocation)
        );
    }

    #[limit_thirdparty::tokio::test]
    async fn reserve_scan_continues_after_busy_first_batch() {
        let candidates = (0..12).map(candidate).collect::<Vec<_>>();
        let mut batches = Vec::new();
        let mut call_count = 0usize;

        let (reclaimed, scanned_candidates) =
            reclaim_reserve_candidate_batches(candidates, 5, |batch: Vec<OwnerReclaimItem>| {
                call_count += 1;
                batches.push(
                    batch
                        .iter()
                        .map(|item| item.key.clone())
                        .collect::<Vec<_>>(),
                );
                ready(if call_count == 1 {
                    0
                } else {
                    u32::try_from(batch.len()).unwrap()
                })
            })
            .await;

        assert_eq!(reclaimed, 5);
        assert_eq!(scanned_candidates, 10);
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0][0], "candidate-0");
        assert_eq!(batches[0][4], "candidate-4");
        assert_eq!(batches[1][0], "candidate-5");
        assert_eq!(batches[1][4], "candidate-9");
    }

    #[limit_thirdparty::tokio::test]
    async fn reserve_scan_shrinks_next_batch_after_partial_reclaim() {
        let candidates = (0..10).map(candidate).collect::<Vec<_>>();
        let mut batch_lengths = Vec::new();
        let mut call_count = 0usize;

        let (reclaimed, scanned_candidates) =
            reclaim_reserve_candidate_batches(candidates, 5, |batch| {
                call_count += 1;
                batch_lengths.push(batch.len());
                ready(if call_count == 1 { 2 } else { 3 })
            })
            .await;

        assert_eq!(reclaimed, 5);
        assert_eq!(scanned_candidates, 8);
        assert_eq!(batch_lengths, vec![5, 3]);
    }
}

#[cfg(test)]
mod owner_get_holding_reclaim_tests {
    use super::{OwnerReclaimBacking, OwnerReclaimReason, reclaim_items, route_item};
    use crate::client_kv_api::PutOptionalArgs;
    use crate::kvcore_test_lib::{
        integration_test_lock, start_master_and_client, stop_master_and_client,
    };
    use crate::memholder::{MemholderManagerTrait, NodeHolderKey};
    use std::time::{Duration, Instant};

    #[limit_thirdparty::tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn completed_get_holding_does_not_block_two_sided_owner_reclaim() {
        let _test_guard = integration_test_lock().await;
        let (master, client) =
            start_master_and_client("reclaim_get_holding_master", "reclaim_get_holding_owner")
                .await;
        let key = "completed_get_holding_reclaim_key";
        let owner_view = client.client_kv_api_view();
        let owner_api = owner_view.client_kv_api();
        owner_api
            .inner()
            .put(key, &[7u8; 4096], PutOptionalArgs::default())
            .await
            .expect("owner put");
        let (holder, _get_info) = owner_api
            .inner()
            .get(key)
            .await
            .expect("owner get")
            .expect("owner get should hit");

        let owner_id = client
            .cluster_manager_view()
            .cluster_manager()
            .get_self_info()
            .id;
        let holding_key = NodeHolderKey::new(owner_id.clone(), holder.holder_id());
        let master_view = master.master_kv_router_view().clone();
        assert!(
            master_view
                .master_kv_router()
                .inner()
                .get_holding
                .inner()
                .contains_key(&holding_key),
            "get_done must install the Allocation lifetime holder"
        );

        assert!(
            master_view
                .master_kv_router()
                .inner()
                .key_activity
                .is_quiescent(key),
            "completed get must release its master key-activity lease"
        );
        let owner_node = owner_id.clone().into();
        let busy_item = route_item(
            &master_view,
            &owner_node,
            key,
            None,
            None,
            OwnerReclaimReason::CapacityEviction,
            master_view.master_kv_router().next_owner_reclaim_epoch(),
        )
        .expect("active-holder owner route should be reclaimable after the reader exits");
        assert_eq!(
            reclaim_items(&master_view, &owner_node, vec![busy_item]).await,
            0,
            "owner Prepare must reject reclaim while the user holder is live"
        );

        drop(holder);
        limit_thirdparty::tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            master_view
                .master_kv_router()
                .inner()
                .get_holding
                .inner()
                .contains_key(&holding_key),
            "the committed local index intentionally keeps MemoryInfo and its ACK holder alive"
        );

        let item = route_item(
            &master_view,
            &owner_node,
            key,
            None,
            None,
            OwnerReclaimReason::CapacityEviction,
            master_view.master_kv_router().next_owner_reclaim_epoch(),
        )
        .expect("current owner route should be reclaimable");
        assert_eq!(
            item.backing,
            OwnerReclaimBacking::Allocation,
            "reuse-replica get_done must publish the owner-local index on the route"
        );
        assert_eq!(
            reclaim_items(&master_view, &owner_node, vec![item]).await,
            1
        );

        let wait_started = Instant::now();
        while master_view
            .master_kv_router()
            .inner()
            .get_holding
            .inner()
            .contains_key(&holding_key)
        {
            assert!(
                wait_started.elapsed() < Duration::from_secs(5),
                "owner reclaim must drop MemoryInfo and deliver its delete ACK"
            );
            limit_thirdparty::tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            !master_view
                .master_kv_router()
                .inner()
                .kv_routes
                .contains_key(key),
            "the reclaimed last replica route must be removed"
        );

        stop_master_and_client(master, client).await;
    }
}

pub(crate) async fn reclaim_owner_slots_for_reserve(
    view: &MasterKvRouterView,
    owner_node_id: &NodeID,
    slot_size: u64,
    required_free_slots: u32,
) -> u32 {
    if slot_size == 0 || required_free_slots == 0 {
        return 0;
    }
    let scan_started = Instant::now();
    let candidates =
        collect_reserve_candidates(view, owner_node_id, slot_size, OWNER_RECLAIM_MAX_BATCH);
    let candidate_scan_us = scan_started.elapsed().as_micros();
    let candidate_count = candidates.len();
    let (reclaimed, scanned_candidate_count) =
        reclaim_reserve_candidate_batches(candidates, required_free_slots, |batch| {
            reclaim_items(view, owner_node_id, batch)
        })
        .await;
    tracing::info!(
        "owner reserve reclaim completed: owner={} slot_size={} requested_slots={} candidates={} scanned_candidates={} reclaimed={} candidate_scan_us={}",
        owner_node_id,
        slot_size,
        required_free_slots,
        candidate_count,
        scanned_candidate_count,
        reclaimed,
        candidate_scan_us
    );
    reclaimed
}

fn spawn_eviction_reclaim_retry(view: MasterKvRouterView, requests: Vec<EvictionReclaimRequest>) {
    if requests.is_empty() {
        return;
    }
    let mut delayed = Vec::with_capacity(requests.len());
    let mut restored_count = 0usize;
    let mut restored_weight = 0u64;
    for mut request in requests {
        request.retry_count = request.retry_count.saturating_add(1);
        let counters = view
            .master_kv_router()
            .eviction_reclaim_counters(&request.owner_node_id);
        let weight = u64::from(request.desc.weight_bytes);
        if !view.master_kv_router().eviction_cache_entry_is_current(
            &request.owner_node_id,
            &request.key,
            &request.desc,
        ) {
            view.master_kv_router()
                .complete_eviction_reclaim_weight(&request.owner_node_id, weight);
            counters.route_changed.fetch_add(1, Ordering::Relaxed);
            counters.retry_completed.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        if should_restore_after_retry(request.retry_count) {
            let restored = view
                .master_kv_router()
                .restore_eviction_cache_entry_if_current(
                    &request.owner_node_id,
                    request.key.clone(),
                    request.desc.clone(),
                );
            if restored {
                view.master_kv_router()
                    .complete_eviction_reclaim_weight(&request.owner_node_id, weight);
                counters.retry_restored.fetch_add(1, Ordering::Relaxed);
                counters.retry_completed.fetch_add(1, Ordering::Relaxed);
                restored_count += 1;
                restored_weight = restored_weight.saturating_add(weight);
                continue;
            }
            if !view.master_kv_router().eviction_cache_entry_is_current(
                &request.owner_node_id,
                &request.key,
                &request.desc,
            ) {
                view.master_kv_router()
                    .complete_eviction_reclaim_weight(&request.owner_node_id, weight);
                counters.route_changed.fetch_add(1, Ordering::Relaxed);
                counters.retry_completed.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        }
        counters.retry_queued.fetch_add(1, Ordering::Relaxed);
        delayed.push(request);
    }
    if restored_count != 0 {
        tracing::info!(
            "safe eviction reclaim restored current cache entries after bounded retry: entries={} weight_bytes={} max_retry_count={}",
            restored_count,
            restored_weight,
            EVICTION_RECLAIM_MAX_RETRY_COUNT
        );
    }
    if delayed.is_empty() {
        return;
    }
    let max_retry_count = delayed
        .iter()
        .map(|request| request.retry_count)
        .max()
        .unwrap_or(1);
    let retry_delay = eviction_reclaim_retry_delay(max_retry_count);
    let spawn_view = view.clone();
    let _ = spawn_view.spawn("eviction_reclaim_retry", async move {
        tokio::time::sleep(retry_delay).await;
        let tx = view.master_kv_router().inner().eviction_reclaim_tx.clone();
        for request in delayed {
            let weight = u64::from(request.desc.weight_bytes);
            let counters = view
                .master_kv_router()
                .eviction_reclaim_counters(&request.owner_node_id);
            if !view.master_kv_router().eviction_cache_entry_is_current(
                &request.owner_node_id,
                &request.key,
                &request.desc,
            ) {
                view.master_kv_router()
                    .complete_eviction_reclaim_weight(&request.owner_node_id, weight);
                counters.route_changed.fetch_add(1, Ordering::Relaxed);
                counters.retry_completed.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let restore_request = request.clone();
            if tx.send(request).await.is_err() {
                view.master_kv_router()
                    .complete_eviction_reclaim_weight(&restore_request.owner_node_id, weight);
                let restored = view
                    .master_kv_router()
                    .restore_eviction_cache_entry_if_current(
                        &restore_request.owner_node_id,
                        restore_request.key,
                        restore_request.desc,
                    );
                counters.retry_completed.fetch_add(1, Ordering::Relaxed);
                if restored {
                    counters.retry_restored.fetch_add(1, Ordering::Relaxed);
                }
                tracing::warn!(
                    "safe eviction reclaim retry queue closed: owner={} restored={}",
                    restore_request.owner_node_id,
                    restored
                );
            }
        }
    });
}

pub(crate) fn spawn_eviction_reclaim_actor(
    view: MasterKvRouterView,
    mut rx: limit_thirdparty::tokio::sync::ampsc::Receiver<EvictionReclaimRequest>,
) {
    let view_task = view.clone();
    let _ = view.spawn("eviction_reclaim_actor", async move {
        let mut shutdown_waiter = view_task.register_shutdown_waiter();
        loop {
            let first = tokio::select! {
                _ = shutdown_waiter.wait() => break,
                request = rx.recv() => {
                    let Some(request) = request else { break; };
                    request
                }
            };
            let mut batch = Vec::with_capacity(OWNER_RECLAIM_MAX_BATCH);
            batch.push(first);
            let mut merge_window = Box::pin(tokio::time::sleep(OWNER_RECLAIM_MERGE_WINDOW));
            while batch.len() < OWNER_RECLAIM_MAX_BATCH {
                tokio::select! {
                    _ = &mut merge_window => break,
                    request = rx.recv() => {
                        let Some(request) = request else { break; };
                        batch.push(request);
                    }
                }
            }

            let mut groups: HashMap<NodeIDString, Vec<EvictionReclaimRequest>> = HashMap::new();
            for request in batch {
                groups
                    .entry(request.owner_node_id.clone())
                    .or_default()
                    .push(request);
            }
            for (owner_node_id, requests) in groups {
                let owner: NodeID = owner_node_id.clone().into();
                let counters = view_task
                    .master_kv_router()
                    .eviction_reclaim_counters(&owner_node_id);
                let mut seen = HashSet::new();
                let mut items = Vec::new();
                let mut unique_requests = Vec::new();
                let mut completed_weight = 0u64;
                for request in requests {
                    if !seen.insert((request.key.clone(), request.desc.put_id)) {
                        completed_weight = completed_weight
                            .saturating_add(u64::from(request.desc.weight_bytes));
                        continue;
                    }
                    if let Some(item) = route_item(
                        &view_task,
                        &owner,
                        &request.key,
                        Some(request.desc.put_id),
                        None,
                        OwnerReclaimReason::CapacityEviction,
                        view_task.master_kv_router().next_owner_reclaim_epoch(),
                    ) {
                        items.push(item);
                    }
                    unique_requests.push(request);
                }
                let candidate_count = items.len();
                let reclaimed = reclaim_items(&view_task, &owner, items).await;
                let mut retry_requests = Vec::new();
                for request in unique_requests {
                    if view_task
                        .master_kv_router()
                        .eviction_cache_entry_is_current(
                            &owner_node_id,
                            &request.key,
                            &request.desc,
                        )
                    {
                        retry_requests.push(request);
                    } else {
                        completed_weight = completed_weight
                            .saturating_add(u64::from(request.desc.weight_bytes));
                        counters.route_changed.fetch_add(1, Ordering::Relaxed);
                        if request.retry_count != 0 {
                            counters.retry_completed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                view_task.master_kv_router().complete_eviction_reclaim_weight(
                    &owner_node_id,
                    completed_weight,
                );
                let retry_count = retry_requests.len();
                spawn_eviction_reclaim_retry(view_task.clone(), retry_requests);
                tracing::trace!(
                    "batched safe eviction reclaim completed: owner={} candidates={} reclaimed={} retry_deferred={} completed_weight={}",
                    owner_node_id,
                    candidate_count,
                    reclaimed,
                    retry_count,
                    completed_weight
                );
            }
        }
    });
}
