use super::{ClientKvApiInner, ClientKvApiView, OwnerPreparedReclaim, OwnerReclaimRecord};
use crate::cluster_manager::{NodeID, NodeRole};
use crate::master_kv_router::msg_pack::{
    BatchOwnerReclaimReq, BatchOwnerReclaimResp, OwnerReclaimBacking, OwnerReclaimItem,
    OwnerReclaimItemResp, OwnerReclaimItemState, OwnerReclaimPhase,
};
use crate::p2p::msg_pack::MsgPack;
use crate::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, OK};
use std::{collections::HashMap, sync::Arc};

fn item_resp(
    item: &OwnerReclaimItem,
    state: OwnerReclaimItemState,
    detail: impl Into<String>,
) -> OwnerReclaimItemResp {
    OwnerReclaimItemResp {
        key: item.key.clone(),
        epoch: item.epoch,
        state,
        detail: detail.into(),
    }
}

fn record_item(record: &OwnerReclaimRecord) -> &OwnerReclaimItem {
    match record {
        OwnerReclaimRecord::Prepared(prepared) => &prepared.item,
        OwnerReclaimRecord::Releasing(item) => item,
        OwnerReclaimRecord::Committed(item) => item,
    }
}

fn memory_matches_reclaim_backing(
    memory_info: &crate::memholder::MemoryInfo,
    backing: &OwnerReclaimBacking,
) -> bool {
    match backing {
        OwnerReclaimBacking::Allocation => memory_info.local_reserve_resident_slot_ref().is_none(),
        OwnerReclaimBacking::UnindexedAllocation => false,
        OwnerReclaimBacking::CommittedSlot {
            grant_id,
            slot_index,
            slot_size,
        } => memory_info.local_reserve_resident_slot_ref().is_some_and(
            |(actual_slot_size, actual_grant_id, actual_slot_index)| {
                actual_slot_size == *slot_size
                    && actual_grant_id == *grant_id
                    && actual_slot_index == *slot_index
            },
        ),
    }
}

fn reclaim_key_control_busy_detail(state: &super::OwnerKeyControlState) -> Option<&'static str> {
    if state.local_puts != 0 {
        Some("owner local put is inflight")
    } else if state.external_pending_puts != 0 {
        Some("owner external put context is still pending")
    } else if state.source_eviction_selection.is_some() {
        Some("owner source eviction selection fence is active")
    } else if state.external_get.is_some() {
        Some("owner external Get is inflight")
    } else {
        None
    }
}

fn prepare_one(inner: &ClientKvApiInner, item: &OwnerReclaimItem) -> OwnerReclaimItemResp {
    let mut controls = inner.owner_key_control.lock_key(&item.key);
    if controls
        .get(&item.key)
        .is_some_and(|state| state.source_eviction_selection.is_some())
    {
        let state = controls
            .get_mut(&item.key)
            .expect("owner source selection control state disappeared");
        let selection_matches = state
            .source_eviction_selection
            .as_ref()
            .is_some_and(|selection| {
                selection.put_id == item.put_id
                    && memory_matches_reclaim_backing(
                        selection.cached_info.mem_holder.as_ref(),
                        &item.backing,
                    )
            });
        if !selection_matches {
            return item_resp(
                item,
                OwnerReclaimItemState::Busy,
                "another owner source selection owns the key fence",
            );
        }
        if state.local_puts != 0 || state.external_pending_puts != 0 {
            return item_resp(
                item,
                OwnerReclaimItemState::Busy,
                "owner put crossed the source selection fence",
            );
        }
        if inner.precommit_local_visible_info.contains_key(&item.key)
            || inner.pending_local_get_info.contains_key(&item.key)
        {
            return item_resp(
                item,
                OwnerReclaimItemState::Busy,
                "owner local publication crossed the source selection fence",
            );
        }
        let selection = state
            .source_eviction_selection
            .take()
            .expect("matching owner source selection must exist");
        if Arc::strong_count(&selection.cached_info.mem_holder) != 1 {
            state.source_eviction_selection = Some(selection);
            return item_resp(
                item,
                OwnerReclaimItemState::Busy,
                "owner local memory still has active holders",
            );
        }
        let local_snapshot = inner
            .local_snapshot_info
            .remove_if(&item.key, |_, snapshot| {
                snapshot.put_time_ms == item.put_id.0 && snapshot.put_version == item.put_id.1
            })
            .map(|(_, snapshot)| snapshot);
        assert!(state.reclaim.is_none());
        state.reclaim = Some(OwnerReclaimRecord::Prepared(OwnerPreparedReclaim {
            item: item.clone(),
            cached_info: selection.cached_info,
            local_snapshot,
        }));
        return item_resp(
            item,
            OwnerReclaimItemState::Prepared,
            "owner source selection promoted to reclaim fence",
        );
    }
    if let Some(state) = controls.get(&item.key) {
        if let Some(detail) = reclaim_key_control_busy_detail(state) {
            return item_resp(item, OwnerReclaimItemState::Busy, detail);
        }
        if let Some(record) = state.reclaim.as_ref() {
            if record_item(record) == item {
                return item_resp(
                    item,
                    match record {
                        OwnerReclaimRecord::Prepared(_) => OwnerReclaimItemState::Prepared,
                        OwnerReclaimRecord::Releasing(_) => OwnerReclaimItemState::Busy,
                        OwnerReclaimRecord::Committed(_) => OwnerReclaimItemState::Committed,
                    },
                    "reclaim phase already applied",
                );
            }
            return item_resp(
                item,
                OwnerReclaimItemState::Busy,
                "another reclaim epoch owns the key fence",
            );
        }
    }
    if inner.precommit_local_visible_info.contains_key(&item.key) {
        return item_resp(
            item,
            OwnerReclaimItemState::Busy,
            "owner precommit local index is still visible",
        );
    }
    if inner.pending_local_get_info.contains_key(&item.key) {
        return item_resp(
            item,
            OwnerReclaimItemState::Busy,
            "owner local Get commit is pending",
        );
    }
    let Some((_key, cached_info)) = inner.get_cached_info.remove_if(&item.key, |_, cached| {
        cached.put_time_ms == item.put_id.0
            && cached.put_version == item.put_id.1
            && memory_matches_reclaim_backing(cached.mem_holder.as_ref(), &item.backing)
    }) else {
        return item_resp(
            item,
            OwnerReclaimItemState::Stale,
            "matching local backing index is absent",
        );
    };

    // The index entry is now hidden while the same control lock keeps all new local readers out.
    // Any reader that cloned the memory just before the fence is visible in the Arc count.
    if Arc::strong_count(&cached_info.mem_holder) != 1 {
        let replaced = inner.get_cached_info.insert(item.key.clone(), cached_info);
        assert!(
            replaced.is_none(),
            "owner reclaim rollback must restore an empty local index slot"
        );
        return item_resp(
            item,
            OwnerReclaimItemState::Busy,
            "owner local memory still has active holders",
        );
    }

    let local_snapshot = inner
        .local_snapshot_info
        .remove_if(&item.key, |_, snapshot| {
            snapshot.put_time_ms == item.put_id.0 && snapshot.put_version == item.put_id.1
        })
        .map(|(_, snapshot)| snapshot);
    let state = controls.entry(item.key.clone()).or_default();
    assert!(
        state.reclaim.is_none()
            && state.local_puts == 0
            && state.external_pending_puts == 0
            && state.external_get.is_none()
    );
    state.reclaim = Some(OwnerReclaimRecord::Prepared(OwnerPreparedReclaim {
        item: item.clone(),
        cached_info,
        local_snapshot,
    }));
    item_resp(
        item,
        OwnerReclaimItemState::Prepared,
        "owner local index fenced",
    )
}

fn release_prepared_backing_now(inner: &ClientKvApiInner, prepared: OwnerPreparedReclaim) {
    let mut memory_info = Arc::try_unwrap(prepared.cached_info.mem_holder).unwrap_or_else(|_| {
        panic!(
            "owner reclaim prepared memory unexpectedly gained a holder: key={} epoch={}",
            prepared.item.key, prepared.item.epoch
        )
    });
    match &prepared.item.backing {
        OwnerReclaimBacking::Allocation => {
            assert!(
                memory_info.local_reserve_resident_slot_ref().is_none(),
                "allocation reclaim must not carry a local-reserve slot"
            );
        }
        OwnerReclaimBacking::UnindexedAllocation => {
            unreachable!("unindexed allocations must be reclaimed entirely on the master")
        }
        OwnerReclaimBacking::CommittedSlot {
            grant_id,
            slot_index,
            slot_size,
        } => {
            let (actual_slot_size, actual_grant_id, actual_slot_index) = memory_info
                .take_local_reserve_resident_slot_ref()
                .expect("committed-slot reclaim must carry a local-reserve slot");
            assert_eq!(actual_slot_size, *slot_size);
            assert_eq!(actual_grant_id, *grant_id);
            assert_eq!(actual_slot_index, *slot_index);

            inner
                .owner_release_local_reserve_committed_resident_slot(
                    actual_slot_size,
                    actual_grant_id,
                    actual_slot_index,
                )
                .expect("owner reclaim committed resident slot release must succeed");
        }
    }
    drop(memory_info);
}

fn reclaim_release_fence_is_intact(
    state: &super::OwnerKeyControlState,
    item: &OwnerReclaimItem,
) -> bool {
    // Prepare hides the local index before installing the reclaim fence. A
    // later external Get may share this key state, but it can only take the
    // remote path and therefore does not hold the detached local backing.
    matches!(
        state.reclaim.as_ref(),
        Some(OwnerReclaimRecord::Releasing(releasing)) if releasing == item
    ) && state.local_puts == 0
        && state.external_pending_puts == 0
        && state.source_eviction_selection.is_none()
}

fn commit_one(inner: &ClientKvApiInner, item: &OwnerReclaimItem) -> OwnerReclaimItemResp {
    let mut controls = inner.owner_key_control.lock_key(&item.key);
    let Some(state) = controls.get_mut(&item.key) else {
        return item_resp(
            item,
            OwnerReclaimItemState::Stale,
            "owner reclaim fence is absent",
        );
    };
    let Some(record) = state.reclaim.take() else {
        return item_resp(
            item,
            OwnerReclaimItemState::Stale,
            "owner reclaim fence is absent",
        );
    };
    let prepared = match record {
        OwnerReclaimRecord::Prepared(prepared) if prepared.item == *item => {
            state.reclaim = Some(OwnerReclaimRecord::Releasing(item.clone()));
            prepared
        }
        OwnerReclaimRecord::Releasing(releasing) if releasing == *item => {
            state.reclaim = Some(OwnerReclaimRecord::Releasing(releasing));
            return item_resp(
                item,
                OwnerReclaimItemState::Busy,
                "owner reclaim slot release is already in progress",
            );
        }
        OwnerReclaimRecord::Committed(committed) if committed == *item => {
            state.reclaim = Some(OwnerReclaimRecord::Committed(committed));
            return item_resp(
                item,
                OwnerReclaimItemState::Committed,
                "owner reclaim commit already applied",
            );
        }
        other => {
            state.reclaim = Some(other);
            return item_resp(
                item,
                OwnerReclaimItemState::Stale,
                "owner reclaim epoch or slot identity changed",
            );
        }
    };

    // The Releasing marker keeps local Put/Get out. Drop the key-shard lock
    // before touching the slot pool so no synchronous locks are nested.
    drop(controls);
    release_prepared_backing_now(inner, prepared);

    let mut controls = inner.owner_key_control.lock_key(&item.key);
    let state = controls
        .get_mut(&item.key)
        .expect("owner reclaim releasing fence disappeared");
    assert!(
        reclaim_release_fence_is_intact(state, item),
        "a local put crossed an owner reclaim fence"
    );
    state.reclaim = Some(OwnerReclaimRecord::Committed(item.clone()));
    drop(controls);
    inner.owner_hot_invalidate_version(&item.key, item.put_id);
    item_resp(
        item,
        OwnerReclaimItemState::Committed,
        "owner committed slot released",
    )
}

#[cfg(test)]
mod tests {
    use super::{reclaim_key_control_busy_detail, reclaim_release_fence_is_intact};
    use crate::client_kv_api::{
        ExternalGetKeySharedOp, OwnerKeyControlState, OwnerKeyControlTable, OwnerReclaimRecord,
        acquire_external_pending_put_fence_for_key,
    };
    use crate::master_kv_router::msg_pack::{
        OwnerReclaimBacking, OwnerReclaimItem, OwnerReclaimReason,
    };
    use std::sync::Arc;

    #[test]
    fn pending_external_put_rejects_reclaim_prepare_precheck() {
        let controls = Arc::new(OwnerKeyControlTable::default());
        let _guard = acquire_external_pending_put_fence_for_key(&controls, "pending-key")
            .expect("pending fence acquisition must succeed");
        let controls = controls.lock_key("pending-key");
        assert_eq!(
            reclaim_key_control_busy_detail(&controls["pending-key"]),
            Some("owner external put context is still pending")
        );
    }

    #[test]
    fn remote_get_marker_can_overlap_reclaim_commit() {
        let item = OwnerReclaimItem {
            key: "remote-during-reclaim".to_string(),
            put_id: (7, 1),
            epoch: 9,
            backing: OwnerReclaimBacking::CommittedSlot {
                grant_id: 3,
                slot_index: 4,
                slot_size: 4096,
            },
            reason: OwnerReclaimReason::OwnerCapacityEviction,
        };
        let state = OwnerKeyControlState {
            local_puts: 0,
            external_pending_puts: 0,
            source_eviction_selection: None,
            reclaim: Some(OwnerReclaimRecord::Releasing(item.clone())),
            external_get: Some(Arc::new(ExternalGetKeySharedOp::new(
                "remote-during-reclaim".to_string(),
            ))),
        };
        assert!(reclaim_release_fence_is_intact(&state, &item));

        let local_put_state = OwnerKeyControlState {
            local_puts: 1,
            external_pending_puts: 0,
            source_eviction_selection: None,
            reclaim: Some(OwnerReclaimRecord::Releasing(item.clone())),
            external_get: state.external_get,
        };
        assert!(!reclaim_release_fence_is_intact(&local_put_state, &item));
    }
}

fn abort_one(inner: &ClientKvApiInner, item: &OwnerReclaimItem) -> OwnerReclaimItemResp {
    let mut controls = inner.owner_key_control.lock_key(&item.key);
    let Some(state) = controls.get_mut(&item.key) else {
        return item_resp(
            item,
            OwnerReclaimItemState::Aborted,
            "owner reclaim was already absent",
        );
    };
    let Some(record) = state.reclaim.take() else {
        return item_resp(
            item,
            OwnerReclaimItemState::Aborted,
            "owner reclaim was already absent",
        );
    };
    match record {
        OwnerReclaimRecord::Prepared(prepared) if prepared.item == *item => {
            let replaced = inner
                .get_cached_info
                .insert(item.key.clone(), prepared.cached_info);
            assert!(
                replaced.is_none(),
                "owner reclaim abort must restore an empty local index slot"
            );
            if let Some(snapshot) = prepared.local_snapshot {
                let replaced = inner.local_snapshot_info.insert(item.key.clone(), snapshot);
                assert!(
                    replaced.is_none(),
                    "owner reclaim abort must restore an empty local snapshot slot"
                );
            }
            if state.is_idle() {
                controls.remove(&item.key);
            }
            item_resp(
                item,
                OwnerReclaimItemState::Aborted,
                "owner local index fence rolled back",
            )
        }
        OwnerReclaimRecord::Releasing(releasing) if releasing == *item => {
            state.reclaim = Some(OwnerReclaimRecord::Releasing(releasing));
            item_resp(
                item,
                OwnerReclaimItemState::Busy,
                "owner slot release is already in progress and cannot be aborted",
            )
        }
        OwnerReclaimRecord::Committed(committed) if committed == *item => {
            state.reclaim = Some(OwnerReclaimRecord::Committed(committed));
            item_resp(
                item,
                OwnerReclaimItemState::Committed,
                "owner slot was already committed and cannot be restored",
            )
        }
        other => {
            state.reclaim = Some(other);
            item_resp(
                item,
                OwnerReclaimItemState::Stale,
                "owner reclaim epoch or slot identity changed",
            )
        }
    }
}

fn finalize_one(inner: &ClientKvApiInner, item: &OwnerReclaimItem) -> OwnerReclaimItemResp {
    let mut controls = inner.owner_key_control.lock_key(&item.key);
    let Some(state) = controls.get_mut(&item.key) else {
        return item_resp(
            item,
            OwnerReclaimItemState::Finalized,
            "owner reclaim was already finalized",
        );
    };
    match state.reclaim.take() {
        Some(OwnerReclaimRecord::Committed(committed)) if committed == *item => {
            if state.is_idle() {
                controls.remove(&item.key);
            }
            item_resp(
                item,
                OwnerReclaimItemState::Finalized,
                "owner reclaim fence cleared",
            )
        }
        Some(OwnerReclaimRecord::Releasing(releasing)) if releasing == *item => {
            state.reclaim = Some(OwnerReclaimRecord::Releasing(releasing));
            item_resp(
                item,
                OwnerReclaimItemState::Busy,
                "owner slot release is still in progress",
            )
        }
        Some(other) => {
            state.reclaim = Some(other);
            item_resp(
                item,
                OwnerReclaimItemState::Busy,
                "owner reclaim is not committed for this epoch",
            )
        }
        None => item_resp(
            item,
            OwnerReclaimItemState::Finalized,
            "owner reclaim was already finalized",
        ),
    }
}

pub async fn handle_batch_owner_reclaim(
    view: &ClientKvApiView,
    req: MsgPack<BatchOwnerReclaimReq>,
    req_node_id: NodeID,
) -> MsgPack<BatchOwnerReclaimResp> {
    let requester_is_master = view
        .cluster_manager()
        .get_member_info_cached(req_node_id.as_ref())
        .is_some_and(|member| matches!(member.node_role(), NodeRole::Master));
    if !requester_is_master {
        let err = KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "batch owner reclaim requester is not the current master: requester={}",
                req_node_id
            ),
        });
        return MsgPack {
            serialize_part: BatchOwnerReclaimResp {
                items: Vec::new(),
                error_code: err.code(),
                error_json: err.to_json(),
            },
            raw_bytes: Vec::new(),
        };
    }
    let inner = view.client_kv_api().inner();
    let phase = req.serialize_part.phase;
    let items = req
        .serialize_part
        .items
        .iter()
        .map(|item| match phase {
            OwnerReclaimPhase::Prepare => prepare_one(inner, item),
            OwnerReclaimPhase::Commit => commit_one(inner, item),
            OwnerReclaimPhase::Abort => abort_one(inner, item),
            OwnerReclaimPhase::Finalize => finalize_one(inner, item),
        })
        .collect::<Vec<_>>();
    let prepared = items
        .iter()
        .filter(|item| item.state == OwnerReclaimItemState::Prepared)
        .count();
    let committed = items
        .iter()
        .filter(|item| item.state == OwnerReclaimItemState::Committed)
        .count();
    let finalized = items
        .iter()
        .filter(|item| item.state == OwnerReclaimItemState::Finalized)
        .count();
    let busy_or_stale = items
        .iter()
        .filter(|item| {
            matches!(
                item.state,
                OwnerReclaimItemState::Busy | OwnerReclaimItemState::Stale
            )
        })
        .count();
    let mut rejection_reason_counts = HashMap::<String, usize>::new();
    for item in &items {
        if matches!(
            item.state,
            OwnerReclaimItemState::Busy | OwnerReclaimItemState::Stale
        ) {
            *rejection_reason_counts
                .entry(item.detail.clone())
                .or_default() += 1;
        }
    }
    let mut rejection_reason_counts = rejection_reason_counts.into_iter().collect::<Vec<_>>();
    rejection_reason_counts.sort_by(|a, b| a.0.cmp(&b.0));
    tracing::info!(
        "owner reclaim phase completed: phase={:?} items={} prepared={} committed={} finalized={} busy_or_stale={} rejection_reasons={:?}",
        phase,
        items.len(),
        prepared,
        committed,
        finalized,
        busy_or_stale,
        rejection_reason_counts
    );
    MsgPack {
        serialize_part: BatchOwnerReclaimResp {
            items,
            error_code: OK,
            error_json: String::new(),
        },
        raw_bytes: Vec::new(),
    }
}
