use super::{
    ClientKvApiInner, ClientKvApiView, OwnerPreparedReclaim, OwnerPreparedReclaimSource,
    OwnerPreparedSsdBacking, OwnerReclaimRecord,
};
use crate::cluster_manager::{NodeID, NodeRole};
use crate::master_kv_router::msg_pack::{
    BatchOwnerReclaimReq, BatchOwnerReclaimResp, OwnerReclaimBacking, OwnerReclaimItem,
    OwnerReclaimItemResp, OwnerReclaimItemState, OwnerReclaimPhase, OwnerReclaimReason,
    OwnerSourceEvictionVictim,
};
use crate::p2p::msg_pack::MsgPack;
use crate::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, OK};
use limit_thirdparty::tokio;
use std::{collections::HashMap, sync::Arc};

/// Keep a route-deleted source readable long enough for an already-issued
/// master Get plan to acquire its exact source lease. A reclaim batch waits
/// once, then still commits every victim independently.
pub(crate) const OWNER_RECLAIM_SOURCE_READ_GRACE: std::time::Duration =
    std::time::Duration::from_millis(50);

fn item_resp(
    item: &OwnerReclaimItem,
    state: OwnerReclaimItemState,
    detail: impl Into<String>,
) -> OwnerReclaimItemResp {
    OwnerReclaimItemResp {
        key: item.key.clone(),
        epoch: item.epoch,
        state,
        ssd_backing_len: None,
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
        OwnerReclaimBacking::UnindexedAllocation { .. } => false,
        OwnerReclaimBacking::CommittedSlot {
            allocation_id,
            segment_offset,
            capacity_bytes,
        } => memory_info.local_reserve_resident_slot_ref().is_some_and(
            |(actual_allocation_id, actual_segment_offset, actual_capacity_bytes)| {
                actual_allocation_id == *allocation_id
                    && actual_segment_offset == *segment_offset
                    && actual_capacity_bytes == *capacity_bytes
            },
        ),
    }
}

fn reclaim_key_control_busy_detail(state: &super::OwnerKeyControlState) -> Option<&'static str> {
    if state.local_puts != 0 {
        Some("owner local put is inflight")
    } else if state.external_pending_puts != 0 {
        Some("owner external put context is still pending")
    } else if state.remote_put.is_some() {
        Some("owner remote put transfer is inflight")
    } else if state.local_ssd_put.is_some() {
        Some("owner local SSD put is inflight")
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
        if state.local_puts != 0
            || state.external_pending_puts != 0
            || state.remote_put.is_some()
            || state.local_ssd_put.is_some()
        {
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
        assert!(
            state.local_access_fence.is_some(),
            "source-selection promotion must retain its local-access completion generation"
        );
        state.reclaim = Some(OwnerReclaimRecord::Prepared(OwnerPreparedReclaim {
            item: item.clone(),
            source: OwnerPreparedReclaimSource::Indexed {
                cached_info: selection.cached_info,
                local_snapshot,
            },
            ssd_prepare_lock: Arc::new(tokio::sync::AMutex::new(())),
            ssd_prepare_complete: false,
            ssd_backing: None,
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
    if let OwnerReclaimBacking::UnindexedAllocation {
        addr,
        base_addr,
        len,
        capacity_bytes,
    } = &item.backing
    {
        if item.reason != OwnerReclaimReason::MasterAllocationCapacity
            || *len == 0
            || *capacity_bytes < *len
            || *addr < *base_addr
            || addr.checked_add(*len).is_none()
        {
            return item_resp(
                item,
                OwnerReclaimItemState::Stale,
                "invalid unindexed Allocation source identity",
            );
        }
        if inner.get_cached_info.contains_key(&item.key)
            || inner.local_snapshot_info.contains_key(&item.key)
        {
            return item_resp(
                item,
                OwnerReclaimItemState::Stale,
                "unindexed Allocation unexpectedly has an owner-local index",
            );
        }
        let state = controls.entry(item.key.clone()).or_default();
        assert!(
            state.reclaim.is_none()
                && state.local_puts == 0
                && state.external_pending_puts == 0
                && state.external_get.is_none()
        );
        state.begin_local_access_fence();
        state.reclaim = Some(OwnerReclaimRecord::Prepared(OwnerPreparedReclaim {
            item: item.clone(),
            source: OwnerPreparedReclaimSource::UnindexedAllocation {
                addr: *addr,
                len: *len,
            },
            ssd_prepare_lock: Arc::new(tokio::sync::AMutex::new(())),
            ssd_prepare_complete: false,
            ssd_backing: None,
        }));
        return item_resp(
            item,
            OwnerReclaimItemState::Prepared,
            "master-owned Allocation source fenced",
        );
    }
    if let OwnerReclaimBacking::CommittedSlot {
        allocation_id,
        segment_offset,
        capacity_bytes,
    } = &item.backing
        && item.reason == OwnerReclaimReason::MasterAllocationCapacity
        && !inner.get_cached_info.contains_key(&item.key)
    {
        // Do not nest the key-shard lock with the segment allocator lock.
        // The master key-activity fence already serializes this exact route;
        // after reacquiring the local key shard we recheck every local state.
        drop(controls);
        let route_only = inner
            .owner_segment_allocator
            .lock()
            .committed_route_only_matches(*allocation_id, *segment_offset, *capacity_bytes);
        let mut controls = inner.owner_key_control.lock_key(&item.key);
        if !route_only
            || inner.get_cached_info.contains_key(&item.key)
            || inner.precommit_local_visible_info.contains_key(&item.key)
            || inner.pending_local_get_info.contains_key(&item.key)
        {
            return item_resp(
                item,
                OwnerReclaimItemState::Busy,
                "GlobalShared owner slot changed while installing its reclaim fence",
            );
        }
        if let Some(state) = controls.get(&item.key) {
            if let Some(detail) = reclaim_key_control_busy_detail(state) {
                return item_resp(item, OwnerReclaimItemState::Busy, detail);
            }
            if state.reclaim.is_some() {
                return item_resp(
                    item,
                    OwnerReclaimItemState::Busy,
                    "another reclaim epoch owns the GlobalShared slot",
                );
            }
        }
        let state = controls.entry(item.key.clone()).or_default();
        state.begin_local_access_fence();
        state.reclaim = Some(OwnerReclaimRecord::Prepared(OwnerPreparedReclaim {
            item: item.clone(),
            source: OwnerPreparedReclaimSource::UnindexedOwnerSlot {
                allocation_id: *allocation_id,
                segment_offset: *segment_offset,
                capacity_bytes: *capacity_bytes,
            },
            ssd_prepare_lock: Arc::new(tokio::sync::AMutex::new(())),
            ssd_prepare_complete: true,
            ssd_backing: None,
        }));
        return item_resp(
            item,
            OwnerReclaimItemState::Prepared,
            "GlobalShared owner slot fenced for exact physical reclaim",
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
    state.begin_local_access_fence();
    state.reclaim = Some(OwnerReclaimRecord::Prepared(OwnerPreparedReclaim {
        item: item.clone(),
        source: OwnerPreparedReclaimSource::Indexed {
            cached_info,
            local_snapshot,
        },
        ssd_prepare_lock: Arc::new(tokio::sync::AMutex::new(())),
        ssd_prepare_complete: false,
        ssd_backing: None,
    }));
    item_resp(
        item,
        OwnerReclaimItemState::Prepared,
        "owner local index fenced",
    )
}

/// Persist one bounded master-capacity batch while every exact source remains
/// fenced. Per-key async locks make an overlapping RPC replay join the first
/// attempt. The storage layer then admits the whole batch without queueing,
/// inserts every independent generation, and executes one durability barrier.
async fn persist_prepared_reclaim_batch_to_ssd(
    inner: &ClientKvApiInner,
    items: &[OwnerReclaimItem],
) -> Vec<Option<u64>> {
    let mut backing_lens = vec![None; items.len()];
    if items.is_empty() || inner.ssd_storage.is_none() {
        return backing_lens;
    }

    let mut prepare_locks = Vec::new();
    for (index, item) in items.iter().enumerate() {
        if item.reason != OwnerReclaimReason::MasterAllocationCapacity {
            continue;
        }
        let controls = inner.owner_key_control.lock_key(&item.key);
        let Some(OwnerReclaimRecord::Prepared(prepared)) = controls
            .get(&item.key)
            .and_then(|state| state.reclaim.as_ref())
        else {
            continue;
        };
        if prepared.item != *item {
            continue;
        }
        if prepared.ssd_prepare_complete {
            backing_lens[index] = prepared.ssd_backing.as_ref().map(|backing| backing.len);
            continue;
        }
        prepare_locks.push((item.key.clone(), index, prepared.ssd_prepare_lock.clone()));
    }
    prepare_locks.sort_by(|left, right| left.0.cmp(&right.0));
    debug_assert!(
        prepare_locks
            .windows(2)
            .all(|window| window[0].0 != window[1].0)
    );
    let mut prepare_guards = Vec::with_capacity(prepare_locks.len());
    for (_, _, lock) in &prepare_locks {
        prepare_guards.push(lock.clone().lock_owned().await);
    }

    let mut sources = Vec::new();
    let mut source_indices = Vec::new();
    let mut source_holders = Vec::new();
    for (_, index, _) in &prepare_locks {
        let item = &items[*index];
        let controls = inner.owner_key_control.lock_key(&item.key);
        let Some(OwnerReclaimRecord::Prepared(prepared)) = controls
            .get(&item.key)
            .and_then(|state| state.reclaim.as_ref())
        else {
            continue;
        };
        if prepared.item != *item {
            continue;
        }
        if prepared.ssd_prepare_complete {
            backing_lens[*index] = prepared.ssd_backing.as_ref().map(|backing| backing.len);
            continue;
        }
        let (addr, len, holder) = match &prepared.source {
            OwnerPreparedReclaimSource::Indexed { cached_info, .. } => {
                let holder = cached_info.mem_holder.clone();
                (holder.addr, u64::from(holder.len), Some(holder))
            }
            OwnerPreparedReclaimSource::UnindexedAllocation { addr, len } => (*addr, *len, None),
            OwnerPreparedReclaimSource::UnindexedOwnerSlot { .. } => continue,
        };
        sources.push(crate::kv_ssd_storage::KvSsdPersistSource {
            key: item.key.clone(),
            put_id: item.put_id,
            addr,
            len,
        });
        source_indices.push(*index);
        source_holders.push(holder);
    }

    let persisted = if sources.is_empty() {
        Vec::new()
    } else {
        match inner.persist_local_kvs_to_ssd(&sources).await {
            Ok(results) => results,
            Err(err) => {
                tracing::warn!(
                    items = sources.len(),
                    error = %err,
                    "master-capacity SSD batch validation failed; continuing DRAM reclaim"
                );
                sources
                    .iter()
                    .map(|_| None)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .map(Ok)
                    .collect()
            }
        }
    };
    drop(source_holders);

    let mut discard = Vec::new();
    for ((index, source), outcome) in source_indices
        .into_iter()
        .zip(sources.into_iter())
        .zip(persisted.into_iter())
    {
        let item = &items[index];
        let mut persist_guard = match outcome {
            Ok(Some(guard)) => Some(guard),
            Ok(None) => None,
            Err(err) => {
                tracing::warn!(
                    key = item.key,
                    put_time_ms = item.put_id.0,
                    put_version = item.put_id.1,
                    epoch = item.epoch,
                    len = source.len,
                    error = %err,
                    "master-capacity victim SSD write-back failed; continuing DRAM reclaim"
                );
                None
            }
        };
        let attached = {
            let mut controls = inner.owner_key_control.lock_key(&item.key);
            match controls
                .get_mut(&item.key)
                .and_then(|state| state.reclaim.as_mut())
            {
                Some(OwnerReclaimRecord::Prepared(prepared)) if prepared.item == *item => {
                    if prepared.ssd_prepare_complete {
                        Some(prepared.ssd_backing.as_ref().map(|backing| backing.len))
                    } else {
                        prepared.ssd_prepare_complete = true;
                        if let Some(guard) = persist_guard.take() {
                            prepared.ssd_backing = Some(OwnerPreparedSsdBacking {
                                len: source.len,
                                _persist_guard: guard,
                            });
                            Some(Some(source.len))
                        } else {
                            Some(None)
                        }
                    }
                }
                _ => None,
            }
        };
        match attached {
            Some(len) => backing_lens[index] = len,
            None => {
                let should_discard = persist_guard.is_some();
                drop(persist_guard);
                if should_discard {
                    discard.push((item.key.clone(), item.put_id));
                }
            }
        }
    }
    drop(prepare_guards);
    for (key, put_id) in discard {
        inner.discard_local_ssd_replica(&key, put_id).await;
    }
    backing_lens
}

fn release_prepared_backing_now(
    inner: &ClientKvApiInner,
    prepared: OwnerPreparedReclaim,
    release_route: bool,
) {
    match prepared.source {
        OwnerPreparedReclaimSource::Indexed { cached_info, .. } => {
            let mut memory_info = Arc::try_unwrap(cached_info.mem_holder).unwrap_or_else(|_| {
                panic!(
                    "owner reclaim prepared memory unexpectedly gained a holder: key={} epoch={}",
                    prepared.item.key, prepared.item.epoch
                )
            });
            match &prepared.item.backing {
                OwnerReclaimBacking::Allocation => {
                    assert!(release_route, "Allocation cannot be metadata-only demoted");
                    assert!(
                        memory_info.local_reserve_resident_slot_ref().is_none(),
                        "allocation reclaim must not carry a local-reserve slot"
                    );
                }
                OwnerReclaimBacking::UnindexedAllocation { .. } => {
                    unreachable!("indexed reclaim source cannot name an unindexed Allocation")
                }
                OwnerReclaimBacking::CommittedSlot {
                    allocation_id,
                    segment_offset,
                    capacity_bytes,
                } => {
                    let (actual_allocation_id, actual_segment_offset, actual_capacity_bytes) =
                        memory_info
                            .take_local_reserve_resident_slot_ref()
                            .expect("committed-slot reclaim must carry a local-reserve slot");
                    assert_eq!(actual_allocation_id, *allocation_id);
                    assert_eq!(actual_segment_offset, *segment_offset);
                    assert_eq!(actual_capacity_bytes, *capacity_bytes);

                    if release_route {
                        inner
                            .owner_release_local_reserve_committed_resident_slot(
                                actual_allocation_id,
                                actual_segment_offset,
                                actual_capacity_bytes,
                            )
                            .expect("owner reclaim committed resident slot release must succeed");
                    } else {
                        inner
                            .owner_release_local_reserve_resident_slot_holder(
                                actual_allocation_id,
                                actual_segment_offset,
                                actual_capacity_bytes,
                            )
                            .expect("owner scope demotion resident holder release must succeed");
                    }
                }
            }
            drop(memory_info);
        }
        OwnerPreparedReclaimSource::UnindexedAllocation { .. } => {
            assert!(matches!(
                prepared.item.backing,
                OwnerReclaimBacking::UnindexedAllocation { .. }
            ));
            // The master route still owns the Allocation. It is removed only after this Commit
            // response, which releases the physical bytes on the master side.
        }
        OwnerPreparedReclaimSource::UnindexedOwnerSlot {
            allocation_id,
            segment_offset,
            capacity_bytes,
        } => {
            assert!(
                release_route,
                "GlobalShared owner slot can only be freed after master removes the exact route"
            );
            inner
                .owner_release_local_reserve_committed_slot_route(
                    allocation_id,
                    segment_offset,
                    capacity_bytes,
                )
                .expect("GlobalShared owner slot route release must succeed");
        }
    }
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
        && state.remote_put.is_none()
        && state.local_ssd_put.is_none()
        && state.source_eviction_selection.is_none()
        && state.local_access_fence.is_some()
}

fn commit_one_with_route_disposition(
    inner: &ClientKvApiInner,
    item: &OwnerReclaimItem,
    release_route: bool,
) -> OwnerReclaimItemResp {
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
    if !release_route {
        let OwnerReclaimBacking::CommittedSlot {
            allocation_id,
            segment_offset,
            capacity_bytes,
        } = &prepared.item.backing
        else {
            unreachable!("metadata-only demotion requires an owner committed slot")
        };
        let mut allocator = inner.owner_segment_allocator.lock();
        if inner.test_spec_config.legacy_a19_global_demotion_one_phase {
            allocator
                .commit_legacy_a19_global_demotion(
                    &prepared.item.key,
                    prepared.item.put_id,
                    *allocation_id,
                    *segment_offset,
                    *capacity_bytes,
                )
                .expect("legacy a19 owner scope demotion must keep the exact slot stable");
        } else {
            allocator
                .commit_reserved_global_demotion(
                    &prepared.item.key,
                    prepared.item.put_id,
                    *allocation_id,
                    *segment_offset,
                    *capacity_bytes,
                )
                .expect("owner scope demotion reservation must remain valid through commit");
        }
    }
    release_prepared_backing_now(inner, prepared, release_route);

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
        if release_route {
            "owner committed slot route and resident holder released"
        } else {
            "owner slot resident holder released after GlobalShared demotion"
        },
    )
}

fn commit_one(inner: &ClientKvApiInner, item: &OwnerReclaimItem) -> OwnerReclaimItemResp {
    commit_one_with_route_disposition(inner, item, true)
}

fn commit_demoted_one(inner: &ClientKvApiInner, item: &OwnerReclaimItem) -> OwnerReclaimItemResp {
    commit_one_with_route_disposition(inner, item, false)
}

#[cfg(test)]
mod tests {
    use super::{
        OWNER_RECLAIM_SOURCE_READ_GRACE, reclaim_key_control_busy_detail,
        reclaim_release_fence_is_intact,
    };
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
    fn reclaim_source_read_grace_is_short_and_bounded() {
        assert_eq!(
            OWNER_RECLAIM_SOURCE_READ_GRACE,
            std::time::Duration::from_millis(50)
        );
    }

    #[test]
    fn remote_get_marker_can_overlap_reclaim_commit() {
        let item = OwnerReclaimItem {
            key: "remote-during-reclaim".to_string(),
            put_id: (7, 1),
            epoch: 9,
            backing: OwnerReclaimBacking::CommittedSlot {
                allocation_id: 3,
                segment_offset: 4 * 4096,
                capacity_bytes: 4096,
            },
            reason: OwnerReclaimReason::OwnerCapacityEviction,
        };
        let mut state = OwnerKeyControlState {
            external_get: Some(Arc::new(ExternalGetKeySharedOp::new(
                "remote-during-reclaim".to_string(),
            ))),
            ..Default::default()
        };
        state.reclaim = Some(OwnerReclaimRecord::Releasing(item.clone()));
        state.begin_local_access_fence();
        assert!(reclaim_release_fence_is_intact(&state, &item));

        let mut local_put_state = OwnerKeyControlState {
            local_puts: 1,
            external_get: state.external_get,
            ..Default::default()
        };
        local_put_state.reclaim = Some(OwnerReclaimRecord::Releasing(item.clone()));
        local_put_state.begin_local_access_fence();
        assert!(!reclaim_release_fence_is_intact(&local_put_state, &item));
    }
}

struct AbortOneResult {
    response: OwnerReclaimItemResp,
    discard_ssd: bool,
}

fn abort_one_fenced(inner: &ClientKvApiInner, item: &OwnerReclaimItem) -> AbortOneResult {
    let mut controls = inner.owner_key_control.lock_key(&item.key);
    let Some(state) = controls.get_mut(&item.key) else {
        return AbortOneResult {
            response: item_resp(
                item,
                OwnerReclaimItemState::Aborted,
                "owner reclaim was already absent",
            ),
            discard_ssd: false,
        };
    };
    let Some(record) = state.reclaim.take() else {
        return AbortOneResult {
            response: item_resp(
                item,
                OwnerReclaimItemState::Aborted,
                "owner reclaim was already absent",
            ),
            discard_ssd: false,
        };
    };
    match record {
        OwnerReclaimRecord::Prepared(prepared) if prepared.item == *item => {
            let OwnerPreparedReclaim {
                source,
                ssd_backing,
                ..
            } = prepared;
            let detail = match source {
                OwnerPreparedReclaimSource::Indexed {
                    cached_info,
                    local_snapshot,
                } => {
                    let replaced = inner.get_cached_info.insert(item.key.clone(), cached_info);
                    assert!(
                        replaced.is_none(),
                        "owner reclaim abort must restore an empty local index slot"
                    );
                    if let Some(snapshot) = local_snapshot {
                        let replaced = inner.local_snapshot_info.insert(item.key.clone(), snapshot);
                        assert!(
                            replaced.is_none(),
                            "owner reclaim abort must restore an empty local snapshot slot"
                        );
                    }
                    "owner local index fence rolled back"
                }
                OwnerPreparedReclaimSource::UnindexedAllocation { .. } => {
                    "master-owned Allocation source fence rolled back"
                }
                OwnerPreparedReclaimSource::UnindexedOwnerSlot { .. } => {
                    "GlobalShared owner slot reclaim fence rolled back"
                }
            };
            state.finish_local_access_fence();
            if state.is_idle() {
                controls.remove(&item.key);
            }
            AbortOneResult {
                response: item_resp(item, OwnerReclaimItemState::Aborted, detail),
                discard_ssd: ssd_backing.is_some(),
            }
        }
        OwnerReclaimRecord::Releasing(releasing) if releasing == *item => {
            state.reclaim = Some(OwnerReclaimRecord::Releasing(releasing));
            AbortOneResult {
                response: item_resp(
                    item,
                    OwnerReclaimItemState::Busy,
                    "owner slot release is already in progress and cannot be aborted",
                ),
                discard_ssd: false,
            }
        }
        OwnerReclaimRecord::Committed(committed) if committed == *item => {
            state.reclaim = Some(OwnerReclaimRecord::Committed(committed));
            AbortOneResult {
                response: item_resp(
                    item,
                    OwnerReclaimItemState::Committed,
                    "owner slot was already committed and cannot be restored",
                ),
                discard_ssd: false,
            }
        }
        other => {
            state.reclaim = Some(other);
            AbortOneResult {
                response: item_resp(
                    item,
                    OwnerReclaimItemState::Stale,
                    "owner reclaim epoch or slot identity changed",
                ),
                discard_ssd: false,
            }
        }
    }
}

async fn abort_one(inner: &ClientKvApiInner, item: &OwnerReclaimItem) -> OwnerReclaimItemResp {
    let outcome = abort_one_fenced(inner, item);
    if outcome.discard_ssd {
        inner
            .discard_local_ssd_replica(&item.key, item.put_id)
            .await;
    }
    outcome.response
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
            state.finish_local_access_fence();
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

pub(crate) fn complete_owner_source_eviction(
    inner: &ClientKvApiInner,
    victim: &OwnerSourceEvictionVictim,
    epoch: u64,
) -> Result<(), String> {
    let item = OwnerReclaimItem {
        key: victim.key.clone(),
        put_id: victim.put_id,
        epoch,
        backing: victim.backing.clone(),
        reason: OwnerReclaimReason::OwnerCapacityEviction,
    };
    let prepared = prepare_one(inner, &item);
    match prepared.state {
        OwnerReclaimItemState::Prepared => {}
        OwnerReclaimItemState::Committed => {
            let finalized = finalize_one(inner, &item);
            return (finalized.state == OwnerReclaimItemState::Finalized)
                .then_some(())
                .ok_or(finalized.detail);
        }
        OwnerReclaimItemState::Finalized => return Ok(()),
        _ => return Err(prepared.detail),
    }

    let committed = commit_one(inner, &item);
    if committed.state != OwnerReclaimItemState::Committed {
        return Err(committed.detail);
    }
    let finalized = finalize_one(inner, &item);
    (finalized.state == OwnerReclaimItemState::Finalized)
        .then_some(())
        .ok_or(finalized.detail)
}

pub(crate) fn complete_owner_source_demotion(
    inner: &ClientKvApiInner,
    victim: &OwnerSourceEvictionVictim,
    epoch: u64,
) -> Result<(), String> {
    let OwnerReclaimBacking::CommittedSlot {
        allocation_id,
        segment_offset,
        capacity_bytes,
    } = &victim.backing
    else {
        return Err("metadata-only demotion requires an exact committed owner slot".to_string());
    };
    if !inner.test_spec_config.legacy_a19_global_demotion_one_phase {
        match inner
            .owner_segment_allocator
            .lock()
            .try_reserve_global_demotion(
                &victim.key,
                victim.put_id,
                *allocation_id,
                *segment_offset,
                *capacity_bytes,
            ) {
            Ok(true) => {}
            Ok(false) => {
                return Err(
                    "metadata-only demotion is waiting for exact GlobalShared owner headroom"
                        .to_string(),
                );
            }
            Err(error) => return Err(error.detail),
        }
    }
    let item = OwnerReclaimItem {
        key: victim.key.clone(),
        put_id: victim.put_id,
        epoch,
        backing: victim.backing.clone(),
        reason: OwnerReclaimReason::OwnerCapacityEviction,
    };
    let prepared = prepare_one(inner, &item);
    match prepared.state {
        OwnerReclaimItemState::Prepared => {}
        OwnerReclaimItemState::Committed => {
            let finalized = finalize_one(inner, &item);
            return (finalized.state == OwnerReclaimItemState::Finalized)
                .then_some(())
                .ok_or(finalized.detail);
        }
        OwnerReclaimItemState::Finalized => return Ok(()),
        _ => return Err(prepared.detail),
    }

    let committed = commit_demoted_one(inner, &item);
    if committed.state != OwnerReclaimItemState::Committed {
        return Err(committed.detail);
    }
    let finalized = finalize_one(inner, &item);
    (finalized.state == OwnerReclaimItemState::Finalized)
        .then_some(())
        .ok_or(finalized.detail)
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
    let phase_started_at = std::time::Instant::now();
    let inner = view.client_kv_api().inner();
    let phase = req.serialize_part.phase;
    let items = match phase {
        OwnerReclaimPhase::Prepare => {
            let mut responses = req
                .serialize_part
                .items
                .iter()
                .map(|item| prepare_one(inner, item))
                .collect::<Vec<_>>();
            let prepared_indices = responses
                .iter()
                .enumerate()
                .filter_map(|(index, response)| {
                    (response.state == OwnerReclaimItemState::Prepared).then_some(index)
                })
                .collect::<Vec<_>>();
            for indices in prepared_indices.chunks(crate::kv_ssd_storage::MAX_PERSIST_BATCH_ITEMS) {
                let batch = indices
                    .iter()
                    .map(|index| req.serialize_part.items[*index].clone())
                    .collect::<Vec<_>>();
                let persisted = persist_prepared_reclaim_batch_to_ssd(inner, &batch).await;
                for (index, ssd_backing_len) in indices.iter().copied().zip(persisted) {
                    responses[index].ssd_backing_len = ssd_backing_len;
                }
            }
            responses
        }
        OwnerReclaimPhase::Commit => {
            if !req.serialize_part.items.is_empty() {
                tracing::debug!(
                    items = req.serialize_part.items.len(),
                    grace_ms = OWNER_RECLAIM_SOURCE_READ_GRACE.as_millis(),
                    "owner reclaim retaining route-deleted sources for the read grace window"
                );
                tokio::time::sleep(OWNER_RECLAIM_SOURCE_READ_GRACE).await;
            }
            req.serialize_part
                .items
                .iter()
                .map(|item| commit_one(inner, item))
                .collect()
        }
        OwnerReclaimPhase::Abort => {
            futures::future::join_all(
                req.serialize_part
                    .items
                    .iter()
                    .map(|item| abort_one(inner, item)),
            )
            .await
        }
        OwnerReclaimPhase::Finalize => req
            .serialize_part
            .items
            .iter()
            .map(|item| finalize_one(inner, item))
            .collect(),
    };
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
    let ssd_prepared = items
        .iter()
        .filter(|item| item.ssd_backing_len.is_some())
        .count();
    let ssd_prepared_bytes = items
        .iter()
        .filter_map(|item| item.ssd_backing_len)
        .fold(0_u64, u64::saturating_add);
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
    let duration_us = phase_started_at.elapsed().as_micros().min(u64::MAX as u128) as u64;
    tracing::info!(
        "owner reclaim phase completed: phase={:?} items={} prepared={} committed={} finalized={} ssd_prepared={} ssd_prepared_bytes={} busy_or_stale={} rejection_reasons={:?} duration_us={}",
        phase,
        items.len(),
        prepared,
        committed,
        finalized,
        ssd_prepared,
        ssd_prepared_bytes,
        busy_or_stale,
        rejection_reason_counts,
        duration_us
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
