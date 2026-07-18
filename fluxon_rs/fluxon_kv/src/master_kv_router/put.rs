use super::{
    CommittedSlotReplica, InflightPutAllocation, InflightPutCommitInfo, InflightPutInfo,
    InflightReplicaTaskInfo, KvReplicaBacking, KvRouteInfo, LocalReserveGrantInfo,
    MasterKeyActivityCompletionGuard, MasterKvRouterView, NodeCacheCapacityReservation,
    OwnerHoldingGetInfo, PreparedPutKeyReservationInfo, PutPlacementMode, ReservedCapacityReason,
    msg_pack::{
        BatchPreparePutKeysReq, BatchPreparePutKeysResp, BatchPutAppendDoneItemResp,
        BatchPutAppendDoneReq, BatchPutAppendDoneResp, BatchPutAppendStartItemResp,
        BatchPutAppendStartReq, BatchPutAppendStartResp, BatchPutDoneItemResp, BatchPutDoneReq,
        BatchPutDoneResp, BatchPutRevokeItemResp, BatchPutRevokeReq, BatchPutRevokeResp,
        BatchPutStartItemResp, BatchPutStartReq, BatchPutStartResp,
        BatchReleasePutKeyReservationsReq, BatchReleasePutKeyReservationsResp,
        GroupedBatchPutDoneReq, GroupedBatchPutDoneResp, PutAppendDoneReq, PutAppendDoneResp,
        PutAppendRevokeReq, PutAppendRevokeResp, PutAppendStartOutcome, PutAppendStartReq,
        PutAppendStartResp, PutAtomicGroup, PutDoneCommittedSlot, PutDoneReq, PutDoneResp,
        PutRevokeReq, PutRevokeResp, PutStartReq, PutStartResp, ReleaseLocalGrantReq,
        ReleaseLocalGrantResp, ReserveLocalGrantOutcome, ReserveLocalGrantReq,
        ReserveLocalGrantResp, build_shared_put_atomic_group_assignments,
    },
    node_generation_is_current_live,
    placement::PutPlacementTarget,
    publish_primary_route_tomb_fenced, publish_route_replica_tomb_fenced,
    route_maintenance::{
        RoutePublishEvent, apply_post_route_maintenance_batch, enqueue_post_route_maintenance,
    },
};
use crate::master_kv_router::OneKvNodesRoutes;
use crate::master_kv_router::delete::DeleteKeyInfo;
use crate::memholder::MemholderManagerTrait;
use crate::{
    OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES,
    cluster_manager::{
        META_KEY_LOCAL_IPC_ROOT, META_KEY_SHARED_STORAGE_NODE_ID,
        META_KEY_SHARED_STORAGE_NODE_START_TIME, NodeID,
    },
    master_seg_manager::{MasterSegManagerAccessTrait, one_seg_allocator::Allocation},
    p2p::msg_pack::MsgPack,
    rpcresp_kvresult_convert::msg_and_error::{self, kv},
};
use chrono::Utc;
use limit_thirdparty::tokio;
use parking_lot::Mutex;
use parking_lot::RwLock;
use rand::seq::SliceRandom;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, atomic::AtomicU32},
};

pub type PutIDForAKey = (u64, u32);

fn validate_put_start_source_node_override(
    view: &MasterKvRouterView,
    requester_node_id: &NodeID,
    source_node_id: &NodeID,
) -> msg_and_error::KvResult<()> {
    if requester_node_id == source_node_id {
        return Ok(());
    }

    let requester = view
        .cluster_manager()
        .get_member_info_cached(requester_node_id.as_ref())
        .ok_or_else(|| {
            msg_and_error::KvError::Api(msg_and_error::ApiError::Unknown {
                detail: format!(
                    "put_start source override requester not found in cluster cache: requester={} source={}",
                    requester_node_id, source_node_id
                ),
            })
        })?;
    let source = view
        .cluster_manager()
        .get_member_info_cached(source_node_id.as_ref())
        .ok_or_else(|| {
            msg_and_error::KvError::Api(msg_and_error::ApiError::Unknown {
                detail: format!(
                    "put_start source override source node not found in cluster cache: requester={} source={}",
                    requester_node_id, source_node_id
                ),
            })
        })?;

    if requester
        .metadata
        .get("side_transfer_worker")
        .is_some_and(|value| value == "true")
        == false
    {
        return Err(msg_and_error::KvError::Api(
            msg_and_error::ApiError::Unknown {
                detail: format!(
                    "put_start source override is only allowed for side-transfer workers: requester={} source={}",
                    requester_node_id, source_node_id
                ),
            },
        ));
    }

    if requester
        .metadata
        .get(META_KEY_SHARED_STORAGE_NODE_ID)
        .is_some_and(|value| value == source_node_id.as_ref())
        == false
    {
        return Err(msg_and_error::KvError::Api(
            msg_and_error::ApiError::Unknown {
                detail: format!(
                    "put_start source override owner mismatch: requester={} source={} requester_owner={:?}",
                    requester_node_id,
                    source_node_id,
                    requester.metadata.get(META_KEY_SHARED_STORAGE_NODE_ID)
                ),
            },
        ));
    }

    let requester_owner_start_time = requester
        .metadata
        .get(META_KEY_SHARED_STORAGE_NODE_START_TIME)
        .and_then(|value| value.parse::<i64>().ok());
    if requester_owner_start_time != Some(source.node_start_time) {
        return Err(msg_and_error::KvError::Api(
            msg_and_error::ApiError::Unknown {
                detail: format!(
                    "put_start source override owner generation mismatch: requester={} source={} requester_owner_start={:?} source_start={}",
                    requester_node_id,
                    source_node_id,
                    requester_owner_start_time,
                    source.node_start_time
                ),
            },
        ));
    }

    let requester_ipc_root = requester.metadata.get(META_KEY_LOCAL_IPC_ROOT);
    let source_ipc_root = source.metadata.get(META_KEY_LOCAL_IPC_ROOT);
    if requester_ipc_root.is_none() || requester_ipc_root != source_ipc_root {
        return Err(msg_and_error::KvError::Api(
            msg_and_error::ApiError::Unknown {
                detail: format!(
                    "put_start source override local_ipc_root mismatch: requester={} source={} requester_ipc_root={:?} source_ipc_root={:?}",
                    requester_node_id, source_node_id, requester_ipc_root, source_ipc_root
                ),
            },
        ));
    }

    Ok(())
}

fn current_route_append_outcome(
    route: &OneKvNodesRoutes,
    source_node_id: &NodeID,
    verify_put_id: PutIDForAKey,
) -> PutAppendStartOutcome {
    let has_complete_remote = route
        .nodes_replicas
        .read()
        .values()
        .any(|replica| !replica.tomb_tag.is_tomb() && replica.node_id != *source_node_id);
    classify_put_append_start_outcome(route.put_id == verify_put_id, has_complete_remote)
}

fn classify_put_append_start_outcome(
    current_identity: bool,
    has_complete_remote: bool,
) -> PutAppendStartOutcome {
    if !current_identity {
        PutAppendStartOutcome::Obsolete
    } else if has_complete_remote {
        PutAppendStartOutcome::AlreadySatisfied
    } else {
        PutAppendStartOutcome::Scheduled
    }
}

#[cfg(test)]
mod append_start_outcome_tests {
    use super::{PutAppendStartOutcome, classify_put_append_start_outcome};

    #[test]
    fn append_start_never_conflates_no_remote_with_already_satisfied() {
        assert_eq!(
            classify_put_append_start_outcome(true, false),
            PutAppendStartOutcome::Scheduled
        );
        assert_eq!(
            classify_put_append_start_outcome(true, true),
            PutAppendStartOutcome::AlreadySatisfied
        );
        assert_eq!(
            classify_put_append_start_outcome(false, true),
            PutAppendStartOutcome::Obsolete
        );
        assert_ne!(
            PutAppendStartOutcome::RetryableNoSpace,
            PutAppendStartOutcome::AlreadySatisfied
        );
    }
}

fn append_current_route_replica_if_matching(
    view: &MasterKvRouterView,
    key: &str,
    put_id: PutIDForAKey,
    node_id: NodeID,
    target_tomb_tag: crate::master_seg_manager::NodeTombTag,
    allocation: Allocation,
) -> Option<RoutePublishEvent> {
    let Some(one_kv_nodes_routes) = view.master_kv_router().inner().kv_routes.get(key) else {
        tracing::debug!(
            "append_current_route_replica_if_matching skipped because route disappeared: key={} put_id=({},{})",
            key,
            put_id.0,
            put_id.1
        );
        return None;
    };
    if one_kv_nodes_routes.put_id != put_id {
        tracing::debug!(
            "append_current_route_replica_if_matching skipped because version changed: key={} current_put_id=({},{}) append_put_id=({},{})",
            key,
            one_kv_nodes_routes.put_id.0,
            one_kv_nodes_routes.put_id.1,
            put_id.0,
            put_id.1
        );
        return None;
    }
    if !node_generation_is_current_live(view, &node_id, &target_tomb_tag) {
        tracing::warn!(
            "append_current_route_replica_if_matching skipped because target generation departed: key={} put_id=({},{}) node_id={}",
            key,
            put_id.0,
            put_id.1,
            node_id
        );
        return None;
    }
    let capacity_bytes = allocation.capcity();
    let lease_id = one_kv_nodes_routes.lease_id;
    let capacity_reservation = match lease_id {
        Some(_) => match view.master_kv_router().reserve_node_cache_capacity(
            &node_id,
            &target_tomb_tag,
            ReservedCapacityReason::LeaseBoundKv,
            capacity_bytes,
        ) {
            Ok(reservation) => reservation,
            Err(err) => {
                tracing::warn!(
                    "append_current_route_replica_if_matching could not reserve lease-bound capacity: key={} put_id=({},{}) node_id={} err={}",
                    key,
                    put_id.0,
                    put_id.1,
                    node_id,
                    err,
                );
                return None;
            }
        },
        None => None,
    };
    let published = publish_route_replica_tomb_fenced(
        &one_kv_nodes_routes,
        node_id.clone(),
        KvRouteInfo {
            node_id: node_id.clone(),
            backing: KvReplicaBacking::Allocation(Arc::new(allocation)),
            owner_local_indexed: false,
            get_durable_reservation: None,
            capacity_reservation,
            tomb_tag: target_tomb_tag,
        },
    );
    if !published {
        tracing::warn!(
            "append_current_route_replica_if_matching rejected by generation/live-replica fence: key={} put_id=({},{}) node_id={}",
            key,
            put_id.0,
            put_id.1,
            node_id
        );
        return None;
    }
    Some(RoutePublishEvent::replica_append(
        key.to_string(),
        put_id,
        lease_id,
        node_id,
        capacity_bytes,
    ))
}

fn allocate_from_node_local_segment(
    view: &MasterKvRouterView,
    node_id: &NodeID,
    len: u64,
    op_name: &str,
) -> msg_and_error::KvResult<Allocation> {
    let node_allocators = view.master_seg_manager().get_node_allocators(node_id);
    if node_allocators.is_empty() {
        tracing::warn!("No allocators found for {} node={}", op_name, node_id);
        return Err(msg_and_error::KvError::Api(
            msg_and_error::ApiError::RegisterSegmentFailed {
                detail: format!(
                    "{} node has no registered segments: node={}",
                    op_name, node_id
                ),
            },
        ));
    }

    let allocator = node_allocators.choose(&mut rand::thread_rng()).unwrap();
    for attempt in 1..=3 {
        if let Ok(allocation) = allocator.allocate(len) {
            return Ok(allocation);
        }
        tracing::warn!(
            "Allocation attempt {}/3 failed for {} node={} len={}",
            attempt,
            op_name,
            node_id,
            len
        );
    }

    let total = allocator.total_size_bytes();
    let used = allocator.used_size_bytes();
    let free = total.saturating_sub(used);
    Err(msg_and_error::KvError::Api(
        msg_and_error::ApiError::NoSpace {
            node: node_id.as_ref().to_string(),
            segment: allocator.seg_device_id.clone(),
            total_capacity: total,
            free_capacity: free,
        },
    ))
}

fn validate_put_done_committed_slot(
    view: &MasterKvRouterView,
    node_id: &NodeID,
    slot: &PutDoneCommittedSlot,
    expected_tomb_tag: Option<&crate::master_seg_manager::NodeTombTag>,
) -> msg_and_error::KvResult<(CommittedSlotReplica, crate::master_seg_manager::NodeTombTag)> {
    let invalid = |detail: String| {
        msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument { detail })
    };
    if slot.slot_size == 0 || slot.len > slot.slot_size {
        return Err(invalid(format!(
            "invalid committed local slot geometry: grant_id={} slot_size={} len={}",
            slot.grant_id, slot.slot_size, slot.len
        )));
    }
    let Some(grant) = view
        .master_kv_router()
        .inner()
        .local_reserve_grants
        .get(&slot.grant_id)
    else {
        return Err(invalid(format!(
            "committed local slot references unknown grant_id={}",
            slot.grant_id
        )));
    };
    if grant.owner_node_id != *node_id {
        return Err(invalid(format!(
            "committed local slot owner mismatch: grant_id={} owner={} requester={}",
            slot.grant_id, grant.owner_node_id, node_id
        )));
    }
    let tomb_tag = grant.tomb_tag.clone();
    if !node_generation_is_current_live(view, node_id, &tomb_tag)
        || expected_tomb_tag.is_some_and(|expected| !expected.same_generation(&tomb_tag))
    {
        return Err(invalid(format!(
            "committed local slot belongs to a different/departed owner generation: grant_id={} requester={}",
            slot.grant_id, node_id
        )));
    }

    let grant_base = grant.allocation.base_addr();
    let grant_addr = grant_base
        .checked_add(grant.allocation.addr())
        .ok_or_else(|| invalid("committed local slot grant address overflow".to_string()))?;
    let slot_offset = slot
        .slot_size
        .checked_mul(u64::from(slot.slot_index))
        .ok_or_else(|| invalid("committed local slot offset overflow".to_string()))?;
    let slot_end = slot_offset
        .checked_add(slot.slot_size)
        .ok_or_else(|| invalid("committed local slot end overflow".to_string()))?;
    if slot_end > grant.allocation.capcity() {
        return Err(invalid(format!(
            "committed local slot is outside grant: grant_id={} slot_index={} slot_size={} grant_len={}",
            slot.grant_id,
            slot.slot_index,
            slot.slot_size,
            grant.allocation.capcity()
        )));
    }
    let expected_addr = grant_addr
        .checked_add(slot_offset)
        .ok_or_else(|| invalid("committed local slot address overflow".to_string()))?;
    if slot.base_addr != grant_base || slot.addr != expected_addr {
        return Err(invalid(format!(
            "committed local slot address mismatch: grant_id={} expected_base={:#x} got_base={:#x} expected_addr={:#x} got_addr={:#x}",
            slot.grant_id, grant_base, slot.base_addr, expected_addr, slot.addr
        )));
    }

    Ok((
        CommittedSlotReplica {
            owner_node_id: node_id.clone(),
            grant_id: slot.grant_id,
            slot_index: slot.slot_index,
            slot_size: slot.slot_size,
            addr: slot.addr,
            len: slot.len,
            base_addr: slot.base_addr,
        },
        tomb_tag,
    ))
}

async fn prepare_route_state(
    view: &MasterKvRouterView,
    lease_id: Option<u64>,
    key: &str,
    put_id: PutIDForAKey,
    node_id: &NodeID,
    tomb_tag: &crate::master_seg_manager::NodeTombTag,
    reservation_reason: Option<ReservedCapacityReason>,
    target_cap_bytes: u64,
) -> msg_and_error::KvResult<Option<Arc<NodeCacheCapacityReservation>>> {
    // Reserve first. If lease attachment fails, dropping this local token
    // restores the exact generation-scoped counter automatically. Committed
    // slots do not reserve again because their whole grant is already excluded.
    let reservation = match reservation_reason {
        Some(reason) => view.master_kv_router().reserve_node_cache_capacity(
            node_id,
            tomb_tag,
            reason,
            target_cap_bytes,
        )?,
        None => None,
    };
    if let Some(lease_id) = lease_id {
        view.master_lease_manager()
            .attach_key(lease_id, key.to_string(), put_id)
            .await
            .map_err(|err| -> msg_and_error::KvError { err.into() })?;
    }
    Ok(reservation)
}

fn reserve_replica_task(
    view: &MasterKvRouterView,
    key: &str,
    put_id: PutIDForAKey,
    source_node_id: &NodeID,
    preferred_sub_cluster: Option<&str>,
    len: u64,
) -> msg_and_error::KvResult<InflightReplicaTaskInfo> {
    reserve_replica_task_excluding(
        view,
        key,
        put_id,
        source_node_id,
        preferred_sub_cluster,
        len,
        &HashSet::new(),
        true,
    )
}

fn reserve_replica_task_excluding(
    view: &MasterKvRouterView,
    key: &str,
    put_id: PutIDForAKey,
    source_node_id: &NodeID,
    preferred_sub_cluster: Option<&str>,
    len: u64,
    excluded_nodes: &HashSet<NodeID>,
    protect_source_on_remote_complete: bool,
) -> msg_and_error::KvResult<InflightReplicaTaskInfo> {
    let activity_lease = view.master_kv_router().reserve_inflight_replica_key(key)?;
    view.master_kv_router()
        .pin_current_master_cache_identity_for_activity(
            &activity_lease,
            source_node_id.as_ref(),
            key,
            put_id,
        );
    let (target_node_id, target_allocation) = view
        .master_kv_router()
        .inner()
        .policy
        .select_remote_target(
            view,
            source_node_id,
            excluded_nodes,
            preferred_sub_cluster,
            len,
        )?;
    let Some(target_tomb_tag) = view
        .master_seg_manager()
        .get_allocation_tomb_tag(&target_node_id, &target_allocation)
    else {
        return Err(msg_and_error::KvError::Api(
            msg_and_error::ApiError::InvalidPutMasterState {
                detail: format!(
                    "replica target generation changed during reservation: key={} put_id=({},{}) target_node_id={}",
                    key, put_id.0, put_id.1, target_node_id
                ),
            },
        ));
    };
    tracing::debug!(
        "replica task reserved: key={} put_id=({},{}) source_node_id={} target_node_id={} preferred_sub_cluster={:?} len={}",
        key,
        put_id.0,
        put_id.1,
        source_node_id,
        target_node_id,
        preferred_sub_cluster,
        len
    );
    Ok(InflightReplicaTaskInfo {
        node_id: target_node_id,
        target_tomb_tag,
        source_node_id: source_node_id.clone(),
        key: key.to_string(),
        put_id,
        target_allocation: Arc::new(Mutex::new(Some(target_allocation))),
        protect_source_on_remote_complete,
        _activity_lease: activity_lease,
    })
}

async fn publish_completed_put_route(
    view: MasterKvRouterView,
    key: String,
    put_id: PutIDForAKey,
    lease_id_opt: Option<u64>,
    atomic_group: Option<Arc<super::msg_pack::PutAtomicGroup>>,
    node_id: NodeID,
    completed_info: KvRouteInfo,
    target_cap_bytes: u64,
    local_cache_holder_id: Option<u64>,
) -> MsgPack<PutDoneResp> {
    let publish_tag = completed_info.tomb_tag.clone();
    let new_route = Arc::new(OneKvNodesRoutes {
        put_id,
        lease_id: lease_id_opt,
        atomic_group: atomic_group.clone(),
        nodes_replicas: RwLock::new(HashMap::from([(node_id.clone(), completed_info)])),
        get_durable_slots_used: AtomicU32::new(0),
    });
    let old_one_kv_routes = match publish_primary_route_tomb_fenced(
        &view.master_kv_router().inner().kv_routes,
        &key,
        new_route,
        &publish_tag,
    ) {
        Ok(previous) => previous,
        Err(()) => {
            if let Some(lease_id) = lease_id_opt {
                view.master_lease_manager()
                    .detach_key_if_version(lease_id, &key, put_id);
            }
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
                detail: format!(
                    "primary route publication rejected because target generation departed: key={} put_id=({},{}) node_id={}",
                    key, put_id.0, put_id.1, node_id
                ),
            });
            return MsgPack {
                serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                raw_bytes: Vec::new(),
            };
        }
    };

    if let Some(old) = old_one_kv_routes {
        view.master_kv_router()
            .remove_route_cache_entries_exact(&key, &old)
            .await;
        if let Err(err) = view
            .master_kv_router()
            .inner()
            .delete_broadcast
            .sender()
            .send(DeleteKeyInfo::Key {
                key: key.clone(),
                nodes_kv_route_info: old,
            })
            .await
        {
            tracing::warn!("Failed to send delete broadcast: {}", err);
        }
    }

    apply_post_route_maintenance_batch(
        &view,
        vec![RoutePublishEvent::primary_put(
            key.clone(),
            put_id,
            lease_id_opt,
            node_id.clone(),
            target_cap_bytes,
        )],
    )
    .await;

    tracing::debug!(
        "Completed put operation with put_id: {:?}, key: {:?}",
        put_id,
        key
    );

    MsgPack {
        serialize_part: PutDoneResp {
            error_code: msg_and_error::OK,
            error_json: String::new(),
            server_process_us: 0,
            local_cache_holder_id,
        },
        raw_bytes: Vec::new(),
    }
}

pub async fn handle_put_start(
    view: MasterKvRouterView,
    req: MsgPack<PutStartReq>,
    req_node_id: NodeID,
) -> (PutIDForAKey, MsgPack<PutStartResp>) {
    let key = req.serialize_part.key.clone();
    let activity_lease = match view.master_kv_router().reserve_inflight_put_key(
        &key,
        req.serialize_part.reject_if_inflight_same_key,
        req.serialize_part.reject_if_exist_same_key,
    ) {
        Ok(activity_lease) => activity_lease,
        Err(err) => {
            let resp: PutStartResp = crate::rpcresp_kvresult_convert::FromError::from_error(&err);
            return (
                (0, 0),
                MsgPack {
                    serialize_part: resp,
                    raw_bytes: Vec::new(),
                },
            );
        }
    };
    let source_node_id = match req.serialize_part.source_node_id.as_ref() {
        Some(source_node_id) => {
            let source_node_id: NodeID = source_node_id.clone().into();
            if let Err(err) =
                validate_put_start_source_node_override(&view, &req_node_id, &source_node_id)
            {
                let resp: PutStartResp =
                    crate::rpcresp_kvresult_convert::FromError::from_error(&err);
                return (
                    (0, 0),
                    MsgPack {
                        serialize_part: resp,
                        raw_bytes: Vec::new(),
                    },
                );
            }
            source_node_id
        }
        None => req_node_id.clone(),
    };
    let put_id: PutIDForAKey = view
        .master_kv_router()
        .get_recent_key_versionid(key.clone());

    let inflight_put_key: (String, u64, u32) = (key.clone(), put_id.0, put_id.1);

    let src_allocation = match allocate_from_node_local_segment(
        &view,
        &source_node_id,
        req.serialize_part.len,
        "put_start source",
    ) {
        Ok(allocation) => allocation,
        Err(err) => {
            let resp: PutStartResp = crate::rpcresp_kvresult_convert::FromError::from_error(&err);
            return (
                (0, 0),
                MsgPack {
                    serialize_part: resp,
                    raw_bytes: Vec::new(),
                },
            );
        }
    };

    // Keep src allocation alive across retry attempts until we have a successful target.
    let mut src_allocation = Some(src_allocation);

    let finalize = |commit_node_id: NodeID,
                    target_tomb_tag: crate::master_seg_manager::NodeTombTag,
                    response_node_id: NodeID,
                    inflight_alloc: InflightPutAllocation,
                    src_addr: u64,
                    target_addr: u64,
                    src_base_addr: u64,
                    target_base_addr: u64,
                    len: u64,
                    replica_target: Option<InflightReplicaTaskInfo>| {
        let info = InflightPutInfo {
            key: key.clone(),
            len,
            req_node_id: req_node_id.clone(),
            commit_info: InflightPutCommitInfo {
                node_id: commit_node_id,
                target_tomb_tag,
                src_target_allocation: Arc::new(Mutex::new(Some(inflight_alloc))),
                replica_target: replica_target.clone(),
            },
            _activity_lease: activity_lease.clone(),
        };

        let view_task = view.clone();
        let inflight_put_key = inflight_put_key.clone();
        async move {
            view_task
                .master_kv_router()
                .inner()
                .inflight_puts
                .insert(inflight_put_key, info)
                .await;

            let response_replica_target = replica_target.as_ref().map(|target| {
                let target_allocation_guard = target.target_allocation.lock();
                let target_allocation = target_allocation_guard.as_ref().expect(
                    "replica target allocation must exist while building put_start response",
                );
                super::msg_pack::PutReplicaTarget {
                    node_id: target.node_id.clone().into(),
                    target_addr: target_allocation.base_addr() + target_allocation.addr(),
                    target_base_addr: target_allocation.base_addr(),
                    len: target_allocation.size(),
                }
            });

            let resp = PutStartResp {
                put_id,
                node_id: response_node_id.into(),
                src_addr,
                target_addr,
                src_base_addr,
                target_base_addr,
                len,
                error_code: msg_and_error::OK,
                error_json: String::new(),
                server_process_us: 0,
                replica_target: response_replica_target,
            };

            (
                put_id,
                MsgPack {
                    serialize_part: resp,
                    raw_bytes: Vec::new(),
                },
            )
        }
    };

    let put_target = if req.serialize_part.make_replica_task {
        Ok(PutPlacementTarget::Local {
            node_id: source_node_id.clone(),
        })
    } else {
        view.master_kv_router()
            .inner()
            .policy
            .select_put_target(
                &view,
                &source_node_id,
                req.serialize_part.preferred_sub_cluster.as_deref(),
                req.serialize_part.len,
            )
            .await
    };

    match put_target {
        Ok(PutPlacementTarget::Local { node_id }) => {
            if node_id != source_node_id {
                unreachable!(
                    "Local placement must be the resolved source node; got node_id={} source_node_id={} requester_node_id={}",
                    node_id, source_node_id, req_node_id
                );
            }

            tracing::debug!(
                "put_start placement decided: local; put_id={:?} key={} requester_node_id={} source_node_id={} target_node_id={} preferred_sub_cluster={:?} len={}",
                put_id,
                key,
                req_node_id,
                source_node_id,
                node_id,
                req.serialize_part.preferred_sub_cluster,
                req.serialize_part.len
            );
            view.master_kv_router().record_put_placement_decision(
                req_node_id.as_ref(),
                node_id.as_ref(),
                PutPlacementMode::Local,
            );

            let src_ref = src_allocation
                .as_ref()
                .expect("src_allocation must exist until put_start returns");
            let src_offset = src_ref.addr();
            let src_base = src_ref.base_addr();
            let allocation_size = src_ref.size();
            let abs = src_base + src_offset;

            let src = src_allocation
                .take()
                .expect("src_allocation must exist when finalizing local put");
            let Some(target_tomb_tag) = view
                .master_seg_manager()
                .get_allocation_tomb_tag(&node_id, &src)
            else {
                let err = msg_and_error::KvError::Api(
                    msg_and_error::ApiError::InvalidPutMasterState {
                        detail: format!(
                            "local put target generation changed before start publication: key={} put_id=({},{}) node_id={}",
                            key, put_id.0, put_id.1, node_id
                        ),
                    },
                );
                return (
                    (0, 0),
                    MsgPack {
                        serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(
                            &err,
                        ),
                        raw_bytes: Vec::new(),
                    },
                );
            };
            let replica_target = if req.serialize_part.make_replica_task {
                match reserve_replica_task(
                    &view,
                    &key,
                    put_id,
                    &source_node_id,
                    req.serialize_part.preferred_sub_cluster.as_deref(),
                    req.serialize_part.len,
                ) {
                    Ok(reservation) => {
                        view.master_kv_router()
                            .record_replica_task_target(reservation.node_id.as_ref());
                        Some(reservation)
                    }
                    Err(msg_and_error::KvError::Api(msg_and_error::ApiError::NoSpace {
                        node,
                        segment,
                        total_capacity,
                        free_capacity,
                    })) => {
                        tracing::info!(
                            "replica task not pre-reserved; local-only commit remains valid: key={} put_id=({},{}) source_node_id={} preferred_sub_cluster={:?} node={} segment={} total_capacity={} free_capacity={}",
                            key,
                            put_id.0,
                            put_id.1,
                            source_node_id,
                            req.serialize_part.preferred_sub_cluster,
                            node,
                            segment,
                            total_capacity,
                            free_capacity
                        );
                        None
                    }
                    Err(err) => {
                        tracing::warn!(
                            "replica task pre-reserve failed; local-only commit remains valid: key={} put_id=({},{}) source_node_id={} preferred_sub_cluster={:?} err={}",
                            key,
                            put_id.0,
                            put_id.1,
                            source_node_id,
                            req.serialize_part.preferred_sub_cluster,
                            err
                        );
                        None
                    }
                }
            } else {
                None
            };
            let fut = finalize(
                node_id.clone(),
                target_tomb_tag,
                node_id,
                InflightPutAllocation::Local(src),
                abs,
                abs,
                src_base,
                src_base,
                allocation_size,
                replica_target,
            );
            return fut.await;
        }
        Ok(PutPlacementTarget::Remote {
            node_id,
            allocation: target_allocation,
            ..
        }) => {
            let src_ref = src_allocation
                .as_ref()
                .expect("src_allocation must exist until put_start returns");

            let src_offset = src_ref.addr();
            let src_base = src_ref.base_addr();
            let target_offset = target_allocation.addr();
            let target_base = target_allocation.base_addr();
            let allocation_size = target_allocation.size();

            tracing::debug!(
                "put_start placement decided: remote; put_id={:?} key={} requester_node_id={} source_node_id={} target_node_id={} preferred_sub_cluster={:?} len={} target_base_addr={} target_offset={} allocation_size={}",
                put_id,
                key,
                req_node_id,
                source_node_id,
                node_id,
                req.serialize_part.preferred_sub_cluster,
                req.serialize_part.len,
                target_base,
                target_offset,
                allocation_size
            );
            view.master_kv_router().record_put_placement_decision(
                req_node_id.as_ref(),
                node_id.as_ref(),
                PutPlacementMode::Remote,
            );

            let src = src_allocation
                .take()
                .expect("src_allocation must exist when finalizing remote put");
            let Some(target_tomb_tag) = view
                .master_seg_manager()
                .get_allocation_tomb_tag(&node_id, &target_allocation)
            else {
                let err = msg_and_error::KvError::Api(
                    msg_and_error::ApiError::InvalidPutMasterState {
                        detail: format!(
                            "remote put target generation changed before start publication: key={} put_id=({},{}) node_id={}",
                            key, put_id.0, put_id.1, node_id
                        ),
                    },
                );
                return (
                    (0, 0),
                    MsgPack {
                        serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(
                            &err,
                        ),
                        raw_bytes: Vec::new(),
                    },
                );
            };
            let fut = finalize(
                node_id.clone(),
                target_tomb_tag,
                node_id,
                InflightPutAllocation::Remote {
                    src,
                    target: target_allocation,
                },
                src_base + src_offset,
                target_base + target_offset,
                src_base,
                target_base,
                allocation_size,
                None,
            );
            return fut.await;
        }
        Err(err) => {
            let resp: PutStartResp = crate::rpcresp_kvresult_convert::FromError::from_error(&err);
            return (
                (0, 0),
                MsgPack {
                    serialize_part: resp,
                    raw_bytes: Vec::new(),
                },
            );
        }
    }
}

pub async fn handle_reserve_local_grant(
    view: MasterKvRouterView,
    _req: MsgPack<ReserveLocalGrantReq>,
    req_node_id: NodeID,
) -> (u64, MsgPack<ReserveLocalGrantResp>) {
    let allocation = match allocate_from_node_local_segment(
        &view,
        &req_node_id,
        OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES,
        "reserve_local_grant",
    ) {
        Ok(allocation) => allocation,
        Err(err) => {
            let resp: ReserveLocalGrantResp =
                crate::rpcresp_kvresult_convert::FromError::from_error(&err);
            return (
                0,
                MsgPack {
                    serialize_part: resp,
                    raw_bytes: Vec::new(),
                },
            );
        }
    };

    let grant_id = view.master_kv_router().next_local_reserve_grant_id();
    let grant_base_addr = allocation.base_addr();
    let grant_abs_addr = grant_base_addr + allocation.addr();
    let grant_len = allocation.capcity();
    let Some(tomb_tag) = view
        .master_seg_manager()
        .get_allocation_tomb_tag(&req_node_id, &allocation)
    else {
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
            detail: format!(
                "local reserve grant generation changed before publication: owner={}",
                req_node_id
            ),
        });
        let resp: ReserveLocalGrantResp =
            crate::rpcresp_kvresult_convert::FromError::from_error(&err);
        return (
            0,
            MsgPack {
                serialize_part: resp,
                raw_bytes: Vec::new(),
            },
        );
    };
    let capacity_reservation = match view.master_kv_router().reserve_node_cache_capacity(
        &req_node_id,
        &tomb_tag,
        ReservedCapacityReason::LocalReserveGrant,
        grant_len,
    ) {
        Ok(reservation) => reservation,
        Err(err) => {
            let resp: ReserveLocalGrantResp =
                crate::rpcresp_kvresult_convert::FromError::from_error(&err);
            return (
                0,
                MsgPack {
                    serialize_part: resp,
                    raw_bytes: Vec::new(),
                },
            );
        }
    };
    view.master_kv_router().install_local_reserve_grant(
        grant_id,
        LocalReserveGrantInfo {
            owner_node_id: req_node_id.clone(),
            tomb_tag: tomb_tag.clone(),
            allocation,
            capacity_reservation,
        },
    );
    if tomb_tag.is_tomb() {
        // MemberLeft may mark the shared tag between allocation and map
        // insertion.  Remove the exact grant before returning so the old
        // allocator cannot be pinned after its cleanup snapshot.
        drop(view.master_kv_router().take_local_reserve_grant(grant_id));
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
            detail: format!(
                "local reserve grant owner departed during publication: owner={} grant_id={}",
                req_node_id, grant_id
            ),
        });
        let resp: ReserveLocalGrantResp =
            crate::rpcresp_kvresult_convert::FromError::from_error(&err);
        return (
            0,
            MsgPack {
                serialize_part: resp,
                raw_bytes: Vec::new(),
            },
        );
    }

    (
        grant_id,
        MsgPack {
            serialize_part: ReserveLocalGrantResp {
                outcome: ReserveLocalGrantOutcome::Granted {
                    grant_id,
                    node_id: req_node_id.into_owned(),
                    addr: grant_abs_addr,
                    base_addr: grant_base_addr,
                    len: grant_len,
                },
                error_code: msg_and_error::OK,
                error_json: String::new(),
                server_process_us: 0,
            },
            raw_bytes: Vec::new(),
        },
    )
}

pub async fn handle_release_local_grant(
    view: MasterKvRouterView,
    req: MsgPack<ReleaseLocalGrantReq>,
    req_node_id: NodeID,
) -> MsgPack<ReleaseLocalGrantResp> {
    let grant_id = req.serialize_part.grant_id;
    let Some(grant) = view.master_kv_router().take_local_reserve_grant(grant_id) else {
        tracing::info!(
            "release_local_grant ignored missing grant_id={} requester_node_id={}",
            grant_id,
            req_node_id
        );
        return MsgPack {
            serialize_part: ReleaseLocalGrantResp::default(),
            raw_bytes: Vec::new(),
        };
    };

    if grant.owner_node_id.as_ref() != req_node_id.as_ref() {
        let owner_node_id = grant.owner_node_id.to_string();
        view.master_kv_router()
            .install_local_reserve_grant(grant_id, grant);
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
            detail: format!(
                "release_local_grant owner mismatch: grant_id={} owner_node_id={} requester_node_id={}",
                grant_id, owner_node_id, req_node_id
            ),
        });
        return MsgPack {
            serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
            raw_bytes: Vec::new(),
        };
    }

    if !node_generation_is_current_live(&view, &req_node_id, &grant.tomb_tag) {
        // Do not reinsert an old-generation allocation: MemberLeft cleanup may
        // already have taken its snapshot.  Dropping it here is the exact
        // generation-safe release.
        drop(grant);
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
            detail: format!(
                "release_local_grant references a departed owner generation: grant_id={} requester_node_id={}",
                grant_id, req_node_id
            ),
        });
        return MsgPack {
            serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
            raw_bytes: Vec::new(),
        };
    }

    drop(grant);
    MsgPack {
        serialize_part: ReleaseLocalGrantResp::default(),
        raw_bytes: Vec::new(),
    }
}

pub async fn handle_batch_prepare_put_keys(
    view: MasterKvRouterView,
    req: MsgPack<BatchPreparePutKeysReq>,
    req_node_id: NodeID,
) -> (Vec<u64>, MsgPack<BatchPreparePutKeysResp>) {
    let mut reservation_ids = Vec::with_capacity(req.serialize_part.items.len());
    for item in req.serialize_part.items {
        let activity_lease = match view.master_kv_router().reserve_inflight_put_key(
            &item.key,
            item.reject_if_inflight_same_key,
            item.reject_if_exist_same_key,
        ) {
            Ok(activity_lease) => activity_lease,
            Err(err) => {
                for reservation_id in reservation_ids.drain(..) {
                    let _ = view
                        .master_kv_router()
                        .take_prepared_put_key_reservation(reservation_id);
                }
                let resp: BatchPreparePutKeysResp =
                    crate::rpcresp_kvresult_convert::FromError::from_error(&err);
                return (
                    Vec::new(),
                    MsgPack {
                        serialize_part: resp,
                        raw_bytes: Vec::new(),
                    },
                );
            }
        };
        let reservation_id = view
            .master_kv_router()
            .next_prepared_put_key_reservation_id();
        view.master_kv_router()
            .install_prepared_put_key_reservation(
                reservation_id,
                PreparedPutKeyReservationInfo {
                    owner_node_id: req_node_id.clone(),
                    key: item.key,
                    _activity_lease: activity_lease,
                },
            );
        reservation_ids.push(reservation_id);
    }

    (
        reservation_ids.clone(),
        MsgPack {
            serialize_part: BatchPreparePutKeysResp {
                reservation_ids,
                error_code: msg_and_error::OK,
                error_json: String::new(),
                server_process_us: 0,
            },
            raw_bytes: Vec::new(),
        },
    )
}

pub async fn handle_batch_release_put_key_reservations(
    view: MasterKvRouterView,
    req: MsgPack<BatchReleasePutKeyReservationsReq>,
    req_node_id: NodeID,
) -> MsgPack<BatchReleasePutKeyReservationsResp> {
    let mut taken = Vec::with_capacity(req.serialize_part.reservation_ids.len());
    for reservation_id in req.serialize_part.reservation_ids {
        let Some(info) = view
            .master_kv_router()
            .take_prepared_put_key_reservation(reservation_id)
        else {
            tracing::info!(
                "batch_release_put_key_reservations ignored missing reservation_id={} requester_node_id={}",
                reservation_id,
                req_node_id
            );
            continue;
        };
        if info.owner_node_id.as_ref() != req_node_id.as_ref() {
            let owner_node_id = info.owner_node_id.to_string();
            view.master_kv_router()
                .install_prepared_put_key_reservation(reservation_id, info);
            for (restore_id, restore_info) in taken {
                view.master_kv_router()
                    .install_prepared_put_key_reservation(restore_id, restore_info);
            }
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
                detail: format!(
                    "batch_release_put_key_reservations owner mismatch: reservation_id={} owner_node_id={} requester_node_id={}",
                    reservation_id, owner_node_id, req_node_id
                ),
            });
            return MsgPack {
                serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                raw_bytes: Vec::new(),
            };
        }
        taken.push((reservation_id, info));
    }

    drop(taken);

    MsgPack {
        serialize_part: BatchReleasePutKeyReservationsResp::default(),
        raw_bytes: Vec::new(),
    }
}

pub async fn handle_put_revoke(
    view: MasterKvRouterView,
    req: MsgPack<PutRevokeReq>,
) -> MsgPack<PutRevokeResp> {
    tracing::debug!("Handling PutRevokeReq: {:?}", req.serialize_part);

    let (put_time_ms, put_version) = req.serialize_part.put_id;

    let kvrouter_key = (req.serialize_part.key, put_time_ms, put_version);
    // Remove from inflight_puts without storing in completed_puts
    if let Some(inflight_info) = view
        .master_kv_router()
        .inner()
        .inflight_puts
        .remove(&kvrouter_key)
        .await
    {
        let _activity_completion =
            MasterKeyActivityCompletionGuard::new(inflight_info._activity_lease.clone());
        let _replica_activity_completion = inflight_info
            .commit_info
            .replica_target
            .as_ref()
            .map(|target| MasterKeyActivityCompletionGuard::new(target._activity_lease.clone()));
        tracing::info!("Revoked put operation with put_id: {:?}", kvrouter_key);
    } else {
        tracing::warn!(
            "Put operation with put_id {:?} not found for revoke",
            kvrouter_key
        );
    }

    MsgPack {
        serialize_part: PutRevokeResp::default(),
        raw_bytes: Vec::new(),
    }
}

pub async fn handle_put_done(
    view: MasterKvRouterView,
    req: MsgPack<PutDoneReq>,
    req_node_id: NodeID,
) -> MsgPack<PutDoneResp> {
    handle_put_done_with_resolved_group(view, req, req_node_id, None).await
}

async fn handle_put_done_with_resolved_group(
    view: MasterKvRouterView,
    req: MsgPack<PutDoneReq>,
    req_node_id: NodeID,
    resolved_atomic_group: Option<Arc<PutAtomicGroup>>,
) -> MsgPack<PutDoneResp> {
    tracing::debug!("Handling PutDoneReq: {:?}", req.serialize_part);

    let put_id = req.serialize_part.put_id;
    let lease_id_opt = req.serialize_part.lease_id;
    let full_put_id: (String, u64, u32) = (req.serialize_part.key.clone(), put_id.0, put_id.1);
    let mut local_cache_holder_id: Option<u64>;
    let atomic_group = if let Some(group) = resolved_atomic_group {
        Some(group)
    } else {
        match view.master_kv_router().resolve_put_atomic_group(
            &req.serialize_part.key,
            put_id,
            req.serialize_part.atomic_group.clone(),
        ) {
            Ok(group) => group,
            Err(err) => {
                return MsgPack {
                    serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                    raw_bytes: Vec::new(),
                };
            }
        }
    };

    // Remove from inflight_puts and store in completed_puts
    if let Some(InflightPutInfo {
        key,
        commit_info,
        _activity_lease,
        ..
    }) = view
        .master_kv_router()
        .inner()
        .inflight_puts
        .remove(&full_put_id)
        .await
    {
        let _activity_completion = MasterKeyActivityCompletionGuard::new(_activity_lease);
        let mut replica_activity_completion = commit_info
            .replica_target
            .as_ref()
            .map(|target| MasterKeyActivityCompletionGuard::new(target._activity_lease.clone()));
        let node_id = commit_info.node_id;
        let tomb_tag = commit_info.target_tomb_tag.clone();
        let Some(allocs) = commit_info.src_target_allocation.lock().take() else {
            tracing::warn!(
                "Put operation with put_id {:?} not found for completion",
                full_put_id
            );
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
                detail: format!(
                    "Put operation with put_id {} not found for completion",
                    full_put_id.1
                ),
            });
            return MsgPack {
                serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                raw_bytes: Vec::new(),
            };
        };

        if !node_generation_is_current_live(&view, &node_id, &tomb_tag) {
            tracing::info!(
                "Put operation with put_id {:?} belongs to a departed target generation, skip",
                put_id
            );
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
                detail: format!(
                    "Put operation with put_id {:?} belongs to a departed target generation",
                    put_id
                ),
            });
            return MsgPack {
                serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                raw_bytes: Vec::new(),
            };
        }

        let route_committed_slot = req.serialize_part.committed_slot.clone();
        if req.serialize_part.publish_local_cache
            && (!matches!(&allocs, InflightPutAllocation::Local(_))
                || route_committed_slot.is_some())
        {
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
                detail: format!(
                    "publish_local_cache requires owner-local allocation backing; key={} put_id=({},{})",
                    key, put_id.0, put_id.1
                ),
            });
            return MsgPack {
                serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                raw_bytes: Vec::new(),
            };
        }
        let (target_cap_bytes, completed_info, local_cache_publish_supported) = match allocs {
            InflightPutAllocation::Local(target_allocation) => {
                if let Some(slot) = route_committed_slot {
                    let (committed_slot, slot_tomb_tag) = match validate_put_done_committed_slot(
                        &view,
                        &node_id,
                        &slot,
                        Some(&tomb_tag),
                    ) {
                        Ok(validated) => validated,
                        Err(err) => {
                            return MsgPack {
                                serialize_part:
                                    crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                                raw_bytes: Vec::new(),
                            };
                        }
                    };
                    let target_cap_bytes = committed_slot.slot_size;
                    let capacity_reservation = match prepare_route_state(
                        &view,
                        lease_id_opt,
                        &key,
                        put_id,
                        &node_id,
                        &slot_tomb_tag,
                        None,
                        target_cap_bytes,
                    )
                    .await
                    {
                        Ok(reservation) => reservation,
                        Err(err) => {
                            return MsgPack {
                                serialize_part:
                                    crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                                raw_bytes: Vec::new(),
                            };
                        }
                    };
                    drop(target_allocation);
                    (
                        target_cap_bytes,
                        KvRouteInfo {
                            node_id: node_id.clone(),
                            backing: KvReplicaBacking::CommittedSlot(committed_slot),
                            owner_local_indexed: true,
                            get_durable_reservation: None,
                            capacity_reservation,
                            tomb_tag: slot_tomb_tag,
                        },
                        false,
                    )
                } else {
                    let target_cap_bytes = target_allocation.capcity();
                    let reservation_reason = if req.serialize_part.publish_local_cache {
                        Some(ReservedCapacityReason::OwnerIndexedAllocation)
                    } else if lease_id_opt.is_some() {
                        Some(ReservedCapacityReason::LeaseBoundKv)
                    } else {
                        None
                    };
                    let capacity_reservation = match prepare_route_state(
                        &view,
                        lease_id_opt,
                        &key,
                        put_id,
                        &node_id,
                        &tomb_tag,
                        reservation_reason,
                        target_cap_bytes,
                    )
                    .await
                    {
                        Ok(reservation) => reservation,
                        Err(err) => {
                            return MsgPack {
                                serialize_part:
                                    crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                                raw_bytes: Vec::new(),
                            };
                        }
                    };
                    (
                        target_cap_bytes,
                        KvRouteInfo {
                            node_id: node_id.clone(),
                            backing: KvReplicaBacking::Allocation(Arc::new(target_allocation)),
                            owner_local_indexed: req.serialize_part.publish_local_cache,
                            get_durable_reservation: None,
                            capacity_reservation,
                            tomb_tag,
                        },
                        true,
                    )
                }
            }
            InflightPutAllocation::Remote { src: _src, target } => {
                let target_cap_bytes = target.capcity();
                let capacity_reservation = match prepare_route_state(
                    &view,
                    lease_id_opt,
                    &key,
                    put_id,
                    &node_id,
                    &tomb_tag,
                    lease_id_opt.map(|_| ReservedCapacityReason::LeaseBoundKv),
                    target_cap_bytes,
                )
                .await
                {
                    Ok(reservation) => reservation,
                    Err(err) => {
                        return MsgPack {
                            serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(
                                &err,
                            ),
                            raw_bytes: Vec::new(),
                        };
                    }
                };
                (
                    target_cap_bytes,
                    KvRouteInfo {
                        node_id: node_id.clone(),
                        backing: KvReplicaBacking::Allocation(Arc::new(target)),
                        owner_local_indexed: false,
                        get_durable_reservation: None,
                        capacity_reservation,
                        tomb_tag,
                    },
                    false,
                )
            }
            InflightPutAllocation::LocalCommittedSlot(slot) => {
                let target_cap_bytes = slot.slot_size;
                let capacity_reservation = match prepare_route_state(
                    &view,
                    lease_id_opt,
                    &key,
                    put_id,
                    &node_id,
                    &tomb_tag,
                    None,
                    target_cap_bytes,
                )
                .await
                {
                    Ok(reservation) => reservation,
                    Err(err) => {
                        return MsgPack {
                            serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(
                                &err,
                            ),
                            raw_bytes: Vec::new(),
                        };
                    }
                };
                (
                    target_cap_bytes,
                    KvRouteInfo {
                        node_id: node_id.clone(),
                        backing: KvReplicaBacking::CommittedSlot(slot),
                        owner_local_indexed: true,
                        get_durable_reservation: None,
                        capacity_reservation,
                        tomb_tag,
                    },
                    false,
                )
            }
        };

        local_cache_holder_id = if req.serialize_part.publish_local_cache {
            if !local_cache_publish_supported {
                let err = msg_and_error::KvError::Api(
                    msg_and_error::ApiError::InvalidPutMasterState {
                        detail: format!(
                            "publish_local_cache requires owner-local allocation backing; key={} put_id=({},{})",
                            key, put_id.0, put_id.1
                        ),
                    },
                );
                return MsgPack {
                    serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                    raw_bytes: Vec::new(),
                };
            };
            let KvReplicaBacking::Allocation(allocation) = &completed_info.backing else {
                let err = msg_and_error::KvError::Api(
                    msg_and_error::ApiError::InvalidPutMasterState {
                        detail: format!(
                            "publish_local_cache requires allocation backing; key={} put_id=({},{})",
                            key, put_id.0, put_id.1
                        ),
                    },
                );
                return MsgPack {
                    serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                    raw_bytes: Vec::new(),
                };
            };
            let holder_id = view
                .master_kv_router()
                .inner()
                .next_holder_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            view.master_kv_router().inner().get_holding.insert(
                crate::memholder::NodeHolderKey::new(node_id.to_string(), holder_id),
                OwnerHoldingGetInfo {
                    key: key.clone(),
                    holding_node_id: node_id.clone(),
                    len: allocation.size(),
                    allocation: allocation.clone(),
                },
            );
            Some(holder_id)
        } else {
            None
        };

        // Publish the primary route under the node-generation fence.  This
        // closes the gap where MemberLeft marked/snapshotted routes between a
        // pre-check and a later DashMap insert.
        let publish_tag = completed_info.tomb_tag.clone();
        let new_route = Arc::new(OneKvNodesRoutes {
            put_id,
            lease_id: lease_id_opt,
            atomic_group: atomic_group.clone(),
            nodes_replicas: RwLock::new(HashMap::from([(node_id.clone(), completed_info)])),
            get_durable_slots_used: AtomicU32::new(0),
        });
        let old_one_kv_routes = match publish_primary_route_tomb_fenced(
            &view.master_kv_router().inner().kv_routes,
            &key,
            new_route,
            &publish_tag,
        ) {
            Ok(previous) => previous,
            Err(()) => {
                if let Some(lease_id) = lease_id_opt {
                    view.master_lease_manager()
                        .detach_key_if_version(lease_id, &key, put_id);
                }
                if let Some(holder_id) = local_cache_holder_id.take() {
                    view.master_kv_router().inner().get_holding.remove(
                        &crate::memholder::NodeHolderKey::new(node_id.to_string(), holder_id),
                    );
                }
                let err = msg_and_error::KvError::Api(
                    msg_and_error::ApiError::InvalidPutMasterState {
                        detail: format!(
                            "primary route publication rejected because target generation departed: key={} put_id=({},{}) node_id={}",
                            key, put_id.0, put_id.1, node_id
                        ),
                    },
                );
                return MsgPack {
                    serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                    raw_bytes: Vec::new(),
                };
            }
        };

        if let Some(replica_target) = commit_info.replica_target {
            view.master_kv_router()
                .inner()
                .inflight_replica_tasks
                .insert(
                    (
                        replica_target.key.clone(),
                        replica_target.put_id.0,
                        replica_target.put_id.1,
                    ),
                    replica_target,
                )
                .await;
            replica_activity_completion
                .as_mut()
                .expect("replica target activity guard must exist")
                .disarm();
        }

        if let Some(old) = old_one_kv_routes {
            view.master_kv_router()
                .remove_route_cache_entries_exact(&key, &old)
                .await;
            if let Err(err) = view
                .master_kv_router()
                .inner()
                .delete_broadcast
                .sender()
                .send(DeleteKeyInfo::Key {
                    key: key.clone(),
                    nodes_kv_route_info: old,
                })
                .await
            {
                tracing::warn!("Failed to send delete broadcast: {}", err);
            }
        }

        enqueue_post_route_maintenance(
            &view,
            RoutePublishEvent::primary_put(
                key.clone(),
                put_id,
                lease_id_opt,
                node_id.clone(),
                target_cap_bytes,
            ),
        )
        .await;

        // Lease attach is handled before kv_routes insertion

        tracing::debug!(
            "Completed put operation with put_id: {:?}, key: {:?}",
            put_id,
            key
        );
    } else {
        if let Some(slot) = req.serialize_part.committed_slot.clone() {
            let key = req.serialize_part.key.clone();
            let _activity_lease = match view
                .master_kv_router()
                .reserve_inflight_put_key(&key, false, false)
            {
                Ok(activity_lease) => activity_lease,
                Err(err) => {
                    return MsgPack {
                        serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(
                            &err,
                        ),
                        raw_bytes: Vec::new(),
                    };
                }
            };
            let node_id = req_node_id;
            let (committed_slot, tomb_tag) =
                match validate_put_done_committed_slot(&view, &node_id, &slot, None) {
                    Ok(validated) => validated,
                    Err(err) => {
                        return MsgPack {
                            serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(
                                &err,
                            ),
                            raw_bytes: Vec::new(),
                        };
                    }
                };
            if req.serialize_part.publish_local_cache {
                let err = msg_and_error::KvError::Api(
                    msg_and_error::ApiError::InvalidPutMasterState {
                        detail: format!(
                            "local-first put_done does not support publish_local_cache: key={} put_id=({},{})",
                            key, put_id.0, put_id.1
                        ),
                    },
                );
                return MsgPack {
                    serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                    raw_bytes: Vec::new(),
                };
            }
            let target_cap_bytes = committed_slot.slot_size;
            let capacity_reservation = match prepare_route_state(
                &view,
                lease_id_opt,
                &key,
                put_id,
                &node_id,
                &tomb_tag,
                None,
                target_cap_bytes,
            )
            .await
            {
                Ok(reservation) => reservation,
                Err(err) => {
                    return MsgPack {
                        serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(
                            &err,
                        ),
                        raw_bytes: Vec::new(),
                    };
                }
            };
            let completed_info = KvRouteInfo {
                node_id: node_id.clone(),
                backing: KvReplicaBacking::CommittedSlot(committed_slot),
                owner_local_indexed: true,
                get_durable_reservation: None,
                capacity_reservation,
                tomb_tag,
            };
            return publish_completed_put_route(
                view,
                key,
                put_id,
                lease_id_opt,
                atomic_group,
                node_id,
                completed_info,
                target_cap_bytes,
                None,
            )
            .await;
        }
        tracing::warn!(
            "Put operation with put_id {:?} not found for completion",
            put_id
        );
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
            detail: format!("Put operation {:?} not found for completion", put_id),
        });
        return MsgPack {
            serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
            raw_bytes: Vec::new(),
        };
    }

    MsgPack {
        serialize_part: PutDoneResp {
            error_code: msg_and_error::OK,
            error_json: String::new(),
            server_process_us: 0,
            local_cache_holder_id,
        },
        raw_bytes: Vec::new(),
    }
}

pub async fn handle_batch_put_start(
    view: MasterKvRouterView,
    req: MsgPack<BatchPutStartReq>,
    req_node_id: NodeID,
) -> MsgPack<BatchPutStartResp> {
    let mut items = Vec::with_capacity(req.serialize_part.items.len());
    for item in req.serialize_part.items {
        let (_put_id, resp) = handle_put_start(
            view.clone(),
            MsgPack {
                serialize_part: PutStartReq {
                    key: item.key,
                    len: item.len,
                    reject_if_inflight_same_key: item.reject_if_inflight_same_key,
                    reject_if_exist_same_key: item.reject_if_exist_same_key,
                    make_replica_task: item.make_replica_task,
                    preferred_sub_cluster: item.preferred_sub_cluster,
                    source_node_id: None,
                },
                raw_bytes: Vec::new(),
            },
            req_node_id.clone(),
        )
        .await;
        let part = resp.serialize_part;
        items.push(BatchPutStartItemResp {
            put_id: part.put_id,
            node_id: part.node_id,
            target_addr: part.target_addr,
            src_addr: part.src_addr,
            target_base_addr: part.target_base_addr,
            src_base_addr: part.src_base_addr,
            len: part.len,
            error_code: part.error_code,
            error_json: part.error_json,
            replica_target: part.replica_target,
        });
    }
    MsgPack {
        serialize_part: BatchPutStartResp {
            items,
            error_code: msg_and_error::OK,
            error_json: String::new(),
            server_process_us: 0,
        },
        raw_bytes: Vec::new(),
    }
}

pub async fn handle_batch_put_revoke(
    view: MasterKvRouterView,
    req: MsgPack<BatchPutRevokeReq>,
) -> MsgPack<BatchPutRevokeResp> {
    let mut items = Vec::with_capacity(req.serialize_part.items.len());
    for item in req.serialize_part.items {
        let key = item.key.clone();
        let put_id = item.put_id;
        let resp = handle_put_revoke(
            view.clone(),
            MsgPack {
                serialize_part: PutRevokeReq { key, put_id },
                raw_bytes: Vec::new(),
            },
        )
        .await;
        let part = resp.serialize_part;
        items.push(BatchPutRevokeItemResp {
            key: item.key,
            put_id: item.put_id,
            error_code: part.error_code,
            error_json: part.error_json,
        });
    }
    MsgPack {
        serialize_part: BatchPutRevokeResp {
            items,
            error_code: msg_and_error::OK,
            error_json: String::new(),
        },
        raw_bytes: Vec::new(),
    }
}

pub async fn handle_batch_put_done(
    view: MasterKvRouterView,
    req: MsgPack<BatchPutDoneReq>,
    req_node_id: NodeID,
) -> MsgPack<BatchPutDoneResp> {
    let mut items = Vec::with_capacity(req.serialize_part.items.len());
    for item in req.serialize_part.items {
        let key = item.key.clone();
        let put_id = item.put_id;
        let lease_id = item.lease_id;
        let resp = handle_put_done(
            view.clone(),
            MsgPack {
                serialize_part: PutDoneReq {
                    key,
                    put_id,
                    lease_id,
                    committed_slot: item.committed_slot,
                    publish_local_cache: item.publish_local_cache,
                    atomic_group: item.atomic_group,
                },
                raw_bytes: Vec::new(),
            },
            req_node_id.clone(),
        )
        .await;
        let part = resp.serialize_part;
        items.push(BatchPutDoneItemResp {
            key: item.key,
            put_id: item.put_id,
            error_code: part.error_code,
            error_json: part.error_json,
            local_cache_holder_id: part.local_cache_holder_id,
        });
    }
    MsgPack {
        serialize_part: BatchPutDoneResp {
            items,
            error_code: msg_and_error::OK,
            error_json: String::new(),
            server_process_us: 0,
        },
        raw_bytes: Vec::new(),
    }
}

/// V2 route publication for local-first puts. The wire carries each key once
/// plus a compact ordered partition. The master materializes one shared group
/// descriptor per partition and passes cheap `Arc` clones to member routes,
/// avoiding both repeated wire descriptors and repeated group validation.
pub async fn handle_grouped_batch_put_done(
    view: MasterKvRouterView,
    req: MsgPack<GroupedBatchPutDoneReq>,
    req_node_id: NodeID,
) -> MsgPack<GroupedBatchPutDoneResp> {
    let GroupedBatchPutDoneReq {
        items: request_items,
        atomic_group_lens,
    } = req.serialize_part;
    let keys_and_put_ids = request_items
        .iter()
        .map(|item| (item.key.clone(), item.put_id))
        .collect::<Vec<_>>();
    let assignments =
        match build_shared_put_atomic_group_assignments(&keys_and_put_ids, &atomic_group_lens) {
            Ok(assignments) => assignments,
            Err(detail) => {
                let err = msg_and_error::ApiError::InvalidArgument { detail };
                let (error_code, error_json) = err.to_code_and_json();
                return MsgPack {
                    serialize_part: GroupedBatchPutDoneResp {
                        items: Vec::new(),
                        error_code,
                        error_json,
                        server_process_us: 0,
                    },
                    raw_bytes: Vec::new(),
                };
            }
        };

    // The partition builder derives membership from these exact ordered items.
    // Reject duplicate/empty keys once per group so every member is represented
    // exactly once before any route becomes visible.
    let mut offset = 0usize;
    for group_len in atomic_group_lens.iter().copied() {
        if group_len > 1 {
            let mut unique = HashSet::with_capacity(group_len);
            let end = offset + group_len;
            if keys_and_put_ids[offset..end]
                .iter()
                .any(|(key, _)| key.is_empty() || !unique.insert(key.as_str()))
            {
                let err = msg_and_error::ApiError::InvalidArgument {
                    detail: format!(
                        "grouped put member keys must be non-empty and unique: offset={} len={}",
                        offset, group_len
                    ),
                };
                let (error_code, error_json) = err.to_code_and_json();
                return MsgPack {
                    serialize_part: GroupedBatchPutDoneResp {
                        items: Vec::new(),
                        error_code,
                        error_json,
                        server_process_us: 0,
                    },
                    raw_bytes: Vec::new(),
                };
            }
        }
        offset += group_len;
    }

    let mut items = Vec::with_capacity(request_items.len());
    for (item, atomic_group) in request_items.into_iter().zip(assignments) {
        let key = item.key.clone();
        let put_id = item.put_id;
        let resp = handle_put_done_with_resolved_group(
            view.clone(),
            MsgPack {
                serialize_part: PutDoneReq {
                    key,
                    put_id,
                    lease_id: item.lease_id,
                    committed_slot: item.committed_slot,
                    publish_local_cache: item.publish_local_cache,
                    atomic_group: None,
                },
                raw_bytes: Vec::new(),
            },
            req_node_id.clone(),
            atomic_group,
        )
        .await;
        let part = resp.serialize_part;
        items.push(BatchPutDoneItemResp {
            key: item.key,
            put_id: item.put_id,
            error_code: part.error_code,
            error_json: part.error_json,
            local_cache_holder_id: part.local_cache_holder_id,
        });
    }
    MsgPack {
        serialize_part: GroupedBatchPutDoneResp {
            items,
            error_code: msg_and_error::OK,
            error_json: String::new(),
            server_process_us: 0,
        },
        raw_bytes: Vec::new(),
    }
}

async fn handle_put_append_start_inner(
    view: MasterKvRouterView,
    req: MsgPack<PutAppendStartReq>,
    req_node_id: NodeID,
) -> MsgPack<PutAppendStartResp> {
    let key = req.serialize_part.key.clone();
    let put_id = req.serialize_part.put_id;
    let route_snapshot = view
        .master_kv_router()
        .inner()
        .kv_routes
        .get(&key)
        .map(|route| route.clone());
    let current_outcome = route_snapshot
        .as_ref()
        .map(|route| current_route_append_outcome(route, &req_node_id, put_id))
        .unwrap_or(PutAppendStartOutcome::Obsolete);
    if current_outcome != PutAppendStartOutcome::Scheduled {
        if let Some(inflight) = view
            .master_kv_router()
            .inner()
            .inflight_replica_tasks
            .remove(&(key.clone(), put_id.0, put_id.1))
            .await
        {
            inflight._activity_lease.release_now();
        }
        return MsgPack {
            serialize_part: PutAppendStartResp {
                outcome: current_outcome,
                error_code: msg_and_error::OK,
                error_json: String::new(),
                ..Default::default()
            },
            raw_bytes: Vec::new(),
        };
    }

    let append_key = (key.clone(), put_id.0, put_id.1);
    let inflight = if let Some(existing) = view
        .master_kv_router()
        .inner()
        .inflight_replica_tasks
        .get(&append_key)
        .await
    {
        existing
    } else {
        let excluded_nodes = route_snapshot
            .as_ref()
            .map(|route| {
                route
                    .nodes_replicas
                    .read()
                    .values()
                    .filter_map(|replica| {
                        (!replica.tomb_tag.is_tomb()).then_some(replica.node_id.clone())
                    })
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();
        let reservation = match reserve_replica_task_excluding(
            &view,
            &key,
            put_id,
            &req_node_id,
            req.serialize_part.preferred_sub_cluster.as_deref(),
            req.serialize_part.len,
            &excluded_nodes,
            req.serialize_part.protect_source_on_remote_complete,
        ) {
            Ok(reservation) => reservation,
            Err(msg_and_error::KvError::Api(msg_and_error::ApiError::NoSpace {
                node,
                segment,
                total_capacity,
                free_capacity,
            })) => {
                tracing::info!(
                    "replica task not scheduled; local-only commit remains valid: key={} put_id=({},{}) source_node_id={} preferred_sub_cluster={:?} node={} segment={} total_capacity={} free_capacity={}",
                    key,
                    put_id.0,
                    put_id.1,
                    req_node_id,
                    req.serialize_part.preferred_sub_cluster,
                    node,
                    segment,
                    total_capacity,
                    free_capacity
                );
                return MsgPack {
                    serialize_part: PutAppendStartResp {
                        outcome: PutAppendStartOutcome::RetryableNoSpace,
                        error_code: msg_and_error::OK,
                        error_json: String::new(),
                        ..Default::default()
                    },
                    raw_bytes: Vec::new(),
                };
            }
            Err(err) => {
                let resp: PutAppendStartResp =
                    crate::rpcresp_kvresult_convert::FromError::from_error(&err);
                return MsgPack {
                    serialize_part: resp,
                    raw_bytes: Vec::new(),
                };
            }
        };
        view.master_kv_router()
            .record_replica_task_target(reservation.node_id.as_ref());
        view.master_kv_router()
            .inner()
            .inflight_replica_tasks
            .insert(append_key.clone(), reservation.clone())
            .await;
        reservation
    };
    let (target_base_addr, target_addr, allocation_size) = {
        let target_allocation_guard = inflight.target_allocation.lock();
        let Some(target_allocation): Option<&Allocation> = target_allocation_guard.as_ref() else {
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
                detail: format!(
                    "Replica task reservation allocation missing: key={} put_id=({},{}) requester_node_id={}",
                    key, put_id.0, put_id.1, req_node_id
                ),
            });
            let resp: PutAppendStartResp =
                crate::rpcresp_kvresult_convert::FromError::from_error(&err);
            return MsgPack {
                serialize_part: resp,
                raw_bytes: Vec::new(),
            };
        };
        let target_base_addr = target_allocation.base_addr();
        let target_addr = target_base_addr + target_allocation.addr();
        let allocation_size = target_allocation.size();
        (target_base_addr, target_addr, allocation_size)
    };

    MsgPack {
        serialize_part: PutAppendStartResp {
            outcome: PutAppendStartOutcome::Scheduled,
            node_id: inflight.node_id.clone().into(),
            target_addr,
            target_base_addr,
            len: allocation_size,
            error_code: msg_and_error::OK,
            error_json: String::new(),
            server_process_us: 0,
        },
        raw_bytes: Vec::new(),
    }
}

pub async fn handle_put_append_start(
    view: MasterKvRouterView,
    req: MsgPack<PutAppendStartReq>,
    req_node_id: NodeID,
) -> MsgPack<PutAppendStartResp> {
    handle_put_append_start_inner(view, req, req_node_id).await
}

pub async fn handle_batch_put_append_start(
    view: MasterKvRouterView,
    req: MsgPack<BatchPutAppendStartReq>,
    req_node_id: NodeID,
) -> MsgPack<BatchPutAppendStartResp> {
    let mut items = Vec::with_capacity(req.serialize_part.items.len());
    for item in req.serialize_part.items {
        let key = item.key.clone();
        let put_id = item.put_id;
        let resp = handle_put_append_start_inner(
            view.clone(),
            MsgPack {
                serialize_part: PutAppendStartReq {
                    key,
                    put_id,
                    len: item.len,
                    preferred_sub_cluster: item.preferred_sub_cluster,
                    protect_source_on_remote_complete: item.protect_source_on_remote_complete,
                },
                raw_bytes: Vec::new(),
            },
            req_node_id.clone(),
        )
        .await;
        let part = resp.serialize_part;
        items.push(BatchPutAppendStartItemResp {
            key: item.key,
            put_id: item.put_id,
            outcome: part.outcome,
            node_id: part.node_id,
            target_addr: part.target_addr,
            target_base_addr: part.target_base_addr,
            len: part.len,
            error_code: part.error_code,
            error_json: part.error_json,
        });
    }
    MsgPack {
        serialize_part: BatchPutAppendStartResp {
            items,
            error_code: msg_and_error::OK,
            error_json: String::new(),
            server_process_us: 0,
        },
        raw_bytes: Vec::new(),
    }
}

pub async fn handle_put_append_revoke(
    view: MasterKvRouterView,
    req: MsgPack<PutAppendRevokeReq>,
) -> MsgPack<PutAppendRevokeResp> {
    let put_id = req.serialize_part.put_id;
    let key = req.serialize_part.key;
    if let Some(inflight) = view
        .master_kv_router()
        .inner()
        .inflight_replica_tasks
        .remove(&(key, put_id.0, put_id.1))
        .await
    {
        inflight._activity_lease.release_now();
    }
    MsgPack {
        serialize_part: PutAppendRevokeResp {
            error_code: msg_and_error::OK,
            error_json: String::new(),
        },
        raw_bytes: Vec::new(),
    }
}

async fn handle_put_append_done_inner(
    view: MasterKvRouterView,
    req: MsgPack<PutAppendDoneReq>,
) -> MsgPack<PutAppendDoneResp> {
    let put_id = req.serialize_part.put_id;
    let key = req.serialize_part.key.clone();
    let Some(inflight) = view
        .master_kv_router()
        .inner()
        .inflight_replica_tasks
        .remove(&(key.clone(), put_id.0, put_id.1))
        .await
    else {
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
            detail: format!(
                "Put append operation not found for completion: key={} put_id=({},{})",
                key, put_id.0, put_id.1
            ),
        });
        return MsgPack {
            serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
            raw_bytes: Vec::new(),
        };
    };
    let _activity_completion =
        MasterKeyActivityCompletionGuard::new(inflight._activity_lease.clone());
    let Some(allocation) = inflight.target_allocation.lock().take() else {
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
            detail: format!(
                "Replica task append target allocation already taken: key={} put_id=({},{})",
                key, put_id.0, put_id.1
            ),
        });
        return MsgPack {
            serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
            raw_bytes: Vec::new(),
        };
    };
    let published = append_current_route_replica_if_matching(
        &view,
        &key,
        inflight.put_id,
        inflight.node_id,
        inflight.target_tomb_tag,
        allocation,
    );
    let appended = published.is_some();
    if let Some(event) = published {
        enqueue_post_route_maintenance(&view, event).await;
    }
    MsgPack {
        serialize_part: PutAppendDoneResp {
            appended,
            error_code: msg_and_error::OK,
            error_json: String::new(),
            server_process_us: 0,
        },
        raw_bytes: Vec::new(),
    }
}

pub async fn handle_put_append_done(
    view: MasterKvRouterView,
    req: MsgPack<PutAppendDoneReq>,
) -> MsgPack<PutAppendDoneResp> {
    handle_put_append_done_inner(view, req).await
}

pub async fn handle_batch_put_append_done(
    view: MasterKvRouterView,
    req: MsgPack<BatchPutAppendDoneReq>,
) -> MsgPack<BatchPutAppendDoneResp> {
    let mut items = Vec::with_capacity(req.serialize_part.items.len());
    for item in req.serialize_part.items {
        let key = item.key.clone();
        let put_id = item.put_id;
        let resp = handle_put_append_done_inner(
            view.clone(),
            MsgPack {
                serialize_part: PutAppendDoneReq { key, put_id },
                raw_bytes: Vec::new(),
            },
        )
        .await;
        let part = resp.serialize_part;
        items.push(BatchPutAppendDoneItemResp {
            key: item.key,
            put_id: item.put_id,
            appended: part.appended,
            error_code: part.error_code,
            error_json: part.error_json,
        });
    }
    MsgPack {
        serialize_part: BatchPutAppendDoneResp {
            items,
            error_code: msg_and_error::OK,
            error_json: String::new(),
            server_process_us: 0,
        },
        raw_bytes: Vec::new(),
    }
}
