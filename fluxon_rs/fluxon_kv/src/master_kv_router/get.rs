use super::{
    CommittedSlotReplica, CompletedGetInfo, InflightGetInfo, InflightGetTarget, KvRouteInfo,
    MasterKeyActivityCompletionGuard, MasterKvRouterView, OwnerHoldingGetInfo,
    ReservedCapacityReason,
    msg_pack::{
        BatchGetBindItemReq, BatchGetBindReq, BatchGetBindResp, BatchGetDoneItemResp,
        BatchGetDoneReq, BatchGetDoneResp, BatchGetPlanItemResp, BatchGetPlanReq, BatchGetPlanResp,
        BatchGetRevokeItemResp, BatchGetRevokeReq, BatchGetRevokeResp, BatchGetStartItemResp,
        BatchGetStartReq, BatchGetStartResp, BatchIsExistReq, BatchIsExistResp, GetAllocationMode,
        GetBindTarget, GetDoneReq, GetDoneResp, GetExternalSinkTarget, GetMetaReq, GetMetaResp,
        GetPreparedLocalReserveTarget, GetRevokeReq, GetRevokeResp, GetStartReq, GetStartResp,
        MemHolderKeepAliveReq, MemHolderKeepAliveResp, MemHolderReleaseReq, MemHolderReleaseResp,
    },
    node_generation_is_current_live, publish_route_replica_tomb_fenced,
    route_maintenance::{RoutePublishEvent, apply_post_route_maintenance_batch},
};
use crate::master_kv_router::OneKvNodesRoutes;
use crate::master_kv_router::put::PutIDForAKey;
use crate::memholder::MemholderManagerTrait;
use crate::{
    cluster_manager::{ClusterManagerAccessTrait, NodeID, NodeRole},
    master_seg_manager::{MasterSegManagerAccessTrait, NodeTombTag, one_seg_allocator::Allocation},
    p2p::msg_pack::MsgPack,
    rpcresp_kvresult_convert::msg_and_error::{self, kv},
};
use dashmap::DashMap;
use fluxon_commu::share_group_owner_ref_from_metadata;
use limit_thirdparty::tokio;
use rand::Rng;
use rand::seq::SliceRandom;
use std::collections::HashSet;
use std::{
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
    time::Instant,
};

fn touch_moka_for_node(view: MasterKvRouterView, node_id: String, key: String) {
    if !view.master_kv_router().replica_cache_enabled() {
        return;
    }
    let view_task = view.clone();
    view.spawn("touch_moka_for_node", async move {
        let owner_cache_lock = view_task
            .master_kv_router()
            .inner()
            .owner_cache_operation_locks
            .get_lock(node_id.clone());
        let _owner_cache_guard = owner_cache_lock.lock().await;
        if let Some(cache) = view_task
            .master_kv_router()
            .get_node_cache_controller(&node_id)
        {
            // A get is a hit signal for ring B when the source is an
            // unindexed Allocation. Owner-indexed routes are intentionally
            // absent from this cache.
            let _ = cache.get(&key);
            if let Some(tier1_cache) = view_task
                .master_kv_router()
                .get_node_writeback_tier1_controller(&node_id)
            {
                // Tier1 has independent admission and replacement state; a
                // hit only touches an already-admitted entry.
                let _ = tier1_cache.get(&key);
            }
            tracing::debug!(
                "Touched key: {:?} on node cache: {} (TTL refresh)",
                key,
                node_id
            );
        } else {
            tracing::warn!(
                "No cache controller found for node: {} when touching moka",
                node_id
            );
        }
    });
}

fn one_kv_routes_has_live_replica(one_kv_nodes_routes: &OneKvNodesRoutes) -> bool {
    one_kv_nodes_routes
        .nodes_replicas
        .read()
        .values()
        .any(|kv_info| !kv_info.tomb_tag.is_tomb())
}

fn validate_prepared_local_reserve_target(
    view: &MasterKvRouterView,
    req_node_id: &NodeID,
    target: &GetPreparedLocalReserveTarget,
    value_len: u64,
) -> Result<(CommittedSlotReplica, NodeTombTag), msg_and_error::KvError> {
    let invalid = |detail: String| {
        msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument { detail })
    };
    if target.slot_size == 0 {
        return Err(invalid(
            "prepared local-reserve Get target has zero slot_size".to_string(),
        ));
    }
    if value_len > target.slot_size {
        return Err(invalid(format!(
            "prepared local-reserve Get target is too small: value_len={} slot_size={}",
            value_len, target.slot_size
        )));
    }
    let Some(grant) = view
        .master_kv_router()
        .inner()
        .local_reserve_grants
        .get(&target.grant_id)
    else {
        return Err(invalid(format!(
            "prepared local-reserve Get target references unknown grant_id={}",
            target.grant_id
        )));
    };
    if grant.owner_node_id != *req_node_id {
        return Err(invalid(format!(
            "prepared local-reserve Get target owner mismatch: grant_id={} owner={} requester={}",
            target.grant_id, grant.owner_node_id, req_node_id
        )));
    }
    let tomb_tag = grant.tomb_tag.clone();
    if !node_generation_is_current_live(view, req_node_id, &tomb_tag) {
        return Err(invalid(format!(
            "prepared local-reserve Get target belongs to a departed owner generation: grant_id={} requester={}",
            target.grant_id, req_node_id
        )));
    }
    let grant_base_addr = grant.allocation.base_addr();
    let grant_addr = grant_base_addr
        .checked_add(grant.allocation.addr())
        .ok_or_else(|| invalid("local-reserve grant address overflow".to_string()))?;
    let slot_offset = target
        .slot_size
        .checked_mul(u64::from(target.slot_index))
        .ok_or_else(|| invalid("prepared local-reserve Get slot offset overflow".to_string()))?;
    let slot_end = slot_offset
        .checked_add(target.slot_size)
        .ok_or_else(|| invalid("prepared local-reserve Get slot end overflow".to_string()))?;
    if slot_end > grant.allocation.capcity() {
        return Err(invalid(format!(
            "prepared local-reserve Get target is outside grant: grant_id={} slot_index={} slot_size={} grant_len={}",
            target.grant_id,
            target.slot_index,
            target.slot_size,
            grant.allocation.capcity()
        )));
    }
    let expected_addr = grant_addr
        .checked_add(slot_offset)
        .ok_or_else(|| invalid("prepared local-reserve Get target address overflow".to_string()))?;
    if target.base_addr != grant_base_addr || target.addr != expected_addr {
        return Err(invalid(format!(
            "prepared local-reserve Get target geometry mismatch: grant_id={} expected_base={:#x} got_base={:#x} expected_addr={:#x} got_addr={:#x}",
            target.grant_id, grant_base_addr, target.base_addr, expected_addr, target.addr
        )));
    }
    Ok((
        CommittedSlotReplica {
            owner_node_id: req_node_id.clone(),
            grant_id: target.grant_id,
            slot_index: target.slot_index,
            slot_size: target.slot_size,
            addr: target.addr,
            len: value_len,
            base_addr: target.base_addr,
        },
        tomb_tag,
    ))
}

fn external_sink_local_owner_id(
    view: &MasterKvRouterView,
    req_node_id: &NodeID,
    requester_node_start_time: i64,
) -> Option<String> {
    let requester = view
        .cluster_manager()
        .get_member_info_cached(req_node_id.as_ref())?;
    if requester.node_start_time != requester_node_start_time
        || requester.node_role() != NodeRole::External
    {
        return None;
    }
    let owner_ref = share_group_owner_ref_from_metadata(&requester.metadata)?;
    let owner = view
        .cluster_manager()
        .get_member_info_cached(&owner_ref.owner_id)?;
    (owner.node_start_time == owner_ref.owner_start_time && owner.node_role() == NodeRole::Client)
        .then_some(owner_ref.owner_id)
}

fn external_sink_requester_generation_is_current(
    view: &MasterKvRouterView,
    req_node_id: &NodeID,
    requester_node_start_time: i64,
) -> bool {
    external_sink_local_owner_id(view, req_node_id, requester_node_start_time).is_some()
}

fn validate_external_sink_target(
    view: &MasterKvRouterView,
    req_node_id: &NodeID,
    target: &GetExternalSinkTarget,
    value_len: u64,
) -> Result<(), msg_and_error::KvError> {
    let invalid = |detail: String| {
        msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument { detail })
    };
    if target.addr == 0 || target.capacity == 0 || target.registration_id == 0 {
        return Err(invalid(format!(
            "external Get sink requires non-zero addr/capacity/registration_id: addr={:#x} capacity={} registration_id={}",
            target.addr, target.capacity, target.registration_id
        )));
    }
    if target.addr.checked_add(target.capacity).is_none() {
        return Err(invalid(format!(
            "external Get sink range overflows: addr={:#x} capacity={}",
            target.addr, target.capacity
        )));
    }
    if value_len > target.capacity {
        return Err(invalid(format!(
            "external Get sink is too small: value_len={} capacity={}",
            value_len, target.capacity
        )));
    }
    if !external_sink_requester_generation_is_current(
        view,
        req_node_id,
        target.requester_node_start_time,
    ) {
        return Err(invalid(format!(
            "external Get sink requester generation is not current: requester={} start_time={}",
            req_node_id, target.requester_node_start_time
        )));
    }
    Ok(())
}

fn get_plan_item_error(err: &msg_and_error::KvError) -> BatchGetPlanItemResp {
    let response: GetStartResp = crate::rpcresp_kvresult_convert::FromError::from_error(err);
    BatchGetPlanItemResp {
        error_code: response.error_code,
        error_json: response.error_json,
        ..Default::default()
    }
}

fn get_bind_item_error(get_id: u64, err: &msg_and_error::KvError) -> BatchGetStartItemResp {
    let response: GetStartResp = crate::rpcresp_kvresult_convert::FromError::from_error(err);
    BatchGetStartItemResp {
        get_id,
        error_code: response.error_code,
        error_json: response.error_json,
        ..Default::default()
    }
}

#[derive(Clone)]
struct PlannedGetSourceSnapshot {
    node_id: NodeID,
    tomb_tag: crate::master_seg_manager::NodeTombTag,
    len: u64,
    addr: u64,
    base_addr: u64,
}

fn snapshot_live_get_sources(route: &OneKvNodesRoutes) -> Vec<PlannedGetSourceSnapshot> {
    route
        .nodes_replicas
        .read()
        .values()
        .filter(|replica| !replica.tomb_tag.is_tomb())
        .map(|replica| PlannedGetSourceSnapshot {
            node_id: replica.node_id.clone(),
            tomb_tag: replica.tomb_tag.clone(),
            len: replica.backing.len(),
            addr: replica.backing.abs_addr(),
            base_addr: replica.backing.base_addr(),
        })
        .collect()
}

fn planned_get_source_is_current(
    planned: &super::PlannedGetInfo,
    route: &Arc<OneKvNodesRoutes>,
) -> bool {
    if planned.src_tomb_tag.is_tomb() || route.put_id != planned.put_id {
        return false;
    }
    route
        .nodes_replicas
        .read()
        .get(&planned.src_node_id)
        .is_some_and(|replica| {
            !replica.tomb_tag.is_tomb()
                && replica.tomb_tag.same_generation(&planned.src_tomb_tag)
                && replica.backing.abs_addr() == planned.src_addr
                && replica.backing.base_addr() == planned.src_base_addr
                && replica.backing.len() == planned.len
        })
}

#[cfg(test)]
mod planned_get_tests {
    use super::{planned_get_source_is_current, snapshot_live_get_sources};
    use crate::cluster_manager::NodeID;
    use crate::master_kv_router::{
        CommittedSlotReplica, KvReplicaBacking, KvRouteInfo, OneKvNodesRoutes, PlannedGetInfo,
    };
    use crate::master_seg_manager::NodeTombTag;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    fn planned_route() -> (PlannedGetInfo, Arc<OneKvNodesRoutes>, NodeTombTag) {
        let source: NodeID = "source".to_string().into();
        let source_tag = NodeTombTag::new();
        let route = Arc::new(OneKvNodesRoutes {
            put_id: (7, 3),
            lease_id: None,
            atomic_group: None,
            nodes_replicas: RwLock::new(HashMap::from([(
                source.clone(),
                KvRouteInfo {
                    node_id: source.clone(),
                    backing: KvReplicaBacking::CommittedSlot(CommittedSlotReplica {
                        owner_node_id: source.clone(),
                        grant_id: 11,
                        slot_index: 2,
                        slot_size: 8192,
                        addr: 0x3000,
                        len: 4096,
                        base_addr: 0x1000,
                    }),
                    owner_local_indexed: true,
                    get_durable_reservation: None,
                    capacity_reservation: None,
                    tomb_tag: source_tag.clone(),
                },
            )])),
            get_durable_slots_used: AtomicU32::new(0),
        });
        let planned = PlannedGetInfo {
            put_id: route.put_id,
            src_node_id: source,
            src_tomb_tag: source_tag.clone(),
            key: "key".to_string(),
            controller_node_id: "external".to_string().into(),
            controller_node_start_time: 17,
            len: 4096,
            src_addr: 0x3000,
            src_base_addr: 0x1000,
            atomic_group: None,
        };
        (planned, route, source_tag)
    }

    #[test]
    fn bind_revalidation_accepts_only_the_exact_source_generation() {
        let (planned, route, source_tag) = planned_route();
        assert!(planned_get_source_is_current(&planned, &route));

        route
            .nodes_replicas
            .write()
            .get_mut(&planned.src_node_id)
            .unwrap()
            .backing = KvReplicaBacking::CommittedSlot(CommittedSlotReplica {
            owner_node_id: planned.src_node_id.clone(),
            grant_id: 11,
            slot_index: 2,
            slot_size: 8192,
            addr: 0x4000,
            len: 4096,
            base_addr: 0x1000,
        });
        assert!(!planned_get_source_is_current(&planned, &route));

        route
            .nodes_replicas
            .write()
            .get_mut(&planned.src_node_id)
            .unwrap()
            .backing = KvReplicaBacking::CommittedSlot(CommittedSlotReplica {
            owner_node_id: planned.src_node_id.clone(),
            grant_id: 11,
            slot_index: 2,
            slot_size: 8192,
            addr: 0x3000,
            len: 4096,
            base_addr: 0x1000,
        });
        source_tag.set_tomb();
        assert!(!planned_get_source_is_current(&planned, &route));
    }

    #[test]
    fn bind_revalidation_rejects_a_replacement_route_generation() {
        let (planned, route, _) = planned_route();
        let replacement_tag = NodeTombTag::new();
        route
            .nodes_replicas
            .write()
            .get_mut(&planned.src_node_id)
            .unwrap()
            .tomb_tag = replacement_tag;
        assert!(!planned_get_source_is_current(&planned, &route));
    }

    #[test]
    fn metadata_plan_does_not_retain_the_route() {
        let (planned, route, _) = planned_route();
        let weak_route = Arc::downgrade(&route);
        let sources = snapshot_live_get_sources(&route);
        drop(route);
        assert!(weak_route.upgrade().is_none());
        assert_eq!(sources.len(), 1);
        assert_eq!(planned.key, "key");
    }
}

async fn handle_get_plan_item(
    view: MasterKvRouterView,
    key: String,
    controller_node_id: NodeID,
) -> BatchGetPlanItemResp {
    view.master_kv_router()
        .inner()
        .planned_get_counters
        .plan_items
        .fetch_add(1, Ordering::Relaxed);
    let Some(controller) = view
        .cluster_manager()
        .get_member_info_cached(controller_node_id.as_ref())
    else {
        return get_plan_item_error(&msg_and_error::KvError::Api(
            msg_and_error::ApiError::InvalidArgument {
                detail: format!(
                    "GetPlan controller is not a current member: {}",
                    controller_node_id
                ),
            },
        ));
    };
    if controller.node_role() != NodeRole::External {
        return get_plan_item_error(&msg_and_error::KvError::Api(
            msg_and_error::ApiError::InvalidArgument {
                detail: format!(
                    "GetPlan is supported only for external controllers: {}",
                    controller_node_id
                ),
            },
        ));
    }

    let Some(route) = view
        .master_kv_router()
        .inner()
        .kv_routes
        .get(&key)
        .map(|route| route.clone())
    else {
        view.master_kv_router()
            .inner()
            .planned_get_counters
            .plan_misses
            .fetch_add(1, Ordering::Relaxed);
        return get_plan_item_error(&msg_and_error::KvError::Api(
            msg_and_error::ApiError::KeyNotFound { key },
        ));
    };

    let local_owner =
        external_sink_local_owner_id(&view, &controller_node_id, controller.node_start_time);
    let mut candidates = snapshot_live_get_sources(&route);
    candidates.shuffle(&mut rand::thread_rng());
    candidates.sort_by_key(|replica| {
        local_owner
            .as_deref()
            .is_some_and(|owner| replica.node_id.as_ref() == owner)
    });
    let Some(source) = candidates.into_iter().next() else {
        view.master_kv_router()
            .inner()
            .planned_get_counters
            .plan_misses
            .fetch_add(1, Ordering::Relaxed);
        return get_plan_item_error(&msg_and_error::KvError::Api(
            msg_and_error::ApiError::KeyNotFound { key },
        ));
    };

    let get_id = view
        .master_kv_router()
        .inner()
        .next_get_id
        .fetch_add(1, Ordering::Relaxed);
    let gpu_direct_eligible = local_owner
        .as_deref()
        .is_none_or(|owner| source.node_id.as_ref() != owner);
    let planned = super::PlannedGetInfo {
        put_id: route.put_id,
        src_node_id: source.node_id.clone(),
        src_tomb_tag: source.tomb_tag.clone(),
        key: key.clone(),
        controller_node_id: controller_node_id.clone(),
        controller_node_start_time: controller.node_start_time,
        len: source.len,
        src_addr: source.addr,
        src_base_addr: source.base_addr,
        atomic_group: route.atomic_group.as_deref().cloned(),
    };
    drop(route);
    view.master_kv_router()
        .inner()
        .planned_gets
        .insert(get_id, planned.clone())
        .await;
    view.master_kv_router()
        .inner()
        .planned_get_counters
        .plan_hits
        .fetch_add(1, Ordering::Relaxed);
    BatchGetPlanItemResp {
        get_id,
        node_id: source.node_id.into(),
        put_id: planned.put_id,
        src_addr: planned.src_addr,
        src_base_addr: planned.src_base_addr,
        len: planned.len,
        atomic_group: planned.atomic_group,
        gpu_direct_eligible,
        error_code: msg_and_error::OK,
        error_json: String::new(),
    }
}

fn bound_get_matches_target(info: &InflightGetInfo, target: &GetBindTarget) -> bool {
    match (target, &info.target) {
        (GetBindTarget::ExternalSink(expected), InflightGetTarget::ExternalSink(actual)) => {
            expected == actual
        }
        (
            GetBindTarget::PreparedLocalReserve(expected),
            InflightGetTarget::PreparedLocalReserveSlot(actual),
        ) => {
            expected.grant_id == actual.grant_id
                && expected.slot_index == actual.slot_index
                && expected.slot_size == actual.slot_size
                && expected.addr == actual.addr
                && expected.base_addr == actual.base_addr
        }
        _ => false,
    }
}

fn bound_get_start_item(get_id: u64, info: &InflightGetInfo) -> BatchGetStartItemResp {
    BatchGetStartItemResp {
        get_id,
        node_id: info.src_node_id.to_string().into(),
        put_id: info.put_id,
        target_addr: info.target.abs_addr(),
        src_addr: info.src_addr,
        target_base_addr: info.target.base_addr(),
        src_base_addr: info.src_base_addr,
        len: info.len,
        prepared_target: match &info.target {
            InflightGetTarget::PreparedLocalReserveSlot(slot) => {
                Some(GetPreparedLocalReserveTarget {
                    grant_id: slot.grant_id,
                    slot_index: slot.slot_index,
                    slot_size: slot.slot_size,
                    addr: slot.addr,
                    base_addr: slot.base_addr,
                })
            }
            _ => None,
        },
        atomic_group: info.atomic_group.clone(),
        error_code: msg_and_error::OK,
        error_json: String::new(),
    }
}

async fn handle_get_bind_item(
    view: MasterKvRouterView,
    request: BatchGetBindItemReq,
    req_node_id: NodeID,
) -> BatchGetStartItemResp {
    let get_id = request.get_id;
    let operation_lock = view
        .master_kv_router()
        .inner()
        .get_done_locks
        .get_lock(get_id);
    let _operation_guard = operation_lock.lock().await;

    if let Some(bound) = view
        .master_kv_router()
        .inner()
        .inflight_gets
        .get(&get_id)
        .await
    {
        if bound.req_node_id != req_node_id || !bound_get_matches_target(&bound, &request.target) {
            return get_bind_item_error(
                get_id,
                &msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
                    detail: format!(
                        "GetBind replay identity/target mismatch: get_id={} requester={}",
                        get_id, req_node_id
                    ),
                }),
            );
        }
        return bound_get_start_item(get_id, &bound);
    }

    let Some(planned) = view
        .master_kv_router()
        .inner()
        .planned_gets
        .get(&get_id)
        .await
    else {
        return get_bind_item_error(
            get_id,
            &msg_and_error::KvError::Api(msg_and_error::ApiError::KeyNotFound {
                key: format!("planned_get_id:{get_id}"),
            }),
        );
    };
    let (target, target_tomb_tag, allocation_mode, prepared_requester_lease) = match &request.target
    {
        GetBindTarget::ExternalSink(target) => {
            if req_node_id != planned.controller_node_id {
                return get_bind_item_error(
                    get_id,
                    &msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
                        detail: format!(
                            "external GetBind controller mismatch: get_id={} expected={} got={}",
                            get_id, planned.controller_node_id, req_node_id
                        ),
                    }),
                );
            }
            if let Err(err) =
                validate_external_sink_target(&view, &req_node_id, target, planned.len)
            {
                return get_bind_item_error(get_id, &err);
            }
            (
                InflightGetTarget::ExternalSink(target.clone()),
                None,
                GetAllocationMode::ExternalSink,
                None,
            )
        }
        GetBindTarget::PreparedLocalReserve(target) => {
            let expected_owner = external_sink_local_owner_id(
                &view,
                &planned.controller_node_id,
                planned.controller_node_start_time,
            );
            if expected_owner.as_deref() != Some(req_node_id.as_ref()) {
                return get_bind_item_error(
                    get_id,
                    &msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
                        detail: format!(
                            "prepared GetBind executor is not the controller's owner: get_id={} controller={} expected_owner={:?} got={}",
                            get_id, planned.controller_node_id, expected_owner, req_node_id
                        ),
                    }),
                );
            }
            let requester_lease = match view.master_kv_router().reserve_prepared_get_requester(
                &planned.key,
                &req_node_id,
                get_id,
            ) {
                Ok(lease) => lease,
                Err(err) => return get_bind_item_error(get_id, &err),
            };
            let (slot, tomb_tag) = match validate_prepared_local_reserve_target(
                &view,
                &req_node_id,
                target,
                planned.len,
            ) {
                Ok(value) => value,
                Err(err) => return get_bind_item_error(get_id, &err),
            };
            (
                InflightGetTarget::PreparedLocalReserveSlot(slot),
                Some(tomb_tag),
                GetAllocationMode::LocalCommittedSlot,
                Some(requester_lease),
            )
        }
        GetBindTarget::Invalid => {
            return get_bind_item_error(
                get_id,
                &msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
                    detail: format!("GetBind requires a concrete target: get_id={get_id}"),
                }),
            );
        }
    };

    let activity_lease = match view
        .master_kv_router()
        .reserve_inflight_get_key(&planned.key)
    {
        Ok(lease) => lease,
        Err(err) => {
            view.master_kv_router()
                .inner()
                .planned_get_counters
                .bind_activity_busy
                .fetch_add(1, Ordering::Relaxed);
            return get_bind_item_error(get_id, &err);
        }
    };
    let current_route = view
        .master_kv_router()
        .inner()
        .kv_routes
        .get(&planned.key)
        .map(|route| route.clone());
    let Some(current_route) =
        current_route.filter(|route| planned_get_source_is_current(&planned, route))
    else {
        view.master_kv_router()
            .inner()
            .planned_get_counters
            .bind_stale
            .fetch_add(1, Ordering::Relaxed);
        return get_bind_item_error(
            get_id,
            &msg_and_error::KvError::Api(msg_and_error::ApiError::StaleGetPlan {
                get_id,
                key: planned.key.clone(),
                detail: format!(
                    "source route changed before Bind: source={}",
                    planned.src_node_id
                ),
            }),
        );
    };
    if matches!(&request.target, GetBindTarget::PreparedLocalReserve(_))
        && current_route
            .nodes_replicas
            .read()
            .get(&req_node_id)
            .is_some_and(|replica| !replica.tomb_tag.is_tomb())
    {
        return get_bind_item_error(
            get_id,
            &msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
                detail: format!(
                    "prepared GetBind cannot replace a live owner replica: get_id={} key={} owner={}",
                    get_id, planned.key, req_node_id
                ),
            }),
        );
    }

    let Some(planned) = view
        .master_kv_router()
        .inner()
        .planned_gets
        .remove(&get_id)
        .await
    else {
        return get_bind_item_error(
            get_id,
            &msg_and_error::KvError::Api(msg_and_error::ApiError::KeyNotFound {
                key: format!("planned_get_id:{get_id}"),
            }),
        );
    };
    let inflight = InflightGetInfo {
        put_id: planned.put_id,
        src_node_id: planned.src_node_id.clone(),
        key: planned.key.clone(),
        req_node_id: req_node_id.clone(),
        controller_node_id: Some(planned.controller_node_id),
        len: planned.len,
        src_addr: planned.src_addr,
        src_base_addr: planned.src_base_addr,
        atomic_group: planned.atomic_group,
        target,
        target_tomb_tag,
        route: current_route.clone(),
        allocation_mode,
        durable_reservation: None,
        _activity_lease: activity_lease,
        _prepared_requester_lease: prepared_requester_lease,
    };
    let response = bound_get_start_item(get_id, &inflight);
    view.master_kv_router().record_get_source_selection(
        req_node_id.as_ref(),
        inflight.src_node_id.as_ref(),
        inflight.len,
        allocation_mode,
    );
    view.master_kv_router()
        .inner()
        .inflight_gets
        .insert(get_id, inflight)
        .await;
    view.master_kv_router()
        .inner()
        .planned_get_counters
        .bind_succeeded
        .fetch_add(1, Ordering::Relaxed);
    if current_route.lease_id.is_none() {
        touch_moka_for_node(view, response.node_id.to_string(), planned.key);
    }
    response
}

pub async fn handle_batch_get_plan(
    view: MasterKvRouterView,
    req: MsgPack<BatchGetPlanReq>,
    req_node_id: NodeID,
) -> MsgPack<BatchGetPlanResp> {
    let mut items = Vec::with_capacity(req.serialize_part.keys.len());
    for key in req.serialize_part.keys {
        items.push(handle_get_plan_item(view.clone(), key, req_node_id.clone()).await);
    }
    MsgPack {
        serialize_part: BatchGetPlanResp {
            items,
            error_code: msg_and_error::OK,
            error_json: String::new(),
            server_process_us: 0,
        },
        raw_bytes: Vec::new(),
    }
}

pub async fn handle_batch_get_bind(
    view: MasterKvRouterView,
    req: MsgPack<BatchGetBindReq>,
    req_node_id: NodeID,
) -> MsgPack<BatchGetBindResp> {
    let mut items = Vec::with_capacity(req.serialize_part.items.len());
    for item in req.serialize_part.items {
        items.push(handle_get_bind_item(view.clone(), item, req_node_id.clone()).await);
    }
    MsgPack {
        serialize_part: BatchGetBindResp {
            items,
            error_code: msg_and_error::OK,
            error_json: String::new(),
            server_process_us: 0,
        },
        raw_bytes: Vec::new(),
    }
}

pub async fn handle_get_start(
    view: MasterKvRouterView,
    req: MsgPack<GetStartReq>,
    req_node_id: NodeID,
) -> (u64, MsgPack<GetStartResp>) {
    fn clean_up_tombs(
        view: &MasterKvRouterView,
        tombs_and_put_id: Option<(HashSet<NodeID>, PutIDForAKey)>,
        key: &str,
    ) {
        if let Some((tombs, put_id)) = tombs_and_put_id {
            let mut remove_in_kv_routes = false;
            if let Some(one_kv_nodes_routes) = view.master_kv_router().inner().kv_routes.get(key) {
                one_kv_nodes_routes.clean_up_tomb_nodes_replicas(put_id, tombs, view);
                if one_kv_nodes_routes.nodes_replicas.read().is_empty() {
                    remove_in_kv_routes = true;
                }
            }

            if remove_in_kv_routes {
                view.master_kv_router()
                    .inner()
                    .kv_routes
                    .remove_if(key, |_, one_kv_nodes_routes| {
                        one_kv_nodes_routes.put_id == put_id
                    });
            }
        }
    }
    fn failed_resp_err(
        err: msg_and_error::KvError,
        tombs_and_put_id: Option<(HashSet<NodeID>, PutIDForAKey)>,
        view: &MasterKvRouterView,
        key: &str,
    ) -> (u64, MsgPack<GetStartResp>) {
        // clean up the tombs
        clean_up_tombs(view, tombs_and_put_id, key);
        (
            0,
            MsgPack {
                serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                raw_bytes: Vec::new(),
            },
        )
    }

    tracing::debug!("Handling GetStartReq: {:?}", req.serialize_part);

    if req.serialize_part.prepared_target.is_some()
        && req.serialize_part.external_sink_target.is_some()
    {
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
            detail: "GetStart prepared_target and external_sink_target are mutually exclusive"
                .to_string(),
        });
        return failed_resp_err(err, None, &view, &req.serialize_part.key);
    }

    let activity_lease = match view
        .master_kv_router()
        .reserve_inflight_get_key(&req.serialize_part.key)
    {
        Ok(activity_lease) => activity_lease,
        Err(err) => return failed_resp_err(err, None, &view, &req.serialize_part.key),
    };

    let get_id = view
        .master_kv_router()
        .inner()
        .next_get_id
        .fetch_add(1, Ordering::Relaxed);
    let prepared_requester_lease = if req.serialize_part.prepared_target.is_some() {
        match view.master_kv_router().reserve_prepared_get_requester(
            &req.serialize_part.key,
            &req_node_id,
            get_id,
        ) {
            Ok(lease) => Some(lease),
            Err(err) => return failed_resp_err(err, None, &view, &req.serialize_part.key),
        }
    } else {
        None
    };

    let one_kv_nodes_routes: Arc<OneKvNodesRoutes> = if let Some(one_kv_nodes_routes) = view
        .master_kv_router()
        .inner()
        .kv_routes
        .get(&req.serialize_part.key)
    {
        one_kv_nodes_routes.clone()
    } else {
        // Key not found
        tracing::debug!("Key not found: {}", req.serialize_part.key);
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::KeyNotFound {
            key: req.serialize_part.key.clone(),
        });
        return failed_resp_err(err, None, &view, &req.serialize_part.key);
    };

    let replicas: HashMap<NodeID, KvRouteInfo> = one_kv_nodes_routes.nodes_replicas.read().clone();
    let prepared_target = req.serialize_part.prepared_target.clone();
    let external_sink_target = req.serialize_part.external_sink_target.clone();
    let external_sink_local_owner = external_sink_target.as_ref().and_then(|target| {
        external_sink_local_owner_id(&view, &req_node_id, target.requester_node_start_time)
    });
    // Currently we are holding the lock with `replicas`
    // 选择一个replica (这里可以实现更复杂的选择逻辑)
    let mut replica_keys = replicas.keys().collect::<Vec<_>>();
    let mut tombs = HashSet::new();
    let mut target = None;
    let mut allocation_mode = GetAllocationMode::Temporary;
    let mut durable_reservation = None;
    for _ in 0..replicas.len() {
        let to_remove_idx = rand::thread_rng().gen_range(0..replica_keys.len());
        let selected_replica_key = replica_keys.remove(to_remove_idx);
        let selected_replica = replicas.get(&*selected_replica_key).unwrap();
        if selected_replica.tomb_tag.is_tomb() {
            tombs.insert(selected_replica_key.to_owned());
            continue;
        }
        if external_sink_local_owner
            .as_deref()
            .is_some_and(|owner_id| selected_replica.node_id.as_ref() == owner_id)
        {
            // The explicit GPU path is RDMA-only. The requester's share-group
            // owner is local IPC/P2P topology, whose fallback would require
            // CPU access to a CUDA virtual address. Leave that replica for the
            // ordinary CPU-buffered Get path and search for a remote route.
            continue;
        }
        let src_node_id = selected_replica.node_id.clone();
        let src_len = selected_replica.backing.len();
        let src_abs_addr = selected_replica.backing.abs_addr();
        let src_base = selected_replica.backing.base_addr();

        // For committed-slot replicas on the requester node, we still allocate a
        // normal target buffer here instead of reusing the committed slot as a
        // MemHolder backing. That keeps the existing get-path carrier stable
        // and avoids the old None->unwrap panic.
        let mut allocate_request_target =
            || -> Result<InflightGetTarget, (u64, MsgPack<GetStartResp>)> {
                let target_allocation = {
                    let req_node_allocators =
                        view.master_seg_manager().get_node_allocators(&req_node_id);
                    if req_node_allocators.is_empty() {
                        tracing::info!(
                            "No allocators found for requesting node: {}, node is not ready",
                            req_node_id
                        );
                        let err = msg_and_error::KvError::Unreachable(
                            msg_and_error::UnreachableError::OwnerNoSeg { detail: "config=0 initializes as external; non-zero initializes as owner; the owner must have memory space (segment)".to_string() }
                        );
                        return Err(failed_resp_err(
                            err,
                            Some((tombs.clone(), one_kv_nodes_routes.put_id)),
                            &view,
                            &req.serialize_part.key,
                        ));
                    }

                    let target_allocator =
                        req_node_allocators.choose(&mut rand::thread_rng()).unwrap();

                    let mut allocated_addr: Option<Allocation> = None;
                    for attempt in 1..=3 {
                        if let Ok(allocation) = target_allocator.allocate(src_len) {
                            allocated_addr = Some(allocation);
                            break;
                        } else {
                            tracing::info!(
                                "Requesting node as target allocation attempt {}/3 failed for get_id {}",
                                attempt,
                                get_id
                            );
                        }
                    }
                    if allocated_addr.is_none() {
                        tracing::info!("No space left for target(Requesting node) allocation");
                        let total = target_allocator.total_size_bytes();
                        let used = target_allocator.used_size_bytes();
                        let free = total.saturating_sub(used);
                        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::NoSpace {
                            node: req_node_id.as_ref().to_string(),
                            segment: target_allocator.seg_device_id.clone(),
                            total_capacity: total,
                            free_capacity: free,
                        });
                        return Err(failed_resp_err(
                            err,
                            Some((tombs.clone(), one_kv_nodes_routes.put_id)),
                            &view,
                            &req.serialize_part.key,
                        ));
                    }
                    allocated_addr.unwrap()
                };
                if let Some(reservation) = one_kv_nodes_routes.try_reserve_get_durable_slot() {
                    allocation_mode = GetAllocationMode::DurableReplica;
                    durable_reservation = Some(reservation);
                } else {
                    allocation_mode = GetAllocationMode::Temporary;
                }
                Ok(InflightGetTarget::Allocation(Arc::new(target_allocation)))
            };

        // 为get调用方分配接收内存作为传输target
        if target.is_none() {
            target = Some(
                if let Some(external_sink_target) = external_sink_target.as_ref() {
                    if let Err(err) = validate_external_sink_target(
                        &view,
                        &req_node_id,
                        external_sink_target,
                        src_len,
                    ) {
                        return failed_resp_err(
                            err,
                            Some((tombs.clone(), one_kv_nodes_routes.put_id)),
                            &view,
                            &req.serialize_part.key,
                        );
                    }
                    allocation_mode = GetAllocationMode::ExternalSink;
                    InflightGetTarget::ExternalSink(external_sink_target.clone())
                } else if let Some(prepared_target) = prepared_target.as_ref() {
                    if replicas
                        .get(&req_node_id)
                        .is_some_and(|replica| !replica.tomb_tag.is_tomb())
                    {
                        let err = msg_and_error::KvError::Api(
                            msg_and_error::ApiError::InvalidArgument {
                                detail: format!(
                                    "prepared local-reserve Get target cannot replace a live replica: key={} requester={}",
                                    req.serialize_part.key, req_node_id
                                ),
                            },
                        );
                        return failed_resp_err(
                            err,
                            Some((tombs.clone(), one_kv_nodes_routes.put_id)),
                            &view,
                            &req.serialize_part.key,
                        );
                    }
                    let (slot, _prepared_tomb_tag) = match validate_prepared_local_reserve_target(
                        &view,
                        &req_node_id,
                        prepared_target,
                        src_len,
                    ) {
                        Ok(slot) => slot,
                        Err(err) => {
                            return failed_resp_err(
                                err,
                                Some((tombs.clone(), one_kv_nodes_routes.put_id)),
                                &view,
                                &req.serialize_part.key,
                            );
                        }
                    };
                    allocation_mode = GetAllocationMode::LocalCommittedSlot;
                    InflightGetTarget::PreparedLocalReserveSlot(slot)
                } else if let Some(replica_on_recv_node) = replicas.get(&req_node_id) {
                    match &replica_on_recv_node.backing {
                        super::KvReplicaBacking::Allocation(allocation) => {
                            allocation_mode = GetAllocationMode::ReuseReplica;
                            InflightGetTarget::Allocation(allocation.clone())
                        }
                        super::KvReplicaBacking::CommittedSlot(_) => {
                            match allocate_request_target() {
                                Ok(allocation) => allocation,
                                Err(resp) => return resp,
                            }
                        }
                    }
                } else {
                    match allocate_request_target() {
                        Ok(allocation) => allocation,
                        Err(resp) => return resp,
                    }
                },
            );
        }

        let target = target
            .as_ref()
            .expect("Get target must be selected before building response")
            .clone();

        // Bind the target to the exact registration generation that owns its
        // allocator/grant.  Looking up only by node id at GetDone would allow
        // an old completion to publish addresses into a reconnected node.
        let target_tomb_tag = match &target {
            InflightGetTarget::Allocation(allocation) => view
                .master_seg_manager()
                .get_allocation_tomb_tag(&req_node_id, allocation),
            InflightGetTarget::PreparedLocalReserveSlot(slot) => view
                .master_kv_router()
                .inner()
                .local_reserve_grants
                .get(&slot.grant_id)
                .and_then(|grant| {
                    (grant.owner_node_id == req_node_id
                        && node_generation_is_current_live(&view, &req_node_id, &grant.tomb_tag))
                    .then(|| grant.tomb_tag.clone())
                }),
            InflightGetTarget::ExternalSink(_) => None,
        };
        if !matches!(&target, InflightGetTarget::ExternalSink(_)) && target_tomb_tag.is_none() {
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
                detail: format!(
                    "Get target generation changed before start publication: get_id={} key={} requester={}",
                    get_id, req.serialize_part.key, req_node_id
                ),
            });
            return failed_resp_err(
                err,
                Some((tombs.clone(), one_kv_nodes_routes.put_id)),
                &view,
                &req.serialize_part.key,
            );
        }

        // Convert to absolute addresses for Mooncake (requires absolute)
        // Use allocation's allocator base directly
        let target_base = target.base_addr();

        // If we reuse existing target on requesting node, declare src=target on req node
        let (resp_node_id, resp_src_addr, resp_target_addr, resp_src_base, resp_target_base) =
            if allocation_mode == GetAllocationMode::ReuseReplica {
                let addr = target.abs_addr();
                // both src/target are on requesting node's allocation in this reuse case
                (req_node_id.clone(), addr, addr, target_base, target_base)
            } else {
                (
                    src_node_id.clone(),
                    src_abs_addr,
                    target.abs_addr(),
                    src_base,
                    target_base,
                )
            };

        let resp = GetStartResp {
            put_id: one_kv_nodes_routes.put_id,
            get_id,
            node_id: resp_node_id.clone().into(),
            src_addr: resp_src_addr,
            target_addr: resp_target_addr,
            src_base_addr: resp_src_base,
            target_base_addr: resp_target_base,
            len: src_len,
            prepared_target: (allocation_mode == GetAllocationMode::LocalCommittedSlot)
                .then(|| prepared_target.clone())
                .flatten(),
            atomic_group: one_kv_nodes_routes.atomic_group.as_deref().cloned(),
            error_code: msg_and_error::OK,
            error_json: String::new(),
            server_process_us: 0,
        };
        view.master_kv_router().record_get_source_selection(
            req_node_id.as_ref(),
            resp_node_id.as_ref(),
            src_len,
            allocation_mode,
        );
        // 创建在途的Get操作信息
        let info = InflightGetInfo {
            put_id: one_kv_nodes_routes.put_id,
            src_node_id: src_node_id.clone(),
            key: req.serialize_part.key.clone(),
            req_node_id,
            controller_node_id: None,
            len: src_len,
            src_addr: resp_src_addr,
            src_base_addr: resp_src_base,
            atomic_group: one_kv_nodes_routes.atomic_group.as_deref().cloned(),
            target,
            target_tomb_tag,
            route: one_kv_nodes_routes.clone(),
            allocation_mode,
            durable_reservation,
            _activity_lease: activity_lease,
            _prepared_requester_lease: prepared_requester_lease,
        };

        view.master_kv_router()
            .inner()
            .inflight_gets
            .insert(get_id, info)
            .await;

        // After selecting source and allocating target, optionally touch the
        // source node's moka to keep the kv alive during transfer (weight=0 => touch).
        // For leased keys, there should be no moka entry; skip touching to avoid
        // unnecessary cache work.
        if one_kv_nodes_routes.lease_id.is_none() {
            touch_moka_for_node(
                view.clone(),
                src_node_id.to_string(),
                req.serialize_part.key.clone(),
            );
        }

        clean_up_tombs(
            &view,
            Some((tombs, one_kv_nodes_routes.put_id)),
            &req.serialize_part.key,
        );
        return (
            get_id,
            MsgPack {
                serialize_part: resp,
                raw_bytes: Vec::new(),
            },
        );
    }
    tracing::debug!("Key not found: {}", req.serialize_part.key);
    {
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::KeyNotFound {
            key: req.serialize_part.key.clone(),
        });
        failed_resp_err(
            err,
            Some((tombs, one_kv_nodes_routes.put_id)),
            &view,
            &req.serialize_part.key,
        )
    }
}

pub async fn handle_get_revoke(
    view: MasterKvRouterView,
    req: MsgPack<GetRevokeReq>,
    req_node_id: NodeID,
) -> MsgPack<GetRevokeResp> {
    tracing::debug!("Handling GetRevokeReq: {:?}", req.serialize_part);

    let get_id = req.serialize_part.get_id;
    let done_lock = view
        .master_kv_router()
        .inner()
        .get_done_locks
        .get_lock(get_id);
    let _done_guard = done_lock.lock().await;

    if let Some(planned) = view
        .master_kv_router()
        .inner()
        .planned_gets
        .get(&get_id)
        .await
    {
        let controller_owner = external_sink_local_owner_id(
            &view,
            &planned.controller_node_id,
            planned.controller_node_start_time,
        );
        let authorized = planned.controller_node_id == req_node_id
            || controller_owner.as_deref() == Some(req_node_id.as_ref());
        if !authorized {
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
                detail: format!(
                    "GetRevoke planned-operation requester mismatch: get_id={} controller={} got={}",
                    get_id, planned.controller_node_id, req_node_id
                ),
            });
            return MsgPack {
                serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                raw_bytes: Vec::new(),
            };
        }
        drop(planned);
        if view
            .master_kv_router()
            .inner()
            .planned_gets
            .remove(&get_id)
            .await
            .is_some()
        {
            view.master_kv_router()
                .inner()
                .planned_get_counters
                .plan_revoked
                .fetch_add(1, Ordering::Relaxed);
        }
        return MsgPack {
            serialize_part: GetRevokeResp {
                error_code: msg_and_error::OK,
                error_json: String::new(),
            },
            raw_bytes: Vec::new(),
        };
    }

    if let Some(inflight_info) = view
        .master_kv_router()
        .inner()
        .inflight_gets
        .get(&get_id)
        .await
    {
        if inflight_info.req_node_id != req_node_id
            && inflight_info.controller_node_id.as_ref() != Some(&req_node_id)
        {
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
                detail: format!(
                    "GetRevoke requester mismatch: get_id={} expected={} got={}",
                    get_id, inflight_info.req_node_id, req_node_id
                ),
            });
            return MsgPack {
                serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                raw_bytes: Vec::new(),
            };
        }
    } else if let Some(completed) = view
        .master_kv_router()
        .inner()
        .completed_gets
        .get(&get_id)
        .await
    {
        let detail = if completed.req_node_id != req_node_id {
            format!(
                "GetRevoke requester mismatch after completion: get_id={} expected={} got={}",
                get_id, completed.req_node_id, req_node_id
            )
        } else {
            format!(
                "GetRevoke lost the Done race; committed target must not be released: get_id={}",
                get_id
            )
        };
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument { detail });
        return MsgPack {
            serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
            raw_bytes: Vec::new(),
        };
    }

    // Remove from inflight_gets
    if let Some(inflight_info) = view
        .master_kv_router()
        .inner()
        .inflight_gets
        .remove(&get_id)
        .await
    {
        let _activity_completion =
            MasterKeyActivityCompletionGuard::new(inflight_info._activity_lease.clone());
        tracing::debug!("Revoked get operation with get_id: {}", get_id);
    } else {
        tracing::warn!("Get operation with get_id {} not found for revoke", get_id);
    }

    MsgPack {
        serialize_part: GetRevokeResp {
            error_code: msg_and_error::OK,
            error_json: String::new(),
        },
        raw_bytes: Vec::new(),
    }
}

async fn handle_get_done_locked(
    view: MasterKvRouterView,
    req: MsgPack<GetDoneReq>,
    req_node_id: NodeID,
    mut deferred_route_events: Option<&mut Vec<RoutePublishEvent>>,
    mut deferred_terminals: Option<&mut Vec<(u64, CompletedGetInfo)>>,
) -> MsgPack<GetDoneResp> {
    tracing::debug!("Handling GetDoneReq: {:?}", req.serialize_part);

    let get_id = req.serialize_part.get_id;
    if let Some(inflight_info) = view
        .master_kv_router()
        .inner()
        .inflight_gets
        .get(&get_id)
        .await
    {
        if inflight_info.req_node_id != req_node_id {
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
                detail: format!(
                    "GetDone requester mismatch: get_id={} expected={} got={}",
                    get_id, inflight_info.req_node_id, req_node_id
                ),
            });
            return MsgPack {
                serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                raw_bytes: Vec::new(),
            };
        }
    } else if let Some(completed) = view
        .master_kv_router()
        .inner()
        .completed_gets
        .get(&get_id)
        .await
    {
        if completed.req_node_id != req_node_id {
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
                detail: format!(
                    "GetDone requester mismatch after completion: get_id={} expected={} got={}",
                    get_id, completed.req_node_id, req_node_id
                ),
            });
            return MsgPack {
                serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                raw_bytes: Vec::new(),
            };
        }
        return MsgPack {
            serialize_part: completed.response,
            raw_bytes: Vec::new(),
        };
    }
    // Remove from inflight_gets and transfer to get_holding
    if let Some(inflight_info) = view
        .master_kv_router()
        .inner()
        .inflight_gets
        .remove(&get_id)
        .await
    {
        let _activity_completion =
            MasterKeyActivityCompletionGuard::new(inflight_info._activity_lease.clone());
        let mut allocation_mode = inflight_info.allocation_mode;
        // clone req_node_id to avoid borrow/move conflict when inserting into kv_routes
        let req_node_id = inflight_info.req_node_id.clone();
        let key = inflight_info.key;
        let target_cap = inflight_info.target.capacity();
        if allocation_mode == GetAllocationMode::ExternalSink {
            let generation_is_current = match &inflight_info.target {
                InflightGetTarget::ExternalSink(target) => {
                    external_sink_requester_generation_is_current(
                        &view,
                        &req_node_id,
                        target.requester_node_start_time,
                    )
                }
                _ => false,
            };
            let terminal = if generation_is_current {
                view.master_kv_router()
                    .view()
                    .metric_reporter()
                    .metrics()
                    .inc_kv_get_done_allocation("external_sink");
                GetDoneResp {
                    holder_id: 0,
                    allocation_mode: GetAllocationMode::ExternalSink,
                    error_code: msg_and_error::OK,
                    error_json: String::new(),
                    server_process_us: 0,
                }
            } else {
                let err = msg_and_error::KvError::Api(
                    msg_and_error::ApiError::InvalidPutMasterState {
                        detail: format!(
                            "external Get sink requester generation departed before Done: get_id={} key={} requester={}",
                            get_id, key, req_node_id
                        ),
                    },
                );
                crate::rpcresp_kvresult_convert::FromError::from_error(&err)
            };
            let completed = CompletedGetInfo {
                req_node_id: req_node_id.clone(),
                response: terminal.clone(),
            };
            if let Some(terminals) = deferred_terminals.as_deref_mut() {
                terminals.push((get_id, completed));
            } else {
                view.master_kv_router()
                    .inner()
                    .completed_gets
                    .insert(get_id, completed)
                    .await;
            }
            return MsgPack {
                serialize_part: terminal,
                raw_bytes: Vec::new(),
            };
        }

        let Some(target_tomb_tag) = inflight_info.target_tomb_tag.as_ref() else {
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
                detail: format!(
                    "allocator-backed Get lost its target generation: get_id={} key={} requester={}",
                    get_id, key, req_node_id
                ),
            });
            let terminal: GetDoneResp =
                crate::rpcresp_kvresult_convert::FromError::from_error(&err);
            let completed = CompletedGetInfo {
                req_node_id: req_node_id.clone(),
                response: terminal.clone(),
            };
            if let Some(terminals) = deferred_terminals.as_deref_mut() {
                terminals.push((get_id, completed));
            } else {
                view.master_kv_router()
                    .inner()
                    .completed_gets
                    .insert(get_id, completed)
                    .await;
            }
            return MsgPack {
                serialize_part: terminal,
                raw_bytes: Vec::new(),
            };
        };
        if !node_generation_is_current_live(&view, &req_node_id, target_tomb_tag) {
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
                detail: format!(
                    "GetDone target generation departed: get_id={} key={} requester={}",
                    get_id, key, req_node_id
                ),
            });
            let terminal: GetDoneResp =
                crate::rpcresp_kvresult_convert::FromError::from_error(&err);
            let completed = CompletedGetInfo {
                req_node_id: req_node_id.clone(),
                response: terminal.clone(),
            };
            if let Some(terminals) = deferred_terminals.as_deref_mut() {
                terminals.push((get_id, completed));
            } else {
                view.master_kv_router()
                    .inner()
                    .completed_gets
                    .insert(get_id, completed)
                    .await;
            }
            return MsgPack {
                serialize_part: terminal,
                raw_bytes: Vec::new(),
            };
        }
        // Allocation-backed Gets need a master holder to keep their allocator guard alive.
        // Local-reserve slots instead carry independent route and holder references in the
        // owner's slot state, so they deliberately do not create a master Allocation holder.
        let mut inserted_holder_key = None;
        let holder_id = match &inflight_info.target {
            InflightGetTarget::Allocation(allocation) => {
                let holder_id = view
                    .master_kv_router()
                    .inner()
                    .next_holder_id
                    .fetch_add(1, Ordering::Relaxed);
                let holder_key =
                    crate::memholder::NodeHolderKey::new(req_node_id.to_string(), holder_id);
                view.master_kv_router().inner().get_holding.insert(
                    holder_key.clone(),
                    OwnerHoldingGetInfo {
                        key: key.clone(),
                        holding_node_id: inflight_info.req_node_id.clone(),
                        len: inflight_info.len,
                        allocation: allocation.clone(),
                    },
                );
                inserted_holder_key = Some(holder_key);
                holder_id
            }
            InflightGetTarget::PreparedLocalReserveSlot(_) => 0,
            InflightGetTarget::ExternalSink(_) => {
                unreachable!("external Get sink must complete before holder publication")
            }
        };

        // Close the insertion-vs-MemberLeft cleanup race for the holder.  If
        // MemberLeft marked the shared tag before this check, remove the exact
        // holder we just inserted.  Otherwise its later cleanup must observe it.
        if target_tomb_tag.is_tomb() {
            if let Some(holder_key) = inserted_holder_key.as_ref() {
                view.master_kv_router()
                    .inner()
                    .get_holding
                    .remove(holder_key);
            }
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
                detail: format!(
                    "GetDone target generation departed during holder publication: get_id={} key={} requester={}",
                    get_id, key, req_node_id
                ),
            });
            let terminal: GetDoneResp =
                crate::rpcresp_kvresult_convert::FromError::from_error(&err);
            let completed = CompletedGetInfo {
                req_node_id: req_node_id.clone(),
                response: terminal.clone(),
            };
            if let Some(terminals) = deferred_terminals.as_deref_mut() {
                terminals.push((get_id, completed));
            } else {
                view.master_kv_router()
                    .inner()
                    .completed_gets
                    .insert(get_id, completed)
                    .await;
            }
            return MsgPack {
                serialize_part: terminal,
                raw_bytes: Vec::new(),
            };
        }

        if allocation_mode == GetAllocationMode::DurableReplica {
            let mut promote_committed = false;
            let mut route_publish_event = None;
            if let Some(one_kv_nodes_routes) = view
                .master_kv_router()
                .inner()
                .kv_routes
                .get(&key)
                .map(|route| route.clone())
            {
                if one_kv_nodes_routes.put_id == inflight_info.put_id {
                    match view.master_kv_router().reserve_node_cache_capacity(
                        &req_node_id,
                        target_tomb_tag,
                        ReservedCapacityReason::OwnerIndexedAllocation,
                        target_cap,
                    ) {
                        Ok(capacity_reservation) => {
                            let replica = KvRouteInfo {
                                node_id: req_node_id.clone(),
                                backing: super::KvReplicaBacking::Allocation(match &inflight_info
                                    .target
                                {
                                    InflightGetTarget::Allocation(allocation) => allocation.clone(),
                                    InflightGetTarget::PreparedLocalReserveSlot(_) => {
                                        unreachable!("durable Get mode must use Allocation target")
                                    }
                                    InflightGetTarget::ExternalSink(_) => {
                                        unreachable!("durable Get mode cannot use external sink")
                                    }
                                }),
                                owner_local_indexed: true,
                                get_durable_reservation: inflight_info.durable_reservation.clone(),
                                capacity_reservation,
                                tomb_tag: target_tomb_tag.clone(),
                            };
                            if publish_route_replica_tomb_fenced(
                                &one_kv_nodes_routes,
                                req_node_id.clone(),
                                replica,
                            ) {
                                promote_committed = true;
                                route_publish_event = Some(RoutePublishEvent::replica_append(
                                    key.clone(),
                                    inflight_info.put_id,
                                    one_kv_nodes_routes.lease_id,
                                    req_node_id.clone(),
                                    target_cap,
                                ));
                            } else {
                                tracing::warn!(
                                    "durable Get replica publication rejected by generation/live-replica fence: get_id={} put_id={:?}",
                                    get_id,
                                    one_kv_nodes_routes.put_id
                                );
                            }
                        }
                        Err(err) => tracing::warn!(
                            "durable Get could not reserve owner-indexed Allocation capacity; keeping temporary: get_id={} key={} owner={} err={}",
                            get_id,
                            key,
                            req_node_id,
                            err,
                        ),
                    }
                } else {
                    tracing::warn!(
                        "Put id mismatch, get replica is out of date, get_id: {}, new_put_id: {:?}, old_put_id: {:?}",
                        get_id,
                        one_kv_nodes_routes.put_id,
                        inflight_info.put_id
                    );
                }
            } else {
                tracing::warn!(
                    "Route disappeared before durable get commit, get_id: {}, key: {}",
                    get_id,
                    key
                );
            }
            if let Some(event) = route_publish_event {
                if let Some(events) = deferred_route_events.as_deref_mut() {
                    events.push(event);
                } else {
                    apply_post_route_maintenance_batch(&view, vec![event]).await;
                }
            }
            if !promote_committed {
                allocation_mode = GetAllocationMode::Temporary;
            }
        } else if allocation_mode == GetAllocationMode::ReuseReplica {
            let mut local_index_published = false;
            let mut route_lease_id = None;
            let capacity_reservation = view.master_kv_router().reserve_node_cache_capacity(
                &req_node_id,
                target_tomb_tag,
                ReservedCapacityReason::OwnerIndexedAllocation,
                target_cap,
            );
            match capacity_reservation {
                Ok(capacity_reservation) => {
                    if let Some(current_route) = view
                        .master_kv_router()
                        .inner()
                        .kv_routes
                        .get(&key)
                        .map(|route| route.clone())
                    {
                        if current_route.put_id == inflight_info.put_id {
                            route_lease_id = current_route.lease_id;
                            let mut replicas = current_route.nodes_replicas.write();
                            if let Some(replica) = replicas.get_mut(&req_node_id) {
                                local_index_published = !replica.tomb_tag.is_tomb()
                                    && replica.tomb_tag.same_generation(target_tomb_tag)
                                    && matches!(
                                        &replica.backing,
                                        super::KvReplicaBacking::Allocation(allocation)
                                            if matches!(
                                                &inflight_info.target,
                                                InflightGetTarget::Allocation(target)
                                                    if Arc::ptr_eq(allocation, target)
                                            )
                                    );
                                if local_index_published {
                                    replica.owner_local_indexed = true;
                                    replica.capacity_reservation = capacity_reservation;
                                }
                            }
                        }
                    }
                    if local_index_published {
                        let old_ring_b_desc = super::NodeValueReplicaDesc {
                            weight_bytes: u32::try_from(target_cap).unwrap_or(u32::MAX),
                            put_id: inflight_info.put_id,
                        };
                        let _ = view
                            .master_kv_router()
                            .remove_node_cache_entry_exact(
                                req_node_id.as_ref(),
                                &key,
                                &old_ring_b_desc,
                            )
                            .await;
                        let event = RoutePublishEvent::replica_append(
                            key.clone(),
                            inflight_info.put_id,
                            route_lease_id,
                            req_node_id.clone(),
                            target_cap,
                        );
                        if let Some(events) = deferred_route_events.as_deref_mut() {
                            events.push(event);
                        } else {
                            apply_post_route_maintenance_batch(&view, vec![event]).await;
                        }
                    }
                }
                Err(err) => tracing::warn!(
                    "reused Get allocation could not reserve owner-indexed capacity; keeping temporary: get_id={} key={} owner={} err={}",
                    get_id,
                    key,
                    req_node_id,
                    err,
                ),
            }
            if !local_index_published {
                tracing::warn!(
                    "Reused get allocation is no longer the current owner route; returning a temporary holder: get_id={} key={} put_id=({},{}) owner={}",
                    get_id,
                    key,
                    inflight_info.put_id.0,
                    inflight_info.put_id.1,
                    req_node_id
                );
                allocation_mode = GetAllocationMode::Temporary;
            }
        } else if allocation_mode == GetAllocationMode::LocalCommittedSlot {
            let slot = match &inflight_info.target {
                InflightGetTarget::PreparedLocalReserveSlot(slot) => slot.clone(),
                InflightGetTarget::Allocation(_) => {
                    unreachable!("local committed-slot Get mode must use a prepared slot")
                }
                InflightGetTarget::ExternalSink(_) => {
                    unreachable!("local committed-slot Get mode cannot use external sink")
                }
            };
            let mut published = false;
            let mut route_publish_event = None;
            if let Some(current_route) = view.master_kv_router().inner().kv_routes.get(&key) {
                if current_route.put_id == inflight_info.put_id {
                    let replica = KvRouteInfo {
                        node_id: req_node_id.clone(),
                        backing: super::KvReplicaBacking::CommittedSlot(slot),
                        owner_local_indexed: true,
                        get_durable_reservation: None,
                        capacity_reservation: None,
                        tomb_tag: target_tomb_tag.clone(),
                    };
                    if publish_route_replica_tomb_fenced(
                        &current_route,
                        req_node_id.clone(),
                        replica,
                    ) {
                        published = true;
                        route_publish_event = Some(RoutePublishEvent::replica_append(
                            key.clone(),
                            inflight_info.put_id,
                            current_route.lease_id,
                            req_node_id.clone(),
                            target_cap,
                        ));
                    }
                }
            }
            if !published {
                let err = msg_and_error::KvError::Api(msg_and_error::ApiError::Unknown {
                    detail: format!(
                        "prepared local-reserve Get target could not publish current route: get_id={} key={} put_id=({},{}) owner={}",
                        get_id, key, inflight_info.put_id.0, inflight_info.put_id.1, req_node_id
                    ),
                });
                let terminal: GetDoneResp =
                    crate::rpcresp_kvresult_convert::FromError::from_error(&err);
                let completed = CompletedGetInfo {
                    req_node_id: req_node_id.clone(),
                    response: terminal.clone(),
                };
                if let Some(terminals) = deferred_terminals.as_deref_mut() {
                    terminals.push((get_id, completed));
                } else {
                    view.master_kv_router()
                        .inner()
                        .completed_gets
                        .insert(get_id, completed)
                        .await;
                }
                return MsgPack {
                    serialize_part: terminal,
                    raw_bytes: Vec::new(),
                };
            }
            if let Some(event) = route_publish_event {
                if let Some(events) = deferred_route_events.as_deref_mut() {
                    events.push(event);
                } else {
                    apply_post_route_maintenance_batch(&view, vec![event]).await;
                }
            }
        }

        tracing::debug!(
            "Completed get operation with get_id: {}, assigned holder_id: {}",
            get_id,
            holder_id
        );
        view.master_kv_router()
            .view()
            .metric_reporter()
            .metrics()
            .inc_kv_get_done_allocation(match allocation_mode {
                GetAllocationMode::Temporary => "temporary",
                GetAllocationMode::ReuseReplica => "reuse_replica",
                GetAllocationMode::DurableReplica => "durable_replica",
                GetAllocationMode::LocalCommittedSlot => "local_committed_slot",
                GetAllocationMode::ExternalSink => "external_sink",
            });

        let terminal = GetDoneResp {
            holder_id,
            allocation_mode,
            error_code: msg_and_error::OK,
            error_json: String::new(),
            server_process_us: 0,
        };
        let completed = CompletedGetInfo {
            req_node_id: req_node_id.clone(),
            response: terminal.clone(),
        };
        if let Some(terminals) = deferred_terminals.as_deref_mut() {
            terminals.push((get_id, completed));
        } else {
            view.master_kv_router()
                .inner()
                .completed_gets
                .insert(get_id, completed)
                .await;
        }
        MsgPack {
            serialize_part: terminal,
            raw_bytes: Vec::new(),
        }
    } else {
        if let Some(completed) = view
            .master_kv_router()
            .inner()
            .completed_gets
            .get(&get_id)
            .await
        {
            if completed.req_node_id != req_node_id {
                let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
                    detail: format!(
                        "GetDone requester mismatch after completion: get_id={} expected={} got={}",
                        get_id, completed.req_node_id, req_node_id
                    ),
                });
                return MsgPack {
                    serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                    raw_bytes: Vec::new(),
                };
            }
            return MsgPack {
                serialize_part: completed.response,
                raw_bytes: Vec::new(),
            };
        }
        tracing::warn!(
            "Get operation with get_id {} not found for completion",
            get_id
        );
        // Inflight get entry likely expired (TTL ~ 60s). Treat as GetTimeout.
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::GetTimeout {
            timeout_ms: 60_000,
            detail: format!(
                "Get operation with get_id {} not found for completion; this is rare unless the system is overloaded or unstable",
                get_id
            ),
        });
        let mut r: GetDoneResp = crate::rpcresp_kvresult_convert::FromError::from_error(&err);
        r.holder_id = 0;
        MsgPack {
            serialize_part: r,
            raw_bytes: Vec::new(),
        }
    }
}

pub async fn handle_get_done(
    view: MasterKvRouterView,
    req: MsgPack<GetDoneReq>,
    req_node_id: NodeID,
) -> MsgPack<GetDoneResp> {
    let done_lock = view
        .master_kv_router()
        .inner()
        .get_done_locks
        .get_lock(req.serialize_part.get_id);
    let _done_guard = done_lock.lock().await;
    handle_get_done_locked(view, req, req_node_id, None, None).await
}

// --- MemHolder Handler Functions ---

// pub async fn handle_mem_holder_keep_alive(
//     view: MasterKvRouterView,
//     req: MsgPack<MemHolderKeepAliveReq>,
// ) -> MsgPack<MemHolderKeepAliveResp> {
//     tracing::debug!("Handling MemHolderKeepAliveReq: {:?}", req.serialize_part);

//     let holder_id = req.serialize_part.holder_id;

//     // Just getting the item from cache will refresh its TTL
//     if let Some(_) = view
//         .master_kv_router()
//         .inner()
//         .get_holding
//         .get(&holder_id)
//         .await
//     {
//         tracing::debug!("Keep alive refreshed for holder_id: {}", holder_id);
//         MsgPack {
//             serialize_part: MemHolderKeepAliveResp {
//                 error_code: KvErrorCode::Ok as u32,
//                 error_msg: String::new(),
//             },
//             raw_bytes: Vec::new(),
//         }
//     } else {
//         tracing::warn!("Holder with holder_id {} not found or expired", holder_id);
//         MsgPack {
//             serialize_part: MemHolderKeepAliveResp {
//                 error_code: KvErrorCode::KeyNotFound as u32,
//                 error_msg: format!("Holder with holder_id {} not found or expired", holder_id),
//             },
//             raw_bytes: Vec::new(),
//         }
//     }
// }

// pub async fn handle_mem_holder_release(
//     view: MasterKvRouterView,
//     req: MsgPack<MemHolderReleaseReq>,
// ) -> MsgPack<MemHolderReleaseResp> {
//     tracing::debug!("Handling MemHolderReleaseReq: {:?}", req.serialize_part);

//     let holder_id = req.serialize_part.holder_id;

//     // Remove from get_holding to release the memory
//     if let Some(_) = view
//         .master_kv_router()
//         .inner()
//         .get_holding
//         .remove(&holder_id)
//     {
//         tracing::info!("Released holder with holder_id: {}", holder_id);
//         MsgPack {
//             serialize_part: MemHolderReleaseResp {
//                 error_code: KvErrorCode::Ok as u32,
//                 error_msg: String::new(),
//             },
//             raw_bytes: Vec::new(),
//         }
//     } else {
//         tracing::warn!("Holder with holder_id {} not found for release", holder_id);
//         MsgPack {
//             serialize_part: MemHolderReleaseResp {
//                 error_code: KvErrorCode::KeyNotFound as u32,
//                 error_msg: format!("Holder with holder_id {} not found", holder_id),
//             },
//             raw_bytes: Vec::new(),
//         }
//     }
// }

pub async fn handle_get_meta(
    view: MasterKvRouterView,
    req: MsgPack<GetMetaReq>,
    _req_node_id: NodeID,
) -> MsgPack<GetMetaResp> {
    tracing::debug!("Handling GetMetaReq: {:?}", req.serialize_part);

    // Note: Do not alter logic path for tests; tests must observe real behavior.

    // Check if key exists in kv_routes
    if let Some(one_kv_nodes_routes) = view
        .master_kv_router()
        .inner()
        .kv_routes
        .get(&req.serialize_part.key)
    {
        // lock and clone, release the lock quickly
        let nodes_replicas: HashMap<NodeID, KvRouteInfo> =
            (*one_kv_nodes_routes.nodes_replicas.read()).clone();

        // Key exists, get metadata from the first replica
        for (_, kv_info) in nodes_replicas.iter() {
            if kv_info.tomb_tag.is_tomb() {
                continue;
            }
            let len = kv_info.backing.len();
            return MsgPack {
                serialize_part: GetMetaResp {
                    exists: true,
                    len,
                    error_code: msg_and_error::OK,
                    error_json: String::new(),
                },
                raw_bytes: Vec::new(),
            };
        }
        // if let Some((_, kv_info)) = replicas.iter().next() {
        //     let len = kv_info.allocation.size();

        //     MsgPack {
        //         serialize_part: GetMetaResp {
        //             exists: true,
        //             len,
        //             error_code: KvErrorCode::Ok as u32,
        //             error_msg: String::new(),
        //         },
        //         raw_bytes: Vec::new(),
        //     }
        // } else {
        //     // This shouldn't happen, but handle it gracefully
        //     MsgPack {
        //         serialize_part: GetMetaResp {
        //             exists: false,
        //             len: 0,
        //             error_code: KvErrorCode::KeyNotFound as u32,
        //             error_msg: "Key not found".to_string(),
        //         },
        //         raw_bytes: Vec::new(),
        //     }
        // }
        {
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::KeyNotFound {
                key: req.serialize_part.key.clone(),
            });
            let mut r: GetMetaResp = crate::rpcresp_kvresult_convert::FromError::from_error(&err);
            r.exists = false;
            r.len = 0;
            MsgPack {
                serialize_part: r,
                raw_bytes: Vec::new(),
            }
        }
    } else {
        // Key not found
        {
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::KeyNotFound {
                key: req.serialize_part.key.clone(),
            });
            let mut r: GetMetaResp = crate::rpcresp_kvresult_convert::FromError::from_error(&err);
            r.exists = false;
            r.len = 0;
            MsgPack {
                serialize_part: r,
                raw_bytes: Vec::new(),
            }
        }
    }
}

pub async fn handle_batch_is_exist(
    view: MasterKvRouterView,
    req: MsgPack<BatchIsExistReq>,
    _req_node_id: NodeID,
) -> MsgPack<BatchIsExistResp> {
    tracing::debug!(
        "Handling BatchIsExistReq: batch_len={}",
        req.serialize_part.keys.len()
    );

    let mut exists_list = Vec::with_capacity(req.serialize_part.keys.len());

    for key in &req.serialize_part.keys {
        if let Some(one_kv_nodes_routes) = view.master_kv_router().inner().kv_routes.get(key) {
            let exists = one_kv_routes_has_live_replica(&one_kv_nodes_routes);
            exists_list.push(exists);
        } else {
            exists_list.push(false);
        }
    }

    MsgPack {
        serialize_part: BatchIsExistResp {
            exists_list,
            error_code: msg_and_error::OK,
            error_json: String::new(),
        },
        raw_bytes: Vec::new(),
    }
}

pub async fn handle_batch_get_start(
    view: MasterKvRouterView,
    req: MsgPack<BatchGetStartReq>,
    req_node_id: NodeID,
) -> MsgPack<BatchGetStartResp> {
    let BatchGetStartReq {
        keys,
        prepared_targets,
        external_sink_targets,
    } = req.serialize_part;
    if !prepared_targets.is_empty() && prepared_targets.len() != keys.len() {
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
            detail: format!(
                "batch_get_start prepared target length mismatch: keys={} targets={}",
                keys.len(),
                prepared_targets.len()
            ),
        });
        let error: GetStartResp = crate::rpcresp_kvresult_convert::FromError::from_error(&err);
        return MsgPack {
            serialize_part: BatchGetStartResp {
                items: Vec::new(),
                error_code: error.error_code,
                error_json: error.error_json,
                server_process_us: 0,
            },
            raw_bytes: Vec::new(),
        };
    }
    if !external_sink_targets.is_empty() && external_sink_targets.len() != keys.len() {
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
            detail: format!(
                "batch_get_start external sink target length mismatch: keys={} targets={}",
                keys.len(),
                external_sink_targets.len()
            ),
        });
        let error: GetStartResp = crate::rpcresp_kvresult_convert::FromError::from_error(&err);
        return MsgPack {
            serialize_part: BatchGetStartResp {
                items: Vec::new(),
                error_code: error.error_code,
                error_json: error.error_json,
                server_process_us: 0,
            },
            raw_bytes: Vec::new(),
        };
    }
    let prepared_targets = if prepared_targets.is_empty() {
        vec![None; keys.len()]
    } else {
        prepared_targets
    };
    let external_sink_targets = if external_sink_targets.is_empty() {
        vec![None; keys.len()]
    } else {
        external_sink_targets
    };
    let mut items = Vec::with_capacity(keys.len());
    for ((key, prepared_target), external_sink_target) in keys
        .into_iter()
        .zip(prepared_targets)
        .zip(external_sink_targets)
    {
        let (_get_id, resp) = handle_get_start(
            view.clone(),
            MsgPack {
                serialize_part: GetStartReq {
                    key,
                    prepared_target,
                    external_sink_target,
                },
                raw_bytes: Vec::new(),
            },
            req_node_id.clone(),
        )
        .await;
        let part = resp.serialize_part;
        items.push(BatchGetStartItemResp {
            get_id: part.get_id,
            node_id: part.node_id,
            put_id: part.put_id,
            target_addr: part.target_addr,
            src_addr: part.src_addr,
            target_base_addr: part.target_base_addr,
            src_base_addr: part.src_base_addr,
            len: part.len,
            prepared_target: part.prepared_target,
            atomic_group: part.atomic_group,
            error_code: part.error_code,
            error_json: part.error_json,
        });
    }
    MsgPack {
        serialize_part: BatchGetStartResp {
            items,
            error_code: msg_and_error::OK,
            error_json: String::new(),
            server_process_us: 0,
        },
        raw_bytes: Vec::new(),
    }
}

pub async fn handle_batch_get_revoke(
    view: MasterKvRouterView,
    req: MsgPack<BatchGetRevokeReq>,
    req_node_id: NodeID,
) -> MsgPack<BatchGetRevokeResp> {
    let mut items = Vec::with_capacity(req.serialize_part.get_ids.len());
    for get_id in req.serialize_part.get_ids {
        let resp = handle_get_revoke(
            view.clone(),
            MsgPack {
                serialize_part: GetRevokeReq { get_id },
                raw_bytes: Vec::new(),
            },
            req_node_id.clone(),
        )
        .await;
        let part = resp.serialize_part;
        items.push(BatchGetRevokeItemResp {
            get_id,
            error_code: part.error_code,
            error_json: part.error_json,
        });
    }
    MsgPack {
        serialize_part: BatchGetRevokeResp {
            items,
            error_code: msg_and_error::OK,
            error_json: String::new(),
        },
        raw_bytes: Vec::new(),
    }
}

pub async fn handle_batch_get_done(
    view: MasterKvRouterView,
    req: MsgPack<BatchGetDoneReq>,
    req_node_id: NodeID,
) -> MsgPack<BatchGetDoneResp> {
    let started_at = Instant::now();
    let get_ids = req.serialize_part.get_ids;

    // Hold every per-get terminal lock until the combined route-maintenance
    // batch and idempotency records are durable. Sorting gives overlapping
    // retries one global acquisition order and avoids lock cycles.
    let mut unique_get_ids = get_ids.clone();
    unique_get_ids.sort_unstable();
    unique_get_ids.dedup();
    let mut _done_guards = Vec::with_capacity(unique_get_ids.len());
    for get_id in unique_get_ids {
        let lock = view
            .master_kv_router()
            .inner()
            .get_done_locks
            .get_lock(get_id);
        _done_guards.push(lock.lock_owned().await);
    }

    let mut items = Vec::with_capacity(get_ids.len());
    let mut route_events = Vec::new();
    let mut deferred_terminals = Vec::new();
    let mut response_by_get_id = HashMap::<u64, GetDoneResp>::new();
    for get_id in get_ids {
        let part = if let Some(part) = response_by_get_id.get(&get_id) {
            part.clone()
        } else {
            let part = handle_get_done_locked(
                view.clone(),
                MsgPack {
                    serialize_part: GetDoneReq { get_id },
                    raw_bytes: Vec::new(),
                },
                req_node_id.clone(),
                Some(&mut route_events),
                Some(&mut deferred_terminals),
            )
            .await
            .serialize_part;
            response_by_get_id.insert(get_id, part.clone());
            part
        };
        items.push(BatchGetDoneItemResp {
            get_id,
            holder_id: part.holder_id,
            allocation_mode: part.allocation_mode,
            error_code: part.error_code,
            error_json: part.error_json,
        });
    }

    let route_event_count = route_events.len();
    let maintenance_started_at = Instant::now();
    if !route_events.is_empty() {
        // One Moka capacity decision for the entire Done RPC replaces the old
        // per-key full-LRU scan while preserving the rule that no success ACK
        // is visible before every route is admitted to resident policy state.
        apply_post_route_maintenance_batch(&view, route_events).await;
    }
    let maintenance_elapsed = maintenance_started_at.elapsed();
    for (get_id, completed) in deferred_terminals {
        view.master_kv_router()
            .inner()
            .completed_gets
            .insert(get_id, completed)
            .await;
    }
    let elapsed = started_at.elapsed();
    if elapsed.as_millis() >= 100 {
        tracing::warn!(
            "slow BatchGetDone convergence: items={} route_events={} maintenance_ms={} total_ms={}",
            items.len(),
            route_event_count,
            maintenance_elapsed.as_millis(),
            elapsed.as_millis()
        );
    }
    MsgPack {
        serialize_part: BatchGetDoneResp {
            items,
            error_code: msg_and_error::OK,
            error_json: String::new(),
            server_process_us: 0,
        },
        raw_bytes: Vec::new(),
    }
}
