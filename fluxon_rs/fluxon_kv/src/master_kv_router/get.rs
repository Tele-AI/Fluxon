use super::{
    CommittedSlotReplica, CompletedGetInfo, InflightGetInfo, InflightGetTarget, KvMemoryReplica,
    KvNodeReplicas, MasterKeyActivityCompletionGuard, MasterKvRouterView, OwnerHoldingGetInfo,
    PostReadRemoteReclaimCandidate, ReservedCapacityReason,
    msg_pack::{
        BatchGetBindItemReq, BatchGetBindReq, BatchGetBindResp, BatchGetDoneItemReq,
        BatchGetDoneItemResp, BatchGetDoneReq, BatchGetDoneResp, BatchGetPlanItemResp,
        BatchGetPlanReq, BatchGetPlanResp, BatchGetRevokeItemResp, BatchGetRevokeReq,
        BatchGetRevokeResp, BatchGetStartItemResp, BatchGetStartReq, BatchGetStartResp,
        BatchIsExistReq, BatchIsExistResp, GetAllocationMode, GetBindTarget, GetDoneReq,
        GetDoneResp, GetExternalSinkTarget, GetMetaReq, GetMetaResp, GetPreparedLocalReserveTarget,
        GetRevokeReq, GetRevokeResp, GetSourceKind, GetStartReq, GetStartResp,
        MemHolderKeepAliveReq, MemHolderKeepAliveResp, MemHolderReleaseReq, MemHolderReleaseResp,
        PutAtomicGroup, SsdStageBeginReq, SsdStageBeginResp, SsdStageDoneReq, SsdStageDoneResp,
    },
    node_generation_is_current_live, publish_route_replica_tomb_fenced,
    route_maintenance::{RoutePublishEvent, apply_post_route_maintenance_batch},
};
use crate::config::SsdReadSourcePolicy;
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
        .node_replicas
        .read()
        .values()
        .any(KvNodeReplicas::has_live_backing)
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
    if target.capacity_bytes == 0 {
        return Err(invalid(
            "prepared owner Get target has zero capacity".to_string(),
        ));
    }
    if value_len > target.capacity_bytes {
        return Err(invalid(format!(
            "prepared owner Get target is too small: value_len={} capacity_bytes={}",
            value_len, target.capacity_bytes
        )));
    }
    let current_owner = view
        .cluster_manager()
        .get_member_info_cached(req_node_id.as_ref())
        .ok_or_else(|| {
            invalid(format!(
                "prepared owner Get target owner is absent: {req_node_id}"
            ))
        })?;
    if target.owner.node_id.as_str() != req_node_id.as_ref()
        || target.owner.node_start_time != current_owner.node_start_time
        || target.len != value_len
        || target.segment_registration_epoch == 0
    {
        return Err(invalid(format!(
            "prepared owner Get target generation/length mismatch: requester={} descriptor_owner={} descriptor_start={} current_start={} descriptor_len={} value_len={} registration_epoch={}",
            req_node_id,
            target.owner.node_id,
            target.owner.node_start_time,
            current_owner.node_start_time,
            target.len,
            value_len,
            target.segment_registration_epoch,
        )));
    }
    let tomb_tag = view.master_seg_manager().validate_owner_slot_geometry(
        req_node_id,
        target.allocation_id,
        target.segment_offset,
        target.capacity_bytes,
        target.base_addr,
        target.addr,
    ).ok_or_else(|| {
        invalid(format!(
            "prepared owner Get target failed segment geometry validation: allocation_id={} segment_offset={} capacity_bytes={} base={:#x} addr={:#x}",
            target.allocation_id,
            target.segment_offset,
            target.capacity_bytes,
            target.base_addr,
            target.addr
        ))
    })?;
    if target.allocation_id == 0 {
        return Err(invalid(format!(
            "prepared owner Get target has zero allocation_id: requester={}",
            req_node_id
        )));
    }
    Ok((target.clone(), tomb_tag))
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
    source_kind: GetSourceKind,
    memory_is_allocation: bool,
    memory_is_committed_slot: bool,
    owner_local_indexed: bool,
    owner_slot: Option<CommittedSlotReplica>,
}

fn planned_get_source_rank(
    policy: SsdReadSourcePolicy,
    source_kind: GetSourceKind,
    requester_local: bool,
) -> Option<u8> {
    match policy {
        SsdReadSourcePolicy::LegacyRemoteFirst => match (source_kind, requester_local) {
            (GetSourceKind::Memory, false) => Some(0),
            (GetSourceKind::Memory, true) => Some(1),
            (GetSourceKind::Ssd, false) => Some(2),
            (GetSourceKind::Ssd, true) => Some(3),
        },
        SsdReadSourcePolicy::LocalSsdOnlyFirst => match (source_kind, requester_local) {
            (GetSourceKind::Memory, true) => Some(0),
            (GetSourceKind::Ssd, true) => Some(1),
            (GetSourceKind::Memory, false) => Some(2),
            (GetSourceKind::Ssd, false) => None,
        },
    }
}

fn snapshot_live_get_sources(route: &OneKvNodesRoutes) -> Vec<PlannedGetSourceSnapshot> {
    route
        .node_replicas
        .read()
        .iter()
        .filter_map(|(node_id, replicas)| {
            if replicas.tomb_tag.is_tomb() {
                return None;
            }
            if let Some(memory) = replicas.memory.as_ref() {
                let owner_slot = match &memory.backing {
                    super::KvReplicaBacking::CommittedSlot(slot) => Some(slot.clone()),
                    super::KvReplicaBacking::Allocation(_) => None,
                };
                return Some(PlannedGetSourceSnapshot {
                    node_id: node_id.clone(),
                    tomb_tag: replicas.tomb_tag.clone(),
                    len: memory.backing.len(),
                    addr: memory.backing.abs_addr(),
                    base_addr: memory.backing.base_addr(),
                    source_kind: GetSourceKind::Memory,
                    memory_is_allocation: matches!(
                        &memory.backing,
                        super::KvReplicaBacking::Allocation(_)
                    ),
                    memory_is_committed_slot: matches!(
                        &memory.backing,
                        super::KvReplicaBacking::CommittedSlot(_)
                    ),
                    owner_local_indexed: memory.owner_local_indexed,
                    owner_slot,
                });
            }
            replicas.ssd.as_ref().map(|ssd| PlannedGetSourceSnapshot {
                node_id: node_id.clone(),
                tomb_tag: replicas.tomb_tag.clone(),
                len: ssd.len,
                addr: 0,
                base_addr: 0,
                source_kind: GetSourceKind::Ssd,
                memory_is_allocation: false,
                memory_is_committed_slot: false,
                owner_local_indexed: false,
                owner_slot: None,
            })
        })
        .collect()
}

fn owner_source_route_token(
    key: &str,
    put_id: PutIDForAKey,
    atomic_batch: Option<PutAtomicGroup>,
    get_id: u64,
    slot: Option<&CommittedSlotReplica>,
) -> Option<crate::owner_segment::OwnerSourceRouteToken> {
    let slot = slot?.clone();
    Some(crate::owner_segment::OwnerSourceRouteToken {
        key: key.to_string(),
        put_id,
        // Owner-managed routes currently publish the exact allocation id as
        // their generation-safe route epoch. Scope conversion keeps it.
        route_epoch: slot.allocation_id,
        source: slot,
        atomic_batch,
        // get_id may start at zero; owner lease identities require non-zero.
        plan_nonce: get_id.checked_add(1).expect("master Get id overflow"),
    })
}

fn owner_ssd_source_route_token(
    view: &MasterKvRouterView,
    owner: &NodeID,
    key: &str,
    put_id: PutIDForAKey,
    len: u64,
    atomic_batch: Option<PutAtomicGroup>,
    get_id: u64,
) -> Option<crate::owner_segment::OwnerSsdSourceRouteToken> {
    if view
        .master_seg_manager()
        .get_node_allocation_authority(owner)
        != Some(crate::master_seg_manager::msg_pack::SegmentAllocationAuthority::Owner)
    {
        return None;
    }
    let member = view
        .cluster_manager()
        .get_member_info_cached(owner.as_ref())?;
    Some(crate::owner_segment::OwnerSsdSourceRouteToken {
        key: key.to_string(),
        put_id,
        owner: crate::owner_segment::OwnerGeneration::new(
            owner.to_string(),
            member.node_start_time,
        ),
        len,
        atomic_batch,
        plan_nonce: get_id.checked_add(1).expect("master Get id overflow"),
    })
}

fn planned_source_requester_local_borrow_eligible(
    source: &PlannedGetSourceSnapshot,
    local_owner: Option<&str>,
) -> bool {
    local_owner.is_some_and(|owner| source.node_id.as_ref() == owner)
        && (source.memory_is_allocation
            || (source.memory_is_committed_slot && !source.owner_local_indexed))
}

fn planned_get_source_is_current(
    planned: &super::PlannedGetInfo,
    route: &Arc<OneKvNodesRoutes>,
) -> bool {
    if planned.src_tomb_tag.is_tomb() || route.put_id != planned.put_id {
        return false;
    }
    route
        .node_replicas
        .read()
        .get(&planned.src_node_id)
        .is_some_and(|replicas| {
            !replicas.tomb_tag.is_tomb()
                && replicas.tomb_tag.same_generation(&planned.src_tomb_tag)
                && match planned.source_kind {
                    GetSourceKind::Memory => replicas.memory.as_ref().is_some_and(|memory| {
                        memory.backing.abs_addr() == planned.src_addr
                            && memory.backing.base_addr() == planned.src_base_addr
                            && memory.backing.len() == planned.len
                    }),
                    GetSourceKind::Ssd => replicas
                        .ssd
                        .as_ref()
                        .is_some_and(|ssd| ssd.len == planned.len),
                }
        })
}

fn planned_get_requester_local_target(
    view: &MasterKvRouterView,
    planned: &super::PlannedGetInfo,
    requester: &NodeID,
    get_id: u64,
) -> Result<
    (
        InflightGetTarget,
        NodeTombTag,
        GetAllocationMode,
        super::PlannedGetInfo,
    ),
    msg_and_error::KvError,
> {
    let route = view
        .master_kv_router()
        .inner()
        .kv_routes
        .get(&planned.key)
        .map(|route| route.clone())
        .ok_or_else(|| {
            msg_and_error::KvError::Api(msg_and_error::ApiError::StaleGetPlan {
                get_id,
                key: planned.key.clone(),
                detail: "requester-local source route disappeared before Bind".to_string(),
            })
        })?;
    planned_get_requester_local_target_from_route(planned, &route, requester, get_id)
}

/// Revalidate requester-local backing at Bind time instead of treating Plan
/// as an allocation reservation.  A remote Plan may legitimately age across
/// a LocalExclusive -> GlobalShared demotion; if the same put generation is
/// now present on the requester, Bind changes the effective source to that
/// exact slot and keeps the operation metadata-only.
fn planned_get_requester_local_target_from_route(
    planned: &super::PlannedGetInfo,
    route: &Arc<OneKvNodesRoutes>,
    requester: &NodeID,
    get_id: u64,
) -> Result<
    (
        InflightGetTarget,
        NodeTombTag,
        GetAllocationMode,
        super::PlannedGetInfo,
    ),
    msg_and_error::KvError,
> {
    let invalid = |detail: String| {
        msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument { detail })
    };
    if route.put_id != planned.put_id {
        return Err(msg_and_error::KvError::Api(
            msg_and_error::ApiError::StaleGetPlan {
                get_id,
                key: planned.key.clone(),
                detail: "requester-local generation changed before Bind".to_string(),
            },
        ));
    }
    let replicas = route.node_replicas.read();
    let node_replicas = replicas.get(requester).ok_or_else(|| {
        msg_and_error::KvError::Api(msg_and_error::ApiError::StaleGetPlan {
            get_id,
            key: planned.key.clone(),
            detail: format!(
                "requester-local source replica disappeared before Bind: requester={requester}"
            ),
        })
    })?;
    if node_replicas.tomb_tag.is_tomb() {
        return Err(msg_and_error::KvError::Api(
            msg_and_error::ApiError::StaleGetPlan {
                get_id,
                key: planned.key.clone(),
                detail: "requester-local owner generation is tombed".to_string(),
            },
        ));
    }
    let tomb_tag = node_replicas.tomb_tag.clone();
    let atomic_group = route.atomic_group.as_deref().cloned();
    let (target, allocation_mode, src_addr, src_base_addr, source_route_token) = match node_replicas
        .memory
        .as_ref()
    {
        Some(memory) => match &memory.backing {
            super::KvReplicaBacking::Allocation(allocation) if allocation.size() == planned.len => {
                (
                    InflightGetTarget::Allocation(allocation.clone()),
                    GetAllocationMode::RequesterLocalBorrow,
                    allocation.base_addr() + allocation.addr(),
                    allocation.base_addr(),
                    None,
                )
            }
            super::KvReplicaBacking::CommittedSlot(slot)
                if !memory.owner_local_indexed
                    && slot.owner.node_id.as_str() == requester.as_ref() =>
            {
                if slot.len != planned.len || !slot.is_valid() {
                    return Err(invalid(format!(
                        "requester-local GlobalShared slot geometry changed: key={} requester={} planned_len={} actual_len={}",
                        planned.key, requester, planned.len, slot.len
                    )));
                }
                let source_route_token = owner_source_route_token(
                    &planned.key,
                    route.put_id,
                    atomic_group.clone(),
                    get_id,
                    Some(slot),
                )
                .ok_or_else(|| {
                    invalid(format!(
                        "requester-local GlobalShared slot has no owner source token: key={} requester={}",
                        planned.key, requester
                    ))
                })?;
                (
                    InflightGetTarget::ReusedCommittedSlot(slot.clone()),
                    GetAllocationMode::RequesterLocalPromote,
                    slot.addr,
                    slot.base_addr,
                    Some(source_route_token),
                )
            }
            super::KvReplicaBacking::CommittedSlot(_) => {
                return Err(invalid(format!(
                    "requester-local CommittedSlot is not a GlobalShared slot owned by the requester: key={} requester={}",
                    planned.key, requester
                )));
            }
            _ => {
                return Err(invalid(format!(
                    "requester-local source length changed: key={} requester={} planned_len={}",
                    planned.key, requester, planned.len
                )));
            }
        },
        None => {
            return Err(invalid(format!(
                "requester-local source memory disappeared: key={} requester={}",
                planned.key, requester
            )));
        }
    };
    let rebound = super::PlannedGetInfo {
        put_id: route.put_id,
        src_node_id: requester.clone(),
        src_tomb_tag: tomb_tag.clone(),
        key: planned.key.clone(),
        controller_node_id: planned.controller_node_id.clone(),
        controller_node_start_time: planned.controller_node_start_time,
        len: planned.len,
        src_addr,
        src_base_addr,
        source_kind: GetSourceKind::Memory,
        source_route_token,
        ssd_source_route_token: None,
        atomic_group,
    };
    if !planned_get_source_is_current(&rebound, route) {
        return Err(msg_and_error::KvError::Api(
            msg_and_error::ApiError::StaleGetPlan {
                get_id,
                key: planned.key.clone(),
                detail: "requester-local source changed during Bind".to_string(),
            },
        ));
    }
    Ok((target, tomb_tag, allocation_mode, rebound))
}

/// Change one exact owner-managed slot from GlobalShared to LocalExclusive.
/// The physical allocation and payload address remain unchanged; the caller
/// removes the matching ring-B identity after this metadata transition.
fn promote_global_shared_committed_slot(
    route: &OneKvNodesRoutes,
    requester: &NodeID,
    expected_tomb_tag: &NodeTombTag,
    expected: &CommittedSlotReplica,
) -> Option<super::NodeValueReplicaDesc> {
    if route.lease_id.is_some() {
        return None;
    }
    let mut replicas = route.node_replicas.write();
    let node_replicas = replicas.get_mut(requester)?;
    if node_replicas.tomb_tag.is_tomb()
        || !node_replicas.tomb_tag.same_generation(expected_tomb_tag)
    {
        return None;
    }
    let replica = node_replicas.memory.as_mut()?;
    let super::KvReplicaBacking::CommittedSlot(actual) = &replica.backing else {
        return None;
    };
    if replica.owner_local_indexed
        || actual.owner.node_id.as_str() != requester.as_ref()
        || actual.allocation_id != expected.allocation_id
        || actual.segment_offset != expected.segment_offset
        || actual.capacity_bytes != expected.capacity_bytes
        || actual.addr != expected.addr
        || actual.base_addr != expected.base_addr
        || actual.len != expected.len
    {
        return None;
    }
    replica.owner_local_indexed = true;
    Some(super::NodeValueReplicaDesc {
        weight_bytes: u32::try_from(expected.capacity_bytes).unwrap_or(u32::MAX),
        put_id: route.put_id,
    })
}

#[cfg(test)]
mod planned_get_tests {
    use super::{
        late_bind_target_for_done, owner_source_route_token,
        planned_get_requester_local_target_from_route, planned_get_source_is_current,
        planned_get_source_rank, planned_source_requester_local_borrow_eligible,
        promote_global_shared_committed_slot, snapshot_live_get_sources,
    };
    use crate::cluster_manager::NodeID;
    use crate::config::SsdReadSourcePolicy;
    use crate::master_kv_router::msg_pack::{
        BatchGetDoneItemReq, GetBindTarget, GetExternalSinkTarget, GetSourceKind,
    };
    use crate::master_kv_router::{
        CommittedSlotReplica, KvMemoryReplica, KvNodeReplicas, KvReplicaBacking, KvSsdReplica,
        OneKvNodesRoutes, PlannedGetInfo,
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
            radix: None,
            lease_id: None,
            atomic_group: None,
            node_replicas: RwLock::new(HashMap::from([(
                source.clone(),
                KvNodeReplicas::memory(
                    source_tag.clone(),
                    KvMemoryReplica {
                        backing: KvReplicaBacking::CommittedSlot(CommittedSlotReplica {
                            owner: crate::owner_segment::OwnerGeneration::new(
                                source.as_ref().to_string(),
                                1,
                            ),
                            allocation_id: 11,
                            segment_offset: 2 * 8192,
                            capacity_bytes: 8192,
                            addr: 0x3000,
                            len: 4096,
                            base_addr: 0x1000,
                            segment_registration_epoch: 1,
                        }),
                        owner_local_indexed: true,
                        get_durable_reservation: None,
                        capacity_reservation: None,
                    },
                ),
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
            source_kind: GetSourceKind::Memory,
            source_route_token: None,
            ssd_source_route_token: None,
            atomic_group: None,
        };
        (planned, route, source_tag)
    }

    #[test]
    fn bind_revalidation_accepts_only_the_exact_source_generation() {
        let (planned, route, source_tag) = planned_route();
        assert!(planned_get_source_is_current(&planned, &route));

        route
            .node_replicas
            .write()
            .get_mut(&planned.src_node_id)
            .unwrap()
            .memory
            .as_mut()
            .unwrap()
            .backing = KvReplicaBacking::CommittedSlot(CommittedSlotReplica {
            owner: crate::owner_segment::OwnerGeneration::new(
                planned.src_node_id.as_ref().to_string(),
                1,
            ),
            allocation_id: 11,
            segment_offset: 2 * 8192,
            capacity_bytes: 8192,
            addr: 0x4000,
            len: 4096,
            base_addr: 0x1000,
            segment_registration_epoch: 1,
        });
        assert!(!planned_get_source_is_current(&planned, &route));

        route
            .node_replicas
            .write()
            .get_mut(&planned.src_node_id)
            .unwrap()
            .memory
            .as_mut()
            .unwrap()
            .backing = KvReplicaBacking::CommittedSlot(CommittedSlotReplica {
            owner: crate::owner_segment::OwnerGeneration::new(
                planned.src_node_id.as_ref().to_string(),
                1,
            ),
            allocation_id: 11,
            segment_offset: 2 * 8192,
            capacity_bytes: 8192,
            addr: 0x3000,
            len: 4096,
            base_addr: 0x1000,
            segment_registration_epoch: 1,
        });
        source_tag.set_tomb();
        assert!(!planned_get_source_is_current(&planned, &route));
    }

    #[test]
    fn owner_source_token_uses_exact_slot_and_nonzero_first_get_nonce() {
        let (_planned, route, _source_tag) = planned_route();
        let source = snapshot_live_get_sources(&route)
            .into_iter()
            .next()
            .expect("owner source snapshot");
        let slot = source.owner_slot.expect("committed owner slot");
        let token = owner_source_route_token("key", route.put_id, None, 0, Some(&slot))
            .expect("owner source token");
        assert_eq!(token.key, "key");
        assert_eq!(token.put_id, route.put_id);
        assert_eq!(token.route_epoch, slot.allocation_id);
        assert_eq!(token.source, slot);
        assert_eq!(token.plan_nonce, 1);
    }

    #[test]
    fn bind_revalidation_rejects_a_replacement_route_generation() {
        let (planned, route, _) = planned_route();
        let replacement_tag = NodeTombTag::new();
        route
            .node_replicas
            .write()
            .get_mut(&planned.src_node_id)
            .unwrap()
            .tomb_tag = replacement_tag;
        assert!(!planned_get_source_is_current(&planned, &route));
    }

    #[test]
    fn ssd_plan_and_bind_revalidate_the_same_owner_backing() {
        let (mut planned, route, _) = planned_route();
        planned.source_kind = GetSourceKind::Ssd;
        planned.src_addr = 0;
        planned.src_base_addr = 0;
        let mut replicas = route.node_replicas.write();
        let source = replicas.get_mut(&planned.src_node_id).unwrap();
        source.memory = None;
        source.ssd = Some(KvSsdReplica { len: planned.len });
        drop(replicas);

        let sources = snapshot_live_get_sources(&route);
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].source_kind, GetSourceKind::Ssd);
        assert_eq!(sources[0].addr, 0);
        assert_eq!(sources[0].base_addr, 0);
        assert!(planned_get_source_is_current(&planned, &route));

        route
            .node_replicas
            .write()
            .get_mut(&planned.src_node_id)
            .unwrap()
            .ssd
            .as_mut()
            .unwrap()
            .len += 1;
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

    #[test]
    fn requester_local_allocation_or_global_shared_slot_is_zero_copy_eligible() {
        let (planned, route, _) = planned_route();
        let sources = snapshot_live_get_sources(&route);
        assert!(!sources[0].memory_is_allocation);
        assert!(sources[0].memory_is_committed_slot);
        assert!(sources[0].owner_local_indexed);
        assert!(!planned_source_requester_local_borrow_eligible(
            &sources[0],
            Some(planned.src_node_id.as_ref())
        ));

        route
            .node_replicas
            .write()
            .get_mut(&planned.src_node_id)
            .unwrap()
            .memory
            .as_mut()
            .unwrap()
            .owner_local_indexed = false;
        let sources = snapshot_live_get_sources(&route);
        assert!(planned_source_requester_local_borrow_eligible(
            &sources[0],
            Some(planned.src_node_id.as_ref())
        ));

        let allocator = Arc::new(
            crate::master_seg_manager::one_seg_allocator::OneSegAllocator::new(
                "borrow-source".to_string(),
                crate::master_seg_manager::msg_pack::SegmentDeviceDescription::Cpu,
                planned.src_base_addr,
                16 * 1024,
            )
            .unwrap(),
        );
        let allocation = Arc::new(allocator.allocate(planned.len).unwrap());
        route
            .node_replicas
            .write()
            .get_mut(&planned.src_node_id)
            .unwrap()
            .memory
            .as_mut()
            .unwrap()
            .backing = KvReplicaBacking::Allocation(allocation);
        let sources = snapshot_live_get_sources(&route);
        assert!(sources[0].memory_is_allocation);
        assert!(planned_source_requester_local_borrow_eligible(
            &sources[0],
            Some(planned.src_node_id.as_ref())
        ));
        assert!(!planned_source_requester_local_borrow_eligible(
            &sources[0],
            Some("another-owner")
        ));
    }

    #[test]
    fn global_shared_committed_slot_promotes_without_replacing_its_backing() {
        let (planned, route, tomb_tag) = planned_route();
        let expected = {
            let mut replicas = route.node_replicas.write();
            let memory = replicas
                .get_mut(&planned.src_node_id)
                .unwrap()
                .memory
                .as_mut()
                .unwrap();
            memory.owner_local_indexed = false;
            match &memory.backing {
                KvReplicaBacking::CommittedSlot(slot) => slot.clone(),
                KvReplicaBacking::Allocation(_) => unreachable!(),
            }
        };

        let before_addr = expected.addr;
        let before_allocation_id = expected.allocation_id;
        let desc = promote_global_shared_committed_slot(
            &route,
            &planned.src_node_id,
            &tomb_tag,
            &expected,
        )
        .expect("exact GlobalShared slot must promote");
        assert_eq!(desc.put_id, planned.put_id);
        assert_eq!(desc.weight_bytes, expected.capacity_bytes as u32);

        let replicas = route.node_replicas.read();
        let memory = replicas[&planned.src_node_id].memory.as_ref().unwrap();
        assert!(memory.owner_local_indexed);
        match &memory.backing {
            KvReplicaBacking::CommittedSlot(slot) => {
                assert_eq!(slot.addr, before_addr);
                assert_eq!(slot.allocation_id, before_allocation_id);
            }
            KvReplicaBacking::Allocation(_) => unreachable!(),
        }
        drop(replicas);
        assert!(
            promote_global_shared_committed_slot(
                &route,
                &planned.src_node_id,
                &tomb_tag,
                &expected,
            )
            .is_none(),
            "promotion replay must not reclassify an already LocalExclusive slot"
        );
    }

    #[test]
    fn stale_remote_plan_rebinds_to_current_requester_global_shared_slot() {
        let (planned, route, _) = planned_route();
        let requester: NodeID = "requester-owner".to_string().into();
        let requester_tag = NodeTombTag::new();
        let requester_slot = CommittedSlotReplica {
            owner: crate::owner_segment::OwnerGeneration::new(requester.as_ref().to_string(), 23),
            allocation_id: 22,
            segment_offset: 4 * 8192,
            capacity_bytes: 8192,
            addr: 0x9000,
            len: planned.len,
            base_addr: 0x1000,
            segment_registration_epoch: 2,
        };
        route.node_replicas.write().insert(
            requester.clone(),
            KvNodeReplicas::memory(
                requester_tag.clone(),
                KvMemoryReplica {
                    backing: KvReplicaBacking::CommittedSlot(requester_slot.clone()),
                    owner_local_indexed: false,
                    get_durable_reservation: None,
                    capacity_reservation: None,
                },
            ),
        );

        let (target, tomb_tag, mode, rebound) =
            planned_get_requester_local_target_from_route(&planned, &route, &requester, 41)
                .expect("the same put generation must late-bind to requester-local GlobalShared");
        assert!(tomb_tag.same_generation(&requester_tag));
        assert_eq!(
            mode,
            crate::master_kv_router::msg_pack::GetAllocationMode::RequesterLocalPromote
        );
        let super::InflightGetTarget::ReusedCommittedSlot(target_slot) = target else {
            panic!("late rebind must reuse the existing committed owner slot")
        };
        assert_eq!(target_slot, requester_slot);
        assert_ne!(planned.src_node_id, requester);
        assert_eq!(rebound.src_node_id, requester);
        assert_eq!(rebound.src_addr, requester_slot.addr);
        assert_eq!(rebound.src_base_addr, requester_slot.base_addr);
        assert_eq!(rebound.source_kind, GetSourceKind::Memory);
        let token = rebound
            .source_route_token
            .as_ref()
            .expect("owner GlobalShared rebind must carry an exact source token");
        assert_eq!(token.source, requester_slot);
        assert_eq!(token.plan_nonce, 42);
        assert!(planned_get_source_is_current(&rebound, &route));
    }

    #[test]
    fn ssd_read_source_policies_have_distinct_explicit_orders() {
        let order = |policy| {
            let classes = vec![
                (GetSourceKind::Memory, false, "remote_memory"),
                (GetSourceKind::Memory, true, "local_memory"),
                (GetSourceKind::Ssd, false, "remote_ssd"),
                (GetSourceKind::Ssd, true, "local_ssd"),
            ];
            let mut classes = classes
                .into_iter()
                .filter_map(|(kind, local, label)| {
                    planned_get_source_rank(policy, kind, local).map(|rank| (rank, label))
                })
                .collect::<Vec<_>>();
            classes.sort_by_key(|(rank, _)| *rank);
            classes
                .into_iter()
                .map(|(_, label)| label)
                .collect::<Vec<_>>()
        };

        assert_eq!(
            order(SsdReadSourcePolicy::LegacyRemoteFirst),
            ["remote_memory", "local_memory", "remote_ssd", "local_ssd"]
        );
        assert_eq!(
            order(SsdReadSourcePolicy::LocalSsdOnlyFirst),
            ["local_memory", "local_ssd", "remote_memory"]
        );
        assert_eq!(
            planned_get_source_rank(
                SsdReadSourcePolicy::LocalSsdOnlyFirst,
                GetSourceKind::Ssd,
                false,
            ),
            None,
            "remote SSD must not be a fallback for the local-only policy"
        );
    }

    #[test]
    fn completed_done_replay_skips_late_bind_but_preserves_pending_target() {
        let item = BatchGetDoneItemReq {
            get_id: 9,
            late_target: Some(GetBindTarget::ExternalSink(GetExternalSinkTarget {
                addr: 0x1000,
                capacity: 4096,
                registration_id: 3,
                requester_node_start_time: 17,
            })),
        };
        assert!(matches!(
            late_bind_target_for_done(&item, false),
            Some(GetBindTarget::ExternalSink(target)) if target.registration_id == 3
        ));
        assert!(late_bind_target_for_done(&item, true).is_none());

        let pre_bound = BatchGetDoneItemReq {
            get_id: 10,
            late_target: None,
        };
        assert!(late_bind_target_for_done(&pre_bound, false).is_none());
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
    let source_policy = view
        .master_kv_router()
        .inner()
        .test_spec_config
        .ssd_read_source_policy;
    let mut filtered_remote_ssd_items = 0u64;
    let mut filtered_remote_ssd_bytes = 0u64;
    let mut candidates = snapshot_live_get_sources(&route)
        .into_iter()
        .filter_map(|source| {
            let requester_local = local_owner
                .as_deref()
                .is_some_and(|owner| source.node_id.as_ref() == owner);
            match planned_get_source_rank(source_policy, source.source_kind, requester_local) {
                Some(rank) => Some((source, rank)),
                None => {
                    filtered_remote_ssd_items = filtered_remote_ssd_items.saturating_add(1);
                    filtered_remote_ssd_bytes =
                        filtered_remote_ssd_bytes.saturating_add(source.len);
                    None
                }
            }
        })
        .collect::<Vec<_>>();
    if filtered_remote_ssd_items != 0 {
        view.master_kv_router()
            .inner()
            .planned_get_counters
            .remote_ssd_filtered_items
            .fetch_add(filtered_remote_ssd_items, Ordering::Relaxed);
        view.master_kv_router()
            .inner()
            .planned_get_counters
            .remote_ssd_filtered_bytes
            .fetch_add(filtered_remote_ssd_bytes, Ordering::Relaxed);
    }
    candidates.shuffle(&mut rand::thread_rng());
    candidates.sort_by_key(|(_, rank)| *rank);
    let has_remote_memory_alternative = candidates.iter().any(|(candidate, _)| {
        candidate.source_kind == GetSourceKind::Memory
            && local_owner
                .as_deref()
                .is_none_or(|owner| candidate.node_id.as_ref() != owner)
    });
    let Some((source, _)) = candidates.into_iter().next() else {
        view.master_kv_router()
            .inner()
            .planned_get_counters
            .plan_misses
            .fetch_add(1, Ordering::Relaxed);
        return get_plan_item_error(&msg_and_error::KvError::Api(
            msg_and_error::ApiError::KeyNotFound { key },
        ));
    };
    let selected_requester_local_ssd = source.source_kind == GetSourceKind::Ssd
        && local_owner
            .as_deref()
            .is_some_and(|owner| source.node_id.as_ref() == owner);
    if selected_requester_local_ssd {
        let counters = &view.master_kv_router().inner().ssd_tier_counters;
        let (items, bytes) = if has_remote_memory_alternative {
            (
                &counters.local_ssd_selected_with_remote_memory_items,
                &counters.local_ssd_selected_with_remote_memory_bytes,
            )
        } else {
            (
                &counters.local_ssd_selected_without_remote_memory_items,
                &counters.local_ssd_selected_without_remote_memory_bytes,
            )
        };
        items.fetch_add(1, Ordering::Relaxed);
        bytes.fetch_add(source.len, Ordering::Relaxed);
    }

    let get_id = view
        .master_kv_router()
        .inner()
        .next_get_id
        .fetch_add(1, Ordering::Relaxed);
    let gpu_direct_eligible = source.source_kind == GetSourceKind::Memory
        && source.owner_slot.is_some()
        && local_owner
            .as_deref()
            .is_none_or(|owner| source.node_id.as_ref() != owner);
    let requester_local_borrow_eligible =
        planned_source_requester_local_borrow_eligible(&source, local_owner.as_deref());
    let atomic_group = route.atomic_group.as_deref().cloned();
    let source_route_token = owner_source_route_token(
        &key,
        route.put_id,
        atomic_group.clone(),
        get_id,
        source.owner_slot.as_ref(),
    );
    let ssd_source_route_token = (source.source_kind == GetSourceKind::Ssd)
        .then(|| {
            owner_ssd_source_route_token(
                &view,
                &source.node_id,
                &key,
                route.put_id,
                source.len,
                atomic_group.clone(),
                get_id,
            )
        })
        .flatten();
    if source.source_kind == GetSourceKind::Ssd && ssd_source_route_token.is_none() {
        return get_plan_item_error(&msg_and_error::KvError::Api(
            msg_and_error::ApiError::NodeNotFound {
                desc: source.node_id.to_string(),
            },
        ));
    }
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
        source_kind: source.source_kind,
        source_route_token: source_route_token.clone(),
        ssd_source_route_token: ssd_source_route_token.clone(),
        atomic_group,
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
        source_kind: planned.source_kind,
        source_route_token,
        ssd_source_route_token,
        atomic_group: planned.atomic_group,
        gpu_direct_eligible,
        requester_local_borrow_eligible,
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
            expected.allocation_id == actual.allocation_id
                && expected.segment_offset == actual.segment_offset
                && expected.capacity_bytes == actual.capacity_bytes
                && expected.addr == actual.addr
                && expected.base_addr == actual.base_addr
        }
        (GetBindTarget::RequesterLocalSource, InflightGetTarget::Allocation(actual)) => {
            info.allocation_mode == GetAllocationMode::RequesterLocalBorrow
                && info.source_kind == GetSourceKind::Memory
                && info.src_node_id == info.req_node_id
                && info.src_addr == actual.base_addr() + actual.addr()
                && info.src_base_addr == actual.base_addr()
        }
        (GetBindTarget::RequesterLocalSource, InflightGetTarget::ReusedCommittedSlot(actual)) => {
            info.allocation_mode == GetAllocationMode::RequesterLocalPromote
                && info.source_kind == GetSourceKind::Memory
                && info.src_node_id == info.req_node_id
                && info.src_addr == actual.addr
                && info.src_base_addr == actual.base_addr
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
        source_kind: info.source_kind,
        source_route_token: info.source_route_token.clone(),
        ssd_source_route_token: info.ssd_source_route_token.clone(),
        prepared_target: match &info.target {
            InflightGetTarget::PreparedLocalReserveSlot(slot) => Some(slot.clone()),
            _ => None,
        },
        reused_committed_slot: match &info.target {
            InflightGetTarget::ReusedCommittedSlot(slot) => Some(slot.clone()),
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
    let mut bound_planned = planned.clone();
    let (target, target_tomb_tag, allocation_mode, prepared_requester_lease) = match &request.target
    {
        GetBindTarget::ExternalSink(target) => {
            if planned.source_kind == GetSourceKind::Ssd {
                return get_bind_item_error(
                    get_id,
                    &msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
                        detail: format!(
                            "SSD Get source requires owner-local CPU staging before an external GPU sink: get_id={get_id}"
                        ),
                    }),
                );
            }
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
        GetBindTarget::RequesterLocalSource => {
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
                            "requester-local source executor is not the controller's owner: get_id={} controller={} expected_owner={:?} got={}",
                            get_id, planned.controller_node_id, expected_owner, req_node_id
                        ),
                    }),
                );
            }
            let (target, tomb_tag, allocation_mode, rebound) =
                match planned_get_requester_local_target(&view, &planned, &req_node_id, get_id) {
                    Ok(value) => value,
                    Err(err) => return get_bind_item_error(get_id, &err),
                };
            if rebound.src_node_id != planned.src_node_id
                || rebound.src_addr != planned.src_addr
                || rebound.src_base_addr != planned.src_base_addr
                || rebound.source_kind != planned.source_kind
            {
                tracing::info!(
                    get_id,
                    key = %planned.key,
                    planned_source = %planned.src_node_id,
                    rebound_source = %rebound.src_node_id,
                    "GetBind rebound a stale Plan to the current requester-local backing"
                );
            }
            bound_planned = rebound;
            (target, Some(tomb_tag), allocation_mode, None)
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
        .get(&bound_planned.key)
        .map(|route| route.clone());
    let Some(current_route) =
        current_route.filter(|route| planned_get_source_is_current(&bound_planned, route))
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
                key: bound_planned.key.clone(),
                detail: format!(
                    "source route changed before Bind: source={}",
                    bound_planned.src_node_id
                ),
            }),
        );
    };
    if matches!(&request.target, GetBindTarget::PreparedLocalReserve(_))
        && current_route
            .node_replicas
            .read()
            .get(&req_node_id)
            .is_some_and(|replica| !replica.tomb_tag.is_tomb() && replica.memory.is_some())
    {
        return get_bind_item_error(
            get_id,
            &msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
                detail: format!(
                    "prepared GetBind cannot replace a live owner replica: get_id={} key={} owner={}",
                    get_id, bound_planned.key, req_node_id
                ),
            }),
        );
    }

    let (src_addr, src_base_addr) = match bound_planned.source_kind {
        GetSourceKind::Memory => (bound_planned.src_addr, bound_planned.src_base_addr),
        // The SSD owner claims transient DRAM inside GetToTarget. Master must
        // not allocate or publish a physical staging address.
        GetSourceKind::Ssd => (0, 0),
    };

    let Some(removed_planned) = view
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
    if removed_planned.put_id != planned.put_id || removed_planned.key != planned.key {
        return get_bind_item_error(
            get_id,
            &msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
                detail: format!("planned Get identity changed during Bind: get_id={get_id}"),
            }),
        );
    }
    let touch_source_kind = bound_planned.source_kind;
    let touch_key = bound_planned.key.clone();
    let inflight = InflightGetInfo {
        put_id: bound_planned.put_id,
        src_node_id: bound_planned.src_node_id.clone(),
        key: bound_planned.key.clone(),
        req_node_id: req_node_id.clone(),
        controller_node_id: Some(bound_planned.controller_node_id),
        len: bound_planned.len,
        src_addr,
        src_base_addr,
        source_kind: bound_planned.source_kind,
        source_route_token: bound_planned.source_route_token,
        ssd_source_route_token: bound_planned.ssd_source_route_token,
        ssd_stage_lifecycle: (bound_planned.source_kind == GetSourceKind::Ssd)
            .then(|| Arc::new(super::SsdStageLifecycle::new())),
        atomic_group: bound_planned.atomic_group,
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
        inflight.source_kind,
        inflight.src_node_id == inflight.req_node_id,
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
    if current_route.lease_id.is_none() && touch_source_kind == GetSourceKind::Memory {
        touch_moka_for_node(view, response.node_id.to_string(), touch_key);
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
                if one_kv_nodes_routes.node_replicas.read().is_empty() {
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

    let replicas: HashMap<NodeID, KvNodeReplicas> =
        one_kv_nodes_routes.node_replicas.read().clone();
    let prepared_target = req.serialize_part.prepared_target.clone();
    let external_sink_target = req.serialize_part.external_sink_target.clone();
    let external_sink_local_owner = external_sink_target.as_ref().and_then(|target| {
        external_sink_local_owner_id(&view, &req_node_id, target.requester_node_start_time)
    });
    let mut tombs = replicas
        .iter()
        .filter_map(|(node_id, replicas)| replicas.tomb_tag.is_tomb().then_some(node_id.clone()))
        .collect::<HashSet<_>>();
    let mut memory_sources = replicas
        .iter()
        .filter_map(|(node_id, replicas)| {
            (!replicas.tomb_tag.is_tomb() && replicas.memory.is_some())
                .then_some((node_id.clone(), GetSourceKind::Memory))
        })
        .collect::<Vec<_>>();
    let mut ssd_sources = replicas
        .iter()
        .filter_map(|(node_id, replicas)| {
            (!replicas.tomb_tag.is_tomb() && replicas.memory.is_none() && replicas.ssd.is_some())
                .then_some((node_id.clone(), GetSourceKind::Ssd))
        })
        .collect::<Vec<_>>();
    memory_sources.shuffle(&mut rand::thread_rng());
    // A GlobalShared slot already owned by the requester is the exact target
    // allocation. Prefer it over every remote replica so the legacy Start
    // path cannot turn a metadata-only scope promotion into a copy.
    if let Some(requester_slot_pos) = memory_sources.iter().position(|(node_id, kind)| {
        *kind == GetSourceKind::Memory
            && node_id == &req_node_id
            && replicas.get(node_id).is_some_and(|replicas| {
                replicas.memory.as_ref().is_some_and(|memory| {
                    !memory.owner_local_indexed
                        && matches!(
                            &memory.backing,
                            super::KvReplicaBacking::CommittedSlot(slot)
                            if slot.owner.node_id.as_str() == req_node_id.as_ref()
                        )
                })
            })
    }) {
        let requester_slot = memory_sources.remove(requester_slot_pos);
        memory_sources.insert(0, requester_slot);
    }
    ssd_sources.shuffle(&mut rand::thread_rng());
    memory_sources.extend(ssd_sources);
    let mut target = None;
    let mut allocation_mode = GetAllocationMode::Temporary;
    let mut durable_reservation = None;
    for (selected_replica_key, source_kind) in memory_sources {
        let selected_replicas = replicas
            .get(&selected_replica_key)
            .expect("selected Get source must exist");
        if external_sink_local_owner
            .as_deref()
            .is_some_and(|owner_id| selected_replica_key.as_ref() == owner_id)
        {
            // The explicit GPU path is RDMA-only. The requester's share-group
            // owner is local IPC/P2P topology, whose fallback would require
            // CPU access to a CUDA virtual address. Leave that replica for the
            // ordinary CPU-buffered Get path and search for a remote route.
            continue;
        }
        if source_kind == GetSourceKind::Ssd && external_sink_target.is_some() {
            continue;
        }
        let src_node_id = selected_replica_key;
        let (src_len, src_abs_addr, src_base, selected_owner_slot) = match source_kind {
            GetSourceKind::Memory => {
                let selected_replica = selected_replicas
                    .memory
                    .as_ref()
                    .expect("memory source candidate must retain memory");
                let owner_slot = match &selected_replica.backing {
                    super::KvReplicaBacking::CommittedSlot(slot) => Some(slot.clone()),
                    super::KvReplicaBacking::Allocation(_) => None,
                };
                (
                    selected_replica.backing.len(),
                    selected_replica.backing.abs_addr(),
                    selected_replica.backing.base_addr(),
                    owner_slot,
                )
            }
            GetSourceKind::Ssd => {
                let ssd = selected_replicas
                    .ssd
                    .as_ref()
                    .expect("SSD source candidate must retain SSD");
                (ssd.len, 0, 0, None)
            }
        };

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
                        let capacity = target_allocator.node_pool_capacity_snapshot();
                        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::NoSpace {
                            node: req_node_id.as_ref().to_string(),
                            segment: target_allocator.seg_device_id.clone(),
                            total_capacity: capacity.active_capacity_bytes,
                            free_capacity: capacity.available_capacity_bytes,
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
                    // Local probing and BatchGetStart are intentionally not one
                    // distributed critical section. A concurrent Put or Get may
                    // publish this generation on the requester after the probe
                    // but before the master consumes the newly claimed target.
                    if let Some(replica_on_recv_node) = replicas
                        .get(&req_node_id)
                        .filter(|replicas| !replicas.tomb_tag.is_tomb())
                        .and_then(|replicas| replicas.memory.as_ref())
                    {
                        match &replica_on_recv_node.backing {
                            super::KvReplicaBacking::Allocation(allocation) => {
                                allocation_mode = GetAllocationMode::ReuseReplica;
                                InflightGetTarget::Allocation(allocation.clone())
                            }
                            super::KvReplicaBacking::CommittedSlot(slot)
                                if !replica_on_recv_node.owner_local_indexed
                                    && slot.owner.node_id.as_str() == req_node_id.as_ref() =>
                            {
                                allocation_mode = GetAllocationMode::RequesterLocalPromote;
                                InflightGetTarget::ReusedCommittedSlot(slot.clone())
                            }
                            super::KvReplicaBacking::CommittedSlot(_) => {
                                let err = msg_and_error::KvError::Api(
                                    msg_and_error::ApiError::KeyAlreadyExists {
                                        key: req.serialize_part.key.clone(),
                                    },
                                );
                                return failed_resp_err(
                                    err,
                                    Some((tombs.clone(), one_kv_nodes_routes.put_id)),
                                    &view,
                                    &req.serialize_part.key,
                                );
                            }
                        }
                    } else {
                        let (slot, _prepared_tomb_tag) =
                            match validate_prepared_local_reserve_target(
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
                    }
                } else if let Some(replica_on_recv_node) = replicas
                    .get(&req_node_id)
                    .filter(|replicas| !replicas.tomb_tag.is_tomb())
                    .and_then(|replicas| replicas.memory.as_ref())
                {
                    match &replica_on_recv_node.backing {
                        super::KvReplicaBacking::Allocation(allocation) => {
                            allocation_mode = GetAllocationMode::ReuseReplica;
                            InflightGetTarget::Allocation(allocation.clone())
                        }
                        super::KvReplicaBacking::CommittedSlot(slot)
                            if !replica_on_recv_node.owner_local_indexed
                                && slot.owner.node_id.as_str() == req_node_id.as_ref() =>
                        {
                            allocation_mode = GetAllocationMode::RequesterLocalPromote;
                            InflightGetTarget::ReusedCommittedSlot(slot.clone())
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
        // allocator/segment generation. Looking up only by node id at GetDone would allow
        // an old completion to publish addresses into a reconnected node.
        let target_tomb_tag = match &target {
            InflightGetTarget::Allocation(allocation) => view
                .master_seg_manager()
                .get_allocation_tomb_tag(&req_node_id, allocation),
            InflightGetTarget::PreparedLocalReserveSlot(slot) => {
                view.master_seg_manager().validate_owner_slot_geometry(
                    &req_node_id,
                    slot.allocation_id,
                    slot.segment_offset,
                    slot.capacity_bytes,
                    slot.base_addr,
                    slot.addr,
                )
            }
            InflightGetTarget::ReusedCommittedSlot(slot) => {
                view.master_seg_manager().validate_owner_slot_geometry(
                    &req_node_id,
                    slot.allocation_id,
                    slot.segment_offset,
                    slot.capacity_bytes,
                    slot.base_addr,
                    slot.addr,
                )
            }
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
        let (resp_node_id, resp_src_addr, resp_target_addr, resp_src_base, resp_target_base) = if matches!(
            allocation_mode,
            GetAllocationMode::ReuseReplica | GetAllocationMode::RequesterLocalPromote
        ) {
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

        let actual_owner_source = match (&target, allocation_mode) {
            (
                InflightGetTarget::ReusedCommittedSlot(slot),
                GetAllocationMode::RequesterLocalPromote,
            ) => Some(slot),
            (_, GetAllocationMode::ReuseReplica) => None,
            _ => selected_owner_slot.as_ref(),
        };
        let source_route_token = owner_source_route_token(
            &req.serialize_part.key,
            one_kv_nodes_routes.put_id,
            one_kv_nodes_routes.atomic_group.as_deref().cloned(),
            get_id,
            actual_owner_source,
        );
        let ssd_source_route_token = (source_kind == GetSourceKind::Ssd)
            .then(|| {
                owner_ssd_source_route_token(
                    &view,
                    &src_node_id,
                    &req.serialize_part.key,
                    one_kv_nodes_routes.put_id,
                    src_len,
                    one_kv_nodes_routes.atomic_group.as_deref().cloned(),
                    get_id,
                )
            })
            .flatten();
        if source_kind == GetSourceKind::Ssd && ssd_source_route_token.is_none() {
            continue;
        }

        let resp = GetStartResp {
            put_id: one_kv_nodes_routes.put_id,
            get_id,
            node_id: resp_node_id.clone().into(),
            src_addr: resp_src_addr,
            target_addr: resp_target_addr,
            src_base_addr: resp_src_base,
            target_base_addr: resp_target_base,
            len: src_len,
            source_kind,
            source_route_token: source_route_token.clone(),
            ssd_source_route_token: ssd_source_route_token.clone(),
            prepared_target: (allocation_mode == GetAllocationMode::LocalCommittedSlot)
                .then(|| prepared_target.clone())
                .flatten(),
            reused_committed_slot: match &target {
                InflightGetTarget::ReusedCommittedSlot(slot) => Some(slot.clone()),
                _ => None,
            },
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
            source_kind,
            src_node_id == req_node_id,
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
            source_kind,
            source_route_token,
            ssd_source_route_token,
            ssd_stage_lifecycle: (source_kind == GetSourceKind::Ssd)
                .then(|| Arc::new(super::SsdStageLifecycle::new())),
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
        if one_kv_nodes_routes.lease_id.is_none() && source_kind == GetSourceKind::Memory {
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

pub async fn handle_ssd_stage_begin(
    view: MasterKvRouterView,
    req: MsgPack<SsdStageBeginReq>,
    req_node_id: NodeID,
) -> MsgPack<SsdStageBeginResp> {
    let get_id = req.serialize_part.get_id;
    let operation_lock = view
        .master_kv_router()
        .inner()
        .get_done_locks
        .get_lock(get_id);
    let _operation_guard = operation_lock.lock().await;
    let result = view
        .master_kv_router()
        .inner()
        .inflight_gets
        .get(&get_id)
        .await
        .and_then(|inflight| {
            (inflight.source_kind == GetSourceKind::Ssd
                && inflight.src_node_id == req_node_id
                && inflight.ssd_source_route_token.is_some())
            .then(|| inflight.ssd_stage_lifecycle.as_ref().cloned())
            .flatten()
        })
        .is_some_and(|lifecycle| lifecycle.begin());
    MsgPack {
        serialize_part: SsdStageBeginResp {
            started: result,
            error_code: msg_and_error::OK,
            error_json: String::new(),
        },
        raw_bytes: Vec::new(),
    }
}

pub async fn handle_ssd_stage_done(
    view: MasterKvRouterView,
    req: MsgPack<SsdStageDoneReq>,
    req_node_id: NodeID,
) -> MsgPack<SsdStageDoneResp> {
    let get_id = req.serialize_part.get_id;
    let operation_lock = view
        .master_kv_router()
        .inner()
        .get_done_locks
        .get_lock(get_id);
    let _operation_guard = operation_lock.lock().await;
    let Some(inflight) = view
        .master_kv_router()
        .inner()
        .inflight_gets
        .get(&get_id)
        .await
    else {
        return MsgPack {
            serialize_part: SsdStageDoneResp {
                error_code: msg_and_error::OK,
                error_json: String::new(),
            },
            raw_bytes: Vec::new(),
        };
    };
    if inflight.source_kind != GetSourceKind::Ssd
        || !ssd_stage_done_request_authorized(
            &inflight.src_node_id,
            &inflight.req_node_id,
            &req_node_id,
            req.serialize_part.drop_ssd_source,
        )
    {
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
            detail: format!(
                "SSD stage terminal requester mismatch: get_id={} source={} target={} got={} drop_ssd_source={}",
                get_id,
                inflight.src_node_id,
                inflight.req_node_id,
                req_node_id,
                req.serialize_part.drop_ssd_source,
            ),
        });
        return MsgPack {
            serialize_part: SsdStageDoneResp {
                error_code: err.code(),
                error_json: err.to_json(),
            },
            raw_bytes: Vec::new(),
        };
    }
    if let Some(lifecycle) = inflight.ssd_stage_lifecycle.as_ref() {
        lifecycle.finish();
    }

    let mut removed_empty_route = None;
    if req.serialize_part.drop_ssd_source {
        let route = inflight.route.clone();
        if route.put_id == inflight.put_id && route.remove_ssd_replica(&inflight.src_node_id) {
            let key = inflight.key.clone();
            if route.node_replicas.read().is_empty()
                && view
                    .master_kv_router()
                    .inner()
                    .kv_routes
                    .remove_if(&key, |_, current| {
                        Arc::ptr_eq(current, &route)
                            && current.put_id == inflight.put_id
                            && current.node_replicas.read().is_empty()
                    })
                    .is_some()
            {
                removed_empty_route = Some((key, inflight.put_id));
            }
        }
    }
    drop(inflight);
    if let Some((key, put_id)) = removed_empty_route
        && view.master_kv_router().prefix_index_enabled()
    {
        view.master_kv_router()
            .inner()
            .prefix_index
            .write()
            .await
            .remove(&key, put_id);
    }

    MsgPack {
        serialize_part: SsdStageDoneResp {
            error_code: msg_and_error::OK,
            error_json: String::new(),
        },
        raw_bytes: Vec::new(),
    }
}

fn ssd_stage_done_request_authorized(
    source_node_id: &NodeID,
    target_node_id: &NodeID,
    requester_node_id: &NodeID,
    drop_ssd_source: bool,
) -> bool {
    requester_node_id == source_node_id || (!drop_ssd_source && requester_node_id == target_node_id)
}

#[cfg(test)]
mod ssd_stage_done_authorization_tests {
    use super::ssd_stage_done_request_authorized;
    use crate::cluster_manager::NodeID;

    #[test]
    fn source_may_close_or_drop_but_target_may_only_close() {
        let source: NodeID = "cpu-source".to_string().into();
        let target: NodeID = "gpu-target".to_string().into();
        let stranger: NodeID = "other-owner".to_string().into();

        assert!(ssd_stage_done_request_authorized(
            &source, &target, &source, false
        ));
        assert!(ssd_stage_done_request_authorized(
            &source, &target, &source, true
        ));
        assert!(ssd_stage_done_request_authorized(
            &source, &target, &target, false
        ));
        assert!(!ssd_stage_done_request_authorized(
            &source, &target, &target, true
        ));
        assert!(!ssd_stage_done_request_authorized(
            &source, &target, &stranger, false
        ));
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
    let _done_guard = loop {
        let guard = done_lock.lock().await;
        let active_lifecycle = view
            .master_kv_router()
            .inner()
            .inflight_gets
            .get(&get_id)
            .await
            .and_then(|inflight| {
                inflight
                    .ssd_stage_lifecycle
                    .as_ref()
                    .filter(|lifecycle| lifecycle.is_active())
                    .cloned()
            });
        let Some(lifecycle) = active_lifecycle else {
            break guard;
        };
        drop(guard);
        lifecycle.wait_until_not_active().await;
    };

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
    mut deferred_post_read_reclaims: Option<&mut Vec<PostReadRemoteReclaimCandidate>>,
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
        let key = inflight_info.key.clone();
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
            InflightGetTarget::ReusedCommittedSlot(_) => 0,
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
                            let replica = KvMemoryReplica {
                                backing: super::KvReplicaBacking::Allocation(match &inflight_info
                                    .target
                                {
                                    InflightGetTarget::Allocation(allocation) => allocation.clone(),
                                    InflightGetTarget::PreparedLocalReserveSlot(_) => {
                                        unreachable!("durable Get mode must use Allocation target")
                                    }
                                    InflightGetTarget::ReusedCommittedSlot(_) => {
                                        unreachable!("durable Get mode cannot reuse an owner slot")
                                    }
                                    InflightGetTarget::ExternalSink(_) => {
                                        unreachable!("durable Get mode cannot use external sink")
                                    }
                                }),
                                owner_local_indexed: true,
                                get_durable_reservation: inflight_info.durable_reservation.clone(),
                                capacity_reservation,
                            };
                            if publish_route_replica_tomb_fenced(
                                &one_kv_nodes_routes,
                                req_node_id.clone(),
                                replica,
                                target_tomb_tag.clone(),
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
                            let mut replicas = current_route.node_replicas.write();
                            if let Some(node_replicas) = replicas.get_mut(&req_node_id) {
                                local_index_published = !node_replicas.tomb_tag.is_tomb()
                                    && node_replicas.tomb_tag.same_generation(target_tomb_tag)
                                    && node_replicas.memory.as_ref().is_some_and(|replica| {
                                        matches!(
                                            &replica.backing,
                                            super::KvReplicaBacking::Allocation(allocation)
                                                if matches!(
                                                    &inflight_info.target,
                                                    InflightGetTarget::Allocation(target)
                                                        if Arc::ptr_eq(allocation, target)
                                                )
                                        )
                                    });
                                if local_index_published {
                                    let replica = node_replicas
                                        .memory
                                        .as_mut()
                                        .expect("matched live memory replica");
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
        } else if allocation_mode == GetAllocationMode::RequesterLocalBorrow {
            let borrow_is_current =
                view.master_kv_router()
                    .inner()
                    .kv_routes
                    .get(&key)
                    .is_some_and(|route| {
                        route.put_id == inflight_info.put_id
                            && route.node_replicas.read().get(&req_node_id).is_some_and(
                                |replicas| {
                                    !replicas.tomb_tag.is_tomb()
                                        && replicas.tomb_tag.same_generation(target_tomb_tag)
                                        && replicas.memory.as_ref().is_some_and(|replica| {
                                            matches!(
                                                (&replica.backing, &inflight_info.target),
                                                (
                                                    super::KvReplicaBacking::Allocation(source),
                                                    InflightGetTarget::Allocation(target)
                                                ) if Arc::ptr_eq(source, target)
                                            )
                                        })
                                },
                            )
                    });
            if !borrow_is_current {
                tracing::warn!(
                    "Requester-local borrowed allocation is no longer the current route; returning a temporary holder: get_id={} key={} put_id=({},{}) owner={}",
                    get_id,
                    key,
                    inflight_info.put_id.0,
                    inflight_info.put_id.1,
                    req_node_id
                );
                allocation_mode = GetAllocationMode::Temporary;
            }
        } else if allocation_mode == GetAllocationMode::RequesterLocalPromote {
            let slot = match &inflight_info.target {
                InflightGetTarget::ReusedCommittedSlot(slot) => slot.clone(),
                InflightGetTarget::Allocation(_)
                | InflightGetTarget::PreparedLocalReserveSlot(_)
                | InflightGetTarget::ExternalSink(_) => {
                    unreachable!("requester-local promote must reuse an existing owner slot")
                }
            };
            let promoted_desc = view
                .master_kv_router()
                .inner()
                .kv_routes
                .get(&key)
                .filter(|route| route.put_id == inflight_info.put_id)
                .and_then(|route| {
                    promote_global_shared_committed_slot(
                        &route,
                        &req_node_id,
                        target_tomb_tag,
                        &slot,
                    )
                });
            let Some(old_ring_b_desc) = promoted_desc else {
                let err = msg_and_error::KvError::Api(msg_and_error::ApiError::Unknown {
                    detail: format!(
                        "GlobalShared owner slot could not be promoted metadata-only: get_id={} key={} put_id=({},{}) owner={} allocation_id={} offset={} capacity={}",
                        get_id,
                        key,
                        inflight_info.put_id.0,
                        inflight_info.put_id.1,
                        req_node_id,
                        slot.allocation_id,
                        slot.segment_offset,
                        slot.capacity_bytes,
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
            let _ = view
                .master_kv_router()
                .remove_node_cache_entry_exact(req_node_id.as_ref(), &key, &old_ring_b_desc)
                .await;
        } else if allocation_mode == GetAllocationMode::LocalCommittedSlot {
            let slot = match &inflight_info.target {
                InflightGetTarget::PreparedLocalReserveSlot(slot) => slot.clone(),
                InflightGetTarget::Allocation(_) => {
                    unreachable!("local committed-slot Get mode must use a prepared slot")
                }
                InflightGetTarget::ReusedCommittedSlot(_) => {
                    unreachable!("new local committed-slot mode cannot reuse an existing slot")
                }
                InflightGetTarget::ExternalSink(_) => {
                    unreachable!("local committed-slot Get mode cannot use external sink")
                }
            };
            let mut published = false;
            let mut route_publish_event = None;
            if let Some(current_route) = view.master_kv_router().inner().kv_routes.get(&key) {
                if current_route.put_id == inflight_info.put_id {
                    let replica = KvMemoryReplica {
                        backing: super::KvReplicaBacking::CommittedSlot(slot),
                        owner_local_indexed: true,
                        get_durable_reservation: None,
                        capacity_reservation: None,
                    };
                    if publish_route_replica_tomb_fenced(
                        &current_route,
                        req_node_id.clone(),
                        replica,
                        target_tomb_tag.clone(),
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
                GetAllocationMode::RequesterLocalBorrow => "requester_local_borrow",
                GetAllocationMode::RequesterLocalPromote => "requester_local_promote",
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
        let post_read_reclaim = view
            .master_kv_router()
            .post_read_remote_reclaim_candidate(&inflight_info, allocation_mode);
        if let Some(terminals) = deferred_terminals.as_deref_mut() {
            terminals.push((get_id, completed));
        } else {
            view.master_kv_router()
                .inner()
                .completed_gets
                .insert(get_id, completed)
                .await;
        }
        drop(_activity_completion);
        if let Some(candidate) = post_read_reclaim {
            if let Some(reclaims) = deferred_post_read_reclaims.as_deref_mut() {
                reclaims.push(candidate);
            } else {
                let _ = view
                    .master_kv_router()
                    .enqueue_post_read_remote_reclaim(candidate);
            }
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
    handle_get_done_locked(view, req, req_node_id, None, None, None).await
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
        let node_replicas: HashMap<NodeID, KvNodeReplicas> =
            (*one_kv_nodes_routes.node_replicas.read()).clone();

        // Key exists, get metadata from the first replica
        for replicas in node_replicas.values() {
            if replicas.tomb_tag.is_tomb() {
                continue;
            }
            let Some(len) = replicas
                .memory
                .as_ref()
                .map(|memory| memory.backing.len())
                .or_else(|| replicas.ssd.as_ref().map(|ssd| ssd.len))
            else {
                continue;
            };
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
            source_kind: part.source_kind,
            source_route_token: part.source_route_token,
            ssd_source_route_token: part.ssd_source_route_token,
            prepared_target: part.prepared_target,
            reused_committed_slot: part.reused_committed_slot,
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

fn late_bind_target_for_done(
    item: &BatchGetDoneItemReq,
    already_completed: bool,
) -> Option<&GetBindTarget> {
    (!already_completed)
        .then_some(item.late_target.as_ref())
        .flatten()
}

pub async fn handle_batch_get_done(
    view: MasterKvRouterView,
    req: MsgPack<BatchGetDoneReq>,
    req_node_id: NodeID,
) -> MsgPack<BatchGetDoneResp> {
    let started_at = Instant::now();
    let request_items = req.serialize_part.items;
    let get_ids = request_items
        .iter()
        .map(|item| item.get_id)
        .collect::<Vec<_>>();

    // Caller-owned late targets remove a separate Bind RTT. Reuse the same
    // validation/state transition before taking the sorted batch of terminal
    // locks. A completed operation skips Bind so a lost Done response remains
    // replayable with the original late target.
    let mut late_bind_errors = HashMap::<u64, GetDoneResp>::new();
    for item in &request_items {
        let already_completed = view
            .master_kv_router()
            .inner()
            .completed_gets
            .get(&item.get_id)
            .await
            .is_some();
        let Some(target) = late_bind_target_for_done(item, already_completed) else {
            continue;
        };
        let bound = handle_get_bind_item(
            view.clone(),
            BatchGetBindItemReq {
                get_id: item.get_id,
                target: target.clone(),
            },
            req_node_id.clone(),
        )
        .await;
        if bound.error_code != msg_and_error::OK {
            late_bind_errors.insert(
                item.get_id,
                GetDoneResp {
                    holder_id: 0,
                    allocation_mode: GetAllocationMode::Temporary,
                    error_code: bound.error_code,
                    error_json: bound.error_json,
                    server_process_us: 0,
                },
            );
        }
    }

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
    let mut deferred_post_read_reclaims = Vec::new();
    let mut response_by_get_id = HashMap::<u64, GetDoneResp>::new();
    for get_id in get_ids {
        let part = if let Some(part) = response_by_get_id.get(&get_id) {
            part.clone()
        } else if late_bind_errors.contains_key(&get_id)
            && view
                .master_kv_router()
                .inner()
                .completed_gets
                .get(&get_id)
                .await
                .is_some()
        {
            // A concurrent replay can observe "not completed", then lose the
            // Bind lock race to the original Done and see the Plan disappear.
            // Once the terminal lock is held, the completed result is the
            // authority; never replace it with that stale Bind error.
            handle_get_done_locked(
                view.clone(),
                MsgPack {
                    serialize_part: GetDoneReq { get_id },
                    raw_bytes: Vec::new(),
                },
                req_node_id.clone(),
                Some(&mut route_events),
                Some(&mut deferred_terminals),
                Some(&mut deferred_post_read_reclaims),
            )
            .await
            .serialize_part
        } else if let Some(error) = late_bind_errors.get(&get_id) {
            error.clone()
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
                Some(&mut deferred_post_read_reclaims),
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
    for candidate in deferred_post_read_reclaims {
        let _ = view
            .master_kv_router()
            .enqueue_post_read_remote_reclaim(candidate);
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
