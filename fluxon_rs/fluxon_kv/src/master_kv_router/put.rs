use super::{
    CommittedSlotReplica, CompletedReplicaTaskInfo, InflightPutAllocation, InflightPutCommitInfo,
    InflightPutInfo, InflightReplicaTarget, InflightReplicaTaskInfo, KvMemoryReplica,
    KvNodeReplicas, KvReplicaBacking, MasterKeyActivityCompletionGuard, MasterKvRouterView,
    NodeCacheCapacityReservation, OwnerHoldingGetInfo, PreparedPutKeyReservationInfo,
    PutPlacementMode, ReservedCapacityReason, SsdReplicaCommitStatus,
    msg_pack::{
        BatchPreparePutKeysReq, BatchPreparePutKeysResp, BatchPublishOwnerSsdReq,
        BatchPublishOwnerSsdResp, BatchPutAppendDoneItemResp, BatchPutAppendDoneReq,
        BatchPutAppendDoneResp, BatchPutAppendStartItemResp, BatchPutAppendStartReq,
        BatchPutAppendStartResp, BatchPutDoneItemResp, BatchPutDoneReq, BatchPutDoneResp,
        BatchPutRevokeItemResp, BatchPutRevokeReq, BatchPutRevokeResp, BatchPutStartItemResp,
        BatchPutStartReq, BatchPutStartResp, BatchReleasePutKeyReservationsReq,
        BatchReleasePutKeyReservationsResp, GroupedBatchPutDoneReq, GroupedBatchPutDoneResp,
        OwnerSsdPublishItem, OwnerSsdPublishItemResp, OwnerSsdPublishOutcome, PutAppendDoneReq,
        PutAppendDoneResp, PutAppendRevokeReq, PutAppendRevokeResp, PutAppendStartOutcome,
        PutAppendStartReq, PutAppendStartResp, PutAtomicGroup, PutDoneCommittedSlot, PutDoneReq,
        PutDoneResp, PutRevokeReq, PutRevokeResp, PutStartReq, PutStartResp, RadixKvMetadata,
        build_shared_put_atomic_group_assignments,
    },
    node_generation_is_current_live,
    placement::{PutPlacementTarget, select_remote_owner_candidates},
    publish_primary_route_tomb_fenced, publish_route_replica_tomb_fenced,
    route_maintenance::{
        RoutePublishEvent, apply_post_route_maintenance_batch, enqueue_post_route_maintenance,
    },
};
use crate::master_kv_router::OneKvNodesRoutes;
use crate::master_kv_router::delete::DeleteKeyInfo;
use crate::memholder::MemholderManagerTrait;
use crate::{
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
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

pub type PutIDForAKey = (u64, u32);

fn validate_radix_metadata(key: &str, radix: &RadixKvMetadata) -> Result<(), String> {
    if radix.parent_key.as_deref().is_some_and(str::is_empty) {
        return Err("radix parent key must be non-empty when present".to_string());
    }
    if radix.depth == 0 && radix.parent_key.is_some() {
        return Err("a depth-zero radix key must not have a parent".to_string());
    }
    if radix.depth > 0 && radix.parent_key.is_none() {
        return Err("a non-root radix key must have a parent".to_string());
    }
    if radix.parent_key.as_deref() == Some(key) {
        return Err("a radix key cannot be its own parent".to_string());
    }
    Ok(())
}

fn publish_owner_ssd_item(
    view: &MasterKvRouterView,
    owner: &NodeID,
    item: OwnerSsdPublishItem,
) -> OwnerSsdPublishItemResp {
    let response = |outcome, detail: String| OwnerSsdPublishItemResp {
        key: item.key.clone(),
        put_id: item.put_id,
        outcome,
        detail,
    };
    let _activity = match view
        .master_kv_router()
        .reserve_inflight_replica_key(&item.key)
    {
        Ok(activity) => activity,
        Err(err) => {
            return response(
                OwnerSsdPublishOutcome::RetryableBusy,
                format!("master key activity is busy: {err}"),
            );
        }
    };

    let Some(route) = view.master_kv_router().inner().kv_routes.get(&item.key) else {
        return response(
            OwnerSsdPublishOutcome::Obsolete,
            "route is absent".to_string(),
        );
    };
    if route.put_id != item.put_id {
        return response(
            OwnerSsdPublishOutcome::Obsolete,
            format!(
                "route generation changed: current=({},{})",
                route.put_id.0, route.put_id.1
            ),
        );
    }

    let has_remote_memory = {
        let replicas = route.node_replicas.read();
        let Some(owner_replicas) = replicas.get(owner) else {
            return response(
                OwnerSsdPublishOutcome::Obsolete,
                "same-owner route entry is absent".to_string(),
            );
        };
        if owner_replicas.tomb_tag.is_tomb() {
            return response(
                OwnerSsdPublishOutcome::Obsolete,
                "same-owner generation is tombed".to_string(),
            );
        }
        if let Some(existing) = owner_replicas.ssd.as_ref() {
            return if existing.len == item.len {
                response(
                    OwnerSsdPublishOutcome::AlreadyPresent,
                    "same-owner SSD backing is already published".to_string(),
                )
            } else {
                response(
                    OwnerSsdPublishOutcome::Rejected,
                    format!(
                        "existing SSD length mismatch: existing={} requested={}",
                        existing.len, item.len
                    ),
                )
            };
        }
        replicas.iter().any(|(node_id, replicas)| {
            node_id != owner && !replicas.tomb_tag.is_tomb() && replicas.memory.is_some()
        })
    };

    match route.commit_ssd_replica(owner, item.len) {
        SsdReplicaCommitStatus::Committed => {
            let counters = &view.master_kv_router().inner().ssd_tier_counters;
            let (items, bytes) = if has_remote_memory {
                (
                    &counters.local_ssd_published_with_remote_memory_items,
                    &counters.local_ssd_published_with_remote_memory_bytes,
                )
            } else {
                (
                    &counters.local_ssd_published_without_remote_memory_items,
                    &counters.local_ssd_published_without_remote_memory_bytes,
                )
            };
            items.fetch_add(1, Ordering::Relaxed);
            bytes.fetch_add(item.len, Ordering::Relaxed);
            response(
                OwnerSsdPublishOutcome::Published,
                "same-owner SSD backing published while memory remains live".to_string(),
            )
        }
        SsdReplicaCommitStatus::MissingMemory | SsdReplicaCommitStatus::TombedNode => response(
            OwnerSsdPublishOutcome::Obsolete,
            "exact live same-owner memory generation is absent".to_string(),
        ),
        SsdReplicaCommitStatus::LengthMismatch => response(
            OwnerSsdPublishOutcome::Rejected,
            format!(
                "SSD length does not match same-owner memory: requested={}",
                item.len
            ),
        ),
    }
}

/// Publish durable same-owner SSD bytes onto the current route without
/// deleting or replacing the memory backing.
pub async fn handle_batch_publish_owner_ssd(
    view: MasterKvRouterView,
    req: MsgPack<BatchPublishOwnerSsdReq>,
    owner: NodeID,
) -> MsgPack<BatchPublishOwnerSsdResp> {
    let current_generation = view
        .cluster_manager()
        .get_member_info_cached(owner.as_ref())
        .map(|member| member.node_start_time);
    if current_generation != Some(req.serialize_part.owner_node_start_time) {
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
            detail: format!(
                "owner SSD publication generation mismatch: owner={} requested={} current={:?}",
                owner, req.serialize_part.owner_node_start_time, current_generation
            ),
        });
        return MsgPack {
            serialize_part: BatchPublishOwnerSsdResp {
                items: Vec::new(),
                error_code: err.code(),
                error_json: err.to_json(),
            },
            raw_bytes: Vec::new(),
        };
    }

    let items = req
        .serialize_part
        .items
        .into_iter()
        .map(|item| publish_owner_ssd_item(&view, &owner, item))
        .collect();
    MsgPack {
        serialize_part: BatchPublishOwnerSsdResp {
            items,
            error_code: msg_and_error::OK,
            error_json: String::new(),
        },
        raw_bytes: Vec::new(),
    }
}

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
        .node_replicas
        .read()
        .iter()
        .any(|(node_id, replicas)| node_id != source_node_id && replicas.has_live_backing());
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
        KvMemoryReplica {
            backing: KvReplicaBacking::Allocation(Arc::new(allocation)),
            owner_local_indexed: false,
            get_durable_reservation: None,
            capacity_reservation,
        },
        target_tomb_tag,
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

fn append_current_route_owner_replica_if_matching(
    view: &MasterKvRouterView,
    key: &str,
    put_id: PutIDForAKey,
    node_id: NodeID,
    target_tomb_tag: crate::master_seg_manager::NodeTombTag,
    slot: CommittedSlotReplica,
) -> Option<RoutePublishEvent> {
    let Some(one_kv_nodes_routes) = view.master_kv_router().inner().kv_routes.get(key) else {
        return None;
    };
    if one_kv_nodes_routes.put_id != put_id
        || !node_generation_is_current_live(view, &node_id, &target_tomb_tag)
    {
        return None;
    }
    let capacity_bytes = slot.capacity_bytes;
    let lease_id = one_kv_nodes_routes.lease_id;
    let published = publish_route_replica_tomb_fenced(
        &one_kv_nodes_routes,
        node_id.clone(),
        KvMemoryReplica {
            backing: KvReplicaBacking::CommittedSlot(slot),
            owner_local_indexed: false,
            get_durable_reservation: None,
            capacity_reservation: None,
        },
        target_tomb_tag,
    );
    published.then(|| {
        RoutePublishEvent::replica_append(
            key.to_string(),
            put_id,
            lease_id,
            node_id,
            capacity_bytes,
        )
    })
}

fn current_route_has_exact_owner_replica(
    route: &OneKvNodesRoutes,
    node_id: &NodeID,
    target_tomb_tag: &crate::master_seg_manager::NodeTombTag,
    slot: &CommittedSlotReplica,
) -> bool {
    route.node_replicas.read().get(node_id).is_some_and(|replicas| {
        !replicas.tomb_tag.is_tomb()
            && replicas.tomb_tag.same_generation(target_tomb_tag)
            && replicas.memory.as_ref().is_some_and(|memory| {
                matches!(&memory.backing, KvReplicaBacking::CommittedSlot(current) if current == slot)
            })
    })
}

#[cfg(test)]
mod exact_owner_replica_tests {
    use super::{
        CommittedSlotReplica, KvMemoryReplica, KvNodeReplicas, KvReplicaBacking, OneKvNodesRoutes,
        current_route_has_exact_owner_replica,
    };
    use crate::cluster_manager::NodeID;
    use crate::master_seg_manager::NodeTombTag;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU32;

    #[test]
    fn existing_exact_get_slot_satisfies_replica_append_without_replacing_local_index() {
        let node: NodeID = "target-owner".to_string().into();
        let tag = NodeTombTag::new();
        let slot = CommittedSlotReplica {
            owner: crate::owner_segment::OwnerGeneration::new(node.clone(), 7),
            allocation_id: 41,
            segment_offset: 4096,
            capacity_bytes: 8192,
            addr: 0x2000,
            len: 5000,
            base_addr: 0x1000,
            segment_registration_epoch: 3,
        };
        let route = OneKvNodesRoutes {
            put_id: (9, 2),
            radix: None,
            lease_id: None,
            atomic_group: None,
            node_replicas: RwLock::new(HashMap::from([(
                node.clone(),
                KvNodeReplicas::memory(
                    tag.clone(),
                    KvMemoryReplica {
                        backing: KvReplicaBacking::CommittedSlot(slot.clone()),
                        owner_local_indexed: true,
                        get_durable_reservation: None,
                        capacity_reservation: None,
                    },
                ),
            )])),
            get_durable_slots_used: AtomicU32::new(0),
        };

        assert!(current_route_has_exact_owner_replica(
            &route, &node, &tag, &slot
        ));
        let mut different = slot.clone();
        different.allocation_id += 1;
        assert!(!current_route_has_exact_owner_replica(
            &route, &node, &tag, &different
        ));
        assert!(!current_route_has_exact_owner_replica(
            &route,
            &node,
            &NodeTombTag::new(),
            &slot,
        ));
        tag.set_tomb();
        assert!(!current_route_has_exact_owner_replica(
            &route, &node, &tag, &slot
        ));
    }
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

    let capacity = allocator.node_pool_capacity_snapshot();
    Err(msg_and_error::KvError::Api(
        msg_and_error::ApiError::NoSpace {
            node: node_id.as_ref().to_string(),
            segment: allocator.seg_device_id.clone(),
            total_capacity: capacity.active_capacity_bytes,
            free_capacity: capacity.available_capacity_bytes,
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
    if slot.allocation_id == 0
        || slot.capacity_bytes == 0
        || slot.capacity_bytes % crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES != 0
        || slot.segment_offset % crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES != 0
        || slot.len > slot.capacity_bytes
    {
        return Err(invalid(format!(
            "invalid committed owner slot geometry: allocation_id={} segment_offset={} capacity_bytes={} len={}",
            slot.allocation_id, slot.segment_offset, slot.capacity_bytes, slot.len
        )));
    }
    let current_owner = view
        .cluster_manager()
        .get_member_info_cached(node_id.as_ref())
        .ok_or_else(|| invalid(format!("committed owner slot owner is absent: {node_id}")))?;
    if slot.owner.node_id.as_str() != node_id.as_ref()
        || slot.owner.node_start_time != current_owner.node_start_time
        || slot.segment_registration_epoch == 0
    {
        return Err(invalid(format!(
            "committed owner slot generation mismatch: owner={} descriptor_owner={} descriptor_start={} current_start={} registration_epoch={}",
            node_id,
            slot.owner.node_id,
            slot.owner.node_start_time,
            current_owner.node_start_time,
            slot.segment_registration_epoch,
        )));
    }
    let tomb_tag = view.master_seg_manager().validate_owner_slot_geometry(
        node_id,
        slot.allocation_id,
        slot.segment_offset,
        slot.capacity_bytes,
        slot.base_addr,
        slot.addr,
    ).ok_or_else(|| {
        invalid(format!(
            "committed owner slot failed segment geometry validation: owner={} allocation_id={} segment_offset={} capacity_bytes={} base={:#x} addr={:#x}",
            node_id,
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
            slot.base_addr,
            slot.addr
        ))
    })?;
    if expected_tomb_tag.is_some_and(|expected| !expected.same_generation(&tomb_tag)) {
        return Err(invalid(format!(
            "committed owner slot belongs to a different generation: allocation_id={} owner={}",
            slot.allocation_id, node_id
        )));
    }

    Ok((slot.clone(), tomb_tag))
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
    // Owner slots do not reserve master-allocator capacity: their complete
    // segment is accounted by the owner authority.
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
    let operation_id = view
        .master_kv_router()
        .inner()
        .next_replica_operation_id
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    assert_ne!(
        operation_id, 0,
        "master replica operation identifier overflow"
    );
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
        "replica task reserved: key={} put_id=({},{}) operation_id={} source_node_id={} target_node_id={} preferred_sub_cluster={:?} len={}",
        key,
        put_id.0,
        put_id.1,
        operation_id,
        source_node_id,
        target_node_id,
        preferred_sub_cluster,
        len
    );
    let source_member = view
        .cluster_manager()
        .get_member_info_cached(source_node_id.as_ref())
        .ok_or_else(|| {
            msg_and_error::KvError::Api(msg_and_error::ApiError::NodeNotFound {
                desc: source_node_id.to_string(),
            })
        })?;
    Ok(InflightReplicaTaskInfo {
        operation_id,
        coordinator_generation: crate::owner_segment::OwnerGeneration::new(
            source_member.id,
            source_member.node_start_time,
        ),
        target: InflightReplicaTarget::MasterAllocation {
            node_id: target_node_id,
            target_tomb_tag,
            target_allocation: Arc::new(Mutex::new(Some(target_allocation))),
        },
        source_node_id: source_node_id.clone(),
        key: key.to_string(),
        put_id,
        len,
        atomic_batch: None,
        protect_source_on_remote_complete,
        _activity_lease: activity_lease,
    })
}

fn reserve_owner_replica_task_excluding(
    view: &MasterKvRouterView,
    key: &str,
    put_id: PutIDForAKey,
    source_node_id: &NodeID,
    preferred_sub_cluster: Option<&str>,
    len: u64,
    atomic_batch: Option<Arc<PutAtomicGroup>>,
    excluded_nodes: &HashSet<NodeID>,
    protect_source_on_remote_complete: bool,
) -> msg_and_error::KvResult<InflightReplicaTaskInfo> {
    let activity_lease = view.master_kv_router().reserve_inflight_replica_key(key)?;
    let operation_id = view
        .master_kv_router()
        .inner()
        .next_replica_operation_id
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    assert_ne!(
        operation_id, 0,
        "master replica operation identifier overflow"
    );
    view.master_kv_router()
        .pin_current_master_cache_identity_for_activity(
            &activity_lease,
            source_node_id.as_ref(),
            key,
            put_id,
        );
    let plan = select_remote_owner_candidates(
        view,
        source_node_id,
        excluded_nodes,
        preferred_sub_cluster,
        len,
        operation_id,
    )?;
    tracing::debug!(
        key,
        put_time_ms = put_id.0,
        put_version = put_id.1,
        operation_id,
        source_node_id = %source_node_id,
        candidate_count = plan.candidates.len(),
        policy_epoch = plan.policy_epoch,
        capacity_snapshot_id = plan.capacity_snapshot_id,
        "owner replica task planned without master allocation"
    );
    let source_member = view
        .cluster_manager()
        .get_member_info_cached(source_node_id.as_ref())
        .ok_or_else(|| {
            msg_and_error::KvError::Api(msg_and_error::ApiError::NodeNotFound {
                desc: source_node_id.to_string(),
            })
        })?;
    Ok(InflightReplicaTaskInfo {
        operation_id,
        coordinator_generation: crate::owner_segment::OwnerGeneration::new(
            source_member.id,
            source_member.node_start_time,
        ),
        target: InflightReplicaTarget::OwnerCandidates { plan },
        source_node_id: source_node_id.clone(),
        key: key.to_string(),
        put_id,
        len,
        atomic_batch,
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
    radix: Option<RadixKvMetadata>,
    node_id: NodeID,
    publish_tag: crate::master_seg_manager::NodeTombTag,
    completed_info: KvMemoryReplica,
    target_cap_bytes: u64,
    local_cache_holder_id: Option<u64>,
    deferred_maintenance_events: Option<&mut Vec<RoutePublishEvent>>,
) -> MsgPack<PutDoneResp> {
    let new_route = Arc::new(OneKvNodesRoutes {
        put_id,
        lease_id: lease_id_opt,
        atomic_group: atomic_group.clone(),
        radix,
        node_replicas: RwLock::new(HashMap::from([(
            node_id.clone(),
            KvNodeReplicas::memory(publish_tag.clone(), completed_info),
        )])),
        get_durable_slots_used: AtomicU32::new(0),
    });
    let old_one_kv_routes = match publish_primary_route_tomb_fenced(
        &view.master_kv_router().inner().kv_routes,
        &key,
        new_route.clone(),
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

    let maintenance_event = RoutePublishEvent::primary_put(
        key.clone(),
        put_id,
        lease_id_opt,
        node_id.clone(),
        target_cap_bytes,
    );
    if let Some(events) = deferred_maintenance_events {
        events.push(maintenance_event);
    } else {
        apply_post_route_maintenance_batch(&view, vec![maintenance_event]).await;
    }

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
                let InflightReplicaTarget::MasterAllocation {
                    node_id,
                    target_allocation,
                    ..
                } = &target.target
                else {
                    unreachable!(
                        "normal Put pre-reservation must remain master-authoritative until stage D"
                    );
                };
                let target_allocation_guard = target_allocation.lock();
                let target_allocation = target_allocation_guard.as_ref().expect(
                    "replica target allocation must exist while building put_start response",
                );
                super::msg_pack::PutReplicaTarget {
                    node_id: node_id.clone().into(),
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
                        let InflightReplicaTarget::MasterAllocation { node_id, .. } =
                            &reservation.target
                        else {
                            unreachable!(
                                "normal Put replica pre-reservation must use master allocation"
                            );
                        };
                        view.master_kv_router()
                            .record_replica_task_target(node_id.as_ref());
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
    handle_put_done_with_resolved_group(view, req, req_node_id, None, None).await
}

async fn handle_put_done_with_resolved_group(
    view: MasterKvRouterView,
    req: MsgPack<PutDoneReq>,
    req_node_id: NodeID,
    resolved_atomic_group: Option<Arc<PutAtomicGroup>>,
    deferred_maintenance_events: Option<&mut Vec<RoutePublishEvent>>,
) -> MsgPack<PutDoneResp> {
    tracing::debug!("Handling PutDoneReq: {:?}", req.serialize_part);

    let put_id = req.serialize_part.put_id;
    let lease_id_opt = req.serialize_part.lease_id;
    if let Some(radix) = req.serialize_part.radix.as_ref() {
        if let Err(detail) = validate_radix_metadata(&req.serialize_part.key, radix) {
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
                detail: format!(
                    "invalid radix metadata for key={}: {detail}",
                    req.serialize_part.key
                ),
            });
            return MsgPack {
                serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                raw_bytes: Vec::new(),
            };
        }
    }
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
                    let target_cap_bytes = committed_slot.capacity_bytes;
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
                        KvMemoryReplica {
                            backing: KvReplicaBacking::CommittedSlot(committed_slot),
                            owner_local_indexed: true,
                            get_durable_reservation: None,
                            capacity_reservation,
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
                        KvMemoryReplica {
                            backing: KvReplicaBacking::Allocation(Arc::new(target_allocation)),
                            owner_local_indexed: req.serialize_part.publish_local_cache,
                            get_durable_reservation: None,
                            capacity_reservation,
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
                    KvMemoryReplica {
                        backing: KvReplicaBacking::Allocation(Arc::new(target)),
                        owner_local_indexed: false,
                        get_durable_reservation: None,
                        capacity_reservation,
                    },
                    false,
                )
            }
            InflightPutAllocation::LocalCommittedSlot(slot) => {
                let target_cap_bytes = slot.capacity_bytes;
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
                    KvMemoryReplica {
                        backing: KvReplicaBacking::CommittedSlot(slot),
                        owner_local_indexed: true,
                        get_durable_reservation: None,
                        capacity_reservation,
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
        let publish_tag = tomb_tag.clone();
        let new_route = Arc::new(OneKvNodesRoutes {
            put_id,
            lease_id: lease_id_opt,
            atomic_group: atomic_group.clone(),
            radix: req.serialize_part.radix.clone(),
            node_replicas: RwLock::new(HashMap::from([(
                node_id.clone(),
                KvNodeReplicas::memory(publish_tag.clone(), completed_info),
            )])),
            get_durable_slots_used: AtomicU32::new(0),
        });
        let old_one_kv_routes = match publish_primary_route_tomb_fenced(
            &view.master_kv_router().inner().kv_routes,
            &key,
            new_route.clone(),
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
            let target_cap_bytes = committed_slot.capacity_bytes;
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
            let completed_info = KvMemoryReplica {
                backing: KvReplicaBacking::CommittedSlot(committed_slot),
                owner_local_indexed: true,
                get_durable_reservation: None,
                capacity_reservation,
            };
            return publish_completed_put_route(
                view,
                key,
                put_id,
                lease_id_opt,
                atomic_group,
                req.serialize_part.radix.clone(),
                node_id,
                tomb_tag,
                completed_info,
                target_cap_bytes,
                None,
                deferred_maintenance_events,
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
    let mut maintenance_events = Vec::with_capacity(req.serialize_part.items.len());
    for item in req.serialize_part.items {
        let key = item.key.clone();
        let put_id = item.put_id;
        let lease_id = item.lease_id;
        let resp = handle_put_done_with_resolved_group(
            view.clone(),
            MsgPack {
                serialize_part: PutDoneReq {
                    key,
                    put_id,
                    lease_id,
                    committed_slot: item.committed_slot,
                    publish_local_cache: item.publish_local_cache,
                    atomic_group: item.atomic_group,
                    radix: item.radix,
                },
                raw_bytes: Vec::new(),
            },
            req_node_id.clone(),
            None,
            Some(&mut maintenance_events),
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
    if !maintenance_events.is_empty() {
        apply_post_route_maintenance_batch(&view, maintenance_events).await;
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
    let mut maintenance_events = Vec::with_capacity(request_items.len());
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
                    radix: item.radix,
                },
                raw_bytes: Vec::new(),
            },
            req_node_id.clone(),
            atomic_group,
            Some(&mut maintenance_events),
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
    if !maintenance_events.is_empty() {
        apply_post_route_maintenance_batch(&view, maintenance_events).await;
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
    let append_key = (key.clone(), put_id.0, put_id.1);
    let operation_lock = view
        .master_kv_router()
        .inner()
        .replica_operation_locks
        .get_lock(append_key.clone());
    let _operation_guard = operation_lock.lock().await;
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

    let route_atomic_batch = route_snapshot
        .as_ref()
        .and_then(|route| route.atomic_group.as_ref().cloned());
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
                    .node_replicas
                    .read()
                    .iter()
                    .filter_map(|(node_id, replicas)| {
                        replicas.has_live_backing().then_some(node_id.clone())
                    })
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();
        let reservation = match reserve_owner_replica_task_excluding(
            &view,
            &key,
            put_id,
            &req_node_id,
            req.serialize_part.preferred_sub_cluster.as_deref(),
            req.serialize_part.len,
            route_atomic_batch,
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
        if let InflightReplicaTarget::OwnerCandidates { plan } = &reservation.target
            && let Some(first) = plan.candidates.first()
        {
            view.master_kv_router()
                .record_replica_task_target(&first.owner.node_id);
        }
        view.master_kv_router()
            .inner()
            .inflight_replica_tasks
            .insert(append_key.clone(), reservation.clone())
            .await;
        reservation
    };
    let InflightReplicaTarget::OwnerCandidates { plan } = &inflight.target else {
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
            detail: format!(
                "replica append found a master-allocation reservation after owner protocol activation: key={} put_id=({},{})",
                key, put_id.0, put_id.1
            ),
        });
        return MsgPack {
            serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
            raw_bytes: Vec::new(),
        };
    };
    let operation = crate::owner_segment::OwnerTransferOpId::new(
        inflight.coordinator_generation.clone(),
        inflight.operation_id,
        crate::owner_segment::OwnerTransferOpKind::ReplicaAppend,
    );
    let atomic_batch = inflight.atomic_batch.as_deref().cloned();
    let owner_candidates = plan
        .candidates
        .iter()
        .cloned()
        .map(|candidate| crate::owner_segment::OwnerTargetRouteToken {
            key: key.clone(),
            put_id,
            operation: operation.clone(),
            target_owner: candidate.owner,
            prior_route_epoch: 0,
            policy_epoch: plan.policy_epoch,
            atomic_batch: atomic_batch.clone(),
            plan_nonce: inflight.operation_id,
        })
        .collect();

    MsgPack {
        serialize_part: PutAppendStartResp {
            outcome: PutAppendStartOutcome::Scheduled,
            operation_id: inflight.operation_id,
            node_id: String::new(),
            target_addr: 0,
            target_base_addr: 0,
            len: req.serialize_part.len,
            owner_candidates,
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
            operation_id: part.operation_id,
            node_id: part.node_id,
            target_addr: part.target_addr,
            target_base_addr: part.target_base_addr,
            len: part.len,
            owner_candidates: part.owner_candidates,
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
    let operation_id = req.serialize_part.operation_id;
    let generation_identity = (key.clone(), put_id.0, put_id.1);
    let operation_identity = (key.clone(), put_id.0, put_id.1, operation_id);
    let operation_lock = view
        .master_kv_router()
        .inner()
        .replica_operation_locks
        .get_lock(generation_identity.clone());
    let _operation_guard = operation_lock.lock().await;
    if view
        .master_kv_router()
        .inner()
        .completed_replica_tasks
        .get(&operation_identity)
        .await
        .is_some()
    {
        return MsgPack {
            serialize_part: PutAppendRevokeResp {
                error_code: msg_and_error::OK,
                error_json: String::new(),
            },
            raw_bytes: Vec::new(),
        };
    }
    let inflight = view
        .master_kv_router()
        .inner()
        .inflight_replica_tasks
        .get(&generation_identity)
        .await;
    if inflight
        .as_ref()
        .is_some_and(|inflight| inflight.operation_id == operation_id)
    {
        if let Some(inflight) = view
            .master_kv_router()
            .inner()
            .inflight_replica_tasks
            .remove(&generation_identity)
            .await
        {
            inflight._activity_lease.release_now();
        }
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
    req_node_id: NodeID,
) -> MsgPack<PutAppendDoneResp> {
    let put_id = req.serialize_part.put_id;
    let key = req.serialize_part.key.clone();
    let operation_id = req.serialize_part.operation_id;
    let generation_identity = (key.clone(), put_id.0, put_id.1);
    let operation_identity = (key.clone(), put_id.0, put_id.1, operation_id);
    let operation_lock = view
        .master_kv_router()
        .inner()
        .replica_operation_locks
        .get_lock(generation_identity.clone());
    let _operation_guard = operation_lock.lock().await;
    if let Some(completed) = view
        .master_kv_router()
        .inner()
        .completed_replica_tasks
        .get(&operation_identity)
        .await
    {
        view.master_kv_router()
            .inner()
            .replica_done_terminal_replay_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return MsgPack {
            serialize_part: PutAppendDoneResp {
                appended: completed.appended,
                route_epoch: completed.route_epoch,
                error_code: msg_and_error::OK,
                error_json: String::new(),
                server_process_us: 0,
            },
            raw_bytes: Vec::new(),
        };
    }
    let Some(current) = view
        .master_kv_router()
        .inner()
        .inflight_replica_tasks
        .get(&generation_identity)
        .await
    else {
        // Start may have been revoked, superseded, or expired while the
        // target was transferring. That is a definitive obsolete terminal
        // for this immutable operation. Cache it before replying so response
        // loss cannot turn the same Done into an unbounded retry loop.
        view.master_kv_router()
            .inner()
            .completed_replica_tasks
            .insert(
                operation_identity,
                CompletedReplicaTaskInfo {
                    appended: false,
                    route_epoch: 0,
                },
            )
            .await;
        tracing::debug!(
            key,
            put_time_ms = put_id.0,
            put_version = put_id.1,
            operation_id,
            "Put append reservation is absent at Done; publishing an obsolete replay terminal"
        );
        return MsgPack {
            serialize_part: PutAppendDoneResp {
                appended: false,
                route_epoch: 0,
                error_code: msg_and_error::OK,
                error_json: String::new(),
                server_process_us: 0,
            },
            raw_bytes: Vec::new(),
        };
    };
    if current.operation_id != operation_id {
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
            detail: format!(
                "Put append operation generation mismatch: key={} put_id=({},{}) requested_operation_id={} current_operation_id={}",
                key, put_id.0, put_id.1, operation_id, current.operation_id
            ),
        });
        return MsgPack {
            serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
            raw_bytes: Vec::new(),
        };
    }
    let (committed_slot, route_token) = match (
        req.serialize_part.committed_slot.as_ref(),
        req.serialize_part.route_token.as_ref(),
    ) {
        (Some(slot), Some(token)) => (slot, token),
        _ => {
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
                detail: format!(
                    "owner replica completion is missing committed slot or route token: key={} put_id=({},{}) operation_id={}",
                    key, put_id.0, put_id.1, operation_id
                ),
            });
            return MsgPack {
                serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                raw_bytes: Vec::new(),
            };
        }
    };
    let InflightReplicaTarget::OwnerCandidates { plan } = &current.target else {
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
            detail: "owner replica completion matched a master-allocation reservation".to_string(),
        });
        return MsgPack {
            serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
            raw_bytes: Vec::new(),
        };
    };
    let expected_operation = crate::owner_segment::OwnerTransferOpId::new(
        current.coordinator_generation.clone(),
        operation_id,
        crate::owner_segment::OwnerTransferOpKind::ReplicaAppend,
    );
    let expected_atomic_batch = current.atomic_batch.as_deref().cloned();
    let target_is_candidate = plan
        .candidates
        .iter()
        .any(|candidate| candidate.owner == route_token.target_owner);
    let mut identity_mismatches = Vec::new();
    if route_token.key != key {
        identity_mismatches.push("key");
    }
    if route_token.put_id != put_id {
        identity_mismatches.push("put_id");
    }
    if route_token.operation != expected_operation {
        identity_mismatches.push("operation");
    }
    if route_token.plan_nonce != operation_id {
        identity_mismatches.push("plan_nonce");
    }
    if route_token.policy_epoch != plan.policy_epoch {
        identity_mismatches.push("policy_epoch");
    }
    if route_token.atomic_batch != expected_atomic_batch {
        identity_mismatches.push("atomic_batch");
    }
    if !target_is_candidate {
        identity_mismatches.push("target_candidate");
    }
    if route_token.target_owner.node_id.as_str() != req_node_id.as_ref() {
        identity_mismatches.push("caller_node");
    }
    if committed_slot.owner != route_token.target_owner {
        identity_mismatches.push("slot_owner_generation");
    }
    if committed_slot.len != current.len {
        identity_mismatches.push("len");
    }
    if !identity_mismatches.is_empty() {
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
            detail: format!(
                "owner replica completion identity mismatch: fields={:?} key={} put_id=({},{}) operation_id={} caller={} target=({},{}) slot_owner=({},{}) expected_coordinator=({},{}) actual_coordinator=({},{}) expected_policy_epoch={} actual_policy_epoch={} expected_len={} actual_len={} expected_atomic_members={} actual_atomic_members={}",
                identity_mismatches,
                key,
                put_id.0,
                put_id.1,
                operation_id,
                req_node_id,
                route_token.target_owner.node_id,
                route_token.target_owner.node_start_time,
                committed_slot.owner.node_id,
                committed_slot.owner.node_start_time,
                expected_operation.coordinator.node_id,
                expected_operation.coordinator.node_start_time,
                route_token.operation.coordinator.node_id,
                route_token.operation.coordinator.node_start_time,
                plan.policy_epoch,
                route_token.policy_epoch,
                current.len,
                committed_slot.len,
                expected_atomic_batch
                    .as_ref()
                    .map_or(0, |group| group.members.len()),
                route_token
                    .atomic_batch
                    .as_ref()
                    .map_or(0, |group| group.members.len()),
            ),
        });
        return MsgPack {
            serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
            raw_bytes: Vec::new(),
        };
    }
    let (validated_slot, target_tomb_tag) =
        match validate_put_done_committed_slot(&view, &req_node_id, committed_slot, None) {
            Ok(validated) => validated,
            Err(err) => {
                return MsgPack {
                    serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                    raw_bytes: Vec::new(),
                };
            }
        };
    let Some(inflight) = view
        .master_kv_router()
        .inner()
        .inflight_replica_tasks
        .remove(&generation_identity)
        .await
    else {
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
            detail: format!(
                "Put append operation disappeared during completion: key={} put_id=({},{}) operation_id={}",
                key, put_id.0, put_id.1, operation_id
            ),
        });
        return MsgPack {
            serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
            raw_bytes: Vec::new(),
        };
    };
    let _activity_completion =
        MasterKeyActivityCompletionGuard::new(inflight._activity_lease.clone());
    let route_epoch = validated_slot.allocation_id;
    let existing_exact = view
        .master_kv_router()
        .inner()
        .kv_routes
        .get(&key)
        .is_some_and(|route| {
            route.put_id == inflight.put_id
                && current_route_has_exact_owner_replica(
                    &route,
                    &req_node_id,
                    &target_tomb_tag,
                    &validated_slot,
                )
        });
    let published = (!existing_exact)
        .then(|| {
            append_current_route_owner_replica_if_matching(
                &view,
                &key,
                inflight.put_id,
                req_node_id,
                target_tomb_tag,
                validated_slot,
            )
        })
        .flatten();
    let appended = existing_exact || published.is_some();
    let committed_route_epoch = if appended { route_epoch } else { 0 };
    view.master_kv_router()
        .inner()
        .completed_replica_tasks
        .insert(
            operation_identity,
            CompletedReplicaTaskInfo {
                appended,
                route_epoch: committed_route_epoch,
            },
        )
        .await;
    if let Some(event) = published {
        enqueue_post_route_maintenance(&view, event).await;
    }
    MsgPack {
        serialize_part: PutAppendDoneResp {
            appended,
            route_epoch: committed_route_epoch,
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
    req_node_id: NodeID,
) -> MsgPack<PutAppendDoneResp> {
    handle_put_append_done_inner(view, req, req_node_id).await
}

pub async fn handle_batch_put_append_done(
    view: MasterKvRouterView,
    req: MsgPack<BatchPutAppendDoneReq>,
    req_node_id: NodeID,
) -> MsgPack<BatchPutAppendDoneResp> {
    let mut items = Vec::with_capacity(req.serialize_part.items.len());
    for item in req.serialize_part.items {
        let key = item.key.clone();
        let put_id = item.put_id;
        let operation_id = item.operation_id;
        let resp = handle_put_append_done_inner(
            view.clone(),
            MsgPack {
                serialize_part: PutAppendDoneReq {
                    key,
                    put_id,
                    operation_id,
                    committed_slot: item.committed_slot.clone(),
                    route_token: item.route_token.clone(),
                },
                raw_bytes: Vec::new(),
            },
            req_node_id.clone(),
        )
        .await;
        let part = resp.serialize_part;
        items.push(BatchPutAppendDoneItemResp {
            key: item.key,
            put_id: item.put_id,
            appended: part.appended,
            route_epoch: part.route_epoch,
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
