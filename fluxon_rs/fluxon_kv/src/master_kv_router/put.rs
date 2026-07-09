use super::NodeValueReplicaDesc;
use super::{
    CommittedSlotReplica, InflightPutAllocation, InflightPutCommitInfo, InflightPutInfo,
    InflightReplicaTaskInfo, KvReplicaBacking, KvRouteInfo, LocalReserveGrantInfo,
    MasterKvRouterView, OwnerHoldingGetInfo, PreparedPutKeyReservationInfo, PutPlacementMode,
    ReservedCapacityReason,
    msg_pack::{
        BatchPreparePutKeysReq, BatchPreparePutKeysResp, BatchPutDoneItemResp, BatchPutDoneReq,
        BatchPutDoneResp, BatchPutRevokeItemResp, BatchPutRevokeReq, BatchPutRevokeResp,
        BatchPutStartItemResp, BatchPutStartReq, BatchPutStartResp,
        BatchReleasePutKeyReservationsReq, BatchReleasePutKeyReservationsResp, PutAppendDoneReq,
        PutAppendDoneResp, PutAppendRevokeReq, PutAppendRevokeResp, PutAppendStartReq,
        PutAppendStartResp, PutDoneReq, PutDoneResp, PutRevokeReq, PutRevokeResp, PutStartReq,
        PutStartResp, ReleaseLocalGrantReq, ReleaseLocalGrantResp, ReserveLocalGrantReq,
        ReserveLocalGrantResp,
    },
    placement::PutPlacementTarget,
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

struct InflightPutKeyReservation {
    view: MasterKvRouterView,
    key: String,
    active: bool,
}

impl InflightPutKeyReservation {
    fn new(view: MasterKvRouterView, key: String) -> Self {
        Self {
            view,
            key,
            active: true,
        }
    }

    fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for InflightPutKeyReservation {
    fn drop(&mut self) {
        if self.active {
            self.view
                .master_kv_router()
                .release_inflight_put_key(&self.key);
        }
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

fn current_route_needs_remote_replica(
    route: &OneKvNodesRoutes,
    source_node_id: &NodeID,
    verify_put_id: PutIDForAKey,
) -> bool {
    if route.put_id != verify_put_id {
        return false;
    }
    route
        .nodes_replicas
        .read()
        .values()
        .all(|replica| replica.tomb_tag.is_tomb() || replica.node_id == *source_node_id)
}

fn append_current_route_replica_if_matching(
    view: &MasterKvRouterView,
    key: &str,
    put_id: PutIDForAKey,
    node_id: NodeID,
    allocation: Allocation,
) -> bool {
    let Some(one_kv_nodes_routes) = view.master_kv_router().inner().kv_routes.get(key) else {
        tracing::debug!(
            "append_current_route_replica_if_matching skipped because route disappeared: key={} put_id=({},{})",
            key,
            put_id.0,
            put_id.1
        );
        return false;
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
        return false;
    }
    let Some(tomb_tag) = view.master_seg_manager().get_node_tomb_tag(&node_id) else {
        tracing::warn!(
            "append_current_route_replica_if_matching skipped because target node tomb-tag missing: key={} put_id=({},{}) node_id={}",
            key,
            put_id.0,
            put_id.1,
            node_id
        );
        return false;
    };
    if tomb_tag.is_tomb() {
        tracing::warn!(
            "append_current_route_replica_if_matching skipped because target node is tomb: key={} put_id=({},{}) node_id={}",
            key,
            put_id.0,
            put_id.1,
            node_id
        );
        return false;
    }
    let alloc_cap = allocation.capcity();
    let req_weight = if alloc_cap > u32::MAX as u64 {
        tracing::warn!(
            "moka weight saturation on put append: key={} put_id=({},{}) cap={}B exceeds u32::MAX; weight set to u32::MAX",
            key,
            put_id.0,
            put_id.1,
            alloc_cap
        );
        u32::MAX
    } else {
        alloc_cap as u32
    };
    let lease_id = one_kv_nodes_routes.lease_id;
    one_kv_nodes_routes.nodes_replicas.write().insert(
        node_id.clone(),
        KvRouteInfo {
            node_id: node_id.clone(),
            backing: KvReplicaBacking::Allocation(Arc::new(allocation)),
            tomb_tag,
        },
    );
    if lease_id.is_none() {
        if let Some(cache) = view.master_kv_router().get_node_cache_controller(&node_id) {
            cache.insert(
                key.to_string(),
                NodeValueReplicaDesc {
                    weight_bytes: req_weight,
                    put_id,
                },
            );
        }
    }
    true
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
) -> msg_and_error::KvResult<InflightReplicaTaskInfo> {
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
        key: key.to_string(),
        put_id,
        target_allocation: Arc::new(Mutex::new(Some(target_allocation))),
    })
}

async fn publish_completed_put_route(
    view: MasterKvRouterView,
    key: String,
    put_id: PutIDForAKey,
    lease_id_opt: Option<u64>,
    node_id: NodeID,
    completed_info: KvRouteInfo,
    target_cap_bytes: u64,
    local_cache_holder_id: Option<u64>,
) -> MsgPack<PutDoneResp> {
    let saturated_weight_u32 = if target_cap_bytes > u32::MAX as u64 {
        tracing::warn!(
            "moka weight saturation: key={} put_id=({},{}) cap={}B exceeds u32::MAX; weight set to u32::MAX",
            key,
            put_id.0,
            put_id.1,
            target_cap_bytes
        );
        u32::MAX
    } else {
        target_cap_bytes as u32
    };

    let mut old_one_kv_routes: Option<Arc<OneKvNodesRoutes>> = None;
    let mut inserted = false;
    {
        let mut one_kv_routes = view
            .master_kv_router()
            .inner()
            .kv_routes
            .entry(key.clone())
            .or_insert_with(|| {
                inserted = true;
                Arc::new(OneKvNodesRoutes {
                    put_id,
                    lease_id: lease_id_opt,
                    nodes_replicas: RwLock::new(HashMap::new()),
                    get_durable_slots_used: AtomicU32::new(0),
                })
            });
        if !inserted {
            old_one_kv_routes = Some(one_kv_routes.clone());
            *one_kv_routes = Arc::new(OneKvNodesRoutes {
                put_id,
                lease_id: lease_id_opt,
                nodes_replicas: RwLock::new(HashMap::new()),
                get_durable_slots_used: AtomicU32::new(0),
            });
        }
        one_kv_routes
            .nodes_replicas
            .write()
            .insert(node_id.clone(), completed_info);
    }

    if let Some(old) = old_one_kv_routes {
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

    {
        let view_task = view.clone();
        let key_for_spawn = key.clone();
        let node_for_spawn = node_id.clone();
        let do_prefix_index_update = view.master_kv_router().prefix_index_enabled();
        let do_cache_insert =
            lease_id_opt.is_none() && view.master_kv_router().replica_cache_enabled();
        let cap_bytes_u32 = saturated_weight_u32;
        view.spawn("post_put_done_maintenance", async move {
            if do_prefix_index_update {
                let inner = view_task.master_kv_router().inner();
                let mut tree = inner.prefix_index.write().await;
                tree.insert(&key_for_spawn);
            }

            if do_cache_insert {
                let cache = view_task
                    .master_kv_router()
                    .get_node_cache_controller(&node_for_spawn);
                if let Some(cache) = cache {
                    let desc = NodeValueReplicaDesc {
                        weight_bytes: cap_bytes_u32,
                        put_id,
                    };
                    tracing::debug!("Inserting key: {:?} into cache", key_for_spawn);
                    cache.insert(key_for_spawn.clone(), desc);
                    tracing::debug!(
                        "Inserted key: {:?} into cache, current cache size: {}",
                        key_for_spawn,
                        cache.weighted_size()
                    );
                } else {
                    tracing::warn!(
                        "No cache controller found for node: {}, node is not ready",
                        node_for_spawn
                    );
                }
            }
        });
    }

    tracing::info!(
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
    if let Err(err) = view.master_kv_router().reserve_inflight_put_key(
        &key,
        req.serialize_part.reject_if_inflight_same_key,
        req.serialize_part.reject_if_exist_same_key,
    ) {
        let resp: PutStartResp = crate::rpcresp_kvresult_convert::FromError::from_error(&err);
        return (
            (0, 0),
            MsgPack {
                serialize_part: resp,
                raw_bytes: Vec::new(),
            },
        );
    }
    let mut key_reservation = InflightPutKeyReservation::new(view.clone(), key.clone());
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
                src_target_allocation: Arc::new(Mutex::new(Some(inflight_alloc))),
                replica_target: replica_target.clone(),
            },
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

    let put_target = Ok(PutPlacementTarget::Local {
        node_id: source_node_id.clone(),
    });

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
                node_id,
                InflightPutAllocation::Local(src),
                abs,
                abs,
                src_base,
                src_base,
                allocation_size,
                replica_target,
            );
            let result = fut.await;
            key_reservation.disarm();
            return result;
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
            let fut = finalize(
                node_id.clone(),
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
            let result = fut.await;
            key_reservation.disarm();
            return result;
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
    req: MsgPack<ReserveLocalGrantReq>,
    req_node_id: NodeID,
) -> (u64, MsgPack<ReserveLocalGrantResp>) {
    let _ = req;

    let mut allocation = match allocate_from_node_local_segment(
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

    let reserved_bytes = allocation.capcity();
    if let Err(err) = view.master_kv_router().adjust_node_cache_reserved_capacity(
        req_node_id.as_ref(),
        ReservedCapacityReason::LocalReserveGrant,
        reserved_bytes as i64,
    ) {
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

    let view_clone = view.clone();
    let node_id_string = req_node_id.as_ref().to_string();
    allocation.set_on_drop(move || {
        if let Err(e) = view_clone
            .master_kv_router()
            .adjust_node_cache_reserved_capacity(
                &node_id_string,
                ReservedCapacityReason::LocalReserveGrant,
                -(reserved_bytes as i64),
            )
        {
            tracing::warn!(
                "Failed to restore moka capacity on local reserve grant drop: node_id={}, bytes={}, err={}",
                node_id_string,
                reserved_bytes,
                e
            );
        }
    });

    let grant_id = view.master_kv_router().next_local_reserve_grant_id();
    let grant_base_addr = allocation.base_addr();
    let grant_abs_addr = grant_base_addr + allocation.addr();
    let grant_len = allocation.capcity();
    view.master_kv_router().install_local_reserve_grant(
        grant_id,
        LocalReserveGrantInfo {
            owner_node_id: req_node_id.clone(),
            allocation,
        },
    );

    (
        grant_id,
        MsgPack {
            serialize_part: ReserveLocalGrantResp {
                grant_id,
                node_id: req_node_id.into_owned(),
                addr: grant_abs_addr,
                base_addr: grant_base_addr,
                len: grant_len,
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
        if let Err(err) = view.master_kv_router().reserve_inflight_put_key(
            &item.key,
            item.reject_if_inflight_same_key,
            item.reject_if_exist_same_key,
        ) {
            for reservation_id in reservation_ids.drain(..) {
                if let Some(info) = view
                    .master_kv_router()
                    .take_prepared_put_key_reservation(reservation_id)
                {
                    view.master_kv_router().release_inflight_put_key(&info.key);
                }
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
        let reservation_id = view
            .master_kv_router()
            .next_prepared_put_key_reservation_id();
        view.master_kv_router()
            .install_prepared_put_key_reservation(
                reservation_id,
                PreparedPutKeyReservationInfo {
                    owner_node_id: req_node_id.clone(),
                    key: item.key,
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

    for (_reservation_id, info) in taken {
        view.master_kv_router().release_inflight_put_key(&info.key);
    }

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
        view.master_kv_router()
            .release_inflight_put_key(&inflight_info.key);
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
    tracing::debug!("Handling PutDoneReq: {:?}", req.serialize_part);

    let put_id = req.serialize_part.put_id;
    let lease_id_opt = req.serialize_part.lease_id;
    let full_put_id: (String, u64, u32) = (req.serialize_part.key.clone(), put_id.0, put_id.1);
    let local_cache_holder_id: Option<u64>;

    // Remove from inflight_puts and store in completed_puts
    if let Some(InflightPutInfo {
        key, commit_info, ..
    }) = view
        .master_kv_router()
        .inner()
        .inflight_puts
        .remove(&full_put_id)
        .await
    {
        view.master_kv_router().release_inflight_put_key(&key);
        let node_id = commit_info.node_id;
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

        let Some(tomb_tag) = view.master_seg_manager().get_node_tomb_tag(&node_id) else {
            tracing::warn!(
                "Put operation with put_id {:?} not found for completion",
                put_id
            );
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
                detail: format!(
                    "Put operation with put_id {:?} not found for completion",
                    put_id
                ),
            });
            return MsgPack {
                serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                raw_bytes: Vec::new(),
            };
        };

        if tomb_tag.is_tomb() {
            tracing::info!("Put operation with put_id {:?} is tomb, skip", put_id);
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
                detail: format!("Put operation with put_id {:?} is tomb, skip", put_id),
            });
            return MsgPack {
                serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                raw_bytes: Vec::new(),
            };
        }

        let route_committed_slot = req.serialize_part.committed_slot.clone();
        let (target_cap_bytes, completed_info, local_cache_publish_supported) = match allocs {
            InflightPutAllocation::Local(mut target_allocation) => {
                if let Some(slot) = route_committed_slot {
                    let target_cap_bytes = slot.len;
                    if let Some(lease_id) = lease_id_opt {
                        if let Err(e) = view
                            .master_lease_manager()
                            .attach_key(lease_id, key.clone(), put_id)
                            .await
                        {
                            let kv_err: crate::rpcresp_kvresult_convert::msg_and_error::KvError =
                                e.into();
                            return MsgPack {
                                serialize_part:
                                    crate::rpcresp_kvresult_convert::FromError::from_error(&kv_err),
                                raw_bytes: Vec::new(),
                            };
                        }
                        if let Err(e) = view.master_kv_router().adjust_node_cache_reserved_capacity(
                            node_id.as_ref(),
                            ReservedCapacityReason::LeaseBoundKv,
                            target_cap_bytes as i64,
                        ) {
                            let kv_err: crate::rpcresp_kvresult_convert::msg_and_error::KvError =
                                e.into();
                            return MsgPack {
                                serialize_part:
                                    crate::rpcresp_kvresult_convert::FromError::from_error(&kv_err),
                                raw_bytes: Vec::new(),
                            };
                        }
                    }
                    drop(target_allocation);
                    (
                        target_cap_bytes,
                        KvRouteInfo {
                            node_id: node_id.clone(),
                            backing: KvReplicaBacking::CommittedSlot(CommittedSlotReplica {
                                owner_node_id: node_id.clone(),
                                grant_id: slot.grant_id,
                                slot_index: slot.slot_index,
                                slot_size: slot.slot_size,
                                addr: slot.addr,
                                len: slot.len,
                                base_addr: slot.base_addr,
                            }),
                            tomb_tag,
                        },
                        false,
                    )
                } else {
                    let target_cap_bytes = target_allocation.capcity();
                    if let Some(lease_id) = lease_id_opt {
                        if let Err(e) = view
                            .master_lease_manager()
                            .attach_key(lease_id, key.clone(), put_id)
                            .await
                        {
                            let kv_err: crate::rpcresp_kvresult_convert::msg_and_error::KvError =
                                e.into();
                            return MsgPack {
                                serialize_part:
                                    crate::rpcresp_kvresult_convert::FromError::from_error(&kv_err),
                                raw_bytes: Vec::new(),
                            };
                        }
                        if let Err(e) = view.master_kv_router().adjust_node_cache_reserved_capacity(
                            node_id.as_ref(),
                            ReservedCapacityReason::LeaseBoundKv,
                            target_cap_bytes as i64,
                        ) {
                            let kv_err: crate::rpcresp_kvresult_convert::msg_and_error::KvError =
                                e.into();
                            return MsgPack {
                                serialize_part:
                                    crate::rpcresp_kvresult_convert::FromError::from_error(&kv_err),
                                raw_bytes: Vec::new(),
                            };
                        }
                        let view_clone = view.clone();
                        let node_id_string = node_id.as_ref().to_string();
                        target_allocation.set_on_drop(move || {
                            if let Err(e) = view_clone
                                .master_kv_router()
                                .adjust_node_cache_reserved_capacity(
                                    &node_id_string,
                                    ReservedCapacityReason::LeaseBoundKv,
                                    -(target_cap_bytes as i64),
                                )
                            {
                                tracing::warn!(
                                    "Failed to restore moka capacity on drop: node_id={}, bytes={}, err={}",
                                    node_id_string,
                                    target_cap_bytes,
                                    e
                                );
                            }
                        });
                    }
                    (
                        target_cap_bytes,
                        KvRouteInfo {
                            node_id: node_id.clone(),
                            backing: KvReplicaBacking::Allocation(Arc::new(target_allocation)),
                            tomb_tag,
                        },
                        true,
                    )
                }
            }
            InflightPutAllocation::Remote {
                src: _src,
                mut target,
            } => {
                let target_cap_bytes = target.capcity();
                if let Some(lease_id) = lease_id_opt {
                    if let Err(e) = view
                        .master_lease_manager()
                        .attach_key(lease_id, key.clone(), put_id)
                        .await
                    {
                        let kv_err: crate::rpcresp_kvresult_convert::msg_and_error::KvError =
                            e.into();
                        return MsgPack {
                            serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(
                                &kv_err,
                            ),
                            raw_bytes: Vec::new(),
                        };
                    }
                    if let Err(e) = view.master_kv_router().adjust_node_cache_reserved_capacity(
                        node_id.as_ref(),
                        ReservedCapacityReason::LeaseBoundKv,
                        target_cap_bytes as i64,
                    ) {
                        let kv_err: crate::rpcresp_kvresult_convert::msg_and_error::KvError =
                            e.into();
                        return MsgPack {
                            serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(
                                &kv_err,
                            ),
                            raw_bytes: Vec::new(),
                        };
                    }
                    let view_clone = view.clone();
                    let node_id_string = node_id.as_ref().to_string();
                    target.set_on_drop(move || {
                        if let Err(e) = view_clone
                            .master_kv_router()
                            .adjust_node_cache_reserved_capacity(
                                &node_id_string,
                                ReservedCapacityReason::LeaseBoundKv,
                                -(target_cap_bytes as i64),
                            )
                        {
                            tracing::warn!(
                                "Failed to restore moka capacity on drop: node_id={}, bytes={}, err={}",
                                node_id_string,
                                target_cap_bytes,
                                e
                            );
                        }
                    });
                }
                (
                    target_cap_bytes,
                    KvRouteInfo {
                        node_id: node_id.clone(),
                        backing: KvReplicaBacking::Allocation(Arc::new(target)),
                        tomb_tag,
                    },
                    false,
                )
            }
            InflightPutAllocation::LocalCommittedSlot(slot) => {
                let target_cap_bytes = slot.len;
                if let Some(lease_id) = lease_id_opt {
                    if let Err(e) = view
                        .master_lease_manager()
                        .attach_key(lease_id, key.clone(), put_id)
                        .await
                    {
                        let kv_err: crate::rpcresp_kvresult_convert::msg_and_error::KvError =
                            e.into();
                        return MsgPack {
                            serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(
                                &kv_err,
                            ),
                            raw_bytes: Vec::new(),
                        };
                    }
                    if let Err(e) = view.master_kv_router().adjust_node_cache_reserved_capacity(
                        node_id.as_ref(),
                        ReservedCapacityReason::LeaseBoundKv,
                        target_cap_bytes as i64,
                    ) {
                        let kv_err: crate::rpcresp_kvresult_convert::msg_and_error::KvError =
                            e.into();
                        return MsgPack {
                            serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(
                                &kv_err,
                            ),
                            raw_bytes: Vec::new(),
                        };
                    }
                }
                (
                    target_cap_bytes,
                    KvRouteInfo {
                        node_id: node_id.clone(),
                        backing: KvReplicaBacking::CommittedSlot(slot),
                        tomb_tag,
                    },
                    false,
                )
            }
        };

        // NOTE on weight sizing for moka cache:
        // - moka's `weigher` returns a u32 per-entry weight while the cache's
        //   `max_capacity` and `weighted_size()` use u64. If an allocation's
        //   capacity exceeds u32::MAX (e.g., >= 4 GiB), a naive `as u32` cast
        //   would truncate and could become 0 for ~exact 4 GiB multiples.
        //   That would effectively disable size-based eviction because such
        //   entries would contribute 0 to the cache weight and the cache would
        //   never reach its configured capacity. This directly causes the
        //   observed "non‑lease mode eviction not working; puts fill to full".
        // - To make eviction robust, we saturate the per-entry weight at
        //   u32::MAX when `capcity()` is larger than u32::MAX. This keeps the
        //   cache accounting conservative (evicts earlier rather than later)
        //   and prevents weight=0 due to truncation.
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

        let saturated_weight_u32 = if target_cap_bytes > u32::MAX as u64 {
            tracing::warn!(
                "moka weight saturation: key={} put_id=({},{}) cap={}B exceeds u32::MAX; weight set to u32::MAX",
                key,
                put_id.0,
                put_id.1,
                target_cap_bytes
            );
            u32::MAX
        } else {
            target_cap_bytes as u32
        };
        // Note: moka cache insertion happens after commit in a spawned task
        // using the same saturated weight; avoid unused local here.
        // Insert into kv_routes with replica support
        let mut old_one_kv_routes: Option<Arc<OneKvNodesRoutes>> = None;
        let mut inserted = false;
        {
            let mut one_kv_routes = view
                .master_kv_router()
                .inner()
                .kv_routes
                .entry(key.clone())
                .or_insert_with(|| {
                    inserted = true;
                    Arc::new(OneKvNodesRoutes {
                        put_id,
                        lease_id: lease_id_opt,
                        nodes_replicas: RwLock::new(HashMap::new()),
                        get_durable_slots_used: AtomicU32::new(0),
                    })
                });
            // we need to take out old one_kv_routes if it is not inserted
            if !inserted {
                old_one_kv_routes = Some(one_kv_routes.clone());
                *one_kv_routes = Arc::new(OneKvNodesRoutes {
                    put_id,
                    lease_id: lease_id_opt,
                    nodes_replicas: RwLock::new(HashMap::new()),
                    get_durable_slots_used: AtomicU32::new(0),
                });
            }
            one_kv_routes
                .nodes_replicas
                .write()
                .insert(node_id.clone(), completed_info);
        }

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
        }

        if let Some(old) = old_one_kv_routes {
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

        // Post-commit maintenance: update prefix-count index (for CountPrefix RPC)
        // and, if applicable, update per-node cache controller. Run both in a
        // spawned task to keep the PutDone RPC path lean and consistent with
        // other async cache control operations. Deletion path already removes
        // the index entry in delete.rs (do_delete_one_kv_all_replicas).
        {
            let view_task = view.clone();
            let key_for_spawn = key.clone();
            let node_for_spawn = node_id.clone();
            let do_prefix_index_update = view.master_kv_router().prefix_index_enabled();
            let do_cache_insert =
                lease_id_opt.is_none() && view.master_kv_router().replica_cache_enabled();
            // Reuse the saturated weight computed above for moka insertion
            let cap_bytes_u32 = saturated_weight_u32;
            view.spawn("post_put_done_maintenance", async move {
                // 1) Update prefix-counting index
                if do_prefix_index_update {
                    let inner = view_task.master_kv_router().inner();
                    let mut tree = inner.prefix_index.write().await;
                    tree.insert(&key_for_spawn);
                }

                // 2) Optionally update node cache controller (non-leased keys)
                if do_cache_insert {
                    let cache = view_task
                        .master_kv_router()
                        .get_node_cache_controller(&node_for_spawn);
                    if let Some(cache) = cache {
                        let desc = NodeValueReplicaDesc {
                            weight_bytes: cap_bytes_u32,
                            put_id,
                        };
                        tracing::debug!("Inserting key: {:?} into cache", key_for_spawn);
                        cache.insert(key_for_spawn.clone(), desc);
                        tracing::debug!(
                            "Inserted key: {:?} into cache, current cache size: {}",
                            key_for_spawn,
                            cache.weighted_size()
                        );
                    } else {
                        tracing::warn!(
                            "No cache controller found for node: {}, node is not ready",
                            node_for_spawn
                        );
                    }
                }
            });
        }

        // Lease attach is handled before kv_routes insertion

        tracing::info!(
            "Completed put operation with put_id: {:?}, key: {:?}",
            put_id,
            key
        );
    } else {
        if let Some(slot) = req.serialize_part.committed_slot.clone() {
            let key = req.serialize_part.key.clone();
            let node_id = req_node_id;
            let Some(tomb_tag) = view.master_seg_manager().get_node_tomb_tag(&node_id) else {
                let err = msg_and_error::KvError::Api(
                    msg_and_error::ApiError::InvalidPutMasterState {
                        detail: format!(
                            "local-first put_done node tomb tag missing: key={} put_id=({},{}) node_id={}",
                            key, put_id.0, put_id.1, node_id
                        ),
                    },
                );
                return MsgPack {
                    serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                    raw_bytes: Vec::new(),
                };
            };
            if tomb_tag.is_tomb() {
                let err =
                    msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
                        detail: format!(
                            "local-first put_done node is tomb: key={} put_id=({},{}) node_id={}",
                            key, put_id.0, put_id.1, node_id
                        ),
                    });
                return MsgPack {
                    serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                    raw_bytes: Vec::new(),
                };
            }
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
            let target_cap_bytes = slot.len;
            if let Some(lease_id) = lease_id_opt {
                if let Err(e) = view
                    .master_lease_manager()
                    .attach_key(lease_id, key.clone(), put_id)
                    .await
                {
                    let kv_err: crate::rpcresp_kvresult_convert::msg_and_error::KvError = e.into();
                    return MsgPack {
                        serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(
                            &kv_err,
                        ),
                        raw_bytes: Vec::new(),
                    };
                }
                if let Err(e) = view.master_kv_router().adjust_node_cache_reserved_capacity(
                    node_id.as_ref(),
                    ReservedCapacityReason::LeaseBoundKv,
                    target_cap_bytes as i64,
                ) {
                    let kv_err: crate::rpcresp_kvresult_convert::msg_and_error::KvError = e.into();
                    return MsgPack {
                        serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(
                            &kv_err,
                        ),
                        raw_bytes: Vec::new(),
                    };
                }
            }
            let completed_info = KvRouteInfo {
                node_id: node_id.clone(),
                backing: KvReplicaBacking::CommittedSlot(CommittedSlotReplica {
                    owner_node_id: node_id.clone(),
                    grant_id: slot.grant_id,
                    slot_index: slot.slot_index,
                    slot_size: slot.slot_size,
                    addr: slot.addr,
                    len: slot.len,
                    base_addr: slot.base_addr,
                }),
                tomb_tag,
            };
            return publish_completed_put_route(
                view,
                key,
                put_id,
                lease_id_opt,
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

pub async fn handle_put_append_start(
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
    let current_route_still_needs_remote = route_snapshot
        .as_ref()
        .map(|route| current_route_needs_remote_replica(route, &req_node_id, put_id))
        .unwrap_or(false);
    if !current_route_still_needs_remote {
        let _ = view
            .master_kv_router()
            .inner()
            .inflight_replica_tasks
            .remove(&(key.clone(), put_id.0, put_id.1))
            .await;
        return MsgPack {
            serialize_part: PutAppendStartResp {
                scheduled: false,
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
                        scheduled: false,
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
            scheduled: true,
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

pub async fn handle_put_append_revoke(
    view: MasterKvRouterView,
    req: MsgPack<PutAppendRevokeReq>,
) -> MsgPack<PutAppendRevokeResp> {
    let put_id = req.serialize_part.put_id;
    let key = req.serialize_part.key;
    let _ = view
        .master_kv_router()
        .inner()
        .inflight_replica_tasks
        .remove(&(key, put_id.0, put_id.1))
        .await;
    MsgPack {
        serialize_part: PutAppendRevokeResp {
            error_code: msg_and_error::OK,
            error_json: String::new(),
        },
        raw_bytes: Vec::new(),
    }
}

pub async fn handle_put_append_done(
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
    let appended = append_current_route_replica_if_matching(
        &view,
        &key,
        inflight.put_id,
        inflight.node_id,
        allocation,
    );
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
