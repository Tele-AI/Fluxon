use super::NodeValueReplicaDesc;
use super::{
    InflightPutAllocation, InflightPutInfo, InflightPutKeyReservation, MasterKvRouterView,
    NodeCacheCapacityReservation, PutPlacementMode, SsdReplicaCommitStatus,
    msg_pack::{
        PutDoneReq, PutDoneResp, PutRevokeReq, PutRevokeResp, PutStartReq, PutStartResp,
        SsdReplicaCommitReq, SsdReplicaCommitResp,
    },
    placement::PutPlacementTarget,
};
use crate::client_kv_api::msg_pack::SsdReplicaPersistReq;
use crate::master_kv_router::OneKvNodesRoutes;
use crate::master_kv_router::delete::DeleteKeyInfo;
use crate::{
    cluster_manager::{META_KEY_LOCAL_IPC_ROOT, NodeID},
    master_seg_manager::one_seg_allocator::Allocation,
    p2p::msg_pack::{MsgPack, RPCCaller},
    rpcresp_kvresult_convert::msg_and_error,
};
use fluxon_commu::{META_KEY_SHARED_STORAGE_NODE_ID, META_KEY_SHARED_STORAGE_NODE_START_TIME};
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use std::{sync::Arc, time::Duration};

pub type PutIDForAKey = (u64, u32);

fn validate_put_start_source_node_override(
    view: &MasterKvRouterView,
    requester_node_id: &NodeID,
    source_node_id: &NodeID,
) -> msg_and_error::KvResult<(i64, i64)> {
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
    if requester_node_id == source_node_id {
        return Ok((requester.node_start_time, requester.node_start_time));
    }

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

    Ok((requester.node_start_time, source.node_start_time))
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
        req.serialize_part.reject_if_exists,
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
    let mut key_reservation =
        InflightPutKeyReservation::from_existing(view.master_kv_router(), key.clone());
    let source_node_id = match req.serialize_part.source_node_id.as_ref() {
        Some(source_node_id) => source_node_id.clone().into(),
        None => req_node_id.clone(),
    };
    let (requester_generation, source_generation) =
        match validate_put_start_source_node_override(&view, &req_node_id, &source_node_id) {
            Ok(generations) => generations,
            Err(err) => {
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
        };
    let put_id: PutIDForAKey = view
        .master_kv_router()
        .get_recent_key_versionid(key.clone());

    let inflight_put_key: (String, u64, u32) = (key.clone(), put_id.0, put_id.1);

    // randomly select one src_allocator
    let src_allocation = {
        let src_node_allocators = view
            .master_seg_manager()
            .get_node_allocators(&source_node_id);
        if src_node_allocators.is_empty() {
            tracing::warn!(
                "No allocators found for put_start source node: requester={} source={}",
                req_node_id,
                source_node_id
            );
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::RegisterSegmentFailed {
                detail: format!(
                    "put_start source node has no registered segments: requester={} source={}",
                    req_node_id, source_node_id
                ),
            });
            let resp: PutStartResp = crate::rpcresp_kvresult_convert::FromError::from_error(&err);
            return (
                (0, 0),
                MsgPack {
                    serialize_part: resp,
                    raw_bytes: Vec::new(),
                },
            );
        }

        let src_allocator = src_node_allocators.choose(&mut rand::thread_rng()).unwrap();

        let mut allocated_addr: Option<Allocation> = None;
        for attempt in 1..=3 {
            if let Ok(allocation) = src_allocator.allocate(req.serialize_part.len) {
                allocated_addr = Some(allocation);
                break;
            } else {
                tracing::warn!(
                    "Allocation attempt {}/3 failed for put_id {:?}",
                    attempt,
                    put_id
                );
            }
        }
        if allocated_addr.is_none() {
            let total = src_allocator.total_size_bytes();
            let used = src_allocator.used_size_bytes();
            let free = total.saturating_sub(used);
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::NoSpace {
                node: source_node_id.as_ref().to_string(),
                segment: src_allocator.seg_device_id.clone(),
                total_capacity: total,
                free_capacity: free,
            });
            let resp: PutStartResp = crate::rpcresp_kvresult_convert::FromError::from_error(&err);
            return (
                (0, 0),
                MsgPack {
                    serialize_part: resp,
                    raw_bytes: Vec::new(),
                },
            );
        }
        allocated_addr.unwrap()
    };

    // Keep src allocation alive across retry attempts until we have a successful target.
    let mut src_allocation = Some(src_allocation);

    let finalize = |target_node_id: NodeID,
                    target_generation: i64,
                    persist_to_ssd: bool,
                    inflight_alloc: InflightPutAllocation,
                    src_addr: u64,
                    target_addr: u64,
                    src_base_addr: u64,
                    target_base_addr: u64,
                    len: u64| {
        let info = Arc::new(InflightPutInfo {
            target_node_id: target_node_id.clone(),
            source_node_id: source_node_id.clone(),
            key: key.clone(),
            len,
            req_node_id: req_node_id.clone(),
            target_generation,
            source_generation,
            requester_generation,
            persist_to_ssd,
            src_target_allocation: Arc::new(Mutex::new(Some(inflight_alloc))),
        });

        let view_task = view.clone();
        let inflight_put_key = inflight_put_key.clone();
        async move {
            if let Err(detail) = view_task
                .master_kv_router()
                .insert_inflight_put(inflight_put_key, info)
                .await
            {
                let err =
                    msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
                        detail,
                    });
                return (
                    (0, 0),
                    MsgPack {
                        serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(
                            &err,
                        ),
                        raw_bytes: Vec::new(),
                    },
                );
            }

            let resp = PutStartResp {
                put_id,
                node_id: target_node_id.into(),
                src_addr,
                target_addr,
                src_base_addr,
                target_base_addr,
                len,
                error_code: msg_and_error::OK,
                error_json: String::new(),
                server_process_us: 0,
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

    let put_target = view
        .master_kv_router()
        .inner()
        .policy
        .select_put_target(
            &view,
            &source_node_id,
            req.serialize_part.preferred_sub_cluster.as_deref(),
            req.serialize_part.len,
        )
        .await;

    match put_target {
        Ok(PutPlacementTarget::Local {
            node_id,
            persist_to_ssd,
        }) => {
            if node_id != source_node_id {
                unreachable!(
                    "Local placement must be the resolved source node; got node_id={} source_node_id={} requester_node_id={}",
                    node_id, source_node_id, req_node_id
                );
            }

            tracing::debug!(
                "put_start placement decided: local; put_id={:?} key={} requester_node_id={} source_node_id={} target_node_id={} preferred_sub_cluster={:?} len={} persist_to_ssd={}",
                put_id,
                key,
                req_node_id,
                source_node_id,
                node_id,
                req.serialize_part.preferred_sub_cluster,
                req.serialize_part.len,
                persist_to_ssd
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
            let fut = finalize(
                node_id,
                source_generation,
                persist_to_ssd,
                InflightPutAllocation::Local(src),
                abs,
                abs,
                src_base,
                src_base,
                allocation_size,
            );
            let result = fut.await;
            if result.0 != (0, 0) {
                key_reservation.disarm();
            }
            return result;
        }
        Ok(PutPlacementTarget::Remote {
            node_id,
            allocation: target_allocation,
            persist_to_ssd,
            ..
        }) => {
            let Some(target_generation) = view
                .cluster_manager()
                .get_member_info_cached(node_id.as_ref())
                .map(|member| member.node_start_time)
            else {
                let err =
                    msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
                        detail: format!("selected PUT target departed before admission: {node_id}"),
                    });
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
            let src_ref = src_allocation
                .as_ref()
                .expect("src_allocation must exist until put_start returns");

            let src_offset = src_ref.addr();
            let src_base = src_ref.base_addr();
            let target_offset = target_allocation.addr();
            let target_base = target_allocation.base_addr();
            let allocation_size = target_allocation.size();

            tracing::debug!(
                "put_start placement decided: remote; put_id={:?} key={} requester_node_id={} source_node_id={} target_node_id={} preferred_sub_cluster={:?} len={} target_base_addr={} target_offset={} allocation_size={} persist_to_ssd={}",
                put_id,
                key,
                req_node_id,
                source_node_id,
                node_id,
                req.serialize_part.preferred_sub_cluster,
                req.serialize_part.len,
                target_base,
                target_offset,
                allocation_size,
                persist_to_ssd
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
                node_id,
                target_generation,
                persist_to_ssd,
                InflightPutAllocation::Remote {
                    src,
                    target: target_allocation,
                },
                src_base + src_offset,
                target_base + target_offset,
                src_base,
                target_base,
                allocation_size,
            );
            let result = fut.await;
            if result.0 != (0, 0) {
                key_reservation.disarm();
            }
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

pub async fn handle_put_revoke(
    view: MasterKvRouterView,
    req: MsgPack<PutRevokeReq>,
) -> MsgPack<PutRevokeResp> {
    tracing::debug!("Handling PutRevokeReq: {:?}", req.serialize_part);

    let (put_time_ms, put_version) = req.serialize_part.put_id;

    let kvrouter_key = (req.serialize_part.key, put_time_ms, put_version);
    if view
        .master_kv_router()
        .cancel_inflight_put(&kvrouter_key)
        .await
        .is_some()
    {
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

pub(super) fn insert_memory_replica_into_cache_if_current(
    view: &MasterKvRouterView,
    key: &str,
    put_id: PutIDForAKey,
    node_id: &NodeID,
    generation: i64,
    allocation: &Arc<Allocation>,
    weight_bytes: u32,
) {
    let Some(route_ref) = view.master_kv_router().inner().kv_routes.get(key) else {
        tracing::debug!(
            "Skipping delayed cache insertion for missing key: key={} put_id=({},{}) node={}",
            key,
            put_id.0,
            put_id.1,
            node_id
        );
        return;
    };
    let route = route_ref.value().clone();
    drop(route_ref);

    if route.put_id != put_id
        || !route.has_memory_replica_allocation(node_id, generation, allocation)
    {
        tracing::debug!(
            "Skipping delayed cache insertion for stale replica: key={} put_id=({},{}) node={}",
            key,
            put_id.0,
            put_id.1,
            node_id
        );
        return;
    }

    let Some(cache) = view
        .master_kv_router()
        .get_node_cache_controller(node_id.as_ref())
    else {
        tracing::warn!(
            "No cache controller found for node: {}, node is not ready",
            node_id
        );
        return;
    };
    let desc = NodeValueReplicaDesc {
        weight_bytes,
        put_id,
        generation,
        allocation: Arc::downgrade(allocation),
    };
    tracing::debug!("Inserting key: {:?} into cache", key);
    cache.insert(key.to_string(), desc);
    tracing::debug!(
        "Inserted key: {:?} into cache, current cache size: {}",
        key,
        cache.weighted_size()
    );
}

fn spawn_ssd_replica_persist_request(
    view: &MasterKvRouterView,
    key: String,
    put_id: PutIDForAKey,
    node_id: NodeID,
    generation: i64,
    len: u64,
    allocation: Arc<Allocation>,
    cache_weight_bytes: Option<u32>,
    pending_persist_reservation: Option<NodeCacheCapacityReservation>,
) {
    let target_addr = allocation.base_addr() + allocation.addr();
    let view = view.clone();
    let view_task = view.clone();
    let _ = view.spawn("post_put_ssd_replica_persist", async move {
        let _allocation_guard = allocation;
        let req = MsgPack {
            serialize_part: SsdReplicaPersistReq {
                key: key.clone(),
                put_id,
                target_addr,
                len,
            },
            raw_bytes: Vec::new(),
        };
        let resp = RPCCaller::<SsdReplicaPersistReq>::new()
            .call(
                view_task.p2p_module(),
                node_id.clone(),
                req,
                Some(Duration::from_secs(60)),
                2,
            )
            .await;
        match resp {
            Ok(resp) => {
                if let Err(err) = crate::rpcresp_kvresult_convert::try_from_code(
                    resp.serialize_part.error_code,
                    resp.serialize_part.error_json,
                ) {
                    tracing::warn!(
                        "SSD replica persist failed: key={} put_id=({},{}) node={} err={}",
                        key,
                        put_id.0,
                        put_id.1,
                        node_id,
                        err
                    );
                } else if resp.serialize_part.persisted {
                    tracing::debug!(
                        "SSD replica persist completed: key={} put_id=({},{}) node={}",
                        key,
                        put_id.0,
                        put_id.1,
                        node_id
                    );
                } else {
                    tracing::debug!(
                        "SSD replica persist skipped because owner has no SSD store: key={} put_id=({},{}) node={}",
                        key,
                        put_id.0,
                        put_id.1,
                        node_id
                    );
                }
            }
            Err(err) => {
                tracing::warn!(
                    "SSD replica persist RPC failed: key={} put_id=({},{}) node={} err={:?}",
                    key,
                    put_id.0,
                    put_id.1,
                    node_id,
                    err
                );
            }
        }

        // The allocation becomes Moka-managed only after the persist attempt.
        // Release its temporary reservation before inserting the same bytes.
        drop(pending_persist_reservation);
        if let Some(weight_bytes) = cache_weight_bytes {
            insert_memory_replica_into_cache_if_current(
                &view_task,
                &key,
                put_id,
                &node_id,
                generation,
                &_allocation_guard,
                weight_bytes,
            );
        }
    });
}

fn ok_ssd_replica_commit_resp() -> MsgPack<SsdReplicaCommitResp> {
    MsgPack {
        serialize_part: SsdReplicaCommitResp {
            error_code: msg_and_error::OK,
            error_json: String::new(),
            server_process_us: 0,
        },
        raw_bytes: Vec::new(),
    }
}

pub async fn handle_ssd_replica_commit(
    view: MasterKvRouterView,
    req: MsgPack<SsdReplicaCommitReq>,
    req_node_id: NodeID,
) -> MsgPack<SsdReplicaCommitResp> {
    let req = req.serialize_part;
    let Some(route_ref) = view.master_kv_router().inner().kv_routes.get(&req.key) else {
        tracing::debug!(
            "Ignoring SSD replica commit for missing key: key={} put_id=({},{}) node={}",
            req.key,
            req.put_id.0,
            req.put_id.1,
            req_node_id
        );
        return ok_ssd_replica_commit_resp();
    };
    let route = route_ref.value().clone();
    drop(route_ref);

    if route.put_id != req.put_id {
        tracing::debug!(
            "Ignoring stale SSD replica commit: key={} req_put_id=({},{}) current_put_id=({},{}) node={}",
            req.key,
            req.put_id.0,
            req.put_id.1,
            route.put_id.0,
            route.put_id.1,
            req_node_id
        );
        return ok_ssd_replica_commit_resp();
    }

    match route.commit_ssd_replica(&req_node_id, req.len) {
        SsdReplicaCommitStatus::MissingMemory => {
            tracing::debug!(
                "Ignoring SSD replica commit without matching memory replica: key={} put_id=({},{}) node={}",
                req.key,
                req.put_id.0,
                req.put_id.1,
                req_node_id
            );
            return ok_ssd_replica_commit_resp();
        }
        SsdReplicaCommitStatus::TombedNode => {
            tracing::debug!(
                "Ignoring SSD replica commit for tombed node: key={} put_id=({},{}) node={}",
                req.key,
                req.put_id.0,
                req.put_id.1,
                req_node_id
            );
            return ok_ssd_replica_commit_resp();
        }
        SsdReplicaCommitStatus::Committed => {}
    }
    tracing::debug!(
        "Committed SSD replica route: key={} put_id=({},{}) node={} len={}",
        req.key,
        req.put_id.0,
        req.put_id.1,
        req_node_id,
        req.len
    );
    ok_ssd_replica_commit_resp()
}

pub async fn handle_put_done(
    view: MasterKvRouterView,
    req: MsgPack<PutDoneReq>,
) -> MsgPack<PutDoneResp> {
    tracing::debug!("Handling PutDoneReq: {:?}", req.serialize_part);

    let put_id = req.serialize_part.put_id;
    let lease_id_opt = req.serialize_part.lease_id;
    let full_put_id: (String, u64, u32) = (req.serialize_part.key.clone(), put_id.0, put_id.1);

    if let Some((inflight_info, key_reservation)) = view
        .master_kv_router()
        .take_active_inflight_put(&full_put_id)
        .await
    {
        let node_id = inflight_info.target_node_id.clone();
        let node_generation = inflight_info.target_generation;
        let key = inflight_info.key.clone();
        let len = inflight_info.len;
        let persist_to_ssd = inflight_info.persist_to_ssd;
        let src_target_allocation = Arc::clone(&inflight_info.src_target_allocation);
        let Some(allocs) = src_target_allocation.lock().take() else {
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

        let mut target_allocation = match allocs {
            InflightPutAllocation::Local(target) => target,
            InflightPutAllocation::Remote { src: _src, target } => target,
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

        let target_generation_is_current = view
            .cluster_manager()
            .get_member_info_cached(node_id.as_ref())
            .is_some_and(|member| member.node_start_time == node_generation);
        if !target_generation_is_current {
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
                detail: format!(
                    "Put operation with put_id {:?} targets departed or replaced node {} generation {}",
                    put_id, node_id, node_generation
                ),
            });
            return MsgPack {
                serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                raw_bytes: Vec::new(),
            };
        }

        let target_cap_bytes = target_allocation.capcity();
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
        let mut pending_persist_reservation = None;
        if let Some(lease_id) = lease_id_opt {
            let lease_cache_reservation = match view
                .master_kv_router()
                .reserve_node_cache_capacity(node_id.as_ref(), target_cap_bytes)
            {
                Ok(reservation) => reservation,
                Err(e) => {
                    let kv_err: crate::rpcresp_kvresult_convert::msg_and_error::KvError = e.into();
                    return MsgPack {
                        serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(
                            &kv_err,
                        ),
                        raw_bytes: Vec::new(),
                    };
                }
            };
            if let Err(e) = view
                .master_lease_manager()
                .attach_key(lease_id, key.clone(), put_id)
                .await
            {
                let kv_err: crate::rpcresp_kvresult_convert::msg_and_error::KvError = e.into();
                return MsgPack {
                    serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&kv_err),
                    raw_bytes: Vec::new(),
                };
            }
            target_allocation.set_on_drop(move || drop(lease_cache_reservation));
        } else {
            pending_persist_reservation = match view
                .master_kv_router()
                .reserve_node_cache_capacity(node_id.as_ref(), target_cap_bytes)
            {
                Ok(reservation) => reservation,
                Err(e) => {
                    let kv_err: crate::rpcresp_kvresult_convert::msg_and_error::KvError = e.into();
                    return MsgPack {
                        serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(
                            &kv_err,
                        ),
                        raw_bytes: Vec::new(),
                    };
                }
            };
        }

        let target_allocation = Arc::new(target_allocation);
        let target_allocation_for_ssd = persist_to_ssd.then(|| Arc::clone(&target_allocation));

        // Insert into kv_routes with replica support
        let mut old_one_kv_routes: Option<Arc<OneKvNodesRoutes>> = None;
        let mut inserted = false;
        let mut failed_new_route: Option<Arc<OneKvNodesRoutes>> = None;
        let replica_published;
        {
            let mut one_kv_routes = view
                .master_kv_router()
                .inner()
                .kv_routes
                .entry(key.clone())
                .or_insert_with(|| {
                    inserted = true;
                    Arc::new(OneKvNodesRoutes::new(put_id, lease_id_opt))
                });
            // we need to take out old one_kv_routes if it is not inserted
            if !inserted {
                old_one_kv_routes = Some(one_kv_routes.clone());
                *one_kv_routes = Arc::new(OneKvNodesRoutes::new(put_id, lease_id_opt));
            }
            replica_published = view.master_kv_router().publish_memory_replica(
                &key,
                &one_kv_routes,
                node_id.clone(),
                node_generation,
                Arc::clone(&target_allocation),
                tomb_tag.clone(),
            );
            if !replica_published {
                if let Some(old) = old_one_kv_routes.as_ref() {
                    *one_kv_routes = Arc::clone(old);
                } else {
                    failed_new_route = Some(one_kv_routes.clone());
                }
            }
        }
        if let Some(failed_route) = failed_new_route {
            view.master_kv_router()
                .inner()
                .kv_routes
                .remove_if(&key, |_, current| Arc::ptr_eq(current, &failed_route));
        }
        if !replica_published {
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidPutMasterState {
                detail: format!(
                    "Put operation with put_id {:?} lost member generation {} before route publication",
                    put_id, node_generation
                ),
            });
            return MsgPack {
                serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                raw_bytes: Vec::new(),
            };
        }
        // Publish the route before releasing admission so reject-if-exists observes either
        // the inflight reservation or the committed route, with no visibility gap.
        drop(key_reservation);

        let cache_weight_bytes = (lease_id_opt.is_none()
            && view.master_kv_router().replica_cache_enabled())
        .then_some(saturated_weight_u32);
        if let Some(target_allocation_for_ssd) = target_allocation_for_ssd {
            spawn_ssd_replica_persist_request(
                &view,
                key.clone(),
                put_id,
                node_id.clone(),
                node_generation,
                len,
                target_allocation_for_ssd,
                cache_weight_bytes,
                pending_persist_reservation,
            );
        } else {
            drop(pending_persist_reservation);
            if let Some(weight_bytes) = cache_weight_bytes {
                insert_memory_replica_into_cache_if_current(
                    &view,
                    &key,
                    put_id,
                    &node_id,
                    node_generation,
                    &target_allocation,
                    weight_bytes,
                );
            }
        }

        if let Some(old) = old_one_kv_routes {
            view.master_kv_router()
                .unregister_route_replicas(&key, &old);
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

        // Update the prefix-count index asynchronously to keep PutDone lean.
        // SSD-backed targets delay cache insertion until the persist request completes.
        {
            let view_task = view.clone();
            let key_for_spawn = key.clone();
            let do_prefix_index_update = view.master_kv_router().prefix_index_enabled();
            let _ = view.spawn("post_put_done_maintenance", async move {
                if do_prefix_index_update {
                    let inner = view_task.master_kv_router().inner();
                    let mut tree = inner.prefix_index.write().await;
                    if inner.kv_routes.contains_key(&key_for_spawn) {
                        tree.insert(&key_for_spawn);
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
        },
        raw_bytes: Vec::new(),
    }
}
