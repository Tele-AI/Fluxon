use super::{
    InflightPutAllocation, InflightPutInfo, KvRouteInfo, MasterKvRouterView, PutPlacementMode,
    msg_pack::{
        PutDoneReq, PutDoneResp, PutRevokeReq, PutRevokeResp, PutStartReq, PutStartResp,
        SsdReplicaCommitReq, SsdReplicaCommitResp,
    },
    placement::PutPlacementTarget,
};
use super::{KvSsdRouteInfo, NodeValueReplicaDesc};
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
use parking_lot::RwLock;
use rand::seq::SliceRandom;
use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicU32},
    time::Duration,
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

pub async fn handle_put_start(
    view: MasterKvRouterView,
    req: MsgPack<PutStartReq>,
    req_node_id: NodeID,
) -> (PutIDForAKey, MsgPack<PutStartResp>) {
    let key = req.serialize_part.key.clone();
    if let Err(err) = view
        .master_kv_router()
        .reserve_inflight_put_key(&key, req.serialize_part.reject_if_inflight_same_key)
    {
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

    let finalize = |node_id: NodeID,
                    inflight_alloc: InflightPutAllocation,
                    src_addr: u64,
                    target_addr: u64,
                    src_base_addr: u64,
                    target_base_addr: u64,
                    len: u64| {
        let info = InflightPutInfo {
            node_id: node_id.clone(),
            key: key.clone(),
            len,
            req_node_id: req_node_id.clone(),
            src_target_allocation: Arc::new(Mutex::new(Some(inflight_alloc))),
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

            let resp = PutStartResp {
                put_id,
                node_id: node_id.into(),
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
            let fut = finalize(
                node_id,
                InflightPutAllocation::Local(src),
                abs,
                abs,
                src_base,
                src_base,
                allocation_size,
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

fn spawn_ssd_replica_persist_request(
    view: &MasterKvRouterView,
    key: String,
    put_id: PutIDForAKey,
    node_id: NodeID,
    len: u64,
    allocation: Arc<Allocation>,
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
) -> MsgPack<SsdReplicaCommitResp> {
    let req = req.serialize_part;
    let node_id: NodeID = req.node_id.clone().into();
    let Some(route_ref) = view.master_kv_router().inner().kv_routes.get(&req.key) else {
        tracing::debug!(
            "Ignoring SSD replica commit for missing key: key={} put_id=({},{}) node={}",
            req.key,
            req.put_id.0,
            req.put_id.1,
            req.node_id
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
            req.node_id
        );
        return ok_ssd_replica_commit_resp();
    }

    let tomb_tag = {
        let replicas = route.nodes_replicas.read();
        let Some(memory_replica) = replicas.get(&node_id) else {
            tracing::debug!(
                "Ignoring SSD replica commit without matching memory replica: key={} put_id=({},{}) node={}",
                req.key,
                req.put_id.0,
                req.put_id.1,
                req.node_id
            );
            return ok_ssd_replica_commit_resp();
        };
        memory_replica.tomb_tag.clone()
    };

    if tomb_tag.is_tomb() {
        tracing::debug!(
            "Ignoring SSD replica commit for tombed node: key={} put_id=({},{}) node={}",
            req.key,
            req.put_id.0,
            req.put_id.1,
            req.node_id
        );
        return ok_ssd_replica_commit_resp();
    }

    route.ssd_replicas.write().insert(
        node_id.clone(),
        KvSsdRouteInfo {
            node_id,
            len: req.len,
            tomb_tag,
        },
    );
    tracing::debug!(
        "Committed SSD replica route: key={} put_id=({},{}) node={} len={}",
        req.key,
        req.put_id.0,
        req.put_id.1,
        req.node_id,
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

    // Remove from inflight_puts and store in completed_puts
    if let Some(InflightPutInfo {
        node_id,
        key,
        len,
        src_target_allocation,
        ..
    }) = view
        .master_kv_router()
        .inner()
        .inflight_puts
        .remove(&full_put_id)
        .await
    {
        view.master_kv_router().release_inflight_put_key(&key);
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
        // Note: moka cache insertion happens after commit in a spawned task
        // using the same saturated weight; avoid unused local here.
        // If lease is provided, attach first and fail fast on error
        if let Some(lease_id) = lease_id_opt {
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
            // Reserve cache capacity on this node for the leased allocation now (fetch_sub semantics)
            if let Err(e) = view
                .master_kv_router()
                .adjust_node_cache_capacity_for_lease(node_id.as_ref(), target_cap_bytes as i64)
            {
                let kv_err: crate::rpcresp_kvresult_convert::msg_and_error::KvError = e.into();
                return MsgPack {
                    serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&kv_err),
                    raw_bytes: Vec::new(),
                };
            }
            // And attach an on-drop hook to restore it (fetch_add on Allocation drop)
            let view_clone = view.clone();
            let node_id_string = node_id.as_ref().to_string();
            target_allocation.set_on_drop(move || {
                match view_clone.try_adjust_node_cache_capacity_for_lease(
                    &node_id_string,
                    -(target_cap_bytes as i64),
                ) {
                    Some(Ok(())) => {}
                    Some(Err(e)) => {
                        tracing::warn!(
                            "Failed to restore moka capacity on drop: node_id={}, bytes={}, err={}",
                            node_id_string,
                            target_cap_bytes,
                            e
                        );
                    }
                    None => {
                        tracing::debug!(
                            "Skipped restoring moka capacity on drop because MasterKvRouterView is gone: node_id={}, bytes={}",
                            node_id_string,
                            target_cap_bytes
                        );
                    }
                }
            });
        }

        let completed_info = KvRouteInfo {
            node_id: node_id.clone(),
            allocation: Arc::new(target_allocation),
            tomb_tag: tomb_tag.clone(),
        };
        let target_allocation_for_ssd = Arc::clone(&completed_info.allocation);

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
                        ssd_replicas: RwLock::new(HashMap::new()),
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
                    ssd_replicas: RwLock::new(HashMap::new()),
                    get_durable_slots_used: AtomicU32::new(0),
                });
            }
            one_kv_routes
                .nodes_replicas
                .write()
                .insert(node_id.clone(), completed_info);
        }

        spawn_ssd_replica_persist_request(
            &view,
            key.clone(),
            put_id,
            node_id.clone(),
            len,
            target_allocation_for_ssd,
        );

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
            let _ = view.spawn("post_put_done_maintenance", async move {
                // 1) Update the derived prefix-counting index asynchronously.
                //    This keeps PutDone lean, but also means CountPrefix visibility
                //    is not an immediate strong-consistency signal for this put.
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
