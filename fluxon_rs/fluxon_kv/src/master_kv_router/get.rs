use super::{
    InflightGetInfo, KvRouteInfo, MasterKvRouterView, NodeValueReplicaDesc, OwnerHoldingGetInfo,
    msg_pack::{
        BatchGetDoneItemResp, BatchGetDoneReq, BatchGetDoneResp, BatchGetRevokeItemResp,
        BatchGetRevokeReq, BatchGetRevokeResp, BatchGetStartItemResp, BatchGetStartReq,
        BatchGetStartResp, BatchIsExistReq, BatchIsExistResp, GetAllocationMode, GetDoneReq,
        GetDoneResp, GetMetaReq, GetMetaResp, GetRevokeReq, GetRevokeResp, GetStartReq,
        GetStartResp, MemHolderKeepAliveReq, MemHolderKeepAliveResp, MemHolderReleaseReq,
        MemHolderReleaseResp,
    },
};
use crate::master_kv_router::OneKvNodesRoutes;
use crate::master_kv_router::put::PutIDForAKey;
use crate::memholder::MemholderManagerTrait;
use crate::{
    cluster_manager::NodeID,
    master_seg_manager::{MasterSegManagerAccessTrait, one_seg_allocator::Allocation},
    p2p::msg_pack::MsgPack,
    rpcresp_kvresult_convert::msg_and_error::{self, kv},
};
use dashmap::DashMap;
use limit_thirdparty::tokio;
use rand::Rng;
use rand::seq::SliceRandom;
use std::collections::HashSet;
use std::{
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
};

fn update_moka_for_node(
    view: MasterKvRouterView,
    node_id: String,
    key: String,
    weight: u32,
    put_id: PutIDForAKey,
    new_inserted: bool,
) {
    if !view.master_kv_router().replica_cache_enabled() {
        return;
    }
    let view_task = view.clone();
    view.spawn("update_moka_for_node", async move {
        if let Some(cache) = view_task
            .master_kv_router()
            .get_node_cache_controller(&node_id)
        {
            if new_inserted {
                cache.insert(
                    key.clone(),
                    NodeValueReplicaDesc {
                        weight_bytes: weight,
                        put_id,
                    },
                );
                tracing::debug!(
                    "Inserted key: {:?} into node cache: {}, weight={}",
                    key,
                    node_id,
                    weight
                );
            } else {
                let _ = cache.get(&key);
                tracing::debug!(
                    "Touched key: {:?} on node cache: {} (TTL refresh)",
                    key,
                    node_id
                );
            }
        } else {
            tracing::warn!(
                "No cache controller found for node: {} when updating moka",
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

    let get_id = view
        .master_kv_router()
        .inner()
        .next_get_id
        .fetch_add(1, Ordering::Relaxed);

    let one_kv_nodes_routes: Arc<OneKvNodesRoutes> = if let Some(one_kv_nodes_routes) = view
        .master_kv_router()
        .inner()
        .kv_routes
        .get(&req.serialize_part.key)
    {
        one_kv_nodes_routes.clone()
    } else {
        // Key not found
        tracing::info!("Key not found: {}", req.serialize_part.key);
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::KeyNotFound {
            key: req.serialize_part.key.clone(),
        });
        return failed_resp_err(err, None, &view, &req.serialize_part.key);
    };

    let replicas: HashMap<NodeID, KvRouteInfo> = one_kv_nodes_routes.nodes_replicas.read().clone();
    // Currently we are holding the lock with `replicas`
    // 选择一个replica (这里可以实现更复杂的选择逻辑)
    let mut replica_keys = replicas.keys().collect::<Vec<_>>();
    let mut tombs = HashSet::new();
    let mut target_allocations = None;
    let mut allocation_mode = GetAllocationMode::Temporary;
    for _ in 0..replicas.len() {
        let to_remove_idx = rand::thread_rng().gen_range(0..replica_keys.len());
        let selected_replica_key = replica_keys.remove(to_remove_idx);
        let selected_replica = replicas.get(&*selected_replica_key).unwrap();
        if selected_replica.tomb_tag.is_tomb() {
            tombs.insert(selected_replica_key.to_owned());
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
            || -> Result<Arc<Allocation>, (u64, MsgPack<GetStartResp>)> {
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
                if one_kv_nodes_routes.try_reserve_get_durable_slot() {
                    allocation_mode = GetAllocationMode::DurableReplica;
                } else {
                    allocation_mode = GetAllocationMode::Temporary;
                }
                Ok(Arc::new(target_allocation))
            };

        // 为get调用方分配接收内存作为传输target
        if target_allocations.is_none() {
            target_allocations = Some(
                if let Some(replica_on_recv_node) = replicas.get(&req_node_id) {
                    match &replica_on_recv_node.backing {
                        super::KvReplicaBacking::Allocation(allocation) => {
                            allocation_mode = GetAllocationMode::ReuseReplica;
                            allocation.clone()
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

        let target_allocation = target_allocations.unwrap();

        // Convert to absolute addresses for Mooncake (requires absolute)
        // Use allocation's allocator base directly
        let target_base = target_allocation.base_addr();

        // If we reuse existing target on requesting node, declare src=target on req node
        let (resp_node_id, resp_src_addr, resp_target_addr, resp_src_base, resp_target_base) =
            if allocation_mode == GetAllocationMode::ReuseReplica {
                let addr = target_base + target_allocation.addr();
                // both src/target are on requesting node's allocation in this reuse case
                (req_node_id.clone(), addr, addr, target_base, target_base)
            } else {
                (
                    src_node_id.clone(),
                    src_abs_addr,
                    target_base + target_allocation.addr(),
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
            error_code: msg_and_error::OK,
            error_json: String::new(),
            server_process_us: 0,
        };
        // 创建在途的Get操作信息
        let info = InflightGetInfo {
            put_id: one_kv_nodes_routes.put_id,
            src_node_id: src_node_id.clone(),
            key: req.serialize_part.key.clone(),
            req_node_id,
            len: src_len,
            allocation: target_allocation, // 存储target allocation
            route: one_kv_nodes_routes.clone(),
            allocation_mode,
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
            update_moka_for_node(
                view.clone(),
                src_node_id.to_string(),
                req.serialize_part.key.clone(),
                0,
                one_kv_nodes_routes.put_id,
                false,
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
    tracing::info!("Key not found: {}", req.serialize_part.key);
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
) -> MsgPack<GetRevokeResp> {
    tracing::debug!("Handling GetRevokeReq: {:?}", req.serialize_part);

    let get_id = req.serialize_part.get_id;

    // Remove from inflight_gets
    if let Some(inflight_info) = view
        .master_kv_router()
        .inner()
        .inflight_gets
        .remove(&get_id)
        .await
    {
        inflight_info.release_durable_slot_if_needed();
        tracing::info!("Revoked get operation with get_id: {}", get_id);
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

pub async fn handle_get_done(
    view: MasterKvRouterView,
    req: MsgPack<GetDoneReq>,
) -> MsgPack<GetDoneResp> {
    tracing::debug!("Handling GetDoneReq: {:?}", req.serialize_part);

    let get_id = req.serialize_part.get_id;
    // Remove from inflight_gets and transfer to get_holding
    if let Some(inflight_info) = view
        .master_kv_router()
        .inner()
        .inflight_gets
        .remove(&get_id)
        .await
    {
        let mut allocation_mode = inflight_info.allocation_mode;
        let route = inflight_info.route.clone();
        // clone req_node_id to avoid borrow/move conflict when inserting into kv_routes
        let req_node_id = inflight_info.req_node_id.clone();
        // capture allocation capacity before moving it
        let alloc_cap = inflight_info.allocation.capcity();
        // Generate holder_id
        let holder_id = view
            .master_kv_router()
            .inner()
            .next_holder_id
            .fetch_add(1, Ordering::Relaxed);

        let src_node_id = inflight_info.src_node_id;
        let key = inflight_info.key;

        // Create holding info
        let holding_info = OwnerHoldingGetInfo {
            key: key.clone(),
            holding_node_id: inflight_info.req_node_id.clone(),
            len: inflight_info.len,
            allocation: inflight_info.allocation.clone(),
        };

        // Store in get_holding cache (owned manager, flattened key)
        view.master_kv_router().inner().get_holding.insert(
            crate::memholder::NodeHolderKey::new(req_node_id.to_string(), holder_id),
            holding_info,
        );

        if allocation_mode == GetAllocationMode::DurableReplica {
            let mut promote_committed = false;
            if let Some(one_kv_nodes_routes) = view.master_kv_router().inner().kv_routes.get(&key) {
                if one_kv_nodes_routes.put_id == inflight_info.put_id {
                    let mut nodes_replicas = one_kv_nodes_routes.nodes_replicas.write();
                    if let Some(tomb_tag) =
                        view.master_seg_manager().get_node_tomb_tag(&src_node_id)
                    {
                        if !tomb_tag.is_tomb() {
                            nodes_replicas.insert(
                                inflight_info.req_node_id.clone(),
                                KvRouteInfo {
                                    node_id: inflight_info.req_node_id,
                                    backing: super::KvReplicaBacking::Allocation(
                                        inflight_info.allocation,
                                    ),
                                    tomb_tag,
                                },
                            );
                            promote_committed = true;
                            // Read lease binding from route snapshot: for this put_id,
                            // if the key is leased, we must NOT insert into moka.
                            if one_kv_nodes_routes.lease_id.is_none() {
                                // notify moka cache controller for requesting node after route insert
                                // See put.rs for rationale: saturate weight to avoid u32 truncation
                                let req_weight = if alloc_cap > u32::MAX as u64 {
                                    tracing::warn!(
                                        "moka weight saturation on get_done: key={} put_id=({},{}) cap={}B exceeds u32::MAX; weight set to u32::MAX",
                                        key,
                                        inflight_info.put_id.0,
                                        inflight_info.put_id.1,
                                        alloc_cap
                                    );
                                    u32::MAX
                                } else {
                                    alloc_cap as u32
                                };
                                update_moka_for_node(
                                    view.clone(),
                                    req_node_id.to_string(),
                                    key.clone(),
                                    req_weight,
                                    inflight_info.put_id,
                                    true,
                                );
                            } else {
                                tracing::debug!(
                                    "Skip moka insert for leased key={} put_id=({},{}) on node {}",
                                    key,
                                    inflight_info.put_id.0,
                                    inflight_info.put_id.1,
                                    req_node_id
                                );
                            }
                        } else {
                            tracing::warn!(
                                "get node is tomb, get_id: {}, put_id: {:?}",
                                get_id,
                                one_kv_nodes_routes.put_id
                            );
                        }
                    } else {
                        tracing::warn!(
                            "get node is tomb, get_id: {}, put_id: {:?}",
                            get_id,
                            one_kv_nodes_routes.put_id
                        );
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
            if !promote_committed {
                allocation_mode = GetAllocationMode::Temporary;
                route.release_get_durable_slot();
            }
        }

        tracing::info!(
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
            });

        MsgPack {
            serialize_part: GetDoneResp {
                holder_id,
                allocation_mode,
                error_code: msg_and_error::OK,
                error_json: String::new(),
                server_process_us: 0,
            },
            raw_bytes: Vec::new(),
        }
    } else {
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

    {
        let _guard = view.master_kv_router().inner().route_lifetime_lock.lock();
        for key in &req.serialize_part.keys {
            if let Some(one_kv_nodes_routes) = view.master_kv_router().inner().kv_routes.get(key) {
                let exists = one_kv_routes_has_live_replica(&one_kv_nodes_routes);
                exists_list.push(exists);
            } else {
                exists_list.push(false);
            }
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
    let mut items = Vec::with_capacity(req.serialize_part.keys.len());
    for key in req.serialize_part.keys {
        let (_get_id, resp) = handle_get_start(
            view.clone(),
            MsgPack {
                serialize_part: GetStartReq { key },
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
) -> MsgPack<BatchGetRevokeResp> {
    let mut items = Vec::with_capacity(req.serialize_part.get_ids.len());
    for get_id in req.serialize_part.get_ids {
        let resp = handle_get_revoke(
            view.clone(),
            MsgPack {
                serialize_part: GetRevokeReq { get_id },
                raw_bytes: Vec::new(),
            },
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
) -> MsgPack<BatchGetDoneResp> {
    let mut items = Vec::with_capacity(req.serialize_part.get_ids.len());
    for get_id in req.serialize_part.get_ids {
        let resp = handle_get_done(
            view.clone(),
            MsgPack {
                serialize_part: GetDoneReq { get_id },
                raw_bytes: Vec::new(),
            },
        )
        .await;
        let part = resp.serialize_part;
        items.push(BatchGetDoneItemResp {
            get_id,
            holder_id: part.holder_id,
            allocation_mode: part.allocation_mode,
            error_code: part.error_code,
            error_json: part.error_json,
        });
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
