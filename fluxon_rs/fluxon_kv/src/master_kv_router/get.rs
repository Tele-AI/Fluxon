use super::{
    CompletedGetInfo, InflightGetInfo, MasterKvRouterView, NodeCacheCapacityReservation,
    NodeValueReplicaDesc, OwnerHoldingGetInfo,
    msg_pack::{
        GetAllocationMode, GetDoneReq, GetDoneResp, GetMetaReq, GetMetaResp, GetRevokeReq,
        GetRevokeResp, GetSourceKind, GetStartReq, GetStartResp, SsdStageBeginReq,
        SsdStageBeginResp,
    },
};
use crate::kv_ssd_storage::{SSD_ALIGNMENT, align_ssd_io_len};
use crate::master_kv_router::OneKvNodesRoutes;
use crate::master_kv_router::delete::remove_one_ssd_replica_for_node;
use crate::master_kv_router::put::PutIDForAKey;
use crate::memholder::MemholderManagerTrait;
use crate::{
    cluster_manager::NodeID, master_seg_manager::one_seg_allocator::Allocation,
    master_seg_manager::one_seg_allocator::OneSegAllocator, p2p::msg_pack::MsgPack,
    rpcresp_kvresult_convert::msg_and_error,
};
use rand::Rng;
use rand::seq::SliceRandom;
use std::collections::HashSet;
use std::future::Future;
use std::sync::{Arc, atomic::Ordering};
use std::time::{Duration, Instant};

const GET_ALLOCATION_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const GET_ALLOCATION_RETRY_INTERVAL: Duration = Duration::from_millis(2);

struct AllocationWaitResult {
    allocation: Option<Allocation>,
    attempts: usize,
    elapsed: Duration,
}

fn try_allocate_from_segments(
    allocators: &[Arc<OneSegAllocator>],
    len: u64,
    attempts: &mut usize,
) -> Option<Allocation> {
    for allocator in allocators {
        *attempts += 1;
        if let Ok(allocation) = allocator.allocate(len) {
            return Some(allocation);
        }
    }
    None
}

async fn allocate_with_bounded_wait<F, Fut>(
    allocators: &[Arc<OneSegAllocator>],
    len: u64,
    timeout: Duration,
    retry_interval: Duration,
    mut advance_reclamation: F,
) -> AllocationWaitResult
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()>,
{
    let started = Instant::now();
    let mut attempts = 0usize;

    loop {
        if let Some(allocation) = try_allocate_from_segments(allocators, len, &mut attempts) {
            return AllocationWaitResult {
                allocation: Some(allocation),
                attempts,
                elapsed: started.elapsed(),
            };
        }

        if started.elapsed() >= timeout {
            return AllocationWaitResult {
                allocation: None,
                attempts,
                elapsed: started.elapsed(),
            };
        }

        // Moka eviction removes the route first and releases the allocation after
        // the owner processes its invalidation RPC. Drive pending eviction work,
        // then yield so that RPC and holder ACK tasks can make progress.
        advance_reclamation().await;

        // In-process maintenance can release an allocation synchronously. Retry
        // once before sleeping so this path does not add a fixed 2 ms penalty.
        if let Some(allocation) = try_allocate_from_segments(allocators, len, &mut attempts) {
            return AllocationWaitResult {
                allocation: Some(allocation),
                attempts,
                elapsed: started.elapsed(),
            };
        }

        tokio::time::sleep(retry_interval).await;
    }
}

async fn allocate_get_buffer_on_node(
    view: &MasterKvRouterView,
    node_id: &NodeID,
    len: u64,
    get_id: u64,
    purpose: &str,
) -> Result<Arc<Allocation>, msg_and_error::KvError> {
    let mut node_allocators = view.master_seg_manager().get_node_allocators(node_id);
    if node_allocators.is_empty() {
        tracing::info!(
            "No allocators found for {} during get: {}, node is not ready",
            purpose,
            node_id
        );
        return Err(msg_and_error::KvError::Unreachable(
            msg_and_error::UnreachableError::OwnerNoSeg { detail: "config=0 initializes as external; non-zero initializes as owner; the owner must have memory space (segment)".to_string() }
        ));
    }

    node_allocators.shuffle(&mut rand::thread_rng());
    let cache = view
        .master_kv_router()
        .get_node_cache_controller(node_id.as_ref());
    let result = allocate_with_bounded_wait(
        &node_allocators,
        len,
        GET_ALLOCATION_WAIT_TIMEOUT,
        GET_ALLOCATION_RETRY_INTERVAL,
        || {
            if let Some(cache) = cache.as_ref() {
                cache.run_pending_tasks();
            }
            let inflight_puts = view.master_kv_router().inner().inflight_puts.clone();
            async move {
                // Moka can retain removed PUT RAII values in its maintenance
                // queue briefly. GET inflight state uses DashMap and drops on remove.
                inflight_puts.run_pending_tasks().await;
            }
        },
    )
    .await;

    if let Some(allocation) = result.allocation {
        if result.attempts > node_allocators.len() {
            tracing::debug!(
                "{} allocation recovered after {} attempts and {:?} for get_id {} on node {}",
                purpose,
                result.attempts,
                result.elapsed,
                get_id,
                node_id
            );
        }
        return Ok(Arc::new(allocation));
    }

    let allocator = node_allocators
        .iter()
        .max_by_key(|allocator| {
            allocator
                .total_size_bytes()
                .saturating_sub(allocator.used_size_bytes())
        })
        .expect("non-empty allocator list must have a diagnostic allocator");
    let total = allocator.total_size_bytes();
    let used = allocator.used_size_bytes();
    let free = total.saturating_sub(used);
    tracing::info!(
        "{} allocation timed out after {} attempts and {:?} for get_id {} on node {}",
        purpose,
        result.attempts,
        result.elapsed,
        get_id,
        node_id
    );
    Err(msg_and_error::KvError::Api(
        msg_and_error::ApiError::NoSpace {
            node: node_id.as_ref().to_string(),
            segment: allocator.seg_device_id.clone(),
            total_capacity: total,
            free_capacity: free,
        },
    ))
}

fn update_moka_for_node(
    view: &MasterKvRouterView,
    node_id: String,
    key: String,
    weight: u32,
    put_id: PutIDForAKey,
    new_inserted: bool,
) {
    if !view.master_kv_router().replica_cache_enabled() {
        return;
    }
    if let Some(cache) = view.master_kv_router().get_node_cache_controller(&node_id) {
        if new_inserted {
            cache.insert(
                key.clone(),
                NodeValueReplicaDesc {
                    weight_bytes: weight,
                    put_id,
                },
            );
            cache.run_pending_tasks();
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
}

fn reserve_durable_get_target(
    view: &MasterKvRouterView,
    route: &Arc<OneKvNodesRoutes>,
    node_id: &NodeID,
    target_allocation: &mut Arc<Allocation>,
) -> (GetAllocationMode, Option<Arc<NodeCacheCapacityReservation>>) {
    if !route.try_reserve_get_durable_slot() {
        return (GetAllocationMode::Temporary, None);
    }

    let reservation = match view
        .master_kv_router()
        .reserve_node_cache_capacity(node_id.as_ref(), target_allocation.capcity())
    {
        Ok(reservation) => reservation,
        Err(err) => {
            route.release_get_durable_slot();
            tracing::warn!(
                "Falling back to a temporary GET target because cache capacity reservation failed: node={} bytes={} err={}",
                node_id,
                target_allocation.capcity(),
                err
            );
            return (GetAllocationMode::Temporary, None);
        }
    };

    if route.lease_id.is_some() {
        if let Some(reservation) = reservation {
            Arc::get_mut(target_allocation)
                .expect("new GET target allocation must be uniquely owned")
                .set_on_drop(move || drop(reservation));
        }
        (GetAllocationMode::DurableReplica, None)
    } else {
        (GetAllocationMode::DurableReplica, reservation.map(Arc::new))
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
            let route_to_remove = view
                .master_kv_router()
                .inner()
                .kv_routes
                .get(key)
                .map(|route| route.value().clone())
                .filter(|route| {
                    route.remove_tombed_node_replicas(put_id, tombs) && !route.has_live_replica()
                });

            if let Some(route_to_remove) = route_to_remove {
                view.master_kv_router().inner().kv_routes.remove_if(
                    key,
                    |_, one_kv_nodes_routes| {
                        Arc::ptr_eq(one_kv_nodes_routes, &route_to_remove)
                            && one_kv_nodes_routes.put_id == put_id
                            && !one_kv_nodes_routes.has_live_replica()
                    },
                );
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
    fn align_ssd_stage_addr(raw_addr: u64) -> Result<u64, msg_and_error::KvError> {
        raw_addr
            .checked_add(SSD_ALIGNMENT as u64 - 1)
            .map(|addr| addr / SSD_ALIGNMENT as u64 * SSD_ALIGNMENT as u64)
            .ok_or_else(|| {
                msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
                    detail: format!("ssd source staging address alignment overflow: {raw_addr}"),
                })
            })
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

    let replicas = one_kv_nodes_routes.node_replicas.read().clone();
    let mut replica_keys = replicas
        .iter()
        .filter_map(|(node_id, replicas)| replicas.memory.is_some().then_some(node_id))
        .collect::<Vec<_>>();
    let mut tombs = HashSet::new();
    let mut target_allocations = None;
    let mut allocation_mode = GetAllocationMode::Temporary;
    let mut target_capacity_reservation = None;
    while !replica_keys.is_empty() {
        let to_remove_idx = rand::thread_rng().gen_range(0..replica_keys.len());
        let selected_replica_key = replica_keys.remove(to_remove_idx);
        let selected_replica = replicas
            .get(selected_replica_key)
            .expect("selected memory replica node must exist");
        if selected_replica.tomb_tag.is_tomb() {
            tombs.insert(selected_replica_key.to_owned());
            continue;
        }
        let src_allocation = selected_replica
            .memory
            .as_ref()
            .expect("selected memory replica must exist")
            .clone();
        let src_node_id = selected_replica_key.to_owned();

        // 为get调用方分配接收内存作为传输target
        if target_allocations.is_none() {
            target_allocations = if let Some(replica_on_recv_node) = replicas
                .get(&req_node_id)
                .filter(|replicas| !replicas.tomb_tag.is_tomb())
                .and_then(|replicas| replicas.memory.as_ref())
            {
                allocation_mode = GetAllocationMode::ReuseReplica;
                Some(replica_on_recv_node.clone())
            } else {
                let mut target_allocation = match allocate_get_buffer_on_node(
                    &view,
                    &req_node_id,
                    src_allocation.size(),
                    get_id,
                    "requesting target",
                )
                .await
                {
                    Ok(allocation) => allocation,
                    Err(err) => {
                        return failed_resp_err(
                            err,
                            Some((tombs, one_kv_nodes_routes.put_id)),
                            &view,
                            &req.serialize_part.key,
                        );
                    }
                };
                let (mode, reservation) = reserve_durable_get_target(
                    &view,
                    &one_kv_nodes_routes,
                    &req_node_id,
                    &mut target_allocation,
                );
                allocation_mode = mode;
                target_capacity_reservation = reservation;
                Some(target_allocation)
            };
        }

        let target_allocation = target_allocations.unwrap();

        // Convert to absolute addresses for Mooncake (requires absolute)
        // Use allocation's allocator base directly
        let src_base = src_allocation.base_addr();
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
                    src_base + src_allocation.addr(),
                    target_base + target_allocation.addr(),
                    src_base,
                    target_base,
                )
            };

        let resp = GetStartResp {
            put_id: one_kv_nodes_routes.put_id,
            get_id,
            node_id: resp_node_id.clone().into(),
            source_kind: GetSourceKind::Memory,
            src_addr: resp_src_addr,
            target_addr: resp_target_addr,
            src_base_addr: resp_src_base,
            target_base_addr: resp_target_base,
            len: src_allocation.size(),
            ssd_stage_len: 0,
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
            len: src_allocation.size(),
            allocation: target_allocation, // 存储target allocation
            // Keep the source alive if Moka evicts its route during the transfer.
            source_allocation: Some(src_allocation),
            route: one_kv_nodes_routes.clone(),
            allocation_mode,
            source_kind: GetSourceKind::Memory,
            ssd_stage_lifecycle: None,
            cache_capacity_reservation: target_capacity_reservation,
        };

        view.master_kv_router()
            .inner()
            .insert_inflight_get(get_id, info);

        // After selecting source and allocating target, optionally touch the
        // source node's moka to keep the kv alive during transfer (weight=0 => touch).
        // For leased keys, there should be no moka entry; skip touching to avoid
        // unnecessary cache work.
        if one_kv_nodes_routes.lease_id.is_none() {
            update_moka_for_node(
                &view,
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

    let mut ssd_replica_keys = replicas
        .iter()
        .filter_map(|(node_id, replicas)| replicas.ssd.is_some().then_some(node_id))
        .collect::<Vec<_>>();
    while !ssd_replica_keys.is_empty() {
        let to_remove_idx = rand::thread_rng().gen_range(0..ssd_replica_keys.len());
        let selected_ssd_key = ssd_replica_keys.remove(to_remove_idx);
        let selected_node_replicas = replicas
            .get(selected_ssd_key)
            .expect("selected SSD replica node must exist");
        if selected_node_replicas.tomb_tag.is_tomb() {
            tombs.insert(selected_ssd_key.to_owned());
        } else {
            let ssd_replica = selected_node_replicas
                .ssd
                .as_ref()
                .expect("selected SSD replica must exist");
            let ssd_stage_len = match align_ssd_io_len(ssd_replica.len) {
                Ok(len) => len,
                Err(err) => {
                    return failed_resp_err(
                        err,
                        Some((tombs, one_kv_nodes_routes.put_id)),
                        &view,
                        &req.serialize_part.key,
                    );
                }
            };
            let mut target_allocation = match allocate_get_buffer_on_node(
                &view,
                &req_node_id,
                ssd_replica.len,
                get_id,
                "requesting target",
            )
            .await
            {
                Ok(allocation) => allocation,
                Err(err) => {
                    return failed_resp_err(
                        err,
                        Some((tombs, one_kv_nodes_routes.put_id)),
                        &view,
                        &req.serialize_part.key,
                    );
                }
            };
            let target_base = target_allocation.base_addr();
            let target_addr = match target_base.checked_add(target_allocation.addr()) {
                Some(addr) => addr,
                None => {
                    let err =
                        msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
                            detail: format!(
                                "requesting target address overflow: base={} offset={}",
                                target_base,
                                target_allocation.addr()
                            ),
                        });
                    return failed_resp_err(
                        err,
                        Some((tombs, one_kv_nodes_routes.put_id)),
                        &view,
                        &req.serialize_part.key,
                    );
                }
            };

            let local_ssd_read = selected_ssd_key == &req_node_id;
            let (source_allocation, source_base, source_addr, response_ssd_stage_len) =
                if local_ssd_read {
                    let target_capacity = target_allocation.capcity();
                    tracing::debug!(
                        "Using local SSD read for get_id {} on node {}: target={:#x} len={} capacity={}",
                        get_id,
                        selected_ssd_key,
                        target_addr,
                        ssd_replica.len,
                        target_capacity
                    );
                    (None, target_base, target_addr, target_capacity)
                } else {
                    let source_alloc_len = match ssd_stage_len.checked_add(SSD_ALIGNMENT as u64 - 1)
                    {
                        Some(len) => len,
                        None => {
                            let err = msg_and_error::KvError::Api(
                                msg_and_error::ApiError::InvalidArgument {
                                    detail: format!(
                                        "ssd source staging allocation length overflow: {ssd_stage_len}"
                                    ),
                                },
                            );
                            return failed_resp_err(
                                err,
                                Some((tombs, one_kv_nodes_routes.put_id)),
                                &view,
                                &req.serialize_part.key,
                            );
                        }
                    };
                    let source_allocation = match allocate_get_buffer_on_node(
                        &view,
                        selected_ssd_key,
                        source_alloc_len,
                        get_id,
                        "ssd source staging",
                    )
                    .await
                    {
                        Ok(allocation) => allocation,
                        Err(err) => {
                            tracing::info!(
                                "Skipping SSD source for get_id {} on node {}: {}",
                                get_id,
                                selected_ssd_key,
                                err
                            );
                            continue;
                        }
                    };
                    let source_base = source_allocation.base_addr();
                    let source_raw_addr = match source_base.checked_add(source_allocation.addr()) {
                        Some(addr) => addr,
                        None => {
                            let err = msg_and_error::KvError::Api(
                                msg_and_error::ApiError::InvalidArgument {
                                    detail: format!(
                                        "ssd source staging raw address overflow: base={} offset={}",
                                        source_base,
                                        source_allocation.addr()
                                    ),
                                },
                            );
                            return failed_resp_err(
                                err,
                                Some((tombs, one_kv_nodes_routes.put_id)),
                                &view,
                                &req.serialize_part.key,
                            );
                        }
                    };
                    let source_addr = match align_ssd_stage_addr(source_raw_addr) {
                        Ok(addr) => addr,
                        Err(err) => {
                            return failed_resp_err(
                                err,
                                Some((tombs, one_kv_nodes_routes.put_id)),
                                &view,
                                &req.serialize_part.key,
                            );
                        }
                    };
                    (
                        Some(source_allocation),
                        source_base,
                        source_addr,
                        ssd_stage_len,
                    )
                };
            let (allocation_mode, cache_capacity_reservation) = reserve_durable_get_target(
                &view,
                &one_kv_nodes_routes,
                &req_node_id,
                &mut target_allocation,
            );
            let resp = GetStartResp {
                put_id: one_kv_nodes_routes.put_id,
                get_id,
                node_id: selected_ssd_key.to_owned().into(),
                source_kind: GetSourceKind::Ssd,
                src_addr: source_addr,
                target_addr,
                src_base_addr: source_base,
                target_base_addr: target_base,
                len: ssd_replica.len,
                ssd_stage_len: response_ssd_stage_len,
                error_code: msg_and_error::OK,
                error_json: String::new(),
                server_process_us: 0,
            };
            let info = InflightGetInfo {
                put_id: one_kv_nodes_routes.put_id,
                src_node_id: selected_ssd_key.to_owned(),
                key: req.serialize_part.key.clone(),
                req_node_id,
                len: ssd_replica.len,
                allocation: target_allocation,
                source_allocation,
                route: one_kv_nodes_routes.clone(),
                allocation_mode,
                source_kind: GetSourceKind::Ssd,
                ssd_stage_lifecycle: (!local_ssd_read)
                    .then(|| Arc::new(parking_lot::Mutex::new(super::SsdStageLifecycle::new()))),
                cache_capacity_reservation,
            };

            view.master_kv_router()
                .inner()
                .insert_inflight_get(get_id, info);

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

fn drop_failed_ssd_source(view: &MasterKvRouterView, inflight_info: &InflightGetInfo) {
    if inflight_info.source_kind != GetSourceKind::Ssd {
        tracing::warn!(
            "Ignoring drop_ssd_source for non-SSD get: get_key={} put_id=({},{}) source_kind={:?}",
            inflight_info.key,
            inflight_info.put_id.0,
            inflight_info.put_id.1,
            inflight_info.source_kind
        );
        return;
    }

    let removal = remove_one_ssd_replica_for_node(
        view,
        &inflight_info.key,
        &inflight_info.src_node_id,
        inflight_info.put_id,
    );
    if !removal.replica_removed {
        return;
    }

    tracing::warn!(
        "Removed failed SSD replica: key={} node={} put_id=({},{})",
        inflight_info.key,
        inflight_info.src_node_id,
        inflight_info.put_id.0,
        inflight_info.put_id.1
    );

    if removal.route_removed && view.master_kv_router().prefix_index_enabled() {
        let view_task = view.clone();
        let key_for_prefix = inflight_info.key.clone();
        let _ = view.spawn("ssd_failure_remove_prefix_index", async move {
            let inner = view_task.master_kv_router().inner();
            let mut tree = inner.prefix_index.write().await;
            if !inner.kv_routes.contains_key(&key_for_prefix) {
                tree.remove(&key_for_prefix);
            }
        });
    }
}

fn finish_revoked_get(
    view: &MasterKvRouterView,
    get_id: u64,
    inflight_info: InflightGetInfo,
    drop_ssd_source: bool,
) {
    if drop_ssd_source {
        drop_failed_ssd_source(view, &inflight_info);
    }
    inflight_info.release_durable_slot_if_needed();
    tracing::info!(get_id, "Revoked get operation");
}

fn invalid_get_caller(
    operation: &str,
    get_id: u64,
    expected: &NodeID,
    actual: &NodeID,
) -> msg_and_error::KvError {
    msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
        detail: format!(
            "{operation} caller mismatch for get_id={get_id}: expected={expected} actual={actual}"
        ),
    })
}

fn invalid_get_source_or_requester(
    operation: &str,
    get_id: u64,
    source: &NodeID,
    requester: &NodeID,
    actual: &NodeID,
) -> msg_and_error::KvError {
    msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
        detail: format!(
            "{operation} caller mismatch for get_id={get_id}: expected source={source} or requester={requester}, actual={actual}"
        ),
    })
}

pub async fn handle_ssd_stage_begin(
    view: MasterKvRouterView,
    req: MsgPack<SsdStageBeginReq>,
    source_node_id: NodeID,
) -> MsgPack<SsdStageBeginResp> {
    let get_id = req.serialize_part.get_id;
    let transition_lock = view
        .master_kv_router()
        .inner()
        .get_transition_locks
        .get_lock(get_id);
    let _transition = transition_lock.lock().await;

    let result = async {
        let inflight = view
            .master_kv_router()
            .inner()
            .get_inflight_get(get_id)
            .ok_or_else(|| {
                msg_and_error::KvError::Api(msg_and_error::ApiError::GetTimeout {
                    timeout_ms: 0,
                    detail: format!("SSD stage begin rejected for inactive get_id={get_id}"),
                })
            })?;
        if inflight.src_node_id != source_node_id {
            return Err(invalid_get_caller(
                "SSD stage begin",
                get_id,
                &inflight.src_node_id,
                &source_node_id,
            ));
        }
        let lifecycle = inflight.ssd_stage_lifecycle.as_ref().ok_or_else(|| {
            msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
                detail: format!("get_id={get_id} has no remote SSD stage"),
            })
        })?;
        let mut lifecycle = lifecycle.lock();
        // Begin is idempotent while active so a lost response can be retried safely.
        if !lifecycle.begin() {
            return Err(msg_and_error::KvError::Api(
                msg_and_error::ApiError::InvalidArgument {
                    detail: format!("SSD stage already ended for get_id={get_id}"),
                },
            ));
        }
        Ok(())
    }
    .await;

    let response = match result {
        Ok(()) => SsdStageBeginResp {
            error_code: msg_and_error::OK,
            error_json: String::new(),
        },
        Err(err) => crate::rpcresp_kvresult_convert::FromError::from_error(&err),
    };
    MsgPack {
        serialize_part: response,
        raw_bytes: Vec::new(),
    }
}

pub async fn handle_get_revoke(
    view: MasterKvRouterView,
    req: MsgPack<GetRevokeReq>,
    caller_node_id: NodeID,
) -> MsgPack<GetRevokeResp> {
    tracing::debug!("Handling GetRevokeReq: {:?}", req.serialize_part);

    let get_id = req.serialize_part.get_id;
    let transition_lock = view
        .master_kv_router()
        .inner()
        .get_transition_locks
        .get_lock(get_id);
    let _transition = transition_lock.lock().await;

    let result = if let Some(completed) =
        view.master_kv_router().inner().completed_gets.get(&get_id)
    {
        if completed.requester_node_id == caller_node_id {
            view.master_kv_router()
                .inner()
                .completed_gets
                .invalidate(&get_id);
            view.master_kv_router()
                .inner()
                .get_holding
                .remove(&completed.holder_key);
            tracing::info!(get_id, "Revoked completed-but-undelivered get holding");
            Ok(())
        } else if completed.committer_node_id == caller_node_id {
            // GetDone may have committed even when its response was lost. A source
            // fallback revoke confirms quiescence but must not delete requester state.
            tracing::debug!(get_id, "Ignored source revoke after committed GetDone");
            Ok(())
        } else {
            Err(invalid_get_source_or_requester(
                "GetRevoke",
                get_id,
                &completed.committer_node_id,
                &completed.requester_node_id,
                &caller_node_id,
            ))
        }
    } else if let Some(inflight) = view.master_kv_router().inner().get_inflight_get(get_id) {
        if let Some(lifecycle) = &inflight.ssd_stage_lifecycle {
            if inflight.src_node_id == caller_node_id {
                let drop_ssd_source = lifecycle
                    .lock()
                    .finish_revoke_from_source(req.serialize_part.drop_ssd_source);
                if let Some(drop_ssd_source) = drop_ssd_source {
                    if let Some(inflight) =
                        view.master_kv_router().inner().remove_inflight_get(get_id)
                    {
                        finish_revoked_get(&view, get_id, inflight, drop_ssd_source);
                    }
                    Ok(())
                } else {
                    Err(msg_and_error::KvError::Api(
                        msg_and_error::ApiError::InvalidArgument {
                            detail: format!("remote SSD source already finalized get_id={get_id}"),
                        },
                    ))
                }
            } else if inflight.req_node_id == caller_node_id {
                let defer_release = lifecycle
                    .lock()
                    .request_revoke(req.serialize_part.drop_ssd_source);
                if defer_release {
                    tracing::info!(
                        get_id,
                        "Deferred requester GetRevoke until remote SSD source finalizes"
                    );
                } else if let Some(inflight) =
                    view.master_kv_router().inner().remove_inflight_get(get_id)
                {
                    finish_revoked_get(&view, get_id, inflight, req.serialize_part.drop_ssd_source);
                }
                Ok(())
            } else {
                Err(invalid_get_source_or_requester(
                    "GetRevoke",
                    get_id,
                    &inflight.src_node_id,
                    &inflight.req_node_id,
                    &caller_node_id,
                ))
            }
        } else if inflight.req_node_id != caller_node_id {
            Err(invalid_get_caller(
                "GetRevoke",
                get_id,
                &inflight.req_node_id,
                &caller_node_id,
            ))
        } else {
            if let Some(inflight) = view.master_kv_router().inner().remove_inflight_get(get_id) {
                finish_revoked_get(&view, get_id, inflight, req.serialize_part.drop_ssd_source);
            }
            Ok(())
        }
    } else {
        tracing::debug!(
            get_id,
            "Get operation already absent during idempotent revoke"
        );
        Ok(())
    };

    if let Err(err) = result {
        return MsgPack {
            serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
            raw_bytes: Vec::new(),
        };
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
    caller_node_id: NodeID,
) -> MsgPack<GetDoneResp> {
    tracing::debug!("Handling GetDoneReq: {:?}", req.serialize_part);

    let get_id = req.serialize_part.get_id;
    let transition_lock = view
        .master_kv_router()
        .inner()
        .get_transition_locks
        .get_lock(get_id);
    let _transition = transition_lock.lock().await;

    if let Some(completed) = view.master_kv_router().inner().completed_gets.get(&get_id) {
        let Some(response) = completed.replay_for(&caller_node_id) else {
            let err = invalid_get_source_or_requester(
                "GetDone",
                get_id,
                &completed.committer_node_id,
                &completed.requester_node_id,
                &caller_node_id,
            );
            return MsgPack {
                serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                raw_bytes: Vec::new(),
            };
        };
        return MsgPack {
            serialize_part: response,
            raw_bytes: Vec::new(),
        };
    }

    let Some(inflight) = view.master_kv_router().inner().get_inflight_get(get_id) else {
        tracing::warn!(get_id, "Get operation not found for completion");
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::GetTimeout {
            timeout_ms: 0,
            detail: format!(
                "Get operation with get_id {get_id} is no longer active and has no replayable completion"
            ),
        });
        let mut response: GetDoneResp =
            crate::rpcresp_kvresult_convert::FromError::from_error(&err);
        response.holder_id = 0;
        return MsgPack {
            serialize_part: response,
            raw_bytes: Vec::new(),
        };
    };

    let committer_node_id = if let Some(lifecycle) = &inflight.ssd_stage_lifecycle {
        if inflight.src_node_id != caller_node_id {
            let err = invalid_get_caller("GetDone", get_id, &inflight.src_node_id, &caller_node_id);
            return MsgPack {
                serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                raw_bytes: Vec::new(),
            };
        }

        let Some((revoke_requested, drop_ssd_source)) = lifecycle.lock().finish_done_from_source()
        else {
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::InvalidArgument {
                detail: format!(
                    "remote SSD source cannot complete get_id={get_id} before stage begin"
                ),
            });
            return MsgPack {
                serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                raw_bytes: Vec::new(),
            };
        };

        if revoke_requested {
            if let Some(inflight) = view.master_kv_router().inner().remove_inflight_get(get_id) {
                finish_revoked_get(&view, get_id, inflight, drop_ssd_source);
            }
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::GetTimeout {
                timeout_ms: 0,
                detail: format!(
                    "requester revoked get_id={get_id} before the SSD source completed transfer"
                ),
            });
            return MsgPack {
                serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                raw_bytes: Vec::new(),
            };
        }

        inflight.src_node_id.clone()
    } else {
        if inflight.req_node_id != caller_node_id {
            let err = invalid_get_caller("GetDone", get_id, &inflight.req_node_id, &caller_node_id);
            return MsgPack {
                serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                raw_bytes: Vec::new(),
            };
        }
        inflight.req_node_id.clone()
    };

    // Remove from inflight_gets and transfer to get_holding
    if let Some(mut inflight_info) = view.master_kv_router().inner().remove_inflight_get(get_id) {
        let mut cache_capacity_reservation = inflight_info.cache_capacity_reservation.take();
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

        let key = inflight_info.key;

        // Create holding info
        let holding_info = OwnerHoldingGetInfo {
            key: key.clone(),
            holding_node_id: inflight_info.req_node_id.clone(),
            len: inflight_info.len,
            allocation: inflight_info.allocation.clone(),
        };

        // Store in get_holding cache (owned manager, flattened key)
        let holder_key = crate::memholder::NodeHolderKey::new(req_node_id.to_string(), holder_id);
        let holding_inserted = view
            .master_kv_router()
            .inner()
            .get_holding
            .insert_if_member_active(holder_key.clone(), holding_info);
        if !holding_inserted {
            if allocation_mode == GetAllocationMode::DurableReplica {
                route.release_get_durable_slot();
            }
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::GetTimeout {
                timeout_ms: 0,
                detail: format!(
                    "requester {} left before get_id={get_id} could commit its holding",
                    req_node_id
                ),
            });
            return MsgPack {
                serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                raw_bytes: Vec::new(),
            };
        }

        if allocation_mode == GetAllocationMode::DurableReplica {
            let mut promote_committed = false;
            let current_route = view
                .master_kv_router()
                .inner()
                .kv_routes
                .get(&key)
                .map(|route| route.value().clone());
            if let Some(one_kv_nodes_routes) = current_route {
                if one_kv_nodes_routes.put_id == inflight_info.put_id {
                    if let Some(tomb_tag) =
                        view.master_seg_manager().get_node_tomb_tag(&req_node_id)
                    {
                        if !tomb_tag.is_tomb() {
                            one_kv_nodes_routes.insert_memory_replica(
                                inflight_info.req_node_id.clone(),
                                inflight_info.allocation,
                                tomb_tag,
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
                                // Move the target from the in-flight reservation into
                                // Moka in the same handler turn.
                                drop(cache_capacity_reservation.take());
                                update_moka_for_node(
                                    &view,
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

        let response = GetDoneResp {
            holder_id,
            allocation_mode,
            error_code: msg_and_error::OK,
            error_json: String::new(),
            server_process_us: 0,
        };
        view.master_kv_router().inner().completed_gets.insert(
            get_id,
            CompletedGetInfo {
                requester_node_id: req_node_id,
                committer_node_id,
                holder_key,
                response: response.clone(),
            },
        );
        MsgPack {
            serialize_part: response,
            raw_bytes: Vec::new(),
        }
    } else {
        tracing::warn!(
            "Get operation with get_id {} not found for completion",
            get_id
        );
        let err = msg_and_error::KvError::Api(msg_and_error::ApiError::GetTimeout {
            timeout_ms: 0,
            detail: format!(
                "Get operation with get_id {} is no longer active and has no replayable completion",
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
        let node_replicas = one_kv_nodes_routes.node_replicas.read().clone();

        for replicas in node_replicas.values() {
            if replicas.tomb_tag.is_tomb() {
                continue;
            }
            let Some(allocation) = replicas.memory.as_ref() else {
                continue;
            };
            return MsgPack {
                serialize_part: GetMetaResp {
                    exists: true,
                    len: allocation.size(),
                    error_code: msg_and_error::OK,
                    error_json: String::new(),
                },
                raw_bytes: Vec::new(),
            };
        }
        for replicas in node_replicas.values() {
            if replicas.tomb_tag.is_tomb() {
                continue;
            }
            let Some(ssd) = replicas.ssd.as_ref() else {
                continue;
            };
            return MsgPack {
                serialize_part: GetMetaResp {
                    exists: true,
                    len: ssd.len,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master_seg_manager::msg_pack::SegmentDeviceDescription;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[::tokio::test]
    async fn allocation_waits_for_delayed_release() {
        let allocator = Arc::new(
            OneSegAllocator::new(
                "get-wait-test".to_string(),
                SegmentDeviceDescription::Cpu,
                0,
                4096,
            )
            .expect("test allocator must be created"),
        );
        let held = allocator
            .allocate(4096)
            .expect("test allocator must initially have capacity");
        let reclamation_attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_wait = Arc::clone(&reclamation_attempts);
        let allocators = [Arc::clone(&allocator)];

        let wait_for_allocation = allocate_with_bounded_wait(
            &allocators,
            4096,
            Duration::from_millis(500),
            Duration::from_millis(1),
            move || {
                attempts_for_wait.fetch_add(1, Ordering::Relaxed);
                async {}
            },
        );
        let release_capacity = async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            drop(held);
        };

        let (result, ()) = tokio::join!(wait_for_allocation, release_capacity);
        assert!(result.allocation.is_some());
        assert!(result.attempts > 1);
        assert!(reclamation_attempts.load(Ordering::Relaxed) > 0);
    }

    #[::tokio::test]
    async fn allocation_wait_drains_removed_moka_value() {
        let allocator = Arc::new(
            OneSegAllocator::new(
                "get-moka-drain-test".to_string(),
                SegmentDeviceDescription::Cpu,
                0,
                4096,
            )
            .expect("test allocator must be created"),
        );
        let allocation = Arc::new(
            allocator
                .allocate(4096)
                .expect("test allocator must initially have capacity"),
        );
        let allocation_weak = Arc::downgrade(&allocation);
        let cache = moka::future::Cache::builder()
            .eviction_listener(|_key: Arc<u64>, _value: Arc<Allocation>, _cause| {})
            .build();
        cache.insert(1, allocation).await;
        cache.run_pending_tasks().await;

        let removed = cache
            .remove(&1)
            .await
            .expect("the test allocation must still be present");
        drop(removed);
        assert!(
            allocation_weak.upgrade().is_some(),
            "Moka's pending remove should still retain the allocation before maintenance"
        );

        let allocators = [Arc::clone(&allocator)];
        let result = allocate_with_bounded_wait(
            &allocators,
            4096,
            Duration::from_millis(500),
            Duration::from_millis(1),
            || {
                let cache = cache.clone();
                async move {
                    cache.run_pending_tasks().await;
                }
            },
        )
        .await;

        assert!(result.allocation.is_some());
        assert!(result.attempts > 1);
        assert!(allocation_weak.upgrade().is_none());
    }
}
