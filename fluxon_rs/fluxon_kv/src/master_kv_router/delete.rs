use super::{
    MasterKvRouterView,
    msg_pack::{
        BatchDeleteAckReq, BatchDeleteAckResp, BatchDeleteClientKvMetaCacheReq, DeleteAckReq,
        DeleteAckResp, DeleteClientKvMetaCacheItem, DeleteReq, DeleteResp,
    },
};
use crate::master_kv_router::OneKvNodesRoutes;
use crate::master_kv_router::put::PutIDForAKey;
use crate::memholder::{
    EnsureMemholderMgmtDeleteActorOwned, MasterOwnerMemMgr, MemholderManagerTrait,
};
use crate::{
    cluster_manager::NodeID,
    p2p::msg_pack::{MsgPack, RPCCaller},
    rpcresp_kvresult_convert::msg_and_error::{self, kv},
};
use limit_thirdparty::tokio;
use std::{sync::Arc, time::Duration};

/// Remove a key from master indices and trigger client cache invalidation broadcast.
///
/// This is the unified delete entry used by both:
/// - RPC delete (client initiated)
/// - Master-side evictions (size/ttl driven)
///
/// It removes the key from `kv_routes`, then asynchronously:
/// - emits a `DeleteKeyInfo` to the shared delete broadcast actor for clients
/// - removes the key from every node's local `node_kv_cache_controller`
pub fn do_delete_one_kv_all_replicas(
    view: &MasterKvRouterView,
    key: String,
) -> Result<PutIDForAKey, msg_and_error::ErrorCode> {
    if let Some((_removed_key, kv_route_info)) =
        view.master_kv_router().inner().kv_routes.remove(&key)
    {
        let deleted_put_id = kv_route_info.put_id;
        tracing::info!("Deleted kv_routes entry for key: {}", key);

        // Spawn async follow-up: broadcast + per-node cache cleanup
        let _ = view.spawn("delete_followup_broadcast_and_cache_cleanup", {
            let view = view.clone();
            let key_clone = key.clone();
            async move {
                if view.master_kv_router().prefix_index_enabled() {
                    let inner = view.master_kv_router().inner();
                    let mut tree = inner.prefix_index.write().await;
                    tree.remove(&key_clone);
                }

                if let Err(err) = view
                    .master_kv_router()
                    .inner()
                    .delete_broadcast
                    .sender()
                    .send(DeleteKeyInfo::Key {
                        key: key_clone.clone(),
                        nodes_kv_route_info: kv_route_info.clone(),
                    })
                    .await
                {
                    tracing::warn!("Failed to send delete broadcast: {}", err);
                }

                // Remove from all node caches that hold replicas of this key
                let nodes_replicas = kv_route_info.nodes_replicas.read();
                for (node_id, _kv_info) in nodes_replicas.iter() {
                    if let Some(cache) = view.master_kv_router().get_node_cache_controller(node_id)
                    {
                        let _ = cache.remove(&key_clone);
                        tracing::debug!(
                            "Removed key {} from node cache controller: {}",
                            key_clone,
                            node_id
                        );
                    }
                }
            }
        });

        Ok(deleted_put_id)
    } else {
        Err(kv::KeyNotFound::CODE)
    }
}

pub fn evict_one_kv_replica_for_node(
    view: &MasterKvRouterView,
    key: String,
    node_id: NodeID,
    put_id: PutIDForAKey,
) -> Result<(), msg_and_error::ErrorCode> {
    let route = if let Some(route) = view.master_kv_router().inner().kv_routes.get(&key) {
        route.clone()
    } else {
        tracing::debug!(
            "Local replica eviction ignored because key is already gone: key={} node_id={} put_id=({},{})",
            key,
            node_id,
            put_id.0,
            put_id.1
        );
        return Ok(());
    };
    if route.put_id != put_id {
        tracing::debug!(
            "Local replica eviction ignored because key version changed: key={} node_id={} evicted_put_id=({},{}) current_put_id=({},{})",
            key,
            node_id,
            put_id.0,
            put_id.1,
            route.put_id.0,
            route.put_id.1
        );
        return Ok(());
    }

    let removed_replica = {
        let mut nodes_replicas = route.nodes_replicas.write();
        nodes_replicas.remove(&node_id).is_some()
    };
    if !removed_replica {
        tracing::debug!(
            "Local replica eviction ignored because node replica is already absent: key={} node_id={} put_id=({},{})",
            key,
            node_id,
            put_id.0,
            put_id.1
        );
        return Ok(());
    }

    let last_replica_gone = route.nodes_replicas.read().is_empty();
    if last_replica_gone {
        let removed = view
            .master_kv_router()
            .inner()
            .kv_routes
            .remove_if(&key, |_, current| current.put_id == put_id)
            .is_some();
        if removed && view.master_kv_router().prefix_index_enabled() {
            let view_task = view.clone();
            let key_for_prefix = key.clone();
            let _ = view.spawn("local_evict_remove_prefix_index", async move {
                let inner = view_task.master_kv_router().inner();
                let mut tree = inner.prefix_index.write().await;
                tree.remove(&key_for_prefix);
            });
        }
    }

    let view_task = view.clone();
    let key_for_delete = key.clone();
    let node_for_delete = node_id.clone();
    let _ = view.spawn("local_evict_delete_client_cache", async move {
        let rpc_caller = RPCCaller::<BatchDeleteClientKvMetaCacheReq>::new();
        rpc_caller.regist(view_task.p2p_module());
        let req = MsgPack {
            serialize_part: BatchDeleteClientKvMetaCacheReq {
                delete_items: vec![DeleteClientKvMetaCacheItem {
                    key: key_for_delete.clone(),
                    put_time_ms: put_id.0,
                    put_version: put_id.1,
                }],
            },
            raw_bytes: Vec::new(),
        };
        match rpc_caller
            .call(
                view_task.p2p_module(),
                node_for_delete.clone(),
                req,
                Some(Duration::from_secs(60)),
                0,
            )
            .await
        {
            Ok(resp) => {
                if resp.serialize_part.error_code == msg_and_error::OK {
                    tracing::info!(
                        "Locally evicted key replica from node {}: key={} put_id=({},{}) deleted_count={}",
                        node_for_delete,
                        key_for_delete,
                        put_id.0,
                        put_id.1,
                        resp.serialize_part.deleted_count
                    );
                } else {
                    tracing::warn!(
                        "Local replica eviction delete failed on node {}: key={} put_id=({},{}) code={} err={}",
                        node_for_delete,
                        key_for_delete,
                        put_id.0,
                        put_id.1,
                        resp.serialize_part.error_code,
                        resp.serialize_part.error_json
                    );
                }
            }
            Err(err) => {
                tracing::warn!(
                    "Failed to send local replica eviction delete to node {}: key={} put_id=({},{}) err={:?}",
                    node_for_delete,
                    key_for_delete,
                    put_id.0,
                    put_id.1,
                    err
                );
            }
        }
    });

    Ok(())
}

pub async fn handle_delete(
    view: MasterKvRouterView,
    req: MsgPack<DeleteReq>,
) -> MsgPack<DeleteResp> {
    tracing::debug!("Handling DeleteReq: {:?}", req.serialize_part);

    let key = req.serialize_part.key.clone();

    match do_delete_one_kv_all_replicas(&view, key.clone()) {
        Ok((deleted_put_time_ms, deleted_put_version)) => MsgPack {
            serialize_part: DeleteResp {
                deleted_put_time_ms,
                deleted_put_version,
                error_code: msg_and_error::OK,
                error_json: String::new(),
            },
            raw_bytes: Vec::new(),
        },
        Err(_code) => {
            tracing::warn!("Key not found for deletion: {}", key);
            let err = msg_and_error::KvError::Api(msg_and_error::ApiError::KeyNotFound {
                key: key.clone(),
            });
            MsgPack {
                serialize_part: crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                raw_bytes: Vec::new(),
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum DeleteKeyInfo {
    /// A concrete key deletion event
    Key {
        key: String,
        /// can help us make sure the delete is done before the memory is released
        nodes_kv_route_info: Arc<OneKvNodesRoutes>,
    },
    /// A shutdown signal to terminate the broadcast loop gracefully
    Shutdown,
}

/// 启动删除广播任务，2秒内向clients发送主动删除kv的信息
pub fn spawn_delete_broadcast(
    view: MasterKvRouterView,
    rx: tokio::sync::ampsc::Receiver<DeleteKeyInfo>,
) {
    let actor = EnsureMemholderMgmtDeleteActorOwned::<MasterOwnerMemMgr>::new(view.clone());
    let _ = view.spawn("delete_broadcast", async move {
        tracing::info!("Starting delete broadcast task");
        actor.run(rx).await;
        tracing::info!("Delete broadcast task ended");
    });
}

/// Handle delete acknowledgment from client
pub async fn handle_delete_ack(
    view: MasterKvRouterView,
    req: MsgPack<DeleteAckReq>,
) -> MsgPack<DeleteAckResp> {
    tracing::debug!("Handling DeleteAckReq: {:?}", req.serialize_part);

    let key = &req.serialize_part.key;
    let client_id = &req.serialize_part.client_id;
    let holder_id = req.serialize_part.holder_id;

    // 从get_holding中删除特定的holder_id（owned manager）
    match view
        .master_kv_router()
        .inner()
        .get_holding
        .remove(&crate::memholder::NodeHolderKey::new(
            client_id.clone(),
            holder_id,
        )) {
        Some(_) => {
            tracing::info!(
                "Successfully removed holder_id: {} for key: {} from client: {} in get_holding",
                holder_id,
                key,
                client_id
            );
        }
        None => {
            tracing::warn!(
                "Holder_id: {} not found for key: {} from client: {}",
                holder_id,
                key,
                client_id
            );
        }
    }

    MsgPack {
        serialize_part: DeleteAckResp {
            error_code: msg_and_error::OK,
            error_json: String::new(),
        },
        raw_bytes: Vec::new(),
    }
}

pub async fn handle_batch_delete_ack(
    view: MasterKvRouterView,
    req: MsgPack<BatchDeleteAckReq>,
) -> MsgPack<BatchDeleteAckResp> {
    tracing::debug!(
        "Handling BatchDeleteAckReq with {} items",
        req.serialize_part.delete_acks.len()
    );

    let mut deleted_count = 0u32;
    for ack in &req.serialize_part.delete_acks {
        match view.master_kv_router().inner().get_holding.remove(
            &crate::memholder::NodeHolderKey::new(ack.client_id.clone(), ack.holder_id),
        ) {
            Some(_) => {
                deleted_count += 1;
            }
            None => {
                tracing::warn!(
                    "Holder_id: {} not found for key: {} from client: {}",
                    ack.holder_id,
                    ack.key,
                    ack.client_id
                );
            }
        }
    }

    MsgPack {
        serialize_part: BatchDeleteAckResp {
            deleted_count,
            error_code: msg_and_error::OK,
            error_json: String::new(),
        },
        raw_bytes: Vec::new(),
    }
}
