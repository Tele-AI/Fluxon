use super::ClientKvApiInner;
use crate::client_kv_api::ClientKvApiView;
use crate::cluster_manager::NodeID;
use crate::master_kv_router::msg_pack::BatchDeleteClientKvMetaCacheReq;
use crate::master_kv_router::msg_pack::BatchDeleteClientKvMetaCacheResp;
use crate::master_kv_router::msg_pack::DeleteClientKvMetaCacheItem;
use crate::memholder::{
    EnsureMemholderMgmtDeleteActorOwned, OwnerDeleteAckItem, OwnerDeleteAckMemMgr,
    OwnerExternalMemMgr,
};
use crate::{
    cluster_manager::app_logic_ext::ClusterManagerAppLogicExt,
    master_kv_router::msg_pack::DeleteReq,
    p2p::msg_pack::MsgPack,
    rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult, OK},
};
use limit_thirdparty::tokio;

impl ClientKvApiInner {
    pub async fn delete(&self, key: &str) -> KvResult<()> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting delete".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: DeleteReq {
                key: key.to_string(),
            },
            raw_bytes: Vec::new(),
        };

        // 获取 master 节点 ID
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;

        // 调用 RPC
        let resp = self
            .rpc_caller_delete
            .call(self.view.p2p_module(), master_node_id.into(), req, None, 0)
            .await
            .map_err(KvError::from)?;

        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;

        let delete_item = DeleteClientKvMetaCacheItem {
            key: key.to_string(),
            put_time_ms: resp.serialize_part.deleted_put_time_ms,
            put_version: resp.serialize_part.deleted_put_version,
        };
        apply_delete_client_kv_meta_cache_item(&self.view, &delete_item).await;

        Ok(())
    }
}

pub fn spawn_external_invalidate_delete(
    view: ClientKvApiView,
    rx: tokio::sync::ampsc::Receiver<
        crate::master_kv_router::msg_pack::DeleteClientKvMetaCacheItem,
    >,
) {
    let actor = EnsureMemholderMgmtDeleteActorOwned::<OwnerExternalMemMgr>::new(view.clone());
    let _ = view.spawn("external_invalidate_delete", async move {
        actor.run(rx).await;
    });
}

pub fn spawn_owner_delete_ack_batch(
    view: ClientKvApiView,
    rx: tokio::sync::ampsc::Receiver<OwnerDeleteAckItem>,
) {
    let actor = EnsureMemholderMgmtDeleteActorOwned::<OwnerDeleteAckMemMgr>::new(view.clone());
    let _ = view.spawn("owner_delete_ack_batch", async move {
        actor.run(rx).await;
    });
}

async fn apply_delete_client_kv_meta_cache_item(
    view: &ClientKvApiView,
    delete_item: &DeleteClientKvMetaCacheItem,
) {
    let client_api = view.client_kv_api();
    let client_inner = client_api.inner();

    let (cached_info, pending_get) = {
        let controls = client_inner.owner_key_control.lock_key(&delete_item.key);
        if controls
            .get(&delete_item.key)
            .is_some_and(|state| state.local_access_fenced())
        {
            tracing::debug!(
                "skip legacy local-index delete while owner reclaim fence is active: key={}",
                delete_item.key
            );
            return;
        }
        let cached_info = client_inner
            .get_cached_info
            .remove_if(&delete_item.key, |_, v| {
                let res = if v.put_time_ms == delete_item.put_time_ms {
                    v.put_version <= delete_item.put_version
                } else {
                    v.put_time_ms <= delete_item.put_time_ms
                };
                if res {
                    tracing::debug!("do remove local cache for key: {}", delete_item.key);
                } else {
                    tracing::debug!(
                        "skip remove local cache for key: {}, request ({},{}), local ({},{})",
                        delete_item.key,
                        delete_item.put_time_ms,
                        delete_item.put_version,
                        v.put_time_ms,
                        v.put_version
                    );
                }
                res
            })
            .map(|(_, cached_info)| cached_info);
        let _ = client_inner
            .local_snapshot_info
            .remove_if(&delete_item.key, |_, snapshot| {
                if snapshot.put_time_ms == delete_item.put_time_ms {
                    snapshot.put_version <= delete_item.put_version
                } else {
                    snapshot.put_time_ms <= delete_item.put_time_ms
                }
            });
        let pending_get = client_inner
            .pending_local_get_info
            .remove_if(&delete_item.key, |_, pending| {
                if pending.put_id.0 == delete_item.put_time_ms {
                    pending.put_id.1 <= delete_item.put_version
                } else {
                    pending.put_id.0 <= delete_item.put_time_ms
                }
            })
            .map(|(_, pending)| pending);
        (cached_info, pending_get)
    };
    drop(pending_get);
    if let Some(cached_info) = cached_info {
        client_inner.owner_hot_invalidate_version(
            &delete_item.key,
            (cached_info.put_time_ms, cached_info.put_version),
        );
        client_inner.release_local_reserve_route_for_memory_info(cached_info.mem_holder.as_ref());
    }

    if let Err(err) = client_inner
        .external_invalidate_delete
        .sender()
        .send(delete_item.clone())
        .await
    {
        tracing::warn!(
            "Failed to enqueue external weak-index invalidation for key '{}': {}",
            delete_item.key,
            err
        );
    }
}

/// 批量删除客户端 KV 元数据缓存的处理函数
pub async fn handle_batch_delete_client_kv_meta_cache(
    view: &ClientKvApiView,
    req: MsgPack<BatchDeleteClientKvMetaCacheReq>,
    req_node_id: NodeID,
) -> MsgPack<BatchDeleteClientKvMetaCacheResp> {
    tracing::debug!(
        "Handling BatchDeleteClientKvMetaCacheReq from node {}: {} items",
        req_node_id,
        req.serialize_part.delete_items.len()
    );

    let mut deleted_count = 0u32;

    for delete_item in &req.serialize_part.delete_items {
        tracing::debug!(
            "Processing delete item: key={}, put_time_ms={}, put_version={}",
            delete_item.key,
            delete_item.put_time_ms,
            delete_item.put_version
        );

        apply_delete_client_kv_meta_cache_item(view, delete_item).await;
        deleted_count += 1;
    }

    tracing::debug!(
        "Batch delete completed for node {}: {} items processed",
        req_node_id,
        deleted_count
    );

    MsgPack {
        serialize_part: BatchDeleteClientKvMetaCacheResp {
            deleted_count,
            error_code: OK,
            error_json: String::new(),
        },
        raw_bytes: Vec::new(),
    }
}
