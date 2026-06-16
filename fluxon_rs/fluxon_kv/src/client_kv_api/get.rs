use super::ClientKvApiInner;
use crate::client_kv_api::CachedValue;
use crate::cluster_manager::app_logic_ext::ClusterManagerAppLogicExt;
use crate::memholder::{MemoryInfo, UserMemHolder, UserMemHolderExposeKind};
// no StageScope; timestamps-based metrics only
use crate::observe_kvope::{
    obe_get_cache_hit, obe_get_cache_miss, obe_get_done_error_status, obe_get_done_success,
    obe_get_end_error_rpc, obe_get_start_error_rpc, obe_get_start_error_status,
    obe_get_start_not_found, obe_get_start_success, obe_get_transfer_error,
    obe_get_transfer_success,
};
use crate::{
    cluster_manager::NodeID,
    master_kv_router::msg_pack::{
        GetAllocationMode, GetDoneReq, GetDoneResp, GetMetaReq, GetMetaResp, GetRevokeReq,
        GetStartReq, GetStartResp,
    },
    p2p::msg_pack::MsgPack,
    rpcresp_kvresult_convert::msg_and_error::codes_api,
    rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult, OK},
};
use chrono::Utc;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct RemoteGetInfo {
    get_id: u64,
    data_len: usize,
    src_addr: u64,
    target_addr: u64,
    node_id: NodeID,
    peer_is_src_or_target: bool,
}

impl std::fmt::Display for RemoteGetInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GetInfo{{ get_id: {}, data_len: {} bytes, src_addr: {:#x}, target_addr: {:#x}, node_id: {:?}, remote_transfer: {} }}",
            self.get_id,
            self.data_len,
            self.src_addr,
            self.target_addr,
            self.node_id,
            self.peer_is_src_or_target
        )
    }
}

impl ClientKvApiInner {
    /// becaused we cached local kv metadata, so we make `MemHolder` with Arc here
    pub async fn get(
        &self,
        key: &str,
    ) -> KvResult<Option<(Arc<UserMemHolder>, Option<RemoteGetInfo>)>> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting get".to_string(),
            }));
        }
        let metrics = self.metrics_handle();
        let client_id = self.client_id_str();
        let node_role = self.node_role();

        if let Some(cached_info) = self.get_cached_info.get(key) {
            if let CachedValue::LocalReplica(memory_info) = &cached_info.value {
                tracing::debug!(
                    "cache hit for key: {} with putid({},{}), directly return",
                    key,
                    cached_info.put_time_ms,
                    cached_info.put_version
                );
                let user_mem_holder = Arc::new(UserMemHolder::new(
                    memory_info.clone(),
                    self.get_or_init_all_memholder_refcount(),
                    UserMemHolderExposeKind::SegPtr,
                ));
                obe_get_cache_hit(
                    &metrics,
                    &client_id,
                    &node_role,
                    key,
                    memory_info.len as u64,
                );
                return Ok(Some((user_mem_holder, None)));
            }
        }

        let lock = self.get_remote_kv_lock.get_lock(key.to_owned());
        let _guard = lock.lock().await;

        // Recheck after acquiring the miss lock so concurrent cache-fillers can collapse here
        // without forcing every cache hit through the async lock path.
        if let Some(cached_info) = self.get_cached_info.get(key) {
            if let CachedValue::LocalReplica(memory_info) = &cached_info.value {
                tracing::debug!(
                    "cache hit after miss-lock for key: {} with putid({},{}), directly return",
                    key,
                    cached_info.put_time_ms,
                    cached_info.put_version
                );
                let user_mem_holder = Arc::new(UserMemHolder::new(
                    memory_info.clone(),
                    self.get_or_init_all_memholder_refcount(),
                    UserMemHolderExposeKind::SegPtr,
                ));
                obe_get_cache_hit(
                    &metrics,
                    &client_id,
                    &node_role,
                    key,
                    memory_info.len as u64,
                );
                return Ok(Some((user_mem_holder, None)));
            }
        }

        obe_get_cache_miss(&metrics, &client_id, &node_role, key);
        let t1 = Utc::now().timestamp_micros();
        let resp = {
            match self.get_start(key).await {
                Ok(resp) => resp,
                Err(err) => {
                    obe_get_start_error_rpc(&metrics, &client_id, &node_role, key);
                    return Err(err);
                }
            }
        };
        let start_handle_us = resp.server_process_us;
        let t2 = Utc::now().timestamp_micros();
        // start stage success
        // Note: only record timestamps; no scope begin/end
        //       errors handled above and below
        if resp.error_code != OK {
            if resp.error_code == codes_api::API_KEY_NOT_FOUND {
                obe_get_start_not_found(&metrics, &client_id, &node_role, key);
                return Ok(None);
            }
            obe_get_start_error_status(&metrics, &client_id, &node_role, key);
            crate::rpcresp_kvresult_convert::try_from_code(
                resp.error_code,
                resp.error_json.clone(),
            )?;
            unreachable!("try_from_code should have returned Err for non-OK, unreachable");
        }
        obe_get_start_success(&metrics, &client_id, &node_role, key, t1, t2);

        let put_id = resp.put_id;
        let get_id = resp.get_id;
        let data_len = resp.len as usize;

        let abs_src = resp.src_addr;
        let abs_target = resp.target_addr;

        // debug get slice from src_addr and len
        tracing::debug!(
            "kv get src addr {:#x} to target addr {:#x}",
            abs_src,
            abs_target
        );

        let peer_id = if &*resp.node_id == &*self.view.cluster_manager().get_self_info().id {
            None
        } else {
            Some(resp.node_id.clone())
        };

        #[cfg(test)]
        {
            self.test_record.add_transfering_get(
                get_id,
                key.to_string(),
                data_len as u32,
                abs_target,
                resp.node_id.to_string(),
                peer_id.is_some(),
            );
        }

        // transfer data (skip if local and src==target to avoid redundant copy)
        if peer_id.is_none() && abs_src == abs_target {
            tracing::debug!(
                "kv get local no-op: src==target {:#x}, len={} (skip transfer)",
                abs_target,
                data_len
            );
        } else {
            // tracing::debug!(
            //     "kv get transfer in transfer engine path from {}",
            //     peer_id.as_ref().map(|v| &**v).unwrap_or("self")
            // );
            tracing::debug!(
                "p2p get transfer: key={}, remote_src={:#x} -> local_target={:#x}, len={}, peer={:?}",
                key,
                abs_src,
                abs_target,
                data_len,
                peer_id
            );
            if let Err(e) = self
                .view
                .client_transfer_engine()
                .transfer_data_no_copy(
                    peer_id.clone(),
                    true,
                    abs_src,
                    abs_target,
                    data_len as u64,
                    None,
                )
                .await
            {
                tracing::warn!("transfer data failed: {:?}", e);

                #[cfg(test)]
                {
                    self.test_record.remove_transfering_get(get_id);
                }

                obe_get_transfer_error(&metrics, &client_id, &node_role, key, data_len as u64);
                self.get_revoke(get_id).await?;
                return Err(KvError::Api(ApiError::Transfer {
                    from_addr: abs_src,
                    to_addr: abs_target,
                    len: data_len as u64,
                    error: e.to_string(),
                }));
            } else {
                tracing::debug!(
                    "get_transfer success key={}, src_addr={:#x}, target_addr={:#x}, len={}, peer_id={:?}",
                    key,
                    abs_src,
                    abs_target,
                    data_len,
                    peer_id
                );
            }
        }
        let t3 = Utc::now().timestamp_micros();
        obe_get_transfer_success(
            &metrics,
            &client_id,
            &node_role,
            key,
            data_len as u64,
            t2,
            t3,
        );

        // Removed post-transfer zero-header verification per request.

        // Complete the get operation and get holder_id
        let done_resp = match self.get_done(get_id).await {
            Ok(resp) => resp,
            Err(err) => {
                obe_get_end_error_rpc(&metrics, &client_id, &node_role, key, data_len as u64);
                return Err(err);
            }
        };
        let end_handle_us = done_resp.server_process_us;
        let t4 = Utc::now().timestamp_micros();
        if done_resp.error_code != OK {
            obe_get_done_error_status(&metrics, &client_id, &node_role, key, data_len as u64);
            #[cfg(test)]
            {
                self.test_record.remove_transfering_get(get_id);
            }

            crate::rpcresp_kvresult_convert::try_from_code(
                done_resp.error_code,
                done_resp.error_json.clone(),
            )?;
            unreachable!("error path should have returned above");
        }
        // end/done stage success and push detailed metrics
        obe_get_done_success(
            &metrics,
            &client_id,
            &node_role,
            key,
            data_len as u64,
            get_id,
            t1,
            t2,
            t3,
            t4,
            start_handle_us,
            end_handle_us,
        );

        #[cfg(test)]
        {
            self.test_record.remove_transfering_get(get_id);
        }

        // pulses and network bytes emitted inside obe_get_done_success

        let holder_id = done_resp.holder_id;
        let expose_kind = if done_resp.allocation_mode == GetAllocationMode::Temporary {
            UserMemHolderExposeKind::OwnedCopy
        } else {
            UserMemHolderExposeKind::SegPtr
        };
        let master_node_id: NodeID = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?
            .into();

        // Create MemHolder with keep alive functionality
        // Convert target_addr to offset using base address from master response
        let offset = resp.target_addr - resp.target_base_addr;
        let memory_info = Arc::new(
            MemoryInfo::new(
                offset,
                data_len as u32,
                holder_id,
                key.to_string(),
                master_node_id,
                self.view.clone(),
            )
            .await,
        );
        // Create GetInfo with information from the response
        let get_info = RemoteGetInfo {
            get_id,
            data_len,
            src_addr: abs_src,
            target_addr: abs_target,
            node_id: resp.node_id.into(),
            peer_is_src_or_target: true,
        };

        if done_resp.allocation_mode != GetAllocationMode::Temporary {
            self.cache_local_replica_after_get(key, put_id, memory_info.clone());
            metrics.observe_cache_value_size(&client_id, node_role.as_str(), data_len as u64);
        }
        let user_mem_holder = Arc::new(UserMemHolder::new(
            memory_info,
            self.get_or_init_all_memholder_refcount(),
            expose_kind,
        ));
        // let partial_hex=&user_mem_holder.bytes()[..std::cmp::min(16, user_mem_holder.bytes().len())];
        // tracing::debug!("external get done, key={}, partial_hex={:?}", key, partial_hex);
        Ok(Some((user_mem_holder, Some(get_info))))
    }

    pub async fn is_exist(&self, key: &str) -> KvResult<bool> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting is_exist".to_string(),
            }));
        }
        let resp = self.get_meta(key).await?;
        if resp.error_code != OK {
            // If error code indicates key not found, return false
            if resp.error_code
                == crate::rpcresp_kvresult_convert::msg_and_error::codes_api::API_KEY_NOT_FOUND
            {
                return Ok(false);
            }
            // For other errors, propagate the error
            crate::rpcresp_kvresult_convert::try_from_code(
                resp.error_code,
                resp.error_json.clone(),
            )?;
            unreachable!("error path should have returned above");
        }

        Ok(resp.exists)
    }

    /// Get metadata for a key without transferring data
    pub async fn get_meta(&self, key: &str) -> KvResult<GetMetaResp> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting get_meta".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: GetMetaReq {
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
            .rpc_caller_get_meta
            .call(self.view.p2p_module(), master_node_id.into(), req, None, 0)
            .await
            .map_err(KvError::from)?;

        Ok(resp.serialize_part)
    }

    /// 开始 Get 操作，获取数据位置和信息
    pub async fn get_start(&self, key: &str) -> KvResult<GetStartResp> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting get_start".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: GetStartReq {
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
            .rpc_caller_get_start
            .call(self.view.p2p_module(), master_node_id.into(), req, None, 0)
            .await
            .map_err(KvError::from)?;

        Ok(resp.serialize_part)
    }

    /// 撤销 Get 操作，释放已分配的资源
    pub async fn get_revoke(&self, get_id: u64) -> KvResult<()> {
        let req = MsgPack {
            serialize_part: GetRevokeReq { get_id },
            raw_bytes: Vec::new(),
        };

        // 获取 master 节点 ID
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;

        // 调用 RPC
        let _resp = self
            .rpc_caller_get_revoke
            .call(self.view.p2p_module(), master_node_id.into(), req, None, 0)
            .await
            .map_err(KvError::from)?;

        Ok(())
    }

    /// 完成 Get 操作，清理资源
    pub async fn get_done(&self, get_id: u64) -> KvResult<GetDoneResp> {
        let req = MsgPack {
            serialize_part: GetDoneReq { get_id },
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
            .rpc_caller_get_done
            .call(self.view.p2p_module(), master_node_id.into(), req, None, 0)
            .await
            .map_err(KvError::from)?;

        Ok(resp.serialize_part)
    }
}
