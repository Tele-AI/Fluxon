use super::{ClientKvApiInner, KvMetrics};
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
        BatchGetDoneReq, BatchGetDoneResp, BatchGetRevokeReq, BatchGetRevokeResp,
        BatchGetStartItemResp, BatchGetStartReq, BatchGetStartResp, BatchIsExistReq,
        GetAllocationMode, GetDoneReq, GetDoneResp, GetMetaReq, GetMetaResp, GetRevokeReq,
        GetStartReq, GetStartResp,
    },
    p2p::msg_pack::MsgPack,
    rpcresp_kvresult_convert::msg_and_error::codes_api,
    rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult, OK},
};
use chrono::Utc;
use futures::stream::{self, StreamExt};
use limit_thirdparty::tokio;
use std::sync::Arc;
use std::time::Instant;

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

impl RemoteGetInfo {
    pub fn data_len(&self) -> usize {
        self.data_len
    }

    pub fn is_remote_transfer(&self) -> bool {
        self.peer_is_src_or_target
    }
}

impl ClientKvApiInner {
    pub async fn batch_get_finish_started(
        &self,
        keys: Vec<String>,
        start_items: Vec<BatchGetStartItemResp>,
        transfer_concurrency: usize,
    ) -> KvResult<Vec<KvResult<Option<(Arc<UserMemHolder>, Option<RemoteGetInfo>)>>>> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_get_finish_started"
                    .to_string(),
            }));
        }
        if keys.len() != start_items.len() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "batch_get_finish_started length mismatch: keys={} start_items={}",
                    keys.len(),
                    start_items.len()
                ),
            }));
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        #[derive(Clone)]
        struct DonePending {
            idx: usize,
            key: String,
            start_item: BatchGetStartItemResp,
            peer_is_remote: bool,
            transfer_us: i64,
        }

        let transfer_concurrency = transfer_concurrency.max(1);
        let metrics = self.metrics_handle();
        let client_id = self.client_id_str();
        let node_role = self.node_role();
        let self_node_id = self.view.cluster_manager().get_self_info().id.clone();

        let mut results: Vec<
            Option<KvResult<Option<(Arc<UserMemHolder>, Option<RemoteGetInfo>)>>>,
        > = (0..keys.len()).map(|_| None).collect();
        let mut done_pending = Vec::new();
        let mut revoke_get_ids = Vec::new();
        let mut transfer_futures = Vec::new();

        for (idx, (key, start_item)) in keys.into_iter().zip(start_items.into_iter()).enumerate() {
            if start_item.error_code == codes_api::API_KEY_NOT_FOUND {
                results[idx] = Some(Ok(None));
                continue;
            }
            if let Err(err) = crate::rpcresp_kvresult_convert::try_from_code(
                start_item.error_code,
                start_item.error_json.clone(),
            ) {
                results[idx] = Some(Err(err));
                continue;
            }

            let peer_id = if start_item.node_id == self_node_id {
                None
            } else {
                Some(start_item.node_id.clone())
            };
            let peer_is_remote = peer_id.is_some();
            let get_id = start_item.get_id;
            let src_addr = start_item.src_addr;
            let target_addr = start_item.target_addr;
            let len = start_item.len;

            if peer_id.is_none() && src_addr == target_addr {
                done_pending.push(DonePending {
                    idx,
                    key,
                    start_item,
                    peer_is_remote,
                    transfer_us: 0,
                });
                continue;
            }

            transfer_futures.push(async move {
                let transfer_started_at = Instant::now();
                let transfer_result = self
                    .view
                    .client_transfer_engine()
                    .transfer_data_no_copy(peer_id, true, src_addr, target_addr, len, None)
                    .await
                    .map_err(|err| {
                        KvError::Api(ApiError::Transfer {
                            from_addr: src_addr,
                            to_addr: target_addr,
                            len,
                            error: err.to_string(),
                        })
                    });
                let transfer_us = transfer_started_at
                    .elapsed()
                    .as_micros()
                    .min(i64::MAX as u128) as i64;
                (
                    idx,
                    key,
                    start_item,
                    peer_is_remote,
                    get_id,
                    transfer_us,
                    transfer_result,
                )
            });
        }

        let mut transfer_stream =
            stream::iter(transfer_futures).buffer_unordered(transfer_concurrency);
        while let Some(joined) = transfer_stream.next().await {
            match joined {
                (idx, key, start_item, peer_is_remote, _get_id, transfer_us, Ok(_breakdown)) => {
                    done_pending.push(DonePending {
                        idx,
                        key,
                        start_item,
                        peer_is_remote,
                        transfer_us,
                    });
                }
                (idx, _key, _start_item, _peer_is_remote, get_id, _transfer_us, Err(err)) => {
                    results[idx] = Some(Err(err));
                    revoke_get_ids.push(get_id);
                }
            }
        }

        if !revoke_get_ids.is_empty() {
            if let Err(err) = self.batch_get_revoke(revoke_get_ids).await {
                tracing::warn!("batch_get_revoke failed after transfer errors: {}", err);
            }
        }

        let done_resp = self
            .batch_get_done(
                done_pending
                    .iter()
                    .map(|pending| pending.start_item.get_id)
                    .collect(),
            )
            .await?;
        if done_resp.items.len() != done_pending.len() {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "batch_get_done response length mismatch: expected={} got={}",
                    done_pending.len(),
                    done_resp.items.len()
                ),
            }));
        }
        let master_node_id: NodeID = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?
            .into();

        for (pending, done_item) in done_pending.into_iter().zip(done_resp.items.into_iter()) {
            if let Err(err) = crate::rpcresp_kvresult_convert::try_from_code(
                done_item.error_code,
                done_item.error_json.clone(),
            ) {
                results[pending.idx] = Some(Err(err));
                continue;
            }
            let expose_kind = if done_item.allocation_mode == GetAllocationMode::Temporary {
                UserMemHolderExposeKind::OwnedCopy
            } else {
                UserMemHolderExposeKind::SegPtr
            };
            let offset = pending.start_item.target_addr - pending.start_item.target_base_addr;
            let data_len = pending.start_item.len as usize;
            metrics.record_l2_hit_locality(pending.peer_is_remote, data_len as u64);
            metrics.record_get_io_locality(
                pending.peer_is_remote,
                data_len as u64,
                pending.transfer_us,
            );
            let memory_info = Arc::new(
                MemoryInfo::new(
                    offset,
                    pending.start_item.len as u32,
                    done_item.holder_id,
                    pending.key.clone(),
                    master_node_id.clone(),
                    self.view.clone(),
                )
                .await,
            );
            let get_info = RemoteGetInfo {
                get_id: pending.start_item.get_id,
                data_len,
                src_addr: pending.start_item.src_addr,
                target_addr: pending.start_item.target_addr,
                node_id: pending.start_item.node_id.clone().into(),
                peer_is_src_or_target: pending.peer_is_remote,
            };
            if done_item.allocation_mode != GetAllocationMode::Temporary {
                if self.install_get_cached_info_if_unfenced(
                    &pending.key,
                    pending.start_item.put_id,
                    memory_info.clone(),
                ) {
                    metrics.observe_cache_value_size(
                        &client_id,
                        node_role.as_str(),
                        data_len as u64,
                    );
                }
            }
            let user_mem_holder = Arc::new(UserMemHolder::new(
                memory_info,
                self.get_or_init_all_memholder_refcount(),
                expose_kind,
            ));
            results[pending.idx] = Some(Ok(Some((user_mem_holder, Some(get_info)))));
        }

        Ok(results
            .into_iter()
            .map(|item| {
                item.unwrap_or_else(|| {
                    Err(KvError::Api(ApiError::Unknown {
                        detail: "batch_get_finish_started result slot was not populated"
                            .to_string(),
                    }))
                })
            })
            .collect())
    }

    pub async fn batch_get(
        &self,
        keys: Vec<String>,
        transfer_concurrency: usize,
    ) -> KvResult<Vec<KvResult<Option<(Arc<UserMemHolder>, Option<RemoteGetInfo>)>>>> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_get".to_string(),
            }));
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        #[derive(Clone)]
        struct DonePending {
            idx: usize,
            key: String,
            start_item: crate::master_kv_router::msg_pack::BatchGetStartItemResp,
            peer_is_remote: bool,
            transfer_us: i64,
        }

        let transfer_concurrency = transfer_concurrency.max(1);
        let metrics = self.metrics_handle();
        let client_id = self.client_id_str();
        let node_role = self.node_role();
        let self_node_id = self.view.cluster_manager().get_self_info().id.clone();

        let mut results: Vec<
            Option<KvResult<Option<(Arc<UserMemHolder>, Option<RemoteGetInfo>)>>>,
        > = (0..keys.len()).map(|_| None).collect();
        let mut missing_indices = Vec::new();
        let mut missing_keys = Vec::new();

        for (idx, key) in keys.iter().enumerate() {
            if let Some(memory_info) = self.local_visible_mem_holder(key) {
                tracing::debug!(
                    "batch_get local visible hit for key: {}, directly return",
                    key
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
                metrics.record_get_io_locality(false, memory_info.len as u64, 0);
                results[idx] = Some(Ok(Some((user_mem_holder, None))));
            } else {
                obe_get_cache_miss(&metrics, &client_id, &node_role, key);
                missing_indices.push(idx);
                missing_keys.push(key.clone());
            }
        }

        if missing_keys.is_empty() {
            return Ok(results
                .into_iter()
                .map(|item| {
                    item.unwrap_or_else(|| {
                        Err(KvError::Api(ApiError::Unknown {
                            detail: "batch_get result slot was not populated".to_string(),
                        }))
                    })
                })
                .collect());
        }

        let start_resp = self.batch_get_start(missing_keys.clone()).await?;
        if start_resp.items.len() != missing_keys.len() {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "batch_get_start response length mismatch: expected={} got={}",
                    missing_keys.len(),
                    start_resp.items.len()
                ),
            }));
        }

        let mut done_pending = Vec::new();
        let mut revoke_get_ids = Vec::new();
        let mut transfer_futures = Vec::new();

        for ((idx, key), start_item) in missing_indices
            .into_iter()
            .zip(missing_keys.into_iter())
            .zip(start_resp.items.into_iter())
        {
            if start_item.error_code == codes_api::API_KEY_NOT_FOUND {
                results[idx] = Some(Ok(None));
                continue;
            }
            if let Err(err) = crate::rpcresp_kvresult_convert::try_from_code(
                start_item.error_code,
                start_item.error_json.clone(),
            ) {
                results[idx] = Some(Err(err));
                continue;
            }

            let peer_id = if start_item.node_id == self_node_id {
                None
            } else {
                Some(start_item.node_id.clone())
            };
            let peer_is_remote = peer_id.is_some();
            let get_id = start_item.get_id;
            let src_addr = start_item.src_addr;
            let target_addr = start_item.target_addr;
            let len = start_item.len;

            if peer_id.is_none() && src_addr == target_addr {
                done_pending.push(DonePending {
                    idx,
                    key,
                    start_item,
                    peer_is_remote,
                    transfer_us: 0,
                });
                continue;
            }

            transfer_futures.push(async move {
                let transfer_started_at = Instant::now();
                let transfer_result = self
                    .view
                    .client_transfer_engine()
                    .transfer_data_no_copy(peer_id, true, src_addr, target_addr, len, None)
                    .await
                    .map_err(|err| {
                        KvError::Api(ApiError::Transfer {
                            from_addr: src_addr,
                            to_addr: target_addr,
                            len,
                            error: err.to_string(),
                        })
                    });
                let transfer_us = transfer_started_at
                    .elapsed()
                    .as_micros()
                    .min(i64::MAX as u128) as i64;
                (
                    idx,
                    key,
                    start_item,
                    peer_is_remote,
                    get_id,
                    transfer_us,
                    transfer_result,
                )
            });
        }

        let mut transfer_stream =
            stream::iter(transfer_futures).buffer_unordered(transfer_concurrency);
        while let Some(joined) = transfer_stream.next().await {
            match joined {
                (idx, key, start_item, peer_is_remote, _get_id, transfer_us, Ok(_breakdown)) => {
                    done_pending.push(DonePending {
                        idx,
                        key,
                        start_item,
                        peer_is_remote,
                        transfer_us,
                    });
                }
                (idx, _key, _start_item, _peer_is_remote, get_id, _transfer_us, Err(err)) => {
                    results[idx] = Some(Err(err));
                    revoke_get_ids.push(get_id);
                }
            }
        }

        if !revoke_get_ids.is_empty() {
            if let Err(err) = self.batch_get_revoke(revoke_get_ids).await {
                tracing::warn!("batch_get_revoke failed after transfer errors: {}", err);
            }
        }

        let done_resp = self
            .batch_get_done(
                done_pending
                    .iter()
                    .map(|pending| pending.start_item.get_id)
                    .collect(),
            )
            .await?;
        if done_resp.items.len() != done_pending.len() {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "batch_get_done response length mismatch: expected={} got={}",
                    done_pending.len(),
                    done_resp.items.len()
                ),
            }));
        }
        let master_node_id: NodeID = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?
            .into();

        for (pending, done_item) in done_pending.into_iter().zip(done_resp.items.into_iter()) {
            if let Err(err) = crate::rpcresp_kvresult_convert::try_from_code(
                done_item.error_code,
                done_item.error_json.clone(),
            ) {
                results[pending.idx] = Some(Err(err));
                continue;
            }
            let expose_kind = if done_item.allocation_mode == GetAllocationMode::Temporary {
                UserMemHolderExposeKind::OwnedCopy
            } else {
                UserMemHolderExposeKind::SegPtr
            };
            let offset = pending.start_item.target_addr - pending.start_item.target_base_addr;
            let data_len = pending.start_item.len as usize;
            metrics.record_l2_hit_locality(pending.peer_is_remote, data_len as u64);
            metrics.record_get_io_locality(
                pending.peer_is_remote,
                data_len as u64,
                pending.transfer_us,
            );
            let memory_info = Arc::new(
                MemoryInfo::new(
                    offset,
                    pending.start_item.len as u32,
                    done_item.holder_id,
                    pending.key.clone(),
                    master_node_id.clone(),
                    self.view.clone(),
                )
                .await,
            );
            let get_info = RemoteGetInfo {
                get_id: pending.start_item.get_id,
                data_len,
                src_addr: pending.start_item.src_addr,
                target_addr: pending.start_item.target_addr,
                node_id: pending.start_item.node_id.clone().into(),
                peer_is_src_or_target: pending.peer_is_remote,
            };
            if done_item.allocation_mode != GetAllocationMode::Temporary {
                if self.install_get_cached_info_if_unfenced(
                    &pending.key,
                    pending.start_item.put_id,
                    memory_info.clone(),
                ) {
                    metrics.observe_cache_value_size(
                        &client_id,
                        node_role.as_str(),
                        data_len as u64,
                    );
                }
            }
            let user_mem_holder = Arc::new(UserMemHolder::new(
                memory_info,
                self.get_or_init_all_memholder_refcount(),
                expose_kind,
            ));
            results[pending.idx] = Some(Ok(Some((user_mem_holder, Some(get_info)))));
        }

        Ok(results
            .into_iter()
            .map(|item| {
                item.unwrap_or_else(|| {
                    Err(KvError::Api(ApiError::Unknown {
                        detail: "batch_get result slot was not populated".to_string(),
                    }))
                })
            })
            .collect())
    }

    pub async fn batch_is_exist(
        &self,
        keys: Vec<String>,
        allow_local_snapshot: bool,
    ) -> KvResult<Vec<bool>> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_is_exist".to_string(),
            }));
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = vec![false; keys.len()];
        let mut missing_indices = Vec::new();
        let mut missing_keys = Vec::new();
        for (idx, key) in keys.iter().enumerate() {
            if allow_local_snapshot && self.has_local_snapshot(key) {
                results[idx] = true;
                continue;
            }
            missing_indices.push(idx);
            missing_keys.push(key.clone());
        }
        if missing_keys.is_empty() {
            return Ok(results);
        }

        let req = MsgPack {
            serialize_part: BatchIsExistReq { keys: missing_keys },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = self
            .rpc_caller_batch_is_exist
            .call(self.view.p2p_module(), master_node_id.into(), req, None, 0)
            .await
            .map_err(KvError::from)?;
        let resp_part = resp.serialize_part;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp_part.error_code,
            resp_part.error_json.clone(),
        )?;
        if resp_part.exists_list.len() != missing_indices.len() {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "batch_is_exist response length mismatch: expected={} got={}",
                    missing_indices.len(),
                    resp_part.exists_list.len()
                ),
            }));
        }
        for (idx, exists) in missing_indices
            .into_iter()
            .zip(resp_part.exists_list.into_iter())
        {
            results[idx] = exists;
        }
        Ok(results)
    }

    pub async fn is_exist_with_local_snapshot(
        &self,
        key: &str,
        allow_local_snapshot: bool,
    ) -> KvResult<bool> {
        let mut results = self
            .batch_is_exist(vec![key.to_string()], allow_local_snapshot)
            .await?;
        Ok(results.pop().unwrap_or(false))
    }

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

        if let Some(memory_info) = self.local_visible_mem_holder(key) {
            // exist, directly return
            tracing::debug!("local visible cache hit for key: {}, directly return", key);
            // Build a fresh UserMemHolder from cached MemoryInfo
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

        let lock = self.get_remote_kv_lock.get_lock(key.to_owned());
        let _guard = lock.lock().await;

        // Recheck after acquiring the miss lock so concurrent cache-fillers can collapse here
        // without forcing every cache hit through the async lock path.
        if let Some(memory_info) = self.local_visible_mem_holder(key) {
            tracing::debug!(
                "local visible cache hit after miss-lock for key: {}, directly return",
                key
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
            if self.install_get_cached_info_if_unfenced(key, put_id, memory_info.clone()) {
                metrics.observe_cache_value_size(&client_id, node_role.as_str(), data_len as u64);
            }
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
        self.is_exist_with_local_snapshot(key, false).await
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

    pub async fn batch_get_start(&self, keys: Vec<String>) -> KvResult<BatchGetStartResp> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_get_start".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: BatchGetStartReq { keys },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = self
            .rpc_caller_batch_get_start
            .call(self.view.p2p_module(), master_node_id.into(), req, None, 0)
            .await
            .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
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

    pub async fn batch_get_revoke(&self, get_ids: Vec<u64>) -> KvResult<BatchGetRevokeResp> {
        if get_ids.is_empty() {
            return Ok(BatchGetRevokeResp {
                items: Vec::new(),
                error_code: OK,
                error_json: String::new(),
            });
        }
        let req = MsgPack {
            serialize_part: BatchGetRevokeReq { get_ids },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = self
            .rpc_caller_batch_get_revoke
            .call(self.view.p2p_module(), master_node_id.into(), req, None, 0)
            .await
            .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
        Ok(resp.serialize_part)
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

    pub async fn batch_get_done(&self, get_ids: Vec<u64>) -> KvResult<BatchGetDoneResp> {
        if get_ids.is_empty() {
            return Ok(BatchGetDoneResp {
                items: Vec::new(),
                error_code: OK,
                error_json: String::new(),
                server_process_us: 0,
            });
        }
        let req = MsgPack {
            serialize_part: BatchGetDoneReq { get_ids },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = self
            .rpc_caller_batch_get_done
            .call(self.view.p2p_module(), master_node_id.into(), req, None, 0)
            .await
            .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
        Ok(resp.serialize_part)
    }
}
