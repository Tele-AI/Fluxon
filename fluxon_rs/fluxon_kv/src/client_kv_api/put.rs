use super::ClientKvApiInner;
use crate::cluster_manager::NodeIDString;
use crate::cluster_manager::app_logic_ext::ClusterManagerAppLogicExt;
use crate::master_kv_router::put::PutIDForAKey;
// no StageScope; timestamps-based metrics only
use crate::memholder::kvclient_encode::{calc_flat_dict_encoded_len, write_flat_dict_ptrs_to_ptr};
use crate::observe_kvope::{
    obe_put_start_error_rpc, obe_put_start_error_status, obe_put_start_success,
    obe_put_transfer_error,
};
use crate::{
    master_kv_router::msg_pack::{PutDoneReq, PutRevokeReq, PutStartReq, PutStartResp},
    p2p::msg_pack::MsgPack,
    rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult},
};
use chrono::Utc;
use fluxon_commu::TransferBreakdown;
use std::time::Instant;
use tracing::info;

fn duration_to_i64_us(duration: std::time::Duration) -> i64 {
    duration.as_micros().min(i64::MAX as u128) as i64
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PutEndStats {
    pub master_put_end_rpc_us: i64,
    pub master_put_end_server_us: i64,
}

impl ClientKvApiInner {
    async fn put_common<F>(
        &self,
        key: &str,
        payload_len: u64,
        len_for_start: u32,
        reject_if_inflight_same_key: bool,
        preferred_sub_cluster: Option<&str>,
        lease_id: Option<u64>,
        _test_payload_len_u32: u32,
        _test_remove_after_fill: bool,
        fill_abs_src: F,
        dbg_addr_summary: bool,
        info_complete_tag: Option<&'static str>,
    ) -> KvResult<()>
    where
        F: FnOnce(u64),
    {
        let client_id = self.client_id_str();
        let node_role = self.node_role();
        let metrics = self.metrics_handle();

        let t1 = Utc::now().timestamp_micros();
        let (resp, _rpc_latency) = {
            match self
                .put_start(
                    key,
                    len_for_start,
                    reject_if_inflight_same_key,
                    preferred_sub_cluster,
                )
                .await
            {
                Ok(resp) => resp,
                Err(err) => {
                    obe_put_start_error_rpc(&metrics, &client_id, &node_role, key, payload_len);
                    return Err(err);
                }
            }
        };
        let t2 = Utc::now().timestamp_micros();
        if let Err(e) =
            crate::rpcresp_kvresult_convert::try_from_code(resp.error_code, resp.error_json.clone())
        {
            obe_put_start_error_status(&metrics, &client_id, &node_role, key, payload_len);
            return Err(e);
        }
        obe_put_start_success(&metrics, &client_id, &node_role, key, t1, t2);

        let put_id = resp.put_id;
        let peer_id = if &*resp.node_id == &*self.view.cluster_manager().get_self_info().id {
            None
        } else {
            Some(resp.node_id.clone())
        };
        let abs_src = resp.src_addr;
        let abs_target = resp.target_addr;

        #[cfg(test)]
        {
            self.test_record.add_transfering_put(
                key.to_string(),
                _test_payload_len_u32,
                put_id.0,
                put_id.1,
                resp.node_id.to_string(),
                format!("{:#x}", resp.target_addr),
            );
        }

        if self.short_circuit_put_payload_path_enabled() {
            #[cfg(test)]
            {
                if _test_remove_after_fill {
                    self.test_record
                        .remove_transfering_put(key.to_string(), put_id);
                }
            }

            let skipped_breakdown = if peer_id.is_none() && abs_src == abs_target {
                TransferBreakdown {
                    local_noop: true,
                    ..TransferBreakdown::default()
                }
            } else {
                TransferBreakdown::default()
            };
            metrics.pending_put_set_transfer_breakdown(
                put_id,
                skipped_breakdown.submit_blocking_us,
                skipped_breakdown.create_xfer_req_us,
                skipped_breakdown.post_xfer_req_us,
                skipped_breakdown.poll_wait_us,
                skipped_breakdown.poll_iters,
                skipped_breakdown.used_fast_path,
                skipped_breakdown.local_noop,
                skipped_breakdown.remote_transfer,
            );
            self.put_end(key, put_id, lease_id).await?;
            if let Some(tag) = info_complete_tag {
                info!("{tag} complete key={} bytes={}", key, payload_len);
            }
            return Ok(());
        }

        fill_abs_src(abs_src);

        #[cfg(test)]
        {
            if _test_remove_after_fill {
                self.test_record
                    .remove_transfering_put(key.to_string(), put_id);
            }
        }

        let base_addr = self
            .view
            .client_seg_pool()
            .cpu_mem_read_guard()
            .await
            .unwrap()
            .allocated_addr;
        let src_offset = abs_src - base_addr;
        let (target_offset, target_base_addr_opt) = match &peer_id {
            Some(_) => (
                abs_target - resp.target_base_addr,
                Some(resp.target_base_addr),
            ),
            None => (abs_target - base_addr, None),
        };
        if dbg_addr_summary {
            tracing::debug!(
                "put path addr summary: key={}, put_id=({},{}) local_base={:#x}, abs_src={:#x}, src_off={:#x}, master_target_base={:#x}, abs_target={:#x}, tgt_off={:#x}, peer_id={:?}",
                key,
                put_id.0,
                put_id.1,
                base_addr,
                abs_src,
                src_offset,
                target_base_addr_opt.unwrap_or(base_addr),
                abs_target,
                target_offset,
                peer_id
            );
        }

        let transfer_breakdown = match self
            .put_transfer(
                key,
                put_id,
                src_offset,
                target_offset,
                payload_len,
                peer_id.clone(),
                target_base_addr_opt,
            )
            .await
        {
            Ok(breakdown) => breakdown,
            Err(e) => {
                self.put_revoke(key, put_id).await?;
                obe_put_transfer_error(&metrics, &client_id, &node_role, key, payload_len);
                return Err(e);
            }
        };
        metrics.pending_put_set_transfer_breakdown(
            put_id,
            transfer_breakdown.submit_blocking_us,
            transfer_breakdown.create_xfer_req_us,
            transfer_breakdown.post_xfer_req_us,
            transfer_breakdown.poll_wait_us,
            transfer_breakdown.poll_iters,
            transfer_breakdown.used_fast_path,
            transfer_breakdown.local_noop,
            transfer_breakdown.remote_transfer,
        );

        if self.skip_put_end_commit_enabled() {
            let _ = metrics.pending_put_remove(&put_id);
            tracing::warn!(
                "skip_put_end_commit test-only fast-path: returning success without put_end; key={} put_id=({},{}) payload_len={}",
                key,
                put_id.0,
                put_id.1,
                payload_len
            );
            if let Some(tag) = info_complete_tag {
                info!(
                    "{tag} complete_without_put_end key={} bytes={}",
                    key, payload_len
                );
            }
            return Ok(());
        }

        self.put_end(key, put_id, lease_id).await?;
        self.cache_metadata_only_after_put(key, put_id);
        if let Some(tag) = info_complete_tag {
            info!("{tag} complete key={} bytes={}", key, payload_len);
        }
        Ok(())
    }

    /// Put a key/value by encoding a flat dict from raw pointers directly into the segment pool.
    ///
    /// # Safety
    /// The caller must guarantee the pointer ranges remain readable for the duration of this async call.
    pub async unsafe fn put_flat_dict_ptrs(
        &self,
        key: &str,
        ptrs: Vec<(u8, usize, u32, u64, u32, Option<u32>)>,
        opts: crate::client_kv_api::PutOptionalArgs,
    ) -> KvResult<()> {
        let lease_id = opts.lease_id();
        let reject_if_inflight_same_key = opts.reject_if_inflight_same_key();
        let preferred_sub_cluster = opts.preferred_sub_cluster().map(|s| s.to_string());

        let payload_len = calc_flat_dict_encoded_len(&ptrs)?;
        self.put_common(
            key,
            payload_len,
            payload_len as u32,
            reject_if_inflight_same_key,
            preferred_sub_cluster.as_deref(),
            lease_id,
            payload_len as u32,
            /*test_remove_after_fill=*/ false,
            move |abs_src| {
                // Fill owner's shared memory at abs_src directly from the raw pointers.
                unsafe {
                    write_flat_dict_ptrs_to_ptr(abs_src as *mut u8, &ptrs);
                }
            },
            /*dbg_addr_summary=*/ false,
            Some("put_flat_dict_ptrs"),
        )
        .await
    }

    /// Put a key/value with optional args (e.g., lease binding)
    pub async fn put(
        &self,
        key: &str,
        value: &[u8],
        opts: crate::client_kv_api::PutOptionalArgs,
    ) -> KvResult<()> {
        let lease_id = opts.lease_id();
        let reject_if_inflight_same_key = opts.reject_if_inflight_same_key();
        let preferred_sub_cluster = opts.preferred_sub_cluster().map(|s| s.to_string());
        let payload_len = value.len() as u64;
        self.put_common(
            key,
            payload_len,
            value.len() as u32,
            reject_if_inflight_same_key,
            preferred_sub_cluster.as_deref(),
            lease_id,
            value.len() as u32,
            /*test_remove_after_fill=*/ true,
            |abs_src| unsafe {
                std::ptr::copy_nonoverlapping(value.as_ptr(), abs_src as *mut u8, value.len());
            },
            /*dbg_addr_summary=*/ true,
            None,
        )
        .await
    }

    /// Transfer data by offsets with instrumentation for external/owner callers.
    /// Records transfer latency (t2..t3) and emits tsbuckets pulses.
    pub async fn put_transfer(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        src_offset: u64,
        target_offset: u64,
        len: u64,
        peer_id: Option<NodeIDString>,
        target_base_addr: Option<u64>,
    ) -> KvResult<TransferBreakdown> {
        let metrics = self.metrics_handle();
        let client_id = self.client_id_str();
        let node_role = self.node_role();

        // owner/external inner is stable after construction; base_addr must exist
        let base_addr = self
            .view
            .client_seg_pool()
            .cpu_mem_read_guard()
            .await
            .unwrap()
            .allocated_addr;
        let abs_src = base_addr + src_offset;
        let abs_target = if peer_id.is_some() {
            let Some(tb) = target_base_addr else {
                // propagate as Unreachable: invalid remote target context from distributed input
                let err = crate::rpcresp_kvresult_convert::msg_and_error::KvError::Unreachable(
                    crate::rpcresp_kvresult_convert::msg_and_error::UnreachableError::RpcDecodeError {
                        rpc_input_json: format!(
                            "missing target_base_addr while peer_id present; src_off={:#x}, tgt_off={:#x}",
                            src_offset, target_offset
                        ),
                    },
                );
                return Err(err);
            };
            tb + target_offset
        } else {
            base_addr + target_offset
        };

        // Local placement can resolve to src==target, which means the payload is already in-place.
        // Skip the transfer-engine hop for this no-op path to avoid paying an extra fixed cost.
        if peer_id.is_none() && abs_src == abs_target {
            tracing::debug!(
                "put_transfer local no-op: key={}, put_id=({},{}) src==target {:#x}, len={}",
                key,
                put_id.0,
                put_id.1,
                abs_target,
                len
            );
            return Ok(TransferBreakdown {
                local_noop: true,
                ..TransferBreakdown::default()
            });
        } else {
            let breakdown = self
                .view
                .client_transfer_engine()
                .transfer_data_no_copy(peer_id.clone(), false, abs_src, abs_target, len, None)
                .await?;
            tracing::debug!(
                "put_transfer breakdown: key={}, put_id=({},{}) fast_path={} local_noop={} remote_transfer={} submit_blocking_us={} create_xfer_req_us={} post_xfer_req_us={} poll_wait_us={} poll_iters={}",
                key,
                put_id.0,
                put_id.1,
                breakdown.used_fast_path,
                breakdown.local_noop,
                breakdown.remote_transfer,
                breakdown.submit_blocking_us,
                breakdown.create_xfer_req_us,
                breakdown.post_xfer_req_us,
                breakdown.poll_wait_us,
                breakdown.poll_iters
            );
            tracing::debug!(
                "put_transfer success: key={}, put_id=({},{}) src_off={:#x}, tgt_off={:#x}, len={}, peer_id={:?}",
                key,
                put_id.0,
                put_id.1,
                src_offset,
                target_offset,
                len,
                peer_id
            );

            // Emit transfer stage success and tsbuckets pulse (computes t2/t3 using pending)
            crate::observe_kvope::obe_put_transfer_success(
                &metrics, &client_id, &node_role, key, len, put_id,
            );
            return Ok(breakdown);
        }
        #[allow(unreachable_code)]
        Ok(TransferBreakdown::default())
    }

    /// 开始 Put 操作，分配存储空间
    pub async fn put_start_with_source_node(
        &self,
        key: &str,
        len: u32,
        reject_if_inflight_same_key: bool,
        preferred_sub_cluster: Option<&str>,
        source_node_id: Option<NodeIDString>,
    ) -> KvResult<(PutStartResp, i64)> {
        let req = MsgPack {
            serialize_part: PutStartReq {
                key: key.to_string(),
                len: len as u64,
                reject_if_inflight_same_key,
                preferred_sub_cluster: preferred_sub_cluster.map(|s| s.to_string()),
                source_node_id,
            },
            raw_bytes: Vec::new(),
        };

        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let rpc_started_at = Instant::now();
        let start_rpc_timestamp = Utc::now().timestamp_micros() as i64;
        let resp = self
            .rpc_caller_put_start
            .call(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                2,
            )
            .await
            .map_err(|e| KvError::P2p(e))?;
        let end_rpc_timestamp = Utc::now().timestamp_micros() as i64;
        let ser = resp.serialize_part.clone();
        if crate::rpcresp_kvresult_convert::try_from_code(ser.error_code, ser.error_json.clone())
            .is_ok()
        {
            let metrics = self.metrics_handle();
            metrics.pending_put_insert(
                ser.put_id,
                key.to_string(),
                len as u64,
                start_rpc_timestamp,
                end_rpc_timestamp,
                ser.server_process_us,
            );
        }
        let rpc_latency_us = duration_to_i64_us(rpc_started_at.elapsed());
        Ok((ser, rpc_latency_us))
    }

    /// 开始 Put 操作，分配存储空间
    pub async fn put_start(
        &self,
        key: &str,
        len: u32,
        reject_if_inflight_same_key: bool,
        preferred_sub_cluster: Option<&str>,
    ) -> KvResult<(PutStartResp, i64)> {
        self.put_start_with_source_node(
            key,
            len,
            reject_if_inflight_same_key,
            preferred_sub_cluster,
            None,
        )
        .await
    }

    /// 撤销 Put 操作，释放已分配的资源
    pub async fn put_revoke(&self, key: &str, put_id: PutIDForAKey) -> KvResult<()> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting put_revoke".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: PutRevokeReq {
                key: key.to_string(),
                put_id,
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
        let _resp = self
            .rpc_caller_put_revoke
            .call(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                2,
            )
            .await
            .map_err(KvError::from)?;
        // cleanup pending stat if any
        let _ = self.metrics_handle().pending_put_remove(&put_id);
        Ok(())
    }

    /// 完成 Put 操作，提交数据（inner，无监控）
    pub async fn put_end_inner(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        lease_id: Option<u64>,
    ) -> KvResult<PutEndStats> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting put_end".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: PutDoneReq {
                key: key.to_string(),
                put_id,
                lease_id,
            },
            raw_bytes: Vec::new(),
        };

        // 获取 master 节点 ID
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let rpc_started_at = Instant::now();

        // 调用 RPC
        let resp = self
            .rpc_caller_put_done
            .call(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                0,
            )
            .await
            .map_err(KvError::from)?;
        if let Err(e) = crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        ) {
            return Err(e);
        }
        Ok(PutEndStats {
            master_put_end_rpc_us: duration_to_i64_us(rpc_started_at.elapsed()),
            master_put_end_server_us: resp.serialize_part.server_process_us,
        })
    }

    /// 完成 Put 操作，提交数据（带监控）：适配 external 路径，统一聚合 t1..t4
    pub async fn put_end(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        lease_id: Option<u64>,
    ) -> KvResult<PutEndStats> {
        let metrics = self.metrics_handle();
        let client_id = self.client_id_str();
        let node_role = self.node_role();

        let end_stats = match self.put_end_inner(key, put_id, lease_id).await {
            Ok(stats) => stats,
            Err(e) => {
                // on error, emit end error using pending info if exists, then cleanup
                crate::observe_kvope::obe_put_end_error_from_pending(
                    &metrics, &client_id, &node_role, put_id,
                );
                return Err(e);
            }
        };

        // record end_handle to pending before aggregation
        metrics.pending_put_set_end_handle(put_id, end_stats.master_put_end_server_us);

        // success: aggregate with pending timestamps; this also clears pending
        crate::observe_kvope::obe_put_done_success_from_pending(
            &metrics, &client_id, &node_role, key, put_id, 0,
        );
        Ok(end_stats)
    }
}
