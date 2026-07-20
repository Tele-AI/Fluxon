use crate::client_kv_api::msg_pack::{
    ExternalDeleteReq, ExternalDeleteResp, ExternalGetReq, ExternalGetResp, ExternalIsExistReq,
    ExternalIsExistResp, ExternalPutCommitReq, ExternalPutCommitResp, ExternalPutRevokeReq,
    ExternalPutRevokeResp, ExternalPutStartReq, ExternalPutStartResp, ExternalPutTransferEndReq,
    ExternalPutTransferEndResp, TestPutPhaseTrace,
};
use crate::client_kv_api::{ClientKvApi, ExternalHoldingGetInfo, ExternalPendingPutCtx};
use crate::client_seg_pool::{ResolveSideTransferLaneReq, parse_side_transfer_worker_lane_idx};
use crate::cluster_manager::NodeIDString;
use crate::cluster_manager::{
    META_KEY_SHARED_STORAGE_NODE_ID, META_KEY_SHARED_STORAGE_NODE_START_TIME,
};
use crate::master_kv_router::msg_pack::GetSourceKind;
use crate::memholder::ExternalMemHolderInfo;
use crate::memholder::NodeHolderKey;
use crate::p2p::msg_pack::MsgPack;
use crate::rpcresp_kvresult_convert::FromError;
use crate::rpcresp_kvresult_convert::ToResult;
use crate::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult, OK};
use async_trait::async_trait;
use std::time::Duration;
use std::time::Instant;
use tracing;

fn duration_to_i64_us(duration: std::time::Duration) -> i64 {
    duration.as_micros().min(i64::MAX as u128) as i64
}

const SIDE_TRANSFER_OWNER_RPC_TIMEOUT_SECS: u64 = 30;
const SIDE_TRANSFER_TARGET_RESOLVE_TIMEOUT_SECS: u64 = 10;

impl ClientKvApi {
    fn is_side_transfer_worker(&self) -> bool {
        self.inner()
            .view
            .cluster_manager()
            .get_self_info()
            .metadata
            .get("side_transfer_worker")
            .is_some_and(|v| v == "true")
    }

    fn expected_owner_start_time_for_external_path(&self) -> i64 {
        let self_info = self.inner().view.cluster_manager().get_self_info();
        if !self.is_side_transfer_worker() {
            return self_info.node_start_time;
        }
        self_info
            .metadata
            .get(META_KEY_SHARED_STORAGE_NODE_START_TIME)
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(self_info.node_start_time)
    }

    fn owner_node_id_for_side_transfer(&self) -> KvResult<String> {
        self.inner()
            .view
            .cluster_manager()
            .get_self_info()
            .metadata
            .get(META_KEY_SHARED_STORAGE_NODE_ID)
            .cloned()
            .ok_or_else(|| {
                KvError::Api(ApiError::Unknown {
                    detail: "side-transfer worker missing shared-storage owner id".to_string(),
                })
            })
    }

    fn side_transfer_worker_lane_idx(&self) -> KvResult<u16> {
        let self_id = self.inner().view.cluster_manager().get_self_info().id;
        parse_side_transfer_worker_lane_idx(&self_id).ok_or_else(|| {
            KvError::Api(ApiError::Unknown {
                detail: format!(
                    "side-transfer worker missing '__side_<idx>' suffix in id: {}",
                    self_id
                ),
            })
        })
    }

    async fn resolve_remote_side_transfer_target(
        &self,
        owner_peer_id: &str,
        lane_idx: u16,
    ) -> Option<(NodeIDString, u64)> {
        tracing::info!(
            "resolving remote side-transfer target: owner={} lane_idx={}",
            owner_peer_id,
            lane_idx
        );
        let resp = match self
            .inner()
            .rpc_caller_resolve_side_transfer_lane
            .call(
                self.inner().view.p2p_module(),
                owner_peer_id.to_string().into(),
                MsgPack {
                    serialize_part: ResolveSideTransferLaneReq { lane_idx },
                    raw_bytes: Vec::new(),
                },
                Some(Duration::from_secs(
                    SIDE_TRANSFER_TARGET_RESOLVE_TIMEOUT_SECS,
                )),
                0,
            )
            .await
        {
            Ok(resp) => resp,
            Err(err) => {
                tracing::warn!(
                    "resolve_remote_side_transfer_target rpc failed: owner={} lane_idx={} err={:?}",
                    owner_peer_id,
                    lane_idx,
                    err
                );
                return None;
            }
        };
        if resp.serialize_part.error_code != OK {
            tracing::warn!(
                "resolve_remote_side_transfer_target returned error: owner={} lane_idx={} error_code={} error_json={}",
                owner_peer_id,
                lane_idx,
                resp.serialize_part.error_code,
                resp.serialize_part.error_json
            );
            return None;
        }
        let side_id = match resp.serialize_part.side_id {
            Some(side_id) => side_id,
            None => {
                tracing::info!(
                    "resolve_remote_side_transfer_target returned no side_id: owner={} lane_idx={} target_base_addr={:?}",
                    owner_peer_id,
                    lane_idx,
                    resp.serialize_part.target_base_addr
                );
                return None;
            }
        };
        let target_base_addr = match resp.serialize_part.target_base_addr {
            Some(target_base_addr) => target_base_addr,
            None => {
                tracing::info!(
                    "resolve_remote_side_transfer_target returned no target_base_addr: owner={} lane_idx={} side_id={}",
                    owner_peer_id,
                    lane_idx,
                    side_id
                );
                return None;
            }
        };
        tracing::info!(
            "resolved remote side-transfer target: owner={} lane_idx={} side_id={} target_base_addr={:#x}",
            owner_peer_id,
            lane_idx,
            side_id,
            target_base_addr
        );
        Some((side_id.into(), target_base_addr))
    }

    fn side_transfer_unsupported(op: &'static str) -> KvError {
        KvError::Api(ApiError::Unknown {
            detail: format!("{op} is unsupported on side-transfer worker"),
        })
    }

    async fn external_put_transfer_end_side_worker(
        &self,
        req: ExternalPutTransferEndReq,
    ) -> KvResult<ExternalPutTransferEndResp> {
        let inner = self.inner();
        let total_started_at = Instant::now();

        let Some(put_id) = req.put_id else {
            let err = KvError::Unreachable(
                crate::rpcresp_kvresult_convert::msg_and_error::UnreachableError::RpcDecodeError {
                    rpc_input_json: format!("missing put_id; key={}", req.key),
                },
            );
            return Ok(ExternalPutTransferEndResp::from_error(&err));
        };

        let owner_id = self.owner_node_id_for_side_transfer()?;
        let lane_idx = self.side_transfer_worker_lane_idx()?;
        let mut target_peer_id = req.peer_id.clone().map(Into::into);
        let mut target_base_addr = req.target_base_addr;
        if let Some(owner_peer_id) = req.peer_id.as_deref() {
            if let Some((side_peer_id, side_target_base_addr)) = self
                .resolve_remote_side_transfer_target(owner_peer_id, lane_idx)
                .await
            {
                tracing::info!(
                    "side-transfer lane resolved: source_lane={} owner_peer={} target_side={} target_base_addr={:#x}",
                    lane_idx,
                    owner_peer_id,
                    side_peer_id,
                    side_target_base_addr
                );
                target_peer_id = Some(side_peer_id);
                target_base_addr = Some(side_target_base_addr);
            }
        }
        let transfer_peer_id_for_trace = target_peer_id.as_ref().map(|peer| peer.to_string());

        let transfer_started_at = Instant::now();
        if let Err(e) = inner
            .put_transfer(
                &req.key,
                put_id,
                req.src_offset,
                req.target_offset,
                req.len,
                target_peer_id,
                target_base_addr,
            )
            .await
        {
            let revoke_req = MsgPack {
                serialize_part: ExternalPutRevokeReq {
                    key: req.key.clone(),
                    put_id: Some(put_id),
                    started_time: req.started_time,
                },
                raw_bytes: Vec::new(),
            };
            if let Err(revoke_err) = inner
                .rpc_caller_external_put_revoke
                .call(
                    inner.view.p2p_module(),
                    owner_id.clone().into(),
                    revoke_req,
                    Some(Duration::from_secs(SIDE_TRANSFER_OWNER_RPC_TIMEOUT_SECS)),
                    0,
                )
                .await
            {
                tracing::warn!(
                    "side-transfer revoke RPC failed after transfer error: owner={} key={} put_id=({},{}) err={:?}",
                    owner_id,
                    req.key,
                    put_id.0,
                    put_id.1,
                    revoke_err
                );
            }
            return Ok(ExternalPutTransferEndResp::from_error(&e));
        }
        let put_transfer_total_us = duration_to_i64_us(transfer_started_at.elapsed());

        let commit_req = MsgPack {
            serialize_part: ExternalPutCommitReq {
                key: req.key.clone(),
                put_id: Some(put_id),
                lease_id: req.lease_id,
                started_time: req.started_time,
                test_observe_put_phases: req.test_observe_put_phases,
            },
            raw_bytes: Vec::new(),
        };
        let commit_resp = inner
            .rpc_caller_external_put_commit
            .call(
                inner.view.p2p_module(),
                owner_id.into(),
                commit_req,
                Some(Duration::from_secs(SIDE_TRANSFER_OWNER_RPC_TIMEOUT_SECS)),
                0,
            )
            .await
            .map_err(KvError::from)?;
        commit_resp.serialize_part.clone().to_result()?;

        let mut trace = req.test_observe_put_phases.then(TestPutPhaseTrace::default);
        if let Some(trace_ref) = trace.as_mut() {
            trace_ref.owner_external_put_transfer_end_total_us =
                duration_to_i64_us(total_started_at.elapsed());
            trace_ref.owner_put_transfer_total_us = put_transfer_total_us;
            trace_ref.owner_put_transfer_peer_id = transfer_peer_id_for_trace;
            if let Some(commit_trace) = commit_resp.serialize_part.test_put_phase_trace.as_ref() {
                trace_ref.merge_from(commit_trace);
            }
        }

        Ok(ExternalPutTransferEndResp {
            error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
            error_json: String::new(),
            test_put_phase_trace: trace,
        })
    }
}

/// External API trait for managing external client requests
#[async_trait]
pub trait HandlerForExternalClient {
    /// Validate external's observed owner start_time (0 means skip validation for legacy callers)
    fn validate_requester_owner_status_updated(&self, started_time: i64) -> KvResult<()>;
    async fn external_get(&self, req: ExternalGetReq) -> KvResult<ExternalGetResp>;
    async fn external_put_start(&self, req: ExternalPutStartReq) -> KvResult<ExternalPutStartResp>;
    // deprecated: transfer merged into external_put_transfer_end
    async fn external_put_transfer_end(
        &self,
        req: ExternalPutTransferEndReq,
    ) -> KvResult<ExternalPutTransferEndResp>;
    async fn external_put_commit(
        &self,
        req: ExternalPutCommitReq,
    ) -> KvResult<ExternalPutCommitResp>;
    async fn external_put_revoke(
        &self,
        req: ExternalPutRevokeReq,
    ) -> KvResult<ExternalPutRevokeResp>;
    async fn external_delete(&self, req: ExternalDeleteReq) -> KvResult<ExternalDeleteResp>;
    async fn external_is_exist(&self, req: ExternalIsExistReq) -> KvResult<ExternalIsExistResp>;
}

/// Handle external get request
#[async_trait]
impl HandlerForExternalClient for ClientKvApi {
    fn validate_requester_owner_status_updated(&self, started_time: i64) -> KvResult<()> {
        // Validate owner start time if provided (non-zero)
        // only when the requestor has the right owner start time, address computation will be right
        let expected = self.expected_owner_start_time_for_external_path();
        if started_time != 0 && started_time != expected {
            return Err(KvError::Api(ApiError::OwnerStartTimeMismatch {
                expected,
                got: started_time,
            }));
        }
        Ok(())
    }
    async fn external_get(&self, req: ExternalGetReq) -> KvResult<ExternalGetResp> {
        if self.is_side_transfer_worker() {
            return Err(Self::side_transfer_unsupported("external_get"));
        }
        let inner = self.inner();
        let _shutdown_guard = inner.shutdown_guard("external_get")?;

        self.validate_requester_owner_status_updated(req.started_time)?;

        // dummy implementation, tmp owner user memholder for temporary holding to make self memholder
        let (memholder, get_info) = match inner.get(&req.key).await? {
            Some(holder) => holder,
            None => {
                return Ok(ExternalGetResp {
                    external_memholder_info: None,
                    error_code:
                        crate::rpcresp_kvresult_convert::msg_and_error::codes_api::API_KEY_NOT_FOUND,
                    error_json: String::from("Key not found"),
                });
            }
        };
        let memory_info = memholder.memory_info();
        // Build holding info to record that this external client holds the memholder
        let client_holding_info = ExternalHoldingGetInfo {
            key: req.key.clone(),
            req_node_id: req.req_node_id.clone(),
            memory_info,
        };

        // Insert/update holding via owned manager
        inner.external_get_holding.insert(
            NodeHolderKey::new(req.req_node_id.clone(), memholder.holder_id()),
            client_holding_info,
        );
        let external_memholder_info = ExternalMemHolderInfo {
            offset: memholder.get_offset(),
            len: memholder.get_length() as u32,
            holder_id: memholder.holder_id(),
            source_kind: get_info
                .map(|info| info.source_kind())
                .unwrap_or(GetSourceKind::Memory),
        };
        Ok(ExternalGetResp {
            external_memholder_info: Some(external_memholder_info),
            error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
            error_json: String::new(),
        })
    }

    /// Handle external put start request: allocate with native KV op and return offsets.
    /// For remote targets, return a local staging offset (no `peer_id` exposed); owner records
    /// remote context internally and completes transfer during `external_put_transfer_end`.
    async fn external_put_start(&self, req: ExternalPutStartReq) -> KvResult<ExternalPutStartResp> {
        let inner = self.inner();
        let started_at = Instant::now();

        self.validate_requester_owner_status_updated(req.started_time)?;

        let put_start_started_at = Instant::now();
        let source_node_id = if self.is_side_transfer_worker() {
            Some(self.owner_node_id_for_side_transfer()?.into())
        } else {
            None
        };
        let (put_start_resp, master_put_start_rpc_us) = inner
            .put_start_with_source_node(
                &req.key,
                req.len as u32,
                req.reject_if_inflight_same_key,
                req.reject_if_exists,
                req.preferred_sub_cluster.as_deref(),
                source_node_id,
            )
            .await
            .map_err(|e| {
                tracing::error!("Failed to start put operation: {}", e);
                e
            })?;
        // Ensure master responded OK before using returned addresses
        crate::rpcresp_kvresult_convert::try_from_code(
            put_start_resp.error_code,
            put_start_resp.error_json.clone(),
        )?;
        tracing::debug!(
            "handle external put start for key: {}, len: {}",
            req.key,
            req.len
        );

        // Master-owner returns absolute addresses due to Mooncake; use base-addrs from RPC to compute offsets
        let src_offset = put_start_resp.src_addr - put_start_resp.src_base_addr;
        let is_local_target =
            &*put_start_resp.node_id == &*inner.view.cluster_manager().get_self_info().id;
        // Compute the offset that the external should write to:
        // - If target is local, return the local target offset
        // - If target is remote, return a local staging offset (src_offset) and record remote ctx internally
        let target_offset = if is_local_target {
            put_start_resp.target_addr - put_start_resp.target_base_addr
        } else {
            // Remote target: external still writes into owner's shared memory (src_offset).
            src_offset
        };
        if !is_local_target {
            // Stash remote context for transfer_end: peer_id, target_base_addr, target_offset(remote)
            let remote_offset = put_start_resp.target_addr - put_start_resp.target_base_addr;
            inner.external_pending_puts.insert(
                (
                    req.key.clone(),
                    put_start_resp.put_id.0,
                    put_start_resp.put_id.1,
                ),
                ExternalPendingPutCtx {
                    peer_id: put_start_resp.node_id.clone(),
                    target_base_addr: put_start_resp.target_base_addr,
                    target_offset: remote_offset,
                },
            );
            tracing::debug!(
                "external_put_start stash remote ctx: key={}, put_id=({},{}) peer_id={}, target_base={:#x}, target_off={:#x}, src_off(staging)={:#x}",
                req.key,
                put_start_resp.put_id.0,
                put_start_resp.put_id.1,
                put_start_resp.node_id,
                put_start_resp.target_base_addr,
                remote_offset,
                src_offset
            );
        }
        Ok(ExternalPutStartResp {
            error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
            src_offset,
            target_offset,
            transfer_target_offset: if is_local_target {
                None
            } else {
                Some(put_start_resp.target_addr - put_start_resp.target_base_addr)
            },
            // Expose peer_id only for owner to reconstruct abs target at transfer_end; external can ignore.
            peer_id: if is_local_target {
                None
            } else {
                Some(put_start_resp.node_id)
            },
            src_base_addr: put_start_resp.src_base_addr,
            target_base_addr: put_start_resp.target_base_addr,
            error_json: String::new(),
            put_id: Some(put_start_resp.put_id),
            test_put_phase_trace: if req.test_observe_put_phases {
                let owner_external_put_start_total_us = duration_to_i64_us(started_at.elapsed());
                let owner_put_start_total_us = duration_to_i64_us(put_start_started_at.elapsed());
                Some(TestPutPhaseTrace {
                    owner_external_put_start_total_us,
                    owner_put_start_total_us,
                    owner_master_put_start_rpc_us: master_put_start_rpc_us,
                    owner_master_put_start_server_us: put_start_resp.server_process_us,
                    ..Default::default()
                })
            } else {
                None
            },
        })
    }

    /// Handle external transfer+end request - transfer data then commit
    async fn external_put_transfer_end(
        &self,
        req: ExternalPutTransferEndReq,
    ) -> KvResult<ExternalPutTransferEndResp> {
        if self.is_side_transfer_worker() {
            return self.external_put_transfer_end_side_worker(req).await;
        }
        let inner = self.inner();
        let total_started_at = Instant::now();

        self.validate_requester_owner_status_updated(req.started_time)?;

        // Extract put_id early so we can revoke on transfer failure
        let Some(put_id) = req.put_id else {
            let err = crate::rpcresp_kvresult_convert::msg_and_error::KvError::Unreachable(
                crate::rpcresp_kvresult_convert::msg_and_error::UnreachableError::RpcDecodeError {
                    rpc_input_json: format!("missing put_id; key={}", req.key),
                },
            );
            return Ok(ExternalPutTransferEndResp::from_error(&err));
        };

        // Delegate transfer to owner put_transfer (offset-based) then end
        // For remote target, ignore caller's target_offset (staging) and use stashed remote ctx.
        let (peer_id_for_transfer, target_off_for_transfer, target_base_for_transfer) =
            if let Some(peer) = req.peer_id.clone() {
                // Remote path requires a stashed context; do not fallback to caller-provided target_offset
                match inner
                    .external_pending_puts
                    .get(&(req.key.clone(), put_id.0, put_id.1))
                {
                    Some(ctx) => (
                        Some(ctx.peer_id.clone()),
                        ctx.target_offset,
                        Some(ctx.target_base_addr),
                    ),
                    None => match req.target_base_addr {
                        Some(target_base_addr) => {
                            (Some(peer.into()), req.target_offset, Some(target_base_addr))
                        }
                        None => {
                            let err = KvError::Unreachable(
                                crate::rpcresp_kvresult_convert::msg_and_error::UnreachableError::RpcDecodeError {
                                    rpc_input_json: format!(
                                        "missing pending remote put ctx and caller target_base_addr; key={}, put_id=({},{}), peer_id={}",
                                        req.key, put_id.0, put_id.1, peer
                                    ),
                                },
                            );
                            return Ok(ExternalPutTransferEndResp::from_error(&err));
                        }
                    },
                }
            } else {
                (None, req.target_offset, None)
            };
        let transfer_peer_id_for_trace = peer_id_for_transfer.as_ref().map(|peer| peer.to_string());

        tracing::debug!(
            "external_put_transfer_end resolved: key={}, put_id=({},{}) src_off={:#x}, tgt_off={:#x}, len={}, peer_id={:?}, target_base={:?}",
            req.key,
            put_id.0,
            put_id.1,
            req.src_offset,
            target_off_for_transfer,
            req.len,
            peer_id_for_transfer,
            target_base_for_transfer
        );

        let transfer_started_at = Instant::now();
        if let Err(e) = inner
            .put_transfer(
                &req.key,
                put_id,
                req.src_offset,
                target_off_for_transfer,
                req.len,
                peer_id_for_transfer,
                target_base_for_transfer,
            )
            .await
        {
            tracing::error!("Failed to transfer data: {}", e);
            // On transfer failure, revoke the put to release resources
            if let Err(revoke_err) = inner.put_revoke(&req.key, put_id).await {
                tracing::warn!(
                    "put_revoke also failed after transfer error: {}",
                    revoke_err
                );
            }
            // Cleanup pending ctx on failure
            inner
                .external_pending_puts
                .invalidate(&(req.key.clone(), put_id.0, put_id.1));
            return Ok(crate::rpcresp_kvresult_convert::FromError::from_error(&e));
        }

        let put_transfer_total_us = duration_to_i64_us(transfer_started_at.elapsed());

        if inner.skip_put_end_commit_enabled() {
            inner
                .external_pending_puts
                .invalidate(&(req.key.clone(), put_id.0, put_id.1));
            tracing::warn!(
                "skip_put_end_commit test-only fast-path: returning success without external put_end; key={} put_id=({},{}) payload_len={}",
                req.key,
                put_id.0,
                put_id.1,
                req.len
            );
            return Ok(ExternalPutTransferEndResp {
                error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
                error_json: String::new(),
                test_put_phase_trace: if req.test_observe_put_phases {
                    Some(TestPutPhaseTrace {
                        owner_external_put_transfer_end_total_us: duration_to_i64_us(
                            total_started_at.elapsed(),
                        ),
                        owner_put_transfer_total_us: put_transfer_total_us,
                        owner_put_transfer_peer_id: transfer_peer_id_for_trace,
                        ..Default::default()
                    })
                } else {
                    None
                },
            });
        }

        let end_started_at = Instant::now();
        let put_end_stats = match inner.put_end(&req.key, put_id, req.lease_id).await {
            Ok(stats) => stats,
            Err(e) => {
                tracing::error!("Failed to end put operation: {}", e);
                inner
                    .external_pending_puts
                    .invalidate(&(req.key.clone(), put_id.0, put_id.1));
                return Ok(crate::rpcresp_kvresult_convert::FromError::from_error(&e));
            }
        };
        let put_end_total_us = duration_to_i64_us(end_started_at.elapsed());

        // Success; cleanup pending ctx
        inner
            .external_pending_puts
            .invalidate(&(req.key.clone(), put_id.0, put_id.1));

        Ok(ExternalPutTransferEndResp {
            error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
            error_json: String::new(),
            test_put_phase_trace: if req.test_observe_put_phases {
                Some(TestPutPhaseTrace {
                    owner_external_put_transfer_end_total_us: duration_to_i64_us(
                        total_started_at.elapsed(),
                    ),
                    owner_put_transfer_total_us: put_transfer_total_us,
                    owner_put_transfer_peer_id: transfer_peer_id_for_trace,
                    owner_put_end_total_us: put_end_total_us,
                    owner_master_put_end_rpc_us: put_end_stats.master_put_end_rpc_us,
                    owner_master_put_end_server_us: put_end_stats.master_put_end_server_us,
                    ..Default::default()
                })
            } else {
                None
            },
        })
    }

    async fn external_put_commit(
        &self,
        req: ExternalPutCommitReq,
    ) -> KvResult<ExternalPutCommitResp> {
        if self.is_side_transfer_worker() {
            return Err(Self::side_transfer_unsupported("external_put_commit"));
        }
        let inner = self.inner();
        self.validate_requester_owner_status_updated(req.started_time)?;
        let Some(put_id) = req.put_id else {
            let err = KvError::Unreachable(
                crate::rpcresp_kvresult_convert::msg_and_error::UnreachableError::RpcDecodeError {
                    rpc_input_json: format!("missing put_id; key={}", req.key),
                },
            );
            return Ok(ExternalPutCommitResp::from_error(&err));
        };

        let end_started_at = Instant::now();
        let put_end_stats = match inner.put_end(&req.key, put_id, req.lease_id).await {
            Ok(stats) => stats,
            Err(e) => {
                inner
                    .external_pending_puts
                    .invalidate(&(req.key.clone(), put_id.0, put_id.1));
                return Ok(ExternalPutCommitResp::from_error(&e));
            }
        };
        let put_end_total_us = duration_to_i64_us(end_started_at.elapsed());
        inner
            .external_pending_puts
            .invalidate(&(req.key.clone(), put_id.0, put_id.1));

        Ok(ExternalPutCommitResp {
            error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
            error_json: String::new(),
            test_put_phase_trace: if req.test_observe_put_phases {
                Some(TestPutPhaseTrace {
                    owner_put_end_total_us: put_end_total_us,
                    owner_master_put_end_rpc_us: put_end_stats.master_put_end_rpc_us,
                    owner_master_put_end_server_us: put_end_stats.master_put_end_server_us,
                    ..Default::default()
                })
            } else {
                None
            },
        })
    }

    async fn external_put_revoke(
        &self,
        req: ExternalPutRevokeReq,
    ) -> KvResult<ExternalPutRevokeResp> {
        if self.is_side_transfer_worker() {
            return Err(Self::side_transfer_unsupported("external_put_revoke"));
        }
        let inner = self.inner();
        self.validate_requester_owner_status_updated(req.started_time)?;
        let Some(put_id) = req.put_id else {
            let err = KvError::Unreachable(
                crate::rpcresp_kvresult_convert::msg_and_error::UnreachableError::RpcDecodeError {
                    rpc_input_json: format!("missing put_id; key={}", req.key),
                },
            );
            return Ok(ExternalPutRevokeResp::from_error(&err));
        };
        inner
            .external_pending_puts
            .invalidate(&(req.key.clone(), put_id.0, put_id.1));
        match inner.put_revoke(&req.key, put_id).await {
            Ok(_) => Ok(ExternalPutRevokeResp {
                error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
                error_json: String::new(),
            }),
            Err(e) => Ok(ExternalPutRevokeResp::from_error(&e)),
        }
    }

    /// Handle external delete request
    async fn external_delete(&self, req: ExternalDeleteReq) -> KvResult<ExternalDeleteResp> {
        if self.is_side_transfer_worker() {
            return Err(Self::side_transfer_unsupported("external_delete"));
        }
        let inner = self.inner();

        self.validate_requester_owner_status_updated(req.started_time)?;

        match inner.delete(&req.key).await {
            Ok(_) => Ok(ExternalDeleteResp {
                error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
                error_json: String::new(),
            }),
            Err(e) => Ok(ExternalDeleteResp::from_error(&e)),
        }
    }

    /// Handle external is_exist request
    async fn external_is_exist(&self, req: ExternalIsExistReq) -> KvResult<ExternalIsExistResp> {
        if self.is_side_transfer_worker() {
            return Err(Self::side_transfer_unsupported("external_is_exist"));
        }
        let inner = self.inner();

        self.validate_requester_owner_status_updated(req.started_time)?;

        match inner.is_exist(&req.key).await {
            Ok(exists) => Ok(ExternalIsExistResp {
                error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
                exists,
                error_json: String::new(),
            }),
            Err(e) => Ok(ExternalIsExistResp {
                exists: false,
                ..ExternalIsExistResp::from_error(&e)
            }),
        }
    }
}
