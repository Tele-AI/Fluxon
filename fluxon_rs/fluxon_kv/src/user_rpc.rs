use std::time::Duration;
use std::time::Instant;

use fluxon_commu::p2p::rpc::{
    MIN_EXPLICIT_RPC_TIMEOUT_MS, USER_RPC_OBSERVE_TRACE_RAW_BYTES_INDEX,
    USER_RPC_OWNER1_OBSERVE_TRACE_RAW_BYTES_INDEX, UserRpcObserveTrace, UserRpcOwner1ObserveTrace,
    UserRpcTransportPathKind, current_cross_process_monotonic_us, decode_user_rpc_observe_trace,
    decode_user_rpc_owner1_observe_trace, validate_explicit_rpc_timeout_ms,
};
use prost::bytes::Bytes;

use crate::cluster_manager::NodeID;
use crate::p2p::msg_pack::MsgPack;
use crate::p2p::p2p_module::UserRpcReq;
use crate::rpcresp_kvresult_convert;
use crate::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult};

pub const USER_RPC_MIN_TIMEOUT_MS: u64 = MIN_EXPLICIT_RPC_TIMEOUT_MS;

pub fn validate_timeout_ms(timeout_ms: u64) -> KvResult<()> {
    validate_explicit_rpc_timeout_ms(timeout_ms).map_err(KvError::from)
}

#[cfg(test)]
mod timeout_validation_tests {
    use super::*;
    use fluxon_commu::p2p::P2pError;

    #[test]
    fn invalid_user_rpc_timeout_preserves_typed_p2p_error() {
        let timeout_ms = USER_RPC_MIN_TIMEOUT_MS - 1;
        let err = validate_timeout_ms(timeout_ms).unwrap_err();
        match err {
            KvError::P2p(P2pError::InvalidRpcTimeout {
                timeout_ms: actual_timeout_ms,
                min_timeout_ms,
                ..
            }) => {
                assert_eq!(actual_timeout_ms, timeout_ms);
                assert_eq!(min_timeout_ms, USER_RPC_MIN_TIMEOUT_MS);
            }
            other => panic!("expected typed InvalidRpcTimeout, got {other:?}"),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum UserRpcOwnerPathKind {
    #[default]
    Unknown,
    Ipc,
    Fast,
    Slow,
}

impl UserRpcOwnerPathKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Ipc => "ipc",
            Self::Fast => "fast",
            Self::Slow => "slow",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct UserRpcCallObserve {
    pub ext_total_us: i64,
    pub ext_rpc_wait_us: i64,
    pub ext_finalize_us: i64,
    pub owner_total_us: i64,
    pub owner_handle_us: i64,
    pub owner_queue_us: i64,
    pub owner_handle_blocking_wait_us: i64,
    pub owner_handle_py_with_gil_us: i64,
    pub owner_handle_py_gil_wait_us: i64,
    pub owner_handle_py_arg_build_us: i64,
    pub owner_handle_py_call_us: i64,
    pub owner_handle_py_result_unpack_us: i64,
    pub owner_handle_py_result_copy_us: i64,
    pub owner_handle_py_decode_us: i64,
    pub owner_handle_py_handler_body_us: i64,
    pub owner_handle_py_encode_us: i64,
    pub owner_path_kind: UserRpcOwnerPathKind,
    pub request_path_kind: UserRpcTransportPathKind,
    pub response_path_kind: UserRpcTransportPathKind,
    pub owner1_request_path_kind: UserRpcTransportPathKind,
    pub owner1_response_path_kind: UserRpcTransportPathKind,
    pub caller_started_ts_us: i64,
    pub caller_submit_us: i64,
    pub caller_complete_us: i64,
    pub caller_submit_ts_us: i64,
    pub owner1_request_send_ts_us: i64,
    pub owner_frame_recv_done_ts_us: i64,
    pub owner_dispatch_send_started_ts_us: i64,
    pub owner_dispatch_enqueued_ts_us: i64,
    pub owner_dispatch_dequeued_ts_us: i64,
    pub owner_reply_path_prepare_started_ts_us: i64,
    pub owner_reply_path_ready_ts_us: i64,
    pub owner_dispatch_started_ts_us: i64,
    pub owner_dispatch_map_enter_ts_us: i64,
    pub owner_user_rpc_spawn_called_ts_us: i64,
    pub owner_dispatch_returned_to_loop_ts_us: i64,
    pub owner_handler_started_ts_us: i64,
    pub owner_blocking_wait_started_ts_us: i64,
    pub owner_blocking_closure_started_ts_us: i64,
    pub owner_handler_done_ts_us: i64,
    pub owner_response_send_enqueued_ts_us: i64,
    pub owner1_response_frame_recv_done_ts_us: i64,
    pub caller_response_frame_recv_done_ts_us: i64,
    pub caller_response_dispatch_enqueued_ts_us: i64,
    pub caller_response_dispatch_started_ts_us: i64,
    pub caller_response_complete_pending_call_ts_us: i64,
    pub caller_decode_done_ts_us: i64,
}

#[derive(Debug, Clone)]
pub struct UserRpcCallOutput {
    pub payload: Vec<u8>,
    pub observe: UserRpcCallObserve,
}

fn duration_to_i64_us(duration: std::time::Duration) -> i64 {
    duration.as_micros().min(i64::MAX as u128) as i64
}

fn classify_user_rpc_owner_path_kind(
    has_owner1_observe_trace: bool,
    owner1_observe: &UserRpcOwner1ObserveTrace,
) -> UserRpcOwnerPathKind {
    if !has_owner1_observe_trace {
        // A missing owner1 observe trace means the request never left owner1, so no
        // owner-to-owner hop happened and the owner path should be treated as ipc.
        return UserRpcOwnerPathKind::Ipc;
    }
    if owner1_observe.owner1_request_path_kind == UserRpcTransportPathKind::Fast
        && owner1_observe.owner1_response_path_kind == UserRpcTransportPathKind::Fast
    {
        return UserRpcOwnerPathKind::Fast;
    }
    if matches!(
        owner1_observe.owner1_request_path_kind,
        UserRpcTransportPathKind::Fast | UserRpcTransportPathKind::Slow
    ) && matches!(
        owner1_observe.owner1_response_path_kind,
        UserRpcTransportPathKind::Fast | UserRpcTransportPathKind::Slow
    ) {
        return UserRpcOwnerPathKind::Slow;
    }
    UserRpcOwnerPathKind::Unknown
}

/// Call a user-defined RPC on a specific node.
///
/// This centralizes MsgPack wire encoding/decoding and response validation so
/// higher-level layers (e.g. PyO3) can stay thin.
///
/// Contract:
/// - `timeout_ms` must be explicitly provided by the caller.
/// - On success, `resp.raw_bytes[0]` must exist; otherwise returns a dedicated
///   `ApiError::UserRpcMissingPayload` for consistent error mapping.
pub async fn user_rpc_call_observed(
    fw: &crate::Framework,
    node_id: NodeID,
    path: String,
    payload: Vec<u8>,
    timeout_ms: u64,
) -> KvResult<UserRpcCallOutput> {
    validate_timeout_ms(timeout_ms)?;

    let path_for_error = path.clone();
    let req = MsgPack {
        serialize_part: UserRpcReq { path },
        raw_bytes: vec![Bytes::from(payload)],
    };

    let ext_started_at = Instant::now();
    let caller_started_ts_us = current_cross_process_monotonic_us();
    let rpc_output = crate::p2p::msg_pack::call_rpc_observed::<UserRpcReq>(
        fw.p2p_view().p2p_module(),
        node_id,
        req,
        Some(Duration::from_millis(timeout_ms)),
    )
    .await
    .map_err(KvError::from)?;
    let ext_rpc_wait_us = duration_to_i64_us(ext_started_at.elapsed());
    let resp_pack = rpc_output.resp;

    let sp = resp_pack.serialize_part;
    rpcresp_kvresult_convert::try_from_code(sp.error_code, sp.error_json)?;

    let owner_observe = match resp_pack
        .raw_bytes
        .get(USER_RPC_OBSERVE_TRACE_RAW_BYTES_INDEX)
        .cloned()
    {
        Some(raw_trace) => {
            decode_user_rpc_observe_trace(raw_trace.as_ref()).map_err(KvError::from)?
        }
        None => UserRpcObserveTrace::default(),
    };
    let (owner1_observe, has_owner1_observe_trace) = match resp_pack
        .raw_bytes
        .get(USER_RPC_OWNER1_OBSERVE_TRACE_RAW_BYTES_INDEX)
        .cloned()
    {
        Some(raw_trace) => (
            decode_user_rpc_owner1_observe_trace(raw_trace.as_ref()).map_err(KvError::from)?,
            true,
        ),
        None => (Default::default(), false),
    };
    let owner_path_kind =
        classify_user_rpc_owner_path_kind(has_owner1_observe_trace, &owner1_observe);

    let Some(raw) = resp_pack.raw_bytes.first().cloned() else {
        return Err(KvError::Api(ApiError::UserRpcMissingPayload {
            path: path_for_error,
        }));
    };

    let payload = raw.as_ref().to_vec();
    let ext_total_us = duration_to_i64_us(ext_started_at.elapsed());
    Ok(UserRpcCallOutput {
        payload,
        observe: UserRpcCallObserve {
            ext_total_us,
            ext_rpc_wait_us,
            ext_finalize_us: ext_total_us.saturating_sub(ext_rpc_wait_us),
            owner_total_us: owner_observe.owner_total_us,
            owner_handle_us: owner_observe.owner_handle_us,
            owner_queue_us: owner_observe.owner_queue_us,
            owner_handle_blocking_wait_us: owner_observe.owner_handle_blocking_wait_us,
            owner_handle_py_with_gil_us: owner_observe.owner_handle_py_with_gil_us,
            owner_handle_py_gil_wait_us: owner_observe.owner_handle_py_gil_wait_us,
            owner_handle_py_arg_build_us: owner_observe.owner_handle_py_arg_build_us,
            owner_handle_py_call_us: owner_observe.owner_handle_py_call_us,
            owner_handle_py_result_unpack_us: owner_observe.owner_handle_py_result_unpack_us,
            owner_handle_py_result_copy_us: owner_observe.owner_handle_py_result_copy_us,
            owner_handle_py_decode_us: owner_observe.owner_handle_py_decode_us,
            owner_handle_py_handler_body_us: owner_observe.owner_handle_py_handler_body_us,
            owner_handle_py_encode_us: owner_observe.owner_handle_py_encode_us,
            owner_path_kind,
            request_path_kind: rpc_output.observe.request_path_kind,
            response_path_kind: owner_observe.response_path_kind,
            owner1_request_path_kind: owner1_observe.owner1_request_path_kind,
            owner1_response_path_kind: owner1_observe.owner1_response_path_kind,
            caller_started_ts_us,
            caller_submit_us: rpc_output.observe.caller_submit_us,
            caller_complete_us: rpc_output.observe.caller_complete_us,
            caller_submit_ts_us: rpc_output.observe.caller_submit_ts_us,
            owner1_request_send_ts_us: owner1_observe.owner1_request_send_ts_us,
            owner_frame_recv_done_ts_us: owner_observe.owner_frame_recv_done_ts_us,
            owner_dispatch_send_started_ts_us: owner_observe.owner_dispatch_send_started_ts_us,
            owner_dispatch_enqueued_ts_us: owner_observe.owner_dispatch_enqueued_ts_us,
            owner_dispatch_dequeued_ts_us: owner_observe.owner_dispatch_dequeued_ts_us,
            owner_reply_path_prepare_started_ts_us: owner_observe
                .owner_reply_path_prepare_started_ts_us,
            owner_reply_path_ready_ts_us: owner_observe.owner_reply_path_ready_ts_us,
            owner_dispatch_started_ts_us: owner_observe.owner_dispatch_started_ts_us,
            owner_dispatch_map_enter_ts_us: owner_observe.owner_dispatch_map_enter_ts_us,
            owner_user_rpc_spawn_called_ts_us: owner_observe.owner_user_rpc_spawn_called_ts_us,
            owner_dispatch_returned_to_loop_ts_us: owner_observe
                .owner_dispatch_returned_to_loop_ts_us,
            owner_handler_started_ts_us: owner_observe.owner_handler_started_ts_us,
            owner_blocking_wait_started_ts_us: owner_observe.owner_blocking_wait_started_ts_us,
            owner_blocking_closure_started_ts_us: owner_observe
                .owner_blocking_closure_started_ts_us,
            owner_handler_done_ts_us: owner_observe.owner_handler_done_ts_us,
            owner_response_send_enqueued_ts_us: owner_observe.owner_response_send_enqueued_ts_us,
            owner1_response_frame_recv_done_ts_us: owner1_observe
                .owner1_response_frame_recv_done_ts_us,
            caller_response_frame_recv_done_ts_us: rpc_output
                .observe
                .caller_response_frame_recv_done_ts_us,
            caller_response_dispatch_enqueued_ts_us: rpc_output
                .observe
                .caller_response_dispatch_enqueued_ts_us,
            caller_response_dispatch_started_ts_us: rpc_output
                .observe
                .caller_response_dispatch_started_ts_us,
            caller_response_complete_pending_call_ts_us: rpc_output
                .observe
                .caller_response_complete_pending_call_ts_us,
            caller_decode_done_ts_us: rpc_output.observe.caller_decode_done_ts_us,
        },
    })
}

pub async fn user_rpc_call(
    fw: &crate::Framework,
    node_id: NodeID,
    path: String,
    payload: Vec<u8>,
    timeout_ms: u64,
) -> KvResult<Vec<u8>> {
    Ok(
        user_rpc_call_observed(fw, node_id, path, payload, timeout_ms)
            .await?
            .payload,
    )
}
