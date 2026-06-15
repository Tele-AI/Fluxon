use crate::cluster_manager::NodeID;
use crate::p2p::{
    MsgPackHeadMeta, MsgPackRelay, P2PResult, P2pError, TaskId, UserRpcReq, UserRpcResp,
    WireMessageBody,
};
use bitcode::{Decode, DecodeOwned, Encode};
use bytes::Bytes;
use fluxon_observability::greptime_otlp_log_orchestrator::{
    GREPTIME_OTLP_LOG_PROXY_REQ_MSG_ID, GREPTIME_OTLP_LOG_PROXY_RESP_MSG_ID,
    GreptimeOtlpLogProxyReq, GreptimeOtlpLogProxyResp,
};
use fluxon_observability::prom_remote_write_orchestrator::{
    PROM_REMOTE_WRITE_PROXY_REQ_MSG_ID, PROM_REMOTE_WRITE_PROXY_RESP_MSG_ID,
    PromRemoteWriteProxyReq, PromRemoteWriteProxyResp,
};
use prost::bytes::Bytes as ProstBytes;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;

pub const MIN_EXPLICIT_RPC_TIMEOUT_SECS: u64 = 10;
pub const USER_RPC_REQ_MSG_ID: u32 = 7001;
pub const USER_RPC_RESP_MSG_ID: u32 = 7002;
pub const USER_RPC_REQUEST_OWNER1_OBSERVE_TRACE_RAW_BYTES_INDEX: usize = 1;
pub const USER_RPC_OBSERVE_TRACE_RAW_BYTES_INDEX: usize = 1;
pub const USER_RPC_OWNER1_OBSERVE_TRACE_RAW_BYTES_INDEX: usize = 2;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum UserRpcTransportPathKind {
    #[default]
    Unknown,
    Fast,
    Slow,
}

impl UserRpcTransportPathKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Fast => "fast",
            Self::Slow => "slow",
        }
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct UserRpcObserveTrace {
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
    pub response_path_kind: UserRpcTransportPathKind,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct UserRpcOwner1ObserveTrace {
    pub owner1_peer_id: String,
    pub owner1_request_send_ts_us: i64,
    pub owner1_response_frame_recv_done_ts_us: i64,
    pub owner1_request_path_kind: UserRpcTransportPathKind,
    pub owner1_response_path_kind: UserRpcTransportPathKind,
}

#[derive(Default, Debug, Clone)]
pub struct RpcCallObserveTrace {
    pub caller_submit_us: i64,
    pub caller_complete_us: i64,
    pub caller_submit_ts_us: i64,
    pub request_path_kind: UserRpcTransportPathKind,
    pub caller_response_frame_recv_done_ts_us: i64,
    pub caller_response_dispatch_enqueued_ts_us: i64,
    pub caller_response_dispatch_started_ts_us: i64,
    pub caller_response_complete_pending_call_ts_us: i64,
    pub caller_decode_done_ts_us: i64,
}

#[derive(Debug, Clone)]
pub struct RpcCallObservedOutput<RESP: MsgPackSerializePart> {
    pub resp: MsgPack<RESP>,
    pub observe: RpcCallObserveTrace,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct UserRpcHandlerLocalObserve {
    pub py_with_gil_us: i64,
    pub py_gil_wait_us: i64,
    pub py_arg_build_us: i64,
    pub py_call_us: i64,
    pub py_result_unpack_us: i64,
    pub py_result_copy_us: i64,
    pub py_decode_us: i64,
    pub py_handler_body_us: i64,
    pub py_encode_us: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct UserRpcBytesOutput {
    pub payload: Vec<u8>,
    pub local_observe: UserRpcHandlerLocalObserve,
}

impl UserRpcBytesOutput {
    pub fn from_payload(payload: Vec<u8>) -> Self {
        Self {
            payload,
            local_observe: UserRpcHandlerLocalObserve::default(),
        }
    }
}

pub fn current_cross_process_monotonic_us() -> i64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut ts) };
    if rc != 0 {
        panic!("clock_gettime CLOCK_MONOTONIC_RAW failed: rc={}", rc);
    }
    let secs_us = i128::from(ts.tv_sec).saturating_mul(1_000_000);
    let nanos_us = i128::from(ts.tv_nsec).saturating_div(1_000);
    secs_us.saturating_add(nanos_us).clamp(0, i64::MAX as i128) as i64
}

pub fn encode_user_rpc_observe_trace(trace: &UserRpcObserveTrace) -> Vec<u8> {
    bitcode::encode(trace)
}

pub fn decode_user_rpc_observe_trace(bytes: &[u8]) -> P2PResult<UserRpcObserveTrace> {
    bitcode::decode(bytes).map_err(|err| P2pError::InvalidMessage {
        detail: format!("decode user rpc observe trace failed: {}", err),
    })
}

pub fn encode_user_rpc_owner1_observe_trace(trace: &UserRpcOwner1ObserveTrace) -> Vec<u8> {
    bitcode::encode(trace)
}

pub fn decode_user_rpc_owner1_observe_trace(bytes: &[u8]) -> P2PResult<UserRpcOwner1ObserveTrace> {
    bitcode::decode(bytes).map_err(|err| P2pError::InvalidMessage {
        detail: format!("decode user rpc owner1 observe trace failed: {}", err),
    })
}

#[derive(Debug, Clone)]
pub struct MsgPack<E: MsgPackSerializePart> {
    pub serialize_part: E,
    pub raw_bytes: Vec<ProstBytes>,
}

impl<E: MsgPackSerializePart> MsgPack<E> {
    pub fn msg_id(&self) -> u32 {
        self.serialize_part.msg_id()
    }

    pub fn into_wire_body(self) -> P2PResult<WireMessageBody> {
        Ok(WireMessageBody {
            serialize_part: Bytes::from(bitcode::encode(&self.serialize_part)),
            raw_bytes: self.raw_bytes,
        })
    }

    pub fn into_encode_with_msg_id_task_id(
        self,
        task_id: TaskId,
        relay: MsgPackRelay,
    ) -> P2PResult<Vec<u8>> {
        crate::p2p::encode_wire_message(self.msg_id(), task_id, relay, self.into_wire_body()?)
    }

    pub fn decode_from_body(head_meta: &MsgPackHeadMeta, body_bytes: &Bytes) -> P2PResult<Self> {
        let body = crate::p2p::decode_wire_body(head_meta, body_bytes)?;
        let serialize_part = E::decode_from(&body.serialize_part)?;
        Ok(Self {
            serialize_part,
            raw_bytes: body.raw_bytes,
        })
    }

    pub fn decode_from_body_view(
        serialize_part_length: usize,
        raw_bytes_lengths: &[u32],
        body_bytes: &Bytes,
    ) -> P2PResult<Self> {
        if body_bytes.len() < serialize_part_length {
            return Err(P2pError::InvalidMessage {
                detail: "Insufficient bytes for serialize part".to_string(),
            });
        }

        let serialize_part = E::decode_from(&body_bytes[..serialize_part_length])?;
        let mut raw_bytes = Vec::with_capacity(raw_bytes_lengths.len());
        let mut current_pos = serialize_part_length;
        for &raw_len in raw_bytes_lengths {
            let raw_end = current_pos.checked_add(raw_len as usize).ok_or_else(|| {
                P2pError::InvalidMessage {
                    detail: "raw bytes length overflow".to_string(),
                }
            })?;
            if body_bytes.len() < raw_end {
                return Err(P2pError::InvalidMessage {
                    detail: "Insufficient bytes for raw bytes".to_string(),
                });
            }
            raw_bytes.push(body_bytes.slice(current_pos..raw_end));
            current_pos = raw_end;
        }
        if current_pos != body_bytes.len() {
            return Err(P2pError::InvalidMessage {
                detail: format!(
                    "Body length mismatch: consumed={} total={}",
                    current_pos,
                    body_bytes.len()
                ),
            });
        }
        Ok(Self {
            serialize_part,
            raw_bytes,
        })
    }
}

pub trait MsgPackSerializePart:
    Encode + DecodeOwned + Sized + Send + Sync + 'static + Default + Debug + Clone
{
    fn msg_id(&self) -> u32;

    fn decode_from(bytes: &[u8]) -> P2PResult<Self> {
        bitcode::decode(bytes).map_err(|err| P2pError::InvalidMessage {
            detail: err.to_string(),
        })
    }
}

pub trait RPCReq: MsgPackSerializePart {
    type Resp: MsgPackSerializePart;
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct UserRpcBytesError {
    pub error_code: u32,
    pub error_json: String,
}

pub type UserRpcBytesFuture =
    Pin<Box<dyn Future<Output = Result<UserRpcBytesOutput, UserRpcBytesError>> + Send + 'static>>;

pub trait UserRpcBytesHandler: Send + Sync + 'static {
    fn handle(
        &self,
        from_node: NodeID,
        payload: &[u8],
    ) -> Result<UserRpcBytesOutput, UserRpcBytesError>;
}

pub trait UserRpcBytesAsyncHandler: Send + Sync + 'static {
    fn handle(&self, from_node: NodeID, payload: Vec<u8>) -> UserRpcBytesFuture;
}

impl MsgPackSerializePart for UserRpcReq {
    fn msg_id(&self) -> u32 {
        USER_RPC_REQ_MSG_ID
    }
}

impl RPCReq for UserRpcReq {
    type Resp = UserRpcResp;
}

impl MsgPackSerializePart for UserRpcResp {
    fn msg_id(&self) -> u32 {
        USER_RPC_RESP_MSG_ID
    }
}

impl MsgPackSerializePart for PromRemoteWriteProxyReq {
    fn msg_id(&self) -> u32 {
        PROM_REMOTE_WRITE_PROXY_REQ_MSG_ID
    }
}

impl RPCReq for PromRemoteWriteProxyReq {
    type Resp = PromRemoteWriteProxyResp;
}

impl MsgPackSerializePart for PromRemoteWriteProxyResp {
    fn msg_id(&self) -> u32 {
        PROM_REMOTE_WRITE_PROXY_RESP_MSG_ID
    }
}

impl MsgPackSerializePart for GreptimeOtlpLogProxyReq {
    fn msg_id(&self) -> u32 {
        GREPTIME_OTLP_LOG_PROXY_REQ_MSG_ID
    }
}

impl RPCReq for GreptimeOtlpLogProxyReq {
    type Resp = GreptimeOtlpLogProxyResp;
}

impl MsgPackSerializePart for GreptimeOtlpLogProxyResp {
    fn msg_id(&self) -> u32 {
        GREPTIME_OTLP_LOG_PROXY_RESP_MSG_ID
    }
}
