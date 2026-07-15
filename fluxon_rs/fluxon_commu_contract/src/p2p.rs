use crate::cluster_manager::NodeID;
use crate::{NodeIDString, ShareGroupOwnerRef};
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;
use thiserror::Error;

pub mod rpc;
pub mod surface;
pub mod wire;

pub use rpc::{
    MIN_EXPLICIT_RPC_TIMEOUT_MS, MIN_EXPLICIT_RPC_TIMEOUT_SECS, MsgPack, MsgPackSerializePart,
    RPCReq, RpcCallObserveTrace, RpcCallObservedOutput, USER_RPC_OBSERVE_TRACE_RAW_BYTES_INDEX,
    USER_RPC_OWNER1_OBSERVE_TRACE_RAW_BYTES_INDEX, USER_RPC_REQ_MSG_ID,
    USER_RPC_REQUEST_OWNER1_OBSERVE_TRACE_RAW_BYTES_INDEX, USER_RPC_RESP_MSG_ID,
    UserRpcBytesAsyncHandler, UserRpcBytesError, UserRpcBytesFuture, UserRpcBytesHandler,
    UserRpcObserveTrace, UserRpcOwner1ObserveTrace, UserRpcTransportPathKind,
    current_cross_process_monotonic_us, decode_user_rpc_observe_trace,
    decode_user_rpc_owner1_observe_trace, encode_user_rpc_observe_trace,
    encode_user_rpc_owner1_observe_trace, validate_explicit_rpc_timeout,
    validate_explicit_rpc_timeout_ms,
};
pub use surface::*;
pub use wire::*;

pub type P2PResult<T> = Result<T, P2pError>;
pub type P2PError = P2pError;

static MONO_BASE: OnceLock<::tokio::time::Instant> = OnceLock::new();

fn mono_base() -> &'static ::tokio::time::Instant {
    MONO_BASE.get_or_init(::tokio::time::Instant::now)
}

#[derive(Debug, Clone, Serialize, Deserialize, Error)]
#[serde(tag = "type", content = "data")]
pub enum P2pError {
    #[error("IO error: {detail}")]
    IoError { detail: String },
    #[error("Serialization error: {detail}")]
    SerdeError { detail: String },
    #[error("No connection ready: {nodeid}")]
    NoConnectionReady { nodeid: NodeIDString },
    #[error("Node not found: {node}")]
    NodeNotFound { node: NodeIDString },
    #[error("Node not connected: {node}")]
    NodeNotConnected { node: NodeIDString },
    #[error("Node port not ready: {node}")]
    NodePortNotReady { node: NodeIDString },
    #[error("Connection error: from={from}, to={to}, context={context}")]
    ConnectionError {
        from: NodeIDString,
        to: NodeIDString,
        context: String,
    },
    #[error("Invalid message: {detail}")]
    InvalidMessage { detail: String },
    #[error("Timeout: {detail}")]
    Timeout { detail: String },
    #[error("Send failed: {detail}")]
    SendFailed { detail: String },
    #[error("Other error: {detail}")]
    Other { detail: String },
    #[error("Failed to start server: {detail}")]
    StartServerError { detail: String },
    #[error("Response downcast error: expected_msg_pack_id: {expected_msg_pack_id}")]
    ResponseDowncastError { expected_msg_pack_id: u32 },
    #[error("Failed to deserialize message ID or task ID: {detail}, context: {context}")]
    DeserialMsgIdTaskIdFailed { detail: String, context: String },
    #[error("System shutdown")]
    SystemShutdown {},
    #[error("RPC call failed (msg_id={msg_id}): {err}")]
    RPCCallFailed { msg_id: u32, err: String },
    #[error("Handshake error: from={from}, to={to}, node_start_time={node_start_time}")]
    HandshakeError {
        from: NodeIDString,
        to: NodeIDString,
        node_start_time: i64,
    },
    #[error(
        "Invalid RPC timeout: timeout_ms={timeout_ms}, min_timeout_ms={min_timeout_ms}, reason={reason}"
    )]
    InvalidRpcTimeout {
        timeout_ms: u64,
        min_timeout_ms: u64,
        reason: String,
    },
    #[error("Iceoryx2 transport not started")]
    Iceoryx2TransportNotStarted {},
}

impl P2pError {
    pub fn code(&self) -> u32 {
        match self {
            Self::IoError { .. } => 600,
            Self::SerdeError { .. } => 601,
            Self::NoConnectionReady { .. } => 602,
            Self::NodeNotFound { .. } => 603,
            Self::NodeNotConnected { .. } => 604,
            Self::NodePortNotReady { .. } => 605,
            Self::ConnectionError { .. } => 606,
            Self::InvalidMessage { .. } => 607,
            Self::Timeout { .. } => 608,
            Self::SendFailed { .. } => 609,
            Self::Other { .. } => 610,
            Self::StartServerError { .. } => 611,
            Self::ResponseDowncastError { .. } => 612,
            Self::DeserialMsgIdTaskIdFailed { .. } => 613,
            Self::SystemShutdown { .. } => 614,
            Self::RPCCallFailed { .. } => 615,
            Self::HandshakeError { .. } => 616,
            Self::InvalidRpcTimeout { .. } => 617,
            Self::Iceoryx2TransportNotStarted { .. } => 618,
        }
    }

    pub fn to_code_and_json(&self) -> (u32, String) {
        (self.code(), serde_json::to_string(self).unwrap())
    }

    pub fn from_code_and_json(code: u32, json: &str) -> Option<Self> {
        let value: Self = serde_json::from_str(json).ok()?;
        if value.code() == code {
            return Some(value);
        }
        None
    }
}

impl From<anyhow::Error> for P2pError {
    fn from(value: anyhow::Error) -> Self {
        Self::Other {
            detail: value.to_string(),
        }
    }
}

impl TierSnapshot {
    pub fn now_ms() -> u64 {
        ::tokio::time::Instant::now()
            .duration_since(*mono_base())
            .as_millis() as u64
    }

    pub fn peer_gen(&self, peer_id: &NodeID) -> Option<PeerGen> {
        self.peers.get(peer_id).and_then(|v| v.peer_gen.clone())
    }

    pub fn is_send_ready_intra(&self, peer_gen: &PeerGen) -> bool {
        self.peers
            .get(&peer_gen.peer_id)
            .is_some_and(|v| v.peer_gen.as_ref() == Some(peer_gen) && v.intra_conn_ready)
    }

    pub fn is_send_ready_direct(&self, peer_gen: &PeerGen) -> bool {
        self.peers
            .get(&peer_gen.peer_id)
            .is_some_and(|v| v.peer_gen.as_ref() == Some(peer_gen) && v.direct_conn_ready)
    }

    fn is_crossowner_intra_disabled_for_peer(&self, peer_gen: &PeerGen) -> bool {
        if !self.disable_crossowner_ipc || !self.is_send_ready_intra(peer_gen) {
            return false;
        }
        let Some(self_owner) = self.share_group_owner(&self.self_peer_gen.peer_id) else {
            return false;
        };
        let Some(peer_owner) = self.share_group_owner(&peer_gen.peer_id) else {
            return false;
        };
        self_owner != peer_owner
    }

    pub fn is_send_ready_intra_effective(&self, peer_gen: &PeerGen) -> bool {
        self.is_send_ready_intra(peer_gen) && !self.is_crossowner_intra_disabled_for_peer(peer_gen)
    }

    pub fn is_any_send_ready(&self, peer_gen: &PeerGen) -> bool {
        self.is_send_ready_intra_effective(peer_gen) || self.is_send_ready_direct(peer_gen)
    }

    pub fn is_transfer_rpc_ready(&self, peer_gen: &PeerGen) -> bool {
        self.peers.get(&peer_gen.peer_id).is_some_and(|v| {
            v.peer_gen.as_ref() == Some(peer_gen)
                && v.transfer_backend_epoch.is_some()
                && v.transfer_backend_epoch == v.transfer_rpc_ready_backend_epoch
        })
    }

    pub fn transfer_backend_epoch(&self, peer_gen: &PeerGen) -> Option<u64> {
        self.peers.get(&peer_gen.peer_id).and_then(|v| {
            (v.peer_gen.as_ref() == Some(peer_gen))
                .then_some(v.transfer_backend_epoch)
                .flatten()
        })
    }

    pub fn verify_peer_gen_is_current(&self, peer_gen: &PeerGen) -> bool {
        self.peers
            .get(&peer_gen.peer_id)
            .is_some_and(|v| v.peer_gen.as_ref() == Some(peer_gen))
    }

    pub fn share_group_owner(&self, peer_id: &NodeID) -> Option<&ShareGroupOwnerRef> {
        self.peers
            .get(peer_id)
            .and_then(|v| v.share_group_owner.as_ref())
    }

    pub fn share_group_owner_id(&self, peer_id: &NodeID) -> Option<&str> {
        self.share_group_owner(peer_id)
            .map(|owner| owner.owner_id.as_str())
    }

    pub fn is_current_relay_member(&self, peer_gen: &PeerGen) -> bool {
        self.peers
            .get(&peer_gen.peer_id)
            .is_some_and(|peer| peer.peer_gen.as_ref() == Some(peer_gen) && peer.is_p2p_relay)
    }

    pub fn same_share_group(&self, lhs: &NodeID, rhs: &NodeID) -> bool {
        match (self.share_group_owner(lhs), self.share_group_owner(rhs)) {
            (Some(lhs_owner), Some(rhs_owner)) => lhs_owner == rhs_owner,
            _ => false,
        }
    }
}
