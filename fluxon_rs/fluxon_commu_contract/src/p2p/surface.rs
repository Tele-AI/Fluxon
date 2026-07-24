use crate::ShareGroupOwnerRef;
use crate::cluster_manager::{ClusterMember, MemberRdmaResolvedConfig, NodeID};
use crate::transfer_engine::{AttachedTransferEngine, TransferEngineResult};
use bitcode::{Decode, Encode};
use std::collections::HashMap;
use std::time::Duration;

// Keep stable p2p support objects in a dedicated open-surface module so runtime files no longer
// act as the long-term contract authority.

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PeerGen {
    pub peer_id: NodeID,
    pub node_start_time: i64,
}

pub const MAX_RELAY_HOPS: u8 = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum P2pLane {
    IntraMachine,
    Direct,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum RpcTransportPolicy {
    AllowTransferRpcFastPath,
    ForceTransport,
}

pub enum TierEvent {
    MemberUpsert {
        member: ClusterMember,
    },
    MemberLeft {
        peer_id: NodeID,
    },
    ConnReady {
        peer_gen: PeerGen,
        lane: P2pLane,
    },
    ConnLost {
        peer_gen: PeerGen,
        lane: P2pLane,
        detail: String,
    },
    ConnFailed {
        peer_gen: PeerGen,
        lane: P2pLane,
        detail: String,
    },
    RelayCapsUpdated {
        hop_gen: PeerGen,
        caps: RelayCapsSnapshot,
    },
    RelayCapsExpired {
        hop_gen: PeerGen,
        caps_as_of_ts_ms: u64,
    },
    TransferEngineAttached {
        transfer_engine: AttachedTransferEngine,
        completion: ::tokio::sync::oneshot::Sender<TransferEngineResult<()>>,
    },
    SelfRdmaConfigUpdated {
        config: MemberRdmaResolvedConfig,
    },
    TransferRpcBackendReady,
    TransferRpcBackendLost {
        detail: String,
    },
    TransferRpcPeerReady {
        peer_gen: PeerGen,
        peer_transfer_backend_epoch: u64,
    },
    TransferRpcPeerLost {
        peer_gen: PeerGen,
        peer_transfer_backend_epoch: Option<u64>,
        detail: String,
    },
    TransferLinkDirectGraphUpdated {
        direct_graph: HashMap<NodeID, Vec<NodeID>>,
    },
    Shutdown,
}

#[derive(Debug, Clone)]
pub struct RelayCapsSnapshot {
    pub as_of_ts_ms: u64,
    pub reachable_targets: Vec<PeerGen>,
    pub deliverable_targets: Vec<PeerGen>,
}

impl RelayCapsSnapshot {
    pub fn is_known(&self, now_ms: u64, max_age: Duration) -> bool {
        let max_age_ms = max_age.as_millis() as u64;
        now_ms.saturating_sub(self.as_of_ts_ms) <= max_age_ms
    }
}

#[derive(Debug, Clone)]
pub struct TierPeerView {
    pub peer_gen: Option<PeerGen>,
    pub intra_conn_ready: bool,
    pub direct_conn_ready: bool,
    pub transfer_backend_epoch: Option<u64>,
    pub transfer_rpc_ready_backend_epoch: Option<u64>,
    pub share_group_owner: Option<ShareGroupOwnerRef>,
    pub is_p2p_relay: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayRoute {
    pub first_hop: PeerGen,
    pub relay_hops: u8,
}

#[derive(Debug, Clone)]
pub struct TierSnapshot {
    pub self_peer_gen: PeerGen,
    pub disable_crossowner_ipc: bool,
    pub peers: HashMap<NodeID, TierPeerView>,
    pub relay_set_snapshot: Vec<PeerGen>,
    pub relay_caps_by_hop: HashMap<PeerGen, RelayCapsSnapshot>,
    pub direct_graph: HashMap<NodeID, Vec<NodeID>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Encode, Decode)]
pub enum P2pTcpThreadReactorWaitMode {
    #[default]
    BusyPoll,
    EventDriven,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Encode, Decode)]
pub struct P2pTcpThreadTransportTuning {
    pub reactor_shard_count: Option<u8>,
    pub reactor_wait_mode: P2pTcpThreadReactorWaitMode,
    pub bulk_lane_count: Option<u8>,
    pub control_lane_count: Option<u8>,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct UserRpcReq {
    pub path: String,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct UserRpcResp {
    pub error_code: u32,
    pub error_json: String,
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct P2pModuleNewArg {
    pub p2p_listen_port: Option<u16>,
    pub tcp_thread_transport_tuning: P2pTcpThreadTransportTuning,
    pub disable_crossowner_ipc: bool,
    pub iceoryx_external_busy_poll: bool,
    pub iceoryx_owner_client_busy_poll: bool,
    pub user_rpc_sync_handler_thread_count: Option<u16>,
}

impl P2pModuleNewArg {
    pub fn new(
        p2p_listen_port: Option<u16>,
        tcp_thread_transport_tuning: P2pTcpThreadTransportTuning,
        disable_crossowner_ipc: bool,
        iceoryx_external_busy_poll: bool,
    ) -> Self {
        Self {
            p2p_listen_port,
            tcp_thread_transport_tuning,
            disable_crossowner_ipc,
            iceoryx_external_busy_poll,
            iceoryx_owner_client_busy_poll: true,
            user_rpc_sync_handler_thread_count: None,
        }
    }

    pub fn with_iceoryx_owner_client_busy_poll(
        mut self,
        iceoryx_owner_client_busy_poll: bool,
    ) -> Self {
        self.iceoryx_owner_client_busy_poll = iceoryx_owner_client_busy_poll;
        self
    }

    pub fn with_user_rpc_sync_handler_thread_count(
        mut self,
        user_rpc_sync_handler_thread_count: Option<u16>,
    ) -> Self {
        self.user_rpc_sync_handler_thread_count = user_rpc_sync_handler_thread_count;
        self
    }
}
