#![allow(unused_assignments)]

use bitcode::{Decode, Encode};
use bytes::Bytes;
use std::collections::HashMap;

use crate::{
    ClientTransferEngineNewArg, ClientTransferEngineRuntimeConfig, ClusterEvent,
    ClusterManagerNewArg, ClusterMember, MemberRdmaResolvedConfig, MemberRdmaTransferEngineRuntime,
    NodeRole, P2pModuleNewArg, P2pTransportKind, ShareGroupOwnerRef, TransferBreakdown,
    TransferLinkP2pState, TransferLinkRecord, TransferReadyInfo, TransferRpcFastPathInbound,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
#[repr(u32)]
pub enum ClosedRuntimeHandleKind {
    ClusterManager,
    P2pModule,
    ClientTransferEngineCore,
    ClusterEventStream,
    ClusterRdmaResolvedConfigStream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
#[repr(C)]
pub struct ClosedRuntimeHandle {
    pub kind: ClosedRuntimeHandleKind,
    pub raw: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
#[repr(C)]
pub struct ClosedRuntimeHostCallbackHandle {
    pub raw: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[repr(u32)]
pub enum ClosedRuntimeDispatchTransportPolicy {
    AllowTransferRpcFastPath = 0,
    ForceTransport = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[repr(C)]
pub struct ClosedRuntimeRawSlice {
    pub ptr: u64,
    pub len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[repr(C)]
pub struct ClosedRuntimeDispatchBodyView {
    pub owner_handle: u64,
    pub full_body: ClosedRuntimeRawSlice,
    pub serialize_part: ClosedRuntimeRawSlice,
    pub raw_bytes_lengths_ptr: u64,
    pub raw_bytes_lengths_len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[repr(C)]
pub struct ClosedRuntimeWireBodyView {
    pub owner_handle: u64,
    pub serialize_part: ClosedRuntimeRawSlice,
    pub raw_bytes_ptr: u64,
    pub raw_bytes_len: usize,
    pub raw_bytes_lengths_ptr: u64,
    pub raw_bytes_lengths_len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[repr(C)]
pub struct ClosedRuntimeWireTransportLocalObserveView {
    pub frame_recv_done_ts_us: i64,
    pub dispatch_enqueued_ts_us: i64,
    pub dispatch_started_ts_us: i64,
    pub complete_pending_call_ts_us: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[repr(C)]
pub struct ClosedRuntimeDispatchRequestView {
    pub reply_next_hop: ClosedRuntimeRawSlice,
    pub msg_id: u32,
    pub task_id: crate::p2p::TaskId,
    pub logical_source_peer_id: ClosedRuntimeRawSlice,
    pub logical_source_node_start_time: i64,
    pub logical_target_peer_id: ClosedRuntimeRawSlice,
    pub logical_target_node_start_time: i64,
    pub remaining_hops: u8,
    pub default_resp_transport_policy: ClosedRuntimeDispatchTransportPolicy,
    pub incoming_frame_recv_done_ts_us: i64,
    pub incoming_dispatch_enqueued_ts_us: i64,
    pub incoming_dispatch_started_ts_us: i64,
    pub incoming_complete_pending_call_ts_us: i64,
    pub body: ClosedRuntimeDispatchBodyView,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[repr(C)]
pub struct ClosedRuntimeP2pCallRawObservedRequestView {
    pub handle: ClosedRuntimeHandle,
    pub node: ClosedRuntimeRawSlice,
    pub msg_id: u32,
    pub timeout_ms: u64,
    pub has_timeout: u8,
    pub transport_policy: ClosedRuntimeDispatchTransportPolicy,
    pub body: ClosedRuntimeWireBodyView,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[repr(C)]
pub struct ClosedRuntimeP2pSendResponseRawRequestView {
    pub handle: ClosedRuntimeHandle,
    pub logical_target: ClosedRuntimeRawSlice,
    pub reply_next_hop: ClosedRuntimeRawSlice,
    pub task_id: crate::p2p::TaskId,
    pub msg_id: u32,
    pub transport_policy: ClosedRuntimeDispatchTransportPolicy,
    pub incoming_local_observe: ClosedRuntimeWireTransportLocalObserveView,
    pub body: ClosedRuntimeWireBodyView,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[repr(C)]
pub struct ClosedRuntimeWireIncomingBodyView {
    pub full_body: ClosedRuntimeRawSlice,
    pub serialize_part: ClosedRuntimeRawSlice,
    pub raw_bytes_lengths_ptr: u64,
    pub raw_bytes_lengths_len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[repr(C)]
pub struct ClosedRuntimeWireIncomingMessageView {
    pub owner_handle: u64,
    pub from_node: ClosedRuntimeRawSlice,
    pub msg_id: u32,
    pub task_id: crate::p2p::TaskId,
    pub logical_source_peer_id: ClosedRuntimeRawSlice,
    pub logical_source_node_start_time: i64,
    pub logical_target_peer_id: ClosedRuntimeRawSlice,
    pub logical_target_node_start_time: i64,
    pub remaining_hops: u8,
    pub local_observe: ClosedRuntimeWireTransportLocalObserveView,
    pub body: ClosedRuntimeWireIncomingBodyView,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[repr(C)]
pub struct ClosedRuntimeRpcCallTransportObserveTraceView {
    pub caller_submit_us: i64,
    pub caller_submit_ts_us: i64,
    pub request_path_kind: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[repr(C)]
pub struct ClosedRuntimeCallRawObservedOutputView {
    pub message: ClosedRuntimeWireIncomingMessageView,
    pub observe: ClosedRuntimeRpcCallTransportObserveTraceView,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[repr(C)]
pub struct ClosedRuntimeUserRpcBytesRequestView {
    pub from_node: ClosedRuntimeRawSlice,
    pub payload: ClosedRuntimeRawSlice,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[repr(C)]
pub struct ClosedRuntimeUserRpcHandlerLocalObserveView {
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

impl From<crate::p2p::rpc::UserRpcHandlerLocalObserve>
    for ClosedRuntimeUserRpcHandlerLocalObserveView
{
    fn from(value: crate::p2p::rpc::UserRpcHandlerLocalObserve) -> Self {
        Self {
            py_with_gil_us: value.py_with_gil_us,
            py_gil_wait_us: value.py_gil_wait_us,
            py_arg_build_us: value.py_arg_build_us,
            py_call_us: value.py_call_us,
            py_result_unpack_us: value.py_result_unpack_us,
            py_result_copy_us: value.py_result_copy_us,
            py_decode_us: value.py_decode_us,
            py_handler_body_us: value.py_handler_body_us,
            py_encode_us: value.py_encode_us,
        }
    }
}

impl From<ClosedRuntimeUserRpcHandlerLocalObserveView>
    for crate::p2p::rpc::UserRpcHandlerLocalObserve
{
    fn from(value: ClosedRuntimeUserRpcHandlerLocalObserveView) -> Self {
        Self {
            py_with_gil_us: value.py_with_gil_us,
            py_gil_wait_us: value.py_gil_wait_us,
            py_arg_build_us: value.py_arg_build_us,
            py_call_us: value.py_call_us,
            py_result_unpack_us: value.py_result_unpack_us,
            py_result_copy_us: value.py_result_copy_us,
            py_decode_us: value.py_decode_us,
            py_handler_body_us: value.py_handler_body_us,
            py_encode_us: value.py_encode_us,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct ClosedRuntimeDispatchRequestOwned {
    pub reply_next_hop: String,
    pub msg_id: u32,
    pub task_id: crate::p2p::TaskId,
    pub logical_source_peer_id: String,
    pub logical_source_node_start_time: i64,
    pub logical_target_peer_id: String,
    pub logical_target_node_start_time: i64,
    pub remaining_hops: u8,
    pub body: ClosedRuntimeWireMessageBodyOwned,
    pub default_resp_transport_policy: ClosedRuntimeDispatchTransportPolicy,
    pub incoming_local_observe: crate::p2p::WireTransportLocalObserve,
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum ClosedRuntimeClusterManagerCall {
    Init2ForInitDag,
    JoinCluster,
    SelfMemberId,
    ClusterName,
    EtcdEndpoints,
    GetMemberInfoCached {
        member_id: String,
    },
    LeaveCluster,
    SubscribeEvents,
    IsWatching,
    WaitMemberCount {
        white_list_roles: Vec<NodeRole>,
    },
    StartWatching,
    StopWatching,
    GetMembers,
    GetPrevMemberInfo {
        member_id: String,
    },
    GetClientMembers,
    GetSelfInfo,
    GetMasterMember,
    CurrentSelfRdmaResolvedConfig,
    WatchSelfRdmaResolvedConfig,
    SetSelfRdmaTransferEngineRuntime {
        runtime: MemberRdmaTransferEngineRuntime,
    },
    SetListeningPort {
        port: u16,
    },
    SetPeerAccessibleIpWithStartTime {
        peer_id: String,
        peer_start_time: i64,
        ip: String,
    },
    WaitAccessibleSelfIpForCurrentStartTime,
    FetchTransferReadyForMember {
        member_id: String,
    },
    PublishSelfTransferReady {
        backend_epoch: u64,
    },
    SetSelfTransferBackendEpoch {
        backend_epoch: u64,
    },
    ClearSelfTransferBackendEpoch,
    SetSelfShareGroupBinding {
        owner_ref: ShareGroupOwnerRef,
    },
    SetSelfSubCluster {
        sub_cluster: Option<String>,
    },
    TryReportTransferEngineRoute {
        from: String,
        to: String,
        record: TransferLinkRecord,
    },
    TryReportTransferLinkP2p {
        from: String,
        to: String,
        record: TransferLinkRecord,
    },
    TryReportTransferLinkTe {
        from: String,
        to: String,
        record: TransferLinkRecord,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum ClosedRuntimeClusterManagerResponse {
    Unit,
    BoolValue(bool),
    UsizeValue(usize),
    StringValue(String),
    StringListValue(Vec<String>),
    ClusterMemberValue(ClusterMember),
    OptionalClusterMemberValue(Option<ClusterMember>),
    ClusterMembersValue(Vec<ClusterMember>),
    MemberRdmaResolvedConfigValue(MemberRdmaResolvedConfig),
    OptionalTransferReadyInfoValue(Option<TransferReadyInfo>),
    TransferReadyInfoValue(TransferReadyInfo),
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum ClosedRuntimeClusterEventStreamItem {
    Event(ClusterEvent),
    Lagged { skipped: u64 },
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum ClosedRuntimeClusterRdmaResolvedConfigStreamItem {
    Value(MemberRdmaResolvedConfig),
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Encode, Decode)]
pub struct ClosedRuntimePeerGen {
    pub peer_id: String,
    pub node_start_time: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct ClosedRuntimeRelayCapsSnapshot {
    pub as_of_ts_ms: u64,
    pub reachable_targets: Vec<ClosedRuntimePeerGen>,
    pub deliverable_targets: Vec<ClosedRuntimePeerGen>,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct ClosedRuntimeTierPeerView {
    pub peer_gen: Option<ClosedRuntimePeerGen>,
    pub intra_conn_ready: bool,
    pub direct_conn_ready: bool,
    pub transfer_backend_epoch: Option<u64>,
    pub transfer_rpc_ready_backend_epoch: Option<u64>,
    pub share_group_owner: Option<ShareGroupOwnerRef>,
    pub is_p2p_relay: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct ClosedRuntimeTierSnapshot {
    pub self_peer_gen: ClosedRuntimePeerGen,
    pub disable_crossowner_ipc: bool,
    pub peers: HashMap<String, ClosedRuntimeTierPeerView>,
    pub relay_set_snapshot: Vec<ClosedRuntimePeerGen>,
    pub relay_caps_by_hop: HashMap<ClosedRuntimePeerGen, ClosedRuntimeRelayCapsSnapshot>,
    pub direct_graph: HashMap<String, Vec<String>>,
}

impl From<crate::PeerGen> for ClosedRuntimePeerGen {
    fn from(value: crate::PeerGen) -> Self {
        Self {
            peer_id: value.peer_id.into_owned(),
            node_start_time: value.node_start_time,
        }
    }
}

impl From<ClosedRuntimePeerGen> for crate::PeerGen {
    fn from(value: ClosedRuntimePeerGen) -> Self {
        Self {
            peer_id: value.peer_id.into(),
            node_start_time: value.node_start_time,
        }
    }
}

impl From<crate::RelayCapsSnapshot> for ClosedRuntimeRelayCapsSnapshot {
    fn from(value: crate::RelayCapsSnapshot) -> Self {
        Self {
            as_of_ts_ms: value.as_of_ts_ms,
            reachable_targets: value
                .reachable_targets
                .into_iter()
                .map(ClosedRuntimePeerGen::from)
                .collect(),
            deliverable_targets: value
                .deliverable_targets
                .into_iter()
                .map(ClosedRuntimePeerGen::from)
                .collect(),
        }
    }
}

impl From<ClosedRuntimeRelayCapsSnapshot> for crate::RelayCapsSnapshot {
    fn from(value: ClosedRuntimeRelayCapsSnapshot) -> Self {
        Self {
            as_of_ts_ms: value.as_of_ts_ms,
            reachable_targets: value
                .reachable_targets
                .into_iter()
                .map(crate::PeerGen::from)
                .collect(),
            deliverable_targets: value
                .deliverable_targets
                .into_iter()
                .map(crate::PeerGen::from)
                .collect(),
        }
    }
}

impl From<crate::TierPeerView> for ClosedRuntimeTierPeerView {
    fn from(value: crate::TierPeerView) -> Self {
        Self {
            peer_gen: value.peer_gen.map(ClosedRuntimePeerGen::from),
            intra_conn_ready: value.intra_conn_ready,
            direct_conn_ready: value.direct_conn_ready,
            transfer_backend_epoch: value.transfer_backend_epoch,
            transfer_rpc_ready_backend_epoch: value.transfer_rpc_ready_backend_epoch,
            share_group_owner: value.share_group_owner,
            is_p2p_relay: value.is_p2p_relay,
        }
    }
}

impl From<ClosedRuntimeTierPeerView> for crate::TierPeerView {
    fn from(value: ClosedRuntimeTierPeerView) -> Self {
        Self {
            peer_gen: value.peer_gen.map(crate::PeerGen::from),
            intra_conn_ready: value.intra_conn_ready,
            direct_conn_ready: value.direct_conn_ready,
            transfer_backend_epoch: value.transfer_backend_epoch,
            transfer_rpc_ready_backend_epoch: value.transfer_rpc_ready_backend_epoch,
            share_group_owner: value.share_group_owner,
            is_p2p_relay: value.is_p2p_relay,
        }
    }
}

impl From<crate::TierSnapshot> for ClosedRuntimeTierSnapshot {
    fn from(value: crate::TierSnapshot) -> Self {
        Self {
            self_peer_gen: value.self_peer_gen.into(),
            disable_crossowner_ipc: value.disable_crossowner_ipc,
            peers: value
                .peers
                .into_iter()
                .map(|(peer_id, peer_view)| (peer_id.into_owned(), peer_view.into()))
                .collect(),
            relay_set_snapshot: value
                .relay_set_snapshot
                .into_iter()
                .map(ClosedRuntimePeerGen::from)
                .collect(),
            relay_caps_by_hop: value
                .relay_caps_by_hop
                .into_iter()
                .map(|(hop, caps)| (hop.into(), caps.into()))
                .collect(),
            direct_graph: value
                .direct_graph
                .into_iter()
                .map(|(from, targets)| {
                    (
                        from.into_owned(),
                        targets
                            .into_iter()
                            .map(|target| target.into_owned())
                            .collect(),
                    )
                })
                .collect(),
        }
    }
}

impl From<ClosedRuntimeTierSnapshot> for crate::TierSnapshot {
    fn from(value: ClosedRuntimeTierSnapshot) -> Self {
        Self {
            self_peer_gen: value.self_peer_gen.into(),
            disable_crossowner_ipc: value.disable_crossowner_ipc,
            peers: value
                .peers
                .into_iter()
                .map(|(peer_id, peer_view)| (peer_id.into(), peer_view.into()))
                .collect(),
            relay_set_snapshot: value
                .relay_set_snapshot
                .into_iter()
                .map(crate::PeerGen::from)
                .collect(),
            relay_caps_by_hop: value
                .relay_caps_by_hop
                .into_iter()
                .map(|(hop, caps)| (hop.into(), caps.into()))
                .collect(),
            direct_graph: value
                .direct_graph
                .into_iter()
                .map(|(from, targets)| (from.into(), targets.into_iter().map(Into::into).collect()))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum ClosedRuntimeP2pLocalIpcDirection {
    Tx,
    Rx,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct ClosedRuntimeWireMessageBodyOwned {
    pub serialize_part: Vec<u8>,
    pub raw_bytes: Vec<Vec<u8>>,
}

impl From<crate::p2p::WireMessageBody> for ClosedRuntimeWireMessageBodyOwned {
    fn from(value: crate::p2p::WireMessageBody) -> Self {
        Self {
            serialize_part: value.serialize_part.to_vec(),
            raw_bytes: value
                .raw_bytes
                .into_iter()
                .map(|bytes| bytes.to_vec())
                .collect(),
        }
    }
}

impl From<ClosedRuntimeWireMessageBodyOwned> for crate::p2p::WireMessageBody {
    fn from(value: ClosedRuntimeWireMessageBodyOwned) -> Self {
        Self {
            serialize_part: Bytes::from(value.serialize_part),
            raw_bytes: value.raw_bytes.into_iter().map(Bytes::from).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum ClosedRuntimeDispatchResponse {
    Ok,
    Err { error_code: u32, error_json: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct ClosedRuntimeRpcCallTransportObserveTrace {
    pub caller_submit_us: i64,
    pub caller_submit_ts_us: i64,
    pub request_path_kind: crate::p2p::rpc::UserRpcTransportPathKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct ClosedRuntimeWireIncomingMessageOwned {
    pub from_node: String,
    pub head: crate::p2p::MsgPackHeadMeta,
    pub body: Vec<u8>,
    pub local_observe: crate::p2p::WireTransportLocalObserve,
}

impl From<crate::p2p::WireIncomingMessage> for ClosedRuntimeWireIncomingMessageOwned {
    fn from(value: crate::p2p::WireIncomingMessage) -> Self {
        Self {
            from_node: value.from_node.into_owned(),
            head: value.head,
            body: value.body.to_vec(),
            local_observe: value.local_observe,
        }
    }
}

impl From<ClosedRuntimeWireIncomingMessageOwned> for crate::p2p::WireIncomingMessage {
    fn from(value: ClosedRuntimeWireIncomingMessageOwned) -> Self {
        Self {
            from_node: value.from_node.into(),
            head: value.head,
            body: Bytes::from(value.body),
            local_observe: value.local_observe,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct ClosedRuntimeCallRawObservedOutput {
    pub message: ClosedRuntimeWireIncomingMessageOwned,
    pub observe: ClosedRuntimeRpcCallTransportObserveTrace,
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum ClosedRuntimeP2pCall {
    Init2ForInitDag,
    Init3ForInitDag,
    AttachTransferEngine {
        transfer_engine: ClosedRuntimeHandle,
    },
    TryRecordLocalIpcBytesForOwnerTopology {
        logical_peer: String,
        direction: ClosedRuntimeP2pLocalIpcDirection,
        bytes: u64,
    },
    TierSnapshot,
    PeekP2pLinkState {
        peer: String,
    },
    PeekP2pTransportKind {
        peer: String,
    },
    VerifyPeerId {
        peer_id: String,
    },
    NotifyPeerConnectedIncomingIce {
        peer: String,
    },
    NotifyPeerDisconnectedAll {
        peer: String,
    },
    NotifyPeerDisconnectedIce {
        peer: String,
    },
    NotifyTransferRpcBackendReady,
    NotifyTransferRpcBackendLost {
        detail: String,
    },
    NotifyTransferRpcPeerReady {
        peer_gen: ClosedRuntimePeerGen,
        peer_transfer_backend_epoch: u64,
    },
    RegisterUserRpcBytesHandler {
        path: String,
        callback: ClosedRuntimeHostCallbackHandle,
        is_async: bool,
    },
    RegisterDispatch {
        msg_id: u32,
        callback: ClosedRuntimeHostCallbackHandle,
    },
    RegisterRpcResponseMsgId {
        msg_id: u32,
    },
    CallRawObserved {
        node: String,
        msg_id: u32,
        body: ClosedRuntimeWireMessageBodyOwned,
        timeout_ms: Option<u64>,
        transport_policy: crate::p2p::RpcTransportPolicy,
    },
    SendResponseRaw {
        logical_target: String,
        reply_next_hop: String,
        task_id: crate::p2p::TaskId,
        msg_id: u32,
        body: ClosedRuntimeWireMessageBodyOwned,
        transport_policy: crate::p2p::RpcTransportPolicy,
        incoming_local_observe: crate::p2p::WireTransportLocalObserve,
    },
    EnsurePeerSendReady {
        logical_target: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct ClosedRuntimeUserRpcBytesRequest {
    pub from_node: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum ClosedRuntimeUserRpcBytesResponse {
    Ok {
        output: crate::p2p::rpc::UserRpcBytesOutput,
    },
    Err {
        error: crate::p2p::rpc::UserRpcBytesError,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum ClosedRuntimeP2pResponse {
    Unit,
    BoolValue(bool),
    TierSnapshotValue(ClosedRuntimeTierSnapshot),
    TransferLinkP2pStateValue(TransferLinkP2pState),
    OptionalP2pTransportKindValue(Option<P2pTransportKind>),
    CallRawObservedValue(ClosedRuntimeCallRawObservedOutput),
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum ClosedRuntimeTransferEngineOpenRuntimeRequest {
    SupportsLocalSegmentTransfer,
    RegisterP2pTransferRpc,
    EnsureLocalSegmentGuard {
        local_addr: u64,
        previous_guard_handle: Option<u64>,
    },
    DropLocalSegmentGuard {
        guard_handle: u64,
    },
    P2pReadToLocal {
        peer: String,
        remote_src: u64,
        local_target: u64,
        len: u64,
        guard_handle: u64,
    },
    P2pWriteFromLocal {
        peer: String,
        local_src: u64,
        remote_target: u64,
        len: u64,
        guard_handle: u64,
    },
    RecordPeerNetworkBytes {
        logical_peer: String,
        direction: ClosedRuntimeP2pLocalIpcDirection,
        bytes: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum ClosedRuntimeTransferEngineOpenRuntimeResponse {
    Unit,
    BoolValue(bool),
    GuardHandleValue(u64),
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct ClosedRuntimeDesiredTransferPeer {
    pub peer_gen: ClosedRuntimePeerGen,
    pub peer_transfer_backend_epoch: u64,
    pub enable_transfer_rpc: bool,
    pub enable_transfer_segment: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum ClosedRuntimeLocalMemoryKind {
    Host,
    Gpu { device_id: u32 },
}

pub const CLOSED_RUNTIME_DIRECT_FAST_PATH_NOT_READY_MARKER: &str =
    "fluxon_direct_fast_path_not_ready";

#[derive(Debug, Clone, Encode, Decode)]
pub enum ClosedRuntimeTransferEngineCall {
    Init2ForInitDag {
        open_runtime_callback: ClosedRuntimeHostCallbackHandle,
        supports_local_segment_transfer: bool,
    },
    EnsureStartedIfNeeded,
    CurrentRuntimeConfig,
    UpdateRuntimeConfig {
        config: ClientTransferEngineRuntimeConfig,
    },
    UpdateEnabledRdmaDevices {
        enabled_devices: Vec<String>,
    },
    SyncDesiredPeers {
        desired_peers: Vec<ClosedRuntimeDesiredTransferPeer>,
    },
    RegisterLocalSegment {
        allocated_addr: u64,
        allocated_size: u64,
        memory_kind: ClosedRuntimeLocalMemoryKind,
    },
    UnregisterLocalSegment {
        allocated_addr: u64,
        allocated_size: u64,
        memory_kind: ClosedRuntimeLocalMemoryKind,
    },
    TransferDataNoCopy {
        peer_node: Option<String>,
        peer_src_or_target: bool,
        src_addr: u64,
        target_addr: u64,
        len: u64,
        initial_local_segment_guard_handle: Option<u64>,
        require_fast_path: bool,
    },
    TrySendWireDirect {
        peer_gen: ClosedRuntimePeerGen,
        peer_transfer_backend_epoch: u64,
        wire_bytes: Vec<u8>,
    },
    DrainInboundFastPathMessages,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum ClosedRuntimeTransferEngineResponse {
    Unit,
    BoolValue(bool),
    RuntimeConfigValue(ClientTransferEngineRuntimeConfig),
    TransferBreakdownValue(TransferBreakdown),
    InboundFastPathMessagesValue(Vec<TransferRpcFastPathInbound>),
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum ClosedRuntimeRequest {
    ConstructClusterManager {
        arg: ClusterManagerNewArg,
    },
    ConstructP2pModule {
        cluster_manager: ClosedRuntimeHandle,
        arg: P2pModuleNewArg,
    },
    ConstructClientTransferEngineCore {
        cluster_manager: ClosedRuntimeHandle,
        p2p_module: ClosedRuntimeHandle,
        arg: ClientTransferEngineNewArg,
    },
    ClusterManagerCall {
        handle: ClosedRuntimeHandle,
        call: ClosedRuntimeClusterManagerCall,
    },
    P2pModuleCall {
        handle: ClosedRuntimeHandle,
        call: ClosedRuntimeP2pCall,
    },
    TransferEngineCall {
        handle: ClosedRuntimeHandle,
        call: ClosedRuntimeTransferEngineCall,
    },
    ClusterEventStreamRecv {
        handle: ClosedRuntimeHandle,
    },
    ClusterRdmaResolvedConfigStreamRecv {
        handle: ClosedRuntimeHandle,
    },
    DropHandle {
        handle: ClosedRuntimeHandle,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum ClosedRuntimeResponse {
    Constructed {
        handle: ClosedRuntimeHandle,
    },
    ClusterManager {
        response: ClosedRuntimeClusterManagerResponse,
    },
    P2p {
        response: ClosedRuntimeP2pResponse,
    },
    TransferEngine {
        response: ClosedRuntimeTransferEngineResponse,
    },
    ClusterEventStreamItem {
        item: ClosedRuntimeClusterEventStreamItem,
    },
    ClusterRdmaResolvedConfigStreamItem {
        item: ClosedRuntimeClusterRdmaResolvedConfigStreamItem,
    },
    Dropped,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum ClosedRuntimeError {
    Cluster {
        detail: String,
    },
    P2p {
        detail: String,
    },
    TransferEngine {
        detail: String,
    },
    InvalidHandle {
        kind: ClosedRuntimeHandleKind,
        raw: u64,
    },
    HandleKindMismatch {
        expected: ClosedRuntimeHandleKind,
        actual: ClosedRuntimeHandleKind,
        raw: u64,
    },
    RuntimeUnavailable {
        detail: String,
    },
    DecodeRequest {
        detail: String,
    },
    InvalidResponse {
        detail: String,
    },
    Internal {
        detail: String,
    },
}
