use crate::p2p::{MsgPackSerializePart, PeerGen, RPCReq};
use crate::{
    ClusterEvent, ClusterMember, MemberRdmaTransferEngineRuntime, NodeID, NodeIDString,
    TransferLinkRecord, TransferReadyInfo,
};
use async_trait::async_trait;
use bitcode::{Decode, Encode};
use bytes::Bytes;
use crossbeam::queue::SegQueue;
use fluxon_framework_compiled::shutdown::ShutdownWaiter;
use limit_thirdparty::tokio::sync::abroadcast;
use std::fs::File;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use thiserror::Error;
use tokio::sync::Notify;

use crate::ClosedRuntimeHandle;

// Keep stable transfer-engine contract types in a dedicated open-surface module so the root
// facade can stop being the long-term implementation authority.

#[derive(
    Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq, Encode, Decode,
)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolType {
    Tcp,
    Rdma,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum TransferEngineType {
    Closed,
    P2p,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum TransferBackendActivationMode {
    RdmaControl,
    TcpTestBypassRdmaControl,
    TestForceEnableBypassRdmaControl,
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct ClientTransferEngineNewArg {
    pub metadata_uri: String,
    /// Number of transient unary etcd RPC retries after the initial attempt.
    pub etcd_rpc_max_retries: u32,
    pub instance_name: String,
    pub transfer_engine: TransferEngineType,
    pub enable_transfer_rpc_fast_path: bool,
    pub rpc_port: u64,
    pub protocol_type: ProtocolType,
    pub rdma_device_names: Option<String>,
    pub backend_activation_mode: TransferBackendActivationMode,
}

#[derive(Clone, Debug, PartialEq, Eq, Encode, Decode)]
pub struct ClientTransferEngineRuntimeConfig {
    pub transfer_engine: TransferEngineType,
    pub enable_transfer_rpc_fast_path: bool,
    pub protocol_type: ProtocolType,
    pub rdma_device_names: Option<String>,
}

#[derive(Debug, Error)]
pub enum TransferEngineError {
    #[error("Open peer segment failed: peer_node={peer_node:?}, detail={detail}")]
    OpenPeerSegmentFailed {
        peer_node: Option<NodeIDString>,
        detail: String,
    },
    #[error("Allocate batch id failed: peer_node={peer_node:?}, detail={detail}")]
    AllocateBatchIdFailed {
        peer_node: Option<NodeIDString>,
        detail: String,
    },
    #[error("Submit transfer failed: peer_node={peer_node:?}, detail={detail}")]
    SubmitTransferFailed {
        peer_node: Option<NodeIDString>,
        detail: String,
    },
    #[error(
        "Get transfer status failed: peer_node={peer_node:?}, task_id={task_id}, detail={detail}"
    )]
    GetTransferStatusFailed {
        peer_node: Option<NodeIDString>,
        task_id: u64,
        detail: String,
    },
    #[error("Free batch id failed: peer_node={peer_node:?}, detail={detail}")]
    FreeBatchIdFailed {
        peer_node: Option<NodeIDString>,
        detail: String,
    },
    #[error("Transfer failed for block: peer_node={peer_node:?}, task_id={task_id}")]
    TransferFailedForBlock {
        peer_node: Option<NodeIDString>,
        task_id: u64,
    },
    #[error("Register local segment failed: detail={detail}")]
    RegisterLocalSegmentFailed { detail: String },
    #[error("Unregister local segment failed: detail={detail}")]
    UnregisterLocalSegmentFailed { detail: String },
    /// Only explicit fast-path requirements use this error; optional attempts return `Ok(false)`.
    #[error("Required direct fast path is not ready")]
    RequiredDirectFastPathNotReady,
    #[error("Create transfer engine failed: detail={detail}")]
    CreateEngineFailed { detail: String },
    #[error("Transfer backend restarting: detail={detail}")]
    BackendRestarting { detail: String },
    #[error("Transfer backend stopped: detail={detail}")]
    BackendStopped { detail: String },
    #[error("Transfer backend fatal: detail={detail}")]
    BackendFatal { detail: String },
}

pub type TransferEngineResult<T> = Result<T, TransferEngineError>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode)]
pub struct TransferBreakdown {
    pub used_fast_path: bool,
    pub local_noop: bool,
    pub remote_transfer: bool,
    pub submit_blocking_us: i64,
    pub create_xfer_req_us: i64,
    pub post_xfer_req_us: i64,
    pub poll_wait_us: i64,
    pub poll_iters: i64,
}

#[derive(Debug, Default)]
pub struct TransferBreakdownShared {
    pub create_xfer_req_us: AtomicI64,
    pub post_xfer_req_us: AtomicI64,
    pub poll_iters: AtomicI64,
}

pub type TransferBreakdownHandle = Arc<TransferBreakdownShared>;

pub const RAW_MEM_READ_REQ_MSG_ID: u32 = 4101;
pub const RAW_MEM_WRITE_REQ_MSG_ID: u32 = 4103;
pub const RAW_MEM_READ_RESP_MSG_ID: u32 = 4102;
pub const RAW_MEM_WRITE_RESP_MSG_ID: u32 = 4104;

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct RawMemReadReq {
    pub src_addr: u64,
    pub len: u64,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct RawMemWriteReq {
    pub target_addr: u64,
    pub len: u64,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct RawMemReadRespWire {
    pub error_code: u32,
    pub error_json: String,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct RawMemWriteRespWire {
    pub error_code: u32,
    pub error_json: String,
}

impl MsgPackSerializePart for RawMemReadReq {
    fn msg_id(&self) -> u32 {
        RAW_MEM_READ_REQ_MSG_ID
    }
}

impl RPCReq for RawMemReadReq {
    type Resp = RawMemReadRespWire;
}

impl MsgPackSerializePart for RawMemWriteReq {
    fn msg_id(&self) -> u32 {
        RAW_MEM_WRITE_REQ_MSG_ID
    }
}

impl RPCReq for RawMemWriteReq {
    type Resp = RawMemWriteRespWire;
}

impl MsgPackSerializePart for RawMemReadRespWire {
    fn msg_id(&self) -> u32 {
        RAW_MEM_READ_RESP_MSG_ID
    }
}

impl MsgPackSerializePart for RawMemWriteRespWire {
    fn msg_id(&self) -> u32 {
        RAW_MEM_WRITE_RESP_MSG_ID
    }
}

pub type P2pRawMemReadFuture = Pin<Box<dyn Future<Output = Result<Bytes, String>> + Send>>;
pub type P2pRawMemWriteFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;

pub type P2pRawMemReadHandler = Arc<dyn Fn(RawMemReadReq) -> P2pRawMemReadFuture + Send + Sync>;
pub type P2pRawMemWriteHandler =
    Arc<dyn Fn(RawMemWriteReq, Bytes) -> P2pRawMemWriteFuture + Send + Sync>;

// This trait represents the cluster/control-plane capabilities that the private transfer-engine
// core needs from its runtime host. Today it is still implemented by an open-tree bootstrap
// adapter, but the long-term owner is the producer-internal closed-sdk runtime context.
#[doc(hidden)]
#[async_trait]
pub trait ClientTransferEngineClusterRuntime: Send + Sync {
    fn cluster_name(&self) -> &str;

    fn self_member_id(&self) -> &str;

    fn get_self_info(&self) -> ClusterMember;

    fn get_member_info_cached(&self, member_id: &str) -> Option<ClusterMember>;

    fn listen(&self) -> abroadcast::Receiver<ClusterEvent>;

    fn set_self_rdma_transfer_engine_runtime(&self, runtime: MemberRdmaTransferEngineRuntime);

    async fn wait_accessible_self_ip_for_current_start_time(&self) -> Result<String, String>;

    async fn fetch_transfer_ready_for_member(
        &self,
        member_id: &str,
    ) -> Result<Option<TransferReadyInfo>, String>;

    async fn publish_self_transfer_ready(
        &self,
        backend_epoch: u64,
    ) -> Result<TransferReadyInfo, String>;

    async fn set_self_transfer_backend_epoch(&self, backend_epoch: u64) -> Result<(), String>;

    async fn clear_self_transfer_backend_epoch(&self) -> Result<(), String>;

    fn try_report_transfer_link_te(
        &self,
        from: NodeIDString,
        to: NodeIDString,
        record: TransferLinkRecord,
    ) -> Result<(), String>;
}

#[doc(hidden)]
#[async_trait]
pub trait ClientTransferEngineSystemRuntime: Clone + Send + Sync + 'static {
    fn cluster_runtime(&self) -> &dyn ClientTransferEngineClusterRuntime;

    fn spawn<F, N>(&self, name: N, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
        N: Into<String>;

    fn register_shutdown_waiter(&self) -> ShutdownWaiter;

    async fn attach_transfer_engine(
        &self,
        transfer_engine: AttachedTransferEngine,
    ) -> Result<(), String>;

    fn notify_transfer_rpc_backend_ready(&self);

    fn notify_transfer_rpc_backend_lost(&self, detail: String);

    fn notify_transfer_rpc_peer_ready(&self, peer_gen: PeerGen, peer_transfer_backend_epoch: u64);

    async fn closed_sdk_runtime_handles(
        &self,
    ) -> Result<(ClosedRuntimeHandle, ClosedRuntimeHandle), String> {
        Err("closed sdk runtime handles are unavailable from this system runtime".to_string())
    }
}

#[doc(hidden)]
#[async_trait]
pub trait ClientTransferEngineOpenRuntime: Clone + Send + Sync + 'static {
    type LocalSegmentGuard: Send + Sync + 'static;

    fn supports_local_segment_transfer(&self) -> bool;

    async fn ensure_local_segment_guard(
        &self,
        local_addr: u64,
        seg_guard: Option<Self::LocalSegmentGuard>,
    ) -> Result<Self::LocalSegmentGuard, String>;

    fn register_p2p_transfer_rpc(&self);

    async fn p2p_read_to_local(
        &self,
        peer: NodeIDString,
        remote_src: u64,
        local_target: u64,
        len: u64,
        seg_guard: Self::LocalSegmentGuard,
    ) -> Result<(), String>;

    async fn p2p_write_from_local(
        &self,
        peer: NodeIDString,
        local_src: u64,
        remote_target: u64,
        len: u64,
        copy_from: Option<Pin<&[u8]>>,
        seg_guard: Self::LocalSegmentGuard,
    ) -> Result<(), String>;

    fn try_record_local_ipc_bytes_for_owner_topology(
        &self,
        logical_peer: &NodeID,
        direction: &'static str,
        bytes: u64,
    ) -> bool;

    fn record_peer_network_bytes(&self, logical_peer: &NodeID, direction: &'static str, bytes: u64);
}

#[doc(hidden)]
#[derive(Clone)]
pub struct ClientTransferEngineComposedRuntime<S, O> {
    system: S,
    open: O,
}

impl<S, O> ClientTransferEngineComposedRuntime<S, O> {
    pub fn new(system: S, open: O) -> Self {
        Self { system, open }
    }

    pub fn system(&self) -> &S {
        &self.system
    }

    pub fn open(&self) -> &O {
        &self.open
    }
}

#[doc(hidden)]
#[async_trait]
pub trait ClientTransferEngineRuntime: Clone + Send + Sync + 'static {
    type LocalSegmentGuard: Send + Sync + 'static;

    fn supports_local_segment_transfer(&self) -> bool;

    fn cluster_runtime(&self) -> &dyn ClientTransferEngineClusterRuntime;

    fn spawn<F, N>(&self, name: N, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
        N: Into<String>;

    fn register_shutdown_waiter(&self) -> ShutdownWaiter;

    async fn ensure_local_segment_guard(
        &self,
        local_addr: u64,
        seg_guard: Option<Self::LocalSegmentGuard>,
    ) -> Result<Self::LocalSegmentGuard, String>;

    fn register_p2p_transfer_rpc(&self);

    async fn attach_transfer_engine(
        &self,
        transfer_engine: AttachedTransferEngine,
    ) -> Result<(), String>;

    fn notify_transfer_rpc_backend_ready(&self);

    fn notify_transfer_rpc_backend_lost(&self, detail: String);

    fn notify_transfer_rpc_peer_ready(&self, peer_gen: PeerGen, peer_transfer_backend_epoch: u64);

    async fn p2p_read_to_local(
        &self,
        peer: NodeIDString,
        remote_src: u64,
        local_target: u64,
        len: u64,
        seg_guard: Self::LocalSegmentGuard,
    ) -> Result<(), String>;

    async fn p2p_write_from_local(
        &self,
        peer: NodeIDString,
        local_src: u64,
        remote_target: u64,
        len: u64,
        copy_from: Option<Pin<&[u8]>>,
        seg_guard: Self::LocalSegmentGuard,
    ) -> Result<(), String>;

    fn try_record_local_ipc_bytes_for_owner_topology(
        &self,
        logical_peer: &NodeID,
        direction: &'static str,
        bytes: u64,
    ) -> bool;

    fn record_peer_network_bytes(&self, logical_peer: &NodeID, direction: &'static str, bytes: u64);

    async fn closed_sdk_runtime_handles(
        &self,
    ) -> Result<(ClosedRuntimeHandle, ClosedRuntimeHandle), String> {
        Err("closed sdk runtime handles are unavailable from this runtime".to_string())
    }
}

#[async_trait]
impl<S, O> ClientTransferEngineRuntime for ClientTransferEngineComposedRuntime<S, O>
where
    S: ClientTransferEngineSystemRuntime,
    O: ClientTransferEngineOpenRuntime,
{
    type LocalSegmentGuard = O::LocalSegmentGuard;

    fn supports_local_segment_transfer(&self) -> bool {
        self.open.supports_local_segment_transfer()
    }

    fn cluster_runtime(&self) -> &dyn ClientTransferEngineClusterRuntime {
        self.system.cluster_runtime()
    }

    fn spawn<F, N>(&self, name: N, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
        N: Into<String>,
    {
        self.system.spawn(name, fut);
    }

    fn register_shutdown_waiter(&self) -> ShutdownWaiter {
        self.system.register_shutdown_waiter()
    }

    async fn ensure_local_segment_guard(
        &self,
        local_addr: u64,
        seg_guard: Option<Self::LocalSegmentGuard>,
    ) -> Result<Self::LocalSegmentGuard, String> {
        self.open
            .ensure_local_segment_guard(local_addr, seg_guard)
            .await
    }

    fn register_p2p_transfer_rpc(&self) {
        self.open.register_p2p_transfer_rpc();
    }

    async fn attach_transfer_engine(
        &self,
        transfer_engine: AttachedTransferEngine,
    ) -> Result<(), String> {
        self.system.attach_transfer_engine(transfer_engine).await
    }

    fn notify_transfer_rpc_backend_ready(&self) {
        self.system.notify_transfer_rpc_backend_ready();
    }

    fn notify_transfer_rpc_backend_lost(&self, detail: String) {
        self.system.notify_transfer_rpc_backend_lost(detail);
    }

    fn notify_transfer_rpc_peer_ready(&self, peer_gen: PeerGen, peer_transfer_backend_epoch: u64) {
        self.system
            .notify_transfer_rpc_peer_ready(peer_gen, peer_transfer_backend_epoch);
    }

    async fn p2p_read_to_local(
        &self,
        peer: NodeIDString,
        remote_src: u64,
        local_target: u64,
        len: u64,
        seg_guard: Self::LocalSegmentGuard,
    ) -> Result<(), String> {
        self.open
            .p2p_read_to_local(peer, remote_src, local_target, len, seg_guard)
            .await
    }

    async fn p2p_write_from_local(
        &self,
        peer: NodeIDString,
        local_src: u64,
        remote_target: u64,
        len: u64,
        copy_from: Option<Pin<&[u8]>>,
        seg_guard: Self::LocalSegmentGuard,
    ) -> Result<(), String> {
        self.open
            .p2p_write_from_local(peer, local_src, remote_target, len, copy_from, seg_guard)
            .await
    }

    fn try_record_local_ipc_bytes_for_owner_topology(
        &self,
        logical_peer: &NodeID,
        direction: &'static str,
        bytes: u64,
    ) -> bool {
        self.open
            .try_record_local_ipc_bytes_for_owner_topology(logical_peer, direction, bytes)
    }

    fn record_peer_network_bytes(
        &self,
        logical_peer: &NodeID,
        direction: &'static str,
        bytes: u64,
    ) {
        self.open
            .record_peer_network_bytes(logical_peer, direction, bytes);
    }

    async fn closed_sdk_runtime_handles(
        &self,
    ) -> Result<(ClosedRuntimeHandle, ClosedRuntimeHandle), String> {
        self.system.closed_sdk_runtime_handles().await
    }
}

impl TransferEngineError {
    pub fn should_restart_backend(&self) -> bool {
        matches!(
            self,
            Self::BackendStopped { .. } | Self::BackendFatal { .. }
        )
    }
}

#[derive(Debug)]
pub struct CpuAllocatedMem {
    pub _file: File,
    pub allocated_addr: u64,
    pub allocated_size: u64,
}

// SAFETY: CpuAllocatedMem contains a mapped virtual address owned by the enclosing module.
// The lifetime is externally synchronized and the address itself is immutable after construction.
unsafe impl Send for CpuAllocatedMem {}
unsafe impl Sync for CpuAllocatedMem {}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct TransferRpcFastPathInbound {
    pub peer: NodeIDString,
    pub wire_bytes: Vec<u8>,
    pub local_observe: crate::p2p::WireTransportLocalObserve,
}

impl TransferRpcFastPathInbound {
    pub fn new(peer: NodeIDString, wire_bytes: Vec<u8>) -> Self {
        let frame_recv_done_ts_us = crate::p2p::rpc::current_cross_process_monotonic_us();
        Self {
            peer,
            wire_bytes,
            local_observe: crate::p2p::WireTransportLocalObserve {
                frame_recv_done_ts_us,
                dispatch_enqueued_ts_us: frame_recv_done_ts_us,
                ..crate::p2p::WireTransportLocalObserve::default()
            },
        }
    }
}

#[doc(hidden)]
#[derive(Clone)]
pub struct TransferRpcFastPathInboundDispatch {
    queue: Arc<SegQueue<TransferRpcFastPathInbound>>,
    notify: Arc<Notify>,
}

impl TransferRpcFastPathInboundDispatch {
    pub fn new() -> Self {
        Self {
            queue: Arc::new(SegQueue::new()),
            notify: Arc::new(Notify::new()),
        }
    }

    #[inline]
    pub fn push(&self, inbound: TransferRpcFastPathInbound) {
        self.queue.push(inbound);
        // Keep the producer path to "publish + wake" only. The async dispatch task owns draining
        // and backpressure decisions; the busypoller thread must not inherit channel sync semantics.
        self.notify.notify_one();
    }

    #[inline]
    pub fn try_pop(&self) -> Option<TransferRpcFastPathInbound> {
        self.queue.pop()
    }

    #[inline]
    pub async fn wait(&self) {
        self.notify.notified().await;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DesiredTransferPeer {
    pub peer_gen: PeerGen,
    pub peer_transfer_backend_epoch: u64,
    pub enable_transfer_rpc: bool,
    pub enable_transfer_segment: bool,
}

#[doc(hidden)]
#[async_trait]
pub trait TransferRpcFastPath: Send + Sync {
    /// Returns `Ok(false)` when the direct path is unavailable so transport fallback can proceed.
    async fn try_send_wire_direct(
        &self,
        peer_gen: &PeerGen,
        peer_transfer_backend_epoch: u64,
        wire_bytes: Vec<u8>,
    ) -> TransferEngineResult<bool>;
}

#[doc(hidden)]
#[async_trait]
pub trait TransferEngineControl: Send + Sync {
    fn rpc_fast_path(&self) -> Option<Arc<dyn TransferRpcFastPath>>;
}

#[doc(hidden)]
#[async_trait]
pub trait TransferEngineBridge: Send + Sync {
    fn backend_activation_requires_rdma_devices(&self) -> bool;

    async fn ensure_started_if_needed(&self) -> TransferEngineResult<()>;

    async fn update_enabled_rdma_devices(
        &self,
        enabled_devices: Vec<String>,
    ) -> TransferEngineResult<()>;

    async fn sync_desired_peers(&self, desired_peers: Vec<DesiredTransferPeer>);

    fn attach_rpc_fast_path_inbound_dispatch(
        &self,
        dispatch: TransferRpcFastPathInboundDispatch,
    ) -> TransferEngineResult<()>;
}

#[doc(hidden)]
#[derive(Clone)]
pub struct AttachedTransferEngine {
    control: Arc<dyn TransferEngineControl>,
    bridge: Arc<dyn TransferEngineBridge>,
}

impl AttachedTransferEngine {
    pub fn new(
        control: Arc<dyn TransferEngineControl>,
        bridge: Arc<dyn TransferEngineBridge>,
    ) -> Self {
        Self { control, bridge }
    }

    pub fn rpc_fast_path(&self) -> Option<Arc<dyn TransferRpcFastPath>> {
        self.control.rpc_fast_path()
    }

    pub fn backend_activation_requires_rdma_devices(&self) -> bool {
        self.bridge.backend_activation_requires_rdma_devices()
    }

    pub async fn ensure_started_if_needed(&self) -> TransferEngineResult<()> {
        self.bridge.ensure_started_if_needed().await
    }

    pub async fn update_enabled_rdma_devices(
        &self,
        enabled_devices: Vec<String>,
    ) -> TransferEngineResult<()> {
        self.bridge
            .update_enabled_rdma_devices(enabled_devices)
            .await
    }

    pub async fn sync_desired_peers(&self, desired_peers: Vec<DesiredTransferPeer>) {
        self.bridge.sync_desired_peers(desired_peers).await;
    }

    pub fn attach_rpc_fast_path_inbound_dispatch(
        &self,
        dispatch: TransferRpcFastPathInboundDispatch,
    ) -> TransferEngineResult<()> {
        self.bridge.attach_rpc_fast_path_inbound_dispatch(dispatch)
    }
}
