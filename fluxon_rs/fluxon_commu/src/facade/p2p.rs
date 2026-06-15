use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};

use std::time::Duration;

use crate::NodeID;
use crate::cluster_manager::ClusterManagerAccessTrait;
use crate::transfer::{P2pTransportKind, TransferLinkP2pState};
use crate::transfer_engine::AttachedTransferEngine;
use async_trait::async_trait;
pub use fluxon_commu_contract::p2p::surface::{
    P2pModuleNewArg, P2pTcpThreadTransportTuning, RpcTransportPolicy, TierSnapshot, UserRpcReq,
    UserRpcResp,
};
use fluxon_commu_contract::p2p::surface::{
    P2pModuleNewArg as CommuP2pModuleNewArg, RpcTransportPolicy as CommuRpcTransportPolicy,
    TierSnapshot as CommuTierSnapshot,
};
use fluxon_commu_contract::{
    ClosedRuntimeHandle, ClosedRuntimeP2pCall, ClosedRuntimeP2pResponse, ClosedRuntimeTierSnapshot,
};
use fluxon_framework::LogicalModule;
use fluxon_framework::{ResourceRegistry, ResourceRegistryAccessTrait};
use fluxon_framework_compiled::async_panic::AsyncPanicSendExt;
use fluxon_framework_compiled::shutdown::{ShutdownPoller, ShutdownWaiter, ViewShutdownExt};
use fluxon_framework_compiled::spawn::ViewSpawnExt;
use fluxon_framework_compiled::upgrade_view_guard::UpgradeViewGuard;
use fluxon_framework_compiled::util::ViewSpawnHandle;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;

use crate::closed_sdk::{
    construct_p2p_module_handle, drop_runtime_handle, is_live_dependent_drop_error,
    p2p_call_raw_observed, p2p_register_dispatch, p2p_register_rpc_response_msg_id,
    p2p_register_user_rpc_bytes_handler, p2p_register_user_rpc_bytes_handler_async,
    p2p_send_response_raw, spawn_deferred_drop_runtime_handle,
};
use fluxon_commu_closed_sdk_consumer::ClosedRuntimeDispatchRequestRef;
use fluxon_commu_closed_sdk_consumer::p2p_module_call;

#[doc(hidden)]
pub trait P2pModuleAccessTrait: Send + Sync {
    fn p2p_module(&self) -> &P2pModule;
}

#[doc(hidden)]
pub mod __hidden {
    use super::*;

    #[doc(hidden)]
    pub trait P2pModuleViewTrait:
        Send
        + Sync
        + P2pModuleAccessTrait
        + ClusterManagerAccessTrait
        + ResourceRegistryAccessTrait
        + ViewShutdownExt
        + AsyncPanicSendExt
        + ViewSpawnExt
    {
    }

    impl<T> P2pModuleViewTrait for T where
        T: Send
            + Sync
            + P2pModuleAccessTrait
            + ClusterManagerAccessTrait
            + ResourceRegistryAccessTrait
            + ViewShutdownExt
            + AsyncPanicSendExt
            + ViewSpawnExt
    {
    }

    #[doc(hidden)]
    #[derive(Clone)]
    pub struct P2pModuleView {
        view: Weak<dyn P2pModuleViewTrait>,
    }

    impl P2pModuleView {
        pub fn new(view: &Arc<dyn P2pModuleViewTrait>) -> Self {
            Self {
                view: Arc::downgrade(view),
            }
        }

        pub fn try_upgrade(&self) -> Option<UpgradeViewGuard<dyn P2pModuleViewTrait>> {
            self.view.upgrade().map(UpgradeViewGuard::new)
        }

        pub(crate) fn upgrade_arc(&self) -> Option<Arc<dyn P2pModuleViewTrait>> {
            self.view.upgrade()
        }

        pub fn resource_registry(&self) -> &ResourceRegistry {
            let arc_view = self.view.upgrade().expect(
                "view of module P2pModule has been dropped when accessing resource registry",
            );
            unsafe {
                let ptr =
                    std::ptr::NonNull::new(Arc::as_ptr(&arc_view) as *const _ as *mut _).unwrap();
                let view_ref: &dyn P2pModuleViewTrait = ptr.as_ref();
                let reg_ptr =
                    std::ptr::NonNull::new(view_ref.resource_registry() as *const _ as *mut _)
                        .unwrap();
                reg_ptr.as_ref()
            }
        }

        pub fn p2p_module(&self) -> &P2pModule {
            let arc_view = self.view.upgrade().expect(
                "view of module P2pModule has been dropped when accessing dependency P2pModule",
            );
            unsafe {
                let ptr =
                    std::ptr::NonNull::new(Arc::as_ptr(&arc_view) as *const _ as *mut _).unwrap();
                let view_ref: &dyn P2pModuleViewTrait = ptr.as_ref();
                let module_ptr =
                    std::ptr::NonNull::new(view_ref.p2p_module() as *const _ as *mut _).unwrap();
                module_ptr.as_ref()
            }
        }

        pub fn cluster_manager(&self) -> &crate::cluster_manager::ClusterManager {
            let arc_view = self.view.upgrade().expect(
            "view of module P2pModule has been dropped when accessing dependency ClusterManager",
        );
            unsafe {
                let ptr =
                    std::ptr::NonNull::new(Arc::as_ptr(&arc_view) as *const _ as *mut _).unwrap();
                let view_ref: &dyn P2pModuleViewTrait = ptr.as_ref();
                let module_ptr =
                    std::ptr::NonNull::new(view_ref.cluster_manager() as *const _ as *mut _)
                        .unwrap();
                module_ptr.as_ref()
            }
        }

        pub fn register_shutdown_poller(&self) -> ShutdownPoller {
            self.view
                .upgrade()
                .expect("view of module P2pModule has been dropped before register_shutdown_poller")
                .register_shutdown_poller()
        }

        pub fn register_shutdown_waiter(&self) -> ShutdownWaiter {
            self.view
                .upgrade()
                .expect("view of module P2pModule has been dropped before register_shutdown_waiter")
                .register_shutdown_waiter()
        }

        pub fn async_panic(&self, msg: String) {
            self.view
                .upgrade()
                .expect("view of module P2pModule has been dropped before async_panic")
                .async_panic(msg);
        }

        pub fn spawn<F, N>(&self, name: N, fut: F) -> ViewSpawnHandle<dyn P2pModuleViewTrait>
        where
            F: std::future::Future<Output = ()> + Send + 'static,
            N: Into<String>,
        {
            let view_ref = self
                .view
                .upgrade()
                .expect("view of module P2pModule has been dropped before spawn");
            let boxed: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
                Box::pin(fut);
            let handle = ViewSpawnExt::spawn_boxed(&*view_ref, boxed);
            ViewSpawnHandle::new(name, handle, view_ref)
        }
    }

    impl P2pModuleAccessTrait for P2pModuleView {
        fn p2p_module(&self) -> &P2pModule {
            P2pModuleView::p2p_module(self)
        }
    }

    impl ClusterManagerAccessTrait for P2pModuleView {
        fn cluster_manager(&self) -> &crate::cluster_manager::ClusterManager {
            P2pModuleView::cluster_manager(self)
        }
    }

    impl ResourceRegistryAccessTrait for P2pModuleView {
        fn resource_registry(&self) -> &ResourceRegistry {
            P2pModuleView::resource_registry(self)
        }
    }

    impl ViewShutdownExt for P2pModuleView {
        fn register_shutdown_waiter(&self) -> ShutdownWaiter {
            P2pModuleView::register_shutdown_waiter(self)
        }

        fn register_shutdown_poller(&self) -> ShutdownPoller {
            P2pModuleView::register_shutdown_poller(self)
        }
    }

    impl AsyncPanicSendExt for P2pModuleView {
        fn async_panic(&self, msg: String) {
            P2pModuleView::async_panic(self, msg);
        }
    }

    impl ViewSpawnExt for P2pModuleView {
        fn push_join_handle(&self, name: String, handle: JoinHandle<()>) {
            if let Some(view) = self.upgrade_arc() {
                view.push_join_handle(name, handle);
            } else {
                handle.abort();
            }
        }

        fn runtime_num_workers(&self) -> usize {
            self.upgrade_arc()
                .map(|view| view.runtime_num_workers())
                .unwrap_or(1)
        }

        fn spawn_boxed(
            &self,
            fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
        ) -> JoinHandle<()> {
            if let Some(view) = self.upgrade_arc() {
                view.spawn_boxed(fut)
            } else {
                tokio::spawn(async {})
            }
        }
    }
}

pub use self::__hidden::{P2pModuleView, P2pModuleViewTrait};

struct ClosedP2pRuntime {
    arg: CommuP2pModuleNewArg,
    handle: OnceLock<ClosedRuntimeHandle>,
    construct_lock: AsyncMutex<()>,
    view: OnceLock<P2pModuleView>,
    tier_snapshot: Arc<RwLock<Arc<CommuTierSnapshot>>>,
    scheduled_rpc_response_msg_ids: Mutex<HashSet<MsgId>>,
    runtime_registered_rpc_response_msg_ids: Mutex<HashSet<MsgId>>,
    snapshot_poller_started: std::sync::atomic::AtomicBool,
}

impl ClosedP2pRuntime {
    fn new(arg: CommuP2pModuleNewArg) -> Self {
        Self {
            tier_snapshot: Arc::new(RwLock::new(Arc::new(default_tier_snapshot(
                arg.disable_crossowner_ipc,
            )))),
            scheduled_rpc_response_msg_ids: Mutex::new(HashSet::new()),
            runtime_registered_rpc_response_msg_ids: Mutex::new(HashSet::new()),
            arg,
            handle: OnceLock::new(),
            construct_lock: AsyncMutex::new(()),
            view: OnceLock::new(),
            snapshot_poller_started: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

fn default_tier_snapshot(disable_crossowner_ipc: bool) -> CommuTierSnapshot {
    CommuTierSnapshot {
        self_peer_gen: crate::p2p::PeerGen {
            peer_id: "".to_string().into(),
            node_start_time: 0,
        },
        disable_crossowner_ipc,
        peers: Default::default(),
        relay_set_snapshot: Default::default(),
        relay_caps_by_hop: Default::default(),
        direct_graph: Default::default(),
    }
}

fn p2p_closed_sdk_error(error: crate::closed_sdk::ClosedSdkConsumerError) -> crate::p2p::P2pError {
    crate::p2p::P2pError::Other {
        detail: format!("closed sdk p2p call failed: {error}"),
    }
}

async fn closed_p2p_unit_call(
    handle: ClosedRuntimeHandle,
    call: ClosedRuntimeP2pCall,
) -> crate::p2p::P2PResult<()> {
    match p2p_module_call(handle, call)
        .await
        .map_err(p2p_closed_sdk_error)?
    {
        ClosedRuntimeP2pResponse::Unit => Ok(()),
        other => Err(crate::p2p::P2pError::Other {
            detail: format!("unexpected closed sdk p2p unit response: {other:?}"),
        }),
    }
}

async fn closed_p2p_tier_snapshot_call(
    handle: ClosedRuntimeHandle,
) -> crate::p2p::P2PResult<CommuTierSnapshot> {
    match p2p_module_call(handle, ClosedRuntimeP2pCall::TierSnapshot)
        .await
        .map_err(p2p_closed_sdk_error)?
    {
        ClosedRuntimeP2pResponse::TierSnapshotValue(snapshot) => {
            Ok(closed_tier_snapshot_into_open(snapshot))
        }
        other => Err(crate::p2p::P2pError::Other {
            detail: format!("unexpected closed sdk p2p tier_snapshot response: {other:?}"),
        }),
    }
}

fn closed_tier_snapshot_into_open(snapshot: ClosedRuntimeTierSnapshot) -> CommuTierSnapshot {
    CommuTierSnapshot {
        self_peer_gen: snapshot.self_peer_gen.into(),
        disable_crossowner_ipc: snapshot.disable_crossowner_ipc,
        peers: snapshot
            .peers
            .into_iter()
            .map(|(peer_id, peer_view)| (peer_id.into(), peer_view.into()))
            .collect(),
        relay_set_snapshot: snapshot
            .relay_set_snapshot
            .into_iter()
            .map(Into::into)
            .collect(),
        relay_caps_by_hop: snapshot
            .relay_caps_by_hop
            .into_iter()
            .map(|(hop, caps)| (hop.into(), caps.into()))
            .collect(),
        direct_graph: snapshot
            .direct_graph
            .into_iter()
            .map(|(from, targets)| (from.into(), targets.into_iter().map(Into::into).collect()))
            .collect(),
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RpcCallTransportObserveTrace {
    pub(crate) caller_submit_us: i64,
    pub(crate) caller_submit_ts_us: i64,
    pub(crate) request_path_kind: crate::p2p::UserRpcTransportPathKind,
}

#[derive(Debug)]
pub(crate) struct RpcCallRawObservedOutput {
    pub(crate) message: crate::p2p::WireIncomingMessage,
    pub(crate) observe: RpcCallTransportObserveTrace,
}

pub struct P2pModule {
    closed: ClosedP2pRuntime,
}

impl P2pModule {
    #[doc(hidden)]
    pub fn closed_runtime_handle(&self) -> Option<ClosedRuntimeHandle> {
        self.closed.handle.get().copied()
    }

    #[doc(hidden)]
    pub async fn ensure_closed_runtime_handle(&self) -> P2PResult<ClosedRuntimeHandle> {
        if let Some(handle) = self.closed_runtime_handle() {
            return Ok(handle);
        }
        let _guard = self.closed.construct_lock.lock().await;
        if let Some(handle) = self.closed_runtime_handle() {
            return Ok(handle);
        }
        let view = self
            .closed
            .view
            .get()
            .cloned()
            .ok_or_else(|| P2pError::Other {
                detail: "P2pModule closed-sdk handle requested before attach_view".to_string(),
            })?;
        let cluster_manager = view.cluster_manager();
        let handle = construct_p2p_module_handle(
            cluster_manager.closed_runtime_handle(),
            self.closed.arg.clone(),
        )
        .await
        .map_err(p2p_closed_sdk_error)?;
        let _ = self.closed.handle.set(handle);
        Ok(handle)
    }

    fn start_tier_snapshot_poller_if_needed(&self, handle: ClosedRuntimeHandle) {
        if self
            .closed
            .snapshot_poller_started
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        let Some(view) = self.closed.view.get().cloned() else {
            self.closed
                .snapshot_poller_started
                .store(false, std::sync::atomic::Ordering::Release);
            return;
        };
        let cache = Arc::clone(&self.closed.tier_snapshot);
        tokio::spawn(async move {
            let mut shutdown_waiter = view.register_shutdown_waiter();
            loop {
                tokio::select! {
                    _ = shutdown_waiter.wait() => break,
                    _ = tokio::time::sleep(Duration::from_millis(200)) => {
                        match closed_p2p_tier_snapshot_call(handle).await {
                            Ok(snapshot) => {
                                *cache.write().expect("tier snapshot cache poisoned") = Arc::new(snapshot);
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        });
    }

    async fn refresh_tier_snapshot_cache(&self) -> P2PResult<()> {
        if let Some(handle) = self.closed_runtime_handle() {
            let snapshot = closed_p2p_tier_snapshot_call(handle).await?;
            *self
                .closed
                .tier_snapshot
                .write()
                .expect("tier snapshot cache poisoned") = Arc::new(snapshot);
        }
        Ok(())
    }

    fn cached_tier_snapshot(&self) -> Arc<CommuTierSnapshot> {
        self.closed
            .tier_snapshot
            .read()
            .expect("tier snapshot cache poisoned")
            .clone()
    }

    pub(crate) fn module_view(&self) -> P2pModuleView {
        {
            self.closed
                .view
                .get()
                .cloned()
                .expect("P2pModule view accessed before attach_view")
        }
    }

    pub async fn construct(arg: CommuP2pModuleNewArg) -> P2PResult<Self> {
        {
            Ok(Self {
                closed: ClosedP2pRuntime::new(arg),
            })
        }
    }

    pub(crate) fn attach_view(&self, view: P2pModuleView) {
        {
            self.closed
                .view
                .set(view.clone())
                .unwrap_or_else(|_| panic!("P2pModule open view attached twice"));
            if let Some(handle) = self.closed_runtime_handle() {
                self.start_tier_snapshot_poller_if_needed(handle);
            }
        }
    }

    pub fn try_record_local_ipc_bytes_for_owner_topology(
        &self,
        logical_peer: &NodeID,
        direction: &str,
        bytes: u64,
    ) -> bool {
        {
            if bytes == 0 {
                return true;
            }
            let view = self.module_view();
            let cm = view.cluster_manager();
            let self_info = cm.get_self_info();
            if self_info.node_role() != crate::NodeRole::External {
                return false;
            }
            let snapshot = self.cached_tier_snapshot();
            let Some(peer_gen) = snapshot.peer_gen(logical_peer) else {
                return false;
            };
            if !snapshot.is_send_ready_intra_effective(&peer_gen) {
                return false;
            }
            let Some(owner_id) = self_info
                .metadata
                .get(crate::META_KEY_SHARED_STORAGE_NODE_ID)
            else {
                return false;
            };
            if logical_peer.as_ref() == owner_id.as_str() {
                return false;
            }
            let Some(handle) = cm.ipc_bandwidth_attributor_handle() else {
                return false;
            };
            match direction {
                "tx" => handle.record_rx_bytes(bytes),
                "rx" => handle.record_tx_bytes(bytes),
                _ => return false,
            }
            true
        }
    }

    pub fn tier_snapshot(&self) -> Arc<CommuTierSnapshot> {
        { self.cached_tier_snapshot() }
    }

    pub async fn attach_transfer_engine(
        &self,
        transfer_engine: AttachedTransferEngine,
    ) -> P2PResult<()> {
        {
            let _ = transfer_engine;
            Ok(())
        }
    }

    pub async fn init2_for_init_dag(&self) -> P2PResult<()> {
        {
            let handle = self.ensure_closed_runtime_handle().await?;
            closed_p2p_unit_call(handle, ClosedRuntimeP2pCall::Init2ForInitDag).await?;
            self.refresh_tier_snapshot_cache().await?;
            self.start_tier_snapshot_poller_if_needed(handle);
            Ok(())
        }
    }

    pub async fn init3_for_init_dag(&self) -> P2PResult<()> {
        {
            let handle = self.ensure_closed_runtime_handle().await?;
            closed_p2p_unit_call(handle, ClosedRuntimeP2pCall::Init3ForInitDag).await?;
            self.refresh_tier_snapshot_cache().await?;
            Ok(())
        }
    }

    pub fn peek_p2p_link_state(&self, peer: &NodeID) -> TransferLinkP2pState {
        {
            let snapshot = self.cached_tier_snapshot();
            let Some(peer_gen) = snapshot.peer_gen(peer) else {
                return TransferLinkP2pState::Unknown;
            };
            if snapshot.is_any_send_ready(&peer_gen) {
                return TransferLinkP2pState::Direct;
            }
            if snapshot.relay_caps_by_hop.values().any(|caps| {
                caps.deliverable_targets
                    .iter()
                    .any(|target| target == &peer_gen)
            }) {
                return TransferLinkP2pState::Relay;
            }
            TransferLinkP2pState::Unknown
        }
    }

    pub fn peek_p2p_transport_kind(&self, peer: &NodeID) -> Option<P2pTransportKind> {
        {
            let snapshot = self.cached_tier_snapshot();
            let peer_gen = snapshot.peer_gen(peer)?;
            if snapshot.is_send_ready_intra_effective(&peer_gen) {
                return Some(P2pTransportKind::Ice);
            }
            if snapshot.is_send_ready_direct(&peer_gen) {
                return Some(crate::p2p::network_transport_kind());
            }
            None
        }
    }

    pub fn verify_peer_id(&self, peer_id: &str) -> bool {
        {
            self.module_view()
                .cluster_manager()
                .get_members()
                .iter()
                .any(|member| member.id == peer_id)
        }
    }

    pub fn notify_peer_connected_incoming_ice(&self, peer: NodeID) {
        {
            if let Some(handle) = self.closed_runtime_handle() {
                tokio::spawn(async move {
                    let _ = closed_p2p_unit_call(
                        handle,
                        ClosedRuntimeP2pCall::NotifyPeerConnectedIncomingIce {
                            peer: peer.to_string(),
                        },
                    )
                    .await;
                });
            }
        }
    }

    pub fn notify_peer_disconnected_all(&self, peer: NodeID) {
        {
            if let Some(handle) = self.closed_runtime_handle() {
                tokio::spawn(async move {
                    let _ = closed_p2p_unit_call(
                        handle,
                        ClosedRuntimeP2pCall::NotifyPeerDisconnectedAll {
                            peer: peer.to_string(),
                        },
                    )
                    .await;
                });
            }
        }
    }

    pub fn notify_peer_disconnected_ice(&self, peer: NodeID) {
        {
            if let Some(handle) = self.closed_runtime_handle() {
                tokio::spawn(async move {
                    let _ = closed_p2p_unit_call(
                        handle,
                        ClosedRuntimeP2pCall::NotifyPeerDisconnectedIce {
                            peer: peer.to_string(),
                        },
                    )
                    .await;
                });
            }
        }
    }

    pub fn emit_transfer_rpc_backend_ready_for_runtime(&self) {
        {
            let view = self.module_view();
            tokio::spawn(async move {
                let Ok(handle) = view.p2p_module().ensure_closed_runtime_handle().await else {
                    return;
                };
                let _ = closed_p2p_unit_call(
                    handle,
                    ClosedRuntimeP2pCall::NotifyTransferRpcBackendReady,
                )
                .await;
            });
        }
    }

    pub fn emit_transfer_rpc_backend_lost_for_runtime(&self, detail: String) {
        {
            let view = self.module_view();
            tokio::spawn(async move {
                let Ok(handle) = view.p2p_module().ensure_closed_runtime_handle().await else {
                    return;
                };
                let _ = closed_p2p_unit_call(
                    handle,
                    ClosedRuntimeP2pCall::NotifyTransferRpcBackendLost { detail },
                )
                .await;
            });
        }
    }

    pub fn emit_transfer_rpc_peer_ready_for_runtime(
        &self,
        peer_gen: crate::p2p::PeerGen,
        peer_transfer_backend_epoch: u64,
    ) {
        {
            let view = self.module_view();
            tokio::spawn(async move {
                let Ok(handle) = view.p2p_module().ensure_closed_runtime_handle().await else {
                    return;
                };
                let _ = closed_p2p_unit_call(
                    handle,
                    ClosedRuntimeP2pCall::NotifyTransferRpcPeerReady {
                        peer_gen: peer_gen.into(),
                        peer_transfer_backend_epoch,
                    },
                )
                .await;
            });
        }
    }

    pub async fn ensure_peer_send_ready(&self, logical_target: &NodeID) -> P2PResult<()> {
        {
            let handle = self.ensure_closed_runtime_handle().await?;
            closed_p2p_unit_call(
                handle,
                ClosedRuntimeP2pCall::EnsurePeerSendReady {
                    logical_target: logical_target.to_string(),
                },
            )
            .await
        }
    }

    pub(crate) fn regist_dispatch_raw<F>(&self, msg_id: MsgId, f: F)
    where
        F: Fn(
                &str,
                &P2pModule,
                ClosedRuntimeDispatchRequestRef<'_>,
                &bytes::Bytes,
            ) -> P2PResult<()>
            + Send
            + Sync
            + 'static,
    {
        {
            let view = self.module_view();
            tokio::spawn(async move {
                let handle = match view.p2p_module().ensure_closed_runtime_handle().await {
                    Ok(handle) => handle,
                    Err(_) => return,
                };
                let _ = p2p_register_dispatch(
                    handle,
                    msg_id,
                    Arc::new(
                        move |request: ClosedRuntimeDispatchRequestRef<'_>, body: bytes::Bytes| {
                            f(request.reply_next_hop, view.p2p_module(), request, &body).map(|_| ())
                        },
                    ),
                )
                .await;
            });
        }
    }

    pub(crate) fn register_rpc_response_msg_id(&self, msg_id: MsgId) {
        {
            if self.is_rpc_response_msg_id_registered_runtime(msg_id) {
                return;
            }
            let should_register = {
                let mut scheduled = self
                    .closed
                    .scheduled_rpc_response_msg_ids
                    .lock()
                    .expect("closed rpc response scheduling lock poisoned");
                scheduled.insert(msg_id)
            };
            if !should_register {
                return;
            }
            let view = self.module_view();
            tokio::spawn(async move {
                let Ok(handle) = view.p2p_module().ensure_closed_runtime_handle().await else {
                    let _ = view
                        .p2p_module()
                        .closed
                        .scheduled_rpc_response_msg_ids
                        .lock()
                        .map(|mut scheduled| scheduled.remove(&msg_id));
                    return;
                };
                match p2p_register_rpc_response_msg_id(handle, msg_id).await {
                    Ok(()) => {
                        let _ = view
                            .p2p_module()
                            .closed
                            .runtime_registered_rpc_response_msg_ids
                            .lock()
                            .map(|mut registered| {
                                registered.insert(msg_id);
                            });
                    }
                    Err(_) => {
                        let _ = view
                            .p2p_module()
                            .closed
                            .scheduled_rpc_response_msg_ids
                            .lock()
                            .map(|mut scheduled| scheduled.remove(&msg_id));
                    }
                }
            });
        }
    }

    fn is_rpc_response_msg_id_registered_runtime(&self, msg_id: MsgId) -> bool {
        self.closed
            .runtime_registered_rpc_response_msg_ids
            .lock()
            .expect("closed rpc response runtime registry lock poisoned")
            .contains(&msg_id)
    }

    pub(crate) fn register_rpc_response_completion_raw(&self, msg_id: MsgId) {
        {
            let _ = msg_id;
        }
    }

    pub(crate) async fn ensure_rpc_response_msg_id_registered_runtime(
        &self,
        msg_id: MsgId,
    ) -> P2PResult<()> {
        if self.is_rpc_response_msg_id_registered_runtime(msg_id) {
            return Ok(());
        }
        let handle = self.ensure_closed_runtime_handle().await?;
        p2p_register_rpc_response_msg_id(handle, msg_id)
            .await
            .map_err(p2p_closed_sdk_error)?;
        let _ = self
            .closed
            .scheduled_rpc_response_msg_ids
            .lock()
            .map(|mut scheduled| {
                scheduled.insert(msg_id);
            });
        let _ = self
            .closed
            .runtime_registered_rpc_response_msg_ids
            .lock()
            .map(|mut registered| {
                registered.insert(msg_id);
            });
        Ok(())
    }

    pub(crate) async fn call_raw_observed(
        &self,
        node: NodeID,
        msg_id: MsgId,
        body: WireMessageBody,
        timeout: Option<std::time::Duration>,
        transport_policy: CommuRpcTransportPolicy,
    ) -> P2PResult<RpcCallRawObservedOutput> {
        {
            let handle = self.ensure_closed_runtime_handle().await?;
            let output = p2p_call_raw_observed(
                handle,
                node.to_string(),
                msg_id,
                body,
                timeout.map(|value| value.as_millis().min(u64::MAX as u128) as u64),
                transport_policy,
            )
            .await
            .map_err(p2p_closed_sdk_error)?;
            Ok(RpcCallRawObservedOutput {
                message: WireIncomingMessage {
                    from_node: output.message.from_node.into(),
                    head: output.message.head,
                    body: output.message.body,
                    local_observe: output.message.local_observe,
                },
                observe: RpcCallTransportObserveTrace {
                    caller_submit_us: output.observe.caller_submit_us,
                    caller_submit_ts_us: output.observe.caller_submit_ts_us,
                    request_path_kind: output.observe.request_path_kind,
                },
            })
        }
    }

    pub(crate) async fn send_resp_raw(
        &self,
        logical_target: NodeID,
        reply_next_hop: NodeID,
        task_id: TaskId,
        msg_id: MsgId,
        body: WireMessageBody,
        transport_policy: RpcTransportPolicy,
        incoming_local_observe: crate::p2p::WireTransportLocalObserve,
    ) -> P2PResult<()> {
        {
            let handle = self.ensure_closed_runtime_handle().await?;
            p2p_send_response_raw(
                handle,
                logical_target.to_string(),
                reply_next_hop.to_string(),
                task_id,
                msg_id,
                body,
                transport_policy,
                incoming_local_observe,
            )
            .await
            .map_err(p2p_closed_sdk_error)
        }
    }

    pub(crate) fn register_user_rpc_bytes_handler(
        &self,
        path: String,
        handler: Arc<dyn crate::p2p::UserRpcBytesHandler>,
    ) {
        {
            let view = self.module_view();
            let task_view = view.clone();
            view.spawn("p2p_register_user_rpc_bytes_handler", async move {
                let Ok(handle) = task_view.p2p_module().ensure_closed_runtime_handle().await else {
                    return;
                };
                let _ = p2p_register_user_rpc_bytes_handler(handle, path, handler).await;
            });
        }
    }

    pub(crate) fn register_user_rpc_bytes_handler_async(
        &self,
        path: String,
        handler: Arc<dyn crate::p2p::UserRpcBytesAsyncHandler>,
    ) {
        {
            let view = self.module_view();
            let task_view = view.clone();
            view.spawn("p2p_register_user_rpc_bytes_handler_async", async move {
                let Ok(handle) = task_view.p2p_module().ensure_closed_runtime_handle().await else {
                    return;
                };
                let _ = p2p_register_user_rpc_bytes_handler_async(handle, path, handler).await;
            });
        }
    }
}

#[async_trait]
impl LogicalModule for P2pModule {
    type View = P2pModuleView;
    type NewArg = CommuP2pModuleNewArg;
    type Error = P2pError;

    fn name(&self) -> &str {
        "P2pModule"
    }

    fn attach_view(&self, view: Self::View) {
        P2pModule::attach_view(self, view);
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        {
            if let Some(handle) = self.closed_runtime_handle() {
                match drop_runtime_handle(handle).await {
                    Ok(()) => {}
                    Err(error) if is_live_dependent_drop_error(&error) => {
                        tracing::info!(
                            handle_raw = handle.raw,
                            "deferring closed P2pModule handle drop until runtime dependents drain"
                        );
                        spawn_deferred_drop_runtime_handle(handle, "p2p_shutdown_deferred");
                    }
                    Err(error) => return Err(p2p_closed_sdk_error(error)),
                }
            }
            Ok(())
        }
    }
}

pub mod rpc {
    use std::marker::PhantomData;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use crate::p2p::__hidden::P2pModuleView;
    use crate::p2p::{P2PResult, P2pError, P2pModule, RpcTransportPolicy};
    use crate::{NodeID, TaskId};
    use tracing::warn;

    pub use fluxon_commu_contract::p2p::rpc::*;

    #[derive(Default)]
    pub struct RPCCaller<R: RPCReq> {
        _phantom: PhantomData<R>,
    }

    #[derive(Default)]
    pub struct RPCHandler<R: RPCReq> {
        _phantom: PhantomData<R>,
    }

    pub struct Responser {
        task_id: TaskId,
        node_id: NodeID,
        reply_next_hop: NodeID,
        incoming_local_observe: crate::p2p::WireTransportLocalObserve,
        view: P2pModuleView,
    }

    pub struct RPCResponsor<R: RPCReq> {
        responsor: Responser,
        _phantom: PhantomData<R>,
    }

    fn duration_to_i64_us(duration: Duration) -> i64 {
        duration.as_micros().min(i64::MAX as u128) as i64
    }

    impl Responser {
        pub async fn send_resp<RESP>(&self, resp: MsgPack<RESP>) -> P2PResult<()>
        where
            RESP: MsgPackSerializePart,
        {
            self.send_resp_with_transport_policy(resp, RpcTransportPolicy::AllowTransferRpcFastPath)
                .await
        }

        pub async fn send_resp_with_transport_policy<RESP>(
            &self,
            resp: MsgPack<RESP>,
            transport_policy: RpcTransportPolicy,
        ) -> P2PResult<()>
        where
            RESP: MsgPackSerializePart,
        {
            self.p2p()
                .send_resp_raw(
                    self.node_id.clone(),
                    self.reply_next_hop.clone(),
                    self.task_id,
                    resp.msg_id(),
                    resp.into_wire_body()?,
                    transport_policy,
                    self.incoming_local_observe,
                )
                .await
        }

        pub async fn send_resp_force_transport<RESP>(&self, resp: MsgPack<RESP>) -> P2PResult<()>
        where
            RESP: MsgPackSerializePart,
        {
            {
                self.send_resp_with_transport_policy(resp, RpcTransportPolicy::ForceTransport)
                    .await
            }
        }

        pub fn node_id(&self) -> NodeID {
            self.node_id.clone()
        }

        pub fn task_id(&self) -> TaskId {
            self.task_id
        }

        pub(crate) fn p2p(&self) -> &P2pModule {
            self.view.p2p_module()
        }
    }

    impl<R: RPCReq> RPCResponsor<R> {
        pub async fn send_resp(&self, resp: MsgPack<R::Resp>) -> P2PResult<()> {
            self.responsor.send_resp(resp).await
        }

        pub async fn send_resp_with_transport_policy(
            &self,
            resp: MsgPack<R::Resp>,
            transport_policy: RpcTransportPolicy,
        ) -> P2PResult<()> {
            self.responsor
                .send_resp_with_transport_policy(resp, transport_policy)
                .await
        }

        pub async fn send_resp_force_transport(&self, resp: MsgPack<R::Resp>) -> P2PResult<()> {
            self.responsor.send_resp_force_transport(resp).await
        }

        pub fn node_id(&self) -> NodeID {
            self.responsor.node_id()
        }

        pub fn task_id(&self) -> TaskId {
            self.responsor.task_id()
        }
    }

    impl<R: RPCReq> RPCCaller<R> {
        pub fn new() -> Self {
            Self {
                _phantom: PhantomData,
            }
        }

        pub fn regist(&self, p2p: &P2pModule) {
            regist_rpc_send::<R>(p2p);
        }

        pub async fn call(
            &self,
            p2p: &P2pModule,
            node_id: NodeID,
            req: MsgPack<R>,
            timeout: Option<std::time::Duration>,
            retry: usize,
        ) -> P2PResult<MsgPack<R::Resp>> {
            self.call_with_transport_policy(
                p2p,
                node_id,
                req,
                timeout,
                RpcTransportPolicy::AllowTransferRpcFastPath,
                retry,
            )
            .await
        }

        pub async fn call_with_transport_policy(
            &self,
            p2p: &P2pModule,
            node_id: NodeID,
            req: MsgPack<R>,
            timeout: Option<std::time::Duration>,
            transport_policy: RpcTransportPolicy,
            retry: usize,
        ) -> P2PResult<MsgPack<R::Resp>> {
            if retry > 0 {
                call_with_retry(p2p, node_id, req, retry, timeout, transport_policy).await
            } else {
                call_with_retry(p2p, node_id, req, 1, timeout, transport_policy).await
            }
        }

        pub async fn call_observed(
            &self,
            p2p: &P2pModule,
            node_id: NodeID,
            req: MsgPack<R>,
            timeout: Option<std::time::Duration>,
            retry: usize,
        ) -> P2PResult<RpcCallObservedOutput<R::Resp>> {
            self.call_with_transport_policy_observed(
                p2p,
                node_id,
                req,
                timeout,
                RpcTransportPolicy::AllowTransferRpcFastPath,
                retry,
            )
            .await
        }

        pub async fn call_with_transport_policy_observed(
            &self,
            p2p: &P2pModule,
            node_id: NodeID,
            req: MsgPack<R>,
            timeout: Option<std::time::Duration>,
            transport_policy: RpcTransportPolicy,
            retry: usize,
        ) -> P2PResult<RpcCallObservedOutput<R::Resp>> {
            if retry > 1 {
                call_with_retry(p2p, node_id, req, retry, timeout, transport_policy)
                    .await
                    .map(|resp| RpcCallObservedOutput {
                        resp,
                        observe: RpcCallObserveTrace::default(),
                    })
            } else {
                call_rpc_with_transport_policy_observed(
                    p2p,
                    node_id,
                    req,
                    timeout,
                    transport_policy,
                )
                .await
            }
        }
    }

    impl<R: RPCReq> RPCHandler<R> {
        pub fn new() -> Self {
            Self {
                _phantom: PhantomData,
            }
        }

        pub fn regist<F>(&self, p2p: &P2pModule, req_handler: F)
        where
            F: Fn(RPCResponsor<R>, MsgPack<R>) -> P2PResult<()> + Send + Sync + 'static,
            R: Default,
        {
            regist_rpc_recv::<R, _>(p2p, req_handler);
        }

        pub async fn call(
            &self,
            p2p: &P2pModule,
            node_id: NodeID,
            req: MsgPack<R>,
            timeout: Option<std::time::Duration>,
        ) -> P2PResult<MsgPack<R::Resp>> {
            self.call_with_transport_policy(
                p2p,
                node_id,
                req,
                timeout,
                RpcTransportPolicy::AllowTransferRpcFastPath,
            )
            .await
        }

        pub async fn call_with_transport_policy(
            &self,
            p2p: &P2pModule,
            node_id: NodeID,
            req: MsgPack<R>,
            timeout: Option<std::time::Duration>,
            transport_policy: RpcTransportPolicy,
        ) -> P2PResult<MsgPack<R::Resp>> {
            call_rpc_with_transport_policy::<R>(p2p, node_id, req, timeout, transport_policy).await
        }
    }

    pub fn regist_dispatch<M, F>(p2p: &P2pModule, m: M, f: F)
    where
        M: MsgPackSerializePart,
        F: Fn(Responser, MsgPack<M>) -> P2PResult<()> + Send + Sync + 'static,
    {
        let msg_id = m.msg_id();
        let view = p2p.module_view();
        p2p.regist_dispatch_raw(msg_id, move |reply_next_hop, _p2p, request_view, data| {
            let raw_lengths = request_view.body_raw_bytes_lengths;
            let value: MsgPack<M> = MsgPack::decode_from_body_view(
                request_view.body_serialize_part_len,
                raw_lengths,
                data,
            )?;
            let logical_source: NodeID = request_view.logical_source_peer_id.to_string().into();
            f(
                Responser {
                    task_id: request_view.task_id,
                    node_id: logical_source,
                    reply_next_hop: reply_next_hop.to_string().into(),
                    incoming_local_observe: crate::p2p::WireTransportLocalObserve {
                        frame_recv_done_ts_us: request_view.incoming_frame_recv_done_ts_us,
                        dispatch_enqueued_ts_us: request_view.incoming_dispatch_enqueued_ts_us,
                        dispatch_started_ts_us: request_view.incoming_dispatch_started_ts_us,
                        complete_pending_call_ts_us: request_view
                            .incoming_complete_pending_call_ts_us,
                    },
                    view: view.clone(),
                },
                value,
            )
        });
    }

    pub fn regist_rpc_send<REQ>(p2p: &P2pModule)
    where
        REQ: RPCReq,
    {
        let resp_msg_id = REQ::Resp::default().msg_id();
        p2p.register_rpc_response_msg_id(resp_msg_id);
        p2p.register_rpc_response_completion_raw(resp_msg_id);
    }

    pub fn regist_rpc_recv<REQ, F>(p2p: &P2pModule, req_handler: F)
    where
        REQ: RPCReq + Default,
        F: Fn(RPCResponsor<REQ>, MsgPack<REQ>) -> P2PResult<()> + Send + Sync + 'static,
    {
        let req_type = REQ::default();
        regist_dispatch(p2p, req_type, move |resp, req| {
            let rpc_resp = RPCResponsor {
                responsor: resp,
                _phantom: PhantomData,
            };
            req_handler(rpc_resp, req)
        });
    }

    pub async fn call_with_retry<REQ: RPCReq>(
        p2p: &P2pModule,
        node: NodeID,
        req: MsgPack<REQ>,
        retry: usize,
        timeout: Option<std::time::Duration>,
        transport_policy: RpcTransportPolicy,
    ) -> P2PResult<MsgPack<REQ::Resp>> {
        let mut failed_count = 0;
        let shutdown_poller = p2p.module_view().register_shutdown_poller();

        for attempt_idx in 0..retry {
            if !shutdown_poller.is_running() {
                return Err(P2pError::SystemShutdown {});
            }
            if attempt_idx != 0 {
                warn!("RPC call retrying, msg_id={}", req.msg_id());
            }
            let resp = call_rpc_with_transport_policy::<REQ>(
                p2p,
                node.clone(),
                req.clone(),
                timeout,
                transport_policy,
            )
            .await;
            match resp {
                Ok(resp) => {
                    if failed_count > 0 {
                        warn!(
                            "RPC call succeeded after {} retries, msg_id={}",
                            failed_count,
                            req.msg_id()
                        );
                    }
                    return Ok(resp);
                }
                Err(err) => {
                    if matches!(err, P2pError::InvalidRpcTimeout { .. }) {
                        return Err(err);
                    }
                    warn!(
                        "RPC call failed with error={:?}, retrying in 5 seconds, msg_id={}",
                        err,
                        req.msg_id()
                    );
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    failed_count += 1;
                }
            }
        }

        Err(P2pError::Timeout {
            detail: format!(
                "RPC call failed with retry {} times, msg_id: {}",
                retry,
                req.msg_id()
            ),
        })
    }

    pub async fn call_rpc_with_transport_policy<REQ: RPCReq>(
        p2p: &P2pModule,
        node: NodeID,
        req: MsgPack<REQ>,
        timeout: Option<std::time::Duration>,
        transport_policy: RpcTransportPolicy,
    ) -> P2PResult<MsgPack<REQ::Resp>> {
        Ok(
            call_rpc_with_transport_policy_observed(p2p, node, req, timeout, transport_policy)
                .await?
                .resp,
        )
    }

    pub async fn call_rpc_with_transport_policy_observed<REQ: RPCReq>(
        p2p: &P2pModule,
        node: NodeID,
        req: MsgPack<REQ>,
        timeout: Option<std::time::Duration>,
        transport_policy: RpcTransportPolicy,
    ) -> P2PResult<RpcCallObservedOutput<REQ::Resp>> {
        regist_rpc_send::<REQ>(p2p);

        {
            // Closed runtime owns pending-call completion. The first response for a msg_id must be
            // registered in that runtime before the call is issued, otherwise a self-local
            // response can be dispatched as an ordinary message and never complete the caller.
            p2p.ensure_rpc_response_msg_id_registered_runtime(REQ::Resp::default().msg_id())
                .await?;
        }

        let timeout_duration = match timeout {
            Some(duration) => {
                let min_duration = Duration::from_secs(MIN_EXPLICIT_RPC_TIMEOUT_SECS);
                if duration < min_duration {
                    return Err(P2pError::InvalidRpcTimeout {
                        timeout_ms: duration.as_secs() * 1_000
                            + u64::from(duration.subsec_millis()),
                        min_timeout_ms: MIN_EXPLICIT_RPC_TIMEOUT_SECS * 1_000,
                        reason: format!(
                            "Explicit RPC timeout below {}s is forbidden.",
                            MIN_EXPLICIT_RPC_TIMEOUT_SECS
                        ),
                    });
                }
                Some(duration)
            }
            None => None,
        };

        let wire_encode_started_at = Instant::now();
        let msg_id = req.msg_id();
        let wire_body = req.into_wire_body()?;
        let wire_encode_us = duration_to_i64_us(wire_encode_started_at.elapsed());
        let raw_output = p2p
            .call_raw_observed(node, msg_id, wire_body, timeout_duration, transport_policy)
            .await?;
        let response_local_observe = raw_output.message.local_observe;
        let resp_decode_started_at = Instant::now();
        let resp = MsgPack::<REQ::Resp>::decode_from_body(
            &raw_output.message.head,
            &raw_output.message.body,
        )?;
        let caller_complete_us = duration_to_i64_us(resp_decode_started_at.elapsed());
        let caller_decode_done_ts_us = current_cross_process_monotonic_us();
        Ok(RpcCallObservedOutput {
            resp,
            observe: RpcCallObserveTrace {
                caller_submit_us: wire_encode_us
                    .saturating_add(raw_output.observe.caller_submit_us),
                caller_complete_us,
                caller_submit_ts_us: raw_output.observe.caller_submit_ts_us,
                request_path_kind: raw_output.observe.request_path_kind,
                caller_response_frame_recv_done_ts_us: response_local_observe.frame_recv_done_ts_us,
                caller_response_dispatch_enqueued_ts_us: response_local_observe
                    .dispatch_enqueued_ts_us,
                caller_response_dispatch_started_ts_us: response_local_observe
                    .dispatch_started_ts_us,
                caller_response_complete_pending_call_ts_us: response_local_observe
                    .complete_pending_call_ts_us,
                caller_decode_done_ts_us,
            },
        })
    }

    pub async fn call_rpc<REQ: RPCReq>(
        p2p: &P2pModule,
        node: NodeID,
        req: MsgPack<REQ>,
        timeout: Option<std::time::Duration>,
    ) -> P2PResult<MsgPack<REQ::Resp>> {
        call_rpc_with_transport_policy(
            p2p,
            node,
            req,
            timeout,
            RpcTransportPolicy::AllowTransferRpcFastPath,
        )
        .await
    }

    pub async fn call_rpc_observed<REQ: RPCReq>(
        p2p: &P2pModule,
        node: NodeID,
        req: MsgPack<REQ>,
        timeout: Option<std::time::Duration>,
    ) -> P2PResult<RpcCallObservedOutput<REQ::Resp>> {
        call_rpc_with_transport_policy_observed(
            p2p,
            node,
            req,
            timeout,
            RpcTransportPolicy::AllowTransferRpcFastPath,
        )
        .await
    }

    pub fn register_user_rpc_bytes_handler(
        p2p: &P2pModule,
        path: String,
        handler: Arc<dyn UserRpcBytesHandler>,
    ) {
        p2p.register_user_rpc_bytes_handler(path, handler);
    }

    pub fn register_user_rpc_bytes_handler_async(
        p2p: &P2pModule,
        path: String,
        handler: Arc<dyn UserRpcBytesAsyncHandler>,
    ) {
        p2p.register_user_rpc_bytes_handler_async(path, handler);
    }
}

pub mod wire {
    pub use fluxon_commu_contract::p2p::wire::*;
}

pub use fluxon_commu_contract::p2p::rpc::*;
pub use fluxon_commu_contract::p2p::surface::{
    MAX_RELAY_HOPS, P2pLane, PeerGen, RelayCapsSnapshot, RelayRoute, TierPeerView,
};
pub use fluxon_commu_contract::p2p::wire::*;
pub use fluxon_commu_contract::p2p::{P2PError, P2PResult, P2pError};

pub fn network_transport_kind() -> P2pTransportKind {
    {
        #[cfg(any(feature = "fastws_transport", feature = "sockudo_ws_transport"))]
        {
            return P2pTransportKind::Websocket;
        }
        #[cfg(any(feature = "tquic_transport", feature = "tquic_transport_v2"))]
        {
            return P2pTransportKind::Tquic;
        }
        P2pTransportKind::Tcp
    }
}
