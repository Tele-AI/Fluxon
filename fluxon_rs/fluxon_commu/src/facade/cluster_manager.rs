use std::collections::HashMap;
use std::sync::{Arc, RwLock, Weak};

use std::sync::atomic::{AtomicBool, Ordering};

use crate::NodeIDString as CommuNodeIDString;
use crate::config::NetworkConfig as CommuNetworkConfig;
use crate::member_metadata::{
    MemberRdmaResolvedConfig as CommuMemberRdmaResolvedConfig,
    MemberRdmaTransferEngineRuntime as CommuMemberRdmaTransferEngineRuntime,
};
use crate::transfer::{
    TransferLinkEtcdWriterHandle as CommuTransferLinkEtcdWriterHandle, TransferLinkKeyKind,
    TransferLinkP2pSnapshotSource as CommuTransferLinkP2pSnapshotSource,
    TransferLinkRecord as CommuTransferLinkRecord, TransferReadyInfo as CommuTransferReadyInfo,
};
use async_trait::async_trait;
pub use fluxon_commu_contract::cluster_manager::{
    ClusterManagerNewArg, ClusterManagerRdmaControlInit, IpcBandwidthAttributorHandle,
};
use fluxon_commu_contract::{
    ClosedRuntimeClusterEventStreamItem, ClosedRuntimeClusterManagerCall,
    ClosedRuntimeClusterManagerResponse, ClosedRuntimeClusterRdmaResolvedConfigStreamItem,
    ClosedRuntimeHandle,
};
use fluxon_framework::LogicalModule;
use fluxon_framework::{ResourceRegistry, ResourceRegistryAccessTrait};
use fluxon_framework_compiled::async_panic::AsyncPanicSendExt;
use fluxon_framework_compiled::shutdown::{ShutdownPoller, ShutdownWaiter, ViewShutdownExt};
use fluxon_framework_compiled::spawn::ViewSpawnExt;
use fluxon_framework_compiled::upgrade_view_guard::UpgradeViewGuard;
use fluxon_framework_compiled::util::ViewSpawnHandle;
use fluxon_observability::kv_metrics_actor::ObserveHandle;
use fluxon_util::etcd::{EtcdEndpointSet, ManagedEtcdClient};
use limit_thirdparty::tokio::sync::{abroadcast, ampsc};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::closed_sdk::{
    cluster_manager_call, construct_cluster_manager_handle,
    current_cluster_manager_self_rdma_resolved_config, drop_runtime_handle,
    is_live_dependent_drop_error, recv_cluster_event_stream,
    recv_cluster_rdma_resolved_config_stream, spawn_deferred_drop_runtime_handle,
    subscribe_cluster_manager_events, watch_cluster_manager_self_rdma_resolved_config,
};

#[doc(hidden)]
pub trait ClusterManagerAccessTrait: Send + Sync {
    fn cluster_manager(&self) -> &ClusterManager;
}

#[doc(hidden)]
pub mod __hidden {
    use super::*;

    #[doc(hidden)]
    pub trait ClusterManagerViewTrait:
        Send
        + Sync
        + ClusterManagerAccessTrait
        + ResourceRegistryAccessTrait
        + ViewShutdownExt
        + AsyncPanicSendExt
        + ViewSpawnExt
    {
    }

    impl<T> ClusterManagerViewTrait for T where
        T: Send
            + Sync
            + ClusterManagerAccessTrait
            + ResourceRegistryAccessTrait
            + ViewShutdownExt
            + AsyncPanicSendExt
            + ViewSpawnExt
    {
    }

    #[doc(hidden)]
    #[derive(Clone)]
    pub struct ClusterManagerView {
        view: Weak<dyn ClusterManagerViewTrait>,
    }

    impl ClusterManagerView {
        pub fn new(view: &Arc<dyn ClusterManagerViewTrait>) -> Self {
            Self {
                view: Arc::downgrade(view),
            }
        }

        pub fn try_upgrade(&self) -> Option<UpgradeViewGuard<dyn ClusterManagerViewTrait>> {
            self.view.upgrade().map(UpgradeViewGuard::new)
        }

        pub(crate) fn upgrade_arc(&self) -> Option<Arc<dyn ClusterManagerViewTrait>> {
            self.view.upgrade()
        }

        pub fn resource_registry(&self) -> &ResourceRegistry {
            let arc_view = self.view.upgrade().expect(
                "view of module ClusterManager has been dropped when accessing resource registry",
            );
            unsafe {
                let ptr =
                    std::ptr::NonNull::new(Arc::as_ptr(&arc_view) as *const _ as *mut _).unwrap();
                let view_ref: &dyn ClusterManagerViewTrait = ptr.as_ref();
                let reg_ptr =
                    std::ptr::NonNull::new(view_ref.resource_registry() as *const _ as *mut _)
                        .unwrap();
                reg_ptr.as_ref()
            }
        }

        pub fn cluster_manager(&self) -> &ClusterManager {
            let arc_view = self
            .view
            .upgrade()
            .expect("view of module ClusterManager has been dropped when accessing dependency ClusterManager");
            unsafe {
                let ptr =
                    std::ptr::NonNull::new(Arc::as_ptr(&arc_view) as *const _ as *mut _).unwrap();
                let view_ref: &dyn ClusterManagerViewTrait = ptr.as_ref();
                let module_ptr =
                    std::ptr::NonNull::new(view_ref.cluster_manager() as *const _ as *mut _)
                        .unwrap();
                module_ptr.as_ref()
            }
        }

        pub fn register_shutdown_poller(&self) -> ShutdownPoller {
            self.view
            .upgrade()
            .expect(
                "view of module ClusterManager has been dropped before register_shutdown_poller",
            )
            .register_shutdown_poller()
        }

        pub fn register_shutdown_waiter(&self) -> ShutdownWaiter {
            self.view
            .upgrade()
            .expect(
                "view of module ClusterManager has been dropped before register_shutdown_waiter",
            )
            .register_shutdown_waiter()
        }

        pub fn async_panic(&self, msg: String) {
            self.view
                .upgrade()
                .expect("view of module ClusterManager has been dropped before async_panic")
                .async_panic(msg);
        }

        pub fn spawn<F, N>(&self, name: N, fut: F) -> ViewSpawnHandle<dyn ClusterManagerViewTrait>
        where
            F: std::future::Future<Output = ()> + Send + 'static,
            N: Into<String>,
        {
            let view_ref = self
                .view
                .upgrade()
                .expect("view of module ClusterManager has been dropped before spawn");
            let boxed: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
                Box::pin(fut);
            let handle = ViewSpawnExt::spawn_boxed(&*view_ref, boxed);
            ViewSpawnHandle::new(name, handle, view_ref)
        }
    }

    impl ClusterManagerAccessTrait for ClusterManagerView {
        fn cluster_manager(&self) -> &ClusterManager {
            ClusterManagerView::cluster_manager(self)
        }
    }

    impl ResourceRegistryAccessTrait for ClusterManagerView {
        fn resource_registry(&self) -> &ResourceRegistry {
            ClusterManagerView::resource_registry(self)
        }
    }

    impl ViewShutdownExt for ClusterManagerView {
        fn register_shutdown_waiter(&self) -> ShutdownWaiter {
            ClusterManagerView::register_shutdown_waiter(self)
        }

        fn register_shutdown_poller(&self) -> ShutdownPoller {
            ClusterManagerView::register_shutdown_poller(self)
        }
    }

    impl AsyncPanicSendExt for ClusterManagerView {
        fn async_panic(&self, msg: String) {
            ClusterManagerView::async_panic(self, msg);
        }
    }

    impl ViewSpawnExt for ClusterManagerView {
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

pub use self::__hidden::{ClusterManagerView, ClusterManagerViewTrait};

struct ClosedClusterManagerRuntime {
    handle: ClosedRuntimeHandle,
    self_member_id: String,
    cluster_name: String,
    etcd_endpoints: Vec<String>,
    _etcd_backend: ManagedEtcdClient,
    observe_handle: std::sync::OnceLock<ObserveHandle>,
    ipc_bandwidth_attributor_handle: std::sync::OnceLock<IpcBandwidthAttributorHandle>,
    self_info: Arc<RwLock<crate::ClusterMember>>,
    members: Arc<RwLock<HashMap<String, crate::ClusterMember>>>,
    prev_members: Arc<RwLock<HashMap<String, crate::ClusterMember>>>,
    is_watching: AtomicBool,
    event_tx: abroadcast::Sender<crate::ClusterEvent>,
    self_rdma_resolved_tx: watch::Sender<CommuMemberRdmaResolvedConfig>,
    transfer_link_writer: CommuTransferLinkEtcdWriterHandle,
    transfer_link_p2p_snapshot_source: CommuTransferLinkP2pSnapshotSource,
}

impl ClosedClusterManagerRuntime {
    async fn construct(arg: ClusterManagerNewArg) -> crate::ClusterResult<Self> {
        let handle = construct_cluster_manager_handle(arg.clone())
            .await
            .map_err(cluster_manager_closed_sdk_error)?;
        let self_member_id = closed_cluster_manager_string_call(
            handle,
            ClosedRuntimeClusterManagerCall::SelfMemberId,
        )
        .await?;
        let cluster_name = closed_cluster_manager_string_call(
            handle,
            ClosedRuntimeClusterManagerCall::ClusterName,
        )
        .await?;
        let self_info = closed_cluster_manager_member_call(
            handle,
            ClosedRuntimeClusterManagerCall::GetSelfInfo,
        )
        .await?;
        let mut members = closed_cluster_manager_members_call(
            handle,
            ClosedRuntimeClusterManagerCall::GetMembers,
        )
        .await?;
        if !members.iter().any(|member| member.id == self_info.id) {
            members.push(self_info.clone());
        }
        let current_rdma = current_cluster_manager_self_rdma_resolved_config(handle)
            .await
            .map_err(cluster_manager_closed_sdk_error)?;
        let (event_tx, _) = abroadcast::channel(100);
        let (self_rdma_resolved_tx, _) = watch::channel(current_rdma);
        let endpoint_set = EtcdEndpointSet::new(arg.etcd_endpoints.clone()).map_err(|error| {
            crate::ClusterError::EtcdConnection {
                endpoints: arg.etcd_endpoints.clone(),
                error: error.to_string(),
            }
        })?;
        let etcd_backend = ManagedEtcdClient::acquire(endpoint_set);
        let transfer_link_p2p_snapshot_source = CommuTransferLinkP2pSnapshotSource::new(
            etcd_backend
                .client()
                .await
                .map_err(|error| crate::ClusterError::EtcdConnection {
                    endpoints: arg.etcd_endpoints.clone(),
                    error: error.to_string(),
                })?,
            format!("/{}/transfer_link/p2p", cluster_name),
        );
        let transfer_link_writer = make_runtime_transfer_link_writer(handle);
        let runtime = Self {
            handle,
            self_member_id,
            cluster_name,
            etcd_endpoints: arg.etcd_endpoints,
            _etcd_backend: etcd_backend,
            observe_handle: std::sync::OnceLock::new(),
            ipc_bandwidth_attributor_handle: std::sync::OnceLock::new(),
            self_info: Arc::new(RwLock::new(self_info)),
            members: Arc::new(RwLock::new(
                members
                    .into_iter()
                    .map(|member| (member.id.to_string(), member))
                    .collect(),
            )),
            prev_members: Arc::new(RwLock::new(HashMap::new())),
            is_watching: AtomicBool::new(false),
            event_tx,
            self_rdma_resolved_tx,
            transfer_link_writer,
            transfer_link_p2p_snapshot_source,
        };
        runtime.spawn_event_mirror();
        runtime.spawn_self_rdma_mirror();
        Ok(runtime)
    }

    fn spawn_event_mirror(&self) {
        let handle = self.handle;
        let event_tx = self.event_tx.clone();
        let self_member_id = self.self_member_id.clone();
        let members = Arc::clone(&self.members);
        let prev_members = Arc::clone(&self.prev_members);
        let self_info = Arc::clone(&self.self_info);
        tokio::spawn(async move {
            let Ok(stream_handle) = subscribe_cluster_manager_events(handle).await else {
                return;
            };
            loop {
                match recv_cluster_event_stream(stream_handle).await {
                    Ok(ClosedRuntimeClusterEventStreamItem::Event(event)) => {
                        apply_cluster_event_cache(
                            &members,
                            &prev_members,
                            &self_info,
                            &self_member_id,
                            &event,
                        );
                        let _ = event_tx.send(event);
                    }
                    Ok(ClosedRuntimeClusterEventStreamItem::Lagged { .. }) => {
                        if let Ok(snapshot) = closed_cluster_manager_members_call(
                            handle,
                            ClosedRuntimeClusterManagerCall::GetMembers,
                        )
                        .await
                        {
                            let mut guard = members.write().expect("members cache poisoned");
                            *guard = snapshot
                                .into_iter()
                                .map(|member| (member.id.to_string(), member))
                                .collect();
                        }
                        if let Ok(snapshot) = closed_cluster_manager_member_call(
                            handle,
                            ClosedRuntimeClusterManagerCall::GetSelfInfo,
                        )
                        .await
                        {
                            *self_info.write().expect("self-info cache poisoned") = snapshot;
                        }
                    }
                    Ok(ClosedRuntimeClusterEventStreamItem::Closed) => break,
                    Err(_) => break,
                }
            }
            let _ = drop_runtime_handle(stream_handle).await;
        });
    }

    fn spawn_self_rdma_mirror(&self) {
        let handle = self.handle;
        let tx = self.self_rdma_resolved_tx.clone();
        tokio::spawn(async move {
            let Ok(stream_handle) = watch_cluster_manager_self_rdma_resolved_config(handle).await
            else {
                return;
            };
            loop {
                match recv_cluster_rdma_resolved_config_stream(stream_handle).await {
                    Ok(ClosedRuntimeClusterRdmaResolvedConfigStreamItem::Value(value)) => {
                        let _ = tx.send(value);
                    }
                    Ok(ClosedRuntimeClusterRdmaResolvedConfigStreamItem::Closed) => break,
                    Err(_) => break,
                }
            }
            let _ = drop_runtime_handle(stream_handle).await;
        });
    }
}

fn cluster_manager_closed_sdk_error(
    error: crate::closed_sdk::ClosedSdkConsumerError,
) -> crate::ClusterError {
    crate::ClusterError::Unknown(format!("closed sdk cluster-manager call failed: {error}"))
}

fn make_runtime_transfer_link_writer(
    handle: ClosedRuntimeHandle,
) -> CommuTransferLinkEtcdWriterHandle {
    let (tx, mut rx) = ampsc::channel::<crate::transfer::TransferLinkEtcdWrite>(4096);
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let call = match msg.kind {
                TransferLinkKeyKind::P2p => {
                    ClosedRuntimeClusterManagerCall::TryReportTransferLinkP2p {
                        from: msg.from,
                        to: msg.to,
                        record: parse_transfer_link_record_for_kind(
                            TransferLinkKeyKind::P2p,
                            &msg.value,
                        ),
                    }
                }
                TransferLinkKeyKind::Te => {
                    ClosedRuntimeClusterManagerCall::TryReportTransferLinkTe {
                        from: msg.from,
                        to: msg.to,
                        record: parse_transfer_link_record_for_kind(
                            TransferLinkKeyKind::Te,
                            &msg.value,
                        ),
                    }
                }
            };
            let _ = cluster_manager_call(handle, call).await;
        }
    });
    CommuTransferLinkEtcdWriterHandle::new(tx)
}

fn parse_transfer_link_record_for_kind(
    kind: TransferLinkKeyKind,
    value: &str,
) -> CommuTransferLinkRecord {
    match kind {
        TransferLinkKeyKind::P2p => {
            let (p2p_state, transport) = CommuTransferLinkRecord::parse_etcd_p2p_value(value)
                .unwrap_or((crate::transfer::TransferLinkP2pState::Unknown, None));
            CommuTransferLinkRecord {
                p2p: p2p_state,
                te: crate::transfer::TransferLinkTeState::None,
                p2p_transport: transport,
            }
        }
        TransferLinkKeyKind::Te => CommuTransferLinkRecord {
            p2p: crate::transfer::TransferLinkP2pState::Unknown,
            p2p_transport: None,
            te: parse_transfer_link_te_value(value),
        },
    }
}

fn parse_transfer_link_te_value(value: &str) -> crate::transfer::TransferLinkTeState {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return crate::transfer::TransferLinkTeState::None;
    }
    let mut engine: Option<&str> = None;
    let mut fallback = false;
    for token in trimmed
        .split('+')
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        match token {
            "fallback" => fallback = true,
            "closed" | "p2p_mode" => engine = Some(token),
            _ => return crate::transfer::TransferLinkTeState::None,
        }
    }
    match (engine, fallback) {
        (Some("closed"), false) => crate::transfer::TransferLinkTeState::ClosedDirect,
        (Some("closed"), true) => crate::transfer::TransferLinkTeState::ClosedFallback,
        (Some("p2p_mode"), false) => crate::transfer::TransferLinkTeState::P2pModeDirect,
        _ => crate::transfer::TransferLinkTeState::None,
    }
}

fn apply_cluster_event_cache(
    members: &Arc<RwLock<HashMap<String, crate::ClusterMember>>>,
    prev_members: &Arc<RwLock<HashMap<String, crate::ClusterMember>>>,
    self_info: &Arc<RwLock<crate::ClusterMember>>,
    self_member_id: &str,
    event: &crate::ClusterEvent,
) {
    match event {
        crate::ClusterEvent::MemberJoined(member) => {
            members
                .write()
                .expect("members cache poisoned")
                .insert(member.id.to_string(), member.clone());
            prev_members
                .write()
                .expect("prev-members cache poisoned")
                .remove(member.id.as_str());
            if member.id == self_member_id {
                *self_info.write().expect("self-info cache poisoned") = member.clone();
            }
        }
        crate::ClusterEvent::MemberUpdated(member) => {
            let previous = members
                .write()
                .expect("members cache poisoned")
                .insert(member.id.to_string(), member.clone());
            if let Some(previous) = previous {
                prev_members
                    .write()
                    .expect("prev-members cache poisoned")
                    .insert(member.id.to_string(), previous);
            }
            if member.id == self_member_id {
                *self_info.write().expect("self-info cache poisoned") = member.clone();
            }
        }
        crate::ClusterEvent::MemberLeft(member_id) => {
            if let Some(previous) = members
                .write()
                .expect("members cache poisoned")
                .remove(member_id.as_str())
            {
                prev_members
                    .write()
                    .expect("prev-members cache poisoned")
                    .insert(member_id.clone(), previous);
            }
        }
    }
}

async fn closed_cluster_manager_string_call(
    handle: ClosedRuntimeHandle,
    call: ClosedRuntimeClusterManagerCall,
) -> crate::ClusterResult<String> {
    match cluster_manager_call(handle, call)
        .await
        .map_err(cluster_manager_closed_sdk_error)?
    {
        ClosedRuntimeClusterManagerResponse::StringValue(value) => Ok(value),
        other => Err(crate::ClusterError::Unknown(format!(
            "unexpected closed sdk cluster-manager string response: {other:?}"
        ))),
    }
}

async fn closed_cluster_manager_member_call(
    handle: ClosedRuntimeHandle,
    call: ClosedRuntimeClusterManagerCall,
) -> crate::ClusterResult<crate::ClusterMember> {
    match cluster_manager_call(handle, call)
        .await
        .map_err(cluster_manager_closed_sdk_error)?
    {
        ClosedRuntimeClusterManagerResponse::ClusterMemberValue(value) => Ok(value),
        other => Err(crate::ClusterError::Unknown(format!(
            "unexpected closed sdk cluster-manager member response: {other:?}"
        ))),
    }
}

async fn closed_cluster_manager_members_call(
    handle: ClosedRuntimeHandle,
    call: ClosedRuntimeClusterManagerCall,
) -> crate::ClusterResult<Vec<crate::ClusterMember>> {
    match cluster_manager_call(handle, call)
        .await
        .map_err(cluster_manager_closed_sdk_error)?
    {
        ClosedRuntimeClusterManagerResponse::ClusterMembersValue(value) => Ok(value),
        other => Err(crate::ClusterError::Unknown(format!(
            "unexpected closed sdk cluster-manager members response: {other:?}"
        ))),
    }
}

async fn closed_cluster_manager_usize_call(
    handle: ClosedRuntimeHandle,
    call: ClosedRuntimeClusterManagerCall,
) -> crate::ClusterResult<usize> {
    match cluster_manager_call(handle, call)
        .await
        .map_err(cluster_manager_closed_sdk_error)?
    {
        ClosedRuntimeClusterManagerResponse::UsizeValue(value) => Ok(value),
        other => Err(crate::ClusterError::Unknown(format!(
            "unexpected closed sdk cluster-manager usize response: {other:?}"
        ))),
    }
}

async fn closed_cluster_manager_optional_transfer_ready_call(
    handle: ClosedRuntimeHandle,
    call: ClosedRuntimeClusterManagerCall,
) -> crate::ClusterResult<Option<CommuTransferReadyInfo>> {
    match cluster_manager_call(handle, call)
        .await
        .map_err(cluster_manager_closed_sdk_error)?
    {
        ClosedRuntimeClusterManagerResponse::OptionalTransferReadyInfoValue(value) => Ok(value),
        other => Err(crate::ClusterError::Unknown(format!(
            "unexpected closed sdk cluster-manager optional-transfer-ready response: {other:?}"
        ))),
    }
}

async fn closed_cluster_manager_transfer_ready_call(
    handle: ClosedRuntimeHandle,
    call: ClosedRuntimeClusterManagerCall,
) -> crate::ClusterResult<CommuTransferReadyInfo> {
    match cluster_manager_call(handle, call)
        .await
        .map_err(cluster_manager_closed_sdk_error)?
    {
        ClosedRuntimeClusterManagerResponse::TransferReadyInfoValue(value) => Ok(value),
        other => Err(crate::ClusterError::Unknown(format!(
            "unexpected closed sdk cluster-manager transfer-ready response: {other:?}"
        ))),
    }
}

async fn closed_cluster_manager_unit_call(
    handle: ClosedRuntimeHandle,
    call: ClosedRuntimeClusterManagerCall,
) -> crate::ClusterResult<()> {
    match cluster_manager_call(handle, call)
        .await
        .map_err(cluster_manager_closed_sdk_error)?
    {
        ClosedRuntimeClusterManagerResponse::Unit => Ok(()),
        other => Err(crate::ClusterError::Unknown(format!(
            "unexpected closed sdk cluster-manager unit response: {other:?}"
        ))),
    }
}

fn refresh_closed_self_info_cache(
    runtime: &ClosedClusterManagerRuntime,
    self_info: crate::ClusterMember,
) {
    *runtime.self_info.write().expect("self-info cache poisoned") = self_info.clone();
    runtime
        .members
        .write()
        .expect("members cache poisoned")
        .insert(self_info.id.to_string(), self_info);
}

pub struct ClusterManager {
    closed: ClosedClusterManagerRuntime,
}

impl ClusterManager {
    #[doc(hidden)]
    pub fn closed_runtime_handle(&self) -> ClosedRuntimeHandle {
        self.closed.handle
    }

    pub async fn construct(arg: ClusterManagerNewArg) -> crate::ClusterResult<Self> {
        {
            Ok(Self {
                closed: ClosedClusterManagerRuntime::construct(arg).await?,
            })
        }
    }

    pub async fn new(
        etcd_endpoints: Vec<String>,
        cluster_name: String,
        instance_name: Option<String>,
        port: Option<u16>,
        metadata: HashMap<String, String>,
        local_ipc_root: Option<String>,
        rdma_control_init: ClusterManagerRdmaControlInit,
        sub_cluster: Option<String>,
        network: Option<CommuNetworkConfig>,
    ) -> crate::ClusterResult<Self> {
        Self::construct(ClusterManagerNewArg {
            etcd_endpoints,
            cluster_name,
            instance_name,
            port,
            metadata,
            local_ipc_root,
            rdma_control_init,
            sub_cluster,
            network,
        })
        .await
    }

    pub fn attach_observe_handle(&self, handle: ObserveHandle) {
        {
            let _ = self.closed.observe_handle.set(handle);
        }
    }

    pub fn observe_handle(&self) -> Option<&ObserveHandle> {
        { self.closed.observe_handle.get() }
    }

    pub fn attach_ipc_bandwidth_attributor_handle(&self, handle: IpcBandwidthAttributorHandle) {
        {
            let _ = self.closed.ipc_bandwidth_attributor_handle.set(handle);
        }
    }

    pub fn ipc_bandwidth_attributor_handle(&self) -> Option<&IpcBandwidthAttributorHandle> {
        { self.closed.ipc_bandwidth_attributor_handle.get() }
    }

    pub(crate) fn attach_view(&self, view: ClusterManagerView) {
        {
            let _ = view;
        }
    }

    pub async fn init2_for_init_dag(&self) -> crate::ClusterResult<()> {
        {
            closed_cluster_manager_unit_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::Init2ForInitDag,
            )
            .await?;
            self.closed.is_watching.store(true, Ordering::Relaxed);
            let self_info = closed_cluster_manager_member_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::GetSelfInfo,
            )
            .await?;
            refresh_closed_self_info_cache(&self.closed, self_info);
            let members = closed_cluster_manager_members_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::GetMembers,
            )
            .await?;
            *self.closed.members.write().expect("members cache poisoned") = members
                .into_iter()
                .map(|member| (member.id.to_string(), member))
                .collect();
            Ok(())
        }
    }

    pub async fn join_cluster(&self) -> crate::ClusterResult<()> {
        {
            closed_cluster_manager_unit_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::JoinCluster,
            )
            .await?;
            let self_info = closed_cluster_manager_member_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::GetSelfInfo,
            )
            .await?;
            refresh_closed_self_info_cache(&self.closed, self_info);
            let members = closed_cluster_manager_members_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::GetMembers,
            )
            .await?;
            *self.closed.members.write().expect("members cache poisoned") = members
                .into_iter()
                .map(|member| (member.id.to_string(), member))
                .collect();
            Ok(())
        }
    }

    pub fn self_member_id(&self) -> &str {
        { &self.closed.self_member_id }
    }

    pub fn transfer_link_writer_handle(&self) -> CommuTransferLinkEtcdWriterHandle {
        { self.closed.transfer_link_writer.clone() }
    }

    pub fn transfer_link_p2p_snapshot_source(&self) -> CommuTransferLinkP2pSnapshotSource {
        { self.closed.transfer_link_p2p_snapshot_source.clone() }
    }

    pub fn cluster_name(&self) -> &str {
        { &self.closed.cluster_name }
    }

    pub fn current_self_rdma_resolved_config(&self) -> CommuMemberRdmaResolvedConfig {
        { self.closed.self_rdma_resolved_tx.borrow().clone() }
    }

    pub fn watch_self_rdma_resolved_config(
        &self,
    ) -> tokio::sync::watch::Receiver<CommuMemberRdmaResolvedConfig> {
        { self.closed.self_rdma_resolved_tx.subscribe() }
    }

    pub fn set_self_rdma_transfer_engine_runtime(
        &self,
        runtime: CommuMemberRdmaTransferEngineRuntime,
    ) {
        {
            let handle = self.closed.handle;
            tokio::spawn(async move {
                let _ = closed_cluster_manager_unit_call(
                    handle,
                    ClosedRuntimeClusterManagerCall::SetSelfRdmaTransferEngineRuntime { runtime },
                )
                .await;
            });
        }
    }

    pub async fn set_listening_port(&self, port: u16) -> crate::ClusterResult<()> {
        {
            closed_cluster_manager_unit_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::SetListeningPort { port },
            )
            .await?;
            let self_info = closed_cluster_manager_member_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::GetSelfInfo,
            )
            .await?;
            refresh_closed_self_info_cache(&self.closed, self_info);
            Ok(())
        }
    }

    pub fn etcd_endpoints(&self) -> Vec<String> {
        { self.closed.etcd_endpoints.clone() }
    }

    pub fn get_member_info_cached(&self, member_id: &str) -> Option<crate::ClusterMember> {
        {
            self.closed
                .members
                .read()
                .expect("members cache poisoned")
                .get(member_id)
                .cloned()
        }
    }

    pub async fn leave_cluster(&self) -> crate::ClusterResult<()> {
        {
            closed_cluster_manager_unit_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::LeaveCluster,
            )
            .await
        }
    }

    pub fn listen(
        &self,
    ) -> limit_thirdparty::tokio::sync::abroadcast::Receiver<crate::ClusterEvent> {
        { self.closed.event_tx.subscribe() }
    }

    pub fn is_watching(&self) -> bool {
        { self.closed.is_watching.load(Ordering::Relaxed) }
    }

    pub async fn wait_member_count(
        &self,
        white_list_roles: Vec<crate::NodeRole>,
    ) -> crate::ClusterResult<usize> {
        {
            closed_cluster_manager_usize_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::WaitMemberCount { white_list_roles },
            )
            .await
        }
    }

    pub async fn start_watching(&self) -> crate::ClusterResult<()> {
        {
            closed_cluster_manager_unit_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::StartWatching,
            )
            .await?;
            self.closed.is_watching.store(true, Ordering::Relaxed);
            Ok(())
        }
    }

    pub async fn stop_watching(&self) -> crate::ClusterResult<()> {
        {
            closed_cluster_manager_unit_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::StopWatching,
            )
            .await?;
            self.closed.is_watching.store(false, Ordering::Relaxed);
            Ok(())
        }
    }

    pub fn get_members(&self) -> Vec<crate::ClusterMember> {
        {
            self.closed
                .members
                .read()
                .expect("members cache poisoned")
                .values()
                .cloned()
                .collect()
        }
    }

    pub fn get_prev_member_info(&self, node_id: &str) -> Option<crate::ClusterMember> {
        {
            self.closed
                .prev_members
                .read()
                .expect("prev-members cache poisoned")
                .get(node_id)
                .cloned()
        }
    }

    pub fn get_client_members(&self) -> Vec<crate::ClusterMember> {
        {
            self.get_members()
                .into_iter()
                .filter(|member| matches!(member.node_role(), crate::NodeRole::Client))
                .collect()
        }
    }

    pub fn get_self_info(&self) -> crate::ClusterMember {
        {
            self.closed
                .self_info
                .read()
                .expect("self-info cache poisoned")
                .clone()
        }
    }

    pub fn get_master_member(&self) -> Option<crate::ClusterMember> {
        {
            let self_info = self.get_self_info();
            if matches!(self_info.node_role(), crate::NodeRole::Master) {
                return Some(self_info);
            }
            self.get_members()
                .into_iter()
                .find(|member| matches!(member.node_role(), crate::NodeRole::Master))
        }
    }

    pub async fn set_peer_accessible_ip_with_start_time(
        &self,
        peer_id: &str,
        peer_start_time: i64,
        ip: &str,
    ) -> crate::ClusterResult<()> {
        {
            closed_cluster_manager_unit_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::SetPeerAccessibleIpWithStartTime {
                    peer_id: peer_id.to_string(),
                    peer_start_time,
                    ip: ip.to_string(),
                },
            )
            .await
        }
    }

    pub async fn wait_accessible_self_ip_for_current_start_time(
        &self,
    ) -> crate::ClusterResult<String> {
        {
            closed_cluster_manager_string_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::WaitAccessibleSelfIpForCurrentStartTime,
            )
            .await
        }
    }

    pub async fn fetch_transfer_ready_for_member(
        &self,
        member_id: &str,
    ) -> crate::ClusterResult<Option<CommuTransferReadyInfo>> {
        {
            closed_cluster_manager_optional_transfer_ready_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::FetchTransferReadyForMember {
                    member_id: member_id.to_string(),
                },
            )
            .await
        }
    }

    pub async fn publish_self_transfer_ready(
        &self,
        backend_epoch: u64,
    ) -> crate::ClusterResult<CommuTransferReadyInfo> {
        {
            closed_cluster_manager_transfer_ready_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::PublishSelfTransferReady { backend_epoch },
            )
            .await
        }
    }

    pub async fn set_self_transfer_backend_epoch(
        &self,
        backend_epoch: u64,
    ) -> crate::ClusterResult<()> {
        {
            closed_cluster_manager_unit_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::SetSelfTransferBackendEpoch { backend_epoch },
            )
            .await?;
            let self_info = closed_cluster_manager_member_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::GetSelfInfo,
            )
            .await?;
            refresh_closed_self_info_cache(&self.closed, self_info);
            Ok(())
        }
    }

    pub async fn clear_self_transfer_backend_epoch(&self) -> crate::ClusterResult<()> {
        {
            closed_cluster_manager_unit_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::ClearSelfTransferBackendEpoch,
            )
            .await?;
            let self_info = closed_cluster_manager_member_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::GetSelfInfo,
            )
            .await?;
            refresh_closed_self_info_cache(&self.closed, self_info);
            Ok(())
        }
    }

    pub async fn set_self_share_group_binding(
        &self,
        owner_ref: crate::ShareGroupOwnerRef,
    ) -> crate::ClusterResult<()> {
        {
            closed_cluster_manager_unit_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::SetSelfShareGroupBinding { owner_ref },
            )
            .await?;
            let self_info = closed_cluster_manager_member_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::GetSelfInfo,
            )
            .await?;
            refresh_closed_self_info_cache(&self.closed, self_info);
            Ok(())
        }
    }

    pub async fn set_self_sub_cluster(
        &self,
        sub_cluster: Option<String>,
    ) -> crate::ClusterResult<()> {
        {
            closed_cluster_manager_unit_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::SetSelfSubCluster { sub_cluster },
            )
            .await?;
            let self_info = closed_cluster_manager_member_call(
                self.closed.handle,
                ClosedRuntimeClusterManagerCall::GetSelfInfo,
            )
            .await?;
            refresh_closed_self_info_cache(&self.closed, self_info);
            Ok(())
        }
    }

    pub fn try_report_transfer_engine_route(
        &self,
        from: CommuNodeIDString,
        to: CommuNodeIDString,
        record: CommuTransferLinkRecord,
    ) -> crate::ClusterResult<()> {
        {
            self.transfer_link_writer_handle()
                .try_report_p2p(from.clone(), to.clone(), record)?;
            self.transfer_link_writer_handle()
                .try_report_te(from, to, record)
        }
    }

    pub fn try_report_transfer_link_p2p(
        &self,
        from: CommuNodeIDString,
        to: CommuNodeIDString,
        record: CommuTransferLinkRecord,
    ) -> crate::ClusterResult<()> {
        {
            self.transfer_link_writer_handle()
                .try_report_p2p(from, to, record)
        }
    }

    pub fn try_report_transfer_link_te(
        &self,
        from: CommuNodeIDString,
        to: CommuNodeIDString,
        record: CommuTransferLinkRecord,
    ) -> crate::ClusterResult<()> {
        {
            self.transfer_link_writer_handle()
                .try_report_te(from, to, record)
        }
    }

    #[doc(hidden)]
    pub fn lease_id_for_test(&self) -> i64 {
        { -1 }
    }

    #[doc(hidden)]
    pub fn is_watching_for_test(&self) -> bool {
        { self.is_watching() }
    }

    #[doc(hidden)]
    pub fn is_lease_keepalive_running_for_test(&self) -> bool {
        { false }
    }
}

#[async_trait]
impl LogicalModule for ClusterManager {
    type View = ClusterManagerView;
    type NewArg = ClusterManagerNewArg;
    type Error = crate::ClusterError;

    fn name(&self) -> &str {
        "ClusterManager"
    }

    fn attach_view(&self, view: Self::View) {
        ClusterManager::attach_view(self, view);
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        {
            let _ = self.leave_cluster().await;
            match drop_runtime_handle(self.closed.handle).await {
                Ok(()) => Ok(()),
                Err(error) if is_live_dependent_drop_error(&error) => {
                    tracing::info!(
                        handle_raw = self.closed.handle.raw,
                        "deferring closed ClusterManager handle drop until runtime dependents drain"
                    );
                    spawn_deferred_drop_runtime_handle(
                        self.closed.handle,
                        "cluster_manager_shutdown_deferred",
                    );
                    Ok(())
                }
                Err(error) => Err(cluster_manager_closed_sdk_error(error)),
            }
        }
    }
}

pub use fluxon_commu_contract::cluster_manager::*;
