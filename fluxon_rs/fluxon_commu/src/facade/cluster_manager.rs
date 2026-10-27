use std::collections::HashMap;
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;

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
use etcd_client::Client as EtcdClient;
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
use limit_thirdparty::tokio::sync::{abroadcast, ampsc};
use tokio::sync::{Mutex, OnceCell, watch};
use tokio::task::JoinHandle;

use crate::closed_sdk::{
    RuntimeHandleRelease, cluster_manager_call, construct_cluster_manager_handle,
    current_cluster_manager_self_rdma_resolved_config, drop_runtime_handle,
    recv_cluster_event_stream, recv_cluster_rdma_resolved_config_stream,
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
    release: RuntimeHandleRelease,
    self_member_id: String,
    cluster_name: String,
    etcd_endpoints: Vec<String>,
    observe_handle: std::sync::OnceLock<ObserveHandle>,
    ipc_bandwidth_attributor_handle: std::sync::OnceLock<IpcBandwidthAttributorHandle>,
    self_info: Arc<RwLock<crate::ClusterMember>>,
    members: Arc<RwLock<HashMap<String, crate::ClusterMember>>>,
    prev_members: Arc<RwLock<HashMap<String, crate::ClusterMember>>>,
    is_watching: AtomicBool,
    event_tx: abroadcast::Sender<crate::ClusterEvent>,
    self_rdma_resolved_tx: watch::Sender<CommuMemberRdmaResolvedConfig>,
    event_mirror_ready: OnceCell<()>,
    snapshot_reconciler_started: AtomicBool,
    authoritative_member_client: Arc<Mutex<EtcdClient>>,
    transfer_link_writer: CommuTransferLinkEtcdWriterHandle,
    transfer_link_p2p_snapshot_source: CommuTransferLinkP2pSnapshotSource,
}

impl ClosedClusterManagerRuntime {
    async fn construct(arg: ClusterManagerNewArg) -> crate::ClusterResult<Self> {
        let etcd_endpoints = arg.etcd_endpoints.clone();
        let handle = construct_cluster_manager_handle(arg)
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
        let transfer_link_p2p_snapshot_source = CommuTransferLinkP2pSnapshotSource::new(
            connect_transfer_link_client(&etcd_endpoints).await?,
            format!("/{}/transfer_link/p2p", cluster_name),
        );
        // ClosedRuntimeClusterManagerCall::GetMembers reflects the closed facade's
        // own event cache.  It cannot repair that same cache after the event stream
        // has missed a join.  Keep an independent etcd client for an authoritative
        // membership snapshot instead.
        let authoritative_member_client = Arc::new(Mutex::new(
            connect_transfer_link_client(&etcd_endpoints).await?,
        ));
        let transfer_link_writer = make_runtime_transfer_link_writer(handle);
        let runtime = Self {
            handle,
            release: RuntimeHandleRelease::new(),
            self_member_id,
            cluster_name,
            etcd_endpoints,
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
            event_mirror_ready: OnceCell::new(),
            snapshot_reconciler_started: AtomicBool::new(false),
            authoritative_member_client,
            transfer_link_writer,
            transfer_link_p2p_snapshot_source,
        };
        runtime.spawn_self_rdma_mirror();
        Ok(runtime)
    }

    fn spawn_snapshot_reconciler(&self) {
        const SNAPSHOT_RECONCILE_INTERVAL: Duration = Duration::from_secs(2);

        if self
            .snapshot_reconciler_started
            .swap(true, Ordering::AcqRel)
        {
            return;
        }

        let self_member_id = self.self_member_id.clone();
        let cluster_name = self.cluster_name.clone();
        let members = Arc::downgrade(&self.members);
        let prev_members = Arc::downgrade(&self.prev_members);
        let self_info = Arc::downgrade(&self.self_info);
        let authoritative_member_client = Arc::clone(&self.authoritative_member_client);
        let event_tx = self.event_tx.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(SNAPSHOT_RECONCILE_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Construction already seeded the facade cache.  The first periodic
            // snapshot is intentionally delayed so it cannot race that seed.
            interval.tick().await;
            let mut failure_reported = false;

            loop {
                interval.tick().await;
                let (Some(members), Some(prev_members), Some(self_info)) = (
                    members.upgrade(),
                    prev_members.upgrade(),
                    self_info.upgrade(),
                ) else {
                    break;
                };

                let snapshot = {
                    let mut client = authoritative_member_client.lock().await;
                    fetch_authoritative_cluster_member_snapshot(&mut client, &cluster_name).await
                };
                match snapshot {
                    Ok(snapshot) => {
                        if failure_reported {
                            tracing::info!(
                                "closed ClusterManager authoritative snapshot reconciliation recovered"
                            );
                            failure_reported = false;
                        }
                        let events = reconcile_cluster_member_snapshot(
                            &members,
                            &prev_members,
                            &self_info,
                            &self_member_id,
                            snapshot,
                        );
                        if !events.is_empty() {
                            tracing::warn!(
                                repaired_events = events.len(),
                                "closed ClusterManager event mirror drift repaired from authoritative snapshot"
                            );
                            for event in events {
                                let _ = event_tx.send(event);
                            }
                        }
                    }
                    Err(error) => {
                        if !failure_reported {
                            tracing::warn!(
                                %error,
                                "closed ClusterManager authoritative snapshot reconciliation failed"
                            );
                            failure_reported = true;
                        }
                    }
                }
            }
        });
    }

    async fn start_event_mirror_for_init_dag(&self) -> crate::ClusterResult<()> {
        self.event_mirror_ready
            .get_or_try_init(|| async {
                let stream_handle = subscribe_cluster_manager_events(self.handle)
                    .await
                    .map_err(cluster_manager_closed_sdk_error)?;
                self.spawn_event_mirror(stream_handle);
                Ok(())
            })
            .await
            .map(|_| ())
    }

    fn spawn_event_mirror(&self, stream_handle: ClosedRuntimeHandle) {
        let handle = self.handle;
        let event_tx = self.event_tx.clone();
        let self_member_id = self.self_member_id.clone();
        let members = Arc::clone(&self.members);
        let prev_members = Arc::clone(&self.prev_members);
        let self_info = Arc::clone(&self.self_info);
        tokio::spawn(async move {
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

async fn connect_transfer_link_client(
    etcd_endpoints: &[String],
) -> crate::ClusterResult<EtcdClient> {
    EtcdClient::connect(etcd_endpoints.to_vec(), None)
        .await
        .map_err(|error| crate::ClusterError::EtcdConnection {
            endpoints: etcd_endpoints.to_vec(),
            error: error.to_string(),
        })
}

fn decode_authoritative_member_kv(
    prefix: &str,
    key: &[u8],
    value: &[u8],
) -> Result<Option<crate::ClusterMember>, String> {
    let key =
        std::str::from_utf8(key).map_err(|error| format!("member key is not UTF-8: {error}"))?;
    let Some(member_id) = key.strip_prefix(prefix) else {
        return Ok(None);
    };
    // Only the direct children of the base-members prefix are authoritative
    // membership records.  Dynamic member extensions live under another prefix.
    if member_id.is_empty() || member_id.contains('/') {
        return Ok(None);
    }
    let member: crate::ClusterMember = serde_json::from_slice(value)
        .map_err(|error| format!("failed to decode authoritative member {member_id}: {error}"))?;
    if member.id != member_id {
        return Err(format!(
            "authoritative member key/value identity mismatch: key={member_id} value={}",
            member.id
        ));
    }
    Ok(Some(member))
}

async fn fetch_authoritative_cluster_member_snapshot(
    client: &mut EtcdClient,
    cluster_name: &str,
) -> crate::ClusterResult<Vec<crate::ClusterMember>> {
    let member_prefix = format!("{}/", crate::cluster_member_base_prefix(cluster_name));
    let mut members = Vec::new();
    crate::scan_etcd_prefix_paginated(client, &member_prefix, |key, value| {
        if let Some(member) = decode_authoritative_member_kv(&member_prefix, key, value)? {
            members.push(member);
        }
        Ok::<crate::EtcdPrefixScanAction, String>(crate::EtcdPrefixScanAction::Continue)
    })
    .await
    .map_err(|error| {
        crate::ClusterError::MemberSync(format!(
            "authoritative membership scan failed for cluster {cluster_name}: {error}"
        ))
    })?;

    Ok(members)
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

fn reconcile_cluster_member_snapshot(
    members: &Arc<RwLock<HashMap<String, crate::ClusterMember>>>,
    prev_members: &Arc<RwLock<HashMap<String, crate::ClusterMember>>>,
    self_info: &Arc<RwLock<crate::ClusterMember>>,
    self_member_id: &str,
    snapshot: Vec<crate::ClusterMember>,
) -> Vec<crate::ClusterEvent> {
    let current_self = self_info.read().expect("self-info cache poisoned").clone();
    let mut next: HashMap<String, crate::ClusterMember> = snapshot
        .into_iter()
        .map(|member| (member.id.to_string(), member))
        .collect();
    // Some closed runtimes omit the caller from GetMembers.  The facade has
    // always promised that get_members() contains self, so reconciliation must
    // preserve that contract instead of publishing a synthetic self-leave.
    next.entry(self_member_id.to_string())
        .or_insert(current_self);

    let mut members_guard = members.write().expect("members cache poisoned");
    // Base membership is the authority for presence and generation.  The closed
    // event cache may already carry same-generation dynamic extension metadata
    // (accessible IP, transfer readiness, share-group binding).  Preserve those
    // fields when etcd's base record does not repeat them, but never carry them
    // across an owner restart.
    for (member_id, member) in &mut next {
        let Some(previous) = members_guard.get(member_id) else {
            continue;
        };
        if previous.node_start_time != member.node_start_time {
            continue;
        }
        for (key, value) in &previous.metadata {
            member
                .metadata
                .entry(key.clone())
                .or_insert_with(|| value.clone());
        }
    }
    let mut events = Vec::new();
    for (member_id, member) in &next {
        match members_guard.get(member_id) {
            None => events.push(crate::ClusterEvent::MemberJoined(member.clone())),
            Some(previous) if previous != member => {
                events.push(crate::ClusterEvent::MemberUpdated(member.clone()))
            }
            Some(_) => {}
        }
    }
    for member_id in members_guard.keys() {
        if !next.contains_key(member_id) {
            events.push(crate::ClusterEvent::MemberLeft(member_id.clone()));
        }
    }

    if events.is_empty() {
        return events;
    }

    let mut prev_guard = prev_members.write().expect("prev-members cache poisoned");
    for event in &events {
        match event {
            crate::ClusterEvent::MemberJoined(member) => {
                prev_guard.remove(member.id.as_str());
            }
            crate::ClusterEvent::MemberUpdated(member) => {
                if let Some(previous) = members_guard.get(member.id.as_str()) {
                    prev_guard.insert(member.id.to_string(), previous.clone());
                }
            }
            crate::ClusterEvent::MemberLeft(member_id) => {
                if let Some(previous) = members_guard.get(member_id) {
                    prev_guard.insert(member_id.clone(), previous.clone());
                }
            }
        }
    }

    let next_self = next.get(self_member_id).cloned();
    *members_guard = next;
    drop(prev_guard);
    drop(members_guard);
    if let Some(next_self) = next_self {
        *self_info.write().expect("self-info cache poisoned") = next_self;
    }
    events
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
            // Install the facade subscriber before the closed runtime joins and starts
            // publishing membership events.  The init DAG does not release
            // `cluster_member_watch_ready` until this method returns, so downstream
            // listeners cannot race an unobserved first snapshot.
            self.closed.start_event_mirror_for_init_dag().await?;
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
            // The closed event stream remains the low-latency path.  This base-member-only
            // reconciliation repairs a stopped/lagged mirror; transfer/P2P readiness is
            // deliberately owned by the P2P tier DAG rather than inferred from etcd keys.
            self.closed.spawn_snapshot_reconciler();
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
        let handle = self.closed.handle;
        self.closed
            .release
            .run(move || async move {
                let _ = closed_cluster_manager_unit_call(
                    handle,
                    ClosedRuntimeClusterManagerCall::LeaveCluster,
                )
                .await;
                drop_runtime_handle(handle).await
            })
            .await
            .map_err(cluster_manager_closed_sdk_error)
    }
}

#[cfg(test)]
mod snapshot_reconcile_tests {
    use super::*;

    fn member(id: &str, node_start_time: i64, port: u16) -> crate::ClusterMember {
        crate::ClusterMember {
            id: id.to_string(),
            addresses: vec!["10.0.0.1".to_string()],
            port: Some(port),
            node_start_time,
            metadata: HashMap::new(),
            sub_cluster: None,
            network: None,
        }
    }

    #[test]
    fn snapshot_reconcile_repairs_missed_join_update_and_leave() {
        let master = member("master", 1, 1000);
        let owner1 = member("owner1", 2, 1001);
        let owner0 = member("owner0", 3, 1002);
        let members = Arc::new(RwLock::new(HashMap::from([
            (master.id.clone(), master.clone()),
            (owner1.id.clone(), owner1.clone()),
        ])));
        let prev_members = Arc::new(RwLock::new(HashMap::new()));
        let self_info = Arc::new(RwLock::new(master.clone()));

        let events = reconcile_cluster_member_snapshot(
            &members,
            &prev_members,
            &self_info,
            &master.id,
            vec![master.clone(), owner1.clone(), owner0.clone()],
        );
        assert_eq!(
            events,
            vec![crate::ClusterEvent::MemberJoined(owner0.clone())]
        );
        assert_eq!(members.read().unwrap().get(&owner0.id), Some(&owner0));

        let mut owner0_updated = owner0.clone();
        owner0_updated.port = Some(2002);
        let events = reconcile_cluster_member_snapshot(
            &members,
            &prev_members,
            &self_info,
            &master.id,
            // Deliberately omit self: reconciliation must preserve it.
            vec![owner0_updated.clone()],
        );
        assert_eq!(events.len(), 2);
        assert!(events.contains(&crate::ClusterEvent::MemberUpdated(owner0_updated.clone())));
        assert!(events.contains(&crate::ClusterEvent::MemberLeft(owner1.id.clone())));
        let members = members.read().unwrap();
        assert_eq!(members.get(&master.id), Some(&master));
        assert_eq!(members.get(&owner0.id), Some(&owner0_updated));
        assert!(!members.contains_key(&owner1.id));
        drop(members);
        let prev_members = prev_members.read().unwrap();
        assert_eq!(prev_members.get(&owner0.id), Some(&owner0));
        assert_eq!(prev_members.get(&owner1.id), Some(&owner1));
    }

    #[test]
    fn authoritative_member_decoder_rejects_identity_mismatch() {
        let prefix = "/fluxon_commu_member_base/cluster-a/members/";
        let owner = member("owner0", 3, 1002);
        let encoded = serde_json::to_vec(&owner).unwrap();
        assert_eq!(
            decode_authoritative_member_kv(
                prefix,
                b"/fluxon_commu_member_base/cluster-a/members/owner0",
                &encoded,
            )
            .unwrap(),
            Some(owner.clone())
        );
        let error = decode_authoritative_member_kv(
            prefix,
            b"/fluxon_commu_member_base/cluster-a/members/owner1",
            &encoded,
        )
        .unwrap_err();
        assert!(error.contains("identity mismatch"));
    }

    #[test]
    fn snapshot_reconcile_preserves_only_same_generation_extensions() {
        let master = member("master", 1, 1000);
        let mut cached_owner = member("owner0", 3, 1002);
        cached_owner
            .metadata
            .insert("accessible_ip".to_string(), "10.0.0.7".to_string());
        let members = Arc::new(RwLock::new(HashMap::from([
            (master.id.clone(), master.clone()),
            (cached_owner.id.clone(), cached_owner.clone()),
        ])));
        let prev_members = Arc::new(RwLock::new(HashMap::new()));
        let self_info = Arc::new(RwLock::new(master.clone()));

        let base_owner = member("owner0", 3, 1002);
        let events = reconcile_cluster_member_snapshot(
            &members,
            &prev_members,
            &self_info,
            &master.id,
            vec![master.clone(), base_owner],
        );
        assert!(events.is_empty());
        assert_eq!(
            members
                .read()
                .unwrap()
                .get("owner0")
                .unwrap()
                .metadata
                .get("accessible_ip")
                .map(String::as_str),
            Some("10.0.0.7")
        );

        let restarted_owner = member("owner0", 4, 1002);
        let events = reconcile_cluster_member_snapshot(
            &members,
            &prev_members,
            &self_info,
            &master.id,
            vec![master.clone(), restarted_owner],
        );
        assert_eq!(events.len(), 1);
        let owner = members.read().unwrap().get("owner0").unwrap().clone();
        assert_eq!(owner.node_start_time, 4);
        assert!(!owner.metadata.contains_key("accessible_ip"));
    }
}

pub use fluxon_commu_contract::cluster_manager::*;
