use super::DeleteTargetMember;
use crate::client_kv_api::msg_pack::ExternalInvalidateWeakIndexReq;
use crate::client_kv_api::{ClientKvApiView, ClientKvApiViewTrait, ExternalHoldingGetInfo};
use crate::cluster_manager::app_logic_ext::ClusterManagerAppLogicExt;
use crate::external_client_api::{ExternalClientApiView, ExternalClientApiViewTrait};
use crate::master_kv_router::delete::DeleteKeyInfo;
use crate::master_kv_router::msg_pack::{
    BatchDeleteAckReq, BatchDeleteClientKvMetaCacheReq, DeleteAckItem, DeleteClientKvMetaCacheItem,
};
use crate::master_kv_router::{MasterKvRouterView, OwnerHoldingGetInfo};
use crate::p2p::msg_pack::{MsgPack, RPCCaller};
use crate::rpcresp_kvresult_convert::msg_and_error;
use async_trait::async_trait;
use dashmap::DashMap;
use fluxon_framework_compiled::shutdown::{ShutdownPoller, ShutdownWaiter};
use fluxon_framework_compiled::upgrade_view_guard::UpgradeViewGuard;
use std::collections::HashSet;
use std::hash::Hash;
use std::time::Duration;

const EXTERNAL_DELETE_ACK_TIMEOUT_SECS: u64 = 5;

pub type OwnerDeleteAckItem = DeleteAckItem;

/// Spawn an async task only if the framework/view is still running.
/// Returns true if the task was spawned, false if skipped due to shutdown.

/// Unified helper for drop-time ACK sending patterns that require:
/// - verifying the view can be upgraded (liveness of underlying framework view)
/// - skipping when system is shutting down
/// - spawning the actual async task when runnable
// Trait-based drop-ack pattern
pub trait MemholderDropAck {
    type View: Clone + Send + Sync + 'static;
    type Guard: Send + 'static;
    fn view(&self) -> &Self::View;
    fn try_upgrade(v: &Self::View) -> Option<Self::Guard>;
    fn is_running(v: &Self::View) -> bool;
    fn ack_future(
        &self,
        guard: Self::Guard,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;
    fn on_view_dropped(&self);
    fn on_skip_shutdown(&self);
    fn run_drop_ack(&self) { /* default no-op; implementers can override to spawn */
    }
}

pub struct OwnerDeleteAckCtx {
    pub view: ClientKvApiView,
    pub key: String,
    pub holder_id: u64,
}

impl MemholderDropAck for OwnerDeleteAckCtx {
    type View = ClientKvApiView;
    type Guard = UpgradeViewGuard<dyn ClientKvApiViewTrait>;
    fn view(&self) -> &Self::View {
        &self.view
    }
    fn try_upgrade(v: &Self::View) -> Option<Self::Guard> {
        v.try_upgrade()
    }
    fn is_running(v: &Self::View) -> bool {
        v.register_shutdown_poller().is_running()
    }
    fn ack_future(
        &self,
        guard: Self::Guard,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        let v = self.view.clone();
        let key = self.key.clone();
        let holder_id = self.holder_id;
        Box::pin(async move {
            let _keep = guard;
            // Read node_id only after guard is held to ensure view is valid
            let node_id = v.cluster_manager().get_self_info().id;
            if let Err(err) = v
                .client_kv_api()
                .inner()
                .delete_ack_batch
                .sender()
                .send(OwnerDeleteAckItem {
                    key,
                    client_id: node_id,
                    holder_id,
                })
                .await
            {
                tracing::warn!(
                    "Failed to enqueue delete_ack batch item for holder_id {}: {}",
                    holder_id,
                    err
                );
            }
        })
    }
    fn on_view_dropped(&self) {
        tracing::warn!(
            "ClientKvApiView has been dropped, cannot send delete_ack for key '{}', holder_id {}.",
            self.key,
            self.holder_id
        );
    }
    fn on_skip_shutdown(&self) {
        tracing::info!(
            "Skipping delete_ack for key={}, holder_id={} due to shutdown",
            self.key,
            self.holder_id
        );
    }
    fn run_drop_ack(&self) {
        let v = self.view().clone();
        let Some(g) = <Self as MemholderDropAck>::try_upgrade(&v) else {
            self.on_view_dropped();
            return;
        };
        if !<Self as MemholderDropAck>::is_running(&v) {
            self.on_skip_shutdown();
            return;
        }
        let fut = self.ack_future(g);
        let _ = v.spawn("memholder_drop_ack_owner", async move { fut.await });
    }
}

pub struct ExternalDeleteAckCtx {
    pub view: ExternalClientApiView,
    pub key: String,
    pub external_client_id: String,
    pub holder_id: u64,
    pub started_time: i64,
}

impl MemholderDropAck for ExternalDeleteAckCtx {
    type View = ExternalClientApiView;
    type Guard = UpgradeViewGuard<dyn ExternalClientApiViewTrait>;
    fn view(&self) -> &Self::View {
        &self.view
    }
    fn try_upgrade(v: &Self::View) -> Option<Self::Guard> {
        v.try_upgrade()
    }
    fn is_running(v: &Self::View) -> bool {
        v.register_shutdown_poller().is_running()
    }
    fn ack_future(
        &self,
        guard: Self::Guard,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        let v = self.view.clone();
        let key = self.key.clone();
        let external_client_id = self.external_client_id.clone();
        let holder_id = self.holder_id;
        let started_time = self.started_time;
        Box::pin(async move {
            let _keep = guard;
            // Best-effort drop ACK must not outlive framework shutdown.
            // During step8 teardown, the owner/external peers can already be concurrently exiting,
            // and the ACK RPC can otherwise stall long enough to block task_registry shutdown.
            match tokio::time::timeout(
                Duration::from_secs(EXTERNAL_DELETE_ACK_TIMEOUT_SECS),
                v.external_client_api().inner().send_external_delete_ack(
                    &key,
                    &external_client_id,
                    holder_id,
                    started_time,
                ),
            )
            .await
            {
                Err(_) => {
                    tracing::warn!(
                        "Timed out sending external_delete_ack for key={}, holder_id={}, external_client_id={} after {}s",
                        key,
                        holder_id,
                        external_client_id,
                        EXTERNAL_DELETE_ACK_TIMEOUT_SECS
                    );
                }
                Ok(Err(e)) => {
                    tracing::warn!(
                        "Failed to send external_delete_ack for key={}, holder_id={}, external_client_id={}: {}",
                        key,
                        holder_id,
                        external_client_id,
                        e
                    );
                }
                Ok(Ok(())) => {
                    tracing::debug!(
                        "Successfully sent external_delete_ack for key={}, holder_id={}, external_client_id={}",
                        key,
                        holder_id,
                        external_client_id
                    );
                }
            }
        })
    }
    fn on_view_dropped(&self) {
        tracing::warn!(
            "ExternalClientApiView has been dropped, cannot send external_delete_ack for key='{}', holder_id {}.",
            self.key,
            self.holder_id
        );
    }
    fn on_skip_shutdown(&self) {
        tracing::info!(
            "Skipping external_delete_ack for key={}, holder_id={} due to shutdown",
            self.key,
            self.holder_id
        );
    }
    fn run_drop_ack(&self) {
        let v = self.view().clone();
        let Some(g) = <Self as MemholderDropAck>::try_upgrade(&v) else {
            self.on_view_dropped();
            return;
        };
        if !<Self as MemholderDropAck>::is_running(&v) {
            self.on_skip_shutdown();
            return;
        }
        let fut = self.ack_future(g);
        let _ = v.spawn("memholder_drop_ack_external", async move { fut.await });
    }
}

/// Canonical composite key: (node_id, holder_id)
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeHolderKey {
    pub node_id: String,
    pub holder_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OwnerDeleteAckTarget;

impl std::fmt::Display for OwnerDeleteAckTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("current_master")
    }
}

impl NodeHolderKey {
    pub fn new(node_id: String, holder_id: u64) -> Self {
        Self { node_id, holder_id }
    }
    #[inline]
    pub fn hold_by_node(&self, node_id: &str) -> bool {
        self.node_id == node_id
    }
}

/// Crate-local helper trait used by the default node-scope cleanup path.
pub(crate) trait HoldByNodeKey {
    fn hold_by_node(&self, node_id: &str) -> bool;
}

impl HoldByNodeKey for NodeHolderKey {
    #[inline]
    fn hold_by_node(&self, node_id: &str) -> bool {
        self.hold_by_node(node_id)
    }
}

/// Owned, flattened inner that stores the generic map only.
pub struct MemholderManagerInner<K, V>
where
    K: Eq + Hash + Send + Sync + Clone + 'static,
{
    holding: DashMap<K, V>,
}

impl<K, V> MemholderManagerInner<K, V>
where
    K: Eq + Hash + Send + Sync + Clone + 'static,
{
    pub fn as_map(&self) -> &DashMap<K, V> {
        &self.holding
    }

    #[inline]
    pub fn cleanup_with<F>(&self, mut predicate: F) -> usize
    where
        F: FnMut(&K) -> bool,
    {
        let mut keys = Vec::new();
        for e in self.holding.iter() {
            if predicate(e.key()) {
                keys.push(e.key().clone());
            }
        }
        let n = keys.len();
        for k in keys {
            self.holding.remove(&k);
        }
        n
    }
}

impl<K, V> Default for MemholderManagerInner<K, V>
where
    K: Eq + Hash + Send + Sync + Clone + 'static,
    V: Send + Sync + 'static,
{
    fn default() -> Self {
        Self {
            holding: DashMap::new(),
        }
    }
}

/// Unified manager trait for memholder authority plus delete delivery control.
pub(crate) trait DeleteShutdownCtx: Clone + Send + Sync + 'static {
    fn delete_shutdown_waiter(&self) -> ShutdownWaiter;
    fn delete_shutdown_poller(&self) -> ShutdownPoller;
}

impl DeleteShutdownCtx for MasterKvRouterView {
    fn delete_shutdown_waiter(&self) -> ShutdownWaiter {
        self.register_shutdown_waiter()
    }

    fn delete_shutdown_poller(&self) -> ShutdownPoller {
        self.register_shutdown_poller()
    }
}

impl DeleteShutdownCtx for ClientKvApiView {
    fn delete_shutdown_waiter(&self) -> ShutdownWaiter {
        self.register_shutdown_waiter()
    }

    fn delete_shutdown_poller(&self) -> ShutdownPoller {
        self.register_shutdown_poller()
    }
}

#[async_trait]
pub(crate) trait MemholderManagerTrait: Sync {
    type Key: Eq + Hash + Send + Sync + Clone + 'static;
    type Value: Send + Sync + 'static;
    type DeleteCtx: DeleteShutdownCtx;
    type DeleteTask: Clone + Send + Sync + 'static;
    type DeleteTarget: Eq + Hash + Clone + std::fmt::Display + Send + Sync + 'static;

    const DELETE_SUBMIT_QUEUE_CAPACITY: usize;
    const DELETE_TARGET_QUEUE_CAPACITY: usize;
    const DELETE_MERGE_WINDOW_MILLIS: u64;
    const DELETE_RETRY_INTERVAL_MILLIS: u64;

    /// Return the authority map of current memholder holdings.
    fn inner_map(&self) -> &DashMap<Self::Key, Self::Value>;

    /// Resolve the concrete manager instance from the running delete context.
    fn delete_manager(ctx: &Self::DeleteCtx) -> &Self;

    /// Fan a delete task out to the current target generations that should observe it.
    fn collect_delete_targets(
        &self,
        ctx: &Self::DeleteCtx,
        task: &Self::DeleteTask,
    ) -> Vec<Self::DeleteTarget>;

    /// Check whether the captured target generation is still the current member generation.
    fn is_delete_target_alive(&self, ctx: &Self::DeleteCtx, target: &Self::DeleteTarget) -> bool;

    /// Send a merged task batch to one concrete target generation.
    async fn send_delete_tasks(
        &self,
        ctx: &Self::DeleteCtx,
        target: Self::DeleteTarget,
        tasks: Vec<Self::DeleteTask>,
    ) -> Result<(), String>;

    /// Spawn one target worker on the owning view instead of using a detached tokio task.
    fn spawn_delete_target_worker(
        &self,
        ctx: &Self::DeleteCtx,
        target: &Self::DeleteTarget,
        fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    );

    /// Allow selected managers to carry an explicit shutdown signal in the queue.
    fn is_delete_shutdown_task(_task: &Self::DeleteTask) -> bool {
        false
    }

    #[inline]
    fn insert(&self, key: Self::Key, value: Self::Value) {
        self.inner_map().insert(key, value);
    }

    #[inline]
    fn remove(&self, key: &Self::Key) -> Option<Self::Value> {
        self.inner_map().remove(key).map(|(_k, v)| v)
    }

    fn cleanup_node(&self, node_id: &str) -> usize
    where
        Self::Key: HoldByNodeKey,
    {
        // 收集后删除，避免持有引用期间修改
        let mut keys = Vec::new();
        for e in self.inner_map().iter() {
            if HoldByNodeKey::hold_by_node(e.key(), node_id) {
                keys.push(e.key().clone());
            }
        }
        let mut removed = 0usize;
        for k in keys {
            if self
                .inner_map()
                .remove_if(&k, |kk, _| HoldByNodeKey::hold_by_node(kk, node_id))
                .is_some()
            {
                removed += 1;
            }
        }
        removed
    }

    #[inline]
    fn total(&self) -> usize {
        self.inner_map().len()
    }

    #[inline]
    fn inner(&self) -> &DashMap<Self::Key, Self::Value> {
        self.inner_map()
    }
}

/// 具体 mgr：绑定 inner + trait，对外固定能力；无需 new，使用 Default。
pub struct MasterOwnerMemMgr {
    inner: MemholderManagerInner<NodeHolderKey, OwnerHoldingGetInfo>,
}

#[async_trait]
impl MemholderManagerTrait for MasterOwnerMemMgr {
    type Key = NodeHolderKey;
    type Value = OwnerHoldingGetInfo;
    type DeleteCtx = MasterKvRouterView;
    type DeleteTask = DeleteKeyInfo;
    type DeleteTarget = DeleteTargetMember;

    const DELETE_SUBMIT_QUEUE_CAPACITY: usize = 1000;
    const DELETE_TARGET_QUEUE_CAPACITY: usize = 1000;
    const DELETE_MERGE_WINDOW_MILLIS: u64 = 1000;
    const DELETE_RETRY_INTERVAL_MILLIS: u64 = 1000;

    #[inline]
    fn inner_map(&self) -> &DashMap<Self::Key, Self::Value> {
        self.inner.as_map()
    }

    fn delete_manager(ctx: &Self::DeleteCtx) -> &Self {
        &ctx.master_kv_router().inner().get_holding
    }

    fn collect_delete_targets(
        &self,
        ctx: &Self::DeleteCtx,
        task: &Self::DeleteTask,
    ) -> Vec<Self::DeleteTarget> {
        let DeleteKeyInfo::Key {
            nodes_kv_route_info,
            ..
        } = task
        else {
            return Vec::new();
        };

        let mut targets = Vec::new();
        let node_replicas = nodes_kv_route_info.node_replicas.read();
        for (node_id, replicas) in node_replicas.iter() {
            if replicas.tomb_tag.is_tomb() || replicas.memory.is_none() {
                continue;
            }
            let Some(member) = ctx
                .cluster_manager()
                .get_member_info_cached(node_id.as_ref())
            else {
                continue;
            };
            targets.push(DeleteTargetMember::new(
                node_id.to_string(),
                member.node_start_time,
            ));
        }
        targets
    }

    fn is_delete_target_alive(&self, ctx: &Self::DeleteCtx, target: &Self::DeleteTarget) -> bool {
        ctx.cluster_manager()
            .get_member_info_cached(&target.node_id)
            .is_some_and(|member| member.node_start_time == target.node_start_time)
    }

    async fn send_delete_tasks(
        &self,
        ctx: &Self::DeleteCtx,
        target: Self::DeleteTarget,
        tasks: Vec<Self::DeleteTask>,
    ) -> Result<(), String> {
        let mut dedupe = HashSet::new();
        let mut delete_items = Vec::new();

        for task in tasks.iter() {
            let DeleteKeyInfo::Key {
                key,
                nodes_kv_route_info,
            } = task
            else {
                continue;
            };

            let node_replicas = nodes_kv_route_info.node_replicas.read();
            let Some(replicas) = node_replicas.get(target.node_id.as_str()) else {
                continue;
            };
            if replicas.tomb_tag.is_tomb() || replicas.memory.is_none() {
                continue;
            }

            let dedupe_key = (
                key.clone(),
                nodes_kv_route_info.put_id.0,
                nodes_kv_route_info.put_id.1,
            );
            if !dedupe.insert(dedupe_key.clone()) {
                continue;
            }

            delete_items.push(DeleteClientKvMetaCacheItem {
                key: dedupe_key.0,
                put_time_ms: dedupe_key.1,
                put_version: dedupe_key.2,
            });
        }

        if delete_items.is_empty() {
            return Ok(());
        }

        let rpc_caller = RPCCaller::<BatchDeleteClientKvMetaCacheReq>::new();
        rpc_caller.regist(ctx.p2p_module());
        let req = MsgPack {
            serialize_part: BatchDeleteClientKvMetaCacheReq { delete_items },
            raw_bytes: Vec::new(),
        };

        let resp = rpc_caller
            .call(
                ctx.p2p_module(),
                target.node_id.clone().into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                0,
            )
            .await
            .map_err(|err| format!("{err:?}"))?;

        if resp.serialize_part.error_code != msg_and_error::OK {
            return Err(format!(
                "code={} error={}",
                resp.serialize_part.error_code, resp.serialize_part.error_json
            ));
        }

        Ok(())
    }

    fn spawn_delete_target_worker(
        &self,
        ctx: &Self::DeleteCtx,
        target: &Self::DeleteTarget,
        fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    ) {
        let view = ctx.clone();
        let target_label = target.to_string();
        let _ = view.spawn("memholder_mgmt_delete_master_target_worker", async move {
            tracing::debug!("Start master delete worker for target {}", target_label);
            fut.await;
        });
    }

    fn is_delete_shutdown_task(task: &Self::DeleteTask) -> bool {
        matches!(task, DeleteKeyInfo::Shutdown)
    }
}

impl Default for MasterOwnerMemMgr {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

pub struct OwnerDeleteAckMemMgr {
    inner: MemholderManagerInner<NodeHolderKey, ()>,
}

impl Default for OwnerDeleteAckMemMgr {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

#[async_trait]
impl MemholderManagerTrait for OwnerDeleteAckMemMgr {
    type Key = NodeHolderKey;
    type Value = ();
    type DeleteCtx = ClientKvApiView;
    type DeleteTask = OwnerDeleteAckItem;
    type DeleteTarget = OwnerDeleteAckTarget;

    const DELETE_SUBMIT_QUEUE_CAPACITY: usize = 1000;
    const DELETE_TARGET_QUEUE_CAPACITY: usize = 1000;
    const DELETE_MERGE_WINDOW_MILLIS: u64 = 10;
    const DELETE_RETRY_INTERVAL_MILLIS: u64 = 200;

    #[inline]
    fn inner_map(&self) -> &DashMap<Self::Key, Self::Value> {
        self.inner.as_map()
    }

    fn delete_manager(ctx: &Self::DeleteCtx) -> &Self {
        &ctx.client_kv_api().inner().owner_delete_ack_mgr
    }

    fn collect_delete_targets(
        &self,
        _ctx: &Self::DeleteCtx,
        _task: &Self::DeleteTask,
    ) -> Vec<Self::DeleteTarget> {
        vec![OwnerDeleteAckTarget]
    }

    fn is_delete_target_alive(&self, ctx: &Self::DeleteCtx, _target: &Self::DeleteTarget) -> bool {
        ctx.register_shutdown_poller().is_running()
    }

    async fn send_delete_tasks(
        &self,
        ctx: &Self::DeleteCtx,
        _target: Self::DeleteTarget,
        tasks: Vec<Self::DeleteTask>,
    ) -> Result<(), String> {
        let mut dedupe = HashSet::new();
        let mut delete_acks = Vec::new();
        for task in tasks {
            let dedupe_key = NodeHolderKey::new(task.client_id.clone(), task.holder_id);
            if !dedupe.insert(dedupe_key) {
                continue;
            }
            delete_acks.push(task);
        }

        if delete_acks.is_empty() {
            return Ok(());
        }

        let master_node_id = ctx
            .cluster_manager()
            .find_or_wait_master_node()
            .await
            .map_err(|err| err.to_string())?;

        let rpc_caller = RPCCaller::<BatchDeleteAckReq>::new();
        rpc_caller.regist(ctx.p2p_module());
        let req = MsgPack {
            serialize_part: BatchDeleteAckReq { delete_acks },
            raw_bytes: Vec::new(),
        };

        let resp = rpc_caller
            .call(
                ctx.p2p_module(),
                master_node_id.into(),
                req,
                Some(Duration::from_secs(60)),
                0,
            )
            .await
            .map_err(|err| format!("{err:?}"))?;

        if resp.serialize_part.error_code != msg_and_error::OK {
            return Err(format!(
                "code={} error={}",
                resp.serialize_part.error_code, resp.serialize_part.error_json
            ));
        }

        Ok(())
    }

    fn spawn_delete_target_worker(
        &self,
        ctx: &Self::DeleteCtx,
        target: &Self::DeleteTarget,
        fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    ) {
        let view = ctx.clone();
        let target_label = target.to_string();
        let _ = view.spawn("memholder_mgmt_delete_ack_target_worker", async move {
            tracing::debug!("Start delete-ack batch worker for target {}", target_label);
            fut.await;
        });
    }
}

pub struct OwnerExternalMemMgr {
    inner: MemholderManagerInner<NodeHolderKey, ExternalHoldingGetInfo>,
}

impl Default for OwnerExternalMemMgr {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

#[async_trait]
impl MemholderManagerTrait for OwnerExternalMemMgr {
    type Key = NodeHolderKey;
    type Value = ExternalHoldingGetInfo;
    type DeleteCtx = ClientKvApiView;
    type DeleteTask = DeleteClientKvMetaCacheItem;
    type DeleteTarget = DeleteTargetMember;

    const DELETE_SUBMIT_QUEUE_CAPACITY: usize = 1000;
    const DELETE_TARGET_QUEUE_CAPACITY: usize = 1000;
    const DELETE_MERGE_WINDOW_MILLIS: u64 = 1000;
    const DELETE_RETRY_INTERVAL_MILLIS: u64 = 1000;

    #[inline]
    fn inner_map(&self) -> &DashMap<Self::Key, Self::Value> {
        self.inner.as_map()
    }

    fn delete_manager(ctx: &Self::DeleteCtx) -> &Self {
        &ctx.client_kv_api().inner().external_get_holding
    }

    fn collect_delete_targets(
        &self,
        ctx: &Self::DeleteCtx,
        task: &Self::DeleteTask,
    ) -> Vec<Self::DeleteTarget> {
        let mut targets = HashSet::new();
        for entry in self.inner().iter() {
            let holding = entry.value();
            if holding.key != task.key {
                continue;
            }
            let Some(member) = ctx
                .cluster_manager()
                .get_member_info_cached(&holding.req_node_id)
            else {
                continue;
            };
            targets.insert(DeleteTargetMember::new(
                holding.req_node_id.clone(),
                member.node_start_time,
            ));
        }
        targets.into_iter().collect()
    }

    fn is_delete_target_alive(&self, ctx: &Self::DeleteCtx, target: &Self::DeleteTarget) -> bool {
        ctx.cluster_manager()
            .get_member_info_cached(&target.node_id)
            .is_some_and(|member| member.node_start_time == target.node_start_time)
    }

    async fn send_delete_tasks(
        &self,
        ctx: &Self::DeleteCtx,
        target: Self::DeleteTarget,
        tasks: Vec<Self::DeleteTask>,
    ) -> Result<(), String> {
        let mut keys: Vec<String> = tasks.into_iter().map(|task| task.key).collect();
        keys.sort();
        keys.dedup();

        if keys.is_empty() {
            return Ok(());
        }

        let rpc_caller = RPCCaller::<ExternalInvalidateWeakIndexReq>::new();
        rpc_caller.regist(ctx.p2p_module());
        let req = MsgPack {
            serialize_part: ExternalInvalidateWeakIndexReq { keys },
            raw_bytes: Vec::new(),
        };

        let resp = rpc_caller
            .call(
                ctx.p2p_module(),
                target.node_id.clone().into(),
                req,
                None,
                0,
            )
            .await
            .map_err(|err| format!("{err:?}"))?;

        if resp.serialize_part.error_code != msg_and_error::OK {
            return Err(format!(
                "code={} error={}",
                resp.serialize_part.error_code, resp.serialize_part.error_json
            ));
        }

        Ok(())
    }

    fn spawn_delete_target_worker(
        &self,
        ctx: &Self::DeleteCtx,
        target: &Self::DeleteTarget,
        fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    ) {
        let view = ctx.clone();
        let target_label = target.to_string();
        let _ = view.spawn("memholder_mgmt_delete_external_target_worker", async move {
            tracing::debug!("Start external delete worker for target {}", target_label);
            fut.await;
        });
    }
}
