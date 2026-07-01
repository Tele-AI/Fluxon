use crate::cluster_manager::META_KEY_LOCAL_IPC_ROOT;
use crate::cluster_manager::{
    ClusterEvent, ClusterManager, ClusterManagerAccessTrait, ClusterManagerView, ClusterMember,
    NodeID, NodeRole,
};
use crate::config::{GreptimeOtlpLogConfig, MonitoringConfig, TestSpecConfig};
use crate::master_seg_manager::MasterSegManager;
use crate::master_seg_manager::MasterSegManagerAccessTrait;
use crate::master_seg_manager::one_seg_allocator::OneSegAllocator;
use crate::metrics::MetricsHandle;
use crate::p2p::msg_pack::{MsgPack, RPCCaller, RPCHandler, RPCReq};
use crate::p2p::p2p_module::{P2pModule, P2pModuleAccessTrait, P2pModuleView};
use crate::rpcresp_kvresult_convert::msg_and_error::KvError;
use async_trait::async_trait;
use fluxon_framework::{LogicalModule, define_module};
use fluxon_framework_compiled::shutdown::ShutdownWaiter;
use fluxon_observability::greptime_otlp_log_orchestrator::{
    GreptimeOtlpLogHttpSender, GreptimeOtlpLogProxyReq, GreptimeOtlpLogProxyResp,
    handle_proxy_request as handle_greptime_proxy_request,
};
use fluxon_observability::kv_metrics_actor::KvMetricsActorOwned;
use fluxon_observability::metrics_actor::{
    MetricsActorOwned, MetricsHandle as ObserveMetricsHandle,
};
use fluxon_observability::prom_remote_write_actor::{
    PromRemoteWriteActorOwned, PromRemoteWriteHandle,
};
use fluxon_observability::prom_remote_write_orchestrator::{
    PromRemoteWriteHttpSender, PromRemoteWriteProxyReq, PromRemoteWriteProxyResp,
    handle_proxy_request as handle_prom_remote_write_proxy_request,
};
use fluxon_observability::types::FsMountKind;
use fluxon_util::fs_statvfs::{
    mount_point_for_abs_dir, normalize_abs_dir_label, statvfs_used_total,
};
use limit_thirdparty::tokio;
use serde::{Deserialize, Serialize};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

const PROM_REMOTE_WRITE_USER_AGENT: &str = "fluxon_kv/1.0.0";
const PROM_REMOTE_WRITE_PER_URL_TIMEOUT_SECS: u64 = 10;
pub(crate) const META_KEY_KV_OBSERVE_BROADCAST: &str = "kv_observe_broadcast";

// One tick owns: system sampling + op stat aggregation + registry.gather + remote-write submit.
pub const METRICS_FLUSH_INTERVAL_SECS: u64 = 30;

// This queue bounds memory in case remote-write is slow or proxy path is unavailable.
// Producer generates at most 1 payload per tick, so a short backlog is enough.
const PROM_REMOTE_WRITE_ACTOR_MAX_PENDING_PAYLOADS: usize = 8;

// This queue bounds memory in case producers outpace Prom remote-write.
// Explicit constant to avoid hidden defaults.
const METRICS_ACTOR_MAX_PENDING_MSGS: usize = 4096;
const SHM_FILE_METRICS_MAX_FILES: usize = 64;

#[derive(Debug, Clone)]
struct ShmFileUsageRec {
    file_path_abs: String,
    logical_size_bytes: u64,
    allocated_bytes: u64,
}

fn collect_shm_file_usage(
    shm_dir_abs: &str,
    max_files: usize,
) -> Result<Vec<ShmFileUsageRec>, std::io::Error> {
    let root = Path::new(shm_dir_abs);
    let mut stack: Vec<PathBuf> = vec![root.to_path_buf()];
    let mut out: Vec<ShmFileUsageRec> = Vec::new();

    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir)?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            if !(file_type.is_file() || file_type.is_symlink()) {
                continue;
            }
            let meta = match std::fs::metadata(&path) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if !meta.is_file() {
                continue;
            }
            let file_path_abs = match path.canonicalize() {
                Ok(p) => p.to_string_lossy().into_owned(),
                Err(_) => path.to_string_lossy().into_owned(),
            };
            out.push(ShmFileUsageRec {
                file_path_abs,
                logical_size_bytes: meta.len(),
                allocated_bytes: meta.blocks().saturating_mul(512),
            });
        }
    }

    out.sort_by(|a, b| {
        b.allocated_bytes
            .cmp(&a.allocated_bytes)
            .then_with(|| b.logical_size_bytes.cmp(&a.logical_size_bytes))
            .then_with(|| a.file_path_abs.cmp(&b.file_path_abs))
    });
    if out.len() > max_files {
        out.truncate(max_files);
    }
    Ok(out)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct MasterObserveBroadcast {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prom_remote_write_url: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub otlp_log_api: Option<GreptimeOtlpLogConfig>,
}

impl MasterObserveBroadcast {
    fn from_monitoring_config(config: &MonitoringConfig) -> Option<Self> {
        let broadcast = Self {
            prom_remote_write_url: normalize_remote_write_urls(
                config.prom_remote_write_url.clone(),
            ),
            otlp_log_api: normalize_otlp_log_api(config.otlp_log_api.clone()),
        };
        if broadcast.prom_remote_write_url.is_none() && broadcast.otlp_log_api.is_none() {
            return None;
        }
        Some(broadcast)
    }

    fn remote_write_urls(&self) -> Vec<String> {
        normalize_remote_write_urls(self.prom_remote_write_url.clone()).unwrap_or_default()
    }

    fn otlp_log_api(&self) -> Option<GreptimeOtlpLogConfig> {
        normalize_otlp_log_api(self.otlp_log_api.clone())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct WaitMasterObserveBroadcastOutcome {
    pub master_id: Option<String>,
    pub observe_broadcast: Option<MasterObserveBroadcast>,
}

impl WaitMasterObserveBroadcastOutcome {
    pub(crate) fn remote_write_urls(&self) -> Vec<String> {
        self.observe_broadcast
            .as_ref()
            .map(|broadcast| broadcast.remote_write_urls())
            .unwrap_or_default()
    }

    pub(crate) fn otlp_log_api(&self) -> Option<GreptimeOtlpLogConfig> {
        self.observe_broadcast
            .as_ref()
            .and_then(|broadcast| broadcast.otlp_log_api())
    }
}

fn normalize_remote_write_urls(urls: Option<Vec<String>>) -> Option<Vec<String>> {
    let urls = urls?;
    if urls.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(urls.len());
    for url in urls {
        let trimmed = url.trim();
        if trimmed.is_empty() {
            return None;
        }
        out.push(trimmed.to_string());
    }
    Some(out)
}

fn normalize_otlp_log_api(cfg: Option<GreptimeOtlpLogConfig>) -> Option<GreptimeOtlpLogConfig> {
    let cfg = cfg?;
    if cfg.otlp_endpoint.trim().is_empty() {
        return None;
    }
    Some(cfg)
}

pub(crate) fn serialize_master_observe_broadcast(
    monitoring: Option<&MonitoringConfig>,
) -> Option<String> {
    let monitoring = monitoring?;
    let broadcast = MasterObserveBroadcast::from_monitoring_config(monitoring)?;
    Some(serde_json::to_string(&broadcast).unwrap())
}

fn parse_master_observe_broadcast(member: &ClusterMember) -> Option<MasterObserveBroadcast> {
    let raw = member.metadata.get(META_KEY_KV_OBSERVE_BROADCAST)?;
    match serde_json::from_str::<MasterObserveBroadcast>(raw) {
        Ok(broadcast) => Some(broadcast),
        Err(err) => {
            warn!(
                "Ignoring invalid {} metadata from master {}: {}",
                META_KEY_KV_OBSERVE_BROADCAST, member.id, err
            );
            None
        }
    }
}

fn current_master_observe_broadcast(cm: &ClusterManager) -> WaitMasterObserveBroadcastOutcome {
    match cm.get_master_member() {
        Some(master) => WaitMasterObserveBroadcastOutcome {
            master_id: Some(master.id.clone()),
            observe_broadcast: parse_master_observe_broadcast(&master),
        },
        None => WaitMasterObserveBroadcastOutcome {
            master_id: None,
            observe_broadcast: None,
        },
    }
}

pub(crate) async fn wait_master_observe_broadcast<V>(
    view: &V,
    timeout: Duration,
    warn_every: Duration,
) -> WaitMasterObserveBroadcastOutcome
where
    V: ObserveWaitView,
{
    let mut rx = view.observe_cluster_manager().listen();
    let mut shutdown_waiter = view.observe_shutdown_waiter();
    let start = Instant::now();

    let mut warn_tick = tokio::time::interval(warn_every);
    warn_tick.tick().await;

    let timeout_sleep = tokio::time::sleep(timeout);
    tokio::pin!(timeout_sleep);

    loop {
        let outcome = current_master_observe_broadcast(view.observe_cluster_manager());
        if outcome.master_id.is_some() {
            return outcome;
        }

        tokio::select! {
            _ = &mut timeout_sleep => {
                return current_master_observe_broadcast(view.observe_cluster_manager());
            }
            _ = warn_tick.tick() => {
                warn!(
                    "Waiting for master member record (elapsed_ms={}, timeout_ms={})",
                    start.elapsed().as_millis(),
                    timeout.as_millis()
                );
            }
            ev = rx.recv() => {
                if ev.is_err() {
                    warn!("Cluster event channel closed while waiting for master member record");
                    return WaitMasterObserveBroadcastOutcome {
                        master_id: None,
                        observe_broadcast: None,
                    };
                }
            }
            _ = shutdown_waiter.wait() => {
                warn!("System shutdown while waiting for master member record");
                return WaitMasterObserveBroadcastOutcome {
                    master_id: None,
                    observe_broadcast: None,
                };
            }
        }
    }
}

async fn apply_master_observe_broadcast_state(
    state: &limit_thirdparty::tokio::sync::ARwLock<Option<MasterObserveBroadcast>>,
    prom_handle: &PromRemoteWriteHandle,
    observe_broadcast: Option<MasterObserveBroadcast>,
) {
    let remote_write_urls = observe_broadcast
        .as_ref()
        .map(|broadcast| broadcast.remote_write_urls())
        .unwrap_or_default();
    prom_handle.try_update_remote_write_urls(remote_write_urls);
    *state.write().await = observe_broadcast;
}

fn resolve_node_labels(member: &ClusterMember) -> (String, String) {
    (member.id.clone(), member.node_role().to_string())
}

define_module!(
    MetricReporter,
    (cluster_manager, ClusterManager),
    (p2p, P2pModule),
    (master_seg_manager, MasterSegManager),
    (metric_reporter, MetricReporter)
);

pub(crate) trait ObserveClusterView {
    fn observe_cluster_manager(&self) -> &ClusterManager;
}

pub(crate) trait ObserveP2pView {
    fn observe_p2p_module(&self) -> &P2pModule;
}

pub(crate) trait ObserveWaitView: ObserveClusterView {
    fn observe_shutdown_waiter(&self) -> ShutdownWaiter;
}

impl ObserveClusterView for MetricReporterView {
    fn observe_cluster_manager(&self) -> &ClusterManager {
        self.cluster_manager()
    }
}

impl ObserveClusterView for ClusterManagerView {
    fn observe_cluster_manager(&self) -> &ClusterManager {
        self.cluster_manager()
    }
}

impl ObserveWaitView for MetricReporterView {
    fn observe_shutdown_waiter(&self) -> ShutdownWaiter {
        self.register_shutdown_waiter()
    }
}

impl ObserveWaitView for ClusterManagerView {
    fn observe_shutdown_waiter(&self) -> ShutdownWaiter {
        self.register_shutdown_waiter()
    }
}

impl ObserveP2pView for MetricReporterView {
    fn observe_p2p_module(&self) -> &P2pModule {
        self.p2p_module()
    }
}

impl ObserveP2pView for P2pModuleView {
    fn observe_p2p_module(&self) -> &P2pModule {
        self.p2p_module()
    }
}

fn is_observe_proxy_allowed(member: &ClusterMember) -> bool {
    matches!(member.node_role(), NodeRole::Master)
        || member
            .metadata
            .get("p2p_relay")
            .map(|v| v == "true")
            .unwrap_or(false)
}

fn pick_reachable_relay_or_master(cm: &ClusterManager, p2p: &P2pModule) -> Option<NodeID> {
    let self_id = cm.get_self_info().id.clone();

    let mut relay_ids = cm
        .get_members()
        .into_iter()
        .filter(|member| {
            member
                .metadata
                .get("p2p_relay")
                .map(|v| v == "true")
                .unwrap_or(false)
        })
        .map(|member| NodeID::from(member.id))
        .collect::<Vec<_>>();
    relay_ids.sort();
    for relay_id in relay_ids {
        if relay_id == self_id {
            continue;
        }
        if p2p.peek_p2p_link_state(&relay_id)
            == crate::cluster_manager::TransferLinkP2pState::Direct
        {
            return Some(relay_id);
        }
    }

    cm.get_master_member()
        .map(|member| NodeID::from(member.id))
        .filter(|master_id| *master_id != self_id)
}

#[derive(Clone)]
pub(crate) struct ObserveProxyPicker<C, P> {
    cluster_view: C,
    p2p_view: P,
}

impl<C, P> ObserveProxyPicker<C, P> {
    pub(crate) fn new(cluster_view: C, p2p_view: P) -> Self {
        Self {
            cluster_view,
            p2p_view,
        }
    }
}

impl<C, P> ObserveProxyPicker<C, P>
where
    C: ObserveClusterView,
    P: ObserveP2pView,
{
    fn pick_proxy_node_inner(&self) -> Option<NodeID> {
        pick_reachable_relay_or_master(
            self.cluster_view.observe_cluster_manager(),
            self.p2p_view.observe_p2p_module(),
        )
    }
}

impl<C, P> fluxon_observability::prom_remote_write_orchestrator::PromRemoteWriteProxyPicker<NodeID>
    for ObserveProxyPicker<C, P>
where
    C: ObserveClusterView + Send + Sync,
    P: ObserveP2pView + Send + Sync,
{
    fn pick_proxy_node(&self) -> Option<NodeID> {
        self.pick_proxy_node_inner()
    }
}

impl<C, P> fluxon_observability::greptime_otlp_log_orchestrator::GreptimeOtlpLogProxyPicker<NodeID>
    for ObserveProxyPicker<C, P>
where
    C: ObserveClusterView + Send + Sync,
    P: ObserveP2pView + Send + Sync,
{
    fn pick_proxy_node(&self) -> Option<NodeID> {
        self.pick_proxy_node_inner()
    }
}

pub(crate) struct ObserveProxyCaller<V, Req: RPCReq> {
    view: V,
    rpc: RPCCaller<Req>,
}

impl<V, Req> Clone for ObserveProxyCaller<V, Req>
where
    V: Clone,
    Req: RPCReq,
{
    fn clone(&self) -> Self {
        Self {
            view: self.view.clone(),
            rpc: RPCCaller::new(),
        }
    }
}

impl<V, Req> ObserveProxyCaller<V, Req>
where
    Req: RPCReq,
{
    pub(crate) fn regist(p2p: &P2pModule) {
        RPCCaller::<Req>::new().regist(p2p);
    }
}

impl<V, Req> ObserveProxyCaller<V, Req>
where
    V: ObserveP2pView,
    Req: RPCReq,
{
    pub(crate) fn new(view: V) -> Self {
        Self {
            view,
            rpc: RPCCaller::new(),
        }
    }

    async fn call_proxy_inner(
        &self,
        proxy_node: NodeID,
        req: Req,
        payload: prost::bytes::Bytes,
        rpc_timeout: Duration,
    ) -> anyhow::Result<Req::Resp> {
        let msg = MsgPack {
            serialize_part: req,
            raw_bytes: vec![payload],
        };
        let resp: MsgPack<Req::Resp> = self
            .rpc
            .call(
                self.view.observe_p2p_module(),
                proxy_node.clone(),
                msg,
                Some(rpc_timeout),
                0,
            )
            .await
            .map_err(|e| {
                anyhow::anyhow!("proxy rpc failed (proxy_node={}): {:?}", proxy_node, e)
            })?;
        Ok(resp.serialize_part)
    }
}

#[async_trait]
impl<V> fluxon_observability::prom_remote_write_orchestrator::PromRemoteWriteProxyCaller<NodeID>
    for ObserveProxyCaller<V, PromRemoteWriteProxyReq>
where
    V: ObserveP2pView + Clone + Send + Sync + 'static,
{
    async fn call_proxy(
        &self,
        proxy_node: NodeID,
        req: PromRemoteWriteProxyReq,
        payload: prost::bytes::Bytes,
        rpc_timeout: Duration,
    ) -> anyhow::Result<PromRemoteWriteProxyResp> {
        self.call_proxy_inner(proxy_node, req, payload, rpc_timeout)
            .await
    }
}

#[async_trait]
impl<V> fluxon_observability::greptime_otlp_log_orchestrator::GreptimeOtlpLogProxyCaller<NodeID>
    for ObserveProxyCaller<V, GreptimeOtlpLogProxyReq>
where
    V: ObserveP2pView + Clone + Send + Sync + 'static,
{
    async fn call_proxy(
        &self,
        proxy_node: NodeID,
        req: GreptimeOtlpLogProxyReq,
        payload: prost::bytes::Bytes,
        rpc_timeout: Duration,
    ) -> anyhow::Result<GreptimeOtlpLogProxyResp> {
        self.call_proxy_inner(proxy_node, req, payload, rpc_timeout)
            .await
    }
}

#[derive(Clone, Debug, Default)]
pub struct MetricReporterNewArg {
    pub test_spec_config: TestSpecConfig,
}

/// Observability host for KV: owns background actors for:
/// - Prometheus remote-write transport (DirectThenProxy)
/// - Metrics aggregation (tick + gather + extra series build)
pub struct MetricReporter {
    view: std::sync::OnceLock<MetricReporterView>,
    master_observe_broadcast:
        Arc<limit_thirdparty::tokio::sync::ARwLock<Option<MasterObserveBroadcast>>>,
    prom_remote_write_http_sender: PromRemoteWriteHttpSender,
    prom_remote_write_actor_handle: std::sync::OnceLock<PromRemoteWriteHandle>,
    metrics_actor_handle: std::sync::OnceLock<ObserveMetricsHandle>,
    metrics: Arc<MetricsHandle>,
    test_spec_config: TestSpecConfig,
}

impl MetricReporter {
    fn view(&self) -> &MetricReporterView {
        self.view.get().unwrap()
    }

    fn prom_remote_write_actor_handle(&self) -> &PromRemoteWriteHandle {
        self.prom_remote_write_actor_handle.get().unwrap()
    }

    fn metrics_actor_handle(&self) -> &ObserveMetricsHandle {
        self.metrics_actor_handle.get().unwrap()
    }

    pub fn metrics_handle(&self) -> ObserveMetricsHandle {
        self.metrics_actor_handle().clone()
    }

    pub fn metrics(&self) -> Arc<MetricsHandle> {
        self.metrics.clone()
    }

    pub async fn construct(arg: MetricReporterNewArg) -> Result<Self, KvError> {
        Ok(Self::new(arg))
    }

    pub fn new(arg: MetricReporterNewArg) -> Self {
        let http_client = reqwest::Client::new();
        let prom_remote_write_http_sender = PromRemoteWriteHttpSender::new(http_client);

        Self {
            view: std::sync::OnceLock::new(),
            master_observe_broadcast: Arc::new(limit_thirdparty::tokio::sync::ARwLock::new(None)),
            prom_remote_write_http_sender,
            prom_remote_write_actor_handle: std::sync::OnceLock::new(),
            metrics_actor_handle: std::sync::OnceLock::new(),
            metrics: Arc::new(MetricsHandle::new(
                arg.test_spec_config.disable_observability,
            )),
            test_spec_config: arg.test_spec_config,
        }
    }

    pub fn attach_view(&self, view: MetricReporterView) {
        self.view
            .set(view)
            .unwrap_or_else(|_| panic!("MetricReporter view attached twice"));
    }

    pub async fn init2_prepare(&self) -> Result<(), KvError> {
        self.start_prepare().await
    }

    pub async fn init2_after_prom_remote_write_wait(&self) -> Result<(), KvError> {
        self.spawn_master_only_collect_loop();
        Ok(())
    }

    pub(crate) async fn wait_prom_remote_write_urls_best_effort_for_init_resource(
        &self,
    ) -> Result<(), KvError> {
        self.wait_prom_remote_write_urls_best_effort().await
    }

    pub fn observability_disabled(&self) -> bool {
        self.test_spec_config.disable_observability
    }

    async fn start_prepare(&self) -> Result<(), KvError> {
        if self.observability_disabled() {
            info!("MetricReporter test_spec_config disables observability background tasks");
            return Ok(());
        }
        self.start_prom_remote_write_actor();
        self.start_metrics_actor();
        self.start_kv_metrics_actor();
        self.start_fs_mount_stat_sampler();

        if let Err(e) = self.start_config_monitoring_task().await {
            error!("Failed to start config monitoring task: {}", e);
        }

        ObserveProxyCaller::<MetricReporterView, PromRemoteWriteProxyReq>::regist(
            self.view().p2p_module(),
        );
        self.register_prom_remote_write_proxy_rpc();
        Ok(())
    }

    fn start_prom_remote_write_actor(&self) {
        let view = self.view().clone();
        let picker = ObserveProxyPicker::new(view.clone(), view.clone());
        let caller =
            ObserveProxyCaller::<MetricReporterView, PromRemoteWriteProxyReq>::new(view.clone());
        let per_url_timeout = Duration::from_secs(PROM_REMOTE_WRITE_PER_URL_TIMEOUT_SECS);
        let (handle, owned) =
            PromRemoteWriteActorOwned::<crate::cluster_manager::NodeID, _, _, _>::new(
                PROM_REMOTE_WRITE_ACTOR_MAX_PENDING_PAYLOADS,
                self.prom_remote_write_http_sender.clone(),
                picker,
                caller,
                per_url_timeout,
                PROM_REMOTE_WRITE_USER_AGENT.to_string(),
            );
        self.prom_remote_write_actor_handle
            .set(handle)
            .unwrap_or_else(|_| panic!("PromRemoteWrite actor attached twice"));

        let view_task = view.clone();
        let _ = view.spawn("prom_remote_write_actor", async move {
            let mut shutdown_waiter = view_task.register_shutdown_waiter();
            owned.run(shutdown_waiter.wait()).await;
        });
    }

    fn start_metrics_actor(&self) {
        let view = self.view().clone();
        let prom = self.prom_remote_write_actor_handle().clone();
        let (handle, owned) = MetricsActorOwned::new(METRICS_ACTOR_MAX_PENDING_MSGS, prom);
        self.metrics_actor_handle
            .set(handle)
            .unwrap_or_else(|_| panic!("Metrics actor attached twice"));

        let view_task = view.clone();
        let _ = view.spawn("metrics_actor", async move {
            let mut shutdown_waiter = view_task.register_shutdown_waiter();
            owned.run(shutdown_waiter.wait()).await;
        });
    }

    fn start_kv_metrics_actor(&self) {
        let view = self.view().clone();
        let member = view.cluster_manager().get_self_info();
        let member_role = member.node_role();
        let (enable_node_metrics, enable_process_metrics) = match member_role {
            NodeRole::Master | NodeRole::Client => (true, true),
            NodeRole::External => (false, true),
            NodeRole::Unknown => {
                // English note:
                // - Node-level metrics must be emitted by owner/master only to avoid duplicate host samples.
                // - Unknown roles are treated as "do not sample" because their metric ownership is unclear.
                warn!(
                    "kv metrics actor: periodic metric sampling disabled due to unknown role: member_id={}",
                    member.id
                );
                (false, false)
            }
        };
        let (node_id, node_role) = resolve_node_labels(&member);
        let prom = self.prom_remote_write_actor_handle().clone();
        let (handle, owned) = KvMetricsActorOwned::new(
            node_id,
            node_role,
            prom,
            enable_node_metrics,
            enable_process_metrics,
        );
        // English note:
        // - This handle is best-effort (try_send) and must not impact hot paths.
        // - We attach it to both MetricsHandle (operation-level metrics) and ClusterManager so that
        //   transport/transfer code can attribute per-peer bandwidth without adding module cycles.
        let handle_for_cluster = handle.clone();
        self.metrics.attach_observe_handle(handle);
        view.cluster_manager()
            .attach_observe_handle(handle_for_cluster);

        let view_task = view.clone();
        let _ = view.spawn("kv_metrics_actor", async move {
            let mut shutdown_waiter = view_task.register_shutdown_waiter();
            owned
                .run(
                    Duration::from_secs(METRICS_FLUSH_INTERVAL_SECS),
                    shutdown_waiter.wait(),
                )
                .await;
        });
    }

    fn start_fs_mount_stat_sampler(&self) {
        // English note:
        // - These mount points are the only filesystem roots the system expects user code to touch:
        //   - shared memory directory (KV membership)
        //   - /tmp
        // - Export roots are sampled by fluxon_fs agents (they own those directories).
        //
        // This sampler is intentionally lightweight (statvfs) and runs outside business hot paths.
        let view = self.view().clone();
        let member = view.cluster_manager().get_self_info();
        match member.node_role() {
            NodeRole::Master | NodeRole::Client => {}
            NodeRole::External | NodeRole::Unknown => {
                // English note:
                // - External processes should not duplicate mountpoint usage sampling; owner/master reports it.
                // - fluxon_fs agents still report export roots explicitly via `set_fs_mount_fs_bytes`.
                return;
            }
        }
        let view_task = view.clone();
        let shutdown_poller = view.register_shutdown_poller();
        let mut shutdown_waiter = view.register_shutdown_waiter();
        let metrics = self.metrics.clone();

        let _ = view.spawn("fs_mount_stat_sampler", async move {
            const SAMPLE_INTERVAL: Duration = Duration::from_secs(METRICS_FLUSH_INTERVAL_SECS);
            let mut warned_missing_shm = false;
            let mut interval = tokio::time::interval(SAMPLE_INTERVAL);

            loop {
                tokio::select! {
                    biased;
                    _ = interval.tick() => {}
                    _ = shutdown_waiter.wait() => {
                        info!("fs mount stat sampler stopped by shutdown signal");
                        return;
                    }
                }

                if !shutdown_poller.is_running() {
                    info!("fs mount stat sampler stopped by shutdown signal");
                    return;
                }

                // shm (shared memory dir)
                let member = view_task.cluster_manager().get_self_info();
                let shm = member
                    .metadata
                    .get(META_KEY_LOCAL_IPC_ROOT)
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string());
                match shm {
                    Some(shm_dir_abs0) => {
                        let shm_dir_abs = normalize_abs_dir_label(&shm_dir_abs0);
                        if shm_dir_abs.is_empty() {
                            continue;
                        }
                        match statvfs_used_total(&shm_dir_abs) {
                            Ok((used, total)) => match mount_point_for_abs_dir(&shm_dir_abs) {
                                Ok(mp) => {
                                    metrics.set_fs_mount_fs_bytes(
                                        FsMountKind::Shm,
                                        &shm_dir_abs,
                                        mp.as_str(),
                                        used,
                                        total,
                                    );
                                    match collect_shm_file_usage(
                                        &shm_dir_abs,
                                        SHM_FILE_METRICS_MAX_FILES,
                                    ) {
                                        Ok(files) => {
                                            for file in files {
                                                metrics.set_shm_file_bytes(
                                                    &shm_dir_abs,
                                                    file.file_path_abs.as_str(),
                                                    file.logical_size_bytes,
                                                    file.allocated_bytes,
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            warn!(
                                                "shm file usage scan failed: dir={} err={}",
                                                shm_dir_abs, e
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(
                                        "fs mount mountinfo lookup failed: kind=shm dir={} err={}",
                                        shm_dir_abs, e
                                    );
                                }
                            },
                            Err(e) => {
                                warn!(
                                    "fs mount statvfs failed: kind=shm dir={} err={}",
                                    shm_dir_abs, e
                                );
                            }
                        }
                    }
                    None => {
                        if !warned_missing_shm {
                            warned_missing_shm = true;
                            warn!(
                                "fs mount stat sampler skipped shm: member metadata missing {}",
                                META_KEY_LOCAL_IPC_ROOT
                            );
                        }
                    }
                }

                // /tmp
                let tmp_dir_abs = "/tmp";
                match statvfs_used_total(tmp_dir_abs) {
                    Ok((used, total)) => match mount_point_for_abs_dir(tmp_dir_abs) {
                        Ok(mp) => {
                            metrics.set_fs_mount_fs_bytes(
                                FsMountKind::Tmp,
                                tmp_dir_abs,
                                mp.as_str(),
                                used,
                                total,
                            );
                        }
                        Err(e) => {
                            warn!(
                                "fs mount mountinfo lookup failed: kind=tmp dir={} err={}",
                                tmp_dir_abs, e
                            );
                        }
                    },
                    Err(e) => {
                        warn!(
                            "fs mount statvfs failed: kind=tmp dir={} err={}",
                            tmp_dir_abs, e
                        );
                    }
                }
            }
        });
    }

    fn spawn_master_only_collect_loop(&self) {
        let view = self.view().clone();
        let shutdown_poller = view.register_shutdown_poller();
        let mut shutdown_waiter = view.register_shutdown_waiter();
        let view_task = view.clone();
        let _ = view.spawn("metric_master_only_collect_loop", async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(METRICS_FLUSH_INTERVAL_SECS));
            loop {
                tokio::select! {
                    _ = shutdown_waiter.wait() => {
                        info!("Master-only metric collector stopped by shutdown signal");
                        break;
                    }
                    _ = interval.tick() => {
                        if !shutdown_poller.is_running() {
                            info!("Master-only metric collector stopped by shutdown signal");
                            break;
                        }
                        view_task.metric_reporter().master_collect();
                    }
                }
            }
        });
    }

    /// Collect per-segment capacity/usage from MasterSegManager (master only).
    fn collect_segment_metrics(&self) {
        let view = self.view().clone();
        let segs: Vec<(crate::cluster_manager::NodeID, Arc<OneSegAllocator>)> =
            view.master_seg_manager().get_all_segments_allocator();
        for (owner_node, alloc) in segs {
            let node = owner_node.as_ref();
            let device = alloc.seg_device_id.as_str();
            let capacity = alloc.total_size_bytes();
            let used = alloc.used_size_bytes();
            self.metrics
                .set_segment_capacity_bytes(node, device, capacity);
            self.metrics.set_segment_used_bytes(node, device, used);
        }
    }

    fn master_collect(&self) {
        let member = self.view().cluster_manager().get_self_info();
        let role = member.node_role();
        if !matches!(role, crate::cluster_manager::NodeRole::Master) {
            return;
        }

        self.collect_segment_metrics();
    }

    async fn wait_prom_remote_write_urls_best_effort(&self) -> Result<(), KvError> {
        if self.observability_disabled() {
            return Ok(());
        }
        let role = self.view().cluster_manager().get_self_info().node_role();
        assert!(
            !matches!(role, crate::cluster_manager::NodeRole::Master),
            "wait_prom_remote_write_urls_best_effort must not be called on master role"
        );

        let timeout_ms = 60_000u64;
        let wait = wait_master_observe_broadcast(
            self.view(),
            Duration::from_millis(timeout_ms),
            Duration::from_secs(5),
        )
        .await;
        apply_master_observe_broadcast_state(
            &self.master_observe_broadcast,
            self.prom_remote_write_actor_handle(),
            wait.observe_broadcast.clone(),
        )
        .await;

        let remote_write_urls = wait.remote_write_urls();
        if remote_write_urls.is_empty() {
            match wait.master_id.as_ref() {
                None => {
                    warn!(
                        "Prometheus remote write is disabled because master was not observed within timeout_ms={}",
                        timeout_ms
                    );
                }
                Some(master_id) => {
                    warn!(
                        "Prometheus remote write is not configured on master (master_id={}); continuing without remote write",
                        master_id
                    );
                }
            }
        }
        Ok(())
    }

    fn register_prom_remote_write_proxy_rpc(&self) {
        let view = self.view().clone();
        RPCHandler::<PromRemoteWriteProxyReq>::new().regist(
            self.view().p2p_module(),
            move |resp, msg| {
                let view_task = view.clone();
                let _ = view.spawn("rpc_prom_remote_write_proxy", async move {
                    let metric_reporter = view_task.metric_reporter();
                    let result = metric_reporter.handle_prom_remote_write_proxy(msg).await;
                    if let Err(e) = resp.send_resp(result).await {
                        warn!(
                            "PromRemoteWriteProxyResp send failed: peer={}, task_id={}, err={:?}",
                            resp.node_id(),
                            resp.task_id(),
                            e
                        );
                    }
                });
                Ok(())
            },
        );
    }

    async fn start_config_monitoring_task(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let view = self.view().clone();
        let master_observe_broadcast = self.master_observe_broadcast.clone();
        let prom_handle = self.prom_remote_write_actor_handle().clone();

        info!("Fetching initial master observe broadcast from cluster membership...");
        let initial = current_master_observe_broadcast(view.cluster_manager());
        if let Some(master_id) = initial.master_id.as_ref() {
            info!("Observed initial master member: {}", master_id);
        } else {
            info!("No master member observed yet");
        }
        apply_master_observe_broadcast_state(
            &master_observe_broadcast,
            &prom_handle,
            initial.observe_broadcast.clone(),
        )
        .await;

        let view_task = view.clone();
        let _ = view.spawn("metric_reporter_cfg_watch", async move {
            info!("Starting cluster manager event monitoring for master observe broadcast changes");
            let mut event_rx = view_task.cluster_manager().listen();
            let mut shutdown_waiter = view_task.register_shutdown_waiter();

            loop {
                tokio::select! {
                    event = event_rx.recv() => {
                        match event {
                            Ok(event) => match event {
                                ClusterEvent::MemberJoined(member)
                                | ClusterEvent::MemberUpdated(member) => {
                                    if !matches!(member.node_role(), NodeRole::Master) {
                                        debug!("Non-master membership change ignored for observe broadcast");
                                        continue;
                                    }
                                    info!("Master membership change detected, refreshing observe broadcast");
                                    let outcome = current_master_observe_broadcast(view_task.cluster_manager());
                                    apply_master_observe_broadcast_state(
                                        &master_observe_broadcast,
                                        &prom_handle,
                                        outcome.observe_broadcast,
                                    )
                                    .await;
                                }
                                ClusterEvent::MemberLeft(_) => {
                                    let outcome = current_master_observe_broadcast(view_task.cluster_manager());
                                    if outcome.master_id.is_none() {
                                        warn!("Master member left, clearing observe broadcast state");
                                    }
                                    apply_master_observe_broadcast_state(
                                        &master_observe_broadcast,
                                        &prom_handle,
                                        outcome.observe_broadcast,
                                    )
                                    .await;
                                }
                            },
                            Err(e) => {
                                error!("Failed to receive cluster event: {}", e);
                                break;
                            }
                        }
                    }
                    _ = shutdown_waiter.wait() => {
                        info!("Shutdown signal received for config watcher");
                        break;
                    }
                }
            }
            info!("Cluster manager observe broadcast monitoring stopped");
        });

        Ok(())
    }

    async fn handle_prom_remote_write_proxy(
        &self,
        msg: MsgPack<PromRemoteWriteProxyReq>,
    ) -> MsgPack<PromRemoteWriteProxyResp> {
        let req = msg.serialize_part;
        let mut resp = PromRemoteWriteProxyResp::default();

        if msg.raw_bytes.len() != 1 {
            resp.ok = false;
            resp.detail = format!(
                "invalid proxy payload: expected 1 raw_bytes item, got {}",
                msg.raw_bytes.len()
            );
            return MsgPack {
                serialize_part: resp,
                raw_bytes: Vec::new(),
            };
        }

        let self_member = self.view().cluster_manager().get_self_info();
        if !is_observe_proxy_allowed(&self_member) {
            resp.ok = false;
            resp.detail = "proxy is not allowed on this node (not master/relay)".to_string();
            return MsgPack {
                serialize_part: resp,
                raw_bytes: Vec::new(),
            };
        }

        let payload = msg.raw_bytes[0].clone();
        let forwarded = handle_prom_remote_write_proxy_request(
            &self.prom_remote_write_http_sender,
            req,
            payload,
        )
        .await;
        MsgPack {
            serialize_part: forwarded,
            raw_bytes: Vec::new(),
        }
    }
}

pub(crate) fn register_greptime_otlp_log_proxy_rpc(
    cm_view: ClusterManagerView,
    p2p: &P2pModule,
    direct_sender: GreptimeOtlpLogHttpSender,
) {
    RPCHandler::<GreptimeOtlpLogProxyReq>::new().regist(p2p, move |responsor, msg| {
        let spawner = cm_view.clone();
        let cm_view = cm_view.clone();
        let direct_sender = direct_sender.clone();
        let _ = spawner.spawn("rpc_greptime_otlp_log_proxy", async move {
            let resp = handle_greptime_otlp_log_proxy(&cm_view, &direct_sender, msg).await;
            let _ = responsor.send_resp(resp).await;
        });
        Ok(())
    });
}

async fn handle_greptime_otlp_log_proxy(
    cm_view: &ClusterManagerView,
    direct_sender: &GreptimeOtlpLogHttpSender,
    msg: MsgPack<GreptimeOtlpLogProxyReq>,
) -> MsgPack<GreptimeOtlpLogProxyResp> {
    let req = msg.serialize_part;
    let mut resp = GreptimeOtlpLogProxyResp::default();

    if msg.raw_bytes.len() != 1 {
        resp.ok = false;
        resp.detail = format!(
            "invalid proxy payload: expected 1 raw_bytes item, got {}",
            msg.raw_bytes.len()
        );
        return MsgPack {
            serialize_part: resp,
            raw_bytes: Vec::new(),
        };
    }

    let self_member = cm_view.cluster_manager().get_self_info();
    if !is_observe_proxy_allowed(&self_member) {
        resp.ok = false;
        resp.detail = "proxy is not allowed on this node (not master/relay)".to_string();
        return MsgPack {
            serialize_part: resp,
            raw_bytes: Vec::new(),
        };
    }

    let payload = msg.raw_bytes[0].clone();
    let forwarded = handle_greptime_proxy_request(direct_sender, req, payload).await;
    MsgPack {
        serialize_part: forwarded,
        raw_bytes: Vec::new(),
    }
}

#[async_trait]
impl LogicalModule for MetricReporter {
    type View = MetricReporterView;
    type NewArg = MetricReporterNewArg;
    type Error = KvError;

    fn name(&self) -> &str {
        "MetricReporter"
    }

    fn attach_view(&self, view: Self::View) {
        MetricReporter::attach_view(self, view);
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        info!("Metric reporter shutting down");
        Ok(())
    }
}
