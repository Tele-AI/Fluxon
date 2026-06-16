pub mod client_kv_api;
pub mod client_seg_pool;
pub mod client_transfer_engine;
pub mod cluster_manager;
pub mod config;
pub mod external_client_api;
pub mod panel_proxy;
// #[cfg(test)]
pub mod key_prefix;
#[cfg(feature = "test_bins")]
pub mod kv_test;
pub mod kvlease;
pub mod master_kv_router;
pub mod master_lease_manager;
pub mod master_seg_manager;
pub mod master_ui_monitor;
pub mod memholder;
pub mod metric_reporter;
pub mod metrics;
pub mod observe_kvope;
pub mod p2p;
pub mod profile;
pub mod rpcresp_kvresult_convert;
#[cfg(unix)]
pub mod segfault_handler;
pub mod user_api;
pub mod user_rpc;

pub use crate::client_seg_pool::SharedJsonMeta;
pub use crate::cluster_manager::{ClusterEvent, ClusterMember};
pub use fluxon_observability::types::FsMountKind;

pub type MembershipEventReceiver =
    limit_thirdparty::tokio::sync::abroadcast::Receiver<ClusterEvent>;

#[cfg(any(test, feature = "test_bins"))]
pub mod kvcore_test_lib;

use crate::client_kv_api::ClientKvApi;
use crate::client_kv_api::ClientKvApiAccessTrait;
use crate::client_kv_api::ClientKvApiView;
use crate::client_kv_api::ClientKvApiViewTrait;
use crate::client_seg_pool::ClientSegPoolAccessTrait;
use crate::client_seg_pool::ClientSegPoolView;
use crate::client_seg_pool::ClientSegPoolViewTrait;
use crate::client_transfer_engine::ClientTransferEngineAccessTrait;
use crate::client_transfer_engine::ClientTransferEngineNewArg;
use crate::client_transfer_engine::ClientTransferEngineView;
use crate::client_transfer_engine::ClientTransferEngineViewTrait;
use crate::cluster_manager::ClusterManagerAccessTrait;
use crate::cluster_manager::ClusterManagerView;
use crate::cluster_manager::ClusterManagerViewTrait;
use crate::external_client_api::ExternalClientApiAccessTrait;
use crate::external_client_api::ExternalClientApiView;
use crate::external_client_api::ExternalClientApiViewTrait;
use crate::master_kv_router::MasterKvRouterAccessTrait;
use crate::master_kv_router::MasterKvRouterView;
use crate::master_kv_router::MasterKvRouterViewTrait;
use crate::master_lease_manager::MasterLeaseManager;
use crate::master_lease_manager::master_lease_manager::MasterLeaseManagerNewArg;
use crate::master_lease_manager::{
    MasterLeaseManagerAccessTrait, MasterLeaseManagerView, MasterLeaseManagerViewTrait,
};
use crate::master_seg_manager::MasterSegManagerAccessTrait;
use crate::master_seg_manager::MasterSegManagerNewArg;
use crate::master_seg_manager::MasterSegManagerView;
use crate::master_seg_manager::MasterSegManagerViewTrait;
use crate::memholder::{ExternalMemHolder as ExtMemHolder, UserMemHolder};
use crate::p2p::p2p_module::P2pModuleAccessTrait;
use crate::p2p::p2p_module::P2pModuleView;
use crate::p2p::p2p_module::P2pModuleViewTrait;
use crate::rpcresp_kvresult_convert::msg_and_error::{ConfigError, KvResult};
use anyhow::Result;
use async_trait::async_trait;
use clap::{Parser, Subcommand};
use client_kv_api::ClientKvApiNewArg;
use client_seg_pool::{ClientSegPool, ClientSegPoolNewArg};
use client_transfer_engine::ClientTransferEngine;
use cluster_manager::{ClusterManager, ClusterManagerNewArg, ClusterManagerRdmaControlInit};
use config::{
    ClientConfig, ClientConfigYaml, ContributeToClusterPoolSize, FluxonKvSpec, MasterConfig,
    MasterConfigYaml, ProtocolConfig, ProtocolType, SideTransferRole, TestSpecConfig,
    TestSpecTransportMode, TransferEngineType, normalize_etcd_addresses,
};
use external_client_api::{ExternalClientApi, ExternalClientApiNewArg};
use fluxon_commu::TransferBackendActivationMode;
use fluxon_framework::LogicalModule;
use fluxon_framework::{AnyResult, define_framework};
use master_kv_router::{MasterKvRouter, MasterKvRouterNewArg};
use master_seg_manager::MasterSegManager;
use metric_reporter::{
    META_KEY_KV_OBSERVE_BROADCAST, MetricReporter, MetricReporterAccessTrait, MetricReporterNewArg,
    MetricReporterView, MetricReporterViewTrait, ObserveProxyCaller, ObserveProxyPicker,
    register_greptime_otlp_log_proxy_rpc, serialize_master_observe_broadcast,
    wait_master_observe_broadcast,
};
use p2p::p2p_module::{P2pModule, P2pModuleNewArg, P2pTcpThreadTransportTuning};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsString;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

fn cluster_manager_rdma_control_init_from_transfer_config(
    _transfer_engine: TransferEngineType,
    _protocol: &ProtocolConfig,
) -> ClusterManagerRdmaControlInit {
    // File-config startup always probes all devices but starts with no enabled RDMA devices.
    // The embedded ops page remains the single control plane for enabling/disabling devices at runtime.
    ClusterManagerRdmaControlInit::Disabled
}

fn cluster_manager_rdma_control_init_from_config(
    config: &ClientConfig,
) -> ClusterManagerRdmaControlInit {
    cluster_manager_rdma_control_init_from_transfer_config(
        config.fluxonkv_spec.transfer_engine,
        &config.protocol,
    )
}

fn test_spec_config_rdma_control_init(
    test_spec_config: Option<&TestSpecConfig>,
) -> Option<ClusterManagerRdmaControlInit> {
    let Some(cfg) = test_spec_config else {
        return None;
    };

    if let Some(devices) = cfg.rdma_device_names.as_ref() {
        // Benchmark/test mode may want deterministic multi-NIC fanout while still bypassing
        // runtime rdma-control gating. Lock the cluster-manager snapshot to the benchmark
        // selection so persisted rdma-control state cannot override the test authority.
        return Some(ClusterManagerRdmaControlInit::LockedExplicitDevices(
            devices.clone(),
        ));
    }

    match cfg.transport_mode {
        Some(TestSpecTransportMode::TransferOnly | TestSpecTransportMode::TransferWithRpc) => {
            Some(ClusterManagerRdmaControlInit::Disabled)
        }
        None => None,
    }
}

fn transfer_engine_rdma_device_names_from_config(
    protocol: &ProtocolConfig,
    test_spec_config: Option<&TestSpecConfig>,
) -> Option<String> {
    if let Some(devices) = test_spec_config.and_then(|cfg| cfg.rdma_device_names.as_ref()) {
        return Some(devices.join(","));
    }
    protocol.rdma_device_names.clone()
}

fn test_spec_config_transfer_backend_activation_mode(
    protocol: &ProtocolConfig,
    test_spec_config: Option<&TestSpecConfig>,
) -> Option<TransferBackendActivationMode> {
    match test_spec_config.and_then(|cfg| cfg.transport_mode) {
        Some(TestSpecTransportMode::TransferOnly | TestSpecTransportMode::TransferWithRpc) => {
            match protocol.protocol_type {
                ProtocolType::Tcp => Some(TransferBackendActivationMode::TcpTestBypassRdmaControl),
                ProtocolType::Rdma => None,
            }
        }
        None => None,
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ClientRunTestOverrides {
    pub rdma_control_init: ClusterManagerRdmaControlInit,
    pub transfer_backend_activation_mode: Option<TransferBackendActivationMode>,
}

#[derive(Clone, Debug)]
pub(crate) struct MasterRunTestOverrides {
    pub rdma_control_init: ClusterManagerRdmaControlInit,
    pub transfer_backend_activation_mode: Option<TransferBackendActivationMode>,
}

/// Result of a unified `get` that carries the role-specific holder types.
#[derive(Clone)]
pub enum KvGetResult {
    /// Owner/client mode: optional in-memory holder
    Owner(Option<Arc<UserMemHolder>>),
    /// External mode: optional external mem holder (shared-mem view)
    External(Option<Arc<ExtMemHolder>>),
}

/// A thin trait on `Framework` to route KV ops by role.
///
/// Rules:
/// - Only chooses the underlying API; no spawning or extra async machinery.
/// - Panics if neither role matches (programming error) which indicates a logic bug.
#[async_trait]
pub trait KvClientTrait {
    /// Whether this framework runs in external-client role
    fn is_external_mode(&self) -> bool;

    /// Return raw etcd endpoints as `host:port` strings (no scheme).
    ///
    /// This is a public API contract aligned with Python `KvClient.get_etcd_config()`.
    fn get_etcd_config(&self) -> KvResult<Vec<String>>;

    /// Put by role
    async fn kv_put(
        &self,
        key: &str,
        value: &[u8],
        opts: crate::client_kv_api::PutOptionalArgs,
    ) -> KvResult<()>;

    /// Put a key/value by encoding a flat-dict from raw entries.
    ///
    /// Each entry is `(type_id, dict_key_ptr, dict_key_len, val_u64, val_len, extra)`:
    /// - `dict_key_ptr/dict_key_len`: UTF-8 bytes of the dict field key.
    /// - For scalar types (bool/int64/float64), `val_u64` stores raw bits and `val_len` is fixed.
    /// - For bytes-like types (string/bytes), `val_u64` stores a pointer and `val_len` is the byte length.
    ///
    /// # Safety
    /// This is async and may run on a non-Python thread; therefore keys/values cannot be borrowed `&str`
    /// from Python across the call. The caller must guarantee that:
    /// - `dict_key_ptr/dict_key_len` points to readable UTF-8 bytes for the whole duration of the call
    /// - for bytes-like entries, `val_u64/val_len` points to readable bytes for the whole duration of the call
    async unsafe fn kv_put_ptrs(
        &self,
        key: &str,
        ptrs: Vec<(u8, usize, u32, u64, u32, Option<u32>)>,
        opts: crate::client_kv_api::PutOptionalArgs,
    ) -> KvResult<()>;

    /// Get by role; returns role-tagged holder
    async fn kv_get(&self, key: &str) -> KvResult<KvGetResult>;

    /// Delete by role
    async fn kv_delete(&self, key: &str) -> KvResult<()>;

    /// Existence check by role
    async fn kv_is_exist(&self, key: &str) -> KvResult<bool>;

    /// Count keys by prefix via master-side radix index.
    async fn kv_count_prefix(&self, prefix: &str) -> KvResult<u64>;

    /// Allocate a client lease with TTL seconds.
    ///
    /// Semantics:
    /// - `ttl_seconds` must be greater than or equal to the minimum client
    ///   lease TTL enforced by the master (see MasterLeaseManager::MIN_CLIENT_TTL_SECONDS,
    ///   currently 90 seconds).
    /// - Passing a value smaller than this minimum will result in
    ///   `LeaseMgrError::InvalidTTL` from the master side.
    async fn kv_allocate_lease(&self, ttl_seconds: u64) -> KvResult<u64>;

    /// Keepalive a client lease using its existing TTL.
    async fn kv_keepalive_lease(&self, lease_id: u64) -> KvResult<()>;
}

#[async_trait]
impl KvClientTrait for Framework {
    fn is_external_mode(&self) -> bool {
        let info = self
            .cluster_manager_view()
            .cluster_manager()
            .get_self_info();
        let is_side_worker = info
            .metadata
            .get("side_transfer_worker")
            .is_some_and(|v| v == "true");
        let is_ext = info.metadata.contains_key("external_client");
        let is_cli = info.metadata.contains_key("client");
        if is_side_worker {
            false
        } else if is_ext {
            true
        } else if is_cli {
            false
        } else {
            panic!(
                "Framework role metadata missing (neither external_client nor client). Implementation bug"
            );
        }
    }

    fn get_etcd_config(&self) -> KvResult<Vec<String>> {
        let endpoints = self
            .cluster_manager_view()
            .cluster_manager()
            .etcd_endpoints();
        crate::config::denormalize_etcd_endpoints(&endpoints)
    }

    async fn kv_put(
        &self,
        key: &str,
        value: &[u8],
        opts: crate::client_kv_api::PutOptionalArgs,
    ) -> KvResult<()> {
        if self.is_external_mode() {
            self.external_client_api_view()
                .external_client_api()
                .inner()
                .put(key, value, opts)
                .await
        } else {
            self.client_kv_api_view()
                .client_kv_api()
                .inner()
                .put(key, value, opts)
                .await
        }
    }

    async unsafe fn kv_put_ptrs(
        &self,
        key: &str,
        ptrs: Vec<(u8, usize, u32, u64, u32, Option<u32>)>,
        opts: crate::client_kv_api::PutOptionalArgs,
    ) -> KvResult<()> {
        if self.is_external_mode() {
            unsafe {
                self.external_client_api_view()
                    .external_client_api()
                    .inner()
                    .put_flat_dict_ptrs(key, ptrs, opts)
                    .await
            }
        } else {
            unsafe {
                self.client_kv_api_view()
                    .client_kv_api()
                    .inner()
                    .put_flat_dict_ptrs(key, ptrs, opts)
                    .await
            }
        }
    }

    async fn kv_get(&self, key: &str) -> KvResult<KvGetResult> {
        if self.is_external_mode() {
            let r = self
                .external_client_api_view()
                .external_client_api()
                .inner()
                .get(key)
                .await?;
            Ok(KvGetResult::External(r))
        } else {
            let r = self
                .client_kv_api_view()
                .client_kv_api()
                .inner()
                .get(key)
                .await?;
            Ok(KvGetResult::Owner(r.map(|(h, _)| h)))
        }
    }

    async fn kv_delete(&self, key: &str) -> KvResult<()> {
        if self.is_external_mode() {
            self.external_client_api_view()
                .external_client_api()
                .inner()
                .delete(key)
                .await
        } else {
            self.client_kv_api_view()
                .client_kv_api()
                .inner()
                .delete(key)
                .await
        }
    }

    async fn kv_is_exist(&self, key: &str) -> KvResult<bool> {
        if self.is_external_mode() {
            self.external_client_api_view()
                .external_client_api()
                .inner()
                .is_exist(key)
                .await
        } else {
            self.client_kv_api_view()
                .client_kv_api()
                .inner()
                .is_exist(key)
                .await
        }
    }

    async fn kv_count_prefix(&self, prefix: &str) -> KvResult<u64> {
        // Delegate to key_prefix helper that talks to master.
        crate::key_prefix::count_prefix_for_framework(self, prefix).await
    }

    async fn kv_allocate_lease(&self, ttl_seconds: u64) -> KvResult<u64> {
        crate::kvlease::allocate_lease(
            self.p2p_view().p2p_module(),
            self.cluster_manager_view().cluster_manager(),
            ttl_seconds,
        )
        .await
    }

    async fn kv_keepalive_lease(&self, lease_id: u64) -> KvResult<()> {
        crate::kvlease::keepalive_lease(
            self.p2p_view().p2p_module(),
            self.cluster_manager_view().cluster_manager(),
            lease_id,
        )
        .await
    }
}

/// Configuration argument types for run_master and run_client functions
#[derive(Debug, Clone)]
pub enum ConfigArg<T> {
    /// No configuration provided, use defaults
    None,
    /// Configuration file path
    File(PathBuf),
    /// Direct configuration object
    Config(T),
}

impl<T> Default for ConfigArg<T> {
    fn default() -> Self {
        ConfigArg::None
    }
}

#[derive(Parser)]
#[command(name = "fluxon_kv")]
#[command(about = "A distributed cache backend system")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run as master node
    Master {
        /// Configuration file path
        #[arg(short = 'f', long = "config")]
        config: Option<PathBuf>,
    },
    /// Run as client node
    Client {
        /// Configuration file path
        #[arg(short = 'f', long = "config")]
        config: Option<PathBuf>,
    },
}

// 定义框架，包含所有模块
define_framework!(
    cluster_manager: ClusterManager,
    p2p: P2pModule,
    master_seg_manager: MasterSegManager,
    master_kv_router: MasterKvRouter,
    metric_reporter: MetricReporter,
    client_kv_api: ClientKvApi,
    client_seg_pool: ClientSegPool,
    client_transfer_engine: ClientTransferEngine,
    external_client_api: ExternalClientApi,
    master_lease_manager: MasterLeaseManager
);

// fluxon-init-dag: yaml=../framework_init_steps.yaml
//
// Generated by build.rs from framework_init_steps.yaml (compile-time init-step DAG).
//
// This defines the generated init entries (variant-specific):
// - `init_framework_master(&Framework, InitArgsMaster)`
// - `init_framework_owner(&Framework, InitArgsOwner)`
// - `init_framework_external(&Framework, InitArgsExternal)`
include!(concat!(env!("OUT_DIR"), "/fluxon_init_dag/lib.rs"));

#[async_trait]
impl InitResourceHooks for Framework {
    async fn publish_cluster_member_watch_ready(fw: &Framework) -> anyhow::Result<()> {
        // Resource gate for steps that require continuous cluster membership observation.
        let cm = fw.init_get_cluster_manager();
        if !cm.is_watching() {
            anyhow::bail!("ClusterManager watching is not started after publisher step");
        }
        Ok(())
    }

    async fn wait_cluster_member_watch_ready(fw: &Framework) -> anyhow::Result<()> {
        // Local resource: ClusterManager::init2 already starts the watch. This wait is a strict
        // invariant check to keep the generated DAG aligned with the module behavior.
        let cm = fw.init_get_cluster_manager();
        if !cm.is_watching() {
            anyhow::bail!("ClusterManager watching is not started");
        }
        Ok(())
    }

    async fn publish_prom_remote_write_wait_ready(fw: &Framework) -> anyhow::Result<()> {
        // Publisher-only hook.
        //
        // Causal chain:
        // - The actual prom remote_write urls are distributed via the cluster membership state.
        // - The init DAG anchors a publish point here to keep scheduling explicit.
        // - v5 resource semantics ensure this hook is executed only in the publisher variant.
        let role = fw.init_get_cluster_manager().get_self_info().node_role();
        assert!(
            matches!(role, crate::cluster_manager::NodeRole::Master),
            "prom_remote_write_wait_ready.publish called on unexpected role: {:?}",
            role
        );
        Ok(())
    }

    async fn wait_prom_remote_write_wait_ready(fw: &Framework) -> anyhow::Result<()> {
        // Wait-only hook.
        //
        // v5 resource semantics ensure this hook is executed only in waiter variants.
        let role = fw.init_get_cluster_manager().get_self_info().node_role();
        assert!(
            !matches!(role, crate::cluster_manager::NodeRole::Master),
            "prom_remote_write_wait_ready.wait called on master role"
        );

        // Wait logic is implemented by MetricReporter, because it owns the master observe-broadcast state.
        fw.init_get_metric_reporter()
            .wait_prom_remote_write_urls_best_effort_for_init_resource()
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    async fn publish_owner_shared_mem_bundle_ready(fw: &Framework) -> anyhow::Result<()> {
        // This resource has no publisher-side hook in YAML (publish_tags: []).
        //
        // If this is called, the init DAG spec and the generated code are inconsistent.
        let _ = fw;
        unreachable!(
            "owner_shared_mem_bundle_ready.publish must not be called (publish_tags is empty)"
        );
    }

    async fn wait_owner_shared_mem_bundle_ready(fw: &Framework) -> anyhow::Result<()> {
        let cm = fw.init_get_cluster_manager();
        let role = cm.get_self_info().node_role();

        assert!(
            matches!(role, crate::cluster_manager::NodeRole::External),
            "owner_shared_mem_bundle_ready.wait called on unexpected role: {:?}",
            role
        );

        // External nodes wait for shared.json + mmap.file and then wait for the owner member.
        fw.init_get_external_client_api()
            .wait_owner_shared_mem_bundle_ready_for_init_resource()
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }
}

fn find_default_config_file() -> Option<PathBuf> {
    for filename in &["config.yaml", "config.yml"] {
        let path = PathBuf::from(filename);
        if path.exists() {
            return Some(path);
        }
    }
    None
}

fn tcp_thread_transport_tuning_from_test_spec_config(
    test_spec_config: &TestSpecConfig,
) -> P2pTcpThreadTransportTuning {
    P2pTcpThreadTransportTuning {
        reactor_shard_count: test_spec_config.tcp_thread_reactor_shard_count,
        bulk_lane_count: test_spec_config.tcp_thread_bulk_lane_count,
        control_lane_count: test_spec_config.tcp_thread_control_lane_count,
    }
}

pub async fn load_client_config(config_arg: ConfigArg<ClientConfig>) -> KvResult<ClientConfig> {
    match config_arg {
        ConfigArg::None => {
            // Try to find default config file
            match find_default_config_file() {
                Some(path) => {
                    println!("Using default config file: {:?}", path);
                    let config_yaml = ClientConfigYaml::from_file(&path)?;
                    let config = config_yaml.verify()?;
                    println!("Client configuration loaded and validated successfully");
                    Ok(config)
                }
                None => Err(ConfigError::FileReadError {
                    detail: "No config file found. Please provide a config file with -f option"
                        .to_string(),
                }
                .into_kverror()),
            }
        }
        ConfigArg::File(config_path) => {
            println!("Loading client configuration from: {:?}", config_path);
            let config_yaml = ClientConfigYaml::from_file(&config_path)?;
            let config = config_yaml.verify()?;
            println!("Client configuration loaded and validated successfully");
            Ok(config)
        }
        ConfigArg::Config(config) => {
            println!("Using provided client configuration");
            Ok(config)
        }
    }
}

pub async fn load_master_config(config_arg: ConfigArg<MasterConfig>) -> KvResult<MasterConfig> {
    match config_arg {
        ConfigArg::None => {
            // Try to find default config file
            match find_default_config_file() {
                Some(path) => {
                    println!("Using default config file: {:?}", path);
                    let config_yaml = MasterConfigYaml::from_file(&path)?;
                    let config = config_yaml.verify()?;
                    println!("Master configuration loaded and validated successfully");
                    Ok(config)
                }
                None => Err(ConfigError::FileReadError {
                    detail: "No config file found. Please provide a config file with -f option"
                        .to_string(),
                }
                .into_kverror()),
            }
        }
        ConfigArg::File(config_path) => {
            println!("Loading master configuration from: {:?}", config_path);
            let config_yaml = MasterConfigYaml::from_file(&config_path)?;
            let config = config_yaml.verify()?;
            println!("Master configuration loaded and validated successfully");
            Ok(config)
        }
        ConfigArg::Config(config) => {
            println!("Using provided master configuration");
            Ok(config)
        }
    }
}

// Fixed policy (not configurable):
// - DirectThenProxy needs an upper bound per send attempt to avoid stalling the exporter loop.
// - Logs are best-effort (dropping is allowed; local file is the safety net), so we prefer a small,
//   predictable timeout over retries/backoff that could accumulate latency and memory pressure.
const GREPTIME_OTLP_LOG_SEND_TIMEOUT_SECS: u64 = 10;

fn start_greptime_otlp_tracing_exporter_kv(
    cm_view: ClusterManagerView,
    p2p_view: P2pModuleView,
    greptime_cfg: Option<crate::config::GreptimeOtlpLogConfig>,
    log_rx: Option<fluxon_observability::greptime_otlp_tracing::GreptimeOtlpTracingReceiver>,
    cluster_name: &str,
    role: fluxon_observability::types::FluxonMemberRole,
    member_id: &str,
) {
    let Some(cfg) = greptime_cfg else {
        return;
    };
    let Some(rx) = log_rx else {
        warn!("greptime otlp tracing exporter disabled: tracing layer not installed");
        return;
    };

    let attrs = fluxon_observability::greptime_otlp_log::LogCollectorAttrs {
        cluster_name: cluster_name.to_string(),
        member_kind: fluxon_observability::types::FluxonMemberKind::Kv,
        role,
        member_id: member_id.to_string(),
    };

    let exporter_cfg =
        fluxon_observability::greptime_otlp_tracing::GreptimeOtlpTracingExporterConfig {
            flush_interval: std::time::Duration::from_millis(cfg.flush_interval_ms),
            max_batch_lines: cfg.max_batch_lines,
        };

    let otlp_req = fluxon_observability::greptime_otlp_log_orchestrator::GreptimeOtlpLogProxyReq {
        endpoint: cfg.otlp_endpoint.clone(),
        db_name: cfg.db_name.clone(),
        table_name: cfg.table_name.clone(),
        timeout_ms: GREPTIME_OTLP_LOG_SEND_TIMEOUT_SECS * 1000,
    };

    let direct_sender =
        fluxon_observability::greptime_otlp_log_orchestrator::GreptimeOtlpLogHttpSender::new(
            reqwest::Client::new(),
        );
    let direct_sender_for_handler = direct_sender.clone();

    let p2p = p2p_view.p2p_module();
    ObserveProxyCaller::<
        P2pModuleView,
        fluxon_observability::greptime_otlp_log_orchestrator::GreptimeOtlpLogProxyReq,
    >::regist(p2p);
    register_greptime_otlp_log_proxy_rpc(cm_view.clone(), p2p, direct_sender_for_handler);

    let picker = ObserveProxyPicker::new(cm_view.clone(), p2p_view.clone());
    let caller = ObserveProxyCaller::<
        P2pModuleView,
        fluxon_observability::greptime_otlp_log_orchestrator::GreptimeOtlpLogProxyReq,
    >::new(p2p_view);

    let mut shutdown_waiter = cm_view.register_shutdown_waiter();
    let task_name = format!("greptime_otlp_tracing_exporter_{}", role.as_str());
    let cm_view_for_task = cm_view.clone();
    let _ = cm_view_for_task.spawn(&task_name, async move {
        if let Err(e) = fluxon_observability::greptime_otlp_tracing::run_exporter_loop::<
            crate::cluster_manager::NodeID,
            _,
            _,
            _,
            _,
        >(
            exporter_cfg,
            otlp_req,
            attrs,
            rx,
            direct_sender,
            picker,
            caller,
            shutdown_waiter.wait(),
        )
        .await
        {
            warn!(err = %e, "greptime otlp tracing exporter task exited");
        }
    });
}

fn build_side_transfer_worker_config(
    owner_config: &ClientConfig,
    worker_idx: u16,
) -> Result<ClientConfig> {
    let p2p_listen_port = owner_config
        .test_spec_config
        .side_transfer_worker_p2p_port_base
        .map(|base_port| {
            base_port
                .checked_add(worker_idx)
                .ok_or_else(|| anyhow::anyhow!("side-transfer worker p2p port overflow"))
        })
        .transpose()?;

    let mut test_spec_config = owner_config.test_spec_config.clone();
    test_spec_config.enable_side_transfer = true;
    test_spec_config.side_transfer_worker_count = 0;
    test_spec_config.side_transfer_worker_p2p_port_base = None;
    test_spec_config.side_transfer_role = Some(SideTransferRole::Worker);
    test_spec_config.transport_mode = None;
    test_spec_config.rdma_device_names = None;

    Ok(ClientConfig {
        cluster_name: owner_config.cluster_name.clone(),
        etcd_addresses_raw: Vec::new(),
        instance_key: format!("{}__side_{}", owner_config.instance_key, worker_idx),
        contribute_to_cluster_pool_size: ContributeToClusterPoolSize {
            dram: 0,
            vram: HashMap::new(),
        },
        protocol: ProtocolConfig {
            protocol_type: ProtocolType::Tcp,
            rdma_device_names: None,
        },
        pprof_duration_seconds: owner_config.pprof_duration_seconds,
        redis_compat_listen_addr: None,
        fluxonkv_spec: FluxonKvSpec {
            etcd_addresses: Vec::new(),
            cluster_name: owner_config.cluster_name.clone(),
            p2p_listen_port,
            transfer_engine: TransferEngineType::P2p,
            enable_transfer_rpc_fast_path: false,
            sub_cluster: None,
        },
        shared_memory_path: owner_config.shared_memory_path.clone(),
        shared_file_path: owner_config.shared_file_path.clone(),
        test_spec_config,
    })
}

const SIDE_TRANSFER_WORKER_BIN_ENV: &str = "FLUXON_KV_SIDE_WORKER_BIN";
const SIDE_TRANSFER_WORKER_PYTHON_ENV: &str = "FLUXON_KV_SIDE_WORKER_PYTHON";
const SIDE_TRANSFER_WORKER_READY_TIMEOUT: Duration = Duration::from_secs(30);
const SIDE_TRANSFER_WORKER_RECONCILE_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Clone)]
enum SideTransferWorkerLauncher {
    FluxonKvBinary { program: OsString },
    PythonFluxonPy { program: OsString },
}

struct SideTransferWorkerProcess {
    worker_idx: u16,
    side_id: String,
    config_path: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    child: Child,
    not_ready_since: Option<Instant>,
}

fn side_transfer_worker_instance_key(owner_instance_key: &str, worker_idx: u16) -> String {
    format!("{owner_instance_key}__side_{worker_idx}")
}

fn build_side_transfer_worker_config_yaml(
    owner_config: &ClientConfig,
    worker_idx: u16,
) -> Result<ClientConfigYaml> {
    let side_config = build_side_transfer_worker_config(owner_config, worker_idx)?;
    Ok(ClientConfigYaml {
        instance_key: side_config.instance_key,
        protocol: Some(side_config.protocol),
        contribute_to_cluster_pool_size: None,
        pprof_duration_seconds: side_config.pprof_duration_seconds,
        fluxonkv_spec: crate::config::FluxonKvSpecYaml {
            etcd_addresses: None,
            cluster_name: side_config.cluster_name,
            shared_memory_path: side_config.shared_memory_path,
            shared_file_path: side_config.shared_file_path,
            p2p_listen_port: side_config.fluxonkv_spec.p2p_listen_port,
            redis_compat: None,
            sub_cluster: None,
        },
        test_spec_config: side_config.test_spec_config,
    })
}

fn side_transfer_runtime_dir(owner_config: &ClientConfig) -> PathBuf {
    Path::new(&owner_config.shared_file_path)
        .join(format!("{}_cluster_kv_logs", owner_config.cluster_name))
        .join("side_transfer_runtime")
        .join(&owner_config.instance_key)
}

fn cluster_manager_local_ipc_root(
    shared_memory_path: &str,
    test_spec_config: &TestSpecConfig,
) -> Option<String> {
    // Test-only override:
    // - default deployments keep publishing local_ipc_root and rely on the existing layered
    //   fallback semantics when iceoryx2 is unavailable.
    // - benchmark/test runs sometimes need a deterministic "pure direct/tcp_thread" topology.
    // - hiding local_ipc_root from cluster membership prevents the same-machine intra lane from
    //   being planned at all, so peer selection naturally converges to direct transport.
    if test_spec_config.disable_local_ipc {
        info!("Local IPC transport disabled by test_spec_config.disable_local_ipc=true");
        return None;
    }

    // Local IPC and the mmap bundle must stay logically tied to the same share-group root, but
    // they do not need to reuse the same literal filesystem path.
    //
    // Causal chain:
    // - `shared_memory_path` is authoritative for mmap.file/shared.json coordination and can be long.
    // - iceoryx2 event listeners materialize AF_UNIX socket files under `local_ipc_root`.
    // - AF_UNIX paths are short; reusing a long `shared_memory_path` makes listener creation fail
    //   as `ResourceCreationFailed`, even on a clean start with no stale resources.
    // - Therefore we derive a short, stable alias from the canonical shared-memory root and publish
    //   only that alias as `local_ipc_root`.
    Some(
        derive_short_local_ipc_root(shared_memory_path)
            .unwrap_or_else(|err| panic!("failed to derive local_ipc_root: {}", err)),
    )
}

fn derive_short_local_ipc_root(shared_memory_path: &str) -> Result<String> {
    if shared_memory_path.trim().is_empty() {
        anyhow::bail!("shared_memory_path cannot be empty");
    }

    std::fs::create_dir_all(shared_memory_path).map_err(|e| {
        anyhow::anyhow!(
            "shared_memory_path must be creatable before deriving local_ipc_root: path='{}', err={}",
            shared_memory_path,
            e
        )
    })?;

    let canonical = std::fs::canonicalize(shared_memory_path).map_err(|e| {
        anyhow::anyhow!(
            "shared_memory_path must be canonicalizable before deriving local_ipc_root: path='{}', err={}",
            shared_memory_path,
            e
        )
    })?;
    let canonical_text = canonical.to_string_lossy();

    let mut hasher = Sha256::new();
    hasher.update(canonical_text.as_bytes());
    let digest_hex = hex::encode(hasher.finalize());
    let short_id = &digest_hex[..20];
    let local_ipc_root = format!("/tmp/fluxon_ipc/{}", short_id);

    Ok(local_ipc_root)
}

fn write_side_transfer_worker_config(
    owner_config: &ClientConfig,
    worker_idx: u16,
    runtime_dir: &Path,
) -> Result<PathBuf> {
    std::fs::create_dir_all(runtime_dir)?;
    let config_yaml = build_side_transfer_worker_config_yaml(owner_config, worker_idx)?;
    let payload = serde_yaml::to_string(&config_yaml)?;
    let config_path = runtime_dir.join(format!("side_worker_{worker_idx}.yaml"));
    let tmp_path = runtime_dir.join(format!(
        "side_worker_{worker_idx}.tmp.{}.{}",
        std::process::id(),
        chrono::Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_else(|| chrono::Utc::now().timestamp_micros() * 1_000),
    ));
    std::fs::write(&tmp_path, payload)?;
    std::fs::rename(&tmp_path, &config_path)?;
    Ok(config_path)
}

fn current_exe_looks_like_python(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.to_ascii_lowercase().starts_with("python"))
}

fn current_exe_looks_like_fluxon_kv(path: &Path) -> bool {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .is_some_and(|stem| stem == "fluxon_kv" || stem == "kv_test")
}

fn resolve_side_transfer_worker_launcher() -> Result<SideTransferWorkerLauncher> {
    if let Some(program) = std::env::var_os(SIDE_TRANSFER_WORKER_BIN_ENV) {
        return Ok(SideTransferWorkerLauncher::FluxonKvBinary { program });
    }
    if let Some(program) = std::env::var_os(SIDE_TRANSFER_WORKER_PYTHON_ENV) {
        return Ok(SideTransferWorkerLauncher::PythonFluxonPy { program });
    }

    let current_exe = std::env::current_exe()?;
    if current_exe_looks_like_fluxon_kv(&current_exe) {
        return Ok(SideTransferWorkerLauncher::FluxonKvBinary {
            program: current_exe.into_os_string(),
        });
    }
    if current_exe_looks_like_python(&current_exe) {
        return Ok(SideTransferWorkerLauncher::PythonFluxonPy {
            program: current_exe.into_os_string(),
        });
    }

    Err(anyhow::anyhow!(
        "Unable to resolve side-transfer worker launcher from current executable '{}'; set {} or {} explicitly",
        current_exe.display(),
        SIDE_TRANSFER_WORKER_BIN_ENV,
        SIDE_TRANSFER_WORKER_PYTHON_ENV,
    ))
}

fn spawn_side_transfer_worker_process(
    launcher: &SideTransferWorkerLauncher,
    current_dir: Option<&Path>,
    owner_instance_key: &str,
    worker_idx: u16,
    config_path: PathBuf,
) -> Result<SideTransferWorkerProcess> {
    let parent_dir = config_path.parent().ok_or_else(|| {
        anyhow::anyhow!(
            "Failed to resolve parent directory for side-transfer worker config '{}'",
            config_path.display()
        )
    })?;
    let stdout_path = parent_dir.join(format!("side_worker_{worker_idx}.stdout.log"));
    let stderr_path = parent_dir.join(format!("side_worker_{worker_idx}.stderr.log"));
    let stdout_file = File::create(&stdout_path).map_err(|e| {
        anyhow::anyhow!(
            "Failed to open side-transfer worker stdout log '{}' for worker {}: {}",
            stdout_path.display(),
            worker_idx,
            e
        )
    })?;
    let stderr_file = File::create(&stderr_path).map_err(|e| {
        anyhow::anyhow!(
            "Failed to open side-transfer worker stderr log '{}' for worker {}: {}",
            stderr_path.display(),
            worker_idx,
            e
        )
    })?;
    let mut command = match launcher {
        SideTransferWorkerLauncher::FluxonKvBinary { program } => {
            let mut command = Command::new(program);
            command.arg("client").arg("-f").arg(&config_path);
            command
        }
        SideTransferWorkerLauncher::PythonFluxonPy { program } => {
            let mut command = Command::new(program);
            command
                .arg("-m")
                .arg("fluxon_py")
                .arg("--server")
                .arg("--config")
                .arg(&config_path);
            command
        }
    };
    let command_current_dir = match launcher {
        SideTransferWorkerLauncher::FluxonKvBinary { .. } => current_dir,
        SideTransferWorkerLauncher::PythonFluxonPy { .. } => current_dir.map(|dir| {
            // When kv_test runs from `<repo>/fluxon_rs`, Python sees the Rust crate directory
            // `fluxon_pyo3/` as a namespace package and that shadows the real extension module.
            // Switch Python-launched side workers to the repo root in that local-dev layout.
            if dir.join("fluxon_pyo3").is_dir() {
                if let Some(parent) = dir.parent() {
                    if parent.join("fluxon_py").is_dir() {
                        return parent;
                    }
                }
            }
            dir
        }),
    };
    if let Some(dir) = command_current_dir {
        command.current_dir(dir);
    }
    command.stdout(Stdio::from(stdout_file));
    command.stderr(Stdio::from(stderr_file));
    let child = command.spawn().map_err(|e| {
        anyhow::anyhow!(
            "Failed to spawn side-transfer worker {} with config '{}': {}",
            worker_idx,
            config_path.display(),
            e
        )
    })?;
    Ok(SideTransferWorkerProcess {
        worker_idx,
        side_id: side_transfer_worker_instance_key(owner_instance_key, worker_idx),
        config_path,
        stdout_path,
        stderr_path,
        child,
        not_ready_since: Some(Instant::now()),
    })
}

fn read_side_transfer_worker_output_tail(path: &Path) -> Option<String> {
    let raw = std::fs::read_to_string(path).ok()?;
    if raw.trim().is_empty() {
        return None;
    }
    let lines: Vec<&str> = raw.lines().collect();
    let tail = if lines.len() > 40 {
        lines[lines.len().saturating_sub(40)..].join("\n")
    } else {
        raw
    };
    Some(tail)
}

fn format_side_transfer_worker_output_tails(worker: &SideTransferWorkerProcess) -> String {
    let mut parts = Vec::new();
    if let Some(stdout_tail) = read_side_transfer_worker_output_tail(&worker.stdout_path) {
        parts.push(format!(
            "stdout_log={}\n--- stdout tail ---\n{}\n--- end stdout tail ---",
            worker.stdout_path.display(),
            stdout_tail
        ));
    } else {
        parts.push(format!(
            "stdout_log={} (empty)",
            worker.stdout_path.display()
        ));
    }
    if let Some(stderr_tail) = read_side_transfer_worker_output_tail(&worker.stderr_path) {
        parts.push(format!(
            "stderr_log={}\n--- stderr tail ---\n{}\n--- end stderr tail ---",
            worker.stderr_path.display(),
            stderr_tail
        ));
    } else {
        parts.push(format!(
            "stderr_log={} (empty)",
            worker.stderr_path.display()
        ));
    }
    parts.join("\n")
}

fn read_side_transfer_peer_file(
    shared_file_path: &str,
    side_id: &str,
) -> Option<crate::client_seg_pool::SideTransferPeerFileMeta> {
    let peer_path = ClientSegPool::side_transfer_peer_file_path(shared_file_path, side_id);
    let payload = std::fs::read_to_string(&peer_path).ok()?;
    serde_json::from_str::<crate::client_seg_pool::SideTransferPeerFileMeta>(&payload).ok()
}

fn is_side_transfer_worker_ready(
    _cluster_manager: &ClusterManager,
    shared_file_path: &str,
    owner_id: &str,
    owner_start_time: i64,
    side_id: &str,
) -> bool {
    let Some(meta) = read_side_transfer_peer_file(shared_file_path, side_id) else {
        return false;
    };
    // Peer files are written only after the worker has attached shared memory and finished
    // client-seg-pool init3. Cluster membership/share-group propagation is eventually
    // consistent and can lag or race across owner restarts, so it must not block owner startup.
    if meta.owner_id != owner_id || meta.owner_start_time != owner_start_time {
        return false;
    }
    true
}

fn start_side_transfer_worker(
    owner_config: &ClientConfig,
    launcher: &SideTransferWorkerLauncher,
    current_dir: Option<&Path>,
    runtime_dir: &Path,
    worker_idx: u16,
) -> Result<SideTransferWorkerProcess> {
    let config_path = write_side_transfer_worker_config(owner_config, worker_idx, runtime_dir)?;
    spawn_side_transfer_worker_process(
        launcher,
        current_dir,
        &owner_config.instance_key,
        worker_idx,
        config_path,
    )
}

fn cleanup_stale_side_transfer_bootstrap_artifacts(owner_config: &ClientConfig) -> Result<()> {
    let shared_file_path = Path::new(&owner_config.shared_file_path);
    let shared_json_path = shared_file_path.join("shared.json");
    match std::fs::remove_file(&shared_json_path) {
        Ok(()) => {
            info!(
                owner_id = %owner_config.instance_key,
                path = %shared_json_path.display(),
                "removed stale shared.json before side-transfer worker launch"
            );
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(anyhow::anyhow!(
                "Failed to remove stale shared.json '{}' before side-transfer worker launch: {}",
                shared_json_path.display(),
                err
            ));
        }
    }

    let peers_dir = ClientSegPool::side_transfer_peers_dir(&owner_config.shared_file_path);
    match std::fs::remove_dir_all(&peers_dir) {
        Ok(()) => {
            info!(
                owner_id = %owner_config.instance_key,
                path = %peers_dir.display(),
                "removed stale side-transfer peer dir before worker launch"
            );
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(anyhow::anyhow!(
                "Failed to remove stale side-transfer peer dir '{}' before worker launch: {}",
                peers_dir.display(),
                err
            ));
        }
    }

    Ok(())
}

async fn wait_for_side_transfer_workers_ready(
    framework: &Arc<Framework>,
    owner_config: &ClientConfig,
    side_workers: &mut BTreeMap<u16, SideTransferWorkerProcess>,
) -> Result<()> {
    let cluster_manager_view = framework.cluster_manager_view();
    let cluster_manager = cluster_manager_view.cluster_manager();
    let owner_info = cluster_manager.get_self_info();
    let expected = side_workers.len();
    let start = Instant::now();

    loop {
        let mut ready = 0usize;
        for worker in side_workers.values_mut() {
            if let Some(status) = worker.child.try_wait()? {
                return Err(anyhow::anyhow!(
                    "side-transfer worker {} exited before ready: status={}, config={}\n{}",
                    worker.worker_idx,
                    status,
                    worker.config_path.display(),
                    format_side_transfer_worker_output_tails(worker),
                ));
            }
            if is_side_transfer_worker_ready(
                cluster_manager,
                &owner_config.shared_file_path,
                &owner_info.id,
                owner_info.node_start_time,
                &worker.side_id,
            ) {
                worker.not_ready_since = None;
                ready += 1;
            } else if worker.not_ready_since.is_none() {
                worker.not_ready_since = Some(Instant::now());
            }
        }

        if ready == expected {
            return Ok(());
        }

        if start.elapsed() >= SIDE_TRANSFER_WORKER_READY_TIMEOUT {
            return Err(anyhow::anyhow!(
                "Timed out waiting for side-transfer workers to publish readiness: ready={}/{}, timeout={}s",
                ready,
                expected,
                SIDE_TRANSFER_WORKER_READY_TIMEOUT.as_secs(),
            ));
        }

        limit_thirdparty::tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn stop_side_transfer_worker_blocking(mut worker: SideTransferWorkerProcess, cleanup_config: bool) {
    let status = match worker.child.try_wait() {
        Ok(Some(status)) => Some(status),
        Ok(None) => {
            if let Err(err) = worker.child.kill() {
                warn!(
                    worker_idx = worker.worker_idx,
                    side_id = %worker.side_id,
                    config = %worker.config_path.display(),
                    err = %err,
                    "failed to kill side-transfer worker"
                );
            }
            match worker.child.wait() {
                Ok(status) => Some(status),
                Err(err) => {
                    warn!(
                        worker_idx = worker.worker_idx,
                        side_id = %worker.side_id,
                        config = %worker.config_path.display(),
                        err = %err,
                        "failed to wait side-transfer worker exit"
                    );
                    None
                }
            }
        }
        Err(err) => {
            warn!(
                worker_idx = worker.worker_idx,
                side_id = %worker.side_id,
                config = %worker.config_path.display(),
                err = %err,
                "failed to poll side-transfer worker status"
            );
            None
        }
    };
    if let Some(status) = status {
        info!(
            worker_idx = worker.worker_idx,
            side_id = %worker.side_id,
            config = %worker.config_path.display(),
            %status,
            "side-transfer worker exited"
        );
    }
    if cleanup_config {
        cleanup_side_transfer_worker_config(&worker.config_path);
    }
}

fn cleanup_side_transfer_worker_config(path: &Path) {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            warn!(
                config = %path.display(),
                err = %e,
                "failed to remove side-transfer worker config"
            );
        }
    }
}

fn shutdown_side_transfer_workers_blocking(side_workers: Vec<SideTransferWorkerProcess>) {
    for worker in side_workers.into_iter().rev() {
        stop_side_transfer_worker_blocking(worker, true);
    }
}

pub async fn entry() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Master { config } => {
            let config_arg = config.map_or(ConfigArg::None, ConfigArg::File);
            let (framework, _) = run_master(config_arg).await?;
            framework.wait_shutdown_signal().await;
            framework
                .shutdown()
                .await
                .map_err(|e| anyhow::anyhow!("{}", e))?;
        }
        Commands::Client { config } => {
            let config_arg = config.map_or(ConfigArg::None, ConfigArg::File);
            let (framework, _) = run_client(config_arg).await?;
            framework.wait_shutdown_signal().await;
            framework
                .shutdown()
                .await
                .map_err(|e| anyhow::anyhow!("{}", e))?;
        }
    }
    Ok(())
}

async fn run_master_impl(
    config_arg: ConfigArg<MasterConfig>,
    test_overrides: Option<MasterRunTestOverrides>,
) -> Result<(Arc<Framework>, MasterConfig)> {
    #[cfg(unix)]
    segfault_handler::install_sigsegv_classifier();

    println!("Starting cache backend in MASTER mode");

    let build_version = fluxon_util::git_version_build_record::get_current_git_commitid().unwrap();
    let source_sha256 = fluxon_util::build_info::SOURCE_SHA256;
    println!("Build version (git commit): {}", build_version);
    println!("Build version (source-sha256): {}", source_sha256);

    // 加载master配置
    let config = load_master_config(config_arg)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to load master config: {}", e))?;

    // Test-only log override:
    // allow benchmark/test configs to expose upstream iceoryx2 logs without broadly
    // lifting third-party noise for all dependencies.
    unsafe {
        std::env::set_var(
            "FLUXON_ENABLE_ICEORYX_LOGS",
            if config.test_spec_config.enable_iceoryx_logs {
                "1"
            } else {
                "0"
            },
        );
    }

    // 初始化日志系统：将日志目录从配置的根目录
    // 切换到 <log_dir>/<cluster_name>_cluster_kv_logs 子目录，与客户端保持一致，避免在根目录堆积文件。
    let kv_logs_dir =
        Path::new(&config.log_dir).join(format!("{}_cluster_kv_logs", config.cluster_name));
    let observability_disabled = config.test_spec_config.disable_observability;
    let greptime_cfg_opt: Option<crate::config::GreptimeOtlpLogConfig> = if observability_disabled {
        None
    } else {
        config
            .monitoring
            .as_ref()
            .and_then(|m| m.otlp_log_api.as_ref())
            .cloned()
    };
    let (greptime_tracing_layer_opt, greptime_tracing_rx_opt) = match greptime_cfg_opt.as_ref() {
        None => (None, None),
        Some(cfg) => {
            let (layer, rx) =
                fluxon_observability::greptime_otlp_tracing::new_tracing_layer(cfg.max_queue_lines);
            (Some(layer), Some(rx))
        }
    };
    match greptime_tracing_layer_opt {
        None => fluxon_util::init_log(&kv_logs_dir, &config.instance_key),
        Some(layer) => {
            fluxon_util::init_log_with_extra_layer(&kv_logs_dir, &config.instance_key, layer)
        }
    }
    println!("Master config: {:?}", config);
    info!("Master config: {:?}", config);
    info!("Build version (git commit): {}", build_version);
    info!("Build version (source-sha256): {}", source_sha256);

    let mut metadata = HashMap::from([
        // ("role".to_string(), "master".to_string()),
        ("master".to_string(), "true".to_string()),
        ("version".to_string(), build_version.clone()),
        // ("instance_name".to_string(), config.instance_key.clone()),
    ]);
    if !observability_disabled {
        if let Some(observe_broadcast) =
            serialize_master_observe_broadcast(config.monitoring.as_ref())
        {
            metadata.insert(META_KEY_KV_OBSERVE_BROADCAST.to_string(), observe_broadcast);
        }
    }

    let rdma_control_init = test_overrides
        .as_ref()
        .map(|overrides| overrides.rdma_control_init.clone())
        .or_else(|| test_spec_config_rdma_control_init(Some(&config.test_spec_config)))
        .unwrap_or_else(|| {
            cluster_manager_rdma_control_init_from_transfer_config(
                config.transfer_engine,
                &config.protocol,
            )
        });
    let transfer_backend_activation_mode = test_overrides
        .as_ref()
        .and_then(|overrides| overrides.transfer_backend_activation_mode)
        .or_else(|| {
            test_spec_config_transfer_backend_activation_mode(
                &config.protocol,
                Some(&config.test_spec_config),
            )
        })
        .unwrap_or(TransferBackendActivationMode::RdmaControl);

    // Init args are role-specific (variant-specific) and generated from the init-step DAG.
    let init_args = InitArgsMaster {
        cluster_manager_arg: ClusterManagerNewArg {
            etcd_endpoints: config.etcd_endpoints.clone(),
            cluster_name: config.cluster_name.clone(),
            instance_name: Some(config.instance_key.clone()),
            port: None,
            metadata,
            local_ipc_root: None,
            rdma_control_init,
            sub_cluster: None,
            network: config.network.clone(),
        },
        p2p_arg: P2pModuleNewArg::new(
            config.port,
            tcp_thread_transport_tuning_from_test_spec_config(&config.test_spec_config),
            config.test_spec_config.disable_crossowner_ipc,
            config.test_spec_config.iceoryx_external_busy_poll,
        )
        .with_iceoryx_owner_client_busy_poll(config.test_spec_config.iceoryx_owner_client_busy_poll)
        .with_user_rpc_sync_handler_thread_count(
            config.test_spec_config.user_rpc_sync_handler_thread_count,
        ),
        client_transfer_engine_arg: ClientTransferEngineNewArg {
            metadata_uri: config.etcd_endpoints[0].clone(),
            instance_name: config.instance_key.clone(),
            enable_transfer_rpc_fast_path: config.enable_transfer_rpc_fast_path,
            rpc_port: 12345,
            protocol_type: config.protocol.protocol_type,
            rdma_device_names: transfer_engine_rdma_device_names_from_config(
                &config.protocol,
                Some(&config.test_spec_config),
            ),
            backend_activation_mode: transfer_backend_activation_mode,
            transfer_engine: config.transfer_engine,
        },
        master_seg_manager_arg: MasterSegManagerNewArg,
        master_kv_router_arg: MasterKvRouterNewArg {
            test_spec_config: config.test_spec_config.clone(),
        },
        metric_reporter_arg: MetricReporterNewArg {
            test_spec_config: config.test_spec_config.clone(),
        },
        master_lease_manager_arg: MasterLeaseManagerNewArg {
            cleanup_interval: 5,
            default_ttl: 30,
        },
    };

    // 创建并初始化框架
    let framework = Framework::new(format!(
        "fluxon_kv.master:{}:{}",
        config.cluster_name, config.instance_key
    ));
    info!("Initializing master framework...");

    init_framework_master(&framework, init_args)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to initialize framework: {:#}", e))?;

    if !observability_disabled {
        start_greptime_otlp_tracing_exporter_kv(
            framework.cluster_manager_view().clone(),
            framework.p2p_view().clone(),
            greptime_cfg_opt,
            greptime_tracing_rx_opt,
            &config.cluster_name,
            fluxon_observability::types::FluxonMemberRole::Master,
            &config.instance_key,
        );
    }

    let shutdown_waiter = framework.cluster_manager_view().register_shutdown_waiter();
    let kv_profiles_dir =
        Path::new(&config.log_dir).join(format!("{}_cluster_kv_profiles", config.cluster_name));
    profile::spawn_pprof_flamegraph_on_timeout_or_shutdown(
        config.pprof_duration_seconds,
        kv_profiles_dir,
        config.cluster_name.clone(),
        profile::PprofRole::Master,
        config.instance_key.clone(),
        shutdown_waiter,
    );

    let framework = Arc::new(framework);
    if crate::master_ui_monitor::try_start_master_ui_monitor(framework.clone(), &config)? {
        return Ok((framework, config));
    }

    Ok((framework, config))
}

pub async fn run_master(
    config_arg: ConfigArg<MasterConfig>,
) -> Result<(Arc<Framework>, MasterConfig)> {
    run_master_impl(config_arg, None).await
}

#[cfg(feature = "test_bins")]
pub(crate) async fn run_master_with_test_overrides(
    config_arg: ConfigArg<MasterConfig>,
    test_overrides: MasterRunTestOverrides,
) -> Result<(Arc<Framework>, MasterConfig)> {
    run_master_impl(config_arg, Some(test_overrides)).await
}

fn merge_startup_member_metadata(
    metadata: &mut HashMap<String, String>,
    extra_metadata: HashMap<String, String>,
) -> Result<()> {
    for (key, value) in extra_metadata {
        if key.trim().is_empty() {
            anyhow::bail!("startup member metadata key must be non-empty");
        }
        if value.trim().is_empty() {
            anyhow::bail!(
                "startup member metadata value must be non-empty: key={}",
                key
            );
        }
        if metadata.contains_key(&key) {
            anyhow::bail!(
                "startup member metadata key conflicts with built-in metadata: key={}",
                key
            );
        }
        metadata.insert(key, value);
    }
    Ok(())
}

async fn run_client_impl(
    config_arg: ConfigArg<ClientConfig>,
    test_overrides: Option<ClientRunTestOverrides>,
    startup_member_metadata: HashMap<String, String>,
) -> Result<(Arc<Framework>, ClientConfig)> {
    #[cfg(unix)]
    segfault_handler::install_sigsegv_classifier();

    println!("Starting cache backend in CLIENT mode");

    // 加载客户端配置
    let config = load_client_config(config_arg)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to load client config: {}", e))?;

    unsafe {
        std::env::set_var(
            "FLUXON_ENABLE_ICEORYX_LOGS",
            if config.test_spec_config.enable_iceoryx_logs {
                "1"
            } else {
                "0"
            },
        );
    }

    let build_version = fluxon_util::git_version_build_record::get_current_git_commitid().unwrap();
    let source_sha256 = fluxon_util::build_info::SOURCE_SHA256;

    // 初始化日志系统：将日志放到共享文件根目录
    // 下的 {cluster_name}_cluster_kv_logs 子目录，避免在 shm 根目录下展开普通文件。
    let kv_logs_dir = Path::new(&config.shared_file_path)
        .join(format!("{}_cluster_kv_logs", config.cluster_name));
    let observability_disabled = config.test_spec_config.disable_observability;
    let greptime_tracing_rx = if observability_disabled {
        fluxon_util::init_log(&kv_logs_dir, &config.instance_key);
        None
    } else {
        // Install the tracing layer on client nodes so OTLP log exporting can be
        // enabled purely via the master broadcast (monitoring.otlp_log_api), without per-node config.
        let (greptime_tracing_layer, greptime_tracing_rx) =
            fluxon_observability::greptime_otlp_tracing::new_tracing_layer(
                crate::config::DEFAULT_OTLP_LOG_MAX_QUEUE_LINES,
            );
        fluxon_util::init_log_with_extra_layer(
            &kv_logs_dir,
            &config.instance_key,
            greptime_tracing_layer,
        );
        Some(greptime_tracing_rx)
    };

    println!("Build version (git commit): {}", build_version);
    println!("Build version (source-sha256): {}", source_sha256);

    println!("Client config: {:?}", config);
    println!(
        "Client shared_memory_path resolved to: {:?}",
        config.shared_memory_path
    );

    info!("Client config: {:?}", config);
    info!(
        "Client shared_memory_path resolved to: {:?}",
        config.shared_memory_path
    );
    info!("Build version (git commit): {}", build_version);
    info!("Build version (source-sha256): {}", source_sha256);

    // Decide mode by contribution: external if dram==0 and all VRAM==0 or empty
    let dram = config.contribute_to_cluster_pool_size.dram;
    let vram_is_zero = config
        .contribute_to_cluster_pool_size
        .vram
        .values()
        .all(|&v| v == 0);
    let is_external = dram == 0 && vram_is_zero;
    let is_side_transfer_worker = is_external
        && matches!(
            config.test_spec_config.side_transfer_role,
            Some(SideTransferRole::Worker)
        );
    let mut bootstrapped_shared_meta: Option<SharedJsonMeta> = None;

    let config = if is_external {
        let shared_memory_dir = Path::new(&config.shared_memory_path);
        let shared_file_dir = Path::new(&config.shared_file_path);
        let shared_json_path = shared_file_dir.join("shared.json");
        let mmap_file_path = shared_memory_dir.join("mmap.file");

        let mut waited_ticks: u64 = 0;
        let (meta, meta_shm_canonical, meta_file_canonical, etcd_endpoints) = loop {
            if !shared_json_path.exists() || !mmap_file_path.exists() {
                limit_thirdparty::tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                waited_ticks += 1;
                if waited_ticks % 25 == 0 {
                    info!(
                        "Waiting owner shared bundle to be ready... ({}s), shm_dir={} file_dir={} (shared.json={}, mmap.file={})",
                        waited_ticks / 5,
                        shared_memory_dir.to_string_lossy(),
                        shared_file_dir.to_string_lossy(),
                        shared_json_path.exists(),
                        mmap_file_path.exists()
                    );
                }
                continue;
            }

            let shared_json_buf = match std::fs::read_to_string(&shared_json_path) {
                Ok(v) => v,
                Err(e) => {
                    limit_thirdparty::tokio::time::sleep(std::time::Duration::from_millis(200))
                        .await;
                    waited_ticks += 1;
                    if waited_ticks % 25 == 0 {
                        warn!(
                            "Waiting owner shared.json readable... ({}s), path={}, err={}",
                            waited_ticks / 5,
                            shared_json_path.to_string_lossy(),
                            e
                        );
                    }
                    continue;
                }
            };

            let meta: crate::SharedJsonMeta = match serde_json::from_str(&shared_json_buf) {
                Ok(v) => v,
                Err(e) => {
                    limit_thirdparty::tokio::time::sleep(std::time::Duration::from_millis(200))
                        .await;
                    waited_ticks += 1;
                    if waited_ticks % 25 == 0 {
                        warn!(
                            "Waiting owner shared.json schema ready... ({}s), path={}, err={}",
                            waited_ticks / 5,
                            shared_json_path.to_string_lossy(),
                            e
                        );
                    }
                    continue;
                }
            };

            if meta.protocol_version != build_version {
                limit_thirdparty::tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                waited_ticks += 1;
                if waited_ticks % 25 == 0 {
                    warn!(
                        "Waiting protocol_version match... ({}s), shm_dir='{}' file_dir='{}', shared='{}', local='{}'",
                        waited_ticks / 5,
                        shared_memory_dir.to_string_lossy(),
                        shared_file_dir.to_string_lossy(),
                        meta.protocol_version,
                        build_version
                    );
                }
                continue;
            }

            if meta.cluster_name != config.cluster_name {
                limit_thirdparty::tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                waited_ticks += 1;
                if waited_ticks % 25 == 0 {
                    warn!(
                        "Waiting cluster_name match... ({}s), shm_dir='{}' file_dir='{}', config='{}', shared.json='{}'",
                        waited_ticks / 5,
                        shared_memory_dir.to_string_lossy(),
                        shared_file_dir.to_string_lossy(),
                        config.cluster_name,
                        meta.cluster_name
                    );
                }
                continue;
            }

            let shared_memory_path_canonical = match std::fs::canonicalize(
                &config.shared_memory_path,
            ) {
                Ok(v) => v.to_string_lossy().into_owned(),
                Err(e) => {
                    limit_thirdparty::tokio::time::sleep(std::time::Duration::from_millis(200))
                        .await;
                    waited_ticks += 1;
                    if waited_ticks % 25 == 0 {
                        warn!(
                            "Waiting shared_memory_path canonicalizable... ({}s), shm_dir='{}', path='{}', err={}",
                            waited_ticks / 5,
                            shared_memory_dir.to_string_lossy(),
                            config.shared_memory_path,
                            e
                        );
                    }
                    continue;
                }
            };

            let meta_shm_canonical = match std::fs::canonicalize(&meta.shared_memory_path) {
                Ok(v) => v.to_string_lossy().into_owned(),
                Err(e) => {
                    limit_thirdparty::tokio::time::sleep(std::time::Duration::from_millis(200))
                        .await;
                    waited_ticks += 1;
                    if waited_ticks % 25 == 0 {
                        warn!(
                            "Waiting shared.json shared_memory_path canonicalizable... ({}s), shm_dir='{}', path='{}', err={}",
                            waited_ticks / 5,
                            shared_memory_dir.to_string_lossy(),
                            meta.shared_memory_path,
                            e
                        );
                    }
                    continue;
                }
            };
            let shared_file_path_canonical = match std::fs::canonicalize(&config.shared_file_path) {
                Ok(v) => v.to_string_lossy().into_owned(),
                Err(e) => {
                    limit_thirdparty::tokio::time::sleep(std::time::Duration::from_millis(200))
                        .await;
                    waited_ticks += 1;
                    if waited_ticks % 25 == 0 {
                        warn!(
                            "Waiting shared_file_path canonicalizable... ({}s), file_dir='{}', path='{}', err={}",
                            waited_ticks / 5,
                            shared_file_dir.to_string_lossy(),
                            config.shared_file_path,
                            e
                        );
                    }
                    continue;
                }
            };
            let meta_file_canonical = match std::fs::canonicalize(&meta.shared_file_path) {
                Ok(v) => v.to_string_lossy().into_owned(),
                Err(e) => {
                    limit_thirdparty::tokio::time::sleep(std::time::Duration::from_millis(200))
                        .await;
                    waited_ticks += 1;
                    if waited_ticks % 25 == 0 {
                        warn!(
                            "Waiting shared.json shared_file_path canonicalizable... ({}s), file_dir='{}', path='{}', err={}",
                            waited_ticks / 5,
                            shared_file_dir.to_string_lossy(),
                            meta.shared_file_path,
                            e
                        );
                    }
                    continue;
                }
            };

            if meta_shm_canonical != shared_memory_path_canonical {
                limit_thirdparty::tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                waited_ticks += 1;
                if waited_ticks % 25 == 0 {
                    warn!(
                        "Waiting shared_memory_path match... ({}s), shm_dir='{}', config='{}', shared.json='{}'",
                        waited_ticks / 5,
                        shared_memory_dir.to_string_lossy(),
                        shared_memory_path_canonical,
                        meta_shm_canonical
                    );
                }
                continue;
            }
            if meta_file_canonical != shared_file_path_canonical {
                limit_thirdparty::tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                waited_ticks += 1;
                if waited_ticks % 25 == 0 {
                    warn!(
                        "Waiting shared_file_path match... ({}s), file_dir='{}', config='{}', shared.json='{}'",
                        waited_ticks / 5,
                        shared_file_dir.to_string_lossy(),
                        shared_file_path_canonical,
                        meta_file_canonical
                    );
                }
                continue;
            }

            if meta.etcd_addresses.is_empty() {
                limit_thirdparty::tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                waited_ticks += 1;
                if waited_ticks % 25 == 0 {
                    warn!(
                        "Waiting shared.json etcd_addresses non-empty... ({}s), shm_dir='{}' file_dir='{}', shared_memory_path='{}'",
                        waited_ticks / 5,
                        shared_memory_dir.to_string_lossy(),
                        shared_file_dir.to_string_lossy(),
                        meta_shm_canonical
                    );
                }
                continue;
            }

            let etcd_endpoints = match normalize_etcd_addresses(&meta.etcd_addresses) {
                Ok(v) => v,
                Err(e) => {
                    limit_thirdparty::tokio::time::sleep(std::time::Duration::from_millis(200))
                        .await;
                    waited_ticks += 1;
                    if waited_ticks % 25 == 0 {
                        warn!(
                            "Waiting shared.json etcd_addresses valid... ({}s), shm_dir='{}' file_dir='{}', raw={:?}, err={}",
                            waited_ticks / 5,
                            shared_memory_dir.to_string_lossy(),
                            shared_file_dir.to_string_lossy(),
                            meta.etcd_addresses,
                            e
                        );
                    }
                    continue;
                }
            };

            break (
                meta,
                meta_shm_canonical,
                meta_file_canonical,
                etcd_endpoints,
            );
        };
        bootstrapped_shared_meta = Some(meta.clone());
        // External bootstrap contract:
        // - Config provides: instance_key, fluxonkv_spec.cluster_name, fluxonkv_spec.shared_memory_path,
        //   fluxonkv_spec.shared_file_path, fluxonkv_spec.p2p_listen_port.
        // - shared.json provides: cluster_name, etcd_addresses (raw), shared_memory_path (canonical),
        //   shared_file_path (canonical), protocol_version, sub_cluster.
        // - pprof_duration_seconds is not inherited; it is controlled solely by config.
        let mut final_config = config.clone();
        final_config.etcd_addresses_raw = meta.etcd_addresses.clone();
        final_config.fluxonkv_spec.etcd_addresses = etcd_endpoints;
        final_config.fluxonkv_spec.sub_cluster = meta.sub_cluster;
        final_config.shared_memory_path = meta_shm_canonical;
        final_config.shared_file_path = meta_file_canonical;
        final_config
    } else {
        config
    };

    if !is_external && config.test_spec_config.side_transfer_worker_count > 0 {
        // Bootstrap artifacts must be cleared before owner init. Owner init may already publish a
        // fresh shared.json bundle, and deleting it afterwards races side workers indefinitely.
        cleanup_stale_side_transfer_bootstrap_artifacts(&config).map_err(|err| {
            anyhow::anyhow!(
                "Failed to prepare side-transfer bootstrap state for owner {}: {:#}",
                config.instance_key,
                err
            )
        })?;
    }

    let mut metadata = if is_external {
        HashMap::from([
            ("external_client".to_string(), "true".to_string()),
            ("version".to_string(), build_version.clone()),
        ])
    } else {
        HashMap::from([
            ("client".to_string(), "true".to_string()),
            ("version".to_string(), build_version.clone()),
        ])
    };
    if is_side_transfer_worker {
        metadata.insert("side_transfer_worker".to_string(), "true".to_string());
    }

    // Local IPC routing requires both share-group owner id and the local IPC root.
    // The owner id is also published via a dedicated share-group key; we denormalize it into
    // member metadata to allow P2P to make decisions without scanning etcd.
    if !is_external {
        // Owner nodes are the global relay set. Keeping a single metadata marker converges
        // route selection, observability proxy eligibility, and ops topology rendering.
        metadata.insert("p2p_relay".to_string(), "true".to_string());
    }
    merge_startup_member_metadata(&mut metadata, startup_member_metadata)?;

    // MasterLeaseManager is master-only and is not constructed for owner/external variants.

    // 创建并初始化框架
    let role = if is_side_transfer_worker {
        "side_transfer_worker"
    } else if is_external {
        "external"
    } else {
        "owner"
    };
    let rdma_control_init = test_overrides
        .as_ref()
        .map(|overrides| overrides.rdma_control_init.clone())
        .or_else(|| test_spec_config_rdma_control_init(Some(&config.test_spec_config)))
        .unwrap_or_else(|| cluster_manager_rdma_control_init_from_config(&config));
    let transfer_backend_activation_mode = test_overrides
        .as_ref()
        .and_then(|overrides| overrides.transfer_backend_activation_mode)
        .or_else(|| {
            test_spec_config_transfer_backend_activation_mode(
                &config.protocol,
                Some(&config.test_spec_config),
            )
        })
        .unwrap_or(TransferBackendActivationMode::RdmaControl);
    let framework = Framework::new(format!(
        "fluxon_kv.client.{}:{}:{}",
        role, config.cluster_name, config.instance_key
    ));
    info!("Initializing client framework...");

    if is_external && !is_side_transfer_worker {
        let init_args = InitArgsExternal {
            cluster_manager_arg: ClusterManagerNewArg {
                etcd_endpoints: config.fluxonkv_spec.etcd_addresses.clone(),
                cluster_name: config.cluster_name.clone(),
                instance_name: Some(config.instance_key.clone()),
                port: None,
                metadata,
                local_ipc_root: cluster_manager_local_ipc_root(
                    &config.shared_memory_path,
                    &config.test_spec_config,
                ),
                rdma_control_init: rdma_control_init.clone(),
                sub_cluster: config.fluxonkv_spec.sub_cluster.clone(),
                network: None,
            },
            p2p_arg: P2pModuleNewArg::new(
                config.fluxonkv_spec.p2p_listen_port,
                tcp_thread_transport_tuning_from_test_spec_config(&config.test_spec_config),
                config.test_spec_config.disable_crossowner_ipc,
                config.test_spec_config.iceoryx_external_busy_poll,
            )
            .with_iceoryx_owner_client_busy_poll(
                config.test_spec_config.iceoryx_owner_client_busy_poll,
            )
            .with_user_rpc_sync_handler_thread_count(
                config.test_spec_config.user_rpc_sync_handler_thread_count,
            ),
            metric_reporter_arg: MetricReporterNewArg {
                test_spec_config: config.test_spec_config.clone(),
            },
            external_client_api_arg: ExternalClientApiNewArg {
                shared_memory_path: config.shared_memory_path.clone(),
                shared_file_path: config.shared_file_path.clone(),
                expected_cluster_name: config.cluster_name.clone(),
                expected_protocol_version: build_version.clone(),
                enable_side_transfer: config.test_spec_config.enable_side_transfer,
                short_circuit_put_payload_path: config
                    .test_spec_config
                    .short_circuit_put_payload_path,
            },
        };

        init_framework_external(&framework, init_args)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to initialize framework: {:#}", e))?;
    } else {
        let init_args = InitArgsOwner {
            cluster_manager_arg: ClusterManagerNewArg {
                etcd_endpoints: config.fluxonkv_spec.etcd_addresses.clone(),
                cluster_name: config.cluster_name.clone(),
                instance_name: Some(config.instance_key.clone()),
                port: None,
                metadata,
                local_ipc_root: cluster_manager_local_ipc_root(
                    &config.shared_memory_path,
                    &config.test_spec_config,
                ),
                rdma_control_init,
                sub_cluster: config.fluxonkv_spec.sub_cluster.clone(),
                network: None,
            },
            p2p_arg: P2pModuleNewArg::new(
                config.fluxonkv_spec.p2p_listen_port,
                tcp_thread_transport_tuning_from_test_spec_config(&config.test_spec_config),
                config.test_spec_config.disable_crossowner_ipc,
                config.test_spec_config.iceoryx_external_busy_poll,
            )
            .with_iceoryx_owner_client_busy_poll(
                config.test_spec_config.iceoryx_owner_client_busy_poll,
            )
            .with_user_rpc_sync_handler_thread_count(
                config.test_spec_config.user_rpc_sync_handler_thread_count,
            ),
            metric_reporter_arg: MetricReporterNewArg {
                test_spec_config: config.test_spec_config.clone(),
            },
            client_kv_api_arg: ClientKvApiNewArg {
                test_spec_config: config.test_spec_config.clone(),
            },
            client_seg_pool_arg: ClientSegPoolNewArg {
                contribute_size: config.contribute_to_cluster_pool_size.clone(),
                // Read shared memory path from config (must not be empty).
                shared_memory_path: config.shared_memory_path.clone(),
                shared_file_path: config.shared_file_path.clone(),
                cluster_name: config.cluster_name.clone(),
                etcd_addresses: config.etcd_addresses_raw.clone(),
                attach_existing_meta: if is_side_transfer_worker {
                    Some(bootstrapped_shared_meta.clone().ok_or_else(|| {
                        anyhow::anyhow!(
                            "side-transfer worker missing bootstrapped shared.json metadata"
                        )
                    })?)
                } else {
                    None
                },
                side_transfer_worker: is_side_transfer_worker,
                require_transfer_rpc_fast_path_ready_timeout: config
                    .test_spec_config
                    .require_transfer_rpc_fast_path_ready_timeout_seconds
                    .map(Duration::from_secs),
            },
            client_transfer_engine_arg: ClientTransferEngineNewArg {
                metadata_uri: config.fluxonkv_spec.etcd_addresses[0].clone(),
                instance_name: config.instance_key.clone(),
                enable_transfer_rpc_fast_path: config.fluxonkv_spec.enable_transfer_rpc_fast_path,
                rpc_port: 12345,
                protocol_type: config.protocol.protocol_type.clone(),
                rdma_device_names: transfer_engine_rdma_device_names_from_config(
                    &config.protocol,
                    Some(&config.test_spec_config),
                ),
                backend_activation_mode: transfer_backend_activation_mode,
                transfer_engine: config.fluxonkv_spec.transfer_engine.clone(),
            },
        };

        init_framework_owner(&framework, init_args)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to initialize framework: {:#}", e))?;
    }

    let framework = Arc::new(framework);

    if !is_external {
        if let Err(err) = framework
            .client_seg_pool_view()
            .client_seg_pool()
            .wait_required_transfer_rpc_fast_path_ready()
            .await
        {
            let _ = framework.shutdown().await;
            return Err(anyhow::anyhow!(
                "Owner {} failed required transfer-rpc fast-path readiness gate: {:#}",
                config.instance_key,
                err
            ));
        }
    }

    if !is_external && config.test_spec_config.side_transfer_worker_count > 0 {
        let launcher = match resolve_side_transfer_worker_launcher() {
            Ok(launcher) => launcher,
            Err(err) => {
                let _ = framework.shutdown().await;
                return Err(anyhow::anyhow!(
                    "Failed to resolve side-transfer launcher for owner {}: {:#}",
                    config.instance_key,
                    err
                ));
            }
        };
        let runtime_dir = side_transfer_runtime_dir(&config);
        let current_dir = std::env::current_dir().ok();
        let mut side_workers: BTreeMap<u16, SideTransferWorkerProcess> = BTreeMap::new();
        for worker_idx in 0..config.test_spec_config.side_transfer_worker_count {
            let side_worker = match start_side_transfer_worker(
                &config,
                &launcher,
                current_dir.as_deref(),
                &runtime_dir,
                worker_idx,
            ) {
                Ok(worker) => worker,
                Err(err) => {
                    shutdown_side_transfer_workers_blocking(
                        side_workers.into_values().collect::<Vec<_>>(),
                    );
                    let _ = framework.shutdown().await;
                    return Err(anyhow::anyhow!(
                        "Failed to spawn side-transfer worker {} for owner {}: {:#}",
                        worker_idx,
                        config.instance_key,
                        err
                    ));
                }
            };
            side_workers.insert(worker_idx, side_worker);
        }

        if let Err(err) =
            wait_for_side_transfer_workers_ready(&framework, &config, &mut side_workers).await
        {
            shutdown_side_transfer_workers_blocking(side_workers.into_values().collect::<Vec<_>>());
            let _ = framework.shutdown().await;
            return Err(anyhow::anyhow!(
                "Failed to start side-transfer workers for owner {}: {:#}",
                config.instance_key,
                err
            ));
        }

        let side_workers = Arc::new(limit_thirdparty::tokio::sync::AMutex::new(side_workers));

        let reconcile_children = side_workers.clone();
        let reconcile_view = framework.cluster_manager_view().clone();
        let reconcile_wait_view = reconcile_view.clone();
        let reconcile_owner_info = reconcile_view.cluster_manager().get_self_info();
        let reconcile_owner_config = config.clone();
        let reconcile_launcher = launcher.clone();
        let reconcile_runtime_dir = runtime_dir.clone();
        let reconcile_current_dir = current_dir.clone();
        let reconcile_spawn_view = reconcile_view.clone();
        let _ = reconcile_spawn_view.spawn("side_transfer_reconcile", async move {
            use limit_thirdparty::tokio;

            let mut shutdown_waiter = reconcile_wait_view.register_shutdown_waiter();
            let shutdown = shutdown_waiter.wait();
            tokio::pin!(shutdown);
            loop {
                tokio::select! {
                    _ = &mut shutdown => break,
                    _ = tokio::time::sleep(SIDE_TRANSFER_WORKER_RECONCILE_INTERVAL) => {}
                }

                let now = Instant::now();
                let mut missing = Vec::new();
                let removed_workers: Vec<(String, SideTransferWorkerProcess)> = {
                    let mut guard = reconcile_children.lock().await;
                    let cluster_manager = reconcile_view.cluster_manager();
                    let mut removed_reasons = Vec::new();
                    let existing_indices: Vec<u16> = guard.keys().copied().collect();
                    for worker_idx in existing_indices {
                        let Some(worker) = guard.get_mut(&worker_idx) else {
                            continue;
                        };
                        match worker.child.try_wait() {
                            Ok(Some(status)) => {
                                removed_reasons.push((
                                    worker_idx,
                                    format!("exited unexpectedly with status {}", status),
                                ));
                            }
                            Ok(None) => {
                                if is_side_transfer_worker_ready(
                                    cluster_manager,
                                    &reconcile_owner_config.shared_memory_path,
                                    &reconcile_owner_info.id,
                                    reconcile_owner_info.node_start_time,
                                    &worker.side_id,
                                ) {
                                    worker.not_ready_since = None;
                                } else {
                                    let not_ready_since =
                                        worker.not_ready_since.get_or_insert(now);
                                    if now.duration_since(*not_ready_since)
                                        >= SIDE_TRANSFER_WORKER_READY_TIMEOUT
                                    {
                                        removed_reasons.push((
                                            worker_idx,
                                            format!(
                                                "not ready for >= {}s",
                                                SIDE_TRANSFER_WORKER_READY_TIMEOUT.as_secs()
                                            ),
                                        ));
                                    }
                                }
                            }
                            Err(err) => {
                                removed_reasons.push((
                                    worker_idx,
                                    format!("status poll failed: {}", err),
                                ));
                            }
                        }
                    }

                    let remove_indices: Vec<u16> =
                        removed_reasons.iter().map(|(idx, _)| *idx).collect();
                    let removed_processes: Vec<SideTransferWorkerProcess> = remove_indices
                        .into_iter()
                        .filter_map(|worker_idx| guard.remove(&worker_idx))
                        .collect();

                    for worker_idx in
                        0..reconcile_owner_config.test_spec_config.side_transfer_worker_count
                    {
                        if !guard.contains_key(&worker_idx) {
                            missing.push(worker_idx);
                        }
                    }
                    removed_reasons
                        .into_iter()
                        .zip(removed_processes.into_iter())
                        .map(|((_, reason), worker)| (reason, worker))
                        .collect()
                };

                for (reason, worker) in removed_workers {
                    warn!(
                        worker_idx = worker.worker_idx,
                        side_id = %worker.side_id,
                        config = %worker.config_path.display(),
                        stdout_log = %worker.stdout_path.display(),
                        stderr_log = %worker.stderr_path.display(),
                        reason = %reason,
                        details = %format_side_transfer_worker_output_tails(&worker),
                        "side-transfer worker removed from reconcile set"
                    );
                    stop_side_transfer_worker_blocking(worker, true);
                }

                let mut started = Vec::new();
                for worker_idx in missing {
                    match start_side_transfer_worker(
                        &reconcile_owner_config,
                        &reconcile_launcher,
                        reconcile_current_dir.as_deref(),
                        &reconcile_runtime_dir,
                        worker_idx,
                    ) {
                        Ok(worker) => {
                            info!(
                                worker_idx = worker.worker_idx,
                                side_id = %worker.side_id,
                                config = %worker.config_path.display(),
                                "spawned side-transfer worker from reconcile loop"
                            );
                            started.push(worker);
                        }
                        Err(err) => {
                            warn!(
                                worker_idx,
                                err = %err,
                                "failed to start side-transfer worker from reconcile loop"
                            );
                        }
                    }
                }

                if !started.is_empty() {
                    let mut guard = reconcile_children.lock().await;
                    for worker in started {
                        if let std::collections::btree_map::Entry::Vacant(entry) =
                            guard.entry(worker.worker_idx)
                        {
                            entry.insert(worker);
                        } else {
                            warn!(
                                worker_idx = worker.worker_idx,
                                side_id = %worker.side_id,
                                "reconcile spawned duplicate side-transfer worker; stopping extra process"
                            );
                            stop_side_transfer_worker_blocking(worker, true);
                        }
                    }
                }
            }
        });

        let supervisor_children = side_workers.clone();
        let supervisor_view = framework.cluster_manager_view().clone();
        let supervisor_wait_view = supervisor_view.clone();
        let _ = supervisor_view.spawn("side_transfer_supervisor", async move {
            let mut shutdown_waiter = supervisor_wait_view.register_shutdown_waiter();
            shutdown_waiter.wait().await;
            let side_workers = {
                let mut guard = supervisor_children.lock().await;
                std::mem::take(&mut *guard)
                    .into_values()
                    .collect::<Vec<_>>()
            };
            if let Err(err) = limit_thirdparty::tokio::task::spawn_blocking(move || {
                shutdown_side_transfer_workers_blocking(side_workers);
            })
            .await
            {
                warn!(err = ?err, "side-transfer worker shutdown join failed");
            }
        });
    }

    // Start OTLP log exporter loop after the framework is initialized and the master member is visible.
    // Clients do not carry their own log exporter config; they only consume the KV broadcast from master metadata.
    if !observability_disabled {
        let otlp_cluster_name = config.cluster_name.clone();
        let otlp_member_id = config.instance_key.clone();
        let otlp_role = if is_side_transfer_worker {
            fluxon_observability::types::FluxonMemberRole::SideTransferWorker
        } else if is_external {
            fluxon_observability::types::FluxonMemberRole::ExternalClient
        } else {
            fluxon_observability::types::FluxonMemberRole::OwnerClient
        };
        let cm_view = framework.cluster_manager_view().clone();
        let p2p_view = framework.p2p_view().clone();
        let spawner = cm_view.clone();
        let _ = spawner.spawn("wait_master_otlp_log_api", async move {
            let outcome = wait_master_observe_broadcast(
                &cm_view,
                std::time::Duration::from_secs(60),
                std::time::Duration::from_secs(10),
            )
            .await;
            let Some(cfg) = outcome.otlp_log_api() else {
                warn!("OTLP log exporter disabled: master metadata does not carry otlp_log_api");
                return;
            };

            start_greptime_otlp_tracing_exporter_kv(
                cm_view,
                p2p_view,
                Some(cfg),
                greptime_tracing_rx,
                &otlp_cluster_name,
                otlp_role,
                &otlp_member_id,
            );
        });
    }

    if let Some(listen_addr) = config.redis_compat_listen_addr {
        if let Err(e) = redis_compat::start_redis_compat_server(framework.clone(), listen_addr) {
            let _ = framework.shutdown().await;
            return Err(anyhow::anyhow!(
                "Failed to start redis_compat server on {}: {}",
                listen_addr,
                e
            ));
        }
        info!(
            "redis_compat RESP2 server started at {} (value_field='payload')",
            listen_addr
        );
    }

    let shutdown_waiter = framework.cluster_manager_view().register_shutdown_waiter();
    let kv_profiles_dir = Path::new(&config.shared_file_path)
        .join(format!("{}_cluster_kv_profiles", config.cluster_name));
    profile::spawn_pprof_flamegraph_on_timeout_or_shutdown(
        config.pprof_duration_seconds,
        kv_profiles_dir,
        config.cluster_name.clone(),
        profile::PprofRole::Client,
        config.instance_key.clone(),
        shutdown_waiter,
    );

    Ok((framework, config))
}

pub async fn run_client(
    config_arg: ConfigArg<ClientConfig>,
) -> Result<(Arc<Framework>, ClientConfig)> {
    run_client_impl(config_arg, None, HashMap::new()).await
}

pub async fn run_client_with_startup_member_metadata(
    config_arg: ConfigArg<ClientConfig>,
    startup_member_metadata: HashMap<String, String>,
) -> Result<(Arc<Framework>, ClientConfig)> {
    run_client_impl(config_arg, None, startup_member_metadata).await
}

#[cfg(feature = "test_bins")]
pub(crate) async fn run_client_with_test_overrides(
    config_arg: ConfigArg<ClientConfig>,
    test_overrides: ClientRunTestOverrides,
) -> Result<(Arc<Framework>, ClientConfig)> {
    run_client_impl(config_arg, Some(test_overrides), HashMap::new()).await
}

mod redis_compat;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        ClientConfig, ContributeToClusterPoolSize, FluxonKvSpec, ProtocolConfig, SideTransferRole,
        TestSpecConfig, TestSpecTransportMode, TransferEngineType,
    };
    use fluxon_commu::ProtocolType;
    use std::collections::HashMap;
    use std::path::Path;
    use uuid::Uuid;

    fn new_test_dir(prefix: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("{}_{}", prefix, Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn owner_config_for_side_transfer_test() -> ClientConfig {
        ClientConfig {
            cluster_name: "test_cluster".to_string(),
            etcd_addresses_raw: vec!["127.0.0.1:2379".to_string()],
            instance_key: "owner-a".to_string(),
            contribute_to_cluster_pool_size: ContributeToClusterPoolSize {
                dram: 64 * 1024 * 1024,
                vram: HashMap::new(),
            },
            protocol: ProtocolConfig {
                protocol_type: ProtocolType::Tcp,
                rdma_device_names: None,
            },
            pprof_duration_seconds: None,
            redis_compat_listen_addr: None,
            fluxonkv_spec: FluxonKvSpec {
                etcd_addresses: vec!["http://127.0.0.1:2379".to_string()],
                cluster_name: "test_cluster".to_string(),
                p2p_listen_port: Some(41000),
                transfer_engine: TransferEngineType::P2p,
                enable_transfer_rpc_fast_path: true,
                sub_cluster: Some("owner-sub".to_string()),
            },
            shared_memory_path: "/tmp/fluxon_side_transfer_test".to_string(),
            shared_file_path: "/tmp/fluxon_side_transfer_test_files".to_string(),
            test_spec_config: TestSpecConfig {
                enable_side_transfer: true,
                side_transfer_worker_count: 4,
                side_transfer_worker_p2p_port_base: Some(42000),
                ..Default::default()
            },
        }
    }

    #[test]
    fn test_spec_transport_mode_uses_protocol_agnostic_force_enable_activation() {
        let cfg = TestSpecConfig {
            transport_mode: Some(TestSpecTransportMode::TransferOnly),
            ..Default::default()
        };
        assert_eq!(
            test_spec_config_transfer_backend_activation_mode(
                &ProtocolConfig {
                    protocol_type: ProtocolType::Tcp,
                    rdma_device_names: None,
                },
                Some(&cfg),
            ),
            Some(TransferBackendActivationMode::TcpTestBypassRdmaControl)
        );
    }

    #[test]
    fn test_spec_transport_mode_keeps_rdma_on_normal_rdma_control() {
        let cfg = TestSpecConfig {
            transport_mode: Some(TestSpecTransportMode::TransferWithRpc),
            rdma_device_names: Some(vec!["mlx5_1".to_string()]),
            ..Default::default()
        };
        assert_eq!(
            test_spec_config_transfer_backend_activation_mode(
                &ProtocolConfig {
                    protocol_type: ProtocolType::Rdma,
                    rdma_device_names: None,
                },
                Some(&cfg),
            ),
            None
        );
    }

    #[test]
    fn test_spec_rdma_control_init_locks_explicit_devices() {
        let cfg = TestSpecConfig {
            transport_mode: Some(TestSpecTransportMode::TransferOnly),
            rdma_device_names: Some(vec!["mlx5_1".to_string(), "mlx5_0".to_string()]),
            ..Default::default()
        };
        assert_eq!(
            test_spec_config_rdma_control_init(Some(&cfg)),
            Some(ClusterManagerRdmaControlInit::LockedExplicitDevices(vec![
                "mlx5_1".to_string(),
                "mlx5_0".to_string(),
            ]))
        );
    }

    #[test]
    fn derive_short_local_ipc_root_is_stable_for_canonical_path() {
        let tempdir = new_test_dir("fluxon_local_ipc_root_stable");
        let shared_memory_root = tempdir.join("owner_shm");
        std::fs::create_dir_all(&shared_memory_root).unwrap();

        let canonical = std::fs::canonicalize(&shared_memory_root).unwrap();
        let alias_a = derive_short_local_ipc_root(shared_memory_root.to_str().unwrap()).unwrap();
        let alias_b = derive_short_local_ipc_root(canonical.to_str().unwrap()).unwrap();

        assert_eq!(alias_a, alias_b);
        assert!(alias_a.starts_with("/tmp/fluxon_ipc/"));
        assert_ne!(alias_a, canonical.to_string_lossy());
        std::fs::remove_dir_all(&tempdir).unwrap();
    }

    #[test]
    fn derive_short_local_ipc_root_keeps_iceoryx_event_path_short() {
        let tempdir = new_test_dir("fluxon_local_ipc_root_short");
        let shared_memory_root = tempdir.join(
            "this_is_a_deliberately_long_shared_memory_root_name_for_iceoryx_socket_length_checks",
        );
        let alias = derive_short_local_ipc_root(shared_memory_root.to_str().unwrap()).unwrap();
        let example_event_path = format!("{}/iox2_254771654226413701181693419284.event", alias);

        assert!(Path::new(&alias).is_absolute());
        assert!(
            example_event_path.len() < 108,
            "event path must stay below AF_UNIX limit: len={} path={}",
            example_event_path.len(),
            example_event_path
        );
        std::fs::remove_dir_all(&tempdir).unwrap();
    }

    #[test]
    fn cluster_manager_local_ipc_root_respects_test_disable_switch() {
        let tempdir = new_test_dir("fluxon_local_ipc_root_disable_switch");
        let shared_memory_root = tempdir.join("owner_shm");
        std::fs::create_dir_all(&shared_memory_root).unwrap();

        let enabled = cluster_manager_local_ipc_root(
            shared_memory_root.to_str().unwrap(),
            &TestSpecConfig::default(),
        );
        assert!(enabled.is_some());

        let disabled = cluster_manager_local_ipc_root(
            shared_memory_root.to_str().unwrap(),
            &TestSpecConfig {
                disable_local_ipc: true,
                ..Default::default()
            },
        );
        assert_eq!(disabled, None);

        std::fs::remove_dir_all(&tempdir).unwrap();
    }

    #[test]
    fn transfer_engine_rdma_device_names_prefers_test_spec_devices() {
        let protocol = ProtocolConfig {
            protocol_type: ProtocolType::Rdma,
            rdma_device_names: Some("mlx5_9:1".to_string()),
        };
        let cfg = TestSpecConfig {
            rdma_device_names: Some(vec!["mlx5_1".to_string(), "mlx5_0".to_string()]),
            ..Default::default()
        };
        assert_eq!(
            transfer_engine_rdma_device_names_from_config(&protocol, Some(&cfg)),
            Some("mlx5_1,mlx5_0".to_string())
        );
    }

    #[test]
    fn build_side_transfer_worker_config_sets_zero_contribution_and_worker_role() {
        let mut owner_cfg = owner_config_for_side_transfer_test();
        owner_cfg.protocol = ProtocolConfig {
            protocol_type: ProtocolType::Rdma,
            rdma_device_names: Some("mlx5_2:1".to_string()),
        };
        owner_cfg.fluxonkv_spec.transfer_engine = TransferEngineType::Closed;
        owner_cfg.fluxonkv_spec.enable_transfer_rpc_fast_path = true;
        owner_cfg.test_spec_config.transport_mode = Some(TestSpecTransportMode::TransferWithRpc);
        owner_cfg.test_spec_config.rdma_device_names = Some(vec!["mlx5_2".to_string()]);
        let side_cfg =
            build_side_transfer_worker_config(&owner_cfg, 2).expect("build side worker config");

        assert_eq!(side_cfg.instance_key, "owner-a__side_2");
        assert_eq!(side_cfg.contribute_to_cluster_pool_size.dram, 0);
        assert!(side_cfg.contribute_to_cluster_pool_size.vram.is_empty());
        assert_eq!(side_cfg.protocol.protocol_type, ProtocolType::Tcp);
        assert_eq!(side_cfg.protocol.rdma_device_names, None);
        assert_eq!(side_cfg.fluxonkv_spec.etcd_addresses, Vec::<String>::new());
        assert_eq!(side_cfg.fluxonkv_spec.sub_cluster, None);
        assert_eq!(side_cfg.fluxonkv_spec.p2p_listen_port, Some(42002));
        assert_eq!(
            side_cfg.fluxonkv_spec.transfer_engine,
            TransferEngineType::P2p
        );
        assert!(!side_cfg.fluxonkv_spec.enable_transfer_rpc_fast_path);
        assert_eq!(
            side_cfg.test_spec_config.side_transfer_role,
            Some(SideTransferRole::Worker)
        );
        assert_eq!(side_cfg.test_spec_config.side_transfer_worker_count, 0);
        assert_eq!(
            side_cfg.test_spec_config.side_transfer_worker_p2p_port_base,
            None
        );
        assert_eq!(side_cfg.test_spec_config.transport_mode, None);
        assert_eq!(side_cfg.test_spec_config.rdma_device_names, None);
        assert!(side_cfg.test_spec_config.enable_side_transfer);
        assert!(!side_cfg.test_spec_config.disable_local_ipc);
    }

    #[test]
    fn build_side_transfer_worker_config_allows_auto_port_selection() {
        let mut owner_cfg = owner_config_for_side_transfer_test();
        owner_cfg
            .test_spec_config
            .side_transfer_worker_p2p_port_base = None;

        let side_cfg =
            build_side_transfer_worker_config(&owner_cfg, 2).expect("build side worker config");

        assert_eq!(side_cfg.instance_key, "owner-a__side_2");
        assert_eq!(side_cfg.fluxonkv_spec.p2p_listen_port, None);
        assert_eq!(
            side_cfg.fluxonkv_spec.transfer_engine,
            TransferEngineType::P2p
        );
        assert!(!side_cfg.fluxonkv_spec.enable_transfer_rpc_fast_path);
        assert_eq!(
            side_cfg.test_spec_config.side_transfer_role,
            Some(SideTransferRole::Worker)
        );
        assert_eq!(side_cfg.test_spec_config.side_transfer_worker_count, 0);
        assert_eq!(
            side_cfg.test_spec_config.side_transfer_worker_p2p_port_base,
            None
        );
        assert!(side_cfg.test_spec_config.enable_side_transfer);
    }

    #[test]
    fn build_side_transfer_worker_config_preserves_test_local_ipc_override() {
        let mut owner_cfg = owner_config_for_side_transfer_test();
        owner_cfg.test_spec_config.disable_local_ipc = true;

        let side_cfg =
            build_side_transfer_worker_config(&owner_cfg, 1).expect("build side worker config");

        assert!(side_cfg.test_spec_config.disable_local_ipc);
    }

    #[test]
    fn build_side_transfer_worker_config_yaml_omits_owner_only_fields() {
        let owner_cfg = owner_config_for_side_transfer_test();
        let side_cfg_yaml = build_side_transfer_worker_config_yaml(&owner_cfg, 1)
            .expect("build side worker config yaml");

        assert_eq!(side_cfg_yaml.instance_key, "owner-a__side_1");
        assert_eq!(
            side_cfg_yaml.protocol,
            Some(ProtocolConfig {
                protocol_type: ProtocolType::Tcp,
                rdma_device_names: None,
            })
        );
        assert!(side_cfg_yaml.contribute_to_cluster_pool_size.is_none());
        assert!(side_cfg_yaml.fluxonkv_spec.etcd_addresses.is_none());
        assert!(side_cfg_yaml.fluxonkv_spec.sub_cluster.is_none());
        assert_eq!(side_cfg_yaml.fluxonkv_spec.p2p_listen_port, Some(42001));
        assert_eq!(
            side_cfg_yaml.test_spec_config.side_transfer_role,
            Some(SideTransferRole::Worker)
        );
    }

    #[test]
    fn current_exe_name_helpers_detect_python_and_fluxon_kv() {
        assert!(current_exe_looks_like_python(Path::new(
            "/usr/bin/python3.11"
        )));
        assert!(current_exe_looks_like_fluxon_kv(Path::new(
            "/tmp/target/debug/fluxon_kv"
        )));
        assert!(!current_exe_looks_like_fluxon_kv(Path::new(
            "/tmp/target/debug/python3"
        )));
    }
}
