use crate::rpcresp_kvresult_convert::msg_and_error::{ConfigError, KvResult};
use fluxon_commu::validate_ip_cidr;
pub use fluxon_commu::{
    ClusterManagerRdmaControlInit, NetworkConfig, ProtocolType, TransferBackendActivationMode,
    TransferEngineType,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;

/// YAML wrapper to distinguish between:
/// - key missing: `Option::None`
/// - key present with null: `Some(YamlNullable::Null)`
/// - key present with a value: `Some(YamlNullable::Value(T))`
///
/// This aligns Rust-side validation with the Python-side "key presence" contract
/// (some fields are forbidden in certain modes even when explicitly set to null).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum YamlNullable<T> {
    Null,
    Value(T),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GreptimeOtlpLogConfigYaml {
    pub otlp_endpoint: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub db_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flush_interval_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_batch_lines: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_queue_lines: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GreptimeOtlpLogConfig {
    pub otlp_endpoint: String,
    pub db_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_name: Option<String>,
    pub flush_interval_ms: u64,
    pub max_batch_lines: usize,
    pub max_queue_lines: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MonitoringConfigYaml {
    pub prometheus_base_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prom_remote_write_url: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub otlp_log_api: Option<GreptimeOtlpLogConfigYaml>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MonitoringConfig {
    pub prometheus_base_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prom_remote_write_url: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub otlp_log_api: Option<GreptimeOtlpLogConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MasterUiConfigYaml {
    pub http_listen_addr: String,
}

#[derive(Debug, Clone)]
pub struct MasterUiConfig {
    pub http_listen_addr: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TestSpecTransportMode {
    TransferOnly,
    TransferWithRpc,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SideTransferRole {
    Worker,
}

const TEST_SPEC_TCP_THREAD_REACTOR_SHARD_COUNT_MIN: u8 = 1;
const TEST_SPEC_TCP_THREAD_REACTOR_SHARD_COUNT_MAX: u8 = 16;
const TEST_SPEC_TCP_THREAD_BULK_LANE_COUNT_MIN: u8 = 1;
const TEST_SPEC_TCP_THREAD_BULK_LANE_COUNT_MAX: u8 = 8;
const TEST_SPEC_TCP_THREAD_CONTROL_LANE_COUNT_MIN: u8 = 1;
const TEST_SPEC_TCP_THREAD_CONTROL_LANE_COUNT_MAX: u8 = 8;

fn default_iceoryx_owner_client_busy_poll() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TestSpecConfig {
    #[serde(default)]
    pub disable_observability: bool,
    #[serde(default)]
    pub disable_master_replica_cache: bool,
    #[serde(default)]
    pub disable_prefix_index: bool,
    #[serde(default)]
    pub disable_local_ipc: bool,
    #[serde(default)]
    pub disable_crossowner_ipc: bool,
    #[serde(default)]
    pub enable_iceoryx_logs: bool,
    #[serde(default)]
    pub iceoryx_external_busy_poll: bool,
    #[serde(default = "default_iceoryx_owner_client_busy_poll")]
    pub iceoryx_owner_client_busy_poll: bool,
    #[serde(default)]
    pub prefer_local_placement: bool,
    #[serde(default)]
    pub short_circuit_put_payload_path: bool,
    #[serde(default)]
    pub skip_put_end_commit: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport_mode: Option<TestSpecTransportMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tcp_thread_reactor_shard_count: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tcp_thread_bulk_lane_count: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tcp_thread_control_lane_count: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_rpc_sync_handler_thread_count: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rdma_device_names: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_transfer_rpc_fast_path_ready_timeout_seconds: Option<u64>,
    #[serde(default)]
    pub enable_side_transfer: bool,
    #[serde(default)]
    pub side_transfer_worker_count: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side_transfer_worker_p2p_port_base: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side_transfer_role: Option<SideTransferRole>,
}

impl Default for TestSpecConfig {
    fn default() -> Self {
        Self {
            disable_observability: false,
            disable_master_replica_cache: false,
            disable_prefix_index: false,
            disable_local_ipc: false,
            disable_crossowner_ipc: false,
            enable_iceoryx_logs: false,
            iceoryx_external_busy_poll: false,
            iceoryx_owner_client_busy_poll: default_iceoryx_owner_client_busy_poll(),
            prefer_local_placement: false,
            short_circuit_put_payload_path: false,
            skip_put_end_commit: false,
            transport_mode: None,
            tcp_thread_reactor_shard_count: None,
            tcp_thread_bulk_lane_count: None,
            tcp_thread_control_lane_count: None,
            user_rpc_sync_handler_thread_count: None,
            rdma_device_names: None,
            require_transfer_rpc_fast_path_ready_timeout_seconds: None,
            enable_side_transfer: false,
            side_transfer_worker_count: 0,
            side_transfer_worker_p2p_port_base: None,
            side_transfer_role: None,
        }
    }
}

fn resolve_enable_transfer_rpc_fast_path(
    default_enabled: bool,
    test_spec_config: Option<&TestSpecConfig>,
) -> bool {
    match test_spec_config.and_then(|cfg| cfg.transport_mode) {
        Some(TestSpecTransportMode::TransferOnly) => false,
        Some(TestSpecTransportMode::TransferWithRpc) => true,
        None => default_enabled,
    }
}

fn materialize_default_test_spec_transport_mode(test_spec_config: &mut TestSpecConfig) {
    if test_spec_config.transport_mode.is_some() {
        return;
    }
    if matches!(
        test_spec_config.side_transfer_role,
        Some(SideTransferRole::Worker)
    ) {
        return;
    }
    test_spec_config.transport_mode = Some(TestSpecTransportMode::TransferWithRpc);
}

fn normalize_test_spec_rdma_device_names(
    test_spec_config: &mut TestSpecConfig,
    transport_mode_was_explicit: bool,
) -> KvResult<Option<Vec<String>>> {
    let is_side_transfer_worker = matches!(
        test_spec_config.side_transfer_role,
        Some(SideTransferRole::Worker)
    );
    if transport_mode_was_explicit && test_spec_config.rdma_device_names.is_none() {
        return Err(ConfigError::InvalidClientConfig {
            detail: "explicit test_spec_config.transport_mode now requires test_spec_config.rdma_device_names to avoid implicit RDMA device selection".to_string(),
        }
        .into_kverror());
    }

    let Some(raw_devices) = test_spec_config.rdma_device_names.take() else {
        return Ok(None);
    };

    if is_side_transfer_worker && !transport_mode_was_explicit {
        return Err(ConfigError::InvalidClientConfig {
            detail: "test_spec_config.rdma_device_names requires test_spec_config.transport_mode"
                .to_string(),
        }
        .into_kverror());
    }

    let mut deduped = std::collections::BTreeSet::new();
    for (idx, raw) in raw_devices.into_iter().enumerate() {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(ConfigError::InvalidClientConfig {
                detail: format!(
                    "test_spec_config.rdma_device_names[{}] must be a non-empty string",
                    idx
                ),
            }
            .into_kverror());
        }
        deduped.insert(trimmed.to_string());
    }

    let normalized: Vec<String> = deduped.into_iter().collect();
    if normalized.is_empty() {
        return Err(ConfigError::InvalidClientConfig {
            detail: "test_spec_config.rdma_device_names must not be empty".to_string(),
        }
        .into_kverror());
    }

    test_spec_config.rdma_device_names = Some(normalized.clone());
    Ok(Some(normalized))
}

fn validate_required_transfer_rpc_fast_path_ready_timeout(
    test_spec_config: &TestSpecConfig,
) -> KvResult<()> {
    let Some(timeout_seconds) =
        test_spec_config.require_transfer_rpc_fast_path_ready_timeout_seconds
    else {
        return Ok(());
    };

    if timeout_seconds == 0 {
        return Err(ConfigError::InvalidClientConfig {
            detail:
                "test_spec_config.require_transfer_rpc_fast_path_ready_timeout_seconds must be > 0"
                    .to_string(),
        }
        .into_kverror());
    }
    if test_spec_config.transport_mode != Some(TestSpecTransportMode::TransferWithRpc) {
        return Err(ConfigError::InvalidClientConfig {
            detail: "test_spec_config.require_transfer_rpc_fast_path_ready_timeout_seconds requires test_spec_config.transport_mode=transfer_with_rpc".to_string(),
        }
        .into_kverror());
    }
    if test_spec_config.rdma_device_names.is_none() {
        return Err(ConfigError::InvalidClientConfig {
            detail: "test_spec_config.require_transfer_rpc_fast_path_ready_timeout_seconds requires explicit test_spec_config.rdma_device_names".to_string(),
        }
        .into_kverror());
    }

    Ok(())
}

fn apply_test_spec_rdma_device_names_to_protocol(
    mut protocol: ProtocolConfig,
    normalized_rdma_device_names: Option<&Vec<String>>,
) -> ProtocolConfig {
    if matches!(protocol.protocol_type, ProtocolType::Rdma) {
        protocol.rdma_device_names = normalized_rdma_device_names.map(|devices| devices.join(","));
    }
    protocol
}

fn validate_test_spec_optional_u8_range(
    value: Option<u8>,
    field_name: &str,
    min: u8,
    max: u8,
) -> KvResult<()> {
    let Some(value) = value else {
        return Ok(());
    };
    if value < min || value > max {
        return Err(ConfigError::InvalidClientConfig {
            detail: format!("{field_name} must be in [{min}, {max}], got {value}"),
        }
        .into_kverror());
    }
    Ok(())
}

fn validate_test_spec_tcp_thread_tuning(test_spec_config: &TestSpecConfig) -> KvResult<()> {
    validate_test_spec_optional_u8_range(
        test_spec_config.tcp_thread_reactor_shard_count,
        "test_spec_config.tcp_thread_reactor_shard_count",
        TEST_SPEC_TCP_THREAD_REACTOR_SHARD_COUNT_MIN,
        TEST_SPEC_TCP_THREAD_REACTOR_SHARD_COUNT_MAX,
    )?;
    validate_test_spec_optional_u8_range(
        test_spec_config.tcp_thread_bulk_lane_count,
        "test_spec_config.tcp_thread_bulk_lane_count",
        TEST_SPEC_TCP_THREAD_BULK_LANE_COUNT_MIN,
        TEST_SPEC_TCP_THREAD_BULK_LANE_COUNT_MAX,
    )?;
    validate_test_spec_optional_u8_range(
        test_spec_config.tcp_thread_control_lane_count,
        "test_spec_config.tcp_thread_control_lane_count",
        TEST_SPEC_TCP_THREAD_CONTROL_LANE_COUNT_MIN,
        TEST_SPEC_TCP_THREAD_CONTROL_LANE_COUNT_MAX,
    )?;
    if let Some(value) = test_spec_config.user_rpc_sync_handler_thread_count {
        if value == 0 {
            return Err(ConfigError::InvalidTestConfig {
                detail: "test_spec_config.user_rpc_sync_handler_thread_count must be > 0"
                    .to_string(),
            }
            .into_kverror());
        }
    }
    Ok(())
}

fn transfer_engine_supports_rpc_fast_path(transfer_engine: TransferEngineType) -> bool {
    matches!(transfer_engine, TransferEngineType::Closed)
}

fn cluster_scoped_shared_path(root: &str, cluster_name: &str) -> KvResult<String> {
    let trimmed_root = root.trim();
    if trimmed_root.is_empty() {
        return Err(ConfigError::InvalidInstanceKey {
            key: "shared path root cannot be empty".to_string(),
        }
        .into_kverror());
    }
    let trimmed_cluster = cluster_name.trim();
    if trimmed_cluster.is_empty() {
        return Err(ConfigError::InvalidClusterName {
            name: cluster_name.to_string(),
        }
        .into_kverror());
    }
    let scoped: PathBuf = Path::new(trimmed_root).join(trimmed_cluster);
    Ok(scoped.to_string_lossy().into_owned())
}

fn resolve_compiled_rdma_transfer_engine() -> KvResult<TransferEngineType> {
    Ok(TransferEngineType::Closed)
}

fn resolve_transfer_engine_for_test_spec(
    _test_spec_config: Option<&TestSpecConfig>,
) -> KvResult<TransferEngineType> {
    resolve_compiled_rdma_transfer_engine()
}

fn resolve_transfer_engine_for_protocol_and_test_spec(
    protocol: &ProtocolConfig,
    test_spec_config: Option<&TestSpecConfig>,
) -> KvResult<TransferEngineType> {
    if matches!(protocol.protocol_type, ProtocolType::Tcp) {
        return Err(ConfigError::InvalidClientConfig {
            detail:
                "protocol.protocol_type=tcp is not supported in the public bundled-runtime build; closed runtime is RDMA-only"
                    .to_string(),
        }
        .into_kverror());
    }
    resolve_transfer_engine_for_test_spec(test_spec_config)
}

// Defaults for `monitoring.otlp_log_api`.
//
// Causal chain:
// - User config should stay minimal: `otlp_endpoint` is enough to enable OTLP logs.
// - We still want deterministic behavior when optional fields are omitted.
// - A stable `table_name` default makes the embedded panel (/logs) work without extra query params,
//   and avoids relying on GreptimeDB's internal default table naming.
pub const DEFAULT_OTLP_LOG_DB_NAME: &str = "public";
pub const DEFAULT_OTLP_LOG_TABLE_NAME: &str = "fluxon_logs";
pub const DEFAULT_OTLP_LOG_FLUSH_INTERVAL_MS: u64 = 2000;
pub const DEFAULT_OTLP_LOG_MAX_BATCH_LINES: usize = 2000;
pub const DEFAULT_OTLP_LOG_MAX_QUEUE_LINES: usize = 20000;

fn verify_otlp_log_api(cfg: &mut GreptimeOtlpLogConfigYaml) -> KvResult<GreptimeOtlpLogConfig> {
    let endpoint = cfg.otlp_endpoint.trim();
    if endpoint.is_empty() || !endpoint.contains("://") {
        return Err(ConfigError::InvalidGreptimeOtlpLogConfig {
            detail: format!(
                "invalid otlp_endpoint (expected http(s)://..): {}",
                cfg.otlp_endpoint
            ),
        }
        .into_kverror());
    }

    let db_name = match cfg
        .db_name
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        Some(v) => v.to_string(),
        None => DEFAULT_OTLP_LOG_DB_NAME.to_string(),
    };

    let table_name = match cfg
        .table_name
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        Some(v) => Some(v.to_string()),
        None => Some(DEFAULT_OTLP_LOG_TABLE_NAME.to_string()),
    };

    let flush_interval_ms = match cfg.flush_interval_ms {
        Some(v) if v > 0 => v,
        Some(_) => {
            return Err(ConfigError::InvalidGreptimeOtlpLogConfig {
                detail: "flush_interval_ms must be > 0 when provided".to_string(),
            }
            .into_kverror());
        }
        None => DEFAULT_OTLP_LOG_FLUSH_INTERVAL_MS,
    };

    let max_batch_lines = match cfg.max_batch_lines {
        Some(v) if v > 0 => v,
        Some(_) => {
            return Err(ConfigError::InvalidGreptimeOtlpLogConfig {
                detail: "max_batch_lines must be > 0 when provided".to_string(),
            }
            .into_kverror());
        }
        None => DEFAULT_OTLP_LOG_MAX_BATCH_LINES,
    };

    let max_queue_lines = match cfg.max_queue_lines {
        Some(v) if v > 0 => v,
        Some(_) => {
            return Err(ConfigError::InvalidGreptimeOtlpLogConfig {
                detail: "max_queue_lines must be > 0 when provided".to_string(),
            }
            .into_kverror());
        }
        None => DEFAULT_OTLP_LOG_MAX_QUEUE_LINES,
    };

    Ok(GreptimeOtlpLogConfig {
        otlp_endpoint: endpoint.to_string(),
        db_name,
        table_name,
        flush_interval_ms,
        max_batch_lines,
        max_queue_lines,
    })
}

/// Master节点YAML配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MasterConfigYaml {
    pub instance_key: String,
    pub cluster_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    pub etcd_endpoints: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<ProtocolConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub monitoring: Option<MonitoringConfigYaml>, // monitoring config (prometheus base url, optional remote write, optional otlp_log_api)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network: Option<NetworkConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pprof_duration_seconds: Option<u64>,
    pub log_dir: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub master_ui: Option<MasterUiConfigYaml>,
    #[serde(default)]
    pub test_spec_config: TestSpecConfig,
}

/// Master节点配置
#[derive(Debug)]
pub struct MasterConfig {
    pub instance_key: String,
    pub cluster_name: String,
    pub port: Option<u16>,
    pub etcd_endpoints: Vec<String>,
    pub protocol: ProtocolConfig,
    pub transfer_engine: TransferEngineType,
    pub enable_transfer_rpc_fast_path: bool,
    pub monitoring: Option<MonitoringConfig>, // monitoring config (prometheus base url, optional remote write, optional otlp_log_api)
    pub network: Option<NetworkConfig>,
    pub pprof_duration_seconds: Option<u64>,
    pub log_dir: String,
    pub master_ui: Option<MasterUiConfig>,
    pub test_spec_config: TestSpecConfig,
}

/// Configuration for cluster pool size contribution
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContributeToClusterPoolSizeYaml {
    pub dram: u64,                  // bytes
    pub vram: HashMap<String, u64>, // gpu_id -> bytes
}

/// Configuration for Fluxon KV backend specifications
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FluxonKvSpecYaml {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub etcd_addresses: Option<YamlNullable<Vec<String>>>,
    pub cluster_name: String,
    pub shared_memory_path: String,
    pub shared_file_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p2p_listen_port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redis_compat: Option<YamlNullable<RedisCompatConfigYaml>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_cluster: Option<YamlNullable<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisCompatConfigYaml {
    pub listen_addr: String,
}

/// Raw YAML configuration structure that matches the new design
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientConfigYaml {
    pub instance_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<ProtocolConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contribute_to_cluster_pool_size: Option<ContributeToClusterPoolSizeYaml>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pprof_duration_seconds: Option<u64>,
    pub fluxonkv_spec: FluxonKvSpecYaml,
    #[serde(default)]
    pub test_spec_config: TestSpecConfig,
}

/// Validated protocol configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProtocolConfig {
    pub protocol_type: ProtocolType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rdma_device_names: Option<String>,
}

/// Validated cluster pool size contribution
#[derive(Debug, Clone)]
pub struct ContributeToClusterPoolSize {
    pub dram: u64,                  // bytes
    pub vram: HashMap<String, u64>, // gpu_id -> bytes
}

/// Validated Fluxon KV specifications
#[derive(Debug, Clone)]
pub struct FluxonKvSpec {
    pub etcd_addresses: Vec<String>,
    pub cluster_name: String,
    pub p2p_listen_port: Option<u16>,
    pub transfer_engine: TransferEngineType,
    pub enable_transfer_rpc_fast_path: bool,
    pub sub_cluster: Option<String>,
}

/// KV client backend types supported by the system
#[derive(Debug, Clone, PartialEq)]
pub enum KvClientType {
    FluxonKv,
}

/// Validated and processed client configuration
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub cluster_name: String,
    pub etcd_addresses_raw: Vec<String>,
    pub instance_key: String,
    pub contribute_to_cluster_pool_size: ContributeToClusterPoolSize,
    pub protocol: ProtocolConfig,
    pub pprof_duration_seconds: Option<u64>,
    pub redis_compat_listen_addr: Option<std::net::SocketAddr>,
    pub fluxonkv_spec: FluxonKvSpec,
    pub shared_memory_path: String, // Mandatory shared memory path
    pub shared_file_path: String,   // Mandatory shared file path
    pub test_spec_config: TestSpecConfig,
}

const CAPACITY_ALIGNMENT_BYTES: u64 = 16 * 1024 * 1024;

fn _validate_host_port_no_scheme(value: &str, field: &str) -> KvResult<()> {
    let s = value.trim();
    if s.is_empty() {
        return Err(ConfigError::InvalidClientConfig {
            detail: format!("{} must be a non-empty string", field),
        }
        .into_kverror());
    }

    // Config contract (aligned with Python): etcd endpoints are raw host:port strings without scheme.
    if s.contains("://") {
        return Err(ConfigError::InvalidClientConfig {
            detail: format!("{} must be raw host:port (no scheme), got: {}", field, s),
        }
        .into_kverror());
    }

    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return Err(ConfigError::InvalidClientConfig {
            detail: format!("{} must match '{{str}}:{{int}}', got: {}", field, s),
        }
        .into_kverror());
    }
    let host = parts[0];
    let port_s = parts[1];
    if host.is_empty() || port_s.is_empty() {
        return Err(ConfigError::InvalidClientConfig {
            detail: format!("{} must match '{{str}}:{{int}}', got: {}", field, s),
        }
        .into_kverror());
    }
    let port_u32: u32 = port_s.parse().map_err(|_| {
        ConfigError::InvalidClientConfig {
            detail: format!("{} port must be an integer, got: {}", field, s),
        }
        .into_kverror()
    })?;
    if port_u32 == 0 || port_u32 > (u16::MAX as u32) {
        return Err(ConfigError::InvalidClientConfig {
            detail: format!("{} port out of range (1..=65535), got: {}", field, s),
        }
        .into_kverror());
    }
    Ok(())
}

fn _validate_capacity_multiple_of_alignment(value: u64, field: &str) -> KvResult<()> {
    if value % CAPACITY_ALIGNMENT_BYTES != 0 {
        return Err(ConfigError::InvalidClientConfig {
            detail: format!(
                "{} must be multiple of {} bytes, got: {}",
                field, CAPACITY_ALIGNMENT_BYTES, value
            ),
        }
        .into_kverror());
    }
    Ok(())
}

pub fn normalize_etcd_addresses(addresses: &[String]) -> KvResult<Vec<String>> {
    // Etcd client requires URL endpoints with scheme; config uses raw host:port strings.
    // This conversion is deterministic and part of the config contract.
    let mut result = Vec::new();
    for address in addresses {
        _validate_host_port_no_scheme(address, "etcd address")?;
        result.push(format!("http://{}", address.trim()));
    }
    Ok(result)
}

pub fn denormalize_etcd_endpoints(endpoints: &[String]) -> KvResult<Vec<String>> {
    // Convert `http(s)://host:port` endpoints back to raw `host:port` strings.
    //
    // Causal chain:
    // - Python KvClient exposes `get_etcd_config()` as raw host:port (no scheme).
    // - Rust business modules (fs/ops/mq) also need raw host:port for a consistent public API.
    // - Internally, `etcd-client` requires scheme-prefixed endpoints; we keep the conversion
    //   deterministic and reject any non-canonical forms (no fallback).
    let mut result = Vec::new();
    for endpoint in endpoints {
        let s = endpoint.trim();
        if s.is_empty() {
            return Err(ConfigError::InvalidClientConfig {
                detail: "etcd endpoint must be non-empty".to_string(),
            }
            .into_kverror());
        }
        let raw = if let Some(rest) = s.strip_prefix("http://") {
            rest
        } else if let Some(rest) = s.strip_prefix("https://") {
            rest
        } else {
            return Err(ConfigError::InvalidClientConfig {
                detail: format!(
                    "etcd endpoint must start with http:// or https://, got: {}",
                    s
                ),
            }
            .into_kverror());
        };
        if raw.contains('/') || raw.contains('?') || raw.contains('#') {
            return Err(ConfigError::InvalidClientConfig {
                detail: format!(
                    "etcd endpoint must be scheme + host:port without path/query/fragment, got: {}",
                    s
                ),
            }
            .into_kverror());
        }
        _validate_host_port_no_scheme(raw, "etcd endpoint")?;
        result.push(raw.to_string());
    }
    Ok(result)
}

impl ClientConfigYaml {
    /// Load configuration from a YAML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> KvResult<Self> {
        let content = fs::read_to_string(path).map_err(|e| {
            ConfigError::FileReadError {
                detail: e.to_string(),
            }
            .into_kverror()
        })?;
        let config: ClientConfigYaml = serde_yaml::from_str(&content).map_err(|e| {
            ConfigError::YamlParseError {
                // English note: serde_yaml errors usually do not include the original document; include it for debugging.
                detail: format!("{}\n--- YAML BEGIN ---\n{}\n--- YAML END ---", e, content),
            }
            .into_kverror()
        })?;
        Ok(config)
    }

    /// Load configuration from a YAML string
    pub fn from_str(yaml_str: &str) -> KvResult<Self> {
        let config: ClientConfigYaml = serde_yaml::from_str(yaml_str).map_err(|e| {
            ConfigError::YamlParseError {
                // English note: serde_yaml errors usually do not include the original document; include it for debugging.
                detail: format!("{}\n--- YAML BEGIN ---\n{}\n--- YAML END ---", e, yaml_str),
            }
            .into_kverror()
        })?;
        Ok(config)
    }

    /// Verify and validate the configuration, returning a processed Config
    pub fn verify(mut self) -> KvResult<ClientConfig> {
        // Validate instance_key
        if self.instance_key.trim().is_empty() {
            return Err(ConfigError::InvalidInstanceKey {
                key: self.instance_key,
            }
            .into_kverror());
        }

        let pprof_duration_seconds = match self.pprof_duration_seconds {
            Some(0) => {
                return Err(ConfigError::InvalidPprofDurationSeconds { seconds: 0 }.into_kverror());
            }
            Some(v) => Some(v),
            None => None,
        };

        let mut test_spec_config = self.test_spec_config;
        let transport_mode_was_explicit = test_spec_config.transport_mode.is_some();
        let normalized_rdma_device_names = normalize_test_spec_rdma_device_names(
            &mut test_spec_config,
            transport_mode_was_explicit,
        )?;
        materialize_default_test_spec_transport_mode(&mut test_spec_config);
        validate_required_transfer_rpc_fast_path_ready_timeout(&test_spec_config)?;
        validate_test_spec_tcp_thread_tuning(&test_spec_config)?;

        // Role selection contract:
        // - Missing contribute_to_cluster_pool_size means "zero-contribution" mode.
        // - Explicit contribute_to_cluster_pool_size with all zeros also means "zero-contribution" mode.
        // - Any partial-zero contribution is rejected to avoid ambiguous behavior.
        let (is_external, contribute_to_cluster_pool_size) = match &self
            .contribute_to_cluster_pool_size
        {
            None => (
                true,
                ContributeToClusterPoolSize {
                    dram: 0,
                    vram: HashMap::new(),
                },
            ),
            Some(c) => {
                _validate_capacity_multiple_of_alignment(
                    c.dram,
                    "contribute_to_cluster_pool_size.dram",
                )?;
                for (gpu_id, size) in c.vram.iter() {
                    _validate_capacity_multiple_of_alignment(
                        *size,
                        &format!("contribute_to_cluster_pool_size.vram.{gpu_id}"),
                    )?;
                }
                let vram_is_zero = c.vram.values().all(|&v| v == 0);
                if c.dram == 0 && !vram_is_zero {
                    return Err(ConfigError::InvalidClientConfig {
                        detail: "contribute_to_cluster_pool_size is partially zero: dram=0 but vram has non-zero values".to_string(),
                    }
                    .into_kverror());
                }
                let is_zero = c.dram == 0 && vram_is_zero;

                (
                    is_zero,
                    ContributeToClusterPoolSize {
                        dram: c.dram,
                        vram: c.vram.clone(),
                    },
                )
            }
        };

        if !is_external {
            let Some(contrib_yaml) = &self.contribute_to_cluster_pool_size else {
                return Err(ConfigError::InvalidClientConfig {
                    detail: "contribute_to_cluster_pool_size is required for owner mode (non-zero contribution)".to_string(),
                }
                .into_kverror());
            };
            if contrib_yaml.dram == 0 {
                return Err(ConfigError::InvalidClientConfig {
                    detail: "owner mode requires non-zero contribute_to_cluster_pool_size.dram"
                        .to_string(),
                }
                .into_kverror());
            }
        }

        let is_side_transfer_worker = matches!(
            test_spec_config.side_transfer_role,
            Some(SideTransferRole::Worker)
        );

        if is_side_transfer_worker && !is_external {
            return Err(ConfigError::InvalidClientConfig {
                detail:
                    "test_spec_config.side_transfer_role=worker requires zero-contribution mode"
                        .to_string(),
            }
            .into_kverror());
        }

        if is_external
            && !is_side_transfer_worker
            && test_spec_config.side_transfer_worker_count > 0
        {
            return Err(ConfigError::InvalidClientConfig {
                detail:
                    "test_spec_config.side_transfer_worker_count is only valid on owner configs"
                        .to_string(),
            }
            .into_kverror());
        }

        // External (zero-contribution) mode forbids additional knobs to keep the schema minimal.
        if is_external {
            if self.fluxonkv_spec.redis_compat.is_some() {
                return Err(ConfigError::InvalidClientConfig {
                    detail: "fluxonkv_spec.redis_compat is forbidden in zero-contribution mode"
                        .to_string(),
                }
                .into_kverror());
            }
            if self.fluxonkv_spec.sub_cluster.is_some() {
                return Err(ConfigError::InvalidClientConfig {
                    detail: "fluxonkv_spec.sub_cluster is forbidden in zero-contribution mode (it is inherited from owner shared.json)".to_string(),
                }
                .into_kverror());
            }
            if self.fluxonkv_spec.etcd_addresses.is_some() {
                return Err(ConfigError::InvalidClientConfig {
                    detail: "fluxonkv_spec.etcd_addresses is forbidden in zero-contribution mode (it is bootstrapped from owner shared.json)".to_string(),
                }
                .into_kverror());
            }
        }

        // Preserve historical behavior for configs that omit `protocol`, but allow
        // generated zero-contribution side-worker configs to explicitly inherit TCP.
        let protocol = apply_test_spec_rdma_device_names_to_protocol(
            self.protocol.unwrap_or(ProtocolConfig {
                protocol_type: ProtocolType::Rdma,
                rdma_device_names: None,
            }),
            normalized_rdma_device_names.as_ref(),
        );

        // Preserve raw etcd_addresses for shared.json (external bootstrap expects raw strings).
        let (etcd_addresses_raw, etcd_endpoints) = if is_external {
            (Vec::new(), Vec::new())
        } else {
            let Some(etcd_raw) = std::mem::take(&mut self.fluxonkv_spec.etcd_addresses) else {
                return Err(ConfigError::EmptyEtcdAddresses {}.into_kverror());
            };
            let etcd_raw = match etcd_raw {
                YamlNullable::Null => {
                    return Err(ConfigError::EmptyEtcdAddresses {}.into_kverror());
                }
                YamlNullable::Value(v) => v,
            };
            if etcd_raw.is_empty() {
                return Err(ConfigError::EmptyEtcdAddresses {}.into_kverror());
            }
            for address in &etcd_raw {
                _validate_host_port_no_scheme(address, "fluxonkv_spec.etcd_addresses[]")?;
            }
            let normalized = normalize_etcd_addresses(&etcd_raw)?;
            (etcd_raw, normalized)
        };

        // for address in &mut self.unifykv_spec.etcd_addresses {
        //     if address.trim().is_empty() {
        //         return Err(ConfigError::InvalidEtcdAddress(address.clone()).into());
        //     }
        //     if !address.contains("://") {
        //         warn!(
        //             "etcd address {} missing protocol prefix, automatically adding http:// prefix",
        //             address
        //         );
        //         *address = format!("http://{}", address);
        //     }
        // }

        // Validate cluster_name
        if self.fluxonkv_spec.cluster_name.trim().is_empty() {
            return Err(ConfigError::InvalidClusterName {
                name: self.fluxonkv_spec.cluster_name,
            }
            .into_kverror());
        }

        if let Some(raw) = self.fluxonkv_spec.sub_cluster.as_ref() {
            if let YamlNullable::Value(s) = raw {
                if s.trim().is_empty() {
                    return Err(ConfigError::InvalidClientConfig {
                        detail:
                            "fluxonkv_spec.sub_cluster must be a non-empty string when provided"
                                .to_string(),
                    }
                    .into_kverror());
                }
                if s != s.trim() {
                    return Err(ConfigError::InvalidClientConfig {
                        detail:
                            "fluxonkv_spec.sub_cluster must not have leading/trailing whitespace"
                                .to_string(),
                    }
                    .into_kverror());
                }
            }
        }

        let transfer_engine = if is_side_transfer_worker {
            TransferEngineType::P2p
        } else {
            resolve_transfer_engine_for_protocol_and_test_spec(&protocol, Some(&test_spec_config))?
        };
        let enable_transfer_rpc_fast_path = if is_side_transfer_worker {
            false
        } else {
            resolve_enable_transfer_rpc_fast_path(
                transfer_engine_supports_rpc_fast_path(transfer_engine),
                Some(&test_spec_config),
            )
        };

        let sub_cluster = if is_external {
            None
        } else {
            match std::mem::take(&mut self.fluxonkv_spec.sub_cluster) {
                None | Some(YamlNullable::Null) => {
                    return Err(ConfigError::InvalidClientConfig {
                        detail: "fluxonkv_spec.sub_cluster is required for owner mode".to_string(),
                    }
                    .into_kverror());
                }
                Some(YamlNullable::Value(s)) => Some(s),
            }
        };

        let fluxonkv_spec = FluxonKvSpec {
            etcd_addresses: etcd_endpoints,
            cluster_name: self.fluxonkv_spec.cluster_name,
            p2p_listen_port: self.fluxonkv_spec.p2p_listen_port,
            transfer_engine,
            enable_transfer_rpc_fast_path,
            sub_cluster,
        };

        if let Some(p) = self.fluxonkv_spec.p2p_listen_port {
            if p == 0 {
                return Err(ConfigError::InvalidPort { port: p }.into_kverror());
            }
        }

        if let Some(p) = test_spec_config.side_transfer_worker_p2p_port_base {
            if p == 0 {
                return Err(ConfigError::InvalidPort { port: p }.into_kverror());
            }
        }

        // Validate shared_memory_path (mandatory and non-empty)
        if self.fluxonkv_spec.shared_memory_path.trim().is_empty() {
            return Err(ConfigError::InvalidInstanceKey {
                key: "shared_memory_path cannot be empty".to_string(),
            }
            .into_kverror());
        }
        if self.fluxonkv_spec.shared_file_path.trim().is_empty() {
            return Err(ConfigError::InvalidInstanceKey {
                key: "shared_file_path cannot be empty".to_string(),
            }
            .into_kverror());
        }

        let shared_memory_path = cluster_scoped_shared_path(
            &self.fluxonkv_spec.shared_memory_path,
            &fluxonkv_spec.cluster_name,
        )?;
        let shared_file_path = cluster_scoped_shared_path(
            &self.fluxonkv_spec.shared_file_path,
            &fluxonkv_spec.cluster_name,
        )?;

        let redis_compat_listen_addr = match self.fluxonkv_spec.redis_compat.as_ref() {
            None | Some(YamlNullable::Null) => None,
            Some(YamlNullable::Value(rc)) => {
                let s = rc.listen_addr.trim();
                if s.is_empty() {
                    return Err(ConfigError::InvalidRedisCompatListenAddr {
                        addr: rc.listen_addr.clone(),
                    }
                    .into_kverror());
                }
                let addr = std::net::SocketAddr::from_str(s).map_err(|_| {
                    ConfigError::InvalidRedisCompatListenAddr {
                        addr: rc.listen_addr.clone(),
                    }
                    .into_kverror()
                })?;
                Some(addr)
            }
        };

        Ok(ClientConfig {
            cluster_name: fluxonkv_spec.cluster_name.clone(),
            etcd_addresses_raw,
            instance_key: self.instance_key,
            contribute_to_cluster_pool_size,
            protocol,
            pprof_duration_seconds,
            redis_compat_listen_addr,
            fluxonkv_spec,
            shared_memory_path,
            shared_file_path,
            test_spec_config,
        })
    }
}

impl MasterConfigYaml {
    /// Load configuration from a YAML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> KvResult<Self> {
        let content = fs::read_to_string(path).map_err(|e| {
            ConfigError::FileReadError {
                detail: e.to_string(),
            }
            .into_kverror()
        })?;
        let config: MasterConfigYaml = serde_yaml::from_str(&content).map_err(|e| {
            ConfigError::YamlParseError {
                // English note: serde_yaml errors usually do not include the original document; include it for debugging.
                detail: format!("{}\n--- YAML BEGIN ---\n{}\n--- YAML END ---", e, content),
            }
            .into_kverror()
        })?;
        Ok(config)
    }

    /// Load configuration from a YAML string
    pub fn from_str(yaml_str: &str) -> KvResult<Self> {
        let config: MasterConfigYaml = serde_yaml::from_str(yaml_str).map_err(|e| {
            ConfigError::YamlParseError {
                // English note: serde_yaml errors usually do not include the original document; include it for debugging.
                detail: format!("{}\n--- YAML BEGIN ---\n{}\n--- YAML END ---", e, yaml_str),
            }
            .into_kverror()
        })?;
        Ok(config)
    }

    /// Verify and validate the configuration, returning a processed MasterConfig
    pub fn verify(mut self) -> KvResult<MasterConfig> {
        // Validate instance_name
        if self.instance_key.trim().is_empty() {
            return Err(ConfigError::InvalidInstanceKey {
                key: self.instance_key,
            }
            .into_kverror());
        }

        // Validate cluster_name
        if self.cluster_name.trim().is_empty() {
            return Err(ConfigError::InvalidClusterName {
                name: self.cluster_name,
            }
            .into_kverror());
        }

        if let Some(p) = self.port {
            if p == 0 {
                return Err(ConfigError::InvalidPort { port: p }.into_kverror());
            }
        }

        // Validate etcd_endpoints
        if self.etcd_endpoints.is_empty() {
            return Err(ConfigError::EmptyEtcdEndpoints {}.into_kverror());
        }
        for endpoint in &self.etcd_endpoints {
            _validate_host_port_no_scheme(endpoint, "master.etcd_endpoints[]")?;
        }
        self.etcd_endpoints = normalize_etcd_addresses(&self.etcd_endpoints)?;

        let monitoring = match self.monitoring.as_mut() {
            Some(monitoring) => {
                let prom_base = monitoring.prometheus_base_url.trim();
                if prom_base.is_empty() || !prom_base.contains("://") {
                    return Err(ConfigError::InvalidPrometheusBaseUrl {
                        detail: monitoring.prometheus_base_url.clone(),
                    }
                    .into_kverror());
                }

                let prom_remote_write_url = match monitoring.prom_remote_write_url.as_mut() {
                    Some(urls) => {
                        if urls.is_empty() {
                            return Err(ConfigError::InvalidPromRemoteWriteUrl {
                                detail: "empty list".to_string(),
                            }
                            .into_kverror());
                        }
                        let mut out: Vec<String> = Vec::with_capacity(urls.len());
                        for url in urls.iter_mut() {
                            let trimmed = url.trim();
                            if trimmed.is_empty() || !trimmed.contains("://") {
                                return Err(ConfigError::InvalidPromRemoteWriteUrl {
                                    detail: trimmed.to_string(),
                                }
                                .into_kverror());
                            }
                            out.push(trimmed.to_string());
                        }
                        Some(out)
                    }
                    None => None,
                };

                let otlp_log_api = match monitoring.otlp_log_api.as_mut() {
                    Some(cfg) => Some(verify_otlp_log_api(cfg)?),
                    None => None,
                };

                MonitoringConfig {
                    prometheus_base_url: prom_base.to_string(),
                    prom_remote_write_url,
                    otlp_log_api,
                }
            }
            None => return Err(ConfigError::MissingMonitoringConfig {}.into_kverror()),
        };

        let network = match self.network.as_mut() {
            Some(cfg) => {
                for cidr in cfg.subnet_whitelist.iter_mut() {
                    let trimmed = cidr.trim();
                    if trimmed.is_empty() {
                        return Err(ConfigError::InvalidSubnetWhitelistCidr {
                            cidr: cidr.clone(),
                            detail: "empty cidr".to_string(),
                        }
                        .into_kverror());
                    }
                    if let Err(detail) = validate_ip_cidr(trimmed) {
                        return Err(ConfigError::InvalidSubnetWhitelistCidr {
                            cidr: trimmed.to_string(),
                            detail,
                        }
                        .into_kverror());
                    }
                    if trimmed != cidr {
                        *cidr = trimmed.to_string();
                    }
                }

                // Keep this mapping strict: keys are locally-discovered IP strings; values are extra reachable IPs.
                if let Some(mapping) = cfg.primary_ip_to_extended_ips.as_mut() {
                    let mut normalized: BTreeMap<String, Vec<String>> = BTreeMap::new();
                    for (primary_ip_raw, extended_ips_raw) in mapping.iter() {
                        let primary_ip_trimmed = primary_ip_raw.trim();
                        if primary_ip_trimmed.is_empty() {
                            return Err(ConfigError::InvalidPrimaryIpToExtendedIpsPrimaryIp {
                                ip: primary_ip_raw.clone(),
                                detail: "empty primary ip".to_string(),
                            }
                            .into_kverror());
                        }
                        if primary_ip_trimmed != primary_ip_raw.as_str() {
                            return Err(ConfigError::InvalidPrimaryIpToExtendedIpsPrimaryIp {
                                ip: primary_ip_raw.clone(),
                                detail: "primary ip has leading/trailing whitespace".to_string(),
                            }
                            .into_kverror());
                        }
                        let primary_ip = match IpAddr::from_str(primary_ip_trimmed) {
                            Ok(v) => v,
                            Err(e) => {
                                return Err(ConfigError::InvalidPrimaryIpToExtendedIpsPrimaryIp {
                                    ip: primary_ip_trimmed.to_string(),
                                    detail: e.to_string(),
                                }
                                .into_kverror());
                            }
                        };
                        let primary_ip_norm = primary_ip.to_string();
                        if normalized.contains_key(&primary_ip_norm) {
                            return Err(ConfigError::InvalidPrimaryIpToExtendedIpsPrimaryIp {
                                ip: primary_ip_norm,
                                detail: "duplicate primary ip after normalization".to_string(),
                            }
                            .into_kverror());
                        }

                        let mut extended_norm: Vec<String> =
                            Vec::with_capacity(extended_ips_raw.len());
                        for ip_raw in extended_ips_raw.iter() {
                            let ip_trimmed = ip_raw.trim();
                            if ip_trimmed.is_empty() {
                                return Err(ConfigError::InvalidPrimaryIpToExtendedIpsExtendedIp {
                                    primary_ip: primary_ip_norm.clone(),
                                    ip: ip_raw.clone(),
                                    detail: "empty extended ip".to_string(),
                                }
                                .into_kverror());
                            }
                            if ip_trimmed != ip_raw.as_str() {
                                return Err(ConfigError::InvalidPrimaryIpToExtendedIpsExtendedIp {
                                    primary_ip: primary_ip_norm.clone(),
                                    ip: ip_raw.clone(),
                                    detail: "extended ip has leading/trailing whitespace"
                                        .to_string(),
                                }
                                .into_kverror());
                            }
                            let ip = match IpAddr::from_str(ip_trimmed) {
                                Ok(v) => v,
                                Err(e) => {
                                    return Err(
                                        ConfigError::InvalidPrimaryIpToExtendedIpsExtendedIp {
                                            primary_ip: primary_ip_norm.clone(),
                                            ip: ip_trimmed.to_string(),
                                            detail: e.to_string(),
                                        }
                                        .into_kverror(),
                                    );
                                }
                            };
                            let ip_norm = ip.to_string();
                            if !extended_norm.contains(&ip_norm) {
                                extended_norm.push(ip_norm);
                            }
                        }
                        if extended_norm.is_empty() {
                            return Err(ConfigError::InvalidPrimaryIpToExtendedIpsExtendedIp {
                                primary_ip: primary_ip_norm.clone(),
                                ip: "".to_string(),
                                detail: "empty extended ip list".to_string(),
                            }
                            .into_kverror());
                        }

                        normalized.insert(primary_ip_norm, extended_norm);
                    }
                    *mapping = normalized;
                }

                Some(cfg.clone())
            }
            None => None,
        };
        // Validate log directory
        if self.log_dir.trim().is_empty() {
            return Err(ConfigError::InvalidLogDir {
                dir: self.log_dir.clone(),
            }
            .into_kverror());
        }

        let pprof_duration_seconds = match self.pprof_duration_seconds {
            Some(0) => {
                return Err(ConfigError::InvalidPprofDurationSeconds { seconds: 0 }.into_kverror());
            }
            Some(v) => Some(v),
            None => None,
        };

        let master_ui = match self.master_ui.as_mut() {
            Some(cfg) => {
                let listen_addr = cfg.http_listen_addr.trim();
                if listen_addr.is_empty() {
                    return Err(ConfigError::InvalidClientConfig {
                        detail: "master_ui.http_listen_addr must be a non-empty string".to_string(),
                    }
                    .into_kverror());
                }
                if listen_addr != cfg.http_listen_addr {
                    return Err(ConfigError::InvalidClientConfig {
                        detail:
                            "master_ui.http_listen_addr must not have leading/trailing whitespace"
                                .to_string(),
                    }
                    .into_kverror());
                }
                Some(MasterUiConfig {
                    http_listen_addr: listen_addr.to_string(),
                })
            }
            None => None,
        };

        let mut test_spec_config = self.test_spec_config;
        let transport_mode_was_explicit = test_spec_config.transport_mode.is_some();
        let normalized_rdma_device_names = normalize_test_spec_rdma_device_names(
            &mut test_spec_config,
            transport_mode_was_explicit,
        )?;
        materialize_default_test_spec_transport_mode(&mut test_spec_config);
        validate_required_transfer_rpc_fast_path_ready_timeout(&test_spec_config)?;
        validate_test_spec_tcp_thread_tuning(&test_spec_config)?;
        let protocol = apply_test_spec_rdma_device_names_to_protocol(
            self.protocol.unwrap_or(ProtocolConfig {
                protocol_type: ProtocolType::Rdma,
                rdma_device_names: None,
            }),
            normalized_rdma_device_names.as_ref(),
        );
        let transfer_engine =
            resolve_transfer_engine_for_protocol_and_test_spec(&protocol, Some(&test_spec_config))?;

        Ok(MasterConfig {
            instance_key: self.instance_key,
            cluster_name: self.cluster_name,
            port: self.port,
            etcd_endpoints: self.etcd_endpoints,
            protocol,
            transfer_engine,
            enable_transfer_rpc_fast_path: resolve_enable_transfer_rpc_fast_path(
                transfer_engine_supports_rpc_fast_path(transfer_engine),
                Some(&test_spec_config),
            ),
            pprof_duration_seconds,
            monitoring: Some(monitoring),
            network,
            log_dir: self.log_dir,
            master_ui,
            test_spec_config,
        })
    }
}

// ExternalClientConfig and ExternalClientConfigYaml are removed.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_scoped_shared_path_appends_cluster_name() {
        let scoped = cluster_scoped_shared_path("/tmp/fluxon_root", "test_cluster").unwrap();
        assert_eq!(scoped, "/tmp/fluxon_root/test_cluster");
    }

    #[test]
    fn client_test_spec_config_transfer_only_disables_rpc_fast_path() {
        let cfg = ClientConfigYaml::from_str(
            r#"
instance_key: test_owner
contribute_to_cluster_pool_size:
  dram: 16777216
  vram: {}
fluxonkv_spec:
  etcd_addresses: ["127.0.0.1:2379"]
  cluster_name: test_cluster
  shared_memory_path: /tmp/test_owner
  shared_file_path: /tmp/test_owner_files
  sub_cluster: rack-a
test_spec_config:
  disable_observability: true
  enable_iceoryx_logs: true
  iceoryx_external_busy_poll: true
  iceoryx_owner_client_busy_poll: false
  transport_mode: transfer_only
  rdma_device_names: ["mlx5_0"]
"#,
        )
        .unwrap();
        let verified = cfg.verify().unwrap();
        assert_eq!(verified.protocol.protocol_type, ProtocolType::Rdma);
        assert!(verified.test_spec_config.disable_observability);
        assert!(verified.test_spec_config.enable_iceoryx_logs);
        assert!(verified.test_spec_config.iceoryx_external_busy_poll);
        assert!(!verified.test_spec_config.iceoryx_owner_client_busy_poll);
        assert_eq!(verified.shared_memory_path, "/tmp/test_owner/test_cluster");
        assert_eq!(
            verified.shared_file_path,
            "/tmp/test_owner_files/test_cluster"
        );
        assert_eq!(
            verified.test_spec_config.transport_mode,
            Some(TestSpecTransportMode::TransferOnly)
        );
        assert_eq!(
            verified.protocol.rdma_device_names,
            Some("mlx5_0".to_string())
        );
        assert!(!verified.fluxonkv_spec.enable_transfer_rpc_fast_path);
    }

    #[test]
    fn client_test_spec_config_defaults_transport_mode_to_transfer_with_rpc() {
        let cfg = ClientConfigYaml::from_str(
            r#"
instance_key: test_owner
contribute_to_cluster_pool_size:
  dram: 16777216
  vram: {}
fluxonkv_spec:
  etcd_addresses: ["127.0.0.1:2379"]
  cluster_name: test_cluster
  shared_memory_path: /tmp/test_owner
  shared_file_path: /tmp/test_owner_files
  sub_cluster: rack-a
"#,
        )
        .unwrap();
        let verified = cfg.verify().unwrap();
        assert_eq!(
            verified.test_spec_config.transport_mode,
            Some(TestSpecTransportMode::TransferWithRpc)
        );
        assert!(verified.fluxonkv_spec.enable_transfer_rpc_fast_path);
    }

    #[test]
    fn client_test_spec_config_accepts_explicit_rdma_device_names() {
        let cfg = ClientConfigYaml::from_str(
            r#"
instance_key: test_owner
contribute_to_cluster_pool_size:
  dram: 16777216
  vram: {}
fluxonkv_spec:
  etcd_addresses: ["127.0.0.1:2379"]
  cluster_name: test_cluster
  shared_memory_path: /tmp/test_owner
  shared_file_path: /tmp/test_owner_files
  sub_cluster: rack-a
test_spec_config:
  transport_mode: transfer_with_rpc
  disable_crossowner_ipc: true
  iceoryx_external_busy_poll: true
  iceoryx_owner_client_busy_poll: false
  tcp_thread_reactor_shard_count: 2
  tcp_thread_bulk_lane_count: 4
  tcp_thread_control_lane_count: 3
  rdma_device_names: [" mlx5_4 ", "mlx5_0", "mlx5_4"]
"#,
        )
        .unwrap();
        let verified = cfg.verify().unwrap();
        assert_eq!(
            verified.test_spec_config.rdma_device_names,
            Some(vec!["mlx5_0".to_string(), "mlx5_4".to_string()])
        );
        assert_eq!(
            verified.protocol.rdma_device_names,
            Some("mlx5_0,mlx5_4".to_string())
        );
        assert!(verified.test_spec_config.disable_crossowner_ipc);
        assert!(verified.test_spec_config.iceoryx_external_busy_poll);
        assert!(!verified.test_spec_config.iceoryx_owner_client_busy_poll);
        assert_eq!(
            verified.test_spec_config.tcp_thread_reactor_shard_count,
            Some(2)
        );
        assert_eq!(
            verified.test_spec_config.tcp_thread_bulk_lane_count,
            Some(4)
        );
        assert_eq!(
            verified.test_spec_config.tcp_thread_control_lane_count,
            Some(3)
        );
    }

    #[test]
    fn client_test_spec_config_implicit_transport_mode_with_rdma_device_names_defaults_to_transfer_with_rpc()
     {
        let cfg = ClientConfigYaml::from_str(
            r#"
instance_key: test_owner
contribute_to_cluster_pool_size:
  dram: 16777216
  vram: {}
fluxonkv_spec:
  etcd_addresses: ["127.0.0.1:2379"]
  cluster_name: test_cluster
  shared_memory_path: /tmp/test_owner
  shared_file_path: /tmp/test_owner_files
  sub_cluster: rack-a
test_spec_config:
  rdma_device_names: ["mlx5_0"]
"#,
        )
        .unwrap();
        let verified = cfg.verify().unwrap();
        assert_eq!(
            verified.test_spec_config.transport_mode,
            Some(TestSpecTransportMode::TransferWithRpc)
        );
        assert_eq!(
            verified.test_spec_config.rdma_device_names,
            Some(vec!["mlx5_0".to_string()])
        );
        assert_eq!(
            verified.protocol.rdma_device_names,
            Some("mlx5_0".to_string())
        );
        assert!(verified.fluxonkv_spec.enable_transfer_rpc_fast_path);
    }

    #[test]
    fn client_test_spec_config_accepts_transfer_rpc_fast_path_ready_timeout() {
        let cfg = ClientConfigYaml::from_str(
            r#"
instance_key: test_owner
contribute_to_cluster_pool_size:
  dram: 16777216
  vram: {}
fluxonkv_spec:
  etcd_addresses: ["127.0.0.1:2379"]
  cluster_name: test_cluster
  shared_memory_path: /tmp/test_owner
  shared_file_path: /tmp/test_owner_files
  sub_cluster: rack-a
test_spec_config:
  transport_mode: transfer_with_rpc
  rdma_device_names: ["mlx5_0"]
  require_transfer_rpc_fast_path_ready_timeout_seconds: 45
"#,
        )
        .unwrap();
        let verified = cfg.verify().unwrap();
        assert_eq!(
            verified
                .test_spec_config
                .require_transfer_rpc_fast_path_ready_timeout_seconds,
            Some(45)
        );
    }

    #[test]
    fn client_test_spec_config_rejects_transfer_rpc_fast_path_ready_timeout_without_explicit_rdma_device_names()
     {
        let cfg = ClientConfigYaml::from_str(
            r#"
instance_key: test_owner
contribute_to_cluster_pool_size:
  dram: 16777216
  vram: {}
fluxonkv_spec:
  etcd_addresses: ["127.0.0.1:2379"]
  cluster_name: test_cluster
  shared_memory_path: /tmp/test_owner
  shared_file_path: /tmp/test_owner_files
  sub_cluster: rack-a
test_spec_config:
  require_transfer_rpc_fast_path_ready_timeout_seconds: 45
"#,
        )
        .unwrap();
        let err = cfg.verify().unwrap_err();
        assert!(err.to_string().contains(
            "test_spec_config.require_transfer_rpc_fast_path_ready_timeout_seconds requires explicit test_spec_config.rdma_device_names"
        ));
    }

    #[test]
    fn client_test_spec_config_rejects_invalid_tcp_thread_control_lane_count() {
        let cfg = ClientConfigYaml::from_str(
            r#"
instance_key: test_owner
contribute_to_cluster_pool_size:
  dram: 16777216
  vram: {}
fluxonkv_spec:
  etcd_addresses: ["127.0.0.1:2379"]
  cluster_name: test_cluster
  shared_memory_path: /tmp/test_owner
  shared_file_path: /tmp/test_owner_files
  sub_cluster: rack-a
test_spec_config:
  tcp_thread_control_lane_count: 0
"#,
        )
        .unwrap();
        let err = cfg.verify().unwrap_err();
        assert!(
            err.to_string()
                .contains("test_spec_config.tcp_thread_control_lane_count must be in [1, 8]")
        );
    }

    #[test]
    fn client_test_spec_config_uses_single_closed_mode() {
        let cfg = ClientConfigYaml::from_str(
            r#"
instance_key: test_owner
contribute_to_cluster_pool_size:
  dram: 16777216
  vram: {}
fluxonkv_spec:
  etcd_addresses: ["127.0.0.1:2379"]
  cluster_name: test_cluster
  shared_memory_path: /tmp/test_owner
  shared_file_path: /tmp/test_owner_files
  sub_cluster: rack-a
test_spec_config:
  transport_mode: transfer_with_rpc
  rdma_device_names: ["mlx5_0"]
"#,
        )
        .unwrap();
        let verified = cfg.verify().unwrap();
        assert_eq!(
            verified.protocol.rdma_device_names,
            Some("mlx5_0".to_string())
        );
        assert_eq!(
            verified.fluxonkv_spec.transfer_engine,
            TransferEngineType::Closed
        );
    }

    #[test]
    fn client_test_spec_config_rejects_unknown_legacy_field() {
        let err = ClientConfigYaml::from_str(
            r#"
instance_key: test_owner
contribute_to_cluster_pool_size:
  dram: 16777216
  vram: {}
fluxonkv_spec:
  etcd_addresses: ["127.0.0.1:2379"]
  cluster_name: test_cluster
  shared_memory_path: /tmp/test_owner
  shared_file_path: /tmp/test_owner_files
  sub_cluster: rack-a
test_spec_config:
  transport_mode: transfer_with_rpc
  rdma_device_names: ["mlx5_0"]
  legacy_transfer_backend: closed
"#,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("unknown field `legacy_transfer_backend`"));
    }

    #[test]
    fn client_test_spec_config_rdma_device_names_use_default_transport_mode() {
        let cfg = ClientConfigYaml::from_str(
            r#"
instance_key: test_owner
contribute_to_cluster_pool_size:
  dram: 16777216
  vram: {}
fluxonkv_spec:
  etcd_addresses: ["127.0.0.1:2379"]
  cluster_name: test_cluster
  shared_memory_path: /tmp/test_owner
  shared_file_path: /tmp/test_owner_files
  sub_cluster: rack-a
test_spec_config:
  rdma_device_names: ["mlx5_0"]
"#,
        )
        .unwrap();
        let verified = cfg.verify().unwrap();
        assert_eq!(
            verified.test_spec_config.transport_mode,
            Some(TestSpecTransportMode::TransferWithRpc)
        );
        assert_eq!(
            verified.test_spec_config.rdma_device_names,
            Some(vec!["mlx5_0".to_string()])
        );
        assert_eq!(
            verified.protocol.rdma_device_names,
            Some("mlx5_0".to_string())
        );
    }

    #[test]
    fn client_test_spec_config_rejects_transport_mode_without_rdma_device_names() {
        let cfg = ClientConfigYaml::from_str(
            r#"
instance_key: test_owner
contribute_to_cluster_pool_size:
  dram: 16777216
  vram: {}
fluxonkv_spec:
  etcd_addresses: ["127.0.0.1:2379"]
  cluster_name: test_cluster
  shared_memory_path: /tmp/test_owner
  shared_file_path: /tmp/test_owner_files
  sub_cluster: rack-a
test_spec_config:
  transport_mode: transfer_only
"#,
        )
        .unwrap();
        assert!(cfg.verify().is_err());
    }

    #[test]
    fn client_config_accepts_explicit_tcp_protocol() {
        let cfg = ClientConfigYaml::from_str(
            r#"
instance_key: test_side_worker
protocol:
  protocol_type: tcp
fluxonkv_spec:
  cluster_name: test_cluster
  shared_memory_path: /tmp/test_side_worker
  shared_file_path: /tmp/test_side_worker_files
  p2p_listen_port: 18081
test_spec_config:
  enable_side_transfer: true
  side_transfer_role: worker
"#,
        )
        .unwrap();
        let verified = cfg.verify().unwrap();
        assert_eq!(verified.protocol.protocol_type, ProtocolType::Tcp);
        assert_eq!(
            verified.shared_memory_path,
            "/tmp/test_side_worker/test_cluster"
        );
        assert_eq!(
            verified.shared_file_path,
            "/tmp/test_side_worker_files/test_cluster"
        );
        assert_eq!(
            verified.fluxonkv_spec.transfer_engine,
            TransferEngineType::P2p
        );
        assert!(!verified.fluxonkv_spec.enable_transfer_rpc_fast_path);
        assert_eq!(
            verified.test_spec_config.side_transfer_role,
            Some(SideTransferRole::Worker)
        );
    }

    #[test]
    fn client_config_accepts_side_transfer_worker_without_explicit_p2p_port() {
        let cfg = ClientConfigYaml::from_str(
            r#"
instance_key: test_side_worker
protocol:
  protocol_type: tcp
fluxonkv_spec:
  cluster_name: test_cluster
  shared_memory_path: /tmp/test_side_worker
  shared_file_path: /tmp/test_side_worker_files
test_spec_config:
  enable_side_transfer: true
  side_transfer_role: worker
"#,
        )
        .unwrap();
        let verified = cfg.verify().unwrap();
        assert_eq!(verified.protocol.protocol_type, ProtocolType::Tcp);
        assert_eq!(verified.fluxonkv_spec.p2p_listen_port, None);
        assert_eq!(
            verified.fluxonkv_spec.transfer_engine,
            TransferEngineType::P2p
        );
        assert!(!verified.fluxonkv_spec.enable_transfer_rpc_fast_path);
        assert_eq!(
            verified.test_spec_config.side_transfer_role,
            Some(SideTransferRole::Worker)
        );
    }

    #[test]
    fn client_config_side_transfer_worker_forces_p2p_without_rpc_fast_path() {
        let cfg = ClientConfigYaml::from_str(
            r#"
instance_key: test_side_worker
protocol:
  protocol_type: tcp
fluxonkv_spec:
  cluster_name: test_cluster
  shared_memory_path: /tmp/test_side_worker
  shared_file_path: /tmp/test_side_worker_files
test_spec_config:
  enable_side_transfer: true
  side_transfer_role: worker
  transport_mode: transfer_with_rpc
  rdma_device_names: ["mlx5_0"]
"#,
        )
        .unwrap();
        let verified = cfg.verify().unwrap();
        assert_eq!(
            verified.fluxonkv_spec.transfer_engine,
            TransferEngineType::P2p
        );
        assert!(!verified.fluxonkv_spec.enable_transfer_rpc_fast_path);
    }

    #[test]
    fn client_config_accepts_owner_side_transfer_workers_without_port_base() {
        let cfg = ClientConfigYaml::from_str(
            r#"
instance_key: test_owner
contribute_to_cluster_pool_size:
  dram: 16777216
  vram: {}
fluxonkv_spec:
  etcd_addresses: ["127.0.0.1:2379"]
  cluster_name: test_cluster
  shared_memory_path: /tmp/test_owner
  shared_file_path: /tmp/test_owner_files
  p2p_listen_port: 18081
  sub_cluster: rack-a
test_spec_config:
  enable_side_transfer: true
  side_transfer_worker_count: 4
"#,
        )
        .unwrap();
        let verified = cfg.verify().unwrap();
        assert_eq!(verified.protocol.protocol_type, ProtocolType::Rdma);
        assert_eq!(verified.test_spec_config.side_transfer_worker_count, 4);
        assert_eq!(
            verified.test_spec_config.side_transfer_worker_p2p_port_base,
            None
        );
    }

    #[test]
    fn client_config_tcp_protocol_rejects_public_closed_runtime() {
        let cfg = ClientConfigYaml::from_str(
            r#"
instance_key: test_owner
protocol:
  protocol_type: tcp
contribute_to_cluster_pool_size:
  dram: 16777216
  vram: {}
fluxonkv_spec:
  etcd_addresses: ["127.0.0.1:2379"]
  cluster_name: test_cluster
  shared_memory_path: /tmp/test_owner
  shared_file_path: /tmp/test_owner_files
  sub_cluster: rack-a
"#,
        )
        .unwrap();
        let err = cfg.verify().unwrap_err();
        assert!(format!("{err}").contains(
            "protocol.protocol_type=tcp is not supported in the public bundled-runtime build"
        ));
    }

    #[test]
    fn client_config_tcp_protocol_rejects_unknown_legacy_override() {
        let err = ClientConfigYaml::from_str(
            r#"
instance_key: test_owner
protocol:
  protocol_type: tcp
contribute_to_cluster_pool_size:
  dram: 16777216
  vram: {}
fluxonkv_spec:
  etcd_addresses: ["127.0.0.1:2379"]
  cluster_name: test_cluster
  shared_memory_path: /tmp/test_owner
  shared_file_path: /tmp/test_owner_files
  sub_cluster: rack-a
test_spec_config:
  transport_mode: transfer_with_rpc
  rdma_device_names: ["mlx5_0"]
  legacy_transfer_backend: closed
"#,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("unknown field `legacy_transfer_backend`"));
    }

    #[test]
    fn master_config_explicit_tcp_protocol_rejects_public_closed_sdk_build() {
        let cfg = MasterConfigYaml::from_str(
            r#"
instance_key: test_master
cluster_name: test_cluster
port: 18080
etcd_endpoints: ["127.0.0.1:2379"]
protocol:
  protocol_type: tcp
network:
  subnet_whitelist: ["127.0.0.0/8"]
monitoring:
  prometheus_base_url: "http://127.0.0.1:4000/v1/prometheus"
log_dir: /tmp/test_master_logs
"#,
        )
        .unwrap();
        let err = cfg.verify().unwrap_err();
        assert!(format!("{err}").contains(
            "protocol.protocol_type=tcp is not supported in the public bundled-runtime build"
        ));
    }

    #[test]
    fn master_test_spec_config_transfer_with_rpc_keeps_rpc_fast_path() {
        let cfg = MasterConfigYaml::from_str(
            r#"
instance_key: test_master
cluster_name: test_cluster
port: 18080
etcd_endpoints: ["127.0.0.1:2379"]
network:
  subnet_whitelist: ["127.0.0.0/8"]
monitoring:
  prometheus_base_url: "http://127.0.0.1:4000/v1/prometheus"
log_dir: /tmp/test_master_logs
test_spec_config:
  disable_prefix_index: true
  disable_crossowner_ipc: true
  iceoryx_owner_client_busy_poll: false
  transport_mode: transfer_with_rpc
  rdma_device_names: [" mlx5_4 ", "mlx5_0", "mlx5_4"]
"#,
        )
        .unwrap();
        let verified = cfg.verify().unwrap();
        assert!(verified.test_spec_config.disable_prefix_index);
        assert!(verified.test_spec_config.disable_crossowner_ipc);
        assert!(!verified.test_spec_config.iceoryx_owner_client_busy_poll);
        assert_eq!(
            verified.test_spec_config.transport_mode,
            Some(TestSpecTransportMode::TransferWithRpc)
        );
        assert_eq!(
            verified.test_spec_config.rdma_device_names,
            Some(vec!["mlx5_0".to_string(), "mlx5_4".to_string()])
        );
        assert_eq!(
            verified.protocol.rdma_device_names,
            Some("mlx5_0,mlx5_4".to_string())
        );
        assert!(verified.enable_transfer_rpc_fast_path);
    }

    #[test]
    fn master_config_accepts_missing_port_for_auto_discovery() {
        let cfg = MasterConfigYaml::from_str(
            r#"
instance_key: test_master
cluster_name: test_cluster
etcd_endpoints: ["127.0.0.1:2379"]
network:
  subnet_whitelist: ["127.0.0.0/8"]
monitoring:
  prometheus_base_url: "http://127.0.0.1:4000/v1/prometheus"
log_dir: /tmp/test_master_logs
"#,
        )
        .unwrap();
        let verified = cfg.verify().unwrap();
        assert_eq!(verified.port, None);
    }
}
