use std::collections::BTreeMap;
use std::fs;
use std::path::{Component, Path};

use base64::Engine as _;
use hmac::{Hmac, Mac as _};
use postcard::{from_bytes as postcard_from_bytes, to_stdvec as postcard_to_stdvec};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::path::safe_relpath;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct FluxonFsGlobalConfig {
    pub stale_window_ms: u64,
    pub write_session_target_inflight_bytes: u64,
    pub rules: Vec<FluxonFsRule>,
    pub exports: BTreeMap<String, FluxonFsExport>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheMode {
    Disabled,
    ReadThrough,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    WriteBack,
    WriteThrough,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnRefreshError {
    ApplyStaleWindow,
    BypassCacheForDir,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FluxonFsS3KvMissPolicy {
    RemoteRead,
    StageToKvThenRead,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FluxonFsS3PermissionAction {
    All,
    ListBucket,
    ListBucketMultipartUploads,
    ListMultipartUploadParts,
    GetObject,
    PutObject,
    DeleteObject,
    AbortMultipartUpload,
}

impl FluxonFsS3PermissionAction {
    pub fn as_config_str(self) -> &'static str {
        match self {
            FluxonFsS3PermissionAction::All => "s3:*",
            FluxonFsS3PermissionAction::ListBucket => "s3:ListBucket",
            FluxonFsS3PermissionAction::ListBucketMultipartUploads => {
                "s3:ListBucketMultipartUploads"
            }
            FluxonFsS3PermissionAction::ListMultipartUploadParts => "s3:ListMultipartUploadParts",
            FluxonFsS3PermissionAction::GetObject => "s3:GetObject",
            FluxonFsS3PermissionAction::PutObject => "s3:PutObject",
            FluxonFsS3PermissionAction::DeleteObject => "s3:DeleteObject",
            FluxonFsS3PermissionAction::AbortMultipartUpload => "s3:AbortMultipartUpload",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FluxonFsS3PermissionRule {
    pub bucket: String,
    pub prefix: String,
    pub actions: Vec<FluxonFsS3PermissionAction>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FluxonFsS3PermissionAccount {
    pub username: String,
    pub password: String,
    pub permissions: Vec<FluxonFsS3PermissionRule>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FluxonFsScopeAccessMode {
    Read,
    ReadWrite,
}

impl FluxonFsScopeAccessMode {
    pub fn form_value(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::ReadWrite => "read_write",
        }
    }

    pub fn title(self) -> &'static str {
        match self {
            Self::Read => "Read",
            Self::ReadWrite => "Read + Write",
        }
    }

    pub fn from_form_value(value: &str) -> Option<Self> {
        match value.trim() {
            "read" => Some(Self::Read),
            "read_write" => Some(Self::ReadWrite),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsAccessUser {
    pub username: String,
    pub password: String,
    pub can_manage_users: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsScopeAccess {
    pub export_name: String,
    pub prefix: String,
    pub mode: FluxonFsScopeAccessMode,
    pub usernames: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsAccessModel {
    pub users: Vec<FluxonFsAccessUser>,
    pub scope_access: Vec<FluxonFsScopeAccess>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsRuntimeAccessUser {
    pub username: String,
    pub can_manage_users: bool,
    // This derived secret is used to verify RPC tokens locally on agents, not as a password verifier.
    pub rpc_token_secret_sha256_hex: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsRuntimeAccessModel {
    pub users: Vec<FluxonFsRuntimeAccessUser>,
    pub scope_access: Vec<FluxonFsScopeAccess>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FluxonFsOp {
    Stat,
    Lstat,
    ListDir,
    ReadLink,
    GetXattr,
    ListXattr,
    ReadChunk,
    WriteChunk,
    Truncate,
    Mkdir,
    Mkfifo,
    Mknod,
    Rmdir,
    Unlink,
    Link,
    Symlink,
    Rename,
    Chmod,
    Chown,
    Lchown,
    Utime,
    SetXattr,
    RemoveXattr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FluxonFsRequestIdentity {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsRpcTokenClaims {
    pub username: String,
    pub expires_unix_ms: i64,
}

pub const FLUXON_FS_RPC_TOKEN_PAYLOAD_KEY: &str = "fs_rpc_token";
pub const FLUXON_FS_INTERNAL_CONTROL_BYPASS_PAYLOAD_KEY: &str = "fs_internal_control_bypass";
pub const FLUXON_FS_RPC_TOKEN_TTL_MS: i64 = 60_000;
pub const FLUXON_FS_CONFIG_ACCESS_MODEL_JSON_KEY: &str = "access_model_json";
pub const FLUXON_FS_EXPORT_OVERLAY_JSON_KEY: &str = "export_overlay_json";
pub const FLUXON_FS_MOUNT_EXPORTS_JSON_KEY: &str = "mount_exports_json";
pub const FLUXON_FS_METADATA_INVALIDATION_STATE_JSON_KEY: &str = "metadata_invalidation_state_json";
pub const FLUXON_FS_COMPONENT_METADATA_KEY: &str = "fluxon_fs_component";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FluxonFsComponent {
    Agent,
    Controller,
}

impl FluxonFsComponent {
    pub fn as_metadata_value(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Controller => "controller",
        }
    }

    pub fn from_metadata_value(value: &str) -> Option<Self> {
        match value.trim() {
            "agent" => Some(Self::Agent),
            "controller" => Some(Self::Controller),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FluxonFsExportRoutingMode {
    StaticNodes,
    AgentRegistry,
}

#[derive(Debug, Clone)]
pub struct FluxonFsS3GatewayConfig {
    pub get_object_inflight_pieces: u64,
    pub kv_miss_policy: FluxonFsS3KvMissPolicy,
}

#[derive(Debug, Clone)]
pub struct FluxonFsRule {
    pub dir_abs: String,
    pub cache_mode: CacheMode,
    pub write_mode: WriteMode,
    pub kv_key_prefix: String,
    pub bytes_field_key: String,
    pub max_cache_bytes: u64,
    pub on_refresh_error: OnRefreshError,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsExport {
    pub remote_root_dir_abs: String,
    pub routing_mode: FluxonFsExportRoutingMode,
    pub nodes: Vec<String>,
    pub cache_kv_key_prefix: String,
    pub cache_bytes_field_key: String,
    pub cache_max_bytes: u64,
    pub inline_bytes_max_bytes: u64,
    pub metadata_cache_ttl_ms: u64,
    pub async_backfill_enabled: bool,
    pub rpc_paths: FluxonFsExportRpcPaths,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsExportRpcPaths {
    pub stat: String,
    pub open_read: String,
    pub lstat: String,
    pub list_dir: String,
    pub readlink: String,
    pub setxattr: String,
    pub getxattr: String,
    pub listxattr: String,
    pub removexattr: String,
    pub read_chunk: String,
    pub open_write_session: String,
    pub write_session_chunk: String,
    pub truncate_write_session: String,
    pub close_write_session: String,
    pub abort_write_session: String,
    pub write_chunk: String,
    pub truncate: String,
    pub mkdir: String,
    pub mkfifo: String,
    pub mknod: String,
    pub rmdir: String,
    pub unlink: String,
    pub link: String,
    pub symlink: String,
    pub rename: String,
    pub chmod: String,
    pub chown: String,
    pub lchown: String,
    pub utime: String,
}

#[derive(thiserror::Error, Debug)]
pub enum FluxonFsConfigError {
    #[error("invalid fluxon_fs.cache YAML: {detail}")]
    Invalid { detail: String },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheYaml {
    stale_window_ms: u64,
    #[serde(default)]
    write_session_target_inflight_bytes: Option<u64>,
    #[serde(default)]
    rules: Vec<RuleYaml>,
    exports: BTreeMap<String, ExportYaml>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuleYaml {
    dir_abs: String,
    cache_mode: String,
    write_mode: String,
    kv_key_prefix: String,
    bytes_field_key: String,
    max_cache_bytes: u64,
    on_refresh_error: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportYaml {
    remote_root_dir_abs: String,
    nodes: Option<Vec<String>>,
    cache_max_bytes: u64,
    #[serde(default)]
    inline_bytes_max_bytes: Option<u64>,
    #[serde(default)]
    metadata_cache_ttl_ms: Option<u64>,
    #[serde(default)]
    async_backfill_enabled: Option<bool>,
}

fn invalid(detail: impl Into<String>) -> FluxonFsConfigError {
    FluxonFsConfigError::Invalid {
        detail: detail.into(),
    }
}

pub fn parse_cache_config_yaml(text: &str) -> Result<FluxonFsGlobalConfig, FluxonFsConfigError> {
    let cfg: CacheYaml = serde_yaml::from_str(text).map_err(|e| {
        // English note: serde_yaml errors usually do not include the original document; include it for debugging.
        invalid(format!(
            "yaml parse failed: {}\n--- YAML BEGIN ---\n{}\n--- YAML END ---",
            e, text
        ))
    })?;

    if cfg.stale_window_ms == 0 {
        return Err(invalid("stale_window_ms must be > 0"));
    }
    let write_session_target_inflight_bytes = cfg
        .write_session_target_inflight_bytes
        .unwrap_or(FS_CACHE_DEFAULT_WRITE_SESSION_TARGET_INFLIGHT_BYTES_V1);
    if write_session_target_inflight_bytes == 0 {
        return Err(invalid("write_session_target_inflight_bytes must be > 0"));
    }

    let mut rules: Vec<FluxonFsRule> = Vec::new();
    for (idx, r) in cfg.rules.into_iter().enumerate() {
        if r.dir_abs.trim().is_empty() {
            return Err(invalid(format!("rules[{}].dir_abs must be non-empty", idx)));
        }
        if !Path::new(&r.dir_abs).is_absolute() {
            return Err(invalid(format!(
                "rules[{}].dir_abs must be an absolute path",
                idx
            )));
        }
        let cache_mode = parse_cache_mode(&r.cache_mode).ok_or_else(|| {
            invalid(format!(
                "rules[{}].cache_mode invalid: {}",
                idx, r.cache_mode
            ))
        })?;
        let write_mode = parse_write_mode(&r.write_mode).ok_or_else(|| {
            invalid(format!(
                "rules[{}].write_mode invalid: {}",
                idx, r.write_mode
            ))
        })?;
        let on_refresh_error = parse_on_refresh_error(&r.on_refresh_error).ok_or_else(|| {
            invalid(format!(
                "rules[{}].on_refresh_error invalid: {}",
                idx, r.on_refresh_error
            ))
        })?;
        if !r.kv_key_prefix.starts_with('/') || !r.kv_key_prefix.ends_with('/') {
            return Err(invalid(format!(
                "rules[{}].kv_key_prefix must start and end with '/'",
                idx
            )));
        }
        if r.bytes_field_key.trim().is_empty() {
            return Err(invalid(format!(
                "rules[{}].bytes_field_key must be non-empty",
                idx
            )));
        }
        if r.max_cache_bytes == 0 {
            return Err(invalid(format!(
                "rules[{}].max_cache_bytes must be > 0",
                idx
            )));
        }
        rules.push(FluxonFsRule {
            dir_abs: r.dir_abs,
            cache_mode,
            write_mode,
            kv_key_prefix: r.kv_key_prefix,
            bytes_field_key: r.bytes_field_key,
            max_cache_bytes: r.max_cache_bytes,
            on_refresh_error,
        });
    }

    let mut exports: BTreeMap<String, FluxonFsExport> = BTreeMap::new();
    for (name, e) in cfg.exports.into_iter() {
        if name.trim().is_empty() {
            return Err(invalid("exports keys must be non-empty strings"));
        }
        if !Path::new(&e.remote_root_dir_abs).is_absolute() {
            return Err(invalid(format!(
                "exports[{}].remote_root_dir_abs must be an absolute path",
                name
            )));
        }
        let (routing_mode, nodes) = match e.nodes {
            Some(nodes) => {
                if nodes.is_empty() {
                    return Err(invalid(format!(
                        "exports[{}].nodes must be non-empty when provided",
                        name
                    )));
                }
                for n in nodes.iter() {
                    if n.trim().is_empty() {
                        return Err(invalid(format!(
                            "exports[{}].nodes contains empty string",
                            name
                        )));
                    }
                }
                (FluxonFsExportRoutingMode::StaticNodes, nodes)
            }
            None => (FluxonFsExportRoutingMode::AgentRegistry, Vec::new()),
        };

        if e.cache_max_bytes == 0 {
            return Err(invalid(format!(
                "exports[{}].cache_max_bytes must be > 0",
                name
            )));
        }

        exports.insert(
            name.clone(),
            FluxonFsExport {
                remote_root_dir_abs: e.remote_root_dir_abs,
                routing_mode,
                nodes,
                cache_kv_key_prefix: export_cache_kv_key_prefix_for_export_name_v1(name.as_str()),
                cache_bytes_field_key: FS_EXPORT_CACHE_BYTES_FIELD_KEY.to_string(),
                cache_max_bytes: e.cache_max_bytes,
                inline_bytes_max_bytes: e
                    .inline_bytes_max_bytes
                    .unwrap_or(FS_EXPORT_DEFAULT_INLINE_BYTES_MAX_BYTES_V1),
                metadata_cache_ttl_ms: e
                    .metadata_cache_ttl_ms
                    .unwrap_or(FS_EXPORT_DEFAULT_METADATA_CACHE_TTL_MS_V1),
                async_backfill_enabled: e.async_backfill_enabled.unwrap_or(true),
                rpc_paths: export_rpc_paths_for_export_name_v1(name.as_str()),
            },
        );
    }

    Ok(FluxonFsGlobalConfig {
        stale_window_ms: cfg.stale_window_ms,
        write_session_target_inflight_bytes,
        rules,
        exports,
    })
}

fn parse_cache_mode(s: &str) -> Option<CacheMode> {
    match s.trim().to_ascii_lowercase().as_str() {
        "disabled" => Some(CacheMode::Disabled),
        "read_through" => Some(CacheMode::ReadThrough),
        _ => None,
    }
}

fn parse_write_mode(s: &str) -> Option<WriteMode> {
    match s.trim().to_ascii_lowercase().as_str() {
        "write_back" => Some(WriteMode::WriteBack),
        "write_through" => Some(WriteMode::WriteThrough),
        _ => None,
    }
}

fn parse_on_refresh_error(s: &str) -> Option<OnRefreshError> {
    match s.trim().to_ascii_lowercase().as_str() {
        "apply_stale_window" => Some(OnRefreshError::ApplyStaleWindow),
        "bypass_cache_for_dir" => Some(OnRefreshError::BypassCacheForDir),
        _ => None,
    }
}

#[derive(Debug, Clone)]
pub struct FluxonFsMasterConfig {
    pub instance_key: String,
    pub pull_interval_ms: Option<u64>,
}

pub const FLUXON_FS_CONTROL_SCHEMA_VERSION: i64 = 1;
pub const FS_MASTER_CONFIG_RPC_PATH: &str = "/fluxon_fs/config";
pub const FS_MASTER_MOUNT_REGISTRY_RPC_PATH: &str = "/fluxon_fs/mount_registry";
pub const FS_MASTER_EXPORT_REGISTRY_RPC_PATH: &str = "/fluxon_fs/export_registry";
pub const FS_MASTER_METADATA_INVALIDATION_PUBLISH_RPC_PATH: &str =
    "/fluxon_fs/v1/master_metadata_invalidation_publish";

// English note:
// - Before the first successful config snapshot, agents do not yet know the authoritative
//   master-configured pull_interval_ms.
// - Bootstrap retry still needs a bounded sleep to avoid a busy loop while master is not ready.
// - This constant applies only before the first successful master sync; steady-state interval is
//   always the value delivered by fs master.
pub const FS_MASTER_BOOTSTRAP_PULL_INTERVAL_MS: u64 = 1000;

// English note:
// - This RPC is a stable v1 contract between fluxon_fs master and fluxon_fs agents.
// - The master uses it to pull a full "exports snapshot" from each online agent, so the master
//   can rebuild export registry after restart and prune exports immediately on agent exit.
pub const FS_AGENT_EXPORTS_SNAPSHOT_RPC_PATH: &str = "/fluxon_fs/v1/agent_exports_snapshot";

// English note:
// - This RPC is the reverse direction: fluxon_fs agents push export snapshot updates to the
//   fluxon_fs master whenever exports change at runtime (dynamic publish/unpublish).
// - The master should treat this as "authoritative snapshot for that agent" and replace any
//   previous records of the same agent_instance_key.
pub const FS_MASTER_AGENT_EXPORTS_PUSH_RPC_PATH: &str = "/fluxon_fs/v1/master_agent_exports_push";

// English note:
// - These RPCs are user/admin-facing control-plane calls to mutate an agent's export list at
//   runtime. They intentionally live on the agent side.
pub const FS_AGENT_EXPORT_PUBLISH_RPC_PATH: &str = "/fluxon_fs/v1/agent_export_publish";
pub const FS_AGENT_EXPORT_UNPUBLISH_RPC_PATH: &str = "/fluxon_fs/v1/agent_export_unpublish";
pub const FS_AGENT_DECLARED_EXPORT_JSON_KEY: &str = "declared_export_json";
// Transfer control plane uses three RPC directions:
// 1. master receives worker progress and completion,
// 2. source agents answer scan and read requests,
// 3. destination agents accept worker-launch requests.
pub const FS_MASTER_TRANSFER_SCHEDULER_HEARTBEAT_RPC_PATH: &str =
    "/fluxon_fs/v1/master_transfer_scheduler_heartbeat";
pub const FS_MASTER_TRANSFER_SCHEDULER_RESULT_RPC_PATH: &str =
    "/fluxon_fs/v1/master_transfer_scheduler_result";
pub const FS_AGENT_TRANSFER_SCAN_RPC_PATH: &str = "/fluxon_fs/v1/agent_transfer_scan";
pub const FS_AGENT_TRANSFER_READ_RPC_PATH: &str = "/fluxon_fs/v1/agent_transfer_read";
pub const FS_AGENT_TRANSFER_STREAM_OPEN_RPC_PATH: &str = "/fluxon_fs/v1/agent_transfer_stream_open";
pub const FS_AGENT_TRANSFER_STREAM_NEXT_RPC_PATH: &str = "/fluxon_fs/v1/agent_transfer_stream_next";
pub const FS_AGENT_TRANSFER_STREAM_CLOSE_RPC_PATH: &str =
    "/fluxon_fs/v1/agent_transfer_stream_close";
pub const FS_AGENT_TRANSFER_WORKER_RPC_PATH: &str = "/fluxon_fs/v1/agent_transfer_worker";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FluxonFsTransferJobState {
    Running,
    Stopping,
    Completed,
    Cancelled,
    Failed,
}

impl FluxonFsTransferJobState {
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }

    pub fn from_db_str(raw: &str) -> Option<Self> {
        match raw {
            "running" => Some(Self::Running),
            "stopping" => Some(Self::Stopping),
            "completed" => Some(Self::Completed),
            "cancelled" => Some(Self::Cancelled),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FluxonFsTransferScanUnitState {
    Ready,
    Scanning,
    SplitCommitted,
    Finished,
    Expired,
}

impl FluxonFsTransferScanUnitState {
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Scanning => "scanning",
            Self::SplitCommitted => "split_committed",
            Self::Finished => "finished",
            Self::Expired => "expired",
        }
    }

    pub fn from_db_str(raw: &str) -> Option<Self> {
        match raw {
            "ready" => Some(Self::Ready),
            "scanning" => Some(Self::Scanning),
            "split_committed" => Some(Self::SplitCommitted),
            "finished" => Some(Self::Finished),
            "expired" => Some(Self::Expired),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FluxonFsTransferBatchKind {
    // The batch covers the entire subtree rooted at root_relpath. Once this
    // disposition is durable, later scans must stop splitting below that root.
    FullDir,
    // The batch is one streaming execution slice owned by a subtree scan unit.
    // It may contain files, symlink notices, and empty directories from any
    // descendants below root_relpath, but it is not a durable full-subtree
    // coverage marker by itself.
    SubtreeSlice,
    // The batch covers only direct non-directory files and collect-info objects
    // under root_relpath. Every direct child directory stays outside the batch
    // and is expected to be scanned by separate child scan units.
    DirectFilesOnly,
}

impl FluxonFsTransferBatchKind {
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::FullDir => "full_dir",
            Self::SubtreeSlice => "subtree_slice",
            Self::DirectFilesOnly => "direct_files_only",
        }
    }

    pub fn from_db_str(raw: &str) -> Option<Self> {
        match raw {
            "full_dir" => Some(Self::FullDir),
            "subtree_slice" => Some(Self::SubtreeSlice),
            "direct_files_only" => Some(Self::DirectFilesOnly),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FluxonFsTransferBatchState {
    Ready,
    Running,
    Done,
    Finished,
    Cancelled,
    Expired,
}

impl FluxonFsTransferBatchState {
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Running => "running",
            Self::Done => "done",
            Self::Finished => "finished",
            Self::Cancelled => "cancelled",
            Self::Expired => "expired",
        }
    }

    pub fn from_db_str(raw: &str) -> Option<Self> {
        match raw {
            "ready" => Some(Self::Ready),
            "running" => Some(Self::Running),
            "done" => Some(Self::Done),
            "finished" => Some(Self::Finished),
            "cancelled" => Some(Self::Cancelled),
            "expired" => Some(Self::Expired),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FluxonFsTransferWorkerState {
    Idle,
    Running,
    Draining,
    Expired,
}

impl FluxonFsTransferWorkerState {
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Running => "running",
            Self::Draining => "draining",
            Self::Expired => "expired",
        }
    }

    pub fn from_db_str(raw: &str) -> Option<Self> {
        match raw {
            "idle" => Some(Self::Idle),
            "running" => Some(Self::Running),
            "draining" => Some(Self::Draining),
            "expired" => Some(Self::Expired),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferScanFrontierEntry {
    pub relpath: String,
    pub size: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferScanFrontierDirEntry {
    pub relpath: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferScanFrontier {
    pub direct_files: Vec<FluxonFsTransferScanFrontierEntry>,
    pub direct_dirs: Vec<FluxonFsTransferScanFrontierDirEntry>,
    pub empty_dirs: Vec<FluxonFsTransferScanFrontierDirEntry>,
}

// A disposition lets a restarted scan observe coverage that is already durable.
// This is how scan restart stays idempotent without persisting scan units.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferDispositionWire {
    pub root_relpath: String,
    pub generation: i64,
    pub batch_kind: FluxonFsTransferBatchKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FluxonFsTransferScanMode {
    FullTree,
    SubtreeStreaming,
    RootDirectFanoutOnly,
    DirectoryDirectFanoutOnly,
}

impl Default for FluxonFsTransferScanMode {
    fn default() -> Self {
        Self::FullTree
    }
}

// generation increases when a directory is split into deeper scan units. The
// pair (root_relpath, generation) distinguishes equivalent scan coverage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferScanChildUnitWire {
    pub scan_unit_id: String,
    pub root_relpath: String,
    pub generation: i64,
    #[serde(default)]
    pub scan_mode: FluxonFsTransferScanMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferManifestEntryWire {
    pub relpath: String,
    pub size: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferManifestWire {
    pub version: u32,
    pub entry_count: i64,
    pub total_bytes: i64,
    pub entries: Vec<FluxonFsTransferManifestEntryWire>,
    pub empty_dir_relpaths: Vec<String>,
}

pub const FLUXON_FS_TRANSFER_MANIFEST_VERSION_V1: u32 = 1;
pub const FLUXON_FS_TRANSFER_MANIFEST_VERSION_V2: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FluxonFsTransferManifestWireV1 {
    pub version: u32,
    pub entry_count: i64,
    pub total_bytes: i64,
    pub entries: Vec<FluxonFsTransferManifestEntryWire>,
}

impl FluxonFsTransferManifestWire {
    // Manifest content is the authoritative batch truth once scan has decided
    // coverage. Workers do not rediscover files or empty directories after a
    // batch is persisted.
    pub fn new(
        entries: Vec<FluxonFsTransferManifestEntryWire>,
        empty_dir_relpaths: Vec<String>,
    ) -> Self {
        let entry_count = entries.len() as i64;
        let total_bytes = entries
            .iter()
            .fold(0_i64, |acc, entry| acc.saturating_add(entry.size));
        Self {
            version: FLUXON_FS_TRANSFER_MANIFEST_VERSION_V2,
            entry_count,
            total_bytes,
            entries,
            empty_dir_relpaths,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.version != FLUXON_FS_TRANSFER_MANIFEST_VERSION_V2 {
            return Err(format!(
                "unsupported transfer manifest version: {}",
                self.version
            ));
        }
        let expected_entry_count = self.entries.len() as i64;
        if self.entry_count != expected_entry_count {
            return Err(format!(
                "transfer manifest entry_count mismatch: header={} actual={}",
                self.entry_count, expected_entry_count
            ));
        }
        let expected_total_bytes = self
            .entries
            .iter()
            .fold(0_i64, |acc, entry| acc.saturating_add(entry.size));
        if self.total_bytes != expected_total_bytes {
            return Err(format!(
                "transfer manifest total_bytes mismatch: header={} actual={}",
                self.total_bytes, expected_total_bytes
            ));
        }
        let mut previous_relpath: Option<&str> = None;
        for relpath in &self.empty_dir_relpaths {
            if relpath.trim().is_empty() {
                return Err("transfer manifest empty_dir_relpath must be non-empty".to_string());
            }
            if previous_relpath.is_some_and(|previous| previous >= relpath.as_str()) {
                return Err(format!(
                    "transfer manifest empty_dir_relpaths must be strictly sorted: previous={} current={}",
                    previous_relpath.unwrap(),
                    relpath
                ));
            }
            previous_relpath = Some(relpath.as_str());
        }
        Ok(())
    }

    pub fn encode_to_blob(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        postcard_to_stdvec(self).map_err(|e| format!("encode transfer manifest blob failed: {}", e))
    }

    pub fn decode_from_blob(blob: &[u8]) -> Result<Self, String> {
        match postcard_from_bytes::<Self>(blob) {
            Ok(manifest) => {
                manifest.validate()?;
                Ok(manifest)
            }
            Err(primary_err) => {
                let legacy_manifest = postcard_from_bytes::<FluxonFsTransferManifestWireV1>(blob)
                    .map_err(|legacy_err| {
                    format!(
                        "decode transfer manifest blob failed: current={} legacy={}",
                        primary_err, legacy_err
                    )
                })?;
                if legacy_manifest.version != FLUXON_FS_TRANSFER_MANIFEST_VERSION_V1 {
                    return Err(format!(
                        "unsupported legacy transfer manifest version: {}",
                        legacy_manifest.version
                    ));
                }
                let manifest = Self::new(legacy_manifest.entries, Vec::new());
                manifest.validate()?;
                Ok(manifest)
            }
        }
    }
}

// One scan result may contribute any number of DirectFilesOnly batches for the
// current directory and any number of subtree-level execution batches through
// full_dir_batches. The field name is kept for wire compatibility.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferScanBatchWire {
    pub batch_id: String,
    pub root_relpath: String,
    pub batch_kind: FluxonFsTransferBatchKind,
    pub manifest_blob: Vec<u8>,
    pub collect_infos: Vec<FluxonFsTransferBatchCollectInfoWire>,
    pub generation: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FluxonFsTransferCollectInfoKind {
    SymlinkNotice,
}

impl FluxonFsTransferCollectInfoKind {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::SymlinkNotice => "symlink_notice",
        }
    }

    pub fn from_db_str(raw: &str) -> Result<Self, String> {
        match raw {
            "symlink_notice" => Ok(Self::SymlinkNotice),
            _ => Err(format!("unknown transfer collect info kind: {}", raw)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferBatchCollectInfoWire {
    pub collect_kind: FluxonFsTransferCollectInfoKind,
    pub collect_blob: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferSymlinkNoticeEntryWire {
    pub relpath: String,
    pub link_target: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FluxonFsTransferSkipEntryKind {
    Dir,
    File,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferSkipEntryWire {
    pub kind: FluxonFsTransferSkipEntryKind,
    pub relpath: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferJobSpecWire {
    pub skip_entries: Vec<FluxonFsTransferSkipEntryWire>,
}

pub const FLUXON_FS_LOCAL_TRANSFER_CHECK_SRC_EXPORT: &str = "__local_check_src__";
pub const FLUXON_FS_LOCAL_TRANSFER_CHECK_DST_EXPORT: &str = "__local_check_dst__";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsLocalTransferCheckJobSpecWire {
    pub src_root_dir_abs: String,
    pub batch_ready_bytes: i64,
    pub skip_entries: Vec<FluxonFsTransferSkipEntryWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferScanAssignmentWire {
    // scan_epoch is the master-issued invalidation number. If the scheduler
    // restarts scanning and bumps the epoch, older scan results must be rejected
    // by durable state application.
    pub job_id: String,
    pub scan_epoch: i64,
    pub scan_unit_id: String,
    pub scan_task_id: String,
    // generation tracks how many split steps produced this unit. It is part of
    // the batch-equivalence key used to dedupe restarted scans.
    pub root_relpath: String,
    pub generation: i64,
    #[serde(default)]
    pub scan_mode: FluxonFsTransferScanMode,
    pub src_export: String,
    pub src_exporter_id: String,
    pub batch_ready_bytes: i64,
    // For scan assignments this field is the master-issued deadline witness.
    // The source checker should return a partial result before this time
    // instead of blocking forever on one deep subtree. Worker assignments reuse
    // the same field name for worker lease expiry, so the meaning is
    // "master-issued stop-after timestamp" for the corresponding control task.
    pub lease_expire_unix_ms: i64,
    pub known_dispositions: Vec<FluxonFsTransferDispositionWire>,
    #[serde(default)]
    pub live_child_scan_roots: Vec<String>,
    pub skip_entries: Vec<FluxonFsTransferSkipEntryWire>,
}

// A scan result can emit ready batches, child scan units, or both. Not storing
// scan-unit state is safe because result acceptance is guarded by scan_epoch and
// batch equivalence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferScanResultWire {
    pub job_id: String,
    pub scan_epoch: i64,
    pub scan_unit_id: String,
    pub scan_task_id: String,
    pub root_relpath: String,
    pub generation: i64,
    pub frontier: FluxonFsTransferScanFrontier,
    pub direct_files_only_batches: Vec<FluxonFsTransferScanBatchWire>,
    pub child_scan_units: Vec<FluxonFsTransferScanChildUnitWire>,
    pub full_dir_batches: Vec<FluxonFsTransferScanBatchWire>,
    pub finished: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FluxonFsTransferScanLaunchDispositionWire {
    Started,
    AlreadyRunning,
    AlreadyCompleted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferScanLaunchResultWire {
    pub disposition: FluxonFsTransferScanLaunchDispositionWire,
}

impl FluxonFsTransferScanLaunchResultWire {
    pub fn started() -> Self {
        Self {
            disposition: FluxonFsTransferScanLaunchDispositionWire::Started,
        }
    }

    pub fn already_running() -> Self {
        Self {
            disposition: FluxonFsTransferScanLaunchDispositionWire::AlreadyRunning,
        }
    }

    pub fn already_completed() -> Self {
        Self {
            disposition: FluxonFsTransferScanLaunchDispositionWire::AlreadyCompleted,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FluxonFsTransferScanEventKindWire {
    Started,
    Append,
    Finished,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferScanEventWire {
    pub job_id: String,
    pub scan_epoch: i64,
    pub scan_unit_id: String,
    pub scan_task_id: String,
    pub root_relpath: String,
    pub generation: i64,
    pub event_seq_no: i64,
    pub event_kind: FluxonFsTransferScanEventKindWire,
    pub direct_files_only_batches: Vec<FluxonFsTransferScanBatchWire>,
    pub child_scan_units: Vec<FluxonFsTransferScanChildUnitWire>,
    pub full_dir_batches: Vec<FluxonFsTransferScanBatchWire>,
    pub error_detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferScanEventAckWire {
    pub accepted: bool,
    pub continue_running: bool,
    pub lease_expire_unix_ms: i64,
}

impl FluxonFsTransferScanEventAckWire {
    pub fn continue_running(accepted: bool, lease_expire_unix_ms: i64) -> Self {
        Self {
            accepted,
            continue_running: true,
            lease_expire_unix_ms,
        }
    }

    pub fn stop(accepted: bool) -> Self {
        Self {
            accepted,
            continue_running: false,
            lease_expire_unix_ms: 0,
        }
    }
}

pub fn normalize_transfer_skip_relpath(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("skip relpath must be non-empty".to_string());
    }
    let path = Path::new(trimmed);
    if path.is_absolute() {
        return Err(format!("skip relpath must be relative: {}", raw));
    }
    for component in path.components() {
        match component {
            Component::Normal(_) => {}
            _ => {
                return Err(format!(
                    "skip relpath must be a normal relative path: {}",
                    raw
                ));
            }
        }
    }
    safe_relpath(trimmed).map_err(|e| format!("invalid skip relpath: input={} err={}", raw, e))
}

pub fn normalize_transfer_skip_entries(
    entries: Vec<FluxonFsTransferSkipEntryWire>,
) -> Result<Vec<FluxonFsTransferSkipEntryWire>, String> {
    let mut normalized = Vec::with_capacity(entries.len());
    for entry in entries {
        normalized.push(FluxonFsTransferSkipEntryWire {
            kind: entry.kind,
            relpath: normalize_transfer_skip_relpath(entry.relpath.as_str())?,
        });
    }
    normalized.sort_by(|a, b| {
        a.relpath.cmp(&b.relpath).then_with(|| {
            let ak = match a.kind {
                FluxonFsTransferSkipEntryKind::Dir => 0_u8,
                FluxonFsTransferSkipEntryKind::File => 1_u8,
            };
            let bk = match b.kind {
                FluxonFsTransferSkipEntryKind::Dir => 0_u8,
                FluxonFsTransferSkipEntryKind::File => 1_u8,
            };
            ak.cmp(&bk)
        })
    });
    for pair in normalized.windows(2) {
        let prev = &pair[0];
        let curr = &pair[1];
        if prev.kind == curr.kind && prev.relpath == curr.relpath {
            return Err(format!("duplicate skip entry: {:?}", curr));
        }
        if prev.kind == FluxonFsTransferSkipEntryKind::Dir
            && (curr.relpath == prev.relpath
                || curr
                    .relpath
                    .strip_prefix(prev.relpath.as_str())
                    .is_some_and(|suffix| suffix.starts_with('/')))
        {
            return Err(format!(
                "nested skip entries are not allowed: parent_dir={} child={}",
                prev.relpath, curr.relpath
            ));
        }
    }
    Ok(normalized)
}

pub fn encode_transfer_job_spec(spec: &FluxonFsTransferJobSpecWire) -> Result<Vec<u8>, String> {
    serde_json::to_vec(spec).map_err(|e| format!("encode transfer job spec failed: {}", e))
}

pub fn decode_transfer_job_spec(blob: &[u8]) -> Result<FluxonFsTransferJobSpecWire, String> {
    if blob.is_empty() {
        return Ok(FluxonFsTransferJobSpecWire {
            skip_entries: Vec::new(),
        });
    }
    let mut spec: FluxonFsTransferJobSpecWire = serde_json::from_slice(blob)
        .map_err(|e| format!("decode transfer job spec failed: {}", e))?;
    spec.skip_entries = normalize_transfer_skip_entries(spec.skip_entries)?;
    Ok(spec)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferWorkerAssignmentWire {
    pub job_id: String,
    pub batch_id: String,
    // worker_task_id identifies one concrete execution attempt for a batch.
    // Retries of launch RPC for the same attempt must reuse this id.
    pub worker_task_id: String,
    // worker_id is the stable scheduler slot identity. Multiple task attempts
    // may exist over time for the same worker_id when old leases expire.
    pub worker_id: String,
    pub batch_kind: FluxonFsTransferBatchKind,
    pub src_export: String,
    pub dst_export: String,
    pub src_exporter_id: String,
    pub dst_exporter_id: String,
    pub dst_root_relpath: String,
    pub root_relpath: String,
    // staging_prefix is the invisible namespace for prepared files. Data becomes
    // visible only after destination-side promotion into the final path.
    pub staging_prefix: String,
    pub lease_expire_unix_ms: i64,
    pub manifest_blob: Vec<u8>,
    pub collect_infos: Vec<FluxonFsTransferBatchCollectInfoWire>,
}

// File results report only promoted outputs. A staged-only file must never be
// acknowledged back to the master as visible progress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferWorkerFileResultWire {
    pub relpath: String,
    pub staging_relpath: String,
    pub final_relpath: String,
    pub visible_size: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferWorkerFailedFileResultWire {
    pub relpath: String,
    pub reason_kind: FluxonFsTransferFailedFileReasonKindWire,
    pub reason_detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FluxonFsTransferFailedFileReasonKindWire {
    SourceContentChanged,
    SourcePermissionDenied,
}

impl FluxonFsTransferFailedFileReasonKindWire {
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::SourceContentChanged => "source_content_changed",
            Self::SourcePermissionDenied => "source_permission_denied",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferWorkerCollectInfoResultWire {
    pub collect_kind: FluxonFsTransferCollectInfoKind,
    pub output_relpath: String,
    pub materialized_bytes: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferWorkerResultWire {
    pub job_id: String,
    pub batch_id: String,
    // Both ids are echoed back so the master can reject stale results from an
    // older worker attempt that already lost ownership.
    pub worker_task_id: String,
    pub worker_id: String,
    pub file_results: Vec<FluxonFsTransferWorkerFileResultWire>,
    pub failed_file_results: Vec<FluxonFsTransferWorkerFailedFileResultWire>,
    pub collect_info_results: Vec<FluxonFsTransferWorkerCollectInfoResultWire>,
    // The worker may finish before the next periodic heartbeat, so result
    // carries one final live snapshot for history/UI telemetry.
    pub final_telemetry: Option<FluxonFsTransferWorkerHeartbeatTelemetryWire>,
}

// Heartbeat is the keepalive for one worker attempt. The master may answer with
// a stop decision when ownership moved elsewhere.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferWorkerHeartbeatWire {
    pub job_id: String,
    pub worker_id: String,
    pub assigned_batch_id: String,
    pub worker_task_id: String,
    pub heartbeat_unix_ms: i64,
    pub telemetry: Option<FluxonFsTransferWorkerHeartbeatTelemetryWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferWorkerHeartbeatTelemetryWire {
    pub total_written_bytes: i64,
    pub window_started_unix_ms: i64,
    pub window_elapsed_ms: i64,
    pub window_bytes: i64,
    pub window_goodput_bytes_per_sec: i64,
    pub desired_file_lanes: i64,
}

// A transfer stream binds one worker attempt to one source file handle so
// chunk RPCs can reuse the open file instead of reopening on every read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferReadStreamOpenWire {
    pub worker_task_id: String,
    pub export: String,
    pub relpath: String,
    // Source-side stream handles are process-local. If the source agent
    // restarts and drops the old handle, the worker must reopen at the exact
    // next required offset instead of replaying from byte 0.
    pub initial_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferReadStreamOpenResultWire {
    pub stream_id: String,
    pub size: i64,
}

// next_offset is client-supplied so retries can safely replay the same chunk
// without advancing the source-side stream twice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferReadStreamNextWire {
    pub stream_id: String,
    pub next_offset: i64,
    pub length: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferReadStreamNextResultWire {
    pub stream_missing: bool,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferReadStreamCloseWire {
    pub stream_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FluxonFsTransferWorkerLaunchDispositionWire {
    Started,
    AlreadyRunning,
    AlreadyCompleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FluxonFsTransferWorkerStopReasonWire {
    Superseded,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferWorkerLaunchResultWire {
    pub disposition: FluxonFsTransferWorkerLaunchDispositionWire,
}

impl FluxonFsTransferWorkerLaunchResultWire {
    pub fn started() -> Self {
        Self {
            disposition: FluxonFsTransferWorkerLaunchDispositionWire::Started,
        }
    }

    pub fn already_running() -> Self {
        Self {
            disposition: FluxonFsTransferWorkerLaunchDispositionWire::AlreadyRunning,
        }
    }

    pub fn already_completed() -> Self {
        Self {
            disposition: FluxonFsTransferWorkerLaunchDispositionWire::AlreadyCompleted,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferWorkerHeartbeatResultWire {
    pub continue_running: bool,
    pub lease_expire_unix_ms: i64,
    pub stop_reason: Option<FluxonFsTransferWorkerStopReasonWire>,
}

impl FluxonFsTransferWorkerHeartbeatResultWire {
    // A continue response extends the durable lease for the current attempt.
    pub fn continue_running(lease_expire_unix_ms: i64) -> Self {
        Self {
            continue_running: true,
            lease_expire_unix_ms,
            stop_reason: None,
        }
    }

    // Stop means the worker has been superseded and must stop making further
    // visible progress, even if local execution could technically continue.
    pub fn stop(reason: FluxonFsTransferWorkerStopReasonWire) -> Self {
        Self {
            continue_running: false,
            lease_expire_unix_ms: 0,
            stop_reason: Some(reason),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferWorkerResultAckWire {
    pub accepted: bool,
    pub stop_reason: Option<FluxonFsTransferWorkerStopReasonWire>,
}

impl FluxonFsTransferWorkerResultAckWire {
    // Accepted means the result belongs to the current durable owner attempt.
    pub fn accepted() -> Self {
        Self {
            accepted: true,
            stop_reason: None,
        }
    }

    // Stop means the result arrived after ownership had already moved.
    pub fn stop(reason: FluxonFsTransferWorkerStopReasonWire) -> Self {
        Self {
            accepted: false,
            stop_reason: Some(reason),
        }
    }
}

pub fn transfer_collect_info_output_relpath(
    batch_id: &str,
    collect_kind: FluxonFsTransferCollectInfoKind,
) -> Result<String, String> {
    let trimmed = batch_id.trim();
    if trimmed.is_empty() {
        return Err("batch_id for transfer collect info must be non-empty".to_string());
    }
    if trimmed.contains('/') {
        return Err(format!(
            "batch_id for transfer collect info must not contain '/': {}",
            batch_id
        ));
    }
    let filename = match collect_kind {
        FluxonFsTransferCollectInfoKind::SymlinkNotice => "symlinks.jsonl",
    };
    Ok(format!(
        "fluxon_collect_info/batches/{}/{}",
        trimmed, filename
    ))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsAgentExportSnapshotItemWire {
    pub export_name: String,
    pub export: FluxonFsExport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsAgentDeclaredExportWire {
    pub export_name: String,
    pub export: FluxonFsExport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsAgentExportOverlayWire {
    pub disabled_exports: Vec<String>,
    pub upsert_exports: BTreeMap<String, FluxonFsExport>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FsMetadataInvalidationScopeWire {
    Exact,
    Prefix,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsMetadataInvalidationEventWire {
    pub export_name: String,
    pub relpath: String,
    pub scope: FsMetadataInvalidationScopeWire,
    pub seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsMetadataInvalidationStateWire {
    pub latest_seq: u64,
    pub events: Vec<FsMetadataInvalidationEventWire>,
}

pub const FS_EXPORT_DEFAULT_CACHE_MAX_BYTES_V1: u64 = 1024 * 1024;
pub const FS_EXPORT_DEFAULT_INLINE_BYTES_MAX_BYTES_V1: u64 =
    crate::s3_gateway::FS_S3_OBJECT_PIECE_BYTES as u64;
pub const FS_EXPORT_DEFAULT_METADATA_CACHE_TTL_MS_V1: u64 = 5_000;
pub const FS_CACHE_DEFAULT_WRITE_SESSION_TARGET_INFLIGHT_BYTES_V1: u64 = 128 * 1024 * 1024;
const FS_ADMIN_BROWSE_EXPORT_PREFIX_V1: &str = "fluxon-admin-root-";

pub fn export_rpc_base_path_for_export_name_v1(export_name: &str) -> String {
    format!("/fluxon_fs/{}", export_name.trim())
}

pub fn export_fallocate_rpc_path_for_export_name_v1(export_name: &str) -> String {
    format!(
        "{}/fallocate",
        export_rpc_base_path_for_export_name_v1(export_name)
    )
}

pub fn export_fiemap_rpc_path_for_export_name_v1(export_name: &str) -> String {
    format!(
        "{}/fiemap",
        export_rpc_base_path_for_export_name_v1(export_name)
    )
}

pub fn export_rpc_paths_for_export_name_v1(export_name: &str) -> FluxonFsExportRpcPaths {
    let base = export_rpc_base_path_for_export_name_v1(export_name);
    FluxonFsExportRpcPaths {
        stat: format!("{}/stat", base),
        open_read: format!("{}/open_read", base),
        lstat: format!("{}/lstat", base),
        list_dir: format!("{}/list_dir", base),
        readlink: format!("{}/readlink", base),
        setxattr: format!("{}/setxattr", base),
        getxattr: format!("{}/getxattr", base),
        listxattr: format!("{}/listxattr", base),
        removexattr: format!("{}/removexattr", base),
        read_chunk: format!("{}/read_chunk", base),
        open_write_session: format!("{}/open_write_session", base),
        write_session_chunk: format!("{}/write_session_chunk", base),
        truncate_write_session: format!("{}/truncate_write_session", base),
        close_write_session: format!("{}/close_write_session", base),
        abort_write_session: format!("{}/abort_write_session", base),
        write_chunk: format!("{}/write_chunk", base),
        truncate: format!("{}/truncate", base),
        mkdir: format!("{}/mkdir", base),
        mkfifo: format!("{}/mkfifo", base),
        mknod: format!("{}/mknod", base),
        rmdir: format!("{}/rmdir", base),
        unlink: format!("{}/unlink", base),
        link: format!("{}/link", base),
        symlink: format!("{}/symlink", base),
        rename: format!("{}/rename", base),
        chmod: format!("{}/chmod", base),
        chown: format!("{}/chown", base),
        lchown: format!("{}/lchown", base),
        utime: format!("{}/utime", base),
    }
}

pub const FS_EXPORT_CACHE_BYTES_FIELD_KEY: &str = "bytes";

pub fn export_cache_kv_key_prefix_for_export_name_v1(export_name: &str) -> String {
    let export = export_name.trim();
    format!("/fluxon_fs_cache/{}/", export)
}

pub fn agent_registry_export_for_name_and_root_v1(
    export_name: &str,
    remote_root_dir_abs: &str,
) -> FluxonFsExport {
    FluxonFsExport {
        remote_root_dir_abs: remote_root_dir_abs.to_string(),
        routing_mode: FluxonFsExportRoutingMode::AgentRegistry,
        nodes: Vec::new(),
        cache_kv_key_prefix: export_cache_kv_key_prefix_for_export_name_v1(export_name),
        cache_bytes_field_key: FS_EXPORT_CACHE_BYTES_FIELD_KEY.to_string(),
        cache_max_bytes: FS_EXPORT_DEFAULT_CACHE_MAX_BYTES_V1,
        inline_bytes_max_bytes: FS_EXPORT_DEFAULT_INLINE_BYTES_MAX_BYTES_V1,
        metadata_cache_ttl_ms: FS_EXPORT_DEFAULT_METADATA_CACHE_TTL_MS_V1,
        async_backfill_enabled: true,
        rpc_paths: export_rpc_paths_for_export_name_v1(export_name),
    }
}

#[derive(Debug, Clone)]
pub struct FluxonFsCacheControllerConfig {
    pub stage_queue_capacity: usize,
    pub stage_worker_count: usize,
    pub stats_gc_scan_interval_secs: u64,
    pub stats_gc_max_entry_age_secs: u64,
    pub admission_policy: String,
    pub max_coalesced_piece_count: usize,
}

impl Default for FluxonFsCacheControllerConfig {
    fn default() -> Self {
        Self {
            stage_queue_capacity: 1024,
            stage_worker_count: 4,
            stats_gc_scan_interval_secs: 60,
            stats_gc_max_entry_age_secs: 600,
            admission_policy: "always".to_string(),
            max_coalesced_piece_count: 8,
        }
    }
}

pub fn is_admin_browse_export_name_v1(export_name: &str) -> bool {
    export_name.starts_with(FS_ADMIN_BROWSE_EXPORT_PREFIX_V1)
}

pub fn admin_browse_export_name_for_agent_instance_key_v1(agent_instance_key: &str) -> String {
    let mut normalized = String::new();
    for ch in agent_instance_key.chars() {
        let next = if ch.is_ascii_alphanumeric() {
            ch.to_ascii_lowercase()
        } else {
            '-'
        };
        if next == '-' && normalized.ends_with('-') {
            continue;
        }
        normalized.push(next);
    }
    let normalized = normalized.trim_matches('-');
    let hash = admin_browse_export_suffix_hash_v1(agent_instance_key);
    let mut suffix = String::new();
    if !normalized.is_empty() {
        suffix.push_str(normalized);
        if suffix.len() > 28 {
            suffix.truncate(28);
            suffix = suffix.trim_matches('-').to_string();
        }
        if !suffix.is_empty() {
            suffix.push('-');
        }
    }
    suffix.push_str(&format!("{:08x}", hash));
    format!("{}{}", FS_ADMIN_BROWSE_EXPORT_PREFIX_V1, suffix)
}

pub fn admin_browse_export_for_agent_instance_key_v1(
    agent_instance_key: &str,
) -> (String, FluxonFsExport) {
    let export_name = admin_browse_export_name_for_agent_instance_key_v1(agent_instance_key);
    let export = FluxonFsExport {
        remote_root_dir_abs: "/".to_string(),
        routing_mode: FluxonFsExportRoutingMode::StaticNodes,
        nodes: vec![agent_instance_key.to_string()],
        cache_kv_key_prefix: export_cache_kv_key_prefix_for_export_name_v1(export_name.as_str()),
        cache_bytes_field_key: FS_EXPORT_CACHE_BYTES_FIELD_KEY.to_string(),
        cache_max_bytes: FS_EXPORT_DEFAULT_CACHE_MAX_BYTES_V1,
        inline_bytes_max_bytes: FS_EXPORT_DEFAULT_INLINE_BYTES_MAX_BYTES_V1,
        metadata_cache_ttl_ms: FS_EXPORT_DEFAULT_METADATA_CACHE_TTL_MS_V1,
        async_backfill_enabled: true,
        rpc_paths: export_rpc_paths_for_export_name_v1(export_name.as_str()),
    };
    (export_name, export)
}

fn admin_browse_export_suffix_hash_v1(text: &str) -> u32 {
    let mut hash: u32 = 0x811c9dc5;
    for byte in text.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

const SCOPE_ACCESS_READ_ACTIONS: [FluxonFsS3PermissionAction; 2] = [
    FluxonFsS3PermissionAction::ListBucket,
    FluxonFsS3PermissionAction::GetObject,
];

const SCOPE_ACCESS_READ_WRITE_ACTIONS: [FluxonFsS3PermissionAction; 7] = [
    FluxonFsS3PermissionAction::ListBucket,
    FluxonFsS3PermissionAction::ListBucketMultipartUploads,
    FluxonFsS3PermissionAction::ListMultipartUploadParts,
    FluxonFsS3PermissionAction::GetObject,
    FluxonFsS3PermissionAction::PutObject,
    FluxonFsS3PermissionAction::DeleteObject,
    FluxonFsS3PermissionAction::AbortMultipartUpload,
];

pub fn access_model_to_json_text(model: &FluxonFsAccessModel) -> Result<String, String> {
    serde_json::to_string(model).map_err(|e| format!("serialize access_model failed: {}", e))
}

pub fn parse_access_model_json_text(text: &str) -> Result<FluxonFsAccessModel, String> {
    serde_json::from_str(text).map_err(|e| format!("parse access_model failed: {}", e))
}

pub fn runtime_access_model_to_json_text(
    model: &FluxonFsRuntimeAccessModel,
) -> Result<String, String> {
    serde_json::to_string(model)
        .map_err(|e| format!("serialize runtime_access_model failed: {}", e))
}

pub fn parse_runtime_access_model_json_text(
    text: &str,
) -> Result<FluxonFsRuntimeAccessModel, String> {
    serde_json::from_str(text).map_err(|e| format!("parse runtime_access_model failed: {}", e))
}

pub fn access_model_from_s3_permission_list(
    permission_list: &[FluxonFsS3PermissionAccount],
) -> Result<FluxonFsAccessModel, String> {
    let mut users: Vec<FluxonFsAccessUser> = Vec::new();
    let mut scope_map: BTreeMap<
        (String, String, FluxonFsScopeAccessMode),
        std::collections::BTreeSet<String>,
    > = BTreeMap::new();

    for account in permission_list {
        let mut can_manage_users = false;
        for rule in &account.permissions {
            if permission_rule_is_manage(rule) {
                can_manage_users = true;
                continue;
            }
            let mode = scope_access_mode_from_rule(rule).map_err(|err| {
                format!(
                    "user {} has unsupported internal permission rule: {}",
                    account.username, err
                )
            })?;
            scope_map
                .entry((
                    rule.bucket.clone(),
                    normalize_scope_prefix(&rule.prefix),
                    mode,
                ))
                .or_default()
                .insert(account.username.clone());
        }
        users.push(FluxonFsAccessUser {
            username: account.username.clone(),
            password: account.password.clone(),
            can_manage_users,
        });
    }

    users.sort_by(|a, b| a.username.cmp(&b.username));

    let mut scope_access: Vec<FluxonFsScopeAccess> = Vec::new();
    for ((export_name, prefix, mode), usernames) in scope_map {
        scope_access.push(FluxonFsScopeAccess {
            export_name,
            prefix,
            mode,
            usernames: usernames.into_iter().collect(),
        });
    }

    Ok(FluxonFsAccessModel {
        users,
        scope_access,
    })
}

pub fn runtime_access_model_from_s3_permission_list(
    permission_list: &[FluxonFsS3PermissionAccount],
) -> Result<FluxonFsRuntimeAccessModel, String> {
    let model = access_model_from_s3_permission_list(permission_list)?;
    Ok(FluxonFsRuntimeAccessModel {
        users: model
            .users
            .into_iter()
            .map(|user| FluxonFsRuntimeAccessUser {
                username: user.username,
                can_manage_users: user.can_manage_users,
                rpc_token_secret_sha256_hex: rpc_token_secret_sha256_hex(&user.password),
            })
            .collect(),
        scope_access: model.scope_access,
    })
}

pub fn s3_permission_list_from_access_model(
    model: &FluxonFsAccessModel,
) -> Result<Vec<FluxonFsS3PermissionAccount>, String> {
    let mut known_users: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut permissions_by_user: BTreeMap<String, Vec<FluxonFsS3PermissionRule>> = BTreeMap::new();

    for user in &model.users {
        if !known_users.insert(user.username.clone()) {
            return Err(format!("duplicate user: {}", user.username));
        }
    }

    for scope in &model.scope_access {
        if scope.usernames.is_empty() {
            return Err(format!(
                "scope_access export_name={} prefix={} mode={} must bind at least one user",
                scope.export_name,
                scope.prefix,
                scope.mode.form_value()
            ));
        }
        let mut local_seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for username in &scope.usernames {
            if !known_users.contains(username) {
                return Err(format!(
                    "scope_access export_name={} prefix={} mode={} references unknown user {}",
                    scope.export_name,
                    scope.prefix,
                    scope.mode.form_value(),
                    username
                ));
            }
            if !local_seen.insert(username.clone()) {
                return Err(format!(
                    "scope_access export_name={} prefix={} mode={} duplicates user {}",
                    scope.export_name,
                    scope.prefix,
                    scope.mode.form_value(),
                    username
                ));
            }
            permissions_by_user
                .entry(username.clone())
                .or_default()
                .push(scope_access_rule(scope));
        }
    }

    let mut out: Vec<FluxonFsS3PermissionAccount> = Vec::new();
    for user in &model.users {
        let mut permissions = permissions_by_user
            .remove(&user.username)
            .unwrap_or_default();
        if user.can_manage_users {
            permissions.push(scope_access_manage_rule());
        }
        permissions.sort_by(|a, b| {
            a.bucket
                .cmp(&b.bucket)
                .then(a.prefix.cmp(&b.prefix))
                .then(a.actions.len().cmp(&b.actions.len()))
        });
        out.push(FluxonFsS3PermissionAccount {
            username: user.username.clone(),
            password: user.password.clone(),
            permissions,
        });
    }
    Ok(out)
}

pub fn access_model_find_user<'a>(
    model: &'a FluxonFsAccessModel,
    username: &str,
) -> Option<&'a FluxonFsAccessUser> {
    model.users.iter().find(|user| user.username == username)
}

pub fn runtime_access_model_find_user<'a>(
    model: &'a FluxonFsRuntimeAccessModel,
    username: &str,
) -> Option<&'a FluxonFsRuntimeAccessUser> {
    model.users.iter().find(|user| user.username == username)
}

pub fn access_model_has_bucket_access(
    model: &FluxonFsAccessModel,
    username: &str,
    export_name: &str,
) -> bool {
    if access_model_can_manage_users(model, username) {
        return true;
    }
    model.scope_access.iter().any(|scope| {
        scope.export_name == export_name && scope.usernames.iter().any(|name| name == username)
    })
}

pub fn runtime_access_model_has_bucket_access(
    model: &FluxonFsRuntimeAccessModel,
    username: &str,
    export_name: &str,
) -> bool {
    if runtime_access_model_can_manage_users(model, username) {
        return true;
    }
    model.scope_access.iter().any(|scope| {
        scope.export_name == export_name && scope.usernames.iter().any(|name| name == username)
    })
}

pub fn access_model_has_bucket_write_access(
    model: &FluxonFsAccessModel,
    username: &str,
    export_name: &str,
) -> bool {
    if access_model_can_manage_users(model, username) {
        return true;
    }
    model.scope_access.iter().any(|scope| {
        scope.export_name == export_name
            && scope.mode == FluxonFsScopeAccessMode::ReadWrite
            && scope.usernames.iter().any(|name| name == username)
    })
}

pub fn runtime_access_model_has_bucket_write_access(
    model: &FluxonFsRuntimeAccessModel,
    username: &str,
    export_name: &str,
) -> bool {
    if runtime_access_model_can_manage_users(model, username) {
        return true;
    }
    model.scope_access.iter().any(|scope| {
        scope.export_name == export_name
            && scope.mode == FluxonFsScopeAccessMode::ReadWrite
            && scope.usernames.iter().any(|name| name == username)
    })
}

pub fn access_model_can_manage_users(model: &FluxonFsAccessModel, username: &str) -> bool {
    access_model_find_user(model, username)
        .map(|user| user.can_manage_users)
        .unwrap_or(false)
}

pub fn runtime_access_model_can_manage_users(
    model: &FluxonFsRuntimeAccessModel,
    username: &str,
) -> bool {
    runtime_access_model_find_user(model, username)
        .map(|user| user.can_manage_users)
        .unwrap_or(false)
}

pub fn access_model_allows_path(
    model: &FluxonFsAccessModel,
    username: &str,
    export_name: &str,
    relpath: &str,
    mode: FluxonFsScopeAccessMode,
) -> bool {
    if access_model_can_manage_users(model, username) {
        return true;
    }
    let relpath = normalize_scope_relpath(relpath);
    model.scope_access.iter().any(|scope| {
        scope.export_name == export_name
            && scope_mode_covers(scope.mode, mode)
            && scope.usernames.iter().any(|name| name == username)
            && scope_prefix_covers_path(scope.prefix.as_str(), relpath.as_str())
    })
}

pub fn runtime_access_model_allows_path(
    model: &FluxonFsRuntimeAccessModel,
    username: &str,
    export_name: &str,
    relpath: &str,
    mode: FluxonFsScopeAccessMode,
) -> bool {
    if runtime_access_model_can_manage_users(model, username) {
        return true;
    }
    let relpath = normalize_scope_relpath(relpath);
    model.scope_access.iter().any(|scope| {
        scope.export_name == export_name
            && scope_mode_covers(scope.mode, mode)
            && scope.usernames.iter().any(|name| name == username)
            && scope_prefix_covers_path(scope.prefix.as_str(), relpath.as_str())
    })
}

pub fn access_model_can_browse_dir(
    model: &FluxonFsAccessModel,
    username: &str,
    export_name: &str,
    relpath: &str,
) -> bool {
    if access_model_can_manage_users(model, username) {
        return true;
    }
    let relpath = normalize_scope_relpath(relpath);
    model.scope_access.iter().any(|scope| {
        scope.export_name == export_name
            && scope.usernames.iter().any(|name| name == username)
            && scope_prefix_intersects_dir(scope.prefix.as_str(), relpath.as_str())
    })
}

pub fn runtime_access_model_can_browse_dir(
    model: &FluxonFsRuntimeAccessModel,
    username: &str,
    export_name: &str,
    relpath: &str,
) -> bool {
    if runtime_access_model_can_manage_users(model, username) {
        return true;
    }
    let relpath = normalize_scope_relpath(relpath);
    model.scope_access.iter().any(|scope| {
        scope.export_name == export_name
            && scope.usernames.iter().any(|name| name == username)
            && scope_prefix_intersects_dir(scope.prefix.as_str(), relpath.as_str())
    })
}

pub fn access_model_visible_dir_entry(
    model: &FluxonFsAccessModel,
    username: &str,
    export_name: &str,
    relpath: &str,
    is_dir: bool,
) -> bool {
    if is_dir {
        return access_model_can_browse_dir(model, username, export_name, relpath);
    }
    access_model_allows_path(
        model,
        username,
        export_name,
        relpath,
        FluxonFsScopeAccessMode::Read,
    )
}

pub fn runtime_access_model_visible_dir_entry(
    model: &FluxonFsRuntimeAccessModel,
    username: &str,
    export_name: &str,
    relpath: &str,
    is_dir: bool,
) -> bool {
    if is_dir {
        return runtime_access_model_can_browse_dir(model, username, export_name, relpath);
    }
    runtime_access_model_allows_path(
        model,
        username,
        export_name,
        relpath,
        FluxonFsScopeAccessMode::Read,
    )
}

pub fn access_model_required_mode_for_op(op: FluxonFsOp) -> FluxonFsScopeAccessMode {
    match op {
        FluxonFsOp::Stat
        | FluxonFsOp::Lstat
        | FluxonFsOp::ListDir
        | FluxonFsOp::ReadLink
        | FluxonFsOp::GetXattr
        | FluxonFsOp::ListXattr
        | FluxonFsOp::ReadChunk => FluxonFsScopeAccessMode::Read,
        FluxonFsOp::WriteChunk
        | FluxonFsOp::Truncate
        | FluxonFsOp::Mkdir
        | FluxonFsOp::Mkfifo
        | FluxonFsOp::Mknod
        | FluxonFsOp::Rmdir
        | FluxonFsOp::Unlink
        | FluxonFsOp::Link
        | FluxonFsOp::Symlink
        | FluxonFsOp::Rename
        | FluxonFsOp::Chmod
        | FluxonFsOp::Chown
        | FluxonFsOp::Lchown
        | FluxonFsOp::Utime
        | FluxonFsOp::SetXattr
        | FluxonFsOp::RemoveXattr => FluxonFsScopeAccessMode::ReadWrite,
    }
}

pub fn build_rpc_token(
    identity: &FluxonFsRequestIdentity,
    now_unix_ms: i64,
) -> Result<String, String> {
    if identity.username.trim().is_empty() {
        return Err("request_identity.username must be non-empty".to_string());
    }
    if identity.password.is_empty() {
        return Err("request_identity.password must be non-empty".to_string());
    }
    let expires_unix_ms = now_unix_ms
        .checked_add(FLUXON_FS_RPC_TOKEN_TTL_MS)
        .ok_or_else(|| format!("rpc token ttl overflow: now_unix_ms={}", now_unix_ms))?;
    let claims = FluxonFsRpcTokenClaims {
        username: identity.username.clone(),
        expires_unix_ms,
    };
    let payload = serde_json::to_vec(&claims)
        .map_err(|e| format!("serialize rpc token claims failed: {}", e))?;
    let token_secret = rpc_token_secret_sha256_hex(&identity.password);
    let sig = sign_rpc_token_payload(token_secret.as_bytes(), payload.as_slice())?;
    let payload_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload);
    let sig_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sig);
    Ok(format!("{}.{}", payload_b64, sig_b64))
}

pub fn verify_rpc_token(
    model: &FluxonFsRuntimeAccessModel,
    token: &str,
    now_unix_ms: i64,
) -> Result<FluxonFsRpcTokenClaims, String> {
    let (payload_b64, sig_b64) = token
        .split_once('.')
        .ok_or_else(|| "invalid rpc token format".to_string())?;
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload_b64)
        .map_err(|e| format!("decode rpc token payload failed: {}", e))?;
    let claims: FluxonFsRpcTokenClaims = serde_json::from_slice(payload.as_slice())
        .map_err(|e| format!("parse rpc token claims failed: {}", e))?;
    if claims.username.trim().is_empty() {
        return Err("rpc token username must be non-empty".to_string());
    }
    if claims.expires_unix_ms < now_unix_ms {
        return Err(format!(
            "rpc token expired: username={} expires_unix_ms={} now_unix_ms={}",
            claims.username, claims.expires_unix_ms, now_unix_ms
        ));
    }
    let user = runtime_access_model_find_user(model, &claims.username)
        .ok_or_else(|| format!("rpc token references unknown user: {}", claims.username))?;
    let sig = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(sig_b64)
        .map_err(|e| format!("decode rpc token signature failed: {}", e))?;
    verify_rpc_token_payload(
        user.rpc_token_secret_sha256_hex.as_bytes(),
        payload.as_slice(),
        sig.as_slice(),
    )?;
    Ok(claims)
}

fn rpc_token_secret_sha256_hex(secret: &str) -> String {
    hex::encode(Sha256::digest(secret.as_bytes()))
}

fn permission_rule_is_manage(rule: &FluxonFsS3PermissionRule) -> bool {
    rule.bucket == "*"
        && rule.prefix.is_empty()
        && rule
            .actions
            .iter()
            .any(|v| *v == FluxonFsS3PermissionAction::All)
}

fn permission_rule_has_action(
    rule: &FluxonFsS3PermissionRule,
    action: FluxonFsS3PermissionAction,
) -> bool {
    rule.actions.iter().any(|v| *v == action)
}

fn scope_access_mode_from_rule(
    rule: &FluxonFsS3PermissionRule,
) -> Result<FluxonFsScopeAccessMode, String> {
    if permission_rule_has_action(rule, FluxonFsS3PermissionAction::All) {
        return Ok(FluxonFsScopeAccessMode::ReadWrite);
    }

    let is_read = rule.actions.len() == SCOPE_ACCESS_READ_ACTIONS.len()
        && SCOPE_ACCESS_READ_ACTIONS
            .iter()
            .all(|action| permission_rule_has_action(rule, *action));
    if is_read {
        return Ok(FluxonFsScopeAccessMode::Read);
    }

    let is_read_write = rule.actions.len() == SCOPE_ACCESS_READ_WRITE_ACTIONS.len()
        && SCOPE_ACCESS_READ_WRITE_ACTIONS
            .iter()
            .all(|action| permission_rule_has_action(rule, *action));
    if is_read_write {
        return Ok(FluxonFsScopeAccessMode::ReadWrite);
    }

    Err(format!(
        "rule bucket={} prefix={} cannot be represented as scope_access with mode read or read_write",
        rule.bucket, rule.prefix
    ))
}

fn scope_access_rule(scope: &FluxonFsScopeAccess) -> FluxonFsS3PermissionRule {
    FluxonFsS3PermissionRule {
        bucket: scope.export_name.clone(),
        prefix: normalize_scope_prefix(&scope.prefix),
        actions: match scope.mode {
            FluxonFsScopeAccessMode::Read => SCOPE_ACCESS_READ_ACTIONS.to_vec(),
            FluxonFsScopeAccessMode::ReadWrite => SCOPE_ACCESS_READ_WRITE_ACTIONS.to_vec(),
        },
    }
}

fn scope_access_manage_rule() -> FluxonFsS3PermissionRule {
    FluxonFsS3PermissionRule {
        bucket: "*".to_string(),
        prefix: "".to_string(),
        actions: vec![FluxonFsS3PermissionAction::All],
    }
}

fn scope_mode_covers(granted: FluxonFsScopeAccessMode, required: FluxonFsScopeAccessMode) -> bool {
    granted == FluxonFsScopeAccessMode::ReadWrite || granted == required
}

fn normalize_scope_relpath(relpath: &str) -> String {
    let mut rel = relpath.replace('\\', "/");
    while rel.starts_with('/') {
        rel = rel[1..].to_string();
    }
    let parts: Vec<&str> = rel
        .split('/')
        .filter(|x| !x.is_empty() && *x != ".")
        .collect();
    parts.join("/")
}

fn normalize_scope_prefix(prefix: &str) -> String {
    let rel = normalize_scope_relpath(prefix);
    if rel.is_empty() {
        return String::new();
    }
    format!("{}/", rel.trim_end_matches('/'))
}

fn scope_prefix_covers_path(scope_prefix: &str, relpath: &str) -> bool {
    if scope_prefix.is_empty() {
        return true;
    }
    let scope_root = scope_prefix.trim_end_matches('/');
    relpath == scope_root || relpath.starts_with(scope_prefix)
}

fn scope_prefix_intersects_dir(scope_prefix: &str, relpath: &str) -> bool {
    if scope_prefix.is_empty() {
        return true;
    }
    if relpath.is_empty() {
        return true;
    }
    if scope_prefix_covers_path(scope_prefix, relpath) {
        return true;
    }
    let dir_prefix = format!("{}/", relpath.trim_end_matches('/'));
    scope_prefix.starts_with(dir_prefix.as_str())
}

fn sign_rpc_token_payload(secret: &[u8], payload: &[u8]) -> Result<Vec<u8>, String> {
    let mut mac = HmacSha256::new_from_slice(secret)
        .map_err(|e| format!("init rpc token hmac failed: {}", e))?;
    mac.update(payload);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn verify_rpc_token_payload(secret: &[u8], payload: &[u8], sig: &[u8]) -> Result<(), String> {
    let mut mac = HmacSha256::new_from_slice(secret)
        .map_err(|e| format!("init rpc token hmac failed: {}", e))?;
    mac.update(payload);
    mac.verify_slice(sig)
        .map_err(|_| "rpc token signature mismatch".to_string())
}

#[derive(Debug, Clone)]
pub struct FluxonFsMasterPanelConfig {
    pub listen_addr: String,
    pub public_base_url: String,
    pub prometheus_base_url: String,
    pub auto_refresh_interval_secs: u64,
    pub access_db_path: String,
    pub bootstrap_access_model: FluxonFsAccessModel,
    pub transfer_state_store: Option<FluxonFsTransferStateStoreConfig>,
    pub s3_gateway: FluxonFsS3GatewayConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferStateStoreConfig {
    pub kind: FluxonFsTransferStateStoreKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FluxonFsTransferStateStoreKind {
    #[serde(rename = "tikv")]
    TiKv(FluxonFsTransferStateStoreTiKvConfig),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FluxonFsTransferStateStoreTiKvConfig {
    pub pd_endpoints: Vec<String>,
    pub key_prefix: String,
}

#[derive(thiserror::Error, Debug)]
pub enum FluxonFsMasterConfigError {
    #[error("invalid fluxon_fs.master config: {detail}")]
    Invalid { detail: String },
}

#[derive(thiserror::Error, Debug)]
pub enum FluxonFsMasterPanelConfigError {
    #[error("invalid fluxon_fs.master_panel config: {detail}")]
    Invalid { detail: String },
}

#[derive(thiserror::Error, Debug)]
pub enum FluxonFsCacheConfigExtractError {
    #[error("invalid fluxon_fs.cache config: {detail}")]
    Invalid { detail: String },
}

fn master_invalid(detail: impl Into<String>) -> FluxonFsMasterConfigError {
    FluxonFsMasterConfigError::Invalid {
        detail: detail.into(),
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct MasterYaml {
    instance_key: String,
    pull_interval_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct S3PermissionAccountYaml {
    username: String,
    password: String,
    permissions: Vec<S3PermissionRuleYaml>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct S3PermissionRuleYaml {
    bucket: String,
    prefix: String,
    actions: Vec<String>,
}

pub fn parse_master_config_from_file(
    path: &str,
) -> Result<FluxonFsMasterConfig, FluxonFsMasterConfigError> {
    let text = fs::read_to_string(path)
        .map_err(|e| master_invalid(format!("read config file failed: {}", e)))?;
    parse_master_config_from_yaml_text(&text)
}

pub fn parse_master_config_from_yaml_text(
    text: &str,
) -> Result<FluxonFsMasterConfig, FluxonFsMasterConfigError> {
    let root: serde_yaml::Value = serde_yaml::from_str(text).map_err(|e| {
        // English note: serde_yaml errors usually do not include the original document; include it for debugging.
        master_invalid(format!(
            "yaml parse failed: {}\n--- YAML BEGIN ---\n{}\n--- YAML END ---",
            e, text
        ))
    })?;

    // The Fluxon config file is a top-level mapping like:
    //   kvclient: ...
    //   fluxon_fs:
    //     master: ...
    //     cache: ...
    // There is no extra top-level `config:` wrapper.
    let top = require_master_mapping(&root, "config file")?;
    let fs_v = require_master_key(top, "fluxon_fs", "fluxon_fs")?;
    let fs = require_master_mapping(fs_v, "fluxon_fs")?;
    if fs.contains_key(&serde_yaml::Value::String("rpc".to_string())) {
        return Err(master_invalid(
            "fluxon_fs.rpc is removed; use fluxon_fs.master",
        ));
    }
    let master_v = require_master_key(fs, "master", "fluxon_fs.master")?;

    let master_map = require_master_mapping(master_v, "fluxon_fs.master")?;
    if master_map.contains_key(&serde_yaml::Value::String("rpc_timeout_ms".to_string())) {
        return Err(master_invalid(
            "fluxon_fs.master.rpc_timeout_ms is removed; Fluxon user-RPC timeout defaults to 10000ms per call",
        ));
    }

    let master_yaml: MasterYaml = serde_yaml::from_value(master_v.clone())
        .map_err(|e| master_invalid(format!("parse config.fluxon_fs.master failed: {}", e)))?;

    if master_yaml.instance_key.trim().is_empty() {
        return Err(master_invalid(
            "config.fluxon_fs.master.instance_key must be non-empty",
        ));
    }
    if matches!(master_yaml.pull_interval_ms, Some(0)) {
        return Err(master_invalid(
            "config.fluxon_fs.master.pull_interval_ms must be > 0",
        ));
    }

    Ok(FluxonFsMasterConfig {
        instance_key: master_yaml.instance_key,
        pull_interval_ms: master_yaml.pull_interval_ms,
    })
}

fn require_master_mapping<'a>(
    v: &'a serde_yaml::Value,
    name: &str,
) -> Result<&'a serde_yaml::Mapping, FluxonFsMasterConfigError> {
    match v {
        serde_yaml::Value::Mapping(m) => Ok(m),
        _ => Err(master_invalid(format!("{} must be a mapping", name))),
    }
}

fn require_master_key<'a>(
    m: &'a serde_yaml::Mapping,
    key: &str,
    name: &str,
) -> Result<&'a serde_yaml::Value, FluxonFsMasterConfigError> {
    let k = serde_yaml::Value::String(key.to_string());
    m.get(&k)
        .ok_or_else(|| master_invalid(format!("{} is required", name)))
}

fn panel_invalid(detail: impl Into<String>) -> FluxonFsMasterPanelConfigError {
    FluxonFsMasterPanelConfigError::Invalid {
        detail: detail.into(),
    }
}

pub fn parse_master_panel_config_from_file(
    path: &str,
) -> Result<FluxonFsMasterPanelConfig, FluxonFsMasterPanelConfigError> {
    let text = fs::read_to_string(path)
        .map_err(|e| panel_invalid(format!("read config file failed: {}", e)))?;
    parse_master_panel_config_from_yaml_text(&text)
}

pub fn parse_master_panel_config_from_yaml_text(
    text: &str,
) -> Result<FluxonFsMasterPanelConfig, FluxonFsMasterPanelConfigError> {
    let root: serde_yaml::Value = serde_yaml::from_str(text).map_err(|e| {
        // English note: serde_yaml errors usually do not include the original document; include it for debugging.
        panel_invalid(format!(
            "yaml parse failed: {}\n--- YAML BEGIN ---\n{}\n--- YAML END ---",
            e, text
        ))
    })?;

    let top = match &root {
        serde_yaml::Value::Mapping(m) => m,
        _ => return Err(panel_invalid("config file must be a mapping")),
    };
    let fs_v = top
        .get(&serde_yaml::Value::String("fluxon_fs".to_string()))
        .ok_or_else(|| panel_invalid("fluxon_fs is required"))?;
    let fs = match fs_v {
        serde_yaml::Value::Mapping(m) => m,
        _ => return Err(panel_invalid("fluxon_fs must be a mapping")),
    };
    let panel_v = fs
        .get(&serde_yaml::Value::String("master_panel".to_string()))
        .ok_or_else(|| panel_invalid("fluxon_fs.master_panel is required"))?;
    let panel_map = match panel_v {
        serde_yaml::Value::Mapping(m) => m,
        _ => return Err(panel_invalid("fluxon_fs.master_panel must be a mapping")),
    };

    let listen_addr = panel_map
        .get(&serde_yaml::Value::String("listen_addr".to_string()))
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            panel_invalid("fluxon_fs.master_panel.listen_addr must be non-empty string")
        })?;
    if !listen_addr.contains(':') {
        return Err(panel_invalid(
            "fluxon_fs.master_panel.listen_addr must be 'host:port'",
        ));
    }

    let mut public_base_url = panel_map
        .get(&serde_yaml::Value::String("public_base_url".to_string()))
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            panel_invalid("fluxon_fs.master_panel.public_base_url must be non-empty string")
        })?;
    if !public_base_url.contains("://") {
        return Err(panel_invalid(
            "fluxon_fs.master_panel.public_base_url must include scheme (http(s)://..)",
        ));
    }
    while public_base_url.ends_with('/') {
        public_base_url.pop();
    }

    let mut prometheus_base_url = panel_map
        .get(&serde_yaml::Value::String(
            "prometheus_base_url".to_string(),
        ))
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            panel_invalid("fluxon_fs.master_panel.prometheus_base_url must be non-empty string")
        })?;
    if !(prometheus_base_url.starts_with("http://") || prometheus_base_url.starts_with("https://"))
    {
        return Err(panel_invalid(
            "fluxon_fs.master_panel.prometheus_base_url must include scheme (http(s)://..)",
        ));
    }
    while prometheus_base_url.ends_with('/') {
        prometheus_base_url.pop();
    }
    let without_scheme = &prometheus_base_url[prometheus_base_url.find("://").unwrap() + 3..];
    let (netloc, path) = match without_scheme.find('/') {
        Some(idx) => (&without_scheme[..idx], &without_scheme[idx..]),
        None => (without_scheme, ""),
    };
    if path.is_empty() || path == "/" {
        return Err(panel_invalid(
            "fluxon_fs.master_panel.prometheus_base_url must include a query path like /v1/prometheus or /api/v1",
        ));
    }
    if !(path.starts_with("/api/v1") || path.starts_with("/v1")) {
        return Err(panel_invalid(
            "fluxon_fs.master_panel.prometheus_base_url path must start with /v1 or /api/v1",
        ));
    }
    let port_ok = if netloc.starts_with('[') {
        if let Some(end) = netloc.find("]:") {
            netloc[end + 2..].parse::<u16>().is_ok()
        } else {
            false
        }
    } else {
        netloc
            .rsplit_once(':')
            .map(|(_, p)| p.parse::<u16>().is_ok())
            .unwrap_or(false)
    };
    if !port_ok {
        return Err(panel_invalid(
            "fluxon_fs.master_panel.prometheus_base_url must include an explicit port",
        ));
    }

    let auto_refresh_interval_secs = panel_map
        .get(&serde_yaml::Value::String(
            "auto_refresh_interval_secs".to_string(),
        ))
        .and_then(|v| v.as_i64())
        .ok_or_else(|| {
            panel_invalid("fluxon_fs.master_panel.auto_refresh_interval_secs must be int")
        })?;
    if auto_refresh_interval_secs <= 0 {
        return Err(panel_invalid(
            "fluxon_fs.master_panel.auto_refresh_interval_secs must be > 0",
        ));
    }

    let access_db_path = panel_map
        .get(&serde_yaml::Value::String("access_db_path".to_string()))
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            panel_invalid("fluxon_fs.master_panel.access_db_path must be non-empty string")
        })?;
    let bootstrap_access_model = parse_bootstrap_access_model_config(panel_map)?;
    let transfer_state_store = parse_optional_transfer_state_store_config(panel_map)?;

    let s3_v = panel_map
        .get(&serde_yaml::Value::String("s3_gateway".to_string()))
        .ok_or_else(|| panel_invalid("fluxon_fs.master_panel.s3_gateway is required"))?;
    let s3_map = match s3_v {
        serde_yaml::Value::Mapping(m) => m,
        _ => {
            return Err(panel_invalid(
                "fluxon_fs.master_panel.s3_gateway must be a mapping",
            ));
        }
    };

    let inflight_pieces = s3_map
        .get(&serde_yaml::Value::String(
            "get_object_inflight_pieces".to_string(),
        ))
        .and_then(|v| v.as_i64())
        .ok_or_else(|| {
            panel_invalid(
                "fluxon_fs.master_panel.s3_gateway.get_object_inflight_pieces must be int",
            )
        })?;
    if inflight_pieces <= 0 {
        return Err(panel_invalid(
            "fluxon_fs.master_panel.s3_gateway.get_object_inflight_pieces must be > 0",
        ));
    }

    let kv_miss_policy_s = s3_map
        .get(&serde_yaml::Value::String("kv_miss_policy".to_string()))
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            panel_invalid(
                "fluxon_fs.master_panel.s3_gateway.kv_miss_policy must be non-empty string",
            )
        })?;
    let kv_miss_policy = parse_s3_kv_miss_policy(&kv_miss_policy_s).ok_or_else(|| {
        panel_invalid(format!(
            "fluxon_fs.master_panel.s3_gateway.kv_miss_policy invalid: {}",
            kv_miss_policy_s
        ))
    })?;

    Ok(FluxonFsMasterPanelConfig {
        listen_addr,
        public_base_url,
        prometheus_base_url,
        auto_refresh_interval_secs: auto_refresh_interval_secs as u64,
        access_db_path,
        bootstrap_access_model,
        transfer_state_store,
        s3_gateway: FluxonFsS3GatewayConfig {
            get_object_inflight_pieces: inflight_pieces as u64,
            kv_miss_policy,
        },
    })
}

fn parse_optional_transfer_state_store_config(
    panel_map: &serde_yaml::Mapping,
) -> Result<Option<FluxonFsTransferStateStoreConfig>, FluxonFsMasterPanelConfigError> {
    let Some(transfer_state_store_v) = panel_map.get(&serde_yaml::Value::String(
        "transfer_state_store".to_string(),
    )) else {
        return Ok(None);
    };
    let transfer_state_store_map = match transfer_state_store_v {
        serde_yaml::Value::Mapping(v) => v,
        _ => {
            return Err(panel_invalid(
                "fluxon_fs.master_panel.transfer_state_store must be a mapping",
            ));
        }
    };
    let kind_raw = transfer_state_store_map
        .get(&serde_yaml::Value::String("kind".to_string()))
        .and_then(|v| v.as_str())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "tikv".to_string());
    if kind_raw != "tikv" {
        return Err(panel_invalid(format!(
            "fluxon_fs.master_panel.transfer_state_store.kind invalid: {}",
            kind_raw
        )));
    }
    parse_transfer_state_store_tikv_config(transfer_state_store_map).map(Some)
}

fn parse_bootstrap_access_model_config(
    panel_map: &serde_yaml::Mapping,
) -> Result<FluxonFsAccessModel, FluxonFsMasterPanelConfigError> {
    let raw = panel_map
        .get(&serde_yaml::Value::String(
            "bootstrap_access_model".to_string(),
        ))
        .ok_or_else(|| {
            panel_invalid("fluxon_fs.master_panel.bootstrap_access_model is required")
        })?;
    let model: FluxonFsAccessModel = serde_yaml::from_value(raw.clone()).map_err(|e| {
        panel_invalid(format!(
            "fluxon_fs.master_panel.bootstrap_access_model invalid: {}",
            e
        ))
    })?;
    let permission_list = s3_permission_list_from_access_model(&model)
        .map_err(|e| panel_invalid(format!("bootstrap_access_model invalid: {}", e)))?;
    let normalized = access_model_from_s3_permission_list(&permission_list)
        .map_err(|e| panel_invalid(format!("bootstrap_access_model invalid: {}", e)))?;
    Ok(normalized)
}

fn parse_transfer_state_store_tikv_config(
    transfer_state_store_map: &serde_yaml::Mapping,
) -> Result<FluxonFsTransferStateStoreConfig, FluxonFsMasterPanelConfigError> {
    let tikv_v = transfer_state_store_map
        .get(&serde_yaml::Value::String("tikv".to_string()))
        .ok_or_else(|| {
            panel_invalid("fluxon_fs.master_panel.transfer_state_store.tikv is required")
        })?;
    let tikv_map = match tikv_v {
        serde_yaml::Value::Mapping(v) => v,
        _ => {
            return Err(panel_invalid(
                "fluxon_fs.master_panel.transfer_state_store.tikv must be a mapping",
            ));
        }
    };
    let pd_endpoints_v = tikv_map
        .get(&serde_yaml::Value::String("pd_endpoints".to_string()))
        .ok_or_else(|| {
            panel_invalid(
                "fluxon_fs.master_panel.transfer_state_store.tikv.pd_endpoints is required",
            )
        })?;
    let pd_endpoints_seq = match pd_endpoints_v {
        serde_yaml::Value::Sequence(v) => v,
        _ => {
            return Err(panel_invalid(
                "fluxon_fs.master_panel.transfer_state_store.tikv.pd_endpoints must be a list",
            ));
        }
    };
    if pd_endpoints_seq.is_empty() {
        return Err(panel_invalid(
            "fluxon_fs.master_panel.transfer_state_store.tikv.pd_endpoints must be non-empty list",
        ));
    }
    let mut pd_endpoints = Vec::with_capacity(pd_endpoints_seq.len());
    for (idx, value) in pd_endpoints_seq.iter().enumerate() {
        let endpoint = value
            .as_str()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                panel_invalid(format!(
                    "fluxon_fs.master_panel.transfer_state_store.tikv.pd_endpoints[{}] must be non-empty string",
                    idx
                ))
            })?;
        pd_endpoints.push(endpoint);
    }
    let key_prefix = tikv_map
        .get(&serde_yaml::Value::String("key_prefix".to_string()))
        .and_then(|v| v.as_str())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            panel_invalid(
                "fluxon_fs.master_panel.transfer_state_store.tikv.key_prefix must be non-empty string",
            )
        })?;
    if !key_prefix.starts_with('/') || !key_prefix.ends_with('/') {
        return Err(panel_invalid(
            "fluxon_fs.master_panel.transfer_state_store.tikv.key_prefix must start with '/' and end with '/'",
        ));
    }
    Ok(FluxonFsTransferStateStoreConfig {
        kind: FluxonFsTransferStateStoreKind::TiKv(FluxonFsTransferStateStoreTiKvConfig {
            pd_endpoints,
            key_prefix,
        }),
    })
}

pub fn parse_s3_permission_list_yaml_text(
    text: &str,
) -> Result<Vec<FluxonFsS3PermissionAccount>, FluxonFsMasterPanelConfigError> {
    let raw: serde_yaml::Value = serde_yaml::from_str(text)
        .map_err(|e| panel_invalid(format!("permission_list yaml parse failed: {}", e)))?;
    parse_s3_permission_list_value(&raw, "permission_list")
}

fn parse_s3_permission_list_value(
    raw: &serde_yaml::Value,
    path: &str,
) -> Result<Vec<FluxonFsS3PermissionAccount>, FluxonFsMasterPanelConfigError> {
    let accounts_yaml: Vec<S3PermissionAccountYaml> = serde_yaml::from_value(raw.clone())
        .map_err(|e| panel_invalid(format!("{} invalid: {}", path, e)))?;

    let mut out: Vec<FluxonFsS3PermissionAccount> = Vec::new();
    for (account_idx, account) in accounts_yaml.into_iter().enumerate() {
        let username = validate_s3_permission_username(&account.username, path, account_idx)?;
        let password = validate_s3_permission_password(&account.password, path, account_idx)?;
        if out.iter().any(|v| v.username == username) {
            return Err(panel_invalid(format!(
                "{}[{}].username duplicates existing username: {}",
                path, account_idx, username
            )));
        }
        let mut permissions: Vec<FluxonFsS3PermissionRule> = Vec::new();
        for (rule_idx, rule) in account.permissions.into_iter().enumerate() {
            let bucket = validate_s3_permission_bucket(&rule.bucket, path, account_idx, rule_idx)?;
            let prefix = validate_s3_permission_prefix(&rule.prefix, path, account_idx, rule_idx)?;
            if rule.actions.is_empty() {
                return Err(panel_invalid(format!(
                    "{}[{}].permissions[{}].actions must be non-empty",
                    path, account_idx, rule_idx
                )));
            }
            let mut actions: Vec<FluxonFsS3PermissionAction> = Vec::new();
            for (action_idx, action_s) in rule.actions.into_iter().enumerate() {
                let action = parse_s3_permission_action(&action_s).ok_or_else(|| {
                    panel_invalid(format!(
                        "{}[{}].permissions[{}].actions[{}] invalid: {}",
                        path, account_idx, rule_idx, action_idx, action_s
                    ))
                })?;
                if actions.contains(&action) {
                    return Err(panel_invalid(format!(
                        "{}[{}].permissions[{}].actions[{}] duplicates existing action: {}",
                        path,
                        account_idx,
                        rule_idx,
                        action_idx,
                        action.as_config_str()
                    )));
                }
                actions.push(action);
            }
            permissions.push(FluxonFsS3PermissionRule {
                bucket,
                prefix,
                actions,
            });
        }

        out.push(FluxonFsS3PermissionAccount {
            username,
            password,
            permissions,
        });
    }
    Ok(out)
}

fn parse_s3_kv_miss_policy(s: &str) -> Option<FluxonFsS3KvMissPolicy> {
    match s.trim().to_ascii_lowercase().as_str() {
        "remote_read" => Some(FluxonFsS3KvMissPolicy::RemoteRead),
        "stage_to_kv_then_read" => Some(FluxonFsS3KvMissPolicy::StageToKvThenRead),
        _ => None,
    }
}

fn parse_s3_permission_action(s: &str) -> Option<FluxonFsS3PermissionAction> {
    match s.trim() {
        "s3:*" => Some(FluxonFsS3PermissionAction::All),
        "s3:ListBucket" => Some(FluxonFsS3PermissionAction::ListBucket),
        "s3:ListBucketMultipartUploads" => {
            Some(FluxonFsS3PermissionAction::ListBucketMultipartUploads)
        }
        "s3:ListMultipartUploadParts" => Some(FluxonFsS3PermissionAction::ListMultipartUploadParts),
        "s3:GetObject" => Some(FluxonFsS3PermissionAction::GetObject),
        "s3:PutObject" => Some(FluxonFsS3PermissionAction::PutObject),
        "s3:DeleteObject" => Some(FluxonFsS3PermissionAction::DeleteObject),
        "s3:AbortMultipartUpload" => Some(FluxonFsS3PermissionAction::AbortMultipartUpload),
        _ => None,
    }
}

fn validate_s3_permission_username(
    username: &str,
    path: &str,
    account_idx: usize,
) -> Result<String, FluxonFsMasterPanelConfigError> {
    if username.trim().is_empty() {
        return Err(panel_invalid(format!(
            "{}[{}].username must be non-empty",
            path, account_idx
        )));
    }
    if username != username.trim() {
        return Err(panel_invalid(format!(
            "{}[{}].username must not have leading/trailing whitespace",
            path, account_idx
        )));
    }
    if username.contains(':') {
        return Err(panel_invalid(format!(
            "{}[{}].username must not contain ':' because Basic auth uses username:password",
            path, account_idx
        )));
    }
    Ok(username.to_string())
}

fn validate_s3_permission_password(
    password: &str,
    path: &str,
    account_idx: usize,
) -> Result<String, FluxonFsMasterPanelConfigError> {
    if password.is_empty() {
        return Err(panel_invalid(format!(
            "{}[{}].password must be non-empty",
            path, account_idx
        )));
    }
    if password != password.trim() {
        return Err(panel_invalid(format!(
            "{}[{}].password must not have leading/trailing whitespace",
            path, account_idx
        )));
    }
    Ok(password.to_string())
}

fn validate_s3_permission_bucket(
    bucket: &str,
    path: &str,
    account_idx: usize,
    rule_idx: usize,
) -> Result<String, FluxonFsMasterPanelConfigError> {
    if bucket.trim().is_empty() {
        return Err(panel_invalid(format!(
            "{}[{}].permissions[{}].bucket must be non-empty",
            path, account_idx, rule_idx
        )));
    }
    if bucket != bucket.trim() {
        return Err(panel_invalid(format!(
            "{}[{}].permissions[{}].bucket must not have leading/trailing whitespace",
            path, account_idx, rule_idx
        )));
    }
    Ok(bucket.to_string())
}

fn validate_s3_permission_prefix(
    prefix: &str,
    path: &str,
    account_idx: usize,
    rule_idx: usize,
) -> Result<String, FluxonFsMasterPanelConfigError> {
    if prefix != prefix.trim() {
        return Err(panel_invalid(format!(
            "{}[{}].permissions[{}].prefix must not have leading/trailing whitespace",
            path, account_idx, rule_idx
        )));
    }
    if prefix.starts_with('/') {
        return Err(panel_invalid(format!(
            "{}[{}].permissions[{}].prefix must not start with '/'",
            path, account_idx, rule_idx
        )));
    }
    if !prefix.is_empty() && !prefix.ends_with('/') {
        return Err(panel_invalid(format!(
            "{}[{}].permissions[{}].prefix must be empty or end with '/'",
            path, account_idx, rule_idx
        )));
    }
    Ok(prefix.to_string())
}

fn cache_extract_invalid(detail: impl Into<String>) -> FluxonFsCacheConfigExtractError {
    FluxonFsCacheConfigExtractError::Invalid {
        detail: detail.into(),
    }
}

pub fn extract_cache_config_yaml_from_file(
    path: &str,
) -> Result<String, FluxonFsCacheConfigExtractError> {
    let text = fs::read_to_string(path)
        .map_err(|e| cache_extract_invalid(format!("read config file failed: {}", e)))?;
    extract_cache_config_yaml_from_yaml_text(&text)
}

pub fn extract_cache_config_yaml_from_yaml_text(
    text: &str,
) -> Result<String, FluxonFsCacheConfigExtractError> {
    let root: serde_yaml::Value = serde_yaml::from_str(text).map_err(|e| {
        // English note: serde_yaml errors usually do not include the original document; include it for debugging.
        cache_extract_invalid(format!(
            "yaml parse failed: {}\n--- YAML BEGIN ---\n{}\n--- YAML END ---",
            e, text
        ))
    })?;

    let top = match &root {
        serde_yaml::Value::Mapping(m) => m,
        _ => return Err(cache_extract_invalid("config file must be a mapping")),
    };
    let fs_v = top
        .get(&serde_yaml::Value::String("fluxon_fs".to_string()))
        .ok_or_else(|| cache_extract_invalid("fluxon_fs is required"))?;
    let fs = match fs_v {
        serde_yaml::Value::Mapping(m) => m,
        _ => return Err(cache_extract_invalid("fluxon_fs must be a mapping")),
    };
    let cache_v = fs
        .get(&serde_yaml::Value::String("cache".to_string()))
        .ok_or_else(|| cache_extract_invalid("fluxon_fs.cache is required"))?;
    let cache_map = match cache_v {
        serde_yaml::Value::Mapping(m) => m,
        _ => return Err(cache_extract_invalid("fluxon_fs.cache must be a mapping")),
    };

    // English note: we serialize only the `fluxon_fs.cache` subtree so callers can feed it into
    // `parse_cache_config_yaml` (which expects a cache-only YAML document).
    let v = serde_yaml::Value::Mapping(cache_map.clone());
    serde_yaml::to_string(&v).map_err(|e| cache_extract_invalid(format!("yaml dump failed: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_master_panel_yaml(extra: &str) -> String {
        // English note: keep this minimal and explicit; tests should validate that
        // `s3_gateway` does not have hidden defaults.
        format!(
            r#"
fluxon_fs:
  master_panel:
    listen_addr: "127.0.0.1:9999"
    public_base_url: "http://127.0.0.1:9999"
    prometheus_base_url: "http://127.0.0.1:4000/v1/prometheus"
    auto_refresh_interval_secs: 3
    access_db_path: "./fluxon_fs_master_access.db"
    bootstrap_access_model:
      users:
        - username: "admin"
          password: "admin-pass-123"
          can_manage_users: true
      scope_access: []
    transfer_state_store:
      tikv:
        pd_endpoints:
          - "127.0.0.1:2379"
        key_prefix: "/fluxon_fs_transfer/"
{extra}
"#
        )
    }

    #[test]
    fn test_master_panel_requires_s3_gateway_mapping() {
        let text = minimal_master_panel_yaml("");
        let err = parse_master_panel_config_from_yaml_text(&text).unwrap_err();
        assert!(
            err.to_string()
                .contains("fluxon_fs.master_panel.s3_gateway is required")
        );
    }

    #[test]
    fn test_master_panel_requires_inflight_pieces_gt_zero() {
        let text = minimal_master_panel_yaml(
            r#"
    s3_gateway:
      get_object_inflight_pieces: 0
      kv_miss_policy: "remote_read"
"#,
        );
        let err = parse_master_panel_config_from_yaml_text(&text).unwrap_err();
        assert!(
            err.to_string().contains(
                "fluxon_fs.master_panel.s3_gateway.get_object_inflight_pieces must be > 0"
            )
        );
    }

    #[test]
    fn test_master_panel_requires_valid_kv_miss_policy() {
        let text = minimal_master_panel_yaml(
            r#"
    s3_gateway:
      get_object_inflight_pieces: 4
      kv_miss_policy: "no_such_policy"
"#,
        );
        let err = parse_master_panel_config_from_yaml_text(&text).unwrap_err();
        assert!(
            err.to_string()
                .contains("fluxon_fs.master_panel.s3_gateway.kv_miss_policy invalid")
        );
    }

    #[test]
    fn test_master_panel_requires_access_db_path() {
        let text = r#"
fluxon_fs:
  master_panel:
    listen_addr: "127.0.0.1:9999"
    public_base_url: "http://127.0.0.1:9999"
    prometheus_base_url: "http://127.0.0.1:4000/v1/prometheus"
    auto_refresh_interval_secs: 3
    bootstrap_access_model:
      users:
        - username: "admin"
          password: "admin-pass-123"
          can_manage_users: true
      scope_access: []
    transfer_state_store:
      tikv:
        pd_endpoints:
          - "127.0.0.1:2379"
        key_prefix: "/fluxon_fs_transfer/"
    s3_gateway:
      get_object_inflight_pieces: 4
      kv_miss_policy: "remote_read"
"#;
        let err = parse_master_panel_config_from_yaml_text(text).unwrap_err();
        assert!(
            err.to_string()
                .contains("fluxon_fs.master_panel.access_db_path must be non-empty string")
        );
    }

    #[test]
    fn test_master_panel_requires_prometheus_base_url() {
        let text = r#"
fluxon_fs:
  master_panel:
    listen_addr: "127.0.0.1:9999"
    public_base_url: "http://127.0.0.1:9999"
    auto_refresh_interval_secs: 3
    access_db_path: "./fluxon_fs_master_access.db"
    bootstrap_access_model:
      users:
        - username: "admin"
          password: "admin-pass-123"
          can_manage_users: true
      scope_access: []
    s3_gateway:
      get_object_inflight_pieces: 4
      kv_miss_policy: "remote_read"
"#;
        let err = parse_master_panel_config_from_yaml_text(text).unwrap_err();
        assert!(
            err.to_string()
                .contains("fluxon_fs.master_panel.prometheus_base_url must be non-empty string")
        );
    }

    #[test]
    fn test_master_panel_parses_default_tikv_transfer_state_store() {
        let text = minimal_master_panel_yaml(
            r#"
    s3_gateway:
      get_object_inflight_pieces: 4
      kv_miss_policy: "remote_read"
"#,
        );
        let cfg = parse_master_panel_config_from_yaml_text(&text).unwrap();
        assert_eq!(cfg.access_db_path, "./fluxon_fs_master_access.db");
        assert_eq!(
            cfg.prometheus_base_url,
            "http://127.0.0.1:4000/v1/prometheus"
        );
        match cfg.transfer_state_store.unwrap().kind {
            FluxonFsTransferStateStoreKind::TiKv(tikv) => {
                assert_eq!(tikv.pd_endpoints, vec!["127.0.0.1:2379".to_string()]);
                assert_eq!(tikv.key_prefix, "/fluxon_fs_transfer/");
            }
        }
    }

    #[test]
    fn test_master_panel_rejects_empty_access_db_path() {
        let text = r#"
fluxon_fs:
  master_panel:
    listen_addr: "127.0.0.1:9999"
    public_base_url: "http://127.0.0.1:9999"
    prometheus_base_url: "http://127.0.0.1:4000/v1/prometheus"
    auto_refresh_interval_secs: 3
    access_db_path: "   "
    bootstrap_access_model:
      users:
        - username: "admin"
          password: "admin-pass-123"
          can_manage_users: true
      scope_access: []
    transfer_state_store:
      tikv:
        pd_endpoints:
          - "127.0.0.1:2379"
        key_prefix: "/fluxon_fs_transfer/"
    s3_gateway:
      get_object_inflight_pieces: 4
      kv_miss_policy: "remote_read"
"#;
        let err = parse_master_panel_config_from_yaml_text(&text).unwrap_err();
        assert!(
            err.to_string()
                .contains("fluxon_fs.master_panel.access_db_path must be non-empty string")
        );
    }

    #[test]
    fn test_master_panel_rejects_sqlite_transfer_state_store_kind() {
        let text = r#"
fluxon_fs:
  master_panel:
    listen_addr: "127.0.0.1:9999"
    public_base_url: "http://127.0.0.1:9999"
    prometheus_base_url: "http://127.0.0.1:4000/v1/prometheus"
    auto_refresh_interval_secs: 3
    access_db_path: "./fluxon_fs_master_access.db"
    bootstrap_access_model:
      users:
        - username: "admin"
          password: "admin-pass-123"
          can_manage_users: true
      scope_access: []
    transfer_state_store:
      kind: "sqlite"
      tikv:
        pd_endpoints:
          - "127.0.0.1:2379"
        key_prefix: "/fluxon_fs_transfer/"
    s3_gateway:
      get_object_inflight_pieces: 4
      kv_miss_policy: "remote_read"
"#;
        let err = parse_master_panel_config_from_yaml_text(text).unwrap_err();
        assert!(
            err.to_string()
                .contains("fluxon_fs.master_panel.transfer_state_store.kind invalid: sqlite")
        );
    }

    #[test]
    fn test_master_panel_requires_transfer_state_store() {
        let text = r#"
fluxon_fs:
  master_panel:
    listen_addr: "127.0.0.1:9999"
    public_base_url: "http://127.0.0.1:9999"
    prometheus_base_url: "http://127.0.0.1:4000/v1/prometheus"
    auto_refresh_interval_secs: 3
    access_db_path: "./fluxon_fs_master_access.db"
    bootstrap_access_model:
      users:
        - username: "admin"
          password: "admin-pass-123"
          can_manage_users: true
      scope_access: []
    s3_gateway:
      get_object_inflight_pieces: 4
      kv_miss_policy: "remote_read"
"#;
        let cfg = parse_master_panel_config_from_yaml_text(text).unwrap();
        assert!(cfg.transfer_state_store.is_none());
    }

    #[test]
    fn test_master_panel_requires_bootstrap_access_model() {
        let text = r#"
fluxon_fs:
  master_panel:
    listen_addr: "127.0.0.1:9999"
    public_base_url: "http://127.0.0.1:9999"
    prometheus_base_url: "http://127.0.0.1:4000/v1/prometheus"
    auto_refresh_interval_secs: 3
    access_db_path: "./fluxon_fs_master_access.db"
    s3_gateway:
      get_object_inflight_pieces: 4
      kv_miss_policy: "remote_read"
"#;
        let err = parse_master_panel_config_from_yaml_text(text).unwrap_err();
        assert!(
            err.to_string()
                .contains("fluxon_fs.master_panel.bootstrap_access_model is required")
        );
    }

    #[test]
    fn test_runtime_access_model_hashes_password() {
        let runtime_model =
            runtime_access_model_from_s3_permission_list(&[FluxonFsS3PermissionAccount {
                username: "alice".to_string(),
                password: "pw-1".to_string(),
                permissions: vec![FluxonFsS3PermissionRule {
                    bucket: "bucket-a".to_string(),
                    prefix: "reports/".to_string(),
                    actions: SCOPE_ACCESS_READ_ACTIONS.to_vec(),
                }],
            }])
            .unwrap();
        assert_eq!(runtime_model.users.len(), 1);
        assert_eq!(runtime_model.users[0].username, "alice");
        assert!(!runtime_model.users[0].can_manage_users);
        assert_ne!(runtime_model.users[0].rpc_token_secret_sha256_hex, "pw-1");
        assert_eq!(
            runtime_model.users[0].rpc_token_secret_sha256_hex,
            "86cc7dcbef5e93f7bc9dd37bf84e7c5e368b4d8315b9e7125ce8a140e2f5cff3"
        );
    }

    #[test]
    fn test_runtime_access_model_preserves_can_manage_users() {
        let runtime_model =
            runtime_access_model_from_s3_permission_list(&[FluxonFsS3PermissionAccount {
                username: "admin".to_string(),
                password: "admin_pw".to_string(),
                permissions: vec![scope_access_manage_rule()],
            }])
            .unwrap();
        assert_eq!(runtime_model.users.len(), 1);
        assert_eq!(runtime_model.users[0].username, "admin");
        assert!(runtime_model.users[0].can_manage_users);
    }

    #[test]
    fn test_rpc_token_verifies_against_runtime_access_model_hash() {
        let runtime_model =
            runtime_access_model_from_s3_permission_list(&[FluxonFsS3PermissionAccount {
                username: "alice".to_string(),
                password: "pw-1".to_string(),
                permissions: vec![FluxonFsS3PermissionRule {
                    bucket: "bucket-a".to_string(),
                    prefix: "reports/".to_string(),
                    actions: SCOPE_ACCESS_READ_ACTIONS.to_vec(),
                }],
            }])
            .unwrap();

        let token = build_rpc_token(
            &FluxonFsRequestIdentity {
                username: "alice".to_string(),
                password: "pw-1".to_string(),
            },
            10_000,
        )
        .unwrap();
        let claims = verify_rpc_token(&runtime_model, &token, 10_500).unwrap();
        assert_eq!(claims.username, "alice");

        let bad_token = build_rpc_token(
            &FluxonFsRequestIdentity {
                username: "alice".to_string(),
                password: "wrong".to_string(),
            },
            10_000,
        )
        .unwrap();
        let err = verify_rpc_token(&runtime_model, &bad_token, 10_500).unwrap_err();
        assert!(err.contains("rpc token signature mismatch"));
    }
}
