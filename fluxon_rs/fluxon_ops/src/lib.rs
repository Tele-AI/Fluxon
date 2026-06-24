//! Fluxon Ops core.
//!
//! This crate implements the ops core logic (agent/controller) in Rust.
//! It is designed to be exposed via `fluxon_pyo3` and started from Python.

use anyhow::Context;
use askama::Template;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::convert::Infallible;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use etcd_client as etcd;
use futures::future::join_all;
use hyper::body::HttpBody as _;
use hyper::{Body, Method, Request, Response, StatusCode};

use fluxon_kv::cluster_manager::NodeID;
use fluxon_kv::config::ClientConfigYaml;
use fluxon_kv::p2p::p2p_module::{UserRpcHandler, user_rpc_register_handler};
use fluxon_kv::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult};
use fluxon_kv::user_rpc;
use fluxon_kv::{ConfigArg, Framework, run_client};

use fluxon_proxy::{HeaderKv, PanelProxyMethod, PanelProxyResp};
use fluxon_util::{
    FluxonCliProxyDescriptorV2, FluxonCliProxyTransportV2, display_runtime_log_path,
    fluxon_cli_proxy_desc_etcd_key_v2, resolve_readable_log_path,
};

pub const OPS_SERVICE_NAME: &str = "ops";

const HDR_PROXY_ORIGINAL_URI: &str = "x-fluxon-cli-proxy-original-uri";
const HDR_PROXY_ORIGINAL_HOST: &str = "x-fluxon-cli-proxy-original-host";
const HDR_OPS_PANEL_AUTHORIZATION: &str = "x-fluxon-ops-authorization";

// English note: ops should focus on deploy/process lifecycle.
// Any "resource distribution" (upload/push_file, KV chunk transfer, etc.) is intentionally out of scope.

const OPS_AGENT_INSTANCE_KEY_PREFIX: &str = "fluxon_ops_";
const OPS_AGENT_WORKLOAD_SERVICE_NAME: &str = "ops_agent";

const OPS_HISTORY_DIR_NAME: &str = "fluxon_ops_history";
const OPS_DESIRED_DIR_NAME: &str = "fluxon_ops_desired";
const OPS_DESIRED_FILENAME: &str = "desired.json"; // legacy format (single file snapshot)
const OPS_DESIRED_WORKLOADS_DIR_NAME: &str = "workloads";
const OPS_DESIRED_APPLIES_DIR_NAME: &str = "applies";
const OPS_LOG_DIR_NAME: &str = "log";
const OPS_AGENT_DESIRED_SNAPSHOT_FILENAME: &str = "agent_desired_snapshot.json";
const OPS_NAMESPACE_ANNOTATION_KEY: &str = "fluxon.io/namespace";
const OPS_LOGICAL_SELECTION_ANNOTATION_KEY: &str = "fluxon.io/logical_selection";
const OPS_SERVICE_NAME_ANNOTATION_KEY: &str = "fluxon.io/service_name";
const OPS_ATOMIC_GROUP_ANNOTATION_KEY: &str = "fluxon.io/atomic_group";
const OPS_ATOMIC_GROUP_PHASE_ANNOTATION_KEY: &str = "fluxon.io/atomic_group_phase";
const OPS_ATOMIC_GROUP_ORDER_ANNOTATION_KEY: &str = "fluxon.io/atomic_group_order";
const OPS_SELECTION_SUPERVISOR_FILENAME: &str = "selection_supervisor.py";
const OPS_LOG_SHARD_HELPER_FILENAME: &str = "log_shard.py";
const OPS_SELECTION_SUPERVISOR_DIR_NAME: &str = "selection_supervisor";
const OPS_SELECTION_SUPERVISOR_RUN_RESTART_DELAY_SECONDS: u64 = 5;
const OPS_SELECTION_SUPERVISOR_RUN_MAX_BACKOFF_SECONDS: u64 = 30;
#[cfg(not(test))]
const OPS_SELECTION_SUPERVISOR_WAIT_PRESENT_TIMEOUT_SECONDS: u64 = 45;
#[cfg(test)]
// English note:
// - Wait-failure regressions must execute inside the normal unit-test budget.
// - Keep test-only wait bounds short so the smoke path can cover "submitted -> wait-present
//   failed -> requested apply still attached" without a 45s stall.
const OPS_SELECTION_SUPERVISOR_WAIT_PRESENT_TIMEOUT_SECONDS: u64 = 3;
#[cfg(not(test))]
const OPS_SELECTION_SUPERVISOR_WAIT_PRESENT_STABLE_SECONDS: u64 = 2;
#[cfg(test)]
const OPS_SELECTION_SUPERVISOR_WAIT_PRESENT_STABLE_SECONDS: u64 = 1;

const STOP_PROCESS_WAIT_SECONDS: u64 = 30;
const DELETE_APPLY_NO_WAIT_DELAY_SECONDS: u64 = 30;

const EMBEDDED_SELECTION_SUPERVISOR_SOURCE: &str =
    include_str!(concat!(env!("OUT_DIR"), "/selection_supervisor.py"));
const EMBEDDED_LOG_SHARD_HELPER_SOURCE: &str = include_str!(concat!(env!("OUT_DIR"), "/log_shard.py"));

// Ops controller uses Fluxon user-RPC to talk to ops agents.
// Keep the timeout as a fixed constant to avoid config surface area.
//
// Causal chain:
// - Fluxon user-RPC rejects explicit timeouts below USER_RPC_MIN_TIMEOUT_MS.
// - A ops-specific timeout must be >= that minimum, otherwise calls fail before sending.
// - Therefore we pin the ops timeout to the minimum.
const OPS_USER_RPC_TIMEOUT_MS: u64 = user_rpc::USER_RPC_MIN_TIMEOUT_MS;
const OPS_USER_RPC_TIMEOUT_GUARD_MARGIN_MS: u64 = 2_000;
const OPS_AGENT_PULL_REPAIR_INTERVAL_MS: u64 = 5_000;

// Fixed wait bound when stopping a managed process before starting a new one.
//
// Causal chain:
// - "start" has apply semantics (replace the running process if any).
// - If the old process ignores SIGTERM, waiting forever blocks the agent RPC thread.
// - We use a small fixed bound and escalate to SIGKILL, then return an explicit error if it still
//   does not exit.

// Fixed subdirectory names under controller workdir.
//
// Causal chain:
// - User explicitly chose a fixed history directory (no config field).
// - Controller must persist deploy history in a predictable location for UI/API queries.
// - We avoid fallback/auto-discovery logic; if workdir is wrong, controller errors out.

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerConfigYaml {
    pub kv_client: ClientConfigYaml,
    pub panel: PanelConfigYaml,
    pub reconcile: ReconcileConfigYaml,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PanelConfigYaml {
    pub max_body_bytes: u64,
    pub auth: PanelAuthConfigYaml,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PanelAuthConfigYaml {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconcileConfigYaml {
    pub interval_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfigYaml {
    pub kv_client: ClientConfigYaml,
    pub controller_instance_key: String,
    pub hostworkdir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerTargetDeployResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment_name: Option<String>,
    pub instance_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    pub ok: bool,
    pub err: Option<String>,
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RpcOkResp {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum WorkloadKind {
    Deployment,
    DaemonSet,
}

impl WorkloadKind {
    fn as_str(&self) -> &'static str {
        match self {
            WorkloadKind::Deployment => "Deployment",
            WorkloadKind::DaemonSet => "DaemonSet",
        }
    }
}

fn workload_key(kind: WorkloadKind, name: &str) -> String {
    format!("{}/{}", kind.as_str(), name.trim())
}

fn selection_supervisor_label_from_workload_name(
    kind: WorkloadKind,
    workload_name: &str,
) -> anyhow::Result<String> {
    Ok(workload_key(
        kind,
        &validate_workload_name_for_file(workload_name)?,
    ))
}

fn workload_name_for_logfile(name: &str) -> anyhow::Result<String> {
    let s = name.trim();
    if s.is_empty() {
        anyhow::bail!("workload name must be non-empty");
    }
    if s.contains('/') || s.contains('\\') {
        anyhow::bail!("workload name must not contain path separators: {:?}", s);
    }
    if s.contains("..") {
        anyhow::bail!("workload name must not contain '..': {:?}", s);
    }
    if s.contains('\n') || s.contains('\r') || s.contains('\0') {
        anyhow::bail!("workload name contains invalid control characters");
    }

    // Keep a narrow allowed charset so the log path is predictable and never needs escaping.
    for ch in s.chars() {
        let ok = ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.';
        if !ok {
            anyhow::bail!(
                "workload name contains unsupported character for log file: ch={:?} name={:?}",
                ch,
                s
            );
        }
    }
    Ok(s.to_string())
}

fn workload_log_filename(kind: WorkloadKind, name: &str) -> anyhow::Result<String> {
    let name = workload_name_for_logfile(name)?;
    Ok(format!("workload__{}__{}.log", kind.as_str(), name))
}

fn workload_log_shard_identity_from_path(
    logical_path: &Path,
    resolved_path: &Path,
) -> anyhow::Result<String> {
    let logical_name = logical_path
        .file_name()
        .and_then(|v| v.to_str())
        .ok_or_else(|| anyhow::anyhow!("logical log path must end with a utf-8 filename"))?;
    let resolved_name = resolved_path
        .file_name()
        .and_then(|v| v.to_str())
        .ok_or_else(|| anyhow::anyhow!("resolved log path must end with a utf-8 filename"))?;
    if resolved_name == logical_name {
        return Ok("base".to_string());
    }
    let stem = logical_name
        .strip_suffix(".log")
        .ok_or_else(|| anyhow::anyhow!("logical log filename must end with .log"))?;
    let prefix = format!("{stem}.");
    let suffix = ".log";
    if !resolved_name.starts_with(prefix.as_str()) || !resolved_name.ends_with(suffix) {
        anyhow::bail!(
            "resolved log path is not a recognized shard of logical log: logical={} resolved={}",
            logical_name,
            resolved_name
        );
    }
    let shard = &resolved_name[prefix.len()..resolved_name.len() - suffix.len()];
    if shard.is_empty() {
        anyhow::bail!(
            "resolved log shard identity is empty: logical={} resolved={}",
            logical_name,
            resolved_name
        );
    }
    Ok(shard.to_string())
}

fn workload_log_path_for_shard(logical_path: &Path, shard: &str) -> anyhow::Result<PathBuf> {
    if shard == "base" {
        return Ok(logical_path.to_path_buf());
    }
    let date = chrono::NaiveDate::parse_from_str(shard, "%Y-%m-%d")
        .map_err(|e| anyhow::anyhow!("invalid workload log shard identity '{}': {}", shard, e))?;
    fluxon_util::daily_sharded_log_path(logical_path, date)
}

fn workload_log_existing_shards(logical_path: &Path) -> anyhow::Result<Vec<String>> {
    let mut dated_shards = Vec::new();
    let mut has_base = false;
    let parent = logical_path.parent().unwrap_or_else(|| Path::new("."));
    let logical_name = logical_path
        .file_name()
        .and_then(|v| v.to_str())
        .ok_or_else(|| anyhow::anyhow!("logical log path must end with a utf-8 filename"))?;
    let stem = logical_name
        .strip_suffix(".log")
        .ok_or_else(|| anyhow::anyhow!("logical log filename must end with .log"))?;
    let prefix = format!("{stem}.");
    let suffix = ".log";
    let entries = std::fs::read_dir(parent)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let entry_name = entry.file_name();
        let Some(entry_name) = entry_name.to_str() else {
            continue;
        };
        if entry_name == logical_name {
            has_base = true;
            continue;
        }
        if !entry_name.starts_with(prefix.as_str()) || !entry_name.ends_with(suffix) {
            continue;
        }
        if entry_name.len() <= prefix.len() + suffix.len() {
            continue;
        }
        let shard = &entry_name[prefix.len()..entry_name.len() - suffix.len()];
        if chrono::NaiveDate::parse_from_str(shard, "%Y-%m-%d").is_ok() {
            dated_shards.push(shard.to_string());
        }
    }
    dated_shards.sort();
    dated_shards.dedup();
    if !dated_shards.is_empty() {
        return Ok(dated_shards);
    }
    if has_base {
        return Ok(vec!["base".to_string()]);
    }
    Ok(Vec::new())
}

fn workload_log_previous_shard(logical_path: &Path, shard: &str) -> anyhow::Result<Option<String>> {
    let shards = workload_log_existing_shards(logical_path)?;
    let Some(idx) = shards.iter().position(|v| v == shard) else {
        return Ok(None);
    };
    if idx == 0 {
        return Ok(None);
    }
    Ok(Some(shards[idx - 1].clone()))
}

fn workload_log_next_shard(logical_path: &Path, shard: &str) -> anyhow::Result<Option<String>> {
    let shards = workload_log_existing_shards(logical_path)?;
    let Some(idx) = shards.iter().position(|v| v == shard) else {
        return Ok(None);
    };
    if idx + 1 >= shards.len() {
        return Ok(None);
    }
    Ok(Some(shards[idx + 1].clone()))
}

fn workload_log_latest_shard_identity(logical_path: &Path) -> anyhow::Result<Option<String>> {
    let Some(path) = resolve_readable_log_path(logical_path) else {
        return Ok(None);
    };
    Ok(Some(workload_log_shard_identity_from_path(logical_path, &path)?))
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkloadId {
    kind: WorkloadKind,
    name: String,
    #[serde(default)]
    authority: String,
}

impl WorkloadId {
    fn new(kind: WorkloadKind, name: impl Into<String>, authority: impl Into<String>) -> Self {
        Self {
            kind,
            name: name.into(),
            authority: authority.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AtomicGroupMeta {
    selection_name: String,
    phase: u64,
    order: u64,
}

fn deployment_group_key_from_workloads(workloads: &[WorkloadId]) -> anyhow::Result<String> {
    let mut w: Vec<WorkloadId> = workloads.to_vec();
    w.sort();
    w.dedup();
    if w.is_empty() {
        anyhow::bail!("deployment_group_key requires at least one workload");
    }
    let keys: Vec<String> = w.iter().map(|x| workload_key(x.kind, &x.name)).collect();
    // Serialize as a JSON array string to avoid delimiter collisions in workload names.
    Ok(serde_json::to_string(&keys).unwrap())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StartReq {
    kind: WorkloadKind,
    name: String,
    authority: String,
    service_name: String,
    apply_id: String,
    owner_ts_ms: u64,
    argv: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
    wait_for_attached: bool,
    #[serde(default = "start_req_wait_for_present_default")]
    wait_for_present: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StartResp {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pid: Option<u32>,
}

fn start_req_wait_for_present_default() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StatusReq {
    kind: WorkloadKind,
    name: String,
    authority: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StatusResp {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
    running: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    present: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    apply_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner_ts_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    authority: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    container_orphan_zombie_pids: Vec<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status_hint: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
struct StatusHttpResp {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
    running: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    present: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    apply_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner_ts_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    authority: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    container_orphan_zombie_pids: Vec<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status_hint: Option<String>,

    // English note: these are controller-side desired-state diagnostics (not agent status fields).
    #[serde(skip_serializing_if = "Option::is_none")]
    desired_apply_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    desired_updated_ts_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    desired_deployment_yaml_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    desired_matches_running: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    desired_matches_present: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteGenerationReq {
    kind: WorkloadKind,
    name: String,
    authority: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    require_apply_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteGenerationResp {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WaitDeleteGenerationResp {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControllerDeleteGenerationMode {
    Immediate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListWorkloadsReq {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadyReq {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadyResp {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkloadStatusSummary {
    kind: WorkloadKind,
    name: String,
    authority: String,
    running: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    present: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    apply_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner_ts_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    container_orphan_zombie_pids: Vec<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status_hint: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
struct WorkloadStatusSummaryHttp {
    kind: WorkloadKind,
    name: String,
    authority: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    namespace: Option<String>,
    running: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    present: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    apply_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner_ts_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    container_orphan_zombie_pids: Vec<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status_hint: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    desired_apply_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    desired_updated_ts_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    desired_deployment_yaml_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    desired_apply_err: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    desired_matches_running: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    desired_matches_present: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListWorkloadsResp {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workloads: Option<Vec<WorkloadStatusSummary>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListApplyRuntimeReq {
    apply_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListApplyRuntimeResp {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workloads: Option<Vec<WorkloadStatusSummary>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentDesiredWorkload {
    kind: WorkloadKind,
    name: String,
    authority: String,
    logical_selection: String,
    service_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    atomic_group: Option<AtomicGroupMeta>,
    apply_id: String,
    argv: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
    updated_ts_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentDeleteWorkload {
    kind: WorkloadKind,
    name: String,
    authority: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    atomic_group: Option<AtomicGroupMeta>,
    apply_id: String,
    phase: ApplyLifecyclePhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    phase_updated_ts_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentDesiredListResp {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
    instance_key: String,
    desired_keys: Vec<WorkloadId>,
    workloads: Vec<AgentDesiredWorkload>,
    #[serde(default)]
    delete_workloads: Vec<AgentDeleteWorkload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentDesiredSnapshotRecord {
    instance_key: String,
    desired_keys: Vec<WorkloadId>,
    workloads: Vec<AgentDesiredWorkload>,
    #[serde(default)]
    delete_workloads: Vec<AgentDeleteWorkload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentDesiredReq {
    instance_key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum LogReadDirection {
    Forward,
    Backward,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadWorkloadLogReq {
    kind: WorkloadKind,
    name: String,
    direction: LogReadDirection,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<WorkloadLogCursor>,
    // Contract:
    // - max_bytes may be omitted to mean "unlimited" (no byte cap).
    // - This supports ad-hoc debugging where the caller wants the full log without knowing file_size up-front.
    // - When specified, it must be > 0 to avoid ambiguous "0 means tail nothing" semantics.
    #[serde(skip_serializing_if = "Option::is_none")]
    max_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadWorkloadLogResp {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start_offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end_offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start_cursor: Option<WorkloadLogCursor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end_cursor: Option<WorkloadLogCursor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkloadLogCursor {
    shard: String,
    offset: u64,
}

fn ensure_positive_u64(v: u64, field: &str) -> KvResult<u64> {
    if v == 0 {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: format!("{} must be > 0", field),
        }));
    }
    Ok(v)
}

fn ensure_u64_fits_usize(v: u64, field: &str) -> anyhow::Result<usize> {
    usize::try_from(v).map_err(|_| anyhow::anyhow!("{} must fit usize (got {})", field, v))
}

fn agent_instance_key_from_node_name(node_name: &str) -> KvResult<String> {
    let n = node_name.trim();
    if n.is_empty() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "node name must be non-empty".to_string(),
        }));
    }
    Ok(format!("{}{}", OPS_AGENT_INSTANCE_KEY_PREFIX, n))
}

fn controller_local_agent_instance_key(controller_instance_key: &str) -> Option<String> {
    // English note:
    // - delete_apply broadcasts target agent instance keys (`fluxon_ops_<node>`), not the controller
    //   workload instance key (`ops_controller_<node>`).
    // - We still need "self last" ordering so the controller does not kill its colocated ops_agent
    //   before finishing remote phase1/phase2 RPCs.
    let node_name = controller_instance_key
        .trim()
        .strip_prefix("ops_controller_")?;
    if node_name.is_empty() {
        return None;
    }
    Some(format!("{}{}", OPS_AGENT_INSTANCE_KEY_PREFIX, node_name))
}

pub async fn build_framework_from_kv_client_yaml(
    kv_client_yaml: &ClientConfigYaml,
) -> anyhow::Result<(Arc<Framework>, fluxon_kv::config::ClientConfig)> {
    let cfg = kv_client_yaml
        .clone()
        .verify()
        .map_err(|e| anyhow::anyhow!("verify kv_client yaml failed: {}", e))?;
    run_client(ConfigArg::Config(cfg))
        .await
        .map_err(|e| anyhow::anyhow!("run_client failed: {}", e))
}

async fn user_rpc_call_json<Req, Resp>(
    fw: &Framework,
    target_instance_key: &str,
    method: &str,
    req: &Req,
) -> anyhow::Result<Resp>
where
    Req: Serialize,
    Resp: for<'de> Deserialize<'de>,
{
    let rpc_timeout_ms = OPS_USER_RPC_TIMEOUT_MS;
    let outer_timeout_ms = rpc_timeout_ms.saturating_add(OPS_USER_RPC_TIMEOUT_GUARD_MARGIN_MS);

    let req_bytes = serde_json::to_vec(req)
        .with_context(|| format!("encode request json for method={}", method))?;

    let resp_bytes = tokio::time::timeout(
        Duration::from_millis(outer_timeout_ms),
        user_rpc::user_rpc_call(
            fw,
            target_instance_key.to_string().into(),
            method.to_string(),
            req_bytes,
            rpc_timeout_ms,
        ),
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "user rpc call exceeded ops outer timeout: target_instance_key={} method={} inner_timeout_ms={} outer_timeout_ms={}",
            target_instance_key,
            method,
            rpc_timeout_ms,
            outer_timeout_ms
        )
    })?
    .map_err(|e| anyhow::anyhow!("user rpc call failed: {}", e))?;

    serde_json::from_slice(&resp_bytes)
        .with_context(|| format!("decode response json for method={}", method))
}

#[derive(Debug, Clone)]
struct SelectionSupervisorStatus {
    label: String,
    pid: Option<u32>,
    #[allow(dead_code)]
    pgid: Option<u32>,
    running: bool,
    present: bool,
    #[allow(dead_code)]
    process_count: u32,
    #[allow(dead_code)]
    child_process_count: u32,
    kind: Option<WorkloadKind>,
    name: Option<String>,
    authority: Option<String>,
    #[allow(dead_code)]
    service_name: Option<String>,
    apply_id: Option<String>,
    argv: Option<Vec<String>>,
    cwd: Option<String>,
    #[allow(dead_code)]
    log_path: Option<String>,
    #[allow(dead_code)]
    started_ts_ms: Option<u64>,
    owner_ts_ms: Option<u64>,
    supervisor_start_time_ticks: Option<u64>,
    container_orphan_zombie_pids: Vec<u32>,
    status_hint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectionSupervisorLaunchState {
    kind: WorkloadKind,
    name: String,
    authority: String,
    service_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    apply_id: Option<String>,
    argv: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
    log_path: String,
}

struct SelectionSupervisorTarget {
    label: String,
    scope_key: String,
}

#[derive(Clone)]
struct SelectionSupervisorRuntime {
    python_exe: PathBuf,
    script_path: PathBuf,
    scope_key: String,
}

#[derive(Debug, Clone)]
struct ProcessInfoObservation {
    pid: u32,
    ppid: u32,
    pgid: u32,
    state: char,
    start_time_ticks: u64,
}

impl ProcessInfoObservation {
    fn is_zombie(&self) -> bool {
        self.state == 'Z'
    }
}

#[derive(Debug, Clone)]
struct LiveSelectionSupervisor {
    process_info: ProcessInfoObservation,
    owner_ts_ms: u64,
    label: String,
    runtime_state: Option<SelectionSupervisorLaunchState>,
}

impl LiveSelectionSupervisor {
    fn pid(&self) -> u32 {
        self.process_info.pid
    }

    fn pgid(&self) -> u32 {
        self.process_info.pgid
    }

    fn start_time_ticks(&self) -> u64 {
        self.process_info.start_time_ticks
    }
}

struct SelectionSupervisorProcSnapshot {
    infos_by_pid: HashMap<u32, ProcessInfoObservation>,
    children_by_ppid: HashMap<u32, Vec<u32>>,
    cmdlines: Vec<(u32, Vec<String>)>,
    zombie_infos: Vec<ProcessInfoObservation>,
}

#[derive(Clone)]
struct AgentDesiredSnapshotStore {
    path: PathBuf,
}

struct SupervisorBackedWorkloads {
    op_guard: std::sync::Mutex<()>,
    log_dir: PathBuf,
    supervisor_runtime: SelectionSupervisorRuntime,
    scope_key: String,
}

#[derive(Debug, Clone)]
struct TargetWorkloadQuery {
    target: String,
    workload: WorkloadId,
}

fn shared_selection_supervisor_runtime_dir(hostworkdir: &Path) -> PathBuf {
    hostworkdir.join(OPS_SELECTION_SUPERVISOR_DIR_NAME)
}

// Keep this authority suffix derivation aligned with `deployment/utils/selection_runtime.py`.
// Self-host desired DaemonSet names may change rollout prefix across generations, but the
// supervisor authority must stay keyed by the stable logical-selection suffix below.
// The desired manifest contract we validate here is therefore:
// - workload transport identity: `<non-empty rollout prefix>-<stable authority suffix>`
// - supervisor ownership identity: `<kind>/<stable authority suffix>`
fn selection_workload_suffix(
    logical_selection: &str,
    service_name: &str,
    is_atomic_group_member: bool,
) -> anyhow::Result<String> {
    let logical_selection = workload_name_for_logfile(logical_selection)?;
    if !is_atomic_group_member {
        return Ok(logical_selection);
    }
    let service_name = workload_name_for_logfile(service_name)?;
    Ok(format!("{}__{}", logical_selection, service_name))
}

fn selection_authority_name(
    kind: WorkloadKind,
    logical_selection: &str,
    service_name: &str,
    atomic_group: Option<&AtomicGroupMeta>,
) -> anyhow::Result<String> {
    match kind {
        WorkloadKind::Deployment => workload_name_for_logfile(logical_selection),
        WorkloadKind::DaemonSet => {
            selection_workload_suffix(logical_selection, service_name, atomic_group.is_some())
        }
    }
}

fn validate_daemonset_workload_name_matches_selection_contract(
    name: &str,
    logical_selection: &str,
    service_name: &str,
    is_atomic_group_member: bool,
) -> anyhow::Result<String> {
    let workload_name = workload_name_for_logfile(name)?;
    let suffix =
        selection_workload_suffix(logical_selection, service_name, is_atomic_group_member)?;
    let expected_tail = format!("-{}", suffix);
    let Some(name_prefix) = workload_name.strip_suffix(expected_tail.as_str()) else {
        anyhow::bail!(
            "daemonset metadata.name must follow the fluxon_ops shared naming contract and end with the selection suffix under a non-empty prefix: workload_name={} logical_selection={} service_name={} atomic_group_member={}",
            workload_name,
            logical_selection,
            service_name,
            is_atomic_group_member
        );
    };
    if name_prefix.is_empty() {
        anyhow::bail!(
            "daemonset metadata.name must keep a non-empty prefix before the selection suffix: workload_name={} logical_selection={} service_name={} atomic_group_member={}",
            workload_name,
            logical_selection,
            service_name,
            is_atomic_group_member
        );
    }
    Ok(workload_name)
}

impl AgentDesiredSnapshotStore {
    fn new(workdir: &Path) -> Self {
        Self {
            path: workdir.join(OPS_AGENT_DESIRED_SNAPSHOT_FILENAME),
        }
    }

    fn load(&self, instance_key: &str) -> anyhow::Result<Option<AgentDesiredSnapshotRecord>> {
        let raw = match std::fs::read(&self.path) {
            Ok(v) => v,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    return Ok(None);
                }
                return Err(anyhow::Error::new(e).context(format!(
                    "read agent desired snapshot: {}",
                    self.path.display()
                )));
            }
        };
        let snapshot = serde_json::from_slice::<AgentDesiredSnapshotRecord>(&raw)
            .with_context(|| format!("decode agent desired snapshot: {}", self.path.display()))?;
        if snapshot.instance_key != instance_key {
            anyhow::bail!(
                "agent desired snapshot instance_key mismatch: expected={} actual={} path={}",
                instance_key,
                snapshot.instance_key,
                self.path.display()
            );
        }
        Ok(Some(snapshot))
    }

    fn persist(&self, snapshot: &AgentDesiredSnapshotRecord) -> anyhow::Result<()> {
        if snapshot.instance_key.trim().is_empty() {
            anyhow::bail!("instance_key must be non-empty for agent desired snapshot");
        }
        let parent = self.path.parent().ok_or_else(|| {
            anyhow::anyhow!(
                "agent desired snapshot path has no parent: {}",
                self.path.display()
            )
        })?;
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create agent desired snapshot dir: {}", parent.display()))?;
        let normalized = AgentDesiredSnapshotRecord {
            instance_key: snapshot.instance_key.clone(),
            desired_keys: workload_id_map(snapshot.desired_keys.as_slice())?
                .into_values()
                .collect(),
            workloads: agent_desired_workload_map(snapshot.workloads.as_slice())?
                .into_values()
                .collect(),
            delete_workloads: agent_delete_workload_map(snapshot.delete_workloads.as_slice())?
                .into_values()
                .collect(),
        };
        let tmp_path = self.path.with_extension("json.tmp");
        let buf = serde_json::to_vec(&normalized).context("encode agent desired snapshot json")?;
        std::fs::write(&tmp_path, buf)
            .with_context(|| format!("write agent desired snapshot tmp: {}", tmp_path.display()))?;
        std::fs::rename(&tmp_path, &self.path).with_context(|| {
            format!(
                "rename agent desired snapshot tmp: {} -> {}",
                tmp_path.display(),
                self.path.display()
            )
        })?;
        Ok(())
    }
}

fn resolve_python_host_executable(python_exe: &Path) -> anyhow::Result<PathBuf> {
    let resolved = if python_exe.is_absolute() {
        python_exe.to_path_buf()
    } else {
        std::env::current_dir()
            .context("resolve current dir for embedded supervisor host python")?
            .join(python_exe)
    };
    if !resolved.is_file() {
        anyhow::bail!(
            "embedded selection supervisor requires an existing Python host executable: {}",
            resolved.display()
        );
    }
    Ok(resolved)
}

fn ensure_embedded_selection_supervisor_runtime(workdir: &Path) -> anyhow::Result<(PathBuf, PathBuf)> {
    let runtime_dir = workdir.join(OPS_SELECTION_SUPERVISOR_DIR_NAME);
    std::fs::create_dir_all(&runtime_dir).with_context(|| {
        format!(
            "create embedded selection supervisor dir: {}",
            runtime_dir.display()
        )
    })?;
    let script_path = runtime_dir.join(OPS_SELECTION_SUPERVISOR_FILENAME);
    let helper_path = runtime_dir.join(OPS_LOG_SHARD_HELPER_FILENAME);
    let should_write = match std::fs::read_to_string(&script_path) {
        Ok(existing) => existing != EMBEDDED_SELECTION_SUPERVISOR_SOURCE,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                true
            } else {
                return Err(anyhow::Error::new(e).context(format!(
                    "read embedded selection supervisor failed: {}",
                    script_path.display()
                )));
            }
        }
    };
    let should_write_helper = match std::fs::read_to_string(&helper_path) {
        Ok(existing) => existing != EMBEDDED_LOG_SHARD_HELPER_SOURCE,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                true
            } else {
                return Err(anyhow::Error::new(e).context(format!(
                    "read embedded log shard helper failed: {}",
                    helper_path.display()
                )));
            }
        }
    };
    if should_write {
        std::fs::write(&script_path, EMBEDDED_SELECTION_SUPERVISOR_SOURCE).with_context(|| {
            format!(
                "write embedded selection supervisor failed: {}",
                script_path.display()
            )
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perm = std::fs::metadata(&script_path)
                .with_context(|| {
                    format!(
                        "stat embedded selection supervisor: {}",
                        script_path.display()
                    )
                })?
                .permissions();
            perm.set_mode(0o755);
            std::fs::set_permissions(&script_path, perm).with_context(|| {
                format!(
                    "chmod embedded selection supervisor failed: {}",
                    script_path.display()
                )
            })?;
        }
    }
    if should_write_helper {
        std::fs::write(&helper_path, EMBEDDED_LOG_SHARD_HELPER_SOURCE).with_context(|| {
            format!(
                "write embedded log shard helper failed: {}",
                helper_path.display()
            )
        })?;
    }
    Ok((script_path, helper_path))
}

impl SelectionSupervisorRuntime {
    fn materialize(workdir: &Path, hostworkdir: &Path, python_exe: &Path) -> anyhow::Result<Self> {
        let python_exe = resolve_python_host_executable(python_exe)?;
        let (script_path, _helper_path) = ensure_embedded_selection_supervisor_runtime(workdir)?;
        if !hostworkdir.is_absolute() {
            anyhow::bail!(
                "hostworkdir must be absolute for shared selection supervisor runtime: {}",
                hostworkdir.display()
            );
        }
        let _ = shared_selection_supervisor_runtime_dir(hostworkdir);
        Ok(Self {
            python_exe,
            script_path,
            scope_key: hostworkdir.display().to_string(),
        })
    }

    fn target(
        &self,
        kind: WorkloadKind,
        workload_name: &str,
    ) -> anyhow::Result<SelectionSupervisorTarget> {
        Ok(SelectionSupervisorTarget {
            label: selection_supervisor_label_from_workload_name(kind, workload_name)?,
            scope_key: self.scope_key.clone(),
        })
    }

    fn command_base(&self, subcommand: &str) -> std::process::Command {
        let mut cmd = std::process::Command::new(&self.python_exe);
        cmd.arg(&self.script_path).arg(subcommand);
        cmd
    }

    fn command_with_target(
        &self,
        subcommand: &str,
        target: &SelectionSupervisorTarget,
    ) -> std::process::Command {
        let mut cmd = self.command_base(subcommand);
        cmd.arg("--label").arg(&target.label);
        cmd.arg("--scope-key").arg(&target.scope_key);
        cmd
    }

    fn stop(
        &self,
        kind: WorkloadKind,
        workload_name: &str,
        missing_ok: bool,
        require_apply_id: Option<&str>,
    ) -> anyhow::Result<()> {
        let target = self.target(kind, workload_name)?;
        let mut cmd = self.command_with_target("stop", &target);
        if let Some(apply_id) = require_apply_id {
            let apply_id = apply_id.trim();
            if apply_id.is_empty() {
                anyhow::bail!("require_apply_id must be non-empty when provided");
            }
            cmd.arg("--require-apply-id").arg(apply_id);
        }
        if missing_ok {
            cmd.arg("--missing-ok");
        }
        let status = cmd.status().with_context(|| {
            format!(
                "run embedded selection supervisor stop failed: label={}",
                target.label
            )
        })?;
        if !status.success() {
            anyhow::bail!(
                "embedded selection supervisor stop failed: label={} exit_status={}",
                target.label,
                status
            );
        }
        Ok(())
    }

    fn launch_detached(
        &self,
        kind: WorkloadKind,
        name: &str,
        authority: &str,
        service_name: &str,
        apply_id: &str,
        owner_ts_ms: u64,
        argv: &[String],
        cwd: Option<&str>,
        log_path: &Path,
    ) -> anyhow::Result<u32> {
        let target = self.target(kind, name)?;
        let supervisor_cmd = self.build_run_command(
            kind,
            name,
            authority,
            service_name,
            &target,
            apply_id,
            owner_ts_ms,
            argv,
            cwd,
            log_path,
        )?;
        self.spawn_detached_command(log_path, supervisor_cmd.as_slice())
    }

    fn build_run_command(
        &self,
        kind: WorkloadKind,
        name: &str,
        authority: &str,
        service_name: &str,
        target: &SelectionSupervisorTarget,
        apply_id: &str,
        owner_ts_ms: u64,
        argv: &[String],
        cwd: Option<&str>,
        log_path: &Path,
    ) -> anyhow::Result<Vec<String>> {
        let state_json = serde_json::to_string(&SelectionSupervisorLaunchState {
            kind,
            name: name.to_string(),
            authority: authority.to_string(),
            service_name: service_name.to_string(),
            apply_id: Some(apply_id.to_string()),
            argv: argv.to_vec(),
            cwd: cwd.map(|v| v.to_string()),
            log_path: log_path.display().to_string(),
        })
        .context("encode embedded selection supervisor state json")?;
        let mut supervisor_cmd: Vec<String> = vec![
            self.python_exe.display().to_string(),
            self.script_path.display().to_string(),
            "run".to_string(),
            "--label".to_string(),
            target.label.clone(),
            "--scope-key".to_string(),
            target.scope_key.clone(),
            "--state-json".to_string(),
            state_json,
            "--owner-ts-ms".to_string(),
            owner_ts_ms.to_string(),
            "--restart-policy".to_string(),
            "always".to_string(),
            "--restart-delay-seconds".to_string(),
            OPS_SELECTION_SUPERVISOR_RUN_RESTART_DELAY_SECONDS.to_string(),
            "--max-backoff-seconds".to_string(),
            OPS_SELECTION_SUPERVISOR_RUN_MAX_BACKOFF_SECONDS.to_string(),
            "--crashloop-consecutive-restarts".to_string(),
            "0".to_string(),
            "--crashloop-interval-lt-seconds".to_string(),
            "0".to_string(),
        ];
        if let Some(cwd) = cwd {
            supervisor_cmd.push("--workdir".to_string());
            supervisor_cmd.push(cwd.to_string());
        }
        supervisor_cmd.push("--".to_string());
        supervisor_cmd.extend(argv.iter().cloned());
        Ok(supervisor_cmd)
    }

    fn spawn_detached_command(&self, log_path: &Path, command: &[String]) -> anyhow::Result<u32> {
        let detacher_script = r#"
import subprocess
import sys

log_path = sys.argv[1]
command = sys.argv[2:]
with open(log_path, "ab", buffering=0) as fp:
    child = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=fp,
        stderr=fp,
        start_new_session=True,
        close_fds=True,
    )
print(child.pid)
"#;
        let output = std::process::Command::new(&self.python_exe)
            .arg("-c")
            .arg(detacher_script)
            .arg(log_path)
            .args(command.iter())
            .output()
            .with_context(|| {
                format!(
                    "run embedded selection supervisor detacher failed: log_path={}",
                    log_path.display()
                )
            })?;
        if !output.status.success() {
            anyhow::bail!(
                "embedded selection supervisor detacher failed: log_path={} stderr={}",
                log_path.display(),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let pid_text = String::from_utf8(output.stdout)
            .context("decode embedded selection supervisor detacher stdout")?;
        let pid = pid_text.trim().parse::<u32>().with_context(|| {
            format!(
                "parse embedded selection supervisor pid failed: stdout={:?}",
                pid_text
            )
        })?;
        Ok(pid)
    }
}

fn parse_selection_supervisor_state_json(
    raw_state_json: &str,
) -> anyhow::Result<SelectionSupervisorLaunchState> {
    serde_json::from_str::<SelectionSupervisorLaunchState>(raw_state_json)
        .context("decode running selection supervisor state-json")
}

fn parse_running_supervisor_owner_ts_ms(raw: &str) -> anyhow::Result<u64> {
    let value = raw
        .trim()
        .parse::<u64>()
        .with_context(|| format!("parse running supervisor owner_ts_ms failed: raw={raw}"))?;
    if value == 0 {
        anyhow::bail!("running supervisor owner_ts_ms must be > 0");
    }
    Ok(value)
}

fn read_process_info_observation(pid: u32) -> anyhow::Result<Option<ProcessInfoObservation>> {
    let stat_path = PathBuf::from(format!("/proc/{pid}/stat"));
    let raw = match std::fs::read_to_string(&stat_path) {
        Ok(v) => v,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                return Ok(None);
            }
            return Err(anyhow::Error::new(e)
                .context(format!("read process stat failed: {}", stat_path.display())));
        }
    };
    let rparen = raw
        .rfind(')')
        .ok_or_else(|| anyhow::anyhow!("unexpected /proc/<pid>/stat format: missing ')'"))?;
    let head = &raw[..=rparen];
    let tail = raw
        .get(rparen + 2..)
        .ok_or_else(|| anyhow::anyhow!("unexpected /proc/<pid>/stat format: truncated"))?;
    let fields: Vec<&str> = tail.split_whitespace().collect();
    if fields.len() < 20 {
        anyhow::bail!("unexpected /proc/<pid>/stat format: too few fields");
    }
    let pid_from_head = head
        .split_once(' ')
        .ok_or_else(|| anyhow::anyhow!("unexpected /proc/<pid>/stat format: missing pid"))?
        .0
        .parse::<u32>()
        .context("parse /proc/<pid>/stat pid")?;
    let state = fields[0]
        .chars()
        .next()
        .ok_or_else(|| anyhow::anyhow!("unexpected /proc/<pid>/stat format: missing state"))?;
    Ok(Some(ProcessInfoObservation {
        pid: pid_from_head,
        ppid: fields[1]
            .parse::<u32>()
            .context("parse /proc/<pid>/stat ppid")?,
        pgid: fields[2]
            .parse::<u32>()
            .context("parse /proc/<pid>/stat pgid")?,
        state,
        start_time_ticks: fields[19]
            .parse::<u64>()
            .context("parse /proc/<pid>/stat start_time_ticks")?,
    }))
}

fn read_process_cmdline(pid: u32) -> anyhow::Result<Option<Vec<String>>> {
    let cmdline_path = PathBuf::from(format!("/proc/{pid}/cmdline"));
    let raw = match std::fs::read(&cmdline_path) {
        Ok(v) => v,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                return Ok(None);
            }
            return Err(anyhow::Error::new(e).context(format!(
                "read process cmdline failed: {}",
                cmdline_path.display()
            )));
        }
    };
    if raw.is_empty() {
        return Ok(None);
    }
    let out: Vec<String> = raw
        .split(|b| *b == 0)
        .filter(|chunk| !chunk.is_empty())
        .map(|chunk| String::from_utf8_lossy(chunk).to_string())
        .collect();
    if out.is_empty() {
        return Ok(None);
    }
    Ok(Some(out))
}

fn selection_supervisor_cmd_arg_value<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    args.windows(2)
        .find(|pair| pair[0] == flag)
        .map(|pair| pair[1].as_str())
}

fn find_selection_supervisor_long_running_command(args: &[String]) -> Option<&str> {
    for (idx, arg) in args.iter().take(4).enumerate() {
        if Path::new(arg).file_name().and_then(|v| v.to_str()) != Some("selection_supervisor.py") {
            continue;
        }
        let command = args.get(idx + 1)?;
        if command == "run" {
            return Some(command.as_str());
        }
        return None;
    }
    None
}

fn selection_supervisor_proc_snapshot() -> anyhow::Result<SelectionSupervisorProcSnapshot> {
    let mut infos_by_pid: HashMap<u32, ProcessInfoObservation> = HashMap::new();
    let mut children_by_ppid: HashMap<u32, Vec<u32>> = HashMap::new();
    let mut cmdlines: Vec<(u32, Vec<String>)> = Vec::new();
    let mut zombie_infos: Vec<ProcessInfoObservation> = Vec::new();
    for entry_res in std::fs::read_dir("/proc").context("read /proc dir")? {
        let entry = match entry_res {
            Ok(v) => v,
            Err(_) => continue,
        };
        let name = entry.file_name();
        let Some(name_text) = name.to_str() else {
            continue;
        };
        let Ok(pid) = name_text.parse::<u32>() else {
            continue;
        };
        let Some(info) = read_process_info_observation(pid)? else {
            continue;
        };
        if info.is_zombie() {
            zombie_infos.push(info);
            continue;
        }
        children_by_ppid
            .entry(info.ppid)
            .or_default()
            .push(info.pid);
        infos_by_pid.insert(info.pid, info);
        if let Some(args) = read_process_cmdline(pid)? {
            cmdlines.push((pid, args));
        }
    }
    for children in children_by_ppid.values_mut() {
        children.sort_unstable();
    }
    Ok(SelectionSupervisorProcSnapshot {
        infos_by_pid,
        children_by_ppid,
        cmdlines,
        zombie_infos,
    })
}

fn live_selection_supervisors(
    snapshot: &SelectionSupervisorProcSnapshot,
    label_filter: Option<&str>,
    scope_key_filter: Option<&str>,
) -> anyhow::Result<Vec<LiveSelectionSupervisor>> {
    let mut out: Vec<LiveSelectionSupervisor> = Vec::new();
    for (pid, args) in snapshot.cmdlines.iter() {
        if find_selection_supervisor_long_running_command(args.as_slice()).is_none() {
            continue;
        }
        let Some(info) = snapshot.infos_by_pid.get(pid) else {
            continue;
        };
        let Some(label) = selection_supervisor_cmd_arg_value(args.as_slice(), "--label") else {
            continue;
        };
        if let Some(expected) = label_filter {
            if label != expected {
                continue;
            }
        }
        let runtime_scope_key = selection_supervisor_cmd_arg_value(args.as_slice(), "--scope-key");
        if let Some(expected_scope_key) = scope_key_filter {
            if runtime_scope_key != Some(expected_scope_key) {
                continue;
            }
        }
        let strict_match = label_filter.is_some();
        let Some(owner_ts_ms_raw) =
            selection_supervisor_cmd_arg_value(args.as_slice(), "--owner-ts-ms")
        else {
            if strict_match {
                anyhow::bail!(
                    "running selection supervisor is missing --owner-ts-ms pid={pid} label={label}"
                );
            }
            continue;
        };
        let owner_ts_ms = match parse_running_supervisor_owner_ts_ms(owner_ts_ms_raw) {
            Ok(v) => v,
            Err(e) => {
                if strict_match {
                    return Err(e.context(format!(
                        "invalid running selection supervisor owner_ts_ms pid={pid} label={label}"
                    )));
                }
                continue;
            }
        };
        let runtime_state = match selection_supervisor_cmd_arg_value(
            args.as_slice(),
            "--state-json",
        ) {
            Some(raw) => match parse_selection_supervisor_state_json(raw) {
                Ok(v) => Some(v),
                Err(e) => {
                    if strict_match {
                        return Err(e.context(format!(
                            "invalid running selection supervisor state-json pid={pid} label={label}"
                        )));
                    }
                    continue;
                }
            },
            None => None,
        };
        out.push(LiveSelectionSupervisor {
            process_info: info.clone(),
            owner_ts_ms,
            label: label.to_string(),
            runtime_state,
        });
    }
    Ok(out)
}

fn selection_supervisor_sort_key(supervisor: &LiveSelectionSupervisor) -> u64 {
    supervisor.owner_ts_ms
}

fn workload_identity_from_selection_label(label: &str) -> anyhow::Result<(WorkloadKind, String)> {
    let (raw_kind, raw_name) = label.split_once('/').ok_or_else(|| {
        anyhow::anyhow!("selection supervisor label must be Kind/name: label={label}")
    })?;
    let kind = match raw_kind {
        "Deployment" => WorkloadKind::Deployment,
        "DaemonSet" => WorkloadKind::DaemonSet,
        other => anyhow::bail!(
            "selection supervisor label has unsupported kind: label={label} kind={other}"
        ),
    };
    let name = validate_workload_name_for_file(raw_name)?;
    Ok((kind, name))
}

fn selection_owner_supervisor(
    snapshot: &SelectionSupervisorProcSnapshot,
    label: &str,
    scope_key: Option<&str>,
    exclude_pid: Option<u32>,
) -> anyhow::Result<Option<LiveSelectionSupervisor>> {
    let owners: Vec<LiveSelectionSupervisor> = live_selection_supervisors(snapshot, Some(label), scope_key)?
        .into_iter()
        .filter(|supervisor| exclude_pid != Some(supervisor.pid()))
        .collect();
    if owners.is_empty() {
        return Ok(None);
    }
    let owner_ts_ms = owners
        .iter()
        .map(|supervisor| selection_supervisor_sort_key(supervisor))
        .max()
        .unwrap();
    let matching: Vec<&LiveSelectionSupervisor> = owners
        .iter()
        .filter(|supervisor| supervisor.owner_ts_ms == owner_ts_ms)
        .collect();
    if matching.len() > 1 {
        let pid_list = matching
            .iter()
            .map(|supervisor| {
                format!(
                    "pid={} runtime_state={}",
                    supervisor.pid(),
                    supervisor.runtime_state.is_some()
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        anyhow::bail!(
            "selection supervisor owner_ts_ms collision label={} owner_ts_ms={} matches=[{}]",
            label,
            owner_ts_ms,
            pid_list
        );
    }
    Ok(owners
        .into_iter()
        .find(|supervisor| supervisor.owner_ts_ms == owner_ts_ms))
}

fn pid_tree_members(snapshot: &SelectionSupervisorProcSnapshot, root_pid: u32) -> Vec<u32> {
    let mut out: Vec<u32> = Vec::new();
    let mut stack: Vec<u32> = vec![root_pid];
    let mut seen: HashSet<u32> = HashSet::new();
    while let Some(pid) = stack.pop() {
        if !seen.insert(pid) {
            continue;
        }
        if !snapshot.infos_by_pid.contains_key(&pid) {
            continue;
        }
        out.push(pid);
        if let Some(children) = snapshot.children_by_ppid.get(&pid) {
            for child in children.iter().rev() {
                stack.push(*child);
            }
        }
    }
    out.sort_unstable();
    out
}

fn container_orphan_zombie_pids(snapshot: &SelectionSupervisorProcSnapshot) -> Vec<u32> {
    let mut out: Vec<u32> = snapshot
        .zombie_infos
        .iter()
        .filter(|info| info.ppid == 1)
        .map(|info| info.pid)
        .collect();
    out.sort_unstable();
    out
}

fn selection_status_hint(
    running: bool,
    present: bool,
    container_orphan_zombie_pids: &[u32],
) -> Option<String> {
    if !container_orphan_zombie_pids.is_empty() {
        let preview: Vec<String> = container_orphan_zombie_pids
            .iter()
            .take(8)
            .map(|pid| pid.to_string())
            .collect();
        let suffix = if container_orphan_zombie_pids.len() > preview.len() {
            format!(",... total={}", container_orphan_zombie_pids.len())
        } else {
            format!(" total={}", container_orphan_zombie_pids.len())
        };
        return Some(format!(
            "container has orphaned zombie processes; they are treated as stopped. pids=[{}]{}. likely pid1/runner did not reap exited children. ensure the daemonset runner waits background supervisors or run the container with an init process such as tini.",
            preview.join(","),
            suffix
        ));
    }
    if running && !present {
        return Some(
            "selection supervisor is attached but has no live child process yet; the workload is still starting or restarting inside the container."
                .to_string(),
        );
    }
    None
}

fn format_selection_status_debug(status: &SelectionSupervisorStatus) -> String {
    let mut parts = vec![
        format!("running={}", status.running),
        format!("present={}", status.present),
        format!("apply_id={:?}", status.apply_id.as_deref()),
    ];
    if !status.container_orphan_zombie_pids.is_empty() {
        parts.push(format!(
            "container_orphan_zombie_pids={:?}",
            status.container_orphan_zombie_pids.as_slice()
        ));
    }
    if let Some(hint) = status.status_hint.as_deref() {
        parts.push(format!("hint={}", hint));
    }
    parts.join(" ")
}

fn selection_status_from_live_supervisor(
    snapshot: &SelectionSupervisorProcSnapshot,
    supervisor: &LiveSelectionSupervisor,
    kind: WorkloadKind,
    name: String,
) -> SelectionSupervisorStatus {
    let process_count = pid_tree_members(snapshot, supervisor.pid()).len() as u32;
    let child_process_count = process_count.saturating_sub(1);
    let runtime_state = supervisor.runtime_state.clone();
    let container_orphan_zombie_pids = container_orphan_zombie_pids(snapshot);
    let status_hint = selection_status_hint(
        true,
        child_process_count > 0,
        container_orphan_zombie_pids.as_slice(),
    );
    SelectionSupervisorStatus {
        label: supervisor.label.clone(),
        pid: Some(supervisor.pid()),
        pgid: Some(supervisor.pgid()),
        running: true,
        present: child_process_count > 0,
        process_count,
        child_process_count,
        kind: Some(kind),
        name: Some(name),
        authority: runtime_state.as_ref().map(|v| v.authority.clone()),
        service_name: runtime_state.as_ref().map(|v| v.service_name.clone()),
        apply_id: runtime_state.as_ref().and_then(|v| v.apply_id.clone()),
        argv: runtime_state.as_ref().map(|v| v.argv.clone()),
        cwd: runtime_state.as_ref().and_then(|v| v.cwd.clone()),
        log_path: runtime_state
            .as_ref()
            .map(|v| display_runtime_log_path(v.log_path.as_str())),
        started_ts_ms: None,
        owner_ts_ms: Some(supervisor.owner_ts_ms),
        supervisor_start_time_ticks: Some(supervisor.start_time_ticks()),
        container_orphan_zombie_pids,
        status_hint,
    }
}

fn observe_selection_status_for_scope(
    kind: WorkloadKind,
    name: &str,
    authority: &str,
    scope_key: Option<&str>,
) -> anyhow::Result<SelectionSupervisorStatus> {
    let name = validate_workload_name_for_file(name)?;
    let authority = validate_workload_name_for_file(authority)?;
    let label = selection_supervisor_label_from_workload_name(kind, name.as_str())?;
    let snapshot = selection_supervisor_proc_snapshot()?;
    let owner = selection_owner_supervisor(&snapshot, label.as_str(), scope_key, None)?;
    let Some(owner) = owner else {
        let container_orphan_zombie_pids = container_orphan_zombie_pids(&snapshot);
        let status_hint =
            selection_status_hint(false, false, container_orphan_zombie_pids.as_slice());
        return Ok(SelectionSupervisorStatus {
            label,
            pid: None,
            pgid: None,
            running: false,
            present: false,
            process_count: 0,
            child_process_count: 0,
            kind: None,
            name: None,
            authority: Some(authority),
            service_name: None,
            apply_id: None,
            argv: None,
            cwd: None,
            log_path: None,
            started_ts_ms: None,
            owner_ts_ms: None,
            supervisor_start_time_ticks: None,
            container_orphan_zombie_pids,
            status_hint,
        });
    };
    let resolved_kind = owner.runtime_state.as_ref().map(|v| v.kind).unwrap_or(kind);
    let resolved_name = owner
        .runtime_state
        .as_ref()
        .map(|v| v.name.clone())
        .unwrap_or_else(|| name.to_string());
    let mut status =
        selection_status_from_live_supervisor(&snapshot, &owner, resolved_kind, resolved_name);
    status.label = label;
    if status.authority.is_none() {
        status.authority = Some(authority);
    }
    Ok(status)
}

fn observe_selection_status(
    kind: WorkloadKind,
    name: &str,
    authority: &str,
) -> anyhow::Result<SelectionSupervisorStatus> {
    observe_selection_status_for_scope(kind, name, authority, None)
}

fn observe_all_selection_statuses_for_snapshot(
    snapshot: &SelectionSupervisorProcSnapshot,
    scope_key: Option<&str>,
) -> anyhow::Result<Vec<SelectionSupervisorStatus>> {
    // English note: list_workloads/wait_delete_apply must derive owner and identity from one
    // snapshot. Re-snapshotting between "pick owner" and "read status" lets a newer supervisor
    // win owner_ts_ms while still lacking runtime_state, which falsely poisons teardown.
    let mut owners_by_label: BTreeMap<String, LiveSelectionSupervisor> = BTreeMap::new();
    for supervisor in live_selection_supervisors(snapshot, None, scope_key)? {
        let replace = match owners_by_label.get(supervisor.label.as_str()) {
            Some(current) if current.owner_ts_ms == supervisor.owner_ts_ms => {
                anyhow::bail!(
                    "selection supervisor owner_ts_ms collision label={} owner_ts_ms={} current_pid={} new_pid={}",
                    supervisor.label,
                    supervisor.owner_ts_ms,
                    current.pid(),
                    supervisor.pid()
                );
            }
            Some(current) => {
                selection_supervisor_sort_key(current) < selection_supervisor_sort_key(&supervisor)
            }
            None => true,
        };
        if replace {
            owners_by_label.insert(supervisor.label.clone(), supervisor);
        }
    }
    let mut out: Vec<SelectionSupervisorStatus> = Vec::new();
    for supervisor in owners_by_label.into_values() {
        let (kind, name) = match supervisor.runtime_state.as_ref() {
            Some(v) => (v.kind, v.name.clone()),
            None => workload_identity_from_selection_label(&supervisor.label)?,
        };
        let mut status = selection_status_from_live_supervisor(snapshot, &supervisor, kind, name);
        if status.authority.is_none() {
            let (_, authority) = workload_identity_from_selection_label(&supervisor.label)?;
            status.authority = Some(authority);
        }
        out.push(status);
    }
    out.sort_by(|left, right| {
        left.kind
            .cmp(&right.kind)
            .then(left.name.cmp(&right.name))
            .then(left.label.cmp(&right.label))
    });
    Ok(out)
}

fn observe_all_selection_statuses() -> anyhow::Result<Vec<SelectionSupervisorStatus>> {
    let snapshot = selection_supervisor_proc_snapshot()?;
    observe_all_selection_statuses_for_snapshot(&snapshot, None)
}

fn observe_apply_runtime_statuses_for_snapshot(
    apply_id: &str,
    snapshot: &SelectionSupervisorProcSnapshot,
    scope_key: Option<&str>,
) -> anyhow::Result<Vec<SelectionSupervisorStatus>> {
    let apply_id = apply_id.trim();
    if apply_id.is_empty() {
        anyhow::bail!("apply_id must be non-empty");
    }
    let mut out: Vec<SelectionSupervisorStatus> = Vec::new();
    for supervisor in live_selection_supervisors(snapshot, None, scope_key)? {
        let Some(runtime_state) = supervisor.runtime_state.as_ref() else {
            continue;
        };
        if runtime_state.apply_id.as_deref() != Some(apply_id) {
            continue;
        }
        let mut status = selection_status_from_live_supervisor(
            &snapshot,
            &supervisor,
            runtime_state.kind,
            runtime_state.name.clone(),
        );
        // English note:
        // - wait_delete_apply drains apply-owned workload runtime, not attached supervisor shells.
        // - A live supervisor with `present=false` only proves the authority process still exists;
        //   its managed child may already be gone because stop won the race or the runtime is in a
        //   restart gap.
        // - Apply teardown must therefore only report workloads whose managed child is still
        //   present. General status/attach convergence keeps using the broader attached semantics.
        if !status.present {
            continue;
        }
        if status.authority.is_none() {
            status.authority = Some(runtime_state.authority.clone());
        }
        out.push(status);
    }
    out.sort_by(|left, right| {
        left.kind
            .cmp(&right.kind)
            .then(left.name.cmp(&right.name))
            .then(left.owner_ts_ms.cmp(&right.owner_ts_ms))
            .then(left.pid.cmp(&right.pid))
    });
    Ok(out)
}

fn observe_apply_runtime_statuses(
    apply_id: &str,
) -> anyhow::Result<Vec<SelectionSupervisorStatus>> {
    let snapshot = selection_supervisor_proc_snapshot()?;
    observe_apply_runtime_statuses_for_snapshot(apply_id, &snapshot, None)
}

fn wait_for_selection_present_for_scope(
    kind: WorkloadKind,
    name: &str,
    authority: &str,
    scope_key: Option<&str>,
) -> anyhow::Result<()> {
    let deadline =
        Instant::now() + Duration::from_secs(OPS_SELECTION_SUPERVISOR_WAIT_PRESENT_TIMEOUT_SECONDS);
    let mut stable_started_at: Option<Instant> = None;
    while Instant::now() < deadline {
        let status = observe_selection_status_for_scope(kind, name, authority, scope_key)?;
        if status.present {
            if stable_started_at.is_none() {
                stable_started_at = Some(Instant::now());
            }
            if stable_started_at.unwrap().elapsed()
                >= Duration::from_secs(OPS_SELECTION_SUPERVISOR_WAIT_PRESENT_STABLE_SECONDS)
            {
                return Ok(());
            }
        } else {
            stable_started_at = None;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    let status = observe_selection_status_for_scope(kind, name, authority, scope_key)?;
    anyhow::bail!(
        "wait for selection present failed: label={} {}",
        selection_supervisor_label_from_workload_name(kind, name)?,
        format_selection_status_debug(&status)
    );
}

fn wait_for_selection_present(
    kind: WorkloadKind,
    name: &str,
    authority: &str,
) -> anyhow::Result<()> {
    wait_for_selection_present_for_scope(kind, name, authority, None)
}

fn wait_for_selection_attached_for_scope(
    kind: WorkloadKind,
    name: &str,
    authority: &str,
    scope_key: Option<&str>,
    apply_id: &str,
    owner_ts_ms: u64,
    argv: &[String],
    cwd: Option<&str>,
) -> anyhow::Result<SelectionSupervisorStatus> {
    let deadline =
        Instant::now() + Duration::from_secs(OPS_SELECTION_SUPERVISOR_WAIT_PRESENT_TIMEOUT_SECONDS);
    while Instant::now() < deadline {
        let status = observe_selection_status_for_scope(kind, name, authority, scope_key)?;
        if selection_status_matches_attached(&status, apply_id, owner_ts_ms, argv, cwd) {
            return Ok(status);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    let status = observe_selection_status_for_scope(kind, name, authority, scope_key)?;
    anyhow::bail!(
        "wait for selection attached failed: label={} {}",
        selection_supervisor_label_from_workload_name(kind, name)?,
        format_selection_status_debug(&status)
    );
}

fn wait_for_selection_attached(
    kind: WorkloadKind,
    name: &str,
    authority: &str,
    apply_id: &str,
    owner_ts_ms: u64,
    argv: &[String],
    cwd: Option<&str>,
) -> anyhow::Result<SelectionSupervisorStatus> {
    wait_for_selection_attached_for_scope(kind, name, authority, None, apply_id, owner_ts_ms, argv, cwd)
}

fn wait_for_selection_attached_without_present_for_scope(
    kind: WorkloadKind,
    name: &str,
    authority: &str,
    scope_key: Option<&str>,
    apply_id: &str,
    owner_ts_ms: u64,
    argv: &[String],
    cwd: Option<&str>,
) -> anyhow::Result<SelectionSupervisorStatus> {
    let deadline =
        Instant::now() + Duration::from_secs(OPS_SELECTION_SUPERVISOR_WAIT_PRESENT_TIMEOUT_SECONDS);
    while Instant::now() < deadline {
        let status = observe_selection_status_for_scope(kind, name, authority, scope_key)?;
        if selection_status_matches_attached(&status, apply_id, owner_ts_ms, argv, cwd)
            && !status.present
        {
            return Ok(status);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    let status = observe_selection_status_for_scope(kind, name, authority, scope_key)?;
    anyhow::bail!(
        "wait for selection attached without present failed: label={} {}",
        selection_supervisor_label_from_workload_name(kind, name)?,
        format_selection_status_debug(&status)
    );
}

fn wait_for_selection_attached_without_present(
    kind: WorkloadKind,
    name: &str,
    authority: &str,
    apply_id: &str,
    owner_ts_ms: u64,
    argv: &[String],
    cwd: Option<&str>,
) -> anyhow::Result<SelectionSupervisorStatus> {
    wait_for_selection_attached_without_present_for_scope(
        kind,
        name,
        authority,
        None,
        apply_id,
        owner_ts_ms,
        argv,
        cwd,
    )
}

fn wait_for_selection_absent_for_scope(
    kind: WorkloadKind,
    name: &str,
    authority: &str,
    scope_key: Option<&str>,
    require_apply_id: Option<&str>,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        let snapshot = selection_supervisor_proc_snapshot()?;
        let label = selection_supervisor_label_from_workload_name(kind, name)?;
        let remaining: Vec<LiveSelectionSupervisor> =
            live_selection_supervisors(&snapshot, Some(label.as_str()), scope_key)?
                .into_iter()
                .filter(|supervisor| match require_apply_id {
                    Some(apply_id) => supervisor
                        .runtime_state
                        .as_ref()
                        .and_then(|v| v.apply_id.as_deref())
                        .map(|running_apply_id| running_apply_id == apply_id)
                        .unwrap_or(false),
                    None => true,
                })
                .collect();
        if remaining.is_empty() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    let status = observe_selection_status_for_scope(kind, name, authority, scope_key)?;
    anyhow::bail!(
        "wait for selection absent failed: label={} {}",
        selection_supervisor_label_from_workload_name(kind, name)?,
        format_selection_status_debug(&status)
    );
}

fn wait_for_selection_absent(
    kind: WorkloadKind,
    name: &str,
    authority: &str,
    require_apply_id: Option<&str>,
) -> anyhow::Result<()> {
    wait_for_selection_absent_for_scope(kind, name, authority, None, require_apply_id)
}

fn wait_for_process_identity_absent(pid: u32, start_time_ticks: u64) -> anyhow::Result<()> {
    let deadline =
        Instant::now() + Duration::from_secs(OPS_SELECTION_SUPERVISOR_WAIT_PRESENT_TIMEOUT_SECONDS);
    while Instant::now() < deadline {
        match read_process_info_observation(pid)? {
            None => return Ok(()),
            Some(info) if info.start_time_ticks != start_time_ticks => return Ok(()),
            Some(_) => {}
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    match read_process_info_observation(pid)? {
        None => Ok(()),
        Some(info) => anyhow::bail!(
            "wait for process identity absent failed: pid={} expected_start_time_ticks={} actual_start_time_ticks={}",
            pid,
            start_time_ticks,
            info.start_time_ticks
        ),
    }
}

fn selection_status_matches_attached(
    status: &SelectionSupervisorStatus,
    apply_id: &str,
    owner_ts_ms: u64,
    argv: &[String],
    cwd: Option<&str>,
) -> bool {
    status.running
        && status.apply_id.as_deref() == Some(apply_id)
        && status.owner_ts_ms == Some(owner_ts_ms)
        && status.argv.as_deref() == Some(argv)
        && status.cwd.as_deref() == cwd
}

fn selection_status_matches_present(
    status: &SelectionSupervisorStatus,
    apply_id: &str,
    owner_ts_ms: u64,
    argv: &[String],
    cwd: Option<&str>,
) -> bool {
    selection_status_matches_attached(status, apply_id, owner_ts_ms, argv, cwd) && status.present
}

impl SupervisorBackedWorkloads {
    fn new(
        _hostworkdir: PathBuf,
        log_dir: PathBuf,
        supervisor_runtime: SelectionSupervisorRuntime,
    ) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&log_dir)
            .with_context(|| format!("create ops agent log dir: {}", log_dir.display()))?;
        Ok(Self {
            op_guard: std::sync::Mutex::new(()),
            log_dir,
            supervisor_runtime,
            scope_key: _hostworkdir.display().to_string(),
        })
    }

    fn get_status(&self, workload: &WorkloadId) -> StatusResp {
        let authority = workload.authority.trim();
        if authority.is_empty() {
            return StatusResp {
                ok: false,
                err: Some("authority must be non-empty".to_string()),
                running: false,
                present: None,
                apply_id: None,
                pid: None,
                exit_code: None,
                owner_ts_ms: None,
                authority: None,
                container_orphan_zombie_pids: Vec::new(),
                status_hint: None,
            };
        }
        match observe_selection_status_for_scope(
            workload.kind,
            &workload.name,
            authority,
            Some(self.scope_key.as_str()),
        ) {
            Ok(status) => StatusResp {
                ok: true,
                err: None,
                running: status.running,
                present: Some(status.present),
                apply_id: status.apply_id,
                pid: status.pid,
                exit_code: None,
                owner_ts_ms: status.owner_ts_ms,
                authority: status.authority,
                container_orphan_zombie_pids: status.container_orphan_zombie_pids,
                status_hint: status.status_hint,
            },
            Err(e) => StatusResp {
                ok: false,
                err: Some(format!("selection supervisor observation failed: {}", e)),
                running: false,
                present: None,
                apply_id: None,
                pid: None,
                exit_code: None,
                owner_ts_ms: None,
                authority: None,
                container_orphan_zombie_pids: Vec::new(),
                status_hint: None,
            },
        }
    }

    fn start(&self, req: StartReq) -> StartResp {
        let kind = req.kind;
        let name = req.name.trim().to_string();
        let authority = req.authority.trim().to_string();
        let service_name = req.service_name.trim().to_string();
        if name.is_empty() {
            return StartResp {
                ok: false,
                err: Some("name must be non-empty".to_string()),
                pid: None,
            };
        }
        if authority.is_empty() {
            return StartResp {
                ok: false,
                err: Some("authority must be non-empty".to_string()),
                pid: None,
            };
        }
        if service_name.is_empty() {
            return StartResp {
                ok: false,
                err: Some("service_name must be non-empty".to_string()),
                pid: None,
            };
        }
        if req.apply_id.trim().is_empty() {
            return StartResp {
                ok: false,
                err: Some("apply_id must be non-empty".to_string()),
                pid: None,
            };
        }
        if req.owner_ts_ms == 0 {
            return StartResp {
                ok: false,
                err: Some("owner_ts_ms must be > 0".to_string()),
                pid: None,
            };
        }
        if req.argv.is_empty() {
            return StartResp {
                ok: false,
                err: Some("argv must be non-empty".to_string()),
                pid: None,
            };
        }
        for (i, a) in req.argv.iter().enumerate() {
            if a.trim().is_empty() {
                return StartResp {
                    ok: false,
                    err: Some(format!("argv[{i}] must be non-empty")),
                    pid: None,
                };
            }
        }
        if let Some(cwd) = req.cwd.as_deref() {
            if cwd.trim().is_empty() {
                return StartResp {
                    ok: false,
                    err: Some("cwd must be non-empty when provided".to_string()),
                    pid: None,
                };
            }
        }
        let apply_id = req.apply_id.clone();
        let owner_ts_ms = req.owner_ts_ms;
        let wait_for_attached = req.wait_for_attached;
        let wait_for_present = req.wait_for_present;
        let start_started_at = Instant::now();
        let _op_guard = self.op_guard.lock().unwrap();
        let log_filename = match workload_log_filename(kind, &name) {
            Ok(v) => v,
            Err(e) => {
                return StartResp {
                    ok: false,
                    err: Some(format!("invalid workload name for log file: {}", e)),
                    pid: None,
                };
            }
        };
        let log_path = self.log_dir.join(log_filename);
        let label = selection_supervisor_label_from_workload_name(kind, &name)
            .unwrap_or_else(|_| format!("{}/{}", kind.as_str(), name));

        let current = match observe_selection_status_for_scope(
            kind,
            &name,
            &authority,
            Some(self.scope_key.as_str()),
        ) {
            Ok(v) => v,
            Err(e) => {
                return StartResp {
                    ok: false,
                    err: Some(format!("selection supervisor observation failed: {}", e)),
                    pid: None,
                };
            }
        };
        eprintln!(
            "[ops_agent:start] begin label={} apply_id={} owner_ts_ms={} wait_for_attached={} wait_for_present={} log_path={} current={}",
            label,
            apply_id,
            owner_ts_ms,
            wait_for_attached,
            wait_for_present,
            log_path.display(),
            format_selection_status_debug(&current)
        );
        if let Some(current_owner_ts_ms) = current.owner_ts_ms {
            if current_owner_ts_ms > owner_ts_ms
                && !phase1_overlap_with_applyless_owner_runtime(
                    &current,
                    apply_id.as_str(),
                    owner_ts_ms,
                )
            {
                eprintln!(
                    "[ops_agent:start] reject superseded label={} apply_id={} requested_owner_ts_ms={} current_owner_ts_ms={} elapsed_ms={}",
                    label,
                    apply_id,
                    owner_ts_ms,
                    current_owner_ts_ms,
                    start_started_at.elapsed().as_millis()
                );
                return StartResp {
                    ok: false,
                    err: Some(format!(
                        "requested generation is superseded by newer owner_ts_ms kind={} name={} apply_id={} current_apply_id={:?} requested_owner_ts_ms={} current_owner_ts_ms={}",
                        kind.as_str(),
                        name,
                        apply_id,
                        current.apply_id,
                        owner_ts_ms,
                        current_owner_ts_ms
                    )),
                    pid: current.pid,
                };
            }
        }

        if selection_status_matches_attached(
            &current,
            &apply_id,
            owner_ts_ms,
            &req.argv,
            req.cwd.as_deref(),
        ) {
            let final_status = if wait_for_present && !current.present {
                if let Err(e) = wait_for_selection_present_for_scope(
                    kind,
                    &name,
                    &authority,
                    Some(self.scope_key.as_str()),
                ) {
                    return StartResp {
                        ok: false,
                        err: Some(format!("selection supervisor wait-present failed: {}", e)),
                        pid: current.pid,
                    };
                }
                match observe_selection_status_for_scope(
                    kind,
                    &name,
                    &authority,
                    Some(self.scope_key.as_str()),
                ) {
                    Ok(v) => v,
                    Err(e) => {
                        return StartResp {
                            ok: false,
                            err: Some(format!(
                                "selection supervisor observation failed after wait-present: {}",
                                e
                            )),
                            pid: current.pid,
                        };
                    }
                }
            } else {
                current.clone()
            };
            let identity_matches = if wait_for_present {
                selection_status_matches_present(
                    &final_status,
                    &apply_id,
                    owner_ts_ms,
                    &req.argv,
                    req.cwd.as_deref(),
                )
            } else {
                selection_status_matches_attached(
                    &final_status,
                    &apply_id,
                    owner_ts_ms,
                    &req.argv,
                    req.cwd.as_deref(),
                )
            };
            if !identity_matches {
                eprintln!(
                    "[ops_agent:start] identity drift after reuse label={} apply_id={} elapsed_ms={} status={}",
                    label,
                    apply_id,
                    start_started_at.elapsed().as_millis(),
                    format_selection_status_debug(&final_status)
                );
                return StartResp {
                    ok: false,
                    err: Some(format!(
                        "embedded selection supervisor identity drifted while waiting present: running={} present={} apply_id={:?}",
                        final_status.running, final_status.present, final_status.apply_id
                    )),
                    pid: final_status.pid.or(current.pid),
                };
            }
            eprintln!(
                "[ops_agent:start] success reused label={} apply_id={} pid={:?} elapsed_ms={}",
                label,
                apply_id,
                final_status.pid.or(current.pid),
                start_started_at.elapsed().as_millis()
            );
            return StartResp {
                ok: true,
                err: None,
                pid: final_status.pid.or(current.pid),
            };
        }

        // English note:
        // - Every start request submits one detached supervisor generation intent.
        // - The live owner label plus owner_ts_ms decides whether it is an initial publish or a
        //   replacement; the caller does not branch on stale local file state anymore.
        eprintln!(
            "[ops_agent:start] submit detached label={} apply_id={} owner_ts_ms={} argv0={} cwd={:?}",
            label,
            apply_id,
            owner_ts_ms,
            req.argv.first().map(String::as_str).unwrap_or(""),
            req.cwd.as_deref()
        );
        let detached_pid = match self.supervisor_runtime.launch_detached(
            kind,
            &name,
            &authority,
            &service_name,
            &apply_id,
            owner_ts_ms,
            &req.argv,
            req.cwd.as_deref(),
            &log_path,
        ) {
            Ok(v) => v,
            Err(e) => {
                eprintln!(
                    "[ops_agent:start] submit detached failed label={} apply_id={} elapsed_ms={} err={}",
                    label,
                    apply_id,
                    start_started_at.elapsed().as_millis(),
                    e
                );
                return StartResp {
                    ok: false,
                    err: Some(format!("submit detached supervisor intent failed: {}", e)),
                    pid: current.pid,
                };
            }
        };
        eprintln!(
            "[ops_agent:start] detached submitted label={} apply_id={} detached_pid={} elapsed_ms={}",
            label,
            apply_id,
            detached_pid,
            start_started_at.elapsed().as_millis()
        );
        if !wait_for_attached {
            // English note:
            // - Returning here still means "phase 1 published successfully" only.
            // - The caller may intentionally avoid waiting inside a retiring ops_agent so it does
            //   not deadlock its own replacement during self-host handover.
            eprintln!(
                "[ops_agent:start] success detached publish-only label={} apply_id={} detached_pid={} elapsed_ms={}",
                label,
                apply_id,
                detached_pid,
                start_started_at.elapsed().as_millis()
            );
            return StartResp {
                ok: true,
                err: None,
                pid: Some(detached_pid),
            };
        }
        // English note:
        // - After detached supervisor submission succeeds, the requested generation becomes the
        //   current runtime candidate.
        // - A local wait/observe failure here only means this caller could not prove convergence
        //   within its observation window.
        // - Destructively stopping `require_apply_id=requested_apply` would let the new generation
        //   kill itself and break monotonic reconcile/apply_wait convergence.
        eprintln!(
            "[ops_agent:start] wait attached label={} apply_id={} detached_pid={}",
            label, apply_id, detached_pid
        );
        let attached_status = match wait_for_selection_attached_for_scope(
            kind,
            &name,
            &authority,
            Some(self.scope_key.as_str()),
            &apply_id,
            owner_ts_ms,
            &req.argv,
            req.cwd.as_deref(),
        ) {
            Ok(v) => v,
            Err(e) => {
                eprintln!(
                    "[ops_agent:start] wait attached failed label={} apply_id={} detached_pid={} elapsed_ms={} err={}",
                    label,
                    apply_id,
                    detached_pid,
                    start_started_at.elapsed().as_millis(),
                    e
                );
                return StartResp {
                    ok: false,
                    err: Some(format!("selection supervisor wait-attached failed: {}", e)),
                    pid: Some(detached_pid),
                };
            }
        };
        if wait_for_present {
            if let Err(e) = wait_for_selection_present_for_scope(
                kind,
                &name,
                &authority,
                Some(self.scope_key.as_str()),
            ) {
                eprintln!(
                    "[ops_agent:start] wait present failed label={} apply_id={} detached_pid={} elapsed_ms={} err={}",
                    label,
                    apply_id,
                    detached_pid,
                    start_started_at.elapsed().as_millis(),
                    e
                );
                return StartResp {
                    ok: false,
                    err: Some(format!("selection supervisor wait-present failed: {}", e)),
                    pid: Some(detached_pid),
                };
            }
        }
        let status = if wait_for_present {
            match observe_selection_status_for_scope(
                kind,
                &name,
                &authority,
                Some(self.scope_key.as_str()),
            ) {
                Ok(v) => v,
                Err(e) => {
                    return StartResp {
                        ok: false,
                        err: Some(format!(
                            "selection supervisor observation failed after start: {}",
                            e
                        )),
                        pid: Some(detached_pid),
                    };
                }
            }
        } else {
            attached_status
        };
        let identity_matches = if wait_for_present {
            selection_status_matches_present(
                &status,
                &apply_id,
                owner_ts_ms,
                &req.argv,
                req.cwd.as_deref(),
            )
        } else {
            selection_status_matches_attached(
                &status,
                &apply_id,
                owner_ts_ms,
                &req.argv,
                req.cwd.as_deref(),
            )
        };
        if !identity_matches {
            eprintln!(
                "[ops_agent:start] identity mismatch after start label={} apply_id={} detached_pid={} elapsed_ms={} status={}",
                label,
                apply_id,
                detached_pid,
                start_started_at.elapsed().as_millis(),
                format_selection_status_debug(&status)
            );
            return StartResp {
                ok: false,
                err: Some(format!(
                    "embedded selection supervisor did not match requested identity after start: running={} present={} apply_id={:?} argv_match={} cwd_match={}",
                    status.running,
                    status.present,
                    status.apply_id,
                    status.argv.as_deref() == Some(req.argv.as_slice()),
                    status.cwd.as_deref() == req.cwd.as_deref()
                )),
                pid: status.pid.or(Some(detached_pid)),
            };
        }

        eprintln!(
            "[ops_agent:start] success label={} apply_id={} pid={:?} detached_pid={} elapsed_ms={}",
            label,
            apply_id,
            status.pid.or(Some(detached_pid)),
            detached_pid,
            start_started_at.elapsed().as_millis()
        );
        StartResp {
            ok: true,
            err: None,
            pid: status.pid.or(Some(detached_pid)),
        }
    }

    fn delete_generation(
        &self,
        kind: WorkloadKind,
        name: &str,
        authority: &str,
        require_apply_id: Option<&str>,
    ) -> DeleteGenerationResp {
        let name = name.trim().to_string();
        let authority = authority.trim().to_string();
        if name.is_empty() {
            return DeleteGenerationResp {
                ok: false,
                err: Some("name must be non-empty".to_string()),
            };
        }
        if authority.is_empty() {
            return DeleteGenerationResp {
                ok: false,
                err: Some("authority must be non-empty".to_string()),
            };
        }

        let current = {
            let _op_guard = self.op_guard.lock().unwrap();
            match observe_selection_status_for_scope(
                kind,
                &name,
                &authority,
                Some(self.scope_key.as_str()),
            ) {
                Ok(v) => v,
                Err(e) => {
                    return DeleteGenerationResp {
                        ok: false,
                        err: Some(format!(
                            "selection supervisor observation failed before delete_generation: {}",
                            e
                        )),
                    };
                }
            }
        };
        let require_apply_id = require_apply_id
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_string)
            .or_else(|| {
                current
                    .apply_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .map(str::to_string)
            });
        if !current.running {
            return DeleteGenerationResp {
                ok: true,
                err: None,
            };
        }
        if let Err(e) = self
            .supervisor_runtime
            .stop(kind, &name, true, require_apply_id.as_deref())
        {
            return DeleteGenerationResp {
                ok: false,
                err: Some(format!("selection supervisor stop failed: {}", e)),
            };
        }
        if let Err(e) = wait_for_selection_absent_for_scope(
            kind,
            &name,
            &authority,
            Some(self.scope_key.as_str()),
            require_apply_id.as_deref(),
        ) {
            return DeleteGenerationResp {
                ok: false,
                err: Some(format!("wait for selection absence failed: {}", e)),
            };
        }
        DeleteGenerationResp {
            ok: true,
            err: None,
        }
    }

    fn list_workloads(&self) -> anyhow::Result<Vec<WorkloadStatusSummary>> {
        let mut out: Vec<WorkloadStatusSummary> = Vec::new();
        let snapshot = selection_supervisor_proc_snapshot()?;
        for status in observe_all_selection_statuses_for_snapshot(
            &snapshot,
            Some(self.scope_key.as_str()),
        )? {
            let kind = status.kind.with_context(|| {
                format!(
                    "selection supervisor list item missing kind: label={}",
                    status.label
                )
            })?;
            let name = status.name.with_context(|| {
                format!(
                    "selection supervisor list item missing name: label={}",
                    status.label
                )
            })?;
            out.push(WorkloadStatusSummary {
                kind,
                name: name.clone(),
                authority: status.authority.clone().unwrap_or_else(|| name.clone()),
                running: status.running,
                present: Some(status.present),
                apply_id: status.apply_id,
                pid: status.pid,
                exit_code: None,
                owner_ts_ms: status.owner_ts_ms,
                err: None,
                container_orphan_zombie_pids: status.container_orphan_zombie_pids,
                status_hint: status.status_hint,
            });
        }
        Ok(out)
    }

    fn list_apply_runtime(&self, apply_id: &str) -> anyhow::Result<Vec<WorkloadStatusSummary>> {
        let mut out: Vec<WorkloadStatusSummary> = Vec::new();
        let snapshot = selection_supervisor_proc_snapshot()?;
        for status in observe_apply_runtime_statuses_for_snapshot(
            apply_id,
            &snapshot,
            Some(self.scope_key.as_str()),
        )? {
            let kind = status.kind.with_context(|| {
                format!(
                    "apply runtime status missing kind: label={} apply_id={}",
                    status.label, apply_id
                )
            })?;
            let name = status.name.with_context(|| {
                format!(
                    "apply runtime status missing name: label={} apply_id={}",
                    status.label, apply_id
                )
            })?;
            out.push(WorkloadStatusSummary {
                kind,
                name: name.clone(),
                authority: status.authority.clone().unwrap_or_else(|| name.clone()),
                running: status.running,
                present: Some(status.present),
                apply_id: status.apply_id,
                pid: status.pid,
                exit_code: None,
                owner_ts_ms: status.owner_ts_ms,
                err: None,
                container_orphan_zombie_pids: status.container_orphan_zombie_pids,
                status_hint: status.status_hint,
            });
        }
        Ok(out)
    }
}

pub struct OpsAgent {
    fw: Arc<Framework>,
    workloads: Arc<SupervisorBackedWorkloads>,
    log_dir: PathBuf,
}

impl OpsAgent {
    fn new(
        fw: Arc<Framework>,
        workloads: Arc<SupervisorBackedWorkloads>,
        log_dir: PathBuf,
    ) -> Self {
        Self {
            fw,
            workloads,
            log_dir,
        }
    }

    pub fn register_handlers(&self) {
        let start = Arc::new(StartHandler {
            workloads: self.workloads.clone(),
        });
        let status = Arc::new(StatusHandler {
            workloads: self.workloads.clone(),
        });
        let delete_generation = Arc::new(DeleteGenerationHandler {
            workloads: self.workloads.clone(),
        });
        let list_workloads = Arc::new(ListWorkloadsHandler {
            workloads: self.workloads.clone(),
        });
        let list_apply_runtime = Arc::new(ListApplyRuntimeHandler {
            workloads: self.workloads.clone(),
        });
        let ready = Arc::new(ReadyHandler {});
        let read_workload_log = Arc::new(ReadWorkloadLogChunkHandler {
            log_dir: self.log_dir.clone(),
        });

        let p2p_view = self.fw.p2p_view();
        let p2p = p2p_view.p2p_module();
        user_rpc_register_handler(p2p, "fluxon_ops/start".to_string(), start);
        user_rpc_register_handler(p2p, "fluxon_ops/status".to_string(), status);
        user_rpc_register_handler(
            p2p,
            "fluxon_ops/delete_generation".to_string(),
            delete_generation,
        );
        user_rpc_register_handler(p2p, "fluxon_ops/list_workloads".to_string(), list_workloads);
        user_rpc_register_handler(
            p2p,
            "fluxon_ops/list_apply_runtime".to_string(),
            list_apply_runtime,
        );
        user_rpc_register_handler(p2p, "fluxon_ops/ready".to_string(), ready);
        user_rpc_register_handler(
            p2p,
            "fluxon_ops/read_workload_log".to_string(),
            read_workload_log,
        );
    }
}

#[derive(Clone)]
struct StartHandler {
    workloads: Arc<SupervisorBackedWorkloads>,
}

impl UserRpcHandler for StartHandler {
    fn handle(&self, _from_node: NodeID, payload: &[u8]) -> Result<Vec<u8>, KvError> {
        let req: StartReq = serde_json::from_slice(payload).map_err(|e| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!("invalid start request json: {}", e),
            })
        })?;

        let resp = self.workloads.start(req);
        Ok(serde_json::to_vec(&resp).unwrap())
    }
}

#[derive(Clone)]
struct StatusHandler {
    workloads: Arc<SupervisorBackedWorkloads>,
}

impl UserRpcHandler for StatusHandler {
    fn handle(&self, _from_node: NodeID, payload: &[u8]) -> Result<Vec<u8>, KvError> {
        let req: StatusReq = serde_json::from_slice(payload).map_err(|e| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!("invalid status request json: {}", e),
            })
        })?;

        let name = req.name.trim();
        if name.is_empty() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "name must be non-empty".to_string(),
            }));
        }
        let authority = req.authority.trim();
        if authority.is_empty() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "authority must be non-empty".to_string(),
            }));
        }

        let resp = self.workloads.get_status(&WorkloadId::new(
            req.kind,
            name.to_string(),
            authority.to_string(),
        ));
        Ok(serde_json::to_vec(&resp).unwrap())
    }
}

#[derive(Clone)]
struct DeleteGenerationHandler {
    workloads: Arc<SupervisorBackedWorkloads>,
}

impl UserRpcHandler for DeleteGenerationHandler {
    fn handle(&self, _from_node: NodeID, payload: &[u8]) -> Result<Vec<u8>, KvError> {
        let req: DeleteGenerationReq = serde_json::from_slice(payload).map_err(|e| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!("invalid delete_generation request json: {}", e),
            })
        })?;

        let resp = self.workloads.delete_generation(
            req.kind,
            &req.name,
            &req.authority,
            req.require_apply_id.as_deref(),
        );
        Ok(serde_json::to_vec(&resp).unwrap())
    }
}

#[derive(Clone)]
struct ListWorkloadsHandler {
    workloads: Arc<SupervisorBackedWorkloads>,
}

impl UserRpcHandler for ListWorkloadsHandler {
    fn handle(&self, _from_node: NodeID, payload: &[u8]) -> Result<Vec<u8>, KvError> {
        let _: ListWorkloadsReq = serde_json::from_slice(payload).map_err(|e| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!("invalid list_workloads request json: {}", e),
            })
        })?;

        let resp = match self.workloads.list_workloads() {
            Ok(workloads) => ListWorkloadsResp {
                ok: true,
                err: None,
                workloads: Some(workloads),
            },
            Err(e) => ListWorkloadsResp {
                ok: false,
                err: Some(format!("list supervisor-backed workloads failed: {}", e)),
                workloads: None,
            },
        };
        Ok(serde_json::to_vec(&resp).unwrap())
    }
}

#[derive(Clone)]
struct ListApplyRuntimeHandler {
    workloads: Arc<SupervisorBackedWorkloads>,
}

impl UserRpcHandler for ListApplyRuntimeHandler {
    fn handle(&self, _from_node: NodeID, payload: &[u8]) -> Result<Vec<u8>, KvError> {
        let req: ListApplyRuntimeReq = serde_json::from_slice(payload).map_err(|e| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!("invalid list_apply_runtime request json: {}", e),
            })
        })?;

        let resp = match self.workloads.list_apply_runtime(&req.apply_id) {
            Ok(workloads) => ListApplyRuntimeResp {
                ok: true,
                err: None,
                workloads: Some(workloads),
            },
            Err(e) => ListApplyRuntimeResp {
                ok: false,
                err: Some(format!("list apply runtime failed: {}", e)),
                workloads: None,
            },
        };
        Ok(serde_json::to_vec(&resp).unwrap())
    }
}

#[derive(Clone)]
struct ReadyHandler {}

impl UserRpcHandler for ReadyHandler {
    fn handle(&self, _from_node: NodeID, payload: &[u8]) -> Result<Vec<u8>, KvError> {
        let _: ReadyReq = serde_json::from_slice(payload).map_err(|e| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!("invalid ready request json: {}", e),
            })
        })?;

        Ok(serde_json::to_vec(&ReadyResp {
            ok: true,
            err: None,
        })
        .unwrap())
    }
}

#[derive(Clone)]
struct ReadWorkloadLogChunkHandler {
    log_dir: PathBuf,
}

impl UserRpcHandler for ReadWorkloadLogChunkHandler {
    fn handle(&self, _from_node: NodeID, payload: &[u8]) -> Result<Vec<u8>, KvError> {
        let req: ReadWorkloadLogReq = serde_json::from_slice(payload).map_err(|e| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!("invalid read_workload_log request json: {}", e),
            })
        })?;

        let max_bytes = match req.max_bytes {
            Some(v) => Some(ensure_positive_u64(v, "max_bytes")?),
            None => None,
        };
        let max_bytes_usize = match max_bytes {
            Some(v) => Some(ensure_u64_fits_usize(v, "max_bytes").map_err(|e| {
                KvError::Api(ApiError::InvalidArgument {
                    detail: format!("{}", e),
                })
            })?),
            None => None,
        };

        let log_filename = match workload_log_filename(req.kind, &req.name) {
            Ok(v) => v,
            Err(e) => {
                let resp = ReadWorkloadLogResp {
                    ok: false,
                    err: Some(format!("{}", e)),
                    file_size: None,
                    start_offset: None,
                    end_offset: None,
                    start_cursor: None,
                    end_cursor: None,
                    text: None,
                };
                return Ok(serde_json::to_vec(&resp).unwrap());
            }
        };

        let logical_path = self.log_dir.join(log_filename);
        let make_err_resp = |err: String, file_size: Option<u64>| ReadWorkloadLogResp {
            ok: false,
            err: Some(err),
            file_size,
            start_offset: None,
            end_offset: None,
            start_cursor: None,
            end_cursor: None,
            text: None,
        };

        let (path, shard) = match req.cursor.as_ref() {
            Some(cursor) => match workload_log_path_for_shard(&logical_path, &cursor.shard) {
                Ok(path) => (path, cursor.shard.clone()),
                Err(e) => {
                    let resp = make_err_resp(format!("{}", e), None);
                    return Ok(serde_json::to_vec(&resp).unwrap());
                }
            },
            None => {
                let Some(path) = resolve_readable_log_path(&logical_path) else {
                    let resp = make_err_resp(
                        format!("log file is not available yet: logical_path={}", logical_path.display()),
                        None,
                    );
                    return Ok(serde_json::to_vec(&resp).unwrap());
                };
                let shard = match workload_log_shard_identity_from_path(&logical_path, &path) {
                    Ok(v) => v,
                    Err(e) => {
                        let resp = make_err_resp(format!("{}", e), None);
                        return Ok(serde_json::to_vec(&resp).unwrap());
                    }
                };
                (path, shard)
            }
        };
        let meta = match std::fs::metadata(&path) {
            Ok(v) => v,
            Err(e) => {
                let resp = make_err_resp(
                    format!("stat log failed: path={} err={}", path.display(), e),
                    None,
                );
                return Ok(serde_json::to_vec(&resp).unwrap());
            }
        };

        let file_size = meta.len();
        let (start, end, start_cursor, end_cursor, effective_path, effective_file_size) =
            match req.direction {
                LogReadDirection::Forward => {
                    if let Some(cursor) = req.cursor.as_ref() {
                        if cursor.offset > file_size {
                            let resp = make_err_resp(
                                format!(
                                    "cursor out of range: shard={} cursor={} file_size={}",
                                    cursor.shard, cursor.offset, file_size
                                ),
                                Some(file_size),
                            );
                            return Ok(serde_json::to_vec(&resp).unwrap());
                        }
                        let mut effective_path = path.clone();
                        let mut effective_shard = shard.clone();
                        let mut effective_file_size = file_size;
                        let mut start = cursor.offset;
                        if cursor.offset == file_size {
                            if let Ok(Some(next_shard)) =
                                workload_log_next_shard(&logical_path, &cursor.shard)
                            {
                                let next_path = match workload_log_path_for_shard(&logical_path, &next_shard) {
                                    Ok(v) => v,
                                    Err(e) => {
                                        let resp = make_err_resp(format!("{}", e), Some(file_size));
                                        return Ok(serde_json::to_vec(&resp).unwrap());
                                    }
                                };
                                match std::fs::metadata(&next_path) {
                                    Ok(next_meta) => {
                                        effective_file_size = next_meta.len();
                                        effective_path = next_path;
                                        effective_shard = next_shard;
                                        start = 0;
                                    }
                                    Err(e) => {
                                        let resp = make_err_resp(
                                            format!(
                                                "stat next log shard failed: path={} err={}",
                                                next_path.display(),
                                                e
                                            ),
                                            Some(file_size),
                                        );
                                        return Ok(serde_json::to_vec(&resp).unwrap());
                                    }
                                }
                            } else if let Ok(Some(latest_shard)) =
                                workload_log_latest_shard_identity(&logical_path)
                            {
                                if latest_shard != cursor.shard {
                                    let latest_path =
                                        match workload_log_path_for_shard(&logical_path, &latest_shard) {
                                            Ok(v) => v,
                                            Err(e) => {
                                                let resp = make_err_resp(format!("{}", e), Some(file_size));
                                                return Ok(serde_json::to_vec(&resp).unwrap());
                                            }
                                        };
                                    match std::fs::metadata(&latest_path) {
                                        Ok(latest_meta) => {
                                            effective_file_size = latest_meta.len();
                                            effective_path = latest_path;
                                            effective_shard = latest_shard;
                                            start = 0;
                                        }
                                        Err(e) => {
                                            let resp = make_err_resp(
                                                format!(
                                                    "stat latest log shard failed: path={} err={}",
                                                    latest_path.display(),
                                                    e
                                                ),
                                                Some(file_size),
                                            );
                                            return Ok(serde_json::to_vec(&resp).unwrap());
                                        }
                                    }
                                }
                            }
                        }
                        let end = match max_bytes {
                            Some(max_bytes) => {
                                std::cmp::min(effective_file_size, start.saturating_add(max_bytes))
                            }
                            None => effective_file_size,
                        };
                        (
                            start,
                            end,
                            Some(WorkloadLogCursor {
                                shard: effective_shard.clone(),
                                offset: start,
                            }),
                            Some(WorkloadLogCursor {
                                shard: effective_shard.clone(),
                                offset: end,
                            }),
                            effective_path,
                            effective_file_size,
                        )
                    } else {
                        let end = file_size;
                        let start = match max_bytes {
                            Some(max_bytes) => end.saturating_sub(max_bytes),
                            None => 0,
                        };
                        (
                            start,
                            end,
                            Some(WorkloadLogCursor {
                                shard: shard.clone(),
                                offset: start,
                            }),
                            Some(WorkloadLogCursor {
                                shard: shard.clone(),
                                offset: end,
                            }),
                            path.clone(),
                            file_size,
                        )
                    }
                }
                LogReadDirection::Backward => {
                    let Some(cursor) = req.cursor.as_ref() else {
                        let resp = make_err_resp(
                            "cursor is required for Backward reads".to_string(),
                            Some(file_size),
                        );
                        return Ok(serde_json::to_vec(&resp).unwrap());
                    };
                    if cursor.offset > file_size {
                        let resp = make_err_resp(
                            format!(
                                "cursor out of range: shard={} cursor={} file_size={}",
                                cursor.shard, cursor.offset, file_size
                            ),
                            Some(file_size),
                        );
                        return Ok(serde_json::to_vec(&resp).unwrap());
                    }
                    let mut effective_path = path.clone();
                    let mut effective_shard = shard.clone();
                    let mut effective_file_size = file_size;
                    let mut end = cursor.offset;
                    if cursor.offset == 0 {
                        if let Ok(Some(prev_shard)) =
                            workload_log_previous_shard(&logical_path, &cursor.shard)
                        {
                            let prev_path = match workload_log_path_for_shard(&logical_path, &prev_shard) {
                                Ok(v) => v,
                                Err(e) => {
                                    let resp = make_err_resp(format!("{}", e), Some(file_size));
                                    return Ok(serde_json::to_vec(&resp).unwrap());
                                }
                            };
                            match std::fs::metadata(&prev_path) {
                                Ok(prev_meta) => {
                                    effective_file_size = prev_meta.len();
                                    effective_path = prev_path;
                                    effective_shard = prev_shard;
                                    end = effective_file_size;
                                }
                                Err(e) => {
                                    let resp = make_err_resp(
                                        format!(
                                            "stat previous log shard failed: path={} err={}",
                                            prev_path.display(),
                                            e
                                        ),
                                        Some(file_size),
                                    );
                                    return Ok(serde_json::to_vec(&resp).unwrap());
                                }
                            }
                        }
                    }
                    let start = match max_bytes {
                        Some(max_bytes) => end.saturating_sub(max_bytes),
                        None => 0,
                    };
                    (
                        start,
                        end,
                        Some(WorkloadLogCursor {
                            shard: effective_shard.clone(),
                            offset: start,
                        }),
                        Some(WorkloadLogCursor {
                            shard: effective_shard.clone(),
                            offset: end,
                        }),
                        effective_path,
                        effective_file_size,
                    )
                }
            };

        if end < start {
            let resp = make_err_resp(
                format!("internal error: end < start: start={} end={}", start, end),
                Some(effective_file_size),
            );
            return Ok(serde_json::to_vec(&resp).unwrap());
        }

        let len_u64 = end - start;
        let len = ensure_u64_fits_usize(len_u64, "read_len").map_err(|e| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!("{}", e),
            })
        })?;
        if let Some(max_bytes_usize) = max_bytes_usize {
            if len > max_bytes_usize {
                let resp = make_err_resp(
                    format!(
                        "internal error: computed read_len exceeds max_bytes: read_len={} max_bytes={}",
                        len, max_bytes_usize
                    ),
                    Some(effective_file_size),
                );
                return Ok(serde_json::to_vec(&resp).unwrap());
            }
        }

        let mut f = match std::fs::File::open(&effective_path) {
            Ok(v) => v,
            Err(e) => {
                let resp = make_err_resp(
                    format!("open log failed: path={} err={}", effective_path.display(), e),
                    Some(effective_file_size),
                );
                return Ok(serde_json::to_vec(&resp).unwrap());
            }
        };

        if let Err(e) = std::io::Seek::seek(&mut f, std::io::SeekFrom::Start(start)) {
            let resp = make_err_resp(
                format!("seek log failed: path={} err={}", effective_path.display(), e),
                Some(effective_file_size),
            );
            return Ok(serde_json::to_vec(&resp).unwrap());
        }

        let mut buf: Vec<u8> = vec![0; len];
        if let Err(e) = std::io::Read::read_exact(&mut f, &mut buf) {
            let resp = make_err_resp(
                format!("read log failed: path={} err={}", effective_path.display(), e),
                Some(effective_file_size),
            );
            return Ok(serde_json::to_vec(&resp).unwrap());
        }

        // Causal chain:
        // - The controller reads arbitrary byte ranges (cursor-based) for tail + lazy-load.
        // - A byte range may split a UTF-8 rune at the boundary.
        // - Lossy decode avoids hard failures while keeping the log viewer responsive.
        let text = String::from_utf8_lossy(&buf).to_string();

        let resp = ReadWorkloadLogResp {
            ok: true,
            err: None,
            file_size: Some(effective_file_size),
            start_offset: Some(start),
            end_offset: Some(end),
            start_cursor,
            end_cursor,
            text: Some(text),
        };
        Ok(serde_json::to_vec(&resp).unwrap())
    }
}

#[derive(Clone)]
struct ControllerAgentDesiredHandler {
    desired: Arc<DesiredStore>,
}

impl UserRpcHandler for ControllerAgentDesiredHandler {
    fn handle(&self, _from_node: NodeID, payload: &[u8]) -> Result<Vec<u8>, KvError> {
        let req: AgentDesiredReq = serde_json::from_slice(payload).map_err(|e| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!("invalid agent_desired request json: {}", e),
            })
        })?;

        let instance_key = req.instance_key.trim();
        if instance_key.is_empty() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "instance_key must be non-empty".to_string(),
            }));
        }

        let resp = controller_agent_desired_snapshot_blocking(self.desired.as_ref(), instance_key);
        Ok(serde_json::to_vec(&resp).unwrap())
    }
}

pub fn parse_controller_config_yaml(doc: &str) -> anyhow::Result<ControllerConfigYaml> {
    let mut cfg: ControllerConfigYaml = serde_yaml::from_str(doc)
        .map_err(|e| anyhow::anyhow!("parse controller config yaml failed: {}", e))?;

    if cfg.panel.max_body_bytes == 0 {
        anyhow::bail!("panel.max_body_bytes must be > 0");
    }
    let _ = ensure_u64_fits_usize(cfg.panel.max_body_bytes, "panel.max_body_bytes")?;
    cfg.panel.auth.username =
        validate_username_for_basic_auth(&cfg.panel.auth.username, "panel.auth.username")?;
    cfg.panel.auth.password =
        validate_password_no_whitespace(&cfg.panel.auth.password, "panel.auth.password")?;

    if cfg.reconcile.interval_ms == 0 {
        anyhow::bail!("reconcile.interval_ms must be > 0");
    }

    Ok(cfg)
}

fn ops_fluxon_cli_proxy_desc_etcd_key(cluster_name: &str) -> String {
    // English note: keep this etcd key format stable because it is published by fluxon_ops
    // and consumed by fluxon_cli as a generic "registered panel proxy" descriptor.
    fluxon_cli_proxy_desc_etcd_key_v2(OPS_SERVICE_NAME, cluster_name)
}

// English note: keep `authorization` forwarded end-to-end because both the ops panel and
// embedded Fluxon panels rely on browser-managed Basic auth over proxied URLs.
const PANEL_PROXY_SKIP_REQ_HEADERS: [&str; 9] = [
    "connection",
    "proxy-connection",
    "keep-alive",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "host",
];

const PANEL_PROXY_SKIP_RESP_HEADERS: [&str; 8] = [
    "connection",
    "proxy-connection",
    "keep-alive",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "content-length",
];

fn should_skip_panel_proxy_header(name: &str, skip: &[&str]) -> bool {
    skip.iter().any(|h| name.eq_ignore_ascii_case(h))
}

fn register_controller_panel_proxy_userrpc_handler(
    fw: &Framework,
    init: Arc<ControllerInitState>,
) -> anyhow::Result<()> {
    // English note:
    // - fluxon_cli proxies registered panels via a descriptor stored in etcd.
    // - ops_controller exposes the panel surface via P2P RPC only; it does not bind a local HTTP port.
    //
    // Causal chain:
    // - fluxon_cli (embedded in KV master) sends HttpPanelProxyReq to node_id over P2P.
    // - ops_controller receives it here and dispatches to its in-process request router.
    // - This keeps the transport explicit (p2p_rpc) and avoids implicit HTTP fallbacks.

    let handler: fluxon_proxy::PanelProxyHandler = Arc::new(move |req| {
        let init = init.clone();
        Box::pin(async move {
            let uri: hyper::Uri = match req.path_and_query.parse() {
                Ok(v) => v,
                Err(e) => {
                    return Ok(PanelProxyResp {
                        status: StatusCode::BAD_REQUEST.as_u16(),
                        headers: Vec::new(),
                        body: format!("invalid uri: {}", e).into_bytes(),
                    });
                }
            };

            let method = match req.method {
                PanelProxyMethod::Get => Method::GET,
                PanelProxyMethod::Head => Method::HEAD,
                PanelProxyMethod::Post => Method::POST,
                PanelProxyMethod::Put => Method::PUT,
                PanelProxyMethod::Delete => Method::DELETE,
                PanelProxyMethod::Options => Method::OPTIONS,
                PanelProxyMethod::Patch => Method::PATCH,
            };

            let mut rb = Request::builder().method(method).uri(uri);
            for kv in &req.headers {
                if should_skip_panel_proxy_header(&kv.k, &PANEL_PROXY_SKIP_REQ_HEADERS) {
                    continue;
                }
                let name = match hyper::header::HeaderName::from_bytes(kv.k.as_bytes()) {
                    Ok(v) => v,
                    Err(e) => {
                        return Ok(PanelProxyResp {
                            status: StatusCode::BAD_REQUEST.as_u16(),
                            headers: Vec::new(),
                            body: format!("invalid request header name '{}': {}", kv.k, e)
                                .into_bytes(),
                        });
                    }
                };
                let value = match hyper::header::HeaderValue::from_str(&kv.v) {
                    Ok(v) => v,
                    Err(e) => {
                        return Ok(PanelProxyResp {
                            status: StatusCode::BAD_REQUEST.as_u16(),
                            headers: Vec::new(),
                            body: format!("invalid request header value for '{}': {}", kv.k, e)
                                .into_bytes(),
                        });
                    }
                };
                rb = rb.header(name, value);
            }

            let req2 = match rb.body(Body::from(req.body.clone())) {
                Ok(v) => v,
                Err(e) => {
                    return Ok(PanelProxyResp {
                        status: StatusCode::BAD_REQUEST.as_u16(),
                        headers: Vec::new(),
                        body: format!("invalid proxy request: {}", e).into_bytes(),
                    });
                }
            };

            let resp = handle_controller_req_with_init(init.clone(), req2)
                .await
                .unwrap();
            let status = resp.status();
            let mut headers: Vec<HeaderKv> = Vec::new();
            for (k, v) in resp.headers().iter() {
                let name = k.as_str();
                if should_skip_panel_proxy_header(name, &PANEL_PROXY_SKIP_RESP_HEADERS) {
                    continue;
                }
                let Ok(vs) = v.to_str() else {
                    continue;
                };
                headers.push(HeaderKv {
                    k: name.to_string(),
                    v: vs.to_string(),
                });
            }

            let body = match hyper::body::to_bytes(resp.into_body()).await {
                Ok(v) => v.to_vec(),
                Err(e) => {
                    return Ok(PanelProxyResp {
                        status: StatusCode::BAD_GATEWAY.as_u16(),
                        headers: Vec::new(),
                        body: format!("panel proxy read body failed: {}", e).into_bytes(),
                    });
                }
            };

            Ok(PanelProxyResp {
                status: status.as_u16(),
                headers,
                body,
            })
        })
    });

    fluxon_proxy::register_panel_proxy_handler_on_userrpc(fw.p2p_view().p2p_module(), handler);

    Ok(())
}

pub fn parse_agent_config_yaml(doc: &str) -> anyhow::Result<AgentConfigYaml> {
    let mut cfg: AgentConfigYaml = serde_yaml::from_str(doc)
        .map_err(|e| anyhow::anyhow!("parse agent config yaml failed: {}", e))?;
    let controller_instance_key = cfg.controller_instance_key.trim();
    if controller_instance_key.is_empty() {
        anyhow::bail!("agent config controller_instance_key must be non-empty");
    }
    cfg.controller_instance_key = controller_instance_key.to_string();
    let hostworkdir = cfg.hostworkdir.trim();
    if hostworkdir.is_empty() {
        anyhow::bail!("agent config hostworkdir must be non-empty");
    }
    let hostworkdir_path = Path::new(hostworkdir);
    if !hostworkdir_path.is_absolute() {
        anyhow::bail!(
            "agent config hostworkdir must be an absolute path: {}",
            hostworkdir
        );
    }
    cfg.hostworkdir = hostworkdir_path.display().to_string();

    Ok(cfg)
}

fn controller_agent_delete_workloads_from_apply_records(
    apply_records: &[DeployApplyRecord],
    instance_key: &str,
) -> anyhow::Result<Vec<AgentDeleteWorkload>> {
    let mut out: BTreeMap<String, AgentDeleteWorkload> = BTreeMap::new();
    for apply_rec in apply_records.iter() {
        let phase = apply_lifecycle_phase_normalized(apply_rec.lifecycle_phase);
        if phase == ApplyLifecyclePhase::Running {
            continue;
        }
        let desired_workloads = desired_workloads_from_apply_record(apply_rec)?;
        for desired_workload in desired_workloads.into_iter() {
            let target_matches = desired_workload.targets.iter().any(|target| {
                match agent_instance_key_from_node_name(target) {
                    Ok(v) => v == instance_key,
                    Err(e) => {
                        eprintln!(
                            "[ops_controller:agent_desired] skip invalid delete target mapping apply_id={} kind={} name={} target={} err={}",
                            apply_rec.id,
                            desired_workload.kind.as_str(),
                            desired_workload.name,
                            target,
                            e
                        );
                        false
                    }
                }
            });
            if !target_matches {
                continue;
            }
            let key =
                delete_workload_key(&apply_rec.id, desired_workload.kind, &desired_workload.name);
            out.insert(
                key,
                AgentDeleteWorkload {
                    kind: desired_workload.kind,
                    name: desired_workload.name.clone(),
                    authority: desired_workload_authority(&desired_workload)?,
                    atomic_group: desired_workload.atomic_group,
                    apply_id: apply_rec.id.clone(),
                    phase,
                    phase_updated_ts_ms: apply_rec.lifecycle_phase_updated_ts_ms,
                },
            );
        }
    }
    Ok(out.into_values().collect())
}

fn controller_agent_desired_snapshot_from_apply_phases(
    desired_snapshot: &[DesiredWorkload],
    instance_key: &str,
    apply_phases: &HashMap<String, ApplyLifecyclePhase>,
    delete_workloads: Vec<AgentDeleteWorkload>,
) -> AgentDesiredListResp {
    let mut desired_keys: BTreeMap<String, WorkloadId> = BTreeMap::new();
    let mut workloads: BTreeMap<String, AgentDesiredWorkload> = BTreeMap::new();

    for desired_workload in desired_snapshot.iter() {
        let target_matches = desired_workload.targets.iter().any(|target| {
            match agent_instance_key_from_node_name(target) {
                Ok(v) => v == instance_key,
                Err(e) => {
                    eprintln!(
                        "[ops_controller:agent_desired] skip invalid target mapping kind={} name={} target={} err={}",
                        desired_workload.kind.as_str(),
                        desired_workload.name,
                        target,
                        e
                    );
                    false
                }
            }
        });
        if !target_matches {
            continue;
        }

        let desired_apply_id = desired_workload
            .apply_id
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty());
        if let Some(apply_id) = desired_apply_id {
            if apply_phases
                .get(apply_id)
                .is_some_and(|phase| *phase != ApplyLifecyclePhase::Running)
            {
                // English note:
                // - Once an apply leaves RUNNING, controller authority moved that exact generation into
                //   delete intent state.
                // - Agents must therefore stop treating the workload as active desired, otherwise the
                //   same pull payload would both request "keep attached" and "delete this apply_id".
                continue;
            }
        }

        let key = workload_key(desired_workload.kind, &desired_workload.name);
        desired_keys.insert(
            key.clone(),
            WorkloadId::new(
                desired_workload.kind,
                desired_workload.name.clone(),
                desired_workload_authority(desired_workload)
                    .unwrap_or_else(|_| desired_workload.name.clone()),
            ),
        );

        let Some(apply_id) = desired_workload.apply_id.as_deref() else {
            eprintln!(
                "[ops_controller:agent_desired] desired workload is missing apply_id instance_key={} kind={} name={}",
                instance_key,
                desired_workload.kind.as_str(),
                desired_workload.name
            );
            continue;
        };
        if apply_id.trim().is_empty() {
            eprintln!(
                "[ops_controller:agent_desired] desired workload apply_id is empty instance_key={} kind={} name={}",
                instance_key,
                desired_workload.kind.as_str(),
                desired_workload.name
            );
            continue;
        }
        if desired_workload.exec_argv.is_empty() {
            eprintln!(
                "[ops_controller:agent_desired] desired workload argv is empty instance_key={} kind={} name={}",
                instance_key,
                desired_workload.kind.as_str(),
                desired_workload.name
            );
            continue;
        }

        workloads.insert(
            key,
            AgentDesiredWorkload {
                kind: desired_workload.kind,
                name: desired_workload.name.clone(),
                authority: desired_workload_authority(desired_workload)
                    .unwrap_or_else(|_| desired_workload.name.clone()),
                logical_selection: desired_workload.logical_selection.clone(),
                service_name: desired_workload.service_name.clone(),
                atomic_group: desired_workload.atomic_group.clone(),
                apply_id: apply_id.to_string(),
                argv: desired_workload.exec_argv.clone(),
                cwd: desired_workload.exec_cwd.clone(),
                updated_ts_ms: desired_workload.updated_ts_ms,
            },
        );
    }

    AgentDesiredListResp {
        ok: true,
        err: None,
        instance_key: instance_key.to_string(),
        desired_keys: desired_keys.into_values().collect(),
        workloads: workloads.into_values().collect(),
        delete_workloads,
    }
}

fn workload_id_map(workloads: &[WorkloadId]) -> anyhow::Result<BTreeMap<String, WorkloadId>> {
    let mut out: BTreeMap<String, WorkloadId> = BTreeMap::new();
    for workload in workloads.iter() {
        let key = workload_key(workload.kind, &workload.name);
        if out.insert(key.clone(), workload.clone()).is_some() {
            anyhow::bail!("duplicate workload key: {}", key);
        }
    }
    Ok(out)
}

fn delete_workload_key(apply_id: &str, kind: WorkloadKind, name: &str) -> String {
    format!("{}::{}", apply_id.trim(), workload_key(kind, name))
}

fn agent_desired_workload_map(
    workloads: &[AgentDesiredWorkload],
) -> anyhow::Result<BTreeMap<String, AgentDesiredWorkload>> {
    let mut out: BTreeMap<String, AgentDesiredWorkload> = BTreeMap::new();
    for workload in workloads.iter() {
        let key = workload_key(workload.kind, &workload.name);
        if out.insert(key.clone(), workload.clone()).is_some() {
            anyhow::bail!("duplicate agent desired workload key: {}", key);
        }
    }
    Ok(out)
}

fn agent_delete_workload_map(
    workloads: &[AgentDeleteWorkload],
) -> anyhow::Result<BTreeMap<String, AgentDeleteWorkload>> {
    let mut out: BTreeMap<String, AgentDeleteWorkload> = BTreeMap::new();
    for workload in workloads.iter() {
        let key = delete_workload_key(&workload.apply_id, workload.kind, &workload.name);
        if out.insert(key.clone(), workload.clone()).is_some() {
            anyhow::bail!("duplicate agent delete workload key: {}", key);
        }
    }
    Ok(out)
}

async fn controller_agent_desired_snapshot(
    desired: &DesiredStore,
    instance_key: &str,
) -> AgentDesiredListResp {
    let desired_snapshot = desired.snapshot();
    let mut apply_phases: HashMap<String, ApplyLifecyclePhase> = HashMap::new();
    let apply_records = match desired.load_apply_records().await {
        Ok(v) => v,
        Err(e) => {
            return AgentDesiredListResp {
                ok: false,
                err: Some(format!("load desired apply records failed: {}", e)),
                instance_key: instance_key.to_string(),
                desired_keys: Vec::new(),
                workloads: Vec::new(),
                delete_workloads: Vec::new(),
            };
        }
    };
    for apply_rec in apply_records.iter() {
        apply_phases.insert(
            apply_rec.id.clone(),
            apply_lifecycle_phase_normalized(apply_rec.lifecycle_phase),
        );
    }
    let delete_workloads = match controller_agent_delete_workloads_from_apply_records(
        apply_records.as_slice(),
        instance_key,
    ) {
        Ok(v) => v,
        Err(e) => {
            return AgentDesiredListResp {
                ok: false,
                err: Some(format!("build agent delete workloads failed: {}", e)),
                instance_key: instance_key.to_string(),
                desired_keys: Vec::new(),
                workloads: Vec::new(),
                delete_workloads: Vec::new(),
            };
        }
    };

    controller_agent_desired_snapshot_from_apply_phases(
        desired_snapshot.as_slice(),
        instance_key,
        &apply_phases,
        delete_workloads,
    )
}

fn controller_agent_desired_snapshot_blocking(
    desired: &DesiredStore,
    instance_key: &str,
) -> AgentDesiredListResp {
    let desired_snapshot = desired.snapshot();
    let mut apply_phases: HashMap<String, ApplyLifecyclePhase> = HashMap::new();
    let apply_records = match desired.load_apply_records_blocking() {
        Ok(v) => v,
        Err(e) => {
            return AgentDesiredListResp {
                ok: false,
                err: Some(format!("load desired apply records failed: {}", e)),
                instance_key: instance_key.to_string(),
                desired_keys: Vec::new(),
                workloads: Vec::new(),
                delete_workloads: Vec::new(),
            };
        }
    };
    for apply_rec in apply_records.iter() {
        apply_phases.insert(
            apply_rec.id.clone(),
            apply_lifecycle_phase_normalized(apply_rec.lifecycle_phase),
        );
    }
    let delete_workloads = match controller_agent_delete_workloads_from_apply_records(
        apply_records.as_slice(),
        instance_key,
    ) {
        Ok(v) => v,
        Err(e) => {
            return AgentDesiredListResp {
                ok: false,
                err: Some(format!("build agent delete workloads failed: {}", e)),
                instance_key: instance_key.to_string(),
                desired_keys: Vec::new(),
                workloads: Vec::new(),
                delete_workloads: Vec::new(),
            };
        }
    };

    controller_agent_desired_snapshot_from_apply_phases(
        desired_snapshot.as_slice(),
        instance_key,
        &apply_phases,
        delete_workloads,
    )
}

fn agent_delete_workload_cmp(
    left: &AgentDeleteWorkload,
    right: &AgentDeleteWorkload,
) -> std::cmp::Ordering {
    match (&left.atomic_group, &right.atomic_group) {
        (Some(left_group), Some(right_group)) => left_group
            .phase
            .cmp(&right_group.phase)
            .then(left_group.order.cmp(&right_group.order))
            .then(left.phase.cmp(&right.phase))
            .then(left.kind.cmp(&right.kind))
            .then(left.name.cmp(&right.name))
            .then(left.apply_id.cmp(&right.apply_id)),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => left
            .phase
            .cmp(&right.phase)
            .then(left.kind.cmp(&right.kind))
            .then(left.name.cmp(&right.name))
            .then(left.apply_id.cmp(&right.apply_id)),
    }
}

async fn fetch_agent_desired(
    fw: &Framework,
    controller_instance_key: &str,
    instance_key: &str,
) -> anyhow::Result<AgentDesiredListResp> {
    let controller_instance_key = controller_instance_key.trim();
    if controller_instance_key.is_empty() {
        anyhow::bail!("controller_instance_key must be non-empty");
    }
    let req = AgentDesiredReq {
        instance_key: instance_key.to_string(),
    };
    let parsed = user_rpc_call_json::<AgentDesiredReq, AgentDesiredListResp>(
        fw,
        controller_instance_key,
        "fluxon_ops/agent_desired",
        &req,
    )
    .await
    .with_context(|| {
        format!(
            "fetch controller agent_desired via userrpc failed: controller_instance_key={} agent_instance_key={}",
            controller_instance_key, instance_key
        )
    })?;
    if !parsed.ok {
        anyhow::bail!(
            "controller agent_desired returned ok=false controller_instance_key={} instance_key={} err={}",
            controller_instance_key,
            parsed.instance_key,
            parsed.err.unwrap_or_else(|| "unknown".to_string())
        );
    }
    if parsed.instance_key != instance_key {
        anyhow::bail!(
            "controller agent_desired returned mismatched instance_key expected={} actual={}",
            instance_key,
            parsed.instance_key
        );
    }
    Ok(parsed)
}

fn desired_workload_matches_running(
    workloads: &SupervisorBackedWorkloads,
    desired: &AgentDesiredWorkload,
) -> bool {
    let Ok(status) = observe_selection_status_for_scope(
        desired.kind,
        &desired.name,
        &desired.authority,
        Some(workloads.scope_key.as_str()),
    )
    else {
        return false;
    };
    desired_workload_status_matches_goal(&status, desired)
}

fn desired_workload_status_matches_goal(
    status: &SelectionSupervisorStatus,
    desired: &AgentDesiredWorkload,
) -> bool {
    if desired_workload_requires_present(desired) {
        return selection_status_matches_present(
            status,
            &desired.apply_id,
            desired.updated_ts_ms,
            desired.argv.as_slice(),
            desired.cwd.as_deref(),
        );
    }
    selection_status_matches_attached(
        status,
        &desired.apply_id,
        desired.updated_ts_ms,
        desired.argv.as_slice(),
        desired.cwd.as_deref(),
    )
}

fn desired_workload_requires_present(desired: &AgentDesiredWorkload) -> bool {
    // English note:
    // - Self-host atomic-group rollout must not treat "new supervisor identity is attached" as
    //   sufficient for non-agent members.
    // - During control-plane handover, the old ops_agent may launch a replacement ops_controller
    //   and then replace itself immediately afterwards.
    // - If the new ops_controller supervisor is attached but its child has not yet reached a
    //   stable live state, the old control plane can retire first and strand the bastion with no
    //   reachable controller.
    // - Therefore atomic-group members other than ops_agent must converge to `present=true`
    //   before the group advances. ops_agent itself stays launch-only because waiting inside the
    //   retiring agent process would turn its own replacement into a self-deadlocking path.
    desired.atomic_group.is_some() && desired.service_name != OPS_AGENT_WORKLOAD_SERVICE_NAME
}

fn phase1_overlap_with_applyless_owner_runtime(
    status: &SelectionSupervisorStatus,
    desired_apply_id: &str,
    desired_owner_ts_ms: u64,
) -> bool {
    // English note:
    // - Bare-then-apply self-host startup is a two-phase handover.
    // - Phase 1 only requires that the newer apply-owned generation is published and becomes
    //   observable while the older applyless bare owner keeps serving the control plane.
    // - Phase 2 performs the fast cutover after the whole atomic group has confirmed that phase 1
    //   completed everywhere.
    // - Therefore this helper models an allowed phase-1 overlap with an older applyless owner; it
    //   does not mean the old owner should be eagerly retired at this point.
    status.running
        && status.apply_id.is_none()
        && !desired_apply_id.trim().is_empty()
        && status
            .owner_ts_ms
            .is_some_and(|current_owner_ts_ms| desired_owner_ts_ms > current_owner_ts_ms)
}

fn phase1_overlap_with_applyless_owner(
    status: &SelectionSupervisorStatus,
    desired: &AgentDesiredWorkload,
) -> bool {
    phase1_overlap_with_applyless_owner_runtime(
        status,
        desired.apply_id.as_str(),
        desired.updated_ts_ms,
    )
}

fn desired_workload_recovery_superseded(
    workloads: &SupervisorBackedWorkloads,
    desired: &AgentDesiredWorkload,
) -> anyhow::Result<bool> {
    // English note:
    // - A newer apply-owned generation overlapping an older applyless bare owner is the expected
    //   phase-1 state of the self-host two-phase handover.
    // - Reconcile must therefore not misclassify that overlap as "the desired workload was already
    //   superseded", otherwise the controller would stop driving the requested generation before
    //   phase 2 has a chance to cut over.
    // - Only an owner_ts that is newer than the requested workload and is not this intentional
    //   phase-1 overlap is treated as a hard superseding fact.
    let status = observe_selection_status_for_scope(
        desired.kind,
        &desired.name,
        &desired.authority,
        Some(workloads.scope_key.as_str()),
    )?;
    if phase1_overlap_with_applyless_owner(&status, desired) {
        return Ok(false);
    }
    let Some(current_owner_ts_ms) = status.owner_ts_ms else {
        return Ok(false);
    };
    Ok(current_owner_ts_ms > desired.updated_ts_ms)
}

fn agent_desired_start_req(desired: &AgentDesiredWorkload, wait_for_attached: bool) -> StartReq {
    // English note:
    // - Agent pull reconcile converges at the supervisor-generation boundary.
    // - In self-host bare-then-apply startup this is only phase 1: publish the requested
    //   generation and wait until the requested identity is observable.
    // - The explicit phase-2 cutover that retires the launch-only bare owner happens outside this
    //   start request after the whole atomic group has confirmed phase 1.
    // - Plain workloads still converge on attachment only so one transient child restart does not
    //   widen into a stop-and-retry cycle on every reconcile tick.
    // - Atomic-group control-plane members are stricter: they must reach `present=true` before
    //   the group advances, otherwise self-host handover can retire the old controller before the
    //   new one has a live child.
    StartReq {
        kind: desired.kind,
        name: desired.name.clone(),
        authority: desired.authority.clone(),
        service_name: desired.service_name.clone(),
        apply_id: desired.apply_id.clone(),
        owner_ts_ms: desired.updated_ts_ms,
        argv: desired.argv.clone(),
        cwd: desired.cwd.clone(),
        wait_for_attached,
        wait_for_present: wait_for_attached && desired_workload_requires_present(desired),
    }
}

fn reconcile_atomic_group_on_agent(
    instance_key: &str,
    workloads: &SupervisorBackedWorkloads,
    members: &[AgentDesiredWorkload],
) -> anyhow::Result<()> {
    if members.is_empty() {
        return Ok(());
    }
    let mut all_settled = true;
    for desired in members.iter() {
        if desired_workload_matches_running(workloads, desired) {
            continue;
        }
        if desired_workload_recovery_superseded(workloads, desired)? {
            continue;
        }
        all_settled = false;
        break;
    }
    if all_settled {
        return Ok(());
    }

    let group = members[0]
        .atomic_group
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("atomic group metadata is required"))?;
    let mut ordered: Vec<AgentDesiredWorkload> = members.to_vec();
    ordered.sort_by(|left, right| {
        let left_group = left.atomic_group.as_ref().unwrap();
        let right_group = right.atomic_group.as_ref().unwrap();
        left_group
            .order
            .cmp(&right_group.order)
            .then(left.kind.cmp(&right.kind))
            .then(left.name.cmp(&right.name))
    });
    // English note:
    // - This loop is the phase-1 publisher for one atomic group.
    // - It intentionally drives each member only to the requested observable identity; it is not
    //   the cutover point for retiring older bare bootstrap owners.
    // - Fast cutover is phase 2 and happens only after the whole atomic group has completed this
    //   phase-1 barrier.
    for desired in ordered.iter() {
        // English note:
        // - Atomic-group metadata defines ordered convergence, not permission to widen one
        //   member mismatch into a stop-all effect for the whole selection.
        // - `workloads.start` already attaches one concrete workload identity at a time.
        // - The launch-only handover gate must therefore retire only the matching bare service
        //   for that member, otherwise the first member start widens into a stop-all of the
        //   whole local control plane and later members block on dependencies that were just
        //   torn down.
        if desired_workload_matches_running(workloads, desired) {
            continue;
        }
        if desired_workload_recovery_superseded(workloads, desired)? {
            continue;
        }
        let wait_for_attached = desired.service_name != OPS_AGENT_WORKLOAD_SERVICE_NAME;
        let start = workloads.start(agent_desired_start_req(desired, wait_for_attached));
        if !start.ok {
            anyhow::bail!(
                "atomic group start failed instance_key={} selection={} workload={}/{} apply_id={} err={}",
                instance_key,
                group.selection_name,
                desired.kind.as_str(),
                desired.name,
                desired.apply_id,
                start.err.unwrap_or_else(|| "unknown".to_string())
            );
        }
    }

    Ok(())
}

async fn agent_pull_reconcile_tick(
    fw: &Framework,
    controller_instance_key: &str,
    instance_key: &str,
    _hostworkdir: &str,
    workloads: Arc<SupervisorBackedWorkloads>,
    desired_snapshot_store: &AgentDesiredSnapshotStore,
) -> anyhow::Result<()> {
    let tick_started_at = Instant::now();
    let fetch_started_at = Instant::now();
    let desired = fetch_agent_desired(fw, controller_instance_key, instance_key).await?;
    let should_log_tick = !desired.desired_keys.is_empty()
        || !desired.workloads.is_empty()
        || !desired.delete_workloads.is_empty();
    if should_log_tick {
        eprintln!(
            "[ops_agent:pull_repair] tick fetched desired instance_key={} controller_instance_key={} desired_keys={} workloads={} delete_workloads={} elapsed_ms={}",
            instance_key,
            controller_instance_key,
            desired.desired_keys.len(),
            desired.workloads.len(),
            desired.delete_workloads.len(),
            fetch_started_at.elapsed().as_millis()
        );
    }
    let current_desired_keys = workload_id_map(desired.desired_keys.as_slice())?;
    let previous_delete_workloads = match desired_snapshot_store.load(instance_key)? {
        Some(previous_snapshot) => {
            agent_delete_workload_map(previous_snapshot.delete_workloads.as_slice())?
        }
        None => BTreeMap::new(),
    };
    let mut ordered_delete_workloads = desired.delete_workloads.clone();
    ordered_delete_workloads.sort_by(agent_delete_workload_cmp);
    for delete_workload in ordered_delete_workloads.into_iter() {
        let delete_key = delete_workload_key(
            &delete_workload.apply_id,
            delete_workload.kind,
            &delete_workload.name,
        );
        if let Some(previous_delete_workload) = previous_delete_workloads.get(&delete_key) {
            if previous_delete_workload.phase == delete_workload.phase {
                match delete_workload.phase {
                    // English note:
                    // - notify is level-triggered now because the agent itself owns the local
                    //   "30s elapsed" decision from the controller-persisted phase timestamp.
                    // - Therefore repeated pull snapshots must keep re-evaluating the same
                    //   DELETE_NOTIFYING intent until it becomes eligible for immediate delete.
                    ApplyLifecyclePhase::DeleteNotifying => {}
                    ApplyLifecyclePhase::Running | ApplyLifecyclePhase::DeleteCommitted => {}
                }
            }
        }
        let require_apply_id = delete_workload.apply_id.trim().to_string();
        if require_apply_id.is_empty() {
            eprintln!(
                "[ops_agent:pull_repair] skip unsafe delete intent without apply_id instance_key={} kind={} name={} phase={:?}",
                instance_key,
                delete_workload.kind.as_str(),
                delete_workload.name,
                delete_workload.phase
            );
            continue;
        }
        match delete_workload.phase {
            ApplyLifecyclePhase::Running => {
                eprintln!(
                    "[ops_agent:pull_repair] ignore invalid delete intent in RUNNING phase instance_key={} kind={} name={} apply_id={}",
                    instance_key,
                    delete_workload.kind.as_str(),
                    delete_workload.name,
                    require_apply_id
                );
            }
            ApplyLifecyclePhase::DeleteNotifying => {
                let Some(phase_updated_ts_ms) = delete_workload.phase_updated_ts_ms else {
                    eprintln!(
                        "[ops_agent:pull_repair] skip notify delete intent without phase_updated_ts_ms instance_key={} kind={} name={} apply_id={}",
                        instance_key,
                        delete_workload.kind.as_str(),
                        delete_workload.name,
                        require_apply_id
                    );
                    continue;
                };
                let notify_deadline_ts_ms = phase_updated_ts_ms
                    .saturating_add(DELETE_APPLY_NO_WAIT_DELAY_SECONDS.saturating_mul(1000));
                if now_ts_ms() < notify_deadline_ts_ms {
                    continue;
                }
                let resp = workloads.delete_generation(
                    delete_workload.kind,
                    &delete_workload.name,
                    &delete_workload.authority,
                    Some(require_apply_id.as_str()),
                );
                if !resp.ok {
                    eprintln!(
                        "[ops_agent:pull_repair] notify window elapsed but delete_generation failed instance_key={} kind={} name={} apply_id={} phase_updated_ts_ms={} err={}",
                        instance_key,
                        delete_workload.kind.as_str(),
                        delete_workload.name,
                        require_apply_id,
                        phase_updated_ts_ms,
                        resp.err.unwrap_or_else(|| "unknown".to_string())
                    );
                }
            }
            ApplyLifecyclePhase::DeleteCommitted => {
                let resp = workloads.delete_generation(
                    delete_workload.kind,
                    &delete_workload.name,
                    &delete_workload.authority,
                    Some(require_apply_id.as_str()),
                );
                if !resp.ok {
                    eprintln!(
                        "[ops_agent:pull_repair] committed delete_generation failed instance_key={} kind={} name={} apply_id={} err={}",
                        instance_key,
                        delete_workload.kind.as_str(),
                        delete_workload.name,
                        require_apply_id,
                        resp.err.unwrap_or_else(|| "unknown".to_string())
                    );
                }
            }
        }
    }
    let snapshot_persist_started_at = Instant::now();
    desired_snapshot_store.persist(&AgentDesiredSnapshotRecord {
        instance_key: instance_key.to_string(),
        desired_keys: current_desired_keys.into_values().collect(),
        workloads: desired.workloads.clone(),
        delete_workloads: desired.delete_workloads.clone(),
    })?;
    if should_log_tick {
        eprintln!(
            "[ops_agent:pull_repair] tick persisted snapshot instance_key={} workloads={} delete_workloads={} elapsed_ms={}",
            instance_key,
            desired.workloads.len(),
            desired.delete_workloads.len(),
            snapshot_persist_started_at.elapsed().as_millis()
        );
    }

    let mut grouped: BTreeMap<(u64, String), Vec<AgentDesiredWorkload>> = BTreeMap::new();
    let mut plain: Vec<AgentDesiredWorkload> = Vec::new();
    for desired_workload in desired.workloads.iter() {
        if let Some(group) = desired_workload.atomic_group.as_ref() {
            grouped
                .entry((group.phase, group.selection_name.clone()))
                .or_default()
                .push(desired_workload.clone());
        } else {
            plain.push(desired_workload.clone());
        }
    }
    if should_log_tick {
        let grouped_member_count: usize = grouped.values().map(Vec::len).sum();
        eprintln!(
            "[ops_agent:pull_repair] tick reconcile plan instance_key={} grouped_groups={} grouped_members={} plain_workloads={}",
            instance_key,
            grouped.len(),
            grouped_member_count,
            plain.len()
        );
    }

    for ((phase, selection_name), members) in grouped.iter() {
        if should_log_tick {
            eprintln!(
                "[ops_agent:pull_repair] reconcile atomic group instance_key={} selection_name={} phase={} members={}",
                instance_key,
                selection_name,
                phase,
                members.len()
            );
        }
        reconcile_atomic_group_on_agent(instance_key, workloads.as_ref(), members)?;
    }

    plain.sort_by(|left, right| left.kind.cmp(&right.kind).then(left.name.cmp(&right.name)));
    for desired_workload in plain.iter() {
        if desired_workload_recovery_superseded(workloads.as_ref(), desired_workload)? {
            if should_log_tick {
                eprintln!(
                    "[ops_agent:pull_repair] skip superseded desired instance_key={} kind={} name={} apply_id={} owner_ts_ms={}",
                    instance_key,
                    desired_workload.kind.as_str(),
                    desired_workload.name,
                    desired_workload.apply_id,
                    desired_workload.updated_ts_ms
                );
            }
            continue;
        }
        if should_log_tick {
            eprintln!(
                "[ops_agent:pull_repair] start plain desired instance_key={} kind={} name={} apply_id={} owner_ts_ms={}",
                instance_key,
                desired_workload.kind.as_str(),
                desired_workload.name,
                desired_workload.apply_id,
                desired_workload.updated_ts_ms
            );
        }
        let resp = workloads.start(agent_desired_start_req(desired_workload, true));
        if !resp.ok {
            eprintln!(
                "[ops_agent:pull_repair] start failed instance_key={} kind={} name={} apply_id={} err={}",
                instance_key,
                desired_workload.kind.as_str(),
                desired_workload.name,
                desired_workload.apply_id,
                resp.err.unwrap_or_else(|| "unknown".to_string())
            );
        }
    }
    if should_log_tick {
        eprintln!(
            "[ops_agent:pull_repair] tick complete instance_key={} elapsed_ms={}",
            instance_key,
            tick_started_at.elapsed().as_millis()
        );
    }

    Ok(())
}

async fn agent_pull_reconcile_loop(
    fw: Arc<Framework>,
    controller_instance_key: String,
    instance_key: String,
    hostworkdir: String,
    workloads: Arc<SupervisorBackedWorkloads>,
    desired_snapshot_store: AgentDesiredSnapshotStore,
) {
    let interval = Duration::from_millis(OPS_AGENT_PULL_REPAIR_INTERVAL_MS);
    loop {
        if let Err(e) = agent_pull_reconcile_tick(
            fw.as_ref(),
            &controller_instance_key,
            &instance_key,
            &hostworkdir,
            workloads.clone(),
            &desired_snapshot_store,
        )
        .await
        {
            eprintln!(
                "[ops_agent:pull_repair] reconcile tick failed instance_key={} err={}",
                instance_key, e
            );
        }
        tokio::time::sleep(interval).await;
    }
}

pub async fn run_agent_blocking(
    config_yaml: &str,
    workdir: &Path,
    python_exe: &Path,
) -> anyhow::Result<()> {
    if config_yaml.trim().is_empty() {
        anyhow::bail!("config_yaml must be non-empty");
    }
    if !workdir.is_dir() {
        anyhow::bail!(
            "workdir must be an existing directory: {}",
            workdir.display()
        );
    }

    std::env::set_current_dir(workdir).context("set_current_dir")?;

    let cfg = parse_agent_config_yaml(config_yaml)?;
    let (fw, _client_cfg) = build_framework_from_kv_client_yaml(&cfg.kv_client).await?;

    let log_dir = workdir.join(OPS_LOG_DIR_NAME);
    let desired_snapshot_store = AgentDesiredSnapshotStore::new(workdir);
    let supervisor_runtime =
        SelectionSupervisorRuntime::materialize(workdir, Path::new(&cfg.hostworkdir), python_exe)?;
    let workloads = Arc::new(SupervisorBackedWorkloads::new(
        PathBuf::from(&cfg.hostworkdir),
        log_dir.clone(),
        supervisor_runtime,
    )?);
    let agent = OpsAgent::new(fw.clone(), workloads, log_dir);
    agent.register_handlers();
    let mut pull_reconcile = tokio::spawn(agent_pull_reconcile_loop(
        fw.clone(),
        cfg.controller_instance_key.clone(),
        cfg.kv_client.instance_key.clone(),
        cfg.hostworkdir.clone(),
        agent.workloads.clone(),
        desired_snapshot_store,
    ));

    // Causal chain:
    // - The ops agent runs as a long-lived service and is commonly managed by external supervisors
    //   (K8s, custom rollout scripts) which use SIGTERM for graceful stop.
    // - If we only wait for Ctrl-C (SIGINT), SIGTERM terminates the process immediately and may leave
    //   detached child processes behind (e.g. when a Python entry spawns the worker in a new session).
    // - Handling SIGTERM/SIGINT here allows the agent to shutdown its framework cleanly and finish
    //   in-flight work before the process exits.
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
        let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;

        tokio::select! {
            res = &mut pull_reconcile => {
                match res {
                    Ok(()) => anyhow::bail!("ops agent pull_repair loop exited unexpectedly"),
                    Err(e) => anyhow::bail!("ops agent pull_repair loop failed: {}", e),
                }
            }
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        anyhow::bail!("ops agent signal handling requires unix (SIGTERM/SIGINT)");
    }
    pull_reconcile.abort();
    let _ = pull_reconcile.await;
    fw.shutdown()
        .await
        .map_err(|e| anyhow::anyhow!("framework shutdown: {}", e))?;
    Ok(())
}

struct ControllerHttpState {
    fw: Arc<Framework>,
    cfg: Arc<ControllerConfigYaml>,
    history_dir: PathBuf,
    desired: Arc<DesiredStore>,
    deploy_guard: tokio::sync::Mutex<()>,
}

struct ControllerRuntime {
    state: Arc<ControllerHttpState>,
    reconcile_handle: tokio::task::JoinHandle<()>,
}

struct ControllerInitState {
    cfg: Arc<ControllerConfigYaml>,
    started_at: Instant,
    stage: tokio::sync::RwLock<String>,
    init_error: tokio::sync::Mutex<Option<String>>,
    fw: tokio::sync::Mutex<Option<Arc<Framework>>>,
    runtime: tokio::sync::Mutex<Option<ControllerRuntime>>,
}

impl ControllerInitState {
    async fn set_stage(&self, stage: &str) {
        *self.stage.write().await = stage.to_string();
    }

    async fn set_init_error(&self, err: String) {
        *self.init_error.lock().await = Some(err);
    }
}

pub fn smoke_selection_supervisor(python_exe: &Path, workdir: &Path) -> anyhow::Result<()> {
    if !workdir.is_dir() {
        anyhow::bail!(
            "workdir must be an existing directory: {}",
            workdir.display()
        );
    }

    let smoke_dir = workdir.join(format!(
        "selection_supervisor_smoke_{}_{}",
        std::process::id(),
        now_ts_ms()
    ));
    std::fs::create_dir_all(&smoke_dir)
        .with_context(|| format!("create supervisor smoke dir: {}", smoke_dir.display()))?;

    let runtime = SelectionSupervisorRuntime::materialize(&smoke_dir, &smoke_dir, python_exe)?;
    let log_dir = smoke_dir.join(OPS_LOG_DIR_NAME);
    std::fs::create_dir_all(&log_dir)
        .with_context(|| format!("create supervisor smoke log dir: {}", log_dir.display()))?;
    let drain_selection_generations =
        |kind: WorkloadKind, name: &str, authority: &str| -> anyhow::Result<()> {
            loop {
                let status = observe_selection_status(kind, name, authority)?;
                if !status.running {
                    return Ok(());
                }
                match status.apply_id.as_deref() {
                    Some(apply_id) if !apply_id.trim().is_empty() => {
                        runtime.stop(kind, name, true, Some(apply_id))?;
                        wait_for_selection_absent(kind, name, authority, Some(apply_id))?;
                    }
                    _ => {
                        runtime.stop(kind, name, true, None)?;
                        wait_for_selection_absent(kind, name, authority, None)?;
                    }
                }
            }
        };

    let daemonset_contract_yaml = |workload_name: &str| {
        format!(
            r#"apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: {}
  annotations:
    fluxon.io/logical_selection: owner
    fluxon.io/service_name: owner
spec:
  template:
    spec:
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - key: kubernetes.io/hostname
                    operator: In
                    values:
                      - smoke-node
      containers:
        - name: owner
          image: smoke
          command:
            - /usr/bin/env
            - bash
            - -lc
            - sleep 30
"#,
            workload_name
        )
    };
    parse_k8s_deployment_subset_documents(daemonset_contract_yaml("smoke-contract-owner").as_str())
        .context("smoke parse valid daemonset naming contract")?;
    let invalid_daemonset_err = match parse_k8s_deployment_subset_documents(
        daemonset_contract_yaml("owner").as_str(),
    ) {
        Ok(_) => {
            anyhow::bail!(
                "smoke parse daemonset naming contract must reject plain desired name without prefix"
            )
        }
        Err(err) => err,
    };
    let invalid_daemonset_err_text = format!("{:#}", invalid_daemonset_err);
    if !invalid_daemonset_err_text.contains("shared naming contract") {
        anyhow::bail!(
            "smoke parse daemonset naming contract returned unexpected error: err={}",
            invalid_daemonset_err_text
        );
    }

    let name = format!("smoke_{}", now_ts_ms());
    let log_path = log_dir.join("smoke.log");
    let argv = vec![
        python_exe.display().to_string(),
        "-c".to_string(),
        "import time; time.sleep(30)".to_string(),
    ];

    let detached_pid = runtime.launch_detached(
        WorkloadKind::Deployment,
        &name,
        &name,
        "smoke_ignored_service_name",
        "smoke-apply",
        1,
        &argv,
        None,
        &log_path,
    )?;
    wait_for_selection_present(WorkloadKind::Deployment, &name, &name)?;

    let status = observe_selection_status(WorkloadKind::Deployment, &name, &name)?;
    if !status.running || !status.present {
        anyhow::bail!(
            "supervisor smoke status mismatch after launch: running={} present={} pid={:?} detached_pid={}",
            status.running,
            status.present,
            status.pid,
            detached_pid
        );
    }
    if status.apply_id.as_deref() != Some("smoke-apply") {
        anyhow::bail!(
            "supervisor smoke apply_id mismatch after launch: got={:?}",
            status.apply_id
        );
    }
    if status.owner_ts_ms != Some(1) {
        anyhow::bail!(
            "supervisor smoke owner_ts_ms mismatch after launch: got={:?}",
            status.owner_ts_ms
        );
    }

    let listed = observe_all_selection_statuses()?;
    let listed_entry = listed
        .into_iter()
        .find(|entry| {
            entry.kind == Some(WorkloadKind::Deployment)
                && entry.name.as_deref() == Some(name.as_str())
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "supervisor smoke missing list entry after launch: name={}",
                name
            )
        })?;
    if listed_entry.apply_id.as_deref() != Some("smoke-apply") {
        anyhow::bail!(
            "supervisor smoke list apply_id mismatch after launch: got={:?}",
            listed_entry.apply_id
        );
    }

    if runtime
        .stop(
            WorkloadKind::Deployment,
            &name,
            true,
            Some("smoke-apply-wrong"),
        )
        .is_ok()
    {
        anyhow::bail!("supervisor smoke wrong apply_id stop must fail");
    }

    let still_running = observe_selection_status(WorkloadKind::Deployment, &name, &name)?;
    if !selection_status_matches_present(&still_running, "smoke-apply", 1, argv.as_slice(), None) {
        anyhow::bail!(
            "supervisor smoke status mismatch after wrong apply_id stop: running={} present={} apply_id={:?}",
            still_running.running,
            still_running.present,
            still_running.apply_id
        );
    }

    let replaced_pid = runtime.launch_detached(
        WorkloadKind::Deployment,
        &name,
        &name,
        "smoke_ignored_service_name",
        "smoke-apply-2",
        2,
        &argv,
        None,
        &log_path,
    )?;
    let replaced_status = wait_for_selection_attached(
        WorkloadKind::Deployment,
        &name,
        &name,
        "smoke-apply-2",
        2,
        &argv,
        None,
    )?;
    wait_for_selection_present(WorkloadKind::Deployment, &name, &name)?;
    if !selection_status_matches_attached(
        &replaced_status,
        "smoke-apply-2",
        2,
        argv.as_slice(),
        None,
    ) {
        anyhow::bail!(
            "supervisor smoke attached status mismatch after replace: running={} present={} apply_id={:?} replaced_pid={}",
            replaced_status.running,
            replaced_status.present,
            replaced_status.apply_id,
            replaced_pid
        );
    }
    if replaced_status.owner_ts_ms != Some(2) {
        anyhow::bail!(
            "supervisor smoke owner_ts_ms mismatch after replace: got={:?}",
            replaced_status.owner_ts_ms
        );
    }

    // English note:
    // - Replacement publishes the newer owner immediately, but the superseded generation may still
    //   exist inside its retire window.
    // - Therefore an explicit guarded stop for the old apply can race with the old supervisor's
    //   own delayed retirement and may legitimately either succeed or report "already gone".
    // - The correctness boundary here is narrower: retiring the old apply must not dislodge the
    //   new visible owner once replacement has happened.
    let _ = runtime.stop(WorkloadKind::Deployment, &name, true, Some("smoke-apply"));
    let after_replace = observe_selection_status(WorkloadKind::Deployment, &name, &name)?;
    if !selection_status_matches_present(&after_replace, "smoke-apply-2", 2, argv.as_slice(), None)
    {
        anyhow::bail!(
            "supervisor smoke status mismatch after old apply_id stop post-replace: running={} present={} apply_id={:?}",
            after_replace.running,
            after_replace.present,
            after_replace.apply_id
        );
    }

    runtime.stop(WorkloadKind::Deployment, &name, true, Some("smoke-apply-2"))?;
    wait_for_selection_absent(
        WorkloadKind::Deployment,
        &name,
        &name,
        Some("smoke-apply-2"),
    )?;
    let relaunched_pid = runtime.launch_detached(
        WorkloadKind::Deployment,
        &name,
        &name,
        "smoke_ignored_service_name",
        "smoke-apply-2",
        2,
        &argv,
        None,
        &log_path,
    )?;
    let relaunched_status = wait_for_selection_attached(
        WorkloadKind::Deployment,
        &name,
        &name,
        "smoke-apply-2",
        2,
        &argv,
        None,
    )?;
    wait_for_selection_present(WorkloadKind::Deployment, &name, &name)?;
    let relaunched_present_status =
        observe_selection_status(WorkloadKind::Deployment, &name, &name)?;
    if !selection_status_matches_present(
        &relaunched_present_status,
        "smoke-apply-2",
        2,
        argv.as_slice(),
        None,
    ) {
        anyhow::bail!(
            "supervisor smoke status mismatch after relaunch same apply: running={} present={} apply_id={:?} relaunched_pid={}",
            relaunched_present_status.running,
            relaunched_present_status.present,
            relaunched_present_status.apply_id,
            relaunched_pid
        );
    }
    if relaunched_present_status.owner_ts_ms != Some(2) {
        anyhow::bail!(
            "supervisor smoke owner_ts_ms mismatch after relaunch same apply: got={:?}",
            relaunched_present_status.owner_ts_ms
        );
    }

    let replaced_pid_2 = runtime.launch_detached(
        WorkloadKind::Deployment,
        &name,
        &name,
        "smoke_ignored_service_name",
        "smoke-apply-3",
        3,
        &argv,
        None,
        &log_path,
    )?;
    let replaced_status_2 = wait_for_selection_attached(
        WorkloadKind::Deployment,
        &name,
        &name,
        "smoke-apply-3",
        3,
        &argv,
        None,
    )?;
    wait_for_selection_present(WorkloadKind::Deployment, &name, &name)?;
    if !selection_status_matches_attached(
        &replaced_status_2,
        "smoke-apply-3",
        3,
        argv.as_slice(),
        None,
    ) {
        anyhow::bail!(
            "supervisor smoke attached status mismatch after second replace: running={} present={} apply_id={:?} replaced_pid={}",
            replaced_status_2.running,
            replaced_status_2.present,
            replaced_status_2.apply_id,
            replaced_pid_2
        );
    }
    if replaced_status_2.owner_ts_ms != Some(3) {
        anyhow::bail!(
            "supervisor smoke owner_ts_ms mismatch after second replace: got={:?}",
            replaced_status_2.owner_ts_ms
        );
    }
    let after_delayed_guard = observe_selection_status(WorkloadKind::Deployment, &name, &name)?;
    if !selection_status_matches_present(
        &after_delayed_guard,
        "smoke-apply-3",
        3,
        argv.as_slice(),
        None,
    ) {
        anyhow::bail!(
            "supervisor smoke second replace did not keep the new apply attached: running={} present={} apply_id={:?}",
            after_delayed_guard.running,
            after_delayed_guard.present,
            after_delayed_guard.apply_id
        );
    }

    runtime.stop(WorkloadKind::Deployment, &name, true, Some("smoke-apply-3"))?;
    wait_for_selection_absent(
        WorkloadKind::Deployment,
        &name,
        &name,
        Some("smoke-apply-3"),
    )?;
    drain_selection_generations(WorkloadKind::Deployment, &name, &name)?;

    let stopped = observe_selection_status(WorkloadKind::Deployment, &name, &name)?;
    if stopped.running {
        anyhow::bail!(
            "supervisor smoke status mismatch after stop: running={}",
            stopped.running
        );
    }
    if stopped.apply_id.is_some() {
        anyhow::bail!(
            "supervisor smoke apply_id must be absent after stop: got={:?}",
            stopped.apply_id
        );
    }

    let listed_after = observe_all_selection_statuses()?;
    if listed_after.iter().any(|entry| {
        entry.kind == Some(WorkloadKind::Deployment) && entry.name.as_deref() == Some(name.as_str())
    }) {
        anyhow::bail!(
            "supervisor smoke list still contains stopped entry: name={}",
            name
        );
    }

    let bare_name = format!("smoke_bare_{}", now_ts_ms());
    let bare_log_path = log_dir.join("smoke_bare.log");
    let bare_target = runtime.target(WorkloadKind::Deployment, &bare_name)?;
    let bare_state_json = serde_json::to_string(&serde_json::json!({
        "kind": WorkloadKind::Deployment,
        "name": bare_name.clone(),
        "authority": bare_name.clone(),
        "service_name": bare_name.clone(),
        "argv": argv.clone(),
        "log_path": bare_log_path.display().to_string(),
    }))
    .context("encode bare-mode smoke state json")?;
    let mut bare_command: Vec<String> = vec![
        python_exe.display().to_string(),
        runtime.script_path.display().to_string(),
        "run".to_string(),
        "--label".to_string(),
        bare_target.label.clone(),
        "--state-json".to_string(),
        bare_state_json,
        "--owner-ts-ms".to_string(),
        "1".to_string(),
        "--restart-policy".to_string(),
        "always".to_string(),
        "--restart-delay-seconds".to_string(),
        OPS_SELECTION_SUPERVISOR_RUN_RESTART_DELAY_SECONDS.to_string(),
        "--max-backoff-seconds".to_string(),
        OPS_SELECTION_SUPERVISOR_RUN_MAX_BACKOFF_SECONDS.to_string(),
        "--crashloop-consecutive-restarts".to_string(),
        "0".to_string(),
        "--crashloop-interval-lt-seconds".to_string(),
        "0".to_string(),
        "--".to_string(),
    ];
    bare_command.extend(argv.iter().cloned());
    runtime.spawn_detached_command(&bare_log_path, bare_command.as_slice())?;
    wait_for_selection_present(WorkloadKind::Deployment, &bare_name, &bare_name)?;

    let bare_status = observe_selection_status(WorkloadKind::Deployment, &bare_name, &bare_name)?;
    if bare_status.apply_id.is_some() {
        anyhow::bail!(
            "supervisor smoke bare-mode apply_id must be absent after launch: got={:?}",
            bare_status.apply_id
        );
    }
    if bare_status.owner_ts_ms != Some(1) {
        anyhow::bail!(
            "supervisor smoke bare-mode owner_ts_ms mismatch after launch: got={:?}",
            bare_status.owner_ts_ms
        );
    }

    if runtime
        .stop(
            WorkloadKind::Deployment,
            &bare_name,
            true,
            Some("smoke-bare-wrong"),
        )
        .is_ok()
    {
        anyhow::bail!("supervisor smoke bare-mode guarded stop must fail");
    }

    let bare_takeover_pid = runtime.launch_detached(
        WorkloadKind::Deployment,
        &bare_name,
        &bare_name,
        "smoke_ignored_service_name",
        "smoke-bare-takeover",
        2,
        &argv,
        None,
        &bare_log_path,
    )?;
    let bare_takeover_status = wait_for_selection_attached(
        WorkloadKind::Deployment,
        &bare_name,
        &bare_name,
        "smoke-bare-takeover",
        2,
        &argv,
        None,
    )?;
    wait_for_selection_present(WorkloadKind::Deployment, &bare_name, &bare_name)?;
    let bare_takeover_present_status =
        observe_selection_status(WorkloadKind::Deployment, &bare_name, &bare_name)?;
    if !selection_status_matches_present(
        &bare_takeover_present_status,
        "smoke-bare-takeover",
        2,
        argv.as_slice(),
        None,
    ) {
        anyhow::bail!(
            "supervisor smoke bare-mode takeover mismatch: running={} present={} apply_id={:?} takeover_pid={}",
            bare_takeover_present_status.running,
            bare_takeover_present_status.present,
            bare_takeover_present_status.apply_id,
            bare_takeover_pid
        );
    }
    if bare_takeover_present_status.owner_ts_ms != Some(2) {
        anyhow::bail!(
            "supervisor smoke bare-mode takeover owner_ts_ms mismatch: got={:?}",
            bare_takeover_present_status.owner_ts_ms
        );
    }
    let bare_takeover_supervisor_pid = bare_takeover_status.pid.ok_or_else(|| {
        anyhow::anyhow!("supervisor smoke bare-mode takeover missing supervisor pid")
    })?;
    let bare_takeover_supervisor_start_time_ticks = bare_takeover_present_status
        .supervisor_start_time_ticks
        .ok_or_else(|| {
            anyhow::anyhow!("supervisor smoke bare-mode takeover missing start_time_ticks")
        })?;

    runtime.launch_detached(
        WorkloadKind::Deployment,
        &bare_name,
        &bare_name,
        "smoke_ignored_service_name",
        "smoke-bare-takeover",
        2,
        &argv,
        None,
        &bare_log_path,
    )?;
    std::thread::sleep(Duration::from_secs(2));
    let after_same_apply_bare_replace =
        observe_selection_status(WorkloadKind::Deployment, &bare_name, &bare_name)?;
    if !selection_status_matches_present(
        &after_same_apply_bare_replace,
        "smoke-bare-takeover",
        2,
        argv.as_slice(),
        None,
    ) {
        anyhow::bail!(
            "supervisor smoke bare-mode same-apply stale replace changed running identity: running={} present={} apply_id={:?}",
            after_same_apply_bare_replace.running,
            after_same_apply_bare_replace.present,
            after_same_apply_bare_replace.apply_id
        );
    }
    if after_same_apply_bare_replace.owner_ts_ms != Some(2) {
        anyhow::bail!(
            "supervisor smoke bare-mode same-apply stale replace changed owner_ts_ms: got={:?}",
            after_same_apply_bare_replace.owner_ts_ms
        );
    }
    if after_same_apply_bare_replace.pid != Some(bare_takeover_supervisor_pid) {
        anyhow::bail!(
            "supervisor smoke bare-mode same-apply stale replace restarted supervisor unexpectedly: before_pid={} after_pid={:?}",
            bare_takeover_supervisor_pid,
            after_same_apply_bare_replace.pid
        );
    }

    runtime.launch_detached(
        WorkloadKind::Deployment,
        &bare_name,
        &bare_name,
        "smoke_ignored_service_name",
        "smoke-bare-takeover",
        3,
        &argv,
        None,
        &bare_log_path,
    )?;
    wait_for_selection_present(WorkloadKind::Deployment, &bare_name, &bare_name)?;
    let after_same_apply_later_generation_replace =
        observe_selection_status(WorkloadKind::Deployment, &bare_name, &bare_name)?;
    if !selection_status_matches_present(
        &after_same_apply_later_generation_replace,
        "smoke-bare-takeover",
        3,
        argv.as_slice(),
        None,
    ) {
        anyhow::bail!(
            "supervisor smoke bare-mode same-apply later-generation replace failed to take ownership: running={} present={} apply_id={:?}",
            after_same_apply_later_generation_replace.running,
            after_same_apply_later_generation_replace.present,
            after_same_apply_later_generation_replace.apply_id
        );
    }
    if after_same_apply_later_generation_replace.pid == Some(bare_takeover_supervisor_pid) {
        anyhow::bail!(
            "supervisor smoke bare-mode same-apply later-generation replace did not publish a new supervisor pid: before_pid={} after_pid={:?}",
            bare_takeover_supervisor_pid,
            after_same_apply_later_generation_replace.pid
        );
    }
    wait_for_process_identity_absent(
        bare_takeover_supervisor_pid,
        bare_takeover_supervisor_start_time_ticks,
    )?;

    runtime.launch_detached(
        WorkloadKind::Deployment,
        &bare_name,
        &bare_name,
        "smoke_ignored_service_name",
        "smoke-bare-stale-other",
        4,
        &argv,
        None,
        &bare_log_path,
    )?;
    wait_for_selection_present(WorkloadKind::Deployment, &bare_name, &bare_name)?;
    let after_other_apply_bare_replace =
        observe_selection_status(WorkloadKind::Deployment, &bare_name, &bare_name)?;
    if !selection_status_matches_present(
        &after_other_apply_bare_replace,
        "smoke-bare-stale-other",
        4,
        argv.as_slice(),
        None,
    ) {
        anyhow::bail!(
            "supervisor smoke bare-mode later generation replace failed to take ownership: running={} present={} apply_id={:?}",
            after_other_apply_bare_replace.running,
            after_other_apply_bare_replace.present,
            after_other_apply_bare_replace.apply_id
        );
    }
    if after_other_apply_bare_replace.owner_ts_ms != Some(4) {
        anyhow::bail!(
            "supervisor smoke bare-mode later generation replace owner_ts_ms mismatch: got={:?}",
            after_other_apply_bare_replace.owner_ts_ms
        );
    }
    if after_other_apply_bare_replace.pid == Some(bare_takeover_supervisor_pid) {
        anyhow::bail!(
            "supervisor smoke bare-mode later generation replace did not publish a new supervisor pid: before_pid={} after_pid={:?}",
            bare_takeover_supervisor_pid,
            after_other_apply_bare_replace.pid
        );
    }

    runtime.stop(
        WorkloadKind::Deployment,
        &bare_name,
        true,
        Some("smoke-bare-stale-other"),
    )?;
    wait_for_selection_absent(
        WorkloadKind::Deployment,
        &bare_name,
        &bare_name,
        Some("smoke-bare-stale-other"),
    )?;
    drain_selection_generations(WorkloadKind::Deployment, &bare_name, &bare_name)?;

    let workloads =
        SupervisorBackedWorkloads::new(smoke_dir.clone(), log_dir.clone(), runtime.clone())?;
    let workloads_bare_name = format!("smoke_workloads_bare_{}", now_ts_ms());
    let workloads_bare_log_path = log_dir.join("smoke_workloads_bare.log");
    let workloads_bare_target = runtime.target(WorkloadKind::Deployment, &workloads_bare_name)?;
    let workloads_bare_state_json = serde_json::to_string(&serde_json::json!({
        "kind": WorkloadKind::Deployment,
        "name": workloads_bare_name.clone(),
        "authority": workloads_bare_name.clone(),
        "service_name": workloads_bare_name.clone(),
        "argv": argv.clone(),
        "log_path": workloads_bare_log_path.display().to_string(),
    }))
    .context("encode supervisor-backed bare smoke state json")?;
    let mut workloads_bare_command: Vec<String> = vec![
        python_exe.display().to_string(),
        runtime.script_path.display().to_string(),
        "run".to_string(),
        "--label".to_string(),
        workloads_bare_target.label.clone(),
        "--state-json".to_string(),
        workloads_bare_state_json,
        "--owner-ts-ms".to_string(),
        "1".to_string(),
        "--restart-policy".to_string(),
        "always".to_string(),
        "--restart-delay-seconds".to_string(),
        OPS_SELECTION_SUPERVISOR_RUN_RESTART_DELAY_SECONDS.to_string(),
        "--max-backoff-seconds".to_string(),
        OPS_SELECTION_SUPERVISOR_RUN_MAX_BACKOFF_SECONDS.to_string(),
        "--crashloop-consecutive-restarts".to_string(),
        "0".to_string(),
        "--crashloop-interval-lt-seconds".to_string(),
        "0".to_string(),
        "--".to_string(),
    ];
    workloads_bare_command.extend(argv.iter().cloned());
    runtime.spawn_detached_command(&workloads_bare_log_path, workloads_bare_command.as_slice())?;
    wait_for_selection_present(
        WorkloadKind::Deployment,
        &workloads_bare_name,
        &workloads_bare_name,
    )?;

    let workloads_takeover = workloads.start(StartReq {
        kind: WorkloadKind::Deployment,
        name: workloads_bare_name.clone(),
        authority: workloads_bare_name.clone(),
        service_name: workloads_bare_name.clone(),
        apply_id: "smoke-workloads-bare-takeover".to_string(),
        owner_ts_ms: 2,
        argv: argv.clone(),
        cwd: None,
        wait_for_attached: true,
        wait_for_present: true,
    });
    if !workloads_takeover.ok {
        anyhow::bail!(
            "supervisor-backed bare takeover start failed: err={}",
            workloads_takeover
                .err
                .unwrap_or_else(|| "unknown".to_string())
        );
    }
    let workloads_takeover_status = observe_selection_status(
        WorkloadKind::Deployment,
        &workloads_bare_name,
        &workloads_bare_name,
    )?;
    if !selection_status_matches_present(
        &workloads_takeover_status,
        "smoke-workloads-bare-takeover",
        2,
        argv.as_slice(),
        None,
    ) {
        anyhow::bail!(
            "supervisor-backed bare takeover mismatch: running={} present={} apply_id={:?}",
            workloads_takeover_status.running,
            workloads_takeover_status.present,
            workloads_takeover_status.apply_id
        );
    }
    if workloads_takeover_status.owner_ts_ms != Some(2) {
        anyhow::bail!(
            "supervisor-backed bare takeover owner_ts_ms mismatch: got={:?}",
            workloads_takeover_status.owner_ts_ms
        );
    }

    runtime.stop(
        WorkloadKind::Deployment,
        &workloads_bare_name,
        true,
        Some("smoke-workloads-bare-takeover"),
    )?;
    wait_for_selection_absent(
        WorkloadKind::Deployment,
        &workloads_bare_name,
        &workloads_bare_name,
        Some("smoke-workloads-bare-takeover"),
    )?;
    drain_selection_generations(
        WorkloadKind::Deployment,
        &workloads_bare_name,
        &workloads_bare_name,
    )?;

    let workloads_generation_name = format!("smoke_workloads_generation_{}", now_ts_ms());
    let workloads_generation_apply_1 = "smoke-workloads-generation-1".to_string();
    let workloads_generation_apply_2 = "smoke-workloads-generation-2".to_string();
    let workloads_generation_first = workloads.start(StartReq {
        kind: WorkloadKind::Deployment,
        name: workloads_generation_name.clone(),
        authority: workloads_generation_name.clone(),
        service_name: workloads_generation_name.clone(),
        apply_id: workloads_generation_apply_1.clone(),
        owner_ts_ms: 1,
        argv: argv.clone(),
        cwd: None,
        wait_for_attached: true,
        wait_for_present: true,
    });
    if !workloads_generation_first.ok {
        anyhow::bail!(
            "supervisor-backed generation smoke initial start failed: err={}",
            workloads_generation_first
                .err
                .unwrap_or_else(|| "unknown".to_string())
        );
    }
    let workloads_generation_first_status = observe_selection_status(
        WorkloadKind::Deployment,
        &workloads_generation_name,
        &workloads_generation_name,
    )?;
    if !selection_status_matches_present(
        &workloads_generation_first_status,
        workloads_generation_apply_1.as_str(),
        1,
        argv.as_slice(),
        None,
    ) {
        anyhow::bail!(
            "supervisor-backed generation smoke initial status mismatch: running={} present={} apply_id={:?}",
            workloads_generation_first_status.running,
            workloads_generation_first_status.present,
            workloads_generation_first_status.apply_id
        );
    }
    if workloads_generation_first_status.owner_ts_ms != Some(1) {
        anyhow::bail!(
            "supervisor-backed generation smoke initial owner_ts_ms mismatch: got={:?}",
            workloads_generation_first_status.owner_ts_ms
        );
    }
    let workloads_generation_first_pid =
        workloads_generation_first_status.pid.ok_or_else(|| {
            anyhow::anyhow!(
                "supervisor-backed generation smoke initial start missing supervisor pid"
            )
        })?;
    let workloads_generation_first_start_time_ticks = workloads_generation_first_status
        .supervisor_start_time_ticks
        .ok_or_else(|| {
            anyhow::anyhow!(
                "supervisor-backed generation smoke initial start missing start_time_ticks"
            )
        })?;

    let workloads_generation_second = workloads.start(StartReq {
        kind: WorkloadKind::Deployment,
        name: workloads_generation_name.clone(),
        authority: workloads_generation_name.clone(),
        service_name: workloads_generation_name.clone(),
        apply_id: workloads_generation_apply_2.clone(),
        owner_ts_ms: 2,
        argv: argv.clone(),
        cwd: None,
        wait_for_attached: true,
        wait_for_present: true,
    });
    if !workloads_generation_second.ok {
        anyhow::bail!(
            "supervisor-backed generation smoke replacement start failed: err={}",
            workloads_generation_second
                .err
                .unwrap_or_else(|| "unknown".to_string())
        );
    }
    let workloads_generation_second_status = observe_selection_status(
        WorkloadKind::Deployment,
        &workloads_generation_name,
        &workloads_generation_name,
    )?;
    if !selection_status_matches_present(
        &workloads_generation_second_status,
        workloads_generation_apply_2.as_str(),
        2,
        argv.as_slice(),
        None,
    ) {
        anyhow::bail!(
            "supervisor-backed generation smoke replacement status mismatch: running={} present={} apply_id={:?}",
            workloads_generation_second_status.running,
            workloads_generation_second_status.present,
            workloads_generation_second_status.apply_id
        );
    }
    if workloads_generation_second_status.owner_ts_ms != Some(2) {
        anyhow::bail!(
            "supervisor-backed generation smoke replacement owner_ts_ms mismatch: got={:?}",
            workloads_generation_second_status.owner_ts_ms
        );
    }
    wait_for_process_identity_absent(
        workloads_generation_first_pid,
        workloads_generation_first_start_time_ticks,
    )?;
    let workloads_generation_second_pid =
        workloads_generation_second_status.pid.ok_or_else(|| {
            anyhow::anyhow!("supervisor-backed generation smoke replacement missing supervisor pid")
        })?;

    let stale_generation_recovery = workloads.start(StartReq {
        kind: WorkloadKind::Deployment,
        name: workloads_generation_name.clone(),
        authority: workloads_generation_name.clone(),
        service_name: workloads_generation_name.clone(),
        apply_id: workloads_generation_apply_1.clone(),
        owner_ts_ms: 1,
        argv: argv.clone(),
        cwd: None,
        wait_for_attached: true,
        wait_for_present: true,
    });
    if stale_generation_recovery.ok {
        anyhow::bail!(
            "supervisor-backed generation smoke stale recovery must be rejected once newer startup exists"
        );
    }
    let stale_generation_err = stale_generation_recovery
        .err
        .clone()
        .unwrap_or_else(|| "unknown".to_string());
    if !stale_generation_err.contains("superseded by newer owner_ts_ms") {
        anyhow::bail!(
            "supervisor-backed generation smoke stale recovery returned unexpected error: {}",
            stale_generation_err
        );
    }
    let after_stale_generation_recovery = observe_selection_status(
        WorkloadKind::Deployment,
        &workloads_generation_name,
        &workloads_generation_name,
    )?;
    if !selection_status_matches_present(
        &after_stale_generation_recovery,
        workloads_generation_apply_2.as_str(),
        2,
        argv.as_slice(),
        None,
    ) {
        anyhow::bail!(
            "supervisor-backed generation smoke stale recovery changed owner unexpectedly: running={} present={} apply_id={:?}",
            after_stale_generation_recovery.running,
            after_stale_generation_recovery.present,
            after_stale_generation_recovery.apply_id
        );
    }
    if after_stale_generation_recovery.owner_ts_ms != Some(2) {
        anyhow::bail!(
            "supervisor-backed generation smoke stale recovery changed owner_ts_ms: got={:?}",
            after_stale_generation_recovery.owner_ts_ms
        );
    }
    if after_stale_generation_recovery.pid != Some(workloads_generation_second_pid) {
        anyhow::bail!(
            "supervisor-backed generation smoke stale recovery restarted supervisor unexpectedly: before_pid={} after_pid={:?}",
            workloads_generation_second_pid,
            after_stale_generation_recovery.pid
        );
    }

    runtime.stop(
        WorkloadKind::Deployment,
        &workloads_generation_name,
        true,
        Some(workloads_generation_apply_2.as_str()),
    )?;
    wait_for_selection_absent(
        WorkloadKind::Deployment,
        &workloads_generation_name,
        &workloads_generation_name,
        Some(workloads_generation_apply_2.as_str()),
    )?;
    drain_selection_generations(
        WorkloadKind::Deployment,
        &workloads_generation_name,
        &workloads_generation_name,
    )?;

    let workloads_backoff_name = format!("smoke_workloads_backoff_{}", now_ts_ms());
    let workloads_backoff_apply = "smoke-workloads-backoff".to_string();
    let backoff_argv = vec![
        python_exe.display().to_string(),
        "-c".to_string(),
        "import sys; sys.exit(1)".to_string(),
    ];
    let workloads_backoff_start = workloads.start(StartReq {
        kind: WorkloadKind::Deployment,
        name: workloads_backoff_name.clone(),
        authority: workloads_backoff_name.clone(),
        service_name: workloads_backoff_name.clone(),
        apply_id: workloads_backoff_apply.clone(),
        owner_ts_ms: 1,
        argv: backoff_argv.clone(),
        cwd: None,
        wait_for_attached: true,
        wait_for_present: false,
    });
    if !workloads_backoff_start.ok {
        anyhow::bail!(
            "supervisor-backed backoff smoke initial start failed: err={}",
            workloads_backoff_start
                .err
                .unwrap_or_else(|| "unknown".to_string())
        );
    }
    let workloads_backoff_absent_status = wait_for_selection_attached_without_present(
        WorkloadKind::Deployment,
        &workloads_backoff_name,
        &workloads_backoff_name,
        workloads_backoff_apply.as_str(),
        1,
        backoff_argv.as_slice(),
        None,
    )?;
    let workloads_backoff_supervisor_pid = workloads_backoff_absent_status
        .pid
        .ok_or_else(|| anyhow::anyhow!("supervisor-backed backoff smoke missing supervisor pid"))?;
    if workloads_backoff_absent_status.present {
        anyhow::bail!("supervisor-backed backoff smoke expected present=false during restart gap");
    }

    let workloads_backoff_reconcile = workloads.start(StartReq {
        kind: WorkloadKind::Deployment,
        name: workloads_backoff_name.clone(),
        authority: workloads_backoff_name.clone(),
        service_name: workloads_backoff_name.clone(),
        apply_id: workloads_backoff_apply.clone(),
        owner_ts_ms: 1,
        argv: backoff_argv.clone(),
        cwd: None,
        wait_for_attached: true,
        wait_for_present: false,
    });
    if !workloads_backoff_reconcile.ok {
        anyhow::bail!(
            "supervisor-backed backoff smoke reconcile start failed while child absent: err={}",
            workloads_backoff_reconcile
                .err
                .unwrap_or_else(|| "unknown".to_string())
        );
    }
    let after_backoff_reconcile = observe_selection_status(
        WorkloadKind::Deployment,
        &workloads_backoff_name,
        &workloads_backoff_name,
    )?;
    if !selection_status_matches_attached(
        &after_backoff_reconcile,
        workloads_backoff_apply.as_str(),
        1,
        backoff_argv.as_slice(),
        None,
    ) {
        anyhow::bail!(
            "supervisor-backed backoff smoke reconcile lost attached identity: running={} present={} apply_id={:?}",
            after_backoff_reconcile.running,
            after_backoff_reconcile.present,
            after_backoff_reconcile.apply_id
        );
    }
    if after_backoff_reconcile.pid != Some(workloads_backoff_supervisor_pid) {
        anyhow::bail!(
            "supervisor-backed backoff smoke reconcile restarted supervisor unexpectedly: before_pid={} after_pid={:?}",
            workloads_backoff_supervisor_pid,
            after_backoff_reconcile.pid
        );
    }

    runtime.stop(
        WorkloadKind::Deployment,
        &workloads_backoff_name,
        true,
        Some(workloads_backoff_apply.as_str()),
    )?;
    wait_for_selection_absent(
        WorkloadKind::Deployment,
        &workloads_backoff_name,
        &workloads_backoff_name,
        Some(workloads_backoff_apply.as_str()),
    )?;
    drain_selection_generations(
        WorkloadKind::Deployment,
        &workloads_backoff_name,
        &workloads_backoff_name,
    )?;

    let wait_present_failure_name = format!("smoke_workloads_wait_present_failure_{}", now_ts_ms());
    let wait_present_failure_apply = "smoke-workloads-wait-present-failure".to_string();
    let wait_present_failure_start = workloads.start(StartReq {
        kind: WorkloadKind::Deployment,
        name: wait_present_failure_name.clone(),
        authority: wait_present_failure_name.clone(),
        service_name: wait_present_failure_name.clone(),
        apply_id: wait_present_failure_apply.clone(),
        owner_ts_ms: 1,
        argv: backoff_argv.clone(),
        cwd: None,
        wait_for_attached: true,
        wait_for_present: true,
    });
    if wait_present_failure_start.ok {
        anyhow::bail!(
            "supervisor-backed wait-present failure smoke must fail when the child never becomes present"
        );
    }
    let wait_present_failure_err = wait_present_failure_start
        .err
        .clone()
        .unwrap_or_else(|| "unknown".to_string());
    if !wait_present_failure_err.contains("wait-present failed") {
        anyhow::bail!(
            "supervisor-backed wait-present failure smoke returned unexpected error: {}",
            wait_present_failure_err
        );
    }
    let after_wait_present_failure = observe_selection_status(
        WorkloadKind::Deployment,
        &wait_present_failure_name,
        &wait_present_failure_name,
    )?;
    if !selection_status_matches_attached(
        &after_wait_present_failure,
        wait_present_failure_apply.as_str(),
        1,
        backoff_argv.as_slice(),
        None,
    ) {
        anyhow::bail!(
            "supervisor-backed wait-present failure smoke lost attached identity after failed start: running={} present={} apply_id={:?}",
            after_wait_present_failure.running,
            after_wait_present_failure.present,
            after_wait_present_failure.apply_id
        );
    }
    if after_wait_present_failure.present {
        anyhow::bail!(
            "supervisor-backed wait-present failure smoke expected present=false after failed start"
        );
    }
    let wait_present_failure_supervisor_pid = after_wait_present_failure.pid.ok_or_else(|| {
        anyhow::anyhow!("supervisor-backed wait-present failure smoke missing supervisor pid")
    })?;
    let wait_present_failure_reconcile = workloads.start(StartReq {
        kind: WorkloadKind::Deployment,
        name: wait_present_failure_name.clone(),
        authority: wait_present_failure_name.clone(),
        service_name: wait_present_failure_name.clone(),
        apply_id: wait_present_failure_apply.clone(),
        owner_ts_ms: 1,
        argv: backoff_argv.clone(),
        cwd: None,
        wait_for_attached: true,
        wait_for_present: false,
    });
    if !wait_present_failure_reconcile.ok {
        anyhow::bail!(
            "supervisor-backed wait-present failure smoke reconcile start failed after preserving requested apply: err={}",
            wait_present_failure_reconcile
                .err
                .unwrap_or_else(|| "unknown".to_string())
        );
    }
    let after_wait_present_failure_reconcile = observe_selection_status(
        WorkloadKind::Deployment,
        &wait_present_failure_name,
        &wait_present_failure_name,
    )?;
    if !selection_status_matches_attached(
        &after_wait_present_failure_reconcile,
        wait_present_failure_apply.as_str(),
        1,
        backoff_argv.as_slice(),
        None,
    ) {
        anyhow::bail!(
            "supervisor-backed wait-present failure smoke reconcile lost attached identity: running={} present={} apply_id={:?}",
            after_wait_present_failure_reconcile.running,
            after_wait_present_failure_reconcile.present,
            after_wait_present_failure_reconcile.apply_id
        );
    }
    if after_wait_present_failure_reconcile.pid != Some(wait_present_failure_supervisor_pid) {
        anyhow::bail!(
            "supervisor-backed wait-present failure smoke restarted supervisor unexpectedly after failed start: before_pid={} after_pid={:?}",
            wait_present_failure_supervisor_pid,
            after_wait_present_failure_reconcile.pid
        );
    }

    runtime.stop(
        WorkloadKind::Deployment,
        &wait_present_failure_name,
        true,
        Some(wait_present_failure_apply.as_str()),
    )?;
    wait_for_selection_absent(
        WorkloadKind::Deployment,
        &wait_present_failure_name,
        &wait_present_failure_name,
        Some(wait_present_failure_apply.as_str()),
    )?;
    drain_selection_generations(
        WorkloadKind::Deployment,
        &wait_present_failure_name,
        &wait_present_failure_name,
    )?;

    Ok(())
}

async fn controller_repair_missing_apply_records_from_history(
    desired: &DesiredStore,
    history_dir: &Path,
) -> anyhow::Result<()> {
    // English note:
    // - Older ops_controller versions persisted the deployment YAML only in history/<apply_id>.json.
    // - For self-host applies, ops_controller may be stopped/replaced before that history write completes.
    // - New versions persist YAML under desired/applies/<apply_id>.json so "Show YAML / Reapply" does not depend
    //   on history being present.
    // - This function performs a deterministic one-time repair for legacy states:
    //   if desired references an apply_id, and desired/applies is missing, but history/<apply_id>.json exists,
    //   then we recreate desired/applies/<apply_id>.json from that history record.
    let desired_snapshot = desired.snapshot();
    let mut apply_ids: BTreeSet<String> = BTreeSet::new();
    for w in desired_snapshot.iter() {
        let Some(apply_id) = w.apply_id.as_deref() else {
            continue;
        };
        let apply_id = apply_id.trim();
        if apply_id.is_empty() {
            continue;
        }
        apply_ids.insert(apply_id.to_string());
    }

    let mut repaired = 0u64;
    for apply_id in apply_ids.into_iter() {
        if desired.apply_record_exists(&apply_id)? {
            continue;
        }

        let hist_path =
            history_dir.join(format!("{}.json", validate_apply_id_for_file(&apply_id)?));
        if tokio::fs::metadata(&hist_path).await.is_err() {
            continue;
        }

        let buf = tokio::fs::read(&hist_path).await.with_context(|| {
            format!(
                "read legacy history for apply record repair: {}",
                hist_path.display()
            )
        })?;
        let hist: DeployHistoryRecord = serde_json::from_slice(&buf).with_context(|| {
            format!(
                "decode legacy history json failed during apply record repair: {}",
                hist_path.display()
            )
        })?;
        if hist.id.trim() != apply_id {
            anyhow::bail!(
                "apply record repair: history id mismatch: expected apply_id={} got history.id={} path={}",
                apply_id,
                hist.id,
                hist_path.display()
            );
        }

        let deployment_yaml_sha256 = sha256_hex(hist.deployment_yaml.as_bytes());
        let rec = DeployApplyRecord {
            id: apply_id.clone(),
            ts_ms: hist.ts_ms,
            deployment_yaml: hist.deployment_yaml,
            namespace: hist.namespace,
            deployment_yaml_sha256,
            lifecycle_phase: None,
            lifecycle_phase_updated_ts_ms: None,
        };
        desired.persist_apply_record(&rec).await.with_context(|| {
            format!(
                "persist repaired desired apply record: apply_id={}",
                apply_id
            )
        })?;
        repaired += 1;
    }

    if repaired > 0 {
        eprintln!(
            "[ops_controller:init] repaired missing desired apply record(s) from history count={}",
            repaired
        );
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DesiredWorkload {
    kind: WorkloadKind,
    name: String,
    #[serde(default)]
    logical_selection: String,
    #[serde(default)]
    service_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    atomic_group: Option<AtomicGroupMeta>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    namespace: Option<String>,
    targets: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    apply_id: Option<String>,
    exec_argv: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exec_cwd: Option<String>,
    updated_ts_ms: u64,
}

fn normalize_desired_workload(mut w: DesiredWorkload) -> DesiredWorkload {
    if w.logical_selection.trim().is_empty() {
        w.logical_selection = w.name.clone();
    }
    if w.service_name.trim().is_empty() {
        w.service_name = w.logical_selection.clone();
    }
    if let Some(group) = w.atomic_group.as_mut() {
        if group.selection_name.trim().is_empty() {
            group.selection_name = w.logical_selection.clone();
        }
    }
    w
}

fn desired_workload_authority(w: &DesiredWorkload) -> anyhow::Result<String> {
    selection_authority_name(
        w.kind,
        w.logical_selection.as_str(),
        w.service_name.as_str(),
        w.atomic_group.as_ref(),
    )
}

struct DesiredStore {
    workloads_dir: PathBuf,
    applies_dir: PathBuf,
    inner: std::sync::Mutex<BTreeMap<String, DesiredWorkload>>,
    persist_guard: tokio::sync::Mutex<()>,
}

impl DesiredStore {
    async fn load(dir: PathBuf) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(&dir)
            .await
            .with_context(|| format!("create desired dir: {}", dir.display()))?;

        let workloads_dir = dir.join(OPS_DESIRED_WORKLOADS_DIR_NAME);
        tokio::fs::create_dir_all(&workloads_dir)
            .await
            .with_context(|| {
                format!("create desired workloads dir: {}", workloads_dir.display())
            })?;

        let applies_dir = dir.join(OPS_DESIRED_APPLIES_DIR_NAME);
        tokio::fs::create_dir_all(&applies_dir)
            .await
            .with_context(|| format!("create desired applies dir: {}", applies_dir.display()))?;

        // Load new per-workload desired files first. If empty and the legacy single-file desired exists,
        // perform a one-time migration (no behavior branching afterwards).
        let mut inner = load_desired_map_from_dir(&workloads_dir).await?;
        if inner.is_empty() {
            let legacy_path = dir.join(OPS_DESIRED_FILENAME);
            if tokio::fs::metadata(&legacy_path).await.is_ok() {
                let legacy = load_legacy_desired_map(&legacy_path).await?;
                for w in legacy.values() {
                    persist_one_desired_workload_file(&workloads_dir, w).await?;
                }
                let ts_ms = now_ts_ms();
                let migrated_path =
                    dir.join(format!("{}.migrated.{}", OPS_DESIRED_FILENAME, ts_ms));
                tokio::fs::rename(&legacy_path, &migrated_path)
                    .await
                    .with_context(|| {
                        format!(
                            "rename legacy desired file: {} -> {}",
                            legacy_path.display(),
                            migrated_path.display()
                        )
                    })?;
                inner = legacy;
            }
        }

        Ok(Self {
            workloads_dir,
            applies_dir,
            inner: std::sync::Mutex::new(inner),
            persist_guard: tokio::sync::Mutex::new(()),
        })
    }

    fn snapshot(&self) -> Vec<DesiredWorkload> {
        let g = self.inner.lock().unwrap();
        g.values().cloned().collect()
    }

    fn apply_record_exists(&self, apply_id: &str) -> anyhow::Result<bool> {
        let path = desired_apply_record_file_path(&self.applies_dir, apply_id)?;
        Ok(path.exists())
    }

    async fn persist_apply_record(&self, rec: &DeployApplyRecord) -> anyhow::Result<()> {
        // Serialize persistence so on-disk desired is never interleaved by concurrent HTTP requests.
        let _guard = self.persist_guard.lock().await;
        persist_one_desired_apply_record_file(&self.applies_dir, rec).await?;
        Ok(())
    }

    fn snapshot_apply_workloads(&self, apply_id: &str) -> Vec<DesiredWorkload> {
        let apply_id = apply_id.trim();
        if apply_id.is_empty() {
            return Vec::new();
        }
        let g = self.inner.lock().unwrap();
        g.values()
            .filter(|w| w.apply_id.as_deref().map(|s| s.trim()) == Some(apply_id))
            .cloned()
            .collect()
    }

    async fn load_apply_record(&self, apply_id: &str) -> anyhow::Result<DeployApplyRecord> {
        let path = desired_apply_record_file_path(&self.applies_dir, apply_id)?;
        let buf = tokio::fs::read(&path)
            .await
            .with_context(|| format!("read desired apply record file: {}", path.display()))?;
        let mut rec: DeployApplyRecord = serde_json::from_slice(&buf).with_context(|| {
            format!(
                "decode desired apply record json failed: {}",
                path.display()
            )
        })?;
        if rec.deployment_yaml_sha256.trim().is_empty() {
            // English note:
            // - Keep "apply record has a stable manifest fingerprint" as an invariant.
            // - Repair is deterministic (sha256 over the stored YAML), and is persisted immediately so
            //   subsequent reads do not branch on legacy/missing fields.
            rec.deployment_yaml_sha256 = sha256_hex(rec.deployment_yaml.as_bytes());
            self.persist_apply_record(&rec).await.with_context(|| {
                format!("repair desired apply record sha256: apply_id={}", apply_id)
            })?;
        }
        Ok(rec)
    }

    async fn load_apply_records(&self) -> anyhow::Result<Vec<DeployApplyRecord>> {
        let mut rd = tokio::fs::read_dir(&self.applies_dir)
            .await
            .with_context(|| format!("read desired applies dir: {}", self.applies_dir.display()))?;
        let mut out: Vec<DeployApplyRecord> = Vec::new();
        while let Some(ent) = rd.next_entry().await.with_context(|| {
            format!(
                "read desired applies dir entry: {}",
                self.applies_dir.display()
            )
        })? {
            let path = ent.path();
            if path.extension().and_then(|v| v.to_str()) != Some("json") {
                continue;
            }
            let Some(apply_id) = path.file_stem().and_then(|v| v.to_str()) else {
                anyhow::bail!(
                    "desired apply record file name must be utf-8: {}",
                    path.display()
                );
            };
            out.push(self.load_apply_record(apply_id).await?);
        }
        out.sort_by(|left, right| left.ts_ms.cmp(&right.ts_ms).then(left.id.cmp(&right.id)));
        Ok(out)
    }

    fn load_apply_records_blocking(&self) -> anyhow::Result<Vec<DeployApplyRecord>> {
        let mut out: Vec<DeployApplyRecord> = Vec::new();
        for ent in std::fs::read_dir(&self.applies_dir)
            .with_context(|| format!("read desired applies dir: {}", self.applies_dir.display()))?
        {
            let ent = ent.with_context(|| {
                format!(
                    "read desired applies dir entry: {}",
                    self.applies_dir.display()
                )
            })?;
            let path = ent.path();
            if path.extension().and_then(|v| v.to_str()) != Some("json") {
                continue;
            }
            let buf = std::fs::read(&path)
                .with_context(|| format!("read desired apply record file: {}", path.display()))?;
            let rec: DeployApplyRecord = serde_json::from_slice(&buf).with_context(|| {
                format!(
                    "decode desired apply record json failed: {}",
                    path.display()
                )
            })?;
            out.push(rec);
        }
        out.sort_by(|left, right| left.ts_ms.cmp(&right.ts_ms).then(left.id.cmp(&right.id)));
        Ok(out)
    }

    async fn update_apply_lifecycle_phase(
        &self,
        apply_id: &str,
        phase: ApplyLifecyclePhase,
    ) -> anyhow::Result<()> {
        let apply_id = validate_apply_id_for_file(apply_id)?;
        let mut rec = self.load_apply_record(&apply_id).await?;
        if apply_lifecycle_phase_normalized(rec.lifecycle_phase) == phase {
            return Ok(());
        }
        rec.lifecycle_phase = Some(phase);
        rec.lifecycle_phase_updated_ts_ms = Some(now_ts_ms());
        self.persist_apply_record(&rec).await.with_context(|| {
            format!(
                "persist apply lifecycle_phase={:?}: apply_id={}",
                phase, apply_id
            )
        })?;
        Ok(())
    }

    async fn mark_apply_delete_notifying(&self, apply_id: &str) -> anyhow::Result<()> {
        self.update_apply_lifecycle_phase(apply_id, ApplyLifecyclePhase::DeleteNotifying)
            .await
    }

    async fn upsert_many(&self, workloads: Vec<DesiredWorkload>) -> anyhow::Result<()> {
        // Serialize persistence so on-disk desired is never interleaved by concurrent HTTP requests.
        let _guard = self.persist_guard.lock().await;
        let changed: Vec<DesiredWorkload> = {
            let mut g = self.inner.lock().unwrap();
            let mut changed: Vec<DesiredWorkload> = Vec::new();
            for w in workloads.into_iter() {
                let key = workload_key(w.kind, &w.name);
                g.insert(key, w.clone());
                changed.push(w);
            }
            changed
        };
        for w in changed.iter() {
            persist_one_desired_workload_file(&self.workloads_dir, w).await?;
        }
        Ok(())
    }

    async fn remove(&self, kind: WorkloadKind, name: &str) -> anyhow::Result<()> {
        // Serialize persistence so on-disk desired is never interleaved by concurrent HTTP requests.
        let _guard = self.persist_guard.lock().await;
        let removed: Option<DesiredWorkload> = {
            let mut g = self.inner.lock().unwrap();
            let key = workload_key(kind, name);
            let removed = g.remove(&key);
            removed
        };
        let Some(removed) = removed else {
            return Ok(());
        };
        // Remove is keyed by kind+name; delete the corresponding desired file.
        let path = desired_workload_file_path(&self.workloads_dir, removed.kind, &removed.name)?;
        match tokio::fs::remove_file(&path).await {
            Ok(_) => {}
            Err(e) => {
                if e.kind() != std::io::ErrorKind::NotFound {
                    return Err(anyhow::Error::new(e)
                        .context(format!("remove desired workload file: {}", path.display())));
                }
            }
        }
        Ok(())
    }

    async fn remove_apply(&self, apply_id: &str) -> anyhow::Result<Vec<DesiredWorkload>> {
        let apply_id = validate_apply_id_for_file(apply_id)?;

        // Serialize persistence so on-disk desired is never interleaved by concurrent HTTP requests.
        let _guard = self.persist_guard.lock().await;

        let apply_path = desired_apply_record_file_path(&self.applies_dir, &apply_id)?;
        if tokio::fs::metadata(&apply_path).await.is_err() {
            anyhow::bail!(
                "apply record not found under desired/applies: apply_id={}",
                apply_id
            );
        }

        let removed: Vec<DesiredWorkload> = {
            let mut g = self.inner.lock().unwrap();
            let keys: Vec<String> = g
                .iter()
                .filter_map(|(k, w)| {
                    if w.apply_id.as_deref().map(|s| s.trim()) == Some(apply_id.as_str()) {
                        Some(k.clone())
                    } else {
                        None
                    }
                })
                .collect();

            let mut removed: Vec<DesiredWorkload> = Vec::new();
            for k in keys {
                if let Some(w) = g.remove(&k) {
                    removed.push(w);
                }
            }
            removed
        };

        for w in removed.iter() {
            let path = desired_workload_file_path(&self.workloads_dir, w.kind, &w.name)?;
            match tokio::fs::remove_file(&path).await {
                Ok(_) => {}
                Err(e) => {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        return Err(anyhow::Error::new(e)
                            .context(format!("remove desired workload file: {}", path.display())));
                    }
                }
            }
        }

        match tokio::fs::remove_file(&apply_path).await {
            Ok(_) => {}
            Err(e) => {
                if e.kind() != std::io::ErrorKind::NotFound {
                    return Err(anyhow::Error::new(e).context(format!(
                        "remove desired apply record file: {}",
                        apply_path.display()
                    )));
                }
            }
        }

        Ok(removed)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyDesiredStateFile {
    workloads: Vec<DesiredWorkload>,
}

async fn load_legacy_desired_map(path: &Path) -> anyhow::Result<BTreeMap<String, DesiredWorkload>> {
    let buf = tokio::fs::read(path)
        .await
        .with_context(|| format!("read legacy desired state file: {}", path.display()))?;

    let f: LegacyDesiredStateFile = serde_json::from_slice(&buf)
        .with_context(|| format!("decode legacy desired json failed: {}", path.display()))?;

    let mut out: BTreeMap<String, DesiredWorkload> = BTreeMap::new();
    for w in f.workloads.into_iter() {
        let w = normalize_desired_workload(w);
        let key = workload_key(w.kind, &w.name);
        if out.contains_key(&key) {
            anyhow::bail!("duplicate desired workload key: {}", key);
        }
        out.insert(key, w);
    }

    Ok(out)
}

async fn load_desired_map_from_dir(
    workloads_dir: &Path,
) -> anyhow::Result<BTreeMap<String, DesiredWorkload>> {
    let mut out: BTreeMap<String, DesiredWorkload> = BTreeMap::new();

    let mut rd = match tokio::fs::read_dir(workloads_dir).await {
        Ok(v) => v,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                return Ok(BTreeMap::new());
            }
            return Err(anyhow::Error::new(e).context(format!(
                "read desired workloads dir: {}",
                workloads_dir.display()
            )));
        }
    };

    while let Some(ent) = rd.next_entry().await.with_context(|| {
        format!(
            "read desired workloads dir entry: {}",
            workloads_dir.display()
        )
    })? {
        let ftype = ent
            .file_type()
            .await
            .with_context(|| format!("stat desired workload file: {}", ent.path().display()))?;
        if !ftype.is_file() {
            continue;
        }
        let path = ent.path();
        let Some(file_name) = path.file_name().and_then(|v| v.to_str()) else {
            anyhow::bail!(
                "desired workload file name must be utf-8: {}",
                path.display()
            );
        };
        if !file_name.ends_with(".json") {
            continue;
        }
        let buf = tokio::fs::read(&path)
            .await
            .with_context(|| format!("read desired workload file: {}", path.display()))?;
        let w: DesiredWorkload = serde_json::from_slice(&buf)
            .with_context(|| format!("decode desired workload json failed: {}", path.display()))?;
        let w = normalize_desired_workload(w);

        let expected_path = desired_workload_file_path(workloads_dir, w.kind, &w.name)?;
        if expected_path != path {
            anyhow::bail!(
                "desired workload file name mismatch: expected={} actual={}",
                expected_path.display(),
                path.display()
            );
        }

        let key = workload_key(w.kind, &w.name);
        if out.contains_key(&key) {
            anyhow::bail!("duplicate desired workload key: {}", key);
        }
        out.insert(key, w);
    }

    Ok(out)
}

fn validate_workload_name_for_file(name: &str) -> anyhow::Result<String> {
    let s = name.trim().to_string();
    if s.is_empty() {
        anyhow::bail!("workload name must be non-empty");
    }
    if s.contains('/') || s.contains('\\') {
        anyhow::bail!("workload name must not contain path separators: {}", s);
    }
    if s == "." || s == ".." {
        anyhow::bail!("workload name is reserved: {}", s);
    }
    Ok(s)
}

fn desired_workload_file_path(
    workloads_dir: &Path,
    kind: WorkloadKind,
    name: &str,
) -> anyhow::Result<PathBuf> {
    let name = validate_workload_name_for_file(name)?;
    Ok(workloads_dir.join(format!("{}__{}.json", kind.as_str(), name)))
}

async fn persist_one_desired_workload_file(
    workloads_dir: &Path,
    w: &DesiredWorkload,
) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(workloads_dir)
        .await
        .with_context(|| format!("create desired workloads dir: {}", workloads_dir.display()))?;
    let final_path = desired_workload_file_path(workloads_dir, w.kind, &w.name)?;
    let tmp = final_path.with_extension("json.tmp");
    let buf = serde_json::to_vec(w).context("encode desired workload json")?;
    tokio::fs::write(&tmp, buf)
        .await
        .with_context(|| format!("write desired workload tmp: {}", tmp.display()))?;
    tokio::fs::rename(&tmp, &final_path)
        .await
        .with_context(|| {
            format!(
                "rename desired workload tmp: {} -> {}",
                tmp.display(),
                final_path.display()
            )
        })?;
    Ok(())
}

fn validate_apply_id_for_file(id: &str) -> anyhow::Result<String> {
    let s = id.trim().to_string();
    if s.is_empty() {
        anyhow::bail!("apply_id must be non-empty");
    }
    if s.contains('/') || s.contains('\\') {
        anyhow::bail!("apply_id must not contain path separators: {}", s);
    }
    if s == "." || s == ".." {
        anyhow::bail!("apply_id is reserved: {}", s);
    }
    Ok(s)
}

fn desired_apply_record_file_path(applies_dir: &Path, apply_id: &str) -> anyhow::Result<PathBuf> {
    let apply_id = validate_apply_id_for_file(apply_id)?;
    Ok(applies_dir.join(format!("{}.json", apply_id)))
}

async fn persist_one_desired_apply_record_file(
    applies_dir: &Path,
    rec: &DeployApplyRecord,
) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(applies_dir)
        .await
        .with_context(|| format!("create desired applies dir: {}", applies_dir.display()))?;
    let final_path = desired_apply_record_file_path(applies_dir, &rec.id)?;
    let tmp = final_path.with_extension("json.tmp");
    let buf = serde_json::to_vec(rec).context("encode desired apply record json")?;
    tokio::fs::write(&tmp, buf)
        .await
        .with_context(|| format!("write desired apply record tmp: {}", tmp.display()))?;
    tokio::fs::rename(&tmp, &final_path)
        .await
        .with_context(|| {
            format!(
                "rename desired apply record tmp: {} -> {}",
                tmp.display(),
                final_path.display()
            )
        })?;
    Ok(())
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
enum DeployPhase {
    Accepted,
    Failed,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct DeployResp {
    ok: bool,
    phase: DeployPhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    history_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    results: Option<Vec<PerTargetDeployResult>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeployApplyRecord {
    id: String,
    ts_ms: u64,
    deployment_yaml: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    namespace: Option<String>,
    // English note:
    // - This is a diagnostic fingerprint of `deployment_yaml` (the raw manifest submitted to /api/deploy).
    // - Older apply records may not have this field; we repair it on read to avoid breaking existing state.
    #[serde(default)]
    deployment_yaml_sha256: String,
    // English note:
    // - This is the lifecycle phase of the apply record itself (control-plane intent).
    // - `None` means the record was created before phases existed; treat it as RUNNING.
    // - Delete-notify / delete-commit are persisted so pull-repair and delete_wait can observe the
    //   same controller authority across retries and controller restarts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lifecycle_phase: Option<ApplyLifecyclePhase>,
    // English note:
    // - This records when controller intent last changed phase.
    // - Delete-notify delay is now agent-local from first observation, so this timestamp is retained
    //   only for lifecycle diagnostics and operator inspection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lifecycle_phase_updated_ts_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ApplyLifecyclePhase {
    Running,
    DeleteNotifying,
    #[serde(alias = "STOPPING")]
    DeleteCommitted,
}

fn apply_lifecycle_phase_normalized(v: Option<ApplyLifecyclePhase>) -> ApplyLifecyclePhase {
    match v {
        Some(p) => p,
        None => ApplyLifecyclePhase::Running,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ApplyRuntimeGoal {
    Attached,
    Detached,
}

impl ApplyRuntimeGoal {
    fn as_str(&self) -> &'static str {
        match self {
            ApplyRuntimeGoal::Attached => "ATTACHED",
            ApplyRuntimeGoal::Detached => "DETACHED",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeployHistoryRecord {
    id: String,
    ts_ms: u64,
    deployment_yaml: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deployment_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    upload_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    upload_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    upload_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    targets: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload_dest_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exec_argv: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exec_cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    results: Option<Vec<PerTargetDeployResult>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
struct DeployHistorySummary {
    id: String,
    ts_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deployment_name: Option<String>,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
}

fn response_plain(status: StatusCode, body: &str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn response_html(status: StatusCode, body: String) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "text/html; charset=utf-8")
        .body(Body::from(body))
        .unwrap()
}

fn response_json<T: Serialize>(status: StatusCode, v: &T) -> Response<Body> {
    let body = match serde_json::to_vec(v) {
        Ok(buf) => buf,
        Err(e) => {
            return response_plain(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("json encode failed: {}", e),
            );
        }
    };

    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap()
}

fn validate_password_no_whitespace(value: &str, label: &str) -> anyhow::Result<String> {
    let trimmed = value.trim().to_string();
    if trimmed.is_empty() {
        anyhow::bail!("{} must be non-empty", label);
    }
    if trimmed != value {
        anyhow::bail!("{} must not have leading/trailing whitespace", label);
    }
    Ok(trimmed)
}

fn validate_username_for_basic_auth(value: &str, label: &str) -> anyhow::Result<String> {
    let trimmed = value.trim().to_string();
    if trimmed.is_empty() {
        anyhow::bail!("{} must be non-empty", label);
    }
    if trimmed != value {
        anyhow::bail!("{} must not have leading/trailing whitespace", label);
    }
    if trimmed.contains(':') {
        anyhow::bail!(
            "{} must not contain ':' because Basic auth uses username:password",
            label
        );
    }
    Ok(trimmed)
}

fn ops_panel_basic_auth_required_response() -> Response<Body> {
    let mut resp = response_plain(StatusCode::UNAUTHORIZED, "basic auth required");
    resp.headers_mut().insert(
        hyper::header::WWW_AUTHENTICATE,
        hyper::header::HeaderValue::from_static("Basic realm=\"fluxon_ops\""),
    );
    resp
}

fn ops_panel_basic_auth_matches(headers: &hyper::HeaderMap, auth: &PanelAuthConfigYaml) -> bool {
    // English note:
    // - Browser UI uses the standard Authorization header for Basic auth.
    // - Automation may also need Authorization for upstream protocols (for example fs_s3 SigV4),
    //   so ops accepts one dedicated side-channel header with the same "Basic <base64>" value.
    let Some(v) = headers
        .get(hyper::header::AUTHORIZATION)
        .or_else(|| headers.get(HDR_OPS_PANEL_AUTHORIZATION))
        .and_then(|v| v.to_str().ok())
    else {
        return false;
    };
    let mut parts = v.splitn(2, ' ');
    let scheme = parts.next().unwrap_or("");
    if !scheme.eq_ignore_ascii_case("Basic") {
        return false;
    }
    let encoded = parts.next().unwrap_or("").trim();
    if encoded.is_empty() {
        return false;
    }
    let raw = match base64::engine::general_purpose::STANDARD.decode(encoded) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let s = String::from_utf8_lossy(&raw);
    let Some((username, password)) = s.split_once(':') else {
        return false;
    };
    username == auth.username && password == auth.password
}

fn ops_panel_path_requires_auth(path: &str) -> bool {
    !matches!(path, "/healthz" | "/readyz" | "/api/health")
        && path != "/fluxon"
        && !path.starts_with("/fluxon/")
}

fn ops_panel_maybe_require_auth(
    cfg: &ControllerConfigYaml,
    req: &Request<Body>,
) -> Option<Response<Body>> {
    let path = req.uri().path();
    let path = if path.is_empty() { "/" } else { path };
    if !ops_panel_path_requires_auth(path) {
        return None;
    }
    if ops_panel_basic_auth_matches(req.headers(), &cfg.panel.auth) {
        return None;
    }
    Some(ops_panel_basic_auth_required_response())
}

fn query_param(uri: &hyper::Uri, key: &str) -> Option<String> {
    let q = uri.query()?;
    for part in q.split('&') {
        let mut it = part.splitn(2, '=');
        let k = it.next().unwrap_or("");
        if k != key {
            continue;
        }
        let v = it.next().unwrap_or("");
        return Some(v.to_string());
    }
    None
}

fn append_raw_query(url: &mut String, raw_query: Option<&str>) {
    let Some(qs) = raw_query else {
        return;
    };
    if qs.is_empty() {
        return;
    }
    if url.contains('?') {
        url.push('&');
    } else {
        url.push('?');
    }
    url.push_str(qs);
}

fn ops_proxy_original_host(req: &Request<Body>) -> anyhow::Result<String> {
    let host = req
        .headers()
        .get(HDR_PROXY_ORIGINAL_HOST)
        .or_else(|| req.headers().get(hyper::header::HOST))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "missing proxy original host header: {}",
                HDR_PROXY_ORIGINAL_HOST
            )
        })?;
    let host = host
        .to_str()
        .map_err(|e| anyhow::anyhow!("invalid proxy original host header: {}", e))?
        .trim()
        .to_string();
    if host.is_empty() {
        anyhow::bail!(
            "empty proxy original host header: {}",
            HDR_PROXY_ORIGINAL_HOST
        );
    }
    Ok(host)
}

fn ops_proxy_upstream_url(
    req: &Request<Body>,
    upstream_path_and_query: &str,
) -> anyhow::Result<String> {
    let host = ops_proxy_original_host(req)?;
    if !upstream_path_and_query.starts_with('/') {
        anyhow::bail!(
            "invalid upstream path (must start with '/'): {}",
            upstream_path_and_query
        );
    }

    // English note:
    // - The browser always reaches ops via the same fluxon_cli HTTP entrypoint.
    // - ops_controller receives the original Host via proxy headers from fluxon_cli.
    // - We proxy back to that same front door so browser traffic stays on ops-scoped URLs while
    //   the business pages remain implemented by their original handlers/panels.
    Ok(format!("http://{}{}", host, upstream_path_and_query))
}

async fn proxy_ops_fluxon_http(
    req: Request<Body>,
    upstream_path_and_query: String,
) -> anyhow::Result<Response<Body>> {
    let url = ops_proxy_upstream_url(&req, &upstream_path_and_query)?;
    let method = reqwest::Method::from_bytes(req.method().as_str().as_bytes())
        .map_err(|e| anyhow::anyhow!("invalid upstream method: {}", e))?;

    let client = reqwest::Client::new();
    let mut rb = client.request(method, url);
    for (k, v) in req.headers().iter() {
        let name = k.as_str();
        if should_skip_panel_proxy_header(name, &PANEL_PROXY_SKIP_REQ_HEADERS) {
            continue;
        }
        if name.eq_ignore_ascii_case(HDR_PROXY_ORIGINAL_URI)
            || name.eq_ignore_ascii_case(HDR_PROXY_ORIGINAL_HOST)
        {
            continue;
        }
        let Ok(vs) = v.to_str() else {
            continue;
        };
        rb = rb.header(name, vs);
    }

    let req_body = hyper::body::to_bytes(req.into_body())
        .await
        .map_err(|e| anyhow::anyhow!("read proxy request body failed: {}", e))?;
    let upstream = rb
        .body(req_body.to_vec())
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("ops proxy upstream request failed: {}", e))?;

    let status = upstream.status();
    let headers = upstream.headers().clone();
    let body = upstream
        .bytes()
        .await
        .map_err(|e| anyhow::anyhow!("ops proxy upstream read body failed: {}", e))?;

    let mut resp = Response::builder()
        .status(status.as_u16())
        .body(Body::from(body.to_vec()))
        .unwrap();
    for (k, v) in headers.iter() {
        let name = k.as_str();
        if should_skip_panel_proxy_header(name, &PANEL_PROXY_SKIP_RESP_HEADERS) {
            continue;
        }
        let Ok(name2) = hyper::header::HeaderName::from_bytes(name.as_bytes()) else {
            continue;
        };
        let Ok(value2) = hyper::header::HeaderValue::from_bytes(v.as_bytes()) else {
            continue;
        };
        resp.headers_mut().append(name2, value2);
    }
    Ok(resp)
}

async fn handle_fluxon_embedded_proxy(
    state: Arc<ControllerHttpState>,
    req: Request<Body>,
) -> anyhow::Result<Response<Body>> {
    let cluster_name = state
        .cfg
        .kv_client
        .fluxonkv_spec
        .cluster_name
        .trim()
        .to_string();
    if cluster_name.is_empty() {
        anyhow::bail!("ops controller cluster_name must be non-empty");
    }

    let path = req.uri().path().to_string();
    let raw_query = req.uri().query();

    let upstream = if path == "/fluxon/kv" {
        let mut url = format!("/view?cluster_name={}&member_kind=kv", cluster_name);
        append_raw_query(&mut url, raw_query);
        url
    } else if path == "/fluxon/mq" {
        let mut url = format!("/view?cluster_name={}&member_kind=mq", cluster_name);
        append_raw_query(&mut url, raw_query);
        url
    } else if path == "/fluxon/topology" {
        let mut url = format!("/topology?cluster_name={}", cluster_name);
        append_raw_query(&mut url, raw_query);
        url
    } else if path == "/fluxon/fs" || path == "/fluxon/fs/" {
        let mut url = format!("/r/fs_s3/{}/ui/", cluster_name);
        append_raw_query(&mut url, raw_query);
        url
    } else if let Some(rest) = path.strip_prefix("/fluxon/fs/") {
        let mut url = format!("/r/fs_s3/{}/ui/{}", cluster_name, rest);
        append_raw_query(&mut url, raw_query);
        url
    } else {
        anyhow::bail!("unsupported embedded fluxon path: {}", path);
    };

    proxy_ops_fluxon_http(req, upstream).await
}

// Keep the ops controller UI in a single Rust file and render it via Askama to avoid
// manual escaping bugs when embedding HTML/JS in Rust string literals.
#[derive(Template)]
#[template(
    source = r#"<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Fluxon Ops</title>
  <style>
    :root {
      --primary: #1677ff;
      --success: #52c41a;
      --error: #ff4d4f;
      --warning: #faad14;
      --bg-main: #f0f2f5;
      --bg-card: #ffffff;
      --text-title: #000000e0;
      --text-secondary: #00000073;
      --border: #f0f0f0;
      --radius: 6px;
    }

    body {
      margin: 0;
      font-family: -apple-system, "Segoe UI", sans-serif;
      background: var(--bg-main);
      color: var(--text-title);
      display: flex;
      height: 100vh;
    }

    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
    .muted { color: var(--text-secondary); }
    .ok { color: #065f46; }
    .bad { color: #991b1b; }

    textarea {
      width: 100%;
      height: 360px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      border: 1px solid var(--border);
      border-radius: 4px;
      padding: 10px 12px;
      box-sizing: border-box;
      outline: none;
    }

    pre { background: #0b1020; color: #e5e7eb; padding: 12px; border-radius: 6px; overflow-x: auto; }
    pre.diff { background: #0b1020; color: #e5e7eb; }
    .diff-add { color: #34d399; }
    .diff-del { color: #f87171; }
    .diff-eq { color: #9ca3af; }

    /* ANSI SGR rendering (workload logs). */
    .ansi-dim { opacity: 0.72; }
    .ansi-bold { font-weight: 600; }
    .ansi-fg-black { color: #111827; }
    .ansi-fg-red { color: #f87171; }
    .ansi-fg-green { color: #34d399; }
    .ansi-fg-yellow { color: #fbbf24; }
    .ansi-fg-blue { color: #60a5fa; }
    .ansi-fg-magenta { color: #c084fc; }
    .ansi-fg-cyan { color: #22d3ee; }
    .ansi-fg-white { color: #f3f4f6; }
    .ansi-fg-bright-black { color: #9ca3af; }
    .ansi-fg-bright-red { color: #fecaca; }
    .ansi-fg-bright-green { color: #bbf7d0; }
    .ansi-fg-bright-yellow { color: #fde68a; }
    .ansi-fg-bright-blue { color: #bfdbfe; }
    .ansi-fg-bright-magenta { color: #e9d5ff; }
    .ansi-fg-bright-cyan { color: #a5f3fc; }
    .ansi-fg-bright-white { color: #ffffff; }

    /* Sidebar (minimal). */
    .sidebar {
      width: 220px;
      background: #001529;
      color: white;
      display: flex;
      flex-direction: column;
      flex-shrink: 0;
    }
    .logo { padding: 20px; font-size: 18px; font-weight: bold; border-bottom: 1px solid #ffffff1a; }
    .nav-item { padding: 12px 20px; cursor: pointer; transition: 0.3s; color: #ffffffa6; display: flex; align-items: center; gap: 10px; }
    .nav-item:hover, .nav-item.active { background: var(--primary); color: white; }
    .nav-divider { height: 1px; background: #ffffff1a; margin: 10px 0; }
    .nav-section-title { padding: 8px 20px; font-size: 12px; letter-spacing: 0.2px; color: #ffffff73; text-transform: uppercase; }
    .nav-unavailable { padding: 6px 20px 14px; }
    .nav-unavailable-title { font-size: 12px; color: #ffffff73; margin-bottom: 6px; }
    .nav-unavailable-item { font-size: 12px; color: #ffffff8c; padding: 2px 0; }
    .nav-unavailable-item .reason { color: #ffffff59; }

    /* Main. */
    .main-container { flex: 1; overflow-y: auto; display: flex; flex-direction: column; }
    .top-bar {
      height: 56px;
      background: white;
      border-bottom: 1px solid var(--border);
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 24px;
    }
    .content { padding: 24px; max-width: 1400px; margin: 0 auto; width: 100%; box-sizing: border-box; }

    /* Stat cards. */
    .stat-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 24px; }
    .stat-card { background: white; padding: 16px; border-radius: var(--radius); border: 1px solid var(--border); }
    .stat-label { font-size: 14px; color: var(--text-secondary); }
    .stat-value { font-size: 24px; font-weight: 500; margin-top: 4px; }

    /* Table card. */
    .service-table-card { background: white; border-radius: var(--radius); border: 1px solid var(--border); margin-bottom: 24px; }
    .table-header-ctrl {
      padding: 16px;
      border-bottom: 1px solid var(--border);
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
    }

    table { width: 100%; border-collapse: collapse; }
    th { background: #fafafa; padding: 12px 16px; text-align: left; font-size: 14px; border-bottom: 1px solid var(--border); }
    td { padding: 16px; font-size: 14px; border-bottom: 1px solid var(--border); vertical-align: middle; }
    .group-row { background: #fafafa; font-weight: 600; font-size: 13px; color: var(--text-secondary); }
    .group-row td { padding: 8px 16px !important; }

    .status-dot { width: 6px; height: 6px; border-radius: 50%; display: inline-block; margin-right: 8px; }
    .status-running { background: var(--success); box-shadow: 0 0 5px var(--success); }
    .status-failed { background: var(--error); }

    .btn {
      border: 1px solid #d9d9d9;
      background: white;
      padding: 4px 12px;
      border-radius: 4px;
      cursor: pointer;
      font-size: 14px;
      transition: 0.2s;
    }
    .btn:hover { color: var(--primary); border-color: var(--primary); }
    .btn-primary { background: var(--primary); color: white; border: none; }
    .btn-primary:hover { opacity: 0.8; color: white; }

    .tag { background: #f5f5f5; border: 1px solid #d9d9d9; border-radius: 2px; padding: 0 7px; font-size: 12px; color: var(--text-secondary); }

    .search-input {
      border: 1px solid var(--border);
      padding: 8px 12px;
      border-radius: 4px;
      width: 260px;
      outline: none;
    }

    .namespace-filter { position: relative; min-width: 280px; }
    .namespace-filter-trigger {
      min-width: 280px;
      display: inline-flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      text-align: left;
    }
    .namespace-filter-menu {
      position: absolute;
      top: calc(100% + 8px);
      left: 0;
      width: 320px;
      max-height: 320px;
      overflow: auto;
      background: white;
      border: 1px solid var(--border);
      border-radius: 8px;
      box-shadow: 0 10px 30px rgba(15, 23, 42, 0.12);
      padding: 8px;
      z-index: 1200;
    }
    .namespace-filter-actions {
      display: flex;
      gap: 8px;
      padding-bottom: 8px;
      margin-bottom: 8px;
      border-bottom: 1px solid var(--border);
    }
    .namespace-filter-option {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 6px 4px;
      font-size: 14px;
      color: var(--text-primary);
    }
    .namespace-filter-option input { margin: 0; }
    .namespace-filter-empty {
      padding: 8px 4px;
      color: var(--text-secondary);
      font-size: 13px;
    }

    .run-status-badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 2px 10px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.3px;
    }
    .run-status-running { background: #f0fdf4; color: #15803d; border: 1px solid #bbf7d0; }
    .run-status-done { background: #eff6ff; color: #1d4ed8; border: 1px solid #bfdbfe; }
    .run-status-failed { background: #fef2f2; color: #b91c1c; border: 1px solid #fecaca; }
    .runs-section-title { font-size: 15px; font-weight: 600; }
    .run-summary {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }
    .run-meta-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 8px 16px;
      margin-top: 8px;
    }
    .run-workload-list {
      display: grid;
      gap: 4px;
      margin-top: 8px;
    }

    .fluxon-frame {
      width: 100%;
      height: 72vh;
      border: 0;
      background: white;
    }

    /* Fluxon iframe full-screen mode (within the Ops layout). */
    body.frame-mode .main-container { overflow: hidden; }
    body.frame-mode .content { padding: 0; max-width: none; flex: 1; overflow: hidden; }
    body.frame-mode #fluxon_frame_card { height: 100%; margin: 0; border-radius: 0; display: flex; flex-direction: column; }
    body.frame-mode #fluxon_frame_card > div:last-child { flex: 1; min-height: 0; }
    body.frame-mode .fluxon-frame { height: 100%; }

    /* Deploy YAML modal. */
    .modal-backdrop {
      position: fixed;
      inset: 0;
      background: rgba(0, 0, 0, 0.35);
      display: none;
      z-index: 2000;
    }
    .modal {
      width: min(1000px, 94vw);
      height: min(82vh, 820px);
      margin: 6vh auto;
      background: white;
      border: 1px solid var(--border);
      border-radius: var(--radius);
      overflow: hidden;
      display: flex;
      flex-direction: column;
      box-shadow: 0 8px 32px rgba(0,0,0,0.18);
    }
    .modal-body {
      padding: 16px;
      flex: 1;
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    #deploy_modal_yaml {
      min-height: 260px;
      flex: 1;
      resize: none;
    }
    #deploy_modal_out {
      margin-top: 8px;
      padding: 8px;
      border: 1px solid var(--border);
      border-radius: 4px;
      background: #fafafa;
      max-height: 240px;
      overflow: auto;
    }
  </style>
</head>
<body data-cluster-name="{{ cluster_name }}">
  <div class="sidebar">
    <div class="logo">FLUXON OPS</div>

    <div class="nav-item active" data-nav-id="dashboard" onclick="navToSection('dashboard')">Dashboard</div>
    <div class="nav-item" data-nav-id="workloads" onclick="navToSection('workloads')">Workloads</div>
    <div class="nav-item" data-nav-id="history" onclick="navToSection('history')">Runs</div>
    <div class="nav-item" data-nav-id="advanced" onclick="navToSection('advanced')">Advanced</div>

    <div class="nav-divider"></div>
    <div class="nav-section-title">Fluxon</div>
    <div id="fluxon_nav_items"></div>
    <div id="fluxon_nav_unavailable" class="nav-unavailable" style="display:none;">
      <div class="nav-unavailable-title">Unavailable</div>
      <div id="fluxon_nav_unavailable_list"></div>
    </div>
  </div>

  <div class="main-container">
    <div class="top-bar">
      <div style="display:flex; gap:16px; align-items:center; flex-wrap:wrap;">
        <div style="font-weight: 500">Cluster: {{ cluster_name }}</div>
        <div style="display:flex; gap:8px; align-items:center;">
          <span class="muted">Namespace Filter</span>
          <div id="namespace_filter_root" class="namespace-filter">
            <button id="namespace_filter_button" type="button" class="btn namespace-filter-trigger" onclick="toggleNamespaceFilterMenu()">
              <span id="namespace_filter_label">All namespaces</span>
              <span class="muted">v</span>
            </button>
            <div id="namespace_filter_menu" class="namespace-filter-menu" style="display:none;">
              <div class="namespace-filter-actions">
                <button type="button" class="btn" onclick="selectAllNamespaces()">All</button>
                <button type="button" class="btn" onclick="clearNamespaceSelection()">Clear</button>
              </div>
              <div id="namespace_filter_options"></div>
            </div>
          </div>
        </div>
      </div>
      <div style="display: flex; gap: 16px; align-items: center;">
        <span class="tag">Controller OK</span>
        <button class="btn btn-primary" onclick="toggleDeployYaml()">+ Deploy YAML</button>
      </div>
    </div>

    <div id="deploy_modal_backdrop" class="modal-backdrop" onclick="onDeployModalBackdropClick(event)">
      <div class="modal" role="dialog" aria-modal="true" onclick="event.stopPropagation()">
        <div class="table-header-ctrl" style="border-bottom: 1px solid var(--border);">
          <div>
            <div style="font-weight: 500">Deploy YAML</div>
            <div class="muted">POST <span class="mono">/api/deploy</span> with YAML text (single or multi-doc).</div>
          </div>
          <div style="display:flex; gap:8px; align-items:center;">
            <button class="btn" onclick="closeDeployYamlModal()">Close</button>
          </div>
        </div>
        <div class="modal-body">
          <div class="muted">Node selection: <span class="mono">spec.template.spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution</span></div>
          <textarea id="deploy_modal_yaml" placeholder="paste Deployment/DaemonSet YAML here"></textarea>
          <div style="display:flex; gap:8px; align-items:center;">
            <button id="deploy_modal_submit" class="btn btn-primary" onclick="deployYamlFromModal()">Deploy</button>
            <button class="btn" onclick="clearDeployYamlModal()">Clear</button>
          </div>
          <div id="deploy_modal_out" class="mono">No deploy yet.</div>
        </div>
      </div>
    </div>

    <div class="content" id="dashboard">
      <div class="service-table-card" id="fluxon_frame_card" style="display:none;">
        <div class="table-header-ctrl">
          <div style="font-weight: 500" id="fluxon_frame_title">Fluxon</div>
          <div style="display:flex; gap:8px; align-items:center;">
            <a class="btn" id="fluxon_frame_open_new" target="_blank" rel="noopener">Open</a>
            <button class="btn" onclick="closeFluxonFrame()">Close</button>
          </div>
        </div>
        <div style="padding: 0;">
          <iframe id="fluxon_frame" class="fluxon-frame" title="Fluxon view"></iframe>
        </div>
      </div>

      <div id="ops_native_sections">
      <div id="ops_stats_row" class="stat-row">
        <div class="stat-card">
          <div class="stat-label">Total Workloads</div>
          <div class="stat-value" id="stat_total_workloads">-</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">Running Instances</div>
          <div class="stat-value" id="stat_running_instances" style="color: var(--success)">-</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">Failed Instances</div>
          <div class="stat-value" id="stat_failed_instances" style="color: var(--error)">-</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">Agents OK</div>
          <div class="stat-value" id="stat_agents_ok">-</div>
        </div>
      </div>

      <div class="service-table-card" id="workloads">
        <div class="table-header-ctrl">
          <div style="display: flex; gap: 12px; align-items: center;">
            <input type="text" class="search-input" placeholder="Search by name or node..." disabled>
            <select class="btn" disabled>
              <option>Group by: Node</option>
            </select>
          </div>
          <div style="display: flex; gap: 8px;">
            <button class="btn" onclick="refreshWorkloads()">Refresh</button>
          </div>
        </div>

        <div id="workloads_out"></div>

        <div style="padding: 16px;">
          <div class="muted">workload log (per agent instance)</div>
          <div id="workload_log_header" class="mono">No log selected. Click "Logs" in the table.</div>
          <div style="margin-top:8px;">
            <button class="btn" onclick="startWorkloadLogTail()">Tail</button>
            <button class="btn" onclick="stopWorkloadLogTail()">Stop</button>
            <button class="btn" onclick="clearWorkloadLogView()">Clear</button>
            <button class="btn" onclick="loadOlderWorkloadLog()">Load older</button>
            <label style="margin-left:10px;"><input id="workload_log_follow" type="checkbox" checked /> follow</label>
          </div>
          <pre id="workload_log_out" style="height:360px; overflow:auto;">(empty)</pre>
          <pre id="workloads_ctl_out" style="margin-top:10px;">No action yet.</pre>
        </div>
      </div>

      <div class="service-table-card" id="history">
        <div class="table-header-ctrl">
          <div style="font-weight: 500">Runs</div>
          <div style="display: flex; gap: 8px;">
            <button class="btn" onclick="refreshRuns()">Refresh</button>
            <button class="btn" onclick="clearRunsView()">Clear</button>
          </div>
        </div>
        <div style="padding: 16px;">
          <div style="display:flex; gap:12px; align-items:center; flex-wrap:wrap;">
            <input id="runs_search_input" type="text" class="search-input" placeholder="Search by scene, namespace, run id..." oninput="onRunsSearchChanged(this.value)">
            <select id="runs_group_select" class="btn" onchange="onRunsGroupByChanged(this.value)">
              <option value="status" selected>Group by: Status</option>
              <option value="namespace">Group by: Namespace</option>
              <option value="none">Group by: None</option>
            </select>
            <select id="runs_sort_select" class="btn" onchange="onRunsSortByChanged(this.value)">
              <option value="updated_desc" selected>Sort: Updated desc</option>
              <option value="updated_asc">Sort: Updated asc</option>
              <option value="status">Sort: Status</option>
              <option value="name">Sort: Name</option>
            </select>
          </div>
          <div class="muted" style="margin-top:8px;">One table for running, done, and failed. Namespace filter applies here too.</div>
          <div id="runs_out" class="mono" style="margin-top:8px;">No runs yet.</div>
          <div style="margin-top:16px;">
            <div class="muted">run detail</div>
            <div id="history_detail_out" class="mono">No run selected.</div>
            <pre id="current_ctl_out" style="margin-top:10px;">No action yet.</pre>
          </div>
        </div>
      </div>

      <div class="service-table-card" id="advanced">
        <div class="table-header-ctrl">
          <div style="font-weight: 500">Advanced</div>
          <div class="muted">Low-level control and debug views.</div>
        </div>
        <div style="padding: 16px;">
          <div style="margin-top:8px;">
            <div style="font-weight: 500">Workload Control</div>
            <div class="muted">Query/stop a managed workload on a single node.</div>
            <div style="margin-top:8px;">
              <label>target</label><br />
              <input id="ctl_target" type="text" class="search-input" placeholder="infra44-ThinkStation-PX" />
            </div>
            <div style="margin-top:8px;">
              <label>kind</label><br />
              <select id="ctl_kind" class="btn" style="width:260px;">
                <option value="Deployment">Deployment</option>
                <option value="DaemonSet">DaemonSet</option>
              </select>
            </div>
            <div style="margin-top:8px;">
              <label>name</label><br />
              <input id="ctl_name" type="text" class="search-input" placeholder="fluxon_core" />
            </div>
            <div style="margin-top:8px;">
              <button class="btn" onclick="queryStatus()">Status</button>
              <button class="btn" onclick="deleteGenerationFromControl()" style="color: var(--error)">Delete Generation</button>
            </div>
            <pre id="ctl_out">No status yet.</pre>
          </div>

          <div style="margin-top:16px;">
            <div style="font-weight: 500">Run Actions</div>
            <div class="muted">Use the Runs page for active and finished state. Action output is shown there.</div>
          </div>
        </div>
      </div>
      </div>
    </div>
  </div>

<script>
function escapeHtml(s) {
  return String(s)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function newAnsiSgrState() {
  return { fg: null, bold: false, dim: false };
}

function ansiSgrClassName(st) {
  let cls = '';
  if (st.bold === true) { cls += ' ansi-bold'; }
  if (st.dim === true) { cls += ' ansi-dim'; }
  if (st.fg != null && String(st.fg).length > 0) { cls += ' ansi-fg-' + String(st.fg); }
  return cls.trim();
}

function ansiSgrWrapEscaped(escaped, st) {
  if (escaped.length === 0) { return ''; }
  const cls = ansiSgrClassName(st);
  if (cls.length === 0) { return escaped; }
  return '<span class="' + cls + '">' + escaped + '</span>';
}

function applyAnsiSgrCodes(st, codes) {
  for (const codeRaw of codes) {
    const code = Number(codeRaw);
    if (!Number.isFinite(code)) {
      continue;
    }
    if (code === 0) {
      st.fg = null;
      st.bold = false;
      st.dim = false;
      continue;
    }
    if (code === 1) {
      st.bold = true;
      continue;
    }
    if (code === 2) {
      st.dim = true;
      continue;
    }
    if (code === 22) {
      st.bold = false;
      st.dim = false;
      continue;
    }
    if (code === 39) {
      st.fg = null;
      continue;
    }
    if (code >= 30 && code <= 37) {
      const names = ['black', 'red', 'green', 'yellow', 'blue', 'magenta', 'cyan', 'white'];
      st.fg = names[code - 30];
      continue;
    }
    if (code >= 90 && code <= 97) {
      const names = [
        'bright-black',
        'bright-red',
        'bright-green',
        'bright-yellow',
        'bright-blue',
        'bright-magenta',
        'bright-cyan',
        'bright-white',
      ];
      st.fg = names[code - 90];
      continue;
    }
  }
}

function ansiSgrToHtmlChunkWithState(raw, st) {
  // English note: only supports CSI SGR: ESC [ ... m. All other control sequences are ignored.
  const s = String(raw || '');
  let out = '';
  let i = 0;

  while (i < s.length) {
    const escIdx = s.indexOf('\x1b[', i);
    if (escIdx < 0) {
      out += ansiSgrWrapEscaped(escapeHtml(s.slice(i)), st);
      break;
    }
    if (escIdx > i) {
      out += ansiSgrWrapEscaped(escapeHtml(s.slice(i, escIdx)), st);
    }

    // Parse params until 'm'. Only accept digits and ';' to keep parsing predictable.
    const paramsStart = escIdx + 2;
    let j = paramsStart;
    while (j < s.length && s[j] !== 'm') {
      j += 1;
    }
    if (j < s.length && s[j] === 'm') {
      const params = s.slice(paramsStart, j);
      if (/^[0-9;]*$/.test(params)) {
        // SGR contract: empty params (ESC[m) means "reset" (0).
        const codes = (params.length === 0) ? [0] : params.split(';').map((v) => (v === '' ? 0 : Number(v)));
        applyAnsiSgrCodes(st, codes);
        i = j + 1;
        continue;
      }
    }

    // Unknown/invalid escape sequence: skip ESC to avoid infinite loops.
    i = escIdx + 1;
  }

  return { html: out, state: st };
}

function ansiSgrToHtmlChunkStateless(raw) {
  const st = newAnsiSgrState();
  return ansiSgrToHtmlChunkWithState(raw, st).html;
}

function renderJsonPre(v) {
  return '<pre>' + escapeHtml(JSON.stringify(v, null, 2)) + '</pre>';
}

function scrollToId(id) {
  const el = document.getElementById(String(id || ''));
  if (!el) { return; }
  el.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function setActiveNavId(navId) {
  const want = String(navId || '');
  const items = Array.from(document.querySelectorAll('.nav-item[data-nav-id]'));
  for (const el of items) {
    const id = String(el.getAttribute('data-nav-id') || '');
    if (id === want) el.classList.add('active');
    else el.classList.remove('active');
  }
}

function navToSection(sectionId) {
  exitFluxonFrameMode();
  setActiveNavId(sectionId);
  scrollToId(sectionId);
}

function toggleDeployYaml() {
  openDeployYamlModal();
}

function normalizeOpsBasePath(pathname) {
  const raw = String(pathname || '');
  if (raw === '/' || raw.length === 0) return '';
  const trimmed = raw.endsWith('/') ? raw.slice(0, -1) : raw;
  if (trimmed.endsWith('/ui')) {
    return trimmed.slice(0, -3);
  }
  return trimmed;
}

// English note: ops UI can run behind a reverse proxy under a base path (e.g. /ops/<cluster>).
// The rendered page itself may live at `<base>/ui`, but all API and Fluxon proxy routes stay at `<base>`.
const BASE_PATH = normalizeOpsBasePath(window.location.pathname);

const PAGE_PATH = (BASE_PATH === '') ? '/' : BASE_PATH;

function apiUrl(path) {
  return BASE_PATH + path;
}

function openDeployYamlModal() {
  const backdrop = document.getElementById('deploy_modal_backdrop');
  if (!backdrop) { return; }
  backdrop.style.display = 'block';
  const ta = document.getElementById('deploy_modal_yaml');
  if (ta) { ta.focus(); }
}

function closeDeployYamlModal() {
  const backdrop = document.getElementById('deploy_modal_backdrop');
  if (!backdrop) { return; }
  backdrop.style.display = 'none';
}

function clearDeployYamlModal() {
  const ta = document.getElementById('deploy_modal_yaml');
  const out = document.getElementById('deploy_modal_out');
  if (ta) { ta.value = ''; }
  if (out) { out.textContent = 'No deploy yet.'; }
}

function onDeployModalBackdropClick(ev) {
  // Click outside => close.
  closeDeployYamlModal();
}

function getClusterNameOrNull() {
  const v = String(document.body?.getAttribute('data-cluster-name') || '').trim();
  return v ? v : null;
}

const FLUXON_PROXY_PATHS = Object.freeze({
  kv: '/fluxon/kv',
  mq: '/fluxon/mq',
  topology: '/fluxon/topology',
  fs: '/fluxon/fs/',
});

function fluxonUrlOrNull(kind) {
  const k = String(kind || '');
  if (!getClusterNameOrNull()) return null;
  const suffix = FLUXON_PROXY_PATHS[k];
  if (!suffix) return null;

  // English note:
  // - Browser-facing Fluxon tabs must stay under the ops-scoped path.
  // - Backend routing decides whether the target is a direct CLI render or a registered panel proxy.
  // - This keeps the sidebar topology stable even when the deployment uses internal panel transport.
  return BASE_PATH + suffix;
}

function exitFluxonFrameMode() {
  document.body.classList.remove('frame-mode');
  const card = document.getElementById('fluxon_frame_card');
  const native = document.getElementById('ops_native_sections');
  const frame = document.getElementById('fluxon_frame');
  const openNew = document.getElementById('fluxon_frame_open_new');
  if (frame) frame.src = 'about:blank';
  if (openNew) openNew.href = 'about:blank';
  if (card) card.style.display = 'none';
  if (native) native.style.display = 'block';
}

function closeFluxonFrame() {
  exitFluxonFrameMode();
  setActiveNavId('dashboard');
  scrollToId('dashboard');
}

function openFluxonFrame(kind) {
  const url = fluxonUrlOrNull(kind);
  if (!url) {
    console.warn('fluxon frame: missing url for kind', kind);
    return;
  }
  document.body.classList.add('frame-mode');
  const card = document.getElementById('fluxon_frame_card');
  const native = document.getElementById('ops_native_sections');
  const title = document.getElementById('fluxon_frame_title');
  const frame = document.getElementById('fluxon_frame');
  const openNew = document.getElementById('fluxon_frame_open_new');
  if (!card || !title || !frame || !openNew) {
    console.warn('fluxon frame: missing elements');
    return;
  }
  const label = (kind === 'kv') ? 'Fluxon KV'
    : (kind === 'mq') ? 'Fluxon MQ'
      : (kind === 'topology') ? 'Fluxon Topology'
        : (kind === 'fs') ? 'Fluxon FS'
          : 'Fluxon';
  title.textContent = label;
  openNew.href = url;
  frame.src = url;
  card.style.display = 'flex';
  if (native) native.style.display = 'none';
  setActiveNavId(`fluxon_${String(kind || '')}`);
  scrollToId('fluxon_frame_card');
}

async function probeFluxonTab(kind) {
  const url = fluxonUrlOrNull(kind);
  if (!url) {
    return { ok: false, status: 0, url: null, err: 'missing cluster_name' };
  }
  try {
    const resp = await fetch(url, { method: 'GET' });
    return { ok: resp.ok || resp.status === 401, status: resp.status, url, err: null };
  } catch (e) {
    console.warn('fluxon probe failed', kind, url, e);
    return { ok: false, status: 0, url, err: String(e) };
  }
}

function renderFluxonUnavailable(items) {
  const box = document.getElementById('fluxon_nav_unavailable');
  const list = document.getElementById('fluxon_nav_unavailable_list');
  if (!box || !list) return;
  if (!items || items.length === 0) {
    box.style.display = 'none';
    list.innerHTML = '';
    return;
  }
  let html = '';
  for (const it of items) {
    const label = String(it.label || '');
    const reason = (it.status && it.status !== 0) ? `HTTP ${String(it.status)}` : (it.err ? String(it.err) : 'not reachable');
    html += `<div class="nav-unavailable-item">${escapeHtml(label)} <span class="reason">(${escapeHtml(reason)})</span></div>`;
  }
  list.innerHTML = html;
  box.style.display = 'block';
}

function renderFluxonNavItems(items) {
  const root = document.getElementById('fluxon_nav_items');
  if (!root) return;
  root.innerHTML = '';
  for (const it of items) {
    const div = document.createElement('div');
    div.className = 'nav-item';
    div.setAttribute('data-nav-id', `fluxon_${String(it.kind)}`);
    div.textContent = String(it.label || '');
    div.addEventListener('click', () => openFluxonFrame(it.kind));
    root.appendChild(div);
  }
}

async function initFluxonSidebar() {
  const cluster = getClusterNameOrNull();
  if (!cluster) {
    renderFluxonNavItems([]);
    renderFluxonUnavailable([{ label: 'cluster_name missing', status: 0, err: 'template rendered without cluster_name' }]);
    return;
  }

  const specs = [
    { kind: 'kv', label: 'Fluxon KV' },
    { kind: 'mq', label: 'Fluxon MQ' },
    { kind: 'topology', label: 'Fluxon Topology' },
    { kind: 'fs', label: 'Fluxon FS' },
  ];

  const results = await Promise.all(specs.map(async (s) => {
    const r = await probeFluxonTab(s.kind);
    return { ...s, ...r };
  }));

  const okItems = results.filter((r) => r.ok);
  const badItems = results.filter((r) => !r.ok);

  renderFluxonNavItems(okItems);
  renderFluxonUnavailable(badItems);
}

async function readRespJsonOrText(resp) {
  const ct = (resp.headers.get('content-type') || '').toLowerCase();
  if (ct.includes('application/json')) {
    return { kind: 'json', value: await resp.json() };
  }
  return { kind: 'text', value: await resp.text() };
}

function setHtml(id, html) {
  document.getElementById(id).innerHTML = html;
}

function setText(id, text) {
  document.getElementById(id).textContent = text;
}

function fmtTs(tsMs) {
  try {
    return new Date(tsMs).toLocaleString();
  } catch (e) {
    return String(tsMs);
  }
}

function renderResultsTable(results) {
  if (!results || results.length === 0) {
    return '<div class="muted">No results.</div>';
  }
  let rows = '';
  for (const r of results) {
    const okCls = r.ok ? 'ok' : 'bad';
    const okText = r.ok ? 'OK' : 'FAIL';
    rows += '<tr>'
      + '<td class="mono">' + escapeHtml(r.deployment_name || '') + '</td>'
      + '<td class="mono">' + escapeHtml(r.instance_key || '') + '</td>'
      + '<td>' + escapeHtml(r.pid == null ? '' : String(r.pid)) + '</td>'
      + '<td class="' + okCls + '">' + okText + '</td>'
      + '<td class="mono">' + escapeHtml(r.exit_code == null ? '' : String(r.exit_code)) + '</td>'
      + '<td class="mono">' + escapeHtml(r.err || '') + '</td>'
      + '</tr>';
  }
  return '<table>'
    + '<thead><tr>'
    + '<th>deployment</th><th>instance_key</th><th>pid</th><th>ok</th><th>exit_code</th><th>err</th>'
    + '</tr></thead>'
    + '<tbody>' + rows + '</tbody>'
    + '</table>';
}

function renderHistoryListTable(records) {
  if (!records || records.length === 0) {
    return '<div class="muted">No run records.</div>';
  }
  let rows = '';
  for (const r of records) {
    const okCls = r.ok ? 'ok' : 'bad';
    const okText = r.ok ? 'OK' : 'FAIL';
    const name = r.deployment_name || '';
    const id = r.id || '';
    rows += '<tr>'
      + '<td>' + escapeHtml(fmtTs(r.ts_ms)) + '</td>'
      + '<td class="mono">' + escapeHtml(name) + '</td>'
      + '<td class="' + okCls + '">' + okText + '</td>'
      + '<td class="mono"><a href="' + escapeHtml(PAGE_PATH) + '?run_id=' + encodeURIComponent(id) + '">' + escapeHtml(id) + '</a></td>'
      + '<td class="mono">' + escapeHtml(r.err || '') + '</td>'
      + '</tr>';
  }
  return '<table>'
    + '<thead><tr><th>time</th><th>deployment_name</th><th>ok</th><th>run_id</th><th>err</th></tr></thead>'
    + '<tbody>' + rows + '</tbody>'
    + '</table>';
}

async function deployYamlFromElements(yamlEl, outEl) {
  const out = outEl;
  const yaml = (yamlEl && typeof yamlEl.value === 'string') ? yamlEl.value : '';
  if (!yaml || !yaml.trim()) {
    out.textContent = 'ERROR: please paste a Deployment/DaemonSet YAML.';
    return;
  }
  out.textContent = 'Deploying...';
  const resp = await fetch(apiUrl('/api/deploy'), { method: 'POST', body: yaml });
  const parsed = await readRespJsonOrText(resp);
  if (parsed.kind !== 'json') {
    out.textContent = parsed.value;
    return;
  }

  const v = parsed.value || {};
  const phase = (v && typeof v.phase === 'string') ? String(v.phase) : '';
  if (!phase) {
    out.textContent = 'ERROR: server returned invalid deploy response: missing phase.\n' + JSON.stringify(v, null, 2);
    return;
  }

  let badgeCls = 'muted';
  let badgeText = phase.toUpperCase();
  if (phase === 'converged') { badgeCls = 'ok'; }
  if (phase === 'failed') { badgeCls = 'bad'; }

  let html = '';
  html += '<div><span class="' + badgeCls + '">' + escapeHtml(badgeText) + '</span>'
    + (v.message ? ' - <span class="muted">' + escapeHtml(String(v.message)) + '</span>' : '')
    + (v.err ? ' - <span class="bad">' + escapeHtml(v.err) + '</span>' : '')
    + '</div>';

  if (v.history_id) {
    const hid = String(v.history_id);
    if (phase === 'accepted') {
      html += '<div>run_id: <span class="mono">' + escapeHtml(hid) + '</span> <span class="muted">(history record will be created after converge)</span></div>';
    } else {
      html += '<div>run: <a class="mono" href="' + escapeHtml(PAGE_PATH) + '?run_id=' + encodeURIComponent(hid) + '">' + escapeHtml(hid) + '</a></div>';
    }
  }

  if (v.results) {
    html += '<div style="margin-top:8px;">' + renderResultsTable(v.results) + '</div>';
  }

  out.innerHTML = html;

  await refreshHistoryGrouped();
  await refreshCurrentDeployments();
  await refreshWorkloads();
}

async function deployYamlFromModal() {
  const out = document.getElementById('deploy_modal_out');
  const ta = document.getElementById('deploy_modal_yaml');
  if (!out || !ta) { return; }
  await deployYamlFromElements(ta, out);
}

document.addEventListener('keydown', (ev) => {
  if (ev.key === 'Escape') {
    const backdrop = document.getElementById('deploy_modal_backdrop');
    if (backdrop && backdrop.style.display === 'block') {
      closeDeployYamlModal();
    }
  }
  // Ctrl+Enter => deploy (when modal is open).
  if (ev.key === 'Enter' && (ev.ctrlKey || ev.metaKey)) {
    const backdrop = document.getElementById('deploy_modal_backdrop');
    if (backdrop && backdrop.style.display === 'block') {
      deployYamlFromModal();
    }
  }
});

async function queryStatus() {
  const out = document.getElementById('ctl_out');
  const target = document.getElementById('ctl_target').value.trim();
  const kind = document.getElementById('ctl_kind').value.trim();
  const name = document.getElementById('ctl_name').value.trim();
  if (!target) { out.textContent = 'ERROR: target is required.'; return; }
  if (!kind) { out.textContent = 'ERROR: kind is required.'; return; }
  if (!name) { out.textContent = 'ERROR: name is required.'; return; }
  out.textContent = 'Querying...';
  const url = apiUrl('/api/status?target=' + encodeURIComponent(target) + '&kind=' + encodeURIComponent(kind) + '&name=' + encodeURIComponent(name));
  const resp = await fetch(url, { method: 'GET' });
  const parsed = await readRespJsonOrText(resp);
  out.textContent = parsed.kind === 'text' ? parsed.value : JSON.stringify(parsed.value, null, 2);
}

async function deleteGenerationFromControl() {
  const out = document.getElementById('ctl_out');
  const target = document.getElementById('ctl_target').value.trim();
  const kind = document.getElementById('ctl_kind').value.trim();
  const name = document.getElementById('ctl_name').value.trim();
  if (!target) { out.textContent = 'ERROR: target is required.'; return; }
  if (!kind) { out.textContent = 'ERROR: kind is required.'; return; }
  if (!name) { out.textContent = 'ERROR: name is required.'; return; }
  out.textContent = 'Deleting generation...';
  const url = apiUrl('/api/delete_generation?target=' + encodeURIComponent(target) + '&kind=' + encodeURIComponent(kind) + '&name=' + encodeURIComponent(name));
  const resp = await fetch(url, { method: 'POST' });
  const parsed = await readRespJsonOrText(resp);
  out.textContent = parsed.kind === 'text' ? parsed.value : JSON.stringify(parsed.value, null, 2);
}

	function jsSingleQuoted(s) {
	  const t = String(s).replaceAll('\\', '\\\\').replaceAll("'", "\\'");
	  return "'" + t + "'";
	}

	const DEFAULT_NAMESPACE_LABEL = 'default';

	function normalizeNamespaceLabel(v) {
	  const s = (v == null) ? '' : String(v).trim();
	  return s.length === 0 ? DEFAULT_NAMESPACE_LABEL : s;
	}

	let selectedNamespaces = null;
	let currentDeploymentsCache = null;
	let workloadsCache = null;
	let historyGroupedCache = null;
	let runsSearchText = '';
	let runsGroupBy = 'status';
	let runsSortBy = 'updated_desc';

	function getNamespaceFilterSet() {
	  return selectedNamespaces;
	}

	function namespaceMatchesFilter(v, filterSet) {
	  if (!filterSet) { return true; }
	  return filterSet.has(normalizeNamespaceLabel(v));
	}

	function getFilteredAgentWorkloads(agent, filterSet) {
	  const workloads = (agent && agent.workloads) ? agent.workloads : [];
	  if (!filterSet) {
	    return workloads;
	  }
	  return workloads.filter(w => namespaceMatchesFilter(w.namespace, filterSet));
	}

	function getFilteredCurrentDeploymentGroups(payload) {
	  const groups = (payload && payload.groups) ? payload.groups : [];
	  const filterSet = getNamespaceFilterSet();
	  if (!filterSet) {
	    return groups;
	  }
	  return groups.filter(g => namespaceMatchesFilter(g.namespace, filterSet));
	}

	function getFilteredHistoryGroupedPayload(payload) {
	  if (!payload || payload.ok !== true) {
	    return payload;
	  }
	  const filterSet = getNamespaceFilterSet();
	  if (!filterSet) {
	    return payload;
	  }
	  const groups = [];
	  for (const g of (payload.groups || [])) {
	    const records = (g.records || []).filter(r => namespaceMatchesFilter(r.namespace, filterSet));
	    if (records.length === 0) {
	      continue;
	    }
	    let latestTsMs = 0;
	    for (const r of records) {
	      const tsMs = Number(r.ts_ms || 0);
	      if (tsMs > latestTsMs) {
	        latestTsMs = tsMs;
	      }
	    }
	    groups.push({
	      deployment_group_key: String(g.deployment_group_key || ''),
	      latest_ts_ms: latestTsMs,
	      records: records,
	    });
	  }
	  const ungrouped = (payload.ungrouped || []).filter(r => namespaceMatchesFilter(r.namespace, filterSet));
	  return {
	    ok: true,
	    err: payload.err || null,
	    groups: groups,
	    ungrouped: ungrouped,
	  };
	}

	function clearCurrentDeploymentsView() {
	  currentDeploymentsCache = null;
	  syncNamespaceFilterUiFromCaches();
	  rerenderRunsView();
	  setText('current_ctl_out', 'No action yet.');
	}

	function namespaceSortKey(namespace) {
	  return String(namespace || '');
	}

	function collectNamespaceOptions() {
	  const namespaces = new Set();
	  for (const agent of ((workloadsCache && workloadsCache.agents) ? workloadsCache.agents : [])) {
	    for (const workload of ((agent && agent.workloads) ? agent.workloads : [])) {
	      namespaces.add(normalizeNamespaceLabel(workload.namespace));
	    }
	  }
	  for (const group of ((currentDeploymentsCache && currentDeploymentsCache.groups) ? currentDeploymentsCache.groups : [])) {
	    namespaces.add(normalizeNamespaceLabel(group.namespace));
	  }
	  if (historyGroupedCache && historyGroupedCache.ok === true) {
	    for (const group of (historyGroupedCache.groups || [])) {
	      for (const record of (group.records || [])) {
	        namespaces.add(normalizeNamespaceLabel(record.namespace));
	      }
	    }
	    for (const record of (historyGroupedCache.ungrouped || [])) {
	      namespaces.add(normalizeNamespaceLabel(record.namespace));
	    }
	  }
	  return Array.from(namespaces).sort((a, b) => namespaceSortKey(a).localeCompare(namespaceSortKey(b)));
	}

	function normalizeNamespaceSelection(selection, options) {
	  if (selection == null) {
	    return null;
	  }
	  const optionSet = new Set(options);
	  const next = new Set();
	  for (const namespace of selection) {
	    if (optionSet.has(namespace)) {
	      next.add(namespace);
	    }
	  }
	  if (options.length > 0 && next.size === options.length) {
	    return null;
	  }
	  return next;
	}

	function namespaceFilterLabel(options) {
	  if (!options || options.length === 0) {
	    return 'Namespaces (no data)';
	  }
	  if (selectedNamespaces == null) {
	    return 'All namespaces (' + String(options.length) + ')';
	  }
	  if (selectedNamespaces.size === 0) {
	    return 'No namespaces';
	  }
	  if (selectedNamespaces.size === 1) {
	    for (const namespace of selectedNamespaces) {
	      return namespace;
	    }
	  }
	  return String(selectedNamespaces.size) + ' namespaces';
	}

	function syncNamespaceFilterUiFromCaches() {
	  const buttonLabel = document.getElementById('namespace_filter_label');
	  const optionsRoot = document.getElementById('namespace_filter_options');
	  const options = collectNamespaceOptions();
	  selectedNamespaces = normalizeNamespaceSelection(selectedNamespaces, options);
	  if (buttonLabel) {
	    buttonLabel.textContent = namespaceFilterLabel(options);
	  }
	  if (!optionsRoot) {
	    return;
	  }
	  if (options.length === 0) {
	    optionsRoot.innerHTML = '<div class="namespace-filter-empty">No namespace data loaded yet.</div>';
	    return;
	  }
	  const allSelected = selectedNamespaces == null;
	  let html = '';
	  for (const namespace of options) {
	    const checked = allSelected || selectedNamespaces.has(namespace);
	    html += '<label class="namespace-filter-option">'
	      + '<input type="checkbox" '
	      + (checked ? 'checked ' : '')
	      + 'onchange="onNamespaceFilterOptionChanged(' + jsSingleQuoted(namespace) + ', this.checked)" />'
	      + '<span class="mono">' + escapeHtml(namespace) + '</span>'
	      + '</label>';
	  }
	  optionsRoot.innerHTML = html;
	}

	function rerenderRunsView() {
	  const root = document.getElementById('runs_out');
	  if (!root) {
	    return;
	  }
	  if (!currentDeploymentsCache && !historyGroupedCache) {
	    setText('runs_out', 'No runs yet.');
	    return;
	  }
	  setHtml('runs_out', renderRunsTable());
	}

	function rerenderNamespaceScopedViews() {
	  if (workloadsCache) {
	    updateDashboardStats(workloadsCache);
	    setHtml('workloads_out', renderWorkloadsTable(workloadsCache));
	  }
	  rerenderRunsView();
	}

	function closeNamespaceFilterMenu() {
	  const menu = document.getElementById('namespace_filter_menu');
	  if (menu) {
	    menu.style.display = 'none';
	  }
	}

	function toggleNamespaceFilterMenu() {
	  const menu = document.getElementById('namespace_filter_menu');
	  if (!menu) {
	    return;
	  }
	  syncNamespaceFilterUiFromCaches();
	  menu.style.display = (menu.style.display === 'block') ? 'none' : 'block';
	}

	function applyNamespaceSelectionAndRerender() {
	  syncNamespaceFilterUiFromCaches();
	  rerenderNamespaceScopedViews();
	}

	function onNamespaceFilterOptionChanged(namespace, checked) {
	  const options = collectNamespaceOptions();
	  if (selectedNamespaces == null) {
	    selectedNamespaces = new Set(options);
	  }
	  if (checked) {
	    selectedNamespaces.add(namespace);
	  } else {
	    selectedNamespaces.delete(namespace);
	  }
	  selectedNamespaces = normalizeNamespaceSelection(selectedNamespaces, options);
	  applyNamespaceSelectionAndRerender();
	}

	function selectAllNamespaces() {
	  selectedNamespaces = null;
	  applyNamespaceSelectionAndRerender();
	}

	function clearNamespaceSelection() {
	  selectedNamespaces = new Set();
	  applyNamespaceSelectionAndRerender();
	}

		function workloadKeyLabel(w) {
		  const kind = (w && w.kind) ? String(w.kind) : '';
		  const name = (w && w.name) ? String(w.name) : '';
		  return kind + '/' + name;
		}

		async function loadApplyYamlIntoPre(applyId, preId) {
		  const pre = document.getElementById(preId);
		  pre.textContent = 'Loading YAML...';
		  const resp = await fetch(apiUrl('/api/apply/' + encodeURIComponent(applyId)), { method: 'GET' });
		  const parsed = await readRespJsonOrText(resp);
		  if (parsed.kind !== 'json') {
		    pre.textContent = parsed.value;
		    return null;
		  }
		  const rec = parsed.value || {};
		  pre.textContent = String(rec.deployment_yaml || '');
		  return rec;
		}

		async function reapplyApply(applyId, outElemId) {
		  const out = document.getElementById(outElemId);
		  out.textContent = 'Reapplying...';
		  const resp0 = await fetch(apiUrl('/api/apply/' + encodeURIComponent(applyId)), { method: 'GET' });
		  const parsed0 = await readRespJsonOrText(resp0);
		  if (parsed0.kind !== 'json') {
		    out.textContent = parsed0.value;
		    return;
		  }
		  const rec = parsed0.value || {};
		  const yaml = String(rec.deployment_yaml || '');
		  if (!yaml.trim()) {
		    out.textContent = 'ERROR: apply record contains empty deployment_yaml.';
		    return;
		  }

		  const resp = await fetch(apiUrl('/api/deploy'), { method: 'POST', body: yaml });
		  const parsed = await readRespJsonOrText(resp);
		  if (parsed.kind !== 'json') {
		    out.textContent = parsed.value;
		    return;
		  }

		  const v = parsed.value || {};
		  const phase = (v && typeof v.phase === 'string') ? String(v.phase) : '';
		  if (!phase) {
		    out.textContent = 'ERROR: server returned invalid deploy response: missing phase.\n' + JSON.stringify(v, null, 2);
		    return;
		  }
		  out.textContent = phase.toUpperCase()
		    + (v.message ? (' - ' + String(v.message)) : '')
		    + (v.err ? (' - ' + String(v.err)) : '')
		    + (v.history_id ? ('\nrun_id=' + String(v.history_id)) : '');

		  await refreshCurrentDeployments();
		  await refreshWorkloads();
		  await refreshHistoryGrouped();
		}

		async function loadHistoryYamlIntoPre(historyId, preId) {
		  const pre = document.getElementById(preId);
		  pre.textContent = 'Loading YAML...';
		  const resp = await fetch(apiUrl('/api/history/' + encodeURIComponent(historyId)), { method: 'GET' });
		  const parsed = await readRespJsonOrText(resp);
	  if (parsed.kind !== 'json') {
	    pre.textContent = parsed.value;
	    return null;
	  }
	  const rec = parsed.value || {};
	  pre.textContent = String(rec.deployment_yaml || '');
	  return rec;
	}

	async function reapplyHistory(historyId, outElemId) {
	  const out = document.getElementById(outElemId);
	  out.textContent = 'Reapplying...';
	  const resp0 = await fetch(apiUrl('/api/history/' + encodeURIComponent(historyId)), { method: 'GET' });
	  const parsed0 = await readRespJsonOrText(resp0);
	  if (parsed0.kind !== 'json') {
	    out.textContent = parsed0.value;
	    return;
	  }
	  const rec = parsed0.value || {};
	  const yaml = String(rec.deployment_yaml || '');
	  if (!yaml.trim()) {
	    out.textContent = 'ERROR: history record contains empty deployment_yaml.';
	    return;
	  }

	  const resp = await fetch(apiUrl('/api/deploy'), { method: 'POST', body: yaml });
	  const parsed = await readRespJsonOrText(resp);
	  if (parsed.kind !== 'json') {
	    out.textContent = parsed.value;
	    return;
	  }

		  const v = parsed.value || {};
		  const phase = (v && typeof v.phase === 'string') ? String(v.phase) : '';
		  if (!phase) {
		    out.textContent = 'ERROR: server returned invalid deploy response: missing phase.\n' + JSON.stringify(v, null, 2);
		    return;
		  }
		  out.textContent = phase.toUpperCase()
		    + (v.message ? (' - ' + String(v.message)) : '')
		    + (v.err ? (' - ' + String(v.err)) : '')
		    + (v.history_id ? ('\nrun_id=' + String(v.history_id)) : '');

		  await refreshCurrentDeployments();
		  await refreshWorkloads();
		  await refreshHistoryGrouped();
		}

	function renderRunStatusBadge(status) {
	  const normalized = String(status || '').trim().toLowerCase();
	  if (normalized === 'running') {
	    return '<span class="run-status-badge run-status-running">running</span>';
	  }
	  if (normalized === 'done') {
	    return '<span class="run-status-badge run-status-done">done</span>';
	  }
	  return '<span class="run-status-badge run-status-failed">failed</span>';
	}

	function workloadArrayFromDeploymentGroupKey(rawKey) {
	  let parsed = null;
	  try {
	    parsed = JSON.parse(String(rawKey || '[]'));
	  } catch (_err) {
	    return [];
	  }
	  if (!Array.isArray(parsed)) {
	    return [];
	  }
	  const out = [];
	  for (const item of parsed) {
	    const s = String(item || '');
	    const idx = s.indexOf('/');
	    if (idx <= 0 || idx >= (s.length - 1)) {
	      continue;
	    }
	    out.push({ kind: s.slice(0, idx), name: s.slice(idx + 1) });
	  }
	  return out;
	}

	function logicalRunNameFromWorkloads(workloads) {
	  const names = [];
	  for (const raw of (workloads || [])) {
	    const name = String((raw && raw.name) || '').trim();
	    if (name.length > 0) {
	      names.push(name);
	    }
	  }
	  if (names.length === 0) {
	    return '(unknown)';
	  }
	  const tokenLists = names.map(name => name.split('__').filter(Boolean));
	  if (tokenLists.length === 1) {
	    const only = tokenLists[0];
	    if (only.length > 1) {
	      return only.slice(0, only.length - 1).join('__');
	    }
	    return names[0];
	  }
	  const prefix = [...tokenLists[0]];
	  for (let i = 1; i < tokenLists.length; i++) {
	    while (prefix.length > 0) {
	      let matched = true;
	      for (let j = 0; j < prefix.length; j++) {
	        if (tokenLists[i][j] !== prefix[j]) {
	          matched = false;
	          break;
	        }
	      }
	      if (matched) {
	        break;
	      }
	      prefix.pop();
	    }
	  }
	  if (prefix.length > 0) {
	    return prefix.join('__');
	  }
	  return names[0];
	}

	function buildCurrentRunRows() {
	  if (!currentDeploymentsCache || currentDeploymentsCache.ok !== true) {
	    return [];
	  }
	  const rows = [];
	  for (const group of getFilteredCurrentDeploymentGroups(currentDeploymentsCache)) {
	    const workloads = group.workloads || [];
	    rows.push({
	      source: 'current',
	      status: 'running',
	      namespace: normalizeNamespaceLabel(group.namespace),
	      logicalName: logicalRunNameFromWorkloads(workloads),
	      runId: String(group.apply_id || ''),
	      updatedTsMs: Number(group.updated_ts_ms || 0),
	      workloadCount: workloads.length,
	      workloads: workloads,
	      err: '',
	    });
	  }
	  return rows;
	}

	function buildHistoryRunRows() {
	  if (!historyGroupedCache || historyGroupedCache.ok !== true) {
	    return [];
	  }
	  const rows = [];
	  const payload = getFilteredHistoryGroupedPayload(historyGroupedCache);
	  for (const group of (payload.groups || [])) {
	    const fallbackWorkloads = workloadArrayFromDeploymentGroupKey(group.deployment_group_key);
	    const records = group.records || [];
	    for (let i = 0; i < records.length; i++) {
	      const record = records[i] || {};
	      const workloads = (record.workloads && record.workloads.length > 0) ? record.workloads : fallbackWorkloads;
	      rows.push({
	        source: 'history',
	        status: record.ok ? 'done' : 'failed',
	        namespace: normalizeNamespaceLabel(record.namespace),
	        logicalName: logicalRunNameFromWorkloads(workloads),
	        runId: String(record.id || ''),
	        updatedTsMs: Number(record.ts_ms || 0),
	        workloadCount: workloads.length,
	        workloads: workloads,
	        err: String(record.err || ''),
	        prevId: ((i + 1) < records.length) ? String(records[i + 1].id || '') : '',
	      });
	    }
	  }
	  for (const record of (payload.ungrouped || [])) {
	    const workloads = (record.workloads && record.workloads.length > 0) ? record.workloads : [];
	    rows.push({
	      source: 'history',
	      status: record.ok ? 'done' : 'failed',
	      namespace: normalizeNamespaceLabel(record.namespace),
	      logicalName: logicalRunNameFromWorkloads(workloads),
	      runId: String(record.id || ''),
	      updatedTsMs: Number(record.ts_ms || 0),
	      workloadCount: workloads.length,
	      workloads: workloads,
	      err: String(record.err || ''),
	      prevId: '',
	    });
	  }
	  return rows;
	}

	function statusSortRank(status) {
	  const normalized = String(status || '').trim().toLowerCase();
	  if (normalized === 'running') {
	    return 0;
	  }
	  if (normalized === 'failed') {
	    return 2;
	  }
	  return 1;
	}

	function runRowSearchText(row) {
	  const workloadKeys = (row.workloads || []).map(workloadKeyLabel).join(' ');
	  return [
	    row.status,
	    row.namespace,
	    row.logicalName,
	    row.runId,
	    row.err,
	    workloadKeys,
	  ].join(' ').toLowerCase();
	}

	function sortedRunRows(rows) {
	  const out = [...rows];
	  out.sort((left, right) => {
	    if (runsSortBy === 'updated_asc') {
	      return Number(left.updatedTsMs || 0) - Number(right.updatedTsMs || 0);
	    }
	    if (runsSortBy === 'status') {
	      const byStatus = statusSortRank(left.status) - statusSortRank(right.status);
	      if (byStatus !== 0) {
	        return byStatus;
	      }
	      return Number(right.updatedTsMs || 0) - Number(left.updatedTsMs || 0);
	    }
	    if (runsSortBy === 'name') {
	      const byName = String(left.logicalName || '').localeCompare(String(right.logicalName || ''));
	      if (byName !== 0) {
	        return byName;
	      }
	      return Number(right.updatedTsMs || 0) - Number(left.updatedTsMs || 0);
	    }
	    return Number(right.updatedTsMs || 0) - Number(left.updatedTsMs || 0);
	  });
	  return out;
	}

	function groupedRunRows(rows) {
	  if (runsGroupBy === 'none') {
	    return [{ label: '', rows: rows }];
	  }
	  const buckets = new Map();
	  const order = [];
	  for (const row of rows) {
	    const key = (runsGroupBy === 'namespace') ? row.namespace : row.status;
	    if (!buckets.has(key)) {
	      buckets.set(key, []);
	      order.push(key);
	    }
	    buckets.get(key).push(row);
	  }
	  if (runsGroupBy === 'status') {
	    order.sort((a, b) => statusSortRank(a) - statusSortRank(b));
	  } else {
	    order.sort((a, b) => String(a || '').localeCompare(String(b || '')));
	  }
	  return order.map(key => ({ label: key, rows: buckets.get(key) || [] }));
	}

	function renderRunsSummary(rows) {
	  let running = 0;
	  let done = 0;
	  let failed = 0;
	  for (const row of rows) {
	    if (row.status === 'running') {
	      running += 1;
	    } else if (row.status === 'failed') {
	      failed += 1;
	    } else {
	      done += 1;
	    }
	  }
	  return '<div style="display:flex; gap:8px; flex-wrap:wrap; margin-bottom:8px;">'
	    + '<span class="tag">total ' + escapeHtml(String(rows.length)) + '</span>'
	    + '<span class="tag">running ' + escapeHtml(String(running)) + '</span>'
	    + '<span class="tag">done ' + escapeHtml(String(done)) + '</span>'
	    + '<span class="tag">failed ' + escapeHtml(String(failed)) + '</span>'
	    + '</div>';
	}

	async function showApplyYamlDetail(applyId) {
	  setText('history_detail_out', 'Loading YAML...');
	  const resp = await fetch(apiUrl('/api/apply/' + encodeURIComponent(applyId)), { method: 'GET' });
	  const parsed = await readRespJsonOrText(resp);
	  if (parsed.kind !== 'json') {
	    setText('history_detail_out', parsed.value);
	    return;
	  }
	  const rec = parsed.value || {};
	  let html = '';
	  html += '<div><b>run_id</b>: <span class="mono">' + escapeHtml(String(rec.id || applyId || '')) + '</span></div>';
	  html += '<div style="margin-top:8px;"><b>yaml</b></div>';
	  html += '<pre>' + escapeHtml(String(rec.deployment_yaml || '')) + '</pre>';
	  setHtml('history_detail_out', html);
	}

	function renderRunActions(row) {
	  if (row.source === 'current') {
	    return '<button class="btn" onclick="showApplyYamlDetail(' + jsSingleQuoted(row.runId) + ')">YAML</button> '
	      + '<button class="btn" onclick="reapplyApply(' + jsSingleQuoted(row.runId) + ',' + jsSingleQuoted('current_ctl_out') + ')">Reapply</button> '
	      + '<button class="btn" onclick="deleteCurrentDeploymentGroup(' + jsSingleQuoted(row.runId) + ')">Delete</button>';
	  }
	  const diffBtn = row.prevId
	    ? ('<button class="btn" onclick="showHistoryDiff(' + jsSingleQuoted(row.runId) + ',' + jsSingleQuoted(row.prevId) + ')">Diff</button> ')
	    : '<button class="btn" disabled>Diff</button> ';
	  return '<button class="btn" onclick="loadHistoryById(' + jsSingleQuoted(row.runId) + ')">Detail</button> '
	    + diffBtn
	    + '<button class="btn" onclick="reapplyHistory(' + jsSingleQuoted(row.runId) + ',' + jsSingleQuoted('current_ctl_out') + ')">Reapply</button>';
	}

	function renderRunsTable() {
	  if (currentDeploymentsCache && currentDeploymentsCache.ok !== true) {
	    const err = currentDeploymentsCache.err ? String(currentDeploymentsCache.err) : 'unknown error';
	    return '<div class="bad mono">ERROR: ' + escapeHtml(err) + '</div>';
	  }
	  if (historyGroupedCache && historyGroupedCache.ok !== true) {
	    const err = historyGroupedCache.err ? String(historyGroupedCache.err) : 'unknown error';
	    return '<div class="bad mono">ERROR: ' + escapeHtml(err) + '</div>';
	  }
	  let rows = buildCurrentRunRows().concat(buildHistoryRunRows());
	  const search = String(runsSearchText || '').trim().toLowerCase();
	  if (search.length > 0) {
	    rows = rows.filter(row => runRowSearchText(row).includes(search));
	  }
	  rows = sortedRunRows(rows);
	  if (rows.length === 0) {
	    return '<div class="muted">No runs matched the current filters.</div>';
	  }
	  const groups = groupedRunRows(rows);
	  let body = '';
	  for (const group of groups) {
	    if (group.label) {
	      body += '<tr class="group-row"><td colspan="8">' + escapeHtml(group.label) + ' (' + escapeHtml(String(group.rows.length)) + ')</td></tr>';
	    }
	    for (const row of group.rows) {
	      body += '<tr>'
	        + '<td>' + renderRunStatusBadge(row.status) + '</td>'
	        + '<td class="mono">' + escapeHtml(row.namespace) + '</td>'
	        + '<td class="mono">' + escapeHtml(row.logicalName) + '</td>'
	        + '<td class="mono">' + escapeHtml(row.runId) + '</td>'
	        + '<td>' + escapeHtml(fmtTs(row.updatedTsMs || 0)) + '</td>'
	        + '<td>' + escapeHtml(String(row.workloadCount || 0)) + '</td>'
	        + '<td class="mono">' + escapeHtml(row.err || '') + '</td>'
	        + '<td class="mono">' + renderRunActions(row) + '</td>'
	        + '</tr>';
	    }
	  }
	  return renderRunsSummary(rows)
	    + '<table>'
	    + '<thead><tr><th>Status</th><th>Namespace</th><th>Run</th><th>Run ID</th><th>Updated</th><th>Workloads</th><th>Err</th><th>Action</th></tr></thead>'
	    + '<tbody>' + body + '</tbody>'
	    + '</table>';
	}

	function onRunsSearchChanged(value) {
	  runsSearchText = String(value || '');
	  rerenderRunsView();
	}

	function onRunsGroupByChanged(value) {
	  runsGroupBy = String(value || 'status');
	  rerenderRunsView();
	}

	function onRunsSortByChanged(value) {
	  runsSortBy = String(value || 'updated_desc');
	  rerenderRunsView();
	}

	async function refreshCurrentDeployments() {
	  setText('runs_out', 'Loading...');
	  const resp = await fetch(apiUrl('/api/current_deployments'), { method: 'GET' });
	  const parsed = await readRespJsonOrText(resp);
	  if (parsed.kind !== 'json') {
	    setText('runs_out', parsed.value);
	    currentDeploymentsCache = null;
	    syncNamespaceFilterUiFromCaches();
	    return;
	  }
	  currentDeploymentsCache = parsed.value;
	  syncNamespaceFilterUiFromCaches();
	  rerenderRunsView();
	}

			async function deleteCurrentDeploymentGroup(applyId) {
			  const out = document.getElementById('current_ctl_out');
			  if (!applyId) { out.textContent = 'ERROR: applyId is required.'; return; }
			  if (!currentDeploymentsCache || !currentDeploymentsCache.groups) {
			    out.textContent = 'ERROR: current deployments cache is empty. Click Refresh first.';
			    return;
			  }
		  const g = (currentDeploymentsCache.groups || []).find(x => String(x.apply_id || '') === String(applyId));
		  if (!g) {
		    out.textContent = 'ERROR: applyId not found in current deployments: ' + String(applyId);
		    return;
		  }
		  const groupKey = String(g.deployment_group_key || '');
		  const msg =
		    'WARNING: This deletes the apply record (YAML) and stops all workloads in this deployment group.\n'
		    + 'This is a destructive operation and may change what the controller considers "desired".\n'
		    + '\n'
		    + 'If you want to restore it later, you must re-deploy a YAML.\n'
		    + '\n'
		    + 'deployment_group_key:\n' + groupKey + '\n'
		    + 'apply_id:\n' + String(applyId);
		  if (!confirm(msg)) {
		    out.textContent = 'Canceled.';
		    return;
		  }
	
	  out.textContent = 'Deleting apply...';
	  const deleteResp = await fetch(apiUrl('/api/delete_apply'), {
	    method: 'POST',
	    headers: { 'Content-Type': 'application/json' },
	    body: JSON.stringify({ apply_id: String(applyId) }),
	  });
	  const deleteParsed = await readRespJsonOrText(deleteResp);
	  if (!deleteResp.ok) {
	    out.textContent = 'ERROR: delete_apply failed for apply_id=' + String(applyId) + '\n'
	      + (deleteParsed.kind === 'text' ? deleteParsed.value : JSON.stringify(deleteParsed.value, null, 2));
	    return;
	  }
	  out.textContent = 'Waiting for stop convergence...';
	  const waitResp = await fetch(apiUrl('/api/wait_delete_apply'), {
	    method: 'POST',
	    headers: { 'Content-Type': 'application/json' },
	    body: JSON.stringify({ apply_id: String(applyId) }),
	  });
	  const waitParsed = await readRespJsonOrText(waitResp);
	  if (!waitResp.ok) {
	    out.textContent = 'ERROR: wait_delete_apply failed for apply_id=' + String(applyId) + '\n'
	      + (waitParsed.kind === 'text' ? waitParsed.value : JSON.stringify(waitParsed.value, null, 2))
	      + '\n\nTip: open the Workloads tab and inspect status_hint / zombie diagnostics on the affected agent rows.';
	    return;
	  }
	  out.textContent = waitParsed.kind === 'text' ? waitParsed.value : JSON.stringify(waitParsed.value, null, 2);
		  await refreshCurrentDeployments();
		  await refreshWorkloads();
			  await refreshHistoryGrouped();
			}

		// Workload log viewer
		//
		// Causal chain:
		// - We want "tail -f" like observability from the ops UI without server-side sessions.
		// - A simple poll loop + cursor offsets keeps the API stateless and debuggable.
		// - Chunk size and poll interval are fixed small constants to bound agent I/O and UI memory growth.
		const WORKLOAD_LOG_POLL_INTERVAL_MS = 1000;
		const WORKLOAD_LOG_CHUNK_BYTES = 65536;

		const LOG_DIR_FORWARD = 'Forward';
		const LOG_DIR_BACKWARD = 'Backward';

			let workloadLogTimer = null;
			let workloadLogSelection = { instanceKey: '', kind: '', name: '' };
			let workloadLogStartOffset = 0;
			let workloadLogEndOffset = 0;
			let workloadLogStartCursor = null;
			let workloadLogEndCursor = null;
			let workloadLogLoadingOlder = false;
			let workloadLogAnsiState = newAnsiSgrState();

		function isWorkloadLogFollowEnabled() {
		  const cb = document.getElementById('workload_log_follow');
		  return cb && cb.checked === true;
		}

			function setWorkloadLogHeader() {
			  const h = document.getElementById('workload_log_header');
		  const ik = workloadLogSelection.instanceKey || '';
		  const kind = workloadLogSelection.kind || '';
		  const name = workloadLogSelection.name || '';
		  if (!h) { return; }
		  if (!ik || !kind || !name) {
		    h.textContent = 'No log selected. Click "Logs" in the table.';
		    return;
			  }
			  const shardText = (workloadLogEndCursor && workloadLogEndCursor.shard)
			    ? String(workloadLogEndCursor.shard)
			    : ((workloadLogStartCursor && workloadLogStartCursor.shard) ? String(workloadLogStartCursor.shard) : '-');
			  h.textContent = 'instance_key=' + ik + ' workload=' + kind + '/' + name
			    + ' shard=' + shardText
			    + ' range=[' + String(workloadLogStartOffset) + ',' + String(workloadLogEndOffset) + ')';
			}

		function stopWorkloadLogTail() {
		  if (workloadLogTimer != null) {
		    clearInterval(workloadLogTimer);
		    workloadLogTimer = null;
		  }
		}

			function clearWorkloadLogView() {
			  stopWorkloadLogTail();
			  workloadLogStartOffset = 0;
			  workloadLogEndOffset = 0;
			  workloadLogStartCursor = null;
			  workloadLogEndCursor = null;
			  workloadLogLoadingOlder = false;
			  workloadLogAnsiState = newAnsiSgrState();
		  setWorkloadLogHeader();
		  const pre = document.getElementById('workload_log_out');
		  if (pre) { pre.textContent = '(empty)'; }
		}

		function preIsAtBottom(pre) {
		  if (!pre) { return false; }
		  const slack = 8;
		  return (pre.scrollTop + pre.clientHeight) >= (pre.scrollHeight - slack);
		}

		async function fetchWorkloadLogChunk(direction, cursorOrNull) {
		  const ik = workloadLogSelection.instanceKey || '';
		  const kind = workloadLogSelection.kind || '';
		  const name = workloadLogSelection.name || '';
		  if (!ik || !kind || !name) {
		    return { ok: false, err: 'log selection is empty (instance_key/kind/name)' };
		  }

		  const body = {
		    instance_key: ik,
		    kind: kind,
		    name: name,
		    direction: direction,
		    max_bytes: WORKLOAD_LOG_CHUNK_BYTES,
		  };
		  if (cursorOrNull != null) {
		    body.cursor = cursorOrNull;
		  }

		  const resp = await fetch(apiUrl('/api/workload_log'), {
		    method: 'POST',
		    headers: { 'Content-Type': 'application/json' },
		    body: JSON.stringify(body),
		  });
		  const parsed = await readRespJsonOrText(resp);
		  if (parsed.kind !== 'json') {
		    return { ok: false, err: parsed.value };
		  }
		  return parsed.value || { ok: false, err: 'empty json response' };
		}

		async function startWorkloadLogTail() {
		  stopWorkloadLogTail();
		  const pre = document.getElementById('workload_log_out');
		  if (pre) { pre.textContent = 'Loading tail...'; }
		  workloadLogAnsiState = newAnsiSgrState();

		  const v = await fetchWorkloadLogChunk(LOG_DIR_FORWARD, null);
		  if (!v || v.ok !== true) {
		    const err = v && v.err ? String(v.err) : 'unknown error';
		    if (pre) { pre.textContent = 'ERROR: ' + err; }
		    return;
		  }

			  const txt = (v.text != null) ? String(v.text) : '';
			  workloadLogStartOffset = (v.start_offset != null) ? Number(v.start_offset) : 0;
			  workloadLogEndOffset = (v.end_offset != null) ? Number(v.end_offset) : workloadLogStartOffset;
			  workloadLogStartCursor = (v.start_cursor != null) ? v.start_cursor : null;
			  workloadLogEndCursor = (v.end_cursor != null) ? v.end_cursor : null;
			  workloadLogLoadingOlder = false;
			  setWorkloadLogHeader();

		  if (pre) {
		    const r = ansiSgrToHtmlChunkWithState(txt, workloadLogAnsiState);
		    workloadLogAnsiState = r.state;
		    pre.innerHTML = r.html;
		    if (isWorkloadLogFollowEnabled()) {
		      pre.scrollTop = pre.scrollHeight;
		    }
		  }

		  workloadLogTimer = setInterval(async () => {
		    const pre2 = document.getElementById('workload_log_out');
		    if (!pre2) { return; }
		    const follow = isWorkloadLogFollowEnabled();
		    const atBottom = preIsAtBottom(pre2);
		    if (!follow && !atBottom) {
		      return;
		    }

			    const v2 = await fetchWorkloadLogChunk(LOG_DIR_FORWARD, workloadLogEndCursor);
			    if (!v2 || v2.ok !== true) {
		      // Keep the existing view; update the header so operators see the error.
		      const h = document.getElementById('workload_log_header');
		      if (h) {
		        const err = v2 && v2.err ? String(v2.err) : 'unknown error';
		        h.textContent = 'log tail ERROR: ' + err;
		      }
		      return;
			    }
			    const txt2 = (v2.text != null) ? String(v2.text) : '';
			    const newEnd = (v2.end_offset != null) ? Number(v2.end_offset) : workloadLogEndOffset;
			    const newEndCursor = (v2.end_cursor != null) ? v2.end_cursor : workloadLogEndCursor;
			    const sameShard = workloadLogEndCursor && newEndCursor
			      && workloadLogEndCursor.shard === newEndCursor.shard;
			    if (sameShard && newEnd < workloadLogEndOffset) {
			      const h = document.getElementById('workload_log_header');
			      if (h) {
		        h.textContent = 'log tail ERROR: end_offset moved backwards (file truncated/rotated?)'
		          + ' old_end=' + String(workloadLogEndOffset)
		          + ' new_end=' + String(newEnd);
		      }
		      stopWorkloadLogTail();
		      return;
		    }

			    if (txt2.length > 0) {
			      const r2 = ansiSgrToHtmlChunkWithState(txt2, workloadLogAnsiState);
			      workloadLogAnsiState = r2.state;
			      pre2.insertAdjacentHTML('beforeend', r2.html);
			      workloadLogEndOffset = newEnd;
			      workloadLogEndCursor = newEndCursor;
			      if (v2.start_cursor != null && workloadLogStartCursor == null) {
			        workloadLogStartCursor = v2.start_cursor;
			      }
			      setWorkloadLogHeader();
		      if (follow || atBottom) {
		        pre2.scrollTop = pre2.scrollHeight;
		      }
			    } else {
			      workloadLogEndOffset = newEnd;
			      workloadLogEndCursor = newEndCursor;
			      setWorkloadLogHeader();
			    }
			  }, WORKLOAD_LOG_POLL_INTERVAL_MS);
			}

		async function loadOlderWorkloadLog() {
		  if (workloadLogLoadingOlder) { return; }
		  const pre = document.getElementById('workload_log_out');
		  if (!pre) { return; }
		  if (!workloadLogSelection.instanceKey || !workloadLogSelection.kind || !workloadLogSelection.name) {
		    pre.textContent = 'ERROR: no log selected.';
		    return;
		  }
			  if (workloadLogStartCursor == null) {
			    return;
			  }
			  workloadLogLoadingOlder = true;
			  const beforeHeight = pre.scrollHeight;
			  const v = await fetchWorkloadLogChunk(LOG_DIR_BACKWARD, workloadLogStartCursor);
			  if (!v || v.ok !== true) {
		    const err = v && v.err ? String(v.err) : 'unknown error';
		    pre.insertAdjacentText('afterbegin', 'ERROR: ' + err + '\n\n');
		    workloadLogLoadingOlder = false;
		    return;
			  }
			  const txt = (v.text != null) ? String(v.text) : '';
			  const newStart = (v.start_offset != null) ? Number(v.start_offset) : workloadLogStartOffset;
			  const newStartCursor = (v.start_cursor != null) ? v.start_cursor : workloadLogStartCursor;
			  if (txt.length > 0) {
		    // English note: prepend is stateless; this is best-effort because boundary SGR state
		    // cannot be re-applied to already-rendered newer content.
		    pre.insertAdjacentHTML('afterbegin', ansiSgrToHtmlChunkStateless(txt));
			  }
			  workloadLogStartOffset = newStart;
			  workloadLogStartCursor = newStartCursor;
			  if (v.end_cursor != null) {
			    workloadLogEndCursor = workloadLogEndCursor || v.end_cursor;
			  }
			  setWorkloadLogHeader();

		  const afterHeight = pre.scrollHeight;
		  pre.scrollTop = (afterHeight - beforeHeight) + pre.scrollTop;
		  workloadLogLoadingOlder = false;
		}

			function openWorkloadLog(instanceKey, kind, name) {
			  workloadLogSelection = { instanceKey: String(instanceKey || ''), kind: String(kind || ''), name: String(name || '') };
			  workloadLogStartOffset = 0;
			  workloadLogEndOffset = 0;
			  workloadLogStartCursor = null;
			  workloadLogEndCursor = null;
			  workloadLogLoadingOlder = false;
			  setWorkloadLogHeader();
		  const pre = document.getElementById('workload_log_out');
		  if (pre) { pre.textContent = 'Selected. Click Tail to start.'; }
		  startWorkloadLogTail();
		}

function clearWorkloadsView() {
  setText('workloads_out', 'No workloads yet.');
  setText('workloads_ctl_out', 'No action yet.');
  setText('stat_total_workloads', '-');
  setText('stat_running_instances', '-');
  setText('stat_failed_instances', '-');
  setText('stat_agents_ok', '-');
}

function nodeNameFromAgentInstanceKey(instanceKey) {
  const s = String(instanceKey || '').trim();
  const prefix = 'fluxon_ops_';
  if (!s.startsWith(prefix)) {
    return null;
  }
  const node = s.slice(prefix.length).trim();
  if (node.length === 0) {
    return null;
  }
  return node;
}

function updateDashboardStats(payload) {
  const agents = (payload && payload.agents) ? payload.agents : [];
  const filterSet = getNamespaceFilterSet();
  const sortedAgents = [...agents].sort((a, b) => String(a.instance_key || '').localeCompare(String(b.instance_key || '')));

  const uniqWorkloads = new Set();
  let runningInstances = 0;
  let failedInstances = 0;
  let visibleAgentCount = 0;
  let okVisibleAgentCount = 0;

  for (const a of sortedAgents) {
    const filteredWls = getFilteredAgentWorkloads(a, filterSet);
    if (filterSet && filteredWls.length === 0) {
      continue;
    }
    visibleAgentCount += 1;
    if (!a.ok) {
      continue;
    }
    okVisibleAgentCount += 1;
    for (const w of filteredWls) {
      const namespace = normalizeNamespaceLabel(w.namespace);
      const kind = String(w.kind || '');
      const name = String(w.name || '');
      uniqWorkloads.add(namespace + '\n' + kind + '\n' + name);
      if (w.running === true) {
        runningInstances += 1;
      } else {
        const desired = (w.desired_apply_id != null) ? String(w.desired_apply_id).trim() : '';
        if (desired.length > 0) {
          failedInstances += 1;
        }
      }
    }
  }

  setText('stat_total_workloads', String(uniqWorkloads.size));
  setText('stat_running_instances', String(runningInstances));
  setText('stat_failed_instances', String(failedInstances));
  setText('stat_agents_ok', String(okVisibleAgentCount) + '/' + String(visibleAgentCount));
}

async function deleteWorkloadGenerationOnAgent(instanceKey, kind, name) {
  const out = document.getElementById('workloads_ctl_out');
  if (!out) { return; }
  const target = nodeNameFromAgentInstanceKey(instanceKey);
  if (target == null) {
    out.textContent = 'ERROR: cannot derive node name from agent instance_key. expected prefix=fluxon_ops_. got=' + String(instanceKey || '');
    return;
  }
  out.textContent = 'Deleting generation...';
  const url = apiUrl('/api/delete_generation?target=' + encodeURIComponent(target) + '&kind=' + encodeURIComponent(kind) + '&name=' + encodeURIComponent(name));
  const resp = await fetch(url, { method: 'POST' });
  const parsed = await readRespJsonOrText(resp);
  out.textContent = parsed.kind === 'text' ? parsed.value : JSON.stringify(parsed.value, null, 2);
  await refreshWorkloads();
}

function renderWorkloadsTable(payload) {
  const agents = (payload && payload.agents) ? payload.agents : [];
  if (!agents || agents.length === 0) {
    return '<div class="muted" style="padding:16px;">No ops agents found in cluster membership.</div>';
  }

  const filterSet = getNamespaceFilterSet();
  const sortedAgents = [...agents].sort((a, b) => String(a.instance_key || '').localeCompare(String(b.instance_key || '')));

  let body = '';
  let renderedAgentCount = 0;
  for (const a of sortedAgents) {
    const ik = String(a.instance_key || '');
    const nodeName = nodeNameFromAgentInstanceKey(ik);
    const groupName = (nodeName == null) ? ('(unparseable) ' + ik) : nodeName;
    const ok = !!a.ok;
    const filteredWls = ok ? getFilteredAgentWorkloads(a, filterSet) : [];

    if (filterSet && filteredWls.length === 0) {
      continue;
    }
    renderedAgentCount += 1;

    const svcCount = ok ? filteredWls.length : 0;
    body += '<tr class="group-row"><td colspan="7">' + escapeHtml(groupName) + ' (' + escapeHtml(String(svcCount)) + ' workloads)</td></tr>';

    if (!ok) {
      body += '<tr>'
        + '<td colspan="7" class="mono" style="color: var(--error);">' + escapeHtml(a.err || 'agent error') + '</td>'
        + '</tr>';
      continue;
    }
    if (!filteredWls || filteredWls.length === 0) {
      body += '<tr><td colspan="7" class="muted">(no workloads)</td></tr>';
      continue;
    }

    const sortedWls = [...filteredWls].sort((x, y) => {
      const left = normalizeNamespaceLabel(x.namespace) + '/' + String(x.kind || '') + '/' + String(x.name || '');
      const right = normalizeNamespaceLabel(y.namespace) + '/' + String(y.kind || '') + '/' + String(y.name || '');
      return left.localeCompare(right);
    });
    for (const w of sortedWls) {
      const kind = String(w.kind || '');
      const name = String(w.name || '');
      const namespace = normalizeNamespaceLabel(w.namespace);
      const running = (w.running === true);
      const pid = (w.pid != null) ? String(w.pid) : '-';
      const applyId = (w.apply_id != null) ? String(w.apply_id) : '';
      const desiredApplyId = (w.desired_apply_id != null) ? String(w.desired_apply_id) : '';
      const desiredTs = (w.desired_updated_ts_ms != null) ? Number(w.desired_updated_ts_ms) : null;

      const statusText = running ? 'Running' : 'Stopped';
      const statusDotCls = running ? 'status-running' : 'status-failed';
      const zombiePids = Array.isArray(w.container_orphan_zombie_pids)
        ? w.container_orphan_zombie_pids.map(v => String(v))
        : [];
      const zombieText = zombiePids.length === 0
        ? ''
        : ('zombie=' + String(zombiePids.length) + ' [' + zombiePids.slice(0, 6).join(',') + (zombiePids.length > 6 ? ',...' : '') + ']');
      const statusHint = (w.status_hint != null) ? String(w.status_hint) : '';
      const statusMeta = zombieText ? ('<div style="font-size: 12px; color: var(--warning); margin-top: 2px;">' + escapeHtml(zombieText) + '</div>') : '';
      const statusHintHtml = statusHint
        ? ('<div style="font-size: 12px; color: var(--text-secondary); margin-top: 4px; line-height: 1.35;">' + escapeHtml(statusHint) + '</div>')
        : '';
      let runText = '-';
      if (desiredApplyId && applyId && desiredApplyId !== applyId) {
        runText = 'current_run_id=' + applyId + ' desired_run_id=' + desiredApplyId;
      } else if (desiredApplyId) {
        runText = 'run_id=' + desiredApplyId;
      } else if (applyId) {
        runText = 'run_id=' + applyId;
      }
      const updatedText = (desiredTs != null && Number.isFinite(desiredTs) && desiredTs > 0) ? fmtTs(desiredTs) : '-';

      const logsBtn = '<button class="btn" onclick="openWorkloadLog('
        + jsSingleQuoted(ik) + ',' + jsSingleQuoted(kind) + ',' + jsSingleQuoted(name)
        + ')">Logs</button>';
      const stopBtn = '<button class="btn" style="color: var(--error);" onclick="deleteWorkloadGenerationOnAgent('
        + jsSingleQuoted(ik) + ',' + jsSingleQuoted(kind) + ',' + jsSingleQuoted(name)
        + ')">Delete Generation</button>';

      body += '<tr>'
        + '<td>'
        + '<div style="font-weight: 500">' + escapeHtml(kind + '/' + name) + '</div>'
        + '<div style="font-size: 12px; color: var(--text-secondary)">agent: ' + escapeHtml(ik) + '</div>'
        + '</td>'
        + '<td class="mono">' + escapeHtml(namespace) + '</td>'
        + '<td><span class="status-dot ' + statusDotCls + '"></span>' + escapeHtml(statusText) + statusMeta + statusHintHtml + '</td>'
        + '<td class="mono">' + escapeHtml(pid) + '</td>'
        + '<td class="mono" style="font-size: 12px; color: var(--text-secondary)">' + escapeHtml(runText) + '</td>'
        + '<td>' + escapeHtml(updatedText) + '</td>'
        + '<td>' + logsBtn + ' ' + stopBtn + '</td>'
        + '</tr>';
    }
  }

  if (renderedAgentCount === 0) {
    return '<div class="muted" style="padding:16px;">No workloads matched the namespace filter.</div>';
  }

  return '<table>'
    + '<thead><tr>'
    + '<th width="280">Service Name</th>'
    + '<th width="140">Namespace</th>'
    + '<th>Status</th>'
    + '<th>PID</th>'
    + '<th width="240">Run</th>'
    + '<th>Updated</th>'
    + '<th width="150">Actions</th>'
    + '</tr></thead>'
    + '<tbody>' + body + '</tbody>'
    + '</table>';
}

async function refreshWorkloads() {
  setText('workloads_out', 'Loading...');
  const resp = await fetch(apiUrl('/api/workloads'), { method: 'GET' });
  const parsed = await readRespJsonOrText(resp);
  if (parsed.kind !== 'json') {
    workloadsCache = null;
    syncNamespaceFilterUiFromCaches();
    setText('workloads_out', parsed.value);
    return;
  }
  workloadsCache = parsed.value;
  syncNamespaceFilterUiFromCaches();
  updateDashboardStats(workloadsCache);
  setHtml('workloads_out', renderWorkloadsTable(workloadsCache));
}

async function deleteWorkload(kind, name) {
  const out = document.getElementById('workloads_ctl_out');
  if (!kind) { out.textContent = 'ERROR: kind is required.'; return; }
  if (!name) { out.textContent = 'ERROR: name is required.'; return; }
  const msg =
    'WARNING: This deletes the workload from controller desired-state and sends stop to ALL agents.\n'
    + '\n'
    + 'This may make the system inconsistent with the last applied YAML:\n'
    + '- The apply record (YAML) may still exist in Current Deployments / History.\n'
    + '- Reapply may bring the workload back.\n'
    + '\n'
    + 'After deleting, you must fix consistency yourself (e.g. delete the related apply group, or deploy a new YAML).\n'
    + '\n'
    + 'workload:\n' + String(kind) + '/' + String(name);
  if (!confirm(msg)) { out.textContent = 'Canceled.'; return; }
  out.textContent = 'Deleting...';

  const resp = await fetch(apiUrl('/api/delete_workload'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ kind: kind, name: name }),
  });
  const parsed = await readRespJsonOrText(resp);
  out.textContent = parsed.kind === 'text' ? parsed.value : JSON.stringify(parsed.value, null, 2);
  await refreshWorkloads();
}

		async function deleteWorkloadOnAgent(instanceKey, kind, name) {
	  const out = document.getElementById('workloads_ctl_out');
	  if (!instanceKey) { out.textContent = 'ERROR: instance_key is required.'; return; }
	  if (!kind) { out.textContent = 'ERROR: kind is required.'; return; }
	  if (!name) { out.textContent = 'ERROR: name is required.'; return; }
	  const msg =
	    'WARNING: This is an emergency "Delete@this" operation.\n'
	    + 'It stops the workload on ONE agent, and also removes the workload from controller desired-state.\n'
	    + '\n'
	    + 'This may cause inconsistency:\n'
	    + '- Other agents may still be running the workload.\n'
	    + '- The last applied YAML (apply record) may still exist and does not auto-update.\n'
	    + '- Reapply may bring it back.\n'
	    + '\n'
	    + 'After deleting, you must fix consistency yourself (e.g. delete the related apply group, or deploy a new YAML).\n'
	    + '\n'
	    + 'agent:\n' + String(instanceKey) + '\n'
	    + 'workload:\n' + String(kind) + '/' + String(name);
	  if (!confirm(msg)) { out.textContent = 'Canceled.'; return; }
  out.textContent = 'Deleting on one agent...';

  const resp = await fetch(apiUrl('/api/delete_workload_on_agent'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ instance_key: instanceKey, kind: kind, name: name }),
  });
  const parsed = await readRespJsonOrText(resp);
	  out.textContent = parsed.kind === 'text' ? parsed.value : JSON.stringify(parsed.value, null, 2);
	  await refreshWorkloads();
	}

	function clearHistoryView() {
	  historyGroupedCache = null;
	  syncNamespaceFilterUiFromCaches();
	  rerenderRunsView();
	  setText('history_detail_out', 'No run selected.');
	}

	function clearRunsView() {
	  clearCurrentDeploymentsView();
	  clearHistoryView();
	}

	async function refreshRuns() {
	  await Promise.allSettled([refreshCurrentDeployments(), refreshHistoryGrouped()]);
	}

	function normalizeText(s) {
	  return String(s || '').replaceAll('\r\n', '\n');
	}

	function splitLines(s) {
	  const t = normalizeText(s);
	  // Keep trailing empty line stable (diff should reflect file-ending newline changes).
	  return t.split('\n');
	}

	function myersDiffLines(aLines, bLines) {
	  const a = aLines || [];
	  const b = bLines || [];
	  const n = a.length;
	  const m = b.length;
	  const max = n + m;
	  const offset = max;

	  let v = new Int32Array(2 * max + 1);
	  for (let i = 0; i < v.length; i++) v[i] = -1;
	  v[offset + 1] = 0;

	  const trace = [];

	  for (let d = 0; d <= max; d++) {
	    const v2 = new Int32Array(2 * max + 1);
	    for (let i = 0; i < v2.length; i++) v2[i] = -1;

	    for (let k = -d; k <= d; k += 2) {
	      const idx = offset + k;
	      let x;
	      if (k === -d || (k !== d && v[offset + k - 1] < v[offset + k + 1])) {
	        x = v[offset + k + 1];
	      } else {
	        x = v[offset + k - 1] + 1;
	      }
	      let y = x - k;
	      while (x < n && y < m && a[x] === b[y]) {
	        x++;
	        y++;
	      }
	      v2[idx] = x;
	      if (x >= n && y >= m) {
	        trace.push(v2);
	        return myersBacktrack(trace, a, b, offset);
	      }
	    }

	    trace.push(v2);
	    v = v2;
	  }

	  return [{ kind: 'equal', line: '(diff failed)' }];
	}

	function myersBacktrack(trace, a, b, offset) {
	  let x = a.length;
	  let y = b.length;
	  const edits = [];

	  for (let d = trace.length - 1; d > 0; d--) {
	    const v = trace[d - 1];
	    const k = x - y;
	    let prevK;
	    if (k === -d || (k !== d && v[offset + k - 1] < v[offset + k + 1])) {
	      prevK = k + 1;
	    } else {
	      prevK = k - 1;
	    }
	    const prevX = v[offset + prevK];
	    const prevY = prevX - prevK;

	    while (x > prevX && y > prevY) {
	      edits.push({ kind: 'equal', line: a[x - 1] });
	      x--;
	      y--;
	    }
	    if (x === prevX) {
	      edits.push({ kind: 'insert', line: b[y - 1] });
	      y--;
	    } else {
	      edits.push({ kind: 'delete', line: a[x - 1] });
	      x--;
	    }
	  }

	  while (x > 0 && y > 0) {
	    edits.push({ kind: 'equal', line: a[x - 1] });
	    x--;
	    y--;
	  }
	  while (x > 0) {
	    edits.push({ kind: 'delete', line: a[x - 1] });
	    x--;
	  }
	  while (y > 0) {
	    edits.push({ kind: 'insert', line: b[y - 1] });
	    y--;
	  }

	  edits.reverse();
	  return edits;
	}

	function renderLineDiffHtml(aText, bText) {
	  const aLines = splitLines(aText);
	  const bLines = splitLines(bText);
	  const edits = myersDiffLines(aLines, bLines);
	  let out = '<pre class="diff">';
	  for (const e of edits) {
	    const kind = e.kind;
	    const line = String(e.line || '');
	    if (kind === 'insert') {
	      out += '<span class="diff-add">+ ' + escapeHtml(line) + '</span>\n';
	    } else if (kind === 'delete') {
	      out += '<span class="diff-del">- ' + escapeHtml(line) + '</span>\n';
	    } else {
	      out += '<span class="diff-eq">  ' + escapeHtml(line) + '</span>\n';
	    }
	  }
	  out += '</pre>';
	  return out;
	}

	async function refreshHistoryGrouped() {
	  setText('runs_out', 'Loading...');
	  const resp = await fetch(apiUrl('/api/history_grouped'), { method: 'GET' });
	  const parsed = await readRespJsonOrText(resp);
	  if (parsed.kind !== 'json') {
	    historyGroupedCache = null;
	    syncNamespaceFilterUiFromCaches();
	    setText('runs_out', parsed.value);
	    return;
	  }
	  historyGroupedCache = parsed.value;
	  syncNamespaceFilterUiFromCaches();
	  rerenderRunsView();
	}

	async function showHistoryDiff(newId, oldId) {
	  setText('history_detail_out', 'Loading diff...');
	  const respA = await fetch(apiUrl('/api/history/' + encodeURIComponent(newId)), { method: 'GET' });
	  const parsedA = await readRespJsonOrText(respA);
	  if (parsedA.kind !== 'json') {
	    setText('history_detail_out', parsedA.value);
	    return;
	  }
	  const respB = await fetch(apiUrl('/api/history/' + encodeURIComponent(oldId)), { method: 'GET' });
	  const parsedB = await readRespJsonOrText(respB);
	  if (parsedB.kind !== 'json') {
	    setText('history_detail_out', parsedB.value);
	    return;
	  }

	  const recA = parsedA.value || {};
	  const recB = parsedB.value || {};
	  const aYaml = String(recA.deployment_yaml || '');
	  const bYaml = String(recB.deployment_yaml || '');

	  let html = '';
	  html += '<div><b>diff</b></div>';
	  html += '<div><b>new run</b>: <span class="mono">' + escapeHtml(String(recA.id || newId)) + '</span> <span class="muted">' + escapeHtml(fmtTs(recA.ts_ms || 0)) + '</span></div>';
	  html += '<div><b>old run</b>: <span class="mono">' + escapeHtml(String(recB.id || oldId)) + '</span> <span class="muted">' + escapeHtml(fmtTs(recB.ts_ms || 0)) + '</span></div>';
	  html += '<div style="margin-top:8px;">' + renderLineDiffHtml(bYaml, aYaml) + '</div>';
	  setHtml('history_detail_out', html);
	}

	async function loadHistoryById(id) {
  setText('history_detail_out', 'Loading run ' + id + '...');
  const resp = await fetch(apiUrl('/api/history/' + encodeURIComponent(id)), { method: 'GET' });
  const parsed = await readRespJsonOrText(resp);
  if (parsed.kind !== 'json') {
    setText('history_detail_out', parsed.value);
    return;
  }

  const rec = parsed.value || {};
  const namespace = normalizeNamespaceLabel(rec.namespace);
  let html = '';
  html += '<div><b>run_id</b>: <span class="mono">' + escapeHtml(rec.id || '') + '</span></div>';
  html += '<div><b>namespace</b>: <span class="mono">' + escapeHtml(namespace) + '</span></div>';
  html += '<div><b>time</b>: ' + escapeHtml(fmtTs(rec.ts_ms)) + '</div>';
  html += '<div><b>deployment_name</b>: <span class="mono">' + escapeHtml(rec.deployment_name || '') + '</span></div>';
  if (rec.err) {
    html += '<div><b>err</b>: <span class="bad mono">' + escapeHtml(rec.err) + '</span></div>';
  }
  if (rec.exec_argv) {
    html += '<div><b>exec_argv</b>: <span class="mono">' + escapeHtml(JSON.stringify(rec.exec_argv)) + '</span></div>';
  }
  if (rec.exec_cwd) {
    html += '<div><b>exec_cwd</b>: <span class="mono">' + escapeHtml(rec.exec_cwd) + '</span></div>';
  }
  html += '<div style="margin-top:8px;"><b>results</b></div>';
  html += renderResultsTable(rec.results || []);
  if (rec.deployment_yaml) {
    html += '<details style="margin-top:10px;"><summary>yaml</summary><pre>' + escapeHtml(String(rec.deployment_yaml)) + '</pre></details>';
  }
  html += '<details style="margin-top:10px;"><summary>raw</summary>' + renderJsonPre(rec) + '</details>';

  setHtml('history_detail_out', html);
}

function getQueryParam(name) {
  const params = new URLSearchParams(window.location.search);
  return params.get(name);
}

let opsPageBootstrapped = false;

async function runBootstrapTask(label, task) {
  try {
    await task();
  } catch (e) {
    console.error('ops bootstrap task failed', label, e);
  }
}

async function bootstrapOpsPage() {
  if (opsPageBootstrapped) {
    return;
  }
  opsPageBootstrapped = true;

  const hid = getQueryParam('run_id') || getQueryParam('history_id');
  const pre = document.getElementById('workload_log_out');
  if (pre) {
    pre.addEventListener('scroll', () => {
      if (pre.scrollTop <= 0) {
        loadOlderWorkloadLog();
      }
    });
  }

  const tasks = [
    runBootstrapTask('workloads', refreshWorkloads),
    runBootstrapTask('current_deployments', refreshCurrentDeployments),
    runBootstrapTask('history_grouped', refreshHistoryGrouped),
    runBootstrapTask('fluxon_sidebar', initFluxonSidebar),
  ];
  if (hid && hid.trim()) {
    tasks.push(runBootstrapTask('history_detail', () => loadHistoryById(hid.trim())));
  }
  await Promise.allSettled(tasks);
}

document.addEventListener('click', (ev) => {
  const root = document.getElementById('namespace_filter_root');
  if (!root) {
    return;
  }
  if (!root.contains(ev.target)) {
    closeNamespaceFilterMenu();
  }
});

window.addEventListener('DOMContentLoaded', () => {
  syncNamespaceFilterUiFromCaches();
  void bootstrapOpsPage();
});
window.addEventListener('load', () => {
  void bootstrapOpsPage();
});
if (document.readyState === 'interactive' || document.readyState === 'complete') {
  void bootstrapOpsPage();
}
	</script>
	</body>
	</html>"#,
    ext = "html"
)]
struct OpsIndexTemplate {
    cluster_name: String,
}

fn render_index_html(cluster_name: String) -> String {
    OpsIndexTemplate { cluster_name }.render().unwrap()
}

async fn handle_controller_req_with_init(
    init: Arc<ControllerInitState>,
    req: Request<Body>,
) -> Result<Response<Body>, Infallible> {
    if let Some(resp) = ops_panel_maybe_require_auth(init.cfg.as_ref(), &req) {
        return Ok(resp);
    }

    let maybe_state = {
        let g = init.runtime.lock().await;
        g.as_ref().map(|rt| rt.state.clone())
    };
    if let Some(state) = maybe_state {
        return handle_controller_req(state, req).await;
    }

    let stage = init.stage.read().await.clone();
    let err = init.init_error.lock().await.clone();
    let elapsed_ms = init.started_at.elapsed().as_millis();

    let method = req.method().clone();
    let path = if req.uri().path().is_empty() {
        "/".to_string()
    } else {
        req.uri().path().to_string()
    };
    let msg = match err.as_deref() {
        Some(e) => format!(
            "ops_controller init failed: stage={} elapsed_ms={} err={}",
            stage, elapsed_ms, e
        ),
        None => format!(
            "ops_controller initializing: stage={} elapsed_ms={}",
            stage, elapsed_ms
        ),
    };

    let resp = match (method, path.as_str()) {
        (Method::GET, "/") | (Method::GET, "/ui") | (Method::GET, "/ui/") => response_html(
            StatusCode::OK,
            render_index_html(init.cfg.kv_client.fluxonkv_spec.cluster_name.clone()),
        ),
        (Method::GET, "/healthz") | (Method::GET, "/readyz") => {
            let code = if err.is_some() {
                StatusCode::INTERNAL_SERVER_ERROR
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            response_plain(code, &msg)
        }
        (Method::GET, "/api/version") => response_json(
            StatusCode::OK,
            &serde_json::json!({
                "ok": false,
                "crate_version": env!("CARGO_PKG_VERSION"),
                "stage": stage,
                "elapsed_ms": elapsed_ms,
                "err": err,
            }),
        ),
        (Method::GET, "/api/health") => response_json(
            StatusCode::SERVICE_UNAVAILABLE,
            &serde_json::json!({"ok": false, "stage": stage, "elapsed_ms": elapsed_ms, "err": err}),
        ),
        _ => response_plain(StatusCode::SERVICE_UNAVAILABLE, &msg),
    };

    Ok(resp)
}

async fn handle_controller_req(
    state: Arc<ControllerHttpState>,
    req: Request<Body>,
) -> Result<Response<Body>, Infallible> {
    if let Some(resp) = ops_panel_maybe_require_auth(state.cfg.as_ref(), &req) {
        return Ok(resp);
    }

    let method = req.method().clone();
    let path = if req.uri().path().is_empty() {
        "/".to_string()
    } else {
        req.uri().path().to_string()
    };

    let r = if req.method() == Method::GET && req.uri().path() == "/api/history" {
        handle_history_list(state).await
    } else if req.method() == Method::GET && req.uri().path().starts_with("/api/history/") {
        handle_history_get(state, req.uri().path()).await
    } else if req.method() == Method::GET && req.uri().path().starts_with("/api/apply/") {
        handle_apply_get(state, req.uri().path()).await
    } else {
        match (method, path.as_str()) {
            (Method::GET, "/") | (Method::GET, "/ui") | (Method::GET, "/ui/") => Ok(response_html(
                StatusCode::OK,
                render_index_html(state.cfg.kv_client.fluxonkv_spec.cluster_name.clone()),
            )),
            (Method::GET, "/healthz") | (Method::GET, "/readyz") => {
                Ok(response_plain(StatusCode::OK, "OK"))
            }
            (Method::GET, "/api/health") => Ok(response_json(
                StatusCode::OK,
                &serde_json::json!({"ok": true}),
            )),
            (Method::GET, "/api/version") => Ok(response_json(
                StatusCode::OK,
                &serde_json::json!({
                    "ok": true,
                    "crate_version": env!("CARGO_PKG_VERSION"),
                }),
            )),
            (Method::GET, "/fluxon/kv")
            | (Method::GET, "/fluxon/mq")
            | (Method::GET, "/fluxon/topology")
            | (Method::GET, "/fluxon/fs")
            | (Method::GET, "/fluxon/fs/") => handle_fluxon_embedded_proxy(state, req).await,
            _ if req.uri().path().starts_with("/fluxon/fs/") => {
                handle_fluxon_embedded_proxy(state, req).await
            }
            (Method::POST, "/api/deploy") => handle_deploy(state, req).await,
            (Method::POST, "/api/apply_wait") => handle_apply_wait(state, req).await,
            (Method::GET, "/api/agents") => handle_agents(state, req).await,
            (Method::GET, "/api/agent_desired") => handle_agent_desired(state, req).await,
            (Method::GET, "/api/workloads") => handle_workloads(state, req).await,
            (Method::POST, "/api/workload_log") => handle_workload_log(state, req).await,
            (Method::GET, "/api/current_deployments") => handle_current_deployments(state).await,
            (Method::GET, "/api/history_grouped") => handle_history_grouped(state).await,
            (Method::POST, "/api/delete_apply") => handle_delete_apply(state, req).await,
            (Method::POST, "/api/wait_delete_apply") => handle_wait_delete_apply(state, req).await,
            (Method::POST, "/api/delete_workload") => handle_delete_workload(state, req).await,
            (Method::POST, "/api/delete_workload_on_agent") => {
                handle_delete_workload_on_agent(state, req).await
            }
            (Method::GET, "/api/status") => handle_status(state, req).await,
            (Method::POST, "/api/delete_generation") => handle_delete_generation(state, req).await,
            (Method::POST, "/api/wait_delete_generation") => {
                handle_wait_delete_generation(state, req).await
            }
            _ => Ok(response_plain(StatusCode::NOT_FOUND, "not found")),
        }
    };

    match r {
        Ok(v) => Ok(v),
        Err(e) => Ok(response_plain(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("{}", e),
        )),
    }
}

async fn handle_delete_generation(
    state: Arc<ControllerHttpState>,
    req: Request<Body>,
) -> anyhow::Result<Response<Body>> {
    let query = match parse_target_workload_query(&req) {
        Ok(v) => v,
        Err(e) => return Ok(response_plain(StatusCode::BAD_REQUEST, &e)),
    };

    let req = DeleteGenerationReq {
        kind: query.workload.kind,
        name: query.workload.name.clone(),
        authority: query.workload.authority.clone(),
        require_apply_id: None,
    };

    let agent_instance_key = agent_instance_key_from_node_name(&query.target).unwrap();
    let resp = user_rpc_call_json::<DeleteGenerationReq, DeleteGenerationResp>(
        state.fw.as_ref(),
        &agent_instance_key,
        "fluxon_ops/delete_generation",
        &req,
    )
    .await;

    match resp {
        Ok(v) => Ok(response_json(StatusCode::OK, &v)),
        Err(e) => {
            let out = DeleteGenerationResp {
                ok: false,
                err: Some(e.to_string()),
            };
            Ok(response_json(StatusCode::BAD_GATEWAY, &out))
        }
    }
}

async fn handle_wait_delete_generation(
    state: Arc<ControllerHttpState>,
    req: Request<Body>,
) -> anyhow::Result<Response<Body>> {
    let query = match parse_target_workload_query(&req) {
        Ok(v) => v,
        Err(e) => return Ok(response_plain(StatusCode::BAD_REQUEST, &e)),
    };

    let agent_instance_key = agent_instance_key_from_node_name(&query.target).unwrap();
    let workloads_by_instance = build_single_workload_map(agent_instance_key, query.workload);
    let mut errs_by_instance: BTreeMap<String, Vec<String>> = BTreeMap::new();
    controller_wait_workloads_stopped_by_instance(
        state.as_ref(),
        &workloads_by_instance,
        &mut errs_by_instance,
        STOP_PROCESS_WAIT_SECONDS,
    )
    .await;

    let results = build_agent_results(&workloads_by_instance, errs_by_instance);
    if results.iter().all(|result| result.ok) {
        let resp = WaitDeleteGenerationResp {
            ok: true,
            err: None,
        };
        return Ok(response_json(StatusCode::OK, &resp));
    }

    let mut errs: Vec<String> = results
        .into_iter()
        .filter_map(|result| result.err)
        .collect();
    errs.sort();
    errs.dedup();
    let resp = WaitDeleteGenerationResp {
        ok: false,
        err: Some(errs.join("; ")),
    };
    Ok(response_json(StatusCode::BAD_GATEWAY, &resp))
}

async fn handle_status(
    state: Arc<ControllerHttpState>,
    req: Request<Body>,
) -> anyhow::Result<Response<Body>> {
    let query = match parse_target_workload_query(&req) {
        Ok(v) => v,
        Err(e) => return Ok(response_plain(StatusCode::BAD_REQUEST, &e)),
    };

    let req = StatusReq {
        kind: query.workload.kind,
        name: query.workload.name.clone(),
        authority: query.workload.authority.clone(),
    };

    let (desired_apply_id, desired_updated_ts_ms, desired_deployment_yaml_sha256) = {
        let desired = state.desired.snapshot();
        let mut apply_id: Option<String> = None;
        let mut updated_ts_ms: Option<u64> = None;
        for w in desired.iter() {
            if w.kind != query.workload.kind {
                continue;
            }
            if w.name != query.workload.name {
                continue;
            }
            apply_id = w.apply_id.clone();
            updated_ts_ms = Some(w.updated_ts_ms);
            break;
        }
        let sha256 = match apply_id.as_deref() {
            None => None,
            Some(id) if id.trim().is_empty() => None,
            Some(id) => {
                let rec = state.desired.load_apply_record(id).await?;
                if rec.deployment_yaml_sha256.trim().is_empty() {
                    None
                } else {
                    Some(rec.deployment_yaml_sha256)
                }
            }
        };
        (apply_id, updated_ts_ms, sha256)
    };

    let agent_instance_key = agent_instance_key_from_node_name(&query.target).unwrap();
    let resp = user_rpc_call_json::<StatusReq, StatusResp>(
        state.fw.as_ref(),
        &agent_instance_key,
        "fluxon_ops/status",
        &req,
    )
    .await;

    match resp {
        Ok(v) => {
            let desired_matches_running = match desired_apply_id.as_deref() {
                None => None,
                Some(id) if id.trim().is_empty() => None,
                Some(id) => Some(v.ok && v.running && v.apply_id.as_deref() == Some(id)),
            };
            let desired_matches_present = match desired_apply_id.as_deref() {
                None => None,
                Some(id) if id.trim().is_empty() => None,
                Some(id) => Some(
                    v.ok && v.running
                        && v.present == Some(true)
                        && v.apply_id.as_deref() == Some(id),
                ),
            };
            let out = StatusHttpResp {
                ok: v.ok,
                err: v.err,
                running: v.running,
                present: v.present,
                apply_id: v.apply_id,
                pid: v.pid,
                exit_code: v.exit_code,
                owner_ts_ms: v.owner_ts_ms,
                authority: v.authority,
                container_orphan_zombie_pids: v.container_orphan_zombie_pids,
                status_hint: v.status_hint,
                desired_apply_id,
                desired_updated_ts_ms,
                desired_deployment_yaml_sha256,
                desired_matches_running,
                desired_matches_present,
            };
            Ok(response_json(StatusCode::OK, &out))
        }
        Err(e) => {
            let out = StatusHttpResp {
                ok: false,
                err: Some(e.to_string()),
                running: false,
                present: None,
                apply_id: None,
                pid: None,
                exit_code: None,
                owner_ts_ms: None,
                authority: Some(query.workload.authority),
                container_orphan_zombie_pids: Vec::new(),
                status_hint: None,
                desired_apply_id,
                desired_updated_ts_ms,
                desired_deployment_yaml_sha256,
                desired_matches_running: None,
                desired_matches_present: None,
            };
            Ok(response_json(StatusCode::BAD_GATEWAY, &out))
        }
    }
}

fn controller_list_agent_instance_keys(fw: &Framework) -> Vec<String> {
    let mut keys: Vec<String> = Vec::new();
    for m in fw.cluster_manager_view().cluster_manager().get_members() {
        let id = m.id.to_string();
        if !id.starts_with(OPS_AGENT_INSTANCE_KEY_PREFIX) {
            continue;
        }
        // Controller itself also joins the KV cluster, but it is not an agent.
        if id == "fluxon_ops_controller" {
            continue;
        }
        keys.push(id);
    }
    keys.sort();
    keys.dedup();
    keys
}

fn request_query_values(req: &Request<Body>, wanted_key: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let Some(raw_query) = req.uri().query() else {
        return out;
    };
    for pair in raw_query.split('&') {
        let mut parts = pair.splitn(2, '=');
        let Some(k) = parts.next() else {
            continue;
        };
        if k != wanted_key {
            continue;
        }
        let Some(v) = parts.next() else {
            continue;
        };
        let v = v.trim();
        if v.is_empty() {
            continue;
        }
        out.push(v.to_string());
    }
    out.sort();
    out.dedup();
    out
}

async fn controller_reconcile_loop(state: Arc<ControllerHttpState>) {
    let interval_ms = state.cfg.reconcile.interval_ms;
    let interval = Duration::from_millis(interval_ms);

    loop {
        controller_reconcile_tick(state.clone()).await;
        tokio::time::sleep(interval).await;
    }
}

async fn controller_reconcile_tick(state: Arc<ControllerHttpState>) {
    if let Err(e) = controller_reconcile_apply_delete_lifecycle(state.as_ref()).await {
        eprintln!(
            "[ops_controller:reconcile] apply delete lifecycle reconcile failed: {}",
            e
        );
    }
    let desired = state.desired.snapshot();
    let desired_keys: HashSet<String> = desired
        .iter()
        .map(|workload| workload_key(workload.kind, &workload.name))
        .collect();
    // English note:
    // - delete lifecycle can legitimately remove the last desired apply before the remote stop
    //   has fully converged.
    // - Stale runtime GC must still run in that empty-desired window, otherwise orphaned held
    //   supervisors can survive indefinitely and poison the next generation.
    controller_gc_stale_workloads(state.clone(), &desired_keys).await;
    if desired.is_empty() {
        return;
    }
    // English note:
    // - The controller is the desired-state authority, not the runtime start authority.
    // - Node-local side effects are serialized inside ops_agent -> shared selection supervisor.
    // - Keeping controller-side direct start/stop out of the loop avoids two actors racing on the same
    //   selection set during self-host handover and atomic-group recovery.
    // - The controller still observes status to finalize history and to serve panel APIs.
    controller_finalize_history_records(state, &desired).await;
}

async fn controller_reconcile_apply_delete_lifecycle(
    state: &ControllerHttpState,
) -> anyhow::Result<()> {
    let apply_records = state.desired.load_apply_records().await?;
    for apply_rec in apply_records.into_iter() {
        let phase = apply_lifecycle_phase_normalized(apply_rec.lifecycle_phase);
        if phase == ApplyLifecyclePhase::Running {
            continue;
        }
        // English note:
        // - Delete lifecycle finalization must use the same apply-scoped runtime truth as
        //   wait_delete_apply.
        // - The per-workload status API is weaker here because a transient owner replacement or
        //   status identity drift can report a workload as "detached" before the old apply_id has
        //   actually disappeared from agent runtime.
        // - If reconcile removes desired/applies too early, pull-repair loses the delete intent and
        //   stale held supervisors can survive into the next benchmark/testbed generation.
        let desired_workloads = match controller_desired_workloads_for_apply(
            state.desired.as_ref(),
            &apply_rec.id,
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                eprintln!(
                    "[ops_controller:reconcile] load desired workloads for delete observation failed apply_id={} err={}",
                    apply_rec.id, e
                );
                continue;
            }
        };
        let runtime_workloads_by_instance =
            match controller_collect_runtime_apply_workloads_by_instance(
                state,
                &apply_rec.id,
                desired_workloads.as_slice(),
            )
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    eprintln!(
                        "[ops_controller:reconcile] observe delete runtime failed apply_id={} err={}",
                        apply_rec.id, e
                    );
                    continue;
                }
            };
        if !runtime_workloads_by_instance.is_empty() {
            let pending_detail = format_workloads_by_instance(&runtime_workloads_by_instance);
            let delete_resp = controller_request_delete_apply_workloads(
                state,
                &runtime_workloads_by_instance,
                &apply_rec.id,
            )
            .await;
            let delete_err_detail = format_agent_op_result_failures(&delete_resp.results);
            if delete_err_detail.is_empty() {
                eprintln!(
                    "[ops_controller:reconcile] apply delete runtime still present; reissued delete apply_id={} pending={} result=accepted",
                    apply_rec.id, pending_detail
                );
            } else {
                eprintln!(
                    "[ops_controller:reconcile] apply delete runtime still present; reissued delete apply_id={} pending={} err={}",
                    apply_rec.id, pending_detail, delete_err_detail
                );
            }
            continue;
        }
        if let Err(e) = state.desired.remove_apply(&apply_rec.id).await {
            eprintln!(
                "[ops_controller:reconcile] finalize delete apply failed apply_id={} err={}",
                apply_rec.id, e
            );
        }
    }
    Ok(())
}

fn stale_gc_should_delete_workload(
    desired_keys: &HashSet<String>,
    workload: &WorkloadStatusSummary,
) -> bool {
    // English note:
    // - Controller stale GC owns apply-scoped runtime only.
    // - Bare/bootstrap workloads intentionally run without apply_id and are outside the apply
    //   lifecycle authority, so GC must never retire them just because they are absent from
    //   desired state.
    // - Empty apply_id is treated the same as missing because controller cannot safely prove
    //   ownership for a non-apply runtime entry.
    let Some(apply_id) = workload
        .apply_id
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    else {
        return false;
    };
    let _ = apply_id;
    let key = workload_key(workload.kind, &workload.name);
    !desired_keys.contains(&key)
}

async fn controller_gc_stale_workloads(
    state: Arc<ControllerHttpState>,
    desired_keys: &HashSet<String>,
) {
    let agents = controller_list_agent_instance_keys(state.fw.as_ref());
    if agents.is_empty() {
        return;
    }

    for instance_key in agents.into_iter() {
        let resp = user_rpc_call_json::<ListWorkloadsReq, ListWorkloadsResp>(
            state.fw.as_ref(),
            &instance_key,
            "fluxon_ops/list_workloads",
            &ListWorkloadsReq {},
        )
        .await;

        let list = match resp {
            Ok(v) => {
                if !v.ok {
                    eprintln!(
                        "[ops_controller:reconcile] gc skip due to list_workloads not ok instance_key={} err={}",
                        instance_key,
                        v.err.unwrap_or_else(|| "unknown".to_string())
                    );
                    continue;
                }
                v.workloads.unwrap_or_default()
            }
            Err(e) => {
                eprintln!(
                    "[ops_controller:reconcile] gc skip due to list_workloads rpc error instance_key={} err={}",
                    instance_key, e
                );
                continue;
            }
        };

        for w in list.into_iter() {
            if !stale_gc_should_delete_workload(desired_keys, &w) {
                continue;
            }

            let delete_req = DeleteGenerationReq {
                kind: w.kind,
                name: w.name.clone(),
                authority: w.authority.clone(),
                require_apply_id: w
                    .apply_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .map(str::to_string),
            };
            let st = user_rpc_call_json::<DeleteGenerationReq, DeleteGenerationResp>(
                state.fw.as_ref(),
                &instance_key,
                "fluxon_ops/delete_generation",
                &delete_req,
            )
            .await;

            match st {
                Ok(v) => {
                    if !v.ok {
                        eprintln!(
                            "[ops_controller:reconcile] gc delete_generation failed instance_key={} kind={} name={} err={}",
                            instance_key,
                            w.kind.as_str(),
                            w.name,
                            v.err.unwrap_or_else(|| "unknown".to_string())
                        );
                    }
                }
                Err(e) => {
                    eprintln!(
                        "[ops_controller:reconcile] gc delete_generation rpc failed instance_key={} kind={} name={} err={}",
                        instance_key,
                        w.kind.as_str(),
                        w.name,
                        e
                    );
                }
            }
        }
    }
}

async fn controller_finalize_history_records(
    state: Arc<ControllerHttpState>,
    desired: &[DesiredWorkload],
) {
    let mut by_apply: BTreeMap<String, Vec<DesiredWorkload>> = BTreeMap::new();
    for w in desired.iter() {
        let Some(apply_id) = w.apply_id.as_deref() else {
            continue;
        };
        let apply_id = apply_id.trim();
        if apply_id.is_empty() {
            continue;
        }
        by_apply
            .entry(apply_id.to_string())
            .or_default()
            .push(w.clone());
    }

    for (apply_id, workloads) in by_apply.into_iter() {
        let hist_path = state.history_dir.join(format!("{}.json", apply_id));
        if hist_path.exists() {
            // English note:
            // - History is a derived artifact for UI/debugging, not the source of truth for deployment.
            // - Older versions may have written a history record on a transient reconcile failure
            //   (e.g. start RPC timeout) and then never repaired it even if the apply later converged.
            // - If we skip finalization just because the file exists, the UI can permanently show
            //   "ok=false" while the workload is actually running with the desired apply_id, which
            //   leads to repeated operator confusion ("deployment didn't start").
            // - Therefore: keep an existing history record only if it is already "converged";
            //   otherwise, overwrite it when the apply is confirmed converged.
            let keep_existing = match tokio::fs::read(&hist_path).await {
                Ok(buf) => match serde_json::from_slice::<DeployHistoryRecord>(&buf) {
                    Ok(rec) => deploy_history_record_is_converged(&rec),
                    Err(e) => {
                        eprintln!(
                            "[ops_controller:finalize_history] decode existing history failed; will overwrite on converge: apply_id={} err={}",
                            apply_id, e
                        );
                        false
                    }
                },
                Err(e) => {
                    eprintln!(
                        "[ops_controller:finalize_history] read existing history failed; will overwrite on converge: apply_id={} err={}",
                        apply_id, e
                    );
                    false
                }
            };
            if keep_existing {
                continue;
            }
        }

        let apply_rec = match state.desired.load_apply_record(&apply_id).await {
            Ok(v) => v,
            Err(e) => {
                eprintln!(
                    "[ops_controller:finalize_history] missing apply record; apply_id={} err={}",
                    apply_id, e
                );
                continue;
            }
        };

        let mut results: Vec<PerTargetDeployResult> = Vec::new();
        let mut all_ok = true;

        for w in workloads.iter() {
            let desired_apply_id = match w.apply_id.as_deref() {
                Some(v) if v.trim() == apply_id => v,
                _ => {
                    all_ok = false;
                    break;
                }
            };
            for target in w.targets.iter() {
                let instance_key = match agent_instance_key_from_node_name(target) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!(
                            "[ops_controller:finalize_history] invalid target node name: apply_id={} target={} err={}",
                            apply_id, target, e
                        );
                        all_ok = false;
                        break;
                    }
                };

                let st = user_rpc_call_json::<StatusReq, StatusResp>(
                    state.fw.as_ref(),
                    &instance_key,
                    "fluxon_ops/status",
                    &StatusReq {
                        kind: w.kind,
                        name: w.name.clone(),
                        authority: desired_workload_authority(w).unwrap_or_else(|_| w.name.clone()),
                    },
                )
                .await;

                let (ok, pid) = match st {
                    Ok(v) => (
                        v.ok && v.running
                            && v.present == Some(true)
                            && v.apply_id.as_deref() == Some(desired_apply_id),
                        v.pid,
                    ),
                    Err(_) => (false, None),
                };

                if !ok {
                    all_ok = false;
                    break;
                }

                results.push(PerTargetDeployResult {
                    deployment_name: Some(w.name.clone()),
                    instance_key,
                    pid,
                    ok: true,
                    err: None,
                    exit_code: None,
                });
            }

            if !all_ok {
                break;
            }
        }

        if !all_ok {
            continue;
        }

        let deployment_name = if workloads.len() == 1 {
            Some(workloads[0].name.clone())
        } else {
            Some("<multi>".to_string())
        };

        let rec = DeployHistoryRecord {
            id: apply_id.clone(),
            ts_ms: apply_rec.ts_ms,
            deployment_yaml: apply_rec.deployment_yaml,
            namespace: apply_rec.namespace.clone(),
            deployment_name,
            upload_id: None,
            upload_path: None,
            upload_bytes: None,
            targets: None,
            payload_dest_path: None,
            exec_argv: None,
            exec_cwd: None,
            results: Some(results),
            err: None,
        };

        if let Err(e) = write_history_record(&state.history_dir, &rec).await {
            eprintln!(
                "[ops_controller:finalize_history] write history failed: apply_id={} err={}",
                apply_id, e
            );
        }
    }
}

fn deploy_history_record_is_converged(rec: &DeployHistoryRecord) -> bool {
    if rec.err.is_some() {
        return false;
    }
    let Some(results) = rec.results.as_ref() else {
        return false;
    };
    if results.is_empty() {
        return false;
    }
    results.iter().all(|r| r.ok)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentWorkloadsSummary {
    instance_key: String,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workloads: Option<Vec<WorkloadStatusSummary>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
struct AgentWorkloadsSummaryHttp {
    instance_key: String,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workloads: Option<Vec<WorkloadStatusSummaryHttp>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkloadsListResp {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
    agents: Vec<AgentWorkloadsSummary>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
struct WorkloadsListRespHttp {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
    agents: Vec<AgentWorkloadsSummaryHttp>,
}

async fn collect_agent_ready_summaries(
    fw: &Framework,
    agents: Vec<String>,
) -> Vec<AgentWorkloadsSummaryHttp> {
    // English note:
    // Keep this fanout/join inside the current async path so the framework
    // stays borrowed under one lexical join boundary instead of being widened
    // to `'static` just to satisfy detached spawn.
    let mut out = join_all(agents.into_iter().map(|instance_key| async {
        let ready_resp = user_rpc_call_json::<ReadyReq, ReadyResp>(
            fw,
            &instance_key,
            "fluxon_ops/ready",
            &ReadyReq {},
        )
        .await;

        match ready_resp {
            Ok(v) => AgentWorkloadsSummaryHttp {
                instance_key,
                ok: v.ok,
                err: if v.ok { None } else { v.err },
                workloads: None,
            },
            Err(e) => AgentWorkloadsSummaryHttp {
                instance_key,
                ok: false,
                err: Some(format!("ready rpc failed: {}", e)),
                workloads: None,
            },
        }
    }))
    .await;
    out.sort_by(|a, b| a.instance_key.cmp(&b.instance_key));
    out
}

async fn collect_agent_workload_summaries(
    fw: &Framework,
    agents: Vec<String>,
    desired_by_agent: BTreeMap<String, Vec<DesiredWorkload>>,
    desired_by_key: HashMap<String, (Option<String>, u64, Option<String>)>,
    apply_sha: HashMap<String, Result<String, String>>,
) -> Vec<AgentWorkloadsSummaryHttp> {
    // English note:
    // Per-agent RPCs still run concurrently, but the read-only framework and
    // cached desired/apply indexes remain borrowed from the current scope.
    let mut out = join_all(agents.into_iter().map(|instance_key| async {
        let mut agent_ok = true;
        let mut agent_errs: Vec<String> = Vec::new();
        let mut agent_workloads: Vec<WorkloadStatusSummaryHttp> = Vec::new();
        let desireds = desired_by_agent
            .get(&instance_key)
            .map(Vec::as_slice)
            .unwrap_or(&[]);

        for desired in desireds {
            let key = workload_key(desired.kind, &desired.name);
            let (desired_apply_id, desired_updated_ts_ms, namespace) = desired_by_key
                .get(&key)
                .cloned()
                .map(|(a, ts, ns)| (a, Some(ts), ns))
                .unwrap_or((None, None, None));

            let (desired_deployment_yaml_sha256, desired_apply_err) =
                match desired_apply_id.as_deref() {
                    None => (None, None),
                    Some(id) if id.trim().is_empty() => (None, None),
                    Some(id) => match apply_sha.get(id) {
                        None => (
                            None,
                            Some(format!("missing apply record cache: apply_id={}", id)),
                        ),
                        Some(Ok(sha)) => (Some(sha.clone()), None),
                        Some(Err(e)) => (None, Some(e.clone())),
                    },
                };

            let status_resp = user_rpc_call_json::<StatusReq, StatusResp>(
                fw,
                &instance_key,
                "fluxon_ops/status",
                &StatusReq {
                    kind: desired.kind,
                    name: desired.name.clone(),
                    authority: desired_workload_authority(desired)
                        .unwrap_or_else(|_| desired.name.clone()),
                },
            )
            .await;

            match status_resp {
                Ok(v) => {
                    if !v.ok {
                        agent_ok = false;
                        agent_errs.push(format!(
                            "status not ok kind={} name={} err={}",
                            desired.kind.as_str(),
                            desired.name,
                            v.err.clone().unwrap_or_else(|| "unknown".to_string())
                        ));
                    }
                    let desired_matches_running = match desired_apply_id.as_deref() {
                        None => None,
                        Some(id) if id.trim().is_empty() => None,
                        Some(id) => Some(v.ok && v.running && v.apply_id.as_deref() == Some(id)),
                    };
                    let desired_matches_present = match desired_apply_id.as_deref() {
                        None => None,
                        Some(id) if id.trim().is_empty() => None,
                        Some(id) => Some(
                            v.ok && v.running
                                && v.present == Some(true)
                                && v.apply_id.as_deref() == Some(id),
                        ),
                    };
                    agent_workloads.push(WorkloadStatusSummaryHttp {
                        kind: desired.kind,
                        name: desired.name.clone(),
                        authority: v.authority.clone().unwrap_or_else(|| desired.name.clone()),
                        namespace,
                        running: v.running,
                        present: v.present,
                        apply_id: v.apply_id,
                        pid: v.pid,
                        exit_code: v.exit_code,
                        owner_ts_ms: v.owner_ts_ms,
                        err: v.err,
                        container_orphan_zombie_pids: v.container_orphan_zombie_pids,
                        status_hint: v.status_hint,
                        desired_apply_id,
                        desired_updated_ts_ms,
                        desired_deployment_yaml_sha256,
                        desired_apply_err,
                        desired_matches_running,
                        desired_matches_present,
                    });
                }
                Err(e) => {
                    agent_ok = false;
                    let err_s = e.to_string();
                    agent_errs.push(format!(
                        "status rpc failed kind={} name={} err={}",
                        desired.kind.as_str(),
                        desired.name,
                        err_s
                    ));
                    agent_workloads.push(WorkloadStatusSummaryHttp {
                        kind: desired.kind,
                        name: desired.name.clone(),
                        authority: desired_workload_authority(desired)
                            .unwrap_or_else(|_| desired.name.clone()),
                        namespace,
                        running: false,
                        present: None,
                        apply_id: None,
                        pid: None,
                        exit_code: None,
                        owner_ts_ms: None,
                        err: Some(err_s),
                        container_orphan_zombie_pids: Vec::new(),
                        status_hint: None,
                        desired_apply_id,
                        desired_updated_ts_ms,
                        desired_deployment_yaml_sha256,
                        desired_apply_err,
                        desired_matches_running: Some(false),
                        desired_matches_present: Some(false),
                    });
                }
            }
        }

        agent_workloads.sort_by(|a, b| {
            (a.kind.as_str(), a.name.as_str()).cmp(&(b.kind.as_str(), b.name.as_str()))
        });
        AgentWorkloadsSummaryHttp {
            instance_key,
            ok: agent_ok,
            err: if agent_errs.is_empty() {
                None
            } else {
                Some(agent_errs.join(" | "))
            },
            workloads: Some(agent_workloads),
        }
    }))
    .await;
    out.sort_by(|a, b| a.instance_key.cmp(&b.instance_key));
    out
}

async fn handle_workloads(
    state: Arc<ControllerHttpState>,
    req: Request<Body>,
) -> anyhow::Result<Response<Body>> {
    let requested: HashSet<String> = request_query_values(&req, "instance_key")
        .into_iter()
        .collect();
    let mut agents = controller_list_agent_instance_keys(state.fw.as_ref());
    if !requested.is_empty() {
        agents.retain(|instance_key| requested.contains(instance_key));
    }

    let desired_snapshot = state.desired.snapshot();
    let mut desired_by_key: HashMap<String, (Option<String>, u64, Option<String>)> = HashMap::new();
    let mut desired_by_agent: BTreeMap<String, Vec<DesiredWorkload>> = BTreeMap::new();
    let mut desired_apply_ids: BTreeSet<String> = BTreeSet::new();
    for w in desired_snapshot.iter() {
        let key = workload_key(w.kind, &w.name);
        desired_by_key.insert(
            key,
            (w.apply_id.clone(), w.updated_ts_ms, w.namespace.clone()),
        );
        for target in w.targets.iter() {
            let instance_key = agent_instance_key_from_node_name(target)?;
            desired_by_agent
                .entry(instance_key)
                .or_default()
                .push(w.clone());
        }
        if let Some(apply_id) = w.apply_id.as_deref() {
            let id = apply_id.trim();
            if !id.is_empty() {
                desired_apply_ids.insert(id.to_string());
            }
        }
    }

    let mut apply_sha: HashMap<String, Result<String, String>> = HashMap::new();
    for apply_id in desired_apply_ids.into_iter() {
        let rec = state.desired.load_apply_record(&apply_id).await;
        match rec {
            Ok(v) => {
                let sha = v.deployment_yaml_sha256.trim().to_string();
                if sha.is_empty() {
                    apply_sha.insert(apply_id, Err("deployment_yaml_sha256 is empty".to_string()));
                } else {
                    apply_sha.insert(apply_id, Ok(sha));
                }
            }
            Err(e) => {
                apply_sha.insert(apply_id, Err(e.to_string()));
            }
        }
    }

    let out = collect_agent_workload_summaries(
        state.fw.as_ref(),
        agents,
        desired_by_agent,
        desired_by_key,
        apply_sha,
    )
    .await;

    let all_ok = out.iter().all(|a| a.ok);
    let resp = WorkloadsListRespHttp {
        ok: all_ok,
        err: if all_ok {
            None
        } else {
            Some("one or more agents failed".to_string())
        },
        agents: out,
    };
    let code = if resp.ok {
        StatusCode::OK
    } else {
        StatusCode::BAD_GATEWAY
    };
    Ok(response_json(code, &resp))
}

async fn handle_agents(
    state: Arc<ControllerHttpState>,
    req: Request<Body>,
) -> anyhow::Result<Response<Body>> {
    let requested: HashSet<String> = request_query_values(&req, "instance_key")
        .into_iter()
        .collect();
    let mut agents = controller_list_agent_instance_keys(state.fw.as_ref());
    if !requested.is_empty() {
        agents.retain(|instance_key| requested.contains(instance_key));
    }

    let out = collect_agent_ready_summaries(state.fw.as_ref(), agents).await;

    let all_ok = out.iter().all(|a| a.ok);
    let resp = WorkloadsListRespHttp {
        ok: all_ok,
        err: if all_ok {
            None
        } else {
            Some("one or more agents failed".to_string())
        },
        agents: out,
    };
    let code = if resp.ok {
        StatusCode::OK
    } else {
        StatusCode::BAD_GATEWAY
    };
    Ok(response_json(code, &resp))
}

async fn handle_agent_desired(
    state: Arc<ControllerHttpState>,
    req: Request<Body>,
) -> anyhow::Result<Response<Body>> {
    let Some(instance_key) = query_param(req.uri(), "instance_key") else {
        return Ok(response_plain(
            StatusCode::BAD_REQUEST,
            "missing query param: instance_key",
        ));
    };
    let instance_key = instance_key.trim().to_string();
    if instance_key.is_empty() {
        return Ok(response_plain(
            StatusCode::BAD_REQUEST,
            "instance_key must be non-empty",
        ));
    }

    let resp = controller_agent_desired_snapshot(state.desired.as_ref(), &instance_key).await;
    Ok(response_json(StatusCode::OK, &resp))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkloadLogHttpReq {
    instance_key: String,
    kind: WorkloadKind,
    name: String,
    direction: LogReadDirection,
    cursor: Option<WorkloadLogCursor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_bytes: Option<u64>,
}

async fn handle_workload_log(
    state: Arc<ControllerHttpState>,
    req: Request<Body>,
) -> anyhow::Result<Response<Body>> {
    let body_bytes =
        read_body_to_bytes_limited(req.into_body(), state.cfg.panel.max_body_bytes).await?;
    if body_bytes.is_empty() {
        let resp = ReadWorkloadLogResp {
            ok: false,
            err: Some("empty workload_log body is not allowed".to_string()),
            file_size: None,
            start_offset: None,
            end_offset: None,
            start_cursor: None,
            end_cursor: None,
            text: None,
        };
        return Ok(response_json(StatusCode::BAD_REQUEST, &resp));
    }

    let req: WorkloadLogHttpReq = serde_json::from_slice(&body_bytes)
        .map_err(|e| anyhow::anyhow!("workload_log body must be json: {}", e))?;

    let instance_key = req.instance_key.trim().to_string();
    if instance_key.is_empty() {
        let resp = ReadWorkloadLogResp {
            ok: false,
            err: Some("instance_key must be non-empty".to_string()),
            file_size: None,
            start_offset: None,
            end_offset: None,
            start_cursor: None,
            end_cursor: None,
            text: None,
        };
        return Ok(response_json(StatusCode::BAD_REQUEST, &resp));
    }

    let agent_req = ReadWorkloadLogReq {
        kind: req.kind,
        name: req.name,
        direction: req.direction,
        cursor: req.cursor,
        max_bytes: req.max_bytes,
    };

    let resp = user_rpc_call_json::<ReadWorkloadLogReq, ReadWorkloadLogResp>(
        state.fw.as_ref(),
        &instance_key,
        "fluxon_ops/read_workload_log",
        &agent_req,
    )
    .await;

    match resp {
        Ok(v) => {
            let code = if v.ok {
                StatusCode::OK
            } else {
                StatusCode::BAD_REQUEST
            };
            Ok(response_json(code, &v))
        }
        Err(e) => {
            let mut err_s = e.to_string();
            if err_s.contains("NotImplemented") {
                err_s = format!(
                    "agent does not implement fluxon_ops/read_workload_log; ops_agent is likely older than ops_controller. Redeploy fluxon_release to this node and restart ops_agent. original_err={}",
                    err_s
                );
            }
            let resp = ReadWorkloadLogResp {
                ok: false,
                err: Some(err_s),
                file_size: None,
                start_offset: None,
                end_offset: None,
                start_cursor: None,
                end_cursor: None,
                text: None,
            };
            Ok(response_json(StatusCode::BAD_GATEWAY, &resp))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteWorkloadReq {
    kind: WorkloadKind,
    name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteApplyReq {
    apply_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteApplyResp {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
    results: Vec<AgentOpResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplyWaitReq {
    apply_id: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
struct ApplyWaitResp {
    ok: bool,
    apply_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    phase: Option<ApplyLifecyclePhase>,
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime_goal: Option<ApplyRuntimeGoal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    matching_target_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pending_detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteWorkloadOnAgentReq {
    instance_key: String,
    kind: WorkloadKind,
    name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentOpResult {
    instance_key: String,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteWorkloadResp {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
    results: Vec<AgentOpResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteWorkloadOnAgentResp {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
    result: AgentOpResult,
}

fn add_workload_for_instance(
    workloads_by_instance: &mut BTreeMap<String, Vec<WorkloadId>>,
    instance_key: String,
    workload: WorkloadId,
) {
    workloads_by_instance
        .entry(instance_key)
        .or_default()
        .push(workload);
}

fn normalize_workloads_by_instance(workloads_by_instance: &mut BTreeMap<String, Vec<WorkloadId>>) {
    for workloads in workloads_by_instance.values_mut() {
        workloads.sort();
        workloads.dedup();
    }
}

fn dedup_workloads_by_instance_preserve_order(
    workloads_by_instance: &mut BTreeMap<String, Vec<WorkloadId>>,
) {
    for workloads in workloads_by_instance.values_mut() {
        let mut seen: BTreeSet<WorkloadId> = BTreeSet::new();
        workloads.retain(|workload| seen.insert(workload.clone()));
    }
}

fn desired_workload_delete_cmp(
    left: &DesiredWorkload,
    right: &DesiredWorkload,
) -> std::cmp::Ordering {
    match (&left.atomic_group, &right.atomic_group) {
        (Some(left_group), Some(right_group)) => left_group
            .phase
            .cmp(&right_group.phase)
            .then(left_group.order.cmp(&right_group.order))
            .then(left.kind.cmp(&right.kind))
            .then(left.name.cmp(&right.name)),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => left.kind.cmp(&right.kind).then(left.name.cmp(&right.name)),
    }
}

fn append_agent_error(
    errs_by_instance: &mut BTreeMap<String, Vec<String>>,
    instance_key: &str,
    err: String,
) {
    errs_by_instance
        .entry(instance_key.to_string())
        .or_default()
        .push(err);
}

fn build_agent_results(
    workloads_by_instance: &BTreeMap<String, Vec<WorkloadId>>,
    errs_by_instance: BTreeMap<String, Vec<String>>,
) -> Vec<AgentOpResult> {
    let mut results: Vec<AgentOpResult> = Vec::new();
    for instance_key in workloads_by_instance.keys() {
        let err = errs_by_instance
            .get(instance_key)
            .map(|errs| errs.join("; "))
            .filter(|joined| !joined.is_empty());
        results.push(AgentOpResult {
            instance_key: instance_key.clone(),
            ok: err.is_none(),
            err,
        });
    }
    results
}

fn format_agent_op_result_failures(results: &[AgentOpResult]) -> String {
    let mut parts: Vec<String> = Vec::new();
    for result in results.iter() {
        if result.ok {
            continue;
        }
        parts.push(format!(
            "{}:{}",
            result.instance_key,
            result.err.as_deref().unwrap_or("unknown")
        ));
    }
    parts.join("; ")
}

fn build_single_workload_map(
    instance_key: String,
    workload: WorkloadId,
) -> BTreeMap<String, Vec<WorkloadId>> {
    let mut workloads_by_instance: BTreeMap<String, Vec<WorkloadId>> = BTreeMap::new();
    add_workload_for_instance(&mut workloads_by_instance, instance_key, workload);
    normalize_workloads_by_instance(&mut workloads_by_instance);
    workloads_by_instance
}

fn parse_target_workload_query(req: &Request<Body>) -> Result<TargetWorkloadQuery, String> {
    let Some(target) = query_param(req.uri(), "target") else {
        return Err("missing query param: target".to_string());
    };
    let Some(kind) = query_param(req.uri(), "kind") else {
        return Err("missing query param: kind".to_string());
    };
    let Some(name) = query_param(req.uri(), "name") else {
        return Err("missing query param: name".to_string());
    };
    let Some(authority) = query_param(req.uri(), "authority") else {
        return Err("missing query param: authority".to_string());
    };

    let target = target.trim();
    let kind = kind.trim();
    let name = name.trim();
    let authority = authority.trim();
    if target.is_empty() {
        return Err("target must be non-empty".to_string());
    }
    let kind = match kind {
        "Deployment" => WorkloadKind::Deployment,
        "DaemonSet" => WorkloadKind::DaemonSet,
        _ => return Err("kind must be 'Deployment' or 'DaemonSet'".to_string()),
    };
    if name.is_empty() {
        return Err("name must be non-empty".to_string());
    }
    if authority.is_empty() {
        return Err("authority must be non-empty".to_string());
    }
    Ok(TargetWorkloadQuery {
        target: target.to_string(),
        workload: WorkloadId::new(kind, name.to_string(), authority.to_string()),
    })
}

async fn controller_delete_generations_by_instance(
    state: &ControllerHttpState,
    workloads_by_instance: &BTreeMap<String, Vec<WorkloadId>>,
    delete_mode: ControllerDeleteGenerationMode,
    require_apply_id: Option<&str>,
) -> BTreeMap<String, Vec<String>> {
    let mut errs_by_instance: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let require_apply_id = require_apply_id.map(|v| v.trim().to_string());
    let local_agent_instance_key =
        controller_local_agent_instance_key(&state.cfg.kv_client.instance_key);
    let mut ordered_instances: Vec<(&String, &Vec<WorkloadId>)> =
        workloads_by_instance.iter().collect();
    ordered_instances.sort_by(|(left_key, _), (right_key, _)| {
        let left_is_local = local_agent_instance_key.as_deref() == Some(left_key.as_str());
        let right_is_local = local_agent_instance_key.as_deref() == Some(right_key.as_str());
        left_is_local
            .cmp(&right_is_local)
            .then(left_key.cmp(right_key))
    });
    let delete_futures = ordered_instances
        .into_iter()
        .flat_map(|(instance_key, workloads)| {
            let require_apply_id = require_apply_id.clone();
            workloads.iter().cloned().map(move |workload| {
                let state = state;
                let instance_key = instance_key.clone();
                let require_apply_id = require_apply_id.clone();
                async move {
                    let delete_action = "delete_generation";
                    let resp = match delete_mode {
                        ControllerDeleteGenerationMode::Immediate => {
                            let delete_req = DeleteGenerationReq {
                                kind: workload.kind,
                                name: workload.name.clone(),
                                authority: workload.authority.clone(),
                                require_apply_id,
                            };
                            user_rpc_call_json::<DeleteGenerationReq, DeleteGenerationResp>(
                                state.fw.as_ref(),
                                &instance_key,
                                "fluxon_ops/delete_generation",
                                &delete_req,
                            )
                            .await
                        }
                    };
                    (instance_key, workload, delete_action, resp)
                }
            })
        })
        .collect::<Vec<_>>();
    for (instance_key, workload, delete_action, resp) in join_all(delete_futures).await {
        match resp {
            Ok(v) => {
                if !v.ok {
                    append_agent_error(
                        &mut errs_by_instance,
                        &instance_key,
                        format!(
                            "{} failed kind={} name={} err={}",
                            delete_action,
                            workload.kind.as_str(),
                            workload.name,
                            v.err.unwrap_or_else(|| "unknown".to_string())
                        ),
                    );
                }
            }
            Err(e) => {
                append_agent_error(
                    &mut errs_by_instance,
                    &instance_key,
                    format!(
                        "{} rpc failed kind={} name={} err={}",
                        delete_action,
                        workload.kind.as_str(),
                        workload.name,
                        e
                    ),
                );
            }
        }
    }
    errs_by_instance
}

async fn controller_wait_workloads_stopped_by_instance(
    state: &ControllerHttpState,
    workloads_by_instance: &BTreeMap<String, Vec<WorkloadId>>,
    errs_by_instance: &mut BTreeMap<String, Vec<String>>,
    timeout_s: u64,
) {
    let mut pending: BTreeMap<String, Vec<WorkloadId>> = workloads_by_instance
        .iter()
        .filter(|(instance_key, _)| !errs_by_instance.contains_key(*instance_key))
        .map(|(instance_key, workloads)| (instance_key.clone(), workloads.clone()))
        .collect();
    if pending.is_empty() {
        return;
    }

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_s);
    loop {
        let mut next_pending: BTreeMap<String, Vec<WorkloadId>> = BTreeMap::new();
        let mut pending_errs: BTreeMap<String, Vec<String>> = BTreeMap::new();

        for (instance_key, workloads) in pending.iter() {
            let mut instance_pending = false;
            for workload in workloads.iter() {
                let st = user_rpc_call_json::<StatusReq, StatusResp>(
                    state.fw.as_ref(),
                    instance_key,
                    "fluxon_ops/status",
                    &StatusReq {
                        kind: workload.kind,
                        name: workload.name.clone(),
                        authority: workload.authority.clone(),
                    },
                )
                .await;

                match st {
                    Ok(v) => {
                        if !v.ok {
                            instance_pending = true;
                            append_agent_error(
                                &mut pending_errs,
                                instance_key,
                                format!(
                                    "status not ok kind={} name={} err={}",
                                    workload.kind.as_str(),
                                    workload.name,
                                    v.err.unwrap_or_else(|| "unknown".to_string())
                                ),
                            );
                            continue;
                        }
                        if v.running {
                            instance_pending = true;
                            append_agent_error(
                                &mut pending_errs,
                                instance_key,
                                format!(
                                    "workload still running kind={} name={}",
                                    workload.kind.as_str(),
                                    workload.name
                                ),
                            );
                        }
                    }
                    Err(e) => {
                        instance_pending = true;
                        append_agent_error(
                            &mut pending_errs,
                            instance_key,
                            format!(
                                "status rpc failed kind={} name={} err={}",
                                workload.kind.as_str(),
                                workload.name,
                                e
                            ),
                        );
                    }
                }
            }
            if instance_pending {
                next_pending.insert(instance_key.clone(), workloads.clone());
            }
        }

        if next_pending.is_empty() {
            return;
        }
        if std::time::Instant::now() >= deadline {
            for (instance_key, mut errs) in pending_errs.into_iter() {
                errs.sort();
                errs.dedup();
                for err in errs.into_iter() {
                    append_agent_error(errs_by_instance, &instance_key, err);
                }
            }
            return;
        }

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        pending = next_pending;
    }
}

async fn handle_delete_workload(
    state: Arc<ControllerHttpState>,
    req: Request<Body>,
) -> anyhow::Result<Response<Body>> {
    let _deploy_guard = match state.deploy_guard.try_lock() {
        Ok(g) => g,
        Err(_) => {
            let resp = DeleteWorkloadResp {
                ok: false,
                err: Some("another deploy operation is in-flight; try again later".to_string()),
                results: Vec::new(),
            };
            return Ok(response_json(StatusCode::CONFLICT, &resp));
        }
    };

    let body_bytes =
        read_body_to_bytes_limited(req.into_body(), state.cfg.panel.max_body_bytes).await?;
    if body_bytes.is_empty() {
        let resp = DeleteWorkloadResp {
            ok: false,
            err: Some("empty delete_workload body is not allowed".to_string()),
            results: Vec::new(),
        };
        return Ok(response_json(StatusCode::BAD_REQUEST, &resp));
    }
    let req: DeleteWorkloadReq = serde_json::from_slice(&body_bytes)
        .map_err(|e| anyhow::anyhow!("delete_workload body must be json: {}", e))?;
    let name = req.name.trim().to_string();
    if name.is_empty() {
        let resp = DeleteWorkloadResp {
            ok: false,
            err: Some("name must be non-empty".to_string()),
            results: Vec::new(),
        };
        return Ok(response_json(StatusCode::BAD_REQUEST, &resp));
    }

    if let Err(e) = state.desired.remove(req.kind, &name).await {
        let resp = DeleteWorkloadResp {
            ok: false,
            err: Some(format!("persist desired state failed: {}", e)),
            results: Vec::new(),
        };
        return Ok(response_json(StatusCode::INTERNAL_SERVER_ERROR, &resp));
    }

    let mut workloads_by_instance: BTreeMap<String, Vec<WorkloadId>> = BTreeMap::new();
    let workload = WorkloadId {
        kind: req.kind,
        name: name.clone(),
        authority: name.clone(),
    };
    for instance_key in controller_list_agent_instance_keys(state.fw.as_ref()).into_iter() {
        add_workload_for_instance(&mut workloads_by_instance, instance_key, workload.clone());
    }
    normalize_workloads_by_instance(&mut workloads_by_instance);

    let mut errs_by_instance = controller_delete_generations_by_instance(
        state.as_ref(),
        &workloads_by_instance,
        ControllerDeleteGenerationMode::Immediate,
        None,
    )
    .await;
    if !errs_by_instance.is_empty() {
        let results = build_agent_results(&workloads_by_instance, errs_by_instance);
        let resp = DeleteWorkloadResp {
            ok: false,
            err: Some("one or more agents failed to delete workload generation".to_string()),
            results,
        };
        return Ok(response_json(StatusCode::BAD_GATEWAY, &resp));
    }

    controller_wait_workloads_stopped_by_instance(
        state.as_ref(),
        &workloads_by_instance,
        &mut errs_by_instance,
        STOP_PROCESS_WAIT_SECONDS,
    )
    .await;
    let results = build_agent_results(&workloads_by_instance, errs_by_instance);
    if results.iter().all(|r| r.ok) {
        let resp = DeleteWorkloadResp {
            ok: true,
            err: None,
            results,
        };
        return Ok(response_json(StatusCode::OK, &resp));
    }
    let resp = DeleteWorkloadResp {
        ok: false,
        err: Some(format!(
            "delete timed out after {}s (workload may still be stopping)",
            STOP_PROCESS_WAIT_SECONDS
        )),
        results,
    };
    Ok(response_json(StatusCode::BAD_GATEWAY, &resp))
}

async fn handle_delete_apply(
    state: Arc<ControllerHttpState>,
    req: Request<Body>,
) -> anyhow::Result<Response<Body>> {
    handle_delete_apply_inner(state, req, false).await
}

async fn handle_wait_delete_apply(
    state: Arc<ControllerHttpState>,
    req: Request<Body>,
) -> anyhow::Result<Response<Body>> {
    handle_delete_apply_inner(state, req, true).await
}

async fn handle_apply_wait(
    state: Arc<ControllerHttpState>,
    req: Request<Body>,
) -> anyhow::Result<Response<Body>> {
    let body_bytes =
        read_body_to_bytes_limited(req.into_body(), state.cfg.panel.max_body_bytes).await?;
    if body_bytes.is_empty() {
        let resp = ApplyWaitResp {
            ok: false,
            apply_id: String::new(),
            phase: None,
            runtime_goal: None,
            target_count: None,
            matching_target_count: None,
            pending_detail: None,
            err: Some("empty apply_wait body is not allowed".to_string()),
        };
        return Ok(response_json(StatusCode::BAD_REQUEST, &resp));
    }

    let req: ApplyWaitReq = serde_json::from_slice(&body_bytes)
        .map_err(|e| anyhow::anyhow!("apply_wait body must be json: {}", e))?;
    let apply_id = req.apply_id.trim().to_string();
    if apply_id.is_empty() {
        let resp = ApplyWaitResp {
            ok: false,
            apply_id,
            phase: None,
            runtime_goal: None,
            target_count: None,
            matching_target_count: None,
            pending_detail: None,
            err: Some("apply_id must be non-empty".to_string()),
        };
        return Ok(response_json(StatusCode::BAD_REQUEST, &resp));
    }

    let apply_rec = match state.desired.load_apply_record(&apply_id).await {
        Ok(v) => v,
        Err(e) => {
            let resp = ApplyWaitResp {
                ok: false,
                apply_id,
                phase: None,
                runtime_goal: None,
                target_count: None,
                matching_target_count: None,
                pending_detail: None,
                err: Some(format!("load apply record failed: {}", e)),
            };
            return Ok(response_json(StatusCode::NOT_FOUND, &resp));
        }
    };
    let phase = apply_lifecycle_phase_normalized(apply_rec.lifecycle_phase);
    if phase != ApplyLifecyclePhase::Running {
        let resp = ApplyWaitResp {
            ok: false,
            apply_id,
            phase: Some(phase),
            runtime_goal: Some(ApplyRuntimeGoal::Attached),
            target_count: None,
            matching_target_count: None,
            pending_detail: None,
            err: Some(format!(
                "apply_wait only supports RUNNING apply records: apply_id={} phase={:?}",
                req.apply_id.trim(),
                phase
            )),
        };
        return Ok(response_json(StatusCode::CONFLICT, &resp));
    }

    let desired_snapshot = state.desired.snapshot_apply_workloads(&apply_id);
    if desired_snapshot.is_empty() {
        let resp = ApplyWaitResp {
            ok: false,
            apply_id,
            phase: Some(phase),
            runtime_goal: Some(ApplyRuntimeGoal::Attached),
            target_count: Some(0),
            matching_target_count: Some(0),
            pending_detail: None,
            err: Some(
                "apply_wait requires desired workloads for the requested apply_id".to_string(),
            ),
        };
        return Ok(response_json(StatusCode::NOT_FOUND, &resp));
    }

    let runtime_goal = ApplyRuntimeGoal::Attached;
    let observation = controller_observe_apply_runtime_targets(
        state.as_ref(),
        &apply_id,
        desired_snapshot.as_slice(),
        runtime_goal,
    )
    .await?;
    let pending_detail = observation.pending_detail();
    let resp = ApplyWaitResp {
        ok: observation.converged(),
        apply_id: apply_id.clone(),
        phase: Some(phase),
        runtime_goal: Some(runtime_goal),
        target_count: Some(observation.target_count),
        matching_target_count: Some(observation.matching_target_count),
        pending_detail: pending_detail.clone(),
        err: if observation.converged() {
            None
        } else {
            Some(format!(
                "apply runtime attach not converged: apply_id={} matching_target_count={} target_count={} pending={}",
                apply_id,
                observation.matching_target_count,
                observation.target_count,
                pending_detail.unwrap_or_default()
            ))
        },
    };
    let code = if resp.ok {
        StatusCode::OK
    } else {
        StatusCode::BAD_GATEWAY
    };
    Ok(response_json(code, &resp))
}

fn controller_collect_apply_workloads_by_instance(
    apply_id: &str,
    desired_workloads: &[DesiredWorkload],
) -> BTreeMap<String, Vec<WorkloadId>> {
    let mut workloads_by_instance: BTreeMap<String, Vec<WorkloadId>> = BTreeMap::new();
    let mut ordered: Vec<&DesiredWorkload> = desired_workloads.iter().collect();
    ordered.sort_by(|left, right| desired_workload_delete_cmp(left, right));

    for w in ordered.into_iter() {
        for target in w.targets.iter() {
            let instance_key = match agent_instance_key_from_node_name(target) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!(
                        "[ops_controller:delete_apply] invalid target node name: apply_id={} target={} err={}",
                        apply_id, target, e
                    );
                    continue;
                }
            };
            add_workload_for_instance(
                &mut workloads_by_instance,
                instance_key,
                WorkloadId {
                    kind: w.kind,
                    name: w.name.clone(),
                    authority: desired_workload_authority(w).unwrap_or_else(|_| w.name.clone()),
                },
            );
        }
    }
    // English note:
    // - wait_delete_apply must keep atomic-group stop order intact on each agent.
    // - Sorting by workload name here would stop `ops_agent` before the workloads it must retire.
    dedup_workloads_by_instance_preserve_order(&mut workloads_by_instance);
    workloads_by_instance
}

fn desired_workload_ids(workloads: &[DesiredWorkload]) -> Vec<WorkloadId> {
    let mut out: Vec<WorkloadId> = workloads
        .iter()
        .map(|w| WorkloadId {
            kind: w.kind,
            name: w.name.clone(),
            authority: desired_workload_authority(w).unwrap_or_else(|_| w.name.clone()),
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

fn desired_apply_updated_ts_ms(workloads: &[DesiredWorkload]) -> u64 {
    workloads.iter().map(|w| w.updated_ts_ms).max().unwrap_or(0)
}

async fn controller_desired_workloads_for_apply(
    desired: &DesiredStore,
    apply_id: &str,
) -> anyhow::Result<Vec<DesiredWorkload>> {
    let desired_snapshot = desired.snapshot_apply_workloads(apply_id);
    if !desired_snapshot.is_empty() {
        return Ok(desired_snapshot);
    }
    let apply_rec = desired.load_apply_record(apply_id).await?;
    desired_workloads_from_apply_record(&apply_rec)
}

#[derive(Debug, Clone)]
struct ApplyRuntimeObservation {
    target_count: u64,
    matching_target_count: u64,
    pending: Vec<String>,
}

impl ApplyRuntimeObservation {
    fn converged(&self) -> bool {
        self.pending.is_empty()
    }

    fn pending_detail(&self) -> Option<String> {
        if self.pending.is_empty() {
            return None;
        }
        Some(self.pending.join("; "))
    }
}

async fn controller_observe_apply_runtime_targets(
    state: &ControllerHttpState,
    apply_id: &str,
    desired_workloads: &[DesiredWorkload],
    goal: ApplyRuntimeGoal,
) -> anyhow::Result<ApplyRuntimeObservation> {
    let apply_id = apply_id.trim();
    if apply_id.is_empty() {
        anyhow::bail!("apply_id must be non-empty");
    }

    let mut target_count = 0_u64;
    let mut matching_target_count = 0_u64;
    let mut pending: Vec<String> = Vec::new();
    let mut ordered_workloads = desired_workloads.to_vec();
    ordered_workloads.sort_by(|left, right| desired_workload_delete_cmp(left, right));

    for desired in ordered_workloads.into_iter() {
        let workload_key_text = format!("{}/{}", desired.kind.as_str(), desired.name);
        let desired_apply_id = desired
            .apply_id
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!("desired workload missing apply_id: {}", workload_key_text)
            })?;
        if desired_apply_id != apply_id {
            anyhow::bail!(
                "desired workload apply_id mismatch: workload={} expected_apply_id={} actual_apply_id={}",
                workload_key_text,
                apply_id,
                desired_apply_id
            );
        }

        let mut ordered_targets = desired.targets.clone();
        ordered_targets.sort();
        ordered_targets.dedup();

        for target in ordered_targets.into_iter() {
            target_count += 1;
            let instance_key = agent_instance_key_from_node_name(&target).with_context(|| {
                format!(
                    "resolve runtime observation target to agent instance failed: apply_id={} workload={} target={}",
                    apply_id, workload_key_text, target
                )
            })?;
            let status_resp = user_rpc_call_json::<StatusReq, StatusResp>(
                state.fw.as_ref(),
                &instance_key,
                "fluxon_ops/status",
                &StatusReq {
                    kind: desired.kind,
                    name: desired.name.clone(),
                    authority: desired_workload_authority(&desired)?,
                },
            )
            .await;

            match goal {
                ApplyRuntimeGoal::Attached => match status_resp {
                    Ok(v) => {
                        if !v.ok {
                            pending.push(format!(
                                "{}@{}:status_not_ok:{}",
                                workload_key_text,
                                instance_key,
                                v.err.unwrap_or_else(|| "unknown".to_string())
                            ));
                            continue;
                        }
                        if !v.running {
                            pending.push(format!(
                                "{}@{}:not_running",
                                workload_key_text, instance_key
                            ));
                            continue;
                        }
                        if v.present != Some(true) {
                            pending.push(format!(
                                "{}@{}:not_present",
                                workload_key_text, instance_key
                            ));
                            continue;
                        }
                        if v.apply_id.as_deref() != Some(desired_apply_id) {
                            pending.push(format!(
                                "{}@{}:apply_id_mismatch:expected={}:got={}",
                                workload_key_text,
                                instance_key,
                                desired_apply_id,
                                v.apply_id.unwrap_or_else(|| "none".to_string())
                            ));
                            continue;
                        }
                        matching_target_count += 1;
                    }
                    Err(e) => {
                        pending.push(format!(
                            "{}@{}:status_rpc_failed:{}",
                            workload_key_text, instance_key, e
                        ));
                    }
                },
                ApplyRuntimeGoal::Detached => match status_resp {
                    Ok(v) => {
                        if !v.ok {
                            pending.push(format!(
                                "{}@{}:status_not_ok:{}",
                                workload_key_text,
                                instance_key,
                                v.err.unwrap_or_else(|| "unknown".to_string())
                            ));
                            continue;
                        }
                        if v.running && v.apply_id.as_deref() == Some(desired_apply_id) {
                            pending.push(format!(
                                "{}@{}:still_running",
                                workload_key_text, instance_key
                            ));
                            continue;
                        }
                        matching_target_count += 1;
                    }
                    Err(e) => {
                        pending.push(format!(
                            "{}@{}:status_rpc_failed:{}",
                            workload_key_text, instance_key, e
                        ));
                    }
                },
            }
        }
    }

    Ok(ApplyRuntimeObservation {
        target_count,
        matching_target_count,
        pending,
    })
}

fn controller_find_latest_successor_apply_for_delete(
    desired_snapshot: &[DesiredWorkload],
    apply_id: &str,
) -> anyhow::Result<Option<(String, Vec<DesiredWorkload>)>> {
    let current_apply_id = apply_id.trim();
    if current_apply_id.is_empty() {
        anyhow::bail!("apply_id must be non-empty");
    }

    let current_workloads: Vec<DesiredWorkload> = desired_snapshot
        .iter()
        .filter(|w| w.apply_id.as_deref().map(|v| v.trim()) == Some(current_apply_id))
        .cloned()
        .collect();
    if current_workloads.is_empty() {
        return Ok(None);
    }
    let current_group_key =
        deployment_group_key_from_workloads(&desired_workload_ids(&current_workloads))?;

    let mut workloads_by_apply: BTreeMap<String, Vec<DesiredWorkload>> = BTreeMap::new();
    for workload in desired_snapshot.iter() {
        let Some(desired_apply_id) = workload.apply_id.as_deref() else {
            continue;
        };
        let desired_apply_id = desired_apply_id.trim();
        if desired_apply_id.is_empty() || desired_apply_id == current_apply_id {
            continue;
        }
        workloads_by_apply
            .entry(desired_apply_id.to_string())
            .or_default()
            .push(workload.clone());
    }

    let mut candidates: Vec<(u64, String, Vec<DesiredWorkload>)> = Vec::new();
    for (candidate_apply_id, candidate_workloads) in workloads_by_apply.into_iter() {
        let candidate_group_key = deployment_group_key_from_workloads(&desired_workload_ids(
            candidate_workloads.as_slice(),
        ))?;
        if candidate_group_key != current_group_key {
            continue;
        }
        candidates.push((
            desired_apply_updated_ts_ms(candidate_workloads.as_slice()),
            candidate_apply_id,
            candidate_workloads,
        ));
    }
    candidates.sort_by(|left, right| right.0.cmp(&left.0).then(left.1.cmp(&right.1)));
    Ok(candidates
        .into_iter()
        .next()
        .map(|(_, candidate_apply_id, candidate_workloads)| {
            (candidate_apply_id, candidate_workloads)
        }))
}

async fn controller_pending_successor_attach_detail_for_delete(
    state: &ControllerHttpState,
    current_apply_id: &str,
) -> anyhow::Result<Option<String>> {
    let desired_snapshot = state.desired.snapshot();
    let Some((successor_apply_id, successor_workloads)) =
        controller_find_latest_successor_apply_for_delete(
            desired_snapshot.as_slice(),
            current_apply_id,
        )?
    else {
        return Ok(None);
    };

    let observation = controller_observe_apply_runtime_targets(
        state,
        &successor_apply_id,
        successor_workloads.as_slice(),
        ApplyRuntimeGoal::Attached,
    )
    .await?;
    if observation.converged() {
        return Ok(None);
    }
    Ok(Some(format!(
        "current_apply_id={} successor_apply_id={} runtime_goal={} matching_target_count={} target_count={} pending={}",
        current_apply_id,
        successor_apply_id,
        ApplyRuntimeGoal::Attached.as_str(),
        observation.matching_target_count,
        observation.target_count,
        observation.pending_detail().unwrap_or_default()
    )))
}

async fn controller_collect_runtime_apply_workloads_by_instance(
    state: &ControllerHttpState,
    apply_id: &str,
    desired_workloads: &[DesiredWorkload],
) -> anyhow::Result<BTreeMap<String, Vec<WorkloadId>>> {
    let apply_id = apply_id.trim();
    if apply_id.is_empty() {
        anyhow::bail!("apply_id must be non-empty");
    }

    let expected_workloads_by_instance =
        controller_collect_apply_workloads_by_instance(apply_id, desired_workloads);
    if expected_workloads_by_instance.is_empty() {
        anyhow::bail!(
            "apply runtime observation has no expected target instances: apply_id={}",
            apply_id
        );
    }

    let live_agents: HashSet<String> = controller_list_agent_instance_keys(state.fw.as_ref())
        .into_iter()
        .collect();
    let mut workloads_by_instance: BTreeMap<String, Vec<WorkloadId>> = BTreeMap::new();

    for instance_key in expected_workloads_by_instance.keys() {
        if !live_agents.contains(instance_key) {
            let mut ordered_live_agents: Vec<String> = live_agents.iter().cloned().collect();
            ordered_live_agents.sort();
            anyhow::bail!(
                "expected agent missing from controller membership while observing apply delete runtime: apply_id={} instance_key={} live_agents={:?}",
                apply_id,
                instance_key,
                ordered_live_agents
            );
        }
        let resp = user_rpc_call_json::<ListApplyRuntimeReq, ListApplyRuntimeResp>(
            state.fw.as_ref(),
            instance_key,
            "fluxon_ops/list_apply_runtime",
            &ListApplyRuntimeReq {
                apply_id: apply_id.to_string(),
            },
        )
        .await
        .with_context(|| {
            format!(
                "list_apply_runtime rpc failed: instance_key={}",
                instance_key
            )
        })?;

        if !resp.ok {
            anyhow::bail!(
                "list_apply_runtime not ok: instance_key={} err={}",
                instance_key,
                resp.err.unwrap_or_else(|| "unknown".to_string())
            );
        }

        for w in resp.workloads.unwrap_or_default().into_iter() {
            if w.apply_id.as_deref().map(|s| s.trim()) != Some(apply_id) {
                continue;
            }
            add_workload_for_instance(
                &mut workloads_by_instance,
                instance_key.clone(),
                WorkloadId {
                    kind: w.kind,
                    name: w.name.clone(),
                    authority: w.authority.clone(),
                },
            );
        }
    }

    normalize_workloads_by_instance(&mut workloads_by_instance);
    Ok(workloads_by_instance)
}

fn format_workloads_by_instance(
    workloads_by_instance: &BTreeMap<String, Vec<WorkloadId>>,
) -> String {
    let mut parts: Vec<String> = Vec::new();
    for (instance_key, workloads) in workloads_by_instance.iter() {
        let mut rendered: Vec<String> = Vec::new();
        for workload in workloads.iter() {
            rendered.push(format!("{}/{}", workload.kind.as_str(), workload.name));
        }
        parts.push(format!("{}:[{}]", instance_key, rendered.join(",")));
    }
    parts.join("; ")
}

async fn controller_request_delete_apply_workloads(
    state: &ControllerHttpState,
    workloads_by_instance: &BTreeMap<String, Vec<WorkloadId>>,
    require_apply_id: &str,
) -> DeleteApplyResp {
    let errs_by_instance = controller_delete_generations_by_instance(
        state,
        workloads_by_instance,
        ControllerDeleteGenerationMode::Immediate,
        Some(require_apply_id),
    )
    .await;
    // English note:
    // - delete_apply persists delete intent first, then issues best-effort delete_generation RPCs
    //   for faster convergence when agents are reachable.
    // - The RPC errors remain diagnostic only here; final "is it gone?" authority belongs to
    //   wait_delete_apply / reconcile, which observe live runtime by apply_id before finalizing desired.
    let results = build_agent_results(workloads_by_instance, errs_by_instance);
    DeleteApplyResp {
        ok: true,
        err: None,
        results,
    }
}

async fn handle_delete_apply_inner(
    state: Arc<ControllerHttpState>,
    req: Request<Body>,
    wait_for_stop: bool,
) -> anyhow::Result<Response<Body>> {
    let op_name = if wait_for_stop {
        "wait_delete_apply"
    } else {
        "delete_apply"
    };
    let _deploy_guard = match state.deploy_guard.try_lock() {
        Ok(g) => g,
        Err(_) => {
            let resp = DeleteApplyResp {
                ok: false,
                err: Some("another deploy operation is in-flight; try again later".to_string()),
                results: Vec::new(),
            };
            return Ok(response_json(StatusCode::CONFLICT, &resp));
        }
    };

    let body_bytes =
        read_body_to_bytes_limited(req.into_body(), state.cfg.panel.max_body_bytes).await?;
    if body_bytes.is_empty() {
        let resp = DeleteApplyResp {
            ok: false,
            err: Some(format!("empty {} body is not allowed", op_name)),
            results: Vec::new(),
        };
        return Ok(response_json(StatusCode::BAD_REQUEST, &resp));
    }

    let req: DeleteApplyReq = serde_json::from_slice(&body_bytes)
        .map_err(|e| anyhow::anyhow!("{} body must be json: {}", op_name, e))?;

    let apply_id = req.apply_id.trim().to_string();
    if apply_id.is_empty() {
        let resp = DeleteApplyResp {
            ok: false,
            err: Some("apply_id must be non-empty".to_string()),
            results: Vec::new(),
        };
        return Ok(response_json(StatusCode::BAD_REQUEST, &resp));
    }

    let apply_rec = match state.desired.load_apply_record(&apply_id).await {
        Ok(v) => v,
        Err(e) => {
            let resp = DeleteApplyResp {
                ok: false,
                err: Some(format!(
                    "apply record not found under desired/applies: apply_id={} err={}",
                    apply_id, e
                )),
                results: Vec::new(),
            };
            return Ok(response_json(StatusCode::NOT_FOUND, &resp));
        }
    };
    let current_phase = apply_lifecycle_phase_normalized(apply_rec.lifecycle_phase);
    let desired_snapshot =
        match controller_desired_workloads_for_apply(state.desired.as_ref(), &apply_id).await {
            Ok(v) => v,
            Err(e) => {
                let resp = DeleteApplyResp {
                    ok: false,
                    err: Some(format!(
                        "load apply desired workloads failed: apply_id={} err={}",
                        apply_id, e
                    )),
                    results: Vec::new(),
                };
                return Ok(response_json(StatusCode::INTERNAL_SERVER_ERROR, &resp));
            }
        };

    if wait_for_stop {
        // English note:
        // - wait_delete_apply is the drain barrier between generations. It must not publish new
        //   delete intent or re-interpret missing desired state as success.
        // - The wait path only observes apply-scoped live runtime and finalizes desired/applies
        //   after that exact apply_id has disappeared from every agent.
        if current_phase == ApplyLifecyclePhase::Running {
            let resp = DeleteApplyResp {
                ok: false,
                err: Some(format!(
                    "wait_delete_apply requires delete_apply first: apply_id={} phase={:?}",
                    apply_id, current_phase
                )),
                results: Vec::new(),
            };
            return Ok(response_json(StatusCode::CONFLICT, &resp));
        }

        let runtime_workloads_by_instance =
            match controller_collect_runtime_apply_workloads_by_instance(
                state.as_ref(),
                &apply_id,
                desired_snapshot.as_slice(),
            )
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    let resp = DeleteApplyResp {
                        ok: false,
                        err: Some(format!(
                            "one or more workloads may still be stopping: runtime listing failed for apply_id={}: {}",
                            apply_id, e
                        )),
                        results: Vec::new(),
                    };
                    return Ok(response_json(StatusCode::BAD_GATEWAY, &resp));
                }
            };
        if !runtime_workloads_by_instance.is_empty() {
            let resp = DeleteApplyResp {
                ok: false,
                err: Some(format!(
                    "one or more workloads may still be stopping: apply_id={} pending={}",
                    apply_id,
                    format_workloads_by_instance(&runtime_workloads_by_instance)
                )),
                results: Vec::new(),
            };
            return Ok(response_json(StatusCode::BAD_GATEWAY, &resp));
        }

        if let Err(e) = state.desired.remove_apply(&apply_id).await {
            let resp = DeleteApplyResp {
                ok: false,
                err: Some(format!("failed to finalize desired apply delete: {}", e)),
                results: Vec::new(),
            };
            return Ok(response_json(StatusCode::INTERNAL_SERVER_ERROR, &resp));
        }
        let resp = DeleteApplyResp {
            ok: true,
            err: None,
            results: Vec::new(),
        };
        return Ok(response_json(StatusCode::OK, &resp));
    }

    // English note:
    // - delete_apply owns the state transition into DELETE_NOTIFYING plus the best-effort
    //   immediate stop requests.
    // - wait_delete_apply / reconcile own the final "runtime absent -> remove apply record"
    //   transition so callers no longer infer host quiescence from desired-state disappearance.
    if current_phase == ApplyLifecyclePhase::Running {
        if let Some(detail) =
            controller_pending_successor_attach_detail_for_delete(state.as_ref(), &apply_id).await?
        {
            let resp = DeleteApplyResp {
                ok: false,
                err: Some(format!(
                    "refusing to stop apply before successor attach converges: {}",
                    detail
                )),
                results: Vec::new(),
            };
            return Ok(response_json(StatusCode::CONFLICT, &resp));
        }
        if let Err(e) = state.desired.mark_apply_delete_notifying(&apply_id).await {
            let resp = DeleteApplyResp {
                ok: false,
                err: Some(format!(
                    "persist apply DELETE_NOTIFYING phase failed: {}",
                    e
                )),
                results: Vec::new(),
            };
            return Ok(response_json(StatusCode::INTERNAL_SERVER_ERROR, &resp));
        }
    }

    let workloads_by_instance =
        controller_collect_apply_workloads_by_instance(&apply_id, &desired_snapshot);
    let resp = controller_request_delete_apply_workloads(
        state.as_ref(),
        &workloads_by_instance,
        &apply_id,
    )
    .await;
    Ok(response_json(StatusCode::OK, &resp))
}

async fn handle_delete_workload_on_agent(
    state: Arc<ControllerHttpState>,
    req: Request<Body>,
) -> anyhow::Result<Response<Body>> {
    let _deploy_guard = match state.deploy_guard.try_lock() {
        Ok(g) => g,
        Err(_) => {
            let resp = DeleteWorkloadOnAgentResp {
                ok: false,
                err: Some("another deploy operation is in-flight; try again later".to_string()),
                result: AgentOpResult {
                    instance_key: String::new(),
                    ok: false,
                    err: None,
                },
            };
            return Ok(response_json(StatusCode::CONFLICT, &resp));
        }
    };

    let body_bytes =
        read_body_to_bytes_limited(req.into_body(), state.cfg.panel.max_body_bytes).await?;
    if body_bytes.is_empty() {
        let resp = DeleteWorkloadOnAgentResp {
            ok: false,
            err: Some("empty delete_workload_on_agent body is not allowed".to_string()),
            result: AgentOpResult {
                instance_key: String::new(),
                ok: false,
                err: None,
            },
        };
        return Ok(response_json(StatusCode::BAD_REQUEST, &resp));
    }

    let req: DeleteWorkloadOnAgentReq = serde_json::from_slice(&body_bytes)
        .map_err(|e| anyhow::anyhow!("delete_workload_on_agent body must be json: {}", e))?;

    let instance_key = req.instance_key.trim().to_string();
    if instance_key.is_empty() {
        let resp = DeleteWorkloadOnAgentResp {
            ok: false,
            err: Some("instance_key must be non-empty".to_string()),
            result: AgentOpResult {
                instance_key,
                ok: false,
                err: None,
            },
        };
        return Ok(response_json(StatusCode::BAD_REQUEST, &resp));
    }
    if !instance_key.starts_with(OPS_AGENT_INSTANCE_KEY_PREFIX)
        || instance_key == "fluxon_ops_controller"
    {
        let resp = DeleteWorkloadOnAgentResp {
            ok: false,
            err: Some(format!(
                "invalid instance_key: {} (expected fluxon_ops_<node>)",
                instance_key
            )),
            result: AgentOpResult {
                instance_key,
                ok: false,
                err: None,
            },
        };
        return Ok(response_json(StatusCode::BAD_REQUEST, &resp));
    }

    let name = req.name.trim().to_string();
    if name.is_empty() {
        let resp = DeleteWorkloadOnAgentResp {
            ok: false,
            err: Some("name must be non-empty".to_string()),
            result: AgentOpResult {
                instance_key,
                ok: false,
                err: None,
            },
        };
        return Ok(response_json(StatusCode::BAD_REQUEST, &resp));
    }

    if let Err(e) = state.desired.remove(req.kind, &name).await {
        let resp = DeleteWorkloadOnAgentResp {
            ok: false,
            err: Some(format!("persist desired state failed: {}", e)),
            result: AgentOpResult {
                instance_key,
                ok: false,
                err: None,
            },
        };
        return Ok(response_json(StatusCode::INTERNAL_SERVER_ERROR, &resp));
    }

    let mut workloads_by_instance: BTreeMap<String, Vec<WorkloadId>> = BTreeMap::new();
    add_workload_for_instance(
        &mut workloads_by_instance,
        instance_key.clone(),
        WorkloadId {
            kind: req.kind,
            name: name.clone(),
            authority: name.clone(),
        },
    );
    normalize_workloads_by_instance(&mut workloads_by_instance);

    let mut errs_by_instance = controller_delete_generations_by_instance(
        state.as_ref(),
        &workloads_by_instance,
        ControllerDeleteGenerationMode::Immediate,
        None,
    )
    .await;
    if !errs_by_instance.is_empty() {
        let result = build_agent_results(&workloads_by_instance, errs_by_instance)
            .into_iter()
            .next()
            .unwrap();
        let resp = DeleteWorkloadOnAgentResp {
            ok: false,
            err: Some("agent failed to delete workload generation".to_string()),
            result,
        };
        return Ok(response_json(StatusCode::BAD_GATEWAY, &resp));
    }

    controller_wait_workloads_stopped_by_instance(
        state.as_ref(),
        &workloads_by_instance,
        &mut errs_by_instance,
        STOP_PROCESS_WAIT_SECONDS,
    )
    .await;
    let result = build_agent_results(&workloads_by_instance, errs_by_instance)
        .into_iter()
        .next()
        .unwrap();
    if result.ok {
        let resp = DeleteWorkloadOnAgentResp {
            ok: true,
            err: None,
            result,
        };
        return Ok(response_json(StatusCode::OK, &resp));
    }
    let resp = DeleteWorkloadOnAgentResp {
        ok: false,
        err: Some(format!(
            "delete timed out after {}s (workload may still be stopping)",
            STOP_PROCESS_WAIT_SECONDS
        )),
        result,
    };
    Ok(response_json(StatusCode::BAD_GATEWAY, &resp))
}

async fn handle_deploy(
    state: Arc<ControllerHttpState>,
    req: Request<Body>,
) -> anyhow::Result<Response<Body>> {
    let _deploy_guard = match state.deploy_guard.try_lock() {
        Ok(g) => g,
        Err(_) => {
            let resp = DeployResp {
                ok: false,
                phase: DeployPhase::Failed,
                message: None,
                err: Some("another deploy operation is in-flight; try again later".to_string()),
                history_id: None,
                results: None,
            };
            return Ok(response_json(StatusCode::CONFLICT, &resp));
        }
    };

    {
        let max_bytes = state.cfg.panel.max_body_bytes;
        if let Some(content_len) = content_length_optional(req.headers())? {
            if content_len > max_bytes {
                let resp = DeployResp {
                    ok: false,
                    phase: DeployPhase::Failed,
                    message: None,
                    err: Some(format!(
                        "deploy payload too large: content_length={} max_bytes={}",
                        content_len, max_bytes
                    )),
                    history_id: None,
                    results: None,
                };
                return Ok(response_json(StatusCode::PAYLOAD_TOO_LARGE, &resp));
            }
        }
    }

    let history_id = uuid::Uuid::new_v4().to_string();
    let ts_ms = now_ts_ms();

    let body_bytes =
        read_body_to_bytes_limited(req.into_body(), state.cfg.panel.max_body_bytes).await?;
    if body_bytes.is_empty() {
        let rec = DeployHistoryRecord {
            id: history_id.clone(),
            ts_ms,
            deployment_yaml: String::new(),
            namespace: None,
            deployment_name: None,
            upload_id: None,
            upload_path: None,
            upload_bytes: None,
            targets: None,
            payload_dest_path: None,
            exec_argv: None,
            exec_cwd: None,
            results: None,
            err: Some("empty deploy body is not allowed".to_string()),
        };
        write_history_record(&state.history_dir, &rec).await?;
        let resp = DeployResp {
            ok: false,
            phase: DeployPhase::Failed,
            message: None,
            err: Some("empty deploy body is not allowed".to_string()),
            history_id: Some(history_id.clone()),
            results: None,
        };
        return Ok(response_json(StatusCode::BAD_REQUEST, &resp));
    }
    let doc = String::from_utf8(body_bytes).map_err(|e| {
        anyhow::anyhow!(
            "deploy body must be utf-8 (Deployment/DaemonSet YAML): {}",
            e
        )
    })?;

    let deployment_yaml = doc.clone();
    let deployment_yaml_sha256 = sha256_hex(deployment_yaml.as_bytes());

    let deploy_specs = match parse_k8s_deployment_subset_documents(&doc) {
        Ok(v) => v,
        Err(e) => {
            let rec = DeployHistoryRecord {
                id: history_id.clone(),
                ts_ms,
                deployment_yaml: deployment_yaml.clone(),
                namespace: None,
                deployment_name: None,
                upload_id: None,
                upload_path: None,
                upload_bytes: None,
                targets: None,
                payload_dest_path: None,
                exec_argv: None,
                exec_cwd: None,
                results: None,
                err: Some(format!("parse deployment failed: {}", e)),
            };
            write_history_record(&state.history_dir, &rec).await?;
            let resp = DeployResp {
                ok: false,
                phase: DeployPhase::Failed,
                message: None,
                err: Some(format!("parse deployment failed: {}", e)),
                history_id: Some(history_id.clone()),
                results: None,
            };
            return Ok(response_json(StatusCode::BAD_REQUEST, &resp));
        }
    };

    let deploy_namespace = match namespace_from_specs(&deploy_specs) {
        Ok(v) => v,
        Err(e) => {
            let rec = DeployHistoryRecord {
                id: history_id.clone(),
                ts_ms,
                deployment_yaml: deployment_yaml.clone(),
                namespace: None,
                deployment_name: if deploy_specs.len() == 1 {
                    Some(deploy_specs[0].name.clone())
                } else {
                    Some("<multi>".to_string())
                },
                upload_id: None,
                upload_path: None,
                upload_bytes: None,
                targets: None,
                payload_dest_path: None,
                exec_argv: None,
                exec_cwd: None,
                results: None,
                err: Some(format!("parse deployment failed: {}", e)),
            };
            write_history_record(&state.history_dir, &rec).await?;
            let resp = DeployResp {
                ok: false,
                phase: DeployPhase::Failed,
                message: None,
                err: Some(format!("parse deployment failed: {}", e)),
                history_id: Some(history_id.clone()),
                results: None,
            };
            return Ok(response_json(StatusCode::BAD_REQUEST, &resp));
        }
    };

    {
        let mut seen: HashSet<String> = HashSet::new();
        for spec in deploy_specs.iter() {
            let key = workload_key(spec.kind, &spec.name);
            if !seen.insert(key.clone()) {
                let rec = DeployHistoryRecord {
                    id: history_id.clone(),
                    ts_ms,
                    deployment_yaml: deployment_yaml.clone(),
                    namespace: deploy_namespace.clone(),
                    deployment_name: Some("<multi>".to_string()),
                    upload_id: None,
                    upload_path: None,
                    upload_bytes: None,
                    targets: None,
                    payload_dest_path: None,
                    exec_argv: None,
                    exec_cwd: None,
                    results: None,
                    err: Some(format!("duplicate workload in deploy payload: {}", key)),
                };
                write_history_record(&state.history_dir, &rec).await?;
                let resp = DeployResp {
                    ok: false,
                    phase: DeployPhase::Failed,
                    message: None,
                    err: Some(format!("duplicate workload in deploy payload: {}", key)),
                    history_id: Some(history_id.clone()),
                    results: None,
                };
                return Ok(response_json(StatusCode::BAD_REQUEST, &resp));
            }
        }
    }

    {
        // English note:
        // - Self-host applies may stop/replace the ops_controller while this HTTP request is still in-flight.
        // - If the YAML only lives in memory (request body), a controller crash makes it impossible to reconstruct
        //   the apply payload later, which breaks UI Show YAML / Reapply.
        // - We persist the apply YAML into Desired first, then Desired workload entries reference it by apply_id.
        let rec = DeployApplyRecord {
            id: history_id.clone(),
            ts_ms,
            deployment_yaml: deployment_yaml.clone(),
            namespace: deploy_namespace.clone(),
            deployment_yaml_sha256: deployment_yaml_sha256.clone(),
            lifecycle_phase: None,
            lifecycle_phase_updated_ts_ms: None,
        };
        state.desired.persist_apply_record(&rec).await?;
    }

    let desired_workloads =
        desired_workloads_from_specs(deploy_specs.as_slice(), &history_id, ts_ms);

    if let Err(e) = state.desired.upsert_many(desired_workloads).await {
        let rec = DeployHistoryRecord {
            id: history_id.clone(),
            ts_ms,
            deployment_yaml: deployment_yaml.clone(),
            namespace: deploy_namespace.clone(),
            deployment_name: if deploy_specs.len() == 1 {
                Some(deploy_specs[0].name.clone())
            } else {
                Some("<multi>".to_string())
            },
            upload_id: None,
            upload_path: None,
            upload_bytes: None,
            targets: None,
            payload_dest_path: None,
            exec_argv: None,
            exec_cwd: None,
            results: None,
            err: Some(format!("persist desired state failed: {}", e)),
        };
        write_history_record(&state.history_dir, &rec).await?;
        let resp = DeployResp {
            ok: false,
            phase: DeployPhase::Failed,
            message: None,
            err: Some(format!("persist desired state failed: {}", e)),
            history_id: Some(history_id.clone()),
            results: None,
        };
        return Ok(response_json(StatusCode::INTERNAL_SERVER_ERROR, &resp));
    }

    let resp = DeployResp {
        ok: true,
        phase: DeployPhase::Accepted,
        message: Some(format!(
            "Desired state persisted. The controller reconcile loop (interval_ms={}) will converge the deployment asynchronously; use history_id as apply_id for POST /api/apply_wait, and use /api/current_deployments or /api/workloads for diagnostics.",
            state.cfg.reconcile.interval_ms
        )),
        err: None,
        history_id: Some(history_id.clone()),
        results: None,
    };
    Ok(response_json(StatusCode::ACCEPTED, &resp))
}

async fn handle_history_list(state: Arc<ControllerHttpState>) -> anyhow::Result<Response<Body>> {
    let mut rd = tokio::fs::read_dir(&state.history_dir)
        .await
        .with_context(|| format!("read history dir: {}", state.history_dir.display()))?;

    let mut records: Vec<DeployHistorySummary> = Vec::new();

    while let Some(ent) = rd.next_entry().await.context("read_dir next_entry")? {
        let path = ent.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let buf = tokio::fs::read(&path)
            .await
            .with_context(|| format!("read history file: {}", path.display()))?;
        let rec: DeployHistoryRecord = serde_json::from_slice(&buf).with_context(|| {
            format!(
                "decode history json failed (temporarily not supported to skip broken record): {}",
                path.display()
            )
        })?;

        let ok = match rec.results.as_ref() {
            None => false,
            Some(v) => v.iter().all(|r| r.ok),
        };

        records.push(DeployHistorySummary {
            id: rec.id,
            ts_ms: rec.ts_ms,
            namespace: rec.namespace,
            deployment_name: rec.deployment_name,
            ok: ok && rec.err.is_none(),
            err: rec.err,
        });
    }

    records.sort_by(|a, b| b.ts_ms.cmp(&a.ts_ms));
    Ok(response_json(StatusCode::OK, &records))
}

async fn handle_history_get(
    state: Arc<ControllerHttpState>,
    path: &str,
) -> anyhow::Result<Response<Body>> {
    let id = path.trim_start_matches("/api/history/").trim();
    if id.is_empty() {
        return Ok(response_plain(
            StatusCode::BAD_REQUEST,
            "missing history id",
        ));
    }
    if id.contains('/') {
        return Ok(response_plain(
            StatusCode::BAD_REQUEST,
            "invalid history id",
        ));
    }

    let file = state.history_dir.join(format!("{}.json", id));
    if !file.exists() {
        return Ok(response_plain(
            StatusCode::NOT_FOUND,
            "history id not found",
        ));
    }

    let buf = tokio::fs::read(&file)
        .await
        .with_context(|| format!("read history file: {}", file.display()))?;
    let rec: DeployHistoryRecord = serde_json::from_slice(&buf)
        .with_context(|| format!("decode history json failed: {}", file.display()))?;

    Ok(response_json(StatusCode::OK, &rec))
}

async fn handle_apply_get(
    state: Arc<ControllerHttpState>,
    path: &str,
) -> anyhow::Result<Response<Body>> {
    let id = path.trim_start_matches("/api/apply/").trim();
    if id.is_empty() {
        return Ok(response_plain(StatusCode::BAD_REQUEST, "missing apply id"));
    }
    if id.contains('/') {
        return Ok(response_plain(StatusCode::BAD_REQUEST, "invalid apply id"));
    }

    let rec = state.desired.load_apply_record(id).await?;
    Ok(response_json(StatusCode::OK, &rec))
}

#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
struct CurrentDeploymentsResp {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
    groups: Vec<CurrentDeploymentGroup>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
struct CurrentDeploymentGroup {
    deployment_group_key: String,
    apply_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    namespace: Option<String>,
    phase: ApplyLifecyclePhase,
    runtime_goal: ApplyRuntimeGoal,
    runtime_converged: bool,
    runtime_matching_target_count: u64,
    runtime_target_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime_pending_detail: Option<String>,
    updated_ts_ms: u64,
    workloads: Vec<WorkloadId>,
}

async fn handle_current_deployments(
    state: Arc<ControllerHttpState>,
) -> anyhow::Result<Response<Body>> {
    let desired = state.desired.snapshot();
    if desired.is_empty() {
        let resp = CurrentDeploymentsResp {
            ok: true,
            err: None,
            groups: Vec::new(),
        };
        return Ok(response_json(StatusCode::OK, &resp));
    }

    let mut by_apply: BTreeMap<String, Vec<WorkloadId>> = BTreeMap::new();
    let mut desired_by_apply: BTreeMap<String, Vec<DesiredWorkload>> = BTreeMap::new();
    let mut apply_ts: BTreeMap<String, u64> = BTreeMap::new();
    let mut apply_namespace: BTreeMap<String, Option<String>> = BTreeMap::new();

    for w in desired.into_iter() {
        let Some(apply_id) = w.apply_id.as_deref() else {
            let resp = CurrentDeploymentsResp {
                ok: false,
                err: Some(format!(
                    "desired workload is missing apply_id (legacy state not supported): kind={} name={}",
                    w.kind.as_str(),
                    w.name
                )),
                groups: Vec::new(),
            };
            return Ok(response_json(StatusCode::INTERNAL_SERVER_ERROR, &resp));
        };
        let apply_id = apply_id.trim();
        if apply_id.is_empty() {
            let resp = CurrentDeploymentsResp {
                ok: false,
                err: Some(format!(
                    "desired workload has empty apply_id (legacy state not supported): kind={} name={}",
                    w.kind.as_str(),
                    w.name
                )),
                groups: Vec::new(),
            };
            return Ok(response_json(StatusCode::INTERNAL_SERVER_ERROR, &resp));
        }

        by_apply
            .entry(apply_id.to_string())
            .or_default()
            .push(WorkloadId {
                kind: w.kind,
                name: w.name.clone(),
                authority: desired_workload_authority(&w).unwrap_or_else(|_| w.name.clone()),
            });
        desired_by_apply
            .entry(apply_id.to_string())
            .or_default()
            .push(w.clone());

        let entry = apply_namespace
            .entry(apply_id.to_string())
            .or_insert_with(|| w.namespace.clone());
        if entry.is_none() {
            *entry = w.namespace.clone();
        } else if w.namespace.is_some() && *entry != w.namespace {
            let resp = CurrentDeploymentsResp {
                ok: false,
                err: Some(format!(
                    "desired workloads for one apply_id must share the same namespace: apply_id={} expected={:?} got={:?}",
                    apply_id, entry, w.namespace
                )),
                groups: Vec::new(),
            };
            return Ok(response_json(StatusCode::INTERNAL_SERVER_ERROR, &resp));
        }

        let ts0 = apply_ts.get(apply_id).copied().unwrap_or(0);
        apply_ts.insert(apply_id.to_string(), ts0.max(w.updated_ts_ms));
    }

    let mut groups: Vec<CurrentDeploymentGroup> = Vec::new();
    let mut missing_apply_records: Vec<(String, Vec<WorkloadId>)> = Vec::new();
    for (apply_id, mut workloads) in by_apply.into_iter() {
        workloads.sort();
        workloads.dedup();
        let desired_workloads = desired_by_apply.remove(&apply_id).unwrap_or_default();
        if desired_workloads.is_empty() {
            let resp = CurrentDeploymentsResp {
                ok: false,
                err: Some(format!(
                    "inconsistent state: apply_id={} missing desired workload entries while current_deployments groups are built",
                    apply_id
                )),
                groups: Vec::new(),
            };
            return Ok(response_json(StatusCode::INTERNAL_SERVER_ERROR, &resp));
        }

        let apply_rec = match state.desired.load_apply_record(&apply_id).await {
            Ok(v) => v,
            Err(_) => {
                missing_apply_records.push((apply_id, workloads));
                continue;
            }
        };

        let namespace = match (
            apply_namespace.get(&apply_id).cloned().flatten(),
            apply_rec.namespace.clone(),
        ) {
            (Some(a), Some(b)) if a != b => {
                let resp = CurrentDeploymentsResp {
                    ok: false,
                    err: Some(format!(
                        "desired/apply namespace mismatch: apply_id={} desired_namespace={} apply_namespace={}",
                        apply_id, a, b
                    )),
                    groups: Vec::new(),
                };
                return Ok(response_json(StatusCode::INTERNAL_SERVER_ERROR, &resp));
            }
            (Some(a), _) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };

        let phase = apply_lifecycle_phase_normalized(apply_rec.lifecycle_phase);
        let deployment_group_key = deployment_group_key_from_workloads(&workloads)?;
        let updated_ts_ms = apply_ts.get(&apply_id).copied().unwrap_or(0);
        let runtime_goal = match phase {
            ApplyLifecyclePhase::Running => ApplyRuntimeGoal::Attached,
            ApplyLifecyclePhase::DeleteNotifying => ApplyRuntimeGoal::Detached,
            ApplyLifecyclePhase::DeleteCommitted => ApplyRuntimeGoal::Detached,
        };
        let runtime_observation = controller_observe_apply_runtime_targets(
            state.as_ref(),
            &apply_id,
            desired_workloads.as_slice(),
            runtime_goal,
        )
        .await?;
        groups.push(CurrentDeploymentGroup {
            deployment_group_key,
            apply_id,
            namespace,
            phase,
            runtime_goal,
            runtime_converged: runtime_observation.converged(),
            runtime_matching_target_count: runtime_observation.matching_target_count,
            runtime_target_count: runtime_observation.target_count,
            runtime_pending_detail: runtime_observation.pending_detail(),
            updated_ts_ms,
            workloads,
        });
    }

    if !missing_apply_records.is_empty() {
        let mut details: Vec<String> = Vec::new();
        for (apply_id, workloads) in missing_apply_records.iter() {
            let mut ws: Vec<String> = Vec::new();
            for w in workloads.iter() {
                ws.push(format!("{}/{}", w.kind.as_str(), w.name));
            }
            details.push(format!(
                "apply_id={} workloads=[{}]",
                apply_id,
                ws.join(", ")
            ));
        }

        let resp = CurrentDeploymentsResp {
            ok: false,
            err: Some(format!(
                "inconsistent state: desired references apply_id(s) with missing apply record(s) (expected under desired/applies); details: {}",
                details.join("; ")
            )),
            groups: Vec::new(),
        };
        return Ok(response_json(StatusCode::INTERNAL_SERVER_ERROR, &resp));
    }

    groups.sort_by(|a, b| b.updated_ts_ms.cmp(&a.updated_ts_ms));
    let resp = CurrentDeploymentsResp {
        ok: true,
        err: None,
        groups,
    };
    Ok(response_json(StatusCode::OK, &resp))
}

#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
struct HistoryGroupedResp {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
    groups: Vec<HistoryGroup>,
    ungrouped: Vec<HistoryGroupedRecord>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
struct HistoryGroup {
    deployment_group_key: String,
    latest_ts_ms: u64,
    records: Vec<HistoryGroupedRecord>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
struct HistoryGroupedRecord {
    id: String,
    ts_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    namespace: Option<String>,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workloads: Option<Vec<WorkloadId>>,
}

async fn handle_history_grouped(state: Arc<ControllerHttpState>) -> anyhow::Result<Response<Body>> {
    let mut rd = tokio::fs::read_dir(&state.history_dir)
        .await
        .with_context(|| format!("read history dir: {}", state.history_dir.display()))?;

    let mut groups: BTreeMap<String, Vec<HistoryGroupedRecord>> = BTreeMap::new();
    let mut ungrouped: Vec<HistoryGroupedRecord> = Vec::new();

    while let Some(ent) = rd.next_entry().await.context("read_dir next_entry")? {
        let path = ent.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let buf = tokio::fs::read(&path)
            .await
            .with_context(|| format!("read history file: {}", path.display()))?;
        let rec: DeployHistoryRecord = serde_json::from_slice(&buf).with_context(|| {
            format!(
                "decode history json failed (temporarily not supported to skip broken record): {}",
                path.display()
            )
        })?;

        let ok = match rec.results.as_ref() {
            None => false,
            Some(v) => v.iter().all(|r| r.ok),
        } && rec.err.is_none();

        let mut workloads: Option<Vec<WorkloadId>> = None;
        let mut group_key: Option<String> = None;
        let mut namespace = rec.namespace.clone();

        if !rec.deployment_yaml.trim().is_empty() {
            if let Ok(specs) = parse_k8s_deployment_subset_documents(&rec.deployment_yaml) {
                if namespace.is_none() {
                    if let Ok(parsed_namespace) = namespace_from_specs(&specs) {
                        namespace = parsed_namespace;
                    }
                }
                let mut wids: Vec<WorkloadId> = specs
                    .into_iter()
                    .map(|s| {
                        let authority = selection_authority_name(
                            s.kind,
                            s.logical_selection.as_str(),
                            s.service_name.as_str(),
                            s.atomic_group.as_ref(),
                        )
                        .unwrap_or_else(|_| s.name.clone());
                        WorkloadId::new(s.kind, s.name, authority)
                    })
                    .collect();
                wids.sort();
                wids.dedup();
                if let Ok(k) = deployment_group_key_from_workloads(&wids) {
                    group_key = Some(k);
                    workloads = Some(wids);
                }
            }
        }

        let item = HistoryGroupedRecord {
            id: rec.id,
            ts_ms: rec.ts_ms,
            namespace,
            ok,
            err: rec.err,
            workloads,
        };

        match group_key {
            Some(k) => groups.entry(k).or_default().push(item),
            None => ungrouped.push(item),
        }
    }

    let mut out_groups: Vec<HistoryGroup> = Vec::new();
    for (k, mut records) in groups.into_iter() {
        records.sort_by(|a, b| b.ts_ms.cmp(&a.ts_ms));
        let latest_ts_ms = records.first().map(|r| r.ts_ms).unwrap_or(0);
        out_groups.push(HistoryGroup {
            deployment_group_key: k,
            latest_ts_ms,
            records,
        });
    }
    out_groups.sort_by(|a, b| b.latest_ts_ms.cmp(&a.latest_ts_ms));
    ungrouped.sort_by(|a, b| b.ts_ms.cmp(&a.ts_ms));

    let resp = HistoryGroupedResp {
        ok: true,
        err: None,
        groups: out_groups,
        ungrouped,
    };
    Ok(response_json(StatusCode::OK, &resp))
}

fn now_ts_ms() -> u64 {
    use std::time::SystemTime;
    let d = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    d.as_millis() as u64
}

fn sha256_hex(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

async fn write_history_record(dir: &Path, rec: &DeployHistoryRecord) -> anyhow::Result<()> {
    let tmp = dir.join(format!("{}.json.tmp", rec.id));
    let final_path = dir.join(format!("{}.json", rec.id));

    let buf = serde_json::to_vec(rec).context("encode history json")?;

    // Write then rename for atomic persistence.
    tokio::fs::write(&tmp, buf)
        .await
        .with_context(|| format!("write history tmp: {}", tmp.display()))?;

    tokio::fs::rename(&tmp, &final_path)
        .await
        .with_context(|| {
            format!(
                "rename history tmp: {} -> {}",
                tmp.display(),
                final_path.display()
            )
        })?;

    Ok(())
}

pub async fn run_controller_blocking(
    config_yaml: &str,
    workdir: &Path,
    fw_ready_tx: tokio::sync::oneshot::Sender<Arc<Framework>>,
) -> anyhow::Result<()> {
    let t0 = Instant::now();
    if config_yaml.trim().is_empty() {
        anyhow::bail!("config_yaml must be non-empty");
    }
    if !workdir.is_dir() {
        anyhow::bail!(
            "workdir must be an existing directory: {}",
            workdir.display()
        );
    }

    std::env::set_current_dir(workdir).context("set_current_dir")?;

    let cfg = parse_controller_config_yaml(config_yaml)?;
    let panel_cluster_name = cfg.kv_client.fluxonkv_spec.cluster_name.clone();

    let init = Arc::new(ControllerInitState {
        cfg: Arc::new(cfg),
        started_at: t0,
        stage: tokio::sync::RwLock::new("enter".to_string()),
        init_error: tokio::sync::Mutex::new(None),
        fw: tokio::sync::Mutex::new(None),
        runtime: tokio::sync::Mutex::new(None),
    });

    let shutdown_notify = Arc::new(tokio::sync::Notify::new());
    let shutdown_notify2 = shutdown_notify.clone();
    let init2 = init.clone();
    let workdir2 = workdir.to_path_buf();
    let panel_cluster_name2 = panel_cluster_name.clone();

    let init_task = tokio::spawn(async move {
        let init_start = init2.started_at;
        let elapsed_ms = || init_start.elapsed().as_millis();

        init2.set_stage("build_framework.begin").await;
        let (fw, client_cfg) = build_framework_from_kv_client_yaml(&init2.cfg.kv_client).await?;
        init2.set_stage("build_framework.done").await;

        {
            let mut g = init2.fw.lock().await;
            *g = Some(fw.clone());
        }

        // English note:
        // - fluxon_cli itself is not a P2P node; it needs an embedder-provided backend to execute
        //   p2p_rpc panel proxy requests.
        // - Ops controller runs ops_controller + fluxon_cli in the same process, so we expose
        //   this framework handle to the embedder via a oneshot channel.
        // - If the receiver is dropped, we keep running normally (no fallback behavior change).
        let _ = fw_ready_tx.send(fw.clone());
        eprintln!("[ops_controller:init] fw_ready sent");

        let history_dir = workdir2.join(OPS_HISTORY_DIR_NAME);
        init2.set_stage("create_history_dir.begin").await;
        tokio::fs::create_dir_all(&history_dir)
            .await
            .with_context(|| format!("create history dir: {}", history_dir.display()))?;
        init2.set_stage("create_history_dir.done").await;

        let desired_dir = workdir2.join(OPS_DESIRED_DIR_NAME);
        init2.set_stage("create_desired_dir.begin").await;
        tokio::fs::create_dir_all(&desired_dir)
            .await
            .with_context(|| format!("create desired dir: {}", desired_dir.display()))?;
        init2.set_stage("create_desired_dir.done").await;

        let desired = Arc::new(DesiredStore::load(desired_dir).await?);

        init2.set_stage("repair_apply_records.begin").await;
        controller_repair_missing_apply_records_from_history(desired.as_ref(), &history_dir)
            .await?;
        init2.set_stage("repair_apply_records.done").await;

        init2
            .set_stage("register_panel_proxy_rpc_handlers.begin")
            .await;
        register_controller_panel_proxy_userrpc_handler(&fw, init2.clone())?;
        init2
            .set_stage("register_panel_proxy_rpc_handlers.done")
            .await;

        init2.set_stage("publish_fluxon_cli_proxy_desc.begin").await;
        let panel_key = ops_fluxon_cli_proxy_desc_etcd_key(&panel_cluster_name2);
        let node_id = fw
            .cluster_manager_view()
            .cluster_manager()
            .get_self_info()
            .id
            .to_string();
        if node_id.trim().is_empty() {
            anyhow::bail!("invalid self node_id (empty) when publishing panel proxy descriptor");
        }
        let panel_desc = FluxonCliProxyDescriptorV2 {
            transport: FluxonCliProxyTransportV2::P2pRpc {
                node_id: node_id.clone(),
            },
            allow_prefixes: vec!["/".to_string()],
            html_inject: false,
        };
        let panel_desc_json = serde_json::to_vec(&panel_desc).unwrap();
        let etcd_endpoints =
            fluxon_kv::config::normalize_etcd_addresses(&client_cfg.etcd_addresses_raw)
                .map_err(|e| anyhow::anyhow!("normalize_etcd_addresses failed: {}", e))?;
        let mut client = etcd::Client::connect(etcd_endpoints, None)
            .await
            .with_context(|| {
                format!(
                    "connect etcd failed when publishing fluxon_cli proxy descriptor: key={} node_id={}",
                    panel_key, node_id
                )
            })?;
        client
            .put(panel_key.clone(), panel_desc_json, None)
            .await
            .with_context(|| {
                format!(
                    "publish fluxon_cli proxy descriptor to etcd: key={} node_id={}",
                    panel_key, node_id
                )
            })?;
        init2.set_stage("publish_fluxon_cli_proxy_desc.done").await;

        let state = Arc::new(ControllerHttpState {
            fw: fw.clone(),
            cfg: init2.cfg.clone(),
            history_dir,
            desired,
            deploy_guard: tokio::sync::Mutex::new(()),
        });
        user_rpc_register_handler(
            fw.p2p_view().p2p_module(),
            "fluxon_ops/agent_desired".to_string(),
            Arc::new(ControllerAgentDesiredHandler {
                desired: state.desired.clone(),
            }),
        );
        let reconcile_handle = tokio::spawn(controller_reconcile_loop(state.clone()));
        {
            let mut g = init2.runtime.lock().await;
            *g = Some(ControllerRuntime {
                state,
                reconcile_handle,
            });
        }
        init2.set_stage("ready").await;

        eprintln!(
            "[ops_controller:init] stage=ready elapsed_ms={}",
            elapsed_ms()
        );

        Ok::<(), anyhow::Error>(())
    });

    let init3 = init.clone();
    let shutdown_notify3 = shutdown_notify.clone();
    tokio::spawn(async move {
        match init_task.await {
            Ok(Ok(())) => {
                // Init succeeded; keep the server running.
            }
            Ok(Err(e)) => {
                eprintln!("[ops_controller:init] failed: {}", e);
                init3.set_stage("init_task.failed").await;
                init3.set_init_error(e.to_string()).await;
                shutdown_notify3.notify_one();
            }
            Err(e) => {
                let err = anyhow::anyhow!("controller init task join failed: {}", e);
                eprintln!("[ops_controller:init] join_failed: {}", err);
                init3.set_stage("init_task.join_failed").await;
                init3.set_init_error(err.to_string()).await;
                shutdown_notify3.notify_one();
            }
        }
    });

    eprintln!("[ops_controller] started (panel transport: p2p_rpc; no local http listen)");

    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
        let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;

        tokio::select! {
            _ = sigterm.recv() => {
                eprintln!("[ops_controller] shutdown: SIGTERM");
            }
            _ = sigint.recv() => {
                eprintln!("[ops_controller] shutdown: SIGINT");
            }
            _ = shutdown_notify2.notified() => {
                eprintln!("[ops_controller] shutdown: init_task_failed");
            }
        }
    }
    #[cfg(not(unix))]
    {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("[ops_controller] shutdown: ctrl_c");
            }
            _ = shutdown_notify2.notified() => {
                eprintln!("[ops_controller] shutdown: init_task_failed");
            }
        }
    }

    let runtime_opt = { init.runtime.lock().await.take() };
    match runtime_opt {
        Some(rt) => {
            rt.reconcile_handle.abort();
            let shutdown_res =
                tokio::time::timeout(std::time::Duration::from_secs(5), rt.state.fw.shutdown())
                    .await;
            match shutdown_res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    eprintln!("[ops_controller] framework shutdown failed: {}", e);
                }
                Err(_) => {
                    eprintln!(
                        "[ops_controller] framework shutdown timed out after 5s; exiting anyway to avoid supervisor deadlock"
                    );
                }
            }
        }
        None => {
            let fw_opt = init.fw.lock().await.take();
            if let Some(fw) = fw_opt {
                let _ =
                    tokio::time::timeout(std::time::Duration::from_secs(5), fw.shutdown()).await;
            }
        }
    }

    if let Some(e) = init.init_error.lock().await.clone() {
        anyhow::bail!("{}", e);
    }

    Ok(())
}

fn content_length_optional(headers: &hyper::HeaderMap) -> anyhow::Result<Option<u64>> {
    let Some(v) = headers.get(hyper::header::CONTENT_LENGTH) else {
        return Ok(None);
    };
    let s = v
        .to_str()
        .map_err(|e| anyhow::anyhow!("invalid Content-Length header: {}", e))?;
    let n: u64 = s
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid Content-Length value: {}", e))?;
    Ok(Some(n))
}

async fn read_body_to_bytes_limited(mut body: Body, max_bytes: u64) -> anyhow::Result<Vec<u8>> {
    let cap = usize::try_from(max_bytes).unwrap_or(0);
    let mut out: Vec<u8> = Vec::with_capacity(std::cmp::min(cap, 1024 * 1024));
    let mut total: u64 = 0;

    while let Some(next) = body.data().await {
        let chunk = next.map_err(|e| anyhow::anyhow!("read http body chunk failed: {}", e))?;
        total += chunk.len() as u64;
        if total > max_bytes {
            anyhow::bail!("request body exceeds max_bytes: {} > {}", total, max_bytes);
        }
        out.extend_from_slice(chunk.as_ref());
    }

    Ok(out)
}

// English note: ops_controller panel requests must be bounded to avoid unbounded memory usage.
// The bound is explicitly configured via panel.max_body_bytes (no implicit default).

#[derive(Debug, Clone)]
struct DeploySpec {
    kind: WorkloadKind,
    name: String,
    logical_selection: String,
    service_name: String,
    atomic_group: Option<AtomicGroupMeta>,
    namespace: Option<String>,
    targets: Vec<String>,
    exec_argv: Vec<String>,
    exec_cwd: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct K8sDeploymentYaml {
    #[serde(rename = "apiVersion")]
    api_version: String,
    kind: String,
    metadata: K8sObjectMetaYaml,
    spec: K8sDeploymentSpecYaml,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct K8sObjectMetaYaml {
    name: String,
    #[serde(default)]
    annotations: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct K8sDeploymentSpecYaml {
    template: K8sPodTemplateYaml,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct K8sPodTemplateYaml {
    spec: K8sPodSpecYaml,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct K8sPodSpecYaml {
    containers: Vec<K8sContainerYaml>,
    affinity: Option<K8sAffinityYaml>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct K8sAffinityYaml {
    #[serde(rename = "nodeAffinity")]
    node_affinity: Option<K8sNodeAffinityYaml>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct K8sNodeAffinityYaml {
    #[serde(rename = "requiredDuringSchedulingIgnoredDuringExecution")]
    required: Option<K8sNodeSelectorYaml>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct K8sNodeSelectorYaml {
    #[serde(rename = "nodeSelectorTerms")]
    node_selector_terms: Vec<K8sNodeSelectorTermYaml>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct K8sNodeSelectorTermYaml {
    #[serde(rename = "matchExpressions")]
    match_expressions: Vec<K8sNodeSelectorRequirementYaml>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct K8sNodeSelectorRequirementYaml {
    key: String,
    operator: String,
    values: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct K8sContainerYaml {
    name: String,
    image: String,
    command: Option<Vec<String>>,
    args: Option<Vec<String>>,
    #[serde(rename = "workingDir")]
    working_dir: Option<String>,
}

fn parse_namespace_annotation_value(raw: &str) -> anyhow::Result<String> {
    let v = raw.trim();
    if v.is_empty() {
        anyhow::bail!(
            "{} annotation must be non-empty when provided",
            OPS_NAMESPACE_ANNOTATION_KEY
        );
    }
    for ch in v.chars() {
        let ok = ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.';
        if !ok {
            anyhow::bail!(
                "{} annotation contains unsupported character: {:?}",
                OPS_NAMESPACE_ANNOTATION_KEY,
                ch
            );
        }
    }
    Ok(v.to_string())
}

fn namespace_from_annotations(
    annotations: &BTreeMap<String, String>,
) -> anyhow::Result<Option<String>> {
    match annotations.get(OPS_NAMESPACE_ANNOTATION_KEY) {
        None => Ok(None),
        Some(v) => Ok(Some(parse_namespace_annotation_value(v)?)),
    }
}

fn parse_non_empty_annotation(raw: &str, key: &str) -> anyhow::Result<String> {
    let v = raw.trim();
    if v.is_empty() {
        anyhow::bail!("{} annotation must be non-empty when provided", key);
    }
    Ok(v.to_string())
}

fn parse_u64_annotation(raw: &str, key: &str) -> anyhow::Result<u64> {
    let v = parse_non_empty_annotation(raw, key)?;
    v.parse::<u64>()
        .map_err(|e| anyhow::anyhow!("{} annotation must be an unsigned integer: {}", key, e))
}

fn logical_selection_from_annotations(
    annotations: &BTreeMap<String, String>,
    workload_name: &str,
) -> anyhow::Result<String> {
    match annotations.get(OPS_LOGICAL_SELECTION_ANNOTATION_KEY) {
        None => Ok(workload_name.to_string()),
        Some(v) => parse_non_empty_annotation(v, OPS_LOGICAL_SELECTION_ANNOTATION_KEY),
    }
}

fn service_name_from_annotations(
    annotations: &BTreeMap<String, String>,
    logical_selection: &str,
) -> anyhow::Result<String> {
    match annotations.get(OPS_SERVICE_NAME_ANNOTATION_KEY) {
        None => Ok(logical_selection.to_string()),
        Some(v) => parse_non_empty_annotation(v, OPS_SERVICE_NAME_ANNOTATION_KEY),
    }
}

fn atomic_group_from_annotations(
    annotations: &BTreeMap<String, String>,
    logical_selection: &str,
) -> anyhow::Result<Option<AtomicGroupMeta>> {
    let group_name = match annotations.get(OPS_ATOMIC_GROUP_ANNOTATION_KEY) {
        None => return Ok(None),
        Some(v) => parse_non_empty_annotation(v, OPS_ATOMIC_GROUP_ANNOTATION_KEY)?,
    };
    if group_name != logical_selection {
        anyhow::bail!(
            "{} annotation must match {} when provided: group_name={} logical_selection={}",
            OPS_ATOMIC_GROUP_ANNOTATION_KEY,
            OPS_LOGICAL_SELECTION_ANNOTATION_KEY,
            group_name,
            logical_selection
        );
    }
    let phase_raw = annotations
        .get(OPS_ATOMIC_GROUP_PHASE_ANNOTATION_KEY)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "missing {} annotation",
                OPS_ATOMIC_GROUP_PHASE_ANNOTATION_KEY
            )
        })?;
    let order_raw = annotations
        .get(OPS_ATOMIC_GROUP_ORDER_ANNOTATION_KEY)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "missing {} annotation",
                OPS_ATOMIC_GROUP_ORDER_ANNOTATION_KEY
            )
        })?;
    Ok(Some(AtomicGroupMeta {
        selection_name: logical_selection.to_string(),
        phase: parse_u64_annotation(phase_raw, OPS_ATOMIC_GROUP_PHASE_ANNOTATION_KEY)?,
        order: parse_u64_annotation(order_raw, OPS_ATOMIC_GROUP_ORDER_ANNOTATION_KEY)?,
    }))
}

fn namespace_from_specs(specs: &[DeploySpec]) -> anyhow::Result<Option<String>> {
    let mut namespace: Option<String> = None;
    for spec in specs.iter() {
        match (&namespace, &spec.namespace) {
            (None, Some(v)) => namespace = Some(v.clone()),
            (Some(existing), Some(v)) if existing != v => {
                anyhow::bail!(
                    "all YAML documents in one deploy request must share the same {} annotation: expected={} got={} workload={}",
                    OPS_NAMESPACE_ANNOTATION_KEY,
                    existing,
                    v,
                    spec.name
                );
            }
            _ => {}
        }
    }
    Ok(namespace)
}

fn desired_workloads_from_specs(
    specs: &[DeploySpec],
    apply_id: &str,
    updated_ts_ms: u64,
) -> Vec<DesiredWorkload> {
    specs
        .iter()
        .map(|spec| DesiredWorkload {
            kind: spec.kind,
            name: spec.name.clone(),
            logical_selection: spec.logical_selection.clone(),
            service_name: spec.service_name.clone(),
            atomic_group: spec.atomic_group.clone(),
            namespace: spec.namespace.clone(),
            targets: spec.targets.clone(),
            apply_id: Some(apply_id.to_string()),
            exec_argv: spec.exec_argv.clone(),
            exec_cwd: spec.exec_cwd.clone(),
            updated_ts_ms,
        })
        .collect()
}

fn desired_workloads_from_apply_record(
    rec: &DeployApplyRecord,
) -> anyhow::Result<Vec<DesiredWorkload>> {
    let specs = parse_k8s_deployment_subset_documents(&rec.deployment_yaml).with_context(|| {
        format!(
            "parse desired workloads from apply record deployment_yaml failed: apply_id={}",
            rec.id
        )
    })?;
    Ok(desired_workloads_from_specs(
        specs.as_slice(),
        &rec.id,
        rec.ts_ms,
    ))
}

fn parse_k8s_deployment_subset_one(deploy: K8sDeploymentYaml) -> anyhow::Result<DeploySpec> {
    if deploy.api_version.trim() != "apps/v1" {
        anyhow::bail!("apiVersion must be 'apps/v1'");
    }
    let kind = match deploy.kind.trim() {
        "Deployment" => WorkloadKind::Deployment,
        "DaemonSet" => WorkloadKind::DaemonSet,
        _ => {
            anyhow::bail!("kind must be 'Deployment' or 'DaemonSet'");
        }
    };

    let name = deploy.metadata.name.trim().to_string();
    if name.is_empty() {
        anyhow::bail!("metadata.name must be non-empty");
    }
    let namespace = namespace_from_annotations(&deploy.metadata.annotations)?;
    let logical_selection =
        logical_selection_from_annotations(&deploy.metadata.annotations, &name)?;
    let service_name =
        service_name_from_annotations(&deploy.metadata.annotations, &logical_selection)?;
    let atomic_group =
        atomic_group_from_annotations(&deploy.metadata.annotations, &logical_selection)?;
    if kind == WorkloadKind::DaemonSet {
        validate_daemonset_workload_name_matches_selection_contract(
            &name,
            &logical_selection,
            &service_name,
            atomic_group.is_some(),
        )
        .with_context(|| {
            format!(
                "validate daemonset metadata.name against shared selection naming contract: workload_name={}",
                name
            )
        })?;
    }

    let containers = deploy.spec.template.spec.containers;
    if containers.len() != 1 {
        anyhow::bail!(
            "spec.template.spec.containers must have exactly 1 item (temporarily not supported)"
        );
    }
    let c = &containers[0];

    if c.name.trim().is_empty() {
        anyhow::bail!("containers[0].name must be non-empty");
    }
    if c.image.trim().is_empty() {
        anyhow::bail!("containers[0].image must be non-empty");
    }

    let Some(cmd) = c.command.as_ref() else {
        anyhow::bail!("containers[0].command is required");
    };
    if cmd.is_empty() {
        anyhow::bail!("containers[0].command must be non-empty");
    }

    let mut exec_argv: Vec<String> = Vec::new();
    for (i, a) in cmd.iter().enumerate() {
        if a.trim().is_empty() {
            anyhow::bail!("containers[0].command[{i}] must be non-empty");
        }
        exec_argv.push(a.to_string());
    }
    if let Some(args) = c.args.as_ref() {
        for (i, a) in args.iter().enumerate() {
            if a.trim().is_empty() {
                anyhow::bail!("containers[0].args[{i}] must be non-empty");
            }
            exec_argv.push(a.to_string());
        }
    }

    let exec_cwd = match c.working_dir.as_deref() {
        None => None,
        Some(s) => {
            let v = s.trim();
            if v.is_empty() {
                anyhow::bail!("containers[0].workingDir must be non-empty when provided");
            }
            Some(v.to_string())
        }
    };

    let affinity = deploy
        .spec
        .template
        .spec
        .affinity
        .ok_or_else(|| anyhow::anyhow!("missing spec.template.spec.affinity"))?;
    let node_affinity = affinity
        .node_affinity
        .ok_or_else(|| anyhow::anyhow!("missing spec.template.spec.affinity.nodeAffinity"))?;
    let required = node_affinity.required.ok_or_else(|| {
        anyhow::anyhow!(
            "missing spec.template.spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution"
        )
    })?;

    if required.node_selector_terms.len() != 1 {
        anyhow::bail!("only one nodeSelectorTerms item is supported");
    }
    let term = &required.node_selector_terms[0];
    if term.match_expressions.len() != 1 {
        anyhow::bail!("only one matchExpressions item is supported");
    }
    let expr = &term.match_expressions[0];

    if expr.key.trim() != "kubernetes.io/hostname" {
        anyhow::bail!("only nodeAffinity key 'kubernetes.io/hostname' is supported");
    }
    if expr.operator.trim() != "In" {
        anyhow::bail!("only nodeAffinity operator 'In' is supported");
    }
    if expr.values.is_empty() {
        anyhow::bail!("nodeAffinity values must be non-empty");
    }

    let mut seen = HashSet::new();
    let mut targets: Vec<String> = Vec::new();
    for n in expr.values.iter() {
        let nn = n.trim();
        if nn.is_empty() {
            anyhow::bail!("nodeAffinity values elements must be non-empty strings");
        }
        if seen.insert(nn.to_string()) {
            targets.push(nn.to_string());
        }
    }

    Ok(DeploySpec {
        kind,
        name,
        logical_selection,
        service_name,
        atomic_group,
        namespace,
        targets,
        exec_argv,
        exec_cwd,
    })
}

fn parse_k8s_deployment_subset_documents(doc: &str) -> anyhow::Result<Vec<DeploySpec>> {
    let mut specs: Vec<DeploySpec> = Vec::new();
    let mut idx: usize = 0;
    for d in serde_yaml::Deserializer::from_str(doc) {
        let deploy: K8sDeploymentYaml = K8sDeploymentYaml::deserialize(d).map_err(|e| {
            anyhow::anyhow!(
                "parse YAML document[{idx}] failed (only a subset of apps/v1 Deployment/DaemonSet is supported): {}",
                e
            )
        })?;
        let spec = parse_k8s_deployment_subset_one(deploy)
            .with_context(|| format!("validate YAML document[{idx}]"))?;
        specs.push(spec);
        idx += 1;
    }
    if specs.is_empty() {
        anyhow::bail!("no YAML document found");
    }
    Ok(specs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeSet, VecDeque};

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum HandoverVisibleOwnerKind {
        Absent,
        Applyless,
        Applied,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum HandoverVisiblePhase {
        Absent,
        Attached,
        Present,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum HandoverWaitMode {
        WaitAttached,
        WaitPresent,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum HandoverWaitFailure {
        AttachedTimeout,
        PresentTimeout,
        ObserveIdentityDrift,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum HandoverRollbackPolicy {
        NoDestructiveRollback,
        StopRequestedApply,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct HandoverModelState {
        visible_owner_kind: HandoverVisibleOwnerKind,
        visible_owner_ts: u8,
        visible_apply_present: bool,
        visible_apply_matches_request: bool,
        visible_phase: HandoverVisiblePhase,
        request_owner_ts: u8,
        wait_mode: HandoverWaitMode,
        wait_failure: Option<HandoverWaitFailure>,
        rollback_policy: HandoverRollbackPolicy,
        requested_apply_alive: bool,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum WaitFailureCleanupFailureKind {
        WaitAttached,
        WaitPresent,
        ObserveFailed,
        IdentityDrift,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct WaitFailureCleanupModelState {
        requested_apply_submitted: bool,
        visible_owner_is_requested: bool,
        visible_phase: HandoverVisiblePhase,
        failure_kind: Option<WaitFailureCleanupFailureKind>,
        cleanup_policy: HandoverRollbackPolicy,
        requested_apply_alive: bool,
    }

    impl HandoverModelState {
        fn initial_states() -> Vec<Self> {
            let mut out = Vec::new();
            for wait_mode in [
                HandoverWaitMode::WaitAttached,
                HandoverWaitMode::WaitPresent,
            ] {
                for rollback_policy in [
                    HandoverRollbackPolicy::NoDestructiveRollback,
                    HandoverRollbackPolicy::StopRequestedApply,
                ] {
                    out.push(Self {
                        visible_owner_kind: HandoverVisibleOwnerKind::Applyless,
                        visible_owner_ts: 2,
                        visible_apply_present: false,
                        visible_apply_matches_request: false,
                        visible_phase: HandoverVisiblePhase::Attached,
                        request_owner_ts: 1,
                        wait_mode,
                        wait_failure: None,
                        rollback_policy,
                        requested_apply_alive: true,
                    });
                }
            }
            out
        }

        fn invariants_hold(&self) -> bool {
            if self.visible_phase == HandoverVisiblePhase::Present
                && self.visible_owner_kind != HandoverVisibleOwnerKind::Applied
            {
                return false;
            }
            if self.visible_owner_kind == HandoverVisibleOwnerKind::Applied
                && !self.visible_apply_present
            {
                return false;
            }
            if self.rollback_policy == HandoverRollbackPolicy::NoDestructiveRollback
                && self.wait_failure.is_some()
                && !self.requested_apply_alive
            {
                return false;
            }
            if self.compatible_applyless_takeover() && !self.requested_apply_alive {
                return false;
            }
            true
        }

        fn compatible_applyless_takeover(&self) -> bool {
            self.visible_owner_kind == HandoverVisibleOwnerKind::Applyless
                && !self.visible_apply_present
                && self.visible_owner_ts > self.request_owner_ts
        }

        fn next_states(&self) -> Vec<Self> {
            let mut out = Vec::new();

            if self.compatible_applyless_takeover() {
                let mut next = *self;
                next.visible_owner_kind = HandoverVisibleOwnerKind::Applied;
                next.visible_apply_present = true;
                next.visible_apply_matches_request = true;
                next.visible_phase = HandoverVisiblePhase::Attached;
                out.push(next);
            }

            if self.visible_owner_kind == HandoverVisibleOwnerKind::Applied
                && self.visible_apply_present
                && self.visible_apply_matches_request
                && self.visible_phase == HandoverVisiblePhase::Attached
            {
                if self.wait_mode == HandoverWaitMode::WaitPresent {
                    let mut next = *self;
                    next.visible_phase = HandoverVisiblePhase::Present;
                    out.push(next);
                } else {
                    out.push(*self);
                }
            }

            if self.visible_owner_kind == HandoverVisibleOwnerKind::Applied
                && self.visible_apply_present
                && self.visible_apply_matches_request
                && self.wait_failure.is_none()
            {
                for wait_failure in [
                    HandoverWaitFailure::AttachedTimeout,
                    HandoverWaitFailure::PresentTimeout,
                    HandoverWaitFailure::ObserveIdentityDrift,
                ] {
                    let mut next = *self;
                    next.wait_failure = Some(wait_failure);
                    if self.rollback_policy == HandoverRollbackPolicy::StopRequestedApply {
                        next.requested_apply_alive = false;
                        next.visible_owner_kind = HandoverVisibleOwnerKind::Absent;
                        next.visible_owner_ts = 0;
                        next.visible_apply_present = false;
                        next.visible_apply_matches_request = false;
                        next.visible_phase = HandoverVisiblePhase::Absent;
                    }
                    out.push(next);
                }
            }

            if self.visible_owner_kind == HandoverVisibleOwnerKind::Applied
                && self.visible_owner_ts > self.request_owner_ts
                && !self.visible_apply_matches_request
            {
                out.push(*self);
            }

            out
        }
    }

    #[test]
    fn single_label_handover_model_bounded_state_space_preserves_invariants() {
        let mut queue: VecDeque<HandoverModelState> =
            VecDeque::from(HandoverModelState::initial_states());
        let mut seen: BTreeSet<HandoverModelState> = BTreeSet::new();

        while let Some(state) = queue.pop_front() {
            if !seen.insert(state) {
                continue;
            }
            assert!(
                state.invariants_hold(),
                "handover model invariant failed for state: {:?}",
                state
            );
            for next in state.next_states() {
                queue.push_back(next);
            }
        }

        assert!(
            seen.iter().any(|state| {
                state.visible_owner_kind == HandoverVisibleOwnerKind::Applied
                    && state.visible_apply_matches_request
                    && state.requested_apply_alive
            }),
            "handover model should admit a successful applyless -> applied takeover path"
        );
    }

    #[test]
    fn single_label_handover_model_exposes_destructive_wait_failure_rollback() {
        let initial = HandoverModelState {
            visible_owner_kind: HandoverVisibleOwnerKind::Applied,
            visible_owner_ts: 2,
            visible_apply_present: true,
            visible_apply_matches_request: true,
            visible_phase: HandoverVisiblePhase::Attached,
            request_owner_ts: 2,
            wait_mode: HandoverWaitMode::WaitPresent,
            wait_failure: None,
            rollback_policy: HandoverRollbackPolicy::StopRequestedApply,
            requested_apply_alive: true,
        };
        let next_states = initial.next_states();
        assert!(
            next_states.iter().any(|state| {
                state.wait_failure.is_some()
                    && !state.requested_apply_alive
                    && state.visible_owner_kind == HandoverVisibleOwnerKind::Absent
            }),
            "destructive rollback policy should make the self-kill transition explicit in the model"
        );
    }

    #[test]
    fn single_label_handover_model_keeps_requested_apply_alive_under_safe_wait_failure() {
        let initial = HandoverModelState {
            visible_owner_kind: HandoverVisibleOwnerKind::Applied,
            visible_owner_ts: 2,
            visible_apply_present: true,
            visible_apply_matches_request: true,
            visible_phase: HandoverVisiblePhase::Attached,
            request_owner_ts: 2,
            wait_mode: HandoverWaitMode::WaitPresent,
            wait_failure: None,
            rollback_policy: HandoverRollbackPolicy::NoDestructiveRollback,
            requested_apply_alive: true,
        };
        let next_states = initial.next_states();
        assert!(
            next_states
                .iter()
                .all(|state| { state.wait_failure.is_none() || state.requested_apply_alive }),
            "safe rollback policy must not kill the requested apply on wait failure"
        );
    }

    impl WaitFailureCleanupModelState {
        fn initial_states() -> Vec<Self> {
            let mut out = Vec::new();
            for visible_phase in [
                HandoverVisiblePhase::Attached,
                HandoverVisiblePhase::Present,
            ] {
                for cleanup_policy in [
                    HandoverRollbackPolicy::NoDestructiveRollback,
                    HandoverRollbackPolicy::StopRequestedApply,
                ] {
                    out.push(Self {
                        requested_apply_submitted: true,
                        visible_owner_is_requested: true,
                        visible_phase,
                        failure_kind: None,
                        cleanup_policy,
                        requested_apply_alive: true,
                    });
                }
            }
            out
        }

        fn invariants_hold(&self) -> bool {
            if self.cleanup_policy == HandoverRollbackPolicy::NoDestructiveRollback
                && self.failure_kind.is_some()
                && !self.requested_apply_alive
            {
                return false;
            }
            if self.requested_apply_submitted
                && self.cleanup_policy == HandoverRollbackPolicy::NoDestructiveRollback
                && self.failure_kind.is_some()
                && !self.requested_apply_alive
            {
                return false;
            }
            if self.visible_owner_is_requested
                && self.visible_phase == HandoverVisiblePhase::Present
                && self.failure_kind.is_some()
                && self.cleanup_policy == HandoverRollbackPolicy::NoDestructiveRollback
                && !self.requested_apply_alive
            {
                return false;
            }
            true
        }

        fn next_states(&self) -> Vec<Self> {
            let mut out = Vec::new();
            if self.failure_kind.is_none() {
                for failure_kind in [
                    WaitFailureCleanupFailureKind::WaitAttached,
                    WaitFailureCleanupFailureKind::WaitPresent,
                    WaitFailureCleanupFailureKind::ObserveFailed,
                    WaitFailureCleanupFailureKind::IdentityDrift,
                ] {
                    let mut next = *self;
                    next.failure_kind = Some(failure_kind);
                    out.push(next);
                }
            }
            if self.failure_kind.is_some() {
                let mut next = *self;
                if self.cleanup_policy == HandoverRollbackPolicy::StopRequestedApply {
                    next.requested_apply_alive = false;
                    next.visible_owner_is_requested = false;
                    next.visible_phase = HandoverVisiblePhase::Absent;
                }
                out.push(next);
            }
            if self.visible_owner_is_requested
                && self.visible_phase == HandoverVisiblePhase::Attached
                && self.failure_kind.is_none()
            {
                let mut next = *self;
                next.visible_phase = HandoverVisiblePhase::Present;
                out.push(next);
            }
            out
        }
    }

    #[test]
    fn wait_failure_cleanup_model_bounded_state_space_preserves_invariants() {
        let mut queue: VecDeque<WaitFailureCleanupModelState> =
            VecDeque::from(WaitFailureCleanupModelState::initial_states());
        let mut seen: BTreeSet<WaitFailureCleanupModelState> = BTreeSet::new();

        while let Some(state) = queue.pop_front() {
            if !seen.insert(state) {
                continue;
            }
            assert!(
                state.invariants_hold(),
                "wait-failure cleanup model invariant failed for state: {:?}",
                state
            );
            for next in state.next_states() {
                queue.push_back(next);
            }
        }

        assert!(
            seen.iter().any(|state| {
                state.failure_kind.is_some()
                    && state.cleanup_policy == HandoverRollbackPolicy::StopRequestedApply
                    && !state.requested_apply_alive
            }),
            "wait-failure cleanup model should expose the destructive cleanup transition"
        );
    }

    #[test]
    fn wait_failure_cleanup_model_safe_policy_preserves_requested_apply() {
        let initial = WaitFailureCleanupModelState {
            requested_apply_submitted: true,
            visible_owner_is_requested: true,
            visible_phase: HandoverVisiblePhase::Present,
            failure_kind: Some(WaitFailureCleanupFailureKind::WaitPresent),
            cleanup_policy: HandoverRollbackPolicy::NoDestructiveRollback,
            requested_apply_alive: true,
        };
        assert!(initial.invariants_hold());
        let next_states = initial.next_states();
        assert!(
            next_states.iter().all(|state| state.requested_apply_alive),
            "safe cleanup policy must preserve the requested apply after wait failure"
        );
    }

    #[test]
    fn daemonset_supervisor_label_uses_workload_name_identity() {
        let plain = selection_supervisor_label_from_workload_name(
            WorkloadKind::DaemonSet,
            "fluxon-bench-n3-closed-20260428-bastion-bootstrap-fluxon_fs_agent",
        )
        .unwrap();
        assert_eq!(
            plain,
            "DaemonSet/fluxon-bench-n3-closed-20260428-bastion-bootstrap-fluxon_fs_agent"
        );

        let grouped = selection_supervisor_label_from_workload_name(
            WorkloadKind::DaemonSet,
            "fluxon-bench-n3-closed-20260428-bastion-bootstrap-fluxon_core_controller__ops_controller",
        )
        .unwrap();
        assert_eq!(
            grouped,
            "DaemonSet/fluxon-bench-n3-closed-20260428-bastion-bootstrap-fluxon_core_controller__ops_controller"
        );
    }

    #[test]
    fn parse_selection_supervisor_state_json_accepts_bare_runtime_without_apply_id() {
        let raw = serde_json::json!({
            "kind": WorkloadKind::Deployment,
            "name": "bare-owner",
            "authority": "bare-owner",
            "service_name": "owner",
            "argv": ["/usr/bin/env", "bash", "-lc", "sleep 30"],
            "cwd": "/tmp/fluxon",
            "log_path": "/tmp/fluxon/owner.log"
        })
        .to_string();
        let state = parse_selection_supervisor_state_json(raw.as_str()).unwrap();
        assert_eq!(state.kind, WorkloadKind::Deployment);
        assert_eq!(state.name, "bare-owner");
        assert_eq!(state.service_name, "owner");
        assert_eq!(state.apply_id, None);
        assert_eq!(
            state.argv,
            vec![
                "/usr/bin/env".to_string(),
                "bash".to_string(),
                "-lc".to_string(),
                "sleep 30".to_string()
            ]
        );
        assert_eq!(state.cwd.as_deref(), Some("/tmp/fluxon"));
        assert_eq!(state.log_path, "/tmp/fluxon/owner.log");
    }

    #[test]
    fn live_selection_supervisors_skip_unrelated_legacy_entries_in_list_mode() {
        let valid_state = serde_json::json!({
            "kind": WorkloadKind::Deployment,
            "name": "target",
            "authority": "target",
            "service_name": "target",
            "apply_id": "apply-1",
            "argv": ["/usr/bin/python3", "-c", "print(1)"],
            "log_path": "/tmp/target.log"
        })
        .to_string();
        let snapshot = SelectionSupervisorProcSnapshot {
            infos_by_pid: std::collections::HashMap::from([
                (
                    11,
                    ProcessInfoObservation {
                        pid: 11,
                        ppid: 1,
                        pgid: 11,
                        state: 'S',
                        start_time_ticks: 100,
                    },
                ),
                (
                    22,
                    ProcessInfoObservation {
                        pid: 22,
                        ppid: 1,
                        pgid: 22,
                        state: 'S',
                        start_time_ticks: 200,
                    },
                ),
            ]),
            children_by_ppid: std::collections::HashMap::new(),
            cmdlines: vec![
                (
                    11,
                    vec![
                        "/usr/bin/python3".to_string(),
                        "selection_supervisor.py".to_string(),
                        "run".to_string(),
                        "--label".to_string(),
                        "Deployment/target".to_string(),
                        "--state-json".to_string(),
                        valid_state,
                        "--owner-ts-ms".to_string(),
                        "1".to_string(),
                    ],
                ),
                (
                    22,
                    vec![
                        "/usr/bin/python3".to_string(),
                        "selection_supervisor.py".to_string(),
                        "run".to_string(),
                        "--label".to_string(),
                        "Deployment/legacy".to_string(),
                        "--state-json".to_string(),
                        "{\"kind\":\"Deployment\",\"name\":\"legacy\",\"authority\":\"legacy\",\"service_name\":\"legacy\",\"argv\":[\"/bin/sleep\",\"1\"],\"log_path\":\"/tmp/legacy.log\"}".to_string(),
                    ],
                ),
            ],
            zombie_infos: Vec::new(),
        };

        let supervisors = live_selection_supervisors(&snapshot, None, None).unwrap();
        assert_eq!(supervisors.len(), 1);
        assert_eq!(supervisors[0].label, "Deployment/target");
        assert_eq!(supervisors[0].owner_ts_ms, 1);
        assert_eq!(
            supervisors[0]
                .runtime_state
                .as_ref()
                .and_then(|v| v.apply_id.as_deref()),
            Some("apply-1")
        );
    }

    #[test]
    fn live_selection_supervisors_owner_ts_ms_decides_owner_even_without_runtime_state() {
        let valid_state = serde_json::json!({
            "kind": WorkloadKind::Deployment,
            "name": "target",
            "authority": "target",
            "service_name": "target",
            "apply_id": "apply-1",
            "argv": ["/usr/bin/python3", "-c", "print(1)"],
            "log_path": "/tmp/target.log"
        })
        .to_string();
        let snapshot = SelectionSupervisorProcSnapshot {
            infos_by_pid: std::collections::HashMap::from([
                (
                    11,
                    ProcessInfoObservation {
                        pid: 11,
                        ppid: 1,
                        pgid: 11,
                        state: 'S',
                        start_time_ticks: 100,
                    },
                ),
                (
                    22,
                    ProcessInfoObservation {
                        pid: 22,
                        ppid: 1,
                        pgid: 22,
                        state: 'S',
                        start_time_ticks: 200,
                    },
                ),
            ]),
            children_by_ppid: std::collections::HashMap::new(),
            cmdlines: vec![
                (
                    11,
                    vec![
                        "/usr/bin/python3".to_string(),
                        "selection_supervisor.py".to_string(),
                        "run".to_string(),
                        "--label".to_string(),
                        "Deployment/target".to_string(),
                        "--state-json".to_string(),
                        valid_state,
                        "--owner-ts-ms".to_string(),
                        "1".to_string(),
                    ],
                ),
                (
                    22,
                    vec![
                        "/usr/bin/python3".to_string(),
                        "selection_supervisor.py".to_string(),
                        "run".to_string(),
                        "--label".to_string(),
                        "Deployment/target".to_string(),
                        "--owner-ts-ms".to_string(),
                        "2".to_string(),
                    ],
                ),
            ],
            zombie_infos: Vec::new(),
        };

        let listed = observe_all_selection_statuses_for_snapshot(&snapshot, None).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].kind, Some(WorkloadKind::Deployment));
        assert_eq!(listed[0].name.as_deref(), Some("target"));
        assert_eq!(listed[0].apply_id.as_deref(), None);
        assert_eq!(listed[0].owner_ts_ms, Some(2));

        let strict =
            live_selection_supervisors(&snapshot, Some("Deployment/target"), None).unwrap();
        assert_eq!(strict.len(), 2);
        assert!(
            strict
                .iter()
                .any(|entry| entry.owner_ts_ms == 2 && entry.runtime_state.is_none())
        );
    }

    #[test]
    fn live_selection_supervisors_reject_owner_ts_ms_collision_for_same_label() {
        let snapshot = SelectionSupervisorProcSnapshot {
            infos_by_pid: std::collections::HashMap::from([
                (
                    11,
                    ProcessInfoObservation {
                        pid: 11,
                        ppid: 1,
                        pgid: 11,
                        state: 'S',
                        start_time_ticks: 100,
                    },
                ),
                (
                    22,
                    ProcessInfoObservation {
                        pid: 22,
                        ppid: 1,
                        pgid: 22,
                        state: 'S',
                        start_time_ticks: 200,
                    },
                ),
            ]),
            children_by_ppid: std::collections::HashMap::new(),
            cmdlines: vec![
                (
                    11,
                    vec![
                        "/usr/bin/python3".to_string(),
                        "selection_supervisor.py".to_string(),
                        "run".to_string(),
                        "--label".to_string(),
                        "Deployment/target".to_string(),
                        "--owner-ts-ms".to_string(),
                        "2".to_string(),
                    ],
                ),
                (
                    22,
                    vec![
                        "/usr/bin/python3".to_string(),
                        "selection_supervisor.py".to_string(),
                        "run".to_string(),
                        "--label".to_string(),
                        "Deployment/target".to_string(),
                        "--owner-ts-ms".to_string(),
                        "2".to_string(),
                    ],
                ),
            ],
            zombie_infos: Vec::new(),
        };

        let err = observe_all_selection_statuses_for_snapshot(&snapshot, None).unwrap_err();
        let err_text = format!("{:#}", err);
        assert!(err_text.contains("owner_ts_ms collision"), "{err_text}");
    }

    #[test]
    fn live_selection_supervisors_isolate_same_label_collision_by_scope_key() {
        let snapshot = SelectionSupervisorProcSnapshot {
            infos_by_pid: std::collections::HashMap::from([
                (
                    11,
                    ProcessInfoObservation {
                        pid: 11,
                        ppid: 1,
                        pgid: 11,
                        state: 'S',
                        start_time_ticks: 100,
                    },
                ),
                (
                    22,
                    ProcessInfoObservation {
                        pid: 22,
                        ppid: 1,
                        pgid: 22,
                        state: 'S',
                        start_time_ticks: 200,
                    },
                ),
            ]),
            children_by_ppid: std::collections::HashMap::new(),
            cmdlines: vec![
                (
                    11,
                    vec![
                        "/usr/bin/python3".to_string(),
                        "selection_supervisor.py".to_string(),
                        "run".to_string(),
                        "--label".to_string(),
                        "DaemonSet/target".to_string(),
                        "--scope-key".to_string(),
                        "/tmp/scope-a".to_string(),
                        "--owner-ts-ms".to_string(),
                        "2".to_string(),
                    ],
                ),
                (
                    22,
                    vec![
                        "/usr/bin/python3".to_string(),
                        "selection_supervisor.py".to_string(),
                        "run".to_string(),
                        "--label".to_string(),
                        "DaemonSet/target".to_string(),
                        "--scope-key".to_string(),
                        "/tmp/scope-b".to_string(),
                        "--owner-ts-ms".to_string(),
                        "2".to_string(),
                    ],
                ),
            ],
            zombie_infos: Vec::new(),
        };

        let scoped_a =
            live_selection_supervisors(&snapshot, Some("DaemonSet/target"), Some("/tmp/scope-a"))
                .unwrap();
        assert_eq!(scoped_a.len(), 1);
        assert_eq!(scoped_a[0].pid(), 11);

        let scoped_b =
            live_selection_supervisors(&snapshot, Some("DaemonSet/target"), Some("/tmp/scope-b"))
                .unwrap();
        assert_eq!(scoped_b.len(), 1);
        assert_eq!(scoped_b[0].pid(), 22);

        let listed_a = observe_all_selection_statuses_for_snapshot(&snapshot, Some("/tmp/scope-a"))
            .unwrap();
        assert_eq!(listed_a.len(), 1);
        assert_eq!(listed_a[0].label, "DaemonSet/target");
        assert_eq!(listed_a[0].pid, Some(11));

        let listed_b = observe_all_selection_statuses_for_snapshot(&snapshot, Some("/tmp/scope-b"))
            .unwrap();
        assert_eq!(listed_b.len(), 1);
        assert_eq!(listed_b[0].label, "DaemonSet/target");
        assert_eq!(listed_b[0].pid, Some(22));
    }

    #[test]
    fn live_selection_supervisors_reject_matching_legacy_entry_without_owner_ts_ms() {
        let snapshot = SelectionSupervisorProcSnapshot {
            infos_by_pid: std::collections::HashMap::from([(
                22,
                ProcessInfoObservation {
                    pid: 22,
                    ppid: 1,
                    pgid: 22,
                    state: 'S',
                    start_time_ticks: 200,
                },
            )]),
            children_by_ppid: std::collections::HashMap::new(),
            cmdlines: vec![(
                22,
                vec![
                    "/usr/bin/python3".to_string(),
                    "selection_supervisor.py".to_string(),
                    "run".to_string(),
                    "--label".to_string(),
                    "Deployment/legacy".to_string(),
                    "--state-json".to_string(),
                    "{\"kind\":\"Deployment\",\"name\":\"legacy\",\"authority\":\"legacy\",\"service_name\":\"legacy\",\"argv\":[\"/bin/sleep\",\"1\"],\"log_path\":\"/tmp/legacy.log\"}".to_string(),
                ],
            )],
            zombie_infos: Vec::new(),
        };

        let err =
            live_selection_supervisors(&snapshot, Some("Deployment/legacy"), None).unwrap_err();
        let err_text = format!("{:#}", err);
        assert!(err_text.contains("missing --owner-ts-ms"), "{err_text}");
    }

    #[test]
    fn observe_all_selection_statuses_uses_single_snapshot_for_owner_without_runtime_state() {
        let snapshot = SelectionSupervisorProcSnapshot {
            infos_by_pid: std::collections::HashMap::from([
                (
                    11,
                    ProcessInfoObservation {
                        pid: 11,
                        ppid: 1,
                        pgid: 11,
                        state: 'S',
                        start_time_ticks: 100,
                    },
                ),
                (
                    22,
                    ProcessInfoObservation {
                        pid: 22,
                        ppid: 1,
                        pgid: 22,
                        state: 'S',
                        start_time_ticks: 200,
                    },
                ),
            ]),
            children_by_ppid: std::collections::HashMap::new(),
            cmdlines: vec![
                (
                    11,
                    vec![
                        "/usr/bin/python3".to_string(),
                        "selection_supervisor.py".to_string(),
                        "run".to_string(),
                        "--label".to_string(),
                        "Deployment/target".to_string(),
                        "--state-json".to_string(),
                        serde_json::json!({
                            "kind": WorkloadKind::Deployment,
                            "name": "target",
                            "authority": "target",
                            "service_name": "target",
                            "apply_id": "apply-1",
                            "argv": ["/usr/bin/python3", "-c", "print(1)"],
                            "log_path": "/tmp/target.log"
                        })
                        .to_string(),
                        "--owner-ts-ms".to_string(),
                        "1".to_string(),
                    ],
                ),
                (
                    22,
                    vec![
                        "/usr/bin/python3".to_string(),
                        "selection_supervisor.py".to_string(),
                        "run".to_string(),
                        "--label".to_string(),
                        "Deployment/target".to_string(),
                        "--owner-ts-ms".to_string(),
                        "2".to_string(),
                    ],
                ),
            ],
            zombie_infos: Vec::new(),
        };

        let listed = observe_all_selection_statuses_for_snapshot(&snapshot, None).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].kind, Some(WorkloadKind::Deployment));
        assert_eq!(listed[0].name.as_deref(), Some("target"));
        assert_eq!(listed[0].apply_id, None);
        assert_eq!(listed[0].owner_ts_ms, Some(2));
    }

    #[test]
    fn observe_apply_runtime_statuses_skips_attached_supervisor_without_present_child() {
        let present_state = serde_json::json!({
            "kind": WorkloadKind::Deployment,
            "name": "target-present",
            "authority": "target-present",
            "service_name": "target-present",
            "apply_id": "apply-1",
            "argv": ["/usr/bin/python3", "-c", "print(1)"],
            "log_path": "/tmp/target-present.log"
        })
        .to_string();
        let attached_only_state = serde_json::json!({
            "kind": WorkloadKind::Deployment,
            "name": "target-attached-only",
            "authority": "target-attached-only",
            "service_name": "target-attached-only",
            "apply_id": "apply-1",
            "argv": ["/usr/bin/python3", "-c", "print(2)"],
            "log_path": "/tmp/target-attached-only.log"
        })
        .to_string();
        let snapshot = SelectionSupervisorProcSnapshot {
            infos_by_pid: std::collections::HashMap::from([
                (
                    11,
                    ProcessInfoObservation {
                        pid: 11,
                        ppid: 1,
                        pgid: 11,
                        state: 'S',
                        start_time_ticks: 100,
                    },
                ),
                (
                    12,
                    ProcessInfoObservation {
                        pid: 12,
                        ppid: 11,
                        pgid: 11,
                        state: 'S',
                        start_time_ticks: 101,
                    },
                ),
                (
                    22,
                    ProcessInfoObservation {
                        pid: 22,
                        ppid: 1,
                        pgid: 22,
                        state: 'S',
                        start_time_ticks: 200,
                    },
                ),
            ]),
            children_by_ppid: std::collections::HashMap::from([(11, vec![12])]),
            cmdlines: vec![
                (
                    11,
                    vec![
                        "/usr/bin/python3".to_string(),
                        "selection_supervisor.py".to_string(),
                        "run".to_string(),
                        "--label".to_string(),
                        "Deployment/target-present".to_string(),
                        "--state-json".to_string(),
                        present_state,
                        "--owner-ts-ms".to_string(),
                        "1".to_string(),
                    ],
                ),
                (
                    22,
                    vec![
                        "/usr/bin/python3".to_string(),
                        "selection_supervisor.py".to_string(),
                        "run".to_string(),
                        "--label".to_string(),
                        "Deployment/target-attached-only".to_string(),
                        "--state-json".to_string(),
                        attached_only_state,
                        "--owner-ts-ms".to_string(),
                        "2".to_string(),
                    ],
                ),
            ],
            zombie_infos: Vec::new(),
        };

        let listed = observe_apply_runtime_statuses_for_snapshot("apply-1", &snapshot, None)
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name.as_deref(), Some("target-present"));
        assert!(listed[0].present);
    }

    #[test]
    fn orphan_zombies_are_reported_but_not_counted_as_running_children() {
        let snapshot = SelectionSupervisorProcSnapshot {
            infos_by_pid: std::collections::HashMap::from([(
                11,
                ProcessInfoObservation {
                    pid: 11,
                    ppid: 1,
                    pgid: 11,
                    state: 'S',
                    start_time_ticks: 100,
                },
            )]),
            children_by_ppid: std::collections::HashMap::new(),
            cmdlines: Vec::new(),
            zombie_infos: vec![ProcessInfoObservation {
                pid: 77,
                ppid: 1,
                pgid: 77,
                state: 'Z',
                start_time_ticks: 300,
            }],
        };
        let supervisor = LiveSelectionSupervisor {
            process_info: ProcessInfoObservation {
                pid: 11,
                ppid: 1,
                pgid: 11,
                state: 'S',
                start_time_ticks: 100,
            },
            owner_ts_ms: 1,
            label: "Deployment/target".to_string(),
            runtime_state: Some(SelectionSupervisorLaunchState {
                kind: WorkloadKind::Deployment,
                name: "target".to_string(),
                authority: "target".to_string(),
                service_name: "target".to_string(),
                apply_id: Some("apply-1".to_string()),
                argv: vec!["/bin/sleep".to_string(), "60".to_string()],
                cwd: None,
                log_path: "/tmp/target.log".to_string(),
            }),
        };

        let status = selection_status_from_live_supervisor(
            &snapshot,
            &supervisor,
            WorkloadKind::Deployment,
            "target".to_string(),
        );
        assert!(status.running);
        assert!(!status.present);
        assert_eq!(status.process_count, 1);
        assert_eq!(status.child_process_count, 0);
        assert_eq!(status.container_orphan_zombie_pids, vec![77]);
        let hint = status.status_hint.as_deref().unwrap_or("");
        assert!(hint.contains("treated as stopped"), "{hint}");
        assert!(hint.contains("tini"), "{hint}");
    }

    #[test]
    fn absent_status_still_reports_container_orphan_zombies() {
        let snapshot = SelectionSupervisorProcSnapshot {
            infos_by_pid: std::collections::HashMap::new(),
            children_by_ppid: std::collections::HashMap::new(),
            cmdlines: Vec::new(),
            zombie_infos: vec![ProcessInfoObservation {
                pid: 91,
                ppid: 1,
                pgid: 91,
                state: 'Z',
                start_time_ticks: 400,
            }],
        };
        let zombie_pids = container_orphan_zombie_pids(&snapshot);
        assert_eq!(zombie_pids, vec![91]);
        let hint = selection_status_hint(false, false, zombie_pids.as_slice()).unwrap_or_default();
        assert!(hint.contains("orphaned zombie processes"), "{hint}");
    }

    #[test]
    fn stale_gc_skips_bare_runtime_without_apply_id() {
        let desired_keys = HashSet::new();
        let workload = WorkloadStatusSummary {
            kind: WorkloadKind::DaemonSet,
            name: "fluxon-self-host-bastion-etcd".to_string(),
            authority: "fluxon-self-host-bastion-etcd".to_string(),
            running: true,
            present: Some(true),
            apply_id: None,
            pid: Some(123),
            exit_code: None,
            owner_ts_ms: Some(1),
            err: None,
            container_orphan_zombie_pids: Vec::new(),
            status_hint: None,
        };
        assert!(!stale_gc_should_delete_workload(&desired_keys, &workload));
    }

    #[test]
    fn stale_gc_deletes_apply_owned_runtime_missing_from_desired() {
        let desired_keys = HashSet::new();
        let workload = WorkloadStatusSummary {
            kind: WorkloadKind::Deployment,
            name: "rpc_bench_owner".to_string(),
            authority: "rpc_bench_owner".to_string(),
            running: true,
            present: Some(true),
            apply_id: Some("apply-123".to_string()),
            pid: Some(456),
            exit_code: None,
            owner_ts_ms: Some(2),
            err: None,
            container_orphan_zombie_pids: Vec::new(),
            status_hint: None,
        };
        assert!(stale_gc_should_delete_workload(&desired_keys, &workload));
    }

    #[test]
    fn stale_gc_keeps_apply_owned_runtime_present_in_desired() {
        let mut desired_keys = HashSet::new();
        desired_keys.insert(workload_key(WorkloadKind::Deployment, "rpc_bench_owner"));
        let workload = WorkloadStatusSummary {
            kind: WorkloadKind::Deployment,
            name: "rpc_bench_owner".to_string(),
            authority: "rpc_bench_owner".to_string(),
            running: true,
            present: Some(true),
            apply_id: Some("apply-123".to_string()),
            pid: Some(456),
            exit_code: None,
            owner_ts_ms: Some(2),
            err: None,
            container_orphan_zombie_pids: Vec::new(),
            status_hint: None,
        };
        assert!(!stale_gc_should_delete_workload(&desired_keys, &workload));
    }

    #[test]
    fn supervisor_backed_delete_generation_without_explicit_apply_id_binds_current_generation() {
        let python_exe = PathBuf::from("/usr/bin/python3");
        assert!(
            python_exe.is_file(),
            "python executable does not exist: {}",
            python_exe.display()
        );

        let workdir = std::env::temp_dir().join(format!(
            "selection_supervisor_delete_bind_{}_{}",
            std::process::id(),
            now_ts_ms()
        ));
        std::fs::create_dir_all(&workdir).unwrap();
        let runtime =
            SelectionSupervisorRuntime::materialize(&workdir, &workdir, python_exe.as_path())
                .unwrap();
        let log_dir = workdir.join(OPS_LOG_DIR_NAME);
        std::fs::create_dir_all(&log_dir).unwrap();
        let workloads =
            SupervisorBackedWorkloads::new(workdir.clone(), log_dir.clone(), runtime.clone())
                .unwrap();

        let name = format!("bind_current_generation_{}", now_ts_ms());
        let argv = vec![
            python_exe.display().to_string(),
            "-c".to_string(),
            "import time; time.sleep(30)".to_string(),
        ];

        let first = workloads.start(StartReq {
            kind: WorkloadKind::Deployment,
            name: name.clone(),
            authority: name.clone(),
            service_name: name.clone(),
            apply_id: "apply-1".to_string(),
            owner_ts_ms: 1,
            argv: argv.clone(),
            cwd: None,
            wait_for_attached: true,
            wait_for_present: false,
        });
        assert!(first.ok, "{first:?}");

        let second = workloads.start(StartReq {
            kind: WorkloadKind::Deployment,
            name: name.clone(),
            authority: name.clone(),
            service_name: name.clone(),
            apply_id: "apply-2".to_string(),
            owner_ts_ms: 2,
            argv: argv.clone(),
            cwd: None,
            wait_for_attached: true,
            wait_for_present: false,
        });
        assert!(second.ok, "{second:?}");

        let status = wait_for_selection_attached_without_present(
            WorkloadKind::Deployment,
            &name,
            &name,
            "apply-2",
            2,
            argv.as_slice(),
            None,
        )
        .unwrap();
        assert!(selection_status_matches_attached(
            &status,
            "apply-2",
            2,
            argv.as_slice(),
            None
        ));

        let delete_old = workloads.delete_generation(
            WorkloadKind::Deployment,
            &name,
            &name,
            Some("apply-1"),
        );
        if !delete_old.ok {
            let err = delete_old.err.as_deref().unwrap_or_default();
            assert!(
                err.contains("apply_id target is absent")
                    || err.contains("wait for selection absence failed"),
                "unexpected stale guarded delete result: {delete_old:?}"
            );
        }

        let after_old = wait_for_selection_attached_without_present(
            WorkloadKind::Deployment,
            &name,
            &name,
            "apply-2",
            2,
            argv.as_slice(),
            None,
        )
        .unwrap();
        assert!(selection_status_matches_attached(
            &after_old,
            "apply-2",
            2,
            argv.as_slice(),
            None
        ));

        let delete_current =
            workloads.delete_generation(WorkloadKind::Deployment, &name, &name, None);
        assert!(
            delete_current.ok,
            "unguarded delete should bind and retire the current visible generation: {delete_current:?}"
        );
        wait_for_selection_absent(WorkloadKind::Deployment, &name, &name, Some("apply-2"))
            .unwrap();
    }

    #[test]
    fn materialize_selection_supervisor_runtime_writes_log_shard_helper() {
        let python_exe = PathBuf::from("/usr/bin/python3");
        assert!(
            python_exe.is_file(),
            "python executable does not exist: {}",
            python_exe.display()
        );
        let workdir = tempfile::tempdir().unwrap();
        let runtime =
            SelectionSupervisorRuntime::materialize(workdir.path(), workdir.path(), python_exe.as_path())
                .unwrap();
        assert!(runtime.script_path.exists());
        assert!(
            runtime
                .script_path
                .parent()
                .unwrap()
                .join(OPS_LOG_SHARD_HELPER_FILENAME)
                .is_file()
        );
    }

    #[test]
    fn detached_selection_supervisor_preserves_early_startup_logs() {
        let python_exe = PathBuf::from("/usr/bin/python3");
        assert!(
            python_exe.is_file(),
            "python executable does not exist: {}",
            python_exe.display()
        );
        let workdir = tempfile::tempdir().unwrap();
        let runtime =
            SelectionSupervisorRuntime::materialize(workdir.path(), workdir.path(), python_exe.as_path())
                .unwrap();
        let log_path = workdir.path().join("startup.log");
        let command = vec![
            python_exe.display().to_string(),
            runtime.script_path.display().to_string(),
            "run".to_string(),
            "--label".to_string(),
            "Deployment/startup_demo".to_string(),
            "--scope-key".to_string(),
            workdir.path().display().to_string(),
            "--owner-ts-ms".to_string(),
            "0".to_string(),
            "--restart-policy".to_string(),
            "always".to_string(),
            "--restart-delay-seconds".to_string(),
            "5".to_string(),
            "--max-backoff-seconds".to_string(),
            "30".to_string(),
            "--crashloop-consecutive-restarts".to_string(),
            "0".to_string(),
            "--crashloop-interval-lt-seconds".to_string(),
            "0".to_string(),
            "--".to_string(),
            "/bin/true".to_string(),
        ];
        let pid = runtime.spawn_detached_command(&log_path, command.as_slice()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let expected = "owner-ts-ms must be positive";
        let mut saw_expected = false;
        while Instant::now() < deadline {
            if let Some(path) = resolve_readable_log_path(&log_path) {
                let text = std::fs::read_to_string(path).unwrap_or_default();
                if text.contains(expected) {
                    saw_expected = true;
                    break;
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        if let Some(path) = resolve_readable_log_path(&log_path) {
            let text = std::fs::read_to_string(path).unwrap_or_default();
            assert!(
                text.contains(expected),
                "expected detached supervisor startup logs to reach runtime log, got: {text:?}"
            );
        } else {
            panic!("runtime log path did not materialize");
        }
        assert!(saw_expected, "startup log was not observed before timeout");
        let _ = std::process::Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status();
    }

    #[test]
    fn atomic_group_non_agent_requires_present_before_running_match() {
        let desired = AgentDesiredWorkload {
            kind: WorkloadKind::DaemonSet,
            name: "fluxon-self-host-bastion-fluxon_core_controller__ops_controller".to_string(),
            authority: "fluxon-self-host-bastion-fluxon_core_controller__ops_controller"
                .to_string(),
            logical_selection: "fluxon_core_controller".to_string(),
            service_name: "ops_controller".to_string(),
            apply_id: "apply-1".to_string(),
            updated_ts_ms: 11,
            argv: vec![
                "/usr/bin/env".to_string(),
                "bash".to_string(),
                "-lc".to_string(),
                "sleep 30".to_string(),
            ],
            cwd: Some("/tmp/ops_controller".to_string()),
            atomic_group: Some(AtomicGroupMeta {
                selection_name: "fluxon_core_controller".to_string(),
                phase: 1,
                order: 2,
            }),
        };
        let status = SelectionSupervisorStatus {
            label: "DaemonSet/fluxon-self-host-bastion-fluxon_core_controller__ops_controller"
                .to_string(),
            pid: Some(123),
            pgid: Some(123),
            running: true,
            present: false,
            process_count: 1,
            child_process_count: 0,
            kind: Some(WorkloadKind::DaemonSet),
            name: Some(
                "fluxon-self-host-bastion-fluxon_core_controller__ops_controller".to_string(),
            ),
            authority: Some(
                "fluxon-self-host-bastion-fluxon_core_controller__ops_controller".to_string(),
            ),
            service_name: Some("ops_controller".to_string()),
            apply_id: Some("apply-1".to_string()),
            argv: Some(desired.argv.clone()),
            cwd: desired.cwd.clone(),
            log_path: Some("/tmp/ops_controller.log".to_string()),
            started_ts_ms: None,
            owner_ts_ms: Some(11),
            supervisor_start_time_ticks: Some(99),
            container_orphan_zombie_pids: Vec::new(),
            status_hint: None,
        };
        assert!(desired_workload_requires_present(&desired));
        assert!(!desired_workload_status_matches_goal(&status, &desired));

        let mut present_status = status.clone();
        present_status.present = true;
        present_status.process_count = 2;
        present_status.child_process_count = 1;
        assert!(desired_workload_status_matches_goal(
            &present_status,
            &desired
        ));
    }

    #[test]
    fn ops_agent_can_match_attached_without_present() {
        let desired = AgentDesiredWorkload {
            kind: WorkloadKind::DaemonSet,
            name: "fluxon-self-host-bastion-fluxon_core_controller__ops_agent".to_string(),
            authority: "fluxon-self-host-bastion-fluxon_core_controller__ops_agent".to_string(),
            logical_selection: "fluxon_core_controller".to_string(),
            service_name: OPS_AGENT_WORKLOAD_SERVICE_NAME.to_string(),
            apply_id: "apply-1".to_string(),
            updated_ts_ms: 11,
            argv: vec![
                "/usr/bin/env".to_string(),
                "bash".to_string(),
                "-lc".to_string(),
                "sleep 30".to_string(),
            ],
            cwd: Some("/tmp/ops_agent".to_string()),
            atomic_group: Some(AtomicGroupMeta {
                selection_name: "fluxon_core_controller".to_string(),
                phase: 1,
                order: 3,
            }),
        };
        let status = SelectionSupervisorStatus {
            label: "DaemonSet/fluxon-self-host-bastion-fluxon_core_controller__ops_agent"
                .to_string(),
            pid: Some(456),
            pgid: Some(456),
            running: true,
            present: false,
            process_count: 1,
            child_process_count: 0,
            kind: Some(WorkloadKind::DaemonSet),
            name: Some("fluxon-self-host-bastion-fluxon_core_controller__ops_agent".to_string()),
            authority: Some(
                "fluxon-self-host-bastion-fluxon_core_controller__ops_agent".to_string(),
            ),
            service_name: Some(OPS_AGENT_WORKLOAD_SERVICE_NAME.to_string()),
            apply_id: Some("apply-1".to_string()),
            argv: Some(desired.argv.clone()),
            cwd: desired.cwd.clone(),
            log_path: Some("/tmp/ops_agent.log".to_string()),
            started_ts_ms: None,
            owner_ts_ms: Some(11),
            supervisor_start_time_ticks: Some(100),
            container_orphan_zombie_pids: Vec::new(),
            status_hint: None,
        };
        assert!(!desired_workload_requires_present(&desired));
        assert!(desired_workload_status_matches_goal(&status, &desired));
    }

    #[test]
    fn newer_phase1_overlap_with_applyless_owner_is_not_treated_as_superseded() {
        let desired = AgentDesiredWorkload {
            kind: WorkloadKind::DaemonSet,
            name: "fluxon-self-host-bastion-fluxon_core_controller__owner".to_string(),
            authority: "fluxon-self-host-bastion-fluxon_core_controller__owner".to_string(),
            logical_selection: "fluxon_core_controller".to_string(),
            service_name: "owner".to_string(),
            apply_id: "apply-1".to_string(),
            updated_ts_ms: 11,
            argv: vec![
                "/usr/bin/env".to_string(),
                "bash".to_string(),
                "-lc".to_string(),
                "sleep 30".to_string(),
            ],
            cwd: Some("/tmp/owner".to_string()),
            atomic_group: Some(AtomicGroupMeta {
                selection_name: "fluxon_core_controller".to_string(),
                phase: 1,
                order: 1,
            }),
        };
        let status = SelectionSupervisorStatus {
            label: "DaemonSet/fluxon-self-host-bastion-fluxon_core_controller__owner".to_string(),
            pid: Some(123),
            pgid: Some(123),
            running: true,
            present: true,
            process_count: 2,
            child_process_count: 1,
            kind: Some(WorkloadKind::DaemonSet),
            name: Some("fluxon-self-host-bastion-fluxon_core_controller__owner".to_string()),
            authority: Some("fluxon-self-host-bastion-fluxon_core_controller__owner".to_string()),
            service_name: Some("owner".to_string()),
            apply_id: None,
            argv: Some(desired.argv.clone()),
            cwd: desired.cwd.clone(),
            log_path: Some("/tmp/owner.log".to_string()),
            started_ts_ms: None,
            owner_ts_ms: Some(7),
            supervisor_start_time_ticks: Some(100),
            container_orphan_zombie_pids: Vec::new(),
            status_hint: None,
        };
        assert!(phase1_overlap_with_applyless_owner(&status, &desired));
    }

    #[test]
    fn stale_phase1_overlap_with_newer_applyless_owner_is_treated_as_superseded() {
        let desired = AgentDesiredWorkload {
            kind: WorkloadKind::DaemonSet,
            name: "fluxon-self-host-bastion-fluxon_core_controller__owner".to_string(),
            authority: "fluxon-self-host-bastion-fluxon_core_controller__owner".to_string(),
            logical_selection: "fluxon_core_controller".to_string(),
            service_name: "owner".to_string(),
            apply_id: "apply-1".to_string(),
            updated_ts_ms: 11,
            argv: vec![
                "/usr/bin/env".to_string(),
                "bash".to_string(),
                "-lc".to_string(),
                "sleep 30".to_string(),
            ],
            cwd: Some("/tmp/owner".to_string()),
            atomic_group: Some(AtomicGroupMeta {
                selection_name: "fluxon_core_controller".to_string(),
                phase: 1,
                order: 1,
            }),
        };
        let status = SelectionSupervisorStatus {
            label: "DaemonSet/fluxon-self-host-bastion-fluxon_core_controller__owner".to_string(),
            pid: Some(123),
            pgid: Some(123),
            running: true,
            present: true,
            process_count: 2,
            child_process_count: 1,
            kind: Some(WorkloadKind::DaemonSet),
            name: Some("fluxon-self-host-bastion-fluxon_core_controller__owner".to_string()),
            authority: Some("fluxon-self-host-bastion-fluxon_core_controller__owner".to_string()),
            service_name: Some("owner".to_string()),
            apply_id: None,
            argv: Some(desired.argv.clone()),
            cwd: desired.cwd.clone(),
            log_path: Some("/tmp/owner.log".to_string()),
            started_ts_ms: None,
            owner_ts_ms: Some(999),
            supervisor_start_time_ticks: Some(100),
            container_orphan_zombie_pids: Vec::new(),
            status_hint: None,
        };
        assert!(!phase1_overlap_with_applyless_owner(&status, &desired));
    }

    #[test]
    fn resolve_readable_log_path_prefers_latest_daily_shard() {
        let td = tempfile::tempdir().unwrap();
        let base_path = td.path().join("workload__Deployment__demo.log");
        std::fs::write(
            td.path().join("workload__Deployment__demo.2026-06-19.log"),
            "old\n",
        )
        .unwrap();
        std::fs::write(
            td.path().join("workload__Deployment__demo.2026-06-20.log"),
            "new\n",
        )
        .unwrap();
        let resolved = resolve_readable_log_path(&base_path).unwrap();
        assert_eq!(
            resolved.file_name().and_then(|v| v.to_str()),
            Some("workload__Deployment__demo.2026-06-20.log")
        );
    }

    #[test]
    fn read_workload_log_forward_cursor_rolls_into_next_shard() {
        let td = tempfile::tempdir().unwrap();
        let log_dir = td.path().to_path_buf();
        std::fs::write(
            log_dir.join("workload__Deployment__demo.2026-06-19.log"),
            "old\n",
        )
        .unwrap();
        std::fs::write(
            log_dir.join("workload__Deployment__demo.2026-06-20.log"),
            "new\n",
        )
        .unwrap();
        let handler = ReadWorkloadLogChunkHandler { log_dir };
        let req = ReadWorkloadLogReq {
            kind: WorkloadKind::Deployment,
            name: "demo".to_string(),
            direction: LogReadDirection::Forward,
            cursor: Some(WorkloadLogCursor {
                shard: "2026-06-19".to_string(),
                offset: 4,
            }),
            max_bytes: Some(65536),
        };
        let raw = handler.handle("n1".into(), &serde_json::to_vec(&req).unwrap()).unwrap();
        let resp: ReadWorkloadLogResp = serde_json::from_slice(&raw).unwrap();
        assert!(resp.ok, "{resp:?}");
        assert_eq!(resp.text.as_deref(), Some("new\n"));
        assert_eq!(
            resp.start_cursor,
            Some(WorkloadLogCursor {
                shard: "2026-06-20".to_string(),
                offset: 0,
            })
        );
        assert_eq!(
            resp.end_cursor,
            Some(WorkloadLogCursor {
                shard: "2026-06-20".to_string(),
                offset: 4,
            })
        );
    }

    #[test]
    fn read_workload_log_backward_cursor_rolls_into_previous_shard() {
        let td = tempfile::tempdir().unwrap();
        let log_dir = td.path().to_path_buf();
        std::fs::write(
            log_dir.join("workload__Deployment__demo.2026-06-19.log"),
            "old\n",
        )
        .unwrap();
        std::fs::write(
            log_dir.join("workload__Deployment__demo.2026-06-20.log"),
            "new\n",
        )
        .unwrap();
        let handler = ReadWorkloadLogChunkHandler { log_dir };
        let req = ReadWorkloadLogReq {
            kind: WorkloadKind::Deployment,
            name: "demo".to_string(),
            direction: LogReadDirection::Backward,
            cursor: Some(WorkloadLogCursor {
                shard: "2026-06-20".to_string(),
                offset: 0,
            }),
            max_bytes: Some(65536),
        };
        let raw = handler.handle("n1".into(), &serde_json::to_vec(&req).unwrap()).unwrap();
        let resp: ReadWorkloadLogResp = serde_json::from_slice(&raw).unwrap();
        assert!(resp.ok, "{resp:?}");
        assert_eq!(resp.text.as_deref(), Some("old\n"));
        assert_eq!(
            resp.start_cursor,
            Some(WorkloadLogCursor {
                shard: "2026-06-19".to_string(),
                offset: 0,
            })
        );
        assert_eq!(
            resp.end_cursor,
            Some(WorkloadLogCursor {
                shard: "2026-06-19".to_string(),
                offset: 4,
            })
        );
    }
}
