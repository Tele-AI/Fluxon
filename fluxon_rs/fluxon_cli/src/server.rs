use crate::config::{AVAILABLE_MEMBER_KINDS, MemberKind, MonitorConfig, OutputFormat};
use crate::model::{AVAILABLE_MEMBER_ROLES, ClustersResponse, MemberRole};
use crate::prom::PromClient;
use anyhow::Context;
use axum::Json;
use axum::Router;
use axum::body::{Body, boxed};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderName, HeaderValue, Request, StatusCode, header};
use axum::response::Redirect;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{any, get, post};
use etcd_client::Client as EtcdClient;
use fluxon_commu::{
    ClusterMember, ETCD_PREFIX_CLUSTER_MEMBER_BASE, EtcdPrefixScanAction, MemberRdmaControl,
    NodeRole, cluster_member_base_key, cluster_owner_rdma_control_key, scan_etcd_prefix_paginated,
};
use hyper::Uri;
use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnectorBuilder;
use serde::Serialize;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{RwLock, watch};

use fluxon_util::{
    FluxonCliProxyDescriptorV2, FluxonCliProxyTransportV2, fluxon_cli_proxy_desc_etcd_key_v2,
    fluxon_cli_proxy_desc_etcd_service_prefix_v2,
};

const HDR_PROXY_ORIGINAL_URI: &str = "x-fluxon-cli-proxy-original-uri";
const HDR_PROXY_ORIGINAL_HOST: &str = "x-fluxon-cli-proxy-original-host";

pub type RegisteredPanelProxyBackendFuture = Pin<
    Box<dyn std::future::Future<Output = anyhow::Result<RegisteredPanelProxyBackendResp>> + Send>,
>;

// English note:
// - fluxon_cli is a standalone crate (no direct dependency on fluxon_kv / P2P framework).
// - Some deployments want "registered panel proxy" to be routed via Fluxon internal RPC instead
//   of direct L7 HTTP.
// - We inject an optional dynamic backend from the embedder (e.g. fluxon_kv master), so we can
//   route requests via RPC without introducing crate dependency cycles.
pub type RegisteredPanelProxyBackend =
    Arc<dyn Fn(RegisteredPanelProxyBackendReq) -> RegisteredPanelProxyBackendFuture + Send + Sync>;

fn new_proxy_client() -> hyper::Client<hyper_rustls::HttpsConnector<HttpConnector>, Body> {
    let https = HttpsConnectorBuilder::new()
        .with_native_roots()
        .https_or_http()
        .enable_http1()
        .build();
    hyper::Client::builder().build::<_, Body>(https)
}

fn fluxon_cli_proxy_desc_etcd_key(service_name: &str, cluster_name: &str) -> String {
    // English note: keep this etcd key format stable because it forms a lightweight "registry"
    // contract between fluxon_cli (consumer) and business panels (publishers).
    //
    // Causal chain:
    // - fluxon_cli must proxy panels without understanding any business-specific API paths.
    // - Each service publishes a small descriptor (base_url + allowlist) into etcd.
    // - fluxon_cli reads the descriptor and performs a pure L7 proxy based on the allowlist.
    fluxon_cli_proxy_desc_etcd_key_v2(service_name, cluster_name)
}

#[derive(Debug, Clone)]
pub struct RegisteredPanelProxyBackendReq {
    pub service_name: String,
    pub cluster_name: String,
    pub node_id: String,
    pub method: axum::http::Method,
    pub path_and_query: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub original_uri: String,
    pub original_host: String,
}

#[derive(Debug, Clone)]
pub struct RegisteredPanelProxyBackendResp {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

fn registered_panel_target(
    service_name: &str,
    cluster_name: &str,
    uri: &axum::http::Uri,
) -> String {
    let mut target = if service_name == crate::OPS_PANEL_SERVICE_NAME {
        format!("/r/{}/{}/ui", service_name, cluster_name)
    } else if service_name == "fs_s3" {
        format!("/r/{}/{}/ui/", service_name, cluster_name)
    } else {
        format!("/r/{}/{}/", service_name, cluster_name)
    };
    if let Some(qs) = uri.query() {
        let mut ser = url::form_urlencoded::Serializer::new(String::new());
        for (k, v) in url::form_urlencoded::parse(qs.as_bytes()) {
            let kk = k.as_ref();
            if kk == "cluster_name" || kk == "member_kind" {
                continue;
            }
            ser.append_pair(k.as_ref(), v.as_ref());
        }
        let qs2 = ser.finish();
        if !qs2.is_empty() {
            target.push('?');
            target.push_str(&qs2);
        }
    }
    target
}

fn fs_s3_ui_panel_target(cluster_name: &str, rest: Option<&str>, uri: &axum::http::Uri) -> String {
    let mut target = match rest {
        Some(rest) if !rest.trim().is_empty() => {
            format!(
                "/r/fs_s3/{}/ui/{}",
                cluster_name,
                rest.trim_start_matches('/')
            )
        }
        _ => format!("/r/fs_s3/{}/ui/", cluster_name),
    };
    if let Some(qs) = uri.query() {
        target.push('?');
        target.push_str(qs);
    }
    target
}

fn redirect_to_registered_panel(
    service_name: &str,
    cluster_name: &str,
    uri: &axum::http::Uri,
) -> Response {
    let target = registered_panel_target(service_name, cluster_name, uri);
    Redirect::temporary(&target).into_response()
}

fn text_response(status: StatusCode, body: String) -> Response {
    let mut resp = body.into_response();
    *resp.status_mut() = status;
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        "text/plain; charset=utf-8".parse().unwrap(),
    );
    resp
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('\"', "&quot;")
        .replace('\'', "&#39;")
}

const FLUXON_CLI_AUTO_REFRESH_TOOL_JS: &str = r#"
(() => {
  if (window.fluxonCliAutoRefresh && window.fluxonCliAutoRefresh._v === 1) return;

  const MODE_REPLACE_APP = 'replace_app';
  const MODE_RELOAD = 'reload';

  function isFn(v) {
    return typeof v === 'function';
  }

  function normalizeCfg(cfg) {
    if (cfg === undefined || cfg === null) return null;
    if (typeof cfg !== 'object') {
      console.warn('auto_refresh: invalid cfg object: window.fluxon_cli_auto_refresh_cfg');
      return null;
    }

    const mode = cfg.mode;
    if (mode !== MODE_REPLACE_APP && mode !== MODE_RELOAD) {
      console.warn('auto_refresh: invalid cfg.mode (expected replace_app|reload):', mode);
      return null;
    }

    const refreshSecs = Number(cfg.refreshSecs);
    if (!Number.isFinite(refreshSecs) || refreshSecs <= 0) {
      console.warn('auto_refresh: invalid cfg.refreshSecs (expected > 0):', cfg.refreshSecs);
      return null;
    }

    const url = String(cfg.url || '');
    if (url.length === 0) {
      console.warn('auto_refresh: invalid cfg.url (expected non-empty string)');
      return null;
    }

    const appId = String(cfg.appId || '');
    const countdownId = String(cfg.countdownId || '');
    if (mode === MODE_REPLACE_APP) {
      if (appId.length === 0) {
        console.warn('auto_refresh: missing cfg.appId for replace_app mode');
        return null;
      }
      if (countdownId.length === 0) {
        console.warn('auto_refresh: missing cfg.countdownId for replace_app mode');
        return null;
      }
    } else if (mode === MODE_RELOAD) {
      if (countdownId.length === 0) {
        console.warn('auto_refresh: missing cfg.countdownId for reload mode');
        return null;
      }
    }

    return {
      mode,
      refreshSecs: Math.trunc(refreshSecs),
      url,
      appId,
      countdownId,
    };
  }

  function normalizeHooks(h) {
    if (!h || typeof h !== 'object') {
      console.warn('auto_refresh: missing hooks object: window.fluxon_cli_auto_refresh_hooks');
      return null;
    }
    if (!isFn(h.captureState) || !isFn(h.restoreState) || !isFn(h.afterReplace)) {
      console.warn('auto_refresh: hooks must provide captureState/restoreState/afterReplace functions');
      return null;
    }
    return h;
  }

  function setCountdown(countdownId, v) {
    const el = document.getElementById(countdownId);
    if (!el) return;
    el.textContent = String(v);
  }

  async function refreshReplaceOnce(cfg, hooks, inFlightRef) {
    if (inFlightRef.inFlight) return;
    inFlightRef.inFlight = true;
    const state = hooks.captureState();
    try {
      const resp = await fetch(cfg.url, { cache: 'no-store' });
      if (!resp.ok) {
        console.warn('auto_refresh: fetch failed:', resp.status, resp.statusText);
        return;
      }
      const text = await resp.text();
      const doc = new DOMParser().parseFromString(text, 'text/html');
      const nextApp = doc.getElementById(cfg.appId);
      const curApp = document.getElementById(cfg.appId);
      if (!nextApp || !curApp) {
        console.warn('auto_refresh: missing app element:', cfg.appId);
        return;
      }
      curApp.innerHTML = nextApp.innerHTML;
      hooks.restoreState(state);
      hooks.afterReplace();
    } catch (e) {
      console.warn('auto_refresh: refresh failed:', e);
    } finally {
      inFlightRef.inFlight = false;
    }
  }

  function start(cfg, hooks) {
    if (window.__fluxon_cli_auto_refresh_installed_v1) {
      console.warn('auto_refresh: already installed');
      return;
    }
    window.__fluxon_cli_auto_refresh_installed_v1 = true;

    let remaining = cfg.refreshSecs;
    const inFlightRef = { inFlight: false };
    setCountdown(cfg.countdownId, remaining);
    setInterval(async () => {
      if (remaining > 0) remaining -= 1;
      if (remaining === 0) {
        if (cfg.mode === MODE_RELOAD) {
          window.location.reload();
          return;
        }
        await refreshReplaceOnce(cfg, hooks, inFlightRef);
        remaining = cfg.refreshSecs;
      }
      setCountdown(cfg.countdownId, remaining);
    }, 1000);
  }

  function installFromGlobal() {
    const cfg = normalizeCfg(window.fluxon_cli_auto_refresh_cfg);
    if (!cfg) return;
    const hooks = normalizeHooks(window.fluxon_cli_auto_refresh_hooks);
    if (!hooks) return;
    start(cfg, hooks);
  }

  window.fluxonCliAutoRefresh = {
    _v: 1,
    MODE_REPLACE_APP,
    MODE_RELOAD,
    installFromGlobal,
  };

  installFromGlobal();
})();
"#;

fn inject_html_script_before_body_end(html: String, script_tag_html: &str) -> String {
    if let Some(pos) = html.rfind("</body>") {
        let mut out = String::with_capacity(html.len() + script_tag_html.len() + 8);
        out.push_str(&html[..pos]);
        out.push_str(script_tag_html);
        out.push_str(&html[pos..]);
        return out;
    }
    if let Some(pos) = html.rfind("</html>") {
        let mut out = String::with_capacity(html.len() + script_tag_html.len() + 8);
        out.push_str(&html[..pos]);
        out.push_str(script_tag_html);
        out.push_str(&html[pos..]);
        return out;
    }
    let mut out = String::with_capacity(html.len() + script_tag_html.len());
    out.push_str(&html);
    out.push_str(script_tag_html);
    out
}

fn inject_auto_refresh_tool(html: String) -> String {
    let mut tag = String::with_capacity(FLUXON_CLI_AUTO_REFRESH_TOOL_JS.len() + 128);
    tag.push_str(
        "\n<!-- fluxon_cli: auto_refresh tool (SSR-friendly, replace #app) -->\n<script>\n",
    );
    tag.push_str(FLUXON_CLI_AUTO_REFRESH_TOOL_JS);
    tag.push_str("\n</script>\n");
    inject_html_script_before_body_end(html, &tag)
}

fn available_member_kind_query_strs() -> Vec<&'static str> {
    AVAILABLE_MEMBER_KINDS
        .iter()
        .copied()
        .map(|k| k.as_query_str())
        .collect()
}

fn parse_member_kind(s: &str) -> Option<MemberKind> {
    MemberKind::parse_query_str(s)
}

#[derive(Clone)]
struct AppState {
    cfg: Arc<MonitorConfig>,
    log_schema_cache: Arc<LogSchemaCache>,
    proxy_client: hyper::Client<hyper_rustls::HttpsConnector<HttpConnector>, Body>,
    registered_panel_proxy_backend: Option<RegisteredPanelProxyBackend>,
}

struct LogSchemaCache {
    by_table: RwLock<std::collections::HashMap<String, LogSchema>>,
}

#[derive(Clone)]
struct LogSchema {
    time_column: String,
    time_data_type: String,
    has_severity_text: bool,
    has_member_id: bool,
}

impl LogSchemaCache {
    fn new() -> Self {
        Self {
            by_table: RwLock::new(std::collections::HashMap::new()),
        }
    }

    async fn get_or_init(
        &self,
        sql: &fluxon_observability::greptime_sql::GreptimeSqlClient,
        table: &str,
    ) -> anyhow::Result<LogSchema> {
        {
            let guard = self.by_table.read().await;
            if let Some(v) = guard.get(table) {
                return Ok(v.clone());
            }
        }

        let desc = sql.describe_table(table).await?;
        let time_col = desc.find_time_column()?;
        if !desc.has_column("body") {
            let cols = desc
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>()
                .join(",");
            anyhow::bail!(
                "greptime log table missing required column 'body': table={} cols={}",
                table,
                cols
            );
        }

        let schema = LogSchema {
            time_column: time_col.name.clone(),
            time_data_type: time_col.data_type.clone(),
            has_severity_text: desc.has_column("severity_text"),
            has_member_id: desc.has_column(fluxon_observability::keys::KEY_MEMBER_ID),
        };

        let mut guard = self.by_table.write().await;
        guard.insert(table.to_string(), schema.clone());
        Ok(schema)
    }
}

async fn list_clusters(etcd: &mut EtcdClient) -> anyhow::Result<Vec<String>> {
    let prefix = format!("{}/", ETCD_PREFIX_CLUSTER_MEMBER_BASE);
    let mut clusters: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    scan_etcd_prefix_paginated(etcd, &prefix, |key, _value| {
        let key = String::from_utf8_lossy(key);
        if !key.starts_with(&prefix) {
            return Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue);
        }
        let rest = &key[prefix.len()..];
        let Some((cluster, _tail)) = rest.split_once('/') else {
            return Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue);
        };
        if !cluster.is_empty() {
            clusters.insert(cluster.to_string());
        }
        Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue)
    })
    .await
    .with_context(|| format!("etcd scan prefix: {}", prefix))?;
    Ok(clusters.into_iter().collect())
}

#[derive(serde::Deserialize)]
struct ClusterQuery {
    cluster_name: Option<String>,
    member_kind: Option<String>,
    member_roles: Option<Vec<String>>,
}

#[derive(serde::Deserialize)]
struct KvMetricPanelQuery {
    cluster_name: Option<String>,
    window: Option<String>,
    member_roles: Option<Vec<String>>,
}

#[derive(serde::Deserialize)]
struct KvMetricMembersQuery {
    cluster_name: Option<String>,
    metric_key: Option<String>,
    window: Option<String>,
    member_roles: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
struct KvMetricPanelResponse {
    range: KvMetricRangeWire,
    metrics: Vec<KvAggregateMetricCardWire>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
struct KvMetricMembersResponse {
    metric: KvMetricMetaWire,
    range: KvMetricRangeWire,
    members: Vec<KvMemberSeriesWire>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
struct KvMetricMetaWire {
    key: String,
    label: String,
    unit: String,
    aggregate: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
struct KvMetricRangeWire {
    window: String,
    step_s: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
struct KvAggregateMetricCardWire {
    metric: KvMetricMetaWire,
    latest: Option<f64>,
    aggregate_series: Vec<(f64, f64)>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
struct KvMemberSeriesWire {
    member_id: String,
    role: String,
    node_key: String,
    latest: Option<f64>,
    series: Vec<(f64, f64)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KvMetricAggregate {
    Sum,
    Max,
}

impl KvMetricAggregate {
    fn as_str(self) -> &'static str {
        match self {
            Self::Sum => "sum",
            Self::Max => "max",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KvMetricValueField {
    PutRps,
    GetRps,
    PutBps,
    GetBps,
    ProcessRss,
    SegUsedBytes,
    TokioGlobalQueueDepth,
    TokioBusyPercent,
}

#[derive(Debug, Clone, Copy)]
struct KvMetricSpec {
    key: &'static str,
    label: &'static str,
    unit: &'static str,
    aggregate: KvMetricAggregate,
    field: KvMetricValueField,
    roles: &'static [MemberRole],
}

const KV_METRIC_OWNER_AND_EXTERNAL_ROLES: &[MemberRole] =
    &[MemberRole::OwnerClient, MemberRole::ExternalClient];
const KV_METRIC_OWNER_ONLY_ROLES: &[MemberRole] = &[MemberRole::OwnerClient];

const KV_METRIC_SPECS: &[KvMetricSpec] = &[
    KvMetricSpec {
        key: "put_rps",
        label: "Put RPS",
        unit: "rps",
        aggregate: KvMetricAggregate::Sum,
        field: KvMetricValueField::PutRps,
        roles: KV_METRIC_OWNER_AND_EXTERNAL_ROLES,
    },
    KvMetricSpec {
        key: "get_rps",
        label: "Get RPS",
        unit: "rps",
        aggregate: KvMetricAggregate::Sum,
        field: KvMetricValueField::GetRps,
        roles: KV_METRIC_OWNER_AND_EXTERNAL_ROLES,
    },
    KvMetricSpec {
        key: "put_bps",
        label: "Put B/s",
        unit: "B/s",
        aggregate: KvMetricAggregate::Sum,
        field: KvMetricValueField::PutBps,
        roles: KV_METRIC_OWNER_AND_EXTERNAL_ROLES,
    },
    KvMetricSpec {
        key: "get_bps",
        label: "Get B/s",
        unit: "B/s",
        aggregate: KvMetricAggregate::Sum,
        field: KvMetricValueField::GetBps,
        roles: KV_METRIC_OWNER_AND_EXTERNAL_ROLES,
    },
    KvMetricSpec {
        key: "process_rss",
        label: "Process RSS",
        unit: "bytes",
        aggregate: KvMetricAggregate::Sum,
        field: KvMetricValueField::ProcessRss,
        roles: KV_METRIC_OWNER_AND_EXTERNAL_ROLES,
    },
    KvMetricSpec {
        key: "seg_used_bytes",
        label: "Segment Used",
        unit: "bytes",
        aggregate: KvMetricAggregate::Sum,
        field: KvMetricValueField::SegUsedBytes,
        roles: KV_METRIC_OWNER_ONLY_ROLES,
    },
    KvMetricSpec {
        key: "tokio_global_queue_depth",
        label: "Tokio Queue Depth",
        unit: "count",
        aggregate: KvMetricAggregate::Sum,
        field: KvMetricValueField::TokioGlobalQueueDepth,
        roles: KV_METRIC_OWNER_ONLY_ROLES,
    },
    KvMetricSpec {
        key: "tokio_busy_percent",
        label: "Tokio Busy %",
        unit: "percent",
        aggregate: KvMetricAggregate::Max,
        field: KvMetricValueField::TokioBusyPercent,
        roles: KV_METRIC_OWNER_ONLY_ROLES,
    },
];

fn kv_metric_spec_by_key(key: &str) -> Option<KvMetricSpec> {
    KV_METRIC_SPECS.iter().copied().find(|spec| spec.key == key)
}

fn kv_metric_meta(spec: KvMetricSpec) -> KvMetricMetaWire {
    KvMetricMetaWire {
        key: spec.key.to_string(),
        label: spec.label.to_string(),
        unit: spec.unit.to_string(),
        aggregate: spec.aggregate.as_str().to_string(),
    }
}

fn parse_kv_metric_window(raw: Option<&str>) -> Result<(String, f64, u64), String> {
    match raw.unwrap_or("15m") {
        "5m" => Ok(("5m".to_string(), 5.0 * 60.0, 5)),
        "15m" => Ok(("15m".to_string(), 15.0 * 60.0, 15)),
        "1h" => Ok(("1h".to_string(), 60.0 * 60.0, 30)),
        "6h" => Ok(("6h".to_string(), 6.0 * 60.0 * 60.0, 120)),
        "24h" => Ok(("24h".to_string(), 24.0 * 60.0 * 60.0, 600)),
        other => Err(format!(
            "invalid window: {} (expected 5m|15m|1h|6h|24h)",
            other
        )),
    }
}

fn parse_member_roles_list(raw: Option<&Vec<String>>) -> Result<Option<Vec<MemberRole>>, String> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    if raw.is_empty() {
        return Err("member_roles cannot be empty".to_string());
    }
    let mut out: Vec<MemberRole> = Vec::with_capacity(raw.len());
    for v in raw {
        let Some(r) = MemberRole::parse_query_str(v) else {
            return Err(format!(
                "invalid member_roles value: {} (expected: {})",
                v,
                AVAILABLE_MEMBER_ROLES
                    .iter()
                    .map(|r| r.as_str())
                    .collect::<Vec<_>>()
                    .join("|")
            ));
        };
        if r == MemberRole::Unknown {
            return Err("member_roles cannot include 'unknown'".to_string());
        }
        if !out.contains(&r) {
            out.push(r);
        }
    }
    Ok(Some(out))
}

fn kv_metric_promql_for_member(spec: KvMetricSpec, member_id: &str) -> String {
    match spec.field {
        KvMetricValueField::PutRps => format!(
            "sum_over_time(kv_op_end_event{{node={member_id:?},op=\"put\",status=\"success\"}}[1s])"
        ),
        KvMetricValueField::GetRps => format!(
            "sum_over_time(kv_op_end_event{{node={member_id:?},op=\"get\",status=~\"hit|success\"}}[1s])"
        ),
        KvMetricValueField::PutBps => format!(
            "sum_over_time(kv_op_end_bytes{{node={member_id:?},op=\"put\",status=\"success\"}}[1s])"
        ),
        KvMetricValueField::GetBps => format!(
            "sum_over_time(kv_op_end_bytes{{node={member_id:?},op=\"get\",status=~\"hit|success\"}}[1s])"
        ),
        KvMetricValueField::ProcessRss => {
            format!("process_resident_memory_bytes{{node={member_id:?}}}")
        }
        KvMetricValueField::SegUsedBytes => {
            format!("sum(kvcache_segment_used_bytes{{node={member_id:?}}})")
        }
        KvMetricValueField::TokioGlobalQueueDepth => {
            format!("tokio_global_queue_depth{{node={member_id:?}}}")
        }
        KvMetricValueField::TokioBusyPercent => {
            format!("tokio_busy_percent{{node={member_id:?}}}")
        }
    }
}

#[derive(Debug, Clone)]
struct KvMetricMemberRef {
    member_id: String,
    role: MemberRole,
    node_key: String,
}

fn select_kv_metric_members(
    snapshot: &crate::model::ClusterSnapshot,
    spec: KvMetricSpec,
    visible_roles: Option<&Vec<MemberRole>>,
) -> Vec<KvMetricMemberRef> {
    let mut out = Vec::new();
    for node in &snapshot.nodes {
        for member in &node.members {
            if !spec.roles.contains(&member.role) {
                continue;
            }
            if let Some(v) = visible_roles {
                if !v.contains(&member.role) {
                    continue;
                }
            }
            out.push(KvMetricMemberRef {
                member_id: member.member_id.clone(),
                role: member.role,
                node_key: node.node_key.clone(),
            });
        }
    }
    out
}

fn prom_regex_escape_literal_local(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for ch in s.chars() {
        match ch {
            '\\' | '.' | '+' | '*' | '?' | '|' | '{' | '}' | '(' | ')' | '[' | ']' | '^' | '$' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

fn prom_regex_union_exact_local(ids: &[String]) -> Option<String> {
    let mut parts = Vec::new();
    for id in ids {
        let t = id.trim();
        if t.is_empty() {
            continue;
        }
        parts.push(prom_regex_escape_literal_local(t));
    }
    if parts.is_empty() {
        None
    } else {
        Some(format!("^(?:{})$", parts.join("|")))
    }
}

fn kv_metric_aggregate_promql(spec: KvMetricSpec, member_ids: &[String]) -> Result<String, String> {
    let member_regex = prom_regex_union_exact_local(member_ids)
        .ok_or_else(|| "no visible members for metric".to_string())?;
    let promql = match spec.field {
        KvMetricValueField::PutRps => format!(
            "sum(sum_over_time(kv_op_end_event{{node=~{member_regex:?},op=\"put\",status=\"success\"}}[1s]))"
        ),
        KvMetricValueField::GetRps => format!(
            "sum(sum_over_time(kv_op_end_event{{node=~{member_regex:?},op=\"get\",status=~\"hit|success\"}}[1s]))"
        ),
        KvMetricValueField::PutBps => format!(
            "sum(sum_over_time(kv_op_end_bytes{{node=~{member_regex:?},op=\"put\",status=\"success\"}}[1s]))"
        ),
        KvMetricValueField::GetBps => format!(
            "sum(sum_over_time(kv_op_end_bytes{{node=~{member_regex:?},op=\"get\",status=~\"hit|success\"}}[1s]))"
        ),
        KvMetricValueField::ProcessRss => {
            format!("sum(process_resident_memory_bytes{{node=~{member_regex:?}}})")
        }
        KvMetricValueField::SegUsedBytes => {
            format!("sum(kvcache_segment_used_bytes{{node=~{member_regex:?}}})")
        }
        KvMetricValueField::TokioGlobalQueueDepth => {
            format!("sum(tokio_global_queue_depth{{node=~{member_regex:?}}})")
        }
        KvMetricValueField::TokioBusyPercent => {
            format!("max(tokio_busy_percent{{node=~{member_regex:?}}})")
        }
    };
    Ok(promql)
}

async fn kv_metric_panel(
    State(st): State<Arc<AppState>>,
    Query(q): Query<KvMetricPanelQuery>,
) -> Response {
    let Some(cluster_name) = q.cluster_name.as_ref() else {
        return text_response(
            StatusCode::BAD_REQUEST,
            "missing query param: cluster_name".to_string(),
        );
    };
    let visible_member_roles = match parse_member_roles_list(q.member_roles.as_ref()) {
        Ok(v) => v,
        Err(e) => return text_response(StatusCode::BAD_REQUEST, e),
    };
    let (window_label, window_secs, step_s) = match parse_kv_metric_window(q.window.as_deref()) {
        Ok(v) => v,
        Err(e) => return text_response(StatusCode::BAD_REQUEST, e),
    };
    let cfg = MonitorConfig {
        etcd_endpoints: st.cfg.etcd_endpoints.clone(),
        prometheus_base_url: st.cfg.prometheus_base_url.clone(),
        cluster_name: cluster_name.clone(),
        member_kind: MemberKind::Kv,
        output: OutputFormat::Web,
        mq_unique_key_prefixes: st.cfg.mq_unique_key_prefixes.clone(),
        http_listen_addr: st.cfg.http_listen_addr.clone(),
        greptime_sql: st.cfg.greptime_sql.clone(),
    };
    let snapshot = match crate::build_cluster_snapshot(&cfg).await {
        Ok(v) => v,
        Err(e) => {
            return text_response(
                StatusCode::BAD_GATEWAY,
                format!("snapshot build failed: {}", e),
            );
        }
    };
    let prom = PromClient::new(st.cfg.prometheus_base_url.clone());
    let end_s = match prom.effective_query_time_s() {
        Ok(v) => v,
        Err(e) => {
            return text_response(
                StatusCode::BAD_GATEWAY,
                format!("resolve query time failed: {}", e),
            );
        }
    };
    let start_s = (end_s - window_secs).max(0.0);
    let step = format!("{}s", step_s);
    let mut cards = Vec::with_capacity(KV_METRIC_SPECS.len());
    let mut warnings = snapshot.warnings.clone();
    for spec in KV_METRIC_SPECS.iter().copied() {
        let members = select_kv_metric_members(&snapshot, spec, visible_member_roles.as_ref());
        if members.is_empty() {
            cards.push(KvAggregateMetricCardWire {
                metric: kv_metric_meta(spec),
                latest: None,
                aggregate_series: Vec::new(),
            });
            continue;
        }
        let member_ids = members
            .iter()
            .map(|m| m.member_id.clone())
            .collect::<Vec<_>>();
        let promql = match kv_metric_aggregate_promql(spec, &member_ids) {
            Ok(v) => v,
            Err(e) => {
                warnings.push(format!("metric {} unavailable: {}", spec.key, e));
                cards.push(KvAggregateMetricCardWire {
                    metric: kv_metric_meta(spec),
                    latest: None,
                    aggregate_series: Vec::new(),
                });
                continue;
            }
        };
        let range = match prom.query_range(&promql, start_s, end_s, &step).await {
            Ok(v) => v,
            Err(e) => {
                warnings.push(format!("metric {} query_range failed: {}", spec.key, e));
                cards.push(KvAggregateMetricCardWire {
                    metric: kv_metric_meta(spec),
                    latest: None,
                    aggregate_series: Vec::new(),
                });
                continue;
            }
        };
        let aggregate_series = range
            .into_iter()
            .flat_map(|series| {
                series
                    .values
                    .into_iter()
                    .filter_map(|(ts, value)| value.parse::<f64>().ok().map(|v| (ts, v)))
            })
            .collect::<Vec<_>>();
        let latest = aggregate_series.last().map(|(_, v)| *v);
        cards.push(KvAggregateMetricCardWire {
            metric: kv_metric_meta(spec),
            latest,
            aggregate_series,
        });
    }

    let mut resp = (
        StatusCode::OK,
        Json(KvMetricPanelResponse {
            range: KvMetricRangeWire {
                window: window_label,
                step_s,
            },
            metrics: cards,
            warnings,
        }),
    )
        .into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/json; charset=utf-8".parse().unwrap(),
    );
    resp
}

async fn kv_metric_members(
    State(st): State<Arc<AppState>>,
    Query(q): Query<KvMetricMembersQuery>,
) -> Response {
    let Some(cluster_name) = q.cluster_name.as_ref() else {
        return text_response(
            StatusCode::BAD_REQUEST,
            "missing query param: cluster_name".to_string(),
        );
    };
    let Some(metric_key) = q.metric_key.as_ref() else {
        return text_response(
            StatusCode::BAD_REQUEST,
            "missing query param: metric_key".to_string(),
        );
    };
    let spec = match kv_metric_spec_by_key(metric_key) {
        Some(v) => v,
        None => {
            return text_response(
                StatusCode::BAD_REQUEST,
                format!("invalid metric_key: {}", metric_key),
            );
        }
    };
    let visible_member_roles = match parse_member_roles_list(q.member_roles.as_ref()) {
        Ok(v) => v,
        Err(e) => return text_response(StatusCode::BAD_REQUEST, e),
    };
    let (window_label, window_secs, step_s) = match parse_kv_metric_window(q.window.as_deref()) {
        Ok(v) => v,
        Err(e) => return text_response(StatusCode::BAD_REQUEST, e),
    };
    let cfg = MonitorConfig {
        etcd_endpoints: st.cfg.etcd_endpoints.clone(),
        prometheus_base_url: st.cfg.prometheus_base_url.clone(),
        cluster_name: cluster_name.clone(),
        member_kind: MemberKind::Kv,
        output: OutputFormat::Web,
        mq_unique_key_prefixes: st.cfg.mq_unique_key_prefixes.clone(),
        http_listen_addr: st.cfg.http_listen_addr.clone(),
        greptime_sql: st.cfg.greptime_sql.clone(),
    };
    let snapshot = match crate::build_cluster_snapshot(&cfg).await {
        Ok(v) => v,
        Err(e) => {
            return text_response(
                StatusCode::BAD_GATEWAY,
                format!("snapshot build failed: {}", e),
            );
        }
    };
    let members = select_kv_metric_members(&snapshot, spec, visible_member_roles.as_ref());
    let prom = PromClient::new(st.cfg.prometheus_base_url.clone());
    let end_s = match prom.effective_query_time_s() {
        Ok(v) => v,
        Err(e) => {
            return text_response(
                StatusCode::BAD_GATEWAY,
                format!("resolve query time failed: {}", e),
            );
        }
    };
    let start_s = (end_s - window_secs).max(0.0);
    let step = format!("{}s", step_s);
    let mut warnings = snapshot.warnings.clone();
    let mut rows = Vec::with_capacity(members.len());
    for member in members {
        let promql = kv_metric_promql_for_member(spec, &member.member_id);
        let range = match prom.query_range(&promql, start_s, end_s, &step).await {
            Ok(v) => v,
            Err(e) => {
                warnings.push(format!(
                    "metric {} member {} query_range failed: {}",
                    spec.key, member.member_id, e
                ));
                continue;
            }
        };
        let series = range
            .into_iter()
            .flat_map(|series| {
                series
                    .values
                    .into_iter()
                    .filter_map(|(ts, value)| value.parse::<f64>().ok().map(|v| (ts, v)))
            })
            .collect::<Vec<_>>();
        rows.push(KvMemberSeriesWire {
            member_id: member.member_id,
            role: member.role.as_str().to_string(),
            node_key: member.node_key,
            latest: series.last().map(|(_, v)| *v),
            series,
        });
    }
    rows.sort_by(|a, b| {
        let av = a.latest.unwrap_or(f64::NEG_INFINITY);
        let bv = b.latest.unwrap_or(f64::NEG_INFINITY);
        bv.partial_cmp(&av)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.member_id.cmp(&b.member_id))
    });

    let mut resp = (
        StatusCode::OK,
        Json(KvMetricMembersResponse {
            metric: kv_metric_meta(spec),
            range: KvMetricRangeWire {
                window: window_label,
                step_s,
            },
            members: rows,
            warnings,
        }),
    )
        .into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/json; charset=utf-8".parse().unwrap(),
    );
    resp
}

#[derive(serde::Deserialize)]
struct TopologyQuery {
    cluster_name: Option<String>,
    // UNIX timestamp in seconds (floating-point allowed). When provided, topology queries Prometheus
    // as-of this time. KV membership tree is still current (etcd), metrics are historical (Prom).
    at: Option<String>,
}

#[derive(serde::Deserialize)]
struct OpsFluxonClusterQuery {
    member_roles: Option<Vec<String>>,
}

#[derive(serde::Deserialize)]
struct OpsFluxonTopologyQuery {
    at: Option<String>,
}

#[derive(serde::Deserialize)]
struct RdmaDeviceUpdateForm {
    member_id: String,
    member_start_time: i64,
    enabled_devices: Vec<String>,
}

fn parse_rdma_device_update_form(body: &[u8]) -> Result<RdmaDeviceUpdateForm, String> {
    let mut member_id: Option<String> = None;
    let mut member_start_time_raw: Option<String> = None;
    let mut enabled_devices: Vec<String> = Vec::new();

    for (k, v) in url::form_urlencoded::parse(body) {
        match k.as_ref() {
            "member_id" => member_id = Some(v.trim().to_string()),
            "member_start_time" => member_start_time_raw = Some(v.trim().to_string()),
            // Accept both repeated "enabled_devices=x" and common bracketed form names.
            "enabled_devices" | "enabled_devices[]" => enabled_devices.push(v.trim().to_string()),
            _ => {}
        }
    }

    let member_id = match member_id {
        Some(v) if !v.is_empty() => v,
        _ => return Err("missing form field: member_id".to_string()),
    };
    let member_start_time_raw = match member_start_time_raw {
        Some(v) if !v.is_empty() => v,
        _ => return Err("missing form field: member_start_time".to_string()),
    };
    let member_start_time = member_start_time_raw.parse::<i64>().map_err(|err| {
        format!(
            "invalid form field member_start_time: value={} err={}",
            member_start_time_raw, err
        )
    })?;

    Ok(RdmaDeviceUpdateForm {
        member_id,
        member_start_time,
        enabled_devices,
    })
}

fn parse_member_roles_query(q: &ClusterQuery) -> Result<Option<Vec<MemberRole>>, String> {
    let Some(raw) = q.member_roles.as_ref() else {
        return Ok(None);
    };
    if raw.is_empty() {
        return Err("member_roles cannot be empty".to_string());
    }
    let mut out: Vec<MemberRole> = Vec::with_capacity(raw.len());
    for v in raw {
        let Some(r) = MemberRole::parse_query_str(v) else {
            return Err(format!(
                "invalid member_roles value: {} (expected: {})",
                v,
                AVAILABLE_MEMBER_ROLES
                    .iter()
                    .map(|r| r.as_str())
                    .collect::<Vec<_>>()
                    .join("|")
            ));
        };
        if r == MemberRole::Unknown {
            return Err("member_roles cannot include 'unknown'".to_string());
        }
        if !out.contains(&r) {
            out.push(r);
        }
    }
    Ok(Some(out))
}

async fn index(State(st): State<Arc<AppState>>, req: Request<Body>) -> Response {
    // English note:
    // - Fluxon Ops exposes a single public HTTP entry via fluxon_cli.
    // - The user-facing homepage should directly open the ops panel.
    // - The cluster_name is explicitly configured in monitor config (not a fallback default).
    redirect_to_registered_panel(
        crate::OPS_PANEL_SERVICE_NAME,
        &st.cfg.cluster_name,
        req.uri(),
    )
}

async fn landing(State(st): State<Arc<AppState>>) -> Response {
    let mut etcd = match EtcdClient::connect(st.cfg.etcd_endpoints.clone(), None).await {
        Ok(c) => c,
        Err(e) => {
            return text_response(
                StatusCode::BAD_GATEWAY,
                format!("etcd connect failed: {}", e),
            );
        }
    };
    let clusters = match list_clusters(&mut etcd).await {
        Ok(v) => v,
        Err(e) => {
            return text_response(
                StatusCode::BAD_GATEWAY,
                format!("etcd list clusters failed: {}", e),
            );
        }
    };
    let html = crate::web_renderer::render_landing_page(&clusters);
    Html(inject_auto_refresh_tool(html)).into_response()
}

async fn api_clusters(State(st): State<Arc<AppState>>) -> Response {
    let mut etcd = match EtcdClient::connect(st.cfg.etcd_endpoints.clone(), None).await {
        Ok(c) => c,
        Err(e) => {
            return text_response(
                StatusCode::BAD_GATEWAY,
                format!("etcd connect failed: {}", e),
            );
        }
    };
    let clusters = match list_clusters(&mut etcd).await {
        Ok(v) => v,
        Err(e) => {
            return text_response(
                StatusCode::BAD_GATEWAY,
                format!("etcd list clusters failed: {}", e),
            );
        }
    };
    let mut resp = (StatusCode::OK, Json(ClustersResponse { clusters })).into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/json; charset=utf-8".parse().unwrap(),
    );
    resp
}

async fn topology_page(
    State(st): State<Arc<AppState>>,
    Query(q): Query<TopologyQuery>,
) -> Response {
    let Some(cluster_name) = q.cluster_name.as_ref() else {
        return text_response(
            StatusCode::BAD_REQUEST,
            "missing query param: cluster_name".to_string(),
        );
    };

    let prom_query_time_s: Option<f64> = match q.at.as_ref() {
        None => None,
        Some(raw) => {
            let t = raw.trim();
            if t.is_empty() {
                return text_response(
                    StatusCode::BAD_REQUEST,
                    "invalid query param: at (expected unix seconds, got empty string)".to_string(),
                );
            }
            let n = match t.parse::<f64>() {
                Ok(v) => v,
                Err(e) => {
                    return text_response(
                        StatusCode::BAD_REQUEST,
                        format!(
                            "invalid query param: at (expected unix seconds as float, got {:?}): {}",
                            t, e
                        ),
                    );
                }
            };
            if !n.is_finite() || n <= 0.0 {
                return text_response(
                    StatusCode::BAD_REQUEST,
                    format!(
                        "invalid query param: at (expected unix seconds > 0, got {})",
                        n
                    ),
                );
            }
            let now_s = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
                Ok(v) => v.as_secs_f64(),
                Err(e) => {
                    return text_response(
                        StatusCode::BAD_GATEWAY,
                        format!("system clock before UNIX_EPOCH: {}", e),
                    );
                }
            };
            if n > now_s + 1.0 {
                return text_response(
                    StatusCode::BAD_REQUEST,
                    format!(
                        "invalid query param: at (must be <= now_s+1, got at={} now_s={:.3})",
                        n, now_s
                    ),
                );
            }
            Some(n)
        }
    };

    let cfg2 = MonitorConfig {
        etcd_endpoints: st.cfg.etcd_endpoints.clone(),
        prometheus_base_url: st.cfg.prometheus_base_url.clone(),
        cluster_name: cluster_name.clone(),
        member_kind: MemberKind::Kv,
        output: OutputFormat::Web,
        mq_unique_key_prefixes: st.cfg.mq_unique_key_prefixes.clone(),
        http_listen_addr: st.cfg.http_listen_addr.clone(),
        greptime_sql: st.cfg.greptime_sql.clone(),
    };

    match crate::build_cluster_snapshot_with_prom_query_time(&cfg2, prom_query_time_s).await {
        Ok(mut snapshot) => {
            // English note:
            // - Topology is rendered from KV membership as the primary tree.
            // - MQ producer/consumer are optional child nodes (best-effort).
            let mut mq_cfg = cfg2.clone();
            mq_cfg.member_kind = MemberKind::Mq;
            match crate::build_cluster_snapshot_with_prom_query_time(&mq_cfg, prom_query_time_s)
                .await
            {
                Ok(mq_snapshot) => {
                    snapshot.warnings.extend(mq_snapshot.warnings);
                    snapshot.mq = mq_snapshot.mq;
                }
                Err(e) => {
                    snapshot
                        .warnings
                        .push(format!("topology: build mq snapshot failed (ignored): {e}"));
                }
            }

            // FS mount registry (best-effort).
            //
            // English note:
            // - FS master publishes mount registry to etcd under:
            //   `/fluxon_fs_mount_registry/{cluster}/mounts/{external_instance_key}/{mount_id}`.
            // - We filter by "online member ids" from the KV snapshot to avoid showing stale mounts.
            let mut online_member_ids: std::collections::BTreeSet<String> =
                std::collections::BTreeSet::new();
            for n in &snapshot.nodes {
                for m in &n.members {
                    online_member_ids.insert(m.member_id.clone());
                }
            }
            if !online_member_ids.is_empty() {
                #[derive(serde::Deserialize)]
                struct FsMountRegistryRecordWire {
                    external_instance_key: String,
                    local_mount_dir_abs: String,
                    remote_root_dir_abs: String,
                    updated_unix_ms: i64,
                }

                let prefix = format!("/fluxon_fs_mount_registry/{}/mounts/", cluster_name);
                match EtcdClient::connect(st.cfg.etcd_endpoints.clone(), None).await {
                    Ok(mut etcd) => {
                        let mut mounts: Vec<crate::model::FsMountRegistryRecordSnapshot> =
                            Vec::new();
                        match scan_etcd_prefix_paginated(&mut etcd, &prefix, |_key, raw| {
                            let rec: FsMountRegistryRecordWire =
                                match serde_json::from_slice(raw) {
                                    Ok(v) => v,
                                    Err(e) => {
                                        snapshot.warnings.push(format!(
                                            "topology: invalid fs mount registry json under prefix={} err={} value={}",
                                            prefix,
                                            e,
                                            String::from_utf8_lossy(raw)
                                        ));
                                        return Ok::<
                                            EtcdPrefixScanAction,
                                            std::convert::Infallible,
                                        >(EtcdPrefixScanAction::Continue);
                                    }
                                };
                            if !online_member_ids.contains(rec.external_instance_key.as_str()) {
                                return Ok::<EtcdPrefixScanAction, std::convert::Infallible>(
                                    EtcdPrefixScanAction::Continue,
                                );
                            }
                            mounts.push(crate::model::FsMountRegistryRecordSnapshot {
                                external_instance_key: rec.external_instance_key,
                                local_mount_dir_abs: rec.local_mount_dir_abs,
                                remote_root_dir_abs: rec.remote_root_dir_abs,
                                updated_unix_ms: rec.updated_unix_ms,
                            });
                            Ok::<EtcdPrefixScanAction, std::convert::Infallible>(
                                EtcdPrefixScanAction::Continue,
                            )
                        })
                        .await
                        {
                            Ok(()) => {
                                mounts.sort_by(|a, b| {
                                    let c = a.external_instance_key.cmp(&b.external_instance_key);
                                    if c != std::cmp::Ordering::Equal {
                                        return c;
                                    }
                                    a.local_mount_dir_abs.cmp(&b.local_mount_dir_abs)
                                });
                                snapshot.fs_mount_registry = mounts;
                            }
                            Err(e) => {
                                snapshot
                                    .warnings
                                    .push(format!("topology: etcd get fs mount registry failed (ignored): prefix={} err={}", prefix, e));
                            }
                        }
                    }
                    Err(e) => {
                        snapshot.warnings.push(format!(
                            "topology: etcd connect failed for fs mount registry (ignored): {e}"
                        ));
                    }
                }
            }

            // FS export registry (best-effort).
            //
            // English note:
            // - FS master persists a full export-registry snapshot to etcd under:
            //   `/fluxon_fs_export_registry/{cluster}/snapshot`.
            // - We still filter by "online member ids" from the KV snapshot to keep the UI stable
            //   if etcd contains a stale snapshot (e.g. after a forced restart).
            if !online_member_ids.is_empty() {
                #[derive(serde::Deserialize)]
                struct FsExportRegistrySnapshotWire {
                    schema_version: i64,
                    updated_unix_ms: i64,
                    records: Vec<FsExportRegistryRecordWire>,
                }

                #[derive(serde::Deserialize)]
                struct FsExportRegistryRecordWire {
                    export_name: String,
                    agent_instance_key: String,
                    remote_root_dir_abs: String,
                    updated_unix_ms: i64,
                }

                let key = format!("/fluxon_fs_export_registry/{}/snapshot", cluster_name);
                match EtcdClient::connect(st.cfg.etcd_endpoints.clone(), None).await {
                    Ok(mut etcd) => match etcd.get(key.clone(), None).await {
                        Ok(resp) => {
                            let mut exports: Vec<crate::model::FsExportRegistryRecordSnapshot> =
                                Vec::new();
                            if let Some(kv) = resp.kvs().first() {
                                let raw = kv.value();
                                match serde_json::from_slice::<FsExportRegistrySnapshotWire>(raw) {
                                    Ok(snap) => {
                                        let _ = snap.schema_version;
                                        let _ = snap.updated_unix_ms;
                                        for rec in snap.records {
                                            if !online_member_ids
                                                .contains(rec.agent_instance_key.as_str())
                                            {
                                                continue;
                                            }
                                            exports.push(
                                                crate::model::FsExportRegistryRecordSnapshot {
                                                    export_name: rec.export_name,
                                                    agent_instance_key: rec.agent_instance_key,
                                                    remote_root_dir_abs: rec.remote_root_dir_abs,
                                                    updated_unix_ms: rec.updated_unix_ms,
                                                },
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        snapshot.warnings.push(format!(
                                            "topology: invalid fs export registry snapshot json under key={} err={} value={}",
                                            key,
                                            e,
                                            String::from_utf8_lossy(raw)
                                        ));
                                    }
                                }
                            }
                            exports.sort_by(|a, b| {
                                let c = a.export_name.cmp(&b.export_name);
                                if c != std::cmp::Ordering::Equal {
                                    return c;
                                }
                                a.agent_instance_key.cmp(&b.agent_instance_key)
                            });
                            snapshot.fs_export_registry = exports;
                        }
                        Err(e) => {
                            snapshot
                                .warnings
                                .push(format!("topology: etcd get fs export registry snapshot failed (ignored): key={} err={}", key, e));
                        }
                    },
                    Err(e) => {
                        snapshot.warnings.push(format!(
                            "topology: etcd connect failed for fs export registry (ignored): {e}"
                        ));
                    }
                }
            }

            let html = crate::web_renderer::render_topology_page(&snapshot);
            Html(inject_auto_refresh_tool(html)).into_response()
        }
        Err(e) => text_response(
            StatusCode::BAD_GATEWAY,
            format!("snapshot build failed: {}", e),
        ),
    }
}

async fn ops_fluxon_kv_view(
    State(st): State<Arc<AppState>>,
    Path(cluster_name): Path<String>,
    uri: axum::http::Uri,
    Query(q): Query<OpsFluxonClusterQuery>,
) -> Response {
    view(
        State(st),
        uri,
        Query(ClusterQuery {
            cluster_name: Some(cluster_name),
            member_kind: Some(MemberKind::Kv.as_query_str().to_string()),
            member_roles: q.member_roles,
        }),
    )
    .await
}

async fn ops_fluxon_kv_update_rdma_devices(
    State(st): State<Arc<AppState>>,
    Path(cluster_name): Path<String>,
    req: Request<Body>,
) -> Response {
    fn normalize_enabled_devices(values: Vec<String>) -> Vec<String> {
        let mut deduped = std::collections::BTreeSet::new();
        for value in values {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                continue;
            }
            deduped.insert(trimmed.to_string());
        }
        deduped.into_iter().collect()
    }

    let body = match hyper::body::to_bytes(req.into_body()).await {
        Ok(body) => body,
        Err(err) => {
            return text_response(
                StatusCode::BAD_REQUEST,
                format!("read rdma device update form body failed: {}", err),
            );
        }
    };
    let form = match parse_rdma_device_update_form(&body) {
        Ok(form) => form,
        Err(err) => {
            return text_response(
                StatusCode::BAD_REQUEST,
                format!("invalid rdma device update form: {}", err),
            );
        }
    };

    let mut etcd = match EtcdClient::connect(st.cfg.etcd_endpoints.clone(), None).await {
        Ok(client) => client,
        Err(err) => {
            return text_response(
                StatusCode::BAD_GATEWAY,
                format!("connect etcd failed: {}", err),
            );
        }
    };

    let base_key = cluster_member_base_key(&cluster_name, &form.member_id);
    let base_resp = match etcd.get(base_key.clone(), None).await {
        Ok(resp) => resp,
        Err(err) => {
            return text_response(
                StatusCode::BAD_GATEWAY,
                format!("get member base failed: key={} err={}", base_key, err),
            );
        }
    };
    let Some(base_kv) = base_resp.kvs().first() else {
        return text_response(
            StatusCode::NOT_FOUND,
            format!("member not found: key={}", base_key),
        );
    };
    let member: ClusterMember = match serde_json::from_slice(base_kv.value()) {
        Ok(member) => member,
        Err(err) => {
            return text_response(
                StatusCode::BAD_GATEWAY,
                format!("parse member base failed: key={} err={}", base_key, err),
            );
        }
    };
    if member.node_role() != NodeRole::Client {
        return text_response(
            StatusCode::BAD_REQUEST,
            format!("RDMA control is owner-only: member_id={}", form.member_id),
        );
    }
    if member.node_start_time != form.member_start_time {
        return text_response(
            StatusCode::CONFLICT,
            format!(
                "member start_time changed: member_id={} expected={} actual={}",
                form.member_id, form.member_start_time, member.node_start_time
            ),
        );
    }

    let control_key = cluster_owner_rdma_control_key(&cluster_name, &form.member_id);
    if let Err(err) = etcd.get(control_key.clone(), None).await {
        return text_response(
            StatusCode::BAD_GATEWAY,
            format!("get rdma_control failed: key={} err={}", control_key, err),
        );
    }
    let enabled_devices = normalize_enabled_devices(form.enabled_devices);

    let control = MemberRdmaControl {
        node_start_time: member.node_start_time,
        machine_hostname: MemberRdmaControl::machine_hostname_from_metadata(&member.metadata),
        machine_product_uuid: MemberRdmaControl::machine_product_uuid_from_metadata(
            &member.metadata,
        ),
        enabled_devices,
    };
    let payload = match serde_json::to_string(&control) {
        Ok(payload) => payload,
        Err(err) => {
            return text_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("serialize rdma_control failed: {}", err),
            );
        }
    };
    if let Err(err) = etcd.put(control_key, payload, None).await {
        return text_response(
            StatusCode::BAD_GATEWAY,
            format!("put rdma_control failed: {}", err),
        );
    }

    (
        StatusCode::SEE_OTHER,
        [(
            header::LOCATION,
            format!("/r/ops/{}/fluxon/kv", cluster_name),
        )],
    )
        .into_response()
}

async fn ops_fluxon_entry(Path(cluster_name): Path<String>, req: Request<Body>) -> Response {
    let mut target = format!("/r/ops/{}/fluxon/kv", cluster_name);
    if let Some(qs) = req.uri().query() {
        target.push('?');
        target.push_str(qs);
    }
    Redirect::temporary(&target).into_response()
}

async fn ops_fluxon_mq_view(
    State(st): State<Arc<AppState>>,
    Path(cluster_name): Path<String>,
    uri: axum::http::Uri,
    Query(q): Query<OpsFluxonClusterQuery>,
) -> Response {
    view(
        State(st),
        uri,
        Query(ClusterQuery {
            cluster_name: Some(cluster_name),
            member_kind: Some(MemberKind::Mq.as_query_str().to_string()),
            member_roles: q.member_roles,
        }),
    )
    .await
}

async fn ops_fluxon_topology_view(
    State(st): State<Arc<AppState>>,
    Path(cluster_name): Path<String>,
    Query(q): Query<OpsFluxonTopologyQuery>,
) -> Response {
    topology_page(
        State(st),
        Query(TopologyQuery {
            cluster_name: Some(cluster_name),
            at: q.at,
        }),
    )
    .await
}

async fn ops_fluxon_fs_entry(Path(cluster_name): Path<String>, req: Request<Body>) -> Response {
    let mut target = format!("/r/ops/{}/fluxon/fs/", cluster_name);
    if let Some(qs) = req.uri().query() {
        target.push('?');
        target.push_str(qs);
    }
    Redirect::temporary(&target).into_response()
}

async fn ops_fluxon_fs_root(
    State(st): State<Arc<AppState>>,
    Path(cluster_name): Path<String>,
    req: Request<Body>,
) -> Response {
    proxy_registered_service_impl(&st, "fs_s3", &cluster_name, "ui/", req).await
}

async fn ops_fluxon_fs_rest(
    State(st): State<Arc<AppState>>,
    Path((cluster_name, rest)): Path<(String, String)>,
    req: Request<Body>,
) -> Response {
    let rest = format!("ui/{}", rest.trim_start_matches('/'));
    proxy_registered_service_impl(&st, "fs_s3", &cluster_name, &rest, req).await
}

async fn view(
    State(st): State<Arc<AppState>>,
    uri: axum::http::Uri,
    Query(q): Query<ClusterQuery>,
) -> Response {
    let Some(cluster_name) = q.cluster_name.as_ref() else {
        return text_response(
            StatusCode::BAD_REQUEST,
            "missing query param: cluster_name".to_string(),
        );
    };
    let Some(member_kind_raw) = q.member_kind.as_ref() else {
        return text_response(
            StatusCode::BAD_REQUEST,
            format!(
                "missing query param: member_kind ({})",
                available_member_kind_query_strs().join("|")
            ),
        );
    };
    let Some(member_kind) = parse_member_kind(member_kind_raw) else {
        return text_response(
            StatusCode::BAD_REQUEST,
            format!("invalid member_kind: {}", member_kind_raw),
        );
    };

    if member_kind == MemberKind::Fs {
        return redirect_to_registered_panel("fs_s3", cluster_name, &uri);
    }

    let visible_member_roles = match parse_member_roles_query(&q) {
        Ok(v) => v,
        Err(e) => return text_response(StatusCode::BAD_REQUEST, e),
    };
    let cfg2 = MonitorConfig {
        etcd_endpoints: st.cfg.etcd_endpoints.clone(),
        prometheus_base_url: st.cfg.prometheus_base_url.clone(),
        cluster_name: cluster_name.clone(),
        member_kind,
        output: OutputFormat::Web,
        mq_unique_key_prefixes: st.cfg.mq_unique_key_prefixes.clone(),
        http_listen_addr: st.cfg.http_listen_addr.clone(),
        greptime_sql: st.cfg.greptime_sql.clone(),
    };

    match crate::build_cluster_snapshot(&cfg2).await {
        Ok(mut snapshot) => {
            snapshot.visible_member_roles = visible_member_roles;
            let html = crate::web_renderer::render_cluster(&snapshot);
            Html(inject_auto_refresh_tool(html)).into_response()
        }
        Err(e) => text_response(
            StatusCode::BAD_GATEWAY,
            format!("snapshot build failed: {}", e),
        ),
    }
}

async fn cli(
    State(st): State<Arc<AppState>>,
    uri: axum::http::Uri,
    Query(q): Query<ClusterQuery>,
) -> Response {
    let Some(cluster_name) = q.cluster_name.as_ref() else {
        let mut etcd = match EtcdClient::connect(st.cfg.etcd_endpoints.clone(), None).await {
            Ok(c) => c,
            Err(e) => {
                return text_response(
                    StatusCode::BAD_REQUEST,
                    format!(
                        "missing query param: cluster_name\n(etcd connect failed while listing clusters: {})\n\nUsage:\n  /cli?cluster_name=<name>&member_kind={}\n  /api/clusters\n",
                        e,
                        available_member_kind_query_strs().join("|")
                    ),
                );
            }
        };
        let clusters = match list_clusters(&mut etcd).await {
            Ok(v) => v,
            Err(e) => {
                return text_response(
                    StatusCode::BAD_REQUEST,
                    format!(
                        "missing query param: cluster_name\n(etcd list clusters failed: {})\n\nUsage:\n  /cli?cluster_name=<name>&member_kind={}\n  /api/clusters\n",
                        e,
                        available_member_kind_query_strs().join("|")
                    ),
                );
            }
        };
        let mut body = String::new();
        body.push_str("missing query param: cluster_name\n\nAvailable clusters:\n");
        for c in clusters {
            body.push_str(&format!("  - {}\n", c));
        }
        body.push_str(&format!(
            "\nUsage:\n  /cli?cluster_name=<name>&member_kind={}\n  /api/clusters\n",
            available_member_kind_query_strs().join("|")
        ));
        return text_response(StatusCode::BAD_REQUEST, body);
    };

    let Some(member_kind_raw) = q.member_kind.as_ref() else {
        return text_response(
            StatusCode::BAD_REQUEST,
            "missing query param: member_kind\nUsage:\n  /cli?cluster_name=<name>&member_kind=<kind>\n  /api/clusters\n".to_string(),
        );
    };
    let Some(member_kind) = parse_member_kind(member_kind_raw) else {
        return text_response(
            StatusCode::BAD_REQUEST,
            format!("invalid member_kind: {}", member_kind_raw),
        );
    };

    if member_kind == MemberKind::Fs {
        return redirect_to_registered_panel("fs_s3", cluster_name, &uri);
    }

    let visible_member_roles = match parse_member_roles_query(&q) {
        Ok(v) => v,
        Err(e) => return text_response(StatusCode::BAD_REQUEST, e),
    };

    let cfg2 = MonitorConfig {
        etcd_endpoints: st.cfg.etcd_endpoints.clone(),
        prometheus_base_url: st.cfg.prometheus_base_url.clone(),
        cluster_name: cluster_name.clone(),
        member_kind,
        output: OutputFormat::Cli,
        mq_unique_key_prefixes: st.cfg.mq_unique_key_prefixes.clone(),
        http_listen_addr: st.cfg.http_listen_addr.clone(),
        greptime_sql: st.cfg.greptime_sql.clone(),
    };

    match crate::build_cluster_snapshot(&cfg2).await {
        Ok(mut snapshot) => {
            snapshot.visible_member_roles = visible_member_roles;
            text_response(
                StatusCode::OK,
                crate::cli_renderer::render_cluster(&snapshot),
            )
        }
        Err(e) => text_response(
            StatusCode::BAD_GATEWAY,
            format!("snapshot build failed: {}", e),
        ),
    }
}

#[derive(serde::Deserialize)]
struct LogsApiQuery {
    cluster_name: Option<String>,
    member_kind: Option<String>,
    role: Option<String>,
    member_id: Option<String>,
    // Keep timestamps as strings because Greptime TimestampNanosecond values exceed JS Number precision.
    // We parse them manually into i64 for SQL.
    before_ts: Option<String>,
    after_ts: Option<String>,
    log_table: Option<String>,
    search: Option<String>,
    /// all|info|warn|error
    level: Option<String>,
}

#[derive(serde::Serialize)]
struct LogItem {
    /// Nanoseconds since Unix epoch, encoded as string to preserve precision in JS.
    ts: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    member_id: Option<String>,
    body: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    severity_text: Option<String>,
}

#[derive(serde::Serialize)]
struct LogsApiResponse {
    items: Vec<LogItem>,
    next_before_ts: Option<String>,
}

fn is_safe_ident(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn sql_quote_literal(s: &str) -> String {
    let escaped = s.replace("'", "''");
    format!("'{}'", escaped)
}

async fn logs_page(Query(q): Query<LogsApiQuery>) -> Response {
    // Logs page is CSR-only: it does not embed any log lines in HTML.
    // The frontend fetches /api/logs based on current query params:
    // - default: load latest batch and stick to bottom (follow)
    // - scroll up: lazy-load older logs
    // - while at bottom: periodically tail new logs
    let cluster = q.cluster_name.unwrap_or_default();
    let kind = q.member_kind.unwrap_or_default();
    let html = format!(
        r#"<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Fluxon Logs</title>
  <style>
    body{{font-family:ui-sans-serif,system-ui,Segoe UI,Roboto,Helvetica,Arial;max-width:1200px;margin:18px auto;padding:0 12px;}}
    .mono{{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;}}
    #box{{height:78vh; overflow:auto; border:1px solid #e5e7eb; border-radius:10px; padding:10px; background:#0b1020; color:#e5e7eb;}}
    .line{{white-space:pre-wrap; word-break:break-word; border-bottom:1px solid rgba(255,255,255,0.06); padding:4px 0;}}
    .hdr{{margin:0 0 10px 0;}}
    .muted{{color:#94a3b8;}}
    .bad{{color:#fca5a5;}}
    .btn{{border:1px solid rgba(148,163,184,0.35); background:transparent; color:#e5e7eb; padding:4px 8px; border-radius:8px; cursor:pointer;}}
    .btn-on{{border-color:#60a5fa;}}
    .inp{{border:1px solid rgba(148,163,184,0.35); background:#0b1020; color:#e5e7eb; padding:6px 8px; border-radius:8px;}}
    .pill{{display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px; border:1px solid rgba(148,163,184,0.35);}}
    .pill-trace{{color:#cbd5e1;}}
    .pill-debug{{color:#93c5fd;}}
    .pill-info{{color:#86efac;}}
    .pill-warn{{color:#fde68a;}}
    .pill-error{{color:#fca5a5;}}
    .tok-member{{color:#60a5fa;}}
    .tok-target{{color:#a78bfa;}}
    .tok-loc{{color:#94a3b8;}}
  </style>
</head>
<body>
  <div class="hdr">
    <div>cluster_name: <span class="mono">{}</span></div>
    <div>member_kind: <span class="mono">{}</span></div>
    <div>log_table: <span class="mono" id="tbl"></span></div>
    <div class="muted">
      Default: follow latest logs (stick to bottom). Scroll up to load older logs.
    </div>
    <div style="margin-top:8px; display:flex; gap:8px; flex-wrap:wrap; align-items:center;">
      <input id="q" class="inp mono" style="min-width:260px;" placeholder="Search (body contains)..." />
      <button id="lvl_all" class="btn mono">All</button>
      <button id="lvl_info" class="btn mono">Info+</button>
      <button id="lvl_warn" class="btn mono">Warn+</button>
      <button id="lvl_error" class="btn mono">Error</button>
      <button id="follow" class="btn mono btn-on">Follow</button>
      <button id="bottom" class="btn mono">Bottom</button>
      <span id="status" class="muted mono"></span>
    </div>
  </div>
  <div id="tblbox" style="display:none; margin:0 0 10px 0;">
    <div class="muted">Pick a GreptimeDB table to load logs:</div>
    <select id="tblsel" class="mono"></select>
    <button id="tblload">Load</button>
  </div>
  <div id="err" class="bad"></div>
  <div id="box"></div>
  <script>
  (() => {{
    const box = document.getElementById('box');
    const err = document.getElementById('err');
    const tbl = document.getElementById('tbl');
    const tblbox = document.getElementById('tblbox');
    const tblsel = document.getElementById('tblsel');
    const tblload = document.getElementById('tblload');
    const qinp = document.getElementById('q');
    const btnAll = document.getElementById('lvl_all');
    const btnInfo = document.getElementById('lvl_info');
    const btnWarn = document.getElementById('lvl_warn');
    const btnError = document.getElementById('lvl_error');
    const btnFollow = document.getElementById('follow');
    const btnBottom = document.getElementById('bottom');
    const status = document.getElementById('status');

    let loading = false;
    let oldestTs = null; // for loading older (before_ts)
    let newestTs = null; // for tailing new (after_ts)
    let noMoreOlder = false;
    let follow = true;

    const TAIL_INTERVAL_MS = 2000;
    const BOTTOM_THRESHOLD_PX = 50;

    const qs = new URLSearchParams(window.location.search);
    const buildApiUrl = (mode) => {{
      const p = new URLSearchParams();
      ['cluster_name','member_kind','role','member_id','log_table','search','level'].forEach((k) => {{
        const v = qs.get(k);
        if (v && v.length > 0) p.set(k, v);
      }});
      if (mode === 'older' && oldestTs !== null) p.set('before_ts', String(oldestTs));
      if (mode === 'tail' && newestTs !== null) p.set('after_ts', String(newestTs));
      return '/api/logs?' + p.toString();
    }};

    const showTablePicker = async () => {{
      tblbox.style.display = 'block';
      try {{
        const resp = await fetch('/api/log_tables', {{ cache: 'no-store' }});
        const text = await resp.text();
        if (!resp.ok) {{
          err.textContent = text;
          done = true;
          return;
        }}
        const data = JSON.parse(text);
        const tables = (data && data.tables) ? data.tables : [];
        tblsel.innerHTML = '';
        for (const t of tables) {{
          const opt = document.createElement('option');
          opt.value = String(t);
          opt.textContent = String(t);
          tblsel.appendChild(opt);
        }}
        tblload.onclick = () => {{
          const v = tblsel.value;
          if (!v || v.length === 0) return;
          qs.set('log_table', v);
          const u = new URL(window.location.href);
          u.search = qs.toString();
          window.location.href = u.toString();
        }};
      }} catch (e) {{
        err.textContent = String(e);
        done = true;
      }}
    }};

    const isNearBottom = () => {{
      const d = box.scrollHeight - (box.scrollTop + box.clientHeight);
      return d <= BOTTOM_THRESHOLD_PX;
    }};

    const updateStatus = () => {{
      status.textContent = follow ? 'following' : 'paused';
      if (follow) {{
        btnFollow.classList.add('btn-on');
      }} else {{
        btnFollow.classList.remove('btn-on');
      }}
    }};

    const clearAll = () => {{
      box.innerHTML = '';
      err.textContent = '';
      oldestTs = null;
      newestTs = null;
      noMoreOlder = false;
    }};

    const normalizeLevel = (s) => {{
      if (!s) return null;
      const v = String(s).trim().toUpperCase();
      if (v === 'TRACE' || v === 'DEBUG' || v === 'INFO' || v === 'WARN' || v === 'ERROR') return v;
      return null;
    }};

    const pillClassForLevel = (lvl) => {{
      switch (lvl) {{
        case 'TRACE': return 'pill pill-trace';
        case 'DEBUG': return 'pill pill-debug';
        case 'INFO': return 'pill pill-info';
        case 'WARN': return 'pill pill-warn';
        case 'ERROR': return 'pill pill-error';
        default: return 'pill';
      }}
    }};

    const parseBodyForHighlight = (raw) => {{
      const s = String(raw || '');
      const sp = s.indexOf(' ');
      if (sp <= 0) return {{ lvl: null, rest: s, target: null, loc: null, msg: null }};
      const lvl = normalizeLevel(s.slice(0, sp));
      const rest0 = (lvl ? s.slice(sp + 1) : s);

      // We generate lines as: "<LEVEL> <target> (file:line): <message> k=v..."
      // Highlight target and (file:line) when present, keep the remainder as message.
      const colon = rest0.indexOf(': ');
      const head = colon >= 0 ? rest0.slice(0, colon) : rest0;
      const msg = colon >= 0 ? rest0.slice(colon + 2) : '';
      let target = head;
      let loc = null;
      const lp = head.indexOf(' (');
      if (lp >= 0 && head.endsWith(')')) {{
        target = head.slice(0, lp);
        loc = head.slice(lp + 1).trim();
      }}
      return {{ lvl, rest: rest0, target, loc, msg }};
    }};

    const appendLines = (items, mode, stickToBottom) => {{
      const atTop = box.scrollTop;
      const oldHeight = box.scrollHeight;
      const frag = document.createDocumentFragment();
      for (const it of items) {{
        const div = document.createElement('div');
        div.className = 'line mono';
        const parts = parseBodyForHighlight(it.body);
        const lvl = normalizeLevel(it.severity_text) || parts.lvl;

        const tsSpan = document.createElement('span');
        tsSpan.className = 'muted';
        tsSpan.textContent = '[' + String(it.ts) + '] ';
        div.appendChild(tsSpan);

        if (it.member_id && String(it.member_id).length > 0) {{
          const mid = document.createElement('span');
          mid.className = 'tok-member';
          mid.textContent = String(it.member_id);
          div.appendChild(mid);
          div.appendChild(document.createTextNode(' '));
        }}

        if (lvl) {{
          const pill = document.createElement('span');
          pill.className = pillClassForLevel(lvl);
          pill.textContent = lvl;
          div.appendChild(pill);
          div.appendChild(document.createTextNode(' '));
        }}

        // Avoid repeating the level token when we already show the pill.
        let bodyRest = String(it.body || '');
        if (lvl && bodyRest.startsWith(lvl + ' ')) {{
          bodyRest = bodyRest.slice(lvl.length + 1);
        }}
        const hl = parseBodyForHighlight((lvl ? (lvl + ' ' + bodyRest) : bodyRest));

        if (hl.target) {{
          const t = document.createElement('span');
          t.className = 'tok-target';
          t.textContent = hl.target;
          div.appendChild(t);
        }} else {{
          div.appendChild(document.createTextNode(bodyRest));
          frag.appendChild(div);
          continue;
        }}

        if (hl.loc) {{
          div.appendChild(document.createTextNode(' '));
          const loc = document.createElement('span');
          loc.className = 'tok-loc';
          loc.textContent = hl.loc;
          div.appendChild(loc);
        }}

        if (hl.msg && hl.msg.length > 0) {{
          div.appendChild(document.createTextNode(': ' + hl.msg));
        }}
        frag.appendChild(div);
      }}
      if (mode === 'prepend') {{
        box.prepend(frag);
        const newHeight = box.scrollHeight;
        box.scrollTop = atTop + (newHeight - oldHeight);
      }} else {{
        box.appendChild(frag);
      }}
      if (stickToBottom) {{
        box.scrollTop = box.scrollHeight;
      }}
    }};

    const loadLatestOnce = async () => {{
      if (loading) return;
      loading = true;
      err.textContent = '';
      try {{
        const resp = await fetch(buildApiUrl('latest'), {{ cache: 'no-store' }});
        const text = await resp.text();
        if (!resp.ok) {{
          err.textContent = text;
          if (resp.status === 400 && text.includes('missing log_table')) {{
            await showTablePicker();
          }}
          return;
        }}
        const data = JSON.parse(text);
        if (!data.items || data.items.length === 0) {{
          return;
        }}
        // API returns latest logs in DESC order (newest first). Display in ASC order.
        const items = data.items.slice().reverse();
        appendLines(items, 'append', true);
        oldestTs = items[0].ts;
        newestTs = items[items.length - 1].ts;
      }} catch (e) {{
        err.textContent = String(e);
      }} finally {{
        loading = false;
      }}
    }};

    const loadOlderOnce = async () => {{
      if (loading || noMoreOlder) return;
      if (oldestTs === null) return;
      loading = true;
      err.textContent = '';
      try {{
        const resp = await fetch(buildApiUrl('older'), {{ cache: 'no-store' }});
        const text = await resp.text();
        if (!resp.ok) {{
          err.textContent = text;
          return;
        }}
        const data = JSON.parse(text);
        if (!data.items || data.items.length === 0) {{
          noMoreOlder = true;
          return;
        }}
        // Older fetch uses DESC order. Normalize to ASC before prepending.
        const items = data.items.slice().reverse();
        const stick = follow && isNearBottom();
        appendLines(items, 'prepend', stick);
        oldestTs = items[0].ts;
        if (newestTs === null) newestTs = items[items.length - 1].ts;
      }} catch (e) {{
        err.textContent = String(e);
      }} finally {{
        loading = false;
      }}
    }};

    const tailNewOnce = async () => {{
      if (loading) return;
      if (!follow) return;
      if (!isNearBottom()) return;
      if (newestTs === null) return;
      loading = true;
      err.textContent = '';
      try {{
        const resp = await fetch(buildApiUrl('tail'), {{ cache: 'no-store' }});
        const text = await resp.text();
        if (!resp.ok) {{
          err.textContent = text;
          return;
        }}
        const data = JSON.parse(text);
        if (!data.items || data.items.length === 0) {{
          return;
        }}
        // Tail uses ASC order.
        appendLines(data.items, 'append', true);
        newestTs = data.items[data.items.length - 1].ts;
        if (oldestTs === null) oldestTs = data.items[0].ts;
      }} catch (e) {{
        err.textContent = String(e);
      }} finally {{
        loading = false;
      }}
    }};

    box.addEventListener('scroll', async () => {{
      // Leaving the bottom pauses follow to avoid scroll jumping.
      if (follow && !isNearBottom()) {{
        follow = false;
        updateStatus();
      }}
      // When user scrolls back to bottom, resume follow.
      if (!follow && isNearBottom()) {{
        follow = true;
        updateStatus();
      }}
      if (box.scrollTop < 80) {{
        await loadOlderOnce();
      }}
    }});

    const setLevel = (lvl) => {{
      if (!lvl || lvl.length === 0) {{
        qs.delete('level');
      }} else {{
        qs.set('level', lvl);
      }}
      const u = new URL(window.location.href);
      u.search = qs.toString();
      window.history.replaceState(null, '', u.toString());
    }};

    const refreshLevelButtons = () => {{
      const v = (qs.get('level') || 'all').trim();
      btnAll.classList.toggle('btn-on', v === 'all' || v.length === 0);
      btnInfo.classList.toggle('btn-on', v === 'info');
      btnWarn.classList.toggle('btn-on', v === 'warn');
      btnError.classList.toggle('btn-on', v === 'error');
    }};

    btnAll.onclick = () => {{ setLevel('all'); refreshLevelButtons(); clearAll(); loadLatestOnce(); }};
    btnInfo.onclick = () => {{ setLevel('info'); refreshLevelButtons(); clearAll(); loadLatestOnce(); }};
    btnWarn.onclick = () => {{ setLevel('warn'); refreshLevelButtons(); clearAll(); loadLatestOnce(); }};
    btnError.onclick = () => {{ setLevel('error'); refreshLevelButtons(); clearAll(); loadLatestOnce(); }};

    const applySearch = () => {{
      const v = (qinp.value || '').trim();
      if (v.length === 0) {{
        qs.delete('search');
      }} else {{
        qs.set('search', v);
      }}
      const u = new URL(window.location.href);
      u.search = qs.toString();
      window.history.replaceState(null, '', u.toString());
      clearAll();
      loadLatestOnce();
    }};

    qinp.addEventListener('keydown', (e) => {{
      if (e.key === 'Enter') {{
        applySearch();
      }}
    }});

    btnFollow.onclick = () => {{
      follow = !follow;
      updateStatus();
      if (follow) {{
        box.scrollTop = box.scrollHeight;
        tailNewOnce();
      }}
    }};
    btnBottom.onclick = () => {{
      box.scrollTop = box.scrollHeight;
      follow = true;
      updateStatus();
      tailNewOnce();
    }};

    tbl.textContent = (qs.get('log_table') && qs.get('log_table').length > 0) ? qs.get('log_table') : '(from config)';
    qinp.value = qs.get('search') || '';
    refreshLevelButtons();
    updateStatus();
    loadLatestOnce();
    setInterval(tailNewOnce, TAIL_INTERVAL_MS);
  }})();
  </script>
</body>
</html>
"#,
        esc(&cluster),
        esc(&kind),
    );

    Html(inject_auto_refresh_tool(html)).into_response()
}

async fn api_logs(State(st): State<Arc<AppState>>, Query(q): Query<LogsApiQuery>) -> Response {
    let Some(cluster_name) = q.cluster_name.as_ref() else {
        return text_response(
            StatusCode::BAD_REQUEST,
            "missing query param: cluster_name".to_string(),
        );
    };
    let Some(member_kind_raw) = q.member_kind.as_ref() else {
        return text_response(
            StatusCode::BAD_REQUEST,
            "missing query param: member_kind".to_string(),
        );
    };
    let Some(member_kind) = parse_member_kind(member_kind_raw) else {
        return text_response(
            StatusCode::BAD_REQUEST,
            format!("invalid member_kind: {}", member_kind_raw),
        );
    };
    let Some(role_raw) = q.role.as_ref() else {
        return text_response(
            StatusCode::BAD_REQUEST,
            "missing query param: role".to_string(),
        );
    };
    let Some(role) = crate::model::MemberRole::parse_query_str(role_raw) else {
        return text_response(
            StatusCode::BAD_REQUEST,
            format!("invalid role: {}", role_raw),
        );
    };
    if role == crate::model::MemberRole::Unknown {
        return text_response(
            StatusCode::BAD_REQUEST,
            "role cannot be unknown".to_string(),
        );
    }

    if q.before_ts.is_some() && q.after_ts.is_some() {
        return text_response(
            StatusCode::BAD_REQUEST,
            "before_ts and after_ts are mutually exclusive".to_string(),
        );
    }

    let Some(gcfg) = st.cfg.greptime_sql.as_ref() else {
        return text_response(
            StatusCode::BAD_REQUEST,
            "greptime_sql is not configured. If you are using the embedded master panel, configure master.monitoring.otlp_log_api in all_config.yaml so the panel can derive Greptime SQL base_url/db."
                .to_string(),
        );
    };

    let table = match q
        .log_table
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        Some(t) => t.to_string(),
        None => match gcfg.log_table.as_ref() {
            Some(t) => t.clone(),
            None => {
                return text_response(
                    StatusCode::BAD_REQUEST,
                    "missing log_table: set greptime_sql.log_table in config, or pass query param log_table".to_string(),
                );
            }
        },
    };
    if !is_safe_ident(&table) {
        return text_response(
            StatusCode::BAD_REQUEST,
            format!("unsafe log table name: {}", table),
        );
    }

    let sql_client = fluxon_observability::greptime_sql::GreptimeSqlClient::new(
        gcfg.base_url.clone(),
        gcfg.db.clone(),
    );

    let schema = match st.log_schema_cache.get_or_init(&sql_client, &table).await {
        Ok(v) => v,
        Err(e) => {
            return text_response(
                StatusCode::BAD_GATEWAY,
                format!("describe log table failed: {}", e),
            );
        }
    };

    if !is_safe_ident(&schema.time_column) {
        return text_response(
            StatusCode::BAD_GATEWAY,
            format!("unsafe time column name: {}", schema.time_column),
        );
    }

    // Greptime returns timestamp values as integers for Timestamp* columns (see exporter_heartbeat sample).
    if !schema.time_data_type.starts_with("Timestamp") {
        return text_response(
            StatusCode::BAD_GATEWAY,
            format!("unsupported time column type: {}", schema.time_data_type),
        );
    }

    fn parse_i64_query(label: &str, v: &str) -> Result<i64, String> {
        let t = v.trim();
        if t.is_empty() {
            return Err(format!("{} cannot be empty", label));
        }
        t.parse::<i64>()
            .map_err(|e| format!("invalid {} (expected int64): {} ({})", label, t, e))
    }

    let before_ts_i64: Option<i64> = match q.before_ts.as_ref() {
        None => None,
        Some(s) => match parse_i64_query("before_ts", s) {
            Ok(v) => Some(v),
            Err(e) => return text_response(StatusCode::BAD_REQUEST, e),
        },
    };
    let after_ts_i64: Option<i64> = match q.after_ts.as_ref() {
        None => None,
        Some(s) => match parse_i64_query("after_ts", s) {
            Ok(v) => Some(v),
            Err(e) => return text_response(StatusCode::BAD_REQUEST, e),
        },
    };

    #[derive(Clone, Copy, Debug)]
    enum LevelFilter {
        All,
        Info,
        Warn,
        Error,
    }

    fn parse_level_filter(s: &str) -> Option<LevelFilter> {
        match s.trim() {
            "" => Some(LevelFilter::All),
            "all" => Some(LevelFilter::All),
            "info" => Some(LevelFilter::Info),
            "warn" => Some(LevelFilter::Warn),
            "error" => Some(LevelFilter::Error),
            _ => None,
        }
    }

    let level_filter = match q.level.as_ref() {
        None => LevelFilter::All,
        Some(s) => match parse_level_filter(s) {
            Some(v) => v,
            None => {
                return text_response(
                    StatusCode::BAD_REQUEST,
                    format!("invalid level (expected all|info|warn|error): {}", s),
                );
            }
        },
    };

    let mut sql = format!("select {tc}, body", tc = schema.time_column);
    if schema.has_severity_text {
        sql.push_str(", severity_text");
    }
    if schema.has_member_id {
        sql.push_str(&format!(", {}", fluxon_observability::keys::KEY_MEMBER_ID));
    }
    sql.push_str(&format!(
        " from {table} where {k_cluster}={v_cluster} and {k_kind}={v_kind} and {k_role}={v_role}",
        table = &table,
        k_cluster = fluxon_observability::keys::KEY_CLUSTER_NAME,
        v_cluster = sql_quote_literal(cluster_name),
        k_kind = fluxon_observability::keys::KEY_MEMBER_KIND,
        v_kind = sql_quote_literal(member_kind.as_query_str()),
        k_role = fluxon_observability::keys::KEY_ROLE,
        v_role = sql_quote_literal(role.as_str()),
    ));

    if let Some(search) = q
        .search
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        // Keep it simple and stable: only filter by body substring.
        sql.push_str(&format!(
            " and body like {v}",
            v = sql_quote_literal(&format!("%{}%", search))
        ));
    }

    // Filtering by body prefix is robust because our tracing exporter encodes level at the start of body.
    match level_filter {
        LevelFilter::All => {}
        LevelFilter::Info => {
            sql.push_str(" and (body like 'INFO %' or body like 'WARN %' or body like 'ERROR %')");
        }
        LevelFilter::Warn => {
            sql.push_str(" and (body like 'WARN %' or body like 'ERROR %')");
        }
        LevelFilter::Error => {
            sql.push_str(" and body like 'ERROR %'");
        }
    }

    if let Some(member_id) = q.member_id.as_ref() {
        sql.push_str(&format!(
            " and {k}={v}",
            k = fluxon_observability::keys::KEY_MEMBER_ID,
            v = sql_quote_literal(member_id)
        ));
    }
    if let Some(before) = before_ts_i64 {
        sql.push_str(&format!(
            " and {tc} < {before}",
            tc = schema.time_column,
            before = before
        ));
    }
    if let Some(after) = after_ts_i64 {
        sql.push_str(&format!(
            " and {tc} > {after}",
            tc = schema.time_column,
            after = after
        ));
    }

    const LIMIT: usize = 200;
    if after_ts_i64.is_some() {
        sql.push_str(&format!(
            " order by {tc} asc limit {limit}",
            tc = schema.time_column,
            limit = LIMIT
        ));
    } else {
        sql.push_str(&format!(
            " order by {tc} desc limit {limit}",
            tc = schema.time_column,
            limit = LIMIT
        ));
    }

    let resp = match sql_client.query_raw_json(&sql).await {
        Ok(v) => v,
        Err(e) => {
            return text_response(
                StatusCode::BAD_GATEWAY,
                format!("greptime sql query failed: {}", e),
            );
        }
    };

    let out0 = match resp.output.get(0) {
        Some(v) => v,
        None => {
            return text_response(
                StatusCode::BAD_GATEWAY,
                "greptime sql missing output[0]".to_string(),
            );
        }
    };
    let rec = match out0.records.as_ref() {
        Some(v) => v,
        None => {
            let mut resp = (
                StatusCode::OK,
                Json(LogsApiResponse {
                    items: Vec::new(),
                    next_before_ts: None,
                }),
            )
                .into_response();
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                "application/json; charset=utf-8".parse().unwrap(),
            );
            return resp;
        }
    };

    let mut items: Vec<LogItem> = Vec::with_capacity(rec.rows.len());
    for row in &rec.rows {
        if row.len() < 2 {
            continue;
        }
        let ts_i64 = match row[0].as_i64() {
            Some(v) => v,
            None => continue,
        };
        let body = match row[1].as_str() {
            Some(v) => v.to_string(),
            None => row[1].to_string(),
        };
        let mut idx = 2;
        let severity_text = if schema.has_severity_text {
            let v = row
                .get(idx)
                .and_then(|v| v.as_str())
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty());
            idx += 1;
            v
        } else {
            None
        };
        let member_id = if schema.has_member_id {
            let v = row
                .get(idx)
                .and_then(|v| v.as_str())
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty());
            idx += 1;
            v
        } else {
            None
        };
        let _ = idx;

        items.push(LogItem {
            ts: ts_i64.to_string(),
            member_id,
            body,
            severity_text,
        });
    }

    let next_before_ts = items.last().map(|it| it.ts.clone());
    let mut resp = (
        StatusCode::OK,
        Json(LogsApiResponse {
            items,
            next_before_ts,
        }),
    )
        .into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/json; charset=utf-8".parse().unwrap(),
    );
    resp
}

#[derive(serde::Serialize)]
struct LogTablesApiResponse {
    tables: Vec<String>,
}

async fn api_log_tables(State(st): State<Arc<AppState>>) -> Response {
    let Some(gcfg) = st.cfg.greptime_sql.as_ref() else {
        return text_response(
            StatusCode::BAD_REQUEST,
            "greptime_sql is not configured. If you are using the embedded master panel, configure master.monitoring.otlp_log_api in all_config.yaml so the panel can derive Greptime SQL base_url/db."
                .to_string(),
        );
    };
    let sql_client = fluxon_observability::greptime_sql::GreptimeSqlClient::new(
        gcfg.base_url.clone(),
        gcfg.db.clone(),
    );
    let resp = match sql_client.query_raw_json("show tables").await {
        Ok(v) => v,
        Err(e) => {
            return text_response(
                StatusCode::BAD_GATEWAY,
                format!("greptime sql query failed: {}", e),
            );
        }
    };
    let out0 = match resp.output.get(0) {
        Some(v) => v,
        None => {
            return text_response(
                StatusCode::BAD_GATEWAY,
                "greptime sql missing output[0]".to_string(),
            );
        }
    };
    let rec = match out0.records.as_ref() {
        Some(v) => v,
        None => {
            let mut resp = (
                StatusCode::OK,
                Json(LogTablesApiResponse { tables: Vec::new() }),
            )
                .into_response();
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                "application/json; charset=utf-8".parse().unwrap(),
            );
            return resp;
        }
    };

    let mut tables: Vec<String> = Vec::with_capacity(rec.rows.len());
    for row in &rec.rows {
        if row.len() != 1 {
            continue;
        }
        let t = match row[0].as_str() {
            Some(v) => v,
            None => continue,
        };
        if is_safe_ident(t) {
            tables.push(t.to_string());
        }
    }
    let mut resp = (StatusCode::OK, Json(LogTablesApiResponse { tables })).into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/json; charset=utf-8".parse().unwrap(),
    );
    resp
}

fn build_router(st: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/landing", get(landing))
        .route("/api/clusters", get(api_clusters))
        .route("/api/kv_metric_panel", get(kv_metric_panel))
        .route("/api/kv_metric_members", get(kv_metric_members))
        .route("/view", get(view))
        .route("/topology", get(topology_page))
        .route("/cli", get(cli))
        .route("/r/ops/:cluster", any(ops_panel_entry))
        .route("/r/ops/:cluster/", any(ops_panel_root))
        .route("/r/ops/:cluster/fluxon", any(ops_fluxon_entry))
        .route("/r/ops/:cluster/fluxon/", any(ops_fluxon_entry))
        .route("/r/ops/:cluster/fluxon/kv", get(ops_fluxon_kv_view))
        .route("/r/ops/:cluster/fluxon/kv/", get(ops_fluxon_kv_view))
        .route(
            "/r/ops/:cluster/fluxon/kv/rdma_devices",
            post(ops_fluxon_kv_update_rdma_devices),
        )
        .route("/r/ops/:cluster/fluxon/mq", get(ops_fluxon_mq_view))
        .route("/r/ops/:cluster/fluxon/mq/", get(ops_fluxon_mq_view))
        .route(
            "/r/ops/:cluster/fluxon/topology",
            get(ops_fluxon_topology_view),
        )
        .route(
            "/r/ops/:cluster/fluxon/topology/",
            get(ops_fluxon_topology_view),
        )
        .route("/r/ops/:cluster/fluxon/fs", any(ops_fluxon_fs_entry))
        .route("/r/ops/:cluster/fluxon/fs/", any(ops_fluxon_fs_root))
        .route("/r/ops/:cluster/fluxon/fs/*rest", any(ops_fluxon_fs_rest))
        .route("/r/fs/:cluster", any(legacy_fs_panel_entry))
        .route("/r/fs/:cluster/", any(legacy_fs_panel_root))
        .route("/r/fs/:cluster/*rest", any(legacy_fs_panel_rest))
        .route("/r/:service/:cluster", any(proxy_registered_service_entry))
        .route("/r/:service/:cluster/", any(proxy_registered_service_root))
        .route(
            "/r/:service/:cluster/*rest",
            any(proxy_registered_service_rest),
        )
        .route("/logs", get(logs_page))
        .route("/api/logs", get(api_logs))
        .route("/api/log_tables", get(api_log_tables))
        .with_state(st)
}

const PROXY_SKIP_REQ_HEADERS: &[&str] = &[
    "host",
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

const PROXY_SKIP_RESP_HEADERS: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

fn should_skip_proxy_header(name: &str, skip: &[&str]) -> bool {
    let n = name.trim().to_ascii_lowercase();
    skip.iter().any(|v| *v == n)
}

async fn ops_panel_entry(
    State(st): State<Arc<AppState>>,
    Path(cluster_name): Path<String>,
    req: Request<Body>,
) -> Response {
    let _ = st;
    redirect_to_registered_panel(crate::OPS_PANEL_SERVICE_NAME, &cluster_name, req.uri())
}

async fn ops_panel_root(
    State(st): State<Arc<AppState>>,
    Path(cluster_name): Path<String>,
    req: Request<Body>,
) -> Response {
    let _ = st;
    redirect_to_registered_panel(crate::OPS_PANEL_SERVICE_NAME, &cluster_name, req.uri())
}

async fn proxy_registered_service_entry(
    State(st): State<Arc<AppState>>,
    Path((service_name, cluster_name)): Path<(String, String)>,
    req: Request<Body>,
) -> Response {
    let target = registered_panel_target(&service_name, &cluster_name, req.uri());
    let _ = st; // keep signature stable with other proxy handlers
    Redirect::temporary(&target).into_response()
}

async fn legacy_fs_panel_entry(Path(cluster_name): Path<String>, req: Request<Body>) -> Response {
    let target = fs_s3_ui_panel_target(&cluster_name, None, req.uri());
    Redirect::temporary(&target).into_response()
}

async fn legacy_fs_panel_root(Path(cluster_name): Path<String>, req: Request<Body>) -> Response {
    let target = fs_s3_ui_panel_target(&cluster_name, None, req.uri());
    Redirect::temporary(&target).into_response()
}

async fn legacy_fs_panel_rest(
    Path((cluster_name, rest)): Path<(String, String)>,
    req: Request<Body>,
) -> Response {
    let target = fs_s3_ui_panel_target(&cluster_name, Some(&rest), req.uri());
    Redirect::temporary(&target).into_response()
}

async fn proxy_registered_service_root(
    State(st): State<Arc<AppState>>,
    Path((service_name, cluster_name)): Path<(String, String)>,
    req: Request<Body>,
) -> Response {
    proxy_registered_service_impl(&st, &service_name, &cluster_name, "", req).await
}

async fn proxy_registered_service_rest(
    State(st): State<Arc<AppState>>,
    Path((service_name, cluster_name, rest)): Path<(String, String, String)>,
    req: Request<Body>,
) -> Response {
    proxy_registered_service_impl(&st, &service_name, &cluster_name, &rest, req).await
}

async fn proxy_registered_service_impl(
    st: &AppState,
    service_name: &str,
    cluster_name: &str,
    rest: &str,
    req: Request<Body>,
) -> Response {
    let orig_uri = req.uri().to_string();
    let orig_host = req
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "".to_string());

    let mut etcd = match EtcdClient::connect(st.cfg.etcd_endpoints.clone(), None).await {
        Ok(c) => c,
        Err(e) => {
            return text_response(
                StatusCode::BAD_GATEWAY,
                format!(
                    "etcd connect failed (panel proxy): service={} err={}",
                    service_name, e
                ),
            );
        }
    };
    let etcd_key = fluxon_cli_proxy_desc_etcd_key(service_name, cluster_name);
    let resp = match etcd.get(etcd_key.clone(), None).await {
        Ok(v) => v,
        Err(e) => {
            return text_response(
                StatusCode::BAD_GATEWAY,
                format!(
                    "etcd get failed (panel proxy): service={} key={} err={}",
                    service_name, etcd_key, e
                ),
            );
        }
    };
    let Some(kv) = resp.kvs().first() else {
        // Help operators quickly spot cluster-name mismatch or a missing publisher by listing
        // the currently registered clusters for the service.
        let prefix = fluxon_cli_proxy_desc_etcd_service_prefix_v2(service_name);
        let mut registered: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        match scan_etcd_prefix_paginated(&mut etcd, &prefix, |key, _value| {
            let key = String::from_utf8_lossy(key);
            if !key.starts_with(&prefix) {
                return Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue);
            }
            let rest = &key[prefix.len()..];
            let Some((cluster, _tail)) = rest.split_once('/') else {
                return Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue);
            };
            if !cluster.is_empty() {
                registered.insert(cluster.to_string());
            }
            Ok::<EtcdPrefixScanAction, anyhow::Error>(EtcdPrefixScanAction::Continue)
        })
        .await
        {
            Ok(()) => {}
            Err(_) => {}
        }
        let mut body = format!(
            "panel proxy descriptor is not published in etcd yet: service={} key={}",
            service_name, etcd_key
        );
        if !registered.is_empty() {
            body.push_str(&format!(
                "\n\nregistered clusters for service '{}':",
                service_name
            ));
            for c in registered {
                body.push_str(&format!("\n  - {}", c));
            }
        }
        body.push_str(
            "\n\nnotes:\n- ensure the panel publisher is upgraded and publishes /fluxon_cli_proxy/v2/<service>/<cluster>/descriptor\n- ensure the panel is connected to the same cluster_name as fluxon_cli is browsing",
        );
        return text_response(StatusCode::BAD_GATEWAY, body);
    };

    let raw_desc = String::from_utf8_lossy(kv.value()).trim().to_string();
    if raw_desc.is_empty() {
        return text_response(
            StatusCode::BAD_GATEWAY,
            format!(
                "invalid panel proxy descriptor in etcd (empty): service={} key={}",
                service_name, etcd_key
            ),
        );
    }

    let desc: FluxonCliProxyDescriptorV2 = match serde_json::from_str(&raw_desc) {
        Ok(v) => v,
        Err(e) => {
            return text_response(
                StatusCode::BAD_GATEWAY,
                format!(
                    "invalid panel proxy descriptor json in etcd: service={} key={} err={}",
                    service_name, etcd_key, e
                ),
            );
        }
    };

    let allow_prefixes: Vec<String> = desc
        .allow_prefixes
        .iter()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .collect();
    if allow_prefixes.is_empty() {
        return text_response(
            StatusCode::BAD_GATEWAY,
            format!(
                "invalid panel proxy descriptor allow_prefixes (empty): service={} key={}",
                service_name, etcd_key
            ),
        );
    }
    for p in allow_prefixes.iter() {
        if !p.starts_with('/') {
            return text_response(
                StatusCode::BAD_GATEWAY,
                format!(
                    "invalid panel proxy descriptor allow_prefixes entry (must start with '/'): service={} key={} prefix={}",
                    service_name, etcd_key, p
                ),
            );
        }
    }

    let upstream_path = if rest.trim().is_empty() {
        "/".to_string()
    } else {
        format!("/{}", rest.trim_start_matches('/'))
    };
    if !allow_prefixes
        .iter()
        .any(|p| upstream_path.starts_with(p.as_str()))
    {
        return text_response(
            StatusCode::NOT_FOUND,
            format!(
                "panel proxy path is not allowed by descriptor: service={} cluster={} path={}",
                service_name, cluster_name, upstream_path
            ),
        );
    }

    let method = req.method().clone();
    if !matches!(
        method,
        axum::http::Method::GET
            | axum::http::Method::HEAD
            | axum::http::Method::POST
            | axum::http::Method::PUT
            | axum::http::Method::DELETE
            | axum::http::Method::OPTIONS
            | axum::http::Method::PATCH
    ) {
        return text_response(
            StatusCode::METHOD_NOT_ALLOWED,
            format!(
                "panel proxy method not allowed: service={} method={}",
                service_name, method
            ),
        );
    }

    let headers = req.headers().clone();
    let qs = req.uri().query().map(|v| v.to_string());
    let req_body = req.into_body();

    let mut fwd_headers: Vec<(String, String)> = Vec::new();
    fwd_headers.push((HDR_PROXY_ORIGINAL_URI.to_string(), orig_uri.clone()));
    if !orig_host.is_empty() {
        fwd_headers.push((HDR_PROXY_ORIGINAL_HOST.to_string(), orig_host.clone()));
    }
    for (k, v) in headers.iter() {
        let name = k.as_str();
        if should_skip_proxy_header(name, PROXY_SKIP_REQ_HEADERS) {
            continue;
        }
        let Ok(s) = v.to_str() else {
            continue;
        };
        fwd_headers.push((name.to_string(), s.to_string()));
    }

    match desc.transport.clone() {
        FluxonCliProxyTransportV2::Http { base_url } => {
            let base = base_url.trim().trim_end_matches('/').to_string();
            if base.is_empty() || !base.contains("://") {
                return text_response(
                    StatusCode::BAD_GATEWAY,
                    format!(
                        "invalid panel proxy descriptor base_url: service={} key={} base_url={}",
                        service_name, etcd_key, base
                    ),
                );
            }

            let mut url = format!("{}{}", base, upstream_path);
            if let Some(qs) = qs.as_ref() {
                url.push('?');
                url.push_str(qs);
            }

            let uri2: Uri = match url.parse() {
                Ok(v) => v,
                Err(e) => {
                    return text_response(
                        StatusCode::BAD_GATEWAY,
                        format!(
                            "panel proxy upstream url parse failed: service={} url={} err={}",
                            service_name, url, e
                        ),
                    );
                }
            };

            let mut upstream_req = match Request::builder()
                .method(method.clone())
                .uri(uri2)
                .body(req_body)
            {
                Ok(v) => v,
                Err(e) => {
                    return text_response(
                        StatusCode::BAD_REQUEST,
                        format!(
                            "panel proxy failed to build upstream request: service={} err={}",
                            service_name, e
                        ),
                    );
                }
            };

            // English note:
            // - Some upstream services (e.g. S3 gateways) need to validate client-side signatures.
            // - fluxon_cli rewrites the upstream path (strips /r/<service>/<cluster>/...), so the upstream
            //   cannot reconstruct the original canonical request by itself.
            // - Therefore we forward the original URI/Host as dedicated headers; upstream may ignore them.
            upstream_req.headers_mut().insert(
                HeaderName::from_static(HDR_PROXY_ORIGINAL_URI),
                HeaderValue::from_str(&orig_uri).unwrap(),
            );
            if !orig_host.is_empty() {
                upstream_req.headers_mut().insert(
                    HeaderName::from_static(HDR_PROXY_ORIGINAL_HOST),
                    HeaderValue::from_str(&orig_host).unwrap(),
                );
            }
            for (k, v) in headers.iter() {
                let name = k.as_str();
                if should_skip_proxy_header(name, PROXY_SKIP_REQ_HEADERS) {
                    continue;
                }
                upstream_req.headers_mut().append(k.clone(), v.clone());
            }

            let upstream = match st.proxy_client.request(upstream_req).await {
                Ok(v) => v,
                Err(e) => {
                    return text_response(
                        StatusCode::BAD_GATEWAY,
                        format!(
                            "panel proxy upstream request failed: service={} url={} err={}",
                            service_name, url, e
                        ),
                    );
                }
            };

            let status = upstream.status();
            let upstream_headers = upstream.headers().clone();
            let content_type = upstream_headers
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(|v| v.to_string());

            let is_html = content_type
                .as_ref()
                .map(|ct| ct.to_ascii_lowercase().starts_with("text/html"))
                .unwrap_or(false);
            if is_html {
                let body = match hyper::body::to_bytes(upstream.into_body()).await {
                    Ok(v) => v,
                    Err(e) => {
                        return text_response(
                            StatusCode::BAD_GATEWAY,
                            format!(
                                "panel proxy upstream read body failed: service={} err={}",
                                service_name, e
                            ),
                        );
                    }
                };
                let mut resp = {
                    let html0 = String::from_utf8_lossy(&body).to_string();
                    if desc.html_inject {
                        Html(inject_auto_refresh_tool(html0)).into_response()
                    } else {
                        Html(html0).into_response()
                    }
                };
                *resp.status_mut() =
                    StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
                if let Some(ct) = content_type.as_ref() {
                    resp.headers_mut()
                        .insert(header::CONTENT_TYPE, ct.parse().unwrap());
                }
                return resp;
            }

            // Streaming passthrough for non-HTML (S3 XML / large binaries).
            let upstream_body = upstream.into_body();
            let mut resp = Response::new(boxed(upstream_body));
            *resp.status_mut() =
                StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            for (k, v) in upstream_headers.iter() {
                let name = k.as_str();
                if should_skip_proxy_header(name, PROXY_SKIP_RESP_HEADERS) {
                    continue;
                }
                resp.headers_mut().append(k.clone(), v.clone());
            }
            resp
        }
        FluxonCliProxyTransportV2::P2pRpc { node_id } => {
            let node_id = node_id.trim().to_string();
            if node_id.is_empty() {
                return text_response(
                    StatusCode::BAD_GATEWAY,
                    format!(
                        "invalid panel proxy descriptor node_id (empty): service={} key={}",
                        service_name, etcd_key
                    ),
                );
            }

            let Some(backend) = st.registered_panel_proxy_backend.as_ref().cloned() else {
                return text_response(
                    StatusCode::BAD_GATEWAY,
                    format!(
                        "panel proxy backend is not installed for p2p_rpc transport: service={} cluster={} key={}",
                        service_name, cluster_name, etcd_key
                    ),
                );
            };

            let body = match hyper::body::to_bytes(req_body).await {
                Ok(v) => v.to_vec(),
                Err(e) => {
                    return text_response(
                        StatusCode::BAD_GATEWAY,
                        format!(
                            "panel proxy failed to read request body: service={} err={}",
                            service_name, e
                        ),
                    );
                }
            };

            let path_and_query = if let Some(qs) = qs.as_ref() {
                format!("{}?{}", upstream_path, qs)
            } else {
                upstream_path.clone()
            };

            let backend_req = RegisteredPanelProxyBackendReq {
                service_name: service_name.to_string(),
                cluster_name: cluster_name.to_string(),
                node_id,
                method,
                path_and_query,
                headers: fwd_headers,
                body,
                original_uri: orig_uri,
                original_host: orig_host,
            };

            let upstream = match (backend)(backend_req).await {
                Ok(v) => v,
                Err(e) => {
                    return text_response(
                        StatusCode::BAD_GATEWAY,
                        format!(
                            "panel proxy upstream request failed (p2p_rpc): service={} err={}",
                            service_name, e
                        ),
                    );
                }
            };

            let status = StatusCode::from_u16(upstream.status).unwrap_or(StatusCode::BAD_GATEWAY);
            let mut content_type: Option<String> = None;
            for (k, v) in upstream.headers.iter() {
                if k.eq_ignore_ascii_case(header::CONTENT_TYPE.as_str()) {
                    content_type = Some(v.to_string());
                    break;
                }
            }
            let is_html = content_type
                .as_ref()
                .map(|ct| ct.to_ascii_lowercase().starts_with("text/html"))
                .unwrap_or(false);
            if is_html {
                let mut resp = {
                    let html0 = String::from_utf8_lossy(&upstream.body).to_string();
                    if desc.html_inject {
                        Html(inject_auto_refresh_tool(html0)).into_response()
                    } else {
                        Html(html0).into_response()
                    }
                };
                *resp.status_mut() = status;
                if let Some(ct) = content_type.as_ref() {
                    resp.headers_mut()
                        .insert(header::CONTENT_TYPE, ct.parse().unwrap());
                }
                return resp;
            }

            let mut resp = Response::new(boxed(Body::from(upstream.body)));
            *resp.status_mut() = status;
            for (k, v) in upstream.headers.iter() {
                if should_skip_proxy_header(k, PROXY_SKIP_RESP_HEADERS) {
                    continue;
                }
                let Ok(name) = HeaderName::from_bytes(k.as_bytes()) else {
                    continue;
                };
                let Ok(value) = HeaderValue::from_str(v) else {
                    continue;
                };
                resp.headers_mut().append(name, value);
            }
            resp
        }
    }
}

pub async fn serve_http_from_tcp(
    cfg: MonitorConfig,
    listener: std::net::TcpListener,
    registered_panel_proxy_backend: Option<RegisteredPanelProxyBackend>,
) -> anyhow::Result<()> {
    let cfg = Arc::new(cfg);
    let log_schema_cache = Arc::new(LogSchemaCache::new());
    let proxy_client = new_proxy_client();
    let app = build_router(Arc::new(AppState {
        cfg,
        log_schema_cache,
        proxy_client,
        registered_panel_proxy_backend,
    }));
    axum::Server::from_tcp(listener)
        .context("http Server::from_tcp failed")?
        .serve(app.into_make_service())
        .await
        .context("http serve failed")?;
    Ok(())
}

pub async fn serve_http_with_shutdown_from_tcp<F>(
    cfg: MonitorConfig,
    listener: std::net::TcpListener,
    shutdown: F,
    registered_panel_proxy_backend: Option<RegisteredPanelProxyBackend>,
) -> anyhow::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let cfg = Arc::new(cfg);
    let log_schema_cache = Arc::new(LogSchemaCache::new());
    let proxy_client = new_proxy_client();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        shutdown.await;
        let _ = shutdown_tx.send(true);
    });

    let app = build_router(Arc::new(AppState {
        cfg,
        log_schema_cache,
        proxy_client,
        registered_panel_proxy_backend,
    }));
    axum::Server::from_tcp(listener)
        .context("http Server::from_tcp failed")?
        .serve(app.into_make_service())
        .with_graceful_shutdown(async move {
            let mut rx = shutdown_rx;
            let _ = rx.changed().await;
        })
        .await
        .context("http serve failed")?;
    Ok(())
}

pub async fn serve_http(cfg: MonitorConfig, listen_addr: SocketAddr) -> anyhow::Result<()> {
    let listener = std::net::TcpListener::bind(listen_addr)
        .with_context(|| format!("http bind failed at {}", listen_addr))?;
    listener
        .set_nonblocking(true)
        .context("http listener set_nonblocking failed")?;
    serve_http_from_tcp(cfg, listener, None)
        .await
        .with_context(|| format!("http serve failed at {}", listen_addr))
}

pub async fn serve_http_with_shutdown<F>(
    cfg: MonitorConfig,
    listen_addr: SocketAddr,
    shutdown: F,
) -> anyhow::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let listener = std::net::TcpListener::bind(listen_addr)
        .with_context(|| format!("http bind failed at {}", listen_addr))?;
    listener
        .set_nonblocking(true)
        .context("http listener set_nonblocking failed")?;
    serve_http_with_shutdown_from_tcp(cfg, listener, shutdown, None)
        .await
        .with_context(|| format!("http serve failed at {}", listen_addr))
}
