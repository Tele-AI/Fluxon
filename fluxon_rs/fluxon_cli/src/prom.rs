use crate::model::MemberRole;
use anyhow::Context;
use fluxon_observability::keys::{
    PROM_LABEL_FS_IO_OP, PROM_LABEL_FS_MOUNT_KIND, PROM_LABEL_FS_MOUNTPOINT_DIR_ABS,
    PROM_LABEL_FS_TARGET_DIR_ABS, PROM_LABEL_MQ_CHAN_ID, PROM_LABEL_MQ_CONSUMER_IDX,
    PROM_LABEL_MQ_METRIC, PROM_LABEL_MQ_PRODUCER_IDX, PROM_LABEL_MQ_STAT, PROM_LABEL_NODE,
    PROM_LABEL_PEER, PROM_METRIC_CONTAINER_MEMORY_LIMIT_BYTES,
    PROM_METRIC_CONTAINER_MEMORY_USAGE_BYTES, PROM_METRIC_FS_IO_OPS_TOTAL,
    PROM_METRIC_FS_MOUNT_FS_TOTAL_BYTES, PROM_METRIC_FS_MOUNT_FS_USED_BYTES,
    PROM_METRIC_KV_PEER_NETWORK_BYTES_TOTAL,
    PROM_METRIC_MQ_CONSUMER_NONBLOCKING_LATEST_INTERVAL_UNIX_MS,
    PROM_METRIC_MQ_CONSUMER_NONBLOCKING_LATEST_PHASE_CALLS,
    PROM_METRIC_MQ_CONSUMER_NONBLOCKING_LATEST_PHASE_RPS, PROM_METRIC_MQ_GET_ONE_LATENCY_US,
    PROM_METRIC_MQ_GET_ONE_WINDOW_BYTES, PROM_METRIC_MQ_GET_ONE_WINDOW_CALLS,
    PROM_METRIC_MQ_GET_ONE_WINDOW_TIMEOUTS, PROM_METRIC_MQ_PREFETCH_INFLIGHT_QUEUE_SIZE,
    PROM_METRIC_MQ_PREFETCH_LATENCY_US, PROM_METRIC_MQ_PREFETCH_TARGET_INFLIGHT,
    PROM_METRIC_MQ_PRODUCER_NONBLOCKING_LATEST_INTERVAL_UNIX_MS,
    PROM_METRIC_MQ_PRODUCER_NONBLOCKING_LATEST_PHASE_CALLS,
    PROM_METRIC_MQ_PRODUCER_NONBLOCKING_LATEST_PHASE_RPS, PROM_METRIC_MQ_PUT_WINDOW_BYTES,
    PROM_METRIC_MQ_PUT_WINDOW_CALLS, PROM_METRIC_SHM_FILE_ALLOCATED_BYTES,
    PROM_METRIC_SHM_FILE_SIZE_BYTES, PROM_METRIC_TOKIO_ALIVE_TASKS, PROM_METRIC_TOKIO_BUSY_PERCENT,
    PROM_METRIC_TOKIO_GLOBAL_QUEUE_DEPTH, PROM_METRIC_TOKIO_MAX_WORKER_BUSY_PERCENT,
    PROM_METRIC_TOKIO_NUM_WORKERS, PROM_METRIC_TOKIO_PARK_UNPARK_RATE_HZ,
    PROM_VALUE_MQ_GET_ONE_METRIC_POST, PROM_VALUE_MQ_GET_ONE_METRIC_SIGNAL,
    PROM_VALUE_MQ_GET_ONE_METRIC_TOTAL, PROM_VALUE_MQ_GET_ONE_METRIC_WAIT_RX,
    PROM_VALUE_MQ_INTERVAL_BEGIN, PROM_VALUE_MQ_INTERVAL_END,
    PROM_VALUE_MQ_PREFETCH_METRIC_ETCD_PUT, PROM_VALUE_MQ_PREFETCH_METRIC_GET_HANDLE,
    PROM_VALUE_MQ_PREFETCH_METRIC_HANDLE_AWAIT, PROM_VALUE_MQ_STAT_AVG, PROM_VALUE_MQ_STAT_LATEST,
    PROM_VALUE_MQ_STAT_MAX,
};
use fluxon_observability::types::FsMountKind;
use reqwest::Url;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct PromClient {
    base_url: String,
    http: reqwest::Client,
    // English note:
    // - When set, all "instant" queries should be evaluated at this absolute UNIX timestamp
    //   (seconds since epoch, floating-point allowed).
    // - This powers topology "time travel": the UI can request a fixed historical timestamp and
    //   Fluxon CLI will query metrics as-of that time.
    query_time_s: Option<f64>,
}

impl PromClient {
    pub fn new(base_url: String) -> Self {
        Self {
            base_url,
            http: reqwest::Client::new(),
            query_time_s: None,
        }
    }

    pub fn new_with_query_time(base_url: String, query_time_s: Option<f64>) -> Self {
        Self {
            base_url,
            http: reqwest::Client::new(),
            query_time_s,
        }
    }

    pub fn effective_query_time_s(&self) -> anyhow::Result<f64> {
        match self.query_time_s {
            Some(t) => Ok(t),
            None => Ok(SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before UNIX_EPOCH")?
                .as_secs_f64()),
        }
    }

    fn query_range_url(&self) -> anyhow::Result<Url> {
        let s = format!("{}/api/v1/query_range", self.base_url.trim_end_matches('/'));
        Url::parse(&s).with_context(|| format!("invalid prometheus_base_url: {}", self.base_url))
    }

    pub async fn query_instant(&self, promql: &str) -> anyhow::Result<Vec<PromSample>> {
        let url = self.query_range_url()?;
        let now_s = match self.query_time_s {
            Some(t) => t,
            None => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before UNIX_EPOCH")?
                .as_secs_f64(),
        };
        let start_s = (now_s - 1.0).max(0.0);
        let end_s = now_s;
        let start_s = format!("{:.3}", start_s);
        let end_s = format!("{:.3}", end_s);
        let step_s = "1s".to_string();
        let resp = self
            .http
            .get(url)
            .query(&[
                ("query", promql),
                ("start", &start_s),
                ("end", &end_s),
                ("step", &step_s),
            ])
            .send()
            .await
            .with_context(|| format!("prometheus query failed: {}", promql))?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .with_context(|| "read prometheus response body".to_string())?;
        if !status.is_success() {
            anyhow::bail!("prometheus http {}: {}", status.as_u16(), body);
        }
        let parsed: PromResp = serde_json::from_str(&body)
            .with_context(|| format!("parse prometheus json: {}", body))?;
        if parsed.status != "success" {
            anyhow::bail!("prometheus status != success: {}", body);
        }
        let mut out: Vec<PromSample> = Vec::with_capacity(parsed.data.result.len());
        for s in parsed.data.result {
            match s {
                PromResultSample::Instant { metric, value } => {
                    out.push(PromSample { metric, value });
                }
                PromResultSample::Range { metric, values } => {
                    let Some(value) = values.last().cloned() else {
                        continue;
                    };
                    out.push(PromSample { metric, value });
                }
            }
        }
        Ok(out)
    }

    pub async fn query_range(
        &self,
        promql: &str,
        start_s: f64,
        end_s: f64,
        step: &str,
    ) -> anyhow::Result<Vec<PromRangeSeries>> {
        let url = self.query_range_url()?;
        let start_s = format!("{:.3}", start_s.max(0.0));
        let end_s = format!("{:.3}", end_s.max(0.0));
        let resp = self
            .http
            .get(url)
            .query(&[
                ("query", promql),
                ("start", &start_s),
                ("end", &end_s),
                ("step", step),
            ])
            .send()
            .await
            .with_context(|| format!("prometheus query_range failed: {}", promql))?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .with_context(|| "read prometheus response body".to_string())?;
        if !status.is_success() {
            anyhow::bail!("prometheus http {}: {}", status.as_u16(), body);
        }
        let parsed: PromResp = serde_json::from_str(&body)
            .with_context(|| format!("parse prometheus json: {}", body))?;
        if parsed.status != "success" {
            anyhow::bail!("prometheus status != success: {}", body);
        }

        let mut out: Vec<PromRangeSeries> = Vec::with_capacity(parsed.data.result.len());
        for s in parsed.data.result {
            match s {
                PromResultSample::Instant { metric, value } => {
                    out.push(PromRangeSeries {
                        metric,
                        values: vec![value],
                    });
                }
                PromResultSample::Range { metric, values } => {
                    out.push(PromRangeSeries { metric, values });
                }
            }
        }
        Ok(out)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PromResp {
    pub status: String,
    pub data: PromData,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PromData {
    pub result_type: String,
    pub result: Vec<PromResultSample>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum PromResultSample {
    Instant {
        metric: BTreeMap<String, String>,
        value: (f64, String),
    },
    Range {
        metric: BTreeMap<String, String>,
        values: Vec<(f64, String)>,
    },
}

#[derive(Debug, Clone, Deserialize)]
pub struct PromSample {
    pub metric: BTreeMap<String, String>,
    pub value: (f64, String),
}

impl PromSample {
    pub fn value_f64(&self) -> Option<f64> {
        self.value.1.parse::<f64>().ok()
    }
}

#[derive(Debug, Clone)]
pub struct PromRangeSeries {
    pub metric: BTreeMap<String, String>,
    pub values: Vec<(f64, String)>,
}

pub struct PromSnapshotMaps {
    pub node_cpu_usage_percent: HashMap<String, f64>,
    pub node_cpu_logical_cores: HashMap<String, f64>,
    pub node_memory_usage_bytes: HashMap<String, f64>,
    pub node_memory_total_bytes: HashMap<String, f64>,
    pub container_memory_usage_bytes: HashMap<String, f64>,
    pub container_memory_limit_bytes: HashMap<String, f64>,
    pub process_resident_memory_bytes: HashMap<String, f64>,
    pub process_cpu_usage_percent: HashMap<String, f64>,
    pub tokio_num_workers: HashMap<String, f64>,
    pub tokio_alive_tasks: HashMap<String, f64>,
    pub tokio_global_queue_depth: HashMap<String, f64>,
    pub tokio_busy_percent: HashMap<String, f64>,
    pub tokio_max_worker_busy_percent: HashMap<String, f64>,
    pub tokio_park_unpark_rate_hz: HashMap<String, f64>,
    pub process_network_tx_mbps: HashMap<String, f64>,
    pub process_network_rx_mbps: HashMap<String, f64>,
    pub node_network_tx_mbps_by_node_device: HashMap<(String, String), f64>,
    pub node_network_rx_mbps_by_node_device: HashMap<(String, String), f64>,

    pub kv_peer_network_tx_mbps_by_node_peer: HashMap<(String, String), f64>,
    pub kv_peer_network_rx_mbps_by_node_peer: HashMap<(String, String), f64>,

    pub fs_mount_fs_used_bytes_by_node_kind_mountpoint_target:
        HashMap<(String, FsMountKind, String, String), f64>,
    pub fs_mount_fs_total_bytes_by_node_kind_mountpoint_target:
        HashMap<(String, FsMountKind, String, String), f64>,
    pub shm_file_size_bytes_by_node_dir_file: HashMap<(String, String, String), f64>,
    pub shm_file_allocated_bytes_by_node_dir_file: HashMap<(String, String, String), f64>,
    pub fs_read_rps: HashMap<String, f64>,
    pub fs_write_rps: HashMap<String, f64>,

    pub put_rps: HashMap<String, f64>,
    pub get_rps: HashMap<String, f64>,
    pub put_bps: HashMap<String, f64>,
    pub get_bps: HashMap<String, f64>,

    pub put_latency_mean_us: HashMap<String, f64>,
    pub put_latency_p95_us: HashMap<String, f64>,
    pub put_latency_p99_us: HashMap<String, f64>,
    pub get_latency_mean_us: HashMap<String, f64>,
    pub get_latency_p95_us: HashMap<String, f64>,
    pub get_latency_p99_us: HashMap<String, f64>,

    pub seg_capacity_bytes_by_node_device: HashMap<(String, String), f64>,
    pub seg_used_bytes_by_node_device: HashMap<(String, String), f64>,
}

impl PromSnapshotMaps {
    pub fn empty() -> Self {
        Self {
            node_cpu_usage_percent: HashMap::new(),
            node_cpu_logical_cores: HashMap::new(),
            node_memory_usage_bytes: HashMap::new(),
            node_memory_total_bytes: HashMap::new(),
            container_memory_usage_bytes: HashMap::new(),
            container_memory_limit_bytes: HashMap::new(),
            process_resident_memory_bytes: HashMap::new(),
            process_cpu_usage_percent: HashMap::new(),
            tokio_num_workers: HashMap::new(),
            tokio_alive_tasks: HashMap::new(),
            tokio_global_queue_depth: HashMap::new(),
            tokio_busy_percent: HashMap::new(),
            tokio_max_worker_busy_percent: HashMap::new(),
            tokio_park_unpark_rate_hz: HashMap::new(),
            process_network_tx_mbps: HashMap::new(),
            process_network_rx_mbps: HashMap::new(),
            node_network_tx_mbps_by_node_device: HashMap::new(),
            node_network_rx_mbps_by_node_device: HashMap::new(),
            kv_peer_network_tx_mbps_by_node_peer: HashMap::new(),
            kv_peer_network_rx_mbps_by_node_peer: HashMap::new(),
            fs_mount_fs_used_bytes_by_node_kind_mountpoint_target: HashMap::new(),
            fs_mount_fs_total_bytes_by_node_kind_mountpoint_target: HashMap::new(),
            shm_file_size_bytes_by_node_dir_file: HashMap::new(),
            shm_file_allocated_bytes_by_node_dir_file: HashMap::new(),
            fs_read_rps: HashMap::new(),
            fs_write_rps: HashMap::new(),
            put_rps: HashMap::new(),
            get_rps: HashMap::new(),
            put_bps: HashMap::new(),
            get_bps: HashMap::new(),
            put_latency_mean_us: HashMap::new(),
            put_latency_p95_us: HashMap::new(),
            put_latency_p99_us: HashMap::new(),
            get_latency_mean_us: HashMap::new(),
            get_latency_p95_us: HashMap::new(),
            get_latency_p99_us: HashMap::new(),
            seg_capacity_bytes_by_node_device: HashMap::new(),
            seg_used_bytes_by_node_device: HashMap::new(),
        }
    }
}

pub struct MqPromSnapshotMaps {
    pub prefetch_avg_get_handle_us: HashMap<(i64, String), f64>,
    pub prefetch_latest_get_handle_us: HashMap<(i64, String), f64>,
    pub prefetch_avg_handle_await_us: HashMap<(i64, String), f64>,
    pub prefetch_latest_handle_await_us: HashMap<(i64, String), f64>,
    pub prefetch_avg_etcd_put_us: HashMap<(i64, String), f64>,
    pub prefetch_latest_etcd_put_us: HashMap<(i64, String), f64>,
    pub prefetch_inflight_queue_size: HashMap<(i64, String), f64>,
    pub prefetch_target_inflight: HashMap<(i64, String), f64>,

    pub get_one_avg_total_us: HashMap<(i64, String), f64>,
    pub get_one_max_total_us: HashMap<(i64, String), f64>,
    pub get_one_avg_wait_rx_us: HashMap<(i64, String), f64>,
    pub get_one_max_wait_rx_us: HashMap<(i64, String), f64>,
    pub get_one_avg_signal_us: HashMap<(i64, String), f64>,
    pub get_one_max_signal_us: HashMap<(i64, String), f64>,
    pub get_one_avg_post_us: HashMap<(i64, String), f64>,
    pub get_one_max_post_us: HashMap<(i64, String), f64>,
    pub get_one_window_calls: HashMap<(i64, String), f64>,
    pub get_one_window_timeouts: HashMap<(i64, String), f64>,
    pub get_one_window_bytes: HashMap<(i64, String), f64>,

    pub put_window_calls: HashMap<(i64, String), f64>,
    pub put_window_bytes: HashMap<(i64, String), f64>,
    pub producer_nonblocking_latest_phase_calls: HashMap<(i64, String), f64>,
    pub producer_nonblocking_latest_phase_rps: HashMap<(i64, String), f64>,
    pub producer_nonblocking_latest_begin_unix_ms: HashMap<(i64, String), f64>,
    pub producer_nonblocking_latest_end_unix_ms: HashMap<(i64, String), f64>,
    pub consumer_nonblocking_latest_phase_calls: HashMap<(i64, String), f64>,
    pub consumer_nonblocking_latest_phase_rps: HashMap<(i64, String), f64>,
    pub consumer_nonblocking_latest_begin_unix_ms: HashMap<(i64, String), f64>,
    pub consumer_nonblocking_latest_end_unix_ms: HashMap<(i64, String), f64>,
}

impl MqPromSnapshotMaps {
    pub fn empty() -> Self {
        Self {
            prefetch_avg_get_handle_us: HashMap::new(),
            prefetch_latest_get_handle_us: HashMap::new(),
            prefetch_avg_handle_await_us: HashMap::new(),
            prefetch_latest_handle_await_us: HashMap::new(),
            prefetch_avg_etcd_put_us: HashMap::new(),
            prefetch_latest_etcd_put_us: HashMap::new(),
            prefetch_inflight_queue_size: HashMap::new(),
            prefetch_target_inflight: HashMap::new(),
            get_one_avg_total_us: HashMap::new(),
            get_one_max_total_us: HashMap::new(),
            get_one_avg_wait_rx_us: HashMap::new(),
            get_one_max_wait_rx_us: HashMap::new(),
            get_one_avg_signal_us: HashMap::new(),
            get_one_max_signal_us: HashMap::new(),
            get_one_avg_post_us: HashMap::new(),
            get_one_max_post_us: HashMap::new(),
            get_one_window_calls: HashMap::new(),
            get_one_window_timeouts: HashMap::new(),
            get_one_window_bytes: HashMap::new(),
            put_window_calls: HashMap::new(),
            put_window_bytes: HashMap::new(),
            producer_nonblocking_latest_phase_calls: HashMap::new(),
            producer_nonblocking_latest_phase_rps: HashMap::new(),
            producer_nonblocking_latest_begin_unix_ms: HashMap::new(),
            producer_nonblocking_latest_end_unix_ms: HashMap::new(),
            consumer_nonblocking_latest_phase_calls: HashMap::new(),
            consumer_nonblocking_latest_phase_rps: HashMap::new(),
            consumer_nonblocking_latest_begin_unix_ms: HashMap::new(),
            consumer_nonblocking_latest_end_unix_ms: HashMap::new(),
        }
    }
}

fn take_node_metric(samples: &[PromSample]) -> HashMap<String, f64> {
    let mut out = HashMap::new();
    for s in samples {
        let Some(node) = s.metric.get(PROM_LABEL_NODE) else {
            continue;
        };
        let Some(v) = s.value_f64() else {
            continue;
        };
        out.insert(node.clone(), v);
    }
    out
}

fn take_node_peer_metric(samples: &[PromSample]) -> HashMap<(String, String), f64> {
    let mut out = HashMap::new();
    for s in samples {
        let Some(node) = s.metric.get(PROM_LABEL_NODE) else {
            continue;
        };
        let Some(peer) = s.metric.get(PROM_LABEL_PEER) else {
            continue;
        };
        let Some(v) = s.value_f64() else {
            continue;
        };
        out.insert((node.clone(), peer.clone()), v);
    }
    out
}

fn take_client_metric(samples: &[PromSample]) -> HashMap<String, f64> {
    let mut out = HashMap::new();
    for s in samples {
        let Some(client) = s.metric.get("client") else {
            continue;
        };
        let Some(v) = s.value_f64() else {
            continue;
        };
        out.insert(client.clone(), v);
    }
    out
}

fn take_node_device_metric(samples: &[PromSample]) -> HashMap<(String, String), f64> {
    let mut out = HashMap::new();
    for s in samples {
        let Some(node) = s.metric.get("node") else {
            continue;
        };
        let Some(device) = s.metric.get("device") else {
            continue;
        };
        let Some(v) = s.value_f64() else {
            continue;
        };
        out.insert((node.clone(), device.clone()), v);
    }
    out
}

pub fn role_from_member_metadata(meta: &BTreeMap<String, String>) -> MemberRole {
    if meta.get("master").map(|v| v == "true").unwrap_or(false) {
        return MemberRole::Master;
    }
    if meta.get("client").map(|v| v == "true").unwrap_or(false) {
        return MemberRole::OwnerClient;
    }
    if meta
        .get("side_transfer_worker")
        .map(|v| v == "true")
        .unwrap_or(false)
    {
        return MemberRole::SideTransferWorker;
    }
    if meta
        .get("external_client")
        .map(|v| v == "true")
        .unwrap_or(false)
    {
        return MemberRole::ExternalClient;
    }
    MemberRole::Unknown
}

pub async fn collect_mq_prom_snapshot(
    prom: &PromClient,
    warnings: &mut Vec<String>,
) -> MqPromSnapshotMaps {
    let mut out = MqPromSnapshotMaps::empty();

    async fn q(
        prom: &PromClient,
        warnings: &mut Vec<String>,
        label: &str,
        expr: &str,
    ) -> Vec<PromSample> {
        match prom.query_instant(expr).await {
            Ok(v) => v,
            Err(e) => {
                warnings.push(format!("prometheus query failed ({label}): {e}"));
                Vec::new()
            }
        }
    }

    let prefetch_latency = q(
        prom,
        warnings,
        "mq_prefetch_latency_microseconds",
        PROM_METRIC_MQ_PREFETCH_LATENCY_US,
    )
    .await;
    for s in &prefetch_latency {
        let Some(chan_id_s) = s.metric.get(PROM_LABEL_MQ_CHAN_ID) else {
            continue;
        };
        let Ok(chan_id) = chan_id_s.parse::<i64>() else {
            continue;
        };
        let Some(consumer_idx) = s.metric.get(PROM_LABEL_MQ_CONSUMER_IDX) else {
            continue;
        };
        let Some(m) = s.metric.get(PROM_LABEL_MQ_METRIC) else {
            continue;
        };
        let Some(stat) = s.metric.get(PROM_LABEL_MQ_STAT) else {
            continue;
        };
        let Some(v) = s.value_f64() else {
            continue;
        };
        let key = (chan_id, consumer_idx.clone());
        match (m.as_str(), stat.as_str()) {
            (PROM_VALUE_MQ_PREFETCH_METRIC_GET_HANDLE, PROM_VALUE_MQ_STAT_AVG) => {
                out.prefetch_avg_get_handle_us.insert(key, v);
            }
            (PROM_VALUE_MQ_PREFETCH_METRIC_GET_HANDLE, PROM_VALUE_MQ_STAT_LATEST) => {
                out.prefetch_latest_get_handle_us.insert(key, v);
            }
            (PROM_VALUE_MQ_PREFETCH_METRIC_HANDLE_AWAIT, PROM_VALUE_MQ_STAT_AVG) => {
                out.prefetch_avg_handle_await_us.insert(key, v);
            }
            (PROM_VALUE_MQ_PREFETCH_METRIC_HANDLE_AWAIT, PROM_VALUE_MQ_STAT_LATEST) => {
                out.prefetch_latest_handle_await_us.insert(key, v);
            }
            (PROM_VALUE_MQ_PREFETCH_METRIC_ETCD_PUT, PROM_VALUE_MQ_STAT_AVG) => {
                out.prefetch_avg_etcd_put_us.insert(key, v);
            }
            (PROM_VALUE_MQ_PREFETCH_METRIC_ETCD_PUT, PROM_VALUE_MQ_STAT_LATEST) => {
                out.prefetch_latest_etcd_put_us.insert(key, v);
            }
            _ => {}
        }
    }

    let prefetch_inflight = q(
        prom,
        warnings,
        "mq_prefetch_inflight_queue_size",
        PROM_METRIC_MQ_PREFETCH_INFLIGHT_QUEUE_SIZE,
    )
    .await;
    for s in &prefetch_inflight {
        let Some(chan_id_s) = s.metric.get(PROM_LABEL_MQ_CHAN_ID) else {
            continue;
        };
        let Ok(chan_id) = chan_id_s.parse::<i64>() else {
            continue;
        };
        let Some(consumer_idx) = s.metric.get(PROM_LABEL_MQ_CONSUMER_IDX) else {
            continue;
        };
        let Some(v) = s.value_f64() else {
            continue;
        };
        out.prefetch_inflight_queue_size
            .insert((chan_id, consumer_idx.clone()), v);
    }

    let prefetch_target = q(
        prom,
        warnings,
        "mq_prefetch_target_inflight",
        PROM_METRIC_MQ_PREFETCH_TARGET_INFLIGHT,
    )
    .await;
    for s in &prefetch_target {
        let Some(chan_id_s) = s.metric.get(PROM_LABEL_MQ_CHAN_ID) else {
            continue;
        };
        let Ok(chan_id) = chan_id_s.parse::<i64>() else {
            continue;
        };
        let Some(consumer_idx) = s.metric.get(PROM_LABEL_MQ_CONSUMER_IDX) else {
            continue;
        };
        let Some(v) = s.value_f64() else {
            continue;
        };
        out.prefetch_target_inflight
            .insert((chan_id, consumer_idx.clone()), v);
    }

    let get_one_latency = q(
        prom,
        warnings,
        "mq_get_one_latency_microseconds",
        PROM_METRIC_MQ_GET_ONE_LATENCY_US,
    )
    .await;
    for s in &get_one_latency {
        let Some(chan_id_s) = s.metric.get(PROM_LABEL_MQ_CHAN_ID) else {
            continue;
        };
        let Ok(chan_id) = chan_id_s.parse::<i64>() else {
            continue;
        };
        let Some(consumer_idx) = s.metric.get(PROM_LABEL_MQ_CONSUMER_IDX) else {
            continue;
        };
        let Some(m) = s.metric.get(PROM_LABEL_MQ_METRIC) else {
            continue;
        };
        let Some(stat) = s.metric.get(PROM_LABEL_MQ_STAT) else {
            continue;
        };
        let Some(v) = s.value_f64() else {
            continue;
        };
        let key = (chan_id, consumer_idx.clone());
        match (m.as_str(), stat.as_str()) {
            (PROM_VALUE_MQ_GET_ONE_METRIC_TOTAL, PROM_VALUE_MQ_STAT_AVG) => {
                out.get_one_avg_total_us.insert(key, v);
            }
            (PROM_VALUE_MQ_GET_ONE_METRIC_TOTAL, PROM_VALUE_MQ_STAT_MAX) => {
                out.get_one_max_total_us.insert(key, v);
            }
            (PROM_VALUE_MQ_GET_ONE_METRIC_WAIT_RX, PROM_VALUE_MQ_STAT_AVG) => {
                out.get_one_avg_wait_rx_us.insert(key, v);
            }
            (PROM_VALUE_MQ_GET_ONE_METRIC_WAIT_RX, PROM_VALUE_MQ_STAT_MAX) => {
                out.get_one_max_wait_rx_us.insert(key, v);
            }
            (PROM_VALUE_MQ_GET_ONE_METRIC_SIGNAL, PROM_VALUE_MQ_STAT_AVG) => {
                out.get_one_avg_signal_us.insert(key, v);
            }
            (PROM_VALUE_MQ_GET_ONE_METRIC_SIGNAL, PROM_VALUE_MQ_STAT_MAX) => {
                out.get_one_max_signal_us.insert(key, v);
            }
            (PROM_VALUE_MQ_GET_ONE_METRIC_POST, PROM_VALUE_MQ_STAT_AVG) => {
                out.get_one_avg_post_us.insert(key, v);
            }
            (PROM_VALUE_MQ_GET_ONE_METRIC_POST, PROM_VALUE_MQ_STAT_MAX) => {
                out.get_one_max_post_us.insert(key, v);
            }
            _ => {}
        }
    }

    let get_one_calls = q(
        prom,
        warnings,
        "mq_get_one_window_calls",
        PROM_METRIC_MQ_GET_ONE_WINDOW_CALLS,
    )
    .await;
    for s in &get_one_calls {
        let Some(chan_id_s) = s.metric.get(PROM_LABEL_MQ_CHAN_ID) else {
            continue;
        };
        let Ok(chan_id) = chan_id_s.parse::<i64>() else {
            continue;
        };
        let Some(consumer_idx) = s.metric.get(PROM_LABEL_MQ_CONSUMER_IDX) else {
            continue;
        };
        let Some(v) = s.value_f64() else {
            continue;
        };
        out.get_one_window_calls
            .insert((chan_id, consumer_idx.clone()), v);
    }

    let get_one_timeouts = q(
        prom,
        warnings,
        "mq_get_one_window_timeouts",
        PROM_METRIC_MQ_GET_ONE_WINDOW_TIMEOUTS,
    )
    .await;
    for s in &get_one_timeouts {
        let Some(chan_id_s) = s.metric.get(PROM_LABEL_MQ_CHAN_ID) else {
            continue;
        };
        let Ok(chan_id) = chan_id_s.parse::<i64>() else {
            continue;
        };
        let Some(consumer_idx) = s.metric.get(PROM_LABEL_MQ_CONSUMER_IDX) else {
            continue;
        };
        let Some(v) = s.value_f64() else {
            continue;
        };
        out.get_one_window_timeouts
            .insert((chan_id, consumer_idx.clone()), v);
    }

    let get_one_bytes = q(
        prom,
        warnings,
        "mq_get_one_window_bytes",
        PROM_METRIC_MQ_GET_ONE_WINDOW_BYTES,
    )
    .await;
    for s in &get_one_bytes {
        let Some(chan_id_s) = s.metric.get(PROM_LABEL_MQ_CHAN_ID) else {
            continue;
        };
        let Ok(chan_id) = chan_id_s.parse::<i64>() else {
            continue;
        };
        let Some(consumer_idx) = s.metric.get(PROM_LABEL_MQ_CONSUMER_IDX) else {
            continue;
        };
        let Some(v) = s.value_f64() else {
            continue;
        };
        out.get_one_window_bytes
            .insert((chan_id, consumer_idx.clone()), v);
    }

    let put_calls = q(
        prom,
        warnings,
        "mq_put_window_calls",
        PROM_METRIC_MQ_PUT_WINDOW_CALLS,
    )
    .await;
    for s in &put_calls {
        let Some(chan_id_s) = s.metric.get(PROM_LABEL_MQ_CHAN_ID) else {
            continue;
        };
        let Ok(chan_id) = chan_id_s.parse::<i64>() else {
            continue;
        };
        let Some(producer_idx) = s.metric.get(PROM_LABEL_MQ_PRODUCER_IDX) else {
            continue;
        };
        let Some(v) = s.value_f64() else {
            continue;
        };
        out.put_window_calls
            .insert((chan_id, producer_idx.clone()), v);
    }

    let put_bytes = q(
        prom,
        warnings,
        "mq_put_window_bytes",
        PROM_METRIC_MQ_PUT_WINDOW_BYTES,
    )
    .await;
    for s in &put_bytes {
        let Some(chan_id_s) = s.metric.get(PROM_LABEL_MQ_CHAN_ID) else {
            continue;
        };
        let Ok(chan_id) = chan_id_s.parse::<i64>() else {
            continue;
        };
        let Some(producer_idx) = s.metric.get(PROM_LABEL_MQ_PRODUCER_IDX) else {
            continue;
        };
        let Some(v) = s.value_f64() else {
            continue;
        };
        out.put_window_bytes
            .insert((chan_id, producer_idx.clone()), v);
    }

    let producer_nonblocking_latest_phase_calls = q(
        prom,
        warnings,
        "mq_producer_nonblocking_latest_phase_calls",
        PROM_METRIC_MQ_PRODUCER_NONBLOCKING_LATEST_PHASE_CALLS,
    )
    .await;
    for s in &producer_nonblocking_latest_phase_calls {
        let Some(chan_id_s) = s.metric.get(PROM_LABEL_MQ_CHAN_ID) else {
            continue;
        };
        let Ok(chan_id) = chan_id_s.parse::<i64>() else {
            continue;
        };
        let Some(producer_idx) = s.metric.get(PROM_LABEL_MQ_PRODUCER_IDX) else {
            continue;
        };
        let Some(v) = s.value_f64() else {
            continue;
        };
        out.producer_nonblocking_latest_phase_calls
            .insert((chan_id, producer_idx.clone()), v);
    }

    let producer_nonblocking_latest_phase_rps = q(
        prom,
        warnings,
        "mq_producer_nonblocking_latest_phase_rps",
        PROM_METRIC_MQ_PRODUCER_NONBLOCKING_LATEST_PHASE_RPS,
    )
    .await;
    for s in &producer_nonblocking_latest_phase_rps {
        let Some(chan_id_s) = s.metric.get(PROM_LABEL_MQ_CHAN_ID) else {
            continue;
        };
        let Ok(chan_id) = chan_id_s.parse::<i64>() else {
            continue;
        };
        let Some(producer_idx) = s.metric.get(PROM_LABEL_MQ_PRODUCER_IDX) else {
            continue;
        };
        let Some(v) = s.value_f64() else {
            continue;
        };
        out.producer_nonblocking_latest_phase_rps
            .insert((chan_id, producer_idx.clone()), v);
    }

    let producer_nonblocking_interval = q(
        prom,
        warnings,
        "mq_producer_nonblocking_latest_interval_unix_ms",
        PROM_METRIC_MQ_PRODUCER_NONBLOCKING_LATEST_INTERVAL_UNIX_MS,
    )
    .await;
    for s in &producer_nonblocking_interval {
        let Some(chan_id_s) = s.metric.get(PROM_LABEL_MQ_CHAN_ID) else {
            continue;
        };
        let Ok(chan_id) = chan_id_s.parse::<i64>() else {
            continue;
        };
        let Some(producer_idx) = s.metric.get(PROM_LABEL_MQ_PRODUCER_IDX) else {
            continue;
        };
        let Some(metric) = s.metric.get(PROM_LABEL_MQ_METRIC) else {
            continue;
        };
        let Some(v) = s.value_f64() else {
            continue;
        };
        let key = (chan_id, producer_idx.clone());
        match metric.as_str() {
            PROM_VALUE_MQ_INTERVAL_BEGIN => {
                out.producer_nonblocking_latest_begin_unix_ms.insert(key, v);
            }
            PROM_VALUE_MQ_INTERVAL_END => {
                out.producer_nonblocking_latest_end_unix_ms.insert(key, v);
            }
            _ => {}
        }
    }

    let consumer_nonblocking_latest_phase_calls = q(
        prom,
        warnings,
        "mq_consumer_nonblocking_latest_phase_calls",
        PROM_METRIC_MQ_CONSUMER_NONBLOCKING_LATEST_PHASE_CALLS,
    )
    .await;
    for s in &consumer_nonblocking_latest_phase_calls {
        let Some(chan_id_s) = s.metric.get(PROM_LABEL_MQ_CHAN_ID) else {
            continue;
        };
        let Ok(chan_id) = chan_id_s.parse::<i64>() else {
            continue;
        };
        let Some(consumer_idx) = s.metric.get(PROM_LABEL_MQ_CONSUMER_IDX) else {
            continue;
        };
        let Some(v) = s.value_f64() else {
            continue;
        };
        out.consumer_nonblocking_latest_phase_calls
            .insert((chan_id, consumer_idx.clone()), v);
    }

    let consumer_nonblocking_latest_phase_rps = q(
        prom,
        warnings,
        "mq_consumer_nonblocking_latest_phase_rps",
        PROM_METRIC_MQ_CONSUMER_NONBLOCKING_LATEST_PHASE_RPS,
    )
    .await;
    for s in &consumer_nonblocking_latest_phase_rps {
        let Some(chan_id_s) = s.metric.get(PROM_LABEL_MQ_CHAN_ID) else {
            continue;
        };
        let Ok(chan_id) = chan_id_s.parse::<i64>() else {
            continue;
        };
        let Some(consumer_idx) = s.metric.get(PROM_LABEL_MQ_CONSUMER_IDX) else {
            continue;
        };
        let Some(v) = s.value_f64() else {
            continue;
        };
        out.consumer_nonblocking_latest_phase_rps
            .insert((chan_id, consumer_idx.clone()), v);
    }

    let consumer_nonblocking_interval = q(
        prom,
        warnings,
        "mq_consumer_nonblocking_latest_interval_unix_ms",
        PROM_METRIC_MQ_CONSUMER_NONBLOCKING_LATEST_INTERVAL_UNIX_MS,
    )
    .await;
    for s in &consumer_nonblocking_interval {
        let Some(chan_id_s) = s.metric.get(PROM_LABEL_MQ_CHAN_ID) else {
            continue;
        };
        let Ok(chan_id) = chan_id_s.parse::<i64>() else {
            continue;
        };
        let Some(consumer_idx) = s.metric.get(PROM_LABEL_MQ_CONSUMER_IDX) else {
            continue;
        };
        let Some(metric) = s.metric.get(PROM_LABEL_MQ_METRIC) else {
            continue;
        };
        let Some(v) = s.value_f64() else {
            continue;
        };
        let key = (chan_id, consumer_idx.clone());
        match metric.as_str() {
            PROM_VALUE_MQ_INTERVAL_BEGIN => {
                out.consumer_nonblocking_latest_begin_unix_ms.insert(key, v);
            }
            PROM_VALUE_MQ_INTERVAL_END => {
                out.consumer_nonblocking_latest_end_unix_ms.insert(key, v);
            }
            _ => {}
        }
    }

    out
}

fn take_node_mount_metric(
    samples: &[PromSample],
) -> HashMap<(String, FsMountKind, String, String), f64> {
    let mut out = HashMap::new();
    for s in samples {
        let Some(node) = s.metric.get(PROM_LABEL_NODE) else {
            continue;
        };
        let Some(kind_s) = s.metric.get(PROM_LABEL_FS_MOUNT_KIND) else {
            continue;
        };
        let Some(kind) = FsMountKind::parse_label(kind_s.as_str()) else {
            continue;
        };
        let Some(target_dir_abs) = s.metric.get(PROM_LABEL_FS_TARGET_DIR_ABS) else {
            continue;
        };
        let Some(mountpoint_dir_abs) = s.metric.get(PROM_LABEL_FS_MOUNTPOINT_DIR_ABS) else {
            continue;
        };
        let Some(v) = s.value_f64() else {
            continue;
        };
        out.insert(
            (
                node.clone(),
                kind,
                mountpoint_dir_abs.clone(),
                target_dir_abs.clone(),
            ),
            v,
        );
    }
    out
}

fn take_shm_file_metric(samples: &[PromSample]) -> HashMap<(String, String, String), f64> {
    let mut out = HashMap::new();
    for s in samples {
        let Some(node) = s.metric.get(PROM_LABEL_NODE) else {
            continue;
        };
        let Some(shm_dir_abs) = s.metric.get("shm_dir_abs") else {
            continue;
        };
        let Some(file_path_abs) = s.metric.get("file_path_abs") else {
            continue;
        };
        let Some(v) = s.value_f64() else {
            continue;
        };
        out.insert(
            (node.clone(), shm_dir_abs.clone(), file_path_abs.clone()),
            v,
        );
    }
    out
}

pub async fn collect_prom_snapshot(
    prom: &PromClient,
    warnings: &mut Vec<String>,
) -> PromSnapshotMaps {
    let mut out = PromSnapshotMaps::empty();

    // English note:
    // - Topology UI uses per-peer bandwidth as the primary signal and needs a short window for "real-time" feel.
    // - KV peer bytes are flushed on a 30s tick; `rate(...[30s])` can become empty because it may not
    //   contain 2 samples. Use a larger window to stabilize correctness.
    // - Keep this explicit to avoid hidden defaults drifting between emitters/query/UI.
    const KV_PEER_RATE_WINDOW: &str = "2m";
    const NODE_NETDEV_RATE_WINDOW: &str = "2m";
    const FS_IO_RATE_WINDOW: &str = "30s";

    async fn q(
        prom: &PromClient,
        warnings: &mut Vec<String>,
        label: &str,
        expr: &str,
    ) -> Vec<PromSample> {
        match prom.query_instant(expr).await {
            Ok(v) => v,
            Err(e) => {
                warnings.push(format!("prometheus query failed ({label}): {e}"));
                Vec::new()
            }
        }
    }

    out.node_cpu_usage_percent = take_node_metric(
        &q(
            prom,
            warnings,
            "node_cpu_usage_percent",
            "node_cpu_usage_percent",
        )
        .await,
    );
    out.node_cpu_logical_cores = take_node_metric(
        &q(
            prom,
            warnings,
            "node_cpu_logical_cores",
            "node_cpu_logical_cores",
        )
        .await,
    );
    out.node_memory_usage_bytes = take_node_metric(
        &q(
            prom,
            warnings,
            "node_memory_usage_bytes",
            "node_memory_usage_bytes",
        )
        .await,
    );
    out.node_memory_total_bytes = take_node_metric(
        &q(
            prom,
            warnings,
            "node_memory_total_bytes",
            "node_memory_total_bytes",
        )
        .await,
    );
    out.container_memory_usage_bytes = take_node_metric(
        &q(
            prom,
            warnings,
            PROM_METRIC_CONTAINER_MEMORY_USAGE_BYTES,
            PROM_METRIC_CONTAINER_MEMORY_USAGE_BYTES,
        )
        .await,
    );
    out.container_memory_limit_bytes = take_node_metric(
        &q(
            prom,
            warnings,
            PROM_METRIC_CONTAINER_MEMORY_LIMIT_BYTES,
            PROM_METRIC_CONTAINER_MEMORY_LIMIT_BYTES,
        )
        .await,
    );
    out.process_resident_memory_bytes = take_node_metric(
        &q(
            prom,
            warnings,
            "process_resident_memory_bytes",
            "process_resident_memory_bytes",
        )
        .await,
    );
    out.process_cpu_usage_percent = take_node_metric(
        &q(
            prom,
            warnings,
            "process_cpu_usage_percent",
            "process_cpu_usage_percent",
        )
        .await,
    );
    out.tokio_num_workers = take_node_metric(
        &q(
            prom,
            warnings,
            PROM_METRIC_TOKIO_NUM_WORKERS,
            PROM_METRIC_TOKIO_NUM_WORKERS,
        )
        .await,
    );
    out.tokio_alive_tasks = take_node_metric(
        &q(
            prom,
            warnings,
            PROM_METRIC_TOKIO_ALIVE_TASKS,
            PROM_METRIC_TOKIO_ALIVE_TASKS,
        )
        .await,
    );
    out.tokio_global_queue_depth = take_node_metric(
        &q(
            prom,
            warnings,
            PROM_METRIC_TOKIO_GLOBAL_QUEUE_DEPTH,
            PROM_METRIC_TOKIO_GLOBAL_QUEUE_DEPTH,
        )
        .await,
    );
    out.tokio_busy_percent = take_node_metric(
        &q(
            prom,
            warnings,
            PROM_METRIC_TOKIO_BUSY_PERCENT,
            PROM_METRIC_TOKIO_BUSY_PERCENT,
        )
        .await,
    );
    out.tokio_max_worker_busy_percent = take_node_metric(
        &q(
            prom,
            warnings,
            PROM_METRIC_TOKIO_MAX_WORKER_BUSY_PERCENT,
            PROM_METRIC_TOKIO_MAX_WORKER_BUSY_PERCENT,
        )
        .await,
    );
    out.tokio_park_unpark_rate_hz = take_node_metric(
        &q(
            prom,
            warnings,
            PROM_METRIC_TOKIO_PARK_UNPARK_RATE_HZ,
            PROM_METRIC_TOKIO_PARK_UNPARK_RATE_HZ,
        )
        .await,
    );

    out.process_network_tx_mbps = take_node_metric(
        &q(
            prom,
            warnings,
            "process_network_tx_mbps",
            "sum by (node) (rate(client_network_bytes_total{direction=\"tx\"}[2m])) * 8 / 1000000",
        )
        .await,
    );
    out.process_network_rx_mbps = take_node_metric(
        &q(
            prom,
            warnings,
            "process_network_rx_mbps",
            "sum by (node) (rate(client_network_bytes_total{direction=\"rx\"}[2m])) * 8 / 1000000",
        )
        .await,
    );
    let node_netdev_tx_expr = format!(
        "sum by (node, device) (rate(node_network_transmit_bytes_total[{}])) * 8 / 1000000",
        NODE_NETDEV_RATE_WINDOW,
    );
    out.node_network_tx_mbps_by_node_device = take_node_device_metric(
        &q(
            prom,
            warnings,
            "node_network_tx_mbps_by_node_device",
            &node_netdev_tx_expr,
        )
        .await,
    );
    let node_netdev_rx_expr = format!(
        "sum by (node, device) (rate(node_network_receive_bytes_total[{}])) * 8 / 1000000",
        NODE_NETDEV_RATE_WINDOW,
    );
    out.node_network_rx_mbps_by_node_device = take_node_device_metric(
        &q(
            prom,
            warnings,
            "node_network_rx_mbps_by_node_device",
            &node_netdev_rx_expr,
        )
        .await,
    );

    let kv_peer_tx_expr = format!(
        "sum by ({}, {}) (rate({}{{direction=\"tx\"}}[{}])) * 8 / 1000000",
        PROM_LABEL_NODE,
        PROM_LABEL_PEER,
        PROM_METRIC_KV_PEER_NETWORK_BYTES_TOTAL,
        KV_PEER_RATE_WINDOW,
    );
    out.kv_peer_network_tx_mbps_by_node_peer = take_node_peer_metric(
        &q(
            prom,
            warnings,
            "kv_peer_network_tx_mbps_by_node_peer",
            &kv_peer_tx_expr,
        )
        .await,
    );

    let kv_peer_rx_expr = format!(
        "sum by ({}, {}) (rate({}{{direction=\"rx\"}}[{}])) * 8 / 1000000",
        PROM_LABEL_NODE,
        PROM_LABEL_PEER,
        PROM_METRIC_KV_PEER_NETWORK_BYTES_TOTAL,
        KV_PEER_RATE_WINDOW,
    );
    out.kv_peer_network_rx_mbps_by_node_peer = take_node_peer_metric(
        &q(
            prom,
            warnings,
            "kv_peer_network_rx_mbps_by_node_peer",
            &kv_peer_rx_expr,
        )
        .await,
    );

    out.fs_mount_fs_used_bytes_by_node_kind_mountpoint_target = take_node_mount_metric(
        &q(
            prom,
            warnings,
            "fs_mount_fs_used_bytes_by_node_kind_mountpoint_target",
            PROM_METRIC_FS_MOUNT_FS_USED_BYTES,
        )
        .await,
    );
    out.fs_mount_fs_total_bytes_by_node_kind_mountpoint_target = take_node_mount_metric(
        &q(
            prom,
            warnings,
            "fs_mount_fs_total_bytes_by_node_kind_mountpoint_target",
            PROM_METRIC_FS_MOUNT_FS_TOTAL_BYTES,
        )
        .await,
    );
    out.shm_file_size_bytes_by_node_dir_file = take_shm_file_metric(
        &q(
            prom,
            warnings,
            PROM_METRIC_SHM_FILE_SIZE_BYTES,
            PROM_METRIC_SHM_FILE_SIZE_BYTES,
        )
        .await,
    );
    out.shm_file_allocated_bytes_by_node_dir_file = take_shm_file_metric(
        &q(
            prom,
            warnings,
            PROM_METRIC_SHM_FILE_ALLOCATED_BYTES,
            PROM_METRIC_SHM_FILE_ALLOCATED_BYTES,
        )
        .await,
    );

    let fs_read_expr = format!(
        "sum by ({}) (rate({}{{{}=\"read\"}}[{}]))",
        PROM_LABEL_NODE, PROM_METRIC_FS_IO_OPS_TOTAL, PROM_LABEL_FS_IO_OP, FS_IO_RATE_WINDOW
    );
    out.fs_read_rps = take_node_metric(&q(prom, warnings, "fs_read_rps", &fs_read_expr).await);

    let fs_write_expr = format!(
        "sum by ({}) (rate({}{{{}=\"write\"}}[{}]))",
        PROM_LABEL_NODE, PROM_METRIC_FS_IO_OPS_TOTAL, PROM_LABEL_FS_IO_OP, FS_IO_RATE_WINDOW
    );
    out.fs_write_rps = take_node_metric(&q(prom, warnings, "fs_write_rps", &fs_write_expr).await);

    // Approximate per-node throughput over a 1s window (Grafana uses sum_over_time).
    out.put_rps = take_node_metric(
        &q(
            prom,
            warnings,
            "put_rps",
            "sum by (node) (sum_over_time(kv_op_end_event{op=\"put\",status=\"success\"}[1s]))",
        )
        .await,
    );
    out.get_rps = take_node_metric(
        &q(
            prom,
            warnings,
            "get_rps",
            "sum by (node) (sum_over_time(kv_op_end_event{op=\"get\",status=~\"hit|success\"}[1s]))",
        )
        .await,
    );
    out.put_bps = take_node_metric(
        &q(
            prom,
            warnings,
            "put_bps",
            "sum by (node) (sum_over_time(kv_op_end_bytes{op=\"put\",status=\"success\"}[1s]))",
        )
        .await,
    );
    out.get_bps = take_node_metric(
        &q(
            prom,
            warnings,
            "get_bps",
            "sum by (node) (sum_over_time(kv_op_end_bytes{op=\"get\",status=~\"hit|success\"}[1s]))",
        )
        .await,
    );

    // Client aggregated latency gauges
    out.put_latency_mean_us = take_client_metric(
        &q(
            prom,
            warnings,
            "put_latency_mean_us",
            "kv_operation_latency_stat_microseconds{metric=\"put_whole\",stat=\"mean\"}",
        )
        .await,
    );
    out.put_latency_p95_us = take_client_metric(
        &q(
            prom,
            warnings,
            "put_latency_p95_us",
            "kv_operation_latency_stat_microseconds{metric=\"put_whole\",stat=\"p95\"}",
        )
        .await,
    );
    out.put_latency_p99_us = take_client_metric(
        &q(
            prom,
            warnings,
            "put_latency_p99_us",
            "kv_operation_latency_stat_microseconds{metric=\"put_whole\",stat=\"p99\"}",
        )
        .await,
    );
    out.get_latency_mean_us = take_client_metric(
        &q(
            prom,
            warnings,
            "get_latency_mean_us",
            "kv_operation_latency_stat_microseconds{metric=\"get_whole\",stat=\"mean\"}",
        )
        .await,
    );
    out.get_latency_p95_us = take_client_metric(
        &q(
            prom,
            warnings,
            "get_latency_p95_us",
            "kv_operation_latency_stat_microseconds{metric=\"get_whole\",stat=\"p95\"}",
        )
        .await,
    );
    out.get_latency_p99_us = take_client_metric(
        &q(
            prom,
            warnings,
            "get_latency_p99_us",
            "kv_operation_latency_stat_microseconds{metric=\"get_whole\",stat=\"p99\"}",
        )
        .await,
    );

    // Segment usage/capacity per (node, device).
    out.seg_capacity_bytes_by_node_device = take_node_device_metric(
        &q(
            prom,
            warnings,
            "seg_capacity_bytes_by_node_device",
            "kvcache_segment_capacity_bytes",
        )
        .await,
    );
    out.seg_used_bytes_by_node_device = take_node_device_metric(
        &q(
            prom,
            warnings,
            "seg_used_bytes_by_node_device",
            "kvcache_segment_used_bytes",
        )
        .await,
    );

    out
}
