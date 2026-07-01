use crate::client_kv_api::{KvMetrics, MetricsSet};
use crate::master_kv_router::put::PutIDForAKey;
use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use fluxon_observability::kv_metrics_actor::{
    KvOpEndBytesPulse as ObserveOpEndBytesPulse, KvOpMetric, KvOpMetricGet, KvOpMetricPut,
    ObserveCacheEvent, ObserveDirection, ObserveFsIoOp, ObserveHandle, ObserveOp,
};
use fluxon_observability::types::FsMountKind;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{OnceLock, RwLock};

pub struct MetricsHandle {
    observe: OnceLock<ObserveHandle>,
    observability_disabled: bool,

    // These queues are only for the user API (Python get_metrics snapshot). They are not used for
    // remote-write; remote-write is fully owned by fluxon_observability actor.
    put_metrics_queue: SegQueue<KvMetrics>,
    get_metrics_queue: SegQueue<KvMetrics>,

    // Latest aggregated metrics snapshot for the user API (Python get_metrics).
    latest_metrics_snapshot: RwLock<HashMap<String, MetricsSet>>,

    // Pending put stats captured at put_start to attribute external-owner path at put_end.
    pending_put_stats: DashMap<PutIDForAKey, PendingPutStat>,
}

#[derive(Clone, Debug)]
pub struct PendingPutStat {
    pub key: String,
    pub len: u64,
    pub t1_us: i64,
    pub t2_us: i64,
    pub t3_us: Option<i64>,
    pub start_handle_us: i64,
    pub end_handle_us: Option<i64>,
    pub transfer_submit_blocking_us: i64,
    pub transfer_create_xfer_req_us: i64,
    pub transfer_post_xfer_req_us: i64,
    pub transfer_poll_wait_us: i64,
    pub transfer_poll_iters: i64,
    pub transfer_used_fast_path: bool,
    pub transfer_local_noop: bool,
    pub transfer_remote_transfer: bool,
    pub top_emitted: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationKind {
    Put,
    Get,
}

impl OperationKind {
    pub const fn as_label(self) -> &'static str {
        match self {
            OperationKind::Put => "put",
            OperationKind::Get => "get",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestStage {
    Start,
    Transfer,
    End,
    Total,
    Cache,
    Rpc,
}

impl RequestStage {
    pub const fn as_label(self) -> &'static str {
        match self {
            RequestStage::Start => "start",
            RequestStage::Transfer => "transfer",
            RequestStage::End => "end",
            RequestStage::Total => "total",
            RequestStage::Cache => "cache",
            RequestStage::Rpc => "rpc",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestStatus {
    Success,
    Error,
    NotFound,
    Hit,
    Miss,
}

impl RequestStatus {
    pub const fn as_label(self) -> &'static str {
        match self {
            RequestStatus::Success => "success",
            RequestStatus::Error => "error",
            RequestStatus::NotFound => "not_found",
            RequestStatus::Hit => "hit",
            RequestStatus::Miss => "miss",
        }
    }
}

// Direction marker for traffic-related metrics.
#[derive(Clone, Copy)]
pub enum TrafficDirection {
    Tx,
    Rx,
}

impl TrafficDirection {
    pub const fn as_label(self) -> &'static str {
        match self {
            TrafficDirection::Tx => "tx",
            TrafficDirection::Rx => "rx",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsIoOp {
    Read,
    Write,
}

impl FsIoOp {
    pub const fn as_label(self) -> &'static str {
        match self {
            FsIoOp::Read => "read",
            FsIoOp::Write => "write",
        }
    }
}

impl MetricsHandle {
    pub fn new(observability_disabled: bool) -> Self {
        Self {
            observe: OnceLock::new(),
            observability_disabled,
            put_metrics_queue: SegQueue::new(),
            get_metrics_queue: SegQueue::new(),
            latest_metrics_snapshot: RwLock::new(HashMap::new()),
            pending_put_stats: DashMap::new(),
        }
    }

    pub fn attach_observe_handle(&self, handle: ObserveHandle) {
        self.observe
            .set(handle)
            .unwrap_or_else(|_| panic!("ObserveHandle attached twice"));
    }

    fn observe(&self) -> Option<&ObserveHandle> {
        self.observe.get()
    }

    pub fn observability_disabled(&self) -> bool {
        self.observability_disabled
    }

    /// Store the latest computed metrics snapshot for non-draining reads.
    pub fn set_latest_metrics_snapshot(&self, snapshot: HashMap<String, MetricsSet>) {
        if let Ok(mut guard) = self.latest_metrics_snapshot.write() {
            *guard = snapshot;
        } else {
            tracing::warn!("failed to write latest_metrics_snapshot");
        }
    }

    /// Get the latest metrics snapshot (does not drain internal queues).
    pub fn get_latest_metrics_snapshot(&self) -> HashMap<String, MetricsSet> {
        match self.latest_metrics_snapshot.read() {
            Ok(guard) => guard.clone(),
            Err(_) => {
                tracing::warn!("failed to read latest_metrics_snapshot");
                HashMap::new()
            }
        }
    }

    pub fn emit_op_end_bytes_pulse(
        &self,
        op: OperationKind,
        status: RequestStatus,
        key: &str,
        bytes: u64,
        timestamp_ms: i64,
    ) {
        let Some(observe) = self.observe() else {
            return;
        };

        observe.try_submit(ObserveOp::EmitOpEndBytesPulse {
            pulse: ObserveOpEndBytesPulse {
                timestamp_ms,
                op: op.as_label(),
                status: status.as_label(),
                key: sanitize_key(key).to_string(),
                bytes,
            },
        });
    }

    pub fn record_client_network_bytes(
        &self,
        _node: &str,
        _role: &str,
        direction: TrafficDirection,
        bytes: u64,
    ) {
        if bytes == 0 {
            return;
        }

        let Some(observe) = self.observe() else {
            return;
        };
        observe.try_submit(ObserveOp::RecordClientNetworkBytes {
            direction: match direction {
                TrafficDirection::Tx => ObserveDirection::Tx,
                TrafficDirection::Rx => ObserveDirection::Rx,
            },
            bytes,
        });
    }

    pub fn set_segment_capacity_bytes(&self, node: &str, device: &str, bytes: u64) {
        let Some(observe) = self.observe() else {
            return;
        };
        observe.try_submit(ObserveOp::SetSegmentCapacityBytes {
            node: node.to_string(),
            device: device.to_string(),
            bytes,
        });
    }

    pub fn set_segment_used_bytes(&self, node: &str, device: &str, bytes: u64) {
        let Some(observe) = self.observe() else {
            return;
        };
        observe.try_submit(ObserveOp::SetSegmentUsedBytes {
            node: node.to_string(),
            device: device.to_string(),
            bytes,
        });
    }

    pub fn set_fs_mount_fs_bytes(
        &self,
        mount_kind: FsMountKind,
        target_dir_abs: &str,
        mountpoint_dir_abs: &str,
        used_bytes: u64,
        total_bytes: u64,
    ) {
        let Some(observe) = self.observe() else {
            return;
        };
        observe.try_submit(ObserveOp::SetFsMountFsBytes {
            mount_kind,
            target_dir_abs: target_dir_abs.to_string(),
            mountpoint_dir_abs: mountpoint_dir_abs.to_string(),
            used_bytes,
            total_bytes,
        });
    }

    pub fn set_shm_file_bytes(
        &self,
        shm_dir_abs: &str,
        file_path_abs: &str,
        logical_size_bytes: u64,
        allocated_bytes: u64,
    ) {
        let Some(observe) = self.observe() else {
            return;
        };
        observe.set_shm_file_bytes(
            shm_dir_abs,
            file_path_abs,
            logical_size_bytes,
            allocated_bytes,
        );
    }

    pub fn record_fs_io_ops(&self, op: FsIoOp, ops: u64) {
        let Some(observe) = self.observe() else {
            return;
        };
        observe.try_submit(ObserveOp::RecordFsIoOps {
            op: match op {
                FsIoOp::Read => ObserveFsIoOp::Read,
                FsIoOp::Write => ObserveFsIoOp::Write,
            },
            ops,
        });
    }

    pub fn push_put_metric(&self, metric: KvMetrics) {
        if self.observability_disabled() {
            if let Some(seq) = sampled_debug_seq(&KV_METRIC_OBSERVABILITY_DISABLED_SEQ) {
                tracing::debug!(
                    "skip kv op metric submit: observability_disabled=true seq={} kind=put",
                    seq
                );
            }
            return;
        }
        if let KvMetrics::Put {
            whole_put,
            start,
            transfer,
            end,
            rpc_of_put_start,
            start_handle,
            end_handle,
            key,
            put_id,
            start_timestamp_us,
            transfer_start_timestamp_us,
            end_start_timestamp_us,
            end_timestamp_us,
            transfer_submit_blocking_us,
            transfer_create_xfer_req_us,
            transfer_post_xfer_req_us,
            transfer_poll_wait_us,
            transfer_poll_iters,
            transfer_used_fast_path,
            transfer_local_noop,
            transfer_remote_transfer,
        } = &metric
        {
            let Some(observe) = self.observe() else {
                if let Some(seq) = sampled_debug_seq(&PUT_METRIC_MISSING_OBSERVE_SEQ) {
                    tracing::debug!(
                        "kv op metric not submitted: observe handle missing seq={} kind=put put_id={} key={} whole_us={} transfer_us={} t1_us={} t4_us={}",
                        seq,
                        put_id,
                        sanitize_key(key),
                        whole_put,
                        transfer,
                        start_timestamp_us,
                        end_timestamp_us
                    );
                }
                self.put_metrics_queue.push(metric);
                return;
            };

            if let Some(seq) = sampled_debug_seq(&PUT_METRIC_SUBMIT_SEQ) {
                tracing::debug!(
                    "submit kv op metric seq={} kind=put put_id={} key={} whole_us={} start_us={} transfer_us={} end_us={} rpc_us={} start_handle_us={} end_handle_us={} t1_us={} t2_us={} t3_us={} t4_us={} transfer_submit_blocking_us={} transfer_create_xfer_req_us={} transfer_post_xfer_req_us={} transfer_poll_wait_us={} transfer_poll_iters={} transfer_used_fast_path={} transfer_local_noop={} transfer_remote_transfer={}",
                    seq,
                    put_id,
                    sanitize_key(key),
                    whole_put,
                    start,
                    transfer,
                    end,
                    rpc_of_put_start,
                    start_handle,
                    end_handle,
                    start_timestamp_us,
                    transfer_start_timestamp_us,
                    end_start_timestamp_us,
                    end_timestamp_us,
                    transfer_submit_blocking_us,
                    transfer_create_xfer_req_us,
                    transfer_post_xfer_req_us,
                    transfer_poll_wait_us,
                    transfer_poll_iters,
                    transfer_used_fast_path,
                    transfer_local_noop,
                    transfer_remote_transfer
                );
            }

            observe.try_submit(ObserveOp::SubmitKvOpMetric {
                metric: KvOpMetric::Put(KvOpMetricPut {
                    whole_put_us: *whole_put,
                    start_us: *start,
                    transfer_us: *transfer,
                    end_us: *end,
                    rpc_of_put_start_us: *rpc_of_put_start,
                    start_handle_us: *start_handle,
                    end_handle_us: *end_handle,
                    key: key.clone(),
                    put_id: put_id.clone(),
                    t1_us: *start_timestamp_us,
                    t2_us: *transfer_start_timestamp_us,
                    t3_us: *end_start_timestamp_us,
                    t4_us: *end_timestamp_us,
                    transfer_submit_blocking_us: *transfer_submit_blocking_us,
                    transfer_create_xfer_req_us: *transfer_create_xfer_req_us,
                    transfer_post_xfer_req_us: *transfer_post_xfer_req_us,
                    transfer_poll_wait_us: *transfer_poll_wait_us,
                    transfer_poll_iters: *transfer_poll_iters,
                    transfer_used_fast_path: *transfer_used_fast_path,
                    transfer_local_noop: *transfer_local_noop,
                    transfer_remote_transfer: *transfer_remote_transfer,
                }),
            });
        }
        self.put_metrics_queue.push(metric);
    }

    pub fn push_get_metric(&self, metric: KvMetrics) {
        if self.observability_disabled() {
            if let Some(seq) = sampled_debug_seq(&KV_METRIC_OBSERVABILITY_DISABLED_SEQ) {
                tracing::debug!(
                    "skip kv op metric submit: observability_disabled=true seq={} kind=get",
                    seq
                );
            }
            return;
        }
        if let KvMetrics::Get {
            whole_get,
            start,
            transfer,
            end,
            start_handle,
            end_handle,
            key,
            get_id,
            start_timestamp_us,
            transfer_start_timestamp_us,
            end_start_timestamp_us,
            end_timestamp_us,
        } = &metric
        {
            let Some(observe) = self.observe() else {
                if let Some(seq) = sampled_debug_seq(&GET_METRIC_MISSING_OBSERVE_SEQ) {
                    tracing::debug!(
                        "kv op metric not submitted: observe handle missing seq={} kind=get get_id={} key={} whole_us={} transfer_us={} t1_us={} t4_us={}",
                        seq,
                        get_id,
                        sanitize_key(key),
                        whole_get,
                        transfer,
                        start_timestamp_us,
                        end_timestamp_us
                    );
                }
                self.get_metrics_queue.push(metric);
                return;
            };

            if let Some(seq) = sampled_debug_seq(&GET_METRIC_SUBMIT_SEQ) {
                tracing::debug!(
                    "submit kv op metric seq={} kind=get get_id={} key={} whole_us={} start_us={} transfer_us={} end_us={} start_handle_us={} end_handle_us={} t1_us={} t2_us={} t3_us={} t4_us={}",
                    seq,
                    get_id,
                    sanitize_key(key),
                    whole_get,
                    start,
                    transfer,
                    end,
                    start_handle,
                    end_handle,
                    start_timestamp_us,
                    transfer_start_timestamp_us,
                    end_start_timestamp_us,
                    end_timestamp_us
                );
            }

            observe.try_submit(ObserveOp::SubmitKvOpMetric {
                metric: KvOpMetric::Get(KvOpMetricGet {
                    whole_get_us: *whole_get,
                    start_us: *start,
                    transfer_us: *transfer,
                    end_us: *end,
                    start_handle_us: *start_handle,
                    end_handle_us: *end_handle,
                    key: key.clone(),
                    get_id: get_id.clone(),
                    t1_us: *start_timestamp_us,
                    t2_us: *transfer_start_timestamp_us,
                    t3_us: *end_start_timestamp_us,
                    t4_us: *end_timestamp_us,
                }),
            });
        }
        self.get_metrics_queue.push(metric);
    }

    pub fn drain_put_metrics(&self) -> Vec<KvMetrics> {
        let mut drained = Vec::new();
        while let Some(item) = self.put_metrics_queue.pop() {
            drained.push(item);
        }
        drained
    }

    pub fn drain_get_metrics(&self) -> Vec<KvMetrics> {
        let mut drained = Vec::new();
        while let Some(item) = self.get_metrics_queue.pop() {
            drained.push(item);
        }
        drained
    }

    // ---- pending put helpers (external-owner fast path attribution) ----
    pub fn pending_put_insert(
        &self,
        put_id: PutIDForAKey,
        key: String,
        len: u64,
        t1_us: i64,
        t2_us: i64,
        start_handle_us: i64,
    ) {
        if self.observability_disabled() {
            return;
        }
        self.pending_put_stats.insert(
            put_id,
            PendingPutStat {
                key,
                len,
                t1_us,
                t2_us,
                t3_us: None,
                start_handle_us,
                end_handle_us: None,
                transfer_submit_blocking_us: 0,
                transfer_create_xfer_req_us: 0,
                transfer_post_xfer_req_us: 0,
                transfer_poll_wait_us: 0,
                transfer_poll_iters: 0,
                transfer_used_fast_path: false,
                transfer_local_noop: false,
                transfer_remote_transfer: false,
                top_emitted: false,
            },
        );
    }

    pub fn pending_put_mark_top_emitted(&self, put_id: PutIDForAKey) {
        if self.observability_disabled() {
            return;
        }
        if let Some(mut guard) = self.pending_put_stats.get_mut(&put_id) {
            guard.top_emitted = true;
        }
    }

    pub fn pending_put_set_t3(&self, put_id: PutIDForAKey, t3_us: i64) {
        if self.observability_disabled() {
            return;
        }
        if let Some(mut guard) = self.pending_put_stats.get_mut(&put_id) {
            guard.t3_us = Some(t3_us);
        }
    }

    pub fn pending_put_set_end_handle(&self, put_id: PutIDForAKey, end_handle_us: i64) {
        if self.observability_disabled() {
            return;
        }
        if let Some(mut guard) = self.pending_put_stats.get_mut(&put_id) {
            guard.end_handle_us = Some(end_handle_us);
        }
    }

    pub fn pending_put_set_transfer_breakdown(
        &self,
        put_id: PutIDForAKey,
        transfer_submit_blocking_us: i64,
        transfer_create_xfer_req_us: i64,
        transfer_post_xfer_req_us: i64,
        transfer_poll_wait_us: i64,
        transfer_poll_iters: i64,
        transfer_used_fast_path: bool,
        transfer_local_noop: bool,
        transfer_remote_transfer: bool,
    ) {
        if self.observability_disabled() {
            return;
        }
        if let Some(mut guard) = self.pending_put_stats.get_mut(&put_id) {
            guard.transfer_submit_blocking_us = transfer_submit_blocking_us;
            guard.transfer_create_xfer_req_us = transfer_create_xfer_req_us;
            guard.transfer_post_xfer_req_us = transfer_post_xfer_req_us;
            guard.transfer_poll_wait_us = transfer_poll_wait_us;
            guard.transfer_poll_iters = transfer_poll_iters;
            guard.transfer_used_fast_path = transfer_used_fast_path;
            guard.transfer_local_noop = transfer_local_noop;
            guard.transfer_remote_transfer = transfer_remote_transfer;
        }
    }

    pub fn pending_put_peek(&self, put_id: &PutIDForAKey) -> Option<PendingPutStat> {
        if self.observability_disabled() {
            return None;
        }
        self.pending_put_stats.get(put_id).map(|g| g.clone())
    }

    pub fn pending_put_remove(
        &self,
        put_id: &PutIDForAKey,
    ) -> Option<(PutIDForAKey, PendingPutStat)> {
        if self.observability_disabled() {
            return None;
        }
        self.pending_put_stats.remove(put_id)
    }

    // ---- legacy APIs kept as no-ops (call sites still exist) ----
    pub fn observe_request_with_labels(
        &self,
        _node: &str,
        _op: OperationKind,
        _stage: RequestStage,
        _status: RequestStatus,
        _key: &str,
        _bytes: u64,
    ) {
    }

    pub fn observe_request_duration_with_labels(
        &self,
        _op: OperationKind,
        _stage: RequestStage,
        _seconds: f64,
    ) {
    }

    pub fn record_cache_hit(&self, _node: &str, _role: &str) {
        let Some(observe) = self.observe() else {
            return;
        };
        observe.try_submit(ObserveOp::RecordCacheEvent {
            event: ObserveCacheEvent::Hit,
        });
    }
    pub fn record_cache_miss(&self, _node: &str, _role: &str) {
        let Some(observe) = self.observe() else {
            return;
        };
        observe.try_submit(ObserveOp::RecordCacheEvent {
            event: ObserveCacheEvent::Miss,
        });
    }
    pub fn set_cache_bytes(&self, _node: &str, _role: &str, _total_bytes: i64) {}
    pub fn observe_cache_value_size(&self, _node: &str, _role: &str, _size_bytes: u64) {}
}

fn sanitize_key(key: &str) -> Cow<'_, str> {
    const MAX_KEY_LEN: usize = 64;
    if key.len() > MAX_KEY_LEN {
        let mut truncated = key[..MAX_KEY_LEN].to_string();
        truncated.push_str("...");
        Cow::Owned(truncated)
    } else {
        Cow::Borrowed(key)
    }
}

fn sampled_debug_seq(counter: &'static AtomicU64) -> Option<u64> {
    let seq = counter.fetch_add(1, Ordering::Relaxed) + 1;
    (seq <= 8 || seq.is_power_of_two()).then_some(seq)
}

static KV_METRIC_OBSERVABILITY_DISABLED_SEQ: AtomicU64 = AtomicU64::new(0);
static PUT_METRIC_SUBMIT_SEQ: AtomicU64 = AtomicU64::new(0);
static GET_METRIC_SUBMIT_SEQ: AtomicU64 = AtomicU64::new(0);
static PUT_METRIC_MISSING_OBSERVE_SEQ: AtomicU64 = AtomicU64::new(0);
static GET_METRIC_MISSING_OBSERVE_SEQ: AtomicU64 = AtomicU64::new(0);

// Expose metrics client side (RPC registration and query) as a submodule.
pub mod client;
pub mod datasource;
