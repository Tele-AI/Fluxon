use crate::client_kv_api::KvMetrics;
use crate::cluster_manager::{NodeIDStr, NodeRole};
use crate::master_kv_router::put::PutIDForAKey;
use crate::metrics::{MetricsHandle, OperationKind, RequestStage, RequestStatus, TrafficDirection};
use chrono::Utc;
use std::sync::Arc;

// ----------------------
// PUT wrappers
// ----------------------

#[inline]
pub fn obe_put_start_error_rpc(
    metrics: &Arc<MetricsHandle>,
    client_id: &NodeIDStr,
    _node_role: &NodeRole,
    key: &str,
    payload_len: u64,
) {
    let now_ms = (Utc::now().timestamp_micros() / 1000) as i64;
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Put,
        RequestStage::Start,
        RequestStatus::Error,
        key,
        payload_len,
    );
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Put,
        RequestStage::Total,
        RequestStatus::Error,
        key,
        0,
    );
    metrics.emit_op_end_bytes_pulse(OperationKind::Put, RequestStatus::Error, key, 0, now_ms);
}

#[inline]
pub fn obe_put_start_error_status(
    metrics: &Arc<MetricsHandle>,
    client_id: &NodeIDStr,
    _node_role: &NodeRole,
    key: &str,
    payload_len: u64,
) {
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Put,
        RequestStage::Start,
        RequestStatus::Error,
        key,
        payload_len,
    );
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Put,
        RequestStage::Total,
        RequestStatus::Error,
        key,
        0,
    );
}

#[inline]
pub fn obe_put_transfer_error(
    metrics: &Arc<MetricsHandle>,
    client_id: &NodeIDStr,
    _node_role: &NodeRole,
    key: &str,
    payload_len: u64,
) {
    let now_ms = (Utc::now().timestamp_micros() / 1000) as i64;
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Put,
        RequestStage::Transfer,
        RequestStatus::Error,
        key,
        payload_len,
    );
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Put,
        RequestStage::Total,
        RequestStatus::Error,
        key,
        0,
    );
    metrics.emit_op_end_bytes_pulse(OperationKind::Put, RequestStatus::Error, key, 0, now_ms);
}

#[inline]
pub fn obe_put_end_error(
    metrics: &Arc<MetricsHandle>,
    client_id: &NodeIDStr,
    _node_role: &NodeRole,
    key: &str,
    payload_len: u64,
) {
    let now_ms = (Utc::now().timestamp_micros() / 1000) as i64;
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Put,
        RequestStage::End,
        RequestStatus::Error,
        key,
        payload_len,
    );
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Put,
        RequestStage::Total,
        RequestStatus::Error,
        key,
        0,
    );
    metrics.emit_op_end_bytes_pulse(OperationKind::Put, RequestStatus::Error, key, 0, now_ms);
}

/// PUT start stage success (no bytes transferred in start stage)
#[inline]
pub fn obe_put_start_success(
    metrics: &Arc<MetricsHandle>,
    client_id: &NodeIDStr,
    _node_role: &NodeRole,
    key: &str,
    _start_ts_us: i64,
    _end_ts_us: i64,
) {
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Put,
        RequestStage::Start,
        RequestStatus::Success,
        key,
        0,
    );
}

/// PUT transfer stage success
#[inline]
pub fn obe_put_transfer_success(
    metrics: &Arc<MetricsHandle>,
    client_id: &NodeIDStr,
    _node_role: &NodeRole,
    key: &str,
    payload_len: u64,
    put_id: PutIDForAKey,
) {
    // Determine start/end timestamps for transfer from pending state
    let t2_us = metrics
        .pending_put_peek(&put_id)
        .map(|p| p.t2_us)
        .unwrap_or_else(|| Utc::now().timestamp_micros());
    let mut end_ts_us = Utc::now().timestamp_micros();
    if end_ts_us <= t2_us {
        end_ts_us = t2_us + 1;
    }
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Put,
        RequestStage::Transfer,
        RequestStatus::Success,
        key,
        payload_len,
    );
    let end_ms = (end_ts_us / 1000) as i64;
    // At transfer end: emit bytes pulse (throughput attribution)
    metrics.emit_op_end_bytes_pulse(
        OperationKind::Put,
        RequestStatus::Success,
        key,
        payload_len,
        end_ms,
    );

    // Mark pending state for end aggregation (top emitted and t3 recorded)
    metrics.pending_put_mark_top_emitted(put_id);
    metrics.pending_put_set_t3(put_id, end_ts_us);
}

/// PUT end/done stage success
#[inline]
pub fn obe_put_done_success(
    metrics: &Arc<MetricsHandle>,
    client_id: &NodeIDStr,
    node_role: &NodeRole,
    key: &str,
    payload_len: u64,
    put_id_str: String,
    rpc_latency_us: i64,
    t1_us: i64,
    t2_us: i64,
    t3_us: i64,
    t4_us: i64,
    start_handle_us: i64,
    end_handle_us: i64,
    transfer_submit_blocking_us: i64,
    transfer_create_xfer_req_us: i64,
    transfer_post_xfer_req_us: i64,
    transfer_poll_wait_us: i64,
    transfer_poll_iters: i64,
    transfer_used_fast_path: bool,
    transfer_local_noop: bool,
    transfer_remote_transfer: bool,
) {
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Put,
        RequestStage::End,
        RequestStatus::Success,
        key,
        payload_len,
    );
    let end_ms = (t4_us / 1000) as i64;
    // At operation end: emit event pulse (bytes=0 to avoid double counting)
    metrics.emit_op_end_bytes_pulse(OperationKind::Put, RequestStatus::Success, key, 0, end_ms);
    if payload_len > 0 {
        metrics.record_client_network_bytes(
            client_id,
            node_role.as_str(),
            TrafficDirection::Rx,
            payload_len,
        );
    }
    metrics.push_put_metric(KvMetrics::Put {
        whole_put: t4_us - t1_us,
        start: t2_us - t1_us,
        transfer: t3_us - t2_us,
        end: t4_us - t3_us,
        rpc_of_put_start: rpc_latency_us,
        start_handle: start_handle_us,
        end_handle: end_handle_us,
        key: key.to_string(),
        put_id: put_id_str,
        start_timestamp_us: t1_us,
        transfer_start_timestamp_us: t2_us,
        end_start_timestamp_us: t3_us,
        end_timestamp_us: t4_us,
        transfer_submit_blocking_us,
        transfer_create_xfer_req_us,
        transfer_post_xfer_req_us,
        transfer_poll_wait_us,
        transfer_poll_iters,
        transfer_used_fast_path,
        transfer_used_nixl: false,
        transfer_local_noop,
        transfer_remote_transfer,
    });
}

/// Put end success using pending timestamps (t1/t2[/t3]) and current t4; also clears pending.
pub fn obe_put_done_success_from_pending(
    metrics: &Arc<MetricsHandle>,
    client_id: &NodeIDStr,
    node_role: &NodeRole,
    key: &str,
    put_id: PutIDForAKey,
    rpc_latency_us: i64,
) {
    let t4_us = Utc::now().timestamp_micros();
    if let Some((_id, stat)) = metrics.pending_put_remove(&put_id) {
        let t1 = stat.t1_us;
        let t2 = stat.t2_us;
        let t3 = stat.t3_us.unwrap_or(t2);
        obe_put_done_success(
            metrics,
            client_id,
            node_role,
            key,
            stat.len,
            format!("{}.{}", put_id.0, put_id.1),
            rpc_latency_us,
            t1,
            t2,
            t3,
            t4_us,
            stat.start_handle_us,
            stat.end_handle_us.unwrap_or(0),
            stat.transfer_submit_blocking_us,
            stat.transfer_create_xfer_req_us,
            stat.transfer_post_xfer_req_us,
            stat.transfer_poll_wait_us,
            stat.transfer_poll_iters,
            stat.transfer_used_fast_path,
            stat.transfer_local_noop,
            stat.transfer_remote_transfer,
        );
    }
}

/// PUT end error helper from pending-put attribution (owner fast path)
#[inline]
pub fn obe_put_end_error_from_pending(
    metrics: &Arc<MetricsHandle>,
    client_id: &NodeIDStr,
    node_role: &NodeRole,
    put_id: PutIDForAKey,
) {
    if let Some((_k, stat)) = metrics.pending_put_remove(&put_id) {
        if !stat.top_emitted {
            obe_put_end_error(metrics, client_id, node_role, &stat.key, stat.len);
        }
    }
}

/// PUT end success helper from pending-put attribution (owner fast path)
#[inline]
pub fn obe_put_end_success_from_pending(
    metrics: &Arc<MetricsHandle>,
    client_id: &NodeIDStr,
    node_role: &NodeRole,
    put_id: PutIDForAKey,
) {
    if let Some((_k, stat)) = metrics.pending_put_remove(&put_id) {
        if !stat.top_emitted {
            let t4_us = Utc::now().timestamp_micros();
            // External fast path: we have client-side t1(us) and t2(us) for put_start RPC; no t3.
            // Use (t1,t2) as start; set transfer duration to 0 by using t3=t2.
            obe_put_done_success(
                metrics,
                client_id,
                node_role,
                &stat.key,
                stat.len,
                format!("{}.{}", put_id.0, put_id.1),
                0,
                stat.t1_us,
                stat.t2_us,
                stat.t2_us,
                t4_us,
                stat.start_handle_us,
                stat.end_handle_us.unwrap_or(0),
                stat.transfer_submit_blocking_us,
                stat.transfer_create_xfer_req_us,
                stat.transfer_post_xfer_req_us,
                stat.transfer_poll_wait_us,
                stat.transfer_poll_iters,
                stat.transfer_used_fast_path,
                stat.transfer_local_noop,
                stat.transfer_remote_transfer,
            );
        }
    }
}

#[inline]
pub fn obe_put_success(
    metrics: &Arc<MetricsHandle>,
    client_id: &NodeIDStr,
    node_role: &NodeRole,
    key: &str,
    payload_len: u64,
    end_ts_ms: i64,
) {
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Put,
        RequestStage::Total,
        RequestStatus::Success,
        key,
        payload_len,
    );
    metrics.emit_op_end_bytes_pulse(
        OperationKind::Put,
        RequestStatus::Success,
        key,
        payload_len,
        end_ts_ms,
    );
    if payload_len > 0 {
        metrics.record_client_network_bytes(
            client_id,
            node_role.as_str(),
            TrafficDirection::Rx,
            payload_len,
        );
    }
}

// ----------------------
// GET wrappers
// ----------------------

#[inline]
pub fn obe_get_cache_hit(
    metrics: &Arc<MetricsHandle>,
    client_id: &NodeIDStr,
    node_role: &NodeRole,
    key: &str,
    bytes: u64,
) {
    let now_ms = (Utc::now().timestamp_micros() / 1000) as i64;
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Get,
        RequestStage::Cache,
        RequestStatus::Hit,
        key,
        bytes,
    );
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Get,
        RequestStage::Total,
        RequestStatus::Hit,
        key,
        bytes,
    );
    metrics.record_cache_hit(client_id, node_role.as_str());
    if bytes > 0 {
        metrics.record_client_network_bytes(
            client_id,
            node_role.as_str(),
            TrafficDirection::Tx,
            bytes,
        );
    }
    metrics.emit_op_end_bytes_pulse(OperationKind::Get, RequestStatus::Hit, key, bytes, now_ms);
}

#[inline]
pub fn obe_get_cache_miss(
    metrics: &Arc<MetricsHandle>,
    client_id: &NodeIDStr,
    node_role: &NodeRole,
    key: &str,
) {
    metrics.record_cache_miss(client_id, node_role.as_str());
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Get,
        RequestStage::Cache,
        RequestStatus::Miss,
        key,
        0,
    );
}

#[inline]
pub fn obe_get_start_error_rpc(
    metrics: &Arc<MetricsHandle>,
    client_id: &NodeIDStr,
    _node_role: &NodeRole,
    key: &str,
) {
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Get,
        RequestStage::Start,
        RequestStatus::Error,
        key,
        0,
    );
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Get,
        RequestStage::Total,
        RequestStatus::Error,
        key,
        0,
    );
}

#[inline]
pub fn obe_get_start_not_found(
    metrics: &Arc<MetricsHandle>,
    client_id: &NodeIDStr,
    _node_role: &NodeRole,
    key: &str,
) {
    let now_ms = (Utc::now().timestamp_micros() / 1000) as i64;
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Get,
        RequestStage::Start,
        RequestStatus::NotFound,
        key,
        0,
    );
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Get,
        RequestStage::Total,
        RequestStatus::NotFound,
        key,
        0,
    );
    metrics.emit_op_end_bytes_pulse(OperationKind::Get, RequestStatus::NotFound, key, 0, now_ms);
}

#[inline]
pub fn obe_get_start_error_status(
    metrics: &Arc<MetricsHandle>,
    client_id: &NodeIDStr,
    _node_role: &NodeRole,
    key: &str,
) {
    let now_ms = (Utc::now().timestamp_micros() / 1000) as i64;
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Get,
        RequestStage::Start,
        RequestStatus::Error,
        key,
        0,
    );
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Get,
        RequestStage::Total,
        RequestStatus::Error,
        key,
        0,
    );
    metrics.emit_op_end_bytes_pulse(OperationKind::Get, RequestStatus::Error, key, 0, now_ms);
}

#[inline]
pub fn obe_get_transfer_error(
    metrics: &Arc<MetricsHandle>,
    client_id: &NodeIDStr,
    _node_role: &NodeRole,
    key: &str,
    bytes: u64,
) {
    let now_ms = (Utc::now().timestamp_micros() / 1000) as i64;
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Get,
        RequestStage::Transfer,
        RequestStatus::Error,
        key,
        bytes,
    );
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Get,
        RequestStage::Total,
        RequestStatus::Error,
        key,
        0,
    );
    metrics.emit_op_end_bytes_pulse(OperationKind::Get, RequestStatus::Error, key, 0, now_ms);
}

#[inline]
pub fn obe_get_end_error_rpc(
    metrics: &Arc<MetricsHandle>,
    client_id: &NodeIDStr,
    _node_role: &NodeRole,
    key: &str,
    bytes: u64,
) {
    let now_ms = (Utc::now().timestamp_micros() / 1000) as i64;
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Get,
        RequestStage::End,
        RequestStatus::Error,
        key,
        bytes,
    );
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Get,
        RequestStage::Total,
        RequestStatus::Error,
        key,
        0,
    );
    metrics.emit_op_end_bytes_pulse(OperationKind::Get, RequestStatus::Error, key, 0, now_ms);
}

#[inline]
pub fn obe_get_done_error_status(
    metrics: &Arc<MetricsHandle>,
    client_id: &NodeIDStr,
    _node_role: &NodeRole,
    key: &str,
    bytes: u64,
) {
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Get,
        RequestStage::End,
        RequestStatus::Error,
        key,
        bytes,
    );
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Get,
        RequestStage::Total,
        RequestStatus::Error,
        key,
        0,
    );
}

/// GET start stage success (no bytes transferred in start stage)
#[inline]
pub fn obe_get_start_success(
    metrics: &Arc<MetricsHandle>,
    client_id: &NodeIDStr,
    _node_role: &NodeRole,
    key: &str,
    _start_ts_us: i64,
    _end_ts_us: i64,
) {
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Get,
        RequestStage::Start,
        RequestStatus::Success,
        key,
        0,
    );
}

/// GET transfer stage success
#[inline]
pub fn obe_get_transfer_success(
    metrics: &Arc<MetricsHandle>,
    client_id: &NodeIDStr,
    _node_role: &NodeRole,
    key: &str,
    bytes: u64,
    _start_ts_us: i64,
    end_ts_us: i64,
) {
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Get,
        RequestStage::Transfer,
        RequestStatus::Success,
        key,
        bytes,
    );
    let end_ms = (end_ts_us / 1000) as i64;
    // At transfer end: emit bytes pulse (throughput attribution)
    metrics.emit_op_end_bytes_pulse(
        OperationKind::Get,
        RequestStatus::Success,
        key,
        bytes,
        end_ms,
    );
}

/// GET end/done stage success
#[inline]
pub fn obe_get_done_success(
    metrics: &Arc<MetricsHandle>,
    client_id: &NodeIDStr,
    node_role: &NodeRole,
    key: &str,
    bytes: u64,
    get_id: u64,
    t1_us: i64,
    t2_us: i64,
    t3_us: i64,
    t4_us: i64,
    start_handle_us: i64,
    end_handle_us: i64,
) {
    // Mark end stage success for GET
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Get,
        RequestStage::End,
        RequestStatus::Success,
        key,
        bytes,
    );
    let end_ms = (t4_us / 1000) as i64;
    // At operation end: emit event pulse (bytes=0 to avoid double counting)
    metrics.emit_op_end_bytes_pulse(OperationKind::Get, RequestStatus::Success, key, 0, end_ms);
    if bytes > 0 {
        metrics.record_client_network_bytes(
            client_id,
            node_role.as_str(),
            TrafficDirection::Tx,
            bytes,
        );
    }
    // Push detailed timeline/duration metrics for GET (only on success)
    metrics.push_get_metric(KvMetrics::Get {
        whole_get: t4_us - t1_us,
        start: t2_us - t1_us,
        transfer: t3_us - t2_us,
        end: t4_us - t3_us,
        start_handle: start_handle_us,
        end_handle: end_handle_us,
        key: key.to_string(),
        get_id: get_id.to_string(),
        start_timestamp_us: t1_us,
        transfer_start_timestamp_us: t2_us,
        end_start_timestamp_us: t3_us,
        end_timestamp_us: t4_us,
    });
}

#[inline]
pub fn obe_get_success(
    metrics: &Arc<MetricsHandle>,
    client_id: &NodeIDStr,
    node_role: &NodeRole,
    key: &str,
    bytes: u64,
    end_ts_ms: i64,
) {
    metrics.observe_request_with_labels(
        client_id,
        OperationKind::Get,
        RequestStage::Total,
        RequestStatus::Success,
        key,
        bytes,
    );
    metrics.emit_op_end_bytes_pulse(
        OperationKind::Get,
        RequestStatus::Success,
        key,
        bytes,
        end_ts_ms,
    );
    if bytes > 0 {
        metrics.record_client_network_bytes(
            client_id,
            node_role.as_str(),
            TrafficDirection::Tx,
            bytes,
        );
    }
}
