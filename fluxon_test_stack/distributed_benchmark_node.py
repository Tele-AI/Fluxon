"""
Distributed benchmark node script.
"""

from __future__ import annotations

import copy
import hashlib
from math import log
import time
import statistics  # Used to compute benchmark statistics
import logging
from unittest import result
import uuid
import socket
import struct
import threading
import random
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple, Callable
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)  # Used to run the benchmark
from enum import Enum, unique

import argparse
import fcntl
import os
import queue
import sys
import json
import urllib.parse
import urllib.request
from pathlib import Path

# Add package root and project root to sys.path.
package_root = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(package_root)
if package_root not in sys.path:
    sys.path.insert(0, package_root)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from .benchmark_node_mq import (
        MQState,
        MQGetStatus,
        MQ_MESSAGE_KIND_PREFEED,
        apply_mq_config_from_test_config,
        build_message,
        get_cluster_info_snapshot,
        init_mq_channel,
        mq_put_to_channel_once,
        mq_put_once,
        mq_get_once,
        MQClosedError,
    )
    from .mpmc_readiness import evaluate_mpmc_topology_ready
    from .benchmark_node_kv import (
        KV_OPERATION_GET,
        KV_OPERATION_PUT,
        KV_NODE_ROLE_SEED,
        KV_NODE_ROLE_WORKER,
        KVGetResultKind,
        FluxonBlockingStore,
        canonicalize_kv_node_role,
        classify_kv_get_result,
        init_kv_store,
        is_kv_seed_role,
        is_kv_worker_role,
        kv_put_once,
        kv_get_once,
        prepare_kv_before_ready,
        run_kv_worker,
    )
    from .benchmark_node_rpc import (
        FLUXON_PHASE_PATH_BUCKET_FAST,
        FLUXON_PHASE_PATH_BUCKET_IPC,
        FLUXON_PHASE_PATH_BUCKET_SLOW,
        FLUXON_PHASE_PATH_METRIC_OWNER1_ROUNDTRIP_US,
        FLUXON_PHASE_PATH_METRIC_RPC_EXT_TOTAL_US,
        _rpc_runtime_config_from_test_config,
        close_rpc_runtime,
        prepare_rpc_before_ready,
        run_rpc_worker,
    )
    from .benchmark_node_fs import close_fs_runtime, prepare_fs_before_ready, run_fs_worker
except ImportError:
    from benchmark_node_mq import (
        MQState,
        MQGetStatus,
        MQ_MESSAGE_KIND_PREFEED,
        apply_mq_config_from_test_config,
        build_message,
        get_cluster_info_snapshot,
        init_mq_channel,
        mq_put_to_channel_once,
        mq_put_once,
        mq_get_once,
        MQClosedError,
    )
    from mpmc_readiness import evaluate_mpmc_topology_ready
    from benchmark_node_kv import (
        KV_OPERATION_GET,
        KV_OPERATION_PUT,
        KV_NODE_ROLE_SEED,
        KV_NODE_ROLE_WORKER,
        KVGetResultKind,
        FluxonBlockingStore,
        canonicalize_kv_node_role,
        classify_kv_get_result,
        init_kv_store,
        is_kv_seed_role,
        is_kv_worker_role,
        kv_put_once,
        kv_get_once,
        prepare_kv_before_ready,
        run_kv_worker,
    )
    from benchmark_node_rpc import (
        FLUXON_PHASE_PATH_BUCKET_FAST,
        FLUXON_PHASE_PATH_BUCKET_IPC,
        FLUXON_PHASE_PATH_BUCKET_SLOW,
        FLUXON_PHASE_PATH_METRIC_OWNER1_ROUNDTRIP_US,
        FLUXON_PHASE_PATH_METRIC_RPC_EXT_TOTAL_US,
        _rpc_runtime_config_from_test_config,
        close_rpc_runtime,
        prepare_rpc_before_ready,
        run_rpc_worker,
    )
    from benchmark_node_fs import close_fs_runtime, prepare_fs_before_ready, run_fs_worker

from fluxon_py.config import FluxonKvClientConfig as KVCacheConfig
from fluxon_py.kvclient.kvclient_interface import KvClient
from fluxon_py._api_ext_chan.mq_config_check import MIN_TTL as CHAN_MIN_TTL_SECONDS


# def get_max_test_time_per_thread(config_path: str) -> int:
#     import importlib.util
#     spec = importlib.util.spec_from_file_location("benchmark_config_module", config_path)
#     if spec is None or spec.loader is None:
#         print(f"❌ Config module not available: {config_path}")
#         exit(1)
#     module = importlib.util.module_from_spec(spec)
#     spec.loader.exec_module(module)
#     cfg = getattr(module, "CONFIG", None)
#     if not isinstance(cfg, dict):
#         print("❌ CONFIG format error (not a dict)")
#         exit(1)
#     benchmark_cfg = cfg.get("benchmark")
#     if not isinstance(benchmark_cfg, dict):
#         print("❌ Missing benchmark section or type error")
#         exit(1)
#     return int(benchmark_cfg.get("max_test_time_per_thread", 300))  # Default: 300 seconds

# KVCACHE_CONFIG_PATH = "./benchmark_config.py"
# MAX_TEST_TIME_PER_THREAD = get_max_test_time_per_thread(KVCACHE_CONFIG_PATH)
# print(f"ℹ️ Loaded MAX_TEST_TIME_PER_THREAD={MAX_TEST_TIME_PER_THREAD} from config")


try:
    from fluxon_py import (
        KvClient,
        KvClientType,
        new_store,
        FluxonKvClientConfig as KVCacheConfig,
        KvFuture,
        MemHolder,
    )
except ImportError as e:
    print("错误: 无法导入 fluxon_py")
    print(f"详细错误: {e}")
    sys.exit(1)

os.environ.setdefault("RUST_LOG", "info")
os.environ.setdefault("FLUXON_LOG", "info")

# Default coordinator address is intentionally not provided.
# Caller must pass --coordinator explicitly to avoid hidden defaults.
COORDINATOR_DEFAULT = None
CHAN_CONFIG = {
    "capacity": 100000,
    "ttl_seconds": CHAN_MIN_TTL_SECONDS,
}

# Metrics warmup: during the first N seconds, execute operations but exclude them from statistics.
METRIC_WARMUP_SECONDS = 60.0
MIN_EFFECTIVE_BENCHMARK_SECONDS = 30.0
START_WAIT_POLL_INTERVAL_SECONDS = 1.0
START_WAIT_POLL_BACKOFF_FACTOR = 1.4
START_WAIT_POLL_MAX_SECONDS = 10.0
START_WAIT_POLL_JITTER_SECONDS = 0.5
DEFAULT_START_WAIT_TIMEOUT_SECONDS = 300.0
REGISTER_RPC_TIMEOUT_SECONDS = 10.0
REGISTER_RPC_RETRY_DEADLINE_SECONDS = 120.0
READY_RPC_TIMEOUT_SECONDS = 10.0
READY_RPC_RETRY_MIN_DEADLINE_SECONDS = 120.0
READY_RPC_RETRY_MAX_DEADLINE_SECONDS = 180.0
COORDINATOR_RPC_RETRY_SLEEP_SECONDS = 5.0
ROUND_GATE_POLL_INTERVAL_SECONDS = 5.0
GREPTIME_OTLP_LOG_TIMEOUT_SECONDS = 10.0
GREPTIME_OTLP_LOG_EXPORT_QUEUE_CAPACITY = 4096
GREPTIME_OTLP_LOG_EXPORT_DRAIN_TIMEOUT_SECONDS = 10.0
GREPTIME_OTLP_LOG_BENCH_MEMBER_KIND = "benchmark_node"
GREPTIME_OTLP_LOG_SERVICE_NAME = "fluxon_benchmark"
GREPTIME_OTLP_BASE_EXTRACT_KEYS = (
    "fluxon_cluster_name",
    "fluxon_member_kind",
    "fluxon_role",
    "fluxon_member_id",
)

# Benchmark heartbeat (diagnostic only)
# - Goal: when the main thread is blocked (e.g. waiting on futures), a background thread
#   periodically prints a liveness line to stdout.
# - If the heartbeat stops printing, it strongly suggests the Python GIL is held by a
#   long-running native call (e.g. a PyO3/Rust path) or a Python-level deadlock.
BENCH_HEARTBEAT_INTERVAL_SECONDS = 5.0
ERROR_DETAILS_MAX_UNIQUE_KEYS = 64
ERROR_DETAILS_OTHER_BUCKET = "OTHER_ERRORS_TRUNCATED"
MPMC_WORKER_EXIT_GRACE_SECONDS = 15.0
MPMC_WORKER_ABORT_GRACE_SECONDS = 5.0
MPMC_NO_MESSAGE_RETRY_SLEEP_SECONDS = 0.01
BENCH_DIRECT_DEBUG_PRINTS_ENABLED = False
KV_WORKER_ABORT_GRACE_SECONDS = 5.0
KV_PAYLOAD_POOL_TARGET_BYTES_PER_SIZE = 128 * 1024 * 1024
KV_PAYLOAD_POOL_MAX_SAMPLES_PER_SIZE = 4
KV_PAYLOAD_POOL_MIN_SAMPLES_PER_SIZE = 1
NETWORK_SAMPLE_INTERVAL_SECONDS = 1.0
TCP_THREAD_TRANSPORT_QUERY_TIMEOUT_SECONDS = 10.0
# No fallback/default: MPMC cluster readiness timeout must be explicitly provided
# by the coordinator via test_config["cluster_ready_timeout_seconds"].
MPMC_READY_INIT_STAGGER_MIN_EXPECTED_NODES = 16
MPMC_READY_INIT_STAGGER_SECONDS_PER_EXTRA_NODE = 0.5
MPMC_READY_INIT_STAGGER_MAX_SECONDS = 60.0
READY_RUNTIME_INIT_RETRY_BASE_SECONDS = 1.0
READY_RUNTIME_INIT_RETRY_MAX_SECONDS = 10.0
RUNTIME_INIT_ETCD_HEALTH_PROBE_TIMEOUT_SECONDS = 1.0
RUNTIME_INIT_ETCD_HEALTH_CACHE_TTL_SECONDS = 2.0
RUNTIME_INIT_ETCD_HEALTH_WAIT_LOG_INTERVAL_SECONDS = 10.0
RUNTIME_INIT_ETCD_HEALTH_MIN_EXPECTED_NODES = 64
RUNTIME_INIT_ETCD_HEALTH_ROOT = Path("/tmp/fluxon_mpmc_runtime_health")
MPMC_PRODUCER_RUNTIME_MODE_ACTIVE = "active"
MPMC_PRODUCER_RUNTIME_MODE_LOGICAL_ONLY = "logical_only"
MPMC_PRODUCER_RUNTIME_MODES = {
    MPMC_PRODUCER_RUNTIME_MODE_ACTIVE,
    MPMC_PRODUCER_RUNTIME_MODE_LOGICAL_ONLY,
}
MPMC_PRODUCER_PREFEED_MESSAGES_PER_CHANNEL = 2
MPMC_PRODUCER_PREFEED_SLOW_PUT_LOG_SECONDS = 5.0

TCP_THREAD_PROM_METRIC_SEND_ENQUEUED = "send_enqueued"
TCP_THREAD_PROM_METRIC_SOCKET_SUBMITTED = "socket_submitted"
TCP_THREAD_PROM_METRIC_BYTES_TOTAL = "tcp_thread_transport_bytes_total"
TCP_THREAD_PROM_METRIC_MESSAGES_TOTAL = "tcp_thread_transport_messages_total"
TCP_THREAD_PROM_METRIC_LATENCY_SAMPLE_COUNT = "tcp_thread_latency_sample_count"
TCP_THREAD_PROM_METRIC_SEND_TOTAL = "send_total"
P2P_RECV_PROM_METRIC_BYTES_TOTAL = "p2p_recv_transport_bytes_total"
P2P_RECV_PROM_METRIC_MESSAGES_TOTAL = "p2p_recv_transport_messages_total"
P2P_RECV_PROM_COMPONENT_RPC_TRANSPORT = "rpc_transport"
P2P_RECV_PROM_COMPONENT_LOCAL_IPC = "local_ipc"
P2P_RECV_PROM_METRIC_RECV_COMPLETED = "recv_completed"
P2P_RECV_PROM_METRIC_DISPATCH_ENQUEUED = "dispatch_enqueued"
P2P_RECV_PROM_METRIC_DISPATCH_DEQUEUED = "dispatch_dequeued"
P2P_RECV_PROM_METRIC_DISPATCH_STARTED = "dispatch_started"
P2P_RECV_PROM_COMPONENTS = (
    P2P_RECV_PROM_COMPONENT_RPC_TRANSPORT,
    P2P_RECV_PROM_COMPONENT_LOCAL_IPC,
)
P2P_RECV_PROM_METRICS = (
    P2P_RECV_PROM_METRIC_RECV_COMPLETED,
    P2P_RECV_PROM_METRIC_DISPATCH_ENQUEUED,
    P2P_RECV_PROM_METRIC_DISPATCH_DEQUEUED,
    P2P_RECV_PROM_METRIC_DISPATCH_STARTED,
)
P2P_RPC_COMPLETION_PROM_METRIC_BYTES_TOTAL = "p2p_rpc_completion_bytes_total"
P2P_RPC_COMPLETION_PROM_METRIC_MESSAGES_TOTAL = "p2p_rpc_completion_messages_total"
P2P_RPC_COMPLETION_PROM_METRIC_RESPONSE_SUBMITTED = "response_submitted"
P2P_RPC_COMPLETION_PROM_METRIC_USER_RPC_REQUEST_FAST_PATH_USED = "user_rpc_request_fast_path_used"
P2P_RPC_COMPLETION_PROM_METRIC_USER_RPC_REQUEST_SLOW_PATH_USED = "user_rpc_request_slow_path_used"
P2P_RPC_COMPLETION_PROM_METRIC_USER_RPC_RESPONSE_FAST_PATH_USED = "user_rpc_response_fast_path_used"
P2P_RPC_COMPLETION_PROM_METRIC_USER_RPC_RESPONSE_SLOW_PATH_USED = "user_rpc_response_slow_path_used"
P2P_RPC_COMPLETION_PROM_ROLE_EXTERNAL_CLIENT = "external_client"
FLUXON_PHASE_OP_RPC = "RPC"
FLUXON_PHASE_PATH_BUCKET_NAMES = (
    FLUXON_PHASE_PATH_BUCKET_FAST,
    FLUXON_PHASE_PATH_BUCKET_SLOW,
    FLUXON_PHASE_PATH_BUCKET_IPC,
)
P2P_RPC_COMPLETION_SUMMARY_SCOPE_SINGLE_SIDE_ROUNDTRIP = "single_side_roundtrip"
P2P_RPC_COMPLETION_SUMMARY_SCOPE_OWNER_OWNER = "owner_owner"
P2P_RPC_COMPLETION_SUMMARY_DEBUG_ROLE = "raw_transport_counters"


@dataclass(frozen=True)
class _GreptimeOtlpLogConfig:
    otlp_endpoint: str
    db_name: str
    table_name: Optional[str]
    cluster_name: str
    member_kind: str
    role: str
    member_id: str


def _empty_roundtrip_bucket(window_seconds: float) -> Dict[str, Any]:
    return {
        "count": 0,
        "avg_us": 0.0,
        "max_us": 0.0,
        "ops_per_sec": 0.0,
    }


def _phase_metric_bucket_stats(
    op_summary: Dict[str, Any],
    metric_name: str,
    path_bucket: str,
    window_seconds: float,
) -> Dict[str, Any]:
    empty = _empty_roundtrip_bucket(window_seconds)
    path_metric_counts_raw = op_summary.get("path_metric_counts", {})
    if not isinstance(path_metric_counts_raw, dict):
        return empty
    metric_counts_raw = path_metric_counts_raw.get(metric_name, {})
    if not isinstance(metric_counts_raw, dict):
        return empty
    count = int(metric_counts_raw.get(path_bucket, 0))
    if count <= 0:
        return empty
    path_metric_avg_raw = op_summary.get("path_metric_avg_us", {})
    metric_avg_raw = {}
    if isinstance(path_metric_avg_raw, dict):
        candidate = path_metric_avg_raw.get(metric_name, {})
        if isinstance(candidate, dict):
            metric_avg_raw = candidate
    path_metric_max_raw = op_summary.get("path_metric_max_us", {})
    metric_max_raw = {}
    if isinstance(path_metric_max_raw, dict):
        candidate = path_metric_max_raw.get(metric_name, {})
        if isinstance(candidate, dict):
            metric_max_raw = candidate
    return {
        "count": count,
        "avg_us": float(metric_avg_raw.get(path_bucket, 0.0)),
        "max_us": float(metric_max_raw.get(path_bucket, 0.0)),
        "ops_per_sec": (float(count) / float(window_seconds)) if window_seconds > 0.0 else 0.0,
    }


def _phase_metric_bucket_map(
    op_summary: Dict[str, Any],
    metric_name: str,
    window_seconds: float,
) -> Dict[str, Dict[str, Any]]:
    return {
        path_bucket: _phase_metric_bucket_stats(
            op_summary=op_summary,
            metric_name=metric_name,
            path_bucket=path_bucket,
            window_seconds=window_seconds,
        )
        for path_bucket in FLUXON_PHASE_PATH_BUCKET_NAMES
    }


def _build_p2p_rpc_completion_summary_from_phase_summary(
    fluxon_phase_summary: Dict[str, Any],
    duration_seconds: float,
    debug_owner_owner_transport_counters: Dict[str, Any],
) -> Dict[str, Any]:
    if not isinstance(fluxon_phase_summary, dict):
        fluxon_phase_summary = {}
    if not isinstance(debug_owner_owner_transport_counters, dict):
        debug_owner_owner_transport_counters = {}
    op_summary = fluxon_phase_summary.get(FLUXON_PHASE_OP_RPC, {})
    if not isinstance(op_summary, dict):
        op_summary = {}
    window_seconds = max(0.0, float(duration_seconds))
    owner_owner = _phase_metric_bucket_map(
        op_summary=op_summary,
        metric_name=FLUXON_PHASE_PATH_METRIC_OWNER1_ROUNDTRIP_US,
        window_seconds=window_seconds,
    )
    external_total_path_buckets = _phase_metric_bucket_map(
        op_summary=op_summary,
        metric_name=FLUXON_PHASE_PATH_METRIC_RPC_EXT_TOTAL_US,
        window_seconds=window_seconds,
    )
    extra_avg_raw = op_summary.get("extra_avg_us", {})
    if not isinstance(extra_avg_raw, dict):
        extra_avg_raw = {}
    external_total_max_us = 0.0
    for bucket_stats in external_total_path_buckets.values():
        external_total_max_us = max(
            external_total_max_us,
            float(bucket_stats.get("max_us", 0.0)),
        )
    external_total_count = int(op_summary.get("count", 0))
    external_total = {
        "metric_name": FLUXON_PHASE_PATH_METRIC_RPC_EXT_TOTAL_US,
        "count": external_total_count,
        "avg_us": float(extra_avg_raw.get(FLUXON_PHASE_PATH_METRIC_RPC_EXT_TOTAL_US, 0.0)),
        "max_us": external_total_max_us,
        "ops_per_sec": (
            float(external_total_count) / float(window_seconds)
            if window_seconds > 0.0
            else 0.0
        ),
    }
    if (
        not op_summary
        and not debug_owner_owner_transport_counters
        and external_total_count <= 0
    ):
        return {}
    return {
        "scope": P2P_RPC_COMPLETION_SUMMARY_SCOPE_SINGLE_SIDE_ROUNDTRIP,
        "op_name": FLUXON_PHASE_OP_RPC,
        "measurement_window_seconds": window_seconds,
        "owner_owner": {
            "metric_name": FLUXON_PHASE_PATH_METRIC_OWNER1_ROUNDTRIP_US,
            FLUXON_PHASE_PATH_BUCKET_FAST: owner_owner[FLUXON_PHASE_PATH_BUCKET_FAST],
            FLUXON_PHASE_PATH_BUCKET_SLOW: owner_owner[FLUXON_PHASE_PATH_BUCKET_SLOW],
            FLUXON_PHASE_PATH_BUCKET_IPC: owner_owner[FLUXON_PHASE_PATH_BUCKET_IPC],
        },
        "external_total": external_total,
        "debug": {
            "semantic_role": P2P_RPC_COMPLETION_SUMMARY_DEBUG_ROLE,
            "external_total_path_buckets": external_total_path_buckets,
            "owner_owner_transport_counters": copy.deepcopy(
                debug_owner_owner_transport_counters
            ),
        },
    }


def _otlp_varint(value: int) -> bytes:
    if value < 0:
        raise ValueError(f"OTLP varint value must be >= 0, got: {value}")
    out = bytearray()
    current = int(value)
    while True:
        to_write = current & 0x7F
        current >>= 7
        if current:
            out.append(to_write | 0x80)
        else:
            out.append(to_write)
            return bytes(out)


def _otlp_tag(field_number: int, wire_type: int) -> bytes:
    return _otlp_varint((int(field_number) << 3) | int(wire_type))


def _otlp_len_field(field_number: int, payload: bytes) -> bytes:
    return _otlp_tag(field_number, 2) + _otlp_varint(len(payload)) + payload


def _otlp_string_field(field_number: int, value: str) -> bytes:
    encoded = value.encode("utf-8")
    return _otlp_len_field(field_number, encoded)


def _otlp_fixed64_field(field_number: int, value: int) -> bytes:
    return _otlp_tag(field_number, 1) + struct.pack("<Q", int(value))


def _otlp_fixed32_field(field_number: int, value: int) -> bytes:
    return _otlp_tag(field_number, 5) + struct.pack("<I", int(value))


def _otlp_varint_field(field_number: int, value: int) -> bytes:
    return _otlp_tag(field_number, 0) + _otlp_varint(int(value))


def _otlp_double_field(field_number: int, value: float) -> bytes:
    return _otlp_tag(field_number, 1) + struct.pack("<d", float(value))


def _otlp_any_value(value: Any) -> bytes:
    if isinstance(value, bool):
        return _otlp_varint_field(2, 1 if value else 0)
    if isinstance(value, int):
        return _otlp_varint_field(3, value)
    if isinstance(value, float):
        return _otlp_double_field(4, value)
    if isinstance(value, str):
        return _otlp_string_field(1, value)
    raise TypeError(f"unsupported OTLP AnyValue type: {type(value)}")


def _otlp_key_value(key: str, value: Any) -> bytes:
    payload = bytearray()
    payload.extend(_otlp_string_field(1, key))
    payload.extend(_otlp_len_field(2, _otlp_any_value(value)))
    return bytes(payload)


def _otlp_resource(resource_attrs: Dict[str, Any]) -> bytes:
    payload = bytearray()
    for key, value in resource_attrs.items():
        payload.extend(_otlp_len_field(1, _otlp_key_value(key, value)))
    return bytes(payload)


def _otlp_scope(scope_name: str) -> bytes:
    payload = bytearray()
    payload.extend(_otlp_string_field(1, scope_name))
    return bytes(payload)


def _otlp_log_record(
    *,
    time_unix_nano: int,
    severity_number: int,
    severity_text: str,
    body: str,
    attrs: Dict[str, Any],
) -> bytes:
    payload = bytearray()
    payload.extend(_otlp_fixed64_field(1, time_unix_nano))
    payload.extend(_otlp_varint_field(2, severity_number))
    payload.extend(_otlp_string_field(3, severity_text))
    payload.extend(_otlp_len_field(5, _otlp_any_value(body)))
    for key, value in attrs.items():
        payload.extend(_otlp_len_field(6, _otlp_key_value(key, value)))
    return bytes(payload)


def _otlp_scope_logs(scope_name: str, log_record_payloads: List[bytes]) -> bytes:
    payload = bytearray()
    payload.extend(_otlp_len_field(1, _otlp_scope(scope_name)))
    for log_record_payload in log_record_payloads:
        payload.extend(_otlp_len_field(2, log_record_payload))
    return bytes(payload)


def _otlp_resource_logs(resource_attrs: Dict[str, Any], log_record_payloads: List[bytes]) -> bytes:
    payload = bytearray()
    payload.extend(_otlp_len_field(1, _otlp_resource(resource_attrs)))
    payload.extend(_otlp_len_field(2, _otlp_scope_logs(GREPTIME_OTLP_LOG_SERVICE_NAME, log_record_payloads)))
    return bytes(payload)


def _otlp_export_logs_service_request(
    *,
    resource_attrs: Dict[str, Any],
    log_record_payloads: List[bytes],
) -> bytes:
    payload = bytearray()
    payload.extend(_otlp_len_field(1, _otlp_resource_logs(resource_attrs, log_record_payloads)))
    return bytes(payload)


def _phase_summary_segment_field_prefix(segment_name: str) -> str:
    if segment_name.endswith("_us"):
        return segment_name[:-3]
    return segment_name


def _flatten_fluxon_phase_summary(summary: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
    summary_kind = str(summary.get("summary_kind", "")).strip()
    op_name = str(summary.get("op_name", "")).strip()
    if not summary_kind or not op_name:
        raise ValueError(f"invalid fluxon phase summary payload: {summary}")
    bucket_counts = summary.get("bucket_counts")
    if not isinstance(bucket_counts, dict):
        raise ValueError(f"fluxon phase summary missing bucket_counts: {summary}")
    segment_stats = summary.get("segment_stats")
    path_metric_stats = summary.get("path_metric_stats")
    has_segment_stats = isinstance(segment_stats, dict) and bool(segment_stats)
    has_path_metric_stats = isinstance(path_metric_stats, dict) and bool(path_metric_stats)
    if not has_segment_stats and not has_path_metric_stats:
        raise ValueError(f"fluxon phase summary missing segment_stats/path_metric_stats: {summary}")

    attrs: Dict[str, Any] = {
        "phase_summary_kind": summary_kind,
        "phase_summary_op": op_name,
        "phase_summary_window_count": int(summary.get("window_count", 0)),
        "phase_summary_total_count": int(summary.get("total_count", 0)),
        "phase_summary_deadline_overrun_count": int(summary.get("deadline_overrun_count", 0)),
        "phase_bucket_ok_count": int(bucket_counts.get("ok", 0)),
        "phase_bucket_miss_count": int(bucket_counts.get("miss", 0)),
        "phase_bucket_timeout_count": int(bucket_counts.get("timeout", 0)),
        "phase_bucket_error_count": int(bucket_counts.get("error", 0)),
    }

    body_parts = [
        "INFO",
        "fluxon_benchmark.phase_summary",
        f"kind={summary_kind}",
        f"op={op_name}",
        f"window_count={attrs['phase_summary_window_count']}",
        f"total_count={attrs['phase_summary_total_count']}",
        f"ok={attrs['phase_bucket_ok_count']}",
        f"timeout={attrs['phase_bucket_timeout_count']}",
        f"error={attrs['phase_bucket_error_count']}",
    ]

    if isinstance(segment_stats, dict):
        for segment_name, raw_segment_stats in sorted(segment_stats.items()):
            if not isinstance(raw_segment_stats, dict):
                continue
            segment_prefix = _phase_summary_segment_field_prefix(str(segment_name))
            segment_count = int(raw_segment_stats.get("count", 0))
            attrs[f"phase_{segment_prefix}_count"] = segment_count
            rendered_stats: Dict[str, float] = {}
            for stat_name in ("avg_us", "p50_us", "p95_us", "p99_us", "max_us"):
                stat_value = float(raw_segment_stats.get(stat_name, 0.0))
                rendered_stats[stat_name] = stat_value
                # Greptime extract keys reject FLOAT values for log tags, so phase
                # summary attrs are exported as rounded integer microseconds.
                attrs[f"phase_{segment_prefix}_{stat_name}"] = int(round(stat_value))
            body_parts.append(
                f"{segment_prefix}_p99_us={rendered_stats['p99_us']:.1f}"
            )
            body_parts.append(
                f"{segment_prefix}_avg_us={rendered_stats['avg_us']:.1f}"
            )
    if isinstance(path_metric_stats, dict):
        for metric_name, bucket_stats in sorted(path_metric_stats.items()):
            if not isinstance(bucket_stats, dict):
                continue
            metric_prefix = _phase_summary_segment_field_prefix(str(metric_name))
            for path_bucket, raw_bucket_stats in sorted(bucket_stats.items()):
                if not isinstance(raw_bucket_stats, dict):
                    continue
                bucket_prefix = f"{metric_prefix}_{str(path_bucket)}"
                bucket_count = int(raw_bucket_stats.get("count", 0))
                attrs[f"phase_path_{bucket_prefix}_count"] = bucket_count
                rendered_stats: Dict[str, float] = {}
                for stat_name in ("avg_us", "p50_us", "p95_us", "p99_us", "max_us"):
                    stat_value = float(raw_bucket_stats.get(stat_name, 0.0))
                    rendered_stats[stat_name] = stat_value
                    attrs[f"phase_path_{bucket_prefix}_{stat_name}"] = int(round(stat_value))
                body_parts.append(
                    f"{bucket_prefix}_p99_us={rendered_stats['p99_us']:.1f}"
                )
                body_parts.append(
                    f"{bucket_prefix}_avg_us={rendered_stats['avg_us']:.1f}"
                )
    return " ".join(body_parts), attrs


class _GreptimeOtlpLogExporter:
    def __init__(self, cfg: _GreptimeOtlpLogConfig) -> None:
        self._cfg = cfg
        self._queue: "queue.Queue[Dict[str, Any]]" = queue.Queue(
            maxsize=GREPTIME_OTLP_LOG_EXPORT_QUEUE_CAPACITY
        )
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name=f"greptime-otlp-exporter-{cfg.member_id}",
            daemon=True,
        )
        self._dropped = 0
        self._last_drop_report_ts = 0.0
        self._thread.start()

    def emit_phase_summary(self, summary: Dict[str, Any]) -> None:
        if self._stop.is_set():
            return
        try:
            self._queue.put_nowait(copy.deepcopy(summary))
        except queue.Full:
            self._dropped += 1
            now = time.time()
            if now - self._last_drop_report_ts >= 10.0:
                logger.warning(
                    "⚠️ Greptime OTLP phase summary queue full; dropped=%s member_id=%s",
                    self._dropped,
                    self._cfg.member_id,
                )
                self._last_drop_report_ts = now

    def wait_idle(self, timeout_s: float) -> bool:
        deadline_ts = time.time() + float(timeout_s)
        while time.time() < deadline_ts:
            if self._queue.unfinished_tasks == 0:
                return True
            time.sleep(0.1)
        return self._queue.unfinished_tasks == 0

    def close(self) -> None:
        self._stop.set()
        self._thread.join()

    def _run(self) -> None:
        while True:
            if self._stop.is_set() and self._queue.unfinished_tasks == 0:
                return
            try:
                summary = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                self._post_summary(summary)
            except Exception as exc:
                logger.warning(
                    "⚠️ Greptime OTLP phase summary export failed: member_id=%s err=%s",
                    self._cfg.member_id,
                    exc,
                )
            finally:
                self._queue.task_done()

    def _post_summary(self, summary: Dict[str, Any]) -> None:
        body, phase_attrs = _flatten_fluxon_phase_summary(summary)
        log_attrs: Dict[str, Any] = {
            "fluxon_cluster_name": self._cfg.cluster_name,
            "fluxon_member_kind": self._cfg.member_kind,
            "fluxon_role": self._cfg.role,
            "fluxon_member_id": self._cfg.member_id,
        }
        log_attrs.update(phase_attrs)
        extract_keys = list(GREPTIME_OTLP_BASE_EXTRACT_KEYS)
        extract_keys.extend(key for key in phase_attrs.keys() if key not in GREPTIME_OTLP_BASE_EXTRACT_KEYS)
        payload = _otlp_export_logs_service_request(
            resource_attrs={"service.name": GREPTIME_OTLP_LOG_SERVICE_NAME},
            log_record_payloads=[
                _otlp_log_record(
                    time_unix_nano=time.time_ns(),
                    severity_number=9,
                    severity_text="INFO",
                    body=body,
                    attrs=log_attrs,
                )
            ],
        )
        headers = {
            "Content-Type": "application/x-protobuf",
            "X-Greptime-DB-Name": self._cfg.db_name,
            "X-Greptime-Log-Extract-Keys": ",".join(extract_keys),
        }
        if self._cfg.table_name is not None:
            headers["X-Greptime-Log-Table-Name"] = self._cfg.table_name
        req = urllib.request.Request(
            self._cfg.otlp_endpoint,
            data=payload,
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=GREPTIME_OTLP_LOG_TIMEOUT_SECONDS) as resp:
            status = getattr(resp, "status", 200)
            if int(status) < 200 or int(status) >= 300:
                body_text = resp.read().decode("utf-8", errors="replace")
                raise RuntimeError(f"greptime otlp http {status}: {body_text}")


def _empty_p2p_receive_transport_components() -> Dict[str, Any]:
    components: Dict[str, Any] = {}
    for component in P2P_RECV_PROM_COMPONENTS:
        component_metrics: Dict[str, Any] = {}
        for metric in P2P_RECV_PROM_METRICS:
            component_metrics[metric] = {
                "bytes_total_delta": 0,
                "messages_total_delta": 0,
                "bytes_per_sec": 0.0,
                "messages_per_sec": 0.0,
            }
        components[component] = component_metrics
    return components


def _compact_error_detail_label(error_msg: str) -> str:
    msg = error_msg.strip().replace("\n", " ")
    if ", details:" in msg:
        msg = msg.split(", details:", 1)[0]
    if ", key='" in msg:
        msg = msg.split(", key='", 1)[0]
    if len(msg) > 240:
        msg = msg[:240]
    return msg


def _prometheus_query_range(
    *,
    base_url: str,
    promql: str,
    start_s: float,
    end_s: float,
    step: str,
) -> List[Dict[str, Any]]:
    query_url = base_url.rstrip("/") + "/api/v1/query_range"
    query = urllib.parse.urlencode(
        {
            "query": promql,
            "start": f"{max(0.0, float(start_s)):.3f}",
            "end": f"{max(0.0, float(end_s)):.3f}",
            "step": step,
        }
    )
    req = urllib.request.Request(
        query_url + "?" + query,
        headers={"User-Agent": "fluxon-benchmark-node/1.0"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=TCP_THREAD_TRANSPORT_QUERY_TIMEOUT_SECONDS) as resp:
        body = resp.read().decode("utf-8")
    payload = json.loads(body)
    if payload.get("status") != "success":
        raise RuntimeError(f"prometheus query_range failed: {body}")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise RuntimeError(f"prometheus query_range missing data: {body}")
    result = data.get("result")
    if not isinstance(result, list):
        raise RuntimeError(f"prometheus query_range missing result list: {body}")
    return result


def _sum_prometheus_range_delta(series_list: List[Dict[str, Any]]) -> float:
    return sum(_prometheus_series_delta(series) for series in series_list)


def _prometheus_series_delta(series: Dict[str, Any]) -> float:
    values = series.get("values")
    if not isinstance(values, list) or len(values) < 2:
        return 0.0
    total_delta = 0.0
    prev_v: Optional[float] = None
    for point in values:
        if not isinstance(point, list) or len(point) != 2:
            continue
        try:
            current_v = float(point[1])
        except (TypeError, ValueError):
            continue
        if prev_v is None:
            prev_v = current_v
            continue
        if current_v >= prev_v:
            total_delta += current_v - prev_v
        else:
            total_delta += current_v
        prev_v = current_v
    return total_delta


def _prometheus_series_labels(series: Dict[str, Any]) -> Dict[str, str]:
    metric = series.get("metric")
    if not isinstance(metric, dict):
        return {}
    return {
        str(raw_key): str(raw_value)
        for raw_key, raw_value in metric.items()
        if raw_key is not None and raw_value is not None
    }


def _prometheus_node_label_matches_target(node_label: str, target: str) -> bool:
    node_s = node_label.strip()
    target_s = target.strip()
    if not node_s or not target_s:
        return False
    if node_s == target_s:
        return True
    return node_s.rsplit("_", 1)[-1] == target_s


def _discover_active_explicit_node_roles(
    *,
    base_url: str,
    promql: str,
    start_s: float,
    end_s: float,
    allowed_nodes: List[str],
    delta_key: str,
) -> Tuple[int, List[Dict[str, Any]], List[Tuple[str, str]]]:
    allowed_node_set = {str(node).strip() for node in allowed_nodes if str(node).strip()}
    if not allowed_node_set:
        return 0, [], []
    discovery_series = _prometheus_query_range(
        base_url=base_url,
        promql=promql,
        start_s=start_s,
        end_s=end_s,
        step="5s",
    )
    matched_label_pairs: Dict[Tuple[str, str], float] = {}
    matched_series_count = 0
    for series in discovery_series:
        labels = _prometheus_series_labels(series)
        node_label = labels.get("node", "")
        role_label = labels.get("role", "")
        if not node_label or not role_label:
            continue
        if node_label not in allowed_node_set:
            continue
        delta = _prometheus_series_delta(series)
        if delta <= 0.0:
            continue
        matched_series_count += 1
        key = (node_label, role_label)
        matched_label_pairs[key] = matched_label_pairs.get(key, 0.0) + delta
    matched_label_pair_list = [
        {
            "node": node_label,
            "role": role_label,
            delta_key: float(delta),
        }
        for (node_label, role_label), delta in sorted(matched_label_pairs.items())
    ]
    return matched_series_count, matched_label_pair_list, sorted(matched_label_pairs.keys())


def _normalize_kv_node_role_in_test_config(test_config: Any) -> None:
    if not isinstance(test_config, dict):
        return
    test_mode = str(test_config.get("test_mode", TestMode.KVSTORE.value))
    if test_mode == TestMode.MPMC.value:
        return
    test_config["node_role"] = canonicalize_kv_node_role(test_config.get("node_role", ""))


class TestMode(Enum):
    """Test mode enum."""

    MPMC = "MPMC"
    KVSTORE = "KVSTORE"
    KVSTORE_WITH_LOCAL_CACHE = "KVSTORE_WITH_LOCAL_CACHE"
    RPC = "RPC"


class ValueSizeMode(Enum):
    """Value size selection mode."""

    FIXED = "FIXED"
    RANDOM_WEIGHTED_SET = "RANDOM_WEIGHTED_SET"


class MsgType(Enum):
    REGISTER = "register"
    READY = "ready"
    START = "start"
    RUNTIME_READY = "runtime_ready"
    RUNTIME_START = "runtime_start"
    RESULT = "result"
    ROUND_STATUS = "round_status"


# Colored logging
class ColoredFormatter(logging.Formatter):
    """Add colors for different log levels."""

    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[41m",  # Red background
        "RESET": "\033[0m",  # Reset
    }

    def format(self, record):
        log_color = self.COLORS.get(record.levelname, self.COLORS["RESET"])
        record.levelname = f"{log_color}[NODE-{record.levelname}]{self.COLORS['RESET']}"
        return super().format(record)


# Colored logging
handler = logging.StreamHandler()
handler.setFormatter(
    ColoredFormatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logging.basicConfig(
    level=logging.DEBUG, handlers=[handler], datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("benchmark_node")


def _debug_print(msg: str) -> None:
    """Lightweight debug print with flush for easier tracing.

    Uses stdout directly so messages appear even if logging buffers.
    """
    if not BENCH_DIRECT_DEBUG_PRINTS_ENABLED:
        return
    print(f"[DEBUG-BENCH] {msg}", flush=True)


@unique
class OperationOutcome(Enum):
    SUCCESS = "success"
    ERROR = "error"
    CACHE_HIT = "cache_hit"
    CACHE_MISS = "cache_miss"


@dataclass
class OperationResult:
    """Single operation result."""

    success: bool
    latency_us: float
    operation_type: str  # kvstore:put or get  mpmc : put_data or get_data
    key: str
    data_size: int
    inflight_at_start: int
    outcome_kind: OperationOutcome
    error_msg: Optional[str] = None
    # Node and worker that produced this result (useful for analyzing tail latency)
    node_id: Optional[str] = None
    worker_id: Optional[int] = None
    # Operation completion time (wall clock), used for precise warmup filtering
    finish_ts: float = 0.0


@dataclass
class PreparedWorkerRuntime:
    """MPMC worker endpoint prepared before the benchmark window starts."""

    producer: Any = None
    consumer: Any = None
    local_mq_state: Optional[MQState] = None
    logical_only: bool = False


@dataclass
class PreparedMPMCRound:
    """One prepared MPMC round that is waiting for the coordinator start signal."""

    pending_threads: Dict[int, threading.Thread] = field(default_factory=dict)
    worker_results: Dict[int, List[OperationResult]] = field(default_factory=dict)
    prepared_runtimes: Dict[int, PreparedWorkerRuntime] = field(default_factory=dict)
    ready_thread_ids: set[int] = field(default_factory=set)
    prepare_errors: Dict[int, str] = field(default_factory=dict)
    fatal_errors: Dict[int, BaseException] = field(default_factory=dict)
    closed_endpoint_ids: set[int] = field(default_factory=set)
    worker_results_lock: threading.Lock = field(default_factory=threading.Lock)
    prepared_lock: threading.Lock = field(default_factory=threading.Lock)
    endpoint_close_lock: threading.Lock = field(default_factory=threading.Lock)
    start_event: threading.Event = field(default_factory=threading.Event)
    benchmark_deadline_ts: Optional[float] = None


@dataclass
class NetworkBandwidthSample:
    ts_s: float
    rx_mbps: float
    tx_mbps: float


class NetworkBandwidthSampler:
    """Collect machine-level bandwidth from /proc/net/dev."""

    def __init__(self, *, target: str, interval_seconds: float):
        self.target = target
        self.interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name=f"net-sampler-{target}",
            daemon=True,
        )
        self._lock = threading.Lock()
        self._samples: List[NetworkBandwidthSample] = []
        self._interface_names: List[str] = []
        self._total_rx_bytes_delta = 0
        self._total_tx_bytes_delta = 0
        self._error = ""
        self._previous_ts = 0.0
        self._previous_counters: Dict[str, Tuple[int, int]] = {}

    @staticmethod
    def _read_snapshot() -> Tuple[float, Dict[str, Tuple[int, int]]]:
        counters: Dict[str, Tuple[int, int]] = {}
        with open("/proc/net/dev", "r", encoding="utf-8") as handle:
            for raw_line in handle:
                if ":" not in raw_line:
                    continue
                iface_part, stat_part = raw_line.split(":", 1)
                iface = iface_part.strip()
                if iface == "lo":
                    continue
                fields = stat_part.split()
                if len(fields) < 16:
                    continue
                counters[iface] = (int(fields[0]), int(fields[8]))
        if not counters:
            raise RuntimeError("no non-loopback interfaces found in /proc/net/dev")
        return time.time(), counters

    def _record_delta(
        self,
        *,
        previous_ts: float,
        previous_counters: Dict[str, Tuple[int, int]],
        current_ts: float,
        current_counters: Dict[str, Tuple[int, int]],
    ) -> None:
        elapsed_seconds = current_ts - previous_ts
        if elapsed_seconds <= 0.0:
            return

        total_rx_bytes_delta = 0
        total_tx_bytes_delta = 0
        for iface in sorted(current_counters.keys()):
            if iface not in previous_counters:
                continue
            prev_rx_bytes, prev_tx_bytes = previous_counters[iface]
            curr_rx_bytes, curr_tx_bytes = current_counters[iface]
            total_rx_bytes_delta += max(0, curr_rx_bytes - prev_rx_bytes)
            total_tx_bytes_delta += max(0, curr_tx_bytes - prev_tx_bytes)

        sample = NetworkBandwidthSample(
            ts_s=current_ts,
            rx_mbps=(float(total_rx_bytes_delta) * 8.0) / elapsed_seconds / 1_000_000.0,
            tx_mbps=(float(total_tx_bytes_delta) * 8.0) / elapsed_seconds / 1_000_000.0,
        )
        with self._lock:
            self._samples.append(sample)
            self._interface_names = sorted(current_counters.keys())
            self._total_rx_bytes_delta += total_rx_bytes_delta
            self._total_tx_bytes_delta += total_tx_bytes_delta

    def start(self) -> None:
        self._previous_ts, self._previous_counters = self._read_snapshot()
        self._interface_names = sorted(self._previous_counters.keys())
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join()

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            samples = [
                {
                    "ts_s": sample.ts_s,
                    "rx_mbps": sample.rx_mbps,
                    "tx_mbps": sample.tx_mbps,
                }
                for sample in self._samples
            ]
            avg_rx_mbps = statistics.mean(sample.rx_mbps for sample in self._samples) if self._samples else 0.0
            avg_tx_mbps = statistics.mean(sample.tx_mbps for sample in self._samples) if self._samples else 0.0
            peak_rx_mbps = max((sample.rx_mbps for sample in self._samples), default=0.0)
            peak_tx_mbps = max((sample.tx_mbps for sample in self._samples), default=0.0)
            return {
                "leader": True,
                "target": self.target,
                "sample_interval_seconds": self.interval_seconds,
                "sample_count": len(self._samples),
                "interface_names": list(self._interface_names),
                "avg_rx_mbps": avg_rx_mbps,
                "avg_tx_mbps": avg_tx_mbps,
                "peak_rx_mbps": peak_rx_mbps,
                "peak_tx_mbps": peak_tx_mbps,
                "total_rx_bytes_delta": self._total_rx_bytes_delta,
                "total_tx_bytes_delta": self._total_tx_bytes_delta,
                "samples": samples,
                "error": self._error,
            }

    def _run(self) -> None:
        previous_ts = self._previous_ts
        previous_counters = dict(self._previous_counters)
        while not self._stop.wait(self.interval_seconds):
            try:
                current_ts, current_counters = self._read_snapshot()
            except Exception as exc:
                self._error = str(exc)
                logger.warning("⚠️ 网络采样读取失败: target=%s err=%s", self.target, exc)
                continue
            self._record_delta(
                previous_ts=previous_ts,
                previous_counters=previous_counters,
                current_ts=current_ts,
                current_counters=current_counters,
            )
            previous_ts = current_ts
            previous_counters = current_counters

        try:
            current_ts, current_counters = self._read_snapshot()
        except Exception as exc:
            self._error = str(exc)
            logger.warning("⚠️ 网络采样最终读取失败: target=%s err=%s", self.target, exc)
            return
        self._record_delta(
            previous_ts=previous_ts,
            previous_counters=previous_counters,
            current_ts=current_ts,
            current_counters=current_counters,
        )


class BenchmarkWorkerStop(RuntimeError):
    """Worker exits because the benchmark window is closed and useful work is exhausted."""


class BenchmarkNode:
    def __init__(self):
        self.test_config: Optional[Dict[str, Any]] = None
        self.node_id: str = f"node_{uuid.uuid4().hex[:8]}"
        self.kv_store: Optional[Any] = None
        self.fluxon_client: Optional[KvClient] = None
        self.channel_id: Optional[str] = None  # Channel ID
        # Coordinator address must be provided by CLI; keep unset until main() assigns.
        self.coordinator_host: str = ""
        self.coordinator_port: int = 0
        self.operation_results: List[OperationResult] = []
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.key_prefix: Optional[str] = None
        self.instance_key: Optional[str] = None
        # MQ/channel state is encapsulated in MQState.
        self.mq_state = MQState()
        self.chan_config: Dict[str, Any] = CHAN_CONFIG.copy()
        self.mq_unique_id: str = ""
        # Optional: simulate MQ consumer handling time (ms range)
        # Shape: (min_ms, max_ms), assigned by coordinator.
        self.consumer_sim_handle_ms_range = None
        self.value_size_mode: str = ValueSizeMode.FIXED.value
        self.value_size_weighted_set: List[Tuple[int, float]] = []
        self._payload_pool_by_size: Dict[int, Tuple[bytes, ...]] = {}
        self._payload_pool_by_size: Dict[int, Tuple[bytes, ...]] = {}

        # Reuse self.end_time as the metrics window end:
        # - KV mode: set to deadline_ts
        # - MPMC mode: set to the time when the main thread broadcasts stop intent

        self._inflight_lock = threading.Lock()
        self._inflight_requests = 0

        # Progress snapshot updated by worker threads; read by heartbeat thread.
        self._progress_lock = threading.Lock()
        self._last_op_finish_ts: Optional[float] = None
        self._thread_last_op_idx: Dict[int, int] = {}
        self._thread_last_latency_us: Dict[int, float] = {}

        # Heartbeat thread (diagnostic only). Initialize eagerly to avoid None checks in close paths.
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"bench-heartbeat-{self.node_id}",
            daemon=True,
        )
        self._network_bandwidth_sampler: Optional[NetworkBandwidthSampler] = None
        self._network_bandwidth_summary: Dict[str, Any] = {}
        self._benchmark_stop = threading.Event()
        self._forced_benchmark_result: Optional[Dict[str, Any]] = None
        self._prepared_mpmc_round: Optional[PreparedMPMCRound] = None
        self._runtime_init_stagger_once_lock = threading.Lock()
        self._runtime_init_stagger_once_done = False
        self._mpmc_producer_kv_store_lock = threading.Lock()
        self._mpmc_endpoint_prepare_lock = threading.Lock()
        self._kv_store_close_lock = threading.Lock()
        self._kv_store_closed = False
        self._fluxon_phase_log_exporter: Optional[_GreptimeOtlpLogExporter] = None
        # 多轮 benchmark 控制：由协调者在 START 响应中告知是否还有后续轮次。
        self.has_more_tests: bool = False

        logger.info(f"🔧 初始化基准测试节点: {self.node_id}")

    @staticmethod
    def _stable_fraction_from_text(text: str) -> float:
        digest = hashlib.sha256(text.encode("utf-8")).digest()
        value = int.from_bytes(digest[:8], "big")
        return value / float((1 << 64) - 1)

    def _runtime_init_stagger_seconds(self) -> float:
        """Spread large MPMC node initialization so etcd is not hit all at once."""
        if not isinstance(self.test_config, dict):
            return 0.0
        if str(self.test_config.get("test_mode", "")).strip() != TestMode.MPMC.value:
            return 0.0
        expected_nodes_raw = self.test_config.get("expected_nodes")
        try:
            expected_nodes = int(expected_nodes_raw)
        except (TypeError, ValueError):
            return 0.0
        if expected_nodes <= MPMC_READY_INIT_STAGGER_MIN_EXPECTED_NODES:
            return 0.0

        max_stagger_s = min(
            MPMC_READY_INIT_STAGGER_MAX_SECONDS,
            (
                expected_nodes - MPMC_READY_INIT_STAGGER_MIN_EXPECTED_NODES
            )
            * MPMC_READY_INIT_STAGGER_SECONDS_PER_EXTRA_NODE,
        )
        key = self.instance_key or self.node_id
        return self._stable_fraction_from_text(str(key)) * max_stagger_s

    def _sleep_for_runtime_init_stagger(
        self,
        *,
        max_sleep_seconds: Optional[float] = None,
    ) -> None:
        stagger_s = self._runtime_init_stagger_seconds()
        if max_sleep_seconds is not None:
            stagger_s = min(stagger_s, max(0.0, float(max_sleep_seconds)))
        if stagger_s <= 0.0:
            return
        logger.info(
            "⏳ MPMC runtime init stagger: sleep_seconds=%.2f expected_nodes=%s instance_key=%s",
            stagger_s,
            self.test_config.get("expected_nodes") if isinstance(self.test_config, dict) else None,
            self.instance_key,
        )
        time.sleep(stagger_s)

    def _sleep_for_runtime_init_stagger_once(
        self,
        *,
        max_sleep_seconds: Optional[float] = None,
    ) -> None:
        """Apply the MPMC fan-out stagger once per benchmark node process."""
        with self._runtime_init_stagger_once_lock:
            if self._runtime_init_stagger_once_done:
                return
            self._runtime_init_stagger_once_done = True
            self._sleep_for_runtime_init_stagger(
                max_sleep_seconds=max_sleep_seconds
            )

    def _mpmc_expected_nodes(self) -> int:
        if not isinstance(self.test_config, dict):
            return 0
        expected_nodes_raw = self.test_config.get("expected_nodes")
        try:
            return int(expected_nodes_raw)
        except (TypeError, ValueError):
            return 0

    def _mpmc_producer_runtime_mode(self) -> str:
        if not isinstance(self.test_config, dict):
            return MPMC_PRODUCER_RUNTIME_MODE_ACTIVE
        raw_mode = self.test_config.get(
            "mpmc_producer_runtime_mode",
            MPMC_PRODUCER_RUNTIME_MODE_ACTIVE,
        )
        if raw_mode is None:
            mode = MPMC_PRODUCER_RUNTIME_MODE_ACTIVE
        else:
            mode = str(raw_mode).strip()
        if mode not in MPMC_PRODUCER_RUNTIME_MODES:
            raise ValueError(
                "mpmc_producer_runtime_mode must be one of "
                f"{sorted(MPMC_PRODUCER_RUNTIME_MODES)}, got: {raw_mode!r}"
            )
        return mode

    def _is_mpmc_producer_prefeed_leader(self, *, thread_id: int) -> bool:
        if not isinstance(self.test_config, dict):
            raise RuntimeError("MPMC producer prefeed leader requires test_config")
        leader = self.test_config.get("mpmc_producer_prefeed_leader")
        if not isinstance(leader, bool):
            raise RuntimeError(
                "mpmc_producer_prefeed_leader must be an explicit bool for every producer"
            )
        return leader and thread_id == 0

    def _expected_mpmc_consumer_workers(self) -> int:
        if not isinstance(self.test_config, dict):
            raise RuntimeError("expected MPMC consumer workers require test_config")
        raw_count = self.test_config.get("expected_mpmc_consumer_workers")
        if not isinstance(raw_count, int) or isinstance(raw_count, bool) or raw_count <= 0:
            raise RuntimeError(
                "expected_mpmc_consumer_workers must be an explicit positive integer, "
                f"got: {raw_count!r}"
            )
        return raw_count

    def _runtime_init_etcd_health_wait_enabled(self) -> bool:
        if not isinstance(self.test_config, dict):
            return False
        if str(self.test_config.get("test_mode", "")).strip() != TestMode.MPMC.value:
            return False
        return self._mpmc_expected_nodes() > RUNTIME_INIT_ETCD_HEALTH_MIN_EXPECTED_NODES

    def _runtime_init_etcd_health_scope_dir(self, kvcache_config: Dict[str, Any]) -> Path:
        fluxonkv_spec = kvcache_config.get("fluxonkv_spec")
        cluster_name = ""
        if isinstance(fluxonkv_spec, dict):
            raw_cluster_name = fluxonkv_spec.get("cluster_name")
            if isinstance(raw_cluster_name, str):
                cluster_name = raw_cluster_name.strip()
        scope_key_parts = {
            "cluster_name": cluster_name,
            "coordinator": f"{self.coordinator_host}:{self.coordinator_port}",
            "mq_unique_id": str(self.mq_unique_id or "").strip(),
            "test_id": str(self.test_config.get("test_id", "") if isinstance(self.test_config, dict) else "").strip(),
        }
        scope_key = json.dumps(scope_key_parts, sort_keys=True, ensure_ascii=True)
        scope_hash = hashlib.sha256(scope_key.encode("utf-8")).hexdigest()[:24]
        return RUNTIME_INIT_ETCD_HEALTH_ROOT / scope_hash

    @staticmethod
    def _is_retryable_runtime_init_error(error_msg: str) -> bool:
        """Classify transient Fluxon runtime init failures seen during fan-out startup."""
        lowered = str(error_msg).lower()
        fatal_markers = (
            "failed to bind to configured p2p_listen_port",
            "p2p tcp bind failed",
        )
        if any(marker in lowered for marker in fatal_markers):
            return False

        retryable_markers = (
            "backendinitfailederror",
            "failed to connect to etcd",
            "connect etcd failed",
            "etcd connection failed",
            "failed to acquire etcd lock",
            "deadline has elapsed",
            "status probe timed out",
            "timed out",
            "connection refused",
            "p2p timeout",
            "payload lease keepalive",
            "lease not found",
            "failed to create mpsc channel",
            "failed to get next available channel",
            "chancreateerror",
            "chanbinderror",
        )
        return any(marker in lowered for marker in retryable_markers)

    def _runtime_init_retry_deadline_seconds(self) -> float:
        if not isinstance(self.test_config, dict):
            return 0.0
        raw_timeout = self.test_config.get("cluster_ready_timeout_seconds")
        try:
            timeout_s = float(raw_timeout)
        except (TypeError, ValueError):
            return 0.0
        return max(0.0, timeout_s)

    def _runtime_init_retry_sleep_seconds(self, *, attempt: int) -> float:
        base_s = min(
            READY_RUNTIME_INIT_RETRY_MAX_SECONDS,
            READY_RUNTIME_INIT_RETRY_BASE_SECONDS * (2 ** max(0, attempt - 1)),
        )
        key = f"{self.instance_key or self.node_id}:{attempt}"
        jitter_s = self._stable_fraction_from_text(key) * 0.5
        return base_s + jitter_s

    @staticmethod
    def _runtime_init_shared_json_candidates(kvcache_config: Dict[str, Any]) -> List[Path]:
        fluxonkv_spec = kvcache_config.get("fluxonkv_spec")
        if not isinstance(fluxonkv_spec, dict):
            return []

        raw_share_mem_path = fluxonkv_spec.get("share_mem_path")
        raw_cluster_name = fluxonkv_spec.get("cluster_name")
        if not isinstance(raw_share_mem_path, str) or not raw_share_mem_path.strip():
            return []
        if not isinstance(raw_cluster_name, str) or not raw_cluster_name.strip():
            return []

        share_mem_path = Path(raw_share_mem_path.strip())
        cluster_name = raw_cluster_name.strip()
        candidates = [
            share_mem_path / cluster_name / "shared.json",
            share_mem_path / "shared.json",
        ]
        out: List[Path] = []
        seen = set()
        for candidate in candidates:
            key = str(candidate)
            if key in seen:
                continue
            seen.add(key)
            out.append(candidate)
        return out

    @staticmethod
    def _runtime_init_raw_etcd_endpoints(kvcache_config: Dict[str, Any]) -> Tuple[List[Any], str, bool]:
        fluxonkv_spec = kvcache_config.get("fluxonkv_spec")
        if not isinstance(fluxonkv_spec, dict):
            return [], "no fluxonkv_spec configured", False

        raw_endpoints = fluxonkv_spec.get("etcd_addresses")
        if raw_endpoints is None:
            raw_endpoints = fluxonkv_spec.get("etcd_addresses_raw")
        if isinstance(raw_endpoints, str):
            return [raw_endpoints], "fluxonkv_spec.etcd_addresses", True
        if isinstance(raw_endpoints, list) and raw_endpoints:
            return list(raw_endpoints), "fluxonkv_spec.etcd_addresses", True

        shared_json_candidates = BenchmarkNode._runtime_init_shared_json_candidates(kvcache_config)
        if not shared_json_candidates:
            return [], "no etcd health endpoint configured", False

        missing_paths: List[str] = []
        for shared_json_path in shared_json_candidates:
            try:
                payload = json.loads(shared_json_path.read_text(encoding="utf-8"))
            except FileNotFoundError:
                missing_paths.append(str(shared_json_path))
                continue
            except Exception as exc:  # noqa: BLE001
                return (
                    [],
                    f"owner shared.json is not readable: path={shared_json_path} err={type(exc).__name__}: {exc}",
                    True,
                )
            if not isinstance(payload, dict):
                return [], f"owner shared.json is not a JSON object: path={shared_json_path}", True
            shared_endpoints = payload.get("etcd_addresses")
            if isinstance(shared_endpoints, str):
                return [shared_endpoints], f"owner shared.json: {shared_json_path}", True
            if isinstance(shared_endpoints, list) and shared_endpoints:
                return list(shared_endpoints), f"owner shared.json: {shared_json_path}", True
            return [], f"owner shared.json missing non-empty etcd_addresses: path={shared_json_path}", True

        return [], f"owner shared.json not ready: candidates={missing_paths}", True

    @staticmethod
    def _runtime_init_etcd_health_urls_from_endpoints(endpoints: List[Any]) -> List[str]:
        urls: List[str] = []
        seen = set()
        for endpoint in endpoints:
            endpoint_text = str(endpoint).strip()
            if not endpoint_text:
                continue
            if "://" not in endpoint_text:
                endpoint_text = f"http://{endpoint_text}"
            parsed = urllib.parse.urlparse(endpoint_text)
            if not parsed.scheme or not parsed.netloc:
                continue
            health_url = urllib.parse.urlunparse(
                (parsed.scheme, parsed.netloc, "/health", "", "", "")
            )
            if health_url in seen:
                continue
            seen.add(health_url)
            urls.append(health_url)
        return urls

    @staticmethod
    def _runtime_init_etcd_health_urls_with_source(kvcache_config: Dict[str, Any]) -> Tuple[List[str], str, bool]:
        endpoints, source, should_wait = BenchmarkNode._runtime_init_raw_etcd_endpoints(kvcache_config)
        return (
            BenchmarkNode._runtime_init_etcd_health_urls_from_endpoints(endpoints),
            source,
            should_wait,
        )

    @staticmethod
    def _runtime_init_etcd_health_urls(kvcache_config: Dict[str, Any]) -> List[str]:
        urls, _source, _should_wait = BenchmarkNode._runtime_init_etcd_health_urls_with_source(kvcache_config)
        return urls

    @staticmethod
    def _runtime_init_etcd_health_probe_expected(kvcache_config: Dict[str, Any]) -> bool:
        endpoints, _source, should_wait = BenchmarkNode._runtime_init_raw_etcd_endpoints(kvcache_config)
        return bool(endpoints) or should_wait

    def _runtime_init_etcd_health_cache_paths(
        self,
        kvcache_config: Dict[str, Any],
    ) -> Tuple[Path, Path]:
        urls, source, should_wait = self._runtime_init_etcd_health_urls_with_source(kvcache_config)
        cache_key = json.dumps(
            {
                "source": source,
                "should_wait": should_wait,
                "urls": urls,
            },
            sort_keys=True,
            ensure_ascii=True,
        )
        cache_hash = hashlib.sha256(cache_key.encode("utf-8")).hexdigest()[:24]
        health_scope_dir = self._runtime_init_etcd_health_scope_dir(kvcache_config)
        return (
            health_scope_dir / f"etcd_health_{cache_hash}.json",
            health_scope_dir / f"etcd_health_{cache_hash}.lock",
        )

    @staticmethod
    def _read_runtime_init_etcd_health_cache(cache_path: Path) -> Optional[Tuple[bool, str]]:
        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        try:
            written_ts = float(payload.get("time", 0.0))
        except (TypeError, ValueError):
            return None
        if time.time() - written_ts > RUNTIME_INIT_ETCD_HEALTH_CACHE_TTL_SECONDS:
            return None
        healthy = bool(payload.get("healthy"))
        detail = str(payload.get("detail", "cached etcd health result"))
        return healthy, detail

    @staticmethod
    def _write_runtime_init_etcd_health_cache(
        cache_path: Path,
        *,
        healthy: bool,
        detail: str,
    ) -> None:
        payload = {
            "time": time.time(),
            "healthy": bool(healthy),
            "detail": str(detail),
            "pid": os.getpid(),
        }
        tmp_path = cache_path.with_name(f"{cache_path.name}.{os.getpid()}.tmp")
        try:
            tmp_path.write_text(
                json.dumps(payload, sort_keys=True, ensure_ascii=True) + "\n",
                encoding="utf-8",
            )
            os.replace(tmp_path, cache_path)
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "runtime init etcd health cache write failed: path=%s err=%s",
                cache_path,
                exc,
            )
            try:
                tmp_path.unlink()
            except OSError:
                pass

    def _probe_runtime_init_etcd_health(
        self,
        kvcache_config: Dict[str, Any],
    ) -> Tuple[bool, str]:
        urls, source, should_wait = self._runtime_init_etcd_health_urls_with_source(kvcache_config)
        if not urls:
            return (False, source) if should_wait else (True, source)

        errors: List[str] = []
        for health_url in urls:
            req = urllib.request.Request(
                health_url,
                headers={"User-Agent": "fluxon-benchmark-node/1.0"},
                method="GET",
            )
            try:
                with urllib.request.urlopen(
                    req,
                    timeout=RUNTIME_INIT_ETCD_HEALTH_PROBE_TIMEOUT_SECONDS,
                ) as resp:
                    status = int(getattr(resp, "status", 200))
                    body_text = resp.read().decode("utf-8", errors="replace")
                if status != 200:
                    errors.append(f"{health_url}: http {status}")
                    continue
                payload = json.loads(body_text) if body_text.strip() else {}
                raw_health = payload.get("health")
                if raw_health is True or str(raw_health).lower() == "true":
                    return True, f"{source} {health_url}"
                errors.append(f"{health_url}: unhealthy body={body_text[:160]!r}")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{health_url}: {type(exc).__name__}: {exc}")
        return False, "; ".join(errors[:3])

    def _probe_runtime_init_etcd_health_shared(
        self,
        kvcache_config: Dict[str, Any],
    ) -> Tuple[bool, str]:
        """Coalesce same-host etcd health probes during large MPMC startup."""
        try:
            cache_path, lock_path = self._runtime_init_etcd_health_cache_paths(kvcache_config)
            cache_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as exc:  # noqa: BLE001
            logger.debug("runtime init etcd health cache unavailable: err=%s", exc)
            return self._probe_runtime_init_etcd_health(kvcache_config)

        cached = self._read_runtime_init_etcd_health_cache(cache_path)
        if cached is not None:
            healthy, detail = cached
            return healthy, f"{detail} cache={cache_path}"

        try:
            lock_handle = lock_path.open("a+", encoding="utf-8")
        except OSError as exc:
            logger.debug("runtime init etcd health lock open failed: path=%s err=%s", lock_path, exc)
            return self._probe_runtime_init_etcd_health(kvcache_config)

        with lock_handle:
            try:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                cached = self._read_runtime_init_etcd_health_cache(cache_path)
                if cached is not None:
                    healthy, detail = cached
                    return healthy, f"{detail} cache={cache_path}"
                return False, f"another process is probing etcd health: cache={cache_path}"
            except OSError as exc:
                logger.debug(
                    "runtime init etcd health lock acquire failed: path=%s err=%s",
                    lock_path,
                    exc,
                )
                return self._probe_runtime_init_etcd_health(kvcache_config)

            try:
                cached = self._read_runtime_init_etcd_health_cache(cache_path)
                if cached is not None:
                    healthy, detail = cached
                    return healthy, f"{detail} cache={cache_path}"

                healthy, detail = self._probe_runtime_init_etcd_health(kvcache_config)
                self._write_runtime_init_etcd_health_cache(
                    cache_path,
                    healthy=healthy,
                    detail=detail,
                )
                return healthy, detail
            finally:
                try:
                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
                except OSError:
                    pass

    def _wait_for_runtime_init_etcd_health(
        self,
        kvcache_config: Dict[str, Any],
        *,
        deadline_ts: float,
        ctx: str,
    ) -> Optional[str]:
        """Delay large MPMC runtime initialization while etcd is unhealthy."""
        if not self._runtime_init_etcd_health_wait_enabled():
            return None
        if not self._runtime_init_etcd_health_probe_expected(kvcache_config):
            return None

        attempt = 0
        next_wait_log_ts = 0.0
        while True:
            healthy, detail = self._probe_runtime_init_etcd_health_shared(kvcache_config)
            if healthy:
                if attempt > 0:
                    logger.info(
                        "✅ etcd health recovered before MPMC runtime init: ctx=%s detail=%s",
                        ctx,
                        detail,
                    )
                return None

            remaining_s = deadline_ts - time.monotonic()
            if remaining_s <= 0.0:
                return (
                    "etcd health wait timed out before MPMC runtime init: "
                    f"ctx={ctx} detail={detail}"
                )

            attempt += 1
            sleep_s = min(
                self._runtime_init_retry_sleep_seconds(attempt=attempt),
                max(0.0, remaining_s),
            )
            now = time.monotonic()
            if now >= next_wait_log_ts:
                logger.warning(
                    "⏳ etcd health not ready; delaying MPMC runtime init: "
                    "ctx=%s sleep=%.2fs remaining=%.2fs detail=%s",
                    ctx,
                    sleep_s,
                    remaining_s,
                    detail,
                )
                next_wait_log_ts = now + RUNTIME_INIT_ETCD_HEALTH_WAIT_LOG_INTERVAL_SECONDS
            time.sleep(sleep_s)

    def _init_kv_store_with_ready_retry(
        self,
        kvcache_config: Dict[str, Any],
        *,
        deadline_ts: Optional[float] = None,
    ) -> Tuple[Optional[Any], Optional[str]]:
        """Initialize the KV client with bounded retry for transient fan-out failures."""
        start_ts = time.monotonic()
        if deadline_ts is None:
            deadline_s = self._runtime_init_retry_deadline_seconds()
            deadline_ts = start_ts + deadline_s if deadline_s > 0.0 else start_ts
        else:
            deadline_s = max(0.0, float(deadline_ts) - start_ts)
        attempt = 0
        last_err: Optional[str] = None

        while True:
            attempt += 1
            remaining_before_attempt_s = deadline_ts - time.monotonic()
            if deadline_s > 0.0 and remaining_before_attempt_s <= 0.0:
                return (
                    None,
                    f"runtime init retry deadline elapsed before attempt {attempt} "
                    f"(within {deadline_s:.1f}s)",
                )

            health_err = self._wait_for_runtime_init_etcd_health(
                kvcache_config,
                deadline_ts=deadline_ts,
                ctx="kv_store_init",
            )
            if health_err is not None:
                return None, health_err
            remaining_before_attempt_s = deadline_ts - time.monotonic()
            if deadline_s > 0.0 and remaining_before_attempt_s <= 0.0:
                return (
                    None,
                    f"runtime init retry deadline elapsed before attempt {attempt} "
                    f"(within {deadline_s:.1f}s)",
                )
            store, err = init_kv_store(kvcache_config)
            if err is None:
                if attempt > 1:
                    logger.info("✅ KVCache 存储实例重试创建成功: attempts=%s", attempt)
                return store, None

            last_err = str(err)
            if not self._is_retryable_runtime_init_error(last_err):
                return None, last_err

            remaining_s = deadline_ts - time.monotonic()
            if remaining_s <= 0.0:
                return (
                    None,
                    f"{last_err} (after {attempt} attempts within {deadline_s:.1f}s)",
                )

            sleep_s = min(
                remaining_s,
                self._runtime_init_retry_sleep_seconds(attempt=attempt),
            )
            logger.warning(
                "⚠️ KVCache 存储实例创建遇到瞬时错误，将重试: attempt=%s sleep_seconds=%.2f remaining_seconds=%.1f err=%s",
                attempt,
                sleep_s,
                remaining_s,
                last_err,
            )
            time.sleep(sleep_s)

    @staticmethod
    def _worker_owned_kvcache_config(
        kvcache_config: Dict[str, Any],
        *,
        thread_id: int,
    ) -> Dict[str, Any]:
        """Return a per-worker KV config with a unique member id and P2P port."""
        out = copy.deepcopy(kvcache_config)
        raw_instance_key = out.get("instance_key")
        if not isinstance(raw_instance_key, str) or not raw_instance_key.strip():
            raise ValueError("kvcache_config.instance_key must be a non-empty string")
        out["instance_key"] = f"{raw_instance_key.strip()}__worker_{int(thread_id)}"
        fluxonkv_spec = out.get("fluxonkv_spec")
        if fluxonkv_spec is None:
            return out
        if not isinstance(fluxonkv_spec, dict):
            raise ValueError("kvcache_config.fluxonkv_spec must be a mapping")
        raw_port = fluxonkv_spec.get("p2p_listen_port")
        if raw_port is None:
            return out
        base_port = int(raw_port)
        worker_port = base_port + int(thread_id)
        if worker_port <= 0 or worker_port > 65535:
            raise ValueError(
                "computed worker-owned p2p_listen_port out of range: "
                f"base={base_port} thread_id={thread_id} port={worker_port}"
            )
        fluxonkv_spec["p2p_listen_port"] = worker_port
        return out

    def _mark_progress(self, *, thread_id: int, op_idx: int, finish_ts: float, latency_us: float) -> None:
        with self._progress_lock:
            self._last_op_finish_ts = finish_ts
            self._thread_last_op_idx[thread_id] = op_idx
            self._thread_last_latency_us[thread_id] = latency_us

    @staticmethod
    def _payload_pool_sample_count_for_size(size_bytes: int) -> int:
        if size_bytes <= 0:
            raise ValueError(f"payload size must be > 0, got: {size_bytes}")
        target_count = KV_PAYLOAD_POOL_TARGET_BYTES_PER_SIZE // int(size_bytes)
        bounded_count = max(KV_PAYLOAD_POOL_MIN_SAMPLES_PER_SIZE, int(target_count))
        return min(KV_PAYLOAD_POOL_MAX_SAMPLES_PER_SIZE, bounded_count)

    def _refresh_payload_pools(self) -> None:
        self._payload_pool_by_size = {}

        if not isinstance(self.test_config, dict):
            return

        sizes_to_prepare: List[int] = []
        if self.value_size_mode == ValueSizeMode.FIXED.value:
            fixed_size = int(self.test_config.get("value_size", 0))
            if fixed_size > 0:
                sizes_to_prepare.append(fixed_size)
        elif self.value_size_mode == ValueSizeMode.RANDOM_WEIGHTED_SET.value:
            sizes_to_prepare.extend(size_bytes for size_bytes, _ in self.value_size_weighted_set)

        unique_sizes = sorted({int(size_bytes) for size_bytes in sizes_to_prepare if int(size_bytes) > 0})
        if not unique_sizes:
            return

        prepared_total_bytes = 0
        prepared_parts: List[str] = []
        for size_bytes in unique_sizes:
            sample_count = self._payload_pool_sample_count_for_size(size_bytes)
            payload_pool = tuple(os.urandom(size_bytes) for _ in range(sample_count))
            self._payload_pool_by_size[size_bytes] = payload_pool
            prepared_total_bytes += size_bytes * sample_count
            prepared_parts.append(f"{size_bytes}B x {sample_count}")

        logger.info(
            "🔧 预生成 payload 池: mode=%s sizes=[%s] total_pool_mib=%.1f",
            self.value_size_mode,
            ", ".join(prepared_parts),
            prepared_total_bytes / 1024.0 / 1024.0,
        )

    def _network_sample_config(self) -> Optional[Dict[str, Any]]:
        if not isinstance(self.test_config, dict):
            return None
        cfg = self.test_config.get("network_sample")
        if not isinstance(cfg, dict):
            return None
        return cfg

    def _fluxon_phase_export_expected(self) -> bool:
        if not isinstance(self.test_config, dict):
            return False
        if str(self.test_config.get("test_mode", "")).strip() != TestMode.RPC.value:
            return False
        backend_kind = str(self.test_config.get("rpc_backend_kind", "")).strip().upper()
        return backend_kind == "FLUXON"

    def _resolve_fluxon_phase_cluster_name(self) -> Optional[str]:
        if not isinstance(self.test_config, dict):
            return None
        kvcache_config = self.test_config.get("kvcache_config")
        if not isinstance(kvcache_config, dict):
            return None
        fluxonkv_spec = kvcache_config.get("fluxonkv_spec")
        if not isinstance(fluxonkv_spec, dict):
            return None
        cluster_name = fluxonkv_spec.get("cluster_name")
        if not isinstance(cluster_name, str) or not cluster_name.strip():
            return None
        return cluster_name.strip()

    def _ensure_fluxon_phase_log_exporter(self) -> Optional[_GreptimeOtlpLogExporter]:
        if self._fluxon_phase_log_exporter is not None:
            return self._fluxon_phase_log_exporter
        if not self._fluxon_phase_export_expected():
            return None
        if not isinstance(self.test_config, dict):
            return None
        raw_otlp_cfg = self.test_config.get("otlp_log_api")
        if raw_otlp_cfg is None:
            logger.warning("⚠️ RPC FLUXON benchmark missing otlp_log_api; phase summary export disabled")
            return None
        if not isinstance(raw_otlp_cfg, dict):
            logger.warning("⚠️ RPC FLUXON benchmark otlp_log_api must be dict; phase summary export disabled")
            return None
        otlp_endpoint = raw_otlp_cfg.get("otlp_endpoint")
        db_name = raw_otlp_cfg.get("db_name")
        table_name = raw_otlp_cfg.get("table_name")
        cluster_name = self._resolve_fluxon_phase_cluster_name()
        role = str(self.test_config.get("node_role", "")).strip()
        member_id = str(self.instance_key or self.node_id).strip()
        if not isinstance(otlp_endpoint, str) or not otlp_endpoint.strip():
            logger.warning("⚠️ RPC FLUXON benchmark otlp_log_api.otlp_endpoint invalid; phase summary export disabled")
            return None
        if not isinstance(db_name, str) or not db_name.strip():
            logger.warning("⚠️ RPC FLUXON benchmark otlp_log_api.db_name invalid; phase summary export disabled")
            return None
        if cluster_name is None:
            logger.warning("⚠️ RPC FLUXON benchmark cluster_name missing; phase summary export disabled")
            return None
        if not role:
            logger.warning("⚠️ RPC FLUXON benchmark node_role missing; phase summary export disabled")
            return None
        normalized_table_name: Optional[str] = None
        if table_name is not None:
            if not isinstance(table_name, str) or not table_name.strip():
                logger.warning("⚠️ RPC FLUXON benchmark otlp_log_api.table_name invalid; phase summary export disabled")
                return None
            normalized_table_name = table_name.strip()
        self._fluxon_phase_log_exporter = _GreptimeOtlpLogExporter(
            _GreptimeOtlpLogConfig(
                otlp_endpoint=otlp_endpoint.strip(),
                db_name=db_name.strip(),
                table_name=normalized_table_name,
                cluster_name=cluster_name,
                member_kind=GREPTIME_OTLP_LOG_BENCH_MEMBER_KIND,
                role=role,
                member_id=member_id,
            )
        )
        logger.info(
            "🔧 Enabled Greptime OTLP phase summary export: endpoint=%s db=%s table=%s member_id=%s",
            otlp_endpoint.strip(),
            db_name.strip(),
            normalized_table_name or "<default>",
            member_id,
        )
        return self._fluxon_phase_log_exporter

    def _attach_fluxon_phase_summary_callback(self, store: Any) -> None:
        if store is None or not hasattr(store, "set_phase_summary_callback"):
            return
        exporter = self._ensure_fluxon_phase_log_exporter()
        if exporter is None:
            return
        store.set_phase_summary_callback(exporter.emit_phase_summary)

    def _flush_fluxon_phase_summary(self) -> None:
        phase_store = getattr(self, "_fluxon_rpc_store", None)
        if phase_store is None:
            phase_store = self.kv_store
        if phase_store is None or not hasattr(phase_store, "flush_phase_summary"):
            return
        try:
            phase_store.flush_phase_summary()
        except Exception as exc:
            logger.warning("⚠️ flush fluxon phase summary failed: %s", exc)

    def _wait_fluxon_phase_log_exporter_idle(self, timeout_s: float) -> None:
        exporter = self._fluxon_phase_log_exporter
        if exporter is None:
            return
        if not exporter.wait_idle(timeout_s):
            logger.warning(
                "⚠️ Greptime OTLP phase summary exporter still busy after %.1fs",
                timeout_s,
            )

    def _close_fluxon_phase_log_exporter(self) -> None:
        exporter = self._fluxon_phase_log_exporter
        if exporter is None:
            return
        exporter.close()
        self._fluxon_phase_log_exporter = None

    def _start_network_bandwidth_sampler(self) -> None:
        self._network_bandwidth_summary = {}
        cfg = self._network_sample_config()
        if cfg is None:
            return
        target = cfg.get("target")
        leader = bool(cfg.get("leader"))
        if not isinstance(target, str) or not target.strip():
            self._network_bandwidth_summary = {
                "leader": leader,
                "target": "",
                "error": "network_sample.target must be a non-empty string",
            }
            logger.error("❌ network_sample.target 缺失或为空")
            return
        if not leader:
            self._network_bandwidth_summary = {
                "leader": False,
                "target": target,
            }
            return

        sampler = NetworkBandwidthSampler(
            target=target,
            interval_seconds=NETWORK_SAMPLE_INTERVAL_SECONDS,
        )
        try:
            sampler.start()
        except Exception as exc:
            self._network_bandwidth_summary = {
                "leader": True,
                "target": target,
                "sample_interval_seconds": NETWORK_SAMPLE_INTERVAL_SECONDS,
                "error": str(exc),
            }
            logger.warning("⚠️ 启动网络采样失败: target=%s err=%s", target, exc)
            return
        self._network_bandwidth_sampler = sampler

    def _stop_network_bandwidth_sampler(self) -> None:
        if self._network_bandwidth_sampler is None:
            return
        self._network_bandwidth_sampler.stop()
        self._network_bandwidth_summary = self._network_bandwidth_sampler.snapshot()
        self._network_bandwidth_sampler = None

    def _network_bandwidth_payload(self) -> Dict[str, Any]:
        if not self._network_bandwidth_summary:
            return {}
        return copy.deepcopy(self._network_bandwidth_summary)

    def _tcp_thread_transport_summary(self) -> Dict[str, Any]:
        if not isinstance(self.test_config, dict):
            return {}
        prom_base_raw = self.test_config.get("prometheus_base_url")
        if not isinstance(prom_base_raw, str) or not prom_base_raw.strip():
            return {}
        network_sample_cfg = self._network_sample_config()
        if network_sample_cfg is None:
            return {}
        target_raw = network_sample_cfg.get("target")
        if not isinstance(target_raw, str) or not target_raw.strip():
            return {"error": "network_sample.target must be a non-empty string"}
        target = target_raw.strip()
        leader = bool(network_sample_cfg.get("leader"))
        if not leader:
            return {
                "target": target,
                "leader": False,
            }
        if self.start_time is None or self.end_time is None:
            return {}

        warmup_deadline_ts = self.start_time + METRIC_WARMUP_SECONDS
        cutoff_ts = self.end_time
        window_seconds = max(0.0, float(cutoff_ts - warmup_deadline_ts))
        if window_seconds <= 0.0:
            return {}

        prom_base = prom_base_raw.strip().rstrip("/")
        start_s = warmup_deadline_ts
        # Metrics flush to Prom every 30s. Extend the query tail slightly so the final flush
        # after benchmark stop is still captured in the counter delta.
        end_s = cutoff_ts + 35.0

        try:
            latency_series = _prometheus_query_range(
                base_url=prom_base,
                promql=(
                    f'{TCP_THREAD_PROM_METRIC_LATENCY_SAMPLE_COUNT}'
                    f'{{metric="{TCP_THREAD_PROM_METRIC_SEND_TOTAL}"}}'
                ),
                start_s=start_s,
                end_s=end_s,
                step="5s",
            )
        except Exception as exc:
            logger.warning(f"⚠️ 收集 tcp_thread_transport_summary 失败: {exc}")
            return {"error": str(exc)}

        matched_label_pairs: Dict[Tuple[str, str], float] = {}
        matched_series_count = 0
        for series in latency_series:
            labels = _prometheus_series_labels(series)
            node_label = labels.get("node", "")
            role_label = labels.get("role", "")
            if not node_label or not role_label:
                continue
            if not _prometheus_node_label_matches_target(node_label, target):
                continue
            delta = _prometheus_series_delta(series)
            if delta <= 0.0:
                continue
            matched_series_count += 1
            key = (node_label, role_label)
            matched_label_pairs[key] = matched_label_pairs.get(key, 0.0) + delta

        if not matched_label_pairs:
            return {
                "target": target,
                "leader": True,
                "window_seconds": window_seconds,
                "matched_latency_series_count": 0,
                "matched_label_pairs": [],
                "send_enqueued_bytes_total_delta": 0,
                "send_enqueued_messages_total_delta": 0,
                "socket_submitted_bytes_total_delta": 0,
                "socket_submitted_messages_total_delta": 0,
                "send_enqueued_bytes_per_sec": 0.0,
                "send_enqueued_messages_per_sec": 0.0,
                "socket_submitted_bytes_per_sec": 0.0,
                "socket_submitted_messages_per_sec": 0.0,
            }

        def _query_total(metric_name: str, metric_label: str) -> float:
            total = 0.0
            for node_label, role_label in matched_label_pairs.keys():
                promql = (
                    f'{metric_name}{{node="{node_label}",role="{role_label}",metric="{metric_label}"}}'
                )
                series_list = _prometheus_query_range(
                    base_url=prom_base,
                    promql=promql,
                    start_s=start_s,
                    end_s=end_s,
                    step="5s",
                )
                total += _sum_prometheus_range_delta(series_list)
            return total

        try:
            send_enqueued_bytes_total = _query_total(
                TCP_THREAD_PROM_METRIC_BYTES_TOTAL,
                TCP_THREAD_PROM_METRIC_SEND_ENQUEUED,
            )
            send_enqueued_messages_total = _query_total(
                TCP_THREAD_PROM_METRIC_MESSAGES_TOTAL,
                TCP_THREAD_PROM_METRIC_SEND_ENQUEUED,
            )
            socket_submitted_bytes_total = _query_total(
                TCP_THREAD_PROM_METRIC_BYTES_TOTAL,
                TCP_THREAD_PROM_METRIC_SOCKET_SUBMITTED,
            )
            socket_submitted_messages_total = _query_total(
                TCP_THREAD_PROM_METRIC_MESSAGES_TOTAL,
                TCP_THREAD_PROM_METRIC_SOCKET_SUBMITTED,
            )
        except Exception as exc:
            logger.warning(f"⚠️ 收集 tcp_thread_transport_summary 失败: {exc}")
            return {"error": str(exc), "target": target, "leader": True}

        matched_label_pair_list = [
            {
                "node": node_label,
                "role": role_label,
                "send_total_sample_count_delta": float(sample_count_delta),
            }
            for (node_label, role_label), sample_count_delta in sorted(matched_label_pairs.items())
        ]

        return {
            "target": target,
            "leader": True,
            "window_seconds": window_seconds,
            "matched_latency_series_count": matched_series_count,
            "matched_label_pairs": matched_label_pair_list,
            "send_enqueued_bytes_total_delta": int(send_enqueued_bytes_total),
            "send_enqueued_messages_total_delta": int(send_enqueued_messages_total),
            "socket_submitted_bytes_total_delta": int(socket_submitted_bytes_total),
            "socket_submitted_messages_total_delta": int(socket_submitted_messages_total),
            "send_enqueued_bytes_per_sec": (
                float(send_enqueued_bytes_total) / float(window_seconds)
                if window_seconds > 0.0
                else 0.0
            ),
            "send_enqueued_messages_per_sec": (
                float(send_enqueued_messages_total) / float(window_seconds)
                if window_seconds > 0.0
                else 0.0
            ),
            "socket_submitted_bytes_per_sec": (
                float(socket_submitted_bytes_total) / float(window_seconds)
                if window_seconds > 0.0
                else 0.0
            ),
            "socket_submitted_messages_per_sec": (
                float(socket_submitted_messages_total) / float(window_seconds)
                if window_seconds > 0.0
                else 0.0
            ),
        }

    def _p2p_receive_transport_summary(self) -> Dict[str, Any]:
        if not isinstance(self.test_config, dict):
            return {}
        prom_base_raw = self.test_config.get("prometheus_base_url")
        if not isinstance(prom_base_raw, str) or not prom_base_raw.strip():
            return {}
        network_sample_cfg = self._network_sample_config()
        if network_sample_cfg is None:
            return {}
        target_raw = network_sample_cfg.get("target")
        if not isinstance(target_raw, str) or not target_raw.strip():
            return {"error": "network_sample.target must be a non-empty string"}
        target = target_raw.strip()
        leader = bool(network_sample_cfg.get("leader"))
        if not leader:
            return {
                "target": target,
                "leader": False,
            }
        if self.start_time is None or self.end_time is None:
            return {}

        warmup_deadline_ts = self.start_time + METRIC_WARMUP_SECONDS
        cutoff_ts = self.end_time
        window_seconds = max(0.0, float(cutoff_ts - warmup_deadline_ts))
        if window_seconds <= 0.0:
            return {}

        prom_base = prom_base_raw.strip().rstrip("/")
        start_s = warmup_deadline_ts
        end_s = cutoff_ts + 35.0

        try:
            discovery_series = _prometheus_query_range(
                base_url=prom_base,
                promql=(
                    f'{P2P_RECV_PROM_METRIC_MESSAGES_TOTAL}'
                    f'{{metric="{P2P_RECV_PROM_METRIC_RECV_COMPLETED}"}}'
                ),
                start_s=start_s,
                end_s=end_s,
                step="5s",
            )
        except Exception as exc:
            logger.warning(f"⚠️ 收集 p2p_receive_transport_summary 失败: {exc}")
            return {"error": str(exc)}

        matched_label_pairs: Dict[Tuple[str, str], float] = {}
        matched_series_count = 0
        for series in discovery_series:
            labels = _prometheus_series_labels(series)
            node_label = labels.get("node", "")
            role_label = labels.get("role", "")
            if not node_label or not role_label:
                continue
            if not _prometheus_node_label_matches_target(node_label, target):
                continue
            delta = _prometheus_series_delta(series)
            if delta <= 0.0:
                continue
            matched_series_count += 1
            key = (node_label, role_label)
            matched_label_pairs[key] = matched_label_pairs.get(key, 0.0) + delta

        def _query_total(component_label: str, metric_label: str, metric_name: str) -> float:
            total = 0.0
            for node_label, role_label in matched_label_pairs.keys():
                promql = (
                    f'{metric_name}{{node="{node_label}",role="{role_label}",'
                    f'component="{component_label}",metric="{metric_label}"}}'
                )
                series_list = _prometheus_query_range(
                    base_url=prom_base,
                    promql=promql,
                    start_s=start_s,
                    end_s=end_s,
                    step="5s",
                )
                total += _sum_prometheus_range_delta(series_list)
            return total

        components = _empty_p2p_receive_transport_components()
        for component in P2P_RECV_PROM_COMPONENTS:
            for metric in P2P_RECV_PROM_METRICS:
                bytes_total = _query_total(
                    component,
                    metric,
                    P2P_RECV_PROM_METRIC_BYTES_TOTAL,
                )
                messages_total = _query_total(
                    component,
                    metric,
                    P2P_RECV_PROM_METRIC_MESSAGES_TOTAL,
                )
                components[component][metric] = {
                    "bytes_total_delta": int(bytes_total),
                    "messages_total_delta": int(messages_total),
                    "bytes_per_sec": (
                        float(bytes_total) / float(window_seconds)
                        if window_seconds > 0.0
                        else 0.0
                    ),
                    "messages_per_sec": (
                        float(messages_total) / float(window_seconds)
                        if window_seconds > 0.0
                        else 0.0
                    ),
                }

        matched_label_pair_list = [
            {
                "node": node_label,
                "role": role_label,
                "recv_completed_messages_total_delta": float(sample_count_delta),
            }
            for (node_label, role_label), sample_count_delta in sorted(matched_label_pairs.items())
        ]

        return {
            "target": target,
            "leader": True,
            "window_seconds": window_seconds,
            "matched_recv_completed_series_count": matched_series_count,
            "matched_label_pairs": matched_label_pair_list,
            "components": components,
        }

    def _p2p_rpc_completion_debug_counters(self) -> Dict[str, Any]:
        if not isinstance(self.test_config, dict):
            return {}
        if str(self.test_config.get("test_mode", "")).strip() != TestMode.RPC.value:
            return {}
        backend_kind = str(self.test_config.get("rpc_backend_kind", "")).strip().upper()
        if backend_kind != "FLUXON":
            return {}
        prom_base_raw = self.test_config.get("prometheus_base_url")
        if not isinstance(prom_base_raw, str) or not prom_base_raw.strip():
            return {}
        network_sample_cfg = self._network_sample_config()
        if network_sample_cfg is None:
            return {}
        target_raw = network_sample_cfg.get("target")
        if not isinstance(target_raw, str) or not target_raw.strip():
            return {"error": "network_sample.target must be a non-empty string"}
        target = target_raw.strip()
        leader = bool(network_sample_cfg.get("leader"))
        if self.start_time is None or self.end_time is None:
            return {}

        warmup_deadline_ts = self.start_time + METRIC_WARMUP_SECONDS
        cutoff_ts = self.end_time
        window_seconds = max(0.0, float(cutoff_ts - warmup_deadline_ts))
        if window_seconds <= 0.0:
            return {}

        prom_base = prom_base_raw.strip().rstrip("/")
        start_s = warmup_deadline_ts
        end_s = cutoff_ts + 35.0

        summary: Dict[str, Any] = {
            "scope": P2P_RPC_COMPLETION_SUMMARY_SCOPE_OWNER_OWNER,
            "semantic_role": P2P_RPC_COMPLETION_SUMMARY_DEBUG_ROLE,
            "target": target,
            "leader": leader,
            "window_seconds": window_seconds,
            "network_sample_target": target,
            "client_instance_key": "",
            "server_instance_keys": [],
            "matched_activity_series_count": 0,
            "matched_label_pairs": [],
            "request_fast_bytes_total_delta": 0,
            "request_fast_messages_total_delta": 0,
            "request_slow_bytes_total_delta": 0,
            "request_slow_messages_total_delta": 0,
            "response_fast_bytes_total_delta": 0,
            "response_fast_messages_total_delta": 0,
            "response_slow_bytes_total_delta": 0,
            "response_slow_messages_total_delta": 0,
            "request_fast_bytes_per_sec": 0.0,
            "request_fast_messages_per_sec": 0.0,
            "request_slow_bytes_per_sec": 0.0,
            "request_slow_messages_per_sec": 0.0,
            "response_fast_bytes_per_sec": 0.0,
            "response_fast_messages_per_sec": 0.0,
            "response_slow_bytes_per_sec": 0.0,
            "response_slow_messages_per_sec": 0.0,
        }

        def _query_total_for_node_role(
            *,
            node_roles: List[Tuple[str, str]],
            metric_name: str,
            metric_label: str,
        ) -> float:
            total = 0.0
            for node_label, role_label in node_roles:
                series_list = _prometheus_query_range(
                    base_url=prom_base,
                    promql=(
                        f'{metric_name}{{node="{node_label}",role="{role_label}",'
                        f'metric="{metric_label}"}}'
                    ),
                    start_s=start_s,
                    end_s=end_s,
                    step="5s",
                )
                total += _sum_prometheus_range_delta(series_list)
            return total

        if not leader:
            return summary

        try:
            runtime_cfg = _rpc_runtime_config_from_test_config(self.test_config)
        except Exception as exc:
            logger.warning(f"⚠️ 解析 rpc runtime config 失败: {exc}")
            return {
                "scope": P2P_RPC_COMPLETION_SUMMARY_SCOPE_OWNER_OWNER,
                "semantic_role": P2P_RPC_COMPLETION_SUMMARY_DEBUG_ROLE,
                "error": str(exc),
                "target": target,
                "leader": True,
            }

        scoped_server_instance_keys = [
            instance_key
            for instance_key in runtime_cfg.server_instance_keys
            if _prometheus_node_label_matches_target(str(instance_key), target)
        ]
        summary["server_instance_keys"] = scoped_server_instance_keys
        if not scoped_server_instance_keys:
            return summary

        try:
            (
                matched_activity_series_count,
                matched_label_pair_list,
                matched_node_roles,
            ) = _discover_active_explicit_node_roles(
                base_url=prom_base,
                promql=(
                    f'{P2P_RPC_COMPLETION_PROM_METRIC_MESSAGES_TOTAL}'
                    f'{{metric="{P2P_RPC_COMPLETION_PROM_METRIC_RESPONSE_SUBMITTED}"}}'
                ),
                start_s=start_s,
                end_s=end_s,
                allowed_nodes=scoped_server_instance_keys,
                delta_key="response_submitted_messages_total_delta",
            )
        except Exception as exc:
            logger.warning(f"⚠️ 收集 p2p_rpc_completion_summary 失败: {exc}")
            return {
                "scope": P2P_RPC_COMPLETION_SUMMARY_SCOPE_OWNER_OWNER,
                "semantic_role": P2P_RPC_COMPLETION_SUMMARY_DEBUG_ROLE,
                "error": str(exc),
                "target": target,
                "leader": True,
            }

        if not matched_node_roles:
            return summary

        try:
            request_fast_bytes_total = _query_total_for_node_role(
                node_roles=matched_node_roles,
                metric_name=P2P_RPC_COMPLETION_PROM_METRIC_BYTES_TOTAL,
                metric_label=P2P_RPC_COMPLETION_PROM_METRIC_USER_RPC_REQUEST_FAST_PATH_USED,
            )
            request_fast_messages_total = _query_total_for_node_role(
                node_roles=matched_node_roles,
                metric_name=P2P_RPC_COMPLETION_PROM_METRIC_MESSAGES_TOTAL,
                metric_label=P2P_RPC_COMPLETION_PROM_METRIC_USER_RPC_REQUEST_FAST_PATH_USED,
            )
            request_slow_bytes_total = _query_total_for_node_role(
                node_roles=matched_node_roles,
                metric_name=P2P_RPC_COMPLETION_PROM_METRIC_BYTES_TOTAL,
                metric_label=P2P_RPC_COMPLETION_PROM_METRIC_USER_RPC_REQUEST_SLOW_PATH_USED,
            )
            request_slow_messages_total = _query_total_for_node_role(
                node_roles=matched_node_roles,
                metric_name=P2P_RPC_COMPLETION_PROM_METRIC_MESSAGES_TOTAL,
                metric_label=P2P_RPC_COMPLETION_PROM_METRIC_USER_RPC_REQUEST_SLOW_PATH_USED,
            )
            response_fast_bytes_total = _query_total_for_node_role(
                node_roles=matched_node_roles,
                metric_name=P2P_RPC_COMPLETION_PROM_METRIC_BYTES_TOTAL,
                metric_label=P2P_RPC_COMPLETION_PROM_METRIC_USER_RPC_RESPONSE_FAST_PATH_USED,
            )
            response_fast_messages_total = _query_total_for_node_role(
                node_roles=matched_node_roles,
                metric_name=P2P_RPC_COMPLETION_PROM_METRIC_MESSAGES_TOTAL,
                metric_label=P2P_RPC_COMPLETION_PROM_METRIC_USER_RPC_RESPONSE_FAST_PATH_USED,
            )
            response_slow_bytes_total = _query_total_for_node_role(
                node_roles=matched_node_roles,
                metric_name=P2P_RPC_COMPLETION_PROM_METRIC_BYTES_TOTAL,
                metric_label=P2P_RPC_COMPLETION_PROM_METRIC_USER_RPC_RESPONSE_SLOW_PATH_USED,
            )
            response_slow_messages_total = _query_total_for_node_role(
                node_roles=matched_node_roles,
                metric_name=P2P_RPC_COMPLETION_PROM_METRIC_MESSAGES_TOTAL,
                metric_label=P2P_RPC_COMPLETION_PROM_METRIC_USER_RPC_RESPONSE_SLOW_PATH_USED,
            )
        except Exception as exc:
            logger.warning(f"⚠️ 收集 owner-owner p2p_rpc_completion_summary 失败: {exc}")
            return {
                "scope": P2P_RPC_COMPLETION_SUMMARY_SCOPE_OWNER_OWNER,
                "semantic_role": P2P_RPC_COMPLETION_SUMMARY_DEBUG_ROLE,
                "error": str(exc),
                "target": target,
                "leader": True,
            }

        summary["matched_activity_series_count"] = matched_activity_series_count
        summary["matched_label_pairs"] = matched_label_pair_list
        summary["request_fast_bytes_total_delta"] = int(request_fast_bytes_total)
        summary["request_fast_messages_total_delta"] = int(request_fast_messages_total)
        summary["request_slow_bytes_total_delta"] = int(request_slow_bytes_total)
        summary["request_slow_messages_total_delta"] = int(request_slow_messages_total)
        summary["response_fast_bytes_total_delta"] = int(response_fast_bytes_total)
        summary["response_fast_messages_total_delta"] = int(response_fast_messages_total)
        summary["response_slow_bytes_total_delta"] = int(response_slow_bytes_total)
        summary["response_slow_messages_total_delta"] = int(response_slow_messages_total)
        summary["request_fast_bytes_per_sec"] = (
            float(request_fast_bytes_total) / float(window_seconds)
            if window_seconds > 0.0
            else 0.0
        )
        summary["request_fast_messages_per_sec"] = (
            float(request_fast_messages_total) / float(window_seconds)
            if window_seconds > 0.0
            else 0.0
        )
        summary["request_slow_bytes_per_sec"] = (
            float(request_slow_bytes_total) / float(window_seconds)
            if window_seconds > 0.0
            else 0.0
        )
        summary["request_slow_messages_per_sec"] = (
            float(request_slow_messages_total) / float(window_seconds)
            if window_seconds > 0.0
            else 0.0
        )
        summary["response_fast_bytes_per_sec"] = (
            float(response_fast_bytes_total) / float(window_seconds)
            if window_seconds > 0.0
            else 0.0
        )
        summary["response_fast_messages_per_sec"] = (
            float(response_fast_messages_total) / float(window_seconds)
            if window_seconds > 0.0
            else 0.0
        )
        summary["response_slow_bytes_per_sec"] = (
            float(response_slow_bytes_total) / float(window_seconds)
            if window_seconds > 0.0
            else 0.0
        )
        summary["response_slow_messages_per_sec"] = (
            float(response_slow_messages_total) / float(window_seconds)
            if window_seconds > 0.0
            else 0.0
        )
        return summary

    def _p2p_rpc_completion_summary(
        self,
        fluxon_phase_summary: Dict[str, Any],
        duration_seconds: float,
    ) -> Dict[str, Any]:
        debug_owner_owner_transport_counters = (
            self._p2p_rpc_completion_debug_counters()
        )
        return _build_p2p_rpc_completion_summary_from_phase_summary(
            fluxon_phase_summary=fluxon_phase_summary,
            duration_seconds=duration_seconds,
            debug_owner_owner_transport_counters=debug_owner_owner_transport_counters,
        )

    @staticmethod
    def _parse_value_size_weighted_set(
        raw_val: Any,
        *,
        ctx: str,
    ) -> List[Tuple[int, float]]:
        """Parse weighted value-size config."""
        if not isinstance(raw_val, list) or not raw_val:
            raise ValueError(f"{ctx} must be a non-empty list")
        parsed: List[Tuple[int, float]] = []
        for idx, item in enumerate(raw_val):
            item_ctx = f"{ctx}[{idx}]"
            if not isinstance(item, dict):
                raise ValueError(f"{item_ctx} must be a mapping")
            if "size_bytes" not in item:
                raise ValueError(f"{item_ctx}.size_bytes is required")
            if "weight" not in item:
                raise ValueError(f"{item_ctx}.weight is required")
            size_bytes = int(item["size_bytes"])
            if size_bytes <= 0:
                raise ValueError(f"{item_ctx}.size_bytes must be > 0, got: {size_bytes}")
            weight = float(item["weight"])
            if weight <= 0:
                raise ValueError(f"{item_ctx}.weight must be > 0, got: {weight}")
            parsed.append((size_bytes, weight))
        return parsed

    def _refresh_value_size_strategy(self) -> bool:
        """Refresh parsed value-size strategy from self.test_config."""
        if not isinstance(self.test_config, dict):
            logger.error("❌ test_config is not available for value_size strategy")
            return False

        mode_raw = self.test_config.get("value_size_mode", ValueSizeMode.FIXED.value)
        mode_str = str(mode_raw).upper()
        try:
            mode = ValueSizeMode[mode_str].value
        except KeyError:
            logger.error(
                "❌ unsupported value_size_mode: %s (expected: FIXED/RANDOM_WEIGHTED_SET)",
                mode_raw,
            )
            return False

        self.value_size_mode = mode
        self.value_size_weighted_set = []
        self._payload_pool_by_size = {}
        if mode != ValueSizeMode.FIXED.value and self.test_config.get("test_mode") == TestMode.MPMC.value:
            logger.error("❌ MPMC requires value_size_mode == FIXED")
            return False
        if mode == ValueSizeMode.FIXED.value:
            if "value_size" not in self.test_config:
                logger.error("❌ value_size is required when value_size_mode == FIXED")
                return False
            self._refresh_payload_pools()
            return True

        try:
            self.value_size_weighted_set = self._parse_value_size_weighted_set(
                self.test_config.get("value_size_weighted_set"),
                ctx="test_config.value_size_weighted_set",
            )
        except Exception as exc:
            logger.error("❌ failed to parse weighted value-size set: %s", exc)
            return False
        self._refresh_payload_pools()
        return True

    def _describe_value_size_strategy(self) -> str:
        """Return a compact human-readable value-size strategy."""
        if self.value_size_mode == ValueSizeMode.RANDOM_WEIGHTED_SET.value:
            parts = [f"{size_bytes}B@{weight}" for size_bytes, weight in self.value_size_weighted_set]
            return f"RANDOM_WEIGHTED_SET[{', '.join(parts)}]"
        if not isinstance(self.test_config, dict):
            return "FIXED[unknown]"
        return f"FIXED[{int(self.test_config.get('value_size', 0))}B]"

    def _resolve_kv_value_size(self, thread_id: int, op_idx: int) -> int:
        """Resolve per-operation KV value size."""
        if self.value_size_mode == ValueSizeMode.FIXED.value:
            return int(self.test_config.get("value_size", 0))

        if self.value_size_mode != ValueSizeMode.RANDOM_WEIGHTED_SET.value:
            raise ValueError(f"unsupported value_size_mode: {self.value_size_mode}")
        if not self.value_size_weighted_set:
            raise ValueError("weighted value-size set is empty")

        test_id = ""
        if isinstance(self.test_config, dict):
            test_id = str(self.test_config.get("test_id", ""))
        stable_key = f"{test_id}|{self.key_prefix}|{thread_id}|{op_idx}".encode("utf-8")
        stable_bucket = int.from_bytes(hashlib.sha256(stable_key).digest()[:8], "big")
        total_weight = sum(weight for _, weight in self.value_size_weighted_set)
        threshold = (stable_bucket / float(1 << 64)) * total_weight
        accum = 0.0
        for size_bytes, weight in self.value_size_weighted_set:
            accum += weight
            if threshold < accum:
                return size_bytes
        return self.value_size_weighted_set[-1][0]

    def _heartbeat_loop(self) -> None:
        while not self._heartbeat_stop.wait(BENCH_HEARTBEAT_INTERVAL_SECONDS):
            now = time.time()
            start_ts = self.start_time
            end_ts = self.end_time
            role = None
            mode = None
            threads_per_process = None
            if isinstance(self.test_config, dict):
                role = self.test_config.get("node_role")
                mode = self.test_config.get("test_mode")
                threads_per_process = self.test_config.get("threads_per_process")

            with self._inflight_lock:
                inflight = self._inflight_requests

            with self._progress_lock:
                last_finish_ts = self._last_op_finish_ts
                last_op_idx_snapshot = dict(self._thread_last_op_idx)

            progress_age_s = None
            if isinstance(last_finish_ts, (int, float)):
                progress_age_s = now - float(last_finish_ts)

            thread_min_op = None
            thread_max_op = None
            if last_op_idx_snapshot:
                vals = list(last_op_idx_snapshot.values())
                thread_min_op = min(vals)
                thread_max_op = max(vals)

            elapsed_s = None
            remaining_s = None
            if isinstance(start_ts, (int, float)):
                elapsed_s = now - float(start_ts)
            if isinstance(end_ts, (int, float)):
                remaining_s = float(end_ts) - now

            _debug_print(
                "[HEARTBEAT] "
                f"node_id={self.node_id} role={role} mode={mode} threads_per_process={threads_per_process} "
                f"inflight={inflight} ops_recorded={len(self.operation_results)} "
                f"elapsed_s={elapsed_s} remaining_s={remaining_s} "
                f"progress_age_s={progress_age_s} thread_min_op={thread_min_op} thread_max_op={thread_max_op}"
            )

    def _start_heartbeat(self) -> None:
        if self._heartbeat_thread.is_alive():
            return
        # A threading.Thread instance can only be started once. Because one
        # BenchmarkNode may run multiple benchmark rounds, we must create a
        # fresh heartbeat thread for each new round after the previous one
        # has exited.
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"bench-heartbeat-{self.node_id}",
            daemon=True,
        )
        self._heartbeat_stop.clear()
        self._heartbeat_thread.start()

    def _stop_heartbeat(self) -> None:
        self._heartbeat_stop.set()
        if self._heartbeat_thread.ident is not None:
            self._heartbeat_thread.join()

    def _inflight_begin(self) -> int:
        """Increment in-flight request count and return the current value."""
        with self._inflight_lock:
            self._inflight_requests += 1
            if self._inflight_requests <= 0:
                raise RuntimeError(
                    "inflight_requests invariant violated (<= 0 after increment)"
                )
            return self._inflight_requests

    def _inflight_end(self) -> int:
        """Decrement in-flight request count and return the current value."""
        with self._inflight_lock:
            self._inflight_requests -= 1
            if self._inflight_requests < 0:
                raise RuntimeError(
                    "inflight_requests invariant violated (< 0 after decrement)"
                )
            return self._inflight_requests

    def _close_kv_store(self, *, reason: str) -> None:
        """Close the shared KvClient exactly once."""
        with self._kv_store_close_lock:
            close_fs_runtime(self, logger=logger, reason=reason)
            close_rpc_runtime(self)
            if self.kv_store is None or self._kv_store_closed:
                return

            logger.info("🔒 Closing kv_store: reason=%s", reason)
            close_started = time.monotonic()
            close_res = self.kv_store.close()
            if not close_res.is_ok():
                close_error = close_res.unwrap_error()
                raise RuntimeError(
                    f"kv_store close failed: reason={reason} err={close_error}"
                )
            close_res.unwrap()
            self._kv_store_closed = True
            self.fluxon_client = None
            logger.info(
                "✅ kv_store closed: reason=%s elapsed_seconds=%.3f",
                reason,
                time.monotonic() - close_started,
            )

    def _prepare_mpmc_worker_runtime(
        self,
        *,
        thread_id: int,
        producer_kvcache_config: Optional[Dict[str, Any]] = None,
        runtime_init_deadline_ts: Optional[float] = None,
    ) -> PreparedWorkerRuntime:
        """Prepare one MPMC endpoint, reusing the producer process KV runtime."""
        node_role = self.test_config.get("node_role", "")
        test_mode = self.test_config.get("test_mode", "")
        if (
            test_mode == TestMode.MPMC.value
            and node_role == "producer"
            and self._mpmc_producer_runtime_mode()
            == MPMC_PRODUCER_RUNTIME_MODE_LOGICAL_ONLY
        ):
            logger.info(
                "⏭️ MPMC producer logical-only runtime prepared: thread_id=%s instance_key=%s",
                thread_id,
                self.instance_key,
            )
            return PreparedWorkerRuntime(
                producer=None,
                consumer=None,
                local_mq_state=None,
                logical_only=True,
            )
        kv_store = self.kv_store
        kv_client = self.fluxon_client
        if test_mode == TestMode.MPMC.value and node_role == "producer":
            self._sleep_for_runtime_init_stagger_once()
            if producer_kvcache_config is None:
                producer_process_kvcache_config = self._worker_owned_kvcache_config(
                    self.test_config["kvcache_config"],
                    thread_id=thread_id,
                )
            else:
                producer_process_kvcache_config = copy.deepcopy(producer_kvcache_config)

            if runtime_init_deadline_ts is None:
                deadline_s = self._runtime_init_retry_deadline_seconds()
                runtime_init_deadline_ts = (
                    time.monotonic() + deadline_s if deadline_s > 0.0 else time.monotonic()
                )
            with self._mpmc_producer_kv_store_lock:
                kv_store = self.kv_store
                kv_client = self.fluxon_client
                if kv_store is None:
                    logger.info(
                        "🔧 线程 %s 正在创建 MPMC producer process-shared KVCache 存储实例",
                        thread_id,
                    )
                    store, err = self._init_kv_store_with_ready_retry(
                        producer_process_kvcache_config,
                        deadline_ts=runtime_init_deadline_ts,
                    )
                    if err is not None:
                        raise RuntimeError(
                            f"线程 {thread_id} 创建 MPMC producer process-shared KV store 失败: {err}"
                        )
                    self._attach_fluxon_phase_summary_callback(store)
                    self.kv_store = store
                    if not isinstance(store, FluxonBlockingStore):
                        raise RuntimeError(
                            "MPMC requires the Fluxon KV backend; benchmark wrapper is missing"
                        )
                    self.fluxon_client = store.kv_client
                    kv_store = store
                    kv_client = self.fluxon_client
                    logger.info(
                        "✅ 线程 %s 已创建 MPMC producer process-shared KVCache 存储实例",
                        thread_id,
                    )

            if kv_store is None or kv_client is None:
                raise RuntimeError(
                    f"线程 {thread_id} 未获得 MPMC producer process-shared Fluxon client"
                )

            return self._prepare_mpmc_endpoint_runtime_from_kv_store(
                thread_id=thread_id,
                kv_client=kv_client,
            )

        if kv_store is None or kv_client is None:
            raise RuntimeError("MPMC 模式下 Fluxon client 未初始化")
        return self._prepare_mpmc_endpoint_runtime_from_kv_store(
            thread_id=thread_id,
            kv_client=kv_client,
        )

    def _prepare_mpmc_endpoint_runtime_from_kv_store(
        self,
        *,
        thread_id: int,
        kv_client: KvClient,
    ) -> PreparedWorkerRuntime:
        """Attach one MPMC endpoint using the canonical Fluxon client."""
        node_role = self.test_config.get("node_role", "")
        producer = None
        consumer = None
        with self._mpmc_endpoint_prepare_lock:
            if node_role == "producer":
                producer, _, err = init_mq_channel(
                    role="producer",
                    kv_store=kv_client,
                    chan_config=self.chan_config,
                    unique_id=self.mq_unique_id,
                    weight=self.mq_state.weight if self.mq_state else 1.0,
                )
                if err is not None:
                    raise RuntimeError(f"线程 {thread_id} 初始化 MPMC producer 失败: {err}")
                if self.mq_state is not None and self.mq_state.producer_id is None:
                    self.mq_state.producer_id = self.instance_key or self.node_id
            elif node_role == "consumer":
                _, consumer, err = init_mq_channel(
                    role="consumer",
                    kv_store=kv_client,
                    chan_config=self.chan_config,
                    unique_id=self.mq_unique_id,
                    weight=self.mq_state.weight if self.mq_state else 1.0,
                )
                if err is not None:
                    raise RuntimeError(f"线程 {thread_id} 初始化 MPMC consumer 失败: {err}")
            else:
                raise RuntimeError(f"不支持的 MPMC 角色: {node_role}")

        local_mq_state: Optional[MQState] = None
        if node_role == "producer":
            local_mq_state = MQState(
                role=self.mq_state.role,
                weight=self.mq_state.weight,
                config=dict(self.mq_state.config),
                chan_config=dict(self.mq_state.chan_config),
                producer_id=self.mq_state.producer_id or self.instance_key or self.node_id,
            )

        return PreparedWorkerRuntime(
            producer=producer,
            consumer=consumer,
            local_mq_state=local_mq_state,
        )

    def _prepare_mpmc_worker_runtime_with_retry(
        self,
        *,
        thread_id: int,
        deadline_ts: float,
        ctx: str,
    ) -> PreparedWorkerRuntime:
        attempt = 0
        last_err = ""
        while True:
            if self._benchmark_stop.is_set():
                raise RuntimeError(
                    f"MPMC endpoint prepare stopped before completion: ctx={ctx} thread_id={thread_id}"
                )
            attempt += 1
            try:
                runtime = self._prepare_mpmc_worker_runtime(
                    thread_id=thread_id,
                    runtime_init_deadline_ts=deadline_ts,
                )
                if attempt > 1:
                    logger.info(
                        "✅ MPMC endpoint prepare retry succeeded: ctx=%s thread_id=%s attempts=%s",
                        ctx,
                        thread_id,
                        attempt,
                    )
                return runtime
            except Exception as exc:
                last_err = str(exc)
                if not self._is_retryable_runtime_init_error(last_err):
                    raise
                remaining_s = deadline_ts - time.monotonic()
                if remaining_s <= 0.0:
                    raise RuntimeError(
                        "MPMC endpoint prepare retry deadline exceeded: "
                        f"ctx={ctx} thread_id={thread_id} attempts={attempt} err={last_err}"
                    ) from exc
                sleep_s = min(
                    remaining_s,
                    self._runtime_init_retry_sleep_seconds(attempt=attempt),
                )
                logger.warning(
                    "⚠️ MPMC endpoint prepare 遇到瞬时错误，将重试: ctx=%s thread_id=%s attempt=%s sleep_seconds=%.2f remaining_seconds=%.1f err=%s",
                    ctx,
                    thread_id,
                    attempt,
                    sleep_s,
                    remaining_s,
                    last_err,
                )
                time.sleep(sleep_s)

    def _wait_mpmc_cluster_ready(
        self,
        *,
        runtime: PreparedWorkerRuntime,
        expected_workers: int,
        timeout_s: float,
    ) -> Any:
        """Wait until the MPMC topology is ready before starting metrics."""
        if timeout_s <= 0:
            raise RuntimeError(f"MPMC cluster ready timeout_s must be > 0, got {timeout_s}")
        endpoint = runtime.producer if runtime.producer is not None else runtime.consumer
        if endpoint is None:
            raise RuntimeError("MPMC cluster ready probe requires a prepared endpoint")

        role = self.test_config.get("node_role", "")
        deadline_ts = time.time() + float(timeout_s)
        last_wait_log_ts = 0.0
        last_wait_reason = ""
        while True:
            try:
                snapshot = get_cluster_info_snapshot(endpoint)
            except Exception as exc:  # noqa: BLE001
                now = time.time()
                observation_reason = (
                    "topology observation unavailable: "
                    f"{type(exc).__name__}: {exc}"
                )
                if now >= deadline_ts:
                    raise RuntimeError(
                        "MPMC topology did not become observable before timeout: "
                        f"role={role} expected_workers={expected_workers} "
                        f"reason={observation_reason}"
                    ) from exc
                if (
                    observation_reason != last_wait_reason
                    or now - last_wait_log_ts >= 10.0
                ):
                    logger.info(
                        "⏳ MPMC topology waiting: role=%s expected_workers=%s reason=%s",
                        role,
                        expected_workers,
                        observation_reason,
                    )
                    last_wait_log_ts = now
                    last_wait_reason = observation_reason
                time.sleep(1.0)
                continue
            ready_channels = snapshot.ready_channels
            active_consumers = snapshot.active_consumers
            readiness = evaluate_mpmc_topology_ready(
                role=role,
                expected_workers=expected_workers,
                ready_channels=ready_channels,
                active_consumers=active_consumers,
            )
            if readiness.ready:
                logger.info(
                    "✅ MPMC topology ready: role=%s expected_workers=%s mpmc_id=%s ready_channels=%s active_consumers=%s",
                    role,
                    expected_workers,
                    snapshot.mpmc_id,
                    ready_channels,
                    active_consumers,
                )
                return snapshot
            now = time.time()
            if now >= deadline_ts:
                raise RuntimeError(
                    "MPMC topology did not become ready before timeout: "
                    f"role={role} expected_workers={expected_workers} "
                    f"mpmc_id={snapshot.mpmc_id} "
                    f"ready_channels={ready_channels} active_consumers={active_consumers} "
                    f"reason={readiness.reason}"
                )
            if readiness.reason != last_wait_reason or now - last_wait_log_ts >= 10.0:
                logger.info(
                    "⏳ MPMC topology waiting: role=%s expected_workers=%s mpmc_id=%s ready_channels=%s active_consumers=%s reason=%s",
                    role,
                    expected_workers,
                    snapshot.mpmc_id,
                    ready_channels,
                    active_consumers,
                    readiness.reason,
                )
                last_wait_log_ts = now
                last_wait_reason = readiness.reason
            time.sleep(1.0)

    def _wait_mpmc_prefeed_topology(
        self,
        *,
        runtime: PreparedWorkerRuntime,
        timeout_s: float,
    ) -> Any:
        """Wait for two identical authoritative observations of the full consumer topology."""
        if runtime.producer is None:
            raise RuntimeError("MPMC prefeed topology requires a producer endpoint")
        expected_channels = self._expected_mpmc_consumer_workers()
        deadline_ts = time.monotonic() + float(timeout_s)
        previous_stable_key: Optional[Tuple[Any, ...]] = None
        stable_observations = 0
        last_reason = ""
        last_log_ts = 0.0

        while True:
            snapshot = get_cluster_info_snapshot(runtime.producer)
            ready_channel_ids = snapshot.ready_channel_ids
            active_consumers = snapshot.active_consumers

            if len(ready_channel_ids) != expected_channels:
                reason = (
                    "ready channel count mismatch: "
                    f"actual={len(ready_channel_ids)} expected={expected_channels} "
                    f"ids={ready_channel_ids}"
                )
                stable_key = None
            elif len(set(ready_channel_ids)) != expected_channels:
                reason = f"ready channel authority contains duplicate ids: ids={ready_channel_ids}"
                stable_key = None
            elif active_consumers != expected_channels:
                reason = (
                    "active consumer count mismatch: "
                    f"actual={active_consumers} expected={expected_channels}"
                )
                stable_key = None
            else:
                stable_key = (
                    snapshot.mpmc_id,
                    ready_channel_ids,
                    active_consumers,
                )
                reason = "ready"

            if stable_key is not None:
                if stable_key == previous_stable_key:
                    stable_observations += 1
                else:
                    previous_stable_key = stable_key
                    stable_observations = 1
                if stable_observations >= 2:
                    logger.info(
                        "✅ MPMC prefeed topology stable: expected_channels=%s ready_channel_ids=%s",
                        expected_channels,
                        ready_channel_ids,
                    )
                    return snapshot
            else:
                previous_stable_key = None
                stable_observations = 0

            now = time.monotonic()
            if now >= deadline_ts:
                raise RuntimeError(
                    "MPMC prefeed topology did not stabilize before timeout: "
                    f"expected_channels={expected_channels} reason={reason}"
                )
            if reason != last_reason or now - last_log_ts >= 10.0:
                logger.info(
                    "⏳ MPMC prefeed topology waiting: expected_channels=%s reason=%s",
                    expected_channels,
                    reason,
                )
                last_reason = reason
                last_log_ts = now
            time.sleep(min(1.0, max(0.0, deadline_ts - now)))

    def _prefeed_mpmc_producer_channels(
        self,
        *,
        runtime: PreparedWorkerRuntime,
        thread_id: int,
        timeout_s: float,
    ) -> None:
        """Open and feed every known MPSC channel before releasing consumers."""
        if runtime.logical_only:
            return
        producer = runtime.producer
        if producer is None:
            raise RuntimeError(
                f"MPMC producer prefeed requires an active producer endpoint: thread_id={thread_id}"
            )

        snapshot = self._wait_mpmc_prefeed_topology(
            runtime=runtime,
            timeout_s=timeout_s,
        )
        channel_ids = snapshot.ready_channel_ids
        total_mpsc_channels = len(channel_ids)
        if total_mpsc_channels <= 0:
            raise RuntimeError(
                "MPMC producer prefeed requires at least one MPSC channel: "
                f"thread_id={thread_id} total_mpsc_channels={total_mpsc_channels}"
            )

        mq_state = runtime.local_mq_state or self.mq_state
        if mq_state is None:
            raise RuntimeError(
                f"MPMC producer prefeed requires MQ state: thread_id={thread_id}"
            )

        value_size = int(self.test_config.get("value_size", 1024))
        message_count = total_mpsc_channels * MPMC_PRODUCER_PREFEED_MESSAGES_PER_CHANNEL
        fallback_producer_id = self.instance_key or self.node_id
        prefeed_deadline_ts = time.monotonic() + float(timeout_s)
        logger.info(
            "🔥 MPMC producer prefeed start: thread_id=%s total_mpsc_channels=%s messages=%s value_size=%s",
            thread_id,
            total_mpsc_channels,
            message_count,
            value_size,
        )

        completed = 0
        for channel_id in channel_ids:
            for channel_message_idx in range(MPMC_PRODUCER_PREFEED_MESSAGES_PER_CHANNEL):
                if self._benchmark_stop.is_set():
                    raise RuntimeError(
                        "MPMC producer prefeed stopped: "
                        f"thread_id={thread_id} channel_id={channel_id} completed={completed}"
                    )
                if time.monotonic() >= prefeed_deadline_ts:
                    raise RuntimeError(
                        "MPMC producer prefeed timed out before completing: "
                        f"thread_id={thread_id} completed={completed}/{message_count} "
                        f"timeout_s={timeout_s}"
                    )

                value = build_message(
                    mq_state,
                    value_size,
                    fallback_producer_id=fallback_producer_id,
                    message_kind=MQ_MESSAGE_KIND_PREFEED,
                )
                put_start = time.perf_counter()
                err = mq_put_to_channel_once(producer, channel_id, value)
                put_latency_s = time.perf_counter() - put_start
                completed += 1
                if put_latency_s >= MPMC_PRODUCER_PREFEED_SLOW_PUT_LOG_SECONDS:
                    logger.warning(
                        "⚠️ MPMC producer prefeed slow PUT: thread_id=%s channel_id=%s "
                        "channel_message=%s/%s completed=%s/%s latency_s=%.3f",
                        thread_id,
                        channel_id,
                        channel_message_idx + 1,
                        MPMC_PRODUCER_PREFEED_MESSAGES_PER_CHANNEL,
                        completed,
                        message_count,
                        put_latency_s,
                    )
                if err is not None:
                    raise RuntimeError(
                        "MPMC producer prefeed PUT failed: "
                        f"thread_id={thread_id} channel_id={channel_id} "
                        f"channel_message={channel_message_idx + 1}/"
                        f"{MPMC_PRODUCER_PREFEED_MESSAGES_PER_CHANNEL} err={err}"
                    )

        final_snapshot = self._wait_mpmc_prefeed_topology(
            runtime=runtime,
            timeout_s=max(0.001, prefeed_deadline_ts - time.monotonic()),
        )
        final_channel_ids = final_snapshot.ready_channel_ids
        if final_channel_ids != channel_ids:
            raise RuntimeError(
                "MPMC channel topology changed during prefeed: "
                f"before={channel_ids} after={final_channel_ids}"
            )

        logger.info(
            "✅ MPMC producer prefeed complete: thread_id=%s total_mpsc_channels=%s messages=%s",
            thread_id,
            total_mpsc_channels,
            message_count,
        )

    def _consume_mpmc_prefeed_messages(
        self,
        *,
        round_state: PreparedMPMCRound,
        timeout_s: float,
    ) -> None:
        """Drain and validate each consumer endpoint's prefeed before metric start."""
        deadline_ts = time.monotonic() + float(timeout_s)
        for thread_id, runtime in sorted(round_state.prepared_runtimes.items()):
            consumer = runtime.consumer
            if consumer is None:
                raise RuntimeError(
                    f"MPMC consumer prefeed drain requires endpoint: thread_id={thread_id}"
                )
            consumed = 0
            while consumed < MPMC_PRODUCER_PREFEED_MESSAGES_PER_CHANNEL:
                if time.monotonic() >= deadline_ts:
                    raise RuntimeError(
                        "MPMC consumer prefeed drain timed out: "
                        f"thread_id={thread_id} consumed={consumed}/"
                        f"{MPMC_PRODUCER_PREFEED_MESSAGES_PER_CHANNEL}"
                    )
                outcome = mq_get_once(consumer, batch_size=1)
                if outcome.status == MQGetStatus.NO_MESSAGE:
                    time.sleep(MPMC_NO_MESSAGE_RETRY_SLEEP_SECONDS)
                    continue
                if outcome.status != MQGetStatus.DATA or not outcome.ok:
                    raise RuntimeError(
                        "MPMC consumer prefeed drain failed: "
                        f"thread_id={thread_id} status={outcome.status.value} "
                        f"err={outcome.error_msg}"
                    )
                if outcome.message_kind != MQ_MESSAGE_KIND_PREFEED:
                    raise RuntimeError(
                        "MPMC consumer received non-prefeed message before runtime start: "
                        f"thread_id={thread_id} message_kind={outcome.message_kind!r}"
                    )
                consumed += 1
            logger.info(
                "✅ MPMC consumer prefeed drained: thread_id=%s messages=%s",
                thread_id,
                consumed,
            )

    def _prepare_mpmc_round_before_ready(self, *, workers: int) -> None:
        """Prepare one MPMC round before reporting READY to the coordinator."""
        if self._prepared_mpmc_round is not None:
            raise RuntimeError("MPMC round is already prepared before READY")

        role = self.test_config.get("node_role")
        mode = self.test_config.get("test_mode")
        cluster_ready_timeout_s = float(self.test_config["cluster_ready_timeout_seconds"])
        if role not in {"producer", "consumer"}:
            raise RuntimeError(f"不支持的 MPMC 角色: {role}")

        round_state = PreparedMPMCRound()
        self._prepared_mpmc_round = round_state

        def worker_target(thread_id: int) -> None:
            try:
                prepare_retry_deadline_ts = time.monotonic() + cluster_ready_timeout_s
                runtime = self._prepare_mpmc_worker_runtime_with_retry(
                    thread_id=thread_id,
                    deadline_ts=prepare_retry_deadline_ts,
                    ctx="before_ready",
                )
                with round_state.prepared_lock:
                    round_state.prepared_runtimes[thread_id] = runtime
                if role == "producer" and self._is_mpmc_producer_prefeed_leader(
                    thread_id=thread_id
                ):
                    self._prefeed_mpmc_producer_channels(
                        runtime=runtime,
                        thread_id=thread_id,
                        timeout_s=cluster_ready_timeout_s,
                    )
                elif role == "producer" and not runtime.logical_only:
                    logger.info(
                        "⏭️ MPMC producer prefeed skipped on non-leader: "
                        "thread_id=%s instance_key=%s",
                        thread_id,
                        self.instance_key,
                    )
                with round_state.prepared_lock:
                    round_state.ready_thread_ids.add(thread_id)
                logger.info("✅ 线程 %s 已完成 MPMC endpoint prepare", thread_id)
                round_state.start_event.wait()
                if self._benchmark_stop.is_set():
                    result_list = []
                else:
                    deadline_ts = round_state.benchmark_deadline_ts
                    if deadline_ts is None:
                        raise RuntimeError(
                            "MPMC benchmark deadline must be published before worker start"
                        )
                    result_list = self._run_worker_thread(
                        thread_id,
                        deadline_ts,
                        prepared_runtime=runtime,
                    )
                if role == "producer":
                    _debug_print(
                        f"worker {thread_id} wrapper done, ops={len(result_list)}"
                    )
            except BaseException as exc:
                with round_state.prepared_lock:
                    if isinstance(exc, Exception):
                        round_state.prepare_errors[thread_id] = str(exc)
                    else:
                        round_state.fatal_errors[thread_id] = exc
                if isinstance(exc, Exception):
                    logger.error(f"❌ 线程 {thread_id} 执行异常: {exc}")
                else:
                    logger.critical(
                        "💥 MPMC worker fatal BaseException: thread_id=%s type=%s err=%s",
                        thread_id,
                        type(exc).__name__,
                        exc,
                    )
                if role == "producer":
                    _debug_print(
                        f"worker {thread_id} wrapper exception: {type(exc).__name__}: {exc}"
                    )
                result_list = []
            with round_state.worker_results_lock:
                round_state.worker_results[thread_id] = result_list

        defer_worker_start_until_start = role == "producer"
        for thread_id in range(workers):
            thread = threading.Thread(
                target=worker_target,
                args=(thread_id,),
                name=f"bench-worker-{self.node_id}-{thread_id}",
                daemon=False,
            )
            round_state.pending_threads[thread_id] = thread
            if not defer_worker_start_until_start:
                thread.start()

        if defer_worker_start_until_start:
            self._prepared_mpmc_round = round_state
            logger.info(
                "✅ MPMC producer round deferred until START: workers=%s role=%s",
                workers,
                role,
            )
            return

        prepare_deadline_ts = time.time() + cluster_ready_timeout_s
        while True:
            with round_state.prepared_lock:
                prepared_count = len(round_state.ready_thread_ids)
                prepare_error_snapshot = dict(round_state.prepare_errors)
                fatal_error_snapshot = dict(round_state.fatal_errors)
            if fatal_error_snapshot:
                self._raise_mpmc_fatal_worker_error(
                    round_state=round_state,
                    fatal_errors=fatal_error_snapshot,
                    stage="prepare_before_ready",
                )
            if prepare_error_snapshot:
                self._finish_mpmc_round(
                    round_state=round_state,
                    reason="prepare_before_ready_failed",
                )
                self._prepared_mpmc_round = None
                raise RuntimeError(
                    "MPMC worker prepare failed: "
                    + ", ".join(
                        f"thread_{thread_id}={err}"
                        for thread_id, err in sorted(prepare_error_snapshot.items())
                    )
                )
            if prepared_count == workers:
                break
            if time.time() >= prepare_deadline_ts:
                self._finish_mpmc_round(
                    round_state=round_state,
                    reason="prepare_before_ready_timeout",
                )
                self._prepared_mpmc_round = None
                raise RuntimeError(
                    f"MPMC worker prepare timed out: prepared={prepared_count}/{workers}"
                )
            time.sleep(0.5)

        try:
            self._wait_mpmc_cluster_ready(
                runtime=round_state.prepared_runtimes[0],
                expected_workers=workers,
                timeout_s=cluster_ready_timeout_s,
            )
        except BaseException:
            self._finish_mpmc_round(
                round_state=round_state,
                reason="cluster_ready_failed",
            )
            self._prepared_mpmc_round = None
            raise
        logger.info(
            "✅ MPMC round prepared before READY: workers=%s role=%s",
            workers,
            role,
        )

    def _consume_prepared_mpmc_round(self, *, expected_workers: int) -> PreparedMPMCRound:
        """Take the prepared MPMC round that was built before READY."""
        round_state = self._prepared_mpmc_round
        if round_state is None:
            raise RuntimeError("MPMC round must be prepared before run_benchmark")
        actual_workers = len(round_state.pending_threads)
        if actual_workers != expected_workers:
            raise RuntimeError(
                f"prepared MPMC worker count mismatch: expected={expected_workers} actual={actual_workers}"
            )
        self._prepared_mpmc_round = None
        return round_state

    def _raise_mpmc_fatal_worker_error(
        self,
        *,
        round_state: PreparedMPMCRound,
        fatal_errors: Dict[int, BaseException],
        stage: str,
    ) -> None:
        """Stop this node after a worker panic or another fatal BaseException."""
        if not fatal_errors:
            raise ValueError("fatal_errors must not be empty")
        try:
            self._finish_mpmc_round(
                round_state=round_state,
                reason=f"fatal_worker_error:{stage}",
            )
            self._close_kv_store(reason=f"mpmc_fatal_worker_error:{stage}")
        except BaseException as cleanup_exc:
            logger.error(
                "MPMC fatal cleanup raised: stage=%s type=%s err=%s",
                stage,
                type(cleanup_exc).__name__,
                cleanup_exc,
            )

        first_thread_id = sorted(fatal_errors)[0]
        first_error = fatal_errors[first_thread_id]
        details = ", ".join(
            f"thread_{thread_id}={type(exc).__name__}: {exc}"
            for thread_id, exc in sorted(fatal_errors.items())
        )
        raise RuntimeError(
            f"fatal MPMC worker BaseException at {stage}; runtime is not recoverable: {details}"
        ) from first_error

    def _close_mpmc_round_endpoints(
        self,
        *,
        round_state: PreparedMPMCRound,
        reason: str,
    ) -> None:
        """Close every round-owned endpoint and consume every close result."""
        role = self.test_config.get("node_role")
        with round_state.prepared_lock:
            runtime_items = sorted(round_state.prepared_runtimes.items())
            if role == "producer":
                endpoint_label = "producer"
                blocked_op_label = "PUT"
                endpoint_items = [
                    (thread_id, runtime.producer)
                    for thread_id, runtime in runtime_items
                    if runtime.producer is not None
                ]
            elif role == "consumer":
                endpoint_label = "consumer"
                blocked_op_label = "GET"
                endpoint_items = [
                    (thread_id, runtime.consumer)
                    for thread_id, runtime in runtime_items
                    if runtime.consumer is not None
                ]
            else:
                raise RuntimeError(f"unsupported MPMC node_role for close: {role}")

        if not endpoint_items:
            return
        close_errors: List[str] = []
        with round_state.endpoint_close_lock:
            logger.info(
                "🛑 Closing round-owned MPMC %ss to interrupt blocked %s: "
                "reason=%s thread_ids=%s",
                endpoint_label,
                blocked_op_label,
                reason,
                [thread_id for thread_id, _ in endpoint_items],
            )
            for thread_id, endpoint in endpoint_items:
                endpoint_id = id(endpoint)
                if endpoint_id in round_state.closed_endpoint_ids:
                    continue
                close_started = time.monotonic()
                try:
                    close_res = endpoint.close()
                    if close_res.is_ok():
                        close_res.unwrap()
                    else:
                        close_error = close_res.unwrap_error()
                        close_errors.append(
                            f"thread_id={thread_id} err={close_error}"
                        )
                        continue
                except BaseException as exc:
                    close_errors.append(
                        f"thread_id={thread_id} {type(exc).__name__}: {exc}"
                    )
                    continue

                round_state.closed_endpoint_ids.add(endpoint_id)
                logger.info(
                    "✅ Closed round-owned MPMC %s: thread_id=%s "
                    "elapsed_seconds=%.3f",
                    endpoint_label,
                    thread_id,
                    time.monotonic() - close_started,
                )

        if close_errors:
            raise RuntimeError(
                f"MPMC {endpoint_label} close failed: reason={reason}; "
                + "; ".join(close_errors)
            )

    def _finish_mpmc_round(
        self,
        *,
        round_state: PreparedMPMCRound,
        reason: str,
    ) -> None:
        """Stop a round, join every started worker, then release its endpoints."""
        self._benchmark_stop.set()
        round_state.start_event.set()
        cleanup_errors: List[BaseException] = []

        try:
            self._close_mpmc_round_endpoints(
                round_state=round_state,
                reason=reason,
            )
        except BaseException as exc:
            cleanup_errors.append(exc)

        for thread_id, thread in sorted(round_state.pending_threads.items()):
            if thread.ident is None:
                continue
            if thread.is_alive():
                logger.info(
                    "⏳ Waiting for MPMC worker exit after endpoint close: "
                    "reason=%s thread_id=%s",
                    reason,
                    thread_id,
                )
            thread.join()

        # A worker may finish endpoint construction while shutdown was being
        # requested. Rescan after every worker has transferred or dropped ownership.
        try:
            self._close_mpmc_round_endpoints(
                round_state=round_state,
                reason=f"{reason}:post_join",
            )
        except BaseException as exc:
            cleanup_errors.append(exc)

        if cleanup_errors:
            details = "; ".join(
                f"{type(exc).__name__}: {exc}" for exc in cleanup_errors
            )
            raise RuntimeError(
                f"MPMC round cleanup failed: reason={reason}; {details}"
            ) from cleanup_errors[0]

    def _collect_finished_mpmc_workers(
        self,
        *,
        pending_threads: Dict[int, threading.Thread],
        worker_results: Dict[int, List[OperationResult]],
        worker_results_lock: threading.Lock,
        completed: int,
        total_workers: int,
    ) -> int:
        """Harvest finished MPMC workers into self.operation_results."""
        finished_worker_ids = [
            thread_id
            for thread_id, thread in pending_threads.items()
            if thread.ident is not None and not thread.is_alive()
        ]
        for thread_id in finished_worker_ids:
            pending_threads.pop(thread_id, None)
            with worker_results_lock:
                self.operation_results.extend(worker_results.pop(thread_id, []))
            completed += 1
            logger.info(f"✅ 已完成 {completed}/{total_workers} 个线程")
        return completed

    def _collect_finished_kv_workers(
        self,
        *,
        pending_threads: Dict[int, threading.Thread],
        worker_results: Dict[int, List[OperationResult]],
        worker_results_lock: threading.Lock,
        completed: int,
        total_workers: int,
    ) -> int:
        """Harvest finished KV workers into self.operation_results.

        English note:
        - KV mode now uses explicit thread ownership instead of relying on
          ThreadPoolExecutor/as_completed forever.
        - This keeps the benchmark main thread in control even when one worker
          is blocked inside a synchronous Fluxon put/get call.
        """
        finished_worker_ids = [
            thread_id
            for thread_id, thread in pending_threads.items()
            if not thread.is_alive()
        ]
        for thread_id in finished_worker_ids:
            pending_threads.pop(thread_id, None)
            with worker_results_lock:
                self.operation_results.extend(worker_results.pop(thread_id, []))
            completed += 1
            logger.info(f"✅ 已完成 {completed}/{total_workers} 个线程")
        return completed

    def register_and_get_test_config(self) -> bool:
        """
        Register to coordinator and fetch test config.
        """
        logger.info(f"📝 向协调者注册节点: {self.node_id}")

        if not self.instance_key:
            logger.error(
                "❌ 缺少必需的实例标识 --instance-key\n示例: python3 fluxon_test_stack/distributed_benchmark_node.py --instance-key bench-node-0 --coordinator 127.0.0.1:7777"
            )
            return False

        register_message = {
            "type": MsgType.REGISTER.value,  # Register message type
            "node_id": self.node_id,  # Node ID
            "node_type": "benchmark_node",  # Node type: benchmark_node or coordinator
            "timestamp": time.time(),  # Current timestamp
            "instance_key": self.instance_key,
        }

        try:
            response = self._send_rpc_with_retry(
                rpc_name="REGISTER",
                message_factory=lambda: {
                    **register_message,
                    "timestamp": time.time(),
                },
                success_statuses=("success",),
                request_timeout_seconds=REGISTER_RPC_TIMEOUT_SECONDS,
                retry_deadline_seconds=REGISTER_RPC_RETRY_DEADLINE_SECONDS,
            )
            if response is None:
                return False

            if response.get("status") == "success":
                self.test_config = response.get("config")
                _normalize_kv_node_role_in_test_config(self.test_config)
                self.key_prefix = self.test_config.get("key_prefix")
                logger.info(f"获取到prefix: {self.key_prefix}")
                logger.info("✅ 注册成功，获取到测试配置2")
                logger.info("✅ 注册成功，获取到测试配置2")
                logger.debug(
                    f"📋 测试配置详情: {json.dumps(self.test_config, indent=2, ensure_ascii=False)}"
                )

                # Parse MQ config (optional)
                mq_cfg = self.test_config.get("mq") if isinstance(self.test_config, dict) else None
                if isinstance(mq_cfg, dict):
                    apply_mq_config_from_test_config(self.mq_state, mq_cfg, CHAN_CONFIG)
                    self.chan_config = dict(self.mq_state.chan_config)

                mq_unique_id_raw = self.test_config.get("mq_new_or_bind_unique_key")
                if self.test_config.get("test_mode") == TestMode.MPMC.value:
                    if not isinstance(mq_unique_id_raw, str) or not mq_unique_id_raw.strip():
                        logger.error("❌ MPMC 缺少 mq_new_or_bind_unique_key")
                        return False
                    self.mq_unique_id = mq_unique_id_raw.strip()

                # Parse optional simulated consumer handling time range (milliseconds)
                cs_range = (
                    self.test_config.get("consumer_sim_handle_ms_range")
                    if isinstance(self.test_config, dict)
                    else None
                )
                if cs_range is not None:
                    if isinstance(cs_range, (list, tuple)) and len(cs_range) == 2:
                        try:
                            min_ms = int(cs_range[0])
                            max_ms = int(cs_range[1])
                            if min_ms < 0 or max_ms < 0 or max_ms < min_ms:
                                logger.error(
                                    "❌ consumer_sim_handle_ms_range 配置非法，应满足 0 <= min_ms <= max_ms"
                                )
                            else:
                                self.consumer_sim_handle_ms_range = (min_ms, max_ms)
                                logger.info(
                                    "🔧 consumer_sim_handle_ms_range: [%d, %d] ms",
                                    min_ms,
                                    max_ms,
                                )
                        except Exception as exc:  # noqa: BLE001
                            logger.error(
                                "❌ consumer_sim_handle_ms_range 解析失败: %s (%s)",
                                cs_range,
                                exc,
                            )
                    else:
                        logger.error(
                            "❌ consumer_sim_handle_ms_range 配置格式错误，应为 [min_ms, max_ms]，实际为: %s",
                            cs_range,
                        )

                # Validate config completeness
                if self.test_config is not None:
                    required_fields = [
                        "node_role",
                        "threads_per_process",
                        "max_benchmark_seconds",
                        "cluster_ready_timeout_seconds",
                        "metric_warmup_seconds",
                        "start_idle_seconds",
                        "value_size_mode",
                        "kvcache_config",
                    ]
                    missing_fields = [
                        field
                        for field in required_fields
                        if field not in self.test_config
                    ]

                    if missing_fields:
                        logger.error(f"❌ 测试配置缺少必要字段: {missing_fields}")
                        return False

                    global METRIC_WARMUP_SECONDS
                    warmup_secs = float(self.test_config["metric_warmup_seconds"])
                    max_secs = int(self.test_config["max_benchmark_seconds"])
                    if warmup_secs < 0:
                        logger.error(
                            f"❌ metric_warmup_seconds must be >= 0, got: {warmup_secs}"
                        )
                        return False
                    if float(max_secs) - warmup_secs < MIN_EFFECTIVE_BENCHMARK_SECONDS:
                        logger.error(
                            "❌ Invalid benchmark durations: "
                            f"max_benchmark_seconds({max_secs}) - metric_warmup_seconds({warmup_secs}) "
                            f"< {int(MIN_EFFECTIVE_BENCHMARK_SECONDS)}"
                        )
                        return False
                    METRIC_WARMUP_SECONDS = warmup_secs

                    start_idle_secs = float(self.test_config["start_idle_seconds"])
                    if start_idle_secs < 0:
                        logger.error(
                            "❌ start_idle_seconds must be >= 0, got: %s",
                            start_idle_secs,
                        )
                        return False

                    cluster_ready_timeout_s = float(self.test_config["cluster_ready_timeout_seconds"])
                    if cluster_ready_timeout_s <= 0:
                        logger.error(
                            "❌ cluster_ready_timeout_seconds must be > 0, got: %s",
                            cluster_ready_timeout_s,
                        )
                        return False

                    if self.test_config["value_size_mode"] == ValueSizeMode.FIXED.value:
                        if "value_size" not in self.test_config:
                            logger.error("❌ FIXED value_size_mode requires value_size")
                            return False
                    elif self.test_config["value_size_mode"] == ValueSizeMode.RANDOM_WEIGHTED_SET.value:
                        if "value_size_weighted_set" not in self.test_config:
                            logger.error("❌ RANDOM_WEIGHTED_SET requires value_size_weighted_set")
                            return False
                    else:
                        logger.error(
                            "❌ unsupported value_size_mode: %s",
                            self.test_config["value_size_mode"],
                        )
                        return False

                    # Update node_id (if coordinator assigned a new id)
                    if "node_id" in self.test_config:
                        old_id = self.node_id
                        self.node_id = self.test_config["node_id"]
                        logger.info(f"🔄 节点ID已更新: {old_id} -> {self.node_id}")

                    if not self._refresh_value_size_strategy():
                        return False

                return True
            else:
                error_msg = response.get("error", "未知错误") if response else "无响应"
                logger.error(f"❌ 注册失败: {error_msg}")
                return False

        except Exception as e:
            logger.error(f"💥 注册请求失败: {e}")
            return False

    def initialize_from_test_config(self) -> bool:
        """Initialize node from the test config."""
        if not self.test_config:
            logger.error("❌ 无法初始化：测试配置为空")
            return False

        logger.info(f"🚀 开始初始化节点，角色: {self.test_config['node_role']}")

        try:
            test_mode = self.test_config.get("test_mode", "KVSTORE")
            node_role = self.test_config["node_role"]
            defer_shared_kv_store = (
                test_mode == TestMode.MPMC.value and node_role == "producer"
            )
            if defer_shared_kv_store:
                self.kv_store = None
                self.fluxon_client = None
                logger.info(
                    "⏭️ MPMC producer skips shared KVCache init; "
                    "the process-shared runtime is prepared after START"
                )
            else:
                # 1) Initialize KVCache store
                kvcache_config = self.test_config["kvcache_config"]
                logger.debug(f"🔧 KVCache配置: {kvcache_config}")
                logger.info("🔧 正在创建KVCache存储实例...")
                self._sleep_for_runtime_init_stagger()
                # KV store initialization is needed only once. A previous merge caused duplicate calls,
                # leading to repeated cluster member registration.
                store, err = self._init_kv_store_with_ready_retry(kvcache_config)
                if err is not None:
                    logger.error(f"❌ KVCache存储实例创建失败: {err}")
                    return False
                self.kv_store = store
                self.fluxon_client = (
                    store.kv_client if isinstance(store, FluxonBlockingStore) else None
                )
                self._attach_fluxon_phase_summary_callback(self.kv_store)
                logger.info("✅ KVCache存储实例创建成功")

            # 2) Initialize MPMC components based on test mode
            if test_mode == TestMode.MPMC.value:
                if not defer_shared_kv_store and self.fluxon_client is None:
                    raise RuntimeError("MPMC requires a FluxonBlockingStore-backed KvClient")
                logger.info("🔧 MPMC模式，初始化 MPMC 相关配置（每线程独立实例）...")

                node_role = (self.mq_state.role or node_role) if self.mq_state else node_role
                if node_role == "producer":
                    producer_runtime_mode = self._mpmc_producer_runtime_mode()
                    logger.info(
                        "🔧 MPMC producer runtime mode: %s",
                        producer_runtime_mode,
                    )
                # Do not create Producer/Consumer instances here; each worker thread initializes them in _run_worker_thread.
            else:
                logger.info("🔧 KVSTORE/RPC模式，只使用KVCache存储")
                if node_role not in [KV_NODE_ROLE_SEED, KV_NODE_ROLE_WORKER]:
                    logger.error(
                        f"❌ KVSTORE/RPC模式下不支持的角色: {node_role}，只支持 {KV_NODE_ROLE_SEED} 和 {KV_NODE_ROLE_WORKER}"
                    )
                    return False

            # Do not spend fixed idle time here. Any post-start stabilization should
            # be expressed via metric_warmup_seconds so requests can already flow and
            # lazy transport state (for example open_segment/NIXL peer setup) can warm up.
            logger.info("⏭️ 跳过固定初始化等待，交由 metric_warmup_seconds 统一处理性能预热")

            # Log config summary
            logger.info("📊 初始化完成摘要:")
            logger.info(f"   - 测试模式: {test_mode}")
            logger.info(f"   - 节点角色: {self.test_config['node_role']}")
            logger.info(f"   - 每进程线程数: {self.test_config['threads_per_process']}")
            logger.info(f"   - 运行时长: {int(self.test_config['max_benchmark_seconds'])} 秒/节点")
            logger.info(f"   - Warmup: {METRIC_WARMUP_SECONDS} 秒")
            logger.info(f"   - 数据大小策略: {self._describe_value_size_strategy()}")
            # Total operations will be computed after the run completes.

            return True

        except Exception as e:
            logger.error(f"💥 初始化失败: {e}")
            logger.debug("📍 异常详情:", exc_info=True)
            return False

    def _prepare_runtime_before_ready(self) -> None:
        if self.test_config is None:
            return
        if prepare_fs_before_ready(self):
            return
        if prepare_rpc_before_ready(self):
            return
        prepare_kv_before_ready(self, logger=logger)

    def report_ready_to_coordinator(self) -> bool:
        """Report ready status to the coordinator."""
        if self.test_config is None:
            logger.error("❌ 测试配置为空")
            return False

        try:
            self._prepare_runtime_before_ready()
        except Exception as exc:
            logger.error("💥 READY 前运行时准备失败: %s", exc)
            logger.debug("📍 异常详情:", exc_info=True)
            return False

        logger.info("📢 向协调者报告节点准备就绪")

        ready_message = {
            "type": MsgType.READY.value,  # Ready report message type
            "node_id": self.node_id,  # Node ID
            "status": "ready",  # Node status
            "timestamp": time.time(),  # Current timestamp
            "config_summary": {
                "role": self.test_config[
                    "node_role"
                ],  # Node role: seed/worker/consumer/producer
                "threads_per_process": self.test_config["threads_per_process"],
                "max_benchmark_seconds": int(
                    self.test_config["max_benchmark_seconds"]
                ),  # Per-node runtime
                "metric_warmup_seconds": float(self.test_config["metric_warmup_seconds"]),
                "start_idle_seconds": float(self.test_config["start_idle_seconds"]),
                "value_size_mode": self.test_config["value_size_mode"],
                "value_size": self.test_config.get("value_size"),
                "value_size_weighted_set": self.test_config.get("value_size_weighted_set"),
            },
        }

        try:
            response = self._send_rpc_with_retry(
                rpc_name="READY",
                message_factory=lambda: {
                    **ready_message,
                    "timestamp": time.time(),
                },
                success_statuses=("success", "acknowledged"),
                request_timeout_seconds=READY_RPC_TIMEOUT_SECONDS,
                retry_deadline_seconds=self._resolve_ready_rpc_retry_deadline_seconds(),
            )
            if response is None:
                return False

            if response and response.get("status") in ["success", "acknowledged"]:
                logger.info("✅ 成功报告就绪状态")
                return True
            else:
                error_msg = response.get("error", "未知错误") if response else "无响应"
                logger.error(f"❌ 报告就绪失败: {error_msg}")
                return False

        except Exception as e:
            logger.error(f"💥 报告就绪请求失败: {e}")
            return False

    def _generate_test_data(self, size: int) -> bytes:
        """Generate test data of the requested size (KV mode only)."""
        size_int = int(size)
        payload_pool = self._payload_pool_by_size.get(size_int)
        if payload_pool:
            if len(payload_pool) == 1:
                return payload_pool[0]
            return payload_pool[random.randrange(len(payload_pool))]
        return os.urandom(size_int)

    def _calculate_benchmark_results(self) -> Dict[str, Any]:
        """Compute benchmark results."""
        def _empty_results() -> Dict[str, Any]:
            return {
                "node_id": self.node_id,
                "node_role": (
                    self.test_config["node_role"] if self.test_config else "unknown"
                ),
                "total_operations": 0,
                "successful_operations": 0,
                "failed_operations": 0,
                "get_total_operations": 0,
                "get_hit_operations": 0,
                "get_miss_operations": 0,
                "get_error_operations": 0,
                "total_duration_seconds": 0,
                "avg_latency_us": 0,
                "p50_latency_us": 0,
                "p95_latency_us": 0,
                "p99_latency_us": 0,
                "throughput_ops_per_sec": 0,
                "total_throughput_ops_per_sec": 0,
                "get_total_throughput_ops_per_sec": 0,
                "get_hit_throughput_ops_per_sec": 0,
                "get_miss_throughput_ops_per_sec": 0,
                "total_bytes_processed": 0,
                "inflight_max": 0,
                "inflight_avg": 0.0,
                "observed_value_size_histogram": {},
                "observed_value_size_avg": 0.0,
                "observed_value_size_min": 0,
                "observed_value_size_max": 0,
                "error_details": {},
                "test_config": self.test_config,
                "top_slowest_operations": [],
                "fluxon_phase_summary": {},
                "network_bandwidth": self._network_bandwidth_payload(),
                "tcp_thread_transport_summary": {},
                "p2p_receive_transport_summary": {},
                "p2p_rpc_completion_summary": {},
            }

        if not self.operation_results or self.test_config is None:
            return _empty_results()

        # Filter by time window: only count operations finished in [start+warmup, end_time).
        # Metrics are cut off before close(): requests completed after close() are excluded.
        # Reuse self.end_time as window end:
        # - KV mode: end_time = deadline_ts
        # - MPMC mode: end_time = the time close() is actually triggered
        if self.start_time is None or self.end_time is None:
            # Logic error: run_benchmark must set start_time and end_time.
            # Return empty stats to surface the bug to the upper layer.
            return _empty_results()

        warmup_deadline_ts = self.start_time + METRIC_WARMUP_SECONDS
        cutoff_ts = self.end_time
        filtered_results = [
            r
            for r in self.operation_results
            if isinstance(r, OperationResult)
            and r.finish_ts != 0.0
            and r.finish_ts >= warmup_deadline_ts
            and r.finish_ts < cutoff_ts
        ]

        # Split successful and failed operations
        successful_ops = [r for r in filtered_results if r.success]
        failed_ops = [r for r in filtered_results if not r.success]
        get_ops = [r for r in filtered_results if r.operation_type == KV_OPERATION_GET]
        get_hit_ops = [
            r for r in get_ops if r.outcome_kind == OperationOutcome.CACHE_HIT
        ]
        get_miss_ops = [
            r for r in get_ops if r.outcome_kind == OperationOutcome.CACHE_MISS
        ]
        get_error_ops = [
            r for r in get_ops if r.outcome_kind == OperationOutcome.ERROR
        ]

        # Effective duration: exclude warmup; cut off at end_time
        effective_start = self.start_time + METRIC_WARMUP_SECONDS
        if effective_start >= cutoff_ts:
            duration = 0
        else:
            duration = cutoff_ts - effective_start

        # Compute latency stats (successful operations only)
        latencies = [r.latency_us for r in successful_ops] if successful_ops else []

        # Use trimmed mean: sort ascending and trim tail samples before averaging

        if latencies:
            sorted_latencies = sorted(latencies)
            n = len(sorted_latencies)
            # Trim tail 10% (keep at least one sample)
            trim_count = int(n * 0.10)
            if trim_count >= n:
                trim_count = n - 1
            trimmed = (
                sorted_latencies[: n - trim_count] if trim_count > 0 else sorted_latencies
            )
            avg_latency = statistics.mean(trimmed) if trimmed else 0
            p50_latency = statistics.median(sorted_latencies)

            # Percentiles are computed on all successful samples (p95/p99).
            p95_index = int(n * 0.95)
            p99_index = int(n * 0.99)
            p95_latency = sorted_latencies[min(p95_index, n - 1)]
            p99_latency = sorted_latencies[min(p99_index, n - 1)]
        else:
            avg_latency = 0
            p50_latency = 0
            p95_latency = 0
            p99_latency = 0

        # Compute throughput
        throughput = len(successful_ops) / duration if duration > 0 else 0
        total_throughput = len(filtered_results) / duration if duration > 0 else 0
        get_total_throughput = len(get_ops) / duration if duration > 0 else 0
        get_hit_throughput = len(get_hit_ops) / duration if duration > 0 else 0
        get_miss_throughput = len(get_miss_ops) / duration if duration > 0 else 0

        # Compute total bytes
        total_bytes = sum(r.data_size for r in successful_ops)
        observed_size_histogram: Dict[str, int] = {}
        observed_size_values = [r.data_size for r in successful_ops if int(r.data_size) > 0]
        for data_size in observed_size_values:
            size_key = str(int(data_size))
            observed_size_histogram[size_key] = observed_size_histogram.get(size_key, 0) + 1

        inflight_values = [r.inflight_at_start for r in filtered_results]
        inflight_max = max(inflight_values) if inflight_values else 0
        inflight_avg = statistics.mean(inflight_values) if inflight_values else 0.0

        # Aggregate error details
        error_details = {}
        truncated_error_count = 0
        for failed_op in failed_ops:
            error_msg = failed_op.error_msg or "Unknown error"
            error_label = _compact_error_detail_label(error_msg)
            if error_label in error_details or len(error_details) < ERROR_DETAILS_MAX_UNIQUE_KEYS:
                error_details[error_label] = error_details.get(error_label, 0) + 1
            else:
                truncated_error_count += 1
        if truncated_error_count > 0:
            error_details[ERROR_DETAILS_OTHER_BUCKET] = truncated_error_count

        # Top-N slowest latencies (useful for locating slow ops by node/worker)
        top_n = 20
        top_slowest_ops: List[OperationResult] = []
        if successful_ops:
            # Sort by latency desc and take top_n
            top_slowest_ops = sorted(
                successful_ops, key=lambda r: r.latency_us, reverse=True
            )[:top_n]

        top_slowest_serialized = [
            {
                "rank": idx + 1,
                "latency_us": op.latency_us,
                "operation_type": op.operation_type,
                "outcome_kind": op.outcome_kind.value,
                "key": op.key,
                "data_size": op.data_size,
                "node_id": op.node_id or self.node_id,
                "worker_id": op.worker_id,
            }
            for idx, op in enumerate(top_slowest_ops)
        ]

        if top_slowest_ops:
            logger.info(
                "📉 Top %d 慢操作延迟 (按 latency_us 降序):",
                len(top_slowest_ops),
            )
            for idx, op in enumerate(top_slowest_ops, start=1):
                logger.info(
                    "   #%02d node=%s worker=%s latency_us=%.0f op=%s key=%s size=%d",
                    idx,
                    op.node_id or self.node_id,
                    str(op.worker_id) if op.worker_id is not None else "-",
                    op.latency_us,
                    op.operation_type,
                    op.key,
                    op.data_size,
                )

        fluxon_phase_summary: Dict[str, Any] = {}
        phase_summary_store = getattr(self, "_fluxon_rpc_store", None)
        if phase_summary_store is None:
            phase_summary_store = self.kv_store
        if phase_summary_store is not None and hasattr(phase_summary_store, "phase_summary"):
            try:
                raw_phase_summary = phase_summary_store.phase_summary()
                if isinstance(raw_phase_summary, dict):
                    fluxon_phase_summary = raw_phase_summary
            except Exception as exc:
                logger.warning(f"⚠️ 收集 fluxon_phase_summary 失败: {exc}")
        tcp_thread_transport_summary = self._tcp_thread_transport_summary()
        p2p_receive_transport_summary = self._p2p_receive_transport_summary()
        p2p_rpc_completion_summary = self._p2p_rpc_completion_summary(
            fluxon_phase_summary=fluxon_phase_summary,
            duration_seconds=duration,
        )

        return {
            "node_id": self.node_id,
            "node_role": self.test_config["node_role"],
            "total_operations": len(filtered_results),
            "successful_operations": len(successful_ops),
            "failed_operations": len(failed_ops),
            "get_total_operations": len(get_ops),
            "get_hit_operations": len(get_hit_ops),
            "get_miss_operations": len(get_miss_ops),
            "get_error_operations": len(get_error_ops),
            "total_duration_seconds": duration,
            "avg_latency_us": avg_latency,
            "p50_latency_us": p50_latency,
            "p95_latency_us": p95_latency,
            "p99_latency_us": p99_latency,
            "throughput_ops_per_sec": throughput,
            "total_throughput_ops_per_sec": total_throughput,
            "get_total_throughput_ops_per_sec": get_total_throughput,
            "get_hit_throughput_ops_per_sec": get_hit_throughput,
            "get_miss_throughput_ops_per_sec": get_miss_throughput,
            "total_bytes_processed": total_bytes,
            "inflight_max": inflight_max,
            "inflight_avg": inflight_avg,
            "observed_value_size_histogram": observed_size_histogram,
            "observed_value_size_avg": (float(total_bytes) / float(len(successful_ops))) if successful_ops else 0.0,
            "observed_value_size_min": min(observed_size_values) if observed_size_values else 0,
            "observed_value_size_max": max(observed_size_values) if observed_size_values else 0,
            "error_details": error_details,
            "test_config": self.test_config,
            # Include Top-N slowest operations for easy inspection (single-node or aggregated).
            "top_slowest_operations": top_slowest_serialized,
            "fluxon_phase_summary": fluxon_phase_summary,
            "network_bandwidth": self._network_bandwidth_payload(),
            "tcp_thread_transport_summary": tcp_thread_transport_summary,
            "p2p_receive_transport_summary": p2p_receive_transport_summary,
            "p2p_rpc_completion_summary": p2p_rpc_completion_summary,
        }

    def report_results(self, results: Dict[str, Any]) -> bool:
        """Report test results to the coordinator."""
        logger.info("📊 向协调者上报测试结果")

        try:
            result_message = {
                "type": MsgType.RESULT.value,  # Result report message type
                "node_id": self.node_id,  # Node ID
                "timestamp": time.time(),  # Current timestamp
                "results": results,  # Result payload
            }

            logger.debug(
                f"📤 上报结果数据大小: {len(json.dumps(result_message))} bytes"
            )
            response = self.send_rpc_message(
                self.coordinator_host, self.coordinator_port, result_message, timeout=120
            )

            if response and response.get("status") == "success":
                logger.info("✅ 测试结果上报成功")
                logger.debug(f"📨 协调者响应: {response}")
                return True
            else:
                error_msg = response.get("error", "未知错误") if response else "无响应"
                logger.error(f"❌ 结果上报失败: {error_msg}")
                return False

        except Exception as e:
            logger.error(f"💥 上报结果请求失败: {e}")
            return False

    def wait_for_round_gate(self) -> bool:
        """Poll the coordinator until the current round reaches a terminal state."""
        if not self.test_config:
            logger.error("❌ 无法等待 round gate：test_config 不存在")
            return False
        test_id_raw = self.test_config.get("test_id")
        if not isinstance(test_id_raw, str) or not test_id_raw.strip():
            logger.error("❌ 无法等待 round gate：test_id 缺失")
            return False
        test_id = test_id_raw.strip()
        request = {
            "type": MsgType.ROUND_STATUS.value,
            "node_id": self.node_id,
            "test_id": test_id,
            "timestamp": time.time(),
        }
        logger.info(
            "⏳ 等待 coordinator round gate: test_id=%s poll_interval_s=%.1f",
            test_id,
            ROUND_GATE_POLL_INTERVAL_SECONDS,
        )
        while True:
            response = self.send_rpc_message(
                self.coordinator_host,
                self.coordinator_port,
                request,
                timeout=30,
            )
            if not isinstance(response, dict):
                logger.warning("⚠️ round gate 无响应，将重试")
                time.sleep(ROUND_GATE_POLL_INTERVAL_SECONDS)
                continue
            status = response.get("status")
            if status == "completed":
                logger.info(
                    "✅ round gate completed: test_id=%s reported=%s/%s",
                    test_id,
                    response.get("reported_result_node_count"),
                    response.get("expected_nodes"),
                )
                return True
            if status == "failed":
                logger.error(
                    "❌ round gate failed: test_id=%s reported=%s/%s completion_error=%s",
                    test_id,
                    response.get("reported_result_node_count"),
                    response.get("expected_nodes"),
                    response.get("completion_error"),
                )
                return False
            if status == "waiting":
                logger.info(
                    "⏳ round gate waiting: test_id=%s reported=%s/%s",
                    test_id,
                    response.get("reported_result_node_count"),
                    response.get("expected_nodes"),
                )
                time.sleep(ROUND_GATE_POLL_INTERVAL_SECONDS)
                continue
            error_msg = response.get("error", f"unexpected status={status!r}")
            logger.error("❌ round gate 查询失败: %s", error_msg)
            time.sleep(ROUND_GATE_POLL_INTERVAL_SECONDS)

    def send_rpc_message(
        self, ip: str, port: int, data: Dict, timeout: int = 5
    ) -> Dict:
        """Send a JSON message to the TCP server and receive a response.

        The connection is closed immediately after the request/response round-trip.
        """
        try:
            connect_timeout = max(float(timeout), 1.0)
            with socket.create_connection((ip, port), timeout=connect_timeout) as sock:
                sock.settimeout(timeout)
                # Send JSON payload with a 4-byte length header
                message = json.dumps(data).encode()
                msg_len = len(message)
                header = struct.pack("!I", msg_len)
                sock.sendall(header + message)

                # Receive response: first read 4-byte length header
                resp_header = b""
                while len(resp_header) < 4:
                    chunk = sock.recv(4 - len(resp_header))
                    if not chunk:
                        raise RuntimeError("连接关闭，未收到完整长度头")
                    resp_header += chunk
                resp_len = struct.unpack("!I", resp_header)[0]
                # Then read resp_len bytes
                resp_body = b""
                while len(resp_body) < resp_len:
                    chunk = sock.recv(resp_len - len(resp_body))
                    if not chunk:
                        raise RuntimeError("连接关闭，未收到完整响应体")
                    resp_body += chunk
                return json.loads(resp_body.decode())
        except Exception as e:
            return {"error": str(e)}

    @staticmethod
    def _rpc_error_text(response: Optional[Dict[str, Any]]) -> str:
        if not isinstance(response, dict):
            return "no response"
        error_msg = response.get("error")
        if isinstance(error_msg, str) and error_msg.strip():
            return error_msg.strip()
        status = response.get("status")
        if status is not None:
            return f"unexpected status={status!r}"
        return repr(response)

    def _send_rpc_with_retry(
        self,
        *,
        rpc_name: str,
        message_factory: Callable[[], Dict[str, Any]],
        success_statuses: Tuple[str, ...],
        request_timeout_seconds: float,
        retry_deadline_seconds: float,
    ) -> Optional[Dict[str, Any]]:
        deadline_ts = time.monotonic() + float(retry_deadline_seconds)
        attempt = 0
        last_error = "unknown"
        while True:
            attempt += 1
            message = message_factory()
            logger.debug("📤 发送 %s 请求 attempt=%s: %s", rpc_name, attempt, message)
            response = self.send_rpc_message(
                self.coordinator_host,
                self.coordinator_port,
                message,
                timeout=int(max(1.0, float(request_timeout_seconds))),
            )
            if isinstance(response, dict) and response.get("status") in success_statuses:
                if attempt > 1:
                    logger.info("✅ %s 在重试后成功: attempts=%s", rpc_name, attempt)
                return response

            last_error = self._rpc_error_text(response)
            now = time.monotonic()
            if now >= deadline_ts:
                logger.error(
                    "❌ %s 超过重试截止时间: attempts=%s last_error=%s",
                    rpc_name,
                    attempt,
                    last_error,
                )
                return None

            remaining_s = max(0.0, deadline_ts - now)
            logger.warning(
                "⚠️ %s 请求失败，准备重试: attempt=%s remaining_s=%.1f err=%s",
                rpc_name,
                attempt,
                remaining_s,
                last_error,
            )
            time.sleep(min(COORDINATOR_RPC_RETRY_SLEEP_SECONDS, remaining_s))

    def _resolve_ready_rpc_retry_deadline_seconds(self) -> float:
        if isinstance(self.test_config, dict):
            raw_timeout = self.test_config.get("cluster_ready_timeout_seconds")
            if raw_timeout is not None:
                try:
                    parsed_timeout = float(raw_timeout)
                except (TypeError, ValueError):
                    logger.warning(
                        "⚠️ 无法解析 cluster_ready_timeout_seconds=%r，READY 重试回退到默认值 %.1fs",
                        raw_timeout,
                        READY_RPC_RETRY_MIN_DEADLINE_SECONDS,
                    )
                else:
                    if parsed_timeout > 0.0:
                        return min(
                            READY_RPC_RETRY_MAX_DEADLINE_SECONDS,
                            max(READY_RPC_RETRY_MIN_DEADLINE_SECONDS, parsed_timeout),
                        )
        return READY_RPC_RETRY_MIN_DEADLINE_SECONDS

    def _resolve_start_wait_timeout_seconds(self) -> float:
        if isinstance(self.test_config, dict):
            raw_timeout = self.test_config.get("cluster_ready_timeout_seconds")
            if raw_timeout is not None:
                try:
                    cluster_ready_timeout_s = float(raw_timeout)
                except (TypeError, ValueError):
                    logger.warning(
                        "⚠️ 无法解析 cluster_ready_timeout_seconds=%r，回退到默认 START 等待超时 %.1fs",
                        raw_timeout,
                        DEFAULT_START_WAIT_TIMEOUT_SECONDS,
                    )
                else:
                    if cluster_ready_timeout_s > 0.0:
                        return cluster_ready_timeout_s + START_WAIT_POLL_INTERVAL_SECONDS
        return DEFAULT_START_WAIT_TIMEOUT_SECONDS

    def _start_wait_poll_sleep_seconds(self, *, attempt: int, remaining_seconds: float) -> float:
        base_s = min(
            START_WAIT_POLL_MAX_SECONDS,
            START_WAIT_POLL_INTERVAL_SECONDS
            * (START_WAIT_POLL_BACKOFF_FACTOR ** max(0, attempt - 1)),
        )
        key = f"{self.instance_key or self.node_id}:start-wait:{attempt}"
        jitter_s = self._stable_fraction_from_text(key) * START_WAIT_POLL_JITTER_SECONDS
        return min(max(0.0, remaining_seconds), base_s + jitter_s)

    def _wait_for_mpmc_runtime_start(self) -> bool:
        """Synchronize timed MPMC workers after every node has opened its runtime endpoints."""
        if not isinstance(self.test_config, dict):
            logger.error("❌ MPMC runtime_start requires test_config")
            return False
        node_id = self.node_id
        runtime_ready_message = {
            "type": MsgType.RUNTIME_READY.value,
            "node_id": node_id,
            "timestamp": time.time(),
        }
        try:
            ready_response = self._send_rpc_with_retry(
                rpc_name="RUNTIME_READY",
                message_factory=lambda: {
                    **runtime_ready_message,
                    "timestamp": time.time(),
                },
                success_statuses=("success", "waiting"),
                request_timeout_seconds=READY_RPC_TIMEOUT_SECONDS,
                retry_deadline_seconds=self._resolve_ready_rpc_retry_deadline_seconds(),
            )
        except Exception as exc:
            logger.error("💥 MPMC runtime_ready 请求失败: %s", exc)
            return False
        if ready_response is None:
            return False

        wait_timeout_s = self._resolve_start_wait_timeout_seconds()
        wait_deadline = time.monotonic() + wait_timeout_s
        runtime_start_message = {
            "type": MsgType.RUNTIME_START.value,
            "node_id": node_id,
            "timestamp": time.time(),
        }
        logger.info(
            "⏳ 等待 MPMC runtime_start: timeout_s=%.1f initial_poll_s=%.1f max_poll_s=%.1f",
            wait_timeout_s,
            START_WAIT_POLL_INTERVAL_SECONDS,
            START_WAIT_POLL_MAX_SECONDS,
        )
        wait_attempt = 0
        while True:
            remaining_s = wait_deadline - time.monotonic()
            if remaining_s <= 0.0:
                logger.error("❌ 等待 MPMC runtime_start 超时: timeout_s=%.1f", wait_timeout_s)
                return False
            response = self.send_rpc_message(
                self.coordinator_host,
                self.coordinator_port,
                {
                    **runtime_start_message,
                    "timestamp": time.time(),
                },
                timeout=max(5.0, min(120.0, remaining_s + START_WAIT_POLL_INTERVAL_SECONDS)),
            )
            status = response.get("status") if isinstance(response, dict) else None
            if status == "success":
                logger.info(
                    "✅ MPMC runtime_start released: runtime_ready=%s/%s",
                    response.get("runtime_ready_count"),
                    response.get("expected_nodes"),
                )
                return True
            wait_attempt += 1
            time.sleep(
                self._start_wait_poll_sleep_seconds(
                    attempt=wait_attempt,
                    remaining_seconds=wait_deadline - time.monotonic(),
                )
            )

    def wait_for_start(self) -> bool:
        """
        等待协调者发出开始信号。

        同时从 START 响应中接收本轮测试的覆盖配置（workers/value_size 等）
        以及 has_more_tests 标志，用于支持多轮 benchmark。
        """
        start_request = {
            "type": MsgType.START.value,  # Start request
            "node_id": self.node_id,  # Node ID
            "timestamp": time.time(),  # Current timestamp
        }
        wait_timeout_s = self._resolve_start_wait_timeout_seconds()
        wait_deadline = time.monotonic() + wait_timeout_s
        try:
            logger.info(
                "⏳ 等待协调者 START: timeout_s=%.1f initial_poll_s=%.1f max_poll_s=%.1f",
                wait_timeout_s,
                START_WAIT_POLL_INTERVAL_SECONDS,
                START_WAIT_POLL_MAX_SECONDS,
            )
            wait_attempt = 0
            while True:
                remaining_s = wait_deadline - time.monotonic()
                if remaining_s <= 0.0:
                    break
                resp = self.send_rpc_message(
                    self.coordinator_host,
                    self.coordinator_port,
                    {
                        **start_request,
                        "timestamp": time.time(),
                    },
                    timeout=max(5.0, min(120.0, remaining_s + START_WAIT_POLL_INTERVAL_SECONDS)),
                )
                status = resp.get("status") if resp else None
                if status == "success":
                    # 协调者在 START 响应中下发本轮的覆盖配置
                    overrides = resp.get("config_overrides") if isinstance(resp, dict) else None
                    if overrides and isinstance(overrides, dict):
                        if not self.test_config:
                            logger.error(
                                "❌ 收到开始信号但本地 test_config 为空，无法应用覆盖配置"
                            )
                            return False
                        # 仅更新当前轮次相关字段，其余配置仍沿用初始注册时的值
                        threads_per_process = overrides.get("threads_per_process")
                        max_secs = overrides.get("max_benchmark_seconds")
                        start_idle_secs = overrides.get("start_idle_seconds")
                        value_size_mode = overrides.get("value_size_mode")
                        value_size = overrides.get("value_size")
                        value_size_weighted_set = overrides.get("value_size_weighted_set")
                        test_mode = overrides.get("test_mode")
                        test_id = overrides.get("test_id")

                        if threads_per_process is not None:
                            self.test_config["threads_per_process"] = int(threads_per_process)
                        if max_secs is not None:
                            self.test_config["max_benchmark_seconds"] = int(max_secs)
                        if start_idle_secs is not None:
                            self.test_config["start_idle_seconds"] = float(start_idle_secs)
                        if value_size_mode is not None:
                            self.test_config["value_size_mode"] = str(value_size_mode)
                        if value_size is not None:
                            self.test_config["value_size"] = int(value_size)
                        if value_size_weighted_set is not None:
                            self.test_config["value_size_weighted_set"] = value_size_weighted_set
                        if test_mode is not None:
                            self.test_config["test_mode"] = str(test_mode)
                        if test_id is not None:
                            self.test_config["test_id"] = str(test_id)
                        if not self._refresh_value_size_strategy():
                            return False
                        if self.test_config.get("test_mode") == TestMode.MPMC.value:
                            prepared_round = self._prepared_mpmc_round
                            node_role = str(self.test_config.get("node_role", "")).strip()
                            if node_role == "consumer":
                                if prepared_round is None:
                                    logger.error("❌ MPMC consumer START 收到覆盖配置，但 READY 之前没有 prepared round")
                                    return False
                                prepared_workers = len(prepared_round.pending_threads)
                                if prepared_workers != int(self.test_config["threads_per_process"]):
                                    logger.error(
                                        "❌ START overrides changed MPMC threads_per_process after READY: prepared=%s start_override=%s",
                                        prepared_workers,
                                        self.test_config["threads_per_process"],
                                    )
                                    return False
                            elif node_role == "producer":
                                if prepared_round is not None:
                                    prepared_workers = len(prepared_round.pending_threads)
                                    if prepared_workers != int(self.test_config["threads_per_process"]):
                                        logger.error(
                                            "❌ START overrides changed MPMC producer threads_per_process after READY: prepared=%s start_override=%s",
                                            prepared_workers,
                                            self.test_config["threads_per_process"],
                                        )
                                        return False
                            else:
                                logger.error("❌ MPMC START 收到不支持的 node_role: %s", node_role)
                                return False

                        logger.info(
                            "🔧 本轮测试参数覆盖完成: threads_per_process=%s, value_size_strategy=%s, "
                            "max_benchmark_seconds=%s, test_mode=%s, test_id=%s",
                            self.test_config.get("threads_per_process"),
                            self._describe_value_size_strategy(),
                            self.test_config.get("max_benchmark_seconds"),
                            self.test_config.get("test_mode"),
                            self.test_config.get("test_id"),
                        )

                    # 记录多轮测试标志
                    self.has_more_tests = bool(resp.get("has_more_tests", False))

                    if self.test_config and self.test_config.get("test_mode") == TestMode.MPMC.value:
                        node_role = self.test_config.get("node_role")
                        if node_role == "producer":
                            logger.info("✅ 收到开始信号，MPMC producer 使用非阻塞 prewarm round 进入 benchmark")
                        else:
                            logger.info("✅ 收到开始信号，MPMC round 已完成 prewarm，立即进入 benchmark")
                    else:
                        start_idle_seconds = float(self.test_config.get("start_idle_seconds", 10.0))
                        logger.info(
                            "✅ 收到开始信号，空等 %.1fs 后进入 benchmark；性能预热仍由 metric_warmup_seconds 过滤统计",
                            start_idle_seconds,
                        )
                        if start_idle_seconds > 0:
                            time.sleep(start_idle_seconds)
                        logger.info("✅ 开始基准测试")
                    return True
                else:
                    error_msg = resp.get("status", "waiting") if resp else "无响应"
                    if error_msg == "waiting":
                        logger.debug("等待开始信号: coordinator 仍在等待所有节点就绪")
                    else:
                        logger.error(f"❌ 等待开始信号失败: {error_msg}")
                remaining_s = wait_deadline - time.monotonic()
                if remaining_s <= 0.0:
                    break
                wait_attempt += 1
                time.sleep(
                    self._start_wait_poll_sleep_seconds(
                        attempt=wait_attempt,
                        remaining_seconds=remaining_s,
                    )
                )
            logger.error(
                "❌ 等待开始信号超时，未收到开始信号: timeout_s=%.1f",
                wait_timeout_s,
            )
            return False
        except Exception as e:
            logger.error(f"💥 等待开始信号请求失败: {e}")
            return False

    def _put_single_operation(
        self, key: str, value: bytes, inflight_at_start: int, *, deadline_ts: float, ctx: str
    ) -> OperationResult:
        """Execute single PUT operation and measure performance."""
        op_start = time.perf_counter()

        try:
            err = kv_put_once(self.kv_store, key, {"payload": value}, deadline_ts=deadline_ts, ctx=ctx)
            op_end = time.perf_counter()

            if err is not None:
                logger.info(f"PUT操作失败: {key}, 错误信息: {err}")
                return OperationResult(
                    success=False,
                    latency_us=(op_end - op_start) * 1000000,
                    operation_type=KV_OPERATION_PUT,
                    key=key,
                    data_size=len(value),
                    inflight_at_start=inflight_at_start,
                    outcome_kind=OperationOutcome.ERROR,
                    error_msg=err,
                )

            return OperationResult(
                success=True,
                latency_us=(op_end - op_start) * 1000000,
                operation_type=KV_OPERATION_PUT,
                key=key,
                data_size=len(value),
                inflight_at_start=inflight_at_start,
                outcome_kind=OperationOutcome.SUCCESS,
                error_msg=None,
            )

        except Exception as e:
            op_end = time.perf_counter()
            return OperationResult(
                success=False,
                latency_us=(op_end - op_start) * 1000000,
                operation_type=KV_OPERATION_PUT,
                key=key,
                data_size=len(value),
                inflight_at_start=inflight_at_start,
                outcome_kind=OperationOutcome.ERROR,
                error_msg=str(e),
            )

    def _get_single_operation(
        self,
        key: str,
        inflight_at_start: int,
        *,
        deadline_ts: float,
        expected_data_size: int,
        ctx: str,
    ) -> OperationResult:
        """Execute single GET operation and measure performance."""
        op_start = time.perf_counter()
        try:
            err = kv_get_once(self.kv_store, key, deadline_ts=deadline_ts, ctx=ctx)
            op_end = time.perf_counter()
            if err is not None:
                get_outcome = classify_kv_get_result(err)
                if get_outcome == KVGetResultKind.CACHE_MISS:
                    outcome_kind = OperationOutcome.CACHE_MISS
                else:
                    outcome_kind = OperationOutcome.ERROR
                return OperationResult(
                    success=False,
                    latency_us=(op_end - op_start) * 1000000,
                    operation_type=KV_OPERATION_GET,
                    key=key,
                    data_size=0,
                    inflight_at_start=inflight_at_start,
                    outcome_kind=outcome_kind,
                    error_msg=err,
                )
            return OperationResult(
                success=True,
                latency_us=(op_end - op_start) * 1000000,
                operation_type=KV_OPERATION_GET,
                key=key,
                data_size=expected_data_size,
                inflight_at_start=inflight_at_start,
                outcome_kind=OperationOutcome.CACHE_HIT,
                error_msg=None,
            )
        except Exception as e:
            op_end = time.perf_counter()
            return OperationResult(
                success=False,
                latency_us=(op_end - op_start) * 1000000,
                operation_type=KV_OPERATION_GET,
                key=key,
                data_size=0,
                inflight_at_start=inflight_at_start,
                outcome_kind=OperationOutcome.ERROR,
                error_msg=f"GET failed: {str(e)}",
            )
    def _execute_chan_put_operation(
        self, producer, value: Dict[str, Any], inflight_at_start: int
    ) -> OperationResult:
        """Execute single MPMC PUT operation and measure performance."""
        op_start = time.perf_counter()
        payload = value.get("payload")
        payload_size = len(payload) if isinstance(payload, bytes) else 0
        try:
            err = mq_put_once(producer, value)
            if err is not None:
                op_end = time.perf_counter()
                return OperationResult(
                    success=False,
                    latency_us=(op_end - op_start) * 1000000,
                    operation_type="MPMC_PUT",
                    key="NO KEY IN CHANNEL",
                    data_size=payload_size,
                    inflight_at_start=inflight_at_start,
                    outcome_kind=OperationOutcome.ERROR,
                    error_msg=err,
                )
            op_end = time.perf_counter()
            return OperationResult(
                success=True,
                latency_us=(op_end - op_start) * 1000000,
                operation_type="MPMC_PUT",
                key="NO KEY IN CHANNEL",
                data_size=payload_size,
                inflight_at_start=inflight_at_start,
                outcome_kind=OperationOutcome.SUCCESS,
                error_msg=None,
            )
        except MQClosedError:
            # Propagate MQClosedError to the upper loop to exit the benchmark.
            raise
        except Exception as e:
            op_end = time.perf_counter()
            return OperationResult(
                success=False,
                latency_us=(op_end - op_start) * 1000000,
                operation_type="MPMC_PUT",
                key="NO KEY IN CHANNEL",
                data_size=payload_size,
                inflight_at_start=inflight_at_start,
                outcome_kind=OperationOutcome.ERROR,
                error_msg=str(e),
            )
    def _execute_chan_get_operation(
        self, consumer, inflight_at_start: int, deadline_ts: float
    ) -> OperationResult:
        """Execute single MPMC GET operation and measure performance."""
        op_start = time.perf_counter()
        result: Optional[OperationResult] = None
        try:
            while True:
                mq_outcome = mq_get_once(consumer, batch_size=1)
                if mq_outcome.status != MQGetStatus.NO_MESSAGE:
                    break

                if self._benchmark_stop.is_set() or time.time() >= deadline_ts:
                    raise BenchmarkWorkerStop(
                        "MPMC consumer observed an empty channel after benchmark stop intent"
                    )
                time.sleep(MPMC_NO_MESSAGE_RETRY_SLEEP_SECONDS)

            op_end = time.perf_counter()
            latency_us = (op_end - op_start) * 1000000
            if not mq_outcome.ok:
                result = OperationResult(
                    success=False,
                    latency_us=latency_us,
                    operation_type="MPMC_GET",
                    key="NO KEY IN CHANNEL",
                    data_size=mq_outcome.data_size,
                    inflight_at_start=inflight_at_start,
                    outcome_kind=OperationOutcome.ERROR,
                    error_msg=mq_outcome.error_msg,
                )
            else:
                result = OperationResult(
                    success=True,
                    latency_us=latency_us,
                    operation_type="MPMC_GET",
                    key="NO KEY IN CHANNEL",
                    data_size=mq_outcome.data_size,
                    inflight_at_start=inflight_at_start,
                    outcome_kind=OperationOutcome.SUCCESS,
                    error_msg=None,
                )
        except BenchmarkWorkerStop:
            raise
        except MQClosedError:
            # Propagate MQClosedError to the upper loop to exit the benchmark.
            raise
        except Exception as e:
            op_end = time.perf_counter()
            result = OperationResult(
                success=False,
                latency_us=(op_end - op_start) * 1000000,
                operation_type="MPMC_GET",
                key="NO KEY IN CHANNEL",
                data_size=0,
                inflight_at_start=inflight_at_start,
                outcome_kind=OperationOutcome.ERROR,
                error_msg=str(e),
            )

        # After MQ get_data, simulate consumer handling time based on config so producers
        # have some time to accumulate messages before the next get_data.
        delay_cfg = self.consumer_sim_handle_ms_range
        if delay_cfg and result and result.success:
            min_ms, max_ms = delay_cfg
            if max_ms > 0 and max_ms >= min_ms >= 0:
                if max_ms == min_ms:
                    delay_ms = float(min_ms)
                else:
                    delay_ms = random.uniform(min_ms, max_ms)
                time.sleep(delay_ms / 1000.0)

        return result
    def _run_worker_thread(
        self,
        thread_id: int,
        deadline_ts: float,
        *,
        prepared_runtime: Optional[PreparedWorkerRuntime] = None,
    ) -> List[OperationResult]:
        """
        Execute operations in a single worker thread (PUT/GET/MPMC, etc.) and return results.
        """
        if self.test_config is None:
            raise RuntimeError("test_config must exist before starting worker threads")

        fs_results = run_fs_worker(
            self,
            thread_id=thread_id,
            deadline_ts=deadline_ts,
            operation_result_cls=OperationResult,
            operation_outcome=OperationOutcome,
            metric_warmup_seconds=METRIC_WARMUP_SECONDS,
            debug_print=_debug_print,
        )
        if fs_results is not None:
            return fs_results

        rpc_results = run_rpc_worker(
            self,
            thread_id=thread_id,
            deadline_ts=deadline_ts,
            operation_result_cls=OperationResult,
            operation_outcome=OperationOutcome,
            metric_warmup_seconds=METRIC_WARMUP_SECONDS,
            debug_print=_debug_print,
        )
        if rpc_results is not None:
            return rpc_results

        kv_results = run_kv_worker(
            self,
            thread_id=thread_id,
            deadline_ts=deadline_ts,
            operation_result_cls=OperationResult,
            operation_outcome=OperationOutcome,
            metric_warmup_seconds=METRIC_WARMUP_SECONDS,
            debug_print=_debug_print,
        )
        if kv_results is not None:
            return kv_results

        results: List[OperationResult] = []
        node_role = self.test_config.get("node_role", "")
        test_mode = self.test_config.get("test_mode", "KVSTORE")
        if test_mode != TestMode.MPMC.value:
            raise RuntimeError(f"未知测试模式: {test_mode}")

        if node_role == "producer":
            _debug_print(
                f"thread {thread_id} start, mode={test_mode}, "
                f"deadline_ts={deadline_ts:.3f}"
            )

        if prepared_runtime is None:
            raise RuntimeError(
                "MPMC worker requires a prepared_runtime from the READY-before-start barrier"
            )
        producer = prepared_runtime.producer
        consumer = prepared_runtime.consumer
        local_mq_state = prepared_runtime.local_mq_state
        if prepared_runtime.logical_only:
            if node_role != "producer":
                raise RuntimeError(
                    f"logical-only MPMC runtime is only valid for producer, got: {node_role}"
                )
            logger.info(
                "⏭️ MPMC logical-only producer worker exits without endpoint operations: thread_id=%s",
                thread_id,
            )
            return []

        op_idx = 0
        try:
            while True:
                if self._benchmark_stop.is_set():
                    _debug_print(
                        f"thread {thread_id} observed benchmark stop intent, op_idx={op_idx}"
                    )
                    break

                inflight_at_start = self._inflight_begin()
                try:
                    value_size = int(self.test_config.get("value_size", 1024))
                    if node_role == "producer":
                        fallback_producer_id = self.instance_key or self.node_id
                        value = build_message(
                            local_mq_state or self.mq_state,
                            value_size,
                            fallback_producer_id=fallback_producer_id,
                        )
                        result = self._execute_chan_put_operation(
                            producer,
                            value,
                            inflight_at_start,
                        )
                    elif node_role == "consumer":
                        result = self._execute_chan_get_operation(
                            consumer,
                            inflight_at_start,
                            deadline_ts,
                        )
                    else:
                        result = OperationResult(
                            success=False,
                            latency_us=0,
                            operation_type="unknown",
                            key="NO KEY ",
                            data_size=0,
                            inflight_at_start=inflight_at_start,
                            outcome_kind=OperationOutcome.ERROR,
                            error_msg=f"不支持的MPMC角色: {node_role}",
                        )
                except (BenchmarkWorkerStop, MQClosedError) as e:
                    _debug_print(
                        f"thread {thread_id} observed worker stop, op_idx={op_idx}, msg={e}"
                    )
                    break
                except Exception as e:
                    result = OperationResult(
                        success=False,
                        latency_us=0,
                        operation_type="exception",
                        key="NO KEY IN CHANNEL",
                        data_size=0,
                        inflight_at_start=inflight_at_start,
                        outcome_kind=OperationOutcome.ERROR,
                        error_msg=str(e),
                    )
                finally:
                    self._inflight_end()

                result.node_id = self.node_id
                result.worker_id = thread_id
                result.finish_ts = time.time()
                op_finish_ts = result.finish_ts
                if self.start_time is not None:
                    warmup_deadline_ts = self.start_time + METRIC_WARMUP_SECONDS
                    if op_finish_ts < warmup_deadline_ts:
                        self._mark_progress(
                            thread_id=thread_id,
                            op_idx=op_idx,
                            finish_ts=op_finish_ts,
                            latency_us=result.latency_us,
                        )
                        op_idx += 1
                        continue

                if result.latency_us > 10_000_000:
                    logger.warning(
                        f"⚠️ 线程 {thread_id} 操作延迟过高: "
                        f"op_idx={op_idx}, latency_us={result.latency_us:.0f}, "
                        f"op_type={result.operation_type}, key={result.key}"
                    )
                print(f"Thread {thread_id} Operation {op_idx}: latency_us {result.latency_us}")
                self._mark_progress(
                    thread_id=thread_id,
                    op_idx=op_idx,
                    finish_ts=op_finish_ts,
                    latency_us=result.latency_us,
                )

                results.append(result)
                op_idx += 1
        finally:
            _debug_print(
                f"thread {thread_id} exit run loop, total_ops={len(results)}, "
                f"last_op_idx={op_idx}"
            )
        return results

    def _set_forced_benchmark_result(
        self,
        *,
        reason: str,
        total_workers: int,
        completed_workers: int,
        timed_out_worker_ids: List[int],
    ) -> None:
        if not self.test_config:
            raise RuntimeError("test_config must exist before forcing benchmark result")
        timed_out_count = len(timed_out_worker_ids)
        if reason.startswith("kv_"):
            error_label = (
                "KV worker exit timeout after benchmark deadline; "
                f"timed_out_workers={timed_out_worker_ids}"
            )
            grace_seconds = self._kv_worker_exit_grace_seconds()
        else:
            error_label = (
                "MPMC worker exit timeout after stop intent; "
                f"timed_out_workers={timed_out_worker_ids}"
            )
            grace_seconds = MPMC_WORKER_EXIT_GRACE_SECONDS
        self._forced_benchmark_result = {
            "node_id": self.node_id,
            "node_role": self.test_config["node_role"],
            "total_operations": 0,
            "successful_operations": 0,
            "failed_operations": timed_out_count if timed_out_count > 0 else total_workers,
            "get_total_operations": 0,
            "get_hit_operations": 0,
            "get_miss_operations": 0,
            "get_error_operations": 0,
            "total_duration_seconds": 0,
            "avg_latency_us": 0,
            "p50_latency_us": 0,
            "p95_latency_us": 0,
            "p99_latency_us": 0,
            "throughput_ops_per_sec": 0,
            "total_throughput_ops_per_sec": 0,
            "get_total_throughput_ops_per_sec": 0,
            "get_hit_throughput_ops_per_sec": 0,
            "get_miss_throughput_ops_per_sec": 0,
            "total_bytes_processed": 0,
            "inflight_max": 0,
            "inflight_avg": 0.0,
            "observed_value_size_histogram": {},
            "observed_value_size_avg": 0.0,
            "observed_value_size_min": 0,
            "observed_value_size_max": 0,
            "error_details": {
                error_label: timed_out_count if timed_out_count > 0 else total_workers,
            },
            "test_config": self.test_config,
            "top_slowest_operations": [],
            "fluxon_phase_summary": {},
            "forced_failure_reason": reason,
            "forced_failure_context": {
                "total_workers": total_workers,
                "completed_workers": completed_workers,
                "timed_out_worker_ids": timed_out_worker_ids,
                "grace_seconds": grace_seconds,
            },
        }

    def _kv_worker_exit_grace_seconds(self) -> float:
        """Use op_timeout_seconds as the authoritative KV worker exit grace.

        English note:
        - A KV worker may legitimately still be inside one blocking Fluxon put/get
          when the benchmark deadline is reached.
        - The benchmark config already declares the maximum tolerated per-op wait
          via op_timeout_seconds, so the shutdown grace should derive from the same
          authority instead of drifting to a separate hard-coded constant.
        """
        if not self.test_config:
            raise RuntimeError("test_config must exist before deriving KV worker grace")
        grace_seconds = float(self.test_config["op_timeout_seconds"])
        if grace_seconds <= 0.0:
            raise ValueError(f"op_timeout_seconds must be > 0, got: {grace_seconds}")
        return grace_seconds

    def _run_kv_workers(self, *, workers: int, deadline_ts: float) -> None:
        """Run KV benchmark workers with explicit deadline/graceful stop control.

        English note:
        - Fluxon sync KV put/get may block indefinitely if the lower layer keeps
          retrying owner/link recovery.
        - We therefore cannot rely on ThreadPoolExecutor/as_completed to finish.
        - The main thread owns the stop policy:
          1. wait until benchmark deadline
          2. set stop intent so no worker starts new ops
          3. wait one per-op grace window
          4. close kv_store to break blocked sync calls
          5. harvest whatever exited; if some threads still remain, emit a forced failure result
        """
        worker_results: Dict[int, List[OperationResult]] = {}
        worker_results_lock = threading.Lock()
        pending_threads: Dict[int, threading.Thread] = {}

        def worker_target(thread_id: int) -> None:
            result_list: List[OperationResult] = []
            try:
                result_list = self._run_worker_thread(thread_id, deadline_ts)
            except Exception as exc:
                logger.error("❌ KV 线程 %s 执行异常: %s", thread_id, exc)
            with worker_results_lock:
                worker_results[thread_id] = result_list

        for thread_id in range(workers):
            thread = threading.Thread(
                target=worker_target,
                args=(thread_id,),
                name=f"bench-kv-worker-{self.node_id}-{thread_id}",
                daemon=False,
            )
            pending_threads[thread_id] = thread
            thread.start()

        completed = 0
        stop_requested = False
        stop_grace_deadline_ts: Optional[float] = None
        kv_worker_grace_seconds = self._kv_worker_exit_grace_seconds()

        while pending_threads:
            now = time.time()
            if not stop_requested and now >= deadline_ts:
                stop_requested = True
                self._benchmark_stop.set()
                stop_grace_deadline_ts = now + kv_worker_grace_seconds
                logger.info(
                    "🛑 benchmark 到达运行时长，主线程发出 KV stop intent; grace_seconds=%.1f",
                    kv_worker_grace_seconds,
                )

            completed = self._collect_finished_kv_workers(
                pending_threads=pending_threads,
                worker_results=worker_results,
                worker_results_lock=worker_results_lock,
                completed=completed,
                total_workers=workers,
            )
            if not pending_threads:
                break

            if stop_requested and stop_grace_deadline_ts is not None and now >= stop_grace_deadline_ts:
                timed_out_worker_ids = sorted(pending_threads.keys())
                logger.error(
                    "❌ 有线程在 KV benchmark deadline 后仍未退出，先关闭 kv_store 解除阻塞，再做最终收束: "
                    f"timed_out_worker_ids={timed_out_worker_ids} grace_seconds={kv_worker_grace_seconds}"
                )
                self._close_kv_store(reason="kv_deadline_timeout")
                abort_deadline_ts = time.time() + KV_WORKER_ABORT_GRACE_SECONDS
                while pending_threads and time.time() < abort_deadline_ts:
                    completed = self._collect_finished_kv_workers(
                        pending_threads=pending_threads,
                        worker_results=worker_results,
                        worker_results_lock=worker_results_lock,
                        completed=completed,
                        total_workers=workers,
                    )
                    if pending_threads:
                        time.sleep(0.2)
                if pending_threads:
                    self._set_forced_benchmark_result(
                        reason="kv_worker_exit_timeout",
                        total_workers=workers,
                        completed_workers=completed,
                        timed_out_worker_ids=sorted(pending_threads.keys()),
                    )
                break

            time.sleep(0.2)

        completed = self._collect_finished_kv_workers(
            pending_threads=pending_threads,
            worker_results=worker_results,
            worker_results_lock=worker_results_lock,
            completed=completed,
            total_workers=workers,
        )
        if pending_threads:
            self._set_forced_benchmark_result(
                reason="kv_worker_exit_timeout",
                total_workers=workers,
                completed_workers=completed,
                timed_out_worker_ids=sorted(pending_threads.keys()),
            )
            logger.error(
                "Waiting for timed-out KV workers before process teardown: thread_ids=%s",
                sorted(pending_threads.keys()),
            )
            for thread in pending_threads.values():
                thread.join()
            self._collect_finished_kv_workers(
                pending_threads=pending_threads,
                worker_results=worker_results,
                worker_results_lock=worker_results_lock,
                completed=completed,
                total_workers=workers,
            )

    def _run_mpmc_workers(self, *, workers: int) -> None:
        round_state = self._consume_prepared_mpmc_round(expected_workers=workers)
        try:
            self._run_mpmc_round(
                round_state=round_state,
                workers=workers,
            )
        finally:
            self._finish_mpmc_round(
                round_state=round_state,
                reason="run_mpmc_workers_exit",
            )

    def _run_mpmc_round(
        self,
        *,
        round_state: PreparedMPMCRound,
        workers: int,
    ) -> None:
        """Run operations while the caller retains round resource ownership."""
        pending_threads = dict(round_state.pending_threads)
        completed = 0
        stop_requested = False
        stop_grace_deadline_ts: Optional[float] = None
        role = self.test_config.get("node_role")

        deferred_thread_ids = [
            thread_id
            for thread_id, thread in sorted(pending_threads.items())
            if thread.ident is None
        ]
        for thread_id in deferred_thread_ids:
            thread = pending_threads[thread_id]
            try:
                logger.info(
                    "▶️ START 后启动 deferred MPMC worker: role=%s thread_id=%s",
                    role,
                    thread_id,
                )
                thread.start()
            except Exception as exc:
                logger.error(
                    "❌ START 后启动 deferred MPMC worker 失败: role=%s thread_id=%s err=%s",
                    role,
                    thread_id,
                    exc,
                )
                with round_state.prepared_lock:
                    round_state.prepare_errors[thread_id] = str(exc)

        if role == "producer" and deferred_thread_ids:
            prepare_deadline_ts = time.monotonic() + float(
                self.test_config["cluster_ready_timeout_seconds"]
            )
            while True:
                with round_state.prepared_lock:
                    prepared_count = len(round_state.ready_thread_ids)
                    prepare_error_snapshot = dict(round_state.prepare_errors)
                    fatal_error_snapshot = dict(round_state.fatal_errors)
                if fatal_error_snapshot:
                    self._raise_mpmc_fatal_worker_error(
                        round_state=round_state,
                        fatal_errors=fatal_error_snapshot,
                        stage="producer_prepare_after_start",
                    )
                if prepare_error_snapshot:
                    self._set_forced_benchmark_result(
                        reason="mpmc_worker_prepare_failed",
                        total_workers=workers,
                        completed_workers=0,
                        timed_out_worker_ids=sorted(prepare_error_snapshot),
                    )
                    self._benchmark_stop.set()
                    round_state.start_event.set()
                    return
                if prepared_count == workers:
                    logger.info(
                        "✅ MPMC producer workers prepared after START: prepared=%s/%s",
                        prepared_count,
                        workers,
                    )
                    break
                if time.monotonic() >= prepare_deadline_ts:
                    missing_worker_ids = sorted(
                        set(range(workers)) - set(round_state.prepared_runtimes)
                    )
                    self._set_forced_benchmark_result(
                        reason="mpmc_worker_prepare_timeout",
                        total_workers=workers,
                        completed_workers=prepared_count,
                        timed_out_worker_ids=missing_worker_ids,
                    )
                    self._benchmark_stop.set()
                    round_state.start_event.set()
                    return
                time.sleep(0.2)

        if role == "consumer":
            self._consume_mpmc_prefeed_messages(
                round_state=round_state,
                timeout_s=float(self.test_config["cluster_ready_timeout_seconds"]),
            )

        if not self._wait_for_mpmc_runtime_start():
            self._set_forced_benchmark_result(
                reason="mpmc_runtime_start_timeout",
                total_workers=workers,
                completed_workers=completed,
                timed_out_worker_ids=sorted(pending_threads.keys()),
            )
            self._benchmark_stop.set()
            round_state.start_event.set()
            return

        self.start_time = time.time()
        self.end_time = self.start_time + int(self.test_config["max_benchmark_seconds"])
        deadline_ts = self.end_time
        round_state.benchmark_deadline_ts = deadline_ts
        self._start_network_bandwidth_sampler()
        self._start_heartbeat()
        round_state.start_event.set()

        while pending_threads:
            with round_state.prepared_lock:
                fatal_error_snapshot = dict(round_state.fatal_errors)
            if fatal_error_snapshot:
                self._raise_mpmc_fatal_worker_error(
                    round_state=round_state,
                    fatal_errors=fatal_error_snapshot,
                    stage="benchmark_operation",
                )
            now = time.time()
            if not stop_requested and now >= deadline_ts:
                stop_requested = True
                self.end_time = now
                self._benchmark_stop.set()
                stop_grace_deadline_ts = now + MPMC_WORKER_EXIT_GRACE_SECONDS
                logger.info("🛑 benchmark 到达运行时长，主线程发出 MPMC stop intent")
                self._close_mpmc_round_endpoints(
                    round_state=round_state,
                    reason="benchmark_deadline",
                )
            completed = self._collect_finished_mpmc_workers(
                pending_threads=pending_threads,
                worker_results=round_state.worker_results,
                worker_results_lock=round_state.worker_results_lock,
                completed=completed,
                total_workers=workers,
            )

            if not pending_threads:
                break

            if stop_requested and stop_grace_deadline_ts is not None and now >= stop_grace_deadline_ts:
                timed_out_worker_ids = sorted(pending_threads.keys())
                logger.error(
                    "❌ 有线程在 MPMC stop intent 后仍未退出，先关闭 kv_store 解除阻塞，再做最终收束: "
                    f"timed_out_worker_ids={timed_out_worker_ids} "
                    f"grace_seconds={MPMC_WORKER_EXIT_GRACE_SECONDS}"
                )
                self._close_kv_store(reason="mpmc_stop_timeout")
                abort_deadline_ts = time.time() + MPMC_WORKER_ABORT_GRACE_SECONDS
                while pending_threads and time.time() < abort_deadline_ts:
                    completed = self._collect_finished_mpmc_workers(
                        pending_threads=pending_threads,
                        worker_results=round_state.worker_results,
                        worker_results_lock=round_state.worker_results_lock,
                        completed=completed,
                        total_workers=workers,
                    )
                    if pending_threads:
                        time.sleep(0.2)
                if pending_threads:
                    timed_out_worker_ids = sorted(pending_threads.keys())
                    self._set_forced_benchmark_result(
                        reason="mpmc_worker_exit_timeout",
                        total_workers=workers,
                        completed_workers=completed,
                        timed_out_worker_ids=timed_out_worker_ids,
                    )
                break

            time.sleep(1.0)

        completed = self._collect_finished_mpmc_workers(
            pending_threads=pending_threads,
            worker_results=round_state.worker_results,
            worker_results_lock=round_state.worker_results_lock,
            completed=completed,
            total_workers=workers,
        )
        with round_state.prepared_lock:
            fatal_error_snapshot = dict(round_state.fatal_errors)
        if fatal_error_snapshot:
            self._raise_mpmc_fatal_worker_error(
                round_state=round_state,
                fatal_errors=fatal_error_snapshot,
                stage="benchmark_completion",
            )
        if pending_threads:
            timed_out_worker_ids = sorted(pending_threads.keys())
            self._set_forced_benchmark_result(
                reason="mpmc_worker_exit_timeout",
                total_workers=workers,
                completed_workers=completed,
                timed_out_worker_ids=timed_out_worker_ids,
            )
    def run_benchmark(self) -> Dict[str, Any]:
        """Run the benchmark with timeout protection.

        This prevents the main flow from hanging due to blocked worker threads.
        """
        if not self.test_config:
            logger.error("❌ 无法运行基准测试：配置未初始化")
            return {}

        test_mode = str(self.test_config.get("test_mode", TestMode.KVSTORE.value))
        node_role = str(self.test_config.get("node_role", ""))
        if self.kv_store is None and not (
            test_mode == TestMode.MPMC.value and node_role == "producer"
        ):
            logger.error("❌ 无法运行基准测试：存储实例未初始化")
            return {}

        threads_per_process = int(self.test_config["threads_per_process"])

        logger.info("🚀 开始基准测试")
        logger.info("📊 测试参数:")
        logger.info(f"   - 角色: {self.test_config['node_role']}")
        logger.info(f"   - 每进程线程数: {threads_per_process}")
        logger.info(f"   - 运行时长: {int(self.test_config['max_benchmark_seconds'])} 秒/节点")
        logger.info(f"   - Warmup: {METRIC_WARMUP_SECONDS} 秒")
        logger.info(f"   - 数据大小策略: {self._describe_value_size_strategy()}")
        self.operation_results = []
        self._benchmark_stop.clear()
        self._forced_benchmark_result = None
        self._kv_store_closed = False
        self._network_bandwidth_sampler = None
        self._network_bandwidth_summary = {}

        try:
            if test_mode == TestMode.MPMC.value:
                self.start_time = None
                self.end_time = None
                self._run_mpmc_workers(workers=threads_per_process)
            else:
                self.start_time = time.time()
                max_benchmark_seconds = int(self.test_config["max_benchmark_seconds"])
                deadline_ts = self.start_time + max_benchmark_seconds
                # Reuse end_time as the metrics window end:
                # - KV mode: end_time is deadline_ts
                # - MPMC mode: overwritten to now when stop intent is triggered
                self.end_time = deadline_ts
                self._start_network_bandwidth_sampler()
                # Start heartbeat after time window is initialized, so it can report elapsed/remaining.
                self._start_heartbeat()
                self._run_kv_workers(workers=threads_per_process, deadline_ts=deadline_ts)
        finally:
            self._stop_heartbeat()
            self._stop_network_bandwidth_sampler()

        # Keep previously set end_time (KV mode: deadline; MPMC mode: close time).
        if self.end_time is None:
            self.end_time = time.time()

        self._flush_fluxon_phase_summary()
        self._wait_fluxon_phase_log_exporter_idle(GREPTIME_OTLP_LOG_EXPORT_DRAIN_TIMEOUT_SECONDS)

        if self._forced_benchmark_result is not None:
            forced_result = dict(self._forced_benchmark_result)
            forced_result["network_bandwidth"] = self._network_bandwidth_payload()
            return forced_result

        # Compute results
        results = self._calculate_benchmark_results()
        return results


def main():

    start_time_main = time.time()
    # CLI args
    parser = argparse.ArgumentParser(
        prog="distributed_benchmark_node",
        description="分布式基准测试节点，向协调者注册并执行测试",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog=(
            "示例:\n"
            "  python3 fluxon_test_stack/distributed_benchmark_node.py \\\n"
            "    --instance-key bench-node-0 --coordinator 127.0.0.1:7777"
        ),
    )
    parser.add_argument(
        "--instance-key",
        "-k",
        required=True,
        help="与 coordinator 配置中的 node_overrides[*].instance_key 对应",
    )
    parser.add_argument(
        "--coordinator",
        "-C",
        required=True,
        help="协调者地址，格式 host:port",
    )
    args = parser.parse_args()
    # Parse coordinator address
    coord = args.coordinator
    if ":" not in coord:
        logger.error("❌ --coordinator 需为 host:port 格式，例如 127.0.0.1:7777")
        return 2
    host_part, port_part = coord.rsplit(":", 1)
    try:
        port_val = int(port_part)
    except ValueError:
        logger.error("❌ --coordinator 端口非整数")
        return 2

    logger.info("🌟 启动分布式基准测试节点")
    proxy_vars = [
        "http_proxy",
        "https_proxy",
        "ftp_proxy",
        "all_proxy",
        "no_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "FTP_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
    ]
    for var in proxy_vars:
        if var in os.environ:
            del os.environ[var]
    benchmark_node = BenchmarkNode()
    benchmark_node.coordinator_host = host_part
    benchmark_node.coordinator_port = port_val
    benchmark_node.instance_key = args.instance_key
    delayed_process_cleanup = False

    try:

        # Register and fetch config
        logger.info("📝 连接成功，正在注册并获取测试配置")
        if not benchmark_node.register_and_get_test_config():
            logger.error("💥 注册失败，退出程序")
            sys.exit(1)

        # Initialize node
        logger.info("🚀 注册成功，正在初始化节点")
        if not benchmark_node.initialize_from_test_config():
            logger.error("💥 节点初始化失败，退出程序")
            sys.exit(1)
        # Multi-round benchmark loop: each round re-reports READY, waits for START, and runs once.
        round_index = 0
        while True:
            round_index += 1
            logger.info(f"🌀 准备开始第 {round_index} 轮基准测试")

            if benchmark_node.test_config.get("test_mode") == TestMode.MPMC.value:
                try:
                    benchmark_node._prepare_mpmc_round_before_ready(
                        workers=int(benchmark_node.test_config["threads_per_process"])
                    )
                except Exception as exc:
                    logger.error("💥 MPMC round prewarm failed before READY: %s", exc)
                    sys.exit(1)

            # 报告就绪状态
            logger.info("📢 节点初始化完成，正在向协调者报告已准备就绪")
            if not benchmark_node.report_ready_to_coordinator():
                logger.error("💥 报告就绪状态失败，退出程序")
                sys.exit(1)

            # 等待开始信号
            logger.info("⏳ 已向协调者报告准备就绪，等待开始信号")
            if not benchmark_node.wait_for_start():
                logger.error("💥 等待开始信号失败，退出程序")
                sys.exit(1)

            # 运行基准测试
            logger.info("🚀 收到开始信号，正在运行基准测试")
            results = benchmark_node.run_benchmark()
            if not results:
                logger.error("💥 基准测试执行失败，退出程序")
                sys.exit(1)
            print(json.dumps(results, indent=2, ensure_ascii=False))

            # 上报结果
            logger.info("📊 基准测试完成，正在上报结果")
            if not benchmark_node.report_results(results):
                logger.error("💥 结果上报失败")
                sys.exit(1)
            if not benchmark_node.wait_for_round_gate():
                logger.error("💥 协调者未确认本轮 benchmark 成功收束")
                sys.exit(1)

            logger.info(
                "🎉 本轮基准测试完成 (round=%d, threads_per_process=%s, value_size_strategy=%s)",
                round_index,
                benchmark_node.test_config.get("threads_per_process")
                if benchmark_node.test_config
                else "N/A",
                benchmark_node._describe_value_size_strategy()
                if benchmark_node.test_config
                else "N/A",
            )

            # 如果协调者未计划更多测试，则结束循环
            if not benchmark_node.has_more_tests:
                logger.info("📌 协调者未计划更多测试轮次，结束基准测试循环")
                break

        logger.info("🎉 所有任务完成，节点即将退出")

        if benchmark_node._forced_benchmark_result is not None:
            logger.error(
                "❌ benchmark produced a forced failure result; "
                "skip post-run cleanup delay and exit immediately so the invalid sample is reported upstream"
            )
            return 0

        delayed_process_cleanup = True

    except KeyboardInterrupt:
        logger.warning("⚠️ 接收到中断信号，正在退出...")
        return 130
    except Exception as e:
        logger.error(f"💥 程序执行出现异常: {e}")
        logger.debug("📍 异常详情:", exc_info=True)
        sys.exit(1)
    finally:
        pending_round = benchmark_node._prepared_mpmc_round
        if pending_round is not None:
            benchmark_node._prepared_mpmc_round = None
            benchmark_node._finish_mpmc_round(
                round_state=pending_round,
                reason="node_process_early_exit",
            )
        if not delayed_process_cleanup:
            benchmark_node._close_kv_store(reason="node_process_early_exit")
            benchmark_node._close_fluxon_phase_log_exporter()

    # Wait 30 seconds before dropping KvClient, so underlying resources can finish cleanup/reporting.
    logger.info("⏳ 析构 KVClient 前等待 30 秒…")
    time.sleep(30)

    # After 30 seconds, do a dummy PUT to ensure the client is still alive and resources were not released early.
    try:
        if benchmark_node.kv_store is not None and not benchmark_node._kv_store_closed:
            dummy_key_prefix = benchmark_node.key_prefix or "benchmark"
            dummy_key = f"{dummy_key_prefix}_dummy_shutdown_{int(time.time())}"
            sampled_val_size = benchmark_node._resolve_kv_value_size(0, 0) if benchmark_node.test_config else 1024
            val_size = min(int(sampled_val_size), 1024)
            dummy_val = benchmark_node._generate_test_data(val_size)
            dummy_deadline_ts = time.time() + 5.0
            put_res = benchmark_node._put_single_operation(
                dummy_key,
                dummy_val,
                inflight_at_start=0,
                deadline_ts=dummy_deadline_ts,
                ctx=f"node={benchmark_node.node_id} role=dummy thread=-1 op=-1",
            )
            logger.info(f"🧪 dummy PUT after sleep: success={put_res.success}, latency_us={put_res.latency_us:.0f}, key={dummy_key}")
        else:
            logger.warning("⚠️ dummy PUT 跳过：kv_store 不存在")
    except Exception as e:
        logger.error(f"❌ dummy PUT 执行失败: {e}")

    # Explicitly close KvClient so lease-keepalive background tasks stop before interpreter exit,
    # avoiding errors like "cannot schedule new futures after shutdown".
    benchmark_node._close_kv_store(reason="node_process_exit")
    benchmark_node._close_fluxon_phase_log_exporter()

    end_time_main = time.time()
    total_duration_main = end_time_main - start_time_main
    logger.info(f"⏱️ 节点总运行时间: {total_duration_main:.2f} 秒")
    logger.info("👋 基准测试节点已退出")


if __name__ == "__main__":
    exit(main())
