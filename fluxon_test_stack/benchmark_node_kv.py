from __future__ import annotations

"""KV helpers for the benchmark stack."""

import bisect
import copy
import hashlib
import json
import os
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum, unique
from functools import lru_cache
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Union

from benchmark_role_names import (
    KV_NODE_ROLE_SEED,
    KV_NODE_ROLE_WORKER,
    canonicalize_kv_node_role,
    is_kv_seed_role,
    is_kv_worker_role,
)
from fluxon_py import FluxonKvClientConfig as KVCacheConfig
from fluxon_py import new_store
from fluxon_py.kvclient.kvclient_interface import KvClient, PutOptionalArgs

TEST_MODE_MPMC = "MPMC"
TEST_MODE_KVSTORE = "KVSTORE"
TEST_MODE_KVSTORE_WITH_LOCAL_CACHE = "KVSTORE_WITH_LOCAL_CACHE"
KV_TEST_MODES = (TEST_MODE_KVSTORE, TEST_MODE_KVSTORE_WITH_LOCAL_CACHE)

VALUE_SIZE_MODE_FIXED = "FIXED"
VALUE_SIZE_MODE_RANDOM_WEIGHTED_SET = "RANDOM_WEIGHTED_SET"

REQUEST_DISTRIBUTION_UNIFORM = "uniform"
REQUEST_DISTRIBUTION_ZIPFIAN = "zipfian"

KV_OPERATION_PUT = "PUT"
KV_OPERATION_GET = "GET"

BACKEND_KIND_FLUXON = "FLUXON"
BACKEND_KIND_REDIS = "REDIS"
BACKEND_KIND_ALLUXIO = "ALLUXIO"
REDIS_BENCH_INFLIGHT_GUARD_PREFIX = "__fluxon_bench_inflight_guard__"

KV_GET_MISS_ERROR = "GET failed: KeyNotFoundError"

BENCHMARK_KEY_MODE = "mode"
BENCHMARK_KEY_WORKLOAD_ID = "workload_id"
BENCHMARK_KEY_READ_RATIO = "read_ratio"
BENCHMARK_KEY_WRITE_RATIO = "write_ratio"
BENCHMARK_KEY_REQUEST_DISTRIBUTION = "request_distribution"
BENCHMARK_KEY_KEYSPACE_SIZE = "keyspace_size"
BENCHMARK_KEY_AFFINITY_LOCALITY_RATIO = "affinity_locality_ratio"
BENCHMARK_KEY_AFFINITY_SLOT_COUNT = "affinity_slot_count"

DEFAULT_KV_KEYSPACE_SIZE = 101
DEFAULT_ZIPFIAN_THETA = 0.99
STABLE_HASH_MODULUS = float(1 << 64)
KV_SEED_BOOTSTRAP_MAX_CONCURRENCY = 16
KV_VERBOSE_PER_OP_LOG = str(os.environ.get("FLUXON_BENCH_KV_VERBOSE", "")).strip().lower() not in ("", "0", "false", "no")
FLUXON_PHASE_LOG_INTERVAL_OPS = 128
FLUXON_PHASE_SLOW_OP_THRESHOLD_US = 50_000.0
FLUXON_PHASE_SEGMENT_EXT_TRANSPORT_US = "ext_transport_us"
FLUXON_PHASE_SEGMENT_TRANSPORT_RESIDUAL_US = "transport_residual_us"
FLUXON_PHASE_SEGMENT_CALLER_SUBMIT_US = "caller_submit_us"
FLUXON_PHASE_SEGMENT_OWNER_QUEUE_US = "owner_queue_us"
FLUXON_PHASE_SEGMENT_OWNER_TRANSPORT_US = "owner_transport_us"
FLUXON_PHASE_SEGMENT_OWNER_HANDLE_US = "owner_handle_us"
FLUXON_PHASE_SEGMENT_OWNER_HANDLE_BLOCKING_WAIT_US = "owner_handle_blocking_wait_us"
FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_WITH_GIL_US = "owner_handle_py_with_gil_us"
FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_GIL_WAIT_US = "owner_handle_py_gil_wait_us"
FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_ARG_BUILD_US = "owner_handle_py_arg_build_us"
FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_CALL_US = "owner_handle_py_call_us"
FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_RESULT_UNPACK_US = "owner_handle_py_result_unpack_us"
FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_RESULT_COPY_US = "owner_handle_py_result_copy_us"
FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_DECODE_US = "owner_handle_py_decode_us"
FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_HANDLER_BODY_US = "owner_handle_py_handler_body_us"
FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_ENCODE_US = "owner_handle_py_encode_us"
FLUXON_PHASE_SEGMENT_CALLER_COMPLETE_US = "caller_complete_us"
FLUXON_PHASE_SEGMENT_EXT_HANDLE_US = "ext_handle_us"
FLUXON_PHASE_SEGMENT_REQUEST_TO_OWNER_RECV_US = "request_to_owner_recv_us"
FLUXON_PHASE_SEGMENT_OWNER_RECV_TO_DISPATCH_SEND_US = "owner_recv_to_dispatch_send_us"
FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_SEND_TO_ENQUEUE_US = "owner_dispatch_send_to_enqueue_us"
FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_ENQUEUE_TO_DEQUEUE_US = (
    "owner_dispatch_enqueue_to_dequeue_us"
)
FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_SEND_TO_DEQUEUE_US = "owner_dispatch_send_to_dequeue_us"
FLUXON_PHASE_SEGMENT_OWNER_DEQUEUE_TO_REPLY_PATH_PREPARE_US = (
    "owner_dequeue_to_reply_path_prepare_us"
)
FLUXON_PHASE_SEGMENT_OWNER_REPLY_PATH_PREPARE_US = "owner_reply_path_prepare_us"
FLUXON_PHASE_SEGMENT_OWNER_REPLY_PATH_READY_TO_DISPATCH_US = (
    "owner_reply_path_ready_to_dispatch_us"
)
FLUXON_PHASE_SEGMENT_OWNER_RECV_TO_DISPATCH_US = "owner_recv_to_dispatch_us"
FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_TO_MAP_ENTER_US = "owner_dispatch_to_map_enter_us"
FLUXON_PHASE_SEGMENT_OWNER_MAP_ENTER_TO_SPAWN_US = "owner_map_enter_to_spawn_us"
FLUXON_PHASE_SEGMENT_OWNER_SPAWN_TO_LOOP_RETURN_US = "owner_spawn_to_loop_return_us"
FLUXON_PHASE_SEGMENT_OWNER_LOOP_RETURN_TO_TASK_START_US = (
    "owner_loop_return_to_task_start_us"
)
FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_TO_LOOP_RETURN_US = (
    "owner_dispatch_to_loop_return_us"
)
FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_TO_HANDLE_US = "owner_dispatch_to_handle_us"
FLUXON_PHASE_SEGMENT_OWNER_TASK_START_TO_BLOCKING_SUBMIT_US = (
    "owner_task_start_to_blocking_submit_us"
)
FLUXON_PHASE_SEGMENT_OWNER_BLOCKING_SUBMIT_TO_CLOSURE_START_US = (
    "owner_blocking_submit_to_closure_start_us"
)
FLUXON_PHASE_SEGMENT_OWNER_HANDLE_TO_RESP_SEND_US = "owner_handle_to_resp_send_us"
FLUXON_PHASE_SEGMENT_RESPONSE_SEND_TO_CALLER_RECV_US = "response_send_to_caller_recv_us"
FLUXON_PHASE_SEGMENT_OWNER1_ROUNDTRIP_US = "owner1_roundtrip_us"
FLUXON_PHASE_SEGMENT_CALLER_POST_SUBMIT_ROUNDTRIP_US = "caller_post_submit_roundtrip_us"
FLUXON_PHASE_SEGMENT_OWNER_LOCAL_SERVICE_US = "owner_local_service_us"
FLUXON_PHASE_SEGMENT_CALLER_RESPONSE_FINALIZE_US = "caller_response_finalize_us"
FLUXON_PHASE_SEGMENT_TRANSPORT_INFLIGHT_ESTIMATED_US = "transport_inflight_estimated_us"
FLUXON_PHASE_SEGMENT_CALLER_RECV_TO_DISPATCH_US = "caller_recv_to_dispatch_us"
FLUXON_PHASE_SEGMENT_CALLER_DISPATCH_TO_COMPLETE_US = "caller_dispatch_to_complete_us"
FLUXON_PHASE_SEGMENT_CALLER_COMPLETE_TO_DECODE_US = "caller_complete_to_decode_us"
FLUXON_PHASE_SEGMENT_NAMES = (
    FLUXON_PHASE_SEGMENT_EXT_TRANSPORT_US,
    FLUXON_PHASE_SEGMENT_TRANSPORT_RESIDUAL_US,
    FLUXON_PHASE_SEGMENT_CALLER_SUBMIT_US,
    FLUXON_PHASE_SEGMENT_OWNER_QUEUE_US,
    FLUXON_PHASE_SEGMENT_OWNER_TRANSPORT_US,
    FLUXON_PHASE_SEGMENT_OWNER_HANDLE_US,
    FLUXON_PHASE_SEGMENT_OWNER_HANDLE_BLOCKING_WAIT_US,
    FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_WITH_GIL_US,
    FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_GIL_WAIT_US,
    FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_ARG_BUILD_US,
    FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_CALL_US,
    FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_RESULT_UNPACK_US,
    FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_RESULT_COPY_US,
    FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_DECODE_US,
    FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_HANDLER_BODY_US,
    FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_ENCODE_US,
    FLUXON_PHASE_SEGMENT_CALLER_COMPLETE_US,
    FLUXON_PHASE_SEGMENT_EXT_HANDLE_US,
    FLUXON_PHASE_SEGMENT_REQUEST_TO_OWNER_RECV_US,
    FLUXON_PHASE_SEGMENT_OWNER_RECV_TO_DISPATCH_SEND_US,
    FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_SEND_TO_ENQUEUE_US,
    FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_ENQUEUE_TO_DEQUEUE_US,
    FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_SEND_TO_DEQUEUE_US,
    FLUXON_PHASE_SEGMENT_OWNER_DEQUEUE_TO_REPLY_PATH_PREPARE_US,
    FLUXON_PHASE_SEGMENT_OWNER_REPLY_PATH_PREPARE_US,
    FLUXON_PHASE_SEGMENT_OWNER_REPLY_PATH_READY_TO_DISPATCH_US,
    FLUXON_PHASE_SEGMENT_OWNER_RECV_TO_DISPATCH_US,
    FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_TO_MAP_ENTER_US,
    FLUXON_PHASE_SEGMENT_OWNER_MAP_ENTER_TO_SPAWN_US,
    FLUXON_PHASE_SEGMENT_OWNER_SPAWN_TO_LOOP_RETURN_US,
    FLUXON_PHASE_SEGMENT_OWNER_LOOP_RETURN_TO_TASK_START_US,
    FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_TO_LOOP_RETURN_US,
    FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_TO_HANDLE_US,
    FLUXON_PHASE_SEGMENT_OWNER_TASK_START_TO_BLOCKING_SUBMIT_US,
    FLUXON_PHASE_SEGMENT_OWNER_BLOCKING_SUBMIT_TO_CLOSURE_START_US,
    FLUXON_PHASE_SEGMENT_OWNER_HANDLE_TO_RESP_SEND_US,
    FLUXON_PHASE_SEGMENT_RESPONSE_SEND_TO_CALLER_RECV_US,
    FLUXON_PHASE_SEGMENT_OWNER1_ROUNDTRIP_US,
    FLUXON_PHASE_SEGMENT_CALLER_POST_SUBMIT_ROUNDTRIP_US,
    FLUXON_PHASE_SEGMENT_OWNER_LOCAL_SERVICE_US,
    FLUXON_PHASE_SEGMENT_CALLER_RESPONSE_FINALIZE_US,
    FLUXON_PHASE_SEGMENT_TRANSPORT_INFLIGHT_ESTIMATED_US,
    FLUXON_PHASE_SEGMENT_CALLER_RECV_TO_DISPATCH_US,
    FLUXON_PHASE_SEGMENT_CALLER_DISPATCH_TO_COMPLETE_US,
    FLUXON_PHASE_SEGMENT_CALLER_COMPLETE_TO_DECODE_US,
)
FLUXON_RPC_PATH_KIND_UNKNOWN = "unknown"
FLUXON_RPC_PATH_KIND_FAST = "fast"
FLUXON_RPC_PATH_KIND_SLOW = "slow"
FLUXON_OWNER_PATH_KIND = "owner_path_kind"
FLUXON_OWNER_PATH_KIND_IPC = "ipc"
FLUXON_OWNER1_REQUEST_PATH_KIND = "owner1_request_path_kind"
FLUXON_OWNER1_RESPONSE_PATH_KIND = "owner1_response_path_kind"
FLUXON_PHASE_PATH_BUCKET_FAST = "fast_path"
FLUXON_PHASE_PATH_BUCKET_SLOW = "slow_path"
FLUXON_PHASE_PATH_BUCKET_IPC = "ipc_path"
FLUXON_PHASE_PATH_BUCKET_NAMES = (
    FLUXON_PHASE_PATH_BUCKET_FAST,
    FLUXON_PHASE_PATH_BUCKET_SLOW,
    FLUXON_PHASE_PATH_BUCKET_IPC,
)
FLUXON_PHASE_PATH_METRIC_RPC_EXT_TOTAL_US = "rpc_ext_total_us"
FLUXON_PHASE_PATH_METRIC_OWNER1_ROUNDTRIP_US = FLUXON_PHASE_SEGMENT_OWNER1_ROUNDTRIP_US
FLUXON_PHASE_PATH_METRIC_CALLER_POST_SUBMIT_ROUNDTRIP_US = (
    FLUXON_PHASE_SEGMENT_CALLER_POST_SUBMIT_ROUNDTRIP_US
)
FLUXON_PHASE_PATH_METRIC_OWNER_LOCAL_SERVICE_US = FLUXON_PHASE_SEGMENT_OWNER_LOCAL_SERVICE_US
FLUXON_PHASE_PATH_METRIC_TRANSPORT_INFLIGHT_ESTIMATED_US = (
    FLUXON_PHASE_SEGMENT_TRANSPORT_INFLIGHT_ESTIMATED_US
)
FLUXON_PHASE_PATH_METRIC_OWNER_RECV_TO_DISPATCH_SEND_US = (
    FLUXON_PHASE_SEGMENT_OWNER_RECV_TO_DISPATCH_SEND_US
)
FLUXON_PHASE_PATH_METRIC_OWNER_DISPATCH_SEND_TO_ENQUEUE_US = (
    FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_SEND_TO_ENQUEUE_US
)
FLUXON_PHASE_PATH_METRIC_OWNER_DISPATCH_ENQUEUE_TO_DEQUEUE_US = (
    FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_ENQUEUE_TO_DEQUEUE_US
)
FLUXON_PHASE_PATH_METRIC_OWNER_DISPATCH_SEND_TO_DEQUEUE_US = (
    FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_SEND_TO_DEQUEUE_US
)
FLUXON_PHASE_PATH_METRIC_OWNER_DEQUEUE_TO_REPLY_PATH_PREPARE_US = (
    FLUXON_PHASE_SEGMENT_OWNER_DEQUEUE_TO_REPLY_PATH_PREPARE_US
)
FLUXON_PHASE_PATH_METRIC_OWNER_REPLY_PATH_PREPARE_US = (
    FLUXON_PHASE_SEGMENT_OWNER_REPLY_PATH_PREPARE_US
)
FLUXON_PHASE_PATH_METRIC_OWNER_REPLY_PATH_READY_TO_DISPATCH_US = (
    FLUXON_PHASE_SEGMENT_OWNER_REPLY_PATH_READY_TO_DISPATCH_US
)
FLUXON_PHASE_PATH_METRIC_OWNER_DISPATCH_TO_MAP_ENTER_US = (
    FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_TO_MAP_ENTER_US
)
FLUXON_PHASE_PATH_METRIC_OWNER_MAP_ENTER_TO_SPAWN_US = (
    FLUXON_PHASE_SEGMENT_OWNER_MAP_ENTER_TO_SPAWN_US
)
FLUXON_PHASE_PATH_METRIC_OWNER_SPAWN_TO_LOOP_RETURN_US = (
    FLUXON_PHASE_SEGMENT_OWNER_SPAWN_TO_LOOP_RETURN_US
)
FLUXON_PHASE_PATH_METRIC_OWNER_DISPATCH_TO_LOOP_RETURN_US = (
    FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_TO_LOOP_RETURN_US
)
FLUXON_PHASE_PATH_METRIC_OWNER_DISPATCH_TO_HANDLE_US = (
    FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_TO_HANDLE_US
)
FLUXON_PHASE_PATH_METRIC_OWNER_HANDLE_TO_RESP_SEND_US = (
    FLUXON_PHASE_SEGMENT_OWNER_HANDLE_TO_RESP_SEND_US
)
FLUXON_PHASE_PATH_METRIC_NAMES = (
    FLUXON_PHASE_PATH_METRIC_RPC_EXT_TOTAL_US,
    FLUXON_PHASE_PATH_METRIC_OWNER1_ROUNDTRIP_US,
    FLUXON_PHASE_PATH_METRIC_CALLER_POST_SUBMIT_ROUNDTRIP_US,
    FLUXON_PHASE_PATH_METRIC_OWNER_LOCAL_SERVICE_US,
    FLUXON_PHASE_PATH_METRIC_TRANSPORT_INFLIGHT_ESTIMATED_US,
    FLUXON_PHASE_PATH_METRIC_OWNER_RECV_TO_DISPATCH_SEND_US,
    FLUXON_PHASE_PATH_METRIC_OWNER_DISPATCH_SEND_TO_ENQUEUE_US,
    FLUXON_PHASE_PATH_METRIC_OWNER_DISPATCH_ENQUEUE_TO_DEQUEUE_US,
    FLUXON_PHASE_PATH_METRIC_OWNER_DISPATCH_SEND_TO_DEQUEUE_US,
    FLUXON_PHASE_PATH_METRIC_OWNER_DEQUEUE_TO_REPLY_PATH_PREPARE_US,
    FLUXON_PHASE_PATH_METRIC_OWNER_REPLY_PATH_PREPARE_US,
    FLUXON_PHASE_PATH_METRIC_OWNER_REPLY_PATH_READY_TO_DISPATCH_US,
    FLUXON_PHASE_PATH_METRIC_OWNER_DISPATCH_TO_MAP_ENTER_US,
    FLUXON_PHASE_PATH_METRIC_OWNER_MAP_ENTER_TO_SPAWN_US,
    FLUXON_PHASE_PATH_METRIC_OWNER_SPAWN_TO_LOOP_RETURN_US,
    FLUXON_PHASE_PATH_METRIC_OWNER_DISPATCH_TO_LOOP_RETURN_US,
    FLUXON_PHASE_PATH_METRIC_OWNER_DISPATCH_TO_HANDLE_US,
    FLUXON_PHASE_PATH_METRIC_OWNER_HANDLE_TO_RESP_SEND_US,
)
_BENCHMARK_CLIENT_STRIP_TEST_SPEC_KEYS = (
    "side_transfer_worker_count",
    "side_transfer_worker_p2p_port_base",
    "side_transfer_role",
)

KV_BENCHMARK_EXTRA_KEYS = (
    BENCHMARK_KEY_WORKLOAD_ID,
    BENCHMARK_KEY_READ_RATIO,
    BENCHMARK_KEY_WRITE_RATIO,
    BENCHMARK_KEY_REQUEST_DISTRIBUTION,
    BENCHMARK_KEY_KEYSPACE_SIZE,
    BENCHMARK_KEY_AFFINITY_LOCALITY_RATIO,
    BENCHMARK_KEY_AFFINITY_SLOT_COUNT,
    "kv_bootstrap_before_ready",
)


@unique
class KVGetResultKind(Enum):
    CACHE_HIT = "cache_hit"
    CACHE_MISS = "cache_miss"
    ERROR = "error"


def classify_kv_get_result(error_msg: Optional[str]) -> KVGetResultKind:
    if error_msg is None:
        return KVGetResultKind.CACHE_HIT
    error_text = str(error_msg).strip()
    if not error_text:
        return KVGetResultKind.ERROR
    error_text_lower = error_text.lower()
    if "keynotfounderror" in error_text_lower:
        return KVGetResultKind.CACHE_MISS
    if "keynotfound" in error_text_lower:
        return KVGetResultKind.CACHE_MISS
    if "notfounderror" in error_text_lower:
        return KVGetResultKind.CACHE_MISS
    if "key not found" in error_text_lower:
        return KVGetResultKind.CACHE_MISS
    if "missing key" in error_text_lower:
        return KVGetResultKind.CACHE_MISS
    if "no such key" in error_text_lower:
        return KVGetResultKind.CACHE_MISS
    return KVGetResultKind.ERROR


def _is_key_being_written_error(error: Any) -> bool:
    return type(error).__name__ == "KeyBeingWrittenError"


def _is_mooncake_replica_not_ready_error(error: Any) -> bool:
    details = getattr(error, "details", None)
    if not isinstance(details, dict):
        return False
    return details.get("mooncake_code") == -703


def _is_put_compat_success_error(error: Any) -> bool:
    return _is_key_being_written_error(error) or _is_mooncake_replica_not_ready_error(error)


def normalize_kv_get_error(error_msg: Optional[str]) -> Optional[str]:
    if error_msg is None:
        return None
    if classify_kv_get_result(error_msg) == KVGetResultKind.CACHE_MISS:
        return KV_GET_MISS_ERROR
    return str(error_msg)


@dataclass(frozen=True)
class KVRuntimeConfig:
    workload_id: str
    key_prefix: str
    keyspace_size: int
    request_distribution: str
    read_ratio: Optional[float]
    write_ratio: Optional[float]
    affinity_locality_ratio: Optional[float]
    affinity_slot_count: int
    affinity_slot_index: Optional[int]

    def has_mixed_operations(self) -> bool:
        return self.read_ratio is not None and self.write_ratio is not None

    def read_cutoff(self) -> float:
        if self.read_ratio is None or self.write_ratio is None:
            raise ValueError("read/write ratio is not configured")
        total = float(self.read_ratio) + float(self.write_ratio)
        if total <= 0.0:
            raise ValueError("read_ratio + write_ratio must be > 0")
        return float(self.read_ratio) / total

    def uses_affinity(self) -> bool:
        return (
            self.affinity_locality_ratio is not None
            and float(self.affinity_locality_ratio) > 0.0
            and int(self.affinity_slot_count) > 1
        )


@dataclass(frozen=True)
class _ZipfianSampler:
    cdf: tuple[float, ...]

    def sample(self, bucket: int) -> int:
        if len(self.cdf) == 1:
            return 0
        threshold = float(bucket) / STABLE_HASH_MODULUS
        idx = bisect.bisect_left(self.cdf, threshold)
        if idx >= len(self.cdf):
            return len(self.cdf) - 1
        return idx


class _SimpleResult:
    def __init__(self, *, ok: bool, value: Any = None, error: Optional[str] = None) -> None:
        self._ok = bool(ok)
        self._value = value
        self._error = error

    def is_ok(self) -> bool:
        return self._ok

    def unwrap(self) -> Any:
        if not self._ok:
            raise RuntimeError(self._error or "result is error")
        return self._value

    def unwrap_error(self) -> str:
        if self._ok:
            raise RuntimeError("result is ok")
        return str(self._error or "unknown error")

    @classmethod
    def ok(cls, value: Any = None) -> "_SimpleResult":
        return cls(ok=True, value=value)

    @classmethod
    def err(cls, error: str) -> "_SimpleResult":
        return cls(ok=False, error=error)

@dataclass(frozen=True)
class _FluxonPhaseSample:
    submit_us: float
    wait_us: float
    finalize_us: float
    total_us: float
    deadline_overrun_us: float
    extra_us: Dict[str, float] = field(default_factory=dict)
    extra_ts_us: Dict[str, float] = field(default_factory=dict)
    extra_tags: Dict[str, str] = field(default_factory=dict)


def _normalize_fluxon_observe_extra_us(raw_payload: Optional[Mapping[str, Any]]) -> Dict[str, float]:
    extras: Dict[str, float] = {}
    if not isinstance(raw_payload, Mapping):
        return extras
    for raw_key, raw_value in raw_payload.items():
        if not isinstance(raw_key, str) or not raw_key.endswith("_us"):
            continue
        if raw_key == "deadline_overrun_us":
            continue
        if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
            continue
        extras[raw_key] = max(0.0, float(raw_value))
    return extras


def _normalize_fluxon_observe_ts_us(raw_payload: Optional[Mapping[str, Any]]) -> Dict[str, float]:
    extras: Dict[str, float] = {}
    if not isinstance(raw_payload, Mapping):
        return extras
    raw_ts_payload = raw_payload.get("observe_ts_us")
    if not isinstance(raw_ts_payload, Mapping):
        return extras
    for raw_key, raw_value in raw_ts_payload.items():
        if not isinstance(raw_key, str) or not raw_key.endswith("_ts_us"):
            continue
        if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
            continue
        extras[raw_key] = max(0.0, float(raw_value))
    return extras


def _normalize_fluxon_observe_extra_tags(
    raw_payload: Optional[Mapping[str, Any]],
) -> Dict[str, str]:
    extras: Dict[str, str] = {}
    if not isinstance(raw_payload, Mapping):
        return extras
    for raw_key in (
        FLUXON_OWNER_PATH_KIND,
        "rpc_request_path_kind",
        "rpc_response_path_kind",
        FLUXON_OWNER1_REQUEST_PATH_KIND,
        FLUXON_OWNER1_RESPONSE_PATH_KIND,
    ):
        raw_value = raw_payload.get(raw_key)
        if not isinstance(raw_value, str):
            continue
        normalized = raw_value.strip().lower()
        if raw_key == FLUXON_OWNER_PATH_KIND:
            if normalized not in (
                FLUXON_RPC_PATH_KIND_UNKNOWN,
                FLUXON_OWNER_PATH_KIND_IPC,
                FLUXON_RPC_PATH_KIND_FAST,
                FLUXON_RPC_PATH_KIND_SLOW,
            ):
                continue
        else:
            if normalized not in (
                FLUXON_RPC_PATH_KIND_UNKNOWN,
                FLUXON_RPC_PATH_KIND_FAST,
                FLUXON_RPC_PATH_KIND_SLOW,
            ):
                continue
        extras[raw_key] = normalized
    return extras


def _build_fluxon_sync_phase_sample(
    *,
    started_at: float,
    done_at: float,
    deadline_ts: float,
    wall_done_ts: Optional[float] = None,
    extra_payload: Optional[Mapping[str, Any]] = None,
) -> _FluxonPhaseSample:
    wall_end = time.time() if wall_done_ts is None else wall_done_ts
    return _FluxonPhaseSample(
        submit_us=0.0,
        wait_us=max(0.0, (done_at - started_at) * 1_000_000.0),
        finalize_us=0.0,
        total_us=max(0.0, (done_at - started_at) * 1_000_000.0),
        deadline_overrun_us=max(0.0, (wall_end - deadline_ts) * 1_000_000.0),
        extra_us=_normalize_fluxon_observe_extra_us(extra_payload),
        extra_ts_us=_normalize_fluxon_observe_ts_us(extra_payload),
        extra_tags=_normalize_fluxon_observe_extra_tags(extra_payload),
    )


def _empty_fluxon_phase_bucket_counts() -> Dict[str, int]:
    return {"ok": 0, "miss": 0, "timeout": 0, "error": 0}


def _cross_process_ts_diff_us(
    later_ts_us: Optional[float],
    earlier_ts_us: Optional[float],
) -> Optional[float]:
    if later_ts_us is None or earlier_ts_us is None:
        return None
    later_value = float(later_ts_us)
    earlier_value = float(earlier_ts_us)
    if later_value <= 0.0 or earlier_value <= 0.0:
        return None
    if later_value < earlier_value:
        return None
    return later_value - earlier_value


def _build_fluxon_phase_segment_sample(
    extra_us: Mapping[str, float],
    extra_ts_us: Optional[Mapping[str, float]] = None,
) -> Dict[str, float]:
    ext_rpc_wait_us = extra_us.get("rpc_ext_rpc_wait_us")
    owner_total_us = extra_us.get("rpc_owner_total_us")
    owner_handle_us = extra_us.get("rpc_owner_handle_us")
    ext_finalize_us = extra_us.get("rpc_ext_finalize_us")
    if (
        ext_rpc_wait_us is None
        or owner_total_us is None
        or owner_handle_us is None
        or ext_finalize_us is None
    ):
        return {}
    segment_sample = {
        FLUXON_PHASE_SEGMENT_EXT_TRANSPORT_US: max(0.0, float(ext_rpc_wait_us) - float(owner_total_us)),
        FLUXON_PHASE_SEGMENT_OWNER_TRANSPORT_US: max(
            0.0,
            float(owner_total_us) - float(owner_handle_us),
        ),
        FLUXON_PHASE_SEGMENT_OWNER_HANDLE_US: max(0.0, float(owner_handle_us)),
        FLUXON_PHASE_SEGMENT_EXT_HANDLE_US: max(0.0, float(ext_finalize_us)),
    }
    owner_handle_detail_fields = (
        ("rpc_owner_handle_blocking_wait_us", FLUXON_PHASE_SEGMENT_OWNER_HANDLE_BLOCKING_WAIT_US),
        ("rpc_owner_handle_py_with_gil_us", FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_WITH_GIL_US),
        ("rpc_owner_handle_py_gil_wait_us", FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_GIL_WAIT_US),
        ("rpc_owner_handle_py_arg_build_us", FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_ARG_BUILD_US),
        ("rpc_owner_handle_py_call_us", FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_CALL_US),
        (
            "rpc_owner_handle_py_result_unpack_us",
            FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_RESULT_UNPACK_US,
        ),
        (
            "rpc_owner_handle_py_result_copy_us",
            FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_RESULT_COPY_US,
        ),
        ("rpc_owner_handle_py_decode_us", FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_DECODE_US),
        ("rpc_owner_handle_py_handler_body_us", FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_HANDLER_BODY_US),
        ("rpc_owner_handle_py_encode_us", FLUXON_PHASE_SEGMENT_OWNER_HANDLE_PY_ENCODE_US),
    )
    for extra_key, segment_name in owner_handle_detail_fields:
        phase_us = extra_us.get(extra_key)
        if phase_us is not None:
            segment_sample[segment_name] = max(0.0, float(phase_us))
    caller_submit_us = extra_us.get("rpc_caller_submit_us")
    owner_queue_us = extra_us.get("rpc_owner_queue_us")
    caller_complete_us = extra_us.get("rpc_caller_complete_us")
    if (
        caller_submit_us is not None
        and owner_queue_us is not None
        and caller_complete_us is not None
    ):
        ext_transport_us = float(segment_sample[FLUXON_PHASE_SEGMENT_EXT_TRANSPORT_US])
        caller_submit_value = max(0.0, float(caller_submit_us))
        owner_queue_value = max(0.0, float(owner_queue_us))
        caller_complete_value = max(0.0, float(caller_complete_us))
        segment_sample[FLUXON_PHASE_SEGMENT_CALLER_SUBMIT_US] = caller_submit_value
        segment_sample[FLUXON_PHASE_SEGMENT_OWNER_QUEUE_US] = owner_queue_value
        segment_sample[FLUXON_PHASE_SEGMENT_CALLER_COMPLETE_US] = caller_complete_value
        segment_sample[FLUXON_PHASE_SEGMENT_TRANSPORT_RESIDUAL_US] = max(
            0.0,
            ext_transport_us - caller_submit_value - owner_queue_value - caller_complete_value,
        )
    if isinstance(extra_ts_us, Mapping):
        caller_submit_ts_us = extra_ts_us.get("rpc_caller_submit_ts_us")
        owner1_request_send_ts_us = extra_ts_us.get("rpc_owner1_request_send_ts_us")
        owner_frame_recv_done_ts_us = extra_ts_us.get("rpc_owner_frame_recv_done_ts_us")
        owner_dispatch_send_started_ts_us = extra_ts_us.get(
            "rpc_owner_dispatch_send_started_ts_us"
        )
        owner_dispatch_enqueued_ts_us = extra_ts_us.get("rpc_owner_dispatch_enqueued_ts_us")
        owner_dispatch_dequeued_ts_us = extra_ts_us.get("rpc_owner_dispatch_dequeued_ts_us")
        owner_reply_path_prepare_started_ts_us = extra_ts_us.get(
            "rpc_owner_reply_path_prepare_started_ts_us"
        )
        owner_reply_path_ready_ts_us = extra_ts_us.get("rpc_owner_reply_path_ready_ts_us")
        owner_dispatch_started_ts_us = extra_ts_us.get("rpc_owner_dispatch_started_ts_us")
        owner_dispatch_map_enter_ts_us = extra_ts_us.get("rpc_owner_dispatch_map_enter_ts_us")
        owner_user_rpc_spawn_called_ts_us = extra_ts_us.get(
            "rpc_owner_user_rpc_spawn_called_ts_us"
        )
        owner_dispatch_returned_to_loop_ts_us = extra_ts_us.get(
            "rpc_owner_dispatch_returned_to_loop_ts_us"
        )
        owner_handler_started_ts_us = extra_ts_us.get("rpc_owner_handler_started_ts_us")
        owner_blocking_wait_started_ts_us = extra_ts_us.get(
            "rpc_owner_blocking_wait_started_ts_us"
        )
        owner_blocking_closure_started_ts_us = extra_ts_us.get(
            "rpc_owner_blocking_closure_started_ts_us"
        )
        owner_handler_done_ts_us = extra_ts_us.get("rpc_owner_handler_done_ts_us")
        owner_response_send_enqueued_ts_us = extra_ts_us.get("rpc_owner_response_send_enqueued_ts_us")
        owner1_response_frame_recv_done_ts_us = extra_ts_us.get(
            "rpc_owner1_response_frame_recv_done_ts_us"
        )
        caller_response_frame_recv_done_ts_us = extra_ts_us.get("rpc_caller_response_frame_recv_done_ts_us")
        caller_response_dispatch_started_ts_us = extra_ts_us.get("rpc_caller_response_dispatch_started_ts_us")
        caller_response_complete_pending_call_ts_us = extra_ts_us.get(
            "rpc_caller_response_complete_pending_call_ts_us"
        )
        caller_decode_done_ts_us = extra_ts_us.get("rpc_caller_decode_done_ts_us")
        owner1_roundtrip_us = _cross_process_ts_diff_us(
            owner1_response_frame_recv_done_ts_us,
            owner1_request_send_ts_us,
        )
        if owner1_roundtrip_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_OWNER1_ROUNDTRIP_US] = owner1_roundtrip_us

        caller_post_submit_roundtrip_us = _cross_process_ts_diff_us(
            caller_response_complete_pending_call_ts_us,
            caller_submit_ts_us,
        )
        if caller_post_submit_roundtrip_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_CALLER_POST_SUBMIT_ROUNDTRIP_US] = (
                caller_post_submit_roundtrip_us
            )

        request_to_owner_recv_us = _cross_process_ts_diff_us(
            owner_frame_recv_done_ts_us,
            caller_submit_ts_us,
        )
        if request_to_owner_recv_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_REQUEST_TO_OWNER_RECV_US] = request_to_owner_recv_us

        owner_recv_to_dispatch_send_us = _cross_process_ts_diff_us(
            owner_dispatch_send_started_ts_us,
            owner_frame_recv_done_ts_us,
        )
        if owner_recv_to_dispatch_send_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_OWNER_RECV_TO_DISPATCH_SEND_US] = (
                owner_recv_to_dispatch_send_us
            )

        owner_dispatch_send_to_enqueue_us = _cross_process_ts_diff_us(
            owner_dispatch_enqueued_ts_us,
            owner_dispatch_send_started_ts_us,
        )
        if owner_dispatch_send_to_enqueue_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_SEND_TO_ENQUEUE_US] = (
                owner_dispatch_send_to_enqueue_us
            )

        owner_dispatch_enqueue_to_dequeue_us = _cross_process_ts_diff_us(
            owner_dispatch_dequeued_ts_us,
            owner_dispatch_enqueued_ts_us,
        )
        if owner_dispatch_enqueue_to_dequeue_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_ENQUEUE_TO_DEQUEUE_US] = (
                owner_dispatch_enqueue_to_dequeue_us
            )

        owner_dispatch_send_to_dequeue_us = _cross_process_ts_diff_us(
            owner_dispatch_dequeued_ts_us,
            owner_dispatch_send_started_ts_us,
        )
        if owner_dispatch_send_to_dequeue_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_SEND_TO_DEQUEUE_US] = (
                owner_dispatch_send_to_dequeue_us
            )

        owner_dequeue_to_reply_path_prepare_us = _cross_process_ts_diff_us(
            owner_reply_path_prepare_started_ts_us,
            owner_dispatch_dequeued_ts_us,
        )
        if owner_dequeue_to_reply_path_prepare_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_OWNER_DEQUEUE_TO_REPLY_PATH_PREPARE_US] = (
                owner_dequeue_to_reply_path_prepare_us
            )

        owner_reply_path_prepare_us = _cross_process_ts_diff_us(
            owner_reply_path_ready_ts_us,
            owner_reply_path_prepare_started_ts_us,
        )
        if owner_reply_path_prepare_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_OWNER_REPLY_PATH_PREPARE_US] = (
                owner_reply_path_prepare_us
            )

        owner_reply_path_ready_to_dispatch_us = _cross_process_ts_diff_us(
            owner_dispatch_started_ts_us,
            owner_reply_path_ready_ts_us,
        )
        if owner_reply_path_ready_to_dispatch_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_OWNER_REPLY_PATH_READY_TO_DISPATCH_US] = (
                owner_reply_path_ready_to_dispatch_us
            )

        owner_recv_to_dispatch_us = _cross_process_ts_diff_us(
            owner_dispatch_started_ts_us,
            owner_frame_recv_done_ts_us,
        )
        if owner_recv_to_dispatch_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_OWNER_RECV_TO_DISPATCH_US] = owner_recv_to_dispatch_us

        owner_dispatch_to_map_enter_us = _cross_process_ts_diff_us(
            owner_dispatch_map_enter_ts_us,
            owner_dispatch_started_ts_us,
        )
        if owner_dispatch_to_map_enter_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_TO_MAP_ENTER_US] = (
                owner_dispatch_to_map_enter_us
            )

        owner_map_enter_to_spawn_us = _cross_process_ts_diff_us(
            owner_user_rpc_spawn_called_ts_us,
            owner_dispatch_map_enter_ts_us,
        )
        if owner_map_enter_to_spawn_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_OWNER_MAP_ENTER_TO_SPAWN_US] = (
                owner_map_enter_to_spawn_us
            )

        owner_spawn_to_loop_return_us = _cross_process_ts_diff_us(
            owner_dispatch_returned_to_loop_ts_us,
            owner_user_rpc_spawn_called_ts_us,
        )
        if owner_spawn_to_loop_return_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_OWNER_SPAWN_TO_LOOP_RETURN_US] = (
                owner_spawn_to_loop_return_us
            )

        owner_loop_return_to_task_start_us = _cross_process_ts_diff_us(
            owner_handler_started_ts_us,
            owner_dispatch_returned_to_loop_ts_us,
        )
        if owner_loop_return_to_task_start_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_OWNER_LOOP_RETURN_TO_TASK_START_US] = (
                owner_loop_return_to_task_start_us
            )

        owner_dispatch_to_loop_return_us = _cross_process_ts_diff_us(
            owner_dispatch_returned_to_loop_ts_us,
            owner_dispatch_started_ts_us,
        )
        if owner_dispatch_to_loop_return_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_TO_LOOP_RETURN_US] = (
                owner_dispatch_to_loop_return_us
            )

        owner_dispatch_to_handle_us = _cross_process_ts_diff_us(
            owner_handler_started_ts_us,
            owner_dispatch_started_ts_us,
        )
        if owner_dispatch_to_handle_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_TO_HANDLE_US] = owner_dispatch_to_handle_us

        owner_task_start_to_blocking_submit_us = _cross_process_ts_diff_us(
            owner_blocking_wait_started_ts_us,
            owner_handler_started_ts_us,
        )
        if owner_task_start_to_blocking_submit_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_OWNER_TASK_START_TO_BLOCKING_SUBMIT_US] = (
                owner_task_start_to_blocking_submit_us
            )

        owner_blocking_submit_to_closure_start_us = _cross_process_ts_diff_us(
            owner_blocking_closure_started_ts_us,
            owner_blocking_wait_started_ts_us,
        )
        if owner_blocking_submit_to_closure_start_us is not None:
            segment_sample[
                FLUXON_PHASE_SEGMENT_OWNER_BLOCKING_SUBMIT_TO_CLOSURE_START_US
            ] = owner_blocking_submit_to_closure_start_us

        owner_handle_to_resp_send_us = _cross_process_ts_diff_us(
            owner_response_send_enqueued_ts_us,
            owner_handler_done_ts_us,
        )
        if owner_handle_to_resp_send_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_OWNER_HANDLE_TO_RESP_SEND_US] = owner_handle_to_resp_send_us

        owner_local_service_us = _cross_process_ts_diff_us(
            owner_response_send_enqueued_ts_us,
            owner_frame_recv_done_ts_us,
        )
        if owner_local_service_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_OWNER_LOCAL_SERVICE_US] = owner_local_service_us

        response_send_to_caller_recv_us = _cross_process_ts_diff_us(
            caller_response_frame_recv_done_ts_us,
            owner_response_send_enqueued_ts_us,
        )
        if response_send_to_caller_recv_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_RESPONSE_SEND_TO_CALLER_RECV_US] = (
                response_send_to_caller_recv_us
            )

        caller_recv_to_dispatch_us = _cross_process_ts_diff_us(
            caller_response_dispatch_started_ts_us,
            caller_response_frame_recv_done_ts_us,
        )
        if caller_recv_to_dispatch_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_CALLER_RECV_TO_DISPATCH_US] = caller_recv_to_dispatch_us

        caller_dispatch_to_complete_us = _cross_process_ts_diff_us(
            caller_response_complete_pending_call_ts_us,
            caller_response_dispatch_started_ts_us,
        )
        if caller_dispatch_to_complete_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_CALLER_DISPATCH_TO_COMPLETE_US] = (
                caller_dispatch_to_complete_us
            )

        caller_response_finalize_us = _cross_process_ts_diff_us(
            caller_response_complete_pending_call_ts_us,
            caller_response_frame_recv_done_ts_us,
        )
        if caller_response_finalize_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_CALLER_RESPONSE_FINALIZE_US] = (
                caller_response_finalize_us
            )

        caller_complete_to_decode_us = _cross_process_ts_diff_us(
            caller_decode_done_ts_us,
            caller_response_complete_pending_call_ts_us,
        )
        if caller_complete_to_decode_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_CALLER_COMPLETE_TO_DECODE_US] = (
                caller_complete_to_decode_us
            )

        if (
            caller_post_submit_roundtrip_us is not None
            and owner_local_service_us is not None
            and caller_response_finalize_us is not None
        ):
            segment_sample[FLUXON_PHASE_SEGMENT_TRANSPORT_INFLIGHT_ESTIMATED_US] = max(
                0.0,
                float(caller_post_submit_roundtrip_us)
                - float(owner_local_service_us)
                - float(caller_response_finalize_us),
            )
    return segment_sample


def _fluxon_phase_percentile_us(samples: Sequence[float], percentile: float) -> float:
    if not samples:
        return 0.0
    sorted_samples = sorted(float(sample) for sample in samples)
    idx = min(int(len(sorted_samples) * float(percentile)), len(sorted_samples) - 1)
    return float(sorted_samples[idx])


def _fluxon_phase_segment_stats(samples: Sequence[float]) -> Dict[str, float]:
    if not samples:
        return {
            "count": 0,
            "avg_us": 0.0,
            "p50_us": 0.0,
            "p95_us": 0.0,
            "p99_us": 0.0,
            "max_us": 0.0,
        }
    normalized = [float(sample) for sample in samples]
    count = len(normalized)
    return {
        "count": count,
        "avg_us": sum(normalized) / float(count),
        "p50_us": _fluxon_phase_percentile_us(normalized, 0.50),
        "p95_us": _fluxon_phase_percentile_us(normalized, 0.95),
        "p99_us": _fluxon_phase_percentile_us(normalized, 0.99),
        "max_us": max(normalized),
    }


def _fluxon_error_bucket(error_msg: Optional[str]) -> str:
    outcome = classify_kv_get_result(error_msg)
    if outcome == KVGetResultKind.CACHE_HIT:
        return "ok"
    if outcome == KVGetResultKind.CACHE_MISS:
        return "miss"
    if error_msg is None:
        return "ok"
    if "timed out" in error_msg.lower():
        return "timeout"
    return "error"


def _classify_fluxon_rpc_path_bucket(extra_tags: Mapping[str, str]) -> Optional[str]:
    request_path_kind = extra_tags.get("rpc_request_path_kind")
    response_path_kind = extra_tags.get("rpc_response_path_kind")
    if (
        request_path_kind == FLUXON_RPC_PATH_KIND_FAST
        and response_path_kind == FLUXON_RPC_PATH_KIND_FAST
    ):
        return FLUXON_PHASE_PATH_BUCKET_FAST
    if request_path_kind not in (
        FLUXON_RPC_PATH_KIND_FAST,
        FLUXON_RPC_PATH_KIND_SLOW,
    ):
        return None
    if response_path_kind not in (
        FLUXON_RPC_PATH_KIND_FAST,
        FLUXON_RPC_PATH_KIND_SLOW,
    ):
        return None
    if (
        request_path_kind == FLUXON_RPC_PATH_KIND_SLOW
        or response_path_kind == FLUXON_RPC_PATH_KIND_SLOW
    ):
        return FLUXON_PHASE_PATH_BUCKET_SLOW
    return None


def _classify_fluxon_owner1_roundtrip_path_bucket(
    extra_tags: Mapping[str, str],
) -> Optional[str]:
    owner_path_kind = extra_tags.get(FLUXON_OWNER_PATH_KIND)
    if owner_path_kind == FLUXON_OWNER_PATH_KIND_IPC:
        return FLUXON_PHASE_PATH_BUCKET_IPC
    if owner_path_kind == FLUXON_RPC_PATH_KIND_FAST:
        return FLUXON_PHASE_PATH_BUCKET_FAST
    if owner_path_kind == FLUXON_RPC_PATH_KIND_SLOW:
        return FLUXON_PHASE_PATH_BUCKET_SLOW
    return None


def _fluxon_owner_path_metric_sample_us(
    segment_sample: Mapping[str, float],
    extra_us: Mapping[str, float],
    extra_tags: Mapping[str, str],
) -> Optional[float]:
    owner_path_kind = extra_tags.get(FLUXON_OWNER_PATH_KIND)
    if owner_path_kind == FLUXON_OWNER_PATH_KIND_IPC:
        owner_total_us = extra_us.get("rpc_owner_total_us")
        if owner_total_us is None:
            return None
        # No owner1 relay leg exists in ipc cases, so reuse the local owner service
        # time as the comparable owner-path latency sample for the ipc bucket.
        return max(0.0, float(owner_total_us))
    return segment_sample.get(FLUXON_PHASE_PATH_METRIC_OWNER1_ROUNDTRIP_US)


def _fluxon_segment_metric_sample_us(
    segment_sample: Mapping[str, float],
    metric_name: str,
) -> Optional[float]:
    sample_us = segment_sample.get(metric_name)
    if sample_us is None:
        return None
    return max(0.0, float(sample_us))


def _record_fluxon_path_metric_sample(
    stat: Dict[str, Any],
    metric_name: str,
    path_bucket: str,
    sample_us: Optional[float],
) -> None:
    if sample_us is None:
        return
    sample_value = max(0.0, float(sample_us))
    path_metric_total_us = stat["path_metric_total_us"]
    path_metric_counts = stat["path_metric_counts"]
    path_metric_max_us = stat["path_metric_max_us"]
    window_path_metric_samples = stat["window_path_metric_samples"]

    metric_totals = path_metric_total_us.setdefault(metric_name, {})
    metric_counts = path_metric_counts.setdefault(metric_name, {})
    metric_maxima = path_metric_max_us.setdefault(metric_name, {})
    metric_window_samples = window_path_metric_samples.setdefault(metric_name, {})

    metric_totals[path_bucket] = float(metric_totals.get(path_bucket, 0.0)) + sample_value
    metric_counts[path_bucket] = int(metric_counts.get(path_bucket, 0)) + 1
    metric_maxima[path_bucket] = max(float(metric_maxima.get(path_bucket, 0.0)), sample_value)
    metric_window_samples.setdefault(path_bucket, []).append(sample_value)


class _FluxonPhaseProfiler:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stats: Dict[str, Dict[str, Any]] = {}
        self._phase_summary_callback: Optional[Callable[[Dict[str, Any]], None]] = None

    def set_phase_summary_callback(
        self,
        callback: Optional[Callable[[Dict[str, Any]], None]],
    ) -> None:
        with self._lock:
            self._phase_summary_callback = callback

    @staticmethod
    def _new_stat() -> Dict[str, Any]:
        return {
            "count": 0,
            "submit_total_us": 0.0,
            "wait_total_us": 0.0,
            "finalize_total_us": 0.0,
            "total_total_us": 0.0,
            "extra_total_us": {},
            "segment_total_us": {},
            "segment_counts": {},
            "segment_max_us": {},
            "path_metric_total_us": {},
            "path_metric_counts": {},
            "path_metric_max_us": {},
            "deadline_overrun_count": 0,
            "max_total_us": 0.0,
            "bucket_counts": _empty_fluxon_phase_bucket_counts(),
            "window_count": 0,
            "window_bucket_counts": _empty_fluxon_phase_bucket_counts(),
            "window_deadline_overrun_count": 0,
            "window_segment_samples": {},
            "window_path_metric_samples": {},
        }

    @staticmethod
    def _format_summary_msg(
        *,
        op_name: str,
        count: int,
        stat: Mapping[str, Any],
        window_summary: Optional[Mapping[str, Any]],
    ) -> str:
        extra_total_us = stat["extra_total_us"]
        extra_avg_parts = []
        for phase_name in sorted(extra_total_us):
            avg_value = float(extra_total_us[phase_name]) / count
            if phase_name.endswith("_us"):
                extra_avg_parts.append(f"{phase_name[:-3]}_avg_us={avg_value:.1f}")
            else:
                extra_avg_parts.append(f"{phase_name}_avg={avg_value:.1f}")
        summary_msg = (
            f"fluxon_phase_summary op={op_name} count={count} "
            f"submit_avg_us={float(stat['submit_total_us']) / count:.1f} "
            f"wait_avg_us={float(stat['wait_total_us']) / count:.1f} "
            f"finalize_avg_us={float(stat['finalize_total_us']) / count:.1f} "
            f"total_avg_us={float(stat['total_total_us']) / count:.1f} "
            f"ok={stat['bucket_counts']['ok']} miss={stat['bucket_counts']['miss']} "
            f"timeout={stat['bucket_counts']['timeout']} err={stat['bucket_counts']['error']} "
            f"deadline_overrun={stat['deadline_overrun_count']} "
            f"max_total_us={float(stat['max_total_us']):.1f}"
        )
        if extra_avg_parts:
            summary_msg = f"{summary_msg} {' '.join(extra_avg_parts)}"
        if window_summary is not None:
            segment_stats = window_summary.get("segment_stats", {})
            segment_parts: List[str] = []
            if isinstance(segment_stats, dict):
                for phase_name, phase_stats in sorted(segment_stats.items()):
                    if not isinstance(phase_stats, dict):
                        continue
                    phase_label = phase_name[:-3] if phase_name.endswith("_us") else phase_name
                    segment_parts.append(
                        f"{phase_label}_avg_us={float(phase_stats.get('avg_us', 0.0)):.1f} "
                        f"{phase_label}_p99_us={float(phase_stats.get('p99_us', 0.0)):.1f}"
                    )
            if segment_parts:
                summary_msg = f"{summary_msg} {' '.join(segment_parts)}"
            path_metric_stats = window_summary.get("path_metric_stats", {})
            path_metric_parts: List[str] = []
            if isinstance(path_metric_stats, dict):
                for metric_name, bucket_stats in sorted(path_metric_stats.items()):
                    if not isinstance(bucket_stats, dict):
                        continue
                    metric_label = metric_name[:-3] if metric_name.endswith("_us") else metric_name
                    for path_bucket, phase_stats in sorted(bucket_stats.items()):
                        if not isinstance(phase_stats, dict):
                            continue
                        path_metric_parts.append(
                            f"{metric_label}_{path_bucket}_avg_us={float(phase_stats.get('avg_us', 0.0)):.1f} "
                            f"{metric_label}_{path_bucket}_p99_us={float(phase_stats.get('p99_us', 0.0)):.1f}"
                        )
            if path_metric_parts:
                summary_msg = f"{summary_msg} {' '.join(path_metric_parts)}"
        return summary_msg

    @staticmethod
    def _flush_window_locked(op_name: str, stat: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        window_count = int(stat["window_count"])
        window_segment_samples = stat["window_segment_samples"]
        window_segment_stats: Dict[str, Dict[str, float]] = {}
        if isinstance(window_segment_samples, dict):
            for phase_name, samples in sorted(window_segment_samples.items()):
                if not isinstance(samples, list) or not samples:
                    continue
                window_segment_stats[str(phase_name)] = _fluxon_phase_segment_stats(samples)
        window_path_metric_samples = stat["window_path_metric_samples"]
        window_path_metric_stats: Dict[str, Dict[str, Dict[str, float]]] = {}
        if isinstance(window_path_metric_samples, dict):
            for metric_name, bucket_samples in sorted(window_path_metric_samples.items()):
                if not isinstance(bucket_samples, dict):
                    continue
                bucket_stats: Dict[str, Dict[str, float]] = {}
                for path_bucket, samples in sorted(bucket_samples.items()):
                    if not isinstance(samples, list) or not samples:
                        continue
                    bucket_stats[str(path_bucket)] = _fluxon_phase_segment_stats(samples)
                if bucket_stats:
                    window_path_metric_stats[str(metric_name)] = bucket_stats
        summary_payload: Optional[Dict[str, Any]] = None
        if window_count > 0 and (window_segment_stats or window_path_metric_stats):
            summary_payload = {
                "summary_kind": "window",
                "op_name": str(op_name),
                "window_count": window_count,
                "total_count": int(stat["count"]),
                "bucket_counts": copy.deepcopy(stat["window_bucket_counts"]),
                "deadline_overrun_count": int(stat["window_deadline_overrun_count"]),
                "segment_stats": window_segment_stats,
                "path_metric_stats": window_path_metric_stats,
            }
        stat["window_count"] = 0
        stat["window_bucket_counts"] = _empty_fluxon_phase_bucket_counts()
        stat["window_deadline_overrun_count"] = 0
        stat["window_segment_samples"] = {}
        stat["window_path_metric_samples"] = {}
        return summary_payload

    def record(
        self,
        *,
        op_name: str,
        key: str,
        sample: _FluxonPhaseSample,
        error_msg: Optional[str],
    ) -> None:
        bucket = _fluxon_error_bucket(error_msg)
        slow = sample.total_us >= FLUXON_PHASE_SLOW_OP_THRESHOLD_US or sample.deadline_overrun_us > 0.0
        segment_sample = _build_fluxon_phase_segment_sample(sample.extra_us, sample.extra_ts_us)
        rpc_path_bucket = _classify_fluxon_rpc_path_bucket(sample.extra_tags)
        owner1_roundtrip_path_bucket = _classify_fluxon_owner1_roundtrip_path_bucket(
            sample.extra_tags
        )
        phase_summary_callback: Optional[Callable[[Dict[str, Any]], None]] = None
        phase_window_summary: Optional[Dict[str, Any]] = None
        summary_msg: Optional[str] = None
        with self._lock:
            stat = self._stats.setdefault(
                op_name,
                self._new_stat(),
            )
            stat["count"] += 1
            stat["submit_total_us"] += sample.submit_us
            stat["wait_total_us"] += sample.wait_us
            stat["finalize_total_us"] += sample.finalize_us
            stat["total_total_us"] += sample.total_us
            stat["max_total_us"] = max(float(stat["max_total_us"]), sample.total_us)
            extra_total_us = stat["extra_total_us"]
            for phase_name, phase_us in sample.extra_us.items():
                extra_total_us[phase_name] = float(extra_total_us.get(phase_name, 0.0)) + float(phase_us)
            if segment_sample:
                segment_total_us = stat["segment_total_us"]
                segment_counts = stat["segment_counts"]
                segment_max_us = stat["segment_max_us"]
                window_segment_samples = stat["window_segment_samples"]
                for phase_name, phase_us in segment_sample.items():
                    segment_total_us[phase_name] = float(segment_total_us.get(phase_name, 0.0)) + float(phase_us)
                    segment_counts[phase_name] = int(segment_counts.get(phase_name, 0)) + 1
                    segment_max_us[phase_name] = max(float(segment_max_us.get(phase_name, 0.0)), float(phase_us))
                    phase_samples = window_segment_samples.setdefault(phase_name, [])
                    phase_samples.append(float(phase_us))
            if rpc_path_bucket is not None:
                _record_fluxon_path_metric_sample(
                    stat,
                    FLUXON_PHASE_PATH_METRIC_RPC_EXT_TOTAL_US,
                    rpc_path_bucket,
                    sample.extra_us.get(FLUXON_PHASE_PATH_METRIC_RPC_EXT_TOTAL_US),
                )
                _record_fluxon_path_metric_sample(
                    stat,
                    FLUXON_PHASE_PATH_METRIC_CALLER_POST_SUBMIT_ROUNDTRIP_US,
                    rpc_path_bucket,
                    _fluxon_segment_metric_sample_us(
                        segment_sample,
                        FLUXON_PHASE_SEGMENT_CALLER_POST_SUBMIT_ROUNDTRIP_US,
                    ),
                )
                _record_fluxon_path_metric_sample(
                    stat,
                    FLUXON_PHASE_PATH_METRIC_TRANSPORT_INFLIGHT_ESTIMATED_US,
                    rpc_path_bucket,
                    _fluxon_segment_metric_sample_us(
                        segment_sample,
                        FLUXON_PHASE_SEGMENT_TRANSPORT_INFLIGHT_ESTIMATED_US,
                    ),
                )
            if owner1_roundtrip_path_bucket is not None:
                _record_fluxon_path_metric_sample(
                    stat,
                    FLUXON_PHASE_PATH_METRIC_OWNER1_ROUNDTRIP_US,
                    owner1_roundtrip_path_bucket,
                    _fluxon_owner_path_metric_sample_us(
                        segment_sample,
                        sample.extra_us,
                        sample.extra_tags,
                    ),
                )
                _record_fluxon_path_metric_sample(
                    stat,
                    FLUXON_PHASE_PATH_METRIC_OWNER_LOCAL_SERVICE_US,
                    owner1_roundtrip_path_bucket,
                    _fluxon_segment_metric_sample_us(
                        segment_sample,
                        FLUXON_PHASE_SEGMENT_OWNER_LOCAL_SERVICE_US,
                    ),
                )
                _record_fluxon_path_metric_sample(
                    stat,
                    FLUXON_PHASE_PATH_METRIC_OWNER_RECV_TO_DISPATCH_SEND_US,
                    owner1_roundtrip_path_bucket,
                    _fluxon_segment_metric_sample_us(
                        segment_sample,
                        FLUXON_PHASE_SEGMENT_OWNER_RECV_TO_DISPATCH_SEND_US,
                    ),
                )
                _record_fluxon_path_metric_sample(
                    stat,
                    FLUXON_PHASE_PATH_METRIC_OWNER_DISPATCH_SEND_TO_ENQUEUE_US,
                    owner1_roundtrip_path_bucket,
                    _fluxon_segment_metric_sample_us(
                        segment_sample,
                        FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_SEND_TO_ENQUEUE_US,
                    ),
                )
                _record_fluxon_path_metric_sample(
                    stat,
                    FLUXON_PHASE_PATH_METRIC_OWNER_DISPATCH_ENQUEUE_TO_DEQUEUE_US,
                    owner1_roundtrip_path_bucket,
                    _fluxon_segment_metric_sample_us(
                        segment_sample,
                        FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_ENQUEUE_TO_DEQUEUE_US,
                    ),
                )
                _record_fluxon_path_metric_sample(
                    stat,
                    FLUXON_PHASE_PATH_METRIC_OWNER_DISPATCH_SEND_TO_DEQUEUE_US,
                    owner1_roundtrip_path_bucket,
                    _fluxon_segment_metric_sample_us(
                        segment_sample,
                        FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_SEND_TO_DEQUEUE_US,
                    ),
                )
                _record_fluxon_path_metric_sample(
                    stat,
                    FLUXON_PHASE_PATH_METRIC_OWNER_DEQUEUE_TO_REPLY_PATH_PREPARE_US,
                    owner1_roundtrip_path_bucket,
                    _fluxon_segment_metric_sample_us(
                        segment_sample,
                        FLUXON_PHASE_SEGMENT_OWNER_DEQUEUE_TO_REPLY_PATH_PREPARE_US,
                    ),
                )
                _record_fluxon_path_metric_sample(
                    stat,
                    FLUXON_PHASE_PATH_METRIC_OWNER_REPLY_PATH_PREPARE_US,
                    owner1_roundtrip_path_bucket,
                    _fluxon_segment_metric_sample_us(
                        segment_sample,
                        FLUXON_PHASE_SEGMENT_OWNER_REPLY_PATH_PREPARE_US,
                    ),
                )
                _record_fluxon_path_metric_sample(
                    stat,
                    FLUXON_PHASE_PATH_METRIC_OWNER_REPLY_PATH_READY_TO_DISPATCH_US,
                    owner1_roundtrip_path_bucket,
                    _fluxon_segment_metric_sample_us(
                        segment_sample,
                        FLUXON_PHASE_SEGMENT_OWNER_REPLY_PATH_READY_TO_DISPATCH_US,
                    ),
                )
                _record_fluxon_path_metric_sample(
                    stat,
                    FLUXON_PHASE_PATH_METRIC_OWNER_DISPATCH_TO_MAP_ENTER_US,
                    owner1_roundtrip_path_bucket,
                    _fluxon_segment_metric_sample_us(
                        segment_sample,
                        FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_TO_MAP_ENTER_US,
                    ),
                )
                _record_fluxon_path_metric_sample(
                    stat,
                    FLUXON_PHASE_PATH_METRIC_OWNER_MAP_ENTER_TO_SPAWN_US,
                    owner1_roundtrip_path_bucket,
                    _fluxon_segment_metric_sample_us(
                        segment_sample,
                        FLUXON_PHASE_SEGMENT_OWNER_MAP_ENTER_TO_SPAWN_US,
                    ),
                )
                _record_fluxon_path_metric_sample(
                    stat,
                    FLUXON_PHASE_PATH_METRIC_OWNER_SPAWN_TO_LOOP_RETURN_US,
                    owner1_roundtrip_path_bucket,
                    _fluxon_segment_metric_sample_us(
                        segment_sample,
                        FLUXON_PHASE_SEGMENT_OWNER_SPAWN_TO_LOOP_RETURN_US,
                    ),
                )
                _record_fluxon_path_metric_sample(
                    stat,
                    FLUXON_PHASE_PATH_METRIC_OWNER_DISPATCH_TO_LOOP_RETURN_US,
                    owner1_roundtrip_path_bucket,
                    _fluxon_segment_metric_sample_us(
                        segment_sample,
                        FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_TO_LOOP_RETURN_US,
                    ),
                )
                _record_fluxon_path_metric_sample(
                    stat,
                    FLUXON_PHASE_PATH_METRIC_OWNER_DISPATCH_TO_HANDLE_US,
                    owner1_roundtrip_path_bucket,
                    _fluxon_segment_metric_sample_us(
                        segment_sample,
                        FLUXON_PHASE_SEGMENT_OWNER_DISPATCH_TO_HANDLE_US,
                    ),
                )
                _record_fluxon_path_metric_sample(
                    stat,
                    FLUXON_PHASE_PATH_METRIC_OWNER_HANDLE_TO_RESP_SEND_US,
                    owner1_roundtrip_path_bucket,
                    _fluxon_segment_metric_sample_us(
                        segment_sample,
                        FLUXON_PHASE_SEGMENT_OWNER_HANDLE_TO_RESP_SEND_US,
                    ),
                )
            if sample.deadline_overrun_us > 0.0:
                stat["deadline_overrun_count"] += 1
                stat["window_deadline_overrun_count"] += 1
            stat["bucket_counts"][bucket] += 1
            stat["window_count"] += 1
            stat["window_bucket_counts"][bucket] += 1
            count = int(stat["count"])
            if count % FLUXON_PHASE_LOG_INTERVAL_OPS == 0:
                phase_window_summary = self._flush_window_locked(op_name, stat)
                phase_summary_callback = self._phase_summary_callback
                summary_msg = self._format_summary_msg(
                    op_name=op_name,
                    count=count,
                    stat=stat,
                    window_summary=phase_window_summary,
                )
        if summary_msg is not None:
            _bench_kv_print(summary_msg)
        if phase_summary_callback is not None and phase_window_summary is not None:
            phase_summary_callback(phase_window_summary)
        if slow:
            extra_detail_map = dict(sample.extra_us)
            extra_detail_map.update(segment_sample)
            extra_detail = " ".join(
                f"{phase_name}={phase_us:.1f}"
                for phase_name, phase_us in sorted(extra_detail_map.items())
            )
            ts_detail = ""
            if sample.extra_ts_us:
                ts_detail = " " + " ".join(
                    f"{ts_name}={ts_value:.1f}"
                    for ts_name, ts_value in sorted(sample.extra_ts_us.items())
                )
            path_detail = ""
            if sample.extra_tags:
                path_detail = " " + " ".join(
                    f"{tag_name}={tag_value}"
                    for tag_name, tag_value in sorted(sample.extra_tags.items())
                )
            _bench_kv_print(
                f"fluxon_phase_slow op={op_name} key={key!r} "
                f"submit_us={sample.submit_us:.1f} wait_us={sample.wait_us:.1f} "
                f"finalize_us={sample.finalize_us:.1f} total_us={sample.total_us:.1f} "
                f"deadline_overrun_us={sample.deadline_overrun_us:.1f} "
                f"bucket={bucket} err={error_msg!r}"
                f"{path_detail}"
                f"{ts_detail}"
                f"{(' ' + extra_detail) if extra_detail else ''}"
            )

    def flush_pending(self) -> None:
        phase_window_summaries: List[Dict[str, Any]] = []
        phase_summary_callback: Optional[Callable[[Dict[str, Any]], None]] = None
        with self._lock:
            phase_summary_callback = self._phase_summary_callback
            for op_name, stat in sorted(self._stats.items()):
                summary = self._flush_window_locked(op_name, stat)
                if summary is not None:
                    phase_window_summaries.append(summary)
        if phase_summary_callback is None:
            return
        for summary in phase_window_summaries:
            phase_summary_callback(summary)

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            raw_stats = copy.deepcopy(self._stats)

        out: Dict[str, Dict[str, Any]] = {}
        for op_name, stat in sorted(raw_stats.items()):
            count = int(stat.get("count", 0))
            if count <= 0:
                continue
            extra_totals = stat.get("extra_total_us", {})
            extra_avg_us: Dict[str, float] = {}
            if isinstance(extra_totals, dict):
                for phase_name, phase_total_us in sorted(extra_totals.items()):
                    extra_avg_us[str(phase_name)] = float(phase_total_us) / float(count)
            segment_totals = stat.get("segment_total_us", {})
            segment_counts_raw = stat.get("segment_counts", {})
            segment_max_raw = stat.get("segment_max_us", {})
            path_metric_totals_raw = stat.get("path_metric_total_us", {})
            path_metric_counts_raw = stat.get("path_metric_counts", {})
            path_metric_max_raw = stat.get("path_metric_max_us", {})
            segment_avg_us: Dict[str, float] = {}
            segment_counts: Dict[str, int] = {}
            segment_max_us: Dict[str, float] = {}
            for phase_name in FLUXON_PHASE_SEGMENT_NAMES:
                segment_count = int(segment_counts_raw.get(phase_name, 0))
                segment_counts[phase_name] = segment_count
                segment_max_us[phase_name] = float(segment_max_raw.get(phase_name, 0.0))
                if segment_count > 0:
                    segment_avg_us[phase_name] = float(segment_totals.get(phase_name, 0.0)) / float(segment_count)
            path_metric_avg_us: Dict[str, Dict[str, float]] = {}
            path_metric_counts: Dict[str, Dict[str, int]] = {}
            path_metric_max_us: Dict[str, Dict[str, float]] = {}
            for metric_name in FLUXON_PHASE_PATH_METRIC_NAMES:
                metric_counts_raw = path_metric_counts_raw.get(metric_name, {})
                metric_totals_raw = path_metric_totals_raw.get(metric_name, {})
                metric_maxima_raw = path_metric_max_raw.get(metric_name, {})
                metric_avg_entry: Dict[str, float] = {}
                metric_count_entry: Dict[str, int] = {}
                metric_max_entry: Dict[str, float] = {}
                for path_bucket in FLUXON_PHASE_PATH_BUCKET_NAMES:
                    metric_count = 0
                    if isinstance(metric_counts_raw, dict):
                        metric_count = int(metric_counts_raw.get(path_bucket, 0))
                    metric_count_entry[path_bucket] = metric_count
                    metric_max_value = 0.0
                    if isinstance(metric_maxima_raw, dict):
                        metric_max_value = float(metric_maxima_raw.get(path_bucket, 0.0))
                    metric_max_entry[path_bucket] = metric_max_value
                    if metric_count > 0 and isinstance(metric_totals_raw, dict):
                        metric_avg_entry[path_bucket] = (
                            float(metric_totals_raw.get(path_bucket, 0.0)) / float(metric_count)
                        )
                path_metric_avg_us[metric_name] = metric_avg_entry
                path_metric_counts[metric_name] = metric_count_entry
                path_metric_max_us[metric_name] = metric_max_entry
            bucket_counts_raw = stat.get("bucket_counts", {})
            bucket_counts = {
                "ok": int(bucket_counts_raw.get("ok", 0)),
                "miss": int(bucket_counts_raw.get("miss", 0)),
                "timeout": int(bucket_counts_raw.get("timeout", 0)),
                "error": int(bucket_counts_raw.get("error", 0)),
            }
            out[str(op_name)] = {
                "count": count,
                "submit_avg_us": float(stat.get("submit_total_us", 0.0)) / float(count),
                "wait_avg_us": float(stat.get("wait_total_us", 0.0)) / float(count),
                "finalize_avg_us": float(stat.get("finalize_total_us", 0.0)) / float(count),
                "total_avg_us": float(stat.get("total_total_us", 0.0)) / float(count),
                "max_total_us": float(stat.get("max_total_us", 0.0)),
                "deadline_overrun_count": int(stat.get("deadline_overrun_count", 0)),
                "bucket_counts": bucket_counts,
                "extra_avg_us": extra_avg_us,
                "segment_avg_us": segment_avg_us,
                "segment_max_us": segment_max_us,
                "segment_counts": segment_counts,
                "path_metric_avg_us": path_metric_avg_us,
                "path_metric_max_us": path_metric_max_us,
                "path_metric_counts": path_metric_counts,
            }
        return out


@dataclass(frozen=True)
class _RedisEndpoint:
    host: str
    port: int


@dataclass
class _RedisConn:
    sock: socket.socket
    reader: Any


class _NoopBenchmarkStore:
    def __init__(self, backend_kind: str) -> None:
        self.backend_kind = str(backend_kind).upper()

    def put_blocking(
        self,
        key: str,
        payload: bytes,
        *,
        deadline_ts: float,
        ctx: str,
    ) -> Optional[str]:
        _ = key
        _ = payload
        _ = deadline_ts
        _ = ctx
        return f"PUT failed: backend {self.backend_kind} does not expose KV operations"

    def get_blocking(
        self,
        key: str,
        *,
        deadline_ts: float,
        ctx: str,
    ) -> Optional[str]:
        _ = key
        _ = deadline_ts
        _ = ctx
        return f"GET failed: backend {self.backend_kind} does not expose KV operations"

    def close(self) -> _SimpleResult:
        return _SimpleResult.ok(None)


class RedisShardClient:
    def __init__(
        self,
        *,
        endpoints: Sequence[_RedisEndpoint],
        connect_timeout_seconds: float,
        socket_timeout_seconds: float,
        database: int,
        password: Optional[str],
    ) -> None:
        if not endpoints:
            raise ValueError("redis endpoints must be non-empty")
        self._endpoints = tuple(endpoints)
        self._connect_timeout_seconds = float(connect_timeout_seconds)
        self._socket_timeout_seconds = float(socket_timeout_seconds)
        self._database = int(database)
        self._password = password
        self._lock = threading.Lock()
        self._closed = False
        self._connections: Dict[tuple[int, int], _RedisConn] = {}

    def _connection_key(self, endpoint_index: int) -> tuple[int, int]:
        return (threading.get_ident(), int(endpoint_index))

    def _endpoint_index_for_key(self, key: str) -> int:
        digest = hashlib.sha256(key.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big") % len(self._endpoints)

    def _read_line(self, reader: Any) -> bytes:
        line = reader.readline()
        if not line:
            raise RuntimeError("redis connection closed while reading line")
        if not line.endswith(b"\r\n"):
            raise RuntimeError(f"redis protocol line missing CRLF suffix: {line!r}")
        return line[:-2]

    def _read_reply(self, reader: Any) -> Any:
        prefix = reader.read(1)
        if not prefix:
            raise RuntimeError("redis connection closed while reading reply prefix")
        if prefix == b"+":
            return self._read_line(reader).decode("utf-8", errors="replace")
        if prefix == b"-":
            raise RuntimeError(self._read_line(reader).decode("utf-8", errors="replace"))
        if prefix == b":":
            return int(self._read_line(reader))
        if prefix == b"$":
            length = int(self._read_line(reader))
            if length < 0:
                return None
            payload = reader.read(length)
            if len(payload) != length:
                raise RuntimeError("redis bulk reply truncated")
            suffix = reader.read(2)
            if suffix != b"\r\n":
                raise RuntimeError("redis bulk reply missing CRLF suffix")
            return payload
        raise RuntimeError(f"unsupported redis reply prefix: {prefix!r}")

    def _send_command(self, conn: _RedisConn, *parts: Union[str, bytes]) -> Any:
        encoded_parts = []
        for part in parts:
            if isinstance(part, bytes):
                encoded_parts.append(part)
            else:
                encoded_parts.append(str(part).encode("utf-8"))
        payload = [f"*{len(encoded_parts)}\r\n".encode("ascii")]
        for part in encoded_parts:
            payload.append(f"${len(part)}\r\n".encode("ascii"))
            payload.append(part)
            payload.append(b"\r\n")
        conn.sock.sendall(b"".join(payload))
        return self._read_reply(conn.reader)

    def _open_connection(self, endpoint_index: int) -> _RedisConn:
        endpoint = self._endpoints[endpoint_index]
        sock = socket.create_connection(
            (endpoint.host, int(endpoint.port)),
            timeout=self._connect_timeout_seconds,
        )
        sock.settimeout(self._socket_timeout_seconds)
        reader = sock.makefile("rb")
        conn = _RedisConn(sock=sock, reader=reader)
        try:
            if self._password is not None and self._password != "":
                auth_reply = self._send_command(conn, "AUTH", self._password)
                if auth_reply != "OK":
                    raise RuntimeError(f"redis AUTH failed: reply={auth_reply!r}")
            if self._database > 0:
                select_reply = self._send_command(conn, "SELECT", str(self._database))
                if select_reply != "OK":
                    raise RuntimeError(f"redis SELECT failed: reply={select_reply!r}")
            return conn
        except Exception:
            try:
                reader.close()
            finally:
                sock.close()
            raise

    def _close_connection(self, endpoint_index: int, *, suppress_errors: bool = True) -> None:
        key = self._connection_key(endpoint_index)
        conn: Optional[_RedisConn] = None
        with self._lock:
            conn = self._connections.pop(key, None)
        if conn is None:
            return
        try:
            conn.reader.close()
        except Exception:
            if not suppress_errors:
                raise
        try:
            conn.sock.close()
        except Exception:
            if not suppress_errors:
                raise

    def _connection(self, endpoint_index: int) -> _RedisConn:
        if self._closed:
            raise RuntimeError("redis benchmark client is already closed")
        key = self._connection_key(endpoint_index)
        with self._lock:
            existing = self._connections.get(key)
        if existing is not None:
            return existing
        conn = self._open_connection(endpoint_index)
        with self._lock:
            if self._closed:
                try:
                    conn.reader.close()
                finally:
                    conn.sock.close()
                raise RuntimeError("redis benchmark client is already closed")
            prev = self._connections.setdefault(key, conn)
        if prev is not conn:
            conn.reader.close()
            conn.sock.close()
            return prev
        return conn

    def _send_command_on_endpoint(
        self,
        endpoint_index: int,
        *parts: Union[str, bytes],
    ) -> Any:
        try:
            return self._send_command(self._connection(endpoint_index), *parts)
        except Exception:
            self._close_connection(endpoint_index)
            raise

    def _inflight_guard_key(self, key: str) -> str:
        return f"{REDIS_BENCH_INFLIGHT_GUARD_PREFIX}:{key}"

    def _try_acquire_inflight_guard(
        self,
        *,
        endpoint_index: int,
        key: str,
        deadline_ts: float,
    ) -> tuple[bool, str, str]:
        guard_key = self._inflight_guard_key(key)
        guard_token = f"{os.getpid()}:{threading.get_ident()}:{time.time_ns()}"
        ttl_ms = max(1, int((float(deadline_ts) - time.time()) * 1000.0))
        reply = self._send_command_on_endpoint(
            endpoint_index,
            "SET",
            guard_key,
            guard_token,
            "NX",
            "PX",
            str(ttl_ms),
        )
        if reply is None:
            return False, guard_key, guard_token
        if reply != "OK":
            raise RuntimeError(f"redis same-key inflight guard SET returned unexpected reply: {reply!r}")
        return True, guard_key, guard_token

    def _release_inflight_guard(
        self,
        *,
        endpoint_index: int,
        guard_key: str,
        guard_token: str,
    ) -> None:
        reply = self._send_command_on_endpoint(
            endpoint_index,
            "EVAL",
            (
                "if redis.call('GET', KEYS[1]) == ARGV[1] then "
                "return redis.call('DEL', KEYS[1]) "
                "else return 0 end"
            ),
            "1",
            guard_key,
            guard_token,
        )
        if not isinstance(reply, int):
            raise RuntimeError(f"redis same-key inflight guard release returned unexpected reply: {reply!r}")

    def put(self, key: str, payload: bytes) -> None:
        endpoint_index = self._endpoint_index_for_key(key)
        try:
            reply = self._send_command(self._connection(endpoint_index), "SET", key, payload)
            if reply != "OK":
                raise RuntimeError(f"redis SET returned unexpected reply: {reply!r}")
        except Exception:
            self._close_connection(endpoint_index)
            raise

    def get(self, key: str) -> Optional[bytes]:
        endpoint_index = self._endpoint_index_for_key(key)
        try:
            reply = self._send_command(self._connection(endpoint_index), "GET", key)
        except Exception:
            self._close_connection(endpoint_index)
            raise
        if reply is None:
            return None
        if not isinstance(reply, (bytes, bytearray)):
            raise RuntimeError(f"redis GET returned unexpected reply type: {type(reply)}")
        return bytes(reply)

    def put_blocking(
        self,
        key: str,
        payload: bytes,
        *,
        deadline_ts: float,
        ctx: str,
    ) -> Optional[str]:
        endpoint_index = self._endpoint_index_for_key(key)
        guard_key = ""
        guard_token = ""
        guard_acquired = False
        try:
            # Keep guard acquire and data write as separate Redis commands so concurrent writers
            # can observe the inflight window and receive same-key rejection semantics.
            guard_acquired, guard_key, guard_token = self._try_acquire_inflight_guard(
                endpoint_index=endpoint_index,
                key=key,
                deadline_ts=deadline_ts,
            )
            if not guard_acquired:
                _bench_kv_print(
                    f"{ctx} PUT compat-success key={key!r} reason=same-key inflight write (redis guard)",
                    verbose_only=True,
                )
                return None
            reply = self._send_command_on_endpoint(endpoint_index, "SET", key, payload)
            if reply != "OK":
                raise RuntimeError(f"redis SET returned unexpected reply: {reply!r}")
            return None
        except Exception as exc:
            return f"PUT failed: {exc}"
        finally:
            if guard_acquired:
                try:
                    self._release_inflight_guard(
                        endpoint_index=endpoint_index,
                        guard_key=guard_key,
                        guard_token=guard_token,
                    )
                except Exception as exc:
                    _bench_kv_print(
                        f"{ctx} PUT redis guard release failed key={key!r} err={exc}",
                    )

    def get_blocking(
        self,
        key: str,
        *,
        deadline_ts: float,
        ctx: str,
    ) -> Optional[str]:
        _ = deadline_ts
        _ = ctx
        try:
            payload = self.get(key)
            if payload is None:
                return KV_GET_MISS_ERROR
            return None
        except Exception as exc:
            return normalize_kv_get_error(f"GET failed: {exc}")

    def close(self) -> _SimpleResult:
        with self._lock:
            if self._closed:
                return _SimpleResult.ok(None)
            self._closed = True
            items = list(self._connections.items())
            self._connections.clear()
        for _, conn in items:
            try:
                conn.reader.close()
            except Exception:
                pass
            try:
                conn.sock.close()
            except Exception:
                pass
        return _SimpleResult.ok(None)


def _bench_kv_print(msg: str, *, verbose_only: bool = False) -> None:
    if verbose_only and not KV_VERBOSE_PER_OP_LOG:
        return
    print(f"[BENCH-KV] {msg}", flush=True)


def _sanitize_benchmark_client_kvcache_config(kvcache_config: dict[str, Any]) -> dict[str, Any]:
    sanitized = copy.deepcopy(kvcache_config)
    backend_kind = str(sanitized.get("backend_kind", "")).strip().upper()
    stripped_root_keys: list[str] = []
    if backend_kind == "MOONCAKE" and "backend_kind" in sanitized:
        sanitized.pop("backend_kind", None)
        stripped_root_keys.append("backend_kind")
    test_spec_config = sanitized.get("test_spec_config")
    stripped_test_spec_keys: list[str] = []
    if isinstance(test_spec_config, dict):
        stripped_test_spec_keys = [key for key in _BENCHMARK_CLIENT_STRIP_TEST_SPEC_KEYS if key in test_spec_config]
        if stripped_test_spec_keys:
            sanitized_test_spec = dict(test_spec_config)
            for key in stripped_test_spec_keys:
                sanitized_test_spec.pop(key, None)
            sanitized["test_spec_config"] = sanitized_test_spec
    if stripped_root_keys or stripped_test_spec_keys:
        parts: list[str] = []
        if stripped_root_keys:
            parts.append("root keys: " + ", ".join(stripped_root_keys))
        if stripped_test_spec_keys:
            parts.append("owner-only test_spec_config keys: " + ", ".join(stripped_test_spec_keys))
        _bench_kv_print("stripped benchmark client config fields for runtime compatibility: " + "; ".join(parts))
    return sanitized


class FluxonBlockingStore:
    def __init__(self, store: KvClient) -> None:
        self.backend_kind = BACKEND_KIND_FLUXON
        self._store = store
        self._phase_profiler = _FluxonPhaseProfiler()

    @property
    def kv_client(self) -> KvClient:
        """Return the uniquely owned client for MQ and RPC control paths."""

        return self._store

    def put_blocking(
        self,
        key: str,
        payload: bytes,
        *,
        deadline_ts: float,
        ctx: str,
    ) -> Optional[str]:
        try:
            _bench_kv_print(f"{ctx} PUT begin key={key!r} payload_size={len(payload)}", verbose_only=True)
            started_at = time.perf_counter()
            result = self._store.put_blocking(
                key,
                {"payload": payload},
                opts=PutOptionalArgs(reject_if_inflight_same_key=True),
            )
            done_at = time.perf_counter()
            wall_done_ts = time.time()
            err: Optional[str] = None
            compat_success = False
            if not result.is_ok():
                put_error = result.unwrap_error()
                if _is_put_compat_success_error(put_error):
                    _bench_kv_print(
                        f"{ctx} PUT compat-success key={key!r} reason={type(put_error).__name__}",
                        verbose_only=True,
                    )
                    put_error = None
                    compat_success = True
                if put_error is not None:
                    err = f"PUT failed: {put_error}"
            else:
                # Fluxon's Python Result must be explicitly consumed on the success path as well,
                # otherwise its destructor raises an assertion and pollutes benchmark logs/CPU.
                _ = result.unwrap()
                if wall_done_ts > deadline_ts:
                    err = (
                        f"PUT timed out after blocking wait: deadline_ts={deadline_ts:.3f} "
                        f"now_ts={wall_done_ts:.3f} now_ms={wall_done_ts * 1000.0:.1f}"
                    )
            if compat_success and wall_done_ts > deadline_ts:
                err = (
                    f"PUT timed out after compatibility success: deadline_ts={deadline_ts:.3f} "
                    f"now_ts={wall_done_ts:.3f} now_ms={wall_done_ts * 1000.0:.1f}"
                )
            phase_sample = _build_fluxon_sync_phase_sample(
                started_at=started_at,
                done_at=done_at,
                deadline_ts=deadline_ts,
                wall_done_ts=wall_done_ts,
            )
            self._phase_profiler.record(op_name="PUT", key=key, sample=phase_sample, error_msg=err)
            if err is not None:
                _bench_kv_print(f"{ctx} PUT failed-after-block key={key!r} err={err}")
                return err
            _bench_kv_print(f"{ctx} PUT done key={key!r}", verbose_only=True)
            return None
        except Exception as exc:
            _bench_kv_print(f"{ctx} PUT exception key={key!r} err={exc}")
            return f"PUT exception: {exc}"

    def get_blocking(
        self,
        key: str,
        *,
        deadline_ts: float,
        ctx: str,
    ) -> Optional[str]:
        try:
            _bench_kv_print(f"{ctx} GET begin key={key!r}", verbose_only=True)
            started_at = time.perf_counter()
            result = self._store.get_blocking(key)
            done_at = time.perf_counter()
            wall_done_ts = time.time()
            err: Optional[str] = None
            if not result.is_ok():
                err = normalize_kv_get_error(f"GET failed: {result.unwrap_error()}")
            else:
                _ = result.unwrap()
                if wall_done_ts > deadline_ts:
                    err = (
                        f"GET timed out after blocking wait: deadline_ts={deadline_ts:.3f} "
                        f"now_ts={wall_done_ts:.3f} now_ms={wall_done_ts * 1000.0:.1f}"
                    )
            phase_sample = _build_fluxon_sync_phase_sample(
                started_at=started_at,
                done_at=done_at,
                deadline_ts=deadline_ts,
                wall_done_ts=wall_done_ts,
            )
            self._phase_profiler.record(op_name="GET", key=key, sample=phase_sample, error_msg=err)
            if err is not None:
                _bench_kv_print(f"{ctx} GET failed-after-block key={key!r} err={err}")
                return err
            _bench_kv_print(f"{ctx} GET done key={key!r}", verbose_only=True)
            return None
        except Exception as exc:
            _bench_kv_print(f"{ctx} GET exception key={key!r} err={exc}")
            return f"GET exception: {exc}"

    def close(self) -> _SimpleResult:
        try:
            return self._store.close()
        except Exception as exc:
            return _SimpleResult.err(str(exc))

    def phase_summary(self) -> Dict[str, Dict[str, Any]]:
        return self._phase_profiler.snapshot()

    def set_phase_summary_callback(
        self,
        callback: Optional[Callable[[Dict[str, Any]], None]],
    ) -> None:
        self._phase_profiler.set_phase_summary_callback(callback)

    def flush_phase_summary(self) -> None:
        self._phase_profiler.flush_pending()


def init_kv_store(kvcache_config: dict[str, Any]) -> tuple[Optional[Any], Optional[str]]:
    backend_kind = str(kvcache_config.get("backend_kind", BACKEND_KIND_FLUXON)).strip().upper()
    if backend_kind == BACKEND_KIND_REDIS:
        try:
            redis_cfg = kvcache_config.get("redis")
            if not isinstance(redis_cfg, dict):
                return None, "Redis benchmark config is missing 'redis' mapping"
            raw_endpoints = redis_cfg.get("endpoints")
            if not isinstance(raw_endpoints, list) or not raw_endpoints:
                return None, "Redis benchmark config requires a non-empty endpoints list"
            endpoints = []
            for idx, raw_endpoint in enumerate(raw_endpoints):
                if not isinstance(raw_endpoint, dict):
                    return None, f"Redis endpoint[{idx}] must be a mapping"
                host = str(raw_endpoint.get("host", "")).strip()
                port = int(raw_endpoint.get("port", 0))
                if not host:
                    return None, f"Redis endpoint[{idx}] host must be non-empty"
                if port <= 0 or port > 65535:
                    return None, f"Redis endpoint[{idx}] port out of range: {port}"
                endpoints.append(_RedisEndpoint(host=host, port=port))
            connect_timeout_seconds = float(redis_cfg.get("connect_timeout_seconds", 5.0))
            socket_timeout_seconds = float(redis_cfg.get("socket_timeout_seconds", 30.0))
            if connect_timeout_seconds <= 0.0:
                return None, "Redis connect_timeout_seconds must be > 0"
            if socket_timeout_seconds <= 0.0:
                return None, "Redis socket_timeout_seconds must be > 0"
            database = int(redis_cfg.get("database", 0))
            if database < 0:
                return None, "Redis database must be >= 0"
            password_raw = redis_cfg.get("password")
            password = None if password_raw is None else str(password_raw)
            return (
                RedisShardClient(
                    endpoints=endpoints,
                    connect_timeout_seconds=connect_timeout_seconds,
                    socket_timeout_seconds=socket_timeout_seconds,
                    database=database,
                    password=password,
                ),
                None,
            )
        except Exception as exc:
            return None, f"Exception while creating Redis benchmark client: {exc}"
    if backend_kind == BACKEND_KIND_ALLUXIO:
        return _NoopBenchmarkStore(BACKEND_KIND_ALLUXIO), None
    try:
        config = KVCacheConfig(_sanitize_benchmark_client_kvcache_config(kvcache_config))
        result = new_store(config)
        if not result.is_ok():
            return None, f"Failed to create KVCache store: {result.unwrap_error()}"
        store = result.unwrap()
        if store is None:
            return None, "Failed to create KVCache store: unwrap() returned None"
        return FluxonBlockingStore(store), None
    except Exception as exc:
        return None, f"Exception while creating KVCache store: {exc}"


def kv_put_once(
    store: Any,
    key: str,
    value: dict[str, Union[int, float, bool, str, bytes, bytearray, memoryview]],
    *,
    deadline_ts: float,
    ctx: str,
) -> Optional[str]:
    if store is None:
        return "KV store is not initialized"
    payload = value.get("payload") if isinstance(value, dict) else None
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        return "PUT failed: benchmark payload must be bytes-like"
    if not hasattr(store, "put_blocking"):
        backend_kind = getattr(store, "backend_kind", type(store).__name__)
        return f"PUT failed: backend {backend_kind} does not expose put_blocking"
    return store.put_blocking(key, bytes(payload), deadline_ts=deadline_ts, ctx=ctx)


def kv_get_once(store: Any, key: str, *, deadline_ts: float, ctx: str) -> Optional[str]:
    if store is None:
        return "KV store is not initialized"
    if not hasattr(store, "get_blocking"):
        backend_kind = getattr(store, "backend_kind", type(store).__name__)
        return f"GET failed: backend {backend_kind} does not expose get_blocking"
    return normalize_kv_get_error(
        store.get_blocking(key, deadline_ts=deadline_ts, ctx=ctx)
    )


def extract_kv_benchmark_extras_from_benchmark_section(benchmark_cfg: Mapping[str, Any]) -> Dict[str, Any]:
    mode_raw = benchmark_cfg.get(BENCHMARK_KEY_MODE)
    mode = str(mode_raw).upper() if mode_raw is not None else ""
    if mode not in KV_TEST_MODES:
        return {}
    extras: Dict[str, Any] = {}
    for key in KV_BENCHMARK_EXTRA_KEYS:
        if key in benchmark_cfg:
            extras[key] = copy.deepcopy(benchmark_cfg[key])
    return extras


def _stable_bucket(parts: Sequence[Any]) -> int:
    digest = hashlib.sha256()
    for part in parts:
        digest.update(str(part).encode("utf-8"))
        digest.update(b"\x1f")
    return int.from_bytes(digest.digest()[:8], "big")


@lru_cache(maxsize=64)
def _build_zipfian_sampler(keyspace_size: int, theta: float = DEFAULT_ZIPFIAN_THETA) -> _ZipfianSampler:
    if keyspace_size <= 0:
        raise ValueError(f"keyspace_size must be > 0, got: {keyspace_size}")
    total_weight = 0.0
    for rank in range(1, keyspace_size + 1):
        total_weight += 1.0 / (float(rank) ** theta)
    accum = 0.0
    cdf = []
    for rank in range(1, keyspace_size + 1):
        accum += (1.0 / (float(rank) ** theta)) / total_weight
        cdf.append(accum)
    cdf[-1] = 1.0
    return _ZipfianSampler(tuple(cdf))


def _kv_runtime_config_from_test_config(test_config: Mapping[str, Any], *, key_prefix: str) -> KVRuntimeConfig:
    workload_id_raw = test_config.get(BENCHMARK_KEY_WORKLOAD_ID) or test_config.get("test_id") or ""
    request_distribution_raw = test_config.get(
        BENCHMARK_KEY_REQUEST_DISTRIBUTION,
        REQUEST_DISTRIBUTION_UNIFORM,
    )
    request_distribution = str(request_distribution_raw).strip().lower() or REQUEST_DISTRIBUTION_UNIFORM
    if request_distribution not in (REQUEST_DISTRIBUTION_UNIFORM, REQUEST_DISTRIBUTION_ZIPFIAN):
        raise ValueError(f"unsupported request_distribution: {request_distribution!r}")
    keyspace_size = int(test_config.get(BENCHMARK_KEY_KEYSPACE_SIZE, DEFAULT_KV_KEYSPACE_SIZE))
    if keyspace_size <= 0:
        raise ValueError(f"keyspace_size must be > 0, got: {keyspace_size}")
    read_ratio = test_config.get(BENCHMARK_KEY_READ_RATIO)
    write_ratio = test_config.get(BENCHMARK_KEY_WRITE_RATIO)
    if read_ratio is None or write_ratio is None:
        raise ValueError(
            "KV benchmark requires explicit read_ratio/write_ratio; "
            "legacy seed/worker operation split has been removed"
        )
    if not isinstance(read_ratio, (int, float)) or not isinstance(write_ratio, (int, float)):
        raise ValueError("read_ratio/write_ratio must be numeric")
    if float(read_ratio) < 0.0 or float(write_ratio) < 0.0:
        raise ValueError("read_ratio/write_ratio must be >= 0")
    if float(read_ratio) + float(write_ratio) <= 0.0:
        raise ValueError("read_ratio + write_ratio must be > 0")
    affinity_locality_ratio_raw = test_config.get(BENCHMARK_KEY_AFFINITY_LOCALITY_RATIO)
    affinity_locality_ratio: Optional[float] = None
    if affinity_locality_ratio_raw is not None:
        if not isinstance(affinity_locality_ratio_raw, (int, float)):
            raise ValueError(
                f"{BENCHMARK_KEY_AFFINITY_LOCALITY_RATIO} must be number when present"
            )
        affinity_locality_ratio = float(affinity_locality_ratio_raw)
        if affinity_locality_ratio < 0.0 or affinity_locality_ratio > 1.0:
            raise ValueError(
                f"{BENCHMARK_KEY_AFFINITY_LOCALITY_RATIO} must be in [0, 1], got: {affinity_locality_ratio}"
            )
    affinity_slot_count = int(test_config.get(BENCHMARK_KEY_AFFINITY_SLOT_COUNT, 1))
    if affinity_slot_count <= 0:
        raise ValueError(
            f"{BENCHMARK_KEY_AFFINITY_SLOT_COUNT} must be > 0, got: {affinity_slot_count}"
        )
    affinity_slot_index_raw = test_config.get("affinity_slot_index")
    affinity_slot_index: Optional[int] = None
    if affinity_slot_index_raw is not None:
        affinity_slot_index = int(affinity_slot_index_raw)
        if affinity_slot_index < 0:
            raise ValueError(f"affinity_slot_index must be >= 0, got: {affinity_slot_index}")
    return KVRuntimeConfig(
        workload_id=str(workload_id_raw),
        key_prefix=key_prefix,
        keyspace_size=keyspace_size,
        request_distribution=request_distribution,
        read_ratio=float(read_ratio),
        write_ratio=float(write_ratio),
        affinity_locality_ratio=affinity_locality_ratio,
        affinity_slot_count=affinity_slot_count,
        affinity_slot_index=affinity_slot_index,
    )


def _sample_key_index_for_distribution(
    *,
    request_distribution: str,
    keyspace_size: int,
    bucket: int,
) -> int:
    if keyspace_size <= 0:
        raise ValueError(f"keyspace_size must be > 0, got: {keyspace_size}")
    if request_distribution == REQUEST_DISTRIBUTION_ZIPFIAN:
        return _build_zipfian_sampler(keyspace_size).sample(bucket)
    return int(bucket % keyspace_size)


def _normalize_affinity_identity(identity: Optional[str]) -> str:
    ident = str(identity or "").strip()
    if ident:
        return ident
    return "benchmark_node"


def _affinity_slot_index(
    identity: str,
    *,
    slot_count: int,
    explicit_slot_index: Optional[int],
) -> int:
    if slot_count <= 0:
        raise ValueError(f"slot_count must be > 0, got: {slot_count}")
    if explicit_slot_index is None:
        raise ValueError(
            "affinity_slot_index must be provided by coordinator when affinity is enabled; "
            f"identity={identity!r} slot_count={slot_count}"
        )
    return int(explicit_slot_index) % slot_count


def _affinity_partition_bounds(
    *,
    keyspace_size: int,
    slot_count: int,
    slot_index: int,
) -> tuple[int, int]:
    if keyspace_size <= 0:
        raise ValueError(f"keyspace_size must be > 0, got: {keyspace_size}")
    effective_slot_count = max(1, min(int(slot_count), int(keyspace_size)))
    bounded_slot_index = int(slot_index) % effective_slot_count
    base = int(keyspace_size) // effective_slot_count
    remainder = int(keyspace_size) % effective_slot_count
    start = bounded_slot_index * base + min(bounded_slot_index, remainder)
    length = base + (1 if bounded_slot_index < remainder else 0)
    return start, max(1, length)


def _select_kv_key_index(
    runtime_cfg: KVRuntimeConfig,
    *,
    identity: Optional[str],
    thread_id: int,
    op_idx: int,
) -> int:
    normalized_identity = _normalize_affinity_identity(identity)
    global_bucket = _stable_bucket(
        (
            runtime_cfg.workload_id,
            runtime_cfg.key_prefix,
            thread_id,
            op_idx,
            "key",
        )
    )
    if runtime_cfg.uses_affinity():
        route_bucket = _stable_bucket(
            (
                runtime_cfg.workload_id,
                runtime_cfg.key_prefix,
                normalized_identity,
                thread_id,
                op_idx,
                "affinity_route",
            )
        )
        if (float(route_bucket) / STABLE_HASH_MODULUS) < float(runtime_cfg.affinity_locality_ratio):
            slot_index = _affinity_slot_index(
                normalized_identity,
                slot_count=int(runtime_cfg.affinity_slot_count),
                explicit_slot_index=runtime_cfg.affinity_slot_index,
            )
            range_start, range_len = _affinity_partition_bounds(
                keyspace_size=int(runtime_cfg.keyspace_size),
                slot_count=int(runtime_cfg.affinity_slot_count),
                slot_index=slot_index,
            )
            local_bucket = _stable_bucket(
                (
                    runtime_cfg.workload_id,
                    runtime_cfg.key_prefix,
                    normalized_identity,
                    thread_id,
                    op_idx,
                    "affinity_key",
                )
            )
            local_offset = _sample_key_index_for_distribution(
                request_distribution=runtime_cfg.request_distribution,
                keyspace_size=range_len,
                bucket=local_bucket,
            )
            return int(range_start + local_offset)

    return _sample_key_index_for_distribution(
        request_distribution=runtime_cfg.request_distribution,
        keyspace_size=runtime_cfg.keyspace_size,
        bucket=global_bucket,
    )


def _select_kv_operation_kind(
    runtime_cfg: KVRuntimeConfig,
    *,
    node_role: str,
    thread_id: int,
    op_idx: int,
) -> str:
    del node_role
    threshold = float(_stable_bucket((runtime_cfg.workload_id, runtime_cfg.key_prefix, thread_id, op_idx, "op")))
    if (threshold / STABLE_HASH_MODULUS) < runtime_cfg.read_cutoff():
        return KV_OPERATION_GET
    return KV_OPERATION_PUT


def _resolve_kv_value_size_for_key(benchmark_node: Any, key_idx: int) -> int:
    return int(benchmark_node._resolve_kv_value_size(0, key_idx))


def _kv_seed_bootstrap_required(runtime_cfg: KVRuntimeConfig) -> bool:
    if runtime_cfg.read_ratio is None or runtime_cfg.write_ratio is None:
        raise ValueError("KV benchmark requires explicit read_ratio/write_ratio before READY bootstrap")
    return float(runtime_cfg.read_ratio) > 0.0


def _kv_bootstrap_before_ready_enabled(test_config: Mapping[str, Any]) -> bool:
    raw = test_config.get("kv_bootstrap_before_ready")
    if isinstance(raw, bool):
        if not raw:
            return False
        affinity_slot_index_raw = test_config.get("affinity_slot_index")
        if affinity_slot_index_raw is None:
            return True
        return int(affinity_slot_index_raw) == 0
    if raw is None:
        return False
    raise ValueError("kv_bootstrap_before_ready must be bool when present")


def merge_kv_benchmark_extras(
    node_config: Mapping[str, Any],
    benchmark_cfg: Mapping[str, Any],
) -> Dict[str, Any]:
    merged_config = copy.deepcopy(dict(node_config))
    for key, value in extract_kv_benchmark_extras_from_benchmark_section(benchmark_cfg).items():
        merged_config[key] = copy.deepcopy(value)
    return merged_config


def prepare_kv_before_ready(benchmark_node: Any, *, logger: Any) -> bool:
    test_config = getattr(benchmark_node, "test_config", None)
    if not isinstance(test_config, dict):
        return False

    test_mode = str(test_config.get("test_mode", "")).strip().upper()
    if test_mode != TEST_MODE_KVSTORE:
        return False
    if benchmark_node.kv_store is None:
        raise RuntimeError("KV benchmark requires kv_store to be initialized")

    node_role = canonicalize_kv_node_role(test_config.get("node_role", ""))
    if not _kv_bootstrap_before_ready_enabled(test_config):
        return True

    key_prefix = benchmark_node.key_prefix or test_config.get("key_prefix")
    if not isinstance(key_prefix, str) or not key_prefix.strip():
        raise ValueError("missing key_prefix for KV bootstrap node")
    runtime_cfg = _kv_runtime_config_from_test_config(
        test_config,
        key_prefix=key_prefix.strip(),
    )
    if not _kv_seed_bootstrap_required(runtime_cfg):
        logger.info(
            "⏭️ KV bootstrap before READY skipped: workload has no read phase "
            "(read_ratio=%s write_ratio=%s)",
            runtime_cfg.read_ratio,
            runtime_cfg.write_ratio,
        )
        return True

    op_timeout_s = float(test_config.get("op_timeout_seconds", 0.0))
    if op_timeout_s <= 0.0:
        raise ValueError("op_timeout_seconds must be > 0 for KV bootstrap")

    fixed_value = None
    if benchmark_node.value_size_mode == VALUE_SIZE_MODE_FIXED:
        fixed_value_size = int(test_config.get("value_size", 0))
        if fixed_value_size > 0:
            fixed_value = benchmark_node._generate_test_data(fixed_value_size)

    logger.info(
        "🧱 KV bootstrap before READY: mode=%s key_prefix=%s keyspace_size=%s distribution=%s",
        test_mode,
        runtime_cfg.key_prefix,
        runtime_cfg.keyspace_size,
        runtime_cfg.request_distribution,
    )

    def _bootstrap_one_key(key_idx: int) -> None:
        key = f"{runtime_cfg.key_prefix}_k{key_idx}"
        kv_value_size = _resolve_kv_value_size_for_key(benchmark_node, key_idx)
        value = (
            fixed_value
            if fixed_value is not None
            else benchmark_node._generate_test_data(kv_value_size)
        )
        deadline_ts = time.time() + op_timeout_s
        result = benchmark_node._put_single_operation(
            key,
            value,
            inflight_at_start=0,
            deadline_ts=deadline_ts,
            ctx=(
                f"node={benchmark_node.node_id} role={node_role} pre_ready=true "
                f"keyspace=kvstore key_idx={key_idx}"
            ),
        )
        if not result.success:
            raise RuntimeError(f"KV bootstrap PUT failed: key={key} err={result.error_msg}")

    bootstrap_concurrency = min(KV_SEED_BOOTSTRAP_MAX_CONCURRENCY, runtime_cfg.keyspace_size)
    if bootstrap_concurrency <= 1:
        for key_idx in range(runtime_cfg.keyspace_size):
            _bootstrap_one_key(key_idx)
    else:
        logger.info(
            "🧱 KV bootstrap using parallel shards: concurrency=%s",
            bootstrap_concurrency,
        )

        def _bootstrap_shard(shard_idx: int) -> int:
            completed = 0
            for key_idx in range(shard_idx, runtime_cfg.keyspace_size, bootstrap_concurrency):
                _bootstrap_one_key(key_idx)
                completed += 1
            return completed

        completed_keys = 0
        with ThreadPoolExecutor(max_workers=bootstrap_concurrency) as executor:
            futures = [
                executor.submit(_bootstrap_shard, shard_idx)
                for shard_idx in range(bootstrap_concurrency)
            ]
            for future in as_completed(futures):
                completed_keys += int(future.result())
                logger.info(
                    "🧱 KV bootstrap shard completed: progress=%s/%s",
                    completed_keys,
                    runtime_cfg.keyspace_size,
                )
    logger.info(
        "✅ KV bootstrap before READY completed: keys=%d",
        runtime_cfg.keyspace_size,
    )
    return True


def run_kv_worker(
    benchmark_node: Any,
    *,
    thread_id: int,
    deadline_ts: float,
    operation_result_cls: Any,
    operation_outcome: Any,
    metric_warmup_seconds: float,
    debug_print: Callable[[str], None],
) -> Optional[list[Any]]:
    test_config = getattr(benchmark_node, "test_config", None)
    if not isinstance(test_config, dict):
        return None

    test_mode = str(test_config.get("test_mode", TEST_MODE_KVSTORE)).strip().upper()
    if test_mode not in KV_TEST_MODES:
        return None

    node_role = canonicalize_kv_node_role(test_config.get("node_role", ""))
    key_prefix = benchmark_node.key_prefix or test_config.get("key_prefix")
    if not isinstance(key_prefix, str) or not key_prefix.strip():
        raise ValueError("missing key_prefix for KV benchmark worker")
    runtime_cfg = _kv_runtime_config_from_test_config(
        test_config,
        key_prefix=key_prefix.strip(),
    )

    results: list[Any] = []
    op_idx = 0
    fixed_value = None
    if benchmark_node.value_size_mode == VALUE_SIZE_MODE_FIXED:
        fixed_value_size = int(test_config.get("value_size", 0))
        if fixed_value_size > 0:
            fixed_value = benchmark_node._generate_test_data(fixed_value_size)

    while True:
        if benchmark_node._benchmark_stop.is_set():
            debug_print(
                f"thread {thread_id} observed benchmark stop intent, "
                f"total_ops={len(results)}, last_op_idx={op_idx}"
            )
            break
        now_ts = time.time()
        if now_ts >= float(deadline_ts):
            break

        inflight_at_start = benchmark_node._inflight_begin()
        try:
            op_timeout_s = float(test_config["op_timeout_seconds"])
            op_deadline_ts = min(float(deadline_ts), time.time() + op_timeout_s)
            if test_mode == TEST_MODE_KVSTORE_WITH_LOCAL_CACHE:
                hotset_size = max(1, min(int(runtime_cfg.keyspace_size), 100))
                key_idx = int(op_idx % hotset_size)
                key = f"{runtime_cfg.key_prefix}_thread{thread_id}_op{key_idx}"
            else:
                key_idx = _select_kv_key_index(
                    runtime_cfg,
                    identity=benchmark_node.instance_key or benchmark_node.node_id,
                    thread_id=thread_id,
                    op_idx=op_idx,
                )
                key = f"{runtime_cfg.key_prefix}_k{key_idx}"
            kv_value_size = _resolve_kv_value_size_for_key(benchmark_node, key_idx)
            op_kind = _select_kv_operation_kind(
                runtime_cfg,
                node_role=node_role,
                thread_id=thread_id,
                op_idx=op_idx,
            )
            if op_kind == KV_OPERATION_PUT:
                value = (
                    fixed_value
                    if fixed_value is not None
                    else benchmark_node._generate_test_data(kv_value_size)
                )
                result = benchmark_node._put_single_operation(
                    key,
                    value,
                    inflight_at_start,
                    deadline_ts=op_deadline_ts,
                    ctx=(
                        f"node={benchmark_node.node_id} role={node_role} thread={thread_id} "
                        f"op={op_idx} key_idx={key_idx} op_kind={op_kind.lower()}"
                    ),
                )
            elif op_kind == KV_OPERATION_GET:
                result = benchmark_node._get_single_operation(
                    key,
                    inflight_at_start,
                    deadline_ts=op_deadline_ts,
                    expected_data_size=kv_value_size,
                    ctx=(
                        f"node={benchmark_node.node_id} role={node_role} thread={thread_id} "
                        f"op={op_idx} key_idx={key_idx} op_kind={op_kind.lower()}"
                    ),
                )
            else:
                result = operation_result_cls(
                    success=False,
                    latency_us=0.0,
                    operation_type="unknown",
                    key=key,
                    data_size=0,
                    inflight_at_start=inflight_at_start,
                    outcome_kind=operation_outcome.ERROR,
                    error_msg=f"unsupported KV operation kind: {op_kind}",
                )
        except Exception as exc:  # noqa: BLE001
            result = operation_result_cls(
                success=False,
                latency_us=0.0,
                operation_type="exception",
                key="NO KEY",
                data_size=0,
                inflight_at_start=inflight_at_start,
                outcome_kind=operation_outcome.ERROR,
                error_msg=str(exc),
            )
        finally:
            benchmark_node._inflight_end()

        result.node_id = benchmark_node.node_id
        result.worker_id = thread_id
        result.finish_ts = time.time()
        op_finish_ts = result.finish_ts

        if benchmark_node.start_time is not None:
            warmup_deadline_ts = benchmark_node.start_time + metric_warmup_seconds
            if op_finish_ts < warmup_deadline_ts:
                benchmark_node._mark_progress(
                    thread_id=thread_id,
                    op_idx=op_idx,
                    finish_ts=op_finish_ts,
                    latency_us=result.latency_us,
                )
                op_idx += 1
                continue

        benchmark_node._mark_progress(
            thread_id=thread_id,
            op_idx=op_idx,
            finish_ts=op_finish_ts,
            latency_us=result.latency_us,
        )
        results.append(result)
        op_idx += 1

    debug_print(
        f"thread {thread_id} exit kv run loop, total_ops={len(results)}, last_op_idx={op_idx}"
    )
    return results
