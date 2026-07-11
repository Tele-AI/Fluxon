from __future__ import annotations

"""RPC benchmark helpers."""

import bisect
import copy
import hashlib
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Union

from benchmark_role_names import canonicalize_kv_node_role

RPC_PAYLOAD_MODE_BYTES = "BYTES"
RPC_PAYLOAD_MODE_FLATDICT = "FLATDICT"
RPC_PAYLOAD_MODES_ALLOWED = (
    RPC_PAYLOAD_MODE_BYTES,
    RPC_PAYLOAD_MODE_FLATDICT,
)

TEST_MODE_RPC = "RPC"
KV_OPERATION_RPC = "RPC"

RPC_BACKEND_KIND_FLUXON = "FLUXON"
RPC_BACKEND_KIND_ZERORPC = "ZERORPC"
RPC_BACKENDS_ALLOWED = (
    RPC_BACKEND_KIND_FLUXON,
    RPC_BACKEND_KIND_ZERORPC,
)
RPC_SCENE_ECHO_SMALL_PAYLOAD = "rpc_echo_small_payload"
RPC_SCENE_ECHO_SMALL_PAYLOAD_ZERORPC = "rpc_echo_small_payload_zerorpc"
RPC_SCENES = (
    RPC_SCENE_ECHO_SMALL_PAYLOAD,
)
RPC_SCENE_FAMILY_BY_WORKLOAD_ID = {
    RPC_SCENE_ECHO_SMALL_PAYLOAD: RPC_SCENE_ECHO_SMALL_PAYLOAD,
    RPC_SCENE_ECHO_SMALL_PAYLOAD_ZERORPC: RPC_SCENE_ECHO_SMALL_PAYLOAD,
}
RPC_BENCHMARK_KEY_BACKEND_KIND = "rpc_backend_kind"
RPC_BENCHMARK_KEY_PATH = "rpc_path"
RPC_BENCHMARK_KEY_PAYLOAD_SIZE = "rpc_payload_size"
RPC_BENCHMARK_KEY_PAYLOAD_MODE = "rpc_payload_mode"
RPC_BENCHMARK_KEY_SERVER_SOURCE = "rpc_server_source"
RPC_BENCHMARK_KEY_TARGET_ROLE = "rpc_target_role"
RPC_BENCHMARK_KEY_SERVER_INSTANCE_KEYS = "rpc_server_instance_keys"
RPC_BENCHMARK_KEY_SERVER_TARGETS = "rpc_server_targets"
RPC_BENCHMARK_KEY_SERVER_ZERORPC_PORTS = "rpc_server_zero_rpc_ports"
RPC_DEFAULT_PATH = "/bench/echo"
RPC_SERVER_SOURCE_BENCHMARK_NODE_ROLE = "benchmark_node_role"
RPC_SERVER_SOURCE_BENCHMARK_NODE_ALL = "benchmark_node_all"
RPC_SERVER_SOURCES_ALLOWED = (
    RPC_SERVER_SOURCE_BENCHMARK_NODE_ROLE,
    RPC_SERVER_SOURCE_BENCHMARK_NODE_ALL,
)
RPC_ECHO_FLATDICT_PAYLOAD_KEY = "payload"

FLUXON_PHASE_LOG_INTERVAL_OPS = 128
FLUXON_PHASE_SLOW_OP_THRESHOLD_US = 50_000.0
STABLE_HASH_MODULUS = float(1 << 64)

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
RPC_BENCHMARK_EXTRA_KEYS = (
    "workload_id",
    RPC_BENCHMARK_KEY_BACKEND_KIND,
    RPC_BENCHMARK_KEY_PATH,
    RPC_BENCHMARK_KEY_PAYLOAD_SIZE,
    RPC_BENCHMARK_KEY_PAYLOAD_MODE,
    RPC_BENCHMARK_KEY_SERVER_SOURCE,
    RPC_BENCHMARK_KEY_TARGET_ROLE,
    RPC_BENCHMARK_KEY_SERVER_INSTANCE_KEYS,
    RPC_BENCHMARK_KEY_SERVER_TARGETS,
    RPC_BENCHMARK_KEY_SERVER_ZERORPC_PORTS,
)


def _bench_rpc_print(msg: str) -> None:
    print(f"[BENCH-RPC] {msg}", flush=True)


def _stable_bucket(parts: Sequence[Any]) -> int:
    digest = hashlib.sha256()
    for part in parts:
        digest.update(str(part).encode("utf-8"))
        digest.update(b"\x1f")
    return int.from_bytes(digest.digest()[:8], "big")


def register_echo_handler(
    kv_store: Any,
    *,
    path: str,
    payload_mode: str = RPC_PAYLOAD_MODE_BYTES,
) -> None:
    normalized_path = str(path).strip()
    if not normalized_path:
        raise ValueError("rpc echo path must be non-empty")
    normalized_mode = str(payload_mode).strip().upper()
    if normalized_mode not in RPC_PAYLOAD_MODES_ALLOWED:
        raise ValueError(
            f"rpc echo payload_mode must be one of {sorted(RPC_PAYLOAD_MODES_ALLOWED)}, got {payload_mode!r}"
        )

    if normalized_mode == RPC_PAYLOAD_MODE_BYTES:
        def _handler(from_node_id: str, payload: bytes) -> bytes:
            _ = from_node_id
            if not isinstance(payload, (bytes, bytearray)):
                raise RuntimeError(f"rpc payload must be bytes: {type(payload)}")
            return bytes(payload)

        result = kv_store.rpc_register_bytes(normalized_path, _handler)
    else:
        def _handler(from_node_id: str, payload: dict[str, object]) -> dict[str, object]:
            _ = from_node_id
            if not isinstance(payload, dict):
                raise RuntimeError(f"rpc payload must be flat dict: {type(payload)}")
            return dict(payload)

        result = kv_store.rpc_register(normalized_path, _handler)
    if not result.is_ok():
        raise RuntimeError(
            f"rpc echo register failed path={normalized_path!r} payload_mode={normalized_mode}: {result.unwrap_error()}"
        )
    result.unwrap()


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
class RPCRuntimeConfig:
    scene_id: str
    backend_kind: str
    path: str
    payload_size: int
    payload_mode: str
    server_source: str
    target_role: Optional[str]
    server_instance_keys: tuple[str, ...]
    server_targets: Dict[str, str]
    server_zero_rpc_ports: Dict[str, int]


class _FluxonRpcStore:
    def __init__(self, kv_store: Any) -> None:
        self._kv_store = kv_store
        self._registered_paths: set[str] = set()
        self._phase_profiler = _FluxonPhaseProfiler()

    def register_echo_handler(self, *, path: str, payload_mode: str) -> None:
        if path in self._registered_paths:
            return
        register_echo_handler(self._kv_store, path=path, payload_mode=payload_mode)
        self._registered_paths.add(path)

    def close(self) -> Any:
        return self._kv_store.close()

    def phase_summary(self) -> Dict[str, Dict[str, Any]]:
        merged: Dict[str, Dict[str, Any]] = {}
        if hasattr(self._kv_store, "phase_summary"):
            raw_summary = self._kv_store.phase_summary()
            if isinstance(raw_summary, dict):
                merged.update(copy.deepcopy(raw_summary))
        merged.update(self._phase_profiler.snapshot())
        return merged

    def set_phase_summary_callback(
        self,
        callback: Optional[Callable[[Dict[str, Any]], None]],
    ) -> None:
        self._phase_profiler.set_phase_summary_callback(callback)
        if hasattr(self._kv_store, "set_phase_summary_callback"):
            self._kv_store.set_phase_summary_callback(callback)

    def flush_phase_summary(self) -> None:
        if hasattr(self._kv_store, "flush_phase_summary"):
            self._kv_store.flush_phase_summary()
        self._phase_profiler.flush_pending()

    def call_echo(
        self,
        *,
        target_instance_key: str,
        path: str,
        payload: Union[bytes, Dict[str, Any]],
        payload_mode: str,
        timeout_ms: int,
        deadline_ts: float,
    ) -> Optional[str]:
        started_at = time.perf_counter()
        err: Optional[str] = None
        observe_payload: Optional[Mapping[str, Any]] = None
        try:
            if payload_mode == RPC_PAYLOAD_MODE_BYTES:
                if not isinstance(payload, (bytes, bytearray)):
                    raise TypeError(f"rpc bytes payload must be bytes-like, got {type(payload)}")
                call_result = self._kv_store.rpc_call_bytes(
                    target_instance_key,
                    path,
                    bytes(payload),
                    timeout_ms=timeout_ms,
                )
            elif payload_mode == RPC_PAYLOAD_MODE_FLATDICT:
                if not isinstance(payload, dict):
                    raise TypeError(f"rpc flatdict payload must be dict, got {type(payload)}")
                call_result = self._kv_store.rpc_call(
                    target_instance_key,
                    path,
                    payload,
                    timeout_ms=timeout_ms,
                )
            else:
                raise ValueError(f"unsupported rpc payload mode: {payload_mode!r}")
            if not call_result.is_ok():
                err = f"RPC failed: {call_result.unwrap_error()}"
            else:
                future = call_result.unwrap()
                wait_result = future.wait_with_observe()
                if not wait_result.is_ok():
                    err = f"RPC failed: {wait_result.unwrap_error()}"
                else:
                    observed = wait_result.unwrap()
                    if not isinstance(observed, tuple) or len(observed) != 2:
                        err = f"RPC failed: invalid observed response type {type(observed)}"
                    else:
                        response, raw_observe_payload = observed
                        if not isinstance(raw_observe_payload, Mapping):
                            err = (
                                "RPC failed: invalid observe payload type "
                                f"{type(raw_observe_payload)}"
                            )
                        else:
                            observe_payload = dict(raw_observe_payload)
                            if payload_mode == RPC_PAYLOAD_MODE_BYTES:
                                if not isinstance(response, (bytes, bytearray)):
                                    err = f"RPC failed: invalid response type {type(response)}"
                                elif bytes(response) != payload:
                                    err = "RPC failed: echo payload mismatch"
                            elif payload_mode == RPC_PAYLOAD_MODE_FLATDICT:
                                if not isinstance(response, dict):
                                    err = f"RPC failed: invalid flatdict response type {type(response)}"
                                elif response != payload:
                                    err = "RPC failed: flatdict echo payload mismatch"
        except Exception as exc:  # noqa: BLE001
            err = f"RPC exception: {exc}"
        done_at = time.perf_counter()
        wall_done_ts = time.time()
        if err is None and wall_done_ts > deadline_ts:
            err = (
                f"RPC timed out after wait: deadline_ts={deadline_ts:.3f} "
                f"now_ts={wall_done_ts:.3f} now_ms={wall_done_ts * 1000.0:.1f}"
            )
        phase_sample = _build_fluxon_sync_phase_sample(
            started_at=started_at,
            done_at=done_at,
            deadline_ts=deadline_ts,
            wall_done_ts=wall_done_ts,
            extra_payload=observe_payload,
        )
        self._phase_profiler.record(
            op_name=KV_OPERATION_RPC,
            key=target_instance_key,
            sample=phase_sample,
            error_msg=err,
        )
        return err


class _ZeroRpcServerHandle:
    def __init__(
        self,
        proc: subprocess.Popen[bytes],
        endpoint: str,
    ) -> None:
        self._proc = proc
        self.endpoint = endpoint

    def close(self) -> None:
        if self._proc.poll() is not None:
            return
        self._proc.terminate()
        try:
            self._proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            self._proc.wait(timeout=5.0)


class _ZeroRpcClient:
    def __init__(self, *, endpoint: str) -> None:
        import zerorpc  # type: ignore

        self._endpoint = endpoint
        self._client = zerorpc.Client(timeout=10.0)
        self._client.connect(endpoint)

    def close(self) -> None:
        self._client.close()

    def call_echo(self, payload: bytes) -> Optional[str]:
        try:
            encoded = payload.decode("latin1")
            raw = self._client.echo(encoded)
            if not isinstance(raw, str):
                return f"RPC failed: invalid zerorpc response type {type(raw)}"
            if raw.encode("latin1") != payload:
                return "RPC failed: zerorpc echo payload mismatch"
            return None
        except Exception as exc:  # noqa: BLE001
            return f"RPC failed: zerorpc exception {type(exc).__name__}: {exc}"


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
            caller_decode_done_ts_us,
            caller_response_complete_pending_call_ts_us,
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

        if request_to_owner_recv_us is not None and response_send_to_caller_recv_us is not None:
            segment_sample[FLUXON_PHASE_SEGMENT_TRANSPORT_INFLIGHT_ESTIMATED_US] = max(
                0.0,
                request_to_owner_recv_us + response_send_to_caller_recv_us,
            )
    return segment_sample


def _fluxon_phase_segment_stats(samples: Sequence[float]) -> Dict[str, float]:
    if not samples:
        return {
            "min_us": 0.0,
            "avg_us": 0.0,
            "p50_us": 0.0,
            "p95_us": 0.0,
            "p99_us": 0.0,
            "max_us": 0.0,
        }
    sorted_samples = sorted(float(sample) for sample in samples)
    count = len(sorted_samples)

    def _percentile(ratio: float) -> float:
        if count == 1:
            return sorted_samples[0]
        idx = int(round((count - 1) * ratio))
        return sorted_samples[max(0, min(count - 1, idx))]

    return {
        "min_us": sorted_samples[0],
        "avg_us": sum(sorted_samples) / float(count),
        "p50_us": _percentile(0.50),
        "p95_us": _percentile(0.95),
        "p99_us": _percentile(0.99),
        "max_us": sorted_samples[-1],
    }


def _fluxon_error_bucket(error_msg: Optional[str]) -> str:
    if error_msg is None:
        return "ok"
    error_text = str(error_msg).lower()
    if "keynotfound" in error_text or "not found" in error_text:
        return "miss"
    if "timeout" in error_text or "deadline" in error_text:
        return "timeout"
    return "error"


def _classify_fluxon_rpc_path_bucket(extra_tags: Mapping[str, str]) -> Optional[str]:
    request_path_kind = extra_tags.get("rpc_request_path_kind")
    response_path_kind = extra_tags.get("rpc_response_path_kind")
    if request_path_kind == FLUXON_RPC_PATH_KIND_FAST and response_path_kind == FLUXON_RPC_PATH_KIND_FAST:
        return FLUXON_PHASE_PATH_BUCKET_FAST
    if request_path_kind == FLUXON_RPC_PATH_KIND_SLOW or response_path_kind == FLUXON_RPC_PATH_KIND_SLOW:
        return FLUXON_PHASE_PATH_BUCKET_SLOW
    return None


def _classify_fluxon_owner1_roundtrip_path_bucket(extra_tags: Mapping[str, str]) -> Optional[str]:
    owner_path_kind = extra_tags.get(FLUXON_OWNER_PATH_KIND)
    owner1_request_path_kind = extra_tags.get(FLUXON_OWNER1_REQUEST_PATH_KIND)
    owner1_response_path_kind = extra_tags.get(FLUXON_OWNER1_RESPONSE_PATH_KIND)
    if owner_path_kind == FLUXON_OWNER_PATH_KIND_IPC:
        return FLUXON_PHASE_PATH_BUCKET_IPC
    if (
        owner1_request_path_kind == FLUXON_RPC_PATH_KIND_FAST
        and owner1_response_path_kind == FLUXON_RPC_PATH_KIND_FAST
    ):
        return FLUXON_PHASE_PATH_BUCKET_FAST
    if (
        owner1_request_path_kind == FLUXON_RPC_PATH_KIND_SLOW
        or owner1_response_path_kind == FLUXON_RPC_PATH_KIND_SLOW
    ):
        return FLUXON_PHASE_PATH_BUCKET_SLOW
    return None


def _fluxon_segment_metric_sample_us(
    segment_sample: Mapping[str, float],
    metric_name: str,
) -> Optional[float]:
    if not segment_sample:
        return None
    value = segment_sample.get(metric_name)
    if value is None:
        return None
    return max(0.0, float(value))


def _fluxon_owner_path_metric_sample_us(
    segment_sample: Mapping[str, float],
    extra_us: Mapping[str, float],
    extra_tags: Mapping[str, str],
) -> Optional[float]:
    owner_path_bucket = _classify_fluxon_owner1_roundtrip_path_bucket(extra_tags)
    if owner_path_bucket is None:
        return None
    if owner_path_bucket == FLUXON_PHASE_PATH_BUCKET_IPC:
        owner_total_us = extra_us.get("rpc_owner_total_us")
        if owner_total_us is None:
            return None
        return max(0.0, float(owner_total_us))
    return segment_sample.get(FLUXON_PHASE_PATH_METRIC_OWNER1_ROUNDTRIP_US)


def _record_fluxon_path_metric_sample(
    stat: Dict[str, Any],
    metric_name: str,
    path_bucket: str,
    sample_us: Optional[float],
) -> None:
    if sample_us is None:
        return
    path_metric_total_us = stat["path_metric_total_us"]
    path_metric_counts = stat["path_metric_counts"]
    path_metric_max_us = stat["path_metric_max_us"]
    window_path_metric_samples = stat["window_path_metric_samples"]
    total_entry = path_metric_total_us.setdefault(metric_name, {})
    total_entry[path_bucket] = float(total_entry.get(path_bucket, 0.0)) + float(sample_us)
    counts_entry = path_metric_counts.setdefault(metric_name, {})
    counts_entry[path_bucket] = int(counts_entry.get(path_bucket, 0)) + 1
    max_entry = path_metric_max_us.setdefault(metric_name, {})
    max_entry[path_bucket] = max(float(max_entry.get(path_bucket, 0.0)), float(sample_us))
    window_bucket_entry = window_path_metric_samples.setdefault(metric_name, {})
    bucket_samples = window_bucket_entry.setdefault(path_bucket, [])
    bucket_samples.append(float(sample_us))


class _FluxonPhaseProfiler:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stats: Dict[str, Dict[str, Any]] = {}
        self._phase_summary_callback: Optional[Callable[[Dict[str, Any]], None]] = None

    @staticmethod
    def _new_stat() -> Dict[str, Any]:
        return {
            "count": 0,
            "submit_total_us": 0.0,
            "wait_total_us": 0.0,
            "finalize_total_us": 0.0,
            "total_total_us": 0.0,
            "max_total_us": 0.0,
            "deadline_overrun_count": 0,
            "bucket_counts": _empty_fluxon_phase_bucket_counts(),
            "extra_total_us": {},
            "segment_total_us": {},
            "segment_counts": {},
            "segment_max_us": {},
            "path_metric_total_us": {},
            "path_metric_counts": {},
            "path_metric_max_us": {},
            "window_count": 0,
            "window_bucket_counts": _empty_fluxon_phase_bucket_counts(),
            "window_deadline_overrun_count": 0,
            "window_segment_samples": {},
            "window_path_metric_samples": {},
        }

    def set_phase_summary_callback(
        self,
        callback: Optional[Callable[[Dict[str, Any]], None]],
    ) -> None:
        with self._lock:
            self._phase_summary_callback = callback

    @staticmethod
    def _format_summary_msg(
        *,
        op_name: str,
        count: int,
        stat: Dict[str, Any],
        window_summary: Optional[Dict[str, Any]],
    ) -> str:
        extra_avg_us = {}
        extra_total_us = stat.get("extra_total_us", {})
        if isinstance(extra_total_us, dict):
            for phase_name, phase_total_us in sorted(extra_total_us.items()):
                extra_avg_us[str(phase_name)] = float(phase_total_us) / float(count)
        extra_avg_parts = [
            f"{phase_name[:-3] if phase_name.endswith('_us') else phase_name}_avg_us={phase_avg_us:.1f}"
            for phase_name, phase_avg_us in sorted(extra_avg_us.items())
        ]
        summary_msg = (
            f"fluxon_phase_summary op={op_name} count={count} "
            f"submit_avg_us={float(stat['submit_total_us']) / float(count):.1f} "
            f"wait_avg_us={float(stat['wait_total_us']) / float(count):.1f} "
            f"finalize_avg_us={float(stat['finalize_total_us']) / float(count):.1f} "
            f"total_avg_us={float(stat['total_total_us']) / float(count):.1f} "
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
            stat = self._stats.setdefault(op_name, self._new_stat())
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
            _bench_rpc_print(summary_msg)
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
            _bench_rpc_print(
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


def _canonicalize_rpc_scene_family(scene_id_raw: Any) -> str:
    scene_id = str(scene_id_raw).strip()
    scene_family = RPC_SCENE_FAMILY_BY_WORKLOAD_ID.get(scene_id)
    if scene_family is None:
        raise ValueError(f"unsupported RPC benchmark scene: {scene_id!r}")
    return scene_family


def extract_rpc_benchmark_extras_from_benchmark_section(benchmark_cfg: Mapping[str, Any]) -> Dict[str, Any]:
    mode_raw = benchmark_cfg.get("mode")
    mode = str(mode_raw).upper() if mode_raw is not None else ""
    if mode != TEST_MODE_RPC:
        return {}
    extras: Dict[str, Any] = {}
    for key in RPC_BENCHMARK_EXTRA_KEYS:
        if key in benchmark_cfg:
            extras[key] = copy.deepcopy(benchmark_cfg[key])
    return extras


def merge_rpc_benchmark_extras(
    node_config: Mapping[str, Any],
    benchmark_cfg: Mapping[str, Any],
) -> Dict[str, Any]:
    merged_config = copy.deepcopy(dict(node_config))
    for key, value in extract_rpc_benchmark_extras_from_benchmark_section(benchmark_cfg).items():
        merged_config[key] = copy.deepcopy(value)
    return merged_config


def _rpc_runtime_config_from_test_config(test_config: Mapping[str, Any]) -> RPCRuntimeConfig:
    scene_id_raw = test_config.get("workload_id") or test_config.get("test_id") or ""
    scene_id = _canonicalize_rpc_scene_family(scene_id_raw)
    backend_kind = str(test_config.get(RPC_BENCHMARK_KEY_BACKEND_KIND, "")).strip().upper()
    if backend_kind not in RPC_BACKENDS_ALLOWED:
        raise ValueError(f"unsupported RPC backend kind: {backend_kind!r}")
    path = str(test_config.get(RPC_BENCHMARK_KEY_PATH, RPC_DEFAULT_PATH)).strip()
    if not path:
        raise ValueError("rpc_path must be non-empty")
    payload_size = int(test_config.get(RPC_BENCHMARK_KEY_PAYLOAD_SIZE, 0))
    if payload_size <= 0:
        raise ValueError(f"rpc_payload_size must be > 0, got: {payload_size}")
    if RPC_BENCHMARK_KEY_PAYLOAD_MODE not in test_config:
        raise ValueError("rpc_payload_mode must be provided explicitly")
    payload_mode_raw = test_config.get(RPC_BENCHMARK_KEY_PAYLOAD_MODE)
    payload_mode = str(payload_mode_raw).strip().upper()
    if payload_mode not in RPC_PAYLOAD_MODES_ALLOWED:
        raise ValueError(f"unsupported rpc_payload_mode: {payload_mode_raw!r}")
    if RPC_BENCHMARK_KEY_SERVER_SOURCE not in test_config:
        raise ValueError("rpc_server_source must be provided explicitly")
    server_source = str(test_config.get(RPC_BENCHMARK_KEY_SERVER_SOURCE)).strip()
    if server_source not in RPC_SERVER_SOURCES_ALLOWED:
        raise ValueError(f"unsupported rpc_server_source: {server_source!r}")
    target_role: Optional[str] = None
    if server_source == RPC_SERVER_SOURCE_BENCHMARK_NODE_ROLE:
        target_role = canonicalize_kv_node_role(test_config.get(RPC_BENCHMARK_KEY_TARGET_ROLE, ""))
    raw_keys = test_config.get(RPC_BENCHMARK_KEY_SERVER_INSTANCE_KEYS)
    if not isinstance(raw_keys, list) or not raw_keys:
        raise ValueError("rpc_server_instance_keys must be a non-empty list")
    server_instance_keys = tuple(str(raw).strip() for raw in raw_keys if str(raw).strip())
    if not server_instance_keys:
        raise ValueError("rpc_server_instance_keys normalized to empty list")
    raw_targets = test_config.get(RPC_BENCHMARK_KEY_SERVER_TARGETS)
    if not isinstance(raw_targets, dict) or not raw_targets:
        raise ValueError("rpc_server_targets must be a non-empty dict")
    server_targets = {
        str(raw_key).strip(): str(raw_value).strip()
        for raw_key, raw_value in raw_targets.items()
        if str(raw_key).strip() and str(raw_value).strip()
    }
    raw_ports = test_config.get(RPC_BENCHMARK_KEY_SERVER_ZERORPC_PORTS, {})
    if not isinstance(raw_ports, dict):
        raise ValueError("rpc_server_zero_rpc_ports must be a dict when present")
    server_zero_rpc_ports = {
        str(raw_key).strip(): int(raw_value)
        for raw_key, raw_value in raw_ports.items()
        if str(raw_key).strip()
    }
    if backend_kind == RPC_BACKEND_KIND_ZERORPC and payload_mode != RPC_PAYLOAD_MODE_BYTES:
        raise ValueError("ZERORPC benchmark only supports rpc_payload_mode=BYTES")
    return RPCRuntimeConfig(
        scene_id=scene_id,
        backend_kind=backend_kind,
        path=path,
        payload_size=payload_size,
        payload_mode=payload_mode,
        server_source=server_source,
        target_role=target_role,
        server_instance_keys=server_instance_keys,
        server_targets=server_targets,
        server_zero_rpc_ports=server_zero_rpc_ports,
    )


def _benchmark_node_is_rpc_server(*, runtime_cfg: RPCRuntimeConfig, node_role: str) -> bool:
    if runtime_cfg.server_source == RPC_SERVER_SOURCE_BENCHMARK_NODE_ALL:
        return True
    if runtime_cfg.server_source != RPC_SERVER_SOURCE_BENCHMARK_NODE_ROLE:
        return False
    if runtime_cfg.target_role is None:
        raise ValueError("rpc target role is missing for benchmark_node_role server source")
    return node_role == runtime_cfg.target_role


def _benchmark_node_rpc_server_runs_client_loop(*, runtime_cfg: RPCRuntimeConfig) -> bool:
    return runtime_cfg.server_source == RPC_SERVER_SOURCE_BENCHMARK_NODE_ALL


def _rpc_payload_bytes(*, runtime_cfg: RPCRuntimeConfig, thread_id: int, op_idx: int) -> bytes:
    seed = f"{runtime_cfg.scene_id}:{runtime_cfg.backend_kind}:{thread_id}:{op_idx}"
    block = hashlib.sha256(seed.encode("utf-8")).digest()
    out = bytearray()
    while len(out) < runtime_cfg.payload_size:
        out.extend(block)
    return bytes(out[: runtime_cfg.payload_size])


def _rpc_payload_flatdict(
    *,
    runtime_cfg: RPCRuntimeConfig,
    thread_id: int,
    op_idx: int,
) -> Dict[str, bytes]:
    return {
        RPC_ECHO_FLATDICT_PAYLOAD_KEY: _rpc_payload_bytes(
            runtime_cfg=runtime_cfg,
            thread_id=thread_id,
            op_idx=op_idx,
        )
    }


def _rpc_target_instance_key(
    runtime_cfg: RPCRuntimeConfig,
    *,
    benchmark_node: Any,
    thread_id: int,
    op_idx: int,
) -> str:
    keys = runtime_cfg.server_instance_keys
    if not keys:
        raise ValueError("rpc server instance keys are empty")
    bucket = _stable_bucket(
        (runtime_cfg.scene_id, benchmark_node.instance_key or benchmark_node.node_id, thread_id, op_idx, "rpc_target")
    )
    return keys[int(bucket % len(keys))]


def _spawn_zerorpc_echo_server(*, endpoint: str) -> _ZeroRpcServerHandle:
    server_code = (
        "import zerorpc\n"
        "class Echo:\n"
        "    def echo(self, payload):\n"
        "        return payload\n"
        "server = zerorpc.Server(Echo())\n"
        f"server.bind({endpoint!r})\n"
        "server.run()\n"
    )
    proc = subprocess.Popen(
        [sys.executable, "-u", "-c", server_code],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    deadline = time.time() + 10.0
    last_error: Optional[Exception] = None
    while time.time() < deadline:
        if proc.poll() is not None:
            output = ""
            if proc.stdout is not None:
                output = proc.stdout.read()
            raise RuntimeError(
                "zerorpc server exited early "
                f"rc={proc.returncode} endpoint={endpoint} output={output!r}"
            )
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(0.2)
            host, port_text = endpoint.removeprefix("tcp://").rsplit(":", 1)
            sock.connect((host, int(port_text)))
            sock.close()
            return _ZeroRpcServerHandle(proc, endpoint)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(0.1)
        finally:
            sock.close()
    proc.terminate()
    proc.wait(timeout=5.0)
    output = ""
    if proc.stdout is not None:
        output = proc.stdout.read()
    raise RuntimeError(
        "zerorpc server did not become ready "
        f"endpoint={endpoint} last_error={last_error} output={output!r}"
    )


def prepare_rpc_before_ready(benchmark_node: Any) -> bool:
    test_config = getattr(benchmark_node, "test_config", None)
    if not isinstance(test_config, dict):
        return False
    test_mode = str(test_config.get("test_mode", "")).strip().upper()
    if test_mode != TEST_MODE_RPC:
        return False
    if benchmark_node.kv_store is None:
        raise RuntimeError("RPC benchmark requires kv_store to be initialized")
    runtime_cfg = _rpc_runtime_config_from_test_config(test_config)
    node_role = canonicalize_kv_node_role(test_config.get("node_role", ""))
    is_rpc_server = _benchmark_node_is_rpc_server(runtime_cfg=runtime_cfg, node_role=node_role)
    if not is_rpc_server:
        return True
    if runtime_cfg.backend_kind == RPC_BACKEND_KIND_FLUXON:
        rpc_store = getattr(benchmark_node, "_fluxon_rpc_store", None)
        if rpc_store is None:
            rpc_store = _FluxonRpcStore(benchmark_node.kv_store)
            attach_phase_callback = getattr(
                benchmark_node,
                "_attach_fluxon_phase_summary_callback",
                None,
            )
            if callable(attach_phase_callback):
                attach_phase_callback(rpc_store)
            setattr(benchmark_node, "_fluxon_rpc_store", rpc_store)
        rpc_store.register_echo_handler(
            path=runtime_cfg.path,
            payload_mode=runtime_cfg.payload_mode,
        )
        return True
    if runtime_cfg.backend_kind == RPC_BACKEND_KIND_ZERORPC:
        instance_key = str(
            benchmark_node.instance_key or test_config.get("instance_key") or ""
        ).strip()
        if not instance_key:
            raise ValueError("zerorpc benchmark server requires instance_key")
        if instance_key not in runtime_cfg.server_zero_rpc_ports:
            raise ValueError(f"zerorpc port missing for instance_key={instance_key}")
        endpoint = f"tcp://0.0.0.0:{int(runtime_cfg.server_zero_rpc_ports[instance_key])}"
        existing_server = getattr(benchmark_node, "_fluxon_zerorpc_server", None)
        if existing_server is not None:
            if getattr(existing_server, "endpoint", None) == endpoint:
                return True
            existing_server.close()
        server_handle = _spawn_zerorpc_echo_server(endpoint=endpoint)
        setattr(benchmark_node, "_fluxon_zerorpc_server", server_handle)
        return True
    raise ValueError(f"unsupported rpc backend kind: {runtime_cfg.backend_kind}")


def run_rpc_worker(
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
    test_mode = str(test_config.get("test_mode", "")).strip().upper()
    if test_mode != TEST_MODE_RPC:
        return None

    runtime_cfg = _rpc_runtime_config_from_test_config(test_config)
    node_role = canonicalize_kv_node_role(test_config.get("node_role", ""))
    is_rpc_server = _benchmark_node_is_rpc_server(runtime_cfg=runtime_cfg, node_role=node_role)
    if is_rpc_server and not _benchmark_node_rpc_server_runs_client_loop(runtime_cfg=runtime_cfg):
        debug_print(f"thread {thread_id} exit rpc server role={node_role} total_ops=0")
        return []

    results: list[Any] = []
    op_idx = 0
    op_timeout_s = float(test_config["op_timeout_seconds"])
    zerorpc_client_cache: Dict[str, _ZeroRpcClient] = {}
    while True:
        now_ts = time.time()
        if now_ts >= float(deadline_ts):
            break
        inflight_at_start = benchmark_node._inflight_begin()
        try:
            target_instance_key = _rpc_target_instance_key(
                runtime_cfg,
                benchmark_node=benchmark_node,
                thread_id=thread_id,
                op_idx=op_idx,
            )
            if runtime_cfg.payload_mode == RPC_PAYLOAD_MODE_BYTES:
                payload: Union[bytes, Dict[str, bytes]] = _rpc_payload_bytes(
                    runtime_cfg=runtime_cfg,
                    thread_id=thread_id,
                    op_idx=op_idx,
                )
            elif runtime_cfg.payload_mode == RPC_PAYLOAD_MODE_FLATDICT:
                payload = _rpc_payload_flatdict(
                    runtime_cfg=runtime_cfg,
                    thread_id=thread_id,
                    op_idx=op_idx,
                )
            else:
                raise ValueError(f"unsupported rpc payload mode: {runtime_cfg.payload_mode!r}")
            started_at = time.time()
            if runtime_cfg.backend_kind == RPC_BACKEND_KIND_FLUXON:
                rpc_store = getattr(benchmark_node, "_fluxon_rpc_store", None)
                if rpc_store is None:
                    if benchmark_node.kv_store is None:
                        raise RuntimeError("RPC benchmark requires kv_store to be initialized")
                    rpc_store = _FluxonRpcStore(benchmark_node.kv_store)
                    attach_phase_callback = getattr(
                        benchmark_node,
                        "_attach_fluxon_phase_summary_callback",
                        None,
                    )
                    if callable(attach_phase_callback):
                        attach_phase_callback(rpc_store)
                    setattr(benchmark_node, "_fluxon_rpc_store", rpc_store)
                err = rpc_store.call_echo(
                    target_instance_key=target_instance_key,
                    path=runtime_cfg.path,
                    payload=payload,
                    payload_mode=runtime_cfg.payload_mode,
                    timeout_ms=max(10_000, int(op_timeout_s * 1000.0)),
                    deadline_ts=deadline_ts,
                )
            elif runtime_cfg.backend_kind == RPC_BACKEND_KIND_ZERORPC:
                port = runtime_cfg.server_zero_rpc_ports.get(target_instance_key)
                if port is None:
                    raise ValueError(f"missing zerorpc port for target={target_instance_key}")
                target_host = runtime_cfg.server_targets.get(target_instance_key)
                if not target_host:
                    raise ValueError(f"missing zerorpc target host for target={target_instance_key}")
                endpoint = f"tcp://{target_host}:{int(port)}"
                client = zerorpc_client_cache.get(endpoint)
                if client is None:
                    client = _ZeroRpcClient(endpoint=endpoint)
                    zerorpc_client_cache[endpoint] = client
                err = client.call_echo(payload)
            else:
                raise ValueError(f"unsupported rpc backend kind: {runtime_cfg.backend_kind}")
            finished_at = time.time()
            if isinstance(payload, dict):
                data_size = sum(
                    len(value)
                    for value in payload.values()
                    if isinstance(value, (bytes, bytearray))
                )
            else:
                data_size = len(payload)
            result = operation_result_cls(
                success=err is None,
                latency_us=max(0.0, (finished_at - started_at) * 1_000_000.0),
                operation_type=KV_OPERATION_RPC,
                key=target_instance_key,
                data_size=data_size,
                inflight_at_start=inflight_at_start,
                outcome_kind=(
                    operation_outcome.SUCCESS
                    if err is None
                    else operation_outcome.ERROR
                ),
                error_msg=err,
            )
        except Exception as exc:  # noqa: BLE001
            result = operation_result_cls(
                success=False,
                latency_us=0.0,
                operation_type=KV_OPERATION_RPC,
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

    for client in zerorpc_client_cache.values():
        client.close()
    debug_print(
        f"thread {thread_id} exit rpc run loop, total_ops={len(results)}, last_op_idx={op_idx}"
    )
    return results


def close_rpc_runtime(benchmark_node: Any) -> None:
    zerorpc_server = getattr(benchmark_node, "_fluxon_zerorpc_server", None)
    if zerorpc_server is not None:
        zerorpc_server.close()
        setattr(benchmark_node, "_fluxon_zerorpc_server", None)
