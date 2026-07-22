"""Fluxon backend implementation for the KV Cache API layer.

This module provides a concrete implementation using the PyO3 Rust bindings.
"""

from dataclasses import dataclass
from typing import Union, Optional, Callable, Any, Dict, List, Tuple
import ctypes
import os
from pathlib import Path
import sys
import sysconfig
import threading
import time

# Import the PyO3 module
try:
    from ..tool import import_fluxon_pyo3_local

    fluxon_pyo3 = import_fluxon_pyo3_local()
except ImportError as e:
    raise ImportError(
        f"Failed to import fluxon_pyo3: {e}. If u need to use fluxonkv, make sure the Rust backend package is installed."
    ) from e

from .kvclient_interface import KvClient
from .kvclient_interface import KvLeaseApi, KvRpcApi, PutOptionalArgs, FlatDict
from .backend_fallback_close import unregister_store_from_cleanup
from .kvclient_interface import GetStartHandle, GetStartResult, KvFuture, MemHolder
from .nonzerocopy_encode import (
    DLPacked,
    INTERNAL_DLPACK_META_KEY,
    _dlpack_cpu_tensor_info,
    encode_dlpack_meta,
    encode_flat_kv_dict,
)
from ..config import FluxonKvClientConfig
from ..api_error import (
    Result,
    ApiError,
    OkNone,
    GeneralError,
    InvalidArgumentError,
    StoreInitFailedError,
    ValueTooLargeError,
)
import logging
from ..metrics import MetricSnapshot


_PyBytes_AsString = ctypes.pythonapi.PyBytes_AsString
_PyBytes_AsString.argtypes = [ctypes.py_object]
_PyBytes_AsString.restype = ctypes.c_void_p


_SIDE_TRANSFER_WORKER_PYTHON_ENV = "FLUXON_KV_SIDE_WORKER_PYTHON"
_BLOCKING_PUT_OUTER_TOTAL_LOG_INTERVAL_NS = 10 * 1_000_000_000
_MIN_EXPLICIT_RPC_TIMEOUT_MS = int(fluxon_pyo3.MIN_EXPLICIT_RPC_TIMEOUT_MS)


def _validate_explicit_rpc_timeout_ms(timeout_ms: int) -> None:
    if not isinstance(timeout_ms, int):
        raise InvalidArgumentError(message=f"timeout_ms must be int; got {type(timeout_ms)}")
    if timeout_ms < _MIN_EXPLICIT_RPC_TIMEOUT_MS:
        raise InvalidArgumentError(
            message=(
                f"timeout_ms must be >= {_MIN_EXPLICIT_RPC_TIMEOUT_MS}; "
                f"got {timeout_ms}"
            )
        )


def _validate_put_atomic_groups(
    keys: List[str],
    atomic_group_lens: Optional[List[int]],
    make_replica_task_mask: Optional[List[bool]],
) -> Optional[List[int]]:
    if atomic_group_lens is None:
        return None
    if not isinstance(atomic_group_lens, list):
        raise ValueError(
            "atomic_group_lens must be list[int] when provided; "
            f"got {type(atomic_group_lens)}"
        )
    normalized_group_lens: List[int] = []
    for index, length in enumerate(atomic_group_lens):
        if type(length) is not int:
            raise ValueError(
                "atomic_group_lens items must be int; "
                f"index={index} got={type(length)}"
            )
        if length <= 0:
            raise ValueError(
                "atomic_group_lens entries must be > 0; "
                f"index={index} got={length}"
            )
        normalized_group_lens.append(length)
    group_sum = sum(normalized_group_lens)
    if group_sum != len(keys):
        raise ValueError(
            "atomic_group_lens must sum to keys length; "
            f"sum={group_sum} keys={len(keys)}"
        )
    if make_replica_task_mask is not None:
        offset = 0
        for group_index, group_len in enumerate(normalized_group_lens):
            group_mask = make_replica_task_mask[offset : offset + group_len]
            if any(item != group_mask[0] for item in group_mask[1:]):
                raise ValueError(
                    "make_replica_task_mask must be uniform within each atomic group; "
                    f"group_index={group_index} offset={offset} len={group_len}"
                )
            offset += group_len
    return normalized_group_lens


def _percentile_nearest_rank_ns(sorted_values: List[int], percentile: int) -> int:
    idx = ((len(sorted_values) * percentile + 99) // 100) - 1
    idx = max(0, min(idx, len(sorted_values) - 1))
    return sorted_values[idx]


class _BlockingPutOuterTotalLogWindow:
    def __init__(self, store_tag: str):
        self._store_tag = store_tag
        self._lock = threading.Lock()
        self._window_started_at_ns: Optional[int] = None
        self._samples_ns: List[int] = []

    def record_success(self, total_ns: int) -> None:
        maybe_log: Optional[tuple[int, List[int]]] = None
        with self._lock:
            now_ns = time.monotonic_ns()
            if self._window_started_at_ns is None:
                self._window_started_at_ns = now_ns
            self._samples_ns.append(total_ns)
            window_started_at_ns = self._window_started_at_ns
            assert window_started_at_ns is not None
            if now_ns - window_started_at_ns < _BLOCKING_PUT_OUTER_TOTAL_LOG_INTERVAL_NS:
                return
            maybe_log = (now_ns - window_started_at_ns, self._samples_ns)
            self._samples_ns = []
            self._window_started_at_ns = now_ns
        if maybe_log is None:
            return
        elapsed_ns, samples_ns = maybe_log
        if len(samples_ns) == 0:
            return
        values_ns = sorted(samples_ns)
        avg_us = (sum(values_ns) / len(values_ns)) / 1_000.0
        p95_us = _percentile_nearest_rank_ns(values_ns, 95) / 1_000.0
        logging.info(
            "%s blocking_put_outer_total_window samples=%d window_s=%.1f "
            "blocking_put_outer_total_avg_us=%.1f blocking_put_outer_total_p95_us=%.1f",
            self._store_tag,
            len(values_ns),
            elapsed_ns / 1_000_000_000.0,
            avg_us,
            p95_us,
        )


def _resolve_side_transfer_worker_python() -> str:
    configured = os.environ.get(_SIDE_TRANSFER_WORKER_PYTHON_ENV)
    if configured:
        return configured

    candidates: List[Path] = []
    prefix = Path(sys.prefix)
    scripts_dir = sysconfig.get_path("scripts")
    if scripts_dir:
        candidates.append(Path(scripts_dir))
    if prefix.as_posix() not in {".", ""}:
        candidates.append(prefix / "bin")

    seen: set[str] = set()
    ordered_bins: List[Path] = []
    for bin_dir in candidates:
        key = str(bin_dir)
        if key in seen:
            continue
        seen.add(key)
        ordered_bins.append(bin_dir)

    for bin_dir in ordered_bins:
        for name in (
            f"python{sys.version_info.major}.{sys.version_info.minor}",
            f"python{sys.version_info.major}",
            "python3",
            "python",
        ):
            candidate = bin_dir / name
            if candidate.is_file() and os.access(candidate, os.X_OK):
                return str(candidate)

    return sys.executable


@dataclass(frozen=True)
class _RegisteredBufferDescriptor:
    ptr: int
    size: int
    device_kind: str = "host"
    device_id: str = ""
    layout: str = "raw"
    metadata: Optional[Dict[str, Any]] = None

    @property
    def end(self) -> int:
        return self.ptr + self.size

    def contains(self, ptr: int, size: int) -> bool:
        req_end = ptr + size
        return ptr >= self.ptr and req_end <= self.end

    def as_dict(self) -> Dict[str, Any]:
        return {
            "ptr": self.ptr,
            "size": self.size,
            "device_kind": self.device_kind,
            "device_id": self.device_id,
            "layout": self.layout,
            "metadata": dict(self.metadata or {}),
        }


@dataclass(frozen=True)
class GpuBufferRegistration:
    registration_id: int
    ptr: int
    size: int
    device_id: int

    def __post_init__(self) -> None:
        if type(self.registration_id) is not int or self.registration_id <= 0:
            raise ValueError("registration_id must be a positive int")
        if type(self.ptr) is not int or self.ptr <= 0:
            raise ValueError("GPU registration ptr must be a positive int")
        if type(self.size) is not int or self.size <= 0:
            raise ValueError("GPU registration size must be a positive int")
        if type(self.device_id) is not int or self.device_id < 0:
            raise ValueError("GPU device_id must be a non-negative int")
        if self.end > (1 << 64):
            raise ValueError("GPU registration range overflows u64")

    @property
    def end(self) -> int:
        return self.ptr + self.size

    def destination(self, ptr: int, capacity: int) -> "GpuDestination":
        destination = GpuDestination(
            registration_id=self.registration_id,
            ptr=ptr,
            capacity=capacity,
        )
        if destination.ptr < self.ptr or destination.end > self.end:
            raise ValueError(
                "GPU destination is outside its registration: "
                f"registration=[{self.ptr:#x},{self.end:#x}) "
                f"destination=[{destination.ptr:#x},{destination.end:#x})"
            )
        return destination


@dataclass(frozen=True)
class GpuDestination:
    registration_id: int
    ptr: int
    capacity: int

    def __post_init__(self) -> None:
        if type(self.registration_id) is not int or self.registration_id <= 0:
            raise ValueError("registration_id must be a positive int")
        if type(self.ptr) is not int or self.ptr <= 0:
            raise ValueError("GPU destination ptr must be a positive int")
        if type(self.capacity) is not int or self.capacity <= 0:
            raise ValueError("GPU destination capacity must be a positive int")
        if self.end > (1 << 64):
            raise ValueError("GPU destination range overflows u64")

    @property
    def end(self) -> int:
        return self.ptr + self.capacity


@dataclass
class GpuGetStartHandle:
    """One-shot handle for a background RDMA pull into GPU staging."""

    keys: Tuple[str, ...]
    destinations: Tuple[GpuDestination, ...]
    result: GetStartResult
    created_at_ns: int
    backend_token: int
    backend_handle: int
    closed: bool = False


def _map_nospace_to_storagefull(err: ApiError) -> ApiError:
    """Normalize storage-capacity errors without depending on backend internals."""
    return err


def _get_start_prefix_hit_groups(
    raw_prefix_hit_len: int,
    group_lens: Tuple[int, ...],
) -> int:
    prefix_hit_groups = 0
    transferable_len = 0
    for group_len in group_lens:
        next_transferable_len = transferable_len + group_len
        if next_transferable_len > raw_prefix_hit_len:
            break
        transferable_len = next_transferable_len
        prefix_hit_groups += 1
    return prefix_hit_groups


def _get_start_group_index_for_key_index(
    group_lens: Tuple[int, ...],
    key_index: int,
) -> Optional[int]:
    cursor = 0
    for group_index, group_len in enumerate(group_lens):
        cursor += group_len
        if key_index < cursor:
            return group_index
    return None


def _build_get_start_result_from_backend_payload(
    payload: Dict[str, Any],
    keys: List[str],
    prefix_best_effort: bool,
    normalized_group_lens: Optional[List[int]],
) -> GetStartResult:
    result_keys = tuple(keys)
    group_lens = (
        (len(result_keys),)
        if normalized_group_lens is None
        else tuple(normalized_group_lens)
    )
    raw_prefix_hit_len = int(payload["raw_prefix_hit_len"])
    if raw_prefix_hit_len < 0 or raw_prefix_hit_len > len(result_keys):
        raise RuntimeError(
            "get_start returned invalid raw_prefix_hit_len: "
            f"raw_prefix_hit_len={raw_prefix_hit_len} keys={len(result_keys)}"
        )
    prefix_hit_groups = _get_start_prefix_hit_groups(
        raw_prefix_hit_len,
        group_lens,
    )
    if not prefix_best_effort and prefix_hit_groups != len(group_lens):
        transferable_len = 0
        prefix_hit_groups = 0
    else:
        transferable_len = sum(group_lens[:prefix_hit_groups])
    first_miss_index = (
        None if raw_prefix_hit_len == len(result_keys) else raw_prefix_hit_len
    )
    first_miss_group_index = (
        None
        if first_miss_index is None
        else _get_start_group_index_for_key_index(group_lens, first_miss_index)
    )
    return GetStartResult(
        keys=result_keys,
        raw_prefix_hit_len=raw_prefix_hit_len,
        transferable_len=transferable_len,
        prefix_hit_groups=prefix_hit_groups,
        atomic_group_lens=tuple(normalized_group_lens) if normalized_group_lens is not None else None,
        prefix_best_effort=prefix_best_effort,
        first_miss_index=first_miss_index,
        first_miss_group_index=first_miss_group_index,
        all_hit=transferable_len == len(result_keys),
    )


def _error_to_ret_code(err: ApiError) -> int:
    if hasattr(err, "code") and callable(err.code):
        try:
            return -int(err.code())
        except Exception:
            return -1
    return -1


def _get_bytes_ptr_len(b: bytes, keepalive: List[bytes]) -> tuple[int, int]:
    if not isinstance(b, bytes):
        raise InvalidArgumentError(message=f"expected bytes to export a pointer; got {type(b)}")
    keepalive.append(b)
    ptr = _PyBytes_AsString(b)
    if not ptr and len(b) != 0:
        raise InvalidArgumentError(message="PyBytes_AsString returned NULL")
    return (int(ptr), len(b))


def _i64_to_u64_bits(v: int) -> int:
    if v < -(1 << 63) or v > (1 << 63) - 1:
        raise InvalidArgumentError(message=f"int out of int64 range: {v!r}")
    return int(ctypes.c_uint64(ctypes.c_int64(v).value).value)


def _f64_to_u64_bits(v: float) -> int:
    d = ctypes.c_double(v)
    bits_ptr = ctypes.cast(ctypes.pointer(d), ctypes.POINTER(ctypes.c_uint64))
    return int(bits_ptr.contents.value)


def build_flat_dict_ptrs(
    value: Dict[str, Union[int, float, bool, str, bytes, DLPacked]],
    keepalive: List[bytes],
    dlpack_capsules: List[object],
) -> List[tuple[int, int, int, int, int, Optional[int]]]:
    if INTERNAL_DLPACK_META_KEY in value:
        raise InvalidArgumentError(message=f"Reserved key not allowed: {INTERNAL_DLPACK_META_KEY!r}")

    ptrs: List[tuple[int, int, int, int, int, Optional[int]]] = []
    dlpack_meta: list[tuple[str, int, int, int, tuple[int, ...]]] = []
    for field_key, field_val in value.items():
        if not isinstance(field_key, str):
            raise InvalidArgumentError(message=f"KV put() requires string keys only; got {type(field_key)}")

        key_bytes = field_key.encode("utf-8")
        key_ptr, key_len = _get_bytes_ptr_len(key_bytes, keepalive)

        if isinstance(field_val, bool):
            ptrs.append((7, key_ptr, key_len, 1 if field_val else 0, 1, None))
            continue

        if isinstance(field_val, int):
            bits = _i64_to_u64_bits(field_val)
            ptrs.append((1, key_ptr, key_len, bits, 8, None))
            continue

        if isinstance(field_val, float):
            bits = _f64_to_u64_bits(field_val)
            ptrs.append((3, key_ptr, key_len, bits, 8, None))
            continue

        if isinstance(field_val, str):
            type_id = 4
            val_buf = field_val.encode("utf-8")
        elif isinstance(field_val, bytes):
            type_id = 5
            val_buf = field_val
        elif hasattr(field_val, "__dlpack__"):
            info = _dlpack_cpu_tensor_info(field_val)  # type: ignore[arg-type]
            if not info.is_ok():
                raise info.unwrap_error()
            ptr, nbytes, capsule, dtype_code, bits, lanes, shape = info.unwrap()
            dlpack_capsules.append(capsule)
            dlpack_meta.append((field_key, dtype_code, bits, lanes, shape))
            ptrs.append((5, key_ptr, key_len, ptr, nbytes, None))
            continue
        else:
            raise InvalidArgumentError(
                message=(
                    "KV put() only supports flat dict values of int|float|bool|str|bytes|dlpack; "
                    f"key={field_key!r} type={type(field_val)}"
                )
            )

        val_ptr, val_len = _get_bytes_ptr_len(val_buf, keepalive)
        ptrs.append((type_id, key_ptr, key_len, val_ptr, val_len, None))

    if dlpack_meta:
        meta_blob = encode_dlpack_meta(dlpack_meta)
        meta_key_bytes = INTERNAL_DLPACK_META_KEY.encode("utf-8")
        meta_key_ptr, meta_key_len = _get_bytes_ptr_len(meta_key_bytes, keepalive)
        meta_val_ptr, meta_val_len = _get_bytes_ptr_len(meta_blob, keepalive)
        ptrs.append((5, meta_key_ptr, meta_key_len, meta_val_ptr, meta_val_len, None))
    return ptrs


def _build_payload_field_ptrs(
    payload_ptr: int,
    payload_size: int,
    keepalive: List[bytes],
) -> List[tuple[int, int, int, int, int, Optional[int]]]:
    if not isinstance(payload_ptr, int):
        raise InvalidArgumentError(
            message=f"payload_ptr must be int; got {type(payload_ptr)}"
        )
    if not isinstance(payload_size, int):
        raise InvalidArgumentError(
            message=f"payload_size must be int; got {type(payload_size)}"
        )
    if payload_ptr < 0:
        raise InvalidArgumentError(message=f"payload_ptr must be >= 0; got {payload_ptr}")
    if payload_size < 0:
        raise InvalidArgumentError(
            message=f"payload_size must be >= 0; got {payload_size}"
        )
    if payload_size > 0xFFFF_FFFF:
        raise ValueTooLargeError(
            message=f"payload_size exceeds u32 limit: {payload_size}",
            value_size=payload_size,
            max_size=0xFFFF_FFFF,
        )

    key_bytes = b"payload"
    key_ptr, key_len = _get_bytes_ptr_len(key_bytes, keepalive)
    return [(5, key_ptr, key_len, payload_ptr, payload_size, None)]


class FluxonMemHolder(MemHolder):
    """Concrete implementation of MemHolder using PyO3 Rust bindings."""

    def __init__(self, inner_holder: Any):
        self._inner_holder = inner_holder

    def access(self) -> Result[Dict[str, Union[int, float, bool, str, bytes, DLPacked]], ApiError]:
        res = self._inner_holder.access()
        if not res.is_ok():
            return Result.new_error(res.unwrap_error())
        return Result.new_ok(res.unwrap())

    # release() intentionally omitted for now.


class FluxonKVCacheStore(KvClient, KvLeaseApi, KvRpcApi):
    """Concrete implementation of KvClient using PyO3 Rust bindings.

    The actual backend client is created in the constructor so that
    backend-specific initialization logic lives inside the fluxon
    client implementation instead of the top-level ``new_store``
    factory.
    """

    def __init__(self, config: FluxonKvClientConfig):
        self._client: Optional[fluxon_pyo3.KvClient] = None
        self._config = config
        self._init_error: Optional[ApiError] = None
        self._registered_buffer_descriptors: List[_RegisteredBufferDescriptor] = []
        self._gpu_buffer_registration: Optional[GpuBufferRegistration] = None
        cluster_name = config.fluxonkv_spec_cluster_name
        self._batch_concurrency = 128
        if self._batch_concurrency <= 0:
            raise ValueError("batch_concurrency must be > 0")
        self._blocking_put_outer_total_log_window = _BlockingPutOuterTotalLogWindow(
            f"FluxonKVCacheStore[{cluster_name}]"
        )

        # Keep Python-spawned side workers on the same interpreter/venv as the owner.
        side_worker_python = _resolve_side_transfer_worker_python()
        os.environ.setdefault(_SIDE_TRANSFER_WORKER_PYTHON_ENV, side_worker_python)

        config_yaml = config.to_fluxon_kv_client_config_yaml_str()
        result = fluxon_pyo3.KvClient.new(config_yaml)

        logging.info("new FluxonKVCacheStore result type: %s", type(result))

        if not result.is_ok():
            err = result.unwrap_error()
            logging.error(f"new FluxonKVCacheStore error: {err}")
            self._init_error = err
            return

        client = result.unwrap()
        assert client is not None

        self._client = client

    @classmethod
    def new(cls, config: FluxonKvClientConfig) -> Result["FluxonKVCacheStore", ApiError]:
        """Factory-style constructor used by ``new_store``.

        This enforces the FactoryOnly pattern and converts
        constructor-side initialization status into a :class:`Result`.
        """
        cls._allow_init = True
        try:
            store = cls(config)
        finally:
            cls._allow_init = False

        init_error = store._init_error
        if init_error is not None:
            return Result.new_error(init_error)

        return Result.new_ok(store)

    def put(
        self,
        key: str,
        value: Dict[str, Union[int, float, bool, str, bytes, DLPacked]],
        opts: Optional[PutOptionalArgs] = None,
    ) -> Result[KvFuture, ApiError]:
        keepalive: List[bytes] = []
        dlpack_capsules: List[object] = []

        try:
            # We pass raw pointers into Rust so the backend can encode directly into the segment memory
            # without holding the Python GIL.
            #
            # Safety/lifetime contract:
            # - For bytes-like values (str/bytes), pointers must remain valid until
            #   the Rust async put future has finished copying.
            # - We enforce this by keeping the underlying `bytes` objects alive in
            #   `_FluxonPutFuture` until `wait()` completes.
            # - For dlpack, we keep the capsule alive until the future completes.
            ptrs = build_flat_dict_ptrs(value, keepalive, dlpack_capsules)
            # Only accept PutOptionalArgs for optional params; extract for PyO3
            lease_id: Optional[int] = opts.lease_id if opts is not None else None
            reject_if_inflight_same_key = (
                bool(opts.reject_if_inflight_same_key) if opts is not None else False
            )
            reject_if_exist_same_key = (
                bool(opts.reject_if_exist_same_key) if opts is not None else False
            )
            write_through = bool(opts.write_through) if opts is not None else True
            make_replica_task = (
                bool(opts.make_replica_task) if opts is not None else True
            )
            inner_res = self._client.put(
                key,
                ptrs,
                lease_id=lease_id,
                reject_if_inflight_same_key=reject_if_inflight_same_key,
                reject_if_exist_same_key=reject_if_exist_same_key,
                write_through=write_through,
                make_replica_task=make_replica_task,
            )
            if not inner_res.is_ok():
                err = inner_res.unwrap_error()
                mapped = _map_nospace_to_storagefull(err)
                return Result.new_error(mapped)

            inner_future = inner_res.unwrap()
            assert inner_future is not None
            outer_future = _FluxonPutFuture(inner_future, keepalive, dlpack_capsules)
            keepalive = []
            dlpack_capsules = []
            return Result.new_ok(outer_future)
        except ApiError as e:
            return Result.new_error(e)
        finally:
            keepalive.clear()
            dlpack_capsules.clear()

    def get(
        self,
        key: str,
    ) -> Result[KvFuture, ApiError]:
        if self._client is None:
            return Result.new_error(
                GeneralError(message="Store not initialized when get(). Call setup() first.")
            )
        return self._client.get(key)

    def put_blocking(
        self,
        key: str,
        value: Dict[str, Union[int, float, bool, str, bytes, DLPacked]],
        opts: Optional[PutOptionalArgs] = None,
    ) -> Result[OkNone, ApiError]:
        keepalive: List[bytes] = []
        dlpack_capsules: List[object] = []
        total_started_at_ns = time.monotonic_ns()
        try:
            ptrs = build_flat_dict_ptrs(value, keepalive, dlpack_capsules)
            lease_id: Optional[int] = opts.lease_id if opts is not None else None
            reject_if_inflight_same_key = (
                bool(opts.reject_if_inflight_same_key) if opts is not None else False
            )
            reject_if_exist_same_key = (
                bool(opts.reject_if_exist_same_key) if opts is not None else False
            )
            write_through = bool(opts.write_through) if opts is not None else True
            make_replica_task = (
                bool(opts.make_replica_task) if opts is not None else True
            )
            inner_res = self._client.put_blocking(
                key,
                ptrs,
                lease_id=lease_id,
                reject_if_inflight_same_key=reject_if_inflight_same_key,
                reject_if_exist_same_key=reject_if_exist_same_key,
                write_through=write_through,
                make_replica_task=make_replica_task,
            )
            if not inner_res.is_ok():
                return Result.new_error(inner_res.unwrap_error())
            _ = inner_res.unwrap()
            self._blocking_put_outer_total_log_window.record_success(
                time.monotonic_ns() - total_started_at_ns
            )
            return Result.new_ok(OkNone())
        except ApiError as e:
            return Result.new_error(e)
        finally:
            keepalive.clear()
            dlpack_capsules.clear()

    def get_blocking(self, key: str) -> Result[MemHolder, ApiError]:
        if self._client is None:
            return Result.new_error(
                GeneralError(message="Store not initialized when get_blocking(). Call setup() first.")
            )
        try:
            inner_res = self._client.get_blocking(key)
            if not inner_res.is_ok():
                return Result.new_error(inner_res.unwrap_error())
            return Result.new_ok(inner_res.unwrap())
        except ApiError as e:
            return Result.new_error(e)

    @staticmethod
    def _normalize_batch_result_list(batch_result: Any, expected_len: int, op_name: str) -> List[Any]:
        if isinstance(batch_result, Result):
            if not batch_result.is_ok():
                raise RuntimeError(f"{op_name} backend error: {batch_result.unwrap_error()}")
            batch_result = batch_result.unwrap()
        if not isinstance(batch_result, list):
            raise RuntimeError(f"{op_name} returned non-list: {type(batch_result)}")
        if len(batch_result) != expected_len:
            raise RuntimeError(
                f"{op_name} returned unexpected length: expected={expected_len} got={len(batch_result)}"
            )
        return list(batch_result)

    def batch_put_blocking(
        self,
        keys: List[str],
        values: List[FlatDict],
        opts: Optional[PutOptionalArgs] = None,
        concurrency: Optional[int] = None,
    ) -> List[Result[OkNone, ApiError]]:
        if len(keys) != len(values):
            raise ValueError("batch_put_blocking requires keys and values to have the same length")
        if len(keys) == 0:
            return []
        if self._client is None:
            err = GeneralError(message="Store not initialized when batch_put_blocking(). Call setup() first.")
            return [Result.new_error(err) for _ in keys]

        lease_id: Optional[int] = opts.lease_id if opts is not None else None
        reject_if_inflight_same_key = (
            bool(opts.reject_if_inflight_same_key) if opts is not None else False
        )
        reject_if_exist_same_key = (
            bool(opts.reject_if_exist_same_key) if opts is not None else False
        )
        write_through = bool(opts.write_through) if opts is not None else True
        make_replica_task = bool(opts.make_replica_task) if opts is not None else True

        keepalive_groups: List[List[bytes]] = []
        dlpack_groups: List[List[object]] = []
        ptr_groups: List[List[tuple[int, int, int, int, int, Optional[int]]]] = []
        try:
            for value in values:
                keepalive: List[bytes] = []
                dlpack_capsules: List[object] = []
                ptr_groups.append(build_flat_dict_ptrs(value, keepalive, dlpack_capsules))
                keepalive_groups.append(keepalive)
                dlpack_groups.append(dlpack_capsules)

            inner_res = self._client.batch_put_blocking(
                keys,
                ptr_groups,
                lease_id=lease_id,
                reject_if_inflight_same_key=reject_if_inflight_same_key,
                reject_if_exist_same_key=reject_if_exist_same_key,
                write_through=write_through,
                make_replica_task=make_replica_task,
                concurrency=concurrency if concurrency is not None else self._batch_concurrency,
            )
            if not inner_res.is_ok():
                err = inner_res.unwrap_error()
                return [Result.new_error(err) for _ in keys]

            batch_results = self._normalize_batch_result_list(
                inner_res.unwrap(), len(keys), "batch_put_blocking"
            )
            submit_out: List[Result[OkNone, ApiError]] = []
            for idx, item in enumerate(batch_results):
                if isinstance(item, Result):
                    submit_out.append(item)
                    continue
                if item is None:
                    submit_out.append(Result.new_ok(OkNone()))
                    continue
                if isinstance(item, int) and item == 0:
                    submit_out.append(Result.new_ok(OkNone()))
                    continue
                if isinstance(item, int):
                    submit_out.append(
                        Result.new_error(
                            GeneralError(
                                message=(
                                    "batch_put_blocking returned backend code "
                                    f"{item} for key {keys[idx]!r}"
                                )
                            )
                        )
                    )
                    continue
                submit_out.append(
                    Result.new_error(
                        GeneralError(
                            message=f"unexpected batch_put result type: {type(item)}"
                        )
                    )
                )
            return submit_out
        except ApiError as e:
            return [Result.new_error(e) for _ in keys]
        except Exception as e:
            return [Result.new_error(GeneralError(f"batch_put_blocking failed: {e}")) for _ in keys]
        finally:
            for keepalive in keepalive_groups:
                keepalive.clear()
            for dlpack_capsules in dlpack_groups:
                dlpack_capsules.clear()

    def batch_get_blocking(
        self,
        keys: List[str],
        concurrency: Optional[int] = None,
    ) -> List[Result[Union[Any, MemHolder], ApiError]]:
        if len(keys) == 0:
            return []
        if self._client is None:
            err = GeneralError(message="Store not initialized when batch_get_blocking(). Call setup() first.")
            return [Result.new_error(err) for _ in keys]

        try:
            inner_res = self._client.batch_get_blocking(
                keys,
                concurrency=concurrency if concurrency is not None else self._batch_concurrency,
            )
            if not inner_res.is_ok():
                err = inner_res.unwrap_error()
                return [Result.new_error(err) for _ in keys]

            batch_results = self._normalize_batch_result_list(
                inner_res.unwrap(), len(keys), "batch_get_blocking"
            )
            out: List[Result[Union[Any, MemHolder], ApiError]] = []
            for idx, item in enumerate(batch_results):
                if isinstance(item, Result):
                    out.append(item)
                    continue
                if item is None:
                    out.append(
                        Result.new_error(
                            GeneralError(message=f"batch_get_blocking returned None for key {keys[idx]!r}")
                        )
                    )
                    continue
                out.append(Result.new_ok(item))
            return out
        except ApiError as e:
            return [Result.new_error(e) for _ in keys]
        except Exception as e:
            return [Result.new_error(GeneralError(f"batch_get_blocking failed: {e}")) for _ in keys]

    def put_payload_from_ptr(
        self,
        key: str,
        payload_ptr: int,
        payload_size: int,
        opts: Optional[PutOptionalArgs] = None,
    ) -> Result[KvFuture, ApiError]:
        if self._client is None:
            return Result.new_error(
                GeneralError(
                    message="Store not initialized when put_payload_from_ptr(). Call setup() first."
                )
            )

        keepalive: List[bytes] = []
        try:
            ptrs = _build_payload_field_ptrs(payload_ptr, payload_size, keepalive)
            lease_id: Optional[int] = opts.lease_id if opts is not None else None
            reject_if_inflight_same_key = (
                bool(opts.reject_if_inflight_same_key) if opts is not None else False
            )
            reject_if_exist_same_key = (
                bool(opts.reject_if_exist_same_key) if opts is not None else False
            )
            write_through = bool(opts.write_through) if opts is not None else True
            make_replica_task = (
                bool(opts.make_replica_task) if opts is not None else True
            )
            inner_res = self._client.put(
                key,
                ptrs,
                lease_id=lease_id,
                reject_if_inflight_same_key=reject_if_inflight_same_key,
                reject_if_exist_same_key=reject_if_exist_same_key,
                write_through=write_through,
                make_replica_task=make_replica_task,
            )
            if not inner_res.is_ok():
                return Result.new_error(_map_nospace_to_storagefull(inner_res.unwrap_error()))
            inner_future = inner_res.unwrap()
            assert inner_future is not None
            outer_future = _FluxonPutFuture(inner_future, keepalive, [])
            keepalive = []
            return Result.new_ok(outer_future)
        except ApiError as e:
            return Result.new_error(e)
        finally:
            keepalive.clear()

    def put_payload_from_ptr_blocking(
        self,
        key: str,
        payload_ptr: int,
        payload_size: int,
        opts: Optional[PutOptionalArgs] = None,
    ) -> Result[OkNone, ApiError]:
        if self._client is None:
            return Result.new_error(
                GeneralError(
                    message="Store not initialized when put_payload_from_ptr_blocking(). Call setup() first."
                )
            )

        keepalive: List[bytes] = []
        try:
            ptrs = _build_payload_field_ptrs(payload_ptr, payload_size, keepalive)
            lease_id: Optional[int] = opts.lease_id if opts is not None else None
            reject_if_inflight_same_key = (
                bool(opts.reject_if_inflight_same_key) if opts is not None else False
            )
            reject_if_exist_same_key = (
                bool(opts.reject_if_exist_same_key) if opts is not None else False
            )
            write_through = bool(opts.write_through) if opts is not None else True
            make_replica_task = (
                bool(opts.make_replica_task) if opts is not None else True
            )
            inner_res = self._client.put_blocking(
                key,
                ptrs,
                lease_id=lease_id,
                reject_if_inflight_same_key=reject_if_inflight_same_key,
                reject_if_exist_same_key=reject_if_exist_same_key,
                write_through=write_through,
                make_replica_task=make_replica_task,
            )
            if not inner_res.is_ok():
                return Result.new_error(inner_res.unwrap_error())
            _ = inner_res.unwrap()
            return Result.new_ok(OkNone())
        except ApiError as e:
            return Result.new_error(e)
        finally:
            keepalive.clear()

    def get_payload_into_ptr_blocking(
        self,
        key: str,
        payload_ptr: int,
        payload_capacity: int,
    ) -> Result[int, ApiError]:
        if self._client is None:
            return Result.new_error(
                GeneralError(
                    message="Store not initialized when get_payload_into_ptr_blocking(). Call setup() first."
                )
            )
        if not isinstance(payload_ptr, int):
            return Result.new_error(
                InvalidArgumentError(
                    message=f"payload_ptr must be int; got {type(payload_ptr)}"
                )
            )
        if not isinstance(payload_capacity, int):
            return Result.new_error(
                InvalidArgumentError(
                    message=f"payload_capacity must be int; got {type(payload_capacity)}"
                )
            )
        if payload_ptr < 0:
            return Result.new_error(
                InvalidArgumentError(
                    message=f"payload_ptr must be >= 0; got {payload_ptr}"
                )
            )
        if payload_capacity < 0:
            return Result.new_error(
                InvalidArgumentError(
                    message=f"payload_capacity must be >= 0; got {payload_capacity}"
                )
            )

        get_res = self.get_blocking(key)
        if not get_res.is_ok():
            return Result.new_error(get_res.unwrap_error())
        holder = get_res.unwrap()
        assert isinstance(holder, MemHolder), (
            f"get_blocking({key!r}) must return MemHolder, got {type(holder)}"
        )

        access_res = holder.access()
        if not access_res.is_ok():
            return Result.new_error(access_res.unwrap_error())
        flat = access_res.unwrap()

        payload = flat.get("payload")
        if not isinstance(payload, (bytes, bytearray)):
            return Result.new_error(
                InvalidArgumentError(
                    message=(
                        f"key {key!r} does not contain bytes payload field; "
                        f"got {type(payload)}"
                    )
                )
            )

        payload_bytes = bytes(payload)
        payload_size = len(payload_bytes)
        if payload_size > payload_capacity:
            return Result.new_error(
                ValueTooLargeError(
                    message=(
                        f"payload for key {key!r} exceeds destination capacity: "
                        f"{payload_size} > {payload_capacity}"
                    ),
                    value_size=payload_size,
                    max_size=payload_capacity,
                )
            )

        if payload_size > 0:
            ctypes.memmove(payload_ptr, payload_bytes, payload_size)

        del flat
        del holder
        return Result.new_ok(payload_size)

    def register_buffer(
        self,
        ptr: int,
        size: int,
        device_kind: str = "host",
        device_id: str = "",
        layout: str = "raw",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Result[OkNone, ApiError]:
        if self._client is None:
            return Result.new_error(
                GeneralError(
                    message="Store not initialized when register_buffer(). Call setup() first."
                )
            )
        if not isinstance(ptr, int):
            return Result.new_error(
                InvalidArgumentError(message=f"ptr must be int; got {type(ptr)}")
            )
        if not isinstance(size, int):
            return Result.new_error(
                InvalidArgumentError(message=f"size must be int; got {type(size)}")
            )
        if ptr < 0:
            return Result.new_error(
                InvalidArgumentError(message=f"ptr must be >= 0; got {ptr}")
            )
        if size < 0:
            return Result.new_error(
                InvalidArgumentError(message=f"size must be >= 0; got {size}")
            )
        if not isinstance(device_kind, str):
            return Result.new_error(
                InvalidArgumentError(
                    message=f"device_kind must be str; got {type(device_kind)}"
                )
            )
        if device_kind.strip().lower() != "host":
            return Result.new_error(
                InvalidArgumentError(
                    message=(
                        "register_buffer supports host memory only; "
                        "use register_gpu_buffer for a CUDA staging range"
                    )
                )
            )
        if not isinstance(device_id, str):
            return Result.new_error(
                InvalidArgumentError(
                    message=f"device_id must be str; got {type(device_id)}"
                )
            )
        if not isinstance(layout, str):
            return Result.new_error(
                InvalidArgumentError(
                    message=f"layout must be str; got {type(layout)}"
                )
            )
        if metadata is not None and not isinstance(metadata, dict):
            return Result.new_error(
                InvalidArgumentError(
                    message=f"metadata must be dict or None; got {type(metadata)}"
                )
            )
        try:
            inner_res = self._client.register_buffer(ptr, size)
            if not inner_res.is_ok():
                return Result.new_error(inner_res.unwrap_error())
            _ = inner_res.unwrap()
            self._registered_buffer_descriptors.append(
                _RegisteredBufferDescriptor(
                    ptr=int(ptr),
                    size=int(size),
                    device_kind=device_kind,
                    device_id=device_id,
                    layout=layout,
                    metadata=dict(metadata or {}),
                )
            )
            return Result.new_ok(OkNone())
        except ApiError as e:
            return Result.new_error(e)

    def register_gpu_buffer(
        self,
        ptr: int,
        size: int,
        device_id: int,
    ) -> Result[GpuBufferRegistration, ApiError]:
        if self._client is None:
            return Result.new_error(
                GeneralError(
                    message="Store not initialized when register_gpu_buffer(). Call setup() first."
                )
            )
        if isinstance(ptr, bool) or not isinstance(ptr, int) or ptr <= 0:
            return Result.new_error(
                InvalidArgumentError(message=f"GPU ptr must be a positive int; got {ptr!r}")
            )
        if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
            return Result.new_error(
                InvalidArgumentError(message=f"GPU size must be a positive int; got {size!r}")
            )
        if (
            isinstance(device_id, bool)
            or not isinstance(device_id, int)
            or device_id < 0
        ):
            return Result.new_error(
                InvalidArgumentError(
                    message=f"GPU device_id must be a non-negative int; got {device_id!r}"
                )
            )
        if ptr + size > (1 << 64):
            return Result.new_error(
                InvalidArgumentError(
                    message=f"GPU registration range overflows u64: ptr={ptr:#x} size={size}"
                )
            )
        if self._gpu_buffer_registration is not None:
            return Result.new_error(
                InvalidArgumentError(
                    message=(
                        "one GPU buffer is already registered: "
                        f"registration_id={self._gpu_buffer_registration.registration_id}"
                    )
                )
            )
        try:
            inner_res = self._client.register_gpu_buffer(ptr, size, device_id)
            if not inner_res.is_ok():
                return Result.new_error(inner_res.unwrap_error())
            payload = inner_res.unwrap()
            if not isinstance(payload, dict):
                return Result.new_error(
                    GeneralError(
                        message=f"register_gpu_buffer returned non-dict payload: {type(payload)}"
                    )
                )
            registration = GpuBufferRegistration(
                registration_id=int(payload["registration_id"]),
                ptr=int(payload["ptr"]),
                size=int(payload["len"]),
                device_id=int(payload["device_id"]),
            )
            if (
                registration.registration_id <= 0
                or registration.ptr != ptr
                or registration.size != size
                or registration.device_id != device_id
            ):
                return Result.new_error(
                    GeneralError(
                        message=(
                            "register_gpu_buffer returned a mismatched registration: "
                            f"{registration!r}"
                        )
                    )
                )
            self._gpu_buffer_registration = registration
            return Result.new_ok(registration)
        except ApiError as e:
            return Result.new_error(e)

    def validate_gpu_destination(
        self,
        destination: GpuDestination,
    ) -> Result[OkNone, ApiError]:
        if self._client is None:
            return Result.new_error(
                GeneralError(
                    message="Store not initialized when validate_gpu_destination()."
                )
            )
        if not isinstance(destination, GpuDestination):
            return Result.new_error(
                InvalidArgumentError(
                    message=(
                        "validate_gpu_destination requires GpuDestination; "
                        f"got {type(destination)}"
                    )
                )
            )
        if destination.capacity <= 0 or destination.ptr <= 0:
            return Result.new_error(
                InvalidArgumentError(
                    message=f"invalid GPU destination: {destination!r}"
                )
            )
        try:
            inner_res = self._client.validate_gpu_destination(
                destination.registration_id,
                destination.ptr,
                destination.capacity,
            )
            if not inner_res.is_ok():
                return Result.new_error(inner_res.unwrap_error())
            _ = inner_res.unwrap()
            return Result.new_ok(OkNone())
        except ApiError as e:
            return Result.new_error(e)

    def unregister_gpu_buffer(
        self,
        registration: GpuBufferRegistration,
    ) -> Result[OkNone, ApiError]:
        if self._client is None:
            return Result.new_error(
                GeneralError(
                    message="Store not initialized when unregister_gpu_buffer()."
                )
            )
        if not isinstance(registration, GpuBufferRegistration):
            return Result.new_error(
                InvalidArgumentError(
                    message=(
                        "unregister_gpu_buffer requires GpuBufferRegistration; "
                        f"got {type(registration)}"
                    )
                )
            )
        active = self._gpu_buffer_registration
        if active != registration:
            return Result.new_error(
                InvalidArgumentError(
                    message=(
                        "GPU registration is not active on this store: "
                        f"requested={registration!r} active={active!r}"
                    )
                )
            )
        try:
            inner_res = self._client.unregister_gpu_buffer(registration.registration_id)
            if not inner_res.is_ok():
                return Result.new_error(inner_res.unwrap_error())
            _ = inner_res.unwrap()
            self._gpu_buffer_registration = None
            return Result.new_ok(OkNone())
        except ApiError as e:
            return Result.new_error(e)

    def batch_put_from(
        self,
        keys: List[str],
        payload_ptrs: List[int],
        payload_sizes: List[int],
        opts: Optional[PutOptionalArgs] = None,
    ) -> List[int]:
        if len(keys) != len(payload_ptrs) or len(keys) != len(payload_sizes):
            raise ValueError(
                "batch_put_from requires keys, payload_ptrs, and payload_sizes to have the same length"
            )
        if len(keys) == 0:
            return []

        if self._client is None:
            code = _error_to_ret_code(
                GeneralError(
                    message="Store not initialized when batch_put_from(). Call setup() first."
                )
            )
            return [code] * len(keys)

        lease_id: Optional[int] = opts.lease_id if opts is not None else None
        reject_if_inflight_same_key = (
            bool(opts.reject_if_inflight_same_key) if opts is not None else False
        )
        reject_if_exist_same_key = (
            bool(opts.reject_if_exist_same_key) if opts is not None else False
        )
        write_through = bool(opts.write_through) if opts is not None else True
        make_replica_task = bool(opts.make_replica_task) if opts is not None else True

        try:
            inner_res = self._client.batch_put_from(
                keys,
                payload_ptrs,
                payload_sizes,
                lease_id=lease_id,
                reject_if_inflight_same_key=reject_if_inflight_same_key,
                reject_if_exist_same_key=reject_if_exist_same_key,
                write_through=write_through,
                make_replica_task=make_replica_task,
            )
            if inner_res.is_ok():
                submit_results = list(inner_res.unwrap())
                if len(submit_results) != len(keys):
                    raise RuntimeError(
                        "batch_put_from returned unexpected result length: "
                        f"expected={len(keys)} got={len(submit_results)}"
                    )
                return [int(item) for item in submit_results]
            err = inner_res.unwrap_error()
            code = _error_to_ret_code(err)
            return [code] * len(keys)
        except ApiError as e:
            code = _error_to_ret_code(e)
            return [code] * len(keys)
        except Exception as e:
            code = _error_to_ret_code(GeneralError(f"batch_put_from failed: {e}"))
            return [code] * len(keys)

    def get_size(self, key: str) -> Result[int, ApiError]:
        """Get the size of a stored value (non-blocking)."""
        return self._client.get_size(key)

    def is_exist(self, key: str, allow_local_snapshot: bool = False) -> Result[bool, ApiError]:
        """Check if a key exists in the store (non-blocking)."""
        try:
            if self._client is None:
                return Result.new_error(
                    GeneralError(
                        message="Store not initialized when is_exist(). Call setup() first."
                    )
                )
            return self._client.is_exist(key, allow_local_snapshot=allow_local_snapshot)
        except Exception as e:
            return Result.new_error(GeneralError(f"Existence check failed: {str(e)}"))

    def batch_get_into(
        self,
        keys: List[str],
        payload_ptrs: List[int],
        payload_capacities: List[int],
    ) -> List[int]:
        if len(keys) != len(payload_ptrs) or len(keys) != len(payload_capacities):
            raise ValueError(
                "batch_get_into requires keys, payload_ptrs, and payload_capacities to have the same length"
            )
        if len(keys) == 0:
            return []

        if self._client is not None:
            inner_res = self._client.batch_get_into(keys, payload_ptrs, payload_capacities)
            if inner_res.is_ok():
                return list(inner_res.unwrap())
            err = inner_res.unwrap_error()
            return [_error_to_ret_code(err)] * len(keys)

        results: List[int] = []
        for key, ptr, size in zip(keys, payload_ptrs, payload_capacities):
            get_result = self.get_payload_into_ptr_blocking(key, ptr, size)
            if get_result.is_ok():
                results.append(int(get_result.unwrap()))
            else:
                results.append(_error_to_ret_code(get_result.unwrap_error()))
        return results

    def batch_is_exist(
        self,
        keys: List[str],
        allow_local_snapshot: bool = False,
    ) -> List[int]:
        if len(keys) == 0:
            return []

        if self._client is None:
            code = _error_to_ret_code(
                GeneralError(
                    message="Store not initialized when batch_is_exist(). Call setup() first."
                )
            )
            return [code] * len(keys)

        try:
            inner_res = self._client.batch_is_exist(
                keys,
                allow_local_snapshot=allow_local_snapshot,
            )
            if not inner_res.is_ok():
                code = _error_to_ret_code(inner_res.unwrap_error())
                return [code] * len(keys)
            batch_results = self._normalize_batch_result_list(
                inner_res.unwrap(), len(keys), "batch_is_exist"
            )
            out: List[int] = []
            for idx, item in enumerate(batch_results):
                if not isinstance(item, int):
                    raise GeneralError(
                        message=(
                            f"batch_is_exist returned non-int item for key {keys[idx]!r}: "
                            f"{type(item)}"
                        )
                    )
                out.append(int(item))
            return out
        except ApiError as e:
            code = _error_to_ret_code(e)
            return [code] * len(keys)
        except Exception as e:
            code = _error_to_ret_code(GeneralError(f"batch_is_exist failed: {e}"))
            return [code] * len(keys)

    def get_meta(self, key: str) -> Result[Dict[str, Any], ApiError]:
        """Query key metadata and one live placement without fetching payload bytes."""
        try:
            inner = self._client.get_meta(key)
            if not inner.is_ok():
                return Result.new_error(inner.unwrap_error())
            meta = inner.unwrap()
            assert isinstance(meta, dict), f"get_meta returned non-dict: {type(meta)}"
            return Result.new_ok(meta)
        except Exception as e:
            return Result.new_error(GeneralError(f"GetMeta failed for key '{key}': {str(e)}"))

    def batch_get_meta(self, keys: List[str]) -> List[Dict[str, Any]]:
        if len(keys) == 0:
            return []

        if self._client is not None and hasattr(self._client, "batch_get_meta"):
            inner_res = self._client.batch_get_meta(keys)
            if inner_res.is_ok():
                rows = inner_res.unwrap()
                assert isinstance(rows, list), (
                    f"batch_get_meta returned non-list: {type(rows)}"
                )
                return list(rows)
            err = inner_res.unwrap_error()
            code = _error_to_ret_code(err)
            return [
                {
                    "exists": False,
                    "len": 0,
                    "node_id": "",
                    "src_addr": 0,
                    "src_base_addr": 0,
                    "segment_device_id": "",
                    "segment_device_desc": "",
                    "replica_count": 0,
                    "transport_error": True,
                    "error_code": -code,
                    "error_json": str(err),
                }
                for _ in keys
            ]

        rows: List[Dict[str, Any]] = []
        for key in keys:
            meta_res = self.get_meta(key)
            if meta_res.is_ok():
                rows.append(meta_res.unwrap())
            else:
                err = meta_res.unwrap_error()
                rows.append(
                    {
                        "exists": False,
                        "len": 0,
                        "node_id": "",
                        "src_addr": 0,
                        "src_base_addr": 0,
                        "segment_device_id": "",
                        "segment_device_desc": "",
                        "replica_count": 0,
                        "transport_error": True,
                        "error_code": -_error_to_ret_code(err),
                        "error_json": str(err),
                    }
                )
        return rows

    def count_prefix(self, prefix: str) -> Result[int, ApiError]:
        """Count number of keys with the given prefix.

        The PyO3 binding historically returned an object exposing ``error()/success()``.
        Our Python Result now standardizes on ``is_ok()/unwrap()/unwrap_error()``.
        Normalize here to the unified Result semantics without adding fallback behaviour.
        """
        try:
            inner = self._client.count_prefix(prefix)
            if not inner.is_ok():
                return Result.new_error(inner.unwrap_error())
            count = inner.unwrap()
            assert isinstance(count, int), f"count_prefix returned non-int: {type(count)}"
            return Result.new_ok(count)
        except Exception as e:
            return Result.new_error(GeneralError(f"CountPrefix failed for prefix '{prefix}': {str(e)}"))

    def rpc_call(
        self,
        node_id: str,
        path: str,
        payload: FlatDict,
        timeout_ms: int = 10_000,
    ) -> Result[KvFuture, ApiError]:
        try:
            if self._client is None:
                raise GeneralError(message="Store not initialized when rpc_call(). Call setup() first.")
            _validate_explicit_rpc_timeout_ms(timeout_ms)

            encoded = encode_flat_kv_dict(payload)
            if not encoded.is_ok():
                return Result.new_error(encoded.unwrap_error())

            inner = self._client.rpc_call(node_id, path, encoded.unwrap(), timeout_ms)
            if not inner.is_ok():
                return Result.new_error(inner.unwrap_error())
            fut = inner.unwrap()
            assert fut is not None
            return Result.new_ok(_FluxonRpcFuture(fut))
        except ApiError as e:
            return Result.new_error(e)

    def rpc_call_bytes(
        self,
        node_id: str,
        path: str,
        payload: bytes,
        timeout_ms: int = 10_000,
    ) -> Result[KvFuture, ApiError]:
        try:
            if self._client is None:
                raise GeneralError(message="Store not initialized when rpc_call_bytes(). Call setup() first.")
            if not isinstance(payload, (bytes, bytearray)):
                raise InvalidArgumentError(message=f"payload must be bytes; got {type(payload)}")
            _validate_explicit_rpc_timeout_ms(timeout_ms)

            inner = self._client.rpc_call(node_id, path, bytes(payload), timeout_ms)
            if not inner.is_ok():
                return Result.new_error(inner.unwrap_error())
            fut = inner.unwrap()
            assert fut is not None
            return Result.new_ok(_FluxonRpcBytesFuture(fut))
        except ApiError as e:
            return Result.new_error(e)

    def rpc_register(
        self,
        path: str,
        handler: Callable[[str, FlatDict], FlatDict],
    ) -> Result[OkNone, ApiError]:
        try:
            if self._client is None:
                raise GeneralError(message="Store not initialized when rpc_register(). Call setup() first.")
            if not callable(handler):
                raise InvalidArgumentError(message=f"handler must be callable; got {type(handler)}")
            inner = self._client.rpc_register_flat_dict(path, handler)
            if not inner.is_ok():
                return Result.new_error(inner.unwrap_error())
            _ = inner.unwrap()
            return Result.new_ok(OkNone())
        except ApiError as e:
            return Result.new_error(e)

    def rpc_register_bytes(
        self,
        path: str,
        handler: Callable[[str, bytes], bytes],
    ) -> Result[OkNone, ApiError]:
        try:
            if self._client is None:
                raise GeneralError(
                    message="Store not initialized when rpc_register_bytes(). Call setup() first."
                )
            if not callable(handler):
                raise InvalidArgumentError(message=f"handler must be callable; got {type(handler)}")

            def raw_handler(from_node_id: str, payload_bytes: bytes) -> bytes:
                out = handler(from_node_id, bytes(payload_bytes))
                if not isinstance(out, (bytes, bytearray)):
                    raise InvalidArgumentError(
                        message=f"rpc bytes handler must return bytes; got {type(out)}"
                    )
                return bytes(out)

            inner = self._client.rpc_register(path, raw_handler)
            if not inner.is_ok():
                return Result.new_error(inner.unwrap_error())
            _ = inner.unwrap()
            return Result.new_ok(OkNone())
        except ApiError as e:
            return Result.new_error(e)

    def remove(self, key: str) -> Result[OkNone, ApiError]:
        return self._client.delete(key)

    def sync_kv_to_file(
        self,
        key: str,
        target_instance_key: str,
        filepath: str,
        file_offset: int,
        bytes_field_key: str,
        timeout_ms: int = 60_000,
    ) -> Result[KvFuture, ApiError]:
        if self._client is None:
            return Result.new_error(
                GeneralError(message="Store not initialized when sync_kv_to_file(). Call setup() first.")
            )

        try:
            if not isinstance(key, str) or not key:
                raise InvalidArgumentError(message=f"key must be a non-empty str; got {type(key)}")
            if not isinstance(target_instance_key, str) or not target_instance_key:
                raise InvalidArgumentError(
                    message=f"target_instance_key must be a non-empty str; got {type(target_instance_key)}"
                )
            if not isinstance(filepath, str) or not filepath:
                raise InvalidArgumentError(message=f"filepath must be a non-empty str; got {type(filepath)}")
            if not isinstance(bytes_field_key, str) or not bytes_field_key:
                raise InvalidArgumentError(
                    message=f"bytes_field_key must be a non-empty str; got {type(bytes_field_key)}"
                )
            if not isinstance(file_offset, int):
                raise InvalidArgumentError(message=f"file_offset must be int; got {type(file_offset)}")
            if file_offset < 0:
                raise InvalidArgumentError(message=f"file_offset must be >= 0; got {file_offset}")
            _validate_explicit_rpc_timeout_ms(timeout_ms)

            return self._client.sync_kv_to_file(
                target_instance_key,
                key,
                filepath,
                int(file_offset),
                bytes_field_key,
                int(timeout_ms),
            )
        except ApiError as e:
            return Result.new_error(e)
        except Exception as e:  # pragma: no cover - thin wrapper
            return Result.new_error(GeneralError(message=f"sync_kv_to_file failed: {e}"))

    def instance_key(self) -> Result[str, ApiError]:
        """Get the unique instance key for this store instance."""
        try:
            key = self._client.instance_key()

            # Newer PyO3 bindings may return a backend Result wrapper; normalize to a plain string
            # to keep the KvClient interface stable for callers and tests.
            if isinstance(key, Result):
                if not key.is_ok():
                    return Result.new_error(key.unwrap_error())
                key = key.unwrap()
            elif hasattr(key, "is_ok") and hasattr(key, "unwrap") and hasattr(key, "unwrap_error"):
                # Avoid importing fluxon_pyo3 types here; use duck-typing to consume the backend Result.
                ok = key.is_ok()  # type: ignore[call-arg]
                if not ok:
                    return Result.new_error(key.unwrap_error())  # type: ignore[call-arg]
                key = key.unwrap()  # type: ignore[call-arg]

            if not isinstance(key, str):
                return Result.new_error(
                    GeneralError(message=f"instance_key must be str; got {type(key)}")
                )
            return Result.new_ok(key)
        except Exception as e:
            return Result.new_error(GeneralError(f"Failed to get instance key: {str(e)}"))

    def close(self) -> Result[OkNone, ApiError]:
        """Close and tear down the store."""
        try:
            if self._gpu_buffer_registration is not None:
                unregister_result = self.unregister_gpu_buffer(
                    self._gpu_buffer_registration
                )
                if not unregister_result.is_ok():
                    return Result.new_error(unregister_result.unwrap_error())
                _ = unregister_result.unwrap()
            # Backend returns a Result; MUST be explicitly consumed to avoid
            # leaking an unconsumed Result that triggers __del__ assertion.
            res = self._client.close()
            if not res.is_ok():
                # Propagate backend error (already an ApiError)
                return Result.new_error(res.unwrap_error())
            # Consume Ok(None-like) to satisfy strict consumption policy
            _ = res.unwrap()
            unregister_store_from_cleanup(self)
            # English note:
            # After a successful close, clear the backend handle to prevent any further calls and
            # allow deterministic resource release without relying on Python GC timing.
            self._client = None
            return Result.new_ok(OkNone())
        except Exception as e:
            return Result.new_error(GeneralError(f"Failed to close client: {str(e)}"))

    def is_write_once(self) -> bool:
        """Whether the store is write-once (keys cannot be overwritten)."""
        return False

    def config(self) -> FluxonKvClientConfig:
        return self._config


    def get_cluster_name(self) -> str:
        if self._client is None:
            raise RuntimeError("Store not initialized")
        return str(self._client.cluster_name())

    def wait_local_segments_ready(self) -> List[dict[str, Any]]:
        if self._client is None:
            raise RuntimeError(
                "Store not initialized when wait_local_segments_ready(). Call setup() first."
            )
        inner_res = self._client.wait_local_segments_ready()
        if not inner_res.is_ok():
            raise RuntimeError(
                f"wait_local_segments_ready backend error: {inner_res.unwrap_error()}"
            )
        inner_res = inner_res.unwrap()
        if not isinstance(inner_res, list):
            raise RuntimeError(
                "wait_local_segments_ready must return a list of segment mappings"
            )
        out: List[dict[str, Any]] = []
        for item in inner_res:
            if not isinstance(item, dict):
                raise RuntimeError(
                    "wait_local_segments_ready segment item must be a dict"
                )
            out.append(dict(item))
        return out

    def local_fast_put_start(
        self,
        keys: List[str],
        value_len: int,
        opts: Optional[PutOptionalArgs] = None,
    ) -> int:
        if self._client is None:
            raise RuntimeError(
                "Store not initialized when local_fast_put_start(). Call setup() first."
            )
        if len(keys) == 0:
            raise ValueError("local_fast_put_start requires at least one key")
        if not isinstance(value_len, int):
            raise ValueError(f"value_len must be int; got {type(value_len)}")
        if value_len <= 0:
            raise ValueError(f"value_len must be > 0; got {value_len}")
        reject_if_inflight_same_key = (
            bool(opts.reject_if_inflight_same_key) if opts is not None else False
        )
        reject_if_exist_same_key = (
            bool(opts.reject_if_exist_same_key) if opts is not None else False
        )
        write_through = bool(opts.write_through) if opts is not None else True
        make_replica_task = bool(opts.make_replica_task) if opts is not None else True
        make_replica_task_mask: Optional[List[bool]] = None
        if opts is not None and opts.make_replica_task_mask is not None:
            requested_mask = opts.make_replica_task_mask
            if not isinstance(requested_mask, list):
                raise ValueError(
                    "make_replica_task_mask must be list[bool] when provided; "
                    f"got {type(requested_mask)}"
                )
            if len(requested_mask) != len(keys):
                raise ValueError(
                    "make_replica_task_mask length must match keys length; "
                    f"keys={len(keys)} mask={len(requested_mask)}"
                )
            for index, item in enumerate(requested_mask):
                if type(item) is not bool:
                    raise ValueError(
                        "make_replica_task_mask items must be bool; "
                        f"index={index} got={type(item)}"
                    )
            make_replica_task_mask = list(requested_mask)
        atomic_group_lens = _validate_put_atomic_groups(
            keys,
            opts.atomic_group_lens if opts is not None else None,
            make_replica_task_mask,
        )
        inner_res = self._client.local_fast_put_start(
            keys,
            value_len,
            reject_if_inflight_same_key=reject_if_inflight_same_key,
            reject_if_exist_same_key=reject_if_exist_same_key,
            write_through=write_through,
            make_replica_task=make_replica_task,
            make_replica_task_mask=make_replica_task_mask,
            atomic_group_lens=atomic_group_lens,
        )
        if not inner_res.is_ok():
            err = inner_res.unwrap_error()
            if isinstance(err, Exception):
                raise err
            raise RuntimeError(f"local_fast_put_start backend error: {err}")
        plan_ptr = inner_res.unwrap()
        if not isinstance(plan_ptr, int) or plan_ptr <= 0:
            raise RuntimeError(f"local_fast_put_start returned invalid plan_ptr: {plan_ptr!r}")
        return int(plan_ptr)

    def local_fast_put_commit(self, plan_ptr: int) -> KvFuture:
        if self._client is None:
            raise RuntimeError(
                "Store not initialized when local_fast_put_commit(). Call setup() first."
            )
        inner_res = self._client.local_fast_put_commit(plan_ptr)
        if not inner_res.is_ok():
            raise RuntimeError(
                f"local_fast_put_commit backend error: {inner_res.unwrap_error()}"
            )
        inner_future = inner_res.unwrap()
        if inner_future is None:
            raise RuntimeError("local_fast_put_commit returned empty future")
        return _FluxonBatchRetCodeFuture(inner_future, [plan_ptr])

    def put_abort(self, plan_ptr: int) -> None:
        if self._client is None:
            raise RuntimeError(
                "Store not initialized when put_abort(). Call setup() first."
            )
        inner_res = self._client.put_abort(plan_ptr)
        if not inner_res.is_ok():
            raise RuntimeError(f"put_abort backend error: {inner_res.unwrap_error()}")
        _ = inner_res.unwrap()

    def get_views(
        self,
        keys: List[str],
        concurrency: Optional[int] = None,
    ) -> int:
        if self._client is None:
            raise RuntimeError(
                "Store not initialized when get_views(). Call setup() first."
            )
        if len(keys) == 0:
            raise ValueError("get_views requires at least one key")
        inner_res = self._client.get_views(
            keys,
            concurrency=concurrency if concurrency is not None else self._batch_concurrency,
        )
        if not inner_res.is_ok():
            raise RuntimeError(f"get_views backend error: {inner_res.unwrap_error()}")
        plan_ptr = inner_res.unwrap()
        if not isinstance(plan_ptr, int) or plan_ptr <= 0:
            raise RuntimeError(f"get_views returned invalid plan_ptr: {plan_ptr!r}")
        return int(plan_ptr)

    def release_views(self, plan_ptr: int) -> None:
        if self._client is None:
            raise RuntimeError(
                "Store not initialized when release_views(). Call setup() first."
            )
        inner_res = self._client.release_views(plan_ptr)
        if not inner_res.is_ok():
            raise RuntimeError(f"release_views backend error: {inner_res.unwrap_error()}")
        _ = inner_res.unwrap()

    def get_start(
        self,
        keys: List[str],
        prefix_best_effort: bool = True,
        atomic_group_lens: Optional[List[int]] = None,
    ) -> GetStartHandle:
        if self._client is None:
            raise RuntimeError(
                "Store not initialized when get_start(). Call setup() first."
            )
        if len(keys) == 0:
            raise ValueError("get_start requires at least one key")
        normalized_group_lens: Optional[List[int]] = None
        if atomic_group_lens is not None:
            normalized_group_lens = [int(length) for length in atomic_group_lens]
            if any(length <= 0 for length in normalized_group_lens):
                raise ValueError("get_start atomic_group_lens entries must be > 0")
            if sum(normalized_group_lens) != len(keys):
                raise ValueError(
                    "get_start atomic_group_lens must sum to keys length: "
                    f"sum={sum(normalized_group_lens)} keys={len(keys)}"
                )

        started_at_ns = time.monotonic_ns()
        inner_res = self._client.get_start(
            list(keys),
            bool(prefix_best_effort),
            normalized_group_lens,
            self._batch_concurrency,
        )
        if not inner_res.is_ok():
            raise RuntimeError(f"get_start backend error: {inner_res.unwrap_error()}")
        payload = inner_res.unwrap()
        if not isinstance(payload, dict):
            raise RuntimeError(f"get_start returned non-dict payload: {type(payload)}")
        backend_handle = int(payload["handle"])
        result = _build_get_start_result_from_backend_payload(
            payload,
            list(keys),
            bool(prefix_best_effort),
            normalized_group_lens,
        )
        logging.info(
            "FluxonKVCacheStore get_start result: keys=%d raw_prefix_hit_len=%d "
            "transferable_len=%d prefix_hit_groups=%d all_hit=%s "
            "first_miss_index=%s first_miss_group_index=%s "
            "prefix_best_effort=%s duration_ms=%.3f",
            len(keys),
            result.raw_prefix_hit_len,
            result.transferable_len,
            result.prefix_hit_groups,
            result.all_hit,
            result.first_miss_index,
            result.first_miss_group_index,
            result.prefix_best_effort,
            (time.monotonic_ns() - started_at_ns) / 1_000_000.0,
        )
        return GetStartHandle(
            keys=tuple(keys),
            result=result,
            created_at_ns=started_at_ns,
            backend_token=id(self),
            backend_handle=backend_handle,
        )

    def cancel_get_transfer(self, handle: GetStartHandle) -> None:
        if not isinstance(handle, GetStartHandle):
            raise TypeError(f"cancel_get_transfer requires GetStartHandle, got {type(handle)}")
        if handle.backend_token is not None and handle.backend_token != id(self):
            raise RuntimeError(
                "cancel_get_transfer handle belongs to a different FluxonKVCacheStore"
            )
        if handle.closed:
            return
        if self._client is None:
            raise RuntimeError(
                "Store not initialized when cancel_get_transfer(). Call setup() first."
            )
        inner_res = self._client.cancel_get_transfer(int(handle.backend_handle))
        if not inner_res.is_ok():
            raise RuntimeError(
                f"cancel_get_transfer backend error: {inner_res.unwrap_error()}"
            )
        _ = inner_res.unwrap()
        handle.closed = True

    def get_transfer(
        self,
        handle: GetStartHandle,
        concurrency: Optional[int] = None,
        *,
        consume_prefix_len: Optional[int] = None,
    ) -> int:
        if not isinstance(handle, GetStartHandle):
            raise TypeError(f"get_transfer requires GetStartHandle, got {type(handle)}")
        if handle.backend_token is not None and handle.backend_token != id(self):
            raise RuntimeError("get_transfer handle belongs to a different FluxonKVCacheStore")
        if handle.closed:
            raise RuntimeError("get_transfer handle has been closed")
        result = handle.result
        if result.transferable_len == 0:
            raise RuntimeError(
                "get_transfer requires a non-empty transferable prefix: "
                f"transferable_len={result.transferable_len} total={len(result.keys)} "
                f"raw_prefix_hit_len={result.raw_prefix_hit_len} "
                f"first_miss_index={result.first_miss_index} "
                f"first_miss_group_index={result.first_miss_group_index}"
            )
        if consume_prefix_len is None:
            normalized_consume_prefix_len = result.transferable_len
        else:
            if isinstance(consume_prefix_len, bool) or not isinstance(
                consume_prefix_len, int
            ):
                raise TypeError(
                    "get_transfer consume_prefix_len must be an int or None, got "
                    f"{type(consume_prefix_len)}"
                )
            normalized_consume_prefix_len = int(consume_prefix_len)
        if (
            normalized_consume_prefix_len <= 0
            or normalized_consume_prefix_len > result.transferable_len
        ):
            raise ValueError(
                "get_transfer consume_prefix_len must be within the live "
                "transferable prefix: "
                f"consume={normalized_consume_prefix_len} "
                f"transferable={result.transferable_len}"
            )
        group_lens = result.atomic_group_lens or (len(result.keys),)
        group_end = 0
        for group_len in group_lens:
            group_end += int(group_len)
            if group_end >= normalized_consume_prefix_len:
                break
        if group_end != normalized_consume_prefix_len:
            raise ValueError(
                "get_transfer consume_prefix_len must end at an atomic-group "
                "boundary: "
                f"consume={normalized_consume_prefix_len} "
                f"atomic_group_lens={group_lens}"
            )
        if self._client is None:
            raise RuntimeError(
                "Store not initialized when get_transfer(). Call setup() first."
            )
        _ = concurrency
        inner_res = self._client.get_transfer(
            handle.backend_handle, normalized_consume_prefix_len
        )
        if not inner_res.is_ok():
            handle.closed = True
            raise RuntimeError(f"get_transfer backend error: {inner_res.unwrap_error()}")
        plan_ptr = inner_res.unwrap()
        handle.closed = True
        if not isinstance(plan_ptr, int) or plan_ptr <= 0:
            raise RuntimeError(f"get_transfer returned invalid plan_ptr: {plan_ptr!r}")
        return int(plan_ptr)

    def get_start_gpu(
        self,
        keys: List[str],
        destinations: List[GpuDestination],
        prefix_best_effort: bool = True,
        atomic_group_lens: Optional[List[int]] = None,
    ) -> GpuGetStartHandle:
        """Start background remote reads directly into caller-owned GPU staging."""
        if self._client is None:
            raise RuntimeError(
                "Store not initialized when get_start_gpu(). Call setup() first."
            )
        if len(keys) == 0:
            raise ValueError("get_start_gpu requires at least one key")
        if len(keys) != len(destinations):
            raise ValueError(
                "get_start_gpu requires one destination per key: "
                f"keys={len(keys)} destinations={len(destinations)}"
            )
        active_registration = self._gpu_buffer_registration
        if active_registration is None:
            raise RuntimeError("get_start_gpu requires an active GPU registration")
        for index, destination in enumerate(destinations):
            if not isinstance(destination, GpuDestination):
                raise TypeError(
                    "get_start_gpu destinations must be GpuDestination: "
                    f"index={index} got={type(destination)}"
                )
            if destination.registration_id != active_registration.registration_id:
                raise ValueError(
                    "get_start_gpu destination uses a stale registration: "
                    f"index={index} destination_id={destination.registration_id} "
                    f"active_id={active_registration.registration_id}"
                )
            if (
                destination.ptr < active_registration.ptr
                or destination.end > active_registration.end
            ):
                raise ValueError(
                    "get_start_gpu destination is outside the active registration: "
                    f"index={index} destination={destination!r} "
                    f"registration={active_registration!r}"
                )

        normalized_group_lens: Optional[List[int]] = None
        if atomic_group_lens is not None:
            normalized_group_lens = [int(length) for length in atomic_group_lens]
            if any(length <= 0 for length in normalized_group_lens):
                raise ValueError("get_start_gpu atomic_group_lens entries must be > 0")
            if sum(normalized_group_lens) != len(keys):
                raise ValueError(
                    "get_start_gpu atomic_group_lens must sum to keys length: "
                    f"sum={sum(normalized_group_lens)} keys={len(keys)}"
                )

        started_at_ns = time.monotonic_ns()
        inner_res = self._client.get_start_gpu(
            list(keys),
            [
                (
                    destination.registration_id,
                    destination.ptr,
                    destination.capacity,
                )
                for destination in destinations
            ],
            bool(prefix_best_effort),
            normalized_group_lens,
            self._batch_concurrency,
        )
        if not inner_res.is_ok():
            raise RuntimeError(f"get_start_gpu backend error: {inner_res.unwrap_error()}")
        payload = inner_res.unwrap()
        if not isinstance(payload, dict):
            raise RuntimeError(
                f"get_start_gpu returned non-dict payload: {type(payload)}"
            )
        backend_handle = int(payload["handle"])
        try:
            result = _build_get_start_result_from_backend_payload(
                payload,
                list(keys),
                bool(prefix_best_effort),
                normalized_group_lens,
            )
        except Exception:
            cancel_res = self._client.cancel_get_transfer_gpu(backend_handle)
            if not cancel_res.is_ok():
                logging.exception(
                    "get_start_gpu payload validation failed and cleanup also failed: %s",
                    cancel_res.unwrap_error(),
                )
            raise
        return GpuGetStartHandle(
            keys=tuple(keys),
            destinations=tuple(destinations),
            result=result,
            created_at_ns=started_at_ns,
            backend_token=id(self),
            backend_handle=backend_handle,
        )

    def cancel_get_transfer_gpu(self, handle: GpuGetStartHandle) -> None:
        if not isinstance(handle, GpuGetStartHandle):
            raise TypeError(
                f"cancel_get_transfer_gpu requires GpuGetStartHandle, got {type(handle)}"
            )
        if handle.backend_token != id(self):
            raise RuntimeError(
                "cancel_get_transfer_gpu handle belongs to a different FluxonKVCacheStore"
            )
        if handle.closed:
            return
        if self._client is None:
            raise RuntimeError(
                "Store not initialized when cancel_get_transfer_gpu(). Call setup() first."
            )
        inner_res = self._client.cancel_get_transfer_gpu(handle.backend_handle)
        handle.closed = True
        if not inner_res.is_ok():
            raise RuntimeError(
                f"cancel_get_transfer_gpu backend error: {inner_res.unwrap_error()}"
            )

    def get_transfer_gpu(
        self,
        handle: GpuGetStartHandle,
        *,
        consume_prefix_len: Optional[int] = None,
    ) -> None:
        """Wait for the GPU transfer terminal; destinations already contain the bytes."""
        if not isinstance(handle, GpuGetStartHandle):
            raise TypeError(
                f"get_transfer_gpu requires GpuGetStartHandle, got {type(handle)}"
            )
        if handle.backend_token != id(self):
            raise RuntimeError(
                "get_transfer_gpu handle belongs to a different FluxonKVCacheStore"
            )
        if handle.closed:
            raise RuntimeError("get_transfer_gpu handle has been closed")
        result = handle.result
        normalized_consume_prefix_len = (
            result.transferable_len
            if consume_prefix_len is None
            else consume_prefix_len
        )
        if (
            isinstance(normalized_consume_prefix_len, bool)
            or not isinstance(normalized_consume_prefix_len, int)
            or normalized_consume_prefix_len <= 0
            or normalized_consume_prefix_len > result.transferable_len
        ):
            raise ValueError(
                "get_transfer_gpu consume_prefix_len must be within the live prefix: "
                f"consume={normalized_consume_prefix_len!r} "
                f"transferable={result.transferable_len}"
            )
        group_lens = result.atomic_group_lens or (len(result.keys),)
        group_end = 0
        for group_len in group_lens:
            group_end += int(group_len)
            if group_end >= normalized_consume_prefix_len:
                break
        if group_end != normalized_consume_prefix_len:
            raise ValueError(
                "get_transfer_gpu consume_prefix_len must end at an atomic-group boundary: "
                f"consume={normalized_consume_prefix_len} groups={group_lens}"
            )
        if self._client is None:
            raise RuntimeError(
                "Store not initialized when get_transfer_gpu(). Call setup() first."
            )
        inner_res = self._client.get_transfer_gpu(
            handle.backend_handle,
            normalized_consume_prefix_len,
        )
        handle.closed = True
        if not inner_res.is_ok():
            raise RuntimeError(
                f"get_transfer_gpu backend error: {inner_res.unwrap_error()}"
            )
        payload = inner_res.unwrap()
        if not isinstance(payload, dict):
            raise RuntimeError(
                f"get_transfer_gpu returned non-dict payload: {type(payload)}"
            )
        transferred_prefix_len = int(payload["transferred_prefix_len"])
        consumed_prefix_len = int(payload["consumed_prefix_len"])
        if (
            transferred_prefix_len != result.transferable_len
            or consumed_prefix_len != normalized_consume_prefix_len
        ):
            raise RuntimeError(
                "get_transfer_gpu returned inconsistent terminal lengths: "
                f"transferred={transferred_prefix_len}/{result.transferable_len} "
                f"consumed={consumed_prefix_len}/{normalized_consume_prefix_len}"
            )

    def get_etcd_config(self) -> List[str]:
        if self._client is None:
            raise RuntimeError("Store not initialized")
        endpoints = self._client.etcd_addresses_raw()
        if not isinstance(endpoints, list) or not endpoints:
            raise RuntimeError(f"Invalid etcd_addresses_raw from backend: {endpoints!r}")
        out: List[str] = []
        for addr in endpoints:
            if not isinstance(addr, str) or not addr.strip():
                raise RuntimeError(f"Invalid etcd endpoint from backend: {addr!r}")
            if "://" in addr:
                raise RuntimeError(f"etcd endpoint must be raw host:port (no scheme), got: {addr!r}")
            out.append(addr)
        return out

    def third_party_logs_dir(self) -> Result[str, ApiError]:
        if self._client is None:
            return Result.new_error(GeneralError(message="Store not initialized"))
        try:
            res = self._client.third_party_logs_dir()
            if not res.is_ok():
                return Result.new_error(res.unwrap_error())
            logs_dir = res.unwrap()
            if not isinstance(logs_dir, str) or not logs_dir:
                return Result.new_error(
                    GeneralError(message=f"third_party_logs_dir must be non-empty str; got {logs_dir!r}")
                )
            return Result.new_ok(logs_dir)
        except ApiError as e:
            return Result.new_error(e)
        except Exception as e:
            return Result.new_error(GeneralError(message=f"third_party_logs_dir failed: {e}"))

    def ensure_zero_contribution_for_channel(self) -> None:
        self._config.ensure_zero_contribution_for_channel()

    # ---- Cluster metrics snapshot ----
    def metrics_snapshot(self) -> MetricSnapshot:
        """Build a MetricSnapshot from the Rust client's metrics snapshot."""
        raw = getattr(self._client, "metrics_snapshot", None)
        if raw is None:
            raise RuntimeError("fluxon_pyo3.KvClient.metrics_snapshot is not available")

        data = self._client.metrics_snapshot()
        # metrics_snapshot may return unified Result, unwrap explicitly
        if isinstance(data, Result):  # type: ignore
            if not data.is_ok():
                raise RuntimeError(f"metrics_snapshot backend error: {data.unwrap_error()}")
            data = data.unwrap()

        if not isinstance(data, dict):
            raise RuntimeError(
                "metrics_snapshot must return dict: {segment: (available,total)} or {segment: {segment_available_bytes, segment_total_bytes}} or aggregated {segment_available_bytes, segment_total_bytes}"
            )

        # Aggregated {segment_*} → single logical segment
        if "segment_available_bytes" in data and "segment_total_bytes" in data:
            avail = data["segment_available_bytes"]
            total = data["segment_total_bytes"]
            if not isinstance(avail, (int, float)) or not isinstance(total, (int, float)):
                raise RuntimeError("segment_available_bytes/segment_total_bytes must be numeric")
            return MetricSnapshot(per_segment={"cluster": (int(avail), int(total))})

        normalized: dict[str, tuple[int, int]] = {}
        for seg, v in data.items():
            if isinstance(v, (tuple, list)) and len(v) == 2:
                a, t = v[0], v[1]
                if not isinstance(a, (int, float)) or not isinstance(t, (int, float)):
                    raise RuntimeError("available/total must be numeric in per-segment pair")
                normalized[str(seg)] = (int(a), int(t))
            elif isinstance(v, dict) and "segment_available_bytes" in v and "segment_total_bytes" in v:
                a = v["segment_available_bytes"]
                t = v["segment_total_bytes"]
                if not isinstance(a, (int, float)) or not isinstance(t, (int, float)):
                    raise RuntimeError("segment_*_bytes must be numeric in per-segment dict")
                normalized[str(seg)] = (int(a), int(t))
            else:
                raise RuntimeError(
                    "Unsupported per-segment value; expected (available,total) or {segment_available_bytes, segment_total_bytes}"
                )

        return MetricSnapshot(per_segment=normalized)

    def observability_snapshot_async(self) -> KvFuture:
        """Return a future for Fluxon locality and IO counters."""
        if self._client is None:
            raise RuntimeError(
                "Store not initialized when observability_snapshot_async(). Call setup() first."
            )
        res = self._client.observability_snapshot_async()
        if not res.is_ok():
            raise RuntimeError(
                f"observability_snapshot_async backend error: {res.unwrap_error()}"
            )
        return res.unwrap()

    # --- Fluxon-kv lease helpers (synchronous) ---
    def allocate_lease(self, ttl_seconds: int) -> Result[int, ApiError]:
        try:
            inner = self._client.allocate_lease(ttl_seconds)
            if not inner.is_ok():
                return Result.new_error(inner.unwrap_error())
            lease_id = inner.unwrap()
            assert isinstance(lease_id, int) and lease_id > 0
            return Result.new_ok(lease_id)
        except Exception as e:  # pragma: no cover - thin wrapper
            return Result.new_error(GeneralError(f"allocate_lease failed: {e}"))

    def keepalive_lease(self, lease_id: int) -> Result[OkNone, ApiError]:
        try:
            inner = self._client.keepalive_lease(lease_id, "kvclient")
            if not inner.is_ok():
                return Result.new_error(inner.unwrap_error())
            # Success returns a None-like sentinel from PyO3; normalize to OkNone
            _ = inner.unwrap()
            return Result.new_ok(OkNone())
        except Exception as e:  # pragma: no cover - thin wrapper
            return Result.new_error(GeneralError(f"keepalive_lease failed: {e}"))


def _decode_rpc_wait_payload_and_observe(
    raw: Any,
) -> Result[tuple[bytes, Dict[str, Any]], ApiError]:
    observe_us: Dict[str, Any] = {}
    payload = raw
    if isinstance(raw, dict):
        payload = raw.get("payload")
        raw_observe = raw.get("observe_us")
        raw_observe_ts = raw.get("observe_ts_us")
        if raw_observe is not None and not isinstance(raw_observe, dict):
            return Result.new_error(
                GeneralError(message=f"rpc_call returned invalid observe_us type: {type(raw_observe)}")
            )
        if raw_observe_ts is not None and not isinstance(raw_observe_ts, dict):
            return Result.new_error(
                GeneralError(
                    message=f"rpc_call returned invalid observe_ts_us type: {type(raw_observe_ts)}"
                )
            )
        if isinstance(raw_observe, dict):
            observe_us = dict(raw_observe)
        if isinstance(raw_observe_ts, dict):
            observe_us["observe_ts_us"] = dict(raw_observe_ts)
    if not isinstance(payload, (bytes, bytearray)):
        return Result.new_error(
            GeneralError(message=f"rpc_call returned non-bytes payload: {type(payload)}")
        )
    return Result.new_ok((bytes(payload), observe_us))


class _FluxonRpcFuture(KvFuture):
    def __init__(self, inner_future: Any) -> None:
        self._inner = inner_future

    def is_waiting(self) -> bool:
        return bool(self._inner.is_waiting())

    def _decode_wait_success(
        self,
        raw: Any,
    ) -> Result[tuple[Union[Any, MemHolder], Dict[str, Any]], ApiError]:
        unpacked = _decode_rpc_wait_payload_and_observe(raw)
        if not unpacked.is_ok():
            return Result.new_error(unpacked.unwrap_error())
        payload_bytes, observe_us = unpacked.unwrap()
        decoded = fluxon_pyo3.decode_flat_dict_payload(payload_bytes)
        if not decoded.is_ok():
            return Result.new_error(decoded.unwrap_error())
        return Result.new_ok((decoded.unwrap(), observe_us))

    def wait(self) -> Result[Union[Any, MemHolder], ApiError]:
        res = self._inner.wait()
        if not res.is_ok():
            return Result.new_error(res.unwrap_error())
        decoded = self._decode_wait_success(res.unwrap())
        if not decoded.is_ok():
            return Result.new_error(decoded.unwrap_error())
        value, _observe_us = decoded.unwrap()
        return Result.new_ok(value)

    def wait_with_observe(self) -> Result[tuple[Union[Any, MemHolder], Dict[str, Any]], ApiError]:
        res = self._inner.wait()
        if not res.is_ok():
            return Result.new_error(res.unwrap_error())
        return self._decode_wait_success(res.unwrap())


class _FluxonRpcBytesFuture(KvFuture):
    def __init__(self, inner_future: Any) -> None:
        self._inner = inner_future

    def is_waiting(self) -> bool:
        return bool(self._inner.is_waiting())

    def wait(self) -> Result[bytes, ApiError]:
        res = self._inner.wait()
        if not res.is_ok():
            return Result.new_error(res.unwrap_error())
        unpacked = _decode_rpc_wait_payload_and_observe(res.unwrap())
        if not unpacked.is_ok():
            return Result.new_error(unpacked.unwrap_error())
        payload_bytes, _observe_us = unpacked.unwrap()
        return Result.new_ok(payload_bytes)

    def wait_with_observe(self) -> Result[tuple[bytes, Dict[str, Any]], ApiError]:
        res = self._inner.wait()
        if not res.is_ok():
            return Result.new_error(res.unwrap_error())
        return _decode_rpc_wait_payload_and_observe(res.unwrap())


class _FluxonPutFuture(KvFuture):
    """Thin wrapper that holds keepalive references until the put future resolves.

    Error typing (e.g. NoSpace → StorageFullError) is handled by the Rust PyO3
    layer via ``py_error_from_kv_error``; this wrapper simply forwards the
    already-typed error.
    """

    def __init__(self, inner_future: Any, keepalive: List[bytes], dlpack_capsules: List[object]) -> None:
        self._inner = inner_future
        self._keepalive = keepalive
        self._dlpack_capsules = dlpack_capsules

    def __del__(self) -> None:
        self._keepalive = []
        self._dlpack_capsules = []

    def is_waiting(self) -> bool:
        return bool(self._inner.is_waiting())

    def wait(self) -> Result[Union[Any, MemHolder], ApiError]:
        from ..api_error import OkNone, Result as PyResult  # type: ignore

        res = self._inner.wait()
        self._keepalive = []
        self._dlpack_capsules = []
        if not res.is_ok():
            return PyResult.new_error(res.unwrap_error())  # type: ignore

        _ = res.unwrap()
        return PyResult.new_ok(OkNone())  # type: ignore


class _FluxonBatchRetCodeFuture(KvFuture):
    """Future wrapper for batch APIs that resolve to one integer ret-code per key."""

    def __init__(self, inner_future: Any, keepalive: List[object]) -> None:
        self._inner = inner_future
        self._keepalive = keepalive

    def __del__(self) -> None:
        self._keepalive = []

    def is_waiting(self) -> bool:
        return bool(self._inner.is_waiting())

    def wait(self) -> Result[List[int], ApiError]:
        res = self._inner.wait()
        self._keepalive = []
        if not res.is_ok():
            return Result.new_error(res.unwrap_error())
        raw = res.unwrap()
        if not isinstance(raw, list):
            return Result.new_error(
                GeneralError(message=f"batch future returned non-list payload: {type(raw)}")
            )
        out: List[int] = []
        for item in raw:
            if not isinstance(item, int):
                return Result.new_error(
                    GeneralError(message=f"batch future returned non-int item: {type(item)}")
                )
            out.append(int(item))
        return Result.new_ok(out)
