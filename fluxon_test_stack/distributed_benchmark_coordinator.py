#!/usr/bin/env python3
"""
Distributed benchmark coordinator.

Coordinates benchmark nodes and aggregates performance metrics.
"""
from os import error
import socket
import copy
import threading
import json
from pathlib import Path
from tracemalloc import start
import uuid
import importlib.util
import logging
import statistics
import struct
import time
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

import os
import sys

PACKAGE_ROOT = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(PACKAGE_ROOT)
if PACKAGE_ROOT not in sys.path:
    sys.path.insert(0, PACKAGE_ROOT)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

try:
    from .benchmark_role_names import (
        KV_NODE_ROLE_SEED,
        KV_NODE_ROLE_WORKER,
        canonicalize_kv_node_role,
    )
    from .benchmark_node_kv import (
        merge_kv_benchmark_extras,
    )
    from .benchmark_node_rpc import (
        FLUXON_PHASE_PATH_BUCKET_FAST,
        FLUXON_PHASE_PATH_BUCKET_IPC,
        FLUXON_PHASE_PATH_BUCKET_SLOW,
        FLUXON_PHASE_PATH_METRIC_OWNER1_ROUNDTRIP_US,
        FLUXON_PHASE_PATH_METRIC_RPC_EXT_TOTAL_US,
        merge_rpc_benchmark_extras,
    )
    from .benchmark_node_fs import merge_fs_benchmark_extras
except ImportError:
    from benchmark_role_names import (
        KV_NODE_ROLE_SEED,
        KV_NODE_ROLE_WORKER,
        canonicalize_kv_node_role,
    )
    from benchmark_node_kv import (
        merge_kv_benchmark_extras,
    )
    from benchmark_node_rpc import (
        FLUXON_PHASE_PATH_BUCKET_FAST,
        FLUXON_PHASE_PATH_BUCKET_IPC,
        FLUXON_PHASE_PATH_BUCKET_SLOW,
        FLUXON_PHASE_PATH_METRIC_OWNER1_ROUNDTRIP_US,
        FLUXON_PHASE_PATH_METRIC_RPC_EXT_TOTAL_US,
        merge_rpc_benchmark_extras,
    )
    from benchmark_node_fs import merge_fs_benchmark_extras


class TestMode(Enum):
    """Test mode enum."""

    MPMC = "MPMC"
    KVSTORE = "KVSTORE"
    KVSTORE_WITH_LOCAL_CACHE = "KVSTORE_WITH_LOCAL_CACHE"  # Read the same key repeatedly
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

MIN_EFFECTIVE_BENCHMARK_SECONDS = 30.0
# Keep a generous post-benchmark result window globally for KV benchmarks.
# Large-value puts can legitimately leave one node draining its last in-flight op
# well after the 60s metrics window ends, and we prefer waiting longer over
# turning the sample into a deterministic RESULT_TIMEOUT.
RESULT_REPORT_TIMEOUT_EXTRA_SECONDS = 600.0
COMPLETION_STATUS_SUCCESS = "SUCCESS"
COMPLETION_STATUS_RESULT_TIMEOUT = "RESULT_TIMEOUT"
ROUND_GATE_STATUS_WAITING = "waiting"
ROUND_GATE_STATUS_COMPLETED = "completed"
ROUND_GATE_STATUS_FAILED = "failed"
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
P2P_RPC_COMPLETION_SUMMARY_SCOPE_SINGLE_SIDE_ROUNDTRIP = "single_side_roundtrip"
P2P_RPC_COMPLETION_SUMMARY_SCOPE_OWNER_OWNER = "owner_owner"
P2P_RPC_COMPLETION_SUMMARY_DEBUG_ROLE = "raw_transport_counters"
FLUXON_PHASE_PATH_BUCKET_NAMES = (
    FLUXON_PHASE_PATH_BUCKET_FAST,
    FLUXON_PHASE_PATH_BUCKET_SLOW,
    FLUXON_PHASE_PATH_BUCKET_IPC,
)


# Global variables: parameters for this test


def _load_full_config(config_path: str) -> Dict[str, Any]:
    """加载 benchmark_config.py 顶层 CONFIG，供后续解析使用。"""
    spec = importlib.util.spec_from_file_location(
        "benchmark_config_module", config_path
    )
    if spec is None or spec.loader is None:
        print(f"❌ 配置模块不可用: {config_path}")
        exit(1)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    cfg = getattr(module, "CONFIG", None)
    if not isinstance(cfg, dict):
        print("❌ CONFIG 配置格式错误（非字典）")
        exit(1)
    return cfg


def _load_benchmark_section(config_path: str) -> Dict[str, Any]:
    """从 benchmark_config.py 中加载 CONFIG['benchmark']。"""
    cfg = _load_full_config(config_path)
    benchmark_cfg = cfg.get("benchmark")
    if not isinstance(benchmark_cfg, dict):
        print("❌ 缺少 benchmark 字段或类型错误")
        exit(1)
    return benchmark_cfg


def get_benchmark_params(config_path: str) -> Tuple[int, int, float, float, int]:
    """Read per-process benchmark thread count and runtime baseline from config."""
    benchmark_cfg = _load_benchmark_section(config_path)
    if "threads_per_process" not in benchmark_cfg:
        print("❌ benchmark.threads_per_process 未配置")
        exit(1)
    threads_per_process = int(benchmark_cfg["threads_per_process"])
    if threads_per_process <= 0:
        raise ValueError(
            "benchmark.threads_per_process must be > 0, "
            f"got: {threads_per_process}"
        )

    if "max_benchmark_seconds" not in benchmark_cfg:
        print("❌ benchmark.max_benchmark_seconds 未配置")
        exit(1)
    max_secs = int(benchmark_cfg["max_benchmark_seconds"])

    if "cluster_ready_timeout_seconds" not in benchmark_cfg:
        print("❌ benchmark.cluster_ready_timeout_seconds 未配置")
        exit(1)
    cluster_ready_timeout_secs = int(benchmark_cfg["cluster_ready_timeout_seconds"])
    if cluster_ready_timeout_secs <= 0:
        raise ValueError(
            "benchmark.cluster_ready_timeout_seconds must be > 0, "
            f"got: {cluster_ready_timeout_secs}"
        )

    if "metric_warmup_seconds" not in benchmark_cfg:
        print("❌ benchmark.metric_warmup_seconds 未配置")
        exit(1)
    warmup_secs = float(benchmark_cfg["metric_warmup_seconds"])
    if warmup_secs < 0:
        raise ValueError(
            f"benchmark.metric_warmup_seconds must be >= 0, got: {warmup_secs}"
        )
    if float(max_secs) - warmup_secs < MIN_EFFECTIVE_BENCHMARK_SECONDS:
        raise ValueError(
            "Invalid benchmark durations: "
            f"max_benchmark_seconds({max_secs}) - metric_warmup_seconds({warmup_secs}) "
            f"< {int(MIN_EFFECTIVE_BENCHMARK_SECONDS)}"
        )

    start_idle_secs = float(benchmark_cfg.get("start_idle_seconds", 10.0))
    if start_idle_secs < 0:
        raise ValueError(
            f"benchmark.start_idle_seconds must be >= 0, got: {start_idle_secs}"
        )

    return threads_per_process, max_secs, warmup_secs, start_idle_secs, cluster_ready_timeout_secs


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


def get_benchmark_value_size_sweep_list(config_path: str) -> List[int]:
    """Read optional value_size sweep list from benchmark_config."""
    benchmark_cfg = _load_benchmark_section(config_path)

    def _parse_int_list(raw_val: Any, field_name: str) -> List[int]:
        if raw_val is None:
            return []
        if not isinstance(raw_val, (list, tuple)):
            print(
                f"⚠️ benchmark.{field_name} 配置应为列表，实际类型为: {type(raw_val)} "
                f"（忽略该字段，仅使用 baseline 配置）"
            )
            return []
        result: List[int] = []
        for idx, item in enumerate(raw_val):
            try:
                v = int(item)
            except Exception as exc:  # noqa: BLE001
                print(
                    f"⚠️ benchmark.{field_name}[{idx}] 无法解析为整数，原始值={item!r}，"
                    f"异常={exc}；该元素将被忽略，其余元素继续生效"
                )
                continue
            if v <= 0:
                print(
                    f"⚠️ benchmark.{field_name}[{idx}] 非正整数（{v}），该元素将被忽略，其余元素继续生效"
                )
                continue
            result.append(v)
        return result

    value_size_list = _parse_int_list(
        benchmark_cfg.get("value_size_list"), "value_size_list"
    )
    return value_size_list


def get_op_timeout_seconds(config_path: str) -> float:
    """Read benchmark.op_timeout_seconds from benchmark_config."""
    benchmark_cfg = _load_benchmark_section(config_path)
    if "op_timeout_seconds" not in benchmark_cfg:
        raise ValueError("benchmark.op_timeout_seconds is required")
    op_timeout_seconds = float(benchmark_cfg["op_timeout_seconds"])
    if op_timeout_seconds <= 0:
        raise ValueError(
            f"benchmark.op_timeout_seconds must be > 0, got: {op_timeout_seconds}"
        )
    return op_timeout_seconds


def _parse_value_size_weighted_set(
    raw_val: Any,
    *,
    ctx: str,
) -> List[Dict[str, Any]]:
    """Parse benchmark.value_size_weighted_set."""
    if not isinstance(raw_val, list) or not raw_val:
        raise ValueError(f"{ctx} must be a non-empty list")
    parsed: List[Dict[str, Any]] = []
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
        parsed.append({"size_bytes": size_bytes, "weight": weight})
    return parsed


def get_value_size_strategy(
    config_path: str,
) -> Tuple[str, Optional[int], List[Dict[str, Any]]]:
    """Read value size strategy from benchmark_config."""
    benchmark_cfg = _load_benchmark_section(config_path)
    mode_raw = benchmark_cfg.get("value_size_mode", ValueSizeMode.FIXED.value)
    mode_str = str(mode_raw).upper()
    try:
        mode = ValueSizeMode[mode_str].value
    except KeyError as exc:
        raise ValueError(
            f"unsupported benchmark.value_size_mode: {mode_raw} "
            f"(expected: FIXED/RANDOM_WEIGHTED_SET)"
        ) from exc

    if mode == ValueSizeMode.FIXED.value:
        if "value_size" not in benchmark_cfg:
            raise ValueError("benchmark.value_size is required when value_size_mode == FIXED")
        return mode, int(benchmark_cfg["value_size"]), []

    weighted_set = _parse_value_size_weighted_set(
        benchmark_cfg.get("value_size_weighted_set"),
        ctx="benchmark.value_size_weighted_set",
    )
    if benchmark_cfg.get("value_size_list"):
        raise ValueError(
            "benchmark.value_size_list must be empty when benchmark.value_size_mode == RANDOM_WEIGHTED_SET"
        )
    return mode, None, weighted_set




def _required_output_result_path(config_path: str) -> str:
    """Return required output.result_path from benchmark config.

    This is used by automation to collect comparable results across variants/scales.
    """
    cfg = _load_full_config(config_path)
    out_cfg = cfg.get("output")
    if not isinstance(out_cfg, dict):
        raise ValueError("benchmark CONFIG.output must be a mapping")
    rp = out_cfg.get("result_path")
    if not isinstance(rp, str) or not rp.strip():
        raise ValueError("benchmark CONFIG.output.result_path must be a non-empty string")
    return rp.strip()
KVCACHE_CONFIG_PATH = "./benchmark_config.py"
THREADS_PER_PROCESS, MAX_BENCHMARK_SECONDS, METRIC_WARMUP_SECONDS, START_IDLE_SECONDS, CLUSTER_READY_TIMEOUT_SECONDS = get_benchmark_params(
    KVCACHE_CONFIG_PATH
)
VALUE_SIZE_MODE, VALUE_SIZE, VALUE_SIZE_WEIGHTED_SET = get_value_size_strategy(
    KVCACHE_CONFIG_PATH
)
VALUE_SIZE_SWEEP_LIST = get_benchmark_value_size_sweep_list(
    KVCACHE_CONFIG_PATH
)
OP_TIMEOUT_SECONDS = get_op_timeout_seconds(KVCACHE_CONFIG_PATH)
print(
    f"ℹ️ 从配置加载 THREADS_PER_PROCESS={THREADS_PER_PROCESS}, MAX_BENCHMARK_SECONDS={MAX_BENCHMARK_SECONDS}, "
    f"METRIC_WARMUP_SECONDS={METRIC_WARMUP_SECONDS}, START_IDLE_SECONDS={START_IDLE_SECONDS}"
)
print(
    f"ℹ️ 从配置加载 value_size_list={VALUE_SIZE_SWEEP_LIST}"
)
print(f"ℹ️ 从配置加载 OP_TIMEOUT_SECONDS: {OP_TIMEOUT_SECONDS}")


def get_consumer_sim_handle_ms_range(config_path: str) -> Optional[Tuple[int, int]]:
    """Read optional consumer simulated handle time range from benchmark_config.

    Returns (min_ms, max_ms) or None (when not configured or invalid).
    """
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "benchmark_config_module", config_path
    )
    if spec is None or spec.loader is None:
        print(f"❌ 配置模块不可用: {config_path}")
        return None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    cfg = getattr(module, "CONFIG", None)
    if not isinstance(cfg, dict):
        print("❌ CONFIG 配置格式错误（非字典）")
        return None

    benchmark_cfg = cfg.get("benchmark")
    if not isinstance(benchmark_cfg, dict):
        print("❌ 缺少 benchmark 字段或类型错误")
        return None

    val = benchmark_cfg.get("consumer_sim_handle_ms_range")
    if val is None:
        return None

    # Allow [min, max] or (min, max)
    if not isinstance(val, (list, tuple)) or len(val) != 2:
        print(
            f"⚠️ consumer_sim_handle_ms_range 配置格式错误，应为 [min_ms, max_ms]，实际为: {val}"
        )
        return None

    try:
        min_ms = int(val[0])
        max_ms = int(val[1])
    except Exception as exc:
        print(f"⚠️ consumer_sim_handle_ms_range 解析失败: {val} ({exc})")
        return None

    if min_ms < 0 or max_ms < 0 or max_ms < min_ms:
        print(
            f"⚠️ consumer_sim_handle_ms_range 值非法，应满足 0 <= min_ms <= max_ms，实际为: {val}"
        )
        return None

    return (min_ms, max_ms)


def get_mpmc_active_producer_runtime_limit(config_path: str) -> Optional[int]:
    """Read the optional MPMC active producer runtime limit."""
    benchmark_cfg = _load_benchmark_section(config_path)
    raw_limit = benchmark_cfg.get("mpmc_active_producer_runtime_limit")
    if raw_limit is None:
        return None
    try:
        limit = int(raw_limit)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "benchmark.mpmc_active_producer_runtime_limit must be an integer"
        ) from exc
    if limit <= 0:
        raise ValueError(
            "benchmark.mpmc_active_producer_runtime_limit must be > 0, "
            f"got: {raw_limit!r}"
        )
    return limit


def get_value_size_from_config(config_path: str) -> int:
    """Read value_size from benchmark_config."""
    _, fixed_value_size, _ = get_value_size_strategy(config_path)
    if fixed_value_size is None:
        raise ValueError("benchmark.value_size is not available for the current value size mode")
    return int(fixed_value_size)


def get_test_mode_from_config(config_path: str) -> str:
    """Read test mode from benchmark_config and map it to TestMode.

    Allowed values: KVSTORE / KVSTORE_WITH_LOCAL_CACHE / RPC / MPMC
    """
    benchmark_cfg = _load_benchmark_section(config_path)
    if "mode" not in benchmark_cfg:
        print("❌ benchmark.mode 未配置")
        exit(1)
    mode_raw = benchmark_cfg["mode"]
    # Normalize to uppercase to align with enum variant names.
    mode_str = str(mode_raw).upper()

    try:
        return TestMode[mode_str].value
    except KeyError:
        print(
            f"❌ 不支持的 benchmark.mode: {mode_raw} (期望: KVSTORE/KVSTORE_WITH_LOCAL_CACHE/RPC/MPMC)"
        )
        exit(1)


def _write_benchmark_result_file(all_summaries: List[Dict[str, Any]]) -> None:
    out_path = _required_output_result_path(KVCACHE_CONFIG_PATH)
    payload = {
        "config_path": KVCACHE_CONFIG_PATH,
        "test_mode": CURR_TEST_MODE,
        "value_size_mode": VALUE_SIZE_MODE,
        "value_size_weighted_set": VALUE_SIZE_WEIGHTED_SET,
        "max_benchmark_seconds": MAX_BENCHMARK_SECONDS,
        "metric_warmup_seconds": METRIC_WARMUP_SECONDS,
        "runs": all_summaries,
    }
    out_p = Path(out_path)
    out_p.parent.mkdir(parents=True, exist_ok=True)
    out_p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"🧾 Wrote benchmark result to: {out_path}")


def _result_wait_timeout_seconds(max_benchmark_seconds: int, metric_warmup_seconds: float) -> float:
    return (
        float(max_benchmark_seconds)
        + float(metric_warmup_seconds)
        + float(RESULT_REPORT_TIMEOUT_EXTRA_SECONDS)
    )


def _hold_forever_after_result_written(*, reason: str) -> None:
    # The coordinator runs under a supervisor that may restart the child process after a
    # clean exit. Exiting after producing benchmark_result.json risks an unintended rerun
    # that overwrites the same output path and corrupts the run directory. Holding keeps
    # the output stable until the deployer tears this workload down.
    logger.info(f"🧷 Holding coordinator after writing benchmark_result.json: reason={reason}")
    while True:
        time.sleep(3600.0)


def get_node_roles_from_config(config_path: str, test_mode: str) -> List[str]:
    """Read required benchmark.node_roles from benchmark_config.

    This avoids implicit role derivation from fixed constants, keeping scale/mode automation
    deterministic and config-driven.
    """
    cfg = _load_full_config(config_path)

    node_overrides = cfg.get("node_overrides")
    if not isinstance(node_overrides, list) or not node_overrides:
        raise ValueError("benchmark CONFIG.node_overrides must be a non-empty list")

    bench_cfg = cfg.get("benchmark")
    if not isinstance(bench_cfg, dict):
        raise ValueError("benchmark CONFIG.benchmark must be a mapping")

    roles_raw = bench_cfg.get("node_roles")
    if not isinstance(roles_raw, list) or not roles_raw:
        raise ValueError("benchmark.benchmark.node_roles must be a non-empty list")

    roles: List[str] = []
    for r in roles_raw:
        if not isinstance(r, str) or not r.strip():
            raise ValueError(f"benchmark.benchmark.node_roles contains invalid role: {r!r}")
        roles.append(r.strip())

    if len(roles) != len(node_overrides):
        raise ValueError(
            "benchmark.benchmark.node_roles length must match node_overrides length: "
            f"roles={len(roles)} node_overrides={len(node_overrides)}"
        )

    if test_mode == TestMode.MPMC.value:
        allowed = {"producer", "consumer"}
    else:
        roles = [canonicalize_kv_node_role(role) for role in roles]
        allowed = {KV_NODE_ROLE_SEED, KV_NODE_ROLE_WORKER}

    bad = [r for r in roles if r not in allowed]
    if bad:
        raise ValueError(
            f"benchmark.benchmark.node_roles contains invalid roles for mode={test_mode}: {bad}"
        )

    return roles



KVCACHE_CONFIG_PATH = "./benchmark_config.py"
if VALUE_SIZE is not None:
    print(f"ℹ️ 从配置加载 VALUE_SIZE: {VALUE_SIZE} bytes")
else:
    print(f"ℹ️ 从配置加载 VALUE_SIZE_MODE: {VALUE_SIZE_MODE}")
    print(f"ℹ️ 从配置加载 VALUE_SIZE_WEIGHTED_SET: {VALUE_SIZE_WEIGHTED_SET}")

CONSUMER_SIM_HANDLE_MS_RANGE = get_consumer_sim_handle_ms_range(KVCACHE_CONFIG_PATH)
print(
    f"ℹ️ 从配置加载 CONSUMER_SIM_HANDLE_MS_RANGE: {CONSUMER_SIM_HANDLE_MS_RANGE}"
)

MPMC_ACTIVE_PRODUCER_RUNTIME_LIMIT = get_mpmc_active_producer_runtime_limit(KVCACHE_CONFIG_PATH)
print(
    f"ℹ️ 从配置加载 MPMC_ACTIVE_PRODUCER_RUNTIME_LIMIT: {MPMC_ACTIVE_PRODUCER_RUNTIME_LIMIT}"
)
MPMC_PRODUCER_RUNTIME_MODE_ACTIVE = "active"
MPMC_PRODUCER_RUNTIME_MODE_LOGICAL_ONLY = "logical_only"


CURR_TEST_MODE = get_test_mode_from_config(KVCACHE_CONFIG_PATH)
print(f"ℹ️ 从配置加载 CURR_TEST_MODE: {CURR_TEST_MODE}")
if CURR_TEST_MODE == TestMode.MPMC.value and VALUE_SIZE_MODE != ValueSizeMode.FIXED.value:
    raise ValueError("benchmark.value_size_mode must be FIXED when benchmark.mode == MPMC")

COORDINATOR_HOST = "127.0.0.1"  # will be overridden by YAML to 0.0.0.0
COORDINATOR_PORT = 7777  # will be overridden by YAML coordinator.port
# Node role assignment strategy
NODE_ROLES = get_node_roles_from_config(KVCACHE_CONFIG_PATH, CURR_TEST_MODE)



# Configure logging with colors
class ColoredFormatter(logging.Formatter):
    """Formatter that adds colors for different log levels."""

    # ANSI color codes
    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[94m",  # Blue
        "WARNING": "\033[35m",  # Purple
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[41m",  # red background
        "RESET": "\033[0m",  # reset color
    }

    def format(self, record):
        log_color = self.COLORS.get(record.levelname, self.COLORS["RESET"])
        record.levelname = (
            f"{log_color}[COORDINATOR-{record.levelname}]{self.COLORS['RESET']}"
        )
        return super().format(record)


# Ensure timely flush
# logging.getLogger().handlers[0].flush = lambda: None
# Colored logging
handler = logging.StreamHandler()
handler.setFormatter(
    ColoredFormatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
# Use a conservative log level by default (INFO+ only) to avoid noisy connection churn logs.
# If more detailed debugging is needed (e.g. TCP send/recv details), temporarily switch to DEBUG.
logging.basicConfig(
    level=logging.INFO,
    handlers=[handler],
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("coordinator")

START_WAITING_WARNING_LOG_INTERVAL_SECONDS = 10.0


@dataclass
class TestConfig:
    """Test configuration."""

    test_id: str
    threads_per_process: int
    value_size_mode: str
    test_mode: str
    max_benchmark_seconds: int
    cluster_ready_timeout_seconds: int
    op_timeout_seconds: float
    # Coordinator round-completion timeout depends on this field as much as the
    # node runtime does, so it must stay part of the shared round config.
    metric_warmup_seconds: float
    start_idle_seconds: float
    value_size: Optional[int] = None
    value_size_weighted_set: Optional[List[Dict[str, Any]]] = None
    kvcache_config: Optional[dict] = None  # KVCache config dict


@dataclass
class NodeConfig:
    """Per-node configuration."""

    node_id: str
    test_mode: str
    node_role: str
    threads_per_process: int
    max_benchmark_seconds: int
    cluster_ready_timeout_seconds: int
    op_timeout_seconds: float
    metric_warmup_seconds: float
    start_idle_seconds: float
    value_size_mode: str
    value_size: Optional[int]
    value_size_weighted_set: Optional[List[Dict[str, Any]]]
    kvcache_config: dict
    key_prefix: str
    affinity_slot_index: Optional[int] = None
    # MQ / channel related config (MPMC only)
    mq_role: Optional[str] = None
    mq_weight: float = 1.0
    mq_config: Optional[dict] = None
    mq_unique_id: Optional[str] = None
    mpmc_producer_runtime_mode: Optional[str] = None
    expected_mpmc_consumer_workers: Optional[int] = None
    mpmc_producer_prefeed_leader: Optional[bool] = None
    # Optional: simulated MQ consumer handling time (milliseconds range)
    consumer_sim_handle_ms_range: Optional[Tuple[int, int]] = None
    network_sample: Optional[Dict[str, Any]] = None
    prometheus_base_url: Optional[str] = None
    otlp_log_api: Optional[Dict[str, Any]] = None


@dataclass
class NodeMetrics:
    """Per-node performance metrics."""

    test_id: str
    node_id: str
    node_role: str
    total_operations: int
    successful_operations: int
    failed_operations: int
    get_total_operations: int
    get_hit_operations: int
    get_miss_operations: int
    get_error_operations: int
    avg_latency_us: float
    p50_latency_us: float
    p99_latency_us: float
    p95_latency_us: float
    throughput_ops_per_sec: float
    total_throughput_ops_per_sec: float
    get_total_throughput_ops_per_sec: float
    get_hit_throughput_ops_per_sec: float
    get_miss_throughput_ops_per_sec: float
    total_bytes_processed: int
    total_duration_seconds: float
    error_details: Dict[str, int]
    # Slowest operations reported by each node (from benchmark_node.top_slowest_operations)
    top_slowest_operations: List[Dict[str, Any]]
    inflight_max: int
    inflight_avg: float
    observed_value_size_histogram: Dict[str, int]
    observed_value_size_avg: float
    observed_value_size_min: int
    observed_value_size_max: int
    fluxon_phase_summary: Dict[str, Any]
    network_bandwidth: Dict[str, Any]
    tcp_thread_transport_summary: Dict[str, Any]
    p2p_receive_transport_summary: Dict[str, Any]
    p2p_rpc_completion_summary: Dict[str, Any]


def _describe_value_size_config(
    value_size_mode: str,
    value_size: Optional[int],
    value_size_weighted_set: Optional[List[Dict[str, Any]]],
) -> str:
    """Return a compact human-readable value-size description."""
    if value_size_mode == ValueSizeMode.RANDOM_WEIGHTED_SET.value:
        weighted_set = value_size_weighted_set or []
        parts = [
            f"{int(item['size_bytes'])}B@{float(item['weight'])}"
            for item in weighted_set
        ]
        return f"RANDOM_WEIGHTED_SET[{', '.join(parts)}]"
    return f"{int(value_size or 0)}B"


def _weighted_mean(values: List[Tuple[float, int]]) -> float:
    denom = sum(weight for _, weight in values)
    if denom <= 0:
        return 0.0
    return sum(value * weight for value, weight in values) / float(denom)


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
    op_summary = fluxon_phase_summary.get("RPC", {})
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
        "op_name": "RPC",
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


def _aggregate_fluxon_phase_summary(results: List[NodeMetrics]) -> Dict[str, Any]:
    acc: Dict[str, Dict[str, Any]] = {}
    for node in results:
        node_summary = getattr(node, "fluxon_phase_summary", {})
        if not isinstance(node_summary, dict):
            continue
        for op_name, op_summary in node_summary.items():
            if not isinstance(op_summary, dict):
                continue
            count = int(op_summary.get("count", 0))
            if count <= 0:
                continue
            bucket_counts_raw = op_summary.get("bucket_counts", {})
            extra_avg_raw = op_summary.get("extra_avg_us", {})
            segment_avg_raw = op_summary.get("segment_avg_us", {})
            segment_counts_raw = op_summary.get("segment_counts", {})
            segment_max_raw = op_summary.get("segment_max_us", {})
            path_metric_avg_raw = op_summary.get("path_metric_avg_us", {})
            path_metric_counts_raw = op_summary.get("path_metric_counts", {})
            path_metric_max_raw = op_summary.get("path_metric_max_us", {})
            op_acc = acc.setdefault(
                str(op_name),
                {
                    "count": 0,
                    "submit_total_us": 0.0,
                    "wait_total_us": 0.0,
                    "finalize_total_us": 0.0,
                    "total_total_us": 0.0,
                    "max_total_us": 0.0,
                    "deadline_overrun_count": 0,
                    "bucket_counts": {"ok": 0, "miss": 0, "timeout": 0, "error": 0},
                    "extra_total_us": {},
                    "segment_total_us": {},
                    "segment_counts": {},
                    "segment_max_us": {},
                    "path_metric_total_us": {},
                    "path_metric_counts": {},
                    "path_metric_max_us": {},
                },
            )
            op_acc["count"] += count
            op_acc["submit_total_us"] += float(op_summary.get("submit_avg_us", 0.0)) * count
            op_acc["wait_total_us"] += float(op_summary.get("wait_avg_us", 0.0)) * count
            op_acc["finalize_total_us"] += float(op_summary.get("finalize_avg_us", 0.0)) * count
            op_acc["total_total_us"] += float(op_summary.get("total_avg_us", 0.0)) * count
            op_acc["max_total_us"] = max(op_acc["max_total_us"], float(op_summary.get("max_total_us", 0.0)))
            op_acc["deadline_overrun_count"] += int(op_summary.get("deadline_overrun_count", 0))
            if isinstance(bucket_counts_raw, dict):
                for bucket_name in ("ok", "miss", "timeout", "error"):
                    op_acc["bucket_counts"][bucket_name] += int(bucket_counts_raw.get(bucket_name, 0))
            if isinstance(extra_avg_raw, dict):
                for phase_name, avg_us in extra_avg_raw.items():
                    op_acc["extra_total_us"][str(phase_name)] = (
                        float(op_acc["extra_total_us"].get(str(phase_name), 0.0))
                        + float(avg_us) * count
                    )
            if isinstance(segment_counts_raw, dict):
                for phase_name, segment_count_raw in segment_counts_raw.items():
                    phase_name_str = str(phase_name)
                    segment_count = int(segment_count_raw)
                    op_acc["segment_counts"][phase_name_str] = (
                        int(op_acc["segment_counts"].get(phase_name_str, 0)) + segment_count
                    )
                    if segment_count <= 0:
                        continue
                    segment_avg_us = 0.0
                    if isinstance(segment_avg_raw, dict):
                        segment_avg_us = float(segment_avg_raw.get(phase_name, 0.0))
                    op_acc["segment_total_us"][phase_name_str] = (
                        float(op_acc["segment_total_us"].get(phase_name_str, 0.0))
                        + segment_avg_us * segment_count
                    )
                    if isinstance(segment_max_raw, dict):
                        op_acc["segment_max_us"][phase_name_str] = max(
                            float(op_acc["segment_max_us"].get(phase_name_str, 0.0)),
                            float(segment_max_raw.get(phase_name, 0.0)),
                        )
            if isinstance(path_metric_counts_raw, dict):
                for metric_name, metric_bucket_counts_raw in path_metric_counts_raw.items():
                    metric_name_str = str(metric_name)
                    if not isinstance(metric_bucket_counts_raw, dict):
                        continue
                    metric_counts_acc = op_acc["path_metric_counts"].setdefault(metric_name_str, {})
                    metric_totals_acc = op_acc["path_metric_total_us"].setdefault(metric_name_str, {})
                    metric_max_acc = op_acc["path_metric_max_us"].setdefault(metric_name_str, {})
                    metric_avg_buckets_raw: Dict[str, Any] = {}
                    if isinstance(path_metric_avg_raw, dict):
                        avg_candidate = path_metric_avg_raw.get(metric_name)
                        if isinstance(avg_candidate, dict):
                            metric_avg_buckets_raw = avg_candidate
                    metric_max_buckets_raw: Dict[str, Any] = {}
                    if isinstance(path_metric_max_raw, dict):
                        max_candidate = path_metric_max_raw.get(metric_name)
                        if isinstance(max_candidate, dict):
                            metric_max_buckets_raw = max_candidate
                    for path_bucket, metric_count_raw in metric_bucket_counts_raw.items():
                        path_bucket_str = str(path_bucket)
                        metric_count = int(metric_count_raw)
                        metric_counts_acc[path_bucket_str] = (
                            int(metric_counts_acc.get(path_bucket_str, 0)) + metric_count
                        )
                        if metric_count <= 0:
                            continue
                        metric_avg_us = float(metric_avg_buckets_raw.get(path_bucket, 0.0))
                        metric_totals_acc[path_bucket_str] = (
                            float(metric_totals_acc.get(path_bucket_str, 0.0))
                            + metric_avg_us * metric_count
                        )
                        metric_max_acc[path_bucket_str] = max(
                            float(metric_max_acc.get(path_bucket_str, 0.0)),
                            float(metric_max_buckets_raw.get(path_bucket, 0.0)),
                        )

    out: Dict[str, Any] = {}
    for op_name, op_acc in sorted(acc.items()):
        count = int(op_acc["count"])
        if count <= 0:
            continue
        extra_avg_us = {
            phase_name: float(total_us) / float(count)
            for phase_name, total_us in sorted(op_acc["extra_total_us"].items())
        }
        segment_avg_us: Dict[str, float] = {}
        segment_counts: Dict[str, int] = {}
        segment_max_us: Dict[str, float] = {}
        for phase_name, segment_count_raw in sorted(op_acc["segment_counts"].items()):
            segment_count = int(segment_count_raw)
            segment_counts[phase_name] = segment_count
            segment_max_us[phase_name] = float(op_acc["segment_max_us"].get(phase_name, 0.0))
            if segment_count > 0:
                segment_avg_us[phase_name] = (
                    float(op_acc["segment_total_us"].get(phase_name, 0.0)) / float(segment_count)
                )
        path_metric_avg_us: Dict[str, Dict[str, float]] = {}
        path_metric_counts: Dict[str, Dict[str, int]] = {}
        path_metric_max_us: Dict[str, Dict[str, float]] = {}
        for metric_name, metric_bucket_counts_raw in sorted(op_acc["path_metric_counts"].items()):
            if not isinstance(metric_bucket_counts_raw, dict):
                continue
            metric_avg_entry: Dict[str, float] = {}
            metric_count_entry: Dict[str, int] = {}
            metric_max_entry: Dict[str, float] = {}
            metric_totals_raw = op_acc["path_metric_total_us"].get(metric_name, {})
            metric_maxima_raw = op_acc["path_metric_max_us"].get(metric_name, {})
            for path_bucket, metric_count_raw in sorted(metric_bucket_counts_raw.items()):
                metric_count = int(metric_count_raw)
                metric_count_entry[str(path_bucket)] = metric_count
                metric_max_entry[str(path_bucket)] = (
                    float(metric_maxima_raw.get(path_bucket, 0.0))
                    if isinstance(metric_maxima_raw, dict)
                    else 0.0
                )
                if metric_count > 0 and isinstance(metric_totals_raw, dict):
                    metric_avg_entry[str(path_bucket)] = (
                        float(metric_totals_raw.get(path_bucket, 0.0)) / float(metric_count)
                    )
            path_metric_avg_us[str(metric_name)] = metric_avg_entry
            path_metric_counts[str(metric_name)] = metric_count_entry
            path_metric_max_us[str(metric_name)] = metric_max_entry
        out[op_name] = {
            "count": count,
            "submit_avg_us": float(op_acc["submit_total_us"]) / float(count),
            "wait_avg_us": float(op_acc["wait_total_us"]) / float(count),
            "finalize_avg_us": float(op_acc["finalize_total_us"]) / float(count),
            "total_avg_us": float(op_acc["total_total_us"]) / float(count),
            "max_total_us": float(op_acc["max_total_us"]),
            "deadline_overrun_count": int(op_acc["deadline_overrun_count"]),
            "bucket_counts": {
                "ok": int(op_acc["bucket_counts"]["ok"]),
                "miss": int(op_acc["bucket_counts"]["miss"]),
                "timeout": int(op_acc["bucket_counts"]["timeout"]),
                "error": int(op_acc["bucket_counts"]["error"]),
            },
            "extra_avg_us": extra_avg_us,
            "segment_avg_us": segment_avg_us,
            "segment_max_us": segment_max_us,
            "segment_counts": segment_counts,
            "path_metric_avg_us": path_metric_avg_us,
            "path_metric_max_us": path_metric_max_us,
            "path_metric_counts": path_metric_counts,
        }
    return out


def _aggregate_network_bandwidth_by_machine(results: List[NodeMetrics]) -> Dict[str, Any]:
    machine_summaries: Dict[str, Dict[str, Any]] = {}
    for node in results:
        network_bandwidth = getattr(node, "network_bandwidth", {})
        if not isinstance(network_bandwidth, dict):
            continue
        if not bool(network_bandwidth.get("leader")):
            continue
        target = network_bandwidth.get("target")
        if not isinstance(target, str) or not target.strip():
            continue
        existing = machine_summaries.get(target)
        sample_count = int(network_bandwidth.get("sample_count", 0))
        if existing is not None and int(existing.get("sample_count", 0)) >= sample_count:
            continue
        machine_summaries[target] = {
            "leader_node_id": node.node_id,
            "target": target,
            "sample_interval_seconds": float(network_bandwidth.get("sample_interval_seconds", 0.0)),
            "sample_count": sample_count,
            "interface_names": copy.deepcopy(network_bandwidth.get("interface_names", [])),
            "avg_rx_mbps": float(network_bandwidth.get("avg_rx_mbps", 0.0)),
            "avg_tx_mbps": float(network_bandwidth.get("avg_tx_mbps", 0.0)),
            "peak_rx_mbps": float(network_bandwidth.get("peak_rx_mbps", 0.0)),
            "peak_tx_mbps": float(network_bandwidth.get("peak_tx_mbps", 0.0)),
            "total_rx_bytes_delta": int(network_bandwidth.get("total_rx_bytes_delta", 0)),
            "total_tx_bytes_delta": int(network_bandwidth.get("total_tx_bytes_delta", 0)),
            "samples": copy.deepcopy(network_bandwidth.get("samples", [])),
            "error": str(network_bandwidth.get("error", "")),
        }

    machine_list = [machine_summaries[target] for target in sorted(machine_summaries.keys())]
    return {
        "machine_count": len(machine_list),
        "sum_avg_rx_mbps": sum(float(machine.get("avg_rx_mbps", 0.0)) for machine in machine_list),
        "sum_avg_tx_mbps": sum(float(machine.get("avg_tx_mbps", 0.0)) for machine in machine_list),
        "sum_peak_rx_mbps": sum(float(machine.get("peak_rx_mbps", 0.0)) for machine in machine_list),
        "sum_peak_tx_mbps": sum(float(machine.get("peak_tx_mbps", 0.0)) for machine in machine_list),
        "max_machine_peak_rx_mbps": max((float(machine.get("peak_rx_mbps", 0.0)) for machine in machine_list), default=0.0),
        "max_machine_peak_tx_mbps": max((float(machine.get("peak_tx_mbps", 0.0)) for machine in machine_list), default=0.0),
        "machines": machine_list,
    }


def _aggregate_tcp_thread_transport_summary(results: List[NodeMetrics]) -> Dict[str, Any]:
    transport_nodes: List[Dict[str, Any]] = []
    machine_summaries: Dict[str, Dict[str, Any]] = {}
    window_seconds = 0.0
    for node in results:
        summary = getattr(node, "tcp_thread_transport_summary", {})
        if not isinstance(summary, dict) or not summary:
            continue
        target = str(summary.get("target", "")).strip()
        leader = bool(summary.get("leader"))
        if summary.get("error"):
            rec = {
                "node_id": node.node_id,
                "node_role": node.node_role,
                "scope": str(summary.get("scope", "owner_owner")).strip() or "owner_owner",
                "target": target,
                "leader": leader,
                "error": str(summary.get("error")),
            }
            transport_nodes.append(rec)
            if target and target not in machine_summaries:
                machine_summaries[target] = rec
            continue
        if not leader:
            continue
        node_window_seconds = float(summary.get("window_seconds", 0.0))
        window_seconds = max(window_seconds, node_window_seconds)
        rec = {
            "node_id": node.node_id,
            "node_role": node.node_role,
            "target": target,
            "leader": True,
            "window_seconds": node_window_seconds,
            "matched_latency_series_count": int(summary.get("matched_latency_series_count", 0)),
            "matched_label_pairs": copy.deepcopy(summary.get("matched_label_pairs", [])),
            "send_enqueued_bytes_total_delta": int(summary.get("send_enqueued_bytes_total_delta", 0)),
            "send_enqueued_messages_total_delta": int(summary.get("send_enqueued_messages_total_delta", 0)),
            "socket_submitted_bytes_total_delta": int(summary.get("socket_submitted_bytes_total_delta", 0)),
            "socket_submitted_messages_total_delta": int(summary.get("socket_submitted_messages_total_delta", 0)),
            "send_enqueued_bytes_per_sec": float(summary.get("send_enqueued_bytes_per_sec", 0.0)),
            "send_enqueued_messages_per_sec": float(summary.get("send_enqueued_messages_per_sec", 0.0)),
            "socket_submitted_bytes_per_sec": float(summary.get("socket_submitted_bytes_per_sec", 0.0)),
            "socket_submitted_messages_per_sec": float(summary.get("socket_submitted_messages_per_sec", 0.0)),
        }
        transport_nodes.append(rec)
        if not target:
            continue
        existing = machine_summaries.get(target)
        if existing is None:
            machine_summaries[target] = rec
            continue
        if existing.get("error") and not rec.get("error"):
            machine_summaries[target] = rec
            continue
        if rec.get("error"):
            continue
        raise RuntimeError(
            f"duplicate tcp_thread transport leader summary for target={target}: "
            f"existing_node_id={existing.get('node_id')} new_node_id={rec.get('node_id')}"
        )

    if not transport_nodes:
        return {}

    machine_list = [machine_summaries[target] for target in sorted(machine_summaries.keys())]
    valid_nodes = [node for node in machine_list if "error" not in node]
    return {
        "window_seconds": window_seconds,
        "node_count": len(valid_nodes),
        "send_enqueued_bytes_total_delta": sum(int(node.get("send_enqueued_bytes_total_delta", 0)) for node in valid_nodes),
        "send_enqueued_messages_total_delta": sum(int(node.get("send_enqueued_messages_total_delta", 0)) for node in valid_nodes),
        "socket_submitted_bytes_total_delta": sum(int(node.get("socket_submitted_bytes_total_delta", 0)) for node in valid_nodes),
        "socket_submitted_messages_total_delta": sum(int(node.get("socket_submitted_messages_total_delta", 0)) for node in valid_nodes),
        "send_enqueued_bytes_per_sec": sum(float(node.get("send_enqueued_bytes_per_sec", 0.0)) for node in valid_nodes),
        "send_enqueued_messages_per_sec": sum(float(node.get("send_enqueued_messages_per_sec", 0.0)) for node in valid_nodes),
        "socket_submitted_bytes_per_sec": sum(float(node.get("socket_submitted_bytes_per_sec", 0.0)) for node in valid_nodes),
        "socket_submitted_messages_per_sec": sum(float(node.get("socket_submitted_messages_per_sec", 0.0)) for node in valid_nodes),
        "nodes": machine_list,
        "raw_node_summaries": transport_nodes,
    }


def _aggregate_p2p_receive_transport_summary(results: List[NodeMetrics]) -> Dict[str, Any]:
    transport_nodes: List[Dict[str, Any]] = []
    machine_summaries: Dict[str, Dict[str, Any]] = {}
    window_seconds = 0.0
    for node in results:
        summary = getattr(node, "p2p_receive_transport_summary", {})
        if not isinstance(summary, dict) or not summary:
            continue
        target = str(summary.get("target", "")).strip()
        leader = bool(summary.get("leader"))
        if summary.get("error"):
            rec = {
                "node_id": node.node_id,
                "node_role": node.node_role,
                "target": target,
                "leader": leader,
                "error": str(summary.get("error")),
            }
            transport_nodes.append(rec)
            if target and target not in machine_summaries:
                machine_summaries[target] = rec
            continue
        if not leader:
            continue
        node_window_seconds = float(summary.get("window_seconds", 0.0))
        window_seconds = max(window_seconds, node_window_seconds)
        node_components = summary.get("components", {})
        rec_components = _empty_p2p_receive_transport_components()
        if isinstance(node_components, dict):
            for component in P2P_RECV_PROM_COMPONENTS:
                component_summary = node_components.get(component, {})
                if not isinstance(component_summary, dict):
                    continue
                for metric in P2P_RECV_PROM_METRICS:
                    metric_summary = component_summary.get(metric, {})
                    if not isinstance(metric_summary, dict):
                        continue
                    rec_components[component][metric] = {
                        "bytes_total_delta": int(metric_summary.get("bytes_total_delta", 0)),
                        "messages_total_delta": int(metric_summary.get("messages_total_delta", 0)),
                        "bytes_per_sec": float(metric_summary.get("bytes_per_sec", 0.0)),
                        "messages_per_sec": float(metric_summary.get("messages_per_sec", 0.0)),
                    }
        rec = {
            "node_id": node.node_id,
            "node_role": node.node_role,
            "target": target,
            "leader": True,
            "window_seconds": node_window_seconds,
            "matched_recv_completed_series_count": int(
                summary.get("matched_recv_completed_series_count", 0)
            ),
            "matched_label_pairs": copy.deepcopy(summary.get("matched_label_pairs", [])),
            "components": rec_components,
        }
        transport_nodes.append(rec)
        if not target:
            continue
        existing = machine_summaries.get(target)
        if existing is None:
            machine_summaries[target] = rec
            continue
        if existing.get("error") and not rec.get("error"):
            machine_summaries[target] = rec
            continue
        if rec.get("error"):
            continue
        raise RuntimeError(
            f"duplicate p2p receive transport leader summary for target={target}: "
            f"existing_node_id={existing.get('node_id')} new_node_id={rec.get('node_id')}"
        )

    if not transport_nodes:
        return {}

    machine_list = [machine_summaries[target] for target in sorted(machine_summaries.keys())]
    valid_nodes = [node for node in machine_list if "error" not in node]
    aggregated_components = _empty_p2p_receive_transport_components()
    for node in valid_nodes:
        node_components = node.get("components", {})
        if not isinstance(node_components, dict):
            continue
        for component in P2P_RECV_PROM_COMPONENTS:
            component_summary = node_components.get(component, {})
            if not isinstance(component_summary, dict):
                continue
            for metric in P2P_RECV_PROM_METRICS:
                metric_summary = component_summary.get(metric, {})
                if not isinstance(metric_summary, dict):
                    continue
                aggregated_metric = aggregated_components[component][metric]
                aggregated_metric["bytes_total_delta"] += int(
                    metric_summary.get("bytes_total_delta", 0)
                )
                aggregated_metric["messages_total_delta"] += int(
                    metric_summary.get("messages_total_delta", 0)
                )
                aggregated_metric["bytes_per_sec"] += float(metric_summary.get("bytes_per_sec", 0.0))
                aggregated_metric["messages_per_sec"] += float(
                    metric_summary.get("messages_per_sec", 0.0)
                )

    return {
        "window_seconds": window_seconds,
        "node_count": len(valid_nodes),
        "matched_recv_completed_series_count": sum(
            int(node.get("matched_recv_completed_series_count", 0)) for node in valid_nodes
        ),
        "components": aggregated_components,
        "nodes": machine_list,
        "raw_node_summaries": transport_nodes,
    }


def _aggregate_owner_owner_transport_counters(results: List[NodeMetrics]) -> Dict[str, Any]:
    completion_nodes: List[Dict[str, Any]] = []
    machine_summaries: Dict[str, Dict[str, Any]] = {}
    window_seconds = 0.0
    for node in results:
        summary = getattr(node, "p2p_rpc_completion_summary", {})
        if not isinstance(summary, dict) or not summary:
            continue
        debug_payload = summary.get("debug", {})
        if not isinstance(debug_payload, dict):
            continue
        raw_counters = debug_payload.get("owner_owner_transport_counters", {})
        if not isinstance(raw_counters, dict) or not raw_counters:
            continue
        target = str(raw_counters.get("target", "")).strip()
        leader = bool(raw_counters.get("leader"))
        if raw_counters.get("error"):
            rec = {
                "node_id": node.node_id,
                "node_role": node.node_role,
                "target": target,
                "leader": leader,
                "error": str(raw_counters.get("error")),
            }
            completion_nodes.append(rec)
            if target and target not in machine_summaries:
                machine_summaries[target] = rec
            continue
        node_window_seconds = float(raw_counters.get("window_seconds", 0.0))
        window_seconds = max(window_seconds, node_window_seconds)
        rec = {
            "node_id": node.node_id,
            "node_role": node.node_role,
            "scope": (
                str(
                    raw_counters.get(
                        "scope",
                        P2P_RPC_COMPLETION_SUMMARY_SCOPE_OWNER_OWNER,
                    )
                ).strip()
                or P2P_RPC_COMPLETION_SUMMARY_SCOPE_OWNER_OWNER
            ),
            "semantic_role": (
                str(
                    raw_counters.get(
                        "semantic_role",
                        P2P_RPC_COMPLETION_SUMMARY_DEBUG_ROLE,
                    )
                ).strip()
                or P2P_RPC_COMPLETION_SUMMARY_DEBUG_ROLE
            ),
            "target": target,
            "leader": leader,
            "window_seconds": node_window_seconds,
            "network_sample_target": str(raw_counters.get("network_sample_target", target)).strip(),
            "client_instance_key": str(raw_counters.get("client_instance_key", "")).strip(),
            "server_instance_keys": copy.deepcopy(raw_counters.get("server_instance_keys", [])),
            "matched_activity_series_count": int(raw_counters.get("matched_activity_series_count", 0)),
            "matched_label_pairs": copy.deepcopy(raw_counters.get("matched_label_pairs", [])),
            "request_fast_bytes_total_delta": int(raw_counters.get("request_fast_bytes_total_delta", 0)),
            "request_fast_messages_total_delta": int(raw_counters.get("request_fast_messages_total_delta", 0)),
            "request_slow_bytes_total_delta": int(raw_counters.get("request_slow_bytes_total_delta", 0)),
            "request_slow_messages_total_delta": int(raw_counters.get("request_slow_messages_total_delta", 0)),
            "response_fast_bytes_total_delta": int(raw_counters.get("response_fast_bytes_total_delta", 0)),
            "response_fast_messages_total_delta": int(raw_counters.get("response_fast_messages_total_delta", 0)),
            "response_slow_bytes_total_delta": int(raw_counters.get("response_slow_bytes_total_delta", 0)),
            "response_slow_messages_total_delta": int(raw_counters.get("response_slow_messages_total_delta", 0)),
            "request_fast_bytes_per_sec": float(raw_counters.get("request_fast_bytes_per_sec", 0.0)),
            "request_fast_messages_per_sec": float(raw_counters.get("request_fast_messages_per_sec", 0.0)),
            "request_slow_bytes_per_sec": float(raw_counters.get("request_slow_bytes_per_sec", 0.0)),
            "request_slow_messages_per_sec": float(raw_counters.get("request_slow_messages_per_sec", 0.0)),
            "response_fast_bytes_per_sec": float(raw_counters.get("response_fast_bytes_per_sec", 0.0)),
            "response_fast_messages_per_sec": float(raw_counters.get("response_fast_messages_per_sec", 0.0)),
            "response_slow_bytes_per_sec": float(raw_counters.get("response_slow_bytes_per_sec", 0.0)),
            "response_slow_messages_per_sec": float(raw_counters.get("response_slow_messages_per_sec", 0.0)),
        }
        completion_nodes.append(rec)
        if not leader:
            continue
        if not target:
            continue
        existing = machine_summaries.get(target)
        if existing is None:
            machine_summaries[target] = rec
            continue
        if existing.get("error") and not rec.get("error"):
            machine_summaries[target] = rec
            continue
        if rec.get("error"):
            continue
        raise RuntimeError(
            f"duplicate p2p rpc completion leader summary for target={target}: "
            f"existing_node_id={existing.get('node_id')} new_node_id={rec.get('node_id')}"
        )

    if not completion_nodes:
        return {}

    machine_list = [machine_summaries[target] for target in sorted(machine_summaries.keys())]
    valid_owner_nodes = [node for node in machine_list if "error" not in node]
    return {
        "scope": P2P_RPC_COMPLETION_SUMMARY_SCOPE_OWNER_OWNER,
        "semantic_role": P2P_RPC_COMPLETION_SUMMARY_DEBUG_ROLE,
        "window_seconds": window_seconds,
        "node_count": len(valid_owner_nodes),
        "request_node_count": len(valid_owner_nodes),
        "matched_activity_series_count": sum(
            int(node.get("matched_activity_series_count", 0)) for node in valid_owner_nodes
        ),
        "request_fast_bytes_total_delta": sum(int(node.get("request_fast_bytes_total_delta", 0)) for node in valid_owner_nodes),
        "request_fast_messages_total_delta": sum(int(node.get("request_fast_messages_total_delta", 0)) for node in valid_owner_nodes),
        "request_slow_bytes_total_delta": sum(int(node.get("request_slow_bytes_total_delta", 0)) for node in valid_owner_nodes),
        "request_slow_messages_total_delta": sum(int(node.get("request_slow_messages_total_delta", 0)) for node in valid_owner_nodes),
        "response_fast_bytes_total_delta": sum(int(node.get("response_fast_bytes_total_delta", 0)) for node in valid_owner_nodes),
        "response_fast_messages_total_delta": sum(int(node.get("response_fast_messages_total_delta", 0)) for node in valid_owner_nodes),
        "response_slow_bytes_total_delta": sum(int(node.get("response_slow_bytes_total_delta", 0)) for node in valid_owner_nodes),
        "response_slow_messages_total_delta": sum(int(node.get("response_slow_messages_total_delta", 0)) for node in valid_owner_nodes),
        "request_fast_bytes_per_sec": sum(float(node.get("request_fast_bytes_per_sec", 0.0)) for node in valid_owner_nodes),
        "request_fast_messages_per_sec": sum(float(node.get("request_fast_messages_per_sec", 0.0)) for node in valid_owner_nodes),
        "request_slow_bytes_per_sec": sum(float(node.get("request_slow_bytes_per_sec", 0.0)) for node in valid_owner_nodes),
        "request_slow_messages_per_sec": sum(float(node.get("request_slow_messages_per_sec", 0.0)) for node in valid_owner_nodes),
        "response_fast_bytes_per_sec": sum(float(node.get("response_fast_bytes_per_sec", 0.0)) for node in valid_owner_nodes),
        "response_fast_messages_per_sec": sum(float(node.get("response_fast_messages_per_sec", 0.0)) for node in valid_owner_nodes),
        "response_slow_bytes_per_sec": sum(float(node.get("response_slow_bytes_per_sec", 0.0)) for node in valid_owner_nodes),
        "response_slow_messages_per_sec": sum(float(node.get("response_slow_messages_per_sec", 0.0)) for node in valid_owner_nodes),
        "nodes": machine_list,
        "raw_node_summaries": completion_nodes,
    }


def _aggregate_p2p_rpc_completion_summary(
    results: List[NodeMetrics],
    fluxon_phase_summary: Dict[str, Any],
    total_duration_seconds: float,
) -> Dict[str, Any]:
    debug_owner_owner_transport_counters = _aggregate_owner_owner_transport_counters(results)
    return _build_p2p_rpc_completion_summary_from_phase_summary(
        fluxon_phase_summary=fluxon_phase_summary,
        duration_seconds=total_duration_seconds,
        debug_owner_owner_transport_counters=debug_owner_owner_transport_counters,
    )


def _build_aggregated_bench_points(results: List[NodeMetrics]) -> Dict[str, Any]:
    total_ops = sum(r.total_operations for r in results)
    total_successful_ops = sum(r.successful_operations for r in results)
    total_failed_ops = sum(r.failed_operations for r in results)
    get_total_operations = sum(r.get_total_operations for r in results)
    get_hit_operations = sum(r.get_hit_operations for r in results)
    get_miss_operations = sum(r.get_miss_operations for r in results)
    get_error_operations = sum(r.get_error_operations for r in results)
    total_duration = max((r.total_duration_seconds for r in results), default=0.0)
    throughput_ops_per_sec = (
        (float(total_successful_ops) / float(total_duration)) if total_duration > 0 else 0.0
    )
    total_throughput_ops_per_sec = (
        (float(total_ops) / float(total_duration)) if total_duration > 0 else 0.0
    )
    get_total_throughput_ops_per_sec = (
        (float(get_total_operations) / float(total_duration)) if total_duration > 0 else 0.0
    )
    get_hit_throughput_ops_per_sec = (
        (float(get_hit_operations) / float(total_duration)) if total_duration > 0 else 0.0
    )
    get_miss_throughput_ops_per_sec = (
        (float(get_miss_operations) / float(total_duration)) if total_duration > 0 else 0.0
    )
    avg_latency_us = _weighted_mean(
        [(float(r.avg_latency_us), int(r.successful_operations)) for r in results]
    )
    p50_latency_us = _weighted_mean(
        [(float(r.p50_latency_us), int(r.successful_operations)) for r in results]
    )
    p95_latency_us = _weighted_mean(
        [(float(r.p95_latency_us), int(r.successful_operations)) for r in results]
    )
    p99_latency_us = _weighted_mean(
        [(float(r.p99_latency_us), int(r.successful_operations)) for r in results]
    )
    inflight_avg = _weighted_mean(
        [(float(r.inflight_avg), int(r.total_operations)) for r in results]
    )
    inflight_max = max((int(r.inflight_max) for r in results), default=0)
    return {
        "total_ops": total_ops,
        "total_successful_ops": total_successful_ops,
        "total_failed_ops": total_failed_ops,
        "get_total_operations": get_total_operations,
        "get_hit_operations": get_hit_operations,
        "get_miss_operations": get_miss_operations,
        "get_error_operations": get_error_operations,
        "total_duration_seconds": total_duration,
        "throughput_ops_per_sec": throughput_ops_per_sec,
        "total_throughput_ops_per_sec": total_throughput_ops_per_sec,
        "get_total_throughput_ops_per_sec": get_total_throughput_ops_per_sec,
        "get_hit_throughput_ops_per_sec": get_hit_throughput_ops_per_sec,
        "get_miss_throughput_ops_per_sec": get_miss_throughput_ops_per_sec,
        "avg_latency_us": avg_latency_us,
        "p50_latency_us": p50_latency_us,
        "p95_latency_us": p95_latency_us,
        "p99_latency_us": p99_latency_us,
        "inflight_avg": inflight_avg,
        "inflight_max": inflight_max,
    }


class CoordinatorServer:
    """coordinator server"""

    def __init__(self, host: str, port: int):
        self.host = host  # will be set to 0.0.0.0 by YAML
        self.port = port  # will be overridden by YAML
        self.registered_nodes: Dict[str, Dict] = (
            {}
        )  # registered node ,key:node_id,value:status of node
        self.node_configs: Dict[str, NodeConfig] = (
            {}
        )  # node config,   key:node_id,value:node_config
        self.node_messages: Dict[str, List[Dict]] = {}
        # message queue key:node_id,value:messages
        self.test_results: Dict[str, List[NodeMetrics]] = (
            {}
        )  # ，key: test_id, value: list of NodeMetrics
        # Keep per-round terminal gate state by test_id so finished nodes can poll
        # safely even after the coordinator advances to the next round.
        self.round_gate_states: Dict[str, Dict[str, Any]] = {}
        self.ready_nodes_for_current_test: set[str] = set()
        self.runtime_ready_nodes_for_current_test: set[str] = set()
        self.lock = threading.Lock()
        self.all_nodes_ready = threading.Event()
        self.all_runtime_ready = threading.Event()
        self.all_results_received = threading.Event()
        self.test_config: Optional[TestConfig] = None
        self._start_waiting_warning_lock = threading.Lock()
        self._start_waiting_warning_next_log_ts = 0.0
        self._start_waiting_warning_suppressed_count = 0
        # Multi-round test control (value-size sweep only; process fanout is deploy-time fixed).
        self.current_round_index: int = 0
        self.total_rounds: int = 1
        self.has_more_tests: bool = False
        # Will be derived from CONFIG.node_overrides length (no hardcoded defaults).
        self.expected_nodes = 0
        # per-node override patches parsed from YAML (flat list)
        self.per_node_patches: List[Dict[str, Any]] = []
        # map instance_key -> patch for O(1) lookup
        self.instance_patch_map: Dict[str, Dict[str, Any]] = {}
        # map instance_key -> role, derived from CONFIG.benchmark.node_roles and node_overrides order
        self.instance_role_map: Dict[str, str] = {}
        # map instance_key -> key_prefix, derived deterministically from role + ordinal in config
        self.instance_key_prefix_map: Dict[str, str] = {}
        # map MPMC producer instance_key -> active/logical_only runtime mode
        self.instance_mpmc_producer_runtime_mode_map: Dict[str, str] = {}
        # Exactly one active producer is elected to perform cluster prefeed.
        self.instance_mpmc_producer_prefeed_leader_map: Dict[str, bool] = {}
        self.expected_mpmc_consumer_workers: int = 0
        # map instance_key -> affinity slot index, assigned explicitly per benchmark member
        self.instance_affinity_slot_map: Dict[str, int] = {}
        self.instance_network_sample_map: Dict[str, Dict[str, Any]] = {}
        self.prometheus_base_url: Optional[str] = None
        self.otlp_log_api: Optional[Dict[str, Any]] = None
        # MQ configs
        from fluxon_py._api_ext_chan.mq_config_check import MIN_TTL as CHAN_MIN_TTL_SECONDS
        self.mq_config: Dict[str, Any] = {"capacity": 100000, "ttl_seconds": CHAN_MIN_TTL_SECONDS}
        self.mq_unique_id: Optional[str] = None
        # per-node mq config: instance_key -> {role, weight}
        self.instance_mq_map: Dict[str, Dict[str, Any]] = {}
        self.load_kvcache_config()

    def load_kvcache_config(self):
        """Load KVCache config from the Python module.

        Expected top-level keys: kv_base / mq_base, with optional node_overrides / coordinator.
        """
        try:
            spec = importlib.util.spec_from_file_location(
                "benchmark_config_module", KVCACHE_CONFIG_PATH
            )
            if spec is None or spec.loader is None:
                logger.error(f"❌ 配置模块不可用: {KVCACHE_CONFIG_PATH}")
                return
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)  # type: ignore[attr-defined]
            full_cfg = getattr(module, "CONFIG", None) or {}

            if not isinstance(full_cfg, dict):
                logger.error(f"❌ 配置格式错误（非字典）: {KVCACHE_CONFIG_PATH}")
                return

            # kv_base: global KV baseline config (historical name: kvcache_config)
            if "kv_base" not in full_cfg or not isinstance(
                full_cfg.get("kv_base"), dict
            ):
                logger.error("❌ 缺少必需的 kv_base 字段或类型错误")
                return

            # Only the new format is supported; unwrap directly.
            kvcache_config = full_cfg["kv_base"]

            # MQ global config (optional); use CHAN_CONFIG defaults if absent.
            mq_cfg = full_cfg.get("mq_base")
            if isinstance(mq_cfg, dict):
                try:
                    cap = int(mq_cfg.get("capacity", 100000))
                    ttl = int(mq_cfg.get("ttl_seconds", self.mq_config["ttl_seconds"]))
                    self.mq_config = {"capacity": cap, "ttl_seconds": ttl}
                    logger.info(
                        f"🔧 加载 MQ 全局配置: capacity={cap}, ttl_seconds={ttl}"
                    )
                except Exception as e:  # noqa: BLE001
                    logger.error(f"❌ MQ 全局配置解析失败: {e}")

            mq_unique_id_raw = full_cfg.get("mq_new_or_bind_unique_key")
            if mq_unique_id_raw is not None:
                if not isinstance(mq_unique_id_raw, str) or not mq_unique_id_raw.strip():
                    logger.error("❌ mq_new_or_bind_unique_key 必须是非空字符串")
                    return
                self.mq_unique_id = mq_unique_id_raw.strip()

            monitoring_cfg = full_cfg.get("monitoring")
            if monitoring_cfg is None:
                self.prometheus_base_url = None
                self.otlp_log_api = None
            elif not isinstance(monitoring_cfg, dict):
                logger.error("❌ monitoring 必须是字典")
                return
            else:
                prom_base = monitoring_cfg.get("prometheus_base_url")
                if prom_base is None:
                    self.prometheus_base_url = None
                elif not isinstance(prom_base, str) or not prom_base.strip():
                    logger.error("❌ monitoring.prometheus_base_url 必须是非空字符串")
                    return
                else:
                    self.prometheus_base_url = prom_base.strip().rstrip("/")

                otlp_log_api = monitoring_cfg.get("otlp_log_api")
                if otlp_log_api is None:
                    self.otlp_log_api = None
                elif not isinstance(otlp_log_api, dict):
                    logger.error("❌ monitoring.otlp_log_api 必须是字典")
                    return
                else:
                    otlp_endpoint = otlp_log_api.get("otlp_endpoint")
                    db_name = otlp_log_api.get("db_name")
                    table_name = otlp_log_api.get("table_name")
                    if not isinstance(otlp_endpoint, str) or not otlp_endpoint.strip():
                        logger.error("❌ monitoring.otlp_log_api.otlp_endpoint 必须是非空字符串")
                        return
                    if not isinstance(db_name, str) or not db_name.strip():
                        logger.error("❌ monitoring.otlp_log_api.db_name 必须是非空字符串")
                        return
                    normalized_otlp_log_api = {
                        "otlp_endpoint": otlp_endpoint.strip(),
                        "db_name": db_name.strip(),
                    }
                    if table_name is not None:
                        if not isinstance(table_name, str) or not table_name.strip():
                            logger.error("❌ monitoring.otlp_log_api.table_name 必须是非空字符串")
                            return
                        normalized_otlp_log_api["table_name"] = table_name.strip()
                    self.otlp_log_api = normalized_otlp_log_api

            # Parse per-node KV/MQ overrides: node_overrides is a list at the top level.
            # Schema:
            #   - kv: dict, must contain instance_key and KV overrides for that node
            #   - mq_role: str, producer/consumer (MPMC mode only)
            #   - mq: dict, overrides mq_base (e.g. weight/capacity/ttl_seconds)
            self.per_node_patches = []
            ovr = full_cfg.get("node_overrides")
            if isinstance(ovr, list):
                self.per_node_patches = [p if isinstance(p, dict) else {} for p in ovr]
            elif ovr is not None:
                logger.error(
                    "❌ node_overrides 必须是列表类型（元素为包含 kv/mq_role/mq 的字典）"
                )

            # Parse coordinator listen config: host is fixed to 0.0.0.0; port must be provided in config.
            co_cfg = full_cfg.get("coordinator")
            if not isinstance(co_cfg, dict) or "port" not in co_cfg:
                logger.error(
                    "❌ 缺少 coordinator.port（benchmark_config.py 必须提供端口）"
                )
                self.server_start_error = "missing coordinator.port"
            else:
                try:
                    self.port = int(co_cfg.get("port"))
                except Exception:
                    logger.error("❌ coordinator.port 必须为整数")
                    self.server_start_error = "invalid coordinator.port"
                # Force listen on 0.0.0.0
                self.host = "0.0.0.0"
                logger.info(f"🖧 协调者监听地址设置: {self.host}:{self.port}")

            # Create a temporary test_config to store kvcache_config (no fallback).
            if not self.test_config:
                self.test_config = TestConfig(
                    test_id="temp",
                    threads_per_process=THREADS_PER_PROCESS,
                    value_size_mode=VALUE_SIZE_MODE,
                    value_size=VALUE_SIZE,
                    value_size_weighted_set=copy.deepcopy(VALUE_SIZE_WEIGHTED_SET),
                    test_mode=CURR_TEST_MODE,
                    max_benchmark_seconds=MAX_BENCHMARK_SECONDS,
                    cluster_ready_timeout_seconds=CLUSTER_READY_TIMEOUT_SECONDS,
                    op_timeout_seconds=OP_TIMEOUT_SECONDS,
                    metric_warmup_seconds=METRIC_WARMUP_SECONDS,
                    start_idle_seconds=START_IDLE_SECONDS,
                    kvcache_config=kvcache_config,
                )
            else:
                self.test_config.kvcache_config = kvcache_config

            logger.info("✅ 加载KVCache配置成功（Python配置）")
            logger.debug(f"📋 KVCache配置根键: {list(kvcache_config.keys())}")
            if isinstance(ovr, list):
                logger.info("🔧 检测到 node_overrides 列表（按 kv.instance_key 应用 KV/MQ 覆盖）")
                # Expected node count equals override list length (can be 0).
                self.expected_nodes = len(self.per_node_patches)
                logger.info(
                    f"🎯 期望节点数设置为 node_overrides 长度: {self.expected_nodes}"
                )
                # Build instance_key -> KV patch / MQ patch maps; require kv.instance_key to be present.
                self.instance_patch_map = {}
                self.instance_mq_map = {}
                self.instance_role_map = {}
                self.instance_key_prefix_map = {}
                self.instance_mpmc_producer_runtime_mode_map = {}
                self.instance_mpmc_producer_prefeed_leader_map = {}
                self.expected_mpmc_consumer_workers = 0
                self.instance_affinity_slot_map = {}
                self.instance_network_sample_map = {}
                missing_keys = []
                instance_keys_in_order: List[str] = []
                for idx, patch in enumerate(self.per_node_patches):
                    kv_section = patch.get("kv") if isinstance(patch.get("kv"), dict) else patch
                    ik = kv_section.get("instance_key") if isinstance(kv_section, dict) else None
                    if not isinstance(ik, str) or not ik.strip():
                        missing_keys.append(idx)
                        continue
                    instance_key = ik.strip()
                    instance_keys_in_order.append(instance_key)

                    # KV patch: store only the kv section; later deep-merge with kv_base.
                    self.instance_patch_map[instance_key] = kv_section

                    # MQ per-node config: mq_role + mq (overrides mq_base)
                    mq_cfg: Dict[str, Any] = {}
                    role = patch.get("mq_role")
                    if isinstance(role, str) and role.strip():
                        mq_cfg["role"] = role.strip()
                    mq_section = patch.get("mq") if isinstance(patch.get("mq"), dict) else None
                    if mq_section is None:
                        mq_section = {}

                    # Extract weight separately; keep the rest as a patch over mq_base.
                    # No defaults: if mq section is provided, weight must be explicit.
                    if mq_section or "role" in mq_cfg:
                        if "weight" not in mq_section:
                            raise ValueError(f"mq.weight must be explicitly set when mq/mq_role is provided (instance_key={instance_key})")
                        weight_raw = mq_section.get("weight")
                        try:
                            mq_cfg["weight"] = float(weight_raw)
                        except Exception as exc:
                            raise ValueError(
                                f"mq.weight 配置非法 (instance_key={instance_key}, value={weight_raw})"
                            ) from exc
                        mq_cfg["patch"] = {k: v for k, v in mq_section.items() if k != "weight"}

                    if mq_cfg:
                        self.instance_mq_map[instance_key] = mq_cfg

                    network_sample_cfg = patch.get("network_sample")
                    if isinstance(network_sample_cfg, dict):
                        self.instance_network_sample_map[instance_key] = copy.deepcopy(network_sample_cfg)

                if missing_keys:
                    self.server_start_error = (
                        f"node_overrides 缺少 kv.instance_key: 索引 {missing_keys}"
                    )
                    logger.error(f"❌ {self.server_start_error}")
                else:
                    if len(NODE_ROLES) != len(instance_keys_in_order):
                        self.server_start_error = (
                            "benchmark.node_roles length must match node_overrides length: "
                            f"roles={len(NODE_ROLES)} node_overrides={len(instance_keys_in_order)}"
                        )
                        logger.error(f"❌ {self.server_start_error}")
                    else:
                        self.instance_role_map = {
                            instance_keys_in_order[i]: NODE_ROLES[i] for i in range(len(instance_keys_in_order))
                        }
                        # KV benchmarks share one scene keyspace across all benchmark members.
                        # Affinity chooses which portion of that shared keyspace a member prefers;
                        # it must not create separate seed/worker namespaces.
                        counters: Dict[str, int] = {
                            KV_NODE_ROLE_SEED: 0,
                            KV_NODE_ROLE_WORKER: 0,
                            "producer": 0,
                            "consumer": 0,
                        }
                        producer_runtime_mode_counts: Dict[str, int] = {
                            MPMC_PRODUCER_RUNTIME_MODE_ACTIVE: 0,
                            MPMC_PRODUCER_RUNTIME_MODE_LOGICAL_ONLY: 0,
                        }
                        for i, ik0 in enumerate(instance_keys_in_order):
                            role0 = NODE_ROLES[i]
                            ord0 = counters.get(role0, 0)
                            counters[role0] = ord0 + 1
                            if role0 in (KV_NODE_ROLE_SEED, KV_NODE_ROLE_WORKER):
                                kp = "benchmark_kv"
                            elif role0 in ("producer", "consumer"):
                                kp = f"benchmark_{role0}_{ord0}"
                                if role0 == "producer":
                                    runtime_mode = MPMC_PRODUCER_RUNTIME_MODE_ACTIVE
                                    if (
                                        MPMC_ACTIVE_PRODUCER_RUNTIME_LIMIT is not None
                                        and ord0 >= MPMC_ACTIVE_PRODUCER_RUNTIME_LIMIT
                                    ):
                                        runtime_mode = MPMC_PRODUCER_RUNTIME_MODE_LOGICAL_ONLY
                                    self.instance_mpmc_producer_runtime_mode_map[ik0] = runtime_mode
                                    producer_runtime_mode_counts[runtime_mode] += 1
                            else:
                                raise ValueError(f"unexpected node role: {role0}")
                            self.instance_key_prefix_map[ik0] = kp
                            self.instance_affinity_slot_map[ik0] = i
                        if CURR_TEST_MODE == TestMode.MPMC.value:
                            self.expected_mpmc_consumer_workers = (
                                counters["consumer"] * THREADS_PER_PROCESS
                            )
                            active_producer_keys = [
                                instance_key
                                for instance_key in instance_keys_in_order
                                if self.instance_role_map[instance_key] == "producer"
                                and self.instance_mpmc_producer_runtime_mode_map.get(instance_key)
                                == MPMC_PRODUCER_RUNTIME_MODE_ACTIVE
                            ]
                            if self.expected_mpmc_consumer_workers <= 0:
                                raise ValueError(
                                    "MPMC benchmark requires at least one consumer worker"
                                )
                            if not active_producer_keys:
                                raise ValueError(
                                    "MPMC benchmark requires at least one active producer"
                                )
                            leader_key = active_producer_keys[0]
                            self.instance_mpmc_producer_prefeed_leader_map = {
                                instance_key: instance_key == leader_key
                                for instance_key in instance_keys_in_order
                                if self.instance_role_map[instance_key] == "producer"
                            }
                            logger.info(
                                "🔧 MPMC producer runtime modes: active=%s logical_only=%s "
                                "limit=%s prefeed_leader=%s expected_consumer_workers=%s",
                                producer_runtime_mode_counts[MPMC_PRODUCER_RUNTIME_MODE_ACTIVE],
                                producer_runtime_mode_counts[MPMC_PRODUCER_RUNTIME_MODE_LOGICAL_ONLY],
                                MPMC_ACTIVE_PRODUCER_RUNTIME_LIMIT,
                                leader_key,
                                self.expected_mpmc_consumer_workers,
                            )

        except Exception as e:
            # By rule: if config is invalid, print the error; do not fallback/recover.
            self.server_start_error = f"invalid benchmark configuration: {e}"
            logger.error(f"❌ 读取或解析KVCache配置失败: {e}")

    def start(self):
        """Start the coordinator server."""
        if not self.test_config or not self.test_config.kvcache_config:
            logger.error("❌ 无法启动：缺少有效的KVCache配置，退出")
            return
        if getattr(self, "server_start_error", None):
            logger.error(f"❌ 无法启动：配置错误 -> {self.server_start_error}")
            # Unblock waiters to avoid hanging.
            self.all_nodes_ready.set()
            self.all_results_received.set()
            return
        if self.expected_nodes <= 0:
            logger.error(
                "❌ 无法启动：期望节点数为 0（请在 benchmark_config.py 中提供非空 node_overrides 列表）"
            )
            # Signal to the caller not to wait.
            self.all_nodes_ready.set()
            self.all_results_received.set()
            # Record error so main() can exit.
            self.server_start_error = "expected_nodes == 0"
            return

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            server_socket.bind((self.host, self.port))
            server_socket.listen(self.expected_nodes + 5)
        except Exception as e:
            logger.error(f"❌ 协调者服务器启动失败（端口占用或权限问题）: {e}")
            # Record error and unblock waiters to avoid hanging.
            self.server_start_error = str(e)
            self.all_nodes_ready.set()
            self.all_results_received.set()
            server_socket.close()
            return

        logger.info(f"🚀 协调者服务器启动成功 {self.host}:{self.port}")
        logger.info(f"📊 等待 {self.expected_nodes} 个节点连接...")

        try:
            # 不以 all_results_received 作为退出条件，以便支持多轮测试。
            while True:
                client_socket, client_address = server_socket.accept()
                # Connection open/close does not affect results; keep at DEBUG for optional diagnostics.
                logger.debug(f"🔗 接受来自 {client_address} 的连接")
                thread = threading.Thread(
                    target=self.wait_for_one_request, args=(client_socket,), daemon=True
                )
                thread.start()
        except KeyboardInterrupt:
            logger.info("⚠️ 接收到中断信号，协调者服务器关闭中...")
        finally:
            server_socket.close()

    def _receive_tcp_message(self, client_socket: socket.socket) -> Optional[Dict]:
        """Receive a TCP message."""
        try:
            # Receive message length (4 bytes)
            length_data = client_socket.recv(4)
            if len(length_data) != 4:
                logger.warning("❌ 接收消息长度失败")
                return None

            message_length = struct.unpack("!I", length_data)[0]
            logger.debug(f"📥 等待接收消息 (长度: {message_length} bytes)")

            # Receive message body
            message_data = b""
            while len(message_data) < message_length:
                chunk = client_socket.recv(
                    min(message_length - len(message_data), 4096)
                )
                if not chunk:
                    logger.warning("❌ 接收消息内容时连接断开")
                    return None
                message_data += chunk

            # Parse JSON message
            message_json = message_data.decode("utf-8")
            message = json.loads(message_json)

            logger.debug(f"📨 收到消息: {message}")
            return message

        except socket.timeout:
            logger.debug("⏱️ 接收消息超时")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON解析失败: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ 接收消息异常: {e}")
            return None

    def _send_tcp_response(self, client_socket: socket.socket, response: Dict) -> bool:
        """Send a TCP response."""
        try:
            response_json = json.dumps(response, ensure_ascii=False)
            response_bytes = response_json.encode("utf-8")

            # Send response length (4 bytes) + body
            response_length = len(response_bytes)
            length_header = struct.pack("!I", response_length)

            logger.debug(f"📤 发送响应 (长度: {response_length} bytes): {response}")
            client_socket.sendall(length_header + response_bytes)
            return True

        except Exception as e:
            logger.error(f"❌ 发送响应失败: {e}")
            return False

    def wait_for_one_request(self, client_socket: socket.socket):
        """Handle a single client connection."""
        node_id = None
        try:

            # Connection-layer events are quiet by default to reduce noise; raise to DEBUG when needed.
            logger.debug(f"📥 等待来自客户端 的消息…")
            # Use a longer timeout to maintain long-lived connections during the benchmark.
            client_socket.settimeout(180.0)  # 3-minute timeout; should be enough for the benchmark run

            message = self._receive_tcp_message(client_socket)
            if message is None:
                logger.debug("📭 客户端断开或无消息")
                return
            node_id = message.get("node_id")
            msg_type = message.get("type")
            logger.debug(f"📨 处理消息类型: {msg_type} from {node_id}")

            if msg_type == "register":
                node_id = message.get("node_id")
                logger.debug(f"📝 处理注册消息: {node_id}")
                success = self.handle_register(message, client_socket)
                if not success:
                    logger.warning("向对方返回注册结果失败")

            elif msg_type == "ready":
                if not node_id:
                    node_id = message.get("node_id")
                logger.debug(f"✅ 处理就绪消息: {node_id}")
                if not self.handle_ready(message, client_socket):
                    logger.warning(f"❌ 向节点 {node_id} 发送就绪响应失败")
            elif msg_type == "start":
                self.handle_start_request(message, client_socket)
            elif msg_type == "runtime_ready":
                if not node_id:
                    node_id = message.get("node_id")
                logger.debug(f"✅ 处理 runtime_ready 消息: {node_id}")
                if not self.handle_runtime_ready(message, client_socket):
                    logger.warning(f"❌ 向节点 {node_id} 发送 runtime_ready 响应失败")
            elif msg_type == "runtime_start":
                if not node_id:
                    node_id = message.get("node_id")
                logger.debug(f"▶️ 处理 runtime_start 消息: {node_id}")
                if not self.handle_runtime_start_request(message, client_socket):
                    logger.warning(f"❌ 向节点 {node_id} 发送 runtime_start 响应失败")
            elif msg_type == "result":
                if not node_id:
                    node_id = message.get("node_id")
                logger.debug(f"📊 处理结果上报消息: {node_id}")
                success = self.handle_report_results(message, client_socket)
                if success:
                    logger.info(f"✅ 节点 {node_id} 已成功上报结果")
                else:
                    logger.warning(f"❌ 节点 {node_id} 结果上报处理失败")
                # Close the connection after results are reported (no explicit log needed).
                logger.debug(f"🔚 节点 {node_id} 测试完成，关闭长连接")
            elif msg_type == "round_status":
                self.handle_round_status_request(message, client_socket)
            else:
                logger.warning(f"⚠️ 未知消息类型 from {node_id}: {msg_type}")
                response = {
                    "status": "error",
                    "error": f"Unknown message type: {msg_type}",
                }
                if not self._send_tcp_response(client_socket, response):
                    logger.warning(f"❌ 向节点 {node_id} 发送错误响应失败")

        except socket.timeout:
            # Connection timeout is expected; keep at DEBUG instead of WARNING to reduce noise.
            logger.debug(f"⏱️ 客户端 {node_id} 连接超时 (3分钟无活动)")
        except (ConnectionResetError, BrokenPipeError):
            # Disconnects are normal; do not warn.
            logger.debug(f"🔌 节点 {node_id} 连接断开")
        except Exception as e:
            logger.error(f"❌ 处理客户端 {node_id} 时发生异常: {e}", exc_info=True)
        finally:
            # 仅关闭本次 socket 连接，不清理已注册的节点信息，以便支持多轮测试复用同一批节点。
            try:
                client_socket.close()
                logger.debug(f"🔒 关闭节点 {node_id} 的socket连接")
            except Exception:
                pass

    def _log_start_waiting_warning(self, *, node_id: Any) -> None:
        now = time.monotonic()
        with self._start_waiting_warning_lock:
            if now < self._start_waiting_warning_next_log_ts:
                self._start_waiting_warning_suppressed_count += 1
                return

            suppressed_count = self._start_waiting_warning_suppressed_count
            self._start_waiting_warning_suppressed_count = 0
            self._start_waiting_warning_next_log_ts = (
                now + START_WAITING_WARNING_LOG_INTERVAL_SECONDS
            )

        if suppressed_count > 0:
            logger.warning(
                "⚠️ 节点 %s 发送请求开始测试，但尚未所有节点就绪; suppressed_waiting_warnings=%s",
                node_id,
                suppressed_count,
            )
        else:
            logger.warning(f"⚠️ 节点 {node_id} 发送请求开始测试，但尚未所有节点就绪")

    def handle_start_request(self, message: Dict, client_socket: socket.socket):
        node_id = message.get("node_id")
        if not self.test_config:
            logger.error("❌ 收到开始测试请求，但当前没有激活的 TestConfig")
            response = {
                "status": "error",
                "error": "No active test_config in coordinator",
            }
            if not self._send_tcp_response(client_socket, response):
                logger.warning(f"❌ 向节点 {node_id} 发送 无测试配置 响应失败")
            return

        if not self.all_nodes_ready.is_set():
            self._log_start_waiting_warning(node_id=node_id)
            # Reply directly
            response = {
                "status": "waiting",  # Keep waiting
                "error": "Not all nodes are ready",  # Some nodes are not ready yet
            }
            if not self._send_tcp_response(client_socket, response):
                logger.warning(f"❌ 向节点 {node_id} 发送 继续等待")
        else:
            logger.info(
                f"🎯 节点 {node_id} 请求开始测试，全员已就绪，可以开始第 "
                f"{self.current_round_index + 1}/{self.total_rounds} 轮测试"
            )
            overrides = {
                "test_id": self.test_config.test_id,
                "threads_per_process": self.test_config.threads_per_process,
                "max_benchmark_seconds": self.test_config.max_benchmark_seconds,
                "op_timeout_seconds": self.test_config.op_timeout_seconds,
                "start_idle_seconds": self.test_config.start_idle_seconds,
                "value_size_mode": self.test_config.value_size_mode,
                "value_size": self.test_config.value_size,
                "value_size_weighted_set": copy.deepcopy(self.test_config.value_size_weighted_set),
                "test_mode": self.test_config.test_mode,
            }
            response = {
                "status": "success",  # 可以开始
                "message": "Test can start",  # 所有节点已就绪，可以开始测试
                "config_overrides": overrides,
                "has_more_tests": self.has_more_tests,
            }
            if not self._send_tcp_response(client_socket, response):
                logger.warning(f"❌ 向节点 {node_id} 发送开始测试响应失败")

    def handle_runtime_ready(self, message: Dict, client_socket: socket.socket) -> bool:
        """Handle post-START runtime-ready state for MPMC benchmarks."""
        node_id = message.get("node_id")
        if not isinstance(node_id, str) or not node_id.strip():
            return self._send_tcp_response(
                client_socket,
                {"status": "error", "error": "runtime_ready missing node_id"},
            )
        with self.lock:
            if node_id not in self.registered_nodes:
                logger.warning("⚠️ 未注册的节点报告 runtime_ready: %s", node_id)
                return self._send_tcp_response(
                    client_socket,
                    {"status": "error", "error": f"unregistered node: {node_id}"},
                )
            self.runtime_ready_nodes_for_current_test.add(node_id)
            ready_count = len(self.runtime_ready_nodes_for_current_test)
            ready_node_ids = sorted(self.runtime_ready_nodes_for_current_test)
            all_runtime_ready = ready_count >= self.expected_nodes
            if all_runtime_ready:
                self.all_runtime_ready.set()
            logger.info(
                "✅ MPMC runtime ready: node_id=%s runtime_ready=%s/%s",
                node_id,
                ready_count,
                self.expected_nodes,
            )
        status = "success" if all_runtime_ready else "waiting"
        return self._send_tcp_response(
            client_socket,
            {
                "status": status,
                "runtime_ready_count": ready_count,
                "runtime_ready_node_ids": ready_node_ids,
                "expected_nodes": int(self.expected_nodes),
            },
        )

    def handle_runtime_start_request(self, message: Dict, client_socket: socket.socket) -> bool:
        """Allow MPMC nodes to start the timed window only after all runtimes are ready."""
        node_id = message.get("node_id")
        if not isinstance(node_id, str) or not node_id.strip():
            return self._send_tcp_response(
                client_socket,
                {"status": "error", "error": "runtime_start missing node_id"},
            )
        with self.lock:
            if node_id not in self.registered_nodes:
                logger.warning("⚠️ 未注册的节点请求 runtime_start: %s", node_id)
                return self._send_tcp_response(
                    client_socket,
                    {"status": "error", "error": f"unregistered node: {node_id}"},
                )
            ready_count = len(self.runtime_ready_nodes_for_current_test)
            ready_node_ids = sorted(self.runtime_ready_nodes_for_current_test)
            all_runtime_ready = ready_count >= self.expected_nodes
        status = "success" if all_runtime_ready else "waiting"
        if status == "success":
            logger.info(
                "🎯 MPMC runtime start released: node_id=%s runtime_ready=%s/%s",
                node_id,
                ready_count,
                self.expected_nodes,
            )
        return self._send_tcp_response(
            client_socket,
            {
                "status": status,
                "runtime_ready_count": ready_count,
                "runtime_ready_node_ids": ready_node_ids,
                "expected_nodes": int(self.expected_nodes),
            },
        )

    @staticmethod
    def _deep_merge(dst: dict, patch: dict) -> dict:
        """Deep-merge dicts in-place and return dst. Lists are replaced (override strategy)."""
        for k, v in (patch or {}).items():
            if isinstance(v, dict) and isinstance(dst.get(k), dict):
                CoordinatorServer._deep_merge(dst[k], v)
            else:
                dst[k] = copy.deepcopy(v)
        return dst

    def _select_kvcache_patch(self, instance_key: str) -> dict:
        """Select KV patch by instance_key (matches node_overrides[*].kv.instance_key)."""
        if not isinstance(instance_key, str):
            return {}
        patch = self.instance_patch_map.get(instance_key)
        return copy.deepcopy(patch or {})

    def _select_mq_config(self, instance_key: str) -> Dict[str, Any]:
        """Select MQ config by instance_key (role/weight/patch); return empty dict when not configured."""
        if not isinstance(instance_key, str):
            return {}
        cfg = self.instance_mq_map.get(instance_key) or {}
        return copy.deepcopy(cfg)

    def _find_registered_node_id_by_instance_key(self, instance_key: str) -> Optional[str]:
        """Return the canonical logical node_id already bound to the instance_key."""
        for registered_node_id, registered_meta in self.registered_nodes.items():
            if registered_meta.get("instance_key") == instance_key:
                return registered_node_id
        return None

    def handle_register(self, message: Dict, client_socket: socket.socket) -> bool:
        """Handle node registration, assign config and send it back.

        Seed/worker nodes are assigned a key_prefix one-by-one.
        """
        node_id = message["node_id"]
        if self.test_config is None:
            logger.error(f"❌ 节点 {node_id} 注册时 coordinator 尚未加载 test_config")
            response = {"status": "error", "error": "coordinator test_config is not ready"}
            self._send_tcp_response(client_socket, response)
            return False
        active_test_mode = self.test_config.test_mode
        instance_key = message.get("instance_key")
        if not isinstance(instance_key, str) or not instance_key.strip():
            logger.error(f"❌ 节点 {node_id} 注册缺少 instance_key")
            response = {"status": "error", "error": "missing instance_key"}
            self._send_tcp_response(client_socket, response)
            return False
        node_type = message["node_type"]
        response_node_id = node_id
        previous_registration: Optional[Tuple[str, Dict[str, Any], Optional[NodeConfig], List[Any]]] = None

        with self.lock:
            existing_node_id = self._find_registered_node_id_by_instance_key(instance_key)
            if existing_node_id is None:
                if len(self.registered_nodes) >= self.expected_nodes:
                    logger.warning(
                        f"⚠️ 节点数量已达上限 ({self.expected_nodes})，拒绝注册 {node_id}"
                    )
                    response = {
                        "status": "error",
                        "error": f"Maximum nodes ({self.expected_nodes}) already registered",
                    }
                    self._send_tcp_response(client_socket, response)
                    return False
            else:
                response_node_id = existing_node_id
                previous_registration = (
                    existing_node_id,
                    copy.deepcopy(self.registered_nodes[existing_node_id]),
                    copy.deepcopy(self.node_configs.get(existing_node_id)),
                    copy.deepcopy(self.node_messages.get(existing_node_id, [])),
                )
                logger.warning(
                    "🔄 检测到节点重连: "
                    f"instance_key={instance_key} "
                    f"old_node_id={existing_node_id} "
                    f"new_node_id={node_id}"
                )

            # Assign role by instance_key (no polling / no registration-order dependency).
            assigned_role = self.instance_role_map.get(instance_key)
            if not isinstance(assigned_role, str) or not assigned_role.strip():
                logger.error(f"❌ 未找到与 instance_key 匹配的 node_role: {instance_key}")
                response = {
                    "status": "error",
                    "error": f"no node_role for instance_key: {instance_key}",
                }
                self._send_tcp_response(client_socket, response)
                return False
            assigned_role = assigned_role.strip()

            mq_cfg = self._select_mq_config(instance_key)
            mq_role: Optional[str] = None
            mq_weight: Optional[float] = None
            mq_patch: Dict[str, Any] = {}
            if isinstance(mq_cfg, dict) and mq_cfg:
                mq_role_val = mq_cfg.get("role")
                if isinstance(mq_role_val, str) and mq_role_val.strip():
                    mq_role = mq_role_val.strip()
                weight_raw = mq_cfg.get("weight")
                if weight_raw is not None:
                    try:
                        mq_weight = float(weight_raw)
                    except Exception as exc:  # noqa: BLE001
                        raise ValueError(
                            f"mq.weight 配置非法 (instance_key={instance_key}, value={weight_raw})"
                        ) from exc
                patch_part = mq_cfg.get("patch")
                if isinstance(patch_part, dict):
                    mq_patch = copy.deepcopy(patch_part)

            if active_test_mode == TestMode.MPMC.value:
                if assigned_role not in {"producer", "consumer"}:
                    raise ValueError(f"invalid node_role for MPMC: {assigned_role!r}")
                if mq_role is not None and mq_role != assigned_role:
                    raise ValueError(
                        f"mq_role mismatch for instance_key={instance_key}: role_list={assigned_role} mq_role={mq_role}"
                    )
                if mq_weight is None:
                    raise ValueError(f"mq.weight must be set for MPMC (instance_key={instance_key})")
                if not isinstance(self.mq_unique_id, str) or not self.mq_unique_id:
                    raise ValueError("mq_new_or_bind_unique_key must be set for MPMC")

            # Base KV config
            base_config = self.test_config.kvcache_config if self.test_config else {}
            node_kvcache_config = copy.deepcopy(base_config or {})

            # Apply patch by instance_key
            k_patch = self._select_kvcache_patch(instance_key)
            if k_patch:
                logger.debug(f"🧩 对 instance_key[{instance_key}] 应用覆盖: {k_patch}")
                node_kvcache_config = self._deep_merge(node_kvcache_config, k_patch)
            else:
                logger.error(f"❌ 未找到与 instance_key 匹配的覆盖项: {instance_key}")
                response = {
                    "status": "error",
                    "error": f"no override for instance_key: {instance_key}",
                }
                self._send_tcp_response(client_socket, response)
                return False

            key_prefix = self.instance_key_prefix_map.get(instance_key)
            if not isinstance(key_prefix, str) or not key_prefix.strip():
                raise ValueError(f"missing key_prefix mapping for instance_key={instance_key}")
            affinity_slot_index = self.instance_affinity_slot_map.get(instance_key)
            if affinity_slot_index is not None:
                affinity_slot_index = int(affinity_slot_index)

            mq_role_effective: Optional[str] = None
            mq_weight_effective: float = 1.0
            mq_final_cfg: Optional[Dict[str, Any]] = None
            mpmc_producer_runtime_mode: Optional[str] = None
            expected_mpmc_consumer_workers: Optional[int] = None
            mpmc_producer_prefeed_leader: Optional[bool] = None
            if active_test_mode == TestMode.MPMC.value:
                mq_role_effective = mq_role
                mq_weight_effective = float(mq_weight)
                mq_final_cfg = copy.deepcopy(self.mq_config)
                if mq_patch:
                    mq_final_cfg = self._deep_merge(mq_final_cfg, mq_patch)
                mq_final_cfg["role"] = mq_role_effective
                mq_final_cfg["weight"] = mq_weight_effective
                if assigned_role == "producer":
                    mpmc_producer_runtime_mode = (
                        self.instance_mpmc_producer_runtime_mode_map.get(
                            instance_key,
                            MPMC_PRODUCER_RUNTIME_MODE_ACTIVE,
                        )
                    )
                    mpmc_producer_prefeed_leader = (
                        self.instance_mpmc_producer_prefeed_leader_map.get(instance_key)
                    )
                    if not isinstance(mpmc_producer_prefeed_leader, bool):
                        raise ValueError(
                            "missing explicit MPMC producer prefeed leader assignment: "
                            f"instance_key={instance_key}"
                        )
                expected_mpmc_consumer_workers = self.expected_mpmc_consumer_workers
                if expected_mpmc_consumer_workers <= 0:
                    raise ValueError(
                        "expected_mpmc_consumer_workers must be positive for MPMC"
                    )

            node_config = NodeConfig(
                node_id=response_node_id,
                test_mode=active_test_mode,
                node_role=assigned_role,
                threads_per_process=THREADS_PER_PROCESS,
                max_benchmark_seconds=MAX_BENCHMARK_SECONDS,
                cluster_ready_timeout_seconds=CLUSTER_READY_TIMEOUT_SECONDS,
                op_timeout_seconds=OP_TIMEOUT_SECONDS,
                metric_warmup_seconds=METRIC_WARMUP_SECONDS,
                start_idle_seconds=START_IDLE_SECONDS,
                value_size_mode=VALUE_SIZE_MODE,
                value_size=VALUE_SIZE,
                value_size_weighted_set=copy.deepcopy(VALUE_SIZE_WEIGHTED_SET),
                kvcache_config=node_kvcache_config,
                key_prefix=key_prefix,
                affinity_slot_index=affinity_slot_index,
                mq_role=mq_role_effective,
                mq_weight=mq_weight_effective,
                mq_config=mq_final_cfg,
                mq_unique_id=self.mq_unique_id,
                mpmc_producer_runtime_mode=mpmc_producer_runtime_mode,
                expected_mpmc_consumer_workers=expected_mpmc_consumer_workers,
                mpmc_producer_prefeed_leader=mpmc_producer_prefeed_leader,
                consumer_sim_handle_ms_range=CONSUMER_SIM_HANDLE_MS_RANGE,
                network_sample=copy.deepcopy(self.instance_network_sample_map.get(instance_key)),
                prometheus_base_url=self.prometheus_base_url,
                otlp_log_api=copy.deepcopy(self.otlp_log_api),
            )

            # Register node. Reconnects reuse the original logical node_id slot so the
            # coordinator can recover from supervisor restarts without consuming extra slots.
            previous_status = "registered"
            previous_ever_ready = False
            previous_registered_at: Optional[str] = None
            if previous_registration is not None:
                previous_status_raw = previous_registration[1].get("status")
                if isinstance(previous_status_raw, str) and previous_status_raw.strip():
                    previous_status = previous_status_raw
                previous_ever_ready = bool(previous_registration[1].get("ever_ready"))
                previous_registered_at_raw = previous_registration[1].get("registered_at")
                if isinstance(previous_registered_at_raw, str) and previous_registered_at_raw.strip():
                    previous_registered_at = previous_registered_at_raw

            self.registered_nodes[response_node_id] = {
                "node_type": node_type,
                "node_role": assigned_role,
                "status": previous_status,
                "ever_ready": previous_ever_ready,
                "instance_key": instance_key,
                "registered_at": previous_registered_at or datetime.now().isoformat(),
                "reconnected_at": datetime.now().isoformat() if previous_registration is not None else None,
            }
            self.node_configs[response_node_id] = node_config
            self.node_messages[response_node_id] = []

            logger.info(
                f"✅ 节点注册成功: {response_node_id} ({node_type}) -> 角色: {assigned_role}"
            )

        # Send config response
        response_config = {
            "node_id": response_node_id,
            "node_role": assigned_role,
            "test_mode": active_test_mode,
            "expected_nodes": int(self.expected_nodes),
            "threads_per_process": THREADS_PER_PROCESS,
            "max_benchmark_seconds": MAX_BENCHMARK_SECONDS,
            "cluster_ready_timeout_seconds": CLUSTER_READY_TIMEOUT_SECONDS,
            "op_timeout_seconds": node_config.op_timeout_seconds,
            "metric_warmup_seconds": METRIC_WARMUP_SECONDS,
            "start_idle_seconds": node_config.start_idle_seconds,
            "value_size_mode": VALUE_SIZE_MODE,
            "value_size": VALUE_SIZE,
            "value_size_weighted_set": copy.deepcopy(VALUE_SIZE_WEIGHTED_SET),
            "kvcache_config": node_kvcache_config,
            "key_prefix": key_prefix,
            "affinity_slot_index": node_config.affinity_slot_index,
            "consumer_sim_handle_ms_range": node_config.consumer_sim_handle_ms_range,
        }
        if node_config.network_sample is not None:
            response_config["network_sample"] = copy.deepcopy(node_config.network_sample)
        if node_config.prometheus_base_url is not None:
            response_config["prometheus_base_url"] = str(node_config.prometheus_base_url)
        if node_config.otlp_log_api is not None:
            response_config["otlp_log_api"] = copy.deepcopy(node_config.otlp_log_api)
        if node_config.mq_config is not None:
            response_config["mq"] = node_config.mq_config
        if node_config.mq_unique_id is not None:
            response_config["mq_new_or_bind_unique_key"] = node_config.mq_unique_id
        if node_config.mpmc_producer_runtime_mode is not None:
            response_config["mpmc_producer_runtime_mode"] = node_config.mpmc_producer_runtime_mode
        if node_config.expected_mpmc_consumer_workers is not None:
            response_config["expected_mpmc_consumer_workers"] = (
                node_config.expected_mpmc_consumer_workers
            )
        if node_config.mpmc_producer_prefeed_leader is not None:
            response_config["mpmc_producer_prefeed_leader"] = (
                node_config.mpmc_producer_prefeed_leader
            )
        benchmark_cfg = _load_benchmark_section(KVCACHE_CONFIG_PATH)
        response_config = merge_kv_benchmark_extras(response_config, benchmark_cfg)
        response_config = merge_rpc_benchmark_extras(response_config, benchmark_cfg)
        response_config = merge_fs_benchmark_extras(response_config, benchmark_cfg)

        response = {
            "status": "success",
            "config": response_config,
        }

        success = self._send_tcp_response(client_socket, response)
        if success:
            logger.info(f"📤 配置发送成功到节点 {response_node_id}")
        else:
            logger.error(f"❌ 配置发送失败到节点 {response_node_id}")
            with self.lock:
                if previous_registration is None:
                    self.registered_nodes.pop(response_node_id, None)
                    self.node_configs.pop(response_node_id, None)
                    self.node_messages.pop(response_node_id, None)
                else:
                    prev_node_id, prev_meta, prev_config, prev_messages = previous_registration
                    self.registered_nodes[prev_node_id] = prev_meta
                    if prev_config is not None:
                        self.node_configs[prev_node_id] = prev_config
                    else:
                        self.node_configs.pop(prev_node_id, None)
                    self.node_messages[prev_node_id] = prev_messages

        return success

    def handle_ready(self, message: Dict, client_socket: socket.socket) -> bool:
        """Handle node ready state."""
        node_id = message["node_id"]

        with self.lock:
            if node_id in self.registered_nodes:
                self.registered_nodes[node_id]["status"] = "ready"
                self.registered_nodes[node_id]["ever_ready"] = True
                self.ready_nodes_for_current_test.add(str(node_id))
                logger.info(f"✅ 节点就绪: {node_id}")

                # Check whether all nodes are ready.
                ready_nodes = len(self.ready_nodes_for_current_test)
                logger.debug(f"📊 就绪状态: {ready_nodes}/{self.expected_nodes} 个节点")

                if ready_nodes >= self.expected_nodes:
                    if not self.all_nodes_ready.is_set():
                        logger.info("🎯 所有节点已就绪，准备广播开始指令")
                        self.all_nodes_ready.set()
            else:
                logger.warning(f"⚠️ 未注册的节点报告就绪: {node_id}")

        response = {"status": "acknowledged"}  # Ack
        return self._send_tcp_response(client_socket, response)

    def handle_report_results(
        self, message: Dict, client_socket: socket.socket
    ) -> bool:
        """Handle result reports."""
        node_id = message.get("node_id")
        results_data = message.get("results", {})

        try:
            if "p50_latency_us" not in results_data:
                raise ValueError("Missing required result field: p50_latency_us")
            if "inflight_max" not in results_data:
                raise ValueError("Missing required result field: inflight_max")
            if "inflight_avg" not in results_data:
                raise ValueError("Missing required result field: inflight_avg")

            # Create NodeMetrics from results_data
            metrics = NodeMetrics(
                test_id=self.test_config.test_id if self.test_config else "unknown",
                node_id=results_data.get("node_id", node_id),
                node_role=results_data.get("node_role", "unknown"),
                total_operations=results_data.get("total_operations", 0),
                successful_operations=results_data.get("successful_operations", 0),
                failed_operations=results_data.get("failed_operations", 0),
                get_total_operations=results_data.get("get_total_operations", 0),
                get_hit_operations=results_data.get("get_hit_operations", 0),
                get_miss_operations=results_data.get("get_miss_operations", 0),
                get_error_operations=results_data.get("get_error_operations", 0),
                avg_latency_us=results_data.get("avg_latency_us", 0.0),
                p50_latency_us=results_data.get("p50_latency_us", 0.0),
                p99_latency_us=results_data.get("p99_latency_us", 0.0),
                p95_latency_us=results_data.get("p95_latency_us", 0.0),
                throughput_ops_per_sec=results_data.get("throughput_ops_per_sec", 0.0),
                total_throughput_ops_per_sec=results_data.get("total_throughput_ops_per_sec", 0.0),
                get_total_throughput_ops_per_sec=results_data.get("get_total_throughput_ops_per_sec", 0.0),
                get_hit_throughput_ops_per_sec=results_data.get("get_hit_throughput_ops_per_sec", 0.0),
                get_miss_throughput_ops_per_sec=results_data.get("get_miss_throughput_ops_per_sec", 0.0),
                total_bytes_processed=results_data.get("total_bytes_processed", 0),
                total_duration_seconds=results_data.get("total_duration_seconds", 0.0),
                error_details=results_data.get("error_details", {}),
                top_slowest_operations=results_data.get("top_slowest_operations", []),
                inflight_max=int(results_data["inflight_max"]),
                inflight_avg=float(results_data["inflight_avg"]),
                observed_value_size_histogram=results_data.get("observed_value_size_histogram", {}),
                observed_value_size_avg=float(results_data.get("observed_value_size_avg", 0.0)),
                observed_value_size_min=int(results_data.get("observed_value_size_min", 0)),
                observed_value_size_max=int(results_data.get("observed_value_size_max", 0)),
                fluxon_phase_summary=copy.deepcopy(results_data.get("fluxon_phase_summary", {})),
                network_bandwidth=copy.deepcopy(results_data.get("network_bandwidth", {})),
                tcp_thread_transport_summary=copy.deepcopy(
                    results_data.get("tcp_thread_transport_summary", {})
                ),
                p2p_receive_transport_summary=copy.deepcopy(
                    results_data.get("p2p_receive_transport_summary", {})
                ),
                p2p_rpc_completion_summary=copy.deepcopy(
                    results_data.get("p2p_rpc_completion_summary", {})
                ),
            )

            reported_result_node_count = self._upsert_test_result(metrics)
            logger.info(f"📊 收到节点 {node_id} 的测试结果")
            self._update_round_gate_waiting(
                test_id=metrics.test_id,
                completion_error=None,
            )

            # Count completion by unique node_id, not raw message count.
            if reported_result_node_count >= self.expected_nodes:
                logger.info("🎉 所有节点都已上报结果")
                self._set_round_gate_terminal(
                    test_id=metrics.test_id,
                    status=ROUND_GATE_STATUS_COMPLETED,
                    completion_error=None,
                )
                self.all_results_received.set()

            # Send ack response
            response = {"status": "success"}  # Results received
            return self._send_tcp_response(client_socket, response)

        except Exception as e:
            logger.error(f"❌ 处理节点 {node_id} 结果时发生异常: {e}")
            response = {"status": "error", "error": str(e)}
            return self._send_tcp_response(client_socket, response)

    def start_new_test(self, config: TestConfig):
        """配置新测试（可多轮调用，用于 sweep 场景）。"""
        with self.lock:
            self.test_config = config
            self.test_results[config.test_id] = []
            self.round_gate_states[config.test_id] = {
                "status": ROUND_GATE_STATUS_WAITING,
                "expected_nodes": int(self.expected_nodes),
                "reported_result_node_count": 0,
                "completion_error": None,
            }
            self.ready_nodes_for_current_test = set()
            self.runtime_ready_nodes_for_current_test = set()
            # 新一轮测试前清理状态
            self.all_nodes_ready.clear()
            self.all_runtime_ready.clear()
            self.all_results_received.clear()
        logger.info(f"Test started: {config.test_id}")
        logger.info(f"Config: {config}")

    def _current_test_id(self) -> Optional[str]:
        with self.lock:
            if self.test_config is None:
                return None
            return str(self.test_config.test_id)

    def _update_round_gate_waiting(
        self,
        *,
        test_id: str,
        completion_error: Optional[str],
    ) -> None:
        with self.lock:
            state = self.round_gate_states.setdefault(
                str(test_id),
                {
                    "status": ROUND_GATE_STATUS_WAITING,
                    "expected_nodes": int(self.expected_nodes),
                    "reported_result_node_count": 0,
                    "completion_error": None,
                },
            )
            results = self.test_results.get(str(test_id), [])
            state["status"] = ROUND_GATE_STATUS_WAITING
            state["expected_nodes"] = int(self.expected_nodes)
            state["reported_result_node_count"] = len(
                {str(node.node_id) for node in results}
            )
            state["completion_error"] = completion_error

    def _set_round_gate_terminal(
        self,
        *,
        test_id: str,
        status: str,
        completion_error: Optional[str],
    ) -> None:
        with self.lock:
            state = self.round_gate_states.setdefault(
                str(test_id),
                {
                    "status": ROUND_GATE_STATUS_WAITING,
                    "expected_nodes": int(self.expected_nodes),
                    "reported_result_node_count": 0,
                    "completion_error": None,
                },
            )
            results = self.test_results.get(str(test_id), [])
            state["status"] = status
            state["expected_nodes"] = int(self.expected_nodes)
            state["reported_result_node_count"] = len(
                {str(node.node_id) for node in results}
            )
            state["completion_error"] = completion_error

    def _round_gate_snapshot(self, *, test_id: str) -> Dict[str, Any]:
        with self.lock:
            state = self.round_gate_states.get(str(test_id))
            if state is None:
                return {
                    "status": "error",
                    "error": f"unknown test_id: {test_id}",
                }
            return {
                "status": str(state["status"]),
                "test_id": str(test_id),
                "expected_nodes": int(state["expected_nodes"]),
                "reported_result_node_count": int(
                    state["reported_result_node_count"]
                ),
                "completion_error": state.get("completion_error"),
            }

    def handle_round_status_request(
        self, message: Dict[str, Any], client_socket: socket.socket
    ) -> bool:
        test_id_raw = message.get("test_id")
        if not isinstance(test_id_raw, str) or not test_id_raw.strip():
            response = {"status": "error", "error": "missing test_id"}
            return self._send_tcp_response(client_socket, response)
        response = self._round_gate_snapshot(test_id=test_id_raw.strip())
        return self._send_tcp_response(client_socket, response)

    def _registered_node_state_snapshot(self) -> List[Dict[str, Any]]:
        with self.lock:
            items = []
            for node_id in sorted(self.registered_nodes.keys()):
                meta = dict(self.registered_nodes[node_id])
                items.append(
                    {
                        "node_id": node_id,
                        "status": meta.get("status"),
                        "node_role": meta.get("node_role"),
                        "instance_key": meta.get("instance_key"),
                    }
                )
            return items

    def _reported_result_node_ids(self) -> List[str]:
        with self.lock:
            if not self.test_config:
                return []
            test_id = self.test_config.test_id
            results = self.test_results.get(test_id, [])
            return sorted({str(node.node_id) for node in results})

    def _registered_node_ids(self) -> List[str]:
        with self.lock:
            return sorted(str(node_id) for node_id in self.registered_nodes.keys())

    def _ready_node_ids(self) -> List[str]:
        with self.lock:
            reported_node_ids = set()
            if self.test_config:
                test_id = self.test_config.test_id
                reported_node_ids = {
                    str(node.node_id) for node in self.test_results.get(test_id, [])
                }
            return sorted(
                str(node_id)
                for node_id in self.registered_nodes.keys()
                if str(node_id) in self.ready_nodes_for_current_test
                or str(node_id) in reported_node_ids
            )

    def _runtime_ready_node_ids(self) -> List[str]:
        with self.lock:
            return sorted(str(node_id) for node_id in self.runtime_ready_nodes_for_current_test)

    def _missing_result_node_ids(self) -> List[str]:
        with self.lock:
            reported = set()
            if self.test_config:
                test_id = self.test_config.test_id
                reported = {str(node.node_id) for node in self.test_results.get(test_id, [])}
            return sorted(node_id for node_id in self.registered_nodes.keys() if node_id not in reported)

    def wait_for_nodes_ready(self, *, timeout_s: float) -> bool:
        """Wait for current-round READY, arming the deadline only after cluster participation begins.

        TEST_STACK may start the coordinator well before benchmark nodes are deployed. Counting that
        orchestration gap against `cluster_ready_timeout_seconds` causes false READY timeouts. For the
        initial round, defer the READY deadline until at least one benchmark node has registered. For
        later rounds in a sweep, nodes are already registered, so the deadline starts immediately.
        """
        if timeout_s <= 0:
            raise ValueError(f"wait_for_nodes_ready timeout_s must be > 0, got {timeout_s}")

        poll_interval_s = 1.0
        stall_log_interval_s = 30.0
        ready_deadline: Optional[float] = None
        last_prereg_log_at = time.monotonic()

        logger.info(
            "⏳ READY wait armed: deadline starts after first node registration "
            f"(or immediately when nodes are already registered); timeout_s={timeout_s} "
            f"expected_nodes={self.expected_nodes}"
        )

        while True:
            if self.all_nodes_ready.wait(timeout=poll_interval_s):
                return True

            now_mono = time.monotonic()
            with self.lock:
                registered_count = len(self.registered_nodes)
                ready_count = len(self.ready_nodes_for_current_test)

            if ready_deadline is None:
                if registered_count > 0:
                    ready_deadline = now_mono + float(timeout_s)
                    logger.info(
                        "⏱️ READY deadline activated after node participation: "
                        f"timeout_s={timeout_s} registered_nodes={registered_count}/{self.expected_nodes} "
                        f"ready_nodes={ready_count}/{self.expected_nodes}"
                    )
                elif now_mono - last_prereg_log_at >= stall_log_interval_s:
                    logger.info(
                        "⏳ still waiting for first benchmark node registration before READY deadline starts: "
                        f"expected_nodes={self.expected_nodes}"
                    )
                    last_prereg_log_at = now_mono
                continue

            if now_mono < ready_deadline:
                continue

            with self.lock:
                registered_nodes = sorted(str(node_id) for node_id in self.registered_nodes.keys())
                ready_nodes = sorted(str(node_id) for node_id in self.ready_nodes_for_current_test)
            missing_ready_nodes = sorted(node_id for node_id in registered_nodes if node_id not in ready_nodes)
            logger.error(
                "❌ nodes did not become READY before deadline after registration began: "
                f"timeout_s={timeout_s} expected_nodes={self.expected_nodes} "
                f"registered_nodes={registered_nodes} ready_nodes={ready_nodes} "
                f"missing_ready_nodes={missing_ready_nodes}"
            )
            return False

    def wait_for_runtime_ready(self, *, timeout_s: float) -> bool:
        """Wait for all MPMC nodes to finish runtime setup before result timing starts."""
        if timeout_s <= 0:
            raise ValueError(f"wait_for_runtime_ready timeout_s must be > 0, got {timeout_s}")

        completed = self.all_runtime_ready.wait(timeout=timeout_s)
        if completed:
            return True

        runtime_ready_nodes = self._runtime_ready_node_ids()
        with self.lock:
            registered_nodes = sorted(str(node_id) for node_id in self.registered_nodes.keys())
        missing_runtime_ready_nodes = sorted(
            node_id for node_id in registered_nodes if node_id not in runtime_ready_nodes
        )
        logger.error(
            "❌ MPMC runtime nodes did not become ready before deadline: "
            f"timeout_s={timeout_s} expected_nodes={self.expected_nodes} "
            f"runtime_ready_nodes={runtime_ready_nodes} "
            f"missing_runtime_ready_nodes={missing_runtime_ready_nodes}"
        )
        return False

    def _build_completion_metadata(
        self,
        *,
        status: str,
        elapsed_seconds: float,
        completion_error: Optional[str],
    ) -> Dict[str, Any]:
        registered_node_ids = self._registered_node_ids()
        ready_node_ids = self._ready_node_ids()
        runtime_ready_node_ids = self._runtime_ready_node_ids()
        reported_result_node_ids = self._reported_result_node_ids()
        pending_result_node_ids = self._missing_result_node_ids()
        return {
            "status": status,
            "elapsed_seconds": float(elapsed_seconds),
            "expected_nodes": int(self.expected_nodes),
            "registered_node_count": len(registered_node_ids),
            "registered_node_ids": registered_node_ids,
            "ready_node_count": len(ready_node_ids),
            "ready_node_ids": ready_node_ids,
            "runtime_ready_node_count": len(runtime_ready_node_ids),
            "runtime_ready_node_ids": runtime_ready_node_ids,
            "reported_result_node_count": len(reported_result_node_ids),
            "reported_result_node_ids": reported_result_node_ids,
            "pending_result_node_count": len(pending_result_node_ids),
            "pending_result_node_ids": pending_result_node_ids,
            "completion_error": completion_error,
        }

    def get_aggregated_error_details(self) -> Dict[str, int]:
        with self.lock:
            if (
                not self.test_config
                or self.test_config.test_id not in self.test_results
            ):
                return {}
            results = self.test_results[self.test_config.test_id]

        aggregated: Dict[str, int] = {}
        for node in results:
            for error_key, count in node.error_details.items():
                aggregated[str(error_key)] = aggregated.get(str(error_key), 0) + int(count)
        return aggregated

    def _upsert_test_result(self, metrics: NodeMetrics) -> int:
        with self.lock:
            test_id = metrics.test_id
            if test_id not in self.test_results:
                self.test_results[test_id] = []
            results = self.test_results[test_id]
            node_id = str(metrics.node_id)
            for idx, existing in enumerate(results):
                if str(existing.node_id) == node_id:
                    results[idx] = metrics
                    break
            else:
                results.append(metrics)
            return len({str(node.node_id) for node in results})

    def wait_for_completion(self, *, timeout_s: float) -> bool:
        """Wait for all node results with a config-derived deadline."""
        if timeout_s <= 0:
            raise ValueError(f"wait_for_completion timeout_s must be > 0, got {timeout_s}")
        test_id = self._current_test_id()
        completed = self.all_results_received.wait(timeout=timeout_s)
        if completed:
            if test_id is not None:
                self._set_round_gate_terminal(
                    test_id=test_id,
                    status=ROUND_GATE_STATUS_COMPLETED,
                    completion_error=None,
                )
            return True
        missing_nodes = self._missing_result_node_ids()
        reported_nodes = self._reported_result_node_ids()
        completion_error = (
            "benchmark result wait timed out after "
            f"{timeout_s:.1f}s"
        )
        logger.error(
            "❌ benchmark result wait timed out: "
            f"timeout_s={timeout_s} expected_nodes={self.expected_nodes} "
            f"reported_nodes={reported_nodes} missing_nodes={missing_nodes}"
        )

        # English note:
        # - In MPMC mode, producer/consumer are separate logical nodes.
        # - If only consumer nodes are missing results, we force a placeholder metrics
        #   record for each missing consumer so the run can converge deterministically,
        #   while keeping the anomaly visible in error_details.
        # - If any non-consumer node is missing (e.g. producer), we still fail the run.
        if self.test_config and self.test_config.test_mode == TestMode.MPMC.value:
            missing_non_consumer: list[str] = []
            missing_consumers: list[str] = []
            with self.lock:
                for node_id in missing_nodes:
                    meta = self.registered_nodes.get(node_id) or {}
                    role = meta.get("node_role")
                    role_s = str(role) if isinstance(role, str) else "unknown"
                    if role_s == "consumer":
                        missing_consumers.append(str(node_id))
                    else:
                        missing_non_consumer.append(str(node_id))

            if not missing_non_consumer and missing_consumers:
                # Only force-complete if we have at least one producer result.
                has_producer_result = False
                with self.lock:
                    for raw in self.test_results.get(self.test_config.test_id, []):
                        if isinstance(raw, NodeMetrics) and raw.node_role == "producer":
                            has_producer_result = True
                            break

                if has_producer_result:
                    forced_error_label = "forced_missing_consumer_result_timeout"
                    forced_completion_ready = False
                    with self.lock:
                        test_id = self.test_config.test_id
                        if test_id not in self.test_results:
                            self.test_results[test_id] = []
                        for node_id in missing_consumers:
                            meta = self.registered_nodes.get(node_id) or {}
                            role = meta.get("node_role")
                            role_s = str(role) if isinstance(role, str) else "consumer"
                            # Force a placeholder result with explicit error_details so downstream
                            # can surface the anomaly without stalling the whole suite.
                            self.test_results[test_id].append(
                                NodeMetrics(
                                    test_id=test_id,
                                    node_id=str(node_id),
                                    node_role=role_s,
                                    total_operations=1,
                                    successful_operations=0,
                                    failed_operations=1,
                                    get_total_operations=0,
                                    get_hit_operations=0,
                                    get_miss_operations=0,
                                    get_error_operations=0,
                                    avg_latency_us=0.0,
                                    p50_latency_us=0.0,
                                    p99_latency_us=0.0,
                                    p95_latency_us=0.0,
                                    throughput_ops_per_sec=0.0,
                                    total_throughput_ops_per_sec=0.0,
                                    get_total_throughput_ops_per_sec=0.0,
                                    get_hit_throughput_ops_per_sec=0.0,
                                    get_miss_throughput_ops_per_sec=0.0,
                                    total_bytes_processed=0,
                                    total_duration_seconds=0.0,
                                    error_details={
                                        forced_error_label: 1,
                                    },
                                    top_slowest_operations=[],
                                    inflight_max=0,
                                    inflight_avg=0.0,
                                    observed_value_size_histogram={},
                                    observed_value_size_avg=0.0,
                                    observed_value_size_min=0,
                                    observed_value_size_max=0,
                                    fluxon_phase_summary={},
                                    network_bandwidth={},
                                    tcp_thread_transport_summary={},
                                    p2p_receive_transport_summary={},
                                    p2p_rpc_completion_summary={},
                                )
                            )

                        logger.error(
                            "⚠️ force-completed MPMC run by inserting placeholder results for missing consumer nodes: "
                            f"test_id={test_id} missing_consumers={missing_consumers} "
                            f"reported_nodes={reported_nodes} timeout_s={timeout_s}"
                        )

                        forced_completion_ready = (
                            len(
                                {
                                    str(node.node_id)
                                    for node in self.test_results[test_id]
                                }
                            )
                            >= self.expected_nodes
                        )

                    if forced_completion_ready:
                        self._set_round_gate_terminal(
                            test_id=test_id,
                            status=ROUND_GATE_STATUS_COMPLETED,
                            completion_error=None,
                        )
                        self.all_results_received.set()
                        return True

        if test_id is not None:
            self._set_round_gate_terminal(
                test_id=test_id,
                status=ROUND_GATE_STATUS_FAILED,
                completion_error=completion_error,
            )
        return False

    def build_incomplete_run_summary(
        self,
        *,
        elapsed_seconds: float,
        completion_error: str,
    ) -> Dict[str, Any]:
        if not self.test_config:
            raise RuntimeError("build_incomplete_run_summary requires active test_config")
        overall = self.get_overall_summary()
        node_summaries = self.get_node_summaries() or []
        bench7 = self.get_bench_7_points()
        if overall is None:
            overall = {
                "total_ops": 0,
                "total_successful_ops": 0,
                "total_failed_ops": 0,
                "get_total_operations": 0,
                "get_hit_operations": 0,
                "get_miss_operations": 0,
                "get_error_operations": 0,
                "total_duration_seconds": 0.0,
                "total_bytes": 0,
                "overall_success_rate": 0.0,
                "overall_avg_latency_us": 0.0,
                "overall_tps": 0.0,
                "overall_total_tps": 0.0,
                "get_total_tps": 0.0,
                "get_hit_tps": 0.0,
                "get_miss_tps": 0.0,
                "fluxon_phase_summary": {},
                "observed_value_size_histogram": {},
                "observed_value_size_avg": 0.0,
                "observed_value_size_min": 0,
                "observed_value_size_max": 0,
                "network_bandwidth_by_machine": {
                    "machine_count": 0,
                    "sum_avg_rx_mbps": 0.0,
                    "sum_avg_tx_mbps": 0.0,
                    "sum_peak_rx_mbps": 0.0,
                    "sum_peak_tx_mbps": 0.0,
                    "max_machine_peak_rx_mbps": 0.0,
                    "max_machine_peak_tx_mbps": 0.0,
                    "machines": [],
                },
                "tcp_thread_transport_summary": {},
                "p2p_receive_transport_summary": {},
                "p2p_rpc_completion_summary": {},
            }
        if bench7 is None:
            bench7 = {
                "total_ops": int(overall["total_ops"]),
                "total_successful_ops": int(overall["total_successful_ops"]),
                "total_failed_ops": int(overall["total_failed_ops"]),
                "get_total_operations": int(overall["get_total_operations"]),
                "get_hit_operations": int(overall["get_hit_operations"]),
                "get_miss_operations": int(overall["get_miss_operations"]),
                "get_error_operations": int(overall["get_error_operations"]),
                "total_duration_seconds": float(overall["total_duration_seconds"]),
                "throughput_ops_per_sec": float(overall["overall_tps"]),
                "total_throughput_ops_per_sec": float(overall["overall_total_tps"]),
                "get_total_throughput_ops_per_sec": float(overall["get_total_tps"]),
                "get_hit_throughput_ops_per_sec": float(overall["get_hit_tps"]),
                "get_miss_throughput_ops_per_sec": float(overall["get_miss_tps"]),
                "avg_latency_us": float(overall["overall_avg_latency_us"]),
                "p50_latency_us": 0.0,
                "p95_latency_us": 0.0,
                "p99_latency_us": 0.0,
                "inflight_avg": 0.0,
                "inflight_max": 0,
            }

        run_summary = {
            "test_id": self.test_config.test_id,
            "threads_per_process": self.test_config.threads_per_process,
            "value_size_mode": self.test_config.value_size_mode,
            "value_size": self.test_config.value_size,
            "value_size_weighted_set": copy.deepcopy(self.test_config.value_size_weighted_set),
            "duration_seconds": elapsed_seconds,
            "completed": False,
            "completion_error": completion_error,
            "completion": self._build_completion_metadata(
                status=COMPLETION_STATUS_RESULT_TIMEOUT,
                elapsed_seconds=elapsed_seconds,
                completion_error=completion_error,
            ),
            "registered_nodes": self._registered_node_state_snapshot(),
            "aggregated_error_details": self.get_aggregated_error_details(),
        }
        run_summary.update(overall)
        run_summary.update({"node_summaries": node_summaries})
        run_summary.update({"bench_7_points": bench7})
        return run_summary

    def build_completed_run_summary(self, *, elapsed_seconds: float) -> Dict[str, Any]:
        if not self.test_config:
            raise RuntimeError("build_completed_run_summary requires active test_config")
        overall = self.get_overall_summary()
        node_summaries = self.get_node_summaries()
        bench7 = self.get_bench_7_points()
        if overall is None or node_summaries is None or bench7 is None:
            raise RuntimeError("completed benchmark run is missing summary components")

        run_summary = {
            "test_id": self.test_config.test_id,
            "threads_per_process": self.test_config.threads_per_process,
            "value_size_mode": self.test_config.value_size_mode,
            "value_size": self.test_config.value_size,
            "value_size_weighted_set": copy.deepcopy(self.test_config.value_size_weighted_set),
            "duration_seconds": elapsed_seconds,
            "completed": True,
            "completion": self._build_completion_metadata(
                status=COMPLETION_STATUS_SUCCESS,
                elapsed_seconds=elapsed_seconds,
                completion_error=None,
            ),
            "aggregated_error_details": self.get_aggregated_error_details(),
        }
        run_summary.update(overall)
        run_summary.update({"node_summaries": node_summaries})
        run_summary.update({"bench_7_points": bench7})
        return run_summary

    def print_summary(self):
        """Print the test result summary."""
        with self.lock:
            if (
                not self.test_config
                or self.test_config.test_id not in self.test_results
            ):
                logger.error("❌ 没有找到活跃测试的结果")
                return

            results = self.test_results[self.test_config.test_id]

        if not results:
            logger.warning("⚠️ 没有结果可以显示")
            return

        # Group by role
        role_groups: Dict[str, List[NodeMetrics]] = {}
        for r in results:
            role = r.node_role
            if role not in role_groups:
                role_groups[role] = []
            role_groups[role].append(r)

        # Debug info
        logger.info(f"🔍 结果统计: 总结果数={len(results)}")
        for role, nodes in role_groups.items():
            logger.info(f"  - {role}: {len(nodes)} 个节点")
            for r in nodes:
                logger.info(
                    f"    - {r.node_id}: success_ops={r.successful_operations} "
                    f"success_tps={r.throughput_ops_per_sec:.2f} "
                    f"get_hit={r.get_hit_operations} get_miss={r.get_miss_operations}"
                )

        print("\n" + "=" * 100)
        print(f"🎯 BENCHMARK RESULTS - {self.test_config.test_id}")
        print(f"📊 Test Mode: {self.test_config.test_mode}")
        print(
            f"⚙️ Test Config: {self.test_config.threads_per_process} threads/process, "
            f"duration {self.test_config.max_benchmark_seconds}s, "
            f"value_size {_describe_value_size_config(self.test_config.value_size_mode, self.test_config.value_size, self.test_config.value_size_weighted_set)}"
        )
        print("=" * 100)

        # Overall stats
        if results:
            bench_points = _build_aggregated_bench_points(results)
            total_ops = int(bench_points["total_ops"])
            total_successful_ops = int(bench_points["total_successful_ops"])
            total_failed_ops = int(bench_points["total_failed_ops"])
            total_duration = float(bench_points["total_duration_seconds"])
            total_bytes = sum(r.total_bytes_processed for r in results)
            overall_success_rate = (
                (total_successful_ops / total_ops * 100) if total_ops > 0 else 0
            )

            # Compute overall latency stats
            all_latencies = []
            for r in results:
                if r.successful_operations > 0:
                    all_latencies.extend([r.avg_latency_us] * r.successful_operations)

            overall_avg_latency = statistics.mean(all_latencies) if all_latencies else 0

            print("🌐 Overall Statistics:")
            print(f"  📅 Test Duration: {total_duration:.2f} seconds")
            print(f"  📈 Total Operations: {total_ops:,}")
            print(f"  ✅ Successful Operations: {total_successful_ops:,}")
            print(f"  ❌ Failed Operations: {total_failed_ops:,}")
            print(
                f"  🎯 GET Outcome Split: total={int(bench_points['get_total_operations']):,} "
                f"hit={int(bench_points['get_hit_operations']):,} "
                f"miss={int(bench_points['get_miss_operations']):,} "
                f"error={int(bench_points['get_error_operations']):,}"
            )
            print(f"  📊 Success Rate: {overall_success_rate:.2f}%")
            print(f"  📦 Total Data Transferred: {total_bytes / (1024*1024):.2f} MB")
            print(
                f"  🚀 Aggregate Total Throughput: {float(bench_points['total_throughput_ops_per_sec']):.2f} ops/sec"
                if total_duration > 0
                else "  🚀 Aggregate Total Throughput: N/A"
            )
            print(
                f"  🚀 Aggregate Success Throughput: {float(bench_points['throughput_ops_per_sec']):.2f} ops/sec"
                if total_duration > 0
                else "  🚀 Aggregate Success Throughput: N/A"
            )
            print(
                f"  🚀 GET Total Throughput: {float(bench_points['get_total_throughput_ops_per_sec']):.2f} ops/sec"
                if total_duration > 0
                else "  🚀 GET Total Throughput: N/A"
            )
            print(
                f"  🚀 GET Hit Throughput: {float(bench_points['get_hit_throughput_ops_per_sec']):.2f} ops/sec"
                if total_duration > 0
                else "  🚀 GET Hit Throughput: N/A"
            )
            print(
                f"  🚀 GET Miss Throughput: {float(bench_points['get_miss_throughput_ops_per_sec']):.2f} ops/sec"
                if total_duration > 0
                else "  🚀 GET Miss Throughput: N/A"
            )
            print(f"  ⏱️ Overall Average Latency: {overall_avg_latency/1000:.3f} ms")
            observed_avg_value_size = (
                (float(total_bytes) / float(total_successful_ops))
                if total_successful_ops > 0
                else 0.0
            )
            print(f"  📏 Observed Average Value Size: {observed_avg_value_size / (1024*1024):.2f} MiB")

        # Aggregate and print global Top-N slowest operations by role (producer/consumer).
        top_n_global = 20
        all_slowest_ops: List[Dict[str, Any]] = []
        for m in results:
            if not getattr(m, "top_slowest_operations", None):
                continue
            for op in m.top_slowest_operations:
                if not isinstance(op, dict):
                    continue
                rec = dict(op)
                # Fill missing fields so later prints have complete information.
                rec.setdefault("node_id", m.node_id)
                rec.setdefault("worker_id", None)
                rec.setdefault("node_role", m.node_role)
                all_slowest_ops.append(rec)

        if all_slowest_ops:
            # Split by node role and show tail latency separately for producer/consumer.
            role_to_ops: Dict[str, List[Dict[str, Any]]] = {"producer": [], "consumer": []}
            for rec in all_slowest_ops:
                role = str(rec.get("node_role", "")).lower()
                if role in role_to_ops:
                    role_to_ops[role].append(rec)

            def _print_role_tail(role_name: str, ops: List[Dict[str, Any]]) -> None:
                if not ops:
                    return
                sorted_ops = sorted(
                    ops,
                    key=lambda x: float(x.get("latency_us", 0.0)),
                    reverse=True,
                )[:top_n_global]

                display_role = role_name.upper()
                print("\n" + "-" * 100)
                print(
                    f"📉 GLOBAL TOP {len(sorted_ops)} SLOWEST OPERATIONS FOR {display_role}:"
                )
                print("-" * 100)
                print(
                    f"{'Rank':<6} {'Latency(us)':<14} {'Node':<18} "
                    f"{'Role':<10} {'Worker':<8} {'OpType':<10} {'Key':<40} {'Size':<8}"
                )
                print("─" * 120)

                for idx, rec in enumerate(sorted_ops, start=1):
                    latency = float(rec.get("latency_us", 0.0))
                    node_id = str(rec.get("node_id", ""))
                    node_role = str(rec.get("node_role", ""))
                    worker_id = rec.get("worker_id")
                    worker_str = str(worker_id) if worker_id is not None else "-"
                    op_type = str(rec.get("operation_type", ""))
                    key = str(rec.get("key", ""))
                    size = int(rec.get("data_size", 0))

                    print(
                        f"{idx:<6} "
                        f"{latency:<14.0f} "
                        f"{node_id:<18.18} "
                        f"{node_role:<10.10} "
                        f"{worker_str:<8.8} "
                        f"{op_type:<10.10} "
                        f"{key[:40]:<40} "
                        f"{size:<8}"
                    )

            _print_role_tail("producer", role_to_ops["producer"])
            _print_role_tail("consumer", role_to_ops["consumer"])

        print("\n" + "-" * 100)
        print("📋 DETAILED STATISTICS BY ROLE:")
        print("-" * 100)

        # Per-role detailed stats
        for role_idx, (role, nodes) in enumerate(role_groups.items()):
            if nodes:
                # Convert role display name
                display_role = role.upper()

                print(f"\n🎭 【{display_role}】 Role Statistics ({len(nodes)} nodes):")
                print("─" * 80)

                # Role-level totals
                role_total_ops = sum(r.total_operations for r in nodes)
                role_successful_ops = sum(r.successful_operations for r in nodes)
                role_failed_ops = sum(r.failed_operations for r in nodes)
                role_success_throughput = sum(r.throughput_ops_per_sec for r in nodes)
                role_total_throughput = sum(r.total_throughput_ops_per_sec for r in nodes)
                role_get_total_ops = sum(r.get_total_operations for r in nodes)
                role_get_hit_ops = sum(r.get_hit_operations for r in nodes)
                role_get_miss_ops = sum(r.get_miss_operations for r in nodes)
                role_get_error_ops = sum(r.get_error_operations for r in nodes)
                role_get_total_throughput = sum(
                    r.get_total_throughput_ops_per_sec for r in nodes
                )
                role_get_hit_throughput = sum(
                    r.get_hit_throughput_ops_per_sec for r in nodes
                )
                role_get_miss_throughput = sum(
                    r.get_miss_throughput_ops_per_sec for r in nodes
                )
                role_total_bytes = sum(r.total_bytes_processed for r in nodes)
                role_success_rate = (
                    (role_successful_ops / role_total_ops * 100)
                    if role_total_ops > 0
                    else 0
                )

                role_inflight_avgs = [r.inflight_avg for r in nodes]
                role_inflight_avg_mean = (
                    statistics.mean(role_inflight_avgs) if role_inflight_avgs else 0.0
                )
                role_inflight_max = max((r.inflight_max for r in nodes), default=0)

                # Latency stats
                role_avg_latencies = [
                    r.avg_latency_us for r in nodes if r.avg_latency_us > 0
                ]
                role_p95_latencies = [
                    r.p95_latency_us for r in nodes if r.p95_latency_us > 0
                ]
                role_p99_latencies = [
                    r.p99_latency_us for r in nodes if r.p99_latency_us > 0
                ]

                role_avg_latency_mean = (
                    statistics.mean(role_avg_latencies) if role_avg_latencies else 0
                )
                role_p95_latency_mean = (
                    statistics.mean(role_p95_latencies) if role_p95_latencies else 0
                )
                role_p99_latency_mean = (
                    statistics.mean(role_p99_latencies) if role_p99_latencies else 0
                )

                # Print role summary
                print(f"📊 Role Summary:")
                print(f"  📈 Total Operations: {role_total_ops:,}")
                print(
                    f"  ✅ Successful: {role_successful_ops:,} ({role_success_rate:.2f}%)"
                )
                print(f"  ❌ Failed: {role_failed_ops:,}")
                print(f"  🎯 GET Outcome Split: total={role_get_total_ops:,} hit={role_get_hit_ops:,} miss={role_get_miss_ops:,} error={role_get_error_ops:,}")
                print(f"  🚀 Aggregate Total Throughput: {role_total_throughput:.2f} ops/sec")
                print(f"  🚀 Aggregate Success Throughput: {role_success_throughput:.2f} ops/sec")
                print(f"  🚀 GET Total Throughput: {role_get_total_throughput:.2f} ops/sec")
                print(f"  🚀 GET Hit Throughput: {role_get_hit_throughput:.2f} ops/sec")
                print(f"  🚀 GET Miss Throughput: {role_get_miss_throughput:.2f} ops/sec")
                print(f"  📦 Data Processed: {role_total_bytes / (1024*1024):.2f} MB")
                print(f"  🔁 Inflight Avg: {role_inflight_avg_mean:.2f}")
                print(f"  🔁 Inflight Max: {role_inflight_max}")
                print(f"  ⏱️ Average Latency: {role_avg_latency_mean/1000:.3f} ms")
                print(f"  📊 P95 Latency: {role_p95_latency_mean/1000:.3f} ms")
                print(f"  📊 P99 Latency: {role_p99_latency_mean/1000:.3f} ms")

                # Per-node detailed stats
                print(f"\n🖥️ Individual Node Performance:")
                print(
                    f"{'Node ID':<15} {'Operations':<12} {'Success':<8} {'Failed':<8} {'Success%':<10} {'TPS':<12} {'Avg Latency':<13} {'P95':<10} {'P99':<10} {'Data(MB)':<10}"
                )
                print("─" * 120)

                for node in sorted(
                    nodes, key=lambda x: x.throughput_ops_per_sec, reverse=True
                ):
                    node_success_rate = (
                        (node.successful_operations / node.total_operations * 100)
                        if node.total_operations > 0
                        else 0
                    )
                    data_mb = node.total_bytes_processed / (1024 * 1024)

                    print(
                        f"{node.node_id:<15} "
                        f"{node.total_operations:<12,} "
                        f"{node.successful_operations:<8,} "
                        f"{node.failed_operations:<8,} "
                        f"{node_success_rate:<10.2f} "
                        f"{node.throughput_ops_per_sec:<12.2f} "
                        f"{node.avg_latency_us/1000:<13.3f} "
                        f"{node.p95_latency_us/1000:<10.3f} "
                        f"{node.p99_latency_us/1000:<10.3f} "
                        f"{data_mb:<10.2f}"
                    )

                # Error details
                role_errors = {}
                for node in nodes:
                    for error, count in node.error_details.items():
                        role_errors[error] = role_errors.get(error, 0) + count

                if role_errors:
                    print(f"\n❌ Error Details for {display_role}:")
                    for error, count in sorted(
                        role_errors.items(), key=lambda x: x[1], reverse=True
                    ):
                        print(f"  - {error}: {count} occurrences")

        # Performance comparison
        if len(role_groups) > 1:
            print("\n" + "-" * 100)
            print("📊 PERFORMANCE COMPARISON:")
            print("-" * 100)

            role_stats = {}
            for role, nodes in role_groups.items():
                if nodes:
                    role_avg_latency_samples = [
                        r.avg_latency_us for r in nodes if r.avg_latency_us > 0
                    ]
                    role_stats[role] = {
                        "throughput": sum(r.total_throughput_ops_per_sec for r in nodes),
                        "get_hit_throughput": sum(
                            r.get_hit_throughput_ops_per_sec for r in nodes
                        ),
                        "get_miss_throughput": sum(
                            r.get_miss_throughput_ops_per_sec for r in nodes
                        ),
                        "avg_latency": (
                            statistics.mean(role_avg_latency_samples)
                            if role_avg_latency_samples
                            else 0.0
                        ),
                        "success_rate": (
                            (
                                sum(r.successful_operations for r in nodes)
                                / sum(r.total_operations for r in nodes)
                                * 100
                            )
                            if sum(r.total_operations for r in nodes) > 0
                            else 0
                        ),
                        "node_count": len(nodes),
                    }

            print(
                f"{'Role':<15} {'Node Count':<12} {'Total TPS':<15} {'GET Hit TPS':<15} {'GET Miss TPS':<15} {'Avg Latency(ms)':<18} {'Success Rate%':<15}"
            )
            print("─" * 80)

            for role, stats in sorted(
                role_stats.items(), key=lambda x: x[1]["throughput"], reverse=True
            ):
                # Convert role display name
                display_role = role.upper()

                print(
                    f"{display_role:<15} "
                    f"{stats['node_count']:<12} "
                    f"{stats['throughput']:<15.2f} "
                    f"{stats['get_hit_throughput']:<15.2f} "
                    f"{stats['get_miss_throughput']:<15.2f} "
                    f"{stats['avg_latency']/1000:<18.3f} "
                    f"{stats['success_rate']:<15.2f}"
                )

        # Bench 7-point summary (aggregated across nodes):
        # throughput + latency percentiles + inflight stats.
        bench_points = _build_aggregated_bench_points(results)

        print("\n" + "-" * 100)
        print("🧾 BENCH 7 POINTS:")
        print("-" * 100)
        print(f"  throughput_ops_per_sec: {float(bench_points['throughput_ops_per_sec']):.2f}")
        print(f"  total_throughput_ops_per_sec: {float(bench_points['total_throughput_ops_per_sec']):.2f}")
        print(f"  get_total_throughput_ops_per_sec: {float(bench_points['get_total_throughput_ops_per_sec']):.2f}")
        print(f"  get_hit_throughput_ops_per_sec: {float(bench_points['get_hit_throughput_ops_per_sec']):.2f}")
        print(f"  get_miss_throughput_ops_per_sec: {float(bench_points['get_miss_throughput_ops_per_sec']):.2f}")
        print(
            f"  get_operations: total={int(bench_points['get_total_operations'])} "
            f"hit={int(bench_points['get_hit_operations'])} "
            f"miss={int(bench_points['get_miss_operations'])} "
            f"error={int(bench_points['get_error_operations'])}"
        )
        print(f"  avg_latency_ms: {float(bench_points['avg_latency_us'])/1000:.3f}")
        print(f"  p50_latency_ms: {float(bench_points['p50_latency_us'])/1000:.3f}")
        print(f"  p95_latency_ms: {float(bench_points['p95_latency_us'])/1000:.3f}")
        print(f"  p99_latency_ms: {float(bench_points['p99_latency_us'])/1000:.3f}")
        print(f"  inflight_avg: {float(bench_points['inflight_avg']):.2f}")
        print(f"  inflight_max: {int(bench_points['inflight_max'])}")

        print("=" * 100)
        print("🎉 BENCHMARK COMPLETED")
        print("=" * 100)

    def get_overall_summary(self) -> Optional[Dict[str, Any]]:
        """返回当前 test_id 的整体统计信息，便于跨多轮测试做汇总对比。"""
        with self.lock:
            if (
                not self.test_config
                or self.test_config.test_id not in self.test_results
            ):
                logger.error("❌ get_overall_summary: 没有找到活跃测试的结果")
                return None
            results = self.test_results[self.test_config.test_id]

        if not results:
            logger.warning("⚠️ get_overall_summary: 当前测试没有任何结果")
            return None

        total_ops = sum(r.total_operations for r in results)
        total_successful_ops = sum(r.successful_operations for r in results)
        total_failed_ops = sum(r.failed_operations for r in results)
        bench_points = _build_aggregated_bench_points(results)
        total_duration = (
            max(r.total_duration_seconds for r in results) if results else 0
        )
        total_bytes = sum(r.total_bytes_processed for r in results)
        overall_success_rate = (
            (total_successful_ops / total_ops * 100) if total_ops > 0 else 0
        )

        # 计算整体平均延迟
        all_latencies = []
        for r in results:
            if r.successful_operations > 0:
                all_latencies.extend([r.avg_latency_us] * r.successful_operations)
        overall_avg_latency_us = (
            statistics.mean(all_latencies) if all_latencies else 0.0
        )

        overall_tps = float(bench_points["throughput_ops_per_sec"])

        observed_histogram: Dict[str, int] = {}
        observed_mins = [
            int(r.observed_value_size_min)
            for r in results
            if int(r.observed_value_size_min) > 0
        ]
        observed_maxs = [
            int(r.observed_value_size_max)
            for r in results
            if int(r.observed_value_size_max) > 0
        ]
        for r in results:
            for size_key, count in r.observed_value_size_histogram.items():
                observed_histogram[str(size_key)] = observed_histogram.get(str(size_key), 0) + int(count)

        observed_avg_value_size = (
            (float(total_bytes) / float(total_successful_ops))
            if total_successful_ops > 0
            else 0.0
        )
        fluxon_phase_summary = _aggregate_fluxon_phase_summary(results)
        network_bandwidth_by_machine = _aggregate_network_bandwidth_by_machine(results)
        tcp_thread_transport_summary = _aggregate_tcp_thread_transport_summary(results)
        p2p_receive_transport_summary = _aggregate_p2p_receive_transport_summary(results)
        p2p_rpc_completion_summary = _aggregate_p2p_rpc_completion_summary(
            results,
            fluxon_phase_summary,
            total_duration,
        )

        return {
            "total_ops": total_ops,
            "total_successful_ops": total_successful_ops,
            "total_failed_ops": total_failed_ops,
            "get_total_operations": int(bench_points["get_total_operations"]),
            "get_hit_operations": int(bench_points["get_hit_operations"]),
            "get_miss_operations": int(bench_points["get_miss_operations"]),
            "get_error_operations": int(bench_points["get_error_operations"]),
            "total_duration_seconds": total_duration,
            "total_bytes": total_bytes,
            "overall_success_rate": overall_success_rate,
            "overall_avg_latency_us": overall_avg_latency_us,
            "overall_tps": overall_tps,
            "overall_total_tps": float(bench_points["total_throughput_ops_per_sec"]),
            "get_total_tps": float(bench_points["get_total_throughput_ops_per_sec"]),
            "get_hit_tps": float(bench_points["get_hit_throughput_ops_per_sec"]),
            "get_miss_tps": float(bench_points["get_miss_throughput_ops_per_sec"]),
            "fluxon_phase_summary": fluxon_phase_summary,
            "observed_value_size_histogram": observed_histogram,
            "observed_value_size_avg": observed_avg_value_size,
            "observed_value_size_min": min(observed_mins) if observed_mins else 0,
            "observed_value_size_max": max(observed_maxs) if observed_maxs else 0,
            "network_bandwidth_by_machine": network_bandwidth_by_machine,
            "tcp_thread_transport_summary": tcp_thread_transport_summary,
            "p2p_receive_transport_summary": p2p_receive_transport_summary,
            "p2p_rpc_completion_summary": p2p_rpc_completion_summary,
        }

    def get_node_summaries(self) -> Optional[List[Dict[str, Any]]]:
        """Return per-node metrics for the active test in a stable JSON shape."""
        with self.lock:
            if (
                not self.test_config
                or self.test_config.test_id not in self.test_results
            ):
                logger.error("❌ get_node_summaries: no active test results")
                return None
            results = self.test_results[self.test_config.test_id]

        if not results:
            logger.warning("⚠️ get_node_summaries: empty results")
            return None

        out: List[Dict[str, Any]] = []
        for node in results:
            out.append(
                {
                    "node_id": node.node_id,
                    "node_role": node.node_role,
                    "total_operations": node.total_operations,
                    "successful_operations": node.successful_operations,
                    "failed_operations": node.failed_operations,
                    "get_total_operations": node.get_total_operations,
                    "get_hit_operations": node.get_hit_operations,
                    "get_miss_operations": node.get_miss_operations,
                    "get_error_operations": node.get_error_operations,
                    "avg_latency_us": node.avg_latency_us,
                    "p50_latency_us": node.p50_latency_us,
                    "p95_latency_us": node.p95_latency_us,
                    "p99_latency_us": node.p99_latency_us,
                    "throughput_ops_per_sec": node.throughput_ops_per_sec,
                    "total_throughput_ops_per_sec": node.total_throughput_ops_per_sec,
                    "get_total_throughput_ops_per_sec": node.get_total_throughput_ops_per_sec,
                    "get_hit_throughput_ops_per_sec": node.get_hit_throughput_ops_per_sec,
                    "get_miss_throughput_ops_per_sec": node.get_miss_throughput_ops_per_sec,
                    "total_bytes_processed": node.total_bytes_processed,
                    "total_duration_seconds": node.total_duration_seconds,
                    "error_details": dict(node.error_details),
                    "top_slowest_operations": copy.deepcopy(node.top_slowest_operations),
                    "inflight_max": node.inflight_max,
                    "inflight_avg": node.inflight_avg,
                    "observed_value_size_histogram": dict(node.observed_value_size_histogram),
                    "observed_value_size_avg": node.observed_value_size_avg,
                    "observed_value_size_min": node.observed_value_size_min,
                    "observed_value_size_max": node.observed_value_size_max,
                    "fluxon_phase_summary": copy.deepcopy(node.fluxon_phase_summary),
                    "network_bandwidth": copy.deepcopy(node.network_bandwidth),
                    "tcp_thread_transport_summary": copy.deepcopy(node.tcp_thread_transport_summary),
                    "p2p_receive_transport_summary": copy.deepcopy(
                        node.p2p_receive_transport_summary
                    ),
                    "p2p_rpc_completion_summary": copy.deepcopy(
                        node.p2p_rpc_completion_summary
                    ),
                }
            )
        return out

    def get_bench_7_points(self) -> Optional[Dict[str, Any]]:
        """Return 7-point aggregated metrics for the current test_id.

        This mirrors the summary printed by print_summary(), but returns structured data
        for automation.
        """
        with self.lock:
            if (
                not self.test_config
                or self.test_config.test_id not in self.test_results
            ):
                logger.error("❌ get_bench_7_points: no active test results")
                return None
            results = self.test_results[self.test_config.test_id]

        if not results:
            logger.warning("⚠️ get_bench_7_points: empty results")
            return None
        return _build_aggregated_bench_points(results)



def main():

    coordinator = CoordinatorServer(COORDINATOR_HOST, COORDINATOR_PORT)

    # Build the test plan. Process fanout is part of deploy-time topology, so the
    # runtime only supports baseline plus value-size sweep within the same instance set.
    tests: List[TestConfig] = []
    base_kvcache_cfg = (
        coordinator.test_config.kvcache_config if coordinator.test_config else {}
    )

    if not VALUE_SIZE_SWEEP_LIST:
        tests.append(
            TestConfig(
                test_id=f"test_{uuid.uuid4().hex[:8]}",
                threads_per_process=THREADS_PER_PROCESS,
                value_size_mode=VALUE_SIZE_MODE,
                value_size=VALUE_SIZE,
                value_size_weighted_set=copy.deepcopy(VALUE_SIZE_WEIGHTED_SET),
                test_mode=CURR_TEST_MODE,
                max_benchmark_seconds=MAX_BENCHMARK_SECONDS,
                cluster_ready_timeout_seconds=CLUSTER_READY_TIMEOUT_SECONDS,
                op_timeout_seconds=OP_TIMEOUT_SECONDS,
                metric_warmup_seconds=METRIC_WARMUP_SECONDS,
                start_idle_seconds=START_IDLE_SECONDS,
                kvcache_config=base_kvcache_cfg,
            )
        )
    else:
        added_ids = set()
        value_size_label = (
            f"v{VALUE_SIZE}"
            if VALUE_SIZE is not None
            else "weighted"
        )

        def _add_test(test_id: str, value_size: Optional[int]) -> None:
            if test_id in added_ids:
                return
            added_ids.add(test_id)
            tests.append(
                TestConfig(
                    test_id=test_id,
                    threads_per_process=THREADS_PER_PROCESS,
                    value_size_mode=VALUE_SIZE_MODE,
                    value_size=value_size,
                    value_size_weighted_set=copy.deepcopy(VALUE_SIZE_WEIGHTED_SET),
                    test_mode=CURR_TEST_MODE,
                    max_benchmark_seconds=MAX_BENCHMARK_SECONDS,
                    cluster_ready_timeout_seconds=CLUSTER_READY_TIMEOUT_SECONDS,
                    op_timeout_seconds=OP_TIMEOUT_SECONDS,
                    metric_warmup_seconds=METRIC_WARMUP_SECONDS,
                    start_idle_seconds=START_IDLE_SECONDS,
                    kvcache_config=base_kvcache_cfg,
                )
            )

        for val_size in VALUE_SIZE_SWEEP_LIST:
            tid = f"value_{val_size}_t{THREADS_PER_PROCESS}"
            _add_test(tid, val_size)

        if tests and all(
            not (
                t.threads_per_process == THREADS_PER_PROCESS
                and t.value_size == VALUE_SIZE
            )
            for t in tests
        ):
            baseline_id = f"baseline_t{THREADS_PER_PROCESS}_{value_size_label}"
            _add_test(baseline_id, VALUE_SIZE)

    if not tests:
        logger.error("❌ 测试计划为空，请检查 benchmark 配置")
        return 2

    coordinator.total_rounds = len(tests)
    logger.info(
        f"📋 已构建测试计划，共 {coordinator.total_rounds} 轮 "
        f"(value_size_list={VALUE_SIZE_SWEEP_LIST})"
    )

    # 在后台启动服务器（只需启动一次）
    server_thread = threading.Thread(target=coordinator.start, daemon=True)
    server_thread.start()

    all_summaries: List[Dict[str, Any]] = []

    for idx, cfg in enumerate(tests):
        coordinator.current_round_index = idx
        coordinator.has_more_tests = idx < len(tests) - 1
        coordinator.start_new_test(cfg)

        logger.info(
            f"⏳ 等待 {coordinator.expected_nodes} 个节点准备就绪 "
            f"(第 {idx + 1}/{len(tests)} 轮, "
            f"threads_per_process={cfg.threads_per_process}, "
            f"value_size={_describe_value_size_config(cfg.value_size_mode, cfg.value_size, cfg.value_size_weighted_set)})..."
        )
        ready_ok = coordinator.wait_for_nodes_ready(
            timeout_s=float(cfg.cluster_ready_timeout_seconds)
        )
        if not ready_ok:
            logger.error(
                "❌ nodes did not become READY before deadline: "
                f"timeout_s={cfg.cluster_ready_timeout_seconds} expected_nodes={coordinator.expected_nodes}"
            )
            timed_out_summary = coordinator.build_incomplete_run_summary(
                elapsed_seconds=0.0,
                completion_error=(
                    "node READY wait timed out after "
                    f"{float(cfg.cluster_ready_timeout_seconds):.1f}s"
                ),
            )
            all_summaries.append(timed_out_summary)
            _write_benchmark_result_file(all_summaries)
            logger.error(
                "❌ wrote incomplete benchmark_result.json due to node READY timeout; "
                "runner will fail the case deterministically"
            )
            _hold_forever_after_result_written(reason="node_ready_timeout")

        # 若服务器启动失败或配置不满足，直接退出
        if getattr(coordinator, "server_start_error", None):
            logger.error(
                f"💥 协调者启动失败或配置不满足: {coordinator.server_start_error}"
            )
            return 2

        if cfg.test_mode == TestMode.MPMC.value:
            runtime_ready_start_time = time.time()
            logger.info(
                "⏳ 等待 MPMC runtime_ready 屏障 "
                f"(timeout_s={cfg.cluster_ready_timeout_seconds} "
                f"expected_nodes={coordinator.expected_nodes})..."
            )
            runtime_ready_ok = coordinator.wait_for_runtime_ready(
                timeout_s=float(cfg.cluster_ready_timeout_seconds)
            )
            runtime_ready_elapsed = time.time() - runtime_ready_start_time
            if not runtime_ready_ok:
                logger.error(
                    "❌ MPMC runtime_ready did not complete before deadline: "
                    f"timeout_s={cfg.cluster_ready_timeout_seconds} "
                    f"expected_nodes={coordinator.expected_nodes}"
                )
                timed_out_summary = coordinator.build_incomplete_run_summary(
                    elapsed_seconds=runtime_ready_elapsed,
                    completion_error=(
                        "MPMC runtime_ready wait timed out after "
                        f"{float(cfg.cluster_ready_timeout_seconds):.1f}s"
                    ),
                )
                all_summaries.append(timed_out_summary)
                _write_benchmark_result_file(all_summaries)
                logger.error(
                    "❌ wrote incomplete benchmark_result.json due to MPMC runtime_ready timeout; "
                    "runner will fail the case deterministically"
                )
                _hold_forever_after_result_written(reason="mpmc_runtime_ready_timeout")
            logger.info(
                "✅ MPMC runtime_ready 屏障完成: elapsed_s=%.1f expected_nodes=%s",
                runtime_ready_elapsed,
                coordinator.expected_nodes,
            )

        start_time = time.time()
        logger.info(
            f"🚀 第 {idx + 1}/{len(tests)} 轮测试进行中，等待结果 "
            f"(threads_per_process={cfg.threads_per_process}, "
            f"value_size={_describe_value_size_config(cfg.value_size_mode, cfg.value_size, cfg.value_size_weighted_set)})..."
        )

        completion_timeout_s = _result_wait_timeout_seconds(
            cfg.max_benchmark_seconds,
            cfg.metric_warmup_seconds,
        )
        completed = coordinator.wait_for_completion(timeout_s=completion_timeout_s)

        end_time = time.time()
        elapsed = end_time - start_time

        if not completed:
            timed_out_summary = coordinator.build_incomplete_run_summary(
                elapsed_seconds=elapsed,
                completion_error=(
                    "benchmark result wait timed out after "
                    f"{completion_timeout_s:.1f}s"
                ),
            )
            all_summaries.append(timed_out_summary)
            _write_benchmark_result_file(all_summaries)
            logger.error(
                "❌ benchmark run did not finish before deadline; "
                "wrote incomplete benchmark_result.json so the runner can fail the case deterministically"
            )
            _hold_forever_after_result_written(reason="result_wait_timeout")

        all_summaries.append(
            coordinator.build_completed_run_summary(elapsed_seconds=elapsed)
        )

        if idx == len(tests) - 1:
            _write_benchmark_result_file(all_summaries)

        logger.info(f"🎉 第 {idx + 1}/{len(tests)} 轮测试完成，结果摘要:")
        coordinator.print_summary()

    # 跨轮次汇总
    if len(all_summaries) > 1:
        print("\n" + "=" * 100)
        print("📊 CROSS-RUN SUMMARY (threads / value_size sweep)")
        print("=" * 100)
        header = (
            f"{'Test ID':<20} {'Threads':<8} {'ValueSize':<36} "
            f"{'TPS':<12} {'Success%':<10} {'AvgLat(ms)':<12} "
            f"{'TotalOps':<12} {'Duration(s)':<12}"
        )
        print(header)
        print("─" * len(header))
        for s in all_summaries:
            print(
                f"{s['test_id']:<20} "
                f"{s['threads']:<8d} "
                f"{_describe_value_size_config(s.get('value_size_mode', ValueSizeMode.FIXED.value), s.get('value_size'), s.get('value_size_weighted_set')):<36.36} "
                f"{s['overall_tps']:<12.2f} "
                f"{s['overall_success_rate']:<10.2f} "
                f"{(s['overall_avg_latency_us'] / 1000.0):<12.3f} "
                f"{s['total_ops']:<12d} "
                f"{s['duration_seconds']:<12.2f}"
            )
        print("=" * 100)

    

    logger.info("🎉 所有测试轮次完成")
    _hold_forever_after_result_written(reason="all_rounds_completed")


if __name__ == "__main__":
    exit(main())
