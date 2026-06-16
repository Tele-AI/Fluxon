#!/usr/bin/env python3

from __future__ import annotations

import argparse
import base64
import copy
import datetime
import hashlib
import hmac
import html
import importlib.util
import json
import math
import os
import re
import shlex
import socket
import subprocess
import sys
import shutil
import signal
import threading
import tarfile
import tempfile
import time
import fcntl
import traceback
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse
import urllib.error
import urllib.parse
import urllib.request

import yaml

from benchmark_role_names import (
    KV_NODE_ROLE_SEED,
    KV_NODE_ROLE_WORKER,
    canonicalize_kv_node_role,
)
from gitops import gitops_lib
from top_attention_index_helper import (
    collect_top_attention_payload,
    iter_quick_entry_paths,
    print_top_attention_payload,
    run_top_attention_entries,
    select_top_attention_entries,
)


# NOTE: This project uses multiple schemas:
# - suite config: user input contract (YAML)
# - runner artifacts: internal on-disk records (case_runs.yaml, resolved_case.yaml, ...)
#
# Keep them decoupled to avoid accidental "schema bumps" across unrelated layers.
SCHEMA_VERSION = 1
SUITE_SCHEMA_VERSION = 9

# Enums (case-sensitive strings; internal routing only - not part of suite config schema)
SCENE_KIND_INFER = "INFER"
SCENE_KIND_CI = "CI"
SCENE_KIND_TEST_STACK = "TEST_STACK"
CASE_FAMILY_INFER = "infer"
CASE_FAMILY_CI = "ci"
CASE_FAMILY_BENCH = "bench"
INFER_PATTERN_REPEAT = "REPEAT_PROMPTS"
INFER_PATTERN_UNIQUE = "UNIQUE_PROMPTS"
INFER_STACK_VLLM_LMCACHE = "VLLM_LMCACHE"
INFER_STACK_SGLANG_HICACHE = "SGLANG_HICACHE"
RUN_OUTCOME_SUCCESS = "SUCCESS"
RUN_OUTCOME_FAILED = "FAILED"
_RUN_SUMMARY_INCOMPLETE_ERROR = "INCOMPLETE: run started but did not reach finalize; runner likely exited abruptly."
_RUN_EXCEPTION_FILENAME = "exception.txt"
CI_PRESERVED_APPLY_IDS_SCHEMA_VERSION = 1
CI_PRESERVED_APPLY_IDS_FILENAME = "ci_preserved_apply_ids.yaml"
CI_RUNTIME_CONTRACT_CLUSTER_KV_OWNER = "cluster_kv_owner"
CI_RUNTIME_CONTRACT_RUST_SELF_MANAGED = "rust_self_managed"
CI_RUNTIME_CONTRACT_IDS = (
    CI_RUNTIME_CONTRACT_CLUSTER_KV_OWNER,
    CI_RUNTIME_CONTRACT_RUST_SELF_MANAGED,
)
RUNTIME_LAYER_TEST_BED = "test_bed"
RUNTIME_LAYER_BASE = "base_runtime"
RUNTIME_LAYER_CASE = "case_runtime"
RUNTIME_LAYER_ORDER = (
    RUNTIME_LAYER_TEST_BED,
    RUNTIME_LAYER_BASE,
    RUNTIME_LAYER_CASE,
)
CI_BASE_RUNTIME_SERVICE_IDS = ("etcd", "greptime")
CI_CLUSTER_MEMBER_INSTANCE_IDS = ("master", "owner_0")
CI_CLUSTER_RUNTIME_INSTANCE_IDS = ("master", "owner_0")
CI_CASE_RUNTIME_INSTANCE_IDS = ("master", "owner_0", "ci_runner")
CI_CLUSTER_RUNTIME_REMOTE_STAGE_INCLUDE_RELPATHS = (
    "configs",
    "src/fluxon_py/runtime",
    "services/share_mem",
    "venv",
)
CI_CLUSTER_RUNTIME_REMOTE_STAGE_VERIFY_RELPATHS = (
    "src/fluxon_py/runtime/start_master.py",
    "src/fluxon_py/runtime/start_owner_kvclient.py",
)
CI_RUNNER_REMOTE_STAGE_INCLUDE_RELPATHS = (
    "ci_runner.sh",
    "configs",
    "services/share_mem",
    "src",
    "venv",
)
CI_RUNNER_REMOTE_STAGE_VERIFY_RELPATHS = ("ci_runner.sh",)
CI_OWNER_SHARED_BUNDLE_RELPATHS = ("services/share_mem/shared.json", "services/share_mem/mmap.file")
CI_RUNNER_SHARED_BUNDLE_TIMEOUT_S = 600
CI_RUNNER_READINESS_PROBE_DEADLINE_S = 120
CI_RUNNER_EXIT_CODE_GRACE_TIMEOUT_S = 300
TEST_STACK_REMOTE_STAGE_SHARED_INCLUDE_RELPATHS = (
    "benchmark_config.py",
    "deployer_deploy.yaml",
    "resolved_case.yaml",
    "resolved_case_full.yaml",
    "services",
    "test_rsc",
    "test_stack_runtime",
)
TEST_STACK_NODE_REMOTE_STAGE_INCLUDE_RELPATHS = (
    *TEST_STACK_REMOTE_STAGE_SHARED_INCLUDE_RELPATHS,
)
TEST_STACK_SERVICE_REMOTE_STAGE_INCLUDE_RELPATHS = (
    "benchmark_config.py",
    "resolved_case.yaml",
    "resolved_case_full.yaml",
    "configs",
    "services",
    "test_rsc",
    "test_stack_runtime",
)
CI_RUNTIME_LAYER_INSTANCE_IDS: Dict[str, Tuple[str, ...]] = {
    RUNTIME_LAYER_TEST_BED: (),
    RUNTIME_LAYER_BASE: (),
    RUNTIME_LAYER_CASE: CI_CASE_RUNTIME_INSTANCE_IDS,
}
CI_RUNTIME_INSTANCE_IDS = CI_CASE_RUNTIME_INSTANCE_IDS
CONTROLLER_STATUS_TRANSIENT_HTTP_CODES = (502, 503, 504)
CONTROLLER_REQUEST_MODE_SSH_EXEC_PER_REQUEST = "ssh_exec_per_request"
# Controller requests during TEST_STACK teardown can fan out to many remote nodes and are prone to
# short SSH/control-plane stalls. Keep each attempt bounded, but allow a wider retry window so
# transient transport errors do not abort a full benchmark matrix.
CONTROLLER_HTTP_TIMEOUT_SECONDS = 30.0
CONTROLLER_HTTP_SHORT_ATTEMPT_TIMEOUT_SECONDS = 5.0
CONTROLLER_HTTP_RETRY_DEADLINE_SECONDS = 300.0
CONTROLLER_HTTP_RETRY_SLEEP_SECONDS = 1.0
REPO_ROOT = Path(__file__).resolve().parent.parent
_CONTROLLER_BASIC_AUTH_HEADER_NAME = "x-fluxon-ops-authorization"
_CONTROLLER_BASIC_AUTH_HEADER: str | None = None
_SSH_STDERR_NOISE_PREFIXES = ("/etc/zsh/zshenv:", "zsh:")
_CURRENT_DEPLOYMENTS_MISSING_APPLY_ERR = (
    "inconsistent state: desired references apply_id(s) with missing apply record(s)"
)


def _resolve_repo_root_cli_path(*, raw_path: Path, field_name: str) -> Path:
    if raw_path.is_absolute():
        return raw_path.resolve()
    resolved = (REPO_ROOT / raw_path).resolve()
    if not resolved:
        raise RuntimeError(f"failed to resolve {field_name} against repo root: raw={raw_path}")
    return resolved


def _json_canonicalize(value: Any) -> Any:
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for raw_key, raw_val in sorted(value.items(), key=lambda item: (type(item[0]).__name__, str(item[0]))):
            key = raw_key if isinstance(raw_key, str) else str(raw_key)
            if key in out:
                raise ValueError(f"json canonicalization produced duplicate key after string conversion: {key!r}")
            out[key] = _json_canonicalize(raw_val)
        return out
    if isinstance(value, list):
        return [_json_canonicalize(item) for item in value]
    return value


class _HttpGetJsonTransientError(RuntimeError):
    pass
HTTP_DOWNLOAD_ATTEMPT_TIMEOUT_SECONDS = 10.0
HTTP_DOWNLOAD_RETRY_DEADLINE_SECONDS = 600.0
REMOTE_RUN_DIR_SYNC_REPLACE = "REPLACE"
REMOTE_RUN_DIR_SYNC_OVERLAY = "OVERLAY"

_ENDPOINT_SCHEME_HTTP = "HTTP"
_ENDPOINT_SCHEME_HTTPS = "HTTPS"

# Fluxon test stack benchmark modes (must match distributed_benchmark_* scripts).
TEST_STACK_MODE_MPMC = "MPMC"
TEST_STACK_MODE_KVSTORE = "KVSTORE"
TEST_STACK_MODE_KVSTORE_WITH_LOCAL_CACHE = "KVSTORE_WITH_LOCAL_CACHE"
TEST_STACK_MODE_PY_FS = "PY_FS"
TEST_STACK_MODE_RPC = "RPC"
TEST_STACK_KV_OWNER_INSTANCE_ID_PREFIX = "kv_owner_"
TEST_STACK_REDIS_INSTANCE_ID_PREFIX = "redis_node_"
TEST_STACK_ALLUXIO_INSTANCE_ID_PREFIX = "alluxio_node_"
TEST_STACK_MOONCAKE_MASTER_INSTANCE_ID = "mooncake_master"
TEST_STACK_BENCHMARK_FIXED_THREADS_PER_PROCESS = 4
# Owner-mode Fluxon KV configs in CI / TEST_STACK share the same canonical label.
FLUXON_KV_OWNER_SUB_CLUSTER = "owner"

TEST_STACK_BACKEND_FLUXON = "FLUXON"
TEST_STACK_BACKEND_REDIS = "REDIS"
TEST_STACK_BACKEND_ALLUXIO = "ALLUXIO"
TEST_STACK_BACKEND_MOONCAKE = "MOONCAKE"
TEST_STACK_BACKENDS_ALLOWED = {
    TEST_STACK_BACKEND_FLUXON,
    TEST_STACK_BACKEND_REDIS,
    TEST_STACK_BACKEND_ALLUXIO,
    TEST_STACK_BACKEND_MOONCAKE,
}

TEST_STACK_COMPLETION_STATUS_SUCCESS = "SUCCESS"
RUN_MODE_DEBUG_ONE_BY_ONE = "debug_one_by_one"
RUN_MODE_FULL_ONCE = "full_once"
RUN_SELECTOR_ALL = "ALL"

# Modes that require a KV master process to publish the master member record into etcd.
#
# English note:
# - Fluxon KV clients block waiting for the master member record at:
#   "/fluxon_kv_member_base/<cluster_name>/members/".
# - Fluxon TEST_STACK benchmark nodes run in external (zero-contribution) mode and rely on
#   per-host dedicated KV owners for shared-memory bootstrap and routing.
# - They still require a KV master because both dedicated owners and external clients depend on
#   the master member record for routing.
# - Without a KV master, nodes never reach READY and the coordinator will time out deterministically.
TEST_STACK_MODES_REQUIRE_KV_MASTER = (
    TEST_STACK_MODE_MPMC,
    TEST_STACK_MODE_KVSTORE,
    TEST_STACK_MODE_KVSTORE_WITH_LOCAL_CACHE,
    TEST_STACK_MODE_PY_FS,
    TEST_STACK_MODE_RPC,
)


def _test_stack_mode_requires_kv_master(mode: str) -> bool:
    return mode in TEST_STACK_MODES_REQUIRE_KV_MASTER

PAYLOAD_DELIVERY_KIND_FLUXON_FS_S3 = "FLUXON_FS_S3"
ARTIFACT_SOURCE_KIND_FLUXON_OPS_FS_S3 = "FLUXON_OPS_FS_S3"
ARTIFACT_SOURCE_KINDS = (
    ARTIFACT_SOURCE_KIND_FLUXON_OPS_FS_S3,
)
K8S_REF_KIND_DEPLOYMENT = "deployment"
K8S_REF_KIND_DAEMONSET = "daemonset"
OPS_AGENT_INSTANCE_KEY_PREFIX = "fluxon_ops_"
OPS_WORKLOAD_KIND_DEPLOYMENT = "Deployment"
OPS_WORKLOAD_KIND_DAEMONSET = "DaemonSet"
OPS_NAMESPACE_ANNOTATION_KEY = "fluxon.io/namespace"
OPS_NAMESPACE_DEFAULT = "default"
OPS_NAMESPACE_FLUXON_TEST_BED = "fluxon-testbed"
OPS_NAMESPACE_TEST_STACK_ENV = "FLUXON_TEST_STACK_OPS_NAMESPACE"

_FILE_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_MANIFEST_RELPATH_RE = re.compile(r"^[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*$")
_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}$")
_CASE_ID_RE = re.compile(
    r"^[a-z0-9][a-z0-9_.-]{0,63}(?:__[a-z0-9][a-z0-9_.-]{0,63}){2}$"
)
_DELETE_APPLY_RETRYABLE_ERRS = (
    "one or more agents failed to stop workload(s)",
    "stop rpc failed",
    "deadline has elapsed",
    "workloads may still be stopping",
)
_WAIT_DELETE_APPLY_REQUIRES_DELETE_ERR = "wait_delete_apply requires delete_apply first"
RUNNER_REPO_ROOT = Path(__file__).resolve().parent.parent
RUNNER_SHARED_RUNTIME_DIR = (RUNNER_REPO_ROOT / "fluxon_test_stack" / "test_runner").resolve()
RUNNER_SHARED_LOCK_DIR = (RUNNER_SHARED_RUNTIME_DIR / "locks").resolve()
RUNNER_STDIO_LOG_FILENAME = "test_runner.log"
_ACTIVE_TEST_BED_SELECTION_SUPERVISOR_CHECK_CACHE_KEY: Optional[str] = None

# TEST_STACK coordinator uses a stable workload name across cases; if a previous run crashed
# before teardown, subsequent runs can collide on this name/ports.
_TEST_STACK_COORD_WORKLOAD_NAME = "test_stack_coord"
_TEST_STACK_NODE_WORKLOAD_PREFIX = "test_stack_node__"
_TEST_STACK_EXTERNAL_SHARED_BUNDLE_CLEANUP_TIMEOUT_S = 120
_TEST_STACK_EXTERNAL_SHARED_BUNDLE_QUIET_PERIOD_S = 15.0
RUNNER_HELPER_ENTRYPOINTS = {
    "test_profile_adapter.py": (RUNNER_REPO_ROOT / "fluxon_test_stack" / "test_profile_adapter.py").resolve(),
}
_TEST_STACK_SHARED_VENV_SEEDED: set[str] = set()


def _test_stack_ops_namespace() -> str:
    raw = os.environ.get(OPS_NAMESPACE_TEST_STACK_ENV, "").strip()
    if not raw:
        return OPS_NAMESPACE_FLUXON_TEST_BED
    if _ID_RE.fullmatch(raw) is None:
        raise ValueError(
            f"{OPS_NAMESPACE_TEST_STACK_ENV} must match {_ID_RE.pattern!r}; got {raw!r}"
        )
    return raw

# Suite schema keeps scene as purpose+subject and pushes concrete sizing/topology into scale.
SCENE_SUBJECT_KV = "kv"
SCENE_SUBJECT_MQ = "mq"
SCENE_SUBJECT_FS = "fs"
SCENE_SUBJECT_RUST = "rust"
SCENE_SUBJECT_INFER = "infer"
SCENE_SUBJECTS_ALLOWED = {
    SCENE_SUBJECT_KV,
    SCENE_SUBJECT_MQ,
    SCENE_SUBJECT_FS,
    SCENE_SUBJECT_RUST,
    SCENE_SUBJECT_INFER,
}
TEST_STACK_REQUEST_DISTRIBUTION_UNIFORM = "uniform"
TEST_STACK_REQUEST_DISTRIBUTION_ZIPFIAN = "zipfian"
TEST_STACK_REQUEST_DISTRIBUTIONS_ALLOWED = {
    TEST_STACK_REQUEST_DISTRIBUTION_UNIFORM,
    TEST_STACK_REQUEST_DISTRIBUTION_ZIPFIAN,
}
TEST_STACK_SCENE_KEY_AFFINITY_LOCALITY_RATIO = "affinity_locality_ratio"
TEST_STACK_BENCHMARK_KEY_AFFINITY_SLOT_COUNT = "affinity_slot_count"
SCENE_ID_KV_READ_HEAVY_AFFINITY = "kv_read_heavy_affinity"
SCENE_ENUMS_ALLOWED = {
    "bench_mq",
    "fs_open_read_close_smallfiles",
    "fs_write_close_commit",
    "kv_read_heavy_zipf",
    SCENE_ID_KV_READ_HEAVY_AFFINITY,
    "kv_write_heavy_large_value",
    "rpc_echo_small_payload",
    "rpc_echo_small_payload_zerorpc",
    "ci_kv",
    "ci_mq",
    "ci_fs",
    "ci_rust",
    "ci_fs_s3",
}

TEST_STACK_RPC_BACKEND_FLUXON = "FLUXON"
TEST_STACK_RPC_BACKEND_ZERORPC = "ZERORPC"
TEST_STACK_RPC_BACKENDS_ALLOWED = {
    TEST_STACK_RPC_BACKEND_FLUXON,
    TEST_STACK_RPC_BACKEND_ZERORPC,
}
TEST_STACK_RPC_PAYLOAD_MODE_BYTES = "BYTES"
TEST_STACK_RPC_PAYLOAD_MODE_FLATDICT = "FLATDICT"
TEST_STACK_RPC_PAYLOAD_MODES_ALLOWED = {
    TEST_STACK_RPC_PAYLOAD_MODE_BYTES,
    TEST_STACK_RPC_PAYLOAD_MODE_FLATDICT,
}
TEST_STACK_RPC_TARGET_ROLE_SEED = "seed"
TEST_STACK_RPC_TARGET_ROLE_WORKER = "worker"
TEST_STACK_RPC_TARGET_ROLES_ALLOWED = {
    TEST_STACK_RPC_TARGET_ROLE_SEED,
    TEST_STACK_RPC_TARGET_ROLE_WORKER,
}
TEST_STACK_RPC_SERVER_SOURCE_BENCHMARK_NODE_ROLE = "benchmark_node_role"
TEST_STACK_RPC_SERVER_SOURCE_BENCHMARK_NODE_ALL = "benchmark_node_all"
TEST_STACK_RPC_SERVER_SOURCES_ALLOWED = {
    TEST_STACK_RPC_SERVER_SOURCE_BENCHMARK_NODE_ROLE,
    TEST_STACK_RPC_SERVER_SOURCE_BENCHMARK_NODE_ALL,
}
TEST_STACK_RPC_SCENE_KEYS = {
    "rpc_backend_kind",
    "rpc_path",
    "rpc_payload_size",
    "rpc_payload_mode",
    "rpc_server_source",
    "rpc_target_role",
    "zerorpc_port_base",
    "zerorpc_port_stride",
}

TEST_STACK_TOPOLOGY_DEFAULT = "DEFAULT"
TEST_STACK_START_TEST_BED_CONFIG_ENV = "FLUXON_TEST_STACK_START_TEST_BED_CONFIG"
_TEST_BED_AUTODISCOVERY_CANDIDATE_RELPATHS = (
    "generated/fluxon4_testbed_upstream/start_test_bed.runner_internal.yaml",
    "generated/fluxon4_testbed_upstream/start_test_bed.runner.yaml",
    "generated/fluxon4_testbed_upstream/start_test_bed.generated.yaml",
)

_LOADED_PY_MODULES: Dict[str, Any] = {}
_RUNNER_STDIO_LOG_FP: Optional[Any] = None
_RUNNER_STDIO_KEEPALIVE_FDS: Optional[Tuple[int, int]] = None
_TEST_RUNNER_UI_MAX_LOG_CHUNK_BYTES = 1024 * 1024
_TEST_RUNNER_UI_HISTORY_SCHEMA_VERSION = 1
_TEST_RUNNER_UI_DEFAULT_LOOKBACK_DAYS = 30
_TEST_RUNNER_UI_HISTORY_CACHE_TTL_SECONDS = 15
_TEST_RUNNER_UI_ACTIVE_RESERVED_GRACE_SECONDS = 7 * 86400
_TEST_RUNNER_UI_HISTORY_CACHE_LOCK = threading.Lock()
_TEST_RUNNER_UI_HISTORY_CACHE: Dict[str, Any] = {}
_TEST_RUNNER_UI_LOCK_FILENAME = ".test_runner_ui.lock"


def _redirect_process_stdio_to_log(workdir_root: Path) -> None:
    """Route runner stdio to a stable workdir log so long suites survive PTY loss.

    English note:
    - test_runner can run for hours under terminal/session wrappers that may disappear while the
      suite is still executing.
    - A deleted PTY turns ordinary `print(..., flush=True)` into `OSError(EIO)`, which aborts the
      runner in collect/finalize paths and leaves case_runs.yaml stuck at a reserved run.
    - Use a deterministic per-workdir log sink for the whole process, including child subprocesses.
    """
    global _RUNNER_STDIO_LOG_FP
    global _RUNNER_STDIO_KEEPALIVE_FDS
    if _RUNNER_STDIO_LOG_FP is not None:
        return

    log_path = (workdir_root / RUNNER_STDIO_LOG_FILENAME).resolve()
    log_fp = log_path.open("a", encoding="utf-8", buffering=1)
    banner = (
        f"[test_runner] redirecting process stdio to stable log: {log_path}\n"
    )
    try:
        sys.stdout.write(banner)
        sys.stdout.flush()
    except OSError:
        pass
    try:
        sys.stderr.write(banner)
        sys.stderr.flush()
    except OSError:
        pass

    try:
        sys.stdout.flush()
    except OSError:
        pass
    try:
        sys.stderr.flush()
    except OSError:
        pass

    # English note:
    # - Some process supervisors (including CI harnesses) treat "stdout/stderr pipe closed" as a signal
    #   to reap the whole process tree, even if the process is still alive.
    # - `dup2(log_fp, stdout_fd)` closes the original stdout pipe. Keep an extra dup() of the original
    #   pipe write end open so such harnesses do not mis-detect process exit.
    if _RUNNER_STDIO_KEEPALIVE_FDS is None:
        try:
            out_fd = os.dup(sys.stdout.fileno())
            err_fd = os.dup(sys.stderr.fileno())
            os.set_inheritable(out_fd, False)
            os.set_inheritable(err_fd, False)
            _RUNNER_STDIO_KEEPALIVE_FDS = (out_fd, err_fd)
        except OSError:
            _RUNNER_STDIO_KEEPALIVE_FDS = (-1, -1)

    os.dup2(log_fp.fileno(), sys.stdout.fileno())
    os.dup2(log_fp.fileno(), sys.stderr.fileno())
    sys.stdout = os.fdopen(sys.stdout.fileno(), "w", encoding="utf-8", buffering=1, closefd=False)
    sys.stderr = os.fdopen(sys.stderr.fileno(), "w", encoding="utf-8", buffering=1, closefd=False)
    _RUNNER_STDIO_LOG_FP = log_fp


def _resolve_history_roots_cli_paths(raw_paths: List[str]) -> List[Path]:
    return [
        _resolve_repo_root_cli_path(raw_path=Path(path), field_name="history_root")
        for path in raw_paths
    ]


def _load_gitops_ctx_for_ui(
    *,
    workdir_root: Path,
    gitops_config_path: Optional[Path],
) -> Optional[gitops_lib.GitOpsContext]:
    if gitops_config_path is None:
        return None
    gitops_workdir = gitops_lib.default_runtime_root(workdir_root)
    gitops_ctx = gitops_lib.load_context(
        config_path=gitops_config_path,
        workdir=gitops_workdir,
    )
    gitops_desc = gitops_lib.describe_context(gitops_ctx)
    print(
        "INFO: test_runner GitOps integrated: "
        f"config={gitops_desc['config_path']} workdir={gitops_desc['workdir']} interval={gitops_desc['interval']}s repos={gitops_desc['repo_count']}",
        flush=True,
    )
    threading.Thread(
        target=gitops_lib.poll_forever,
        args=(gitops_ctx,),
        kwargs={"stop_event": None},
        daemon=True,
    ).start()
    return gitops_ctx


def run_ui_service(
    *,
    workdir_root: Path,
    host: str,
    port: int,
    lookback_days: int,
    extra_history_roots: Optional[List[Path]],
    gitops_config_path: Optional[Path],
) -> None:
    workdir_root = workdir_root.resolve()
    if workdir_root.exists():
        if not workdir_root.is_dir():
            raise ValueError(f"ui workdir is not a directory: {workdir_root}")
    else:
        workdir_root.mkdir(parents=True, exist_ok=True)
    ui_lock = _acquire_ui_service_lock(workdir_root=workdir_root)
    _ = ui_lock
    gitops_ctx = _load_gitops_ctx_for_ui(
        workdir_root=workdir_root,
        gitops_config_path=gitops_config_path,
    )
    _serve_test_runner_ui(
        workdir_root=workdir_root,
        host=str(host),
        port=int(port),
        lookback_days=int(lookback_days),
        extra_history_roots=extra_history_roots,
        gitops_ctx=gitops_ctx,
    )


def main() -> None:
    # Treat SIGHUP as non-fatal for the bench runner.
    #
    # Causal chain:
    # - In some managed environments, long-running jobs can receive HUP due to session or supervisor restarts.
    # - If the runner exits on HUP, it leaves run_dir artifacts in the initial INCOMPLETE placeholder state,
    #   and the suite cannot converge without manual intervention.
    # - Ignoring HUP keeps the runner deterministic: failures must come from explicit errors, not terminal state.
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, signal.SIG_IGN)

    parser = argparse.ArgumentParser(
        description="Fluxon perf runner (Scene × Scale × Profile). Serial execution only."
    )
    parser.add_argument(
        "--config",
        "-c",
        required=False,
        help="Suite config YAML (required for --action run); if relative, resolve against the repo root inferred from this script path",
    )
    parser.add_argument(
        "--workdir",
        "-w",
        required=False,
        help="Work directory root; if relative, resolve against the repo root inferred from this script path",
    )
    parser.add_argument(
        "--action",
        choices=["run", "clean", "ui", "top_attention_list", "top_attention_run", "top_attention_quick"],
        help="clean deletes local workdir artifacts (case_runs.yaml/results/analysis); ui is deprecated in favor of test_runner_ui.py.",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Bind host for --action ui.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=18080,
        help="Bind port for --action ui.",
    )
    parser.add_argument(
        "--history-lookback-days",
        type=int,
        default=_TEST_RUNNER_UI_DEFAULT_LOOKBACK_DAYS,
        help="How many recent days of suite history to show for --action ui.",
    )
    parser.add_argument(
        "--history-root",
        dest="history_roots",
        action="append",
        default=[],
        help="Additional suite history root to scan for --action ui; may be passed multiple times.",
    )
    parser.add_argument(
        "--gitops-config",
        required=False,
        help="Optional GitOps config YAML. When set with --action ui, test_runner owns the GitOps poller/UI/API as part of the same service.",
    )
    parser.add_argument(
        "--top-attention-prefix",
        dest="top_attention_prefixes",
        action="append",
        default=[],
        help="Prefix filter for --action top_attention_list; may be passed multiple times.",
    )
    parser.add_argument(
        "--top-attention-all",
        action="store_true",
        help="List every top-attention entry for --action top_attention_list.",
    )
    parser.add_argument(
        "--top-attention-json",
        action="store_true",
        help="Emit JSON for --action top_attention_list.",
    )
    parser.add_argument(
        "--top-attention-requirements-only",
        action="store_true",
        help="Print only the requirement union for --action top_attention_list.",
    )
    args = parser.parse_args()

    action = args.action or "run"
    if action == "top_attention_list":
        prefixes = list(args.top_attention_prefixes)
        if args.top_attention_all:
            payload = collect_top_attention_payload(None)
        else:
            if not prefixes:
                print("ERROR: --top-attention-prefix is required unless --top-attention-all is set")
                raise SystemExit(2)
            payload = collect_top_attention_payload(prefixes)
        if args.top_attention_json:
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), flush=True)
            return
        print_top_attention_payload(payload, requirements_only=args.top_attention_requirements_only)
        return
    if action == "top_attention_run":
        prefixes = list(args.top_attention_prefixes)
        if args.top_attention_all:
            paths = list(iter_index_entry_paths())
        else:
            if not prefixes:
                print("ERROR: --top-attention-prefix is required unless --top-attention-all is set")
                raise SystemExit(2)
            paths = select_top_attention_entries(prefixes)
        rc = run_top_attention_entries(paths)
        raise SystemExit(rc)
    if action == "top_attention_quick":
        rc = run_top_attention_entries(iter_quick_entry_paths())
        raise SystemExit(rc)

    if args.workdir is None:
        print(f"ERROR: --workdir is required for --action {action}")
        raise SystemExit(2)
    workdir_root = _resolve_repo_root_cli_path(raw_path=Path(args.workdir), field_name="workdir")
    if workdir_root.exists():
        if not workdir_root.is_dir():
            print(f"ERROR: --workdir is not a directory: {workdir_root}")
            raise SystemExit(2)
    elif action in ("run", "ui"):
        workdir_root.mkdir(parents=True, exist_ok=True)
    else:
        return

    if action == "ui":
        print(
            "WARNING: test_runner.py --action ui is deprecated; use fluxon_test_stack/test_runner_ui.py",
            flush=True,
        )
        gitops_cfg_path = None
        if args.gitops_config:
            gitops_cfg_path = _resolve_repo_root_cli_path(
                raw_path=Path(args.gitops_config),
                field_name="gitops_config",
            )
        run_ui_service(
            workdir_root=workdir_root,
            host=str(args.host),
            port=int(args.port),
            lookback_days=int(args.history_lookback_days),
            extra_history_roots=_resolve_history_roots_cli_paths(args.history_roots),
            gitops_config_path=gitops_cfg_path,
        )
        return

    _ui_history_register_workdir(workdir_root)
    _redirect_process_stdio_to_log(workdir_root)

    if action == "clean":
        _clean_workdir(workdir_root)
        return

    if args.config is None:
        print("ERROR: --config is required for --action run")
        raise SystemExit(2)

    cfg_path = _resolve_repo_root_cli_path(raw_path=Path(args.config), field_name="config")
    if not cfg_path.exists():
        print(f"ERROR: --config not found: {cfg_path}")
        raise SystemExit(2)

    suite_cfg = _load_yaml_file(cfg_path)
    suite = _parse_suite_config(suite_cfg)
    resolved_cases = _expand_cases(suite)
    if not resolved_cases:
        print(
            "ERROR: no cases can be formed from scene constraints (scene.select.scales + scene.select.profiles).",
            flush=True,
        )
        raise SystemExit(2)
    if _suite_requires_benchmark_bundle(suite=suite, resolved_cases=resolved_cases):
        _require_test_bed_bundle_authority_for_benchmark(workdir_root=workdir_root)
    else:
        _maybe_autoconfigure_test_bed_bootstrap_config(
            anchor_paths=(workdir_root, cfg_path, Path.cwd()),
        )
    config_root = str(cfg_path.parent.resolve())
    stack_identity = _load_stack_identity(workdir_root=workdir_root)
    # Serialize runner state within one ops cluster namespace.
    #
    # Causal chain:
    # - Unrelated clusters must not block each other just because they share the same runner repo.
    # - Within the same ops cluster, extra runners would still reserve new run_index slots and leave
    #   INCOMPLETE run_dir artifacts when they exit early (e.g. due to broken stdout pipes).
    # - The per-controller lock remains the second barrier for same-controller collisions inside one cluster.
    _suite_lock_fp = _acquire_suite_lock(
        ops_cluster_name=_require_str(stack_identity.get("ops_cluster_name"), "stack_identity.ops_cluster_name"),
        controller_url=_require_str(stack_identity.get("controller_url"), "stack_identity.controller_url"),
    )
    _install_controller_basic_auth(
        stack_identity.get("controller_basic_auth"),
        field_name="stack_identity.controller_basic_auth",
    )

    case_runs_path = workdir_root / "case_runs.yaml"
    case_runs_preexisting = case_runs_path.exists()
    case_runs = _load_or_init_case_runs(case_runs_path)
    results_root = workdir_root / "results"
    results_root.mkdir(parents=True, exist_ok=True)
    repaired_case_ids = _repair_reserved_last_runs(case_runs, results_root=results_root)
    if repaired_case_ids:
        print(
            "INFO: repaired reserved last_run entries from run_dir artifacts: "
            + ", ".join(repaired_case_ids),
            flush=True,
        )
        _write_yaml_file(case_runs_path, case_runs)

    # case_runs.yaml is the single source of truth for execution state in the current suite workdir.
    # Keep it convergent when suite content evolves: prune any case history that is no longer
    # representable by the current suite schema (scene × scale × profile).
    suite_case_ids = {case.case_id for case in resolved_cases}
    removed = _prune_case_runs_to_case_ids(case_runs, case_ids=suite_case_ids)
    if removed:
        print(
            "INFO: pruned case history not in the current suite from case_runs.yaml: "
            f"removed={removed} kept={len(case_runs.get('cases', []))}",
            flush=True,
        )
        _write_yaml_file(case_runs_path, case_runs)

    added = _ensure_case_runs_include_all_suite_cases(case_runs, resolved_cases)
    if added:
        print(
            "INFO: initialized missing cases in case_runs.yaml from the current suite: "
            f"added={added} total={len(case_runs.get('cases', []))}",
            flush=True,
        )
        _write_yaml_file(case_runs_path, case_runs)

    scheduled = _build_execution_plan(suite, resolved_cases)

    if not scheduled:
        print("No runnable cases after applying run selectors.")
        return

    if not case_runs_preexisting and any(
        _case_family_from_scene_item(
            _require_dict(suite.scenes.get(planned.case.scene_id), f"suite.scenes[{planned.case.scene_id!r}]"),
            f"suite.scenes[{planned.case.scene_id!r}]",
        )
        == CASE_FAMILY_BENCH
        for planned in scheduled
    ):
        _ensure_stack_controller_online(stack_identity)
        controller_url = _require_str(stack_identity.get("controller_url"), "stack_identity.controller_url")
        _cleanup_bench_namespace_preflight(
            controller_url=controller_url,
            namespace=_test_stack_ops_namespace(),
        )

    _resume_reserved_runs(
        planned=scheduled,
        case_runs=case_runs,
        case_runs_path=case_runs_path,
        results_root=results_root,
    )

    suite_failed = False
    for planned_case in scheduled:
        case = planned_case.case
        if suite.run_mode == RUN_MODE_FULL_ONCE and planned_case.counted:
            run_map = _case_runs_map(case_runs)
            prev = run_map.get(case.case_id)
            if prev is not None:
                last_run = prev.get("last_run")
                if isinstance(last_run, dict) and last_run.get("outcome") == RUN_OUTCOME_SUCCESS:
                    print(
                        "SKIP: case already SUCCESS in case_runs.yaml: "
                        f"case_id={case.case_id} run_index={last_run.get('run_index')}",
                        flush=True,
                    )
                    # A skipped case must not leave its deploy-backed runtime hanging around,
                    # otherwise subsequent cases can collide with stable workload names/ports.
                    #
                    # Cleanup is best-effort but must converge: if we still see desired
                    # apply groups for this case_id after cleanup, we fail early instead of
                    # continuing in a poisoned test bed.
                    _cleanup_skipped_case_desired_applies(
                        controller_url=_require_str(stack_identity.get("controller_url"), "stack_identity.controller_url"),
                        case_id=case.case_id,
                    )
                    continue
        _bench_case_preflight_cleanup(
            stack_identity=stack_identity,
            planned_case=planned_case,
            suite=suite,
        )
        run_slot = _reserve_run_slot(case_runs, case, results_root=results_root)
        run_dir = results_root / case.case_id / f"run_{run_slot.run_index}"
        # Persist reservation early so interrupted runs do not lose their last_run reservation.
        _write_yaml_file(case_runs_path, case_runs)
        if run_dir.exists():
            print(f"ERROR: run_dir already exists: {run_dir}")
            raise SystemExit(1)
        run_dir.mkdir(parents=True, exist_ok=False)

        started_at = int(time.time())
        summary_path = run_dir / "summary.yaml"
        if not summary_path.exists():
            # Create an initial summary.yaml immediately, so every run_dir is observable even if
            # the runner is killed before reaching the finalize() path.
            _write_yaml_file(
                summary_path,
                {
                    "schema_version": SCHEMA_VERSION,
                    "case_id": case.case_id,
                    "case_key": case.case_key,
                    "run_index": int(run_slot.run_index),
                    # This will be overwritten in finalize. If it remains, treat as an interrupted run.
                    "outcome": RUN_OUTCOME_FAILED,
                    "counted": False,
                    "timing": {
                        "started_at_unix_s": int(started_at),
                        "finished_at_unix_s": int(started_at),
                    },
                    "error": _RUN_SUMMARY_INCOMPLETE_ERROR,
                },
            )
        infer_deploy_attempted = False
        runtime_tracking = _CaseRuntimeTracking()
        counted = False
        outcome = RUN_OUTCOME_FAILED
        finished_at = started_at
        fatal_stop_after_finalize = False
        resolved_case: Optional[Dict[str, Any]] = None
        case_family: Optional[str] = None
        case_plan: Optional[_CasePlan] = None
        case_error: Optional[str] = None
        finalize_error: Optional[str] = None

        try:
            resolved_case = _build_resolved_case_yaml(
                case,
                suite,
                config_root=config_root,
                workdir_root=str(workdir_root),
                run_dir=str(run_dir),
                ci_commands=planned_case.ci_commands,
                execution_label=planned_case.label,
                command_id=planned_case.command_id,
                test_id=planned_case.test_id,
                stack_identity=stack_identity,
            )
            case_family = _resolved_case_family(resolved_case)
            _ensure_deployer_online(resolved_case)
            test_stack_meta = _compile_case_runtime_artifacts(
                resolved_case,
                run_index=run_slot.run_index,
            )
            if _case_family_uses_case_plan(case_family):
                case_plan = _compile_case_plan(resolved_case)
            if case_family in (CASE_FAMILY_INFER, CASE_FAMILY_CI, CASE_FAMILY_BENCH):
                _apply_stable_deploy_names(resolved_case)
                _sync_case_runtime_model_from_deploy(resolved_case)

            _write_yaml_file(run_dir / "resolved_case.yaml", resolved_case)
            full_resolved_case_path = run_dir / "resolved_case_full.yaml"
            if not full_resolved_case_path.exists():
                _write_yaml_file(full_resolved_case_path, resolved_case)

            if case_plan is not None:
                _acquire_case_runtime_locks(
                    resolved_case,
                    run_dir=run_dir,
                    case_plan=case_plan,
                    runtime_tracking=runtime_tracking,
                )
            if case_family == CASE_FAMILY_INFER:
                _ensure_deployer_online(resolved_case)
                _write_deployer_manifests(resolved_case, run_dir, allow_overwrite=False)

                infer_deploy_attempted = True
                deploy_result = _run_adapter_action(
                    resolved_case, run_dir=run_dir, action="deploy"
                )
                _validate_deploy_result(resolved_case, deploy_result)

                endpoint_url = _resolved_endpoint_url(resolved_case, deploy_result)
                _tcp_check_endpoint(endpoint_url)

                infer_out = _run_infer_ai_perf(resolved_case, deploy_result, run_dir)
                summary = _build_infer_summary_yaml(
                    resolved_case,
                    deploy_result,
                    run_index=run_slot.run_index,
                    started_at_unix_s=started_at,
                    finished_at_unix_s=int(time.time()),
                    outcome=RUN_OUTCOME_SUCCESS,
                    counted=False,
                    ai_perf_out=infer_out,
                )
                _write_yaml_file(run_dir / "summary.yaml", summary)

                _run_adapter_action(
                    resolved_case, run_dir=run_dir, action="collect"
                )

                outcome = RUN_OUTCOME_SUCCESS


            elif _case_family_uses_case_plan(case_family):
                if case_plan is None:
                    raise ValueError(f"internal error: case_plan is missing for case_family={case_family}")
                prepared_case = _prepare_case(
                    planned_case,
                    resolved_case=resolved_case,
                    run_dir=run_dir,
                    run_index=run_slot.run_index,
                    case_plan=case_plan,
                    test_stack_meta=test_stack_meta,
                    runtime_tracking=runtime_tracking,
                )
                executed_case = _execute_case(
                    planned_case,
                    resolved_case=resolved_case,
                    run_dir=run_dir,
                    run_index=run_slot.run_index,
                    started_at=started_at,
                    prepared_case=prepared_case,
                    runtime_tracking=runtime_tracking,
                )
                outcome = executed_case.outcome
                _write_yaml_file(run_dir / "summary.yaml", executed_case.summary)

            else:
                raise ValueError(f"unsupported case family: {case_family}")

        except Exception as exc:
            # Keep the terminal error stable and record the full traceback for diagnosis.
            # This is not a fallback: the run still fails deterministically.
            try:
                (run_dir / _RUN_EXCEPTION_FILENAME).write_text(traceback.format_exc(), encoding="utf-8")
                case_error = f"{type(exc).__name__}: {exc} (see {_RUN_EXCEPTION_FILENAME})"
            except Exception as write_exc:  # noqa: BLE001
                case_error = f"{type(exc).__name__}: {exc} (failed to write {_RUN_EXCEPTION_FILENAME}: {type(write_exc).__name__}: {write_exc})"
            print(f"ERROR: case failed: case_id={case.case_id} err={case_error}")
            outcome = RUN_OUTCOME_FAILED

        finally:
            if case_family == CASE_FAMILY_INFER and resolved_case is not None:
                try:
                    if infer_deploy_attempted:
                        _run_adapter_action(
                            resolved_case,
                            run_dir=run_dir,
                            action="teardown",
                        )
                except Exception as exc:
                    print(
                        "ERROR: teardown failed; stopping (no fallback). "
                        f"case_id={case.case_id} err={type(exc).__name__}: {exc}"
                    )
                    raise SystemExit(1)
            if case_plan is not None and resolved_case is not None:
                try:
                    _finalize_case_runtime(
                        resolved_case,
                        run_dir=run_dir,
                        case_plan=case_plan,
                        runtime_tracking=runtime_tracking,
                        outcome=outcome,
                    )
                except Exception as exc:
                    finalize_error = f"{type(exc).__name__}: {exc}"
                    print(
                        "ERROR: teardown failed; stopping after finalize (no fallback). "
                        f"case_id={case.case_id} err={finalize_error}"
                    )
                    if case_family == CASE_FAMILY_BENCH and outcome == RUN_OUTCOME_SUCCESS:
                        print(
                            "WARN: TEST_STACK finalize failed after terminal benchmark success; "
                            f"preserving SUCCESS outcome for case_id={case.case_id} finalize_err={finalize_error}"
                        )
                    else:
                        outcome = RUN_OUTCOME_FAILED
                    if suite.run_mode == RUN_MODE_DEBUG_ONE_BY_ONE and outcome != RUN_OUTCOME_SUCCESS:
                        fatal_stop_after_finalize = True

            finished_at = int(time.time())
            _close_case_runtime_locks(runtime_tracking)
            # case_runs.yaml is execution history. Only a full case pass is counted.
            counted = outcome == RUN_OUTCOME_SUCCESS and planned_case.counted
            _finalize_run_slot(
                case_runs,
                run_slot,
                outcome=outcome,
                counted=counted,
                finished_at_unix_s=finished_at,
            )
            _write_yaml_file(case_runs_path, case_runs)

            summary_path = run_dir / "summary.yaml"
            try:
                if summary_path.exists():
                    s = _load_yaml_file(summary_path)
                    if isinstance(s, dict):
                        s["outcome"] = outcome
                        s["counted"] = bool(counted)
                        timing = s.get("timing")
                        if isinstance(timing, dict):
                            timing["finished_at_unix_s"] = finished_at
                        # English note:
                        # - We always create an initial summary.yaml placeholder early so run_dir is observable.
                        # - If the case fails before producing a richer family-specific summary, we must
                        #   overwrite the placeholder error with the real exception, otherwise the run looks
                        #   "INCOMPLETE" even though it reached finalize().
                        if case_error is not None:
                            s["error"] = case_error
                        if finalize_error is not None:
                            if case_family == CASE_FAMILY_BENCH:
                                test_stack_summary = s.get("test_stack")
                                if not isinstance(test_stack_summary, dict):
                                    test_stack_summary = {}
                                    s["test_stack"] = test_stack_summary
                                test_stack_summary["teardown_error"] = finalize_error
                            else:
                                s["teardown_error"] = finalize_error
                        _write_yaml_file(summary_path, s)
                else:
                    # Guarantee a terminal artifact even for early failures.
                    summary_obj = {
                        "schema_version": SCHEMA_VERSION,
                        "case_id": case.case_id,
                        "case_key": case.case_key,
                        "run_index": int(run_slot.run_index),
                        "outcome": outcome,
                        "counted": bool(counted),
                        "timing": {
                            "started_at_unix_s": int(started_at),
                            "finished_at_unix_s": int(finished_at),
                        },
                        "error": case_error,
                    }
                    if finalize_error is not None:
                        summary_obj["teardown_error"] = finalize_error
                    _write_yaml_file(
                        summary_path,
                        summary_obj,
                    )
            except Exception as exc:
                print(f"ERROR: failed to write/update summary.yaml: {exc}")
                raise SystemExit(1)

            if fatal_stop_after_finalize:
                raise SystemExit(1)

        if outcome != RUN_OUTCOME_SUCCESS:
            suite_failed = True
            # RUN_MODE_DEBUG_ONE_BY_ONE is intended for local iteration: stop at first failure.
            # RUN_MODE_FULL_ONCE should run the whole matrix so we can see every failing case
            # in one case_runs.yaml, then exit non-zero at the end.
            if suite.run_mode == RUN_MODE_DEBUG_ONE_BY_ONE:
                raise SystemExit(1)

    if suite_failed:
        raise SystemExit(1)


@dataclass(frozen=True)
class _Suite:
    run_mode: str
    run_selectors: "_RunSelectors"
    scenes: Dict[str, Dict[str, Any]]
    scales: Dict[str, Dict[str, Any]]
    artifact_sets: Dict[str, Dict[str, Any]]
    profiles: Dict[str, Dict[str, Any]]


@dataclass(frozen=True)
class _RunSelectors:
    case_ids: Optional[Tuple[str, ...]]
    profile_ids: Tuple[str, ...]
    command_ids: Optional[Tuple[str, ...]]
    test_ids: Optional[Tuple[str, ...]]


@dataclass(frozen=True)
class _ResolvedCase:
    scene_id: str
    scale_id: str
    profile_id: str
    case_id: str
    case_key: str


@dataclass
class _RunSlot:
    case_key: str
    case_id: str
    run_index: int
    rec: Dict[str, Any]


@dataclass(frozen=True)
class _PlannedCase:
    case: _ResolvedCase
    ci_commands: Optional[List[Dict[str, Any]]]
    label: str
    command_id: Optional[str]
    test_id: Optional[str]
    counted: bool


@dataclass(frozen=True)
class _ObservedFileState:
    size: int
    mtime_ns: int


@dataclass(frozen=True)
class _RemoteRunDirStage:
    archive_prefix: str
    stage_prefix: str
    verify_relpaths: Tuple[str, ...]
    ctx: str
    sync_mode: str
    include_relpaths: Optional[Tuple[str, ...]] = None


@dataclass(frozen=True)
class _RuntimePhase:
    phase_id: str
    layer: str
    instance_ids: Tuple[str, ...]
    write_ctx: str
    stage_run_dir: Optional[_RemoteRunDirStage] = None


class _RetryableControllerStatusError(RuntimeError):
    pass


@dataclass(frozen=True)
class _CasePlan:
    case_family: str
    prepare_phases: Tuple[_RuntimePhase, ...]
    execute_phases: Tuple[_RuntimePhase, ...]
    collect_phases: Tuple[_RuntimePhase, ...]


@dataclass(frozen=True)
class _PreparedCase:
    plan: _CasePlan
    ci_runner_exit_code_baseline: Optional[_ObservedFileState] = None
    test_stack_result_path: Optional[Path] = None
    test_stack_coordinator_addr: Optional[str] = None
    test_stack_result_timeout_s: Optional[int] = None


@dataclass(frozen=True)
class _ExecutedCase:
    outcome: str
    summary: Dict[str, Any]


@dataclass
class _CaseRuntimeTracking:
    ci_lock_fp: Optional[Any] = None
    controller_lock_fp: Optional[Any] = None
    ci_attempted_instance_ids: List[str] = field(default_factory=list)
    ci_apply_ids: Dict[str, str] = field(default_factory=dict)
    ts_coord_deploy_attempted: bool = False
    ts_nodes_deploy_attempted: bool = False
    ts_coord_apply_id: Optional[str] = None
    ts_nodes_apply_id: Optional[str] = None


def _load_yaml_file(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _load_yaml_file_if_present(path: Path, *, ctx: str) -> Optional[Any]:
    try:
        return _load_yaml_file(path)
    except FileNotFoundError:
        print(
            f"[{ctx}] yaml file disappeared during read; treat as already converged cleanup and skip: path={path}. "
            "This cleanup path only consumes preserved apply ids from an earlier failed CI run. "
            "That file is allowed to vanish after the earlier exists() check because another cleanup branch may already "
            "have removed it after converging the preserved runtime. In that situation the correct behavior is to stop "
            "re-reading stale preserved state and move on to the next previous run, not to fail the whole rerun for a "
            "file that no longer represents live desired state.",
            flush=True,
        )
        return None


def _write_yaml_file(path: Path, obj: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        yaml.safe_dump(
            obj,
            f,
            sort_keys=False,
            default_flow_style=False,
            allow_unicode=False,
        )
    tmp.replace(path)


def _load_run_dir_resolved_case(run_dir: Path) -> Dict[str, Any]:
    """Load the canonical resolved_case for a run_dir.

    English note:
    - `resolved_case.yaml` is overwritten per phase (deploy.instances filtered to the current phase),
      because the deployer adapter consumes it as its primary input.
    - Resume/repair logic must instead read a stable full-case view; we persist that as
      `resolved_case_full.yaml` once per run_dir.
    - Backward compatibility: older run_dirs may not have the full file; in that case we fall back
      to `resolved_case.yaml`.
    """
    full_path = (run_dir / "resolved_case_full.yaml").resolve()
    primary_path = full_path if full_path.exists() else (run_dir / "resolved_case.yaml").resolve()
    resolved_case = _require_dict(_load_yaml_file(primary_path), f"resolved_case {primary_path}")
    if primary_path == full_path:
        return resolved_case

    # English note:
    # - If we only have the per-phase resolved_case.yaml, it may contain only a subset of deploy.instances.
    # - Resume/repair paths need a full deploy.instance set. CI can be reconstructed deterministically
    #   from profile templates without touching the on-disk phase inputs.
    case_obj = resolved_case.get("case")
    if isinstance(case_obj, dict) and case_obj.get("family") == CASE_FAMILY_CI:
        _compile_ci_case(resolved_case)
        _apply_stable_deploy_names(resolved_case)
    return resolved_case


def _hostworkdir_suffix(path: str, *, field_name: str) -> str:
    prefix = "${HOSTWORKDIR}"
    if not path.startswith(prefix + "/"):
        raise ValueError(f"{field_name} must stay under {prefix}/: {path!r}")
    return path[len(prefix):]


def _resolve_stack_contract_path(
    hostworkdir: str,
    raw_path: str,
    *,
    field_name: str,
    allow_absolute: bool,
) -> str:
    prefix = "${HOSTWORKDIR}"
    if raw_path.startswith(prefix + "/"):
        return hostworkdir + raw_path[len(prefix):]
    if allow_absolute and raw_path.startswith("/"):
        return raw_path
    if allow_absolute:
        raise ValueError(
            f"{field_name} must be either an absolute path or stay under {prefix}/: {raw_path!r}"
        )
    raise ValueError(f"{field_name} must stay under {prefix}/: {raw_path!r}")


def _load_source_stack_contract() -> Dict[str, Any]:
    source_deployconf_path = _load_test_bed_deployconf_path()
    source_deployconf = _require_dict(
        _load_yaml_file(source_deployconf_path),
        f"bootstrap source deployconf {source_deployconf_path}",
    )
    cluster_nodes = _require_list(source_deployconf.get("cluster_nodes"), "bootstrap source deployconf.cluster_nodes")
    hostworkdirs: set[str] = set()
    for index, raw in enumerate(cluster_nodes):
        node = _require_dict(raw, f"bootstrap source deployconf.cluster_nodes[{index}]")
        hostworkdirs.add(_require_str(node.get("hostworkdir"), f"bootstrap source deployconf.cluster_nodes[{index}].hostworkdir"))
    if len(hostworkdirs) != 1:
        raise ValueError(
            "bootstrap source deployconf must use one shared hostworkdir across nodes for profile-scoped self-host stacks"
        )

    global_envs = _require_dict(source_deployconf.get("global_envs"), "bootstrap source deployconf.global_envs")
    cluster_name = _require_str(
        global_envs.get("FLUXON_CLUSTER_NAME"),
        "bootstrap source deployconf.global_envs.FLUXON_CLUSTER_NAME",
    )
    shared_memory_hostworkdir = _require_str(
        global_envs.get("FLUXON_SHARED_MEM"),
        "bootstrap source deployconf.global_envs.FLUXON_SHARED_MEM",
    )
    shared_file_hostworkdir = _require_str(
        global_envs.get("FLUXON_SHARED_MEM2"),
        "bootstrap source deployconf.global_envs.FLUXON_SHARED_MEM2",
    )
    _resolve_stack_contract_path(
        next(iter(hostworkdirs)),
        shared_memory_hostworkdir,
        field_name="bootstrap source deployconf.global_envs.FLUXON_SHARED_MEM",
        allow_absolute=True,
    )
    _resolve_stack_contract_path(
        next(iter(hostworkdirs)),
        shared_file_hostworkdir,
        field_name="bootstrap source deployconf.global_envs.FLUXON_SHARED_MEM2",
        allow_absolute=False,
    )

    source_bootstrap_cfg_path = _load_test_bed_bootstrap_config_path()
    source_bootstrap_cfg = _require_dict(
        _load_yaml_file(source_bootstrap_cfg_path),
        f"bootstrap source config {source_bootstrap_cfg_path}",
    )
    controller_basic_auth = _parse_controller_basic_auth(
        source_bootstrap_cfg.get("controller_basic_auth"),
        field_name="bootstrap source config.controller_basic_auth",
    )
    controller_url = _require_str(
        source_bootstrap_cfg.get("controller_url"),
        "bootstrap source config.controller_url",
    ).rstrip("/")
    controller_base_url, sep, controller_cluster_name = controller_url.rpartition("/")
    if not sep:
        raise ValueError(f"invalid bootstrap source controller_url: {controller_url!r}")
    if controller_cluster_name != cluster_name:
        raise ValueError(
            "bootstrap source controller_url cluster and deployconf FLUXON_CLUSTER_NAME must match: "
            f"controller_url={controller_url!r} cluster_name={cluster_name!r}"
        )

    return {
        "hostworkdir": next(iter(hostworkdirs)),
        # "cluster_name" here is the ops bed cluster name (from deployconf). The runner must not
        # reuse it for test workloads, otherwise we lose isolation and benchmarks collide with
        # long-lived ops members.
        "ops_cluster_name": cluster_name,
        # Keep the full URL as the single authority. It routes to ops bed namespace and is used by:
        # - /api/* controller operations (deploy/delete/status)
        # - /r/fs_s3/* proxy for downloading release artifacts
        "ops_controller_url": controller_url,
        "controller_basic_auth": controller_basic_auth,
        "shared_memory_hostworkdir": shared_memory_hostworkdir,
        "shared_file_hostworkdir": shared_file_hostworkdir,
    }


def _discover_test_bed_bootstrap_config_override_opt(*, anchor_paths: Tuple[Path, ...]) -> Optional[Path]:
    seen_roots: set[Path] = set()
    for anchor_path in anchor_paths:
        resolved_anchor = anchor_path.expanduser().resolve()
        search_root = resolved_anchor if resolved_anchor.is_dir() else resolved_anchor.parent
        for candidate_root in (search_root, *search_root.parents):
            candidate_root = candidate_root.resolve()
            if candidate_root in seen_roots:
                continue
            seen_roots.add(candidate_root)
            for relpath in _TEST_BED_AUTODISCOVERY_CANDIDATE_RELPATHS:
                candidate = (candidate_root / relpath).resolve()
                if candidate.exists() and candidate.is_file():
                    return candidate
    return None


def _maybe_autoconfigure_test_bed_bootstrap_config(*, anchor_paths: Tuple[Path, ...]) -> Optional[Path]:
    raw_override = os.environ.get(TEST_STACK_START_TEST_BED_CONFIG_ENV)
    if raw_override:
        return Path(raw_override).expanduser().resolve()
    discovered = _discover_test_bed_bootstrap_config_override_opt(anchor_paths=anchor_paths)
    if discovered is None:
        return None
    os.environ[TEST_STACK_START_TEST_BED_CONFIG_ENV] = str(discovered)
    print(f"INFO: auto-discovered {TEST_STACK_START_TEST_BED_CONFIG_ENV}={discovered}", flush=True)
    return discovered


def _require_path_within_root(*, path: Path, root: Path, ctx: str) -> Path:
    resolved_path = path.resolve()
    resolved_root = root.resolve()
    if resolved_path != resolved_root and resolved_root not in resolved_path.parents:
        raise ValueError(f"{ctx} escaped root: path={resolved_path} root={resolved_root}")
    return resolved_path


def _require_test_bed_bundle_authority_for_benchmark(*, workdir_root: Path) -> Path:
    raw_override = os.environ.get(TEST_STACK_START_TEST_BED_CONFIG_ENV, "").strip()
    if not raw_override:
        raise ValueError(
            "benchmark mode requires explicit FLUXON_TEST_STACK_START_TEST_BED_CONFIG; "
            "repo-root auto-discovery is not allowed"
        )
    bundle_root = (workdir_root / "testbed_bundle").resolve()
    if not bundle_root.exists() or not bundle_root.is_dir():
        raise ValueError(f"benchmark mode requires run-local testbed_bundle dir: {bundle_root}")
    override_path = Path(raw_override).expanduser()
    if not override_path.is_absolute():
        override_path = override_path.resolve()
    if not override_path.exists():
        raise ValueError(
            f"{TEST_STACK_START_TEST_BED_CONFIG_ENV} points to a missing file: {override_path}"
        )
    override_path = _require_path_within_root(
        path=override_path,
        root=bundle_root,
        ctx=f"{TEST_STACK_START_TEST_BED_CONFIG_ENV} benchmark bundle ownership",
    )
    cfg = _require_dict(
        _load_yaml_file(override_path),
        f"benchmark test bed bootstrap config {override_path}",
    )
    raw_deployconf_path = _require_str(cfg.get("deployconf_path"), "start_test_bed.deployconf_path")
    deployconf_path = Path(raw_deployconf_path)
    if not deployconf_path.is_absolute():
        deployconf_path = (override_path.parent / deployconf_path).resolve()
    if not deployconf_path.exists():
        raise ValueError(f"benchmark test bed deployconf_path not found: {deployconf_path}")
    deployconf_path = _require_path_within_root(
        path=deployconf_path,
        root=bundle_root,
        ctx="benchmark test bed deployconf bundle ownership",
    )
    deployconf = _require_dict(
        _load_yaml_file(deployconf_path),
        f"benchmark test bed deployconf {deployconf_path}",
    )
    mirror_outdir = Path(
        _require_str(
            deployconf.get("gen_k8s_daemonset_mirror_outdir"),
            "deployconf.gen_k8s_daemonset_mirror_outdir",
        )
    )
    if not mirror_outdir.is_absolute():
        mirror_outdir = (deployconf_path.parent / mirror_outdir).resolve()
    if not mirror_outdir.exists():
        raise ValueError(f"benchmark test bed mirror outdir not found: {mirror_outdir}")
    _require_path_within_root(
        path=mirror_outdir,
        root=bundle_root,
        ctx="benchmark test bed mirror outdir bundle ownership",
    )
    manifest_path = override_path.with_name("manifest.json")
    if not manifest_path.exists():
        raise ValueError(f"benchmark mode requires manifest.json beside start config: {manifest_path}")
    manifest_path = _require_path_within_root(
        path=manifest_path,
        root=bundle_root,
        ctx="benchmark test bed manifest bundle ownership",
    )
    try:
        manifest = _require_dict(
            json.loads(manifest_path.read_text(encoding="utf-8")),
            f"benchmark test bed manifest {manifest_path}",
        )
    except Exception as exc:
        raise ValueError(f"failed to load benchmark test bed manifest {manifest_path}: {exc}") from exc
    for field_name in ("deployconf_path", "start_config_path", "ssh_config_path", "workdir"):
        raw_field_path = _require_str(manifest.get(field_name), f"manifest.{field_name}")
        field_path = Path(raw_field_path).expanduser()
        if not field_path.is_absolute():
            field_path = (manifest_path.parent / field_path).resolve()
        if not field_path.exists():
            raise ValueError(f"benchmark test bed manifest path not found: field={field_name} path={field_path}")
        resolved_field_path = _require_path_within_root(
            path=field_path,
            root=bundle_root,
            ctx=f"benchmark test bed manifest {field_name} bundle ownership",
        )
        if field_name == "deployconf_path" and resolved_field_path != deployconf_path:
            raise ValueError(
                "benchmark test bed manifest deployconf_path mismatch: "
                f"manifest={resolved_field_path} start_cfg_resolved={deployconf_path}"
            )
        if field_name == "start_config_path" and resolved_field_path != override_path:
            raise ValueError(
                "benchmark test bed manifest start_config_path mismatch: "
                f"manifest={resolved_field_path} env={override_path}"
            )
    return override_path


def _load_test_bed_bootstrap_config_path() -> Path:
    raw_override = os.environ.get(TEST_STACK_START_TEST_BED_CONFIG_ENV)
    if raw_override:
        override_path = Path(raw_override).expanduser()
        if not override_path.is_absolute():
            override_path = override_path.resolve()
        if not override_path.exists():
            raise ValueError(
                f"{TEST_STACK_START_TEST_BED_CONFIG_ENV} points to a missing file: {override_path}"
            )
        return override_path
    return (_runner_repo_root() / "fluxon_test_stack" / "start_test_bed.yaml").resolve()


def _load_test_bed_deployconf_path() -> Path:
    cfg_path = _load_test_bed_bootstrap_config_path()
    cfg = _require_dict(_load_yaml_file(cfg_path), f"test bed bootstrap config {cfg_path}")
    raw = _require_str(cfg.get("deployconf_path"), "start_test_bed.deployconf_path")
    p = Path(raw)
    if not p.is_absolute():
        p = (cfg_path.parent / p).resolve()
    if not p.exists():
        raise ValueError(f"test bed deployconf_path not found: {p}")
    return p


def _load_test_bed_manifest_opt() -> Optional[Tuple[Path, Dict[str, Any]]]:
    manifest_path = _load_test_bed_bootstrap_config_path().with_name("manifest.json")
    if not manifest_path.exists():
        return None
    try:
        raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"failed to load test bed manifest {manifest_path}: {exc}") from exc
    manifest = _require_dict(raw, f"test bed manifest {manifest_path}")
    return manifest_path, manifest


def _test_bed_cluster_proxy_env() -> Dict[str, str]:
    manifest_info = _load_test_bed_manifest_opt()
    proxy_cfg: Dict[str, str] = {}
    if manifest_info is not None:
        _, manifest = manifest_info
        raw_cluster_proxy = manifest.get("cluster_proxy")
        if isinstance(raw_cluster_proxy, dict):
            for key in ("http_proxy", "https_proxy", "all_proxy", "no_proxy"):
                raw_value = raw_cluster_proxy.get(key)
                if raw_value is None:
                    raw_value = raw_cluster_proxy.get(key.upper())
                if raw_value is None:
                    continue
                proxy_cfg[key] = str(raw_value)
    if not proxy_cfg:
        deployconf_path = _load_test_bed_deployconf_path()
        deployconf = _require_dict(_load_yaml_file(deployconf_path), f"test bed deployconf {deployconf_path}")
        global_envs = _require_dict(deployconf.get("global_envs"), "test bed deployconf.global_envs")
        for key in ("http_proxy", "https_proxy", "all_proxy", "no_proxy"):
            raw_value = global_envs.get(f"FLUXON_CLUSTER_PROXY__{key.upper()}")
            if raw_value is None:
                continue
            proxy_cfg[key] = _require_str(
                raw_value,
                f"test bed deployconf.global_envs.FLUXON_CLUSTER_PROXY__{key.upper()}",
            )
    out: Dict[str, str] = {}
    for key, value in proxy_cfg.items():
        out[key] = value
        out[key.upper()] = value
    return out


def _render_env_exports(env_map: Dict[str, str]) -> str:
    if not env_map:
        return ""
    lines: List[str] = []
    for key in sorted(env_map):
        lines.append(f"export {key}={_shell_quote(env_map[key])}")
    return "\n".join(lines) + "\n"


def _load_test_bed_cluster_hostnames_by_ip_opt() -> Optional[Dict[str, List[str]]]:
    deployconf_path = _load_test_bed_deployconf_path()
    if not deployconf_path.exists():
        return None
    deployconf = _require_dict(_load_yaml_file(deployconf_path), f"test bed deployconf {deployconf_path}")
    raw_nodes = deployconf.get("cluster_nodes")
    if not isinstance(raw_nodes, list):
        return None
    out: Dict[str, List[str]] = {}
    for idx, raw_node in enumerate(raw_nodes):
        node = _require_dict(raw_node, f"deployconf.cluster_nodes[{idx}]")
        hostname = _require_str(node.get("hostname"), f"deployconf.cluster_nodes[{idx}].hostname")
        ip = _require_str(node.get("ip"), f"deployconf.cluster_nodes[{idx}].ip")
        out.setdefault(ip, []).append(hostname)
    for ip, names in out.items():
        out[ip] = sorted(names)
    return out


def _canonical_targets_for_ip_from_test_bed(node_ip: str) -> List[str]:
    by_ip = _load_test_bed_cluster_hostnames_by_ip_opt()
    if by_ip is None:
        return []
    return list(by_ip.get(node_ip, []))


def _test_bed_bundle_root_opt() -> Optional[Path]:
    manifest_info = _load_test_bed_manifest_opt()
    if manifest_info is None:
        return None
    manifest_path, _ = manifest_info
    return manifest_path.parent.resolve()


def _test_bed_bundle_manifest_required_path(*, field_name: str) -> Path:
    manifest_info = _load_test_bed_manifest_opt()
    if manifest_info is None:
        raise ValueError(f"test bed manifest is required to resolve {field_name}")
    manifest_path, manifest = manifest_info
    raw_path = _require_str(manifest.get(field_name), f"test bed manifest {manifest_path}.{field_name}")
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = (manifest_path.parent / path).resolve()
    else:
        path = path.resolve()
    if not path.exists():
        raise ValueError(f"test bed manifest path not found: field={field_name} path={path}")
    return path


def _test_bed_bundle_optional_path(*, relpath: str) -> Optional[Path]:
    bundle_root = _test_bed_bundle_root_opt()
    if bundle_root is None:
        return None
    candidate = (bundle_root / relpath).resolve()
    if not candidate.exists():
        return None
    return candidate


def _test_bed_controller_ready_timeout_seconds() -> int:
    cfg_path = _load_test_bed_bootstrap_config_path()
    cfg = _require_dict(_load_yaml_file(cfg_path), f"test bed bootstrap config {cfg_path}")
    return _require_int(
        cfg.get("controller_ready_timeout_seconds"),
        "start_test_bed.controller_ready_timeout_seconds",
        min_v=1,
    )


def _test_bed_generated_ssh_config_path_opt() -> Optional[Path]:
    manifest_info = _load_test_bed_manifest_opt()
    if manifest_info is None:
        return None
    return _test_bed_bundle_manifest_required_path(field_name="ssh_config_path")


def _test_bed_manifest_transport_ctx_opt() -> Optional[Dict[str, Any]]:
    manifest_info = _load_test_bed_manifest_opt()
    if manifest_info is None:
        return None
    manifest_path, manifest = manifest_info
    bastion = _require_dict(manifest.get("bastion"), f"test bed manifest {manifest_path}.bastion")
    bastion_user_raw = manifest.get("bastion_user")
    bastion_private_key_raw = manifest.get("bastion_private_key")
    bastion_password_raw = manifest.get("bastion_password")
    return {
        "manifest_path": manifest_path,
        "manifest": manifest,
        "bastion_name": _require_str(bastion.get("name"), f"test bed manifest {manifest_path}.bastion.name"),
        "bastion_host": _require_str(bastion.get("host"), f"test bed manifest {manifest_path}.bastion.host"),
        "bastion_port": _require_int(
            bastion.get("ssh_port"),
            f"test bed manifest {manifest_path}.bastion.ssh_port",
            min_v=1,
        ),
        "bastion_user": (
            "root"
            if bastion_user_raw is None or not str(bastion_user_raw).strip()
            else _require_str(bastion_user_raw, f"test bed manifest {manifest_path}.bastion_user")
        ),
        "bastion_private_key": (
            None
            if bastion_private_key_raw is None or not str(bastion_private_key_raw).strip()
            else str(Path(str(bastion_private_key_raw)).expanduser().resolve())
        ),
        "bastion_password": (
            None
            if bastion_password_raw is None
            else _require_str(bastion_password_raw, f"test bed manifest {manifest_path}.bastion_password")
        ),
    }


def _clean_ssh_stderr_text(text: str) -> str:
    if not text:
        return ""
    lines: List[str] = []
    for raw in text.splitlines():
        if any(raw.startswith(prefix) for prefix in _SSH_STDERR_NOISE_PREFIXES):
            continue
        lines.append(raw)
    return "\n".join(lines).strip()


def _controller_transport_manifest_opt(*, url: str) -> Optional[Dict[str, Any]]:
    manifest_info = _load_test_bed_manifest_opt()
    if manifest_info is None:
        return None
    manifest_path, manifest = manifest_info
    mode = _require_str(
        manifest.get("controller_request_mode"),
        f"testbed manifest {manifest_path}.controller_request_mode",
    )
    if mode != CONTROLLER_REQUEST_MODE_SSH_EXEC_PER_REQUEST:
        return None
    controller_url = _require_str(
        manifest.get("controller_url"),
        f"testbed manifest {manifest_path}.controller_url",
    ).rstrip("/")
    controller_public_url = _require_str(
        manifest.get("controller_public_url"),
        f"testbed manifest {manifest_path}.controller_public_url",
    ).rstrip("/")
    controller_cluster_url = str(manifest.get("controller_cluster_url") or "").rstrip("/")
    normalized_url = _require_str(url, "controller transport url").rstrip("/")
    allowed_prefixes = [controller_url, controller_public_url]
    if controller_cluster_url:
        allowed_prefixes.append(controller_cluster_url)
    if not any(normalized_url.startswith(prefix) for prefix in allowed_prefixes):
        return None
    return manifest


def _controller_request_exec_host(manifest: Dict[str, Any]) -> Tuple[str, Optional[str], Optional[int], Optional[str]]:
    raw_exec_host = manifest.get("controller_exec_host")
    if raw_exec_host is not None and str(raw_exec_host).strip():
        exec_host = _require_str(raw_exec_host, "testbed manifest.controller_exec_host")
        exec_user_raw = manifest.get("controller_exec_user")
        exec_port_raw = manifest.get("controller_exec_port")
        exec_password_raw = manifest.get("controller_exec_password")
        exec_user = None if exec_user_raw is None else _require_str(exec_user_raw, "testbed manifest.controller_exec_user")
        exec_port = None if exec_port_raw is None else _require_int(exec_port_raw, "testbed manifest.controller_exec_port", min_v=1)
        exec_password = (
            None
            if exec_password_raw is None
            else _require_str(exec_password_raw, "testbed manifest.controller_exec_password")
        )
        return exec_host, exec_user, exec_port, exec_password
    bastion = _require_dict(manifest.get("bastion"), "testbed manifest.bastion")
    return _require_str(bastion.get("host"), "testbed manifest.bastion.host"), None, None, None


def _controller_request_url_via_manifest(manifest: Dict[str, Any], *, url: str) -> str:
    request_parts = urllib.parse.urlsplit(url)
    exec_host, _, _, _ = _controller_request_exec_host(manifest)
    bastion = _require_dict(manifest.get("bastion"), "testbed manifest.bastion")
    bastion_host = _require_str(bastion.get("host"), "testbed manifest.bastion.host")
    local_base = ""
    if exec_host == bastion_host:
        local_base = str(manifest.get("controller_bastion_local_url") or "").strip()
    if not local_base:
        local_base = str(manifest.get("controller_cluster_url") or "").strip()
    if not local_base:
        local_base = _require_str(
            manifest.get("controller_bastion_local_url"),
            "testbed manifest.controller_bastion_local_url",
        )
    local_parts = urllib.parse.urlsplit(local_base)
    return urllib.parse.urlunsplit(
        (local_parts.scheme, local_parts.netloc, request_parts.path, request_parts.query, "")
    )


def _controller_request_via_manifest(
    req: urllib.request.Request,
    *,
    timeout_seconds: float,
) -> Optional[Tuple[int, bytes]]:
    manifest = _controller_transport_manifest_opt(url=str(req.full_url))
    if manifest is None:
        return None
    transport_ctx = _test_bed_manifest_transport_ctx_opt()
    if transport_ctx is None:
        raise ValueError("testbed transport manifest not found")
    exec_host, exec_user, exec_port, exec_password = _controller_request_exec_host(manifest)
    effective_url = _controller_request_url_via_manifest(manifest, url=str(req.full_url))
    headers_json = json.dumps(dict(req.header_items()), separators=(",", ":"))
    remote_script = (
        "import json, sys, urllib.error, urllib.request\n"
        "url, method, timeout_seconds, headers_json = sys.argv[1:5]\n"
        "headers = json.loads(headers_json)\n"
        "payload = sys.stdin.buffer.read()\n"
        "if payload == b'':\n"
        "    payload = None\n"
        "request = urllib.request.Request(url, data=payload, method=method)\n"
        "for key, value in headers.items():\n"
        "    request.add_header(key, value)\n"
        "try:\n"
        "    with urllib.request.urlopen(request, timeout=float(timeout_seconds)) as resp:\n"
        "        body = resp.read()\n"
        "        status = int(resp.status)\n"
        "except urllib.error.HTTPError as err:\n"
        "    body = err.read()\n"
        "    status = int(err.code)\n"
        "except Exception as exc:\n"
        "    print(json.dumps({'transport_error': f'{type(exc).__name__}: {exc}'}), file=sys.stderr)\n"
        "    sys.exit(0)\n"
        "sys.stdout.buffer.write(body)\n"
        "sys.stdout.buffer.flush()\n"
        "print(json.dumps({'status': status}), file=sys.stderr)\n"
    )
    remote_cmd = (
        "python3 -c "
        + _shell_quote(remote_script)
        + " "
        + _shell_quote(effective_url)
        + " "
        + _shell_quote(req.get_method())
        + " "
        + _shell_quote(str(float(timeout_seconds)))
        + " "
        + _shell_quote(headers_json)
    )
    argv = []
    effective_password = exec_password
    direct_bastion = exec_host == str(transport_ctx["bastion_host"])
    if direct_bastion and effective_password is None and transport_ctx.get("bastion_password") is not None:
        effective_password = str(transport_ctx["bastion_password"])
    if effective_password is not None:
        argv.extend(["sshpass", "-p", effective_password])
    argv.extend(
        [
            "ssh",
            "-o",
            "BatchMode=yes" if effective_password is None else "BatchMode=no",
            "-o",
            "StrictHostKeyChecking=accept-new",
            "-o",
            "ConnectTimeout=10",
        ]
    )
    if direct_bastion:
        argv.extend(
            [
                "-o",
                "HostKeyAlgorithms=+ssh-rsa",
                "-o",
                "PubkeyAcceptedAlgorithms=+ssh-rsa",
            ]
        )
        if transport_ctx["bastion_private_key"]:
            argv.extend(["-i", str(transport_ctx["bastion_private_key"]), "-o", "IdentitiesOnly=yes"])
    else:
        proxy_parts = []
        if transport_ctx.get("bastion_password"):
            proxy_parts.extend(["sshpass", "-p", str(transport_ctx["bastion_password"])])
        proxy_parts.extend(
            [
                "ssh",
                "-o",
                "StrictHostKeyChecking=accept-new",
                "-o",
                "ConnectTimeout=10",
                "-o",
                "HostKeyAlgorithms=+ssh-rsa",
                "-o",
                "PubkeyAcceptedAlgorithms=+ssh-rsa",
            ]
        )
        if transport_ctx["bastion_private_key"]:
            proxy_parts.extend(["-i", str(transport_ctx["bastion_private_key"]), "-o", "IdentitiesOnly=yes"])
        proxy_parts.extend(
            [
                "-p",
                str(transport_ctx["bastion_port"]),
                f"{transport_ctx['bastion_user']}@{transport_ctx['bastion_host']}",
                "-W",
                "%h:%p",
            ]
        )
        argv.extend(["-o", "ProxyCommand=" + " ".join(shlex.quote(str(part)) for part in proxy_parts)])
    if exec_port is not None:
        argv.extend(["-p", str(int(exec_port))])
    target = exec_host if exec_user is None else f"{exec_user}@{exec_host}"
    argv.extend([target, remote_cmd])
    try:
        completed = subprocess.run(
            argv,
            input=req.data if isinstance(req.data, bytes) else b"",
            capture_output=True,
            timeout=max(float(timeout_seconds) + 5.0, 15.0),
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise urllib.error.URLError(
            f"ssh controller request timed out: url={effective_url} timeout={timeout_seconds}"
        ) from exc
    stdout_bytes = completed.stdout
    stderr_text = _clean_ssh_stderr_text(completed.stderr.decode("utf-8", errors="replace"))
    if completed.returncode != 0:
        detail = stderr_text or stdout_bytes.decode("utf-8", errors="replace") or f"ssh exited with rc={completed.returncode}"
        raise urllib.error.URLError(f"ssh controller request failed: url={effective_url} detail={detail}")
    lines = [line for line in stderr_text.splitlines() if line.strip()]
    if not lines:
        raise ValueError(f"empty ssh controller response envelope: url={effective_url}")
    envelope = _require_dict(json.loads(lines[-1]), f"ssh controller response {effective_url}")
    transport_error = envelope.get("transport_error")
    if transport_error is not None:
        raise urllib.error.URLError(f"ssh controller transport error: url={effective_url} err={transport_error}")
    status_code = _require_int(envelope.get("status"), f"ssh controller response {effective_url}.status", min_v=100)
    return int(status_code), stdout_bytes


def _remote_ssh_common_argv() -> List[str]:
    argv = ["-o", "LogLevel=ERROR"]
    transport_ctx = _test_bed_manifest_transport_ctx_opt()
    if transport_ctx is not None:
        argv.extend(
            [
                "-o",
                "StrictHostKeyChecking=accept-new",
                "-o",
                "ConnectTimeout=10",
                "-o",
                "ProxyCommand="
                + " ".join(
                    shlex.quote(str(part))
                    for part in (
                        (["sshpass", "-p", str(transport_ctx["bastion_password"])] if transport_ctx.get("bastion_password") else [])
                        + [
                            "ssh",
                            "-o",
                            "StrictHostKeyChecking=accept-new",
                            "-o",
                            "ConnectTimeout=10",
                            "-o",
                            "HostKeyAlgorithms=+ssh-rsa",
                            "-o",
                            "PubkeyAcceptedAlgorithms=+ssh-rsa",
                        ]
                        + (
                            ["-i", str(transport_ctx["bastion_private_key"]), "-o", "IdentitiesOnly=yes"]
                            if transport_ctx["bastion_private_key"]
                            else []
                        )
                        + [
                            "-p",
                            str(transport_ctx["bastion_port"]),
                            f"{transport_ctx['bastion_user']}@{transport_ctx['bastion_host']}",
                            "-W",
                            "%h:%p",
                        ]
                    )
                ),
            ]
        )
        return argv
    argv.extend(
        [
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "ConnectTimeout=10",
        ]
    )
    return argv


def _suite_cluster_name_for_workdir(workdir_root: Path) -> str:
    """Return the fixed benchmark workload cluster namespace.

    English note:
    - Benchmark runs intentionally reuse one stable cluster namespace so shared-memory and
      etcd state stay under a predictable top-level path.
    - Isolation from the ops bed still comes from the explicit `cluster_name !=
      ops_cluster_name` guard in `_load_stack_identity`.
    """

    _ = workdir_root
    return "fluxon_benchmark"


def _cluster_scoped_shared_dir(*, root_path: str, cluster_name: str) -> Path:
    return (Path(root_path) / cluster_name).resolve()


def _shared_bundle_paths_for_cluster(
    *,
    shared_memory_root: str,
    shared_file_root: str,
    cluster_name: str,
) -> List[Path]:
    shared_memory_dir = _cluster_scoped_shared_dir(
        root_path=shared_memory_root,
        cluster_name=cluster_name,
    )
    shared_file_dir = _cluster_scoped_shared_dir(
        root_path=shared_file_root,
        cluster_name=cluster_name,
    )
    return [
        shared_file_dir / "shared.json",
        shared_memory_dir / "mmap.file",
    ]


def _owner_target_slug(*, owner_target: str, ctx: str) -> str:
    owner_target_str = _require_str(owner_target, ctx).strip().lower()
    slug = re.sub(r"[^a-z0-9_.-]+", "-", owner_target_str)
    if not slug:
        raise ValueError(f"{ctx} produces empty owner target slug: owner_target={owner_target!r}")
    return slug


def _owner_bundle_roots_for_target(
    *,
    shared_memory_root: str,
    shared_file_root: str,
    owner_target: str,
    ctx: str,
) -> Tuple[str, str]:
    owner_slug = _owner_target_slug(owner_target=owner_target, ctx=ctx)
    return (
        str((Path(shared_memory_root) / owner_slug).resolve()),
        str((Path(shared_file_root) / owner_slug).resolve()),
    )


def _owner_bundle_paths_for_target(
    *,
    shared_memory_root: str,
    shared_file_root: str,
    cluster_name: str,
    owner_target: str,
    ctx: str,
) -> List[Path]:
    owner_shared_memory_root, owner_shared_file_root = _owner_bundle_roots_for_target(
        shared_memory_root=shared_memory_root,
        shared_file_root=shared_file_root,
        owner_target=owner_target,
        ctx=ctx,
    )
    return _shared_bundle_paths_for_cluster(
        shared_memory_root=owner_shared_memory_root,
        shared_file_root=owner_shared_file_root,
        cluster_name=cluster_name,
    )


def _test_stack_owner_group_processes(
    *,
    scale: Dict[str, Any],
    owner_targets: List[str],
    target_ip_map: Dict[str, Any],
    processes_per_target: int,
    ctx: str,
) -> Optional[int]:
    benchmark = _require_dict(scale.get("benchmark"), f"{ctx}.benchmark")
    raw_group_processes = benchmark.get("owner_group_processes")
    if raw_group_processes is None:
        return None
    group_processes = _require_int(
        raw_group_processes,
        f"{ctx}.benchmark.owner_group_processes",
        min_v=1,
    )
    if processes_per_target % int(group_processes) != 0:
        raise ValueError(
            f"{ctx}.benchmark.owner_group_processes must divide processes_per_target: "
            f"group={group_processes} processes_per_target={processes_per_target}"
        )
    owner_machine_ips: set[str] = set()
    for idx, owner_target in enumerate(owner_targets):
        target_name = _require_str(owner_target, f"{ctx}.owner.targets[{idx}]")
        owner_machine_ips.add(
            _require_str(
                target_ip_map.get(target_name),
                f"resolved_case.deploy.target_ip_map[{target_name!r}]",
            )
        )
    if not owner_machine_ips:
        raise ValueError(f"{ctx}.owner.targets resolved to no machine IPs")
    owner_targets_per_machine = len(owner_targets) // len(owner_machine_ips)
    expected_owner_targets_per_machine = processes_per_target // int(group_processes)
    if owner_targets_per_machine != expected_owner_targets_per_machine:
        raise ValueError(
            f"{ctx}.benchmark.owner_group_processes implies unexpected owner fanout per machine: "
            f"group={group_processes} processes_per_target={processes_per_target} "
            f"owners_per_machine={owner_targets_per_machine} expected={expected_owner_targets_per_machine}"
        )
    return int(group_processes)


def _test_stack_owner_targets_by_machine(
    *,
    owner_targets: List[str],
    target_ip_map: Dict[str, Any],
    ctx: str,
) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for idx, owner_target in enumerate(owner_targets):
        target_name = _require_str(owner_target, f"{ctx}[{idx}]")
        node_ip = _require_str(
            target_ip_map.get(target_name),
            f"resolved_case.deploy.target_ip_map[{target_name!r}]",
        )
        out.setdefault(node_ip, []).append(target_name)
    for machine_owner_targets in out.values():
        machine_owner_targets.sort()
    return out


def _test_stack_owner_target_for_node_process(
    *,
    target: str,
    process_idx: int,
    owner_targets_by_machine: Dict[str, List[str]],
    target_ip_map: Dict[str, Any],
    owner_group_processes: Optional[int],
) -> Optional[str]:
    if owner_group_processes is None:
        return None
    node_ip = _require_str(
        target_ip_map.get(target),
        f"resolved_case.deploy.target_ip_map[{target!r}]",
    )
    machine_owner_targets = owner_targets_by_machine.get(node_ip)
    if not machine_owner_targets:
        raise ValueError(
            f"strict dual-owner routing requires owner targets for node: target={target!r} node_ip={node_ip!r}"
        )
    owner_index = int(process_idx) // int(owner_group_processes)
    if owner_index < 0 or owner_index >= len(machine_owner_targets):
        raise ValueError(
            "strict dual-owner routing produced out-of-range owner index: "
            f"target={target!r} process_idx={process_idx} owner_group_processes={owner_group_processes} "
            f"owner_index={owner_index} owners={machine_owner_targets}"
        )
    return machine_owner_targets[owner_index]


def _require_explicit_owner_group_processes_for_multi_owner_same_machine(
    *,
    owner_targets_by_machine: Dict[str, List[str]],
    owner_group_processes: Optional[int],
    uses_external_fluxon_kv: bool,
    ctx: str,
) -> None:
    if not uses_external_fluxon_kv or owner_group_processes is not None:
        return
    multi_owner_machines = {
        node_ip: targets
        for node_ip, targets in owner_targets_by_machine.items()
        if len(targets) > 1
    }
    if not multi_owner_machines:
        return
    raise ValueError(
        f"{ctx}.benchmark.owner_group_processes is required when external Fluxon KV owners share a machine: "
        f"machines={multi_owner_machines}. Without an explicit group size, benchmark nodes and owners "
        "silently reuse the same shared_memory_path/shared_file_path roots, which invalidates owner binding."
    )


def _load_stack_identity(*, workdir_root: Path) -> Dict[str, Any]:
    contract = _load_source_stack_contract()
    hostworkdir = _require_str(contract.get("hostworkdir"), "bootstrap_contract.hostworkdir")
    ops_cluster_name = _require_str(contract.get("ops_cluster_name"), "bootstrap_contract.ops_cluster_name")
    ops_controller_url = _require_str(contract.get("ops_controller_url"), "bootstrap_contract.ops_controller_url").rstrip("/")
    controller_basic_auth = _parse_controller_basic_auth(
        contract.get("controller_basic_auth"),
        field_name="bootstrap_contract.controller_basic_auth",
    )
    shared_mem_hostworkdir = _require_str(
        contract.get("shared_memory_hostworkdir"),
        "bootstrap_contract.shared_memory_hostworkdir",
    )
    shared_file_hostworkdir = _require_str(
        contract.get("shared_file_hostworkdir"),
        "bootstrap_contract.shared_file_hostworkdir",
    )
    cluster_name = _suite_cluster_name_for_workdir(workdir_root)
    if cluster_name == ops_cluster_name:
        raise ValueError(
            "test stack cluster_name must not equal ops cluster namespace: "
            f"cluster_name={cluster_name!r} ops={ops_cluster_name!r}"
        )
    return {
        # "ops_cluster_name" is the ops bed namespace that the controller URL routes to.
        # "cluster_name" is the test workload namespace (bench KV/MQ cluster membership).
        "ops_cluster_name": ops_cluster_name,
        "cluster_name": cluster_name,
        "controller_url": ops_controller_url,
        "controller_basic_auth": controller_basic_auth,
        "shared_memory_path": _resolve_stack_contract_path(
            hostworkdir,
            shared_mem_hostworkdir,
            field_name="bootstrap_contract.shared_memory_hostworkdir",
            allow_absolute=True,
        ),
        "shared_file_path": _resolve_stack_contract_path(
            hostworkdir,
            shared_file_hostworkdir,
            field_name="bootstrap_contract.shared_file_hostworkdir",
            allow_absolute=False,
        ),
    }


def _build_runtime_token_mapping(
    *,
    workdir_root: str,
    run_dir: str,
    release_root: str,
    test_rsc_root: str,
    case_id: str,
    profile_id: str,
    stack_identity: Dict[str, Any],
    extra_tokens: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    mapping = {
        "__WORKDIR_ROOT__": workdir_root,
        "__RUN_DIR__": run_dir,
        "__RELEASE_ROOT__": release_root,
        "__TEST_RSC_ROOT__": test_rsc_root,
        "__CASE_ID__": case_id,
        "__PROFILE_ID__": profile_id,
        "__STACK_CLUSTER_NAME__": _require_str(
            stack_identity.get("cluster_name"),
            "stack_identity.cluster_name",
        ),
        "__STACK_CONTROLLER_URL__": _require_str(
            stack_identity.get("controller_url"),
            "stack_identity.controller_url",
        ),
        "__STACK_SHARED_MEMORY_PATH__": _require_str(
            stack_identity.get("shared_memory_path"),
            "stack_identity.shared_memory_path",
        ),
        "__STACK_SHARED_FILE_PATH__": _require_str(
            stack_identity.get("shared_file_path"),
            "stack_identity.shared_file_path",
        ),
    }
    if extra_tokens is not None:
        for token_name, token_value in extra_tokens.items():
            mapping[f"__{token_name}__"] = token_value
    return mapping


def _is_runtime_token_placeholder(raw: str) -> bool:
    return raw.startswith("__") and raw.endswith("__") and len(raw) >= 4


def _resolved_case_runtime_token_mapping(resolved_case: Dict[str, Any]) -> Dict[str, str]:
    runtime = _require_dict(resolved_case.get("runtime"), "resolved_case.runtime")
    workdir_root = _require_str(runtime.get("workdir_root"), "resolved_case.runtime.workdir_root")
    run_dir = _require_str(runtime.get("run_dir"), "resolved_case.runtime.run_dir")
    stack_identity = _require_dict(runtime.get("stack_identity"), "resolved_case.runtime.stack_identity")
    case = _require_dict(resolved_case.get("case"), "resolved_case.case")
    artifact_set = _require_dict(resolved_case.get("artifact_set"), "resolved_case.artifact_set")
    release_root = _require_str(artifact_set.get("release_root"), "resolved_case.artifact_set.release_root")
    test_rsc_root = _require_str(artifact_set.get("test_rsc_root"), "resolved_case.artifact_set.test_rsc_root")
    extra_tokens: Dict[str, str] = {}
    profile = _require_dict(resolved_case.get("profile"), "resolved_case.profile")
    ci = profile.get("ci")
    if isinstance(ci, dict):
        command_tokens = ci.get("command_tokens")
        if isinstance(command_tokens, dict):
            for raw_token_name, raw_token_value in command_tokens.items():
                token_name = _require_str(
                    raw_token_name,
                    "resolved_case.profile.ci.command_tokens key",
                ).strip()
                extra_tokens[token_name] = _require_str(
                    raw_token_value,
                    f"resolved_case.profile.ci.command_tokens[{token_name!r}]",
                )
    return _build_runtime_token_mapping(
        workdir_root=workdir_root,
        run_dir=run_dir,
        release_root=release_root,
        test_rsc_root=test_rsc_root,
        case_id=_require_str(case.get("case_id"), "resolved_case.case.case_id"),
        profile_id=_require_str(case.get("profile_id"), "resolved_case.case.profile_id"),
        stack_identity=stack_identity,
        extra_tokens=extra_tokens,
    )


def _rewrite_cli_option_value(
    argv: List[Any],
    *,
    option_names: Tuple[str, ...],
    value: str,
    field_name: str,
) -> List[str]:
    rewritten = [_require_str(raw, f"{field_name}[]") for raw in argv]
    matched = False
    for idx, token in enumerate(rewritten):
        if token not in option_names:
            continue
        if idx + 1 >= len(rewritten):
            raise ValueError(f"{field_name} is missing a value after {token}")
        rewritten[idx + 1] = value
        matched = True
    if not matched:
        joined = "/".join(option_names)
        raise ValueError(f"{field_name} must include {joined}")
    return rewritten


def _subst_runtime_tokens(resolved_case: Dict[str, Any], s: str) -> str:
    out = s
    mapping = _resolved_case_runtime_token_mapping(resolved_case)
    if "__RELEASE_ROOT__" in mapping:
        release_root = mapping["__RELEASE_ROOT__"]
        if not Path(release_root).is_absolute():
            raise ValueError("deploy.release_root must be an absolute path")
    for token, value in mapping.items():
        out = out.replace(token, value)
    return out


def _resolve_runtime_tokens_nested(resolved_case: Dict[str, Any], obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _resolve_runtime_tokens_nested(resolved_case, v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_runtime_tokens_nested(resolved_case, v) for v in obj]
    if isinstance(obj, str):
        return _subst_runtime_tokens(resolved_case, obj)
    return obj


def _stable_runtime_identity(resolved_case: Dict[str, Any]) -> str:
    case = _require_dict(resolved_case.get("case"), "resolved_case.case")
    runtime = _require_dict(resolved_case.get("runtime"), "resolved_case.runtime")
    run_dir = _require_str(runtime.get("run_dir"), "runtime.run_dir")
    run_scope = hashlib.sha256(run_dir.encode("utf-8")).hexdigest()[:12]

    # English note:
    # - TEST_STACK instance_key must be run_dir-scoped (not just case_id-scoped) so reruns inside the
    #   same suite cluster namespace do not collide on etcd member keys.
    # - run_dir is stable for resume within the same run, but changes across reruns/workdirs.
    parts = [_require_str(case.get("case_id"), "case.case_id"), run_scope]
    command_id_raw = case.get("command_id")
    if command_id_raw is not None:
        parts.append(_require_str(command_id_raw, "case.command_id"))
    test_id_raw = case.get("test_id")
    if test_id_raw is not None:
        parts.append(_require_str(test_id_raw, "case.test_id"))
    return "__".join(parts)


def _ci_cluster_name(resolved_case: Dict[str, Any]) -> str:
    case = _require_dict(resolved_case.get("case"), "resolved_case.case")
    runtime = _require_dict(resolved_case.get("runtime"), "resolved_case.runtime")
    case_id = _require_str(case.get("case_id"), "case.case_id")
    run_dir = _require_str(runtime.get("run_dir"), "runtime.run_dir")
    run_scope = hashlib.sha256(run_dir.encode("utf-8")).hexdigest()[:12]
    return f"fluxon-ci-{case_id}-{run_scope}"


def _logical_deploy_identity(resolved_case: Dict[str, Any]) -> str:
    case = _require_dict(resolved_case.get("case"), "resolved_case.case")
    case_id = _require_str(case.get("case_id"), "case.case_id")
    if _resolved_case_kind(resolved_case) != SCENE_KIND_TEST_STACK:
        return case_id
    runtime = _require_dict(resolved_case.get("runtime"), "resolved_case.runtime")
    run_dir = _require_str(runtime.get("run_dir"), "runtime.run_dir")
    run_scope = hashlib.sha256(run_dir.encode("utf-8")).hexdigest()[:12]
    return f"{case_id}__{run_scope}"


def _resolved_case_case_id(resolved_case: Dict[str, Any]) -> str:
    case = _require_dict(resolved_case.get("case"), "resolved_case.case")
    return _require_str(case.get("case_id"), "case.case_id")



def _resolved_case_ops_namespace(resolved_case: Dict[str, Any]) -> str:
    scene = _require_dict(resolved_case.get("scene"), "resolved_case.scene")
    if scene.get("test_stack") is not None:
        return _test_stack_ops_namespace()
    return OPS_NAMESPACE_DEFAULT



def _apply_stable_deploy_names(resolved_case: Dict[str, Any]) -> None:
    """Rewrite deploy.instances[].k8s_ref into a stable logical deployment name.

    For CI/infer, replacement semantics follow the logical case identity and stay rerun-stable.
    For TEST_STACK benchmark workloads, names are additionally scoped by run_dir hash so a stale
    controller/runtime from an older runner cannot collide with the current run.
    """
    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    instances = _require_list(deploy.get("instances"), "resolved_case.deploy.instances")
    prefix = _logical_deploy_identity(resolved_case)

    for raw in instances:
        inst = _require_dict(raw, "deploy.instances[]")
        instance_id = _require_str(inst.get("id"), "deploy.instances[].id")
        k8s_ref = _require_str(inst.get("k8s_ref"), "deploy.instances[].k8s_ref")
        if "/" not in k8s_ref:
            raise ValueError(f"deploy.instances[].k8s_ref must be <deployment|daemonset>/<name>, got: {k8s_ref!r}")
        kind, base_name = k8s_ref.split("/", 1)
        if kind not in (K8S_REF_KIND_DEPLOYMENT, K8S_REF_KIND_DAEMONSET):
            raise ValueError(
                f"deploy.instances[].k8s_ref kind must be deployment or daemonset, got: {k8s_ref!r}"
            )
        if not base_name.strip():
            raise ValueError(f"deploy.instances[].k8s_ref name must be non-empty, got: {k8s_ref!r}")
        inst["k8s_ref"] = f"{kind}/{prefix}__{instance_id}"



def _resolved_case_kind(resolved_case: Dict[str, Any]) -> str:
    scene = _require_dict(resolved_case.get("scene"), "resolved_case.scene")
    if scene.get("infer") is not None:
        return SCENE_KIND_INFER
    if scene.get("ci") is not None:
        return SCENE_KIND_CI
    if scene.get("test_stack") is not None:
        return SCENE_KIND_TEST_STACK
    raise ValueError("resolved_case.scene must contain exactly one supported scene kind")


def _resolved_case_family(resolved_case: Dict[str, Any]) -> str:
    case = _require_dict(resolved_case.get("case"), "resolved_case.case")
    family = _require_str(case.get("family"), "resolved_case.case.family")
    if family not in (CASE_FAMILY_INFER, CASE_FAMILY_CI, CASE_FAMILY_BENCH):
        raise ValueError(f"resolved_case.case.family unsupported: {family!r}")
    return family


def _ci_case_instance_ids(resolved_case: Dict[str, Any]) -> Tuple[str, ...]:
    return tuple(_runtime_layer_instance_ids(resolved_case, layer=RUNTIME_LAYER_CASE))


def _ci_cluster_runtime_instance_ids(resolved_case: Dict[str, Any]) -> Tuple[str, ...]:
    return tuple(instance_id for instance_id in _ci_case_instance_ids(resolved_case) if instance_id != "ci_runner")


def _ci_has_instance(resolved_case: Dict[str, Any], *, instance_id: str) -> bool:
    return instance_id in set(_ci_case_instance_ids(resolved_case))


def _ci_runtime_contract_id(resolved_case: Dict[str, Any]) -> str:
    scene = _require_dict(resolved_case.get("scene"), "resolved_case.scene")
    ci = _require_dict(scene.get("ci"), "resolved_case.scene.ci")
    return _require_ci_runtime_contract(
        ci.get("runtime_contract"),
        "resolved_case.scene.ci.runtime_contract",
    )


def _case_family_id(case_kind: str) -> str:
    if case_kind == SCENE_KIND_INFER:
        return CASE_FAMILY_INFER
    if case_kind == SCENE_KIND_CI:
        return CASE_FAMILY_CI
    if case_kind == SCENE_KIND_TEST_STACK:
        return CASE_FAMILY_BENCH
    raise ValueError(f"unsupported case kind for family mapping: {case_kind}")


def _case_family_from_scene_item(item: Dict[str, Any], ctx: str) -> str:
    return _case_family_id(_scene_kind_from_item(item, ctx))


def _case_family_uses_case_plan(case_family: str) -> bool:
    return case_family in (CASE_FAMILY_CI, CASE_FAMILY_BENCH)


def _acquire_case_runtime_locks(
    resolved_case: Dict[str, Any],
    *,
    run_dir: Path,
    case_plan: _CasePlan,
    runtime_tracking: _CaseRuntimeTracking,
) -> None:
    runtime_tracking.controller_lock_fp = _acquire_controller_lock(
        resolved_case,
        case_family=case_plan.case_family,
        run_dir=run_dir,
    )


def _close_case_runtime_locks(runtime_tracking: _CaseRuntimeTracking) -> None:
    if runtime_tracking.ci_lock_fp is not None:
        runtime_tracking.ci_lock_fp.close()
        runtime_tracking.ci_lock_fp = None
    if runtime_tracking.controller_lock_fp is not None:
        runtime_tracking.controller_lock_fp.close()
        runtime_tracking.controller_lock_fp = None


def _build_runtime_model(case_family: str) -> Dict[str, Any]:
    if case_family == CASE_FAMILY_CI:
        case_instance_ids = list(CI_RUNTIME_LAYER_INSTANCE_IDS[RUNTIME_LAYER_CASE])
    elif case_family in (CASE_FAMILY_INFER, CASE_FAMILY_BENCH):
        case_instance_ids = []
    else:
        raise ValueError(f"unsupported runtime model case family: {case_family}")
    model = {
        RUNTIME_LAYER_TEST_BED: {"kind": "ops"},
        RUNTIME_LAYER_BASE: {},
        RUNTIME_LAYER_CASE: {"instance_ids": case_instance_ids},
    }
    if case_family == CASE_FAMILY_CI:
        model[RUNTIME_LAYER_BASE]["service_ids"] = list(CI_BASE_RUNTIME_SERVICE_IDS)
    return model


def _runtime_model(resolved_case: Dict[str, Any]) -> Dict[str, Any]:
    model = _require_dict(resolved_case.get("runtime_model"), "resolved_case.runtime_model")
    for layer in RUNTIME_LAYER_ORDER:
        _ = _require_dict(model.get(layer), f"resolved_case.runtime_model[{layer!r}]")
    return model


def _runtime_layer_instance_ids(resolved_case: Dict[str, Any], *, layer: str) -> List[str]:
    if layer not in RUNTIME_LAYER_ORDER:
        raise ValueError(f"unsupported runtime layer: {layer!r}")
    model = _runtime_model(resolved_case)
    layer_obj = _require_dict(model.get(layer), f"resolved_case.runtime_model[{layer!r}]")
    raw_instance_ids = layer_obj.get("instance_ids")
    if raw_instance_ids is None:
        return []
    return [
        _require_str(raw_instance_id, f"resolved_case.runtime_model[{layer!r}].instance_ids[]")
        for raw_instance_id in _require_list(raw_instance_ids, f"resolved_case.runtime_model[{layer!r}].instance_ids")
    ]


def _runtime_layer_service_ids(resolved_case: Dict[str, Any], *, layer: str) -> List[str]:
    if layer not in RUNTIME_LAYER_ORDER:
        raise ValueError(f"unsupported runtime layer: {layer!r}")
    model = _runtime_model(resolved_case)
    layer_obj = _require_dict(model.get(layer), f"resolved_case.runtime_model[{layer!r}]")
    raw_service_ids = layer_obj.get("service_ids")
    if raw_service_ids is None:
        return []
    return [
        _require_str(raw_service_id, f"resolved_case.runtime_model[{layer!r}].service_ids[]")
        for raw_service_id in _require_list(raw_service_ids, f"resolved_case.runtime_model[{layer!r}].service_ids")
    ]


def _set_runtime_layer_instance_ids(
    resolved_case: Dict[str, Any],
    *,
    layer: str,
    instance_ids: List[str],
) -> None:
    if layer not in RUNTIME_LAYER_ORDER:
        raise ValueError(f"unsupported runtime layer: {layer!r}")
    model = _runtime_model(resolved_case)
    layer_obj = _require_dict(model.get(layer), f"resolved_case.runtime_model[{layer!r}]")
    layer_obj["instance_ids"] = [
        _require_str(instance_id, f"resolved_case.runtime_model[{layer!r}].instance_ids[]")
        for instance_id in instance_ids
    ]


def _runtime_case_instance_ids_from_deploy(resolved_case: Dict[str, Any]) -> List[str]:
    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    raw_instances = deploy.get("instances")
    if raw_instances is None:
        return []
    raw_instances = _require_list(raw_instances, "resolved_case.deploy.instances")
    instance_ids: List[str] = []
    for index, raw_instance in enumerate(raw_instances):
        instance = _require_dict(raw_instance, f"resolved_case.deploy.instances[{index}]")
        instance_ids.append(_require_str(instance.get("id"), f"resolved_case.deploy.instances[{index}].id"))
    return instance_ids


def _sync_case_runtime_model_from_deploy(resolved_case: Dict[str, Any]) -> None:
    _set_runtime_layer_instance_ids(
        resolved_case,
        layer=RUNTIME_LAYER_CASE,
        instance_ids=_runtime_case_instance_ids_from_deploy(resolved_case),
    )


def _compile_case_runtime_artifacts(
    resolved_case: Dict[str, Any],
    *,
    run_index: int,
) -> Optional[Dict[str, Any]]:
    _prepare_case_release_inputs(resolved_case)
    case_family = _resolved_case_family(resolved_case)
    if case_family == CASE_FAMILY_CI:
        _compile_ci_case(resolved_case)
        _sync_case_runtime_model_from_deploy(resolved_case)
        return None
    if case_family == CASE_FAMILY_BENCH:
        test_stack_meta = _compile_test_stack_case(resolved_case, run_index=run_index)
        _sync_case_runtime_model_from_deploy(resolved_case)
        return test_stack_meta
    if case_family == CASE_FAMILY_INFER:
        _sync_case_runtime_model_from_deploy(resolved_case)
        return None
    raise ValueError(f"unsupported case family for runtime artifact compilation: {case_family}")


def _prepare_case_release_inputs(resolved_case: Dict[str, Any]) -> None:
    """Materialize the run-scoped release as the first prepare substep.

    English note:
    - `artifact_set.release_source` is the release acquisition contract.
    - CI cases and baseline-backed TEST_STACK cases also consume `artifact_set.test_rsc_source`.
    - The runner must finish artifact download/copy/verification before any case-local compile step.
    - Later execution consumes only run-scoped materialized inputs (`run_dir/fluxon_release` and,
      when required, `run_dir/test_rsc`), never host-global ad hoc paths and never test-script `curl` logic.
    """
    _ensure_case_release_ready(resolved_case)
    if _resolved_case_uses_test_rsc(resolved_case):
        _ensure_case_test_rsc_ready(resolved_case)


def _resolved_case_uses_test_rsc(resolved_case: Dict[str, Any]) -> bool:
    case_family = _resolved_case_family(resolved_case)
    if case_family == CASE_FAMILY_CI:
        return True
    return case_family == CASE_FAMILY_BENCH


def _write_deployer_manifests(resolved_case: Dict[str, Any], run_dir: Path, *, allow_overwrite: bool) -> None:
    """Generate fluxon_deployer-compatible Deployment-subset YAML.

    This file is a run artifact written by the runner.
    """
    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    instances = _require_list(deploy.get("instances"), "resolved_case.deploy.instances")

    out_path = run_dir / "deployer_deploy.yaml"
    if out_path.exists() and not allow_overwrite:
        raise ValueError(f"deployer_deploy.yaml already exists (no overwrite): {out_path}")

    # English note: deployer does not distribute files; payload delivery is owned by the workload entrypoint.
    #
    # Causal chain:
    # - Deployer is a lifecycle controller (start/status/stop/log), not a file transfer service.
    # - Bench cases may still need to place artifacts (wheels/models/configs) on target nodes.
    # - We support that by generating an entrypoint wrapper that downloads the payload (Fluxon FS S3 gateway)
    #   and then `exec`s the original command.
    #
    # No defaults:
    # - If any instance declares payload_file/payload_dest_path, deploy.payload_delivery must be explicitly set.
    any_payload = False
    for raw in instances:
        inst = _require_dict(raw, "deploy.instances[]")
        deployer = _require_dict(inst.get("deployer"), "deploy.instances[].deployer")
        if deployer.get("payload_file") is not None or deployer.get("payload_dest_path") is not None:
            any_payload = True
            break

    payload_s3_base_url: Optional[str] = None
    payload_s3_bucket: Optional[str] = None
    payload_s3_access_key: Optional[str] = None
    payload_s3_secret_key: Optional[str] = None
    payload_s3_region: Optional[str] = None
    payload_s3_key_prefix: Optional[str] = None

    if any_payload:
        pd = _require_dict(deploy.get("payload_delivery"), "deploy.payload_delivery")
        _forbid_unknown_keys(
            pd,
            {"kind", "s3_base_url", "bucket", "access_key", "secret_key", "region", "key_prefix"},
            "deploy.payload_delivery",
        )
        kind = _require_str(pd.get("kind"), "deploy.payload_delivery.kind")
        if kind != PAYLOAD_DELIVERY_KIND_FLUXON_FS_S3:
            raise ValueError(
                f"deploy.payload_delivery.kind must be {PAYLOAD_DELIVERY_KIND_FLUXON_FS_S3!r}, got: {kind!r}"
            )
        base_url = _require_str(pd.get("s3_base_url"), "deploy.payload_delivery.s3_base_url").rstrip("/")
        u = urlparse(base_url)
        if u.scheme not in ("http", "https"):
            raise ValueError("deploy.payload_delivery.s3_base_url must start with http:// or https://")
        if not u.netloc:
            raise ValueError("deploy.payload_delivery.s3_base_url must include host:port")
        if not u.path or u.path == "/":
            raise ValueError("deploy.payload_delivery.s3_base_url must include a non-root path prefix (e.g. /fs_s3)")
        bucket = _require_str(pd.get("bucket"), "deploy.payload_delivery.bucket")
        access_key = _require_str(pd.get("access_key"), "deploy.payload_delivery.access_key")
        secret_key = _require_str(pd.get("secret_key"), "deploy.payload_delivery.secret_key")
        region = _require_str(pd.get("region"), "deploy.payload_delivery.region")
        key_prefix = _require_str(pd.get("key_prefix"), "deploy.payload_delivery.key_prefix")
        if key_prefix.startswith("/") or key_prefix.endswith("/"):
            raise ValueError("deploy.payload_delivery.key_prefix must not start or end with '/'")
        if "\\" in key_prefix:
            raise ValueError("deploy.payload_delivery.key_prefix must not contain backslashes")
        if any(p in (".", "..", "") for p in key_prefix.split("/")):
            raise ValueError("deploy.payload_delivery.key_prefix must not contain empty / '.' / '..' segments")

        payload_s3_base_url = base_url
        payload_s3_bucket = bucket
        payload_s3_access_key = access_key
        payload_s3_secret_key = secret_key
        payload_s3_region = region
        payload_s3_key_prefix = key_prefix

    docs: List[Dict[str, Any]] = []
    namespace = _resolved_case_ops_namespace(resolved_case)
    for raw in instances:
        inst = _require_dict(raw, "deploy.instances[]")
        iid = _require_str(inst.get("id"), "deploy.instances[].id")
        k8s_ref = _require_str(inst.get("k8s_ref"), "deploy.instances[].k8s_ref")
        if "/" not in k8s_ref:
            raise ValueError(f"deploy.instances[].k8s_ref must be <deployment|daemonset>/<name>, got: {k8s_ref!r}")
        k8s_ref_kind, deploy_name = k8s_ref.split("/", 1)
        if k8s_ref_kind not in (K8S_REF_KIND_DEPLOYMENT, K8S_REF_KIND_DAEMONSET) or not deploy_name.strip():
            raise ValueError(f"deploy.instances[].k8s_ref must be <deployment|daemonset>/<name>, got: {k8s_ref!r}")
        workload_kind = "Deployment" if k8s_ref_kind == K8S_REF_KIND_DEPLOYMENT else "DaemonSet"
        lifecycle = _require_str(inst.get("lifecycle"), "deploy.instances[].lifecycle")

        deployer = _require_dict(inst.get("deployer"), "deploy.instances[].deployer")
        target = _require_str(deployer.get("target"), "deployer.target")

        payload_file = deployer.get("payload_file")
        payload_dest_path = deployer.get("payload_dest_path")
        if payload_file is not None or payload_dest_path is not None:
            if payload_s3_base_url is None:
                raise ValueError("internal error: payload_delivery is not parsed but payload fields are present")
            payload_file_s = _require_str(payload_file, "deployer.payload_file")
            if os.path.isabs(payload_file_s):
                raise ValueError("deployer.payload_file must be workdir-relative")
            if payload_file_s.startswith("./") or payload_file_s.startswith(".\\"):
                raise ValueError("deployer.payload_file must not start with './' (use a clean workdir-relative path)")
            if "\\" in payload_file_s:
                raise ValueError("deployer.payload_file must not contain backslashes")
            if any(p in (".", "..", "") for p in payload_file_s.split("/")):
                raise ValueError("deployer.payload_file must not contain empty / '.' / '..' segments")

            payload_dest_path_s = _require_str(payload_dest_path, "deployer.payload_dest_path")
            if not payload_dest_path_s.startswith("/"):
                raise ValueError("deployer.payload_dest_path must be an absolute path")

        command = _require_list(deployer.get("command"), "deployer.command")
        if not command:
            raise ValueError("deployer.command must be non-empty")
        cmd0 = _require_str(command[0], "deployer.command[0]")
        cmd0 = _subst_runtime_tokens(resolved_case, cmd0)

        args: List[str] = []
        for j, x in enumerate(command[1:], start=1):
            sx = _require_str(x, f"deployer.command[{j}]")
            args.append(_subst_runtime_tokens(resolved_case, sx))

        raw_args = deployer.get("args")
        if raw_args is not None:
            al = _require_list(raw_args, "deployer.args")
            for j, x in enumerate(al):
                sx = _require_str(x, f"deployer.args[{j}]")
                args.append(_subst_runtime_tokens(resolved_case, sx))

        if payload_file is not None or payload_dest_path is not None:
            # Deterministic object key mapping (no defaults / no hidden conventions):
            # - key_prefix is explicitly configured (deploy.payload_delivery.key_prefix).
            # - payload_file is explicitly configured per instance.
            # - object_key == "<key_prefix>/<payload_file>".
            s3_base_url = payload_s3_base_url
            s3_bucket = payload_s3_bucket
            s3_access_key = payload_s3_access_key
            s3_secret_key = payload_s3_secret_key
            s3_region = payload_s3_region
            s3_key_prefix = payload_s3_key_prefix
            if s3_base_url is None:
                raise ValueError("internal error: payload_delivery.s3_base_url is missing")
            if s3_bucket is None:
                raise ValueError("internal error: payload_delivery.bucket is missing")
            if s3_access_key is None:
                raise ValueError("internal error: payload_delivery.access_key is missing")
            if s3_secret_key is None:
                raise ValueError("internal error: payload_delivery.secret_key is missing")
            if s3_region is None:
                raise ValueError("internal error: payload_delivery.region is missing")
            if s3_key_prefix is None:
                raise ValueError("internal error: payload_delivery.key_prefix is missing")

            payload_file_s = _require_str(payload_file, "deployer.payload_file")
            object_key = f"{s3_key_prefix}/{payload_file_s}"
            if object_key.startswith("/") or "\\" in object_key:
                raise ValueError("computed s3 object key is invalid (must be a clean relpath)")
            if object_key == ".fluxon_fs_s3_multipart" or object_key.startswith(".fluxon_fs_s3_multipart/"):
                raise ValueError("computed s3 object key uses reserved prefix: .fluxon_fs_s3_multipart")

            orig_argv = [cmd0] + args
            exec_cmd = " ".join(_shell_quote(x) for x in orig_argv)

            # Generate a self-contained SigV4 GET downloader (Fluxon FS S3 gateway) and then exec the original argv.
            bash_script = (
                "set -euo pipefail\n"
                "python3 - <<'PY'\n"
                "import datetime\n"
                "import hashlib\n"
                "import hmac\n"
                "import os\n"
                "import urllib.parse\n"
                "import urllib.request\n"
                "from pathlib import Path\n"
                "\n"
                f"BASE_URL = {s3_base_url!r}\n"
                f"BUCKET = {s3_bucket!r}\n"
                f"OBJECT_KEY = {object_key!r}\n"
                f"DEST_PATH = {payload_dest_path_s!r}\n"
                f"ACCESS_KEY = {s3_access_key!r}\n"
                f"SECRET_KEY = {s3_secret_key!r}\n"
                f"REGION = {s3_region!r}\n"
                "\n"
                "ALG = 'AWS4-HMAC-SHA256'\n"
                "SERVICE = 's3'\n"
                "TERM = 'aws4_request'\n"
                "UNSIGNED = 'UNSIGNED-PAYLOAD'\n"
                "\n"
                "def _hmac_sha256(key: bytes, msg: bytes) -> bytes:\n"
                "    return hmac.new(key, msg, hashlib.sha256).digest()\n"
                "\n"
                "def _sha256_hex(msg: bytes) -> str:\n"
                "    return hashlib.sha256(msg).hexdigest()\n"
                "\n"
                "def _derive_signing_key(secret_key: str, scope_date: str, region: str) -> bytes:\n"
                "    k_date = _hmac_sha256(('AWS4' + secret_key).encode('utf-8'), scope_date.encode('utf-8'))\n"
                "    k_region = _hmac_sha256(k_date, region.encode('utf-8'))\n"
                "    k_service = _hmac_sha256(k_region, SERVICE.encode('utf-8'))\n"
                "    return _hmac_sha256(k_service, TERM.encode('utf-8'))\n"
                "\n"
                "def _sigv4_headers(*, method: str, signing_path: str, query: str, host: str, scope_date: str, amz_date: str, payload_hash: str) -> dict:\n"
                "    signed_headers = 'host;x-amz-content-sha256;x-amz-date'\n"
                "    canonical_headers = ''\n"
                "    canonical_headers += f'host:{host}\\n'\n"
                "    canonical_headers += f'x-amz-content-sha256:{payload_hash}\\n'\n"
                "    canonical_headers += f'x-amz-date:{amz_date}\\n'\n"
                "    canonical_request = '\\n'.join([method, signing_path, query, canonical_headers, signed_headers, payload_hash])\n"
                "    cr_hash = _sha256_hex(canonical_request.encode('utf-8'))\n"
                "    scope = f'{scope_date}/{REGION}/{SERVICE}/{TERM}'\n"
                "    string_to_sign = '\\n'.join([ALG, amz_date, scope, cr_hash])\n"
                "    signing_key = _derive_signing_key(SECRET_KEY, scope_date, REGION)\n"
                "    sig = hmac.new(signing_key, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()\n"
                "    auth = f\"{ALG} Credential={ACCESS_KEY}/{scope}, SignedHeaders={signed_headers}, Signature={sig}\"\n"
                "    return {\n"
                "        'Authorization': auth,\n"
                "        'x-amz-date': amz_date,\n"
                "        'x-amz-content-sha256': payload_hash,\n"
                "        'Host': host,\n"
                "    }\n"
                "\n"
                "u = urllib.parse.urlparse(BASE_URL)\n"
                "if u.scheme not in ('http', 'https'):\n"
                "    raise ValueError('BASE_URL must be http(s)')\n"
                "if not u.netloc:\n"
                "    raise ValueError('BASE_URL missing host')\n"
                "base_path = u.path.rstrip('/')\n"
                "if base_path == '':\n"
                "    raise ValueError('BASE_URL must include a non-root path prefix (e.g. /fs_s3)')\n"
                "\n"
                "bucket_enc = urllib.parse.quote(BUCKET, safe='-_.~')\n"
                "key_enc = urllib.parse.quote(OBJECT_KEY, safe='/-_.~')\n"
                "full_path = base_path + '/' + bucket_enc + '/' + key_enc\n"
                # Sign the *actual* client-visible request path (including s3_base_url path prefix, e.g. "/fs_s3").
                "signing_path = full_path\n"
                "url = f'{u.scheme}://{u.netloc}{full_path}'\n"
                "\n"
                "now = datetime.datetime.utcnow()\n"
                "amz_date = now.strftime('%Y%m%dT%H%M%SZ')\n"
                "scope_date = now.strftime('%Y%m%d')\n"
                "hdrs = _sigv4_headers(method='GET', signing_path=signing_path, query='', host=u.netloc, scope_date=scope_date, amz_date=amz_date, payload_hash=UNSIGNED)\n"
                "\n"
                "dest = Path(DEST_PATH)\n"
                "dest.parent.mkdir(parents=True, exist_ok=True)\n"
                "tmp = Path(str(dest) + '.tmp')\n"
                "if tmp.exists():\n"
                "    tmp.unlink()\n"
                "req = urllib.request.Request(url, method='GET')\n"
                "for k, v in hdrs.items():\n"
                "    req.add_header(k, v)\n"
                "with urllib.request.urlopen(req, timeout=60) as resp:\n"
                "    if getattr(resp, 'status', None) != 200:\n"
                "        body = resp.read(4096)\n"
                "        raise RuntimeError(f'download failed: status={getattr(resp, \"status\", None)} body={body!r}')\n"
                "    with tmp.open('wb') as f:\n"
                "        while True:\n"
                "            b = resp.read(1024 * 1024)\n"
                "            if not b:\n"
                "                break\n"
                "            f.write(b)\n"
                "tmp.replace(dest)\n"
                "PY\n"
                f"exec {exec_cmd}\n"
            )

            # Deployer only consumes argv/cwd; container image is required by the YAML subset parser
            # but has no effect on execution.
            container: Dict[str, Any] = {
                "name": iid,
                "image": "deployer.local/exec",
                "command": ["/bin/bash", "-lc"],
                "args": [bash_script],
            }
        else:
            # Deployer only consumes argv/cwd; container image is required by the YAML subset parser
            # but has no effect on execution.
            container = {
                "name": iid,
                "image": "deployer.local/exec",
                "command": [cmd0],
            }
            if args:
                container["args"] = args

        working_dir = deployer.get("working_dir")
        working_dir_s = None
        if working_dir is not None:
            wd = _require_str(working_dir, "deployer.working_dir")
            working_dir_s = _subst_runtime_tokens(resolved_case, wd)

        if working_dir_s is not None:
            container["workingDir"] = working_dir_s
        if lifecycle == "job":
            container = _wrap_job_container(container)

        affinity: Dict[str, Any] = {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": "kubernetes.io/hostname",
                                    "operator": "In",
                                    "values": [target],
                                }
                            ]
                        }
                    ]
                }
            }
        }

        doc: Dict[str, Any] = {
            "apiVersion": "apps/v1",
            "kind": workload_kind,
            "metadata": {
                "name": deploy_name,
                "annotations": {
                    OPS_NAMESPACE_ANNOTATION_KEY: namespace,
                },
            },
            "spec": {
                "template": {
                    "spec": {
                        "affinity": affinity,
                        "containers": [container],
                    }
                }
            },
        }
        docs.append(doc)

    # Write multi-document YAML.
    parts: List[str] = []
    for i, d in enumerate(docs):
        if i > 0:
            parts.append("---\n")
        parts.append(yaml.safe_dump(d, sort_keys=False, default_flow_style=False, allow_unicode=False))
    out_path.write_text("".join(parts), encoding="utf-8")


def _wrap_job_container(container: Dict[str, Any]) -> Dict[str, Any]:
    command = _require_list(container.get("command"), "container.command")
    argv: List[str] = []
    for index, item in enumerate(command):
        argv.append(_require_str(item, f"container.command[{index}]"))

    raw_args = container.get("args")
    if raw_args is not None:
        args = _require_list(raw_args, "container.args")
        for index, item in enumerate(args):
            argv.append(_require_str(item, f"container.args[{index}]"))

    job_once_script = "\n".join(
        [
            "set +e",
            'trap "" HUP',
            'child_pid=""',
            'hold_pid=""',
            "on_term() {",
            '  if [ -n "${hold_pid:-}" ] && kill -0 "$hold_pid" 2>/dev/null; then',
            '    kill -TERM "$hold_pid" 2>/dev/null || true',
            '    wait "$hold_pid" || true',
            "  fi",
            '  if [ -n "${child_pid:-}" ] && kill -0 "$child_pid" 2>/dev/null; then',
            '    kill -TERM "$child_pid" 2>/dev/null || true',
            '    wait "$child_pid" || true',
            "  fi",
            "  exit 0",
            "}",
            "trap on_term TERM INT",
            f"{' '.join(_shell_quote(item) for item in argv)} &",
            'child_pid=$!',
            'wait "$child_pid"',
            'rc=$?',
            'child_pid=""',
            'if [ "$rc" -ne 0 ]; then',
            '  echo "[job-once] child exited rc=$rc; exiting so supervisor can restart"',
            '  exit "$rc"',
            "fi",
            'echo "[job-once] child exited rc=$rc; holding until controller stop"',
            "while true; do",
            "  sleep 3600 &",
            '  hold_pid=$!',
            '  wait "$hold_pid"',
            '  hold_pid=""',
            "done",
        ]
    )

    wrapped: Dict[str, Any] = {
        "name": _require_str(container.get("name"), "container.name"),
        "image": _require_str(container.get("image"), "container.image"),
        "command": ["/bin/bash", "-lc"],
        "args": [job_once_script],
    }
    working_dir = container.get("workingDir")
    if working_dir is not None:
        wrapped["workingDir"] = _require_str(working_dir, "container.workingDir")
    return wrapped


def _write_phase_inputs(
    resolved_case: Dict[str, Any],
    *,
    run_dir: Path,
    instance_ids: List[str],
    ctx: str,
) -> None:
    if not instance_ids:
        raise ValueError(f"{ctx}: instance_ids is empty")

    phase_case = copy.deepcopy(resolved_case)
    deploy = _require_dict(phase_case.get("deploy"), "resolved_case.deploy")
    instances = _require_list(deploy.get("instances"), "resolved_case.deploy.instances")

    selected: List[Dict[str, Any]] = []
    for iid in instance_ids:
        found = [x for x in instances if isinstance(x, dict) and x.get("id") == iid]
        if len(found) != 1:
            raise ValueError(f"{ctx}: requires exactly one deploy instance with id={iid!r}, got={len(found)}")
        selected.append(_require_dict(found[0], f"deploy.instances[{iid}]"))

    deploy["instances"] = selected
    _write_yaml_file(run_dir / "resolved_case.yaml", phase_case)
    _write_deployer_manifests(phase_case, run_dir, allow_overwrite=True)


def _ci_write_phase_inputs(resolved_case: Dict[str, Any], *, run_dir: Path, instance_ids: List[str]) -> None:
    _write_phase_inputs(resolved_case, run_dir=run_dir, instance_ids=instance_ids, ctx="CI")


def _runtime_phase_label(phase: _RuntimePhase) -> str:
    return f"{phase.write_ctx} {phase.phase_id} [{phase.layer}]"


def _require_runtime_phase_by_id(
    phases: Tuple[_RuntimePhase, ...],
    *,
    phase_id: str,
    ctx: str,
) -> _RuntimePhase:
    matches = [phase for phase in phases if phase.phase_id == phase_id]
    if len(matches) != 1:
        raise ValueError(f"{ctx} requires exactly one runtime phase with phase_id={phase_id!r}, got={len(matches)}")
    return matches[0]


def _write_runtime_phase_inputs(
    resolved_case: Dict[str, Any],
    *,
    run_dir: Path,
    phase: _RuntimePhase,
) -> None:
    _write_phase_inputs(
        resolved_case,
        run_dir=run_dir,
        instance_ids=list(phase.instance_ids),
        ctx=_runtime_phase_label(phase),
    )


def _stage_runtime_phase_run_dir(
    resolved_case: Dict[str, Any],
    *,
    run_dir: Path,
    phase: _RuntimePhase,
) -> None:
    _write_runtime_phase_inputs(resolved_case, run_dir=run_dir, phase=phase)
    if phase.stage_run_dir is None:
        return
    _stage_run_dir_for_remote_targets(
        resolved_case,
        run_dir=run_dir,
        instance_ids=list(phase.instance_ids),
        archive_prefix=phase.stage_run_dir.archive_prefix,
        stage_prefix=phase.stage_run_dir.stage_prefix,
        verify_relpaths=list(phase.stage_run_dir.verify_relpaths),
        sync_mode=phase.stage_run_dir.sync_mode,
        include_relpaths=None if phase.stage_run_dir.include_relpaths is None else list(phase.stage_run_dir.include_relpaths),
        ctx=phase.stage_run_dir.ctx,
    )


def _deploy_runtime_phase_after_stage(
    resolved_case: Dict[str, Any],
    *,
    run_dir: Path,
    phase: _RuntimePhase,
) -> Dict[str, Any]:
    deploy_result = _run_adapter_action(resolved_case, run_dir=run_dir, action="deploy")
    if deploy_result is None:
        raise ValueError(f"{_runtime_phase_label(phase)} deploy must produce deploy_result")
    return deploy_result


def _deploy_runtime_phase(
    resolved_case: Dict[str, Any],
    *,
    run_dir: Path,
    phase: _RuntimePhase,
) -> Dict[str, Any]:
    _stage_runtime_phase_run_dir(resolved_case, run_dir=run_dir, phase=phase)
    return _deploy_runtime_phase_after_stage(resolved_case, run_dir=run_dir, phase=phase)


def _collect_runtime_phase(
    resolved_case: Dict[str, Any],
    *,
    run_dir: Path,
    phase: _RuntimePhase,
) -> None:
    _write_runtime_phase_inputs(resolved_case, run_dir=run_dir, phase=phase)
    _run_adapter_action(resolved_case, run_dir=run_dir, action="collect")


def _ci_cluster_runtime_stage(resolved_case: Dict[str, Any]) -> _RemoteRunDirStage:
    verify_relpaths = list(CI_CLUSTER_RUNTIME_REMOTE_STAGE_VERIFY_RELPATHS)
    if _ci_has_instance(resolved_case, instance_id="owner_0"):
        verify_relpaths.append("configs/ci_owner_0.yaml")
    if _ci_has_instance(resolved_case, instance_id="master"):
        verify_relpaths.append("configs/ci_master.yaml")
    return _RemoteRunDirStage(
        archive_prefix="fluxon_ci_cluster_runtime_run_dir__",
        stage_prefix="fluxon_ci_cluster_runtime_stage_",
        verify_relpaths=tuple(verify_relpaths),
        ctx="stage_ci_cluster_runtime_run_dir",
        sync_mode=REMOTE_RUN_DIR_SYNC_OVERLAY,
        include_relpaths=CI_CLUSTER_RUNTIME_REMOTE_STAGE_INCLUDE_RELPATHS,
    )


def _ci_runner_runtime_stage(resolved_case: Dict[str, Any]) -> _RemoteRunDirStage:
    verify_relpaths = list(CI_RUNNER_REMOTE_STAGE_VERIFY_RELPATHS)
    if _ci_has_instance(resolved_case, instance_id="owner_0"):
        verify_relpaths.append("configs/ci_owner_0.yaml")
    if _ci_has_instance(resolved_case, instance_id="master"):
        verify_relpaths.append("configs/ci_master.yaml")
    include_relpaths = list(CI_RUNNER_REMOTE_STAGE_INCLUDE_RELPATHS)
    if _ci_runtime_contract_id(resolved_case) == CI_RUNTIME_CONTRACT_CLUSTER_KV_OWNER:
        for relpath in ("fluxon_release", "test_rsc"):
            if relpath not in include_relpaths:
                include_relpaths.append(relpath)
    return _RemoteRunDirStage(
        archive_prefix="fluxon_ci_run_dir__",
        stage_prefix="fluxon_ci_stage_",
        verify_relpaths=tuple(verify_relpaths),
        ctx="stage_ci_run_dir",
        sync_mode=REMOTE_RUN_DIR_SYNC_OVERLAY,
        include_relpaths=tuple(include_relpaths),
    )


def _ci_runtime_phase(resolved_case: Dict[str, Any], phase_id: str) -> _RuntimePhase:
    phases = {
        "cluster_runtime": _RuntimePhase(
            phase_id="cluster_runtime",
            layer=RUNTIME_LAYER_CASE,
            instance_ids=CI_CLUSTER_RUNTIME_INSTANCE_IDS,
            write_ctx="CI",
            stage_run_dir=_ci_cluster_runtime_stage(resolved_case),
        ),
        "ci_runner": _RuntimePhase(
            phase_id="ci_runner",
            layer=RUNTIME_LAYER_CASE,
            instance_ids=("ci_runner",),
            write_ctx="CI",
            stage_run_dir=_ci_runner_runtime_stage(resolved_case),
        ),
        "collect_all": _RuntimePhase(
            phase_id="collect_all",
            layer=RUNTIME_LAYER_CASE,
            instance_ids=CI_RUNTIME_INSTANCE_IDS,
            write_ctx="CI",
        ),
    }
    try:
        return phases[phase_id]
    except KeyError as exc:
        raise ValueError(f"unsupported CI runtime phase: {phase_id}") from exc


def _test_stack_runtime_phase(
    *,
    phase_id: str,
    node_ids: Optional[Tuple[str, ...]] = None,
    include_stage_run_dir: bool = True,
) -> _RuntimePhase:
    if phase_id == "coordinator":
        if node_ids is not None:
            raise ValueError("TEST_STACK coordinator phase does not accept node_ids")
        return _RuntimePhase(
            phase_id="coordinator",
            layer=RUNTIME_LAYER_CASE,
            instance_ids=("coordinator",),
            write_ctx="TEST_STACK",
        )
    if phase_id in ("nodes", "node_runtime"):
        if node_ids is None or not node_ids:
            raise ValueError(f"TEST_STACK {phase_id} phase requires non-empty node_ids")
        stage_run_dir = None
        if include_stage_run_dir:
            stage_run_dir = _RemoteRunDirStage(
                archive_prefix="fluxon_test_stack_run_dir__",
                stage_prefix="fluxon_test_stack_stage_",
                verify_relpaths=("benchmark_config.py",),
                ctx="stage_test_stack_run_dir",
                # English note:
                # - Some TEST_STACK modes deploy long-lived sidecars (e.g. KV owner service) that run from run_dir.
                # - Do not rm -rf run_dir on the remote host while those services are running.
                sync_mode=REMOTE_RUN_DIR_SYNC_OVERLAY,
                # English note:
                # - Remote benchmark nodes only need the benchmark config plus the prepared
                #   runtime bundle (`test_stack_runtime/` with wheels + source).
                # - Archiving the whole run_dir also pulls in materialized release artifacts
                #   like ext_images/, which makes node staging unnecessarily heavy
                #   and stalls the benchmark handoff before deploy.
                include_relpaths=TEST_STACK_NODE_REMOTE_STAGE_INCLUDE_RELPATHS,
            )
        return _RuntimePhase(
            phase_id=phase_id,
            layer=RUNTIME_LAYER_CASE,
            instance_ids=node_ids,
            write_ctx="TEST_STACK",
            stage_run_dir=stage_run_dir,
        )
    if phase_id == "collect_nodes":
        if node_ids is None or not node_ids:
            raise ValueError("TEST_STACK collect_nodes phase requires non-empty node_ids")
        return _RuntimePhase(
            phase_id="collect_nodes",
            layer=RUNTIME_LAYER_CASE,
            instance_ids=node_ids,
            write_ctx="TEST_STACK",
        )
    if phase_id == "collect_coordinator":
        if node_ids is not None:
            raise ValueError("TEST_STACK collect_coordinator phase does not accept node_ids")
        return _RuntimePhase(
            phase_id="collect_coordinator",
            layer=RUNTIME_LAYER_CASE,
            instance_ids=("coordinator",),
            write_ctx="TEST_STACK",
        )
    raise ValueError(f"unsupported TEST_STACK runtime phase: {phase_id}")


def _compile_case_plan(resolved_case: Dict[str, Any]) -> _CasePlan:
    case_family = _resolved_case_family(resolved_case)
    if case_family == CASE_FAMILY_CI:
        case_instance_ids = _ci_case_instance_ids(resolved_case)
        if "ci_runner" not in set(case_instance_ids):
            raise ValueError("CI case plan requires a ci_runner instance")
        prepare_instance_ids = _ci_cluster_runtime_instance_ids(resolved_case)
        prepare_phases: Tuple[_RuntimePhase, ...] = ()
        if prepare_instance_ids:
            prepare_phases = (
                _RuntimePhase(
                    phase_id="cluster_runtime",
                    layer=RUNTIME_LAYER_CASE,
                    instance_ids=prepare_instance_ids,
                    write_ctx="CI",
                    stage_run_dir=_ci_cluster_runtime_stage(resolved_case),
                ),
            )
        return _CasePlan(
            case_family=case_family,
            prepare_phases=prepare_phases,
            execute_phases=(
                _ci_runtime_phase(resolved_case, "ci_runner"),
            ),
            collect_phases=(
                _RuntimePhase(
                    phase_id="collect_all",
                    layer=RUNTIME_LAYER_CASE,
                    instance_ids=case_instance_ids,
                    write_ctx="CI",
                ),
            ),
        )
    if case_family == CASE_FAMILY_BENCH:
        deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
        deploy_instances = _require_list(deploy.get("instances"), "resolved_case.deploy.instances")
        case_instance_ids = tuple(_runtime_layer_instance_ids(resolved_case, layer=RUNTIME_LAYER_CASE))
        prepare_ids: List[str] = []
        node_ids: List[str] = []
        for index, raw_instance in enumerate(deploy_instances):
            instance = _require_dict(raw_instance, f"resolved_case.deploy.instances[{index}]")
            instance_id = _require_str(instance.get("id"), f"resolved_case.deploy.instances[{index}].id")
            lifecycle = _require_str(instance.get("lifecycle"), f"resolved_case.deploy.instances[{index}].lifecycle")
            if lifecycle == "service":
                prepare_ids.append(instance_id)
                continue
            if lifecycle == "job":
                node_ids.append(instance_id)
                continue
            raise ValueError(
                f"TEST_STACK deploy instance lifecycle must be 'service' or 'job', got: {lifecycle!r}"
            )
        ordered_partition = tuple(prepare_ids + node_ids)
        if ordered_partition != case_instance_ids:
            raise ValueError("TEST_STACK deploy.instances lifecycle partition does not match runtime layer instance_ids order")
        prepare_ids_tuple = tuple(prepare_ids)
        node_ids_tuple = tuple(node_ids)
        if not node_ids_tuple:
            raise ValueError("TEST_STACK case plan requires non-empty case runtime node_ids")
        return _CasePlan(
            case_family=case_family,
            prepare_phases=(
                _RuntimePhase(
                    phase_id="coordinator",
                    layer=RUNTIME_LAYER_CASE,
                    instance_ids=prepare_ids_tuple,
                    write_ctx="TEST_STACK",
                    stage_run_dir=_RemoteRunDirStage(
                        archive_prefix="fluxon_test_stack_services_run_dir__",
                        stage_prefix="fluxon_test_stack_services_stage_",
                        verify_relpaths=(
                            "benchmark_config.py",
                            "test_stack_runtime/src/fluxon_test_stack/distributed_benchmark_coordinator.py",
                        ),
                        ctx="stage_test_stack_services_run_dir",
                        sync_mode=REMOTE_RUN_DIR_SYNC_OVERLAY,
                        include_relpaths=TEST_STACK_SERVICE_REMOTE_STAGE_INCLUDE_RELPATHS,
                    ),
                ),
                _test_stack_runtime_phase(phase_id="node_runtime", node_ids=node_ids_tuple),
            ),
            execute_phases=(
                _test_stack_runtime_phase(
                    phase_id="nodes",
                    node_ids=node_ids_tuple,
                    include_stage_run_dir=False,
                ),
            ),
            collect_phases=(
                _test_stack_runtime_phase(phase_id="collect_nodes", node_ids=node_ids_tuple),
                _RuntimePhase(
                    phase_id="collect_coordinator",
                    layer=RUNTIME_LAYER_CASE,
                    instance_ids=prepare_ids_tuple,
                    write_ctx="TEST_STACK",
                ),
            ),
        )
    raise ValueError(f"unsupported case family for case plan: {case_family}")


def _prepare_case(
    planned_case: _PlannedCase,
    *,
    resolved_case: Dict[str, Any],
    run_dir: Path,
    run_index: int,
    case_plan: _CasePlan,
    test_stack_meta: Optional[Dict[str, Any]],
    runtime_tracking: _CaseRuntimeTracking,
) -> _PreparedCase:
    if case_plan.case_family == CASE_FAMILY_CI:
        return _prepare_ci_case(
            planned_case,
            resolved_case=resolved_case,
            run_dir=run_dir,
            run_index=run_index,
            case_plan=case_plan,
            runtime_tracking=runtime_tracking,
        )
    if case_plan.case_family == CASE_FAMILY_BENCH:
        return _prepare_test_stack_case(
            resolved_case,
            run_dir=run_dir,
            case_plan=case_plan,
            test_stack_meta=_require_dict(test_stack_meta, "TEST_STACK test_stack_meta"),
            runtime_tracking=runtime_tracking,
        )
    raise ValueError(f"unsupported case family for prepare_case: {case_plan.case_family}")


def _execute_case(
    planned_case: _PlannedCase,
    *,
    resolved_case: Dict[str, Any],
    run_dir: Path,
    run_index: int,
    started_at: int,
    prepared_case: _PreparedCase,
    runtime_tracking: _CaseRuntimeTracking,
) -> _ExecutedCase:
    if prepared_case.plan.case_family == CASE_FAMILY_CI:
        return _execute_ci_case(
            planned_case,
            resolved_case=resolved_case,
            run_dir=run_dir,
            run_index=run_index,
            started_at=started_at,
            prepared_case=prepared_case,
            runtime_tracking=runtime_tracking,
        )
    if prepared_case.plan.case_family == CASE_FAMILY_BENCH:
        return _execute_test_stack_case(
            resolved_case,
            run_dir=run_dir,
            run_index=run_index,
            started_at=started_at,
            prepared_case=prepared_case,
            runtime_tracking=runtime_tracking,
        )
    raise ValueError(f"unsupported case family for execute_case: {prepared_case.plan.case_family}")


def _prepare_ci_case(
    planned_case: _PlannedCase,
    *,
    resolved_case: Dict[str, Any],
    run_dir: Path,
    run_index: int,
    case_plan: _CasePlan,
    runtime_tracking: _CaseRuntimeTracking,
) -> _PreparedCase:
    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    ci_checkout_root = _runner_repo_root()
    runtime_tracking.ci_lock_fp = _acquire_ci_lock()
    _ensure_deployer_online(resolved_case)
    out_cluster_name = _ci_cluster_name(resolved_case)

    scale = _require_dict(resolved_case.get("scale"), "resolved_case.scale")
    owner_scale = _require_dict(scale.get("owner"), "resolved_case.scale.owner")
    owner_count = _require_int(owner_scale.get("owner_count"), "scale.owner.owner_count", min_v=1)
    if owner_count != 1:
        raise ValueError("CI currently supports only owner_count=1")
    owner_dram_bytes = _require_int(
        owner_scale.get("owner_dram_bytes"), "scale.owner.owner_dram_bytes", min_v=16777216
    )
    if owner_dram_bytes % 16777216 != 0:
        raise ValueError("scale.owner.owner_dram_bytes must be 16MiB aligned")

    release_root = _resolved_case_release_root(resolved_case)
    if not release_root.exists():
        raise ValueError(f"materialized case release_root is missing: {release_root}")

    if _ci_has_instance(resolved_case, instance_id="owner_0"):
        owner0 = _find_deploy_instance(resolved_case, instance_id="owner_0")
        ci_runner = _find_deploy_instance(resolved_case, instance_id="ci_runner")
        owner0_target = _require_str(
            _require_dict(owner0.get("deployer"), "owner_0.deployer").get("target"),
            "owner_0.target",
        )
        ci_target = _require_str(
            _require_dict(ci_runner.get("deployer"), "ci_runner.deployer").get("target"),
            "ci_runner.target",
        )
        if owner0_target != ci_target:
            raise ValueError("ci_runner must run on the same target as owner_0")

    _ci_cleanup_runtime(resolved_case, timeout_s=120)
    _cleanup_previous_failed_ci_runtime(
        resolved_case,
        run_dir=run_dir,
        run_index=run_index,
    )
    _ci_assert_ports_free(resolved_case)
    _wait_ci_base_runtime_ready(resolved_case)

    services_root = (run_dir / "services").resolve()
    services_root.mkdir(parents=True, exist_ok=True)
    # Keep this directory as a stable staging anchor for CI runtime bundles.
    # The actual shared_memory_path used by iceoryx2 must NOT embed run_dir (path length limit risk).
    (services_root / "share_mem").mkdir(parents=True, exist_ok=True)
    share_mem_path = _ci_shared_memory_path(resolved_case, run_dir=run_dir)
    share_file_path = _ci_shared_file_path(resolved_case, run_dir=run_dir)
    # Create the directory on the local host to make the config deterministic for same-host runs.
    # Remote targets (via deployer) run on their own host filesystem and will create the directory
    # as needed when initializing the KV client.
    Path(share_mem_path).mkdir(parents=True, exist_ok=True)
    Path(share_file_path).mkdir(parents=True, exist_ok=True)

    venv_dir = (run_dir / "venv").resolve()
    if venv_dir.exists():
        raise ValueError(f"venv dir already exists (no overwrite): {venv_dir}")
    _run_subprocess([sys.executable, "-m", "venv", str(venv_dir)], cwd=str(run_dir))
    venv_python = venv_dir / "bin" / "python3"
    if not venv_python.exists():
        raise ValueError(f"venv python not found after creation: {venv_python}")

    src_root = (run_dir / "src").resolve()
    _ci_prepare_run_inputs(
        resolved_case=resolved_case,
        source_root=ci_checkout_root,
        release_root=release_root,
        test_rsc_root=_resolved_case_test_rsc_root(resolved_case),
        src_root=src_root,
        venv_python=venv_python,
        ci_commands=planned_case.ci_commands,
        overlay_live_checkout=True,
    )

    _write_ci_build_config_ext(
        resolved_case,
        out_dir=src_root,
    )
    if _ci_cluster_runtime_instance_ids(resolved_case):
        _write_ci_master_owner_configs(
            resolved_case,
            run_dir=run_dir,
            cluster_name=out_cluster_name,
            share_mem_path=share_mem_path,
            share_file_path=share_file_path,
            owner_dram_bytes=owner_dram_bytes,
        )
    _ = _write_ci_runner_script(
        resolved_case,
        run_dir=run_dir,
        src_root=src_root,
        share_mem_path=share_mem_path,
        share_file_path=share_file_path,
    )
    ci_runner_exit_code_path = (run_dir / "logs" / "ci_runner" / "exit_code.txt").resolve()
    ci_runner_exit_code_baseline = _observe_file_state(ci_runner_exit_code_path)

    for cluster_runtime_phase in case_plan.prepare_phases:
        cluster_runtime_deploy_result = _deploy_runtime_phase(
            resolved_case,
            run_dir=run_dir,
            phase=cluster_runtime_phase,
        )
        for instance_id in cluster_runtime_phase.instance_ids:
            _record_ci_apply_id(
                runtime_tracking.ci_attempted_instance_ids,
                runtime_tracking.ci_apply_ids,
                instance_id=instance_id,
                deploy_result=cluster_runtime_deploy_result,
                ctx=f"CI cluster_runtime deploy_result[{instance_id}]",
            )
        for instance_id in cluster_runtime_phase.instance_ids:
            _wait_ci_instance_ready(resolved_case, instance_id=instance_id)
    return _PreparedCase(
        plan=case_plan,
        ci_runner_exit_code_baseline=ci_runner_exit_code_baseline,
    )


def _prepare_test_stack_case(
    resolved_case: Dict[str, Any],
    *,
    run_dir: Path,
    case_plan: _CasePlan,
    test_stack_meta: Dict[str, Any],
    runtime_tracking: _CaseRuntimeTracking,
) -> _PreparedCase:
    _ensure_deployer_online(resolved_case)
    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    controller_url = _require_str(deploy.get("controller_url"), "resolved_case.deploy.controller_url").rstrip("/")
    case_obj = _require_dict(resolved_case.get("case"), "resolved_case.case")
    case_id = _require_str(case_obj.get("case_id"), "resolved_case.case.case_id")
    _cleanup_skipped_case_desired_applies(controller_url=controller_url, case_id=case_id)
    _write_deployer_manifests(resolved_case, run_dir, allow_overwrite=False)

    scale = _require_dict(resolved_case.get("scale"), "resolved_case.scale")
    max_secs = _require_int(scale.get("duration_seconds"), "scale.duration_seconds", min_v=1)
    benchmark_scale = _require_dict(
        scale.get("benchmark"),
        "resolved_case.scale.benchmark",
    )
    metric_warmup_seconds = _require_number(
        benchmark_scale.get("metric_warmup_seconds"),
        "resolved_case.scale.benchmark.metric_warmup_seconds",
    )
    profile = _require_dict(resolved_case.get("profile"), "resolved_case.profile")
    profile_test_stack = _require_dict(profile.get("test_stack"), "resolved_case.profile.test_stack")
    coordinator_ready_timeout_seconds = _require_int(
        profile_test_stack.get("coordinator_ready_timeout_seconds"),
        "resolved_case.profile.test_stack.coordinator_ready_timeout_seconds",
        min_v=1,
    )

    coordinator_addr = _require_str(test_stack_meta.get("coordinator_addr"), "test_stack_meta.coordinator_addr")
    if ":" not in coordinator_addr:
        raise ValueError(f"invalid coordinator_addr: {coordinator_addr!r}")
    coord_host, coord_port_s = coordinator_addr.rsplit(":", 1)
    coord_port = int(coord_port_s)
    coordinator_phase = _require_runtime_phase_by_id(
        case_plan.prepare_phases,
        phase_id="coordinator",
        ctx="TEST_STACK prepare",
    )
    # Fluxon external benchmark nodes bootstrap from a local KV owner shared bundle on each host.
    # Clean stale owner-published files before deploying the coordinator phase so owners cannot
    # race the delete verification by recreating shared.json/mmap.file during startup.
    scene = _require_dict(resolved_case.get("scene"), "resolved_case.scene")
    ts_scene = _require_dict(scene.get("test_stack"), "resolved_case.scene.test_stack")
    mode = _require_str(ts_scene.get("mode"), "scene.test_stack.mode")
    backend_kind = _require_test_stack_backend_kind(
        profile_test_stack.get("kind"),
        "resolved_case.profile.test_stack.kind",
    )
    owner_instance_ids: List[str] = []
    shared_memory_path: Optional[str] = None
    shared_file_path: Optional[str] = None
    stack_cluster_name: Optional[str] = None
    if _test_stack_backend_uses_dedicated_kv_owners(backend_kind=backend_kind, mode=mode):
        runtime = _require_dict(resolved_case.get("runtime"), "resolved_case.runtime")
        owner_instance_ids = [
            iid
            for iid in coordinator_phase.instance_ids
            if isinstance(iid, str) and iid.startswith(TEST_STACK_KV_OWNER_INSTANCE_ID_PREFIX)
        ]
        if not owner_instance_ids:
            raise ValueError(
                f"{mode} requires dedicated KV owner instances (missing kv_owner_* in prepare phase)"
            )
        if _test_stack_backend_uses_external_fluxon_kv(backend_kind=backend_kind, mode=mode):
            stack_identity = _require_dict(runtime.get("stack_identity"), "resolved_case.runtime.stack_identity")
            stack_cluster_name = _require_str(
                stack_identity.get("cluster_name"),
                "runtime.stack_identity.cluster_name",
            )
            shared_memory_path = _require_str(
                stack_identity.get("shared_memory_path"),
                "runtime.stack_identity.shared_memory_path",
            )
            shared_file_path = _require_str(
                stack_identity.get("shared_file_path"),
                "runtime.stack_identity.shared_file_path",
            )
            _converge_test_stack_external_owner_shared_bundle_cleanup(
                resolved_case,
                controller_url=controller_url,
                owner_instance_ids=owner_instance_ids,
            )
    _stage_runtime_phase_run_dir(resolved_case, run_dir=run_dir, phase=coordinator_phase)
    _ensure_test_stack_runtime_env_ready_for_instance_ids(
        resolved_case,
        run_dir=run_dir,
        instance_ids=coordinator_phase.instance_ids,
    )
    runtime_tracking.ts_coord_deploy_attempted = True
    coord_deploy_result = _deploy_runtime_phase_after_stage(
        resolved_case,
        run_dir=run_dir,
        phase=coordinator_phase,
    )
    runtime_tracking.ts_coord_apply_id = _deploy_result_history_id(
        coord_deploy_result,
        ctx="TEST_STACK coordinator deploy_result",
    )
    _wait_instance_running(resolved_case, instance_id="coordinator", timeout_s=30)
    _wait_instance_tcp_ready(
        resolved_case,
        instance_id="coordinator",
        host=coord_host,
        port=coord_port,
        timeout_s=coordinator_ready_timeout_seconds,
    )
    if backend_kind == TEST_STACK_BACKEND_MOONCAKE:
        bench_cfg = _load_test_stack_benchmark_config(run_dir)
        run_kv_base = _require_dict(bench_cfg.get("kv_base"), "benchmark_config.CONFIG.kv_base")
        run_mooncake_spec = _require_dict(
            run_kv_base.get("mooncake_spec"),
            "benchmark_config.CONFIG.kv_base.mooncake_spec",
        )
        metadata_server = _require_str(
            run_mooncake_spec.get("metadata_server"),
            "benchmark_config.CONFIG.kv_base.mooncake_spec.metadata_server",
        )
        master_server_address = _require_str(
            run_mooncake_spec.get("master_server_address"),
            "benchmark_config.CONFIG.kv_base.mooncake_spec.master_server_address",
        )
        metadata_host_port = urllib.parse.urlparse(metadata_server)
        metadata_port = int(metadata_host_port.port or 0)
        if metadata_port <= 0:
            raise ValueError(f"invalid TEST_STACK Mooncake metadata_server port: {metadata_server!r}")
        if ":" not in master_server_address:
            raise ValueError(f"invalid TEST_STACK Mooncake master_server_address: {master_server_address!r}")
        _, rpc_port_s = master_server_address.rsplit(":", 1)
        rpc_port = int(rpc_port_s)
        _wait_instance_running(
            resolved_case,
            instance_id=TEST_STACK_MOONCAKE_MASTER_INSTANCE_ID,
            timeout_s=60,
        )
        _wait_instance_tcp_ready(
            resolved_case,
            instance_id=TEST_STACK_MOONCAKE_MASTER_INSTANCE_ID,
            host=coord_host,
            port=rpc_port,
            timeout_s=coordinator_ready_timeout_seconds,
        )
        _wait_instance_tcp_ready(
            resolved_case,
            instance_id=TEST_STACK_MOONCAKE_MASTER_INSTANCE_ID,
            host=coord_host,
            port=metadata_port,
            timeout_s=coordinator_ready_timeout_seconds,
        )

    node_runtime_phase = _require_runtime_phase_by_id(
        case_plan.prepare_phases,
        phase_id="node_runtime",
        ctx="TEST_STACK prepare",
    )
    if _test_stack_backend_uses_external_fluxon_kv(backend_kind=backend_kind, mode=mode):
        if shared_memory_path is None or shared_file_path is None or stack_cluster_name is None:
            raise ValueError(
                "internal error: TEST_STACK shared bundle identity is missing after pre-deploy cleanup"
            )
        if "master" in set(coordinator_phase.instance_ids):
            _wait_instance_running(resolved_case, instance_id="master", timeout_s=60)
        bench = _require_dict(_require_dict(resolved_case.get("scale"), "resolved_case.scale").get("benchmark"), "scale.benchmark")
        cluster_ready_timeout_seconds = _require_int(
            bench.get("cluster_ready_timeout_seconds"),
            "scale.benchmark.cluster_ready_timeout_seconds",
            min_v=1,
        )
        for owner_id in owner_instance_ids:
            owner_target = _instance_target_name(resolved_case, instance_id=owner_id)
            shared_bundle_paths = _test_stack_external_owner_shared_bundle_paths(
                resolved_case,
                owner_target=owner_target,
            )
            _wait_instance_running(resolved_case, instance_id=owner_id, timeout_s=60)
            _wait_instance_files_present(
                resolved_case,
                instance_id=owner_id,
                paths=shared_bundle_paths,
                timeout_s=int(cluster_ready_timeout_seconds),
                ctx="TEST_STACK owner shared bundle",
            )
    elif _test_stack_backend_uses_dedicated_kv_owners(backend_kind=backend_kind, mode=mode):
        for owner_id in owner_instance_ids:
            _wait_instance_running(resolved_case, instance_id=owner_id, timeout_s=60)
    _stage_runtime_phase_run_dir(resolved_case, run_dir=run_dir, phase=node_runtime_phase)
    _ensure_test_stack_runtime_env_ready_for_instance_ids(
        resolved_case,
        run_dir=run_dir,
        instance_ids=node_runtime_phase.instance_ids,
    )
    return _PreparedCase(
        plan=case_plan,
        test_stack_result_path=Path(
            _require_str(test_stack_meta.get("result_path"), "test_stack_meta.result_path")
        ),
        test_stack_coordinator_addr=coordinator_addr,
        test_stack_result_timeout_s=_test_stack_result_timeout_seconds(
            max_benchmark_seconds=int(max_secs),
            metric_warmup_seconds=float(metric_warmup_seconds),
        ),
    )


def _execute_ci_case(
    planned_case: _PlannedCase,
    *,
    resolved_case: Dict[str, Any],
    run_dir: Path,
    run_index: int,
    started_at: int,
    prepared_case: _PreparedCase,
    runtime_tracking: _CaseRuntimeTracking,
) -> _ExecutedCase:
    ci_runner_exit_timeout_s = _ci_runner_exit_code_timeout_seconds(resolved_case)
    ci_runner_phase = prepared_case.plan.execute_phases[0]
    ci_runner_deploy_result = _deploy_runtime_phase(
        resolved_case,
        run_dir=run_dir,
        phase=ci_runner_phase,
    )
    _record_ci_apply_id(
        runtime_tracking.ci_attempted_instance_ids,
        runtime_tracking.ci_apply_ids,
        instance_id="ci_runner",
        deploy_result=ci_runner_deploy_result,
        ctx="CI ci_runner deploy_result",
    )
    _wait_ci_instance_ready(resolved_case, instance_id="ci_runner")
    rc = _wait_ci_runner_exit_code(
        resolved_case=resolved_case,
        run_dir=run_dir,
        timeout_s=ci_runner_exit_timeout_s,
        baseline_state=_require_ci_runner_exit_code_baseline(
            prepared_case.ci_runner_exit_code_baseline,
        ),
    )
    outcome = RUN_OUTCOME_SUCCESS if rc == 0 else RUN_OUTCOME_FAILED
    if outcome == RUN_OUTCOME_SUCCESS and runtime_tracking.ci_apply_ids.get("ci_runner") is not None:
        _delete_apply_id(
            resolved_case,
            apply_id=_require_str(runtime_tracking.ci_apply_ids.get("ci_runner"), "CI ci_runner apply_id"),
            ctx="CI ci_runner apply",
        )
        del runtime_tracking.ci_apply_ids["ci_runner"]
    summary = _build_ci_summary_yaml(
        resolved_case,
        run_index=run_index,
        started_at_unix_s=started_at,
        finished_at_unix_s=int(time.time()),
        outcome=outcome,
        counted=False,
        ci_out={"rc": rc},
    )
    for phase in prepared_case.plan.collect_phases:
        _collect_runtime_phase(resolved_case, run_dir=run_dir, phase=phase)
    return _ExecutedCase(outcome=outcome, summary=summary)


def _execute_test_stack_case(
    resolved_case: Dict[str, Any],
    *,
    run_dir: Path,
    run_index: int,
    started_at: int,
    prepared_case: _PreparedCase,
    runtime_tracking: _CaseRuntimeTracking,
) -> _ExecutedCase:
    case_obj = _require_dict(resolved_case.get("case"), "resolved_case.case")
    case_id = _require_str(case_obj.get("case_id"), "case.case_id")

    # Always return a summary (including FAIL), so every run_dir has a terminal artifact.
    # This is required for FULL_ONCE resume and for postmortem without log spelunking.
    outcome = RUN_OUTCOME_FAILED
    error_detail: Optional[str] = None
    collect_error_detail: Optional[str] = None
    result_obj: Optional[Dict[str, Any]] = None

    try:
        node_phase = prepared_case.plan.execute_phases[0]
        runtime_tracking.ts_nodes_deploy_attempted = True
        node_deploy_result = _deploy_runtime_phase(
            resolved_case,
            run_dir=run_dir,
            phase=node_phase,
        )
        runtime_tracking.ts_nodes_apply_id = _deploy_result_history_id(
            node_deploy_result,
            ctx="TEST_STACK node deploy_result",
        )

        result_path = _require_test_stack_result_path(prepared_case.test_stack_result_path)
        timeout_s = _require_test_stack_result_timeout(prepared_case.test_stack_result_timeout_s)
        result_obj = _wait_and_load_test_stack_benchmark_result_json(
            resolved_case,
            result_path,
            timeout_s=timeout_s,
            case_id=case_id,
            writer_instance_id="coordinator",
        )

        # The result file is a terminal artifact even when the benchmark failed.
        # Therefore:
        # - wait for it to become readable/structurally complete (shared-fs races)
        # - validate once to decide SUCCESS/FAILED deterministically (do not "wait for success")
        _validate_test_stack_benchmark_result(result_obj, case_id=case_id)
        outcome = RUN_OUTCOME_SUCCESS
    except Exception as exc:  # noqa: BLE001
        error_detail = f"{type(exc).__name__}: {exc}"
    finally:
        # Collect is best-effort: benchmark_result.json is the terminal artifact for pass/fail.
        # If /api/status is flaky (e.g. 502), we still want deterministic case outcome and
        # keep the collect error visible in summary.yaml.
        try:
            for phase in prepared_case.plan.collect_phases:
                _collect_runtime_phase(resolved_case, run_dir=run_dir, phase=phase)
        except Exception as exc:  # noqa: BLE001
            collect_error_detail = f"{type(exc).__name__}: {exc}"

    summary = {
        "schema_version": SCHEMA_VERSION,
        "case_id": case_id,
        "case_key": _require_str(case_obj.get("case_key"), "case.case_key"),
        "run_index": int(run_index),
        "outcome": outcome,
        "counted": False,
        "timing": {
            "started_at_unix_s": int(started_at),
            "finished_at_unix_s": int(time.time()),
        },
        "test_stack": {
            "coordinator_addr": _require_str(
                prepared_case.test_stack_coordinator_addr,
                "prepared_case.test_stack_coordinator_addr",
            ),
            "completion_signal": "benchmark_result_json",
            "result_path": str(_require_test_stack_result_path(prepared_case.test_stack_result_path)),
            "result": result_obj,
            "error": error_detail,
            "collect_error": collect_error_detail,
        },
    }
    return _ExecutedCase(outcome=outcome, summary=summary)


def _wait_and_load_test_stack_benchmark_result_json(
    resolved_case: Dict[str, Any],
    result_path: Path,
    *,
    timeout_s: int,
    case_id: str,
    writer_instance_id: str,
) -> Dict[str, Any]:
    """Wait until benchmark_result.json becomes readable and structurally complete.

    English note:
    - In the self-host test stack, the benchmark coordinator may run on a remote node and write
      `benchmark_result.json` only to that host-local staged run_dir.
    - Therefore the runner must observe the result through the coordinator instance, not only via
      its own local filesystem view.
    - Previous runs also observed transient partial reads right after the file appeared.
    - Keep convergence deterministic:
      - retry until the JSON parses AND the expected top-level structure exists
      - do NOT retry waiting for a failed benchmark to "turn into" SUCCESS; validation happens once
        in the caller and the run is finalized immediately.
    """
    deadline = time.time() + float(timeout_s)
    last_err: Optional[str] = None
    while True:
        try:
            raw = _instance_read_text_if_present(
                resolved_case,
                instance_id=writer_instance_id,
                path=result_path,
            )
            if raw is None:
                raise FileNotFoundError(
                    f"result file is not present yet: instance_id={writer_instance_id} path={result_path}"
                )
            parsed = json.loads(raw)
            result_obj = _require_dict(parsed, "test_stack.benchmark_result")
            # Structural checks only (avoid waiting on stable FAIL status).
            runs = _require_list(result_obj.get("runs"), "benchmark_result.runs")
            if not runs:
                raise ValueError("benchmark_result.runs is empty")
            run0 = _require_dict(runs[0], "benchmark_result.runs[0]")
            completion = _require_dict(run0.get("completion"), "benchmark_result.runs[0].completion")
            _ = _require_str(completion.get("status"), "benchmark_result.runs[0].completion.status")
            if not result_path.exists() or result_path.read_text(encoding="utf-8") != raw:
                result_path.write_text(raw, encoding="utf-8")
            return result_obj
        except Exception as exc:  # noqa: BLE001
            last_err = f"{type(exc).__name__}: {exc}"
            if time.time() >= deadline:
                raise ValueError(
                    f"benchmark result json did not become readable/valid within timeout: "
                    f"case_id={case_id} path={result_path} timeout_s={timeout_s} last_err={last_err}"
                ) from exc
            time.sleep(0.5)


def _finalize_case_runtime(
    resolved_case: Dict[str, Any],
    *,
    run_dir: Path,
    case_plan: _CasePlan,
    runtime_tracking: _CaseRuntimeTracking,
    outcome: str,
) -> None:
    if case_plan.case_family == CASE_FAMILY_CI:
        _finalize_ci_case_runtime(
            resolved_case,
            run_dir=run_dir,
            runtime_tracking=runtime_tracking,
            outcome=outcome,
        )
        return
    if case_plan.case_family == CASE_FAMILY_BENCH:
        _finalize_test_stack_case_runtime(
            resolved_case,
            runtime_tracking=runtime_tracking,
            outcome=outcome,
        )
        return
    raise ValueError(f"unsupported case family for finalize_case_runtime: {case_plan.case_family}")


def _finalize_ci_case_runtime(
    resolved_case: Dict[str, Any],
    *,
    run_dir: Path,
    runtime_tracking: _CaseRuntimeTracking,
    outcome: str,
) -> None:
    case = _require_dict(resolved_case.get("case"), "resolved_case.case")
    run_mode = _require_str(case.get("run_mode"), "resolved_case.case.run_mode")
    ci_preserved_apply_ids: list[dict[str, str]] = []
    for instance_id in runtime_tracking.ci_attempted_instance_ids:
        apply_id = runtime_tracking.ci_apply_ids.get(instance_id)
        if apply_id is None:
            continue
        ci_preserved_apply_ids.append(
            {
                "instance_id": instance_id,
                "apply_id": _require_str(apply_id, f"CI {instance_id} apply_id"),
            }
        )
    # In FULL_ONCE runs, always teardown to keep the shared test bed clean for the next case.
    # Debug runs preserve failed runtime for interactive inspection.
    should_teardown = outcome == RUN_OUTCOME_SUCCESS or run_mode == RUN_MODE_FULL_ONCE
    if should_teardown:
        (run_dir / CI_PRESERVED_APPLY_IDS_FILENAME).unlink(missing_ok=True)
        for instance_id in reversed(runtime_tracking.ci_attempted_instance_ids):
            apply_id = runtime_tracking.ci_apply_ids.get(instance_id)
            if apply_id is None:
                continue
            _delete_apply_id(
                resolved_case,
                apply_id=_require_str(apply_id, f"CI {instance_id} apply_id"),
                ctx=f"CI {instance_id} apply",
            )
        _ci_cleanup_runtime(resolved_case, timeout_s=120)
        return
    if not ci_preserved_apply_ids:
        return
    _write_yaml_file(
        run_dir / CI_PRESERVED_APPLY_IDS_FILENAME,
        {
            "schema_version": CI_PRESERVED_APPLY_IDS_SCHEMA_VERSION,
            "apply_ids": ci_preserved_apply_ids,
        },
    )
    print(
        "[CI preserve_runtime] "
        "case_id="
        f"{_resolved_case_case_id(resolved_case)} outcome={outcome} apply_ids="
        + ", ".join(f"{entry['instance_id']}={entry['apply_id']}" for entry in ci_preserved_apply_ids)
    )


def _finalize_test_stack_case_runtime(
    resolved_case: Dict[str, Any],
    *,
    runtime_tracking: _CaseRuntimeTracking,
    outcome: str,
) -> None:
    case = _require_dict(resolved_case.get("case"), "resolved_case.case")
    run_mode = _require_str(case.get("run_mode"), "resolved_case.case.run_mode")
    ts_preserved_apply_ids: list[str] = []
    if runtime_tracking.ts_nodes_deploy_attempted and runtime_tracking.ts_nodes_apply_id is not None:
        ts_preserved_apply_ids.append(
            f"nodes={_require_str(runtime_tracking.ts_nodes_apply_id, 'TEST_STACK node apply_id')}"
        )
    if runtime_tracking.ts_coord_deploy_attempted and runtime_tracking.ts_coord_apply_id is not None:
        ts_preserved_apply_ids.append(
            f"coordinator={_require_str(runtime_tracking.ts_coord_apply_id, 'TEST_STACK coordinator apply_id')}"
        )
    # In FULL_ONCE runs, always teardown to keep the shared test bed clean for the next case.
    # Debug runs preserve failed runtime for interactive inspection.
    should_teardown = outcome == RUN_OUTCOME_SUCCESS or run_mode == RUN_MODE_FULL_ONCE
    if should_teardown:
        if runtime_tracking.ts_nodes_deploy_attempted and runtime_tracking.ts_nodes_apply_id is not None:
            _delete_apply_id(
                resolved_case,
                apply_id=_require_str(runtime_tracking.ts_nodes_apply_id, "TEST_STACK node apply_id"),
                ctx="TEST_STACK node apply",
            )
        if runtime_tracking.ts_coord_deploy_attempted and runtime_tracking.ts_coord_apply_id is not None:
            _delete_apply_id(
                resolved_case,
                apply_id=_require_str(runtime_tracking.ts_coord_apply_id, "TEST_STACK coordinator apply_id"),
                ctx="TEST_STACK coordinator apply",
            )
        return
    if not ts_preserved_apply_ids:
        return
    print(
        "[TEST_STACK preserve_runtime] "
        f"case_id={_resolved_case_case_id(resolved_case)} outcome={outcome} apply_ids={', '.join(ts_preserved_apply_ids)}"
    )


def _require_ci_runner_exit_code_baseline(
    baseline_state: Optional[_ObservedFileState],
) -> Optional[_ObservedFileState]:
    return baseline_state


def _require_test_stack_result_path(result_path: Optional[Path]) -> Path:
    if result_path is None:
        raise ValueError("prepared_case.test_stack_result_path is missing")
    return result_path


def _require_test_stack_result_timeout(timeout_s: Optional[int]) -> int:
    if timeout_s is None or timeout_s < 1:
        raise ValueError("prepared_case.test_stack_result_timeout_s must be positive")
    return int(timeout_s)


def _test_stack_result_timeout_seconds(
    *,
    max_benchmark_seconds: int,
    metric_warmup_seconds: float,
) -> int:
    """Keep runner-side result waiting aligned with coordinator semantics."""
    if max_benchmark_seconds <= 0:
        raise ValueError(
            f"max_benchmark_seconds must be > 0, got: {max_benchmark_seconds}"
        )
    if metric_warmup_seconds < 0.0:
        raise ValueError(
            f"metric_warmup_seconds must be >= 0, got: {metric_warmup_seconds}"
        )
    return int(
        float(max_benchmark_seconds)
        + float(metric_warmup_seconds)
        + 600.0
    )


def _deploy_instance_target_name(resolved_case: Dict[str, Any], *, instance_id: str) -> str:
    inst = _find_deploy_instance(resolved_case, instance_id=instance_id)
    return _require_str(
        _require_dict(inst.get("deployer"), f"{instance_id}.deployer").get("target"),
        f"{instance_id}.target",
    )


def _deploy_instance_target_ip(resolved_case: Dict[str, Any], *, instance_id: str) -> str:
    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    target_ip_map = _require_dict(deploy.get("target_ip_map"), "resolved_case.deploy.target_ip_map")
    target = _deploy_instance_target_name(resolved_case, instance_id=instance_id)
    return _require_str(target_ip_map.get(target), f"deploy.target_ip_map[{instance_id}]")


def _deploy_instance_endpoint_port(resolved_case: Dict[str, Any], *, instance_id: str) -> int:
    inst = _find_deploy_instance(resolved_case, instance_id=instance_id)
    ep = _require_dict(inst.get("endpoint"), f"{instance_id}.endpoint")
    return _require_int(ep.get("host_port"), f"{instance_id}.endpoint.host_port", min_v=1)


def _ci_instance_endpoint_url(resolved_case: Dict[str, Any], *, instance_id: str) -> str:
    inst = _find_deploy_instance(resolved_case, instance_id=instance_id)
    ep = _require_dict(inst.get("endpoint"), f"{instance_id}.endpoint")
    scheme = _require_str(ep.get("scheme"), f"{instance_id}.endpoint.scheme")
    port = _deploy_instance_endpoint_port(resolved_case, instance_id=instance_id)
    ip = _deploy_instance_target_ip(resolved_case, instance_id=instance_id)

    if scheme == _ENDPOINT_SCHEME_HTTP:
        return f"http://{ip}:{port}"
    if scheme == _ENDPOINT_SCHEME_HTTPS:
        return f"https://{ip}:{port}"
    raise ValueError(f"{instance_id}.endpoint.scheme invalid: {scheme!r}")


def _ci_runtime(resolved_case: Dict[str, Any]) -> Dict[str, Any]:
    profile = _require_dict(resolved_case.get("profile"), "resolved_case.profile")
    profile_ci = _require_dict(profile.get("ci"), "resolved_case.profile.ci")
    return _require_dict(profile_ci.get("runtime"), "resolved_case.profile.ci.runtime")


def _ci_base_runtime_service(resolved_case: Dict[str, Any], *, service_id: str) -> Dict[str, Any]:
    if service_id not in CI_BASE_RUNTIME_SERVICE_IDS:
        raise ValueError(f"unsupported CI base runtime service_id: {service_id!r}")
    runtime = _ci_runtime(resolved_case)
    base_runtime = _require_dict(runtime.get(RUNTIME_LAYER_BASE), "resolved_case.profile.ci.runtime.base_runtime")
    return _require_dict(base_runtime.get(service_id), f"resolved_case.profile.ci.runtime.base_runtime[{service_id!r}]")


def _ci_base_runtime_service_target_name(resolved_case: Dict[str, Any], *, service_id: str) -> str:
    svc = _ci_base_runtime_service(resolved_case, service_id=service_id)
    return _require_str(svc.get("target"), f"resolved_case.profile.ci.runtime.base_runtime[{service_id!r}].target")


def _ci_base_runtime_service_target_ip(resolved_case: Dict[str, Any], *, service_id: str) -> str:
    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    target_ip_map = _require_dict(deploy.get("target_ip_map"), "resolved_case.deploy.target_ip_map")
    target_name = _ci_base_runtime_service_target_name(resolved_case, service_id=service_id)
    return _require_str(target_ip_map.get(target_name), f"deploy.target_ip_map[{service_id}]")


def _ci_base_runtime_service_port(resolved_case: Dict[str, Any], *, service_id: str) -> int:
    svc = _ci_base_runtime_service(resolved_case, service_id=service_id)
    endpoint = _require_dict(
        svc.get("endpoint"),
        f"resolved_case.profile.ci.runtime.base_runtime[{service_id!r}].endpoint",
    )
    return _require_int(
        endpoint.get("host_port"),
        f"resolved_case.profile.ci.runtime.base_runtime[{service_id!r}].endpoint.host_port",
        min_v=1,
    )


def _ci_base_runtime_service_url(resolved_case: Dict[str, Any], *, service_id: str) -> str:
    svc = _ci_base_runtime_service(resolved_case, service_id=service_id)
    endpoint = _require_dict(
        svc.get("endpoint"),
        f"resolved_case.profile.ci.runtime.base_runtime[{service_id!r}].endpoint",
    )
    scheme = _require_str(
        endpoint.get("scheme"),
        f"resolved_case.profile.ci.runtime.base_runtime[{service_id!r}].endpoint.scheme",
    )
    host = _ci_base_runtime_service_target_ip(resolved_case, service_id=service_id)
    port = _ci_base_runtime_service_port(resolved_case, service_id=service_id)
    if scheme == _ENDPOINT_SCHEME_HTTP:
        return f"http://{host}:{port}"
    if scheme == _ENDPOINT_SCHEME_HTTPS:
        return f"https://{host}:{port}"
    raise ValueError(f"base runtime endpoint scheme invalid: service_id={service_id} scheme={scheme!r}")


def _resolved_run_dir_path(resolved_case: Dict[str, Any]) -> Path:
    runtime = _require_dict(resolved_case.get("runtime"), "resolved_case.runtime")
    return Path(_require_str(runtime.get("run_dir"), "runtime.run_dir")).resolve()


def _ci_shared_memory_path(resolved_case: Dict[str, Any], *, run_dir: Path) -> str:
    runtime = _require_dict(resolved_case.get("runtime"), "resolved_case.runtime")
    stack_identity = _require_dict(runtime.get("stack_identity"), "resolved_case.runtime.stack_identity")
    shared_memory_root = _require_str(
        stack_identity.get("shared_memory_path"),
        "resolved_case.runtime.stack_identity.shared_memory_path",
    )
    # English note:
    # - iceoryx2 uses shared_memory_path as a base for per-node paths (e.g. .../nodes/<id>/iox2_<hash>/.service_tag).
    # - The per-node suffix can be long, and some filesystems enforce a max path length of 255 bytes.
    # - Therefore shared_memory_path must be short and must not embed run_dir (which can be deep under repo/workdir).
    token = hashlib.sha256(str(run_dir.resolve()).encode("utf-8")).hexdigest()[:16]
    return str((Path(shared_memory_root) / "ci" / token).resolve())


def _ci_shared_file_path(resolved_case: Dict[str, Any], *, run_dir: Path) -> str:
    runtime = _require_dict(resolved_case.get("runtime"), "resolved_case.runtime")
    stack_identity = _require_dict(runtime.get("stack_identity"), "resolved_case.runtime.stack_identity")
    shared_file_root = _require_str(
        stack_identity.get("shared_file_path"),
        "resolved_case.runtime.stack_identity.shared_file_path",
    )
    token = hashlib.sha256(str(run_dir.resolve()).encode("utf-8")).hexdigest()[:16]
    return str((Path(shared_file_root) / "ci" / token).resolve())


def _ci_owner_shared_bundle_paths(run_dir: Path, *, owner_config_path: Path) -> List[Path]:
    cfg = _require_dict(
        yaml.safe_load(owner_config_path.read_text(encoding="utf-8")),
        "ci_owner_0.yaml",
    )
    fluxonkv_spec = _require_dict(cfg.get("fluxonkv_spec"), "ci_owner_0.yaml.fluxonkv_spec")
    cluster_name = _require_str(
        fluxonkv_spec.get("cluster_name"),
        "ci_owner_0.yaml.fluxonkv_spec.cluster_name",
    )
    shm = _require_str(fluxonkv_spec.get("shared_memory_path"), "ci_owner_0.yaml.fluxonkv_spec.shared_memory_path")
    shared_file = _require_str(
        fluxonkv_spec.get("shared_file_path"),
        "ci_owner_0.yaml.fluxonkv_spec.shared_file_path",
    )
    return _shared_bundle_paths_for_cluster(
        shared_memory_root=shm,
        shared_file_root=shared_file,
        cluster_name=cluster_name,
    )


def _wait_ci_owner_shared_bundle_ready_and_stage_shared_json(
    resolved_case: Dict[str, Any],
    *,
    run_dir: Path,
    instance_id: str,
    shared_json_path: Path,
    mmap_file_path: Path,
    timeout_s: int,
) -> None:
    # English note:
    # - `shared_memory_path` is host-local. When owner_0 runs on a remote node, the runner host
    #   cannot see shared.json/mmap.file by filesystem path.
    # - CI execution already depends on the remote shared bundle being ready. Here we additionally
    #   fetch shared.json back to a stable local path for determinism and postmortem.
    # - We do NOT copy mmap.file here to avoid pulling large artifacts in prepare.
    out_shared_json = (run_dir / "services" / "share_mem" / "shared.json").resolve()
    out_shared_json.parent.mkdir(parents=True, exist_ok=True)

    deadline = time.time() + float(timeout_s)
    last_err: Optional[str] = None
    while True:
        if _instance_file_exists(resolved_case, instance_id=instance_id, path=mmap_file_path):
            raw = _instance_read_text_if_present(resolved_case, instance_id=instance_id, path=shared_json_path)
            if raw is not None:
                try:
                    meta = json.loads(raw)
                    if not isinstance(meta, dict):
                        raise ValueError("shared.json must be a JSON object")
                    required_str_keys = (
                        "owner_id",
                        "cluster_name",
                        "shared_memory_path",
                        "shared_file_path",
                        "protocol_version",
                    )
                    for k in required_str_keys:
                        v = meta.get(k)
                        if not isinstance(v, str) or not v.strip():
                            raise ValueError(f"shared.json missing/invalid key: {k}")
                    if not isinstance(meta.get("node_start_time"), int):
                        raise ValueError("shared.json missing/invalid key: node_start_time (int)")
                    if not isinstance(meta.get("segment_len"), int):
                        raise ValueError("shared.json missing/invalid key: segment_len (int)")
                    etcd_addresses = meta.get("etcd_addresses")
                    if not isinstance(etcd_addresses, list) or not etcd_addresses:
                        raise ValueError("shared.json missing/invalid key: etcd_addresses (non-empty list)")
                    if meta.get("cluster_name") != _ci_cluster_name(resolved_case):
                        raise ValueError(
                            f"shared.json cluster_name mismatch: shared={meta.get('cluster_name')!r} "
                            f"expected={_ci_cluster_name(resolved_case)!r}"
                        )
                    expected_shm_dir = str(mmap_file_path.parent.resolve())
                    if meta.get("shared_memory_path") != expected_shm_dir:
                        raise ValueError(
                            f"shared.json shared_memory_path mismatch: shared={meta.get('shared_memory_path')!r} "
                            f"expected={expected_shm_dir!r}"
                        )
                    expected_file_dir = str(shared_json_path.parent.resolve())
                    if meta.get("shared_file_path") != expected_file_dir:
                        raise ValueError(
                            f"shared.json shared_file_path mismatch: shared={meta.get('shared_file_path')!r} "
                            f"expected={expected_file_dir!r}"
                        )
                except Exception as exc:  # noqa: BLE001
                    last_err = f"{type(exc).__name__}: {exc}"
                else:
                    out_shared_json.write_text(raw, encoding="utf-8")
                    return

        status = _instance_status(resolved_case, instance_id=instance_id)
        exit_code = status.get("exit_code")
        if status.get("ok") is True and status.get("running") is False and isinstance(exit_code, int):
            raise ValueError(
                f"CI owner shared bundle: {instance_id} exited before shared.json was ready: "
                f"status={status} last_err={last_err}"
            )
        if time.time() >= deadline:
            raise ValueError(
                f"CI owner shared bundle: wait timeout; instance_id={instance_id} "
                f"shared_json={shared_json_path} mmap_file={mmap_file_path} status={status} last_err={last_err}"
            )
        time.sleep(2.0)


def _wait_ci_instance_ready(resolved_case: Dict[str, Any], *, instance_id: str) -> None:
    if instance_id == "master":
        _wait_instance_running(resolved_case, instance_id=instance_id, timeout_s=60)
        return
    if instance_id == "owner_0":
        _wait_instance_running(resolved_case, instance_id=instance_id, timeout_s=60)
        run_dir = _resolved_run_dir_path(resolved_case)
        owner_cfg_path = (run_dir / "configs" / "ci_owner_0.yaml").resolve()
        if not owner_cfg_path.exists():
            raise ValueError(f"CI owner config missing (cannot locate shared bundle path): {owner_cfg_path}")
        shared_json_path, mmap_file_path = _ci_owner_shared_bundle_paths(run_dir, owner_config_path=owner_cfg_path)
        _wait_ci_owner_shared_bundle_ready_and_stage_shared_json(
            resolved_case,
            run_dir=run_dir,
            instance_id=instance_id,
            shared_json_path=shared_json_path,
            mmap_file_path=mmap_file_path,
            timeout_s=180,
        )
        return
    if instance_id == "ci_runner":
        _wait_instance_running(resolved_case, instance_id=instance_id, timeout_s=30)
        return
    raise ValueError(f"unsupported CI readiness instance_id: {instance_id}")


def _wait_ci_base_runtime_service_ready(resolved_case: Dict[str, Any], *, service_id: str) -> None:
    endpoint_url = _ci_base_runtime_service_url(resolved_case, service_id=service_id)
    _wait_tcp_endpoint(endpoint_url, timeout_s=30)
    if service_id == "etcd":
        _wait_http_get_ok(endpoint_url + "/health", timeout_s=30)
        return
    if service_id == "greptime":
        return
    raise ValueError(f"unsupported CI base runtime service_id: {service_id}")


def _wait_ci_base_runtime_ready(resolved_case: Dict[str, Any]) -> None:
    for service_id in _runtime_layer_service_ids(resolved_case, layer=RUNTIME_LAYER_BASE):
        _wait_ci_base_runtime_service_ready(resolved_case, service_id=service_id)


def _wait_http_get_ok(url: str, *, timeout_s: int) -> None:
    deadline = time.time() + float(timeout_s)
    last_err: str | None = None
    while True:
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=3) as resp:
                _ = resp.read(256)
            return
        except Exception as exc:  # noqa: BLE001
            last_err = f"{type(exc).__name__}: {exc}"
        if time.time() >= deadline:
            raise ValueError(f"http not ready in {timeout_s}s: url={url} last_err={last_err}")
        time.sleep(0.5)


def _wait_tcp_host_port(host: str, port: int, *, timeout_s: int) -> None:
    deadline = time.time() + float(timeout_s)
    last_err: str | None = None
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(1.0)
            sock.connect((host, port))
            return
        except Exception as exc:  # noqa: BLE001
            last_err = f"{type(exc).__name__}: {exc}"
        finally:
            sock.close()
        if time.time() >= deadline:
            raise ValueError(f"tcp not ready in {timeout_s}s: host={host} port={port} last_err={last_err}")
        time.sleep(0.5)


def _wait_instance_tcp_ready(
    resolved_case: Dict[str, Any],
    *,
    instance_id: str,
    host: str,
    port: int,
    timeout_s: int,
) -> None:
    remote_access = _instance_remote_target_access_opt(resolved_case, instance_id=instance_id)
    if remote_access is None:
        _wait_tcp_host_port(host, port, timeout_s=timeout_s)
        return

    target_name, node_cfg, dispatch_mod = remote_access
    deadline = time.time() + float(timeout_s)
    last_err: Optional[str] = None
    probe_script = (
        "python3 - <<'PY'\n"
        "import socket\n"
        "sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)\n"
        "sock.settimeout(1.0)\n"
        f"sock.connect(('127.0.0.1', {int(port)}))\n"
        "sock.close()\n"
        "PY"
    )
    remote_cmd = "bash -lc " + dispatch_mod.sh_quote(probe_script)
    while True:
        try:
            _run_remote_bash_capture(
                target_name=target_name,
                node_cfg=node_cfg,
                remote_cmd=remote_cmd,
            )
            return
        except Exception as exc:  # noqa: BLE001
            last_err = f"{type(exc).__name__}: {exc}"
        if time.time() >= deadline:
            raise ValueError(
                f"tcp not ready in {timeout_s}s: instance_id={instance_id} host={host} port={port} last_err={last_err}"
            )
        time.sleep(0.5)


def _ci_local_runtime_targets(resolved_case: Dict[str, Any]) -> set[str]:
    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    target_ip_map = _require_dict(deploy.get("target_ip_map"), "resolved_case.deploy.target_ip_map")
    local_ipv4_addrs = _local_ipv4_addresses()
    out: set[str] = set()
    for target, ip in target_ip_map.items():
        target_name = _require_str(target, "resolved_case.deploy.target_ip_map key")
        ip_value = _require_str(ip, f"resolved_case.deploy.target_ip_map[{target_name!r}]")
        if ip_value in local_ipv4_addrs:
            out.add(target_name)
    return out


def _ci_required_ports(resolved_case: Dict[str, Any]) -> List[Tuple[str, int]]:
    resolved_case = _ci_runtime_cleanup_case(resolved_case, ctx="CI required ports")
    _ = _ci_local_runtime_targets(resolved_case)
    return []


def _ci_assert_ports_free(resolved_case: Dict[str, Any]) -> None:
    # CI only prechecks ports on targets hosted by the local machine.
    for name, port in _ci_required_ports(resolved_case):
        _assert_local_port_free(port, ctx=name)
def _wait_ci_ports_free(resolved_case: Dict[str, Any], *, timeout_s: int) -> None:
    deadline = time.time() + float(timeout_s)
    last_busy: List[str] = []
    while True:
        busy: List[Tuple[str, int]] = []
        for name, port in _ci_required_ports(resolved_case):
            try:
                _assert_local_port_free(port, ctx=name)
            except Exception:
                busy.append((name, port))
        if not busy:
            return
        last_busy = [f"{name}:{port}" for name, port in busy]
        if time.time() >= deadline:
            raise ValueError(f"CI cleanup left occupied ports after {timeout_s}s: {last_busy}")
        time.sleep(1.0)


def _assert_local_port_free(port: int, *, ctx: str) -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", int(port)))
    except OSError as exc:
        raise ValueError(
            f"port is already in use: {ctx} port={port}. "
            "Stop the stale process (e.g. via `ss -lntp`) and retry."
        ) from exc
    finally:
        sock.close()


def _runner_repo_root() -> Path:
    return RUNNER_REPO_ROOT


def _runner_test_stack_root() -> Path:
    return (_runner_repo_root() / "fluxon_test_stack").resolve()


def _bench_lock_dir() -> Path:
    lock_dir = RUNNER_SHARED_LOCK_DIR
    lock_dir.mkdir(parents=True, exist_ok=True)
    return lock_dir


def _acquire_named_lock(*, lock_path: Path, owner_lines: List[str], busy_message: str) -> Any:
    fp = lock_path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        fp.seek(0)
        holder = fp.read().strip()
        holder_lines = [line.strip() for line in holder.splitlines() if line.strip()]
        current_pid_line = f"pid={os.getpid()}"
        if current_pid_line in holder_lines:
            return fp
        fp.close()
        holder_text = holder if holder else "unknown"
        raise ValueError(f"{busy_message}: {lock_path} holder={holder_text}")
    fp.seek(0)
    fp.truncate()
    fp.write("\n".join(owner_lines) + "\n")
    fp.flush()
    return fp


def _acquire_ui_service_lock(*, workdir_root: Path) -> Any:
    lock_path = (workdir_root / _TEST_RUNNER_UI_LOCK_FILENAME).resolve()
    return _acquire_named_lock(
        lock_path=lock_path,
        owner_lines=[
            f"pid={os.getpid()}",
            f"repo_root={_runner_repo_root()}",
            f"workdir={workdir_root.resolve()}",
        ],
        busy_message=f"another test_runner_ui service is active for workdir={workdir_root.resolve()}",
    )


def _acquire_ci_lock() -> Any:
    lock_path = _bench_lock_dir() / "bench_ci.lock"
    return _acquire_named_lock(
        lock_path=lock_path,
        owner_lines=[f"pid={os.getpid()}", f"repo_root={_runner_repo_root()}"],
        busy_message="another CI run is active (lock busy)",
    )


def _suite_lock_name_for_ops_cluster_name(ops_cluster_name: str) -> str:
    return "bench_suite__" + hashlib.sha256(ops_cluster_name.encode("utf-8")).hexdigest()[:16] + ".lock"


def _acquire_suite_lock(*, ops_cluster_name: str, controller_url: str) -> Any:
    lock_path = _bench_lock_dir() / _suite_lock_name_for_ops_cluster_name(ops_cluster_name)
    return _acquire_named_lock(
        lock_path=lock_path,
        owner_lines=[
            f"pid={os.getpid()}",
            f"repo_root={_runner_repo_root()}",
            f"ops_cluster_name={ops_cluster_name}",
            f"controller_url={controller_url}",
        ],
        busy_message=f"another suite run is active for ops_cluster_name={ops_cluster_name} (lock busy)",
    )


def _acquire_controller_lock(resolved_case: Dict[str, Any], *, case_family: str, run_dir: Path) -> Any:
    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    controller_url = _require_str(deploy.get("controller_url"), "resolved_case.deploy.controller_url").rstrip("/")
    lock_name = "bench_controller__" + hashlib.sha256(controller_url.encode("utf-8")).hexdigest()[:16] + ".lock"
    lock_path = _bench_lock_dir() / lock_name
    return _acquire_named_lock(
        lock_path=lock_path,
        owner_lines=[
            f"pid={os.getpid()}",
            f"case_family={case_family}",
            f"run_dir={run_dir}",
            f"controller_url={controller_url}",
        ],
        busy_message=f"another deploy-backed run is active for controller_url={controller_url}",
    )





def _require_dict(d: Any, ctx: str) -> Dict[str, Any]:
    if not isinstance(d, dict):
        raise ValueError(f"{ctx} must be a mapping")
    return d


def _require_list(d: Any, ctx: str) -> List[Any]:
    if not isinstance(d, list):
        raise ValueError(f"{ctx} must be a list")
    return d


def _require_str(v: Any, ctx: str) -> str:
    if not isinstance(v, str) or not v.strip():
        raise ValueError(f"{ctx} must be a non-empty string")
    return v


def _require_basic_auth_username(v: Any, ctx: str) -> str:
    if not isinstance(v, str) or not v:
        raise ValueError(f"{ctx} must be a non-empty string")
    if v.strip() != v:
        raise ValueError(f"{ctx} must not have leading/trailing whitespace")
    if ":" in v:
        raise ValueError(f"{ctx} must not contain ':'")
    return v


def _require_basic_auth_password(v: Any, ctx: str) -> str:
    if not isinstance(v, str) or not v:
        raise ValueError(f"{ctx} must be a non-empty string")
    if v.strip() != v:
        raise ValueError(f"{ctx} must not have leading/trailing whitespace")
    return v


def _parse_controller_basic_auth(value: Any, *, field_name: str) -> Dict[str, str]:
    auth = _require_dict(value, field_name)
    return {
        "username": _require_basic_auth_username(auth.get("username"), f"{field_name}.username"),
        "password": _require_basic_auth_password(auth.get("password"), f"{field_name}.password"),
    }


def _install_controller_basic_auth(value: Any, *, field_name: str) -> None:
    auth = _parse_controller_basic_auth(value, field_name=field_name)
    raw = f"{auth['username']}:{auth['password']}".encode("utf-8")
    global _CONTROLLER_BASIC_AUTH_HEADER
    _CONTROLLER_BASIC_AUTH_HEADER = "Basic " + base64.b64encode(raw).decode("ascii")


def _require_int(v: Any, ctx: str, *, min_v: int, max_v: int | None = None) -> int:
    if not isinstance(v, int):
        raise ValueError(f"{ctx} must be int")
    if v < min_v:
        raise ValueError(f"{ctx} must be >= {min_v}")
    if max_v is not None and v > max_v:
        raise ValueError(f"{ctx} must be <= {max_v}")
    return v


def _require_bool(v: Any, ctx: str) -> bool:
    if not isinstance(v, bool):
        raise ValueError(f"{ctx} must be bool")
    return v


def _forbid_unknown_keys(d: Dict[str, Any], allowed: set[str], ctx: str) -> None:
    unknown = set(d.keys()) - allowed
    if unknown:
        raise ValueError(f"{ctx} contains unknown keys: {sorted(unknown)}")


def _forbid_removed_fluxon_kv_config_keys(kv_base: Dict[str, Any], ctx: str) -> Dict[str, Any]:
    if "rdma_device_names" in kv_base:
        raise ValueError(f"{ctx}.rdma_device_names has been removed from Fluxon KV config")

    fluxonkv_spec = _require_dict(kv_base.get("fluxonkv_spec"), f"{ctx}.fluxonkv_spec")
    if "transfer_engine" in fluxonkv_spec:
        raise ValueError(f"{ctx}.fluxonkv_spec.transfer_engine has been removed from Fluxon KV config")
    return fluxonkv_spec


def _normalize_test_stack_zero_contribution_pool(raw: Any, ctx: str) -> Dict[str, Any]:
    if raw is None:
        return {"dram": 0, "vram": {}}

    pool = _require_dict(raw, ctx)
    dram = _require_int(pool.get("dram"), f"{ctx}.dram", min_v=0)
    vram_raw = pool.get("vram")
    if vram_raw is None:
        vram = {}
    else:
        vram = _require_dict(vram_raw, f"{ctx}.vram")

    if int(dram) != 0:
        raise ValueError(f"{ctx}.dram must be 0 for TEST_STACK external baseline clients")

    normalized_vram: Dict[str, int] = {}
    for raw_gpu_id, raw_size in vram.items():
        gpu_id = _require_str(raw_gpu_id, f"{ctx}.vram key")
        size = _require_int(raw_size, f"{ctx}.vram[{gpu_id!r}]", min_v=0)
        if int(size) != 0:
            raise ValueError(
                f"{ctx}.vram[{gpu_id!r}] must be 0 for TEST_STACK external baseline clients"
            )
        normalized_vram[gpu_id] = int(size)

    return {
        "dram": 0,
        "vram": normalized_vram,
    }


def _normalize_test_spec_config(raw: Any, ctx: str) -> Dict[str, Any]:
    if raw is None:
        return {}
    cfg = _require_dict(raw, ctx)
    _forbid_unknown_keys(
        cfg,
        {
            "disable_observability",
            "disable_master_replica_cache",
            "disable_prefix_index",
            "disable_local_ipc",
            "disable_crossowner_ipc",
            "enable_iceoryx_logs",
            "iceoryx_external_busy_poll",
            "iceoryx_owner_client_busy_poll",
            "prefer_local_placement",
            "short_circuit_put_payload_path",
            "skip_put_end_commit",
            "p2p_transport_impl",
            "transport_mode",
            "tcp_thread_reactor_shard_count",
            "tcp_thread_bulk_lane_count",
            "tcp_thread_control_lane_count",
            "user_rpc_sync_handler_thread_count",
            "require_transfer_rpc_fast_path_ready_timeout_seconds",
            "rdma_device_names",
            "enable_side_transfer",
            "side_transfer_worker_count",
            "side_transfer_worker_p2p_port_base",
            "side_transfer_role",
        },
        ctx,
    )
    out: Dict[str, Any] = {}
    for key in (
        "disable_observability",
        "disable_master_replica_cache",
        "disable_prefix_index",
        "disable_local_ipc",
        "disable_crossowner_ipc",
        "enable_iceoryx_logs",
        "iceoryx_external_busy_poll",
        "iceoryx_owner_client_busy_poll",
        "prefer_local_placement",
        "short_circuit_put_payload_path",
        "skip_put_end_commit",
        "enable_side_transfer",
    ):
        if key in cfg:
            out[key] = _require_bool(cfg.get(key), f"{ctx}.{key}")
    if "p2p_transport_impl" in cfg:
        p2p_transport_impl = _require_str(cfg.get("p2p_transport_impl"), f"{ctx}.p2p_transport_impl")
        allowed_p2p_transport_impls = {"tcp", "tcp_thread"}
        if p2p_transport_impl not in allowed_p2p_transport_impls:
            raise ValueError(
                f"{ctx}.p2p_transport_impl must be one of {sorted(allowed_p2p_transport_impls)}, got {p2p_transport_impl!r}"
            )
        out["p2p_transport_impl"] = p2p_transport_impl
    transport_mode_was_explicit = "transport_mode" in cfg
    side_transfer_role_raw = cfg.get("side_transfer_role")
    default_transport_mode = None if side_transfer_role_raw == "worker" else "transfer_with_rpc"
    if "transport_mode" in cfg:
        transport_mode = _require_str(cfg.get("transport_mode"), f"{ctx}.transport_mode")
        allowed_transport_modes = {"transfer_only", "transfer_with_rpc"}
        if transport_mode not in allowed_transport_modes:
            raise ValueError(
                f"{ctx}.transport_mode must be one of {sorted(allowed_transport_modes)}, got {transport_mode!r}"
            )
        out["transport_mode"] = transport_mode
    if "rdma_device_names" in cfg:
        values = _require_list(cfg.get("rdma_device_names"), f"{ctx}.rdma_device_names")
        normalized: List[str] = []
        seen: set[str] = set()
        for idx, value in enumerate(values):
            device = _require_str(value, f"{ctx}.rdma_device_names[{idx}]").strip()
            if not device:
                raise ValueError(f"{ctx}.rdma_device_names[{idx}] must be a non-empty string")
            if device in seen:
                continue
            seen.add(device)
            normalized.append(device)
        if not normalized:
            raise ValueError(f"{ctx}.rdma_device_names must be non-empty")
        out["rdma_device_names"] = sorted(normalized)
    if "require_transfer_rpc_fast_path_ready_timeout_seconds" in cfg:
        timeout_seconds = _require_int(
            cfg.get("require_transfer_rpc_fast_path_ready_timeout_seconds"),
            f"{ctx}.require_transfer_rpc_fast_path_ready_timeout_seconds",
            min_v=1,
        )
        effective_transport_mode = out.get("transport_mode", default_transport_mode)
        if effective_transport_mode != "transfer_with_rpc":
            raise ValueError(
                f"{ctx}.require_transfer_rpc_fast_path_ready_timeout_seconds requires {ctx}.transport_mode=transfer_with_rpc"
            )
        if "rdma_device_names" not in out:
            raise ValueError(
                f"{ctx}.require_transfer_rpc_fast_path_ready_timeout_seconds requires explicit {ctx}.rdma_device_names"
            )
        out["require_transfer_rpc_fast_path_ready_timeout_seconds"] = timeout_seconds
    for key, min_v, max_v in (
        ("tcp_thread_reactor_shard_count", 1, 16),
        ("tcp_thread_bulk_lane_count", 1, 8),
        ("tcp_thread_control_lane_count", 1, 8),
    ):
        if key in cfg:
            out[key] = _require_int(cfg.get(key), f"{ctx}.{key}", min_v=min_v, max_v=max_v)
    if "user_rpc_sync_handler_thread_count" in cfg:
        out["user_rpc_sync_handler_thread_count"] = _require_int(
            cfg.get("user_rpc_sync_handler_thread_count"),
            f"{ctx}.user_rpc_sync_handler_thread_count",
            min_v=1,
        )
    if "side_transfer_worker_count" in cfg:
        out["side_transfer_worker_count"] = _require_int(
            cfg.get("side_transfer_worker_count"),
            f"{ctx}.side_transfer_worker_count",
            min_v=0,
        )
    if "side_transfer_worker_p2p_port_base" in cfg:
        out["side_transfer_worker_p2p_port_base"] = _require_int(
            cfg.get("side_transfer_worker_p2p_port_base"),
            f"{ctx}.side_transfer_worker_p2p_port_base",
            min_v=0,
        )
    if "side_transfer_role" in cfg:
        side_transfer_role = _require_str(cfg.get("side_transfer_role"), f"{ctx}.side_transfer_role")
        allowed_side_transfer_roles = {"worker"}
        if side_transfer_role not in allowed_side_transfer_roles:
            raise ValueError(
                f"{ctx}.side_transfer_role must be one of {sorted(allowed_side_transfer_roles)}, got {side_transfer_role!r}"
            )
        out["side_transfer_role"] = side_transfer_role
    if out.get("side_transfer_role") == "worker":
        if "rdma_device_names" in out and not transport_mode_was_explicit:
            raise ValueError(f"{ctx}.rdma_device_names requires {ctx}.transport_mode")
    elif "transport_mode" not in out:
        out["transport_mode"] = "transfer_with_rpc"
    if transport_mode_was_explicit and "rdma_device_names" not in out:
        raise ValueError(
            f"explicit {ctx}.transport_mode now requires {ctx}.rdma_device_names because it maps to TestForceEnableBypassRdmaControl and must avoid UCX default device selection"
        )
    return out


def _merge_test_spec_config_with_legacy_alias(
    *,
    test_spec_config: Dict[str, Any],
    legacy_benchmark_fast_path: Dict[str, Any],
    ctx: str,
) -> Dict[str, Any]:
    merged = copy.deepcopy(test_spec_config)
    for key, value in legacy_benchmark_fast_path.items():
        if key in merged and merged[key] != value:
            raise ValueError(
                f"{ctx} has conflicting benchmark_fast_path/test_spec_config value for {key!r}"
            )
        merged.setdefault(key, value)
    return merged


def _test_spec_config_runtime_view(test_spec_config: Dict[str, Any]) -> Dict[str, Any]:
    runtime_cfg = copy.deepcopy(test_spec_config)
    runtime_cfg.pop("p2p_transport_impl", None)
    # Keep transfer_with_rpc as an internal runner default only.
    # Writing it into runtime YAML makes it look explicit to fluxon_py/config.py,
    # which then requires rdma_device_names for no-RDMA tcp_thread cases.
    if runtime_cfg.get("transport_mode") == "transfer_with_rpc" and "rdma_device_names" not in runtime_cfg:
        runtime_cfg.pop("transport_mode", None)
    return runtime_cfg


def _test_spec_config_p2p_transport_impl(test_spec_config: Dict[str, Any]) -> Optional[str]:
    raw = test_spec_config.get("p2p_transport_impl")
    if raw is None:
        return None
    return _require_str(raw, "test_spec_config.p2p_transport_impl")


def _resolve_test_stack_fluxon_protocol_cfg(
    *,
    kv_base: Dict[str, Any],
    merged_test_spec_config: Dict[str, Any],
    ctx: str,
) -> Optional[Dict[str, Any]]:
    raw_protocol = kv_base.get("protocol")
    if raw_protocol is None:
        return None
    return copy.deepcopy(_require_dict(raw_protocol, f"{ctx}.protocol"))


def _rewrite_artifact_set_transport_impl(
    *,
    artifact_set_id: str,
    target_transport_impl: str,
    available_artifact_sets: Dict[str, Dict[str, Any]],
    ctx: str,
) -> str:
    current_transport_impl: Optional[str] = None
    if re.search(r"(^|[_-])tcp_thread(?=$|[_-])", artifact_set_id):
        current_transport_impl = "tcp_thread"
    elif re.search(r"(^|[_-])tcp(?=$|[_-])", artifact_set_id):
        current_transport_impl = "tcp"
    if current_transport_impl is None:
        raise ValueError(
            f"{ctx}={target_transport_impl!r} requires profiles[].artifact_set to contain a tcp/tcp_thread token, got {artifact_set_id!r}"
        )
    if current_transport_impl == target_transport_impl:
        return artifact_set_id
    candidate, replace_count = re.subn(
        rf"(^|[_-]){re.escape(current_transport_impl)}(?=$|[_-])",
        rf"\1{target_transport_impl}",
        artifact_set_id,
        count=1,
    )
    if replace_count != 1:
        raise ValueError(
            f"failed to rewrite artifact_set transport token: artifact_set={artifact_set_id!r} current={current_transport_impl!r} target={target_transport_impl!r}"
        )
    if candidate not in available_artifact_sets:
        raise ValueError(
            f"{ctx}={target_transport_impl!r} rewrote artifact_set {artifact_set_id!r} -> {candidate!r}, but target artifact_set is missing"
        )
    return candidate


def _resolved_case_artifact_set_id(
    *,
    case: _ResolvedCase,
    suite: _Suite,
    profile_src: Dict[str, Any],
) -> str:
    artifact_set_id = _require_str(
        profile_src.get("artifact_set"),
        f"profiles[{case.profile_id!r}].artifact_set",
    )
    runtime_src = _require_dict(
        profile_src.get("runtime"),
        f"profiles[{case.profile_id!r}].runtime",
    )
    profile_ts_raw = runtime_src.get("test_stack")
    if profile_ts_raw is None:
        return artifact_set_id
    profile_ts = _require_dict(
        profile_ts_raw,
        f"profiles[{case.profile_id!r}].runtime.test_stack",
    )
    runtime_config_raw = profile_ts.get("runtime_config")
    if runtime_config_raw is None:
        return artifact_set_id
    runtime_config = _require_dict(
        runtime_config_raw,
        f"profiles[{case.profile_id!r}].runtime.test_stack.runtime_config",
    )
    kv_base_raw = runtime_config.get("kv_base")
    if kv_base_raw is None:
        return artifact_set_id
    kv_base = _require_dict(
        kv_base_raw,
        f"profiles[{case.profile_id!r}].runtime.test_stack.runtime_config.kv_base",
    )
    test_spec_config = _normalize_test_spec_config(
        kv_base.get("test_spec_config"),
        f"profiles[{case.profile_id!r}].runtime.test_stack.runtime_config.kv_base.test_spec_config",
    )
    legacy_benchmark_fast_path = _normalize_test_spec_config(
        kv_base.get("benchmark_fast_path"),
        f"profiles[{case.profile_id!r}].runtime.test_stack.runtime_config.kv_base.benchmark_fast_path",
    )
    merged_test_spec_config = _merge_test_spec_config_with_legacy_alias(
        test_spec_config=test_spec_config,
        legacy_benchmark_fast_path=legacy_benchmark_fast_path,
        ctx=f"profiles[{case.profile_id!r}].runtime.test_stack.runtime_config.kv_base",
    )
    target_transport_impl = _test_spec_config_p2p_transport_impl(merged_test_spec_config)
    if target_transport_impl is None:
        return artifact_set_id
    backend_kind = _require_test_stack_backend_kind(
        profile_ts.get("kind"),
        f"profiles[{case.profile_id!r}].runtime.test_stack.kind",
    )
    if backend_kind != TEST_STACK_BACKEND_FLUXON:
        raise ValueError(
            "profiles[{!r}].runtime.test_stack.runtime_config.kv_base.test_spec_config.p2p_transport_impl "
            "is only valid when profiles[{!r}].runtime.test_stack.kind is FLUXON".format(
                case.profile_id,
                case.profile_id,
            )
        )
    return _rewrite_artifact_set_transport_impl(
        artifact_set_id=artifact_set_id,
        target_transport_impl=target_transport_impl,
        available_artifact_sets=suite.artifact_sets,
        ctx=f"profiles[{case.profile_id!r}].runtime.test_stack.runtime_config.kv_base.test_spec_config.p2p_transport_impl",
    )


def _normalize_test_stack_perf_config(raw: Any, ctx: str) -> Optional[Dict[str, Any]]:
    if raw is None:
        return None
    cfg = _require_dict(raw, ctx)
    _forbid_unknown_keys(
        cfg,
        {
            "enabled",
            "duration_seconds",
            "extra_buffer_seconds",
            "start_delay_seconds",
            "frequency_hz",
            "call_graph",
            "targets",
        },
        ctx,
    )
    if "enabled" in cfg and not _require_bool(cfg.get("enabled"), f"{ctx}.enabled"):
        return None

    out: Dict[str, Any] = {
        "extra_buffer_seconds": 60,
        "start_delay_seconds": 0,
        "frequency_hz": 99,
        "call_graph": "dwarf,16384",
        "targets": ["owner"],
    }
    if "duration_seconds" in cfg:
        out["duration_seconds"] = _require_int(cfg.get("duration_seconds"), f"{ctx}.duration_seconds", min_v=1)
    if "extra_buffer_seconds" in cfg:
        out["extra_buffer_seconds"] = _require_int(
            cfg.get("extra_buffer_seconds"),
            f"{ctx}.extra_buffer_seconds",
            min_v=0,
        )
    if "start_delay_seconds" in cfg:
        out["start_delay_seconds"] = _require_int(
            cfg.get("start_delay_seconds"),
            f"{ctx}.start_delay_seconds",
            min_v=0,
        )
    if "frequency_hz" in cfg:
        out["frequency_hz"] = _require_int(cfg.get("frequency_hz"), f"{ctx}.frequency_hz", min_v=1)
    if "call_graph" in cfg:
        out["call_graph"] = _require_str(cfg.get("call_graph"), f"{ctx}.call_graph")
    if "targets" in cfg:
        raw_targets = _require_list(cfg.get("targets"), f"{ctx}.targets")
        if not raw_targets:
            raise ValueError(f"{ctx}.targets must be non-empty when provided")
        allowed_targets = {"owner", "master"}
        targets: List[str] = []
        for idx, raw_target in enumerate(raw_targets):
            target = _require_str(raw_target, f"{ctx}.targets[{idx}]")
            if target not in allowed_targets:
                raise ValueError(
                    f"{ctx}.targets[{idx}] must be one of {sorted(allowed_targets)}, got {target!r}"
                )
            targets.append(target)
        out["targets"] = targets
    return out


def _normalize_runtime_env_map(raw: Any, ctx: str) -> Dict[str, str]:
    if raw is None:
        return {}
    env_map = _require_dict(raw, ctx)
    out: Dict[str, str] = {}
    for raw_key, raw_value in env_map.items():
        key = _require_str(raw_key, f"{ctx} key").strip()
        if not key:
            raise ValueError(f"{ctx} contains an empty variable name")
        out[key] = str(raw_value)
    return out


def _normalize_owner_cpu_core_by_target(
    raw: Any,
    ctx: str,
    *,
    target_ip_map: Optional[Dict[str, Any]] = None,
) -> Dict[str, int]:
    if raw is None:
        return {}
    core_map = _require_dict(raw, ctx)
    out: Dict[str, int] = {}
    for raw_target, raw_core in core_map.items():
        target = _require_str(raw_target, f"{ctx} key")
        if target_ip_map is not None:
            _ = _require_str(target_ip_map.get(target), f"resolved_case.deploy.target_ip_map[{target!r}]")
        if target in out:
            raise ValueError(f"{ctx} contains duplicate target: {target!r}")
        out[target] = _require_int(raw_core, f"{ctx}[{target!r}]", min_v=0)
    return out


def _render_runtime_env_exports(runtime_env: Dict[str, str]) -> str:
    return _render_env_exports(runtime_env)


def _parse_selector_ids(raw: Any, ctx: str) -> Optional[Tuple[str, ...]]:
    if isinstance(raw, str):
        value = _require_str(raw, ctx)
        if value != RUN_SELECTOR_ALL:
            raise ValueError(f"{ctx} must be {RUN_SELECTOR_ALL!r} or a non-empty list")
        return None
    items = _require_list(raw, ctx)
    if not items:
        raise ValueError(f"{ctx} must be non-empty when it is a list")
    out: List[str] = []
    seen: set[str] = set()
    for i, item in enumerate(items):
        value = _require_str(item, f"{ctx}[{i}]").strip()
        if _ID_RE.match(value) is None:
            raise ValueError(f"{ctx}[{i}] format invalid")
        if value in seen:
            raise ValueError(f"duplicate selector in {ctx}: {value}")
        out.append(value)
        seen.add(value)
    return tuple(out)


def _parse_case_selector_ids(raw: Any, ctx: str) -> Optional[Tuple[str, ...]]:
    if isinstance(raw, str):
        value = _require_str(raw, ctx)
        if value != RUN_SELECTOR_ALL:
            raise ValueError(f"{ctx} must be {RUN_SELECTOR_ALL!r} or a non-empty list")
        return None
    items = _require_list(raw, ctx)
    if not items:
        raise ValueError(f"{ctx} must be non-empty when it is a list")
    out: List[str] = []
    seen: set[str] = set()
    for i, item in enumerate(items):
        value = _require_str(item, f"{ctx}[{i}]").strip()
        if _CASE_ID_RE.match(value) is None:
            raise ValueError(f"{ctx}[{i}] format invalid")
        if value in seen:
            raise ValueError(f"duplicate selector in {ctx}: {value}")
        out.append(value)
        seen.add(value)
    return tuple(out)


def _parse_run_selectors(run: Dict[str, Any]) -> _RunSelectors:
    selectors = _require_dict(run.get("selectors"), "config.run.selectors")
    _forbid_unknown_keys(
        selectors,
        {"case_ids", "profile_ids", "command_ids", "test_ids"},
        "config.run.selectors",
    )
    raw_profile_ids = selectors.get("profile_ids")
    if raw_profile_ids is None:
        raise ValueError("config.run.selectors.profile_ids is required (non-empty list; no ALL)")
    profile_ids = _parse_selector_ids(raw_profile_ids, "config.run.selectors.profile_ids")
    if profile_ids is None:
        raise ValueError("config.run.selectors.profile_ids must be a non-empty list (no ALL)")
    return _RunSelectors(
        case_ids=_parse_case_selector_ids(selectors.get("case_ids"), "config.run.selectors.case_ids"),
        profile_ids=profile_ids,
        command_ids=_parse_selector_ids(selectors.get("command_ids"), "config.run.selectors.command_ids"),
        test_ids=_parse_selector_ids(selectors.get("test_ids"), "config.run.selectors.test_ids"),
    )


def _parse_suite_config(cfg: Any) -> _Suite:
    d = _require_dict(cfg, "config")
    if "matrix" in d:
        raise ValueError(
            "config.matrix is removed. "
            "To control what runs, edit scenes[].select (Scene constrains Scale and Profile)."
        )
    _forbid_unknown_keys(
        d,
        {
            "schema_version",
            "run",
            "scenes",
            "scales",
            "artifact_sets",
            "profiles",
        },
        "config",
    )

    schema_version = d.get("schema_version")
    if schema_version != SUITE_SCHEMA_VERSION:
        raise ValueError(f"config.schema_version must be {SUITE_SCHEMA_VERSION}")

    run = _require_dict(d.get("run"), "config.run")
    _forbid_unknown_keys(run, {"mode", "selectors"}, "config.run")
    run_mode = _require_str(run.get("mode"), "config.run.mode")
    if run_mode not in (RUN_MODE_DEBUG_ONE_BY_ONE, RUN_MODE_FULL_ONCE):
        raise ValueError(
            f"config.run.mode must be {RUN_MODE_DEBUG_ONE_BY_ONE!r} or {RUN_MODE_FULL_ONCE!r}, got: {run_mode!r}"
        )
    run_selectors = _parse_run_selectors(run)

    scenes_raw = _require_dict(d.get("scenes"), "config.scenes")
    scenes = _index_by_enum(scenes_raw, "scenes", _parse_scene)
    bad_scenes = [k for k in scenes.keys() if k not in SCENE_ENUMS_ALLOWED]
    if bad_scenes:
        raise ValueError(f"config.scenes contains unsupported scene enums in v{SUITE_SCHEMA_VERSION}: {sorted(bad_scenes)}")

    scales_raw = _require_dict(d.get("scales"), "config.scales")
    scales = _index_by_enum(scales_raw, "scales", _parse_scale)

    artifact_sets_raw = _require_dict(d.get("artifact_sets"), "config.artifact_sets")
    artifact_sets = _index_by_enum(artifact_sets_raw, "artifact_sets", _parse_artifact_set)

    profiles_raw = _require_dict(d.get("profiles"), "config.profiles")
    profiles = _index_by_enum(profiles_raw, "profiles", _parse_profile)
    for profile_id, profile in profiles.items():
        artifact_set_id = _require_str(
            profile.get("artifact_set"),
            f"profiles[{profile_id!r}].artifact_set",
        )
        if artifact_set_id not in artifact_sets:
            raise ValueError(
                f"profiles[{profile_id!r}].artifact_set references unknown artifact_sets enum: {artifact_set_id}"
            )

    unknown_profiles = [pid for pid in run_selectors.profile_ids if pid not in profiles]
    if unknown_profiles:
        raise ValueError(
            "config.run.selectors.profile_ids selects unknown profile enums: "
            + ", ".join(sorted(unknown_profiles))
        )

    return _Suite(
        run_mode=run_mode,
        run_selectors=run_selectors,
        scenes=scenes,
        scales=scales,
        artifact_sets=artifact_sets,
        profiles=profiles,
    )

def _prune_case_runs_to_case_ids(case_runs: Dict[str, Any], *, case_ids: set[str]) -> int:
    if not case_ids:
        raise ValueError("case_ids must be non-empty for case_runs pruning")
    raw_cases = case_runs.get("cases")
    if raw_cases is None:
        return 0
    cases = _require_list(raw_cases, "case_runs.cases")
    kept: List[Dict[str, Any]] = []
    removed = 0
    for idx, raw in enumerate(cases):
        rec = _require_dict(raw, f"case_runs.cases[{idx}]")
        case_id = _require_str(rec.get("case_id"), f"case_runs.cases[{idx}].case_id")
        if case_id in case_ids:
            kept.append(rec)
        else:
            removed += 1
    if removed:
        case_runs["cases"] = kept
    return int(removed)


def _clean_workdir(workdir_root: Path) -> None:
    workdir_root = Path(workdir_root).resolve()
    if not workdir_root.exists() or not workdir_root.is_dir():
        raise ValueError(f"clean_workdir requires an existing directory: {workdir_root}")

    owned_paths = [
        workdir_root / "case_runs.yaml",
        workdir_root / "results",
        workdir_root / "analysis",
        # Stage archives for remote run_dir sync (created under workdir to avoid OS temp namespaces).
        workdir_root / "_stage_tmp",
    ]
    removed_any = False
    for p in owned_paths:
        if not p.exists():
            continue
        removed_any = True
        if p.is_dir():
            shutil.rmtree(p)
        else:
            p.unlink()
        print(f"[clean_workdir] removed: {p}", flush=True)
    if not removed_any:
        print(f"[clean_workdir] nothing to remove under: {workdir_root}", flush=True)


def _repair_reserved_last_runs(case_runs: Dict[str, Any], *, results_root: Path) -> List[str]:
    """Repair reserved-only `last_run` entries based on existing run_dir artifacts.

    English note:
    - Reserving a run_index persists `last_run.run_index` early for observability.
    - If the runner is killed after the remote CI job wrote `exit_code.txt` but before finalize(),
      case_runs.yaml stays stuck with a reserved-only last_run and the run_dir summary.yaml remains
      at the initial INCOMPLETE placeholder state.
    - Repair is deterministic: we only finalize a reserved entry when we can read a concrete
      `exit_code.txt` from the local run_dir (preferred) or the remote target (if the CI runner
      is deployed as a remote instance).
    """
    repaired_case_ids: List[str] = []
    cases = _require_list(case_runs.get("cases"), "case_runs.cases")
    for i, raw in enumerate(cases):
        rec = _require_dict(raw, f"case_runs.cases[{i}]")
        case_id = _require_str(rec.get("case_id"), f"case_runs.cases[{i}].case_id")
        last_run = _require_dict(rec.get("last_run"), f"case_runs.cases[{i}].last_run")
        run_index = _require_int(last_run.get("run_index"), f"case_runs.cases[{i}].last_run.run_index", min_v=0)
        if run_index <= 0:
            continue
        if "outcome" in last_run:
            continue

        run_dir = (results_root / case_id / f"run_{run_index}").resolve()
        resolved_case_path = run_dir / "resolved_case.yaml"
        if not resolved_case_path.exists():
            continue
        resolved_case = _load_run_dir_resolved_case(run_dir)
        if _resolved_case_family(resolved_case) != CASE_FAMILY_CI:
            continue

        exit_code_path = (run_dir / "logs" / "ci_runner" / "exit_code.txt").resolve()
        if exit_code_path.exists():
            raw_rc = exit_code_path.read_text(encoding="utf-8").strip()
        else:
            # Some deployments run the CI runner remotely (as a deploy instance) and only stage
            # exit_code.txt back later. In that case we can attempt remote reads.
            deploy = resolved_case.get("deploy")
            if not isinstance(deploy, dict) or not isinstance(deploy.get("instances"), list):
                continue
            if not any(isinstance(inst, dict) and inst.get("id") == "ci_runner" for inst in deploy.get("instances")):
                continue
            remote_raw = _instance_read_text_if_present(resolved_case, instance_id="ci_runner", path=exit_code_path)
            if remote_raw is None:
                continue
            raw_rc = remote_raw.strip()
        try:
            rc = int(raw_rc)
        except ValueError as exc:
            raise ValueError(
                f"reserved run exit_code.txt is not an int: case_id={case_id} run_index={run_index} raw={raw_rc!r}"
            ) from exc

        outcome = RUN_OUTCOME_SUCCESS if rc == 0 else RUN_OUTCOME_FAILED
        case_obj = _require_dict(resolved_case.get("case"), "repair.resolved_case.case")
        run_mode = _require_str(case_obj.get("run_mode"), "repair.resolved_case.case.run_mode")
        has_selection = case_obj.get("command_id") is not None or case_obj.get("test_id") is not None
        counted = (outcome == RUN_OUTCOME_SUCCESS) and (run_mode == RUN_MODE_FULL_ONCE) and (not has_selection)

        rec["total_runs"] = int(rec.get("total_runs", 0)) + 1
        if outcome == RUN_OUTCOME_SUCCESS:
            rec["success_runs"] = int(rec.get("success_runs", 0)) + 1
        else:
            rec["failed_runs"] = int(rec.get("failed_runs", 0)) + 1
        if counted:
            rec["counted_runs"] = int(rec.get("counted_runs", 0)) + 1

        finished_at = int(time.time())
        rec["last_run"] = {
            "run_index": int(run_index),
            "outcome": outcome,
            "finished_at_unix_s": int(finished_at),
        }

        summary_path = run_dir / "summary.yaml"
        if summary_path.exists():
            s = _require_dict(_load_yaml_file(summary_path), "repair.summary")
            s["outcome"] = outcome
            s["counted"] = bool(counted)
            timing = s.get("timing")
            if isinstance(timing, dict):
                timing["finished_at_unix_s"] = int(finished_at)
            if outcome == RUN_OUTCOME_SUCCESS:
                s.pop("error", None)
            else:
                if s.get("error") is None:
                    s["error"] = f"recovered reserved CI run: rc={rc}"
            _write_yaml_file(summary_path, s)

        total_runs = int(rec.get("total_runs", 0))
        success_runs = int(rec.get("success_runs", 0))
        failed_runs = int(rec.get("failed_runs", 0))
        counted_runs = int(rec.get("counted_runs", 0))
        if total_runs != success_runs + failed_runs:
            raise ValueError(f"case_runs invariant failed after reserved repair: total_runs case_id={case_id}")
        if counted_runs > success_runs:
            raise ValueError(f"case_runs invariant failed after reserved repair: counted_runs case_id={case_id}")

        repaired_case_ids.append(case_id)
    return repaired_case_ids

def _index_by_enum(
    items: Dict[str, Any], ctx: str, parse_item: Any
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for k, raw in items.items():
        if not isinstance(k, str):
            raise ValueError(f"{ctx} enum key must be a string, got: {type(k)}")
        key = k.strip()
        if _ID_RE.match(key) is None:
            raise ValueError(f"{ctx} enum key format invalid: {k!r}")
        if key in out:
            raise ValueError(f"duplicate {ctx} enum key: {key}")
        item = _require_dict(raw, f"{ctx}[{key!r}]")
        item = parse_item(item, f"{ctx}[{key!r}]")
        out[key] = item
    if not out:
        raise ValueError(f"{ctx} must be non-empty")
    return out


def _scene_kind_from_item(item: Dict[str, Any], ctx: str) -> str:
    blocks = [k for k in ("infer", "ci", "test_stack") if item.get(k) is not None]
    if len(blocks) != 1:
        raise ValueError(f"{ctx} must contain exactly one of infer/ci/test_stack blocks")
    kind = blocks[0]
    if kind == "infer":
        return SCENE_KIND_INFER
    if kind == "ci":
        return SCENE_KIND_CI
    if kind == "test_stack":
        return SCENE_KIND_TEST_STACK
    raise ValueError(f"{ctx} unsupported kind: {kind!r}")


def _require_id_list(raw: Any, ctx: str) -> List[str]:
    values = _require_list(raw, ctx)
    if not values:
        raise ValueError(f"{ctx} must be non-empty")
    out: List[str] = []
    seen: set[str] = set()
    for i, raw_value in enumerate(values):
        value = _require_str(raw_value, f"{ctx}[{i}]").strip()
        if _ID_RE.match(value) is None:
            raise ValueError(f"{ctx}[{i}] format invalid")
        if value in seen:
            raise ValueError(f"duplicate id in {ctx}: {value}")
        out.append(value)
        seen.add(value)
    return out


def _require_scene_subject(raw: Any, ctx: str) -> str:
    subject = _require_str(raw, ctx).strip()
    if subject not in SCENE_SUBJECTS_ALLOWED:
        raise ValueError(f"{ctx} invalid subject: {subject!r}")
    return subject


def _infer_test_stack_subject_from_mode(mode: str) -> str:
    if mode == TEST_STACK_MODE_MPMC:
        return SCENE_SUBJECT_MQ
    if mode in (TEST_STACK_MODE_KVSTORE, TEST_STACK_MODE_KVSTORE_WITH_LOCAL_CACHE, TEST_STACK_MODE_RPC):
        return SCENE_SUBJECT_KV
    if mode == TEST_STACK_MODE_PY_FS:
        return SCENE_SUBJECT_FS
    raise ValueError(f"unsupported test_stack mode for subject inference: {mode!r}")


def _require_test_stack_backend_kind(raw: Any, ctx: str) -> str:
    if raw is None:
        return TEST_STACK_BACKEND_FLUXON
    kind = _require_str(raw, ctx).strip().upper()
    if kind not in TEST_STACK_BACKENDS_ALLOWED:
        raise ValueError(
            f"{ctx} invalid test_stack backend kind: {kind!r}; expected one of {sorted(TEST_STACK_BACKENDS_ALLOWED)}"
        )
    return kind


def _test_stack_backend_supports_subject(*, backend_kind: str, subject: str) -> bool:
    if backend_kind == TEST_STACK_BACKEND_FLUXON:
        return subject in {SCENE_SUBJECT_KV, SCENE_SUBJECT_MQ, SCENE_SUBJECT_FS}
    if backend_kind == TEST_STACK_BACKEND_REDIS:
        return subject == SCENE_SUBJECT_KV
    if backend_kind == TEST_STACK_BACKEND_MOONCAKE:
        return subject == SCENE_SUBJECT_KV
    if backend_kind == TEST_STACK_BACKEND_ALLUXIO:
        return subject == SCENE_SUBJECT_FS
    raise ValueError(f"unsupported test_stack backend_kind: {backend_kind!r}")


def _validate_test_stack_backend_subject(*, backend_kind: str, subject: str, ctx: str) -> None:
    if _test_stack_backend_supports_subject(backend_kind=backend_kind, subject=subject):
        return
    raise ValueError(
        f"{ctx} backend_kind={backend_kind!r} does not support subject={subject!r}"
    )


def _test_stack_backend_requires_fluxon_kv_master(*, backend_kind: str, mode: str) -> bool:
    if backend_kind != TEST_STACK_BACKEND_FLUXON:
        return False
    return _test_stack_mode_requires_kv_master(mode)


def _test_stack_backend_uses_external_fluxon_kv(*, backend_kind: str, mode: str) -> bool:
    if backend_kind != TEST_STACK_BACKEND_FLUXON:
        return False
    return mode in (
        TEST_STACK_MODE_MPMC,
        TEST_STACK_MODE_KVSTORE,
        TEST_STACK_MODE_KVSTORE_WITH_LOCAL_CACHE,
        TEST_STACK_MODE_PY_FS,
        TEST_STACK_MODE_RPC,
    )


def _test_stack_backend_uses_dedicated_kv_owners(*, backend_kind: str, mode: str) -> bool:
    if backend_kind == TEST_STACK_BACKEND_FLUXON:
        return _test_stack_backend_uses_external_fluxon_kv(backend_kind=backend_kind, mode=mode)
    if backend_kind == TEST_STACK_BACKEND_MOONCAKE:
        return mode in (
            TEST_STACK_MODE_KVSTORE,
            TEST_STACK_MODE_KVSTORE_WITH_LOCAL_CACHE,
            TEST_STACK_MODE_RPC,
        )
    return False


def _require_ci_runtime_contract(raw: Any, ctx: str) -> str:
    runtime_contract = _require_str(raw, ctx).strip()
    if runtime_contract not in CI_RUNTIME_CONTRACT_IDS:
        raise ValueError(f"{ctx} invalid ci runtime_contract: {runtime_contract!r}")
    return runtime_contract


def _validate_test_stack_subject_mode(*, subject: str, mode: str, ctx: str) -> None:
    if subject == SCENE_SUBJECT_MQ:
        if mode != TEST_STACK_MODE_MPMC:
            raise ValueError(f"{ctx} subject={subject!r} requires mode={TEST_STACK_MODE_MPMC!r}")
        return
    if subject == SCENE_SUBJECT_KV:
        if mode not in (TEST_STACK_MODE_KVSTORE, TEST_STACK_MODE_KVSTORE_WITH_LOCAL_CACHE, TEST_STACK_MODE_RPC):
            raise ValueError(f"{ctx} subject={subject!r} requires a KV-family mode")
        return
    if subject == SCENE_SUBJECT_FS:
        if mode != TEST_STACK_MODE_PY_FS:
            raise ValueError(f"{ctx} subject={subject!r} requires mode={TEST_STACK_MODE_PY_FS!r}")
        return
    raise ValueError(f"{ctx} unsupported test_stack subject: {subject!r}")


def _parse_scene_value_size_weighted_set(raw_val: Any, *, ctx: str) -> List[Dict[str, Any]]:
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
            raise ValueError(f"{item_ctx}.size_bytes must be > 0")
        weight = float(item["weight"])
        if weight <= 0:
            raise ValueError(f"{item_ctx}.weight must be > 0")
        parsed.append({"size_bytes": size_bytes, "weight": weight})
    return parsed


_TEST_STACK_NODE_TARGET_RE = re.compile(r"node-([1-9][0-9]*)$")


def _validate_test_stack_targets(*, topology: Any, targets: Any, ctx: str) -> None:
    machine_count = _require_test_stack_machine_count(topology, f"{ctx}.__machine_count__")
    _ = _normalize_test_stack_target_hosts(targets, machine_count=machine_count, ctx=ctx)


def _require_test_stack_machine_count(raw_topology: Any, field_name: str) -> int:
    if isinstance(raw_topology, bool):
        raise ValueError(f"{field_name} must not be boolean")
    if isinstance(raw_topology, int):
        if raw_topology <= 0:
            raise ValueError(f"{field_name} must be > 0")
        return int(raw_topology)
    if isinstance(raw_topology, str):
        topology = raw_topology.strip()
        if re.fullmatch(r"[1-9][0-9]*", topology):
            return int(topology)
    raise ValueError(f"{field_name} must be a positive integer machine count")


def _parse_test_stack_port_alloc_topology_key(raw_key: Any, *, field_name: str) -> Tuple[Optional[int], bool]:
    if isinstance(raw_key, bool):
        raise ValueError(f"{field_name} invalid topology key: {raw_key!r}")
    if isinstance(raw_key, int):
        if raw_key <= 0:
            raise ValueError(f"{field_name} invalid machine-count key: {raw_key!r}")
        return int(raw_key), False

    key = _require_str(raw_key, field_name).strip()
    if re.fullmatch(r"[1-9][0-9]*", key):
        return int(key), False
    if key == TEST_STACK_TOPOLOGY_DEFAULT:
        return None, True
    raise ValueError(
        f"{field_name} invalid topology key: {raw_key!r}; expected a positive integer machine count "
        f"or fallback key {TEST_STACK_TOPOLOGY_DEFAULT!r}"
    )


def _ordered_auto_test_stack_target_candidates(*, target_ip_map: Dict[str, Any], ctx: str) -> List[str]:
    ordered_numeric: List[Tuple[int, str]] = []
    ordered_fallback: List[str] = []
    seen: set[str] = set()
    for raw_target in target_ip_map.keys():
        target = _require_str(raw_target, f"{ctx} key")
        if target in seen or "bastion" in target.lower():
            continue
        seen.add(target)
        m = _TEST_STACK_NODE_TARGET_RE.fullmatch(target)
        if m is not None:
            ordered_numeric.append((int(m.group(1)), target))
        else:
            ordered_fallback.append(target)
    ordered = [target for _, target in sorted(ordered_numeric)] + ordered_fallback
    if not ordered:
        raise ValueError(f"{ctx} has no usable targets for automatic TEST_STACK placement")
    return ordered


def _auto_test_stack_target_hosts(
    *,
    machine_count: int,
    ctx: str,
    target_ip_map: Optional[Dict[str, Any]],
    exclude_hosts: Optional[List[str]] = None,
) -> List[str]:
    excluded = set(exclude_hosts or [])
    if target_ip_map is None:
        auto_hosts = [f"__auto_target_{idx + 1}__" for idx in range(machine_count)]
        if len(excluded) > 0:
            auto_hosts = [host for host in auto_hosts if host not in excluded]
        if len(auto_hosts) < machine_count:
            raise ValueError(
                f"{ctx} automatic TEST_STACK placement has insufficient synthetic targets after exclusion: "
                f"machine_count={machine_count} excluded={sorted(excluded)}"
            )
        return auto_hosts[:machine_count]

    candidates = _ordered_auto_test_stack_target_candidates(
        target_ip_map=target_ip_map,
        ctx=f"{ctx}.target_ip_map",
    )
    filtered = [target for target in candidates if target not in excluded]
    if len(filtered) < machine_count:
        raise ValueError(
            f"{ctx} automatic TEST_STACK placement needs {machine_count} targets but only has "
            f"{len(filtered)} usable target(s) after exclusion {sorted(excluded)}"
        )
    return filtered[:machine_count]


def _normalize_test_stack_target_hosts(
    raw_targets: Any,
    *,
    machine_count: int,
    ctx: str,
    target_ip_map: Optional[Dict[str, Any]] = None,
) -> List[str]:
    if raw_targets is None:
        return _auto_test_stack_target_hosts(
            machine_count=machine_count,
            ctx=ctx,
            target_ip_map=target_ip_map,
        )

    targets = _require_dict(raw_targets, ctx)
    exclude_hosts: Optional[List[str]] = None
    raw_exclude_hosts = targets.get("exclude_hosts")
    if raw_exclude_hosts is not None:
        exclude_list = _require_list(raw_exclude_hosts, f"{ctx}.exclude_hosts")
        exclude_hosts = []
        for idx, raw_host in enumerate(exclude_list):
            exclude_hosts.append(_require_str(raw_host, f"{ctx}.exclude_hosts[{idx}]"))

    if "hosts" in targets:
        _forbid_unknown_keys(targets, {"hosts", "exclude_hosts"}, ctx)
        if exclude_hosts:
            raise ValueError(f"{ctx}.exclude_hosts must not be combined with explicit hosts")
        hosts = _require_list(targets.get("hosts"), f"{ctx}.hosts")
        if len(hosts) != machine_count:
            raise ValueError(
                f"{ctx}.hosts length must equal machine count: machine_count={machine_count} hosts={len(hosts)}"
            )
        out_hosts: List[str] = []
        seen: set[str] = set()
        for idx, raw_host in enumerate(hosts):
            host = _require_str(raw_host, f"{ctx}.hosts[{idx}]")
            if host in seen:
                raise ValueError(f"{ctx}.hosts contains duplicate target: {host!r}")
            seen.add(host)
            out_hosts.append(host)
        return out_hosts

    if exclude_hosts is not None:
        _forbid_unknown_keys(targets, {"exclude_hosts"}, ctx)
        return _auto_test_stack_target_hosts(
            machine_count=machine_count,
            ctx=ctx,
            target_ip_map=target_ip_map,
            exclude_hosts=exclude_hosts,
        )

    if machine_count == 1:
        allowed_target_keys = {"primary"}
    elif machine_count == 2:
        allowed_target_keys = {"primary", "secondary"}
    else:
        raise ValueError(
            f"{ctx} machine_count={machine_count} requires targets.hosts=[...] instead of legacy primary/secondary keys"
        )
    _forbid_unknown_keys(targets, allowed_target_keys, ctx)

    ordered_keys = ["primary"] if machine_count == 1 else ["primary", "secondary"]
    out_hosts = [_require_str(targets.get(key), f"{ctx}.{key}") for key in ordered_keys]
    if len(set(out_hosts)) != len(out_hosts):
        raise ValueError(f"{ctx} legacy targets must be distinct, got: {out_hosts}")
    return out_hosts


def _test_stack_scale_machine_targets(
    scale: Dict[str, Any],
    *,
    ctx: str,
    target_ip_map: Optional[Dict[str, Any]] = None,
) -> List[str]:
    machine_count = _require_test_stack_machine_count(scale.get("topology"), f"{ctx}.topology")
    return _normalize_test_stack_target_hosts(
        scale.get("targets"),
        machine_count=machine_count,
        ctx=f"{ctx}.targets",
        target_ip_map=target_ip_map,
    )


def _validate_test_stack_port_alloc_entry(
    port_alloc: Dict[str, Any],
    ctx: str,
    *,
    backend_kind: str,
) -> None:
    allowed = {
        "coordinator_port_base",
        "coordinator_port_stride",
    }
    if backend_kind == TEST_STACK_BACKEND_FLUXON:
        allowed.update(
            {
                "kv_master_port_base",
                "kv_master_port_stride",
                "kv_p2p_port_base",
                "kv_p2p_port_stride",
            }
        )
    elif backend_kind == TEST_STACK_BACKEND_REDIS:
        allowed.update(
            {
                "redis_port_base",
                "redis_port_stride",
            }
        )
    elif backend_kind == TEST_STACK_BACKEND_MOONCAKE:
        allowed.update(
            {
                "mooncake_rpc_port_base",
                "mooncake_rpc_port_stride",
                "mooncake_metadata_port_base",
                "mooncake_metadata_port_stride",
                "mooncake_metrics_port_base",
                "mooncake_metrics_port_stride",
            }
        )
    elif backend_kind != TEST_STACK_BACKEND_ALLUXIO:
        raise ValueError(f"{ctx} unsupported test_stack backend_kind: {backend_kind!r}")

    _forbid_unknown_keys(port_alloc, allowed, ctx)
    base_port = _require_int(port_alloc.get("coordinator_port_base"), f"{ctx}.coordinator_port_base", min_v=1)
    stride = _require_int(port_alloc.get("coordinator_port_stride"), f"{ctx}.coordinator_port_stride", min_v=1)
    if base_port > 65535:
        raise ValueError(f"{ctx}.coordinator_port_base out of range")
    if stride > 65535:
        raise ValueError(f"{ctx}.coordinator_port_stride out of range")

    if backend_kind == TEST_STACK_BACKEND_FLUXON:
        master_base = _require_int(port_alloc.get("kv_master_port_base"), f"{ctx}.kv_master_port_base", min_v=1)
        master_stride = _require_int(port_alloc.get("kv_master_port_stride"), f"{ctx}.kv_master_port_stride", min_v=1)
        if master_base > 65535:
            raise ValueError(f"{ctx}.kv_master_port_base out of range")
        if master_stride > 65535:
            raise ValueError(f"{ctx}.kv_master_port_stride out of range")

        # English note:
        # - TEST_STACK nodes can be co-located on one machine and would otherwise collide on implicit P2P listen ports.
        # - Keep port allocation explicit in suite config so failures remain configuration-visible.
        p2p_base = _require_int(port_alloc.get("kv_p2p_port_base"), f"{ctx}.kv_p2p_port_base", min_v=1)
        p2p_stride = _require_int(port_alloc.get("kv_p2p_port_stride"), f"{ctx}.kv_p2p_port_stride", min_v=1)
        if p2p_base > 65535:
            raise ValueError(f"{ctx}.kv_p2p_port_base out of range")
        if p2p_stride > 65535:
            raise ValueError(f"{ctx}.kv_p2p_port_stride out of range")
        return

    if backend_kind == TEST_STACK_BACKEND_REDIS:
        redis_base = _require_int(port_alloc.get("redis_port_base"), f"{ctx}.redis_port_base", min_v=1)
        redis_stride = _require_int(port_alloc.get("redis_port_stride"), f"{ctx}.redis_port_stride", min_v=1)
        if redis_base > 65535:
            raise ValueError(f"{ctx}.redis_port_base out of range")
        if redis_stride > 65535:
            raise ValueError(f"{ctx}.redis_port_stride out of range")
        return

    if backend_kind == TEST_STACK_BACKEND_MOONCAKE:
        for field in (
            "mooncake_rpc_port_base",
            "mooncake_rpc_port_stride",
            "mooncake_metadata_port_base",
            "mooncake_metadata_port_stride",
            "mooncake_metrics_port_base",
            "mooncake_metrics_port_stride",
        ):
            value = _require_int(port_alloc.get(field), f"{ctx}.{field}", min_v=1)
            if value > 65535:
                raise ValueError(f"{ctx}.{field} out of range")


def _resolve_test_stack_port_alloc(raw_port_alloc: Any, *, topology: Any, backend_kind: str, ctx: str) -> Dict[str, Any]:
    port_alloc = _require_dict(raw_port_alloc, ctx)
    _forbid_unknown_keys(port_alloc, {"by_topology"}, ctx)
    by_topology = _require_dict(port_alloc.get("by_topology"), f"{ctx}.by_topology")
    if not by_topology:
        raise ValueError(f"{ctx}.by_topology must be non-empty")
    machine_count = _require_test_stack_machine_count(topology, f"{ctx}.selected_topology")
    exact_entries: Dict[int, Dict[str, Any]] = {}
    default_entry: Optional[Dict[str, Any]] = None
    for topology_key, raw_entry in by_topology.items():
        entry_ctx = f"{ctx}.by_topology[{topology_key!r}]"
        entry = _require_dict(raw_entry, entry_ctx)
        _validate_test_stack_port_alloc_entry(entry, entry_ctx, backend_kind=backend_kind)

        selector_count, is_default = _parse_test_stack_port_alloc_topology_key(
            topology_key,
            field_name=f"{ctx}.by_topology key",
        )
        if is_default:
            default_entry = entry
            continue
        assert selector_count is not None
        exact_entries[int(selector_count)] = entry

    selected: Optional[Dict[str, Any]] = exact_entries.get(machine_count)
    if selected is None:
        selected = default_entry
    if selected is None:
        raise ValueError(
            f"{ctx}.by_topology missing selected topology for machine_count={machine_count}; "
            f"expected exact key {machine_count!r} or fallback key {TEST_STACK_TOPOLOGY_DEFAULT!r}"
        )
    return copy.deepcopy(selected)


def _ci_target_token_mapping(scale_ci: Dict[str, Any], *, ctx: str) -> Dict[str, str]:
    machine_count = _require_test_stack_machine_count(scale_ci.get("topology"), f"{ctx}.topology")
    if machine_count not in (1, 2):
        raise ValueError(f"{ctx}.topology invalid for CI: machine_count={machine_count}")
    targets = _require_dict(scale_ci.get("targets"), f"{ctx}.targets")
    _validate_test_stack_targets(topology=machine_count, targets=targets, ctx=f"{ctx}.targets")
    mapping = {"__PRIMARY__": _require_str(targets.get("primary"), f"{ctx}.targets.primary")}
    if machine_count == 2:
        mapping["__SECONDARY__"] = _require_str(targets.get("secondary"), f"{ctx}.targets.secondary")
    return mapping


def _compile_ci_profile_instances(
    profile_ci: Dict[str, Any],
    *,
    scale_ci: Dict[str, Any],
    ctx: str,
) -> List[Dict[str, Any]]:
    deploy = _require_dict(profile_ci.get("deploy"), f"{ctx}.deploy")
    target_ip_map = _require_dict(deploy.get("target_ip_map"), f"{ctx}.deploy.target_ip_map")
    raw_instances = _require_list(deploy.get("instances"), f"{ctx}.deploy.instances")
    if not raw_instances:
        raise ValueError(f"{ctx}.deploy.instances must be non-empty")
    mapping = _ci_target_token_mapping(scale_ci, ctx=f"{ctx}.scale_ci")
    compiled: List[Dict[str, Any]] = []
    for index, raw in enumerate(raw_instances):
        inst = _require_dict(raw, f"{ctx}.deploy.instances[{index}]")
        compiled_inst = _require_dict(
            _subst_obj_tokens(inst, mapping, f"{ctx}.deploy.instances[{index}]"),
            f"{ctx}.deploy.instances[{index}].compiled",
        )
        deployer = _require_dict(compiled_inst.get("deployer"), f"{ctx}.deploy.instances[{index}].compiled.deployer")
        target = _require_str(deployer.get("target"), f"{ctx}.deploy.instances[{index}].compiled.deployer.target")
        if target not in target_ip_map:
            raise ValueError(f"{ctx}.deploy.instances[{index}] compiled target not found in deploy.target_ip_map: {target}")
        compiled.append(compiled_inst)
    return compiled


def _parse_scene_select(item: Dict[str, Any], ctx: str) -> None:
    sel = _require_dict(item.get("select"), f"{ctx}.select")
    _forbid_unknown_keys(sel, {"scales", "profiles"}, f"{ctx}.select")
    _require_id_list(sel.get("scales"), f"{ctx}.select.scales")
    _require_id_list(sel.get("profiles"), f"{ctx}.select.profiles")


def _parse_ci_commands(raw_commands: Any, ctx: str) -> List[Dict[str, Any]]:
    commands = _require_list(raw_commands, ctx)
    if not commands:
        raise ValueError(f"{ctx} must be non-empty")
    out: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    for i, raw_command in enumerate(commands):
        command = _require_dict(raw_command, f"{ctx}[{i}]")
        _forbid_unknown_keys(command, {"id", "command", "test_ids", "test_id_arg", "timeout_seconds"}, f"{ctx}[{i}]")
        command_id = _require_str(command.get("id"), f"{ctx}[{i}].id").strip()
        if _ID_RE.match(command_id) is None:
            raise ValueError(f"{ctx}[{i}].id format invalid")
        if command_id in seen_ids:
            raise ValueError(f"duplicate command id in {ctx}: {command_id}")
        command_text = _require_str(command.get("command"), f"{ctx}[{i}].command")
        rec: Dict[str, Any] = {"id": command_id, "command": command_text}
        if command.get("timeout_seconds") is not None:
            rec["timeout_seconds"] = _require_int(
                command.get("timeout_seconds"),
                f"{ctx}[{i}].timeout_seconds",
                min_v=1,
            )
        raw_test_ids = command.get("test_ids")
        raw_test_id_arg = command.get("test_id_arg")
        if raw_test_ids is None:
            if raw_test_id_arg is not None:
                raise ValueError(f"{ctx}[{i}] sets test_id_arg without test_ids")
        else:
            test_ids = _require_list(raw_test_ids, f"{ctx}[{i}].test_ids")
            if not test_ids:
                raise ValueError(f"{ctx}[{i}].test_ids must be non-empty")
            test_id_arg = _require_str(raw_test_id_arg, f"{ctx}[{i}].test_id_arg")
            normalized_test_ids: List[str] = []
            seen_test_ids: set[str] = set()
            for j, raw_test_id in enumerate(test_ids):
                test_id = _require_str(raw_test_id, f"{ctx}[{i}].test_ids[{j}]").strip()
                if _ID_RE.match(test_id) is None:
                    raise ValueError(f"{ctx}[{i}].test_ids[{j}] format invalid")
                if test_id in seen_test_ids:
                    raise ValueError(f"duplicate test id in {ctx}[{i}]: {test_id}")
                normalized_test_ids.append(test_id)
                seen_test_ids.add(test_id)
            rec["test_ids"] = normalized_test_ids
            rec["test_id_arg"] = test_id_arg
        out.append(rec)
        seen_ids.add(command_id)
    return out


def _parse_scene(item: Dict[str, Any], ctx: str) -> Dict[str, Any]:
    kind = _scene_kind_from_item(item, ctx)
    _parse_scene_select(item, ctx)

    if kind == SCENE_KIND_INFER:
        _forbid_unknown_keys(item, {"infer", "select"}, ctx)
        infer = _require_dict(item.get("infer"), f"{ctx}.infer")
        _forbid_unknown_keys(infer, {"pattern"}, f"{ctx}.infer")
        pattern = _require_str(infer.get("pattern"), f"{ctx}.infer.pattern")
        if pattern not in (INFER_PATTERN_REPEAT, INFER_PATTERN_UNIQUE):
            raise ValueError(f"{ctx}.infer.pattern invalid")
        return item

    if kind == SCENE_KIND_CI:
        _forbid_unknown_keys(item, {"ci", "select"}, ctx)
        ci = _require_dict(item.get("ci"), f"{ctx}.ci")
        _forbid_unknown_keys(ci, {"subject", "commands", "runtime_contract"}, f"{ctx}.ci")
        subject = _require_scene_subject(ci.get("subject"), f"{ctx}.ci.subject")
        if subject == SCENE_SUBJECT_INFER:
            raise ValueError(f"{ctx}.ci.subject must not be {SCENE_SUBJECT_INFER!r}")
        item["ci"] = {
            "subject": subject,
            "runtime_contract": _require_ci_runtime_contract(
                ci.get("runtime_contract"),
                f"{ctx}.ci.runtime_contract",
            ),
            "commands": _parse_ci_commands(ci.get("commands"), f"{ctx}.ci.commands"),
        }
        return item

    _forbid_unknown_keys(item, {"test_stack", "select"}, ctx)
    ts = _require_dict(item.get("test_stack"), f"{ctx}.test_stack")
    _forbid_unknown_keys(
        ts,
        {
            "subject",
            "mode",
            "role_weights",
            "read_ratio",
            "write_ratio",
            "request_distribution",
            "keyspace_size",
            TEST_STACK_SCENE_KEY_AFFINITY_LOCALITY_RATIO,
            "value_size_mode",
            "value_size_weighted_set",
            "file_size_bytes",
            "chunk_size_bytes",
            "files_per_worker",
            "cache_max_bytes",
            *TEST_STACK_RPC_SCENE_KEYS,
        },
        f"{ctx}.test_stack",
    )
    mode = _require_str(ts.get("mode"), f"{ctx}.test_stack.mode")
    subject_raw = ts.get("subject")
    subject = _infer_test_stack_subject_from_mode(mode)
    if subject_raw is not None:
        subject = _require_scene_subject(subject_raw, f"{ctx}.test_stack.subject")
        _validate_test_stack_subject_mode(subject=subject, mode=mode, ctx=f"{ctx}.test_stack")

    out_ts: Dict[str, Any] = {"subject": subject, "mode": mode}
    role_weights = ts.get("role_weights")
    if mode == TEST_STACK_MODE_MPMC:
        rw = _require_dict(role_weights, f"{ctx}.test_stack.role_weights")
        expected_roles = {"producer", "consumer"}
        _forbid_unknown_keys(rw, expected_roles, f"{ctx}.test_stack.role_weights")
        for role in sorted(expected_roles):
            weight = rw.get(role)
            if not isinstance(weight, (int, float)):
                raise ValueError(f"{ctx}.test_stack.role_weights.{role} must be number")
        out_ts["role_weights"] = copy.deepcopy(rw)
        item["test_stack"] = out_ts
        return item

    if role_weights is not None:
        raise ValueError(f"{ctx}.test_stack.role_weights is only allowed for mode={TEST_STACK_MODE_MPMC}")

    if mode in (TEST_STACK_MODE_KVSTORE, TEST_STACK_MODE_KVSTORE_WITH_LOCAL_CACHE, TEST_STACK_MODE_RPC):
        read_ratio = ts.get("read_ratio")
        write_ratio = ts.get("write_ratio")
        if read_ratio is not None or write_ratio is not None:
            if not isinstance(read_ratio, (int, float)):
                raise ValueError(f"{ctx}.test_stack.read_ratio must be number when present")
            if not isinstance(write_ratio, (int, float)):
                raise ValueError(f"{ctx}.test_stack.write_ratio must be number when present")
            if float(read_ratio) < 0.0 or float(write_ratio) < 0.0:
                raise ValueError(f"{ctx}.test_stack read/write ratio must be >= 0")
            if float(read_ratio) + float(write_ratio) <= 0.0:
                raise ValueError(f"{ctx}.test_stack read/write ratio sum must be > 0")
            out_ts["read_ratio"] = float(read_ratio)
            out_ts["write_ratio"] = float(write_ratio)

        request_distribution = ts.get("request_distribution")
        if request_distribution is not None:
            request_distribution_str = _require_str(
                request_distribution,
                f"{ctx}.test_stack.request_distribution",
            ).strip().lower()
            if request_distribution_str not in TEST_STACK_REQUEST_DISTRIBUTIONS_ALLOWED:
                raise ValueError(
                    f"{ctx}.test_stack.request_distribution invalid: {request_distribution_str!r}"
                )
            out_ts["request_distribution"] = request_distribution_str

        keyspace_size = ts.get("keyspace_size")
        if keyspace_size is not None:
            out_ts["keyspace_size"] = _require_int(
                keyspace_size,
                f"{ctx}.test_stack.keyspace_size",
                min_v=1,
            )

        affinity_locality_ratio = ts.get(TEST_STACK_SCENE_KEY_AFFINITY_LOCALITY_RATIO)
        if affinity_locality_ratio is not None:
            if not isinstance(affinity_locality_ratio, (int, float)):
                raise ValueError(
                    f"{ctx}.test_stack.{TEST_STACK_SCENE_KEY_AFFINITY_LOCALITY_RATIO} must be number"
                )
            affinity_ratio_f = float(affinity_locality_ratio)
            if affinity_ratio_f < 0.0 or affinity_ratio_f > 1.0:
                raise ValueError(
                    f"{ctx}.test_stack.{TEST_STACK_SCENE_KEY_AFFINITY_LOCALITY_RATIO} must be in [0, 1]"
                )
            out_ts[TEST_STACK_SCENE_KEY_AFFINITY_LOCALITY_RATIO] = affinity_ratio_f

        value_size_mode = ts.get("value_size_mode")
        if value_size_mode is not None:
            value_size_mode_str = _require_str(
                value_size_mode,
                f"{ctx}.test_stack.value_size_mode",
            ).strip().upper()
            if value_size_mode_str not in {"FIXED", "RANDOM_WEIGHTED_SET"}:
                raise ValueError(
                    f"{ctx}.test_stack.value_size_mode invalid: {value_size_mode_str!r}"
                )
            out_ts["value_size_mode"] = value_size_mode_str
            if value_size_mode_str == "RANDOM_WEIGHTED_SET":
                out_ts["value_size_weighted_set"] = _parse_scene_value_size_weighted_set(
                    ts.get("value_size_weighted_set"),
                    ctx=f"{ctx}.test_stack.value_size_weighted_set",
                )
            elif ts.get("value_size_weighted_set") is not None:
                raise ValueError(
                    f"{ctx}.test_stack.value_size_weighted_set is only allowed when value_size_mode=RANDOM_WEIGHTED_SET"
                )
        elif ts.get("value_size_weighted_set") is not None:
            raise ValueError(
                f"{ctx}.test_stack.value_size_weighted_set requires test_stack.value_size_mode"
            )
        if mode == TEST_STACK_MODE_RPC:
            rpc_backend_kind = _require_str(
                ts.get("rpc_backend_kind"),
                f"{ctx}.test_stack.rpc_backend_kind",
            ).strip().upper()
            if rpc_backend_kind not in TEST_STACK_RPC_BACKENDS_ALLOWED:
                raise ValueError(
                    f"{ctx}.test_stack.rpc_backend_kind invalid: {rpc_backend_kind!r}"
                )
            rpc_path = _require_str(ts.get("rpc_path"), f"{ctx}.test_stack.rpc_path").strip()
            if not rpc_path:
                raise ValueError(f"{ctx}.test_stack.rpc_path must be non-empty")
            rpc_payload_size = _require_int(
                ts.get("rpc_payload_size"),
                f"{ctx}.test_stack.rpc_payload_size",
                min_v=1,
            )
            rpc_payload_mode_raw = ts.get("rpc_payload_mode")
            if rpc_payload_mode_raw is None:
                if rpc_backend_kind == TEST_STACK_RPC_BACKEND_FLUXON:
                    raise ValueError(f"{ctx}.test_stack.rpc_payload_mode is required")
                rpc_payload_mode_raw = TEST_STACK_RPC_PAYLOAD_MODE_BYTES
            rpc_payload_mode = _require_str(
                rpc_payload_mode_raw,
                f"{ctx}.test_stack.rpc_payload_mode",
            ).strip().upper()
            if rpc_payload_mode not in TEST_STACK_RPC_PAYLOAD_MODES_ALLOWED:
                raise ValueError(
                    f"{ctx}.test_stack.rpc_payload_mode invalid: {rpc_payload_mode!r}"
                )
            rpc_server_source_raw = ts.get("rpc_server_source")
            if rpc_server_source_raw is None:
                if rpc_backend_kind == TEST_STACK_RPC_BACKEND_FLUXON:
                    raise ValueError(f"{ctx}.test_stack.rpc_server_source is required")
                rpc_server_source = TEST_STACK_RPC_SERVER_SOURCE_BENCHMARK_NODE_ROLE
            else:
                rpc_server_source = _require_str(
                    rpc_server_source_raw,
                    f"{ctx}.test_stack.rpc_server_source",
                ).strip()
            if rpc_server_source not in TEST_STACK_RPC_SERVER_SOURCES_ALLOWED:
                raise ValueError(
                    f"{ctx}.test_stack.rpc_server_source invalid: {rpc_server_source!r}"
                )
            out_ts["rpc_backend_kind"] = rpc_backend_kind
            out_ts["rpc_path"] = rpc_path
            out_ts["rpc_payload_size"] = rpc_payload_size
            out_ts["rpc_payload_mode"] = rpc_payload_mode
            out_ts["rpc_server_source"] = rpc_server_source
            if rpc_server_source == TEST_STACK_RPC_SERVER_SOURCE_BENCHMARK_NODE_ROLE:
                rpc_target_role = canonicalize_kv_node_role(
                    _require_str(ts.get("rpc_target_role"), f"{ctx}.test_stack.rpc_target_role").strip()
                )
                if rpc_target_role not in TEST_STACK_RPC_TARGET_ROLES_ALLOWED:
                    raise ValueError(
                        f"{ctx}.test_stack.rpc_target_role invalid: {rpc_target_role!r}"
                    )
                out_ts["rpc_target_role"] = rpc_target_role
            elif rpc_server_source == TEST_STACK_RPC_SERVER_SOURCE_BENCHMARK_NODE_ALL:
                if ts.get("rpc_target_role") is not None:
                    raise ValueError(
                        f"{ctx}.test_stack.rpc_target_role is not allowed when "
                        f"{ctx}.test_stack.rpc_server_source={TEST_STACK_RPC_SERVER_SOURCE_BENCHMARK_NODE_ALL}"
                    )
            elif ts.get("rpc_target_role") is not None:
                raise ValueError(
                    f"{ctx}.test_stack.rpc_target_role is only allowed when "
                    f"{ctx}.test_stack.rpc_server_source={TEST_STACK_RPC_SERVER_SOURCE_BENCHMARK_NODE_ROLE}"
                )
            if (
                rpc_backend_kind == TEST_STACK_RPC_BACKEND_ZERORPC
                and rpc_payload_mode != TEST_STACK_RPC_PAYLOAD_MODE_BYTES
            ):
                raise ValueError(
                    f"{ctx}.test_stack.rpc_backend_kind={TEST_STACK_RPC_BACKEND_ZERORPC} "
                    f"requires rpc_payload_mode={TEST_STACK_RPC_PAYLOAD_MODE_BYTES}"
                )
            if rpc_backend_kind == TEST_STACK_RPC_BACKEND_ZERORPC:
                out_ts["zerorpc_port_base"] = _require_int(
                    ts.get("zerorpc_port_base"),
                    f"{ctx}.test_stack.zerorpc_port_base",
                    min_v=1,
                    max_v=65535,
                )
                out_ts["zerorpc_port_stride"] = _require_int(
                    ts.get("zerorpc_port_stride"),
                    f"{ctx}.test_stack.zerorpc_port_stride",
                    min_v=1,
                    max_v=65535,
                )
            else:
                for key in ("zerorpc_port_base", "zerorpc_port_stride"):
                    if ts.get(key) is not None:
                        raise ValueError(
                            f"{ctx}.test_stack.{key} is only allowed when rpc_backend_kind={TEST_STACK_RPC_BACKEND_ZERORPC}"
                        )
        else:
            for key in TEST_STACK_RPC_SCENE_KEYS:
                if ts.get(key) is not None:
                    raise ValueError(
                        f"{ctx}.test_stack.{key} is only allowed for mode={TEST_STACK_MODE_RPC}"
                    )
    elif mode == TEST_STACK_MODE_PY_FS:
        for key in (
            "read_ratio",
            "write_ratio",
            "request_distribution",
            "keyspace_size",
            "value_size_mode",
            "value_size_weighted_set",
            *TEST_STACK_RPC_SCENE_KEYS,
        ):
            if ts.get(key) is not None:
                raise ValueError(f"{ctx}.test_stack.{key} is not allowed for mode={TEST_STACK_MODE_PY_FS}")
        if ts.get("file_size_bytes") is not None:
            out_ts["file_size_bytes"] = _require_int(
                ts.get("file_size_bytes"),
                f"{ctx}.test_stack.file_size_bytes",
                min_v=1,
            )
        if ts.get("chunk_size_bytes") is not None:
            out_ts["chunk_size_bytes"] = _require_int(
                ts.get("chunk_size_bytes"),
                f"{ctx}.test_stack.chunk_size_bytes",
                min_v=1,
            )
        if ts.get("files_per_worker") is not None:
            out_ts["files_per_worker"] = _require_int(
                ts.get("files_per_worker"),
                f"{ctx}.test_stack.files_per_worker",
                min_v=1,
            )
        if ts.get("cache_max_bytes") is not None:
            out_ts["cache_max_bytes"] = _require_int(
                ts.get("cache_max_bytes"),
                f"{ctx}.test_stack.cache_max_bytes",
                min_v=1,
            )

    item["test_stack"] = out_ts
    return item


def _parse_scale(item: Dict[str, Any], ctx: str) -> Dict[str, Any]:
    _forbid_unknown_keys(item, {"duration_seconds", "topology", "targets", "owner", "benchmark", "infer"}, ctx)

    # English note:
    # - Scale ids are shared by CI and BENCH scenes.
    # - Current convention is one owner-contributing process per target host, so owner_count == machine count.
    # - CI scenes choose a compatible subset via scene.select.scales; CI runtime is still single-owner today.
    # - "infer" scales are mutually exclusive with the runtime (ci/bench) scale fields.
    if item.get("infer") is not None:
        if any(item.get(k) is not None for k in ("duration_seconds", "topology", "targets", "owner", "benchmark")):
            raise ValueError(f"{ctx}.infer must not be combined with duration_seconds/topology/targets/owner/benchmark")

    infer = item.get("infer")
    if infer is not None:
        infer_d = _require_dict(infer, f"{ctx}.infer")
        _forbid_unknown_keys(
            infer_d,
            {"client_concurrency", "prompt_tokens", "output_tokens"},
            f"{ctx}.infer",
        )
        _ = _require_int(infer_d.get("client_concurrency"), f"{ctx}.infer.client_concurrency", min_v=1)
        _ = _require_int(infer_d.get("prompt_tokens"), f"{ctx}.infer.prompt_tokens", min_v=1)
        _ = _require_int(infer_d.get("output_tokens"), f"{ctx}.infer.output_tokens", min_v=1)
        return item

    _ = _require_int(item.get("duration_seconds"), f"{ctx}.duration_seconds", min_v=1)
    topology = item.get("topology")
    _ = _require_test_stack_machine_count(topology, f"{ctx}.topology")
    _validate_test_stack_targets(
        topology=topology,
        targets=item.get("targets"),
        ctx=f"{ctx}.targets",
    )

    owner = item.get("owner")
    if owner is None:
        raise ValueError(f"{ctx}.owner is required")
    owner_d = _require_dict(owner, f"{ctx}.owner")
    _forbid_unknown_keys(owner_d, {"owner_count", "owner_dram_bytes", "targets"}, f"{ctx}.owner")
    owner_count = _require_int(owner_d.get("owner_count"), f"{ctx}.owner.owner_count", min_v=1)
    _ = _require_int(owner_d.get("owner_dram_bytes"), f"{ctx}.owner.owner_dram_bytes", min_v=16777216)
    _ = _test_stack_explicit_owner_targets(
        owner_scale=owner_d,
        owner_count=int(owner_count),
        ctx=f"{ctx}.owner",
    )

    benchmark = item.get("benchmark")
    if benchmark is not None:
        bench = _require_dict(benchmark, f"{ctx}.benchmark")
        _forbid_unknown_keys(
            bench,
            {
                "processes_per_target",
                "threads_per_process",
                "owner_group_processes",
                "value_size",
                "metric_warmup_seconds",
                "start_idle_seconds",
                "op_timeout_seconds",
                "cluster_ready_timeout_seconds",
                "value_size_list",
                "consumer_sim_handle_ms_range",
            },
            f"{ctx}.benchmark",
        )
        _ = _require_int(
            bench.get("processes_per_target"),
            f"{ctx}.benchmark.processes_per_target",
            min_v=1,
        )
        threads_per_process = _require_int(
            bench.get("threads_per_process"),
            f"{ctx}.benchmark.threads_per_process",
            min_v=1,
        )
        if int(threads_per_process) != TEST_STACK_BENCHMARK_FIXED_THREADS_PER_PROCESS:
            raise ValueError(
                f"{ctx}.benchmark.threads_per_process must be fixed to "
                f"{TEST_STACK_BENCHMARK_FIXED_THREADS_PER_PROCESS}"
            )
        owner_group_processes = bench.get("owner_group_processes")
        if owner_group_processes is not None:
            _ = _require_int(owner_group_processes, f"{ctx}.benchmark.owner_group_processes", min_v=1)
        _ = _require_int(bench.get("value_size"), f"{ctx}.benchmark.value_size", min_v=0)
        _ = _require_int(
            bench.get("cluster_ready_timeout_seconds"),
            f"{ctx}.benchmark.cluster_ready_timeout_seconds",
            min_v=1,
        )
        warm = bench.get("metric_warmup_seconds")
        if not isinstance(warm, (int, float)):
            raise ValueError(f"{ctx}.benchmark.metric_warmup_seconds must be number")
        if float(warm) < 0.0:
            raise ValueError(f"{ctx}.benchmark.metric_warmup_seconds must be >= 0")
        op_to = bench.get("op_timeout_seconds")
        if not isinstance(op_to, (int, float)):
            raise ValueError(f"{ctx}.benchmark.op_timeout_seconds must be number")
        if float(op_to) <= 0.0:
            raise ValueError(f"{ctx}.benchmark.op_timeout_seconds must be > 0")
        vl = _require_list(bench.get("value_size_list"), f"{ctx}.benchmark.value_size_list")
        for j, x in enumerate(vl):
            _ = _require_int(x, f"{ctx}.benchmark.value_size_list[{j}]", min_v=1)
        csr = bench.get("consumer_sim_handle_ms_range")
        if csr is not None:
            r = _require_list(csr, f"{ctx}.benchmark.consumer_sim_handle_ms_range")
            if len(r) != 2:
                raise ValueError(f"{ctx}.benchmark.consumer_sim_handle_ms_range must have 2 items")
            _ = _require_int(r[0], f"{ctx}.benchmark.consumer_sim_handle_ms_range[0]", min_v=0)
            _ = _require_int(r[1], f"{ctx}.benchmark.consumer_sim_handle_ms_range[1]", min_v=0)
            if int(r[0]) > int(r[1]):
                raise ValueError(f"{ctx}.benchmark.consumer_sim_handle_ms_range invalid: {r}")

    return item


def _validate_profile_deploy_block(
    deploy: Dict[str, Any],
    ctx: str,
    *,
    allow_instances: bool,
    allow_target_tokens: bool,
) -> None:
    _forbid_unknown_keys(
        deploy,
        {
            "adapter_cmd",
            "controller_url",
            "target_ip_map",
            "instances",
            "bootstrap_ready_timeout_seconds",
        },
        ctx,
    )
    adapter_cmd = _require_list(deploy.get("adapter_cmd"), f"{ctx}.adapter_cmd")
    if not adapter_cmd:
        raise ValueError(f"{ctx}.adapter_cmd must be non-empty")
    for j, x in enumerate(adapter_cmd):
        _ = _require_str(x, f"{ctx}.adapter_cmd[{j}]")
    if os.path.isabs(str(adapter_cmd[0])):
        raise ValueError(f"{ctx}.adapter_cmd[0] must be config-relative")

    controller_url = _require_str(deploy.get("controller_url"), f"{ctx}.controller_url")
    if (
        not controller_url.startswith("http://")
        and not controller_url.startswith("https://")
        and not _is_runtime_token_placeholder(controller_url)
    ):
        raise ValueError(f"{ctx}.controller_url must start with http:// or https://")

    _ = _require_int(deploy.get("bootstrap_ready_timeout_seconds"), f"{ctx}.bootstrap_ready_timeout_seconds", min_v=1)

    target_ip_map = _require_dict(deploy.get("target_ip_map"), f"{ctx}.target_ip_map")
    for k, v in target_ip_map.items():
        if not isinstance(k, str) or not k.strip():
            raise ValueError(f"{ctx}.target_ip_map has invalid key: {k!r}")
        if not isinstance(v, str) or not v.strip():
            raise ValueError(f"{ctx}.target_ip_map[{k!r}] must be non-empty string")

    instances_raw = deploy.get("instances")
    if not allow_instances:
        if instances_raw is not None:
            raise ValueError(f"{ctx}.instances must be omitted")
        return

    instances = _require_list(instances_raw, f"{ctx}.instances")
    if not instances:
        raise ValueError(f"{ctx}.instances must be non-empty")

    endpoint_count = 0
    seen_ids: set[str] = set()
    for j, raw in enumerate(instances):
        inst = _require_dict(raw, f"{ctx}.instances[{j}]")
        _forbid_unknown_keys(inst, {"id", "k8s_ref", "lifecycle", "endpoint", "deployer"}, f"{ctx}.instances[{j}]")
        iid = _require_str(inst.get("id"), f"{ctx}.instances[{j}].id")
        if iid in seen_ids:
            raise ValueError(f"duplicate instance id in {ctx}.instances: {iid}")
        seen_ids.add(iid)

        lifecycle = _require_str(inst.get("lifecycle"), f"{ctx}.instances[{j}].lifecycle")
        if lifecycle not in ("service", "job"):
            raise ValueError(f"{ctx}.instances[{j}].lifecycle must be 'service' or 'job'")

        k8s_ref = _require_str(inst.get("k8s_ref"), f"{ctx}.instances[{j}].k8s_ref")
        if "/" not in k8s_ref:
            raise ValueError(
                f"{ctx}.instances[{j}].k8s_ref must be '<deployment|daemonset>/<name>' in v{SUITE_SCHEMA_VERSION}, got: {k8s_ref!r}"
            )
        k8s_ref_kind, k8s_ref_name = k8s_ref.split("/", 1)
        if k8s_ref_kind not in (K8S_REF_KIND_DEPLOYMENT, K8S_REF_KIND_DAEMONSET) or not k8s_ref_name.strip():
            raise ValueError(
                f"{ctx}.instances[{j}].k8s_ref must be '<deployment|daemonset>/<name>' in v{SUITE_SCHEMA_VERSION}, got: {k8s_ref!r}"
            )

        deployer = _require_dict(inst.get("deployer"), f"{ctx}.instances[{j}].deployer")
        _forbid_unknown_keys(deployer, {"target", "payload_file", "payload_dest_path", "command", "args", "working_dir"}, f"{ctx}.instances[{j}].deployer")
        target = _require_str(deployer.get("target"), f"{ctx}.instances[{j}].deployer.target")
        if allow_target_tokens and target in ("__PRIMARY__", "__SECONDARY__"):
            pass
        elif target not in target_ip_map:
            raise ValueError(f"{ctx}.instances[{j}].deployer.target not found in deploy.target_ip_map: {target}")

        raw_payload_file = deployer.get("payload_file")
        raw_payload_dest_path = deployer.get("payload_dest_path")
        if raw_payload_file is not None or raw_payload_dest_path is not None:
            payload_file = _require_str(raw_payload_file, f"{ctx}.instances[{j}].deployer.payload_file")
            if os.path.isabs(payload_file):
                raise ValueError(f"{ctx}.instances[{j}].deployer.payload_file must be workdir-relative")
            payload_dest_path = _require_str(raw_payload_dest_path, f"{ctx}.instances[{j}].deployer.payload_dest_path")
            if not payload_dest_path.startswith("/"):
                raise ValueError(f"{ctx}.instances[{j}].deployer.payload_dest_path must be absolute path")

        cmd_list = _require_list(deployer.get("command"), f"{ctx}.instances[{j}].deployer.command")
        if not cmd_list:
            raise ValueError(f"{ctx}.instances[{j}].deployer.command must be non-empty")
        for k, x in enumerate(cmd_list):
            _ = _require_str(x, f"{ctx}.instances[{j}].deployer.command[{k}]")

        args_list = deployer.get("args")
        if args_list is not None:
            al = _require_list(args_list, f"{ctx}.instances[{j}].deployer.args")
            for k, x in enumerate(al):
                _ = _require_str(x, f"{ctx}.instances[{j}].deployer.args[{k}]")

        wd = deployer.get("working_dir")
        if wd is not None:
            _ = _require_str(wd, f"{ctx}.instances[{j}].deployer.working_dir")

        ep = inst.get("endpoint")
        if ep is not None:
            endpoint_count += 1
            ep_d = _require_dict(ep, f"{ctx}.instances[{j}].endpoint")
            _forbid_unknown_keys(ep_d, {"scheme", "host_port"}, f"{ctx}.instances[{j}].endpoint")
            scheme = _require_str(ep_d.get("scheme"), f"{ctx}.instances[{j}].endpoint.scheme")
            if scheme not in (_ENDPOINT_SCHEME_HTTP, _ENDPOINT_SCHEME_HTTPS):
                raise ValueError(f"invalid endpoint.scheme: {scheme}")
            _ = _require_int(ep_d.get("host_port"), f"{ctx}.instances[{j}].endpoint.host_port", min_v=1)
            if int(ep_d.get("host_port")) > 65535:
                raise ValueError("endpoint.host_port out of range")

    if endpoint_count == 0:
        raise ValueError(f"{ctx}.instances must include at least one endpoint instance")


def _validate_profile_ci_runtime_block(runtime: Dict[str, Any], ctx: str, target_ip_map: Dict[str, Any]) -> None:
    _forbid_unknown_keys(runtime, {RUNTIME_LAYER_BASE, RUNTIME_LAYER_CASE}, ctx)

    base_runtime = _require_dict(runtime.get(RUNTIME_LAYER_BASE), f"{ctx}.{RUNTIME_LAYER_BASE}")
    _forbid_unknown_keys(base_runtime, set(CI_BASE_RUNTIME_SERVICE_IDS), f"{ctx}.{RUNTIME_LAYER_BASE}")
    if set(base_runtime.keys()) != set(CI_BASE_RUNTIME_SERVICE_IDS):
        raise ValueError(
            f"{ctx}.{RUNTIME_LAYER_BASE} must define exactly {list(CI_BASE_RUNTIME_SERVICE_IDS)}"
        )
    for service_id in CI_BASE_RUNTIME_SERVICE_IDS:
        svc_ctx = f"{ctx}.{RUNTIME_LAYER_BASE}[{service_id!r}]"
        svc = _require_dict(base_runtime.get(service_id), svc_ctx)
        _forbid_unknown_keys(svc, {"target", "endpoint"}, svc_ctx)
        target = _require_str(svc.get("target"), f"{svc_ctx}.target")
        if target not in target_ip_map:
            raise ValueError(f"{svc_ctx}.target not found in deploy.target_ip_map: {target}")
        endpoint = _require_dict(svc.get("endpoint"), f"{svc_ctx}.endpoint")
        _forbid_unknown_keys(endpoint, {"scheme", "host_port"}, f"{svc_ctx}.endpoint")
        scheme = _require_str(endpoint.get("scheme"), f"{svc_ctx}.endpoint.scheme")
        if scheme not in (_ENDPOINT_SCHEME_HTTP, _ENDPOINT_SCHEME_HTTPS):
            raise ValueError(f"{svc_ctx}.endpoint.scheme invalid: {scheme!r}")
        host_port = _require_int(endpoint.get("host_port"), f"{svc_ctx}.endpoint.host_port", min_v=1)
        if host_port > 65535:
            raise ValueError(f"{svc_ctx}.endpoint.host_port out of range")

    case_runtime = _require_dict(runtime.get(RUNTIME_LAYER_CASE), f"{ctx}.{RUNTIME_LAYER_CASE}")
    _forbid_unknown_keys(case_runtime, set(CI_CASE_RUNTIME_INSTANCE_IDS), f"{ctx}.{RUNTIME_LAYER_CASE}")
    if not case_runtime:
        raise ValueError(f"{ctx}.{RUNTIME_LAYER_CASE} must be non-empty")
    if "ci_runner" not in case_runtime:
        raise ValueError(f"{ctx}.{RUNTIME_LAYER_CASE} must define 'ci_runner'")
    if "owner_0" in case_runtime and "master" not in case_runtime:
        raise ValueError(f"{ctx}.{RUNTIME_LAYER_CASE} cannot define 'owner_0' without 'master'")
    for instance_id in (instance_id for instance_id in CI_CASE_RUNTIME_INSTANCE_IDS if instance_id in case_runtime):
        tpl_ctx = f"{ctx}.{RUNTIME_LAYER_CASE}[{instance_id!r}]"
        tpl = _require_dict(case_runtime.get(instance_id), tpl_ctx)
        _forbid_unknown_keys(tpl, {"k8s_ref", "lifecycle", "endpoint", "deployer"}, tpl_ctx)
        lifecycle = _require_str(tpl.get("lifecycle"), f"{tpl_ctx}.lifecycle")
        expected_lifecycle = "job" if instance_id == "ci_runner" else "service"
        if lifecycle != expected_lifecycle:
            raise ValueError(f"{tpl_ctx}.lifecycle must be {expected_lifecycle!r}")

        k8s_ref = _require_str(tpl.get("k8s_ref"), f"{tpl_ctx}.k8s_ref")
        _ops_kind_from_k8s_ref(k8s_ref, ctx=f"{tpl_ctx}.k8s_ref")

        deployer = _require_dict(tpl.get("deployer"), f"{tpl_ctx}.deployer")
        _forbid_unknown_keys(
            deployer,
            {"target", "payload_file", "payload_dest_path", "command", "args", "working_dir"},
            f"{tpl_ctx}.deployer",
        )
        target = _require_str(deployer.get("target"), f"{tpl_ctx}.deployer.target")
        if instance_id in ("owner_0", "ci_runner"):
            if target != "__TARGET__":
                raise ValueError(f"{tpl_ctx}.deployer.target must be '__TARGET__'")
        elif target not in target_ip_map:
            raise ValueError(f"{tpl_ctx}.deployer.target not found in deploy.target_ip_map: {target}")

        cmd_list = _require_list(deployer.get("command"), f"{tpl_ctx}.deployer.command")
        if not cmd_list:
            raise ValueError(f"{tpl_ctx}.deployer.command must be non-empty")
        for idx, raw in enumerate(cmd_list):
            _ = _require_str(raw, f"{tpl_ctx}.deployer.command[{idx}]")

        args_list = deployer.get("args")
        if args_list is not None:
            args = _require_list(args_list, f"{tpl_ctx}.deployer.args")
            for idx, raw in enumerate(args):
                _ = _require_str(raw, f"{tpl_ctx}.deployer.args[{idx}]")

        working_dir = deployer.get("working_dir")
        if working_dir is not None:
            _ = _require_str(working_dir, f"{tpl_ctx}.deployer.working_dir")

        if tpl.get("endpoint") is not None:
            raise ValueError(f"{tpl_ctx}.endpoint must be omitted")


def _require_clean_relpath(raw: Any, ctx: str) -> str:
    relpath = _require_str(raw, ctx).strip()
    if not relpath:
        raise ValueError(f"{ctx} must be non-empty")
    rel_obj = Path(relpath)
    if rel_obj.is_absolute():
        raise ValueError(f"{ctx} must be relative, got absolute path: {relpath!r}")
    normalized = rel_obj.as_posix()
    if normalized.startswith("./") or normalized.startswith("../"):
        raise ValueError(f"{ctx} must not start with '.' or '..': {relpath!r}")
    if any(part in ("", ".", "..") for part in normalized.split("/")):
        raise ValueError(f"{ctx} must not contain empty / '.' / '..' path segments: {relpath!r}")
    return normalized


def _validate_profile_test_stack_monitoring_config(raw: Any, ctx: str) -> None:
    if raw is None:
        return
    monitoring = _require_dict(raw, ctx)
    _forbid_unknown_keys(
        monitoring,
        {"prometheus_base_url", "prom_remote_write_url", "otlp_log_api"},
        ctx,
    )
    prom_base = monitoring.get("prometheus_base_url")
    if prom_base is not None:
        _ = _require_str(prom_base, f"{ctx}.prometheus_base_url")
    prom_remote_write = monitoring.get("prom_remote_write_url")
    if prom_remote_write is not None:
        urls = _require_list(prom_remote_write, f"{ctx}.prom_remote_write_url")
        for index, raw_url in enumerate(urls):
            _ = _require_str(raw_url, f"{ctx}.prom_remote_write_url[{index}]")
    raw_otlp_log_api = monitoring.get("otlp_log_api")
    if raw_otlp_log_api is None:
        return
    otlp_log_api = _require_dict(raw_otlp_log_api, f"{ctx}.otlp_log_api")
    _forbid_unknown_keys(
        otlp_log_api,
        {"otlp_endpoint", "db_name", "table_name"},
        f"{ctx}.otlp_log_api",
    )
    _ = _require_str(otlp_log_api.get("otlp_endpoint"), f"{ctx}.otlp_log_api.otlp_endpoint")
    _ = _require_str(otlp_log_api.get("db_name"), f"{ctx}.otlp_log_api.db_name")
    table_name = otlp_log_api.get("table_name")
    if table_name is not None:
        _ = _require_str(table_name, f"{ctx}.otlp_log_api.table_name")


def _validate_profile_test_stack_fluxon_runtime_config(rc: Dict[str, Any], ctx: str) -> None:
    _forbid_unknown_keys(
        rc,
        {"kv_base", "mq_base", "kv_node_patch_template", "runtime_env", "monitoring", "owner_cpu_core_by_target"},
        ctx,
    )
    kv_base = _require_dict(rc.get("kv_base"), f"{ctx}.kv_base")
    _forbid_removed_fluxon_kv_config_keys(kv_base, f"{ctx}.kv_base")
    if rc.get("mq_base") is not None:
        _ = _require_dict(rc.get("mq_base"), f"{ctx}.mq_base")
    tpl = _require_dict(rc.get("kv_node_patch_template"), f"{ctx}.kv_node_patch_template")
    if "instance_key" in tpl:
        raise ValueError(f"{ctx}.kv_node_patch_template must not contain instance_key")
    _ = _normalize_runtime_env_map(rc.get("runtime_env"), f"{ctx}.runtime_env")
    _validate_profile_test_stack_monitoring_config(rc.get("monitoring"), f"{ctx}.monitoring")
    _ = _normalize_owner_cpu_core_by_target(
        rc.get("owner_cpu_core_by_target"),
        f"{ctx}.owner_cpu_core_by_target",
    )


def _validate_profile_test_stack_redis_runtime_config(rc: Dict[str, Any], ctx: str) -> None:
    _forbid_unknown_keys(rc, {"redis"}, ctx)
    redis_cfg = _require_dict(rc.get("redis"), f"{ctx}.redis")
    _forbid_unknown_keys(
        redis_cfg,
        {
            "server_binary_test_rsc_relpath",
            "server_bundle_test_rsc_relpath",
            "server_args",
            "connect_timeout_seconds",
            "socket_timeout_seconds",
            "database",
            "password",
        },
        f"{ctx}.redis",
    )
    _require_clean_relpath(
        redis_cfg.get("server_binary_test_rsc_relpath"),
        f"{ctx}.redis.server_binary_test_rsc_relpath",
    )
    bundle_relpath = redis_cfg.get("server_bundle_test_rsc_relpath")
    if bundle_relpath is not None:
        _require_clean_relpath(
            bundle_relpath,
            f"{ctx}.redis.server_bundle_test_rsc_relpath",
        )
    server_args = redis_cfg.get("server_args")
    if server_args is not None:
        args = _require_list(server_args, f"{ctx}.redis.server_args")
        for index, raw_arg in enumerate(args):
            _ = _require_str(raw_arg, f"{ctx}.redis.server_args[{index}]")
    connect_timeout_seconds = redis_cfg.get("connect_timeout_seconds")
    if connect_timeout_seconds is not None:
        if not isinstance(connect_timeout_seconds, (int, float)) or float(connect_timeout_seconds) <= 0.0:
            raise ValueError(f"{ctx}.redis.connect_timeout_seconds must be > 0")
    socket_timeout_seconds = redis_cfg.get("socket_timeout_seconds")
    if socket_timeout_seconds is not None:
        if not isinstance(socket_timeout_seconds, (int, float)) or float(socket_timeout_seconds) <= 0.0:
            raise ValueError(f"{ctx}.redis.socket_timeout_seconds must be > 0")
    database = redis_cfg.get("database")
    if database is not None:
        _ = _require_int(database, f"{ctx}.redis.database", min_v=0)
    password = redis_cfg.get("password")
    if password is not None:
        _ = _require_str(password, f"{ctx}.redis.password")


def _validate_profile_test_stack_mooncake_runtime_config(rc: Dict[str, Any], ctx: str) -> None:
    _forbid_unknown_keys(rc, {"kv_base", "master"}, ctx)
    kv_base = _require_dict(rc.get("kv_base"), f"{ctx}.kv_base")
    if "rdma_device_names" in kv_base:
        raise ValueError(f"{ctx}.kv_base.rdma_device_names has been removed from Fluxon KV config")
    if "fluxonkv_spec" in kv_base:
        raise ValueError(
            f"{ctx}.kv_base.fluxonkv_spec is invalid for TEST_STACK Mooncake baseline; use mooncake_spec"
        )
    mooncake_spec = _require_dict(kv_base.get("mooncake_spec"), f"{ctx}.kv_base.mooncake_spec")
    _forbid_unknown_keys(
        mooncake_spec,
        {
            "local_buffer_size",
            "metadata_server",
            "master_server_address",
            "etcd_addresses",
        },
        f"{ctx}.kv_base.mooncake_spec",
    )
    _require_int(
        mooncake_spec.get("local_buffer_size"),
        f"{ctx}.kv_base.mooncake_spec.local_buffer_size",
        min_v=1,
    )
    if "metadata_server" in mooncake_spec:
        raise ValueError(
            f"{ctx}.kv_base.mooncake_spec.metadata_server is generated by TEST_STACK Mooncake master; do not set it"
        )
    if "master_server_address" in mooncake_spec:
        raise ValueError(
            f"{ctx}.kv_base.mooncake_spec.master_server_address is generated by TEST_STACK Mooncake master; do not set it"
        )
    raw_etcd_addresses = mooncake_spec.get("etcd_addresses")
    if raw_etcd_addresses is not None:
        etcd_addresses = _require_list(
            raw_etcd_addresses,
            f"{ctx}.kv_base.mooncake_spec.etcd_addresses",
        )
        for index, raw_addr in enumerate(etcd_addresses):
            _ = _require_str(
                raw_addr,
                f"{ctx}.kv_base.mooncake_spec.etcd_addresses[{index}]",
            )
    _ = _normalize_test_stack_zero_contribution_pool(
        kv_base.get("contribute_to_cluster_pool_size"),
        f"{ctx}.kv_base.contribute_to_cluster_pool_size",
    )
    _ = _normalize_test_spec_config(
        kv_base.get("test_spec_config"),
        f"{ctx}.kv_base.test_spec_config",
    )
    _ = _normalize_test_spec_config(
        kv_base.get("benchmark_fast_path"),
        f"{ctx}.kv_base.benchmark_fast_path",
    )
    pprof_duration_seconds = kv_base.get("pprof_duration_seconds")
    if pprof_duration_seconds is not None:
        _ = _require_int(
            pprof_duration_seconds,
            f"{ctx}.kv_base.pprof_duration_seconds",
            min_v=1,
        )
    _ = _normalize_test_stack_perf_config(
        kv_base.get("perf"),
        f"{ctx}.kv_base.perf",
    )
    master_cfg = _require_dict(rc.get("master"), f"{ctx}.master")
    _forbid_unknown_keys(
        master_cfg,
        {
            "wheel_name",
            "python_abi",
            "cluster_id_prefix",
            "rpc_thread_num",
            "rpc_address",
            "http_metadata_host",
            "extra_args",
        },
        f"{ctx}.master",
    )
    wheel_name = _require_str(master_cfg.get("wheel_name"), f"{ctx}.master.wheel_name").strip()
    if not wheel_name:
        raise ValueError(f"{ctx}.master.wheel_name must be non-empty")
    python_abi = _require_str(master_cfg.get("python_abi"), f"{ctx}.master.python_abi").strip()
    if not re.fullmatch(r"cpython[0-9]+\.[0-9]+", python_abi):
        raise ValueError(f"{ctx}.master.python_abi format invalid: {python_abi!r}")
    cluster_id_prefix = _require_str(master_cfg.get("cluster_id_prefix"), f"{ctx}.master.cluster_id_prefix").strip()
    if not cluster_id_prefix:
        raise ValueError(f"{ctx}.master.cluster_id_prefix must be non-empty")
    rpc_thread_num = master_cfg.get("rpc_thread_num")
    if rpc_thread_num is not None:
        _ = _require_int(rpc_thread_num, f"{ctx}.master.rpc_thread_num", min_v=1)
    rpc_address = master_cfg.get("rpc_address")
    if rpc_address is not None:
        rpc_address_text = _require_str(rpc_address, f"{ctx}.master.rpc_address").strip()
        if not rpc_address_text:
            raise ValueError(f"{ctx}.master.rpc_address must be non-empty when set")
    http_metadata_host = master_cfg.get("http_metadata_host")
    if http_metadata_host is not None:
        host_text = _require_str(http_metadata_host, f"{ctx}.master.http_metadata_host").strip()
        if not host_text:
            raise ValueError(f"{ctx}.master.http_metadata_host must be non-empty when set")
    extra_args = master_cfg.get("extra_args")
    if extra_args is not None:
        args = _require_list(extra_args, f"{ctx}.master.extra_args")
        for index, raw_arg in enumerate(args):
            arg = _require_str(raw_arg, f"{ctx}.master.extra_args[{index}]").strip()
            if not arg:
                raise ValueError(f"{ctx}.master.extra_args[{index}] must be non-empty")


def _validate_profile_test_stack_alluxio_runtime_config(
    rc: Dict[str, Any],
    ctx: str,
    *,
    target_ip_map: Dict[str, Any],
) -> None:
    _forbid_unknown_keys(rc, {"alluxio"}, ctx)
    alluxio_cfg = _require_dict(rc.get("alluxio"), f"{ctx}.alluxio")
    _forbid_unknown_keys(
        alluxio_cfg,
        {
            "bundle_test_rsc_relpath",
            "bundle_root_relpath",
            "mount_root_by_target",
            "namespace_prefix",
            "mount_command_template",
        },
        f"{ctx}.alluxio",
    )
    _ = _require_clean_relpath(
        alluxio_cfg.get("bundle_test_rsc_relpath"),
        f"{ctx}.alluxio.bundle_test_rsc_relpath",
    )
    _ = _require_clean_relpath(
        alluxio_cfg.get("bundle_root_relpath"),
        f"{ctx}.alluxio.bundle_root_relpath",
    )
    mount_root_by_target = _require_dict(alluxio_cfg.get("mount_root_by_target"), f"{ctx}.alluxio.mount_root_by_target")
    if not mount_root_by_target:
        raise ValueError(f"{ctx}.alluxio.mount_root_by_target must be non-empty")
    for raw_target, raw_mount_root in mount_root_by_target.items():
        target = _require_str(raw_target, f"{ctx}.alluxio.mount_root_by_target key")
        if target not in target_ip_map:
            raise ValueError(
                f"{ctx}.alluxio.mount_root_by_target key not found in deploy.target_ip_map: {target!r}"
            )
        mount_root = _require_str(raw_mount_root, f"{ctx}.alluxio.mount_root_by_target[{target!r}]").strip()
        if not mount_root or not Path(mount_root).is_absolute():
            raise ValueError(
                f"{ctx}.alluxio.mount_root_by_target[{target!r}] must be an absolute path"
            )
    namespace_prefix = alluxio_cfg.get("namespace_prefix")
    if namespace_prefix is not None:
        _ = _require_clean_relpath(namespace_prefix, f"{ctx}.alluxio.namespace_prefix")
    mount_command_template = _require_str(
        alluxio_cfg.get("mount_command_template"),
        f"{ctx}.alluxio.mount_command_template",
    )
    if "__MOUNT_ROOT__" not in mount_command_template or "__ALLUXIO_BUNDLE_ROOT__" not in mount_command_template:
        raise ValueError(
            f"{ctx}.alluxio.mount_command_template must include '__MOUNT_ROOT__' and '__ALLUXIO_BUNDLE_ROOT__' tokens"
        )


def _validate_profile_test_stack_block(ts: Dict[str, Any], ctx: str, target_ip_map: Dict[str, Any]) -> None:
    _forbid_unknown_keys(
        ts,
        {
            "kind",
            "deploy",
            "runtime_config",
            "deploy_templates",
            "coordinator_ready_timeout_seconds",
            "port_alloc",
            "runtime_env",
        },
        ctx,
    )
    backend_kind = _require_test_stack_backend_kind(ts.get("kind"), f"{ctx}.kind")
    _ = _require_int(
        ts.get("coordinator_ready_timeout_seconds"),
        f"{ctx}.coordinator_ready_timeout_seconds",
        min_v=1,
    )
    port_alloc = _require_dict(ts.get("port_alloc"), f"{ctx}.port_alloc")
    _forbid_unknown_keys(port_alloc, {"by_topology"}, f"{ctx}.port_alloc")
    by_topology = _require_dict(port_alloc.get("by_topology"), f"{ctx}.port_alloc.by_topology")
    if not by_topology:
        raise ValueError(f"{ctx}.port_alloc.by_topology must be non-empty")
    for topology_key, raw_entry in by_topology.items():
        _parse_test_stack_port_alloc_topology_key(
            topology_key,
            field_name=f"{ctx}.port_alloc.by_topology key",
        )
        _validate_test_stack_port_alloc_entry(
            _require_dict(raw_entry, f"{ctx}.port_alloc.by_topology[{topology_key!r}]"),
            f"{ctx}.port_alloc.by_topology[{topology_key!r}]",
            backend_kind=backend_kind,
        )

    rc = _require_dict(ts.get("runtime_config"), f"{ctx}.runtime_config")
    if backend_kind == TEST_STACK_BACKEND_FLUXON:
        _validate_profile_test_stack_fluxon_runtime_config(rc, f"{ctx}.runtime_config")
    elif backend_kind == TEST_STACK_BACKEND_REDIS:
        _validate_profile_test_stack_redis_runtime_config(rc, f"{ctx}.runtime_config")
    elif backend_kind == TEST_STACK_BACKEND_MOONCAKE:
        _validate_profile_test_stack_mooncake_runtime_config(rc, f"{ctx}.runtime_config")
    elif backend_kind == TEST_STACK_BACKEND_ALLUXIO:
        _validate_profile_test_stack_alluxio_runtime_config(
            rc,
            f"{ctx}.runtime_config",
            target_ip_map=target_ip_map,
        )
    else:
        raise ValueError(f"{ctx}.kind invalid: {backend_kind!r}")

    _ = _normalize_runtime_env_map(ts.get("runtime_env"), f"{ctx}.runtime_env")

    dt = _require_dict(ts.get("deploy_templates"), f"{ctx}.deploy_templates")
    _forbid_unknown_keys(dt, {"coordinator", "node"}, f"{ctx}.deploy_templates")
    coord_tpl = _require_dict(dt.get("coordinator"), f"{ctx}.deploy_templates.coordinator")
    node_tpl = _require_dict(dt.get("node"), f"{ctx}.deploy_templates.node")

    k8s_ref = _require_str(node_tpl.get("k8s_ref"), f"{ctx}.deploy_templates.node.k8s_ref")
    if "__INSTANCE_KEY__" not in k8s_ref:
        raise ValueError(f"{ctx}.deploy_templates.node.k8s_ref must include '__INSTANCE_KEY__' token")
    node_dep = _require_dict(node_tpl.get("deployer"), f"{ctx}.deploy_templates.node.deployer")
    raw_node_target = node_dep.get("target")
    if raw_node_target is not None:
        tgt = _require_str(raw_node_target, f"{ctx}.deploy_templates.node.deployer.target")
        if tgt != "__TARGET__":
            raise ValueError(f"{ctx}.deploy_templates.node.deployer.target must be '__TARGET__'")
    args = _require_list(node_dep.get("args"), f"{ctx}.deploy_templates.node.deployer.args")
    joined = " ".join(_require_str(x, f"{ctx}.deploy_templates.node.deployer.args[]") for x in args)
    if "__INSTANCE_KEY__" not in joined or "__COORDINATOR__" not in joined:
        raise ValueError(
            f"{ctx}.deploy_templates.node.deployer.args must include '__INSTANCE_KEY__' and '__COORDINATOR__' tokens"
        )

    coord_dep = _require_dict(coord_tpl.get("deployer"), f"{ctx}.deploy_templates.coordinator.deployer")
    raw_coord_target = coord_dep.get("target")
    if raw_coord_target is not None:
        coord_target = _require_str(raw_coord_target, f"{ctx}.deploy_templates.coordinator.deployer.target")
        if coord_target != "__TARGET__" and coord_target not in target_ip_map:
            raise ValueError(
                f"{ctx}.deploy_templates.coordinator.deployer.target must be '__TARGET__' or a key in deploy.target_ip_map: "
                f"{coord_target}"
            )


def _parse_artifact_set(item: Dict[str, Any], ctx: str) -> Dict[str, Any]:
    _forbid_unknown_keys(item, {"release_source", "release_artifacts", "test_rsc_source", "test_rsc_artifacts"}, ctx)
    release_source = _require_dict(item.get("release_source"), f"{ctx}.release_source")
    _parse_artifact_source(release_source, f"{ctx}.release_source")
    release_artifacts = _require_dict(item.get("release_artifacts"), f"{ctx}.release_artifacts")
    _forbid_unknown_keys(
        release_artifacts,
        {"wheel"},
        f"{ctx}.release_artifacts",
    )
    for field_name in ("wheel",):
        _ = _require_str(release_artifacts.get(field_name), f"{ctx}.release_artifacts.{field_name}")
    test_rsc_source = _require_dict(item.get("test_rsc_source"), f"{ctx}.test_rsc_source")
    _parse_artifact_source(test_rsc_source, f"{ctx}.test_rsc_source")
    test_rsc_artifacts = _require_dict(item.get("test_rsc_artifacts"), f"{ctx}.test_rsc_artifacts")
    _forbid_unknown_keys(
        test_rsc_artifacts,
        {"ci_src_archive", "ci_ext_rsc_archive"},
        f"{ctx}.test_rsc_artifacts",
    )
    for field_name in ("ci_src_archive", "ci_ext_rsc_archive"):
        _ = _require_str(test_rsc_artifacts.get(field_name), f"{ctx}.test_rsc_artifacts.{field_name}")
    return item


def _parse_artifact_source(source: Dict[str, Any], ctx: str) -> None:
    kind = _require_str(source.get("kind"), f"{ctx}.kind")
    if kind == ARTIFACT_SOURCE_KIND_FLUXON_OPS_FS_S3:
        _forbid_unknown_keys(
            source,
            {"kind", "bucket", "access_key", "secret_key", "region", "key_prefix", "local_cache_root"},
            ctx,
        )
        _ = _require_str(source.get("bucket"), f"{ctx}.bucket")
        _ = _require_str(source.get("access_key"), f"{ctx}.access_key")
        _ = _require_str(source.get("secret_key"), f"{ctx}.secret_key")
        _ = _require_str(source.get("region"), f"{ctx}.region")
        key_prefix = _require_str(source.get("key_prefix"), f"{ctx}.key_prefix")
        if key_prefix.startswith("/") or key_prefix.endswith("/"):
            raise ValueError(f"{ctx}.key_prefix must not start or end with '/'")
        if "\\" in key_prefix:
            raise ValueError(f"{ctx}.key_prefix must not contain backslashes")
        if any(part in ("", ".", "..") for part in key_prefix.split("/")):
            raise ValueError(f"{ctx}.key_prefix must not contain empty / '.' / '..' segments")
        _ = _artifact_source_local_cache_root_opt(source, ctx=ctx)
        return
    raise ValueError(
        f"{ctx}.kind must be {ARTIFACT_SOURCE_KIND_FLUXON_OPS_FS_S3!r} (LOCAL_DIR has been removed), got: {kind!r}"
    )


def _parse_profile(item: Dict[str, Any], ctx: str) -> Dict[str, Any]:
    _forbid_unknown_keys(item, {"artifact_set", "runtime"}, ctx)
    _ = _require_str(item.get("artifact_set"), f"{ctx}.artifact_set")
    runtime = _require_dict(item.get("runtime"), f"{ctx}.runtime")
    _forbid_unknown_keys(runtime, {"ci", "test_stack"}, f"{ctx}.runtime")
    if runtime.get("ci") is None and runtime.get("test_stack") is None:
        raise ValueError(f"{ctx}.runtime must contain at least one of ci/test_stack blocks")

    if runtime.get("ci") is not None:
        ci = _require_dict(runtime.get("ci"), f"{ctx}.runtime.ci")
        _forbid_unknown_keys(ci, {"deploy", "runtime_contracts", "command_tokens"}, f"{ctx}.runtime.ci")
        deploy = _require_dict(ci.get("deploy"), f"{ctx}.runtime.ci.deploy")
        _validate_profile_deploy_block(
            deploy,
            f"{ctx}.runtime.ci.deploy",
            allow_instances=False,
            allow_target_tokens=False,
        )
        target_ip_map = _require_dict(deploy.get("target_ip_map"), f"{ctx}.runtime.ci.deploy.target_ip_map")
        runtime_contracts = _require_dict(ci.get("runtime_contracts"), f"{ctx}.runtime.ci.runtime_contracts")
        if not runtime_contracts:
            raise ValueError(f"{ctx}.runtime.ci.runtime_contracts must be non-empty")
        for contract_id, raw_runtime in runtime_contracts.items():
            _ = _require_ci_runtime_contract(contract_id, f"{ctx}.runtime.ci.runtime_contracts key")
            _validate_profile_ci_runtime_block(
                _require_dict(raw_runtime, f"{ctx}.runtime.ci.runtime_contracts[{contract_id!r}]"),
                f"{ctx}.runtime.ci.runtime_contracts[{contract_id!r}]",
                target_ip_map,
            )
        command_tokens = ci.get("command_tokens")
        if command_tokens is not None:
            command_tokens = _require_dict(command_tokens, f"{ctx}.runtime.ci.command_tokens")
            for raw_token_name, raw_token_value in command_tokens.items():
                token_name = _require_str(raw_token_name, f"{ctx}.runtime.ci.command_tokens key").strip()
                if re.fullmatch(r"[A-Z0-9_]+", token_name) is None:
                    raise ValueError(
                        f"{ctx}.runtime.ci.command_tokens keys must match [A-Z0-9_]+, got: {token_name!r}"
                    )
                _ = _require_str(raw_token_value, f"{ctx}.runtime.ci.command_tokens[{token_name!r}]")

    if runtime.get("test_stack") is not None:
        ts = _require_dict(runtime.get("test_stack"), f"{ctx}.runtime.test_stack")
        _forbid_unknown_keys(
            ts,
            {
                "kind",
                "deploy",
                "runtime_config",
                "deploy_templates",
                "coordinator_ready_timeout_seconds",
                "port_alloc",
                "runtime_env",
            },
            f"{ctx}.runtime.test_stack",
        )
        deploy = _require_dict(ts.get("deploy"), f"{ctx}.runtime.test_stack.deploy")
        _validate_profile_deploy_block(
            deploy,
            f"{ctx}.runtime.test_stack.deploy",
            allow_instances=False,
            allow_target_tokens=False,
        )
        target_ip_map = _require_dict(deploy.get("target_ip_map"), f"{ctx}.runtime.test_stack.deploy.target_ip_map")
        _validate_profile_test_stack_block(ts, f"{ctx}.runtime.test_stack", target_ip_map)

    return item


def _build_test_stack_role_plan(
    scene_ts: Dict[str, Any],
    scale: Dict[str, Any],
    *,
    ctx: str,
    target_ip_map: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    mode = _require_str(scene_ts.get("mode"), f"{ctx}.mode")
    roles_order = _test_stack_roles_by_mode(mode)
    machine_targets = _test_stack_scale_machine_targets(scale, ctx=ctx, target_ip_map=target_ip_map)
    machine_count = len(machine_targets)
    role_weights = scene_ts.get("role_weights")
    if mode != TEST_STACK_MODE_MPMC and role_weights is not None:
        raise ValueError(f"{ctx}.role_weights is only allowed for mode={TEST_STACK_MODE_MPMC}")
    out: Dict[str, Any] = {}
    if machine_count == 1:
        for role in roles_order:
            rec: Dict[str, Any] = {"count": 1, "targets": [machine_targets[0]]}
            if mode == TEST_STACK_MODE_MPMC:
                rw = _require_dict(role_weights, f"{ctx}.role_weights")
                weight = rw.get(role)
                if not isinstance(weight, (int, float)) or float(weight) <= 0.0:
                    raise ValueError(f"{ctx}.role_weights.{role} must be > 0")
                rec["weight"] = float(weight)
            out[role] = rec
        return out

    if mode == TEST_STACK_MODE_MPMC:
        rw = _require_dict(role_weights, f"{ctx}.role_weights")
        if machine_count < len(roles_order):
            raise ValueError(
                f"{ctx}.topology machine_count={machine_count} is too small for roles {roles_order}"
            )
        weights: Dict[str, float] = {}
        total_weight = 0.0
        for role in roles_order:
            weight = rw.get(role)
            if not isinstance(weight, (int, float)) or float(weight) <= 0.0:
                raise ValueError(f"{ctx}.role_weights.{role} must be > 0")
            w = float(weight)
            weights[role] = w
            total_weight += w
        counts = {role: 1 for role in roles_order}
        remaining = machine_count - len(roles_order)
        if remaining > 0:
            assigned = 0
            remainders: List[Tuple[float, int, str]] = []
            for index, role in enumerate(roles_order):
                share = float(remaining) * weights[role] / total_weight
                extra = int(share)
                counts[role] += extra
                assigned += extra
                remainders.append((share - float(extra), -index, role))
            leftover = remaining - assigned
            for _, _, role in sorted(remainders, reverse=True)[:leftover]:
                counts[role] += 1

        cursor = 0
        for role in roles_order:
            count = counts[role]
            role_targets = machine_targets[cursor : cursor + count]
            if len(role_targets) != count:
                raise ValueError(f"{ctx} role allocation overflow for role={role!r}")
            cursor += count
            out[role] = {"count": count, "targets": role_targets, "weight": weights[role]}
        return out

    if mode == TEST_STACK_MODE_KVSTORE:
        out[roles_order[0]] = {"count": machine_count, "targets": machine_targets}
        return out

    if len(roles_order) != 2:
        raise ValueError(f"{ctx} arbitrary-N placement currently requires exactly 2 non-MPMC roles, got {roles_order}")
    out[roles_order[0]] = {"count": 1, "targets": [machine_targets[0]]}
    out[roles_order[1]] = {"count": machine_count - 1, "targets": machine_targets[1:]}
    return out


def _expand_cases(suite: _Suite) -> List[_ResolvedCase]:
    out: List[_ResolvedCase] = []
    for scene_id in suite.scenes:
        scene = suite.scenes[scene_id]
        sel = _require_dict(scene.get("select"), f"scene[{scene_id}].select")
        scales = _require_id_list(sel.get("scales"), f"scene[{scene_id}].select.scales")
        profiles = _require_id_list(sel.get("profiles"), f"scene[{scene_id}].select.profiles")

        scene_kind = _scene_kind_from_item(scene, f"scene[{scene_id}]")

        for scale_id_raw in scales:
            scale_id = _require_str(scale_id_raw, "scale_id").strip()
            if scale_id not in suite.scales:
                raise ValueError(f"scene[{scene_id}] selects unknown scale enum: {scale_id}")
            scale = suite.scales[scale_id]

            if scene_kind == SCENE_KIND_CI:
                _ = _require_test_stack_machine_count(scale.get("topology"), f"scale[{scale_id}].topology")
            elif scene_kind == SCENE_KIND_TEST_STACK:
                scene_ts = _require_dict(scene.get("test_stack"), f"scene[{scene_id}].test_stack")
                _ = _build_test_stack_role_plan(scene_ts, scale, ctx=f"scene[{scene_id}].test_stack")
                if scale.get("benchmark") is None:
                    raise ValueError(f"scene[{scene_id}] kind={scene_kind} selects scale[{scale_id}] without benchmark block")
            else:
                raise ValueError(f"scene[{scene_id}] kind={scene_kind} is not supported by suite schema v{SUITE_SCHEMA_VERSION}")

            for profile_id_raw in profiles:
                profile_id = _require_str(profile_id_raw, "profile_id").strip()
                if profile_id not in suite.profiles:
                    raise ValueError(f"scene[{scene_id}] selects unknown profile enum: {profile_id}")
                profile = suite.profiles[profile_id]
                profile_runtime = _require_dict(profile.get("runtime"), f"profile[{profile_id}].runtime")

                if scene_kind == SCENE_KIND_CI:
                    if profile_runtime.get("ci") is None:
                        raise ValueError(f"scene[{scene_id}] kind={scene_kind} selects profile[{profile_id}] without ci block")
                    scene_ci = _require_dict(scene.get("ci"), f"scene[{scene_id}].ci")
                    runtime_contract = _require_ci_runtime_contract(
                        scene_ci.get("runtime_contract"),
                        f"scene[{scene_id}].ci.runtime_contract",
                    )
                    profile_ci = _require_dict(profile_runtime.get("ci"), f"profile[{profile_id}].runtime.ci")
                    runtime_contracts = _require_dict(
                        profile_ci.get("runtime_contracts"),
                        f"profile[{profile_id}].runtime.ci.runtime_contracts",
                    )
                    if runtime_contract not in runtime_contracts:
                        raise ValueError(
                            f"scene[{scene_id}] runtime_contract={runtime_contract!r} is missing from profile[{profile_id}].runtime.ci.runtime_contracts"
                        )
                elif scene_kind == SCENE_KIND_TEST_STACK:
                    profile_ts = _require_dict(profile_runtime.get("test_stack"), f"profile[{profile_id}].runtime.test_stack")
                    deploy = _require_dict(profile_ts.get("deploy"), f"profile[{profile_id}].runtime.test_stack.deploy")
                    target_ip_map = _require_dict(deploy.get("target_ip_map"), f"profile[{profile_id}].runtime.test_stack.deploy.target_ip_map")
                    scene_ts = _require_dict(scene.get("test_stack"), f"scene[{scene_id}].test_stack")
                    role_plan = _build_test_stack_role_plan(
                        scene_ts,
                        scale,
                        ctx=f"scene[{scene_id}].test_stack",
                        target_ip_map=target_ip_map,
                    )
                    for role, plan in role_plan.items():
                        p = _require_dict(plan, f"scene[{scene_id}].test_stack.role_plan[{role!r}]")
                        targets = _require_list(p.get("targets"), f"scene[{scene_id}].test_stack.role_plan[{role!r}].targets")
                        for t in targets:
                            if _require_str(t, "target").strip() not in target_ip_map:
                                raise ValueError(
                                    f"scene[{scene_id}] target not found in profile[{profile_id}].test_stack.deploy.target_ip_map: {t!r}"
                                )

                case_id = f"{scene_id}__{scale_id}__{profile_id}"

                identity_obj = {
                    "schema_version": SCHEMA_VERSION,
                    "scene": scene,
                    "scale": scale,
                    "profile": profile,
                }
                case_key = "sha256:" + hashlib.sha256(
                    json.dumps(
                        _json_canonicalize(identity_obj),
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=True,
                    ).encode("utf-8")
                ).hexdigest()

                out.append(
                    _ResolvedCase(
                        scene_id=scene_id,
                        scale_id=scale_id,
                        profile_id=profile_id,
                        case_id=case_id,
                        case_key=case_key,
                    )
                )

    return out


def _load_or_init_case_runs(path: Path) -> Dict[str, Any]:
    if not path.exists():
        print(f"INFO: case_runs.yaml not found; initializing: {path}")
        obj = {"schema_version": SCHEMA_VERSION, "cases": []}
        # Persist immediately so:
        # - users can tail progress from the very beginning
        # - an early crash still leaves a well-formed case_runs.yaml on disk
        _write_yaml_file(path, obj)
        return obj
    d = _load_yaml_file(path)
    d = _require_dict(d, "case_runs")
    _forbid_unknown_keys(d, {"schema_version", "cases"}, "case_runs")
    if d.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("case_runs.schema_version mismatch")
    cases = _require_list(d.get("cases"), "case_runs.cases")
    for i, item in enumerate(cases):
        rec = _require_dict(item, f"case_runs.cases[{i}]")
        _forbid_unknown_keys(
            rec,
            {
                "case_id",
                "case_key",
                "total_runs",
                "counted_runs",
                "success_runs",
                "failed_runs",
                "last_run",
            },
            f"case_runs.cases[{i}]",
        )
        total_runs = _require_int(rec.get("total_runs"), f"case_runs.cases[{i}].total_runs", min_v=0)
        success_runs = _require_int(rec.get("success_runs"), f"case_runs.cases[{i}].success_runs", min_v=0)
        failed_runs = _require_int(rec.get("failed_runs"), f"case_runs.cases[{i}].failed_runs", min_v=0)
        counted_runs = _require_int(rec.get("counted_runs"), f"case_runs.cases[{i}].counted_runs", min_v=0)
        # English note:
        # - `total_runs` is a finalized counter and must match success+failed.
        # - Older runner versions incremented total_runs during reservation; if the process
        #   crashed before finalize(), the invariant breaks and blocks future runs.
        # - Repair deterministically by recomputing total_runs from finalized counters.
        if total_runs != success_runs + failed_runs:
            total_runs = int(success_runs + failed_runs)
            rec["total_runs"] = total_runs
        if counted_runs > total_runs:
            raise ValueError("case_runs invariant failed: counted_runs")
        if counted_runs > success_runs:
            raise ValueError("case_runs invariant failed: counted_runs must not exceed success_runs")
        last_run = _require_dict(rec.get("last_run"), f"case_runs.cases[{i}].last_run")
        _forbid_unknown_keys(last_run, {"run_index", "outcome", "finished_at_unix_s"}, f"case_runs.cases[{i}].last_run")
        run_index = _require_int(last_run.get("run_index"), f"case_runs.cases[{i}].last_run.run_index", min_v=0)
        if run_index > 0:
            # Reserving a run_index persists last_run.run_index early; outcome/finished may be absent
            # when the runner crashed before reaching finalize().
            if last_run.get("outcome") is not None:
                _ = _require_str(last_run.get("outcome"), f"case_runs.cases[{i}].last_run.outcome")
                _ = _require_int(last_run.get("finished_at_unix_s"), f"case_runs.cases[{i}].last_run.finished_at_unix_s", min_v=1)

    # Deduplicate by case_id. case_key is metadata and can drift across harness refactors,
    # but case_id is the logical identity (scene_id__scale_id__profile_id).
    merged_by_id: Dict[str, Dict[str, Any]] = {}
    for item in cases:
        if not isinstance(item, dict):
            continue
        case_id = item.get("case_id")
        if not isinstance(case_id, str) or not case_id:
            continue
        prev = merged_by_id.get(case_id)
        if prev is None:
            merged_by_id[case_id] = item
            continue

        prev["success_runs"] = int(prev.get("success_runs", 0)) + int(item.get("success_runs", 0))
        prev["failed_runs"] = int(prev.get("failed_runs", 0)) + int(item.get("failed_runs", 0))
        prev["counted_runs"] = int(prev.get("counted_runs", 0)) + int(item.get("counted_runs", 0))
        prev["total_runs"] = int(prev.get("success_runs", 0)) + int(prev.get("failed_runs", 0))

        # Prefer a completed last_run (with outcome+finished) over a reserved-only last_run.
        def _last_run_score(lr: Any) -> tuple[int, int, int]:
            if not isinstance(lr, dict):
                return (0, 0, 0)
            run_index = int(lr.get("run_index", 0) or 0)
            finished = int(lr.get("finished_at_unix_s", 0) or 0)
            outcome = lr.get("outcome")
            completed = 1 if isinstance(outcome, str) and finished > 0 else 0
            return (completed, finished, run_index)

        prev_lr = prev.get("last_run")
        cur_lr = item.get("last_run")
        if _last_run_score(cur_lr) > _last_run_score(prev_lr):
            prev["last_run"] = cur_lr

        # Keep the most recent case_key if present.
        ck = item.get("case_key")
        if isinstance(ck, str) and ck:
            prev["case_key"] = ck

    d["cases"] = [merged_by_id[k] for k in sorted(merged_by_id.keys())]
    return d


def _ensure_case_runs_include_all_suite_cases(
    case_runs: Dict[str, Any],
    resolved_cases: List[_ResolvedCase],
) -> int:
    cases = _require_list(case_runs.get("cases"), "case_runs.cases")
    by_id: Dict[str, Dict[str, Any]] = {}
    for raw in cases:
        if not isinstance(raw, dict):
            continue
        cid = raw.get("case_id")
        if isinstance(cid, str) and cid:
            by_id[cid] = raw

    added = 0
    for case in resolved_cases:
        rec = by_id.get(case.case_id)
        if rec is None:
            cases.append(
                {
                    "case_id": case.case_id,
                    "case_key": case.case_key,
                    "total_runs": 0,
                    "counted_runs": 0,
                    "success_runs": 0,
                    "failed_runs": 0,
                    "last_run": {"run_index": 0},
                }
            )
            added += 1
            continue
        # Keep case_key convergent to the current suite snapshot.
        rec["case_key"] = case.case_key
    return added


def _select_cases(selectors: _RunSelectors, cases: List[_ResolvedCase]) -> List[_ResolvedCase]:
    selected: List[_ResolvedCase] = list(cases)
    allowed = set(selectors.profile_ids)
    selected = [case for case in selected if case.profile_id in allowed]
    if not selected:
        raise ValueError(
            "config.run.selectors.profile_ids selects no cases. "
            f"profile_ids={sorted(allowed)}"
        )

    if selectors.case_ids is None:
        return selected

    selected_by_id = {case.case_id: case for case in selected}
    out: List[_ResolvedCase] = []
    for case_id in selectors.case_ids:
        case = selected_by_id.get(case_id)
        if case is None:
            raise ValueError(
                "config.run.selectors.case_ids selects unknown case_id "
                f"(after applying profile_ids filter): {case_id}"
            )
        out.append(case)
    return out


def _command_step_label(command: Dict[str, Any]) -> str:
    command_id = _require_str(command.get("id"), "planned_command.id")
    raw_test_id = command.get("test_id")
    if raw_test_id is None:
        return command_id
    test_id = _require_str(raw_test_id, "planned_command.test_id")
    return f"{command_id}[{test_id}]"


def _materialize_selected_ci_steps(
    case: _ResolvedCase,
    command: Dict[str, Any],
    selectors: _RunSelectors,
) -> List[Dict[str, Any]]:
    command_id = _require_str(command.get("id"), f"scene[{case.scene_id}].ci.command.id")
    command_text = _require_str(command.get("command"), f"scene[{case.scene_id}].ci.command.command")
    timeout_seconds = command.get("timeout_seconds")
    if timeout_seconds is not None:
        timeout_seconds = _require_int(
            timeout_seconds,
            f"scene[{case.scene_id}].ci.command[{command_id}].timeout_seconds",
            min_v=1,
        )
    if selectors.test_ids is None:
        rec: Dict[str, Any] = {"id": command_id, "command": command_text}
        if timeout_seconds is not None:
            rec["timeout_seconds"] = int(timeout_seconds)
        return [rec]

    raw_test_ids = command.get("test_ids")
    if raw_test_ids is None:
        return []
    test_ids = _require_list(raw_test_ids, f"scene[{case.scene_id}].ci.command[{command_id}].test_ids")
    test_id_arg = _require_str(command.get("test_id_arg"), f"scene[{case.scene_id}].ci.command[{command_id}].test_id_arg")

    out: List[Dict[str, Any]] = []
    for i, raw_test_id in enumerate(test_ids):
        test_id = _require_str(raw_test_id, f"scene[{case.scene_id}].ci.command[{command_id}].test_ids[{i}]")
        if test_id not in selectors.test_ids:
            continue
        rec: Dict[str, Any] = {
            "id": command_id,
            "command": f"{command_text} {test_id_arg} {_shell_quote(test_id)}",
            "test_id": test_id,
        }
        if timeout_seconds is not None:
            rec["timeout_seconds"] = int(timeout_seconds)
        out.append(rec)
    return out


def _build_ci_execution_plan(case: _ResolvedCase, suite: _Suite) -> List[_PlannedCase]:
    selectors = suite.run_selectors
    scene_ci = _require_dict(suite.scenes[case.scene_id].get("ci"), f"scene[{case.scene_id}].ci")
    raw_commands = _require_list(scene_ci.get("commands"), f"scene[{case.scene_id}].ci.commands")
    commands = [_require_dict(raw, f"scene[{case.scene_id}].ci.commands[]") for raw in raw_commands]

    selected_commands: List[Dict[str, Any]] = []
    if selectors.command_ids is None:
        selected_commands = commands
    else:
        commands_by_id = {
            _require_str(raw.get("id"), f"scene[{case.scene_id}].ci.commands[].id"): raw
            for raw in commands
        }
        for command_id in selectors.command_ids:
            command = commands_by_id.get(command_id)
            if command is not None:
                selected_commands.append(command)

    if selectors.command_ids is not None and not selected_commands:
        return []

    if selectors.test_ids is not None and selectors.command_ids is not None and len(selected_commands) != 1:
        raise ValueError(f"config.run.selectors.test_ids requires exactly one selected CI command for case {case.case_id}")

    if selectors.test_ids is not None and selectors.command_ids is not None:
        selected_command = selected_commands[0]
        if selected_command.get("test_ids") is None:
            command_id = _require_str(selected_command.get("id"), "ci.command.id")
            raise ValueError(f"selected command does not support test_ids: case={case.case_id} command_id={command_id}")

    if suite.run_mode == RUN_MODE_DEBUG_ONE_BY_ONE:
        planned: List[_PlannedCase] = []
        for command in selected_commands:
            steps = _materialize_selected_ci_steps(case, command, selectors)
            for step in steps:
                planned.append(_PlannedCase(case=case, ci_commands=[step], label=f"{case.case_id}::{_command_step_label(step)}", command_id=step["id"], test_id=step.get("test_id"), counted=False))
        return planned

    grouped_steps: List[Dict[str, str]] = []
    for command in selected_commands:
        grouped_steps.extend(_materialize_selected_ci_steps(case, command, selectors))
    if not grouped_steps:
        return []
    return [_PlannedCase(case=case, ci_commands=grouped_steps, label=case.case_id if selectors.command_ids is None and selectors.test_ids is None else f"{case.case_id}::selected", command_id=None, test_id=None, counted=selectors.command_ids is None and selectors.test_ids is None)]


def _build_execution_plan(suite: _Suite, cases: List[_ResolvedCase]) -> List[_PlannedCase]:
    selected_cases = _select_cases(suite.run_selectors, cases)
    planned: List[_PlannedCase] = []
    matched_command_ids: set[str] = set()
    matched_test_ids: set[str] = set()

    for case in selected_cases:
        case_family = _case_family_from_scene_item(
            suite.scenes[case.scene_id],
            f"scene[{case.scene_id}]",
        )
        if case_family == CASE_FAMILY_CI:
            case_plans = _build_ci_execution_plan(case, suite)
            for planned_case in case_plans:
                if planned_case.command_id is not None:
                    matched_command_ids.add(planned_case.command_id)
                if planned_case.test_id is not None:
                    matched_test_ids.add(planned_case.test_id)
                if planned_case.ci_commands is not None:
                    for step in planned_case.ci_commands:
                        matched_command_ids.add(_require_str(step.get("id"), "planned_case.ci_commands[].id"))
                        raw_test_id = step.get("test_id")
                        if raw_test_id is not None:
                            matched_test_ids.add(_require_str(raw_test_id, "planned_case.ci_commands[].test_id"))
            planned.extend(case_plans)
            continue

        if suite.run_selectors.command_ids is not None or suite.run_selectors.test_ids is not None:
            continue
        planned.append(_PlannedCase(case=case, ci_commands=None, label=case.case_id, command_id=None, test_id=None, counted=suite.run_mode == RUN_MODE_FULL_ONCE))

    if suite.run_selectors.command_ids is not None:
        missing_command_ids = [command_id for command_id in suite.run_selectors.command_ids if command_id not in matched_command_ids]
        if missing_command_ids:
            raise ValueError(f"config.run.selectors.command_ids contains ids not selected by the current cases: {missing_command_ids}")
    if suite.run_selectors.test_ids is not None:
        missing_test_ids = [test_id for test_id in suite.run_selectors.test_ids if test_id not in matched_test_ids]
        if missing_test_ids:
            raise ValueError(f"config.run.selectors.test_ids contains ids not selected by the current cases/commands: {missing_test_ids}")

    return planned


def _suite_requires_benchmark_bundle(*, suite: _Suite, resolved_cases: List[_ResolvedCase]) -> bool:
    for case in resolved_cases:
        scene_item = _require_dict(
            suite.scenes.get(case.scene_id),
            f"suite.scenes[{case.scene_id!r}]",
        )
        if _case_family_from_scene_item(scene_item, f"suite.scenes[{case.scene_id!r}]") == CASE_FAMILY_BENCH:
            return True
    return False


def _case_runs_map(case_runs: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for raw in case_runs.get("cases", []):
        if isinstance(raw, dict) and isinstance(raw.get("case_id"), str):
            # case_id is the primary key. case_key is metadata and may change across
            # harness refactors (e.g. config structure edits) without needing a full rerun.
            out[raw["case_id"]] = raw
    return out


def _reserve_run_slot(case_runs: Dict[str, Any], case: _ResolvedCase, *, results_root: Path) -> _RunSlot:
    run_map = _case_runs_map(case_runs)
    rec = run_map.get(case.case_id)
    if rec is None:
        rec = {
            "case_id": case.case_id,
            "case_key": case.case_key,
            "total_runs": 0,
            "counted_runs": 0,
            "success_runs": 0,
            "failed_runs": 0,
            "last_run": {"run_index": 0},
        }
        case_runs["cases"].append(rec)
    else:
        # Keep case_key in sync with the current suite config.
        rec["case_key"] = case.case_key

    # Keep run directories unique per case_id even if:
    # - the case_key changes (e.g. config edits)
    # - a previous run created run_N on disk but did not reach case_runs.yaml update (crash/kill)
    max_idx = 0
    for raw in case_runs.get("cases", []):
        if not isinstance(raw, dict):
            continue
        if raw.get("case_id") != case.case_id:
            continue
        lr = raw.get("last_run")
        if not isinstance(lr, dict):
            continue
        try:
            max_idx = max(max_idx, int(lr.get("run_index", 0)))
        except Exception:
            continue

    case_dir = results_root / case.case_id
    if case_dir.exists():
        for p in case_dir.iterdir():
            if not p.is_dir():
                continue
            name = p.name
            if not name.startswith("run_"):
                continue
            suffix = name[4:]
            if not suffix.isdigit():
                continue
            max_idx = max(max_idx, int(suffix))

    run_index = max_idx + 1
    # Reserve by updating last_run with run_index=reserved.
    rec["last_run"] = {"run_index": run_index}
    return _RunSlot(case_key=case.case_key, case_id=case.case_id, run_index=run_index, rec=rec)


def _resume_reserved_runs(
    *,
    planned: List[_PlannedCase],
    case_runs: Dict[str, Any],
    case_runs_path: Path,
    results_root: Path,
) -> None:
    planned_by_id = {p.case.case_id: p for p in planned}
    run_map = _case_runs_map(case_runs)

    for case_id, planned_case in planned_by_id.items():
        rec = run_map.get(case_id)
        if rec is None:
            continue
        last_run = rec.get("last_run")
        if not isinstance(last_run, dict):
            continue
        if last_run.get("run_index") in (None, 0):
            continue
        if last_run.get("outcome") is not None:
            continue

        run_index = _require_int(last_run.get("run_index"), f"case_runs[{case_id}].last_run.run_index", min_v=1)
        run_dir = results_root / case_id / f"run_{run_index}"
        if not run_dir.exists():
            # English note:
            # - We persist last_run.run_index early for observability.
            # - The runner creates run_dir shortly after. If the process exits between these steps,
            #   we can end up with a reserved last_run but no run_dir on disk.
            # - This is not a recoverable run and must not block future reruns. Repair by clearing
            #   the reservation deterministically (run_index=0). The next reservation will re-scan
            #   existing run_* dirs to keep run indices unique.
            rec["last_run"] = {"run_index": 0}
            _write_yaml_file(case_runs_path, case_runs)
            print(
                "[RESUME reserved last_run] "
                f"case_id={case_id} run_index={run_index} repaired=cleared_reservation reason=missing_run_dir",
                flush=True,
            )
            continue

        resolved_case_path = run_dir / "resolved_case.yaml"
        if not resolved_case_path.exists():
            # English note:
            # - We reserve run_index early for observability.
            # - If the runner exits between reservation and writing resolved_case.yaml,
            #   this reserved run becomes unrecoverable and must not block future reruns.
            finished_at = int(time.time())
            summary_path = run_dir / "summary.yaml"
            if not summary_path.exists():
                _write_yaml_file(
                    summary_path,
                    {
                        "schema_version": SCHEMA_VERSION,
                        "case_id": case_id,
                        "run_index": int(run_index),
                        "outcome": RUN_OUTCOME_FAILED,
                        "counted": False,
                        "timing": {"finished_at_unix_s": int(finished_at)},
                        "error": (
                            "reserved run is missing resolved_case.yaml; previous runner likely exited "
                            "before completing run_dir materialization"
                        ),
                    },
                )
            slot = _RunSlot(
                case_key=_require_str(rec.get("case_key"), f"case_runs[{case_id}].case_key"),
                case_id=case_id,
                run_index=run_index,
                rec=rec,
            )
            _finalize_run_slot(
                case_runs,
                slot,
                outcome=RUN_OUTCOME_FAILED,
                counted=False,
                finished_at_unix_s=finished_at,
            )
            _write_yaml_file(case_runs_path, case_runs)
            print(
                "[RESUME reserved last_run] "
                f"case_id={case_id} run_index={run_index} outcome=FAILED reason=missing_resolved_case_yaml",
                flush=True,
            )
            continue

        resolved_case = _load_run_dir_resolved_case(run_dir)
        case_family = _resolved_case_family(resolved_case)
        if case_family != CASE_FAMILY_CI:
            # English note:
            # - FULL_ONCE reserves run_index early.
            # - If the runner exits mid-run, last_run stays reserved (outcome=null) and blocks convergence.
            # - For BENCH cases, we keep resume deterministic by finalizing based on local terminal artifacts
            #   (summary.yaml or benchmark_result.json). If those are missing, mark FAILED so rerun can proceed.
            summary_path = run_dir / "summary.yaml"
            outcome = RUN_OUTCOME_FAILED
            counted = False
            finished_at = int(time.time())
            error_detail: Optional[str] = None

            summary_obj: Optional[Dict[str, Any]] = None
            if summary_path.exists():
                try:
                    raw_summary = _load_yaml_file(summary_path)
                    if isinstance(raw_summary, dict):
                        summary_obj = raw_summary
                except Exception:
                    summary_obj = None

            summary_is_terminal = False
            if summary_obj is not None and summary_obj.get("outcome") in (RUN_OUTCOME_SUCCESS, RUN_OUTCOME_FAILED):
                # English note:
                # - We always create summary.yaml upfront with outcome=FAILED and an "INCOMPLETE" marker.
                # - If the runner exits before finalize(), this placeholder must not be treated as a terminal record.
                err = summary_obj.get("error")
                if not (isinstance(err, str) and err == _RUN_SUMMARY_INCOMPLETE_ERROR):
                    summary_is_terminal = True

            if summary_is_terminal:
                outcome = _require_str(summary_obj.get("outcome"), "reserved resume.summary.outcome")
                counted = bool(summary_obj.get("counted") is True)
                timing = summary_obj.get("timing")
                if isinstance(timing, dict) and isinstance(timing.get("finished_at_unix_s"), int):
                    finished_at = int(timing["finished_at_unix_s"])
                error_detail = summary_obj.get("error") if isinstance(summary_obj.get("error"), str) else None
            else:
                result_path = run_dir / "benchmark_result.json"
                if result_path.exists():
                    try:
                        parsed = json.loads(result_path.read_text(encoding="utf-8"))
                        result_obj = _require_dict(parsed, "reserved resume.test_stack.benchmark_result")
                        _validate_test_stack_benchmark_result(result_obj, case_id=case_id)
                        outcome = RUN_OUTCOME_SUCCESS
                    except Exception as exc:  # noqa: BLE001
                        outcome = RUN_OUTCOME_FAILED
                        error_detail = (
                            "failed to validate benchmark_result.json for reserved resume: "
                            f"{type(exc).__name__}: {exc}"
                        )
                else:
                    error_detail = "reserved run cannot be resumed: missing terminal artifacts (summary.yaml/benchmark_result.json)"

                # Always overwrite the placeholder summary.yaml so the run_dir is diagnosable after resume.
                # Keep any existing fields (e.g. test_stack result blocks) if present.
                merged = summary_obj if isinstance(summary_obj, dict) else {}
                merged["schema_version"] = SCHEMA_VERSION
                merged["case_id"] = case_id
                merged["case_key"] = _require_str(rec.get("case_key"), f"case_runs[{case_id}].case_key")
                merged["run_index"] = int(run_index)
                merged["outcome"] = outcome
                merged["counted"] = False
                timing = merged.get("timing")
                if not isinstance(timing, dict):
                    timing = {}
                    merged["timing"] = timing
                timing["finished_at_unix_s"] = int(finished_at)
                if error_detail is not None:
                    merged["error"] = error_detail
                _write_yaml_file(summary_path, merged)

            slot = _RunSlot(
                case_key=_require_str(rec.get("case_key"), f"case_runs[{case_id}].case_key"),
                case_id=case_id,
                run_index=run_index,
                rec=rec,
            )
            _finalize_run_slot(
                case_runs,
                slot,
                outcome=outcome,
                counted=bool(outcome == RUN_OUTCOME_SUCCESS and counted and planned_case.counted),
                finished_at_unix_s=int(finished_at),
            )
            _write_yaml_file(case_runs_path, case_runs)
            print(
                "[RESUME reserved last_run] "
                f"case_id={case_id} run_index={run_index} outcome={outcome} reason=non_ci_terminal_artifacts",
                flush=True,
            )
            continue

        deploy = _require_dict(resolved_case.get("deploy"), "reserved resume.resolved_case.deploy")
        controller_url = _require_str(deploy.get("controller_url"), "reserved resume.resolved_case.deploy.controller_url").rstrip("/")
        runtime = _require_dict(resolved_case.get("runtime"), "reserved resume.resolved_case.runtime")
        stack_identity = _require_dict(
            runtime.get("stack_identity"),
            "reserved resume.resolved_case.runtime.stack_identity",
        )
        existing_contract = _ci_command_contract_from_resolved_case(resolved_case)
        expected_contract = _ci_command_contract_from_planned(
            planned_case.ci_commands,
            ctx=f"planned[{case_id}].ci_commands",
        )
        if existing_contract != expected_contract:
            _ensure_stack_controller_online(stack_identity)
            _cleanup_skipped_case_desired_applies(controller_url=controller_url, case_id=case_id)
            finished_at = int(time.time())
            summary_path = run_dir / "summary.yaml"
            if summary_path.exists():
                s = _require_dict(_load_yaml_file(summary_path), "reserved resume.summary")
                s["outcome"] = RUN_OUTCOME_FAILED
                s["counted"] = False
                timing = s.get("timing")
                if isinstance(timing, dict):
                    timing["finished_at_unix_s"] = int(finished_at)
                s["error"] = (
                    "reserved run cannot be resumed: CI command contract changed and the old run_dir "
                    "materialized stale ci_runner inputs. A fresh run is required to rebuild "
                    "resolved_case/ci_runner with the current suite config. "
                    f"old={json.dumps(_json_canonicalize(existing_contract), ensure_ascii=True, sort_keys=True)} "
                    f"new={json.dumps(_json_canonicalize(expected_contract), ensure_ascii=True, sort_keys=True)}"
                )
                _write_yaml_file(summary_path, s)
            slot = _RunSlot(
                case_key=_require_str(rec.get("case_key"), f"case_runs[{case_id}].case_key"),
                case_id=case_id,
                run_index=run_index,
                rec=rec,
            )
            _finalize_run_slot(
                case_runs,
                slot,
                outcome=RUN_OUTCOME_FAILED,
                counted=False,
                finished_at_unix_s=finished_at,
            )
            _write_yaml_file(case_runs_path, case_runs)
            print(
                "[RESUME reserved last_run] "
                f"case_id={case_id} run_index={run_index} outcome=FAILED reason=ci_command_contract_changed",
                flush=True,
            )
            continue

        # Resume is only valid if the controller still reports live desired state for this case.
        #
        # Causal chain:
        # - We reserve last_run.run_index early for observability.
        # - If the runner exits before deploying the CI runner workload, there will be no remote exit_code.txt.
        # - The previous logic would then wait up to CI_RUNNER_EXIT_CODE_TIMEOUT_S on a non-existent workload.
        # - We keep resume deterministic by checking controller desired state: if no workload name matches
        #   "<case_id>__*", this reserved run cannot make progress and must be finalized as FAILED, allowing a rerun.
        # Resume is only meaningful if the controller still desires the CI runner workload itself.
        # Master/owner workloads can remain desired after a partial deploy, which must not block resume.
        inst = _find_deploy_instance(resolved_case, instance_id="ci_runner")
        k8s_ref = _require_str(inst.get("k8s_ref"), "reserved resume.ci_runner.k8s_ref")
        if "/" not in k8s_ref:
            raise ValueError("reserved resume.ci_runner.k8s_ref must be <deployment|daemonset>/<name>")
        _, ci_runner_workload_name = k8s_ref.split("/", 1)
        if not _ops_current_deployments_has_workload_name(controller_url, workload_name=ci_runner_workload_name):
            finished_at = int(time.time())
            summary_path = run_dir / "summary.yaml"
            if summary_path.exists():
                s = _require_dict(_load_yaml_file(summary_path), "reserved resume.summary")
                s["outcome"] = RUN_OUTCOME_FAILED
                s["counted"] = False
                timing = s.get("timing")
                if isinstance(timing, dict):
                    timing["finished_at_unix_s"] = int(finished_at)
                s["error"] = (
                    "reserved run cannot be resumed: controller no longer reports desired workloads for ci_runner; "
                    "previous runner likely exited before deploying CI runner."
                )
                _write_yaml_file(summary_path, s)
            slot = _RunSlot(
                case_key=_require_str(rec.get("case_key"), f"case_runs[{case_id}].case_key"),
                case_id=case_id,
                run_index=run_index,
                rec=rec,
            )
            _finalize_run_slot(
                case_runs,
                slot,
                outcome=RUN_OUTCOME_FAILED,
                counted=False,
                finished_at_unix_s=finished_at,
            )
            _write_yaml_file(case_runs_path, case_runs)
            print(
                "[RESUME reserved last_run] "
                f"case_id={case_id} run_index={run_index} outcome=FAILED reason=no_desired_ci_runner",
                flush=True,
            )
            continue

        rc = _wait_ci_runner_exit_code_resume(
            resolved_case=resolved_case,
            run_dir=run_dir,
            timeout_s=_ci_runner_exit_code_timeout_seconds(resolved_case),
        )
        outcome = RUN_OUTCOME_SUCCESS if rc == 0 else RUN_OUTCOME_FAILED
        counted = bool(outcome == RUN_OUTCOME_SUCCESS and planned_case.counted)

        started_at = int(time.time())
        summary_path = run_dir / "summary.yaml"
        if summary_path.exists():
            try:
                raw_summary = _load_yaml_file(summary_path)
                if isinstance(raw_summary, dict):
                    timing = raw_summary.get("timing")
                    if isinstance(timing, dict) and isinstance(timing.get("started_at_unix_s"), int):
                        started_at = int(timing["started_at_unix_s"])
            except Exception:
                # Resume is deterministic and must not guess. If summary.yaml is corrupted,
                # keep started_at as "now" so case_runs.yaml can still converge.
                pass

        finished_at = int(time.time())
        summary = _build_ci_summary_yaml(
            resolved_case,
            run_index=run_index,
            started_at_unix_s=started_at,
            finished_at_unix_s=finished_at,
            outcome=outcome,
            counted=counted,
            ci_out={"rc": int(rc)},
        )
        _write_yaml_file(summary_path, summary)

        # Complete the reserved slot so subsequent runs do not allocate a new run_N while an earlier
        # run already has a terminal exit_code on disk.
        slot = _RunSlot(
            case_key=_require_str(rec.get("case_key"), f"case_runs[{case_id}].case_key"),
            case_id=case_id,
            run_index=run_index,
            rec=rec,
        )
        _finalize_run_slot(
            case_runs,
            slot,
            outcome=outcome,
            counted=counted,
            finished_at_unix_s=finished_at,
        )
        _write_yaml_file(case_runs_path, case_runs)
        print(
            "[RESUME reserved last_run] "
            f"case_id={case_id} run_index={run_index} rc={rc} outcome={outcome} counted={counted}",
            flush=True,
        )


def _ops_current_deployments_has_case_id(controller_url: str, *, case_id: str) -> bool:
    prefix = case_id + "__"
    for raw_group in _ops_current_deployments(controller_url):
        group = _require_dict(raw_group, "current_deployments.groups[]")
        workloads = _require_list(group.get("workloads"), "current_deployments.groups[].workloads")
        for raw_workload in workloads:
            workload = _require_dict(raw_workload, "current_deployments.groups[].workloads[]")
            name = _require_str(workload.get("name"), "current_deployments.groups[].workloads[].name")
            if name.startswith(prefix):
                return True
    return False


def _ops_current_deployments_in_namespace(
    controller_url: str,
    *,
    namespace: str,
) -> List[Tuple[str, List[str]]]:
    namespace = _require_str(namespace, "current_deployments.namespace")
    matched: List[Tuple[str, List[str]]] = []
    for group_index, raw_group in enumerate(_ops_current_deployments(controller_url)):
        group = _require_dict(raw_group, f"current_deployments.groups[{group_index}]")
        group_namespace = _require_str(
            group.get("namespace"),
            f"current_deployments.groups[{group_index}].namespace",
        )
        if group_namespace != namespace:
            continue
        apply_id = _require_str(
            group.get("apply_id"),
            f"current_deployments.groups[{group_index}].apply_id",
        )
        workloads = _require_list(
            group.get("workloads"),
            f"current_deployments.groups[{group_index}].workloads",
        )
        names: List[str] = []
        for workload_index, raw_workload in enumerate(workloads):
            workload = _require_dict(
                raw_workload,
                f"current_deployments.groups[{group_index}].workloads[{workload_index}]",
            )
            names.append(
                _require_str(
                    workload.get("name"),
                    f"current_deployments.groups[{group_index}].workloads[{workload_index}].name",
                )
            )
        matched.append((apply_id, names))
    return matched


def _cleanup_bench_namespace_preflight(*, controller_url: str, namespace: str) -> None:
    deadline = time.time() + 300.0
    while True:
        matched = _ops_current_deployments_in_namespace(controller_url, namespace=namespace)
        if not matched:
            print(
                f"[BENCH preflight cleanup] namespace is clean: namespace={namespace}",
                flush=True,
            )
            return
        print(
            f"[BENCH preflight cleanup] deleting namespace leftovers: namespace={namespace} groups={len(matched)}",
            flush=True,
        )
        for apply_id, names in matched:
            print(
                "[BENCH preflight cleanup] "
                f"apply_id={apply_id} workloads={names}",
                flush=True,
            )
            _ops_delete_apply_id(
                controller_url,
                apply_id=apply_id,
                ctx=f"BENCH preflight cleanup namespace={namespace}",
            )
        if time.time() >= deadline:
            raise ValueError(
                "BENCH preflight cleanup did not converge after timeout: "
                f"namespace={namespace} remaining_apply_ids={[apply_id for (apply_id, _) in matched]}"
            )
        time.sleep(1.0)


def _bench_case_preflight_cleanup(*, stack_identity: Dict[str, Any], planned_case: _PlannedCase, suite: _SuiteConfig) -> None:
    scene_item = _require_dict(
        suite.scenes.get(planned_case.case.scene_id),
        f"suite.scenes[{planned_case.case.scene_id!r}]",
    )
    if _case_family_from_scene_item(scene_item, f"suite.scenes[{planned_case.case.scene_id!r}]") != CASE_FAMILY_BENCH:
        return
    _ensure_stack_controller_online(stack_identity)
    _verify_active_test_bed_selection_supervisor_matches_bundle()
    controller_url = _require_str(stack_identity.get("controller_url"), "stack_identity.controller_url")
    _cleanup_bench_namespace_preflight(
        controller_url=controller_url,
        namespace=_test_stack_ops_namespace(),
    )


def _ops_current_deployments_has_workload_name(controller_url: str, *, workload_name: str) -> bool:
    # English note:
    # - current_deployments groups list concrete workload names (stable, unique).
    # - For CI reserved resume, checking "any workload with case_id prefix" is too broad because
    #   master/owner workloads can remain desired even after ci_runner is gone, which would block
    #   resume waiting forever. The caller should check the specific instance workload name.
    workload_name = _require_str(workload_name, "current_deployments.workload_name")
    for raw_group in _ops_current_deployments(controller_url):
        group = _require_dict(raw_group, "current_deployments.groups[]")
        workloads = _require_list(group.get("workloads"), "current_deployments.groups[].workloads")
        for raw_workload in workloads:
            workload = _require_dict(raw_workload, "current_deployments.groups[].workloads[]")
            name = _require_str(workload.get("name"), "current_deployments.groups[].workloads[].name")
            if name == workload_name:
                return True
    return False


def _finalize_run_slot(
    case_runs: Dict[str, Any],
    slot: _RunSlot,
    *,
    outcome: str,
    counted: bool,
    finished_at_unix_s: int,
) -> None:
    rec = slot.rec
    rec["total_runs"] = int(rec.get("total_runs", 0)) + 1
    if outcome == RUN_OUTCOME_SUCCESS:
        rec["success_runs"] = int(rec.get("success_runs", 0)) + 1
    else:
        rec["failed_runs"] = int(rec.get("failed_runs", 0)) + 1

    if counted:
        rec["counted_runs"] = int(rec.get("counted_runs", 0)) + 1

    rec["last_run"] = {
        "run_index": slot.run_index,
        "outcome": outcome,
        "finished_at_unix_s": int(finished_at_unix_s),
    }

    # Validate invariants
    total_runs = int(rec.get("total_runs", 0))
    success_runs = int(rec.get("success_runs", 0))
    failed_runs = int(rec.get("failed_runs", 0))
    counted_runs = int(rec.get("counted_runs", 0))
    if total_runs != success_runs + failed_runs:
        raise ValueError("case_runs invariant failed at finalize: total_runs")
    if counted_runs > total_runs:
        raise ValueError("case_runs invariant failed at finalize: counted_runs")



def _build_resolved_case_yaml(
    case: _ResolvedCase,
    suite: _Suite,
    *,
    config_root: str,
    workdir_root: str,
    run_dir: str,
    ci_commands: Optional[List[Dict[str, str]]],
    execution_label: str,
    command_id: Optional[str],
    test_id: Optional[str],
    stack_identity: Dict[str, Any],
) -> Dict[str, Any]:
    scene_src = copy.deepcopy(suite.scenes[case.scene_id])
    scale_src = copy.deepcopy(suite.scales[case.scale_id])
    profile_src = copy.deepcopy(suite.profiles[case.profile_id])
    case_family = _case_family_from_scene_item(scene_src, f"scene[{case.scene_id}]")
    artifact_set_id = _resolved_case_artifact_set_id(
        case=case,
        suite=suite,
        profile_src=profile_src,
    )
    artifact_set = _require_dict(
        suite.artifact_sets.get(artifact_set_id),
        f"artifact_sets[{artifact_set_id!r}]",
    )
    artifact_release_source = copy.deepcopy(
        _require_dict(
            artifact_set.get("release_source"),
            f"artifact_sets[{artifact_set_id!r}].release_source",
        )
    )
    artifact_release_artifacts = copy.deepcopy(
        _require_dict(
            artifact_set.get("release_artifacts"),
            f"artifact_sets[{artifact_set_id!r}].release_artifacts",
        )
    )
    artifact_test_rsc_source = copy.deepcopy(
        _require_dict(
            artifact_set.get("test_rsc_source"),
            f"artifact_sets[{artifact_set_id!r}].test_rsc_source",
        )
    )
    artifact_test_rsc_artifacts = copy.deepcopy(
        _require_dict(
            artifact_set.get("test_rsc_artifacts"),
            f"artifact_sets[{artifact_set_id!r}].test_rsc_artifacts",
        )
    )
    release_root_override_opt = _local_release_root_override_opt()
    if release_root_override_opt is not None:
        release_key_prefix_relpath = _require_str(
            artifact_release_source.get("key_prefix"),
            f"artifact_sets[{artifact_set_id!r}].release_source.key_prefix",
        )
        test_rsc_key_prefix_name = Path(
            _require_str(
                artifact_test_rsc_source.get("key_prefix"),
                f"artifact_sets[{artifact_set_id!r}].test_rsc_source.key_prefix",
            )
        ).name
        artifact_release_source["local_cache_root"] = str(
            _artifact_cache_subtree_from_parent_root(
                parent_root=release_root_override_opt,
                key_prefix_relpath=release_key_prefix_relpath,
                manifest_filename=_RELEASE_MANIFEST_FILENAME,
                ctx=_LOCAL_RELEASE_CACHE_ROOT_OVERRIDE_ENV,
            ).resolve()
        )
        artifact_test_rsc_source["local_cache_root"] = str(
            _artifact_cache_subtree_from_parent_root(
                parent_root=release_root_override_opt / "test_rsc",
                key_prefix_relpath=test_rsc_key_prefix_name,
                manifest_filename=_TEST_RSC_MANIFEST_FILENAME,
                ctx=_LOCAL_RELEASE_CACHE_ROOT_OVERRIDE_ENV,
            ).resolve()
        )
    materialized_release_root = str((Path(run_dir).resolve() / "fluxon_release").resolve())
    materialized_test_rsc_root = str((Path(run_dir).resolve() / "test_rsc").resolve())
    profile_runtime_src = _require_dict(
        profile_src.get("runtime"),
        f"profiles[{case.profile_id!r}].runtime",
    )

    if case_family == CASE_FAMILY_CI:
        if ci_commands is None or not ci_commands:
            raise ValueError(f"ci_commands must be provided for CI case: {case.case_id}")

        scene_ci = _require_dict(scene_src.get("ci"), "resolved_case.scene_source.ci")
        runtime_contract = _require_ci_runtime_contract(
            scene_ci.get("runtime_contract"),
            "resolved_case.scene_source.ci.runtime_contract",
        )
        topology = _require_test_stack_machine_count(scale_src.get("topology"), "resolved_case.scale_source.topology")
        if runtime_contract == CI_RUNTIME_CONTRACT_CLUSTER_KV_OWNER and topology != 2:
            raise ValueError(
                f"CI runtime_contract={runtime_contract!r} requires topology=2, got: {topology!r}"
            )
        targets = copy.deepcopy(_require_dict(scale_src.get("targets"), "resolved_case.scale_source.targets"))
        scale_out: Dict[str, Any] = {
            "duration_seconds": _require_int(scale_src.get("duration_seconds"), "scale.duration_seconds", min_v=1),
            "topology": topology,
            "targets": targets,
        }
        scale_out["owner"] = copy.deepcopy(_require_dict(scale_src.get("owner"), "resolved_case.scale_source.owner"))

        profile_ci = _require_dict(profile_runtime_src.get("ci"), "resolved_case.profile_source.runtime.ci")
        deploy_out = copy.deepcopy(_require_dict(profile_ci.get("deploy"), "resolved_case.profile_source.ci.deploy"))
        deploy_out["release_root"] = materialized_release_root
        deploy_out["test_rsc_root"] = materialized_test_rsc_root
        runtime_contracts = _require_dict(
            profile_ci.get("runtime_contracts"),
            "resolved_case.profile_source.ci.runtime_contracts",
        )
        selected_runtime = copy.deepcopy(
            _require_dict(
                runtime_contracts.get(runtime_contract),
                f"resolved_case.profile_source.ci.runtime_contracts[{runtime_contract!r}]",
            )
        )
        command_tokens = profile_ci.get("command_tokens")
        if command_tokens is not None:
            command_tokens = copy.deepcopy(
                _require_dict(command_tokens, "resolved_case.profile_source.ci.command_tokens")
            )

        scene = {
            "ci": {
                "subject": _require_scene_subject(scene_ci.get("subject"), "resolved_case.scene_source.ci.subject"),
                "runtime_contract": runtime_contract,
                "commands": copy.deepcopy(ci_commands),
            }
        }
        scale = scale_out
        profile = {
            "deploy": deploy_out,
            "ci": {
                "runtime_contract": runtime_contract,
                "runtime": selected_runtime,
            },
        }
        if command_tokens is not None:
            profile["ci"]["command_tokens"] = command_tokens
    elif case_family == CASE_FAMILY_BENCH:
        scene_ts = _require_dict(scene_src.get("test_stack"), "resolved_case.scene_source.test_stack")
        benchmark = copy.deepcopy(_require_dict(scale_src.get("benchmark"), "resolved_case.scale_source.benchmark"))
        duration_seconds = _require_int(scale_src.get("duration_seconds"), "scale.duration_seconds", min_v=1)
        topology = copy.deepcopy(scale_src.get("topology"))
        mode = _require_str(scene_ts.get("mode"), "scene.test_stack.mode")
        subject = _require_scene_subject(scene_ts.get("subject"), "scene.test_stack.subject")

        profile_ts = _require_dict(profile_runtime_src.get("test_stack"), "resolved_case.profile_source.runtime.test_stack")
        backend_kind = _require_test_stack_backend_kind(
            profile_ts.get("kind"),
            "resolved_case.profile_source.test_stack.kind",
        )
        _validate_test_stack_backend_subject(
            backend_kind=backend_kind,
            subject=subject,
            ctx="resolved_case.profile_source.test_stack.kind",
        )
        deploy_out = copy.deepcopy(_require_dict(profile_ts.get("deploy"), "resolved_case.profile_source.test_stack.deploy"))
        profile_target_ip_map = copy.deepcopy(
            _require_dict(
                deploy_out.get("target_ip_map"),
                "resolved_case.profile_source.test_stack.deploy.target_ip_map",
            )
        )
        active_target_ip_map = _active_test_stack_target_ip_map(
            ctx="resolved_case.profile_source.test_stack.deploy.target_ip_map"
        )
        merged_target_ip_map = copy.deepcopy(profile_target_ip_map)
        for target, ip in active_target_ip_map.items():
            merged_target_ip_map[
                _require_str(target, "resolved_case.profile_source.test_stack.deploy.target_ip_map key")
            ] = _require_str(
                ip,
                f"resolved_case.profile_source.test_stack.deploy.target_ip_map[{target!r}]",
            )
        deploy_out["target_ip_map"] = merged_target_ip_map
        target_ip_map = _require_dict(
            deploy_out.get("target_ip_map"),
            "resolved_case.profile_source.test_stack.deploy.target_ip_map",
        )
        machine_targets = _test_stack_scale_machine_targets(
            scale_src,
            ctx="resolved_case.scale_source",
            target_ip_map=target_ip_map,
        )
        role_plan = _build_test_stack_role_plan(
            scene_ts,
            scale_src,
            ctx="resolved_case.scene_source.test_stack",
            target_ip_map=target_ip_map,
        )
        targets = {"hosts": copy.deepcopy(machine_targets)}
        deploy_out["release_root"] = materialized_release_root
        runtime_config = copy.deepcopy(_require_dict(profile_ts.get("runtime_config"), "resolved_case.profile_source.test_stack.runtime_config"))
        deploy_templates = copy.deepcopy(_require_dict(profile_ts.get("deploy_templates"), "resolved_case.profile_source.test_stack.deploy_templates"))
        port_alloc = _resolve_test_stack_port_alloc(
            profile_ts.get("port_alloc"),
            topology=topology,
            backend_kind=backend_kind,
            ctx="resolved_case.profile_source.test_stack.port_alloc",
        )
        runtime_env = _normalize_runtime_env_map(
            profile_ts.get("runtime_env"),
            "resolved_case.profile_source.test_stack.runtime_env",
        )
        coordinator_ready_timeout_seconds = _require_int(
            profile_ts.get("coordinator_ready_timeout_seconds"),
            "resolved_case.profile_source.test_stack.coordinator_ready_timeout_seconds",
            min_v=1,
        )

        owner_out = copy.deepcopy(_require_dict(scale_src.get("owner"), "resolved_case.scale_source.owner"))
        scene = {"test_stack": copy.deepcopy(scene_ts)}
        scene["test_stack"]["workload_id"] = case.scene_id
        scale = {
            "duration_seconds": duration_seconds,
            "topology": topology,
            "targets": targets,
            "machine_targets": machine_targets,
            "owner": owner_out,
            "role_plan": role_plan,
            "benchmark": benchmark,
        }
        profile = {
            "deploy": deploy_out,
            "test_stack": {
                "kind": backend_kind,
                "port_alloc": port_alloc,
                "runtime_config": runtime_config,
                "deploy_templates": deploy_templates,
                "runtime_env": runtime_env,
                "coordinator_ready_timeout_seconds": coordinator_ready_timeout_seconds,
            },
        }
    else:
        raise ValueError(f"unsupported case family for suite schema v{SUITE_SCHEMA_VERSION}: {case_family}")

    case_out: Dict[str, Any] = {
        "case_id": case.case_id,
        "case_key": case.case_key,
        "profile_id": case.profile_id,
        "family": case_family,
        "artifact_set": artifact_set_id,
        "run_mode": suite.run_mode,
        "execution_label": execution_label,
    }
    if command_id is not None:
        case_out["command_id"] = command_id
    if test_id is not None:
        case_out["test_id"] = test_id

    out: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "runtime": {
            "config_root": config_root,
            "workdir_root": workdir_root,
            "run_dir": run_dir,
            "stack_identity": stack_identity,
        },
        "runtime_model": _build_runtime_model(case_family),
        "artifact_set": {
            "id": artifact_set_id,
            "release_source": artifact_release_source,
            "release_root": materialized_release_root,
            "release_artifacts": artifact_release_artifacts,
            "test_rsc_source": artifact_test_rsc_source,
            "test_rsc_root": materialized_test_rsc_root,
            "test_rsc_artifacts": artifact_test_rsc_artifacts,
        },
        "case": case_out,
        "scene": scene,
        "scale": scale,
        "profile": profile,
    }
    runtime_tokens = _build_runtime_token_mapping(
        workdir_root=workdir_root,
        run_dir=run_dir,
        release_root=materialized_release_root,
        test_rsc_root=materialized_test_rsc_root,
        case_id=case.case_id,
        profile_id=case.profile_id,
        stack_identity=stack_identity,
    )
    out["scene"] = _subst_obj_tokens(out["scene"], runtime_tokens, "resolved_case.scene")
    out["scale"] = _subst_obj_tokens(out["scale"], runtime_tokens, "resolved_case.scale")
    out["profile"] = _subst_obj_tokens(out["profile"], runtime_tokens, "resolved_case.profile")

    out["deploy"] = _require_dict(copy.deepcopy(out["profile"].get("deploy")), "resolved_case.profile.deploy")
    out["deploy"] = _require_dict(
        _subst_obj_tokens(out["deploy"], runtime_tokens, "resolved_case.deploy"),
        "resolved_case.deploy",
    )
    _sync_case_runtime_model_from_deploy(out)

    return out


def _deep_merge_dict(dst: Dict[str, Any], src: Dict[str, Any]) -> Dict[str, Any]:
    """Deep-merge two dicts (dict->dict merges recursively; lists/scalars overwrite).

    This is used only for building runtime config artifacts deterministically.
    """
    out = copy.deepcopy(dst)
    for k, v in src.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge_dict(_require_dict(out[k], "merge.dst"), _require_dict(v, "merge.src"))
        else:
            out[k] = copy.deepcopy(v)
    return out


def _subst_obj_tokens(obj: Any, mapping: Dict[str, str], ctx: str) -> Any:
    """Replace tokens inside values of nested (dict/list/str) structures.

    Keys are kept as-is. Token substitution inside keys is forbidden to avoid schema ambiguity.
    """
    if isinstance(obj, str):
        out = obj
        for k, v in mapping.items():
            out = out.replace(k, v)
        return out
    if isinstance(obj, list):
        return [_subst_obj_tokens(x, mapping, ctx) for x in obj]
    if isinstance(obj, dict):
        out: Dict[str, Any] = {}
        for k, v in obj.items():
            if isinstance(k, str) and any(tok in k for tok in mapping.keys()):
                raise ValueError(f"{ctx} contains token in dict key: {k!r}")
            out[k] = _subst_obj_tokens(v, mapping, ctx)
        return out
    return obj


def _test_stack_roles_by_mode(mode: str) -> List[str]:
    if mode == TEST_STACK_MODE_MPMC:
        return ["producer", "consumer"]
    if mode == TEST_STACK_MODE_KVSTORE:
        return [KV_NODE_ROLE_WORKER]
    if mode == TEST_STACK_MODE_KVSTORE_WITH_LOCAL_CACHE:
        return [KV_NODE_ROLE_SEED, KV_NODE_ROLE_WORKER]
    if mode == TEST_STACK_MODE_RPC:
        return [KV_NODE_ROLE_SEED, KV_NODE_ROLE_WORKER]
    if mode == TEST_STACK_MODE_PY_FS:
        return ["agent", "client"]
    raise ValueError(f"invalid test_stack mode: {mode!r}")


def _test_stack_runtime_mode_for_scene_mode(mode: str) -> str:
    if mode == TEST_STACK_MODE_PY_FS:
        return TEST_STACK_MODE_KVSTORE
    return mode


def _test_stack_scene_mode_uses_external_kv(mode: str) -> bool:
    return mode in (TEST_STACK_MODE_MPMC, TEST_STACK_MODE_PY_FS)


def _test_stack_runtime_role_for_scene_role(*, scene_mode: str, role: str) -> str:
    if scene_mode != TEST_STACK_MODE_PY_FS:
        return role
    if role == "agent":
        return KV_NODE_ROLE_SEED
    if role == "client":
        return KV_NODE_ROLE_WORKER
    raise ValueError(f"unsupported PY_FS scene role: {role!r}")


def _test_stack_scene_uses_per_target_process_fanout(*, scene_mode: str) -> bool:
    return scene_mode in (
        TEST_STACK_MODE_KVSTORE,
        TEST_STACK_MODE_KVSTORE_WITH_LOCAL_CACHE,
        TEST_STACK_MODE_RPC,
    )


def _write_benchmark_config_py(run_dir: Path, config_obj: Dict[str, Any]) -> Path:
    import pprint

    out_path = run_dir / "benchmark_config.py"
    if out_path.exists():
        raise ValueError(f"benchmark_config.py already exists (no overwrite): {out_path}")
    text = (
        "from __future__ import annotations\n"
        "from typing import Any, Dict\n\n"
        f"CONFIG: Dict[str, Any] = {pprint.pformat(config_obj, width=120, sort_dicts=False)}\n"
    )
    out_path.write_text(text, encoding="utf-8")
    return out_path


def _load_test_stack_benchmark_config(run_dir: Path) -> Dict[str, Any]:
    config_path = (run_dir / "benchmark_config.py").resolve()
    if not config_path.exists():
        raise ValueError(f"benchmark_config.py not found: {config_path}")
    mod = _load_python_module(config_path, module_name="fluxon_test_stack_benchmark_config")
    return _require_dict(getattr(mod, "CONFIG", None), "benchmark_config.CONFIG")


def _test_stack_owner_targets_from_role_plan(
    *,
    role_plan: Dict[str, Any],
    roles_order: List[str],
) -> List[str]:
    owner_target_set: set[str] = set()
    for role in roles_order:
        plan = _require_dict(role_plan.get(role), f"scale.role_plan[{role}]")
        targets = _require_list(plan.get("targets"), f"scale.role_plan[{role}].targets")
        for raw_target in targets:
            owner_target_set.add(_require_str(raw_target, f"scale.role_plan[{role}].targets[]"))
    return sorted(owner_target_set)


def _test_stack_explicit_owner_targets(
    *,
    owner_scale: Dict[str, Any],
    owner_count: Optional[int] = None,
    ctx: str,
) -> Optional[List[str]]:
    raw_targets = owner_scale.get("targets")
    if raw_targets is None:
        return None
    if owner_count is None:
        owner_count = _require_int(owner_scale.get("owner_count"), f"{ctx}.owner_count", min_v=1)
    targets = _require_list(raw_targets, f"{ctx}.targets")
    if len(targets) != int(owner_count):
        raise ValueError(
            f"{ctx}.targets length must equal owner_count: owner_count={owner_count} targets={len(targets)}"
        )
    out: List[str] = []
    seen: set[str] = set()
    for idx, raw_target in enumerate(targets):
        target = _require_str(raw_target, f"{ctx}.targets[{idx}]")
        if target in seen:
            raise ValueError(f"{ctx}.targets contains duplicate target: {target!r}")
        seen.add(target)
        out.append(target)
    return out


def _test_stack_owner_targets(
    *,
    scale: Dict[str, Any],
    role_plan: Dict[str, Any],
    roles_order: List[str],
    target_ip_map: Optional[Dict[str, Any]] = None,
    ctx: str,
) -> List[str]:
    owner_scale = _require_dict(scale.get("owner"), f"{ctx}.owner")
    owner_count = _require_int(owner_scale.get("owner_count"), f"{ctx}.owner.owner_count", min_v=1)
    explicit_targets = _test_stack_explicit_owner_targets(
        owner_scale=owner_scale,
        owner_count=int(owner_count),
        ctx=f"{ctx}.owner",
    )
    owner_targets = (
        explicit_targets
        if explicit_targets is not None
        else _test_stack_owner_targets_from_role_plan(role_plan=role_plan, roles_order=roles_order)
    )
    if not owner_targets:
        raise ValueError(f"{ctx} requires at least one owner target")
    if target_ip_map is not None:
        for target in owner_targets:
            _ = _require_str(target_ip_map.get(target), f"resolved_case.deploy.target_ip_map[{target!r}]")
    return owner_targets


def _test_stack_redis_command(
    *,
    server_binary: Path,
    bundle_root: Path,
    data_dir: Path,
    port: int,
    server_args: List[str],
    password: Optional[str],
) -> str:
    extra_args = list(server_args)
    if password is not None and password != "":
        extra_args.extend(["--requirepass", password])
    arg_list = [
        _shell_quote(str(server_binary.resolve())),
        "--bind",
        "0.0.0.0",
        "--protected-mode",
        "no",
        "--save",
        "",
        "--appendonly",
        "no",
        "--port",
        str(int(port)),
        "--dir",
        _shell_quote(str(data_dir.resolve())),
    ] + [_shell_quote(arg) for arg in extra_args]
    return (
        "set -euo pipefail\n"
        + "mkdir -p "
        + _shell_quote(str(data_dir.resolve()))
        + "\n"
        + "exec "
        + " ".join(arg_list)
        + "\n"
    )


TEST_STACK_REDIS_MAXMEMORY_NUMERATOR = 8
TEST_STACK_REDIS_MAXMEMORY_DENOMINATOR = 10
TEST_STACK_REDIS_MAXMEMORY_POLICY = "noeviction"
TEST_STACK_KV_KEYSPACE_FILL_RATIO_NUMERATOR = 7
TEST_STACK_KV_KEYSPACE_FILL_RATIO_DENOMINATOR = 10


def _strip_cli_option(argv: List[str], option: str) -> List[str]:
    out: List[str] = []
    skip_next = False
    for raw_arg in argv:
        arg = _require_str(raw_arg, f"redis argv item for {option}")
        if skip_next:
            skip_next = False
            continue
        if arg == option:
            skip_next = True
            continue
        if arg.startswith(option + "="):
            continue
        out.append(arg)
    return out


def _test_stack_redis_server_args_with_owner_limit(
    *,
    server_args: List[str],
    owner_dram_bytes: int,
) -> tuple[List[str], int]:
    if int(owner_dram_bytes) <= 0:
        raise ValueError(f"owner_dram_bytes must be > 0 for redis baseline, got: {owner_dram_bytes}")
    maxmemory_bytes = max(
        1,
        (int(owner_dram_bytes) * TEST_STACK_REDIS_MAXMEMORY_NUMERATOR) // TEST_STACK_REDIS_MAXMEMORY_DENOMINATOR,
    )
    effective_args = _strip_cli_option(list(server_args), "--maxmemory")
    effective_args = _strip_cli_option(effective_args, "--maxmemory-policy")
    effective_args.extend(
        [
            "--maxmemory",
            str(int(maxmemory_bytes)),
            "--maxmemory-policy",
            TEST_STACK_REDIS_MAXMEMORY_POLICY,
        ]
    )
    return effective_args, int(maxmemory_bytes)


def _test_stack_expected_value_size_bytes(
    *,
    ts_scene: Dict[str, Any],
    bench_value_size: int,
    ctx: str,
) -> int:
    value_size_mode = ts_scene.get("value_size_mode")
    if value_size_mode is None:
        return max(1, int(bench_value_size))
    value_size_mode_str = _require_str(value_size_mode, f"{ctx}.value_size_mode").strip().upper()
    if value_size_mode_str == "FIXED":
        return max(1, int(bench_value_size))
    if value_size_mode_str != "RANDOM_WEIGHTED_SET":
        raise ValueError(f"{ctx}.value_size_mode invalid: {value_size_mode_str!r}")
    weighted_set = _require_list(ts_scene.get("value_size_weighted_set"), f"{ctx}.value_size_weighted_set")
    total_weight = 0.0
    weighted_size = 0.0
    for idx, raw_item in enumerate(weighted_set):
        item = _require_dict(raw_item, f"{ctx}.value_size_weighted_set[{idx}]")
        size_bytes = _require_int(item.get("size_bytes"), f"{ctx}.value_size_weighted_set[{idx}].size_bytes", min_v=1)
        weight_raw = item.get("weight")
        if not isinstance(weight_raw, (int, float)):
            raise ValueError(f"{ctx}.value_size_weighted_set[{idx}].weight must be number")
        weight = float(weight_raw)
        if weight <= 0.0:
            raise ValueError(f"{ctx}.value_size_weighted_set[{idx}].weight must be > 0")
        total_weight += weight
        weighted_size += float(size_bytes) * weight
    if total_weight <= 0.0:
        raise ValueError(f"{ctx}.value_size_weighted_set total weight must be > 0")
    expected = int(weighted_size / total_weight)
    return max(1, expected)


def _test_stack_effective_kv_keyspace_size(
    *,
    case_id: str,
    ts_scene: Dict[str, Any],
    scale: Dict[str, Any],
    bench_value_size: int,
) -> int:
    raw_keyspace_size = ts_scene.get("keyspace_size")
    if raw_keyspace_size is None:
        raise ValueError("scene.test_stack.keyspace_size is required for KV capacity sizing")
    requested_keyspace_size = _require_int(raw_keyspace_size, "scene.test_stack.keyspace_size", min_v=1)
    owner_scale = _require_dict(scale.get("owner"), "resolved_case.scale.owner")
    owner_count = _require_int(owner_scale.get("owner_count"), "resolved_case.scale.owner.owner_count", min_v=1)
    owner_dram_bytes = _require_int(
        owner_scale.get("owner_dram_bytes"),
        "resolved_case.scale.owner.owner_dram_bytes",
        min_v=16777216,
    )
    expected_value_size_bytes = _test_stack_expected_value_size_bytes(
        ts_scene=ts_scene,
        bench_value_size=int(bench_value_size),
        ctx="scene.test_stack",
    )
    cluster_effective_capacity_bytes = max(
        1,
        (int(owner_count) * int(owner_dram_bytes) * TEST_STACK_REDIS_MAXMEMORY_NUMERATOR)
        // TEST_STACK_REDIS_MAXMEMORY_DENOMINATOR,
    )
    cluster_target_fill_bytes = max(
        1,
        (int(cluster_effective_capacity_bytes) * TEST_STACK_KV_KEYSPACE_FILL_RATIO_NUMERATOR)
        // TEST_STACK_KV_KEYSPACE_FILL_RATIO_DENOMINATOR,
    )
    max_keyspace_size = max(1, int(cluster_target_fill_bytes) // int(expected_value_size_bytes))
    effective_keyspace_size = min(int(requested_keyspace_size), int(max_keyspace_size))
    if effective_keyspace_size < requested_keyspace_size:
        print(
            "INFO: clamped TEST_STACK KV keyspace_size for capacity guard: "
            f"case_id={case_id} requested={requested_keyspace_size} effective={effective_keyspace_size} "
            f"max_fit={max_keyspace_size} owner_count={owner_count} owner_dram_bytes={owner_dram_bytes} "
            f"expected_value_size_bytes={expected_value_size_bytes} "
            f"redis_limit_ratio={TEST_STACK_REDIS_MAXMEMORY_NUMERATOR}/{TEST_STACK_REDIS_MAXMEMORY_DENOMINATOR} "
            f"fill_ratio={TEST_STACK_KV_KEYSPACE_FILL_RATIO_NUMERATOR}/{TEST_STACK_KV_KEYSPACE_FILL_RATIO_DENOMINATOR}"
        )
    return int(effective_keyspace_size)


def _build_test_stack_redis_instances(
    *,
    resolved_case: Dict[str, Any],
    owner_targets: List[str],
    target_ip_map: Dict[str, Any],
    redis_port: int,
    run_dir: Path,
    test_stack_runtime: Dict[str, str],
    rc_redis: Dict[str, Any],
    owner_dram_bytes: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    server_binary = Path(
        _require_str(test_stack_runtime.get("redis_server_binary"), "test_stack_runtime.redis_server_binary")
    ).resolve()
    bundle_root = Path(
        _require_str(test_stack_runtime.get("redis_bundle_root"), "test_stack_runtime.redis_bundle_root")
    ).resolve()
    server_args = [
        _require_str(raw_arg, "profile.test_stack.runtime_config.redis.server_args[]")
        for raw_arg in _require_list(
            rc_redis.get("server_args", []),
            "profile.test_stack.runtime_config.redis.server_args",
        )
    ]
    server_args, maxmemory_bytes = _test_stack_redis_server_args_with_owner_limit(
        server_args=server_args,
        owner_dram_bytes=int(owner_dram_bytes),
    )
    rc_redis["server_args"] = copy.deepcopy(server_args)
    rc_redis["maxmemory_bytes"] = int(maxmemory_bytes)
    rc_redis["maxmemory_policy"] = TEST_STACK_REDIS_MAXMEMORY_POLICY
    password = rc_redis.get("password")
    if password is not None:
        password = _require_str(password, "profile.test_stack.runtime_config.redis.password")

    redis_instances: List[Dict[str, Any]] = []
    endpoints: List[Dict[str, Any]] = []
    for target in owner_targets:
        if target not in target_ip_map:
            raise ValueError(f"target not found in deploy.target_ip_map for redis baseline: {target!r}")
        target_slug = re.sub(r"[^a-z0-9_.-]+", "-", target.strip().lower())
        if not target_slug:
            raise ValueError(f"invalid TEST_STACK redis target: {target!r}")
        instance_id = f"{TEST_STACK_REDIS_INSTANCE_ID_PREFIX}{target_slug}"
        if not _ID_RE.fullmatch(instance_id):
            raise ValueError(f"computed TEST_STACK redis instance_id is invalid: {instance_id!r} target={target!r}")
        data_dir = (run_dir / "services" / "redis" / target_slug).resolve()
        redis_instances.append(
            {
                "id": instance_id,
                "k8s_ref": f"deployment/test_stack_redis__{target_slug}",
                "lifecycle": "service",
                "deployer": {
                    "target": target,
                    "command": ["/bin/bash", "-lc"],
                    "args": [
                        _test_stack_redis_command(
                            server_binary=server_binary,
                            bundle_root=bundle_root,
                            data_dir=data_dir,
                            port=redis_port,
                            server_args=server_args,
                            password=password,
                        )
                    ],
                },
            }
        )
        endpoints.append(
            {
                "host": _require_str(target_ip_map.get(target), f"deploy.target_ip_map[{target!r}]"),
                "port": int(redis_port),
            }
        )

    database_raw = rc_redis.get("database")
    database = 0 if database_raw is None else _require_int(database_raw, "profile.test_stack.runtime_config.redis.database", min_v=0)
    connect_timeout_raw = rc_redis.get("connect_timeout_seconds")
    socket_timeout_raw = rc_redis.get("socket_timeout_seconds")
    kv_base = {
        "backend_kind": TEST_STACK_BACKEND_REDIS,
        "redis": {
            "endpoints": endpoints,
            "database": int(database),
            "connect_timeout_seconds": 5.0 if connect_timeout_raw is None else float(connect_timeout_raw),
            "socket_timeout_seconds": 30.0 if socket_timeout_raw is None else float(socket_timeout_raw),
        },
    }
    if password is not None and password != "":
        kv_base["redis"]["password"] = password
    return redis_instances, kv_base


def _test_stack_alluxio_command(
    *,
    bundle_root: Path,
    mount_root: Path,
    command_body: str,
) -> str:
    bundle_root = bundle_root.resolve()
    mount_root = mount_root.resolve()
    if not bundle_root.exists() or not bundle_root.is_dir():
        raise ValueError(f"TEST_STACK alluxio bundle root must exist: {bundle_root}")
    body = _require_str(command_body, "profile.test_stack.runtime_config.alluxio.mount_command_template").strip()
    if not body:
        raise ValueError("profile.test_stack.runtime_config.alluxio.mount_command_template must be non-empty")
    return (
        "set -euo pipefail\n"
        + "mkdir -p "
        + _shell_quote(str(mount_root))
        + "\n"
        + body
        + ("\n" if not body.endswith("\n") else "")
    )


def _build_test_stack_alluxio_instances(
    *,
    owner_targets: List[str],
    target_ip_map: Dict[str, Any],
    test_stack_runtime: Dict[str, Any],
    rc_alluxio: Dict[str, Any],
) -> List[Dict[str, Any]]:
    bundle_root = Path(
        _require_str(test_stack_runtime.get("alluxio_bundle_root"), "test_stack_runtime.alluxio_bundle_root")
    ).resolve()
    mount_root_by_target = _require_dict(
        rc_alluxio.get("mount_root_by_target"),
        "profile.test_stack.runtime_config.alluxio.mount_root_by_target",
    )
    command_template = _require_str(
        rc_alluxio.get("mount_command_template"),
        "profile.test_stack.runtime_config.alluxio.mount_command_template",
    )

    alluxio_instances: List[Dict[str, Any]] = []
    for target in owner_targets:
        if target not in target_ip_map:
            raise ValueError(f"target not found in deploy.target_ip_map for alluxio baseline: {target!r}")
        mount_root = Path(
            _require_str(
                mount_root_by_target.get(target),
                f"profile.test_stack.runtime_config.alluxio.mount_root_by_target[{target!r}]",
            )
        ).resolve()
        target_slug = re.sub(r"[^a-z0-9_.-]+", "-", target.strip().lower())
        if not target_slug:
            raise ValueError(f"invalid TEST_STACK alluxio target: {target!r}")
        instance_id = f"{TEST_STACK_ALLUXIO_INSTANCE_ID_PREFIX}{target_slug}"
        if not _ID_RE.fullmatch(instance_id):
            raise ValueError(f"computed TEST_STACK alluxio instance_id is invalid: {instance_id!r} target={target!r}")
        command_body = _require_str(
            _subst_obj_tokens(
                command_template,
                {
                    "__TARGET__": target,
                    "__INSTANCE_KEY__": instance_id,
                    "__MOUNT_ROOT__": str(mount_root),
                    "__ALLUXIO_BUNDLE_ROOT__": str(bundle_root),
                },
                "profile.test_stack.runtime_config.alluxio.mount_command_template",
            ),
            "profile.test_stack.runtime_config.alluxio.mount_command_template",
        )
        alluxio_instances.append(
            {
                "id": instance_id,
                "k8s_ref": f"deployment/test_stack_alluxio__{target_slug}",
                "lifecycle": "service",
                "deployer": {
                    "target": target,
                    "command": ["/bin/bash", "-lc"],
                    "args": [
                        _test_stack_alluxio_command(
                            bundle_root=bundle_root,
                            mount_root=mount_root,
                            command_body=command_body,
                        )
                    ],
                },
            }
        )
    return alluxio_instances


def _build_test_stack_alluxio_base(
    *,
    runtime_instance_prefix: str,
    rc_alluxio: Dict[str, Any],
) -> Dict[str, Any]:
    namespace_prefix_raw = rc_alluxio.get("namespace_prefix")
    namespace_prefix = (
        _require_clean_relpath(
            namespace_prefix_raw,
            "profile.test_stack.runtime_config.alluxio.namespace_prefix",
        )
        if namespace_prefix_raw is not None
        else f"fluxon_test_stack/{runtime_instance_prefix}"
    )
    return {
        "backend_kind": TEST_STACK_BACKEND_ALLUXIO,
        "alluxio": {
            "namespace_prefix": namespace_prefix,
        },
    }


def _test_stack_target_slug(*, target: str, ctx: str) -> str:
    target_slug = re.sub(r"[^a-z0-9_.-]+", "-", target.strip().lower())
    if not target_slug:
        raise ValueError(f"invalid TEST_STACK target for {ctx}: {target!r}")
    return target_slug


def _test_stack_kv_owner_instance_id(*, owner_target: str, ctx: str) -> tuple[str, str]:
    target_slug = _test_stack_target_slug(target=owner_target, ctx=ctx)
    instance_id = f"{TEST_STACK_KV_OWNER_INSTANCE_ID_PREFIX}{target_slug}"
    if not _ID_RE.fullmatch(instance_id):
        raise ValueError(
            f"computed TEST_STACK owner instance_id is invalid: {instance_id!r} target={owner_target!r}"
        )
    return target_slug, instance_id


def _test_stack_kv_owner_runtime_instance_key(*, runtime_instance_prefix: str, owner_target: str, ctx: str) -> str:
    target_slug = _test_stack_target_slug(target=owner_target, ctx=ctx)
    return f"{runtime_instance_prefix}__kv_owner__{target_slug}"


def _build_test_stack_external_kv_owner_instances(
    *,
    scene_mode: str,
    resolved_case: Dict[str, Any],
    scale: Dict[str, Any],
    runtime: Dict[str, Any],
    run_dir: Path,
    cfg_dir: Path,
    coord_tpl: Dict[str, Any],
    test_stack_runtime: Dict[str, Any],
    cluster_nodes: Dict[str, Dict[str, Any]],
    owner_targets: List[str],
    needs_kv_master: bool,
    kv_p2p_port_base: int,
    kv_p2p_port_stride: int,
    kv_p2p_slot_offset: int,
    p2p_ports_per_slot: int,
    node_total: int,
    run_index: int,
    runtime_instance_prefix: str,
    kv_base: Dict[str, Any],
    test_spec_config: Dict[str, Any],
    perf_config: Optional[Dict[str, Any]],
    runtime_env: Dict[str, str],
    owner_group_processes: Optional[int],
    owner_cpu_core_by_target: Dict[str, int],
) -> List[Dict[str, Any]]:
    required_python_abi = _test_stack_runtime_required_python_abi(
        resolved_case=resolved_case,
        run_dir=run_dir,
    )
    owner_scale = _require_dict(scale.get("owner"), "resolved_case.scale.owner")
    owner_count = _require_int(owner_scale.get("owner_count"), "scale.owner.owner_count", min_v=1)
    if owner_count != len(owner_targets):
        raise ValueError(
            f"TEST_STACK {scene_mode} requires owner_count to match owner target list length: "
            f"owner_count={owner_count} targets={len(owner_targets)}"
        )
    owner_dram_bytes = _require_int(owner_scale.get("owner_dram_bytes"), "scale.owner.owner_dram_bytes", min_v=16777216)
    if owner_dram_bytes % 16777216 != 0:
        raise ValueError("scale.owner.owner_dram_bytes must be 16MiB aligned")

    stack_identity = _require_dict(runtime.get("stack_identity"), "runtime.stack_identity")
    cluster_name = _require_str(stack_identity.get("cluster_name"), "runtime.stack_identity.cluster_name")
    shared_memory_root = _require_str(stack_identity.get("shared_memory_path"), "runtime.stack_identity.shared_memory_path")
    shared_file_root = _require_str(stack_identity.get("shared_file_path"), "runtime.stack_identity.shared_file_path")
    etcd_endpoints = _test_stack_etcd_addresses(resolved_case)
    master_port_offset = 0
    owner_instances: List[Dict[str, Any]] = []
    for owner_ordinal, target in enumerate(owner_targets):
        target_slug, instance_id = _test_stack_kv_owner_instance_id(
            owner_target=target,
            ctx="external kv owner",
        )
        owner_p2p_listen_port = (
            int(kv_p2p_port_base)
            + int(kv_p2p_port_stride) * int(run_index - 1)
            + int(kv_p2p_slot_offset) * int(p2p_ports_per_slot)
            + int(node_total)
            + int(master_port_offset)
            + int(owner_ordinal)
        )
        if owner_p2p_listen_port <= 0 or owner_p2p_listen_port > 65535:
            raise ValueError(f"computed owner_p2p_listen_port out of range: {owner_p2p_listen_port}")

        if owner_group_processes is None:
            owner_shared_memory_path = shared_memory_root
            owner_shared_file_path = shared_file_root
        else:
            owner_shared_memory_path, owner_shared_file_path = _owner_bundle_roots_for_target(
                shared_memory_root=shared_memory_root,
                shared_file_root=shared_file_root,
                owner_target=target,
                ctx="runtime.stack_identity owner bundle roots",
            )
        owner_cfg = {
            "instance_key": _test_stack_kv_owner_runtime_instance_key(
                runtime_instance_prefix=runtime_instance_prefix,
                owner_target=target,
                ctx="external kv owner",
            ),
            "contribute_to_cluster_pool_size": {"dram": int(owner_dram_bytes), "vram": {}},
            "fluxonkv_spec": {
                "etcd_addresses": list(etcd_endpoints),
                "cluster_name": cluster_name,
                "shared_memory_path": owner_shared_memory_path,
                "shared_file_path": owner_shared_file_path,
                "sub_cluster": FLUXON_KV_OWNER_SUB_CLUSTER,
                "p2p_listen_port": int(owner_p2p_listen_port),
            },
        }
        owner_protocol = kv_base.get("protocol")
        if owner_protocol is not None:
            owner_cfg["protocol"] = copy.deepcopy(
                _require_dict(
                    owner_protocol,
                    "profile.test_stack.runtime_config.kv_base.protocol",
                )
            )
        pprof_duration_seconds = kv_base.get("pprof_duration_seconds")
        if pprof_duration_seconds is not None:
            owner_cfg["pprof_duration_seconds"] = int(pprof_duration_seconds)
        if test_spec_config:
            owner_cfg["test_spec_config"] = copy.deepcopy(test_spec_config)
        owner_cfg_path = (cfg_dir / f"test_stack_kv_owner__{target_slug}.yaml").resolve()
        if owner_cfg_path.exists():
            raise ValueError(f"test_stack owner config already exists (no overwrite): {owner_cfg_path}")
        _write_yaml_file(owner_cfg_path, owner_cfg)

        owner_services_dir = run_dir / "services" / "kv_owner" / target_slug
        owner_services_dir.mkdir(parents=True, exist_ok=True)
        owner_inst = copy.deepcopy(coord_tpl)
        owner_inst["id"] = instance_id
        owner_inst["k8s_ref"] = f"deployment/test_stack_kv_owner__{target_slug}"
        owner_deployer = _require_dict(owner_inst.get("deployer"), "test_stack_owner_template.deployer")
        owner_deployer["target"] = target
        owner_deployer["command"] = ["/bin/bash", "-lc"]
        exec_wrapper_argv: Optional[List[str]] = None
        if target in owner_cpu_core_by_target:
            exec_wrapper_argv = ["taskset", "-c", str(owner_cpu_core_by_target[target])]
        owner_cmd = _test_stack_runtime_module_command(
            run_dir=run_dir,
            venv_python=_test_stack_target_host_venv_python(
                node_cfg=_require_dict(cluster_nodes.get(target), f"cluster_nodes[{target}]"),
                target_name=target,
                python_abi=required_python_abi,
            ),
            module_name="fluxon_py.runtime.start_owner_kvclient",
            module_args=[
                "-c",
                str(owner_cfg_path),
                "-w",
                str(owner_services_dir.resolve()),
            ],
            runtime_env=runtime_env,
            exec_wrapper_argv=exec_wrapper_argv,
        )
        if perf_config is not None and "owner" in _require_list(perf_config.get("targets"), "perf_config.targets"):
            owner_cmd = _test_stack_perf_wrapper_command(
                inner_command=owner_cmd,
                output_dir=owner_services_dir,
                perf_label=f"kv_owner__{target_slug}",
                perf_config=perf_config,
            )
        owner_deployer["args"] = [owner_cmd]
        owner_instances.append(owner_inst)
    return owner_instances


def _build_test_stack_mooncake_owner_instances(
    *,
    resolved_case: Dict[str, Any],
    scale: Dict[str, Any],
    run_dir: Path,
    cfg_dir: Path,
    coord_tpl: Dict[str, Any],
    cluster_nodes: Dict[str, Dict[str, Any]],
    owner_targets: List[str],
    runtime_instance_prefix: str,
    kv_base: Dict[str, Any],
    test_spec_config: Dict[str, Any],
    perf_config: Optional[Dict[str, Any]],
    runtime_env: Dict[str, str],
) -> List[Dict[str, Any]]:
    required_python_abi = _test_stack_runtime_required_python_abi(
        resolved_case=resolved_case,
        run_dir=run_dir,
    )
    owner_scale = _require_dict(scale.get("owner"), "resolved_case.scale.owner")
    owner_count = _require_int(owner_scale.get("owner_count"), "scale.owner.owner_count", min_v=1)
    if owner_count != len(owner_targets):
        raise ValueError(
            "TEST_STACK Mooncake requires owner_count to match owner target list length: "
            f"owner_count={owner_count} targets={len(owner_targets)}"
        )
    owner_dram_bytes = _require_int(
        owner_scale.get("owner_dram_bytes"),
        "scale.owner.owner_dram_bytes",
        min_v=16777216,
    )
    if owner_dram_bytes % 16777216 != 0:
        raise ValueError("scale.owner.owner_dram_bytes must be 16MiB aligned")

    mooncake_spec_base = copy.deepcopy(
        _require_dict(
            kv_base.get("mooncake_spec"),
            "profile.test_stack.runtime_config.kv_base.mooncake_spec",
        )
    )
    metadata_server = _require_str(
        mooncake_spec_base.get("metadata_server"),
        "profile.test_stack.runtime_config.kv_base.mooncake_spec.metadata_server",
    )
    master_server_address = _require_str(
        mooncake_spec_base.get("master_server_address"),
        "profile.test_stack.runtime_config.kv_base.mooncake_spec.master_server_address",
    )
    etcd_addresses = [
        _require_str(raw_addr, "profile.test_stack.runtime_config.kv_base.mooncake_spec.etcd_addresses[]")
        for raw_addr in _require_list(
            mooncake_spec_base.get("etcd_addresses"),
            "profile.test_stack.runtime_config.kv_base.mooncake_spec.etcd_addresses",
        )
    ]
    _ = _require_int(
        mooncake_spec_base.get("local_buffer_size"),
        "profile.test_stack.runtime_config.kv_base.mooncake_spec.local_buffer_size",
        min_v=16777216,
    )

    owner_instances: List[Dict[str, Any]] = []
    for target in owner_targets:
        target_slug, instance_id = _test_stack_kv_owner_instance_id(
            owner_target=target,
            ctx="mooncake owner",
        )

        owner_cfg = {
            "instance_key": _test_stack_kv_owner_runtime_instance_key(
                runtime_instance_prefix=runtime_instance_prefix,
                owner_target=target,
                ctx="mooncake owner",
            ),
            "contribute_to_cluster_pool_size": {"dram": int(owner_dram_bytes), "vram": {}},
            "mooncake_spec": {
                # Mooncake owner/server should contribute storage capacity only.
                # The benchmark node keeps the local transfer buffer; owner stays pure server mode.
                "local_buffer_size": 0,
                "metadata_server": metadata_server,
                "master_server_address": master_server_address,
                "etcd_addresses": list(etcd_addresses),
            },
            "protocol": copy.deepcopy(
                _require_dict(
                    kv_base.get("protocol"),
                    "profile.test_stack.runtime_config.kv_base.protocol",
                )
            ),
        }
        pprof_duration_seconds = kv_base.get("pprof_duration_seconds")
        if pprof_duration_seconds is not None:
            owner_cfg["pprof_duration_seconds"] = int(pprof_duration_seconds)
        if test_spec_config:
            owner_cfg["test_spec_config"] = copy.deepcopy(test_spec_config)
        owner_cfg_path = (cfg_dir / f"test_stack_kv_owner__{target_slug}.yaml").resolve()
        if owner_cfg_path.exists():
            raise ValueError(f"test_stack owner config already exists (no overwrite): {owner_cfg_path}")
        _write_yaml_file(owner_cfg_path, owner_cfg)

        owner_services_dir = run_dir / "services" / "kv_owner" / target_slug
        owner_services_dir.mkdir(parents=True, exist_ok=True)
        owner_inst = copy.deepcopy(coord_tpl)
        owner_inst["id"] = instance_id
        owner_inst["k8s_ref"] = f"deployment/test_stack_kv_owner__{target_slug}"
        owner_deployer = _require_dict(owner_inst.get("deployer"), "test_stack_owner_template.deployer")
        owner_deployer["target"] = target
        owner_deployer["command"] = ["/bin/bash", "-lc"]
        owner_cmd = _test_stack_runtime_module_command(
            run_dir=run_dir,
            venv_python=_test_stack_target_host_venv_python(
                node_cfg=_require_dict(cluster_nodes.get(target), f"cluster_nodes[{target}]"),
                target_name=target,
                python_abi=required_python_abi,
            ),
            module_name="fluxon_py.runtime.start_owner_kvclient",
            module_args=[
                "-c",
                str(owner_cfg_path),
                "-w",
                str(owner_services_dir.resolve()),
            ],
            runtime_env=runtime_env,
        )
        if perf_config is not None and "owner" in _require_list(perf_config.get("targets"), "perf_config.targets"):
            owner_cmd = _test_stack_perf_wrapper_command(
                inner_command=owner_cmd,
                output_dir=owner_services_dir,
                perf_label=f"kv_owner__{target_slug}",
                perf_config=perf_config,
            )
        owner_deployer["args"] = [owner_cmd]
        owner_instances.append(owner_inst)
    return owner_instances


def _ci_materialized_target_for_instance(*, topology: Any, targets: Dict[str, Any], instance_id: str, ctx: str) -> str:
    machine_count = _require_test_stack_machine_count(topology, f"{ctx}.topology")
    primary = _require_str(targets.get("primary"), f"{ctx}.targets.primary")
    if instance_id == "master":
        return primary
    if instance_id in ("owner_0", "ci_runner"):
        if machine_count == 1:
            return primary
        if machine_count == 2:
            return _require_str(targets.get("secondary"), f"{ctx}.targets.secondary")
    raise ValueError(f"{ctx} unsupported CI instance id for placement: {instance_id}")


def _compile_ci_case(resolved_case: Dict[str, Any]) -> None:
    scale = _require_dict(resolved_case.get("scale"), "resolved_case.scale")
    topology = scale.get("topology")
    _ = _require_test_stack_machine_count(topology, "resolved_case.scale.topology")
    targets = _require_dict(scale.get("targets"), "resolved_case.scale.targets")

    profile = _require_dict(resolved_case.get("profile"), "resolved_case.profile")
    profile_ci = _require_dict(profile.get("ci"), "resolved_case.profile.ci")
    runtime_templates = _require_dict(profile_ci.get("runtime"), "resolved_case.profile.ci.runtime")
    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    case_runtime_templates = _require_dict(
        runtime_templates.get(RUNTIME_LAYER_CASE),
        f"resolved_case.profile.ci.runtime.{RUNTIME_LAYER_CASE}",
    )

    ordered_instance_ids = [
        instance_id for instance_id in CI_CASE_RUNTIME_INSTANCE_IDS if instance_id in case_runtime_templates
    ]
    if not ordered_instance_ids:
        raise ValueError("resolved_case.profile.ci.runtime.case_runtime must be non-empty")
    compiled_instances: List[Dict[str, Any]] = []
    for instance_id in ordered_instance_ids:
        template = copy.deepcopy(
            _require_dict(
                case_runtime_templates.get(instance_id),
                f"resolved_case.profile.ci.runtime.{RUNTIME_LAYER_CASE}[{instance_id!r}]",
            )
        )
        materialized_target = _ci_materialized_target_for_instance(
            topology=topology,
            targets=targets,
            instance_id=instance_id,
            ctx="resolved_case.scale",
        )
        instance = _require_dict(
            _subst_obj_tokens(
                template,
                {
                    "__TARGET__": materialized_target,
                },
                f"ci.runtime.{RUNTIME_LAYER_CASE}[{instance_id}]",
            ),
            f"ci.runtime.{RUNTIME_LAYER_CASE}[{instance_id}].compiled",
        )
        instance["id"] = instance_id
        compiled_instances.append(instance)

    deploy["instances"] = compiled_instances
    _set_runtime_layer_instance_ids(
        resolved_case,
        layer=RUNTIME_LAYER_CASE,
        instance_ids=[_require_str(inst.get("id"), "deploy.instances[].id") for inst in compiled_instances],
    )



def _compile_test_stack_case(resolved_case: Dict[str, Any], *, run_index: int) -> Dict[str, Any]:
    """Compile TEST_STACK (Scene, Scale, Profile) into (deploy.instances, benchmark_config.py)."""
    scene = _require_dict(resolved_case.get("scene"), "resolved_case.scene")
    scale = _require_dict(resolved_case.get("scale"), "resolved_case.scale")
    profile = _require_dict(resolved_case.get("profile"), "resolved_case.profile")
    case_obj = _require_dict(resolved_case.get("case"), "resolved_case.case")
    case_id = _require_str(case_obj.get("case_id"), "resolved_case.case.case_id")

    ts_scene = _require_dict(scene.get("test_stack"), "resolved_case.scene.test_stack")
    scene_mode = _require_str(ts_scene.get("mode"), "scene.test_stack.mode")
    runtime_mode = _test_stack_runtime_mode_for_scene_mode(scene_mode)
    roles_order = _test_stack_roles_by_mode(scene_mode)

    role_plan = _require_dict(scale.get("role_plan"), "scale.role_plan")

    # Enforce Scale.role_plan.roles ⊆ Scene.roles (Scene.roles are derived from mode).
    bad = [r for r in role_plan.keys() if r not in set(roles_order)]
    if bad:
        raise ValueError(f"scale.role_plan contains roles not allowed by mode={scene_mode}: {sorted(bad)}")
    missing = [r for r in roles_order if r not in role_plan]
    if missing:
        raise ValueError(f"scale.role_plan missing required roles for mode={scene_mode}: {missing}")

    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    target_ip_map = _require_dict(deploy.get("target_ip_map"), "resolved_case.deploy.target_ip_map")

    ts_profile = _require_dict(profile.get("test_stack"), "resolved_case.profile.test_stack")
    backend_kind = _require_test_stack_backend_kind(
        ts_profile.get("kind"),
        "resolved_case.profile.test_stack.kind",
    )
    runtime_env = _normalize_runtime_env_map(
        ts_profile.get("runtime_env"),
        "resolved_case.profile.test_stack.runtime_env",
    )
    scene_subject = _require_scene_subject(ts_scene.get("subject"), "resolved_case.scene.test_stack.subject")
    _validate_test_stack_backend_subject(
        backend_kind=backend_kind,
        subject=scene_subject,
        ctx="resolved_case.profile.test_stack.kind",
    )
    port_alloc = _require_dict(ts_profile.get("port_alloc"), "profile.test_stack.port_alloc")
    port_base = _require_int(port_alloc.get("coordinator_port_base"), "profile.test_stack.port_alloc.coordinator_port_base", min_v=1)
    port_stride = _require_int(port_alloc.get("coordinator_port_stride"), "profile.test_stack.port_alloc.coordinator_port_stride", min_v=1)
    kv_master_port_base: Optional[int] = None
    kv_master_port_stride: Optional[int] = None
    kv_p2p_port_base: Optional[int] = None
    kv_p2p_port_stride: Optional[int] = None
    redis_port_base: Optional[int] = None
    redis_port_stride: Optional[int] = None
    if backend_kind == TEST_STACK_BACKEND_FLUXON:
        kv_master_port_base = _require_int(port_alloc.get("kv_master_port_base"), "profile.test_stack.port_alloc.kv_master_port_base", min_v=1)
        kv_master_port_stride = _require_int(port_alloc.get("kv_master_port_stride"), "profile.test_stack.port_alloc.kv_master_port_stride", min_v=1)
        kv_p2p_port_base = _require_int(port_alloc.get("kv_p2p_port_base"), "profile.test_stack.port_alloc.kv_p2p_port_base", min_v=1)
        kv_p2p_port_stride = _require_int(port_alloc.get("kv_p2p_port_stride"), "profile.test_stack.port_alloc.kv_p2p_port_stride", min_v=1)
    elif backend_kind == TEST_STACK_BACKEND_REDIS:
        redis_port_base = _require_int(port_alloc.get("redis_port_base"), "profile.test_stack.port_alloc.redis_port_base", min_v=1)
        redis_port_stride = _require_int(port_alloc.get("redis_port_stride"), "profile.test_stack.port_alloc.redis_port_stride", min_v=1)
    runtime = _require_dict(resolved_case.get("runtime"), "resolved_case.runtime")
    run_dir = Path(_require_str(runtime.get("run_dir"), "runtime.run_dir")).resolve()
    cfg_dir = (run_dir / "configs").resolve()
    cfg_dir.mkdir(parents=True, exist_ok=True)
    runner_root = _test_stack_runner_root(run_dir)
    port_slot_offset = _test_stack_runner_port_slot(
        runner_root=runner_root,
        stride=port_stride,
    )
    # Keep run_index as the major port bucket so sequential runs in the same runner root stay ordered.
    # Add a runner-root-local offset inside one stride slot so two different workdirs no longer reuse
    # the exact same coordinator port when both start from run_1.
    coordinator_port = int(port_base) + int(port_stride) * int(run_index - 1) + int(port_slot_offset)
    if coordinator_port <= 0 or coordinator_port > 65535:
        raise ValueError(f"computed coordinator_port out of range: {coordinator_port}")

    kv_master_port: Optional[int] = None
    if backend_kind == TEST_STACK_BACKEND_FLUXON:
        assert kv_master_port_base is not None
        assert kv_master_port_stride is not None
        master_port_slot_offset = _test_stack_runner_port_slot(
            runner_root=runner_root,
            stride=kv_master_port_stride,
        )
        kv_master_port = (
            int(kv_master_port_base)
            + int(kv_master_port_stride) * int(run_index - 1)
            + int(master_port_slot_offset)
        )
        if kv_master_port <= 0 or kv_master_port > 65535:
            raise ValueError(f"computed kv_master_port out of range: {kv_master_port}")

    runtime_instance_prefix = _stable_runtime_identity(resolved_case)
    release_root = Path(_require_str(deploy.get("release_root"), "resolved_case.deploy.release_root")).resolve()
    test_stack_runtime = _prepare_test_stack_runtime(
        resolved_case=resolved_case,
        release_root=release_root,
        run_dir=run_dir,
    )
    cluster_nodes, _ = _load_test_stack_cluster_nodes_and_dispatch(resolved_case)

    deploy_templates = _require_dict(ts_profile.get("deploy_templates"), "profile.test_stack.deploy_templates")
    coord_tpl = _require_dict(deploy_templates.get("coordinator"), "profile.test_stack.deploy_templates.coordinator")
    node_tpl = _require_dict(deploy_templates.get("node"), "profile.test_stack.deploy_templates.node")

    coord_dep = _require_dict(coord_tpl.get("deployer"), "coordinator_template.deployer")
    raw_machine_targets = scale.get("machine_targets")
    if raw_machine_targets is None:
        machine_targets = _test_stack_scale_machine_targets(scale, ctx="resolved_case.scale", target_ip_map=target_ip_map)
    else:
        machine_targets = _require_list(raw_machine_targets, "resolved_case.scale.machine_targets")
    if not machine_targets:
        raise ValueError("resolved_case.scale.machine_targets must be non-empty")
    coordinator_target = _require_str(machine_targets[0], "resolved_case.scale.machine_targets[0]")
    raw_coord_target = coord_dep.get("target")
    if raw_coord_target is None:
        coord_target = coordinator_target
    else:
        coord_target_str = _require_str(raw_coord_target, "coordinator_template.deployer.target")
        coord_target = coordinator_target if coord_target_str == "__TARGET__" else coord_target_str
    coord_ip = _require_str(target_ip_map.get(coord_target), "deploy.target_ip_map[coordinator_target]")
    coordinator_addr = f"{coord_ip}:{coordinator_port}"

    bench = _require_dict(scale.get("benchmark"), "scale.benchmark")
    processes_per_target = _require_int(
        bench.get("processes_per_target"),
        "scale.benchmark.processes_per_target",
        min_v=1,
    )
    threads_per_process = _require_int(
        bench.get("threads_per_process"),
        "scale.benchmark.threads_per_process",
        min_v=1,
    )
    if int(threads_per_process) != TEST_STACK_BENCHMARK_FIXED_THREADS_PER_PROCESS:
        raise ValueError(
            "scale.benchmark.threads_per_process must be fixed to "
            f"{TEST_STACK_BENCHMARK_FIXED_THREADS_PER_PROCESS}"
        )
    value_size = _require_int(bench.get("value_size"), "scale.benchmark.value_size", min_v=0)
    warmup = bench.get("metric_warmup_seconds")
    if not isinstance(warmup, (int, float)):
        raise ValueError("scale.benchmark.metric_warmup_seconds must be number")
    warmup_f = float(warmup)
    start_idle = bench.get("start_idle_seconds", 10)
    if not isinstance(start_idle, (int, float)):
        raise ValueError("scale.benchmark.start_idle_seconds must be number")
    start_idle_f = float(start_idle)
    if start_idle_f < 0.0:
        raise ValueError("scale.benchmark.start_idle_seconds must be >= 0")
    op_timeout = bench.get("op_timeout_seconds")
    if not isinstance(op_timeout, (int, float)):
        raise ValueError("scale.benchmark.op_timeout_seconds must be number")
    op_timeout_f = float(op_timeout)
    if op_timeout_f <= 0.0:
        raise ValueError("scale.benchmark.op_timeout_seconds must be > 0")
    cluster_ready_timeout_seconds = _require_int(
        bench.get("cluster_ready_timeout_seconds"),
        "scale.benchmark.cluster_ready_timeout_seconds",
        min_v=1,
    )
    max_secs = _require_int(scale.get("duration_seconds"), "scale.duration_seconds", min_v=1)
    if float(max_secs) - warmup_f < 30.0:
        raise ValueError(
            "Invalid benchmark durations: "
            f"duration_seconds({max_secs}) - metric_warmup_seconds({warmup_f}) < 30"
        )
    if op_timeout_f > float(max_secs):
        raise ValueError(
            "Invalid op timeout: "
            f"op_timeout_seconds({op_timeout_f}) > duration_seconds({max_secs})"
        )

    value_size_list = _require_list(bench.get("value_size_list"), "scale.benchmark.value_size_list")
    consumer_sim_handle_ms_range = bench.get("consumer_sim_handle_ms_range")

    # Compute a deterministic per-node P2P listen port allocation for this case.
    #
    # English note:
    # - Multiple TEST_STACK nodes may run on the same host, so they must not share an implicit port.
    # - Port allocation must be stable across resume within the same run_dir, but also avoid collisions across
    #   different workdirs on different machines (runner_root slot offset).
    node_total = 0
    for role in roles_order:
        plan = _require_dict(role_plan.get(role), f"scale.role_plan[{role}]")
        role_target_count = _require_int(plan.get("count"), f"scale.role_plan[{role}].count", min_v=1)
        if _test_stack_scene_uses_per_target_process_fanout(scene_mode=scene_mode):
            node_total += int(role_target_count) * int(processes_per_target)
        else:
            node_total += int(role_target_count)
    if node_total <= 0:
        raise ValueError("computed node_total must be positive")
    owner_targets = _test_stack_owner_targets(
        scale=scale,
        role_plan=role_plan,
        roles_order=roles_order,
        target_ip_map=target_ip_map,
        ctx="resolved_case.scale",
    )
    owner_group_processes = _test_stack_owner_group_processes(
        scale=scale,
        owner_targets=owner_targets,
        target_ip_map=target_ip_map,
        processes_per_target=int(processes_per_target),
        ctx="resolved_case.scale",
    )
    owner_targets_by_machine = _test_stack_owner_targets_by_machine(
        owner_targets=owner_targets,
        target_ip_map=target_ip_map,
        ctx="resolved_case.scale.owner.targets",
    )
    needs_kv_master = _test_stack_backend_requires_fluxon_kv_master(backend_kind=backend_kind, mode=scene_mode)
    uses_external_fluxon_kv = _test_stack_backend_uses_external_fluxon_kv(backend_kind=backend_kind, mode=scene_mode)
    uses_dedicated_kv_owners = _test_stack_backend_uses_dedicated_kv_owners(
        backend_kind=backend_kind,
        mode=scene_mode,
    )
    _require_explicit_owner_group_processes_for_multi_owner_same_machine(
        owner_targets_by_machine=owner_targets_by_machine,
        owner_group_processes=owner_group_processes,
        uses_external_fluxon_kv=uses_external_fluxon_kv,
        ctx="resolved_case.scale",
    )
    kv_p2p_slot_offset = 0
    p2p_ports_per_slot = 0
    if backend_kind == TEST_STACK_BACKEND_FLUXON:
        assert kv_p2p_port_stride is not None
        # English note:
        # - Port allocation must be stable across resume within the same run_dir.
        # - Allocate a disjoint KV P2P listen port for every KV process in the case:
        #   benchmark nodes + (optional) dedicated KV owners.
        owner_total = len(owner_targets) if uses_dedicated_kv_owners else 0
        p2p_ports_per_slot = node_total + owner_total
        if kv_p2p_port_stride < p2p_ports_per_slot:
            raise ValueError(
                "profile.test_stack.port_alloc.kv_p2p_port_stride too small for this case: "
                f"stride={kv_p2p_port_stride} required_ports_per_slot={p2p_ports_per_slot} "
                f"node_total={node_total} mode={scene_mode}"
            )
        kv_p2p_slot_count = kv_p2p_port_stride // p2p_ports_per_slot
        if kv_p2p_slot_count <= 0:
            raise ValueError(
                "profile.test_stack.port_alloc.kv_p2p_port_stride yields no slots for this case: "
                f"stride={kv_p2p_port_stride} required_ports_per_slot={p2p_ports_per_slot}"
            )
        kv_p2p_slot_offset = _test_stack_runner_port_slot(runner_root=runner_root, stride=kv_p2p_slot_count)

    # Expand nodes in deterministic role order.
    node_instances: List[Dict[str, Any]] = []
    node_roles: List[str] = []
    node_overrides: List[Dict[str, Any]] = []
    stack_cluster_name: Optional[str] = None
    stack_shared_memory_path: Optional[str] = None
    stack_shared_file_path: Optional[str] = None
    if backend_kind == TEST_STACK_BACKEND_FLUXON:
        stack_identity = _require_dict(runtime.get("stack_identity"), "runtime.stack_identity")
        stack_cluster_name = _require_str(
            stack_identity.get("cluster_name"),
            "runtime.stack_identity.cluster_name",
        )
        stack_shared_memory_path = _require_str(
            stack_identity.get("shared_memory_path"),
            "runtime.stack_identity.shared_memory_path",
        )
        stack_shared_file_path = _require_str(
            stack_identity.get("shared_file_path"),
            "runtime.stack_identity.shared_file_path",
        )

    rc = _require_dict(ts_profile.get("runtime_config"), "profile.test_stack.runtime_config")
    kv_base: Dict[str, Any]
    kv_node_patch_template: Dict[str, Any] = {}
    mq_base: Optional[Any] = None
    rc_redis: Optional[Dict[str, Any]] = None
    rc_alluxio: Optional[Dict[str, Any]] = None
    rc_mooncake_master: Optional[Dict[str, Any]] = None
    owner_cpu_core_by_target: Dict[str, int] = {}
    alluxio_mount_root_by_target: Dict[str, str] = {}
    redis_instances: List[Dict[str, Any]] = []
    alluxio_instances: List[Dict[str, Any]] = []
    mooncake_rpc_port: Optional[int] = None
    mooncake_metadata_port: Optional[int] = None
    mooncake_metrics_port: Optional[int] = None
    if backend_kind == TEST_STACK_BACKEND_FLUXON:
        owner_cpu_core_by_target = _normalize_owner_cpu_core_by_target(
            rc.get("owner_cpu_core_by_target"),
            "profile.test_stack.runtime_config.owner_cpu_core_by_target",
            target_ip_map=target_ip_map,
        )
        if owner_cpu_core_by_target:
            missing_owner_cpu_targets = [
                target for target in owner_targets if target not in owner_cpu_core_by_target
            ]
            if missing_owner_cpu_targets:
                raise ValueError(
                    "profile.test_stack.runtime_config.owner_cpu_core_by_target must cover every owner target: "
                    f"missing={missing_owner_cpu_targets}"
                )
            unexpected_owner_cpu_targets = sorted(
                target for target in owner_cpu_core_by_target.keys() if target not in set(owner_targets)
            )
            if unexpected_owner_cpu_targets:
                raise ValueError(
                    "profile.test_stack.runtime_config.owner_cpu_core_by_target contains non-owner targets for this case: "
                    f"unexpected={unexpected_owner_cpu_targets}"
                )
        kv_base = copy.deepcopy(_require_dict(rc.get("kv_base"), "profile.test_stack.runtime_config.kv_base"))
        owner_scale = _require_dict(scale.get("owner"), "resolved_case.scale.owner")
        owner_dram_bytes = _require_int(
            owner_scale.get("owner_dram_bytes"),
            "resolved_case.scale.owner.owner_dram_bytes",
            min_v=16777216,
        )
        # Apply kvowner memory from Scale as the single source of truth.
        #
        # English note:
        # - Scale encodes the baseline KV cluster capacity (node count + per-kvowner DRAM).
        # - Profile.test_stack.runtime_config.kv_base may provide a template shape only.
        # - External benchmark workloads must stay zero-contribution at the benchmark node.
        pool = _require_dict(
            kv_base.get("contribute_to_cluster_pool_size"),
            "profile.test_stack.runtime_config.kv_base.contribute_to_cluster_pool_size",
        )
        vram = _require_dict(pool.get("vram"), "profile.test_stack.runtime_config.kv_base.contribute_to_cluster_pool_size.vram")
        dram_contribution_bytes = 0 if uses_external_fluxon_kv else int(owner_dram_bytes)
        kv_base["contribute_to_cluster_pool_size"] = {
            "dram": int(dram_contribution_bytes),
            "vram": copy.deepcopy(vram),
        }
        if uses_external_fluxon_kv:
            if int(dram_contribution_bytes) != 0:
                raise ValueError(
                    f"profile.test_stack.runtime_config.kv_base must be zero-contribution for mode={scene_mode}: "
                    f"got contribute_to_cluster_pool_size.dram={dram_contribution_bytes}"
                )
            for gpu_id, raw_size in vram.items():
                if int(raw_size) != 0:
                    raise ValueError(
                        f"profile.test_stack.runtime_config.kv_base must be zero-contribution for mode={scene_mode}: "
                        f"non-zero vram entry detected: gpu_id={gpu_id!r} size={raw_size}"
                    )
        # TEST_STACK benchmark workloads must be isolated from the ops bed namespace.
        #
        # Isolation model:
        # - cluster_name is fixed to a dedicated benchmark namespace so shared-memory and etcd
        #   roots do not fan out into one top-level directory per suite run.
        # - instance_key is still run_dir scoped to avoid stale member-key collisions across reruns inside the same
        #   benchmark cluster namespace.
        kv_base["instance_key"] = f"{runtime_instance_prefix}__bench_base"
        pre_test_spec_config = _normalize_test_spec_config(
            kv_base.get("test_spec_config"),
            "profile.test_stack.runtime_config.kv_base.test_spec_config",
        )
        pre_legacy_benchmark_fast_path = _normalize_test_spec_config(
            kv_base.get("benchmark_fast_path"),
            "profile.test_stack.runtime_config.kv_base.benchmark_fast_path",
        )
        pre_merged_test_spec_config = _merge_test_spec_config_with_legacy_alias(
            test_spec_config=pre_test_spec_config,
            legacy_benchmark_fast_path=pre_legacy_benchmark_fast_path,
            ctx="profile.test_stack.runtime_config.kv_base",
        )
        resolved_protocol_cfg = _resolve_test_stack_fluxon_protocol_cfg(
            kv_base=kv_base,
            merged_test_spec_config=pre_merged_test_spec_config,
            ctx="profile.test_stack.runtime_config.kv_base",
        )
        if resolved_protocol_cfg is not None:
            kv_base["protocol"] = resolved_protocol_cfg
        fluxonkv_spec = _forbid_removed_fluxon_kv_config_keys(kv_base, "profile.test_stack.runtime_config.kv_base")
        if uses_external_fluxon_kv:
            # External (zero-contribution) mode forbids etcd_addresses and several advanced knobs.
            # External bootstrap derives etcd addresses and routing from owner shared.json.
            forbidden_spec_keys = ("etcd_addresses", "redis_compat", "sub_cluster", "p2p_relay")
            bad = [k for k in forbidden_spec_keys if k in fluxonkv_spec]
            if bad:
                raise ValueError(
                    f"profile.test_stack.runtime_config.kv_base.fluxonkv_spec contains forbidden keys for {scene_mode} "
                    f"(external/zero-contribution mode): {sorted(bad)}"
                )
        else:
            # Dedicated TEST_STACK KV owners run in owner mode and require etcd endpoints.
            # Derive them from deployconf to avoid hardcoding node IPs into suite configs.
            fluxonkv_spec["etcd_addresses"] = _test_stack_etcd_addresses(resolved_case)
            fluxonkv_spec["sub_cluster"] = FLUXON_KV_OWNER_SUB_CLUSTER
        kv_node_patch_template = _require_dict(
            rc.get("kv_node_patch_template"),
            "profile.test_stack.runtime_config.kv_node_patch_template",
        )
        mq_base = rc.get("mq_base")
    elif backend_kind == TEST_STACK_BACKEND_REDIS:
        rc_redis = copy.deepcopy(_require_dict(rc.get("redis"), "profile.test_stack.runtime_config.redis"))
        owner_scale = _require_dict(scale.get("owner"), "resolved_case.scale.owner")
        owner_dram_bytes = _require_int(
            owner_scale.get("owner_dram_bytes"),
            "resolved_case.scale.owner.owner_dram_bytes",
            min_v=16777216,
        )
        assert redis_port_base is not None
        assert redis_port_stride is not None
        redis_port_slot_offset = _test_stack_runner_port_slot(
            runner_root=runner_root,
            stride=redis_port_stride,
        )
        redis_port = int(redis_port_base) + int(redis_port_stride) * int(run_index - 1) + int(redis_port_slot_offset)
        if redis_port <= 0 or redis_port > 65535:
            raise ValueError(f"computed redis_port out of range: {redis_port}")
        redis_instances, kv_base = _build_test_stack_redis_instances(
            resolved_case=resolved_case,
            owner_targets=owner_targets,
            target_ip_map=target_ip_map,
            redis_port=redis_port,
            run_dir=run_dir,
            test_stack_runtime=test_stack_runtime,
            rc_redis=rc_redis,
            owner_dram_bytes=int(owner_dram_bytes),
        )
        kv_base["instance_key"] = f"{runtime_instance_prefix}__bench_base"
    elif backend_kind == TEST_STACK_BACKEND_ALLUXIO:
        rc_alluxio = copy.deepcopy(_require_dict(rc.get("alluxio"), "profile.test_stack.runtime_config.alluxio"))
        alluxio_mount_root_by_target = {
            _require_str(raw_target, "profile.test_stack.runtime_config.alluxio.mount_root_by_target key"): _require_str(
                raw_mount_root,
                "profile.test_stack.runtime_config.alluxio.mount_root_by_target value",
            )
            for raw_target, raw_mount_root in _require_dict(
                rc_alluxio.get("mount_root_by_target"),
                "profile.test_stack.runtime_config.alluxio.mount_root_by_target",
            ).items()
        }
        kv_base = _build_test_stack_alluxio_base(
            runtime_instance_prefix=runtime_instance_prefix,
            rc_alluxio=rc_alluxio,
        )
        alluxio_instances = _build_test_stack_alluxio_instances(
            owner_targets=owner_targets,
            target_ip_map=target_ip_map,
            test_stack_runtime=test_stack_runtime,
            rc_alluxio=rc_alluxio,
        )
        kv_base["instance_key"] = f"{runtime_instance_prefix}__bench_base"
    elif backend_kind == TEST_STACK_BACKEND_MOONCAKE:
        kv_base = copy.deepcopy(_require_dict(rc.get("kv_base"), "profile.test_stack.runtime_config.kv_base"))
        if "fluxonkv_spec" in kv_base:
            raise ValueError(
                "profile.test_stack.runtime_config.kv_base.fluxonkv_spec is invalid for TEST_STACK Mooncake baseline"
            )
        rc_mooncake_master = copy.deepcopy(
            _require_dict(rc.get("master"), "profile.test_stack.runtime_config.master")
        )
        mooncake_spec = _require_dict(
            kv_base.get("mooncake_spec"),
            "profile.test_stack.runtime_config.kv_base.mooncake_spec",
        )
        etcd_endpoints = _test_stack_etcd_addresses(resolved_case)
        if mooncake_spec.get("metadata_server") is not None or mooncake_spec.get("master_server_address") is not None:
            raise ValueError(
                "profile.test_stack.runtime_config.kv_base.mooncake_spec metadata/master endpoints are generated by TEST_STACK"
            )
        kv_base["contribute_to_cluster_pool_size"] = _normalize_test_stack_zero_contribution_pool(
            kv_base.get("contribute_to_cluster_pool_size"),
            "profile.test_stack.runtime_config.kv_base.contribute_to_cluster_pool_size",
        )
        kv_base["backend_kind"] = TEST_STACK_BACKEND_MOONCAKE
        kv_base["instance_key"] = f"{runtime_instance_prefix}__bench_base"
        mooncake_rpc_port_base = _require_int(
            port_alloc.get("mooncake_rpc_port_base"),
            "profile.test_stack.port_alloc.mooncake_rpc_port_base",
            min_v=1,
        )
        mooncake_rpc_port_stride = _require_int(
            port_alloc.get("mooncake_rpc_port_stride"),
            "profile.test_stack.port_alloc.mooncake_rpc_port_stride",
            min_v=1,
        )
        mooncake_metadata_port_base = _require_int(
            port_alloc.get("mooncake_metadata_port_base"),
            "profile.test_stack.port_alloc.mooncake_metadata_port_base",
            min_v=1,
        )
        mooncake_metadata_port_stride = _require_int(
            port_alloc.get("mooncake_metadata_port_stride"),
            "profile.test_stack.port_alloc.mooncake_metadata_port_stride",
            min_v=1,
        )
        mooncake_metrics_port_base = _require_int(
            port_alloc.get("mooncake_metrics_port_base"),
            "profile.test_stack.port_alloc.mooncake_metrics_port_base",
            min_v=1,
        )
        mooncake_metrics_port_stride = _require_int(
            port_alloc.get("mooncake_metrics_port_stride"),
            "profile.test_stack.port_alloc.mooncake_metrics_port_stride",
            min_v=1,
        )
        mooncake_rpc_slot_offset = _test_stack_runner_port_slot(
            runner_root=runner_root,
            stride=mooncake_rpc_port_stride,
        )
        mooncake_metadata_slot_offset = _test_stack_runner_port_slot(
            runner_root=runner_root,
            stride=mooncake_metadata_port_stride,
        )
        mooncake_metrics_slot_offset = _test_stack_runner_port_slot(
            runner_root=runner_root,
            stride=mooncake_metrics_port_stride,
        )
        mooncake_rpc_port = (
            int(mooncake_rpc_port_base)
            + int(mooncake_rpc_port_stride) * int(run_index - 1)
            + int(mooncake_rpc_slot_offset)
        )
        mooncake_metadata_port = (
            int(mooncake_metadata_port_base)
            + int(mooncake_metadata_port_stride) * int(run_index - 1)
            + int(mooncake_metadata_slot_offset)
        )
        mooncake_metrics_port = (
            int(mooncake_metrics_port_base)
            + int(mooncake_metrics_port_stride) * int(run_index - 1)
            + int(mooncake_metrics_slot_offset)
        )
        for field_name, value in (
            ("mooncake_rpc_port", mooncake_rpc_port),
            ("mooncake_metadata_port", mooncake_metadata_port),
            ("mooncake_metrics_port", mooncake_metrics_port),
        ):
            if value <= 0 or value > 65535:
                raise ValueError(f"computed {field_name} out of range: {value}")
        mooncake_host = coord_ip
        mooncake_spec["metadata_server"] = f"http://{mooncake_host}:{int(mooncake_metadata_port)}/metadata"
        mooncake_spec["master_server_address"] = f"{mooncake_host}:{int(mooncake_rpc_port)}"
        mooncake_spec["etcd_addresses"] = list(etcd_endpoints)
        raw_protocol = kv_base.get("protocol")
        if raw_protocol is None:
            kv_base["protocol"] = {"protocol_type": "tcp"}
        else:
            kv_base["protocol"] = copy.deepcopy(
                _require_dict(
                    raw_protocol,
                    "profile.test_stack.runtime_config.kv_base.protocol",
                )
            )
    else:
        raise ValueError(f"unsupported TEST_STACK backend kind: {backend_kind!r}")
    test_spec_config = _normalize_test_spec_config(
        kv_base.get("test_spec_config"),
        "profile.test_stack.runtime_config.kv_base.test_spec_config",
    )
    legacy_benchmark_fast_path = _normalize_test_spec_config(
        kv_base.get("benchmark_fast_path"),
        "profile.test_stack.runtime_config.kv_base.benchmark_fast_path",
    )
    if "test_config" in kv_base:
        raise ValueError(
            "profile.test_stack.runtime_config.kv_base.test_config has been removed; "
            "use test_spec_config.transport_mode instead"
        )
    pprof_duration_seconds = kv_base.get("pprof_duration_seconds")
    if pprof_duration_seconds is not None:
        pprof_duration_seconds = _require_int(
            pprof_duration_seconds,
            "profile.test_stack.runtime_config.kv_base.pprof_duration_seconds",
            min_v=1,
        )
        kv_base["pprof_duration_seconds"] = int(pprof_duration_seconds)
    perf_config = _normalize_test_stack_perf_config(
        kv_base.pop("perf", None),
        "profile.test_stack.runtime_config.kv_base.perf",
    )
    if perf_config is not None and "duration_seconds" not in perf_config:
        perf_config["duration_seconds"] = (
            int(math.ceil(start_idle_f + float(max_secs)))
            + _require_int(perf_config.get("extra_buffer_seconds"), "perf_config.extra_buffer_seconds", min_v=0)
        )
    test_spec_config = _merge_test_spec_config_with_legacy_alias(
        test_spec_config=test_spec_config,
        legacy_benchmark_fast_path=legacy_benchmark_fast_path,
        ctx="profile.test_stack.runtime_config.kv_base",
    )
    runtime_test_spec_config = _test_spec_config_runtime_view(test_spec_config)
    runtime_protocol_cfg = None
    if backend_kind == TEST_STACK_BACKEND_FLUXON:
        runtime_protocol_cfg = kv_base.get("protocol")
        if runtime_protocol_cfg is not None:
            runtime_protocol_cfg = copy.deepcopy(
                _require_dict(
                    runtime_protocol_cfg,
                    "profile.test_stack.runtime_config.kv_base.protocol",
                )
            )
    kv_base.pop("benchmark_fast_path", None)
    kv_base.pop("test_spec_config", None)
    if runtime_test_spec_config:
        kv_base["test_spec_config"] = copy.deepcopy(runtime_test_spec_config)
    if runtime_protocol_cfg is not None:
        kv_base["protocol"] = runtime_protocol_cfg
    # mq_new_or_bind_unique_key is used to derive etcd keys for MPMC channel creation,
    # including a distributed lock key "<unique_id>_lock".
    #
    # If two runners on the same cluster accidentally reuse the same "<case_id, run_index>"
    # (e.g. different workdirs on different machines), they collide on the same etcd lock key
    # and can leave a stale lock behind, causing the next run to stall until READY timeout.
    #
    # Make the key stable for resume within the same run_dir, but unique across workdirs.
    run_dir_nonce = hashlib.sha256(str(run_dir.resolve()).encode("utf-8")).hexdigest()[:12]
    mq_unique_id = f"{runtime_instance_prefix}__run_{run_index}__mpmc__{run_dir_nonce}"

    master_instance: Optional[Dict[str, Any]] = None
    if needs_kv_master:
        assert kv_p2p_port_base is not None
        assert kv_p2p_port_stride is not None
        assert kv_master_port is not None
        services_dir = run_dir / "services"
        services_dir.mkdir(parents=True, exist_ok=True)
        master_services_dir = (services_dir / "master").resolve()
        master_services_dir.mkdir(parents=True, exist_ok=True)
        master_log_dir = (services_dir / "master_logs").resolve()
        master_log_dir.mkdir(parents=True, exist_ok=True)

        etcd_endpoints = _test_stack_etcd_addresses(resolved_case)
        greptime_ip, greptime_port = _test_stack_greptime_host_port(resolved_case)
        greptime_origin = f"http://{greptime_ip}:{int(greptime_port)}"
        master_cfg = {
            "instance_key": f"{runtime_instance_prefix}__kv_master",
            "cluster_name": _require_str(
                _require_dict(runtime.get("stack_identity"), "runtime.stack_identity").get("cluster_name"),
                "runtime.stack_identity.cluster_name",
            ),
            "port": int(kv_master_port),
            "etcd_endpoints": list(etcd_endpoints),
            "network": {
                "subnet_whitelist": _test_stack_master_network_subnet_whitelist(
                    target_ip_map=target_ip_map,
                    machine_targets=machine_targets,
                    coord_target=coord_target,
                    owner_targets=owner_targets,
                )
            },
            "monitoring": {
                "prometheus_base_url": greptime_origin + "/v1/prometheus",
                "prom_remote_write_url": [greptime_origin + "/v1/prometheus/write"],
                "otlp_log_api": {
                    "otlp_endpoint": greptime_origin + "/v1/otlp/v1/logs",
                    "db_name": "public",
                    "table_name": "fluxon_logs",
                },
            },
            "log_dir": str(master_log_dir),
        }
        if pprof_duration_seconds is not None:
            master_cfg["pprof_duration_seconds"] = int(pprof_duration_seconds)
        if runtime_test_spec_config:
            master_cfg["test_spec_config"] = copy.deepcopy(runtime_test_spec_config)
        if runtime_protocol_cfg is not None:
            master_cfg["protocol"] = copy.deepcopy(runtime_protocol_cfg)
        master_cfg_path = cfg_dir / "test_stack_master.yaml"
        if master_cfg_path.exists():
            raise ValueError(f"test_stack master config already exists (no overwrite): {master_cfg_path}")
        _write_yaml_file(master_cfg_path, master_cfg)

        master_instance = copy.deepcopy(coord_tpl)
        master_instance["id"] = "master"
        master_instance["k8s_ref"] = "deployment/test_stack_kv_master"
        master_instance = _require_dict(
            _subst_obj_tokens(master_instance, {"__TARGET__": coord_target}, "test_stack_master_template"),
            "test_stack_master_template.compiled",
        )
        master_deployer = _require_dict(master_instance.get("deployer"), "test_stack_master_template.deployer")
        master_deployer["target"] = coord_target
        master_deployer["command"] = ["/bin/bash", "-lc"]
        master_cmd = _test_stack_runtime_module_command(
            run_dir=run_dir,
            venv_python=_test_stack_target_host_venv_python(
                node_cfg=_require_dict(cluster_nodes.get(coord_target), f"cluster_nodes[{coord_target}]"),
                target_name=coord_target,
                python_abi=(
                    _test_stack_runtime_required_python_abi(
                        resolved_case=resolved_case,
                        run_dir=run_dir,
                    )
                ),
            ),
            module_name="fluxon_py.runtime.start_master",
            module_args=[
                "-c",
                str(master_cfg_path.resolve()),
                "-w",
                str(master_services_dir),
            ],
            runtime_env=runtime_env,
        )
        if perf_config is not None and "master" in _require_list(perf_config.get("targets"), "perf_config.targets"):
            master_cmd = _test_stack_perf_wrapper_command(
                inner_command=master_cmd,
                output_dir=master_log_dir,
                perf_label="kv_master",
                perf_config=perf_config,
            )
        master_deployer["args"] = [master_cmd]
    elif backend_kind == TEST_STACK_BACKEND_MOONCAKE:
        assert rc_mooncake_master is not None
        assert mooncake_rpc_port is not None
        assert mooncake_metadata_port is not None
        assert mooncake_metrics_port is not None
        mooncake_master_log_dir = (run_dir / "services" / "mooncake_master_logs").resolve()
        mooncake_master_log_dir.mkdir(parents=True, exist_ok=True)
        mooncake_cluster_id = (
            _require_str(
                rc_mooncake_master.get("cluster_id_prefix"),
                "profile.test_stack.runtime_config.master.cluster_id_prefix",
            )
            + "--"
            + runtime_instance_prefix
        )
        rpc_address = str(rc_mooncake_master.get("rpc_address", "0.0.0.0")).strip()
        http_metadata_host = str(rc_mooncake_master.get("http_metadata_host", "0.0.0.0")).strip()
        rpc_thread_num_raw = rc_mooncake_master.get("rpc_thread_num")
        extra_args = [
            _require_str(raw_arg, "profile.test_stack.runtime_config.master.extra_args[]")
            for raw_arg in _require_list(
                rc_mooncake_master.get("extra_args", []),
                "profile.test_stack.runtime_config.master.extra_args",
            )
        ]
        mooncake_args = [
            "--enable_http_metadata_server=true",
            f"--http_metadata_server_host={http_metadata_host}",
            f"--http_metadata_server_port={int(mooncake_metadata_port)}",
            f"--rpc_address={rpc_address}",
            f"--rpc_port={int(mooncake_rpc_port)}",
            f"--metrics_port={int(mooncake_metrics_port)}",
            f"--cluster_id={mooncake_cluster_id}",
            "--logtostderr=true",
            f"--log_dir={str(mooncake_master_log_dir)}",
        ]
        if rpc_thread_num_raw is not None:
            mooncake_args.append(
                f"--rpc_thread_num={_require_int(rpc_thread_num_raw, 'profile.test_stack.runtime_config.master.rpc_thread_num', min_v=1)}"
            )
        mooncake_args.extend(extra_args)
        mooncake_master_cmd = _test_stack_runtime_module_command(
            run_dir=run_dir,
            venv_python=_test_stack_target_host_venv_python(
                node_cfg=_require_dict(cluster_nodes.get(coord_target), f"cluster_nodes[{coord_target}]"),
                target_name=coord_target,
                python_abi=(
                    _test_stack_runtime_required_python_abi(
                        resolved_case=resolved_case,
                        run_dir=run_dir,
                    )
                ),
            ),
            module_name="mooncake.cli",
            module_args=mooncake_args,
            runtime_env=runtime_env,
        )
        if perf_config is not None and TEST_STACK_MOONCAKE_MASTER_INSTANCE_ID in _require_list(
            perf_config.get("targets"),
            "perf_config.targets",
        ):
            mooncake_master_cmd = _test_stack_perf_wrapper_command(
                inner_command=mooncake_master_cmd,
                output_dir=mooncake_master_log_dir,
                perf_label="mooncake_master",
                perf_config=perf_config,
            )
        master_instance = copy.deepcopy(coord_tpl)
        master_instance["id"] = TEST_STACK_MOONCAKE_MASTER_INSTANCE_ID
        master_instance["k8s_ref"] = "deployment/test_stack_mooncake_master"
        master_instance = _require_dict(
            _subst_obj_tokens(master_instance, {"__TARGET__": coord_target}, "test_stack_mooncake_master_template"),
            "test_stack_mooncake_master_template.compiled",
        )
        master_deployer = _require_dict(
            master_instance.get("deployer"),
            "test_stack_mooncake_master_template.deployer",
        )
        master_deployer["target"] = coord_target
        master_deployer["command"] = ["/bin/bash", "-lc"]
        master_deployer["args"] = [mooncake_master_cmd]

    rpc_scene_server_source: Optional[str] = None
    rpc_scene_target_role: Optional[str] = None
    if scene_mode == TEST_STACK_MODE_RPC:
        rpc_scene_server_source = _require_str(
            ts_scene.get("rpc_server_source"),
            "scene.test_stack.rpc_server_source",
        )
        rpc_scene_payload_mode_raw = ts_scene.get("rpc_payload_mode")
        if rpc_scene_payload_mode_raw is None:
            rpc_scene_payload_mode_raw = TEST_STACK_RPC_PAYLOAD_MODE_BYTES
        rpc_scene_payload_mode = _require_str(
            rpc_scene_payload_mode_raw,
            "scene.test_stack.rpc_payload_mode",
        ).strip().upper()
        if rpc_scene_server_source == TEST_STACK_RPC_SERVER_SOURCE_BENCHMARK_NODE_ROLE:
            rpc_scene_target_role = canonicalize_kv_node_role(
                _require_str(
                    ts_scene.get("rpc_target_role"),
                    "scene.test_stack.rpc_target_role",
                )
            )
        elif rpc_scene_server_source == TEST_STACK_RPC_SERVER_SOURCE_BENCHMARK_NODE_ALL:
            if ts_scene.get("rpc_target_role") is not None:
                raise ValueError(
                    "scene.test_stack.rpc_target_role is not allowed when "
                    f"scene.test_stack.rpc_server_source={TEST_STACK_RPC_SERVER_SOURCE_BENCHMARK_NODE_ALL}"
                )
        else:
            raise ValueError(
                f"unsupported scene.test_stack.rpc_server_source: {rpc_scene_server_source!r}"
            )

    owner_instances: List[Dict[str, Any]] = []
    if uses_external_fluxon_kv:
        assert kv_p2p_port_base is not None
        assert kv_p2p_port_stride is not None
        owner_instances = _build_test_stack_external_kv_owner_instances(
            scene_mode=scene_mode,
            resolved_case=resolved_case,
            scale=scale,
            runtime=runtime,
            run_dir=run_dir,
            cfg_dir=cfg_dir,
            coord_tpl=coord_tpl,
            test_stack_runtime=test_stack_runtime,
            cluster_nodes=cluster_nodes,
            owner_targets=owner_targets,
            needs_kv_master=needs_kv_master,
            kv_p2p_port_base=kv_p2p_port_base,
            kv_p2p_port_stride=kv_p2p_port_stride,
            kv_p2p_slot_offset=kv_p2p_slot_offset,
            p2p_ports_per_slot=p2p_ports_per_slot,
            node_total=node_total,
            run_index=run_index,
            runtime_instance_prefix=runtime_instance_prefix,
            kv_base=kv_base,
            test_spec_config=runtime_test_spec_config,
            perf_config=perf_config,
            runtime_env=runtime_env,
            owner_group_processes=owner_group_processes,
            owner_cpu_core_by_target=owner_cpu_core_by_target,
        )
    elif backend_kind == TEST_STACK_BACKEND_MOONCAKE and uses_dedicated_kv_owners:
        owner_instances = _build_test_stack_mooncake_owner_instances(
            resolved_case=resolved_case,
            scale=scale,
            run_dir=run_dir,
            cfg_dir=cfg_dir,
            coord_tpl=coord_tpl,
            cluster_nodes=cluster_nodes,
            owner_targets=owner_targets,
            runtime_instance_prefix=runtime_instance_prefix,
            kv_base=kv_base,
            test_spec_config=runtime_test_spec_config,
            perf_config=perf_config,
            runtime_env=runtime_env,
        )

    rpc_server_instance_keys: List[str] = []
    rpc_server_targets: Dict[str, str] = {}
    rpc_server_zero_rpc_ports: Dict[str, int] = {}
    network_sample_targets_seen: set[str] = set()

    for role in roles_order:
        plan = _require_dict(role_plan.get(role), f"scale.role_plan[{role}]")
        cnt = _require_int(plan.get("count"), f"scale.role_plan[{role}].count", min_v=1)
        targets = _require_list(plan.get("targets"), f"scale.role_plan[{role}].targets")
        if len(targets) != cnt:
            raise ValueError(
                f"scale.role_plan[{role}].targets length must equal count: count={cnt} targets={len(targets)}"
            )
        weight = plan.get("weight")
        if scene_mode == TEST_STACK_MODE_MPMC and weight is None:
            raise ValueError(f"scale.role_plan[{role}].weight is required for mode={scene_mode}")
        if weight is not None and not isinstance(weight, (int, float)):
            raise ValueError(f"scale.role_plan[{role}].weight must be number")

        for i in range(cnt):
            target = _require_str(targets[i], f"scale.role_plan[{role}].targets[{i}]")
            if target not in target_ip_map:
                raise ValueError(f"target not found in deploy.target_ip_map: {target}")
            process_count = (
                int(processes_per_target)
                if _test_stack_scene_uses_per_target_process_fanout(scene_mode=scene_mode)
                else 1
            )
            for process_idx in range(process_count):
                base_instance_key = f"{role}_{i}"
                instance_key = (
                    f"{base_instance_key}_proc_{process_idx}"
                    if process_count > 1
                    else base_instance_key
                )
                runtime_instance_key = f"{runtime_instance_prefix}__{instance_key}"
                runtime_role = _test_stack_runtime_role_for_scene_role(
                    scene_mode=scene_mode,
                    role=role,
                )
                if scene_mode == TEST_STACK_MODE_KVSTORE:
                    runtime_role = KV_NODE_ROLE_WORKER

                mapping = {
                    "__INSTANCE_KEY__": instance_key,
                    "__COORDINATOR__": coordinator_addr,
                    "__TARGET__": target,
                }
                inst = _require_dict(_subst_obj_tokens(node_tpl, mapping, "node_template"), "node_template.compiled")
                inst["id"] = instance_key
                inst_deployer = _require_dict(inst.get("deployer"), f"node_template[{instance_key}].deployer")
                inst_deployer["target"] = target
                inst_deployer["command"] = ["/bin/bash", "-lc"]
                inst_deployer["args"] = [
                    _test_stack_runtime_command(
                        run_dir=run_dir,
                        venv_python=_test_stack_target_host_venv_python(
                            node_cfg=_require_dict(cluster_nodes.get(target), f"cluster_nodes[{target}]"),
                            target_name=target,
                            python_abi=(
                                _test_stack_runtime_required_python_abi(
                                    resolved_case=resolved_case,
                                    run_dir=run_dir,
                                )
                            ),
                        ),
                        script_path=test_stack_runtime["node_script"],
                        script_args=[
                            "--instance-key",
                            runtime_instance_key,
                            "--coordinator",
                            coordinator_addr,
                        ],
                        runtime_env=runtime_env,
                    )
                ]
                node_instances.append(inst)

                kv = {"instance_key": runtime_instance_key}
                if backend_kind == TEST_STACK_BACKEND_FLUXON:
                    assert kv_p2p_port_base is not None
                    assert kv_p2p_port_stride is not None
                    kv_patch = _require_dict(
                        _subst_obj_tokens(kv_node_patch_template, mapping, "kv_node_patch_template"),
                        "kv_node_patch_template.compiled",
                    )
                    kv = _deep_merge_dict(kv, kv_patch)
                    node_ordinal = len(node_overrides)
                    kv_p2p_listen_port = (
                        int(kv_p2p_port_base)
                        + int(kv_p2p_port_stride) * int(run_index - 1)
                        + int(kv_p2p_slot_offset) * int(p2p_ports_per_slot)
                        + int(node_ordinal)
                    )
                    if kv_p2p_listen_port <= 0 or kv_p2p_listen_port > 65535:
                        raise ValueError(f"computed kv_p2p_listen_port out of range: {kv_p2p_listen_port}")
                    fluxonkv_override = kv.get("fluxonkv_spec")
                    if fluxonkv_override is None:
                        fluxonkv_override = {}
                    fluxonkv_override = _require_dict(
                        fluxonkv_override,
                        f"node_override[{runtime_instance_key}].fluxonkv_spec",
                    )
                    # Benchmark nodes bootstrap from owner shared bundles. Strict dual-owner mode
                    # routes each process group to a different same-machine owner bundle root.
                    assert stack_cluster_name is not None
                    assert stack_shared_memory_path is not None
                    assert stack_shared_file_path is not None
                    selected_owner_target = _test_stack_owner_target_for_node_process(
                        target=target,
                        process_idx=process_idx,
                        owner_targets_by_machine=owner_targets_by_machine,
                        target_ip_map=target_ip_map,
                        owner_group_processes=owner_group_processes,
                    )
                    if selected_owner_target is None:
                        selected_shared_memory_path = stack_shared_memory_path
                        selected_shared_file_path = stack_shared_file_path
                    else:
                        selected_shared_memory_path, selected_shared_file_path = _owner_bundle_roots_for_target(
                            shared_memory_root=stack_shared_memory_path,
                            shared_file_root=stack_shared_file_path,
                            owner_target=selected_owner_target,
                            ctx=f"strict dual-owner routing target={target} process_idx={process_idx}",
                        )
                    fluxonkv_override["cluster_name"] = stack_cluster_name
                    fluxonkv_override["shared_memory_path"] = selected_shared_memory_path
                    fluxonkv_override["shared_file_path"] = selected_shared_file_path
                    fluxonkv_override["p2p_listen_port"] = int(kv_p2p_listen_port)
                    kv["fluxonkv_spec"] = fluxonkv_override
                elif backend_kind == TEST_STACK_BACKEND_ALLUXIO:
                    if target not in alluxio_mount_root_by_target:
                        raise ValueError(
                            f"alluxio mount_root_by_target missing node target: {target!r}"
                        )
                    kv["alluxio"] = {
                        "mount_root_abs": _require_str(
                            alluxio_mount_root_by_target[target],
                            f"profile.test_stack.runtime_config.alluxio.mount_root_by_target[{target!r}]",
                        )
                    }
                rec: Dict[str, Any] = {"kv": kv}
                if scene_mode == TEST_STACK_MODE_MPMC:
                    rec["mq_role"] = role
                    rec["mq"] = {"weight": float(weight)}
                rec["network_sample"] = {
                    "target": target,
                    "leader": process_idx == 0 and target not in network_sample_targets_seen,
                }
                node_overrides.append(rec)
                if rec["network_sample"]["leader"]:
                    network_sample_targets_seen.add(target)
                node_roles.append(runtime_role)
                if (
                    scene_mode == TEST_STACK_MODE_RPC
                    and (
                        (
                            rpc_scene_server_source == TEST_STACK_RPC_SERVER_SOURCE_BENCHMARK_NODE_ROLE
                            and runtime_role == rpc_scene_target_role
                        )
                        or rpc_scene_server_source == TEST_STACK_RPC_SERVER_SOURCE_BENCHMARK_NODE_ALL
                    )
                ):
                    rpc_server_instance_keys.append(runtime_instance_key)
                    rpc_server_targets[runtime_instance_key] = _require_str(
                        target_ip_map.get(target),
                        f"deploy.target_ip_map[{target!r}]",
                    )
                    zerorpc_port_base_raw = ts_scene.get("zerorpc_port_base")
                    zerorpc_port_stride_raw = ts_scene.get("zerorpc_port_stride")
                    if zerorpc_port_base_raw is not None and zerorpc_port_stride_raw is not None:
                        zerorpc_port = (
                            int(zerorpc_port_base_raw)
                            + int(zerorpc_port_stride_raw) * int(run_index - 1)
                            + int(node_ordinal)
                        )
                        if zerorpc_port <= 0 or zerorpc_port > 65535:
                            raise ValueError(f"computed zerorpc_port out of range: {zerorpc_port}")
                        rpc_server_zero_rpc_ports[runtime_instance_key] = int(zerorpc_port)

    coord_inst = copy.deepcopy(coord_tpl)
    coord_inst["id"] = "coordinator"
    # Coordinator binds to 0.0.0.0 in the inner script; port is controlled via runtime config.
    coord_inst = _require_dict(_subst_obj_tokens(coord_inst, {"__TARGET__": coord_target}, "coordinator_template"), "coordinator_template.compiled")
    coord_deployer = _require_dict(coord_inst.get("deployer"), "coordinator_template.deployer")
    coord_deployer["target"] = coord_target
    coord_deployer["command"] = ["/bin/bash", "-lc"]
    coord_deployer["args"] = [
        _test_stack_runtime_command(
            run_dir=run_dir,
            venv_python=_test_stack_target_host_venv_python(
                node_cfg=_require_dict(cluster_nodes.get(coord_target), f"cluster_nodes[{coord_target}]"),
                target_name=coord_target,
                python_abi=(
                    _test_stack_runtime_required_python_abi(
                        resolved_case=resolved_case,
                        run_dir=run_dir,
                    )
                ),
            ),
            script_path=test_stack_runtime["coordinator_script"],
            script_args=[],
            runtime_env=runtime_env,
        )
    ]

    compiled_instances = (
        [coord_inst]
        + ([] if master_instance is None else [master_instance])
        + owner_instances
        + redis_instances
        + alluxio_instances
        + node_instances
    )
    deploy["instances"] = compiled_instances

    result_path = str((run_dir / "benchmark_result.json").resolve())
    benchmark_out: Dict[str, Any] = {
        "mode": runtime_mode,
        "backend_kind": backend_kind,
        "workload_id": _require_str(ts_scene.get("workload_id"), "scene.test_stack.workload_id"),
        "processes_per_target": processes_per_target,
        "threads_per_process": threads_per_process,
        "max_benchmark_seconds": max_secs,
        # MPMC prewarm-before-READY can legitimately exceed max_benchmark_seconds (e.g. transport backoff),
        # so this must be explicitly configured in the suite YAML for determinism.
        "cluster_ready_timeout_seconds": cluster_ready_timeout_seconds,
        "metric_warmup_seconds": warmup_f,
        "start_idle_seconds": start_idle_f,
        "op_timeout_seconds": op_timeout_f,
        "value_size": value_size,
        "node_roles": node_roles,
        "value_size_list": value_size_list,
    }
    value_size_mode = ts_scene.get("value_size_mode")
    if value_size_mode is None:
        benchmark_out["value_size_mode"] = "FIXED"
    else:
        benchmark_out["value_size_mode"] = _require_str(
            value_size_mode,
            "scene.test_stack.value_size_mode",
        )
        if benchmark_out["value_size_mode"] == "RANDOM_WEIGHTED_SET":
            benchmark_out["value_size_weighted_set"] = copy.deepcopy(
                _require_list(
                    ts_scene.get("value_size_weighted_set"),
                    "scene.test_stack.value_size_weighted_set",
                )
            )
    if scene_mode in (TEST_STACK_MODE_KVSTORE, TEST_STACK_MODE_KVSTORE_WITH_LOCAL_CACHE):
        for optional_key in (
            "read_ratio",
            "write_ratio",
            "request_distribution",
            TEST_STACK_SCENE_KEY_AFFINITY_LOCALITY_RATIO,
        ):
            if optional_key in ts_scene:
                benchmark_out[optional_key] = copy.deepcopy(ts_scene[optional_key])
        if scene_mode == TEST_STACK_MODE_KVSTORE:
            benchmark_out["kv_bootstrap_before_ready"] = True
        if "keyspace_size" in ts_scene:
            benchmark_out["keyspace_size"] = _test_stack_effective_kv_keyspace_size(
                case_id=case_id,
                ts_scene=ts_scene,
                scale=scale,
                bench_value_size=value_size,
            )
        if TEST_STACK_SCENE_KEY_AFFINITY_LOCALITY_RATIO in ts_scene:
            # Affinity locality is defined per benchmark member. Process fanout creates
            # additional benchmark members, and each process must receive its own slot so
            # the full benchmark population spreads its preferred key ranges evenly across
            # the shared scene keyspace.
            benchmark_out[TEST_STACK_BENCHMARK_KEY_AFFINITY_SLOT_COUNT] = sum(
                _require_int(
                    _require_dict(role_plan.get(role), f"scale.role_plan[{role}]").get("count"),
                    f"scale.role_plan[{role}].count",
                    min_v=1,
                )
                * int(processes_per_target)
                for role in roles_order
            )
    elif scene_mode == TEST_STACK_MODE_RPC:
        for optional_key in (
            "read_ratio",
            "write_ratio",
            "request_distribution",
            "keyspace_size",
            "rpc_backend_kind",
            "rpc_path",
            "rpc_payload_size",
            "rpc_payload_mode",
            "rpc_server_source",
            "rpc_target_role",
        ):
            if optional_key in ts_scene:
                benchmark_out[optional_key] = copy.deepcopy(ts_scene[optional_key])
        benchmark_out["rpc_server_instance_keys"] = copy.deepcopy(rpc_server_instance_keys)
        benchmark_out["rpc_server_targets"] = copy.deepcopy(rpc_server_targets)
        if rpc_server_zero_rpc_ports:
            benchmark_out["rpc_server_zero_rpc_ports"] = copy.deepcopy(rpc_server_zero_rpc_ports)
    elif scene_mode == TEST_STACK_MODE_PY_FS:
        benchmark_out["workload_mode"] = scene_mode
        benchmark_out["file_size_bytes"] = int(ts_scene.get("file_size_bytes", value_size))
        if ts_scene.get("chunk_size_bytes") is not None:
            benchmark_out["chunk_size_bytes"] = int(ts_scene["chunk_size_bytes"])
        if ts_scene.get("files_per_worker") is not None:
            benchmark_out["files_per_worker"] = int(ts_scene["files_per_worker"])
        if ts_scene.get("cache_max_bytes") is not None:
            benchmark_out["cache_max_bytes"] = int(ts_scene["cache_max_bytes"])
        benchmark_out["fs_agent_instance_keys"] = [
            f"{runtime_instance_prefix}__agent_{index}"
            for index in range(
                _require_int(
                    _require_dict(role_plan.get("agent"), "scale.role_plan['agent']").get("count"),
                    "scale.role_plan['agent'].count",
                    min_v=1,
                )
            )
        ]

    monitoring_config: Optional[Dict[str, Any]] = None
    raw_monitoring_config = rc.get("monitoring")
    if raw_monitoring_config is not None:
        monitoring_config = copy.deepcopy(
            _require_dict(
                raw_monitoring_config,
                "profile.test_stack.runtime_config.monitoring",
            )
        )

    config_obj: Dict[str, Any] = {
        "benchmark": benchmark_out,
        "kv_base": kv_base,
        "node_overrides": node_overrides,
        "coordinator": {"port": coordinator_port},
        "output": {"result_path": result_path},
    }
    if needs_kv_master:
        if monitoring_config is None:
            monitoring_config = {}
        monitoring_config["prometheus_base_url"] = greptime_origin + "/v1/prometheus"
    if monitoring_config is not None:
        config_obj["monitoring"] = monitoring_config
    if consumer_sim_handle_ms_range is not None:
        config_obj["benchmark"]["consumer_sim_handle_ms_range"] = consumer_sim_handle_ms_range
    if mq_base is not None:
        config_obj["mq_base"] = mq_base
    if scene_mode == TEST_STACK_MODE_MPMC:
        config_obj["mq_new_or_bind_unique_key"] = mq_unique_id

    _write_benchmark_config_py(run_dir, config_obj)
    return {
        "coordinator_addr": coordinator_addr,
        "coordinator_port": coordinator_port,
        "result_path": result_path,
    }


def _stage_run_dir_for_remote_targets(
    resolved_case: Dict[str, Any],
    *,
    run_dir: Path,
    instance_ids: List[str],
    archive_prefix: str,
    stage_prefix: str,
    verify_relpaths: List[str],
    sync_mode: str,
    include_relpaths: Optional[List[str]] = None,
    ctx: str,
) -> None:
    remote_targets = _collect_remote_run_dir_targets(resolved_case, instance_ids=instance_ids)
    if not remote_targets:
        return
    if not verify_relpaths:
        raise ValueError(f"{ctx}.verify_relpaths must be non-empty")

    normalized_verify_relpaths: List[str] = []
    for raw_path in verify_relpaths:
        rel_path = _require_str(raw_path, f"{ctx}.verify_relpaths[]")
        rel_obj = Path(rel_path)
        if rel_obj.is_absolute():
            raise ValueError(f"{ctx}.verify_relpaths must be relative: {rel_path}")
        normalized_verify_relpaths.append(rel_obj.as_posix())

    normalized_include_relpaths: Optional[List[str]] = None
    if include_relpaths is not None:
        if not include_relpaths:
            raise ValueError(f"{ctx}.include_relpaths must be non-empty when provided")
        normalized_include_relpaths = []
        for raw_path in include_relpaths:
            rel_path = _require_str(raw_path, f"{ctx}.include_relpaths[]")
            rel_obj = Path(rel_path)
            if rel_obj.is_absolute():
                raise ValueError(f"{ctx}.include_relpaths must be relative: {rel_path}")
            normalized_include_relpaths.append(rel_obj.as_posix())

    run_dir_abs = run_dir.resolve()
    archive_name = (
        archive_prefix
        + hashlib.sha256(str(run_dir_abs).encode("utf-8")).hexdigest()[:16]
        + ".tar.gz"
    )
    archive_member = run_dir_abs.relative_to(Path("/")).as_posix()

    # Use a suite workdir-scoped staging root instead of the global OS temp dir:
    # - Some dev environments set TMPDIR to tool-managed paths that are not stable or easy to inspect.
    # - Keeping staging artifacts inside the suite workdir makes paths deterministic and
    #   avoids spreading a second "workspace namespace" outside the runner workdir.
    # - The staging root must NOT be under run_dir_abs, otherwise full-run_dir tar could
    #   accidentally include stage artifacts.
    stage_root = (_find_suite_workdir_for_run_dir(run_dir_abs) / "_stage_tmp").resolve()
    stage_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=stage_prefix, dir=str(stage_root)) as td:
        archive_path = (Path(td) / archive_name).resolve()
        with tarfile.open(archive_path, "w:gz") as tf:
            if normalized_include_relpaths is None:
                tf.add(run_dir_abs, arcname=archive_member)
            else:
                tf.add(run_dir_abs, arcname=archive_member, recursive=False)
                for rel_path in normalized_include_relpaths:
                    include_abs = (run_dir_abs / rel_path).resolve()
                    if not include_abs.exists():
                        raise ValueError(f"{ctx}.include_relpaths entry does not exist: {include_abs}")
                    tf.add(include_abs, arcname=f"{archive_member}/{rel_path}")

        _sync_run_dir_archive_via_bastion(
            resolved_case=resolved_case,
            archive_path=archive_path,
            remote_targets=remote_targets,
            run_dir_abs=run_dir_abs,
            verify_relpaths=normalized_verify_relpaths,
            sync_mode=sync_mode,
            ctx=ctx,
        )


def _find_suite_workdir_for_run_dir(run_dir: Path) -> Path:
    cur = run_dir.resolve()
    for _ in range(12):
        if (cur / "case_runs.yaml").exists():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    raise ValueError(f"failed to locate suite workdir for run_dir: run_dir={run_dir}")


def _collect_remote_run_dir_targets(
    resolved_case: Dict[str, Any],
    *,
    instance_ids: List[str],
) -> List[Tuple[str, Dict[str, Any], Any]]:
    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    instances = _require_list(deploy.get("instances"), "resolved_case.deploy.instances")
    selected_ids = set(instance_ids)
    local_ipv4_addrs = _local_ipv4_addresses()
    cluster_nodes, dispatch_mod = _load_test_stack_cluster_nodes_and_dispatch(resolved_case)

    out: List[Tuple[str, Dict[str, Any], Any]] = []
    seen_targets: set[str] = set()
    for raw in instances:
        inst = _require_dict(raw, "deploy.instances[]")
        instance_id = _require_str(inst.get("id"), "deploy.instances[].id")
        if instance_id not in selected_ids:
            continue
        deployer = _require_dict(inst.get("deployer"), f"deploy.instances[{instance_id!r}].deployer")
        target = _require_str(deployer.get("target"), f"deploy.instances[{instance_id!r}].deployer.target")
        if target in seen_targets:
            continue
        seen_targets.add(target)
        node_cfg = cluster_nodes.get(target)
        if node_cfg is None:
            raise ValueError(f"TEST_STACK target is missing from self-host cluster_nodes: {target}")
        node_cfg = _require_dict(node_cfg, f"cluster_nodes[{target}]")
        if _cluster_node_is_local_host(node_cfg, target_name=target, local_ipv4_addrs=local_ipv4_addrs):
            continue
        out.append((target, node_cfg, dispatch_mod))
    return out


def _collect_instance_target_accesses(
    resolved_case: Dict[str, Any],
    *,
    instance_ids: List[str],
) -> List[Tuple[str, Dict[str, Any], Any, bool]]:
    selected_ids = set(instance_ids)
    if not selected_ids:
        raise ValueError("instance target access collection requires non-empty instance_ids")
    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    instances = _require_list(deploy.get("instances"), "resolved_case.deploy.instances")
    local_ipv4_addrs = _local_ipv4_addresses()
    cluster_nodes, dispatch_mod = _load_test_stack_cluster_nodes_and_dispatch(resolved_case)

    out: List[Tuple[str, Dict[str, Any], Any, bool]] = []
    seen_targets: set[str] = set()
    for raw in instances:
        inst = _require_dict(raw, "deploy.instances[]")
        instance_id = _require_str(inst.get("id"), "deploy.instances[].id")
        if instance_id not in selected_ids:
            continue
        deployer = _require_dict(inst.get("deployer"), f"deploy.instances[{instance_id!r}].deployer")
        target = _require_str(deployer.get("target"), f"deploy.instances[{instance_id!r}].deployer.target")
        if target in seen_targets:
            continue
        seen_targets.add(target)
        node_cfg = cluster_nodes.get(target)
        if node_cfg is None:
            raise ValueError(f"TEST_STACK target is missing from self-host cluster_nodes: {target}")
        node_cfg = _require_dict(node_cfg, f"cluster_nodes[{target}]")
        out.append(
            (
                target,
                node_cfg,
                dispatch_mod,
                _cluster_node_is_local_host(node_cfg, target_name=target, local_ipv4_addrs=local_ipv4_addrs),
            )
        )
    return out


def _local_ipv4_addresses() -> set[str]:
    out = {"127.0.0.1"}
    raw = subprocess.check_output(["bash", "-lc", "hostname -I"], text=True).strip()
    addrs = [part.strip() for part in raw.split() if part.strip()]
    if not addrs:
        raise ValueError("hostname -I returned no IPv4 addresses")
    out.update(addrs)
    return out


def _cluster_node_is_local_host(
    node_cfg: Dict[str, Any],
    *,
    target_name: str,
    local_ipv4_addrs: set[str],
) -> bool:
    node_ip = _require_str(node_cfg.get("ip"), f"cluster_nodes[{target_name}].ip")
    if node_ip in local_ipv4_addrs:
        return True
    ssh_host = str(node_cfg.get("ssh_host") or "").strip()
    if ssh_host and ssh_host in local_ipv4_addrs:
        return True
    return False


def _ensure_path_symlink(*, link_path: Path, target_path: Path) -> None:
    link_path = link_path.resolve(strict=False)
    target_path = target_path.resolve()
    if link_path.is_symlink():
        if link_path.resolve() == target_path:
            return
        link_path.unlink()
    elif link_path.exists():
        if link_path.is_dir():
            shutil.rmtree(link_path)
        else:
            link_path.unlink()
    os.symlink(str(target_path), str(link_path), target_is_directory=target_path.is_dir())


def _release_manifest_relpaths(manifest_path: Path) -> List[str]:
    lines = manifest_path.read_text(encoding="utf-8").splitlines()
    out: List[str] = []
    for index, raw in enumerate(lines, start=1):
        line = raw.strip()
        if not line:
            continue
        _, relpath = _parse_sha256_manifest_line(raw, index=index)
        out.append(relpath)
    if not out:
        raise ValueError(f"release manifest is empty: {manifest_path}")
    return out


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _write_release_manifest(*, release_root: Path, relpaths: List[str]) -> None:
    lines = []
    for relpath in relpaths:
        file_path = release_root / relpath
        if not file_path.exists():
            raise ValueError(f"release manifest relpath is missing after assembly: {file_path}")
        lines.append(f"{_sha256_file(file_path)}  {relpath}")
    (release_root / "fluxon_release.sha256").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _load_test_stack_cluster_nodes_and_dispatch(resolved_case: Dict[str, Any]) -> Tuple[Dict[str, Dict[str, Any]], Any]:
    start_test_bed_mod = _load_test_stack_start_test_bed_module(resolved_case)
    deployconf_path = _load_test_bed_deployconf_path()
    deployconf = _require_dict(_load_yaml_file(deployconf_path), f"deployconf {deployconf_path}")
    cluster_nodes = start_test_bed_mod._parse_cluster_nodes(deployconf)
    deploy = resolved_case.get("deploy")
    if isinstance(deploy, dict):
        target_ip_map = deploy.get("target_ip_map")
        if target_ip_map is not None:
            target_ip_map_d = _require_dict(target_ip_map, "resolved_case.deploy.target_ip_map")
            cluster_nodes_by_ip: Dict[str, List[Dict[str, Any]]] = {}
            for hostname, raw_node_cfg in cluster_nodes.items():
                node_cfg = _require_dict(raw_node_cfg, f"cluster_nodes[{hostname}]")
                node_ip = _require_str(node_cfg.get("ip"), f"cluster_nodes[{hostname}].ip")
                cluster_nodes_by_ip.setdefault(node_ip, []).append(node_cfg)
            cluster_nodes = dict(cluster_nodes)
            for raw_target, raw_ip in target_ip_map_d.items():
                target = _require_str(raw_target, "resolved_case.deploy.target_ip_map key")
                if target in cluster_nodes:
                    continue
                node_ip = _require_str(raw_ip, f"resolved_case.deploy.target_ip_map[{target!r}]")
                matches = cluster_nodes_by_ip.get(node_ip, [])
                if not matches:
                    continue
                if len(matches) > 1:
                    raise ValueError(
                        "resolved_case.deploy.target_ip_map alias expansion is ambiguous: "
                        f"target={target!r} ip={node_ip!r} matches={len(matches)}"
                    )
                cluster_nodes[target] = copy.deepcopy(matches[0])
    return cluster_nodes, start_test_bed_mod.manual_dispatch_release


def _test_bed_cluster_nodes_from_bundle() -> Dict[str, Dict[str, Any]]:
    start_test_bed_mod = _load_test_stack_start_test_bed_module({})
    deployconf_path = _load_test_bed_deployconf_path()
    deployconf = _require_dict(_load_yaml_file(deployconf_path), f"deployconf {deployconf_path}")
    return start_test_bed_mod._parse_cluster_nodes(deployconf)


def _expected_test_bed_selection_supervisor_text() -> Tuple[str, Path]:
    upstream_gen_bare_script = (_runner_repo_root() / "deployment" / "gen_bare_deploy_bash.py").resolve()
    if not upstream_gen_bare_script.exists():
        raise ValueError(
            f"test bed selection supervisor authority is missing: {upstream_gen_bare_script}"
        )
    upstream_deployment_dir = str(upstream_gen_bare_script.parent.resolve())
    added_sys_path = False
    if upstream_deployment_dir not in sys.path:
        sys.path.insert(0, upstream_deployment_dir)
        added_sys_path = True
    try:
        gen_bare_module = _load_python_module(
            upstream_gen_bare_script,
            module_name="fluxon_testbed_gen_bare_deploy_bash_selection_supervisor",
        )
    finally:
        if added_sys_path and sys.path and sys.path[0] == upstream_deployment_dir:
            sys.path.pop(0)
    render_fn = getattr(gen_bare_module, "render_python_selection_supervisor_module", None)
    stop_timeouts = getattr(gen_bare_module, "STOP_TIMEOUTS", None)
    if render_fn is None or stop_timeouts is None:
        raise ValueError(
            "upstream gen_bare_deploy_bash.py is missing selection supervisor render authority: "
            f"upstream={upstream_gen_bare_script}"
        )
    text = render_fn(timeouts=stop_timeouts)
    if not isinstance(text, str) or not text.strip():
        raise ValueError(
            "upstream gen_bare_deploy_bash.py rendered an empty selection supervisor module: "
            f"upstream={upstream_gen_bare_script}"
        )
    return text, upstream_gen_bare_script


def _verify_active_test_bed_selection_supervisor_matches_bundle() -> None:
    global _ACTIVE_TEST_BED_SELECTION_SUPERVISOR_CHECK_CACHE_KEY
    manifest_info = _load_test_bed_manifest_opt()
    if manifest_info is None:
        return
    manifest_path, _ = manifest_info
    expected_text, upstream_gen_bare_script = _expected_test_bed_selection_supervisor_text()
    expected_sha256 = _sha256_text(expected_text)
    cache_key = f"{manifest_path}:{expected_sha256}"
    if _ACTIVE_TEST_BED_SELECTION_SUPERVISOR_CHECK_CACHE_KEY == cache_key:
        return
    cluster_nodes = _test_bed_cluster_nodes_from_bundle()
    mismatches: List[str] = []
    for node_name, raw_node_cfg in cluster_nodes.items():
        node_cfg = _require_dict(raw_node_cfg, f"cluster_nodes[{node_name}]")
        hostworkdir = _require_str(node_cfg.get("hostworkdir"), f"cluster_nodes[{node_name}].hostworkdir")
        remote_path = str(Path(hostworkdir) / "gen_bare_deploy_bash" / "selection_supervisor.py")
        remote_cmd = "bash -lc " + _shell_quote(
            "python3 - <<'PY'\n"
            "from pathlib import Path\n"
            "import hashlib\n"
            f"path = Path({remote_path!r})\n"
            "if not path.is_file():\n"
            "    print('__MISSING__')\n"
            "else:\n"
            "    print(hashlib.sha256(path.read_bytes()).hexdigest())\n"
            "PY"
        )
        remote_sha256 = _run_remote_bash_capture(
            target_name=node_name,
            node_cfg=node_cfg,
            remote_cmd=remote_cmd,
        ).strip()
        if remote_sha256 == "__MISSING__":
            mismatches.append(f"{node_name}:missing:{remote_path}")
            continue
        if remote_sha256 != expected_sha256:
            mismatches.append(f"{node_name}:{remote_sha256}:{remote_path}")
    if mismatches:
        mismatch_text = "; ".join(mismatches)
        raise ValueError(
            "active test bed selection_supervisor.py does not match the current bundle/codegen authority; "
            f"manifest={manifest_path} expected_sha256={expected_sha256} upstream_gen_bare_script={upstream_gen_bare_script} "
            f"mismatches=[{mismatch_text}]. Re-run test bed dispatch/start so hostworkdir/gen_bare_deploy_bash is refreshed "
            "before launching benchmark workloads."
        )
    _ACTIVE_TEST_BED_SELECTION_SUPERVISOR_CHECK_CACHE_KEY = cache_key


def _load_test_stack_start_test_bed_module(resolved_case: Dict[str, Any]) -> Any:
    _ = resolved_case
    module_path = _runner_repo_root() / "fluxon_test_stack" / "start_test_bed.py"
    return _load_python_module(module_path, module_name="fluxon_test_stack_start_test_bed")


def _test_stack_etcd_addresses(resolved_case: Dict[str, Any]) -> List[str]:
    """Infer etcd endpoints for TEST_STACK KV owner mode from the self-host deployconf.

    English note:
    - TEST_STACK dedicated KV owners create KVCache stores in "owner" mode.
    - Owner mode requires fluxonkv_spec.etcd_addresses, otherwise the dedicated owners crash-loop on startup.
    - We derive etcd placement from the test bed deployconf to keep suite configs clean
      (no hardcoded host IPs in ci_test_list.yaml) while remaining deterministic.
    """
    deployconf_path = _load_test_bed_deployconf_path()
    deployconf = _require_dict(_load_yaml_file(deployconf_path), f"deployconf {deployconf_path}")
    services = _require_dict(deployconf.get("service"), "deployconf.service")
    etcd = _require_dict(services.get("etcd"), "deployconf.service.etcd")
    port = _require_int(etcd.get("port"), "deployconf.service.etcd.port", min_v=1)
    node_bind = _require_dict(etcd.get("node_bind"), "deployconf.service.etcd.node_bind")
    nodes = _require_list(node_bind.get("node"), "deployconf.service.etcd.node_bind.node")
    if not nodes:
        raise ValueError("deployconf.service.etcd.node_bind.node must be non-empty")

    cluster_nodes, _ = _load_test_stack_cluster_nodes_and_dispatch(resolved_case)
    out: List[str] = []
    for raw in nodes:
        hostname = _require_str(raw, "deployconf.service.etcd.node_bind.node[]")
        node_cfg = _require_dict(cluster_nodes.get(hostname), f"cluster_nodes[{hostname}]")
        ip = _require_str(node_cfg.get("ip"), f"cluster_nodes[{hostname}].ip")
        out.append(f"{ip}:{port}")
    return out


def _active_test_stack_target_ip_map(*, ctx: str) -> Dict[str, str]:
    deployconf_path = _load_test_bed_deployconf_path()
    deployconf = _require_dict(_load_yaml_file(deployconf_path), f"deployconf {deployconf_path}")
    raw_nodes = _require_list(deployconf.get("cluster_nodes"), "deployconf.cluster_nodes")
    out: Dict[str, str] = {}
    for idx, raw_node in enumerate(raw_nodes):
        node = _require_dict(raw_node, f"deployconf.cluster_nodes[{idx}]")
        hostname = _require_str(node.get("hostname"), f"deployconf.cluster_nodes[{idx}].hostname")
        ip = _require_str(node.get("ip"), f"deployconf.cluster_nodes[{idx}].ip")
        if hostname in out:
            raise ValueError(f"{ctx} duplicate active TEST_STACK target hostname: {hostname!r}")
        out[hostname] = ip
    if not out:
        raise ValueError(f"{ctx} active TEST_STACK deployconf has no cluster nodes")
    return out


def _test_stack_greptime_host_port(resolved_case: Dict[str, Any]) -> Tuple[str, int]:
    """Infer Greptime host:port for TEST_STACK master monitoring config from the self-host deployconf."""
    deployconf_path = _load_test_bed_deployconf_path()
    deployconf = _require_dict(_load_yaml_file(deployconf_path), f"deployconf {deployconf_path}")
    services = _require_dict(deployconf.get("service"), "deployconf.service")
    greptime = _require_dict(services.get("greptime"), "deployconf.service.greptime")
    port = _require_int(greptime.get("port"), "deployconf.service.greptime.port", min_v=1)
    node_bind = _require_dict(greptime.get("node_bind"), "deployconf.service.greptime.node_bind")
    nodes = _require_list(node_bind.get("node"), "deployconf.service.greptime.node_bind.node")
    if not nodes:
        raise ValueError("deployconf.service.greptime.node_bind.node must be non-empty")
    hostname = _require_str(nodes[0], "deployconf.service.greptime.node_bind.node[0]")
    cluster_nodes, _ = _load_test_stack_cluster_nodes_and_dispatch(resolved_case)
    node_cfg = _require_dict(cluster_nodes.get(hostname), f"cluster_nodes[{hostname}]")
    ip = _require_str(node_cfg.get("ip"), f"cluster_nodes[{hostname}].ip")
    return ip, int(port)


def _test_stack_master_network_subnet_whitelist(
    *,
    target_ip_map: Dict[str, Any],
    machine_targets: List[str],
    coord_target: str,
    owner_targets: List[str],
) -> List[str]:
    """Return Fluxon master whitelist CIDRs derived from the participating TEST_STACK targets."""
    ordered_targets: List[str] = []
    seen_targets: set[str] = set()
    for raw_target in [coord_target, *machine_targets, *owner_targets]:
        target = _require_str(raw_target, "_test_stack_master_network_subnet_whitelist.target")
        if target in seen_targets:
            continue
        seen_targets.add(target)
        ordered_targets.append(target)

    whitelist: List[str] = []
    seen_cidrs: set[str] = set()
    for target in ordered_targets:
        ip = _require_str(target_ip_map.get(target), f"deploy.target_ip_map[{target!r}]")
        octets = ip.split(".")
        if len(octets) != 4:
            raise ValueError(
                "_test_stack_master_network_subnet_whitelist only supports IPv4 target IPs: "
                f"target={target!r} ip={ip!r}"
            )
        cidr = f"{octets[0]}.{octets[1]}.0.0/16"
        if cidr in seen_cidrs:
            continue
        seen_cidrs.add(cidr)
        whitelist.append(cidr)

    if not whitelist:
        raise ValueError("_test_stack_master_network_subnet_whitelist requires at least one participating target")
    return whitelist


def _resolved_profile_cmd(
    resolved_case: Dict[str, Any],
    *,
    field_name: str,
    argv: List[Any],
) -> List[str]:
    resolved_argv = [_subst_runtime_tokens(resolved_case, _require_str(raw, f"{field_name}[]")) for raw in argv]
    if not resolved_argv:
        raise ValueError(f"{field_name} must be non-empty")
    cmd0 = _require_str(resolved_argv[0], f"{field_name}[0]")
    argv0 = _resolve_profile_entrypoint_token(cmd0, field_name=field_name)
    if _is_python_entrypoint_token(argv0):
        out = [argv0] + [str(x) for x in resolved_argv[1:]]
        if len(out) >= 2:
            out[1] = _resolve_profile_entrypoint_token(out[1], field_name=field_name)
        return out
    if _is_python_script_entrypoint(argv0):
        return [sys.executable, argv0] + [str(x) for x in resolved_argv[1:]]
    return [argv0] + [str(x) for x in resolved_argv[1:]]


def _profile_cmd_entrypoint_root(field_name: str) -> Path:
    if field_name == "deploy.adapter_cmd":
        return _runner_test_stack_root()
    raise ValueError(f"unsupported profile command field: {field_name}")


def _resolve_profile_entrypoint_token(token: str, *, field_name: str) -> str:
    if os.path.isabs(token):
        return token
    helper_path = RUNNER_HELPER_ENTRYPOINTS.get(Path(token).name)
    if helper_path is not None:
        return str(helper_path)
    if token.endswith(".py"):
        raise ValueError(
            f"{field_name} uses unsupported python helper entrypoint: {token!r}; "
            f"allowed={sorted(RUNNER_HELPER_ENTRYPOINTS.keys())}"
        )
    if "/" in token:
        raise ValueError(f"{field_name} uses unsupported relative entrypoint token: {token!r}")
    if token:
        return token
    raise ValueError(f"{field_name} contains an empty entrypoint token")


def _is_python_entrypoint_token(token: str) -> bool:
    token_name = Path(token).name
    return token_name in ("python", "python3")


def _is_python_script_entrypoint(token: str) -> bool:
    return Path(token).suffix == ".py"


def _load_python_module(module_path: Path, *, module_name: str) -> Any:
    module_path = module_path.resolve()
    cache_key = f"{module_name}:{module_path}"
    cached = _LOADED_PY_MODULES.get(cache_key)
    if cached is not None:
        return cached
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ValueError(f"cannot load python module from path: {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _LOADED_PY_MODULES[cache_key] = module
    return module


def _deploy_result_history_id(deploy_result: Dict[str, Any], *, ctx: str) -> str:
    history_id = _require_str(deploy_result.get("history_id"), f"{ctx}.history_id")
    return history_id


def _record_ci_apply_id(
    ci_attempted_instance_ids: List[str],
    ci_apply_ids: Dict[str, str],
    *,
    instance_id: str,
    deploy_result: Dict[str, Any],
    ctx: str,
) -> None:
    if instance_id not in CI_RUNTIME_INSTANCE_IDS:
        raise ValueError(f"{ctx} unsupported CI instance_id: {instance_id}")
    ci_attempted_instance_ids.append(instance_id)
    ci_apply_ids[instance_id] = _deploy_result_history_id(deploy_result, ctx=ctx)


def _delete_apply_id(resolved_case: Dict[str, Any], *, apply_id: str, ctx: str) -> None:
    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    controller_url = _require_str(deploy.get("controller_url"), "deploy.controller_url").rstrip("/")
    _ops_delete_apply_id(controller_url, apply_id=apply_id, ctx=ctx)


def _ops_delete_apply_id(controller_url: str, *, apply_id: str, ctx: str) -> None:
    deadline = time.time() + 300.0
    while True:
        delete_req = _new_controller_request(
            controller_url + "/api/delete_apply",
            method="POST",
            data=json.dumps({"apply_id": apply_id}).encode("utf-8"),
            content_type="application/json",
        )
        status_code, resp = _http_json_allow_error_status(delete_req)
        if status_code == 200:
            break
        if status_code == 409:
            if time.time() >= deadline:
                raise ValueError(f"{ctx} delete_apply timed out waiting for deploy guard: apply_id={apply_id} resp={resp}")
            time.sleep(1.0)
            continue
        raise ValueError(f"{ctx} delete_apply failed: apply_id={apply_id} status={status_code} resp={resp}")

    while True:
        req = _new_controller_request(
            controller_url + "/api/wait_delete_apply",
            method="POST",
            data=json.dumps({"apply_id": apply_id}).encode("utf-8"),
            content_type="application/json",
        )
        status_code, resp = _http_json_allow_error_status_allow_empty_success(req)
        if status_code == 200:
            return
        if status_code == 404 and _resp_contains_any_text(resp, ("not found",)):
            return
        if status_code == 409:
            if _resp_contains_any_text(resp, (_WAIT_DELETE_APPLY_REQUIRES_DELETE_ERR,)):
                raise ValueError(f"{ctx} wait_delete_apply called before delete_apply converged: apply_id={apply_id} resp={resp}")
            if time.time() >= deadline:
                raise ValueError(f"{ctx} wait_delete_apply timed out waiting for deploy guard: apply_id={apply_id} resp={resp}")
            time.sleep(1.0)
            continue
        if _delete_apply_should_retry(status_code=status_code, resp=resp):
            if time.time() >= deadline:
                raise ValueError(f"{ctx} wait_delete_apply timed out waiting for workload stop: apply_id={apply_id} resp={resp}")
            print(
                f"[{ctx}] wait_delete_apply stop still converging; retrying: apply_id={apply_id} resp={resp}",
                flush=True,
            )
            time.sleep(1.0)
            continue
        raise ValueError(f"{ctx} wait_delete_apply failed: apply_id={apply_id} status={status_code} resp={resp}")


def _delete_apply_should_retry(*, status_code: int, resp: Dict[str, Any]) -> bool:
    if status_code not in (500, 502):
        return False
    return _resp_contains_any_text(resp, _DELETE_APPLY_RETRYABLE_ERRS)


def _resp_contains_any_text(obj: Any, needles: Tuple[str, ...]) -> bool:
    if isinstance(obj, str):
        return any(needle in obj for needle in needles)
    if isinstance(obj, dict):
        return any(_resp_contains_any_text(v, needles) for v in obj.values())
    if isinstance(obj, list):
        return any(_resp_contains_any_text(v, needles) for v in obj)
    return False


def _validate_test_stack_benchmark_result(result_obj: Dict[str, Any], *, case_id: str) -> None:
    runs = _require_list(result_obj.get("runs"), "benchmark_result.runs")
    if not runs:
        raise ValueError(f"TEST_STACK benchmark produced no runs: case_id={case_id}")

    invalid_run_details: List[str] = []
    for idx, raw in enumerate(runs):
        run = _require_dict(raw, f"benchmark_result.runs[{idx}]")
        completed_raw = run.get("completed", True)
        if not isinstance(completed_raw, bool):
            raise ValueError(
                f"benchmark_result.runs[{idx}].completed must be bool when present: case_id={case_id}"
            )
        completion = _require_dict(
            run.get("completion"),
            f"benchmark_result.runs[{idx}].completion",
        )
        completion_status = _require_str(
            completion.get("status"),
            f"benchmark_result.runs[{idx}].completion.status",
        )
        expected_nodes = _require_int(
            completion.get("expected_nodes"),
            f"benchmark_result.runs[{idx}].completion.expected_nodes",
            min_v=1,
        )
        registered_node_count = _require_int(
            completion.get("registered_node_count"),
            f"benchmark_result.runs[{idx}].completion.registered_node_count",
            min_v=0,
        )
        ready_node_count = _require_int(
            completion.get("ready_node_count"),
            f"benchmark_result.runs[{idx}].completion.ready_node_count",
            min_v=0,
        )
        reported_result_node_count = _require_int(
            completion.get("reported_result_node_count"),
            f"benchmark_result.runs[{idx}].completion.reported_result_node_count",
            min_v=0,
        )
        pending_result_node_count = _require_int(
            completion.get("pending_result_node_count"),
            f"benchmark_result.runs[{idx}].completion.pending_result_node_count",
            min_v=0,
        )
        reported_result_node_ids = _require_list(
            completion.get("reported_result_node_ids"),
            f"benchmark_result.runs[{idx}].completion.reported_result_node_ids",
        )
        pending_result_node_ids = _require_list(
            completion.get("pending_result_node_ids"),
            f"benchmark_result.runs[{idx}].completion.pending_result_node_ids",
        )
        total_ops = _require_int(run.get("total_ops"), f"benchmark_result.runs[{idx}].total_ops", min_v=0)
        total_successful_ops = _require_int(
            run.get("total_successful_ops"),
            f"benchmark_result.runs[{idx}].total_successful_ops",
            min_v=0,
        )
        total_failed_ops = _require_int(
            run.get("total_failed_ops"),
            f"benchmark_result.runs[{idx}].total_failed_ops",
            min_v=0,
        )
        completion_error_raw = completion.get("completion_error")
        completion_error = completion_error_raw if isinstance(completion_error_raw, str) else "unknown"
        if (
            not completed_raw
            or completion_status != TEST_STACK_COMPLETION_STATUS_SUCCESS
            or registered_node_count != expected_nodes
            or ready_node_count != expected_nodes
            or reported_result_node_count != expected_nodes
            or pending_result_node_count != 0
            or len(reported_result_node_ids) != reported_result_node_count
            or len(pending_result_node_ids) != pending_result_node_count
        ):
            invalid_run_details.append(
                "run[{idx}] completion_status={completion_status} completion_error={completion_error} "
                "expected_nodes={expected_nodes} registered_node_count={registered_node_count} "
                "ready_node_count={ready_node_count} reported_result_node_count={reported_result_node_count} "
                "pending_result_node_count={pending_result_node_count} total_ops={total_ops} "
                "total_successful_ops={total_successful_ops} total_failed_ops={total_failed_ops}".format(
                    idx=idx,
                    completion_status=completion_status,
                    completion_error=completion_error,
                    expected_nodes=expected_nodes,
                    registered_node_count=registered_node_count,
                    ready_node_count=ready_node_count,
                    reported_result_node_count=reported_result_node_count,
                    pending_result_node_count=pending_result_node_count,
                    total_ops=total_ops,
                    total_successful_ops=total_successful_ops,
                    total_failed_ops=total_failed_ops,
                )
            )
            continue
        if total_ops <= 0 or total_successful_ops <= 0:
            invalid_run_details.append(
                f"run[{idx}] total_ops={total_ops} total_successful_ops={total_successful_ops} total_failed_ops={total_failed_ops}"
            )

    if invalid_run_details:
        raise ValueError(
            "TEST_STACK benchmark result has no successful operations in one or more runs: "
            f"case_id={case_id} details={'; '.join(invalid_run_details)}"
        )


def _cleanup_previous_failed_ci_runtime(
    resolved_case: Dict[str, Any],
    *,
    run_dir: Path,
    run_index: int,
) -> None:
    case_results_dir = run_dir.parent
    previous_run_dirs: list[Path] = []
    for candidate in case_results_dir.glob("run_*"):
        suffix = candidate.name.removeprefix("run_")
        if not suffix.isdigit():
            continue
        candidate_index = int(suffix)
        if candidate_index >= run_index:
            continue
        previous_run_dirs.append(candidate)
    previous_run_dirs.sort(key=lambda path: int(path.name.removeprefix("run_")), reverse=True)

    for previous_run_dir in previous_run_dirs:
        previous_cleanup_case = _load_previous_ci_cleanup_case(
            previous_run_dir,
            fallback_resolved_case=resolved_case,
        )
        preserved_path = previous_run_dir / CI_PRESERVED_APPLY_IDS_FILENAME
        deleted_apply_ids: set[str] = set()
        for entry in _ci_runtime_current_apply_ids(previous_cleanup_case):
            apply_id = _require_str(entry.get("apply_id"), "current_apply_entry.apply_id")
            instance_ids = _require_list(entry.get("instance_ids"), "current_apply_entry.instance_ids")
            instance_id_text = ",".join(
                _require_str(raw_instance_id, "current_apply_entry.instance_ids[]")
                for raw_instance_id in instance_ids
            )
            print(
                f"[CI cleanup_previous_failed_runtime] current deployment match run_dir={previous_run_dir} instance_ids={instance_id_text} apply_id={apply_id}",
                flush=True,
            )
            _delete_apply_id(
                previous_cleanup_case,
                apply_id=apply_id,
                ctx=f"CI cleanup previous failed runtime {previous_run_dir.name} current_deployments {instance_id_text}",
            )
            deleted_apply_ids.add(apply_id)
        if not preserved_path.exists():
            continue
        raw_payload = _load_yaml_file_if_present(
            preserved_path,
            ctx=f"CI cleanup previous failed runtime {previous_run_dir.name}",
        )
        if raw_payload is None:
            continue
        payload = _require_dict(raw_payload, f"{preserved_path}")
        schema_version = _require_int(
            payload.get("schema_version"),
            f"{preserved_path}.schema_version",
            min_v=1,
        )
        if schema_version != CI_PRESERVED_APPLY_IDS_SCHEMA_VERSION:
            raise ValueError(f"unsupported preserved apply schema_version: {preserved_path} schema_version={schema_version}")
        raw_apply_ids = _require_list(payload.get("apply_ids"), f"{preserved_path}.apply_ids")
        for index, raw in enumerate(raw_apply_ids):
            entry = _require_dict(raw, f"{preserved_path}.apply_ids[{index}]")
            instance_id = _require_str(entry.get("instance_id"), f"{preserved_path}.apply_ids[{index}].instance_id")
            apply_id = _require_str(entry.get("apply_id"), f"{preserved_path}.apply_ids[{index}].apply_id")
            if apply_id in deleted_apply_ids:
                continue
            print(
                f"[CI cleanup_previous_failed_runtime] run_dir={previous_run_dir} instance_id={instance_id} apply_id={apply_id}",
                flush=True,
            )
            _delete_apply_id(
                previous_cleanup_case,
                apply_id=apply_id,
                ctx=f"CI cleanup previous failed runtime {previous_run_dir.name} {instance_id}",
            )
        preserved_path.unlink(missing_ok=True)


def _ops_kind_from_k8s_ref(k8s_ref: str, *, ctx: str) -> Tuple[str, str]:
    if "/" not in k8s_ref:
        raise ValueError(f"{ctx} must be <kind>/<name>, got: {k8s_ref!r}")
    kind, name = k8s_ref.split("/", 1)
    if kind == K8S_REF_KIND_DEPLOYMENT:
        return OPS_WORKLOAD_KIND_DEPLOYMENT, _require_str(name, f"{ctx}.name")
    if kind == K8S_REF_KIND_DAEMONSET:
        return OPS_WORKLOAD_KIND_DAEMONSET, _require_str(name, f"{ctx}.name")
    raise ValueError(f"{ctx} has unsupported kind: {k8s_ref!r}")


def _ci_cleanup_runtime(
    resolved_case: Dict[str, Any],
    *,
    timeout_s: int,
) -> None:
    cleanup_case = _ci_runtime_cleanup_case(resolved_case, ctx="CI cleanup runtime")
    for entry in _ci_runtime_current_apply_ids(cleanup_case):
        apply_id = _require_str(entry.get("apply_id"), "current_apply_entry.apply_id")
        instance_ids = _require_list(entry.get("instance_ids"), "current_apply_entry.instance_ids")
        instance_id_text = ",".join(
            _require_str(raw_instance_id, "current_apply_entry.instance_ids[]")
            for raw_instance_id in instance_ids
        )
        _delete_apply_id(
            cleanup_case,
            apply_id=apply_id,
            ctx=f"CI cleanup runtime current_deployments {instance_id_text}",
        )
    _wait_ci_ports_free(cleanup_case, timeout_s=timeout_s)


def _http_json_allow_error_status(req: urllib.request.Request) -> Tuple[int, Dict[str, Any]]:
    deadline = time.time() + CONTROLLER_HTTP_RETRY_DEADLINE_SECONDS
    while True:
        try:
            transported = _controller_request_via_manifest(req, timeout_seconds=CONTROLLER_HTTP_TIMEOUT_SECONDS)
            if transported is not None:
                status_code, body = transported
                return int(status_code), _decode_http_json(body, ctx=str(req.full_url))
            with urllib.request.urlopen(req, timeout=CONTROLLER_HTTP_TIMEOUT_SECONDS) as resp:
                return int(resp.status), _decode_http_json(resp.read(), ctx=str(req.full_url))
        except urllib.error.HTTPError as exc:
            if int(exc.code) in CONTROLLER_STATUS_TRANSIENT_HTTP_CODES:
                if time.time() >= deadline:
                    raise ValueError(
                        "controller request timed out after retry deadline: "
                        f"url={req.full_url} err=HTTPError: {exc}"
                    ) from exc
                print(
                    f"[_http_json_allow_error_status] transient HTTP error; retrying: "
                    f"url={req.full_url} status={exc.code}",
                    flush=True,
                )
                time.sleep(CONTROLLER_HTTP_RETRY_SLEEP_SECONDS)
                continue
            return int(exc.code), _decode_http_json(exc.read(), ctx=str(req.full_url))
        except (urllib.error.URLError, TimeoutError, OSError, ConnectionError) as exc:
            if time.time() >= deadline:
                raise ValueError(
                    "controller request timed out after retry deadline: "
                    f"url={req.full_url} err={type(exc).__name__}: {exc}"
                ) from exc
            print(
                f"[_http_json_allow_error_status] transient transport error; retrying: "
                f"url={req.full_url} err={type(exc).__name__}: {exc}",
                flush=True,
            )
            time.sleep(CONTROLLER_HTTP_RETRY_SLEEP_SECONDS)


def _http_json_allow_error_status_allow_empty_success(
    req: urllib.request.Request,
) -> Tuple[int, Any]:
    deadline = time.time() + CONTROLLER_HTTP_RETRY_DEADLINE_SECONDS
    while True:
        try:
            transported = _controller_request_via_manifest(req, timeout_seconds=CONTROLLER_HTTP_TIMEOUT_SECONDS)
            if transported is not None:
                status_code, body = transported
                if int(status_code) == 200:
                    return 200, {}
                try:
                    return int(status_code), _decode_http_json(body, ctx=str(req.full_url))
                except ValueError:
                    return int(status_code), body.decode("utf-8", errors="replace")
            with urllib.request.urlopen(req, timeout=CONTROLLER_HTTP_TIMEOUT_SECONDS) as resp:
                body = resp.read()
                if int(resp.status) == 200:
                    return 200, {}
                try:
                    return int(resp.status), _decode_http_json(body, ctx=str(req.full_url))
                except ValueError:
                    return int(resp.status), body.decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            if int(exc.code) in CONTROLLER_STATUS_TRANSIENT_HTTP_CODES:
                if time.time() >= deadline:
                    raise ValueError(
                        "controller request timed out after retry deadline: "
                        f"url={req.full_url} err=HTTPError: {exc}"
                    ) from exc
                print(
                    f"[_http_json_allow_error_status_allow_empty_success] transient HTTP error; retrying: "
                    f"url={req.full_url} status={exc.code}",
                    flush=True,
                )
                time.sleep(CONTROLLER_HTTP_RETRY_SLEEP_SECONDS)
                continue
            body = exc.read()
            try:
                return int(exc.code), _decode_http_json(body, ctx=str(req.full_url))
            except ValueError:
                return int(exc.code), body.decode("utf-8", errors="replace")
        except (urllib.error.URLError, TimeoutError, OSError, ConnectionError) as exc:
            if time.time() >= deadline:
                raise ValueError(
                    "controller request timed out after retry deadline: "
                    f"url={req.full_url} err={type(exc).__name__}: {exc}"
                ) from exc
            print(
                f"[_http_json_allow_error_status_allow_empty_success] transient transport error; retrying: "
                f"url={req.full_url} err={type(exc).__name__}: {exc}",
                flush=True,
            )
            time.sleep(CONTROLLER_HTTP_RETRY_SLEEP_SECONDS)


def _decode_http_json(data: bytes, *, ctx: str) -> Dict[str, Any]:
    try:
        obj = json.loads(data.decode("utf-8"))
    except Exception as exc:
        raise ValueError(f"http response is not valid json: ctx={ctx} err={exc}") from exc
    return _require_dict(obj, f"http_json {ctx}")


def _cluster_node_ssh_host(node_cfg: Dict[str, Any], *, target_name: str) -> str:
    ssh_host = node_cfg.get("ssh_host")
    if ssh_host is None:
        return _require_str(node_cfg.get("ip"), f"cluster_nodes[{target_name}].ip")
    return _require_str(ssh_host, f"cluster_nodes[{target_name}].ssh_host")


def _sync_run_dir_archive_to_remote_target(
    *,
    archive_path: Path,
    target_name: str,
    node_cfg: Dict[str, Any],
    run_dir_abs: Path,
    verify_relpaths: List[str],
    sync_mode: str,
    ctx: str,
) -> None:
    ssh_user = _require_str(node_cfg.get("ssh_user"), f"cluster_nodes[{target_name}].ssh_user")
    ssh_port = _require_int(node_cfg.get("ssh_port"), f"cluster_nodes[{target_name}].ssh_port", min_v=1)
    ssh_password_raw = node_cfg.get("ssh_password")
    ssh_password = None if ssh_password_raw is None else _require_str(
        ssh_password_raw,
        f"cluster_nodes[{target_name}].ssh_password",
    )
    ssh_host = _cluster_node_ssh_host(node_cfg, target_name=target_name)
    node_ip = _require_str(node_cfg.get("ip"), f"cluster_nodes[{target_name}].ip")

    if not verify_relpaths:
        raise ValueError(f"{ctx}.verify_relpaths must be non-empty")
    if sync_mode not in (REMOTE_RUN_DIR_SYNC_REPLACE, REMOTE_RUN_DIR_SYNC_OVERLAY):
        raise ValueError(f"{ctx}.sync_mode invalid: {sync_mode!r}")

    remote_archive = f"/tmp/{archive_path.stem}__{target_name}.tar.gz"
    sh_quote = _shell_quote
    remote_target = f"{ssh_user}@{ssh_host}"
    verify_cmd = " && ".join(
        "test -f " + sh_quote(str((run_dir_abs / rel_path).resolve()))
        for rel_path in verify_relpaths
    )
    scp_argv = [
        "scp",
        "-O",
        *_remote_ssh_common_argv(),
        "-P",
        str(ssh_port),
        str(archive_path),
        f"{remote_target}:{remote_archive}",
    ]
    remote_prepare_cmd = "set -euo pipefail && mkdir -p " + sh_quote(str(run_dir_abs.parent))
    if sync_mode == REMOTE_RUN_DIR_SYNC_REPLACE:
        remote_prepare_cmd += " && rm -rf " + sh_quote(str(run_dir_abs))
    extract_remote_cmd = (
        remote_prepare_cmd
        + " && tar xzf "
        + sh_quote(remote_archive)
        + " -C / && rm -f "
        + sh_quote(remote_archive)
        + " && test -f "
        + sh_quote(str((run_dir_abs / verify_relpaths[0]).resolve()))
        + " && "
        + verify_cmd
    )
    ssh_argv = [
        "ssh",
        *_remote_ssh_common_argv(),
        "-p",
        str(ssh_port),
        remote_target,
        extract_remote_cmd,
    ]
    print(
        f"[{ctx}] target={target_name} ip={node_ip} run_dir={run_dir_abs} archive={archive_path}",
        flush=True,
    )
    _run_ssh_transport_command(
        argv=scp_argv,
        password=ssh_password,
        ctx=f"{ctx} scp {target_name}",
        timeout_seconds=_SSH_TRANSPORT_ARCHIVE_TRANSFER_TIMEOUT_SECONDS,
        emit_output=True,
    )
    _run_ssh_transport_argv(argv=ssh_argv, password=ssh_password, ctx=f"{ctx} ssh {target_name}")


def _sync_run_dir_archive_via_bastion(
    *,
    resolved_case: Dict[str, Any],
    archive_path: Path,
    remote_targets: List[Tuple[str, Dict[str, Any], Any]],
    run_dir_abs: Path,
    verify_relpaths: List[str],
    sync_mode: str,
    ctx: str,
) -> None:
    _ = resolved_case
    if sync_mode not in (REMOTE_RUN_DIR_SYNC_REPLACE, REMOTE_RUN_DIR_SYNC_OVERLAY):
        raise ValueError(f"{ctx}.sync_mode invalid: {sync_mode!r}")
    for target_name, node_cfg, _ in remote_targets:
        _sync_run_dir_archive_to_remote_target(
            archive_path=archive_path,
            target_name=target_name,
            node_cfg=node_cfg,
            run_dir_abs=run_dir_abs,
            verify_relpaths=verify_relpaths,
            sync_mode=sync_mode,
            ctx=ctx,
        )


def _run_adapter_action(
    resolved_case: Dict[str, Any],
    *,
    run_dir: Path,
    action: str,
) -> Optional[Dict[str, Any]]:
    if action not in ("deploy", "collect", "teardown"):
        raise ValueError(f"invalid adapter action: {action}")

    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    adapter_cmd = _require_list(deploy.get("adapter_cmd"), "resolved_case.deploy.adapter_cmd")
    if not adapter_cmd:
        raise ValueError("deploy.adapter_cmd is empty")

    runtime = _require_dict(resolved_case.get("runtime"), "resolved_case.runtime")
    config_root = Path(_require_str(runtime.get("config_root"), "resolved_case.runtime.config_root")).resolve()
    argv = _resolved_profile_cmd(
        resolved_case,
        field_name="deploy.adapter_cmd",
        argv=adapter_cmd,
    ) + ["--action", action, "--workdir", str(run_dir)]
    cmd_cwd = str(_profile_cmd_entrypoint_root("deploy.adapter_cmd"))
    if os.path.isabs(argv[0]) and not Path(argv[0]).exists():
        raise ValueError(f"deploy.adapter_cmd entrypoint does not exist: {argv[0]}")
    _run_subprocess(argv, cwd=cmd_cwd)

    if action == "deploy":
        p = run_dir / "deploy_result.yaml"
        if not p.exists():
            raise ValueError("adapter deploy did not produce deploy_result.yaml")
        out = _load_yaml_file(p)
        return _require_dict(out, "deploy_result")
    return None


def _run_subprocess(argv: List[str], *, cwd: str) -> None:
    print("RUN:", " ".join(_shell_quote(a) for a in argv), flush=True)
    proc = subprocess.run(argv, cwd=cwd)
    if proc.returncode != 0:
        raise RuntimeError(f"command failed: rc={proc.returncode}")


_SSH_TRANSPORT_TIMEOUT_SECONDS = 180.0
_SSH_TRANSPORT_ARCHIVE_TRANSFER_TIMEOUT_SECONDS = 1800.0
_SSH_TRANSPORT_MAX_ATTEMPTS = 10
_SSH_TRANSPORT_RETRY_SLEEP_SECONDS = 5.0
_SSH_TRANSPORT_RETRYABLE_ERROR_SNIPPETS = (
    "connection timed out",
    "operation timed out",
    "timed out during banner exchange",
    "banner exchange",
    "connection reset by peer",
    "connection closed by remote host",
    "connection closed",
    "broken pipe",
    "kex_exchange_identification",
    "no route to host",
    "network is unreachable",
    "connection refused",
    "resource temporarily unavailable",
    "client_loop: send disconnect",
)


def _emit_ssh_transport_completed_output(completed: subprocess.CompletedProcess[str]) -> None:
    if completed.stdout:
        sys.stdout.write(completed.stdout)
        if not completed.stdout.endswith("\n"):
            sys.stdout.write("\n")
        sys.stdout.flush()
    if completed.stderr:
        sys.stderr.write(completed.stderr)
        if not completed.stderr.endswith("\n"):
            sys.stderr.write("\n")
        sys.stderr.flush()


def _format_ssh_transport_failure(*, ctx: str, completed: subprocess.CompletedProcess[str]) -> str:
    parts = [f"{ctx} failed: rc={completed.returncode}"]
    stdout = completed.stdout.strip()
    stderr = completed.stderr.strip()
    if stdout:
        parts.append(f"stdout:\n{stdout}")
    if stderr:
        parts.append(f"stderr:\n{stderr}")
    return "\n".join(parts)


def _is_retryable_ssh_transport_failure(completed: subprocess.CompletedProcess[str]) -> bool:
    error_text = "\n".join(part for part in (completed.stdout, completed.stderr) if part).lower()
    if any(snippet in error_text for snippet in _SSH_TRANSPORT_RETRYABLE_ERROR_SNIPPETS):
        return True
    return completed.returncode == 255 and not completed.stdout.strip() and not completed.stderr.strip()


def _run_ssh_transport_command(
    *,
    argv: List[str],
    password: Optional[str],
    ctx: str,
    timeout_seconds: Optional[float],
    emit_output: bool,
    max_attempts: Optional[int] = None,
) -> subprocess.CompletedProcess[str]:
    full_argv = list(argv)
    if password is not None:
        full_argv = ["sshpass", "-p", password, *full_argv]
    rendered = " ".join(_shell_quote(a) for a in full_argv)
    effective_max_attempts = _SSH_TRANSPORT_MAX_ATTEMPTS if max_attempts is None else max(1, int(max_attempts))
    for attempt in range(1, effective_max_attempts + 1):
        print("RUN:", rendered, flush=True)
        try:
            completed = subprocess.run(
                full_argv,
                timeout=timeout_seconds,
                capture_output=True,
                text=True,
            )
        except subprocess.TimeoutExpired as exc:
            if attempt >= effective_max_attempts:
                raise RuntimeError(
                    f"{ctx} timed out after {timeout_seconds:.0f}s "
                    f"(attempt {attempt}/{effective_max_attempts})"
                ) from exc
            print(
                f"[{ctx}] ssh transport timed out after {timeout_seconds:.0f}s; "
                f"retrying attempt {attempt + 1}/{effective_max_attempts}",
                flush=True,
            )
            time.sleep(_SSH_TRANSPORT_RETRY_SLEEP_SECONDS)
            continue
        if completed.returncode == 0:
            if emit_output:
                _emit_ssh_transport_completed_output(completed)
            return completed
        failure_text = _format_ssh_transport_failure(ctx=ctx, completed=completed)
        if not _is_retryable_ssh_transport_failure(completed):
            raise RuntimeError(failure_text)
        if attempt >= effective_max_attempts:
            raise RuntimeError(
                f"{failure_text}\nreached retry limit={effective_max_attempts}"
            )
        print(failure_text, flush=True)
        print(
            f"[{ctx}] transient ssh transport failure; retrying attempt "
            f"{attempt + 1}/{effective_max_attempts} after {_SSH_TRANSPORT_RETRY_SLEEP_SECONDS:.0f}s",
            flush=True,
        )
        time.sleep(_SSH_TRANSPORT_RETRY_SLEEP_SECONDS)
    raise RuntimeError(f"{ctx} exhausted ssh transport retry loop unexpectedly")


def _run_ssh_transport_argv(*, argv: List[str], password: Optional[str], ctx: str) -> None:
    _run_ssh_transport_command(
        argv=argv,
        password=password,
        ctx=ctx,
        timeout_seconds=_SSH_TRANSPORT_TIMEOUT_SECONDS,
        emit_output=True,
    )


def _new_controller_request(
    url: str,
    *,
    method: str,
    data: bytes | None = None,
    content_type: str | None = None,
) -> urllib.request.Request:
    if _CONTROLLER_BASIC_AUTH_HEADER is None:
        raise RuntimeError("controller_basic_auth is not initialized")
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header(_CONTROLLER_BASIC_AUTH_HEADER_NAME, _CONTROLLER_BASIC_AUTH_HEADER)
    if data is not None and content_type is not None:
        req.add_header("Content-Type", content_type)
    return req


def _http_get_plain(url: str) -> str:
    req = _new_controller_request(url, method="GET")
    transported = _controller_request_via_manifest(req, timeout_seconds=CONTROLLER_HTTP_SHORT_ATTEMPT_TIMEOUT_SECONDS)
    if transported is not None:
        status_code, body = transported
        if status_code < 200 or status_code >= 300:
            raise urllib.error.HTTPError(req.full_url, status_code, f"status={status_code}", hdrs=None, fp=None)
        return body.decode("utf-8", errors="replace")
    with urllib.request.urlopen(req, timeout=CONTROLLER_HTTP_SHORT_ATTEMPT_TIMEOUT_SECONDS) as resp:
        return resp.read().decode("utf-8", errors="replace")



def _is_deployer_online(controller_url: str) -> bool:
    u = controller_url.rstrip("/") + "/api/health"
    try:
        body = _http_get_plain(u)
        payload = json.loads(body)
        return bool(payload.get("ok"))
    except Exception:
        return False


def _wait_deployer_online(*, controller_url: str, timeout_s: int) -> None:
    deadline = time.time() + float(timeout_s)
    while True:
        if _is_deployer_online(controller_url):
            return
        if time.time() >= deadline:
            raise ValueError(
                f"deployer controller did not become ready within timeout: controller_url={controller_url} timeout_s={timeout_s}"
            )
        time.sleep(1.0)


def _bootstrap_test_bed_via_runner() -> bool:
    manifest_info = _load_test_bed_manifest_opt()
    if manifest_info is None:
        return False
    start_config_path = _test_bed_bundle_manifest_required_path(field_name="start_config_path")
    workdir_path = _test_bed_bundle_manifest_required_path(field_name="workdir")
    bootstrap_mode = "bare_then_apply"
    manifest_path, manifest = manifest_info
    raw_mode = manifest.get("bootstrap_mode")
    if raw_mode is not None and str(raw_mode).strip():
        bootstrap_mode = _require_str(
            raw_mode,
            f"testbed manifest {manifest_path}.bootstrap_mode",
        )
    start_test_bed_path = (_runner_repo_root() / "fluxon_test_stack" / "start_test_bed.py").resolve()
    cmd = [
        sys.executable,
        str(start_test_bed_path),
        "--config",
        str(start_config_path),
        "--workdir",
        str(workdir_path),
        "--bootstrap-mode",
        bootstrap_mode,
    ]
    print(
        f"INFO: controller is offline; bootstrapping shared test bed via test_runner authority: {start_test_bed_path}",
        flush=True,
    )
    print("RUN: " + " ".join(_shell_quote(str(part)) for part in cmd), flush=True)
    completed = subprocess.run(
        cmd,
        cwd=str(_runner_repo_root()),
        check=False,
    )
    if completed.returncode != 0:
        print(
            f"ERROR: shared test bed bootstrap via start_test_bed failed rc={completed.returncode}",
            flush=True,
        )
        return False
    return True



def _ensure_controller_online(*, controller_url: str, ops_cluster_name: Optional[str]) -> None:
    controller_url = _require_str(controller_url, "controller_url").rstrip("/")
    if _is_deployer_online(controller_url):
        return

    if _bootstrap_test_bed_via_runner():
        _wait_deployer_online(
            controller_url=controller_url,
            timeout_s=_test_bed_controller_ready_timeout_seconds(),
        )
        return

    ops_cluster_text = ""
    if ops_cluster_name is not None and str(ops_cluster_name).strip():
        ops_cluster_text = f" ops_cluster_name={ops_cluster_name!r}."
    raise ValueError(
        "deployer controller is not online; test bed must be prepared manually before running test_runner. "
        f"controller_url={controller_url}.{ops_cluster_text} "
        "Run test bed bootstrap (one-time): "
        "`python3 deployment/manual_dispatch_release.py -c fluxon_test_stack/deployconf_testbed.yml --release-dir <YOUR_RELEASE_DIR>`; "
        "then: "
        "`python3 -u fluxon_test_stack/start_test_bed.py -c fluxon_test_stack/start_test_bed.yaml -w <YOUR_WORKDIR>`; "
        "and wait for controller_url:/api/health to return {\"ok\": true}."
    )


def _ensure_stack_controller_online(stack_identity: Dict[str, Any]) -> None:
    _ensure_controller_online(
        controller_url=_require_str(
            stack_identity.get("controller_url"),
            "stack_identity.controller_url",
        ),
        ops_cluster_name=_require_str(
            stack_identity.get("ops_cluster_name"),
            "stack_identity.ops_cluster_name",
        ),
    )


def _ensure_deployer_online(resolved_case: Dict[str, Any]) -> None:
    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    runtime = _require_dict(resolved_case.get("runtime"), "resolved_case.runtime")
    stack_identity = _require_dict(runtime.get("stack_identity"), "resolved_case.runtime.stack_identity")
    _ensure_controller_online(
        controller_url=_require_str(deploy.get("controller_url"), "deploy.controller_url"),
        ops_cluster_name=_require_str(
            stack_identity.get("ops_cluster_name"),
            "resolved_case.runtime.stack_identity.ops_cluster_name",
        ),
    )



def _shell_quote(s: str) -> str:
    if s == "":
        return "''"
    if re.fullmatch(r"[A-Za-z0-9_./:=@+-]+", s):
        return s
    return "'" + s.replace("'", "'\\''") + "'"



def _find_deploy_instance_opt(resolved_case: Dict[str, Any], *, instance_id: str) -> Optional[Dict[str, Any]]:
    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    instances = _require_list(deploy.get("instances"), "resolved_case.deploy.instances")
    for raw in instances:
        if not isinstance(raw, dict):
            continue
        if raw.get("id") == instance_id:
            return raw
    return None


def _find_deploy_instance(resolved_case: Dict[str, Any], *, instance_id: str) -> Dict[str, Any]:
    inst = _find_deploy_instance_opt(resolved_case, instance_id=instance_id)
    if inst is None:
        raise ValueError(f"missing deploy instance: {instance_id}")
    return inst


def _find_deploy_result_instance(deploy_result: Dict[str, Any], *, instance_id: str) -> Dict[str, Any]:
    insts = _require_list(deploy_result.get("instances"), "deploy_result.instances")
    for raw in insts:
        row = _require_dict(raw, "deploy_result.instances[]")
        if row.get("id") == instance_id:
            return row
    raise ValueError(f"missing instance in deploy_result: {instance_id}")


def _ci_runtime_cleanup_case(resolved_case: Dict[str, Any], *, ctx: str) -> Dict[str, Any]:
    cleanup_case = copy.deepcopy(resolved_case)
    deploy = _require_dict(cleanup_case.get("deploy"), "resolved_case.deploy")
    deploy_instances = _require_list(deploy.get("instances"), "resolved_case.deploy.instances")
    deploy_by_id: Dict[str, Dict[str, Any]] = {}
    for raw in deploy_instances:
        inst = _require_dict(raw, "resolved_case.deploy.instances[]")
        instance_id = _require_str(inst.get("id"), "resolved_case.deploy.instances[].id")
        deploy_by_id[instance_id] = copy.deepcopy(inst)

    required_ids = list(_ci_case_instance_ids(cleanup_case))
    missing_ids = [instance_id for instance_id in required_ids if instance_id not in deploy_by_id]
    if missing_ids:
        _compile_ci_case(cleanup_case)
        deploy = _require_dict(cleanup_case.get("deploy"), "resolved_case.deploy")
        deploy_instances = _require_list(deploy.get("instances"), "resolved_case.deploy.instances")
        deploy_by_id = {}
        for raw in deploy_instances:
            inst = _require_dict(raw, "resolved_case.deploy.instances[]")
            instance_id = _require_str(inst.get("id"), "resolved_case.deploy.instances[].id")
            deploy_by_id[instance_id] = copy.deepcopy(inst)
        required_ids = list(_ci_case_instance_ids(cleanup_case))
        missing_ids = [instance_id for instance_id in required_ids if instance_id not in deploy_by_id]
        if missing_ids:
            raise ValueError(f"{ctx}: missing CI deploy instances after CI materialization: {missing_ids}")
    _apply_stable_deploy_names(cleanup_case)
    return cleanup_case


def _load_previous_ci_cleanup_case(
    previous_run_dir: Path,
    *,
    fallback_resolved_case: Dict[str, Any],
) -> Dict[str, Any]:
    previous_resolved_case_path = previous_run_dir / "resolved_case.yaml"
    raw_previous = _load_yaml_file_if_present(
        previous_resolved_case_path,
        ctx=f"CI cleanup previous failed runtime {previous_run_dir.name} resolved_case",
    )
    if raw_previous is None:
        return _ci_runtime_cleanup_case(
            fallback_resolved_case,
            ctx=f"CI cleanup previous failed runtime {previous_run_dir.name}",
        )
    previous_case = _require_dict(raw_previous, f"{previous_resolved_case_path}")
    previous_profile = previous_case.get("profile")
    previous_ci_runtime = None
    if isinstance(previous_profile, dict):
        previous_profile_ci = previous_profile.get("ci")
        if isinstance(previous_profile_ci, dict):
            previous_ci_runtime = previous_profile_ci.get("runtime")
    if not isinstance(previous_ci_runtime, dict):
        return _ci_runtime_cleanup_case(
            fallback_resolved_case,
            ctx=f"CI cleanup previous failed runtime {previous_run_dir.name}",
        )
    return _ci_runtime_cleanup_case(
        previous_case,
        ctx=f"CI cleanup previous failed runtime {previous_run_dir.name}",
    )


def _ops_current_deployments(controller_url: str) -> List[Dict[str, Any]]:
    req = _new_controller_request(controller_url + "/api/current_deployments", method="GET")
    status_code, resp = _http_json_allow_error_status(req)
    if status_code == 200:
        if resp.get("ok") is not True:
            raise ValueError(f"ops current_deployments returned ok=false: resp={resp}")
        return _require_list(resp.get("groups"), "current_deployments.groups")
    if _ops_current_deployments_is_effectively_empty(status_code=status_code, resp=resp):
        return []
    raise ValueError(f"ops current_deployments failed: status={status_code} resp={resp}")


def _ops_current_deployments_is_effectively_empty(*, status_code: int, resp: Dict[str, Any]) -> bool:
    if status_code not in (500, 502):
        return False
    groups = resp.get("groups")
    if not isinstance(groups, list) or groups:
        return False
    return _resp_contains_any_text(resp, (_CURRENT_DEPLOYMENTS_MISSING_APPLY_ERR,))


def _cleanup_skipped_case_desired_applies(*, controller_url: str, case_id: str) -> None:
    """Delete any active apply groups whose workloads belong to the skipped case.

    English rationale:
    - A FULL_ONCE runner can legitimately SKIP a case that already succeeded.
    - If we skip without cleanup, a previous desired apply group can keep running and poison
      later cases via stable workload name collisions (case_id is the name prefix).
    - This cleanup is not a fallback. If it cannot converge, the test bed is unsafe to continue.
    """
    controller_url = _require_str(controller_url, "cleanup_skipped_case.controller_url").rstrip("/")
    case_id = _require_str(case_id, "cleanup_skipped_case.case_id")
    prefix = case_id + "__"
    deadline = time.time() + 120.0

    def workload_name_matches(name: str) -> bool:
        if name.startswith(prefix):
            return True
        if name == _TEST_STACK_COORD_WORKLOAD_NAME:
            return True
        if name.startswith(_TEST_STACK_NODE_WORKLOAD_PREFIX) and case_id in name:
            return True
        return False

    while True:
        matched: List[Tuple[str, List[str]]] = []
        for group_index, raw_group in enumerate(_ops_current_deployments(controller_url)):
            group = _require_dict(raw_group, f"current_deployments.groups[{group_index}]")
            apply_id = _require_str(group.get("apply_id"), f"current_deployments.groups[{group_index}].apply_id")
            raw_workloads = group.get("workloads")
            if not isinstance(raw_workloads, list):
                continue
            names: List[str] = []
            for workload_index, raw_workload in enumerate(raw_workloads):
                if not isinstance(raw_workload, dict):
                    continue
                raw_name = raw_workload.get("name")
                if not isinstance(raw_name, str) or not raw_name:
                    continue
                if workload_name_matches(raw_name):
                    names.append(raw_name)
            if names:
                matched.append((apply_id, sorted(set(names))))

        if not matched:
            return

        for apply_id, names in matched:
            print(
                "[SKIP cleanup] deleting leftover desired apply for skipped case: "
                f"case_id={case_id} apply_id={apply_id} workloads={names}",
                flush=True,
            )
            _ops_delete_apply_id(controller_url, apply_id=apply_id, ctx=f"SKIP cleanup {case_id}")

        if time.time() >= deadline:
            raise ValueError(
                "SKIP cleanup did not converge after timeout: "
                f"case_id={case_id} still_present_apply_ids={[aid for (aid, _) in matched]}"
            )
        time.sleep(1.0)


def _ci_runtime_current_apply_ids(resolved_case: Dict[str, Any]) -> List[Dict[str, Any]]:
    cleanup_case = _ci_runtime_cleanup_case(resolved_case, ctx="CI current runtime apply ids")
    deploy = _require_dict(cleanup_case.get("deploy"), "resolved_case.deploy")
    controller_url = _require_str(deploy.get("controller_url"), "deploy.controller_url").rstrip("/")
    deploy_instances = _require_list(deploy.get("instances"), "resolved_case.deploy.instances")

    workload_to_instance_ids: Dict[Tuple[str, str], List[str]] = {}
    for raw in deploy_instances:
        inst = _require_dict(raw, "resolved_case.deploy.instances[]")
        instance_id = _require_str(inst.get("id"), "resolved_case.deploy.instances[].id")
        if instance_id not in set(_ci_case_instance_ids(cleanup_case)):
            continue
        k8s_ref = _require_str(inst.get("k8s_ref"), f"{instance_id}.k8s_ref")
        kind, name = _ops_kind_from_k8s_ref(k8s_ref, ctx=f"{instance_id}.k8s_ref")
        key = (kind, name)
        instance_ids = workload_to_instance_ids.setdefault(key, [])
        if instance_id not in instance_ids:
            instance_ids.append(instance_id)

    matches: List[Dict[str, Any]] = []
    seen_apply_ids: set[str] = set()
    for index, raw_group in enumerate(_ops_current_deployments(controller_url)):
        group = _require_dict(raw_group, f"current_deployments.groups[{index}]")
        apply_id = _require_str(group.get("apply_id"), f"current_deployments.groups[{index}].apply_id")
        workloads = _require_list(group.get("workloads"), f"current_deployments.groups[{index}].workloads")

        matched_instance_ids: List[str] = []
        for workload_index, raw_workload in enumerate(workloads):
            workload = _require_dict(
                raw_workload,
                f"current_deployments.groups[{index}].workloads[{workload_index}]",
            )
            kind = _require_str(
                workload.get("kind"),
                f"current_deployments.groups[{index}].workloads[{workload_index}].kind",
            )
            name = _require_str(
                workload.get("name"),
                f"current_deployments.groups[{index}].workloads[{workload_index}].name",
            )
            for instance_id in workload_to_instance_ids.get((kind, name), []):
                if instance_id not in matched_instance_ids:
                    matched_instance_ids.append(instance_id)

        if not matched_instance_ids or apply_id in seen_apply_ids:
            continue
        seen_apply_ids.add(apply_id)
        matches.append({"apply_id": apply_id, "instance_ids": matched_instance_ids})

    return matches


def _parse_sha256_manifest(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for i, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        digest, name = _parse_sha256_manifest_line(raw, index=i)
        if name in out:
            raise ValueError(f"duplicate relpath in sha256 manifest: {name}")
        out[name] = digest
    if not out:
        raise ValueError("sha256 manifest is empty")
    return out


def _parse_sha256_manifest_line(raw: str, *, index: int) -> Tuple[str, str]:
    line = raw.rstrip("\n")
    if len(line) < 67 or line[64:66] != "  ":
        raise ValueError(f"invalid sha256 manifest line {index}: {raw!r}")
    digest = line[:64]
    name = line[66:]
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError(f"invalid sha256 digest at line {index}: {digest!r}")
    if not _is_clean_manifest_relpath(name):
        raise ValueError(f"invalid relpath in sha256 manifest at line {index}: {name!r}")
    return digest, name


def _is_clean_manifest_relpath(name: str) -> bool:
    if not name or name.startswith("/") or name.startswith("\\"):
        return False
    if "\\" in name or "\x00" in name:
        return False
    return all(part not in ("", ".", "..") for part in name.split("/"))


def _validate_release_manifest_integrity(
    *,
    release_root: Path,
    manifest: Dict[str, str],
    ctx: str,
) -> None:
    manifest_digest = _manifest_integrity_cache_digest(manifest)
    resolved_root = release_root.resolve()
    stat_fingerprint = _manifest_integrity_stat_fingerprint(
        release_root=release_root,
        manifest=manifest,
        ctx=ctx,
    )
    cache_key = (str(resolved_root), manifest_digest)
    if _MANIFEST_INTEGRITY_CACHE.get(cache_key) == stat_fingerprint:
        return
    for name, expected_sha256 in manifest.items():
        file_path = release_root / name
        got_sha256 = _sha256_file(file_path)
        if got_sha256 != expected_sha256:
            rerun_cmd = (
                "fluxon_test_stack/pack_test_stack_rsc.py --all-profiles"
                if "test_rsc" in ctx
                else "the release pack authority"
            )
            raise ValueError(
                f"{ctx} manifest drift detected: file={file_path} "
                f"expected_sha256={expected_sha256} got_sha256={got_sha256}. "
                f"Re-run {rerun_cmd} so manifest and artifacts are regenerated together."
            )
    _MANIFEST_INTEGRITY_CACHE[cache_key] = stat_fingerprint

def _safe_extract_tar_gz(*, archive_path: Path, dest_dir: Path) -> None:
    if not archive_path.exists():
        raise ValueError(f"missing tarball: {archive_path}")
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_root = dest_dir.resolve()
    with tarfile.open(archive_path, "r:gz") as tf:
        members = tf.getmembers()
        for m in members:
            name = m.name
            if not name or name.startswith("/") or name.startswith("\\"):
                raise ValueError(f"unsafe tar path in {archive_path}: {name!r}")
            out = (dest_root / name).resolve()
            if not str(out).startswith(str(dest_root) + os.sep):
                raise ValueError(f"unsafe tar path in {archive_path}: {name!r}")
        tf.extractall(dest_root)



_CI_SOURCE_OVERLAY_ROOTS: Tuple[str, ...] = ("setup_and_pack", "fluxon_py")
_CI_SOURCE_OVERLAY_IGNORE_NAMES: Tuple[str, ...] = (
    "target",
    ".git",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".venv",
    "venv",
    "logs",
)
_CI_SOURCE_OVERLAY_IGNORED_SUBTREES: Tuple[str, ...] = ()
_RELEASE_MANIFEST_FILENAME = "fluxon_release.sha256"
_TEST_RSC_MANIFEST_FILENAME = "fluxon_test_rsc.sha256"
_RELEASE_INVARIANT_FILE_RELPATHS: Tuple[str, ...] = ("install.py",)
_RELEASE_INVARIANT_DIR_RELPATHS: Tuple[str, ...] = ("ext_images",)
_TEST_STACK_RUNTIME_DIRNAME = "test_stack_runtime"
_TEST_STACK_RUNTIME_SOURCE_DIRNAME = "src"
_TEST_STACK_RUNTIME_WHEEL_DIRNAME = "wheels"
_TEST_STACK_RUNTIME_VENDOR_SITE_PACKAGES_DIRNAME = "vendor_site_packages"
_TEST_STACK_RUNTIME_PYTHON_RUNTIME_DIRNAME = "python_runtime"
_TEST_STACK_RUNTIME_PYTHON_RUNTIME_WHEELHOUSE_DIRNAME = "wheels"
_TEST_STACK_RUNTIME_PREPARE_CONFIG_NAME = "prepare.yaml"
_TEST_STACK_DEFAULT_PYTHON_ABI = "cpython3.10"
_TEST_STACK_HOST_VENV_PYTHON_BIN_RELPATH = ("bin", "python")
_TEST_STACK_HOST_VENV_IMPORT_PROBE_BASE = "import fluxon_py\n"
_TEST_STACK_RUNTIME_WHEEL_MARKER_FILENAME = ".fluxon_test_stack_runtime_wheels.sha256"
# Launcher passes the parent artifact root that contains both `profiles/...` and
# `test_rsc/...` subtrees. When set, this override is the single authority for
# benchmark artifact selection so the run cannot drift back to YAML-pinned caches.
_LOCAL_RELEASE_CACHE_ROOT_OVERRIDE_ENV = "FLUXON_TEST_STACK_LOCAL_RELEASE_ROOT"
_MANIFEST_INTEGRITY_CACHE: Dict[Tuple[str, str], Tuple[Tuple[str, int, int, int, int], ...]] = {}
_WHEEL_PY_TAG_RE = re.compile(r"-(cp([0-9])([0-9]{1,2}))-\1-")


def _manifest_integrity_cache_digest(manifest: Dict[str, str]) -> str:
    hasher = hashlib.sha256()
    for name, expected_sha256 in sorted(manifest.items()):
        hasher.update(name.encode("utf-8"))
        hasher.update(b"\0")
        hasher.update(expected_sha256.encode("ascii"))
        hasher.update(b"\n")
    return hasher.hexdigest()


def _manifest_integrity_stat_fingerprint(
    *,
    release_root: Path,
    manifest: Dict[str, str],
    ctx: str,
) -> Tuple[Tuple[str, int, int, int, int], ...]:
    fingerprint: List[Tuple[str, int, int, int, int]] = []
    for name in sorted(manifest):
        file_path = release_root / name
        if not file_path.exists() or not file_path.is_file():
            raise ValueError(f"{ctx} manifest references missing file: {file_path}")
        st = file_path.stat()
        fingerprint.append(
            (
                name,
                int(st.st_dev),
                int(st.st_ino),
                int(st.st_size),
                int(st.st_mtime_ns),
            )
        )
    return tuple(fingerprint)


def _test_stack_runtime_offline_dependency_requirements_for_resolved_case(
    resolved_case: Dict[str, Any],
) -> Tuple[str, ...]:
    dependency_set_ids = ["base"]
    scene = _require_dict(resolved_case.get("scene"), "resolved_case.scene")
    scene_ts_raw = scene.get("test_stack")
    if isinstance(scene_ts_raw, dict):
        rpc_backend_kind = str(scene_ts_raw.get("rpc_backend_kind", "")).strip().upper()
        if rpc_backend_kind == TEST_STACK_RPC_BACKEND_ZERORPC:
            dependency_set_ids.append("zerorpc")
    test_rsc_root = _resolved_case_test_rsc_root(resolved_case)
    prepare_cfg_path = (test_rsc_root / _TEST_STACK_RUNTIME_PREPARE_CONFIG_NAME).resolve()
    prepare_cfg = _require_dict(_load_yaml_file(prepare_cfg_path), f"TEST_STACK runtime prepare config {prepare_cfg_path}")
    python_runtime_cfg = _require_dict(
        prepare_cfg.get("python_runtime"),
        f"TEST_STACK runtime prepare config python_runtime {prepare_cfg_path}",
    )
    dependency_sets_cfg = _require_dict(
        python_runtime_cfg.get("dependency_sets"),
        f"TEST_STACK runtime prepare config python_runtime.dependency_sets {prepare_cfg_path}",
    )
    out: List[str] = []
    seen: set[str] = set()
    for set_id in dependency_set_ids:
        set_cfg = _require_dict(
            dependency_sets_cfg.get(set_id),
            f"TEST_STACK runtime prepare config python_runtime.dependency_sets.{set_id} {prepare_cfg_path}",
        )
        requirements = set_cfg.get("requirements")
        if not isinstance(requirements, list):
            raise ValueError(
                "TEST_STACK runtime prepare config dependency set requirements must be a list: "
                f"path={prepare_cfg_path} set_id={set_id}"
            )
        for index, raw_item in enumerate(requirements):
            requirement_cfg = _require_dict(
                raw_item,
                (
                    "TEST_STACK runtime prepare config "
                    f"python_runtime.dependency_sets.{set_id}.requirements[{index}]"
                ),
            )
            requirement = _require_str(
                requirement_cfg.get("pinned"),
                (
                    "TEST_STACK runtime prepare config "
                    f"python_runtime.dependency_sets.{set_id}.requirements[{index}].pinned"
                ),
            ).strip()
            if not requirement:
                raise ValueError(
                    "TEST_STACK runtime prepare config dependency requirement must be non-empty: "
                    f"path={prepare_cfg_path} set_id={set_id} index={index}"
                )
            if requirement in seen:
                continue
            seen.add(requirement)
            out.append(requirement)
    return tuple(out)


def _test_stack_runtime_wheelhouse_root(
    *,
    test_rsc_root: Path,
    python_abi: str,
) -> Path:
    return (
        test_rsc_root
        / _TEST_STACK_RUNTIME_PYTHON_RUNTIME_DIRNAME
        / python_abi
        / _TEST_STACK_RUNTIME_PYTHON_RUNTIME_WHEELHOUSE_DIRNAME
    ).resolve()
_TEST_STACK_RUNTIME_BACKEND_DIRNAME = "backend"
_TEST_STACK_RUNTIME_SOURCE_IGNORE_NAMES: Tuple[str, ...] = (
    ".dever",
    ".manual_dispatch_release_tmp",
    "__pycache__",
    "*.pyc",
    "boot_*",
    "bootstrap_*",
    "bench_runner",
    "test_rsc",
    "test_runner",
    "target_locks",
)


def _copy_runtime_resource_relpath_into_runtime(*, source_root: Path, runtime_root: Path, relpath: str) -> Path:
    relpath = _require_clean_relpath(relpath, "test_stack runtime resource relpath")
    src = (source_root / relpath).resolve()
    if not src.exists():
        raise ValueError(f"missing TEST_STACK backend runtime artifact: {src}")
    if src.is_file() and src.name.endswith(".tar.gz"):
        dest_root = (runtime_root / Path(relpath).parent).resolve()
        _safe_extract_tar_gz(archive_path=src, dest_dir=dest_root)
        extracted_root = (runtime_root / relpath[: -len(".tar.gz")]).resolve()
        if not extracted_root.exists():
            raise ValueError(
                f"TEST_STACK backend tarball did not materialize expected root: archive={src} expected={extracted_root}"
            )
        return extracted_root

    dest = (runtime_root / relpath).resolve()
    if src.is_dir():
        if dest.exists():
            raise ValueError(f"TEST_STACK backend runtime destination already exists: {dest}")
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(src, dest, dirs_exist_ok=False)
        return dest
    if dest.exists():
        raise ValueError(f"TEST_STACK backend runtime destination already exists: {dest}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    return dest


def _test_stack_cached_mooncake_wheel_path(*, wheel_name: str) -> Path:
    cached_root = (_runner_repo_root() / ".cached").resolve()
    wheel_path = (cached_root / wheel_name).resolve()
    if not wheel_path.exists() or not wheel_path.is_file():
        raise ValueError(f"TEST_STACK Mooncake cached wheel is missing: {wheel_path}")
    return wheel_path


def _test_stack_test_rsc_mooncake_wheel_path_opt(*, test_rsc_root: Path, wheel_name: str) -> Optional[Path]:
    if not test_rsc_root.exists():
        return None
    wheel_relpath = _require_clean_relpath(
        f"mooncake/{wheel_name}",
        "TEST_STACK Mooncake test_rsc wheel relpath",
    )
    wheel_path = (test_rsc_root / wheel_relpath).resolve()
    if not wheel_path.exists():
        return None
    if not wheel_path.is_file():
        raise ValueError(f"TEST_STACK Mooncake test_rsc wheel path must be a file: {wheel_path}")
    return wheel_path


def _test_stack_mooncake_wheel_source_path(*, test_rsc_root: Path, wheel_name: str) -> Path:
    test_rsc_wheel_path = _test_stack_test_rsc_mooncake_wheel_path_opt(
        test_rsc_root=test_rsc_root,
        wheel_name=wheel_name,
    )
    if test_rsc_wheel_path is not None:
        return test_rsc_wheel_path
    return _test_stack_cached_mooncake_wheel_path(wheel_name=wheel_name)


def _prepare_test_stack_runtime(
    *,
    resolved_case: Dict[str, Any],
    release_root: Path,
    run_dir: Path,
) -> Dict[str, str]:
    """Stage one explicit TEST_STACK runtime bundle into the run directory."""
    run_dir = run_dir.resolve()
    repo_root = _runner_repo_root()

    release_root = release_root.resolve()
    if not release_root.exists() or not release_root.is_dir():
        raise ValueError(f"missing fluxon_release directory for TEST_STACK runtime: {release_root}")
    test_rsc_root = _resolved_case_test_rsc_root(resolved_case)
    if test_rsc_root.exists() and not test_rsc_root.is_dir():
        raise ValueError(f"materialized test_rsc_root must be a directory: {test_rsc_root}")

    manifest_path = release_root / "fluxon_release.sha256"
    if not manifest_path.exists():
        raise ValueError(f"missing fluxon_release.sha256 for TEST_STACK runtime: {manifest_path}")
    manifest = _parse_sha256_manifest(manifest_path.read_text(encoding="utf-8"))
    _validate_release_manifest_integrity(
        release_root=release_root,
        manifest=manifest,
        ctx="TEST_STACK source release",
    )
    release_artifacts = _artifact_set_release_artifacts(resolved_case)
    wheel_name = release_artifacts["wheel"]
    if wheel_name not in manifest:
        raise ValueError(f"TEST_STACK source release manifest missing artifact declared by artifact_set: {wheel_name}")

    profile = _require_dict(resolved_case.get("profile"), "resolved_case.profile")
    profile_ts = _require_dict(profile.get("test_stack"), "resolved_case.profile.test_stack")
    backend_kind = _require_test_stack_backend_kind(
        profile_ts.get("kind"),
        "resolved_case.profile.test_stack.kind",
    )

    runtime_root = (run_dir / _TEST_STACK_RUNTIME_DIRNAME).resolve()
    src_root = (runtime_root / _TEST_STACK_RUNTIME_SOURCE_DIRNAME).resolve()
    wheels_root = (runtime_root / _TEST_STACK_RUNTIME_WHEEL_DIRNAME).resolve()
    vendor_site_packages_root = (runtime_root / _TEST_STACK_RUNTIME_VENDOR_SITE_PACKAGES_DIRNAME).resolve()
    backend_root = (runtime_root / _TEST_STACK_RUNTIME_BACKEND_DIRNAME).resolve()
    offline_dependency_requirements = _test_stack_runtime_offline_dependency_requirements_for_resolved_case(
        resolved_case
    )

    test_stack_src = (repo_root / "fluxon_test_stack").resolve()
    if not test_stack_src.exists() or not test_stack_src.is_dir():
        raise ValueError(f"missing fluxon_test_stack source directory: {test_stack_src}")

    wheel_src = (release_root / wheel_name).resolve()
    if not wheel_src.exists():
        raise ValueError(f"missing TEST_STACK wheel artifact: {wheel_src}")
    wheelhouse_root_src = _test_stack_runtime_wheelhouse_root(
        test_rsc_root=test_rsc_root,
        python_abi=_TEST_STACK_DEFAULT_PYTHON_ABI,
    )
    if not wheelhouse_root_src.exists() or not wheelhouse_root_src.is_dir():
        raise ValueError(
            "TEST_STACK runtime offline wheelhouse is missing from test_rsc: "
            f"{wheelhouse_root_src}"
        )

    # Resume semantics: if the runtime bundle already exists (e.g. crash mid-run), reuse it.
    # The deploy stage packages run_dir, so overwriting the runtime bundle is unnecessary and
    # risks losing postmortem context. Instead, verify expected structure and continue.
    redis_binary_dest: Optional[Path] = None
    redis_bundle_root_dest: Optional[Path] = None
    alluxio_bundle_root_dest: Optional[Path] = None
    mooncake_wheel_dest: Optional[Path] = None
    mooncake_wheel_src: Optional[Path] = None
    if backend_kind == TEST_STACK_BACKEND_MOONCAKE:
        rc_mooncake = _require_dict(
            _require_dict(profile_ts.get("runtime_config"), "resolved_case.profile.test_stack.runtime_config").get("master"),
            "resolved_case.profile.test_stack.runtime_config.master",
        )
        mooncake_wheel_src = _test_stack_mooncake_wheel_source_path(
            test_rsc_root=test_rsc_root,
            wheel_name=_require_str(
                rc_mooncake.get("wheel_name"),
                "resolved_case.profile.test_stack.runtime_config.master.wheel_name",
            ),
        )

    if not runtime_root.exists():
        runtime_root.mkdir(parents=True, exist_ok=False)
        src_root.mkdir(parents=True, exist_ok=False)
        wheels_root.mkdir(parents=True, exist_ok=False)
        backend_root.mkdir(parents=True, exist_ok=False)
        # English note:
        # - `fluxon_test_stack/test_runner/` contains runner outputs (workdirs/results/logs).
        # - `fluxon_test_stack/bench_runner/` contains local benchmark workdirs/results.
        # - Workdirs are typically created under that subtree.
        # - Copying `fluxon_test_stack/` into a run_dir that itself lives under either subtree
        #   would recursively copy historical outputs and potentially the destination into itself.
        # - Therefore the runtime source bundle excludes runner/workdir trees and keeps only code.
        shutil.copytree(
            test_stack_src,
            src_root / "fluxon_test_stack",
            dirs_exist_ok=False,
            ignore=shutil.ignore_patterns(*_TEST_STACK_RUNTIME_SOURCE_IGNORE_NAMES),
        )
        wheel_dest = (wheels_root / wheel_src.name).resolve()
        shutil.copy2(wheel_src, wheel_dest)
        if mooncake_wheel_src is not None:
            mooncake_wheel_dest = (wheels_root / mooncake_wheel_src.name).resolve()
            shutil.copy2(mooncake_wheel_src, mooncake_wheel_dest)
        # Let copytree own the wheelhouse directory creation for the initial materialization.
        shutil.copytree(wheelhouse_root_src, vendor_site_packages_root, dirs_exist_ok=False)
        if backend_kind == TEST_STACK_BACKEND_REDIS:
            if not test_rsc_root.exists():
                raise ValueError(f"missing test_rsc directory for TEST_STACK redis runtime: {test_rsc_root}")
            rc = _require_dict(
                _require_dict(profile_ts.get("runtime_config"), "resolved_case.profile.test_stack.runtime_config").get("redis"),
                "resolved_case.profile.test_stack.runtime_config.redis",
            )
            bundle_relpath_raw = rc.get("server_bundle_test_rsc_relpath")
            binary_relpath = _require_clean_relpath(
                rc.get("server_binary_test_rsc_relpath"),
                "resolved_case.profile.test_stack.runtime_config.redis.server_binary_test_rsc_relpath",
            )
            bundle_relpath = (
                _require_clean_relpath(
                    bundle_relpath_raw,
                    "resolved_case.profile.test_stack.runtime_config.redis.server_bundle_test_rsc_relpath",
                )
                if bundle_relpath_raw is not None
                else str(Path(binary_relpath).parent.as_posix())
            )
            redis_bundle_root_dest = _copy_runtime_resource_relpath_into_runtime(
                source_root=test_rsc_root,
                runtime_root=backend_root,
                relpath=bundle_relpath,
            )
            redis_binary_dest = (backend_root / binary_relpath).resolve()
            if not redis_binary_dest.exists() or not redis_binary_dest.is_file():
                raise ValueError(f"TEST_STACK redis runtime missing copied server binary: {redis_binary_dest}")
            redis_binary_dest.chmod(redis_binary_dest.stat().st_mode | 0o111)
        elif backend_kind == TEST_STACK_BACKEND_ALLUXIO:
            if not test_rsc_root.exists():
                raise ValueError(f"missing test_rsc directory for TEST_STACK alluxio runtime: {test_rsc_root}")
            rc = _require_dict(
                _require_dict(profile_ts.get("runtime_config"), "resolved_case.profile.test_stack.runtime_config").get("alluxio"),
                "resolved_case.profile.test_stack.runtime_config.alluxio",
            )
            bundle_relpath = _require_clean_relpath(
                rc.get("bundle_test_rsc_relpath"),
                "resolved_case.profile.test_stack.runtime_config.alluxio.bundle_test_rsc_relpath",
            )
            alluxio_bundle_root_dest = _copy_runtime_resource_relpath_into_runtime(
                source_root=test_rsc_root,
                runtime_root=backend_root,
                relpath=bundle_relpath,
            )
    else:
        if not runtime_root.is_dir():
            raise ValueError(f"TEST_STACK runtime_root must be a directory: {runtime_root}")
        if not src_root.exists() or not src_root.is_dir():
            raise ValueError(f"TEST_STACK runtime missing source root: {src_root}")
        if not wheels_root.exists() or not wheels_root.is_dir():
            raise ValueError(f"TEST_STACK runtime missing wheels root: {wheels_root}")
        if not vendor_site_packages_root.exists():
            shutil.copytree(wheelhouse_root_src, vendor_site_packages_root, dirs_exist_ok=False)
        elif not vendor_site_packages_root.is_dir():
            raise ValueError(
                f"TEST_STACK runtime vendor_site_packages root must be a directory: {vendor_site_packages_root}"
            )
        else:
            for wheel_path in sorted(wheelhouse_root_src.glob("*.whl")):
                dest_path = (vendor_site_packages_root / wheel_path.name).resolve()
                if dest_path.exists():
                    continue
                shutil.copy2(wheel_path, dest_path)
        if not backend_root.exists():
            backend_root.mkdir(parents=True, exist_ok=False)
        elif not backend_root.is_dir():
            raise ValueError(f"TEST_STACK runtime backend root must be a directory: {backend_root}")
        wheel_dest = (wheels_root / wheel_src.name).resolve()
        if not wheel_dest.exists():
            raise ValueError(f"TEST_STACK runtime missing wheel: {wheel_dest}")
        if mooncake_wheel_src is not None:
            mooncake_wheel_dest = (wheels_root / mooncake_wheel_src.name).resolve()
            if not mooncake_wheel_dest.exists():
                raise ValueError(f"TEST_STACK runtime missing Mooncake wheel: {mooncake_wheel_dest}")
        if backend_kind == TEST_STACK_BACKEND_REDIS:
            if not test_rsc_root.exists():
                raise ValueError(f"missing test_rsc directory for TEST_STACK redis runtime: {test_rsc_root}")
            rc = _require_dict(
                _require_dict(profile_ts.get("runtime_config"), "resolved_case.profile.test_stack.runtime_config").get("redis"),
                "resolved_case.profile.test_stack.runtime_config.redis",
            )
            bundle_relpath_raw = rc.get("server_bundle_test_rsc_relpath")
            binary_relpath = _require_clean_relpath(
                rc.get("server_binary_test_rsc_relpath"),
                "resolved_case.profile.test_stack.runtime_config.redis.server_binary_test_rsc_relpath",
            )
            bundle_relpath = (
                _require_clean_relpath(
                    bundle_relpath_raw,
                    "resolved_case.profile.test_stack.runtime_config.redis.server_bundle_test_rsc_relpath",
                )
                if bundle_relpath_raw is not None
                else str(Path(binary_relpath).parent.as_posix())
            )
            if bundle_relpath.endswith(".tar.gz"):
                redis_bundle_root_dest = (backend_root / bundle_relpath[: -len(".tar.gz")]).resolve()
            else:
                redis_bundle_root_dest = (backend_root / bundle_relpath).resolve()
            redis_binary_dest = (backend_root / binary_relpath).resolve()
            if not redis_bundle_root_dest.exists():
                raise ValueError(f"TEST_STACK runtime missing redis bundle root: {redis_bundle_root_dest}")
            if not redis_binary_dest.exists():
                raise ValueError(f"TEST_STACK runtime missing redis server binary: {redis_binary_dest}")
        elif backend_kind == TEST_STACK_BACKEND_ALLUXIO:
            if not test_rsc_root.exists():
                raise ValueError(f"missing test_rsc directory for TEST_STACK alluxio runtime: {test_rsc_root}")
            rc = _require_dict(
                _require_dict(profile_ts.get("runtime_config"), "resolved_case.profile.test_stack.runtime_config").get("alluxio"),
                "resolved_case.profile.test_stack.runtime_config.alluxio",
            )
            bundle_root_relpath = _require_clean_relpath(
                rc.get("bundle_root_relpath"),
                "resolved_case.profile.test_stack.runtime_config.alluxio.bundle_root_relpath",
            )
            alluxio_bundle_root_dest = (backend_root / bundle_root_relpath).resolve()
            if not alluxio_bundle_root_dest.exists():
                raise ValueError(f"TEST_STACK runtime missing alluxio bundle root: {alluxio_bundle_root_dest}")
    out = {
        "coordinator_script": str((src_root / "fluxon_test_stack" / "distributed_benchmark_coordinator.py").resolve()),
        "node_script": str((src_root / "fluxon_test_stack" / "distributed_benchmark_node.py").resolve()),
    }
    if redis_binary_dest is not None and redis_bundle_root_dest is not None:
        out["redis_server_binary"] = str(redis_binary_dest)
        out["redis_bundle_root"] = str(redis_bundle_root_dest)
    if alluxio_bundle_root_dest is not None:
        out["alluxio_bundle_root"] = str(alluxio_bundle_root_dest)
    if mooncake_wheel_dest is not None:
        out["mooncake_wheel"] = str(mooncake_wheel_dest)
    out["offline_dependency_requirements"] = "\n".join(offline_dependency_requirements)
    return out


def _test_stack_runtime_command(
    *,
    run_dir: Path,
    venv_python: Path,
    script_path: str,
    script_args: List[str],
    runtime_env: Dict[str, str],
    exec_wrapper_argv: Optional[List[str]] = None,
) -> str:
    assert_env_cmd = _test_stack_runtime_env_assert_command(
        venv_python=venv_python,
        require_mooncake=_test_stack_runtime_mooncake_wheel_path_opt(run_dir=run_dir) is not None,
    )
    require_unlimited_memlock = _test_stack_runtime_mooncake_wheel_path_opt(run_dir=run_dir) is not None
    vendor_site_packages = (
        run_dir / _TEST_STACK_RUNTIME_DIRNAME / _TEST_STACK_RUNTIME_VENDOR_SITE_PACKAGES_DIRNAME
    ).resolve()
    runtime_env_exports = _render_runtime_env_exports(runtime_env)
    exec_cmd = ["exec"]
    if exec_wrapper_argv is not None:
        exec_cmd.extend(_shell_quote(arg) for arg in exec_wrapper_argv)
    exec_cmd.extend(
        [
            _shell_quote(str(venv_python)),
            "-u",
            _shell_quote(script_path),
        ]
    )
    exec_cmd.extend(_shell_quote(arg) for arg in script_args)
    memlock_prelude = ""
    if require_unlimited_memlock:
        memlock_prelude = (
            "ulimit -l unlimited\n"
            + "memlock_after=\"$(ulimit -l)\"\n"
            + "if [ \"${memlock_after}\" != \"unlimited\" ]; then\n"
            + "  printf 'ERROR: ulimit -l verification failed, got: %s\\n' \"${memlock_after}\" >&2\n"
            + "  grep -i 'locked memory' /proc/self/limits >&2 || true\n"
            + "  exit 1\n"
            + "fi\n"
            + "if ! grep -Eq '^Max locked memory[[:space:]]+unlimited[[:space:]]+unlimited([[:space:]]+bytes)?[[:space:]]*$' /proc/self/limits; then\n"
            + "  printf '%s\\n' 'ERROR: /proc/self/limits did not record unlimited memlock after ulimit -l unlimited' >&2\n"
            + "  grep -i 'locked memory' /proc/self/limits >&2 || true\n"
            + "  exit 1\n"
            + "fi\n"
        )
    return (
        "set -euo pipefail\n"
        + memlock_prelude
        + "cd "
        + _shell_quote(str(run_dir.resolve()))
        + "\n"
        + "export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python\n"
        + "export PYTHONPATH="
        + _shell_quote(str(vendor_site_packages))
        + ":${PYTHONPATH:-}\n"
        + _test_stack_runtime_env_prelude_command()
        + runtime_env_exports
        + assert_env_cmd
        + "\n"
        + " ".join(exec_cmd)
        + "\n"
    )


def _test_stack_runtime_module_command(
    *,
    run_dir: Path,
    venv_python: Path,
    module_name: str,
    module_args: List[str],
    runtime_env: Dict[str, str],
    pre_exec_shell: str = "",
    exec_wrapper_argv: Optional[List[str]] = None,
) -> str:
    assert_env_cmd = _test_stack_runtime_env_assert_command(
        venv_python=venv_python,
        require_mooncake=_test_stack_runtime_mooncake_wheel_path_opt(run_dir=run_dir) is not None,
    )
    require_unlimited_memlock = _test_stack_runtime_mooncake_wheel_path_opt(run_dir=run_dir) is not None
    vendor_site_packages = (
        run_dir / _TEST_STACK_RUNTIME_DIRNAME / _TEST_STACK_RUNTIME_VENDOR_SITE_PACKAGES_DIRNAME
    ).resolve()
    runtime_env_exports = _render_runtime_env_exports(runtime_env)
    exec_cmd = ["exec"]
    if exec_wrapper_argv is not None:
        exec_cmd.extend(_shell_quote(arg) for arg in exec_wrapper_argv)
    exec_cmd.extend(
        [
            _shell_quote(str(venv_python)),
            "-u",
            "-m",
            _shell_quote(module_name),
        ]
    )
    exec_cmd.extend(_shell_quote(arg) for arg in module_args)
    memlock_prelude = ""
    if require_unlimited_memlock:
        memlock_prelude = (
            "ulimit -l unlimited\n"
            + "memlock_after=\"$(ulimit -l)\"\n"
            + "if [ \"${memlock_after}\" != \"unlimited\" ]; then\n"
            + "  printf 'ERROR: ulimit -l verification failed, got: %s\\n' \"${memlock_after}\" >&2\n"
            + "  grep -i 'locked memory' /proc/self/limits >&2 || true\n"
            + "  exit 1\n"
            + "fi\n"
            + "if ! grep -Eq '^Max locked memory[[:space:]]+unlimited[[:space:]]+unlimited([[:space:]]+bytes)?[[:space:]]*$' /proc/self/limits; then\n"
            + "  printf '%s\\n' 'ERROR: /proc/self/limits did not record unlimited memlock after ulimit -l unlimited' >&2\n"
            + "  grep -i 'locked memory' /proc/self/limits >&2 || true\n"
            + "  exit 1\n"
            + "fi\n"
        )
    return (
        "set -euo pipefail\n"
        + memlock_prelude
        + "cd "
        + _shell_quote(str(run_dir.resolve()))
        + "\n"
        + "export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python\n"
        + "export PYTHONPATH="
        + _shell_quote(str(vendor_site_packages))
        + ":${PYTHONPATH:-}\n"
        + _test_stack_runtime_env_prelude_command()
        + runtime_env_exports
        + assert_env_cmd
        + "\n"
        + pre_exec_shell
        + " ".join(exec_cmd)
        + "\n"
    )


def _test_stack_perf_wrapper_command(
    *,
    inner_command: str,
    output_dir: Path,
    perf_label: str,
    perf_config: Dict[str, Any],
) -> str:
    perf_output_dir = output_dir.resolve()
    perf_data_path = (perf_output_dir / f"{perf_label}.perf.data").resolve()
    perf_log_path = (perf_output_dir / f"{perf_label}.perf.log").resolve()
    perf_duration_seconds = _require_int(
        perf_config.get("duration_seconds"),
        "perf_config.duration_seconds",
        min_v=1,
    )
    perf_start_delay_seconds = _require_int(
        perf_config.get("start_delay_seconds"),
        "perf_config.start_delay_seconds",
        min_v=0,
    )
    perf_frequency_hz = _require_int(
        perf_config.get("frequency_hz"),
        "perf_config.frequency_hz",
        min_v=1,
    )
    perf_call_graph = _require_str(perf_config.get("call_graph"), "perf_config.call_graph")
    perf_header = (
        f"[TEST_STACK_PERF] label={perf_label} duration_seconds={perf_duration_seconds} "
        f"frequency_hz={perf_frequency_hz} call_graph={perf_call_graph} output={perf_data_path}"
    )
    return (
        "set -euo pipefail\n"
        + "mkdir -p "
        + _shell_quote(str(perf_output_dir))
        + "\n"
        + "child_pid=''\n"
        + "perf_pid=''\n"
        + "cleanup() {\n"
        + "  local status=$?\n"
        + "  if [ -n \"${perf_pid:-}\" ]; then\n"
        + "    kill \"${perf_pid}\" >/dev/null 2>&1 || true\n"
        + "    wait \"${perf_pid}\" >/dev/null 2>&1 || true\n"
        + "  fi\n"
        + "  if [ -n \"${child_pid:-}\" ]; then\n"
        + "    kill \"${child_pid}\" >/dev/null 2>&1 || true\n"
        + "    wait \"${child_pid}\" >/dev/null 2>&1 || true\n"
        + "  fi\n"
        + "  exit \"${status}\"\n"
        + "}\n"
        + "trap cleanup INT TERM\n"
        + "/bin/bash -lc "
        + _shell_quote(inner_command)
        + " &\n"
        + "child_pid=$!\n"
        + "if ! command -v perf >/dev/null 2>&1; then\n"
        + "  printf '%s\\n' "
        + _shell_quote("[TEST_STACK_PERF] perf not found on PATH; continuing without perf")
        + " >> "
        + _shell_quote(str(perf_log_path))
        + "\n"
        + "else\n"
        + "  sleep "
        + str(perf_start_delay_seconds)
        + "\n"
        + "  if kill -0 \"${child_pid}\" >/dev/null 2>&1; then\n"
        + "    printf '%s\\n' "
        + _shell_quote(perf_header)
        + " >> "
        + _shell_quote(str(perf_log_path))
        + "\n"
        + "    perf record -q -F "
        + str(perf_frequency_hz)
        + " --call-graph "
        + _shell_quote(perf_call_graph)
        + " -o "
        + _shell_quote(str(perf_data_path))
        + " -p \"${child_pid}\" -- sleep "
        + str(perf_duration_seconds)
        + " >> "
        + _shell_quote(str(perf_log_path))
        + " 2>&1 &\n"
        + "    perf_pid=$!\n"
        + "  fi\n"
        + "fi\n"
        + "if wait \"${child_pid}\"; then\n"
        + "  child_status=0\n"
        + "else\n"
        + "  child_status=$?\n"
        + "fi\n"
        + "if [ -n \"${perf_pid:-}\" ]; then\n"
        + "  if wait \"${perf_pid}\"; then\n"
        + "    perf_status=0\n"
        + "  else\n"
        + "    perf_status=$?\n"
        + "    printf '%s\\n' "
        + _shell_quote("[TEST_STACK_PERF] perf exited non-zero; benchmark process kept running")
        + " >> "
        + _shell_quote(str(perf_log_path))
        + "\n"
        + "  fi\n"
        + "fi\n"
        + "exit \"${child_status}\"\n"
    )


def _test_stack_target_host_venv_python(
    *,
    node_cfg: Dict[str, Any],
    target_name: str,
    python_abi: str,
) -> Path:
    hostworkdir = _require_str(node_cfg.get("hostworkdir"), f"cluster_nodes[{target_name}].hostworkdir").strip()
    if not hostworkdir:
        raise ValueError(f"cluster_nodes[{target_name}].hostworkdir must be non-empty")
    return _test_stack_host_venv_python_path(hostworkdir=hostworkdir, python_abi=python_abi)


def _test_stack_runtime_wheel_paths(*, run_dir: Path) -> Tuple[Path]:
    wheels_root = (run_dir / _TEST_STACK_RUNTIME_DIRNAME / _TEST_STACK_RUNTIME_WHEEL_DIRNAME).resolve()
    wheel_candidates = sorted(path for path in wheels_root.glob("fluxon-*.whl") if path.is_file())
    if len(wheel_candidates) != 1:
        raise ValueError(
            "TEST_STACK runtime must contain exactly one Fluxon wheel: "
            f"wheels_root={wheels_root} matches={[path.name for path in wheel_candidates]}"
        )
    return (wheel_candidates[0],)


def _test_stack_normalize_python_abi(raw_python_abi: str) -> str:
    python_abi = _require_str(raw_python_abi, "python_abi").strip()
    if not re.fullmatch(r"cpython[0-9]+\.[0-9]+", python_abi):
        raise ValueError(f"TEST_STACK python_abi format invalid: {python_abi!r}")
    return python_abi


def _test_stack_host_venv_dirname(*, python_abi: str) -> str:
    normalized = _test_stack_normalize_python_abi(python_abi)
    return "venv_" + normalized.replace(".", "_")


def _test_stack_host_venv_python_path(*, hostworkdir: str, python_abi: str) -> Path:
    if not hostworkdir:
        raise ValueError("hostworkdir must be non-empty")
    if not os.path.isabs(hostworkdir):
        raise ValueError(f"hostworkdir must be absolute: {hostworkdir!r}")
    return Path(hostworkdir).joinpath(
        _test_stack_host_venv_dirname(python_abi=python_abi),
        *_TEST_STACK_HOST_VENV_PYTHON_BIN_RELPATH,
    )


def _test_stack_runtime_mooncake_wheel_path_opt(*, run_dir: Path) -> Optional[Path]:
    wheels_root = (run_dir / _TEST_STACK_RUNTIME_DIRNAME / _TEST_STACK_RUNTIME_WHEEL_DIRNAME).resolve()
    candidates = sorted(wheels_root.glob("mooncake_transfer_engine*.whl"))
    if not candidates:
        return None
    if len(candidates) != 1:
        raise ValueError(
            "TEST_STACK runtime must contain at most one Mooncake wheel: "
            f"wheels_root={wheels_root} matches={[path.name for path in candidates]}"
        )
    return candidates[0]


def _test_stack_wheel_python_abi_tag(path: Path) -> str:
    match = _WHEEL_PY_TAG_RE.search(path.name)
    if match is None:
        raise ValueError(f"failed to infer python ABI tag from wheel name: {path.name}")
    major = match.group(2)
    minor = match.group(3)
    return f"cpython{major}.{minor}"


def _test_stack_runtime_env_prelude_command() -> str:
    return ""


def _test_stack_runtime_env_import_probe(*, require_mooncake: bool) -> str:
    probe = _TEST_STACK_HOST_VENV_IMPORT_PROBE_BASE
    if require_mooncake:
        probe += (
            "import mooncake.cli\n"
            "import mooncake.store\n"
        )
    return probe


def _test_stack_runtime_env_assert_command(
    *,
    venv_python: Path,
    required_python_abi: Optional[str] = None,
    require_mooncake: bool = False,
) -> str:
    abi_probe = ""
    if required_python_abi is not None:
        abi = _require_str(required_python_abi, "required_python_abi").strip()
        if not abi:
            raise ValueError("required_python_abi must be non-empty when set")
        abi_probe = (
            _shell_quote(str(venv_python))
            + " - <<'PY'\n"
            + "import sys\n"
            + f"expected = {abi!r}\n"
            + "actual = f\"cpython{sys.version_info[0]}.{sys.version_info[1]}\"\n"
            + "if actual != expected:\n"
            + "    raise SystemExit(f\"TEST_STACK runtime venv Python ABI mismatch: expected={expected} actual={actual}\")\n"
            + "PY\n"
        )
    return (
        "if [ ! -x "
        + _shell_quote(str(venv_python))
        + " ]; then\n"
        + "  echo >&2 "
        + _shell_quote(
            f"TEST_STACK runtime requires prepared hostworkdir venv: python={venv_python}"
        )
        + "\n"
        + "  exit 1\n"
        + "fi\n"
        + abi_probe
        + "(\n"
        + _test_stack_runtime_env_prelude_command()
        + _shell_quote(str(venv_python))
        + " - <<'PY'\n"
        + _test_stack_runtime_env_import_probe(require_mooncake=require_mooncake)
        + "PY\n"
        + ")\n"
    )


def _test_stack_runtime_env_prepare_command(
    *,
    venv_python: Path,
    run_dir: Path,
    offline_dependency_requirements: Tuple[str, ...],
) -> str:
    (wheel,) = _test_stack_runtime_wheel_paths(run_dir=run_dir)
    mooncake_wheel = _test_stack_runtime_mooncake_wheel_path_opt(run_dir=run_dir)
    wheelhouse_root = (
        run_dir
        / _TEST_STACK_RUNTIME_DIRNAME
        / _TEST_STACK_RUNTIME_VENDOR_SITE_PACKAGES_DIRNAME
    ).resolve()
    required_python_abi = (
        _test_stack_wheel_python_abi_tag(mooncake_wheel)
        if mooncake_wheel is not None
        else _TEST_STACK_DEFAULT_PYTHON_ABI
    )
    marker_path = (venv_python.parent.parent / _TEST_STACK_RUNTIME_WHEEL_MARKER_FILENAME).resolve()
    assert_env_cmd = _test_stack_runtime_env_assert_command(
        venv_python=venv_python,
        required_python_abi=required_python_abi,
        require_mooncake=mooncake_wheel is not None,
    )
    python_bin_name = "python" + required_python_abi.removeprefix("cpython")
    venv_root = venv_python.parent.parent
    expected_sha256_inputs = _shell_quote(str(wheel))
    mooncake_install_suffix = ""
    if mooncake_wheel is not None:
        expected_sha256_inputs += " " + _shell_quote(str(mooncake_wheel))
        mooncake_install_suffix = " " + _shell_quote(str(mooncake_wheel))
    offline_requirement_args = " ".join(_shell_quote(item) for item in offline_dependency_requirements)
    mooncake_system_prepare = ""
    if mooncake_wheel is not None:
        mooncake_system_prepare = (
            "if ! ldconfig -p | grep -F 'libibverbs.so.1' >/dev/null 2>&1; then\n"
            + "  export DEBIAN_FRONTEND=noninteractive\n"
            + "  apt-get update\n"
            + "  apt-get install -y rdma-core libibverbs1\n"
            + "fi\n"
        )
    return (
        "PYTHON_BIN=''\n"
        + "for _candidate in "
        + _shell_quote(python_bin_name)
        + " python3.10 python3; do\n"
        + "  if command -v \"${_candidate}\" >/dev/null 2>&1; then\n"
        + "    PYTHON_BIN=\"${_candidate}\"\n"
        + "    break\n"
        + "  fi\n"
        + "done\n"
        + "if [ -z \"${PYTHON_BIN}\" ]; then\n"
        + "  echo >&2 "
        + _shell_quote(
            f"TEST_STACK runtime requires {python_bin_name} on PATH to create venv: python={venv_python}"
        )
        + "\n"
        + "  exit 1\n"
        + "fi\n"
        + "if ! \"${PYTHON_BIN}\" -m ensurepip --version >/dev/null 2>&1; then\n"
        + "  export DEBIAN_FRONTEND=noninteractive\n"
        + "  apt-get update\n"
        + "  if [ \"${PYTHON_BIN}\" = \"python3\" ]; then\n"
        + "    apt-get install -y python3-venv\n"
        + "  else\n"
        + "    apt-get install -y python3-venv \"${PYTHON_BIN}-venv\"\n"
        + "  fi\n"
        + "fi\n"
        + "if [ -x "
        + _shell_quote(str(venv_python))
        + " ] && ! "
        + _shell_quote(str(venv_python))
        + " -m pip --version >/dev/null 2>&1; then\n"
        + "  rm -rf "
        + _shell_quote(str(venv_root))
        + "\n"
        + "fi\n"
        + "if [ ! -x "
        + _shell_quote(str(venv_python))
        + " ]; then\n"
        + "  rm -rf "
        + _shell_quote(str(venv_root))
        + "\n"
        + "  \"${PYTHON_BIN}\""
        + " -m venv --system-site-packages "
        + _shell_quote(str(venv_root))
        + "\n"
        + "fi\n"
        + mooncake_system_prepare
        + "if [ ! -d "
        + _shell_quote(str(wheelhouse_root))
        + " ]; then\n"
        + "  echo >&2 "
        + _shell_quote(f"TEST_STACK runtime wheelhouse is missing: {wheelhouse_root}")
        + "\n"
        + "  exit 1\n"
        + "fi\n"
        + "expected_wheel_pair_sha256=$(\n"
        + "  {\n"
        + "    sha256sum "
        + expected_sha256_inputs
        + "\n"
        + "    find "
        + _shell_quote(str(wheelhouse_root))
        + " -type f -print0 | LC_ALL=C sort -z | xargs -0r sha256sum\n"
        + "  } | sha256sum | awk '{print $1}'\n"
        + ")\n"
        + "current_wheel_pair_sha256=\"$(cat "
        + _shell_quote(str(marker_path))
        + " 2>/dev/null || true)\"\n"
        + "if [ \"$current_wheel_pair_sha256\" != \"$expected_wheel_pair_sha256\" ]; then\n"
        + "  "
        + _shell_quote(str(venv_python))
        + " -m pip install --no-index --find-links "
        + _shell_quote(str(wheelhouse_root))
        + " "
        + offline_requirement_args
        + "\n"
        + "  "
        + _shell_quote(str(venv_python))
        + " -m pip install --force-reinstall --no-deps "
        + _shell_quote(str(wheel))
        + mooncake_install_suffix
        + "\n"
        + "  printf '%s\\n' \"$expected_wheel_pair_sha256\" > "
        + _shell_quote(str(marker_path))
        + "\n"
        + "fi\n"
        + assert_env_cmd
    )


def _test_stack_instance_needs_python_runtime_env(*, instance_id: str) -> bool:
    if instance_id.startswith(TEST_STACK_REDIS_INSTANCE_ID_PREFIX):
        return False
    if instance_id.startswith(TEST_STACK_ALLUXIO_INSTANCE_ID_PREFIX):
        return False
    return True


def _test_stack_runtime_required_python_abi(*, resolved_case: Dict[str, Any], run_dir: Path) -> str:
    mooncake_wheel = _test_stack_runtime_mooncake_wheel_path_opt(run_dir=run_dir)
    if mooncake_wheel is not None:
        profile = _require_dict(resolved_case.get("profile"), "resolved_case.profile")
        profile_test_stack = _require_dict(profile.get("test_stack"), "resolved_case.profile.test_stack")
        runtime_config = _require_dict(
            profile_test_stack.get("runtime_config"),
            "resolved_case.profile.test_stack.runtime_config",
        )
        master_cfg = _require_dict(
            runtime_config.get("master"),
            "resolved_case.profile.test_stack.runtime_config.master",
        )
        configured_python_abi = _test_stack_normalize_python_abi(
            _require_str(
                master_cfg.get("python_abi"),
                "resolved_case.profile.test_stack.runtime_config.master.python_abi",
            )
        )
        wheel_python_abi = _test_stack_wheel_python_abi_tag(mooncake_wheel)
        if configured_python_abi != wheel_python_abi:
            raise ValueError(
                "TEST_STACK Mooncake python_abi must match the bundled wheel tag: "
                f"configured={configured_python_abi} wheel={wheel_python_abi} wheel_name={mooncake_wheel.name}"
            )
        return configured_python_abi
    return _TEST_STACK_DEFAULT_PYTHON_ABI


def _ensure_test_stack_runtime_env_ready_for_instance_ids(
    resolved_case: Dict[str, Any],
    *,
    run_dir: Path,
    instance_ids: Tuple[str, ...],
) -> None:
    install_instance_ids = tuple(
        instance_id
        for instance_id in instance_ids
        if _test_stack_instance_needs_python_runtime_env(instance_id=instance_id)
    )
    if not install_instance_ids:
        return

    required_python_abi = _test_stack_runtime_required_python_abi(
        resolved_case=resolved_case,
        run_dir=run_dir,
    )
    venv_dirname = _test_stack_host_venv_dirname(python_abi=required_python_abi)

    if venv_dirname not in _TEST_STACK_SHARED_VENV_SEEDED:
        print(
            f"INFO: shared hostworkdir venv will be prepared lazily on demand: venv_dirname={venv_dirname}",
            flush=True,
        )
        _TEST_STACK_SHARED_VENV_SEEDED.add(venv_dirname)

    offline_dependency_requirements = _test_stack_runtime_offline_dependency_requirements_for_resolved_case(
        resolved_case
    )

    for target_name, node_cfg, dispatch_mod, is_local in _collect_instance_target_accesses(
        resolved_case,
        instance_ids=list(install_instance_ids),
    ):
        venv_python = _test_stack_target_host_venv_python(
            node_cfg=node_cfg,
            target_name=target_name,
            python_abi=required_python_abi,
        )
        assert_env_cmd = _test_stack_runtime_env_prepare_command(
            run_dir=run_dir,
            venv_python=venv_python,
            offline_dependency_requirements=offline_dependency_requirements,
        )
        if is_local:
            _run_subprocess(["bash", "-lc", assert_env_cmd], cwd=str(run_dir))
            continue
        print(
            f"[TEST_STACK env_prepare] target={target_name} run_dir={run_dir.resolve()} "
            f"host_venv={venv_python}",
            flush=True,
        )
        _run_remote_bash(
            target_name=target_name,
            node_cfg=node_cfg,
            remote_cmd="bash -lc " + dispatch_mod.sh_quote(assert_env_cmd),
        )


def _test_stack_runner_root(run_dir: Path) -> Path:
    run_dir = run_dir.resolve()
    if len(run_dir.parents) < 3:
        raise ValueError(f"TEST_STACK run_dir is too shallow to resolve runner root: {run_dir}")
    return run_dir.parents[2]


def _test_stack_runner_port_slot(*, runner_root: Path, stride: int) -> int:
    if stride <= 0:
        raise ValueError(f"TEST_STACK coordinator port stride must be positive: {stride}")
    if stride == 1:
        return 0
    digest = hashlib.sha256(str(runner_root.resolve()).encode("utf-8")).digest()
    return int.from_bytes(digest[:2], byteorder="big", signed=False) % stride


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _overlay_ci_source_files(
    *,
    source_root: Path,
    src_root: Path,
    ci_commands: Optional[List[Dict[str, str]]],
) -> None:
    """Overlay live checkout CI sources into the isolated run workspace.

    English note:
    - `src_ci.tar.gz` provides a bounded CI source snapshot inside the materialized case release.
    - CI debug / short-circuit reruns must still execute the current checkout content, otherwise
      the run workspace can drift from the code we are validating right now.
    - Keep the overlay surface explicit and converged: only the CI-owned top-level trees are synced.
    """
    if ci_commands is not None and not isinstance(ci_commands, list):
        raise ValueError(f"ci_commands must be a list or None, got: {type(ci_commands).__name__}")

    source_root = source_root.resolve()
    src_root = src_root.resolve()
    for rel_name in _CI_SOURCE_OVERLAY_ROOTS:
        src_path = (source_root / rel_name).resolve()
        dest_path = (src_root / rel_name).resolve()
        if not src_path.exists():
            raise ValueError(f"missing CI source overlay path in checkout: {src_path}")
        if not str(dest_path).startswith(str(src_root) + os.sep):
            raise ValueError(f"unsafe overlay destination outside src_root: {dest_path}")
        if src_path.is_dir():
            shutil.copytree(
                src_path,
                dest_path,
                dirs_exist_ok=True,
                ignore=lambda dir_path, names: _ci_source_overlay_ignore_entries(
                    source_root=source_root,
                    dir_path=dir_path,
                    names=names,
                ),
            )
            continue
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_path, dest_path)


def _ci_source_overlay_ignore_entries(
    *,
    source_root: Path,
    dir_path: str,
    names: List[str],
) -> List[str]:
    ignored = {name for name in names if name in _CI_SOURCE_OVERLAY_IGNORE_NAMES}
    dir_root = Path(dir_path).resolve()
    for name in names:
        candidate = (dir_root / name).resolve()
        if not candidate.exists():
            continue
        rel_path = candidate.relative_to(source_root).as_posix()
        for skipped_root in _CI_SOURCE_OVERLAY_IGNORED_SUBTREES:
            if rel_path == skipped_root or rel_path.startswith(skipped_root + "/"):
                ignored.add(name)
                break
    return sorted(ignored)


def _artifact_set_release_artifacts(resolved_case: Dict[str, Any]) -> Dict[str, str]:
    artifact_set = _require_dict(resolved_case.get("artifact_set"), "resolved_case.artifact_set")
    release_artifacts = _require_dict(
        artifact_set.get("release_artifacts"),
        "resolved_case.artifact_set.release_artifacts",
    )
    return {
        field_name: _require_str(
            release_artifacts.get(field_name),
            f"resolved_case.artifact_set.release_artifacts.{field_name}",
        )
        for field_name in ("wheel",)
    }


def _artifact_set_test_rsc_artifacts(resolved_case: Dict[str, Any]) -> Dict[str, str]:
    artifact_set = _require_dict(resolved_case.get("artifact_set"), "resolved_case.artifact_set")
    test_rsc_artifacts = _require_dict(
        artifact_set.get("test_rsc_artifacts"),
        "resolved_case.artifact_set.test_rsc_artifacts",
    )
    return {
        field_name: _require_str(
            test_rsc_artifacts.get(field_name),
            f"resolved_case.artifact_set.test_rsc_artifacts.{field_name}",
        )
        for field_name in ("ci_src_archive", "ci_ext_rsc_archive")
    }


def _artifact_set_release_source(resolved_case: Dict[str, Any]) -> Dict[str, Any]:
    artifact_set = _require_dict(resolved_case.get("artifact_set"), "resolved_case.artifact_set")
    return _require_dict(artifact_set.get("release_source"), "resolved_case.artifact_set.release_source")


def _artifact_set_test_rsc_source(resolved_case: Dict[str, Any]) -> Dict[str, Any]:
    artifact_set = _require_dict(resolved_case.get("artifact_set"), "resolved_case.artifact_set")
    return _require_dict(artifact_set.get("test_rsc_source"), "resolved_case.artifact_set.test_rsc_source")


def _artifact_source_local_cache_root_opt(source: Dict[str, Any], *, ctx: str) -> Optional[Path]:
    raw_local_cache_root = source.get("local_cache_root")
    if raw_local_cache_root is None:
        return None
    local_cache_root = Path(
        _require_str(raw_local_cache_root, f"{ctx}.local_cache_root")
    ).expanduser()
    if not local_cache_root.is_absolute():
        raise ValueError(f"{ctx}.local_cache_root must be an absolute path")
    return local_cache_root.resolve()


def _resolved_case_release_source_key_prefix_relpath(resolved_case: Dict[str, Any]) -> str:
    source = _artifact_set_release_source(resolved_case)
    return _require_clean_relpath(
        source.get("key_prefix"),
        "resolved_case.artifact_set.release_source.key_prefix",
    )


def _resolved_case_test_rsc_source_key_prefix_relpath(resolved_case: Dict[str, Any]) -> str:
    source = _artifact_set_test_rsc_source(resolved_case)
    return _require_clean_relpath(
        source.get("key_prefix"),
        "resolved_case.artifact_set.test_rsc_source.key_prefix",
    )


def _artifact_cache_subtree_from_parent_root(
    *,
    parent_root: Path,
    key_prefix_relpath: str,
    manifest_filename: str,
    ctx: str,
) -> Path:
    # Preserve the subtree path as-is: some release caches expose the profile
    # subtree through a symlink, and resolving it would collapse back to the
    # parent cache root and hide the actual materialized manifest.
    candidate = parent_root / key_prefix_relpath
    if candidate.exists() and candidate.is_dir() and (candidate / manifest_filename).exists():
        return candidate
    raise ValueError(
        f"{ctx} does not provide the selected artifact cache subtree: "
        f"parent_root={parent_root} candidate={candidate}"
    )


def _artifact_cache_root_from_configured_source(
    *,
    configured_root: Path,
    key_prefix_relpath: str,
    manifest_filename: str,
    ctx: str,
) -> Path:
    if configured_root.exists() and configured_root.is_dir() and (configured_root / manifest_filename).exists():
        return configured_root
    return _artifact_cache_subtree_from_parent_root(
        parent_root=configured_root,
        key_prefix_relpath=key_prefix_relpath,
        manifest_filename=manifest_filename,
        ctx=ctx,
    )


def _local_release_root_override_opt() -> Optional[Path]:
    override_raw = os.environ.get(_LOCAL_RELEASE_CACHE_ROOT_OVERRIDE_ENV, "").strip()
    if not override_raw:
        return None
    return Path(override_raw).expanduser().resolve()


def _workdir_test_bed_artifacts_root_opt(resolved_case: Dict[str, Any]) -> Optional[Path]:
    runtime = _require_dict(resolved_case.get("runtime"), "resolved_case.runtime")
    workdir_root = Path(
        _require_str(runtime.get("workdir_root"), "resolved_case.runtime.workdir_root")
    ).resolve()
    candidate = (workdir_root / "testbed_bundle" / "artifacts").resolve()
    if candidate.exists() and candidate.is_dir():
        return candidate
    return None


def _local_release_cache_root_for_case(resolved_case: Dict[str, Any]) -> Path:
    override_root = _local_release_root_override_opt()
    if override_root is not None:
        # The launcher-selected parent root is the benchmark authority for both release
        # and test_rsc materialization, so reject any drift instead of silently falling
        # back to suite-configured caches.
        return _artifact_cache_subtree_from_parent_root(
            parent_root=override_root,
            key_prefix_relpath=_resolved_case_release_source_key_prefix_relpath(resolved_case),
            manifest_filename=_RELEASE_MANIFEST_FILENAME,
            ctx=_LOCAL_RELEASE_CACHE_ROOT_OVERRIDE_ENV,
        )

    configured_root = _artifact_source_local_cache_root_opt(
        _artifact_set_release_source(resolved_case),
        ctx="resolved_case.artifact_set.release_source",
    )
    if configured_root is not None:
        return _artifact_cache_root_from_configured_source(
            configured_root=configured_root,
            key_prefix_relpath=_resolved_case_release_source_key_prefix_relpath(resolved_case),
            manifest_filename=_RELEASE_MANIFEST_FILENAME,
            ctx="resolved_case.artifact_set.release_source.local_cache_root",
        )
    test_bed_artifacts_root = _workdir_test_bed_artifacts_root_opt(resolved_case)
    if test_bed_artifacts_root is not None:
        try:
            return _artifact_cache_subtree_from_parent_root(
                parent_root=test_bed_artifacts_root,
                key_prefix_relpath=_resolved_case_release_source_key_prefix_relpath(resolved_case),
                manifest_filename=_RELEASE_MANIFEST_FILENAME,
                ctx="resolved_case.runtime.workdir_root.testbed_bundle.artifacts",
            )
        except ValueError:
            pass
    base_root = (_runner_repo_root() / "fluxon_release").resolve()
    candidate = (base_root / _resolved_case_release_source_key_prefix_relpath(resolved_case)).resolve()
    if candidate.exists() and candidate.is_dir() and (candidate / _RELEASE_MANIFEST_FILENAME).exists():
        return candidate
    return base_root


def _local_test_rsc_cache_root_for_case_opt(resolved_case: Dict[str, Any]) -> Optional[Path]:
    override_root = _local_release_root_override_opt()
    if override_root is not None:
        key_prefix_name = Path(_resolved_case_test_rsc_source_key_prefix_relpath(resolved_case)).name
        return _artifact_cache_subtree_from_parent_root(
            parent_root=override_root / "test_rsc",
            key_prefix_relpath=key_prefix_name,
            manifest_filename=_TEST_RSC_MANIFEST_FILENAME,
            ctx=_LOCAL_RELEASE_CACHE_ROOT_OVERRIDE_ENV,
        )

    configured_root = _artifact_source_local_cache_root_opt(
        _artifact_set_test_rsc_source(resolved_case),
        ctx="resolved_case.artifact_set.test_rsc_source",
    )
    if configured_root is None:
        test_bed_artifacts_root = _workdir_test_bed_artifacts_root_opt(resolved_case)
        if test_bed_artifacts_root is None:
            return None
        try:
            return _artifact_cache_subtree_from_parent_root(
                parent_root=test_bed_artifacts_root,
                key_prefix_relpath=_resolved_case_test_rsc_source_key_prefix_relpath(resolved_case),
                manifest_filename=_TEST_RSC_MANIFEST_FILENAME,
                ctx="resolved_case.runtime.workdir_root.testbed_bundle.artifacts",
            )
        except ValueError:
            return None
    return _artifact_cache_root_from_configured_source(
        configured_root=configured_root,
        key_prefix_relpath=_resolved_case_test_rsc_source_key_prefix_relpath(resolved_case),
        manifest_filename=_TEST_RSC_MANIFEST_FILENAME,
        ctx="resolved_case.artifact_set.test_rsc_source.local_cache_root",
    )


def _resolved_case_release_root(resolved_case: Dict[str, Any]) -> Path:
    artifact_set = _require_dict(resolved_case.get("artifact_set"), "resolved_case.artifact_set")
    return Path(_require_str(artifact_set.get("release_root"), "resolved_case.artifact_set.release_root")).resolve()


def _resolved_case_test_rsc_root(resolved_case: Dict[str, Any]) -> Path:
    artifact_set = _require_dict(resolved_case.get("artifact_set"), "resolved_case.artifact_set")
    return Path(_require_str(artifact_set.get("test_rsc_root"), "resolved_case.artifact_set.test_rsc_root")).resolve()


def _fluxon_ops_fs_s3_base_url(resolved_case: Dict[str, Any]) -> str:
    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    controller_url = _require_str(deploy.get("controller_url"), "resolved_case.deploy.controller_url").rstrip("/")
    u = urlparse(controller_url)
    if u.scheme not in ("http", "https") or not u.netloc:
        raise ValueError(f"resolved_case.deploy.controller_url is invalid for fs_s3 proxy: {controller_url!r}")
    runtime = _require_dict(resolved_case.get("runtime"), "resolved_case.runtime")
    stack_identity = _require_dict(runtime.get("stack_identity"), "resolved_case.runtime.stack_identity")
    ops_cluster_name = _require_str(
        stack_identity.get("ops_cluster_name"),
        "resolved_case.runtime.stack_identity.ops_cluster_name",
    )
    return f"{u.scheme}://{u.netloc}/r/fs_s3/{ops_cluster_name}"


def _sigv4_hmac_sha256(key: bytes, msg: bytes) -> bytes:
    return hmac.new(key, msg, hashlib.sha256).digest()


def _sigv4_sha256_hex(msg: bytes) -> str:
    return hashlib.sha256(msg).hexdigest()


def _sigv4_derive_signing_key(secret_key: str, scope_date: str, region: str) -> bytes:
    k_date = _sigv4_hmac_sha256(("AWS4" + secret_key).encode("utf-8"), scope_date.encode("utf-8"))
    k_region = _sigv4_hmac_sha256(k_date, region.encode("utf-8"))
    k_service = _sigv4_hmac_sha256(k_region, b"s3")
    return _sigv4_hmac_sha256(k_service, b"aws4_request")


def _sigv4_headers_for_unsigned_get(
    *,
    request_path: str,
    host: str,
    access_key: str,
    secret_key: str,
    region: str,
) -> Dict[str, str]:
    amz_now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = amz_now.strftime("%Y%m%dT%H%M%SZ")
    scope_date = amz_now.strftime("%Y%m%d")
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    payload_hash = "UNSIGNED-PAYLOAD"
    canonical_headers = (
        f"host:{host}\n"
        f"x-amz-content-sha256:{payload_hash}\n"
        f"x-amz-date:{amz_date}\n"
    )
    canonical_request = "\n".join(
        [
            "GET",
            request_path,
            "",
            canonical_headers,
            signed_headers,
            payload_hash,
        ]
    )
    scope = f"{scope_date}/{region}/s3/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            scope,
            _sigv4_sha256_hex(canonical_request.encode("utf-8")),
        ]
    )
    signing_key = _sigv4_derive_signing_key(secret_key, scope_date, region)
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    return {
        "Authorization": (
            "AWS4-HMAC-SHA256 "
            f"Credential={access_key}/{scope}, "
            f"SignedHeaders={signed_headers}, "
            f"Signature={signature}"
        ),
        "Host": host,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }


def _download_http_file(
    *,
    url: str,
    out_path: Path,
    expected_sha256: Optional[str],
    headers: Optional[Dict[str, str]] = None,
) -> None:
    if out_path.exists():
        raise ValueError(f"download output already exists (no overwrite): {out_path}")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    # English note:
    # - Download is critical-path for "prebuilt artifacts only".
    # - The fs_s3 proxy can transiently stall; a single 30s socket timeout would otherwise fail the whole case
    #   with an unhelpful "TimeoutError: timed out".
    # - Retry within a bounded window. This is not a fallback for configuration errors: sha256 mismatch and
    #   HTTP non-transient failures still fail fast.
    deadline = time.time() + HTTP_DOWNLOAD_RETRY_DEADLINE_SECONDS
    last_err: Optional[Exception] = None
    while True:
        if tmp.exists():
            tmp.unlink()

        h = hashlib.sha256()
        req = urllib.request.Request(url, method="GET")
        if headers is not None:
            for key, value in headers.items():
                req.add_header(key, value)
        try:
            with urllib.request.urlopen(req, timeout=HTTP_DOWNLOAD_ATTEMPT_TIMEOUT_SECONDS) as resp:
                with tmp.open("wb") as f:
                    while True:
                        b = resp.read(1024 * 1024)
                        if not b:
                            break
                        f.write(b)
                        h.update(b)
        except urllib.error.HTTPError as exc:
            if int(exc.code) not in CONTROLLER_STATUS_TRANSIENT_HTTP_CODES:
                raise
            last_err = exc
            tmp.unlink(missing_ok=True)
            if time.time() >= deadline:
                raise ValueError(f"http download transient retry deadline exceeded: url={url} out={out_path} err={last_err}") from last_err
            time.sleep(1.0)
            continue
        except (TimeoutError, urllib.error.URLError, ConnectionResetError, OSError) as exc:
            last_err = exc
            tmp.unlink(missing_ok=True)
            if time.time() >= deadline:
                raise ValueError(f"http download transient retry deadline exceeded: url={url} out={out_path} err={last_err}") from last_err
            time.sleep(1.0)
            continue

        got = h.hexdigest()
        if expected_sha256 is not None and got != expected_sha256:
            tmp.unlink(missing_ok=True)
            raise ValueError(f"sha256 mismatch: url={url} expected={expected_sha256} got={got}")
        tmp.replace(out_path)
        return


def _download_fluxon_ops_fs_s3_file(
    *,
    resolved_case: Dict[str, Any],
    bucket: str,
    key: str,
    access_key: str,
    secret_key: str,
    region: str,
    out_path: Path,
    expected_sha256: Optional[str],
) -> None:
    if _CONTROLLER_BASIC_AUTH_HEADER is None:
        raise RuntimeError("controller_basic_auth is not initialized")
    base_url = _fluxon_ops_fs_s3_base_url(resolved_case)
    u = urlparse(base_url)
    if u.scheme not in ("http", "https") or not u.netloc or not u.path:
        raise ValueError(f"invalid ops fs s3 base url: {base_url!r}")
    bucket_enc = urllib.parse.quote(bucket, safe="-_.~")
    key_enc = urllib.parse.quote(key, safe="/-_.~")
    request_path = u.path.rstrip("/") + "/" + bucket_enc + "/" + key_enc
    url = f"{u.scheme}://{u.netloc}{request_path}"
    headers = _sigv4_headers_for_unsigned_get(
        request_path=request_path,
        host=u.netloc,
        access_key=access_key,
        secret_key=secret_key,
        region=region,
    )
    headers[_CONTROLLER_BASIC_AUTH_HEADER_NAME] = _CONTROLLER_BASIC_AUTH_HEADER
    _download_http_file(
        url=url,
        out_path=out_path,
        expected_sha256=expected_sha256,
        headers=headers,
    )

def _extract_release_invariant_dirs_from_tarballs(release_root: Path) -> None:
    ext_images_dir = release_root / "ext_images"
    if ext_images_dir.exists():
        if not ext_images_dir.is_dir():
            raise ValueError(f"materialized release invariant path must be a directory: {ext_images_dir}")
        return
    ext_images_tarball = release_root / "ext_images.tar.gz"
    if not ext_images_tarball.exists():
        raise ValueError(
            "materialized release is missing both ext_images/ and ext_images.tar.gz: "
            f"{release_root}"
        )
    _safe_extract_tar_gz(archive_path=ext_images_tarball, dest_dir=release_root)
    if not ext_images_dir.exists() or not ext_images_dir.is_dir():
        raise ValueError(f"ext_images.tar.gz did not materialize ext_images/: {ext_images_dir}")


def _require_release_invariant_runtime(release_root: Path) -> None:
    for relpath in _RELEASE_INVARIANT_FILE_RELPATHS:
        path = release_root / relpath
        if not path.exists() or not path.is_file():
            raise ValueError(f"materialized release is missing invariant file: {path}")
    for relpath in _RELEASE_INVARIANT_DIR_RELPATHS:
        path = release_root / relpath
        if not path.exists() or not path.is_dir():
            raise ValueError(f"materialized release is missing invariant directory: {path}")


def _ensure_case_release_ready(resolved_case: Dict[str, Any]) -> None:
    release_root = _resolved_case_release_root(resolved_case)
    manifest_path = release_root / _RELEASE_MANIFEST_FILENAME
    if manifest_path.exists():
        manifest = _parse_sha256_manifest(manifest_path.read_text(encoding="utf-8"))
        _validate_release_manifest_integrity(
            release_root=release_root,
            manifest=manifest,
            ctx="materialized case release",
        )
        _require_release_invariant_runtime(release_root)
        return
    if release_root.exists():
        raise ValueError(f"case release_root exists without manifest (no overwrite): {release_root}")

    local_cache_root = _local_release_cache_root_for_case(resolved_case)
    local_cache_manifest_path = local_cache_root / _RELEASE_MANIFEST_FILENAME
    if local_cache_manifest_path.exists():
        local_cache_manifest = _parse_sha256_manifest(local_cache_manifest_path.read_text(encoding="utf-8"))
        _validate_release_manifest_integrity(
            release_root=local_cache_root,
            manifest=local_cache_manifest,
            ctx="local release cache",
        )
        _require_release_invariant_runtime(local_cache_root)
        release_artifacts = _artifact_set_release_artifacts(resolved_case)
        for name in release_artifacts.values():
            if name not in local_cache_manifest:
                raise ValueError(
                    f"local release cache manifest missing artifact declared by artifact_set: {name}"
                )
        release_root.parent.mkdir(parents=True, exist_ok=True)
        os.symlink(str(local_cache_root), str(release_root), target_is_directory=True)
        manifest = _parse_sha256_manifest((release_root / _RELEASE_MANIFEST_FILENAME).read_text(encoding="utf-8"))
        _validate_release_manifest_integrity(
            release_root=release_root,
            manifest=manifest,
            ctx="materialized case release",
        )
        _require_release_invariant_runtime(release_root)
        return

    source = _artifact_set_release_source(resolved_case)
    kind = _require_str(source.get("kind"), "resolved_case.artifact_set.release_source.kind")
    if kind == ARTIFACT_SOURCE_KIND_FLUXON_OPS_FS_S3:
        bucket = _require_str(source.get("bucket"), "resolved_case.artifact_set.release_source.bucket")
        access_key = _require_str(source.get("access_key"), "resolved_case.artifact_set.release_source.access_key")
        secret_key = _require_str(source.get("secret_key"), "resolved_case.artifact_set.release_source.secret_key")
        region = _require_str(source.get("region"), "resolved_case.artifact_set.release_source.region")
        key_prefix = _require_str(source.get("key_prefix"), "resolved_case.artifact_set.release_source.key_prefix")
        release_root.mkdir(parents=True, exist_ok=False)
        manifest_path = release_root / _RELEASE_MANIFEST_FILENAME
        _download_fluxon_ops_fs_s3_file(
            resolved_case=resolved_case,
            bucket=bucket,
            key=f"{key_prefix}/{_RELEASE_MANIFEST_FILENAME}",
            access_key=access_key,
            secret_key=secret_key,
            region=region,
            out_path=manifest_path,
            expected_sha256=None,
        )
        manifest = _parse_sha256_manifest(manifest_path.read_text(encoding="utf-8"))
        for relpath, sha256_hex in manifest.items():
            _download_fluxon_ops_fs_s3_file(
                resolved_case=resolved_case,
                bucket=bucket,
                key=f"{key_prefix}/{relpath}",
                access_key=access_key,
                secret_key=secret_key,
                region=region,
                out_path=release_root / relpath,
                expected_sha256=sha256_hex,
            )
        for relpath in _RELEASE_INVARIANT_FILE_RELPATHS:
            _download_fluxon_ops_fs_s3_file(
                resolved_case=resolved_case,
                bucket=bucket,
                key=f"{key_prefix}/{relpath}",
                access_key=access_key,
                secret_key=secret_key,
                region=region,
                out_path=release_root / relpath,
                expected_sha256=None,
            )
        _extract_release_invariant_dirs_from_tarballs(release_root)
    else:
        raise ValueError(
            "resolved_case.artifact_set.release_source.kind must be "
            f"{ARTIFACT_SOURCE_KIND_FLUXON_OPS_FS_S3!r} (LOCAL_DIR has been removed), got: {kind!r}"
        )

    manifest = _parse_sha256_manifest((release_root / _RELEASE_MANIFEST_FILENAME).read_text(encoding="utf-8"))
    _validate_release_manifest_integrity(
        release_root=release_root,
        manifest=manifest,
        ctx="materialized case release",
    )
    _require_release_invariant_runtime(release_root)


def _ensure_case_test_rsc_ready(resolved_case: Dict[str, Any]) -> None:
    test_rsc_root = _resolved_case_test_rsc_root(resolved_case)
    manifest_path = test_rsc_root / _TEST_RSC_MANIFEST_FILENAME
    if manifest_path.exists():
        manifest = _parse_sha256_manifest(manifest_path.read_text(encoding="utf-8"))
        _validate_release_manifest_integrity(
            release_root=test_rsc_root,
            manifest=manifest,
            ctx="materialized case test_rsc",
        )
        return
    if test_rsc_root.exists():
        raise ValueError(f"case test_rsc_root exists without manifest (no overwrite): {test_rsc_root}")

    local_cache_root = _local_test_rsc_cache_root_for_case_opt(resolved_case)
    if local_cache_root is not None:
        local_cache_manifest_path = local_cache_root / _TEST_RSC_MANIFEST_FILENAME
        local_cache_manifest = _parse_sha256_manifest(local_cache_manifest_path.read_text(encoding="utf-8"))
        _validate_release_manifest_integrity(
            release_root=local_cache_root,
            manifest=local_cache_manifest,
            ctx="local test_rsc cache",
        )
        test_rsc_root.parent.mkdir(parents=True, exist_ok=True)
        os.symlink(str(local_cache_root), str(test_rsc_root), target_is_directory=True)
        manifest = _parse_sha256_manifest((test_rsc_root / _TEST_RSC_MANIFEST_FILENAME).read_text(encoding="utf-8"))
        _validate_release_manifest_integrity(
            release_root=test_rsc_root,
            manifest=manifest,
            ctx="materialized case test_rsc",
        )
        return

    source = _artifact_set_test_rsc_source(resolved_case)
    kind = _require_str(source.get("kind"), "resolved_case.artifact_set.test_rsc_source.kind")
    if kind != ARTIFACT_SOURCE_KIND_FLUXON_OPS_FS_S3:
        raise ValueError(
            "resolved_case.artifact_set.test_rsc_source.kind must be "
            f"{ARTIFACT_SOURCE_KIND_FLUXON_OPS_FS_S3!r}, got: {kind!r}"
        )

    bucket = _require_str(source.get("bucket"), "resolved_case.artifact_set.test_rsc_source.bucket")
    access_key = _require_str(source.get("access_key"), "resolved_case.artifact_set.test_rsc_source.access_key")
    secret_key = _require_str(source.get("secret_key"), "resolved_case.artifact_set.test_rsc_source.secret_key")
    region = _require_str(source.get("region"), "resolved_case.artifact_set.test_rsc_source.region")
    key_prefix = _require_str(source.get("key_prefix"), "resolved_case.artifact_set.test_rsc_source.key_prefix")
    test_rsc_root.mkdir(parents=True, exist_ok=False)
    _download_fluxon_ops_fs_s3_file(
        resolved_case=resolved_case,
        bucket=bucket,
        key=f"{key_prefix}/{_TEST_RSC_MANIFEST_FILENAME}",
        access_key=access_key,
        secret_key=secret_key,
        region=region,
        out_path=manifest_path,
        expected_sha256=None,
    )
    manifest = _parse_sha256_manifest(manifest_path.read_text(encoding="utf-8"))
    for relpath, sha256_hex in manifest.items():
        _download_fluxon_ops_fs_s3_file(
            resolved_case=resolved_case,
            bucket=bucket,
            key=f"{key_prefix}/{relpath}",
            access_key=access_key,
            secret_key=secret_key,
            region=region,
            out_path=test_rsc_root / relpath,
            expected_sha256=sha256_hex,
        )
    _validate_release_manifest_integrity(
        release_root=test_rsc_root,
        manifest=manifest,
        ctx="materialized case test_rsc",
    )

def _ci_prepare_run_inputs(
    *,
    resolved_case: Dict[str, Any],
    source_root: Path,
    release_root: Path,
    test_rsc_root: Path,
    src_root: Path,
    venv_python: Path,
    ci_commands: Optional[List[Dict[str, str]]],
    overlay_live_checkout: bool,
) -> None:
    """Materialize CI run inputs from the case release into an isolated run_dir.

    Inputs are explicit release artifacts (verified by sha256 manifest) to keep the CI surface small:
    - wheels: fluxon + fluxon_pyo3
    - src_ci.tar.gz: source snapshot only (setup_and_pack/ + fluxon_py/ + fluxon_rs + sockudo-ws)

    When `overlay_live_checkout` is true, CI-owned source trees are overlaid from source_root after
    extraction so debug/test execution matches the current checkout. Distributed/staged runs can set
    this false to remain fully pinned to prepared artifacts.
    """
    if not venv_python.exists():
        raise ValueError(f"missing venv python: {venv_python}")
    if not source_root.exists() or not source_root.is_dir():
        raise ValueError(f"source_root must be an existing directory: {source_root}")
    if not release_root.exists() or not release_root.is_dir():
        raise ValueError(f"materialized release_root must exist before CI prepare: {release_root}")
    if not test_rsc_root.exists() or not test_rsc_root.is_dir():
        raise ValueError(f"materialized test_rsc_root must exist before CI prepare: {test_rsc_root}")
    if src_root.exists():
        raise ValueError(f"src_root already exists (no overwrite): {src_root}")
    src_root.mkdir(parents=True, exist_ok=False)

    manifest_path = release_root / "fluxon_release.sha256"
    if not manifest_path.exists():
        raise ValueError(f"materialized release manifest is missing: {manifest_path}")
    manifest = _parse_sha256_manifest(manifest_path.read_text(encoding="utf-8", errors="replace"))
    release_artifacts = _artifact_set_release_artifacts(resolved_case)
    wheel_name = release_artifacts["wheel"]
    test_rsc_manifest_path = test_rsc_root / _TEST_RSC_MANIFEST_FILENAME
    if not test_rsc_manifest_path.exists():
        raise ValueError(f"materialized test_rsc manifest is missing: {test_rsc_manifest_path}")
    test_rsc_manifest = _parse_sha256_manifest(test_rsc_manifest_path.read_text(encoding="utf-8", errors="replace"))
    test_rsc_artifacts = _artifact_set_test_rsc_artifacts(resolved_case)
    ci_src_archive = test_rsc_artifacts["ci_src_archive"]

    if wheel_name not in manifest:
        raise ValueError(f"missing required release artifact in sha256 manifest: {wheel_name}")

    if ci_src_archive not in test_rsc_manifest:
        raise ValueError(f"missing required test_rsc artifact in sha256 manifest: {ci_src_archive}")

    out_path = release_root / wheel_name
    if not out_path.exists():
        raise ValueError(f"materialized release artifact is missing: {out_path}")
    got_sha256 = _sha256_file(out_path)
    if got_sha256 != manifest[wheel_name]:
        raise ValueError(
            f"materialized release artifact sha256 mismatch: file={out_path} "
            f"expected={manifest[wheel_name]} got={got_sha256}"
        )

    ci_src_archive_path = test_rsc_root / ci_src_archive
    if not ci_src_archive_path.exists():
        raise ValueError(f"materialized test_rsc artifact is missing: {ci_src_archive_path}")
    got_sha256 = _sha256_file(ci_src_archive_path)
    if got_sha256 != test_rsc_manifest[ci_src_archive]:
        raise ValueError(
            f"materialized test_rsc artifact sha256 mismatch: file={ci_src_archive_path} "
            f"expected={test_rsc_manifest[ci_src_archive]} got={got_sha256}"
        )

    _safe_extract_tar_gz(archive_path=ci_src_archive_path, dest_dir=src_root)
    if overlay_live_checkout:
        _overlay_ci_source_files(source_root=source_root, src_root=src_root, ci_commands=ci_commands)

    # Reconstruct the repo-root-visible runtime artifacts/config view expected by existing CI tests.
    build_config_ext_path = src_root / "build_config_ext.yml"
    if not build_config_ext_path.exists():
        build_config_ext_path.write_text("", encoding="utf-8")
    release_link_path = src_root / "fluxon_release"
    if release_link_path.exists():
        raise ValueError(f"src runtime release path already exists (no overwrite): {release_link_path}")
    os.symlink(str(release_root), str(release_link_path), target_is_directory=True)
    test_rsc_link_path = release_link_path / "test_rsc"
    if test_rsc_link_path.exists():
        raise ValueError(f"src runtime test_rsc path already exists (no overwrite): {test_rsc_link_path}")
    os.symlink(str(test_rsc_root), str(test_rsc_link_path), target_is_directory=True)

    wheel = release_root / wheel_name
    _run_subprocess(
        [
            str(venv_python),
            "-m",
            "pip",
            "install",
            "--force-reinstall",
            str(wheel),
        ],
        cwd=str(src_root),
    )
def _write_ci_build_config_ext(
    resolved_case: Dict[str, Any], *, out_dir: Path
) -> None:
    etcd_ip = _ci_base_runtime_service_target_ip(resolved_case, service_id="etcd")
    etcd_port = _ci_base_runtime_service_port(resolved_case, service_id="etcd")
    greptime_ip = _ci_base_runtime_service_target_ip(resolved_case, service_id="greptime")
    greptime_port = _ci_base_runtime_service_port(resolved_case, service_id="greptime")

    cfg = {
        "etcd": f"{etcd_ip}:{etcd_port}",
        "prom": f"http://{greptime_ip}:{greptime_port}/v1/prometheus",
        "prom_remote_write_url": f"http://{greptime_ip}:{greptime_port}/v1/prometheus/write",
    }

    out_path = out_dir / "build_config_ext.yml"
    _write_yaml_file(out_path, cfg)


def _write_ci_master_owner_configs(
    resolved_case: Dict[str, Any], *, run_dir: Path, cluster_name: str, share_mem_path: str, share_file_path: str, owner_dram_bytes: int
) -> tuple[Path, Path]:
    master_cfg = {
        "etcd_endpoints": ["__ETCD__"],
        "cluster_name": cluster_name,
        "instance_key": "ci_master",
        "port": 50052,
        "monitoring": {
            "prometheus_base_url": "__PROM_BASE__",
            "prom_remote_write_url": ["__PROM_WRITE__"],
            "otlp_log_api": {
                "otlp_endpoint": "__OTLP_LOG_API__",
                "db_name": "public",
                "table_name": "fluxon_logs",
            },
        },
        # CI nodes often expose multiple NICs (docker bridges, libvirt, etc.).
        # Keep the whitelist strict, but include every cluster-member host that must join this case.
        "network": {"subnet_whitelist": []},
        "log_dir": str((run_dir / "services" / "master_logs").resolve()),
    }

    owner_cfg = {
        "instance_key": "ci_owner_0",
        "contribute_to_cluster_pool_size": {"dram": owner_dram_bytes, "vram": {}},
        "fluxonkv_spec": {
            "etcd_addresses": ["__ETCD__"],
            "cluster_name": cluster_name,
            "shared_memory_path": share_mem_path,
            "shared_file_path": share_file_path,
            "sub_cluster": FLUXON_KV_OWNER_SUB_CLUSTER,
        },
    }

    etcd_ip = _ci_base_runtime_service_target_ip(resolved_case, service_id="etcd")
    etcd_port = _ci_base_runtime_service_port(resolved_case, service_id="etcd")
    greptime_ip = _ci_base_runtime_service_target_ip(resolved_case, service_id="greptime")
    greptime_port = _ci_base_runtime_service_port(resolved_case, service_id="greptime")

    etcd_addr = f"{etcd_ip}:{etcd_port}"
    prom_base = f"http://{greptime_ip}:{greptime_port}/v1/prometheus"
    prom_write = f"http://{greptime_ip}:{greptime_port}/v1/prometheus/write"
    otlp_log_api = f"http://{greptime_ip}:{greptime_port}/v1/otlp/v1/logs"
    member_whitelist = [f"{ip}/32" for ip in _ci_cluster_member_target_ips(resolved_case)]

    master_cfg["etcd_endpoints"] = [etcd_addr]
    master_cfg["monitoring"]["prometheus_base_url"] = prom_base
    master_cfg["monitoring"]["prom_remote_write_url"] = [prom_write]
    master_cfg["monitoring"]["otlp_log_api"]["otlp_endpoint"] = otlp_log_api
    master_cfg["network"]["subnet_whitelist"] = member_whitelist

    owner_cfg["fluxonkv_spec"]["etcd_addresses"] = [etcd_addr]

    cfg_dir = run_dir / "configs"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    master_path = cfg_dir / "ci_master.yaml"
    owner_path = cfg_dir / "ci_owner_0.yaml"
    _write_yaml_file(master_path, master_cfg)
    _write_yaml_file(owner_path, owner_cfg)
    return master_path, owner_path


def _ci_cluster_member_target_ips(resolved_case: Dict[str, Any]) -> List[str]:
    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    target_ip_map = _require_dict(deploy.get("target_ip_map"), "resolved_case.deploy.target_ip_map")
    ordered_ips: List[str] = []
    seen: set[str] = set()
    for instance_id in CI_CLUSTER_MEMBER_INSTANCE_IDS:
        inst = _find_deploy_instance(resolved_case, instance_id=instance_id)
        deployer = _require_dict(inst.get("deployer"), f"{instance_id}.deployer")
        target_name = _require_str(deployer.get("target"), f"{instance_id}.target")
        target_ip = _require_str(target_ip_map.get(target_name), f"resolved_case.deploy.target_ip_map[{target_name!r}]")
        if target_ip in seen:
            continue
        seen.add(target_ip)
        ordered_ips.append(target_ip)
    return ordered_ips



def _resolved_ci_command_list(resolved_case: Dict[str, Any]) -> List[Dict[str, str]]:
    scene = _require_dict(resolved_case.get("scene"), "resolved_case.scene")
    ci = _require_dict(scene.get("ci"), "resolved_case.scene.ci")
    raw_commands = _require_list(ci.get("commands"), "resolved_case.scene.ci.commands")
    if not raw_commands:
        raise ValueError("resolved_case.scene.ci.commands must be non-empty")
    commands: List[Dict[str, str]] = []
    for i, raw_command in enumerate(raw_commands):
        command = _require_dict(raw_command, f"resolved_case.scene.ci.commands[{i}]")
        _forbid_unknown_keys(command, {"id", "command", "test_id", "timeout_seconds"}, f"resolved_case.scene.ci.commands[{i}]")
        rec: Dict[str, Any] = {"id": _require_str(command.get("id"), f"resolved_case.scene.ci.commands[{i}].id"), "command": _require_str(command.get("command"), f"resolved_case.scene.ci.commands[{i}].command")}
        if command.get("test_id") is not None:
            rec["test_id"] = _require_str(command.get("test_id"), f"resolved_case.scene.ci.commands[{i}].test_id")
        if command.get("timeout_seconds") is not None:
            rec["timeout_seconds"] = _require_int(
                command.get("timeout_seconds"),
                f"resolved_case.scene.ci.commands[{i}].timeout_seconds",
                min_v=1,
            )
        commands.append(rec)
    return commands


def _ci_command_contract_from_planned(ci_commands: Optional[List[Dict[str, Any]]], *, ctx: str) -> List[Dict[str, Any]]:
    if ci_commands is None or not ci_commands:
        raise ValueError(f"{ctx} must be a non-empty list")
    contract: List[Dict[str, Any]] = []
    for index, raw_command in enumerate(ci_commands):
        command = _require_dict(raw_command, f"{ctx}[{index}]")
        rec: Dict[str, Any] = {
            "id": _require_str(command.get("id"), f"{ctx}[{index}].id"),
        }
        if command.get("test_id") is not None:
            rec["test_id"] = _require_str(command.get("test_id"), f"{ctx}[{index}].test_id")
        if command.get("timeout_seconds") is not None:
            rec["timeout_seconds"] = _require_int(
                command.get("timeout_seconds"),
                f"{ctx}[{index}].timeout_seconds",
                min_v=1,
            )
        contract.append(rec)
    return contract


def _ci_command_contract_from_resolved_case(resolved_case: Dict[str, Any]) -> List[Dict[str, Any]]:
    contract: List[Dict[str, Any]] = []
    for index, command in enumerate(_resolved_ci_command_list(resolved_case)):
        rec: Dict[str, Any] = {
            "id": _require_str(command.get("id"), f"resolved_case.scene.ci.commands[{index}].id"),
        }
        if command.get("test_id") is not None:
            rec["test_id"] = _require_str(
                command.get("test_id"),
                f"resolved_case.scene.ci.commands[{index}].test_id",
            )
        if command.get("timeout_seconds") is not None:
            rec["timeout_seconds"] = _require_int(
                command.get("timeout_seconds"),
                f"resolved_case.scene.ci.commands[{index}].timeout_seconds",
                min_v=1,
            )
        contract.append(rec)
    return contract


def _ci_runner_exit_code_timeout_seconds(resolved_case: Dict[str, Any]) -> int:
    """Derive CI runner wait timeout from the concrete case plan.

    English note:
    - The controller waits for a single remote `ci_runner` process that executes the full command list sequentially.
    - A fixed global cap diverges from `ci_runner.sh` as soon as suite config changes (for example, raising a
      command timeout from 300s to 10800s), and then the control plane can falsely fail a still-running test.
    - Keep one causal source of truth: sum the explicit per-command timeouts and add the bounded pre-command phases
      that are also encoded in the generated script.
    """
    timeout_seconds = (
        CI_RUNNER_SHARED_BUNDLE_TIMEOUT_S
        + CI_RUNNER_READINESS_PROBE_DEADLINE_S
        + CI_RUNNER_EXIT_CODE_GRACE_TIMEOUT_S
    )
    commands = _resolved_ci_command_list(resolved_case)
    for index, command in enumerate(commands):
        raw_timeout = command.get("timeout_seconds")
        if raw_timeout is None:
            raise ValueError(
                f"resolved_case.scene.ci.commands[{index}].timeout_seconds must be set for deployed ci_runner cases"
            )
        timeout_seconds += _require_int(
            raw_timeout,
            f"resolved_case.scene.ci.commands[{index}].timeout_seconds",
            min_v=1,
        )
    return int(timeout_seconds)


def _write_ci_runner_script(
    resolved_case: Dict[str, Any],
    *,
    run_dir: Path,
    src_root: Path,
    share_mem_path: str,
    share_file_path: str,
) -> Path:
    commands = _resolved_ci_command_list(resolved_case)
    venv_python = run_dir / "venv" / "bin" / "python3"
    test_backend = (src_root / "fluxon_py" / "tests" / "test_backend.py").resolve()
    requires_owner_shared_bundle = _ci_has_instance(resolved_case, instance_id="owner_0")

    out_path = run_dir / "ci_runner.sh"
    if out_path.exists():
        raise ValueError(f"ci_runner.sh already exists (no overwrite): {out_path}")

    cmd_lines: list[str] = []
    for idx, command in enumerate(commands, start=1):
        step_label = _command_step_label(command)
        cmd = _subst_runtime_tokens(resolved_case, command["command"])
        timeout_seconds = command.get("timeout_seconds")
        if timeout_seconds is not None:
            timeout_seconds = _require_int(timeout_seconds, "ci.command.timeout_seconds", min_v=1)
        cmd_lines.append("echo")
        cmd_lines.append(f"echo {_shell_quote('=' * 80)}")
        cmd_lines.append(f"echo {_shell_quote(f'STEP {idx}: {step_label} :: {cmd}')}")
        cmd_lines.append(f"echo {_shell_quote('=' * 80)}")
        if timeout_seconds is None:
            cmd_lines.append(f"{cmd}")
        else:
            # English note:
            # - CI commands are standalone scripts that can hang on distributed timeouts.
            # - Timeout must be configured explicitly per command in suite config (no hidden defaults).
            cmd_lines.append(f"timeout --preserve-status --signal=KILL {int(timeout_seconds)} {cmd}")
        cmd_lines.append("rc=$?")
        cmd_lines.append('if [ "$rc" -ne 0 ]; then')
        cmd_lines.append('  echo "[ci_runner] FAILED rc=$rc"')
        cmd_lines.append('  fail_and_exit "$rc"')
        cmd_lines.append("fi")

    cmd_block = "\n".join(cmd_lines)

    shared_bundle_block = ""
    readiness_probe_block = ""
    if requires_owner_shared_bundle:
        bundle_cluster_name = _ci_cluster_name(resolved_case)
        bundle_shared_memory_dir = str(
            _cluster_scoped_shared_dir(root_path=share_mem_path, cluster_name=bundle_cluster_name)
        )
        bundle_shared_file_dir = str(
            _cluster_scoped_shared_dir(root_path=share_file_path, cluster_name=bundle_cluster_name)
        )
        shared_bundle_block = f"""
echo "[ci_runner] waiting for owner shared bundle..."
deadline=$(( $(date +%s) + {CI_RUNNER_SHARED_BUNDLE_TIMEOUT_S} ))
shm={bundle_shared_memory_dir}
shared_file={bundle_shared_file_dir}
while [ $(date +%s) -lt "$deadline" ]; do
  if [ -f "$shared_file/shared.json" ] && [ -f "$shm/mmap.file" ]; then
    echo "[ci_runner] owner shared bundle ready"
    break
  fi
  sleep 1
done
if [ ! -f "$shared_file/shared.json" ] || [ ! -f "$shm/mmap.file" ]; then
  echo "[ci_runner] ERROR: owner shared bundle not ready in {CI_RUNNER_SHARED_BUNDLE_TIMEOUT_S}s"
  echo "[ci_runner] shm=$shm"
  echo "[ci_runner] shared_file=$shared_file"
  ls -la "$shm"
  ls -la "$shared_file"
  fail_and_exit 2
fi
"""
        readiness_probe_block = f"""
echo "[ci_runner] running backend readiness probe..."
readiness_rc=1
readiness_attempt=0
readiness_deadline=$(( $(date +%s) + {CI_RUNNER_READINESS_PROBE_DEADLINE_S} ))
while [ $(date +%s) -lt "$readiness_deadline" ]; do
  readiness_attempt=$((readiness_attempt + 1))
  echo "[CI readiness_probe] attempt=$readiness_attempt argv={venv_python.as_posix()} -u {test_backend.as_posix()} --test-id basic_put_and_get --instance-suffix readiness_probe"
  timeout --preserve-status --signal=KILL 60 {venv_python.as_posix()} -u {test_backend.as_posix()} --test-id basic_put_and_get --instance-suffix readiness_probe
  readiness_rc=$?
  if [ "$readiness_rc" -eq 0 ]; then
    echo "[CI readiness_probe] basic_put_and_get passed on attempt=$readiness_attempt"
    break
  fi
  echo "[CI readiness_probe] failed rc=$readiness_rc attempt=$readiness_attempt"
  sleep 2
done
if [ "$readiness_rc" -ne 0 ]; then
  echo "[ci_runner] ERROR: backend readiness probe failed rc=$readiness_rc"
  fail_and_exit 3
fi
"""

    script = f"""#!/usr/bin/env bash
	# Deployer starts `bash -lc`, which may export `errexit` via SHELLOPTS into this script.
	# CI runner must capture per-command rc explicitly to decide PASS/FAIL and to write exit_code.
	set +e
	set -uo pipefail
	# Treat SIGHUP as non-fatal for CI. Some environments deliver HUP to managed jobs
	# (e.g. session/control-plane restarts). Without this, CI can fail with exit_code=-1
	# mid-test without producing a meaningful failure log.
	trap "" HUP
	hold_pid=""
	# TERM/INT are used by the controller to stop the job; treat them as clean shutdown.
	on_term() {{
	  if [ -n "${{hold_pid:-}}" ] && kill -0 "$hold_pid" 2>/dev/null; then
	    kill -TERM "$hold_pid" 2>/dev/null || true
	    wait "$hold_pid" || true
	  fi
	  exit 0
	}}
	trap on_term TERM INT

	log_dir="{run_dir.as_posix()}/logs/ci_runner"
	mkdir -p "$log_dir"
	exit_code_path="$log_dir/exit_code.txt"
	# The CI runner workload is a Deployment and may be restarted.
	# Once exit_code.txt is written, the run is terminal. If we restart, we must not delete it
	# or re-run tests, otherwise the runner can never converge.
	if [ -f "$exit_code_path" ]; then
	  prev="$(cat "$exit_code_path" 2>/dev/null || echo "")"
	  echo "[ci_runner] found existing exit_code=$prev; holding until controller stop"
	  while true; do
	    sleep 3600 &
	    hold_pid=$!
	    wait "$hold_pid"
	    hold_pid=""
	  done
	fi
	write_exit_code() {{
	  printf '%s\n' "$1" > "$exit_code_path"
	}}
	fail_and_exit() {{
	  write_exit_code "$1"
	  echo "[ci_runner] wrote exit_code=$1; holding until controller stop"
	  while true; do
	    sleep 3600 &
	    hold_pid=$!
	    wait "$hold_pid"
	    hold_pid=""
	  done
	}}
exec >"$log_dir/stdout.log" 2>&1

# Run from src_root so repo-local test commands execute inside the prepared runtime source tree.
cd {src_root.as_posix()}

echo "[ci_runner] pwd=$(pwd)"
{shared_bundle_block}{readiness_probe_block}

	{cmd_block}

	echo "[ci_runner] SUCCESS rc=0"
	fail_and_exit 0
	"""

    out_path.write_text(script, encoding="utf-8")
    os.chmod(out_path, 0o777)
    return out_path


def _http_get_json(url: str) -> Dict[str, Any]:
    req = _new_controller_request(url, method="GET")
    deadline = time.time() + 30.0
    last_err: Optional[Exception] = None
    while True:
        try:
            transported = _controller_request_via_manifest(req, timeout_seconds=CONTROLLER_HTTP_SHORT_ATTEMPT_TIMEOUT_SECONDS)
            if transported is not None:
                status_code, data = transported
                if status_code in CONTROLLER_STATUS_TRANSIENT_HTTP_CODES:
                    raise urllib.error.HTTPError(req.full_url, status_code, "transient", hdrs=None, fp=None)
                if status_code < 200 or status_code >= 300:
                    raise urllib.error.HTTPError(req.full_url, status_code, f"status={status_code}", hdrs=None, fp=None)
                obj = json.loads(data.decode("utf-8"))
                return _require_dict(obj, "http_json")
            # English note:
            # - Controller HTTP endpoints should respond quickly; a long per-attempt socket timeout makes transient
            #   stalls indistinguishable from fatal hangs and causes flaky case failures (TimeoutError: timed out).
            # - Use a short per-attempt timeout, and bound the whole retry window via `deadline` above.
            with urllib.request.urlopen(req, timeout=CONTROLLER_HTTP_SHORT_ATTEMPT_TIMEOUT_SECONDS) as resp:
                data = resp.read()
            obj = json.loads(data.decode("utf-8"))
            return _require_dict(obj, "http_json")
        except urllib.error.HTTPError as exc:
            if int(exc.code) not in CONTROLLER_STATUS_TRANSIENT_HTTP_CODES:
                raise
            last_err = exc
        except (TimeoutError, urllib.error.URLError) as exc:
            last_err = exc
        if time.time() >= deadline:
            if last_err is None:
                raise _HttpGetJsonTransientError(
                    f"http get json transient retry deadline exceeded: url={url}"
                )
            raise _HttpGetJsonTransientError(
                f"http get json transient retry deadline exceeded: url={url} err={last_err}"
            ) from last_err
        time.sleep(1.0)


def _wait_instance_running(resolved_case: Dict[str, Any], *, instance_id: str, timeout_s: int) -> Dict[str, Any]:
    deadline = time.time() + float(timeout_s)
    last: Dict[str, Any] = {}
    last_status_err: str | None = None
    while True:
        try:
            st = _instance_status(resolved_case, instance_id=instance_id)
        except _HttpGetJsonTransientError as exc:
            last_status_err = str(exc)
            if time.time() >= deadline:
                raise ValueError(
                    f"{instance_id} wait running timeout with transient controller errors: err={exc}"
                ) from exc
            time.sleep(2.0)
            continue
        last = st
        if st.get("ok") is True and st.get("running") is True:
            return st
        exit_code = st.get("exit_code")
        if st.get("ok") is True and st.get("running") is False and isinstance(exit_code, int):
            raise ValueError(f"{instance_id} exited before ready: status={last}")
        if time.time() >= deadline:
            raise ValueError(f"{instance_id} wait running timeout: status={last} last_status_err={last_status_err}")
        time.sleep(1.0)



def _wait_file_exists(path: Path, *, timeout_s: int, ctx: str) -> None:
    deadline = time.time() + float(timeout_s)
    while True:
        if path.exists():
            return
        if time.time() >= deadline:
            raise ValueError(f"{ctx} wait timeout: path={path}")
        time.sleep(2.0)


def _instance_remote_target_access(
    resolved_case: Dict[str, Any], *, instance_id: str
) -> Optional[Tuple[str, Dict[str, Any], Any]]:
    inst = _find_deploy_instance(resolved_case, instance_id=instance_id)
    deployer = _require_dict(inst.get("deployer"), f"{instance_id}.deployer")
    target_name = _require_str(deployer.get("target"), f"{instance_id}.target")
    cluster_nodes, dispatch_mod = _load_test_stack_cluster_nodes_and_dispatch(resolved_case)
    node_cfg = _require_dict(cluster_nodes.get(target_name), f"cluster_nodes[{target_name}]")
    node_ip = _require_str(node_cfg.get("ip"), f"cluster_nodes[{target_name}].ip")
    if node_ip in _local_ipv4_addresses():
        return None
    return target_name, node_cfg, dispatch_mod


def _instance_remote_target_access_opt(
    resolved_case: Dict[str, Any], *, instance_id: str
) -> Optional[Tuple[str, Dict[str, Any], Any]]:
    inst = _find_deploy_instance_opt(resolved_case, instance_id=instance_id)
    if inst is None:
        return None
    deployer = _require_dict(inst.get("deployer"), f"{instance_id}.deployer")
    target_name = _require_str(deployer.get("target"), f"{instance_id}.target")
    cluster_nodes, dispatch_mod = _load_test_stack_cluster_nodes_and_dispatch(resolved_case)
    node_cfg = _require_dict(cluster_nodes.get(target_name), f"cluster_nodes[{target_name}]")
    node_ip = _require_str(node_cfg.get("ip"), f"cluster_nodes[{target_name}].ip")
    if node_ip in _local_ipv4_addresses():
        return None
    return target_name, node_cfg, dispatch_mod


def _run_remote_bash_capture(
    *, target_name: str, node_cfg: Dict[str, Any], remote_cmd: str
) -> str:
    transport_ctx = _test_bed_manifest_transport_ctx_opt()
    if transport_ctx is not None and target_name == str(transport_ctx["bastion_name"]):
        completed = _run_remote_bash_via_bastion_transport(
            target_name=target_name,
            node_cfg=node_cfg,
            remote_cmd=remote_cmd,
            ctx=f"remote ssh capture {target_name}",
            emit_output=False,
        )
        return completed.stdout
    ssh_user = _require_str(node_cfg.get("ssh_user"), f"cluster_nodes[{target_name}].ssh_user")
    ssh_port = _require_int(node_cfg.get("ssh_port"), f"cluster_nodes[{target_name}].ssh_port", min_v=1)
    ssh_password_raw = node_cfg.get("ssh_password")
    ssh_host = _cluster_node_ssh_host(node_cfg, target_name=target_name)
    argv: List[str] = []
    if ssh_password_raw is not None:
        argv.extend(
            [
                "sshpass",
                "-p",
                _require_str(ssh_password_raw, f"cluster_nodes[{target_name}].ssh_password"),
            ]
        )
    argv.extend(
        [
            "ssh",
            *_remote_ssh_common_argv(),
            "-p",
            str(ssh_port),
            f"{ssh_user}@{ssh_host}",
            remote_cmd,
        ]
    )
    ctx = f"remote ssh capture {target_name}"
    try:
        completed = _run_ssh_transport_command(
            argv=argv,
            password=None,
            ctx=ctx,
            timeout_seconds=None,
            emit_output=False,
            max_attempts=1,
        )
    except RuntimeError as direct_exc:
        print(
            f"[{ctx}] direct proxy ssh failed; retrying via bastion-local ssh: {direct_exc}",
            flush=True,
        )
        completed = _run_remote_bash_via_bastion_transport(
            target_name=target_name,
            node_cfg=node_cfg,
            remote_cmd=remote_cmd,
            ctx=ctx,
            emit_output=False,
        )
    return completed.stdout


def _run_remote_bash(
    *, target_name: str, node_cfg: Dict[str, Any], remote_cmd: str
) -> None:
    transport_ctx = _test_bed_manifest_transport_ctx_opt()
    if transport_ctx is not None and target_name == str(transport_ctx["bastion_name"]):
        _run_remote_bash_via_bastion_transport(
            target_name=target_name,
            node_cfg=node_cfg,
            remote_cmd=remote_cmd,
            ctx=f"remote ssh {target_name}",
            emit_output=True,
        )
        return
    ssh_user = _require_str(node_cfg.get("ssh_user"), f"cluster_nodes[{target_name}].ssh_user")
    ssh_port = _require_int(node_cfg.get("ssh_port"), f"cluster_nodes[{target_name}].ssh_port", min_v=1)
    ssh_password_raw = node_cfg.get("ssh_password")
    ssh_host = _cluster_node_ssh_host(node_cfg, target_name=target_name)
    argv: List[str] = []
    if ssh_password_raw is not None:
        argv.extend(
            [
                "sshpass",
                "-p",
                _require_str(ssh_password_raw, f"cluster_nodes[{target_name}].ssh_password"),
            ]
        )
    argv.extend(
        [
            "ssh",
            *_remote_ssh_common_argv(),
            "-p",
            str(ssh_port),
            f"{ssh_user}@{ssh_host}",
            remote_cmd,
        ]
    )
    ctx = f"remote ssh {target_name}"
    try:
        _run_ssh_transport_command(
            argv=argv,
            password=None,
            ctx=ctx,
            timeout_seconds=None,
            emit_output=True,
            max_attempts=1,
        )
    except RuntimeError as direct_exc:
        print(
            f"[{ctx}] direct proxy ssh failed; retrying via bastion-local ssh: {direct_exc}",
            flush=True,
        )
        _run_remote_bash_via_bastion_transport(
            target_name=target_name,
            node_cfg=node_cfg,
            remote_cmd=remote_cmd,
            ctx=ctx,
            emit_output=True,
        )


def _run_remote_bash_via_bastion_transport(
    *,
    target_name: str,
    node_cfg: Dict[str, Any],
    remote_cmd: str,
    ctx: str,
    emit_output: bool,
) -> subprocess.CompletedProcess[str]:
    transport_ctx = _test_bed_manifest_transport_ctx_opt()
    if transport_ctx is None:
        raise RuntimeError(f"{ctx} bastion fallback requires test bed transport manifest")

    if target_name == str(transport_ctx["bastion_name"]):
        argv: List[str] = [
            "ssh",
            "-o",
            "BatchMode=yes" if transport_ctx.get("bastion_password") is None else "BatchMode=no",
            "-o",
            "StrictHostKeyChecking=accept-new",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "HostKeyAlgorithms=+ssh-rsa",
            "-o",
            "PubkeyAcceptedAlgorithms=+ssh-rsa",
        ]
        if transport_ctx["bastion_private_key"]:
            argv.extend(["-i", str(transport_ctx["bastion_private_key"]), "-o", "IdentitiesOnly=yes"])
        argv.extend(
            [
                "-p",
                str(transport_ctx["bastion_port"]),
                f"{transport_ctx['bastion_user']}@{transport_ctx['bastion_host']}",
                remote_cmd,
            ]
        )
        return _run_ssh_transport_command(
            argv=argv,
            password=(None if transport_ctx.get("bastion_password") is None else str(transport_ctx["bastion_password"])),
            ctx=f"{ctx} direct bastion",
            timeout_seconds=None,
            emit_output=emit_output,
        )

    ssh_user = _require_str(node_cfg.get("ssh_user"), f"cluster_nodes[{target_name}].ssh_user")
    ssh_port = _require_int(node_cfg.get("ssh_port"), f"cluster_nodes[{target_name}].ssh_port", min_v=1)
    ssh_password_raw = node_cfg.get("ssh_password")
    ssh_password = (
        None
        if ssh_password_raw is None
        else _require_str(ssh_password_raw, f"cluster_nodes[{target_name}].ssh_password")
    )
    ssh_host = _cluster_node_ssh_host(node_cfg, target_name=target_name)

    nested_ssh_parts = [
        "ssh",
        "-o",
        "LogLevel=ERROR",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        "ConnectTimeout=10",
    ]
    if ssh_password is not None:
        nested_ssh_parts.extend(
            [
                "-o",
                "PreferredAuthentications=password,keyboard-interactive",
                "-o",
                "PubkeyAuthentication=no",
                "-o",
                "NumberOfPasswordPrompts=1",
            ]
        )
    nested_ssh_parts.extend(
        [
            "-p",
            str(ssh_port),
            f"{ssh_user}@{ssh_host}",
            remote_cmd,
        ]
    )

    bastion_script_lines = ["set -euo pipefail"]
    nested_cmd = " ".join(_shell_quote(part) for part in nested_ssh_parts)
    if ssh_password is not None:
        bastion_script_lines.extend(
            [
                'ASKPASS_DIR="$(mktemp -d)"',
                'cleanup() { rm -rf "$ASKPASS_DIR"; }',
                "trap cleanup EXIT",
                'cat >"$ASKPASS_DIR/askpass.sh" <<\'EOF\'',
                "#!/bin/sh",
                f"printf '%s\\n' {_shell_quote(ssh_password)}",
                "EOF",
                'chmod 700 "$ASKPASS_DIR/askpass.sh"',
                'setsid env DISPLAY=codex SSH_ASKPASS="$ASKPASS_DIR/askpass.sh" '
                "SSH_ASKPASS_REQUIRE=force " + nested_cmd,
            ]
        )
    else:
        bastion_script_lines.append(nested_cmd)

    bastion_cmd = "bash -lc " + _shell_quote("\n".join(bastion_script_lines))
    argv: List[str] = [
        "ssh",
        "-o",
        "BatchMode=yes" if transport_ctx.get("bastion_password") is None else "BatchMode=no",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "HostKeyAlgorithms=+ssh-rsa",
        "-o",
        "PubkeyAcceptedAlgorithms=+ssh-rsa",
    ]
    if transport_ctx["bastion_private_key"]:
        argv.extend(["-i", str(transport_ctx["bastion_private_key"]), "-o", "IdentitiesOnly=yes"])
    argv.extend(
        [
            "-p",
            str(transport_ctx["bastion_port"]),
            f"{transport_ctx['bastion_user']}@{transport_ctx['bastion_host']}",
            bastion_cmd,
        ]
    )
    return _run_ssh_transport_command(
        argv=argv,
        password=(None if transport_ctx.get("bastion_password") is None else str(transport_ctx["bastion_password"])),
        ctx=f"{ctx} via bastion",
        timeout_seconds=None,
        emit_output=emit_output,
    )


def _instance_read_text_if_present(
    resolved_case: Dict[str, Any], *, instance_id: str, path: Path
) -> Optional[str]:
    if path.exists():
        return path.read_text(encoding="utf-8")
    remote_access = _instance_remote_target_access_opt(resolved_case, instance_id=instance_id)
    if remote_access is None:
        return None
    target_name, node_cfg, dispatch_mod = remote_access
    sentinel = "__DEVER_REMOTE_FILE_PRESENT__"
    remote_cmd = (
        "if [ -f "
        + dispatch_mod.sh_quote(str(path))
        + " ]; then printf '%s\\n' "
        + dispatch_mod.sh_quote(sentinel)
        + "; cat "
        + dispatch_mod.sh_quote(str(path))
        + "; fi"
    )
    output = _run_remote_bash_capture(target_name=target_name, node_cfg=node_cfg, remote_cmd=remote_cmd)
    if not output.startswith(sentinel + "\n"):
        return None
    return output[len(sentinel) + 1 :]


def _instance_file_exists(
    resolved_case: Dict[str, Any], *, instance_id: str, path: Path
) -> bool:
    if path.exists():
        return True
    remote_access = _instance_remote_target_access_opt(resolved_case, instance_id=instance_id)
    if remote_access is None:
        return False
    target_name, node_cfg, dispatch_mod = remote_access
    sentinel = "__DEVER_REMOTE_FILE_PRESENT__"
    remote_cmd = (
        "if [ -f "
        + dispatch_mod.sh_quote(str(path))
        + " ]; then printf '%s\\n' "
        + dispatch_mod.sh_quote(sentinel)
        + "; fi"
    )
    output = _run_remote_bash_capture(target_name=target_name, node_cfg=node_cfg, remote_cmd=remote_cmd)
    return output == sentinel + "\n"


def _wait_instance_files_present(
    resolved_case: Dict[str, Any],
    *,
    instance_id: str,
    paths: List[Path],
    timeout_s: int,
    ctx: str,
) -> None:
    if not paths:
        raise ValueError(f"{ctx}: paths is empty")
    deadline = time.time() + float(timeout_s)
    while True:
        missing = [
            str(path)
            for path in paths
            if not _instance_file_exists(resolved_case, instance_id=instance_id, path=path)
        ]
        if not missing:
            return
        status = _instance_status(resolved_case, instance_id=instance_id)
        exit_code = status.get("exit_code")
        if status.get("ok") is True and status.get("running") is False and isinstance(exit_code, int):
            raise ValueError(f"{ctx}: {instance_id} exited before artifacts were ready: status={status}")
        if time.time() >= deadline:
            raise ValueError(f"{ctx}: wait timeout; missing={missing} status={status}")
        time.sleep(2.0)


def _instance_target_name(resolved_case: Dict[str, Any], *, instance_id: str) -> str:
    inst = _find_deploy_instance(resolved_case, instance_id=instance_id)
    deployer = _require_dict(inst.get("deployer"), f"{instance_id}.deployer")
    return _require_str(deployer.get("target"), f"{instance_id}.target")


def _test_stack_external_owner_target_instance_map(
    resolved_case: Dict[str, Any],
    *,
    owner_instance_ids: List[str],
) -> Dict[str, str]:
    if not owner_instance_ids:
        raise ValueError("TEST_STACK owner shared bundle cleanup requires non-empty owner_instance_ids")
    target_to_instance_id: Dict[str, str] = {}
    for owner_id in owner_instance_ids:
        target_name = _instance_target_name(resolved_case, instance_id=owner_id)
        target_to_instance_id.setdefault(target_name, owner_id)
    return target_to_instance_id


def _test_stack_runtime_target_names_for_instance_ids(
    resolved_case: Dict[str, Any],
    *,
    instance_ids: List[str],
) -> List[str]:
    if not instance_ids:
        raise ValueError("TEST_STACK runtime target collection requires non-empty instance_ids")
    target_names: List[str] = []
    seen: Set[str] = set()
    for instance_id in instance_ids:
        target_name = _instance_target_name(resolved_case, instance_id=instance_id)
        if target_name in seen:
            continue
        seen.add(target_name)
        target_names.append(target_name)
    return target_names


def _test_stack_external_owner_shared_bundle_wait_instance_ids(
    resolved_case: Dict[str, Any],
    *,
    owner_instance_ids: List[str],
    node_runtime_instance_ids: List[str],
) -> List[str]:
    if not owner_instance_ids:
        raise ValueError("TEST_STACK owner shared bundle wait requires non-empty owner_instance_ids")
    owner_target_to_instance_id = _test_stack_external_owner_target_instance_map(
        resolved_case,
        owner_instance_ids=owner_instance_ids,
    )
    node_runtime_targets = _test_stack_runtime_target_names_for_instance_ids(
        resolved_case,
        instance_ids=node_runtime_instance_ids,
    )
    wait_instance_ids: List[str] = []
    for target_name in node_runtime_targets:
        owner_id = owner_target_to_instance_id.get(target_name)
        if owner_id is None:
            continue
        wait_instance_ids.append(owner_id)
    if wait_instance_ids:
        return wait_instance_ids
    raise ValueError(
        "TEST_STACK owner shared bundle wait could not match any owner target to node runtime targets: "
        f"owner_targets={sorted(owner_target_to_instance_id.keys())} "
        f"node_runtime_targets={node_runtime_targets}"
    )


def _instance_remove_files_and_verify_absent(
    resolved_case: Dict[str, Any],
    *,
    instance_id: str,
    paths: List[Path],
    timeout_s: int,
    ctx: str,
) -> None:
    if not paths:
        raise ValueError(f"{ctx}: paths is empty")
    normalized_paths = [path.resolve() for path in paths]
    remote_access = _instance_remote_target_access_opt(resolved_case, instance_id=instance_id)
    if remote_access is None:
        for path in normalized_paths:
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            except IsADirectoryError as exc:
                raise ValueError(f"{ctx}: expected file but found directory: path={path}") from exc
    else:
        target_name, node_cfg, dispatch_mod = remote_access
        remote_cmd = "rm -f " + " ".join(dispatch_mod.sh_quote(str(path)) for path in normalized_paths)
        _run_remote_bash(target_name=target_name, node_cfg=node_cfg, remote_cmd=remote_cmd)

    deadline = time.time() + float(timeout_s)
    while True:
        remaining = [
            str(path)
            for path in normalized_paths
            if _instance_file_exists(resolved_case, instance_id=instance_id, path=path)
        ]
        if not remaining:
            return
        if time.time() >= deadline:
            raise ValueError(f"{ctx}: delete timeout; remaining={remaining}")
        time.sleep(0.5)


def _test_stack_external_owner_shared_bundle_paths(
    resolved_case: Dict[str, Any],
    *,
    owner_target: Optional[str] = None,
) -> List[Path]:
    runtime = _require_dict(resolved_case.get("runtime"), "resolved_case.runtime")
    stack_identity = _require_dict(runtime.get("stack_identity"), "resolved_case.runtime.stack_identity")
    cluster_name = _require_str(
        stack_identity.get("cluster_name"),
        "resolved_case.runtime.stack_identity.cluster_name",
    )
    shared_memory_path = _require_str(
        stack_identity.get("shared_memory_path"),
        "resolved_case.runtime.stack_identity.shared_memory_path",
    )
    shared_file_path = _require_str(
        stack_identity.get("shared_file_path"),
        "resolved_case.runtime.stack_identity.shared_file_path",
    )
    if owner_target is not None:
        scale = _require_dict(resolved_case.get("scale"), "resolved_case.scale")
        role_plan = _require_dict(scale.get("role_plan"), "resolved_case.scale.role_plan")
        deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
        target_ip_map = _require_dict(deploy.get("target_ip_map"), "resolved_case.deploy.target_ip_map")
        scene = _require_dict(resolved_case.get("scene"), "resolved_case.scene")
        ts_scene = _require_dict(scene.get("test_stack"), "resolved_case.scene.test_stack")
        scene_mode = _require_str(ts_scene.get("mode"), "resolved_case.scene.test_stack.mode")
        owner_targets = _test_stack_owner_targets(
            scale=scale,
            role_plan=role_plan,
            roles_order=_test_stack_roles_by_mode(scene_mode),
            ctx="resolved_case.scale",
        )
        benchmark = _require_dict(scale.get("benchmark"), "resolved_case.scale.benchmark")
        owner_group_processes = _test_stack_owner_group_processes(
            scale=scale,
            owner_targets=owner_targets,
            target_ip_map=target_ip_map,
            processes_per_target=_require_int(
                benchmark.get("processes_per_target"),
                "resolved_case.scale.benchmark.processes_per_target",
                min_v=1,
            ),
            ctx="resolved_case.scale",
        )
        if owner_group_processes is not None:
            return _owner_bundle_paths_for_target(
                shared_memory_root=shared_memory_path,
                shared_file_root=shared_file_path,
                cluster_name=cluster_name,
                owner_target=owner_target,
                ctx="TEST_STACK owner shared bundle paths",
            )
    return _shared_bundle_paths_for_cluster(
        shared_memory_root=shared_memory_path,
        shared_file_root=shared_file_path,
        cluster_name=cluster_name,
    )


def _cleanup_test_stack_external_owner_shared_bundle_stale_files(
    resolved_case: Dict[str, Any],
    *,
    owner_instance_ids: List[str],
) -> None:
    for target_name, owner_id in _test_stack_external_owner_target_instance_map(
        resolved_case,
        owner_instance_ids=owner_instance_ids,
    ).items():
        shared_bundle_paths = _test_stack_external_owner_shared_bundle_paths(
            resolved_case,
            owner_target=target_name,
        )
        print(
            "[TEST_STACK owner shared bundle cleanup] "
            f"target={target_name} paths={[str(path) for path in shared_bundle_paths]}",
            flush=True,
        )
        _instance_remove_files_and_verify_absent(
            resolved_case,
            instance_id=owner_id,
            paths=shared_bundle_paths,
            timeout_s=10,
            ctx=f"TEST_STACK owner shared bundle cleanup target={target_name}",
        )


def _test_stack_external_owner_shared_bundle_present_paths_by_target(
    resolved_case: Dict[str, Any],
    *,
    owner_instance_ids: List[str],
) -> Dict[str, List[str]]:
    present_by_target: Dict[str, List[str]] = {}
    for target_name, owner_id in _test_stack_external_owner_target_instance_map(
        resolved_case,
        owner_instance_ids=owner_instance_ids,
    ).items():
        shared_bundle_paths = _test_stack_external_owner_shared_bundle_paths(
            resolved_case,
            owner_target=target_name,
        )
        present = [
            str(path)
            for path in shared_bundle_paths
            if _instance_file_exists(resolved_case, instance_id=owner_id, path=path)
        ]
        if present:
            present_by_target[target_name] = present
    return present_by_target


def _converge_test_stack_external_owner_shared_bundle_cleanup(
    resolved_case: Dict[str, Any],
    *,
    controller_url: str,
    owner_instance_ids: List[str],
) -> None:
    deadline = time.time() + float(_TEST_STACK_EXTERNAL_SHARED_BUNDLE_CLEANUP_TIMEOUT_S)
    present_by_target: Dict[str, List[str]] = {}
    while True:
        _cleanup_bench_namespace_preflight(
            controller_url=controller_url,
            namespace=_test_stack_ops_namespace(),
        )
        _cleanup_test_stack_external_owner_shared_bundle_stale_files(
            resolved_case,
            owner_instance_ids=owner_instance_ids,
        )
        quiet_deadline = min(
            deadline,
            time.time() + float(_TEST_STACK_EXTERNAL_SHARED_BUNDLE_QUIET_PERIOD_S),
        )
        while True:
            present_by_target = _test_stack_external_owner_shared_bundle_present_paths_by_target(
                resolved_case,
                owner_instance_ids=owner_instance_ids,
            )
            if present_by_target:
                print(
                    "[TEST_STACK owner shared bundle cleanup] "
                    "shared bundle reappeared during quiet window; retrying "
                    f"present_by_target={present_by_target}",
                    flush=True,
                )
                break
            now = time.time()
            if now >= quiet_deadline:
                return
            time.sleep(min(1.0, quiet_deadline - now))
        if time.time() >= deadline:
            raise ValueError(
                "TEST_STACK owner shared bundle cleanup did not converge after timeout: "
                f"present_by_target={present_by_target}"
            )


def _observe_file_state(path: Path) -> Optional[_ObservedFileState]:
    if not path.exists():
        return None
    st = path.stat()
    return _ObservedFileState(size=int(st.st_size), mtime_ns=int(st.st_mtime_ns))


def _has_new_file_state(
    *,
    before: Optional[_ObservedFileState],
    after: Optional[_ObservedFileState],
) -> bool:
    if after is None:
        return False
    if before is None:
        return True
    return after.size != before.size or after.mtime_ns != before.mtime_ns


def _wait_instance_exit(
    resolved_case: Dict[str, Any], *, instance_id: str, timeout_s: int
) -> Dict[str, Any]:
    deadline = time.time() + float(timeout_s)
    last: Dict[str, Any] = {}
    last_status_err: str | None = None
    while True:
        try:
            st = _instance_status(resolved_case, instance_id=instance_id)
        except _HttpGetJsonTransientError as exc:
            last_status_err = str(exc)
            if time.time() >= deadline:
                raise ValueError(
                    f"{instance_id} wait timeout with transient controller errors: err={exc}"
                ) from exc
            time.sleep(2.0)
            continue
        last = st
        exit_code = st.get("exit_code")
        if (
            st.get("ok") is True
            and st.get("running") is False
            and isinstance(exit_code, int)
        ):
            return st
        if time.time() >= deadline:
            raise ValueError(f"{instance_id} wait timeout: status={last} last_status_err={last_status_err}")
        time.sleep(2.0)


def _wait_ci_runner_exit_code_resume(
    *,
    resolved_case: Dict[str, Any],
    run_dir: Path,
    timeout_s: int,
) -> int:
    """Resume a reserved-only CI run without requiring the original baseline file state.

    A reserved-only last_run means the runner persisted run_index early, but exited before finalize().
    If exit_code.txt exists (local or remote), treat it as terminal and converge case_runs.yaml.
    """
    inst = _find_deploy_instance(resolved_case, instance_id="ci_runner")
    k8s_ref = _require_str(inst.get("k8s_ref"), "ci_runner.k8s_ref")
    if "/" not in k8s_ref:
        raise ValueError("ci_runner.k8s_ref must be <deployment|daemonset>/<name>")
    _, ci_runner_workload_name = k8s_ref.split("/", 1)
    if not ci_runner_workload_name.strip():
        raise ValueError("ci_runner.k8s_ref name must be non-empty")

    exit_code_path = (run_dir / "logs" / "ci_runner" / "exit_code.txt").resolve()
    deadline = time.time() + float(timeout_s)
    last_status_err: str | None = None
    while True:
        raw = _instance_read_text_if_present(
            resolved_case,
            instance_id="ci_runner",
            path=exit_code_path,
        )
        if raw is not None:
            try:
                rc = int(raw.strip())
            except ValueError as exc:
                raise ValueError(
                    f"ci_runner remote exit_code file is not an int: path={exit_code_path} raw={raw!r}"
                ) from exc
            return _require_int(rc, "ci_runner.resume_exit_code", min_v=-255)

        try:
            status = _instance_status(resolved_case, instance_id="ci_runner")
        except _HttpGetJsonTransientError as exc:
            last_status_err = str(exc)
            if time.time() >= deadline:
                print(
                    "[CI resume] ci_runner.exit_code wait timeout with transient controller errors; treating as rc=-1: "
                    f"path={exit_code_path} err={exc}",
                    flush=True,
                )
                return -1
            time.sleep(2.0)
            continue
        status_exit_code = status.get("exit_code")
        if status.get("ok") is True and status.get("running") is False and isinstance(status_exit_code, int):
            return _require_int(status_exit_code, "ci_runner.resume.status.exit_code", min_v=-255)
        if status.get("ok") is True and status.get("running") is False:
            # Deterministic behavior:
            # - If controller no longer reports desired workloads for this case, the CI runner cannot start.
            # - Otherwise, a transient "running=false without exit_code" state is not terminal; keep waiting.
            deploy = _require_dict(resolved_case.get("deploy"), "ci_runner.resume.resolved_case.deploy")
            controller_url = _require_str(deploy.get("controller_url"), "ci_runner.resume.deploy.controller_url").rstrip("/")
            try:
                has_desired = _ops_current_deployments_has_workload_name(
                    controller_url, workload_name=ci_runner_workload_name
                )
            except _HttpGetJsonTransientError:
                has_desired = True
            if not has_desired:
                print(
                    "[CI resume] ci_runner stopped before producing exit_code.txt and controller has no desired workload for ci_runner; "
                    f"treating as rc=-1: path={exit_code_path} status={status}",
                    flush=True,
                )
                return -1
        if time.time() >= deadline:
            # Resume happens outside the per-case try/except loop. Do not raise here, otherwise the
            # whole suite exits and case_runs.yaml stays stuck at reserved-only last_run.
            print(
                "[CI resume] ci_runner.exit_code wait timeout; treating as rc=-1: "
                f"path={exit_code_path} status={status} last_status_err={last_status_err}",
                flush=True,
            )
            return -1
        time.sleep(2.0)


def _wait_ci_runner_exit_code(
    *,
    resolved_case: Dict[str, Any],
    run_dir: Path,
    timeout_s: int,
    baseline_state: Optional[_ObservedFileState],
) -> int:
    exit_code_path = (run_dir / "logs" / "ci_runner" / "exit_code.txt").resolve()
    deadline = time.time() + float(timeout_s)
    last_status_err: str | None = None
    while True:
        current_state = _observe_file_state(exit_code_path)
        if _has_new_file_state(before=baseline_state, after=current_state):
            raw = exit_code_path.read_text(encoding="utf-8").strip()
            try:
                rc = int(raw)
            except ValueError as exc:
                raise ValueError(
                    f"ci_runner exit_code file is not an int: path={exit_code_path} raw={raw!r}"
                ) from exc
            return _require_int(rc, "ci_runner.exit_code", min_v=-255)
        remote_raw = _instance_read_text_if_present(
            resolved_case,
            instance_id="ci_runner",
            path=exit_code_path,
        )
        if remote_raw is not None:
            raw = remote_raw.strip()
            try:
                rc = int(raw)
            except ValueError as exc:
                raise ValueError(
                    f"ci_runner remote exit_code file is not an int: path={exit_code_path} raw={raw!r}"
                ) from exc
            return _require_int(rc, "ci_runner.remote_exit_code", min_v=-255)
        try:
            status = _instance_status(resolved_case, instance_id="ci_runner")
        except _HttpGetJsonTransientError as exc:
            last_status_err = str(exc)
            if time.time() >= deadline:
                raise ValueError(
                    "ci_runner.exit_code wait timeout with transient controller errors: "
                    f"path={exit_code_path} baseline_state={baseline_state} current_state={current_state} err={exc}"
                ) from exc
            time.sleep(2.0)
            continue
        status_exit_code = status.get("exit_code")
        if status.get("ok") is True and status.get("running") is False and isinstance(status_exit_code, int):
            return _require_int(status_exit_code, "ci_runner.status.exit_code", min_v=-255)
        if status.get("ok") is True and status.get("running") is False:
            # Deterministic behavior:
            # - If controller no longer reports desired workloads for this case, the CI runner cannot start.
            # - Otherwise, a transient "running=false without exit_code" state is not terminal; keep waiting.
            deploy = _require_dict(resolved_case.get("deploy"), "ci_runner.wait.resolved_case.deploy")
            controller_url = _require_str(deploy.get("controller_url"), "ci_runner.wait.deploy.controller_url").rstrip("/")
            # English note:
            # - Checking "any workload with case_id prefix" is too broad: master/owner workloads can remain
            #   desired even when ci_runner is already gone, which would block this loop forever.
            # - For CI, we must check the specific ci_runner workload name.
            inst = _find_deploy_instance(resolved_case, instance_id="ci_runner")
            k8s_ref = _require_str(inst.get("k8s_ref"), "ci_runner.k8s_ref")
            if "/" not in k8s_ref:
                raise ValueError("ci_runner.k8s_ref must be <deployment|daemonset>/<name>")
            _, ci_runner_workload_name = k8s_ref.split("/", 1)
            try:
                has_desired = _ops_current_deployments_has_workload_name(
                    controller_url, workload_name=ci_runner_workload_name
                )
            except _HttpGetJsonTransientError:
                has_desired = True
            if not has_desired:
                print(
                    "[CI wait exit_code] ci_runner stopped before producing exit_code.txt and controller has no desired workloads; "
                    f"treating as rc=-1: path={exit_code_path} baseline_state={baseline_state} current_state={current_state} status={status}",
                    flush=True,
                )
                return -1
        if time.time() >= deadline:
            raise ValueError(
                "ci_runner.exit_code wait timeout: "
                f"path={exit_code_path} baseline_state={baseline_state} current_state={current_state} status={status} last_status_err={last_status_err}"
            )
        time.sleep(2.0)


def _controller_target_for_target(
    target: str,
    *,
    target_ip_map: Dict[str, Any],
) -> str:
    node_ip_raw = target_ip_map.get(target)
    if not isinstance(node_ip_raw, str) or not node_ip_raw.strip():
        return target
    node_ip = node_ip_raw.strip()

    same_ip_targets: List[str] = []
    for raw_target, raw_ip in target_ip_map.items():
        candidate = _require_str(raw_target, "target_ip_map key")
        ip_value = _require_str(raw_ip, f"target_ip_map[{candidate!r}]")
        if ip_value == node_ip:
            same_ip_targets.append(candidate)
    if not same_ip_targets:
        return target

    test_bed_targets = [candidate for candidate in _canonical_targets_for_ip_from_test_bed(node_ip) if candidate in same_ip_targets]
    if test_bed_targets:
        return test_bed_targets[0]

    bastion_targets = sorted(
        (candidate for candidate in same_ip_targets if candidate == "primary-bastion" or candidate.endswith("bastion")),
        key=lambda candidate: (0 if candidate == "primary-bastion" else 1, len(candidate), candidate),
    )
    if bastion_targets:
        return bastion_targets[0]

    canonical_targets = sorted(candidate for candidate in same_ip_targets if re.fullmatch(r"node-\d+", candidate))
    if canonical_targets:
        return canonical_targets[0]

    ordered = sorted(same_ip_targets, key=lambda candidate: (0 if candidate == target else 1, len(candidate), candidate))
    return ordered[0]


def _instance_status(resolved_case: Dict[str, Any], *, instance_id: str) -> Dict[str, Any]:
    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    controller_url = _require_str(deploy.get("controller_url"), "deploy.controller_url").rstrip("/")
    target_ip_map = _require_dict(deploy.get("target_ip_map"), "deploy.target_ip_map")

    inst = _find_deploy_instance(resolved_case, instance_id=instance_id)
    k8s_ref = _require_str(inst.get("k8s_ref"), f"{instance_id}.k8s_ref")
    if "/" not in k8s_ref:
        raise ValueError(f"{instance_id}.k8s_ref must be <deployment|daemonset>/<name>")
    k8s_ref_kind, name = k8s_ref.split("/", 1)
    if k8s_ref_kind not in (K8S_REF_KIND_DEPLOYMENT, K8S_REF_KIND_DAEMONSET) or not name.strip():
        raise ValueError(f"{instance_id}.k8s_ref must be <deployment|daemonset>/<name>")
    kind = "Deployment" if k8s_ref_kind == K8S_REF_KIND_DEPLOYMENT else "DaemonSet"
    target = _require_str(
        _require_dict(inst.get("deployer"), f"{instance_id}.deployer").get("target"),
        f"{instance_id}.target",
    )
    controller_target = _controller_target_for_target(target, target_ip_map=target_ip_map)
    qs = urllib.parse.urlencode(
        {
            "target": controller_target,
            "kind": kind,
            "name": name,
            "authority": name,
        }
    )
    return _http_get_json(controller_url + "/api/status?" + qs)


def _run_ci_commands(resolved_case: Dict[str, Any], *, run_dir: Path, workdir_root: Path) -> Dict[str, Any]:
    commands = _resolved_ci_command_list(resolved_case)

    log_path = run_dir / "ci.log"
    if log_path.exists():
        raise ValueError(f"ci.log already exists (no overwrite): {log_path}")

    failed_step: int | None = None
    rc = 0

    with log_path.open("w", encoding="utf-8") as lf:
        for idx, command in enumerate(commands, start=1):
            step_label = _command_step_label(command)
            cmd = _subst_runtime_tokens(resolved_case, command["command"])
            lf.write("\n" + ("=" * 80) + "\n")
            lf.write(f"STEP {idx}: {step_label} :: {cmd}\n")
            lf.write(("=" * 80) + "\n")
            lf.flush()

            proc = subprocess.Popen(["bash", "-lc", cmd], cwd=str(workdir_root), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
            if proc.stdout is None:
                raise RuntimeError("Popen stdout is None")
            for line in proc.stdout:
                lf.write(line)
                lf.flush()
                print(line, end="", flush=True)
            rc = int(proc.wait())
            if rc == 0:
                continue
            failed_step = idx
            break

    out: Dict[str, Any] = {"rc": rc}
    if failed_step is not None:
        out["failed_step"] = int(failed_step)
        out["failed_cmd"] = _command_step_label(commands[failed_step - 1])
    return out


def _build_ci_summary_yaml(
    resolved_case: Dict[str, Any],
    *,
    run_index: int,
    started_at_unix_s: int,
    finished_at_unix_s: int,
    outcome: str,
    counted: bool,
    ci_out: Dict[str, Any],
) -> Dict[str, Any]:
    case = _require_dict(resolved_case.get("case"), "resolved_case.case")
    case_id = _require_str(case.get("case_id"), "case.case_id")
    case_key = _require_str(case.get("case_key"), "case.case_key")

    # Deployer reports exit_code=-1 when a process is terminated without a normal exit code.
    rc = _require_int(ci_out.get("rc"), "ci_out.rc", min_v=-1)
    failed_step = ci_out.get("failed_step")
    if failed_step is not None:
        failed_step_i = _require_int(failed_step, "ci_out.failed_step", min_v=1)
    else:
        failed_step_i = None

    ci: Dict[str, Any] = {"rc": int(rc)}
    if failed_step_i is not None:
        ci["failed_step"] = int(failed_step_i)
        if "failed_cmd" in ci_out:
            ci["failed_cmd"] = _require_str(ci_out.get("failed_cmd"), "ci_out.failed_cmd")

    return {
        "schema_version": SCHEMA_VERSION,
        "case_id": case_id,
        "case_key": case_key,
        "run_index": int(run_index),
        "outcome": outcome,
        "counted": bool(counted),
        "timing": {"started_at_unix_s": int(started_at_unix_s), "finished_at_unix_s": int(finished_at_unix_s)},
        "ci": ci,
    }


def _html_escape(value: Any) -> str:
    return html.escape(str(value), quote=True)


def _safe_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _ui_run_dir_name(run_index: int) -> str:
    return f"run_{int(run_index)}"


def _ui_parse_run_index(name: str) -> Optional[int]:
    if not isinstance(name, str) or not re.fullmatch(r"run_[1-9][0-9]*", name):
        return None
    return int(name.split("_", 1)[1])


def _ui_relpath(base: Path, path: Path) -> str:
    return path.resolve().relative_to(base.resolve()).as_posix()


def _ui_is_within_base(base: Path, path: Path) -> bool:
    try:
        path.resolve().relative_to(base.resolve())
        return True
    except Exception:
        return False


def _ui_load_case_runs_for_workdir(workdir_root: Path) -> Dict[str, Any]:
    case_runs_path = (workdir_root / "case_runs.yaml").resolve()
    if not case_runs_path.exists():
        return {"schema_version": SCHEMA_VERSION, "cases": []}
    return _load_or_init_case_runs(case_runs_path)


def _ui_case_runs_records(case_runs: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw_cases = case_runs.get("cases")
    if not isinstance(raw_cases, list):
        return []
    out: List[Dict[str, Any]] = []
    for raw in raw_cases:
        if isinstance(raw, dict):
            out.append(raw)
    return out


def _ui_case_record_map(workdir_root: Path) -> Dict[str, Dict[str, Any]]:
    case_runs = _ui_load_case_runs_for_workdir(workdir_root)
    out: Dict[str, Dict[str, Any]] = {}
    for record in _ui_case_runs_records(case_runs):
        case_id = record.get("case_id")
        if isinstance(case_id, str) and case_id:
            out[case_id] = record
    return out


def _ui_case_result_root(workdir_root: Path, case_id: str) -> Path:
    return (workdir_root / "results" / case_id).resolve()


def _ui_sorted_run_dirs(case_root: Path) -> List[Path]:
    if not case_root.exists() or not case_root.is_dir():
        return []
    out: List[Tuple[int, Path]] = []
    for child in case_root.iterdir():
        if not child.is_dir():
            continue
        run_index = _ui_parse_run_index(child.name)
        if run_index is None:
            continue
        out.append((run_index, child.resolve()))
    out.sort(key=lambda item: item[0], reverse=True)
    return [path for _, path in out]


def _ui_case_ids_from_results(workdir_root: Path) -> List[str]:
    results_root = (workdir_root / "results").resolve()
    if not results_root.exists() or not results_root.is_dir():
        return []
    out: List[str] = []
    for child in results_root.iterdir():
        if child.is_dir():
            out.append(child.name)
    return sorted(set(out))


def _ui_collect_case_ids(workdir_root: Path) -> List[str]:
    record_ids = set(_ui_case_record_map(workdir_root).keys())
    result_ids = set(_ui_case_ids_from_results(workdir_root))
    return sorted(record_ids | result_ids)


def _ui_case_run_summary(run_dir: Path) -> Dict[str, Any]:
    summary_path = (run_dir / "summary.yaml").resolve()
    summary: Optional[Dict[str, Any]] = None
    if summary_path.exists():
        loaded = _load_yaml_file(summary_path)
        if isinstance(loaded, dict):
            summary = loaded
    resolved_case: Optional[Dict[str, Any]] = None
    try:
        resolved_case = _load_run_dir_resolved_case(run_dir)
    except Exception:
        resolved_case = None
    run_index = _ui_parse_run_index(run_dir.name)
    if run_index is None:
        raise ValueError(f"invalid run_dir name: {run_dir}")
    outcome = None
    if isinstance(summary, dict):
        raw_outcome = summary.get("outcome")
        if isinstance(raw_outcome, str) and raw_outcome.strip():
            outcome = raw_outcome.strip()
        if outcome == RUN_OUTCOME_FAILED:
            raw_error = summary.get("error")
            if isinstance(raw_error, str) and raw_error == _RUN_SUMMARY_INCOMPLETE_ERROR:
                outcome = "INCOMPLETE"
    if outcome is None:
        outcome = "INCOMPLETE"
    finished_at_unix_s = 0
    started_at_unix_s = 0
    counted = False
    if isinstance(summary, dict):
        timing = summary.get("timing")
        if isinstance(timing, dict):
            started_at_unix_s = _safe_int(timing.get("started_at_unix_s"), default=0)
            finished_at_unix_s = _safe_int(timing.get("finished_at_unix_s"), default=0)
        counted = bool(summary.get("counted"))
    case_id = ""
    case_key = ""
    scene_family = ""
    if isinstance(summary, dict):
        raw_case_id = summary.get("case_id")
        raw_case_key = summary.get("case_key")
        if isinstance(raw_case_id, str):
            case_id = raw_case_id
        if isinstance(raw_case_key, str):
            case_key = raw_case_key
    if isinstance(resolved_case, dict):
        case_obj = resolved_case.get("case")
        if isinstance(case_obj, dict):
            case_id = str(case_obj.get("case_id") or case_id)
            case_key = str(case_obj.get("case_key") or case_key)
            family_raw = case_obj.get("family")
            if isinstance(family_raw, str):
                scene_family = family_raw
    return {
        "run_index": int(run_index),
        "run_dir": run_dir.resolve(),
        "summary_path": summary_path if summary_path.exists() else None,
        "summary": summary,
        "resolved_case": resolved_case,
        "case_id": case_id,
        "case_key": case_key,
        "family": scene_family,
        "outcome": outcome,
        "counted": counted,
        "started_at_unix_s": int(started_at_unix_s),
        "finished_at_unix_s": int(finished_at_unix_s),
    }


def _ui_format_unix_ts(raw_value: Any) -> str:
    unix_s = _safe_int(raw_value, default=0)
    if unix_s <= 0:
        return ""
    try:
        return datetime.datetime.fromtimestamp(int(unix_s)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


def _ui_format_iso_ts(raw_value: Any) -> str:
    if not isinstance(raw_value, str):
        return ""
    text = raw_value.strip()
    if not text:
        return ""
    try:
        dt = datetime.datetime.fromisoformat(text)
    except Exception:
        return text
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _ui_case_reserved_activity_unix_s(
    workdir_root: Path,
    *,
    case_id: str,
    run_index: int,
    run_summary: Optional[Dict[str, Any]],
) -> int:
    latest = 0

    def _consume_path(path: Path) -> None:
        nonlocal latest
        if not path.exists():
            return
        try:
            latest = max(latest, int(path.stat().st_mtime))
        except Exception:
            return

    _consume_path((workdir_root / "case_runs.yaml").resolve())
    _consume_path((workdir_root / RUNNER_STDIO_LOG_FILENAME).resolve())

    run_dir = (_ui_case_result_root(workdir_root, case_id) / _ui_run_dir_name(run_index)).resolve()
    _consume_path(run_dir)
    for rel_path in ("summary.yaml", "benchmark_result.json", "exception.txt", "ci.log"):
        _consume_path((run_dir / rel_path).resolve())
    logs_root = (run_dir / "logs").resolve()
    if logs_root.exists() and logs_root.is_dir():
        for path in logs_root.rglob("*"):
            if path.is_file():
                _consume_path(path.resolve())

    if isinstance(run_summary, dict):
        latest = max(
            latest,
            _safe_int(run_summary.get("started_at_unix_s"), default=0),
            _safe_int(run_summary.get("finished_at_unix_s"), default=0),
        )
    return int(latest)


def _ui_reserved_run_status(
    workdir_root: Path,
    *,
    case_id: str,
    active_run_index: int,
    run_summary: Optional[Dict[str, Any]],
) -> str:
    if active_run_index <= 0:
        return "EMPTY"
    if isinstance(run_summary, dict):
        run_outcome = run_summary.get("outcome")
        if isinstance(run_outcome, str) and run_outcome.strip():
            if run_outcome == "INCOMPLETE":
                recent_activity_unix_s = _ui_case_reserved_activity_unix_s(
                    workdir_root,
                    case_id=case_id,
                    run_index=active_run_index,
                    run_summary=run_summary,
                )
                if recent_activity_unix_s > 0 and (
                    int(time.time()) - int(recent_activity_unix_s)
                ) <= _TEST_RUNNER_UI_ACTIVE_RESERVED_GRACE_SECONDS:
                    return "RUNNING"
            return run_outcome
    run_dir = (_ui_case_result_root(workdir_root, case_id) / _ui_run_dir_name(active_run_index)).resolve()
    if run_dir.exists():
        recent_activity_unix_s = _ui_case_reserved_activity_unix_s(
            workdir_root,
            case_id=case_id,
            run_index=active_run_index,
            run_summary=run_summary,
        )
        if recent_activity_unix_s > 0 and (
            int(time.time()) - int(recent_activity_unix_s)
        ) <= _TEST_RUNNER_UI_ACTIVE_RESERVED_GRACE_SECONDS:
            return "RUNNING"
        return "INCOMPLETE"
    return "RESERVED"


def _ui_collect_case_runs(workdir_root: Path, *, case_id: str) -> List[Dict[str, Any]]:
    case_root = _ui_case_result_root(workdir_root, case_id)
    out: List[Dict[str, Any]] = []
    for run_dir in _ui_sorted_run_dirs(case_root):
        out.append(_ui_case_run_summary(run_dir))
    return out


def _ui_case_overview(workdir_root: Path, *, case_id: str) -> Dict[str, Any]:
    record = _ui_case_record_map(workdir_root).get(case_id)
    runs = _ui_collect_case_runs(workdir_root, case_id=case_id)
    last_run = record.get("last_run") if isinstance(record, dict) else None
    active_run_index = 0
    status = "EMPTY"
    if isinstance(last_run, dict):
        active_run_index = _safe_int(last_run.get("run_index"), default=0)
        if isinstance(last_run.get("outcome"), str):
            status = str(last_run.get("outcome"))
        elif active_run_index > 0:
            active_run = next((run for run in runs if _safe_int(run.get("run_index"), default=0) == active_run_index), None)
            status = _ui_reserved_run_status(
                workdir_root,
                case_id=case_id,
                active_run_index=active_run_index,
                run_summary=active_run,
            )
    if status == "EMPTY" and runs:
        status = str(runs[0].get("outcome") or "UNKNOWN")
    return {
        "case_id": case_id,
        "case_key": str(record.get("case_key") or case_id) if isinstance(record, dict) else case_id,
        "record": record,
        "last_run": last_run if isinstance(last_run, dict) else None,
        "total_runs": _safe_int(record.get("total_runs"), default=0) if isinstance(record, dict) else 0,
        "success_runs": _safe_int(record.get("success_runs"), default=0) if isinstance(record, dict) else 0,
        "failed_runs": _safe_int(record.get("failed_runs"), default=0) if isinstance(record, dict) else 0,
        "counted_runs": _safe_int(record.get("counted_runs"), default=0) if isinstance(record, dict) else 0,
        "status": status,
        "active_run_index": int(active_run_index),
        "runs": runs,
    }


def _ui_collect_suite_overview(workdir_root: Path) -> Dict[str, Any]:
    case_ids = _ui_collect_case_ids(workdir_root)
    cases = [_ui_case_overview(workdir_root, case_id=case_id) for case_id in case_ids]
    runner_log_path = (workdir_root / RUNNER_STDIO_LOG_FILENAME).resolve()
    running_cases = [case for case in cases if case.get("status") == "RUNNING"]
    incomplete_cases = [case for case in cases if case.get("status") in {"INCOMPLETE", "RESERVED"}]
    last_updated_unix_s = 0
    for path in (
        (workdir_root / "case_runs.yaml").resolve(),
        runner_log_path,
    ):
        if isinstance(path, Path) and path.exists():
            try:
                last_updated_unix_s = max(last_updated_unix_s, int(path.stat().st_mtime))
            except Exception:
                pass
    for case in cases:
        for run in case.get("runs", []):
            last_updated_unix_s = max(
                last_updated_unix_s,
                _safe_int(run.get("finished_at_unix_s"), default=0),
                _safe_int(run.get("started_at_unix_s"), default=0),
            )
    return {
        "workdir_root": workdir_root.resolve(),
        "case_runs_path": (workdir_root / "case_runs.yaml").resolve(),
        "runner_log_path": runner_log_path if runner_log_path.exists() else None,
        "running_case_count": len(running_cases),
        "status": "RUNNING" if running_cases else ("INCOMPLETE" if incomplete_cases else ("IDLE" if cases else "EMPTY")),
        "last_updated_unix_s": int(last_updated_unix_s),
        "cases": cases,
    }


def _ui_history_index_path() -> Path:
    return (_runner_repo_root() / "fluxon_test_stack" / "test_runner" / "ui_history.yaml").resolve()


def _ui_history_load() -> Dict[str, Any]:
    path = _ui_history_index_path()
    if not path.exists():
        return {"schema_version": _TEST_RUNNER_UI_HISTORY_SCHEMA_VERSION, "workdirs": []}
    loaded = _load_yaml_file(path)
    if not isinstance(loaded, dict):
        raise ValueError(f"invalid ui history file: {path}")
    if loaded.get("schema_version") != _TEST_RUNNER_UI_HISTORY_SCHEMA_VERSION:
        raise ValueError(f"ui history schema_version mismatch: {path}")
    workdirs = loaded.get("workdirs")
    if not isinstance(workdirs, list):
        raise ValueError(f"ui history workdirs must be a list: {path}")
    return loaded


def _ui_history_register_workdir(workdir_root: Path) -> None:
    path = _ui_history_index_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    history = _ui_history_load()
    entries = history.get("workdirs")
    if not isinstance(entries, list):
        entries = []
        history["workdirs"] = entries
    resolved = str(workdir_root.resolve())
    now_unix_s = int(time.time())
    updated = False
    for raw in entries:
        if not isinstance(raw, dict):
            continue
        if raw.get("path") == resolved:
            raw["last_seen_unix_s"] = int(now_unix_s)
            updated = True
            break
    if not updated:
        entries.append({"path": resolved, "last_seen_unix_s": int(now_unix_s)})
    dedup: Dict[str, Dict[str, Any]] = {}
    for raw in entries:
        if not isinstance(raw, dict):
            continue
        path_text = raw.get("path")
        if not isinstance(path_text, str) or not path_text.strip():
            continue
        prev = dedup.get(path_text)
        cur_ts = _safe_int(raw.get("last_seen_unix_s"), default=0)
        if prev is None or cur_ts >= _safe_int(prev.get("last_seen_unix_s"), default=0):
            dedup[path_text] = {"path": path_text, "last_seen_unix_s": int(cur_ts)}
    history["workdirs"] = sorted(
        dedup.values(),
        key=lambda item: (_safe_int(item.get("last_seen_unix_s"), default=0), str(item.get("path"))),
        reverse=True,
    )
    _write_yaml_file(path, history)


def _ui_history_list_recent_workdirs(*, now_unix_s: Optional[int] = None, lookback_days: int = _TEST_RUNNER_UI_DEFAULT_LOOKBACK_DAYS) -> List[Path]:
    history = _ui_history_load()
    entries = history.get("workdirs")
    if not isinstance(entries, list):
        return []
    if now_unix_s is None:
        now_unix_s = int(time.time())
    cutoff = int(now_unix_s) - int(lookback_days) * 86400
    out: List[Tuple[int, Path]] = []
    for raw in entries:
        if not isinstance(raw, dict):
            continue
        path_text = raw.get("path")
        if not isinstance(path_text, str) or not path_text.strip():
            continue
        last_seen = _safe_int(raw.get("last_seen_unix_s"), default=0)
        if last_seen < cutoff:
            continue
        path = Path(path_text).resolve()
        if not path.exists() or not path.is_dir():
            continue
        case_runs_path = (path / "case_runs.yaml").resolve()
        if not case_runs_path.exists():
            continue
        out.append((last_seen, path))
    out.sort(key=lambda item: (item[0], str(item[1])), reverse=True)
    return [path for _, path in out]


def _ui_workdir_id(workdir_root: Path) -> str:
    return hashlib.sha256(str(workdir_root.resolve()).encode("utf-8")).hexdigest()[:16]


def _ui_workdir_touch_unix_s(workdir_root: Path) -> int:
    touched = 0
    for name in ("case_runs.yaml", RUNNER_STDIO_LOG_FILENAME):
        path = (workdir_root / name).resolve()
        if not path.exists():
            continue
        try:
            touched = max(touched, int(path.stat().st_mtime))
        except Exception:
            continue
    return int(touched)


def _ui_scan_suite_workdirs_under(root: Path, *, max_depth: int = 5) -> List[Path]:
    resolved_root = root.resolve()
    if not resolved_root.exists() or not resolved_root.is_dir():
        return []
    out: List[Path] = []
    seen: set[Path] = set()
    stack: List[Tuple[Path, int]] = [(resolved_root, 0)]
    while stack:
        cur, depth = stack.pop()
        cur = cur.resolve()
        if cur in seen:
            continue
        seen.add(cur)
        if (cur / "case_runs.yaml").exists():
            out.append(cur)
            continue
        if depth >= max_depth:
            continue
        try:
            children = list(cur.iterdir())
        except Exception:
            continue
        for child in children:
            if not child.is_dir():
                continue
            if child.name in {".git", "__pycache__", "vendor", "venv", "src", "wheels", "backend", "locks"}:
                continue
            stack.append((child.resolve(), depth + 1))
    out.sort(key=lambda path: str(path))
    return out


def _ui_default_external_history_roots() -> List[Path]:
    return []


def _ui_discovery_roots(service_root: Path, extra_history_roots: Optional[List[Path]] = None) -> List[Path]:
    roots = [
        service_root.resolve(),
        (_runner_repo_root() / "fluxon_test_stack" / "test_runner").resolve(),
        (_runner_repo_root() / "fluxon_test_stack" / "bench_runner").resolve(),
        *_ui_default_external_history_roots(),
    ]
    if extra_history_roots:
        roots.extend(path.resolve() for path in extra_history_roots)
    out: List[Path] = []
    seen: set[Path] = set()
    for root in roots:
        if root in seen:
            continue
        seen.add(root)
        out.append(root)
    return out


def _ui_discover_recent_workdirs(
    service_root: Path,
    *,
    lookback_days: int = _TEST_RUNNER_UI_DEFAULT_LOOKBACK_DAYS,
    extra_history_roots: Optional[List[Path]] = None,
) -> List[Path]:
    now_unix_s = int(time.time())
    cutoff = now_unix_s - int(lookback_days) * 86400
    candidates: Dict[str, Tuple[int, Path]] = {}
    for path in _ui_history_list_recent_workdirs(now_unix_s=now_unix_s, lookback_days=lookback_days):
        touched = max(_ui_workdir_touch_unix_s(path), now_unix_s)
        candidates[str(path)] = (int(touched), path)
    for root in _ui_discovery_roots(service_root, extra_history_roots):
        for path in _ui_scan_suite_workdirs_under(root):
            touched = _ui_workdir_touch_unix_s(path)
            if touched < cutoff:
                continue
            prev = candidates.get(str(path))
            if prev is None or touched >= prev[0]:
                candidates[str(path)] = (int(touched), path)
    ordered = sorted(candidates.values(), key=lambda item: (item[0], str(item[1])), reverse=True)
    return [path for _, path in ordered]


def _ui_workdir_by_id(service_root: Path, workdir_id: str, extra_history_roots: Optional[List[Path]] = None) -> Path:
    wanted = _require_str(workdir_id, "workdir_id").strip()
    if not wanted:
        raise ValueError("workdir_id must be non-empty")
    for suite in _ui_collect_recent_suite_history(
        service_root,
        extra_history_roots=extra_history_roots,
    ):
        if suite.get("workdir_id") == wanted:
            workdir_root = suite.get("workdir_root")
            if isinstance(workdir_root, Path):
                return workdir_root.resolve()
            if isinstance(workdir_root, str) and workdir_root.strip():
                return Path(workdir_root).resolve()
            raise ValueError("suite.workdir_root must be a non-empty path")
    raise FileNotFoundError(f"workdir_id not found in recent history: {wanted}")


def _ui_collect_recent_suite_history(
    service_root: Path,
    *,
    lookback_days: int = _TEST_RUNNER_UI_DEFAULT_LOOKBACK_DAYS,
    extra_history_roots: Optional[List[Path]] = None,
) -> List[Dict[str, Any]]:
    cache_key = json.dumps(
        {
            "service_root": str(service_root.resolve()),
            "lookback_days": int(lookback_days),
            "extra_history_roots": [str(path.resolve()) for path in (extra_history_roots or [])],
        },
        sort_keys=True,
    )
    now = time.time()
    with _TEST_RUNNER_UI_HISTORY_CACHE_LOCK:
        cached = _TEST_RUNNER_UI_HISTORY_CACHE.get(cache_key)
        if isinstance(cached, dict):
            expires_at = float(cached.get("expires_at", 0.0) or 0.0)
            cached_value = cached.get("value")
            if expires_at > now and isinstance(cached_value, list):
                return copy.deepcopy(cached_value)
    out: List[Dict[str, Any]] = []
    for workdir_root in _ui_discover_recent_workdirs(
        service_root,
        lookback_days=lookback_days,
        extra_history_roots=extra_history_roots,
    ):
        suite = _ui_collect_suite_overview(workdir_root)
        out.append(
            {
                "workdir_id": _ui_workdir_id(workdir_root),
                **suite,
            }
        )
    out.sort(
        key=lambda item: (
            _safe_int(item.get("last_updated_unix_s"), default=0),
            str(item.get("workdir_root")),
        ),
        reverse=True,
    )
    with _TEST_RUNNER_UI_HISTORY_CACHE_LOCK:
        _TEST_RUNNER_UI_HISTORY_CACHE[cache_key] = {
            "expires_at": float(now + _TEST_RUNNER_UI_HISTORY_CACHE_TTL_SECONDS),
            "value": copy.deepcopy(out),
        }
    return out


def _ui_find_run_dir(workdir_root: Path, *, case_id: str, run_index: int) -> Path:
    case_id_text = _require_str(case_id, "ui.case_id").strip()
    if not case_id_text:
        raise ValueError("ui.case_id must be non-empty")
    run_index_i = _require_int(run_index, "ui.run_index", min_v=1)
    run_dir = (_ui_case_result_root(workdir_root, case_id_text) / _ui_run_dir_name(run_index_i)).resolve()
    if not _ui_is_within_base(workdir_root.resolve(), run_dir):
        raise ValueError(f"invalid run_dir outside workdir: {run_dir}")
    if not run_dir.exists() or not run_dir.is_dir():
        raise FileNotFoundError(f"run_dir not found: {run_dir}")
    return run_dir


def _ui_collect_run_logs(run_dir: Path) -> List[Dict[str, Any]]:
    candidates: List[Tuple[str, Path]] = []
    for rel_path in ("ci.log", "exception.txt", "benchmark_result.json"):
        path = (run_dir / rel_path).resolve()
        if path.exists() and path.is_file():
            candidates.append((rel_path, path))
    logs_root = (run_dir / "logs").resolve()
    if logs_root.exists() and logs_root.is_dir():
        for path in sorted(logs_root.rglob("*")):
            if not path.is_file():
                continue
            if path.suffix not in (".log", ".txt", ".json"):
                continue
            if not _ui_is_within_base(run_dir, path):
                continue
            candidates.append((_ui_relpath(run_dir, path), path.resolve()))
    seen: set[str] = set()
    out: List[Dict[str, Any]] = []
    for label, path in candidates:
        if label in seen:
            continue
        seen.add(label)
        try:
            size = int(path.stat().st_size)
        except Exception:
            size = 0
        out.append({"name": label, "path": path, "size": size})
    out.sort(key=lambda item: item["name"])
    return out


def _ui_resolve_run_log_path(run_dir: Path, *, name: str) -> Path:
    raw_name = _require_str(name, "ui.log_name").strip()
    if not raw_name:
        raise ValueError("ui.log_name must be non-empty")
    candidate = (run_dir / raw_name).resolve()
    if not _ui_is_within_base(run_dir, candidate):
        raise ValueError(f"invalid run log path: {raw_name!r}")
    if not candidate.exists() or not candidate.is_file():
        raise FileNotFoundError(f"run log not found: {candidate}")
    return candidate


def _ui_log_chunk(path: Path, *, from_offset: Optional[int], before_offset: Optional[int], max_bytes: int) -> Dict[str, Any]:
    if max_bytes <= 0:
        raise ValueError("max_bytes must be > 0")
    if max_bytes > _TEST_RUNNER_UI_MAX_LOG_CHUNK_BYTES:
        raise ValueError("max_bytes too large")
    if from_offset is not None and before_offset is not None:
        raise ValueError("from and before are mutually exclusive")
    size = int(path.stat().st_size)
    if from_offset is not None:
        start = max(0, int(from_offset))
        end = min(size, start + int(max_bytes))
    elif before_offset is not None:
        end = min(size, max(0, int(before_offset)))
        start = max(0, end - int(max_bytes))
    else:
        end = size
        start = max(0, size - int(max_bytes))
    with path.open("rb") as fh:
        fh.seek(start)
        data = fh.read(max(0, end - start))
    return {
        "path": str(path),
        "start": int(start),
        "end": int(end),
        "size": int(size),
        "eof": end == size,
        "text": data.decode("utf-8", errors="replace"),
    }


def _ui_ops_logs_base_url(controller_url: str) -> str:
    text = _require_str(controller_url, "controller_url").rstrip("/")
    if "/r/ops/" not in text:
        raise ValueError(f"controller_url is not an ops proxy URL: {text!r}")
    prefix, marker, suffix = text.partition("/r/ops/")
    if not marker or not suffix.strip():
        raise ValueError(f"controller_url is not an ops proxy URL: {text!r}")
    cluster_name = suffix.split("/", 1)[0].strip()
    if not cluster_name:
        raise ValueError(f"controller_url cluster is empty: {text!r}")
    return prefix.rstrip("/") + "/logs"


def _ui_test_stack_member_role_for_instance_id(instance_id: str) -> str:
    if instance_id == "master":
        return "master"
    return "owner_client"


def _ui_test_stack_ops_logs_query(
    resolved_case: Dict[str, Any],
    *,
    instance_id: str,
    after_ts: Optional[str] = None,
    before_ts: Optional[str] = None,
    log_table: Optional[str] = None,
    level: Optional[str] = None,
    search: Optional[str] = None,
) -> Tuple[str, Dict[str, str]]:
    runtime = _require_dict(resolved_case.get("runtime"), "resolved_case.runtime")
    stack_identity = _require_dict(runtime.get("stack_identity"), "resolved_case.runtime.stack_identity")
    cluster_name = _require_str(stack_identity.get("cluster_name"), "resolved_case.runtime.stack_identity.cluster_name")
    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    controller_url = _require_str(deploy.get("controller_url"), "resolved_case.deploy.controller_url").rstrip("/")
    query: Dict[str, str] = {
        "cluster_name": cluster_name,
        "member_kind": "kv",
        "role": _ui_test_stack_member_role_for_instance_id(instance_id),
        "member_id": _require_str(instance_id, "instance_id"),
    }
    if after_ts is not None:
        query["after_ts"] = _require_str(after_ts, "after_ts")
    if before_ts is not None:
        query["before_ts"] = _require_str(before_ts, "before_ts")
    if log_table is not None and str(log_table).strip():
        query["log_table"] = str(log_table).strip()
    if level is not None and str(level).strip():
        query["level"] = str(level).strip()
    if search is not None and str(search).strip():
        query["search"] = str(search).strip()
    return _ui_ops_logs_base_url(controller_url), query


def _ui_collect_run_instance_statuses(run_summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    resolved_case = run_summary.get("resolved_case")
    if not isinstance(resolved_case, dict):
        return []
    deploy = resolved_case.get("deploy")
    if not isinstance(deploy, dict):
        return []
    raw_instances = deploy.get("instances")
    if not isinstance(raw_instances, list):
        return []
    out: List[Dict[str, Any]] = []
    for raw in raw_instances:
        if not isinstance(raw, dict):
            continue
        instance_id = raw.get("id")
        if not isinstance(instance_id, str) or not instance_id.strip():
            continue
        item: Dict[str, Any] = {
            "instance_id": instance_id,
            "target": "",
            "k8s_ref": "",
            "status": None,
            "status_error": None,
        }
        deployer = raw.get("deployer")
        if isinstance(deployer, dict):
            target = deployer.get("target")
            if isinstance(target, str):
                item["target"] = target
        k8s_ref = raw.get("k8s_ref")
        if isinstance(k8s_ref, str):
            item["k8s_ref"] = k8s_ref
        try:
            item["status"] = _instance_status(resolved_case, instance_id=instance_id)
        except Exception as exc:
            item["status_error"] = f"{type(exc).__name__}: {exc}"
        out.append(item)
    out.sort(key=lambda item: str(item.get("instance_id")))
    return out


def _ui_http_get_json(url: str) -> Dict[str, Any]:
    req = _new_controller_request(url, method="GET")
    status_code, payload = _http_json_allow_error_status(req)
    if int(status_code) != 200:
        raise ValueError(f"http status={status_code} url={url}")
    return payload


def _ui_fetch_ops_logs(
    resolved_case: Dict[str, Any],
    *,
    instance_id: str,
    after_ts: Optional[str],
    before_ts: Optional[str],
    log_table: Optional[str],
    level: Optional[str],
    search: Optional[str],
) -> Dict[str, Any]:
    base_url, query = _ui_test_stack_ops_logs_query(
        resolved_case,
        instance_id=instance_id,
        after_ts=after_ts,
        before_ts=before_ts,
        log_table=log_table,
        level=level,
        search=search,
    )
    url = base_url + "?" + urllib.parse.urlencode(query)
    return _ui_http_get_json(url)


def _ui_render_log_viewer_html(*, title: str, back_href: str, bootstrap_query: Dict[str, str]) -> str:
    title_html = _html_escape(title)
    back_href_html = _html_escape(back_href)
    bootstrap_json = _html_escape(json.dumps(bootstrap_query, ensure_ascii=False, sort_keys=True))
    return """<!doctype html>
<html><head><meta charset='utf-8'/>
<meta name='viewport' content='width=device-width, initial-scale=1'/>
<title>@@TITLE@@</title>
<style>
body{font-family:ui-sans-serif,system-ui,Segoe UI,Roboto,Helvetica,Arial;max-width:1400px;margin:16px auto;padding:0 12px;}
code,pre{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;}
#log{white-space:pre-wrap;background:#0b1020;color:#e5e7eb;padding:12px;border-radius:8px;height:78vh;overflow:auto;font-size:12px;line-height:1.35;tab-size:4;}
.small{color:#6b7280;font-size:12px;}
.btn{padding:4px 8px;border:1px solid #e5e7eb;border-radius:6px;background:#fff;cursor:pointer;}
.row{display:flex;gap:10px;align-items:center;flex-wrap:wrap;}
.inp{padding:4px 8px;border:1px solid #e5e7eb;border-radius:6px;background:#fff;}
</style>
</head><body>
<a href='@@BACK_HREF@@'>back</a>
<h2>@@TITLE@@</h2>
<div class='row'>
  <button class='btn' id='btnFollow'>follow: on</button>
  <button class='btn' id='btnReload'>reload tail</button>
  <label class='small'>search <input class='inp' id='search' /></label>
  <label class='small'>level
    <select class='inp' id='level'>
      <option value=''>all</option>
      <option value='info'>info+</option>
      <option value='warn'>warn+</option>
      <option value='error'>error</option>
    </select>
  </label>
  <div class='small' id='status'></div>
</div>
<div id='log'></div>
<script>
(function(){
  const bootstrap = JSON.parse('@@BOOTSTRAP_JSON@@');
  const logEl = document.getElementById('log');
  const statusEl = document.getElementById('status');
  const btnFollow = document.getElementById('btnFollow');
  const btnReload = document.getElementById('btnReload');
  const searchEl = document.getElementById('search');
  const levelEl = document.getElementById('level');
  let follow = true;
  let loading = false;
  let loadedStart = null;
  let loadedEnd = null;
  const MAX_BYTES = 65536;
  const POLL_MS = 1200;
  function setStatus(s){ statusEl.textContent = s; }
  function nearBottom(){ return (logEl.scrollHeight - (logEl.scrollTop + logEl.clientHeight)) < 40; }
  function scrollToBottom(){ logEl.scrollTop = logEl.scrollHeight; }
  function buildUrl(params){
    const url = new URL(bootstrap.api_path, window.location.origin);
    Object.entries(bootstrap.query || {}).forEach(([k,v]) => {
      if (v !== null && v !== undefined && String(v).length > 0) url.searchParams.set(k, String(v));
    });
    url.searchParams.set('max_bytes', String(MAX_BYTES));
    if (searchEl.value.trim().length > 0) url.searchParams.set('search', searchEl.value.trim());
    const level = levelEl.value.trim();
    if (level.length > 0) url.searchParams.set('level', level);
    Object.entries(params || {}).forEach(([k,v]) => {
      if (v !== null && v !== undefined && String(v).length > 0) {
        url.searchParams.set(k, String(v));
      }
    });
    return url.toString();
  }
  async function fetchChunk(params){
    const resp = await fetch(buildUrl(params), { cache: 'no-store' });
    const text = await resp.text();
    if (!resp.ok) throw new Error('http ' + resp.status + ' ' + text);
    return JSON.parse(text);
  }
  function extractText(data){
    if (typeof data.text === 'string') return data.text;
    if (Array.isArray(data.items)) {
      const items = data.items.slice();
      const tailMode = !!data.after_ts_used;
      const ordered = tailMode ? items : items.slice().reverse();
      return ordered.map((it) => {
        const ts = it && it.ts ? '[' + String(it.ts) + '] ' : '';
        const mid = it && it.member_id ? String(it.member_id) + ' ' : '';
        const body = it && it.body ? String(it.body) : JSON.stringify(it);
        return ts + mid + body;
      }).join('\\n') + (ordered.length > 0 ? '\\n' : '');
    }
    return JSON.stringify(data, null, 2);
  }
  function extractWindow(data){
    if (typeof data.start === 'number' && typeof data.end === 'number') return [data.start, data.end, data.size || 0];
    if (Array.isArray(data.items) && data.items.length > 0) {
      const vals = data.items.map((it) => Number(it.ts || 0)).filter((v) => Number.isFinite(v));
      if (vals.length > 0) return [Math.min.apply(null, vals), Math.max.apply(null, vals), vals.length];
    }
    return [0, 0, 0];
  }
  async function loadTail(){
    if (loading) return;
    loading = true;
    try {
      setStatus('loading tail...');
      const data = await fetchChunk({});
      logEl.textContent = extractText(data);
      const win = extractWindow(data);
      loadedStart = win[0];
      loadedEnd = win[1];
      setStatus('loaded=[' + loadedStart + ',' + loadedEnd + '] size=' + win[2]);
      scrollToBottom();
    } catch (e) {
      setStatus('error: ' + (e && e.message ? e.message : String(e)));
    } finally {
      loading = false;
    }
  }
  async function pollAppend(){
    if (!follow || loading || loadedEnd === null) return;
    loading = true;
    try {
      const shouldScroll = nearBottom();
      const data = await fetchChunk({ from: loadedEnd });
      const text = extractText(data);
      if (text.length > 0) logEl.textContent += text;
      const win = extractWindow(data);
      if (win[1] > loadedEnd) loadedEnd = win[1];
      if (shouldScroll) scrollToBottom();
      setStatus('loaded=[' + loadedStart + ',' + loadedEnd + '] size=' + win[2]);
    } catch (e) {
      setStatus('error: ' + (e && e.message ? e.message : String(e)));
    } finally {
      loading = false;
    }
  }
  async function loadMoreBefore(){
    if (loading || loadedStart === null || loadedStart <= 0) return;
    loading = true;
    try {
      const prevScrollHeight = logEl.scrollHeight;
      const data = await fetchChunk({ before: loadedStart });
      const text = extractText(data);
      if (text.length > 0) {
        logEl.textContent = text + logEl.textContent;
        const win = extractWindow(data);
        loadedStart = win[0];
        const newScrollHeight = logEl.scrollHeight;
        logEl.scrollTop = newScrollHeight - prevScrollHeight;
      }
    } catch (e) {
      setStatus('error: ' + (e && e.message ? e.message : String(e)));
    } finally {
      loading = false;
    }
  }
  btnFollow.addEventListener('click', function(){
    follow = !follow;
    btnFollow.textContent = 'follow: ' + (follow ? 'on' : 'off');
    if (follow) scrollToBottom();
  });
  btnReload.addEventListener('click', function(){ loadTail(); });
  searchEl.addEventListener('change', function(){ loadedStart = null; loadedEnd = null; loadTail(); });
  levelEl.addEventListener('change', function(){ loadedStart = null; loadedEnd = null; loadTail(); });
  logEl.addEventListener('scroll', function(){ if (logEl.scrollTop < 20) loadMoreBefore(); });
  loadTail().then(function(){ setInterval(pollAppend, POLL_MS); });
})();
</script>
</body></html>""".replace("@@TITLE@@", title_html).replace("@@BACK_HREF@@", back_href_html).replace("@@BOOTSTRAP_JSON@@", bootstrap_json)


def _serve_test_runner_ui(
    *,
    workdir_root: Path,
    host: str,
    port: int,
    lookback_days: int,
    extra_history_roots: Optional[List[Path]],
    gitops_ctx: Optional[gitops_lib.GitOpsContext],
) -> None:
    if not workdir_root.exists() or not workdir_root.is_dir():
        raise ValueError(f"ui workdir does not exist or is not a directory: {workdir_root}")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args) -> None:  # noqa: A003
            pass

        def _send_json(self, code: int, payload: Dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, code: int, text: str) -> None:
            body = text.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _require_gitops_ctx_html(self) -> Optional[gitops_lib.GitOpsContext]:
            if gitops_ctx is None:
                self._send_html(404, "gitops is not configured for this test_runner service")
                return None
            return gitops_ctx

        def _require_gitops_ctx_json(self) -> Optional[gitops_lib.GitOpsContext]:
            if gitops_ctx is None:
                self._send_json(404, {"error": "gitops is not configured for this test_runner service"})
                return None
            return gitops_ctx

        def _gitops_event_text(self, event: Any) -> str:
            if not isinstance(event, dict):
                return ""
            name = str(event.get("event") or "").strip()
            payload = event.get("payload")
            parts: List[str] = []
            if name:
                parts.append(name)
            if isinstance(payload, dict):
                idx = _safe_int(payload.get("idx"), default=0)
                if idx > 0:
                    parts.append(f"step={idx}")
                if payload.get("rc") is not None:
                    parts.append(f"rc={payload.get('rc')}")
            return " ".join(parts)

        def _gitops_run_payload(
            self,
            ctx: gitops_lib.GitOpsContext,
            run_id: str,
            info: Dict[str, Any],
        ) -> Dict[str, Any]:
            try:
                last_event = gitops_lib.get_run_last_event(ctx, run_id)
            except Exception:
                last_event = None
            try:
                step_count = gitops_lib.get_run_step_count(ctx, run_id)
            except Exception:
                step_count = 0
            started_ts = str(info.get("started_ts") or "")
            finished_ts = str(info.get("finished_ts") or "")
            return {
                "id": run_id,
                "status": str(info.get("status") or ""),
                "repo": str(info.get("repo") or ""),
                "branch": str(info.get("branch") or ""),
                "commit": str(info.get("commit") or ""),
                "name_prefix": str(info.get("name_prefix") or ""),
                "rc": info.get("rc"),
                "failed_step": info.get("failed_step"),
                "started_ts": started_ts,
                "started_text": _ui_format_iso_ts(started_ts),
                "finished_ts": finished_ts,
                "finished_text": _ui_format_iso_ts(finished_ts),
                "run_dir": str(info.get("run_dir") or ""),
                "meta_dir": str(info.get("meta_dir") or ""),
                "log_file": str(info.get("log_file") or ""),
                "progress_file": str(info.get("progress_file") or ""),
                "result_file": str(info.get("result_file") or ""),
                "last_event": last_event,
                "last_event_text": self._gitops_event_text(last_event),
                "step_count": int(step_count),
            }

        def _gitops_read_target_from_request(self, parsed) -> Optional[str]:
            target = (parse_qs(parsed.query).get("target") or [None])[0]
            length = _safe_int(self.headers.get("Content-Length"), default=0)
            if length <= 0:
                return str(target).strip() if isinstance(target, str) and target.strip() else None
            body = self.rfile.read(length).decode("utf-8")
            if body:
                try:
                    payload = json.loads(body)
                except Exception:
                    payload = None
                if isinstance(payload, dict):
                    raw_target = payload.get("target")
                    if isinstance(raw_target, str) and raw_target.strip():
                        return raw_target.strip()
                form_target = (parse_qs(body).get("target") or [None])[0]
                if isinstance(form_target, str) and form_target.strip():
                    return form_target.strip()
            return str(target).strip() if isinstance(target, str) and target.strip() else None

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "service": "test_runner_ui",
                        "host": str(host),
                        "port": int(port),
                        "workdir_root": str(workdir_root.resolve()),
                        "lookback_days": int(lookback_days),
                        "history_roots": [str(path.resolve()) for path in (extra_history_roots or [])],
                        "gitops_configured": gitops_ctx is not None,
                        "gitops_config_path": (
                            str(gitops_ctx.config_path.resolve()) if gitops_ctx is not None else None
                        ),
                    },
                )
                return
            if parsed.path == "/gitops":
                self._handle_gitops_index()
                return
            if parsed.path == "/gitops/run":
                self._handle_gitops_run(parsed)
                return
            if parsed.path == "/gitops/log":
                self._handle_gitops_log(parsed)
                return
            if parsed.path == "/gitops/step_log":
                self._handle_gitops_step_log(parsed)
                return
            if parsed.path == "/":
                self._handle_index()
                return
            if parsed.path == "/suite":
                self._handle_suite(parsed)
                return
            if parsed.path == "/case":
                self._handle_case(parsed)
                return
            if parsed.path == "/run":
                self._handle_run(parsed)
                return
            if parsed.path == "/log":
                self._handle_log(parsed)
                return
            if parsed.path == "/ops_log":
                self._handle_ops_log(parsed)
                return
            if parsed.path == "/api/suite_state":
                self._handle_api_suite_state()
                return
            if parsed.path == "/api/gitops/state":
                self._handle_api_gitops_state()
                return
            if parsed.path == "/api/gitops/run_state":
                self._handle_api_gitops_run_state(parsed)
                return
            if parsed.path == "/api/gitops/log_chunk":
                self._handle_api_gitops_log_chunk(parsed)
                return
            if parsed.path == "/api/run_state":
                self._handle_api_run_state(parsed)
                return
            if parsed.path == "/api/log_chunk":
                self._handle_api_log_chunk(parsed)
                return
            if parsed.path == "/api/ops_log_chunk":
                self._handle_api_ops_log_chunk(parsed)
                return
            self._send_json(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/api/gitops/rerun":
                self._handle_api_gitops_rerun(parsed)
                return
            self._send_json(404, {"error": "not found"})

        def _handle_index(self) -> None:
            suites = _ui_collect_recent_suite_history(
                workdir_root,
                lookback_days=lookback_days,
                extra_history_roots=extra_history_roots,
            )
            rows: List[str] = []
            total_running = 0
            for suite in suites:
                running_case_count = _safe_int(suite.get("running_case_count"), default=0)
                total_running += running_case_count
                rows.append(
                    "<tr>"
                    f"<td><a href='/suite?workdir_id={_html_escape(suite['workdir_id'])}'>{_html_escape(suite['workdir_id'])}</a></td>"
                    f"<td>{_html_escape(suite.get('status', ''))}</td>"
                    f"<td>{_html_escape(running_case_count)}</td>"
                    f"<td>{_html_escape(len(suite.get('cases', [])))}</td>"
                    f"<td>{_html_escape(_ui_format_unix_ts(suite.get('last_updated_unix_s', 0)))}</td>"
                    f"<td>{_html_escape(suite.get('workdir_root', ''))}</td>"
                    "</tr>"
                )
            gitops_link_html = ""
            if gitops_ctx is not None:
                gitops_desc = gitops_lib.describe_context(gitops_ctx)
                gitops_link_html = (
                    "<div class='small'>"
                    f"<a href='/gitops'>gitops</a> "
                    f"repos={_html_escape(gitops_desc['repo_count'])} "
                    f"interval={_html_escape(gitops_desc['interval'])}s "
                    f"workdir={_html_escape(gitops_desc['workdir'])}"
                    "</div>"
                )
            html_text = """<!doctype html>
<html><head><meta charset='utf-8'/>
<meta name='viewport' content='width=device-width, initial-scale=1'/>
<title>Test Runner UI</title>
<style>
body{font-family:ui-sans-serif,system-ui,Segoe UI,Roboto,Helvetica,Arial;max-width:1400px;margin:16px auto;padding:0 12px;}
.table{width:100%;border-collapse:collapse;}
.table th,.table td{border:1px solid #e5e7eb;padding:6px 8px;text-align:left;vertical-align:top;}
.table th{background:#f9fafb;}
.small{color:#6b7280;font-size:12px;}
</style></head><body>
<h1>Test Runner</h1>
<div class='small'>service_root: @@WORKDIR@@</div>
<div class='small'>history_window_days: @@LOOKBACK_DAYS@@</div>
<div class='small'>recent_suites: @@SUITE_COUNT@@ running_cases: @@RUNNING_CASES@@</div>
@@GITOPS_LINK@@
<table class='table'>
<tr><th>suite</th><th>status</th><th>running_cases</th><th>case_count</th><th>last_updated</th><th>workdir</th></tr>
@@ROWS@@
</table>
</body></html>"""
            html_text = (
                html_text
                .replace("@@WORKDIR@@", _html_escape(workdir_root.resolve()))
                .replace("@@LOOKBACK_DAYS@@", _html_escape(lookback_days))
                .replace("@@SUITE_COUNT@@", _html_escape(len(suites)))
                .replace("@@RUNNING_CASES@@", _html_escape(total_running))
                .replace("@@GITOPS_LINK@@", gitops_link_html)
                .replace("@@ROWS@@", "\n".join(rows))
            )
            self._send_html(200, html_text)

        def _handle_suite(self, parsed) -> None:
            qs = parse_qs(parsed.query)
            workdir_id = (qs.get("workdir_id") or [""])[0]
            if not workdir_id:
                self._send_html(400, "missing workdir_id")
                return
            suite_workdir = _ui_workdir_by_id(workdir_root, workdir_id, extra_history_roots)
            suite = _ui_collect_suite_overview(suite_workdir)
            rows: List[str] = []
            for case in suite["cases"]:
                last_run = case.get("last_run") or {}
                run_index = _html_escape(last_run.get("run_index") or case.get("active_run_index") or "")
                rows.append(
                    "<tr>"
                    f"<td><a href='/case?workdir_id={_html_escape(workdir_id)}&id={_html_escape(case['case_id'])}'>{_html_escape(case['case_id'])}</a></td>"
                    f"<td>{_html_escape(case.get('case_key', ''))}</td>"
                    f"<td>{_html_escape(case.get('status', ''))}</td>"
                    f"<td>{run_index}</td>"
                    f"<td>{_html_escape(case.get('counted_runs', 0))}</td>"
                    f"<td>{_html_escape(case.get('success_runs', 0))}</td>"
                    f"<td>{_html_escape(case.get('failed_runs', 0))}</td>"
                    "</tr>"
                )
            runner_link = ""
            if isinstance(suite.get("runner_log_path"), Path):
                runner_link = f"<div><a href='/log?kind=runner&workdir_id={_html_escape(workdir_id)}'>runner log</a></div>"
            html_text = """<!doctype html>
<html><head><meta charset='utf-8'/>
<meta name='viewport' content='width=device-width, initial-scale=1'/>
<title>Suite @@WORKDIR_ID@@</title>
<style>
body{font-family:ui-sans-serif,system-ui,Segoe UI,Roboto,Helvetica,Arial;max-width:1400px;margin:16px auto;padding:0 12px;}
.table{width:100%;border-collapse:collapse;}
.table th,.table td{border:1px solid #e5e7eb;padding:6px 8px;text-align:left;vertical-align:top;}
.table th{background:#f9fafb;}
.small{color:#6b7280;font-size:12px;}
</style></head><body>
<a href='/'>back</a>
<h2>Suite @@WORKDIR_ID@@</h2>
<div class='small'>workdir: @@WORKDIR@@</div>
<div class='small'>case_runs: @@CASE_RUNS@@</div>
<div class='small'>status: @@STATUS@@ running_cases: @@RUNNING_CASES@@</div>
@@RUNNER_LINK@@
<table class='table'>
<tr><th>case_id</th><th>case_key</th><th>status</th><th>active_run</th><th>counted</th><th>success</th><th>failed</th></tr>
@@ROWS@@
</table>
</body></html>"""
            html_text = (
                html_text
                .replace("@@WORKDIR_ID@@", _html_escape(workdir_id))
                .replace("@@WORKDIR@@", _html_escape(suite["workdir_root"]))
                .replace("@@CASE_RUNS@@", _html_escape(suite["case_runs_path"]))
                .replace("@@STATUS@@", _html_escape(suite.get("status", "")))
                .replace("@@RUNNING_CASES@@", _html_escape(suite.get("running_case_count", 0)))
                .replace("@@RUNNER_LINK@@", runner_link)
                .replace("@@ROWS@@", "\n".join(rows))
            )
            self._send_html(200, html_text)

        def _handle_case(self, parsed) -> None:
            qs = parse_qs(parsed.query)
            workdir_id = (qs.get("workdir_id") or [""])[0]
            case_id = (qs.get("id") or [""])[0]
            if not workdir_id or not case_id:
                self._send_html(400, "missing workdir_id or id")
                return
            suite_workdir = _ui_workdir_by_id(workdir_root, workdir_id, extra_history_roots)
            case = _ui_case_overview(suite_workdir, case_id=case_id)
            rows: List[str] = []
            for run in case["runs"]:
                run_index = _safe_int(run.get("run_index"), default=0)
                rows.append(
                    "<tr>"
                    f"<td><a href='/run?workdir_id={_html_escape(workdir_id)}&case_id={_html_escape(case_id)}&run_index={run_index}'>{run_index}</a></td>"
                    f"<td>{_html_escape(run.get('outcome', ''))}</td>"
                    f"<td>{_html_escape(run.get('counted', False))}</td>"
                    f"<td>{_html_escape(_ui_format_unix_ts(run.get('started_at_unix_s', 0)))}</td>"
                    f"<td>{_html_escape(_ui_format_unix_ts(run.get('finished_at_unix_s', 0)))}</td>"
                    f"<td>{_html_escape(run.get('family', ''))}</td>"
                    "</tr>"
                )
            html_text = """<!doctype html>
<html><head><meta charset='utf-8'/>
<meta name='viewport' content='width=device-width, initial-scale=1'/>
<title>Case @@CASE_ID@@</title>
<style>
body{font-family:ui-sans-serif,system-ui,Segoe UI,Roboto,Helvetica,Arial;max-width:1400px;margin:16px auto;padding:0 12px;}
.table{width:100%;border-collapse:collapse;}
.table th,.table td{border:1px solid #e5e7eb;padding:6px 8px;text-align:left;vertical-align:top;}
.table th{background:#f9fafb;}
.small{color:#6b7280;font-size:12px;}
</style></head><body>
<a href='/suite?workdir_id=@@WORKDIR_ID@@'>back</a>
<h2>@@CASE_ID@@</h2>
<div class='small'>case_key: @@CASE_KEY@@</div>
<table class='table'>
<tr><th>run</th><th>outcome</th><th>counted</th><th>started</th><th>finished</th><th>family</th></tr>
@@ROWS@@
</table>
</body></html>"""
            html_text = (
                html_text
                .replace("@@WORKDIR_ID@@", _html_escape(workdir_id))
                .replace("@@CASE_ID@@", _html_escape(case["case_id"]))
                .replace("@@CASE_KEY@@", _html_escape(case["case_key"]))
                .replace("@@ROWS@@", "\n".join(rows))
            )
            self._send_html(200, html_text)

        def _handle_run(self, parsed) -> None:
            qs = parse_qs(parsed.query)
            workdir_id = (qs.get("workdir_id") or [""])[0]
            case_id = (qs.get("case_id") or [""])[0]
            run_index_raw = (qs.get("run_index") or [""])[0]
            if not workdir_id or not case_id or not run_index_raw:
                self._send_html(400, "missing workdir_id, case_id, or run_index")
                return
            suite_workdir = _ui_workdir_by_id(workdir_root, workdir_id, extra_history_roots)
            run_dir = _ui_find_run_dir(suite_workdir, case_id=case_id, run_index=int(run_index_raw))
            run_summary = _ui_case_run_summary(run_dir)
            logs = _ui_collect_run_logs(run_dir)
            log_links = []
            for item in logs:
                log_links.append(
                    f"<li><a href='/log?workdir_id={_html_escape(workdir_id)}&case_id={_html_escape(case_id)}&run_index={_html_escape(run_index_raw)}&name={urllib.parse.quote(str(item['name']), safe='')}'>"
                    f"{_html_escape(item['name'])}</a> <span class='small'>{_html_escape(item['size'])} bytes</span></li>"
                )
            instance_rows = []
            for item in _ui_collect_run_instance_statuses(run_summary):
                status_json = ""
                if isinstance(item.get("status"), dict):
                    status_json = json.dumps(item["status"], ensure_ascii=False)
                else:
                    status_json = str(item.get("status_error") or "")
                ops_link = ""
                if isinstance(run_summary.get("resolved_case"), dict):
                    ops_link = (
                        f"<a href='/ops_log?workdir_id={_html_escape(workdir_id)}&case_id={_html_escape(case_id)}&run_index={_html_escape(run_index_raw)}"
                        f"&instance_id={_html_escape(item['instance_id'])}'>ops log</a>"
                    )
                instance_rows.append(
                    "<tr>"
                    f"<td>{_html_escape(item.get('instance_id', ''))}</td>"
                    f"<td>{_html_escape(item.get('target', ''))}</td>"
                    f"<td>{_html_escape(item.get('k8s_ref', ''))}</td>"
                    f"<td><code>{_html_escape(status_json)}</code></td>"
                    f"<td>{ops_link}</td>"
                    "</tr>"
                )
            summary_json = json.dumps(run_summary.get("summary") or {}, ensure_ascii=False, indent=2, sort_keys=True)
            html_text = """<!doctype html>
<html><head><meta charset='utf-8'/>
<meta name='viewport' content='width=device-width, initial-scale=1'/>
<title>Run @@CASE_ID@@ @@RUN_INDEX@@</title>
<style>
body{font-family:ui-sans-serif,system-ui,Segoe UI,Roboto,Helvetica,Arial;max-width:1400px;margin:16px auto;padding:0 12px;}
.table{width:100%;border-collapse:collapse;}
.table th,.table td{border:1px solid #e5e7eb;padding:6px 8px;text-align:left;vertical-align:top;}
.table th{background:#f9fafb;}
.small{color:#6b7280;font-size:12px;}
pre{background:#0b1020;color:#e5e7eb;padding:10px;border-radius:8px;overflow:auto;}
</style></head><body>
<a href='/case?workdir_id=@@WORKDIR_ID@@&id=@@CASE_ID@@'>back</a>
<h2>Run @@RUN_INDEX@@</h2>
<div class='small'>run_dir: @@RUN_DIR@@</div>
<div>outcome: <code>@@OUTCOME@@</code></div>
<div>family: <code>@@FAMILY@@</code></div>
<h3>Logs</h3>
<ul>@@LOG_LINKS@@</ul>
<h3>Instances</h3>
<table class='table'>
<tr><th>instance_id</th><th>target</th><th>k8s_ref</th><th>status</th><th>log</th></tr>
@@INSTANCE_ROWS@@
</table>
<h3>summary.yaml</h3>
<pre>@@SUMMARY_JSON@@</pre>
</body></html>"""
            html_text = (
                html_text
                .replace("@@WORKDIR_ID@@", _html_escape(workdir_id))
                .replace("@@CASE_ID@@", _html_escape(case_id))
                .replace("@@RUN_INDEX@@", _html_escape(run_index_raw))
                .replace("@@RUN_DIR@@", _html_escape(run_dir))
                .replace("@@OUTCOME@@", _html_escape(run_summary.get("outcome", "")))
                .replace("@@FAMILY@@", _html_escape(run_summary.get("family", "")))
                .replace("@@LOG_LINKS@@", "\n".join(log_links))
                .replace("@@INSTANCE_ROWS@@", "\n".join(instance_rows))
                .replace("@@SUMMARY_JSON@@", _html_escape(summary_json))
            )
            self._send_html(200, html_text)

        def _handle_log(self, parsed) -> None:
            qs = parse_qs(parsed.query)
            kind = (qs.get("kind") or ["run"])[0]
            if kind == "runner":
                workdir_id = (qs.get("workdir_id") or [""])[0]
                if not workdir_id:
                    self._send_html(400, "missing workdir_id")
                    return
                html_text = _ui_render_log_viewer_html(
                    title="runner log",
                    back_href=f"/suite?workdir_id={urllib.parse.quote(workdir_id, safe='')}",
                    bootstrap_query={"api_path": "/api/log_chunk", "query": {"kind": "runner", "workdir_id": workdir_id}},
                )
                self._send_html(200, html_text)
                return
            workdir_id = (qs.get("workdir_id") or [""])[0]
            case_id = (qs.get("case_id") or [""])[0]
            run_index_raw = (qs.get("run_index") or [""])[0]
            name = (qs.get("name") or [""])[0]
            if not workdir_id or not case_id or not run_index_raw or not name:
                self._send_html(400, "missing workdir_id, case_id, run_index, or name")
                return
            html_text = _ui_render_log_viewer_html(
                title=f"{case_id} run_{run_index_raw} {name}",
                back_href=f"/run?workdir_id={urllib.parse.quote(workdir_id, safe='')}&case_id={urllib.parse.quote(case_id, safe='')}&run_index={urllib.parse.quote(run_index_raw, safe='')}",
                bootstrap_query={
                    "api_path": "/api/log_chunk",
                    "query": {"kind": "run", "workdir_id": workdir_id, "case_id": case_id, "run_index": run_index_raw, "name": name},
                },
            )
            self._send_html(200, html_text)

        def _handle_ops_log(self, parsed) -> None:
            qs = parse_qs(parsed.query)
            workdir_id = (qs.get("workdir_id") or [""])[0]
            case_id = (qs.get("case_id") or [""])[0]
            run_index_raw = (qs.get("run_index") or [""])[0]
            instance_id = (qs.get("instance_id") or [""])[0]
            if not workdir_id or not case_id or not run_index_raw or not instance_id:
                self._send_html(400, "missing workdir_id, case_id, run_index, or instance_id")
                return
            html_text = _ui_render_log_viewer_html(
                title=f"ops log {instance_id}",
                back_href=f"/run?workdir_id={urllib.parse.quote(workdir_id, safe='')}&case_id={urllib.parse.quote(case_id, safe='')}&run_index={urllib.parse.quote(run_index_raw, safe='')}",
                bootstrap_query={
                    "api_path": "/api/ops_log_chunk",
                    "query": {"workdir_id": workdir_id, "case_id": case_id, "run_index": run_index_raw, "instance_id": instance_id},
                },
            )
            self._send_html(200, html_text)

        def _handle_api_suite_state(self) -> None:
            suites = _ui_collect_recent_suite_history(
                workdir_root,
                lookback_days=lookback_days,
                extra_history_roots=extra_history_roots,
            )
            out_suites = []
            for suite in suites:
                out_suites.append(
                    {
                        "workdir_id": suite["workdir_id"],
                        "workdir_root": str(suite["workdir_root"]),
                        "status": suite["status"],
                        "running_case_count": suite["running_case_count"],
                        "last_updated_unix_s": suite["last_updated_unix_s"],
                        "last_updated_text": _ui_format_unix_ts(suite["last_updated_unix_s"]),
                        "case_count": len(suite["cases"]),
                    }
                )
            self._send_json(
                200,
                {
                    "ok": True,
                    "service_root": str(workdir_root.resolve()),
                    "lookback_days": int(lookback_days),
                    "suites": out_suites,
                },
            )

        def _handle_gitops_index(self) -> None:
            ctx = self._require_gitops_ctx_html()
            if ctx is None:
                return
            try:
                desc = gitops_lib.describe_context(ctx)
                runs = [
                    self._gitops_run_payload(ctx, run_id, info)
                    for run_id, info in gitops_lib.list_runs(ctx)
                ]
            except Exception as exc:
                self._send_html(500, f"gitops state load failed: {type(exc).__name__}: {exc}")
                return
            rows: List[str] = []
            for item in runs:
                rows.append(
                    "<tr>"
                    f"<td><a href='/gitops/run?id={urllib.parse.quote(item['id'], safe='')}'>{_html_escape(item['id'])}</a></td>"
                    f"<td>{_html_escape(item['status'])}</td>"
                    f"<td>{_html_escape(item['rc'])}</td>"
                    f"<td>{_html_escape(item['name_prefix'])}</td>"
                    f"<td>{_html_escape(item['repo'])}</td>"
                    f"<td>{_html_escape(item['branch'])}</td>"
                    f"<td>{_html_escape(item['commit'][:12])}</td>"
                    f"<td>{_html_escape(item['started_text'])}</td>"
                    f"<td>{_html_escape(item['finished_text'])}</td>"
                    f"<td>{_html_escape(item['last_event_text'])}</td>"
                    "</tr>"
                )
            html_text = """<!doctype html>
<html><head><meta charset='utf-8'/>
<meta name='viewport' content='width=device-width, initial-scale=1'/>
<title>GitOps</title>
<style>
body{font-family:ui-sans-serif,system-ui,Segoe UI,Roboto,Helvetica,Arial;max-width:1400px;margin:16px auto;padding:0 12px;}
.table{width:100%;border-collapse:collapse;}
.table th,.table td{border:1px solid #e5e7eb;padding:6px 8px;text-align:left;vertical-align:top;}
.table th{background:#f9fafb;}
.small{color:#6b7280;font-size:12px;}
.inp{padding:4px 8px;border:1px solid #e5e7eb;border-radius:6px;background:#fff;min-width:520px;}
.btn{padding:4px 8px;border:1px solid #e5e7eb;border-radius:6px;background:#fff;cursor:pointer;}
.row{display:flex;gap:10px;align-items:center;flex-wrap:wrap;}
</style></head><body>
<a href='/'>back</a>
<h1>GitOps</h1>
<div class='small'>config: @@CONFIG@@</div>
<div class='small'>workdir: @@WORKDIR@@</div>
<div class='small'>repos: @@REPO_COUNT@@ interval: @@INTERVAL@@s retention: @@RETENTION@@d runs: @@RUN_COUNT@@</div>
<form class='row' method='post' action='/api/gitops/rerun' style='margin:12px 0;'>
  <label>rerun target</label>
  <input class='inp' name='target' placeholder='repo:branch:commit' />
  <button class='btn' type='submit'>rerun</button>
</form>
<table class='table'>
<tr><th>run_id</th><th>status</th><th>rc</th><th>name_prefix</th><th>repo</th><th>branch</th><th>commit</th><th>started</th><th>finished</th><th>last_event</th></tr>
@@ROWS@@
</table>
</body></html>"""
            html_text = (
                html_text
                .replace("@@CONFIG@@", _html_escape(desc["config_path"]))
                .replace("@@WORKDIR@@", _html_escape(desc["workdir"]))
                .replace("@@REPO_COUNT@@", _html_escape(desc["repo_count"]))
                .replace("@@INTERVAL@@", _html_escape(desc["interval"]))
                .replace("@@RETENTION@@", _html_escape(desc["max_age_days"]))
                .replace("@@RUN_COUNT@@", _html_escape(len(runs)))
                .replace("@@ROWS@@", "\n".join(rows))
            )
            self._send_html(200, html_text)

        def _handle_gitops_run(self, parsed) -> None:
            ctx = self._require_gitops_ctx_html()
            if ctx is None:
                return
            run_id = (parse_qs(parsed.query).get("id") or [""])[0]
            if not run_id:
                self._send_html(400, "missing id")
                return
            try:
                info = gitops_lib.get_run(ctx, run_id)
                payload = self._gitops_run_payload(ctx, run_id, info)
                progress_tail = gitops_lib.get_run_progress_tail(ctx, run_id, max_lines=200)
            except FileNotFoundError as exc:
                self._send_html(404, str(exc))
                return
            except Exception as exc:
                self._send_html(500, f"gitops run load failed: {type(exc).__name__}: {exc}")
                return
            step_links: List[str] = []
            for step_idx in range(1, int(payload["step_count"]) + 1):
                step_links.append(
                    f"<a href='/gitops/step_log?id={urllib.parse.quote(run_id, safe='')}&step={step_idx}' target='_blank'>step_{step_idx}</a>"
                )
            progress_html = "\n".join(
                f"<pre>{_html_escape(json.dumps(item, ensure_ascii=False))}</pre>"
                for item in progress_tail
            )
            html_text = """<!doctype html>
<html><head><meta charset='utf-8'/>
<meta name='viewport' content='width=device-width, initial-scale=1'/>
<meta http-equiv='refresh' content='2'/>
<title>GitOps Run @@RUN_ID@@</title>
<style>
body{font-family:ui-sans-serif,system-ui,Segoe UI,Roboto,Helvetica,Arial;max-width:1200px;margin:16px auto;padding:0 12px;}
pre{background:#0b1020;color:#e5e7eb;padding:10px;border-radius:8px;overflow:auto;}
.small{color:#6b7280;font-size:12px;}
</style></head><body>
<a href='/gitops'>back</a>
<h2>@@RUN_ID@@</h2>
<div>status: <code>@@STATUS@@</code></div>
<div>repo: <code>@@REPO@@</code></div>
<div>branch: <code>@@BRANCH@@</code></div>
<div>commit: <code>@@COMMIT@@</code></div>
<div>started: <code>@@STARTED@@</code></div>
<div>finished: <code>@@FINISHED@@</code></div>
<div>last_event: <code>@@LAST_EVENT@@</code></div>
<div>run_dir: <code>@@RUN_DIR@@</code></div>
<div class='small'>log_file: @@LOG_FILE@@</div>
<div class='small'>step_logs: @@STEP_LINKS@@</div>
<h3>Run Log</h3>
<iframe src='@@LOG_LINK@@' style='width:100%;height:72vh;border:1px solid #e5e7eb;border-radius:8px;'></iframe>
<details style='margin-top:12px' open>
  <summary>progress.jsonl tail</summary>
  @@PROGRESS_HTML@@
</details>
</body></html>"""
            html_text = (
                html_text
                .replace("@@RUN_ID@@", _html_escape(run_id))
                .replace("@@STATUS@@", _html_escape(payload["status"]))
                .replace("@@REPO@@", _html_escape(payload["repo"]))
                .replace("@@BRANCH@@", _html_escape(payload["branch"]))
                .replace("@@COMMIT@@", _html_escape(payload["commit"]))
                .replace("@@STARTED@@", _html_escape(payload["started_text"]))
                .replace("@@FINISHED@@", _html_escape(payload["finished_text"]))
                .replace("@@LAST_EVENT@@", _html_escape(payload["last_event_text"]))
                .replace("@@RUN_DIR@@", _html_escape(payload["run_dir"]))
                .replace("@@LOG_FILE@@", _html_escape(payload["log_file"]))
                .replace("@@STEP_LINKS@@", " ".join(step_links))
                .replace("@@LOG_LINK@@", f"/gitops/log?id={urllib.parse.quote(run_id, safe='')}")
                .replace("@@PROGRESS_HTML@@", progress_html)
            )
            self._send_html(200, html_text)

        def _handle_gitops_log(self, parsed) -> None:
            ctx = self._require_gitops_ctx_html()
            if ctx is None:
                return
            run_id = (parse_qs(parsed.query).get("id") or [""])[0]
            if not run_id:
                self._send_html(400, "missing id")
                return
            try:
                gitops_lib.get_run(ctx, run_id)
            except FileNotFoundError as exc:
                self._send_html(404, str(exc))
                return
            html_text = _ui_render_log_viewer_html(
                title=f"gitops run log {run_id}",
                back_href=f"/gitops/run?id={urllib.parse.quote(run_id, safe='')}",
                bootstrap_query={"api_path": "/api/gitops/log_chunk", "query": {"id": run_id, "kind": "run"}},
            )
            self._send_html(200, html_text)

        def _handle_gitops_step_log(self, parsed) -> None:
            ctx = self._require_gitops_ctx_html()
            if ctx is None:
                return
            qs = parse_qs(parsed.query)
            run_id = (qs.get("id") or [""])[0]
            step_raw = (qs.get("step") or [""])[0]
            if not run_id or not step_raw:
                self._send_html(400, "missing id or step")
                return
            try:
                step = int(step_raw)
            except Exception:
                self._send_html(400, "invalid step")
                return
            try:
                gitops_lib.read_log_chunk(
                    ctx,
                    run_id=run_id,
                    kind="step",
                    step=step,
                    from_offset=None,
                    before_offset=None,
                    max_bytes=1,
                )
            except FileNotFoundError as exc:
                self._send_html(404, str(exc))
                return
            except ValueError as exc:
                self._send_html(400, str(exc))
                return
            html_text = _ui_render_log_viewer_html(
                title=f"gitops step log {run_id} step_{step}",
                back_href=f"/gitops/run?id={urllib.parse.quote(run_id, safe='')}",
                bootstrap_query={
                    "api_path": "/api/gitops/log_chunk",
                    "query": {"id": run_id, "kind": "step", "step": str(step)},
                },
            )
            self._send_html(200, html_text)

        def _handle_api_gitops_state(self) -> None:
            ctx = self._require_gitops_ctx_json()
            if ctx is None:
                return
            try:
                desc = gitops_lib.describe_context(ctx)
                runs = [
                    self._gitops_run_payload(ctx, run_id, info)
                    for run_id, info in gitops_lib.list_runs(ctx)
                ]
            except Exception as exc:
                self._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})
                return
            self._send_json(
                200,
                {
                    "ok": True,
                    "gitops": desc,
                    "run_count": len(runs),
                    "runs": runs,
                },
            )

        def _handle_api_gitops_run_state(self, parsed) -> None:
            ctx = self._require_gitops_ctx_json()
            if ctx is None:
                return
            run_id = (parse_qs(parsed.query).get("id") or [""])[0]
            if not run_id:
                self._send_json(400, {"error": "missing id"})
                return
            try:
                info = gitops_lib.get_run(ctx, run_id)
                payload = self._gitops_run_payload(ctx, run_id, info)
                progress_tail = gitops_lib.get_run_progress_tail(ctx, run_id, max_lines=100)
            except FileNotFoundError as exc:
                self._send_json(404, {"error": str(exc)})
                return
            except Exception as exc:
                self._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})
                return
            self._send_json(
                200,
                {
                    "ok": True,
                    "run": payload,
                    "progress_tail": progress_tail,
                },
            )

        def _handle_api_gitops_log_chunk(self, parsed) -> None:
            ctx = self._require_gitops_ctx_json()
            if ctx is None:
                return
            qs = parse_qs(parsed.query)
            run_id = (qs.get("id") or [""])[0]
            kind = (qs.get("kind") or ["run"])[0]
            step_raw = (qs.get("step") or [""])[0]
            from_raw = (qs.get("from") or [""])[0]
            before_raw = (qs.get("before") or [""])[0]
            max_bytes_raw = (qs.get("max_bytes") or [""])[0]
            if not run_id:
                self._send_json(400, {"error": "missing id"})
                return
            step = None
            if step_raw:
                try:
                    step = int(step_raw)
                except Exception:
                    self._send_json(400, {"error": "invalid step"})
                    return
            try:
                max_bytes = int(max_bytes_raw) if max_bytes_raw else 65536
            except Exception:
                self._send_json(400, {"error": "invalid max_bytes"})
                return
            try:
                payload = gitops_lib.read_log_chunk(
                    ctx,
                    run_id=run_id,
                    kind=kind,
                    step=step,
                    from_offset=int(from_raw) if from_raw else None,
                    before_offset=int(before_raw) if before_raw else None,
                    max_bytes=max_bytes,
                )
            except FileNotFoundError as exc:
                self._send_json(404, {"error": str(exc)})
                return
            except ValueError as exc:
                self._send_json(400, {"error": str(exc)})
                return
            except Exception as exc:
                self._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})
                return
            self._send_json(200, {"ok": True, **payload})

        def _handle_api_gitops_rerun(self, parsed) -> None:
            ctx = self._require_gitops_ctx_json()
            if ctx is None:
                return
            target = self._gitops_read_target_from_request(parsed)
            if not target:
                self._send_json(400, {"error": "missing target"})
                return
            try:
                result = gitops_lib.rerun_target(ctx, target=target)
            except FileNotFoundError as exc:
                self._send_json(404, {"error": str(exc)})
                return
            except ValueError as exc:
                self._send_json(400, {"error": str(exc)})
                return
            except Exception as exc:
                self._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})
                return
            self._send_json(200, {"ok": result.get("status") == "ok", **result})

        def _handle_api_run_state(self, parsed) -> None:
            qs = parse_qs(parsed.query)
            workdir_id = (qs.get("workdir_id") or [""])[0]
            case_id = (qs.get("case_id") or [""])[0]
            run_index_raw = (qs.get("run_index") or [""])[0]
            if not workdir_id or not case_id or not run_index_raw:
                self._send_json(400, {"error": "missing workdir_id, case_id, or run_index"})
                return
            suite_workdir = _ui_workdir_by_id(workdir_root, workdir_id, extra_history_roots)
            run_dir = _ui_find_run_dir(suite_workdir, case_id=case_id, run_index=int(run_index_raw))
            run_summary = _ui_case_run_summary(run_dir)
            self._send_json(
                200,
                {
                    "ok": True,
                    "workdir_id": workdir_id,
                    "case_id": case_id,
                    "run_index": int(run_index_raw),
                    "run_dir": str(run_dir),
                    "summary": run_summary.get("summary"),
                    "logs": [
                        {"name": item["name"], "path": _ui_relpath(run_dir, item["path"]), "size": item["size"]}
                        for item in _ui_collect_run_logs(run_dir)
                    ],
                    "instance_statuses": _ui_collect_run_instance_statuses(run_summary),
                },
            )

        def _handle_api_log_chunk(self, parsed) -> None:
            qs = parse_qs(parsed.query)
            kind = (qs.get("kind") or ["run"])[0]
            from_raw = (qs.get("from") or [""])[0]
            before_raw = (qs.get("before") or [""])[0]
            max_bytes_raw = (qs.get("max_bytes") or [""])[0]
            max_bytes = 65536
            if max_bytes_raw:
                try:
                    max_bytes = int(max_bytes_raw)
                except Exception:
                    self._send_json(400, {"error": "invalid max_bytes"})
                    return
            from_offset = int(from_raw) if from_raw else None
            before_offset = int(before_raw) if before_raw else None
            try:
                if kind == "runner":
                    workdir_id = (qs.get("workdir_id") or [""])[0]
                    if not workdir_id:
                        self._send_json(400, {"error": "missing workdir_id"})
                        return
                    suite_workdir = _ui_workdir_by_id(workdir_root, workdir_id, extra_history_roots)
                    path = (suite_workdir / RUNNER_STDIO_LOG_FILENAME).resolve()
                    if not path.exists():
                        raise FileNotFoundError(f"runner log not found: {path}")
                elif kind == "run":
                    workdir_id = (qs.get("workdir_id") or [""])[0]
                    case_id = (qs.get("case_id") or [""])[0]
                    run_index_raw = (qs.get("run_index") or [""])[0]
                    name = (qs.get("name") or [""])[0]
                    if not workdir_id or not case_id or not run_index_raw or not name:
                        self._send_json(400, {"error": "missing workdir_id, case_id, run_index, or name"})
                        return
                    suite_workdir = _ui_workdir_by_id(workdir_root, workdir_id, extra_history_roots)
                    run_dir = _ui_find_run_dir(suite_workdir, case_id=case_id, run_index=int(run_index_raw))
                    path = _ui_resolve_run_log_path(run_dir, name=name)
                else:
                    self._send_json(400, {"error": "invalid kind"})
                    return
                payload = _ui_log_chunk(
                    path,
                    from_offset=from_offset,
                    before_offset=before_offset,
                    max_bytes=max_bytes,
                )
            except FileNotFoundError as exc:
                self._send_json(404, {"error": str(exc)})
                return
            except Exception as exc:
                self._send_json(400, {"error": str(exc)})
                return
            self._send_json(200, {"ok": True, "kind": kind, **payload})

        def _handle_api_ops_log_chunk(self, parsed) -> None:
            qs = parse_qs(parsed.query)
            workdir_id = (qs.get("workdir_id") or [""])[0]
            case_id = (qs.get("case_id") or [""])[0]
            run_index_raw = (qs.get("run_index") or [""])[0]
            instance_id = (qs.get("instance_id") or [""])[0]
            after_ts = (qs.get("from") or [""])[0] or None
            before_ts = (qs.get("before") or [""])[0] or None
            log_table = (qs.get("log_table") or [""])[0] or None
            level = (qs.get("level") or [""])[0] or None
            search = (qs.get("search") or [""])[0] or None
            if not workdir_id or not case_id or not run_index_raw or not instance_id:
                self._send_json(400, {"error": "missing workdir_id, case_id, run_index, or instance_id"})
                return
            try:
                suite_workdir = _ui_workdir_by_id(workdir_root, workdir_id, extra_history_roots)
                run_dir = _ui_find_run_dir(suite_workdir, case_id=case_id, run_index=int(run_index_raw))
                run_summary = _ui_case_run_summary(run_dir)
                resolved_case = _require_dict(run_summary.get("resolved_case"), "ui.run_summary.resolved_case")
                payload = _ui_fetch_ops_logs(
                    resolved_case,
                    instance_id=instance_id,
                    after_ts=after_ts,
                    before_ts=before_ts,
                    log_table=log_table,
                    level=level,
                    search=search,
                )
            except FileNotFoundError as exc:
                self._send_json(404, {"error": str(exc)})
                return
            except Exception as exc:
                self._send_json(400, {"error": f"{type(exc).__name__}: {exc}"})
                return
            out = {"ok": True, "instance_id": instance_id, "after_ts_used": after_ts is not None}
            out.update(payload)
            self._send_json(200, out)

    httpd = ThreadingHTTPServer((host, port), Handler)
    print(f"INFO: test_runner UI listening on http://{host}:{port} workdir={workdir_root}", flush=True)
    httpd.serve_forever()

def _validate_deploy_result(resolved_case: Dict[str, Any], deploy_result: Dict[str, Any]) -> None:
    _forbid_unknown_keys(
        deploy_result,
        {"schema_version", "instances", "ready", "history_id"},
        "deploy_result",
    )
    if deploy_result.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("deploy_result.schema_version mismatch")
    if deploy_result.get("ready") is not True:
        raise ValueError("deploy_result.ready must be true")

    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    req_instances = _require_list(deploy.get("instances"), "resolved_case.deploy.instances")

    req_ids = []
    endpoint_ids = []
    for raw in req_instances:
        inst = _require_dict(raw, "deploy.instances[]")
        iid = _require_str(inst.get("id"), "deploy.instances[].id")
        req_ids.append(iid)
        if inst.get("endpoint") is not None:
            endpoint_ids.append(iid)

    got_instances = _require_list(deploy_result.get("instances"), "deploy_result.instances")
    got_by_id: Dict[str, Dict[str, Any]] = {}
    for raw in got_instances:
        inst = _require_dict(raw, "deploy_result.instances[]")
        _forbid_unknown_keys(
            inst,
            {"id", "k8s_ref", "pod_name", "node_name", "node_ip", "endpoint_url"},
            "deploy_result.instances[]",
        )
        iid = _require_str(inst.get("id"), "deploy_result.instances[].id")
        got_by_id[iid] = inst
        _ = _require_str(inst.get("k8s_ref"), "deploy_result.instances[].k8s_ref")
        _ = _require_str(inst.get("pod_name"), "deploy_result.instances[].pod_name")
        _ = _require_str(inst.get("node_name"), "deploy_result.instances[].node_name")
        _ = _require_str(inst.get("node_ip"), "deploy_result.instances[].node_ip")

    if set(got_by_id.keys()) != set(req_ids):
        raise ValueError("deploy_result.instances ids mismatch with requested instances")

    # INFER requires exactly one endpoint instance; CI may have 0..N endpoints.
    scene = _require_dict(resolved_case.get("scene"), "resolved_case.scene")
    kind = (
        SCENE_KIND_INFER
        if scene.get("infer") is not None
        else SCENE_KIND_CI
        if scene.get("ci") is not None
        else SCENE_KIND_TEST_STACK
    )
    if kind == SCENE_KIND_INFER:
        if len(endpoint_ids) != 1:
            raise ValueError("resolved_case.deploy.instances must include exactly one endpoint instance")
        ep_id = endpoint_ids[0]
        ep_inst = got_by_id.get(ep_id)
        if ep_inst is None:
            raise ValueError("deploy_result missing endpoint instance")
        ep_url = ep_inst.get("endpoint_url")
        if not isinstance(ep_url, str) or not ep_url.strip():
            raise ValueError("deploy_result endpoint instance missing endpoint_url")



def _resolved_endpoint_url(resolved_case: Dict[str, Any], deploy_result: Dict[str, Any]) -> str:
    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    req_instances = _require_list(deploy.get("instances"), "resolved_case.deploy.instances")
    endpoint_id = None
    for raw in req_instances:
        if not isinstance(raw, dict):
            continue
        if raw.get("endpoint") is None:
            continue
        endpoint_id = raw.get("id")
        break
    if not isinstance(endpoint_id, str) or not endpoint_id:
        raise ValueError("cannot find endpoint instance id")

    got_instances = _require_list(deploy_result.get("instances"), "deploy_result.instances")
    for raw in got_instances:
        if not isinstance(raw, dict):
            continue
        if raw.get("id") == endpoint_id:
            ep = raw.get("endpoint_url")
            if not isinstance(ep, str) or not ep.strip():
                raise ValueError("deploy_result endpoint_url missing")
            return ep
    raise ValueError("deploy_result missing endpoint instance")


def _tcp_check_endpoint(endpoint_url: str) -> None:
    u = urlparse(endpoint_url)
    if u.scheme not in ("http", "https"):
        raise ValueError(f"endpoint_url scheme unsupported: {u.scheme}")
    host = u.hostname
    port = u.port
    if host is None or port is None:
        raise ValueError("endpoint_url must include host and port")

    # Keep it minimal: one connect attempt, fixed timeout.
    # Adapter is responsible for readiness waits; this is only a sanity check.
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.settimeout(3.0)
        sock.connect((host, port))
    finally:
        sock.close()


def _wait_tcp_endpoint(endpoint_url: str, *, timeout_s: int) -> None:
    deadline = time.time() + float(timeout_s)
    last_err: str | None = None
    while True:
        try:
            _tcp_check_endpoint(endpoint_url)
            return
        except Exception as exc:  # noqa: BLE001
            last_err = f"{type(exc).__name__}: {exc}"
        if time.time() >= deadline:
            raise ValueError(f"endpoint not ready in {timeout_s}s: endpoint_url={endpoint_url} last_err={last_err}")
        time.sleep(0.5)


def _run_infer_ai_perf(
    resolved_case: Dict[str, Any], deploy_result: Dict[str, Any], run_dir: Path
) -> Dict[str, Any]:
    profile = _require_dict(resolved_case.get("profile"), "resolved_case.profile")
    infer = _require_dict(profile.get("infer"), "resolved_case.profile.infer")
    ai_perf = _require_dict(infer.get("ai_perf"), "resolved_case.profile.infer.ai_perf")

    runtime = _require_dict(resolved_case.get("runtime"), "resolved_case.runtime")
    config_root = Path(_require_str(runtime.get("config_root"), "resolved_case.runtime.config_root"))

    cmd_rel = _require_str(ai_perf.get("cmd"), "ai_perf.cmd")
    if os.path.isabs(cmd_rel):
        raise ValueError("ai_perf.cmd must be config-relative")
    cmd_abs = str((config_root / cmd_rel).resolve())
    if not os.access(cmd_abs, os.X_OK):
        raise ValueError(f"ai_perf.cmd is not executable: {cmd_abs}")

    out_name = _require_str(ai_perf.get("output_yaml"), "ai_perf.output_yaml")
    if _FILE_NAME_RE.match(out_name) is None:
        raise ValueError("ai_perf.output_yaml must be a file name only")

    endpoint_url = _resolved_endpoint_url(resolved_case, deploy_result)

    scene = _require_dict(resolved_case.get("scene"), "resolved_case.scene")
    scene_infer = _require_dict(scene.get("infer"), "resolved_case.scene.infer")
    pattern = _require_str(scene_infer.get("pattern"), "scene.infer.pattern")

    scale = _require_dict(resolved_case.get("scale"), "resolved_case.scale")
    duration_seconds = _require_int(scale.get("duration_seconds"), "scale.duration_seconds", min_v=1)
    scale_infer = _require_dict(scale.get("infer"), "scale.infer")
    concurrency = _require_int(scale_infer.get("client_concurrency"), "scale.infer.client_concurrency", min_v=1)
    prompt_tokens = _require_int(scale_infer.get("prompt_tokens"), "scale.infer.prompt_tokens", min_v=1)
    output_tokens = _require_int(scale_infer.get("output_tokens"), "scale.infer.output_tokens", min_v=1)

    args_map = _require_dict(ai_perf.get("args"), "ai_perf.args")
    args_yaml_path = run_dir / "ai_perf_args.yaml"
    _write_yaml_file(args_yaml_path, args_map)

    out_path = run_dir / out_name
    if out_path.exists():
        raise ValueError(f"ai perf output file already exists (no overwrite): {out_path}")

    argv = [
        cmd_abs,
        "--endpoint-url",
        endpoint_url,
        "--client-concurrency",
        str(concurrency),
        "--prompt-tokens",
        str(prompt_tokens),
        "--output-tokens",
        str(output_tokens),
        "--duration-seconds",
        str(duration_seconds),
        "--pattern",
        pattern,
        "--args-yaml",
        str(args_yaml_path),
        "--output-yaml",
        str(out_path),
    ]

    _run_subprocess(argv, cwd=str(config_root))

    if not out_path.exists():
        raise ValueError(f"ai perf did not produce output_yaml: {out_path}")

    out = _load_yaml_file(out_path)
    return _require_dict(out, "ai_perf_output")


def _build_infer_summary_yaml(
    resolved_case: Dict[str, Any],
    deploy_result: Dict[str, Any],
    *,
    run_index: int,
    started_at_unix_s: int,
    finished_at_unix_s: int,
    outcome: str,
    counted: bool,
    ai_perf_out: Dict[str, Any],
) -> Dict[str, Any]:
    case = _require_dict(resolved_case.get("case"), "resolved_case.case")
    case_id = _require_str(case.get("case_id"), "case.case_id")
    case_key = _require_str(case.get("case_key"), "case.case_key")

    profile = _require_dict(resolved_case.get("profile"), "resolved_case.profile")
    infer = _require_dict(profile.get("infer"), "resolved_case.profile.infer")
    stack = _require_str(infer.get("stack"), "profile.infer.stack")

    scene = _require_dict(resolved_case.get("scene"), "resolved_case.scene")
    scene_infer = _require_dict(scene.get("infer"), "resolved_case.scene.infer")
    pattern = _require_str(scene_infer.get("pattern"), "scene.infer.pattern")

    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    controller_url = _require_str(deploy.get("controller_url"), "deploy.controller_url")

    # Validate ai perf output schema (minimal, no fallback)
    _require_number(ai_perf_out.get("throughput_rps"), "ai_perf_output.throughput_rps")
    _require_number(ai_perf_out.get("success_rate"), "ai_perf_output.success_rate")
    _require_percentiles(ai_perf_out.get("latency_ms"), "ai_perf_output.latency_ms")
    _require_percentiles(ai_perf_out.get("ttft_ms"), "ai_perf_output.ttft_ms")

    deploy_instances = _flatten_deploy_instances_for_summary(resolved_case, deploy_result)

    return {
        "schema_version": SCHEMA_VERSION,
        "case_id": case_id,
        "case_key": case_key,
        "run_index": int(run_index),
        "outcome": outcome,
        "counted": bool(counted),
        "timing": {"started_at_unix_s": int(started_at_unix_s), "finished_at_unix_s": int(finished_at_unix_s)},
        "deploy": {"controller_url": controller_url, "instances": deploy_instances},
        "infer": {
            "stack": stack,
            "pattern": pattern,
            "throughput_rps": float(ai_perf_out["throughput_rps"]),
            "latency_ms": ai_perf_out["latency_ms"],
            "ttft_ms": ai_perf_out["ttft_ms"],
            "success_rate": float(ai_perf_out["success_rate"]),
        },
    }


def _flatten_deploy_instances_for_summary(
    resolved_case: Dict[str, Any], deploy_result: Dict[str, Any]
) -> List[Dict[str, Any]]:
    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    req_instances = _require_list(deploy.get("instances"), "resolved_case.deploy.instances")
    endpoint_id = None
    for raw in req_instances:
        if isinstance(raw, dict) and raw.get("endpoint") is not None:
            endpoint_id = raw.get("id")
            break

    out: List[Dict[str, Any]] = []
    got_instances = _require_list(deploy_result.get("instances"), "deploy_result.instances")
    got_by_id: Dict[str, Dict[str, Any]] = {}
    for raw in got_instances:
        if isinstance(raw, dict) and isinstance(raw.get("id"), str):
            got_by_id[raw["id"]] = raw

    for raw in req_instances:
        inst = _require_dict(raw, "resolved_case.deploy.instances[]")
        iid = _require_str(inst.get("id"), "deploy.instances[].id")
        got = got_by_id.get(iid)
        if got is None:
            raise ValueError(f"deploy_result missing instance: {iid}")
        node_ip = _require_str(got.get("node_ip"), "deploy_result.instances[].node_ip")
        row: Dict[str, Any] = {"id": iid, "node_ip": node_ip}
        if iid == endpoint_id:
            row["endpoint_url"] = _require_str(got.get("endpoint_url"), "deploy_result.instances[].endpoint_url")
        out.append(row)
    return out


def _require_number(v: Any, ctx: str) -> float:
    if not isinstance(v, (int, float)):
        raise ValueError(f"{ctx} must be number")
    return float(v)


def _require_percentiles(v: Any, ctx: str) -> Dict[str, float]:
    d = _require_dict(v, ctx)
    _forbid_unknown_keys(d, {"p50", "p95", "p99"}, ctx)
    return {
        "p50": _require_number(d.get("p50"), f"{ctx}.p50"),
        "p95": _require_number(d.get("p95"), f"{ctx}.p95"),
        "p99": _require_number(d.get("p99"), f"{ctx}.p99"),
    }


if __name__ == "__main__":
    main()
