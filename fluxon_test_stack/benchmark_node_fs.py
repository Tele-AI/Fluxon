from __future__ import annotations

"""FS helpers for the benchmark stack."""

import copy
import hashlib
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence

import yaml

from benchmark_role_names import KV_NODE_ROLE_SEED, KV_NODE_ROLE_WORKER
from fluxon_py.fluxon_fs.patcher import FluxonFsPatcher

TEST_WORKLOAD_MODE_PY_FS = "PY_FS"
FS_BACKEND_KIND_FLUXON = "FLUXON"
FS_BACKEND_KIND_ALLUXIO = "ALLUXIO"

FS_SCENE_OPEN_READ_CLOSE_SMALLFILES = "fs_open_read_close_smallfiles"
FS_SCENE_WRITE_CLOSE_COMMIT = "fs_write_close_commit"
FS_SCENES = (
    FS_SCENE_OPEN_READ_CLOSE_SMALLFILES,
    FS_SCENE_WRITE_CLOSE_COMMIT,
)

FS_RUNTIME_ROLE_AGENT = KV_NODE_ROLE_SEED
FS_RUNTIME_ROLE_CLIENT = KV_NODE_ROLE_WORKER

BENCHMARK_KEY_WORKLOAD_MODE = "workload_mode"
BENCHMARK_KEY_WORKLOAD_ID = "workload_id"
BENCHMARK_KEY_FILE_SIZE_BYTES = "file_size_bytes"
BENCHMARK_KEY_CHUNK_SIZE_BYTES = "chunk_size_bytes"
BENCHMARK_KEY_FILES_PER_WORKER = "files_per_worker"
BENCHMARK_KEY_CACHE_MAX_BYTES = "cache_max_bytes"
BENCHMARK_KEY_FS_AGENT_INSTANCE_KEYS = "fs_agent_instance_keys"

DEFAULT_FS_FILE_SIZE_BYTES = 4096
DEFAULT_FS_FILES_PER_WORKER = 256
DEFAULT_FS_STALE_WINDOW_MS = 1000

FS_BENCHMARK_EXTRA_KEYS = (
    BENCHMARK_KEY_WORKLOAD_MODE,
    BENCHMARK_KEY_WORKLOAD_ID,
    BENCHMARK_KEY_FILE_SIZE_BYTES,
    BENCHMARK_KEY_CHUNK_SIZE_BYTES,
    BENCHMARK_KEY_FILES_PER_WORKER,
    BENCHMARK_KEY_CACHE_MAX_BYTES,
    BENCHMARK_KEY_FS_AGENT_INSTANCE_KEYS,
)


@dataclass(frozen=True)
class FSRuntimeConfig:
    scene_id: str
    file_size_bytes: int
    chunk_size_bytes: int
    files_per_worker: int
    cache_max_bytes: int
    agent_instance_keys: tuple[str, ...]


@dataclass
class FSNodeRuntimeState:
    runtime_cfg: FSRuntimeConfig
    backend_kind: str
    node_role: str
    instance_key: str
    export_name: str
    remote_root_dir: Path
    placeholder_root_dir: Path
    mount_dir: Path
    payload: bytes
    patcher: Optional[FluxonFsPatcher] = None


def _bench_fs_print(msg: str) -> None:
    print(f"[BENCH-FS] {msg}", flush=True)


def extract_fs_benchmark_extras_from_benchmark_section(benchmark_cfg: Mapping[str, Any]) -> Dict[str, Any]:
    workload_mode_raw = benchmark_cfg.get(BENCHMARK_KEY_WORKLOAD_MODE)
    workload_mode = str(workload_mode_raw).upper() if workload_mode_raw is not None else ""
    if workload_mode != TEST_WORKLOAD_MODE_PY_FS:
        return {}
    extras: Dict[str, Any] = {BENCHMARK_KEY_WORKLOAD_MODE: TEST_WORKLOAD_MODE_PY_FS}
    for key in FS_BENCHMARK_EXTRA_KEYS:
        if key in benchmark_cfg:
            extras[key] = copy.deepcopy(benchmark_cfg[key])
    return extras


def _stable_bucket(parts: Sequence[Any]) -> int:
    digest = hashlib.sha256()
    for part in parts:
        digest.update(str(part).encode("utf-8"))
        digest.update(b"\x1f")
    return int.from_bytes(digest.digest()[:8], "big")


def _safe_slug(raw: str) -> str:
    out = []
    for ch in raw.lower():
        if ch.isalnum():
            out.append(ch)
        else:
            out.append("_")
    slug = "".join(out).strip("_")
    return slug or "fsbench"


def _make_payload_bytes(size: int, *, seed: str) -> bytes:
    if size <= 0:
        raise ValueError(f"payload size must be > 0, got: {size}")
    block = hashlib.sha256(seed.encode("utf-8")).digest()
    out = bytearray()
    while len(out) < size:
        out.extend(block)
    return bytes(out[:size])


def _clear_directory(dir_path: Path) -> None:
    if not dir_path.exists():
        dir_path.mkdir(parents=True, exist_ok=True)
        return
    for child in list(dir_path.iterdir()):
        if child.is_dir():
            shutil.rmtree(child, ignore_errors=False)
        else:
            child.unlink()


def _fs_runtime_config_from_test_config(test_config: Mapping[str, Any]) -> FSRuntimeConfig:
    workload_mode_raw = test_config.get(BENCHMARK_KEY_WORKLOAD_MODE)
    workload_mode = str(workload_mode_raw).upper() if workload_mode_raw is not None else ""
    if workload_mode != TEST_WORKLOAD_MODE_PY_FS:
        raise ValueError(f"unsupported FS workload_mode: {workload_mode_raw!r}")

    scene_id_raw = test_config.get(BENCHMARK_KEY_WORKLOAD_ID) or test_config.get("test_id") or ""
    scene_id = str(scene_id_raw).strip()
    if scene_id not in FS_SCENES:
        raise ValueError(f"unsupported FS benchmark scene: {scene_id!r}")

    file_size_bytes = int(test_config.get(BENCHMARK_KEY_FILE_SIZE_BYTES, test_config.get("value_size", DEFAULT_FS_FILE_SIZE_BYTES)))
    if file_size_bytes <= 0:
        raise ValueError(f"file_size_bytes must be > 0, got: {file_size_bytes}")

    chunk_size_raw = test_config.get(BENCHMARK_KEY_CHUNK_SIZE_BYTES)
    chunk_size_bytes = file_size_bytes if chunk_size_raw is None else int(chunk_size_raw)
    if chunk_size_bytes <= 0:
        raise ValueError(f"chunk_size_bytes must be > 0, got: {chunk_size_bytes}")

    files_per_worker = int(test_config.get(BENCHMARK_KEY_FILES_PER_WORKER, DEFAULT_FS_FILES_PER_WORKER))
    if files_per_worker <= 0:
        raise ValueError(f"files_per_worker must be > 0, got: {files_per_worker}")

    cache_max_bytes_raw = test_config.get(BENCHMARK_KEY_CACHE_MAX_BYTES)
    cache_max_bytes = file_size_bytes if cache_max_bytes_raw is None else int(cache_max_bytes_raw)
    cache_max_bytes = max(cache_max_bytes, file_size_bytes)
    if cache_max_bytes <= 0:
        raise ValueError(f"cache_max_bytes must be > 0, got: {cache_max_bytes}")

    agent_keys_raw = test_config.get(BENCHMARK_KEY_FS_AGENT_INSTANCE_KEYS)
    if not isinstance(agent_keys_raw, list) or not agent_keys_raw:
        raise ValueError("fs_agent_instance_keys must be a non-empty list")
    agent_instance_keys = []
    for idx, raw_value in enumerate(agent_keys_raw):
        value = str(raw_value).strip()
        if not value:
            raise ValueError(f"fs_agent_instance_keys[{idx}] must be non-empty")
        agent_instance_keys.append(value)

    return FSRuntimeConfig(
        scene_id=scene_id,
        file_size_bytes=file_size_bytes,
        chunk_size_bytes=chunk_size_bytes,
        files_per_worker=files_per_worker,
        cache_max_bytes=cache_max_bytes,
        agent_instance_keys=tuple(agent_instance_keys),
    )


def _fs_export_name(runtime_cfg: FSRuntimeConfig) -> str:
    slug = _safe_slug(runtime_cfg.scene_id)[:24]
    digest = hashlib.sha256(runtime_cfg.scene_id.encode("utf-8")).hexdigest()[:8]
    return f"{slug}_{digest}"


def _build_fs_cache_yaml(
    *,
    export_name: str,
    remote_root_dir_abs: Path,
    agent_instance_keys: Sequence[str],
    cache_max_bytes: int,
) -> str:
    payload = {
        "stale_window_ms": int(DEFAULT_FS_STALE_WINDOW_MS),
        "rules": [],
        "exports": {
            export_name: {
                "remote_root_dir_abs": str(remote_root_dir_abs.resolve()),
                "nodes": list(agent_instance_keys),
                "cache_max_bytes": int(cache_max_bytes),
            }
        },
    }
    return yaml.safe_dump(payload, sort_keys=False)


def _register_fs_agent(kv_store: Any, *, cache_yaml: str) -> None:
    inner = getattr(kv_store, "_client", None)
    if inner is None:
        raise RuntimeError(
            "fluxon_fs benchmark agent requires kv_store to expose _client (fluxon_pyo3.KvClient)"
        )
    import fluxon_pyo3  # type: ignore

    reg = fluxon_pyo3.fluxon_fs_register_agent(inner, str(cache_yaml))
    if not reg.is_ok():
        raise RuntimeError(f"fluxon_fs_register_agent failed: {reg.unwrap_error()}")
    _ = reg.unwrap()


def _close_fs_runtime_state(state: FSNodeRuntimeState) -> None:
    if state.patcher is not None:
        state.patcher.uninstall()
        state.patcher = None


def _fs_backend_kind_from_test_config(test_config: Mapping[str, Any]) -> str:
    kvcache_config = test_config.get("kvcache_config")
    if not isinstance(kvcache_config, dict):
        return FS_BACKEND_KIND_FLUXON
    backend_kind = str(kvcache_config.get("backend_kind", FS_BACKEND_KIND_FLUXON)).strip().upper()
    if backend_kind not in (FS_BACKEND_KIND_FLUXON, FS_BACKEND_KIND_ALLUXIO):
        raise ValueError(f"unsupported FS backend_kind: {backend_kind!r}")
    return backend_kind


def _ensure_fs_runtime(self: Any, runtime_cfg: FSRuntimeConfig) -> FSNodeRuntimeState:
    if not isinstance(self.instance_key, str) or not self.instance_key.strip():
        raise RuntimeError("FS benchmark requires BenchmarkNode.instance_key")

    node_role = str(self.test_config.get("node_role", ""))
    if node_role not in (FS_RUNTIME_ROLE_AGENT, FS_RUNTIME_ROLE_CLIENT):
        raise ValueError(f"unsupported FS node_role: {node_role!r}")
    backend_kind = _fs_backend_kind_from_test_config(self.test_config)
    if backend_kind == FS_BACKEND_KIND_FLUXON and self.kv_store is None:
        raise RuntimeError("Fluxon FS benchmark requires kv_store to be initialized")

    state = getattr(self, "_fluxon_fs_runtime_state", None)
    if isinstance(state, FSNodeRuntimeState):
        if (
            state.runtime_cfg == runtime_cfg
            and state.node_role == node_role
            and state.backend_kind == backend_kind
        ):
            return state
        _close_fs_runtime_state(state)

    instance_key = self.instance_key.strip()
    export_name = _fs_export_name(runtime_cfg)
    if backend_kind == FS_BACKEND_KIND_ALLUXIO:
        kvcache_config = self.test_config.get("kvcache_config")
        if not isinstance(kvcache_config, dict):
            raise RuntimeError("alluxio FS benchmark requires kvcache_config mapping")
        alluxio_cfg = kvcache_config.get("alluxio")
        if not isinstance(alluxio_cfg, dict):
            raise RuntimeError("alluxio FS benchmark requires kvcache_config.alluxio mapping")
        mount_root_abs = str(alluxio_cfg.get("mount_root_abs", "")).strip()
        if not mount_root_abs:
            raise RuntimeError("alluxio FS benchmark requires alluxio.mount_root_abs")
        mount_root = Path(mount_root_abs).resolve()
        namespace_prefix = str(alluxio_cfg.get("namespace_prefix", "")).strip()
        if not namespace_prefix:
            raise RuntimeError("alluxio FS benchmark requires alluxio.namespace_prefix")
        namespace_root = (mount_root / namespace_prefix / export_name).resolve()
        remote_root_dir = namespace_root
        placeholder_root_dir = namespace_root
        mount_dir = namespace_root
        mount_dir.parent.mkdir(parents=True, exist_ok=True)
    else:
        base_dir = (Path.cwd() / "services" / "fs_benchmark" / _safe_slug(instance_key)).resolve()
        remote_root_dir = (base_dir / "remote_root").resolve()
        placeholder_root_dir = (base_dir / "placeholder_root").resolve()
        mount_dir = (base_dir / "mount").resolve()
        remote_root_dir.mkdir(parents=True, exist_ok=True)
        placeholder_root_dir.mkdir(parents=True, exist_ok=True)
        mount_dir.parent.mkdir(parents=True, exist_ok=True)

    agent_cache_yaml = _build_fs_cache_yaml(
        export_name=export_name,
        remote_root_dir_abs=remote_root_dir,
        agent_instance_keys=runtime_cfg.agent_instance_keys,
        cache_max_bytes=runtime_cfg.cache_max_bytes,
    )
    client_cache_yaml = _build_fs_cache_yaml(
        export_name=export_name,
        remote_root_dir_abs=placeholder_root_dir,
        agent_instance_keys=runtime_cfg.agent_instance_keys,
        cache_max_bytes=runtime_cfg.cache_max_bytes,
    )

    patcher: Optional[FluxonFsPatcher] = None
    if node_role == FS_RUNTIME_ROLE_AGENT:
        if instance_key not in runtime_cfg.agent_instance_keys:
            raise ValueError(
                f"FS agent instance_key is missing from fs_agent_instance_keys: {instance_key!r}"
            )
        if backend_kind == FS_BACKEND_KIND_FLUXON:
            assert self.kv_store is not None
            _register_fs_agent(self.kv_store, cache_yaml=agent_cache_yaml)
    else:
        if backend_kind == FS_BACKEND_KIND_FLUXON:
            patcher = FluxonFsPatcher(self.kv_store)
            patcher.set_cache_config_yaml(client_cache_yaml)
            patcher.mount_remote_dir(
                local_mount_dir_abs=str(mount_dir),
                export_name=export_name,
            )
            patcher.install()
        else:
            mount_dir.mkdir(parents=True, exist_ok=True)

    payload = _make_payload_bytes(
        runtime_cfg.file_size_bytes,
        seed=f"{runtime_cfg.scene_id}:{instance_key}",
    )
    state = FSNodeRuntimeState(
        runtime_cfg=runtime_cfg,
        backend_kind=backend_kind,
        node_role=node_role,
        instance_key=instance_key,
        export_name=export_name,
        remote_root_dir=remote_root_dir,
        placeholder_root_dir=placeholder_root_dir,
        mount_dir=mount_dir,
        payload=payload,
        patcher=patcher,
    )
    setattr(self, "_fluxon_fs_runtime_state", state)
    return state


def _prepare_fs_agent_before_ready(*, state: FSNodeRuntimeState) -> None:
    runtime_cfg = state.runtime_cfg
    _clear_directory(state.remote_root_dir)
    if runtime_cfg.scene_id == FS_SCENE_OPEN_READ_CLOSE_SMALLFILES:
        for file_idx in range(runtime_cfg.files_per_worker):
            file_path = state.remote_root_dir / f"read_{file_idx:06d}.bin"
            file_path.write_bytes(state.payload)
        _bench_fs_print(
            f"agent prepared read dataset export={state.export_name} files={runtime_cfg.files_per_worker} "
            f"file_size={runtime_cfg.file_size_bytes}"
        )
        return
    if runtime_cfg.scene_id == FS_SCENE_WRITE_CLOSE_COMMIT:
        (state.remote_root_dir / "writes").mkdir(parents=True, exist_ok=True)
        _bench_fs_print(
            f"agent prepared write dataset export={state.export_name} root={state.remote_root_dir}"
        )
        return
    raise ValueError(f"unsupported FS benchmark scene: {runtime_cfg.scene_id!r}")


def _prepare_fs_client_before_ready(*, state: FSNodeRuntimeState) -> None:
    if state.backend_kind == FS_BACKEND_KIND_FLUXON:
        if state.patcher is None:
            raise RuntimeError("FS client runtime is missing patcher")
    else:
        state.mount_dir.mkdir(parents=True, exist_ok=True)
    state.placeholder_root_dir.mkdir(parents=True, exist_ok=True)
    _bench_fs_print(
        f"client ready backend={state.backend_kind} export={state.export_name} "
        f"mount_dir={state.mount_dir} agents={list(state.runtime_cfg.agent_instance_keys)}"
    )


def _select_fs_file_index(runtime_cfg: FSRuntimeConfig, *, thread_id: int, op_idx: int) -> int:
    bucket = _stable_bucket((runtime_cfg.scene_id, thread_id, op_idx, "fs_file"))
    return int(bucket % runtime_cfg.files_per_worker)


def _fs_read_one_file(*, state: FSNodeRuntimeState, thread_id: int, op_idx: int) -> tuple[str, int]:
    runtime_cfg = state.runtime_cfg
    file_idx = _select_fs_file_index(runtime_cfg, thread_id=thread_id, op_idx=op_idx)
    relpath = f"read_{file_idx:06d}.bin"
    file_path = state.mount_dir / relpath
    total_bytes = 0
    with open(file_path, "rb") as fp:
        while True:
            chunk = fp.read(runtime_cfg.chunk_size_bytes)
            if not chunk:
                break
            total_bytes += len(chunk)
    if total_bytes != runtime_cfg.file_size_bytes:
        raise RuntimeError(
            f"read size mismatch: relpath={relpath} expected={runtime_cfg.file_size_bytes} got={total_bytes}"
        )
    return relpath, total_bytes


def _fs_write_one_file(*, state: FSNodeRuntimeState, thread_id: int, op_idx: int) -> tuple[str, int]:
    runtime_cfg = state.runtime_cfg
    thread_dir = state.mount_dir / "writes" / _safe_slug(state.instance_key) / f"thread_{thread_id}"
    thread_dir.mkdir(parents=True, exist_ok=True)
    file_idx = _select_fs_file_index(runtime_cfg, thread_id=thread_id, op_idx=op_idx)
    relpath = f"writes/{_safe_slug(state.instance_key)}/thread_{thread_id}/file_{file_idx:06d}.bin"
    file_path = thread_dir / f"file_{file_idx:06d}.bin"
    with open(file_path, "wb") as fp:
        for offset in range(0, len(state.payload), runtime_cfg.chunk_size_bytes):
            fp.write(state.payload[offset : offset + runtime_cfg.chunk_size_bytes])
    return relpath, runtime_cfg.file_size_bytes


def _run_fs_client_worker(
    benchmark_node: Any,
    *,
    thread_id: int,
    deadline_ts: float,
    state: FSNodeRuntimeState,
    operation_result_cls: Any,
    operation_outcome: Any,
    metric_warmup_seconds: float,
    debug_print: Any,
) -> list[Any]:
    runtime_cfg = state.runtime_cfg
    results: list[Any] = []
    op_idx = 0
    if runtime_cfg.scene_id == FS_SCENE_OPEN_READ_CLOSE_SMALLFILES:
        operation_type = "fs_open_read_close"
    elif runtime_cfg.scene_id == FS_SCENE_WRITE_CLOSE_COMMIT:
        operation_type = "fs_write_close_commit"
    else:
        raise ValueError(f"unsupported FS benchmark scene: {runtime_cfg.scene_id!r}")

    while True:
        now_ts = time.time()
        if now_ts >= float(deadline_ts):
            break

        inflight_at_start = benchmark_node._inflight_begin()
        try:
            start_ts = time.time()
            if runtime_cfg.scene_id == FS_SCENE_OPEN_READ_CLOSE_SMALLFILES:
                relpath, data_size = _fs_read_one_file(
                    state=state,
                    thread_id=thread_id,
                    op_idx=op_idx,
                )
            else:
                relpath, data_size = _fs_write_one_file(
                    state=state,
                    thread_id=thread_id,
                    op_idx=op_idx,
                )
            latency_us = (time.time() - start_ts) * 1_000_000.0
            result = operation_result_cls(
                success=True,
                latency_us=latency_us,
                operation_type=operation_type,
                key=relpath,
                data_size=data_size,
                inflight_at_start=inflight_at_start,
                outcome_kind=operation_outcome.SUCCESS,
                error_msg=None,
            )
        except Exception as exc:  # noqa: BLE001
            result = operation_result_cls(
                success=False,
                latency_us=0.0,
                operation_type=operation_type,
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
        f"thread {thread_id} exit fs run loop, total_ops={len(results)}, last_op_idx={op_idx}"
    )
    return results


def is_fs_workload(test_config: Mapping[str, Any]) -> bool:
    workload_mode_raw = test_config.get(BENCHMARK_KEY_WORKLOAD_MODE)
    workload_mode = str(workload_mode_raw).strip().upper() if workload_mode_raw is not None else ""
    return workload_mode == TEST_WORKLOAD_MODE_PY_FS


def merge_fs_benchmark_extras(
    node_config: Mapping[str, Any],
    benchmark_cfg: Mapping[str, Any],
) -> Dict[str, Any]:
    merged_config = copy.deepcopy(dict(node_config))
    for key, value in extract_fs_benchmark_extras_from_benchmark_section(benchmark_cfg).items():
        merged_config[key] = copy.deepcopy(value)
    return merged_config


def prepare_fs_before_ready(benchmark_node: Any) -> bool:
    test_config = getattr(benchmark_node, "test_config", None)
    if not isinstance(test_config, dict) or not is_fs_workload(test_config):
        return False
    state = _ensure_fs_runtime(benchmark_node, _fs_runtime_config_from_test_config(test_config))
    if state.node_role == FS_RUNTIME_ROLE_AGENT:
        _prepare_fs_agent_before_ready(state=state)
        return True
    if state.node_role == FS_RUNTIME_ROLE_CLIENT:
        _prepare_fs_client_before_ready(state=state)
        return True
    raise ValueError(f"unsupported FS node_role: {state.node_role!r}")


def run_fs_worker(
    benchmark_node: Any,
    *,
    thread_id: int,
    deadline_ts: float,
    operation_result_cls: Any,
    operation_outcome: Any,
    metric_warmup_seconds: float,
    debug_print: Any,
) -> Optional[list[Any]]:
    test_config = getattr(benchmark_node, "test_config", None)
    if not isinstance(test_config, dict) or not is_fs_workload(test_config):
        return None

    state = _ensure_fs_runtime(benchmark_node, _fs_runtime_config_from_test_config(test_config))
    if state.node_role == FS_RUNTIME_ROLE_AGENT:
        while time.time() < float(deadline_ts):
            time.sleep(0.2)
        return []
    if state.node_role != FS_RUNTIME_ROLE_CLIENT:
        raise ValueError(f"unsupported FS node_role: {state.node_role!r}")
    return _run_fs_client_worker(
        benchmark_node,
        thread_id=thread_id,
        deadline_ts=deadline_ts,
        state=state,
        operation_result_cls=operation_result_cls,
        operation_outcome=operation_outcome,
        metric_warmup_seconds=metric_warmup_seconds,
        debug_print=debug_print,
    )


def close_fs_runtime(benchmark_node: Any, *, logger: Any, reason: str) -> None:
    state = getattr(benchmark_node, "_fluxon_fs_runtime_state", None)
    if isinstance(state, FSNodeRuntimeState):
        try:
            _close_fs_runtime_state(state)
        except Exception as exc:  # noqa: BLE001
            logger.warning("FS runtime cleanup failed: reason=%s err=%s", reason, exc)
        finally:
            setattr(benchmark_node, "_fluxon_fs_runtime_state", None)
