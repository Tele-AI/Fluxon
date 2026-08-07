"""Compatibility quick-start helpers for installed `fluxon_py` packages."""

from __future__ import annotations

from collections.abc import Mapping
import copy
import os
import re
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from fluxon_py.config import _to_plain_yaml_obj
from fluxon_py.runtime import (
    start_fs_agent_process,
    start_fs_master_process,
    start_kv_master_process,
    start_owner_kvclient_process,
)
from fluxon_py.runtime.process_runner import ManagedSubprocess, wait_subproc_or_ctrlc

__all__ = ["serve_s3_single_node"]

_DEFAULT_PANEL_PORT = 26180
_DEFAULT_EXPORT_NAME = "quick-start-export"
_DEFAULT_CACHE_MAX_BYTES = 1024 * 1024 * 1024
_DEFAULT_CLUSTER_NAME = "fluxon_s3"
_DEFAULT_FS_MASTER_INSTANCE_KEY = "fluxon_s3_fs_master"
_DEFAULT_FS_AGENT_INSTANCE_KEY = "fluxon_s3_fs_agent"
_DEFAULT_KV_MASTER_INSTANCE_KEY = "fluxon_s3_master"
_DEFAULT_KV_OWNER_INSTANCE_KEY = "fluxon_s3_owner"
_DEFAULT_SHARE_MEM_DIRNAME = "sharemem"
_DEFAULT_FS_MASTER_LOG_DIRNAME = "kv-master"
_DEFAULT_FS_OWNER_LOG_DIRNAME = "kv-owner"
_DEFAULT_ACCESS_DB_RELATIVE_PATH = Path("fs_master") / "access.db"
_DEFAULT_PY_REACTOR_MODE = "event_driven"
_EXPORT_NAME_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?$")


@dataclass(frozen=True)
class _S3SingleNodeBundle:
    data_root: Path
    state_root: Path
    kv_master_config: dict[str, Any]
    kv_owner_config: dict[str, Any]
    fs_master_config: dict[str, Any]
    fs_agent_config: dict[str, Any]
    kv_master_config_path: Path
    kv_owner_config_path: Path
    fs_master_config_path: Path
    fs_agent_config_path: Path
    kv_master_workdir: Path
    kv_owner_workdir: Path
    fs_master_workdir: Path
    fs_agent_workdir: Path
    share_mem_path: Path
    access_db_path: Path
    panel_port: int
    panel_public_base_url: str
    export_name: str

    @property
    def s3_endpoint(self) -> str:
        return f"{self.panel_public_base_url}/fs_s3"

    @property
    def s3_ui_url(self) -> str:
        return f"{self.s3_endpoint}/ui/"


def serve_s3_single_node(
    data_dir: str | os.PathLike[str],
    state_dir: str | os.PathLike[str],
    *,
    kv_master_config: Mapping[str, Any],
    kv_owner_config: Mapping[str, Any],
    export_name: str = _DEFAULT_EXPORT_NAME,
    start_middleware: bool = False,
    greptime_base_url: str | None = None,
    panel_port: int = _DEFAULT_PANEL_PORT,
    panel_listen_host: str = "0.0.0.0",
    bootstrap_username: str = "admin",
    bootstrap_password: str = "admin",
    export_cache_max_bytes: int = _DEFAULT_CACHE_MAX_BYTES,
) -> None:
    if start_middleware:
        raise NotImplementedError("quick_start only supports start_middleware=False")

    bundle = _build_s3_single_node_bundle(
        data_dir=data_dir,
        state_dir=state_dir,
        kv_master_config=kv_master_config,
        kv_owner_config=kv_owner_config,
        export_name=export_name,
        greptime_base_url=greptime_base_url,
        panel_port=panel_port,
        panel_listen_host=panel_listen_host,
        bootstrap_username=bootstrap_username,
        bootstrap_password=bootstrap_password,
        export_cache_max_bytes=export_cache_max_bytes,
    )

    _prepare_runtime_dirs(bundle)

    children: list[ManagedSubprocess] = []
    started = False
    try:
        print("[fluxon_quick_start] starting kv master...")
        kv_master_proc = start_kv_master_process(
            workdir=bundle.kv_master_workdir,
            config_path=bundle.kv_master_config_path,
            log_path=bundle.state_root / "log" / "kv_master.log",
        )
        children.append(ManagedSubprocess(label="kv_master", proc=kv_master_proc))
        _wait_for_process_alive(kv_master_proc, label="kv_master", seconds=10, log_path=bundle.state_root / "log" / "kv_master.log")

        print("[fluxon_quick_start] starting owner kvclient...")
        _clear_stale_shared_json(bundle.share_mem_path, bundle.kv_owner_config["fluxonkv_spec"]["cluster_name"])
        owner_proc = start_owner_kvclient_process(
            workdir=bundle.kv_owner_workdir,
            config_path=bundle.kv_owner_config_path,
            log_path=bundle.state_root / "log" / "kv_owner.log",
        )
        children.append(ManagedSubprocess(label="kv_owner", proc=owner_proc))
        _wait_for_shared_json(
            share_mem_path=bundle.share_mem_path,
            cluster_name=bundle.kv_owner_config["fluxonkv_spec"]["cluster_name"],
            proc=owner_proc,
            label="kv_owner",
            log_path=bundle.state_root / "log" / "kv_owner.log",
        )

        print("[fluxon_quick_start] starting fluxon_fs master...")
        fs_master_proc = start_fs_master_process(
            workdir=bundle.fs_master_workdir,
            config_path=bundle.fs_master_config_path,
            log_path=bundle.state_root / "log" / "fs_master.log",
        )
        children.append(ManagedSubprocess(label="fs_master", proc=fs_master_proc))
        _wait_for_tcp_ready(
            fs_master_proc,
            label="fs_master",
            host="127.0.0.1",
            port=bundle.panel_port,
            timeout=30,
            log_path=bundle.state_root / "log" / "fs_master.log",
        )

        print("[fluxon_quick_start] starting fluxon_fs agent...")
        fs_agent_proc = start_fs_agent_process(
            workdir=bundle.fs_agent_workdir,
            config_path=bundle.fs_agent_config_path,
            log_path=bundle.state_root / "log" / "fs_agent.log",
        )
        children.append(ManagedSubprocess(label="fs_agent", proc=fs_agent_proc))
        _wait_for_log_text(
            bundle.state_root / "log" / "fs_agent.log",
            "fluxon_fs agent ready",
            proc=fs_agent_proc,
            label="fs_agent",
        )

        print()
        print(f"S3 endpoint: {bundle.s3_endpoint}")
        print(f"Web UI:      {bundle.s3_ui_url}")
        print(f"bucket:      {bundle.export_name}")
        print(f"Basic Auth:   {bootstrap_username} / {bootstrap_password}")
        print(f"data dir:    {bundle.data_root}")
        print(f"state dir:   {bundle.state_root}")

        started = True
        wait_subproc_or_ctrlc(children, stop_timeout_seconds=5.0)
    finally:
        if not started:
            _terminate_children(children)


def _build_s3_single_node_bundle(
    *,
    data_dir: str | os.PathLike[str],
    state_dir: str | os.PathLike[str],
    kv_master_config: Mapping[str, Any],
    kv_owner_config: Mapping[str, Any],
    export_name: str,
    greptime_base_url: str | None,
    panel_port: int,
    panel_listen_host: str,
    bootstrap_username: str,
    bootstrap_password: str,
    export_cache_max_bytes: int,
) -> _S3SingleNodeBundle:
    if panel_port <= 0:
        raise ValueError("panel_port must be > 0")
    if export_cache_max_bytes <= 0:
        raise ValueError("export_cache_max_bytes must be > 0")
    if not bootstrap_username.strip():
        raise ValueError("bootstrap_username must be non-empty")
    if not bootstrap_password.strip():
        raise ValueError("bootstrap_password must be non-empty")
    if not panel_listen_host.strip():
        raise ValueError("panel_listen_host must be non-empty")
    _validate_export_name(export_name)

    data_root = Path(data_dir).expanduser().resolve()
    state_root = Path(state_dir).expanduser().resolve()
    data_root.mkdir(parents=True, exist_ok=True)
    state_root.mkdir(parents=True, exist_ok=True)

    kv_master = _plain_mapping_copy(kv_master_config, "kv_master_config")
    kv_owner = _plain_mapping_copy(kv_owner_config, "kv_owner_config")

    master_cluster_name = _require_str(
        kv_master.get("cluster_name") or _DEFAULT_CLUSTER_NAME,
        "kv_master_config.cluster_name",
    )
    _ensure_master_defaults(kv_master, state_root=state_root, greptime_base_url=greptime_base_url)

    owner_spec = _require_mapping(kv_owner.get("fluxonkv_spec"), "kv_owner_config.fluxonkv_spec")
    owner_cluster_name = owner_spec.get("cluster_name")
    if owner_cluster_name is None:
        owner_spec["cluster_name"] = master_cluster_name
    else:
        owner_cluster_name = _require_str(owner_cluster_name, "kv_owner_config.fluxonkv_spec.cluster_name")
        if owner_cluster_name != master_cluster_name:
            raise ValueError(
                "kv_owner_config.fluxonkv_spec.cluster_name must match kv_master_config.cluster_name"
            )

    etcd_endpoints = kv_master.get("etcd_endpoints")
    if not isinstance(etcd_endpoints, list) or not etcd_endpoints:
        raise ValueError("kv_master_config.etcd_endpoints must be a non-empty list")
    normalized_etcd_endpoints = [_require_str(endpoint, "kv_master_config.etcd_endpoints[]") for endpoint in etcd_endpoints]

    share_mem_path = Path(
        owner_spec.get("share_mem_path")
        or (state_root / _DEFAULT_SHARE_MEM_DIRNAME)
    ).expanduser().resolve()
    owner_spec["share_mem_path"] = str(share_mem_path)
    owner_spec.setdefault("sub_cluster", "default")
    owner_spec.setdefault("large_file_paths", [str(state_root / "kv-owner" / "large")])
    owner_spec.setdefault("etcd_addresses", list(normalized_etcd_endpoints))
    _ensure_owner_defaults(kv_owner, master_cluster_name=master_cluster_name)

    panel_public_base_url = f"http://127.0.0.1:{panel_port}"
    prometheus_base_url = _resolve_prometheus_base_url(
        greptime_base_url=greptime_base_url,
        kv_master_config=kv_master,
    )
    access_db_path = (state_root / _DEFAULT_ACCESS_DB_RELATIVE_PATH).resolve()

    fs_master_instance_key = str(
        kv_master.get("fs_master_instance_key")
        or f"{master_cluster_name}_fs_master"
        or _DEFAULT_FS_MASTER_INSTANCE_KEY
    )
    fs_agent_instance_key = str(
        kv_master.get("fs_agent_instance_key")
        or f"{master_cluster_name}_fs_agent"
        or _DEFAULT_FS_AGENT_INSTANCE_KEY
    )

    fs_master_config = {
        "kvclient": _build_external_kvclient_config(
            instance_key=fs_master_instance_key,
            cluster_name=master_cluster_name,
            share_mem_path=share_mem_path,
        ),
        "fluxon_fs": {
            "master": {
                "instance_key": fs_master_instance_key,
                "pull_interval_ms": 1000,
            },
            "master_panel": {
                "listen_addr": f"{panel_listen_host}:{panel_port}",
                "public_base_url": panel_public_base_url,
                "prometheus_base_url": prometheus_base_url,
                "auto_refresh_interval_secs": 2,
                "access_db_path": str(access_db_path),
                "bootstrap_access_model": {
                    "users": [
                        {
                            "username": bootstrap_username,
                            "password": bootstrap_password,
                            "can_manage_users": True,
                        }
                    ],
                    "scope_access": [],
                },
                "s3_gateway": {
                    "get_object_inflight_pieces": 8,
                    "kv_miss_policy": "remote_read",
                },
            },
            "cache": {
                "stale_window_ms": 1000,
                "rules": [],
                "exports": {
                    export_name: {
                        "remote_root_dir_abs": str(data_root),
                        "cache_max_bytes": export_cache_max_bytes,
                    }
                },
            },
        },
    }

    fs_agent_config = {
        "kvclient": _build_external_kvclient_config(
            instance_key=fs_agent_instance_key,
            cluster_name=master_cluster_name,
            share_mem_path=share_mem_path,
        ),
        "fluxon_fs": {
            "master": {
                "instance_key": fs_master_instance_key,
            },
            "cache": {
                "stale_window_ms": 1000,
                "rules": [],
                "exports": {
                    export_name: {
                        "remote_root_dir_abs": str(data_root),
                        "cache_max_bytes": export_cache_max_bytes,
                    }
                },
            },
        },
    }

    kv_master_config = dict(kv_master)
    kv_owner_config = dict(kv_owner)

    kv_master_workdir = state_root / _DEFAULT_FS_MASTER_LOG_DIRNAME
    kv_owner_workdir = state_root / _DEFAULT_FS_OWNER_LOG_DIRNAME
    fs_master_workdir = state_root / "fs_master_runtime"
    fs_agent_workdir = state_root / "fs_agent_runtime"

    kv_master_config_path = kv_master_workdir / "config.yaml"
    kv_owner_config_path = kv_owner_workdir / "config.yaml"
    fs_master_config_path = fs_master_workdir / "config.yaml"
    fs_agent_config_path = fs_agent_workdir / "config.yaml"

    return _S3SingleNodeBundle(
        data_root=data_root,
        state_root=state_root,
        kv_master_config=kv_master_config,
        kv_owner_config=kv_owner_config,
        fs_master_config=fs_master_config,
        fs_agent_config=fs_agent_config,
        kv_master_config_path=kv_master_config_path,
        kv_owner_config_path=kv_owner_config_path,
        fs_master_config_path=fs_master_config_path,
        fs_agent_config_path=fs_agent_config_path,
        kv_master_workdir=kv_master_workdir,
        kv_owner_workdir=kv_owner_workdir,
        fs_master_workdir=fs_master_workdir,
        fs_agent_workdir=fs_agent_workdir,
        share_mem_path=share_mem_path,
        access_db_path=access_db_path,
        panel_port=panel_port,
        panel_public_base_url=panel_public_base_url,
        export_name=export_name,
    )


def _ensure_master_defaults(
    kv_master: dict[str, Any],
    *,
    state_root: Path,
    greptime_base_url: str | None,
) -> None:
    kv_master.setdefault("cluster_name", _DEFAULT_CLUSTER_NAME)
    kv_master.setdefault("instance_key", _DEFAULT_KV_MASTER_INSTANCE_KEY)
    kv_master.setdefault("port", 25100)
    kv_master.setdefault("log_dir", str(state_root / "kv-master" / "log"))
    kv_master.setdefault("network", {"tcp_reactor_mode": _DEFAULT_PY_REACTOR_MODE})
    if "monitoring" not in kv_master:
        kv_master["monitoring"] = _build_monitoring_block(greptime_base_url)


def _ensure_owner_defaults(kv_owner: dict[str, Any], *, master_cluster_name: str) -> None:
    kv_owner.setdefault("instance_key", _DEFAULT_KV_OWNER_INSTANCE_KEY)
    kv_owner.setdefault("contribute_to_cluster_pool_size", {"dram": 1024 * 1024 * 1024, "vram": {}})
    kv_owner.setdefault("network", {"tcp_reactor_mode": _DEFAULT_PY_REACTOR_MODE})
    owner_spec = _require_mapping(kv_owner.get("fluxonkv_spec"), "kv_owner_config.fluxonkv_spec")
    owner_spec.setdefault("cluster_name", master_cluster_name)


def _build_external_kvclient_config(
    *,
    instance_key: str,
    cluster_name: str,
    share_mem_path: Path,
) -> dict[str, Any]:
    return {
        "instance_key": instance_key,
        "network": {"tcp_reactor_mode": _DEFAULT_PY_REACTOR_MODE},
        "fluxonkv_spec": {
            "cluster_name": cluster_name,
            "share_mem_path": str(share_mem_path),
        },
    }


def _build_monitoring_block(greptime_base_url: str | None) -> dict[str, Any]:
    base_url = _resolve_greptime_base_url(greptime_base_url)
    return {
        "prometheus_base_url": f"{base_url}/v1/prometheus",
        "prom_remote_write_url": [f"{base_url}/v1/prometheus/write"],
        "otlp_log_api": {
            "otlp_endpoint": f"{base_url}/v1/otlp/v1/logs",
            "db_name": "public",
            "table_name": "fluxon_logs",
        },
    }


def _resolve_prometheus_base_url(*, greptime_base_url: str | None, kv_master_config: dict[str, Any]) -> str:
    if greptime_base_url:
        return f"{greptime_base_url.rstrip('/')}/v1/prometheus"
    monitoring = kv_master_config.get("monitoring")
    if isinstance(monitoring, Mapping):
        prometheus_base_url = monitoring.get("prometheus_base_url")
        if isinstance(prometheus_base_url, str) and prometheus_base_url.strip():
            return prometheus_base_url
    return "http://127.0.0.1:24000/v1/prometheus"


def _resolve_greptime_base_url(greptime_base_url: str | None) -> str:
    if greptime_base_url:
        return greptime_base_url.rstrip("/")
    return "http://127.0.0.1:24000"


def _plain_mapping_copy(value: Mapping[str, Any], name: str) -> dict[str, Any]:
    plain = _to_plain_yaml_obj(value, name)
    if not isinstance(plain, dict):
        raise TypeError(f"{name} must decode to a mapping")
    return copy.deepcopy(plain)


def _require_mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be a mapping")
    return value


def _require_str(value: Any, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"{name} must be non-empty")
    return stripped


def _validate_export_name(export_name: str) -> None:
    if not _EXPORT_NAME_RE.fullmatch(export_name):
        raise ValueError(
            "export_name must match ^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?$"
        )


def _prepare_runtime_dirs(bundle: _S3SingleNodeBundle) -> None:
    for path in (
        bundle.state_root / "log",
        bundle.kv_master_workdir,
        bundle.kv_master_workdir / "log",
        bundle.kv_owner_workdir,
        bundle.fs_master_workdir,
        bundle.fs_agent_workdir,
        bundle.share_mem_path,
        bundle.access_db_path.parent,
        bundle.state_root / "kv-owner" / "large",
    ):
        path.mkdir(parents=True, exist_ok=True)
    _write_yaml(bundle.kv_master_config_path, bundle.kv_master_config)
    _write_yaml(bundle.kv_owner_config_path, bundle.kv_owner_config)
    _write_yaml(bundle.fs_master_config_path, bundle.fs_master_config)
    _write_yaml(bundle.fs_agent_config_path, bundle.fs_agent_config)


def _write_yaml(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    plain = _to_plain_yaml_obj(value, str(path))
    path.write_text(yaml.safe_dump(plain, sort_keys=False), encoding="utf-8")


def _clear_stale_shared_json(share_mem_path: Path, cluster_name: str) -> None:
    target = share_mem_path / cluster_name / "shared.json"
    if target.exists():
        target.unlink()


def _wait_for_shared_json(
    *,
    share_mem_path: Path,
    cluster_name: str,
    timeout: int = 180,
    proc: subprocess.Popen[bytes] | None = None,
    label: str = "owner",
    log_path: Path | None = None,
) -> None:
    target = share_mem_path / cluster_name / "shared.json"
    deadline = time.time() + timeout
    while time.time() < deadline:
        _raise_if_process_exited(proc, label=label, log_path=log_path)
        if target.exists():
            return
        time.sleep(0.5)
    _raise_if_process_exited(proc, label=label, log_path=log_path)
    raise RuntimeError(f"{label} did not create shared.json under {target.parent} within {timeout}s")


def _wait_for_tcp_ready(
    proc: subprocess.Popen[bytes],
    *,
    label: str,
    host: str,
    port: int,
    timeout: int,
    log_path: Path | None = None,
) -> None:
    probe_host = "127.0.0.1" if host in {"0.0.0.0", "::", "[::]"} else host
    deadline = time.time() + timeout
    while time.time() < deadline:
        _raise_if_process_exited(proc, label=label, log_path=log_path)
        try:
            with socket.create_connection((probe_host, port), timeout=1):
                return
        except OSError:
            time.sleep(0.5)
    _raise_if_process_exited(proc, label=label, log_path=log_path)
    raise RuntimeError(f"{label} did not open {probe_host}:{port} within {timeout}s")


def _wait_for_log_text(
    log_path: Path,
    needle: str,
    *,
    proc: subprocess.Popen[bytes] | None = None,
    label: str = "process",
    timeout: int = 60,
) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        _raise_if_process_exited(proc, label=label, log_path=log_path)
        if log_path.exists():
            try:
                text = log_path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                text = ""
            if needle in text:
                return
        time.sleep(0.5)
    _raise_if_process_exited(proc, label=label, log_path=log_path)
    raise RuntimeError(f"{label} log did not contain {needle!r} within {timeout}s: {log_path}")


def _wait_for_process_alive(
    proc: subprocess.Popen[bytes],
    *,
    label: str,
    seconds: int,
    log_path: Path | None = None,
) -> None:
    deadline = time.time() + seconds
    while time.time() < deadline:
        _raise_if_process_exited(proc, label=label, log_path=log_path)
        time.sleep(0.5)
    _raise_if_process_exited(proc, label=label, log_path=log_path)


def _raise_if_process_exited(
    proc: subprocess.Popen[bytes] | None,
    *,
    label: str,
    log_path: Path | None = None,
) -> None:
    if proc is None:
        return
    rc = proc.poll()
    if rc is None:
        return
    detail = f"{label} exited unexpectedly with rc={rc}"
    if log_path is not None and log_path.exists():
        detail += f"; log tail:\n{_tail_text(log_path)}"
    raise RuntimeError(detail)


def _tail_text(path: Path, limit: int = 4000) -> str:
    try:
        data = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    if len(data) <= limit:
        return data
    return data[-limit:]


def _terminate_children(children: list[ManagedSubprocess], timeout_seconds: float = 5.0) -> None:
    for child in reversed(children):
        if child.proc.poll() is None:
            try:
                child.proc.terminate()
            except Exception:
                pass
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if all(child.proc.poll() is not None for child in children):
            return
        time.sleep(0.2)
    for child in reversed(children):
        if child.proc.poll() is None:
            try:
                child.proc.kill()
            except Exception:
                pass
