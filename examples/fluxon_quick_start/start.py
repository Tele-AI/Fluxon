#!/usr/bin/env python3
"""
Fluxon Quick Start - unified entrypoint.

Usage:
  python3 start.py --mode kv   [options]   # interactive KV CLI (put/get/del)
  python3 start.py --mode mq   [options]   # interactive MQ shell (send/recv)
  python3 start.py --mode fs   [options]   # interactive FS shell (ls/cat) + web UI
"""

from __future__ import annotations

import argparse
import atexit
import json
import os
import select
import shlex
import subprocess
import sys
import textwrap
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from flask import Flask, Response, jsonify, request

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
REPO_ROOT_STR = str(REPO_ROOT)
if REPO_ROOT_STR not in sys.path:
    sys.path.insert(0, REPO_ROOT_STR)

from fluxon_py._api_ext_chan.mq_config_check import MIN_TTL as MQ_MIN_TTL_SECONDS
from fluxon_py.api_error import (
    ApiError,
    ChannelClosedError,
    GeneralError,
    InvalidArgumentError,
    KeyNotFoundError,
    ProducerClosedError,
    ValueTooLargeError,
)
from fluxon_py.config import FluxonKvClientConfig
from fluxon_py.kvclient import new_store
from fluxon_py.kvclient.kvclient_interface import KvClient
from fluxon_py.runtime import (
    register_ctrlc_callback,
    start_fs_agent_process,
    start_fs_master_process,
    start_kv_master_process,
    start_owner_kvclient_process,
)
from fluxon_py.runtime.process_runner import (
    build_parent_death_sigterm_preexec,
    decode_runtime_config_b64,
    encode_runtime_config_b64,
)


# helpers

# Ensure local connections bypass HTTP proxy
os.environ.setdefault("no_proxy", "127.0.0.1,localhost")
os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost")


def _wait_for_etcd(endpoint: str, timeout: int = 30) -> None:
    """Block until etcd responds or timeout."""
    import urllib.request
    deadline = time.time() + timeout
    url = f"http://{endpoint}/health"
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as r:
                if r.status == 200:
                    return
        except Exception:
            pass
        time.sleep(0.5)
    print(f"[quick_start] etcd not ready after {timeout}s at {endpoint}", file=sys.stderr)
    sys.exit(1)


_BINARY_URLS = {
    "etcd": {
        "url": "https://ghfast.top/https://github.com/etcd-io/etcd/releases/download/v3.5.17/etcd-v3.5.17-linux-amd64.tar.gz",
        "strip_prefix": "etcd-v3.5.17-linux-amd64",
        "files": ["etcd", "etcdctl"],
    },
    "greptime": {
        "url": "https://ghfast.top/https://github.com/GreptimeTeam/greptimedb/releases/download/v0.15.1/greptime-linux-amd64-v0.15.1.tar.gz",
        "strip_prefix": "greptime-linux-amd64-v0.15.1",
        "files": ["greptime"],
    },
}


def _auto_download_binary(name: str) -> str:
    """Download binary to bin/ if not found."""
    import tarfile
    import urllib.request

    info = _BINARY_URLS.get(name)
    if not info:
        print(f"[quick_start] no download source for: {name}", file=sys.stderr)
        sys.exit(1)

    bin_dir = SCRIPT_DIR / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)

    url = info["url"]
    print(f"[quick_start] downloading {name} from {url} ...")
    tar_path = bin_dir / f"{name}.tar.gz"
    urllib.request.urlretrieve(url, str(tar_path))

    with tarfile.open(str(tar_path), "r:gz") as tf:
        for fname in info["files"]:
            member = f"{info['strip_prefix']}/{fname}"
            try:
                m = tf.getmember(member)
            except KeyError:
                # try without prefix
                m = tf.getmember(fname)
            m.name = fname  # extract flat into bin/
            tf.extract(m, path=str(bin_dir))
            (bin_dir / fname).chmod(0o755)
            print(f"[quick_start]   extracted {fname}")

    tar_path.unlink()
    return str(bin_dir / name)


def _find_binary(name: str) -> str:
    """Find binary in bin/ or PATH, auto-download if missing."""
    local = SCRIPT_DIR / "bin" / name
    if local.exists():
        return str(local)
    import shutil
    found = shutil.which(name)
    if found:
        return found
    return _auto_download_binary(name)


_children: List[subprocess.Popen] = []
_URL_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
_KV_HTTP_APP = Flask(__name__)
_kv_http_store: Optional[KvClient] = None


def _cleanup():
    for p in reversed(_children):
        try:
            p.terminate()
        except Exception:
            pass
    for p in reversed(_children):
        try:
            p.wait(timeout=5)
        except Exception:
            try:
                p.kill()
            except Exception:
                pass


atexit.register(_cleanup)


def _track_child(proc: subprocess.Popen) -> subprocess.Popen:
    _children.append(proc)
    return proc


def _spawn(cmd: List[str], workdir: Path, logfile: Optional[Path] = None, **kwargs) -> subprocess.Popen:
    workdir.mkdir(parents=True, exist_ok=True)
    kwargs.setdefault("preexec_fn", build_parent_death_sigterm_preexec(expected_parent_pid=os.getpid()))
    if logfile:
        logfile.parent.mkdir(parents=True, exist_ok=True)
        fh = open(logfile, "a")
        p = subprocess.Popen(cmd, cwd=str(workdir), stdout=fh, stderr=subprocess.STDOUT, **kwargs)
    else:
        p = subprocess.Popen(cmd, cwd=str(workdir), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, **kwargs)
    _children.append(p)
    return p


def _http_open(req: urllib.request.Request, *, timeout: int) -> urllib.response.addinfourl:
    return _URL_OPENER.open(req, timeout=timeout)  # type: ignore[return-value]


def _kv_http_put(*, base_url: str, key: str, value: bytes, timeout: int) -> bool:
    url = f"{base_url}/api/kv/{urllib.parse.quote(key)}"
    req = urllib.request.Request(url, data=value, method="PUT")
    req.add_header("Content-Type", "application/octet-stream")
    try:
        with _http_open(req, timeout=timeout) as resp:
            if resp.status == 200:
                print(f"OK put key={key} size={len(value)}")
                return True
            print(f"ERR put status={resp.status}")
            return False
    except urllib.error.HTTPError as e:
        print(f"ERR put http_status={e.status} reason={e.reason}")
        return False
    except urllib.error.URLError as e:
        print(f"ERR put url={base_url} reason={e.reason}")
        return False


def _kv_http_get(*, base_url: str, key: str, timeout: int) -> Optional[bytes]:
    url = f"{base_url}/api/kv/{urllib.parse.quote(key)}"
    req = urllib.request.Request(url, method="GET")
    try:
        with _http_open(req, timeout=timeout) as resp:
            if resp.status == 200:
                return resp.read()
            print(f"ERR get status={resp.status}")
            return None
    except urllib.error.HTTPError as e:
        if e.status == 404:
            print(f"ERR get not_found key={key}")
        else:
            print(f"ERR get http_status={e.status} reason={e.reason}")
        return None
    except urllib.error.URLError as e:
        print(f"ERR get url={base_url} reason={e.reason}")
        return None


def _kv_http_delete(*, base_url: str, key: str, timeout: int) -> bool:
    url = f"{base_url}/api/kv/{urllib.parse.quote(key)}"
    req = urllib.request.Request(url, method="DELETE")
    try:
        with _http_open(req, timeout=timeout) as resp:
            if resp.status == 200:
                print(f"OK del key={key}")
                return True
            print(f"ERR del status={resp.status}")
            return False
    except urllib.error.HTTPError as e:
        if e.status == 404:
            print(f"ERR del not_found key={key}")
        else:
            print(f"ERR del http_status={e.status} reason={e.reason}")
        return False
    except urllib.error.URLError as e:
        print(f"ERR del url={base_url} reason={e.reason}")
        return False


def _kv_http_size(*, base_url: str, key: str, timeout: int) -> Optional[int]:
    url = f"{base_url}/api/kv/{urllib.parse.quote(key)}/size"
    req = urllib.request.Request(url, method="GET")
    try:
        with _http_open(req, timeout=timeout) as resp:
            if resp.status != 200:
                print(f"ERR size status={resp.status}")
                return None
            data = json.loads(resp.read().decode("utf-8"))
            size = data.get("size")
            if not isinstance(size, int):
                print(f"ERR size invalid_response={data!r}")
                return None
            print(f"OK size key={key} bytes={size}")
            return size
    except urllib.error.HTTPError as e:
        if e.status == 404:
            print(f"ERR size not_found key={key}")
        else:
            print(f"ERR size http_status={e.status} reason={e.reason}")
        return None
    except urllib.error.URLError as e:
        print(f"ERR size url={base_url} reason={e.reason}")
        return None


def _kv_http_health(*, base_url: str, timeout: int) -> bool:
    url = f"{base_url}/health"
    req = urllib.request.Request(url, method="GET")
    try:
        with _http_open(req, timeout=timeout) as resp:
            if resp.status == 200:
                print(f"OK health url={base_url}")
                return True
            print(f"ERR health status={resp.status}")
            return False
    except urllib.error.HTTPError as e:
        print(f"ERR health http_status={e.status} reason={e.reason}")
        return False
    except urllib.error.URLError as e:
        print(f"ERR health url={base_url} reason={e.reason}")
        return False


def _print_bytes(value: bytes) -> None:
    try:
        text = value.decode("utf-8")
    except UnicodeDecodeError:
        print(value.hex())
        return
    print(text)


def _list_non_loopback_ipv4() -> list[str]:
    res = subprocess.run(
        ["hostname", "-I"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    raw = res.stdout.strip()
    if not raw:
        return []

    out: list[str] = []
    for tok in raw.split():
        parts = tok.split(".")
        if len(parts) != 4:
            continue
        try:
            nums = [int(p) for p in parts]
        except ValueError:
            continue
        if any(n < 0 or n > 255 for n in nums):
            continue
        if tok.startswith("127.") or tok.startswith("169.254."):
            continue
        out.append(tok)

    seen: set[str] = set()
    uniq: list[str] = []
    for ip in out:
        if ip in seen:
            continue
        seen.add(ip)
        uniq.append(ip)
    return uniq


def _require_non_loopback_ipv4_for_host_mode() -> list[str]:
    non_loopback = _list_non_loopback_ipv4()
    if non_loopback:
        return non_loopback
    print(
        "ERR cannot detect any non-loopback IPv4 address via `hostname -I`. "
        "With `--network host`, the demo expects at least one non-127.x IPv4 address.",
        file=sys.stderr,
    )
    sys.exit(1)


def _build_panel_urls(*, panel_port: int, path: str) -> list[str]:
    if not panel_port:
        return []
    candidates = [f"http://127.0.0.1:{panel_port}{path}"]
    candidates.extend(f"http://{ip}:{panel_port}{path}" for ip in _require_non_loopback_ipv4_for_host_mode())
    seen: set[str] = set()
    result: list[str] = []
    for url in candidates:
        if url in seen:
            continue
        seen.add(url)
        result.append(url)
    return result


def _print_panel_urls(*, label: str, urls: list[str]) -> None:
    if not urls:
        return
    print(f"{label} (primary): {urls[0]}")
    if len(urls) > 1:
        print(f"{label} (all non-loopback):")
        for url in urls:
            print(f"- {url}")


def _kv_http_error_response(error: ApiError) -> Response:
    status_code = 500
    if isinstance(error, KeyNotFoundError):
        status_code = 404
    elif isinstance(error, InvalidArgumentError):
        status_code = 400
    elif isinstance(error, ValueTooLargeError):
        status_code = 413
    return jsonify(error.to_dict()), status_code


def _kv_http_read_payload_or_response(key: str) -> tuple[Optional[bytes], Optional[Response]]:
    if _kv_http_store is None:
        raise RuntimeError("kv http store is not initialized")
    result = _kv_http_store.get_blocking(key)
    if not result.is_ok():
        return None, _kv_http_error_response(result.unwrap_error())
    mem_holder = result.unwrap()
    decoded = mem_holder.access()
    if not decoded.is_ok():
        return None, _kv_http_error_response(decoded.unwrap_error())
    decoded_dict = decoded.unwrap()
    payload = decoded_dict.get("payload")
    if not isinstance(payload, bytes):
        return None, _kv_http_error_response(
            GeneralError(message=f"flat dict payload field is not bytes: {type(payload)}")
        )
    return payload, None


@_KV_HTTP_APP.route("/health", methods=["GET"])
def _kv_http_health_route():
    return jsonify({"status": "healthy", "service": "quick-start-kv-http"})


@_KV_HTTP_APP.route("/api/kv/<key>", methods=["PUT"])
def _kv_http_put_route(key: str):
    if _kv_http_store is None:
        raise RuntimeError("kv http store is not initialized")
    data = request.get_data()
    if not data:
        return _kv_http_error_response(InvalidArgumentError(message="Request body is empty"))
    result = _kv_http_store.put_blocking(key, {"payload": data})
    if not result.is_ok():
        return _kv_http_error_response(result.unwrap_error())
    _ = result.unwrap()
    return jsonify({"message": "Value stored successfully", "key": key, "size": len(data)})


@_KV_HTTP_APP.route("/api/kv/<key>", methods=["GET"])
def _kv_http_get_route(key: str):
    data, err_resp = _kv_http_read_payload_or_response(key)
    if err_resp is not None:
        return err_resp
    assert data is not None
    return Response(
        data,
        mimetype="application/octet-stream",
        headers={"Content-Length": str(len(data)), "X-KV-Key": key},
    )


@_KV_HTTP_APP.route("/api/kv/<key>", methods=["DELETE"])
def _kv_http_delete_route(key: str):
    if _kv_http_store is None:
        raise RuntimeError("kv http store is not initialized")
    result = _kv_http_store.remove(key)
    if not result.is_ok():
        return _kv_http_error_response(result.unwrap_error())
    _ = result.unwrap()
    return jsonify({"message": "Key deleted successfully", "key": key})


@_KV_HTTP_APP.route("/api/kv/<key>/size", methods=["GET"])
def _kv_http_size_route(key: str):
    data, err_resp = _kv_http_read_payload_or_response(key)
    if err_resp is not None:
        return err_resp
    assert data is not None
    return jsonify({"key": key, "size": len(data)})


def _load_config_from_b64(config_b64: str) -> Dict[str, Any]:
    loaded = yaml.safe_load(decode_runtime_config_b64(config_b64))
    if not isinstance(loaded, dict):
        raise ValueError("decoded config must be a mapping")
    return loaded


def _resolve_fluxonkv_spec_paths(*, spec: Dict[str, Any], workdir: Path) -> Dict[str, Any]:
    resolved = dict(spec)
    raw_path = resolved.get("share_mem_path")
    if isinstance(raw_path, str) and raw_path and not Path(raw_path).is_absolute():
        resolved["share_mem_path"] = str((workdir / raw_path).resolve())
    return resolved


def _run_kv_http_service(*, config: Dict[str, Any], workdir: Path) -> None:
    global _kv_http_store

    store_cfg = config.get("kvexternal_rexport_httpserver")
    if not isinstance(store_cfg, dict):
        raise ValueError("missing kvexternal_rexport_httpserver config")
    http_cfg = config.get("kvexternal_rexport_httpserver_http")
    if not isinstance(http_cfg, dict):
        raise ValueError("missing kvexternal_rexport_httpserver_http config")

    spec = store_cfg.get("fluxonkv_spec")
    if not isinstance(spec, dict):
        raise ValueError("kvexternal_rexport_httpserver.fluxonkv_spec must be a mapping")
    store_cfg = dict(store_cfg)
    store_cfg["fluxonkv_spec"] = _resolve_fluxonkv_spec_paths(spec=spec, workdir=workdir)

    result = new_store(FluxonKvClientConfig(store_cfg))
    if not result.is_ok():
        raise RuntimeError(f"kv http store init failed: {result.unwrap_error()}")
    _kv_http_store = result.unwrap()
    _wait_for_external_store_ready(_kv_http_store, label="kv http store", timeout=60)

    try:
        _KV_HTTP_APP.run(
            host=str(http_cfg.get("listen_addr", "0.0.0.0")),
            port=int(http_cfg.get("port", 8083)),
            debug=bool(http_cfg.get("debug", False)),
        )
    finally:
        if _kv_http_store is not None:
            _kv_http_store.close().unwrap("close failed")
            _kv_http_store = None


# config generation

def _monitoring_block(greptime_http_port: int) -> Dict[str, Any]:
    return {
        "prometheus_base_url": f"http://127.0.0.1:{greptime_http_port}/v1/prometheus",
        "prom_remote_write_url": [f"http://127.0.0.1:{greptime_http_port}/v1/prometheus/write"],
        "otlp_log_api": {
            "otlp_endpoint": f"http://127.0.0.1:{greptime_http_port}/v1/otlp/v1/logs",
            "db_name": "public",
            "table_name": "fluxon_logs",
        },
    }


def _owner_large_file_paths(workdir: Path) -> List[str]:
    return [str(workdir / "large" / "owner")]


def _gen_kv_config(etcd_ep: str, cluster: str, master_port: int, kv_http_port: int,
                    panel_port: int, greptime_http_port: int, workdir: Path) -> Dict[str, Any]:
    shm = str(workdir / "sharemem")
    log_dir = str(workdir / "log" / "master")
    master_cfg: Dict[str, Any] = {
        "etcd_endpoints": [etcd_ep],
        "cluster_name": cluster,
        "instance_key": "qs_master",
        "port": master_port,
        "log_dir": log_dir,
        "monitoring": _monitoring_block(greptime_http_port),
    }
    if panel_port:
        master_cfg["master_ui"] = {"http_listen_addr": f"0.0.0.0:{panel_port}"}
    cfg: Dict[str, Any] = {
        "master": master_cfg,
        "kvclient": {
            "instance_key": "qs_kvclient",
            "contribute_to_cluster_pool_size": {"dram": 1073741824, "vram": {}},
            "fluxonkv_spec": {
                "etcd_addresses": [etcd_ep],
                "cluster_name": cluster,
                "share_mem_path": shm,
                "sub_cluster": "default",
                "large_file_paths": _owner_large_file_paths(workdir),
            },
        },
        "kvexternal_rexport_httpserver_http": {
            "listen_addr": "0.0.0.0",
            "port": kv_http_port,
            "debug": False,
        },
        "kvexternal_rexport_httpserver": {
            "instance_key": "qs_http_accessor",
            "fluxonkv_spec": {
                "cluster_name": cluster,
                "share_mem_path": shm,
            },
        },
    }
    return cfg


def _gen_mq_config(etcd_ep: str, cluster: str, master_port: int, greptime_http_port: int,
                    workdir: Path, panel_port: int = 0) -> Dict[str, Any]:
    shm = str(workdir / "sharemem")
    log_dir = str(workdir / "log" / "master")
    master_cfg: Dict[str, Any] = {
        "etcd_endpoints": [etcd_ep],
        "cluster_name": cluster,
        "instance_key": "qs_master",
        "port": master_port,
        "log_dir": log_dir,
        "monitoring": _monitoring_block(greptime_http_port),
    }
    if panel_port:
        master_cfg["master_ui"] = {"http_listen_addr": f"0.0.0.0:{panel_port}"}
    cfg: Dict[str, Any] = {
        "master": master_cfg,
        "kvclient": {
            "instance_key": "qs_kvclient",
            "contribute_to_cluster_pool_size": {"dram": 1073741824, "vram": {}},
            "fluxonkv_spec": {
                "etcd_addresses": [etcd_ep],
                "cluster_name": cluster,
                "share_mem_path": shm,
                "sub_cluster": "default",
                "large_file_paths": _owner_large_file_paths(workdir),
            },
        },
        "kvexternal": {
            "instance_key": "qs_mq_external",
            "fluxonkv_spec": {
                "cluster_name": cluster,
                "share_mem_path": shm,
            },
        },
        "mpmc_demo": {
            "key": "qs_mq_chan",
            "capacity": 100,
            "ttl_seconds": MQ_MIN_TTL_SECONDS,
            "producer": {
                "instance_key": "qs_producer",
                "interval_seconds": 2,
                "count": 0,
            },
            "consumer": {
                "instance_key": "qs_consumer",
                "batch": 1,
            },
        },
    }
    return cfg


def _gen_fs_config(etcd_ep: str, cluster: str, master_port: int, panel_port: int,
                    greptime_http_port: int, workdir: Path) -> Dict[str, Any]:
    shm = str(workdir / "sharemem")
    log_dir = str(workdir / "log" / "master")
    remote_root_dir = str(workdir / "fs_remote_root")
    access_db_path = str(workdir / "fs_master" / "access.db")
    export_name = "quick-start-export"
    export_cache_max_bytes = 1073741824
    panel_public_base_url = ""
    if panel_port:
        panel_public_base_url = _build_panel_urls(panel_port=panel_port, path="")[0]
    cfg: Dict[str, Any] = {
        "master": {
            "etcd_endpoints": [etcd_ep],
            "cluster_name": cluster,
            "instance_key": "qs_master",
            "port": master_port,
            "log_dir": log_dir,
            "monitoring": _monitoring_block(greptime_http_port),
        },
        "kvclient": {
            "instance_key": "qs_kvclient",
            "contribute_to_cluster_pool_size": {"dram": 1073741824, "vram": {}},
            "fluxonkv_spec": {
                "etcd_addresses": [etcd_ep],
                "cluster_name": cluster,
                "share_mem_path": shm,
                "sub_cluster": "default",
                "large_file_paths": _owner_large_file_paths(workdir),
            },
        },
        "fs_master": {
            "kvclient": {
                "instance_key": "qs_fs_master",
                "fluxonkv_spec": {
                    "cluster_name": cluster,
                    "share_mem_path": shm,
                },
            },
            "fluxon_fs": {
                "master": {
                    "instance_key": "qs_fs_master",
                    "pull_interval_ms": 1000,
                },
                "master_panel": {
                    "listen_addr": f"0.0.0.0:{panel_port}",
                    "public_base_url": panel_public_base_url,
                    "prometheus_base_url": f"http://127.0.0.1:{greptime_http_port}/v1/prometheus",
                    "auto_refresh_interval_secs": 2,
                    "access_db_path": access_db_path,
                    "bootstrap_access_model": {
                        "users": [
                            {
                                "username": "admin",
                                "password": "admin",
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
                            "remote_root_dir_abs": remote_root_dir,
                            "cache_max_bytes": export_cache_max_bytes,
                        },
                    },
                },
            },
        },
        "fs_agent": {
            "kvclient": {
                "instance_key": "qs_fs_agent",
                "fluxonkv_spec": {
                    "cluster_name": cluster,
                    "share_mem_path": shm,
                },
            },
            "fluxon_fs": {
                "master": {
                    "instance_key": "qs_fs_master",
                },
                "cache": {
                    "stale_window_ms": 1000,
                    "rules": [],
                    "exports": {
                        export_name: {
                            "remote_root_dir_abs": remote_root_dir,
                            "cache_max_bytes": export_cache_max_bytes,
                        },
                    },
                },
            },
        },
        "fs_quick_start": {
            "panel_port": panel_port,
            "remote_root_dir_abs": remote_root_dir,
        },
    }
    return cfg


# start infrastructure

def _start_greptime(http_port: int, workdir: Path) -> subprocess.Popen:
    greptime_bin = _find_binary("greptime")
    data_dir = workdir / "greptime-data"
    data_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        greptime_bin, "standalone", "start",
        "--http-addr", f"127.0.0.1:{http_port}",
        "--rpc-bind-addr", "127.0.0.1:0",
        "--mysql-addr", "127.0.0.1:0",
        "--postgres-addr", "127.0.0.1:0",
        "--data-home", str(data_dir),
    ]
    return _spawn(cmd, workdir, logfile=workdir / "log" / "greptime.log")


def _normalize_local_probe_host(host: str) -> str:
    if host in {"0.0.0.0", "::", "[::]"}:
        return "127.0.0.1"
    return host


def _wait_for_tcp(host: str, port: int, label: str, timeout: int = 30) -> None:
    import socket
    probe_host = _normalize_local_probe_host(host)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            s.connect((probe_host, port))
            s.close()
            return
        except Exception:
            pass
        time.sleep(0.5)
    print(f"[quick_start] {label} not ready after {timeout}s at {probe_host}:{port}", file=sys.stderr)
    sys.exit(1)


def _start_etcd(port: int, workdir: Path) -> subprocess.Popen:
    etcd_bin = _find_binary("etcd")
    data_dir = workdir / "etcd-data"
    data_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        etcd_bin,
        "--data-dir", str(data_dir),
        "--listen-client-urls", f"http://0.0.0.0:{port}",
        "--advertise-client-urls", f"http://127.0.0.1:{port}",
        "--listen-peer-urls", "http://127.0.0.1:0",
    ]
    return _spawn(cmd, workdir, logfile=workdir / "log" / "etcd.log")


# start fluxon services

def _start_master(config: Dict[str, Any], workdir: Path) -> subprocess.Popen:
    return _track_child(
        start_kv_master_process(
            config=config,
            log_path=workdir / "log" / "master.log",
        )
    )


def _start_kvclient(config: Dict[str, Any], workdir: Path) -> subprocess.Popen:
    return _track_child(
        start_owner_kvclient_process(
            config=config,
            log_path=workdir / "log" / "kvclient.log",
        )
    )


def _start_kv_http_service(config: Dict[str, Any], workdir: Path) -> subprocess.Popen:
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "start.py"),
        "--quick-start-kv-http-server",
        "--config-b64",
        encode_runtime_config_b64(config),
        "--http-workdir",
        str(workdir / "kv_http_work"),
    ]
    return _spawn(cmd, workdir, logfile=workdir / "log" / "kv_http.log")


def _wait_for_service(name: str, seconds: int = 15) -> None:
    """Simple delay to let the service initialize."""
    print(f"[quick_start] waiting for {name} ({seconds}s)...")
    time.sleep(seconds)


def _raise_if_process_exited(
    proc: subprocess.Popen,
    *,
    label: str,
    log_path: Optional[Path] = None,
    log_dir: Optional[Path] = None,
) -> None:
    rc = proc.poll()
    if rc is None:
        return
    detail = f"[quick_start] {label} exited unexpectedly with rc={rc}"
    if log_path is not None:
        detail += f"; see log: {log_path}"
    if log_dir is not None:
        detail += f"; inspect logs under: {log_dir}"
    raise SystemExit(detail)


def _wait_for_process_alive(
    proc: subprocess.Popen,
    *,
    label: str,
    seconds: int,
    log_path: Optional[Path] = None,
    log_dir: Optional[Path] = None,
) -> None:
    print(f"[quick_start] waiting for {label} ({seconds}s)...")
    deadline = time.time() + seconds
    while time.time() < deadline:
        _raise_if_process_exited(
            proc,
            label=label,
            log_path=log_path,
            log_dir=log_dir,
        )
        time.sleep(0.5)
    _raise_if_process_exited(
        proc,
        label=label,
        log_path=log_path,
        log_dir=log_dir,
    )


def _wait_for_process_tcp_ready(
    proc: subprocess.Popen,
    *,
    label: str,
    host: str,
    port: int,
    timeout: int = 30,
    log_path: Optional[Path] = None,
    stable_seconds: int = 2,
) -> None:
    import socket

    probe_host = _normalize_local_probe_host(host)
    deadline = time.time() + timeout
    ready_since: Optional[float] = None
    while time.time() < deadline:
        _raise_if_process_exited(proc, label=label, log_path=log_path)
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            s.connect((probe_host, port))
            s.close()
            if ready_since is None:
                ready_since = time.time()
            if time.time() - ready_since >= stable_seconds:
                print(f"[quick_start] {label} ready at {probe_host}:{port}")
                return
        except Exception:
            ready_since = None
        time.sleep(0.5)
    _raise_if_process_exited(proc, label=label, log_path=log_path)
    raise SystemExit(f"[quick_start] {label} not ready after {timeout}s at {probe_host}:{port}")


def _wait_for_process_http_ready(
    proc: subprocess.Popen,
    *,
    label: str,
    url: str,
    timeout: int = 30,
    log_path: Optional[Path] = None,
    stable_seconds: int = 2,
) -> None:
    deadline = time.time() + timeout
    ready_since: Optional[float] = None
    while time.time() < deadline:
        _raise_if_process_exited(proc, label=label, log_path=log_path)
        try:
            with _http_open(urllib.request.Request(url, method="GET"), timeout=3) as resp:
                if 200 <= resp.status < 300:
                    if ready_since is None:
                        ready_since = time.time()
                    if time.time() - ready_since >= stable_seconds:
                        print(f"[quick_start] {label} ready at {url}")
                        return
                else:
                    ready_since = None
        except Exception:
            ready_since = None
        time.sleep(0.5)
    _raise_if_process_exited(proc, label=label, log_path=log_path)
    raise SystemExit(f"[quick_start] {label} not ready after {timeout}s at {url}")


def _wait_for_process_tcp_ready_best_effort(
    proc: subprocess.Popen,
    *,
    label: str,
    host: str,
    port: int,
    timeout: int = 30,
    log_path: Optional[Path] = None,
    stable_seconds: int = 2,
) -> bool:
    import socket

    probe_host = _normalize_local_probe_host(host)
    deadline = time.time() + timeout
    ready_since: Optional[float] = None
    while time.time() < deadline:
        _raise_if_process_exited(proc, label=label, log_path=log_path)
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            s.connect((probe_host, port))
            s.close()
            if ready_since is None:
                ready_since = time.time()
            if time.time() - ready_since >= stable_seconds:
                print(f"[quick_start] {label} ready at {probe_host}:{port}")
                return True
        except Exception:
            ready_since = None
        time.sleep(0.5)

    _raise_if_process_exited(proc, label=label, log_path=log_path)
    print(
        f"[quick_start] {label} not ready after {timeout}s at {probe_host}:{port}; continue with process-alive fallback",
        file=sys.stderr,
    )
    return False


def _kvclient_shared_json_target(share_mem_path: Path, cluster_name: str) -> Path:
    return share_mem_path / cluster_name / "shared.json"


def _clear_stale_shared_json(share_mem_path: Path, cluster_name: str) -> None:
    target = _kvclient_shared_json_target(share_mem_path, cluster_name)
    if target.exists():
        print(f"[quick_start] removing stale shared.json: {target}")
        target.unlink()


def _wait_for_shared_json(
    share_mem_path: Path,
    cluster_name: str,
    timeout: int = 180,
    *,
    proc: Optional[subprocess.Popen] = None,
    label: str = "kvclient",
    log_path: Optional[Path] = None,
) -> None:
    """Block until shared.json appears (owner kvclient ready)."""
    target = _kvclient_shared_json_target(share_mem_path, cluster_name)
    target_dir = target.parent
    deadline = time.time() + timeout
    elapsed = 0
    while time.time() < deadline:
        if proc is not None:
            _raise_if_process_exited(proc, label=label, log_path=log_path)
        if target.exists():
            print(f"[quick_start] shared.json ready ({elapsed}s)")
            return
        time.sleep(2)
        elapsed += 2
        if elapsed % 20 == 0:
            print(f"[quick_start] waiting for shared.json... ({elapsed}s)")
    if proc is not None:
        _raise_if_process_exited(proc, label=label, log_path=log_path)
    detail = f"[quick_start] ERROR: shared.json not found after {timeout}s at {target_dir}"
    if log_path is not None:
        detail += f"; see log: {log_path}"
    print(detail, file=sys.stderr)
    sys.exit(1)


def _start_cluster_infra(
    *,
    cfg: Dict[str, Any],
    workdir: Path,
    etcd_client_port: int,
    greptime_port: int,
) -> None:
    etcd_ep = f"127.0.0.1:{etcd_client_port}"
    greptime_log_path = workdir / "log" / "greptime.log"
    etcd_log_path = workdir / "log" / "etcd.log"
    master_log_path = workdir / "log" / "master.log"
    kvclient_log_path = workdir / "log" / "kvclient.log"
    share_mem_path = _kvclient_share_mem_path_from_cfg(cfg)
    cluster_name = _kvclient_cluster_name_from_cfg(cfg)
    log_dir = workdir / "log"

    print("[quick_start] starting greptime...")
    greptime_proc = _start_greptime(greptime_port, workdir)
    _wait_for_process_tcp_ready(
        greptime_proc,
        label="greptime",
        host="127.0.0.1",
        port=greptime_port,
        timeout=30,
        log_path=greptime_log_path,
    )

    print("[quick_start] starting etcd...")
    etcd_proc = _start_etcd(etcd_client_port, workdir)
    _wait_for_process_http_ready(
        etcd_proc,
        label="etcd",
        url=f"http://{etcd_ep}/health",
        timeout=30,
        log_path=etcd_log_path,
    )

    print("[quick_start] starting master...")
    master_proc = _start_master(cfg["master"], workdir)
    master_ui_cfg = cfg["master"].get("master_ui")
    if isinstance(master_ui_cfg, dict):
        listen_addr = master_ui_cfg.get("http_listen_addr")
        if isinstance(listen_addr, str) and ":" in listen_addr:
            host, port_text = listen_addr.rsplit(":", 1)
            ui_ready = _wait_for_process_tcp_ready_best_effort(
                master_proc,
                label="master",
                host=host,
                port=int(port_text),
                timeout=30,
                log_path=master_log_path,
            )
            if not ui_ready:
                _wait_for_process_alive(
                    master_proc,
                    label="master",
                    seconds=10,
                    log_path=master_log_path,
                    log_dir=log_dir,
                )
        else:
            _wait_for_process_alive(
                master_proc,
                label="master",
                seconds=10,
                log_path=master_log_path,
                log_dir=log_dir,
            )
    else:
        _wait_for_process_alive(
            master_proc,
            label="master",
            seconds=10,
            log_path=master_log_path,
            log_dir=log_dir,
        )

    print("[quick_start] starting kvclient...")
    _clear_stale_shared_json(share_mem_path, cluster_name)
    kvclient_proc = _start_kvclient(cfg["kvclient"], workdir)
    _wait_for_shared_json(
        share_mem_path,
        cluster_name,
        proc=kvclient_proc,
        label="kvclient",
        log_path=kvclient_log_path,
    )


def _kvclient_share_mem_path_from_cfg(cfg: Dict[str, Any]) -> Path:
    kvclient_cfg = cfg.get("kvclient")
    if not isinstance(kvclient_cfg, dict):
        raise ValueError("missing kvclient config")
    spec = kvclient_cfg.get("fluxonkv_spec")
    if not isinstance(spec, dict):
        raise ValueError("missing kvclient.fluxonkv_spec config")
    raw_path = spec.get("share_mem_path")
    if not isinstance(raw_path, str) or not raw_path:
        raise ValueError("kvclient.fluxonkv_spec.share_mem_path must be a non-empty string")
    return Path(raw_path)


def _kvclient_cluster_name_from_cfg(cfg: Dict[str, Any]) -> str:
    kvclient_cfg = cfg.get("kvclient")
    if not isinstance(kvclient_cfg, dict):
        raise ValueError("missing kvclient config")
    spec = kvclient_cfg.get("fluxonkv_spec")
    if not isinstance(spec, dict):
        raise ValueError("missing kvclient.fluxonkv_spec config")
    cluster_name = spec.get("cluster_name")
    if not isinstance(cluster_name, str) or not cluster_name:
        raise ValueError("kvclient.fluxonkv_spec.cluster_name must be a non-empty string")
    return cluster_name


def _wait_for_http(url: str, label: str, timeout: int = 120) -> None:
    """Block until HTTP endpoint returns 2xx."""
    import urllib.request
    deadline = time.time() + timeout
    elapsed = 0
    while time.time() < deadline:
        try:
            r = urllib.request.urlopen(url, timeout=3)
            if 200 <= r.status < 300:
                print(f"[quick_start] {label} ready ({elapsed}s)")
                return
        except Exception:
            pass
        time.sleep(2)
        elapsed += 2
        if elapsed % 20 == 0:
            print(f"[quick_start] waiting for {label}... ({elapsed}s)")
    print(f"[quick_start] WARNING: {label} not ready after {timeout}s", file=sys.stderr)


def _wait_for_external_store_ready(store, *, label: str, timeout: int = 60) -> None:
    deadline = time.time() + timeout
    last_error = "unknown"
    while time.time() < deadline:
        try:
            endpoints = store.get_etcd_config()
            if isinstance(endpoints, list) and endpoints:
                print(f"[quick_start] {label} external bootstrap ready ({len(endpoints)} etcd endpoint(s))")
                return
            last_error = f"empty etcd endpoints: {endpoints!r}"
        except Exception as e:  # noqa: BLE001
            last_error = str(e)
        time.sleep(0.5)
    raise SystemExit(f"[quick_start] {label} external bootstrap not ready after {timeout}s: {last_error}")


def _run_kv_interactive(kv_http_port: int, panel_port: int, cluster_name: str) -> None:
    url = f"http://127.0.0.1:{kv_http_port}"

    _wait_for_http(f"{url}/health", "kv http", timeout=120)

    ui_urls = _build_panel_urls(
        panel_port=panel_port,
        path=f"/view?cluster_name={cluster_name}&member_kind=kv",
    )

    print("\n=== Fluxon KV Quick Start ===")
    print(f"KV HTTP: {url}")
    _print_panel_urls(label="KV Web UI", urls=ui_urls)
    print("Commands: put/get/del/size/health/exit")

    prompt = f"[kv={url}]> "
    while True:
        try:
            line = input(prompt)
        except EOFError:
            print()
            return
        except KeyboardInterrupt:
            print()
            return

        line = line.strip()
        if not line:
            continue

        try:
            parts = shlex.split(line)
        except ValueError as e:
            print(f"ERR parse: {e}")
            continue

        cmd = parts[0].lower()
        if cmd in ("exit", "quit"):
            return
        if cmd == "help":
            print("Commands: put/get/del/size/health/exit")
            continue
        if cmd == "health":
            _ = _kv_http_health(base_url=url, timeout=5)
            continue
        if cmd == "put":
            if len(parts) < 3:
                print("ERR usage: put <key> <value>")
                continue
            key = parts[1]
            value = " ".join(parts[2:]).encode("utf-8")
            _ = _kv_http_put(base_url=url, key=key, value=value, timeout=5)
            continue
        if cmd == "get":
            if len(parts) != 2:
                print("ERR usage: get <key>")
                continue
            data = _kv_http_get(base_url=url, key=parts[1], timeout=5)
            if data is not None:
                _print_bytes(data)
            continue
        if cmd in ("del", "delete"):
            if len(parts) != 2:
                print("ERR usage: del <key>")
                continue
            _ = _kv_http_delete(base_url=url, key=parts[1], timeout=5)
            continue
        if cmd == "size":
            if len(parts) != 2:
                print("ERR usage: size <key>")
                continue
            _ = _kv_http_size(base_url=url, key=parts[1], timeout=5)
            continue
        print(f"ERR unknown_command={cmd}")


def mode_kv(args) -> None:
    workdir = args.workdir
    cluster = "qs_kv_cluster"
    greptime_port = args.greptime_http_port

    cfg = _gen_kv_config(f"127.0.0.1:{args.etcd_client_port}", cluster, args.master_p2p_port, args.kv_http_port,
                          args.panel_port, greptime_port, workdir)

    _start_cluster_infra(
        cfg=cfg,
        workdir=workdir,
        etcd_client_port=args.etcd_client_port,
        greptime_port=greptime_port,
    )

    print("[quick_start] starting kv http server...")
    kv_http_log_path = workdir / "log" / "kv_http.log"
    kv_http_proc = _start_kv_http_service(cfg, workdir)
    _wait_for_process_http_ready(
        kv_http_proc,
        label="kv http server",
        url=f"http://127.0.0.1:{args.kv_http_port}/health",
        timeout=30,
        log_path=kv_http_log_path,
    )

    _run_kv_interactive(args.kv_http_port, args.panel_port, cluster)


# mode: MQ

def _mq_start_infra(args, workdir: Path, etcd_ep: str, greptime_port: int) -> Dict[str, Any]:
    """Start greptime/etcd/master/kvclient, return aggregated config dict."""
    cluster = "qs_mq_cluster"
    cfg = _gen_mq_config(etcd_ep, cluster, args.kv_master_port, greptime_port, workdir,
                          panel_port=args.panel_port)
    _start_cluster_infra(
        cfg=cfg,
        workdir=workdir,
        etcd_client_port=args.etcd_client_port,
        greptime_port=greptime_port,
    )
    return cfg


def mode_mq(args) -> None:
    workdir = args.workdir
    etcd_ep = f"127.0.0.1:{args.etcd_client_port}"
    greptime_port = args.greptime_http_port
    cfg = _mq_start_infra(args, workdir, etcd_ep, greptime_port)
    cfg["quickstart_monitor"] = {"panel_port": args.panel_port}
    _run_mq_shell(cfg, workdir)


def _must_ok(res, msg: str):
    if not res.is_ok():
        raise SystemExit(f"{msg}: {res.unwrap_error()}")
    return res.unwrap()


def _best_effort_close_result(obj, logger, role: str) -> None:
    try:
        close_res = obj.close()
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[{role}] close raised (ignored): {e}")
        return

    if close_res.is_ok():
        _ = close_res.unwrap()
    else:
        logger.warning(f"[{role}] close error (ignored): {close_res.unwrap_error()}")


def _build_mq_store_config(*, cfg: Dict[str, Any], workdir: Path) -> FluxonKvClientConfig:
    ext_cfg = dict(cfg["kvexternal"])
    spec = ext_cfg.get("fluxonkv_spec")
    if not isinstance(spec, dict):
        raise ValueError("kvexternal.fluxonkv_spec must be a mapping")
    ext_cfg["fluxonkv_spec"] = _resolve_fluxonkv_spec_paths(spec=spec, workdir=workdir)
    return FluxonKvClientConfig(ext_cfg)


def _run_mq_shell(cfg: Dict[str, Any], workdir: Path) -> None:
    os.environ.setdefault("RUST_LOG", "warn")

    from fluxon_py.api_ext_chan import ChanRole, ChanType, new_or_bind_with_unique_key
    from fluxon_py.logging import init_logger

    logger = init_logger("qs_mq_shell")
    panel_port = 0
    quickstart_monitor_cfg = cfg.get("quickstart_monitor")
    if isinstance(quickstart_monitor_cfg, dict):
        raw_panel_port = quickstart_monitor_cfg.get("panel_port")
        if isinstance(raw_panel_port, int):
            panel_port = raw_panel_port
    demo_cfg = cfg["mpmc_demo"]
    chan_key = str(demo_cfg["key"])
    chan_cfg = {"capacity": int(demo_cfg["capacity"]), "ttl_seconds": int(demo_cfg["ttl_seconds"])}
    shutdown_requested = threading.Event()
    consumer_done = threading.Event()
    close_lock = threading.Lock()
    interrupted = False
    handles_closed = False
    store = None
    producer = None
    consumer = None
    restore_signal_listener = lambda: None
    prompt_delay_lock = threading.Lock()
    next_prompt_allowed_at = 0.0

    def _delay_next_prompt(seconds: float) -> None:
        nonlocal next_prompt_allowed_at
        with prompt_delay_lock:
            next_prompt_allowed_at = max(next_prompt_allowed_at, time.monotonic() + seconds)

    def _remaining_prompt_delay() -> float:
        with prompt_delay_lock:
            return max(0.0, next_prompt_allowed_at - time.monotonic())

    def _mq_status_lines() -> list[str]:
        return [
            "MQ shell status:",
            f"  channel_key={chan_key}",
            f"  producer_chan_id={producer.get_chan_id()}",
            f"  producer_id={producer.get_producer_id()}",
            f"  consumer_chan_id={consumer.get_chan_id()}",
            f"  consumer_id={consumer.get_consumer_id()}",
            f"  shutdown_requested={shutdown_requested.is_set()}",
            f"  consumer_done={consumer_done.is_set()}",
        ]

    def _handle_mq_shell_line(line: str) -> tuple[bool, str | None]:
        parts = line.split(None, 1)
        cmd = parts[0].lower()
        if cmd in ("exit", "quit", "q"):
            shutdown_requested.set()
            return True, None
        if cmd == "help":
            print("Commands:  put <message>  |  status  |  exit")
            return True, None
        if cmd == "status":
            for status_line in _mq_status_lines():
                print(status_line)
            return True, None

        msg = parts[1] if cmd == "put" and len(parts) >= 2 else line
        return False, msg

    def _close_handles_once(*, reason: str) -> None:
        nonlocal handles_closed
        with close_lock:
            if handles_closed:
                return
            handles_closed = True
        logger.info(f"[mq] closing handles because {reason}")
        if producer is not None:
            _best_effort_close_result(producer, logger, "producer")
        if consumer is not None:
            _best_effort_close_result(consumer, logger, "consumer")

    def _consumer_loop() -> None:
        try:
            logger.info(f"[consumer] ready: channel_key={chan_key}")
            while not shutdown_requested.is_set():
                get_res = consumer.get_data(batch_size=int(demo_cfg["consumer"]["batch"]))
                if not get_res.is_ok():
                    err = get_res.unwrap_error()
                    if isinstance(err, ChannelClosedError):
                        logger.info("[consumer] close observed, exit loop")
                        break
                    raise SystemExit(f"get_data failed: {err}")
                for item in get_res.unwrap() or []:
                    payload = item.get("payload", b"") if isinstance(item, dict) else item
                    if isinstance(payload, (bytes, bytearray, memoryview)):
                        print(f"  [recv] {bytes(payload).decode('utf-8', 'ignore')}")
                    else:
                        print(f"  [recv] {payload}")
                    _delay_next_prompt(1.0)
                if shutdown_requested.wait(0.2):
                    break
        finally:
            consumer_done.set()

    try:
        print("[quick_start] connecting to cluster...")
        store = _must_ok(new_store(_build_mq_store_config(cfg=cfg, workdir=workdir)), "new_store failed")
        _wait_for_external_store_ready(store, label="mq store", timeout=60)

        print("[quick_start] binding to channel...")
        producer = _must_ok(
            new_or_bind_with_unique_key(
                store,
                chan_cfg,
                unique_id=chan_key,
                chan_type=ChanType.MPMC,
                chan_role=ChanRole.PRODUCER,
            ),
            "bind producer failed",
        )
        consumer = _must_ok(
            new_or_bind_with_unique_key(
                store,
                chan_cfg,
                unique_id=chan_key,
                chan_type=ChanType.MPMC,
                chan_role=ChanRole.CONSUMER,
            ),
            "bind consumer failed",
        )

        def _on_ctrlc(reason: str) -> None:
            nonlocal interrupted
            interrupted = True
            shutdown_requested.set()
            _close_handles_once(reason=reason)

        restore_signal_listener = register_ctrlc_callback(
            _on_ctrlc,
            thread_name="qs-mq-shell-signal",
        )

        consumer_thread = threading.Thread(target=_consumer_loop, name="qs-mq-consumer", daemon=True)
        consumer_thread.start()

        print("\n=== Fluxon MQ Producer ===")
        mq_ui_urls = _build_panel_urls(
            panel_port=panel_port,
            path="/view?cluster_name=qs_mq_cluster&member_kind=mq",
        )
        _print_panel_urls(label="MQ Web UI", urls=mq_ui_urls)
        print("Commands:  put <message>  |  status  |  exit\n")
        time.sleep(1.0)

        prompt_visible = False
        while not shutdown_requested.is_set():
            if not prompt_visible:
                remaining_delay = _remaining_prompt_delay()
                if remaining_delay > 0:
                    if shutdown_requested.wait(min(remaining_delay, 0.2)):
                        break
                    continue
                print("mq> ", end="", flush=True)
                prompt_visible = True
            ready, _, _ = select.select([sys.stdin], [], [], 0.2)
            if not ready:
                continue
            raw_line = sys.stdin.readline()
            prompt_visible = False
            if raw_line == "":
                print()
                shutdown_requested.set()
                break

            line = raw_line.strip()
            if not line:
                continue

            handled, msg = _handle_mq_shell_line(line)
            if handled:
                if shutdown_requested.is_set():
                    break
                continue

            assert msg is not None
            ts_ms = int(time.time() * 1000)
            put_res = producer.put_data({"payload": msg.encode("utf-8"), "ts_ms": ts_ms})
            if put_res.is_ok():
                _ = put_res.unwrap()
                print(f"  [sent] {msg}")
                _delay_next_prompt(1.0)
                continue

            err = put_res.unwrap_error()
            if isinstance(err, ProducerClosedError):
                logger.info("[producer] close observed, exit loop")
                break
            raise SystemExit(f"put_data failed: {err}")

        if shutdown_requested.is_set() and not handles_closed:
            _close_handles_once(reason="mq shell shutdown requested")
        if not consumer_done.wait(timeout=5):
            raise SystemExit("mq consumer thread did not exit within 5 seconds")
    finally:
        restore_signal_listener()
        shutdown_requested.set()
        if not handles_closed:
            _close_handles_once(reason="quick start shutdown")
        if store is not None:
            _best_effort_close_result(store, logger, "store")

    print("\nBye.")
    if interrupted:
        raise SystemExit(130)




# mode: FS

def mode_fs(args) -> None:
    workdir = args.workdir
    cluster = "qs_fs_cluster"
    greptime_port = args.greptime_http_port

    cfg = _gen_fs_config(f"127.0.0.1:{args.etcd_client_port}", cluster, args.kv_master_port, args.panel_port,
                          greptime_port, workdir)

    # prepare fs remote root with a sample file
    fs_root = workdir / "fs_remote_root"
    fs_root.mkdir(parents=True, exist_ok=True)
    sample = fs_root / "hello.txt"
    if not sample.exists():
        sample.write_text("Hello from Fluxon FS!\n")

    _start_cluster_infra(
        cfg=cfg,
        workdir=workdir,
        etcd_client_port=args.etcd_client_port,
        greptime_port=greptime_port,
    )

    print("[quick_start] starting fluxon_fs master...")
    fs_master_log_path = workdir / "log" / "fs_master.log"
    fs_master_proc = _track_child(
        start_fs_master_process(
            workdir=workdir / "fs_master_runtime",
            config=cfg["fs_master"],
            log_path=fs_master_log_path,
        )
    )
    if args.panel_port:
        _wait_for_process_tcp_ready(
            fs_master_proc,
            label="fs_master",
            host="127.0.0.1",
            port=args.panel_port,
            timeout=30,
            log_path=fs_master_log_path,
        )
    else:
        _wait_for_process_alive(
            fs_master_proc,
            label="fs_master",
            seconds=5,
            log_path=fs_master_log_path,
        )

    print("[quick_start] starting fluxon_fs agent...")
    fs_agent_log_path = workdir / "log" / "fs_agent.log"
    fs_agent_proc = _track_child(
        start_fs_agent_process(
            workdir=workdir / "fs_agent_runtime",
            config=cfg["fs_agent"],
            log_path=fs_agent_log_path,
        )
    )
    _wait_for_process_alive(
        fs_agent_proc,
        label="fs_agent",
        seconds=3,
        log_path=fs_agent_log_path,
    )

    panel_urls = _build_panel_urls(panel_port=args.panel_port, path="")
    if panel_urls:
        print()
        _print_panel_urls(label="FS Web UI", urls=panel_urls)
    print(f"  FS remote root: {fs_root}")

    _run_fs_interactive(fs_root)


def _run_fs_interactive(fs_root: Path) -> None:
    """Interactive FS shell: ls / cat over the remote-mounted directory."""

    print("\n=== Fluxon FS Quick Start ===")
    print(f"Root: {fs_root}")
    print("Commands:  ls [path]  |  cat <file>  |  echo \"text\" > <file>  |  ui  |  exit")
    print()

    cwd = fs_root

    try:
        while True:
            rel = os.path.relpath(cwd, fs_root)
            prompt = f"fs:{rel}> " if rel != "." else "fs:/> "
            try:
                line = input(prompt).strip()
            except EOFError:
                break
            if not line:
                continue

            parts = line.split()
            cmd = parts[0].lower()

            if cmd in ("exit", "quit", "q"):
                break
            elif cmd == "ls":
                target = cwd / parts[1] if len(parts) > 1 else cwd
                if not target.exists():
                    print(f"  not found: {target}")
                    continue
                if target.is_dir():
                    entries = sorted(target.iterdir())
                    for e in entries:
                        suffix = "/" if e.is_dir() else ""
                        print(f"  {e.name}{suffix}")
                    if not entries:
                        print("  (empty)")
                else:
                    print(f"  {target.name}")
            elif cmd == "cat" and len(parts) >= 2:
                target = cwd / parts[1]
                if not target.exists():
                    print(f"  not found: {target}")
                elif target.is_dir():
                    print(f"  is a directory: {target.name}")
                else:
                    try:
                        print(target.read_text(errors="replace"))
                    except Exception as e:
                        print(f"  error: {e}")
            elif cmd == "cd" and len(parts) >= 2:
                target = (cwd / parts[1]).resolve()
                if not str(target).startswith(str(fs_root)):
                    print("  cannot navigate above root")
                elif not target.is_dir():
                    print(f"  not a directory: {parts[1]}")
                else:
                    cwd = target
            elif cmd == "echo" and ">" in line:
                # simple: echo "text" > filename
                idx = line.index(">")
                text_part = line[4:idx].strip().strip('"').strip("'")
                fname = line[idx + 1:].strip()
                if fname:
                    target = cwd / fname
                    target.write_text(text_part + "\n")
                    print(f"  wrote {target.name}")
                else:
                    print("  usage: echo \"text\" > filename")
            elif cmd == "ui":
                print("  (see Web UI URL printed above)")
            else:
                print("Commands:  ls [path]  |  cat <file>  |  cd <dir>  |  echo \"text\" > <file>  |  ui  |  exit")
    except KeyboardInterrupt:
        pass
    finally:
        print("\nBye.")

# main

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fluxon Quick Start",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Examples:
              python3 start.py --mode kv --etcd-client-port 12379 --master-p2p-port 31000 --greptime-http-port 14000 --kv-http-port 8083
              python3 start.py --mode mq --etcd-client-port 37379 --kv-master-port 34200 --greptime-http-port 14000 --panel-port 18080
              python3 start.py --mode fs --etcd-client-port 36379 --kv-master-port 34100 --greptime-http-port 14000 --panel-port 34180
        """),
    )
    parser.add_argument("--quick-start-kv-http-server", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--config-b64", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--http-workdir", type=Path, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--mode", choices=["kv", "mq", "fs"], help="Quick start mode")
    parser.add_argument("--etcd-client-port", type=int, default=12379, help="etcd client port")
    parser.add_argument("--master-p2p-port", type=int, default=31000, help="master p2p port (kv mode)")
    parser.add_argument("--kv-master-port", type=int, default=34200, help="master port (mq/fs mode)")
    parser.add_argument("--kv-http-port", type=int, default=8083, help="KV HTTP re-export port (kv mode)")
    parser.add_argument("--panel-port", type=int, default=0, help="Web panel port (mq/fs; optional for kv)")
    parser.add_argument("--greptime-http-port", type=int, default=0, help="Greptime HTTP port")
    parser.add_argument("--workdir", type=Path, default=None, help="Working directory (default: auto)")
    args = parser.parse_args()

    if args.quick_start_kv_http_server:
        if args.config_b64 is None:
            raise ValueError("--config-b64 is required with --quick-start-kv-http-server")
        if args.http_workdir is None:
            raise ValueError("--http-workdir is required with --quick-start-kv-http-server")
        _run_kv_http_service(
            config=_load_config_from_b64(args.config_b64),
            workdir=args.http_workdir.resolve(),
        )
        return

    if args.mode is None:
        raise ValueError("--mode is required unless --quick-start-kv-http-server is set")
    if args.greptime_http_port == 0:
        raise ValueError("--greptime-http-port is required")
    if args.mode == "mq" and args.panel_port == 0:
        raise ValueError("--panel-port is required in mq mode")

    if args.workdir is None:
        args.workdir = SCRIPT_DIR / "fluxon_work" / f"qs_{args.mode}"
    args.workdir = args.workdir.resolve()
    args.workdir.mkdir(parents=True, exist_ok=True)

    print(f"[quick_start] mode={args.mode} workdir={args.workdir}")

    if args.mode == "kv":
        mode_kv(args)
    elif args.mode == "mq":
        mode_mq(args)
    elif args.mode == "fs":
        mode_fs(args)


if __name__ == "__main__":
    main()
