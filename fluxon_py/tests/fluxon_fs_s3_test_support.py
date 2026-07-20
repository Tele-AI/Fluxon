from __future__ import annotations

import base64
import http.server
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
INTEGRATION_READY_TIMEOUT_SECS = 180.0


def _read_text_or_empty(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _build_subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    for var in (
        "http_proxy",
        "https_proxy",
        "no_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
    ):
        env.pop(var, None)
    env["PYTHONUNBUFFERED"] = "1"
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        str(REPO_ROOT)
        if not existing_pythonpath
        else f"{REPO_ROOT}:{existing_pythonpath}"
    )
    env.setdefault("FLUXON_LOG", "info")
    env.setdefault("LOG_LEVEL", "INFO")
    return env


def _spawn_logged(
    *,
    cmd: list[str],
    workdir: Path,
    log_path: Path,
    env: dict[str, str],
) -> subprocess.Popen[str]:
    workdir.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        return subprocess.Popen(
            cmd,
            cwd=str(workdir),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
        )


def _require_process_running(
    proc: subprocess.Popen[str],
    *,
    label: str,
    log_path: Path,
) -> None:
    if proc.poll() is None:
        return
    raise AssertionError(
        f"{label} exited unexpectedly with code {proc.returncode}.\n"
        f"log_path={log_path}\n"
        f"output=\n{_read_text_or_empty(log_path)}"
    )


def _terminate_process(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=10.0)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10.0)


def _wait_for_tcp(
    host: str,
    port: int,
    *,
    label: str,
    proc: subprocess.Popen[str],
    log_path: Path,
) -> None:
    deadline = time.monotonic() + INTEGRATION_READY_TIMEOUT_SECS
    while time.monotonic() < deadline:
        _require_process_running(proc, label=label, log_path=log_path)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.2)
            if sock.connect_ex((host, port)) == 0:
                return
        time.sleep(0.1)
    raise AssertionError(
        f"timed out waiting for {label} on {host}:{port}\n"
        f"log_path={log_path}\n"
        f"output=\n{_read_text_or_empty(log_path)}"
    )


def _wait_for_path(
    path: Path,
    *,
    label: str,
    proc: subprocess.Popen[str],
    log_path: Path,
) -> None:
    deadline = time.monotonic() + INTEGRATION_READY_TIMEOUT_SECS
    while time.monotonic() < deadline:
        _require_process_running(proc, label=label, log_path=log_path)
        if path.exists():
            return
        time.sleep(0.5)
    raise AssertionError(
        f"{label} did not create required path: {path}\n"
        f"log_path={log_path}\n"
        f"output=\n{_read_text_or_empty(log_path)}"
    )


def _wait_for_log_text(
    log_path: Path,
    pattern: str,
    *,
    label: str,
    proc: subprocess.Popen[str],
) -> None:
    deadline = time.monotonic() + INTEGRATION_READY_TIMEOUT_SECS
    while time.monotonic() < deadline:
        _require_process_running(proc, label=label, log_path=log_path)
        if pattern in _read_text_or_empty(log_path):
            return
        time.sleep(0.5)
    raise AssertionError(
        f"{label} did not report readiness marker={pattern!r}\n"
        f"log_path={log_path}\n"
        f"output=\n{_read_text_or_empty(log_path)}"
    )


_HTTP_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _http_status(*, url: str, headers: dict[str, str]) -> int:
    request = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with _HTTP_OPENER.open(request, timeout=3.0) as response:
            return int(response.status)
    except urllib.error.HTTPError as err:
        return int(err.code)


def _wait_for_http_status(
    *,
    url: str,
    accepted_statuses: tuple[int, ...],
    headers: dict[str, str],
    label: str,
    proc: subprocess.Popen[str],
    log_path: Path,
) -> None:
    deadline = time.monotonic() + INTEGRATION_READY_TIMEOUT_SECS
    last_status: int | None = None
    while time.monotonic() < deadline:
        _require_process_running(proc, label=label, log_path=log_path)
        try:
            last_status = _http_status(url=url, headers=headers)
            if last_status in accepted_statuses:
                return
        except OSError:
            pass
        time.sleep(0.5)
    raise AssertionError(
        f"{label} HTTP did not reach accepted_statuses={accepted_statuses} at {url}; "
        f"last_status={last_status}\n"
        f"log_path={log_path}\n"
        f"output=\n{_read_text_or_empty(log_path)}"
    )


def _write_yaml(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(value, sort_keys=False), encoding="utf-8")


def _basic_auth_headers(username: str, password: str) -> dict[str, str]:
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {token}"}


def _etcd_start_script() -> Path:
    start_script = REPO_ROOT / "fluxon_release" / "ext_images" / "etcd" / "start.sh"
    if not start_script.is_file():
        raise FileNotFoundError(
            "missing etcd ext runtime. Run "
            "`python3 setup_and_pack/pack_release_ext.py --release-dir fluxon_release` first."
        )
    return start_script


class _EtcdHarness:
    def __init__(self, *, work_root: Path) -> None:
        self._work_root = work_root
        self._work_root.mkdir(parents=True, exist_ok=False)
        self._client_port = _pick_free_port()
        self._peer_port = _pick_free_port()
        self._endpoint = f"127.0.0.1:{self._client_port}"
        self._config_path = self._work_root / "etcd_config.sh"
        self._log_path = self._work_root / "etcd.log"
        self._config_path.write_text(
            "\n".join(
                [
                    "declare -a ETCD_ARGS=(",
                    '  --data-dir "$WORKDIR/etcd-data"',
                    "  --name etcd0",
                    f'  --advertise-client-urls "http://127.0.0.1:{self._client_port}"',
                    f'  --listen-client-urls "http://127.0.0.1:{self._client_port}"',
                    f'  --listen-peer-urls "http://127.0.0.1:{self._peer_port}"',
                    f'  --initial-advertise-peer-urls "http://127.0.0.1:{self._peer_port}"',
                    f'  --initial-cluster "etcd0=http://127.0.0.1:{self._peer_port}"',
                    '  --initial-cluster-token "fluxon-fs-s3-test"',
                    '  --initial-cluster-state "new"',
                    "  --auto-compaction-retention=1",
                    ")",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        self._stdout = self._log_path.open("w", encoding="utf-8")
        self._proc = subprocess.Popen(
            [
                str(_etcd_start_script()),
                "--config",
                str(self._config_path),
                "--workdir",
                str(self._work_root),
            ],
            stdin=subprocess.DEVNULL,
            stdout=self._stdout,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            _wait_for_tcp(
                "127.0.0.1",
                self._client_port,
                label="etcd-server",
                proc=self._proc,
                log_path=self._log_path,
            )
        except Exception:
            self.close()
            raise

    @property
    def endpoint(self) -> str:
        return self._endpoint

    def close(self) -> None:
        proc = getattr(self, "_proc", None)
        if proc is not None:
            _terminate_process(proc)
        stdout = getattr(self, "_stdout", None)
        if stdout is not None:
            stdout.close()


class _MonitoringRequestHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")

    def do_POST(self) -> None:  # noqa: N802
        _ = self.rfile.read(int(self.headers.get("Content-Length", "0") or "0"))
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, format: str, *args: Any) -> None:
        return


class _DummyMonitoringHarness:
    def __init__(self) -> None:
        self.port = _pick_free_port()
        self._server = http.server.ThreadingHTTPServer(
            ("127.0.0.1", self.port),
            _MonitoringRequestHandler,
        )
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="fluxon_fs_s3_test_monitor",
            daemon=True,
        )
        self._thread.start()

    @property
    def prometheus_base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}/v1/prometheus"

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=10.0)


class FluxonFsS3Harness:
    def __init__(
        self,
        *,
        tag: str,
        work_root: Path,
        export_root: Path,
    ) -> None:
        self._tag = tag
        self._work_root = work_root
        self._export_root = export_root.resolve()
        if not self._export_root.is_dir():
            raise ValueError(f"export_root must be an existing directory: {self._export_root}")
        self._work_root.mkdir(parents=True, exist_ok=False)
        self._env = _build_subprocess_env()
        self._processes: list[subprocess.Popen[str]] = []
        self._admin_username = "admin"
        self._admin_password = "admin-pass-123"
        self._export_name = "src"
        self._cluster_name = f"ffs3-{int(time.time() * 1000):x}-{os.getpid():x}"
        self._ui_port = _pick_free_port()
        self._kv_master_port = _pick_free_port()
        self._ui_base_url = f"http://127.0.0.1:{self._ui_port}"
        self._fs_s3_base_url = f"{self._ui_base_url}/fs_s3"
        self._share_mem_root = self._work_root / "share_mem"
        self._share_mem_root.mkdir(parents=True, exist_ok=True)
        self._etcd: _EtcdHarness | None = None
        self._monitor: _DummyMonitoringHarness | None = None
        try:
            self._etcd = _EtcdHarness(work_root=self._work_root / "etcd")
            self._monitor = _DummyMonitoringHarness()
            self._prepare_configs()
            self._start_stack()
        except Exception:
            self.close()
            raise

    @property
    def s3_endpoint(self) -> str:
        return self._fs_s3_base_url

    @property
    def s3_access_key(self) -> str:
        return self._admin_username

    @property
    def s3_secret_key(self) -> str:
        return self._admin_password

    @property
    def source_export_name(self) -> str:
        return self._export_name

    def _monitoring_block(self) -> dict[str, str]:
        if self._monitor is None:
            raise RuntimeError("monitoring harness is not initialized")
        return {"prometheus_base_url": self._monitor.prometheus_base_url}

    def _owner_config(self) -> dict[str, Any]:
        if self._etcd is None:
            raise RuntimeError("etcd harness is not initialized")
        return {
            "instance_key": f"{self._tag}_owner",
            "contribute_to_cluster_pool_size": {
                "dram": 1024 * 1024 * 1024,
                "vram": {},
            },
            "fluxonkv_spec": {
                "etcd_addresses": [self._etcd.endpoint],
                "cluster_name": self._cluster_name,
                "share_mem_path": str(self._share_mem_root),
                "sub_cluster": "s3_test_owner",
                "large_file_paths": [str(self._work_root / "large" / "owner")],
            },
            "test_spec_config": {"disable_observability": True},
        }

    def _external_config(self, *, instance_key: str) -> dict[str, Any]:
        return {
            "instance_key": instance_key,
            "fluxonkv_spec": {
                "cluster_name": self._cluster_name,
                "share_mem_path": str(self._share_mem_root),
            },
            "test_spec_config": {"disable_observability": True},
        }

    def _prepare_configs(self) -> None:
        if self._etcd is None:
            raise RuntimeError("etcd harness is not initialized")
        self._owner_workdir = self._work_root / "owner"
        self._owner_config_path = self._owner_workdir / "config.yaml"
        self._kv_master_workdir = self._work_root / "kv_master"
        self._kv_master_config_path = self._kv_master_workdir / "config.yaml"
        self._fs_master_workdir = self._work_root / "fs_master"
        self._fs_master_config_path = self._fs_master_workdir / "config.yaml"
        self._fs_agent_workdir = self._work_root / "fs_agent"
        self._fs_agent_config_path = self._fs_agent_workdir / "config.yaml"

        _write_yaml(self._owner_config_path, self._owner_config())
        _write_yaml(
            self._kv_master_config_path,
            {
                "instance_key": f"{self._tag}_kv_master",
                "cluster_name": self._cluster_name,
                "port": self._kv_master_port,
                "etcd_endpoints": [self._etcd.endpoint],
                "log_dir": str((self._kv_master_workdir / "logs").resolve()),
                "monitoring": self._monitoring_block(),
                "test_spec_config": {"disable_observability": True},
            },
        )
        _write_yaml(
            self._fs_master_config_path,
            {
                "kvclient": self._external_config(
                    instance_key=f"{self._tag}_fs_master"
                ),
                "fluxon_fs": {
                    "master": {
                        "instance_key": f"{self._tag}_fs_master",
                        "pull_interval_ms": 1000,
                    },
                    "master_panel": {
                        "listen_addr": f"127.0.0.1:{self._ui_port}",
                        "public_base_url": self._ui_base_url,
                        "auto_refresh_interval_secs": 2,
                        "access_db_path": str(
                            (self._fs_master_workdir / "access.db").resolve()
                        ),
                        "bootstrap_access_model": {
                            "users": [
                                {
                                    "username": self._admin_username,
                                    "password": self._admin_password,
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
                            self._export_name: {
                                "remote_root_dir_abs": str(self._export_root),
                                "cache_max_bytes": 1024 * 1024 * 1024,
                            }
                        },
                    },
                },
            },
        )
        _write_yaml(
            self._fs_agent_config_path,
            {
                "kvclient": self._external_config(
                    instance_key=f"{self._tag}_fs_agent"
                ),
                "fluxon_fs": {
                    "master": {"instance_key": f"{self._tag}_fs_master"},
                    "cache": {
                        "stale_window_ms": 1000,
                        "rules": [],
                        "exports": {
                            self._export_name: {
                                "remote_root_dir_abs": str(self._export_root),
                                "cache_max_bytes": 1024 * 1024 * 1024,
                            }
                        },
                    },
                },
            },
        )
        self._owner_shared_json_path = (
            self._share_mem_root / self._cluster_name / "shared.json"
        )

    def _start_logged_process(
        self,
        *,
        label: str,
        cmd: list[str],
        workdir: Path,
    ) -> tuple[subprocess.Popen[str], Path]:
        log_path = workdir / f"{label}.log"
        proc = _spawn_logged(
            cmd=cmd,
            workdir=workdir,
            log_path=log_path,
            env=self._env,
        )
        self._processes.append(proc)
        return proc, log_path

    def _start_stack(self) -> None:
        kv_master_proc, kv_master_log = self._start_logged_process(
            label="kv_master",
            cmd=[
                sys.executable,
                "-m",
                "fluxon_py.runtime.start_master",
                "--config",
                str(self._kv_master_config_path),
                "--workdir",
                str(self._kv_master_workdir),
            ],
            workdir=self._kv_master_workdir,
        )
        _wait_for_log_text(
            kv_master_log,
            "KV Master started successfully",
            label="kv-master",
            proc=kv_master_proc,
        )

        owner_proc, owner_log = self._start_logged_process(
            label="owner",
            cmd=[
                sys.executable,
                "-m",
                "fluxon_py.runtime.start_owner_kvclient",
                "--config",
                str(self._owner_config_path),
                "--workdir",
                str(self._owner_workdir),
            ],
            workdir=self._owner_workdir,
        )
        _wait_for_path(
            self._owner_shared_json_path,
            label="owner-shared-json",
            proc=owner_proc,
            log_path=owner_log,
        )

        fs_master_proc, fs_master_log = self._start_logged_process(
            label="fs_master",
            cmd=[
                sys.executable,
                "-m",
                "fluxon_py.fluxon_fs.master_cli",
                "--config",
                str(self._fs_master_config_path),
                "--workdir",
                str(self._fs_master_workdir),
            ],
            workdir=self._fs_master_workdir,
        )
        _wait_for_tcp(
            "127.0.0.1",
            self._ui_port,
            label="fs-master-http",
            proc=fs_master_proc,
            log_path=fs_master_log,
        )
        _wait_for_http_status(
            url=f"{self._fs_s3_base_url}/ui/",
            accepted_statuses=(200,),
            headers=_basic_auth_headers(
                self._admin_username,
                self._admin_password,
            ),
            label="fs-master-ui-home",
            proc=fs_master_proc,
            log_path=fs_master_log,
        )

        fs_agent_proc, fs_agent_log = self._start_logged_process(
            label="fs_agent",
            cmd=[
                sys.executable,
                "-m",
                "fluxon_py.fluxon_fs.agent_cli",
                "--config",
                str(self._fs_agent_config_path),
                "--workdir",
                str(self._fs_agent_workdir),
            ],
            workdir=self._fs_agent_workdir,
        )
        _wait_for_log_text(
            fs_agent_log,
            "fluxon_fs agent ready",
            label="fs-agent",
            proc=fs_agent_proc,
        )

    def close(self) -> None:
        for proc in reversed(self._processes):
            _terminate_process(proc)
        self._processes.clear()
        if self._monitor is not None:
            self._monitor.close()
            self._monitor = None
        if self._etcd is not None:
            self._etcd.close()
            self._etcd = None
