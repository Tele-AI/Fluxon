#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import pprint
import resource
import shutil
import signal
import socket
import subprocess
import sys
import time
import traceback
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, BinaryIO, Iterable

import yaml

from _common import REPO_ROOT


TEST_REQUIREMENTS = ["fluxon-release"]

DEFAULT_WORKDIR = REPO_ROOT / ".dever" / "ci_large_scale_mq"
DEFAULT_RELEASE_DIR = REPO_ROOT / "fluxon_release"
COORDINATOR_SOURCE = REPO_ROOT / "fluxon_test_stack" / "distributed_benchmark_coordinator.py"
NODE_SOURCE = REPO_ROOT / "fluxon_test_stack" / "distributed_benchmark_node.py"
RUNTIME_SOURCE_NAMES = (
    "benchmark_node_fs.py",
    "benchmark_node_kv.py",
    "benchmark_node_mq.py",
    "benchmark_node_rpc.py",
    "benchmark_role_names.py",
    "distributed_benchmark_coordinator.py",
    "distributed_benchmark_node.py",
    "mpmc_readiness.py",
)

PORT_MIN = 20000
PORT_MAX = 32767
OWNER_SUB_CLUSTER = "owner"
MQ_CAPACITY = 40
MQ_TTL_SECONDS = 90
START_IDLE_SECONDS = 10.0
POST_RESULT_WORKER_EXIT_TIMEOUT_SECONDS = 600.0
PROCESS_STOP_GRACE_SECONDS = 30.0
PROCESS_TERM_GRACE_SECONDS = 10.0
RESOURCE_SAMPLE_INTERVAL_SECONDS = 30.0
LOG_TAIL_LINES = 100
REQUIRED_NOFILE_LIMIT = 65536


@dataclass(frozen=True)
class PortPlan:
    etcd_client: int
    etcd_peer: int
    greptime_http: int
    master: int
    coordinator: int
    owners: list[int]
    workers: list[int]


@dataclass
class ManagedProcess:
    name: str
    kind: str
    command: list[str]
    process: subprocess.Popen[bytes]
    log_path: Path
    log_stream: BinaryIO
    started_at_unix_s: float

    def snapshot(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": self.kind,
            "pid": self.process.pid,
            "returncode": self.process.poll(),
            "command": self.command,
            "log_path": str(self.log_path),
            "started_at_unix_s": self.started_at_unix_s,
        }


class ProcessRegistry:
    def __init__(self, *, workdir: Path, child_env: dict[str, str]) -> None:
        self.workdir = workdir
        self.child_env = child_env
        self.records: list[ManagedProcess] = []
        self.status_path = workdir / "processes.json"

    def start(
        self,
        *,
        name: str,
        kind: str,
        command: list[str],
        cwd: Path,
        log_path: Path,
    ) -> ManagedProcess:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_stream = log_path.open("ab", buffering=0)
        print("+ " + " ".join(command), flush=True)
        try:
            process = subprocess.Popen(
                command,
                cwd=str(cwd),
                env=self.child_env,
                stdin=subprocess.DEVNULL,
                stdout=log_stream,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except BaseException:
            log_stream.close()
            raise
        record = ManagedProcess(
            name=name,
            kind=kind,
            command=list(command),
            process=process,
            log_path=log_path,
            log_stream=log_stream,
            started_at_unix_s=time.time(),
        )
        self.records.append(record)
        self.write_status()
        return record

    def write_status(self) -> None:
        _write_json_atomic(
            self.status_path,
            {
                "schema_version": 1,
                "updated_at_unix_s": time.time(),
                "processes": [record.snapshot() for record in self.records],
            },
        )

    def alive(self, *, kinds: set[str] | None = None) -> list[ManagedProcess]:
        return [
            record
            for record in self.records
            if record.process.poll() is None and (kinds is None or record.kind in kinds)
        ]

    def assert_alive(self, records: Iterable[ManagedProcess], *, context: str) -> None:
        exited = [
            f"{record.name}(rc={record.process.poll()})"
            for record in records
            if record.process.poll() is not None
        ]
        if exited:
            self.write_status()
            raise RuntimeError(f"{context}: required process exited: {', '.join(exited)}")

    def stop_all(self) -> None:
        stop_groups = (
            [record for record in reversed(self.records) if record.kind == "worker"],
            [record for record in reversed(self.records) if record.kind == "coordinator"],
            [record for record in reversed(self.records) if record.kind == "owner"],
            [
                record
                for record in reversed(self.records)
                if record.name == "master"
            ],
            [
                record
                for record in reversed(self.records)
                if record.name == "greptime"
            ],
            [
                record
                for record in reversed(self.records)
                if record.name == "etcd"
            ],
        )
        for records in stop_groups:
            self._signal_and_wait(records, signal.SIGINT, PROCESS_STOP_GRACE_SECONDS)
            self._signal_and_wait(records, signal.SIGTERM, PROCESS_TERM_GRACE_SECONDS)
            self._signal_and_wait(records, signal.SIGKILL, 5.0)
        self.write_status()
        for record in self.records:
            record.log_stream.close()

    @staticmethod
    def _signal_and_wait(
        records: list[ManagedProcess],
        sig: signal.Signals,
        timeout_seconds: float,
    ) -> None:
        alive = [record for record in records if record.process.poll() is None]
        if not alive:
            return
        for record in alive:
            try:
                os.killpg(record.process.pid, sig)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if all(record.process.poll() is not None for record in alive):
                return
            time.sleep(0.2)


def _write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".tmp")
    tmp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tmp_path.replace(path)


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )


def _write_benchmark_config(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "from __future__ import annotations\n"
        "from typing import Any, Dict\n\n"
        f"CONFIG: Dict[str, Any] = {pprint.pformat(payload, width=120, sort_dicts=False)}\n",
        encoding="utf-8",
    )


def _busy_tcp_ports() -> set[int]:
    ports: set[int] = set()
    for proc_path in (Path("/proc/net/tcp"), Path("/proc/net/tcp6")):
        try:
            lines = proc_path.read_text(encoding="utf-8").splitlines()[1:]
        except FileNotFoundError:
            continue
        for line in lines:
            parts = line.split()
            if len(parts) < 2:
                continue
            try:
                ports.add(int(parts[1].rsplit(":", 1)[1], 16))
            except ValueError:
                continue
    return ports


def _find_tcp_port_block(
    *,
    preferred_start: int,
    required_count: int,
    busy_ports: set[int] | None = None,
) -> int:
    if required_count <= 0:
        raise ValueError("required_count must be positive")
    max_start = PORT_MAX - required_count + 1
    if max_start < PORT_MIN:
        raise ValueError(
            f"requested local port block is too large: count={required_count} range={PORT_MIN}-{PORT_MAX}"
        )
    busy = _busy_tcp_ports() if busy_ports is None else set(busy_ports)
    preferred = min(max(int(preferred_start), PORT_MIN), max_start)
    for start in (*range(preferred, max_start + 1), *range(PORT_MIN, preferred)):
        if all(port not in busy for port in range(start, start + required_count)):
            return start
    raise RuntimeError(
        f"no free local TCP port block: count={required_count} range={PORT_MIN}-{PORT_MAX}"
    )


def _allocate_port_plan(
    *,
    workdir: Path,
    owner_count: int,
    worker_count: int,
    busy_ports: set[int] | None = None,
) -> PortPlan:
    required_count = 5 + owner_count + worker_count
    search_span = PORT_MAX - PORT_MIN - required_count + 2
    if search_span <= 0:
        raise ValueError(f"process topology needs too many local ports: {required_count}")
    digest = hashlib.sha256(str(workdir.resolve()).encode("utf-8")).digest()
    preferred_start = PORT_MIN + int.from_bytes(digest[:4], "big") % search_span
    base = _find_tcp_port_block(
        preferred_start=preferred_start,
        required_count=required_count,
        busy_ports=busy_ports,
    )
    cursor = base
    etcd_client = cursor
    cursor += 1
    etcd_peer = cursor
    cursor += 1
    greptime_http = cursor
    cursor += 1
    master = cursor
    cursor += 1
    coordinator = cursor
    cursor += 1
    owners = list(range(cursor, cursor + owner_count))
    cursor += owner_count
    workers = list(range(cursor, cursor + worker_count))
    return PortPlan(
        etcd_client=etcd_client,
        etcd_peer=etcd_peer,
        greptime_http=greptime_http,
        master=master,
        coordinator=coordinator,
        owners=owners,
        workers=workers,
    )


def _host_ipv4_addresses() -> list[str]:
    candidates: set[str] = {"127.0.0.1"}
    try:
        infos = socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET)
    except socket.gaierror:
        infos = []
    for info in infos:
        raw = info[4][0]
        try:
            address = ipaddress.ip_address(raw)
        except ValueError:
            continue
        if isinstance(address, ipaddress.IPv4Address):
            candidates.add(str(address))
    return sorted(candidates, key=lambda value: (value == "127.0.0.1", value))


def _runtime_test_spec() -> dict[str, bool]:
    return {
        "disable_observability": True,
        "disable_master_replica_cache": True,
        "disable_prefix_index": True,
    }


def _validate_args(args: argparse.Namespace) -> None:
    positive_fields = (
        "owner_count",
        "owner_dram_gib",
        "producer_count",
        "consumer_count",
        "duration_seconds",
        "threads_per_process",
        "op_timeout_seconds",
        "cluster_ready_timeout_seconds",
    )
    for field_name in positive_fields:
        value = int(getattr(args, field_name))
        if value <= 0:
            raise ValueError(f"--{field_name.replace('_', '-')} must be > 0")
    if int(args.value_size) < 0:
        raise ValueError("--value-size must be >= 0")
    if int(args.metric_warmup_seconds) < 0:
        raise ValueError("--metric-warmup-seconds must be >= 0")
    if int(args.duration_seconds) - int(args.metric_warmup_seconds) < 30:
        raise ValueError("duration-seconds minus metric-warmup-seconds must be at least 30")
    if int(args.op_timeout_seconds) > int(args.duration_seconds):
        raise ValueError("--op-timeout-seconds cannot exceed --duration-seconds")
    if int(args.consumer_sim_min_ms) < 0:
        raise ValueError("--consumer-sim-min-ms must be >= 0")
    if int(args.consumer_sim_max_ms) < int(args.consumer_sim_min_ms):
        raise ValueError("--consumer-sim-max-ms must be >= --consumer-sim-min-ms")


def _build_runtime_artifacts(
    *,
    args: argparse.Namespace,
    workdir: Path,
    ports: PortPlan,
    host_ips: list[str],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    worker_count = int(args.producer_count) + int(args.consumer_count)
    if len(ports.workers) != worker_count:
        raise ValueError("worker port plan does not match requested topology")
    if len(ports.owners) != int(args.owner_count):
        raise ValueError("owner port plan does not match requested topology")

    scope = hashlib.sha256(str(workdir.resolve()).encode("utf-8")).hexdigest()[:12]
    cluster_name = f"fluxon_largescale_{scope}"
    runtime_prefix = f"largescale_{scope}"
    etcd_address = f"127.0.0.1:{ports.etcd_client}"
    greptime_origin = f"http://127.0.0.1:{ports.greptime_http}"
    share_root = (workdir / "services" / "share_mem").resolve()
    result_path = (workdir / "benchmark_result.json").resolve()
    owner_dram_bytes = int(args.owner_dram_gib) * 1024 * 1024 * 1024
    test_spec = _runtime_test_spec()

    owner_configs: list[dict[str, Any]] = []
    owner_roots: list[Path] = []
    for owner_index, owner_port in enumerate(ports.owners):
        owner_root = (share_root / f"owner_{owner_index}").resolve()
        owner_roots.append(owner_root)
        owner_workdir = (workdir / "services" / f"owner_{owner_index}").resolve()
        owner_configs.append(
            {
                "instance_key": f"{runtime_prefix}__owner_{owner_index}",
                "contribute_to_cluster_pool_size": {
                    "dram": owner_dram_bytes,
                    "vram": {},
                },
                "fluxonkv_spec": {
                    "etcd_addresses": [etcd_address],
                    "cluster_name": cluster_name,
                    "share_mem_path": str(owner_root),
                    "large_file_paths": [str((owner_workdir / "large").resolve())],
                    "sub_cluster": OWNER_SUB_CLUSTER,
                    "p2p_listen_port": owner_port,
                },
                "test_spec_config": dict(test_spec),
            }
        )

    master_config = {
        "instance_key": f"{runtime_prefix}__master",
        "cluster_name": cluster_name,
        "port": ports.master,
        "etcd_endpoints": [etcd_address],
        "network": {
            "subnet_whitelist": [f"{address}/32" for address in host_ips],
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
        "log_dir": str((workdir / "services" / "master_logs").resolve()),
        "test_spec_config": dict(test_spec),
    }

    node_roles: list[str] = []
    node_overrides: list[dict[str, Any]] = []
    worker_specs: list[dict[str, Any]] = []
    global_index = 0
    for role, role_count in (
        ("producer", int(args.producer_count)),
        ("consumer", int(args.consumer_count)),
    ):
        for role_index in range(role_count):
            owner_index = global_index % int(args.owner_count)
            instance_key = f"{runtime_prefix}__{role}_{role_index:03d}"
            p2p_port = ports.workers[global_index]
            node_roles.append(role)
            node_overrides.append(
                {
                    "kv": {
                        "instance_key": instance_key,
                        "fluxonkv_spec": {
                            "cluster_name": cluster_name,
                            "share_mem_path": str(owner_roots[owner_index]),
                            "p2p_listen_port": p2p_port,
                        },
                    },
                    "mq_role": role,
                    "mq": {"weight": 1.0},
                    "network_sample": {
                        "target": host_ips[0],
                        "leader": global_index == 0,
                    },
                }
            )
            worker_specs.append(
                {
                    "instance_key": instance_key,
                    "role": role,
                    "role_index": role_index,
                    "owner_index": owner_index,
                    "p2p_listen_port": p2p_port,
                }
            )
            global_index += 1

    benchmark_config = {
        "benchmark": {
            "mode": "MPMC",
            "workload_id": "largescale_mq",
            "threads_per_process": int(args.threads_per_process),
            "max_benchmark_seconds": int(args.duration_seconds),
            "cluster_ready_timeout_seconds": int(args.cluster_ready_timeout_seconds),
            "metric_warmup_seconds": float(args.metric_warmup_seconds),
            "start_idle_seconds": START_IDLE_SECONDS,
            "op_timeout_seconds": float(args.op_timeout_seconds),
            "value_size": int(args.value_size),
            "value_size_mode": "FIXED",
            "value_size_list": [],
            "node_roles": node_roles,
            "consumer_sim_handle_ms_range": [
                int(args.consumer_sim_min_ms),
                int(args.consumer_sim_max_ms),
            ],
        },
        "kv_base": {
            "instance_key": f"{runtime_prefix}__benchmark_base",
            "contribute_to_cluster_pool_size": {"dram": 0, "vram": {}},
            "fluxonkv_spec": {
                "cluster_name": cluster_name,
                "share_mem_path": str(owner_roots[0]),
            },
            "test_spec_config": dict(test_spec),
        },
        "mq_base": {
            "capacity": MQ_CAPACITY,
            "ttl_seconds": MQ_TTL_SECONDS,
        },
        "mq_new_or_bind_unique_key": f"{runtime_prefix}__mpmc",
        "node_overrides": node_overrides,
        "coordinator": {"port": ports.coordinator},
        "output": {"result_path": str(result_path)},
    }

    plan = {
        "schema_version": 1,
        "execution_model": "bare_local_processes",
        "uses_testbed": False,
        "workdir": str(workdir),
        "cluster_name": cluster_name,
        "host_ipv4_addresses": host_ips,
        "ports": asdict(ports),
        "topology": {
            "owner_count": int(args.owner_count),
            "owner_dram_bytes": owner_dram_bytes,
            "producer_count": int(args.producer_count),
            "consumer_count": int(args.consumer_count),
            "worker_count": worker_count,
            "threads_per_process": int(args.threads_per_process),
        },
        "workers": worker_specs,
        "paths": {
            "benchmark_config": str((workdir / "benchmark_config.py").resolve()),
            "benchmark_result": str(result_path),
            "master_config": str((workdir / "configs" / "master.yaml").resolve()),
            "owner_configs": [
                str((workdir / "configs" / f"owner_{index}.yaml").resolve())
                for index in range(int(args.owner_count))
            ],
        },
    }
    return plan, benchmark_config, master_config, owner_configs


def _materialize_runtime(
    *,
    workdir: Path,
    plan: dict[str, Any],
    benchmark_config: dict[str, Any],
    master_config: dict[str, Any],
    owner_configs: list[dict[str, Any]],
) -> dict[str, Path]:
    _write_json_atomic(workdir / "run_plan.json", plan)
    _write_benchmark_config(workdir / "benchmark_config.py", benchmark_config)
    master_path = workdir / "configs" / "master.yaml"
    _write_yaml(master_path, master_config)
    owner_paths: list[Path] = []
    for index, owner_config in enumerate(owner_configs):
        owner_path = workdir / "configs" / f"owner_{index}.yaml"
        _write_yaml(owner_path, owner_config)
        owner_paths.append(owner_path)
        owner_root = Path(owner_config["fluxonkv_spec"]["share_mem_path"])
        owner_root.mkdir(parents=True, exist_ok=True)
        Path(owner_config["fluxonkv_spec"]["large_file_paths"][0]).mkdir(
            parents=True,
            exist_ok=True,
        )
    Path(master_config["log_dir"]).mkdir(parents=True, exist_ok=True)

    runtime_package = workdir / "runtime" / "fluxon_test_stack"
    runtime_package.mkdir(parents=True, exist_ok=True)
    source_root = REPO_ROOT / "fluxon_test_stack"
    for source_name in RUNTIME_SOURCE_NAMES:
        source_path = source_root / source_name
        if not source_path.is_file():
            raise FileNotFoundError(f"benchmark runtime source is missing: {source_path}")
        shutil.copy2(source_path, runtime_package / source_name)

    return {
        "master_config": master_path,
        "coordinator_script": runtime_package / COORDINATOR_SOURCE.name,
        "node_script": runtime_package / NODE_SOURCE.name,
        **{
            f"owner_config_{index}": owner_path
            for index, owner_path in enumerate(owner_paths)
        },
    }


def _prepare_new_workdir(workdir: Path) -> None:
    if workdir.exists() and any(workdir.iterdir()):
        raise RuntimeError(
            f"workdir is not empty: {workdir}; run --action clean before a new bare-local run"
        )
    workdir.mkdir(parents=True, exist_ok=True)


def _clean_workdir(workdir: Path) -> None:
    status_path = workdir / "processes.json"
    process_groups: list[int] = []
    if status_path.is_file():
        try:
            payload = json.loads(status_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise RuntimeError(f"cannot parse process registry before clean: {status_path}: {exc}") from exc
        for raw in reversed(payload.get("processes", [])):
            if not isinstance(raw, dict) or not isinstance(raw.get("pid"), int):
                continue
            pid = int(raw["pid"])
            proc_root = Path("/proc") / str(pid)
            try:
                cwd = (proc_root / "cwd").resolve(strict=True)
                pgid = os.getpgid(pid)
            except (FileNotFoundError, ProcessLookupError):
                continue
            if cwd != workdir and workdir not in cwd.parents:
                raise RuntimeError(
                    f"refusing to stop recorded pid outside workdir: pid={pid} cwd={cwd} workdir={workdir}"
                )
            if pgid != pid:
                raise RuntimeError(f"refusing to signal non-leader recorded pid: pid={pid} pgid={pgid}")
            process_groups.append(pid)
        for pgid in process_groups:
            try:
                os.killpg(pgid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + PROCESS_TERM_GRACE_SECONDS
        while time.monotonic() < deadline:
            if all(not _process_group_exists(pgid) for pgid in process_groups):
                break
            time.sleep(0.2)
        for pgid in process_groups:
            if not _process_group_exists(pgid):
                continue
            try:
                os.killpg(pgid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    if workdir.exists():
        shutil.rmtree(workdir)


def _process_group_exists(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _remove_large_runtime_data(workdir: Path) -> None:
    errors: list[str] = []
    for mmap_path in (workdir / "services").rglob("mmap.file"):
        try:
            mmap_path.unlink(missing_ok=True)
        except OSError as exc:
            errors.append(f"{mmap_path}: {type(exc).__name__}: {exc}")
    for data_root in (
        workdir / "services" / "etcd" / "data",
        workdir / "services" / "greptime" / "data",
    ):
        try:
            if data_root.exists():
                shutil.rmtree(data_root)
        except OSError as exc:
            errors.append(f"{data_root}: {type(exc).__name__}: {exc}")
    for large_root in (workdir / "services").glob("owner_*/large"):
        try:
            if large_root.exists():
                shutil.rmtree(large_root)
        except OSError as exc:
            errors.append(f"{large_root}: {type(exc).__name__}: {exc}")
    if errors:
        _write_json_atomic(
            workdir / "cleanup_errors.json",
            {
                "schema_version": 1,
                "errors": errors,
                "timestamp_unix_s": time.time(),
            },
        )
        print(f"[bare-large-scale] cleanup errors: {errors}", flush=True)


def _child_environment() -> dict[str, str]:
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env.pop("PYTHONHOME", None)
    env["RUST_BACKTRACE"] = "1"
    env["RUST_LIB_BACKTRACE"] = "1"
    env.setdefault("RUST_LOG", "info")
    env.setdefault("FLUXON_LOG", "info")
    return env


def _ensure_nofile_limit() -> None:
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if hard != resource.RLIM_INFINITY and hard < REQUIRED_NOFILE_LIMIT:
        raise RuntimeError(
            "large-scale MQ requires a higher file-descriptor hard limit: "
            f"required={REQUIRED_NOFILE_LIMIT} soft={soft} hard={hard}"
        )
    if soft < REQUIRED_NOFILE_LIMIT:
        resource.setrlimit(
            resource.RLIMIT_NOFILE,
            (REQUIRED_NOFILE_LIMIT, hard),
        )
    effective_soft, effective_hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    print(
        "[bare-large-scale] nofile limit: "
        f"soft={effective_soft} hard={effective_hard}",
        flush=True,
    )


def _validate_release(release_dir: Path, python: str, workdir: Path, child_env: dict[str, str]) -> dict[str, Path]:
    binaries = {
        "etcd": release_dir / "ext_images" / "etcd" / "etcd",
        "etcdctl": release_dir / "ext_images" / "etcd" / "etcdctl",
        "greptime": release_dir / "ext_images" / "greptime" / "greptime",
    }
    for name, path in binaries.items():
        if not path.is_file() or not os.access(path, os.X_OK):
            raise RuntimeError(f"release binary is missing or not executable: {name}={path}")
    probe = subprocess.run(
        [
            python,
            "-c",
            (
                "import json, pathlib, fluxon_py, fluxon_pyo3; "
                "print(json.dumps({"
                "'fluxon_py': str(pathlib.Path(fluxon_py.__file__).resolve()), "
                "'fluxon_pyo3': str(pathlib.Path(fluxon_pyo3.__file__).resolve())"
                "}))"
            ),
        ],
        cwd=str(workdir),
        env=child_env,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=30,
    )
    if probe.returncode != 0:
        raise RuntimeError(
            "selected Python cannot import the packaged Fluxon wheel: "
            f"python={python} output={probe.stdout.strip()}"
        )
    try:
        imported = json.loads(probe.stdout.strip().splitlines()[-1])
        imported_path = Path(imported["fluxon_py"]).resolve()
        imported_pyo3_path = Path(imported["fluxon_pyo3"]).resolve()
    except (IndexError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"cannot parse packaged Fluxon import probe: output={probe.stdout.strip()}"
        ) from exc
    checkout_package_roots = (
        (REPO_ROOT / "fluxon_py").resolve(),
        (REPO_ROOT / "src" / "fluxon_py").resolve(),
    )
    if any(
        imported_path == package_root or package_root in imported_path.parents
        for package_root in checkout_package_roots
    ):
        raise RuntimeError(
            "bare-local runtime resolved fluxon_py from the checkout instead of the packaged wheel: "
            f"{imported_path}"
        )
    print(
        "[bare-large-scale] packaged runtime: "
        f"fluxon_py={imported_path} fluxon_pyo3={imported_pyo3_path}",
        flush=True,
    )
    return binaries


def _validate_materialized_benchmark_runtime(
    *,
    python: str,
    runtime_dir: Path,
    workdir: Path,
    child_env: dict[str, str],
) -> None:
    node_script = runtime_dir / "distributed_benchmark_node.py"
    if not node_script.is_file():
        raise RuntimeError(f"materialized benchmark node is missing: {node_script}")
    probe = subprocess.run(
        [
            python,
            "-I",
            "-c",
            (
                "import pathlib, sys; "
                "runtime_dir = pathlib.Path(sys.argv[1]).resolve(); "
                "sys.path.insert(0, str(runtime_dir)); "
                "import distributed_benchmark_node; "
                "print(distributed_benchmark_node.__file__)"
            ),
            str(runtime_dir),
        ],
        cwd=str(workdir),
        env=child_env,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=60,
    )
    if probe.returncode != 0:
        raise RuntimeError(
            "materialized benchmark runtime import failed: "
            f"python={python} output={probe.stdout.strip()}"
        )
    print(
        "[bare-large-scale] materialized benchmark runtime import passed: "
        f"{probe.stdout.strip().splitlines()[-1]}",
        flush=True,
    )


def _wait_tcp_ready(
    *,
    host: str,
    port: int,
    timeout_seconds: float,
    registry: ProcessRegistry,
    required: Iterable[ManagedProcess],
    context: str,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error = ""
    while time.monotonic() < deadline:
        registry.assert_alive(required, context=context)
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        time.sleep(0.5)
    raise TimeoutError(f"{context} did not listen on {host}:{port}: last_error={last_error}")


def _wait_etcd_ready(
    *,
    etcdctl: Path,
    port: int,
    timeout_seconds: float,
    registry: ProcessRegistry,
    process: ManagedProcess,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    endpoint = f"http://127.0.0.1:{port}"
    last_output = ""
    while time.monotonic() < deadline:
        registry.assert_alive([process], context="etcd readiness")
        probe = subprocess.run(
            [str(etcdctl), "--endpoints", endpoint, "endpoint", "health"],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=5,
        )
        last_output = probe.stdout.strip()
        if probe.returncode == 0:
            return
        time.sleep(0.5)
    raise TimeoutError(f"etcd did not become healthy: endpoint={endpoint} output={last_output}")


def _owner_bundle_path(owner_config: dict[str, Any], cluster_name: str) -> Path:
    return Path(owner_config["fluxonkv_spec"]["share_mem_path"]) / cluster_name


def _wait_owner_bundles(
    *,
    owner_configs: list[dict[str, Any]],
    cluster_name: str,
    timeout_seconds: float,
    registry: ProcessRegistry,
    required: Iterable[ManagedProcess],
) -> None:
    deadline = time.monotonic() + timeout_seconds
    pending = set(range(len(owner_configs)))
    last_errors: dict[int, str] = {}
    while pending and time.monotonic() < deadline:
        registry.assert_alive(required, context="owner shared bundle readiness")
        for owner_index in list(pending):
            bundle_dir = _owner_bundle_path(owner_configs[owner_index], cluster_name)
            shared_path = bundle_dir / "shared.json"
            mmap_path = bundle_dir / "mmap.file"
            try:
                if not mmap_path.is_file():
                    raise FileNotFoundError(f"missing {mmap_path}")
                payload = json.loads(shared_path.read_text(encoding="utf-8"))
                if not isinstance(payload, dict):
                    raise ValueError("shared.json is not an object")
                if payload.get("cluster_name") != cluster_name:
                    raise ValueError(
                        f"cluster_name mismatch: {payload.get('cluster_name')!r} != {cluster_name!r}"
                    )
                endpoints = payload.get("etcd_addresses")
                if not isinstance(endpoints, list) or not endpoints:
                    raise ValueError("shared.json has no etcd_addresses")
                if int(payload.get("segment_len", 0)) <= 0:
                    raise ValueError("shared.json segment_len is not positive")
            except Exception as exc:
                last_errors[owner_index] = f"{type(exc).__name__}: {exc}"
                continue
            pending.remove(owner_index)
            print(
                f"[bare-large-scale] owner ready: index={owner_index} bundle={bundle_dir}",
                flush=True,
            )
        if pending:
            time.sleep(0.5)
    if pending:
        raise TimeoutError(
            "owner shared bundles did not become ready: "
            f"pending={sorted(pending)} last_errors={last_errors}"
        )


def _read_proc_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8").strip()
    except (FileNotFoundError, PermissionError, OSError):
        return None


def _resource_snapshot(registry: ProcessRegistry) -> dict[str, Any]:
    meminfo: dict[str, str] = {}
    raw_meminfo = _read_proc_text(Path("/proc/meminfo"))
    if raw_meminfo:
        for line in raw_meminfo.splitlines():
            key, separator, value = line.partition(":")
            if separator and key in {"MemTotal", "MemAvailable", "SwapTotal", "SwapFree"}:
                meminfo[key] = value.strip()
    return {
        "timestamp_unix_s": time.time(),
        "loadavg": _read_proc_text(Path("/proc/loadavg")),
        "meminfo": meminfo,
        "cgroup": {
            "memory.current": _read_proc_text(Path("/sys/fs/cgroup/memory.current")),
            "memory.events": _read_proc_text(Path("/sys/fs/cgroup/memory.events")),
            "memory.pressure": _read_proc_text(Path("/sys/fs/cgroup/memory.pressure")),
            "cpu.pressure": _read_proc_text(Path("/sys/fs/cgroup/cpu.pressure")),
            "pids.current": _read_proc_text(Path("/sys/fs/cgroup/pids.current")),
        },
        "tracked_processes": len(registry.records),
        "tracked_alive": len(registry.alive()),
        "workers_alive": len(registry.alive(kinds={"worker"})),
        "benchmark_progress": _benchmark_progress_snapshot(registry),
    }


def _benchmark_progress_snapshot(registry: ProcessRegistry) -> dict[str, Any]:
    coordinator_log = registry.workdir / "logs" / "coordinator.log"
    try:
        raw = coordinator_log.read_bytes()
    except (FileNotFoundError, OSError):
        raw = b""
    workers = [record for record in registry.records if record.kind == "worker"]
    return {
        "registered": raw.count("节点注册成功:".encode("utf-8")),
        "ready": raw.count("节点就绪:".encode("utf-8")),
        "runtime_ready": raw.count("MPMC runtime ready:".encode("utf-8")),
        "reported_result": raw.count("的测试结果".encode("utf-8")),
        "worker_total": len(workers),
        "worker_exited": sum(record.process.poll() is not None for record in workers),
        "worker_nonzero": sum(
            record.process.poll() not in (None, 0)
            for record in workers
        ),
        "result_file_present": (registry.workdir / "benchmark_result.json").is_file(),
    }


def _append_resource_snapshot(workdir: Path, registry: ProcessRegistry) -> None:
    snapshot = _resource_snapshot(registry)
    print("[bare-large-scale resource] " + json.dumps(snapshot, sort_keys=True), flush=True)
    with (workdir / "resource_samples.jsonl").open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(snapshot, sort_keys=True) + "\n")


def _load_complete_result(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None
    if not isinstance(payload, dict):
        return None
    runs = payload.get("runs")
    if not isinstance(runs, list) or not runs:
        return None
    for run in runs:
        if not isinstance(run, dict):
            return None
        completion = run.get("completion")
        if not isinstance(completion, dict) or not isinstance(completion.get("status"), str):
            return None
    return payload


def _validate_benchmark_result(result: dict[str, Any], *, expected_nodes: int) -> None:
    runs = result.get("runs")
    if not isinstance(runs, list) or not runs:
        raise ValueError("benchmark_result.runs must be a non-empty list")
    failures: list[str] = []
    for run_index, raw_run in enumerate(runs):
        if not isinstance(raw_run, dict):
            raise ValueError(f"benchmark_result.runs[{run_index}] must be an object")
        completion = raw_run.get("completion")
        if not isinstance(completion, dict):
            raise ValueError(f"benchmark_result.runs[{run_index}].completion must be an object")
        observed = {
            "status": completion.get("status"),
            "expected_nodes": completion.get("expected_nodes"),
            "registered_node_count": completion.get("registered_node_count"),
            "ready_node_count": completion.get("ready_node_count"),
            "runtime_ready_node_count": completion.get("runtime_ready_node_count"),
            "reported_result_node_count": completion.get("reported_result_node_count"),
            "pending_result_node_count": completion.get("pending_result_node_count"),
            "completed": raw_run.get("completed", True),
            "total_ops": raw_run.get("total_ops"),
            "total_successful_ops": raw_run.get("total_successful_ops"),
            "total_failed_ops": raw_run.get("total_failed_ops"),
            "completion_error": completion.get("completion_error"),
        }
        valid = (
            observed["status"] == "SUCCESS"
            and observed["expected_nodes"] == expected_nodes
            and observed["registered_node_count"] == expected_nodes
            and observed["ready_node_count"] == expected_nodes
            and observed["runtime_ready_node_count"] == expected_nodes
            and observed["reported_result_node_count"] == expected_nodes
            and observed["pending_result_node_count"] == 0
            and observed["completed"] is True
            and isinstance(observed["total_ops"], int)
            and observed["total_ops"] > 0
            and isinstance(observed["total_successful_ops"], int)
            and observed["total_successful_ops"] > 0
            and isinstance(observed["total_failed_ops"], int)
            and not isinstance(observed["total_failed_ops"], bool)
            and observed["total_failed_ops"] == 0
        )
        if not valid:
            failures.append(f"run[{run_index}]={observed}")
    if failures:
        raise ValueError("large-scale MQ did not complete on every worker: " + "; ".join(failures))


def _wait_for_result(
    *,
    result_path: Path,
    timeout_seconds: float,
    registry: ProcessRegistry,
    critical: list[ManagedProcess],
    workers: list[ManagedProcess],
    workdir: Path,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    next_resource_sample = 0.0
    while time.monotonic() < deadline:
        result = _load_complete_result(result_path)
        if result is not None:
            return result
        registry.assert_alive(critical, context="benchmark result wait")
        registry.assert_alive(workers, context="benchmark result wait")
        now = time.monotonic()
        if now >= next_resource_sample:
            _append_resource_snapshot(workdir, registry)
            registry.write_status()
            next_resource_sample = now + RESOURCE_SAMPLE_INTERVAL_SECONDS
        time.sleep(0.5)
    raise TimeoutError(
        f"benchmark_result.json did not become complete within {timeout_seconds:.1f}s: {result_path}"
    )


def _wait_workers_exit(
    *,
    workers: list[ManagedProcess],
    critical: list[ManagedProcess],
    registry: ProcessRegistry,
    workdir: Path,
) -> None:
    deadline = time.monotonic() + POST_RESULT_WORKER_EXIT_TIMEOUT_SECONDS
    next_resource_sample = 0.0
    while time.monotonic() < deadline:
        registry.assert_alive(critical, context="post-result worker shutdown")
        nonzero = [
            f"{record.name}(rc={record.process.poll()})"
            for record in workers
            if record.process.poll() not in (None, 0)
        ]
        if nonzero:
            raise RuntimeError("workers exited nonzero after reporting result: " + ", ".join(nonzero))
        alive = [record for record in workers if record.process.poll() is None]
        if not alive:
            registry.write_status()
            return
        now = time.monotonic()
        if now >= next_resource_sample:
            print(
                f"[bare-large-scale] waiting for worker shutdown: alive={len(alive)}/{len(workers)}",
                flush=True,
            )
            _append_resource_snapshot(workdir, registry)
            next_resource_sample = now + RESOURCE_SAMPLE_INTERVAL_SECONDS
        time.sleep(0.5)
    alive_names = [record.name for record in workers if record.process.poll() is None]
    raise TimeoutError(
        "workers did not finish their normal close path after benchmark result: "
        f"alive_count={len(alive_names)} alive={alive_names}"
    )


def _tail_text(path: Path, line_count: int = LOG_TAIL_LINES) -> str:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except FileNotFoundError:
        return f"missing log: {path}"
    return "\n".join(lines[-line_count:])


def _print_failure_tails(registry: ProcessRegistry) -> None:
    selected: list[ManagedProcess] = []
    selected.extend(record for record in registry.records if record.kind != "worker")
    failed_workers = [
        record
        for record in registry.records
        if record.kind == "worker" and record.process.poll() not in (None, 0)
    ]
    selected.extend(failed_workers[:10])
    if not failed_workers:
        selected.extend(
            [record for record in registry.records if record.kind == "worker"][:4]
        )
    seen: set[str] = set()
    for record in selected:
        if record.name in seen:
            continue
        seen.add(record.name)
        print(
            f"=== {record.name} rc={record.process.poll()} log={record.log_path} tail ===",
            flush=True,
        )
        print(_tail_text(record.log_path), flush=True)
    registered_logs = {record.log_path.resolve() for record in registry.records}
    nested_logs = [
        path
        for path in sorted((registry.workdir / "services").rglob("*.log"))
        if path.resolve() not in registered_logs
    ]
    for path in nested_logs[:20]:
        print(f"=== nested service log={path} tail ===", flush=True)
        print(_tail_text(path), flush=True)


def _install_signal_handlers() -> dict[signal.Signals, Any]:
    previous: dict[signal.Signals, Any] = {}

    def _raise_interrupt(signum: int, _frame: Any) -> None:
        raise KeyboardInterrupt(f"received signal {signum}")

    for sig in (signal.SIGINT, signal.SIGTERM):
        previous[sig] = signal.getsignal(sig)
        signal.signal(sig, _raise_interrupt)
    return previous


def _restore_signal_handlers(previous: dict[signal.Signals, Any]) -> None:
    for sig, handler in previous.items():
        signal.signal(sig, handler)


def _run_bare_local(args: argparse.Namespace) -> int:
    _validate_args(args)
    workdir = Path(args.workdir).expanduser().resolve()
    release_dir = Path(args.release_dir).expanduser().resolve()
    _prepare_new_workdir(workdir)

    worker_count = int(args.producer_count) + int(args.consumer_count)
    ports = _allocate_port_plan(
        workdir=workdir,
        owner_count=int(args.owner_count),
        worker_count=worker_count,
    )
    host_ips = _host_ipv4_addresses()
    plan, benchmark_config, master_config, owner_configs = _build_runtime_artifacts(
        args=args,
        workdir=workdir,
        ports=ports,
        host_ips=host_ips,
    )
    runtime_paths = _materialize_runtime(
        workdir=workdir,
        plan=plan,
        benchmark_config=benchmark_config,
        master_config=master_config,
        owner_configs=owner_configs,
    )
    print(
        "[bare-large-scale] plan materialized: "
        f"owners={args.owner_count} producers={args.producer_count} "
        f"consumers={args.consumer_count} workdir={workdir}",
        flush=True,
    )
    if args.plan_only:
        return 0

    child_env = _child_environment()
    registry = ProcessRegistry(workdir=workdir, child_env=child_env)
    previous_handlers = _install_signal_handlers()
    failure: BaseException | None = None
    exit_code = 1
    started_at = time.time()
    try:
        _ensure_nofile_limit()
        binaries = _validate_release(release_dir, args.python, workdir, child_env)
        _validate_materialized_benchmark_runtime(
            python=args.python,
            runtime_dir=runtime_paths["node_script"].parent,
            workdir=workdir,
            child_env=child_env,
        )
        etcd_workdir = workdir / "services" / "etcd"
        etcd_workdir.mkdir(parents=True, exist_ok=True)
        etcd = registry.start(
            name="etcd",
            kind="service",
            command=[
                str(binaries["etcd"]),
                "--name",
                "largescale-etcd",
                "--data-dir",
                str((etcd_workdir / "data").resolve()),
                "--listen-client-urls",
                f"http://127.0.0.1:{ports.etcd_client}",
                "--advertise-client-urls",
                f"http://127.0.0.1:{ports.etcd_client}",
                "--listen-peer-urls",
                f"http://127.0.0.1:{ports.etcd_peer}",
                "--initial-advertise-peer-urls",
                f"http://127.0.0.1:{ports.etcd_peer}",
                "--initial-cluster",
                f"largescale-etcd=http://127.0.0.1:{ports.etcd_peer}",
                "--initial-cluster-state",
                "new",
                "--initial-cluster-token",
                plan["cluster_name"],
                "--auto-compaction-mode",
                "periodic",
                "--auto-compaction-retention",
                "1h",
                "--log-level",
                "info",
            ],
            cwd=etcd_workdir,
            log_path=workdir / "logs" / "etcd.log",
        )
        _wait_etcd_ready(
            etcdctl=binaries["etcdctl"],
            port=ports.etcd_client,
            timeout_seconds=60,
            registry=registry,
            process=etcd,
        )

        greptime_workdir = workdir / "services" / "greptime"
        greptime_workdir.mkdir(parents=True, exist_ok=True)
        greptime = registry.start(
            name="greptime",
            kind="service",
            command=[
                str(binaries["greptime"]),
                "standalone",
                "start",
                "--data-home",
                str((greptime_workdir / "data").resolve()),
                "--http-addr",
                f"127.0.0.1:{ports.greptime_http}",
            ],
            cwd=greptime_workdir,
            log_path=workdir / "logs" / "greptime.log",
        )
        _wait_tcp_ready(
            host="127.0.0.1",
            port=ports.greptime_http,
            timeout_seconds=120,
            registry=registry,
            required=[greptime],
            context="greptime readiness",
        )

        master_workdir = workdir / "services" / "master"
        master_workdir.mkdir(parents=True, exist_ok=True)
        master = registry.start(
            name="master",
            kind="service",
            command=[
                args.python,
                "-u",
                "-m",
                "fluxon_py.runtime.start_master",
                "-c",
                str(runtime_paths["master_config"]),
                "-w",
                str(master_workdir),
            ],
            cwd=workdir,
            log_path=workdir / "logs" / "master.log",
        )
        _wait_tcp_ready(
            host="127.0.0.1",
            port=ports.master,
            timeout_seconds=120,
            registry=registry,
            required=[etcd, greptime, master],
            context="master readiness",
        )
        time.sleep(2.0)
        registry.assert_alive([master], context="master post-readiness stability")

        owners: list[ManagedProcess] = []
        for owner_index in range(int(args.owner_count)):
            owner_workdir = workdir / "services" / f"owner_{owner_index}"
            owner_workdir.mkdir(parents=True, exist_ok=True)
            owners.append(
                registry.start(
                    name=f"owner_{owner_index}",
                    kind="owner",
                    command=[
                        args.python,
                        "-u",
                        "-m",
                        "fluxon_py.runtime.start_owner_kvclient",
                        "-c",
                        str(runtime_paths[f"owner_config_{owner_index}"]),
                        "-w",
                        str(owner_workdir),
                    ],
                    cwd=workdir,
                    log_path=workdir / "logs" / f"owner_{owner_index}.log",
                )
            )
            time.sleep(0.1)
        _wait_owner_bundles(
            owner_configs=owner_configs,
            cluster_name=plan["cluster_name"],
            timeout_seconds=float(args.cluster_ready_timeout_seconds),
            registry=registry,
            required=[etcd, greptime, master, *owners],
        )

        coordinator = registry.start(
            name="coordinator",
            kind="coordinator",
            command=[
                args.python,
                "-u",
                str(runtime_paths["coordinator_script"]),
            ],
            cwd=workdir,
            log_path=workdir / "logs" / "coordinator.log",
        )
        critical = [etcd, greptime, master, *owners, coordinator]
        _wait_tcp_ready(
            host="127.0.0.1",
            port=ports.coordinator,
            timeout_seconds=120,
            registry=registry,
            required=critical,
            context="coordinator readiness",
        )

        workers: list[ManagedProcess] = []
        for worker_index, worker_spec in enumerate(plan["workers"]):
            instance_key = str(worker_spec["instance_key"])
            role = str(worker_spec["role"])
            role_index = int(worker_spec["role_index"])
            workers.append(
                registry.start(
                    name=instance_key,
                    kind="worker",
                    command=[
                        args.python,
                        "-u",
                        str(runtime_paths["node_script"]),
                        "--instance-key",
                        instance_key,
                        "--coordinator",
                        f"127.0.0.1:{ports.coordinator}",
                    ],
                    cwd=workdir,
                    log_path=workdir / "logs" / "workers" / f"{role}_{role_index:03d}.log",
                )
            )
            if worker_index % 16 == 15 or worker_index + 1 == worker_count:
                print(
                    f"[bare-large-scale] workers started: {worker_index + 1}/{worker_count}",
                    flush=True,
                )
            time.sleep(0.05)

        result_timeout_seconds = (
            float(args.duration_seconds)
            + float(args.metric_warmup_seconds)
            + float(args.cluster_ready_timeout_seconds) * 2.0
            + 600.0
        )
        result = _wait_for_result(
            result_path=workdir / "benchmark_result.json",
            timeout_seconds=result_timeout_seconds,
            registry=registry,
            critical=critical,
            workers=workers,
            workdir=workdir,
        )
        _validate_benchmark_result(result, expected_nodes=worker_count)
        print(
            f"[bare-large-scale] benchmark result validated for all {worker_count} workers",
            flush=True,
        )
        _wait_workers_exit(
            workers=workers,
            critical=critical,
            registry=registry,
            workdir=workdir,
        )
        summary = {
            "schema_version": 1,
            "outcome": "SUCCESS",
            "started_at_unix_s": started_at,
            "finished_at_unix_s": time.time(),
            "expected_workers": worker_count,
            "normal_worker_exits": sum(record.process.poll() == 0 for record in workers),
            "benchmark_result": result,
        }
        _write_json_atomic(workdir / "summary.json", summary)
        print(
            f"[bare-large-scale] SUCCESS: all {worker_count} workers reported and exited normally",
            flush=True,
        )
        exit_code = 0
    except KeyboardInterrupt as exc:
        failure = exc
        exit_code = 130
    except BaseException as exc:
        failure = exc
        exit_code = 1
    finally:
        if failure is not None:
            _write_json_atomic(
                workdir / "failure.json",
                {
                    "schema_version": 1,
                    "error_type": type(failure).__name__,
                    "error": str(failure),
                    "traceback": "".join(
                        traceback.format_exception(type(failure), failure, failure.__traceback__)
                    ),
                    "timestamp_unix_s": time.time(),
                },
            )
            print(
                f"[bare-large-scale] FAILED: {type(failure).__name__}: {failure}",
                flush=True,
            )
            _print_failure_tails(registry)
        registry.stop_all()
        _remove_large_runtime_data(workdir)
        _restore_signal_handlers(previous_handlers)
    return exit_code


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the large-scale MPMC benchmark as direct local processes. "
            "This entrypoint does not use testbed, ops, ci_2_virt_node, or test_runner."
        )
    )
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--release-dir", default=str(DEFAULT_RELEASE_DIR))
    parser.add_argument("--workdir", default=str(DEFAULT_WORKDIR))
    parser.add_argument("--action", choices=("run", "clean"), default="run")
    parser.add_argument(
        "--plan-only",
        action="store_true",
        help="Materialize the direct-process plan and runtime configs without starting processes.",
    )
    parser.add_argument("--owner-count", type=int, default=4)
    parser.add_argument("--owner-dram-gib", type=int, default=1)
    parser.add_argument("--producer-count", type=int, default=160)
    parser.add_argument("--consumer-count", type=int, default=8)
    parser.add_argument("--threads-per-process", type=int, default=1)
    parser.add_argument("--duration-seconds", type=int, default=90)
    parser.add_argument("--metric-warmup-seconds", type=int, default=60)
    parser.add_argument("--value-size", type=int, default=256)
    parser.add_argument("--op-timeout-seconds", type=int, default=5)
    parser.add_argument("--cluster-ready-timeout-seconds", type=int, default=1800)
    parser.add_argument("--consumer-sim-min-ms", type=int, default=1)
    parser.add_argument("--consumer-sim-max-ms", type=int, default=1)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    workdir = Path(args.workdir).expanduser().resolve()
    if args.action == "clean":
        _clean_workdir(workdir)
        return 0
    return _run_bare_local(args)


if __name__ == "__main__":
    raise SystemExit(main())
