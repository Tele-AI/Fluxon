"""Integration smoke test for MQ Ctrl-C exit.

For each role:
- generate a temporary config/workdir
- materialize a dedicated producer/consumer script under the temp root
- start the script in a subprocess
- wait until the script reports ready
- send SIGINT
- assert the script prints the Ctrl-C marker and exits with code 130
"""

from __future__ import annotations

import importlib.util
import logging
import os
import select
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

import yaml


def _find_project_root(start: Path) -> Path:
    for candidate in (start,) + tuple(start.parents):
        if (candidate / "setup.py").is_file():
            return candidate
    return start


CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = _find_project_root(CURRENT_DIR)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("test_example_ctrl_c_exit")

READY_TIMEOUT_SECONDS = 60.0
EXIT_TIMEOUT_SECONDS = 60.0
INFRA_READY_TIMEOUT_SECONDS = 180.0
BLOCKING_WINDOW_SECONDS = 0.5
CHAN_CONFIG_TEST = {"capacity": 10, "ttl_seconds": 90, "weight": 1}

MASTER_SCRIPT = [sys.executable, "-m", "fluxon_py.runtime.start_master"]
KVCLIENT_SCRIPT = [sys.executable, "-m", "fluxon_py.runtime.start_owner_kvclient"]
ETCD_BIN = PROJECT_ROOT / "fluxon_release" / "ext_images" / "etcd" / "etcd"
GREPTIME_BIN = PROJECT_ROOT / "fluxon_release" / "ext_images" / "greptime" / "greptime"

MQ_PRODUCER_SCRIPT_BODY = """#!/usr/bin/env python3
import logging
import os
import signal
import threading
import time
from pathlib import Path

import yaml

from fluxon_py.api_ext_chan import ChanRole, ChanType, MPMCChanProducer, new_or_bind_with_unique_key
from fluxon_py.api_error import ProducerClosedError
from fluxon_py.config import FluxonKvClientConfig
from fluxon_py.kvclient import new_store
from fluxon_py.logging import init_logger
from fluxon_py.runtime import register_ctrlc_callback


def _must_ok(res, msg: str):
    if not res.is_ok():
        raise SystemExit(f"{msg}: {res.unwrap_error()}")
    return res.unwrap()


def _best_effort_close_result(obj, logger, role: str) -> None:
    try:
        close_res = obj.close()
    except Exception as e:
        logger.warning(f"[{role}] close raised (ignored): {e}")
        return
    if close_res.is_ok():
        _ = close_res.unwrap()
    else:
        logger.warning(f"[{role}] close error (ignored): {close_res.unwrap_error()}")


def _build_store_config(*, config_path: Path, workdir: Path) -> FluxonKvClientConfig:
    loaded = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    kvexternal_cfg = dict(loaded["kvexternal"])
    producer_cfg = dict(loaded["mpmc_demo"]["producer"])
    kvexternal_cfg["instance_key"] = str(producer_cfg["instance_key"])
    spec = dict(kvexternal_cfg["fluxonkv_spec"])
    raw_path = spec.get("share_mem_path")
    if isinstance(raw_path, str) and raw_path and not Path(raw_path).is_absolute():
        spec["share_mem_path"] = str((workdir / raw_path).resolve())
    kvexternal_cfg["fluxonkv_spec"] = spec
    return FluxonKvClientConfig(kvexternal_cfg)


def main() -> None:
    import sys

    config_path = Path(sys.argv[1]).resolve()
    workdir = Path(sys.argv[2]).resolve()
    loaded = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    demo_cfg = dict(loaded["mpmc_demo"])
    producer_cfg = dict(demo_cfg["producer"])
    logger = init_logger("test_mq_ctrlc_producer")
    shutdown_requested = threading.Event()
    interrupted = False
    shutdown_notified = False
    closed = False
    store = None
    producer = None

    try:
        store = _must_ok(new_store(_build_store_config(config_path=config_path, workdir=workdir)), "new_store failed")
        producer = _must_ok(
            new_or_bind_with_unique_key(
                store,
                {"capacity": int(demo_cfg["capacity"]), "ttl_seconds": int(demo_cfg["ttl_seconds"])},
                unique_id=str(demo_cfg["key"]),
                chan_type=ChanType.MPMC,
                chan_role=ChanRole.PRODUCER,
            ),
            "bind producer failed",
        )
        assert isinstance(producer, MPMCChanProducer)

        def _on_ctrlc(reason: str) -> None:
            nonlocal interrupted, shutdown_notified
            interrupted = True
            shutdown_requested.set()
            if shutdown_notified:
                return
            shutdown_notified = True
            logger.info(f"[producer] caught {reason}, requesting shutdown...")
            producer.request_shutdown()

        restore_signal_listener = register_ctrlc_callback(_on_ctrlc, thread_name="test-mq-producer-signal")
        logger.info(f"[producer] joined mpmc_id={producer.get_chan_id()}, key={demo_cfg['key']}")
        try:
            while not shutdown_requested.is_set():
                ts_ms = int(time.time() * 1000)
                payload = f"{producer_cfg['instance_key']}:{ts_ms}".encode("utf-8")
                put_res = producer.put_data({"payload": payload, "ts_ms": ts_ms})
                if put_res.is_ok():
                    _ = put_res.unwrap()
                else:
                    err = put_res.unwrap_error()
                    if isinstance(err, ProducerClosedError):
                        logger.info("[producer] close observed, exit loop")
                        break
                    raise SystemExit(f"put_data failed: {err}")
                if shutdown_requested.wait(float(producer_cfg["interval_seconds"])):
                    break
        finally:
            restore_signal_listener()
    finally:
        if producer is not None and not closed:
            _best_effort_close_result(producer, logger, "producer")
        if interrupted:
            logger.info("[producer] exit")
            logging.shutdown()
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(130)
        if store is not None:
            _best_effort_close_result(store, logger, "store")
        logger.info("[producer] exit")


if __name__ == "__main__":
    main()
"""

MQ_CONSUMER_SCRIPT_BODY = """#!/usr/bin/env python3
import logging
import os
import threading
import time
from pathlib import Path

import yaml

from fluxon_py.api_ext_chan import ChanRole, ChanType, MPMCChanConsumer, new_or_bind_with_unique_key
from fluxon_py.api_error import ChannelClosedError
from fluxon_py.config import FluxonKvClientConfig
from fluxon_py.kvclient import new_store
from fluxon_py.logging import init_logger
from fluxon_py.runtime import register_ctrlc_callback


def _must_ok(res, msg: str):
    if not res.is_ok():
        raise SystemExit(f"{msg}: {res.unwrap_error()}")
    return res.unwrap()


def _best_effort_close_result(obj, logger, role: str) -> None:
    try:
        close_res = obj.close()
    except Exception as e:
        logger.warning(f"[{role}] close raised (ignored): {e}")
        return
    if close_res.is_ok():
        _ = close_res.unwrap()
    else:
        logger.warning(f"[{role}] close error (ignored): {close_res.unwrap_error()}")


def _build_store_config(*, config_path: Path, workdir: Path) -> FluxonKvClientConfig:
    loaded = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    kvexternal_cfg = dict(loaded["kvexternal"])
    consumer_cfg = dict(loaded["mpmc_demo"]["consumer"])
    kvexternal_cfg["instance_key"] = str(consumer_cfg["instance_key"])
    spec = dict(kvexternal_cfg["fluxonkv_spec"])
    raw_path = spec.get("share_mem_path")
    if isinstance(raw_path, str) and raw_path and not Path(raw_path).is_absolute():
        spec["share_mem_path"] = str((workdir / raw_path).resolve())
    kvexternal_cfg["fluxonkv_spec"] = spec
    return FluxonKvClientConfig(kvexternal_cfg)


def main() -> None:
    import sys

    config_path = Path(sys.argv[1]).resolve()
    workdir = Path(sys.argv[2]).resolve()
    loaded = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    demo_cfg = dict(loaded["mpmc_demo"])
    consumer_cfg = dict(demo_cfg["consumer"])
    logger = init_logger("test_mq_ctrlc_consumer")
    shutdown_requested = threading.Event()
    interrupted = False
    shutdown_notified = False
    closed = False
    store = None
    consumer = None

    try:
        store = _must_ok(new_store(_build_store_config(config_path=config_path, workdir=workdir)), "new_store failed")
        consumer = _must_ok(
            new_or_bind_with_unique_key(
                store,
                {"capacity": int(demo_cfg["capacity"]), "ttl_seconds": int(demo_cfg["ttl_seconds"])},
                unique_id=str(demo_cfg["key"]),
                chan_type=ChanType.MPMC,
                chan_role=ChanRole.CONSUMER,
            ),
            "bind consumer failed",
        )
        assert isinstance(consumer, MPMCChanConsumer)

        def _on_ctrlc(reason: str) -> None:
            nonlocal interrupted, shutdown_notified
            interrupted = True
            shutdown_requested.set()
            if shutdown_notified:
                return
            shutdown_notified = True
            logger.info(f"[consumer] caught {reason}, requesting shutdown...")
            consumer.request_shutdown()

        restore_signal_listener = register_ctrlc_callback(_on_ctrlc, thread_name="test-mq-consumer-signal")
        logger.info(f"[consumer] joined mpmc_id={consumer.get_chan_id()}, key={demo_cfg['key']}")
        try:
            while not shutdown_requested.is_set():
                res = consumer.get_data(batch_size=int(consumer_cfg["batch"]))
                if not res.is_ok():
                    err = res.unwrap_error()
                    if isinstance(err, ChannelClosedError):
                        logger.info("[consumer] close observed, exit loop")
                        break
                    raise SystemExit(f"get_data failed: {err}")
                for raw in res.unwrap() or []:
                    payload = raw.get("payload", b"") if isinstance(raw, dict) else raw
                    if isinstance(payload, (bytes, bytearray, memoryview)):
                        logger.info(f"[consumer] got: {bytes(payload).decode('utf-8', 'ignore')}")
                    else:
                        logger.info(f"[consumer] got: {payload}")
                if shutdown_requested.wait(0.2):
                    break
        finally:
            restore_signal_listener()
    finally:
        if consumer is not None and not closed:
            _best_effort_close_result(consumer, logger, "consumer")
        if interrupted:
            logger.info("[consumer] exit")
            logging.shutdown()
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(130)
        if store is not None:
            _best_effort_close_result(store, logger, "store")
        logger.info("[consumer] exit")


if __name__ == "__main__":
    main()
"""


def _prepare_parent_environment() -> None:
    for var in (
        "http_proxy",
        "https_proxy",
        "no_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
    ):
        os.environ.pop(var, None)
    os.environ["LOG_LEVEL"] = "DEBUG"
    os.environ["FLUXON_LOG"] = "DEBUG"


def _build_subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        str(PROJECT_ROOT)
        if not existing_pythonpath
        else f"{PROJECT_ROOT}:{existing_pythonpath}"
    )
    return env


def _monitoring_block(*, greptime_http_port: int) -> dict[str, Any]:
    return {
        "prometheus_base_url": f"http://127.0.0.1:{greptime_http_port}/v1/prometheus",
        "prom_remote_write_url": [f"http://127.0.0.1:{greptime_http_port}/v1/prometheus/write"],
        "otlp_log_api": {
            "otlp_endpoint": f"http://127.0.0.1:{greptime_http_port}/v1/otlp/v1/logs",
            "db_name": "public",
            "table_name": "fluxon_logs",
        },
    }


def _pick_free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = int(s.getsockname()[1])
    s.close()
    return port


def _read_text_or_empty(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def _require_process_running(proc: subprocess.Popen[str], *, label: str, log_path: Path) -> None:
    if proc.poll() is None:
        return
    raise AssertionError(
        f"{label} exited unexpectedly with code {proc.returncode}.\n"
        f"Log path: {log_path}\n"
        f"Output:\n{_read_text_or_empty(log_path)}"
    )


def _wait_for_tcp(host: str, port: int, *, label: str, proc: subprocess.Popen[str], log_path: Path) -> None:
    deadline = time.time() + INFRA_READY_TIMEOUT_SECONDS
    while time.time() < deadline:
        _require_process_running(proc, label=label, log_path=log_path)
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1.0)
            s.connect((host, port))
            s.close()
            return
        except Exception:
            time.sleep(0.5)
    raise AssertionError(
        f"{label} TCP did not become ready on {host}:{port}.\n"
        f"Log path: {log_path}\n"
        f"Output:\n{_read_text_or_empty(log_path)}"
    )


def _wait_for_http_ok(url: str, *, label: str, proc: subprocess.Popen[str], log_path: Path) -> None:
    import urllib.request

    deadline = time.time() + INFRA_READY_TIMEOUT_SECONDS
    while time.time() < deadline:
        _require_process_running(proc, label=label, log_path=log_path)
        try:
            with urllib.request.urlopen(url, timeout=3) as r:
                if 200 <= r.status < 300:
                    return
        except Exception:
            time.sleep(0.5)
    raise AssertionError(
        f"{label} HTTP did not become ready at {url}.\n"
        f"Log path: {log_path}\n"
        f"Output:\n{_read_text_or_empty(log_path)}"
    )


def _wait_for_path(path: Path, *, label: str, proc: subprocess.Popen[str], log_path: Path) -> None:
    deadline = time.time() + INFRA_READY_TIMEOUT_SECONDS
    while time.time() < deadline:
        _require_process_running(proc, label=label, log_path=log_path)
        if path.exists():
            return
        time.sleep(1.0)
    raise AssertionError(
        f"{label} did not create required path: {path}\n"
        f"Log path: {log_path}\n"
        f"Output:\n{_read_text_or_empty(log_path)}"
    )


def _spawn_logged(
    *,
    cmd: list[str],
    workdir: Path,
    log_path: Path,
    env: dict[str, str],
) -> subprocess.Popen[str]:
    workdir.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as fh:
        proc = subprocess.Popen(
            cmd,
            cwd=str(workdir),
            env=env,
            stdout=fh,
            stderr=subprocess.STDOUT,
            text=True,
        )
    return proc


def _terminate_process(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5.0)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5.0)


def _build_example_config(
    *,
    unique_suffix: str,
    cluster_name: str,
    etcd_endpoint: str,
    share_mem_path: str,
    greptime_http_port: int,
    master_port: int,
) -> dict[str, Any]:
    capacity = max(128, int(CHAN_CONFIG_TEST["capacity"]))
    ttl_seconds = max(90, int(CHAN_CONFIG_TEST["ttl_seconds"]))
    return {
        "master": {
            "etcd_endpoints": [etcd_endpoint],
            "cluster_name": cluster_name,
            "instance_key": f"example_ctrlc_master_{unique_suffix}",
            "port": master_port,
            "log_dir": str((Path(share_mem_path).parent / "log" / "master").resolve()),
            "monitoring": _monitoring_block(greptime_http_port=greptime_http_port),
        },
        "kvclient": {
            "instance_key": f"example_ctrlc_owner_{unique_suffix}",
            "contribute_to_cluster_pool_size": {"dram": 1073741824, "vram": {}},
            "fluxonkv_spec": {
                "etcd_addresses": [etcd_endpoint],
                "cluster_name": cluster_name,
                "share_mem_path": share_mem_path,
                "sub_cluster": "demo",
                "large_file_paths": [str((Path(share_mem_path).parent / "large" / "owner").resolve())],
            },
        },
        "kvexternal": {
            "instance_key": f"example_ctrlc_base_{unique_suffix}",
            "contribute_to_cluster_pool_size": {"dram": 0, "vram": {}},
            "fluxonkv_spec": {
                "cluster_name": cluster_name,
                "share_mem_path": share_mem_path,
            },
        },
        "mpmc_demo": {
            "key": f"example_ctrlc_{unique_suffix}",
            "capacity": capacity,
            "ttl_seconds": ttl_seconds,
            "producer": {
                "interval_seconds": 0.5,
                "count": 0,
                "instance_key": f"example_ctrlc_producer_{unique_suffix}",
            },
            "consumer": {
                "batch": 1,
                "instance_key": f"example_ctrlc_consumer_{unique_suffix}",
            },
        },
    }


def _write_runtime_subconfig(*, path: Path, config: dict[str, Any], key: str) -> None:
    raw = config.get(key)
    if not isinstance(raw, dict):
        raise TypeError(f"config[{key!r}] must be a mapping, got {type(raw).__name__}")
    path.write_text(
        yaml.safe_dump(raw, sort_keys=False),
        encoding="utf-8",
    )


def _kvclient_shared_json_target(*, share_mem_path: Path, cluster_name: str) -> Path:
    return share_mem_path / cluster_name / "shared.json"


def _start_local_stack(*, temp_root: Path, config_path: Path) -> list[tuple[subprocess.Popen[str], Path]]:
    if not ETCD_BIN.is_file():
        raise FileNotFoundError(f"Missing etcd binary: {ETCD_BIN}")
    if not GREPTIME_BIN.is_file():
        raise FileNotFoundError(f"Missing greptime binary: {GREPTIME_BIN}")

    env = _build_subprocess_env()
    etcd_port = _pick_free_port()
    greptime_http_port = _pick_free_port()
    etcd_endpoint = f"127.0.0.1:{etcd_port}"
    etcd_log = temp_root / "log" / "etcd.log"
    greptime_log = temp_root / "log" / "greptime.log"
    master_log = temp_root / "log" / "master.log"
    kvclient_log = temp_root / "log" / "kvclient.log"

    greptime_proc = _spawn_logged(
        cmd=[
            str(GREPTIME_BIN),
            "standalone",
            "start",
            "--http-addr",
            f"127.0.0.1:{greptime_http_port}",
            "--rpc-bind-addr",
            "127.0.0.1:0",
            "--mysql-addr",
            "127.0.0.1:0",
            "--postgres-addr",
            "127.0.0.1:0",
            "--data-home",
            str((temp_root / "greptime-data").resolve()),
        ],
        workdir=temp_root,
        log_path=greptime_log,
        env=env,
    )
    _wait_for_tcp("127.0.0.1", greptime_http_port, label="greptime", proc=greptime_proc, log_path=greptime_log)

    etcd_proc = _spawn_logged(
        cmd=[
            str(ETCD_BIN),
            "--data-dir",
            str((temp_root / "etcd-data").resolve()),
            "--listen-client-urls",
            f"http://127.0.0.1:{etcd_port}",
            "--advertise-client-urls",
            f"http://127.0.0.1:{etcd_port}",
            "--listen-peer-urls",
            "http://127.0.0.1:0",
        ],
        workdir=temp_root,
        log_path=etcd_log,
        env=env,
    )
    _wait_for_http_ok(
        f"http://{etcd_endpoint}/health",
        label="etcd",
        proc=etcd_proc,
        log_path=etcd_log,
    )

    unique_suffix = uuid.uuid4().hex[:12]
    cluster_name = f"example_ctrlc_cluster_{unique_suffix}"
    share_mem_path = str((temp_root / "sharemem").resolve())
    master_port = _pick_free_port()
    config = _build_example_config(
        unique_suffix=unique_suffix,
        cluster_name=cluster_name,
        etcd_endpoint=etcd_endpoint,
        share_mem_path=share_mem_path,
        greptime_http_port=greptime_http_port,
        master_port=master_port,
    )
    config_path.write_text(
        yaml.safe_dump(config, sort_keys=False),
        encoding="utf-8",
    )
    master_config_path = temp_root / "master.yaml"
    kvclient_config_path = temp_root / "kvclient.yaml"
    _write_runtime_subconfig(path=master_config_path, config=config, key="master")
    _write_runtime_subconfig(path=kvclient_config_path, config=config, key="kvclient")

    master_proc = _spawn_logged(
        cmd=[
            *MASTER_SCRIPT,
            "-c",
            str(master_config_path),
            "-w",
            str((temp_root / "master_work").resolve()),
        ],
        workdir=PROJECT_ROOT,
        log_path=master_log,
        env=env,
    )
    time.sleep(2.0)
    _require_process_running(master_proc, label="master", log_path=master_log)

    kvclient_proc = _spawn_logged(
        cmd=[
            *KVCLIENT_SCRIPT,
            "-c",
            str(kvclient_config_path),
            "-w",
            str((temp_root / "kvclient_work").resolve()),
        ],
        workdir=PROJECT_ROOT,
        log_path=kvclient_log,
        env=env,
    )
    kvclient_shared_json = _kvclient_shared_json_target(
        share_mem_path=Path(str(config["kvclient"]["fluxonkv_spec"]["share_mem_path"])).resolve(),
        cluster_name=cluster_name,
    )
    _wait_for_path(
        kvclient_shared_json,
        label="kvclient shared memory",
        proc=kvclient_proc,
        log_path=kvclient_log,
    )
    return [
        (kvclient_proc, kvclient_log),
        (master_proc, master_log),
        (etcd_proc, etcd_log),
        (greptime_proc, greptime_log),
    ]


def _read_until(
    proc: subprocess.Popen[str],
    output: list[str],
    needle: str,
    *,
    timeout_seconds: float,
) -> bool:
    assert proc.stdout is not None
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if proc.poll() is not None:
            _drain_output(proc, output, timeout_seconds=0.2)
            return needle in "".join(output)

        remaining = max(0.0, deadline - time.time())
        ready, _, _ = select.select([proc.stdout], [], [], min(0.2, remaining))
        if not ready:
            continue

        line = proc.stdout.readline()
        if not line:
            continue
        output.append(line)
        if needle in line or needle in "".join(output):
            return True

    _drain_output(proc, output, timeout_seconds=0.2)
    return needle in "".join(output)


def _drain_output(
    proc: subprocess.Popen[str],
    output: list[str],
    *,
    timeout_seconds: float,
) -> None:
    assert proc.stdout is not None
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        ready, _, _ = select.select([proc.stdout], [], [], 0.05)
        if not ready:
            if proc.poll() is not None:
                break
            continue
        line = proc.stdout.readline()
        if not line:
            if proc.poll() is not None:
                break
            continue
        output.append(line)


def _drain_proc_outputs(
    proc_outputs: dict[subprocess.Popen[str], list[str]],
    *,
    timeout_seconds: float,
) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        stream_map = {
            proc.stdout.fileno(): (proc, output)
            for proc, output in proc_outputs.items()
            if proc.stdout is not None
        }
        if not stream_map:
            return

        ready, _, _ = select.select(
            [proc.stdout for proc in proc_outputs if proc.stdout is not None],
            [],
            [],
            0.05,
        )
        if not ready:
            if all(proc.poll() is not None for proc in proc_outputs):
                break
            continue

        for stream in ready:
            entry = stream_map.get(stream.fileno())
            if entry is None:
                continue
            _, output = entry
            line = stream.readline()
            if line:
                output.append(line)


def _wait_for_needles(
    proc_outputs: dict[subprocess.Popen[str], list[str]],
    proc_needles: dict[subprocess.Popen[str], str],
    *,
    timeout_seconds: float,
) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if all(needle in "".join(proc_outputs[proc]) for proc, needle in proc_needles.items()):
            return True

        for proc, needle in proc_needles.items():
            if proc.poll() is not None:
                _drain_proc_outputs(proc_outputs, timeout_seconds=0.2)
                return needle in "".join(proc_outputs[proc])

        _drain_proc_outputs(proc_outputs, timeout_seconds=0.2)

    _drain_proc_outputs(proc_outputs, timeout_seconds=0.2)
    return all(needle in "".join(proc_outputs[proc]) for proc, needle in proc_needles.items())


def _terminate_child(proc: subprocess.Popen[str], output: list[str]) -> None:
    if proc.poll() is not None:
        _drain_output(proc, output, timeout_seconds=0.2)
        return

    proc.terminate()
    try:
        proc.wait(timeout=5.0)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5.0)
    finally:
        _drain_output(proc, output, timeout_seconds=0.5)


def _assert_sigint_exit(
    proc: subprocess.Popen[str],
    output: list[str],
    *,
    label: str,
    signal_needle: str,
    other_proc_outputs: dict[subprocess.Popen[str], list[str]] | None = None,
) -> None:
    proc.send_signal(signal.SIGINT)

    exited = False
    deadline = time.time() + EXIT_TIMEOUT_SECONDS
    proc_outputs = dict(other_proc_outputs or {})
    proc_outputs[proc] = output
    while time.time() < deadline:
        _drain_proc_outputs(proc_outputs, timeout_seconds=0.2)
        if proc.poll() is not None:
            exited = True
            break
    assert exited, (
        f"{label} did not exit within {EXIT_TIMEOUT_SECONDS}s after SIGINT.\n"
        f"Output:\n{''.join(output)}"
    )

    _drain_proc_outputs(proc_outputs, timeout_seconds=0.5)
    joined = "".join(output)
    assert signal_needle in joined, (
        f"{label} did not print Ctrl-C marker {signal_needle!r}.\n"
        f"Output:\n{joined}"
    )
    assert proc.returncode == 130, (
        f"{label} should exit with code 130 after SIGINT, "
        f"got {proc.returncode}.\nOutput:\n{joined}"
    )


def _run_sigint_case(*, script_path: Path, ready_needle: str, signal_needle: str) -> None:
    with tempfile.TemporaryDirectory(prefix=f"test_example_ctrlc_{script_path.stem}_") as td:
        temp_root = Path(td)
        config_path = temp_root / "all_config.yaml"
        workdir = temp_root / "workdir"
        workdir.mkdir(parents=True, exist_ok=True)
        infra_processes = _start_local_stack(temp_root=temp_root, config_path=config_path)
        env = _build_subprocess_env()

        proc = subprocess.Popen(
            [
                sys.executable,
                str(script_path),
                str(config_path),
                str(workdir),
            ],
            cwd=str(PROJECT_ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        output: list[str] = []
        try:
            ready = _read_until(
                proc,
                output,
                ready_needle,
                timeout_seconds=READY_TIMEOUT_SECONDS,
            )
            assert ready, (
                f"{script_path.name} did not reach ready state within {READY_TIMEOUT_SECONDS}s.\n"
                f"Output:\n{''.join(output)}"
            )

            time.sleep(BLOCKING_WINDOW_SECONDS)
            _assert_sigint_exit(
                proc,
                output,
                label=script_path.name,
                signal_needle=signal_needle,
            )
        finally:
            _terminate_child(proc, output)
            for infra_proc, _ in infra_processes:
                _terminate_process(infra_proc)


def _run_consumer_sigint_after_producer_exit_case() -> None:
    with tempfile.TemporaryDirectory(prefix="test_example_ctrlc_consumer_after_producer_") as td:
        temp_root = Path(td)
        config_path = temp_root / "all_config.yaml"
        infra_processes = _start_local_stack(temp_root=temp_root, config_path=config_path)
        env = _build_subprocess_env()
        producer_script = temp_root / "start_mpmc_demo_producer.py"
        consumer_script = temp_root / "start_mpmc_demo_consumer.py"
        producer_script.write_text(MQ_PRODUCER_SCRIPT_BODY, encoding="utf-8")
        consumer_script.write_text(MQ_CONSUMER_SCRIPT_BODY, encoding="utf-8")

        producer = subprocess.Popen(
            [
                sys.executable,
                str(producer_script),
                str(config_path),
                str((temp_root / "producer_work").resolve()),
            ],
            cwd=str(PROJECT_ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        consumer = subprocess.Popen(
            [
                sys.executable,
                str(consumer_script),
                str(config_path),
                str((temp_root / "consumer_work").resolve()),
            ],
            cwd=str(PROJECT_ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        producer_output: list[str] = []
        consumer_output: list[str] = []
        proc_outputs = {
            producer: producer_output,
            consumer: consumer_output,
        }
        try:
            ready = _wait_for_needles(
                proc_outputs,
                {
                    producer: "[producer] joined mpmc_id=",
                    consumer: "[consumer] joined mpmc_id=",
                },
                timeout_seconds=READY_TIMEOUT_SECONDS,
            )
            assert ready, (
                "producer/consumer did not reach ready state within "
                f"{READY_TIMEOUT_SECONDS}s.\n"
                f"Producer output:\n{''.join(producer_output)}\n"
                f"Consumer output:\n{''.join(consumer_output)}"
            )

            time.sleep(1.0)
            _assert_sigint_exit(
                producer,
                producer_output,
                label="producer",
                signal_needle="[producer] caught Ctrl-C, requesting shutdown...",
                other_proc_outputs={consumer: consumer_output},
            )

            time.sleep(1.0)
            _assert_sigint_exit(
                consumer,
                consumer_output,
                label="consumer after producer exit",
                signal_needle="[consumer] caught Ctrl-C, requesting shutdown...",
            )
        finally:
            _terminate_child(producer, producer_output)
            _terminate_child(consumer, consumer_output)
            for infra_proc, _ in infra_processes:
                _terminate_process(infra_proc)


def test_example_ctrl_c_exit() -> bool:
    if importlib.util.find_spec("etcd3") is None:
        print("[test_example_ctrl_c_exit-skip] missing runtime dependency: etcd3")
        return False

    _prepare_parent_environment()
    LOGGER.info("running example Ctrl-C exit integration test")

    with tempfile.TemporaryDirectory(prefix="test_example_ctrlc_scripts_") as td:
        temp_root = Path(td)
        producer_script = temp_root / "start_mpmc_demo_producer.py"
        consumer_script = temp_root / "start_mpmc_demo_consumer.py"
        producer_script.write_text(MQ_PRODUCER_SCRIPT_BODY, encoding="utf-8")
        consumer_script.write_text(MQ_CONSUMER_SCRIPT_BODY, encoding="utf-8")

        _run_sigint_case(
            script_path=producer_script,
            ready_needle="[producer] joined mpmc_id=",
            signal_needle="[producer] caught Ctrl-C, requesting shutdown...",
        )
        _run_sigint_case(
            script_path=consumer_script,
            ready_needle="[consumer] joined mpmc_id=",
            signal_needle="[consumer] caught Ctrl-C, requesting shutdown...",
        )
    _run_consumer_sigint_after_producer_exit_case()
    return True


def main() -> None:
    if test_example_ctrl_c_exit():
        print("[test_example_ctrl_c_exit-ok] passed")


if __name__ == "__main__":
    main()
