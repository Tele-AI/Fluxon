"""MPMC rebind client test: producers continue while consumer restarts.

This test starts two producers and one consumer, stops the consumer, then
starts a new consumer and verifies production is uninterrupted and that
produced equals consumed at the end.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, List, Optional

# Bootstrap import path to project root so absolute imports always work
CURRENT_DIR = Path(__file__).resolve().parent

def _find_project_root(start: Path) -> Path:
    for candidate in (start,) + tuple(start.parents):
        if (candidate / "setup.py").is_file():
            return candidate
    return start

PROJECT_ROOT = _find_project_root(CURRENT_DIR)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from fluxon_py.api_ext_chan import ChanType  # noqa: E402
from fluxon_py.api_error import MessageConsumptionNoNewMessageError  # noqa: E402
from fluxon_py.logging import init_logger  # noqa: E402
from fluxon_py.tests.test_lib import (  # noqa: E402
    KV_SVC_TYPE,
    KV_SVC_IP,
    setup_test_environment,
    CHAN_CONFIG_TEST,
    new_test_producer,
    new_test_consumer,
    new_shared_stores,
    run_with_argmatrix,
    etcd_control_call_with_retry as _etcd_call_with_retry,
    etcd_control_delete_prefix as _etcd_delete_prefix,
    etcd_control_get as _etcd_get,
    etcd_control_put as _etcd_put,
)
from setup_and_pack.utils.repo_config_utils import (  # noqa: E402
    load_test_fluxon_cluster_name_from_test_config,
)
from fluxon_py.kvclient import KvClientType, new_store  # noqa: E402
from fluxon_py.kvclient.kvclient_interface import KvClient  # noqa: E402
from fluxon_py.config import FluxonKvClientConfig  # noqa: E402
from fluxon_py.tests.test_api_chan_mpmc.rebind_coordinator import (  # noqa: E402
    BarrierTarget,
    RebindRoundActions,
    WorkerProcess,
    WorkerRole,
    coordinate_rebind_rounds,
    pause_ack_key,
    ready_key,
    wait_for_barrier,
)


logging = init_logger()

SCRIPT_PATH_SELF = Path(__file__).resolve()
NEW_OR_BIND_KEY = "mpmc_rebind_client_test"
REBIND_LOOP_KEY = "/test_mpmc_rebind/loop_idx"
PRODUCER_PAUSE_KEY = "/test_mpmc_pause_producer"
LOOPS = 5  # number of consumer restart cycles
PRODUCER_MESSAGE_COUNT = 80  # per producer, should exceed total active windows
ACTIVE_WINDOW_SEC = 3  # each consumer stays active for this many seconds
INACTIVE_GAP_SEC = 1   # gap between stopping current and starting next consumer
READY_TIMEOUT_SEC = 120
PAUSE_ACK_TIMEOUT_SEC = 60
CONSUMER_EXIT_TIMEOUT_SEC = 600
PRODUCER_EXIT_TIMEOUT_SEC = 60
PROCESS_CLEANUP_GRACE_SEC = 5


def _producer_cmd(
    backend_type: str,
    ip: str,
    producer_id: str,
    message_count: int,
    session_id: str,
) -> List[str]:
    return [
        sys.executable,
        str(SCRIPT_PATH_SELF),
        "run_producer",
        "--backend_type",
        backend_type,
        "--ip",
        ip,
        "--construct_type",
        "new_or_bind",
        "--new_or_bind_key",
        NEW_OR_BIND_KEY,
        "--chan_type",
        ChanType.MPMC.value,
        "--producer_id",
        producer_id,
        "--message_count",
        str(message_count),
        "--session_id",
        session_id,
    ]


def _consumer_cmd(
    backend_type: str,
    ip: str,
    consumer_id: str,
    session_id: str,
    prefetch: int = 0,
) -> List[str]:
    return [
        sys.executable,
        str(SCRIPT_PATH_SELF),
        "run_consumer",
        "--backend_type",
        backend_type,
        "--ip",
        ip,
        "--construct_type",
        "new_or_bind",
        "--new_or_bind_key",
        NEW_OR_BIND_KEY,
        "--chan_type",
        ChanType.MPMC.value,
        "--consumer_id",
        consumer_id,
        "--prefetch",
        str(prefetch),
        "--session_id",
        session_id,
    ]

def _wait_fluxon_member_absent(instance_key: str, *, timeout_s: int = 45) -> None:
    """Wait until a fluxon cluster member key disappears from etcd.

    Purpose: avoid init failures like "Member already exists" when the previous test run
    exited abnormally and the member lease has not expired yet. Do not delete keys here;
    only wait for expiry to minimize test intrusion.
    """
    cluster = load_test_fluxon_cluster_name_from_test_config()
    key = f"/fluxon_kv_member_base/{cluster}/members/{instance_key}"
    deadline = time.time() + float(timeout_s)
    while True:
        val = _etcd_get(key)
        if val is None:
            return
        if time.time() >= deadline:
            raise RuntimeError(
                f"member key still exists after wait: {key}. Previous lease not expired"
            )
        # Progress logging is handled by the caller; keep quiet here.
        time.sleep(1.0)


# ------------------- Local CLI for subprocess workers -------------------
import argparse


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MPMC Rebind Client Test Runner")
    subparsers = parser.add_subparsers(dest="mode", help="Execution mode")

    subparsers.add_parser("main", help="Run main rebind test")

    producer_parser = subparsers.add_parser("run_producer", help="Run producer")
    producer_parser.add_argument("--backend_type", required=True, type=str)
    producer_parser.add_argument("--ip", required=True, type=str)
    producer_parser.add_argument("--construct_type", required=True, type=str)
    producer_parser.add_argument("--new_or_bind_key", required=True, type=str)
    producer_parser.add_argument("--chan_type", required=True, type=str)
    producer_parser.add_argument("--producer_id", required=True, type=str)
    producer_parser.add_argument("--message_count", required=True, type=int)
    producer_parser.add_argument("--session_id", required=True, type=str)

    consumer_parser = subparsers.add_parser("run_consumer", help="Run consumer")
    consumer_parser.add_argument("--backend_type", required=True, type=str)
    consumer_parser.add_argument("--ip", required=True, type=str)
    consumer_parser.add_argument("--construct_type", required=True, type=str)
    consumer_parser.add_argument("--new_or_bind_key", required=True, type=str)
    consumer_parser.add_argument("--chan_type", required=True, type=str)
    consumer_parser.add_argument("--consumer_id", required=True, type=str)
    consumer_parser.add_argument("--prefetch", required=False, type=int, default=0)
    consumer_parser.add_argument("--session_id", required=True, type=str)
    return parser


def _parse_args(parser: Optional[argparse.ArgumentParser] = None) -> argparse.Namespace:
    parser = parser or _build_parser()
    ns = parser.parse_args()
    if ns.mode is None:
        ns.mode = "main"
    return ns


PRODUCER_NORMAL_EXIT_MARKER = "PRODUCER_NORMAL_EXIT:"
PRODUCER_CRASH_MARKER = "PRODUCER_CRASH:"
CONSUMER_NORMAL_EXIT_MARKER = "CONSUMER_NORMAL_EXIT:"
CONSUMER_CRASH_MARKER = "CONSUMER_CRASH:"


# ------------------- Self-contained env + store helpers -------------------
class ChannelState:
    __slots__ = (
        "default_backend_type",
        "default_backend_ip",
        "backend_type",
        "backend_ip",
        "stores",
        "store_lock",
        "logger",
    )

    def __init__(self, default_backend_type: str, default_backend_ip: str) -> None:
        self.default_backend_type = default_backend_type
        self.default_backend_ip = default_backend_ip
        self.backend_type = default_backend_type
        self.backend_ip = default_backend_ip
        self.stores: dict[str, KvClient] = {}
        import threading

        self.store_lock = threading.Lock()
        self.logger = logging


def create_channel_env(
    *, backend_type: Optional[str] = None, backend_ip: Optional[str] = None
) -> ChannelState:
    return ChannelState(backend_type or KV_SVC_TYPE, backend_ip or KV_SVC_IP)


def configure_backend(
    env: ChannelState, *, backend_type: Optional[str] = None, backend_ip: Optional[str] = None
) -> None:
    target_type = backend_type if backend_type is not None else env.backend_type
    target_ip = backend_ip if backend_ip is not None else env.backend_ip
    if target_type != env.backend_type or target_ip != env.backend_ip:
        release(env)
    env.backend_type = target_type
    env.backend_ip = target_ip


def require_store(
    env: ChannelState, instance_key: str, *, backend_type: Optional[str] = None, backend_ip: Optional[str] = None
) -> KvClient:
    if backend_type is not None or backend_ip is not None:
        configure_backend(env, backend_type=backend_type, backend_ip=backend_ip)
    return _get_or_create_store(env, instance_key)


def release(env: ChannelState, *resources) -> None:
    if resources:
        targets = resources
    else:
        targets = tuple(env.stores.keys())
    with env.store_lock:
        for identifier in targets:
            name = None
            store_obj = None
            if isinstance(identifier, str):
                name = identifier
                store_obj = env.stores.pop(name, None)
            else:
                store_obj = identifier
                for k, v in list(env.stores.items()):
                    if v is store_obj:
                        name = k
                        env.stores.pop(k)
                        break
            if store_obj is None:
                continue
            try:
                res = store_obj.close()
                if res.is_ok():
                    _ = res.unwrap()
                else:
                    err = res.unwrap_error()
                    env.logger.warning("Failed to close store %s: %s", name, err)
            except Exception as exc:  # noqa: BLE001
                env.logger.warning("Failed to close store %s: %s", name, exc)


def _get_or_create_store(env: ChannelState, instance_key: str) -> KvClient:
    with env.store_lock:
        store = env.stores.get(instance_key)
        if store is None:
            store = _create_store(env, instance_key)
            env.stores[instance_key] = store
        return store


def _create_store(env: ChannelState, instance_key: str) -> KvClient:
    # Reuse the unified constructor so etcd address and related configs share the same source (tests/test_lib).
    store_list = new_shared_stores(
        instance_key,
        1,
        backend_type=env.backend_type,
        ip=env.backend_ip,
    )
    return store_list[0]


# ------------------- Local verification and cleanup -------------------
def clean_etcd() -> None:
    _etcd_delete_prefix("/mpmc_channels")
    _etcd_delete_prefix("/channels")
    _etcd_delete_prefix("/test_mpmc_stop_consumer")
    _etcd_delete_prefix("/test_mpmc_consumer")
    _etcd_delete_prefix("/test_mpmc_stop_producer")
    _etcd_delete_prefix(PRODUCER_PAUSE_KEY)
    _etcd_delete_prefix("/test_mpmc_rebind")


def _read_log_tail(path: str, *, max_lines: int = 80) -> str:
    try:
        lines = Path(path).read_text(encoding="utf-8", errors="replace").splitlines()
    except FileNotFoundError:
        return f"<missing log: {path}>"
    except OSError as exc:
        return f"<failed to read log {path}: {exc}>"
    if not lines:
        return "<empty log>"
    return "\n".join(lines[-max_lines:])


def _close_endpoint(endpoint: Any, *, label: str) -> None:
    close_result = endpoint.close()
    if not close_result.is_ok():
        raise RuntimeError(f"{label} close failed: {close_result.unwrap_error()}")
    close_result.unwrap()


def _wait_for_worker_exit(worker: WorkerProcess, *, timeout_s: float) -> None:
    try:
        return_code = worker.process.wait(timeout=timeout_s)
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"{worker.role.value} {worker.identifier} did not exit within "
            f"{timeout_s:.1f}s; log={worker.log_file}\n"
            "--- child log tail ---\n"
            f"{_read_log_tail(worker.log_file)}\n"
            "--- end child log tail ---"
        ) from exc
    if return_code != 0:
        raise RuntimeError(
            f"{worker.role.value} {worker.identifier} failed with return code "
            f"{return_code}; log={worker.log_file}\n"
            "--- child log tail ---\n"
            f"{_read_log_tail(worker.log_file)}\n"
            "--- end child log tail ---"
        )


def _cleanup_owned_workers(workers: List[WorkerProcess]) -> None:
    alive = [worker for worker in workers if worker.process.poll() is None]
    if not alive:
        return

    try:
        _etcd_put("/test_mpmc_stop_producer", b"cleanup")
    except Exception as exc:  # noqa: BLE001
        logging.warning("[RBD-CTL-CLEANUP] failed to signal producers: %s", exc)
    for worker in alive:
        if worker.role is not WorkerRole.CONSUMER:
            continue
        try:
            _etcd_put(
                f"/test_mpmc_stop_consumer/{worker.identifier}",
                b"cleanup",
            )
        except Exception as exc:  # noqa: BLE001
            logging.warning(
                "[RBD-CTL-CLEANUP] failed to signal consumer %s: %s",
                worker.identifier,
                exc,
            )

    deadline = time.monotonic() + PROCESS_CLEANUP_GRACE_SEC
    for worker in alive:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or worker.process.poll() is not None:
            continue
        try:
            worker.process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            pass

    alive = [worker for worker in workers if worker.process.poll() is None]
    for worker in alive:
        logging.warning(
            "[RBD-CTL-CLEANUP] terminating %s %s",
            worker.role.value,
            worker.identifier,
        )
        worker.process.terminate()

    deadline = time.monotonic() + PROCESS_CLEANUP_GRACE_SEC
    for worker in alive:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or worker.process.poll() is not None:
            continue
        try:
            worker.process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            pass

    for worker in workers:
        if worker.process.poll() is None:
            logging.warning(
                "[RBD-CTL-CLEANUP] killing %s %s",
                worker.role.value,
                worker.identifier,
            )
            worker.process.kill()
            worker.process.wait()


def verify_production_consumption_counts(
    subprocesses: List[WorkerProcess],
) -> None:
    print("=== Verifying Production and Consumption Counts ===")
    total_produced = 0
    total_consumed = 0
    produced_messages = set()
    consumed_messages = set()
    for worker in subprocesses:
        log_file = worker.log_file
        try:
            with open(log_file, "r", encoding="utf-8") as handle:
                for raw_line in handle:
                    line = raw_line.strip()
                    if line.startswith("PRODUCE_MARKER:"):
                        parts = line.split(": ", 1)
                        if len(parts) == 2 and ":" in parts[1]:
                            _, unique_id = parts[1].split(":", 1)
                            total_produced += 1
                            produced_messages.add(unique_id)
                    elif line.startswith("CONSUME_MARKER:"):
                        parts = line.split(": ", 1)
                        if len(parts) == 2 and ":" in parts[1]:
                            _, unique_id = parts[1].split(":", 1)
                            total_consumed += 1
                            consumed_messages.add(unique_id)
        except FileNotFoundError:
            print(f"Warning: Log file not found: {log_file}")
        except Exception as exc:  # noqa: BLE001
            print(f"Error reading log file {log_file}: {exc}")

    print(f"Total produced messages: {total_produced}")
    print(f"Total consumed messages: {total_consumed}")
    print(f"Unique produced messages: {len(produced_messages)}")
    print(f"Unique consumed messages: {len(consumed_messages)}")

    unconsumed = produced_messages - consumed_messages
    unproduced = consumed_messages - produced_messages
    assert total_produced > 0, "Total produced messages must be greater than 0"
    assert total_consumed > 0, "Total consumed messages must be greater than 0"
    if total_produced != total_consumed or len(produced_messages) != len(consumed_messages) or unconsumed or unproduced:
        raise AssertionError("Production and consumption counts do not match")
    print("✅ VERIFICATION PASSED: Production count equals consumption count")


# (Removed) per-loop minimum production verification, no longer needed when each loop drains.


def verify_exit_status(
    subprocesses: List[WorkerProcess],
) -> None:
    print("=== Verifying Exit Status ===")
    normal_exits: list[str] = []
    crashes: list[str] = []
    for worker in subprocesses:
        log_file = worker.log_file
        try:
            with open(log_file, "r", encoding="utf-8") as handle:
                content = handle.read()
            for line in content.split("\n"):
                if line.startswith(PRODUCER_NORMAL_EXIT_MARKER):
                    producer_id = line.split(": ", 1)[1]
                    normal_exits.append(f"PRODUCER_{producer_id}")
                if line.startswith(CONSUMER_NORMAL_EXIT_MARKER):
                    consumer_id = line.split(": ", 1)[1]
                    normal_exits.append(f"CONSUMER_{consumer_id}")
                if line.startswith(PRODUCER_CRASH_MARKER):
                    producer_id = line.split(": ", 1)[1]
                    crashes.append(f"PRODUCER_{producer_id}")
                if line.startswith(CONSUMER_CRASH_MARKER):
                    consumer_id = line.split(": ", 1)[1]
                    crashes.append(f"CONSUMER_{consumer_id}")
        except FileNotFoundError:
            print(f"Warning: Log file not found: {log_file}")
        except Exception as exc:  # noqa: BLE001
            print(f"Error reading log file {log_file}: {exc}")

    expected_processes = len(subprocesses)
    actual_markers = len(normal_exits) + len(crashes)
    if actual_markers != expected_processes:
        raise AssertionError(
            f"Not all processes have proper exit markers: {actual_markers}/{expected_processes}"
        )
    print("✅ EXIT STATUS VERIFICATION PASSED: All processes have exit markers")
PRODUCER_NORMAL_EXIT_MARKER = "PRODUCER_NORMAL_EXIT:"
PRODUCER_CRASH_MARKER = "PRODUCER_CRASH:"
CONSUMER_NORMAL_EXIT_MARKER = "CONSUMER_NORMAL_EXIT:"
CONSUMER_CRASH_MARKER = "CONSUMER_CRASH:"


def _chan_type_from_str(v: str) -> ChanType:
    if isinstance(v, str) and (v == ChanType.MPMC.value or v.upper() == "MPMC"):
        return ChanType.MPMC
    return ChanType.MPMC


def run_producer(env, args: argparse.Namespace) -> None:
    chan_type = _chan_type_from_str(args.chan_type)
    store_key = f"rebind_producer_{args.producer_id}"
    prev_type, prev_ip = env.backend_type, env.backend_ip
    configure_backend(env, backend_type=args.backend_type, backend_ip=args.ip)
    producer = None
    try:
        try:
            setup_test_environment(logging)
            # Keep the existing P2P settle precondition; readiness below is the
            # authoritative controller barrier.
            _wait_fluxon_member_absent(f"{store_key}_main")
            store = require_store(env, store_key)
            time.sleep(10.0)
            producer = new_test_producer(
                args.construct_type,
                store,
                None,
                CHAN_CONFIG_TEST,
                args.new_or_bind_key,
                chan_type,
            )
            producer_lease_id = int(
                producer.mpmc_channel.mpmc_global_lease.id
            )
            _etcd_put(
                ready_key(
                    args.session_id,
                    WorkerRole.PRODUCER,
                    args.producer_id,
                ),
                str(os.getpid()).encode(),
                lease_id=producer_lease_id,
            )
            logging.info(
                "[RBD-READY] Producer-%s ready session=%s",
                args.producer_id,
                args.session_id,
            )
            print(f"[Producer-{args.producer_id}] Started", flush=True)

            import random

            index = 0
            acknowledged_pause_generation = 0

            while True:
                stop_flag = _etcd_get("/test_mpmc_stop_producer")
                if stop_flag:
                    logging.info(
                        f"[RBD-STOP] Producer-{args.producer_id} stop flag detected"
                    )
                    break

                pause_poll_count = 0
                while True:
                    pause_poll_count += 1
                    pause_flag = _etcd_get(PRODUCER_PAUSE_KEY)
                    if not pause_flag:
                        logging.info(
                            f"[RBD-RESUME] Producer-{args.producer_id} resumed"
                        )
                        break

                    pause_generation_text = (
                        pause_flag.decode()
                        if isinstance(pause_flag, (bytes, bytearray))
                        else str(pause_flag)
                    )
                    if pause_generation_text.isdigit():
                        pause_generation = int(pause_generation_text)
                        if (
                            pause_generation > 0
                            and pause_generation
                            != acknowledged_pause_generation
                        ):
                            _etcd_put(
                                pause_ack_key(
                                    args.session_id,
                                    args.producer_id,
                                    pause_generation,
                                ),
                                b"paused",
                                lease_id=producer_lease_id,
                            )
                            acknowledged_pause_generation = pause_generation
                            logging.info(
                                "[RBD-PAUSE-ACK] Producer-%s generation=%s",
                                args.producer_id,
                                pause_generation,
                            )
                    logging.info(
                        "[RBD-PAUSE] Producer-%s paused, poll=%s",
                        args.producer_id,
                        pause_poll_count,
                    )
                    stop_flag = _etcd_get("/test_mpmc_stop_producer")
                    if stop_flag:
                        logging.info(
                            f"[RBD-STOP] Producer-{args.producer_id} stop while paused"
                        )
                        break
                    time.sleep(0.1)
                if stop_flag:
                    break

                try:
                    loop_val = _etcd_get(REBIND_LOOP_KEY)
                    loop_idx = int(loop_val.decode()) if loop_val else -1
                except Exception:
                    loop_idx = -1
                logging.info(
                    f"[RBD-LOOP] Producer-{args.producer_id} loop={loop_idx} idx={index}"
                )
                unique_id = str(uuid.uuid4())
                payload = (
                    f"rebind-{producer.get_chan_id()}-p{args.producer_id}-l{loop_idx}-{index}-"
                ).encode()
                msg_id = payload.decode() + unique_id
                msg = {"unique_id": msg_id, "payload": payload}
                res = producer.put_data(msg)
                if res.is_ok():
                    _ = res.unwrap()
                    logging.info(
                        f"[RBD-SEND] Producer-{args.producer_id} sent idx={index} msg={msg_id}"
                    )
                    print(
                        f"[Producer-{args.producer_id}] Sent idx {index}: {msg_id}",
                        flush=True,
                    )
                    print(f"PRODUCE_MARKER: {args.producer_id}:{msg_id}")
                    _etcd_put(
                        f"/test_mpmc_rebind/sessions/{args.session_id}/produced/"
                        f"{loop_idx}/{args.producer_id}/{unique_id}",
                        b"",
                    )
                else:
                    err = res.unwrap_error()
                    logging.info(
                        f"[RBD-ERROR] Producer-{args.producer_id} put_data error: {err}"
                    )
                    print(f"[Producer-{args.producer_id}] Error: {err}")
                    raise RuntimeError(err)
                index += 1
                time.sleep(random.uniform(0.1, 1))
        finally:
            logging.info(
                f"[RBD-FINISH] Producer-{args.producer_id} finished and closing"
            )
            if producer is not None:
                _close_endpoint(
                    producer,
                    label=f"Producer-{args.producer_id}",
                )
            release(env, store_key)
    except Exception as exc:  # noqa: BLE001
        logging.info(
            f"[RBD-ERROR] Producer-{args.producer_id} exception: {exc}"
        )
        print(f"[Producer-{args.producer_id}] Error: {exc}")
        print(f"{PRODUCER_CRASH_MARKER} {args.producer_id}", flush=True)
        raise
    else:
        print(f"[Producer-{args.producer_id}] Finished", flush=True)
        print(f"{PRODUCER_NORMAL_EXIT_MARKER} {args.producer_id}", flush=True)
    finally:
        configure_backend(env, backend_type=prev_type, backend_ip=prev_ip)


def run_consumer(env, args: argparse.Namespace) -> None:
    chan_type = _chan_type_from_str(args.chan_type)
    store_key = f"rebind_consumer_{args.consumer_id}"
    prev_type, prev_ip = env.backend_type, env.backend_ip
    configure_backend(env, backend_type=args.backend_type, backend_ip=args.ip)
    consumer = None
    consumed_count = 0
    draining = False
    try:
        try:
            setup_test_environment(logging)
            # Keep the existing P2P settle precondition; readiness below is the
            # authoritative controller barrier.
            _wait_fluxon_member_absent(f"{store_key}_main")
            store = require_store(env, store_key)
            time.sleep(10.0)
            consumer = new_test_consumer(
                args.construct_type,
                store,
                None,
                CHAN_CONFIG_TEST,
                args.new_or_bind_key,
                chan_type,
            )
            consumer_lease_id = int(
                consumer.mpmc_channel.mpmc_global_lease.id
            )
            logging.info(
                f"[RBD-INIT] Consumer-{args.consumer_id} started with member {consumer.mpmc_channel.mpmc_member_id} prefetch={int(getattr(args, 'prefetch', 0))}"
            )
            print(
                f"[Consumer-{args.consumer_id}] Started with mpmc consumer {consumer.mpmc_channel.mpmc_member_id}",
                flush=True,
            )
            _etcd_put(
                f"/test_mpmc_consumer/{args.consumer_id}",
                b"dummy_value",
                lease_id=consumer_lease_id,
            )
            _etcd_put(
                ready_key(
                    args.session_id,
                    WorkerRole.CONSUMER,
                    args.consumer_id,
                ),
                str(os.getpid()).encode(),
                lease_id=consumer_lease_id,
            )
            logging.info(
                "[RBD-READY] Consumer-%s ready session=%s",
                args.consumer_id,
                args.session_id,
            )

            import random

            consecutive_no_data = 0
            no_data_required = 10  # break as soon as one timed-out get occurs during draining
            while True:
                res = consumer.get_data(
                    batch_size=1,
                    try_time=3,
                    prefetch_num=int(getattr(args, "prefetch", 0)),
                )
                if res.is_ok():
                    success = res.unwrap()
                    if isinstance(success, list) and success:
                        msg = success[0]
                        if isinstance(msg, dict):
                            msg_id = msg["unique_id"]
                            if isinstance(msg_id, (bytes, bytearray)):
                                unique_id_str = msg_id.decode()
                            else:
                                unique_id_str = str(msg_id)
                            consumed_count += 1
                            logging.info(
                                f"[RBD-CONSUME] Consumer-{args.consumer_id} count={consumed_count} id={unique_id_str}"
                            )
                            print(
                                f"[Consumer-{args.consumer_id}] Consumed {consumed_count}: {unique_id_str}",
                                flush=True,
                            )
                            print(f"CONSUME_MARKER: {args.consumer_id}:{unique_id_str}")
                            # Track consumption per loop for gating
                            # Extract loop index from message key pattern with '-l{idx}-'
                            li = -1
                            tag = "-l"
                            pos = unique_id_str.find(tag)
                            if pos != -1:
                                end = unique_id_str.find("-", pos + len(tag))
                                if end != -1:
                                    li_str = unique_id_str[pos + len(tag) : end]
                                    if not li_str.isdigit():
                                        raise ValueError(f"Invalid loop index in message id: {unique_id_str}")
                                    li = int(li_str)
                            if li >= 0:
                                _etcd_put(
                                    f"/test_mpmc_rebind/sessions/{args.session_id}/"
                                    f"consumed/{li}/{args.consumer_id}/{unique_id_str}",
                                    b"",
                                )
                            # Random delay after each successful consumption to simulate slow processing
                            time.sleep(random.uniform(1, 10))
                            consecutive_no_data = 0
                    else:
                        # no data available
                        if draining:
                            consecutive_no_data += 1
                            if consecutive_no_data >= no_data_required:
                                logging.info(
                                    f"[RBD-DRAIN-DONE] Consumer-{args.consumer_id} no-data reached; drained"
                                )
                                # drained
                                break
                            else:
                                logging.info(
                                    f"[RBD-DRAIN-NODATA] Consumer-{args.consumer_id} no data (count={consecutive_no_data})"
                                )
                        else:
                            logging.info(
                                f"[RBD-NODATA] Consumer-{args.consumer_id} no data"
                            )
                        time.sleep(0.5)
                else:
                    err = res.unwrap_error()
                    logging.info(
                        f"[RBD-GET-ERR] Consumer-{args.consumer_id} get_data error: {err}"
                    )
                    if draining:
                        consecutive_no_data += 1
                        if consecutive_no_data >= no_data_required:
                            logging.info(
                                f"[RBD-DRAIN-DONE] Consumer-{args.consumer_id} get_data error during draining; "
                                f"treat as no-data and stop (count={consecutive_no_data})"
                            )
                            break
                    time.sleep(0.5)
                stop_flag = _etcd_get(f"/test_mpmc_stop_consumer/{args.consumer_id}")
                if stop_flag:
                    # enter draining mode: keep getting until one timeout/no-data
                    if not draining:
                        logging.info(
                            f"[RBD-DRAIN-START] Consumer-{args.consumer_id} stop flag; start draining"
                        )
                    draining = True
        finally:
            if draining:
                logging.info(
                    f"[RBD-DRAIN-END] Consumer-{args.consumer_id} drained (no more data)"
                )
            logging.info(
                f"[RBD-FINISH] Consumer-{args.consumer_id} finished, consumed={consumed_count}"
            )
            if consumer is not None:
                _close_endpoint(
                    consumer,
                    label=f"Consumer-{args.consumer_id}",
                )
            release(env, store_key)
    except Exception as exc:  # noqa: BLE001
        logging.info(
            f"[RBD-ERROR] Consumer-{args.consumer_id} exception: {exc}"
        )
        print(f"[Consumer-{args.consumer_id}] Error: {exc}")
        print(f"{CONSUMER_CRASH_MARKER} {args.consumer_id}", flush=True)
        raise
    else:
        print(
            f"[Consumer-{args.consumer_id}] Finished, consumed {consumed_count} messages",
            flush=True,
        )
        print(f"{CONSUMER_NORMAL_EXIT_MARKER} {args.consumer_id}", flush=True)
    finally:
        configure_backend(env, backend_type=prev_type, backend_ip=prev_ip)


def test_mpmc_rebind_client() -> None:
    """Run the rebind client test across the shared parameter matrix.

    The matrix is provided by tests/test_lib.py (e.g., `prefetch`).
    """

    def _once(prefetch: int) -> None:
        print(f"[rebind_client] starting test... prefetch={prefetch}", flush=True)
        env = create_channel_env()
        prev_type, prev_ip = env.backend_type, env.backend_ip
        session_id = uuid.uuid4().hex
        subprocesses: List[WorkerProcess] = []
        workers_by_identity: dict[tuple[WorkerRole, str], WorkerProcess] = {}
        configure_backend(
            env,
            backend_type=env.default_backend_type,
            backend_ip=env.default_backend_ip,
        )
        try:
            setup_test_environment(logging)
            logging.info(
                f"[RBD-CTL-INIT] start prefetch={prefetch} backend={env.backend_type} ip={env.backend_ip}"
            )
            shutil.rmtree("logs", ignore_errors=True)
            clean_etcd()
            if _etcd_get("/test_mpmc_stop_producer") is not None:
                raise RuntimeError(
                    "precondition failed: /test_mpmc_stop_producer exists before test start"
                )
            logging.info("[RBD-CTL-ETCD-CLEAN] cleared test prefixes")

            os.makedirs("logs", exist_ok=True)
            os.system("chmod -R 777 logs")
            print("[rebind_client] spawned logs/ with 777 perms", flush=True)
            logging.info("[RBD-CTL-LOGDIR] logs/ prepared with 777 perms")

            def spawn(
                role: WorkerRole,
                cmd: List[str],
                identifier: str,
            ) -> WorkerProcess:
                log_file = (
                    f"logs/mpmc_producer_{identifier}.log"
                    if role is WorkerRole.PRODUCER
                    else f"logs/mpmc_consumer_{identifier}.log"
                )
                logging.info(
                    "[RBD-CTL-SPAWN] type=%s id=%s log=%s session=%s",
                    role.value,
                    identifier,
                    log_file,
                    session_id,
                )
                with open(log_file, "w", encoding="utf-8") as log_f:
                    proc = subprocess.Popen(cmd, stdout=log_f, stderr=log_f, text=True)
                worker = WorkerProcess(role, identifier, proc, log_file)
                identity = (role, identifier)
                if identity in workers_by_identity:
                    raise RuntimeError(f"worker spawned twice: {identity!r}")
                subprocesses.append(worker)
                workers_by_identity[identity] = worker
                return worker

            def resume_producers(*, reason: str) -> None:
                _etcd_call_with_retry(
                    f"delete producer pause key {PRODUCER_PAUSE_KEY}",
                    lambda client: client.delete(PRODUCER_PAUSE_KEY),
                )
                logging.info("[RBD-CTL-RESUME] producers resumed reason=%s", reason)

            consumer_ids = tuple(f"C{index}" for index in range(LOOPS))
            _etcd_put(REBIND_LOOP_KEY, b"0")
            # Hold admission until P0, P1, and C0 have all completed endpoint setup.
            _etcd_put(PRODUCER_PAUSE_KEY, b"0")
            logging.info(
                "[RBD-CTL-ADMISSION] initial producer pause set session=%s",
                session_id,
            )

            producer_p0 = spawn(
                WorkerRole.PRODUCER,
                _producer_cmd(
                    env.backend_type,
                    env.backend_ip,
                    "P0",
                    PRODUCER_MESSAGE_COUNT,
                    session_id,
                ),
                "P0",
            )
            producer_p1 = spawn(
                WorkerRole.PRODUCER,
                _producer_cmd(
                    env.backend_type,
                    env.backend_ip,
                    "P1",
                    PRODUCER_MESSAGE_COUNT,
                    session_id,
                ),
                "P1",
            )
            consumer_c0 = spawn(
                WorkerRole.CONSUMER,
                _consumer_cmd(
                    env.backend_type,
                    env.backend_ip,
                    consumer_ids[0],
                    session_id,
                    prefetch,
                ),
                consumer_ids[0],
            )
            producer_workers = (producer_p0, producer_p1)
            wait_for_barrier(
                [
                    BarrierTarget(
                        ready_key(session_id, worker.role, worker.identifier),
                        worker,
                    )
                    for worker in (producer_p0, producer_p1, consumer_c0)
                ],
                label=f"initial worker readiness session={session_id}",
                read_key=_etcd_get,
                timeout_s=READY_TIMEOUT_SEC,
            )
            logging.info("[RBD-CTL-READY] initial workers ready session=%s", session_id)
            resume_producers(reason="initial workers ready")

            class _Actions:
                def run_active_window(
                    self,
                    round_index: int,
                    consumer_id: str,
                ) -> None:
                    logging.info(
                        "[RBD-CTL-LOOP] round=%s consumer=%s active_window=%ss",
                        round_index,
                        consumer_id,
                        ACTIVE_WINDOW_SEC,
                    )
                    time.sleep(ACTIVE_WINDOW_SEC)

                def pause_producers(self, generation: int) -> None:
                    _etcd_put(PRODUCER_PAUSE_KEY, str(generation).encode())
                    logging.info(
                        "[RBD-CTL-PAUSE] generation=%s waiting for producers",
                        generation,
                    )
                    wait_for_barrier(
                        [
                            BarrierTarget(
                                pause_ack_key(
                                    session_id,
                                    worker.identifier,
                                    generation,
                                ),
                                worker,
                            )
                            for worker in producer_workers
                        ],
                        label=(
                            f"producer pause generation={generation} "
                            f"session={session_id}"
                        ),
                        read_key=_etcd_get,
                        timeout_s=PAUSE_ACK_TIMEOUT_SEC,
                    )
                    logging.info(
                        "[RBD-CTL-PAUSE-ACK] generation=%s complete",
                        generation,
                    )

                def stop_consumer(self, consumer_id: str) -> None:
                    _etcd_put(
                        f"/test_mpmc_stop_consumer/{consumer_id}",
                        b"stop",
                    )
                    logging.info(
                        "[RBD-CTL-STOP-CONS] request stop consumer=%s",
                        consumer_id,
                    )

                def wait_consumer_exit(self, consumer_id: str) -> None:
                    worker = workers_by_identity[
                        (WorkerRole.CONSUMER, consumer_id)
                    ]
                    _wait_for_worker_exit(
                        worker,
                        timeout_s=CONSUMER_EXIT_TIMEOUT_SEC,
                    )
                    logging.info(
                        "[RBD-CTL-WAIT-CONS] consumer exited id=%s",
                        consumer_id,
                    )

                def set_loop_index(self, loop_index: int) -> None:
                    _etcd_put(REBIND_LOOP_KEY, str(loop_index).encode())
                    logging.info(
                        "[RBD-CTL-LOOPKEY] set loop_idx=%s",
                        loop_index,
                    )

                def wait_inactive_gap(self) -> None:
                    time.sleep(INACTIVE_GAP_SEC)

                def start_consumer(self, consumer_id: str) -> None:
                    print(
                        f"[rebind_client] starting next consumer {consumer_id}",
                        flush=True,
                    )
                    spawn(
                        WorkerRole.CONSUMER,
                        _consumer_cmd(
                            env.backend_type,
                            env.backend_ip,
                            consumer_id,
                            session_id,
                            prefetch,
                        ),
                        consumer_id,
                    )

                def wait_consumer_ready(self, consumer_id: str) -> None:
                    worker = workers_by_identity[
                        (WorkerRole.CONSUMER, consumer_id)
                    ]
                    wait_for_barrier(
                        [
                            BarrierTarget(
                                ready_key(
                                    session_id,
                                    worker.role,
                                    worker.identifier,
                                ),
                                worker,
                            )
                        ],
                        label=(
                            f"replacement consumer {consumer_id} readiness "
                            f"session={session_id}"
                        ),
                        read_key=_etcd_get,
                        timeout_s=READY_TIMEOUT_SEC,
                    )
                    logging.info(
                        "[RBD-CTL-READY] replacement consumer ready id=%s",
                        consumer_id,
                    )

                def resume_producers(
                    self,
                    round_index: int,
                    consumer_id: str,
                ) -> None:
                    resume_producers(
                        reason=(
                            f"round={round_index} consumer={consumer_id} ready"
                        )
                    )

            actions: RebindRoundActions = _Actions()
            coordinate_rebind_rounds(consumer_ids, actions)

            # Every consumer has drained and exited; producers remain paused at
            # the final acknowledged generation.
            _etcd_put("/test_mpmc_stop_producer", b"dummy_value")
            logging.info("[RBD-CTL-STOP-PROD] stop producers signaled")
            for worker in producer_workers:
                logging.info(
                    "[RBD-CTL-WAIT-PROD] waiting producer id=%s log=%s",
                    worker.identifier,
                    worker.log_file,
                )
                _wait_for_worker_exit(
                    worker,
                    timeout_s=PRODUCER_EXIT_TIMEOUT_SEC,
                )
            logging.info("[RBD-CTL-PROD-DONE] producers exited")
            logging.info("[RBD-CTL-ALL-DONE] all subprocesses exited")

            logging.info("[RBD-CTL-VERIFY] verify production/consumption counts")
            verify_production_consumption_counts(subprocesses)
            logging.info("[RBD-CTL-VERIFY] verify exit status markers")
            verify_exit_status(subprocesses)
            logging.info("[RBD-CTL-PASS] test passed")
            print("=== MPMC Rebind Client Test PASSED ===", flush=True)
        finally:
            logging.info("[RBD-CTL-FINISH] cleanup and restore backend")
            _cleanup_owned_workers(subprocesses)
            for prefix in (
                "/test_mpmc_stop_consumer",
                "/test_mpmc_consumer",
                "/test_mpmc_stop_producer",
                PRODUCER_PAUSE_KEY,
                REBIND_LOOP_KEY,
                f"/test_mpmc_rebind/sessions/{session_id}",
            ):
                try:
                    _etcd_delete_prefix(prefix)
                except Exception as exc:  # noqa: BLE001
                    logging.warning(
                        "[RBD-CTL-CLEANUP] failed to delete prefix %s: %s",
                        prefix,
                        exc,
                    )
            configure_backend(env, backend_type=prev_type, backend_ip=prev_ip)
            release(env)

    setup_test_environment(logging)
    # Execute across parameter matrix (uses TEST_ARGMATRIX by default)
    run_with_argmatrix(_once)


if __name__ == "__main__":
    # Allow running as a simple script (non-pytest path). Support worker subcommands.
    os.environ.setdefault("TEST_MPMC", "1")
    ns = _parse_args()
    env = create_channel_env()
    if ns.mode == "run_producer":
        run_producer(env, ns)
        release(env)
    elif ns.mode == "run_consumer":
        run_consumer(env, ns)
        release(env)
    else:
        print(
            "[rebind_client] __main__ entry — invoking test_mpmc_rebind_client()",
            flush=True,
        )
        test_mpmc_rebind_client()
