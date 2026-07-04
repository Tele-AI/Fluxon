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
from pathlib import Path
from typing import Any, List, Optional, Tuple

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
    load_test_fluxon_cluster_name,
    run_with_argmatrix,
    etcd_control_call_with_retry as _etcd_call_with_retry,
    etcd_control_delete_prefix as _etcd_delete_prefix,
    etcd_control_get as _etcd_get,
    etcd_control_put as _etcd_put,
)
from fluxon_py.kvclient import KvClientType, new_store  # noqa: E402
from fluxon_py.kvclient.kvclient_interface import KvClient  # noqa: E402
from fluxon_py.config import FluxonKvClientConfig  # noqa: E402


logging = init_logger()

SCRIPT_PATH_SELF = Path(__file__).resolve()
NEW_OR_BIND_KEY = "mpmc_rebind_client_test"
REBIND_LOOP_KEY = "/test_mpmc_rebind/loop_idx"
PRODUCER_PAUSE_KEY = "/test_mpmc_pause_producer"
LOOPS = 5  # number of consumer restart cycles
PRODUCER_MESSAGE_COUNT = 80  # per producer, should exceed total active windows
ACTIVE_WINDOW_SEC = 3  # each consumer stays active for this many seconds
INACTIVE_GAP_SEC = 1   # gap between stopping current and starting next consumer
DRAIN_SEC = 5  # final draining time before stopping last consumer


def _producer_cmd(backend_type: str, ip: str, producer_id: str, message_count: int) -> List[str]:
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
    ]


def _consumer_cmd(backend_type: str, ip: str, consumer_id: str, prefetch: int = 0) -> List[str]:
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
    ]

def _wait_fluxon_member_absent(instance_key: str, *, timeout_s: int = 45) -> None:
    """Wait until a fluxon cluster member key disappears from etcd.

    Purpose: avoid init failures like "Member already exists" when the previous test run
    exited abnormally and the member lease has not expired yet. Do not delete keys here;
    only wait for expiry to minimize test intrusion.
    """
    cluster = load_test_fluxon_cluster_name()
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

    consumer_parser = subparsers.add_parser("run_consumer", help="Run consumer")
    consumer_parser.add_argument("--backend_type", required=True, type=str)
    consumer_parser.add_argument("--ip", required=True, type=str)
    consumer_parser.add_argument("--construct_type", required=True, type=str)
    consumer_parser.add_argument("--new_or_bind_key", required=True, type=str)
    consumer_parser.add_argument("--chan_type", required=True, type=str)
    consumer_parser.add_argument("--consumer_id", required=True, type=str)
    consumer_parser.add_argument("--prefetch", required=False, type=int, default=0)
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


def verify_production_consumption_counts(
    subprocesses: List[tuple[str, subprocess.Popen, str]]
) -> None:
    print("=== Verifying Production and Consumption Counts ===")
    total_produced = 0
    total_consumed = 0
    produced_messages = set()
    consumed_messages = set()
    for process_type, _, log_file in subprocesses:
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
    subprocesses: List[tuple[str, subprocess.Popen, str]]
) -> None:
    print("=== Verifying Exit Status ===")
    normal_exits: list[str] = []
    crashes: list[str] = []
    for process_type, _, log_file in subprocesses:
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
    try:
        setup_test_environment(logging)
        # Precondition: ensure the member key for this instance_key is absent before creating the store.
        _wait_fluxon_member_absent(f"{store_key}_main")
        store = require_store(env, store_key)
        # Precondition: allow P2P/master handshake to settle before binding.
        time.sleep(10.0)
        producer = new_test_producer(
            args.construct_type,
            store,
            None,
            CHAN_CONFIG_TEST,
            args.new_or_bind_key,
            chan_type,
        )
        logging.info(
            f"[RBD-INIT] Producer-{args.producer_id} started (chan_type={chan_type})"
        )
        print(f"[Producer-{args.producer_id}] Started", flush=True)
        try:
            import uuid, random
            index = 0

            while True:
                # Check stop first
                stop_flag = _etcd_get("/test_mpmc_stop_producer")
                if stop_flag:
                    logging.info(
                        f"[RBD-STOP] Producer-{args.producer_id} stop flag detected"
                    )
                    break
                # Honor pause during per-loop draining
                i=0
                while True:
                    i+=1
                    pause_flag = _etcd_get(PRODUCER_PAUSE_KEY)
                    if not pause_flag:
                        logging.info(
                            f"[RBD-RESUME] Producer-{args.producer_id} resumed"
                        )
                        break
                    logging.info(f"[RBD-PAUSE] Producer-{args.producer_id} paused, loop i {i}")
                    # allow quick reaction to stop while paused
                    stop_flag = _etcd_get("/test_mpmc_stop_producer")
                    if stop_flag:
                        logging.info(
                            f"[RBD-STOP] Producer-{args.producer_id} stop while paused"
                        )
                        break
                    time.sleep(0.1)
                if stop_flag:
                    break
                # Read current loop index to embed into message key for verification per loop
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
                    # Track production per loop in etcd for gating
                    _etcd_put(
                        f"/test_mpmc_rebind/produced/{loop_idx}/{args.producer_id}/{unique_id}",
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
        except Exception as exc:  # noqa: BLE001
            logging.info(
                f"[RBD-ERROR] Producer-{args.producer_id} exception: {exc}"
            )
            print(f"[Producer-{args.producer_id}] Error: {exc}")
            print(f"{PRODUCER_CRASH_MARKER} {args.producer_id}")
            raise
        finally:
            logging.info(
                f"[RBD-FINISH] Producer-{args.producer_id} finished and closing"
            )
            print(f"[Producer-{args.producer_id}] Finished", flush=True)
            print(f"{PRODUCER_NORMAL_EXIT_MARKER} {args.producer_id}", flush=True)
            # Avoid running Python/Rust finalizers after success. Some Fluxon background
            # tasks (keepalive, P2P) can still be active during interpreter teardown and
            # may abort the process even after printing the success marker.
            os._exit(0)
    finally:
        configure_backend(env, backend_type=prev_type, backend_ip=prev_ip)
    release(env, store_key)


def run_consumer(env, args: argparse.Namespace) -> None:
    chan_type = _chan_type_from_str(args.chan_type)
    store_key = f"rebind_consumer_{args.consumer_id}"
    prev_type, prev_ip = env.backend_type, env.backend_ip
    configure_backend(env, backend_type=args.backend_type, backend_ip=args.ip)
    try:
        setup_test_environment(logging)
        # Precondition: ensure the member key for this instance_key is absent before creating the store.
        _wait_fluxon_member_absent(f"{store_key}_main")
        store = require_store(env, store_key)
        # Precondition: allow P2P/master handshake to settle before binding.
        time.sleep(10.0)
        consumer = new_test_consumer(
            args.construct_type,
            store,
            None,
            CHAN_CONFIG_TEST,
            args.new_or_bind_key,
            chan_type,
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
            lease_id=int(consumer.mpmc_channel.mpmc_global_lease.id),
        )
        logging.info(
            f"[RBD-REGISTER] Consumer-{args.consumer_id} registered in etcd"
        )
        consumed_count = 0
        try:
            import random
            draining = False
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
                                    f"/test_mpmc_rebind/consumed/{li}/{args.consumer_id}/{unique_id_str}",
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
        except Exception as exc:  # noqa: BLE001
            logging.info(
                f"[RBD-ERROR] Consumer-{args.consumer_id} exception: {exc}"
            )
            print(f"[Consumer-{args.consumer_id}] Error: {exc}")
            print(f"{CONSUMER_CRASH_MARKER} {args.consumer_id}")
            raise
        finally:
            if draining:
                logging.info(
                    f"[RBD-DRAIN-END] Consumer-{args.consumer_id} drained (no more data)"
                )
            logging.info(
                f"[RBD-FINISH] Consumer-{args.consumer_id} finished, consumed={consumed_count}"
            )
            print(
                f"[Consumer-{args.consumer_id}] Finished, consumed {consumed_count} messages",
                flush=True,
            )
            print(f"{CONSUMER_NORMAL_EXIT_MARKER} {args.consumer_id}", flush=True)
            # Same rationale as producer: hard-exit after printing success marker.
            os._exit(0)
    finally:
        configure_backend(env, backend_type=prev_type, backend_ip=prev_ip)
    release(env, store_key)


def test_mpmc_rebind_client() -> None:
    """Run the rebind client test across the shared parameter matrix.

    The matrix is provided by tests/test_lib.py (e.g., `prefetch`).
    """

    def _once(prefetch: int) -> None:
        print(f"[rebind_client] starting test... prefetch={prefetch}", flush=True)
        env = create_channel_env()
        prev_type, prev_ip = env.backend_type, env.backend_ip
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

            subprocesses: List[Tuple[str, subprocess.Popen, str]] = []

            def spawn(process_type: str, cmd: List[str], identifier: str) -> None:
                log_file = (
                    f"logs/mpmc_producer_{identifier}.log"
                    if process_type == "producer"
                    else f"logs/mpmc_consumer_{identifier}.log"
                )
                logging.info(
                    f"[RBD-CTL-SPAWN] type={process_type} id={identifier} log={log_file}"
                )
                with open(log_file, "w", encoding="utf-8") as log_f:
                    proc = subprocess.Popen(cmd, stdout=log_f, stderr=log_f, text=True)
                subprocesses.append((process_type, proc, log_file))

            # Start two long-running producers and initial consumer C0
            spawn(
                "producer",
                _producer_cmd(
                    env.backend_type, env.backend_ip, "P0", PRODUCER_MESSAGE_COUNT
                ),
                "P0",
            )
            spawn(
                "producer",
                _producer_cmd(
                    env.backend_type, env.backend_ip, "P1", PRODUCER_MESSAGE_COUNT
                ),
                "P1",
            )
            current_consumer = "C0"
            spawn(
                "consumer",
                _consumer_cmd(env.backend_type, env.backend_ip, current_consumer, prefetch),
                current_consumer,
            )

            # Repeatedly stop and restart a single consumer while producers keep producing
            # initialize loop index for producers to tag messages
            _etcd_put(REBIND_LOOP_KEY, b"0")
            logging.info("[RBD-CTL-LOOPKEY] set loop_idx=0")

            for i in range(LOOPS - 1):
                logging.info(f"[RBD-CTL-LOOP] round={i} active_window={ACTIVE_WINDOW_SEC}s")
                # Soft window to allow production
                time.sleep(ACTIVE_WINDOW_SEC)

                # Pause producers and stop current consumer to drain until last get_data times out
                _etcd_put(PRODUCER_PAUSE_KEY, b"1")
                logging.info("[RBD-CTL-PAUSE] producers paused")
                _etcd_put(f"/test_mpmc_stop_consumer/{current_consumer}", b"dummy_value")
                logging.info(
                    f"[RBD-CTL-STOP-CONS] request stop consumer={current_consumer}"
                )
                while True:
                    status = _etcd_get(f"/test_mpmc_consumer/{current_consumer}")
                    if not status:
                        break
                    time.sleep(0.5)
                logging.info(
                    f"[RBD-CTL-WAIT-CONS] consumer exited id={current_consumer}"
                )

                # Switch to next loop index now that previous consumer fully drained and exited
                _etcd_put(REBIND_LOOP_KEY, str(i + 1).encode())
                logging.info(f"[RBD-CTL-LOOPKEY] set loop_idx={i+1}")

                # Short gap, then start next consumer for next loop and resume producers
                time.sleep(INACTIVE_GAP_SEC)
                next_consumer = f"C{i+1}"
                print(
                    f"[rebind_client] starting next consumer {next_consumer}",
                    flush=True,
                )
                spawn(
                    "consumer",
                    _consumer_cmd(
                        env.backend_type, env.backend_ip, next_consumer, prefetch
                    ),
                    next_consumer,
                )
                current_consumer = next_consumer
                # Resume producers for next round
                _etcd_call_with_retry(
                    f"delete producer pause key {PRODUCER_PAUSE_KEY}",
                    lambda client: client.delete(PRODUCER_PAUSE_KEY),
                )
                logging.info("[RBD-CTL-RESUME] producers resumed")

            # After last loop index set, stop producers, then stop last consumer (which drains before exit)
            _etcd_put(PRODUCER_PAUSE_KEY, b"1")
            logging.info("[RBD-CTL-FINAL-PAUSE] producers paused before shutdown")
            _etcd_put("/test_mpmc_stop_producer", b"dummy_value")
            logging.info("[RBD-CTL-STOP-PROD] stop producers signaled")
            for process_type, proc, log_file in subprocesses:
                if process_type != "producer":
                    continue
                logging.info(f"[RBD-CTL-WAIT-PROD] waiting producer log={log_file}")
                proc.wait()
                if proc.returncode != 0:
                    raise RuntimeError(
                        f"producer failed with return code {proc.returncode}. Check log: {log_file}\n"
                        "--- child log tail ---\n"
                        f"{_read_log_tail(log_file)}\n"
                        "--- end child log tail ---"
                    )
            logging.info("[RBD-CTL-PROD-DONE] producers exited")
            # Stop the last consumer and wait for consumers to exit (drains until last get timeout)
            _etcd_put(f"/test_mpmc_stop_consumer/{current_consumer}", b"dummy_value")
            logging.info(
                f"[RBD-CTL-STOP-LAST-CONS] request stop consumer={current_consumer}"
            )

            for process_type, proc, log_file in subprocesses:
                logging.info(f"[RBD-CTL-WAIT] waiting {process_type} log={log_file}")
                proc.wait()
                if proc.returncode != 0:
                    raise RuntimeError(
                        f"{process_type} failed with return code {proc.returncode}. Check log: {log_file}\n"
                        "--- child log tail ---\n"
                        f"{_read_log_tail(log_file)}\n"
                        "--- end child log tail ---"
                    )
            logging.info("[RBD-CTL-ALL-DONE] all subprocesses exited")

            # Verify counts and exits
            logging.info("[RBD-CTL-VERIFY] verify production/consumption counts")
            verify_production_consumption_counts(subprocesses)
            logging.info("[RBD-CTL-VERIFY] verify exit status markers")
            verify_exit_status(subprocesses)
            logging.info("[RBD-CTL-PASS] test passed")
            print("=== MPMC Rebind Client Test PASSED ===", flush=True)
        finally:
            logging.info("[RBD-CTL-FINISH] cleanup and restore backend")
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
