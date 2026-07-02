"""Refactored MPMC channel integration test harness.

This module mirrors the legacy dynamic producer/consumer scenario while exposing
shared helpers so follow-up scenarios can be implemented per-class.
"""
from __future__ import annotations

import argparse
import os
import random
import shutil
import subprocess
import sys
import threading
import time
import uuid
import re
from pathlib import Path
import logging as _py_logging


def _configure_stdio_for_tail() -> None:
    # Ensure `tail -f` on redirected logs shows progress immediately.
    # The MPMC harness is often executed with stdout/stderr redirected to a file,
    # and Python defaults to block-buffered text IO in that case.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(line_buffering=True, write_through=True)


_configure_stdio_for_tail()

CURRENT_DIR = Path(__file__).resolve().parent
# Global log directory name and path
LOG_DIR_NAME = "logs"
LOG_DIR = CURRENT_DIR / LOG_DIR_NAME
def _find_project_root(start: Path) -> Path:
    for candidate in (start,) + tuple(start.parents):
        if (candidate / "setup.py").is_file():
            return candidate
    return start

PROJECT_ROOT = _find_project_root(CURRENT_DIR)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from typing import Dict, List, Optional, Tuple
from types import SimpleNamespace

import etcd3

from fluxon_py.api_ext_chan import (  # noqa: E402
    MPMCChanConsumer,
    MPMCChanProducer,
    ChanType,
    _new_unique_mapping_key,
)
from fluxon_py._api_ext_chan import mpsc  # noqa: E402
from fluxon_py._api_ext_chan.mpmc import (  # noqa: E402
    _new_mpmc_ready_channels_prefix,
    _new_mpmc_meta_key,
)
from fluxon_py.api_error import (  # noqa: E402
    ChannelClosedError,
    MessageConsumptionNoNewMessageError,
    ProducerClosedError,
)
from fluxon_py.logging import init_logger  # noqa: E402

# Base prefix for ready channels (before mpmc_id).
# Uses the library function pattern for consistency.
_READY_CHANNELS_BASE_PREFIX = "/mpmc_channels/ready/"
from fluxon_py.tests.test_api_chan_mpsc.test_api_chan_mpsc_base import (  # noqa: E402
    ChannelState,
    chan_type_from_string,
    configure_backend,
    create_channel_env,
    release,
    require_store,
)
from fluxon_py.tests.test_lib import (  # noqa: E402
    KV_SVC_IP,
    KV_SVC_TYPE,
    CHAN_CONFIG_TEST,
    TEST_TIMEOUT_SECONDS,
    ETCD_HOST,
    ETCD_PORT,
    setup_test_environment,
    new_test_consumer,
    new_test_producer,
    run_with_argmatrix,
)
from fluxon_py.tests.test_lib import pre_kill_existing_test_processes_by_script_name  # noqa: E402


logging = init_logger()

# Ensure log directory exists early (for handlers)
os.makedirs(LOG_DIR, exist_ok=True)

# Dedicated scan logger to avoid interfering with main log
_SCAN_LOG_PATH = (LOG_DIR / "mpmc_scan_offset.log")
_scan_logger = _py_logging.getLogger("mpmc_scan_offset")
if not _scan_logger.handlers:
    _scan_logger.setLevel(_py_logging.INFO)
    _scan_logger.propagate = False
    _handler = _py_logging.FileHandler(str(_SCAN_LOG_PATH), mode="a", encoding="utf-8")
    _handler.setFormatter(_py_logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    _scan_logger.addHandler(_handler)


PRODUCER_NORMAL_EXIT_MARKER = "PRODUCER_NORMAL_EXIT:"
PRODUCER_CRASH_MARKER = "PRODUCER_CRASH:"
CONSUMER_NORMAL_EXIT_MARKER = "CONSUMER_NORMAL_EXIT:"
CONSUMER_CRASH_MARKER = "CONSUMER_CRASH:"

def _atomic_stdout_write_line(line: str) -> None:
    payload = (line + "\n").encode("utf-8", "replace")
    os.write(sys.stdout.fileno(), payload)


INITIAL_PRODUCERS_COUNT = 3
INITIAL_CONSUMERS_COUNT = 2
NEW_PRODUCERS_MIN_COUNT = 1
NEW_PRODUCERS_MAX_COUNT = 3
NEW_CONSUMERS_MIN_COUNT = 2
NEW_CONSUMERS_MAX_COUNT = 4
DYNAMIC_PHASES_COUNT = 5
# CI should validate basic correctness deterministically. Consumer stop/recover
# phases tend to introduce non-deterministic message loss under current MPMC
# semantics (by design, stopped consumers may leave messages in their bound
# sub-channels). Keep this at 0 for CI; scale/stress belongs in benchmark runs.
CONSUMER_CRASH_RECOVER_PHASES = 0
MESSAGE_COUNT_PER_PRODUCER = 50
MESSAGE_COUNT_PER_PHASE_PRODUCER = 50
NEW_OR_BIND_KEY = "mpmc_dynamic_test"
NEW_OR_BIND_MAPPING_KEY = _new_unique_mapping_key(NEW_OR_BIND_KEY)




SCRIPT_PATH = Path(__file__).resolve()



def cli(args: Optional[argparse.Namespace] = None) -> None:
    def build_parser() -> argparse.ArgumentParser:
        """Construct the internal CLI parser for the harness subprocesses."""
        parser = argparse.ArgumentParser(description="MPMC Channel Test Runner")
        subparsers = parser.add_subparsers(dest="mode", help="Execution mode")

        subparsers.add_parser("main", help="Run main test suite")

        producer_parser = subparsers.add_parser(
            "run_producer", help="Run producer process"
        )
        # NOTE: run_producer/run_consumer are spawned by the harness subprocesses, not intended for manual CLI usage.
        producer_parser.add_argument("--backend_type", required=True, type=str)
        producer_parser.add_argument("--ip", required=True, type=str)
        producer_parser.add_argument("--construct_type", required=True, type=str)
        producer_parser.add_argument("--new_or_bind_key", required=True, type=str)
        producer_parser.add_argument("--chan_type", required=True, type=str)
        producer_parser.add_argument("--producer_id", required=True, type=str)
        producer_parser.add_argument("--message_count", required=True, type=int)

        consumer_parser = subparsers.add_parser(
            "run_consumer", help="Run consumer process"
        )
        # NOTE: run_producer/run_consumer are spawned by the harness subprocesses, not intended for manual CLI usage.
        consumer_parser.add_argument("--backend_type", required=True, type=str)
        consumer_parser.add_argument("--ip", required=True, type=str)
        consumer_parser.add_argument("--construct_type", required=True, type=str)
        consumer_parser.add_argument("--new_or_bind_key", required=True, type=str)
        consumer_parser.add_argument("--chan_type", required=True, type=str)
        consumer_parser.add_argument("--consumer_id", required=True, type=str)
        consumer_parser.add_argument("--prefetch", required=False, type=int, default=0)
        return parser


    def parse_args(parser: Optional[argparse.ArgumentParser] = None) -> argparse.Namespace:
        """Apply default mode handling around the harness parser."""
        parser = parser or build_parser()
        args = parser.parse_args()
        if args.mode is None:
            args.mode = "main"
        return args
    
    # cli main logic
    os.environ["TEST_MPMC"] = "1"
    ns = args or parse_args()
    if ns.mode == "main":
        # Strong precondition: before running a new round, kill stale test
        # subprocesses started by previous runs of this script to avoid
        # occupying channels/leases. This is not a fallback; it enforces a
        # clean environment similar to the MPSC harness.
        import os as _os
        pre_kill_existing_test_processes_by_script_name(_os.path.basename(str(SCRIPT_PATH)), 30)
        # Ensure argmatrix execution when invoked via CLI
        test_mpmc_dynamic_suite()
        return
    env = create_channel_env()
    try:
        handlers = {
            "run_producer": run_producer,
            "run_consumer": run_consumer,
        }
        handler = handlers.get(ns.mode)
        if handler is None:
            raise ValueError(f"Unsupported mode: {ns.mode}")
        handler(env, ns)
    finally:
        release(env)
    # Avoid running CPython's full interpreter teardown in these subprocess roles.
    #
    # In CI we observed sporadic SIGABRT with "FATAL: exception not rethrown" after
    # the handler completed successfully. Fast-exiting keeps the test semantics
    # (exit code 0 == success) while avoiding native teardown hazards.
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)


def run_main(env: "ChannelState", args: argparse.Namespace) -> None:
    prev_type, prev_ip = env.backend_type, env.backend_ip
    configure_backend(
        env,
        backend_type=env.default_backend_type,
        backend_ip=env.default_backend_ip,
    )
    try:
        setup_test_environment(logging)
        shutil.rmtree("logs", ignore_errors=True)
        clean_etcd()
        test_mpmc_member_lease_expiry_closes_owner()
        clean_etcd()
        test_mpmc_same_process_second_producer_survives_first_close()
        clean_etcd()
        scenario_dynamic_producer_consumer(
            env,
            env.backend_type,
            env.backend_ip,
            prefetch=int(getattr(args, "prefetch", 0)),
        )
        clean_etcd()
    finally:
        configure_backend(env, backend_type=prev_type, backend_ip=prev_ip)
    print("=============== MPMC TEST COMPLETED ==============")


def run_producer(env: "ChannelState", args: argparse.Namespace) -> None:
    chan_type = chan_type_from_string(args.chan_type)
    store_key = f"mpmc_dynamic_producer_{args.producer_id}"
    prev_type, prev_ip = env.backend_type, env.backend_ip
    configure_backend(env, backend_type=args.backend_type, backend_ip=args.ip)
    try:
        setup_test_environment(logging)
        store = require_store(env, store_key)
        producer = new_test_producer(
            args.construct_type,
            store,
            None,
            CHAN_CONFIG_TEST,
            args.new_or_bind_key,
            chan_type,
        )
        assert isinstance(producer, MPMCChanProducer)
        print(f"[Producer-{args.producer_id}] Started")
        etcd_client = producer.etcd_client
        try:
            for index in range(args.message_count):
                unique_id = str(uuid.uuid4())
                message_data = (
                    f"mpmc-{producer.get_chan_id()}-p{args.producer_id}-{index}-"
                ).encode()
                msg_id = message_data.decode() + unique_id
                msg = {"unique_id": msg_id, "payload": message_data}
                res = producer.put_data(msg)
                if res.is_ok():
                    _ = res.unwrap()
                    print(
                        f"[Producer-{args.producer_id}] Sent message "
                        f"{index + 1}/{args.message_count}: {msg_id}"
                    )
                    _atomic_stdout_write_line(f"PRODUCE_MARKER: {args.producer_id}:{msg_id}")
                else:
                    error_msg = (
                        f"Failed to send message {index + 1}/{args.message_count}:"
                        f" {res.unwrap_error()}"
                    )
                    print(f"[Producer-{args.producer_id}] {error_msg}")
                    raise RuntimeError(error_msg)
                time.sleep(random.uniform(0.1, 1))
        except Exception as exc:  # noqa: BLE001
            print(f"[Producer-{args.producer_id}] Error: {exc}")
            _atomic_stdout_write_line(f"{PRODUCER_CRASH_MARKER} {args.producer_id}")
            raise
        finally:
            print(f"[Producer-{args.producer_id}] Finished")
            _atomic_stdout_write_line(f"{PRODUCER_NORMAL_EXIT_MARKER} {args.producer_id}")
            producer.close().unwrap()
    finally:
        configure_backend(env, backend_type=prev_type, backend_ip=prev_ip)
    release(env, store_key)


def run_consumer(env: "ChannelState", args: argparse.Namespace) -> None:
    chan_type = chan_type_from_string(args.chan_type)
    store_key = f"mpmc_dynamic_consumer_{args.consumer_id}"
    prev_type, prev_ip = env.backend_type, env.backend_ip
    configure_backend(env, backend_type=args.backend_type, backend_ip=args.ip)
    try:
        setup_test_environment(logging)
        store = require_store(env, store_key)
        consumer = new_test_consumer(
            args.construct_type,
            store,
            None,
            CHAN_CONFIG_TEST,
            args.new_or_bind_key,
            chan_type,
        )
        assert isinstance(consumer, MPMCChanConsumer)
        print(
            f"[Consumer-{args.consumer_id}] Started with mpmc consumer "
            f"{consumer.mpmc_channel.mpmc_member_id}",
            flush=True,
        )
        etcd_client = consumer.etcd_client
        etcd_client.put(
            f"/test_mpmc_consumer/{args.consumer_id}",
            b"dummy_value",
            consumer.mpmc_channel.mpmc_global_lease,
        )

        consumed_count = 0
        try:
            no_data_count = 0
            no_data_timeout = 3.0  # Exit after 3s with no data
            last_activity = time.time()
            all_producers_done = False
            producer_done_check_interval = 0.5  # Check every 0.5s
            last_producer_check = time.time()
            
            while True:
                # Periodically check whether all producers are done.
                now = time.time()
                if now - last_producer_check >= producer_done_check_interval:
                    stop_flag, _ = etcd_client.get(f"/test_mpmc_stop_producer")
                    all_producers_done = stop_flag is not None
                    last_producer_check = now

                # External stop signal from harness.
                stop_flag, _ = etcd_client.get(
                    f"/test_mpmc_stop_consumer/{args.consumer_id}"
                )
                if stop_flag:
                    logging.info(
                        "[Consumer-%s] received stop signal, stop consumer",
                        args.consumer_id,
                    )
                    break
                
                # Try to fetch data, but keep the polling rate low.
                res = consumer.get_data(batch_size=1, try_time=10, prefetch_num=int(getattr(args, "prefetch", 0)))
                
                if res.is_ok():
                    success = res.unwrap()
                    if isinstance(success, list) and success:
                        msg = success[0]
                        if isinstance(msg, dict):
                            msg_id = msg["unique_id"]
                            if isinstance(msg_id, (bytes, bytearray)):
                                msg_id_str = msg_id.decode()
                            else:
                                msg_id_str = str(msg_id)
                            consumed_count += 1
                            last_activity = time.time()  # Update only on successful consume
                            no_data_count = 0
                            print(
                                f"[Consumer-{args.consumer_id}] Consumed message "
                                f"{consumed_count}: {msg_id_str}"
                            )
                            _atomic_stdout_write_line(f"CONSUME_MARKER: {args.consumer_id}:{msg_id_str}")
                    else:
                        # Empty list: check whether we should exit.
                        idle_time = time.time() - last_activity
                        if all_producers_done and idle_time >= no_data_timeout:
                            print(
                                f"[Consumer-{args.consumer_id}] All producers done and idle for "
                                f"{idle_time:.1f}s, exiting"
                            )
                            break
                        
                        time.sleep(0.1)  # Small delay to avoid busy spinning
                        no_data_count += 1
                else:
                    err = res.unwrap_error()
                    if isinstance(err, MessageConsumptionNoNewMessageError):
                        idle_time = time.time() - last_activity
                        if all_producers_done and idle_time >= no_data_timeout:
                            print(
                                f"[Consumer-{args.consumer_id}] All producers done and idle for "
                                f"{idle_time:.1f}s, exiting",
                                flush=True,
                            )
                            break
                        time.sleep(0.1)
                        no_data_count += 1
                        continue

                    print(
                        f"[Consumer-{args.consumer_id}] Error getting data: {err}",
                        flush=True,
                    )
                    time.sleep(0.1)
                # Stop signal is checked at loop start to keep all paths responsive.
                    
        except Exception as exc:  # noqa: BLE001
            print(f"[Consumer-{args.consumer_id}] Error: {exc}")
            _atomic_stdout_write_line(f"{CONSUMER_CRASH_MARKER} {args.consumer_id}")
            raise
        finally:
            print(
                f"[Consumer-{args.consumer_id}] Finished, consumed"
                f" {consumed_count} messages"
            )
            _atomic_stdout_write_line(f"{CONSUMER_NORMAL_EXIT_MARKER} {args.consumer_id}")
            etcd_client.delete(f"/test_mpmc_consumer/{args.consumer_id}")
            consumer.close().unwrap()
    finally:
        configure_backend(env, backend_type=prev_type, backend_ip=prev_ip)
    release(env, store_key)


def clean_etcd() -> None:
    with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
        etcd_client.delete_prefix("/mpmc_channels")
        etcd_client.delete_prefix("/channels")
        etcd_client.delete_prefix("/test_mpmc_stop_consumer")
        etcd_client.delete_prefix("/test_mpmc_consumer")
        etcd_client.delete_prefix("/test_mpmc_stop_producer")


def _wait_until_lease_revoked(
    etcd_client: etcd3.Etcd3Client,
    lease_id: int,
    *,
    timeout_s: float = 10.0,
) -> None:
    deadline = time.time() + timeout_s
    while True:
        try:
            info = etcd_client.get_lease_info(int(lease_id))
        except Exception as exc:  # noqa: BLE001
            msg = str(exc).lower()
            if "not found" in msg or "requested lease not found" in msg:
                return
            if time.time() >= deadline:
                raise RuntimeError(
                    f"lease revoke verification failed for lease_id={lease_id}: {exc}"
                ) from exc
        else:
            ttl_val = getattr(info, "TTL", None)
            if not isinstance(ttl_val, int):
                raise RuntimeError(
                    f"invalid TTL returned for lease_id={lease_id}: {ttl_val!r}"
                )
            if ttl_val <= 0:
                return
        if time.time() >= deadline:
            raise RuntimeError(
                f"lease_id={lease_id} still alive after revoke timeout={timeout_s}s"
            )
        time.sleep(0.1)


def test_mpmc_member_lease_expiry_closes_owner() -> None:
    setup_test_environment(logging)
    env = create_channel_env()
    store_key = "mpmc_member_lease_expiry_store"
    producer: Optional[MPMCChanProducer] = None
    try:
        clean_etcd()
        store = require_store(env, store_key)
        producer = new_test_producer(
            "new_or_bind",
            store,
            None,
            CHAN_CONFIG_TEST,
            "mpmc_member_lease_expiry_case",
            ChanType.MPMC,
        )
        assert isinstance(producer, MPMCChanProducer)
        chan_id = producer.get_chan_id()
        lease_id = int(producer.mpmc_channel.mpmc_member_lease.id)

        with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
            mpsc_meta_before = list(etcd_client.get_prefix("/channels/meta/"))
            assert len(mpsc_meta_before) == 0, (
                "fresh MPMC producer should not create any sub-MPSC metadata before the first put, "
                f"found {len(mpsc_meta_before)} keys"
            )
            etcd_client.revoke_lease(lease_id)
            _wait_until_lease_revoked(etcd_client, lease_id)

        first_put = producer.put_data(
            {
                "unique_id": f"mpmc-member-lease-expiry-{chan_id}-first",
                "payload": b"mpmc-member-lease-expiry-first",
            }
        )
        assert not first_put.is_ok(), "expected first put_data to fail after member lease revoke"
        first_err = first_put.unwrap_error()
        assert isinstance(first_err, ChannelClosedError), (
            f"expected ChannelClosedError after member lease revoke, got {first_err!r}"
        )
        assert first_err.channel_id == chan_id
        assert producer.shutdown_ctl.closed, "producer must mark itself closed after member lease loss"

        with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
            mpsc_meta_after = list(etcd_client.get_prefix("/channels/meta/"))
            assert len(mpsc_meta_after) == 0, (
                "dead member lease must stop sub-MPSC creation before any new channel meta is published"
            )

        second_put = producer.put_data(
            {
                "unique_id": f"mpmc-member-lease-expiry-{chan_id}-second",
                "payload": b"mpmc-member-lease-expiry-second",
            }
        )
        assert not second_put.is_ok(), "expected second put_data on closed producer to fail"
        second_err = second_put.unwrap_error()
        assert isinstance(second_err, ProducerClosedError), (
            f"expected ProducerClosedError on subsequent put_data, got {second_err!r}"
        )
    finally:
        if producer is not None:
            close_res = producer.close()
            if close_res.is_ok():
                _ = close_res.unwrap()
            else:
                _ = close_res.unwrap_error()
        release(env)
        clean_etcd()


def test_mpmc_same_process_second_producer_survives_first_close() -> None:
    setup_test_environment(logging)
    env = create_channel_env()
    store_key_a = "mpmc_same_process_producer_a_store"
    store_key_b = "mpmc_same_process_producer_b_store"
    producer_a: Optional[MPMCChanProducer] = None
    producer_b: Optional[MPMCChanProducer] = None
    try:
        clean_etcd()
        store_a = require_store(env, store_key_a)
        producer_a = new_test_producer(
            "new_or_bind",
            store_a,
            None,
            CHAN_CONFIG_TEST,
            "mpmc_same_process_second_producer_survives_first_close",
            ChanType.MPMC,
        )
        assert isinstance(producer_a, MPMCChanProducer)

        first_put = producer_a.put_data(
            {
                "unique_id": f"{producer_a.get_chan_id()}-producer-a-first",
                "payload": b"producer-a-first",
            }
        )
        assert first_put.is_ok(), f"first producer initial put failed: {first_put.unwrap_error()}"
        _ = first_put.unwrap()

        store_b = require_store(env, store_key_b)
        producer_b = new_test_producer(
            "new_or_bind",
            store_b,
            None,
            CHAN_CONFIG_TEST,
            "mpmc_same_process_second_producer_survives_first_close",
            ChanType.MPMC,
        )
        assert isinstance(producer_b, MPMCChanProducer)
        assert producer_a.get_chan_id() == producer_b.get_chan_id()
        assert producer_a.mpmc_channel.mpmc_member_id != producer_b.mpmc_channel.mpmc_member_id

        warmup_put = producer_b.put_data(
            {
                "unique_id": f"{producer_b.get_chan_id()}-producer-b-before-close",
                "payload": b"producer-b-before-close",
            }
        )
        assert warmup_put.is_ok(), f"second producer warmup put failed: {warmup_put.unwrap_error()}"
        _ = warmup_put.unwrap()

        close_res = producer_a.close()
        assert close_res.is_ok(), f"first producer close failed: {close_res.unwrap_error()}"
        _ = close_res.unwrap()
        producer_a = None

        second_put = producer_b.put_data(
            {
                "unique_id": f"{producer_b.get_chan_id()}-producer-b-after-close",
                "payload": b"producer-b-after-close",
            }
        )
        assert second_put.is_ok(), (
            "closing the first same-process producer must not invalidate the second producer: "
            f"{second_put.unwrap_error()}"
        )
        _ = second_put.unwrap()
    finally:
        if producer_b is not None:
            close_res = producer_b.close()
            if close_res.is_ok():
                _ = close_res.unwrap()
            else:
                _ = close_res.unwrap_error()
        if producer_a is not None:
            close_res = producer_a.close()
            if close_res.is_ok():
                _ = close_res.unwrap()
            else:
                _ = close_res.unwrap_error()
        release(env)
        clean_etcd()


def scenario_dynamic_producer_consumer(
    env: "ChannelState",
    backend_type: str,
    ip: str,
    *,
    prefetch: int = 0,
) -> None:
    print(
        f"=== Testing MPMC Dynamic Producer/Consumer with {backend_type} backend ==="
    )
    processes: List[Tuple[str, List[str]]] = []
    subprocesses: List[Tuple[str, subprocess.Popen, str]] = []
    # Map all process handles by identifier (producer_id/consumer_id)
    process_handles_by_id: Dict[str, Tuple[str, subprocess.Popen, str]] = {}
    joined_ids: set[str] = set()
    etcd_client = etcd3.client(ETCD_HOST, ETCD_PORT)
    initial_consumers_id: List[str] = []
    dyn_consumers: List[str] = []
    recovered_consumers: List[str] = []
    test_mpmc_id: Optional[str] = None

    def _print_process_log_tail(log_file: str, *, max_lines: int = 200) -> None:
        print(f"=== subprocess log tail: {log_file} ===", flush=True)
        try:
            with open(log_file, "rb") as handle:
                lines = handle.readlines()[-max_lines:]
            for raw in lines:
                print(raw.decode("utf-8", "replace").rstrip("\n"), flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"failed to read subprocess log {log_file}: {exc}", flush=True)
        print(f"=== end subprocess log tail: {log_file} ===", flush=True)

    def fail_fast_on_subprocess_error(*, process_type_filter: Optional[str] = None) -> None:
        for identifier, (process_type, proc, log_file) in process_handles_by_id.items():
            if process_type_filter is not None and process_type != process_type_filter:
                continue
            rc = proc.poll()
            if rc is None:
                continue
            if rc != 0:
                _print_process_log_tail(log_file)
                raise RuntimeError(
                    f"{process_type} {identifier} exited early with return code {rc}. "
                    f"Check log file for details: {log_file}"
                )

    def wait_all_of_type(process_type: str, *, timeout_s: int) -> None:
        deadline = time.time() + float(timeout_s)
        while True:
            fail_fast_on_subprocess_error(process_type_filter=process_type)
            running: List[Tuple[str, str]] = []
            for identifier, (ptype, proc, log_file) in process_handles_by_id.items():
                if ptype != process_type:
                    continue
                if identifier in joined_ids:
                    continue
                if proc.poll() is None:
                    running.append((identifier, log_file))
                    continue
                if proc.returncode == 0:
                    joined_ids.add(identifier)
                    print(f"{ptype} {identifier} completed successfully")
                    print(f"Log file: {log_file}")
                    continue
                _print_process_log_tail(log_file)
                raise RuntimeError(
                    f"{ptype} {identifier} failed with return code {proc.returncode}."
                    f" Check log file for details: {log_file}"
                )

            if not running:
                return

            if time.time() >= deadline:
                details = ", ".join(f"{ident}({log})" for ident, log in running)
                raise RuntimeError(
                    f"Timed out waiting for {process_type} processes to exit after {timeout_s}s. "
                    f"Still running: {details}"
                )

            time.sleep(1.0)

    def _extract_mpmc_member_id_from_consumer_log(log_file: str) -> Optional[int]:
        patterns = (
            # Old format used by some test entrypoints.
            re.compile(br"Started with mpmc consumer\s+(\d+)"),
            # Current lease init logs from fluxon_py/_api_ext_chan/mpmc.py.
            re.compile(br"\bmember_id=(\d+)\b"),
        )
        # Subprocess logs may contain non-UTF8 bytes from native components;
        # parse in binary to avoid decode failures.
        with open(log_file, "rb") as handle:
            for raw in handle:
                for pattern in patterns:
                    match = pattern.search(raw)
                    if match is not None:
                        return int(match.group(1))
        return None

    def _count_ready_keys_for_member_id(member_id: int) -> int:
        assert test_mpmc_id is not None, "test_mpmc_id must be initialized before counting ready keys"
        ready_chans_kvs = list(etcd_client.get_prefix(_new_mpmc_ready_channels_prefix(test_mpmc_id)))
        count = 0
        for value, _meta in ready_chans_kvs:
            if value is None:
                continue
            if value.decode() == str(member_id):
                count += 1
        return count

    def _list_ready_keys_for_member_id(member_id: int) -> List[str]:
        assert test_mpmc_id is not None, "test_mpmc_id must be initialized before listing ready keys"
        member_id_str = str(member_id)
        ready_chans_kvs = list(etcd_client.get_prefix(_new_mpmc_ready_channels_prefix(test_mpmc_id)))
        keys: List[str] = []
        for value, meta in ready_chans_kvs:
            if value is None:
                continue
            if value.decode() != member_id_str:
                continue
            try:
                keys.append(meta.key.decode())
            except Exception:
                keys.append(repr(meta.key))
        return keys

    def _wait_ready_keys_cleared_for_member_id(member_id: int, timeout_s: float) -> List[str]:
        assert timeout_s > 0, f"timeout_s must be > 0, got {timeout_s!r}"
        deadline = time.time() + timeout_s
        while True:
            leftover_keys = _list_ready_keys_for_member_id(member_id)
            if len(leftover_keys) == 0:
                return leftover_keys
            if time.time() >= deadline:
                return leftover_keys
            time.sleep(0.2)

    def _wait_for_test_mpmc_id() -> str:
        deadline = time.time() + float(TEST_TIMEOUT_SECONDS)
        while True:
            value, _meta = etcd_client.get(NEW_OR_BIND_MAPPING_KEY)
            if value is not None:
                raw = value.decode().strip()
                if raw.isdigit():
                    candidate = raw
                    meta_val, _meta = etcd_client.get(_new_mpmc_meta_key(candidate))
                    if meta_val is not None:
                        return candidate
                    logging.warning(
                        "Ignoring stale mpmc id from etcd key %s: mpmc_id=%s has no meta yet; waiting for recreate",
                        NEW_OR_BIND_MAPPING_KEY,
                        candidate,
                    )
                    time.sleep(0.2)
                    continue
                raise RuntimeError(
                    f"Invalid mpmc id in etcd key {NEW_OR_BIND_MAPPING_KEY!r}: {raw!r}"
                )
            if time.time() >= deadline:
                raise RuntimeError(
                    f"Timed out waiting for etcd key {NEW_OR_BIND_MAPPING_KEY!r} to appear; "
                    "MPMC channel was not created/bound in time."
                )
            time.sleep(0.2)

    def _assert_test_mpmc_id_stable() -> None:
        assert test_mpmc_id is not None
        value, _meta = etcd_client.get(NEW_OR_BIND_MAPPING_KEY)
        if value is None:
            raise RuntimeError(
                f"etcd key {NEW_OR_BIND_MAPPING_KEY!r} disappeared during test; expected mpmc_id={test_mpmc_id}"
            )
        raw = value.decode().strip()
        if not raw.isdigit():
            raise RuntimeError(
                f"Invalid mpmc id in etcd key {NEW_OR_BIND_MAPPING_KEY!r} during test: {raw!r}"
            )
        current = raw
        if current != test_mpmc_id:
            raise RuntimeError(
                f"mpmc_id changed during test: etcd key {NEW_OR_BIND_MAPPING_KEY!r} moved from {test_mpmc_id} to {current}. "
                "This will split producers/consumers across different MPMC ids and can cause hangs."
            )

    def get_identifier(process_type: str, cmd: List[str]) -> str:
        """Extract the producer/consumer identifier from a command list."""
        flag = "--producer_id" if process_type == "producer" else "--consumer_id"
        for index, arg in enumerate(cmd):
            if arg == flag and index + 1 < len(cmd):
                return cmd[index + 1]
        return "unknown"

    def get_process_log(process_type: str, cmd: List[str]) -> str:
        identifier = get_identifier(process_type, cmd)
        if process_type == "producer":
            return str(LOG_DIR / f"mpmc_producer_{identifier}.log")
        return str(LOG_DIR / f"mpmc_consumer_{identifier}.log")

    def start_processes() -> None:
        os.makedirs(LOG_DIR, exist_ok=True)
        os.system(f"chmod -R 777 {LOG_DIR}")
        for process_type, cmd in processes:
            log_file = get_process_log(process_type, cmd)
            print(f"Starting {process_type}: {' '.join(cmd)}")
            print(f"Log file: {log_file}")
            with open(log_file, "w", encoding="utf-8") as log_f:
                proc = subprocess.Popen(cmd, stdout=log_f, stderr=log_f, text=True)
            subprocesses.append((process_type, proc, log_file))
            # Track handle by identifier for fine-grained joins
            identifier = get_identifier(process_type, cmd)
            process_handles_by_id[identifier] = (process_type, proc, log_file)
        processes.clear()

    def producer_process_cmd(producer_id: str, message_count: int) -> List[str]:
        return [
            sys.executable,
            "-u",
            str(SCRIPT_PATH),
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

    def consumer_process_cmd(consumer_id: str) -> List[str]:
        return [
            sys.executable,
            "-u",
            str(SCRIPT_PATH),
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

    def scan_producer_offset() -> None:
        # Use dedicated scan logger (single file mpmc_scan_offset.log)
        logger = _scan_logger
        mpsc_producer_offset_pair: Dict[int, Dict[int, List[int]]] = {}
        scan_client = etcd3.client(ETCD_HOST, ETCD_PORT)
        mpsc_chans: List[int] = []

        def get_chans() -> List[int]:
            prefix = mpsc._new_etcd_meta_key_prefix()
            return [
                int(meta.key.decode().split("/")[-1])
                for _, meta in scan_client.get_prefix(prefix)
            ]

        try:
            while True:
                if mpsc_producer_offset_pair:
                    sum_unconsumed_count = 0
                    sum_consumed_count = 0
                    logger.info("================================================")
                    logger.info("================================================")
                    for mpsc_id in list(mpsc_producer_offset_pair.keys()):
                        logger.info("================================================")
                        logger.info("- mpsc_id: %s unconsumed summary:", mpsc_id)
                        mpsc_unconsumed: Dict[int, Tuple[int, int, int]] = {}
                        for producer_id in list(
                            mpsc_producer_offset_pair[mpsc_id].keys()
                        ):
                            produce_offset = mpsc_producer_offset_pair[mpsc_id][producer_id][0]
                            consume_offset = mpsc_producer_offset_pair[mpsc_id][producer_id][1]
                            mpsc_unconsumed[producer_id] = (
                                produce_offset,
                                consume_offset,
                                produce_offset + 1 - consume_offset,
                            )
                        for producer_id, detail in mpsc_unconsumed.items():
                            logger.info(
                                "-- mpsc_id: %s producer_id: %s produce_offset: %s, "
                                "consume_offset: %s, unconsumed: %s",
                                mpsc_id,
                                producer_id,
                                detail[0],
                                detail[1],
                                detail[2],
                            )
                            sum_unconsumed_count += detail[2]
                            sum_consumed_count += detail[1]
                        logger.info(
                            "- mpmc sum unconsumed: %s",
                            sum(detail[2] for detail in mpsc_unconsumed.values()),
                        )
                    logger.info(">>> sum_unconsumed_count: %s", sum_unconsumed_count)
                    logger.info(">>> sum_consumed_count: %s", sum_consumed_count)
                ready_chans_kvs = list(scan_client.get_prefix(_READY_CHANNELS_BASE_PREFIX))
                mpmc_ids: set[str] = set()
                ready_mpscs: List[str] = []
                for value, meta in ready_chans_kvs:
                    key = meta.key.decode()
                    mpmc_id = key.split("/")[-2]
                    mpsc_id = key.split("/")[-1]
                    mpmc_ids.add(mpmc_id)
                    if value is not None:
                        ready_mpscs.append(
                            f"mpmc consumer {value.decode()} binded to mpsc {mpsc_id}"
                        )
                # Only for diagnostics. The actual test is scoped to `test_mpmc_id`
                # below and does not assume the shared etcd is empty.
                ready_mpscs_str = "\n   ".join(ready_mpscs)
                logger.info("ready_mpscs: %s", ready_mpscs_str)

                new_mpsc_chans = get_chans()
                for mpsc_id in new_mpsc_chans:
                    if mpsc_id not in mpsc_chans:
                        mpsc_chans.append(mpsc_id)

                logger.info("all_mpscs: %s", mpsc_chans)
                for mpsc_id in mpsc_chans:
                    mpsc_producer_offset_pair.setdefault(mpsc_id, {})
                    producer_offset_kvs = list(
                        scan_client.get_prefix(
                            mpsc._new_produce_offset_of_all_producer_key(mpsc_id)
                        )
                    )
                    logger.info(
                        "mpsc %s producer_offset_kvs: %s",
                        mpsc_id,
                        producer_offset_kvs,
                    )
                    mpsc_producer_offsets = {
                        int(meta.key.decode().split("/")[-1]): int(value.decode())
                        for value, meta in producer_offset_kvs
                    }
                    logger.info(
                        "mpsc %s producer_offsets dict: %s",
                        mpsc_id,
                        mpsc_producer_offsets,
                    )
                    for mpsc_producer_key, mpsc_producer_offset in (
                        mpsc_producer_offsets.items()
                    ):
                        mpsc_producer_offset_pair[mpsc_id].setdefault(
                            mpsc_producer_key, [-1, 0]
                        )
                        mpsc_producer_offset_pair[mpsc_id][mpsc_producer_key][0] = (
                            mpsc_producer_offset
                        )
                        consume_offset_key = mpsc._new_consume_offset_of_one_producer_key(
                            mpsc_id, str(mpsc_producer_key)
                        )
                        consume_value, _ = scan_client.get(consume_offset_key)
                        logger.info(
                            "mpsc_id: %s mpsc_producer_key: %s consume_offset_key: %s "
                            "mpsc_consume_offset: %s",
                            mpsc_id,
                            mpsc_producer_key,
                            consume_offset_key,
                            consume_value,
                        )
                        if consume_value is not None:
                            mpsc_producer_offset_pair[mpsc_id][mpsc_producer_key][1] = int(
                                consume_value.decode()
                            )
                time.sleep(5)
        finally:
            scan_client.close()

    try:
        # NOTE: `new_or_bind` has a race window during first-time channel creation when multiple
        # producers start concurrently. Only one producer is allowed to create the first channel.
        # To keep this test stable in CI (no retries/fallback), we start a single producer first,
        # wait for the channel to be created (etcd key appears), then start the remaining producers.
        processes.append(
            ("producer", producer_process_cmd("P0", MESSAGE_COUNT_PER_PRODUCER))
        )
        start_processes()

        test_mpmc_id = _wait_for_test_mpmc_id()
        logging.info("Using test_mpmc_id=%s (from etcd key=%s)", test_mpmc_id, NEW_OR_BIND_MAPPING_KEY)

        for idx in range(1, INITIAL_PRODUCERS_COUNT):
            producer_id = f"P{idx}"
            processes.append(
                ("producer", producer_process_cmd(producer_id, MESSAGE_COUNT_PER_PRODUCER))
            )
        for idx in range(INITIAL_CONSUMERS_COUNT):
            consumer_id = f"C{idx}"
            processes.append(("consumer", consumer_process_cmd(consumer_id)))
            initial_consumers_id.append(consumer_id)
        start_processes()

        scan_thread = threading.Thread(target=scan_producer_offset, daemon=True)
        scan_thread.start()

        print("=== Starting dynamic management phase ===")
        for phase in range(DYNAMIC_PHASES_COUNT):
            print(f"=== Phase {phase + 1} ===")
            new_producers = random.randint(
                NEW_PRODUCERS_MIN_COUNT, NEW_PRODUCERS_MAX_COUNT
            )
            for idx in range(new_producers):
                print(
                    f"Adding {new_producers} new producers in phase {phase + 1}"
                )
                producer_id = f"P{phase}_{idx}"
                processes.append(
                    (
                        "producer",
                        producer_process_cmd(
                            producer_id, MESSAGE_COUNT_PER_PHASE_PRODUCER
                        ),
                    )
                )

            new_consumers = random.randint(
                NEW_CONSUMERS_MIN_COUNT, NEW_CONSUMERS_MAX_COUNT
            )
            for idx in range(new_consumers):
                print(
                    f"Adding {new_consumers} new consumers in phase {phase + 1}"
                )
                consumer_id = f"C{phase}_{idx}"
                processes.append(("consumer", consumer_process_cmd(consumer_id)))
                dyn_consumers.append(consumer_id)

            start_processes()
            time.sleep(5)
            _assert_test_mpmc_id_stable()

        for phase in range(CONSUMER_CRASH_RECOVER_PHASES):
            phase_name = f"simulate_consumer_crash_and_recover_{phase}"
            consumes_to_stop: List[str] = []
            candidate_consumers = dyn_consumers + recovered_consumers
            for consumer_id in candidate_consumers:
                if len(consumes_to_stop) == min(3, max(len(candidate_consumers) - 2, 0)):
                    break
                if random.random() < 0.1:
                    consumes_to_stop.append(consumer_id)

            for consumer_id in consumes_to_stop:
                etcd_client.put(
                    f"/test_mpmc_stop_consumer/{consumer_id}", b"dummy_value"
                )

            for consumer_id in consumes_to_stop:
                handle = process_handles_by_id.get(consumer_id)
                assert handle is not None, f"consumer {consumer_id} join handle not found"
                process_type, proc, log_file = handle
                logging.info(
                    "waiting for consumer %s to stop (pid=%s, log: %s)",
                    consumer_id,
                    proc.pid,
                    log_file,
                )
                deadline = time.time() + float(TEST_TIMEOUT_SECONDS)
                while proc.poll() is None:
                    fail_fast_on_subprocess_error(process_type_filter="producer")
                    if time.time() >= deadline:
                        raise RuntimeError(
                            f"Timed out waiting for consumer {consumer_id} to exit after stop signal. "
                            f"pid={proc.pid}, log_file={log_file}"
                        )
                    time.sleep(1)

                logging.info("Joining stopped consumer %s (log: %s)", consumer_id, log_file)
                fail_fast_on_subprocess_error(process_type_filter="producer")
                proc.wait()

                member_id = _extract_mpmc_member_id_from_consumer_log(log_file)

                # Verify that ready keys exist (global view) before join
                ready_keys_before = list(etcd_client.get_prefix(_READY_CHANNELS_BASE_PREFIX))
                logging.info(
                    "Before join: total ready keys=%d (all mpmc) for consumer %s",
                    len(ready_keys_before),
                    consumer_id,
                )
                if member_id is not None:
                    assert test_mpmc_id is not None
                    ready_keys_under_test = list(
                        etcd_client.get_prefix(_new_mpmc_ready_channels_prefix(test_mpmc_id))
                    )
                    logging.info(
                        "Before join: test_mpmc_id=%s ready_keys=%d, consumer %s mpmc_member_id=%s ready_keys_by_member=%d",
                        test_mpmc_id,
                        len(ready_keys_under_test),
                        consumer_id,
                        member_id,
                        _count_ready_keys_for_member_id(member_id),
                    )
                    logging.info(
                        "Before join: consumer %s mpmc_member_id=%s ready_keys_by_member=%d",
                        consumer_id,
                        member_id,
                        _count_ready_keys_for_member_id(member_id),
                    )
                
                proc.wait(timeout=1)
                if proc.returncode != 0:
                    raise RuntimeError(
                        f"{process_type} {consumer_id} failed with return code {proc.returncode}."
                        f" Check log file for details: {log_file}"
                    )
                print(f"{process_type} {consumer_id} completed successfully")
                print(f"Log file: {log_file}")
                joined_ids.add(consumer_id)

                # Give etcd a brief moment to propagate member-lease revoke and
                # delete ready keys written under that lease. This avoids a race
                # where a newly started consumer immediately sees a stale ready
                # key before the revoke/delete finishes. Not a fallback: the
                # underlying shutdown already performs delete+revoke; we only
                # Allow async revoke/delete tasks and watch events to settle, but keep it bounded.
                cleanup_timeout_s = float(int(CHAN_CONFIG_TEST["ttl_seconds"]))
                if member_id is not None:
                    leftover_keys = _wait_ready_keys_cleared_for_member_id(member_id, cleanup_timeout_s)
                else:
                    leftover_keys = []
                
                # Verify that ready keys have been deleted for this consumer after sleep
                ready_keys_after = list(etcd_client.get_prefix(_READY_CHANNELS_BASE_PREFIX))
                logging.info(
                    "After join and sleep: total ready keys=%d (all mpmc) for consumer %s",
                    len(ready_keys_after),
                    consumer_id,
                )
                if member_id is not None:
                    assert test_mpmc_id is not None
                    ready_keys_under_test_after = list(
                        etcd_client.get_prefix(_new_mpmc_ready_channels_prefix(test_mpmc_id))
                    )
                    left = _count_ready_keys_for_member_id(member_id)
                    logging.info(
                        "After join and sleep: consumer %s mpmc_member_id=%s ready_keys_by_member=%d",
                        consumer_id,
                        member_id,
                        left,
                    )
                    logging.info(
                        "Before recovery consumers start: stopped consumer %s mpmc_member_id=%s ready_keys_by_member=%d",
                        consumer_id,
                        member_id,
                        left,
                    )
                    logging.info(
                        "Before recovery consumers start: stopped consumer %s mpmc_member_id=%s leftover_ready_keys=%s",
                        consumer_id,
                        member_id,
                        leftover_keys,
                    )
                    logging.info(
                        "Before recovery consumers start: test_mpmc_id=%s ready_keys=%d",
                        test_mpmc_id,
                        len(ready_keys_under_test_after),
                    )
                    assert (
                        left == 0
                    ), (
                        "Ready keys for the stopped consumer must be cleared before starting new consumers. "
                        f"consumer_id={consumer_id}, mpmc_member_id={member_id}, "
                        f"ready_keys_by_member={left}, leftover_ready_keys={leftover_keys}"
                    )
                else:
                    raise RuntimeError(
                        f"Failed to parse mpmc_member_id from consumer log: consumer_id={consumer_id}, log_file={log_file}"
                    )

            # time.sleep(10)

            def debug_all_ready_channels() -> None:
                logging.info(
                    "debug_all_ready_channels after close consumers %s",
                    consumes_to_stop,
                )
                ready_chans_kvs = list(etcd_client.get_prefix(_READY_CHANNELS_BASE_PREFIX))
                for value, meta in ready_chans_kvs:
                    key = meta.key.decode()
                    mpmc_id = key.split("/")[-2]
                    mpsc_id = key.split("/")[-1]
                    logging.info(
                        "mpmc_id: %s mpsc_id: %s, mpmc_member_id: %s",
                        mpmc_id,
                        mpsc_id,
                        value.decode() if value else "",
                    )

            debug_all_ready_channels()

            recover_count = random.randint(
                len(consumes_to_stop), len(consumes_to_stop) + 3
            )
            logging.info(
                "consumers %s stopped, start to recover %s consumers",
                consumes_to_stop,
                recover_count,
            )
            for idx in range(recover_count):
                consumer_id = f"C{phase_name}_{idx}"
                processes.append(("consumer", consumer_process_cmd(consumer_id)))
                recovered_consumers.append(consumer_id)
            start_processes()
            time.sleep(5)
            _assert_test_mpmc_id_stable()

        join_timeout_s = int(TEST_TIMEOUT_SECONDS)

        # 1) Wait all producers to finish (or fail fast).
        wait_all_of_type("producer", timeout_s=join_timeout_s)

        # 2) Notify consumers that no more producers will publish; they will exit after idle timeout.
        etcd_client.put("/test_mpmc_stop_producer", b"dummy_value")

        # 3) Wait remaining consumers to drain and exit (or fail fast).
        wait_all_of_type("consumer", timeout_s=join_timeout_s)

        time.sleep(10)
        verify_production_consumption_counts(subprocesses)
        verify_exit_status(subprocesses)
        print("=== MPMC Dynamic Test PASSED ===")
    finally:
        etcd_client.close()


def verify_production_consumption_counts(
    subprocesses: List[Tuple[str, subprocess.Popen, str]]
) -> None:
    print("=== Verifying Production and Consumption Counts ===")
    total_produced = 0
    total_consumed = 0
    produced_messages = set()
    consumed_messages = set()

    def _clean_marker_unique_id(raw: str) -> str:
        before_escape = raw.split("\x1b", 1)[0]
        return before_escape.strip()

    for process_type, _, log_file in subprocesses:
        try:
            with open(log_file, "r", encoding="utf-8") as handle:
                for raw_line in handle:
                    line = raw_line.strip()
                    if line.startswith("PRODUCE_MARKER:"):
                        parts = line.split(": ", 1)
                        if len(parts) == 2 and ":" in parts[1]:
                            producer_id, unique_id = parts[1].split(":", 1)
                            total_produced += 1
                            produced_messages.add(_clean_marker_unique_id(unique_id))
                            print(
                                f"Found production marker ({process_type}):"
                                f" {producer_id}:{unique_id}"
                            )
                    elif line.startswith("CONSUME_MARKER:"):
                        parts = line.split(": ", 1)
                        if len(parts) == 2 and ":" in parts[1]:
                            consumer_id, unique_id = parts[1].split(":", 1)
                            total_consumed += 1
                            consumed_messages.add(_clean_marker_unique_id(unique_id))
                            print(
                                f"Found consumption marker ({process_type}):"
                                f" {consumer_id}:{unique_id}"
                            )
        except FileNotFoundError:
            print(f"Warning: Log file not found: {log_file}")
        except Exception as exc:  # noqa: BLE001
            print(f"Error reading log file {log_file}: {exc}")

    print(f"Total produced messages: {total_produced}")
    print(f"Total consumed messages: {total_consumed}")
    print(f"Unique produced messages: {len(produced_messages)}")
    print(f"Unique consumed messages: {len(consumed_messages)}")

    if len(produced_messages) != total_produced:
        print(
            f"WARNING: Found {total_produced - len(produced_messages)} duplicate produced messages"
        )
    if len(consumed_messages) != total_consumed:
        print(
            f"WARNING: Found {total_consumed - len(consumed_messages)} duplicate consumed messages"
        )

    unconsumed = produced_messages - consumed_messages
    if unconsumed:
        print(
            f"WARNING: {len(unconsumed)} messages were produced but not consumed: {unconsumed}"
        )

    unproduced = consumed_messages - produced_messages
    if unproduced:
        print(
            f"WARNING: {len(unproduced)} messages were consumed but not produced: {unproduced}"
        )

    assert total_produced > 0, "Total produced messages must be greater than 0"
    assert total_consumed > 0, "Total consumed messages must be greater than 0"

    if (
        total_produced == total_consumed
        and len(produced_messages) == len(consumed_messages)
        and not unconsumed
        and not unproduced
    ):
        print("✅ VERIFICATION PASSED: Production count equals consumption count")
    else:
        print("❌ VERIFICATION FAILED: Production count does not equal consumption count")
        raise AssertionError("Production and consumption counts do not match")


def verify_exit_status(
    subprocesses: List[Tuple[str, subprocess.Popen, str]]
) -> None:
    print("=== Verifying Exit Status ===")
    normal_exits: List[str] = []
    crashes: List[str] = []

    for process_type, _, log_file in subprocesses:
        try:
            with open(log_file, "r", encoding="utf-8") as handle:
                content = handle.read()
            for line in content.split("\n"):
                if line.startswith(PRODUCER_NORMAL_EXIT_MARKER):
                    producer_id = line.split(": ", 1)[1]
                    normal_exits.append(f"PRODUCER_{producer_id}")
                    print(f"Found normal exit: PRODUCER_{producer_id}")
                if line.startswith(CONSUMER_NORMAL_EXIT_MARKER):
                    consumer_id = line.split(": ", 1)[1]
                    normal_exits.append(f"CONSUMER_{consumer_id}")
                    print(f"Found normal exit: CONSUMER_{consumer_id}")
                if line.startswith(PRODUCER_CRASH_MARKER):
                    producer_id = line.split(": ", 1)[1]
                    crashes.append(f"PRODUCER_{producer_id}")
                    print(f"Found crash: PRODUCER_{producer_id}")
                if line.startswith(CONSUMER_CRASH_MARKER):
                    consumer_id = line.split(": ", 1)[1]
                    crashes.append(f"CONSUMER_{consumer_id}")
                    print(f"Found crash: CONSUMER_{consumer_id}")
        except FileNotFoundError:
            print(f"Warning: Log file not found: {log_file}")
        except Exception as exc:  # noqa: BLE001
            print(f"Error reading log file {log_file}: {exc}")

    print(f"Normal exits: {normal_exits}")
    print(f"Crashes: {crashes}")

    expected_processes = len(subprocesses)
    actual_markers = len(normal_exits) + len(crashes)
    if actual_markers == expected_processes:
        print("✅ EXIT STATUS VERIFICATION PASSED: All processes have exit markers")
    else:
        print(
            f"❌ EXIT STATUS VERIFICATION FAILED: Expected {expected_processes} processes, "
            f"found {actual_markers} markers"
        )
        missing = expected_processes - actual_markers
        print(f"Missing markers for {missing} processes")
        raise AssertionError("Not all processes have proper exit markers")


def _test_mpmc_dynamic_suite_once(prefetch: int) -> None:
    env = create_channel_env()
    args = argparse.Namespace(mode="main", prefetch=prefetch)
    run_main(env, args)
    release(env)


def test_mpmc_dynamic_suite() -> None:
    run_with_argmatrix(_test_mpmc_dynamic_suite_once)


def test_mpmc_get_data_prefetch_is_per_consumer_not_divided() -> None:
    calls: List[Tuple[int, Optional[int], int]] = []

    class _DummyInnerConsumer:
        def get_data(
            self,
            batch_size: int,
            try_time: Optional[int] = None,
            prefetch_num: int = 0,
        ) -> Result[List[Dict[str, object]], ApiError]:
            calls.append((batch_size, try_time, prefetch_num))
            return Result.new_ok([])

    consumer = object.__new__(MPMCChanConsumer)
    consumer.shutdown_ctl = mpsc.MqShutdownCtl()
    consumer.mpmc_id = "123"
    consumer.mpmc_channel = SimpleNamespace(
        _get_active_consumer_count=lambda: 8,
    )
    consumer.mpsc_consumer = _DummyInnerConsumer()

    res = consumer.get_data(batch_size=40, try_time=2, prefetch_num=40)

    assert res.is_ok()
    assert calls == [(40, 2, 40)]




if __name__ == "__main__":
    cli()
