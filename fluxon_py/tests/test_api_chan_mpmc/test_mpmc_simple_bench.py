from __future__ import annotations

import argparse
from enum import Enum
import json
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import etcd3


CURRENT_DIR = Path(__file__).resolve().parent


def main() -> None:
    args = _parse_args()

    if args.mode == "main":
        _run_main(args)
        return

    if args.mode == "run_producer":
        _run_producer(args)
        return

    if args.mode == "run_consumer":
        _run_consumer(args)
        return

    raise ValueError(f"Unsupported mode: {args.mode}")


def _find_project_root(start: Path) -> Path:
    for candidate in (start,) + tuple(start.parents):
        if (candidate / "setup.py").is_file():
            return candidate
    return start


PROJECT_ROOT = _find_project_root(CURRENT_DIR)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from fluxon_py import FluxonKvClientConfig, new_store  # noqa: E402
from fluxon_py.api_error import (  # noqa: E402
    ChannelClosedError,
    MessageConsumptionNoNewMessageError,
    ProducerClosedError,
)
from fluxon_py.api_ext_chan import ChanType  # noqa: E402
from fluxon_py.kvclient import KvClientType  # noqa: E402
from fluxon_py.kvclient.nonzerocopy_encode import DLPackBytesView  # noqa: E402
from fluxon_py.logging import init_logger  # noqa: E402
from fluxon_py.runtime import register_ctrlc_callback  # noqa: E402
from fluxon_py.tests.test_lib import (  # noqa: E402
    CHAN_CONFIG_TEST,
    ETCD_HOST,
    ETCD_PORT,
    KV_SVC_IP,
    KV_SVC_TYPE,
    MOONCAKE_MASTER_SERVER_ADDRESS,
    MOONCAKE_METADATA_SERVER,
    load_test_fluxon_cluster_name,
    load_test_fluxon_share_mem_path,
    new_test_consumer,
    new_test_producer,
    pre_kill_existing_test_processes_by_script_name,
    setup_test_environment,
)


SCRIPT_PATH = Path(__file__).resolve()
SCRIPT_BASENAME = SCRIPT_PATH.name
logging = init_logger()

PAYLOAD_BYTES = 7 * 1024 * 1024
MIN_BENCH_DURATION_SECONDS = 60
DEFAULT_DURATION_SECONDS = MIN_BENCH_DURATION_SECONDS
DEFAULT_SAMPLE_START_SECONDS = 10
DEFAULT_SAMPLE_DURATION_SECONDS = 10
DEFAULT_BATCH_SIZE = 10
DEFAULT_PREFETCH_NUM = 10
DEFAULT_PRODUCER_COUNT = 8
DEFAULT_CONSUMER_COUNTS = (1, 2, 4, 8)
DEFAULT_CHANNEL_CAPACITY = 2048
MOONCAKE_LOCAL_BUFFER_BYTES = 16_777_216 * 10
PRODUCER_TRY_SLEEP_SECONDS = 0.01
CONSUMER_IDLE_SLEEP_SECONDS = 0.02
GET_DATA_TRY_TIME_SECONDS = 2
CHANNEL_CONSTRUCTION_READY_WAIT_SECONDS = 1.0
SUMMARY_REPORT_INTERVAL_SECONDS = 0.5
SUMMARY_STARTUP_TIMEOUT_SECONDS = 30
SUMMARY_STOP_GRACE_SECONDS = 2.0
WORKER_EXIT_TIMEOUT_SECONDS = 60.0
STOP_KEY_PREFIX = "/test_mpmc_simple_bench/stop/"
SUMMARY_KEY_PREFIX = "/test_mpmc_simple_bench/summary/"
SharedBundle = str
PayloadFieldValue = bytes | DLPackBytesView
PayloadFields = dict[str, PayloadFieldValue]
SINGLE_FIELD_PAYLOAD_KEY = "payload"
FLATDICT6_DLPACK_FIELD_PREFIX = "payload_"
FLATDICT6_DLPACK_FIELD_COUNT = 6


class PayloadKind(str, Enum):
    BYTES = "bytes"
    DLPACK = "dlpack"
    FLATDICT6_DLPACK = "flatdict6_dlpack"

    def __str__(self) -> str:
        return self.value


PAYLOAD_KIND_CHOICES = tuple(kind.value for kind in PayloadKind)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MPMC simple throughput bench")
    subparsers = parser.add_subparsers(dest="mode", help="Execution mode")

    main_parser = subparsers.add_parser("main", help="Run the benchmark matrix")
    main_parser.add_argument("--duration-seconds", type=int, required=False, default=DEFAULT_DURATION_SECONDS)
    main_parser.add_argument(
        "--sample-start-seconds",
        type=int,
        required=False,
        default=DEFAULT_SAMPLE_START_SECONDS,
    )
    main_parser.add_argument(
        "--sample-duration-seconds",
        type=int,
        required=False,
        default=DEFAULT_SAMPLE_DURATION_SECONDS,
    )
    main_parser.add_argument("--payload-bytes", type=int, required=False, default=PAYLOAD_BYTES)
    main_parser.add_argument("--producer-count", type=int, required=False, default=DEFAULT_PRODUCER_COUNT)
    main_parser.add_argument(
        "--consumer-counts",
        type=str,
        required=False,
        default=",".join(str(v) for v in DEFAULT_CONSUMER_COUNTS),
    )
    main_parser.add_argument(
        "--payload-kind",
        type=str,
        required=False,
        choices=PAYLOAD_KIND_CHOICES,
        default=PayloadKind.BYTES.value,
    )
    main_parser.add_argument("--batch-size", type=int, required=False, default=DEFAULT_BATCH_SIZE)
    main_parser.add_argument("--prefetch-num", type=int, required=False, default=DEFAULT_PREFETCH_NUM)
    main_parser.add_argument("--channel-capacity", type=int, required=False, default=DEFAULT_CHANNEL_CAPACITY)
    main_parser.add_argument("--share-mem-paths", type=str, required=False)
    producer_parser = subparsers.add_parser("run_producer", help="Run one producer worker")
    producer_parser.add_argument("--backend-type", required=True, type=str)
    producer_parser.add_argument("--ip", required=True, type=str)
    producer_parser.add_argument("--bench-id", required=True, type=str)
    producer_parser.add_argument("--producer-id", required=True, type=str)
    producer_parser.add_argument("--chan-id", required=True, type=str)
    producer_parser.add_argument("--payload-bytes", required=True, type=int)
    producer_parser.add_argument("--payload-kind", required=True, type=str, choices=PAYLOAD_KIND_CHOICES)
    producer_parser.add_argument("--channel-capacity", required=True, type=int)
    producer_parser.add_argument("--share-mem-path", required=False, type=str)
    producer_parser.add_argument("--stop-key", required=True, type=str)

    consumer_parser = subparsers.add_parser("run_consumer", help="Run one consumer worker")
    consumer_parser.add_argument("--backend-type", required=True, type=str)
    consumer_parser.add_argument("--ip", required=True, type=str)
    consumer_parser.add_argument("--bench-id", required=True, type=str)
    consumer_parser.add_argument("--consumer-id", required=True, type=str)
    consumer_parser.add_argument("--chan-id", required=True, type=str)
    consumer_parser.add_argument("--batch-size", required=True, type=int)
    consumer_parser.add_argument("--payload-bytes", required=True, type=int)
    consumer_parser.add_argument("--payload-kind", required=True, type=str, choices=PAYLOAD_KIND_CHOICES)
    consumer_parser.add_argument("--prefetch-num", required=True, type=int)
    consumer_parser.add_argument("--channel-capacity", required=True, type=int)
    consumer_parser.add_argument("--share-mem-path", required=False, type=str)
    consumer_parser.add_argument("--stop-key", required=True, type=str)
    consumer_parser.add_argument("--summary-key", required=True, type=str)
    return parser


def _parse_args() -> argparse.Namespace:
    parser = _build_parser()
    argv = sys.argv[1:]
    if len(argv) == 0:
        argv = ["main"]
    elif argv[0] not in {"main", "run_producer", "run_consumer"}:
        argv = ["main", *argv]
    return parser.parse_args(argv)


def _run_main(args: argparse.Namespace) -> None:
    os.environ["TEST_MPMC"] = "1"
    setup_test_environment(logging)
    pre_kill_existing_test_processes_by_script_name(SCRIPT_BASENAME, 10)
    _validate_main_args(args)
    consumer_counts = _parse_consumer_counts(args.consumer_counts)
    shared_bundles = _parse_shared_bundles(
        share_mem_paths_raw=args.share_mem_paths,
    )
    for consumer_count in consumer_counts:
        _run_one_case(
            producer_count=int(args.producer_count),
            consumer_count=consumer_count,
            payload_bytes=int(args.payload_bytes),
            payload_kind=_parse_payload_kind(args.payload_kind),
            duration_seconds=int(args.duration_seconds),
            sample_start_seconds=int(args.sample_start_seconds),
            sample_duration_seconds=int(args.sample_duration_seconds),
            batch_size=int(args.batch_size),
            prefetch_num=int(args.prefetch_num),
            channel_capacity=int(args.channel_capacity),
            shared_bundles=shared_bundles,
        )


def _run_one_case(
    *,
    producer_count: int,
    consumer_count: int,
    payload_bytes: int,
    payload_kind: PayloadKind,
    duration_seconds: int,
    sample_start_seconds: int,
    sample_duration_seconds: int,
    batch_size: int,
    prefetch_num: int,
    channel_capacity: int,
    shared_bundles: tuple[SharedBundle, ...],
) -> None:
    effective_payload_bytes = _payload_message_bytes(
        payload_kind=payload_kind,
        payload_bytes=payload_bytes,
    )
    bench_id = (
        f"mpmc_simple_bench_"
        f"p{producer_count}_c{consumer_count}_pk{payload_kind.value}_b{batch_size}_pf{prefetch_num}_"
        f"{uuid.uuid4().hex}"
    )
    stop_key = f"{STOP_KEY_PREFIX}{bench_id}"
    logging.info(
        "[bench] start case bench_id=%s producers=%s consumers=%s payload_bytes=%s effective_payload_bytes=%s payload_kind=%s duration_seconds=%s sample_start_seconds=%s sample_duration_seconds=%s batch_size=%s prefetch_num=%s channel_capacity=%s",
        bench_id,
        producer_count,
        consumer_count,
        payload_bytes,
        effective_payload_bytes,
        payload_kind.value,
        duration_seconds,
        sample_start_seconds,
        sample_duration_seconds,
        batch_size,
        prefetch_num,
        channel_capacity,
    )
    _clear_etcd_prefix(f"{SUMMARY_KEY_PREFIX}{bench_id}/")
    bootstrap_bundle = shared_bundles[0]
    bootstrap_store = _new_channel_store(
        role_key=f"{bench_id}_bootstrap",
        backend_type=KV_SVC_TYPE,
        share_mem_path=bootstrap_bundle,
    )
    bootstrap_producer = None
    worker_processes: list[subprocess.Popen[str]] = []
    summary_keys = [f"{SUMMARY_KEY_PREFIX}{bench_id}/{idx}" for idx in range(consumer_count)]
    sample_begin_summaries: list[dict[str, Any]] | None = None
    sample_end_summaries: list[dict[str, Any]] | None = None
    sampled_summaries: list[dict[str, Any]] | None = None
    try:
        _wait_for_channel_construction_rpc_ready(role="bootstrap_producer")
        bootstrap_producer = new_test_producer(
            "cstyle",
            bootstrap_store,
            None,
            _new_channel_config(capacity=int(channel_capacity)),
            None,
            ChanType.MPMC,
        )
        chan_id = bootstrap_producer.get_chan_id()
        if not isinstance(chan_id, str) or not chan_id.isdigit():
            raise ValueError(f"invalid bootstrap chan_id: {chan_id!r}")
        logging.info("[bench] bootstrap MPMC channel created bench_id=%s chan_id=%s", bench_id, chan_id)
        for producer_idx in range(producer_count):
            producer_bundle = _select_shared_bundle(shared_bundles, producer_idx)
            worker_processes.append(
                _spawn_producer(
                    bench_id=bench_id,
                    producer_id=str(producer_idx),
                    chan_id=chan_id,
                    payload_bytes=payload_bytes,
                    payload_kind=payload_kind,
                    channel_capacity=channel_capacity,
                    share_mem_path=producer_bundle,
                    stop_key=stop_key,
                )
            )
        for consumer_idx, summary_key in enumerate(summary_keys):
            consumer_bundle = _select_shared_bundle(shared_bundles, consumer_idx)
            worker_processes.append(
                _spawn_worker(
                    [
                        sys.executable,
                        str(SCRIPT_PATH),
                        "run_consumer",
                        "--backend-type",
                        KV_SVC_TYPE,
                        "--ip",
                        KV_SVC_IP,
                        "--bench-id",
                        bench_id,
                        "--consumer-id",
                        str(consumer_idx),
                        "--chan-id",
                        chan_id,
                        "--batch-size",
                        str(batch_size),
                        "--payload-bytes",
                        str(payload_bytes),
                        "--payload-kind",
                        payload_kind.value,
                        "--prefetch-num",
                        str(prefetch_num),
                        "--channel-capacity",
                        str(channel_capacity),
                        "--share-mem-path",
                        consumer_bundle,
                        "--stop-key",
                        stop_key,
                        "--summary-key",
                        summary_key,
                    ]
                )
            )

        _wait_for_summaries(summary_keys, timeout_seconds=SUMMARY_STARTUP_TIMEOUT_SECONDS)

        case_start_monotonic = time.monotonic()
        sample_begin_deadline = case_start_monotonic + float(sample_start_seconds)
        sample_end_deadline = sample_begin_deadline + float(sample_duration_seconds)
        case_end_deadline = case_start_monotonic + float(duration_seconds)

        _sleep_until(sample_begin_deadline)
        sample_begin_summaries = _load_summaries(summary_keys)
        logging.info("[bench] sample begin snapshot captured bench_id=%s", bench_id)

        _sleep_until(sample_end_deadline)
        sample_end_summaries = _load_summaries(summary_keys)
        logging.info("[bench] sample end snapshot captured bench_id=%s", bench_id)
        if sample_begin_summaries is None or sample_end_summaries is None:
            raise RuntimeError(f"sample snapshots must be captured before shutdown: bench_id={bench_id}")
        sampled_summaries = _build_sampled_summaries(
            sample_begin_summaries=sample_begin_summaries,
            sample_end_summaries=sample_end_summaries,
            sample_duration_seconds=sample_duration_seconds,
        )

        _sleep_until(case_end_deadline)
        # The sampled snapshots are the benchmark authority, so emit them before teardown.
        if sampled_summaries is None:
            raise RuntimeError(f"sampled summaries must be ready before case shutdown: bench_id={bench_id}")
        _print_case_summary(
            producer_count=producer_count,
            consumer_count=consumer_count,
            payload_bytes=payload_bytes,
            effective_payload_bytes=effective_payload_bytes,
            payload_kind=payload_kind,
            total_duration_seconds=duration_seconds,
            sample_start_seconds=sample_start_seconds,
            sample_duration_seconds=sample_duration_seconds,
            batch_size=batch_size,
            prefetch_num=prefetch_num,
            summaries=sampled_summaries,
        )
        _put_etcd_key(stop_key, b"1")
        time.sleep(SUMMARY_STOP_GRACE_SECONDS)
        _signal_live_processes(worker_processes, signum=signal.SIGINT)
        try:
            _wait_for_processes_exit(worker_processes, timeout_seconds=WORKER_EXIT_TIMEOUT_SECONDS)
        except RuntimeError as err:
            logging.warning("[bench] worker shutdown timeout bench_id=%s error=%s", bench_id, err)
        else:
            _warn_if_worker_exited_nonzero(worker_processes, bench_id=bench_id)
    finally:
        _terminate_processes(worker_processes)
        _delete_etcd_key(stop_key)
        _clear_etcd_prefix(f"{SUMMARY_KEY_PREFIX}{bench_id}/")
        if bootstrap_producer is not None:
            _best_effort_close(bootstrap_producer, role="bootstrap_producer")
        _best_effort_close(bootstrap_store, role="bootstrap_store")


def _run_producer(args: argparse.Namespace) -> None:
    os.environ["TEST_MPMC"] = "1"
    setup_test_environment(logging)
    if not isinstance(args.chan_id, str) or not args.chan_id.isdigit():
        raise ValueError(f"chan_id must be digit-only string, got {args.chan_id!r}")
    _validate_positive_int("payload_bytes", args.payload_bytes)
    _validate_positive_int("channel_capacity", args.channel_capacity)
    payload_kind = _parse_payload_kind(args.payload_kind)
    store = _new_channel_store(
        role_key=f"{args.bench_id}_producer_{args.producer_id}",
        backend_type=args.backend_type,
        share_mem_path=args.share_mem_path,
    )
    producer = None
    restore_signal_listener = None
    shutdown_requested = threading.Event()
    shutdown_notified = False
    payload_fields = _new_payload_fields(
        payload_kind=payload_kind,
        payload_bytes=int(args.payload_bytes),
    )
    produced_messages = 0
    with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
        try:
            _wait_for_channel_construction_rpc_ready(role=f"producer_{args.producer_id}")
            producer = new_test_producer(
                "bind",
                store,
                args.chan_id,
                _new_channel_config(capacity=int(args.channel_capacity)),
                None,
                ChanType.MPMC,
            )
            def _on_ctrlc(reason: str) -> None:
                nonlocal shutdown_notified
                shutdown_requested.set()
                if shutdown_notified:
                    return
                shutdown_notified = True
                logging.info(
                    "[bench producer %s] caught %s, requesting shutdown...",
                    args.producer_id,
                    reason,
                )
                if producer is not None:
                    producer.request_shutdown()

            restore_signal_listener = register_ctrlc_callback(
                _on_ctrlc,
                thread_name=f"bench-producer-signal-{args.producer_id}",
            )
            while not shutdown_requested.is_set() and not _stop_requested(etcd_client, args.stop_key):
                result = producer.put_data(
                    _new_message_fields(
                        producer_id=str(args.producer_id),
                        sequence=produced_messages,
                        payload_fields=payload_fields,
                    )
                )
                if result.is_ok():
                    _ = result.unwrap()
                    produced_messages += 1
                    continue

                err = result.unwrap_error()
                if isinstance(err, ProducerClosedError):
                    break
                if shutdown_requested.is_set() or _stop_requested(etcd_client, args.stop_key):
                    break
                logging.warning("[bench producer %s] put_data error: %s", args.producer_id, err)
                time.sleep(PRODUCER_TRY_SLEEP_SECONDS)
        finally:
            if restore_signal_listener is not None:
                restore_signal_listener()
            if producer is not None:
                _best_effort_close(producer, role=f"producer_{args.producer_id}")
            _best_effort_close(store, role=f"producer_store_{args.producer_id}")


def _run_consumer(args: argparse.Namespace) -> None:
    os.environ["TEST_MPMC"] = "1"
    setup_test_environment(logging)
    if not isinstance(args.chan_id, str) or not args.chan_id.isdigit():
        raise ValueError(f"chan_id must be digit-only string, got {args.chan_id!r}")
    _validate_positive_int("batch_size", args.batch_size)
    _validate_positive_int("payload_bytes", args.payload_bytes)
    _validate_non_negative_int("prefetch_num", args.prefetch_num)
    _validate_positive_int("channel_capacity", args.channel_capacity)
    payload_kind = _parse_payload_kind(args.payload_kind)
    store = _new_channel_store(
        role_key=f"{args.bench_id}_consumer_{args.consumer_id}",
        backend_type=args.backend_type,
        share_mem_path=args.share_mem_path,
    )
    consumer = None
    restore_signal_listener = None
    shutdown_requested = threading.Event()
    shutdown_notified = False
    consumed_messages = 0
    consumed_bytes = 0
    started_at = time.time()
    stopped_by_signal = False
    last_summary_report_at = 0.0
    with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
        try:
            _wait_for_channel_construction_rpc_ready(role=f"consumer_{args.consumer_id}")
            consumer = new_test_consumer(
                "bind",
                store,
                args.chan_id,
                _new_channel_config(capacity=int(args.channel_capacity)),
                None,
                ChanType.MPMC,
            )
            def _on_ctrlc(reason: str) -> None:
                nonlocal shutdown_notified, stopped_by_signal
                shutdown_requested.set()
                stopped_by_signal = True
                if shutdown_notified:
                    return
                shutdown_notified = True
                logging.info(
                    "[bench consumer %s] caught %s, requesting shutdown...",
                    args.consumer_id,
                    reason,
                )
                if consumer is not None:
                    consumer.request_shutdown()

            restore_signal_listener = register_ctrlc_callback(
                _on_ctrlc,
                thread_name=f"bench-consumer-signal-{args.consumer_id}",
            )
            _write_consumer_summary(
                etcd_client=etcd_client,
                summary_key=args.summary_key,
                consumer_id=str(args.consumer_id),
                consumed_messages=consumed_messages,
                consumed_bytes=consumed_bytes,
                started_at=started_at,
                stopped_by_signal=stopped_by_signal,
            )
            last_summary_report_at = time.time()
            while True:
                if shutdown_requested.is_set():
                    stopped_by_signal = True
                    break
                result = consumer.get_data(
                    batch_size=int(args.batch_size),
                    try_time=GET_DATA_TRY_TIME_SECONDS,
                    prefetch_num=int(args.prefetch_num),
                )
                stop_requested = shutdown_requested.is_set() or _stop_requested(etcd_client, args.stop_key)
                if result.is_ok():
                    batch = result.unwrap()
                    if len(batch) == 0:
                        if stop_requested:
                            stopped_by_signal = True
                            break
                        last_summary_report_at = _maybe_write_consumer_summary(
                            etcd_client=etcd_client,
                            summary_key=args.summary_key,
                            consumer_id=str(args.consumer_id),
                            consumed_messages=consumed_messages,
                            consumed_bytes=consumed_bytes,
                            started_at=started_at,
                            stopped_by_signal=stopped_by_signal,
                            last_report_at=last_summary_report_at,
                        )
                        time.sleep(CONSUMER_IDLE_SLEEP_SECONDS)
                        continue
                    for item in batch:
                        consumed_bytes += _consume_payload_fields(
                            item=item,
                            payload_kind=payload_kind,
                            payload_bytes=int(args.payload_bytes),
                        )
                        consumed_messages += 1
                    last_summary_report_at = _maybe_write_consumer_summary(
                        etcd_client=etcd_client,
                        summary_key=args.summary_key,
                        consumer_id=str(args.consumer_id),
                        consumed_messages=consumed_messages,
                        consumed_bytes=consumed_bytes,
                        started_at=started_at,
                        stopped_by_signal=stopped_by_signal,
                        last_report_at=last_summary_report_at,
                    )
                    if stop_requested:
                        # Stop the bench promptly after the current returned batch.
                        # Draining the entire backlog can make the parent summary wait
                        # exceed the fixed-case timeout in multi-case runs.
                        stopped_by_signal = True
                        break
                    continue

                err = result.unwrap_error()
                if isinstance(err, MessageConsumptionNoNewMessageError):
                    if stop_requested:
                        stopped_by_signal = True
                        break
                    last_summary_report_at = _maybe_write_consumer_summary(
                        etcd_client=etcd_client,
                        summary_key=args.summary_key,
                        consumer_id=str(args.consumer_id),
                        consumed_messages=consumed_messages,
                        consumed_bytes=consumed_bytes,
                        started_at=started_at,
                        stopped_by_signal=stopped_by_signal,
                        last_report_at=last_summary_report_at,
                    )
                    time.sleep(CONSUMER_IDLE_SLEEP_SECONDS)
                    continue
                if isinstance(err, ChannelClosedError):
                    if stop_requested:
                        stopped_by_signal = True
                    break
                if stop_requested:
                    stopped_by_signal = True
                    break
                logging.warning("[bench consumer %s] get_data error: %s", args.consumer_id, err)
                last_summary_report_at = _maybe_write_consumer_summary(
                    etcd_client=etcd_client,
                    summary_key=args.summary_key,
                    consumer_id=str(args.consumer_id),
                    consumed_messages=consumed_messages,
                    consumed_bytes=consumed_bytes,
                    started_at=started_at,
                    stopped_by_signal=stopped_by_signal,
                    last_report_at=last_summary_report_at,
                )
                time.sleep(CONSUMER_IDLE_SLEEP_SECONDS)
        finally:
            if restore_signal_listener is not None:
                restore_signal_listener()
            _write_consumer_summary(
                etcd_client=etcd_client,
                summary_key=args.summary_key,
                consumer_id=str(args.consumer_id),
                consumed_messages=consumed_messages,
                consumed_bytes=consumed_bytes,
                started_at=started_at,
                stopped_by_signal=stopped_by_signal,
            )
            if consumer is not None:
                _best_effort_close(consumer, role=f"consumer_{args.consumer_id}")
            _best_effort_close(store, role=f"consumer_store_{args.consumer_id}")


def _validate_main_args(args: argparse.Namespace) -> None:
    _validate_min_int("duration_seconds", args.duration_seconds, minimum=MIN_BENCH_DURATION_SECONDS)
    _validate_non_negative_int("sample_start_seconds", args.sample_start_seconds)
    _validate_positive_int("sample_duration_seconds", args.sample_duration_seconds)
    _validate_positive_int("payload_bytes", args.payload_bytes)
    _ = _parse_payload_kind(args.payload_kind)
    _validate_positive_int("producer_count", args.producer_count)
    _validate_positive_int("batch_size", args.batch_size)
    _validate_non_negative_int("prefetch_num", args.prefetch_num)
    _validate_positive_int("channel_capacity", args.channel_capacity)
    _parse_shared_bundles(
        share_mem_paths_raw=args.share_mem_paths,
    )
    _validate_sample_window(
        total_duration_seconds=int(args.duration_seconds),
        sample_start_seconds=int(args.sample_start_seconds),
        sample_duration_seconds=int(args.sample_duration_seconds),
    )


def _parse_consumer_counts(raw: str) -> tuple[int, ...]:
    values: list[int] = []
    for part in raw.split(","):
        stripped = part.strip()
        if stripped == "":
            raise ValueError("consumer-counts must not contain empty items")
        value = int(stripped)
        if value <= 0:
            raise ValueError(f"consumer count must be > 0, got {value}")
        values.append(value)
    if len(values) == 0:
        raise ValueError("consumer-counts must not be empty")
    return tuple(values)


def _validate_positive_int(name: str, value: int) -> None:
    if int(value) <= 0:
        raise ValueError(f"{name} must be > 0, got {value}")


def _validate_min_int(name: str, value: int, *, minimum: int) -> None:
    if int(value) < int(minimum):
        raise ValueError(f"{name} must be >= {minimum}, got {value}")


def _validate_non_negative_int(name: str, value: int) -> None:
    if int(value) < 0:
        raise ValueError(f"{name} must be >= 0, got {value}")


def _wait_for_channel_construction_rpc_ready(*, role: str) -> None:
    logging.info(
        "[bench] waiting channel construction RPC ready role=%s seconds=%s",
        role,
        CHANNEL_CONSTRUCTION_READY_WAIT_SECONDS,
    )
    time.sleep(CHANNEL_CONSTRUCTION_READY_WAIT_SECONDS)


def _new_channel_config(*, capacity: int) -> dict[str, int]:
    ttl_seconds = int(CHAN_CONFIG_TEST["ttl_seconds"])
    weight = int(CHAN_CONFIG_TEST["weight"])
    return {
        "capacity": int(capacity),
        "ttl_seconds": ttl_seconds,
        "weight": weight,
    }


def _new_channel_store(
    *,
    role_key: str,
    backend_type: str,
    share_mem_path: str | None,
):
    config = _new_store_config(
        instance_key=role_key,
        backend_type=backend_type,
        share_mem_path=share_mem_path,
    )
    result = new_store(config)
    if not result.is_ok():
        raise RuntimeError(f"new channel store failed: {result.unwrap_error()}")
    return result.unwrap()


def _new_store_config(
    *,
    instance_key: str,
    backend_type: str,
    share_mem_path: str | None,
) -> FluxonKvClientConfig:
    if backend_type == KvClientType.MOONCAKE.value:
        return FluxonKvClientConfig(
            {
                "instance_key": instance_key,
                "contribute_to_cluster_pool_size": {
                    "dram": 0,
                    "vram": {},
                },
                "mooncake_spec": {
                    "local_buffer_size": MOONCAKE_LOCAL_BUFFER_BYTES,
                    "metadata_server": MOONCAKE_METADATA_SERVER,
                    "master_server_address": MOONCAKE_MASTER_SERVER_ADDRESS,
                    "etcd_addresses": [f"{ETCD_HOST}:{ETCD_PORT}"],
                },
            }
        )

    if backend_type == KvClientType.FLUXON.value:
        resolved_share_mem_path = _resolve_fluxon_shared_bundle(
            share_mem_path=share_mem_path,
        )
        fluxon_spec: dict[str, Any] = {
            "cluster_name": load_test_fluxon_cluster_name(),
            "share_mem_path": resolved_share_mem_path,
        }
        return FluxonKvClientConfig(
            {
                "instance_key": instance_key,
                "contribute_to_cluster_pool_size": {
                    "dram": 0,
                    "vram": {},
                },
                "fluxonkv_spec": fluxon_spec,
            }
        )

    raise ValueError(f"Unsupported backend type: {backend_type}")


def _spawn_worker(cmd: list[str]) -> subprocess.Popen[str]:
    return subprocess.Popen(
        cmd,
        cwd=str(PROJECT_ROOT),
        stdout=sys.stdout,
        stderr=sys.stderr,
        text=True,
    )


def _spawn_producer(
    *,
    bench_id: str,
    producer_id: str,
    chan_id: str,
    payload_bytes: int,
    payload_kind: PayloadKind,
    channel_capacity: int,
    share_mem_path: str,
    stop_key: str,
) -> subprocess.Popen[str]:
    return _spawn_worker(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "run_producer",
            "--backend-type",
            KV_SVC_TYPE,
            "--ip",
            KV_SVC_IP,
            "--bench-id",
            bench_id,
            "--producer-id",
            producer_id,
            "--chan-id",
            chan_id,
            "--payload-bytes",
            str(payload_bytes),
            "--payload-kind",
            payload_kind.value,
            "--channel-capacity",
            str(channel_capacity),
            "--share-mem-path",
            share_mem_path,
            "--stop-key",
            stop_key,
        ]
    )


def _parse_shared_bundles(
    *,
    share_mem_paths_raw: str | None,
) -> tuple[SharedBundle, ...]:
    if share_mem_paths_raw is None:
        return (load_test_fluxon_share_mem_path(),)
    return _parse_csv_paths(raw=share_mem_paths_raw, arg_name="share-mem-paths")


def _parse_csv_paths(*, raw: str, arg_name: str) -> tuple[str, ...]:
    values: list[str] = []
    for part in str(raw).split(","):
        stripped = part.strip()
        if stripped == "":
            raise ValueError(f"{arg_name} must not contain empty items")
        values.append(stripped)
    if len(values) == 0:
        raise ValueError(f"{arg_name} must not be empty")
    return tuple(values)


def _select_shared_bundle(shared_bundles: tuple[SharedBundle, ...], worker_idx: int) -> SharedBundle:
    if len(shared_bundles) == 0:
        raise ValueError("shared_bundles must not be empty")
    return shared_bundles[int(worker_idx) % len(shared_bundles)]


def _resolve_fluxon_shared_bundle(
    *,
    share_mem_path: str | None,
) -> SharedBundle:
    if share_mem_path is None:
        raise ValueError("fluxon backend requires explicit share_mem_path for each worker")
    resolved_share_mem_path = str(share_mem_path).strip()
    if resolved_share_mem_path == "":
        raise ValueError("share_mem_path must be a non-empty string")
    return resolved_share_mem_path


def _terminate_processes(processes: list[subprocess.Popen[str]]) -> None:
    for proc in processes:
        if proc.poll() is not None:
            continue
        proc.terminate()
    for proc in processes:
        if proc.poll() is not None:
            continue
        try:
            proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5.0)


def _signal_live_processes(processes: list[subprocess.Popen[str]], *, signum: int) -> None:
    for proc in processes:
        if proc.poll() is not None:
            continue
        try:
            proc.send_signal(signum)
        except ProcessLookupError:
            continue


def _wait_for_processes_exit(processes: list[subprocess.Popen[str]], *, timeout_seconds: float) -> None:
    deadline = time.time() + float(timeout_seconds)
    while True:
        alive = [proc for proc in processes if proc.poll() is None]
        if len(alive) == 0:
            return
        if time.time() >= deadline:
            alive_pids = ",".join(str(proc.pid) for proc in alive)
            raise RuntimeError(
                f"Timed out waiting workers to exit after {timeout_seconds}s: pids={alive_pids}"
            )
        time.sleep(0.2)


def _wait_for_summaries(summary_keys: list[str], *, timeout_seconds: int) -> list[dict[str, Any]]:
    deadline = time.time() + float(timeout_seconds)
    pending = set(summary_keys)
    with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
        while pending:
            for key in list(pending):
                value, _ = etcd_client.get(key)
                if value is None:
                    continue
                pending.remove(key)
            if not pending:
                break
            if time.time() >= deadline:
                raise RuntimeError(f"Timed out waiting consumer summaries: {sorted(pending)}")
            time.sleep(0.5)
    return _load_summaries(summary_keys)


def _load_summaries(summary_keys: list[str]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
        for key in summary_keys:
            value, _ = etcd_client.get(key)
            if value is None:
                raise RuntimeError(f"Missing consumer summary: {key}")
            loaded = json.loads(value.decode("utf-8"))
            if not isinstance(loaded, dict):
                raise TypeError(f"summary at {key} must be dict")
            summaries.append(loaded)
    summaries.sort(key=lambda item: str(item["consumer_id"]))
    return summaries


def _build_sampled_summaries(
    *,
    sample_begin_summaries: list[dict[str, Any]],
    sample_end_summaries: list[dict[str, Any]],
    sample_duration_seconds: int,
) -> list[dict[str, Any]]:
    begin_by_consumer_id = _index_summaries_by_consumer_id(sample_begin_summaries)
    end_by_consumer_id = _index_summaries_by_consumer_id(sample_end_summaries)
    if begin_by_consumer_id.keys() != end_by_consumer_id.keys():
        raise RuntimeError(
            "sample snapshot consumer set mismatch: "
            f"begin={sorted(begin_by_consumer_id.keys())} end={sorted(end_by_consumer_id.keys())}"
        )
    sampled_summaries: list[dict[str, Any]] = []
    for consumer_id in sorted(begin_by_consumer_id.keys()):
        begin_summary = begin_by_consumer_id[consumer_id]
        end_summary = end_by_consumer_id[consumer_id]
        consumed_messages = int(end_summary["consumed_messages"]) - int(begin_summary["consumed_messages"])
        consumed_bytes = int(end_summary["consumed_bytes"]) - int(begin_summary["consumed_bytes"])
        if consumed_messages < 0 or consumed_bytes < 0:
            raise RuntimeError(
                "sample snapshot counters must be monotonic: "
                f"consumer_id={consumer_id} begin={begin_summary} end={end_summary}"
            )
        sampled_summaries.append(
            {
                "consumer_id": consumer_id,
                "consumed_messages": consumed_messages,
                "consumed_bytes": consumed_bytes,
                "elapsed_seconds": float(sample_duration_seconds),
                "stopped_by_signal": bool(end_summary["stopped_by_signal"]),
            }
        )
    return sampled_summaries


def _index_summaries_by_consumer_id(summaries: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for summary in summaries:
        consumer_id = str(summary["consumer_id"])
        if consumer_id in indexed:
            raise RuntimeError(f"duplicate consumer summary: consumer_id={consumer_id}")
        indexed[consumer_id] = summary
    return indexed


def _warn_if_worker_exited_nonzero(processes: list[subprocess.Popen[str]], *, bench_id: str) -> None:
    for proc in processes:
        return_code = proc.poll()
        if return_code is None:
            continue
        if return_code != 0:
            logging.warning(
                "[bench] worker exited non-zero during teardown bench_id=%s pid=%s code=%s",
                bench_id,
                proc.pid,
                return_code,
            )


def _maybe_write_consumer_summary(
    *,
    etcd_client: etcd3.Etcd3Client,
    summary_key: str,
    consumer_id: str,
    consumed_messages: int,
    consumed_bytes: int,
    started_at: float,
    stopped_by_signal: bool,
    last_report_at: float,
) -> float:
    now = time.time()
    if now - last_report_at < SUMMARY_REPORT_INTERVAL_SECONDS:
        return last_report_at
    _write_consumer_summary(
        etcd_client=etcd_client,
        summary_key=summary_key,
        consumer_id=consumer_id,
        consumed_messages=consumed_messages,
        consumed_bytes=consumed_bytes,
        started_at=started_at,
        stopped_by_signal=stopped_by_signal,
    )
    return now


def _write_consumer_summary(
    *,
    etcd_client: etcd3.Etcd3Client,
    summary_key: str,
    consumer_id: str,
    consumed_messages: int,
    consumed_bytes: int,
    started_at: float,
    stopped_by_signal: bool,
) -> None:
    summary = {
        "consumer_id": consumer_id,
        "consumed_messages": int(consumed_messages),
        "consumed_bytes": int(consumed_bytes),
        "elapsed_seconds": max(time.time() - started_at, 0.0),
        "stopped_by_signal": bool(stopped_by_signal),
    }
    etcd_client.put(summary_key, json.dumps(summary).encode("utf-8"))


def _print_case_summary(
    *,
    producer_count: int,
    consumer_count: int,
    payload_bytes: int,
    effective_payload_bytes: int,
    payload_kind: PayloadKind,
    total_duration_seconds: int,
    sample_start_seconds: int,
    sample_duration_seconds: int,
    batch_size: int,
    prefetch_num: int,
    summaries: list[dict[str, Any]],
) -> None:
    total_messages = sum(int(item["consumed_messages"]) for item in summaries)
    total_bytes = sum(int(item["consumed_bytes"]) for item in summaries)
    throughput_mb_s = total_bytes / float(1024 * 1024) / float(sample_duration_seconds)
    print(
        (
            "BENCH_SUMMARY "
            f"producers={producer_count} "
            f"consumers={consumer_count} "
            f"payload_bytes={payload_bytes} "
            f"effective_payload_bytes={effective_payload_bytes} "
            f"payload_kind={payload_kind.value} "
            f"total_duration_seconds={total_duration_seconds} "
            f"sample_start_seconds={sample_start_seconds} "
            f"sample_duration_seconds={sample_duration_seconds} "
            f"batch_size={batch_size} "
            f"prefetch_num={prefetch_num} "
            f"sampled_messages={total_messages} "
            f"sampled_bytes={total_bytes} "
            f"throughput_mb_s={throughput_mb_s:.2f}"
        ),
        flush=True,
    )
    for item in summaries:
        consumer_elapsed = float(item["elapsed_seconds"])
        consumer_bytes = int(item["consumed_bytes"])
        consumer_mb_s = consumer_bytes / float(1024 * 1024) / max(consumer_elapsed, 0.001)
        print(
            (
                "BENCH_CONSUMER "
                f"consumer_id={item['consumer_id']} "
                f"sampled_messages={item['consumed_messages']} "
                f"sampled_bytes={consumer_bytes} "
                f"sampled_elapsed_seconds={consumer_elapsed:.2f} "
                f"throughput_mb_s={consumer_mb_s:.2f} "
                f"stopped_by_signal={item['stopped_by_signal']}"
            ),
            flush=True,
        )


def _validate_sample_window(
    *,
    total_duration_seconds: int,
    sample_start_seconds: int,
    sample_duration_seconds: int,
) -> None:
    sample_end_seconds = int(sample_start_seconds) + int(sample_duration_seconds)
    if sample_end_seconds > int(total_duration_seconds):
        raise ValueError(
            "sample window must fit inside total duration: "
            f"total_duration_seconds={total_duration_seconds} "
            f"sample_start_seconds={sample_start_seconds} "
            f"sample_duration_seconds={sample_duration_seconds}"
        )


def _sleep_until(deadline_monotonic: float) -> None:
    while True:
        remaining_seconds = float(deadline_monotonic) - time.monotonic()
        if remaining_seconds <= 0:
            return
        time.sleep(min(remaining_seconds, 0.5))


def _parse_payload_kind(raw: str) -> PayloadKind:
    return PayloadKind(str(raw))


def _new_payload_fields(*, payload_kind: PayloadKind, payload_bytes: int) -> PayloadFields:
    if payload_kind is PayloadKind.BYTES:
        return {SINGLE_FIELD_PAYLOAD_KEY: b"x" * int(payload_bytes)}
    if payload_kind is PayloadKind.DLPACK:
        return {
            SINGLE_FIELD_PAYLOAD_KEY: _new_dlpack_payload_tensor(payload_bytes=int(payload_bytes)),
        }
    if payload_kind is PayloadKind.FLATDICT6_DLPACK:
        payload_fields: PayloadFields = {}
        for field_idx in range(FLATDICT6_DLPACK_FIELD_COUNT):
            payload_fields[f"{FLATDICT6_DLPACK_FIELD_PREFIX}{field_idx}"] = _new_dlpack_payload_tensor(
                payload_bytes=int(payload_bytes)
            )
        return payload_fields
    raise ValueError(f"Unsupported payload kind: {payload_kind.value}")


def _new_dlpack_payload_tensor(*, payload_bytes: int) -> DLPackBytesView:
    return DLPackBytesView(
        bytes(int(payload_bytes)),
        dtype_code=1,
        bits=8,
        lanes=1,
        shape=(int(payload_bytes),),
    )


def _new_message_fields(*, producer_id: str, sequence: int, payload_fields: PayloadFields) -> dict[str, Any]:
    message: dict[str, Any] = {
        "producer_id": producer_id,
        "sequence": sequence,
    }
    message.update(payload_fields)
    return message


def _payload_message_bytes(*, payload_kind: PayloadKind, payload_bytes: int) -> int:
    if payload_kind in (PayloadKind.BYTES, PayloadKind.DLPACK):
        return int(payload_bytes)
    if payload_kind is PayloadKind.FLATDICT6_DLPACK:
        return int(payload_bytes) * FLATDICT6_DLPACK_FIELD_COUNT
    raise ValueError(f"Unsupported payload kind: {payload_kind.value}")


def _consume_payload_fields(*, item: dict[str, Any], payload_kind: PayloadKind, payload_bytes: int) -> int:
    if payload_kind is PayloadKind.BYTES:
        payload = item.get(SINGLE_FIELD_PAYLOAD_KEY)
        if not isinstance(payload, (bytes, bytearray)):
            raise TypeError(f"payload must be bytes, got {type(payload).__name__}")
        return len(payload)
    if payload_kind is PayloadKind.DLPACK:
        payload = item.get(SINGLE_FIELD_PAYLOAD_KEY)
        if not hasattr(payload, "__dlpack__"):
            raise TypeError(f"payload must expose __dlpack__, got {type(payload).__name__}")
        return int(payload_bytes)
    if payload_kind is PayloadKind.FLATDICT6_DLPACK:
        consumed_bytes = 0
        for field_idx in range(FLATDICT6_DLPACK_FIELD_COUNT):
            field_key = f"{FLATDICT6_DLPACK_FIELD_PREFIX}{field_idx}"
            payload = item.get(field_key)
            if not hasattr(payload, "__dlpack__"):
                raise TypeError(f"{field_key} must expose __dlpack__, got {type(payload).__name__}")
            consumed_bytes += int(payload_bytes)
        return consumed_bytes
    raise ValueError(f"Unsupported payload kind: {payload_kind.value}")


def _stop_requested(etcd_client: etcd3.Etcd3Client, stop_key: str) -> bool:
    value, _ = etcd_client.get(stop_key)
    return value is not None


def _put_etcd_key(key: str, value: bytes) -> None:
    with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
        etcd_client.put(key, value)


def _delete_etcd_key(key: str) -> None:
    with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
        etcd_client.delete(key)


def _clear_etcd_prefix(prefix: str) -> None:
    with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
        for _, meta in etcd_client.get_prefix(prefix):
            etcd_client.delete(meta.key)


def _best_effort_close(obj: Any, *, role: str) -> None:
    close_res = obj.close()
    if close_res.is_ok():
        _ = close_res.unwrap()
        return
    logging.warning("[%s] close error: %s", role, close_res.unwrap_error())


if __name__ == "__main__":
    main()
