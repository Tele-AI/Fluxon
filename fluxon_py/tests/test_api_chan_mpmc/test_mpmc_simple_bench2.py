from __future__ import annotations

import argparse
import json
import os
import struct
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

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

DEFAULT_DURATION_SECONDS = 0
DEFAULT_PRODUCER_COUNT = 16
DEFAULT_VIDEO_MESSAGES_PER_PRODUCER = 63
DEFAULT_BATCH_SIZE = 256
DEFAULT_PREFETCH_NUM = 0
DEFAULT_CHANNEL_CAPACITY = 128
DEFAULT_FRAME_DTYPE = "float16"
DEFAULT_SEGMENT_FRAMES = 16
DEFAULT_FRAME_CHANNELS = 3
DEFAULT_FRAME_SIZE = 224
DEFAULT_GET_TRY_TIME_SECONDS: Optional[int] = None
MOONCAKE_LOCAL_BUFFER_BYTES = 16_777_216 * 10
PRODUCER_TRY_SLEEP_SECONDS = 0.01
CONSUMER_IDLE_SLEEP_SECONDS = 0.05
CHANNEL_CONSTRUCTION_READY_WAIT_SECONDS = 1.0
SUMMARY_STARTUP_TIMEOUT_SECONDS = 30
SUMMARY_KEY_PREFIX = "/test_mpmc_simple_bench2/summary/"
CONSUMER_STATE_LOG_INTERVAL_SECONDS = 2.0
DLPACK_DTYPE_BY_NAME: dict[str, tuple[int, int, int]] = {
    "float16": (2, 16, 1),
    "float32": (2, 32, 1),
    "int64": (0, 64, 1),
}
DTYPE_NBYTES: dict[str, int] = {
    "float16": 2,
    "float32": 4,
    "int64": 8,
}


@dataclass
class PumpStats:
    get_calls: int = 0
    get_batches: int = 0
    decoded_video_messages: int = 0
    decoded_eos_messages: int = 0
    decoded_payload_bytes: int = 0
    decoded_raw_frame_bytes: int = 0
    total_get_decode_ns: int = 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="MPMC bench that simulates the current motionpredictor transport shape"
    )
    subparsers = parser.add_subparsers(dest="mode", help="Execution mode")

    main_parser = subparsers.add_parser("main", help="Run one motionpredictor-shaped benchmark case")
    main_parser.add_argument("--duration-seconds", type=int, required=False, default=DEFAULT_DURATION_SECONDS)
    main_parser.add_argument("--producer-count", type=int, required=False, default=DEFAULT_PRODUCER_COUNT)
    main_parser.add_argument(
        "--video-messages-per-producer",
        type=int,
        required=False,
        default=DEFAULT_VIDEO_MESSAGES_PER_PRODUCER,
    )
    main_parser.add_argument("--batch-size", type=int, required=False, default=DEFAULT_BATCH_SIZE)
    main_parser.add_argument("--prefetch-num", type=int, required=False, default=DEFAULT_PREFETCH_NUM)
    main_parser.add_argument("--channel-capacity", type=int, required=False, default=DEFAULT_CHANNEL_CAPACITY)
    main_parser.add_argument("--frame-dtype", type=str, required=False, default=DEFAULT_FRAME_DTYPE)
    main_parser.add_argument("--segment-frames", type=int, required=False, default=DEFAULT_SEGMENT_FRAMES)
    main_parser.add_argument("--frame-channels", type=int, required=False, default=DEFAULT_FRAME_CHANNELS)
    main_parser.add_argument("--frame-size", type=int, required=False, default=DEFAULT_FRAME_SIZE)
    main_parser.add_argument("--get-try-time-seconds", type=int, required=False, default=DEFAULT_GET_TRY_TIME_SECONDS)

    producer_parser = subparsers.add_parser("run_producer", help="Run one simulated motionpredictor producer")
    producer_parser.add_argument("--backend-type", required=True, type=str)
    producer_parser.add_argument("--ip", required=True, type=str)
    producer_parser.add_argument("--bench-id", required=True, type=str)
    producer_parser.add_argument("--producer-id", required=True, type=str)
    producer_parser.add_argument("--chan-id", required=True, type=str)
    producer_parser.add_argument("--channel-capacity", required=True, type=int)
    producer_parser.add_argument("--video-messages-per-producer", required=True, type=int)
    producer_parser.add_argument("--frame-dtype", required=True, type=str)
    producer_parser.add_argument("--segment-frames", required=True, type=int)
    producer_parser.add_argument("--frame-channels", required=True, type=int)
    producer_parser.add_argument("--frame-size", required=True, type=int)

    consumer_parser = subparsers.add_parser("run_consumer", help="Run one simulated motionpredictor consumer")
    consumer_parser.add_argument("--backend-type", required=True, type=str)
    consumer_parser.add_argument("--ip", required=True, type=str)
    consumer_parser.add_argument("--bench-id", required=True, type=str)
    consumer_parser.add_argument("--consumer-id", required=True, type=str)
    consumer_parser.add_argument("--chan-id", required=True, type=str)
    consumer_parser.add_argument("--batch-size", required=True, type=int)
    consumer_parser.add_argument("--prefetch-num", required=True, type=int)
    consumer_parser.add_argument("--channel-capacity", required=True, type=int)
    consumer_parser.add_argument("--expected-producers", required=True, type=int)
    consumer_parser.add_argument("--frame-dtype", required=True, type=str)
    consumer_parser.add_argument("--segment-frames", required=True, type=int)
    consumer_parser.add_argument("--frame-channels", required=True, type=int)
    consumer_parser.add_argument("--frame-size", required=True, type=int)
    consumer_parser.add_argument("--summary-key", required=True, type=str)
    consumer_parser.add_argument("--get-try-time-seconds", required=False, type=int, default=DEFAULT_GET_TRY_TIME_SECONDS)
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
    _run_one_case(
        producer_count=int(args.producer_count),
        video_messages_per_producer=int(args.video_messages_per_producer),
        batch_size=int(args.batch_size),
        prefetch_num=int(args.prefetch_num),
        channel_capacity=int(args.channel_capacity),
        frame_dtype=str(args.frame_dtype),
        segment_frames=int(args.segment_frames),
        frame_channels=int(args.frame_channels),
        frame_size=int(args.frame_size),
        get_try_time_seconds=args.get_try_time_seconds,
    )


def _run_one_case(
    *,
    producer_count: int,
    video_messages_per_producer: int,
    batch_size: int,
    prefetch_num: int,
    channel_capacity: int,
    frame_dtype: str,
    segment_frames: int,
    frame_channels: int,
    frame_size: int,
    get_try_time_seconds: Optional[int],
) -> None:
    bench_id = (
        f"mpmc_motionpredictor_bench_"
        f"p{producer_count}_m{video_messages_per_producer}_b{batch_size}_pf{prefetch_num}_"
        f"{uuid.uuid4().hex}"
    )
    summary_key = f"{SUMMARY_KEY_PREFIX}{bench_id}/0"
    total_messages_including_eos = producer_count * (video_messages_per_producer + 1)
    if total_messages_including_eos % batch_size != 0:
        raise ValueError(
            "motionpredictor-shaped bench requires full batches because the current consumer "
            "calls get_data(batch_size=N) without partial-tail handling. "
            f"producer_count={producer_count}, video_messages_per_producer={video_messages_per_producer}, "
            f"total_messages_including_eos={total_messages_including_eos}, batch_size={batch_size}"
        )
    logging.info(
        "[bench2] start case bench_id=%s producers=%s video_messages_per_producer=%s "
        "batch_size=%s prefetch_num=%s channel_capacity=%s frame_dtype=%s segment_frames=%s "
        "frame_channels=%s frame_size=%s get_try_time_seconds=%s",
        bench_id,
        producer_count,
        video_messages_per_producer,
        batch_size,
        prefetch_num,
        channel_capacity,
        frame_dtype,
        segment_frames,
        frame_channels,
        frame_size,
        get_try_time_seconds,
    )
    _delete_etcd_key(summary_key)
    bootstrap_store = _new_channel_store(role_key=f"{bench_id}_bootstrap", backend_type=KV_SVC_TYPE)
    bootstrap_producer = None
    worker_processes: list[subprocess.Popen[str]] = []
    case_begin = time.time()
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
        logging.info("[bench2] bootstrap MPMC channel created bench_id=%s chan_id=%s", bench_id, chan_id)

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
                    "0",
                    "--chan-id",
                    chan_id,
                    "--batch-size",
                    str(batch_size),
                    "--prefetch-num",
                    str(prefetch_num),
                    "--channel-capacity",
                    str(channel_capacity),
                    "--expected-producers",
                    str(producer_count),
                    "--frame-dtype",
                    str(frame_dtype),
                    "--segment-frames",
                    str(segment_frames),
                    "--frame-channels",
                    str(frame_channels),
                    "--frame-size",
                    str(frame_size),
                    "--summary-key",
                    summary_key,
                ]
                + (
                    ["--get-try-time-seconds", str(get_try_time_seconds)]
                    if get_try_time_seconds is not None
                    else []
                )
            )
        )
        _wait_for_summary(summary_key, timeout_seconds=SUMMARY_STARTUP_TIMEOUT_SECONDS)
        _best_effort_close(bootstrap_producer, role="bootstrap_producer_after_consumer_bind")
        bootstrap_producer = None

        for producer_idx in range(producer_count):
            worker_processes.append(
                _spawn_worker(
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
                        str(producer_idx),
                        "--chan-id",
                        chan_id,
                        "--channel-capacity",
                        str(channel_capacity),
                        "--video-messages-per-producer",
                        str(video_messages_per_producer),
                        "--frame-dtype",
                        str(frame_dtype),
                        "--segment-frames",
                        str(segment_frames),
                        "--frame-channels",
                        str(frame_channels),
                        "--frame-size",
                        str(frame_size),
                    ]
                )
            )

        _wait_for_processes(worker_processes)
        summary = _load_summary(summary_key)
        elapsed_seconds = max(time.time() - case_begin, 0.001)
        _print_case_summary(
            producer_count=producer_count,
            video_messages_per_producer=video_messages_per_producer,
            batch_size=batch_size,
            prefetch_num=prefetch_num,
            channel_capacity=channel_capacity,
            frame_dtype=frame_dtype,
            segment_frames=segment_frames,
            frame_channels=frame_channels,
            frame_size=frame_size,
            get_try_time_seconds=get_try_time_seconds,
            summary=summary,
            wall_elapsed_seconds=elapsed_seconds,
        )
    finally:
        _terminate_processes(worker_processes)
        _delete_etcd_key(summary_key)
        if bootstrap_producer is not None:
            _best_effort_close(bootstrap_producer, role="bootstrap_producer")
        _best_effort_close(bootstrap_store, role="bootstrap_store")


def _run_producer(args: argparse.Namespace) -> None:
    os.environ["TEST_MPMC"] = "1"
    setup_test_environment(logging)
    if not isinstance(args.chan_id, str) or not args.chan_id.isdigit():
        raise ValueError(f"chan_id must be digit-only string, got {args.chan_id!r}")
    _validate_positive_int("channel_capacity", args.channel_capacity)
    _validate_non_negative_int("video_messages_per_producer", args.video_messages_per_producer)
    _validate_positive_int("segment_frames", args.segment_frames)
    _validate_positive_int("frame_channels", args.frame_channels)
    _validate_positive_int("frame_size", args.frame_size)
    store = _new_channel_store(role_key=f"{args.bench_id}_producer_{args.producer_id}", backend_type=args.backend_type)
    producer = None
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
        template = _build_motion_video_template(
            frame_dtype=str(args.frame_dtype),
            segment_frames=int(args.segment_frames),
            frame_channels=int(args.frame_channels),
            frame_size=int(args.frame_size),
        )
        for seq in range(int(args.video_messages_per_producer)):
            item = _new_motion_video_message(template, str(args.producer_id), seq)
            _put_motion_item_with_retry(producer, item, producer_id=str(args.producer_id))
        eos_item = _new_motion_eos_message(
            producer_id=str(args.producer_id),
            sent=int(args.video_messages_per_producer),
        )
        _put_motion_item_with_retry(producer, eos_item, producer_id=str(args.producer_id))
    finally:
        if producer is not None:
            _best_effort_close(producer, role=f"producer_{args.producer_id}")
        _best_effort_close(store, role=f"producer_store_{args.producer_id}")


def _put_motion_item_with_retry(producer: Any, item: dict[str, Any], *, producer_id: str) -> None:
    while True:
        result = producer.put_data(item)
        if result.is_ok():
            _ = result.unwrap()
            return
        err = result.unwrap_error()
        if isinstance(err, ProducerClosedError):
            raise RuntimeError(f"bench2 producer {producer_id} closed during put_data") from err
        logging.warning("[bench2 producer %s] put_data error: %s", producer_id, err)
        time.sleep(PRODUCER_TRY_SLEEP_SECONDS)


def _run_consumer(args: argparse.Namespace) -> None:
    os.environ["TEST_MPMC"] = "1"
    setup_test_environment(logging)
    if not isinstance(args.chan_id, str) or not args.chan_id.isdigit():
        raise ValueError(f"chan_id must be digit-only string, got {args.chan_id!r}")
    _validate_positive_int("batch_size", args.batch_size)
    _validate_non_negative_int("prefetch_num", args.prefetch_num)
    _validate_positive_int("channel_capacity", args.channel_capacity)
    _validate_positive_int("expected_producers", args.expected_producers)
    _validate_positive_int("segment_frames", args.segment_frames)
    _validate_positive_int("frame_channels", args.frame_channels)
    _validate_positive_int("frame_size", args.frame_size)
    store = _new_channel_store(role_key=f"{args.bench_id}_consumer_{args.consumer_id}", backend_type=args.backend_type)
    consumer = None
    pump_stats = PumpStats()
    started_at = time.time()
    hot_started_at: Optional[float] = None
    summary = _new_consumer_summary_template(args)
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
        _write_summary(args.summary_key, summary)
        expected_producers = int(args.expected_producers)
        next_state_log_at = time.time() + CONSUMER_STATE_LOG_INTERVAL_SECONDS
        while True:
            now = time.time()
            if now >= next_state_log_at:
                _log_consumer_state(
                    args=args,
                    summary=summary,
                    pump_stats=pump_stats,
                )
                next_state_log_at = now + CONSUMER_STATE_LOG_INTERVAL_SECONDS

            call_begin = time.monotonic_ns()
            items = _get_motionpredictor_fluxon_batch(
                consumer,
                batch_size=int(args.batch_size),
                prefetch_num=int(args.prefetch_num),
                get_try_time_seconds=args.get_try_time_seconds,
            )
            pump_stats.get_calls += 1
            pump_stats.total_get_decode_ns += time.monotonic_ns() - call_begin
            if not items:
                if int(summary["received_eos_messages"]) >= expected_producers:
                    break
                time.sleep(CONSUMER_IDLE_SLEEP_SECONDS)
                continue

            if any(item.get("type") == "channel_closed" for item in items):
                break

            pump_stats.get_batches += 1

            batch_payload_bytes = 0
            batch_raw_frame_bytes = 0
            batch_video_messages = 0
            for item in items:
                item_type = item.get("type")
                if item_type == "video":
                    if hot_started_at is None:
                        hot_started_at = time.time()
                    batch_video_messages += 1
                    batch_payload_bytes += int(item["_bench_payload_bytes"])
                    batch_raw_frame_bytes += int(item["_bench_raw_frame_bytes"])
                    summary["received_video_messages"] += 1
                    summary["received_payload_bytes"] += int(item["_bench_payload_bytes"])
                    summary["received_raw_frame_bytes"] += int(item["_bench_raw_frame_bytes"])
                    summary["consumer_processed_video_messages"] += 1
                    summary["consumer_processed_payload_bytes"] += int(item["_bench_payload_bytes"])
                    summary["consumer_processed_raw_frame_bytes"] += int(item["_bench_raw_frame_bytes"])
                    pump_stats.decoded_video_messages += 1
                    pump_stats.decoded_payload_bytes += int(item["_bench_payload_bytes"])
                    pump_stats.decoded_raw_frame_bytes += int(item["_bench_raw_frame_bytes"])
                    continue
                if item_type == "eos":
                    summary["received_eos_messages"] += 1
                    pump_stats.decoded_eos_messages += 1
                    continue
                raise ValueError(f"Unexpected motionpredictor bench item type: {item_type!r}")

            if batch_video_messages > 0:
                summary["consumer_processed_batches"] += 1
                print(
                    (
                        "BENCH2_BATCH "
                        f"consumer_id={args.consumer_id} "
                        f"video_messages={batch_video_messages} "
                        f"payload_bytes={batch_payload_bytes} "
                        f"raw_frame_bytes={batch_raw_frame_bytes}"
                    ),
                    flush=True,
                )

            if int(summary["received_eos_messages"]) >= expected_producers:
                summary["stopped_by_expected_eos"] = True
                break

            if time.time() - float(summary["last_summary_write_at"]) >= 0.5:
                _refresh_summary_from_pump(summary, pump_stats)
                _refresh_summary_timing(
                    summary,
                    started_at=started_at,
                    hot_started_at=hot_started_at,
                    now=time.time(),
                )
                _write_summary(args.summary_key, summary)

        summary["stopped_by_expected_eos"] = True
    finally:
        if consumer is not None:
            _best_effort_close(consumer, role=f"consumer_{args.consumer_id}")
        _best_effort_close(store, role=f"consumer_store_{args.consumer_id}")
        _refresh_summary_from_pump(summary, pump_stats)
        _refresh_summary_timing(
            summary,
            started_at=started_at,
            hot_started_at=hot_started_at,
            now=time.time(),
        )
        summary["consumer_id"] = str(args.consumer_id)
        _write_summary(args.summary_key, summary)


def _new_consumer_summary_template(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "consumer_id": str(args.consumer_id),
        "received_video_messages": 0,
        "received_eos_messages": 0,
        "received_payload_bytes": 0,
        "received_raw_frame_bytes": 0,
        "mq_get_calls": 0,
        "mq_full_batches": 0,
        "mq_decoded_video_messages": 0,
        "mq_decoded_eos_messages": 0,
        "mq_decoded_payload_bytes": 0,
        "mq_decoded_raw_frame_bytes": 0,
        "mq_get_decode_seconds": 0.0,
        "consumer_processed_batches": 0,
        "consumer_processed_video_messages": 0,
        "consumer_processed_raw_frame_bytes": 0,
        "consumer_processed_payload_bytes": 0,
        "elapsed_seconds": 0.0,
        "cold_start_seconds": None,
        "hot_elapsed_seconds": None,
        "hot_received_payload_throughput_mb_s": None,
        "hot_received_raw_frame_throughput_mb_s": None,
        "stopped_by_expected_eos": False,
        "last_summary_write_at": 0.0,
    }


def _refresh_summary_from_pump(summary: dict[str, Any], pump_stats: PumpStats) -> None:
    summary["mq_get_calls"] = int(pump_stats.get_calls)
    summary["mq_full_batches"] = int(pump_stats.get_batches)
    summary["mq_decoded_video_messages"] = int(pump_stats.decoded_video_messages)
    summary["mq_decoded_eos_messages"] = int(pump_stats.decoded_eos_messages)
    summary["mq_decoded_payload_bytes"] = int(pump_stats.decoded_payload_bytes)
    summary["mq_decoded_raw_frame_bytes"] = int(pump_stats.decoded_raw_frame_bytes)
    summary["mq_get_decode_seconds"] = float(pump_stats.total_get_decode_ns) / 1_000_000_000.0


def _refresh_summary_timing(
    summary: dict[str, Any],
    *,
    started_at: float,
    hot_started_at: Optional[float],
    now: float,
) -> None:
    total_elapsed_seconds = max(float(now) - float(started_at), 0.0)
    summary["elapsed_seconds"] = total_elapsed_seconds

    if hot_started_at is None:
        summary["cold_start_seconds"] = None
        summary["hot_elapsed_seconds"] = None
        summary["hot_received_payload_throughput_mb_s"] = None
        summary["hot_received_raw_frame_throughput_mb_s"] = None
        return

    cold_start_seconds = max(float(hot_started_at) - float(started_at), 0.0)
    hot_elapsed_seconds = max(float(now) - float(hot_started_at), 0.0)
    summary["cold_start_seconds"] = cold_start_seconds
    summary["hot_elapsed_seconds"] = hot_elapsed_seconds

    if hot_elapsed_seconds <= 0.0:
        summary["hot_received_payload_throughput_mb_s"] = None
        summary["hot_received_raw_frame_throughput_mb_s"] = None
        return

    received_payload_bytes = int(summary["received_payload_bytes"])
    received_raw_frame_bytes = int(summary["received_raw_frame_bytes"])
    mib = float(1024 * 1024)
    summary["hot_received_payload_throughput_mb_s"] = received_payload_bytes / mib / hot_elapsed_seconds
    summary["hot_received_raw_frame_throughput_mb_s"] = received_raw_frame_bytes / mib / hot_elapsed_seconds


def _log_consumer_state(
    *,
    args: argparse.Namespace,
    summary: dict[str, Any],
    pump_stats: PumpStats,
) -> None:
    logging.info(
        "[bench2-consumer-state] bench_id=%s consumer_id=%s received_video=%s received_eos=%s "
        "mq_get_calls=%s mq_full_batches=%s mq_decoded_video=%s consumer_processed_batches=%s "
        "consumer_processed_video_messages=%s",
        args.bench_id,
        args.consumer_id,
        summary["received_video_messages"],
        summary["received_eos_messages"],
        pump_stats.get_calls,
        pump_stats.get_batches,
        pump_stats.decoded_video_messages,
        summary["consumer_processed_batches"],
        summary["consumer_processed_video_messages"],
    )


def _get_motionpredictor_fluxon_batch(
    consumer: Any,
    *,
    batch_size: int,
    prefetch_num: int,
    get_try_time_seconds: Optional[int],
) -> list[dict[str, Any]]:
    if get_try_time_seconds is None and prefetch_num == 0:
        get_res = consumer.get_data(batch_size=batch_size)
    else:
        get_res = consumer.get_data(
            batch_size=batch_size,
            try_time=get_try_time_seconds,
            prefetch_num=prefetch_num,
        )
    if not get_res.is_ok():
        err = get_res.unwrap_error()
        if isinstance(err, ChannelClosedError):
            return [{"type": "channel_closed"}]
        if isinstance(err, MessageConsumptionNoNewMessageError):
            return []
        raise RuntimeError(f"get_data failed: {err}")
    decoded_items: list[dict[str, Any]] = []
    for item in (get_res.unwrap() or []):
        decoded_items.append(_decode_motion_item(item))
    return decoded_items


def _build_motion_video_template(
    *,
    frame_dtype: str,
    segment_frames: int,
    frame_channels: int,
    frame_size: int,
) -> dict[str, Any]:
    frames_shape = (
        segment_frames,
        frame_channels,
        frame_size,
        frame_size,
    )
    frames_tensor = _new_dlpack_bytes_view(
        data=_new_zero_bytes(_calc_tensor_nbytes(frames_shape, frame_dtype)),
        dtype_text=frame_dtype,
        shape=frames_shape,
    )
    lens_shape = (1,)
    lens_dtype = "int64"
    lens_tensor = _new_dlpack_bytes_view(
        data=struct.pack("<q", int(segment_frames)),
        dtype_text=lens_dtype,
        shape=lens_shape,
    )
    return {
        "type": "video",
        "path_rgb": "/bench/video.mp4",
        "read_path_rgb": "/bench/video.mp4",
        "path_txt": "/bench/out.json",
        "source_json": "/bench/source.json",
        "category": "bench",
        "clip_index": 0,
        "clip_id": 0,
        "clip_length": 16.0,
        "clip_fps": 16.0,
        "duration_seconds": 1.0,
        "sample_fps": 16.0,
        "max_frames": 81,
        "segment_frames": segment_frames,
        "num_frames": segment_frames,
        "frames": frames_tensor,
        "frames_shape": _shape_to_text(frames_shape),
        "frames_dtype": frame_dtype,
        "lens": lens_tensor,
        "lens_shape": _shape_to_text(lens_shape),
        "lens_dtype": lens_dtype,
    }


def _new_motion_video_message(template: dict[str, Any], producer_id: str, sequence: int) -> dict[str, Any]:
    item = dict(template)
    item["item_id"] = f"{producer_id}_{sequence}"
    item["producer_id"] = producer_id
    item["preprocess_ts_ms"] = int(time.time() * 1000)
    return item


def _new_motion_eos_message(*, producer_id: str, sent: int) -> dict[str, Any]:
    return {
        "type": "eos",
        "producer_id": producer_id,
        "sent": sent,
        "skipped": 0,
        "ts_ms": int(time.time() * 1000),
    }


def _decode_motion_item(item: dict[str, Any]) -> dict[str, Any]:
    decoded = dict(item)
    if decoded.get("type") != "video":
        return decoded
    frames = decoded.get("frames")
    lens = decoded.get("lens")
    if not hasattr(frames, "__dlpack__"):
        raise TypeError(f"video item frames must expose __dlpack__, got {type(frames).__name__}")
    if not hasattr(lens, "__dlpack__"):
        raise TypeError(f"video item lens must expose __dlpack__, got {type(lens).__name__}")
    frames_nbytes = _calc_tensor_nbytes(
        _shape_from_text(decoded.get("frames_shape")),
        str(decoded.get("frames_dtype")),
    )
    lens_nbytes = _calc_tensor_nbytes(
        _shape_from_text(decoded.get("lens_shape")),
        str(decoded.get("lens_dtype")),
    )
    decoded["_bench_payload_bytes"] = frames_nbytes + lens_nbytes
    decoded["_bench_raw_frame_bytes"] = frames_nbytes + lens_nbytes
    return decoded


def _shape_to_text(shape: tuple[int, ...]) -> str:
    return ",".join(str(dim) for dim in shape)


def _shape_from_text(value: Any) -> tuple[int, ...]:
    text = str(value)
    if not text:
        return ()
    return tuple(int(dim) for dim in text.split(","))


def _calc_tensor_nbytes(shape: tuple[int, ...], dtype_text: str) -> int:
    if dtype_text not in DTYPE_NBYTES:
        raise ValueError(f"Unsupported dtype: {dtype_text!r}")
    elements = 1
    for dim in shape:
        elements *= int(dim)
    return elements * DTYPE_NBYTES[dtype_text]


def _new_zero_bytes(size: int) -> bytes:
    if int(size) < 0:
        raise ValueError(f"size must be >= 0, got {size}")
    return bytes(int(size))


def _new_dlpack_bytes_view(*, dtype_text: str, shape: tuple[int, ...], data: bytes) -> DLPackBytesView:
    dtype = DLPACK_DTYPE_BY_NAME.get(str(dtype_text))
    if dtype is None:
        raise ValueError(f"Unsupported dlpack dtype: {dtype_text!r}")
    dtype_code, bits, lanes = dtype
    expected_nbytes = _calc_tensor_nbytes(shape, dtype_text)
    if len(data) != expected_nbytes:
        raise ValueError(
            f"dlpack payload size mismatch for dtype={dtype_text!r}, shape={shape}: "
            f"expected {expected_nbytes} bytes, got {len(data)}"
        )
    return DLPackBytesView(
        data,
        dtype_code=dtype_code,
        bits=bits,
        lanes=lanes,
        shape=shape,
    )


def _validate_main_args(args: argparse.Namespace) -> None:
    _validate_non_negative_int("duration_seconds", args.duration_seconds)
    _validate_positive_int("producer_count", args.producer_count)
    _validate_non_negative_int("video_messages_per_producer", args.video_messages_per_producer)
    _validate_positive_int("batch_size", args.batch_size)
    _validate_non_negative_int("prefetch_num", args.prefetch_num)
    _validate_positive_int("channel_capacity", args.channel_capacity)
    _validate_positive_int("segment_frames", args.segment_frames)
    _validate_positive_int("frame_channels", args.frame_channels)
    _validate_positive_int("frame_size", args.frame_size)
    if str(args.frame_dtype) not in {"float16", "float32"}:
        raise ValueError(f"frame_dtype must be float16 or float32, got {args.frame_dtype!r}")


def _validate_positive_int(name: str, value: int) -> None:
    if int(value) <= 0:
        raise ValueError(f"{name} must be > 0, got {value}")


def _validate_non_negative_int(name: str, value: int) -> None:
    if int(value) < 0:
        raise ValueError(f"{name} must be >= 0, got {value}")


def _validate_non_negative_float(name: str, value: float) -> None:
    if float(value) < 0:
        raise ValueError(f"{name} must be >= 0, got {value}")


def _wait_for_channel_construction_rpc_ready(*, role: str) -> None:
    logging.info(
        "[bench2] waiting channel construction RPC ready role=%s seconds=%s",
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


def _new_channel_store(*, role_key: str, backend_type: str):
    config = _new_store_config(
        instance_key=role_key,
        backend_type=backend_type,
    )
    result = new_store(config)
    if not result.is_ok():
        raise RuntimeError(f"new channel store failed: {result.unwrap_error()}")
    return result.unwrap()


def _new_store_config(*, instance_key: str, backend_type: str) -> FluxonKvClientConfig:
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
        fluxon_spec: dict[str, Any] = {
            "cluster_name": load_test_fluxon_cluster_name(),
            "share_mem_path": load_test_fluxon_share_mem_path(),
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


def _wait_for_processes(processes: list[subprocess.Popen[str]]) -> None:
    for proc in processes:
        return_code = proc.wait()
        if return_code != 0:
            raise RuntimeError(f"Worker exited with non-zero code: pid={proc.pid} code={return_code}")


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


def _write_summary(summary_key: str, summary: dict[str, Any]) -> None:
    summary["last_summary_write_at"] = time.time()
    with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
        etcd_client.put(summary_key, json.dumps(summary).encode("utf-8"))


def _wait_for_summary(summary_key: str, *, timeout_seconds: int) -> dict[str, Any]:
    deadline = time.time() + float(timeout_seconds)
    with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
        while True:
            value, _ = etcd_client.get(summary_key)
            if value is not None:
                loaded = json.loads(value.decode("utf-8"))
                if not isinstance(loaded, dict):
                    raise TypeError(f"summary at {summary_key} must be dict")
                return loaded
            if time.time() >= deadline:
                raise RuntimeError(f"Timed out waiting consumer summary: {summary_key}")
            time.sleep(0.5)


def _load_summary(summary_key: str) -> dict[str, Any]:
    with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
        value, _ = etcd_client.get(summary_key)
        if value is None:
            raise RuntimeError(f"Missing summary: {summary_key}")
        loaded = json.loads(value.decode("utf-8"))
        if not isinstance(loaded, dict):
            raise TypeError(f"summary at {summary_key} must be dict")
        return loaded


def _delete_etcd_key(key: str) -> None:
    with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
        etcd_client.delete(key)


def _best_effort_close(obj: Any, *, role: str) -> None:
    try:
        close_res = obj.close()
    except Exception as exc:
        logging.warning("%s close raised (ignored): %s", role, exc)
        return
    if close_res.is_ok():
        _ = close_res.unwrap()
        return
    logging.warning("%s close error (ignored): %s", role, close_res.unwrap_error())


def _print_case_summary(
    *,
    producer_count: int,
    video_messages_per_producer: int,
    batch_size: int,
    prefetch_num: int,
    channel_capacity: int,
    frame_dtype: str,
    segment_frames: int,
    frame_channels: int,
    frame_size: int,
    get_try_time_seconds: Optional[int],
    summary: dict[str, Any],
    wall_elapsed_seconds: float,
) -> None:
    received_payload_bytes = int(summary["received_payload_bytes"])
    received_raw_frame_bytes = int(summary["received_raw_frame_bytes"])
    consumer_elapsed_seconds = max(float(summary["elapsed_seconds"]), 0.001)
    received_payload_throughput_mb_s = received_payload_bytes / float(1024 * 1024) / consumer_elapsed_seconds
    received_raw_frame_throughput_mb_s = received_raw_frame_bytes / float(1024 * 1024) / consumer_elapsed_seconds
    cold_start_seconds = summary["cold_start_seconds"]
    hot_elapsed_seconds = summary["hot_elapsed_seconds"]
    hot_received_payload_throughput_mb_s = summary["hot_received_payload_throughput_mb_s"]
    hot_received_raw_frame_throughput_mb_s = summary["hot_received_raw_frame_throughput_mb_s"]
    print(
        (
            "BENCH2_SUMMARY "
            f"producers={producer_count} "
            f"video_messages_per_producer={video_messages_per_producer} "
            f"batch_size={batch_size} "
            f"prefetch_num={prefetch_num} "
            f"channel_capacity={channel_capacity} "
            f"frame_dtype={frame_dtype} "
            f"segment_frames={segment_frames} "
            f"frame_channels={frame_channels} "
            f"frame_size={frame_size} "
            f"get_try_time_seconds={get_try_time_seconds} "
            f"received_video_messages={summary['received_video_messages']} "
            f"received_eos_messages={summary['received_eos_messages']} "
            f"received_payload_bytes={received_payload_bytes} "
            f"received_raw_frame_bytes={received_raw_frame_bytes} "
            f"consumer_elapsed_seconds={consumer_elapsed_seconds:.2f} "
            f"wall_elapsed_seconds={wall_elapsed_seconds:.2f} "
            f"cold_start_seconds={_format_optional_seconds(cold_start_seconds)} "
            f"hot_elapsed_seconds={_format_optional_seconds(hot_elapsed_seconds)} "
            f"received_payload_throughput_mb_s={received_payload_throughput_mb_s:.2f} "
            f"received_raw_frame_throughput_mb_s={received_raw_frame_throughput_mb_s:.2f} "
            f"hot_received_payload_throughput_mb_s={_format_optional_rate(hot_received_payload_throughput_mb_s)} "
            f"hot_received_raw_frame_throughput_mb_s={_format_optional_rate(hot_received_raw_frame_throughput_mb_s)}"
        ),
        flush=True,
    )
    print(
        (
            "BENCH2_CONSUMER "
            f"mq_get_calls={summary['mq_get_calls']} "
            f"mq_full_batches={summary['mq_full_batches']} "
            f"mq_decoded_video_messages={summary['mq_decoded_video_messages']} "
            f"mq_decoded_eos_messages={summary['mq_decoded_eos_messages']} "
            f"mq_get_decode_seconds={float(summary['mq_get_decode_seconds']):.2f} "
            f"consumer_processed_batches={summary['consumer_processed_batches']} "
            f"consumer_processed_video_messages={summary['consumer_processed_video_messages']} "
            f"consumer_processed_raw_frame_bytes={summary['consumer_processed_raw_frame_bytes']} "
            f"consumer_processed_payload_bytes={summary['consumer_processed_payload_bytes']} "
            f"stopped_by_expected_eos={summary['stopped_by_expected_eos']}"
        ),
        flush=True,
    )


def _format_optional_seconds(value: Any) -> str:
    if value is None:
        return "None"
    return f"{float(value):.2f}"


def _format_optional_rate(value: Any) -> str:
    if value is None:
        return "None"
    return f"{float(value):.2f}"


if __name__ == "__main__":
    main()
