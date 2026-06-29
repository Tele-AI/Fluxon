"""Quick-and-fair MPMC channel scenario.

Spawns one producer and multiple consumers via subprocesses, then checks that
load distribution stays balanced and that every consumer participates in the
run.
"""
from __future__ import annotations

import argparse
import math
import os
import random
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import etcd3

# Ensure absolute imports work when running this file directly
import os as _os
import sys as _sys
_sys.path.insert(0, _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "../../..")))

from fluxon_py.api_ext_chan import ChanType  # noqa: E402
from fluxon_py.logging import init_logger  # noqa: E402
from fluxon_py.api_error import MessageConsumptionNoNewMessageError  # noqa: E402
from fluxon_py.tests.test_api_chan_mpsc.test_api_chan_mpsc_base import (  # noqa: E402
    _wait_fluxon_member_absent,
    chan_type_from_string,
    configure_backend,
    create_channel_env,
    release,
    require_store,
)
from fluxon_py.tests.test_lib import (  # noqa: E402
    CHAN_CONFIG_TEST,
    ETCD_HOST,
    ETCD_PORT,
    TEST_TIMEOUT_SECONDS,
    setup_test_environment,
    new_test_consumer,
    new_test_producer,
    load_test_fluxon_cluster_name,
    run_with_argmatrix,
)
from fluxon_py.api_ext_chan import (  # noqa: E402
    _new_unique_lock_key,
    _new_unique_mapping_key,
)


logging = init_logger()

PRODUCER_COUNT = 4  # Number of producers with different weights
CONSUMER_COUNT = 4  # Number of consumers
MESSAGE_COUNT = 1000
CHANNEL_KEY = "quick_fair_consume"
PRODUCER_DONE_KEY = f"/tests/{CHANNEL_KEY}/producer_done"
PRODUCER_DONE_POLL_INTERVAL = 0.2
MAX_CONSUMER_RUNTIME = max(100000.0, MESSAGE_COUNT * 0.1)
BOOTSTRAP_CHANNEL_READY_TIMEOUT = float(TEST_TIMEOUT_SECONDS)
# Consumer fairness is inherently stochastic under shared CI scheduling. Model
# the final consumer counts as deviations around the expected mean instead of a
# brittle fixed max-minus-min range.
FAIR_CONSUMER_SIGMA_MULTIPLIER = 3.0
# Producer weights: P0=1, P1=2, P2=3, P3=4 (total=10)
# Expected message distribution from producers: P0=10%, P1=20%, P2=30%, P3=40%
PRODUCER_WEIGHTS = [1, 2, 3, 4]
WEIGHT_TOLERANCE_PERCENT = 0.15  # 15% tolerance for weight-based distribution (global)
WEIGHT_TOLERANCE_PERCENT_PER_CONSUMER = 0.30  # 30% tolerance for per-consumer distribution
MIN_MESSAGES_FOR_WEIGHT_CHECK = 50  # Minimum messages consumed to check weight distribution

# Keep logs scoped to this scenario to avoid cross-test interference and expensive rmtree() of a
# shared repo-root `logs/` directory.
LOG_DIR = Path("logs/test_api_chan_mpmc_quick_and_weighted_consume")

# Defensive check: ensure PRODUCER_COUNT matches PRODUCER_WEIGHTS length
assert len(PRODUCER_WEIGHTS) == PRODUCER_COUNT, \
    f"PRODUCER_WEIGHTS length ({len(PRODUCER_WEIGHTS)}) must match PRODUCER_COUNT ({PRODUCER_COUNT})"


SCRIPT_PATH = Path(__file__).resolve()


def build_parser() -> argparse.ArgumentParser:
    """Construct the internal CLI parser for the harness subprocesses."""
    parser = argparse.ArgumentParser(
        description="MPMC quick-and-fair consume scenario"
    )
    subparsers = parser.add_subparsers(dest="mode", help="Execution mode")
    subparsers.add_parser("main", help="Run the quick-and-fair scenario")

    producer_parser = subparsers.add_parser("run_producer", help="Producer")
    producer_parser.add_argument("--backend_type", required=True, type=str)
    producer_parser.add_argument("--ip", required=True, type=str)
    producer_parser.add_argument("--construct_type", required=True, type=str)
    producer_parser.add_argument("--new_or_bind_key", required=True, type=str)
    producer_parser.add_argument("--chan_type", required=True, type=str)
    producer_parser.add_argument("--producer_id", required=True, type=str)
    producer_parser.add_argument("--weight", required=True, type=int)

    consumer_parser = subparsers.add_parser("run_consumer", help="Consumer")
    consumer_parser.add_argument("--backend_type", required=True, type=str)
    consumer_parser.add_argument("--ip", required=True, type=str)
    consumer_parser.add_argument("--construct_type", required=True, type=str)
    consumer_parser.add_argument("--new_or_bind_key", required=True, type=str)
    consumer_parser.add_argument("--chan_type", required=True, type=str)
    consumer_parser.add_argument("--consumer_id", required=True, type=str)
    return parser


def parse_args(parser: Optional[argparse.ArgumentParser] = None) -> Dict[str, Any]:
    """Apply default mode handling around the harness parser."""
    parser = parser or build_parser()
    parsed = parser.parse_args()
    options = vars(parsed)
    if options.get("mode") is None:
        options["mode"] = "main"
    return options


def cli(args: Optional[Dict[str, Any]] = None) -> None:
    options = args or parse_args()
    if not isinstance(options, dict):
        options = vars(options)
    assert isinstance(options, dict)
    mode = options.get("mode")
    if mode == "main":
        # Route main through the argmatrix-backed scenario entry.
        test_quick_and_fair_consume()
        return
    env = create_channel_env()
    try:
        if mode == "run_producer":
            run_producer(env, options)
        elif mode == "run_consumer":
            run_consumer(env, options)
        else:
            raise ValueError(f"Unsupported mode: {mode}")
    finally:
        release(env)


def run_main(env, _) -> None:
    prev_type, prev_ip = env.backend_type, env.backend_ip
    configure_backend(env)
    try:
        clean_namespace()
        reset_producer_done_flag()
        processes = spawn_processes(env)
        wait_for_processes(processes)
        verify_quick_and_fair([item[2] for item in processes])
    finally:
        configure_backend(env, backend_type=prev_type, backend_ip=prev_ip)
    print("=== quick_and_fair_consume scenario finished ===")


def test_quick_and_fair_consume() -> None:
    # Prepare environment once per matrix run
    setup_test_environment(logging)

    def _once(prefetch: int) -> None:  # prefetch injected by argmatrix; unused here
        env = create_channel_env()
        args = {"mode": "main"}
        run_main(env, args)
        release(env)

    run_with_argmatrix(_once)


def spawn_processes(env) -> List[Tuple[str, subprocess.Popen, str]]:
    # MPMC channel creation under `new_or_bind` is single-writer: only one producer should race
    # to publish the unique key mapping. To keep CI deterministic, start exactly one producer
    # first, wait until it publishes `CHANNEL_KEY` in etcd, then start remaining producers and
    # all consumers.
    producer_cmds: List[List[str]] = []
    consumer_cmds: List[List[str]] = []

    for index in range(PRODUCER_COUNT):
        weight = PRODUCER_WEIGHTS[index]
        producer_cmds.append(
            [
                sys.executable,
                str(SCRIPT_PATH),
                "run_producer",
                "--backend_type",
                env.backend_type,
                "--ip",
                env.backend_ip,
                "--construct_type",
                "new_or_bind",
                "--new_or_bind_key",
                CHANNEL_KEY,
                "--chan_type",
                ChanType.MPMC.value,
                "--producer_id",
                f"P{index}",
                "--weight",
                str(weight),
            ]
        )

    for index in range(CONSUMER_COUNT):
        consumer_cmds.append(
            [
                sys.executable,
                str(SCRIPT_PATH),
                "run_consumer",
                "--backend_type",
                env.backend_type,
                "--ip",
                env.backend_ip,
                "--construct_type",
                "new_or_bind",
                "--new_or_bind_key",
                CHANNEL_KEY,
                "--chan_type",
                ChanType.MPMC.value,
                "--consumer_id",
                f"C{index}",
            ]
        )

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    os.system(f"chmod -R 777 {LOG_DIR.as_posix()}")

    processes: List[Tuple[str, subprocess.Popen, str]] = []
    bootstrap_cmd = producer_cmds[0]
    bootstrap_log = log_path("producer", bootstrap_cmd)
    print(f"Starting producer (bootstrap): {' '.join(bootstrap_cmd)}")
    print(f"Log file: {bootstrap_log}")
    with open(bootstrap_log, "w", encoding="utf-8") as log_f:
        bootstrap_proc = subprocess.Popen(bootstrap_cmd, stdout=log_f, stderr=log_f, text=True)
    processes.append(("producer", bootstrap_proc, bootstrap_log))

    chan_id = _wait_unique_key_mapping(
        timeout_seconds=BOOTSTRAP_CHANNEL_READY_TIMEOUT,
        bootstrap_proc=bootstrap_proc,
        bootstrap_log=bootstrap_log,
    )
    print(f"Channel mapping ready: unique_key={CHANNEL_KEY} chan_id={chan_id}")

    for cmd in producer_cmds[1:]:
        log_file = log_path("producer", cmd)
        print(f"Starting producer: {' '.join(cmd)}")
        print(f"Log file: {log_file}")
        with open(log_file, "w", encoding="utf-8") as log_f:
            proc = subprocess.Popen(cmd, stdout=log_f, stderr=log_f, text=True)
        processes.append(("producer", proc, log_file))

    for cmd in consumer_cmds:
        log_file = log_path("consumer", cmd)
        print(f"Starting consumer: {' '.join(cmd)}")
        print(f"Log file: {log_file}")
        with open(log_file, "w", encoding="utf-8") as log_f:
            proc = subprocess.Popen(cmd, stdout=log_f, stderr=log_f, text=True)
        processes.append(("consumer", proc, log_file))
    return processes


def wait_for_processes(processes: List[Tuple[str, subprocess.Popen, str]]) -> None:
    for process_type, proc, log_file in processes:
        proc.wait()
        if proc.returncode != 0:
            raise RuntimeError(
                f"Process {process_type} failed (log: {log_file}),"
                f" return code: {proc.returncode}"
            )


def verify_quick_and_fair(log_files: List[str]) -> None:
    consume_counts: Dict[str, int] = {}
    producer_total_counts: Dict[str, int] = {}  # Total messages produced by each producer
    consumer_from_producer: Dict[str, Dict[str, int]] = {}  # consumer_id -> {producer_id -> count}

    for log_file in log_files:
        try:
            with open(log_file, "r", encoding="utf-8") as file_obj:
                for line in file_obj:
                    line = line.strip()
                    if line.startswith("PRODUCER_") and "_TOTAL:" in line:
                        # Format: PRODUCER_{id}_TOTAL:{count}
                        parts = line.split("_TOTAL:")
                        if len(parts) == 2:
                            producer_id = parts[0].replace("PRODUCER_", "")
                            count = int(parts[1])
                            producer_total_counts[producer_id] = count
                    elif line.startswith("CONSUMER_COUNT:"):
                        _, consumer_id, count = line.split(":", 2)
                        consume_counts[consumer_id] = int(count)
                    elif line.startswith("CONSUMER_FROM_PRODUCER:"):
                        # Format: CONSUMER_FROM_PRODUCER:{consumer_id}:{producer_id}:{count}
                        parts = line.split(":", 3)
                        if len(parts) == 4:
                            _, consumer_id, producer_id, count = parts
                            if consumer_id not in consumer_from_producer:
                                consumer_from_producer[consumer_id] = {}
                            consumer_from_producer[consumer_id][producer_id] = int(count)
        except FileNotFoundError:
            raise RuntimeError(f"Missing log file: {log_file}") from None

    # Verify all producers produced their expected message counts based on weight
    if len(producer_total_counts) != PRODUCER_COUNT:
        raise AssertionError(
            f"Expected {PRODUCER_COUNT} producers, got {len(producer_total_counts)}"
        )
    
    total_weight = sum(PRODUCER_WEIGHTS)
    for producer_idx, weight in enumerate(PRODUCER_WEIGHTS):
        producer_id = f"P{producer_idx}"
        expected_count = int(MESSAGE_COUNT * weight / total_weight)
        actual_count = producer_total_counts.get(producer_id, 0)
        if actual_count != expected_count:
            raise AssertionError(
                f"Producer {producer_id} (weight={weight}) produced {actual_count} messages, "
                f"expected {expected_count}"
            )
    
    # Verify consumer counts
    if len(consume_counts) != CONSUMER_COUNT:
        raise AssertionError(
            f"Expected {CONSUMER_COUNT} consumer counts, got {consume_counts}"
        )

    # Aggregate consumed messages from all producers across all consumers
    total_produced = sum(producer_total_counts.values())
    total_consumed = sum(consume_counts.values())
    if total_consumed != total_produced:
        raise AssertionError(
            f"Expected total consumed {total_produced}, got {total_consumed}"
        )
    if any(count == 0 for count in consume_counts.values()):
        raise AssertionError(
            f"Each consumer should receive messages, counts: {consume_counts}"
        )

    # Verify weight-based distribution
    # Calculate total messages consumed from each producer across all consumers
    producer_consumed_totals: Dict[str, int] = {}
    for consumer_id, producer_counts in consumer_from_producer.items():
        for producer_id, count in producer_counts.items():
            producer_consumed_totals[producer_id] = producer_consumed_totals.get(producer_id, 0) + count
    
    # Verify that consumption totals match production totals
    # Each producer produces MESSAGE_COUNT * (weight / total_weight) messages
    # Consumers should consume all of them
    total_weight = sum(PRODUCER_WEIGHTS)
    for producer_idx, weight in enumerate(PRODUCER_WEIGHTS):
        producer_id = f"P{producer_idx}"  # Match the ID format used in logs (P0, P1, P2, P3)
        consumed_from_this = producer_consumed_totals.get(producer_id, 0)
        expected_produced = int(MESSAGE_COUNT * weight / total_weight)
        
        # Verify consumed count matches what was produced
        if consumed_from_this != expected_produced:
            raise AssertionError(
                f"Producer {producer_id} (weight={weight}) produced {expected_produced} messages, "
                f"but consumers only consumed {consumed_from_this}"
            )
        
        # Verify consumption ratio matches weight ratio
        expected_ratio = weight / total_weight
        actual_ratio = consumed_from_this / total_consumed if total_consumed > 0 else 0
        
        # Use WEIGHT_TOLERANCE_PERCENT constant
        tolerance = WEIGHT_TOLERANCE_PERCENT
        if abs(actual_ratio - expected_ratio) > tolerance:
            raise AssertionError(
                f"Producer {producer_id} (weight={weight}) consumption ratio {actual_ratio:.2%} "
                f"deviates from expected {expected_ratio:.2%} by more than {tolerance:.0%}"
            )
        print(f"✅ Producer {producer_id} (weight={weight}): consumed {consumed_from_this} messages, "
              f"ratio {actual_ratio:.2%} (expected {expected_ratio:.2%})")

    # Verify per-consumer weight-based distribution
    # Each consumer should also follow the weight distribution when consuming from different producers
    total_weight = sum(PRODUCER_WEIGHTS)
    weight_map = {f"P{i}": PRODUCER_WEIGHTS[i] for i in range(PRODUCER_COUNT)}
    
    for consumer_id, producer_counts in consumer_from_producer.items():
        consumer_total = sum(producer_counts.values())
        
        # Only check if consumer consumed enough messages for statistical significance
        if consumer_total >= MIN_MESSAGES_FOR_WEIGHT_CHECK:
            print(f"\n📊 Checking weight distribution for {consumer_id} (consumed {consumer_total} messages):")
            
            for producer_id, count in producer_counts.items():
                weight = weight_map.get(producer_id, 1)
                expected_ratio = weight / total_weight
                actual_ratio = count / consumer_total if consumer_total > 0 else 0
                deviation = abs(actual_ratio - expected_ratio)
                
                # Use more lenient tolerance for per-consumer checks
                tolerance = WEIGHT_TOLERANCE_PERCENT_PER_CONSUMER
                
                status = "✅" if deviation <= tolerance else "⚠️"
                print(f"  {status} {producer_id} (weight={weight}): {count} messages, "
                      f"ratio {actual_ratio:.2%} (expected {expected_ratio:.2%}, "
                      f"deviation {deviation:.2%})")
                
                if deviation > tolerance:
                    raise AssertionError(
                        f"Consumer {consumer_id}: Producer {producer_id} (weight={weight}) "
                        f"consumption ratio {actual_ratio:.2%} deviates from expected "
                        f"{expected_ratio:.2%} by {deviation:.2%}, exceeding tolerance "
                        f"{tolerance:.0%}"
                    )
        else:
            print(f"\n⏭️  Skipping weight check for {consumer_id}: only consumed {consumer_total} messages "
                  f"(minimum {MIN_MESSAGES_FOR_WEIGHT_CHECK} required)")

    # Verify consumer fairness around the expected per-consumer mean.
    expected_per_consumer = total_consumed / CONSUMER_COUNT
    fairness_tolerance = consumer_fairness_tolerance(total_consumed)
    consumer_deviations = {
        consumer_id: abs(count - expected_per_consumer)
        for consumer_id, count in consume_counts.items()
    }
    worst_consumer_id, worst_deviation = max(
        consumer_deviations.items(),
        key=lambda item: item[1],
    )
    if worst_deviation > fairness_tolerance:
        raise AssertionError(
            "Consumer load not balanced around expected mean: "
            f"expected={expected_per_consumer:.2f}, "
            f"worst_consumer={worst_consumer_id}, "
            f"worst_deviation={worst_deviation:.2f}, "
            f"tolerance={fairness_tolerance}, "
            f"counts={consume_counts}"
        )

    print(
        "✅ quick_and_fair_consume checks passed: "
        f"balanced consumer counts {consume_counts} "
        f"(expected={expected_per_consumer:.2f}, tolerance={fairness_tolerance})"
    )


def consumer_fairness_tolerance(total_consumed: int) -> int:
    consume_probability = 1.0 / CONSUMER_COUNT
    variance = total_consumed * consume_probability * (1.0 - consume_probability)
    return max(1, math.ceil(math.sqrt(variance) * FAIR_CONSUMER_SIGMA_MULTIPLIER))


def clean_namespace() -> None:
    if LOG_DIR.exists():
        shutil.rmtree(LOG_DIR)
    with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
        # Delete unique mapping and its lock key to keep this scenario deterministic across reruns.
        etcd_client.delete(_new_unique_mapping_key(CHANNEL_KEY))
        etcd_client.delete(_new_unique_lock_key(CHANNEL_KEY))
        etcd_client.delete_prefix("/mpmc_channels")
        etcd_client.delete_prefix("/channels")
        # Delete all producer done keys with correct format
        for p_idx in range(PRODUCER_COUNT):
            producer_id = f"P{p_idx}"
            etcd_client.delete(f"{PRODUCER_DONE_KEY}_{producer_id}")


def reset_producer_done_flag() -> None:
    with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
        # Delete legacy unsuffixed key (if any)
        etcd_client.delete(PRODUCER_DONE_KEY)
        # Delete all per-producer done keys
        for p_idx in range(PRODUCER_COUNT):
            producer_id = f"P{p_idx}"
            etcd_client.delete(f"{PRODUCER_DONE_KEY}_{producer_id}")


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


def _bootstrap_process_state(
    *,
    bootstrap_proc: subprocess.Popen,
    bootstrap_log: str,
) -> Optional[str]:
    rc = bootstrap_proc.poll()
    if rc is None:
        return None
    return (
        f"bootstrap producer exited rc={rc} log={bootstrap_log}\n"
        "--- bootstrap log tail ---\n"
        f"{_read_log_tail(bootstrap_log)}\n"
        "--- end bootstrap log tail ---"
    )


def _wait_unique_key_mapping(
    *,
    timeout_seconds: float,
    bootstrap_proc: subprocess.Popen,
    bootstrap_log: str,
) -> str:
    deadline = time.time() + float(timeout_seconds)
    with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
        while time.time() < deadline:
            bootstrap_state = _bootstrap_process_state(
                bootstrap_proc=bootstrap_proc,
                bootstrap_log=bootstrap_log,
            )
            if bootstrap_state is not None:
                raise RuntimeError(
                    "Bootstrap producer exited before publishing channel mapping: "
                    f"unique_key={CHANNEL_KEY!r} {bootstrap_state}"
                )
            value, _ = etcd_client.get(_new_unique_mapping_key(CHANNEL_KEY))
            if value is not None:
                try:
                    chan_id = value.decode("utf-8")
                except Exception as err:  # noqa: BLE001
                    raise RuntimeError(
                        f"Invalid channel mapping value for unique_key={CHANNEL_KEY!r}: {value!r}, err={err}"
                    ) from None
                if chan_id.isdigit():
                    return chan_id
                raise RuntimeError(
                    f"Invalid channel mapping for unique_key={CHANNEL_KEY!r}: {chan_id!r} (expected digit-only chan_id)"
                )
            time.sleep(0.2)
    bootstrap_state = _bootstrap_process_state(
        bootstrap_proc=bootstrap_proc,
        bootstrap_log=bootstrap_log,
    )
    raise RuntimeError(
        "Timed out waiting for channel mapping "
        f"unique_key={CHANNEL_KEY!r} in etcd after {timeout_seconds}s "
        f"log={bootstrap_log} state={bootstrap_state or 'bootstrap producer still running'}"
    )


def run_producer(env, args: Dict[str, Any]) -> None:
    chan_type = chan_type_from_string(args["chan_type"])
    producer_id = args.get("producer_id", 0)
    weight = args.get("weight", 1)
    prev_type, prev_ip = env.backend_type, env.backend_ip
    configure_backend(
        env,
        backend_type=args["backend_type"],
        backend_ip=args["ip"],
    )
    try:
        setup_test_environment(logging)
        # Precondition: ensure the member key for this instance_key is absent before creating the store.
        _wait_fluxon_member_absent(f"quick_producer_{producer_id}_main")
        store = require_store(env, f"quick_producer_{producer_id}")
        # Precondition: allow P2P/master handshake to settle before binding.
        time.sleep(5.0)
        # Put weight into chan_config
        producer_config = CHAN_CONFIG_TEST.copy()
        producer_config["weight"] = weight
        producer = new_test_producer(
            args["construct_type"],
            store,
            None,
            producer_config,
            args["new_or_bind_key"],
            chan_type,
        )
        
        # Calculate messages to produce based on weight
        # Total messages = MESSAGE_COUNT, distributed by weight ratio
        total_weight = sum(PRODUCER_WEIGHTS)
        messages_to_produce = int(MESSAGE_COUNT * weight / total_weight)
        
        print(f"🚀 Producer {producer_id} (weight={weight}) starting: will produce {messages_to_produce} messages")
        
        produced = 0
        try:
            for index in range(messages_to_produce):
                # Message format: "quick-msg-{producer_id}-{index}" (e.g., "quick-msg-P0-123")
                msg_id = f"quick-msg-{producer_id}-{index}"
                payload = msg_id.encode()
                msg = {"unique_id": msg_id, "payload": payload}
                res = producer.put_data(msg)
                if not res.is_ok():
                    # Explicitly consume the error for strict Result semantics
                    err = res.unwrap_error()
                    print(f"❌ Producer {producer_id} FAILED at message {index}: {err}")
                    raise RuntimeError(
                        f"Producer {producer_id} failed at message {index}: {err}"
                    )
                else:
                    # Consume ok branch to satisfy Result destructor contract
                    _ok = res.unwrap()
                produced += 1
                if produced % 10 == 0:
                    print(f"📊 Producer {producer_id} progress: {produced}/{messages_to_produce}")
                time.sleep(random.uniform(0.01, 0.05))
            print(f"✅ Producer {producer_id} completed ALL {produced} messages successfully!")
        except Exception as e:
            print(f"💥 Producer {producer_id} exception after {produced} messages: {type(e).__name__}: {e}")
            raise
        finally:
            print(f"PRODUCER_{producer_id}_TOTAL:{produced}")
            print(f"✅ Producer {producer_id} (weight={weight}) finished: produced {produced} messages")
            try:
                # Close returns Result[OkNone, ApiError]; consume explicitly
                producer.close().unwrap()
            finally:
                with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
                    etcd_client.put(f"{PRODUCER_DONE_KEY}_{producer_id}", str(produced))
    finally:
        configure_backend(env, backend_type=prev_type, backend_ip=prev_ip)


def run_consumer(env, args: Dict[str, Any]) -> None:
    chan_type = chan_type_from_string(args["chan_type"])
    consumer_id = args["consumer_id"]
    prev_type, prev_ip = env.backend_type, env.backend_ip
    configure_backend(
        env,
        backend_type=args["backend_type"],
        backend_ip=args["ip"],
    )
    try:
        setup_test_environment(logging)
        # Precondition: ensure the member key for this instance_key is absent before creating the store.
        _wait_fluxon_member_absent(f"quick_consumer_{consumer_id}_main")
        store = require_store(env, f"quick_consumer_{consumer_id}")
        # Precondition: allow P2P/master handshake to settle before binding.
        time.sleep(5.0)
        consumer = new_test_consumer(
            args["construct_type"],
            store,
            None,
            CHAN_CONFIG_TEST,
            args["new_or_bind_key"],
            chan_type,
        )
        consumed = 0
        producer_consumed_counts: Dict[str, int] = {}  # Track messages per producer
        last_activity = time.monotonic()
        idle_timeout = 3.0
        start_time = time.monotonic()
        max_deadline = start_time + MAX_CONSUMER_RUNTIME
        try:
            with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
                all_producers_done = False
                last_producer_check = time.monotonic()
                producer_check_interval = 0.5

                while True:
                    now = time.monotonic()

                    if now - last_producer_check >= producer_check_interval:
                        if not all_producers_done:
                            done_count = 0
                            for p_idx in range(PRODUCER_COUNT):
                                producer_id = f"P{p_idx}"
                                value, _metadata = etcd_client.get(f"{PRODUCER_DONE_KEY}_{producer_id}")
                                if value is not None:
                                    done_count += 1
                            all_producers_done = done_count == PRODUCER_COUNT
                            if all_producers_done:
                                import sys
                                msg = f"🎉 Consumer {consumer_id}: All {PRODUCER_COUNT} producers done! consumed={consumed}"
                                print(msg, file=sys.stdout, flush=True)
                        last_producer_check = now

                    res = consumer.get_data(batch_size=1, try_time=1)

                    if res is None:
                        now = time.monotonic()
                        if now >= max_deadline:
                            raise RuntimeError(
                                f"Consumer {consumer_id} get_data returned None unexpectedly"
                            )
                    elif res.is_ok():
                        success = res.unwrap()
                        now = time.monotonic()
                        if isinstance(success, list) and success:
                            consumed += 1
                            last_activity = now
                            if isinstance(success[0], dict):
                                msg_key = str(success[0]["unique_id"])
                                if msg_key.startswith("quick-msg-"):
                                    parts = msg_key.split("-")
                                    if len(parts) >= 3:
                                        producer_id_str = parts[2]
                                        producer_consumed_counts[producer_id_str] = producer_consumed_counts.get(producer_id_str, 0) + 1
                    else:
                        err = res.unwrap_error()
                        now = time.monotonic()
                        if isinstance(err, MessageConsumptionNoNewMessageError):
                            if now >= max_deadline:
                                raise RuntimeError(
                                    f"Consumer {consumer_id} exceeded max runtime with no new message"
                                )
                        else:
                            raise RuntimeError(
                                f"Consumer {consumer_id} get_data failed: {err}"
                            )

                    idle_time = now - last_activity
                    if all_producers_done and idle_time >= idle_timeout:
                        print(
                            f"✅ Consumer {consumer_id} exiting: all producers done and idle for {idle_time:.1f}s (consumed {consumed} messages)"
                        )
                        break
                    if now >= max_deadline:
                        raise RuntimeError(
                            f"Consumer {consumer_id} exceeded max runtime"
                        )
                    time.sleep(PRODUCER_DONE_POLL_INTERVAL)
            if consumed == 0:
                raise AssertionError(
                    f"Consumer {consumer_id} did not receive any message"
                )
            print(f"CONSUMER_COUNT:{consumer_id}:{consumed}")
            # Print per-producer consumption counts
            for p_id, count in producer_consumed_counts.items():
                print(f"CONSUMER_FROM_PRODUCER:{consumer_id}:{p_id}:{count}")
        finally:
            # Consume close result to satisfy strict unwrap policy
            consumer.close().unwrap()
    finally:
        configure_backend(env, backend_type=prev_type, backend_ip=prev_ip)


def log_path(process_type: str, cmd: List[str]) -> str:
    if process_type == "producer":
        identifier = "unknown"
        for index, arg in enumerate(cmd):
            if arg == "--producer_id" and index + 1 < len(cmd):
                identifier = cmd[index + 1]
                break
        return str(LOG_DIR / f"quick_fair_producer_{identifier}.log")
    identifier = "unknown"
    for index, arg in enumerate(cmd):
        if arg == "--consumer_id" and index + 1 < len(cmd):
            identifier = cmd[index + 1]
            break
    return str(LOG_DIR / f"quick_fair_consumer_{identifier}.log")



if __name__ == "__main__":
    cli()
