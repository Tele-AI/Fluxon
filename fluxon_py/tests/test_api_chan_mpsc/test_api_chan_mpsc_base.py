"""
Test harness for MPSC channel behaviour.

This module restructures the legacy script into a class-based layout so that
future scenario files can inherit shared helpers.
"""
from __future__ import annotations

import argparse
import copy
import os
import random
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import etcd3
import gc

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from fluxon_py.api_error import (  # noqa: E402
    ChanCreateError,
    ChanDeleteError,
    ChanIdxDuplicateError,
    ChanKeyNotFoundError,
    ChanMessageConsumptionError,
    ChanMessageProduceError,
    ConsumerRegistrationError,
    ProducerRegistrationError,
)
from fluxon_py.kvclient.kvclient_interface import KvClient  # noqa: E402
from fluxon_py.api_ext_chan import (  # noqa: E402
    MPMCChanConsumer,
    MPMCChanProducer,
    MPSCChanConsumer,
    MPSCChanProducer,
    ChanRole,
    ChanType,
    chan_bind,
    chan_new,
    chan_unbind,
    _new_unique_lock_key,
    _new_unique_mapping_key,
    new_or_bind_with_unique_key,
)
from fluxon_py import api_ext_chan  # noqa: E402
# no direct store constructors here; use test_lib helpers
from fluxon_py.api_error import ApiError, Result, OkNone  # noqa: E402
from fluxon_py._api_ext_chan.mpsc import (  # noqa: E402
    _new_produce_offset_of_all_producer_key,
)
from fluxon_py.logging import init_logger  # noqa: E402
from fluxon_py.tests.test_lib import (  # noqa: E402
    KV_SVC_IP,
    KV_SVC_TYPE,
    CHAN_CONFIG_TEST,
    TEST_TIMEOUT_SECONDS,
    ETCD_HOST,
    ETCD_PORT,
    MOONCAKE_MASTER_SERVER_ADDRESS,
    MOONCAKE_METADATA_SERVER,
    new_shared_stores,
    check_chan_key_all_removed,
    manully_unbind_if_cstyle_construct,
    new_shared_stores,
    new_test_consumer,
    new_test_producer,
    setup_test_environment,
    run_with_argmatrix,
    pre_kill_existing_test_processes,
)

logging = init_logger()

# This harness spawns multiple producers concurrently and each message is a flat dict payload.
# capacity=10 (the generic test default) is too small and can cause producers to block indefinitely
# on the internal MPMC-per-MPSC prefix capacity check.
CHAN_CONFIG_MPSC_TEST = dict(CHAN_CONFIG_TEST)
CHAN_CONFIG_MPSC_TEST["capacity"] = 200


# Exit status verification markers
PRODUCER_NORMAL_EXIT_MARKER = "PRODUCER_NORMAL_EXIT:"
PRODUCER_CRASH_MARKER = "PRODUCER_CRASH:"
CONSUMER_NORMAL_EXIT_MARKER = "CONSUMER_NORMAL_EXIT:"
CONSUMER_CRASH_MARKER = "CONSUMER_CRASH:"

# Construction readiness markers: printed once a subprocess finishes store+channel construction
PRODUCER_CONSTRUCTED_MARKER = "PRODUCER_CONSTRUCTED:"
CONSUMER_CONSTRUCTED_MARKER = "CONSUMER_CONSTRUCTED:"

# Pre-init must stay alive to keep MPMC payload leases valid; the parent releases it via an etcd key.
# Time limits are explicit to avoid orphaned pre_init processes when the parent crashes.
PRE_INIT_RELEASE_KEY_PREFIX = "/test_api_chan_mpsc_preinit_release"
PRE_INIT_RELEASE_WAIT_SECONDS = 300


TEST_INACTIVE_TIME = 0

# CI runs should be deterministic. Random crash simulation is useful for manual stress
# but makes CI flaky and can leave missing exit markers when the interpreter aborts.
ENABLE_CRASH_SIMULATION = False




class ChannelState:
    """Lightweight container for test harness state."""

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
        self.stores: Dict[str, KvClient] = {}
        self.store_lock = threading.Lock()
        self.logger = logging


def create_channel_env(
    *,
    backend_type: Optional[str] = None,
    backend_ip: Optional[str] = None,
) -> "ChannelState":
    """Return a mutable container that tracks backend/test resources."""
    default_type = backend_type or KV_SVC_TYPE
    default_ip = backend_ip or KV_SVC_IP
    return ChannelState(default_type, default_ip)


def configure_backend(
    env: "ChannelState",
    *,
    backend_type: Optional[str] = None,
    backend_ip: Optional[str] = None,
) -> None:
    target_type = backend_type if backend_type is not None else env.backend_type
    target_ip = backend_ip if backend_ip is not None else env.backend_ip
    if target_type != env.backend_type or target_ip != env.backend_ip:
        release(env)
    env.backend_type = target_type
    env.backend_ip = target_ip


def restore_default_backend(env: "ChannelState") -> None:
    configure_backend(
        env,
        backend_type=env.default_backend_type,
        backend_ip=env.default_backend_ip,
    )


def require_store(
    env: "ChannelState",
    instance_key: str,
    *,
    backend_type: Optional[str] = None,
    backend_ip: Optional[str] = None,
) -> KvClient:
    if backend_type is not None or backend_ip is not None:
        configure_backend(env, backend_type=backend_type, backend_ip=backend_ip)
    return _get_or_create_store(env, instance_key)


def require_channel(
    env: "ChannelState",
    *,
    construct_type: str,
    store: KvClient,
    chan_config: Dict[str, int],
    role: ChanRole,
    chan_type: ChanType = ChanType.MPSC,
    chan_id: Optional[str] = None,
    new_or_bind_key: Optional[str] = None,
) -> Union[MPSCChanProducer, MPMCChanProducer, MPSCChanConsumer, MPMCChanConsumer]:
    if role == ChanRole.PRODUCER:
        return new_test_producer(
            construct_type,
            store,
            chan_id,
            chan_config,
            new_or_bind_key,
            chan_type,
        )
    if role == ChanRole.CONSUMER:
        return new_test_consumer(
            construct_type,
            store,
            chan_id,
            chan_config,
            new_or_bind_key,
            chan_type,
        )
    raise ValueError(f"Unsupported channel role: {role}")


def release(env: "ChannelState", *resources: Union[str, KvClient]) -> None:
    targets: Tuple[Union[str, KvClient], ...]
    if resources:
        targets = resources
    else:
        targets = tuple(env.stores.keys())

    with env.store_lock:
        for identifier in targets:
            name: Optional[str] = None
            store_obj: Optional[KvClient]
            if isinstance(identifier, str):
                name = identifier
                store_obj = env.stores.pop(name, None)
            else:
                store_obj = identifier
                for key, value in list(env.stores.items()):
                    if value is store_obj:
                        name = key
                        env.stores.pop(key)
                        break
            if store_obj is None:
                continue
            try:
                print(f"test close store {name} begin", flush=True)
                res = store_obj.close()
                # Strict Result policy: must consume explicitly; errors are logged.
                if res.is_ok():
                    _ = res.unwrap()
                else:
                    err = res.unwrap_error()
                    env.logger.warning(
                        "Failed to close store %s: %s", name or repr(store_obj), err
                    )
                print(f"test close store {name} done", flush=True)

            except Exception as exc:  # noqa: BLE001
                env.logger.warning(
                    "Failed to close store %s: %s", name or repr(store_obj), exc
                )


def _get_or_create_store(env: "ChannelState", instance_key: str) -> KvClient:
    with env.store_lock:
        store = env.stores.get(instance_key)
        if store is None:
            store = _create_store(env, instance_key)
            env.stores[instance_key] = store
        return store


def _create_store(env: "ChannelState", instance_key: str) -> KvClient:
    # Reuse the unified constructor so etcd endpoints and related config are consistent
    # (controlled by tests/test_lib).
    store_list = new_shared_stores(
        instance_key,
        1,
        backend_type=env.backend_type,
        ip=env.backend_ip,
    )
    return store_list[0]



SCRIPT_PATH = Path(__file__).resolve()


def build_parser() -> argparse.ArgumentParser:
    """Construct the internal CLI parser for the harness subprocesses."""
    parser = argparse.ArgumentParser(description="MPSC Channel Test Runner")
    subparsers = parser.add_subparsers(dest="mode", help="Execution mode")

    subparsers.add_parser("main", help="Run main test suite")

    pre_init_parser = subparsers.add_parser(
        "pre_init", help="Initialize channels and get channel IDs"
    )
    pre_init_parser.add_argument("--backend_type", required=True, type=str)
    pre_init_parser.add_argument("--ip", required=True, type=str)
    pre_init_parser.add_argument("--construct_type", required=True, type=str)
    pre_init_parser.add_argument("--new_or_bind_key", required=True, type=str)
    pre_init_parser.add_argument("--chan_type", required=True, type=str)

    producer_parser = subparsers.add_parser(
        "run_producer", help="Run producer process"
    )
    producer_parser.add_argument("--chan_id", required=True, type=str)
    producer_parser.add_argument("--backend_type", required=True, type=str)
    producer_parser.add_argument("--ip", required=True, type=str)
    producer_parser.add_argument("--construct_type", required=True, type=str)
    producer_parser.add_argument("--new_or_bind_key", required=True, type=str)
    producer_parser.add_argument("--chan_type", required=True, type=str)
    producer_parser.add_argument("--msg_type", type=str, default="bytes")
    producer_parser.add_argument("--process_idx", type=int, default=0)

    consumer_parser = subparsers.add_parser(
        "run_consumer", help="Run consumer process"
    )
    consumer_parser.add_argument("--chan_id", required=True, type=str)
    consumer_parser.add_argument("--backend_type", required=True, type=str)
    consumer_parser.add_argument("--ip", required=True, type=str)
    consumer_parser.add_argument("--construct_type", required=True, type=str)
    consumer_parser.add_argument("--new_or_bind_key", required=True, type=str)
    consumer_parser.add_argument("--chan_type", required=True, type=str)
    consumer_parser.add_argument("--msg_type", type=str, default="bytes")
    consumer_parser.add_argument("--process_idx", type=int, default=0)
    consumer_parser.add_argument("--prefetch", type=int, default=0)
    return parser


def parse_args(parser: Optional[argparse.ArgumentParser] = None) -> argparse.Namespace:
    """Apply default mode handling around the harness parser."""
    parser = parser or build_parser()
    args = parser.parse_args()
    if args.mode is None:
        args.mode = "main"
    return args


def cli(args: Optional[argparse.Namespace] = None) -> None:
    parser = build_parser()
    ns = args or parse_args(parser)
    if ns.mode == "main":
        # Before the main test entry, clean up leftover producer/consumer processes.
        # This is a strict precondition (not fallback logic): if old processes remain,
        # cluster-side payload/lease/offset state can be held for a long time, making the
        # next run's topology/timing uncontrollable. We strictly match this test script
        # + subcommand and only terminate run_producer/run_consumer, then sleep 15s as required.
        # Convention: cleanup is done by script name and executed only at the main entry.
        from fluxon_py.tests.test_lib import pre_kill_existing_test_processes_by_script_name
        pre_kill_existing_test_processes_by_script_name(os.path.basename(str(SCRIPT_PATH)), 15)
        # Ensure argmatrix execution when invoked via CLI
        test_mpsc_channel_suite()
        return
    env = create_channel_env()
    try:
        handlers = {
            "pre_init": run_pre_init,
            "run_producer": run_producer,
            "run_consumer": run_consumer,
        }
        handler = handlers.get(ns.mode)
        if handler is None:
            raise ValueError(f"Unsupported mode: {ns.mode}")
        handler(env, ns)
    finally:
        release(env)



def run_pre_init(env: "ChannelState", args: argparse.Namespace) -> None:
    chan_type = chan_type_from_string(args.chan_type)
    store_key = f"mpsc_test_pre_init_{args.backend_type}"
    prev_type, prev_ip = env.backend_type, env.backend_ip
    configure_backend(env, backend_type=args.backend_type, backend_ip=args.ip)
    try:
        setup_test_environment(logging)
        # Precondition: ensure the member key for this instance_key is absent before creating the store.
        # This avoids a same-run pre_init retry colliding with the still-live member lease.
        _wait_fluxon_member_absent(f"{store_key}_main")
        store = require_store(env, store_key)
        # Note: Producer/Consumer construction allocates/binds payload leases on the Rust side.
        # This depends on P2P/master handshake and RPC readiness. CI previously hit
        # NodeNotConnected/Timeout because pre_init created channels before handshake completion.
        # The fixed short readiness window (1s) is not fallback logic; it explicitly satisfies
        # the precondition to avoid flaky races. See pre_init.log for diagnostics.
        time.sleep(1.0)

        env.logger.info("pre_init with chan_type: %s", chan_type)
        # For new_or_bind construct, ensure two distinct channels by using
        # two different unique keys derived from the provided base key.
        base_key = args.new_or_bind_key
        key_1 = f"{base_key}_1"
        key_2 = f"{base_key}_2"

        producer = new_test_producer(
            args.construct_type,
            store,
            None,
            CHAN_CONFIG_MPSC_TEST,
            key_1,
            chan_type,
        )
        chan_id_1 = producer.get_chan_id()
        env.logger.info(
            "new_test_producer done, chan_id: %s, producer_id: %s",
            chan_id_1,
            producer.get_producer_id(),
        )
        consumer = new_test_consumer(
            args.construct_type,
            store,
            None,
            CHAN_CONFIG_MPSC_TEST,
            key_2,
            chan_type,
        )
        chan_id_2 = consumer.get_chan_id()
        env.logger.info(
            "new_test_consumer done, chan_id: %s, consumer_id: %s",
            chan_id_2,
            consumer.get_consumer_id(),
        )
        # Use a dedicated etcd client for test bookkeeping; do not rely on
        # internal producer attributes that are implementation-specific.
        with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
            etcd_client.delete("/test_api_chan_mpsc_instance_count")
        
        assert chan_id_1 != chan_id_2, "Channel IDs should be different!"

        print(f"CHAN_ID_1:{chan_id_1}", flush=True)
        print(f"CHAN_ID_2:{chan_id_2}", flush=True)

        # Hold the producer/consumer handles so the MPMC payload lease stays valid until the
        # parent process finishes launching all children. The parent will signal release via etcd.
        _wait_for_pre_init_release(base_key)
        producer.close().unwrap()
        if chan_type == ChanType.MPSC:
            assert producer_2 is not None
            producer_2.close().unwrap()
        else:
            assert consumer is not None
            consumer.close().unwrap()
    finally:
        configure_backend(env, backend_type=prev_type, backend_ip=prev_ip)
    release(env, store_key)


def run_producer(env: "ChannelState", args: argparse.Namespace) -> None:
    chan_type = chan_type_from_string(args.chan_type)
    store_key = f"mpsc_test_producer_{args.process_idx}"
    prev_type, prev_ip = env.backend_type, env.backend_ip
    configure_backend(env, backend_type=args.backend_type, backend_ip=args.ip)
    try:
        setup_test_environment(logging)
        store = require_store(env, store_key)
        # Same rationale as run_pre_init: ensure RPC is ready before constructing channels to avoid
        # allocate_lease hitting NodeNotConnected/Timeout. Wait 1s here.
        time.sleep(1.0)
        producer = new_test_producer(
            args.construct_type,
            store,
            args.chan_id,
            CHAN_CONFIG_MPSC_TEST,
            args.new_or_bind_key,
            chan_type,
        )
        # mark constructed to help orchestrator detect readiness
        print(f"{PRODUCER_CONSTRUCTED_MARKER} {producer.get_producer_id()}", flush=True)
        # Instance-count bookkeeping uses a dedicated etcd client, independent
        # from the channel implementation details.
        with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
            map_instance_count(env, etcd_client, lambda count: count + 1)

            try:
                time.sleep(2)
                for i in range(10):
                    # Rust-backed MPSC/MPMC expects a flat dict payload (no nested structures) and
                    # requires `unique_id` to be present.
                    uid = f"msg-{producer.get_producer_id()}-{i}"
                    if args.msg_type == "bytes":
                        data_to_put: Any = {"unique_id": uid, "payload": f"msg-{i}".encode()}
                    elif args.msg_type == "bytes_with_meta":
                        data_to_put = {"unique_id": uid, "meta": f"msg-{i}", "payload": f"msg-{i}".encode()}
                    else:
                        raise ValueError(f"Unsupported msg_type: {args.msg_type}")
                    res = producer.put_data(data_to_put)
                    # Directly unwrap to enforce explicit consumption and fail fast on error
                    okv = res.unwrap()
                    env.logger.info(
                        "[Producer-%s] put msg-%s: %s, channel: %s",
                        producer.get_producer_id(),
                        i,
                        okv,
                        args.chan_id,
                    )
                    if random.random() > 0.9:
                        if not ENABLE_CRASH_SIMULATION:
                            continue
                        flag = {"do_crashdown": False}

                        def crashdown(instance_count: int) -> int:
                            if instance_count > 1:
                                instance_count -= 1
                                flag["do_crashdown"] = True
                                env.logger.debug(
                                    "crashdown, instance_count: %s", instance_count
                                )
                                return instance_count
                            env.logger.debug(
                                "can't crashdown, instance_count: %s", instance_count
                            )
                            return instance_count

                        map_instance_count(env, etcd_client, crashdown)
                        if flag["do_crashdown"]:
                            env.logger.info(
                                "Simulate for crashdown! Producer-%s in run_producer",
                                producer.get_producer_id(),
                            )
                            print(f"{PRODUCER_CRASH_MARKER} {producer.get_producer_id()}", flush=True)
                            os._exit(1)
                    time.sleep(random.randrange(2, 5))
            finally:
                map_instance_count(env, etcd_client, lambda count: count - 1)
        # Call close() first (drop the PyO3 handle) before printing the normal-exit marker,
        # to ensure ChanManager and its lease keepalive are unregistered before process exit.
        producer.close().unwrap()
        print(f"{PRODUCER_NORMAL_EXIT_MARKER} {producer.get_producer_id()}", flush=True)
        os._exit(0)
    finally:
        configure_backend(env, backend_type=prev_type, backend_ip=prev_ip)
    release(env, store_key)


def run_consumer(env: "ChannelState", args: argparse.Namespace) -> None:
    chan_type = chan_type_from_string(args.chan_type)
    store_key = f"mpsc_test_consumer_{args.process_idx}"
    prev_type, prev_ip = env.backend_type, env.backend_ip
    configure_backend(env, backend_type=args.backend_type, backend_ip=args.ip)
    try:
        setup_test_environment(logging)
        store = require_store(env, store_key)
        # Same rationale as run_pre_init: ensure RPC is ready before constructing channels. Wait 1s here.
        time.sleep(1.0)
        consumer = new_test_consumer(
            args.construct_type,
            store,
            args.chan_id,
            CHAN_CONFIG_MPSC_TEST,
            args.new_or_bind_key,
            chan_type,
        )
        # mark constructed to help orchestrator detect readiness
        print(f"{CONSUMER_CONSTRUCTED_MARKER} {consumer.get_consumer_id()}", flush=True)
        # Use an explicit etcd client for crashdown bookkeeping to decouple
        # tests from internal consumer implementation details.
        with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
            map_instance_count(env, etcd_client, lambda count: count + 1)
            try:
                time.sleep(5)
                for _ in range(10):
                    # Keep each get_data attempt bounded to keep CI runtime deterministic.
                    res = consumer.get_data(batch_size=1, try_time=10, prefetch_num=args.prefetch)
                    if res.is_ok():
                        success = res.unwrap()
                        env.logger.info(
                            "[Consumer-%s] got: %s, channel: %s",
                            consumer.get_chan_id(),
                            success,
                            args.chan_id,
                        )
                        assert isinstance(success, list)
                        if success:
                            item = success[0]
                            if args.msg_type == "bytes":
                                assert isinstance(item, dict)
                                payload = item["payload"]
                                assert isinstance(payload, bytes)
                                assert payload.decode().startswith("msg-")
                            elif args.msg_type == "bytes_with_meta":
                                assert isinstance(item, dict)
                                meta = item["meta"]
                                payload = item["payload"]
                                assert isinstance(meta, str)
                                assert isinstance(payload, bytes)
                                assert meta.startswith("msg-")
                    else:
                        env.logger.info(
                            "[Consumer-%s] error: %s, channel: %s",
                            consumer.get_chan_id(),
                            res.unwrap_error(),
                            args.chan_id,
                        )
                    if random.random() > 0.9:
                        if not ENABLE_CRASH_SIMULATION:
                            continue
                        flag = {"do_crashdown": False}

                        def crashdown(instance_count: int) -> int:
                            if instance_count > 1:
                                instance_count -= 1
                                flag["do_crashdown"] = True
                                env.logger.debug(
                                    "crashdown, instance_count: %s", instance_count
                                )
                                return instance_count
                            env.logger.debug(
                                "can't crashdown, instance_count: %s", instance_count
                            )
                            return instance_count

                        map_instance_count(env, etcd_client, crashdown)
                        if flag["do_crashdown"]:
                            env.logger.info(
                                "Simulate for crashdown! Consumer-%s in run_consumer",
                                consumer.get_chan_id(),
                            )
                            print(f"{CONSUMER_CRASH_MARKER} {consumer.get_consumer_id()}", flush=True)
                            os._exit(1)
                    time.sleep(random.randrange(2, 5))
            finally:
                map_instance_count(env, etcd_client, lambda count: count - 1)
        # Likewise, close() before printing the exit marker to avoid the outer harness entering
        # the TTL waiting window while a final keepalive may still have refreshed recently.
        consumer.close().unwrap()
        print(f"{CONSUMER_NORMAL_EXIT_MARKER} {consumer.get_consumer_id()}", flush=True)
        os._exit(0)
    finally:
        configure_backend(env, backend_type=prev_type, backend_ip=prev_ip)
    release(env, store_key)


def run_main(env: "ChannelState", _: argparse.Namespace, *, prefetch: int = 0) -> None:
    prev_type, prev_ip = env.backend_type, env.backend_ip
    configure_backend(
        env,
        backend_type=env.default_backend_type,
        backend_ip=env.default_backend_ip,
    )
    try:
        setup_test_environment(logging)
        clear_channels(env)
        test_global_chan_id_allocator_monotonic_regression()
        clear_channels(env)
        construct_types = ["new_or_bind"]
        chan_types = [ChanType.MPSC]

        for construct_type in construct_types:
            for chan_type in chan_types:
                setup_test_environment(logging)
                env.logger.info(
                    "\n\n=== Testing %s with construct_type = %s ===",
                    chan_type,
                    construct_type,
                )
                test_mpsc_producer_consumer(
                    env,
                    env.backend_type,
                    env.backend_ip,
                    construct_type,
                    "bytes",
                    chan_type,
                    prefetch=prefetch,
                )

                # #     sleep_for_chan_expired(env, chan_type)
                # setup_test_environment(logging)
                # test_inactive_producer(
                #     env,
                #     env.backend_type,
                #     env.backend_ip,
                #     construct_type,
                #     prefetch=prefetch,
                # )
                sleep_for_chan_expired(env, ChanType.MPSC)

        env.logger.info(
            "\n\n=== Testing MPSC with construct_type = %s and msg_type = bytes_with_meta ===",
            construct_types[0],
        )
        for chan_type in chan_types:
            setup_test_environment(logging)
            test_mpsc_producer_consumer(
                env,
                env.backend_type,
                env.backend_ip,
                construct_types[0],
                "bytes_with_meta",
                chan_type,
                prefetch=prefetch,
            )
            sleep_for_chan_expired(env, chan_type)

        clear_channels(env)
        env.logger.info("✅ All tests finished.")
    finally:
        configure_backend(env, backend_type=prev_type, backend_ip=prev_ip)


def clear_channels(env: "ChannelState") -> None:
    with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
        etcd_client.delete_prefix("/channels")
        etcd_client.delete_prefix("/mpmc_channels")
        etcd_client.delete_prefix("semaphore")


def test_global_chan_id_allocator_monotonic_regression() -> None:
    setup_test_environment(logging)
    env = create_channel_env()
    store_key_a = "mpsc_global_chan_id_allocator_store_a"
    store_key_b = "mpsc_global_chan_id_allocator_store_b"
    unique_key_a = "mpsc_global_chan_id_allocator_case_a"
    unique_key_b = "mpsc_global_chan_id_allocator_case_b"
    producer_a: Optional[MPSCChanProducer] = None
    producer_b: Optional[MPSCChanProducer] = None
    try:
        clear_channels(env)
        with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
            etcd_client.delete_prefix("dist_id_allocator/channels")

        store_a = require_store(env, store_key_a)
        producer_a = new_test_producer(
            "new_or_bind",
            store_a,
            None,
            CHAN_CONFIG_MPSC_TEST,
            unique_key_a,
            ChanType.MPSC,
        )
        assert isinstance(producer_a, MPSCChanProducer)
        chan_id_a = int(producer_a.get_chan_id())

        with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
            value_a, meta_a = etcd_client.get("dist_id_allocator/channels")
            assert value_a is not None, "missing top-level chan_id allocator key after first create"
            assert meta_a is not None, "missing metadata for top-level chan_id allocator key after first create"
            assert int(value_a.decode("utf-8")) == chan_id_a, (
                f"allocator value must match first chan_id: value={value_a!r} chan_id_a={chan_id_a}"
            )
            assert int(meta_a.lease_id) == 0, (
                f"top-level chan_id allocator must be unleased, got lease_id={meta_a.lease_id}"
            )

        close_a = producer_a.close()
        if close_a.is_ok():
            _ = close_a.unwrap()
        else:
            _ = close_a.unwrap_error()
        producer_a = None
        release(env, store_key_a)

        with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
            value_after_close, meta_after_close = etcd_client.get("dist_id_allocator/channels")
            assert value_after_close is not None, (
                "top-level chan_id allocator key must survive channel close so the counter stays monotonic"
            )
            assert meta_after_close is not None, "missing metadata for top-level chan_id allocator after close"
            assert int(value_after_close.decode("utf-8")) == chan_id_a, (
                "closing the first channel must not remove or rewrite the top-level chan_id allocator"
            )
            assert int(meta_after_close.lease_id) == 0, (
                f"top-level chan_id allocator must remain unleased after close, got lease_id={meta_after_close.lease_id}"
            )

        store_b = require_store(env, store_key_b)
        producer_b = new_test_producer(
            "new_or_bind",
            store_b,
            None,
            CHAN_CONFIG_MPSC_TEST,
            unique_key_b,
            ChanType.MPSC,
        )
        assert isinstance(producer_b, MPSCChanProducer)
        chan_id_b = int(producer_b.get_chan_id())
        assert chan_id_b == chan_id_a + 1, (
            f"top-level chan_id allocator must stay monotonic across channel close: "
            f"chan_id_a={chan_id_a} chan_id_b={chan_id_b}"
        )

        with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
            value_b, meta_b = etcd_client.get("dist_id_allocator/channels")
            assert value_b is not None, "missing top-level chan_id allocator key after second create"
            assert meta_b is not None, "missing metadata for top-level chan_id allocator key after second create"
            assert int(value_b.decode("utf-8")) == chan_id_b, (
                f"allocator value must match second chan_id: value={value_b!r} chan_id_b={chan_id_b}"
            )
            assert int(meta_b.lease_id) == 0, (
                f"top-level chan_id allocator must stay unleased after second create, got lease_id={meta_b.lease_id}"
            )
    finally:
        if producer_a is not None:
            close_a = producer_a.close()
            if close_a.is_ok():
                _ = close_a.unwrap()
            else:
                _ = close_a.unwrap_error()
        if producer_b is not None:
            close_b = producer_b.close()
            if close_b.is_ok():
                _ = close_b.unwrap()
            else:
                _ = close_b.unwrap_error()
        release(env)
        clear_channels(env)


def sleep_for_chan_expired(env: "ChannelState", chan_type: ChanType) -> None:
    ttl = CHAN_CONFIG_TEST["ttl_seconds"] + 10
    for index in range(ttl):
        env.logger.info(
            "Sleep %s/%s seconds for chan to be expired.",
            index + 1,
            ttl,
        )
        time.sleep(1)
    with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
        check_chan_key_all_removed(etcd_client, chan_type)


def test_inactive_producer(
    env: "ChannelState",
    backend_type: str,
    ip: str,
    construct_type: str,
    *,
    prefetch: int = 0,
) -> None:
    global TEST_INACTIVE_TIME
    TEST_INACTIVE_TIME += 1
    env.logger.info(
        "=== Testing inactive producer with %s backend ===",
        backend_type,
    )
    new_or_bind_key = "inactive_test"
    base_name = f"inactive_test_{construct_type}_consumer"
    prev_type, prev_ip = env.backend_type, env.backend_ip
    configure_backend(env, backend_type=backend_type, backend_ip=ip)
    try:
        consumer_store = require_store(env, base_name)
        producer_names = [f"{base_name}_producer_{index}" for index in range(4)]
        producer_stores = [require_store(env, name) for name in producer_names]

        # No sleep here: P2P implements a first-available handshake lock.
        # If construction fails, it should be a real error to surface.

        consumer = new_test_consumer(
            construct_type,
            consumer_store,
            None,
            CHAN_CONFIG_MPSC_TEST,
            new_or_bind_key,
        )
        channel_id = consumer.get_chan_id()
        env.logger.info("Get channel_id: %s", channel_id)
        assert channel_id is not None, "Channel id should not be None!"

        producers: List[Optional[Union[MPSCChanProducer, MPMCChanProducer]]] = []
        for store in producer_stores:
            producer = new_test_producer(
                construct_type,
                store,
                channel_id,
                CHAN_CONFIG_MPSC_TEST,
                new_or_bind_key,
            )
            env.logger.info("producer tag: %s", producer.get_producer_id())
            producers.append(producer)

        produced_data: List[bytes] = []
        for index in range(8):
            data = {
                "payload": f"msg-p{index % 4 + 1}-{index + 1}-time{TEST_INACTIVE_TIME}".encode()
            }
            produced_data.append(data)
            result = producers[index % 4].put_data({"payload": data})  # type: ignore[index]
            # Directly unwrap per strict Result policy; raises on error
            result.unwrap()

        def check_all_producer_offset(
            producer_obj: Union[MPSCChanProducer, MPMCChanProducer]
        ) -> None:
            assert isinstance(producer_obj, MPSCChanProducer)
            prefix = _new_produce_offset_of_all_producer_key(
                producer_obj.get_chan_id()
            )
            # Use a fresh etcd client to inspect offsets; producer no longer
            # exposes its internal etcd client in the Rust-backed path.
            with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
                res = list(etcd_client.get_prefix(prefix))
                offset_sum = 0
                for item in res:
                    env.logger.info(
                        "offset kv item: %s, key: %s",
                        item[0].decode(),
                        item[1].key,
                    )
                    offset_sum += int(item[0].decode())
            assert offset_sum == 4, (
                "offset_sum should be 4（beginning is -1）。"
                f" {offset_sum}, offset_kvs: {res}"
            )
            assert len(res) == 4, (
                "offset_kvs length should be 4."
                f" {len(res)}, offset_kvs: {res}"
            )

        check_all_producer_offset(producers[0])  # type: ignore[arg-type]

        down_producer_list: List[str] = []
        for idx, producer in enumerate(producers):
            if (
                producer is not None
                and random.random() >= 0.5
                and len(down_producer_list) < 3
            ):
                down_producer_list.append(producer.get_producer_id())
                env.logger.info(
                    "simulate down producer, destruction supposed to be called: %s",
                    producer.get_producer_id(),
                )
                env.logger.info(f"producer {producer.get_producer_id()} refs: {gc.get_referrers(producer)}")
                # pres = producer.close()
                # if not pres.is_ok():
                #     env.logger.warning("down producer %s close error: %s", producer.get_producer_id(), pres.unwrap_error())
                # else:
                #     _ = pres.unwrap()
                producers[idx] = None
                gc.collect()
        gc.collect()
                
        result = consumer.get_data(8, prefetch_num=prefetch).unwrap()
        env.logger.info("result length: %s", len(result))
        env.logger.info("result: %s", result)
        assert len(result) == 8, "result length should be 8."

        produced_data_all = copy.deepcopy(produced_data)
        for item in result:
            assert isinstance(item, dict)
            payload = item["payload"]
            assert payload in produced_data, (
                "result should be the same as produced_data."
                f" {payload}, results: {result}, produced_data: {produced_data_all}"
            )
            produced_data.remove(payload)

        # Non-blocking-like retry: use get_data with try_time=1 (minimum unit)
        retry_res = consumer.get_data(8, try_time=1)
        if not retry_res.is_ok():
            # When no new message, it is expected to return a typed error
            from fluxon_py.api_error import MessageConsumptionNoNewMessageError
            assert isinstance(retry_res.unwrap_error(), MessageConsumptionNoNewMessageError)
        else:
            retry_result = retry_res.unwrap()
            assert len(retry_result) == 0, f"result should be empty. {retry_result}"

        chan = consumer.get_chan_id()
        env.logger.info("closing consumer & producers of %s", chan)
        # Prepare GC-close verification markers
        from fluxon_py._api_ext_chan import mpsc as _mpsc_mod  # local import for test-only helpers
        # Clear any stale markers for this channel
        # down_producer_list captured producer_ids which we expect to be GC-closed
        for pid in down_producer_list:
            _mpsc_mod.test_clear_close_marker(f"mpsc:producer:{channel_id}:{pid}")
        # also clear markers for consumer and still-alive producers to avoid cross-run leakage
        _mpsc_mod.test_clear_close_marker(
            f"mpsc:consumer:{channel_id}:{consumer.get_consumer_id()}"
        )
        for p0 in producers:
            if p0 is not None:
                _mpsc_mod.test_clear_close_marker(
                    f"mpsc:producer:{channel_id}:{p0.get_producer_id()}"
                )
        # For cstyle construction we must explicitly unbind; new_or_bind path
        # does not use the global registry and thus has nothing to unbind here.
        manully_unbind_if_cstyle_construct(ChanType.MPSC, channel_id, construct_type)

        # Explicitly close still-alive handles to ensure their Rust-side
        # ChanManager drops before we close the underlying store. Relying on
        # GC-only finalizers can race with store shutdown and leave the kvclient
        # keepalive actor briefly calling into a closing client (seen as
        # SystemShutdown in logs). We intentionally keep the randomly
        # "down" producers unclosed to simulate crash.
        # Intentionally skip explicit consumer.close() for now to observe GC-driven
        # shutdown behavior and referrers. This makes the test rely on __del__
        # to trigger close, which helps diagnose unexpected strong references
        # that would keep the consumer alive after handles are dropped.
        env.logger.info("skip explicit consumer.close() to observe GC path")

        # Capture weakrefs before dropping strong references so we can assert
        # objects are really collected (no hidden references left).
        try:
            import weakref as _weakref  # local import for test-only assertion
            _wrefs_producers = [_weakref.ref(pp) for pp in producers if pp is not None]
            _wref_consumer = _weakref.ref(consumer) if consumer is not None else None
        except Exception:
            _wrefs_producers = []
            _wref_consumer = None

        for p in producers:
            if p is None:
                continue
            # Log referrers for debugging, then close explicitly.
            env.logger.info(
                "producer %s refs: %s", p.get_producer_id(), gc.get_referrers(p)
            )
            try:
                pres = p.close()
                if not pres.is_ok():
                    env.logger.warning(
                        "producer %s close error: %s", p.get_producer_id(), pres.unwrap_error()
                    )
                else:
                    _ = pres.unwrap()
                    # explicit close should be recorded as non-GC
                    ptag = f"mpsc:producer:{channel_id}:{p.get_producer_id()}"
                    pval = _mpsc_mod.test_get_close_marker(ptag)
                    assert pval is False, f"producer {ptag} expected explicit close marker False, got {pval}"
            except Exception as e:  # noqa: BLE001
                env.logger.warning("producer %s close raised: %s", p.get_producer_id(), e)

        # Important: clear loop variable reference to avoid keeping the last
        # producer alive via the local name `p` (which would keep its leases
        # alive and block TTL-based cleanup for the channel keys).
        p = None  # type: ignore[assignment]

        producers = []
        consumer = None

        # Drive finalizers deterministically to unregister kvclient leases
        # before stores are closed by the harness. Avoid long sleeps; a few
        # short cycles suffice to flush ref cycles in CPython.
        for _ in range(3):
            gc.collect()
            time.sleep(0.2)

        # Verify that simulated-down producers were GC-closed
        for pid in down_producer_list:
            tag = f"mpsc:producer:{channel_id}:{pid}"
            val = _mpsc_mod.test_get_close_marker(tag)
            assert val is False, f"expected explicit close for {tag}, got {val}"

        # After clearing strong references, print weakref status and traverse up to
        # three levels of referrers for any object that still appears alive.
        # This helps identify the parent chain that keeps it referenced.
        def _short(s: str, n: int = 120) -> str:
            return s if len(s) <= n else (s[: n - 3] + "...")

        def _describe(o: object) -> str:
            tname = type(o).__name__
            rid = hex(id(o))
            try:
                rep = _short(repr(o))
            except Exception:  # repr may raise for some objects; keep robust
                rep = f"<repr-error of {tname}>"
            return f"{tname}@{rid} {rep}"

        def _print_ref_chain(root_obj: object, *, max_levels: int = 3, sample: int = 6) -> None:
            seen: set[int] = set()
            cur = [root_obj]
            for level in range(1, max_levels + 1):
                nxt: list[object] = []
                # Aggregate distinct referrers for this level
                for ch in cur:
                    for r in gc.get_referrers(ch):
                        if id(r) in seen:
                            continue
                        seen.add(id(r))
                        nxt.append(r)
                if not nxt:
                    env.logger.info("weakref parent L%s: (none)", level)
                    break
                # Sample to avoid massive log noise; show types and short reprs
                head = nxt[:sample]
                env.logger.info(
                    "weakref parent L%s count=%s sample=%s",
                    level,
                    len(nxt),
                    [
                        _describe(x if not isinstance(x, dict) else {k: type(v).__name__ for k, v in list(x.items())[:3]})
                        for x in head
                    ],
                )
                cur = nxt

        # Consumer weakref status
        if _wref_consumer is not None:
            cobj = _wref_consumer()
            if cobj is None:
                env.logger.info("consumer weakref: None (collected)")
            else:
                env.logger.info("consumer weakref: ALIVE -> %s", _describe(cobj))
                _print_ref_chain(cobj)

        # Producer weakref status
        for idx, w in enumerate(_wrefs_producers):
            pobj = w()
            if pobj is None:
                env.logger.info("producer[%s] weakref: None (collected)", idx)
            else:
                env.logger.info("producer[%s] weakref: ALIVE -> %s", idx, _describe(pobj))
                _print_ref_chain(pobj)

        env.logger.info("closed consumer & producers of %s", chan)
    finally:
        configure_backend(env, backend_type=prev_type, backend_ip=prev_ip)

    release(env, base_name, *producer_names)


def test_mpsc_producer_consumer(
    env: "ChannelState",
    backend_type: str,
    ip: str,
    construct_type: str,
    msg_type: str,
    chan_type: ChanType,
    *,
    prefetch: int = 0,
) -> None:
    env.logger.info(
        "=== Testing %s with %s backend ===",
        chan_type,
        backend_type,
    )
    ok = test_mpsc_producer_consumer_inner(
        env,
        backend_type,
        ip,
        construct_type,
        msg_type,
        chan_type,
        prefetch=prefetch,
    )
    if not ok:
        raise AssertionError("test_mpsc_producer_consumer failed; aborting")


def test_mpsc_producer_consumer_inner(
    env: "ChannelState",
    backend_type: str,
    ip: str,
    construct_type: str,
    msg_type: str,
    chan_type: ChanType,
    *,
    prefetch: int = 0,
) -> bool:
    # Use a base key and derive two unique ids for two distinct channels
    new_or_bind_key = "mpsc_test"
    new_or_bind_key_1 = f"{new_or_bind_key}_1"
    new_or_bind_key_2 = f"{new_or_bind_key}_2"
    # Create channels in-process and keep the handles alive while subprocesses construct.
    #
    # Rationale: the Rust-backed MQ layer associates the payload lease with the channel manager
    # lifetime. If we create channels in a short-lived pre_init subprocess and close/drop the
    # handles before other processes join, the payload lease can expire (TTL-based) and later
    # bind attempts will fail with LeaseNotFound.
    pre_init_store_key = f"mpsc_test_pre_init_{backend_type}"
    pre_init_store = require_store(env, pre_init_store_key, backend_type=backend_type, backend_ip=ip)
    time.sleep(1.0)
    pre_init_producer_1 = new_test_producer(
        construct_type,
        pre_init_store,
        None,
        CHAN_CONFIG_MPSC_TEST,
        new_or_bind_key_1,
        chan_type,
    )
    chan_id_1 = pre_init_producer_1.get_chan_id()
    # Keep only producer bindings alive during the subprocess phase.
    #
    # MPSC enforces "at most 1 consumer binding" at the Rust layer. If we keep an
    # in-process consumer alive here, it will conflict with the consumer subprocess
    # and make the test nondeterministic (invalid binding state).
    pre_init_producer_2 = new_test_producer(
        construct_type,
        pre_init_store,
        None,
        CHAN_CONFIG_MPSC_TEST,
        new_or_bind_key_2,
        chan_type,
    )
    chan_id_2 = pre_init_producer_2.get_chan_id()
    if not chan_id_1 or not chan_id_2:
        env.logger.warning(
            "Failed to create channel IDs in-process: chan_id_1=%s, chan_id_2=%s",
            chan_id_1,
            chan_id_2,
        )
        pre_init_producer_1.close().unwrap()
        pre_init_producer_2.close().unwrap()
        release(env, pre_init_store_key)
        return False
    if chan_id_1 == chan_id_2:
        env.logger.warning("pre-init produced duplicate channel IDs: %s", chan_id_1)
        pre_init_producer_1.close().unwrap()
        pre_init_producer_2.close().unwrap()
        release(env, pre_init_store_key)
        return False

    processes: List[Tuple[str, List[str]]] = []
    for idx in range(4):
        cmd = [
            sys.executable,
            str(SCRIPT_PATH),
            "run_producer",
            "--chan_id",
            chan_id_1,
            "--backend_type",
            backend_type,
            "--ip",
            ip,
            "--construct_type",
            construct_type,
            "--new_or_bind_key",
            new_or_bind_key_1,
            "--chan_type",
            chan_type.value,
            "--msg_type",
            msg_type,
            "--process_idx",
            str(idx),
        ]
        processes.append(("producer", cmd))

    for idx in range(4):
        cmd = [
            sys.executable,
            str(SCRIPT_PATH),
            "run_producer",
            "--chan_id",
            chan_id_2,
            "--backend_type",
            backend_type,
            "--ip",
            ip,
            "--construct_type",
            construct_type,
            "--new_or_bind_key",
            new_or_bind_key_2,
            "--chan_type",
            chan_type.value,
            "--msg_type",
            msg_type,
            "--process_idx",
            str(idx + 4),
        ]
        processes.append(("producer", cmd))

    consumer_cmd_1 = [
        sys.executable,
        str(SCRIPT_PATH),
        "run_consumer",
        "--chan_id",
        chan_id_1,
        "--backend_type",
        backend_type,
        "--ip",
        ip,
        "--construct_type",
        construct_type,
        "--new_or_bind_key",
        new_or_bind_key_1,
        "--chan_type",
        chan_type.value,
        "--msg_type",
        msg_type,
        "--process_idx",
        "8",
        "--prefetch",
        str(prefetch),
    ]
    processes.append(("consumer", consumer_cmd_1))

    consumer_cmd_2 = [
        sys.executable,
        str(SCRIPT_PATH),
        "run_consumer",
        "--chan_id",
        chan_id_2,
        "--backend_type",
        backend_type,
        "--ip",
        ip,
        "--construct_type",
        construct_type,
        "--new_or_bind_key",
        new_or_bind_key_2,
        "--chan_type",
        chan_type.value,
        "--msg_type",
        msg_type,
        "--process_idx",
        "9",
        "--prefetch",
        str(prefetch),
    ]
    processes.append(("consumer", consumer_cmd_2))

    subprocesses: List[Tuple[str, subprocess.Popen, str]] = []
    try:
        random.shuffle(processes)
        if os.path.exists("logs"):
            shutil.rmtree("logs")
        os.makedirs("logs", exist_ok=True)
        os.system("chmod -R 777 logs")

        for process_type, cmd in processes:
            if process_type == "producer":
                log_file = f"logs/mpsc_producer_{len(subprocesses)}.log"
            else:
                log_file = f"logs/mpsc_consumer_{len(subprocesses)}.log"
            env.logger.info("Starting %s: %s", process_type, " ".join(cmd))
            env.logger.info("Log file: %s", log_file)
            with open(log_file, "w", encoding="utf-8") as log_f:
                proc = subprocess.Popen(cmd, stdout=log_f, stderr=log_f, text=True)
            subprocesses.append((process_type, proc, log_file))

        # Wait up to N seconds for all subprocesses to print their constructed markers.
        start = time.time()
        pending = set(range(len(subprocesses)))
        early_fail_idx: Optional[int] = None
        while pending and (time.time() - start) < float(TEST_TIMEOUT_SECONDS) and early_fail_idx is None:
            done_now: List[int] = []
            for idx in list(pending):
                process_type, proc, log_file = subprocesses[idx]
                marker = (
                    PRODUCER_CONSTRUCTED_MARKER
                    if process_type == "producer"
                    else CONSUMER_CONSTRUCTED_MARKER
                )
                if not os.path.exists(log_file):
                    continue
                with open(log_file, "r", encoding="utf-8") as f:
                    for raw in f:
                        if marker in raw:
                            done_now.append(idx)
                            break
                if idx not in done_now and proc.poll() is not None:
                    env.logger.warning(
                        "Early exit without constructed marker: %s (log=%s, code=%s, pid=%s)",
                        process_type,
                        log_file,
                        proc.returncode,
                        getattr(proc, "pid", None),
                    )
                    early_fail_idx = idx
                    break
            for idx in done_now:
                pending.discard(idx)
            if pending and early_fail_idx is None:
                time.sleep(0.2)

        if early_fail_idx is not None:
            ptype, proc, logf = subprocesses[early_fail_idx]
            env.logger.warning(
                "Fail fast: process exited before constructed marker: %s (log=%s, code=%s)",
                ptype,
                logf,
                proc.returncode,
            )
            for _, proc, _ in subprocesses:
                if proc.poll() is None:
                    proc.terminate()
            deadline = time.time() + 2.0
            for _, proc, _ in subprocesses:
                if proc.poll() is None:
                    remain = deadline - time.time()
                    if remain > 0:
                        try:
                            proc.wait(timeout=remain)
                        except subprocess.TimeoutExpired:
                            env.logger.debug(
                                "Process did not exit after terminate within %.3fs; sending SIGKILL",
                                remain,
                            )
                if proc.poll() is None:
                    proc.kill()
            return False

        if pending:
            env.logger.warning(
                "Construction timeout: %s/%s ready. Returning failure to outer wrapper...",
                len(subprocesses) - len(pending),
                len(subprocesses),
            )
            for idx in sorted(pending):
                ptype, proc, logf = subprocesses[idx]
                env.logger.warning(
                    "Missing constructed marker: %s (log=%s, pid=%s)",
                    ptype,
                    logf,
                    getattr(proc, "pid", None),
                )
            for _, proc, _ in subprocesses:
                if proc.poll() is None:
                    proc.terminate()
            deadline = time.time() + 2.0
            for _, proc, _ in subprocesses:
                if proc.poll() is None:
                    remain = deadline - time.time()
                    if remain > 0:
                        proc.wait(timeout=remain)
                if proc.poll() is None:
                    proc.kill()
            return False

        # All constructed — proceed to normal wait and verification.
        # Strict bound to prevent a single stuck subprocess from hanging CI indefinitely.
        deadline = time.time() + float(TEST_TIMEOUT_SECONDS)
        pending_run = set(range(len(subprocesses)))
        while pending_run and time.time() < deadline:
            done_now: List[int] = []
            for idx in list(pending_run):
                _, proc, _ = subprocesses[idx]
                if proc.poll() is not None:
                    done_now.append(idx)
            for idx in done_now:
                pending_run.discard(idx)
            if pending_run:
                time.sleep(0.2)

        if pending_run:
            env.logger.warning(
                "Runtime timeout: %s/%s exited. Returning failure to outer wrapper...",
                len(subprocesses) - len(pending_run),
                len(subprocesses),
            )
            for idx in sorted(pending_run):
                ptype, proc, logf = subprocesses[idx]
                env.logger.warning(
                    "Timeout waiting: %s (pid=%s, log=%s)",
                    ptype,
                    getattr(proc, "pid", None),
                    logf,
                )
            for _, proc, _ in subprocesses:
                if proc.poll() is None:
                    proc.terminate()
            deadline2 = time.time() + 2.0
            for _, proc, _ in subprocesses:
                if proc.poll() is None:
                    remain = deadline2 - time.time()
                    if remain > 0:
                        try:
                            proc.wait(timeout=remain)
                        except subprocess.TimeoutExpired:
                            pass
                if proc.poll() is None:
                    proc.kill()
            return False

        for process_type, proc, log_file in subprocesses:
            if proc.returncode != 0:
                env.logger.info("%s failed with return code %s", process_type, proc.returncode)
                env.logger.info("Check log file for details: %s", log_file)
            else:
                env.logger.info("%s completed successfully", process_type)
                env.logger.info("Log file: %s", log_file)

        log_files = [log_file for _, _, log_file in subprocesses]
        verify_exit_status(env, log_files)
        env.logger.info("[test_mpsc_producer_consumer] All processes finished.")
        return True
    finally:
        pre_init_producer_1.close().unwrap()
        pre_init_producer_2.close().unwrap()
        release(env, pre_init_store_key)


def verify_exit_status(env: "ChannelState", log_files: List[str]) -> None:
    print("=== Verifying Exit Status ===")
    time.sleep(3)
    normal_exits: List[str] = []
    crashes: List[str] = []

    for log_file in log_files:
        try:
            with open(log_file, "r", encoding="utf-8") as file_obj:
                status: Optional[str] = None
                entity: Optional[str] = None
                for raw_line in file_obj:
                    line = raw_line.strip()
                    if line.startswith(PRODUCER_CRASH_MARKER):
                        producer_id = line.split(": ", 1)[1]
                        entity = f"PRODUCER_{producer_id}"
                        status = "crash"
                        break
                    if line.startswith(CONSUMER_CRASH_MARKER):
                        consumer_id = line.split(": ", 1)[1]
                        entity = f"CONSUMER_{consumer_id}"
                        status = "crash"
                        break
                    if (
                        status is None
                        and line.startswith(PRODUCER_NORMAL_EXIT_MARKER)
                    ):
                        producer_id = line.split(": ", 1)[1]
                        entity = f"PRODUCER_{producer_id}"
                        status = "normal"
                    elif (
                        status is None
                        and line.startswith(CONSUMER_NORMAL_EXIT_MARKER)
                    ):
                        consumer_id = line.split(": ", 1)[1]
                        entity = f"CONSUMER_{consumer_id}"
                        status = "normal"

                if status == "normal" and entity:
                    normal_exits.append(entity)
                    print(f"Found normal exit: {entity}")
                elif status == "crash" and entity:
                    crashes.append(entity)
                    print(f"Found crash: {entity}")
                else:
                    print(
                        f"Warning: No exit markers found in log file: {log_file}"
                    )
        except FileNotFoundError:
            print(f"Warning: Log file not found: {log_file}")
        except Exception as exc:  # noqa: BLE001
            print(f"Error reading log file {log_file}: {exc}")

    print(f"Normal exits: {normal_exits}")
    print(f"Crashes: {crashes}")

    expected_processes = len(log_files)
    actual_markers = len(normal_exits) + len(crashes)
    if actual_markers == expected_processes:
        print("✅ EXIT STATUS VERIFICATION PASSED: All processes have exit markers")
    else:
        print(
            "❌ EXIT STATUS VERIFICATION FAILED: Expected"
            f" {expected_processes} processes, found {actual_markers} markers"
        )
        print(
            f"Missing markers for {expected_processes - actual_markers} processes"
        )
        raise AssertionError("Not all processes have proper exit markers")


def map_instance_count(
    env: "ChannelState",
    etcd_client: etcd3.Etcd3Client,
    func: Callable[[int], int],
) -> None:
    with etcd_client.lock("test_api_chan_mpsc_crashdown", ttl=30):
        raw_count = etcd_client.get("/test_api_chan_mpsc_instance_count")[0]
        instance_count = 0
        if raw_count is not None:
            assert isinstance(raw_count, bytes)
            instance_count = int(raw_count.decode())
        new_count = func(instance_count)
        etcd_client.put(
            "/test_api_chan_mpsc_instance_count",
            str(new_count).encode(),
        )


def chan_type_from_string(value: str) -> ChanType:
    value_lower = value.lower()
    if value_lower == "mpsc":
        return ChanType.MPSC
    if value_lower == "mpmc":
        return ChanType.MPMC
    raise ValueError(f"Unsupported chan_type: {value}")


def _test_mpsc_channel_suite_once(prefetch: int) -> None:
    env = create_channel_env()
    args = argparse.Namespace(mode="main")
    run_main(env, args, prefetch=prefetch)
    release(env)


def test_mpsc_channel_suite() -> None:
    run_with_argmatrix(_test_mpsc_channel_suite_once)


def test_new_or_bind_unique_key_namespace_collision() -> None:
    setup_test_environment(logging)
    env = create_channel_env()
    store_key = "unique_key_namespace_collision_store"
    key_a = "namespace_collision_case"
    key_b = f"{key_a}_lock"

    with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
        etcd_client.delete(_new_unique_mapping_key(key_a))
        etcd_client.delete(_new_unique_lock_key(key_a))
        etcd_client.delete(_new_unique_mapping_key(key_b))
        etcd_client.delete(_new_unique_lock_key(key_b))

    try:
        store = require_store(env, store_key)
        producer_a = new_test_producer(
            "new_or_bind",
            store,
            None,
            CHAN_CONFIG_MPSC_TEST,
            key_a,
            ChanType.MPSC,
        )
        producer_b = new_test_producer(
            "new_or_bind",
            store,
            None,
            CHAN_CONFIG_MPSC_TEST,
            key_b,
            ChanType.MPSC,
        )

        chan_id_a = producer_a.get_chan_id()
        chan_id_b = producer_b.get_chan_id()
        assert chan_id_a != chan_id_b, (
            "unique keys must not collide through raw lock-key naming: "
            f"key_a={key_a!r} key_b={key_b!r} chan_id_a={chan_id_a!r} chan_id_b={chan_id_b!r}"
        )

        with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
            value_a, _ = etcd_client.get(_new_unique_mapping_key(key_a))
            value_b, _ = etcd_client.get(_new_unique_mapping_key(key_b))
            assert value_a is not None, f"missing mapping for key_a={key_a!r}"
            assert value_b is not None, f"missing mapping for key_b={key_b!r}"
            assert value_a.decode("utf-8") == chan_id_a
            assert value_b.decode("utf-8") == chan_id_b
    finally:
        release(env)

def _wait_fluxon_member_absent(instance_key: str, *, timeout_s: int = TEST_TIMEOUT_SECONDS) -> None:
    """Wait until a fluxon cluster member key disappears from etcd.

    Purpose: avoid init failures like "Member already exists" when the previous test run
    exited abnormally and the member lease has not expired yet. Do not delete keys here;
    only wait for expiry to minimize test intrusion.
    """
    # Lazy import to avoid expanding top-level dependencies.
    from fluxon_py.tests.test_lib import load_test_fluxon_cluster_name  # type: ignore

    cluster = load_test_fluxon_cluster_name()
    key = f"/fluxon_kv_member_base/{cluster}/members/{instance_key}"
    deadline = time.time() + float(timeout_s)
    with etcd3.client(ETCD_HOST, ETCD_PORT) as etcd_client:
        while True:
            val = etcd_client.get(key)[0]
            if val is None:
                return
            if time.time() >= deadline:
                raise RuntimeError(
                    f"member key still exists after wait: {key}. Previous lease not expired"
                )
            # Progress logging is handled by the caller; keep quiet here.
            time.sleep(1.0)


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
