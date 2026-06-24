#!/usr/bin/env python3

import argparse
import threading
from pathlib import Path

from fluxon_py.api_ext_chan import (  # type: ignore
    ChanRole,
    ChanType,
    new_or_bind_with_unique_key,
)
from fluxon_py.api_error import ChannelClosedError, ProducerClosedError  # type: ignore
from fluxon_py.config import FluxonKvClientConfig  # type: ignore
from fluxon_py.kvclient import new_store  # type: ignore
from fluxon_py.logging import init_logger  # type: ignore
from fluxon_py.runtime import register_ctrlc_callback

# These constants are the only user-facing knobs in the minimal example.
CLUSTER_NAME = "demo-kv-cluster"
SHARE_MEM_PATH = "/dev/shm/fluxon_kv_demo"
CHANNEL_KEY = "demo_mq_channel_doc"
CHANNEL_CAPACITY = 128
CHANNEL_TTL_SECONDS = 300
PRODUCER_INTERVAL_SECONDS = 1.0
CONSUMER_BATCH_SIZE = 1


def _must_ok(res, msg: str):
    if not res.is_ok():
        raise SystemExit(f"{msg}: {res.unwrap_error()}")
    return res.unwrap()


def _best_effort_close_result(obj, logger, role: str) -> None:
    try:
        close_res = obj.close()
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[{role}] close raised (ignored): {e}")
        return

    if close_res.is_ok():
        _ = close_res.unwrap()
    else:
        logger.warning(f"[{role}] close error (ignored): {close_res.unwrap_error()}")


def _build_store_config(*, role: str) -> FluxonKvClientConfig:
    # MQ first attaches to the local owner via one external KvClient,
    # then binds a producer or consumer handle on top of that store.
    return FluxonKvClientConfig(
        {
            "instance_key": f"demo_mq_{role}",
            "fluxonkv_spec": {
                "cluster_name": CLUSTER_NAME,
                "share_mem_path": SHARE_MEM_PATH,
            },
        }
    )


def _run_producer(store, logger, shutdown_requested: threading.Event) -> None:
    interrupted = False
    closed = False
    producer = None
    restore_signal_listener = lambda: None
    seq = 1
    try:
        # Producer and consumer must bind the same channel key so they land on
        # the same channel id.
        producer = _must_ok(
            new_or_bind_with_unique_key(
                store,
                {"capacity": CHANNEL_CAPACITY, "ttl_seconds": CHANNEL_TTL_SECONDS},
                unique_id=CHANNEL_KEY,
                chan_type=ChanType.MPMC,
                chan_role=ChanRole.PRODUCER,
            ),
            "bind producer failed",
        )

        def _on_ctrlc(reason: str) -> None:
            nonlocal interrupted, closed
            # The signal callback only requests shutdown and closes the handle once.
            # The main loop still exits through its normal close-observation path.
            interrupted = True
            shutdown_requested.set()
            if closed:
                return
            closed = True
            logger.info(f"[producer] caught {reason}, calling close...")
            _best_effort_close_result(producer, logger, "producer")

        restore_signal_listener = register_ctrlc_callback(
            _on_ctrlc,
            thread_name="mpmc-demo-producer-signal",
        )
        logger.info(f"[producer] ready: channel_key={CHANNEL_KEY}")
        while not shutdown_requested.is_set():
            payload_text = f"hello mq #{seq}"
            payload = payload_text.encode("utf-8")
            put_res = producer.put_data(
                {
                    "seq": seq,
                    "payload": payload,
                }
            )
            if put_res.is_ok():
                _ = put_res.unwrap()
                logger.info(f"[producer] sent: seq={seq} payload={payload_text}")
                seq += 1
            else:
                err = put_res.unwrap_error()
                # ProducerClosedError is the expected signal that close() already
                # propagated into the handle, not an unexpected data-path failure.
                if isinstance(err, ProducerClosedError):
                    logger.info("[producer] close observed, exit loop")
                    break
                raise SystemExit(f"put_data failed: {err}")
            if shutdown_requested.wait(PRODUCER_INTERVAL_SECONDS):
                break
    finally:
        restore_signal_listener()
        # Handle lifetime must end before store lifetime.
        if producer is not None and not closed:
            _best_effort_close_result(producer, logger, "producer")
    if interrupted:
        raise SystemExit(130)


def _run_consumer(store, logger, shutdown_requested: threading.Event) -> None:
    interrupted = False
    closed = False
    consumer = None
    restore_signal_listener = lambda: None
    try:
        # Consumer binds the same channel key as producer and only changes role.
        consumer = _must_ok(
            new_or_bind_with_unique_key(
                store,
                {"capacity": CHANNEL_CAPACITY, "ttl_seconds": CHANNEL_TTL_SECONDS},
                unique_id=CHANNEL_KEY,
                chan_type=ChanType.MPMC,
                chan_role=ChanRole.CONSUMER,
            ),
            "bind consumer failed",
        )

        def _on_ctrlc(reason: str) -> None:
            nonlocal interrupted, closed
            # Keep the callback minimal: request shutdown, close the MQ handle once,
            # and let the main loop observe ChannelClosedError.
            interrupted = True
            shutdown_requested.set()
            if closed:
                return
            closed = True
            logger.info(f"[consumer] caught {reason}, calling close...")
            _best_effort_close_result(consumer, logger, "consumer")

        restore_signal_listener = register_ctrlc_callback(
            _on_ctrlc,
            thread_name="mpmc-demo-consumer-signal",
        )
        logger.info(f"[consumer] ready: channel_key={CHANNEL_KEY}")
        while not shutdown_requested.is_set():
            get_res = consumer.get_data(batch_size=CONSUMER_BATCH_SIZE)
            if not get_res.is_ok():
                err = get_res.unwrap_error()
                # ChannelClosedError is the normal close path after Ctrl-C/SIGTERM.
                if isinstance(err, ChannelClosedError):
                    logger.info("[consumer] close observed, exit loop")
                    break
                raise SystemExit(f"get_data failed: {err}")
            for item in get_res.unwrap() or []:
                payload = item.get("payload", b"") if isinstance(item, dict) else item
                seq = item.get("seq") if isinstance(item, dict) else None
                if isinstance(payload, (bytes, bytearray, memoryview)):
                    logger.info(
                        f"[consumer] got: seq={seq} payload={bytes(payload).decode('utf-8', 'ignore')}"
                    )
                else:
                    logger.info(f"[consumer] got: seq={seq} payload={payload}")
            if shutdown_requested.wait(0.2):
                break
    finally:
        restore_signal_listener()
        # Always close the consumer before main() closes the backing store.
        if consumer is not None and not closed:
            _best_effort_close_result(consumer, logger, "consumer")
    if interrupted:
        raise SystemExit(130)


def main() -> None:
    parser = argparse.ArgumentParser(description="Start MQ minimal demo")
    parser.add_argument("--role", choices=["producer", "consumer"], required=True)
    args = parser.parse_args()

    # init_logger() reads FLUXON_LOG and sets the user-process console log level.
    logger = init_logger(f"mpmc_demo_{args.role}")
    shutdown_requested = threading.Event()
    store = None
    try:
        store = _must_ok(new_store(_build_store_config(role=args.role)), "new_store failed")
        if args.role == "producer":
            _run_producer(store, logger, shutdown_requested)
        else:
            _run_consumer(store, logger, shutdown_requested)
    finally:
        store_to_close = store
        store = None
        # Store is closed last because MQ handles are already closed inside _run_*.
        if store_to_close is not None:
            _best_effort_close_result(store_to_close, logger, "store")
        logger.info(f"[{args.role}] exit")


if __name__ == "__main__":
    main()
