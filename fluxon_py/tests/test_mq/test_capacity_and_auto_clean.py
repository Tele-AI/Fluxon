"""
MQ capacity + auto-clean check (standalone, fluxon only).

- Use a tiny per-(mpsc_id) capacity with no consumer; verify the next put blocks.
- Close producer and wait TTL; verify metrics available bytes recover to at least
  the baseline for every visible segment.

Backend: fluxon only (print-and-exit when not fluxon; no fallback backend).
Entry: python3 fluxon_py/tests/test_mq/test_capacity_and_auto_clean.py
"""

from __future__ import annotations

import os
import sys
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Tuple

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from fluxon_py import ChanType
from fluxon_py.api_error import ProducerClosedError
from fluxon_py._api_ext_chan.mq_config_check import MIN_TTL as CHAN_MIN_TTL_SECONDS
from fluxon_py.kvclient.kvclient_interface import KvClient
from fluxon_py._api_ext_chan.mpmc import MPMCChanProducer
from fluxon_py.logging import init_logger
from fluxon_py.metrics import get_metric_snapshot
from fluxon_py.tests.test_lib import (
    KV_SVC_IP,
    KV_SVC_TYPE,
    new_shared_stores,
    new_test_producer,
    setup_test_environment,
)


logging = init_logger()

TEST_TAG = "TEST-MQCAPAUTOCLEAN"
CLUSTER_JOIN_SECONDS = 30
CAPACITY = 2
TTL_SECONDS = CHAN_MIN_TTL_SECONDS
PAYLOAD_SIZE_BYTES = 100 * 1024 * 1024
BLOCK_TIMEOUT_SECONDS = 10.0
CLOSE_JOIN_TIMEOUT_SECONDS = 10.0
PREFIX_INDEX_TIMEOUT_SECONDS = 30.0
NON_CAPACITY_EXTRA_SUCCESS_PUTS = 8
MPSC_PHYSICAL_FILL_HEADROOM_DENOMINATOR = 2


def main() -> None:
    if KV_SVC_TYPE != "fluxon":
        print(f"[{TEST_TAG}-skip] KV_SVC_TYPE is not 'fluxon'; skip")
        return

    setup_test_environment(logging)

    for chan_type, mode in CHAN_TYPES_UNDER_TEST:
        print(f"[{TEST_TAG}-run] chan={chan_type.name.lower()} mode={mode.name.lower()}")
        run_for_chan_type(chan_type, mode)
        time.sleep(15)

    print(f"[{TEST_TAG}-ok] capacity-and-auto-clean passed")


class CapCheck(Enum):
    """Enum to indicate whether this chan type should check capacity gating."""

    REQUIRED = 1
    NON_CAPACITY = 2


@dataclass
class BlockingProbeState:
    done: bool = False
    err: Exception | None = None


CHAN_TYPES_UNDER_TEST = [
    (ChanType.MPMC, CapCheck.REQUIRED),
    (ChanType.MPSC, CapCheck.NON_CAPACITY),
]


def _put_or_raise(target: object, payload: dict[str, bytes], *, context: str) -> None:
    res = target.put_data(payload)
    if res.is_ok():
        _ = res.unwrap(context)
        return

    err = res.unwrap_error()
    if isinstance(err, Exception):
        raise err
    raise RuntimeError(f"{context}: {err}")


def _run_blocking_probe(
    producer: MPMCChanProducer,
    payload: dict[str, bytes],
    state: BlockingProbeState,
    chan_type: ChanType,
) -> None:
    try:
        _put_or_raise(
            producer,
            payload,
            context=f"expected blocking put for {chan_type.name.lower()} after capacity={CAPACITY}",
        )
        state.err = None
    except ProducerClosedError as err:
        state.err = err
    except Exception as err:  # noqa: BLE001
        state.err = err
    finally:
        state.done = True


def _assert_probe_released_by_close(
    chan_type: ChanType,
    probe_thread: threading.Thread,
    probe_state: BlockingProbeState,
) -> None:
    probe_thread.join(timeout=CLOSE_JOIN_TIMEOUT_SECONDS)
    assert not probe_thread.is_alive(), (
        f"blocking probe thread did not exit within {CLOSE_JOIN_TIMEOUT_SECONDS}s after close for {chan_type.name.lower()}"
    )
    assert probe_state.done, f"blocking probe thread exited without recording state for {chan_type.name.lower()}"
    assert isinstance(probe_state.err, ProducerClosedError), (
        f"blocking probe after close must end as ProducerClosedError for {chan_type.name.lower()}, got {probe_state.err!r}"
    )


def _select_payload_size_bytes(
    *,
    chan_type: ChanType,
    cap_check: CapCheck,
    baseline: dict[str, Tuple[int, int]],
) -> int:
    if chan_type != ChanType.MPSC:
        return PAYLOAD_SIZE_BYTES

    # MPSC here only needs to verify "no logical per-channel capacity gate before close".
    # Keep aggregate payload well below the physical segment limit so the test does not
    # block on shared-memory exhaustion, which is a different behavior than the one under test.
    assert baseline, f"{chan_type.name.lower()} baseline metrics must not be empty"
    smallest_available = min(avail for avail, _total in baseline.values())
    planned_puts = CAPACITY
    if cap_check == CapCheck.NON_CAPACITY:
        planned_puts += NON_CAPACITY_EXTRA_SUCCESS_PUTS
    payload_size_bytes = min(
        PAYLOAD_SIZE_BYTES,
        smallest_available // (planned_puts * MPSC_PHYSICAL_FILL_HEADROOM_DENOMINATOR),
    )
    assert payload_size_bytes > 0, (
        f"{chan_type.name.lower()} computed non-positive payload size: "
        f"smallest_available={smallest_available}, planned_puts={planned_puts}"
    )
    return payload_size_bytes


def run_for_chan_type(chan_type: ChanType, cap_check: CapCheck) -> None:
    name_prefix = f"{chan_type.name.lower()}_capacity_test"
    stores = new_shared_stores(name_prefix, 1, backend_type=KV_SVC_TYPE, ip=KV_SVC_IP)
    assert len(stores) == 1
    store = stores[0]

    time.sleep(CLUSTER_JOIN_SECONDS)

    chan_config = {"capacity": CAPACITY, "ttl_seconds": TTL_SECONDS}
    producer = new_test_producer(
        "cstyle",
        store,
        None,
        chan_config,
        None,
        chan_type,
    )

    extra_producer: MPMCChanProducer | None = None
    if chan_type == ChanType.MPMC:
        assert isinstance(producer, MPMCChanProducer), f"expected MPMCChanProducer, got {type(producer)}"
        extra_producer = MPMCChanProducer(store, producer.get_chan_id(), chan_config)

    baseline = _extract_cluster_segment_capacity(store)
    _print_segment_capacity(f"{chan_type.name.lower()} baseline", baseline)
    payload_size_bytes = _select_payload_size_bytes(
        chan_type=chan_type,
        cap_check=cap_check,
        baseline=baseline,
    )
    print(f"[{TEST_TAG}-payload] chan={chan_type.name.lower()} bytes={payload_size_bytes}")
    payload = {"payload": (b"\x00" * payload_size_bytes)}

    for i in range(CAPACITY):
        _put_or_raise(producer, payload, context=f"initial put failed unexpectedly at i={i}")
        print(f"[{TEST_TAG}-put-success] chan={chan_type.name.lower()} i={i}")

    block_probe_thread: threading.Thread | None = None
    block_probe_state: BlockingProbeState | None = None
    if cap_check == CapCheck.REQUIRED:
        assert extra_producer is not None
        # Capacity uses an asynchronous prefix index; wait for the initial puts.
        prefix = f"/mpmc/{producer.get_chan_id()}/"
        deadline = time.monotonic() + PREFIX_INDEX_TIMEOUT_SECONDS
        while True:
            count = store.count_prefix(prefix).unwrap("observe initial MQ payloads")
            if count == CAPACITY:
                break
            assert count < CAPACITY, f"unexpected MQ payload count: {count} > {CAPACITY}"
            assert time.monotonic() < deadline, (
                f"prefix index did not observe {CAPACITY} initial MQ payloads: count={count}"
            )
            time.sleep(0.05)
        block_probe_state = BlockingProbeState()
        block_probe_thread = threading.Thread(
            target=_run_blocking_probe,
            args=(extra_producer, payload, block_probe_state, chan_type),
            daemon=True,
        )
        block_probe_thread.start()
        block_probe_thread.join(timeout=BLOCK_TIMEOUT_SECONDS)
        if not block_probe_thread.is_alive():
            err = block_probe_state.err
            if err is not None:
                raise AssertionError(str(err))
            raise AssertionError(
                f"expected blocking put for {chan_type.name.lower()} after capacity={CAPACITY}, but probe returned"
            )
        print(f"[{TEST_TAG}-block] chan={chan_type.name.lower()} at=attempt#1")
    else:
        for attempt in range(1, NON_CAPACITY_EXTRA_SUCCESS_PUTS + 1):
            _put_or_raise(
                producer,
                payload,
                context=(
                    f"non-capacity put failed unexpectedly for {chan_type.name.lower()} at i={attempt}"
                ),
            )
            print(f"[{TEST_TAG}-put-success] chan={chan_type.name.lower()} i={attempt}")

    pre_close = _extract_cluster_segment_capacity(store)

    print(f"[{TEST_TAG}-close] chan={chan_type.name.lower()} action=producer.close t_epoch={int(time.time())} ttl={TTL_SECONDS}")
    if extra_producer is not None:
        extra_producer.close().unwrap()
    producer.close().unwrap()
    if block_probe_thread is not None and block_probe_state is not None:
        _assert_probe_released_by_close(chan_type, block_probe_thread, block_probe_state)
    time.sleep(TTL_SECONDS + 10)

    after = _extract_cluster_segment_capacity(store)
    _print_segment_capacity(f"{chan_type.name.lower()} pre_close", pre_close)
    _print_segment_capacity(f"{chan_type.name.lower()} after", after)

    for seg, (avail_after, total_after) in after.items():
        assert total_after >= 0 and avail_after >= 0, f"negative bytes for segment {seg}"

    for seg, (avail_base, total_base) in baseline.items():
        pair = after.get(seg)
        if pair is None:
            print(f"[{TEST_TAG}-warn] {chan_type.name.lower()} baseline segment disappeared after test: {seg}")
            continue
        avail_after, total_after = pair
        assert total_after >= total_base, (
            f"{chan_type.name.lower()} segment total shrank for {seg}: baseline_total={total_base}, after_total={total_after}"
        )
        assert avail_after >= avail_base, (
            f"{chan_type.name.lower()} capacity should recover to baseline after close+TTL for segment {seg}: "
            f"baseline_available={avail_base}, after_available={avail_after}, total={total_after}"
        )

    res = store.close()
    if res.is_ok():
        _ = res.unwrap()
    else:
        _ = res.unwrap_error()


def _extract_cluster_segment_capacity(store: KvClient) -> dict[str, Tuple[int, int]]:
    snap = get_metric_snapshot(store)
    return snap.per_segment_size()


def _print_segment_capacity(tag: str, mapping: dict[str, Tuple[int, int]]) -> None:
    items = ", ".join(
        f"{k}=(avail={v[0]}, total={v[1]})" for k, v in sorted(mapping.items(), key=lambda x: x[0])
    )
    print(f"[{TEST_TAG}-metrics] {tag}: {items}")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"[{TEST_TAG}-fail] {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[{TEST_TAG}-error] unexpected exception: {e}")
        sys.exit(2)
    else:
        print(f"[{TEST_TAG}-ok] test_capacity_and_auto_clean passed")
