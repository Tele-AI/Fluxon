#!/usr/bin/env python3
"""Contract tests for MPMC ready claims and lock-scoped sub-channel creation."""

from __future__ import annotations

import importlib
import json
import sys
import threading
import types
import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# This contract exercises only Python coordination logic. Keep importing that
# logic independent from the native extension and replace all native entrypoints
# used by the module before loading it.
_pyo3_loader = importlib.import_module("fluxon_py.tool.pyo3")
_pyo3_loader._FLUXON_PYO3_MODULE_LAZY = types.SimpleNamespace(
    MpscContext=object,
    LeaseManagerHandle=object,
    EtcdLock=object,
)

from fluxon_py._api_ext_chan import mpmc  # noqa: E402


@dataclass(frozen=True)
class _Compare:
    kind: str
    key: str
    expected: object


@dataclass(frozen=True)
class _Operation:
    kind: str
    key: str
    value: Optional[bytes] = None


class _CompareOperand:
    def __init__(self, kind: str, key: str) -> None:
        self.kind = kind
        self.key = key

    def __eq__(self, expected: object) -> _Compare:  # type: ignore[override]
        return _Compare(self.kind, self.key, expected)


class _FakeTransactions:
    def create(self, key: str) -> _CompareOperand:
        return _CompareOperand("create", key)

    def value(self, key: str) -> _CompareOperand:
        return _CompareOperand("value", key)

    def put(self, key: str, value: bytes, _lease: object) -> _Operation:
        return _Operation("put", key, value)

    def delete(self, key: str) -> _Operation:
        return _Operation("delete", key)


class _FakeMetadata:
    def __init__(self, key: str) -> None:
        self.key = key.encode()


class _FakeLeaseInfo:
    TTL = 60


class _FakeEtcd:
    def __init__(self) -> None:
        self.values: Dict[str, bytes] = {}
        self.transactions = _FakeTransactions()
        self.fail_publish = False
        self.raise_after_publish_commit = False
        self.defer_publish_until_cleanup = False
        self.pending_publish_ops: Optional[List[_Operation]] = None
        self.defer_reservation_create_until_revoke = False
        self.pending_reservation_ops: Optional[List[_Operation]] = None
        self.raise_after_ready_claim_commit = False
        self.publish_attempts = 0
        self.revoked_lease_ids: List[int] = []

    def get(self, key: str):
        value = self.values.get(key)
        return value, _FakeMetadata(key) if value is not None else None

    def get_prefix(self, prefix: str):
        return [
            (value, _FakeMetadata(key))
            for key, value in sorted(self.values.items())
            if key.startswith(prefix)
        ]

    def get_lease_info(self, _lease_id: int) -> _FakeLeaseInfo:
        return _FakeLeaseInfo()

    def delete(self, key: str) -> bool:
        return self.values.pop(key, None) is not None

    def put(self, key: str, value: bytes) -> None:
        self.values[key] = value

    def delete_prefix(self, prefix: str) -> int:
        keys = [key for key in self.values if key.startswith(prefix)]
        for key in keys:
            del self.values[key]
        return len(keys)

    def revoke_lease(self, lease_id: int) -> None:
        self.revoked_lease_ids.append(lease_id)
        self.pending_reservation_ops = None

    def transaction(self, *, compare, success, failure):
        del failure
        is_publish = any(
            item.kind == "put" and item.key.endswith("/mpsc_channels")
            for item in success
        )
        if is_publish:
            self.publish_attempts += 1
            if self.fail_publish:
                return False, []
            if self.defer_publish_until_cleanup:
                self.pending_publish_ops = list(success)
                raise RuntimeError("injected delayed publish response loss")

        is_reservation_create = any(
            item.kind == "put" and "/create_reservations/" in item.key
            for item in success
        )
        if is_reservation_create and self.defer_reservation_create_until_revoke:
            self.pending_reservation_ops = list(success)
            raise RuntimeError("injected delayed reservation create response loss")

        is_reservation_cleanup = any(
            item.kind == "delete" and "/create_reservations/" in item.key
            for item in success
        )
        if is_reservation_cleanup and self.pending_publish_ops is not None:
            self._apply_operations(self.pending_publish_ops)
            self.pending_publish_ops = None

        matches = all(self._compare_matches(item) for item in compare)
        if not matches:
            return False, []
        self._apply_operations(success)
        if is_publish and self.raise_after_publish_commit:
            raise RuntimeError("injected response loss after publish commit")
        is_ready_claim = (
            not is_publish
            and any(
                item.kind == "put" and "/mpmc_channels/ready/" in item.key
                for item in success
            )
        )
        if is_ready_claim and self.raise_after_ready_claim_commit:
            raise RuntimeError("injected response loss after ready claim commit")
        return True, []

    def _apply_operations(self, operations: List[_Operation]) -> None:
        for operation in operations:
            if operation.kind == "put":
                assert operation.value is not None
                self.values[operation.key] = operation.value
            elif operation.kind == "delete":
                self.values.pop(operation.key, None)
            else:  # pragma: no cover - catches incomplete fake coverage
                raise AssertionError(f"unsupported operation: {operation!r}")

    def _compare_matches(self, item: _Compare) -> bool:
        if item.kind == "create":
            return (0 if item.key not in self.values else 1) == item.expected
        if item.kind == "value":
            return self.values.get(item.key) == item.expected
        raise AssertionError(f"unsupported comparison: {item!r}")


@dataclass(frozen=True)
class _LockCall:
    endpoints: List[str]
    name: str
    ttl_seconds: int
    timeout_seconds: float


class _FakeLock:
    def __init__(self, tracker: "_LockTracker") -> None:
        self.tracker = tracker

    def __enter__(self) -> "_FakeLock":
        self.tracker.depth += 1
        self.tracker.max_depth = max(self.tracker.max_depth, self.tracker.depth)
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        del exc_type, exc_value, traceback
        self.tracker.depth -= 1
        if self.tracker.depth < 0:  # pragma: no cover - fake invariant
            raise AssertionError("negative fake lock depth")


class _LockTracker:
    def __init__(self) -> None:
        self.calls: List[_LockCall] = []
        self.depth = 0
        self.max_depth = 0

    def new_lock(
        self,
        endpoints: List[str],
        name: str,
        ttl_seconds: int,
        timeout_seconds: float,
    ) -> _FakeLock:
        self.calls.append(
            _LockCall(
                list(endpoints),
                name,
                ttl_seconds,
                timeout_seconds,
            )
        )
        return _FakeLock(self)


class _ReleaseResult:
    def unwrap(self) -> None:
        return None


class _FakeMpscState:
    def __init__(self, lock_tracker: _LockTracker) -> None:
        self.lock_tracker = lock_tracker
        self.constructor_lock_depths: List[int] = []
        self.instances: List[object] = []
        self.next_channel_id = 1000
        self.fail_constructor = False
        self.rollback_count = 0
        self.on_construct: Optional[Callable[[], None]] = None


def _new_fake_mpsc_consumer_type(state: _FakeMpscState):
    class _FakeMpscConsumer:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del kwargs
            state.constructor_lock_depths.append(state.lock_tracker.depth)
            if state.fail_constructor:
                raise RuntimeError("injected MPSC constructor failure")

            requested_id = args[1]
            if requested_id is None:
                state.next_channel_id += 1
                requested_id = str(state.next_channel_id)
            self.chan_id = str(requested_id)
            self._mpmc_ready_claimed = False
            self.release_count = 0
            state.instances.append(self)

            callback = state.on_construct
            state.on_construct = None
            if callback is not None:
                callback()

        def get_chan_id(self) -> str:
            return self.chan_id

        def release_local_handle(self) -> _ReleaseResult:
            self.release_count += 1
            return _ReleaseResult()

        def _rollback_unpublished_channel(self):
            state.rollback_count += 1
            return mpmc.Result.new_ok(mpmc.OK_NONE)

    return _FakeMpscConsumer


class _FakeLease:
    def __init__(self, lease_id: int) -> None:
        self.id = lease_id


def _new_channel(etcd: _FakeEtcd, member_id: int) -> mpmc.MPMCChannel:
    channel = object.__new__(mpmc.MPMCChannel)
    channel.mpmc_id = "7"
    channel.etcd_client = etcd
    channel._etcd_endpoints = ["127.0.0.1:2379"]
    channel.mpmc_member_id = member_id
    channel.mpmc_member_lease = _FakeLease(100 + member_id)
    channel._lm_mpmc_member = None
    channel.mpmc_global_lease = _FakeLease(1)
    channel.payload_lease_id = 10
    channel.shutdown_ctl = mpmc.MqShutdownCtl()
    channel.ready_channels = []
    channel.unready_channels = []
    channel._ready_channels_lock = threading.Lock()
    channel.new_ready_channels_callback = None
    channel.remove_ready_channels_callback = None
    return channel


def _add_active_consumers(etcd: _FakeEtcd, count: int) -> None:
    prefix = mpmc._new_mpmc_role_key_prefix("7", mpmc.ChanRole.CONSUMER)
    for member_id in range(1, count + 1):
        etcd.values[f"{prefix}{member_id}"] = b"active"




class TestCreateLockContract(unittest.TestCase):
    def test_ready_claim_response_loss_reconciles_owned_key(self) -> None:
        etcd = _FakeEtcd()
        etcd.raise_after_ready_claim_commit = True
        channel = _new_channel(etcd, member_id=11)

        result = channel.try_claim_ready_channel("91")

        self.assertTrue(result.is_ok())
        self.assertTrue(result.unwrap())
        ready_key = mpmc._new_mpmc_ready_channel_key("7", "91")
        self.assertEqual(etcd.values[ready_key], b"11")
        self.assertFalse(channel.shutdown_ctl.closed)

    def test_ambiguous_ready_claim_with_other_owner_revokes_member(self) -> None:
        etcd = _FakeEtcd()
        channel = _new_channel(etcd, member_id=11)
        ready_key = mpmc._new_mpmc_ready_channel_key("7", "91")
        etcd.values[ready_key] = b"22"

        with mock.patch.object(
            etcd,
            "transaction",
            side_effect=RuntimeError("injected ambiguous ready claim"),
        ):
            result = channel.try_claim_ready_channel("91")

        self.assertFalse(result.is_ok())
        _ = result.unwrap_error()
        self.assertTrue(channel.shutdown_ctl.closed)
        self.assertEqual(etcd.revoked_lease_ids, [111])
        self.assertEqual(etcd.values[ready_key], b"22")

    def test_existing_unready_consumer_does_not_construct_create_lock(self) -> None:
        etcd = _FakeEtcd()
        channels_key = mpmc._new_mpmc_mpsc_channels_key("7")
        etcd.values[channels_key] = json.dumps(["91"]).encode()
        channel = _new_channel(etcd, member_id=11)
        locks = _LockTracker()
        mpsc_state = _FakeMpscState(locks)
        fake_consumer = _new_fake_mpsc_consumer_type(mpsc_state)

        with mock.patch.object(mpmc, "EtcdLock", locks.new_lock), mock.patch.object(
            mpmc, "MPSCChanConsumer", fake_consumer
        ):
            result = channel.get_next_available_channel(object(), {})

        self.assertTrue(result.is_ok())
        bound = result.unwrap()
        self.assertEqual(bound.chan_id, "91")
        self.assertTrue(bound._mpmc_ready_claimed)
        self.assertEqual(locks.calls, [])
        self.assertEqual(mpsc_state.constructor_lock_depths, [0])

    def test_new_consumer_is_constructed_and_published_under_one_lock(self) -> None:
        etcd = _FakeEtcd()
        _add_active_consumers(etcd, 1)
        channel = _new_channel(etcd, member_id=11)
        locks = _LockTracker()
        mpsc_state = _FakeMpscState(locks)
        fake_consumer = _new_fake_mpsc_consumer_type(mpsc_state)

        with mock.patch.object(mpmc, "EtcdLock", locks.new_lock), mock.patch.object(
            mpmc, "MPSCChanConsumer", fake_consumer
        ):
            result = channel.try_create_mpsc_channel(
                object(), {}, mpmc.ChanRole.CONSUMER
            )

        self.assertTrue(result.is_ok())
        consumer = result.unwrap()
        self.assertTrue(consumer._mpmc_ready_claimed)
        self.assertEqual(mpsc_state.constructor_lock_depths, [1])
        self.assertEqual(len(locks.calls), 1)
        self.assertEqual(
            locks.calls[0].timeout_seconds,
            mpmc.MPMC_CREATE_LOCK_TIMEOUT_SECONDS,
        )
        channels_value = etcd.values[mpmc._new_mpmc_mpsc_channels_key("7")]
        self.assertEqual(json.loads(channels_value.decode()), ["1001"])
        self.assertEqual(
            etcd.values[mpmc._new_mpmc_ready_channel_key("7", "1001")],
            b"11",
        )

    def test_constructor_failure_leaves_no_published_channel(self) -> None:
        etcd = _FakeEtcd()
        _add_active_consumers(etcd, 1)
        channel = _new_channel(etcd, member_id=11)
        locks = _LockTracker()
        mpsc_state = _FakeMpscState(locks)
        mpsc_state.fail_constructor = True
        fake_consumer = _new_fake_mpsc_consumer_type(mpsc_state)

        with mock.patch.object(mpmc, "EtcdLock", locks.new_lock), mock.patch.object(
            mpmc, "MPSCChanConsumer", fake_consumer
        ):
            result = channel.try_create_mpsc_channel(
                object(), {}, mpmc.ChanRole.CONSUMER
            )

        self.assertFalse(result.is_ok())
        self.assertIn("injected MPSC constructor failure", result.unwrap_error().message)
        self.assertEqual(mpsc_state.constructor_lock_depths, [1])
        self.assertNotIn(mpmc._new_mpmc_mpsc_channels_key("7"), etcd.values)

    def test_channel_list_cas_failure_rolls_back_unpublished_consumer(self) -> None:
        etcd = _FakeEtcd()
        etcd.fail_publish = True
        _add_active_consumers(etcd, 1)
        channel = _new_channel(etcd, member_id=11)
        locks = _LockTracker()
        mpsc_state = _FakeMpscState(locks)
        fake_consumer = _new_fake_mpsc_consumer_type(mpsc_state)

        with mock.patch.object(mpmc, "EtcdLock", locks.new_lock), mock.patch.object(
            mpmc, "MPSCChanConsumer", fake_consumer
        ):
            result = channel.try_create_mpsc_channel(
                object(), {}, mpmc.ChanRole.CONSUMER
            )

        self.assertFalse(result.is_ok())
        _ = result.unwrap_error()
        self.assertEqual(mpsc_state.rollback_count, 1)
        self.assertNotIn(mpmc._new_mpmc_mpsc_channels_key("7"), etcd.values)


if __name__ == "__main__":
    unittest.main()
