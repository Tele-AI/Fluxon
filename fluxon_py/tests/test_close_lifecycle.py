#!/usr/bin/env python3

from __future__ import annotations

import threading
import time
import unittest
import sys
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from fluxon_py.api_error import GeneralError, OkNone, Result
from fluxon_py.kvclient.fluxon import FluxonKVCacheStore
from fluxon_py.kvclient.kvclient_interface import KvClient, KvCloseRegistration
from fluxon_py.runtime import start_owner_kvclient
from fluxon_py._api_ext_chan.mq_lifecycle import publish_mq_construction


class _FakeBackendClient:
    def __init__(self, events=None) -> None:
        self.close_calls = 0
        self.close_started = threading.Event()
        self.events = events

    def close(self):
        self.close_calls += 1
        if self.events is not None:
            self.events.append("backend")
        self.close_started.set()
        time.sleep(0.05)
        return Result.new_ok(OkNone())


class _FakeChild:
    def __init__(self, events=None, *, fail=False) -> None:
        self.events = events
        self.fail = fail
        self.close_calls = 0
        self.registration = KvCloseRegistration.noop()

    def close(self):
        self.close_calls += 1
        if self.events is not None:
            self.events.append("child")
        if self.fail:
            return Result.new_error(GeneralError(message="child close failed"))
        self.registration.unregister()
        return Result.new_ok(OkNone())


class _FakeStore:
    def __init__(self) -> None:
        self.close_calls = 0

    def close(self):
        self.close_calls += 1
        return Result.new_ok(OkNone())


class _ConstructingChild:
    @publish_mq_construction
    def __init__(self, store, entered, release, events) -> None:
        self._close_lock = threading.Lock()
        self._close_done = False
        self.events = events
        self.registration = KvCloseRegistration.noop()
        self.registration = store.register_child_close(self._close_from_kv)
        entered.set()
        release.wait(timeout=2.0)

    def _close_from_kv(self):
        self._kv_child_construction_done.wait()
        return self.close()

    def close(self):
        with self._close_lock:
            if self._close_done:
                return Result.new_ok(OkNone())
            self.events.append("child")
            self._close_done = True
            self.registration.unregister()
            return Result.new_ok(OkNone())


class _BlockingChild:
    def __init__(self) -> None:
        self._close_lock = threading.Lock()
        self._close_done = False
        self.close_started = threading.Event()
        self.release_close = threading.Event()
        self.close_calls = 0
        self.registration = KvCloseRegistration.noop()

    def close(self):
        with self._close_lock:
            if self._close_done:
                return Result.new_ok(OkNone())
            self.close_calls += 1
            self.close_started.set()
            self.release_close.wait(timeout=2.0)
            self._close_done = True
            self.registration.unregister()
            return Result.new_ok(OkNone())


class TestCloseLifecycle(unittest.TestCase):
    def test_fluxon_store_concurrent_close_is_linearized(self) -> None:
        backend = _FakeBackendClient()
        store = object.__new__(FluxonKVCacheStore)
        KvClient.__init__(store)
        store._client = backend
        store._close_lock = threading.Lock()
        errors = []

        def close_store() -> None:
            try:
                result = store.close()
                if result.is_ok():
                    result.unwrap()
                else:
                    errors.append(result.unwrap_error())
            except BaseException as exc:
                errors.append(exc)

        first = threading.Thread(target=close_store)
        second = threading.Thread(target=close_store)
        first.start()
        self.assertTrue(backend.close_started.wait(timeout=1.0))
        second.start()
        first.join(timeout=2.0)
        second.join(timeout=2.0)

        self.assertEqual(errors, [])
        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(backend.close_calls, 1)
        self.assertIsNone(store._client)

    def test_fluxon_store_closes_registered_child_before_backend(self) -> None:
        events = []
        backend = _FakeBackendClient(events)
        store = object.__new__(FluxonKVCacheStore)
        KvClient.__init__(store)
        store._client = backend
        store._close_lock = threading.Lock()
        child = _FakeChild(events)
        child.registration = store.register_child_close(child.close)

        result = store.close()

        self.assertTrue(result.is_ok())
        result.unwrap()
        self.assertEqual(events, ["child", "backend"])
        self.assertEqual(child.close_calls, 1)
        with self.assertRaisesRegex(RuntimeError, "KvClient is closing"):
            store.register_child_close(_FakeChild().close)

    def test_fluxon_store_keeps_backend_open_when_child_close_fails(self) -> None:
        backend = _FakeBackendClient()
        store = object.__new__(FluxonKVCacheStore)
        KvClient.__init__(store)
        store._client = backend
        store._close_lock = threading.Lock()
        child = _FakeChild(fail=True)
        child.registration = store.register_child_close(child.close)

        result = store.close()

        self.assertFalse(result.is_ok())
        self.assertIn("child close failed", str(result.unwrap_error()))
        self.assertEqual(child.close_calls, 1)
        self.assertEqual(backend.close_calls, 0)
        self.assertIs(store._client, backend)

        child.fail = False
        retry_result = store.close()
        self.assertTrue(retry_result.is_ok())
        retry_result.unwrap()
        self.assertEqual(child.close_calls, 2)
        self.assertEqual(backend.close_calls, 1)

    def test_kv_close_waits_for_registered_child_construction(self) -> None:
        events = []
        backend = _FakeBackendClient(events)
        store = object.__new__(FluxonKVCacheStore)
        KvClient.__init__(store)
        store._client = backend
        store._close_lock = threading.Lock()
        construction_entered = threading.Event()
        release_construction = threading.Event()
        child = object.__new__(_ConstructingChild)
        construction_errors = []
        close_results = []

        def construct_child() -> None:
            try:
                child.__init__(
                    store,
                    construction_entered,
                    release_construction,
                    events,
                )
            except BaseException as exc:
                construction_errors.append(exc)

        constructor = threading.Thread(target=construct_child)
        constructor.start()
        self.assertTrue(construction_entered.wait(timeout=1.0))

        closer = threading.Thread(target=lambda: close_results.append(store.close()))
        closer.start()
        self.assertFalse(backend.close_started.wait(timeout=0.05))
        release_construction.set()
        constructor.join(timeout=2.0)
        closer.join(timeout=2.0)

        self.assertEqual(construction_errors, [])
        self.assertFalse(constructor.is_alive())
        self.assertFalse(closer.is_alive())
        self.assertEqual(len(close_results), 1)
        self.assertTrue(close_results[0].is_ok())
        close_results[0].unwrap()
        self.assertTrue(child._close_done)
        self.assertEqual(events, ["child", "backend"])

    def test_explicit_child_close_racing_kv_close_runs_once(self) -> None:
        backend = _FakeBackendClient()
        store = object.__new__(FluxonKVCacheStore)
        KvClient.__init__(store)
        store._client = backend
        store._close_lock = threading.Lock()
        child = _BlockingChild()
        child.registration = store.register_child_close(child.close)
        child_results = []
        store_results = []

        child_closer = threading.Thread(
            target=lambda: child_results.append(child.close())
        )
        child_closer.start()
        self.assertTrue(child.close_started.wait(timeout=1.0))

        store_closer = threading.Thread(
            target=lambda: store_results.append(store.close())
        )
        store_closer.start()
        self.assertFalse(backend.close_started.wait(timeout=0.05))
        child.release_close.set()
        child_closer.join(timeout=2.0)
        store_closer.join(timeout=2.0)

        self.assertFalse(child_closer.is_alive())
        self.assertFalse(store_closer.is_alive())
        self.assertEqual(child.close_calls, 1)
        self.assertEqual(backend.close_calls, 1)
        self.assertEqual(len(child_results), 1)
        self.assertEqual(len(store_results), 1)
        self.assertTrue(child_results[0].is_ok())
        child_results[0].unwrap()
        self.assertTrue(store_results[0].is_ok())
        store_results[0].unwrap()

    def test_owner_service_consumes_successful_close_result(self) -> None:
        store = _FakeStore()

        def register(callback, *, thread_name):
            callback("test")
            return lambda: None

        with mock.patch.object(
            start_owner_kvclient,
            "register_ctrlc_callback",
            side_effect=register,
        ):
            start_owner_kvclient._wait_until_stopped(store)

        self.assertEqual(store.close_calls, 1)


if __name__ == "__main__":
    raise SystemExit(unittest.main())
