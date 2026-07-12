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

from fluxon_py.api_error import OkNone, Result
from fluxon_py.kvclient.fluxon import FluxonKVCacheStore
from fluxon_py.runtime import start_owner_kvclient


class _FakeBackendClient:
    def __init__(self) -> None:
        self.close_calls = 0
        self.close_started = threading.Event()

    def close(self):
        self.close_calls += 1
        self.close_started.set()
        time.sleep(0.05)
        return Result.new_ok(OkNone())


class _FakeStore:
    def __init__(self) -> None:
        self.close_calls = 0

    def close(self):
        self.close_calls += 1
        return Result.new_ok(OkNone())


class TestCloseLifecycle(unittest.TestCase):
    def test_fluxon_store_concurrent_close_is_linearized(self) -> None:
        backend = _FakeBackendClient()
        store = object.__new__(FluxonKVCacheStore)
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
