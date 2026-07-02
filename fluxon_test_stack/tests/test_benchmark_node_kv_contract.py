#!/usr/bin/env python3

from __future__ import annotations

import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
TEST_STACK_DIR = REPO_ROOT / "fluxon_test_stack"
if str(TEST_STACK_DIR) not in sys.path:
    sys.path.insert(0, str(TEST_STACK_DIR))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from fluxon_py.api_error import OkNone, Result
from fluxon_py.kvclient.kvclient_interface import KvLeaseApi
from benchmark_node_kv import FluxonBlockingStore


class _FakeFluxonStore:
    def __init__(self) -> None:
        self._client = object()
        self.zero_contribution_checked = False
        self.calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    def _record(self, name: str, *args: object, **kwargs: object) -> str:
        self.calls.append((name, args, kwargs))
        return f"{name}-result"

    def put(self, *args: object, **kwargs: object) -> str:
        return self._record("put", *args, **kwargs)

    def get(self, *args: object, **kwargs: object) -> str:
        return self._record("get", *args, **kwargs)

    def get_size(self, *args: object, **kwargs: object) -> str:
        return self._record("get_size", *args, **kwargs)

    def is_exist(self, *args: object, **kwargs: object) -> str:
        return self._record("is_exist", *args, **kwargs)

    def remove(self, *args: object, **kwargs: object) -> str:
        return self._record("remove", *args, **kwargs)

    def sync_kv_to_file(self, *args: object, **kwargs: object) -> str:
        return self._record("sync_kv_to_file", *args, **kwargs)

    def instance_key(self) -> Result[str, object]:
        return Result.new_ok("bench-instance")

    def config(self) -> str:
        return "bench-config"

    def get_cluster_name(self) -> str:
        return "fluxon_benchmark"

    def get_etcd_config(self) -> list[str]:
        return ["127.0.0.1:2379"]

    def third_party_logs_dir(self) -> Result[str, object]:
        return Result.new_ok("/tmp/fluxon-logs")

    def ensure_zero_contribution_for_channel(self) -> None:
        self.zero_contribution_checked = True

    def count_prefix(self, prefix: str) -> Result[int, object]:
        self.calls.append(("count_prefix", (prefix,), {}))
        return Result.new_ok(3)

    def allocate_lease(self, ttl_seconds: int) -> Result[int, object]:
        self.calls.append(("allocate_lease", (ttl_seconds,), {}))
        return Result.new_ok(42)

    def keepalive_lease(self, lease_id: int) -> Result[OkNone, object]:
        self.calls.append(("keepalive_lease", (lease_id,), {}))
        return Result.new_ok(OkNone())

    def close(self) -> Result[OkNone, object]:
        return Result.new_ok(OkNone())


class TestBenchmarkNodeKvContract(unittest.TestCase):
    def test_fluxon_blocking_store_exposes_channel_backend_contract(self) -> None:
        raw_store = _FakeFluxonStore()
        store = FluxonBlockingStore(raw_store)  # type: ignore[arg-type]

        self.assertIsInstance(store, KvLeaseApi)
        self.assertIs(store._client, raw_store._client)
        self.assertEqual(store.get_etcd_config(), ["127.0.0.1:2379"])
        self.assertEqual(store.get_cluster_name(), "fluxon_benchmark")
        self.assertEqual(store.config(), "bench-config")

        store.ensure_zero_contribution_for_channel()
        self.assertTrue(raw_store.zero_contribution_checked)

        self.assertEqual(store.count_prefix("/mpmc/1/").unwrap(), 3)
        self.assertEqual(store.allocate_lease(90).unwrap(), 42)
        self.assertIsInstance(store.keepalive_lease(42).unwrap(), OkNone)

        self.assertEqual(store.get("k"), "get-result")
        self.assertEqual(store.remove("k"), "remove-result")
        self.assertEqual(
            store.sync_kv_to_file("k", "node-a", "/tmp/out", 7, "payload", timeout_ms=10000),
            "sync_kv_to_file-result",
        )
        self.assertEqual(
            raw_store.calls[-3:],
            [
                ("get", ("k",), {}),
                ("remove", ("k",), {}),
                (
                    "sync_kv_to_file",
                    ("k", "node-a", "/tmp/out", 7, "payload"),
                    {"timeout_ms": 10000},
                ),
            ],
        )


if __name__ == "__main__":
    raise SystemExit(unittest.main())
