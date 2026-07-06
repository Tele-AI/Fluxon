from __future__ import annotations

import os
import sys
import time
import unittest
import uuid
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from setup_and_pack.utils.repo_config_utils import (  # noqa: E402
    _verify_host_port,
    load_test_etcd_address_from_test_config,
)
from fluxon_py.tool import import_fluxon_pyo3_local  # noqa: E402


_ETCD_ADDRESS = load_test_etcd_address_from_test_config()
ETCD_HOST, _ETCD_PORT = _verify_host_port(_ETCD_ADDRESS, field="test_config.yaml.etcd_address")
ETCD_PORT = int(_ETCD_PORT)


class TestPyO3EtcdKvClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.endpoint = f"{ETCD_HOST}:{ETCD_PORT}"
        cls.fluxon_pyo3 = import_fluxon_pyo3_local()

    def setUp(self) -> None:
        self.prefix = f"/fluxon_py_tests/pyo3_etcd/{os.getpid()}/{uuid.uuid4().hex}/"
        self.client = self.fluxon_pyo3.EtcdKvClient([self.endpoint])
        self.addCleanup(self._cleanup_prefix)

    def _cleanup_prefix(self) -> None:
        self.client.delete_prefix(self.prefix)

    def _wait_key_deleted(self, key: str, *, timeout_s: float = 10.0) -> None:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if self.client.get(key) is None:
                return
            time.sleep(0.2)
        self.fail(f"timed out waiting for etcd key to be deleted: {key}")

    def _assert_lease_not_live(self, lease_id: int) -> None:
        try:
            ttl = self.client.lease_ttl(lease_id)
        except RuntimeError as exc:
            if "not found" in str(exc).lower():
                return
            raise
        self.assertLessEqual(ttl, 0)

    def test_kv_prefix_and_delete_roundtrip(self) -> None:
        key_a = self.prefix + "a"
        key_b = self.prefix + "nested/b"

        self.assertIsNone(self.client.get(key_a))
        self.client.put(key_a, b"value-a")
        self.client.put(key_b, b"value-b")

        self.assertEqual(self.client.get(key_a), b"value-a")
        other_client = self.fluxon_pyo3.EtcdKvClient([self.endpoint])
        self.assertEqual(other_client.get(key_b), b"value-b")

        rows = sorted(
            (key.decode("utf-8"), value.decode("utf-8"))
            for key, value in self.client.get_prefix(self.prefix)
        )
        self.assertEqual(rows, [(key_a, "value-a"), (key_b, "value-b")])

        self.assertTrue(self.client.delete(key_a))
        self.assertFalse(self.client.delete(key_a))
        self.assertIsNone(self.client.get(key_a))

        self.assertEqual(self.client.delete_prefix(self.prefix), 1)
        self.assertEqual(self.client.delete_prefix(self.prefix), 0)
        self.assertIsNone(self.client.get(key_b))

    def test_put_with_lease_and_revoke_deletes_key(self) -> None:
        lease_mgr = self.fluxon_pyo3.LeaseManagerHandle()
        lease: Any = lease_mgr.allocate_etcd_lease([self.endpoint], 30, False)
        lease_id = int(lease.id)
        key = self.prefix + "leased"

        self.client.put(key, b"leased-value", lease_id=lease_id)
        self.assertEqual(self.client.get(key), b"leased-value")
        self.assertGreater(self.client.lease_ttl(lease_id), 0)

        self.client.revoke_lease(lease_id)
        self._wait_key_deleted(key)
        self._assert_lease_not_live(lease_id)

    def test_lock_exclusive_and_context_manager_release(self) -> None:
        lock_name = self.prefix + "lock"
        lock_a = self.fluxon_pyo3.EtcdLock([self.endpoint], lock_name, 10, 1.0)
        lock_b = self.fluxon_pyo3.EtcdLock([self.endpoint], lock_name, 10, 0.5)

        self.assertFalse(lock_a.held)
        self.assertIsNone(lock_a.lease_id)

        self.assertTrue(lock_a.acquire())
        self.assertTrue(lock_a.held)
        self.assertIsInstance(lock_a.lease_id, int)

        self.assertFalse(lock_b.acquire())
        self.assertFalse(lock_b.held)
        self.assertIsNone(lock_b.lease_id)

        first_lease_id = int(lock_a.lease_id)
        self.assertTrue(lock_a.release())
        self.assertFalse(lock_a.held)
        self.assertIsNone(lock_a.lease_id)
        self.assertFalse(lock_a.release())
        self._assert_lease_not_live(first_lease_id)

        with self.fluxon_pyo3.EtcdLock([self.endpoint], lock_name, 10, 1.0) as held_lock:
            self.assertTrue(held_lock.held)
            context_lease_id = int(held_lock.lease_id)
        self._assert_lease_not_live(context_lease_id)


if __name__ == "__main__":
    raise SystemExit(unittest.main())
