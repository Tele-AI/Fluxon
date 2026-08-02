from __future__ import annotations

import sys
import threading
import types
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))


if "mooncake.store" not in sys.modules:
    mooncake_module = types.ModuleType("mooncake")
    mooncake_store_module = types.ModuleType("mooncake.store")

    class _MooncakeDistributedStore:
        pass

    mooncake_store_module.MooncakeDistributedStore = _MooncakeDistributedStore
    mooncake_module.store = mooncake_store_module
    sys.modules["mooncake"] = mooncake_module
    sys.modules["mooncake.store"] = mooncake_store_module


from fluxon_py.api_error import KeyAlreadyExistsError
from fluxon_py.config import FluxonKvClientConfig
from fluxon_py.kvclient import mooncake as mooncake_backend
from fluxon_py.kvclient.kvclient_interface import PutOptionalArgs
from fluxon_py.kvclient.mooncake import MooncakeStore


class _ReadWriteLock:
    def read_lock(self):
        return nullcontext()

    def write_lock(self):
        return nullcontext()


class _NativeStore:
    def __init__(self, *, put_retcode: int = 0) -> None:
        self.put_retcode = put_retcode
        self.calls: list[tuple[object, ...]] = []

    def remove(self, key: str, force: bool) -> int:
        self.calls.append(("remove", key, force))
        return 0

    def put(self, key: str, value: bytes) -> int:
        self.calls.append(("put", key, value))
        return self.put_retcode


class _SetupStore:
    def __init__(self) -> None:
        self.setup_args: tuple[object, ...] | None = None

    def setup(self, *args: object) -> int:
        self.setup_args = args
        return 0


class TestMooncakeTransportConfig(unittest.TestCase):
    def _config(
        self,
        protocol_type: str,
        rdma_device_names: list[str] | None = None,
    ) -> FluxonKvClientConfig:
        test_spec_config: dict[str, object] = {"protocol_type": protocol_type}
        if rdma_device_names is not None:
            test_spec_config["rdma_device_names"] = rdma_device_names
        return FluxonKvClientConfig(
            {
                "instance_key": f"mooncake-{protocol_type}",
                "contribute_to_cluster_pool_size": {
                    "dram": 16777216,
                    "vram": {},
                },
                "test_spec_config": test_spec_config,
                "mooncake_spec": {
                    "local_buffer_size": 16777216,
                    "metadata_server": "http://localhost:8080/metadata",
                    "master_server_address": "localhost:8081",
                    "etcd_addresses": ["localhost:2379"],
                },
            }
        )

    def test_setup_uses_test_spec_protocol(self) -> None:
        cases = (
            ("tcp", None, ""),
            ("rdma", ["mlx5_1", "mlx5_0"], "mlx5_0,mlx5_1"),
        )
        for protocol_type, rdma_device_names, expected_device_name in cases:
            with self.subTest(protocol_type=protocol_type):
                native_store = _SetupStore()
                with mock.patch.object(
                    mooncake_backend,
                    "MooncakeDistributedStore",
                    return_value=native_store,
                ):
                    MooncakeStore._allow_init = True
                    try:
                        store = MooncakeStore(
                            self._config(protocol_type, rdma_device_names)
                        )
                    finally:
                        MooncakeStore._allow_init = False
                store._thread_pool.shutdown(wait=True)

                self.assertIsNotNone(native_store.setup_args)
                assert native_store.setup_args is not None
                self.assertEqual(native_store.setup_args[4], protocol_type)
                self.assertEqual(native_store.setup_args[5], expected_device_name)


class TestMooncakePutContract(unittest.TestCase):
    def _new_store(self, native_store: _NativeStore) -> tuple[MooncakeStore, list[int]]:
        store = object.__new__(MooncakeStore)
        store._initialized = True
        store._store = native_store
        store._rwlock = _ReadWriteLock()
        store._renew_lock = threading.Lock()
        store._thread_pool = ThreadPoolExecutor(max_workers=1)
        renew_calls: list[int] = []

        def _unexpected_renew():
            renew_calls.append(1)
            raise AssertionError("Mooncake write-once rejection must not renew the store")

        store.renew_store = _unexpected_renew
        self.addCleanup(store._thread_pool.shutdown, wait=True)
        return store, renew_calls

    def test_reject_if_exists_put_skips_remove(self) -> None:
        native_store = _NativeStore()
        store, renew_calls = self._new_store(native_store)

        result = store.put_blocking(
            "key",
            {"payload": b"value"},
            opts=PutOptionalArgs(reject_if_exists=True),
        )

        self.assertTrue(result.is_ok())
        _ = result.unwrap()
        self.assertEqual([call[0] for call in native_store.calls], ["put"])
        self.assertEqual(renew_calls, [])

    def test_overwrite_put_removes_before_put(self) -> None:
        for opts in (None, PutOptionalArgs(reject_if_exists=False)):
            with self.subTest(opts=opts):
                native_store = _NativeStore()
                store, renew_calls = self._new_store(native_store)

                result = store.put_blocking("key", {"payload": b"value"}, opts=opts)

                self.assertTrue(result.is_ok())
                _ = result.unwrap()
                self.assertEqual(
                    [call[0] for call in native_store.calls],
                    ["remove", "put"],
                )
                self.assertEqual(renew_calls, [])

    def test_reject_if_exists_native_already_exists_does_not_renew(self) -> None:
        native_store = _NativeStore(put_retcode=-705)
        store, renew_calls = self._new_store(native_store)

        result = store.put_blocking(
            "key",
            {"payload": b"value"},
            opts=PutOptionalArgs(reject_if_exists=True),
        )

        self.assertFalse(result.is_ok())
        self.assertIsInstance(result.unwrap_error(), KeyAlreadyExistsError)
        self.assertEqual([call[0] for call in native_store.calls], ["put"])
        self.assertEqual(renew_calls, [])


if __name__ == "__main__":
    raise SystemExit(unittest.main())
