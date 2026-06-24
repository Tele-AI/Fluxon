import os
import sys
import time
import shutil
import builtins
import unittest
from pathlib import Path

import types


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))


def main() -> None:
    unittest.main()


from fluxon_py.config import FluxonKvClientConfig  # noqa: E402
from fluxon_py.fluxon_fs.patcher import FluxonFsPatcher, _FluxonRemoteFileRaw  # noqa: E402
from fluxon_py.kvclient import new_store  # noqa: E402
from fluxon_py.api_error import KeyNotFoundError  # noqa: E402
from fluxon_py.tests.test_lib import (  # noqa: E402
    load_test_fluxon_cluster_name,
    load_test_fluxon_share_mem_path,
)


def _new_test_dir(tag: str) -> Path:
    base = REPO_ROOT / "fluxon_py" / "tests" / ".tmp_fluxon_fs"
    base.mkdir(parents=True, exist_ok=True)
    p = base / f"{tag}_{int(time.time() * 1000)}_{os.getpid()}"
    p.mkdir(parents=True, exist_ok=False)
    return p


def _load_ci_cluster() -> tuple[str, str]:
    return (
        load_test_fluxon_cluster_name(),
        load_test_fluxon_share_mem_path(),
    )


def _new_fluxon_external_store(*, instance_key: str):
    cluster_name, share_mem_path = _load_ci_cluster()
    cfg = FluxonKvClientConfig(
        {
            "instance_key": instance_key,
            "contribute_to_cluster_pool_size": {"dram": 0, "vram": {}},
            "fluxonkv_spec": {
                "cluster_name": cluster_name,
                "share_mem_path": share_mem_path,
            },
        }
    )
    # English note:
    # - Store creation hits etcd during init (member registration / metadata publish).
    # - In real dual-node CI, we occasionally observe transient etcd "request timed out"
    #   during cluster bring-up or tear-down (especially under heavier transports).
    # - This is bounded retry only: it must not mask persistent misconfiguration.
    max_attempts = 5
    sleep_ms_base = 200
    sleep_ms_max = 2_000
    last_err = None
    for attempt in range(1, max_attempts + 1):
        res = new_store(cfg)
        if res.is_ok():
            return res.unwrap()
        err = res.unwrap_error()
        last_err = err
        rendered = str(err)
        transient_etcd = (
            ("transport_user='etcd'" in rendered and "etcdserver: request timed out" in rendered)
            or ("transport_user='etcd'" in rendered and "GRpcStatus(Status { code: Unavailable" in rendered)
        )
        if (not transient_etcd) or attempt == max_attempts:
            raise RuntimeError(f"new_store failed: {last_err}")
        sleep_ms = min(sleep_ms_base * attempt, sleep_ms_max)
        print(
            f"[test_fluxon_fs_patcher] transient etcd error while creating store; "
            f"retrying in {sleep_ms}ms (attempt {attempt}/{max_attempts})",
            flush=True,
        )
        time.sleep(float(sleep_ms) / 1000.0)
    raise RuntimeError(f"new_store failed: {last_err}")


class TestFluxonFsPatcherRejectsNonFluxonStore(unittest.TestCase):
    def test_rejects_store_without_pyo3_client(self) -> None:
        class _NoClient:
            pass

        with self.assertRaisesRegex(RuntimeError, r"fluxon_fs requires a fluxon backend store"):
            _ = FluxonFsPatcher(_NoClient())


class TestFluxonFsRemoteFileRaw(unittest.TestCase):
    def test_close_flushes_before_session_close_without_second_session_flush(self) -> None:
        class _FakeSession:
            def __init__(self) -> None:
                self.closed = False
                self.flush_calls = 0
                self.close_calls = 0

            def stat(self):
                return (True, True, False, 0, 0, 0, 0, 0, 0, 0, 1)

            def flush(self) -> None:
                if self.closed:
                    raise OSError(9, "file session is closed")
                self.flush_calls += 1

            def close(self) -> None:
                if self.closed:
                    raise OSError(9, "file session is closed")
                self.close_calls += 1
                self.closed = True

        raw = _FluxonRemoteFileRaw(
            session=_FakeSession(),
            mode="wb",
            file_abs="/tmp/demo.bin",
            chunk_bytes=4096,
        )
        raw.close()

        self.assertTrue(raw.closed)
        self.assertEqual(raw._session.flush_calls, 1)
        self.assertEqual(raw._session.close_calls, 1)

        with self.assertRaises(ValueError):
            raw.flush()


class TestFluxonFsPatcherInstallUninstall(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = _new_test_dir("patcher_basic")
        self.addCleanup(lambda: shutil.rmtree(self._tmp, ignore_errors=False))

        self._store = _new_fluxon_external_store(
            instance_key=f"test_fluxon_fs_patcher_{os.getpid()}_basic",
        )
        self.addCleanup(self._close_store)

    def _close_store(self) -> None:
        res = self._store.close()
        if res.is_ok():
            _ = res.unwrap()
        else:
            # Consume the error to satisfy Result.__del__ invariants (do not leave it unconsumed).
            err = res.unwrap_error()
            print(f"[test_fluxon_fs_patcher][WARNING] store.close returned error (ignored): {err}", flush=True)

    def test_install_and_uninstall_restore_globals(self) -> None:
        orig_open = builtins.open
        orig_os_open = os.open
        orig_os_close = os.close
        orig_stat = os.stat
        orig_listdir = os.listdir

        patcher = FluxonFsPatcher(self._store)
        patcher.install()
        try:
            self.assertIsNot(builtins.open, orig_open)
            self.assertIsNot(os.open, orig_os_open)
            self.assertIsNot(os.close, orig_os_close)
            self.assertIsNot(os.stat, orig_stat)
            self.assertIsNot(os.listdir, orig_listdir)

            local_file = self._tmp / "hello.txt"
            local_file.write_text("hello", encoding="utf-8")

            with open(str(local_file), "r", encoding="utf-8") as f:
                self.assertEqual(f.read(), "hello")
        finally:
            patcher.uninstall()

        self.assertIs(builtins.open, orig_open)
        self.assertIs(os.open, orig_os_open)
        self.assertIs(os.close, orig_os_close)
        self.assertIs(os.stat, orig_stat)
        self.assertIs(os.listdir, orig_listdir)


class TestFluxonFsPatcherPatchesLoadedModules(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = _new_test_dir("patcher_patch_loaded_modules")
        self.addCleanup(lambda: shutil.rmtree(self._tmp, ignore_errors=False))

        self._store = _new_fluxon_external_store(
            instance_key=f"test_fluxon_fs_patcher_{os.getpid()}_loaded_modules",
        )
        self.addCleanup(self._close_store)

    def _close_store(self) -> None:
        res = self._store.close()
        if res.is_ok():
            _ = res.unwrap()
        else:
            # Consume the error to satisfy Result.__del__ invariants (do not leave it unconsumed).
            err = res.unwrap_error()
            print(f"[test_fluxon_fs_patcher][WARNING] store.close returned error (ignored): {err}", flush=True)

    def test_patches_open_alias_in_preloaded_module(self) -> None:
        m = types.ModuleType("_fluxon_fs_test_preloaded")
        m.open = builtins.open
        sys.modules[m.__name__] = m
        self.addCleanup(lambda: sys.modules.pop(m.__name__, None))

        patcher = FluxonFsPatcher(self._store)
        patcher.install()
        try:
            self.assertIsNot(m.open, builtins.open)
        finally:
            patcher.uninstall()

        self.assertIs(m.open, builtins.open)


class TestFluxonFsLocalWriteThrough(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # English note: reuse one store across tests to avoid churn in etcd membership
        # operations (join/leave) that can amplify transient etcd timeouts under load.
        cls._shared_store = _new_fluxon_external_store(
            instance_key=f"test_fluxon_fs_patcher_{os.getpid()}_local_shared",
        )

    @classmethod
    def tearDownClass(cls) -> None:
        res = cls._shared_store.close()
        if res.is_ok():
            _ = res.unwrap()
        else:
            # Consume the error to satisfy Result.__del__ invariants (do not leave it unconsumed).
            err = res.unwrap_error()
            print(
                f"[test_fluxon_fs_patcher][WARNING] shared store.close returned error (ignored): {err}",
                flush=True,
            )

    def setUp(self) -> None:
        self._tmp = _new_test_dir("patcher_local_write_through")
        self.addCleanup(lambda: shutil.rmtree(self._tmp, ignore_errors=False))

        self._store = type(self)._shared_store

    def test_local_write_through_on_close_updates_kv(self) -> None:
        local_dir_abs = str(self._tmp.resolve())
        kv_key_prefix = f"/tests/fluxon_fs_local_write_through/{os.getpid()}/"
        bytes_field_key = "payload"

        cache_yaml = "\n".join(
            [
                "stale_window_ms: 1000",
                "rules:",
                "  - dir_abs: " + local_dir_abs,
                "    cache_mode: read_through",
                "    write_mode: write_through",
                "    kv_key_prefix: " + kv_key_prefix,
                "    bytes_field_key: " + bytes_field_key,
                "    max_cache_bytes: 1048576",
                "    on_refresh_error: apply_stale_window",
                "exports: {}",
            ]
        )

        patcher = FluxonFsPatcher(self._store)
        patcher.set_cache_config_yaml(cache_yaml)
        patcher.install()
        self.addCleanup(patcher.uninstall)

        file_path = self._tmp / "data.bin"
        payload = b"hello_fluxon_fs"

        kv_key = kv_key_prefix + "data.bin"

        # Our write-through guarantee is bound to close (which implies flush), not to write().
        f = open(str(file_path), "wb")
        try:
            f.write(payload)

            get_res = self._store.get(kv_key)
            if not get_res.is_ok():
                self.fail(f"store.get failed: {get_res.unwrap_error()}")
            fut = get_res.unwrap()
            wait_res = fut.wait()
            self.assertFalse(wait_res.is_ok())
            self.assertIsInstance(wait_res.unwrap_error(), KeyNotFoundError)
        finally:
            f.close()

        get_res = self._store.get(kv_key)
        if not get_res.is_ok():
            self.fail(f"store.get failed: {get_res.unwrap_error()}")
        fut = get_res.unwrap()
        wait_res = fut.wait()
        if not wait_res.is_ok():
            self.fail(f"store.get.wait failed: {wait_res.unwrap_error()}")
        holder = wait_res.unwrap()
        data = holder.access().unwrap()
        got = data.get(bytes_field_key)
        self.assertEqual(got, payload)

    def test_local_write_through_not_visible_after_flush(self) -> None:
        local_dir_abs = str(self._tmp.resolve())
        kv_key_prefix = f"/tests/fluxon_fs_local_write_through_flush/{os.getpid()}/"
        bytes_field_key = "payload"

        cache_yaml = "\n".join(
            [
                "stale_window_ms: 1000",
                "rules:",
                "  - dir_abs: " + local_dir_abs,
                "    cache_mode: read_through",
                "    write_mode: write_through",
                "    kv_key_prefix: " + kv_key_prefix,
                "    bytes_field_key: " + bytes_field_key,
                "    max_cache_bytes: 1048576",
                "    on_refresh_error: apply_stale_window",
                "exports: {}",
            ]
        )

        patcher = FluxonFsPatcher(self._store)
        patcher.set_cache_config_yaml(cache_yaml)
        patcher.install()
        self.addCleanup(patcher.uninstall)

        file_path = self._tmp / "flush_only.bin"
        payload = b"flush_only"
        kv_key = kv_key_prefix + "flush_only.bin"

        # The write-through guarantee is bound to close (and thus flush), not to flush().
        f = open(str(file_path), "wb")
        try:
            f.write(payload)
            f.flush()

            get_res = self._store.get(kv_key)
            if not get_res.is_ok():
                self.fail(f"store.get failed: {get_res.unwrap_error()}")
            wait_res = get_res.unwrap().wait()
            self.assertFalse(wait_res.is_ok())
            self.assertIsInstance(wait_res.unwrap_error(), KeyNotFoundError)
        finally:
            f.close()

        wait_res = self._store.get(kv_key).unwrap().wait()
        if not wait_res.is_ok():
            self.fail(f"store.get.wait failed: {wait_res.unwrap_error()}")
        holder = wait_res.unwrap()
        got = holder.access().unwrap().get(bytes_field_key)
        self.assertEqual(got, payload)

    def test_local_write_through_os_open_triggers_on_close(self) -> None:
        local_dir_abs = str(self._tmp.resolve())
        kv_key_prefix = f"/tests/fluxon_fs_local_write_through_os_open/{os.getpid()}/"
        bytes_field_key = "payload"

        cache_yaml = "\n".join(
            [
                "stale_window_ms: 1000",
                "rules:",
                "  - dir_abs: " + local_dir_abs,
                "    cache_mode: read_through",
                "    write_mode: write_through",
                "    kv_key_prefix: " + kv_key_prefix,
                "    bytes_field_key: " + bytes_field_key,
                "    max_cache_bytes: 1048576",
                "    on_refresh_error: apply_stale_window",
                "exports: {}",
            ]
        )

        patcher = FluxonFsPatcher(self._store)
        patcher.set_cache_config_yaml(cache_yaml)
        patcher.install()
        self.addCleanup(patcher.uninstall)

        file_path = self._tmp / "fd.bin"
        payload = b"fd_payload"
        kv_key = kv_key_prefix + "fd.bin"

        fd = os.open(str(file_path), os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o644)
        try:
            os.write(fd, payload)

            wait_res = self._store.get(kv_key).unwrap().wait()
            self.assertFalse(wait_res.is_ok())
            self.assertIsInstance(wait_res.unwrap_error(), KeyNotFoundError)
        finally:
            os.close(fd)

        wait_res = self._store.get(kv_key).unwrap().wait()
        if not wait_res.is_ok():
            self.fail(f"store.get.wait failed: {wait_res.unwrap_error()}")
        holder = wait_res.unwrap()
        got = holder.access().unwrap().get(bytes_field_key)
        self.assertEqual(got, payload)


if __name__ == "__main__":
    main()
