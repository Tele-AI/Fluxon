import os
import sys
import time
import shutil
import errno
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))


def main() -> None:
    unittest.main()


from fluxon_py.config import FluxonKvClientConfig  # noqa: E402
from fluxon_py.fluxon_fs.patcher import FluxonFsPatcher  # noqa: E402
from fluxon_py.kvclient import new_store  # noqa: E402
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


def _new_fluxon_external_store_with_cluster(
    *,
    instance_key: str,
    cluster_name: str,
    share_mem_path: str,
):
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
    res = new_store(cfg)
    if not res.is_ok():
        raise RuntimeError(f"new_store failed: {res.unwrap_error()}")
    return res.unwrap()


def _clear_dir(dir_path: Path) -> None:
    for child in list(dir_path.iterdir()):
        if child.is_dir():
            shutil.rmtree(child, ignore_errors=False)
        else:
            child.unlink()


class TestFluxonFsRemoteMount(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._tmp = _new_test_dir("remote_mount")
        cls._remote_root = (cls._tmp / "remote_root").resolve()
        cls._remote_root.mkdir(parents=True, exist_ok=False)

        cls._cluster_name, cls._share_mem_path = _load_ci_cluster()

        # Keep the mountpoint under a writable temp directory to avoid relying on root paths.
        # The engine will create the mountpoint if it does not exist.
        cls._mount_dir_abs = str((cls._tmp / "mnt").resolve())
        if os.path.exists(cls._mount_dir_abs):
            raise RuntimeError(f"mount dir unexpectedly exists: {cls._mount_dir_abs}")

        cls._agent_store = _new_fluxon_external_store_with_cluster(
            instance_key=f"test_fluxon_fs_agent_{os.getpid()}",
            cluster_name=cls._cluster_name,
            share_mem_path=cls._share_mem_path,
        )
        cls._client_store = _new_fluxon_external_store_with_cluster(
            instance_key=f"test_fluxon_fs_client_{os.getpid()}",
            cluster_name=cls._cluster_name,
            share_mem_path=cls._share_mem_path,
        )

        agent_key_res = cls._agent_store.instance_key()
        if not agent_key_res.is_ok():
            raise RuntimeError(f"agent_store.instance_key failed: {agent_key_res.unwrap_error()}")
        cls._agent_node_id = agent_key_res.unwrap()

        cls._export_name = "exp1"
        cls._cache_yaml = "\n".join(
            [
                "stale_window_ms: 1000",
                "rules: []",
                "exports:",
                f"  {cls._export_name}:",
                f"    remote_root_dir_abs: {str(cls._remote_root)}",
                f"    nodes: [{cls._agent_node_id}]",
                "    cache_max_bytes: 1048576",
            ]
        )

        inner = getattr(cls._agent_store, "_client", None)
        if inner is None:
            raise RuntimeError("expected agent_store to expose _client (fluxon_pyo3.KvClient)")
        import fluxon_pyo3  # type: ignore

        reg = fluxon_pyo3.fluxon_fs_register_agent(inner, str(cls._cache_yaml))
        if not reg.is_ok():
            raise RuntimeError(f"fluxon_fs_register_agent failed: {reg.unwrap_error()}")
        _ = reg.unwrap()

        cls._patcher = FluxonFsPatcher(cls._client_store)
        cls._patcher.set_cache_config_yaml(cls._cache_yaml)
        cls._patcher.mount_remote_dir(
            local_mount_dir_abs=str(cls._mount_dir_abs),
            export_name=str(cls._export_name),
        )
        cls._patcher.install()

    @classmethod
    def tearDownClass(cls) -> None:
        if hasattr(cls, "_patcher"):
            cls._patcher.uninstall()

        for s in (
            getattr(cls, "_client_store", None),
            getattr(cls, "_agent_store", None),
        ):
            if s is None:
                continue
            res = s.close()
            if res.is_ok():
                _ = res.unwrap()
            else:
                raise RuntimeError(f"store.close failed: {res.unwrap_error()}")

        if hasattr(cls, "_tmp"):
            shutil.rmtree(cls._tmp, ignore_errors=False)

    def setUp(self) -> None:
        _clear_dir(self._remote_root)

    def _mount_path(self, rel: str) -> str:
        rel = rel.lstrip("/")
        return f"{self._mount_dir_abs}/{rel}" if rel else str(self._mount_dir_abs)

    def test_read_existing_file(self) -> None:
        (self._remote_root / "hello.txt").write_bytes(b"hello")
        with open(self._mount_path("hello.txt"), "rb") as f:
            self.assertEqual(f.read(), b"hello")

    def test_write_then_visible_in_remote_root(self) -> None:
        with open(self._mount_path("out.bin"), "wb") as f:
            f.write(b"payload")
        self.assertEqual((self._remote_root / "out.bin").read_bytes(), b"payload")

    def test_write_not_visible_until_close(self) -> None:
        p_remote = self._remote_root / "delayed.bin"
        f = open(self._mount_path("delayed.bin"), "wb")
        try:
            f.write(b"payload")

            # The remote side should not observe the new content before close.
            # `open('w')` may create/truncate the remote file early, so only check content/size.
            if p_remote.exists():
                self.assertEqual(p_remote.stat().st_size, 0)
        finally:
            f.close()

        self.assertEqual(p_remote.read_bytes(), b"payload")

    def test_append_semantics(self) -> None:
        (self._remote_root / "a.bin").write_bytes(b"a")
        with open(self._mount_path("a.bin"), "ab") as f:
            f.write(b"b")
        self.assertEqual((self._remote_root / "a.bin").read_bytes(), b"ab")

    def test_flush_uploads_remote_content(self) -> None:
        p_remote = self._remote_root / "flush.bin"
        f = open(self._mount_path("flush.bin"), "wb")
        try:
            f.write(b"payload")
            f.flush()
            self.assertEqual(p_remote.read_bytes(), b"payload")
        finally:
            f.close()
        self.assertEqual(p_remote.read_bytes(), b"payload")

    def test_stat_and_getsize(self) -> None:
        (self._remote_root / "s.bin").write_bytes(b"abc")
        st = os.stat(self._mount_path("s.bin"))
        self.assertEqual(int(st.st_size), 3)
        self.assertEqual(os.path.getsize(self._mount_path("s.bin")), 3)

    def test_exists_isfile_isdir(self) -> None:
        (self._remote_root / "f.txt").write_text("x", encoding="utf-8")
        (self._remote_root / "d").mkdir()

        self.assertTrue(os.path.exists(self._mount_path("f.txt")))
        self.assertTrue(os.path.isfile(self._mount_path("f.txt")))
        self.assertFalse(os.path.isdir(self._mount_path("f.txt")))

        self.assertTrue(os.path.exists(self._mount_path("d")))
        self.assertFalse(os.path.isfile(self._mount_path("d")))
        self.assertTrue(os.path.isdir(self._mount_path("d")))

    def test_stat_raises_for_missing(self) -> None:
        with self.assertRaises(FileNotFoundError):
            _ = os.stat(self._mount_path("missing.bin"))

    def test_getsize_raises_for_missing(self) -> None:
        with self.assertRaises(FileNotFoundError):
            _ = os.path.getsize(self._mount_path("missing.bin"))

    def test_os_open_read_is_not_supported_for_remote_mount(self) -> None:
        (self._remote_root / "r.bin").write_bytes(b"abc")
        with self.assertRaises(OSError) as ctx:
            _ = os.open(self._mount_path("r.bin"), os.O_RDONLY)
        self.assertEqual(ctx.exception.errno, errno.ENOTSUP)

    def test_os_open_write_is_not_supported_for_remote_mount(self) -> None:
        p_remote = self._remote_root / "w.bin"
        with self.assertRaises(OSError) as ctx:
            _ = os.open(self._mount_path("w.bin"), os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o644)
        self.assertEqual(ctx.exception.errno, errno.ENOTSUP)
        self.assertFalse(p_remote.exists())

    def test_truncate_on_open_w(self) -> None:
        p_remote = self._remote_root / "t.bin"
        p_remote.write_bytes(b"old")
        f = open(self._mount_path("t.bin"), "wb")
        try:
            # `open('w')` applies truncate semantics early.
            self.assertEqual(p_remote.stat().st_size, 0)
            f.write(b"new")
        finally:
            f.close()
        self.assertEqual(p_remote.read_bytes(), b"new")

    def test_listdir_and_scandir(self) -> None:
        (self._remote_root / "a.txt").write_text("a", encoding="utf-8")
        (self._remote_root / "sub").mkdir()
        (self._remote_root / "sub" / "b.txt").write_text("b", encoding="utf-8")

        names = set(os.listdir(self._mount_path("")))
        self.assertIn("a.txt", names)
        self.assertIn("sub", names)

        with os.scandir(self._mount_path("")) as it:
            got = {e.name: e for e in it}
        self.assertTrue(got["a.txt"].is_file())
        self.assertTrue(got["sub"].is_dir())
        st = got["a.txt"].stat()
        self.assertGreater(st.st_size, 0)

    def test_mkdir_rmdir(self) -> None:
        os.mkdir(self._mount_path("d"))
        self.assertTrue((self._remote_root / "d").is_dir())
        os.rmdir(self._mount_path("d"))
        self.assertFalse((self._remote_root / "d").exists())

    def test_rename_and_unlink(self) -> None:
        with open(self._mount_path("x.txt"), "wb") as f:
            f.write(b"x")

        os.rename(self._mount_path("x.txt"), self._mount_path("y.txt"))
        self.assertFalse((self._remote_root / "x.txt").exists())
        self.assertEqual((self._remote_root / "y.txt").read_bytes(), b"x")

        os.unlink(self._mount_path("y.txt"))
        self.assertFalse((self._remote_root / "y.txt").exists())


if __name__ == "__main__":
    main()
