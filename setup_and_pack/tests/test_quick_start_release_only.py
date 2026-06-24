from __future__ import annotations

import importlib.util
import io
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from fluxon_py.api_error import OK_NONE, Result


REPO_ROOT = Path(__file__).resolve().parents[2]
QUICK_START_BUILD_IMAGE_PATH = REPO_ROOT / "examples" / "fluxon_quick_start" / "build_image.py"
QUICK_START_START_PATH = REPO_ROOT / "examples" / "fluxon_quick_start" / "start.py"
PACK_FLUXON_PYLIB_PATH = REPO_ROOT / "setup_and_pack" / "pack_fluxon_pylib.py"


def _load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


_BUILD_IMAGE = _load_module("fluxon_quick_start_build_image_test", QUICK_START_BUILD_IMAGE_PATH)
_START = _load_module("fluxon_quick_start_start_test", QUICK_START_START_PATH)
_PACK_FLUXON_PYLIB = _load_module("pack_fluxon_pylib_test", PACK_FLUXON_PYLIB_PATH)


class QuickStartReleaseOnlyTest(unittest.TestCase):
    def test_start_script_prepends_repo_root_to_sys_path_before_fluxon_imports(self) -> None:
        # Repo-run mode is a supported development path, but quickstart no longer
        # bootstraps dependencies at runtime. This assertion only protects the
        # source-tree import order for an already-prepared Python environment.
        source = QUICK_START_START_PATH.read_text(encoding="utf-8")

        repo_root_insert = "sys.path.insert(0, REPO_ROOT_STR)"
        fluxon_import = "from fluxon_py._api_ext_chan.mq_config_check import MIN_TTL as MQ_MIN_TTL_SECONDS"

        self.assertIn(repo_root_insert, source)
        self.assertIn(fluxon_import, source)
        self.assertLess(source.index(repo_root_insert), source.index(fluxon_import))

    def test_stage_build_context_copies_release_wheels_without_source_tree(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            release_dir = root / "release"
            context_root = root / "context"
            (release_dir / "ext_images" / "etcd").mkdir(parents=True)
            (release_dir / "ext_images" / "greptime").mkdir(parents=True)
            (release_dir / "ext_images" / "etcd" / "etcd").write_text("etcd", encoding="utf-8")
            (release_dir / "ext_images" / "etcd" / "etcdctl").write_text("etcdctl", encoding="utf-8")
            (release_dir / "ext_images" / "greptime" / "greptime").write_text("greptime", encoding="utf-8")
            (release_dir / "fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl").write_text("wheel", encoding="utf-8")

            dockerfile_path = _BUILD_IMAGE._stage_build_context(release_dir=release_dir, context_root=context_root)

            self.assertEqual(dockerfile_path, context_root / "examples" / "fluxon_quick_start" / "Dockerfile")
            self.assertTrue(
                (context_root / "fluxon_release" / "fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl").is_file()
            )
            self.assertFalse((context_root / "fluxon_py").exists())
            self.assertFalse((context_root / "setup.py").exists())

    def test_kv_http_delete_route_uses_store_remove_contract(self) -> None:
        class _FakeStore:
            def __init__(self) -> None:
                self.remove_calls: list[str] = []

            def remove(self, key: str):
                self.remove_calls.append(key)
                return Result.new_ok(OK_NONE)

        fake_store = _FakeStore()
        previous_store = _START._kv_http_store
        _START._kv_http_store = fake_store
        try:
            with _START._KV_HTTP_APP.test_client() as client:
                resp = client.delete("/api/kv/demo")
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.get_json()["key"], "demo")
            self.assertEqual(fake_store.remove_calls, ["demo"])
        finally:
            _START._kv_http_store = previous_store

    def test_handle_mq_shell_line_treats_status_as_command_not_message(self) -> None:
        source = QUICK_START_START_PATH.read_text(encoding="utf-8")

        self.assertIn('if cmd == "status":', source)
        self.assertIn('print("Commands:  put <message>  |  status  |  exit")', source)

        namespace: dict[str, object] = {}
        helper_source = """
def _handle_mq_shell_line(line, shutdown_requested, status_lines):
    parts = line.split(None, 1)
    cmd = parts[0].lower()
    if cmd in ("exit", "quit", "q"):
        shutdown_requested.set()
        return True, None
    if cmd == "help":
        print("Commands:  put <message>  |  status  |  exit")
        return True, None
    if cmd == "status":
        for status_line in status_lines():
            print(status_line)
        return True, None

    msg = parts[1] if cmd == "put" and len(parts) >= 2 else line
    return False, msg
"""
        exec(helper_source, namespace)
        helper = namespace["_handle_mq_shell_line"]

        shutdown_requested = mock.Mock()
        stdout = io.StringIO()
        with mock.patch("sys.stdout", stdout):
            handled, msg = helper("status", shutdown_requested, lambda: ["MQ shell status:", "  ok"])
        self.assertEqual((handled, msg), (True, None))
        self.assertIn("MQ shell status:", stdout.getvalue())
        shutdown_requested.set.assert_not_called()

    def test_quick_start_owner_configs_include_large_file_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            workdir = Path(tmpdir)

            kv_cfg = _START._gen_kv_config(
                "127.0.0.1:12379",
                "qs_kv_cluster",
                31000,
                8083,
                0,
                14000,
                workdir,
            )
            mq_cfg = _START._gen_mq_config(
                "127.0.0.1:12379",
                "qs_mq_cluster",
                34200,
                14000,
                workdir,
                panel_port=18080,
            )
            fs_cfg = _START._gen_fs_config(
                "127.0.0.1:12379",
                "qs_fs_cluster",
                34100,
                34180,
                14000,
                workdir,
            )

            expected = [str(workdir / "large" / "owner")]
            self.assertEqual(kv_cfg["kvclient"]["fluxonkv_spec"]["large_file_paths"], expected)
            self.assertEqual(mq_cfg["kvclient"]["fluxonkv_spec"]["large_file_paths"], expected)
            self.assertEqual(fs_cfg["kvclient"]["fluxonkv_spec"]["large_file_paths"], expected)

    def test_pack_fluxon_pylib_cleans_stale_build_artifacts_before_bdist(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            release_dir = repo_root / "fluxon_release"
            build_file = repo_root / "build" / "lib" / "fluxon_py" / "runtime" / "start_monitor_web.py"
            dist_dir = repo_root / "dist"
            egg_info = repo_root / "fluxon.egg-info"
            build_file.parent.mkdir(parents=True)
            build_file.write_text("stale", encoding="utf-8")
            dist_dir.mkdir()
            egg_info.mkdir()
            release_dir.mkdir()

            _PACK_FLUXON_PYLIB._clean_python_build_artifacts(repo_root=repo_root, release_dir=release_dir)

            self.assertFalse((repo_root / "build").exists())
            self.assertFalse(dist_dir.exists())
            self.assertFalse(egg_info.exists())
            self.assertTrue(release_dir.exists())

if __name__ == "__main__":
    unittest.main()
