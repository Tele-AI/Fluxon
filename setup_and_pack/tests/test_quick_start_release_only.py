from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
QUICK_START_BUILD_IMAGE_PATH = REPO_ROOT / "examples" / "fluxon_quick_start" / "build_image.py"
QUICK_START_START_PATH = REPO_ROOT / "examples" / "fluxon_quick_start" / "start.py"


def _load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


_BUILD_IMAGE = _load_module("fluxon_quick_start_build_image_test", QUICK_START_BUILD_IMAGE_PATH)
_START = _load_module("fluxon_quick_start_start_test", QUICK_START_START_PATH)


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

if __name__ == "__main__":
    unittest.main()
