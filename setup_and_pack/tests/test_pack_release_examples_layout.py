from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]
PACK_RELEASE_PATH = REPO_ROOT / "setup_and_pack" / "pack_release.py"


def _load_module():
    for import_root in (
        REPO_ROOT,
        PACK_RELEASE_PATH.parent,
        PACK_RELEASE_PATH.parent.parent,
    ):
        import_root_str = str(import_root)
        if import_root_str in sys.path:
            sys.path.remove(import_root_str)
        sys.path.insert(0, import_root_str)
    spec = importlib.util.spec_from_file_location("setup_and_pack_pack_release_examples_test", PACK_RELEASE_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_PACK_RELEASE = _load_module()


class PackReleaseExamplesLayoutTest(unittest.TestCase):
    def test_top_level_release_manifest_relpaths_include_closed_sdk_runtime(self) -> None:
        relpaths = _PACK_RELEASE._top_level_release_manifest_relpaths(
            wheel_name="fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl",
        )

        self.assertIn("closed_sdk/manifest.json", relpaths)
        self.assertIn("closed_sdk/lib/libfluxon_commu_core.so", relpaths)
        self.assertIn("closed_sdk/lib/libfluxon_rdma_probe.so", relpaths)
        self.assertIn("closed_sdk/native/native_runtime/include/rdma_probe_c.h", relpaths)
        self.assertIn("closed_sdk/native/native_runtime/lib/libfluxon_rdma_probe.so", relpaths)

    def test_public_parser_does_not_expose_transport_backend_flag(self) -> None:
        parser = _PACK_RELEASE._build_arg_parser()

        option_strings = {option for action in parser._actions for option in action.option_strings}

        self.assertNotIn("--transport-backend", option_strings)

    def test_public_pack_release_fixes_transport_backend_internally(self) -> None:
        self.assertEqual(_PACK_RELEASE._FIXED_TRANSPORT_BACKEND, "tcp_thread")
        self.assertEqual(_PACK_RELEASE._FIXED_TRANSPORT_PROFILE_ID, "fluxon_tcp_thread")

    def test_resolve_examples_dir_prefers_app_examples(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            (repo_root / "app" / "examples").mkdir(parents=True)
            (repo_root / "examples").mkdir()

            resolved = _PACK_RELEASE._resolve_examples_dir(repo_root=repo_root)

            self.assertEqual(resolved, repo_root / "app" / "examples")

    def test_resolve_examples_dir_falls_back_to_examples(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            (repo_root / "examples").mkdir(parents=True)

            resolved = _PACK_RELEASE._resolve_examples_dir(repo_root=repo_root)

            self.assertEqual(resolved, repo_root / "examples")

    def test_merged_fluxon_wheel_name_uses_runtime_platform_tag(self) -> None:
        merged = _PACK_RELEASE._merged_fluxon_wheel_name(
            pure_python_wheel_name="fluxon-0.2.1-py3-none-any.whl",
            pyo3_wheel_name="fluxon_pyo3-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl",
        )

        self.assertEqual(merged, "fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl")

    def test_run_pack_steps_verifies_built_and_merged_wheels_explicitly(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            release_dir = repo_root / "fluxon_release"
            pack_script = repo_root / "setup_and_pack" / "pack_fluxon_pylib.py"
            release_dir.mkdir(parents=True)
            pack_script.parent.mkdir(parents=True, exist_ok=True)
            pack_script.write_text("print('stub')\n", encoding="utf-8")
            built_pyo3_wheel = release_dir / "fluxon_pyo3-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl"
            pure_python_wheel = release_dir / "fluxon-0.2.1-py3-none-any.whl"
            merged_wheel = release_dir / "fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl"

            built_pyo3_wheel.write_text("", encoding="utf-8")
            pure_python_wheel.write_text("", encoding="utf-8")
            merged_wheel.write_text("", encoding="utf-8")

            with (
                mock.patch.object(
                    _PACK_RELEASE,
                    "_pack_rust_pyo3_wheel_via_nix",
                    return_value=built_pyo3_wheel,
                ) as pack_pyo3,
                mock.patch.object(_PACK_RELEASE, "_require_pyo3_wheel_import_probe") as verify_pyo3,
                mock.patch.object(_PACK_RELEASE, "_remove_release_wheels") as remove_wheels,
                mock.patch.object(_PACK_RELEASE.subprocess, "check_call") as check_call,
                mock.patch.object(_PACK_RELEASE, "_find_single", return_value=pure_python_wheel) as find_single,
                mock.patch.object(
                    _PACK_RELEASE,
                    "_assemble_unified_release_wheel",
                    return_value=merged_wheel,
                ) as assemble_wheel,
                mock.patch.object(_PACK_RELEASE, "_require_release_wheel_import_probe") as verify_release,
            ):
                _PACK_RELEASE._run_pack_steps(
                    repo_root=repo_root,
                    release_dir=release_dir,
                    rdma_backend="closed_sdk",
                    with_tikv_runtime=True,
                )

            pack_pyo3.assert_called_once()
            verify_pyo3.assert_called_once_with(built_pyo3_wheel)
            remove_wheels.assert_called_once()
            check_call.assert_called_once()
            find_single.assert_called_once()
            assemble_wheel.assert_called_once()
            verify_release.assert_called_once_with(merged_wheel)

    def test_release_wheel_import_probe_code_avoids_fluxon_py_package_init(self) -> None:
        probe_code = _PACK_RELEASE._release_wheel_import_probe_code()

        self.assertIn('Path(raw_path) / "fluxon_py" / "tool" / "pyo3.py"', probe_code)
        self.assertNotIn("from fluxon_py.tool import import_fluxon_pyo3_local", probe_code)
        self.assertNotIn("import fluxon_py", probe_code)

    def test_seed_invariant_release_runtime_skips_same_path_closed_sdk_copy(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            release_dir = repo_root / "fluxon_release"
            closed_sdk_dir = release_dir / "closed_sdk"
            pack_release_ext = repo_root / "setup_and_pack" / "pack_release_ext.py"
            closed_sdk_dir.mkdir(parents=True)
            pack_release_ext.parent.mkdir(parents=True, exist_ok=True)
            (release_dir / "install.py").write_text("print('install')\n", encoding="utf-8")
            (closed_sdk_dir / "manifest.json").write_text("{}", encoding="utf-8")
            pack_release_ext.write_text("print('stub')\n", encoding="utf-8")

            with mock.patch.object(_PACK_RELEASE.subprocess, "check_call") as check_call:
                _PACK_RELEASE._seed_invariant_release_runtime(
                    repo_root=repo_root,
                    release_dir=release_dir,
                    with_tikv_runtime=False,
                )

            self.assertTrue((closed_sdk_dir / "manifest.json").is_file())
            check_call.assert_called_once()


if __name__ == "__main__":
    unittest.main()
