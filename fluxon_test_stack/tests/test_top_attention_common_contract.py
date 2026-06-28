#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "fluxon_test_stack" / "top_attention_test_index" / "_common.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("fluxon_test_stack_top_attention_common_contract", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_ENTRY = _load_module()


class TestTopAttentionCommonContract(unittest.TestCase):
    def test_call_enables_rust_backtrace_for_default_env(self) -> None:
        with mock.patch.object(_ENTRY.subprocess, "call", return_value=0) as subprocess_call:
            rc = _ENTRY.call(["cargo", "test"])

        self.assertEqual(rc, 0)
        prepared_env = subprocess_call.call_args.kwargs["env"]
        self.assertEqual(prepared_env["RUST_BACKTRACE"], "1")
        self.assertEqual(prepared_env["RUST_LIB_BACKTRACE"], "1")

    def test_call_forces_rust_backtrace_for_explicit_env(self) -> None:
        with mock.patch.object(_ENTRY.subprocess, "call", return_value=0) as subprocess_call:
            rc = _ENTRY.call(
                ["cargo", "test"],
                env={
                    "PATH": "/usr/bin",
                    "RUST_BACKTRACE": "0",
                    "RUST_LIB_BACKTRACE": "0",
                },
            )

        self.assertEqual(rc, 0)
        prepared_env = subprocess_call.call_args.kwargs["env"]
        self.assertEqual(prepared_env["PATH"], "/usr/bin")
        self.assertEqual(prepared_env["RUST_BACKTRACE"], "1")
        self.assertEqual(prepared_env["RUST_LIB_BACKTRACE"], "1")

    def test_prepare_cargo_env_prefers_active_fluxon_pyo3_libs_dir_and_sanitizes_loader_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            active_site_packages = root / "venv" / "lib" / "python3.12" / "site-packages"
            active_libs_dir = active_site_packages / "fluxon_pyo3.libs"
            active_libs_dir.mkdir(parents=True)
            stale_libs_dir = root / "stale" / "site-packages" / "fluxon_pyo3.libs"
            stale_libs_dir.mkdir(parents=True)

            with mock.patch.object(
                _ENTRY.sysconfig,
                "get_paths",
                return_value={
                    "platlib": str(active_site_packages),
                    "purelib": str(active_site_packages),
                },
            ):
                with mock.patch.object(_ENTRY.site, "getsitepackages", return_value=[str(stale_libs_dir.parent)]):
                    with mock.patch.object(_ENTRY.site, "getusersitepackages", return_value=""):
                        with mock.patch.object(_ENTRY, "_resolve_repo_closed_sdk_root", return_value=None):
                            prepared_env = _ENTRY._prepare_cargo_env(
                                {
                                    "LD_LIBRARY_PATH": f"{stale_libs_dir}:/usr/lib:/opt/custom",
                                    "PATH": "/usr/bin",
                                }
                            )

            assert prepared_env is not None
            self.assertEqual(prepared_env["FLUXON_PYO3_LIBS_DIR"], str(active_libs_dir.resolve()))
            self.assertEqual(prepared_env["LD_LIBRARY_PATH"], f"{active_libs_dir.resolve()}:/usr/lib:/opt/custom")
            self.assertEqual(prepared_env["PATH"], "/usr/bin")

    def test_prepare_cargo_env_places_authoritative_fluxon_root_before_closed_sdk_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            active_site_packages = root / "venv" / "lib" / "python3.12" / "site-packages"
            active_libs_dir = active_site_packages / "fluxon_pyo3.libs"
            active_libs_dir.mkdir(parents=True)
            closed_sdk_root = root / "fluxon_release" / "closed_sdk"
            (closed_sdk_root / "lib").mkdir(parents=True)
            (closed_sdk_root / "manifest.json").write_text("{}", encoding="utf-8")
            stale_libs_dir = root / "stale" / "site-packages" / "fluxon_pyo3.libs"
            stale_libs_dir.mkdir(parents=True)

            with mock.patch.object(
                _ENTRY.sysconfig,
                "get_paths",
                return_value={
                    "platlib": str(active_site_packages),
                    "purelib": str(active_site_packages),
                },
            ):
                with mock.patch.object(_ENTRY.site, "getsitepackages", return_value=[str(stale_libs_dir.parent)]):
                    with mock.patch.object(_ENTRY.site, "getusersitepackages", return_value=""):
                        with mock.patch.object(_ENTRY, "REPO_ROOT", root):
                            prepared_env = _ENTRY._prepare_cargo_env(
                                {
                                    "LD_LIBRARY_PATH": f"{stale_libs_dir}:/usr/lib:/opt/custom",
                                    "PATH": "/usr/bin",
                                }
                            )

            assert prepared_env is not None
            self.assertEqual(prepared_env["FLUXON_PYO3_LIBS_DIR"], str(active_libs_dir.resolve()))
            self.assertEqual(prepared_env["FLUXON_COMMU_CLOSED_SDK_ROOT"], str(closed_sdk_root.resolve()))
            self.assertEqual(
                prepared_env["LD_LIBRARY_PATH"],
                f"{active_libs_dir.resolve()}:{(closed_sdk_root / 'lib').resolve()}:/usr/lib:/opt/custom",
            )
            self.assertEqual(prepared_env["PATH"], "/usr/bin")

    def test_prepare_cargo_env_prepends_repo_closed_sdk_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            closed_sdk_root = root / "fluxon_release" / "closed_sdk"
            (closed_sdk_root / "lib").mkdir(parents=True)
            (closed_sdk_root / "manifest.json").write_text("{}", encoding="utf-8")

            with mock.patch.object(_ENTRY, "REPO_ROOT", root):
                with mock.patch.object(_ENTRY, "_resolve_authoritative_fluxon_pyo3_libs_dir", return_value=None):
                    prepared_env = _ENTRY._prepare_cargo_env(
                        {
                            "LD_LIBRARY_PATH": "/usr/lib:/opt/custom",
                            "PATH": "/usr/bin",
                        }
                    )

            assert prepared_env is not None
            self.assertEqual(prepared_env["FLUXON_COMMU_CLOSED_SDK_ROOT"], str(closed_sdk_root.resolve()))
            self.assertEqual(
                prepared_env["LD_LIBRARY_PATH"],
                f"{(closed_sdk_root / 'lib').resolve()}:/usr/lib:/opt/custom",
            )
            self.assertEqual(prepared_env["PATH"], "/usr/bin")


if __name__ == "__main__":
    raise SystemExit(unittest.main())
