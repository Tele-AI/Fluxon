#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "fluxon_test_stack" / "top_attention_test_index" / "_cargo_fs_core.py"


def _load_module():
    module_dir = MODULE_PATH.parent
    sys.path.insert(0, str(module_dir))
    try:
        spec = importlib.util.spec_from_file_location("fluxon_test_stack_top_attention_cargo_fs_core_contract", MODULE_PATH)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        if sys.path and sys.path[0] == str(module_dir):
            sys.path.pop(0)


_ENTRY = _load_module()


class TestTopAttentionCargoFsCoreContract(unittest.TestCase):
    def test_main_calls_cargo_test_for_fs_core_crate(self) -> None:
        with mock.patch.object(_ENTRY, "run_cargo", return_value=0) as run_cargo:
            with mock.patch.object(sys, "argv", [str(MODULE_PATH)]):
                rc = _ENTRY.main()

        self.assertEqual(rc, 0)
        self.assertEqual(
            run_cargo.call_args.args[0],
            [
                "test",
                "--manifest-path",
                str(REPO_ROOT / "fluxon_rs" / "fluxon_fs_core" / "Cargo.toml"),
            ],
        )

    def test_main_rejects_pytest_style_passthrough_flags(self) -> None:
        with mock.patch.object(sys, "argv", [str(MODULE_PATH), "-k", "lease"]):
            with self.assertRaises(SystemExit) as cm:
                _ENTRY.main()

        self.assertEqual(cm.exception.code, 2)


if __name__ == "__main__":
    raise SystemExit(unittest.main())
