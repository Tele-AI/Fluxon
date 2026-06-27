#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]
INDEX_DIR = REPO_ROOT / "fluxon_test_stack" / "top_attention_test_index"

MODULE_SPECS = {
    "_cargo_fs.py": "fluxon_rs/fluxon_fs/Cargo.toml",
    "_cargo_fs_s3_gateway.py": "fluxon_rs/fluxon_fs_s3_gateway/Cargo.toml",
}


def _load_module(module_name: str):
    module_path = INDEX_DIR / module_name
    module_dir = module_path.parent
    sys.path.insert(0, str(module_dir))
    try:
        spec = importlib.util.spec_from_file_location(
            f"fluxon_test_stack_{module_path.stem}_contract",
            module_path,
        )
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        if sys.path and sys.path[0] == str(module_dir):
            sys.path.pop(0)


class TestTopAttentionCargoTikvContract(unittest.TestCase):
    def test_main_calls_cargo_test_for_expected_manifest(self) -> None:
        for module_name, manifest_relpath in MODULE_SPECS.items():
            with self.subTest(module_name=module_name):
                entry = _load_module(module_name)
                module_path = INDEX_DIR / module_name
                with mock.patch.object(entry, "run_cargo", return_value=0) as run_cargo:
                    with mock.patch.object(sys, "argv", [str(module_path)]):
                        rc = entry.main()

                self.assertEqual(rc, 0)
                self.assertEqual(
                    run_cargo.call_args.args[0],
                    [
                        "test",
                        "--manifest-path",
                        str(REPO_ROOT / manifest_relpath),
                    ],
                )

    def test_main_rejects_pytest_style_passthrough_flags(self) -> None:
        for module_name in MODULE_SPECS:
            with self.subTest(module_name=module_name):
                entry = _load_module(module_name)
                module_path = INDEX_DIR / module_name
                with mock.patch.object(sys, "argv", [str(module_path), "-k", "lease"]):
                    with self.assertRaises(SystemExit) as cm:
                        entry.main()
                self.assertEqual(cm.exception.code, 2)


if __name__ == "__main__":
    raise SystemExit(unittest.main())
