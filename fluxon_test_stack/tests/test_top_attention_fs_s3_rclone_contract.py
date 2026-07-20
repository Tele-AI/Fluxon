#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import os
import sys
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = (
    REPO_ROOT
    / "fluxon_test_stack"
    / "top_attention_test_index"
    / "_fs_s3_rclone.py"
)
E2E_PATH = REPO_ROOT / "fluxon_py" / "tests" / "fluxon_fs_rclone_e2e.py"


def _load_module():
    module_dir = MODULE_PATH.parent
    sys.path.insert(0, str(module_dir))
    try:
        spec = importlib.util.spec_from_file_location(
            "fluxon_test_stack_top_attention_fs_s3_rclone_contract",
            MODULE_PATH,
        )
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        if sys.path and sys.path[0] == str(module_dir):
            sys.path.pop(0)


_ENTRY = _load_module()


def _load_e2e_module():
    spec = importlib.util.spec_from_file_location(
        "fluxon_fs_rclone_e2e_fixture_contract",
        E2E_PATH,
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_E2E = _load_e2e_module()


class TestTopAttentionFsS3RcloneContract(unittest.TestCase):
    def test_complex_fixture_stays_below_one_s3_list_page(self) -> None:
        files = _E2E._build_complex_fixture_files()

        self.assertEqual(len(files), 405)
        self.assertLess(len(files), 1000)
        self.assertEqual(sum(relpath.startswith("fanout/") for relpath in files), 400)
        self.assertIn("deep/l1/l2/l3/l4/l5/l6/l7/l8/final.bin", files)
        self.assertEqual(len(files["blobs/medium-8m.bin"]), 8 * 1024 * 1024)
        self.assertTrue(all(" " not in relpath for relpath in files))

    def test_main_runs_direct_e2e_with_pinned_image(self) -> None:
        python = "/tmp/test-python"
        with mock.patch.dict(
            os.environ,
            {_ENTRY.RCLONE_IMAGE_ENV: _ENTRY.RCLONE_IMAGE_REF},
            clear=False,
        ):
            with mock.patch.object(_ENTRY, "call", return_value=0) as call:
                with mock.patch.object(
                    sys,
                    "argv",
                    [str(MODULE_PATH), "--python", python],
                ):
                    rc = _ENTRY.main()

        self.assertEqual(rc, 0)
        self.assertEqual(
            call.call_args.args[0],
            [
                python,
                "-u",
                str(REPO_ROOT / "fluxon_py" / "tests" / "fluxon_fs_rclone_e2e.py"),
                "--rclone-image-ref",
                "rclone/rclone:1.60.1",
            ],
        )

    def test_main_requires_exact_pinned_image(self) -> None:
        for image_ref in ("", "rclone/rclone:latest", "rclone/rclone:1.74.4"):
            with self.subTest(image_ref=image_ref):
                with mock.patch.dict(
                    os.environ,
                    {_ENTRY.RCLONE_IMAGE_ENV: image_ref},
                    clear=False,
                ):
                    with mock.patch.object(sys, "argv", [str(MODULE_PATH)]):
                        with self.assertRaisesRegex(ValueError, "must be exactly"):
                            _ENTRY.main()

    def test_main_rejects_pytest_style_passthrough_flags(self) -> None:
        with mock.patch.dict(
            os.environ,
            {_ENTRY.RCLONE_IMAGE_ENV: _ENTRY.RCLONE_IMAGE_REF},
            clear=False,
        ):
            with mock.patch.object(sys, "argv", [str(MODULE_PATH), "-k", "copy"]):
                with self.assertRaises(SystemExit) as cm:
                    _ENTRY.main()

        self.assertEqual(cm.exception.code, 2)


if __name__ == "__main__":
    raise SystemExit(unittest.main())
