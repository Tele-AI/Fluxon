#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "fluxon_test_stack" / "top_attention_test_index" / "_mq_core.py"


def _load_module():
    module_dir = MODULE_PATH.parent
    sys.path.insert(0, str(module_dir))
    try:
        spec = importlib.util.spec_from_file_location("fluxon_test_stack_top_attention_mq_core_contract", MODULE_PATH)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        if sys.path and sys.path[0] == str(module_dir):
            sys.path.pop(0)


_ENTRY = _load_module()


class TestTopAttentionMqCoreContract(unittest.TestCase):
    def test_main_accepts_case_config_and_runs_mq_scripts_in_order(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            cfg_dir = run_dir / "configs"
            cfg_dir.mkdir(parents=True)
            case_cfg = cfg_dir / "ci_scene_config.yaml"
            case_cfg.write_text(
                yaml.safe_dump(
                    {
                        "case": {
                            "scene_id": "ci_top_attention_mq_core",
                            "scale_id": "n1_kvowner_dram_20gib",
                            "profile_id": "fluxon_tcp_thread",
                            "case_id": "ci_top_attention_mq_core__n1_kvowner_dram_20gib__fluxon_tcp_thread",
                        },
                        "scene_config": {},
                        "scene_runtime": {
                            "etcd": {"ip": "127.0.0.1", "port": 19180},
                        },
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            with mock.patch.object(_ENTRY, "call", side_effect=[0, 0]) as call:
                with mock.patch.object(
                    sys,
                    "argv",
                    [str(MODULE_PATH), "--python", "/tmp/venv/bin/python3", "--case-config", str(case_cfg)],
                ):
                    rc = _ENTRY.main()

            self.assertEqual(rc, 0)
            self.assertEqual(call.call_count, 2)
            self.assertEqual(
                call.call_args_list[0].args[0],
                [
                    "/tmp/venv/bin/python3",
                    "-u",
                    str(REPO_ROOT / "fluxon_py/tests/test_mq/test_capacity_and_auto_clean.py"),
                ],
            )
            self.assertEqual(
                call.call_args_list[1].args[0],
                [
                    "/tmp/venv/bin/python3",
                    "-u",
                    str(REPO_ROOT / "fluxon_py/tests/test_mq/test_payload_lease_error.py"),
                ],
            )

    def test_main_without_case_config_runs_scripts_without_extra_args(self) -> None:
        with mock.patch.object(_ENTRY, "call", side_effect=[0, 0]) as call:
            with mock.patch.object(sys, "argv", [str(MODULE_PATH), "--python", "/tmp/venv/bin/python3"]):
                rc = _ENTRY.main()

        self.assertEqual(
            call.call_args_list[0].args[0],
            [
                "/tmp/venv/bin/python3",
                "-u",
                str(REPO_ROOT / "fluxon_py/tests/test_mq/test_capacity_and_auto_clean.py"),
            ],
        )
        self.assertEqual(
            call.call_args_list[1].args[0],
            [
                "/tmp/venv/bin/python3",
                "-u",
                str(REPO_ROOT / "fluxon_py/tests/test_mq/test_payload_lease_error.py"),
            ],
        )
        self.assertEqual(rc, 0)

    def test_main_returns_first_non_zero_script_exit_code(self) -> None:
        with mock.patch.object(_ENTRY, "call", side_effect=[7]) as call:
            with mock.patch.object(sys, "argv", [str(MODULE_PATH), "--python", "/tmp/venv/bin/python3"]):
                rc = _ENTRY.main()

        self.assertEqual(rc, 7)
        self.assertEqual(call.call_count, 1)

    def test_main_rejects_pytest_style_passthrough_flags(self) -> None:
        with mock.patch.object(sys, "argv", [str(MODULE_PATH), "--python", "/tmp/venv/bin/python3", "-k", "payload"]):
            with self.assertRaises(SystemExit) as cm:
                _ENTRY.main()

        self.assertEqual(cm.exception.code, 2)


if __name__ == "__main__":
    raise SystemExit(unittest.main())
