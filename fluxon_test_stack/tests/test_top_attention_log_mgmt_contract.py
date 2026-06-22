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
MODULE_PATH = REPO_ROOT / "fluxon_test_stack" / "top_attention_test_index" / "_log_mgmt.py"


def _load_module():
    module_dir = MODULE_PATH.parent
    sys.path.insert(0, str(module_dir))
    try:
        spec = importlib.util.spec_from_file_location("fluxon_test_stack_top_attention_log_mgmt_contract", MODULE_PATH)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        if sys.path and sys.path[0] == str(module_dir):
            sys.path.pop(0)


_ENTRY = _load_module()


class TestTopAttentionLogMgmtContract(unittest.TestCase):
    def test_main_accepts_case_config_and_runs_canonical_tests(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            cfg_dir = run_dir / "configs"
            cfg_dir.mkdir(parents=True)
            case_cfg = cfg_dir / "ci_scene_config.yaml"
            case_cfg.write_text(
                yaml.safe_dump(
                    {
                        "case": {
                            "scene_id": "ci_top_attention_log_mgmt",
                            "scale_id": "n1_kvowner_dram_20gib",
                            "profile_id": "fluxon_tcp_thread",
                            "case_id": "ci_top_attention_log_mgmt__n1_kvowner_dram_20gib__fluxon_tcp_thread",
                        },
                        "scene_config": {
                            "enabled": True,
                        },
                        "scene_runtime": {
                            "etcd": {"ip": "127.0.0.1", "port": 19180},
                            "greptime": {"ip": "127.0.0.1", "port": 19190},
                        },
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            python_calls: list[tuple[str, tuple[str, ...]]] = []

            def fake_run_python_file(description: str, path: str, extra_args=()):
                del description
                python_calls.append((path, tuple(extra_args)))
                return 0

            with mock.patch.object(_ENTRY, "run_python_file", side_effect=fake_run_python_file):
                with mock.patch.object(_ENTRY, "run_cargo", return_value=0) as run_cargo:
                    with mock.patch.object(
                        sys,
                        "argv",
                        [str(MODULE_PATH), "--case-config", str(case_cfg), "--", "--nocapture"],
                    ):
                        rc = _ENTRY.main()

            self.assertEqual(rc, 0)
            self.assertEqual(
                python_calls,
                [
                    ("deployment/tests/test_log_shard.py", ("--", "--nocapture")),
                    (
                        "deployment/tests/test_selection_supervisor_codegen.py",
                        ("--test-id", "runtime_log_path_uses_daily_shard_files", "--", "--nocapture"),
                    ),
                    (
                        "deployment/tests/test_selection_supervisor_codegen.py",
                        ("--test-id", "runtime_log_shards_roll_and_preserve_content_boundaries", "--", "--nocapture"),
                    ),
                ],
            )
            self.assertEqual(
                run_cargo.call_args.args[0],
                [
                    "test",
                    "--manifest-path",
                    str(REPO_ROOT / "fluxon_rs" / "fluxon_util" / "Cargo.toml"),
                    "--test",
                    "log_mgmt",
                    "--",
                    "--nocapture",
                ],
            )

    def test_main_strips_passthrough_case_config_before_delegating(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            cfg_dir = run_dir / "configs"
            cfg_dir.mkdir(parents=True)
            case_cfg = cfg_dir / "ci_scene_config.yaml"
            case_cfg.write_text(
                yaml.safe_dump(
                    {
                        "case": {
                            "scene_id": "ci_top_attention_log_mgmt",
                            "scale_id": "n1_kvowner_dram_20gib",
                            "profile_id": "fluxon_tcp_thread",
                            "case_id": "ci_top_attention_log_mgmt__n1_kvowner_dram_20gib__fluxon_tcp_thread",
                        },
                        "scene_config": {"enabled": True},
                        "scene_runtime": {
                            "etcd": {"ip": "127.0.0.1", "port": 19180},
                            "greptime": {"ip": "127.0.0.1", "port": 19190},
                        },
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            python_calls: list[tuple[str, tuple[str, ...]]] = []

            def fake_run_python_file(description: str, path: str, extra_args=()):
                del description
                python_calls.append((path, tuple(extra_args)))
                return 0

            with mock.patch.object(_ENTRY, "run_python_file", side_effect=fake_run_python_file):
                with mock.patch.object(_ENTRY, "run_cargo", return_value=0) as run_cargo:
                    with mock.patch.object(
                        sys,
                        "argv",
                        [
                            str(MODULE_PATH),
                            "--case-config",
                            str(case_cfg),
                            "--",
                            "--case-config",
                            str(case_cfg),
                            "--nocapture",
                        ],
                    ):
                        rc = _ENTRY.main()

            self.assertEqual(rc, 0)
            self.assertEqual(
                python_calls[0],
                ("deployment/tests/test_log_shard.py", ("--", "--nocapture")),
            )
            self.assertNotIn("--case-config", run_cargo.call_args.args[0])


if __name__ == "__main__":
    raise SystemExit(unittest.main())
