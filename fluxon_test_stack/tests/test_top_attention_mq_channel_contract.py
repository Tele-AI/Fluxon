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
INDEX_DIR = REPO_ROOT / "fluxon_test_stack" / "top_attention_test_index"


def _load_module(script_name: str):
    module_path = INDEX_DIR / script_name
    sys.path.insert(0, str(INDEX_DIR))
    try:
        spec = importlib.util.spec_from_file_location(f"fluxon_test_stack_top_attention_{script_name}", module_path)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        if sys.path and sys.path[0] == str(INDEX_DIR):
            sys.path.pop(0)


class TestTopAttentionMqChannelContract(unittest.TestCase):
    def test_mq_channel_wrappers_accept_case_config_and_run_script_processes(self) -> None:
        cases = {
            "_mq_mpsc.py": (
                "ci_top_attention_mq_mpsc",
                ["fluxon_py/tests/test_api_chan_mpsc/test_api_chan_mpsc_base.py"],
            ),
            "_mq_mpmc.py": (
                "ci_top_attention_mq_mpmc",
                [
                    "fluxon_py/tests/test_api_chan_mpmc/test_api_chan_mpmc_base.py",
                    "fluxon_py/tests/test_api_chan_mpmc/test_api_chan_mpmc_quick_and_weighted_consume.py",
                    "fluxon_py/tests/test_api_chan_mpmc/test_rebind_client.py",
                    "fluxon_py/tests/test_api_chan_mpmc/test_ready_channels_access.py",
                ],
            ),
        }
        for script_name, (scene_id, test_paths) in cases.items():
            with self.subTest(script_name=script_name):
                entry = _load_module(script_name)
                with tempfile.TemporaryDirectory() as td:
                    case_cfg = Path(td) / "ci_scene_config.yaml"
                    case_cfg.write_text(
                        yaml.safe_dump(
                            {
                                "case": {
                                    "scene_id": scene_id,
                                    "scale_id": "n1_kvowner_dram_20gib",
                                    "profile_id": "fluxon_tcp_thread",
                                    "case_id": f"{scene_id}__n1_kvowner_dram_20gib__fluxon_tcp_thread",
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

                    with mock.patch.object(entry, "call", return_value=0) as call:
                        with mock.patch.object(
                            sys,
                            "argv",
                            [
                                str(INDEX_DIR / script_name),
                                "--python",
                                "/tmp/venv/bin/python3",
                                "--case-config",
                                str(case_cfg),
                            ],
                        ):
                            rc = entry.main()

                    self.assertEqual(rc, 0)
                    self.assertEqual(
                        [item.args[0] for item in call.call_args_list],
                        [
                            ["/tmp/venv/bin/python3", "-u", test_path]
                            for test_path in test_paths
                        ],
                    )

    def test_mq_mpmc_bench_wrapper_runs_script_processes(self) -> None:
        entry = _load_module("_mq_mpmc_bench.py")
        with tempfile.TemporaryDirectory() as td:
            case_cfg = Path(td) / "ci_scene_config.yaml"
            case_cfg.write_text(
                yaml.safe_dump(
                    {
                        "case": {
                            "scene_id": "ci_top_attention_mq_mpmc_bench",
                            "scale_id": "n1_kvowner_dram_20gib",
                            "profile_id": "fluxon_tcp_thread",
                            "case_id": "ci_top_attention_mq_mpmc_bench__n1_kvowner_dram_20gib__fluxon_tcp_thread",
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

            with mock.patch.object(entry, "call", return_value=0) as call:
                with mock.patch.object(
                    sys,
                    "argv",
                    [
                        str(INDEX_DIR / "_mq_mpmc_bench.py"),
                        "--python",
                        "/tmp/venv/bin/python3",
                        "--case-config",
                        str(case_cfg),
                    ],
                ):
                    rc = entry.main()

            self.assertEqual(rc, 0)
            self.assertEqual(
                [item.args[0] for item in call.call_args_list],
                [
                    [
                        "/tmp/venv/bin/python3",
                        "-u",
                        "fluxon_py/tests/test_api_chan_mpmc/test_mpmc_simple_bench.py",
                        "--producer-count",
                        "4",
                        "--consumer-counts",
                        "2",
                        "--duration-seconds",
                        "60",
                        "--sample-start-seconds",
                        "10",
                        "--sample-duration-seconds",
                        "10",
                    ],
                    [
                        "/tmp/venv/bin/python3",
                        "-u",
                        "fluxon_py/tests/test_api_chan_mpmc/test_mpmc_simple_bench2.py",
                        "--producer-count",
                        "4",
                        "--video-messages-per-producer",
                        "15",
                        "--batch-size",
                        "64",
                        "--prefetch-num",
                        "0",
                        "--channel-capacity",
                        "128",
                    ],
                ],
            )

    def test_mq_channel_wrappers_reject_mismatched_case_config_scene(self) -> None:
        entry = _load_module("_mq_mpsc.py")
        with tempfile.TemporaryDirectory() as td:
            case_cfg = Path(td) / "ci_scene_config.yaml"
            case_cfg.write_text(
                yaml.safe_dump(
                    {
                        "case": {
                            "scene_id": "ci_top_attention_mq_mpmc",
                        },
                        "scene_config": {},
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            with mock.patch.object(
                sys,
                "argv",
                [str(INDEX_DIR / "_mq_mpsc.py"), "--case-config", str(case_cfg)],
            ):
                with self.assertRaisesRegex(ValueError, "case config scene_id mismatch"):
                    entry.main()


if __name__ == "__main__":
    raise SystemExit(unittest.main())
