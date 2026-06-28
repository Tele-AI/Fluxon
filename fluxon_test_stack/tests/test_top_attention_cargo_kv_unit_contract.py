#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "fluxon_test_stack" / "top_attention_test_index" / "_cargo_kv_unit.py"


def _load_module():
    module_dir = MODULE_PATH.parent
    sys.path.insert(0, str(module_dir))
    try:
        spec = importlib.util.spec_from_file_location("fluxon_test_stack_top_attention_cargo_kv_unit_contract", MODULE_PATH)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        if sys.path and sys.path[0] == str(module_dir):
            sys.path.pop(0)


_ENTRY = _load_module()


class TestTopAttentionCargoKvUnitContract(unittest.TestCase):
    def test_main_accepts_case_config_and_uses_scene_config_feature(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            cfg_dir = run_dir / "configs"
            cfg_dir.mkdir(parents=True)
            src_dir = run_dir / "src"
            src_dir.mkdir(parents=True)
            case_cfg = cfg_dir / "ci_scene_config.yaml"
            case_cfg.write_text(
                yaml.safe_dump(
                    {
                        "case": {
                            "scene_id": "ci_top_attention_cargo_kv_unit",
                            "scale_id": "n1_kvowner_dram_20gib",
                            "profile_id": "fluxon_tcp",
                            "case_id": "ci_top_attention_cargo_kv_unit__n1_kvowner_dram_20gib__fluxon_tcp",
                        },
                        "scene_config": {
                            "kv_transport_feature": "tcp_thread_transport",
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

            with mock.patch.dict(os.environ, {"FLUXON_KV_TEST_TRANSPORT_FEATURE": "fastws_transport"}, clear=False):
                with mock.patch.object(_ENTRY, "run_cargo", return_value=0) as run_cargo:
                    with mock.patch.object(
                        sys,
                        "argv",
                        [str(MODULE_PATH), "--case-config", str(case_cfg)],
                    ):
                        rc = _ENTRY.main()

            self.assertEqual(rc, 0)
            build_cfg = yaml.safe_load((src_dir / "build_config_ext.yml").read_text(encoding="utf-8"))
            self.assertEqual(
                build_cfg,
                {
                    "etcd": "127.0.0.1:19180",
                    "prom": "http://127.0.0.1:19190/v1/prometheus",
                    "prom_remote_write_url": "http://127.0.0.1:19190/v1/prometheus/write",
                },
            )
            self.assertEqual(
                run_cargo.call_args.args[0],
                [
                    "test",
                    "--manifest-path",
                    str(REPO_ROOT / "fluxon_rs" / "fluxon_kv" / "Cargo.toml"),
                    "--no-default-features",
                    "--features",
                    "p2p_transfer,tcp_thread_transport",
                ],
            )
            self.assertNotIn("env", run_cargo.call_args.kwargs)

    def test_main_rejects_feature_override_flag(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            cfg_dir = run_dir / "configs"
            cfg_dir.mkdir(parents=True)
            case_cfg = cfg_dir / "ci_scene_config.yaml"
            case_cfg.write_text(
                yaml.safe_dump(
                    {
                        "case": {"scene_id": "ci_top_attention_cargo_kv_unit"},
                        "scene_config": {"kv_transport_feature": "tcp_thread_transport"},
                        "scene_runtime": {
                            "etcd": {"ip": "127.0.0.1", "port": 19180},
                            "greptime": {"ip": "127.0.0.1", "port": 19190},
                        },
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            with mock.patch.object(
                sys,
                "argv",
                [str(MODULE_PATH), "--case-config", str(case_cfg), "--feature", "fastws_transport"],
            ):
                with self.assertRaises(SystemExit) as cm:
                    _ENTRY.main()

        self.assertEqual(cm.exception.code, 2)

    def test_main_rejects_pytest_style_passthrough_flags(self) -> None:
        with mock.patch.object(sys, "argv", [str(MODULE_PATH), "-k", "lease"]):
            with self.assertRaises(SystemExit) as cm:
                _ENTRY.main()

        self.assertEqual(cm.exception.code, 2)


if __name__ == "__main__":
    raise SystemExit(unittest.main())
