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


def _load_module(relpath: str, module_name: str):
    module_path = REPO_ROOT / relpath
    module_dir = module_path.parent
    sys.path.insert(0, str(module_dir))
    try:
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        if sys.path and sys.path[0] == str(module_dir):
            sys.path.pop(0)


class TestTopAttentionWrapperCaseConfigContract(unittest.TestCase):
    def _case_cfg_path(self, *, scene_id: str) -> Path:
        td = tempfile.TemporaryDirectory()
        self.addCleanup(td.cleanup)
        cfg_path = Path(td.name) / "ci_scene_config.yaml"
        cfg_path.write_text(
            yaml.safe_dump(
                {
                    "case": {
                        "scene_id": scene_id,
                        "scale_id": "n1_kvowner_dram_3gib",
                        "profile_id": "fluxon_tcp_thread",
                        "case_id": f"{scene_id}__n1_kvowner_dram_3gib__fluxon_tcp_thread",
                    },
                    "scene_config": {},
                    "scene_runtime": {
                        "etcd": {"ip": "127.0.0.1", "port": 19180},
                        "greptime": {"ip": "127.0.0.1", "port": 19190},
                    },
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        return cfg_path

    def test_config_kv_wrapper_accepts_case_config_without_forwarding_it(self) -> None:
        entry = _load_module(
            "fluxon_test_stack/top_attention_test_index/_config_kv.py",
            "fluxon_test_stack_top_attention_config_kv_contract",
        )
        cfg_path = self._case_cfg_path(scene_id="ci_top_attention_config_kv")
        with (
            mock.patch.object(entry, "run_python_file", return_value=0) as run_python_file,
            mock.patch.object(sys, "argv", ["_config_kv.py", "--case-config", str(cfg_path)]),
        ):
            self.assertEqual(entry.main(), 0)
        self.assertEqual(run_python_file.call_args.kwargs["expected_scene_id"], "ci_top_attention_config_kv")

    def test_config_mq_wrapper_accepts_case_config_without_forwarding_it(self) -> None:
        entry = _load_module(
            "fluxon_test_stack/top_attention_test_index/_config_mq.py",
            "fluxon_test_stack_top_attention_config_mq_contract",
        )
        cfg_path = self._case_cfg_path(scene_id="ci_top_attention_config_mq")
        with (
            mock.patch.object(entry, "run_pytest", return_value=0) as run_pytest,
            mock.patch.object(sys, "argv", ["_config_mq.py", "--case-config", str(cfg_path)]),
        ):
            self.assertEqual(entry.main(), 0)
        self.assertEqual(run_pytest.call_args.kwargs["expected_scene_id"], "ci_top_attention_config_mq")

    def test_ctrl_c_mq_wrapper_accepts_case_config_without_forwarding_it(self) -> None:
        entry = _load_module(
            "fluxon_test_stack/top_attention_test_index/_ctrl_c_mq.py",
            "fluxon_test_stack_top_attention_ctrl_c_mq_contract",
        )
        cfg_path = self._case_cfg_path(scene_id="ci_top_attention_ctrl_c_mq")
        with (
            mock.patch.object(entry, "run_python_file", return_value=0) as run_python_file,
            mock.patch.object(sys, "argv", ["_ctrl_c_mq.py", "--case-config", str(cfg_path)]),
        ):
            self.assertEqual(entry.main(), 0)
        self.assertEqual(run_python_file.call_args.kwargs["expected_scene_id"], "ci_top_attention_ctrl_c_mq")


if __name__ == "__main__":
    raise SystemExit(unittest.main())
