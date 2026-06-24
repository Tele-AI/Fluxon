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
INDEX_ROOT = REPO_ROOT / "fluxon_test_stack" / "top_attention_test_index"


def _load_module(filename: str, module_name: str):
    module_path = INDEX_ROOT / filename
    module_dir = module_path.parent
    sys.path.insert(0, str(module_dir))
    try:
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return mod, module_path
    finally:
        if sys.path and sys.path[0] == str(module_dir):
            sys.path.pop(0)


class TestTopAttentionCtrlCContract(unittest.TestCase):
    def _write_case_config(self, root: Path, *, scene_id: str) -> Path:
        cfg_dir = root / "configs"
        cfg_dir.mkdir(parents=True, exist_ok=True)
        case_cfg = cfg_dir / "ci_scene_config.yaml"
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
                        "greptime": {"ip": "127.0.0.1", "port": 19190},
                    },
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        return case_cfg

    def test_ctrl_c_kv_accepts_case_config_and_runs_single_test(self) -> None:
        entry, module_path = _load_module("_ctrl_c_kv.py", "fluxon_test_stack_top_attention_ctrl_c_kv_contract")
        with tempfile.TemporaryDirectory() as td:
            case_cfg = self._write_case_config(Path(td), scene_id="ci_top_attention_ctrl_c_kv")
            with mock.patch.object(entry, "run_python_file", return_value=0) as run_python_file:
                with mock.patch.object(
                    sys,
                    "argv",
                    [str(module_path), "--python", "/tmp/venv/bin/python3", "--case-config", str(case_cfg)],
                ):
                    rc = entry.main()
        self.assertEqual(rc, 0)
        self.assertEqual(
            list(run_python_file.call_args.args),
            [
                "Flat index entry for existing KV/runtime Ctrl-C shutdown coverage.",
                "fluxon_py/tests/test_process_runner.py",
                ["TestProcessRunner.test_wait_subproc_or_ctrlc_retires_children_on_sigterm"],
            ],
        )
        self.assertEqual(run_python_file.call_args.kwargs["passthrough"], [])
        self.assertEqual(run_python_file.call_args.kwargs["python"], "/tmp/venv/bin/python3")

    def test_ctrl_c_mq_accepts_case_config_and_runs_python_file(self) -> None:
        entry, module_path = _load_module("_ctrl_c_mq.py", "fluxon_test_stack_top_attention_ctrl_c_mq_contract")
        with tempfile.TemporaryDirectory() as td:
            case_cfg = self._write_case_config(Path(td), scene_id="ci_top_attention_ctrl_c_mq")
            with mock.patch.object(entry, "run_python_file", return_value=0) as run_python_file:
                with mock.patch.object(
                    sys,
                    "argv",
                    [str(module_path), "--python", "/tmp/venv/bin/python3", "--case-config", str(case_cfg)],
                ):
                    rc = entry.main()
        self.assertEqual(rc, 0)
        self.assertEqual(
            list(run_python_file.call_args.args),
            [
                "Flat index entry for existing MQ Ctrl-C integration coverage.",
                "fluxon_py/tests/test_mq/test_example_ctrl_c_exit.py",
            ],
        )
        self.assertEqual(run_python_file.call_args.kwargs["passthrough"], [])
        self.assertEqual(run_python_file.call_args.kwargs["python"], "/tmp/venv/bin/python3")


if __name__ == "__main__":
    raise SystemExit(unittest.main())
