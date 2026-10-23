#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "fluxon_test_stack" / "test_runner_ui.py"


def _load_module():
    module_dir = MODULE_PATH.parent
    sys.path.insert(0, str(module_dir))
    try:
        spec = importlib.util.spec_from_file_location("fluxon_test_stack_test_runner_ui_entry_contract", MODULE_PATH)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        if sys.path and sys.path[0] == str(module_dir):
            sys.path.pop(0)


_UI = _load_module()


class TestTestRunnerUiEntryContract(unittest.TestCase):
    def test_main_delegates_to_test_runner_ui_service(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            workdir = root / "ui_workdir"
            history_root = root / "history_root"
            gitops_cfg = root / "gitops.yaml"
            history_root.mkdir()
            gitops_cfg.write_text("repos: []\n", encoding="utf-8")

            argv = [
                "test_runner_ui.py",
                "--workdir",
                str(workdir),
                "--host",
                "0.0.0.0",
                "--port",
                "18081",
                "--history-lookback-days",
                "14",
                "--history-root",
                str(history_root),
                "--gitops-config",
                str(gitops_cfg),
            ]
            with mock.patch.object(sys, "argv", argv):
                with mock.patch.object(
                    _UI.test_runner,
                    "_redirect_process_stdio_to_log",
                ) as redirect_mock, mock.patch.object(
                    _UI.test_runner,
                    "run_ui_service",
                ) as run_mock:
                    _UI.main()

            self.assertTrue(workdir.is_dir())
            redirect_mock.assert_called_once_with(
                workdir.resolve(),
                filename="test_runner_ui.log",
            )
            run_mock.assert_called_once()
            kwargs = run_mock.call_args.kwargs
            self.assertEqual(kwargs["workdir_root"], workdir.resolve())
            self.assertEqual(kwargs["host"], "0.0.0.0")
            self.assertEqual(kwargs["port"], 18081)
            self.assertEqual(kwargs["lookback_days"], 14)
            self.assertEqual(kwargs["extra_history_roots"], [history_root.resolve()])
            self.assertEqual(kwargs["gitops_config_path"], gitops_cfg.resolve())


if __name__ == "__main__":
    raise SystemExit(unittest.main())
